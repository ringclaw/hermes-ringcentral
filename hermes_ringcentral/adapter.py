"""RingCentral Team Messaging gateway adapter for Hermes Agent.

Connects a RingCentral bot to the Hermes agent via:

  * REST API (``rc_client.RingCentralClient``) — outbound posts, edits,
    deletes, file uploads, person lookups.
  * WebSocket (``rc_ws.RingCentralWebSocket``) — inbound ``PostAdded`` events
    streamed over the platform's subscription API.

Authentication is **bot-token only** (JWT bearer header on every request) —
no OAuth refresh dance, no installable app config. Configure via env vars::

    RC_BOT_TOKEN           Bot JWT (required)
    RC_SERVER_URL          API base URL (default https://platform.ringcentral.com)
    RC_ALLOWED_USERS       Comma-separated allowed RC person IDs
    RC_ALLOW_ALL_USERS     true/false — open access (dev only)
    RC_HOME_CHANNEL        Default chat ID for cron / notification delivery
    RC_HOME_CHANNEL_NAME   Display name for the home chat
"""

from __future__ import annotations

import asyncio
import logging
import mimetypes
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
    cache_audio_from_bytes,
    cache_document_from_bytes,
    cache_image_from_bytes,
)
from gateway.platforms.helpers import MessageDeduplicator

from .rc_client import DEFAULT_SERVER_URL, RingCentralClient
from .rc_ws import RingCentralWebSocket

logger = logging.getLogger(__name__)

# RingCentral post body cap. Their public limit is 10000 chars; we use a
# practical 4000 to keep posts readable and align with Mattermost/Slack norms.
MAX_POST_LENGTH = 4000

# Inline mention syntax used by RC group posts: ``![:Person](12345)`` (or
# ``![:Team](6789)``, etc.). Recognized at the start of the message text so
# we can strip the addressing prefix before handing it to the agent.
_RC_MENTION_RE = re.compile(r"!\[:[A-Za-z]+\]\((\d+)\)")


def check_requirements() -> bool:
    """Return True if the RC adapter has its mandatory env vars + deps."""
    token = os.getenv("RC_BOT_TOKEN", "")
    if not token:
        logger.debug("RingCentral: RC_BOT_TOKEN not set")
        return False
    try:
        import aiohttp  # noqa: F401
    except ImportError:
        logger.warning("RingCentral: aiohttp not installed (pip install aiohttp)")
        return False
    return True


def _content_type_for_filename(filename: str) -> str:
    """Guess a sensible Content-Type from a filename's extension."""
    if not filename:
        return "application/octet-stream"
    guessed, _ = mimetypes.guess_type(filename)
    return guessed or "application/octet-stream"


def _strip_rc_mentions(text: str, own_person_id: Optional[str]) -> str:
    """Strip RC inline mentions from the start of ``text``.

    RC group chats prefix bot-addressed messages with one or more
    ``![:Person](12345)`` tokens. The agent should see the clean text only.
    When ``own_person_id`` is provided, the *first* matching prefix that
    targets the bot is removed; any subsequent mentions are simplified to
    their display form (the segment after the colon, stripped of markup) so
    references to other users still read naturally.
    """
    if not text:
        return text

    stripped = text.lstrip()
    leading_ws = text[: len(text) - len(stripped)]

    # Remove any leading mention prefix(es) — bot mention or otherwise.
    addressed = False
    while True:
        match = _RC_MENTION_RE.match(stripped)
        if not match:
            break
        if own_person_id and match.group(1) == str(own_person_id):
            addressed = True
        stripped = stripped[match.end():].lstrip()

    # Replace any remaining inline mentions (mid-sentence references to
    # other users) with their display form so the agent sees readable text.
    stripped = _RC_MENTION_RE.sub("", stripped)

    if not addressed:
        # No bot mention found — preserve the original leading whitespace
        # so callers that need to detect un-addressed channel chatter can
        # still compare against the raw text.
        return (leading_ws + stripped).rstrip() or text

    return stripped.strip()


def _ensure_ringcentral_platform() -> Platform:
    """Register 'ringcentral' in the Platform enum if absent.

    External plugins live outside the hermes-agent tree, so their platform
    name is not part of the built-in ``Platform`` enum.  We inject it at
    import time so ``Platform("ringcentral")`` resolves cleanly.
    """
    try:
        return Platform("ringcentral")
    except ValueError:
        # Dynamically extend the enum — stdlib Enum allows injecting new
        # members via _value2member_map_ (used by other Hermes plugins).
        member = object.__new__(Platform)
        member._name_ = "ringcentral"
        member._value_ = "ringcentral"
        Platform._value2member_map_["ringcentral"] = member
        Platform._member_map_["ringcentral"] = member
        Platform._member_names_.append("ringcentral")
        return member


_RC_PLATFORM = _ensure_ringcentral_platform()


class RingCentralAdapter(BasePlatformAdapter):
    """Gateway adapter for RingCentral Team Messaging."""

    def __init__(self, config: PlatformConfig):
        super().__init__(config, _RC_PLATFORM)

        extra = getattr(config, "extra", {}) or {}

        self._token: str = (
            (config.token or "")
            or extra.get("token", "")
            or os.getenv("RC_BOT_TOKEN", "")
        )
        self._server_url: str = (
            extra.get("server_url", "")
            or os.getenv("RC_SERVER_URL", "")
            or DEFAULT_SERVER_URL
        )

        self._client: Optional[RingCentralClient] = None
        self._ws: Optional[RingCentralWebSocket] = None

        self._own_person_id: str = ""
        self._own_name: str = ""

        self._dedup = MessageDeduplicator()

    @property
    def name(self) -> str:
        return "RingCentral"

    # ── Connection lifecycle ──────────────────────────────────────────────

    async def connect(self) -> bool:
        if not self._token:
            logger.error("RingCentral: RC_BOT_TOKEN not configured")
            self._set_fatal_error(
                "config_missing",
                "RC_BOT_TOKEN must be set",
                retryable=False,
            )
            return False

        self._client = RingCentralClient(self._token, self._server_url)

        # Resolve our own extension ID so we can filter echoes.
        ext = await self._client.get_own_extension()
        if not ext or not ext.get("id"):
            logger.error("RingCentral: failed to fetch own extension (bad token?)")
            await self._client.close()
            self._client = None
            self._set_fatal_error(
                "auth_failed",
                "RingCentral rejected bot token",
                retryable=False,
            )
            return False
        self._own_person_id = str(ext.get("id"))
        contact = ext.get("contact") or {}
        self._own_name = (
            contact.get("firstName")
            or ext.get("name")
            or "Hermes Bot"
        )
        logger.info(
            "RingCentral: authenticated as %s (id=%s) at %s",
            self._own_name, self._own_person_id, self._server_url,
        )

        # Start the WebSocket monitor.
        self._ws = RingCentralWebSocket(
            self._client,
            on_event=self._handle_ws_event,
            own_person_id=self._own_person_id,
        )
        await self._ws.start()

        self._mark_connected()
        return True

    async def disconnect(self) -> None:
        self._mark_disconnected()

        if self._ws is not None:
            try:
                await self._ws.stop()
            except Exception:
                logger.exception("RingCentral: error stopping WS")
            self._ws = None

        if self._client is not None:
            try:
                await self._client.close()
            except Exception:
                logger.exception("RingCentral: error closing client")
            self._client = None

    # ── Sending ───────────────────────────────────────────────────────────

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        if self._client is None:
            return SendResult(success=False, error="Not connected")
        if not content:
            return SendResult(success=True)

        chunks = self.truncate_message(content, MAX_POST_LENGTH)
        last_id: Optional[str] = None
        for chunk in chunks:
            data = await self._client.send_post(chat_id, chunk)
            if not data or not data.get("id"):
                return SendResult(success=False, error="Failed to create post")
            last_id = str(data["id"])
            if self._ws is not None:
                self._ws.mark_own_post(last_id)

        return SendResult(success=True, message_id=last_id)

    async def send_typing(
        self,
        chat_id: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        # RingCentral Team Messaging has no typing-indicator primitive. No-op
        # keeps the base class's typing heartbeat happy without burning API
        # quota on a request that does nothing.
        return None

    async def edit_message(
        self,
        chat_id: str,
        message_id: str,
        content: str,
        *,
        finalize: bool = False,
    ) -> SendResult:
        if self._client is None:
            return SendResult(success=False, error="Not connected")
        data = await self._client.update_post(chat_id, message_id, content or "")
        if not data or not data.get("id"):
            return SendResult(success=False, error="Failed to edit post")
        return SendResult(success=True, message_id=str(data["id"]))

    async def delete_message(self, chat_id: str, message_id: str) -> bool:
        if self._client is None:
            return False
        return await self._client.delete_post(chat_id, message_id)

    async def send_image(
        self,
        chat_id: str,
        image_url: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Download ``image_url`` and upload as a RC file attachment."""
        return await self._send_url_as_file(chat_id, image_url, caption, kind="image")

    async def send_image_file(
        self,
        chat_id: str,
        image_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        return await self._send_local_file(chat_id, image_path, caption)

    async def send_document(
        self,
        chat_id: str,
        file_path: str,
        caption: Optional[str] = None,
        file_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        return await self._send_local_file(chat_id, file_path, caption, file_name)

    async def send_voice(
        self,
        chat_id: str,
        audio_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        return await self._send_local_file(chat_id, audio_path, caption)

    async def send_video(
        self,
        chat_id: str,
        video_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        return await self._send_local_file(chat_id, video_path, caption)

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        """Return basic chat info — name + type."""
        if self._client is None:
            return {"name": chat_id, "type": "group"}
        # The RC chat endpoint isn't exposed as a single fetch on this
        # plugin's client; list_chats() is the safe portable lookup.
        chats = await self._client.list_chats(record_count=250) or []
        for chat in chats:
            if str(chat.get("id")) == str(chat_id):
                ctype = (chat.get("type") or "").lower()
                # RC types: ``Direct`` (1:1), ``Group``, ``Team``, ``Personal``.
                if ctype == "direct":
                    kind = "dm"
                elif ctype == "personal":
                    kind = "dm"
                else:
                    kind = "group"
                return {
                    "name": chat.get("name") or chat_id,
                    "type": kind,
                    "chat_id": chat_id,
                }
        return {"name": chat_id, "type": "group", "chat_id": chat_id}

    # ── File helpers ──────────────────────────────────────────────────────

    async def _send_url_as_file(
        self,
        chat_id: str,
        url: str,
        caption: Optional[str],
        kind: str = "file",
    ) -> SendResult:
        """SSRF-safe URL fetch → RC file upload → post with attachment."""
        if self._client is None:
            return SendResult(success=False, error="Not connected")

        from tools.url_safety import is_safe_url
        if not is_safe_url(url):
            logger.warning("RingCentral: blocked unsafe URL (SSRF protection)")
            return await self.send(chat_id, f"{caption or ''}\n{url}".strip())

        import aiohttp

        filename = url.rsplit("/", 1)[-1].split("?")[0] or f"{kind}.bin"
        session = await self._client._ensure_session()
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status >= 400:
                    return await self.send(chat_id, f"{caption or ''}\n{url}".strip())
                file_data = await resp.read()
                content_type = resp.content_type or _content_type_for_filename(filename)
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            logger.warning("RingCentral: download failed for %s: %s", url[:80], exc)
            return await self.send(chat_id, f"{caption or ''}\n{url}".strip())

        upload = await self._client.upload_file(chat_id, file_data, filename, content_type)
        if not upload:
            return await self.send(chat_id, f"{caption or ''}\n{url}".strip())

        # RC's file endpoint posts the attachment as part of the upload —
        # but it leaves the caption empty. Send the caption as a follow-up
        # post when one is provided.
        if caption:
            cap_result = await self.send(chat_id, caption)
            if not cap_result.success:
                return cap_result
            return cap_result
        return SendResult(success=True, message_id=str(upload.get("id") or ""))

    async def _send_local_file(
        self,
        chat_id: str,
        file_path: str,
        caption: Optional[str],
        file_name: Optional[str] = None,
    ) -> SendResult:
        if self._client is None:
            return SendResult(success=False, error="Not connected")

        path = Path(file_path)
        if not path.exists():
            logger.warning("RingCentral: local file not found, skipping: %s", file_path)
            return SendResult(success=True)

        filename = file_name or path.name
        content_type = _content_type_for_filename(filename)
        file_data = path.read_bytes()

        upload = await self._client.upload_file(chat_id, file_data, filename, content_type)
        if not upload:
            return SendResult(success=False, error="File upload failed")

        if caption:
            return await self.send(chat_id, caption)
        return SendResult(success=True, message_id=str(upload.get("id") or ""))

    # ── Inbound WebSocket events ──────────────────────────────────────────

    async def _handle_ws_event(self, body: Dict[str, Any]) -> None:
        """Convert a RC ``PostAdded`` body into a Hermes MessageEvent."""
        event_type = str(body.get("eventType") or "")
        if event_type != "PostAdded":
            # PostChanged / PostDeleted / TaskAdded / etc. are ignored for
            # now — the bot is purely conversational.
            return

        post_id = str(body.get("id") or "")
        chat_id = str(body.get("groupId") or "")
        creator_id = str(body.get("creatorId") or "")
        raw_text = body.get("text") or ""
        attachments = body.get("attachments") or []

        if not chat_id or not creator_id:
            return

        # Drop duplicates (the WS can briefly re-deliver during reconnect).
        if post_id and self._dedup.is_duplicate(post_id):
            return

        # Resolve chat type via the chats listing; default to ``group`` when
        # the listing is stale or the chat is brand-new.
        chat_info = await self.get_chat_info(chat_id)
        chat_type = chat_info.get("type") or "group"

        # Decide whether the bot is addressed.
        addressed_explicit = bool(
            self._own_person_id and f"({self._own_person_id})" in raw_text
        )
        text = _strip_rc_mentions(raw_text, self._own_person_id)

        # In group chats, ignore messages that don't mention the bot. DMs are
        # always answered.
        if chat_type != "dm" and not addressed_explicit:
            logger.debug(
                "RingCentral: skipping un-addressed group message in %s", chat_id,
            )
            return

        # Resolve sender info (best-effort; failures don't block dispatch).
        sender_name = creator_id
        sender_user = await self._client.get_person(creator_id) if self._client else None
        if sender_user:
            sender_name = (
                sender_user.get("firstName")
                or sender_user.get("displayName")
                or sender_user.get("email")
                or creator_id
            )

        # Inline attachments — RC sends file/image refs as ``attachments``.
        # Download what we can and cache locally so the agent's vision tool
        # can pick the files up via plain file paths.
        media_urls: List[str] = []
        media_types: List[str] = []
        if attachments and self._client is not None:
            for att in attachments:
                downloaded = await self._download_attachment(att)
                if downloaded:
                    local_path, mime = downloaded
                    media_urls.append(local_path)
                    media_types.append(mime)

        msg_type = MessageType.TEXT
        if text.startswith("/"):
            msg_type = MessageType.COMMAND
        elif media_types:
            if any(m.startswith("image/") for m in media_types):
                msg_type = MessageType.PHOTO
            elif any(m.startswith("audio/") for m in media_types):
                msg_type = MessageType.VOICE
            else:
                msg_type = MessageType.DOCUMENT

        source = self.build_source(
            chat_id=chat_id,
            chat_name=chat_info.get("name"),
            chat_type=chat_type,
            user_id=creator_id,
            user_name=sender_name,
            message_id=post_id or None,
        )

        event = MessageEvent(
            text=text,
            message_type=msg_type,
            source=source,
            raw_message=body,
            message_id=post_id or None,
            media_urls=media_urls or None,
            media_types=media_types or None,
        )

        await self.handle_message(event)

    async def _download_attachment(
        self,
        attachment: Dict[str, Any],
    ) -> Optional[tuple[str, str]]:
        """Download a single inbound attachment to the local cache.

        Returns ``(local_path, mime_type)`` on success, or ``None`` on
        failure. Bearer auth is required for RC's content URLs so we can't
        just hand the URL to downstream tools directly.
        """
        if self._client is None:
            return None

        uri = attachment.get("uri") or attachment.get("contentUri") or ""
        if not uri:
            return None

        filename = attachment.get("fileName") or attachment.get("name") or "attachment"
        mime = attachment.get("contentType") or _content_type_for_filename(filename)

        import aiohttp

        session = await self._client._ensure_session()
        try:
            async with session.get(
                uri,
                headers=self._client._bearer_headers(),
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                if resp.status >= 400:
                    logger.warning("RingCentral: attachment download HTTP %s for %s", resp.status, filename)
                    return None
                data = await resp.read()
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            logger.warning("RingCentral: attachment download failed: %s", exc)
            return None

        try:
            if mime.startswith("image/"):
                ext = Path(filename).suffix or ".png"
                local_path = cache_image_from_bytes(data, ext)
            elif mime.startswith("audio/"):
                ext = Path(filename).suffix or ".ogg"
                local_path = cache_audio_from_bytes(data, ext)
            else:
                local_path = cache_document_from_bytes(data, filename)
        except ValueError as exc:
            logger.warning("RingCentral: skipping attachment %s: %s", filename, exc)
            return None
        return local_path, mime


# ---------------------------------------------------------------------------
# Env-driven auto-configuration
# ---------------------------------------------------------------------------


def _env_enablement() -> Optional[dict]:
    """Seed ``PlatformConfig.extra`` from RC_* env vars.

    Called by the plugin registry's env-enablement hook BEFORE the adapter is
    constructed. Returns ``None`` when ``RC_BOT_TOKEN`` is missing so that
    ``gateway status`` correctly reports the platform as disabled.
    """
    token = os.getenv("RC_BOT_TOKEN", "").strip()
    if not token:
        return None

    seed: dict = {"token": token}
    server_url = os.getenv("RC_SERVER_URL", "").strip()
    if server_url:
        seed["server_url"] = server_url

    home = os.getenv("RC_HOME_CHANNEL", "").strip()
    if home:
        seed["home_channel"] = {
            "chat_id": home,
            "name": os.getenv("RC_HOME_CHANNEL_NAME", "").strip() or home,
        }
    return seed


def _is_connected(config) -> bool:
    """Return True when RC_BOT_TOKEN is configured."""
    extra = getattr(config, "extra", {}) or {}
    token = (
        getattr(config, "token", "")
        or extra.get("token")
        or os.getenv("RC_BOT_TOKEN", "")
    )
    return bool((token or "").strip())


def validate_config(config) -> bool:
    """Same shape as ``_is_connected`` — token presence is the only gate."""
    return _is_connected(config)


# ---------------------------------------------------------------------------
# Standalone (out-of-process) sender
# ---------------------------------------------------------------------------


async def _standalone_send(
    pconfig,
    chat_id: str,
    message: str,
    *,
    thread_id: Optional[str] = None,
    media_files: Optional[list] = None,
    force_document: bool = False,
) -> Dict[str, Any]:
    """Send a single post without an in-process adapter.

    Used by ``tools/send_message_tool._send_via_adapter`` when the gateway
    runner lives in a different process from cron (the common case for
    ``hermes cron`` deliveries). Opens an ephemeral REST session, uploads
    any attached files into the target chat, posts the message, and exits.

    ``thread_id`` is accepted for signature parity but unused — RingCentral
    Team Messaging does not expose a threaded-reply primitive on posts.
    ``force_document`` is accepted but unused for the same reason: every
    upload is a generic file attachment.
    """
    try:
        import aiohttp  # noqa: F401
    except ImportError:
        return {"error": "aiohttp not installed. Run: pip install aiohttp"}

    extra = getattr(pconfig, "extra", {}) or {}
    token = (
        getattr(pconfig, "token", None)
        or extra.get("token")
        or os.getenv("RC_BOT_TOKEN", "")
    ).strip()
    server_url = (
        extra.get("server_url")
        or os.getenv("RC_SERVER_URL", "")
        or DEFAULT_SERVER_URL
    )
    if not token:
        return {"error": "RingCentral standalone send: RC_BOT_TOKEN must be set"}
    if not chat_id:
        return {"error": "RingCentral standalone send: chat_id is required"}

    client = RingCentralClient(token, server_url)
    try:
        for media in media_files or []:
            path = media.get("path") if isinstance(media, dict) else media
            if not path or not os.path.exists(path):
                continue
            file_data = Path(path).read_bytes()
            filename = os.path.basename(path)
            upload = await client.upload_file(
                chat_id, file_data, filename, _content_type_for_filename(filename),
            )
            if not upload:
                return {
                    "error": f"RingCentral file upload failed for {filename}",
                }

        data = await client.send_post(chat_id, message or "")
        if not data or not data.get("id"):
            return {"error": "RingCentral API error: send_post returned no id"}
        return {
            "success": True,
            "platform": "ringcentral",
            "chat_id": chat_id,
            "message_id": str(data["id"]),
        }
    except Exception as exc:  # noqa: BLE001
        return {"error": f"RingCentral standalone send failed: {exc}"}
    finally:
        try:
            await client.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Plugin registration entry point
# ---------------------------------------------------------------------------


def register(ctx) -> None:
    """Hermes plugin entry point."""
    ctx.register_platform(
        name="ringcentral",
        label="RingCentral",
        adapter_factory=lambda cfg: RingCentralAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=_is_connected,
        required_env=["RC_BOT_TOKEN"],
        install_hint="pip install aiohttp websockets",
        env_enablement_fn=_env_enablement,
        cron_deliver_env_var="RC_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        allowed_users_env="RC_ALLOWED_USERS",
        allow_all_env="RC_ALLOW_ALL_USERS",
        max_message_length=MAX_POST_LENGTH,
        emoji="📞",
        allow_update_command=True,
        platform_hint=(
            "You are chatting via RingCentral Team Messaging. Posts support "
            "standard Markdown (bold, italic, lists, code blocks, links). "
            "Use plain Markdown — RingCentral does not render Telegram-style "
            "MarkdownV2. Long responses are split automatically across "
            "multiple posts."
        ),
    )
