"""Async RingCentral Team Messaging REST API client.

A thin, plugin-local wrapper over the v1 Team Messaging endpoints used by
:class:`RingCentralAdapter`. Authenticates with a bot JWT bearer token and
handles rate-limiting (HTTP 429 + ``Retry-After``) transparently with a small
bounded retry budget.

Endpoints covered:

* ``POST   /team-messaging/v1/chats/{chatID}/posts``         — send post
* ``PATCH  /team-messaging/v1/chats/{chatID}/posts/{postID}`` — update post
* ``DELETE /team-messaging/v1/chats/{chatID}/posts/{postID}`` — delete post
* ``GET    /team-messaging/v1/chats/{chatID}/posts``          — list posts
* ``GET    /team-messaging/v1/chats``                         — list chats
* ``GET    /team-messaging/v1/persons/{personID}``            — person info
* ``POST   /team-messaging/v1/files``                         — upload file
* ``GET    /restapi/v1.0/account/~/extension/~``              — own extension
* ``POST   /team-messaging/v1/conversations``                 — create/find DM
* ``POST   /restapi/v1.0/subscription``                       — WebSocket token

All methods return either the parsed JSON dict on success or ``None`` on
failure; errors are logged at warning/error level. Callers should treat
``None`` as "request failed — retry or surface to user".
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional
from urllib.parse import quote

logger = logging.getLogger(__name__)

# Default API base. ``https://platform.devtest.ringcentral.com`` is the
# sandbox; production lives at ``https://platform.ringcentral.com``.
DEFAULT_SERVER_URL = "https://platform.ringcentral.com"

# Cap on transparent 429 retries so a sustained throttle does not block the
# event loop indefinitely. RingCentral's ``Retry-After`` is honored verbatim
# up to this cap.
MAX_RETRIES = 2

# Honor a server-supplied ``Retry-After`` only up to this many seconds; beyond
# that we surface failure to the caller so the gateway can react.
MAX_RETRY_AFTER_SECONDS = 30.0


class RingCentralClient:
    """Async REST client for RingCentral Team Messaging v1.

    Holds an aiohttp ``ClientSession`` for the lifetime of the adapter; call
    :meth:`close` on shutdown to release the connection pool.
    """

    def __init__(
        self,
        bot_token: str,
        server_url: str = DEFAULT_SERVER_URL,
        *,
        request_timeout: float = 30.0,
    ) -> None:
        self._token = (bot_token or "").strip()
        self._base_url = (server_url or DEFAULT_SERVER_URL).rstrip("/")
        self._request_timeout = request_timeout
        self._session: Any = None  # aiohttp.ClientSession — lazy init

    # ------------------------------------------------------------------
    # Session lifecycle
    # ------------------------------------------------------------------

    async def _ensure_session(self) -> Any:
        if self._session is None or getattr(self._session, "closed", True):
            import aiohttp
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self._request_timeout),
            )
        return self._session

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None

    # ------------------------------------------------------------------
    # Auth headers
    # ------------------------------------------------------------------

    def _json_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _bearer_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._token}",
            "Accept": "application/json",
        }

    # ------------------------------------------------------------------
    # Low-level request helper (handles 429 retry-after)
    # ------------------------------------------------------------------

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: Optional[Dict[str, Any]] = None,
        data: Any = None,
        headers: Optional[Dict[str, str]] = None,
        expect_json: bool = True,
    ) -> Optional[Any]:
        """Issue an HTTP request, transparently retrying HTTP 429.

        Returns parsed JSON on success (when ``expect_json``), raw bytes when
        ``expect_json`` is False, or ``None`` on any failure that exhausted
        the retry budget.
        """
        import aiohttp

        url = f"{self._base_url}/{path.lstrip('/')}"
        _headers = headers if headers is not None else self._json_headers()
        session = await self._ensure_session()

        for attempt in range(MAX_RETRIES + 1):
            try:
                async with session.request(
                    method,
                    url,
                    headers=_headers,
                    json=json_body,
                    data=data,
                ) as resp:
                    if resp.status == 429 and attempt < MAX_RETRIES:
                        retry_after = self._parse_retry_after(resp.headers.get("Retry-After"))
                        logger.warning(
                            "RC API %s %s rate-limited; sleeping %.1fs (attempt %d/%d)",
                            method, path, retry_after, attempt + 1, MAX_RETRIES,
                        )
                        await asyncio.sleep(min(retry_after, MAX_RETRY_AFTER_SECONDS))
                        continue
                    if resp.status >= 400:
                        body = await resp.text()
                        logger.error(
                            "RC API %s %s → %s: %s",
                            method, path, resp.status, body[:300],
                        )
                        return None
                    if not expect_json:
                        return await resp.read()
                    if resp.status == 204 or resp.content_length == 0:
                        return {}
                    return await resp.json()
            except aiohttp.ClientError as exc:
                logger.error("RC API %s %s network error: %s", method, path, exc)
                return None
            except asyncio.TimeoutError:
                logger.error("RC API %s %s timed out", method, path)
                return None

        return None

    @staticmethod
    def _parse_retry_after(raw: Optional[str]) -> float:
        """Parse the ``Retry-After`` header. Defaults to 1s on malformed input."""
        if not raw:
            return 1.0
        try:
            return max(0.5, float(raw))
        except (TypeError, ValueError):
            # HTTP-date form is not used by RC's Team Messaging API in
            # practice — fall back to a short, safe sleep.
            return 1.0

    # ------------------------------------------------------------------
    # Posts (send / update / delete / list)
    # ------------------------------------------------------------------

    async def send_post(
        self,
        chat_id: str,
        text: str,
        *,
        attachments: Optional[List[Dict[str, Any]]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Create a post in ``chat_id``. Returns the post dict on success."""
        payload: Dict[str, Any] = {"text": text or ""}
        if attachments:
            payload["attachments"] = attachments
        return await self._request(
            "POST",
            f"/team-messaging/v1/chats/{quote(chat_id, safe='')}/posts",
            json_body=payload,
        )

    async def update_post(
        self,
        chat_id: str,
        post_id: str,
        text: str,
    ) -> Optional[Dict[str, Any]]:
        """Edit an existing post."""
        return await self._request(
            "PATCH",
            f"/team-messaging/v1/chats/{quote(chat_id, safe='')}/posts/{quote(post_id, safe='')}",
            json_body={"text": text or ""},
        )

    async def delete_post(self, chat_id: str, post_id: str) -> bool:
        """Delete a post. Returns True on success."""
        result = await self._request(
            "DELETE",
            f"/team-messaging/v1/chats/{quote(chat_id, safe='')}/posts/{quote(post_id, safe='')}",
            expect_json=False,
        )
        return result is not None

    async def list_posts(
        self,
        chat_id: str,
        record_count: int = 50,
    ) -> Optional[List[Dict[str, Any]]]:
        """List recent posts in a chat (newest first per RC API contract)."""
        path = (
            f"/team-messaging/v1/chats/{quote(chat_id, safe='')}/posts"
            f"?recordCount={int(record_count)}"
        )
        data = await self._request("GET", path)
        if not data:
            return None
        return data.get("records", []) if isinstance(data, dict) else None

    async def list_chats(self, record_count: int = 250) -> Optional[List[Dict[str, Any]]]:
        """List chats accessible to the bot."""
        path = f"/team-messaging/v1/chats?recordCount={int(record_count)}"
        data = await self._request("GET", path)
        if not data:
            return None
        return data.get("records", []) if isinstance(data, dict) else None

    # ------------------------------------------------------------------
    # Persons + own identity
    # ------------------------------------------------------------------

    async def get_person(self, person_id: str) -> Optional[Dict[str, Any]]:
        """Fetch profile info for a single RC person."""
        if not person_id:
            return None
        return await self._request(
            "GET",
            f"/team-messaging/v1/persons/{quote(person_id, safe='')}",
        )

    async def get_own_extension(self) -> Optional[Dict[str, Any]]:
        """Fetch the bot's own extension record (id, name, etc.)."""
        return await self._request("GET", "/restapi/v1.0/account/~/extension/~")

    # ------------------------------------------------------------------
    # Conversations (DMs)
    # ------------------------------------------------------------------

    async def create_or_find_dm(self, member_ids: List[str]) -> Optional[Dict[str, Any]]:
        """Create or look up a 1:1 / group DM conversation.

        RingCentral's ``POST /team-messaging/v1/conversations`` is idempotent
        — repeated calls with the same membership return the same chat.
        """
        members = [{"id": str(m)} for m in member_ids if m]
        if not members:
            return None
        return await self._request(
            "POST",
            "/team-messaging/v1/conversations",
            json_body={"members": members},
        )

    # ------------------------------------------------------------------
    # File upload
    # ------------------------------------------------------------------

    async def upload_file(
        self,
        chat_id: str,
        file_data: bytes,
        filename: str,
        content_type: str = "application/octet-stream",
    ) -> Optional[Dict[str, Any]]:
        """Upload a file into a chat. Returns the response (contains attachment info).

        RC's file endpoint accepts the binary as the request body with the
        target chat passed via ``groupId`` and the filename via ``name`` —
        not multipart/form-data.
        """
        import aiohttp

        path = (
            f"/team-messaging/v1/files"
            f"?name={quote(filename or 'file', safe='')}"
            f"&groupId={quote(chat_id, safe='')}"
        )
        url = f"{self._base_url}/{path.lstrip('/')}"

        headers = {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": content_type or "application/octet-stream",
            "Accept": "application/json",
        }

        session = await self._ensure_session()
        for attempt in range(MAX_RETRIES + 1):
            try:
                async with session.post(url, headers=headers, data=file_data) as resp:
                    if resp.status == 429 and attempt < MAX_RETRIES:
                        retry_after = self._parse_retry_after(resp.headers.get("Retry-After"))
                        await asyncio.sleep(min(retry_after, MAX_RETRY_AFTER_SECONDS))
                        continue
                    if resp.status >= 400:
                        body = await resp.text()
                        logger.error(
                            "RC file upload (%s, %d bytes) → %s: %s",
                            filename, len(file_data), resp.status, body[:300],
                        )
                        return None
                    return await resp.json()
            except aiohttp.ClientError as exc:
                logger.error("RC file upload network error: %s", exc)
                return None
            except asyncio.TimeoutError:
                logger.error("RC file upload timed out (%s)", filename)
                return None

        return None

    # ------------------------------------------------------------------
    # WebSocket subscription token
    # ------------------------------------------------------------------

    async def create_websocket_subscription(
        self,
        event_filters: Optional[List[str]] = None,
        expires_in: int = 7200,
    ) -> Optional[Dict[str, Any]]:
        """Request a WebSocket subscription token.

        Returns the subscription record on success — callers extract the
        ``deliveryMode.address`` (WebSocket URI) to connect to.
        """
        filters = event_filters or ["/team-messaging/v1/posts"]
        payload = {
            "eventFilters": filters,
            "deliveryMode": {"transportType": "WebSocket"},
            "expiresIn": int(expires_in),
        }
        return await self._request(
            "POST",
            "/restapi/v1.0/subscription",
            json_body=payload,
        )
