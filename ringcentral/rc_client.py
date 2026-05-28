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
* ``GET    /restapi/v1.0/glip/groups/{chatID}/posts``         — legacy group posts fallback
* ``GET    /team-messaging/v1/chats``                         — list chats
* ``GET    /team-messaging/v1/persons/{personID}``            — person info
* ``POST   /restapi/v1.0/account/~/directory/entries/search`` — directory search
* ``POST   /team-messaging/v1/files``                         — upload file
* ``GET    /restapi/v1.0/account/~/extension/~``              — own extension
* ``POST   /team-messaging/v1/conversations``                 — create/find DM
* ``POST   /restapi/oauth/wstoken``                            — WebSocket token

All methods return either the parsed JSON dict on success or ``None`` on
failure; errors are logged at warning/error level. Callers should treat
``None`` as "request failed — retry or surface to user".
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional
from urllib.parse import quote, urlencode

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

# RingCentral access tokens are short-lived. Refresh a little early so a
# long-running request does not race the exact expiry boundary.
TOKEN_REFRESH_SKEW_SECONDS = 60.0

JWT_GRANT_TYPE = "urn:ietf:params:oauth:grant-type:jwt-bearer"


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
        client_id: str = "",
        client_secret: str = "",
        jwt_token: str = "",
        request_timeout: float = 30.0,
    ) -> None:
        self._token = (bot_token or "").strip()
        self._client_id = (client_id or "").strip()
        self._client_secret = (client_secret or "").strip()
        self._jwt_token = (jwt_token or "").strip()
        self._base_url = (server_url or DEFAULT_SERVER_URL).rstrip("/")
        self._request_timeout = request_timeout
        self._session: Any = None  # aiohttp.ClientSession — lazy init
        self._auth_lock = asyncio.Lock()
        self._token_expires_at = 0.0
        self._owner_id = ""
        self._last_status: Optional[int] = None

    @classmethod
    def from_jwt(
        cls,
        *,
        client_id: str,
        client_secret: str,
        jwt_token: str,
        server_url: str = DEFAULT_SERVER_URL,
        request_timeout: float = 30.0,
    ) -> "RingCentralClient":
        """Build a client that exchanges a RingCentral JWT for access tokens."""
        return cls(
            "",
            server_url,
            client_id=client_id,
            client_secret=client_secret,
            jwt_token=jwt_token,
            request_timeout=request_timeout,
        )

    @property
    def last_status(self) -> Optional[int]:
        """HTTP status from the most recent REST call, when available."""
        return self._last_status

    @property
    def owner_id(self) -> str:
        """RingCentral owner_id returned by the OAuth token endpoint."""
        return self._owner_id

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

    def _uses_jwt_auth(self) -> bool:
        return bool(self._client_id and self._client_secret and self._jwt_token)

    async def _ensure_access_token(self) -> None:
        """Exchange/refresh a configured JWT for an OAuth access token."""
        if not self._uses_jwt_auth():
            return

        now = time.time()
        if self._token and now < self._token_expires_at - TOKEN_REFRESH_SKEW_SECONDS:
            return

        async with self._auth_lock:
            now = time.time()
            if self._token and now < self._token_expires_at - TOKEN_REFRESH_SKEW_SECONDS:
                return

            import aiohttp

            session = await self._ensure_session()
            url = f"{self._base_url}/restapi/oauth/token"
            headers = {"Accept": "application/json"}
            data = {
                "grant_type": JWT_GRANT_TYPE,
                "assertion": self._jwt_token,
            }
            try:
                async with session.post(
                    url,
                    headers=headers,
                    data=data,
                    auth=aiohttp.BasicAuth(self._client_id, self._client_secret),
                ) as resp:
                    self._last_status = resp.status
                    if resp.status >= 400:
                        body = await resp.text()
                        logger.error(
                            "RC OAuth JWT exchange → %s: %s",
                            resp.status,
                            body[:300],
                        )
                        self._token = ""
                        return
                    payload = await resp.json()
            except aiohttp.ClientError as exc:
                self._last_status = None
                logger.error("RC OAuth JWT exchange network error: %s", exc)
                self._token = ""
                return
            except asyncio.TimeoutError:
                self._last_status = None
                logger.error("RC OAuth JWT exchange timed out")
                self._token = ""
                return

            access_token = str(payload.get("access_token") or "").strip()
            if not access_token:
                logger.error("RC OAuth JWT exchange response missing access_token")
                self._token = ""
                return

            expires_in = payload.get("expires_in") or payload.get("expiresIn") or 3600
            try:
                ttl = max(60.0, float(expires_in))
            except (TypeError, ValueError):
                ttl = 3600.0

            self._token = access_token
            self._token_expires_at = time.time() + ttl
            owner_id = payload.get("owner_id") or payload.get("ownerId")
            if owner_id:
                self._owner_id = str(owner_id)

    def _json_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    async def json_headers(self) -> Dict[str, str]:
        await self._ensure_access_token()
        return self._json_headers()

    def _bearer_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._token}",
            "Accept": "application/json",
        }

    async def bearer_headers(self) -> Dict[str, str]:
        await self._ensure_access_token()
        return self._bearer_headers()

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
        _headers = headers if headers is not None else await self.json_headers()
        if not _headers.get("Authorization", "").strip().removeprefix("Bearer").strip():
            logger.error("RC API %s %s missing access token", method, path)
            self._last_status = None
            return None
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
                    self._last_status = resp.status
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
                self._last_status = None
                logger.error("RC API %s %s network error: %s", method, path, exc)
                return None
            except asyncio.TimeoutError:
                self._last_status = None
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

    async def list_legacy_group_posts(
        self,
        chat_id: str,
        record_count: int = 50,
    ) -> Optional[List[Dict[str, Any]]]:
        """List recent posts using the legacy Glip group endpoint.

        Some integration/webhook-authored Team Messaging posts are returned by
        the modern posts endpoint with empty ``text`` and ``creatorId`` fields,
        while the legacy Glip shape still carries their readable text and
        ``activity`` label.
        """
        path = (
            f"/restapi/v1.0/glip/groups/{quote(chat_id, safe='')}/posts"
            f"?recordCount={int(record_count)}"
        )
        data = await self._request("GET", path)
        if not data:
            return None
        return data.get("records", []) if isinstance(data, dict) else None

    async def get_chat(self, chat_id: str) -> Optional[Dict[str, Any]]:
        """Fetch one chat by ID."""
        if not chat_id:
            return None
        return await self._request(
            "GET",
            f"/team-messaging/v1/chats/{quote(chat_id, safe='')}",
        )

    async def list_chats(
        self,
        record_count: int = 250,
        *,
        chat_type: str = "",
    ) -> Optional[List[Dict[str, Any]]]:
        """List chats accessible to this identity."""
        params: Dict[str, Any] = {"recordCount": int(record_count)}
        if chat_type:
            params["type"] = chat_type
        path = f"/team-messaging/v1/chats?{urlencode(params)}"
        data = await self._request("GET", path)
        if not data:
            return None
        return data.get("records", []) if isinstance(data, dict) else None

    async def list_recent_chats(
        self,
        record_count: int = 250,
        *,
        chat_type: str = "",
    ) -> Optional[List[Dict[str, Any]]]:
        """List recently active chats accessible to this identity."""
        params: Dict[str, Any] = {"recordCount": int(record_count)}
        if chat_type:
            params["type"] = chat_type
        path = f"/team-messaging/v1/recent/chats?{urlencode(params)}"
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

    async def search_directory(self, query: str) -> Optional[List[Dict[str, Any]]]:
        """Search the RingCentral company directory for people."""
        search_string = str(query or "").strip()
        if not search_string:
            return []
        data = await self._request(
            "POST",
            "/restapi/v1.0/account/~/directory/entries/search",
            json_body={"searchString": search_string},
        )
        if not data:
            return None
        return data.get("records", []) if isinstance(data, dict) else None

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

        await self._ensure_access_token()
        if not self._token:
            logger.error("RC file upload missing access token")
            self._last_status = None
            return None

        headers = {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": content_type or "application/octet-stream",
            "Accept": "application/json",
        }

        session = await self._ensure_session()
        for attempt in range(MAX_RETRIES + 1):
            try:
                async with session.post(url, headers=headers, data=file_data) as resp:
                    self._last_status = resp.status
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
                self._last_status = None
                logger.error("RC file upload network error: %s", exc)
                return None
            except asyncio.TimeoutError:
                self._last_status = None
                logger.error("RC file upload timed out (%s)", filename)
                return None

        return None

    # ------------------------------------------------------------------
    # WebSocket access token
    # ------------------------------------------------------------------

    async def create_websocket_token(self) -> Optional[Dict[str, Any]]:
        """Request a single-use RingCentral WebSocket access token."""
        return await self._request(
            "POST",
            "/restapi/oauth/wstoken",
        )
