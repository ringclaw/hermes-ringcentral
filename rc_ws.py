"""RingCentral WebSocket monitor.

Owns the lifetime of the bot's WebSocket subscription:

  1. Asks the REST client for a subscription token (``POST /restapi/v1.0/subscription``)
  2. Connects to the returned WebSocket URI
  3. Streams events to a user-supplied callback
  4. Reconnects with exponential backoff + jitter on disconnect

Echo dedup is handled by an explicit ``mark_own_post`` API: the adapter
records every post ID it sends so the monitor can drop the inbound echo
that follows. ``PostAdded`` events whose ``creatorId`` matches the bot's
own RC person ID are also filtered.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
from collections import deque
from typing import Any, Awaitable, Callable, Deque, Optional, Set

from rc_client import RingCentralClient

logger = logging.getLogger(__name__)

# Reconnect tuning. Matches the conventions used by other Hermes streaming
# adapters (Mattermost, Discord) so SREs don't have to learn per-platform knobs.
_RECONNECT_BASE_DELAY = 2.0
_RECONNECT_MAX_DELAY = 60.0
_RECONNECT_JITTER_FRACTION = 0.2

# How long we remember our own outbound post IDs for echo suppression.
# RC's WS round-trip is usually <1s; 256 entries with FIFO eviction keeps
# the working set tiny even under sustained traffic.
_OWN_POST_HISTORY = 256

# Event filter for inbound Team Messaging posts. Kept here so callers can
# extend it (e.g. add ``/team-messaging/v1/chats`` events later) without
# touching the client surface.
DEFAULT_EVENT_FILTERS = ["/team-messaging/v1/posts"]


EventCallback = Callable[[dict], Awaitable[None]]


class RingCentralWebSocket:
    """Long-lived WebSocket listener with reconnect.

    The monitor owns no state beyond the connection itself; message
    dispatch is delegated to ``on_event`` which the adapter wires to its
    own ``handle_message`` pipeline.
    """

    def __init__(
        self,
        client: RingCentralClient,
        on_event: EventCallback,
        *,
        own_person_id: Optional[str] = None,
        event_filters: Optional[list] = None,
    ) -> None:
        self._client = client
        self._on_event = on_event
        self._own_person_id: Optional[str] = str(own_person_id) if own_person_id else None
        self._event_filters = list(event_filters or DEFAULT_EVENT_FILTERS)

        # Dedup: our own outbound post IDs. ``deque`` for O(1) FIFO bounds,
        # ``set`` for O(1) lookup — kept in sync.
        self._own_post_ids: Set[str] = set()
        self._own_post_history: Deque[str] = deque(maxlen=_OWN_POST_HISTORY)

        self._ws: Any = None  # aiohttp.ClientWebSocketResponse
        self._task: Optional[asyncio.Task] = None
        self._closing = False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def set_own_person_id(self, person_id: str) -> None:
        """Update the bot's own person ID for echo filtering."""
        if person_id:
            self._own_person_id = str(person_id)

    def mark_own_post(self, post_id: str) -> None:
        """Record an outbound post ID so its inbound echo is suppressed."""
        if not post_id:
            return
        pid = str(post_id)
        if pid in self._own_post_ids:
            return
        if len(self._own_post_history) == _OWN_POST_HISTORY:
            # deque is full — discard oldest from the lookup set too.
            evicted = self._own_post_history[0]
            self._own_post_ids.discard(evicted)
        self._own_post_history.append(pid)
        self._own_post_ids.add(pid)

    def is_own_post(self, post_id: str) -> bool:
        """Return True if ``post_id`` is one we sent (echo)."""
        return bool(post_id) and str(post_id) in self._own_post_ids

    async def start(self) -> None:
        """Spawn the reconnect loop in the background."""
        if self._task and not self._task.done():
            return
        self._closing = False
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        """Tear down the WebSocket and cancel the reconnect loop."""
        self._closing = True

        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        self._task = None

    # ------------------------------------------------------------------
    # Reconnect loop
    # ------------------------------------------------------------------

    async def _run_loop(self) -> None:
        delay = _RECONNECT_BASE_DELAY
        while not self._closing:
            try:
                await self._connect_and_listen()
                # Clean disconnect — reset backoff.
                delay = _RECONNECT_BASE_DELAY
            except asyncio.CancelledError:
                return
            except _PermanentAuthError as exc:
                logger.error("RC WebSocket auth permanently failed: %s — stopping reconnect", exc)
                return
            except Exception as exc:  # noqa: BLE001
                if self._closing:
                    return
                logger.warning("RC WebSocket error: %s — reconnecting in %.0fs", exc, delay)

            if self._closing:
                return

            jitter = delay * _RECONNECT_JITTER_FRACTION * random.random()
            await asyncio.sleep(delay + jitter)
            delay = min(delay * 2, _RECONNECT_MAX_DELAY)

    async def _connect_and_listen(self) -> None:
        """Single WebSocket session: subscribe, connect, dispatch events."""
        import aiohttp

        subscription = await self._client.create_websocket_subscription(
            event_filters=self._event_filters,
        )
        if not subscription:
            raise RuntimeError("RC subscription request failed")

        ws_uri = self._extract_ws_uri(subscription)
        if not ws_uri:
            raise RuntimeError(
                f"RC subscription response missing WebSocket URI: {subscription!r}"
            )

        session = await self._client._ensure_session()
        logger.info("RC WebSocket: connecting to %s", _redact_ws_uri(ws_uri))

        try:
            self._ws = await session.ws_connect(ws_uri, heartbeat=30.0)
        except aiohttp.WSServerHandshakeError as exc:
            if exc.status in {401, 403}:
                raise _PermanentAuthError(f"HTTP {exc.status} on WS handshake") from exc
            raise

        logger.info("RC WebSocket: connected")

        try:
            async for raw_msg in self._ws:
                if self._closing:
                    return
                if raw_msg.type in {aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY}:
                    try:
                        event = json.loads(raw_msg.data)
                    except (json.JSONDecodeError, TypeError):
                        logger.debug("RC WS: non-JSON frame, skipping")
                        continue
                    await self._dispatch(event)
                elif raw_msg.type in {
                    aiohttp.WSMsgType.ERROR,
                    aiohttp.WSMsgType.CLOSE,
                    aiohttp.WSMsgType.CLOSING,
                    aiohttp.WSMsgType.CLOSED,
                }:
                    logger.info("RC WebSocket: closed (%s)", raw_msg.type)
                    return
        finally:
            self._ws = None

    # ------------------------------------------------------------------
    # Event dispatch + filtering
    # ------------------------------------------------------------------

    async def _dispatch(self, event: dict) -> None:
        """Filter system frames + own-message echoes, then call ``on_event``."""
        # RC sends control frames (heartbeats, connection-details, etc.) on
        # the same channel as event payloads. Real events have a ``body``
        # dict with the resource payload.
        body = event.get("body") if isinstance(event, dict) else None
        if not isinstance(body, dict):
            return

        # PostAdded carries the new post inline. PostChanged / PostDeleted
        # are passed through too — the adapter decides whether to act on them.
        event_type = body.get("eventType") or ""
        if not event_type:
            return

        post_id = str(body.get("id") or "")
        creator_id = str(body.get("creatorId") or "")

        # Drop our own outbound echoes.
        if self._own_person_id and creator_id == self._own_person_id:
            return
        if post_id and self.is_own_post(post_id):
            return

        try:
            await self._on_event(body)
        except Exception:
            logger.exception("RC WebSocket: on_event callback raised")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_ws_uri(subscription: dict) -> Optional[str]:
        """Pull the WebSocket URI out of a subscription response.

        RC nests it under ``deliveryMode.address`` for WebSocket transports.
        """
        if not isinstance(subscription, dict):
            return None
        dm = subscription.get("deliveryMode") or {}
        if isinstance(dm, dict):
            uri = dm.get("address") or dm.get("uri")
            if uri:
                return str(uri)
        # Some responses surface the URI at the top level — fall back to that.
        return subscription.get("uri") or None


class _PermanentAuthError(RuntimeError):
    """Raised when the RC server rejects WS auth in a way retry cannot fix."""


def _redact_ws_uri(uri: str) -> str:
    """Strip query-string secrets from a WebSocket URI for safe logging."""
    if not uri:
        return ""
    qpos = uri.find("?")
    return uri if qpos < 0 else uri[:qpos] + "?<redacted>"
