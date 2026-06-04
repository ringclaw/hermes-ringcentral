"""Tests for the RingCentral platform adapter plugin."""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

# Ensure the repo root is on sys.path so `ringcentral` package resolves.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from gateway.platforms.base import MessageEvent, ProcessingOutcome  # noqa: E402
from ringcentral import (  # noqa: E402
    register,
)
from ringcentral.adapter import (  # noqa: E402
    RingCentralAdapter,
    check_requirements,
    _allowed_channels,
    _allowed_user_emails,
    _channel_ids_from,
    _content_type_for_filename,
    _email_allowlist_from,
    _env_enablement,
    _free_response_channels,
    _history_directory_search_terms,
    _history_message_limit_from,
    _ignored_channels,
    _is_connected,
    _no_thread_channels,
    _normalize_allowed_user_emails_env,
    _require_mention,
    _ringcentral_bot_tool_available,
    _ringcentral_confirm_artifact_action,
    _ringcentral_create_adaptive_card,
    _ringcentral_create_calendar_event,
    _ringcentral_create_note,
    _ringcentral_delete_adaptive_card,
    _ringcentral_delete_calendar_event,
    _ringcentral_delete_note,
    _ringcentral_get_adaptive_card,
    _ringcentral_get_calendar_event,
    _ringcentral_get_note,
    _ringcentral_get_recent_messages,
    _ringcentral_history_tool_available,
    _ringcentral_list_calendar_events,
    _ringcentral_list_notes,
    _ringcentral_owner_tool_available,
    _ringcentral_update_adaptive_card,
    _ringcentral_update_calendar_event,
    _ringcentral_publish_note,
    _ringcentral_update_note,
    _RINGCENTRAL_HISTORY_TOOL_NAME,
    _standalone_send,
    _thread_require_mention,
    _strip_rc_mentions,
    DEFAULT_SERVER_URL,
)
from ringcentral.rc_client import JWT_GRANT_TYPE, RingCentralClient  # noqa: E402
from ringcentral.rc_ws import RingCentralWebSocket, _OWN_POST_HISTORY  # noqa: E402
from ringcentral import adapter as _rc_mod  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_adapter(token: str = "test-token", extra: dict[str, Any] | None = None) -> Any:
    """Build a RingCentralAdapter without invoking network I/O."""
    from gateway.config import PlatformConfig

    cfg = PlatformConfig(enabled=True, token=token, extra=extra or {})
    return RingCentralAdapter(cfg)


def _make_message_event(
    adapter: RingCentralAdapter,
    *,
    chat_id: str = "g-1",
    message_id: str = "p-parent",
    thread_id: str | None = None,
) -> MessageEvent:
    source = adapter.build_source(
        chat_id=chat_id,
        chat_type="group",
        user_id="alice@example.com",
        user_name="Alice",
        thread_id=thread_id,
        message_id=message_id,
    )
    return MessageEvent(
        text="hello",
        source=source,
        raw_message={"id": message_id},
        message_id=message_id,
    )


# ---------------------------------------------------------------------------
# OAuth client
# ---------------------------------------------------------------------------


class TestRingCentralClientOAuth:
    def test_jwt_exchange_caches_access_token_and_owner_id(self):
        class _Resp:
            status = 200

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            async def json(self):
                return {
                    "access_token": "access-123",
                    "expires_in": 3600,
                    "owner_id": "owner-1",
                }

            async def text(self):
                return ""

        class _Session:
            closed = False

            def __init__(self):
                self.post_kwargs = None

            def post(self, url, **kwargs):
                self.post_url = url
                self.post_kwargs = kwargs
                return _Resp()

        session = _Session()
        client = RingCentralClient.from_jwt(
            client_id="cid",
            client_secret="secret",
            jwt_token="jwt-token",
            server_url="https://platform.example.test",
        )
        client._session = session

        asyncio.run(client._ensure_access_token())

        assert client._token == "access-123"
        assert client.owner_id == "owner-1"
        assert session.post_url == "https://platform.example.test/restapi/oauth/token"
        assert session.post_kwargs["data"] == {
            "grant_type": JWT_GRANT_TYPE,
            "assertion": "jwt-token",
        }
        assert session.post_kwargs["headers"]["Accept"] == "application/json"
        assert session.post_kwargs["headers"]["Authorization"].startswith("Basic ")
        assert "auth" not in session.post_kwargs

    def test_websocket_token_uses_wstoken_endpoint(self):
        calls = []
        client = RingCentralClient("access-token", server_url="https://platform.example.test")

        async def fake_request(method, path, **kwargs):
            calls.append((method, path, kwargs))
            return {"uri": "wss://example.test/ws", "ws_access_token": "ws-token"}

        client._request = fake_request

        result = asyncio.run(client.create_websocket_token())

        assert result == {"uri": "wss://example.test/ws", "ws_access_token": "ws-token"}
        assert calls == [("POST", "/restapi/oauth/wstoken", {})]

    def test_send_post_includes_parent_post_id(self):
        calls = []
        client = RingCentralClient("access-token", server_url="https://platform.example.test")

        async def fake_request(method, path, **kwargs):
            calls.append((method, path, kwargs))
            return {"id": "p-reply", "threadId": "t-1"}

        client._request = fake_request

        result = asyncio.run(
            client.send_post("g-1", "reply", parent_post_id="p-parent")
        )

        assert result == {"id": "p-reply", "threadId": "t-1"}
        assert calls == [
            (
                "POST",
                "/team-messaging/v1/chats/g-1/posts",
                {"json_body": {"text": "reply", "parentPostId": "p-parent"}},
            )
        ]

    def test_send_post_includes_thread_id(self):
        calls = []
        client = RingCentralClient("access-token", server_url="https://platform.example.test")

        async def fake_request(method, path, **kwargs):
            calls.append((method, path, kwargs))
            return {"id": "p-reply", "threadId": "t-1"}

        client._request = fake_request

        result = asyncio.run(client.send_post("g-1", "reply", thread_id="t-1"))

        assert result == {"id": "p-reply", "threadId": "t-1"}
        assert calls == [
            (
                "POST",
                "/team-messaging/v1/chats/g-1/posts",
                {"json_body": {"text": "reply", "threadId": "t-1"}},
            )
        ]

    def test_send_post_sends_numeric_parent_post_id_as_number(self):
        calls = []
        client = RingCentralClient("access-token", server_url="https://platform.example.test")

        async def fake_request(method, path, **kwargs):
            calls.append((method, path, kwargs))
            return {"id": "p-reply", "threadId": "333333333333"}

        client._request = fake_request

        result = asyncio.run(
            client.send_post("g-1", "reply", parent_post_id="11111111111111")
        )

        assert result == {"id": "p-reply", "threadId": "333333333333"}
        assert calls == [
            (
                "POST",
                "/team-messaging/v1/chats/g-1/posts",
                {"json_body": {"text": "reply", "parentPostId": 11111111111111}},
            )
        ]

    def test_send_post_sends_numeric_thread_id_as_number(self):
        calls = []
        client = RingCentralClient("access-token", server_url="https://platform.example.test")

        async def fake_request(method, path, **kwargs):
            calls.append((method, path, kwargs))
            return {"id": "p-reply", "threadId": "333333333333"}

        client._request = fake_request

        result = asyncio.run(client.send_post("g-1", "reply", thread_id="333333333333"))

        assert result == {"id": "p-reply", "threadId": "333333333333"}
        assert calls == [
            (
                "POST",
                "/team-messaging/v1/chats/g-1/posts",
                {"json_body": {"text": "reply", "threadId": 333333333333}},
            )
        ]

    def test_search_directory_posts_search_string(self):
        calls = []
        client = RingCentralClient("access-token", server_url="https://platform.example.test")

        async def fake_request(method, path, **kwargs):
            calls.append((method, path, kwargs))
            return {"records": [{"id": "user-1", "firstName": "Alice"}]}

        client._request = fake_request

        result = asyncio.run(client.search_directory("Alice"))

        assert result == [{"id": "user-1", "firstName": "Alice"}]
        assert calls == [(
            "POST",
            "/restapi/v1.0/account/~/directory/entries/search",
            {"json_body": {"searchString": "Alice"}},
        )]

    def test_calendar_event_methods_use_verified_endpoints(self):
        calls = []
        client = RingCentralClient("access-token", server_url="https://platform.example.test")

        async def fake_request(method, path, **kwargs):
            calls.append((method, path, kwargs))
            if method == "GET" and path.endswith("/events?recordCount=10"):
                return {"records": [{"id": "event-1", "title": "Planning"}]}
            if method == "DELETE":
                return b""
            return {"id": "event-1", "title": "Planning"}

        client._request = fake_request

        events = asyncio.run(client.list_events("g-1", 10))
        created = asyncio.run(client.create_event("g-1", {
            "title": "Planning",
            "startTime": "2026-06-04T10:00:00Z",
            "endTime": "2026-06-04T11:00:00Z",
        }))
        read = asyncio.run(client.get_event("event-1"))
        updated = asyncio.run(client.update_event("event-1", {
            "title": "Updated",
            "startTime": "2026-06-04T10:00:00Z",
            "endTime": "2026-06-04T11:00:00Z",
        }))
        deleted = asyncio.run(client.delete_event("event-1"))

        assert events == [{"id": "event-1", "title": "Planning"}]
        assert created == {"id": "event-1", "title": "Planning"}
        assert read == {"id": "event-1", "title": "Planning"}
        assert updated == {"id": "event-1", "title": "Planning"}
        assert deleted is True
        assert calls == [
            ("GET", "/team-messaging/v1/groups/g-1/events?recordCount=10", {}),
            (
                "POST",
                "/team-messaging/v1/groups/g-1/events",
                {
                    "json_body": {
                        "title": "Planning",
                        "startTime": "2026-06-04T10:00:00Z",
                        "endTime": "2026-06-04T11:00:00Z",
                    },
                },
            ),
            ("GET", "/team-messaging/v1/events/event-1", {}),
            (
                "PUT",
                "/team-messaging/v1/events/event-1",
                {
                    "json_body": {
                        "title": "Updated",
                        "startTime": "2026-06-04T10:00:00Z",
                        "endTime": "2026-06-04T11:00:00Z",
                    },
                },
            ),
            ("DELETE", "/team-messaging/v1/events/event-1", {"expect_json": False}),
        ]

    def test_adaptive_card_methods_use_verified_endpoints(self):
        calls = []
        client = RingCentralClient("access-token", server_url="https://platform.example.test")

        async def fake_request(method, path, **kwargs):
            calls.append((method, path, kwargs))
            if method == "DELETE":
                return b""
            return {"id": "card-1", "type": "AdaptiveCard"}

        client._request = fake_request

        card = {"version": "1.3", "body": [{"type": "TextBlock", "text": "hello"}]}
        created = asyncio.run(client.create_adaptive_card("g-1", card))
        read = asyncio.run(client.get_adaptive_card("card-1"))
        updated = asyncio.run(client.update_adaptive_card("card-1", card))
        deleted = asyncio.run(client.delete_adaptive_card("card-1"))

        assert created == {"id": "card-1", "type": "AdaptiveCard"}
        assert read == {"id": "card-1", "type": "AdaptiveCard"}
        assert updated == {"id": "card-1", "type": "AdaptiveCard"}
        assert deleted is True
        assert calls == [
            (
                "POST",
                "/team-messaging/v1/chats/g-1/adaptive-cards",
                {
                    "json_body": {
                        "version": "1.3",
                        "body": [{"type": "TextBlock", "text": "hello"}],
                        "type": "AdaptiveCard",
                    },
                },
            ),
            ("GET", "/team-messaging/v1/adaptive-cards/card-1", {}),
            (
                "PUT",
                "/team-messaging/v1/adaptive-cards/card-1",
                {
                    "json_body": {
                        "version": "1.3",
                        "body": [{"type": "TextBlock", "text": "hello"}],
                        "type": "AdaptiveCard",
                    },
                },
            ),
            ("DELETE", "/team-messaging/v1/adaptive-cards/card-1", {"expect_json": False}),
        ]

    def test_note_methods_use_verified_endpoints(self):
        calls = []
        client = RingCentralClient("access-token", server_url="https://platform.example.test")

        async def fake_request(method, path, **kwargs):
            await asyncio.sleep(0)
            calls.append((method, path, kwargs))
            if method == "GET" and path.endswith("/notes?recordCount=10"):
                return {"records": [{"id": "note-1", "title": "Note"}]}
            if method == "POST" and path.endswith("/publish"):
                return b""
            if method == "DELETE":
                return b""
            return {"id": "note-1", "title": "Note"}

        client._request = fake_request

        notes = asyncio.run(client.list_notes("g-1", 10))
        created = asyncio.run(client.create_note("g-1", {"title": "Note", "body": "<b>Body</b>"}))
        read = asyncio.run(client.get_note("note-1"))
        updated = asyncio.run(client.update_note("note-1", {"title": "Updated"}))
        published = asyncio.run(client.publish_note("note-1"))
        deleted = asyncio.run(client.delete_note("note-1"))

        assert notes == [{"id": "note-1", "title": "Note"}]
        assert created == {"id": "note-1", "title": "Note"}
        assert read == {"id": "note-1", "title": "Note"}
        assert updated == {"id": "note-1", "title": "Note"}
        assert published is True
        assert deleted is True
        assert calls == [
            ("GET", "/team-messaging/v1/chats/g-1/notes?recordCount=10", {}),
            (
                "POST",
                "/team-messaging/v1/chats/g-1/notes",
                {"json_body": {"title": "Note", "body": "<b>Body</b>"}},
            ),
            ("GET", "/team-messaging/v1/notes/note-1", {}),
            (
                "PATCH",
                "/team-messaging/v1/notes/note-1",
                {"json_body": {"title": "Updated"}},
            ),
            (
                "POST",
                "/team-messaging/v1/notes/note-1/publish",
                {"expect_json": False},
            ),
            (
                "DELETE",
                "/team-messaging/v1/notes/note-1",
                {"expect_json": False},
            ),
        ]


# ---------------------------------------------------------------------------
# Content-type guessing
# ---------------------------------------------------------------------------


class TestContentTypeGuessing:
    def test_known_image_extension(self):
        assert _content_type_for_filename("hello.png") == "image/png"

    def test_known_audio_extension(self):
        ct = _content_type_for_filename("clip.mp3")
        assert ct in {"audio/mpeg", "audio/mp3"}

    def test_known_document_extension(self):
        assert _content_type_for_filename("report.pdf") == "application/pdf"

    def test_unknown_extension_falls_back(self):
        assert _content_type_for_filename("blob.xyzzy") == "application/octet-stream"

    def test_no_filename_falls_back(self):
        assert _content_type_for_filename("") == "application/octet-stream"


# ---------------------------------------------------------------------------
# Mention-prefix stripping
# ---------------------------------------------------------------------------


class TestStripRCMentions:
    BOT_ID = "55512345"

    def test_plain_text_passthrough(self):
        assert _strip_rc_mentions("hello world", self.BOT_ID) == "hello world"

    def test_strip_leading_bot_mention(self):
        text = f"![:Person]({self.BOT_ID}) summarize this"
        assert _strip_rc_mentions(text, self.BOT_ID) == "summarize this"

    def test_strip_multiple_leading_mentions(self):
        text = f"![:Person]({self.BOT_ID}) ![:Person](99999999) hi"
        assert _strip_rc_mentions(text, self.BOT_ID) == "hi"

    def test_mid_text_mention_dropped(self):
        text = f"![:Person]({self.BOT_ID}) cc ![:Person](99999999) test"
        assert "![:Person]" not in _strip_rc_mentions(text, self.BOT_ID)

    def test_team_mention_form(self):
        text = f"![:Team]({self.BOT_ID}) check status"
        assert _strip_rc_mentions(text, self.BOT_ID) == "check status"

    def test_no_bot_id_preserves_text(self):
        text = "![:Person](99999999) hello"
        result = _strip_rc_mentions(text, None)
        assert "![:Person]" not in result
        assert "hello" in result

    def test_preserve_mode_keeps_target_mention(self):
        text = "总结 ![:Team](987654321000) 最近一天"
        assert (
            _strip_rc_mentions(
                text,
                self.BOT_ID,
                preserve_non_bot_mentions=True,
            )
            == text
        )

    def test_preserve_mode_strips_bot_mention_but_keeps_target_mention(self):
        text = f"![:Person]({self.BOT_ID}) 总结 ![:Team](987654321000) 最近一天"
        assert _strip_rc_mentions(
            text,
            self.BOT_ID,
            preserve_non_bot_mentions=True,
        ) == "总结 ![:Team](987654321000) 最近一天"

    def test_preserve_mode_keeps_leading_non_bot_mention(self):
        text = "![:Team](987654321000) 最近一天总结"
        assert (
            _strip_rc_mentions(
                text,
                self.BOT_ID,
                preserve_non_bot_mentions=True,
            )
            == text
        )


# ---------------------------------------------------------------------------
# History tool config
# ---------------------------------------------------------------------------


class TestHistoryConfig:
    def test_history_limit_defaults(self, monkeypatch):
        monkeypatch.delenv("RC_HISTORY_MESSAGE_LIMIT", raising=False)
        assert _history_message_limit_from({}) == 250

    def test_history_limit_reads_and_clamps(self, monkeypatch):
        monkeypatch.delenv("RC_HISTORY_MESSAGE_LIMIT", raising=False)
        assert _history_message_limit_from({"history_message_limit": "5000"}) == 1000
        assert _history_message_limit_from({"history_message_limit": "bad"}) == 250

    def test_history_directory_terms_extract_name_candidate_without_stopwords(self):
        terms = _history_directory_search_terms("我跟 Justin Wu 这一周的聊天")
        assert terms[0] == "Justin Wu"
        assert "我跟 Justin Wu 这一周的聊天" in terms

    def test_history_directory_terms_keep_non_latin_request(self):
        terms = _history_directory_search_terms("我跟张三这一周的聊天")
        assert terms == ["我跟张三这一周的聊天"]


class TestEmailAllowlistConfig:
    def test_allowed_user_emails_parse_commas_semicolons_and_case(self):
        assert _email_allowlist_from(
            " Owner@Example.COM ; alice@example.com,bob@example.com "
        ) == {
            "owner@example.com",
            "alice@example.com",
            "bob@example.com",
        }

    def test_allowed_user_emails_ignore_person_ids(self):
        assert _email_allowlist_from("owner-1,12345") == set()

    def test_channel_ids_parse_commas_semicolons_and_wildcard(self):
        assert _channel_ids_from(" g-1 ; g-2,* ") == {"g-1", "g-2", "*"}

    def test_channel_env_helpers_parse_allowed_and_ignored(self, monkeypatch):
        monkeypatch.setenv("RC_ALLOWED_CHANNELS", "g-1; g-2")
        monkeypatch.setenv("RC_IGNORED_CHANNELS", "g-muted,*")

        assert _allowed_channels() == {"g-1", "g-2"}
        assert _ignored_channels() == {"g-muted", "*"}

    def test_trigger_channel_env_helpers_parse_free_and_no_thread(self, monkeypatch):
        monkeypatch.setenv("RC_FREE_RESPONSE_CHANNELS", "g-free; *")
        monkeypatch.setenv("RC_NO_THREAD_CHANNELS", "g-direct,g-plain")

        assert _free_response_channels() == {"g-free", "*"}
        assert _no_thread_channels() == {"g-direct", "g-plain"}

    def test_trigger_bool_env_helpers_use_discord_defaults(self, monkeypatch):
        monkeypatch.delenv("RC_REQUIRE_MENTION", raising=False)
        monkeypatch.delenv("RC_THREAD_REQUIRE_MENTION", raising=False)

        assert _require_mention() is True
        assert _thread_require_mention() is False

        monkeypatch.setenv("RC_REQUIRE_MENTION", "off")
        monkeypatch.setenv("RC_THREAD_REQUIRE_MENTION", "yes")
        assert _require_mention() is False
        assert _thread_require_mention() is True

    def test_normalize_allowed_user_emails_env_ignores_legacy_var(
        self,
        monkeypatch,
        caplog,
    ):
        monkeypatch.setenv("RC_ALLOWED_USERS", "owner-1")
        monkeypatch.setenv("RC_ALLOWED_USER_EMAILS", "Owner@Example.COM; alice@example.com")

        _normalize_allowed_user_emails_env()

        assert _allowed_user_emails() == {"owner@example.com", "alice@example.com"}
        assert os.environ["RC_ALLOWED_USER_EMAILS"] == "alice@example.com,owner@example.com"
        assert "RC_ALLOWED_USERS is ignored" in caplog.text


# ---------------------------------------------------------------------------
# WebSocket echo dedup
# ---------------------------------------------------------------------------


class TestWebSocketEchoDedup:
    def test_build_ws_uri_appends_ws_access_token(self):
        uri = RingCentralWebSocket._build_ws_uri(
            {
                "uri": "wss://example.test/ws?existing=1",
                "ws_access_token": "ws-token",
            }
        )
        assert uri == "wss://example.test/ws?existing=1&access_token=ws-token"

    def test_send_subscription_request_uses_websocket_client_request(self):
        class _WS:
            def __init__(self):
                self.sent = None

            async def send_str(self, payload):
                self.sent = payload

        fake_ws = _WS()
        ws = RingCentralWebSocket(
            client=MagicMock(),
            on_event=AsyncMock(),
            event_filters=["/team-messaging/v1/posts"],
        )
        ws._ws = fake_ws

        asyncio.run(ws._send_subscription_request())

        payload = json.loads(fake_ws.sent)
        assert payload[0]["type"] == "ClientRequest"
        assert payload[0]["method"] == "POST"
        assert payload[0]["path"] == "/restapi/v1.0/subscription/"
        assert payload[1] == {
            "eventFilters": ["/team-messaging/v1/posts"],
            "deliveryMode": {"transportType": "WebSocket"},
        }

    def test_mark_own_post_then_filter(self):
        ws = RingCentralWebSocket(
            client=MagicMock(),
            on_event=AsyncMock(),
            own_person_id="bot-1",
        )
        ws.mark_own_post("p-100")
        assert ws.is_own_post("p-100") is True
        assert ws.is_own_post("p-999") is False

    def test_creator_id_filters_own_messages(self):
        callback = AsyncMock()
        ws = RingCentralWebSocket(
            client=MagicMock(),
            on_event=callback,
            own_person_id="bot-1",
        )
        body = {
            "eventType": "PostAdded",
            "id": "p-100",
            "groupId": "g-1",
            "creatorId": "bot-1",
            "text": "echo",
        }
        asyncio.run(ws._dispatch({"body": body}))
        callback.assert_not_awaited()

    def test_creator_filter_can_be_disabled_for_owner_ws(self):
        callback = AsyncMock()
        ws = RingCentralWebSocket(
            client=MagicMock(),
            on_event=callback,
            own_person_id="owner-1",
            filter_own_creator=False,
        )
        body = {
            "eventType": "PostAdded",
            "id": "p-owner",
            "groupId": "g-1",
            "creatorId": "owner-1",
            "text": "/status",
        }
        asyncio.run(ws._dispatch({"body": body}))
        callback.assert_awaited_once_with(body)

    def test_marked_post_id_filters_echoes(self):
        callback = AsyncMock()
        ws = RingCentralWebSocket(
            client=MagicMock(),
            on_event=callback,
            own_person_id="bot-1",
        )
        ws.mark_own_post("p-42")
        body = {
            "eventType": "PostAdded",
            "id": "p-42",
            "groupId": "g-1",
            "creatorId": "someone-else",
            "text": "echo of our edit",
        }
        asyncio.run(ws._dispatch({"body": body}))
        callback.assert_not_awaited()

    def test_unknown_post_passes_through(self):
        callback = AsyncMock()
        ws = RingCentralWebSocket(
            client=MagicMock(),
            on_event=callback,
            own_person_id="bot-1",
        )
        body = {
            "eventType": "PostAdded",
            "id": "p-77",
            "groupId": "g-1",
            "creatorId": "user-2",
            "text": "hi",
        }
        asyncio.run(ws._dispatch({"body": body}))
        callback.assert_awaited_once_with(body)

    def test_ringcentral_array_notification_passes_body_through(self):
        callback = AsyncMock()
        ws = RingCentralWebSocket(
            client=MagicMock(),
            on_event=callback,
            own_person_id="bot-1",
        )
        body = {
            "eventType": "PostAdded",
            "id": "p-88",
            "groupId": "g-1",
            "creatorId": "user-2",
            "text": "hi from ws array",
        }
        event = [
            {"type": "ServerNotification", "status": 200},
            {"event": "/team-messaging/v1/posts", "body": body},
        ]
        asyncio.run(ws._dispatch(event))
        callback.assert_awaited_once_with(body)

    def test_subscription_response_emits_safe_state(self):
        callback = AsyncMock()
        on_state = AsyncMock()
        ws = RingCentralWebSocket(
            client=MagicMock(),
            on_event=callback,
            on_state=on_state,
        )
        event = [
            {"type": "ClientResponse", "status": 200},
            {"id": "sub-1", "uuid": "uuid-1"},
        ]

        asyncio.run(ws._dispatch(event))

        on_state.assert_awaited_once_with("ws_subscription_confirmed", {"status": 200})
        callback.assert_not_awaited()

    def test_client_request_subscription_response_emits_safe_state(self):
        callback = AsyncMock()
        on_state = AsyncMock()
        ws = RingCentralWebSocket(
            client=MagicMock(),
            on_event=callback,
            on_state=on_state,
        )
        event = [
            {"type": "ClientRequest", "status": 200},
            {"id": "sub-1", "uuid": "uuid-1"},
        ]

        asyncio.run(ws._dispatch(event))

        on_state.assert_awaited_once_with("ws_subscription_confirmed", {"status": 200})
        callback.assert_not_awaited()

    def test_post_notification_emits_safe_state(self):
        callback = AsyncMock()
        on_state = AsyncMock()
        ws = RingCentralWebSocket(
            client=MagicMock(),
            on_event=callback,
            on_state=on_state,
        )
        body = {
            "eventType": "PostAdded",
            "id": "p-88",
            "groupId": "g-1",
            "creatorId": "user-2",
            "text": "hi from ws array",
        }
        event = [
            {"type": "ServerNotification", "status": 200},
            {"event": "/team-messaging/v1/posts", "body": body},
        ]

        asyncio.run(ws._dispatch(event))

        on_state.assert_awaited_once_with("ws_post_received", {})
        callback.assert_awaited_once_with(body)

    def test_history_bounded(self):
        ws = RingCentralWebSocket(MagicMock(), AsyncMock(), own_person_id="bot")
        for i in range(_OWN_POST_HISTORY + 50):
            ws.mark_own_post(f"p-{i}")
        assert ws.is_own_post("p-0") is False
        assert ws.is_own_post(f"p-{_OWN_POST_HISTORY + 49}") is True


# ---------------------------------------------------------------------------
# Requirements check
# ---------------------------------------------------------------------------


class TestRequirementsCheck:
    def test_requires_token(self, monkeypatch):
        monkeypatch.delenv("RC_BOT_TOKEN", raising=False)
        assert check_requirements() is False

    def test_passes_with_token_and_aiohttp(self, monkeypatch):
        monkeypatch.setenv("RC_BOT_TOKEN", "jwt-abc")
        try:
            import aiohttp  # noqa: F401
        except ImportError:
            pytest.skip("aiohttp not installed in this test env")
        assert check_requirements() is True

    def test_fails_when_aiohttp_missing(self, monkeypatch):
        monkeypatch.setenv("RC_BOT_TOKEN", "jwt-abc")
        import builtins

        real_import = builtins.__import__

        def _fake_import(name, *args, **kwargs):
            if name == "aiohttp":
                raise ImportError("forced for test")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=_fake_import):
            assert check_requirements() is False


# ---------------------------------------------------------------------------
# Env enablement
# ---------------------------------------------------------------------------


class TestEnvEnablement:
    def test_returns_none_without_token(self, monkeypatch):
        monkeypatch.delenv("RC_BOT_TOKEN", raising=False)
        assert _env_enablement() is None

    def test_seeds_token_and_server_url(self, monkeypatch):
        monkeypatch.setenv("RC_BOT_TOKEN", "jwt-abc")
        monkeypatch.setenv("RC_SERVER_URL", "https://platform.devtest.ringcentral.com")
        monkeypatch.delenv("RC_HOME_CHANNEL", raising=False)
        seed = _env_enablement()
        assert seed is not None
        assert seed["token"] == "jwt-abc"
        assert seed["server_url"] == "https://platform.devtest.ringcentral.com"
        assert "home_channel" not in seed

    def test_home_channel_seeded(self, monkeypatch):
        monkeypatch.setenv("RC_BOT_TOKEN", "jwt-abc")
        monkeypatch.setenv("RC_HOME_CHANNEL", "g-home-1")
        monkeypatch.setenv("RC_HOME_CHANNEL_NAME", "Updates")
        seed = _env_enablement() or {}
        assert seed.get("home_channel") == {
            "chat_id": "g-home-1",
            "name": "Updates",
        }

    def test_owner_credentials_seeded_when_complete(self, monkeypatch):
        monkeypatch.setenv("RC_BOT_TOKEN", "jwt-abc")
        monkeypatch.setenv("RC_USER_CLIENT_ID", "cid")
        monkeypatch.setenv("RC_USER_CLIENT_SECRET", "secret")
        monkeypatch.setenv("RC_USER_JWT_TOKEN", "user-jwt")
        seed = _env_enablement() or {}
        assert seed["user_client_id"] == "cid"
        assert seed["user_client_secret"] == "secret"
        assert seed["user_jwt_token"] == "user-jwt"

    def test_history_limit_seeded(self, monkeypatch):
        monkeypatch.setenv("RC_BOT_TOKEN", "jwt-abc")
        monkeypatch.setenv("RC_HISTORY_MESSAGE_LIMIT", "500")
        seed = _env_enablement() or {}
        assert seed["history_message_limit"] == 500


# ---------------------------------------------------------------------------
# Adapter init
# ---------------------------------------------------------------------------


class TestAdapterInit:
    def test_reads_token_from_config(self, monkeypatch):
        monkeypatch.delenv("RC_BOT_TOKEN", raising=False)
        from gateway.config import PlatformConfig

        cfg = PlatformConfig(enabled=True, token="cfg-tok", extra={})
        adapter = RingCentralAdapter(cfg)
        assert adapter._token == "cfg-tok"

    def test_reads_token_from_env(self, monkeypatch):
        monkeypatch.setenv("RC_BOT_TOKEN", "env-tok")
        from gateway.config import PlatformConfig

        cfg = PlatformConfig(enabled=True, extra={})
        adapter = RingCentralAdapter(cfg)
        assert adapter._token == "env-tok"

    def test_default_server_url(self, monkeypatch):
        monkeypatch.delenv("RC_SERVER_URL", raising=False)
        from gateway.config import PlatformConfig

        cfg = PlatformConfig(enabled=True, token="t", extra={})
        adapter = RingCentralAdapter(cfg)
        assert adapter._server_url == DEFAULT_SERVER_URL

    def test_server_url_from_env(self, monkeypatch):
        monkeypatch.setenv("RC_SERVER_URL", "https://platform.devtest.ringcentral.com")
        from gateway.config import PlatformConfig

        cfg = PlatformConfig(enabled=True, token="t", extra={})
        adapter = RingCentralAdapter(cfg)
        assert adapter._server_url == "https://platform.devtest.ringcentral.com"

    def test_history_limit_from_config(self, monkeypatch):
        monkeypatch.delenv("RC_HISTORY_MESSAGE_LIMIT", raising=False)
        from gateway.config import PlatformConfig

        cfg = PlatformConfig(enabled=True, token="t", extra={"history_message_limit": "333"})
        adapter = RingCentralAdapter(cfg)
        assert adapter._history_message_limit == 333

    def test_name_is_ringcentral(self):
        adapter = _make_adapter()
        assert adapter.name == "RingCentral"

    def test_owner_allowlist_auto_seeded(self, monkeypatch):
        monkeypatch.delenv("RC_ALLOWED_USERS", raising=False)
        monkeypatch.delenv("RC_ALLOWED_USER_EMAILS", raising=False)
        monkeypatch.delenv("RC_ALLOW_ALL_USERS", raising=False)
        adapter = _make_adapter()
        adapter._owner_person_id = "owner-1"
        adapter._owner_email = "owner@example.com"
        adapter._seed_owner_allowlist()
        assert adapter._owner_only_gate_enabled is True
        assert os.environ["RC_ALLOWED_USER_EMAILS"] == "owner@example.com"


# ---------------------------------------------------------------------------
# Plugin registration
# ---------------------------------------------------------------------------


class TestPluginRegistration:
    def test_register_registers_platform_and_history_tool(self):
        from gateway.config import PlatformConfig

        class Ctx:
            def register_platform(self, **kwargs):
                self.platform_kwargs = kwargs

            def register_tool(self, **kwargs):
                self.tool_kwargs_list = getattr(self, "tool_kwargs_list", [])
                self.tool_kwargs_list.append(kwargs)

        ctx = Ctx()
        register(ctx)
        adapter = ctx.platform_kwargs["adapter_factory"](
            PlatformConfig(enabled=True, token="t", extra={}),
        )

        assert isinstance(adapter, RingCentralAdapter)
        assert ctx.platform_kwargs["allowed_users_env"] == "RC_ALLOWED_USER_EMAILS"
        assert ctx.platform_kwargs["allow_all_env"] == "RC_ALLOW_ALL_USERS"
        history_tool = next(
            tool for tool in ctx.tool_kwargs_list
            if tool["name"] == _RINGCENTRAL_HISTORY_TOOL_NAME
        )
        assert history_tool["toolset"] == "ringcentral"
        assert history_tool["handler"] is _ringcentral_get_recent_messages
        assert history_tool["check_fn"] is _ringcentral_history_tool_available
        assert history_tool["is_async"] is True


# ---------------------------------------------------------------------------
# Send routing
# ---------------------------------------------------------------------------


class TestAdapterSend:
    def test_send_without_client_returns_error(self):
        adapter = _make_adapter()
        result = asyncio.run(adapter.send("g-1", "hi"))
        assert result.success is False
        assert "Not connected" in (result.error or "")

    def test_send_empty_text_is_noop(self):
        adapter = _make_adapter()
        adapter._client = MagicMock()
        result = asyncio.run(adapter.send("g-1", ""))
        assert result.success is True

    def test_send_routes_through_client_and_marks_echo(self):
        adapter = _make_adapter()
        client = MagicMock()
        client.send_post = AsyncMock(return_value={"id": "p-new-1"})
        adapter._client = client
        ws = MagicMock()
        ws.mark_own_post = MagicMock()
        adapter._ws = ws

        result = asyncio.run(adapter.send("g-1", "hello"))
        assert result.success is True
        assert result.message_id == "p-new-1"
        client.send_post.assert_awaited_once_with("g-1", "hello")
        ws.mark_own_post.assert_called_once_with("p-new-1")

    def test_send_falls_back_to_owner_on_permission_failure(self):
        adapter = _make_adapter()
        bot = MagicMock()
        bot.last_status = 403
        bot.send_post = AsyncMock(return_value=None)
        owner = MagicMock()
        owner.send_post = AsyncMock(return_value={"id": "p-owner"})
        adapter._client = bot
        adapter._owner_client = owner
        adapter._ws = MagicMock()
        adapter._owner_ws = MagicMock()

        result = asyncio.run(adapter.send("g-1", "hello"))

        assert result.success is True
        assert result.message_id == "p-owner"
        assert adapter._sent_message_identity["p-owner"] == "owner"
        bot.send_post.assert_awaited_once_with("g-1", "hello")
        owner.send_post.assert_awaited_once_with("g-1", "hello")
        adapter._ws.mark_own_post.assert_called_once_with("p-owner")
        adapter._owner_ws.mark_own_post.assert_called_once_with("p-owner")

    def test_send_uses_parent_post_id_for_reply_anchor(self):
        adapter = _make_adapter()
        client = MagicMock()
        client.send_post = AsyncMock(return_value={"id": "p-new-1", "threadId": "t-1"})
        adapter._client = client
        adapter._threads = MagicMock()

        result = asyncio.run(adapter.send("g-1", "hello", reply_to="p-parent"))

        assert result.success is True
        assert result.message_id == "p-new-1"
        client.send_post.assert_awaited_once_with(
            "g-1",
            "hello",
            parent_post_id="p-parent",
        )
        adapter._threads.mark.assert_has_calls([call("t-1"), call("p-parent")])

    def test_send_numeric_reply_anchor_reaches_api_as_number(self):
        adapter = _make_adapter()
        calls = []
        client = RingCentralClient("access-token", server_url="https://platform.example.test")

        async def fake_request(method, path, **kwargs):
            calls.append((method, path, kwargs))
            return {
                "id": "22222222222222",
                "parentPostId": "11111111111111",
                "threadId": "333333333333",
            }

        client._request = fake_request
        adapter._client = client
        adapter._threads = MagicMock()

        result = asyncio.run(
            adapter.send("444444444444", "hello", reply_to="11111111111111")
        )

        assert result.success is True
        assert result.message_id == "22222222222222"
        assert calls == [
            (
                "POST",
                "/team-messaging/v1/chats/444444444444/posts",
                {"json_body": {"text": "hello", "parentPostId": 11111111111111}},
            )
        ]
        adapter._threads.mark.assert_has_calls([
            call("333333333333"),
            call("11111111111111"),
        ])

    def test_send_marks_parent_anchor_when_reply_response_has_no_thread_id(self):
        adapter = _make_adapter()
        client = MagicMock()
        client.send_post = AsyncMock(return_value={"id": "p-new-1"})
        adapter._client = client
        adapter._threads = MagicMock()

        result = asyncio.run(adapter.send("g-1", "hello", reply_to="p-parent"))

        assert result.success is True
        adapter._threads.mark.assert_called_once_with("p-parent")

    def test_send_uses_metadata_thread_id_for_existing_thread(self):
        adapter = _make_adapter()
        client = MagicMock()
        client.send_post = AsyncMock(return_value={"id": "p-new-1", "threadId": "t-1"})
        adapter._client = client
        adapter._threads = MagicMock()

        result = asyncio.run(
            adapter.send(
                "g-1",
                "hello",
                reply_to="p-child",
                metadata={"thread_id": "t-1"},
            )
        )

        assert result.success is True
        client.send_post.assert_awaited_once_with("g-1", "hello", thread_id="t-1")

    def test_send_numeric_metadata_thread_id_reaches_api_as_number(self):
        adapter = _make_adapter()
        calls = []
        client = RingCentralClient("access-token", server_url="https://platform.example.test")

        async def fake_request(method, path, **kwargs):
            calls.append((method, path, kwargs))
            return {"id": "22222222222222", "threadId": "333333333333"}

        client._request = fake_request
        adapter._client = client
        adapter._threads = MagicMock()

        result = asyncio.run(
            adapter.send(
                "444444444444",
                "hello",
                metadata={"thread_id": "333333333333"},
            )
        )

        assert result.success is True
        assert result.message_id == "22222222222222"
        assert calls == [
            (
                "POST",
                "/team-messaging/v1/chats/444444444444/posts",
                {"json_body": {"text": "hello", "threadId": 333333333333}},
            )
        ]
        adapter._threads.mark.assert_called_once_with("333333333333")

    def test_send_uses_parent_post_metadata_for_parent_only_thread(self):
        adapter = _make_adapter()
        client = MagicMock()
        client.send_post = AsyncMock(return_value={"id": "p-new-1", "threadId": "t-1"})
        adapter._client = client
        adapter._threads = MagicMock()

        result = asyncio.run(
            adapter.send(
                "g-1",
                "hello",
                metadata={"thread_id": "parentPostId:p-parent"},
            )
        )

        assert result.success is True
        client.send_post.assert_awaited_once_with(
            "g-1",
            "hello",
            parent_post_id="p-parent",
        )

    def test_reply_to_mode_off_sends_unthreaded(self):
        adapter = _make_adapter(extra={"reply_to_mode": "off"})
        client = MagicMock()
        client.send_post = AsyncMock(return_value={"id": "p-new-1"})
        adapter._client = client

        result = asyncio.run(adapter.send("g-1", "hello", reply_to="p-parent"))

        assert result.success is True
        client.send_post.assert_awaited_once_with("g-1", "hello")

    def test_no_thread_channel_sends_unthreaded(self, monkeypatch):
        monkeypatch.setenv("RC_NO_THREAD_CHANNELS", "g-1")
        adapter = _make_adapter()
        client = MagicMock()
        client.send_post = AsyncMock(return_value={"id": "p-new-1"})
        adapter._client = client

        result = asyncio.run(adapter.send("g-1", "hello", reply_to="p-parent"))

        assert result.success is True
        client.send_post.assert_awaited_once_with("g-1", "hello")

    def test_no_thread_channel_wildcard_ignores_metadata_thread(self, monkeypatch):
        monkeypatch.setenv("RC_NO_THREAD_CHANNELS", "*")
        adapter = _make_adapter()
        client = MagicMock()
        client.send_post = AsyncMock(return_value={"id": "p-new-1"})
        adapter._client = client

        result = asyncio.run(
            adapter.send(
                "g-1",
                "hello",
                metadata={"thread_id": "t-1"},
            )
        )

        assert result.success is True
        client.send_post.assert_awaited_once_with("g-1", "hello")

    def test_send_chunks_continue_in_returned_thread(self):
        adapter = _make_adapter()
        client = MagicMock()
        client.send_post = AsyncMock(
            side_effect=[
                {"id": "p-new-1", "threadId": "t-1"},
                {"id": "p-new-2", "threadId": "t-1"},
            ]
        )
        adapter._client = client
        adapter._threads = MagicMock()
        adapter.truncate_message = MagicMock(return_value=["part 1", "part 2"])

        result = asyncio.run(adapter.send("g-1", "hello", reply_to="p-parent"))

        assert result.success is True
        assert result.message_id == "p-new-2"
        assert client.send_post.await_args_list[0].args == ("g-1", "part 1")
        assert client.send_post.await_args_list[0].kwargs == {
            "parent_post_id": "p-parent"
        }
        assert client.send_post.await_args_list[1].args == ("g-1", "part 2")
        assert client.send_post.await_args_list[1].kwargs == {"thread_id": "t-1"}

    def test_owner_fallback_preserves_parent_post_id(self):
        adapter = _make_adapter()
        bot = MagicMock()
        bot.last_status = 403
        bot.send_post = AsyncMock(return_value=None)
        owner = MagicMock()
        owner.send_post = AsyncMock(return_value={"id": "p-owner", "threadId": "t-1"})
        adapter._client = bot
        adapter._owner_client = owner
        adapter._threads = MagicMock()

        result = asyncio.run(adapter.send("g-1", "hello", reply_to="p-parent"))

        assert result.success is True
        bot.send_post.assert_awaited_once_with(
            "g-1",
            "hello",
            parent_post_id="p-parent",
        )
        owner.send_post.assert_awaited_once_with(
            "g-1",
            "hello",
            parent_post_id="p-parent",
        )

    def test_edit_uses_recorded_owner_identity(self):
        adapter = _make_adapter()
        bot = MagicMock()
        bot.update_post = AsyncMock(return_value={"id": "p-owner"})
        owner = MagicMock()
        owner.update_post = AsyncMock(return_value={"id": "p-owner"})
        adapter._client = bot
        adapter._owner_client = owner
        adapter._sent_message_identity["p-owner"] = "owner"

        result = asyncio.run(adapter.edit_message("g-1", "p-owner", "updated"))

        assert result.success is True
        owner.update_post.assert_awaited_once_with("g-1", "p-owner", "updated")
        bot.update_post.assert_not_awaited()


class TestProcessingEmoji:
    def test_processing_start_posts_waiting_emoji_in_root_thread(self, monkeypatch):
        monkeypatch.setenv("RC_PROCESSING_EMOJI_ENABLED", "true")
        monkeypatch.setenv("RC_PROCESSING_EMOJI_EDIT_DELAY_SECONDS", "3600")
        adapter = _make_adapter()
        client = MagicMock()
        client.send_post = AsyncMock(side_effect=[
            {"id": "p-wait", "threadId": "t-1"},
            {"id": "p-status", "threadId": "t-1"},
            {"id": "p-final", "threadId": "t-1"},
        ])
        client.update_post = AsyncMock(return_value={"id": "p-wait"})
        client.delete_post = AsyncMock(return_value=True)
        adapter._client = client
        event = _make_message_event(adapter)

        async def run():
            await adapter.on_processing_start(event)
            assert adapter._processing_emoji_posts == {"g-1:p-parent": "p-wait"}
            assert adapter._processing_emoji_thread_ids == {"g-1:p-parent": "t-1"}
            await adapter.send("g-1", "🗜️ Compacting context")
            await adapter.send("g-1", "final", reply_to="p-parent")
            await adapter.on_processing_complete(event, ProcessingOutcome.SUCCESS)

        asyncio.run(run())

        assert client.send_post.await_args_list[0].args == ("g-1", "👀")
        assert client.send_post.await_args_list[0].kwargs == {
            "parent_post_id": "p-parent"
        }
        assert client.send_post.await_args_list[1].args == (
            "g-1",
            "🗜️ Compacting context",
        )
        assert client.send_post.await_args_list[1].kwargs == {"thread_id": "t-1"}
        assert client.send_post.await_args_list[2].args == ("g-1", "final")
        assert client.send_post.await_args_list[2].kwargs == {"thread_id": "t-1"}
        client.delete_post.assert_awaited_once_with("g-1", "p-wait")
        client.update_post.assert_not_awaited()
        assert adapter._processing_emoji_posts == {}
        assert adapter._processing_emoji_edit_tasks == {}
        assert adapter._processing_emoji_thread_ids == {}
        assert adapter._processing_thread_routes == {}
        assert adapter._processing_keys_by_chat == {}

    def test_processing_start_posts_waiting_emoji_with_parent_post_id(self, monkeypatch):
        monkeypatch.setenv("RC_PROCESSING_EMOJI_ENABLED", "true")
        monkeypatch.setenv("RC_PROCESSING_EMOJI_EDIT_DELAY_SECONDS", "3600")
        adapter = _make_adapter()
        client = MagicMock()
        client.send_post = AsyncMock(return_value={"id": "p-wait", "threadId": "t-1"})
        client.update_post = AsyncMock(return_value={"id": "p-wait"})
        client.delete_post = AsyncMock(return_value=True)
        adapter._client = client
        event = _make_message_event(adapter)

        async def run():
            await adapter.on_processing_start(event)
            await adapter.on_processing_complete(event, ProcessingOutcome.SUCCESS)

        asyncio.run(run())

        client.send_post.assert_awaited_once_with(
            "g-1",
            "👀",
            parent_post_id="p-parent",
        )
        client.update_post.assert_not_awaited()
        client.delete_post.assert_awaited_once_with("g-1", "p-wait")
        assert adapter._processing_emoji_posts == {}
        assert adapter._processing_emoji_edit_tasks == {}

    def test_processing_emoji_edits_after_delay_in_existing_thread(self, monkeypatch):
        monkeypatch.setenv("RC_PROCESSING_EMOJI_ENABLED", "true")
        monkeypatch.setenv("RC_PROCESSING_EMOJI_EDIT_DELAY_SECONDS", "0")
        adapter = _make_adapter()
        client = MagicMock()
        client.send_post = AsyncMock(return_value={"id": "p-wait", "threadId": "t-1"})
        client.update_post = AsyncMock(return_value={"id": "p-wait"})
        client.delete_post = AsyncMock(return_value=True)
        adapter._client = client
        event = _make_message_event(adapter, thread_id="t-1")

        async def run():
            await adapter.on_processing_start(event)
            await asyncio.wait_for(
                adapter._processing_emoji_edit_tasks["g-1:p-parent"],
                timeout=1,
            )
            await adapter.on_processing_complete(event, ProcessingOutcome.SUCCESS)

        asyncio.run(run())

        client.send_post.assert_awaited_once_with("g-1", "👀", thread_id="t-1")
        client.update_post.assert_awaited_once_with("g-1", "p-wait", "⏳")
        client.delete_post.assert_awaited_once_with("g-1", "p-wait")

    def test_processing_emoji_can_be_disabled(self, monkeypatch):
        monkeypatch.setenv("RC_PROCESSING_EMOJI_ENABLED", "false")
        adapter = _make_adapter()
        client = MagicMock()
        client.send_post = AsyncMock(return_value={"id": "p-wait"})
        adapter._client = client
        event = _make_message_event(adapter)

        asyncio.run(adapter.on_processing_start(event))

        client.send_post.assert_not_awaited()
        assert adapter._processing_emoji_posts == {}
        assert adapter._processing_emoji_edit_tasks == {}

    def test_unanchored_status_threads_while_processing_emoji_disabled(self, monkeypatch):
        monkeypatch.setenv("RC_PROCESSING_EMOJI_ENABLED", "false")
        adapter = _make_adapter()
        client = MagicMock()
        client.send_post = AsyncMock(return_value={"id": "p-status", "threadId": "t-1"})
        adapter._client = client
        event = _make_message_event(adapter)

        async def run():
            await adapter.on_processing_start(event)
            await adapter.send("g-1", "🗜️ Compacting context")
            await adapter.on_processing_complete(event, ProcessingOutcome.SUCCESS)

        asyncio.run(run())

        client.send_post.assert_awaited_once_with(
            "g-1",
            "🗜️ Compacting context",
            parent_post_id="p-parent",
        )
        assert adapter._processing_thread_routes == {}
        assert adapter._processing_keys_by_chat == {}


# ---------------------------------------------------------------------------
# Owner-only inbound handling
# ---------------------------------------------------------------------------


class TestOwnerInboundHandling:
    @staticmethod
    def _owner_adapter():
        adapter = _make_adapter()
        client = MagicMock()
        client.list_chats = AsyncMock(return_value=[
            {"id": "g-1", "type": "Team", "name": "Project"},
        ])
        client.get_person = AsyncMock(side_effect=lambda pid: {
            "firstName": "Owner" if pid == "owner-1" else "Alice",
            "email": "owner@example.com" if pid == "owner-1" else "alice@example.com",
        })
        adapter._client = client
        adapter._owner_client = client
        adapter._own_person_id = "bot-1"
        adapter._owner_person_id = "owner-1"
        adapter._owner_email = "owner@example.com"
        adapter._owner_only_gate_enabled = True
        adapter.handle_message = AsyncMock()
        return adapter

    @staticmethod
    def _allowed_group_adapter():
        adapter = _make_adapter()
        client = MagicMock()
        client.list_chats = AsyncMock(return_value=[
            {"id": "g-1", "type": "Team", "name": "Project"},
            {"id": "g-2", "type": "Team", "name": "Other"},
        ])
        client.get_person = AsyncMock(return_value={
            "firstName": "Alice",
            "email": "alice@example.com",
        })
        adapter._client = client
        adapter._own_person_id = "bot-1"
        adapter.handle_message = AsyncMock()
        return adapter

    @staticmethod
    def _mentioned_group_body(chat_id: str = "g-1") -> dict:
        return {
            "eventType": "PostAdded",
            "id": "p-allowed",
            "chatId": chat_id,
            "creatorId": "user-2",
            "text": "![:Person](bot-1) hello",
        }

    @staticmethod
    def _plain_group_body(chat_id: str = "g-1") -> dict:
        return {
            "eventType": "PostAdded",
            "id": "p-plain",
            "chatId": chat_id,
            "creatorId": "user-2",
            "text": "hello without mention",
        }

    def test_non_owner_group_message_is_observed_not_dispatched(self, monkeypatch):
        monkeypatch.setenv("RC_ALLOWED_USER_EMAILS", "owner@example.com")
        monkeypatch.delenv("RC_ALLOW_ALL_USERS", raising=False)
        adapter = self._owner_adapter()
        store = MagicMock()
        store.get_or_create_session.return_value = MagicMock(session_id="s-1")
        adapter._session_store = store
        body = {
            "eventType": "PostAdded",
            "id": "p-1",
            "groupId": "g-1",
            "creatorId": "user-2",
            "text": "deployment is red",
        }

        asyncio.run(adapter._handle_ws_event(body, identity="owner"))

        adapter.handle_message.assert_not_awaited()
        appended = store.append_to_transcript.call_args.args[1]
        assert appended["observed"] is True
        assert "[Alice|user-2]" in appended["content"]
        assert "deployment is red" in appended["content"]

    def test_allowed_channel_permits_mentioned_group_message(self, monkeypatch):
        monkeypatch.setenv("RC_ALLOWED_USER_EMAILS", "alice@example.com")
        monkeypatch.setenv("RC_ALLOWED_CHANNELS", "g-1")
        adapter = self._allowed_group_adapter()

        asyncio.run(
            adapter._handle_ws_event(self._mentioned_group_body(), identity="bot")
        )

        adapter.handle_message.assert_awaited_once()

    def test_non_allowed_channel_blocks_mentioned_group_message(self, monkeypatch):
        monkeypatch.setenv("RC_ALLOWED_USER_EMAILS", "alice@example.com")
        monkeypatch.setenv("RC_ALLOWED_CHANNELS", "g-2")
        adapter = self._allowed_group_adapter()

        asyncio.run(
            adapter._handle_ws_event(self._mentioned_group_body(), identity="bot")
        )

        adapter.handle_message.assert_not_awaited()

    def test_ignored_channel_blocks_mentioned_group_message(self, monkeypatch):
        monkeypatch.setenv("RC_ALLOWED_USER_EMAILS", "alice@example.com")
        monkeypatch.setenv("RC_IGNORED_CHANNELS", "g-1")
        adapter = self._allowed_group_adapter()

        asyncio.run(
            adapter._handle_ws_event(self._mentioned_group_body(), identity="bot")
        )

        adapter.handle_message.assert_not_awaited()

    def test_ignored_channel_beats_allowed_channel(self, monkeypatch):
        monkeypatch.setenv("RC_ALLOWED_USER_EMAILS", "alice@example.com")
        monkeypatch.setenv("RC_ALLOWED_CHANNELS", "g-1")
        monkeypatch.setenv("RC_IGNORED_CHANNELS", "g-1")
        adapter = self._allowed_group_adapter()

        asyncio.run(
            adapter._handle_ws_event(self._mentioned_group_body(), identity="bot")
        )

        adapter.handle_message.assert_not_awaited()

    def test_allowed_channels_wildcard_permits_group_message(self, monkeypatch):
        monkeypatch.setenv("RC_ALLOWED_USER_EMAILS", "alice@example.com")
        monkeypatch.setenv("RC_ALLOWED_CHANNELS", "*")
        adapter = self._allowed_group_adapter()

        asyncio.run(
            adapter._handle_ws_event(self._mentioned_group_body(), identity="bot")
        )

        adapter.handle_message.assert_awaited_once()

    def test_ignored_channels_wildcard_blocks_group_message(self, monkeypatch):
        monkeypatch.setenv("RC_ALLOWED_USER_EMAILS", "alice@example.com")
        monkeypatch.setenv("RC_IGNORED_CHANNELS", "*")
        adapter = self._allowed_group_adapter()

        asyncio.run(
            adapter._handle_ws_event(self._mentioned_group_body(), identity="bot")
        )

        adapter.handle_message.assert_not_awaited()

    def test_group_slash_message_respects_channel_gate(self, monkeypatch):
        monkeypatch.setenv("RC_ALLOWED_USER_EMAILS", "owner@example.com")
        monkeypatch.setenv("RC_ALLOWED_CHANNELS", "g-2")
        adapter = self._owner_adapter()
        adapter._send_chunks = AsyncMock(return_value=MagicMock(success=True))
        body = {
            "eventType": "PostAdded",
            "id": "p-slash",
            "groupId": "g-1",
            "creatorId": "owner-1",
            "text": "/summarize Project Team",
        }

        asyncio.run(adapter._handle_ws_event(body, identity="owner"))

        adapter.handle_message.assert_not_awaited()
        adapter._send_chunks.assert_not_awaited()

    def test_authorized_group_message_requires_mention_by_default(self, monkeypatch):
        monkeypatch.setenv("RC_ALLOWED_USER_EMAILS", "alice@example.com")
        adapter = self._allowed_group_adapter()

        asyncio.run(
            adapter._handle_ws_event(self._plain_group_body(), identity="bot")
        )

        adapter.handle_message.assert_not_awaited()

    def test_require_mention_false_allows_plain_group_message(self, monkeypatch):
        monkeypatch.setenv("RC_ALLOWED_USER_EMAILS", "alice@example.com")
        monkeypatch.setenv("RC_REQUIRE_MENTION", "false")
        adapter = self._allowed_group_adapter()

        asyncio.run(
            adapter._handle_ws_event(self._plain_group_body(), identity="bot")
        )

        adapter.handle_message.assert_awaited_once()

    def test_free_response_channel_allows_plain_group_message(self, monkeypatch):
        monkeypatch.setenv("RC_ALLOWED_USER_EMAILS", "alice@example.com")
        monkeypatch.setenv("RC_FREE_RESPONSE_CHANNELS", "g-1")
        adapter = self._allowed_group_adapter()

        asyncio.run(
            adapter._handle_ws_event(self._plain_group_body(), identity="bot")
        )

        adapter.handle_message.assert_awaited_once()

    def test_non_free_response_channel_still_requires_mention(self, monkeypatch):
        monkeypatch.setenv("RC_ALLOWED_USER_EMAILS", "alice@example.com")
        monkeypatch.setenv("RC_FREE_RESPONSE_CHANNELS", "g-2")
        adapter = self._allowed_group_adapter()

        asyncio.run(
            adapter._handle_ws_event(self._plain_group_body(), identity="bot")
        )

        adapter.handle_message.assert_not_awaited()

    def test_ignored_channel_beats_free_response_channel(self, monkeypatch):
        monkeypatch.setenv("RC_ALLOWED_USER_EMAILS", "alice@example.com")
        monkeypatch.setenv("RC_FREE_RESPONSE_CHANNELS", "g-1")
        monkeypatch.setenv("RC_IGNORED_CHANNELS", "g-1")
        adapter = self._allowed_group_adapter()

        asyncio.run(
            adapter._handle_ws_event(self._plain_group_body(), identity="bot")
        )

        adapter.handle_message.assert_not_awaited()

    def test_participated_thread_followup_dispatches_without_mention(self, monkeypatch):
        monkeypatch.setenv("RC_ALLOWED_USER_EMAILS", "alice@example.com")
        adapter = _make_adapter()
        client = MagicMock()
        client.list_chats = AsyncMock(return_value=[
            {"id": "g-1", "type": "Team", "name": "Project"},
        ])
        client.get_person = AsyncMock(return_value={
            "firstName": "Alice",
            "email": "alice@example.com",
        })
        adapter._client = client
        adapter._own_person_id = "bot-1"
        adapter._threads = {"t-1"}
        adapter.handle_message = AsyncMock()
        body = {
            "eventType": "PostAdded",
            "id": "p-child",
            "chatId": "g-1",
            "creatorId": "user-2",
            "text": "follow-up in thread",
            "parentPostId": "p-parent",
            "threadId": "t-1",
        }

        asyncio.run(adapter._handle_ws_event(body, identity="bot"))

        adapter.handle_message.assert_awaited_once()
        event = adapter.handle_message.await_args.args[0]
        assert event.source.thread_id == "t-1"
        assert event.message_id == "p-child"
        assert event.reply_to_message_id == "p-parent"

    def test_thread_require_mention_blocks_unmentioned_followup(self, monkeypatch):
        monkeypatch.setenv("RC_ALLOWED_USER_EMAILS", "alice@example.com")
        monkeypatch.setenv("RC_THREAD_REQUIRE_MENTION", "true")
        adapter = _make_adapter()
        client = MagicMock()
        client.list_chats = AsyncMock(return_value=[
            {"id": "g-1", "type": "Team", "name": "Project"},
        ])
        client.get_person = AsyncMock(return_value={
            "firstName": "Alice",
            "email": "alice@example.com",
        })
        adapter._client = client
        adapter._own_person_id = "bot-1"
        adapter._threads = {"t-1"}
        adapter.handle_message = AsyncMock()
        body = {
            "eventType": "PostAdded",
            "id": "p-child",
            "chatId": "g-1",
            "creatorId": "user-2",
            "text": "follow-up in thread",
            "parentPostId": "p-parent",
            "threadId": "t-1",
        }

        asyncio.run(adapter._handle_ws_event(body, identity="bot"))

        adapter.handle_message.assert_not_awaited()

    def test_participated_thread_id_only_dispatches_without_mention(self, monkeypatch):
        monkeypatch.setenv("RC_ALLOWED_USER_EMAILS", "alice@example.com")
        adapter = _make_adapter()
        client = MagicMock()
        client.list_chats = AsyncMock(return_value=[
            {"id": "g-1", "type": "Team", "name": "Project"},
        ])
        client.get_person = AsyncMock(return_value={
            "firstName": "Alice",
            "email": "alice@example.com",
        })
        adapter._client = client
        adapter._own_person_id = "bot-1"
        adapter._threads = {"t-1"}
        adapter.handle_message = AsyncMock()
        body = {
            "eventType": "PostAdded",
            "id": "p-child",
            "chatId": "g-1",
            "creatorId": "user-2",
            "text": "follow-up in thread",
            "threadId": "t-1",
        }

        asyncio.run(adapter._handle_ws_event(body, identity="bot"))

        adapter.handle_message.assert_awaited_once()
        event = adapter.handle_message.await_args.args[0]
        assert event.source.thread_id == "t-1"
        assert event.message_id == "p-child"
        assert event.reply_to_message_id is None

    def test_participated_parent_only_dispatches_thread_followup(self, monkeypatch):
        monkeypatch.setenv("RC_ALLOWED_USER_EMAILS", "alice@example.com")
        adapter = _make_adapter()
        client = MagicMock()
        client.list_chats = AsyncMock(return_value=[
            {"id": "g-1", "type": "Team", "name": "Project"},
        ])
        client.get_person = AsyncMock(return_value={
            "firstName": "Alice",
            "email": "alice@example.com",
        })
        adapter._client = client
        adapter._own_person_id = "bot-1"
        adapter._threads = {"p-parent"}
        adapter.handle_message = AsyncMock()
        body = {
            "eventType": "PostAdded",
            "id": "p-child",
            "chatId": "g-1",
            "creatorId": "user-2",
            "text": "follow-up in thread",
            "parentPostId": "p-parent",
        }

        asyncio.run(adapter._handle_ws_event(body, identity="bot"))

        adapter.handle_message.assert_awaited_once()
        event = adapter.handle_message.await_args.args[0]
        assert event.source.thread_id == "parentPostId:p-parent"
        assert event.message_id == "p-child"
        assert event.reply_to_message_id == "p-parent"

    def test_participated_parent_anchor_dispatches_thread_followup(self, monkeypatch):
        monkeypatch.setenv("RC_ALLOWED_USER_EMAILS", "alice@example.com")
        adapter = _make_adapter()
        client = MagicMock()
        client.list_chats = AsyncMock(return_value=[
            {"id": "g-1", "type": "Team", "name": "Project"},
        ])
        client.get_person = AsyncMock(return_value={
            "firstName": "Alice",
            "email": "alice@example.com",
        })
        adapter._client = client
        adapter._own_person_id = "bot-1"
        adapter._threads = {"p-parent"}
        adapter.handle_message = AsyncMock()
        body = {
            "eventType": "PostAdded",
            "id": "p-child",
            "chatId": "g-1",
            "creatorId": "user-2",
            "text": "follow-up in thread",
            "parentPostId": "p-parent",
            "threadId": "t-1",
        }

        asyncio.run(adapter._handle_ws_event(body, identity="bot"))

        adapter.handle_message.assert_awaited_once()
        assert "t-1" in adapter._threads

    def test_untracked_thread_followup_is_ignored_without_mention(self, monkeypatch):
        monkeypatch.setenv("RC_ALLOWED_USER_EMAILS", "alice@example.com")
        adapter = _make_adapter()
        client = MagicMock()
        client.list_chats = AsyncMock(return_value=[
            {"id": "g-1", "type": "Team", "name": "Project"},
        ])
        client.get_person = AsyncMock(return_value={
            "firstName": "Alice",
            "email": "alice@example.com",
        })
        adapter._client = client
        adapter._own_person_id = "bot-1"
        adapter._threads = set()
        adapter.handle_message = AsyncMock()
        body = {
            "eventType": "PostAdded",
            "id": "p-child",
            "chatId": "g-1",
            "creatorId": "user-2",
            "text": "follow-up in unknown thread",
            "parentPostId": "p-parent",
            "threadId": "t-1",
        }

        asyncio.run(adapter._handle_ws_event(body, identity="bot"))

        adapter.handle_message.assert_not_awaited()

    def test_non_owner_group_message_with_chat_id_is_observed(self, monkeypatch):
        monkeypatch.setenv("RC_ALLOWED_USER_EMAILS", "owner@example.com")
        monkeypatch.delenv("RC_ALLOW_ALL_USERS", raising=False)
        adapter = self._owner_adapter()
        store = MagicMock()
        store.get_or_create_session.return_value = MagicMock(session_id="s-1")
        adapter._session_store = store
        body = {
            "eventType": "PostAdded",
            "id": "p-chat-id",
            "chatId": "g-1",
            "creatorId": "user-2",
            "text": "deployment is still red",
        }

        asyncio.run(adapter._handle_ws_event(body, identity="owner"))

        adapter.handle_message.assert_not_awaited()
        appended = store.append_to_transcript.call_args.args[1]
        assert appended["observed"] is True
        assert "deployment is still red" in appended["content"]

    def test_owner_slash_message_in_group_is_ignored_without_mention(self, monkeypatch):
        monkeypatch.setenv("RC_ALLOWED_USER_EMAILS", "owner@example.com")
        adapter = self._owner_adapter()
        body = {
            "eventType": "PostAdded",
            "id": "p-2",
            "groupId": "g-1",
            "creatorId": "owner-1",
            "text": "/status",
        }

        asyncio.run(adapter._handle_ws_event(body, identity="owner"))

        adapter.handle_message.assert_not_awaited()

    def test_owner_mentioned_group_message_includes_observed_context(self, monkeypatch):
        monkeypatch.setenv("RC_ALLOWED_USER_EMAILS", "owner@example.com")
        adapter = self._owner_adapter()
        store = MagicMock()
        store.get_or_create_session.return_value = MagicMock(session_id="s-1")
        store.load_transcript.return_value = [
            {
                "role": "user",
                "content": "[Alice|user-2]\ndeployment is red",
                "observed": True,
            }
        ]
        adapter._session_store = store
        body = {
            "eventType": "PostAdded",
            "id": "p-2",
            "groupId": "g-1",
            "creatorId": "owner-1",
            "text": "![:Person](bot-1) /status",
        }

        asyncio.run(adapter._handle_ws_event(body, identity="owner"))

        event = adapter.handle_message.await_args.args[0]
        assert "Observed RingCentral group context" in event.text
        assert "deployment is red" in event.text
        assert event.text.endswith("/status")
        assert "RingCentral group chat message from the owner" in event.channel_prompt

    def test_owner_visible_non_bot_dm_from_owner_is_ignored(self):
        adapter = _make_adapter()
        client = MagicMock()
        client.get_chat = AsyncMock(return_value={
            "id": "dm-other",
            "type": "Direct",
            "members": [{"id": "owner-1"}, {"id": "user-2"}],
        })
        client.list_chats = AsyncMock(return_value=[])
        adapter._client = client
        adapter._owner_client = client
        adapter._own_person_id = "bot-1"
        adapter._owner_person_id = "owner-1"
        adapter._owner_only_gate_enabled = True
        adapter.handle_message = AsyncMock()
        adapter._send_chunks = AsyncMock(return_value=MagicMock(success=True))
        body = {
            "eventType": "PostAdded",
            "id": "p-dm",
            "chatId": "dm-other",
            "creatorId": "owner-1",
            "text": "hello alice",
        }

        asyncio.run(adapter._handle_ws_event(body, identity="owner"))

        adapter.handle_message.assert_not_awaited()
        adapter._send_chunks.assert_not_awaited()

    def test_owner_visible_bot_dm_from_owner_is_allowed(self):
        adapter = _make_adapter()
        client = MagicMock()
        client.get_chat = AsyncMock(return_value={
            "id": "dm-bot",
            "type": "Direct",
            "members": [{"id": "owner-1"}, {"id": "bot-1"}],
        })
        client.list_chats = AsyncMock(return_value=[])
        client.get_person = AsyncMock(return_value={
            "firstName": "Owner",
            "email": "owner@example.com",
        })
        adapter._client = client
        adapter._owner_client = client
        adapter._own_person_id = "bot-1"
        adapter._owner_person_id = "owner-1"
        adapter._owner_email = "owner@example.com"
        adapter._owner_only_gate_enabled = True
        adapter.handle_message = AsyncMock()
        body = {
            "eventType": "PostAdded",
            "id": "p-dm",
            "chatId": "dm-bot",
            "creatorId": "owner-1",
            "text": "hello bot",
        }

        asyncio.run(adapter._handle_ws_event(body, identity="owner"))

        adapter.handle_message.assert_awaited_once()
        event = adapter.handle_message.await_args.args[0]
        assert event.source.user_id == "owner@example.com"
        assert event.source.user_id_alt == "owner-1"

    def test_bot_dm_from_allowed_email_is_dispatched(self, monkeypatch):
        monkeypatch.setenv("RC_ALLOWED_USER_EMAILS", "alice@example.com")
        adapter = _make_adapter()
        client = MagicMock()
        client.get_chat = AsyncMock(return_value={
            "id": "dm-bot",
            "type": "Direct",
            "members": [{"id": "user-2"}, {"id": "bot-1"}],
        })
        client.list_chats = AsyncMock(return_value=[])
        client.get_person = AsyncMock(return_value={
            "firstName": "Alice",
            "email": "alice@example.com",
        })
        adapter._client = client
        adapter._own_person_id = "bot-1"
        adapter.handle_message = AsyncMock()
        body = {
            "eventType": "PostAdded",
            "id": "p-dm",
            "chatId": "dm-bot",
            "creatorId": "user-2",
            "text": "hello bot",
        }

        asyncio.run(adapter._handle_ws_event(body, identity="bot"))

        adapter.handle_message.assert_awaited_once()
        event = adapter.handle_message.await_args.args[0]
        assert event.source.user_id == "alice@example.com"
        assert event.source.user_id_alt == "user-2"

    def test_bot_dm_ignores_group_channel_allowlist(self, monkeypatch):
        monkeypatch.setenv("RC_ALLOWED_USER_EMAILS", "alice@example.com")
        monkeypatch.setenv("RC_ALLOWED_CHANNELS", "g-allowed")
        adapter = _make_adapter()
        client = MagicMock()
        client.get_chat = AsyncMock(return_value={
            "id": "dm-bot",
            "type": "Direct",
            "members": [{"id": "user-2"}, {"id": "bot-1"}],
        })
        client.list_chats = AsyncMock(return_value=[])
        client.get_person = AsyncMock(return_value={
            "firstName": "Alice",
            "email": "alice@example.com",
        })
        adapter._client = client
        adapter._own_person_id = "bot-1"
        adapter.handle_message = AsyncMock()
        body = {
            "eventType": "PostAdded",
            "id": "p-dm",
            "chatId": "dm-bot",
            "creatorId": "user-2",
            "text": "hello bot",
        }

        asyncio.run(adapter._handle_ws_event(body, identity="bot"))

        adapter.handle_message.assert_awaited_once()

    def test_bot_dm_from_unauthorized_email_is_silent(self, monkeypatch):
        monkeypatch.delenv("RC_ALLOWED_USER_EMAILS", raising=False)
        monkeypatch.delenv("RC_ALLOW_ALL_USERS", raising=False)
        adapter = _make_adapter()
        client = MagicMock()
        client.get_chat = AsyncMock(return_value={
            "id": "dm-bot",
            "type": "Direct",
            "members": [{"id": "user-2"}, {"id": "bot-1"}],
        })
        client.list_chats = AsyncMock(return_value=[])
        client.get_person = AsyncMock(return_value={
            "firstName": "Alice",
            "email": "alice@example.com",
        })
        adapter._client = client
        adapter._own_person_id = "bot-1"
        adapter.handle_message = AsyncMock()
        adapter._send_chunks = AsyncMock(return_value=MagicMock(success=True))
        body = {
            "eventType": "PostAdded",
            "id": "p-dm",
            "chatId": "dm-bot",
            "creatorId": "user-2",
            "text": "hello bot",
        }

        asyncio.run(adapter._handle_ws_event(body, identity="bot"))

        adapter.handle_message.assert_not_awaited()
        adapter._send_chunks.assert_not_awaited()


class TestChatInfo:
    def test_get_chat_info_prefers_get_chat_direct_type(self):
        adapter = _make_adapter()
        client = MagicMock()
        client.get_chat = AsyncMock(return_value={
            "id": "555555555555",
            "type": "Direct",
            "name": None,
        })
        client.list_chats = AsyncMock(return_value=[])
        adapter._client = client

        result = asyncio.run(adapter._get_chat_info("555555555555"))

        assert result["type"] == "dm"
        assert result["chat_id"] == "555555555555"


class TestRingCentralHistoryTool:
    @staticmethod
    def _set_owner_env(monkeypatch):
        monkeypatch.setenv("RC_BOT_TOKEN", "bot-token")
        monkeypatch.setenv("RC_USER_CLIENT_ID", "owner-client")
        monkeypatch.setenv("RC_USER_CLIENT_SECRET", "owner-secret")
        monkeypatch.setenv("RC_USER_JWT_TOKEN", "owner-jwt")
        monkeypatch.setenv("RC_HISTORY_MESSAGE_LIMIT", "250")

    @staticmethod
    def _profile(person_id: str) -> dict:
        profiles = {
            "owner-1": {
                "firstName": "Owner",
                "email": "owner@example.com",
            },
            "bot-1": {
                "firstName": "Hermes",
                "email": "bot@example.com",
            },
            "user-2": {
                "firstName": "Alice",
                "lastName": "Wang",
                "email": "alice@example.com",
            },
        }
        return profiles.get(str(person_id), {"firstName": "Unknown"})

    @classmethod
    def _clients(cls):
        bot = MagicMock()
        bot.get_own_extension = AsyncMock(return_value={
            "id": "bot-1",
            "contact": {"firstName": "Hermes"},
        })
        bot.get_person = AsyncMock(side_effect=cls._profile)
        bot.close = AsyncMock()

        async def bot_get_chat(chat_id):
            if chat_id == "dm-1":
                return {
                    "id": "dm-1",
                    "type": "Direct",
                    "name": "Owner DM",
                    "members": [{"id": "owner-1"}, {"id": "bot-1"}],
                }
            if chat_id == "g-1":
                return {
                    "id": "g-1",
                    "type": "Team",
                    "name": "Project Team",
                }
            return None

        bot.get_chat = AsyncMock(side_effect=bot_get_chat)
        bot.list_chats = AsyncMock(return_value=[
            {
                "id": "dm-1",
                "type": "Direct",
                "name": "Owner DM",
                "members": [{"id": "owner-1"}, {"id": "bot-1"}],
            },
            {"id": "g-1", "type": "Team", "name": "Project Team"},
        ])

        owner = MagicMock()
        owner.owner_id = "owner-1"
        owner.get_own_extension = AsyncMock(return_value={
            "id": "owner-1",
            "contact": {
                "firstName": "Owner",
                "email": "owner@example.com",
            },
        })
        owner.get_person = AsyncMock(side_effect=cls._profile)
        owner.close = AsyncMock()
        owner.list_recent_chats = AsyncMock(return_value=[
            {"id": "g-1", "type": "Team", "name": "Project Team"},
        ])
        owner.list_chats = AsyncMock(return_value=[
            {"id": "g-1", "type": "Team", "name": "Project Team"},
        ])
        owner.get_chat = AsyncMock(side_effect=lambda cid: {
            "id": cid,
            "type": "Team",
            "name": "Project Team",
        } if cid in {"g-1", "555555555555"} else None)
        owner.list_posts = AsyncMock(return_value=[
            {
                "id": "p-new",
                "creatorId": "user-2",
                "text": "deployment finished",
                "creationTime": "2026-05-28T02:20:00Z",
            },
            {
                "id": "p-old",
                "creatorId": "owner-1",
                "text": "please deploy",
                "creationTime": "2026-05-28T02:10:00Z",
            },
        ])
        owner.list_legacy_group_posts = AsyncMock(return_value=[])
        owner.search_directory = AsyncMock(return_value=[])
        owner.create_or_find_dm = AsyncMock(return_value=None)
        return bot, owner

    @staticmethod
    def _call_tool(
        args: dict,
        *,
        platform: str = "ringcentral",
        chat_id: str = "dm-1",
        user_id: str = "owner@example.com",
    ) -> dict:
        from gateway.session_context import set_session_vars

        tokens = set_session_vars(
            platform=platform,
            chat_id=chat_id,
            user_id=user_id,
            user_name="Owner",
        )
        try:
            return json.loads(asyncio.run(_ringcentral_get_recent_messages(args)))
        finally:
            for token in reversed(tokens):
                token.var.reset(token)

    def _call_tool_with_clients(self, monkeypatch, bot, owner, args, **session):
        self._set_owner_env(monkeypatch)
        with patch.object(_rc_mod, "RingCentralClient", return_value=bot) as client_cls:
            client_cls.from_jwt.return_value = owner
            return self._call_tool(args, **session)

    @staticmethod
    def _call_calendar_tool(
        handler,
        args: dict,
        *,
        platform: str = "ringcentral",
        chat_id: str = "g-1",
        user_id: str = "owner@example.com",
    ) -> dict:
        from gateway.session_context import set_session_vars

        tokens = set_session_vars(
            platform=platform,
            chat_id=chat_id,
            user_id=user_id,
            user_name="Owner",
        )
        try:
            return json.loads(asyncio.run(handler(args)))
        finally:
            for token in reversed(tokens):
                token.var.reset(token)

    @staticmethod
    def _call_artifact_tool(
        handler,
        args: dict,
        *,
        platform: str = "ringcentral",
        chat_id: str = "g-1",
    ) -> dict:
        from gateway.session_context import set_session_vars

        tokens = set_session_vars(
            platform=platform,
            chat_id=chat_id,
            user_id="owner@example.com",
            user_name="Owner",
        )
        try:
            return json.loads(asyncio.run(handler(args)))
        finally:
            for token in reversed(tokens):
                token.var.reset(token)

    def test_history_tool_availability_requires_bot_and_owner_env(self, monkeypatch):
        monkeypatch.delenv("RC_BOT_TOKEN", raising=False)
        monkeypatch.delenv("RC_USER_CLIENT_ID", raising=False)
        monkeypatch.delenv("RC_USER_CLIENT_SECRET", raising=False)
        monkeypatch.delenv("RC_USER_JWT_TOKEN", raising=False)

        assert _ringcentral_history_tool_available() is False

        self._set_owner_env(monkeypatch)
        assert _ringcentral_history_tool_available() is True

    def test_calendar_event_tool_availability_requires_owner_env(self, monkeypatch):
        monkeypatch.delenv("RC_USER_CLIENT_ID", raising=False)
        monkeypatch.delenv("RC_USER_CLIENT_SECRET", raising=False)
        monkeypatch.delenv("RC_USER_JWT_TOKEN", raising=False)

        assert _ringcentral_owner_tool_available() is False

        self._set_owner_env(monkeypatch)
        assert _ringcentral_owner_tool_available() is True

    def test_adaptive_card_tool_availability_requires_bot_token(self, monkeypatch):
        monkeypatch.delenv("RC_BOT_TOKEN", raising=False)
        monkeypatch.delenv("RC_USER_CLIENT_ID", raising=False)
        monkeypatch.delenv("RC_USER_CLIENT_SECRET", raising=False)
        monkeypatch.delenv("RC_USER_JWT_TOKEN", raising=False)

        assert _ringcentral_bot_tool_available() is False

        monkeypatch.setenv("RC_BOT_TOKEN", "bot-token")
        assert _ringcentral_bot_tool_available() is True

    def test_adaptive_card_tools_use_current_ringcentral_session(self, monkeypatch):
        monkeypatch.setenv("RC_BOT_TOKEN", "bot-token")
        bot = MagicMock()
        bot.create_adaptive_card = AsyncMock(return_value={"id": "card-1", "type": "AdaptiveCard"})
        bot.get_adaptive_card = AsyncMock(return_value={"id": "card-1", "type": "AdaptiveCard"})
        bot.update_adaptive_card = AsyncMock(return_value={"id": "card-1", "type": "AdaptiveCard"})
        bot.delete_adaptive_card = AsyncMock(return_value=True)
        bot.close = AsyncMock()

        with patch.object(_rc_mod, "RingCentralClient", return_value=bot):
            created = self._call_artifact_tool(
                _ringcentral_create_adaptive_card,
                {"text": "hello"},
            )
            read = self._call_artifact_tool(_ringcentral_get_adaptive_card, {"card_id": "card-1"})
            updated = self._call_artifact_tool(
                _ringcentral_update_adaptive_card,
                {"card_id": "card-1", "text": "updated"},
            )
            deleted = self._call_artifact_tool(_ringcentral_delete_adaptive_card, {"card_id": "card-1"})

        assert created == {"success": True, "card_id": "card-1", "type": "AdaptiveCard"}
        assert read == {"success": True, "card": {"id": "card-1", "type": "AdaptiveCard"}}
        assert updated == {"success": True, "card_id": "card-1", "type": "AdaptiveCard"}
        assert deleted == {"success": True, "deleted": True}
        bot.create_adaptive_card.assert_awaited_once_with("g-1", {
            "version": "1.3",
            "body": [{"type": "TextBlock", "text": "hello", "wrap": True}],
            "type": "AdaptiveCard",
        })
        bot.get_adaptive_card.assert_awaited_once_with("card-1")
        bot.update_adaptive_card.assert_awaited_once_with("card-1", {
            "version": "1.3",
            "body": [{"type": "TextBlock", "text": "updated", "wrap": True}],
            "type": "AdaptiveCard",
        })
        bot.delete_adaptive_card.assert_awaited_once_with("card-1")
        assert bot.close.await_count == 4

    def test_adaptive_card_tool_rejects_non_ringcentral_session(self, monkeypatch):
        monkeypatch.setenv("RC_BOT_TOKEN", "bot-token")

        result = self._call_artifact_tool(
            _ringcentral_create_adaptive_card,
            {"text": "hello"},
            platform="discord",
        )

        assert result["success"] is False
        assert "RingCentral session" in result["error"]

    def test_adaptive_card_tool_rejects_cross_chat_target(self, monkeypatch):
        monkeypatch.setenv("RC_BOT_TOKEN", "bot-token")

        result = self._call_artifact_tool(
            _ringcentral_create_adaptive_card,
            {"chat_id": "g-2", "text": "hello"},
        )

        assert result["success"] is False
        assert "current session chat" in result["error"]

    def test_note_tools_use_owner_current_session(self, monkeypatch):
        self._set_owner_env(monkeypatch)
        monkeypatch.setenv("RC_HOME_CHANNEL", "home-dm")
        _rc_mod._PENDING_ARTIFACT_ACTIONS.clear()
        owner = MagicMock()
        owner.owner_id = "owner-1"
        owner.get_own_extension = AsyncMock(return_value={
            "id": "owner-1",
            "contact": {"email": "owner@example.com"},
        })
        owner.close = AsyncMock()
        owner.list_notes = AsyncMock(return_value=[{"id": "n1", "title": "Note", "status": "Draft"}])
        owner.create_note = AsyncMock(return_value={"id": "n1", "title": "Note", "status": "Draft"})
        owner.get_note = AsyncMock(return_value={"id": "n1", "title": "Note", "body": "<b>Body</b>"})
        owner.update_note = AsyncMock(return_value={"id": "n1", "title": "Updated", "status": "Draft"})
        owner.delete_note = AsyncMock(return_value=True)
        owner.publish_note = AsyncMock(return_value=True)

        with patch.object(_rc_mod.RingCentralClient, "from_jwt", return_value=owner):
            listed = self._call_calendar_tool(_ringcentral_list_notes, {"record_count": 10})
            pending_create = self._call_calendar_tool(
                _ringcentral_create_note,
                {"title": "Note", "body": "<b>Body</b>", "publish": True},
            )
            read = self._call_calendar_tool(_ringcentral_get_note, {"note_id": "n1"})
            pending_update = self._call_calendar_tool(_ringcentral_update_note, {"note_id": "n1", "title": "Updated"})
            pending_publish = self._call_calendar_tool(_ringcentral_publish_note, {"note_id": "n1"})
            pending_delete = self._call_calendar_tool(_ringcentral_delete_note, {"note_id": "n1"})
            rejected_confirm = self._call_calendar_tool(
                _ringcentral_confirm_artifact_action,
                {"confirmation_id": pending_create["confirmation_id"]},
            )
            confirmed = self._call_calendar_tool(
                _ringcentral_confirm_artifact_action,
                {"confirmation_id": pending_create["confirmation_id"]},
                chat_id="home-dm",
            )
            reused = self._call_calendar_tool(
                _ringcentral_confirm_artifact_action,
                {"confirmation_id": pending_create["confirmation_id"]},
                chat_id="home-dm",
            )

        assert listed["success"] is True
        assert listed["notes"] == [{"id": "n1", "title": "Note", "status": "Draft"}]
        assert pending_create["requires_confirmation"] is True
        assert pending_create["target_chat_id"] == "g-1"
        assert read["note"]["body"] == "<b>Body</b>"
        assert pending_update["requires_confirmation"] is True
        assert pending_publish["requires_confirmation"] is True
        assert pending_delete["requires_confirmation"] is True
        assert rejected_confirm["success"] is False
        assert "Home DM" in rejected_confirm["error"]
        assert confirmed == {
            "success": True,
            "note_id": "n1",
            "published": True,
            "note": {"id": "n1", "title": "Note", "status": "Draft"},
            "confirmed": True,
            "summary": "Create note: Note",
        }
        assert reused["success"] is False
        assert "Invalid or expired" in reused["error"]
        owner.list_notes.assert_awaited_once_with("g-1", 10)
        owner.create_note.assert_awaited_once_with("g-1", {"title": "Note", "body": "<b>Body</b>"})
        owner.publish_note.assert_any_await("n1")
        owner.update_note.assert_not_awaited()
        owner.delete_note.assert_not_awaited()
        assert owner.close.await_count == 9

    def test_note_tool_rejects_non_owner_session_user(self, monkeypatch):
        self._set_owner_env(monkeypatch)
        owner = MagicMock()
        owner.owner_id = "owner-1"
        owner.get_own_extension = AsyncMock(return_value={
            "id": "owner-1",
            "contact": {"email": "owner@example.com"},
        })
        owner.close = AsyncMock()

        with patch.object(_rc_mod.RingCentralClient, "from_jwt", return_value=owner):
            result = self._call_calendar_tool(
                _ringcentral_create_note,
                {"title": "Note"},
                user_id="alice@example.com",
            )

        assert result["success"] is False
        assert "owner" in result["error"]
        owner.create_note.assert_not_called()

    def test_owner_write_requires_home_channel(self, monkeypatch):
        self._set_owner_env(monkeypatch)
        monkeypatch.delenv("RC_HOME_CHANNEL", raising=False)
        owner = MagicMock()
        owner.owner_id = "owner-1"
        owner.get_own_extension = AsyncMock(return_value={
            "id": "owner-1",
            "contact": {"email": "owner@example.com"},
        })
        owner.close = AsyncMock()
        owner.create_note = AsyncMock()

        with patch.object(_rc_mod.RingCentralClient, "from_jwt", return_value=owner):
            result = self._call_calendar_tool(
                _ringcentral_create_note,
                {"title": "Note"},
            )

        assert result["success"] is False
        assert "RC_HOME_CHANNEL" in result["error"]
        owner.create_note.assert_not_called()

    def test_calendar_event_tools_use_owner_current_session(self, monkeypatch):
        self._set_owner_env(monkeypatch)
        monkeypatch.setenv("RC_HOME_CHANNEL", "home-dm")
        _rc_mod._PENDING_ARTIFACT_ACTIONS.clear()
        owner = MagicMock()
        owner.get_own_extension = AsyncMock(return_value={
            "id": "owner-1",
            "contact": {"email": "owner@example.com"},
        })
        owner.close = AsyncMock()
        owner.list_events = AsyncMock(return_value=[{
            "id": "event-1",
            "title": "Planning",
            "startTime": "2026-06-04T10:00:00Z",
            "endTime": "2026-06-04T11:00:00Z",
        }])
        owner.create_event = AsyncMock(return_value={
            "id": "event-1",
            "title": "Planning",
            "startTime": "2026-06-04T10:00:00Z",
            "endTime": "2026-06-04T11:00:00Z",
        })
        owner.get_event = AsyncMock(return_value={"id": "event-1", "title": "Planning"})
        owner.update_event = AsyncMock(return_value={
            "id": "event-1",
            "title": "Updated",
            "startTime": "2026-06-04T12:00:00Z",
            "endTime": "2026-06-04T13:00:00Z",
        })
        owner.delete_event = AsyncMock(return_value=True)

        with patch.object(_rc_mod.RingCentralClient, "from_jwt", return_value=owner):
            listed = self._call_calendar_tool(_ringcentral_list_calendar_events, {"record_count": 10})
            pending_create = self._call_calendar_tool(
                _ringcentral_create_calendar_event,
                {
                    "title": "Planning",
                    "start_time": "2026-06-04T10:00:00Z",
                    "end_time": "2026-06-04T11:00:00Z",
                    "description": "Agenda",
                },
            )
            read = self._call_calendar_tool(_ringcentral_get_calendar_event, {"event_id": "event-1"})
            pending_update = self._call_calendar_tool(
                _ringcentral_update_calendar_event,
                {
                    "event_id": "event-1",
                    "title": "Updated",
                    "start_time": "2026-06-04T12:00:00Z",
                    "end_time": "2026-06-04T13:00:00Z",
                },
            )
            pending_delete = self._call_calendar_tool(_ringcentral_delete_calendar_event, {"event_id": "event-1"})
            confirmed = self._call_calendar_tool(
                _ringcentral_confirm_artifact_action,
                {"confirmation_id": pending_create["confirmation_id"]},
                chat_id="home-dm",
            )

        assert listed["events"] == [{
            "id": "event-1",
            "title": "Planning",
            "start_time": "2026-06-04T10:00:00Z",
            "end_time": "2026-06-04T11:00:00Z",
        }]
        assert pending_create["requires_confirmation"] is True
        assert pending_create["target_chat_id"] == "g-1"
        assert read["event"]["id"] == "event-1"
        assert pending_update["requires_confirmation"] is True
        assert pending_delete["requires_confirmation"] is True
        assert confirmed["confirmed"] is True
        assert confirmed["event_id"] == "event-1"
        owner.list_events.assert_awaited_once_with("g-1", 10)
        owner.create_event.assert_awaited_once_with("g-1", {
            "title": "Planning",
            "startTime": "2026-06-04T10:00:00Z",
            "endTime": "2026-06-04T11:00:00Z",
            "description": "Agenda",
        })
        owner.get_event.assert_awaited_once_with("event-1")
        owner.update_event.assert_not_awaited()
        owner.delete_event.assert_not_awaited()
        assert owner.close.await_count == 6

    def test_calendar_event_tool_rejects_non_owner_session_user(self, monkeypatch):
        self._set_owner_env(monkeypatch)
        owner = MagicMock()
        owner.get_own_extension = AsyncMock(return_value={
            "id": "owner-1",
            "contact": {"email": "owner@example.com"},
        })
        owner.close = AsyncMock()
        owner.create_event = AsyncMock()

        with patch.object(_rc_mod.RingCentralClient, "from_jwt", return_value=owner):
            result = self._call_calendar_tool(
                _ringcentral_create_calendar_event,
                {
                    "title": "Planning",
                    "start_time": "2026-06-04T10:00:00Z",
                    "end_time": "2026-06-04T11:00:00Z",
                },
                user_id="alice@example.com",
            )

        assert result["success"] is False
        assert "Only the configured RingCentral owner" in result["error"]
        owner.create_event.assert_not_called()

    def test_owner_dm_tool_fetches_group_history(self, monkeypatch):
        bot, owner = self._clients()

        result = self._call_tool_with_clients(
            monkeypatch,
            bot,
            owner,
            {"target": "Project Team"},
        )

        assert result["success"] is True
        assert result["target_chat"] == {
            "id": "g-1",
            "name": "Project Team",
            "type": "group",
            "person_id": "",
        }
        assert result["post_source"] == "team_messaging"
        assert result["included_count"] == 2
        assert [msg["id"] for msg in result["messages"]] == ["p-old", "p-new"]
        assert result["messages"][0]["sender"] == "Owner (owner-1)"
        assert result["messages"][1]["sender"] == "Alice (user-2)"
        owner.list_posts.assert_awaited_once_with("g-1", record_count=250)
        owner.list_legacy_group_posts.assert_not_awaited()
        bot.close.assert_awaited_once()
        owner.close.assert_awaited_once()

    def test_owner_dm_tool_resolves_typed_team_mention_to_group_history(self, monkeypatch):
        bot, owner = self._clients()

        result = self._call_tool_with_clients(
            monkeypatch,
            bot,
            owner,
            {"target": "![:Team](555555555555)"},
        )

        assert result["success"] is True
        assert result["target_chat"] == {
            "id": "555555555555",
            "name": "Project Team",
            "type": "group",
            "person_id": "",
        }
        owner.list_posts.assert_awaited_once_with("555555555555", record_count=250)
        owner.list_recent_chats.assert_not_awaited()

    def test_owner_dm_tool_resolves_directory_person_to_direct_history(self, monkeypatch):
        bot, owner = self._clients()
        owner.search_directory = AsyncMock(return_value=[{
            "id": "user-2",
            "firstName": "Alice",
            "lastName": "Wang",
            "email": "alice@example.com",
        }])
        owner.create_or_find_dm = AsyncMock(return_value={
            "id": "dm-alice",
            "type": "Direct",
        })

        result = self._call_tool_with_clients(
            monkeypatch,
            bot,
            owner,
            {"target": "Alice Wang", "target_type": "person", "record_count": 10},
        )

        assert result["success"] is True
        assert result["target_chat"]["id"] == "dm-alice"
        assert result["target_chat"]["type"] == "dm"
        assert result["post_source"] == "team_messaging"
        owner.search_directory.assert_awaited()
        owner.create_or_find_dm.assert_awaited_once_with(["user-2"])
        owner.list_posts.assert_awaited_once_with("dm-alice", record_count=10)
        owner.list_legacy_group_posts.assert_not_awaited()

    def test_owner_dm_tool_uses_legacy_text_for_integration_posts(self, monkeypatch):
        bot, owner = self._clients()
        owner.list_posts = AsyncMock(return_value=[
            {
                "id": "p-webhook",
                "type": "TextMessage",
                "creatorId": "",
                "text": "",
                "creationTime": "2026-05-28T02:20:00Z",
            },
            {
                "id": "p-user",
                "type": "TextMessage",
                "creatorId": "user-2",
                "text": "manual update",
                "creationTime": "2026-05-28T02:10:00Z",
            },
        ])
        owner.list_legacy_group_posts = AsyncMock(return_value=[
            {
                "id": "p-webhook",
                "groupId": "g-1",
                "type": "TextMessage",
                "creatorId": "",
                "activity": "NewsBot",
                "text": "integration update",
                "creationTime": "2026-05-28T02:20:00Z",
            },
        ])

        result = self._call_tool_with_clients(
            monkeypatch,
            bot,
            owner,
            {"target": "Project Team"},
        )

        assert result["success"] is True
        assert result["post_source"] == "team_messaging+legacy_glip_groups"
        assert result["fallback_attempted"] is True
        assert result["included_count"] == 2
        assert result["messages"][0]["text"] == "manual update"
        assert result["messages"][1]["sender"] == "NewsBot (integration)"
        assert result["messages"][1]["source"] == "legacy_glip_groups"
        owner.list_legacy_group_posts.assert_awaited_once_with(
            "g-1",
            record_count=250,
        )

    def test_tool_rejects_non_owner_session_user(self, monkeypatch):
        bot, owner = self._clients()

        result = self._call_tool_with_clients(
            monkeypatch,
            bot,
            owner,
            {"target": "Project Team"},
            user_id="alice@example.com",
        )

        assert result["success"] is False
        assert "Only the configured RingCentral owner" in result["error"]
        owner.list_posts.assert_not_awaited()

    def test_tool_rejects_group_session(self, monkeypatch):
        bot, owner = self._clients()

        result = self._call_tool_with_clients(
            monkeypatch,
            bot,
            owner,
            {"target": "Project Team"},
            chat_id="g-1",
        )

        assert result["success"] is False
        assert "bot DM" in result["error"]
        owner.list_posts.assert_not_awaited()

    def test_tool_rejects_dm_without_owner_bot_members(self, monkeypatch):
        bot, owner = self._clients()
        bot.get_chat = AsyncMock(return_value={
            "id": "dm-other",
            "type": "Direct",
            "members": [{"id": "owner-1"}, {"id": "user-2"}],
        })

        result = self._call_tool_with_clients(
            monkeypatch,
            bot,
            owner,
            {"target": "Project Team"},
            chat_id="dm-other",
        )

        assert result["success"] is False
        assert "owner-bot DM" in result["error"]
        owner.list_posts.assert_not_awaited()

    def test_tool_rejects_missing_owner_credentials(self, monkeypatch):
        monkeypatch.setenv("RC_BOT_TOKEN", "bot-token")
        monkeypatch.delenv("RC_USER_CLIENT_ID", raising=False)
        monkeypatch.delenv("RC_USER_CLIENT_SECRET", raising=False)
        monkeypatch.delenv("RC_USER_JWT_TOKEN", raising=False)

        result = self._call_tool({"target": "Project Team"})

        assert result["success"] is False
        assert "RC_USER_CLIENT_ID" in result["error"]

    def test_tool_rejects_non_ringcentral_session(self, monkeypatch):
        self._set_owner_env(monkeypatch)

        result = self._call_tool(
            {"target": "Project Team"},
            platform="discord",
        )

        assert result["success"] is False
        assert "RingCentral session" in result["error"]

    def test_owner_dm_message_is_forwarded_to_agent_with_history_tool_prompt(
        self,
        monkeypatch,
    ):
        monkeypatch.setenv("RC_ALLOWED_USER_EMAILS", "owner@example.com")
        adapter = _make_adapter()
        bot = MagicMock()
        bot.get_chat = AsyncMock(return_value={
            "id": "dm-1",
            "type": "Direct",
            "members": [{"id": "owner-1"}, {"id": "bot-1"}],
        })
        bot.list_chats = AsyncMock(return_value=[])
        bot.get_person = AsyncMock(return_value={
            "firstName": "Owner",
            "email": "owner@example.com",
        })
        owner = MagicMock()
        owner.list_posts = AsyncMock()
        adapter._client = bot
        adapter._owner_client = owner
        adapter._own_person_id = "bot-1"
        adapter._owner_person_id = "owner-1"
        adapter._owner_email = "owner@example.com"
        adapter._owner_only_gate_enabled = True
        adapter.handle_message = AsyncMock()
        body = {
            "eventType": "PostAdded",
            "id": "p-trigger",
            "chatId": "dm-1",
            "creatorId": "owner-1",
            "text": "总结 Project Team 最近一天",
        }

        asyncio.run(adapter._handle_ws_event(body, identity="bot"))

        owner.list_posts.assert_not_awaited()
        adapter.handle_message.assert_awaited_once()
        event = adapter.handle_message.await_args.args[0]
        assert event.message_type == _rc_mod.MessageType.TEXT
        assert event.text == "总结 Project Team 最近一天"
        assert _RINGCENTRAL_HISTORY_TOOL_NAME in event.channel_prompt
        assert "current owner message" in event.channel_prompt
        assert "previous tool results" in event.channel_prompt
        assert "ask the owner to clarify" in event.channel_prompt

    def test_owner_dm_message_preserves_ringcentral_target_mention(self, monkeypatch):
        monkeypatch.setenv("RC_ALLOWED_USER_EMAILS", "owner@example.com")
        adapter = _make_adapter()
        bot = MagicMock()
        bot.get_chat = AsyncMock(return_value={
            "id": "dm-1",
            "type": "Direct",
            "members": [{"id": "owner-1"}, {"id": "bot-1"}],
        })
        bot.list_chats = AsyncMock(return_value=[])
        bot.get_person = AsyncMock(return_value={
            "firstName": "Owner",
            "email": "owner@example.com",
        })
        adapter._client = bot
        adapter._own_person_id = "bot-1"
        adapter._owner_person_id = "owner-1"
        adapter._owner_email = "owner@example.com"
        adapter._owner_only_gate_enabled = True
        adapter.handle_message = AsyncMock()
        body = {
            "eventType": "PostAdded",
            "id": "p-trigger",
            "chatId": "dm-1",
            "creatorId": "owner-1",
            "text": "总结 ![:Team](987654321000) 最近一天",
        }

        asyncio.run(adapter._handle_ws_event(body, identity="bot"))

        adapter.handle_message.assert_awaited_once()
        event = adapter.handle_message.await_args.args[0]
        assert event.text == "总结 ![:Team](987654321000) 最近一天"


# ---------------------------------------------------------------------------
# Auto-resume guard (issue #9)
# ---------------------------------------------------------------------------


class TestAutoResumeGuard:
    """Restart auto-resume must not blast unsolicited replies into groups."""

    def _make_event(self, adapter, *, chat_type, text="", internal=True):
        from gateway.platforms.base import MessageEvent, MessageType

        source = adapter.build_source(
            chat_id="g-99",
            chat_type=chat_type,
            user_id="u-1",
            user_name="Alice",
        )
        return MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            source=source,
            internal=internal,
        )

    def test_group_empty_internal_is_dropped_and_cleared(self):
        adapter = _make_adapter()
        adapter.set_message_handler(AsyncMock())
        store = MagicMock()
        adapter._session_store = store
        event = self._make_event(adapter, chat_type="group")

        with patch(
            "gateway.platforms.base.BasePlatformAdapter.handle_message",
            new=AsyncMock(),
        ) as super_hm:
            asyncio.run(adapter.handle_message(event))

        super_hm.assert_not_awaited()
        store.clear_resume_pending.assert_called_once()
        # Verify the key passed to clear_resume_pending matches what
        # build_session_key produces for the same source — a drift in
        # key construction would silently break the guard.
        from gateway.session import build_session_key

        expected_key = build_session_key(
            event.source,
            group_sessions_per_user=True,
            thread_sessions_per_user=False,
        )
        store.clear_resume_pending.assert_called_once_with(expected_key)

    def test_dm_empty_internal_is_forwarded(self):
        adapter = _make_adapter()
        adapter._session_store = MagicMock()
        event = self._make_event(adapter, chat_type="dm")

        with patch(
            "gateway.platforms.base.BasePlatformAdapter.handle_message",
            new=AsyncMock(),
        ) as super_hm:
            asyncio.run(adapter.handle_message(event))

        super_hm.assert_awaited_once()
        adapter._session_store.clear_resume_pending.assert_not_called()

    def test_normal_group_message_is_forwarded(self):
        adapter = _make_adapter()
        adapter._session_store = MagicMock()
        event = self._make_event(
            adapter, chat_type="group", text="hello bot", internal=False
        )

        with patch(
            "gateway.platforms.base.BasePlatformAdapter.handle_message",
            new=AsyncMock(),
        ) as super_hm:
            asyncio.run(adapter.handle_message(event))

        super_hm.assert_awaited_once()
        adapter._session_store.clear_resume_pending.assert_not_called()

    def test_internal_with_text_is_forwarded(self):
        # Only empty-text internal events are auto-resume; non-empty internal
        # events (continuations, kickoffs) must still pass through.
        adapter = _make_adapter()
        adapter._session_store = MagicMock()
        event = self._make_event(
            adapter, chat_type="group", text="continue please", internal=True
        )

        with patch(
            "gateway.platforms.base.BasePlatformAdapter.handle_message",
            new=AsyncMock(),
        ) as super_hm:
            asyncio.run(adapter.handle_message(event))

        super_hm.assert_awaited_once()
        adapter._session_store.clear_resume_pending.assert_not_called()


# ---------------------------------------------------------------------------
# _is_connected
# ---------------------------------------------------------------------------


class TestIsConnected:
    def test_false_when_no_token(self, monkeypatch):
        monkeypatch.delenv("RC_BOT_TOKEN", raising=False)
        from gateway.config import PlatformConfig

        assert _is_connected(PlatformConfig()) is False

    def test_true_with_env(self, monkeypatch):
        monkeypatch.setenv("RC_BOT_TOKEN", "jwt-abc")
        from gateway.config import PlatformConfig

        assert _is_connected(PlatformConfig()) is True


# ---------------------------------------------------------------------------
# Standalone send
# ---------------------------------------------------------------------------


class TestStandaloneSend:
    def test_missing_token_returns_error(self, monkeypatch):
        monkeypatch.delenv("RC_BOT_TOKEN", raising=False)
        from gateway.config import PlatformConfig

        cfg = PlatformConfig()
        result = asyncio.run(_standalone_send(cfg, "g-1", "hi"))
        assert "RC_BOT_TOKEN" in result.get("error", "")

    def test_missing_chat_id_returns_error(self, monkeypatch):
        monkeypatch.setenv("RC_BOT_TOKEN", "jwt")
        from gateway.config import PlatformConfig

        cfg = PlatformConfig()
        result = asyncio.run(_standalone_send(cfg, "", "hi"))
        assert "chat_id" in result.get("error", "")

    def test_happy_path_uses_client(self, monkeypatch):
        monkeypatch.setenv("RC_BOT_TOKEN", "jwt")
        from gateway.config import PlatformConfig

        fake_client = MagicMock()
        fake_client.send_post = AsyncMock(return_value={"id": "p-x"})
        fake_client.close = AsyncMock()

        cfg = PlatformConfig(token="jwt", extra={})
        with patch.object(_rc_mod, "RingCentralClient", return_value=fake_client):
            result = asyncio.run(_standalone_send(cfg, "g-1", "hi"))

        assert result.get("success") is True
        assert result.get("message_id") == "p-x"
        fake_client.send_post.assert_awaited_once_with("g-1", "hi")
        fake_client.close.assert_awaited_once()

    def test_thread_id_is_used_for_text_post(self, monkeypatch):
        monkeypatch.setenv("RC_BOT_TOKEN", "jwt")
        from gateway.config import PlatformConfig

        fake_client = MagicMock()
        fake_client.send_post = AsyncMock(return_value={"id": "p-x", "threadId": "t-1"})
        fake_client.close = AsyncMock()

        cfg = PlatformConfig(token="jwt", extra={})
        with patch.object(_rc_mod, "RingCentralClient", return_value=fake_client):
            result = asyncio.run(
                _standalone_send(cfg, "g-1", "hi", thread_id="t-1")
            )

        assert result.get("success") is True
        assert result.get("thread_id") == "t-1"
        fake_client.send_post.assert_awaited_once_with("g-1", "hi", thread_id="t-1")
        fake_client.close.assert_awaited_once()

    def test_parent_post_metadata_is_used_for_text_post(self, monkeypatch):
        monkeypatch.setenv("RC_BOT_TOKEN", "jwt")
        from gateway.config import PlatformConfig

        fake_client = MagicMock()
        fake_client.send_post = AsyncMock(return_value={"id": "p-x", "threadId": "t-1"})
        fake_client.close = AsyncMock()

        cfg = PlatformConfig(token="jwt", extra={})
        with patch.object(_rc_mod, "RingCentralClient", return_value=fake_client):
            result = asyncio.run(
                _standalone_send(
                    cfg,
                    "g-1",
                    "hi",
                    thread_id="parentPostId:p-parent",
                )
            )

        assert result.get("success") is True
        assert result.get("thread_id") == "t-1"
        fake_client.send_post.assert_awaited_once_with(
            "g-1",
            "hi",
            parent_post_id="p-parent",
        )
        fake_client.close.assert_awaited_once()

    def test_no_thread_channel_sends_plain_text_post(self, monkeypatch):
        monkeypatch.setenv("RC_BOT_TOKEN", "jwt")
        monkeypatch.setenv("RC_NO_THREAD_CHANNELS", "g-1")
        from gateway.config import PlatformConfig

        fake_client = MagicMock()
        fake_client.send_post = AsyncMock(return_value={"id": "p-x"})
        fake_client.close = AsyncMock()

        cfg = PlatformConfig(token="jwt", extra={})
        with patch.object(_rc_mod, "RingCentralClient", return_value=fake_client):
            result = asyncio.run(
                _standalone_send(cfg, "g-1", "hi", thread_id="t-1")
            )

        assert result.get("success") is True
        assert result.get("thread_id") == ""
        fake_client.send_post.assert_awaited_once_with("g-1", "hi")
        fake_client.close.assert_awaited_once()
