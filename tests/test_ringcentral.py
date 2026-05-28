"""Tests for the RingCentral platform adapter plugin."""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Ensure the repo root is on sys.path so `ringcentral` package resolves.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ringcentral import (  # noqa: E402
    register,
)
from ringcentral.adapter import (  # noqa: E402
    RingCentralAdapter,
    check_requirements,
    _content_type_for_filename,
    _env_enablement,
    _is_connected,
    _standalone_send,
    _summary_directory_search_terms,
    _summary_message_limit_from,
    _summary_query_from_text,
    _strip_rc_mentions,
    DEFAULT_SERVER_URL,
)
from ringcentral.rc_client import JWT_GRANT_TYPE, RingCentralClient  # noqa: E402
from ringcentral.rc_ws import RingCentralWebSocket, _OWN_POST_HISTORY  # noqa: E402
from ringcentral import adapter as _rc_mod  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_adapter(token: str = "test-token") -> Any:
    """Build a RingCentralAdapter without invoking network I/O."""
    from gateway.config import PlatformConfig

    cfg = PlatformConfig(enabled=True, token=token, extra={})
    return RingCentralAdapter(cfg)


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


# ---------------------------------------------------------------------------
# Summary parsing + config
# ---------------------------------------------------------------------------


class TestSummaryConfig:
    def test_summary_query_accepts_slash_command(self):
        assert _summary_query_from_text("/summarize Project 最近一天") == "Project 最近一天"

    def test_summary_query_accepts_chinese_keyword(self):
        assert _summary_query_from_text("总结 Project") == "Project"

    def test_summary_query_rejects_normal_text(self):
        assert _summary_query_from_text("please help with deployment") is None

    def test_summary_limit_defaults(self, monkeypatch):
        monkeypatch.delenv("RC_SUMMARY_MESSAGE_LIMIT", raising=False)
        assert _summary_message_limit_from({}) == 250

    def test_summary_limit_reads_and_clamps(self, monkeypatch):
        monkeypatch.delenv("RC_SUMMARY_MESSAGE_LIMIT", raising=False)
        assert _summary_message_limit_from({"summary_message_limit": "5000"}) == 1000
        assert _summary_message_limit_from({"summary_message_limit": "bad"}) == 250

    def test_summary_directory_terms_extract_latin_name_from_chinese_request(self):
        terms = _summary_directory_search_terms("我跟 Justin Wu 这一周的聊天")
        assert terms[0] == "Justin Wu"
        assert "我跟 Justin Wu 这一周的聊天" in terms


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

    def test_summary_limit_seeded(self, monkeypatch):
        monkeypatch.setenv("RC_BOT_TOKEN", "jwt-abc")
        monkeypatch.setenv("RC_SUMMARY_MESSAGE_LIMIT", "500")
        seed = _env_enablement() or {}
        assert seed["summary_message_limit"] == 500


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

    def test_summary_limit_from_config(self, monkeypatch):
        monkeypatch.delenv("RC_SUMMARY_MESSAGE_LIMIT", raising=False)
        from gateway.config import PlatformConfig

        cfg = PlatformConfig(enabled=True, token="t", extra={"summary_message_limit": "333"})
        adapter = RingCentralAdapter(cfg)
        assert adapter._summary_message_limit == 333

    def test_name_is_ringcentral(self):
        adapter = _make_adapter()
        assert adapter.name == "RingCentral"

    def test_owner_allowlist_auto_seeded(self, monkeypatch):
        monkeypatch.delenv("RC_ALLOWED_USERS", raising=False)
        monkeypatch.delenv("RC_ALLOW_ALL_USERS", raising=False)
        adapter = _make_adapter()
        adapter._owner_person_id = "owner-1"
        adapter._seed_owner_allowlist()
        assert adapter._owner_only_gate_enabled is True
        assert os.environ["RC_ALLOWED_USERS"] == "owner-1"


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
        })
        adapter._client = client
        adapter._owner_client = client
        adapter._own_person_id = "bot-1"
        adapter._owner_person_id = "owner-1"
        adapter._owner_only_gate_enabled = True
        adapter.handle_message = AsyncMock()
        return adapter

    def test_non_owner_group_message_is_observed_not_dispatched(self):
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

    def test_non_owner_group_message_with_chat_id_is_observed(self):
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

    def test_owner_non_summary_slash_message_includes_observed_context(self):
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
            "text": "/status",
        }

        asyncio.run(adapter._handle_ws_event(body, identity="owner"))

        event = adapter.handle_message.await_args.args[0]
        assert "Observed RingCentral group context" in event.text
        assert "deployment is red" in event.text
        assert event.text.endswith("/status")
        assert "RingCentral group chat message from the owner" in event.channel_prompt


class TestChatInfo:
    def test_get_chat_info_prefers_get_chat_direct_type(self):
        adapter = _make_adapter()
        client = MagicMock()
        client.get_chat = AsyncMock(return_value={
            "id": "1603237781506",
            "type": "Direct",
            "name": None,
        })
        client.list_chats = AsyncMock(return_value=[])
        adapter._client = client

        result = asyncio.run(adapter._get_chat_info("1603237781506"))

        assert result["type"] == "dm"
        assert result["chat_id"] == "1603237781506"


class TestOwnerDMSummary:
    @staticmethod
    def _summary_adapter():
        adapter = _make_adapter()
        bot = MagicMock()
        bot.list_chats = AsyncMock(return_value=[
            {"id": "dm-1", "type": "Direct", "name": "Owner DM"},
            {"id": "g-1", "type": "Team", "name": "Project Team"},
        ])
        bot.get_person = AsyncMock(side_effect=lambda pid: {
            "firstName": "Owner" if pid == "owner-1" else "Alice",
        })
        owner = MagicMock()
        owner.list_recent_chats = AsyncMock(return_value=[
            {"id": "g-1", "type": "Team", "name": "Project Team"},
        ])
        owner.list_chats = AsyncMock(return_value=[
            {"id": "dm-1", "type": "Direct", "name": "Owner DM"},
            {"id": "g-1", "type": "Team", "name": "Project Team"},
        ])
        owner.get_chat = AsyncMock(side_effect=lambda cid: {
            "id": cid,
            "type": "Team",
            "name": "Project Team",
        } if cid in {"g-1", "1603237781506"} else None)
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
        owner.get_person = bot.get_person
        adapter._client = bot
        adapter._owner_client = owner
        adapter._own_person_id = "bot-1"
        adapter._owner_person_id = "owner-1"
        adapter._owner_only_gate_enabled = True
        adapter._summary_message_limit = 250
        adapter.handle_message = AsyncMock()
        return adapter, owner

    def test_owner_dm_summary_fetches_group_posts_and_dispatches_to_agent(self):
        adapter, owner = self._summary_adapter()
        body = {
            "eventType": "PostAdded",
            "id": "p-trigger",
            "groupId": "dm-1",
            "creatorId": "owner-1",
            "text": "/summarize Project Team 最近一天",
        }

        asyncio.run(adapter._handle_ws_event(body, identity="owner"))

        owner.list_posts.assert_awaited_once_with("g-1", record_count=250)
        event = adapter.handle_message.await_args.args[0]
        assert event.message_type == _rc_mod.MessageType.TEXT
        assert not event.text.startswith("/")
        assert "Summarize the RingCentral chat history" in event.text
        assert "Project Team" in event.channel_context
        assert "Owner (owner-1): please deploy" in event.channel_context
        assert "Alice (user-2): deployment finished" in event.channel_context
        assert "owner's RC_USER credentials" in event.channel_prompt
        owner.list_legacy_group_posts.assert_not_awaited()
        metadata = event.raw_message["ringcentral_summary"]
        assert metadata["post_source"] == "team_messaging"
        assert metadata["usable_posts"] == 2

    def test_owner_dm_summary_accepts_chat_id_only_dm_event(self):
        adapter, owner = self._summary_adapter()
        body = {
            "eventType": "PostAdded",
            "id": "p-trigger",
            "chatId": "dm-1",
            "creatorId": "owner-1",
            "text": "/summarize Project Team 最近一天",
        }

        asyncio.run(adapter._handle_ws_event(body, identity="bot"))

        owner.list_posts.assert_awaited_once_with("g-1", record_count=250)
        adapter.handle_message.assert_awaited_once()
        event = adapter.handle_message.await_args.args[0]
        assert event.source.chat_id == "dm-1"
        assert "Project Team" in event.channel_context

    def test_owner_dm_summary_resolves_person_mention_to_direct_chat(self):
        adapter, owner = self._summary_adapter()
        owner.create_or_find_dm = AsyncMock(return_value={
            "id": "dm-user-1",
            "type": "Direct",
        })
        body = {
            "eventType": "PostAdded",
            "id": "p-trigger",
            "groupId": "dm-1",
            "creatorId": "owner-1",
            "text": "/summarize ![:Person](20001) 今天",
        }

        asyncio.run(adapter._handle_ws_event(body, identity="owner"))

        owner.create_or_find_dm.assert_awaited_once_with(["20001"])
        owner.list_posts.assert_awaited_once_with("dm-user-1", record_count=250)
        owner.list_legacy_group_posts.assert_not_awaited()
        adapter.handle_message.assert_awaited_once()
        event = adapter.handle_message.await_args.args[0]
        assert "Summarize the RingCentral chat history" in event.text
        assert "Target chat: Alice (id: dm-user-1)" in event.channel_context
        metadata = event.raw_message["ringcentral_summary"]
        assert metadata["target_chat"]["type"] == "dm"
        assert metadata["post_source"] == "team_messaging"

    def test_owner_dm_summary_resolves_directory_name_to_direct_chat(self):
        adapter, owner = self._summary_adapter()
        owner.search_directory = AsyncMock(return_value=[
            {
                "id": "20002",
                "firstName": "Alice",
                "lastName": "Wang",
                "email": "alice@example.com",
            },
        ])
        owner.create_or_find_dm = AsyncMock(return_value={
            "id": "dm-alice",
            "type": "Direct",
        })
        body = {
            "eventType": "PostAdded",
            "id": "p-trigger",
            "groupId": "dm-1",
            "creatorId": "owner-1",
            "text": "/summarize Alice Wang 昨天",
        }

        asyncio.run(adapter._handle_ws_event(body, identity="owner"))

        owner.search_directory.assert_awaited()
        assert owner.search_directory.await_args_list[0].args == ("Alice Wang",)
        owner.create_or_find_dm.assert_awaited_once_with(["20002"])
        owner.list_posts.assert_awaited_once_with("dm-alice", record_count=250)
        event = adapter.handle_message.await_args.args[0]
        assert "Target chat: Alice Wang (id: dm-alice)" in event.channel_context

    def test_owner_dm_summary_resolves_numeric_user_id_to_direct_chat(self):
        adapter, owner = self._summary_adapter()
        owner.create_or_find_dm = AsyncMock(return_value={
            "id": "dm-user-3",
            "type": "Direct",
        })
        body = {
            "eventType": "PostAdded",
            "id": "p-trigger",
            "groupId": "dm-1",
            "creatorId": "owner-1",
            "text": "/summarize 20003",
        }

        asyncio.run(adapter._handle_ws_event(body, identity="owner"))

        owner.get_chat.assert_any_await("20003")
        owner.search_directory.assert_not_awaited()
        owner.create_or_find_dm.assert_awaited_once_with(["20003"])
        owner.list_posts.assert_awaited_once_with("dm-user-3", record_count=250)
        event = adapter.handle_message.await_args.args[0]
        assert event.raw_message["ringcentral_summary"]["target_chat"]["type"] == "dm"

    def test_owner_dm_summary_uses_legacy_text_for_integration_posts(self):
        adapter, owner = self._summary_adapter()
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
        body = {
            "eventType": "PostAdded",
            "id": "p-trigger",
            "groupId": "dm-1",
            "creatorId": "owner-1",
            "text": "/summarize Project Team 最近一天",
        }

        asyncio.run(adapter._handle_ws_event(body, identity="owner"))

        owner.list_legacy_group_posts.assert_awaited_once_with(
            "g-1",
            record_count=250,
        )
        adapter.handle_message.assert_awaited_once()
        event = adapter.handle_message.await_args.args[0]
        assert "NewsBot (integration): integration update" in event.channel_context
        assert "Alice (user-2): manual update" in event.channel_context
        metadata = event.raw_message["ringcentral_summary"]
        assert metadata["post_source"] == "team_messaging+legacy_glip_groups"
        assert metadata["fallback_attempted"] is True
        assert metadata["usable_posts"] == 2

    def test_owner_dm_summary_skips_system_events_without_fallback(self):
        adapter, owner = self._summary_adapter()
        owner.list_posts = AsyncMock(return_value=[
            {
                "id": "p-system",
                "type": "PersonsAdded",
                "creatorId": "owner-1",
                "mentions": [{"id": "user-2"}],
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
        body = {
            "eventType": "PostAdded",
            "id": "p-trigger",
            "groupId": "dm-1",
            "creatorId": "owner-1",
            "text": "/summarize Project Team",
        }

        asyncio.run(adapter._handle_ws_event(body, identity="owner"))

        owner.list_legacy_group_posts.assert_not_awaited()
        adapter.handle_message.assert_awaited_once()
        event = adapter.handle_message.await_args.args[0]
        assert "manual update" in event.channel_context
        assert "p-system" not in event.channel_context
        assert "PersonsAdded" not in event.channel_context

    def test_owner_dm_summary_notices_when_fallback_has_no_readable_text(self):
        adapter, owner = self._summary_adapter()
        adapter._send_chunks = AsyncMock(return_value=MagicMock(success=True))
        owner.list_posts = AsyncMock(return_value=[
            {
                "id": "p-webhook",
                "type": "TextMessage",
                "creatorId": "",
                "text": "",
                "creationTime": "2026-05-28T02:20:00Z",
            },
        ])
        owner.list_legacy_group_posts = AsyncMock(return_value=[
            {
                "id": "p-webhook",
                "groupId": "g-1",
                "type": "TextMessage",
                "creatorId": "",
                "text": "",
                "creationTime": "2026-05-28T02:20:00Z",
            },
        ])
        body = {
            "eventType": "PostAdded",
            "id": "p-trigger",
            "groupId": "dm-1",
            "creatorId": "owner-1",
            "text": "/summarize Project Team",
        }

        asyncio.run(adapter._handle_ws_event(body, identity="owner"))

        owner.list_legacy_group_posts.assert_awaited_once()
        adapter.handle_message.assert_not_awaited()
        adapter._send_chunks.assert_awaited_once()
        assert "no readable message text" in adapter._send_chunks.await_args.args[1]

    def test_owner_dm_summary_uses_explicit_chat_id(self):
        adapter, owner = self._summary_adapter()
        body = {
            "eventType": "PostAdded",
            "id": "p-trigger",
            "groupId": "dm-1",
            "creatorId": "owner-1",
            "text": "/summarize 1603237781506",
        }

        asyncio.run(adapter._handle_ws_event(body, identity="owner"))

        owner.get_chat.assert_awaited()
        owner.list_posts.assert_awaited_once_with("1603237781506", record_count=250)
        adapter.handle_message.assert_awaited_once()

    def test_non_owner_chat_id_summary_in_dm_is_rejected(self):
        adapter, owner = self._summary_adapter()
        adapter._send_chunks = AsyncMock(return_value=MagicMock(success=True))
        body = {
            "eventType": "PostAdded",
            "id": "p-trigger",
            "chatId": "dm-1",
            "creatorId": "user-2",
            "text": "/summarize Project Team",
        }

        asyncio.run(adapter._handle_ws_event(body, identity="bot"))

        adapter.handle_message.assert_not_awaited()
        owner.list_posts.assert_not_awaited()
        adapter._send_chunks.assert_awaited_once()
        assert "Only the RingCentral owner" in adapter._send_chunks.await_args.args[1]

    def test_missing_owner_credentials_returns_notice_not_agent_dispatch(self):
        adapter = _make_adapter()
        bot = MagicMock()
        bot.list_chats = AsyncMock(return_value=[
            {"id": "dm-1", "type": "Direct", "name": "Owner DM"},
        ])
        bot.get_person = AsyncMock(return_value={"firstName": "Owner"})
        adapter._client = bot
        adapter._send_chunks = AsyncMock(return_value=MagicMock(success=True))
        adapter.handle_message = AsyncMock()
        body = {
            "eventType": "PostAdded",
            "id": "p-trigger",
            "groupId": "dm-1",
            "creatorId": "owner-1",
            "text": "/summarize Project Team",
        }

        asyncio.run(adapter._handle_ws_event(body, identity="bot"))

        adapter.handle_message.assert_not_awaited()
        adapter._send_chunks.assert_awaited_once()
        assert "RC_USER_CLIENT_ID" in adapter._send_chunks.await_args.args[1]

    def test_group_summary_request_is_blocked_with_dm_hint(self):
        adapter = TestOwnerInboundHandling._owner_adapter()
        adapter._send_chunks = AsyncMock(return_value=MagicMock(success=True))
        body = {
            "eventType": "PostAdded",
            "id": "p-trigger",
            "groupId": "g-1",
            "creatorId": "owner-1",
            "text": "/summarize Project Team",
        }

        asyncio.run(adapter._handle_ws_event(body, identity="owner"))

        adapter.handle_message.assert_not_awaited()
        adapter._send_chunks.assert_awaited_once()
        assert "bot DM" in adapter._send_chunks.await_args.args[1]


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
