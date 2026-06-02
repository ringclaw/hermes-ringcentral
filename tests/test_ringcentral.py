"""Tests for the RingCentral platform adapter plugin."""

from __future__ import annotations

import asyncio
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
    _strip_rc_mentions,
    DEFAULT_SERVER_URL,
)
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
# WebSocket echo dedup
# ---------------------------------------------------------------------------


class TestWebSocketEchoDedup:
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

    def test_name_is_ringcentral(self):
        adapter = _make_adapter()
        assert adapter.name == "RingCentral"


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
