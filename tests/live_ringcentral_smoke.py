"""Live RingCentral smoke test for the Hermes RingCentral plugin.

This test is intentionally excluded from the normal pytest discovery pattern.
Run it explicitly with RC_E2E_ENABLED=true and real RingCentral credentials.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional

import pytest

DEFAULT_SERVER_URL = "https://platform.ringcentral.com"


def read_bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name, "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


_REQUIRED = read_bool_env("RC_E2E_REQUIRED", False)
_ENABLED = read_bool_env("RC_E2E_ENABLED", False)

if _REQUIRED and not _ENABLED:
    raise RuntimeError("Set RC_E2E_ENABLED=true to run RingCentral live smoke tests.")

pytestmark = pytest.mark.skipif(
    not _ENABLED,
    reason="Set RC_E2E_ENABLED=true to run RingCentral live smoke tests.",
)


@pytest.mark.asyncio
async def test_ringcentral_live_smoke() -> None:
    global active_summary

    summary = LiveSummary("hermes-ringcentral")
    summary.set_context(build_base_summary_context())
    active_summary = summary
    env: Optional[LiveEnv] = None
    bot_client: Any = None
    owner_client: Any = None
    created_bot_post_ids: List[str] = []
    created_owner_post_ids: List[str] = []
    ringcentral_logger = logging.getLogger("ringcentral")
    previous_log_level = ringcentral_logger.level
    ringcentral_logger.setLevel(logging.CRITICAL)

    try:
        from ringcentral.rc_client import RingCentralClient

        env = read_live_env()
        summary.set_context(build_summary_context(env))
        bot_client = RingCentralClient(env.bot_token, env.server_url)
        owner_client = RingCentralClient.from_jwt(
            client_id=env.owner_client_id,
            client_secret=env.owner_client_secret,
            jwt_token=env.owner_jwt_token,
            server_url=env.server_url,
        )

        log_safe("live_start", chat=mask_id(env.chat_id))

        bot_extension = await live_step("bot_auth", bot_client.get_own_extension)
        assert_live(isinstance(bot_extension, dict) and bot_extension.get("id"), "bot_auth", bot_client)

        owner_extension = await live_step("owner_auth", owner_client.get_own_extension)
        assert_live(
            isinstance(owner_extension, dict) and owner_extension.get("id"),
            "owner_auth",
            owner_client,
        )

        chat = await live_step(
            "chat_metadata_preflight",
            lambda: get_chat_metadata(owner_client, bot_client, env.chat_id),
        )
        assert_live(isinstance(chat, dict) and str(chat.get("id")) == env.chat_id, "chat_metadata_preflight")

        await live_step(
            "owner_history_preflight",
            lambda: assert_client_can_read_history(owner_client, env.chat_id, env.record_count),
        )
        await live_step(
            "bot_history_preflight",
            lambda: assert_client_can_read_history(bot_client, env.chat_id, env.record_count),
        )

        await run_bot_send_owner_read_scenario(
            env=env,
            bot_client=bot_client,
            owner_client=owner_client,
            owner_person_id=str(owner_extension.get("id") or ""),
            created_bot_post_ids=created_bot_post_ids,
        )
        await run_owner_send_bot_receive_scenario(
            env=env,
            bot_client=bot_client,
            owner_client=owner_client,
            bot_person_id=str(bot_extension.get("id") or ""),
            created_bot_post_ids=created_bot_post_ids,
            created_owner_post_ids=created_owner_post_ids,
        )
        await run_threaded_reply_scenario(
            env=env,
            bot_client=bot_client,
            owner_client=owner_client,
            created_bot_post_ids=created_bot_post_ids,
            created_owner_post_ids=created_owner_post_ids,
        )
    except Exception as exc:  # noqa: BLE001
        summary.fail(exc)
        raise
    finally:
        try:
            if env and bot_client and owner_client:
                await cleanup_posts(
                    cleanup=env.cleanup,
                    chat_id=env.chat_id,
                    bot_client=bot_client,
                    owner_client=owner_client,
                    bot_post_ids=created_bot_post_ids,
                    owner_post_ids=created_owner_post_ids,
                )
            if bot_client:
                await bot_client.close()
            if owner_client:
                await owner_client.close()
        finally:
            ringcentral_logger.setLevel(previous_log_level)
            summary.write()
            if active_summary is summary:
                active_summary = None


@dataclass(frozen=True)
class LiveEnv:
    server_url: str
    bot_token: str
    owner_client_id: str
    owner_client_secret: str
    owner_jwt_token: str
    chat_id: str
    record_count: int
    cleanup: bool
    ws_timeout_ms: int


class LiveSummary:
    def __init__(self, name: str) -> None:
        self.name = name
        self.status = "passed"
        self.context: Dict[str, Any] = {}
        self.rows: Dict[str, Dict[str, Any]] = {}
        self.failure: Optional[Dict[str, str]] = None

    def set_context(self, details: Dict[str, Any]) -> None:
        self.context.update(details)

    def record(self, stage: str, details: Dict[str, Any]) -> None:
        row = self.rows.setdefault(
            stage,
            {
                "stage": stage,
                "status": "info",
                "details": {},
            },
        )
        row["details"].update(details)
        if details.get("ok") is True:
            row["status"] = "ok"
        elif details.get("ok") is False:
            row["status"] = "failed"
        if isinstance(details.get("duration_ms"), (int, float)):
            row["duration_ms"] = int(details["duration_ms"])

    def fail(self, exc: Exception) -> None:
        self.status = "failed"
        self.failure = summarize_failure_for_summary(exc)
        self.record(
            self.failure["stage"],
            {
                "ok": False,
                "error": self.failure["error"],
            },
        )

    def write(self) -> None:
        summary_path = os.getenv("GITHUB_STEP_SUMMARY", "").strip()
        if not summary_path:
            return
        with Path(summary_path).open("a", encoding="utf-8") as summary_file:
            summary_file.write(f"{self.to_markdown()}\n")

    def to_markdown(self) -> str:
        context_rows = "\n".join(
            f"| {escape_markdown_cell(key)} | {escape_markdown_cell(value)} |"
            for key, value in self.context.items()
        )
        stage_rows = "\n".join(
            self.format_stage_row(row)
            for row in self.rows.values()
        )
        failure = ""
        if self.failure:
            failure = (
                f"\n\nFailure: {escape_markdown_cell(self.failure['stage'])} "
                f"{escape_markdown_cell(self.failure['error'])}"
            )
        return "\n".join(
            [
                f"## {self.name} live smoke",
                "",
                f"Overall: {self.status}",
                "",
                "| Context | Value |",
                "| --- | --- |",
                context_rows or "| none | none |",
                "",
                "| Stage | Status | Duration ms | Details |",
                "| --- | --- | ---: | --- |",
                stage_rows or "| none | info |  |  |",
                failure,
            ]
        )

    @staticmethod
    def format_stage_row(row: Dict[str, Any]) -> str:
        details = " ".join(
            f"{key}={format_summary_value(value)}"
            for key, value in row["details"].items()
            if key not in {"ok", "duration_ms"}
        )
        return (
            f"| {escape_markdown_cell(row['stage'])} | "
            f"{escape_markdown_cell(row['status'])} | "
            f"{escape_markdown_cell(row.get('duration_ms', ''))} | "
            f"{escape_markdown_cell(details)} |"
        )


active_summary: Optional[LiveSummary] = None


def read_live_env() -> LiveEnv:
    missing: List[str] = []

    def read_required(name: str) -> str:
        value = os.getenv(name, "").strip()
        if not value:
            missing.append(name)
        return value

    env = LiveEnv(
        server_url=os.getenv("RC_SERVER_URL", "").strip() or DEFAULT_SERVER_URL,
        bot_token=read_required("RC_BOT_TOKEN"),
        owner_client_id=read_required("RC_USER_CLIENT_ID"),
        owner_client_secret=read_required("RC_USER_CLIENT_SECRET"),
        owner_jwt_token=read_required("RC_USER_JWT_TOKEN"),
        chat_id=read_required("RC_E2E_CHAT_ID"),
        record_count=read_positive_int_env("RC_E2E_RECORD_COUNT", 50, 1, 1000),
        cleanup=read_bool_env("RC_E2E_CLEANUP", False),
        ws_timeout_ms=read_positive_int_env("RC_E2E_WS_TIMEOUT_MS", 30_000, 5_000, 120_000),
    )
    if missing:
        raise RuntimeError(f"Missing RingCentral live smoke variables: {', '.join(missing)}")
    return env


def build_summary_context(env: LiveEnv) -> Dict[str, Any]:
    return {
        **build_base_summary_context(),
        "cleanup": env.cleanup,
        "record_count": env.record_count,
        "ws_timeout_ms": env.ws_timeout_ms,
    }


def build_base_summary_context() -> Dict[str, Any]:
    return {
        "repository": os.getenv("GITHUB_REPOSITORY", "local"),
        "event": os.getenv("GITHUB_EVENT_NAME", "local"),
        "source_present": bool(os.getenv("RC_E2E_SOURCE_URL", "").strip()),
        "commit_present": bool(os.getenv("RC_E2E_COMMIT_SHA", "").strip()),
        "cleanup": read_bool_env("RC_E2E_CLEANUP", False),
        "record_count": read_positive_int_env("RC_E2E_RECORD_COUNT", 50, 1, 1000),
        "ws_timeout_ms": read_positive_int_env("RC_E2E_WS_TIMEOUT_MS", 30_000, 5_000, 120_000),
    }


def read_positive_int_env(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(float(os.getenv(name, str(default))))
    except (TypeError, ValueError):
        value = default
    return min(max(value, minimum), maximum)


async def run_bot_send_owner_read_scenario(
    *,
    env: LiveEnv,
    bot_client: RingCentralClient,
    owner_client: RingCentralClient,
    owner_person_id: str,
    created_bot_post_ids: List[str],
) -> None:
    text = build_unique_text("bot-send")
    edited_text = build_unique_text("bot-edit")
    owner_waiter = await start_websocket_wait(
        client=owner_client,
        person_id=owner_person_id,
        filter_own_creator=False,
        state_event="owner_ws_state",
        label="live-owner",
        chat_id=env.chat_id,
        expected_text=text,
    )
    try:
        await live_step(
            "owner_ws_connect",
            lambda: with_timeout(owner_waiter.connected, env.ws_timeout_ms, "owner_ws_connect"),
        )
        await live_step(
            "owner_ws_subscribe",
            lambda: with_timeout(owner_waiter.subscribed, env.ws_timeout_ms, "owner_ws_subscribe"),
        )

        sent = await live_step("bot_send", lambda: bot_client.send_post(env.chat_id, text))
        assert_live(isinstance(sent, dict) and sent.get("id"), "bot_send", bot_client)
        created_bot_post_ids.append(str(sent["id"]))
        log_safe("bot_send", sent=True)

        owner_ws_received = await live_step(
            "owner_ws_receive_bot_message",
            lambda: with_timeout(owner_waiter.received, env.ws_timeout_ms, "owner_ws_receive_bot_message"),
        )
        assert_live(
            str(owner_ws_received.get("groupId") or "") == env.chat_id
            and text in str(owner_ws_received.get("text") or ""),
            "owner_ws_receive_bot_message",
        )
        log_safe("owner_ws_receive_bot_message", ws_received=True)

        found = await live_step(
            "owner_read_bot_message",
            lambda: wait_for_post(owner_client, env.chat_id, text, env.record_count),
        )
        assert_live(found and str(found.get("id")) == str(sent["id"]), "owner_read_bot_message", owner_client)
        log_safe("owner_read_bot_message", found=True)

        edited = await live_step(
            "bot_edit",
            lambda: bot_client.update_post(env.chat_id, str(sent["id"]), edited_text),
        )
        assert_live(isinstance(edited, dict) and str(edited.get("id")) == str(sent["id"]), "bot_edit", bot_client)
        log_safe("bot_edit", edited=True)

        edited_found = await live_step(
            "owner_read_bot_edit",
            lambda: wait_for_post(owner_client, env.chat_id, edited_text, env.record_count),
        )
        assert_live(
            edited_found and str(edited_found.get("id")) == str(sent["id"]),
            "owner_read_bot_edit",
            owner_client,
        )
        log_safe("owner_read_bot_edit", found=True)
    finally:
        await owner_waiter.stop()


async def run_owner_send_bot_receive_scenario(
    *,
    env: LiveEnv,
    bot_client: RingCentralClient,
    owner_client: RingCentralClient,
    bot_person_id: str,
    created_bot_post_ids: List[str],
    created_owner_post_ids: List[str],
) -> None:
    owner_text = build_unique_text("owner-send")
    reply_text = build_unique_text("bot-reply")
    waiter = await start_websocket_wait(
        client=bot_client,
        person_id=bot_person_id,
        filter_own_creator=True,
        state_event="bot_ws_state",
        label="live-bot",
        chat_id=env.chat_id,
        expected_text=owner_text,
    )
    try:
        await live_step(
            "bot_ws_connect",
            lambda: with_timeout(waiter.connected, env.ws_timeout_ms, "bot_ws_connect"),
        )
        await live_step(
            "bot_ws_subscribe",
            lambda: with_timeout(waiter.subscribed, env.ws_timeout_ms, "bot_ws_subscribe"),
        )

        owner_post = await live_step("owner_send", lambda: owner_client.send_post(env.chat_id, owner_text))
        assert_live(isinstance(owner_post, dict) and owner_post.get("id"), "owner_send", owner_client)
        created_owner_post_ids.append(str(owner_post["id"]))
        log_safe("owner_send", sent=True)

        received = await live_step(
            "bot_ws_receive",
            lambda: with_timeout(waiter.received, env.ws_timeout_ms, "bot_ws_receive"),
        )
        assert_live(
            str(received.get("groupId") or "") == env.chat_id and owner_text in str(received.get("text") or ""),
            "bot_ws_receive",
        )
        log_safe("bot_ws_receive", ws_received=True)

        bot_read = await live_step(
            "bot_read_owner_message",
            lambda: wait_for_post(bot_client, env.chat_id, owner_text, env.record_count),
        )
        assert_live(bot_read and owner_text in str(bot_read.get("text") or ""), "bot_read_owner_message", bot_client)
        log_safe("bot_read_owner_message", bot_read_found=True)

        reply = await live_step("bot_reply", lambda: bot_client.send_post(env.chat_id, reply_text))
        assert_live(isinstance(reply, dict) and reply.get("id"), "bot_reply", bot_client)
        created_bot_post_ids.append(str(reply["id"]))
        log_safe("bot_reply", sent=True)

        owner_read_reply = await live_step(
            "owner_read_bot_reply",
            lambda: wait_for_post(owner_client, env.chat_id, reply_text, env.record_count),
        )
        assert_live(
            owner_read_reply and reply_text in str(owner_read_reply.get("text") or ""),
            "owner_read_bot_reply",
            owner_client,
        )
        log_safe("owner_read_bot_reply", owner_read_found=True)
    finally:
        await waiter.stop()


async def run_threaded_reply_scenario(
    *,
    env: LiveEnv,
    bot_client: RingCentralClient,
    owner_client: RingCentralClient,
    created_bot_post_ids: List[str],
    created_owner_post_ids: List[str],
) -> None:
    root_text = build_unique_text("owner-thread-root")
    reply_text = build_unique_text("bot-thread-reply")

    root = await live_step("owner_thread_root_send", lambda: owner_client.send_post(env.chat_id, root_text))
    assert_live(isinstance(root, dict) and root.get("id"), "owner_thread_root_send", owner_client)
    root_id = str(root["id"])
    created_owner_post_ids.append(root_id)
    log_safe("owner_thread_root_send", sent=True)

    reply = await live_step(
        "bot_thread_reply",
        lambda: bot_client.send_post(env.chat_id, reply_text, parent_post_id=root_id),
    )
    assert_live(isinstance(reply, dict) and reply.get("id"), "bot_thread_reply", bot_client)
    created_bot_post_ids.append(str(reply["id"]))
    log_safe("bot_thread_reply", sent=True)

    owner_read_reply = await live_step(
        "owner_read_thread_reply",
        lambda: wait_for_post(owner_client, env.chat_id, reply_text, env.record_count),
    )
    assert_live(
        owner_read_reply
        and str(owner_read_reply.get("id")) == str(reply["id"])
        and has_thread_metadata(owner_read_reply, root_id),
        "owner_read_thread_reply",
        owner_client,
    )
    log_safe("owner_read_thread_reply", owner_read_found=True, thread_metadata=True)


async def get_chat_metadata(
    owner_client: RingCentralClient,
    bot_client: RingCentralClient,
    chat_id: str,
) -> Dict[str, Any]:
    owner_chat = await owner_client.get_chat(chat_id)
    if isinstance(owner_chat, dict):
        return owner_chat
    bot_chat = await bot_client.get_chat(chat_id)
    if isinstance(bot_chat, dict):
        log_safe("chat_metadata_preflight", owner_lookup=False, bot_lookup=True)
        return bot_chat
    raise LiveSmokeFailure(status=bot_client.last_status or owner_client.last_status)


async def assert_client_can_read_history(
    client: RingCentralClient,
    chat_id: str,
    record_count: int,
) -> None:
    await read_recent_posts(client, chat_id, min(record_count, 1))


async def wait_for_post(
    client: RingCentralClient,
    chat_id: str,
    expected_text: str,
    record_count: int,
) -> Optional[Dict[str, Any]]:
    for _ in range(10):
        post = await find_recent_post(client, chat_id, expected_text, record_count)
        if post:
            return post
        await asyncio.sleep(1.5)
    return None


async def find_recent_post(
    client: RingCentralClient,
    chat_id: str,
    expected_text: str,
    record_count: int,
) -> Optional[Dict[str, Any]]:
    posts = await read_recent_posts(client, chat_id, record_count)
    return next(
        (
            post
            for post in posts
            if expected_text in str(post.get("text") or post.get("activity") or "")
        ),
        None,
    )


async def read_recent_posts(
    client: RingCentralClient,
    chat_id: str,
    record_count: int,
) -> List[Dict[str, Any]]:
    posts = await client.list_posts(chat_id, record_count)
    if posts is not None:
        return posts
    posts = await client.list_legacy_group_posts(chat_id, record_count)
    if posts is not None:
        return posts
    raise LiveSmokeFailure(status=client.last_status)


@dataclass
class WebSocketWaiter:
    connected: "asyncio.Future[None]"
    subscribed: "asyncio.Future[None]"
    received: "asyncio.Future[Dict[str, Any]]"
    stop: Callable[[], Awaitable[None]]


async def start_websocket_wait(
    *,
    client: RingCentralClient,
    person_id: str,
    filter_own_creator: bool,
    state_event: str,
    label: str,
    chat_id: str,
    expected_text: str,
) -> WebSocketWaiter:
    from ringcentral.rc_ws import RingCentralWebSocket

    loop = asyncio.get_running_loop()
    connected: "asyncio.Future[None]" = loop.create_future()
    subscribed: "asyncio.Future[None]" = loop.create_future()
    received: "asyncio.Future[Dict[str, Any]]" = loop.create_future()

    def reject_pending(exc: Exception) -> None:
        for future in (connected, subscribed, received):
            if not future.done():
                future.set_exception(exc)

    async def on_state(event: str, details: Dict[str, Any]) -> None:
        log_safe(state_event, **sanitize_diagnostic(event, details))
        if event == "ws_connected" and not connected.done():
            connected.set_result(None)
        elif event == "ws_subscription_confirmed" and not subscribed.done():
            subscribed.set_result(None)
        elif event == "ws_subscription_rejected":
            reject_pending(LiveSmokeFailure(status=read_status(details)))

    async def on_event(body: Dict[str, Any]) -> None:
        if (
            str(body.get("groupId") or "") == chat_id
            and expected_text in str(body.get("text") or "")
            and not received.done()
        ):
            received.set_result(body)

    monitor = RingCentralWebSocket(
        client,
        on_event,
        own_person_id=person_id,
        filter_own_creator=filter_own_creator,
        on_state=on_state,
        label=label,
    )
    await monitor.start()

    async def stop() -> None:
        await monitor.stop()

    return WebSocketWaiter(
        connected=connected,
        subscribed=subscribed,
        received=received,
        stop=stop,
    )


async def cleanup_posts(
    *,
    cleanup: bool,
    chat_id: str,
    bot_client: RingCentralClient,
    owner_client: RingCentralClient,
    bot_post_ids: List[str],
    owner_post_ids: List[str],
) -> None:
    if not cleanup:
        log_safe("cleanup", enabled=False)
        return

    cleanup_bot_post = True
    cleanup_owner_post = True
    for post_id in reversed(bot_post_ids):
        try:
            cleanup_bot_post = bool(await bot_client.delete_post(chat_id, post_id)) and cleanup_bot_post
        except Exception:  # noqa: BLE001
            cleanup_bot_post = False
    for post_id in reversed(owner_post_ids):
        try:
            cleanup_owner_post = bool(await owner_client.delete_post(chat_id, post_id)) and cleanup_owner_post
        except Exception:  # noqa: BLE001
            cleanup_owner_post = False
    log_safe(
        "cleanup",
        cleanup_bot_post=cleanup_bot_post,
        cleanup_owner_post=cleanup_owner_post,
    )


def build_unique_text(label: str) -> str:
    run_id = os.getenv("GITHUB_RUN_ID", "local")
    attempt = os.getenv("GITHUB_RUN_ATTEMPT", "1")
    marker = f"[hermes-ringcentral-e2e:{label}:{run_id}:{attempt}:{int(time.time() * 1000)}]"
    source_url = os.getenv("RC_E2E_SOURCE_URL", "").strip()
    commit_sha = os.getenv("RC_E2E_COMMIT_SHA", "").strip()
    return "\n".join(
        part
        for part in (
            marker,
            f"source: {source_url}" if source_url else "",
            f"commit: {commit_sha}" if commit_sha else "",
        )
        if part
    )


def has_thread_metadata(post: Dict[str, Any], parent_post_id: str) -> bool:
    parent = post.get("parentPostId") or post.get("parentId")
    if parent and str(parent) == str(parent_post_id):
        return True
    return bool(post.get("threadId") or post.get("rootPostId") or post.get("rootId"))


async def live_step(stage: str, fn: Callable[[], Awaitable[Any]]) -> Any:
    start = time.time()
    try:
        result = await fn()
        log_safe(stage, ok=True, duration_ms=int((time.time() - start) * 1000))
        return result
    except Exception as exc:  # noqa: BLE001
        raise to_safe_live_error(stage, exc) from None


def assert_live(
    condition: Any,
    stage: str,
    client: Optional[RingCentralClient] = None,
) -> None:
    if condition:
        return
    status = client.last_status if client else None
    raise to_safe_live_error(stage, LiveSmokeFailure(status=status))


def to_safe_live_error(stage: str, exc: Exception) -> RuntimeError:
    return RuntimeError(f"Hermes RingCentral live smoke failed at {stage}: {summarize_safe_error(exc)}")


def summarize_safe_error(exc: Exception) -> str:
    if isinstance(exc, LiveTimeoutError):
        return "timeout"
    if isinstance(exc, LiveSmokeFailure):
        return f"HTTP {exc.status}" if exc.status else "assertion failed"
    return "failed"


class LiveSmokeFailure(Exception):
    def __init__(self, *, status: Optional[int] = None) -> None:
        super().__init__("live smoke failure")
        self.status = status


class LiveTimeoutError(Exception):
    pass


async def with_timeout(awaitable: Awaitable[Any], timeout_ms: int, stage: str) -> Any:
    try:
        return await asyncio.wait_for(awaitable, timeout=timeout_ms / 1000)
    except asyncio.TimeoutError as exc:
        raise LiveTimeoutError(stage) from exc


def read_status(details: Dict[str, Any]) -> Optional[int]:
    raw = details.get("status")
    return raw if isinstance(raw, int) else None


def mask_id(value: Any) -> str:
    raw = str(value or "")
    if not raw:
        return "<masked>"
    return f"<masked:length={len(raw)}>"


def sanitize_diagnostic(event: str, details: Dict[str, Any]) -> Dict[str, Any]:
    safe: Dict[str, Any] = {"state": event}
    for key, value in details.items():
        if isinstance(value, (bool, int, float)):
            safe[key] = value
    return safe


def log_safe(event: str, **details: Any) -> None:
    safe_details = {
        key: format_safe_value(value)
        for key, value in details.items()
    }
    suffix = " ".join(f"{key}={format_safe_display(value)}" for key, value in safe_details.items())
    print(f"[hermes-ringcentral-live] event={event}{f' {suffix}' if suffix else ''}", flush=True)
    if active_summary:
        active_summary.record(event, safe_details)


def format_safe_value(value: Any) -> Any:
    if isinstance(value, (int, float)):
        return value
    raw = str(value)
    if raw.startswith("<masked:length=") or raw in SAFE_STRING_VALUES or re.match(
        r"^HTTP \d{3}(?: [A-Z0-9_-]+)?$",
        raw,
    ):
        return raw
    return "<masked>"


def format_safe_display(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


SAFE_STRING_VALUES = {
    "<masked>",
    "ws_connected",
    "ws_subscription_confirmed",
    "ws_subscription_rejected",
    "ws_subscription_request_sent",
    "ws_post_received",
    "timeout",
    "assertion failed",
    "failed",
    "missing required variables",
}


def summarize_failure_for_summary(exc: Exception) -> Dict[str, str]:
    message = str(exc)
    match = re.match(r"^Hermes RingCentral live smoke failed at ([a-z0-9_]+): (.+)$", message)
    if match:
        return {
            "stage": match.group(1),
            "error": sanitize_failure_message(match.group(2)),
        }
    if message.startswith("Missing RingCentral live smoke variables:"):
        return {
            "stage": "configuration",
            "error": "missing required variables",
        }
    return {
        "stage": "unknown",
        "error": "failed",
    }


def sanitize_failure_message(message: str) -> str:
    clean = message.strip()
    if re.match(r"^HTTP \d{3}(?: [A-Z0-9_-]+)?$", clean):
        return clean
    if clean in SAFE_STRING_VALUES:
        return clean
    return "failed"


def escape_markdown_cell(value: Any) -> str:
    return format_summary_value(value).replace("|", "\\|").replace("\n", " ")


def format_summary_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)
