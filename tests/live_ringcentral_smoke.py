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
    created_owner_event_ids: List[str] = []
    created_bot_adaptive_card_ids: List[str] = []
    created_owner_note_ids: List[str] = []
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

        await run_file_upload_scenario(
            env=env,
            bot_client=bot_client,
            owner_client=owner_client,
            bot_person_id=str(bot_extension.get("id") or ""),
            created_owner_post_ids=created_owner_post_ids,
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
        await run_calendar_event_scenario(
            env=env,
            owner_client=owner_client,
            created_owner_event_ids=created_owner_event_ids,
        )
        await run_adaptive_card_scenario(
            env=env,
            bot_client=bot_client,
            created_bot_adaptive_card_ids=created_bot_adaptive_card_ids,
        )
        await run_note_scenario(
            env=env,
            owner_client=owner_client,
            created_owner_note_ids=created_owner_note_ids,
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
                await cleanup_events(
                    cleanup=env.cleanup,
                    owner_client=owner_client,
                    event_ids=created_owner_event_ids,
                )
                await cleanup_adaptive_cards(
                    cleanup=env.cleanup,
                    bot_client=bot_client,
                    adaptive_card_ids=created_bot_adaptive_card_ids,
                )
                await cleanup_notes(
                    cleanup=env.cleanup,
                    owner_client=owner_client,
                    note_ids=created_owner_note_ids,
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
    file_upload: bool


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
        file_upload=read_bool_env("RC_E2E_FILE_UPLOAD", True),
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
        "file_upload": env.file_upload,
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
        "file_upload": read_bool_env("RC_E2E_FILE_UPLOAD", True),
    }


def read_positive_int_env(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(float(os.getenv(name, str(default))))
    except (TypeError, ValueError):
        value = default
    return min(max(value, minimum), maximum)


async def run_file_upload_scenario(
    *,
    env: LiveEnv,
    bot_client: RingCentralClient,
    owner_client: RingCentralClient,
    bot_person_id: str,
    created_owner_post_ids: List[str],
) -> None:
    if not env.file_upload:
        log_safe("file_upload", enabled=False)
        return

    image_file_name = build_unique_file_name("image", "png")
    document_file_name = build_unique_file_name("document", "txt")
    waiter = await start_websocket_attachment_wait(
        client=bot_client,
        person_id=bot_person_id,
        chat_id=env.chat_id,
        expected_file_name=image_file_name,
    )
    try:
        await live_step(
            "file_upload_ws_connect",
            lambda: with_timeout(waiter.connected, env.ws_timeout_ms, "file_upload_ws_connect"),
        )
        await live_step(
            "file_upload_ws_subscribe",
            lambda: with_timeout(waiter.subscribed, env.ws_timeout_ms, "file_upload_ws_subscribe"),
        )

        image_upload = await live_step(
            "file_upload_image",
            lambda: owner_client.upload_file(
                env.chat_id,
                tiny_png_bytes(),
                image_file_name,
                "image/png",
            ),
        )
        assert_live(image_upload, "file_upload_image", owner_client)
        log_safe("file_upload_image", uploaded=True)

        received = await live_step(
            "file_upload_ws_receive",
            lambda: with_timeout(waiter.received, env.ws_timeout_ms, "file_upload_ws_receive"),
        )
        assert_live(has_attachment(received, image_file_name), "file_upload_ws_receive")
        log_safe("file_upload_ws_receive", ws_received=True)

        image_post = await live_step(
            "file_upload_image_bot_read",
            lambda: wait_for_attachment_post(bot_client, env.chat_id, image_file_name, image_upload, env.record_count),
        )
        assert_live(image_post, "file_upload_image_bot_read", bot_client)
        created_owner_post_ids.append(str(image_post.get("id")))
        log_safe("file_upload_image_bot_read", found=True)

        await assert_attachment_download_handoff(
            stage="file_upload_image_handoff",
            env=env,
            bot_client=bot_client,
            uploaded=image_upload,
            post=image_post,
        )

        document_upload = await live_step(
            "file_upload_document",
            lambda: owner_client.upload_file(
                env.chat_id,
                b"RingCentral live smoke document attachment\n",
                document_file_name,
                "text/plain",
            ),
        )
        assert_live(document_upload, "file_upload_document", owner_client)
        log_safe("file_upload_document", uploaded=True)

        document_post = await live_step(
            "file_upload_document_bot_read",
            lambda: wait_for_attachment_post(
                bot_client,
                env.chat_id,
                document_file_name,
                document_upload,
                env.record_count,
            ),
        )
        assert_live(document_post, "file_upload_document_bot_read", bot_client)
        created_owner_post_ids.append(str(document_post.get("id")))
        log_safe("file_upload_document_bot_read", found=True)

        await assert_attachment_download_handoff(
            stage="file_upload_document_handoff",
            env=env,
            bot_client=bot_client,
            uploaded=document_upload,
            post=document_post,
        )
    finally:
        await waiter.stop()


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


async def run_calendar_event_scenario(
    *,
    env: LiveEnv,
    owner_client: RingCentralClient,
    created_owner_event_ids: List[str],
) -> None:
    event_payload = build_calendar_event_payload("calendar-event")
    updated_payload = build_calendar_event_payload("calendar-event-updated")

    created = await live_step(
        "calendar_event_create",
        lambda: owner_client.create_event(env.chat_id, event_payload),
    )
    assert_live(isinstance(created, dict) and created.get("id"), "calendar_event_create", owner_client)
    event_id = str(created["id"])
    created_owner_event_ids.append(event_id)
    log_safe("calendar_event_create", created=True)

    listed = await live_step(
        "calendar_event_list",
        lambda: owner_client.list_events(env.chat_id, min(env.record_count, 50)),
    )
    assert_live(
        isinstance(listed, list) and any(str(event.get("id")) == event_id for event in listed),
        "calendar_event_list",
        owner_client,
    )
    log_safe("calendar_event_list", found=True)

    read = await live_step("calendar_event_get", lambda: owner_client.get_event(event_id))
    assert_live(isinstance(read, dict) and str(read.get("id")) == event_id, "calendar_event_get", owner_client)
    log_safe("calendar_event_get", found=True)

    async def update_and_read_event() -> Optional[Dict[str, Any]]:
        response = await owner_client.update_event(event_id, updated_payload)
        if isinstance(response, dict) and response.get("title") == updated_payload["title"]:
            return response
        return await owner_client.get_event(event_id)

    updated = await live_step("calendar_event_update", update_and_read_event)
    assert_live(
        isinstance(updated, dict)
        and str(updated.get("id")) == event_id
        and updated.get("title") == updated_payload["title"],
        "calendar_event_update",
        owner_client,
    )
    log_safe("calendar_event_update", updated=True)


async def run_adaptive_card_scenario(
    *,
    env: LiveEnv,
    bot_client: RingCentralClient,
    created_bot_adaptive_card_ids: List[str],
) -> None:
    initial_text = build_unique_text("adaptive-card")
    updated_text = build_unique_text("adaptive-card-updated")

    created = await live_step(
        "adaptive_card_create",
        lambda: bot_client.create_adaptive_card(env.chat_id, build_adaptive_card(initial_text)),
    )
    assert_live(isinstance(created, dict) and created.get("id"), "adaptive_card_create", bot_client)
    card_id = str(created["id"])
    created_bot_adaptive_card_ids.append(card_id)
    log_safe("adaptive_card_create", created=True)

    read = await live_step("adaptive_card_get", lambda: bot_client.get_adaptive_card(card_id))
    assert_live(isinstance(read, dict) and str(read.get("id")) == card_id, "adaptive_card_get", bot_client)
    log_safe("adaptive_card_get", found=True)

    updated = await live_step(
        "adaptive_card_update",
        lambda: bot_client.update_adaptive_card(card_id, build_adaptive_card(updated_text)),
    )
    assert_live(
        isinstance(updated, dict) and str(updated.get("id")) == card_id,
        "adaptive_card_update",
        bot_client,
    )
    log_safe("adaptive_card_update", updated=True)


async def run_note_scenario(
    *,
    env: LiveEnv,
    owner_client: RingCentralClient,
    created_owner_note_ids: List[str],
) -> None:
    title = build_unique_text("note-title")
    updated_title = build_unique_text("note-title-updated")
    body = f"<strong>{escape_html(title)}</strong>"
    updated_body = f"<strong>{escape_html(updated_title)}</strong>"

    created = await live_step(
        "note_create",
        lambda: owner_client.create_note(env.chat_id, {"title": title, "body": body}),
    )
    assert_live(isinstance(created, dict) and created.get("id"), "note_create", owner_client)
    note_id = str(created["id"])
    created_owner_note_ids.append(note_id)
    log_safe("note_create", created=True)

    read = await live_step("note_get", lambda: owner_client.get_note(note_id))
    assert_live(isinstance(read, dict) and str(read.get("id")) == note_id, "note_get", owner_client)
    log_safe("note_get", found=True)

    updated = await live_step(
        "note_update",
        lambda: owner_client.update_note(note_id, {"title": updated_title, "body": updated_body}),
    )
    assert_live(
        isinstance(updated, dict) and str(updated.get("id")) == note_id,
        "note_update",
        owner_client,
    )
    log_safe("note_update", updated=True)

    await live_step("note_publish", lambda: owner_client.publish_note(note_id))
    log_safe("note_publish", published=True)


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


async def wait_for_attachment_post(
    client: RingCentralClient,
    chat_id: str,
    file_name: str,
    uploaded: Any,
    record_count: int,
) -> Optional[Dict[str, Any]]:
    uploaded_post = normalize_uploaded_post(uploaded)
    if uploaded_post and uploaded_post.get("attachments"):
        return uploaded_post
    for _ in range(10):
        posts = await read_recent_posts(client, chat_id, record_count)
        found = next((post for post in posts if has_attachment(post, file_name)), None)
        if found:
            return found
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


def has_attachment(post: Dict[str, Any], expected_file_name: str) -> bool:
    attachments = post.get("attachments")
    if not isinstance(attachments, list):
        return False
    for attachment in attachments:
        if not isinstance(attachment, dict):
            continue
        names = [
            str(attachment.get("fileName") or "").strip(),
            str(attachment.get("name") or "").strip(),
        ]
        if expected_file_name in names:
            return True
        if not any(names) and (attachment.get("contentUri") or attachment.get("uri") or attachment.get("id")):
            return True
    return False


def normalize_uploaded_post(uploaded: Any) -> Optional[Dict[str, Any]]:
    if isinstance(uploaded, dict) and uploaded.get("id") and uploaded.get("groupId"):
        return uploaded
    return None


def extract_upload_attachments(uploaded: Any) -> Optional[List[Dict[str, Any]]]:
    if isinstance(uploaded, dict):
        attachments = uploaded.get("attachments")
        if isinstance(attachments, list) and attachments:
            return [item for item in attachments if isinstance(item, dict)]
        if uploaded.get("contentUri") or uploaded.get("uri"):
            return [uploaded]
    if isinstance(uploaded, list):
        attachments = [item for item in uploaded if isinstance(item, dict)]
        return attachments or None
    return None


async def assert_attachment_download_handoff(
    *,
    stage: str,
    env: LiveEnv,
    bot_client: RingCentralClient,
    uploaded: Any,
    post: Dict[str, Any],
) -> None:
    from gateway.config import PlatformConfig
    from ringcentral.adapter import RingCentralAdapter

    attachments = extract_upload_attachments(uploaded)
    if not attachments:
        raw = post.get("attachments")
        attachments = [item for item in raw if isinstance(item, dict)] if isinstance(raw, list) else []
    assert_live(attachments, stage)
    adapter = RingCentralAdapter(
        PlatformConfig(
            enabled=True,
            token=env.bot_token,
            extra={"attachment_max_bytes": 5 * 1024 * 1024},
        )
    )
    downloaded = await live_step(
        stage,
        lambda: adapter._download_attachment(attachments[0], bot_client),
    )
    assert_live(downloaded, stage, bot_client)
    log_safe(stage, media_payload=True)


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


async def start_websocket_attachment_wait(
    *,
    client: RingCentralClient,
    person_id: str,
    chat_id: str,
    expected_file_name: str,
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
        log_safe("file_upload_ws_state", **sanitize_diagnostic(event, details))
        if event == "ws_connected" and not connected.done():
            connected.set_result(None)
        elif event == "ws_subscription_confirmed" and not subscribed.done():
            subscribed.set_result(None)
        elif event == "ws_subscription_rejected":
            reject_pending(LiveSmokeFailure(status=read_status(details)))

    async def on_event(body: Dict[str, Any]) -> None:
        if (
            str(body.get("groupId") or "") == chat_id
            and has_attachment(body, expected_file_name)
            and not received.done()
        ):
            received.set_result(body)

    monitor = RingCentralWebSocket(
        client,
        on_event,
        own_person_id=person_id,
        filter_own_creator=True,
        on_state=on_state,
        label="live-bot-file-upload",
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


async def cleanup_events(
    *,
    cleanup: bool,
    owner_client: RingCentralClient,
    event_ids: List[str],
) -> None:
    if not cleanup:
        log_safe("calendar_event_cleanup", enabled=False)
        return

    cleanup_events_ok = True
    for event_id in reversed(event_ids):
        try:
            cleanup_events_ok = bool(await owner_client.delete_event(event_id)) and cleanup_events_ok
        except Exception:  # noqa: BLE001
            cleanup_events_ok = False
    log_safe("calendar_event_cleanup", cleanup_events=cleanup_events_ok)


async def cleanup_adaptive_cards(
    *,
    cleanup: bool,
    bot_client: RingCentralClient,
    adaptive_card_ids: List[str],
) -> None:
    if not cleanup:
        log_safe("adaptive_card_cleanup", enabled=False)
        return

    cleanup_adaptive_cards_ok = True
    for card_id in reversed(adaptive_card_ids):
        try:
            cleanup_adaptive_cards_ok = (
                bool(await bot_client.delete_adaptive_card(card_id)) and cleanup_adaptive_cards_ok
            )
        except Exception:  # noqa: BLE001
            cleanup_adaptive_cards_ok = False
    log_safe("adaptive_card_cleanup", cleanup_adaptive_cards=cleanup_adaptive_cards_ok)


async def cleanup_notes(
    *,
    cleanup: bool,
    owner_client: RingCentralClient,
    note_ids: List[str],
) -> None:
    if not cleanup:
        log_safe("note_cleanup", enabled=False)
        return

    cleanup_notes_ok = True
    for note_id in reversed(note_ids):
        try:
            cleanup_notes_ok = bool(await owner_client.delete_note(note_id)) and cleanup_notes_ok
        except Exception:  # noqa: BLE001
            cleanup_notes_ok = False
    log_safe("note_cleanup", cleanup_notes=cleanup_notes_ok)


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


def build_unique_file_name(label: str, extension: str) -> str:
    run_id = os.getenv("GITHUB_RUN_ID", "local")
    attempt = os.getenv("GITHUB_RUN_ATTEMPT", "1")
    return f"hermes-ringcentral-e2e-{label}-{run_id}-{attempt}-{int(time.time() * 1000)}.{extension}"


def tiny_png_bytes() -> bytes:
    import base64

    return base64.b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAFgwJ/lS0N2wAAAABJRU5ErkJggg=="
    )


def build_calendar_event_payload(label: str) -> Dict[str, Any]:
    marker = build_unique_text(label).split("\n")[0]
    start_time = time.time() + 60 * 60
    end_time = time.time() + 2 * 60 * 60
    return {
        "title": marker,
        "startTime": iso_timestamp(start_time),
        "endTime": iso_timestamp(end_time),
        "description": build_unique_text(f"{label}-description"),
    }


def build_adaptive_card(text: str) -> Dict[str, Any]:
    return {
        "type": "AdaptiveCard",
        "$schema": "https://adaptivecards.io/schemas/adaptive-card.json",
        "version": "1.3",
        "body": [{"type": "TextBlock", "text": text, "wrap": True}],
    }


def escape_html(value: str) -> str:
    return (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def iso_timestamp(epoch_seconds: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(epoch_seconds))


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
