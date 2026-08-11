"""Behavior tests for invisible in-process Fresh Context rollover."""

from __future__ import annotations

import hashlib
import json
import queue
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import cli as cli_mod
from cli import HermesCLI


def _bundle(
    tmp_path: Path,
    session_id: str = "session-old",
    *,
    progress_changed: bool = True,
    progress_repeat_count: int = 0,
):
    handoff = tmp_path / "handoff.md"
    marker = tmp_path / "context-switch.once"
    prefill = tmp_path / "prefill.json"
    state = tmp_path / "transition.json"
    handoff.write_text("# Validated handoff\n", encoding="utf-8")
    marker.write_text(f"{handoff}\n", encoding="utf-8")
    messages = [
        {
            "role": "user",
            "content": f"CMM invisible continuation context. Continue from {handoff}",
        }
    ]
    prefill.write_text(json.dumps(messages) + "\n", encoding="utf-8")
    state.write_text(
        json.dumps(
            {
                "status": "prefill_prepared",
                "source_session_id": session_id,
                "handoff_path": str(handoff),
                "handoff_sha256": hashlib.sha256(handoff.read_bytes()).hexdigest(),
                "handoff_smoke": "passed",
                "prefill_sha256": hashlib.sha256(prefill.read_bytes()).hexdigest(),
                "task_progress_changed": progress_changed,
                "task_progress_repeat_count": progress_repeat_count,
            }
        ),
        encoding="utf-8",
    )
    policy = SimpleNamespace(
        soft_rollover_enabled=True,
        soft_rollover_marker_path=str(marker),
        soft_rollover_prefill_path=str(prefill),
        soft_rollover_state_path=str(state),
        soft_rollover_timeout_seconds=1,
    )
    return policy, handoff, marker, prefill, state, messages


def test_soft_rollover_bundle_requires_hash_bound_handoff_and_prefill(tmp_path):
    policy, _handoff, _marker, prefill, _state, messages = _bundle(tmp_path)

    bundle, reason = cli_mod._load_soft_rollover_bundle(policy, "session-old")

    assert reason == "soft_rollover_bundle_ready"
    assert bundle is not None
    assert bundle["messages"] == messages

    prefill.write_text("[]\n", encoding="utf-8")
    bundle, reason = cli_mod._load_soft_rollover_bundle(policy, "session-old")
    assert bundle is None
    assert reason == "soft_rollover_prefill_integrity_failed"


def test_soft_rollover_allows_multiple_automatic_continuations_for_same_task(tmp_path):
    policy, _handoff, marker, prefill, _state, messages = _bundle(tmp_path)
    cli_obj = HermesCLI.__new__(HermesCLI)
    cli_obj.session_id = "session-old"
    cli_obj.agent = SimpleNamespace(_fresh_context_gate_config={}, prefill_messages=[])
    cli_obj._pending_input = queue.Queue()
    cli_obj._session_db = None

    calls = []

    session_ids = iter(("session-new", "session-next"))

    def rotate(**kwargs):
        calls.append(kwargs)
        cli_obj.session_id = next(session_ids)

    cli_obj.new_session = rotate
    with patch(
        "agent.fresh_context_gate.resolve_fresh_context_gate_policy",
        return_value=policy,
    ):
        ok, reason = cli_obj._complete_soft_fresh_context_rollover(
            "continue unfinished work without repeating tools"
        )

    assert (ok, reason) == (True, "soft_rollover_completed")
    assert calls == [
        {"silent": True, "title": None, "parent_session_id": "session-old"}
    ]
    assert cli_obj.agent.prefill_messages == messages
    assert cli_obj.agent._fresh_context_gate_skip_once is True
    queued = cli_obj._pending_input.get_nowait()
    assert queued.message == "continue unfinished work without repeating tools"
    assert queued.images == ()
    assert queued.task_id == cli_obj._fresh_context_task_id
    assert cli_obj._fresh_context_auto_continuations == 1
    assert not marker.exists()
    assert not prefill.exists()

    second_policy, _handoff, second_marker, second_prefill, _state, _messages = (
        _bundle(tmp_path, session_id="session-new")
    )
    with patch(
        "agent.fresh_context_gate.resolve_fresh_context_gate_policy",
        return_value=second_policy,
    ):
        second_ok, second_reason = cli_obj._complete_soft_fresh_context_rollover(
            "continue the same unfinished task again"
        )
    assert (second_ok, second_reason) == (True, "soft_rollover_completed")
    assert calls[-1] == {
        "silent": True,
        "title": None,
        "parent_session_id": "session-new",
    }
    second_queued = cli_obj._pending_input.get_nowait()
    assert second_queued.message == "continue the same unfinished task again"
    assert second_queued.task_id == queued.task_id
    assert cli_obj._fresh_context_auto_continuations == 2
    assert not second_marker.exists()
    assert not second_prefill.exists()


def test_soft_rollover_repeated_state_queues_fresh_context_finalization(tmp_path):
    policy, _handoff, _marker, _prefill, _state, _messages = _bundle(
        tmp_path,
        progress_changed=False,
        progress_repeat_count=2,
    )
    cli_obj = HermesCLI.__new__(HermesCLI)
    cli_obj.session_id = "session-old"
    cli_obj.agent = SimpleNamespace(_fresh_context_gate_config={}, prefill_messages=[])
    cli_obj._pending_input = queue.Queue()
    cli_obj._session_db = None

    def rotate(**_kwargs):
        cli_obj.session_id = "session-new"

    cli_obj.new_session = rotate
    with patch(
        "agent.fresh_context_gate.resolve_fresh_context_gate_policy",
        return_value=policy,
    ):
        ok, reason = cli_obj._complete_soft_fresh_context_rollover(
            "must be replaced by finalization"
        )

    assert (ok, reason) == (True, "soft_rollover_stall_finalization_queued")
    queued = cli_obj._pending_input.get_nowait()
    assert "Do not use tools" in queued.message
    assert "repeated across two context transitions" in queued.message


def test_ready_bundle_left_after_previous_turn_is_consumed_before_next_provider_turn(tmp_path):
    policy, _handoff, marker, prefill, _state, messages = _bundle(tmp_path)
    cli_obj = HermesCLI.__new__(HermesCLI)
    cli_obj.session_id = "session-old"
    cli_obj.agent = SimpleNamespace(_fresh_context_gate_config={}, prefill_messages=[])
    cli_obj._pending_input = queue.Queue()
    cli_obj._session_db = None

    calls = []

    def rotate(**kwargs):
        calls.append(kwargs)
        cli_obj.session_id = "session-new"

    cli_obj.new_session = rotate
    with patch(
        "agent.fresh_context_gate.resolve_fresh_context_gate_policy",
        return_value=policy,
    ):
        ok, reason = cli_obj._consume_prepared_soft_fresh_context_rollover(
            "next user task",
            continuation_images=[Path("screenshot.png")],
        )

    assert (ok, reason) == (True, "soft_rollover_completed")
    assert calls == [
        {"silent": True, "title": None, "parent_session_id": "session-old"}
    ]
    assert cli_obj.agent.prefill_messages == messages
    assert cli_obj.agent._fresh_context_gate_skip_once is True
    queued = cli_obj._pending_input.get_nowait()
    assert queued.message == "next user task"
    assert queued.images == (Path("screenshot.png"),)
    assert queued.task_id == cli_obj._fresh_context_task_id
    assert not marker.exists()
    assert not prefill.exists()


def test_task_scope_keeps_identity_across_internal_continuations_and_resets_for_user_task():
    cli_obj = HermesCLI.__new__(HermesCLI)
    cli_obj.agent = SimpleNamespace()
    cli_obj._fresh_context_task_id = "task-one"
    cli_obj._fresh_context_auto_continuations = 1

    ok, reason = cli_obj._bind_fresh_context_task_scope(
        internal=True,
        task_id="task-one",
    )
    assert (ok, reason) == (True, "fresh_context_continuation_bound")
    assert cli_obj._fresh_context_auto_continuations == 1

    mismatch = cli_obj._bind_fresh_context_task_scope(
        internal=True,
        task_id="different-task",
    )
    assert mismatch == (False, "fresh_context_task_id_mismatch")
    assert cli_obj._fresh_context_task_id == "task-one"
    assert cli_obj._fresh_context_auto_continuations == 1

    reset = cli_obj._bind_fresh_context_task_scope(internal=False)
    assert reset == (True, "fresh_context_new_task_bound")
    assert cli_obj._fresh_context_task_id != "task-one"
    assert cli_obj._fresh_context_auto_continuations == 0


def test_restart_bootstrap_is_queued_as_private_continuation_not_terminal_input():
    cli_obj = HermesCLI.__new__(HermesCLI)
    cli_obj._pending_input = queue.Queue()
    cli_obj._fresh_context_bootstrap_internal_pending = True
    cli_obj._fresh_context_task_id = "task-one"
    query = (
        "DIRECTOR_CONTINUATION: CMM fresh-session handoff is ready: "
        "/tmp/validated-handoff.md"
    )

    with patch.dict("os.environ", {"HERMES_TUI_QUERY": query}):
        queued = cli_obj._queue_hidden_fresh_context_bootstrap()

    assert queued is True
    payload = cli_obj._pending_input.get_nowait()
    assert isinstance(payload, cli_mod._FreshContextContinuationInput)
    assert payload.message == query
    assert payload.task_id == "task-one"
    assert payload.images == ()
    assert cli_obj._fresh_context_bootstrap_internal_pending is False


def test_private_continuation_never_renders_as_submitted_user_message():
    cli_obj = HermesCLI.__new__(HermesCLI)
    cli_obj._print_user_message_preview = SimpleNamespace()

    with (
        patch.object(cli_obj, "_print_user_message_preview") as preview,
        patch("builtins.print") as plain_print,
        patch.object(cli_mod, "_cprint") as cprint,
    ):
        cli_obj._print_submitted_input_preview(
            "Continue the current task from the validated CMM handoff.",
            [],
            internal=True,
        )

    preview.assert_not_called()
    plain_print.assert_not_called()
    cprint.assert_not_called()
