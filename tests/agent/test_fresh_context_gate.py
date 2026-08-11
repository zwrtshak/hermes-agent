"""Authority tests for the synchronous Hermes-core Fresh Context gate."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from agent.fresh_context_gate import (
    DEFAULT_THRESHOLD_CONTEXT_PCT,
    DEFAULT_THRESHOLD_TOKENS,
    ROLLOVER_REQUEST_ENV,
    ROLLOVER_REQUEST_SCHEMA,
    evaluate_fresh_context_gate,
    request_fresh_context_rollover,
    resolve_fresh_context_gate_policy,
)
from run_agent import AIAgent


@pytest.fixture(autouse=True)
def _clear_rollover_request_env(monkeypatch):
    monkeypatch.delenv(ROLLOVER_REQUEST_ENV, raising=False)


def _tool_defs() -> list[dict]:
    return [
        {
            "type": "function",
            "function": {
                "name": "web_search",
                "description": "test tool",
                "parameters": {"type": "object", "properties": {}},
            },
        }
    ]


def _response(content: str = "OK") -> SimpleNamespace:
    message = SimpleNamespace(content=content, tool_calls=None)
    choice = SimpleNamespace(message=message, finish_reason="stop")
    return SimpleNamespace(choices=[choice], model="test/model", usage=None)


def _tool_response() -> SimpleNamespace:
    tool_call = SimpleNamespace(
        id="call-1",
        type="function",
        function=SimpleNamespace(name="web_search", arguments="{}"),
    )
    message = SimpleNamespace(content="", tool_calls=[tool_call])
    choice = SimpleNamespace(message=message, finish_reason="tool_calls")
    return SimpleNamespace(choices=[choice], model="test/model", usage=None)


@pytest.fixture()
def agent(monkeypatch, tmp_path):
    hermes_home = tmp_path / "hermes-home"
    monkeypatch.setenv("HERMES_HOME", str(hermes_home))
    with (
        patch("run_agent._hermes_home", hermes_home),
        patch("run_agent.get_tool_definitions", return_value=_tool_defs()),
        patch("run_agent.check_toolset_requirements", return_value={}),
        patch("run_agent.OpenAI"),
        patch("hermes_cli.config.load_config", return_value={}),
    ):
        value = AIAgent(
            api_key="test-key-1234567890",
            base_url="https://openrouter.ai/api/v1",
            quiet_mode=True,
            skip_context_files=True,
            skip_memory=True,
        )
    value.client = MagicMock()
    value._cached_system_prompt = "You are helpful."
    value._use_prompt_caching = False
    value.tool_delay = 0
    value.save_trajectories = False
    value.context_compressor.context_length = 272_000
    return value


def test_policy_defaults_are_disabled_with_120k_and_45_percent():
    policy = resolve_fresh_context_gate_policy({})

    assert policy.enabled is False
    assert policy.threshold_tokens == DEFAULT_THRESHOLD_TOKENS == 120_000
    assert policy.threshold_context_pct == DEFAULT_THRESHOLD_CONTEXT_PCT == 45


def test_panel_thresholds_override_profile_values_and_refresh_live(tmp_path):
    panel = tmp_path / "config.env"
    panel.write_text(
        "SMART_ZONE_TOKENS=100000\n"
        "SMART_ZONE_CONTEXT_PCT=40\n"
        "SECRET_SHOULD_NOT_BE_READ=value\n",
        encoding="utf-8",
    )
    raw = {
        "enabled": True,
        "threshold_tokens": 120_000,
        "threshold_context_pct": 45,
        "panel_config_path": str(panel),
    }

    first = resolve_fresh_context_gate_policy(raw)
    panel.write_text(
        "SMART_ZONE_TOKENS=110000\nSMART_ZONE_CONTEXT_PCT=42\n",
        encoding="utf-8",
    )
    second = resolve_fresh_context_gate_policy(raw)

    assert (first.threshold_tokens, first.threshold_context_pct) == (100_000, 40)
    assert (second.threshold_tokens, second.threshold_context_pct) == (110_000, 42)


def test_rollover_request_path_can_come_from_profile_or_allowlisted_panel_key(
    tmp_path,
):
    profile_request = tmp_path / "profile-request.json"
    panel_request = tmp_path / "panel-request.json"
    panel = tmp_path / "config.env"
    panel.write_text(
        f'FRESH_CONTEXT_ROLLOVER_REQUEST_FILE="{panel_request}"\n',
        encoding="utf-8",
    )

    direct = resolve_fresh_context_gate_policy(
        {"rollover_request_path": str(profile_request)}
    )
    overridden = resolve_fresh_context_gate_policy(
        {
            "rollover_request_path": str(profile_request),
            "panel_config_path": str(panel),
        }
    )

    assert direct.rollover_request_path == str(profile_request)
    assert overridden.rollover_request_path == str(panel_request)


def test_soft_rollover_contract_is_loaded_only_from_allowlisted_panel_keys(tmp_path):
    marker = tmp_path / "context-switch.once"
    prefill = tmp_path / "prefill.json"
    state = tmp_path / "transition.json"
    panel = tmp_path / "config.env"
    panel.write_text(
        "CMM_SOFT_ROLLOVER_ENABLED=1\n"
        f'MARKER_FILE="{marker}"\n'
        f'CMM_SOFT_ROLLOVER_PREFILL_FILE="{prefill}"\n'
        f'TRANSITION_STATE_FILE="{state}"\n'
        "UNRELATED_SECRET=must-not-be-read\n",
        encoding="utf-8",
    )

    policy = resolve_fresh_context_gate_policy(
        {"enabled": True, "panel_config_path": str(panel)}
    )

    assert policy.soft_rollover_enabled is True
    assert policy.soft_rollover_marker_path == str(marker)
    assert policy.soft_rollover_prefill_path == str(prefill)
    assert policy.soft_rollover_state_path == str(state)
    assert "must-not-be-read" not in repr(policy)


def test_process_env_rollover_path_overrides_profile_and_panel(monkeypatch, tmp_path):
    profile_request = tmp_path / "profile-request.json"
    panel_request = tmp_path / "panel-request.json"
    process_request = tmp_path / "process-request.json"
    panel = tmp_path / "config.env"
    panel.write_text(
        f'FRESH_CONTEXT_ROLLOVER_REQUEST_FILE="{panel_request}"\n',
        encoding="utf-8",
    )
    monkeypatch.setenv(ROLLOVER_REQUEST_ENV, str(process_request))

    policy = resolve_fresh_context_gate_policy(
        {
            "rollover_request_path": str(profile_request),
            "panel_config_path": str(panel),
        }
    )

    assert policy.rollover_request_path == str(process_request)
    assert policy.panel_config_path == str(panel)


def test_process_env_enables_request_without_changing_panel_path(
    monkeypatch, tmp_path
):
    request_path = tmp_path / "process-request.json"
    panel = tmp_path / "base-config.env"
    panel.write_text("SMART_ZONE_TOKENS=120000\n", encoding="utf-8")
    monkeypatch.setenv(ROLLOVER_REQUEST_ENV, str(request_path))
    policy = resolve_fresh_context_gate_policy(
        {
            "enabled": True,
            "panel_config_path": str(panel),
        }
    )
    decision = evaluate_fresh_context_gate(
        policy,
        prompt_tokens=120_000,
        context_length=272_000,
        token_source="estimated",
    )

    result = request_fresh_context_rollover(
        policy,
        decision,
        session_id="session-env",
        turn_id="turn-env",
    )

    assert result.requested is True
    assert policy.panel_config_path == str(panel)
    payload = json.loads(request_path.read_text(encoding="utf-8"))
    assert payload["token_source"] == "estimated"
    assert payload["prompt_tokens_estimate"] == 120_000
    assert "prompt_tokens_actual" not in payload
    assert "actual_prompt_tokens" not in payload


def test_invalid_panel_values_do_not_replace_valid_profile_thresholds(tmp_path):
    panel = tmp_path / "config.env"
    panel.write_text(
        "SMART_ZONE_TOKENS=invalid\nSMART_ZONE_CONTEXT_PCT=101\n",
        encoding="utf-8",
    )

    policy = resolve_fresh_context_gate_policy(
        {
            "enabled": True,
            "threshold_tokens": 130_000,
            "threshold_context_pct": 50,
            "panel_config_path": str(panel),
        }
    )

    assert policy.threshold_tokens == 130_000
    assert policy.threshold_context_pct == 50


def test_exact_119999_120000_token_boundary_when_pct_is_below_threshold():
    policy = resolve_fresh_context_gate_policy(
        {
            "enabled": True,
            "threshold_tokens": 120_000,
            "threshold_context_pct": 45,
        }
    )

    below = evaluate_fresh_context_gate(
        policy, prompt_tokens=119_999, context_length=272_000
    )
    boundary = evaluate_fresh_context_gate(
        policy, prompt_tokens=120_000, context_length=272_000
    )

    assert below.context_pct < 45
    assert below.should_block is False
    assert boundary.context_pct < 45
    assert boundary.should_block is True
    assert boundary.reason == "token_threshold"


def test_context_percentage_boundary_uses_inclusive_or_semantics():
    policy = resolve_fresh_context_gate_policy(
        {
            "enabled": True,
            "threshold_tokens": 999_999,
            "threshold_context_pct": 45,
        }
    )

    decision = evaluate_fresh_context_gate(
        policy, prompt_tokens=45_000, context_length=100_000
    )

    assert decision.should_block is True
    assert decision.reason == "context_pct_threshold"


def test_request_file_is_atomic_machine_readable_and_not_actual_telemetry(tmp_path):
    request_path = tmp_path / "rollover.json"
    policy = resolve_fresh_context_gate_policy(
        {
            "enabled": True,
            "threshold_tokens": 120_000,
            "threshold_context_pct": 45,
            "rollover_request_path": str(request_path),
        }
    )
    decision = evaluate_fresh_context_gate(
        policy,
        prompt_tokens=120_000,
        context_length=272_000,
        token_source="estimated",
    )

    result = request_fresh_context_rollover(
        policy,
        decision,
        session_id="session-1",
        turn_id="turn-1",
    )

    assert result.requested is True
    payload = json.loads(request_path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == ROLLOVER_REQUEST_SCHEMA
    assert payload["status"] == "requested"
    assert payload["prompt_tokens_estimate"] == 120_000
    assert payload["token_source"] == "estimated"
    assert payload["external_authorization_required"] is True
    assert payload["native_compression_requested"] is False
    assert request_path.stat().st_mode & 0o777 == 0o600


def test_pre_provider_request_records_gate_position_and_api_call_index(tmp_path):
    request_path = tmp_path / "rollover.json"
    policy = resolve_fresh_context_gate_policy(
        {"enabled": True, "rollover_request_path": str(request_path)}
    )
    decision = evaluate_fresh_context_gate(
        policy, prompt_tokens=131_000, context_length=272_000
    )

    result = request_fresh_context_rollover(
        policy,
        decision,
        session_id="session-1",
        turn_id="turn-1",
        gate_position="pre_provider_call",
        api_call_index=2,
    )

    assert result.requested is True
    payload = json.loads(request_path.read_text(encoding="utf-8"))
    assert payload["gate_position"] == "pre_provider_call"
    assert payload["api_call_index"] == 2


@pytest.mark.parametrize(
    ("raw_path", "expected_reason"),
    [
        (None, "request_path_not_configured"),
        ("relative/request.json", "request_path_not_absolute"),
        ("/tmp/not-json.txt", "request_path_not_json"),
    ],
)
def test_invalid_request_paths_fail_closed(raw_path, expected_reason):
    policy = resolve_fresh_context_gate_policy(
        {"enabled": True, "rollover_request_path": raw_path}
    )
    decision = evaluate_fresh_context_gate(
        policy,
        prompt_tokens=120_000,
        context_length=272_000,
    )

    result = request_fresh_context_rollover(
        policy,
        decision,
        session_id="session-1",
        turn_id="turn-1",
    )

    assert result.requested is False
    assert result.reason == expected_reason


def test_unwritable_request_path_fails_closed(tmp_path):
    request_path = tmp_path / "rollover.json"
    policy = resolve_fresh_context_gate_policy(
        {"enabled": True, "rollover_request_path": str(request_path)}
    )
    decision = evaluate_fresh_context_gate(
        policy,
        prompt_tokens=120_000,
        context_length=272_000,
    )

    with patch("agent.fresh_context_gate.os.link", side_effect=PermissionError):
        result = request_fresh_context_rollover(
            policy,
            decision,
            session_id="session-1",
            turn_id="turn-1",
        )

    assert result.requested is False
    assert result.reason == "request_write_or_verification_failed"
    assert request_path.exists() is False
    assert not list(tmp_path.glob(".*.tmp"))


def test_below_threshold_does_not_request_block_or_compress(agent, tmp_path):
    request_path = tmp_path / "rollover.json"
    agent.compression_enabled = True
    agent._fresh_context_gate_config = {
        "enabled": True,
        "threshold_tokens": 120_000,
        "threshold_context_pct": 45,
        "rollover_request_path": str(request_path),
    }
    agent.context_compressor.should_compress = MagicMock(return_value=False)
    agent._compress_context = MagicMock()
    agent.client.chat.completions.create.return_value = _response("below threshold")

    with (
        patch("agent.turn_context.estimate_request_tokens_rough", return_value=119_999),
        patch("hermes_cli.plugins.invoke_hook", return_value=[]),
        patch("hermes_cli.plugins.has_hook", return_value=False),
        patch.object(agent, "_persist_session"),
        patch.object(agent, "_save_trajectory"),
        patch.object(agent, "_cleanup_task_resources"),
    ):
        result = agent.run_conversation("continue")

    assert result["final_response"] == "below threshold"
    assert request_path.exists() is False
    agent._compress_context.assert_not_called()
    agent.client.chat.completions.create.assert_called_once()


def test_soft_rollover_retry_skips_gate_once_then_rearms(agent, tmp_path):
    request_path = tmp_path / "rollover.json"
    agent.compression_enabled = True
    agent._fresh_context_gate_config = {
        "enabled": True,
        "threshold_tokens": 1,
        "threshold_context_pct": 1,
        "rollover_request_path": str(request_path),
    }
    agent._fresh_context_gate_skip_once = True
    agent.context_compressor.should_compress = MagicMock(return_value=False)
    agent.client.chat.completions.create.return_value = _response("fresh retry")

    with (
        patch("agent.turn_context.estimate_request_tokens_rough", return_value=120_000),
        patch("hermes_cli.plugins.invoke_hook", return_value=[]),
        patch("hermes_cli.plugins.has_hook", return_value=False),
        patch.object(agent, "_persist_session"),
        patch.object(agent, "_save_trajectory"),
        patch.object(agent, "_cleanup_task_resources"),
    ):
        result = agent.run_conversation("retry after rollover")

    assert result["final_response"] == "fresh retry"
    assert request_path.exists() is False
    assert agent._fresh_context_gate_skip_once is False
    agent.client.chat.completions.create.assert_called_once()


def test_same_turn_tool_growth_is_blocked_before_second_provider_call(
    agent, tmp_path
):
    request_path = tmp_path / "rollover.json"
    agent._fresh_context_gate_config = {
        "enabled": True,
        "threshold_tokens": 120_000,
        "threshold_context_pct": 45,
        "rollover_request_path": str(request_path),
    }
    agent.context_compressor.should_compress = MagicMock(return_value=False)
    agent.client.chat.completions.create.return_value = _tool_response()
    executed = MagicMock()

    def execute_tool(_assistant_message, messages, *_args):
        executed()
        messages.append(
            {
                "role": "tool",
                "tool_call_id": "call-1",
                "name": "web_search",
                "content": "large tool result already completed",
            }
        )

    with (
        patch("agent.turn_context.estimate_request_tokens_rough", return_value=100_000),
        patch(
            "agent.conversation_loop.estimate_messages_tokens_rough",
            side_effect=lambda messages: (
                131_000
                if any(message.get("role") == "tool" for message in messages)
                else 100_000
            ),
        ),
        patch("agent.conversation_loop._estimate_tools_tokens_rough", return_value=0),
        patch("hermes_cli.plugins.invoke_hook", return_value=[]),
        patch("hermes_cli.plugins.has_hook", return_value=False),
        patch.object(agent, "_execute_tool_calls", side_effect=execute_tool),
        patch.object(agent, "_persist_session"),
        patch.object(agent, "_flush_messages_to_session_db"),
        patch.object(agent, "_save_trajectory"),
        patch.object(agent, "_cleanup_task_resources"),
    ):
        result = agent.run_conversation("finish the long task")

    assert result["failed"] is False
    assert result["rollover_requested"] is True
    assert result["rollover_mid_turn"] is True
    assert result["turn_exit_reason"] == "fresh_context_rollover_requested_mid_turn"
    assert result["api_calls"] == 1
    assert "Do not repeat completed tool calls" in result["rollover_continuation_message"]
    assert executed.call_count == 1
    agent.client.chat.completions.create.assert_called_once()
    payload = json.loads(request_path.read_text(encoding="utf-8"))
    assert payload["gate_position"] == "pre_provider_call"
    assert payload["api_call_index"] == 2
    assert payload["prompt_tokens_estimate"] == 131_000


def test_legacy_auto_continuation_flag_cannot_block_next_context_request(
    agent, tmp_path
):
    request_path = tmp_path / "rollover.json"
    agent._fresh_context_gate_config = {
        "enabled": True,
        "threshold_tokens": 120_000,
        "threshold_context_pct": 45,
        "rollover_request_path": str(request_path),
    }
    agent._fresh_context_auto_rollover_blocked = True
    agent.context_compressor.should_compress = MagicMock(return_value=False)

    with (
        patch("agent.turn_context.estimate_request_tokens_rough", return_value=120_000),
        patch("hermes_cli.plugins.invoke_hook", return_value=[]),
        patch("hermes_cli.plugins.has_hook", return_value=False),
        patch.object(agent, "_persist_session"),
    ):
        result = agent.run_conversation("do not loop this task again")

    assert result["failed"] is False
    assert result["turn_exit_reason"] == "fresh_context_rollover_requested"
    assert result["rollover_requested"] is True
    assert request_path.exists() is True
    agent.client.chat.completions.create.assert_not_called()


def test_legacy_auto_continuation_flag_cannot_block_mid_turn_context_request(
    agent, tmp_path
):
    request_path = tmp_path / "rollover.json"
    agent._fresh_context_gate_config = {
        "enabled": True,
        "threshold_tokens": 120_000,
        "threshold_context_pct": 45,
        "rollover_request_path": str(request_path),
    }
    agent._fresh_context_auto_rollover_blocked = True
    agent.context_compressor.should_compress = MagicMock(return_value=False)
    agent.client.chat.completions.create.return_value = _tool_response()
    executed = MagicMock()

    def execute_tool(_assistant_message, messages, *_args):
        executed()
        messages.append(
            {
                "role": "tool",
                "tool_call_id": "call-1",
                "name": "web_search",
                "content": "completed once",
            }
        )

    with (
        patch("agent.turn_context.estimate_request_tokens_rough", return_value=100_000),
        patch(
            "agent.conversation_loop.estimate_messages_tokens_rough",
            side_effect=lambda messages: (
                131_000
                if any(message.get("role") == "tool" for message in messages)
                else 100_000
            ),
        ),
        patch("agent.conversation_loop._estimate_tools_tokens_rough", return_value=0),
        patch("hermes_cli.plugins.invoke_hook", return_value=[]),
        patch("hermes_cli.plugins.has_hook", return_value=False),
        patch.object(agent, "_execute_tool_calls", side_effect=execute_tool),
        patch.object(agent, "_persist_session"),
        patch.object(agent, "_flush_messages_to_session_db"),
        patch.object(agent, "_save_trajectory"),
        patch.object(agent, "_cleanup_task_resources"),
    ):
        result = agent.run_conversation("finish once and return to prompt")

    assert result["failed"] is False
    assert result["turn_exit_reason"] == "fresh_context_rollover_requested_mid_turn"
    assert result["rollover_requested"] is True
    assert result["api_calls"] == 1
    assert executed.call_count == 1
    assert request_path.exists() is True
    agent.client.chat.completions.create.assert_called_once()


def test_gate_requests_rollover_before_compression_hook_and_provider_request(
    agent, tmp_path
):
    request_path = tmp_path / "rollover.json"
    agent.compression_enabled = True
    # The 120k orderly gate and 0.85 emergency compressor are deliberately
    # independent. Even if native compression would otherwise be eligible,
    # the configured rollover request wins at the earlier boundary.
    agent.context_compressor.threshold_percent = 0.85
    agent.context_compressor.threshold_tokens = 110_000
    agent._fresh_context_gate_config = {
        "enabled": True,
        "threshold_tokens": 120_000,
        "threshold_context_pct": 45,
        "rollover_request_path": str(request_path),
    }
    agent._compress_context = MagicMock()
    hook = MagicMock(return_value=[])

    with (
        patch("agent.turn_context.estimate_request_tokens_rough", return_value=120_000),
        patch("hermes_cli.plugins.invoke_hook", hook),
        patch("hermes_cli.plugins.has_hook", return_value=True) as has_hook,
        patch.object(agent, "_persist_session"),
    ):
        result = agent.run_conversation("continue")

    assert result["failed"] is False
    assert result["rollover_requested"] is True
    assert result["turn_exit_reason"] == "fresh_context_rollover_requested"
    assert result["api_calls"] == 0
    assert "rollover requested" in result["final_response"]
    assert json.loads(request_path.read_text())["token_source"] == "estimated"
    agent._compress_context.assert_not_called()
    hook.assert_not_called()
    has_hook.assert_not_called()
    agent.client.chat.completions.create.assert_not_called()


def test_gate_fails_closed_before_compression_hook_and_provider_without_request_path(
    agent,
):
    agent.compression_enabled = True
    agent._fresh_context_gate_config = {
        "enabled": True,
        "threshold_tokens": 120_000,
        "threshold_context_pct": 45,
    }
    agent._compress_context = MagicMock()
    hook = MagicMock(return_value=[])

    with (
        patch("agent.turn_context.estimate_request_tokens_rough", return_value=120_000),
        patch("hermes_cli.plugins.invoke_hook", hook),
        patch("hermes_cli.plugins.has_hook", return_value=True) as has_hook,
        patch.object(agent, "_persist_session"),
    ):
        result = agent.run_conversation("continue")

    assert result["failed"] is True
    assert result["error"] == "fresh_context_gate_blocked"
    assert result["api_calls"] == 0
    assert "No native compression ran for this turn" in result["final_response"]
    assert "no API request was sent" in result["final_response"]
    agent._compress_context.assert_not_called()
    hook.assert_not_called()
    has_hook.assert_not_called()
    agent.client.chat.completions.create.assert_not_called()


def test_disabled_gate_preserves_existing_native_preflight_compression(agent):
    agent.compression_enabled = True
    agent._fresh_context_gate_config = {
        "enabled": False,
        "threshold_tokens": 120_000,
        "threshold_context_pct": 45,
    }
    agent.context_compressor.should_compress = MagicMock(return_value=True)
    agent._compress_context = MagicMock(
        side_effect=lambda messages, system_message, **kwargs: (
            messages,
            "You are helpful.",
        )
    )
    agent.client.chat.completions.create.return_value = _response("existing path")

    with (
        patch("agent.turn_context._should_run_preflight_estimate", return_value=True),
        patch("agent.turn_context.estimate_request_tokens_rough", return_value=999_999),
        patch("hermes_cli.plugins.invoke_hook", return_value=[]),
        patch("hermes_cli.plugins.has_hook", return_value=False),
        patch.object(agent, "_persist_session"),
        patch.object(agent, "_save_trajectory"),
        patch.object(agent, "_cleanup_task_resources"),
    ):
        result = agent.run_conversation("continue")

    assert result["final_response"] == "existing path"
    agent._compress_context.assert_called_once()
    agent.client.chat.completions.create.assert_called_once()
