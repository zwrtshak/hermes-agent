"""Provider-free qualification of automatic Fresh Context rollover.

These tests run the production ``AIAgent.run_conversation`` turn path against
an in-memory scripted provider.  The provider has a hard request budget, so a
turn that should stop at the Fresh Context gate fails immediately if any wire
method is reached.
"""

from __future__ import annotations

import hashlib
import json
import queue
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import cli as cli_mod
from agent.fresh_context_gate import (
    ROLLOVER_REQUEST_ENV,
    ROLLOVER_REQUEST_SCHEMA,
    resolve_fresh_context_gate_policy,
)
from cli import HermesCLI
from run_agent import AIAgent
from tests.fakes.scripted_provider import (
    ScriptedProvider,
    scripted_text_response,
)


PRIVATE_PROMPT = "PRIVATE-CMM-CONTENT-MUST-NOT-LEAVE-THE-TURN"


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


@pytest.fixture()
def make_agent(monkeypatch, tmp_path):
    hermes_home = tmp_path / "hermes-home"
    (hermes_home / "logs").mkdir(parents=True)
    monkeypatch.setenv("HERMES_HOME", str(hermes_home))

    def factory(provider: ScriptedProvider, *, session_id: str) -> AIAgent:
        with (
            patch("run_agent._hermes_home", hermes_home),
            patch("run_agent.get_tool_definitions", return_value=_tool_defs()),
            patch("run_agent.check_toolset_requirements", return_value={}),
            patch("run_agent.OpenAI"),
            patch("hermes_cli.config.load_config", return_value={}),
            patch(
                "agent.context_compressor.get_model_context_length",
                return_value=272_000,
            ),
        ):
            agent = AIAgent(
                api_key="scripted-provider-no-network",
                api_mode="chat_completions",
                base_url="https://scripted.invalid/v1",
                provider="custom",
                session_id=session_id,
                quiet_mode=True,
                skip_context_files=True,
                skip_memory=True,
                skip_background_review=True,
            )
        agent.client = provider.client
        agent._create_request_openai_client = provider.create_request_client
        agent._cached_system_prompt = "You are helpful."
        agent._use_prompt_caching = False
        agent.save_trajectories = False
        agent.context_compressor.context_length = 272_000
        agent.context_compressor.should_compress = MagicMock(return_value=False)
        agent._compress_context = MagicMock(
            side_effect=AssertionError(
                "native compression is forbidden in this harness"
            )
        )
        return agent

    return factory


@contextmanager
def _turn_runtime(agent: AIAgent, *, estimated_tokens: int):
    with (
        patch(
            "agent.turn_context.estimate_request_tokens_rough",
            return_value=estimated_tokens,
        ),
        patch("hermes_cli.plugins.invoke_hook", return_value=[]),
        patch("hermes_cli.plugins.has_hook", return_value=False),
        patch.object(agent, "_persist_session"),
        patch.object(agent, "_flush_messages_to_session_db"),
        patch.object(agent, "_save_trajectory"),
        patch.object(agent, "_cleanup_task_resources"),
    ):
        yield


def _gate_config(tmp_path: Path) -> tuple[dict, dict[str, Path]]:
    paths = {
        "request": tmp_path / "rollover-request.json",
        "marker": tmp_path / "context-switch.once",
        "prefill": tmp_path / "prefill.json",
        "state": tmp_path / "transition.json",
    }
    config = {
        "enabled": True,
        "threshold_tokens": 120_000,
        "threshold_context_pct": 45,
        "rollover_request_path": str(paths["request"]),
        "soft_rollover": True,
        "soft_rollover_marker_path": str(paths["marker"]),
        "soft_rollover_prefill_path": str(paths["prefill"]),
        "soft_rollover_state_path": str(paths["state"]),
        "soft_rollover_timeout_seconds": 1,
    }
    return config, paths


def _assert_content_free_request(path: Path, *, session_id: str) -> dict:
    raw = path.read_text(encoding="utf-8")
    payload = json.loads(raw)
    assert set(payload) == {
        "schema_version",
        "status",
        "requested_at",
        "session_id",
        "turn_id",
        "gate_position",
        "gate_reason",
        "prompt_tokens_estimate",
        "token_source",
        "context_length",
        "context_percent_estimate",
        "threshold_tokens",
        "threshold_context_percent",
        "native_compression_requested",
        "external_authorization_required",
    }
    assert payload["schema_version"] == ROLLOVER_REQUEST_SCHEMA
    assert payload["status"] == "requested"
    assert payload["session_id"] == session_id
    assert payload["turn_id"]
    assert payload["gate_position"] == "turn_start"
    assert payload["gate_reason"] == "token_threshold"
    assert payload["prompt_tokens_estimate"] == 120_000
    assert payload["token_source"] == "estimated"
    assert payload["context_length"] == 272_000
    assert payload["threshold_tokens"] == 120_000
    assert payload["threshold_context_percent"] == 45
    assert payload["native_compression_requested"] is False
    assert payload["external_authorization_required"] is True
    assert PRIVATE_PROMPT not in raw
    assert not ({"message", "messages", "content", "prompt"} & payload.keys())
    return payload


def _prepare_validated_bundle(
    tmp_path: Path,
    paths: dict[str, Path],
    *,
    cycle: int,
    source_session_id: str,
) -> list[dict]:
    """Model the external owner's fail-closed, hash-bound handoff output."""

    request = json.loads(paths["request"].read_text(encoding="utf-8"))
    assert request["schema_version"] == ROLLOVER_REQUEST_SCHEMA
    assert request["session_id"] == source_session_id
    for guarded in (paths["marker"], paths["prefill"]):
        if guarded.exists() or guarded.is_symlink():
            raise FileExistsError(f"refusing to overwrite stale {guarded.name}")

    handoff = tmp_path / f"handoff-cycle-{cycle}.md"
    handoff.write_text(
        f"# Validated scripted handoff {cycle}\nTask progress changed.\n",
        encoding="utf-8",
    )
    messages = [
        {
            "role": "user",
            "content": (
                "CMM invisible continuation context. Continue from "
                f"{handoff}"
            ),
        }
    ]
    paths["prefill"].write_text(json.dumps(messages) + "\n", encoding="utf-8")
    paths["marker"].write_text(f"{handoff}\n", encoding="utf-8")
    paths["state"].write_text(
        json.dumps(
            {
                "status": "prefill_prepared",
                "source_session_id": source_session_id,
                "handoff_path": str(handoff),
                "handoff_sha256": hashlib.sha256(handoff.read_bytes()).hexdigest(),
                "handoff_smoke": "passed",
                "prefill_sha256": hashlib.sha256(
                    paths["prefill"].read_bytes()
                ).hexdigest(),
                "task_progress_changed": True,
                "task_progress_repeat_count": 0,
            }
        ),
        encoding="utf-8",
    )
    return messages


def _assert_bundle_ready_then_consume_request(
    config: dict,
    paths: dict[str, Path],
    *,
    source_session_id: str,
) -> None:
    policy = resolve_fresh_context_gate_policy(config)
    bundle, reason = cli_mod._load_soft_rollover_bundle(policy, source_session_id)
    assert reason == "soft_rollover_bundle_ready"
    assert bundle is not None
    # The scripted external owner clears the request only after the production
    # loader has verified source identity, handoff smoke, and both hashes.
    paths["request"].unlink()


def _minimal_cli(agent: AIAgent, *, session_id: str) -> tuple[HermesCLI, list[dict]]:
    cli = HermesCLI.__new__(HermesCLI)
    cli.session_id = session_id
    cli.agent = agent
    cli._pending_input = queue.Queue()
    cli._session_db = None
    cli._fresh_context_task_id = "scripted-task"
    cli._fresh_context_auto_continuations = 0
    rotations: list[dict] = []

    def rotate(**kwargs):
        rotations.append(kwargs)
        new_session_id = f"session-fresh-{len(rotations)}"
        cli.session_id = new_session_id
        agent.session_id = new_session_id

    cli.new_session = rotate
    return cli, rotations


def test_scripted_provider_budget_vetoes_every_wire_method():
    for method in ("chat", "responses"):
        provider = ScriptedProvider.veto()
        with pytest.raises(
            AssertionError,
            match="provider call budget exceeded",
        ):
            if method == "chat":
                provider.client.chat.completions.create(model="scripted")
            else:
                provider.client.responses.create(model="scripted")

        assert provider.attempt_count == 1
        assert provider.calls[0].method.endswith(".create")


def test_threshold_vetoes_provider_and_compression_with_content_free_request(
    make_agent,
    tmp_path,
):
    provider = ScriptedProvider.veto()
    agent = make_agent(provider, session_id="session-old")
    config, paths = _gate_config(tmp_path)
    agent._fresh_context_gate_config = config

    with _turn_runtime(agent, estimated_tokens=120_000):
        result = agent.run_conversation(PRIVATE_PROMPT, task_id="scripted-task")

    assert result["failed"] is False
    assert result["rollover_requested"] is True
    assert result["turn_exit_reason"] == "fresh_context_rollover_requested"
    assert result["api_calls"] == 0
    assert provider.attempt_count == 0
    agent._compress_context.assert_not_called()
    agent.context_compressor.should_compress.assert_not_called()
    _assert_content_free_request(paths["request"], session_id="session-old")


def test_two_scripted_rollovers_preserve_task_and_do_not_duplicate_work(
    make_agent,
    tmp_path,
):
    provider = ScriptedProvider(
        [
            scripted_text_response("cycle one progressed"),
            scripted_text_response("cycle two completed"),
        ]
    )
    agent = make_agent(provider, session_id="session-old")
    config, paths = _gate_config(tmp_path)
    agent._fresh_context_gate_config = config
    cli, rotations = _minimal_cli(agent, session_id="session-old")

    with _turn_runtime(agent, estimated_tokens=120_000):
        first_gate = agent.run_conversation(
            PRIVATE_PROMPT,
            conversation_history=[],
            task_id="scripted-task",
        )
    assert first_gate["rollover_requested"] is True
    assert provider.attempt_count == 0
    first_request = paths["request"].read_bytes()

    # A stale request must block the next threshold turn. It is neither
    # overwritten nor allowed to fall through to compression/provider I/O.
    with _turn_runtime(agent, estimated_tokens=120_000):
        stale_gate = agent.run_conversation(
            "retry while request is still stale",
            conversation_history=[],
            task_id="scripted-task",
        )
    assert stale_gate["failed"] is True
    assert stale_gate["error"] == "fresh_context_gate_blocked"
    assert paths["request"].read_bytes() == first_request
    assert provider.attempt_count == 0

    paths["marker"].write_text("stale marker must not be overwritten\n")
    with pytest.raises(FileExistsError, match="stale context-switch.once"):
        _prepare_validated_bundle(
            tmp_path,
            paths,
            cycle=1,
            source_session_id="session-old",
        )
    assert paths["request"].read_bytes() == first_request
    paths["marker"].unlink()

    first_prefill = _prepare_validated_bundle(
        tmp_path,
        paths,
        cycle=1,
        source_session_id="session-old",
    )
    _assert_bundle_ready_then_consume_request(
        config,
        paths,
        source_session_id="session-old",
    )
    first_ok, first_reason = cli._complete_soft_fresh_context_rollover(
        "continue cycle one without repeating work"
    )
    assert (first_ok, first_reason) == (True, "soft_rollover_completed")
    first_queued = cli._pending_input.get_nowait()
    assert first_queued.task_id == "scripted-task"
    assert agent.prefill_messages == first_prefill
    assert not paths["marker"].exists()
    assert not paths["prefill"].exists()
    assert paths["state"].exists()

    with _turn_runtime(agent, estimated_tokens=120_000):
        first_continuation = agent.run_conversation(
            first_queued.message,
            conversation_history=[],
            task_id=first_queued.task_id,
        )
    assert first_continuation["final_response"] == "cycle one progressed"
    assert provider.attempt_count == 1
    first_wire_messages = provider.calls[0].kwargs["messages"]
    assert first_prefill[0] in first_wire_messages

    with _turn_runtime(agent, estimated_tokens=120_000):
        second_gate = agent.run_conversation(
            "continue into cycle two",
            conversation_history=[],
            task_id=first_queued.task_id,
        )
    assert second_gate["rollover_requested"] is True
    assert provider.attempt_count == 1
    _assert_content_free_request(
        paths["request"],
        session_id="session-fresh-1",
    )

    second_prefill = _prepare_validated_bundle(
        tmp_path,
        paths,
        cycle=2,
        source_session_id="session-fresh-1",
    )
    _assert_bundle_ready_then_consume_request(
        config,
        paths,
        source_session_id="session-fresh-1",
    )
    second_ok, second_reason = cli._complete_soft_fresh_context_rollover(
        "continue cycle two without repeating work"
    )
    assert (second_ok, second_reason) == (True, "soft_rollover_completed")
    second_queued = cli._pending_input.get_nowait()
    assert second_queued.task_id == first_queued.task_id == "scripted-task"
    assert agent.prefill_messages == second_prefill

    with _turn_runtime(agent, estimated_tokens=120_000):
        second_continuation = agent.run_conversation(
            second_queued.message,
            conversation_history=[],
            task_id=second_queued.task_id,
        )
    assert second_continuation["final_response"] == "cycle two completed"
    provider.assert_exhausted()
    assert provider.attempt_count == 2
    assert [call.method for call in provider.calls] == [
        "chat.completions.create",
        "chat.completions.create",
    ]
    assert rotations == [
        {"silent": True, "title": None, "parent_session_id": "session-old"},
        {
            "silent": True,
            "title": None,
            "parent_session_id": "session-fresh-1",
        },
    ]
    assert cli._fresh_context_auto_continuations == 2
    assert cli._pending_input.empty()
    assert not paths["request"].exists()
    assert not paths["marker"].exists()
    assert not paths["prefill"].exists()
    assert paths["state"].exists()


def test_below_threshold_reaches_scripted_provider_once(make_agent, tmp_path):
    provider = ScriptedProvider([scripted_text_response("below threshold")])
    agent = make_agent(provider, session_id="session-below")
    config, paths = _gate_config(tmp_path)
    agent._fresh_context_gate_config = config

    with _turn_runtime(agent, estimated_tokens=119_999):
        result = agent.run_conversation(
            "ordinary below-boundary turn",
            conversation_history=[],
            task_id="scripted-task",
        )

    assert result["final_response"] == "below threshold"
    provider.assert_exhausted()
    assert provider.attempt_count == 1
    assert provider.calls[0].method == "chat.completions.create"
    assert not paths["request"].exists()
    agent._compress_context.assert_not_called()
