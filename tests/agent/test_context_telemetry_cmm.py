"""Behavior tests for CMM startup and idle native telemetry."""

from __future__ import annotations

import json
import os
import time
from types import SimpleNamespace

import pytest

from agent.context_telemetry import (
    _heartbeat_interval_seconds,
    _write_idle_context_telemetry,
    emit_context_telemetry,
    emit_startup_context_telemetry,
)


def _agent(last_prompt_tokens=12_345, context_length=272_000):
    return SimpleNamespace(
        model="gpt-5.5",
        provider="openai-codex",
        base_url="https://example.invalid/v1",
        session_id="session-123",
        session_start=None,
        session_input_tokens=123,
        session_output_tokens=45,
        session_prompt_tokens=123,
        session_completion_tokens=45,
        session_total_tokens=168,
        session_api_calls=1,
        session_estimated_cost_usd=0.0,
        session_cost_status="unknown",
        session_cost_source="none",
        context_compressor=SimpleNamespace(
            last_prompt_tokens=last_prompt_tokens,
            context_length=context_length,
            compression_count=1,
        ),
    )


@pytest.fixture(autouse=True)
def _clear_runtime_telemetry_override(monkeypatch):
    for name in (
        "HERMES_CONTEXT_TELEMETRY_PATH",
        "CONTEXT_TARGET_BACKEND",
        "TMUX_SESSION",
        "CMUX_WORKSPACE_ID",
        "CMUX_SURFACE_ID",
        "CMM_TERMINAL_TITLE",
        "CMM_TERMINAL_TTY",
        "CONTEXT_TELEMETRY_MAX_AGE_SECONDS",
    ):
        monkeypatch.delenv(name, raising=False)


def test_startup_telemetry_is_native_pending_and_api_free(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "profiles" / "diggr-main"))
    path = tmp_path / "state" / "target-context.json"
    monkeypatch.setenv("HERMES_CONTEXT_TELEMETRY_PATH", str(path))
    monkeypatch.setenv("CONTEXT_TARGET_BACKEND", "tmux")
    monkeypatch.setenv("TMUX_SESSION", "diggr-wb-coding-coordinator")

    written = emit_startup_context_telemetry(
        session_id="startup-session",
        model="gpt-5.5",
        provider="openai-codex",
        config={
            "context": {
                "telemetry": {
                    "enabled": True,
                    "path": str(path),
                    "min_write_interval_seconds": 0,
                }
            }
        },
    )

    assert written is True
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["source_kind"] == "native_context_telemetry"
    assert data["session_id"] == "startup-session"
    assert data["pid"] == os.getpid()
    assert data["context"] == {
        "used_tokens": None,
        "max_tokens": None,
        "percent": None,
        "token_source": "unknown",
    }
    assert data["session_usage"]["api_calls"] == 0
    assert data["target_identity"] == {
        "backend": "tmux",
        "tmux_session": "diggr-wb-coding-coordinator",
    }


def test_runtime_path_override_wins_only_when_nonempty(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "profiles" / "diggr-main"))
    config_path = tmp_path / "state" / "profile-context.json"
    override_path = tmp_path / "state" / "target-context.json"
    config = {
        "context": {
            "telemetry": {
                "enabled": True,
                "path": str(config_path),
                "min_write_interval_seconds": 0,
            }
        }
    }

    monkeypatch.setenv("HERMES_CONTEXT_TELEMETRY_PATH", str(override_path))
    assert emit_context_telemetry(_agent(), config=config, force=True) is True
    assert override_path.exists()
    assert not config_path.exists()

    monkeypatch.setenv("HERMES_CONTEXT_TELEMETRY_PATH", "")
    assert emit_context_telemetry(_agent(), config=config, force=True) is True
    assert config_path.exists()


def test_idle_heartbeat_refreshes_owner_without_losing_identity(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "profiles" / "diggr-main"))
    path = tmp_path / "state" / "target-context.json"
    monkeypatch.setenv("HERMES_CONTEXT_TELEMETRY_PATH", str(path))
    monkeypatch.setenv("CONTEXT_TARGET_BACKEND", "tmux")
    monkeypatch.setenv("TMUX_SESSION", "diggr-wb-coding-coordinator")
    config = {
        "context": {
            "telemetry": {
                "enabled": True,
                "path": str(path),
                "min_write_interval_seconds": 0,
            }
        }
    }
    agent = _agent(last_prompt_tokens=42_000, context_length=100_000)

    assert emit_context_telemetry(agent, config=config, force=True) is True
    first = json.loads(path.read_text(encoding="utf-8"))
    stale_mtime = time.time() - 30
    os.utime(path, (stale_mtime, stale_mtime))

    assert _write_idle_context_telemetry(
        agent,
        config=config,
        min_write_interval_seconds=0,
    ) is True
    refreshed = json.loads(path.read_text(encoding="utf-8"))
    assert refreshed["context"] == first["context"]
    assert refreshed["target_identity"] == {
        "backend": "tmux",
        "tmux_session": "diggr-wb-coding-coordinator",
    }
    assert os.stat(path).st_mtime > time.time() - 5


def test_idle_heartbeat_interval_tracks_cmm_max_age(monkeypatch):
    monkeypatch.setenv("CONTEXT_TELEMETRY_MAX_AGE_SECONDS", "10")

    assert _heartbeat_interval_seconds({}) == 5.0
