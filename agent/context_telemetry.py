"""Hermes-native machine-readable context telemetry.

This module emits a small local JSON artifact for external operator UIs that
need current context-window occupancy without scraping terminal text. It never
includes prompt/message content or credentials.
"""
from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping

from hermes_constants import get_hermes_home

_SCHEMA_VERSION = 1
_LAST_WRITE: dict[str, tuple[float, str]] = {}
_HEARTBEATS: dict[str, "_ContextTelemetryHeartbeat"] = {}
_HEARTBEATS_LOCK = threading.Lock()
_DEFAULT_IDLE_HEARTBEAT_SECONDS = 60.0


class _ContextTelemetryHeartbeat:
    """Refresh target-scoped telemetry while the owning Hermes process idles."""

    def __init__(self, path: str) -> None:
        self.path = path
        self.agent: Any = None
        self.config: Mapping[str, Any] | None = None
        self.interval = _DEFAULT_IDLE_HEARTBEAT_SECONDS
        self.min_write_interval = 0.0
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def update(
        self,
        *,
        agent: Any,
        config: Mapping[str, Any] | None,
        interval: float,
        min_write_interval: float,
    ) -> None:
        with self._lock:
            self.agent = agent
            self.config = config
            self.interval = max(1.0, float(interval))
            self.min_write_interval = max(0.0, float(min_write_interval))
            if self._thread is None:
                self._thread = threading.Thread(
                    target=self._run,
                    name="context-telemetry-heartbeat",
                    daemon=True,
                )
                self._thread.start()

    def _run(self) -> None:
        while not self._stop.wait(self.interval):
            with self._lock:
                agent = self.agent
                config = self.config
                min_write_interval = self.min_write_interval
            if agent is None:
                continue
            _write_idle_context_telemetry(
                agent,
                config=config,
                min_write_interval_seconds=min_write_interval,
            )


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on", "enabled"}


def _number(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return default


def _float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        text = str(value).strip()
        if not text:
            return default
        return float(text)
    except (TypeError, ValueError):
        return default


def _profile_name() -> str:
    home = get_hermes_home()
    parts = home.parts
    if len(parts) >= 2 and parts[-2] == "profiles":
        return parts[-1]
    return os.environ.get("HERMES_PROFILE") or "default"


def _clean_env_text(name: str) -> str:
    return str(os.environ.get(name) or "").strip()


def _target_identity_from_env() -> dict[str, str] | None:
    """Return target-scoped CMM terminal identity from wrapper env, if present."""
    if not _clean_env_text("HERMES_CONTEXT_TELEMETRY_PATH"):
        return None

    tmux_session = _clean_env_text("TMUX_SESSION")
    cmux_workspace_id = _clean_env_text("CMUX_WORKSPACE_ID")
    cmux_surface_id = _clean_env_text("CMUX_SURFACE_ID")
    backend = _clean_env_text("CONTEXT_TARGET_BACKEND")
    if backend in {"", "auto"}:
        if cmux_surface_id or cmux_workspace_id:
            backend = "cmux"
        elif tmux_session:
            backend = "tmux"
        else:
            backend = ""

    identity = {
        "backend": backend,
        "tmux_session": tmux_session,
        "cmux_workspace_id": cmux_workspace_id,
        "cmux_surface_id": cmux_surface_id,
        "terminal_title": _clean_env_text("CMM_TERMINAL_TITLE"),
        "terminal_tty": _clean_env_text("CMM_TERMINAL_TTY"),
    }
    identity = {key: value for key, value in identity.items() if value}
    if not any(
        identity.get(key)
        for key in ("backend", "tmux_session", "cmux_workspace_id", "cmux_surface_id")
    ):
        return None
    return identity


def _telemetry_config(config: Mapping[str, Any] | None = None) -> dict[str, Any]:
    if config is None:
        try:
            from hermes_cli.config import load_config

            config = load_config() or {}
        except Exception:
            config = {}
    context_cfg = config.get("context", {}) if isinstance(config, Mapping) else {}
    telemetry = context_cfg.get("telemetry", {}) if isinstance(context_cfg, Mapping) else {}
    if not isinstance(telemetry, Mapping):
        return {}
    cfg = dict(telemetry)
    runtime_path = os.environ.get("HERMES_CONTEXT_TELEMETRY_PATH", "").strip()
    if runtime_path:
        cfg["path"] = runtime_path
    return cfg


def telemetry_enabled(config: Mapping[str, Any] | None = None) -> bool:
    cfg = _telemetry_config(config)
    return _truthy(cfg.get("enabled")) and bool(str(cfg.get("path") or "").strip())


def build_context_telemetry_payload(
    agent: Any,
    *,
    usage: Mapping[str, Any] | None = None,
    snapshot: Mapping[str, Any] | None = None,
    config: Mapping[str, Any] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a content-free JSON-serializable context telemetry payload."""
    cfg = _telemetry_config(config)
    model = str(
        (snapshot or {}).get("model_name")
        or (usage or {}).get("model")
        or getattr(agent, "model", "")
        or ""
    )
    provider = str(getattr(agent, "provider", "") or "")
    session_id = str(getattr(agent, "session_id", "") or "")

    used = _number((snapshot or {}).get("context_tokens"), 0)
    max_tokens = _number((snapshot or {}).get("context_length"), 0)
    percent_value: Any = (snapshot or {}).get("context_percent")
    token_source = "unknown"

    if usage:
        used = _number(usage.get("context_used"), used)
        max_tokens = _number(usage.get("context_max"), max_tokens)
        percent_value = usage.get("context_percent", percent_value)

    compressor = getattr(agent, "context_compressor", None)
    if compressor is not None:
        raw_prompt = getattr(compressor, "last_prompt_tokens", None)
        raw_context_len = getattr(compressor, "context_length", None)
        prompt_tokens = _number(raw_prompt, 0)
        context_length = _number(raw_context_len, 0)
        if prompt_tokens < 0:
            prompt_tokens = 0
        if prompt_tokens and context_length:
            used = prompt_tokens
            max_tokens = context_length
            percent_value = max(0, min(100, round((used / max_tokens) * 100)))
            token_source = "actual"
        elif context_length and not max_tokens:
            max_tokens = context_length

    if token_source == "unknown" and used and max_tokens:
        token_source = "actual"
    percent: int | None
    if token_source == "unknown" or not max_tokens:
        percent = None
    else:
        percent = _number(percent_value, max(0, min(100, round((used / max_tokens) * 100))))
        percent = max(0, min(100, percent))

    session_usage = {
        "input_tokens": _number((snapshot or {}).get("session_input_tokens"), _number((usage or {}).get("input"), _number(getattr(agent, "session_input_tokens", 0)))),
        "output_tokens": _number((snapshot or {}).get("session_output_tokens"), _number((usage or {}).get("output"), _number(getattr(agent, "session_output_tokens", 0)))),
        "reasoning_tokens": _number((usage or {}).get("reasoning"), _number(getattr(agent, "session_reasoning_tokens", 0))),
        "prompt_tokens": _number((snapshot or {}).get("session_prompt_tokens"), _number((usage or {}).get("prompt"), _number(getattr(agent, "session_prompt_tokens", 0)))),
        "completion_tokens": _number((snapshot or {}).get("session_completion_tokens"), _number((usage or {}).get("completion"), _number(getattr(agent, "session_completion_tokens", 0)))),
        "total_tokens": _number((snapshot or {}).get("session_total_tokens"), _number((usage or {}).get("total"), _number(getattr(agent, "session_total_tokens", 0)))),
        "api_calls": _number((snapshot or {}).get("session_api_calls"), _number((usage or {}).get("calls"), _number(getattr(agent, "session_api_calls", 0)))),
        "compressions": _number((snapshot or {}).get("compressions"), _number((usage or {}).get("compressions"), _number(getattr(compressor, "compression_count", 0) if compressor else 0))),
    }

    if now is None:
        now = datetime.now(timezone.utc)
    thresholds = {
        "smart_zone_tokens": _number(cfg.get("smart_zone_tokens"), 120000),
        "smart_zone_context_pct": _number(cfg.get("smart_zone_context_pct"), 45),
        "compression_threshold": _float(cfg.get("compression_threshold"), _float((config or {}).get("compression", {}).get("threshold") if isinstance((config or {}).get("compression"), Mapping) else None, 0.85)),
    }
    payload = {
        "schema_version": _SCHEMA_VERSION,
        "source": "hermes_runtime",
        "source_kind": "native_context_telemetry",
        "profile": _profile_name(),
        "session_id": session_id,
        "model": model,
        "provider": provider,
        "pid": os.getpid(),
        "updated_at": now.isoformat().replace("+00:00", "Z"),
        "context": {
            "used_tokens": used if token_source != "unknown" else None,
            "max_tokens": max_tokens or None,
            "percent": percent,
            "token_source": token_source,
        },
        "session_usage": session_usage,
        "thresholds": thresholds,
    }
    target_identity = _target_identity_from_env()
    if target_identity:
        payload["target_identity"] = target_identity
    return payload


def write_context_telemetry(
    payload: Mapping[str, Any],
    path: str | Path,
    *,
    min_write_interval_seconds: float = 1.0,
    force: bool = False,
) -> bool:
    """Atomically write telemetry JSON when changed or the interval elapsed."""
    target = Path(path).expanduser()
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    now = time.monotonic()
    key = str(target)
    last = _LAST_WRITE.get(key)
    if not force and last is not None:
        last_at, last_blob = last
        if blob == last_blob and now - last_at < max(0.0, float(min_write_interval_seconds)):
            return False
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    pretty = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    tmp.write_text(pretty, encoding="utf-8")
    os.replace(tmp, target)
    _LAST_WRITE[key] = (now, blob)
    return True


def _heartbeat_interval_seconds(cfg: Mapping[str, Any]) -> float:
    explicit = _float(
        os.environ.get("HERMES_CONTEXT_TELEMETRY_IDLE_HEARTBEAT_SECONDS")
        or cfg.get("idle_heartbeat_seconds")
        or cfg.get("heartbeat_interval_seconds"),
        0.0,
    )
    if explicit > 0:
        return explicit
    max_age = _float(os.environ.get("CONTEXT_TELEMETRY_MAX_AGE_SECONDS"), 0.0)
    if max_age > 0:
        return max(1.0, min(_DEFAULT_IDLE_HEARTBEAT_SECONDS, max_age / 2.0))
    return _DEFAULT_IDLE_HEARTBEAT_SECONDS


def _write_idle_context_telemetry(
    agent: Any,
    *,
    config: Mapping[str, Any] | None = None,
    min_write_interval_seconds: float = 0.0,
) -> bool:
    """Rewrite the current payload timestamp from this live Hermes process."""
    cfg = _telemetry_config(config)
    path = str(cfg.get("path") or "").strip()
    if not (_truthy(cfg.get("enabled")) and path):
        return False
    try:
        payload = build_context_telemetry_payload(agent, config=config)
        return write_context_telemetry(
            payload,
            path,
            min_write_interval_seconds=min_write_interval_seconds,
            force=True,
        )
    except Exception:
        return False


def _ensure_idle_context_telemetry_heartbeat(
    agent: Any,
    *,
    config: Mapping[str, Any] | None,
    cfg: Mapping[str, Any],
) -> None:
    path = str(cfg.get("path") or "").strip()
    if not path or not _truthy(cfg.get("enabled")):
        return
    if _truthy(
        cfg.get("idle_heartbeat_enabled")
        if "idle_heartbeat_enabled" in cfg
        else True
    ) is False:
        return
    interval = _heartbeat_interval_seconds(cfg)
    min_write_interval = min(
        interval,
        _float(cfg.get("min_write_interval_seconds"), 1.0),
    )
    with _HEARTBEATS_LOCK:
        heartbeat = _HEARTBEATS.get(path)
        if heartbeat is None:
            heartbeat = _ContextTelemetryHeartbeat(path)
            _HEARTBEATS[path] = heartbeat
        heartbeat.update(
            agent=agent,
            config=config,
            interval=interval,
            min_write_interval=min_write_interval,
        )


def emit_context_telemetry(
    agent: Any,
    *,
    usage: Mapping[str, Any] | None = None,
    snapshot: Mapping[str, Any] | None = None,
    config: Mapping[str, Any] | None = None,
    force: bool = False,
) -> bool:
    """Best-effort telemetry emission; never raises into the UI/runtime path."""
    cfg = _telemetry_config(config)
    if not (_truthy(cfg.get("enabled")) and str(cfg.get("path") or "").strip()):
        return False
    try:
        payload = build_context_telemetry_payload(agent, usage=usage, snapshot=snapshot, config=config)
        return write_context_telemetry(
            payload,
            cfg["path"],
            min_write_interval_seconds=_float(cfg.get("min_write_interval_seconds"), 1.0),
            force=force,
        )
    except Exception:
        return False
    finally:
        try:
            _ensure_idle_context_telemetry_heartbeat(agent, config=config, cfg=cfg)
        except Exception:
            pass


def emit_startup_context_telemetry(
    *,
    session_id: str,
    model: str = "",
    provider: str = "",
    config: Mapping[str, Any] | None = None,
) -> bool:
    """Publish native pending telemetry before the lazy agent is initialized."""
    startup_agent = SimpleNamespace(
        session_id=str(session_id or ""),
        model=str(model or ""),
        provider=str(provider or ""),
        session_input_tokens=0,
        session_output_tokens=0,
        session_reasoning_tokens=0,
        session_prompt_tokens=0,
        session_completion_tokens=0,
        session_total_tokens=0,
        session_api_calls=0,
        context_compressor=None,
    )
    return emit_context_telemetry(startup_agent, config=config, force=True)
