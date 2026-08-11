"""Synchronous Hermes-core Fresh Context gate policy and decision helpers.

The gateway may rotate a session before constructing an ``AIAgent``, but not
every Hermes surface goes through the gateway.  This module owns the narrow,
content-free decision used by the core turn prologue immediately before native
preflight compression and before any provider request can be constructed.
"""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional


DEFAULT_THRESHOLD_TOKENS = 120_000
DEFAULT_THRESHOLD_CONTEXT_PCT = 45.0
ROLLOVER_REQUEST_SCHEMA = "hermes.fresh_context_rollover_request.v1"
ROLLOVER_REQUEST_ENV = "FRESH_CONTEXT_ROLLOVER_REQUEST_FILE"


@dataclass(frozen=True)
class FreshContextGatePolicy:
    """Resolved per-turn policy for the Hermes-core Fresh Context gate."""

    enabled: bool = False
    threshold_tokens: int = DEFAULT_THRESHOLD_TOKENS
    threshold_context_pct: float = DEFAULT_THRESHOLD_CONTEXT_PCT
    panel_config_path: Optional[str] = None
    rollover_request_path: Optional[str] = None
    soft_rollover_enabled: bool = False
    soft_rollover_marker_path: Optional[str] = None
    soft_rollover_prefill_path: Optional[str] = None
    soft_rollover_state_path: Optional[str] = None
    soft_rollover_timeout_seconds: int = 15
    emergency_compression_fallback: bool = True


@dataclass(frozen=True)
class FreshContextGateDecision:
    """Content-free result of evaluating the core gate."""

    should_block: bool
    reason: str
    prompt_tokens: int
    token_source: str
    context_length: int
    context_pct: float
    threshold_tokens: int
    threshold_context_pct: float


@dataclass(frozen=True)
class FreshContextRolloverRequest:
    """Result of producing the configured external rollover request."""

    requested: bool
    reason: str
    path: Optional[str] = None


def _bool_value(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "on"}:
            return True
        if normalized in {"false", "0", "no", "off"}:
            return False
        return default
    return bool(value)


def _positive_int(value: Any, default: int) -> int:
    if value is None or isinstance(value, bool):
        return default
    try:
        parsed = int(str(value).strip(), 10)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _context_pct(value: Any, default: float) -> float:
    if value is None or isinstance(value, bool):
        return default
    try:
        parsed = float(str(value).strip())
    except (TypeError, ValueError):
        return default
    return parsed if 0 < parsed <= 100 else default


def _panel_threshold_overrides(path: Optional[str]) -> dict[str, Any]:
    """Read only allow-listed gate keys from a KEY=VALUE panel file."""
    if not path:
        return {}
    try:
        panel_path = Path(path).expanduser()
        if not panel_path.is_file():
            return {}
        overrides: dict[str, Any] = {}
        for raw_line in panel_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key == "SMART_ZONE_TOKENS":
                parsed_tokens = _positive_int(value, 0)
                if parsed_tokens > 0:
                    overrides["threshold_tokens"] = parsed_tokens
            elif key == "SMART_ZONE_CONTEXT_PCT":
                parsed_pct = _context_pct(value, 0)
                if parsed_pct > 0:
                    overrides["threshold_context_pct"] = parsed_pct
            elif key == "FRESH_CONTEXT_ROLLOVER_REQUEST_FILE" and value:
                overrides["rollover_request_path"] = value
            elif key == "CMM_SOFT_ROLLOVER_ENABLED":
                overrides["soft_rollover_enabled"] = _bool_value(value, False)
            elif key == "MARKER_FILE" and value:
                overrides["soft_rollover_marker_path"] = value
            elif key == "CMM_SOFT_ROLLOVER_PREFILL_FILE" and value:
                overrides["soft_rollover_prefill_path"] = value
            elif key == "TRANSITION_STATE_FILE" and value:
                overrides["soft_rollover_state_path"] = value
        return overrides
    except (OSError, UnicodeError):
        return {}


def fresh_context_gate_config(config: Mapping[str, Any] | None) -> dict[str, Any]:
    """Extract a narrow, copy-safe policy section from profile config."""
    if not isinstance(config, Mapping):
        return {}
    context = config.get("context")
    if not isinstance(context, Mapping):
        return {}
    raw = context.get("fresh_session_before_compression")
    return dict(raw) if isinstance(raw, Mapping) else {}


def resolve_fresh_context_gate_policy(
    raw_config: Mapping[str, Any] | None,
) -> FreshContextGatePolicy:
    """Resolve config defaults plus live panel and process overrides.

    ``raw_config`` is the already-extracted
    ``context.fresh_session_before_compression`` mapping.  The panel file is
    intentionally reread for every turn so an operator threshold change takes
    effect without rebuilding a long-lived agent.
    """
    raw = raw_config if isinstance(raw_config, Mapping) else {}
    panel_value = raw.get("panel_config_path")
    panel_path = str(panel_value).strip() if panel_value else None
    overrides = _panel_threshold_overrides(panel_path)
    process_request_path = os.environ.get(ROLLOVER_REQUEST_ENV, "").strip()
    return FreshContextGatePolicy(
        enabled=_bool_value(raw.get("enabled"), False),
        threshold_tokens=overrides.get(
            "threshold_tokens",
            _positive_int(raw.get("threshold_tokens"), DEFAULT_THRESHOLD_TOKENS),
        ),
        threshold_context_pct=overrides.get(
            "threshold_context_pct",
            _context_pct(
                raw.get("threshold_context_pct"),
                DEFAULT_THRESHOLD_CONTEXT_PCT,
            ),
        ),
        panel_config_path=panel_path,
        rollover_request_path=process_request_path
        or overrides.get(
            "rollover_request_path",
            str(raw.get("rollover_request_path") or "").strip() or None,
        ),
        soft_rollover_enabled=overrides.get(
            "soft_rollover_enabled",
            _bool_value(raw.get("soft_rollover"), False),
        ),
        soft_rollover_marker_path=overrides.get(
            "soft_rollover_marker_path",
            str(raw.get("soft_rollover_marker_path") or "").strip() or None,
        ),
        soft_rollover_prefill_path=overrides.get(
            "soft_rollover_prefill_path",
            str(raw.get("soft_rollover_prefill_path") or "").strip() or None,
        ),
        soft_rollover_state_path=overrides.get(
            "soft_rollover_state_path",
            str(raw.get("soft_rollover_state_path") or "").strip() or None,
        ),
        soft_rollover_timeout_seconds=_positive_int(
            raw.get("soft_rollover_timeout_seconds"), 15
        ),
        emergency_compression_fallback=_bool_value(
            raw.get("emergency_compression_fallback"), True
        ),
    )


def request_fresh_context_rollover(
    policy: FreshContextGatePolicy,
    decision: FreshContextGateDecision,
    *,
    session_id: str,
    turn_id: str,
    gate_position: str = "turn_start",
    api_call_index: Optional[int] = None,
) -> FreshContextRolloverRequest:
    """Atomically create a content-free request for an external rollover owner.

    This file is a request, never transition authorization.  In particular,
    the estimated prompt count is labelled as an estimate and must not be
    consumed as provider-confirmed telemetry by an external transition guard.
    The configured parent directory must already exist; Hermes neither creates
    wrapper-owned directories nor overwrites an existing request.
    """
    raw_path = str(policy.rollover_request_path or "").strip()
    if not raw_path:
        return FreshContextRolloverRequest(False, "request_path_not_configured")

    request_path = Path(raw_path).expanduser()
    if not request_path.is_absolute():
        return FreshContextRolloverRequest(False, "request_path_not_absolute")
    if request_path.suffix.lower() != ".json":
        return FreshContextRolloverRequest(False, "request_path_not_json")
    if request_path.exists() or request_path.is_symlink():
        return FreshContextRolloverRequest(False, "request_path_already_exists")
    if not request_path.parent.is_dir():
        return FreshContextRolloverRequest(False, "request_parent_missing")

    payload = {
        "schema_version": ROLLOVER_REQUEST_SCHEMA,
        "status": "requested",
        "requested_at": datetime.now(timezone.utc).isoformat(),
        "session_id": str(session_id or ""),
        "turn_id": str(turn_id or ""),
        "gate_position": str(gate_position or "turn_start"),
        "gate_reason": decision.reason,
        "prompt_tokens_estimate": decision.prompt_tokens,
        "token_source": "estimated",
        "context_length": decision.context_length,
        "context_percent_estimate": decision.context_pct,
        "threshold_tokens": decision.threshold_tokens,
        "threshold_context_percent": decision.threshold_context_pct,
        "native_compression_requested": False,
        "external_authorization_required": True,
    }
    if api_call_index is not None:
        payload["api_call_index"] = int(api_call_index)
    encoded = (json.dumps(payload, sort_keys=True) + "\n").encode("utf-8")
    temp_path = request_path.with_name(
        f".{request_path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    )
    target_created = False
    try:
        fd = os.open(temp_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            # A same-directory hard link publishes the fully-written request
            # without replacing a file that appeared after the checks above.
            os.link(temp_path, request_path)
            target_created = True
        finally:
            try:
                temp_path.unlink()
            except FileNotFoundError:
                pass
        if request_path.read_bytes() != encoded:
            raise OSError("rollover request verification failed")
    except (OSError, UnicodeError):
        if target_created:
            try:
                request_path.unlink()
            except OSError:
                pass
        return FreshContextRolloverRequest(
            False,
            "request_write_or_verification_failed",
            str(request_path),
        )
    return FreshContextRolloverRequest(True, "rollover_requested", str(request_path))


def evaluate_fresh_context_gate(
    policy: FreshContextGatePolicy,
    *,
    prompt_tokens: int,
    context_length: int,
    token_source: str = "estimated",
) -> FreshContextGateDecision:
    """Block at either inclusive Smart Zone boundary (token OR percentage)."""
    tokens = max(0, int(prompt_tokens or 0))
    window = max(0, int(context_length or 0))
    context_pct = (tokens / window * 100.0) if window else 0.0
    token_boundary = tokens >= policy.threshold_tokens
    pct_boundary = bool(window) and context_pct >= policy.threshold_context_pct
    should_block = bool(policy.enabled and (token_boundary or pct_boundary))
    if not policy.enabled:
        reason = "disabled"
    elif token_boundary and pct_boundary:
        reason = "token_and_context_pct_threshold"
    elif token_boundary:
        reason = "token_threshold"
    elif pct_boundary:
        reason = "context_pct_threshold"
    else:
        reason = "below_threshold"
    return FreshContextGateDecision(
        should_block=should_block,
        reason=reason,
        prompt_tokens=tokens,
        token_source=str(token_source or "estimated"),
        context_length=window,
        context_pct=context_pct,
        threshold_tokens=policy.threshold_tokens,
        threshold_context_pct=policy.threshold_context_pct,
    )


def fresh_context_gate_blocked_message(decision: FreshContextGateDecision) -> str:
    """Return the operator-facing fail-closed response for a blocked turn."""
    return (
        "Fresh Context Gate blocked at threshold "
        f"({decision.token_source} prompt: {decision.prompt_tokens:,} tokens; "
        f"context: {decision.context_pct:.1f}%; thresholds: "
        f"{decision.threshold_tokens:,} tokens or "
        f"{decision.threshold_context_pct:g}%). "
        "No native compression ran for this turn and no API request was sent. "
        "Start a fresh session through the configured CMM wrapper, then retry "
        "the turn."
    )


def fresh_context_rollover_requested_message(
    decision: FreshContextGateDecision,
) -> str:
    """Return the operator-facing terminal response for a queued rollover."""
    return (
        "Fresh Context rollover requested at threshold "
        f"({decision.token_source} prompt: {decision.prompt_tokens:,} tokens; "
        f"context: {decision.context_pct:.1f}%). "
        "The current turn ended before native compression or a provider "
        "request. The configured wrapper can now prepare the CMM handoff and "
        "continue in a fresh context."
    )


def fresh_context_rollover_failed_message(
    decision: FreshContextGateDecision,
    request: FreshContextRolloverRequest,
) -> str:
    """Return the fail-closed response when no safe request was produced."""
    return (
        "Fresh Context Gate blocked at threshold because the configured "
        f"rollover request could not be produced ({request.reason}). "
        f"Estimated prompt: {decision.prompt_tokens:,} tokens; context: "
        f"{decision.context_pct:.1f}%. No native compression ran for this turn "
        "and no API request was sent."
    )


__all__ = [
    "DEFAULT_THRESHOLD_CONTEXT_PCT",
    "DEFAULT_THRESHOLD_TOKENS",
    "FreshContextGateDecision",
    "FreshContextGatePolicy",
    "FreshContextRolloverRequest",
    "ROLLOVER_REQUEST_SCHEMA",
    "evaluate_fresh_context_gate",
    "fresh_context_gate_blocked_message",
    "fresh_context_gate_config",
    "fresh_context_rollover_failed_message",
    "fresh_context_rollover_requested_message",
    "request_fresh_context_rollover",
    "resolve_fresh_context_gate_policy",
]
