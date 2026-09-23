"""Deterministic, network-free provider client for agent-path tests.

The fake deliberately implements the same ``client.chat.completions.create``
surface used by Hermes' OpenAI-compatible production transport.  Every wire
attempt is counted before the configured call budget is enforced, so a test
cannot accidentally hide a provider request behind a mock assertion made
after the turn.
"""

from __future__ import annotations

from collections import deque
from copy import deepcopy
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Iterable


@dataclass(frozen=True)
class ScriptedProviderCall:
    """One attempted provider request captured at the wire-compatible seam."""

    method: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


def scripted_text_response(
    content: str,
    *,
    model: str = "scripted/test-model",
) -> SimpleNamespace:
    """Build the minimal OpenAI-compatible response consumed by Hermes."""

    message = SimpleNamespace(content=content, tool_calls=None)
    choice = SimpleNamespace(message=message, finish_reason="stop")
    return SimpleNamespace(choices=[choice], model=model, usage=None)


class ScriptedProvider:
    """Scripted provider with a hard, code-enforced request budget.

    ``max_calls=0`` is a provider-call veto.  Supplying responses without an
    explicit budget permits exactly that many calls.  Both Chat Completions
    and Responses API entry points are guarded so changing a test agent's
    transport cannot silently escape the cost boundary.
    """

    def __init__(
        self,
        responses: Iterable[Any] = (),
        *,
        max_calls: int | None = None,
    ) -> None:
        self._responses = deque(responses)
        self._max_calls = len(self._responses) if max_calls is None else max_calls
        if self._max_calls < 0:
            raise ValueError("max_calls must be non-negative")
        self.calls: list[ScriptedProviderCall] = []
        self.client = SimpleNamespace(
            chat=SimpleNamespace(
                completions=SimpleNamespace(
                    create=self._chat_completions_create,
                )
            ),
            responses=SimpleNamespace(create=self._responses_create),
            close=lambda: None,
        )

    def create_request_client(self, **_kwargs: Any) -> Any:
        """Return the fake at Hermes' per-request streaming client seam.

        Production streaming calls do not use ``agent.client`` directly;
        they obtain a request-local client through
        ``AIAgent._create_request_openai_client``.  Tests install this method
        at that seam so both streaming and non-streaming requests stay inside
        the same budgeted provider fake.
        """

        return self.client

    @classmethod
    def veto(cls) -> "ScriptedProvider":
        """Return a provider that fails immediately on any wire attempt."""

        return cls(max_calls=0)

    @property
    def attempt_count(self) -> int:
        return len(self.calls)

    @property
    def remaining_responses(self) -> int:
        return len(self._responses)

    def assert_exhausted(self) -> None:
        if self._responses:
            raise AssertionError(
                f"{len(self._responses)} scripted provider response(s) "
                "were not consumed"
            )

    def _record_and_respond(
        self,
        method: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Any:
        self.calls.append(
            ScriptedProviderCall(
                method=method,
                args=deepcopy(args),
                kwargs=deepcopy(kwargs),
            )
        )
        if self.attempt_count > self._max_calls:
            raise AssertionError(
                "scripted provider call budget exceeded: "
                f"attempt={self.attempt_count} budget={self._max_calls} "
                f"method={method}"
            )
        if not self._responses:
            raise AssertionError(
                f"provider request had no scripted response: method={method}"
            )
        response = self._responses.popleft()
        if isinstance(response, BaseException):
            raise response
        if callable(response):
            return response(*args, **kwargs)
        return response

    def _chat_completions_create(self, *args: Any, **kwargs: Any) -> Any:
        return self._record_and_respond("chat.completions.create", args, kwargs)

    def _responses_create(self, *args: Any, **kwargs: Any) -> Any:
        return self._record_and_respond("responses.create", args, kwargs)
