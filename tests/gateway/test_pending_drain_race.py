"""Regression tests: pending-drain + finally-cleanup races must not spawn
duplicate agents OR silently drop messages that arrived during cleanup.

Two related races in gateway/platforms/base.py:_process_message_background:

1. Pending-drain path (previous line 1931):
   ``del self._active_sessions[session_key]`` opened a window where a
   concurrent inbound message could pass the Level-1 guard, spawn its
   own _process_message_background, and run simultaneously with the
   recursive drain.  Two agents on one session_key = duplicate responses.

2. Finally-cleanup path (previous line 1990-1991):
   Between the awaits in finally (typing_task, stop_typing) and the
   ``del self._active_sessions[session_key]``, a new message could
   land in _pending_messages.  The del ran anyway, and the message was
   silently dropped — user never got a reply.

Fix: keep the _active_sessions entry live across the turn chain and
clear the Event instead of deleting; in finally, drain any
late-arrival pending message by spawning a task instead of
dropping it.
"""

import asyncio
from unittest.mock import AsyncMock

import pytest

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)
from gateway.session import SessionSource, build_session_key


class _StubAdapter(BasePlatformAdapter):
    async def connect(self, *, is_reconnect: bool = False):
        pass

    async def disconnect(self):
        pass

    async def send(self, chat_id, text, **kwargs):
        return None

    async def get_chat_info(self, chat_id):
        return {}


def _make_adapter():
    adapter = _StubAdapter(PlatformConfig(enabled=True, token="t"), Platform.TELEGRAM)
    adapter._send_with_retry = AsyncMock(return_value=None)
    return adapter


def _make_event(text="hi", chat_id="42"):
    return MessageEvent(
        text=text,
        message_type=MessageType.TEXT,
        source=SessionSource(platform=Platform.TELEGRAM, chat_id=chat_id, chat_type="dm"),
    )


def _sk(chat_id="42"):
    return build_session_key(
        SessionSource(platform=Platform.TELEGRAM, chat_id=chat_id, chat_type="dm")
    )


@pytest.mark.asyncio
async def test_pending_drain_keeps_active_session_guard_live():
    """Fix for R5: during pending-drain cleanup, _active_sessions must stay
    populated so concurrent inbound messages can't spawn a duplicate
    _process_message_background.  We only CLEAR the Event, never delete."""
    adapter = _make_adapter()
    sk = _sk()

    # Register a slow handler so the agent is "mid-processing" when the
    # pending message arrives.
    first_started = asyncio.Event()
    release_first = asyncio.Event()

    async def handler(event):
        first_started.set()
        await release_first.wait()
        return "done"

    adapter._message_handler = handler

    # Spawn M1 through handle_message.
    await adapter.handle_message(_make_event(text="M1"))

    # Wait until M1 is actively running inside the handler.
    await asyncio.wait_for(first_started.wait(), timeout=1.0)

    # Assert: session is active.
    assert sk in adapter._active_sessions
    active_event = adapter._active_sessions[sk]

    # Simulate pending message (M2) queued while M1 runs.
    adapter._pending_messages[sk] = _make_event(text="M2")

    # Release M1 — pending-drain block now runs.  During its cleanup
    # awaits, _active_sessions[sk] must remain populated (same object
    # reference) so any M3 arriving in that window hits the busy-handler.
    release_first.set()

    # Give the drain a moment to execute its .clear() + await typing_task
    # without letting it fully finish the recursive call.
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    # Across the drain transition, the Event object must be the SAME
    # reference (not replaced, not deleted).  If del happened, the key
    # would be missing briefly; if a new Event was installed, the
    # identity would differ.
    assert sk in adapter._active_sessions, (
        "_active_sessions[session_key] was deleted during pending-drain — "
        "opens a window for duplicate-agent spawn"
    )
    assert adapter._active_sessions[sk] is active_event, (
        "_active_sessions[session_key] was replaced during pending-drain — "
        "the old Event may have waiters that now won't be signaled"
    )

    # Finish drain.
    await asyncio.sleep(0.1)
    await adapter.cancel_background_tasks()


@pytest.mark.asyncio
async def test_finally_cleanup_drains_late_arrival_pending():
    """Fix for R6: if a message lands in _pending_messages during the
    finally-block cleanup awaits, the finally must spawn a drain task
    instead of deleting _active_sessions and dropping the message."""
    adapter = _make_adapter()
    sk = _sk()

    processed = []

    async def handler(event):
        processed.append(event.text)
        return "ok"

    adapter._message_handler = handler

    # Instrument stop_typing to inject a late-arrival pending message
    # during the finally-block await window.  This exactly simulates the
    # R6 race: the message arrives after the response has been sent but
    # before _active_sessions is deleted.
    original_stop = adapter.stop_typing if hasattr(adapter, "stop_typing") else None

    injected = {"done": False}

    async def stop_typing_injects_pending(*args, **kwargs):
        # Yield so the injection happens mid-await.
        await asyncio.sleep(0)
        if not injected["done"]:
            adapter._pending_messages[sk] = _make_event(text="LATE")
            injected["done"] = True
        if original_stop:
            return await original_stop(*args, **kwargs)
        return None

    adapter.stop_typing = stop_typing_injects_pending

    # Send M1.
    await adapter.handle_message(_make_event(text="M1"))

    # Drain: wait for M1 to finish and the late-drain task to process LATE.
    for _ in range(50):  # up to ~0.5s
        if "LATE" in processed:
            break
        await asyncio.sleep(0.01)

    await adapter.cancel_background_tasks()

    assert "M1" in processed, "M1 was not processed"
    assert "LATE" in processed, (
        "Late-arrival pending message was silently dropped — finally "
        "cleanup should have spawned a drain task"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("hold_cleanup", [False, True])
async def test_no_pending_cleans_up_normally(hold_cleanup):
    """The completed owner task must release its guard, including slow cleanup."""
    adapter = _make_adapter()
    sk = _sk()
    entered = asyncio.Event()
    release = asyncio.Event()
    original_stop = adapter.stop_typing

    async def controlled_stop(*args, **kwargs):
        entered.set()
        await asyncio.wait_for(release.wait(), timeout=2.0)
        return await original_stop(*args, **kwargs)

    if hold_cleanup:
        adapter.stop_typing = controlled_stop
    adapter._message_handler = AsyncMock(return_value="ok")
    adapter._send_with_retry.return_value = SendResult(success=True, message_id="reply")
    event = _make_event(text="solo")
    await adapter.handle_message(event)
    task = adapter._session_tasks[sk]
    guard = adapter._active_sessions[sk]
    try:
        if hold_cleanup:
            await asyncio.wait_for(entered.wait(), timeout=1.0)
            # A still-running cleanup must retain ownership, not look like a leak.
            assert not task.done()
            assert adapter._session_tasks[sk] is task
            assert adapter._active_sessions[sk] is guard
            assert sk not in adapter._pending_messages
            release.set()

        # Wait for the actual owner, not a nominal polling window that races
        # with typing cleanup and asynchronous delivery-ledger persistence.
        await asyncio.wait_for(asyncio.shield(task), timeout=2.0)
        assert not task.cancelled()
        assert sk not in adapter._active_sessions, (
            "_active_sessions was not cleaned up after a normal turn with no pending"
        )
        assert sk not in adapter._session_tasks
        assert sk not in adapter._pending_messages
        adapter._message_handler.assert_awaited_once_with(event)
        adapter._send_with_retry.assert_awaited_once()
        assert adapter._send_with_retry.await_args.kwargs["chat_id"] == "42"
        assert adapter._send_with_retry.await_args.kwargs["content"] == "ok"
    finally:
        release.set()
        await adapter.cancel_background_tasks()
