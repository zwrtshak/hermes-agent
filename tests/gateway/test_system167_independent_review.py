"""Review-only reproductions. All transport and topic data are synthetic."""
from types import SimpleNamespace
from unittest.mock import AsyncMock
import pytest

from tests.gateway.test_telegram_thread_fallback import (
    _inject_fake_telegram, _make_adapter, FakeTimedOut,
)
from tests.gateway.test_diggr_origin_delivery import setup, result
from hermes_cli.diggr_delivery import NativeWake


@pytest.mark.asyncio
async def test_actual_adapter_timeout_is_not_blindly_repeated(tmp_path, monkeypatch):
    r = await setup(tmp_path, monkeypatch)
    result(r)
    adapter = _make_adapter()
    adapter._should_attempt_rich = lambda *a, **kw: False
    adapter._bot = SimpleNamespace(send_message=AsyncMock(side_effect=FakeTimedOut('Timed out')))
    adapter.send_typing = AsyncMock()
    r.runner._adapter_for_source = lambda source: adapter
    for _ in range(3):
        await r.runner._diggr_deliver(r.guard, r.identity)
        r.clock[0] += 100
    row = r.guard.get(r.task['task'])
    print('transport attempts:', adapter._bot.send_message.call_count)
    print('delivery:', row['deliveries'])
    assert adapter._bot.send_message.call_count == 1, 'Uncertain Telegram timeout must not be resent'
    assert row['deliveries'][0]['status'] == 'uncertain'


@pytest.mark.asyncio
async def test_native_wake_keeps_origin_through_real_topic_recovery(tmp_path, monkeypatch):
    r = await setup(tmp_path, monkeypatch)
    result(r)
    jobs = []
    r.adapter.get_pending_message = lambda key: r.adapter._pending_messages.pop(key, None)
    r.adapter._start_session_processing = lambda event, key: jobs.append(event)
    await r.runner._diggr_tick(r.source, r.identity['session'], idle=True)
    event = jobs[0]
    assert type(event._diggr_wake) is NativeWake
    assert await r.runner._diggr_accept(event)
    r.runner._telegram_topic_mode_enabled = lambda source: True
    r.runner._session_db = SimpleNamespace(list_telegram_topic_bindings_for_chat=lambda **kw: [
        {'user_id': r.identity['user_id'], 'thread_id': '73'},
    ])
    # Stop at the first handler session lookup: never instantiate/run an agent.
    class StopBeforeAgent(Exception):
        pass
    seen = []
    async def lookup(source):
        seen.append(source)
        raise StopBeforeAgent()
    monkeypatch.setattr(r.runner.async_session_store, 'get_or_create_session', lookup)
    with pytest.raises(StopBeforeAgent):
        await r.runner._handle_message_with_agent(event, event.source, 'synthetic', 1)
    print('bound topic:', repr(r.identity['thread_id']), 'handler topic:', repr(seen[0].thread_id))
    assert (seen[0].thread_id or '') == r.identity['thread_id'], 'Native origin must not be rewritten to latest topic'
