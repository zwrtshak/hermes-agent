"""Opt-in receipt tests; fake Telegram only, no credentials or live sends."""
import asyncio
from collections import OrderedDict
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
import threading
import pytest
import gateway.run as run
from gateway.config import Platform
from gateway.session import SessionSource
from gateway.platforms.base import SendResult

@pytest.fixture
def case(monkeypatch):
    event = dict(type='completion', session_id='proc_fixture', started_at=1000.0,
                 session_key='agent:main:telegram:dm:123', exit_code=0,
                 output='SECRET RAW OUTPUT', command='SECRET COMMAND')
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='123', chat_type='dm')
    entry = SimpleNamespace(session_id='parent', origin=source, suspended=False,
                            created_at=datetime.fromtimestamp(900, timezone.utc))
    receipt = dict(task='completion-probe', started_at=1000.0, session_key=event['session_key'],
                   parent_session_id='parent', chat_id='123', thread_id='')
    event['completion_receipt'] = receipt
    runner = object.__new__(run.GatewayRunner)
    runner.adapters = {Platform.TELEGRAM: SimpleNamespace(send=AsyncMock(return_value=SendResult(True, 'tg-1')))}
    runner.session_store = SimpleNamespace(_ensure_loaded=lambda: None, _entries={event['session_key']: entry})
    runner._completion_delivery_lock = threading.Lock()
    runner._completion_deliveries_inflight = set()
    runner._completion_deliveries_delivered = OrderedDict()
    runner._completion_receipts = OrderedDict()
    runner._completion_delivery_retention = 2048
    runner._inject_watch_notification = AsyncMock(return_value=True)
    return runner, event, entry, receipt


def test_receipt_send_precedes_main_and_duplicate_suppressed(case):
    runner, evt, _, _ = case
    adapter = runner.adapters[Platform.TELEGRAM]
    async def inject(*args):
        adapter.send.assert_awaited_once()
        return True
    runner._inject_watch_notification.side_effect = inject
    assert asyncio.run(runner._deliver_completion_notification('raw', evt)) is True
    asyncio.run(runner._deliver_completion_notification('raw', dict(evt)))
    adapter.send.assert_awaited_once()
    runner._inject_watch_notification.assert_awaited_once()
    args = adapter.send.await_args
    assert args.args[0] == '123'
    assert 'completion-probe' in args.args[1] and 'Main review pending' in args.args[1]
    assert 'SECRET' not in args.args[1] and 'raw' not in args.args[1]
    assert runner._completion_receipts[runner._completion_delivery_identity(evt)]['message_id'] == 'tg-1'


def test_failed_send_retries_without_reinjecting_main(case):
    runner, evt, _, _ = case
    adapter = runner.adapters[Platform.TELEGRAM]
    adapter.send.side_effect = [SendResult(False, error='temporary'), SendResult(True, 'tg-2')]
    assert asyncio.run(runner._deliver_completion_notification('raw', evt)) is False
    runner._inject_watch_notification.assert_awaited_once()
    assert asyncio.run(runner._deliver_completion_notification('raw', evt)) is True
    assert adapter.send.await_count == 2
    runner._inject_watch_notification.assert_awaited_once()


def test_injection_retry_does_not_repeat_successful_send(case):
    runner, evt, _, _ = case
    runner._inject_watch_notification.side_effect = [False, True]
    assert asyncio.run(runner._deliver_completion_notification('raw', evt)) is False
    assert asyncio.run(runner._deliver_completion_notification('raw', evt)) is True
    runner.adapters[Platform.TELEGRAM].send.assert_awaited_once()


@pytest.mark.parametrize('boundary', ['stop', 'new', 'incarnation', 'owner'])
def test_stale_or_stopped_receipt_drops_without_send_or_injection(case, boundary):
    runner, evt, entry, receipt = case
    if boundary == 'stop': entry.suspended = True
    if boundary == 'new':
        entry.session_id = 'new-parent'
        runner._session_db = SimpleNamespace(get_session=AsyncMock(return_value={'ended_at': 1, 'end_reason': 'session_reset'}))
    if boundary == 'incarnation': evt['started_at'] = 2000.0
    if boundary == 'owner': evt['session_key'] = 'agent:main:telegram:dm:other'
    assert asyncio.run(runner._deliver_completion_notification('raw', evt)) is None
    runner.adapters[Platform.TELEGRAM].send.assert_not_awaited()
    runner._inject_watch_notification.assert_not_awaited()


def test_send_exception_is_not_delivery(case):
    runner, evt, _, _ = case
    runner.adapters[Platform.TELEGRAM].send.side_effect = RuntimeError('offline')
    assert asyncio.run(runner._deliver_completion_notification('raw', evt)) is False
    runner._inject_watch_notification.assert_awaited_once()


def test_no_message_id_is_not_confirmed_send(case):
    runner, evt, _, _ = case
    runner.adapters[Platform.TELEGRAM].send.return_value = SendResult(True)
    assert asyncio.run(runner._deliver_completion_notification('raw', evt)) is False


def test_idle_stop_fences_later_receipts(case):
    runner, evt, entry, _ = case
    entry.metadata = {}
    runner.session_store.set_session_metadata = lambda key, name, value: entry.metadata.update({name: value})
    runner._stop_completion_receipts(evt['session_key'])
    assert asyncio.run(runner._deliver_completion_notification('raw', evt)) is None
    runner.adapters[Platform.TELEGRAM].send.assert_not_awaited()
    runner._inject_watch_notification.assert_not_awaited()


@pytest.mark.parametrize('failed', [False, True])
def test_stop_during_send_never_injects_followup(case, failed):
    runner, evt, entry, _ = case
    async def send(*args, **kwargs):
        entry.metadata = {'completion_receipt_stop_at': 1001.0}
        if failed: raise RuntimeError('uncertain')
        return SendResult(True, 'tg-raced')
    runner.adapters[Platform.TELEGRAM].send.side_effect = send
    assert asyncio.run(runner._deliver_completion_notification('raw', evt)) is None
    runner._inject_watch_notification.assert_not_awaited()


def test_original_topic_and_failure_status(case):
    runner, evt, entry, binding = case
    binding['thread_id'] = '77'
    entry.origin.thread_id = '77'
    entry.origin.chat_type = 'group'
    evt['exit_code'] = 1
    assert asyncio.run(runner._deliver_completion_notification('raw', evt)) is True
    call = runner.adapters[Platform.TELEGRAM].send.await_args
    assert call.kwargs['metadata']['thread_id'] == '77'
    assert 'process blocked or failed' in call.args[1]


def test_opt_out_preserves_existing_injection(case, monkeypatch):
    runner, evt, _, _ = case
    evt.pop('completion_receipt')
    assert asyncio.run(runner._deliver_completion_notification('raw', evt)) is True
    runner.adapters[Platform.TELEGRAM].send.assert_not_awaited()


def test_stop_persistence_failure_still_fences(case):
    runner, evt, entry, _ = case
    def fail(*args):
        raise OSError('disk full')
    runner.session_store.set_session_metadata = fail
    runner._stop_completion_receipts(evt['session_key'])
    assert asyncio.run(runner._deliver_completion_notification('raw', evt)) is None
    runner.adapters[Platform.TELEGRAM].send.assert_not_awaited()


@pytest.mark.parametrize('during_send', [False, True])
def test_verified_compression_with_real_database(case, tmp_path, during_send):
    from hermes_state import SessionDB, AsyncSessionDB
    runner, evt, entry, binding = case
    db = SessionDB(tmp_path / 'lineage.db')
    db.create_session('parent', 'telegram')
    runner._session_db = AsyncSessionDB(db)
    def compress():
        db.end_session('parent', 'compression')
        db.create_session('child', 'telegram', parent_session_id='parent')
        entry.session_id = 'child'
    if during_send:
        async def send(*args, **kwargs):
            compress()
            return SendResult(True, 'compressed-send')
        runner.adapters[Platform.TELEGRAM].send.side_effect = send
    else:
        compress()
    try:
        assert asyncio.run(runner._deliver_completion_notification('safe', evt)) is True
        assert binding['parent_session_id'] == 'parent'
        runner._inject_watch_notification.assert_awaited_once()
    finally:
        db.close()


def test_incomplete_compression_retries_without_send_or_injection(case):
    runner, evt, entry, _ = case
    entry.session_id = 'child'
    runner._session_db = SimpleNamespace(
        get_session=AsyncMock(return_value={'ended_at': 1, 'end_reason': 'compression'}),
        get_compression_tip=AsyncMock(return_value='parent'))
    assert asyncio.run(runner._deliver_completion_notification('safe', evt)) is False
    runner.adapters[Platform.TELEGRAM].send.assert_not_awaited()
    runner._inject_watch_notification.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("consume", ["wait", "log"])
@pytest.mark.parametrize("exit_code", [0, 7])
async def test_real_terminal_watcher_survives_consumption_and_pruning(case, tmp_path, monkeypatch, consume, exit_code):
    import json
    import tools.terminal_tool as terminal
    import tools.process_registry as processes
    from gateway.session import SessionContext
    from gateway.session_context import clear_session_vars
    runner, evt, entry, _ = case
    registry = processes.ProcessRegistry()
    monkeypatch.setattr(processes, 'CHECKPOINT_PATH', tmp_path / 'processes.json')
    monkeypatch.setattr(processes, 'process_registry', registry)
    monkeypatch.setattr(terminal, '_start_cleanup_thread', lambda: None)
    monkeypatch.setenv('TERMINAL_ENV', 'local')
    monkeypatch.setenv('TERMINAL_CWD', str(tmp_path))
    monkeypatch.setattr(runner, '_load_background_notifications_mode', lambda: 'all')
    tasks = []
    original_watcher = runner._run_process_watcher
    async def fast_watcher(watcher):
        watcher['check_interval'] = 0.01
        tasks.append(asyncio.current_task())
        await original_watcher(watcher)
    runner._run_process_watcher = fast_watcher
    context = SessionContext(source=entry.origin, connected_platforms=[Platform.TELEGRAM],
                             home_channels={}, session_key=evt['session_key'], session_id='parent')
    runner._set_session_env(context)
    context.session_id = 'mutated-after-bind'
    sent = asyncio.Event()
    calls = []
    async def send(*args, **kwargs):
        calls.append(args)
        if len(calls) == 1:
            return SendResult(False, error='temporary')
        sent.set()
        return SendResult(True, 'receipt-after-prune')
    runner.adapters[Platform.TELEGRAM].send.side_effect = send
    async def inject(text, event):
        pid = event['session_id']
        if consume == 'wait':
            registry.wait(pid, timeout=1)
        else:
            registry.read_log(pid)
        assert registry.is_completion_consumed(pid)
        with registry._lock:
            registry._finished.pop(pid, None)
            registry._running.pop(pid, None)
        assert registry.get(pid) is None
        return True
    runner._inject_watch_notification.side_effect = inject
    try:
        # Actual tool, actual spawn + reader, actual immediate scheduling.
        result = json.loads(await asyncio.to_thread(terminal.terminal_tool,
            command=f"printf 'PRIVATE-STDOUT'; exit {exit_code}", background=True,
            task_id=evt['session_key'], completion_receipt='completion-probe', force=True))
        assert result.get('completion_receipt') == 'completion-probe', result
        assert result['notify_on_complete'] is True
        assert registry.pending_watchers == []
        assert registry.completion_queue.empty()
        await asyncio.wait_for(sent.wait(), 5)
        await asyncio.gather(*tasks)
        assert len(calls) == 2
        assert all('PRIVATE' not in str(call) for call in calls)
        runner._inject_watch_notification.assert_awaited_once()
    finally:
        clear_session_vars([])
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def test_cli_receipt_rejected_before_launch(monkeypatch):
    import json
    from gateway.session_context import completion_launch_context
    from tools.terminal_tool import terminal_tool
    completion_launch_context.set(None)
    result = json.loads(terminal_tool(command='true', background=True, completion_receipt='probe'))
    assert result['exit_code'] == -1
    assert 'native Telegram' in result['error']


@pytest.mark.asyncio
async def test_persistence_failure_does_not_prevent_real_interrupt(case, monkeypatch):
    from unittest.mock import Mock
    runner, evt, entry, _ = case
    def fail(*args):
        raise OSError('disk full')
    runner.session_store.set_session_metadata = fail
    agent = SimpleNamespace()
    state = SimpleNamespace(turn=SimpleNamespace(agent=agent),
                            persistent=SimpleNamespace(pending_command_text='queued'))
    runner._peek_session_state = lambda key: state
    runner._invalidate_session_run_generation = Mock(return_value=2)
    runner._adapter_for_source = lambda source: None
    runner._release_running_agent_state = Mock()
    runner._evict_cached_agent = Mock()
    interrupt = Mock()
    monkeypatch.setattr(run, 'request_hard_interrupt', interrupt)
    await runner._interrupt_and_clear_session(evt['session_key'], entry.origin,
        interrupt_reason='stop', invalidation_reason='stop_command_handler')
    interrupt.assert_called_once_with(agent, 'stop')
    runner._release_running_agent_state.assert_called_once()
    assert state.persistent.pending_command_text is None
    assert evt['session_key'] in runner._completion_stop_fences


def test_incomplete_rotation_same_route_retries(case, tmp_path):
    from hermes_state import SessionDB, AsyncSessionDB
    runner, evt, entry, _ = case
    db = SessionDB(tmp_path / 'rotation.db')
    db.create_session('parent', 'telegram')
    db.end_session('parent', 'compression')
    runner._session_db = AsyncSessionDB(db)
    try:
        assert asyncio.run(runner._deliver_completion_notification('safe', evt)) is False
        runner.adapters[Platform.TELEGRAM].send.assert_not_awaited()
        runner._inject_watch_notification.assert_not_awaited()
    finally:
        db.close()


@pytest.mark.asyncio
async def test_native_launch_context_clears_on_foreign_rebind(case):
    from gateway.session import SessionContext
    from gateway.session_context import completion_launch_context, set_session_vars
    runner, evt, entry, _ = case
    runner._set_session_env(SessionContext(source=entry.origin,
        connected_platforms=[Platform.TELEGRAM], home_channels={},
        session_key=evt['session_key'], session_id='parent'))
    assert completion_launch_context.get() is not None
    set_session_vars(platform='cli')
    assert completion_launch_context.get() is None


def receipt_watcher(case, monkeypatch, tmp_path, *, exited=True):
    import tools.process_registry as processes
    runner, evt, _, binding = case
    registry = processes.ProcessRegistry()
    monkeypatch.setattr(processes, 'CHECKPOINT_PATH', tmp_path / 'processes.json')
    monkeypatch.setattr(processes, 'process_registry', registry)
    monkeypatch.setattr(runner, '_load_background_notifications_mode', lambda: 'all')
    session = processes.ProcessSession(id=evt['session_id'], command='private',
        session_key=evt['session_key'], started_at=evt['started_at'],
        exited=exited, exit_code=0 if exited else None,
        completion_receipt=binding, notify_on_complete=True)
    registry._running[session.id] = session
    watcher = dict(session_id=session.id, session_key=session.session_key,
        check_interval=0.001, platform='telegram', chat_id='123',
        notify_on_complete=True, _receipt_process=session)
    return runner, registry, session, watcher


@pytest.mark.asyncio
@pytest.mark.parametrize('failure', ['rejection', 'timeout'])
async def test_watcher_exhausts_receipt_budget_without_main_reinjection(case, monkeypatch, tmp_path, failure):
    runner, registry, session, watcher = receipt_watcher(case, monkeypatch, tmp_path)
    monkeypatch.setattr(runner, '_RECEIPT_ATTEMPT_SECONDS', 0.02)
    monkeypatch.setattr(runner, '_RECEIPT_TOTAL_SECONDS', 1.0)
    monkeypatch.setattr(runner, '_RECEIPT_FINAL_SECONDS', 0.2)
    adapter = runner.adapters[Platform.TELEGRAM]
    async def send(*args, **kwargs):
        if failure == 'timeout':
            await asyncio.Event().wait()
        return SendResult(False, error='topic permanently rejected')
    adapter.send.side_effect = send
    await asyncio.wait_for(runner._run_process_watcher(watcher), 3)
    assert adapter.send.await_count == runner._RECEIPT_MAX_ATTEMPTS
    runner._inject_watch_notification.assert_awaited_once()
    text, event = runner._inject_watch_notification.await_args.args
    assert 'unconfirmed' in text
    if failure == 'timeout':
        assert 'exhausted' in text
    ledger = runner._completion_receipts[runner._completion_delivery_identity(event)]
    assert ledger['status'] == 'exhausted'
    assert ledger['retry_exhausted'] is True
    assert ledger['attempts'] == runner._RECEIPT_MAX_ATTEMPTS
    assert '_receipt_process' not in watcher


@pytest.mark.asyncio
async def test_watcher_total_budget_limits_attempts(case, monkeypatch, tmp_path):
    runner, _, _, watcher = receipt_watcher(case, monkeypatch, tmp_path)
    monkeypatch.setattr(runner, '_RECEIPT_MAX_ATTEMPTS', 100)
    monkeypatch.setattr(runner, '_RECEIPT_TOTAL_SECONDS', 0.06)
    monkeypatch.setattr(runner, '_RECEIPT_FINAL_SECONDS', 0.02)
    async def hang(*args, **kwargs):
        await asyncio.Event().wait()
    runner.adapters[Platform.TELEGRAM].send.side_effect = hang
    await asyncio.wait_for(runner._run_process_watcher(watcher), 3)
    assert runner.adapters[Platform.TELEGRAM].send.await_count < 100
    runner._inject_watch_notification.assert_awaited_once()
    assert 'exhausted' in runner._inject_watch_notification.await_args.args[0]


@pytest.mark.asyncio
async def test_detached_watcher_native_refresh_until_actual_exit(case, monkeypatch, tmp_path):
    import subprocess
    import sys
    runner, registry, session, watcher = receipt_watcher(case, monkeypatch, tmp_path, exited=False)
    # The child waits for our stdin EOF, so the first refresh is certainly live.
    child = subprocess.Popen([sys.executable, '-B', '-c', 'import sys; sys.stdin.read()'],
                             stdin=subprocess.PIPE)
    session.detached = True
    session.pid = child.pid
    session.host_start_time = registry._safe_host_start_time(child.pid)
    session.watcher_interval = 1
    session.watcher_platform = 'telegram'
    session.watcher_chat_id = '123'
    registry._write_checkpoint()
    import tools.process_registry as processes
    registry = processes.ProcessRegistry()
    monkeypatch.setattr(processes, 'process_registry', registry)
    assert registry.recover_from_checkpoint() == 1
    watcher = registry.pending_watchers.pop()
    watcher['check_interval'] = 0.001
    session = watcher['_receipt_process']
    assert session.detached and not session.exited
    native_refresh = registry._refresh_detached_session
    observed_live = asyncio.Event()
    def refresh(current):
        result = native_refresh(current)
        if result is not None and not result.exited:
            observed_live.set()
        return result
    monkeypatch.setattr(registry, '_refresh_detached_session', refresh)
    task = asyncio.create_task(runner._run_process_watcher(watcher))
    try:
        await asyncio.wait_for(observed_live.wait(), 3)
        assert not session.exited
        runner.adapters[Platform.TELEGRAM].send.assert_not_awaited()
        runner._inject_watch_notification.assert_not_awaited()
        child.stdin.close()
        await asyncio.to_thread(child.wait, timeout=3)
        await asyncio.wait_for(task, 3)
        assert session.exited and session.exit_code is None
        runner.adapters[Platform.TELEGRAM].send.assert_awaited_once()
        runner._inject_watch_notification.assert_awaited_once()
        assert 'exit status unavailable' in runner.adapters[Platform.TELEGRAM].send.await_args.args[1]
    finally:
        if child.poll() is None:
            child.stdin.close()
            child.wait(timeout=3)
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_unsupported_detached_recovery_never_fabricates_exit(case, monkeypatch, tmp_path, caplog):
    runner, _, session, watcher = receipt_watcher(case, monkeypatch, tmp_path, exited=False)
    session.detached = True
    session.pid_scope = 'sandbox'
    await asyncio.wait_for(runner._run_process_watcher(watcher), 3)
    assert not session.exited
    assert 'Unsupported completion receipt recovery' in caplog.text
    runner.adapters[Platform.TELEGRAM].send.assert_not_awaited()
    runner._inject_watch_notification.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize('fence', ['stop', 'new'])
async def test_exhausted_watcher_still_honors_control_fences(case, monkeypatch, tmp_path, fence):
    runner, _, _, watcher = receipt_watcher(case, monkeypatch, tmp_path)
    _, _, entry, _ = case
    monkeypatch.setattr(runner, '_RECEIPT_MAX_ATTEMPTS', 1)
    monkeypatch.setattr(runner, '_RECEIPT_ATTEMPT_SECONDS', 0.01)
    async def send(*args, **kwargs):
        if fence == 'stop':
            entry.suspended = True
        else:
            runner.session_store._entries.clear()
        await asyncio.Event().wait()
    runner.adapters[Platform.TELEGRAM].send.side_effect = send
    await asyncio.wait_for(runner._run_process_watcher(watcher), 3)
    runner.adapters[Platform.TELEGRAM].send.assert_awaited_once()
    runner._inject_watch_notification.assert_not_awaited()


@pytest.mark.asyncio
async def test_exhaustion_preserves_successful_receipt_when_main_retries(case, monkeypatch, tmp_path):
    runner, _, _, watcher = receipt_watcher(case, monkeypatch, tmp_path)
    runner._inject_watch_notification.side_effect = [False, False, False, True]
    await asyncio.wait_for(runner._run_process_watcher(watcher), 3)
    runner.adapters[Platform.TELEGRAM].send.assert_awaited_once()
    text, event = runner._inject_watch_notification.await_args.args
    ledger = runner._completion_receipts[runner._completion_delivery_identity(event)]
    assert ledger['status'] == 'sent'
    assert ledger['message_id'] == 'tg-1'
    assert ledger['retry_exhausted'] is True
    assert 'delivery unconfirmed' not in text
    assert event['_main_injected'] is True


@pytest.mark.asyncio
@pytest.mark.parametrize('observation', ['none', 'exception', 'mismatch'])
@pytest.mark.parametrize('consumer', ['refresh', 'kill'])
async def test_detached_current_identity_observation(case, monkeypatch, tmp_path, observation, consumer):
    import gateway.status as status
    import tools.process_registry as processes
    import psutil
    from unittest.mock import Mock
    runner, registry, session, watcher = receipt_watcher(case, monkeypatch, tmp_path, exited=False)
    signals = Mock(side_effect=AssertionError('unexpected signal boundary'))
    monkeypatch.setattr(processes.os, 'kill', signals)
    monkeypatch.setattr(processes.os, 'killpg', signals)
    monkeypatch.setattr(processes.subprocess, 'run', signals)
    monkeypatch.setattr(psutil, 'Process', signals)
    monkeypatch.setattr(processes, '_stop_systemd_unit', signals)
    session.detached = True
    session.pid = 424242
    session.host_start_time = 123
    state = {'alive': True, 'start': observation}
    monkeypatch.setattr(type(registry), '_is_host_pid_alive', staticmethod(lambda pid: state['alive']))
    def lookup(pid):
        if state['start'] == 'exception':
            raise OSError('temporary identity query failure')
        return {'none': None, 'mismatch': 456, 'restored': 123}[state['start']]
    monkeypatch.setattr(status, 'get_process_start_time', lookup)
    observed = asyncio.Event()
    native_refresh = registry._refresh_detached_session
    def refresh(current):
        result = native_refresh(current)
        observed.set()
        return result
    monkeypatch.setattr(registry, '_refresh_detached_session', refresh)
    # Unknown identity must never authorize signaling, just like a known mismatch.
    assert registry._host_pid_is_ours(session.pid, session.host_start_time) is False
    if consumer == 'kill':
        result = registry.kill_process(session.id, consume_output=False)
        if observation != 'mismatch':
            assert result['status'] == 'error'
            assert not session.exited
        else:
            assert result['status'] == 'already_exited'
    assert registry.poll(session.id)['status'] == ('exited' if observation == 'mismatch' else 'running')
    signals.assert_not_called()
    task = asyncio.create_task(runner._run_process_watcher(watcher))
    try:
        await asyncio.wait_for(observed.wait(), 3)
        if observation != 'mismatch':
            assert not session.exited
            assert session.id in registry._running
            runner.adapters[Platform.TELEGRAM].send.assert_not_awaited()
            runner._inject_watch_notification.assert_not_awaited()
            assert not task.done()
            observed.clear()
            state['start'] = 'restored'
            await asyncio.wait_for(observed.wait(), 3)
            assert not session.exited and not task.done()
            runner.adapters[Platform.TELEGRAM].send.assert_not_awaited()
            state['alive'] = False
        await asyncio.wait_for(task, 3)
        assert session.exited and session.exit_code is None
        assert session.id not in registry._running
        runner.adapters[Platform.TELEGRAM].send.assert_awaited_once()
        runner._inject_watch_notification.assert_awaited_once()
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize('observation', ['none', 'exception', 'match', 'dead', 'reuse', 'no_baseline'])
def test_identity_recovery_and_signal_boundary(case, monkeypatch, tmp_path, observation):
    import json
    import psutil
    import gateway.status as status
    import tools.process_registry as processes
    from unittest.mock import Mock
    _, registry, session, _ = receipt_watcher(case, monkeypatch, tmp_path, exited=False)
    session.pid = 424242
    session.detached = True
    session.host_start_time = None if observation == 'no_baseline' else 123
    session.watcher_interval = 1
    session.systemd_unit = 'hermes-worker-fixture.scope'
    registry._write_checkpoint()
    signal_boundary = Mock()
    monkeypatch.setattr(processes.os, 'kill', signal_boundary)
    monkeypatch.setattr(processes.os, 'killpg', signal_boundary)
    monkeypatch.setattr(processes.subprocess, 'run', signal_boundary)
    monkeypatch.setattr(processes, '_stop_systemd_unit', signal_boundary)
    parent = Mock()
    parent.children.return_value = []
    monkeypatch.setattr(psutil, 'Process', Mock(return_value=parent))
    monkeypatch.setattr(processes.ProcessRegistry, '_daemon_term_grace_seconds', staticmethod(lambda: 0))
    monkeypatch.setattr(processes.ProcessRegistry, '_is_host_pid_alive', staticmethod(lambda pid: observation != 'dead'))
    def lookup(pid):
        if observation == 'exception':
            raise OSError('identity unavailable')
        return None if observation == 'none' else 456 if observation == 'reuse' else 123
    monkeypatch.setattr(status, 'get_process_start_time', lookup)
    recovered = processes.ProcessRegistry()
    assert recovered.recover_from_checkpoint() == (0 if observation in ('dead', 'reuse') else 1)
    if observation in ('dead', 'reuse'):
        parent.terminate.assert_not_called()
        assert signal_boundary.call_count == 1  # owned scope cleanup only
        return
    current = recovered.get(session.id)
    assert current is not None and not current.exited
    assert recovered.poll(session.id)['status'] == 'running'
    # Exercise wait's native refresh without sleeping or fabricating an exit.
    monkeypatch.setattr('tools.interrupt.is_interrupted', lambda: True)
    assert recovered.wait(session.id, timeout=1)['status'] == 'interrupted'
    result = recovered.kill_process(session.id, consume_output=False)
    if observation == 'match':
        assert result['status'] == 'killed'
        parent.terminate.assert_called_once()
    else:
        assert result['status'] == 'error'
        assert not current.exited
        parent.terminate.assert_not_called()
        signal_boundary.assert_not_called()
        assert not current._completion_event.is_set()
        assert recovered.completion_queue.empty()
        assert current.id not in recovered._completion_consumed
        assert json.loads(processes.CHECKPOINT_PATH.read_text())[0]['host_start_time'] == session.host_start_time


@pytest.mark.parametrize('handle', ['detached', 'popen', 'pty'])
@pytest.mark.parametrize('observation', ['none', 'exception'])
def test_host_kill_unknown_identity_handles(case, monkeypatch, tmp_path, handle, observation):
    import gateway.status as status
    import tools.process_registry as processes
    import psutil
    from unittest.mock import Mock
    _, registry, session, _ = receipt_watcher(case, monkeypatch, tmp_path, exited=False)
    session.pid = 424242
    session.host_start_time = 123
    session.detached = handle == 'detached'
    if handle == 'popen':
        session.process = Mock(pid=session.pid)
    if handle == 'pty':
        session._pty = Mock()
    boundary = Mock(side_effect=AssertionError('unexpected signal'))
    monkeypatch.setattr(processes.os, 'kill', boundary)
    monkeypatch.setattr(processes.os, 'killpg', boundary)
    monkeypatch.setattr(processes.subprocess, 'run', boundary)
    monkeypatch.setattr(processes, '_stop_systemd_unit', boundary)
    monkeypatch.setattr(psutil, 'Process', boundary)
    monkeypatch.setattr(type(registry), '_is_host_pid_alive', staticmethod(lambda pid: True))
    def lookup(pid):
        if observation == 'exception':
            raise OSError('identity unavailable')
        return None
    monkeypatch.setattr(status, 'get_process_start_time', lookup)
    assert registry.kill_process(session.id)['status'] == 'error'
    assert not session.exited
    boundary.assert_not_called()
    if session._pty:
        session._pty.terminate.assert_not_called()


def test_kill_identity_lost_at_termination_boundary(case, monkeypatch, tmp_path):
    import gateway.status as status
    import tools.process_registry as processes
    import psutil
    from unittest.mock import Mock
    _, registry, session, _ = receipt_watcher(case, monkeypatch, tmp_path, exited=False)
    session.pid = 424242
    session.host_start_time = 123
    session.process = Mock(pid=session.pid)
    monkeypatch.setattr(type(registry), '_is_host_pid_alive', staticmethod(lambda pid: True))
    monkeypatch.setattr(status, 'get_process_start_time', Mock(side_effect=[123, None]))
    boundary = Mock(side_effect=AssertionError('unexpected signal'))
    monkeypatch.setattr(processes.os, 'kill', boundary)
    monkeypatch.setattr(processes.subprocess, 'run', boundary)
    monkeypatch.setattr(psutil, 'Process', boundary)
    monkeypatch.setattr(processes, '_stop_systemd_unit', boundary)
    assert registry.kill_process(session.id)['status'] == 'error'
    assert not session.exited
    boundary.assert_not_called()
    assert registry.completion_queue.empty()


def test_explicit_unknown_baseline_refuses_termination(monkeypatch):
    import tools.process_registry as processes
    import psutil
    from unittest.mock import Mock
    monkeypatch.setattr(processes.ProcessRegistry, '_is_host_pid_alive', staticmethod(lambda pid: True))
    monkeypatch.setattr(processes.ProcessRegistry, '_safe_host_start_time', staticmethod(lambda pid: 123))
    boundary = Mock(side_effect=AssertionError('unexpected signal'))
    monkeypatch.setattr(psutil, 'Process', boundary)
    monkeypatch.setattr(processes.os, 'kill', boundary)
    monkeypatch.setattr(processes.subprocess, 'run', boundary)
    assert processes.ProcessRegistry._terminate_host_pid(424242, None) is False
    boundary.assert_not_called()
