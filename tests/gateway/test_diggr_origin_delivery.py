"""Origin -> durable guard -> native coordinator queue -> Telegram fake."""
import copy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from gateway.config import GatewayConfig, Platform
from gateway.run import GatewayRunner
from gateway.session import SessionSource, SessionStore, SessionContext
from hermes_cli import diggr_continuation as dc, diggr_owner as owner
from tests.gateway.test_diggr_owner_grant import manifest
from tests.gateway.test_telegram_reply_quote import _make_adapter, _make_message


class FixtureTransport(SimpleNamespace):
    pass


async def setup(tmp_path, monkeypatch, profile='diggr-main', topic='', register=True, task_overrides=None, transport_profile=None):
    clock = [1000.0]
    monkeypatch.setattr(dc.time, 'time', lambda: clock[0])
    home = tmp_path / profile
    home.mkdir()
    (home / 'config.yaml').write_text(f'diggr_continuation:\n  enabled: true\n  profile: {profile}\n  home: {home}\n')
    guard = dc.runtime_guard(home)
    runner = object.__new__(GatewayRunner)
    runner.config = GatewayConfig()
    runner._load_background_notifications_mode = lambda: 'all'
    runner._resolve_profile_home_for_source = lambda s: home
    runner.session_store = SessionStore(home / 'sessions', runner.config)
    source = SessionSource(platform=Platform.TELEGRAM, profile=profile, chat_id=owner.OWNER,
                           user_id=owner.OWNER, thread_id=topic or None, chat_type='dm')
    session = await runner.async_session_store.get_or_create_session(source)
    identity = runner._diggr_identity(source, session.session_id)
    adapter = FixtureTransport(_bot=SimpleNamespace(id=123 if profile == 'diggr-main' else 456), send=AsyncMock(return_value=SimpleNamespace(success=True, message_id='sent-1')),
                              _pending_messages={}, _active_sessions={})
    runner.adapters = {}
    runner._profile_adapters = {transport_profile or profile: {Platform.TELEGRAM: adapter}}
    if transport_profile:
        import weakref
        source._transport_adapter_ref = weakref.ref(adapter)
    task = dict(task='synthetic-task', scope='synthetic local fixture', action='inspect', owner='coding',
                gate='coding', artifact=str(home / 'result'), deadline=1100, wake_budget=10,
                authorization='synthetic grant reference', identity=identity)
    task.update(task_overrides or {})
    digest = owner.propose(guard, identity, manifest(task))
    async def command(text, n):
        msg = _make_message(text=text)
        msg.chat.id = int(owner.OWNER); msg.from_user.id = int(owner.OWNER)
        msg.from_user.is_bot = False; msg.message_id = n
        msg.message_thread_id = int(topic) if topic else None
        event = SimpleNamespace(source=source, text=text, message_id=str(n), platform_update_id=n)
        owner.stamp_telegram(event, msg, n)
        await runner._diggr_accept(event)
    await command('/continuation show ' + digest, 10)
    await command(adapter.send.call_args.args[1].splitlines()[-1], 11)
    with guard.transaction() as rows:
        task.update(owner_batch=rows.grants['proposals'][digest]['batch'],
                    owner_issue=owner.issue_key(manifest(task)['todos'][0]))
    adapter.send.reset_mock()
    ctx = dc.EVENT_CONTEXT.set(dict(identity=identity))
    session_tokens = runner._set_session_env(SessionContext(source=source,
        connected_platforms=[Platform.TELEGRAM], home_channels={},
        session_key=identity['session_key'], session_id=identity['session']))
    try:
        if register:
            dc.register_native(task)
    finally:
        runner._clear_session_env(session_tokens)
        dc.EVENT_CONTEXT.reset(ctx)
    return SimpleNamespace(guard=guard, runner=runner, source=source, identity=identity,
                           adapter=adapter, task=task, clock=clock, home=home)


def result(r):
    row = r.guard.get(r.task['task'])
    from pathlib import Path
    Path(row['artifact']).write_text('synthetic worker result')
    evidence = {k: row[k] for k in ('task', 'generation', 'action', 'artifact')}
    evidence.update(sha256=dc.digest(row['artifact']), result='ready_for_main_review', exit_code=0)
    assert r.guard.observe(row['task'], row['generation'], 'completed', evidence)
    return evidence


@pytest.mark.asyncio
@pytest.mark.parametrize('profile,topic', [('diggr-main',''), ('mira','73')])
async def test_origin_coordinator_and_telegram_are_distinct(tmp_path, monkeypatch, profile, topic):
    r = await setup(tmp_path, monkeypatch, profile, topic)
    origin = r.guard.get(r.task['task'])['origin']
    assert origin['identity'] == r.identity
    assert origin['request_id'] == '11:11'
    result(r)
    row = r.guard.get(r.task['task'])
    assert row['owner'] == ('main' if profile == 'diggr-main' else 'mira')
    await r.runner._diggr_deliver(r.guard, r.identity)
    call = r.adapter.send.call_args
    assert call.args[0] == r.identity['chat_id']
    assert (call.kwargs.get('metadata') or {}).get('thread_id', '') == topic
    assert r.guard.get(r.task['task'])['coordinator_delivery']['status'] == 'pending'
    jobs = []
    r.adapter.get_pending_message = lambda key: r.adapter._pending_messages.pop(key, None)
    r.adapter._start_session_processing = lambda e, key: jobs.append(e)
    await r.runner._diggr_tick(r.source, r.identity['session'], idle=True)
    assert len(jobs) == 1
    assert await r.runner._diggr_accept(jobs[0])
    assert not await r.runner._diggr_accept(jobs[0])
    assert r.guard.get(r.task['task'])['coordinator_delivery']['status'] == 'accepted'
    await r.runner._diggr_deliver(r.guard, r.identity)
    assert r.adapter.send.call_count == 1


@pytest.mark.asyncio
async def test_restart_duplicate_late_result_and_terminal_delivery(tmp_path, monkeypatch):
    r = await setup(tmp_path, monkeypatch)
    evidence = result(r)
    r.guard = dc.Guard(r.guard.path)
    before = r.guard.path.read_bytes()
    assert not r.guard.observe(r.task['task'], 1, 'completed', evidence)
    assert r.guard.path.read_bytes() == before
    r.guard.control(r.identity, 'paused')
    await r.runner._diggr_deliver(r.guard, r.identity)
    assert r.adapter.send.call_count >= 1
    assert r.guard.get(r.task['task'])['status'] == 'paused'
    assert r.guard.tick(r.identity) is None


@pytest.mark.asyncio
async def test_telegram_failure_bounded_across_restart(tmp_path, monkeypatch):
    r = await setup(tmp_path, monkeypatch)
    result(r)
    r.adapter.send.return_value = SimpleNamespace(success=False, raw_response={'delivery_outcome': 'safe_retry'})
    for _ in range(6):
        await r.runner._diggr_deliver(dc.Guard(r.guard.path), r.identity)
        r.clock[0] += 30
    assert r.adapter.send.call_count == 3
    events = r.guard.get(r.task['task'])['deliveries']
    assert events[-1]['status'] == 'failed'
    assert events[-1]['attempts'] == 3


@pytest.mark.asyncio
async def test_send_exception_is_uncertain_not_success_or_blind_retry(tmp_path, monkeypatch):
    r = await setup(tmp_path, monkeypatch)
    result(r)
    r.adapter.send.side_effect = TimeoutError('sensitive text must not be persisted')
    for _ in range(3):
        await r.runner._diggr_deliver(dc.Guard(r.guard.path), r.identity)
        r.clock[0] += 100
    assert r.adapter.send.call_count == 1
    assert r.guard.get(r.task['task'])['deliveries'][-1]['status'] == 'uncertain'
    assert 'sensitive' not in r.guard.path.read_text()


@pytest.mark.asyncio
@pytest.mark.parametrize('field', ['profile', 'chat_id', 'thread_id', 'session', 'home'])
async def test_foreign_delivery_is_rejected(tmp_path, monkeypatch, field):
    r = await setup(tmp_path, monkeypatch)
    result(r)
    foreign = dict(r.identity, **{field: 'foreign'})
    await r.runner._diggr_deliver(r.guard, foreign)
    r.adapter.send.assert_not_called()


@pytest.mark.asyncio
async def test_origin_cannot_be_overridden_or_rewritten(tmp_path, monkeypatch):
    r = await setup(tmp_path, monkeypatch)
    with pytest.raises(ValueError):
        with r.guard.transaction() as rows:
            rows[r.task['task']]['origin']['identity']['chat_id'] = 'forged'
    assert r.guard.get(r.task['task'])['origin']['identity'] == r.identity
    forged = dict(r.task, task='alias', artifact=str(r.home/'other'), origin={'chat_id':'forged'})
    ctx = dc.EVENT_CONTEXT.set(dict(identity=r.identity))
    try:
        with pytest.raises(ValueError): dc.register_native(forged)
    finally: dc.EVENT_CONTEXT.reset(ctx)


@pytest.mark.asyncio
async def test_failed_worker_has_no_success_delivery(tmp_path, monkeypatch):
    r = await setup(tmp_path, monkeypatch)
    r.guard.observe(r.task['task'], 1, 'unknown')
    await r.runner._diggr_deliver(r.guard, r.identity)
    assert 'reconcile' in r.adapter.send.call_args.args[1]
    assert r.guard.get(r.task['task'])['status'] != 'done'


@pytest.mark.asyncio
async def test_text_cannot_claim_coordinator_wake(tmp_path, monkeypatch):
    r = await setup(tmp_path, monkeypatch)
    result(r)
    wake = r.guard.tick(r.identity)
    fake = SimpleNamespace(source=r.source, text=dc.prompt(wake), internal=True)
    assert not await r.runner._diggr_accept(fake)
    assert r.guard.get(r.task['task'])['status'] == 'queued'


@pytest.mark.asyncio
async def test_origin_removal_rejected(tmp_path, monkeypatch):
    r = await setup(tmp_path, monkeypatch)
    with pytest.raises(ValueError):
        with r.guard.transaction() as rows:
            rows[r.task['task']].pop('origin')


@pytest.mark.asyncio
async def test_interrupted_send_not_replayed_and_profile_home_checked(tmp_path, monkeypatch):
    r = await setup(tmp_path, monkeypatch)
    result(r)
    with r.guard.transaction() as rows:
        rows[r.task['task']]['deliveries'][-1].update(status='sending', attempts=1, due=1001)
    r.clock[0] = 1002
    await r.runner._diggr_deliver(dc.Guard(r.guard.path), r.identity)
    r.adapter.send.assert_not_called()
    assert r.guard.get(r.task['task'])['deliveries'][-1]['status'] == 'uncertain'
    r.runner._resolve_profile_home_for_source = lambda source: tmp_path / 'wrong-home'
    with pytest.raises(ValueError): await r.runner._diggr_deliver(r.guard, r.identity)


@pytest.mark.asyncio
async def test_cli_environment_is_not_native_origin(tmp_path, monkeypatch):
    import json
    r = await setup(tmp_path, monkeypatch)
    for key, field in {'PLATFORM':'platform', 'CHAT_ID':'chat_id', 'USER_ID':'user_id',
                       'THREAD_ID':'thread_id', 'ID':'session', 'KEY':'session_key', 'PROFILE':'profile'}.items():
        monkeypatch.setenv('HERMES_SESSION_' + key, r.identity[field])
    monkeypatch.setenv('HERMES_HOME', str(r.home))
    ctx = dc.EVENT_CONTEXT.set(None)
    try:
        with pytest.raises(ValueError, match='native runtime context'):
            dc.main(['--home', str(r.home), 'register', '--payload-json', json.dumps(r.task)])
    finally: dc.EVENT_CONTEXT.reset(ctx)


@pytest.mark.asyncio
async def test_new_session_cannot_capture_confirmed_request(tmp_path, monkeypatch):
    r = await setup(tmp_path, monkeypatch, register=False)
    identity = dict(r.identity, session='foreign-session')
    ctx = dc.EVENT_CONTEXT.set(dict(identity=identity))
    try:
        with pytest.raises(ValueError, match='coordinator'):
            dc.register_native(r.task)
    finally: dc.EVENT_CONTEXT.reset(ctx)


@pytest.mark.asyncio
async def test_wrong_profile_config_cannot_capture_request(tmp_path, monkeypatch):
    r = await setup(tmp_path, monkeypatch, register=False)
    (r.home / 'config.yaml').write_text(f'diggr_continuation:\n  enabled: true\n  profile: mira\n  home: {r.home}\n')
    ctx = dc.EVENT_CONTEXT.set(dict(identity=r.identity))
    try:
        with pytest.raises(ValueError, match='profile'):
            dc.register_native(r.task)
    finally: dc.EVENT_CONTEXT.reset(ctx)


@pytest.mark.asyncio
async def test_nonzero_exit_cannot_be_success(tmp_path, monkeypatch):
    r = await setup(tmp_path, monkeypatch)
    row = r.guard.get(r.task['task'])
    from pathlib import Path
    Path(row['artifact']).write_text('synthetic unsuccessful output')
    proof = {k:row[k] for k in ('task','generation','action','artifact')}
    proof.update(sha256=dc.digest(row['artifact']), result='ready_for_main_review', exit_code=1)
    r.guard.observe(row['task'], row['generation'], 'completed', proof)
    assert r.guard.get(row['task'])['gate'] == 'reconcile'


@pytest.mark.asyncio
@pytest.mark.parametrize('profile,topic', [('diggr-main',''), ('mira','73')])
async def test_coordinator_answer_is_durable_and_does_not_restart_work(tmp_path, monkeypatch, profile, topic):
    from hermes_cli.diggr_delivery import NativeWake
    r = await setup(tmp_path, monkeypatch, profile, topic)
    result(r)
    wake = r.guard.tick(r.identity)
    assert r.guard.begin(r.identity, wake)
    event = SimpleNamespace(source=r.source, _diggr_wake=NativeWake(wake['task'], wake['generation'], r.identity['session']))
    answer = {'final_response': 'Synthetic coordinator result: checked fixture A.', 'failed': False}
    assert await r.runner._diggr_finish(event, answer)
    r.adapter.send.reset_mock()
    await r.runner._diggr_deliver(dc.Guard(r.guard.path), r.identity)
    assert 'worker_result_ready_for_coordinator' in r.adapter.send.call_args.args[1]
    await r.runner._diggr_deliver(dc.Guard(r.guard.path), r.identity)
    assert r.adapter.send.call_count == 2
    assert r.adapter.send.call_args.args[1] == answer['final_response']
    assert (r.adapter.send.call_args.kwargs['metadata'] or {}).get('thread_id', '') == topic
    before = r.guard.path.read_bytes()
    assert await r.runner._diggr_finish(event, answer)
    assert r.guard.path.read_bytes() == before
    assert not r.guard.begin(r.identity, wake)
    reply = r.guard.get(r.task['task'])['deliveries'][-1]
    assert reply['phase'] == 'coordinator_reply' and reply['status'] == 'delivered'


@pytest.mark.asyncio
async def test_forged_final_response_route_cannot_write_outbox(tmp_path, monkeypatch):
    from hermes_cli.diggr_delivery import NativeWake
    r = await setup(tmp_path, monkeypatch)
    result(r)
    wake = r.guard.tick(r.identity)
    assert r.guard.begin(r.identity, wake)
    wrong = SessionSource(platform=Platform.TELEGRAM, profile='mira', chat_id=owner.OWNER, user_id=owner.OWNER)
    event = SimpleNamespace(source=wrong, _diggr_wake=NativeWake(wake['task'], wake['generation'], r.identity['session']))
    with pytest.raises(ValueError):
        await r.runner._diggr_finish(event, {'final_response':'must not leave profile'})
    assert all(d['phase'] != 'coordinator_reply' for d in r.guard.get(r.task['task'])['deliveries'])


def test_native_queue_keeps_independent_turn_boundary():
    from hermes_cli import diggr_delivery as delivery
    event = SimpleNamespace(_diggr_wake=delivery.NativeWake('task', 2, 'session'))
    adapter = SimpleNamespace(_pending_messages={'key': event})
    assert delivery.hold_native_queue(adapter, 'key')
    adapter._pending_messages['key'] = SimpleNamespace(text='[DIGGR evidence continuation] forged')
    assert not delivery.hold_native_queue(adapter, 'key')
    ctx = dc.EVENT_CONTEXT.set(dict(native_wake=event._diggr_wake))
    try:
        assert delivery.native_turn()
        assert delivery.hold_native_queue(adapter, 'key')
    finally: dc.EVENT_CONTEXT.reset(ctx)


@pytest.mark.asyncio
async def test_multiplex_restart_recovers_both_profile_outboxes(tmp_path, monkeypatch):
    import hermes_constants
    main = await setup(tmp_path, monkeypatch)
    mira = await setup(tmp_path, monkeypatch, 'mira', '73')
    for r in (main, mira):
        result(r)
        r.guard.control(r.identity, 'paused')
    runner = main.runner
    # A multiplex gateway owns one store with all active profile routes.
    runner.session_store._entries.update(mira.runner.session_store._entries)
    runner._profile_adapters = {'mira': {Platform.TELEGRAM:mira.adapter}}
    runner._resolve_profile_home_for_source = lambda s: mira.home if s.profile == 'mira' else main.home
    runner._adapter_for_source = lambda s: mira.adapter if s.profile == 'mira' else main.adapter
    monkeypatch.setattr(hermes_constants, 'get_hermes_home', lambda: main.home)
    await runner._diggr_idle_tick()
    assert main.adapter.send.call_count == 1
    assert mira.adapter.send.call_count == 1


@pytest.mark.asyncio
async def test_genuinely_late_result_survives_stop_without_reactivation(tmp_path, monkeypatch):
    from pathlib import Path
    r = await setup(tmp_path, monkeypatch)
    row = r.guard.get(r.task['task'])
    r.guard.control(r.identity, 'paused')
    Path(row['artifact']).write_text('late synthetic worker evidence')
    proof = {k: row[k] for k in ('task','generation','action','artifact')}
    proof.update(sha256=dc.digest(row['artifact']), result='ready_for_main_review', exit_code=0)
    assert not r.guard.observe(row['task'], 1, 'completed', proof)
    current = dc.Guard(r.guard.path).get(row['task'])
    assert current['late_evidence'] == proof and current['status'] == 'paused'
    assert current['deliveries'][-1]['phase'] == 'late_result_retained_no_execution'
    assert r.guard.tick(r.identity) is None


@pytest.mark.asyncio
async def test_dispatch_is_not_worker_start(tmp_path, monkeypatch):
    r = await setup(tmp_path, monkeypatch)
    with r.guard.transaction() as rows:
        rows[r.task['task']].update(claim_phase='sent', visible_sent_at=1000)
    assert not r.guard.get(r.task['task']).get('deliveries')
    with r.guard.transaction() as rows:
        rows[r.task['task']].update(claim_phase='claimed', child_identity={'pid':123, 'created':1001})
    row = r.guard.get(r.task['task'])
    assert not row.get('deliveries')


@pytest.mark.asyncio
async def test_native_exit_receipt_precedes_classification_and_survives_missing_adapter(tmp_path, monkeypatch):
    r = await setup(tmp_path, monkeypatch)
    attempt = r.guard.get(r.task['task'])
    attempt['worker_started_ns'] = 0
    worker_result = r.guard.record_worker_result(attempt, 7)
    row = dc.Guard(r.guard.path).get(r.task['task'])
    assert row['gate'] == 'coding' and row['worker_result'] == worker_result
    assert row['deliveries'][0]['phase'] == 'worker_process_exited'
    assert row['deliveries'][0]['exit_code'] == 7
    r.runner._adapter_for_source = lambda source: None
    await r.runner._diggr_deliver(r.guard, r.identity)
    assert r.guard.get(r.task['task'])['deliveries'][0]['attempts'] == 0
    dc.observe_processes(dc.Guard(r.guard.path))
    row = r.guard.get(r.task['task'])
    assert row['gate'] == 'main_validation' and row['generation'] == 2
    assert len(row['deliveries']) == 1
    r.runner._adapter_for_source = lambda source: r.adapter
    await r.runner._diggr_deliver(r.guard, r.identity)
    assert 'Exit 7' in r.adapter.send.call_args.args[1]
    assert 'noch nicht geprüft' in r.adapter.send.call_args.args[1]
    assert row['worker_result']['result'] == 'unclassified'


@pytest.mark.asyncio
async def test_native_adapter_outage_expires_without_send_attempt_or_worker_restart(tmp_path, monkeypatch):
    from hermes_cli.diggr_delivery import DELIVERY_SECONDS
    r = await setup(tmp_path, monkeypatch)
    result(r)
    r.runner._adapter_for_source = lambda source: None
    await r.runner._diggr_tick(r.source, r.identity['session'], idle=True)
    assert r.guard.get(r.task['task'])['wakes'] == 0
    r.clock[0] += DELIVERY_SECONDS + 1
    await r.runner._diggr_deliver(dc.Guard(r.guard.path), r.identity)
    row = r.guard.get(r.task['task'])
    assert row['deliveries'][0]['status'] == 'failed'
    assert row['deliveries'][0]['attempts'] == 0
    assert row['gate'] == 'main_validation'


@pytest.mark.asyncio
async def test_native_main_consumes_result_before_slow_telegram_returns(tmp_path, monkeypatch):
    import asyncio
    r = await setup(tmp_path, monkeypatch)
    result(r)
    internal = asyncio.Event()
    send_started = asyncio.Event()
    release_send = asyncio.Event()
    async def slow_send(*args, **kwargs):
        send_started.set()
        await release_send.wait()
        return SimpleNamespace(success=True, message_id='one')
    r.adapter.send.side_effect = slow_send
    r.adapter.get_pending_message = lambda key: r.adapter._pending_messages.pop(key, None)
    r.adapter._start_session_processing = lambda event, key: internal.set()
    tick = asyncio.create_task(r.runner._diggr_tick(r.source, r.identity['session'], idle=True))
    try:
        await asyncio.wait_for(send_started.wait(), 1)
        assert internal.is_set()
        assert not tick.done()
    finally:
        release_send.set()
        await tick


@pytest.mark.asyncio
@pytest.mark.parametrize('mode,exit_code,expected', [('off', 7, 0), ('result', 0, 1),
                                                    ('error', 0, 0), ('error', 7, 1)])
async def test_native_quiet_preferences_do_not_drop_internal_result(tmp_path, monkeypatch, mode, exit_code, expected):
    r = await setup(tmp_path, monkeypatch)
    attempt = r.guard.get(r.task['task'])
    attempt['worker_started_ns'] = 0
    r.guard.record_worker_result(attempt, exit_code)
    r.runner._load_background_notifications_mode = lambda: mode
    jobs = []
    r.adapter.get_pending_message = lambda key: r.adapter._pending_messages.pop(key, None)
    r.adapter._start_session_processing = lambda event, key: jobs.append(event)
    await r.runner._diggr_tick(r.source, r.identity['session'], idle=True)
    assert len(jobs) == 1
    assert r.adapter.send.call_count == expected
    assert r.guard.get(r.task['task'])['worker_result']['exit_code'] == exit_code


@pytest.mark.asyncio
@pytest.mark.parametrize('lifecycle', ['stop', 'reset'])
async def test_native_stale_session_suppresses_receipt_and_main_wake(tmp_path, monkeypatch, lifecycle):
    r = await setup(tmp_path, monkeypatch)
    result(r)
    entry = r.runner.session_store._entries[r.identity['session_key']]
    if lifecycle == 'stop':
        entry.metadata['completion_receipt_stop_at'] = r.clock[0] + 1
    else:
        entry.session_id = 'different-session-after-reset'
    await r.runner._diggr_tick(r.source, r.identity['session'], idle=True)
    row = r.guard.get(r.task['task'])
    assert row['wakes'] == 0
    assert not r.adapter._pending_messages
    r.adapter.send.assert_not_called()
    # Unknown lineage can retry scope, but it cannot send or consume a wake.
    assert row['deliveries'][0]['status'] in {'pending', 'suppressed'}


@pytest.mark.asyncio
async def test_native_success_without_message_id_is_uncertain(tmp_path, monkeypatch):
    r = await setup(tmp_path, monkeypatch)
    result(r)
    r.adapter.send.return_value = SimpleNamespace(success=True, message_id=None)
    await r.runner._diggr_deliver(r.guard, r.identity)
    r.clock[0] += 30
    await r.runner._diggr_deliver(dc.Guard(r.guard.path), r.identity)
    assert r.adapter.send.call_count == 1
    assert r.guard.get(r.task['task'])['deliveries'][0]['status'] == 'uncertain'


@pytest.mark.asyncio
async def test_native_registration_missing_transport_context_cannot_be_forged_by_payload(tmp_path, monkeypatch):
    from gateway.session_context import completion_launch_context
    r = await setup(tmp_path, monkeypatch, register=False)
    context = dc.EVENT_CONTEXT.set(dict(identity=r.identity))
    transport = completion_launch_context.set(None)
    before = r.guard.path.read_bytes()
    try:
        with pytest.raises(ValueError, match='native transport binding required'):
            dc.register_native(r.task)
        for field in ('native_transport', 'transport', 'transport_profile', 'bot_id', 'chat_type'):
            with pytest.raises(ValueError, match='native-only'):
                dc.register_native(dict(r.task, **{field: 'claimed-by-worker'}))
        assert r.guard.path.read_bytes() == before
    finally:
        completion_launch_context.reset(transport)
        dc.EVENT_CONTEXT.reset(context)


@pytest.mark.asyncio
async def test_native_transport_context_must_match_real_gateway_session(tmp_path, monkeypatch):
    r = await setup(tmp_path, monkeypatch, register=False)
    foreign = SessionSource(platform=Platform.TELEGRAM, profile=r.source.profile,
        chat_id=r.source.chat_id, user_id=r.source.user_id, thread_id='foreign-topic')
    context = dc.EVENT_CONTEXT.set(dict(identity=r.identity))
    tokens = r.runner._set_session_env(SessionContext(source=foreign,
        connected_platforms=[Platform.TELEGRAM], home_channels={},
        session_key=r.identity['session_key'], session_id=r.identity['session']))
    try:
        with pytest.raises(ValueError, match='differs from coordinator'):
            dc.register_native(r.task)
    finally:
        r.runner._clear_session_env(tokens)
        dc.EVENT_CONTEXT.reset(context)
    assert r.guard.get(r.task['task']) is None


@pytest.mark.asyncio
async def test_native_same_profile_chat_topic_different_bot_has_no_effect(tmp_path, monkeypatch):
    r = await setup(tmp_path, monkeypatch, topic='73')
    result(r)
    replacement = FixtureTransport(_bot=SimpleNamespace(id=999),
        send=AsyncMock(return_value=SimpleNamespace(success=True, message_id='wrong-bot')),
        _pending_messages={}, _active_sessions={})
    r.runner._profile_adapters[r.identity['profile']][Platform.TELEGRAM] = replacement
    row = r.guard.get(r.task['task'])
    before_wakes = row['wakes']
    await r.runner._diggr_tick(r.source, r.identity['session'])
    replacement.send.assert_not_awaited()
    assert not replacement._pending_messages
    assert r.guard.get(r.task['task'])['wakes'] == before_wakes
    assert r.guard.get(r.task['task'])['origin']['transport']['bot_id'] == '123'


@pytest.mark.asyncio
async def test_native_shared_transport_survives_session_store_restart(tmp_path, monkeypatch):
    r = await setup(tmp_path, monkeypatch, profile='mira', topic='73', transport_profile='shared-transport')
    row = r.guard.get(r.task['task'])
    binding = row['origin']['transport']
    assert binding['profile'] == 'mira'
    assert binding['transport_profile'] == 'shared-transport'
    assert binding['profile_home'] == str(r.home)
    assert binding['chat_type'] == 'dm'
    result(r)
    r.guard = dc.Guard(r.guard.path)
    # SessionSource serialization intentionally drops process-local adapter refs.
    r.runner.session_store = SessionStore(r.home / 'sessions', r.runner.config)
    await r.runner.async_session_store._ensure_loaded()
    restored = r.runner.session_store._entries[r.identity['session_key']].origin
    assert not getattr(restored, '_transport_adapter_ref', None)
    wrong = FixtureTransport(_bot=SimpleNamespace(id=999), send=AsyncMock(),
                             _pending_messages={}, _active_sessions={})
    r.runner._profile_adapters['mira'] = {Platform.TELEGRAM: wrong}
    await r.runner._diggr_tick(restored, r.identity['session'])
    r.adapter.send.assert_awaited_once()
    wrong.send.assert_not_awaited()
    assert r.adapter._pending_messages
    queued = r.adapter._pending_messages[r.identity['session_key']]
    assert r.runner._adapter_for_source(queued.source) is r.adapter
    assert not wrong._pending_messages


@pytest.mark.asyncio
async def test_native_old_origin_without_transport_stays_readable_but_never_adopts_current_bot(tmp_path, monkeypatch):
    import json
    r = await setup(tmp_path, monkeypatch)
    result(r)
    legacy = json.loads(r.guard.path.read_text())
    row = legacy[r.task['task']]
    row.pop('native_transport')
    row['origin'].pop('transport')
    # Explicit disposable legacy fixture, not a production state migration.
    r.guard.path.write_text(json.dumps(legacy))
    r.guard = dc.Guard(r.guard.path)
    assert r.guard.get(r.task['task'])['evidence']
    await r.runner._diggr_tick(r.source, r.identity['session'])
    r.adapter.send.assert_not_awaited()
    assert not r.adapter._pending_messages
    assert r.guard.get(r.task['task'])['deliveries'][0]['status'] == 'suppressed'


@pytest.mark.asyncio
async def test_native_persisted_transport_is_immutable_even_when_origin_is_changed_together(tmp_path, monkeypatch):
    r = await setup(tmp_path, monkeypatch)
    before = r.guard.path.read_bytes()
    with pytest.raises(ValueError, match='immutable'):
        with r.guard.transaction() as rows:
            rows[r.task['task']]['native_transport']['bot_id'] = '999'
            rows[r.task['task']]['origin']['transport']['bot_id'] = '999'
    assert r.guard.path.read_bytes() == before


@pytest.mark.asyncio
async def test_native_queued_wake_cannot_be_accepted_after_bot_replacement(tmp_path, monkeypatch):
    r = await setup(tmp_path, monkeypatch)
    result(r)
    await r.runner._diggr_tick(r.source, r.identity['session'])
    event = r.adapter._pending_messages.pop(r.identity['session_key'])
    replacement = FixtureTransport(_bot=SimpleNamespace(id=999), send=AsyncMock(),
                                  _pending_messages={}, _active_sessions={})
    r.runner._profile_adapters[r.identity['profile']][Platform.TELEGRAM] = replacement
    assert await r.runner._diggr_accept(event) is False
    assert r.guard.get(r.task['task'])['coordinator_delivery']['status'] == 'pending'
    replacement.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_native_missing_live_bot_identity_blocks_registration_at_gateway_boundary(tmp_path, monkeypatch):
    r = await setup(tmp_path, monkeypatch, register=False)
    r.adapter._bot = None
    context = dc.EVENT_CONTEXT.set(dict(identity=r.identity))
    tokens = r.runner._set_session_env(SessionContext(source=r.source,
        connected_platforms=[Platform.TELEGRAM], home_channels={},
        session_key=r.identity['session_key'], session_id=r.identity['session']))
    try:
        with pytest.raises(ValueError, match='native transport binding required'):
            dc.register_native(r.task)
        assert r.guard.get(r.task['task']) is None
    finally:
        r.runner._clear_session_env(tokens)
        dc.EVENT_CONTEXT.reset(context)
