"""Origin -> durable guard -> native coordinator queue -> Telegram fake."""
import copy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from gateway.config import GatewayConfig, Platform
from gateway.run import GatewayRunner
from gateway.session import SessionSource, SessionStore
from hermes_cli import diggr_continuation as dc, diggr_owner as owner
from tests.gateway.test_diggr_owner_grant import manifest
from tests.gateway.test_telegram_reply_quote import _make_adapter, _make_message


async def setup(tmp_path, monkeypatch, profile='diggr-main', topic='', register=True, task_overrides=None):
    clock = [1000.0]
    monkeypatch.setattr(dc.time, 'time', lambda: clock[0])
    home = tmp_path / profile
    home.mkdir()
    (home / 'config.yaml').write_text(f'diggr_continuation:\n  enabled: true\n  profile: {profile}\n  home: {home}\n')
    guard = dc.runtime_guard(home)
    runner = object.__new__(GatewayRunner)
    runner.config = GatewayConfig()
    runner._resolve_profile_home_for_source = lambda s: home
    runner.session_store = SessionStore(home / 'sessions', runner.config)
    source = SessionSource(platform=Platform.TELEGRAM, profile=profile, chat_id=owner.OWNER,
                           user_id=owner.OWNER, thread_id=topic or None, chat_type='dm')
    session = await runner.async_session_store.get_or_create_session(source)
    identity = runner._diggr_identity(source, session.session_id)
    adapter = SimpleNamespace(send=AsyncMock(return_value=SimpleNamespace(success=True, message_id='sent-1')),
                              _pending_messages={}, _active_sessions={})
    runner._adapter_for_source = lambda s: adapter
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
    try:
        if register:
            dc.register_native(task)
    finally:
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
        r.clock[0] += 100
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
    assert row['deliveries'][-1]['phase'] == 'worker_process_started'
    assert row['deliveries'][-1]['phase'] != 'work_verified'
