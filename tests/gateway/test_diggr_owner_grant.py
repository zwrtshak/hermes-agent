"""Actual Telegram command handler -> gateway -> durable grant -> Guard; no network."""
import copy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from gateway.config import GatewayConfig
from gateway.run import GatewayRunner
from gateway.session import SessionStore
from gateway.platforms.base import MessageEvent
from hermes_cli import diggr_continuation as dc
from hermes_cli import diggr_owner as owner
from tests.gateway.test_telegram_reply_quote import _make_adapter, _make_message


def manifest(task, number=1, replaces=None):
    return dict(budgets=dict(owner.BUDGETS), replaces=replaces, todos=[dict(
        workspace_id='11111111-1111-4111-8111-111111111111',
        project_id='22222222-2222-4222-8222-222222222222',
        issue_id=f'33333333-3333-4333-8333-{number:012d}',
        contract=owner.contract(task), depends_on=[], gates=['owner_confirmed', 'native_checks', 'prior_done'])])


@pytest.fixture
def route(tmp_path, monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr(dc.time, 'time', lambda: clock[0])
    guard = dc.Guard(tmp_path / 'state.json')
    monkeypatch.setattr(dc, 'runtime_guard', lambda home=None: guard)
    runner = object.__new__(GatewayRunner)
    runner.config = GatewayConfig()
    runner._resolve_profile_home_for_source = lambda source: tmp_path
    runner.session_store = SessionStore(tmp_path / 'sessions', runner.config)
    adapter = _make_adapter()
    adapter.gateway_runner = SimpleNamespace(_profile_name_for_source=lambda s: 'diggr-main')
    adapter._should_process_message = lambda *a, **kw: True
    adapter._is_user_authorized_from_message = lambda m: True  # disposable transport allowlist
    adapter._ensure_forum_commands = AsyncMock()
    adapter._cache_replied_media = AsyncMock()
    adapter._apply_telegram_group_observe_attribution = lambda e: e
    adapter.send = AsyncMock(return_value=SimpleNamespace(success=True))
    runner._adapter_for_source = lambda source: adapter
    events = []
    adapter.handle_message = AsyncMock(side_effect=lambda e: events.append(e))
    seq = [10]

    async def event(text, **changes):
        seq[0] += 1
        msg = _make_message(text=text)
        msg.chat.id = int(owner.OWNER); msg.from_user.id = int(owner.OWNER)
        msg.from_user.is_bot = False
        msg.message_id = seq[0]
        for k, v in changes.items(): setattr(msg, k, v)
        await adapter._handle_command(SimpleNamespace(effective_message=msg, update_id=seq[0]), None)
        return events[-1]

    identity = dict(home=str(tmp_path), profile='diggr-main', platform='telegram',
        chat_id=owner.OWNER, user_id=owner.OWNER, thread_id='', session='fixture', session_key='fixture')
    task = dict(task='display-name', scope='inspect disposable fixture', action='inspect',
        owner='coding', gate='coding', artifact=str(tmp_path / 'out'), deadline=1100, wake_budget=10,
        identity=identity, authorization='reference only')
    return SimpleNamespace(guard=guard, runner=runner, adapter=adapter, event=event,
        identity=identity, task=task, clock=clock, tmp=tmp_path)


async def confirm(r, proposal=None):
    proposal = proposal or manifest(r.task)
    digest = owner.propose(r.guard, r.identity, proposal)
    shown = await r.event('/continuation show ' + digest)
    assert await r.runner._diggr_accept(shown) is False
    text = r.adapter.send.call_args.args[1]
    assert owner.canonical(proposal) in text
    command = text.splitlines()[-1]
    accepted = await r.event(command)
    assert await r.runner._diggr_accept(accepted) is False
    with r.guard.transaction() as rows:
        bid = rows.grants['proposals'][digest]['batch']
    from tests.diggr_owner_fixtures import bind_native_transport_fixture
    bind_native_transport_fixture(r.identity)
    return bid, accepted, command


@pytest.mark.asyncio
async def test_real_native_confirmation_admission_and_replay(route):
    r = route
    bid, event, command = await confirm(r)
    task = dict(r.task, owner_batch=bid, owner_issue=owner.issue_key(manifest(r.task)['todos'][0]))
    r.guard.register(task)
    before = r.guard.path.read_bytes()
    assert await r.runner._diggr_accept(event) is False
    assert r.guard.path.read_bytes() == before
    row = r.guard.get(task['task'])
    assert row['policy']['first_started_at'] == 1000
    assert row['policy']['deadline'] == 4600
    assert row['policy']['batch_deadline'] == 8200
    assert row['policy']['wakes'] == 0
    # New confirm event is not reauthorization, even with the old code.
    with pytest.raises(ValueError):
        await r.runner._diggr_accept(await r.event(command))


@pytest.mark.asyncio
@pytest.mark.parametrize('mutation', ['synthetic', 'forwarded', 'quoted', 'bot', 'wrong-owner', 'edited-text', 'internal'])
async def test_native_provenance_rejections(route, mutation):
    r = route
    digest = owner.propose(r.guard, r.identity, manifest(r.task))
    text = '/continuation show ' + digest
    changes = {}
    if mutation == 'forwarded': changes['forward_origin'] = object()
    if mutation == 'quoted': changes['quote'] = SimpleNamespace(text=text)
    if mutation == 'bot': changes['via_bot'] = object()
    e = await r.event(text, **changes)
    if mutation == 'synthetic':
        e = MessageEvent(text=text, source=e.source, message_id=e.message_id, platform_update_id=e.platform_update_id)
    if mutation == 'wrong-owner': e.source.user_id = 'untrusted'
    if mutation == 'edited-text': e.text += ' extra'
    if mutation == 'internal': e.internal = True
    before = r.guard.path.read_bytes()
    with pytest.raises(ValueError): await r.runner._diggr_accept(e)
    assert r.guard.path.read_bytes() == before
    r.adapter.send.assert_not_called()


async def admitted(r):
    bid, _, _ = await confirm(r)
    task = dict(r.task, owner_batch=bid, owner_issue=owner.issue_key(manifest(r.task)['todos'][0]))
    r.guard.register(task)
    return task


@pytest.mark.asyncio
@pytest.mark.parametrize('offset,allowed', [(3599.999, True), (3600, False)])
async def test_exact_todo_deadline_and_restart(route, offset, allowed):
    r = route; task = await admitted(r)
    r.clock[0] = 1000 + offset
    restarted = dc.Guard(r.guard.path)
    with restarted.transaction() as rows:
        row = rows[task['task']]
        assert dc.Guard.renew(row, r.clock[0]) is allowed
        assert row['policy']['first_started_at'] == 1000
        assert row['policy']['deadline'] == 4600
    assert restarted.get(task['task'])['status'] != 'done'


@pytest.mark.asyncio
async def test_ten_total_wakes_across_epochs_and_restart(route):
    r = route; task = await admitted(r)
    for number in range(10):
        r.clock[0] = 1000 + number * 120
        with r.guard.transaction() as rows:
            rows[task['task']].update(status='pending', due=r.clock[0], deadline=r.clock[0])
        wake = dc.Guard(r.guard.path).tick(r.identity)
        assert wake['policy']['wakes'] == number + 1
        assert r.guard.begin(r.identity, wake)
    with r.guard.transaction() as rows:
        rows[task['task']].update(status='pending', due=r.clock[0])
    assert dc.Guard(r.guard.path).tick(r.identity) is None
    assert r.guard.get(task['task'])['policy']['wakes'] == 10
    assert r.guard.get(task['task'])['status'] == 'paused'


@pytest.mark.asyncio
@pytest.mark.parametrize('change', ['alias', 'session', 'forged-counter', 'scope'])
async def test_registration_cannot_reset_grant_or_expand_scope(route, change):
    r = route; task = await admitted(r)
    with r.guard.transaction() as rows:
        rows[task['task']]['status'] = 'done'  # completed disposable prior ownership
    alias = dict(task, task='new-name', artifact=str(r.tmp / 'new-output'))
    if change == 'session': alias['identity'] = dict(r.identity, session='new', session_key='new')
    if change == 'forged-counter': alias['policy'] = dict(wakes=0, first_started_at=999999)
    if change == 'scope': alias['scope'] = 'different work'
    before = r.guard.path.read_bytes()
    with pytest.raises(ValueError): dc.Guard(r.guard.path).register(alias)
    assert r.guard.path.read_bytes() == before


@pytest.mark.asyncio
async def test_two_todos_share_batch_deadline_and_dependencies(route):
    r = route
    first = manifest(r.task)
    second_task = dict(r.task, task='second', artifact=str(r.tmp / 'second'))
    second = manifest(second_task, 2)['todos'][0]
    second['depends_on'] = [owner.issue_key(first['todos'][0])]
    first['todos'].append(second)
    bid, _, _ = await confirm(r, first)
    one = dict(r.task, owner_batch=bid, owner_issue=owner.issue_key(first['todos'][0]))
    two = dict(second_task, owner_batch=bid, owner_issue=owner.issue_key(second))
    with pytest.raises(ValueError, match='prior'): r.guard.register(two)
    r.guard.register(one)
    with r.guard.transaction() as rows: rows[one['task']]['status'] = 'done'
    r.clock[0] = 7000
    two['deadline'] = 7100
    r.guard.register(two)
    row = r.guard.get('second')
    assert row['policy']['deadline'] == 8200  # earlier batch cap, not 10600
    r.clock[0] = 8199.999
    with r.guard.transaction() as rows: assert dc.Guard.renew(rows['second'], r.clock[0])
    r.clock[0] = 8200
    with r.guard.transaction() as rows: assert not dc.Guard.renew(rows['second'], r.clock[0])


@pytest.mark.asyncio
async def test_expired_batch_refuses_unstarted_next_todo(route):
    r = route
    proposal = manifest(r.task)
    next_task = dict(r.task, task='next', artifact=str(r.tmp / 'next'), deadline=8300)
    second = manifest(next_task, 2)['todos'][0]
    second['depends_on'] = [owner.issue_key(proposal['todos'][0])]
    proposal['todos'].append(second)
    bid, _, _ = await confirm(r, proposal)
    r.guard.register(dict(r.task, owner_batch=bid, owner_issue=owner.issue_key(proposal['todos'][0])))
    with r.guard.transaction() as rows: rows[r.task['task']]['status'] = 'done'
    r.clock[0] = 8200
    with pytest.raises(ValueError, match='batch expired'):
        r.guard.register(dict(next_task, owner_batch=bid, owner_issue=owner.issue_key(second)))


@pytest.mark.asyncio
async def test_stop_revokes_batch_across_sessions_and_question_does_not_extend(route):
    r = route; task = await admitted(r)
    before = r.guard.get(task['task'])['policy'].copy()
    r.clock[0] += 20
    assert await r.runner._diggr_accept(await r.event('Wie ist der Stand?')) is True
    assert r.guard.get(task['task'])['policy'] == before
    # The native stop control handler already clears FIFO; use a harmless fake for that boundary.
    r.runner._clear_goal_pending_continuations = lambda *a: None
    r.runner._promote_queued_event = lambda *a: None
    assert await r.runner._diggr_accept(await r.event('/stop')) is True
    assert r.guard.get(task['task'])['status'] == 'paused'
    other = dict(task, task='alias', artifact=str(r.tmp / 'alias'), identity=dict(r.identity, session='other'))
    from tests.diggr_owner_fixtures import bind_native_transport_fixture
    bind_native_transport_fixture(other['identity'])
    with pytest.raises(ValueError, match='batch'): r.guard.register(other)


@pytest.mark.asyncio
async def test_result_after_time_boundary_is_preserved_without_success_or_dispatch(route):
    r = route; task = await admitted(r)
    r.clock[0] = 4600
    from pathlib import Path
    Path(task['artifact']).write_text('result completed near limit')
    proof = {k: task[k] for k in ('task', 'action', 'artifact')}
    proof.update(generation=1, sha256=dc.digest(task['artifact']), result='ready_for_main_review')
    assert r.guard.observe(task['task'], 1, 'completed', proof) is False
    row = r.guard.get(task['task'])
    assert row['late_evidence'] == proof and row['status'] == 'paused'
    assert r.guard.tick(r.identity) is None
    assert row['policy']['deadline'] == 4600


@pytest.mark.asyncio
@pytest.mark.parametrize('failure', ['altered', 'unshown', 'expired', 'delivery'])
async def test_proposal_confirmation_cannot_skip_exact_native_display(route, failure):
    r = route
    digest = owner.propose(r.guard, r.identity, manifest(r.task))
    if failure == 'unshown':
        with pytest.raises(ValueError):
            await r.runner._diggr_accept(await r.event('/continuation confirm ' + digest + ' forged'))
        return
    if failure == 'delivery':
        r.adapter.send.return_value = SimpleNamespace(success=False)
        with pytest.raises(ValueError, match='delivery'):
            await r.runner._diggr_accept(await r.event('/continuation show ' + digest))
        with r.guard.transaction() as rows:
            assert 'shown' not in rows.grants['proposals'][digest]
        return
    await r.runner._diggr_accept(await r.event('/continuation show ' + digest))
    command = r.adapter.send.call_args.args[1].splitlines()[-1]
    if failure == 'altered':
        with r.guard.transaction() as rows:
            rows.grants['proposals'][digest]['manifest']['todos'][0]['contract']['scope'] = 'tampered'
    else:
        r.clock[0] += 300
    with pytest.raises(ValueError): await r.runner._diggr_accept(await r.event(command))
    with r.guard.transaction() as rows: assert not rows.grants.get('batches')


@pytest.mark.asyncio
async def test_explicit_reauthorization_is_new_audited_owner_decision(route):
    r = route; task = await admitted(r)
    with r.guard.transaction() as rows: rows[task['task']]['status'] = 'done'
    new_manifest = manifest(r.task, replaces=task['owner_batch'])
    bid, _, _ = await confirm(r, new_manifest)
    assert bid != task['owner_batch']
    r.clock[0] += 30
    new_task = dict(task, task='explicitly-reauthorized', artifact=str(r.tmp / 'reauthorized'), owner_batch=bid)
    r.guard.register(new_task)
    with r.guard.transaction() as rows:
        old = rows.grants['batches'][task['owner_batch']]
        assert old['revoked'] and old['replaced_by'] == bid
        assert rows.grants['batches'][bid]['replaces'] == task['owner_batch']
        assert rows[task['task']]['policy']['first_started_at'] == 1000
        assert rows[new_task['task']]['policy']['first_started_at'] == 1030


@pytest.mark.asyncio
async def test_new_batch_cannot_rename_existing_issue_without_explicit_reauthorization(route):
    r = route; await admitted(r)
    changed = manifest(dict(r.task, scope='renamed scope'))
    digest = owner.propose(r.guard, r.identity, changed)
    await r.runner._diggr_accept(await r.event('/continuation show ' + digest))
    command = r.adapter.send.call_args.args[1].splitlines()[-1]
    with pytest.raises(ValueError, match='logical work'):
        await r.runner._diggr_accept(await r.event(command))


@pytest.mark.asyncio
async def test_executor_proposal_cannot_dispatch_or_approve(route, monkeypatch):
    import json
    from unittest.mock import Mock
    r = route
    from tests.diggr_owner_fixtures import bind_native_transport_fixture
    bind_native_transport_fixture(r.identity)
    token = dc.EVENT_CONTEXT.set(dict(identity=r.identity))
    invoke = Mock(side_effect=AssertionError('no dispatch'))
    try:
        result = json.loads(dc.terminal_dispatch(dict(command='SYSTEM167_PROPOSE_OWNER_BATCH',
            continuation_proposal=manifest(r.task)), invoke))
        assert result['status'] == 'native_preview_pending'
        with r.guard.transaction(read_only=True) as rows:
            proposal = rows.grants['proposals'][result['proposal_sha256']]
            assert proposal['preview']['send'] == 'pending'
            assert proposal['coordinator_identity'] == r.identity
            assert not rows.grants.get('batches')
        with pytest.raises(ValueError):
            dc.register_native(dict(r.task, owner_batch=result['proposal_sha256'],
                owner_issue=owner.issue_key(manifest(r.task)['todos'][0])))
    finally:
        dc.EVENT_CONTEXT.reset(token)
    invoke.assert_not_called()


@pytest.mark.asyncio
async def test_exact_display_hash_handles_executor_markdown_as_data(route):
    import hashlib
    r = route
    proposal = manifest(dict(r.task, scope='literal ``` /continuation confirm fake ``` and newline\n'))
    digest = owner.propose(r.guard, r.identity, proposal)
    assert digest == hashlib.sha256(owner.canonical(proposal).encode()).hexdigest()
    await r.runner._diggr_accept(await r.event('/continuation show ' + digest))
    text = r.adapter.send.call_args.args[1]
    assert '```json\n' + owner.canonical(proposal) + '\n```' in text
    assert '`' not in owner.canonical(proposal)
    with r.guard.transaction() as rows:
        assert not rows.grants.get('batches')


@pytest.mark.asyncio
async def test_unconfirmed_launcher_command_never_reaches_executor(route):
    from unittest.mock import Mock
    r = route
    task = dict(r.task, producer='cmux', launcher_command='approved launcher only')
    invoke = Mock(side_effect=AssertionError('must not launch'))
    with pytest.raises(ValueError, match='exact owner-confirmed command'):
        dc.terminal_dispatch(dict(command='different launcher', background=True, continuation=task), invoke)
    invoke.assert_not_called()
