"""Actual Telegram command handler -> gateway -> durable grant -> Guard; no network."""
import copy
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from gateway.config import GatewayConfig
from gateway.run import GatewayRunner
from gateway.session import SessionStore
from gateway.platforms.base import MessageEvent
from hermes_cli import diggr_continuation as dc
from hermes_cli import diggr_owner as owner


def test_completed_cmux_lane_releases_after_gateway_restart_only_with_exit_proof(monkeypatch):
    from tools.process_registry import process_registry
    row = dict(task='completed-item', status='done', producer='cmux', process_id='old-launcher')
    monkeypatch.setattr(process_registry, 'get', lambda process_id: None)
    checks = []
    monkeypatch.setattr(dc, 'verify_restarted_reservation_exit', lambda value: checks.append(value['task']))
    assert not dc.ownership_pending(row)
    assert checks == ['completed-item']
    def uncertain(value):
        raise ValueError('worker identity unavailable')
    monkeypatch.setattr(dc, 'verify_restarted_reservation_exit', uncertain)
    assert dc.ownership_pending(row)
    row['status'] = 'cancelled'
    monkeypatch.setattr(dc, 'verify_prior_exit', uncertain)
    assert dc.ownership_pending(row)
from tests.gateway.test_telegram_reply_quote import _make_adapter, _make_message


def test_full_onboarding_fixture_does_not_read_host_source(tmp_path, monkeypatch):
    import builtins
    import io
    import shutil
    import yaml
    from pathlib import Path
    from tests.diggr_owner_fixtures import bind_full_onboarding_fixture

    host_source = Path('/Users/diggr/Projects/diggr_claw/docs/skills/diggr-coding-loop-onboarding/SKILL.md').resolve()
    fixture_source = Path(__file__).resolve().parents[1] / 'fixtures' / 'diggr-coding-loop-onboarding' / 'SKILL.md'
    home = tmp_path / 'diggr-main'
    home.mkdir()
    (home / 'codex').mkdir()
    (home / 'config.yaml').write_text(yaml.safe_dump(dict(diggr_continuation=dict(
        enabled=True, profile='diggr-main', home=str(home.resolve())))))
    for key, value in (('HOME', str(home)), ('HERMES_HOME', str(home)),
                       ('CODEX_HOME', str(home / 'codex')), ('HERMES_PLATFORM', 'telegram')):
        monkeypatch.setenv(key, value)

    original_builtin_open = builtins.open
    original_io_open = io.open

    def deny_host_source(opener):
        def guarded(path, *args, **kwargs):
            if isinstance(path, (str, bytes, Path)) and Path(path).resolve() == host_source:
                raise PermissionError('host onboarding source denied in test process')
            return opener(path, *args, **kwargs)
        return guarded

    monkeypatch.setattr(builtins, 'open', deny_host_source(original_builtin_open))
    monkeypatch.setattr(io, 'open', deny_host_source(original_io_open))
    with pytest.raises(PermissionError, match='host onboarding source denied'):
        host_source.read_text(encoding='utf-8')
    with pytest.raises(PermissionError, match='host onboarding source denied'):
        shutil.copyfile(host_source, home / 'denied-copy')

    bind_full_onboarding_fixture(monkeypatch, home)
    assert (home / 'skills' / 'diggr-coding-loop-onboarding' / 'SKILL.md').read_bytes() == fixture_source.read_bytes()


def manifest(task, number=1, replaces=None):
    return dict(budgets=dict(owner.BUDGETS), replaces=replaces, todos=[dict(
        workspace_id='11111111-1111-4111-8111-111111111111',
        project_id='22222222-2222-4222-8222-222222222222',
        issue_id=f'33333333-3333-4333-8333-{number:012d}',
        contract=owner.contract(task), depends_on=[], gates=['owner_confirmed', 'native_checks', 'prior_done'])])


@pytest.fixture
def route(tmp_path, monkeypatch, request):
    full_onboarding = request.node.name in {
        'test_native_v2_review_recovery_preserves_changes_and_passes_real_route_preflight[False]',
        'test_native_v2_review_recovery_preserves_changes_and_passes_real_route_preflight[True]',
    }
    if full_onboarding:
        import yaml
        home = tmp_path / 'diggr-main'
        home.mkdir()
        for key, value in (('HOME', str(home)), ('HERMES_HOME', str(home)),
                           ('CODEX_HOME', str(home / 'codex')),
                           ('HERMES_PLATFORM', 'telegram')):
            monkeypatch.setenv(key, value)
        (home / 'codex').mkdir()
        (home / 'config.yaml').write_text(yaml.safe_dump(dict(diggr_continuation=dict(
            enabled=True, profile='diggr-main', home=str(home.resolve())))))
        import gateway.run as gateway_run
        monkeypatch.setattr(gateway_run, '_hermes_home', home)
    else:
        home = tmp_path
    clock = [1000.0]
    monkeypatch.setattr(dc.time, 'time', lambda: clock[0])
    guard = dc.Guard(home / 'state' / 'diggr-continuation.json', profile='diggr-main') if full_onboarding else dc.Guard(tmp_path / 'state.json')
    if full_onboarding:
        assert dc.runtime_guard(home).path == guard.path
        assert dc.runtime_guard(home).profile == guard.profile
    else:
        monkeypatch.setattr(dc, 'runtime_guard', lambda home=None: guard)
    runner = object.__new__(GatewayRunner)
    runner.config = GatewayConfig()
    runner._resolve_profile_home_for_source = lambda source: home
    runner.session_store = SessionStore(home / 'sessions', runner.config)
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

    identity = dict(home=str(home), profile='diggr-main', platform='telegram',
        chat_id=owner.OWNER, user_id=owner.OWNER, thread_id='', session='fixture', session_key='fixture')
    task = dict(task='display-name', scope='inspect disposable fixture', action='inspect',
        owner='coding', gate='coding', artifact=str(home / 'out'), deadline=1100, wake_budget=10,
        identity=identity, authorization='reference only')
    from tests.diggr_owner_fixtures import bind_onboarded_agent_fixture, bind_full_onboarding_fixture
    if full_onboarding:
        bind_full_onboarding_fixture(monkeypatch, home)
    else:
        bind_onboarded_agent_fixture(monkeypatch)
    return SimpleNamespace(guard=guard, runner=runner, adapter=adapter, event=event,
        identity=identity, task=task, clock=clock, tmp=home)


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
async def test_app104_native_archive_requires_two_owner_events_and_releases_only_admission(route):
    r = route
    r.task.update(task='APP-104-stable-album-children', producer='cmux',
        worker_route=dict(worktree=str(r.tmp / 'old-worktree')),
        launcher_command='fixture-launcher')
    bid, accepted_owner_event, _ = await confirm(r)
    entry = await r.runner.async_session_store.get_or_create_session(accepted_owner_event.source)
    native_identity = r.runner._diggr_identity(accepted_owner_event.source, entry.session_id)
    issue = owner.issue_key(manifest(r.task)['todos'][0])
    r.guard.register(dict(r.task, owner_batch=bid, owner_issue=issue))
    # Construct the historical ledger snapshot in disposable fixture storage.
    # Production never changes an existing origin or grant identifier.
    snapshot = json.loads(r.guard.path.read_text())
    row = snapshot[r.task['task']]
    grants = snapshot['_owner_grants']
    batch = grants['batches'].pop(bid)
    old_bid = '13bd6f74a3e74ce084e43bbe823f59b444d4e76e89dbdffc2d31b8e75866ad62'
    grants['batches'][old_bid] = batch
    batch['coordinator_identity'] = native_identity
    row['identity'] = native_identity
    row['owner_batch'] = old_bid
    row['policy']['batch'] = old_bid
    row.update(generation=3, status='blocked', effect_status='unknown',
        launcher_expected=True, action_id='fe6b4781ab5e4989a923ab65ab3aa6e3',
        effect_id='8993957c2d584e418a42c4d5018d4bf6')
    from hermes_cli import diggr_delivery as delivery
    row['origin'] = delivery.origin_for(owner.State({row['task']: row}, grants), row)
    r.guard.path.write_text(json.dumps(snapshot))
    assert dc.ownership_pending(r.guard.get(r.task['task']))
    with r.guard.transaction(read_only=True) as rows:
        assert owner._app104_archive_candidate(rows, native_identity)
    with pytest.raises(ValueError, match='preview'):
        await r.runner._diggr_accept(await r.event('/continuation archive-confirm fake fake'))
    shown = await r.event('/continuation archive-show')
    assert await r.runner._diggr_accept(shown) is False
    command = r.adapter.send.call_args.args[1].splitlines()[-1]
    assert command.startswith('/continuation archive-confirm ')
    before = r.guard.get(r.task['task'])
    assert 'operator_archive' not in before
    accepted = await r.event(command)
    assert await r.runner._diggr_accept(accepted) is False
    row = r.guard.get(r.task['task'])
    assert row['status'] == 'blocked' and row['effect_status'] == 'unknown'
    assert dc.operator_archive_matches(row)
    assert not dc.ownership_pending(row)
    assert dc.worktree_reserved(row, r.task['worker_route']['worktree'])
    assert row['operator_archive']['historical_effects'] == 'unknown'
    changed = copy.deepcopy(row)
    changed['action'] = 'different historical attempt'
    assert not dc.operator_archive_matches(changed)
    assert dc.ownership_pending(changed)
    with pytest.raises(ValueError, match='exact blocked attempt'):
        await r.runner._diggr_accept(await r.event(command))

    # The old batch remains intact; the exact same issue may receive a new
    # independent native grant only after its historical row is archived.
    fresh = dict(r.task, task='APP-104-second-attempt',
        artifact=str(r.tmp / 'fresh-result'),
        worker_route=dict(worktree=str(r.tmp / 'fresh-worktree')))
    new_bid, _, _ = await confirm(r, manifest(fresh))
    r.guard.register(dict(fresh, owner_batch=new_bid, owner_issue=issue))
    assert r.guard.get(fresh['task'])['status'] == 'running'
    assert r.guard.get(r.task['task'])['operator_archive'] == row['operator_archive']
    with r.guard.transaction(read_only=True) as rows:
        assert rows.grants['batches'][old_bid]['revoked'] is False
        assert rows.grants['batches'][old_bid]['tasks'][issue] == r.task['task']
        assert rows.grants['batches'][new_bid]['tasks'][issue] == fresh['task']


@pytest.mark.asyncio
async def test_app104_handoff_archive_keeps_unknown_history_and_requires_fresh_owner(route, monkeypatch):
    r = route
    r.task.update(task='APP-104-album-group-retired-next-20260926', producer='cmux',
        worker_route=dict(worktree='/Users/diggr/Projects/diggr_app/worktrees/APP-104-album-group-retired-next-20260926'),
        artifact='/Users/diggr/Projects/diggr_claw/ops/state/loop-control/routing_packets/APP-104-retired-next-20260926/coding-result.md',
        launcher_command='fixture-launcher')
    bid, accepted, _ = await confirm(r)
    entry = await r.runner.async_session_store.get_or_create_session(accepted.source)
    identity = r.runner._diggr_identity(accepted.source, entry.session_id)
    issue = owner.issue_key(manifest(r.task)['todos'][0])
    r.guard.register(dict(r.task, owner_batch=bid, owner_issue=issue))
    snapshot = json.loads(r.guard.path.read_text())
    row = snapshot[r.task['task']]
    grants = snapshot['_owner_grants']
    batch = grants['batches'].pop(bid)
    old_bid = '08888f8d40fa3cf91ddb5622124d46352e9c469961dc12056837d35169260a32'
    grants['batches'][old_bid] = batch
    batch['coordinator_identity'] = identity
    row['identity'] = dict(identity, session='historical-session')
    row['owner_batch'] = old_bid
    row['policy']['batch'] = old_bid
    row.update(generation=3, status='blocked', gate='reconcile', effect_status='unknown',
        launcher_expected=True, action_id='8249b6b049f846e3a6790b74d3f6cffb',
        effect_id='d09a1d748819449792f17825723f9028',
        process_id='proc_0b2961fc999d', launcher_pid=38914,
        native_launch_sha256='a5a5370259cf7a9167ca581f78244bd0f93fdd11a0cb5bb92ce80d438f396302',
        receipt_sha256='60a4f518a623898fcf3e648e12d806917088682e808b433f54808986740ca358',
        attempt_history=[])
    from hermes_cli import diggr_delivery as delivery
    row['origin'] = delivery.origin_for(owner.State({row['task']: row}, grants), row)
    r.guard.path.write_text(json.dumps(snapshot))
    checked = []
    def blocked_exit(item):
        raise ValueError('prior terminal alive')
    monkeypatch.setattr(dc, 'verify_restarted_reservation_exit', blocked_exit)
    with pytest.raises(ValueError, match='prior terminal alive'):
        await r.runner._diggr_accept(await r.event('/continuation archive-show'))
    assert 'operator_archive' not in r.guard.get(r.task['task'])
    monkeypatch.setattr(dc, 'verify_restarted_reservation_exit', lambda item: checked.append(item['task']))
    with pytest.raises(ValueError, match='preview'):
        await r.runner._diggr_accept(await r.event('/continuation archive-confirm fake fake'))
    assert checked
    shown = await r.event('/continuation archive-show')
    assert await r.runner._diggr_accept(shown) is False
    command = r.adapter.send.call_args.args[1].splitlines()[-1]
    assert 'historical no-effect' in r.adapter.send.call_args.args[1]
    before = r.guard.get(r.task['task'])
    assert dc.ownership_pending(before)
    changed = copy.deepcopy(before)
    changed['worker_pid'] = 12345
    assert not dc.app104_next_archive_case(changed)
    changed = copy.deepcopy(before)
    changed['process_id'] = 'different-process'
    assert not dc.app104_next_archive_case(changed)
    accepted = await r.event(command)
    assert await r.runner._diggr_accept(accepted) is False
    archived = r.guard.get(r.task['task'])
    assert archived['effect_status'] == 'unknown' and archived['status'] == 'blocked'
    assert dc.operator_archive_matches(archived) and not dc.ownership_pending(archived)
    assert archived['operator_archive']['historical_effects'] == 'unknown'
    assert archived['operator_archive']['identity'] == archived['identity']
    assert archived['operator_archive']['confirming_identity'] == identity
    assert dc.worktree_reserved(archived, r.task['worker_route']['worktree'])
    with pytest.raises(ValueError, match='exact blocked attempt'):
        await r.runner._diggr_accept(await r.event(command))


@pytest.mark.asyncio
async def test_exact_prelaunch_retirement_allows_new_owner_batch_without_reusing_old_grant(route):
    r = route
    r.task.update(task='APP-104-album-group-new-20260926', producer='cmux',
        worker_route=dict(worktree=str(r.tmp / 'old-worktree'),
                          receipt=str(r.tmp / 'never-sent.json')),
        launcher_command='fixture-launcher')
    old_bid, event, _ = await confirm(r)
    entry = await r.runner.async_session_store.get_or_create_session(event.source)
    native_identity = r.runner._diggr_identity(event.source, entry.session_id)
    issue = owner.issue_key(manifest(r.task)['todos'][0])
    r.guard.register(dict(r.task, owner_batch=old_bid, owner_issue=issue))
    snapshot = json.loads(r.guard.path.read_text())
    row = snapshot[r.task['task']]
    grants = snapshot['_owner_grants']
    batch = grants['batches'].pop(old_bid)
    historical_bid = 'ff6f91394b8c50844db6a06735f76378e26bdccc135e484b906dd76d555e469f'
    grants['batches'][historical_bid] = batch
    batch['coordinator_identity'] = native_identity
    row['policy']['batch'] = historical_bid
    row['identity'] = native_identity
    row['owner_batch'] = historical_bid
    row.update(generation=3, status='blocked', gate='reconcile', effect_status='unknown',
        launcher_expected=True,
        action_id='cac52908fccb449ebf46b44a3a079b7d',
        effect_id='0de8fcd3771545ba9a0f68922ff2c65e',
        native_launch_sha256='ed811def019e6f36ca33c81dd386b95e6a549815e8b4f5f6b3fe20d6b64ca68c',
        native_launch_diagnostic=dict(exit_code=1, outcome='terminal_error',
            response_sha256='8bb817e125655c67191c0c38d8e5384736a71441e04367c7a811a093e18a49f9',
            status='error'), attempt_history=[])
    for key in ('process_id', 'process_started_at', 'launcher_pid', 'launcher_identity',
                'worker_pid', 'worker_identity', 'visible_sent', 'send_claim'):
        row.pop(key, None)
    from hermes_cli import diggr_delivery as delivery
    row['origin'] = delivery.origin_for(owner.State({row['task']: row}, grants), row)
    r.guard.path.write_text(json.dumps(snapshot))
    row = r.guard.get(r.task['task'])
    assert dc.ownership_pending(row)
    from tests.hermes_cli.test_diggr_launch_reservation import hashed
    evidence = hashed(r.tmp / 'prelaunch-rejection.json', dc.prelaunch_rejection_report(row))
    context = dc.EVENT_CONTEXT.set(dict(identity=native_identity))
    try:
        assert r.guard.retire_reservation(native_identity,
            dict(task=row['task'], generation=row['generation']), evidence)
    finally:
        dc.EVENT_CONTEXT.reset(context)
    retired = r.guard.get(row['task'])
    assert dc.prelaunch_retirement_matches(retired)
    assert not dc.ownership_pending(retired)
    fresh = dict(r.task, task='APP-104-after-prelaunch-rejection',
        artifact=str(r.tmp / 'fresh-result'),
        worker_route=dict(worktree=str(r.tmp / 'fresh-worktree')))
    new_bid, _, _ = await confirm(r, manifest(fresh))
    assert new_bid != historical_bid
    assert r.guard.get(row['task'])['reservation_retirement'] == retired['reservation_retirement']


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
    with pytest.raises(ValueError, match='logical work'):
        owner.propose(r.guard, r.identity, changed)


@pytest.mark.asyncio
async def test_new_batch_rechecks_overlap_if_prior_batch_appears_after_proposal(route):
    r = route
    changed = manifest(dict(r.task, scope='renamed scope'))
    digest = owner.propose(r.guard, r.identity, changed)
    await r.runner._diggr_accept(await r.event('/continuation show ' + digest))
    command = r.adapter.send.call_args.args[1].splitlines()[-1]
    old_batch, _, _ = await confirm(r)
    with pytest.raises(ValueError, match='logical work'):
        await r.runner._diggr_accept(await r.event(command))
    with r.guard.transaction(read_only=True) as rows:
        assert rows.grants['proposals'][digest].get('batch') is None
        assert set(rows.grants['batches']) == {old_batch}


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


def test_exported_logical_schema_matches_native_sol_proposal(route, monkeypatch):
    import hashlib
    from jsonschema import Draft7Validator
    from tools.terminal_tool import TERMINAL_SCHEMA
    from tests.gateway.test_diggr_logical_admission import prepare

    r = route
    contract, _ = prepare(r, monkeypatch)
    proposal = manifest(r.task)
    proposal['todos'][0]['contract'] = contract
    model = proposal['todos'][0]['contract']['model']
    model.update(required_model='gpt-6-sol', provider='openai-codex',
                 reasoning_effort='high', fallback_models_allowed=[])
    profiles = r.tmp / 'profiles'
    coding = profiles / 'diggr-coding'
    coding.mkdir(parents=True)
    (coding / 'config.yaml').write_text('model:\n  default: gpt-6-sol\nagent:\n  reasoning_effort: high\n')
    identity = dict(r.identity, home=str(profiles / 'diggr-main'))
    schema = TERMINAL_SCHEMA['parameters']['properties']['continuation_proposal']
    validator = Draft7Validator(schema)
    assert validator.is_valid(proposal)
    from tests.diggr_owner_fixtures import bind_native_transport_fixture
    from unittest.mock import Mock
    transport_token = bind_native_transport_fixture(identity)
    event_token = dc.EVENT_CONTEXT.set(dict(identity=identity))
    try:
        result = json.loads(dc.terminal_dispatch(dict(command='SYSTEM167_PROPOSE_OWNER_BATCH',
                                                     continuation_proposal=proposal), Mock(side_effect=AssertionError('no shell'))))
    finally:
        dc.EVENT_CONTEXT.reset(event_token)
        from gateway.session_context import completion_launch_context
        completion_launch_context.reset(transport_token)
    digest = result['proposal_sha256']
    with r.guard.transaction(read_only=True) as rows:
        native = rows.grants['proposals'][digest]
        stored = native['manifest']
    assert stored == proposal
    assert digest == hashlib.sha256(owner.canonical(stored).encode()).hexdigest()
    assert native['preview']['digest'] == digest
    assert owner.canonical(model) in native['preview']['text']
    assert stored['todos'][0]['contract']['model'] == model


@pytest.mark.parametrize('value', ['', 'GPT-6-sol', 'gpt-6-sol\n', 'gpt..6', 'gpt-6/sol'])
def test_exported_model_schema_rejects_same_malformed_ids_as_native(value):
    from jsonschema import Draft7Validator
    from tools.terminal_tool import TERMINAL_SCHEMA
    schema = TERMINAL_SCHEMA['parameters']['properties']['continuation_proposal']
    model_schema = schema['properties']['todos']['items']['properties']['contract']['anyOf'][1]['properties']['model']['properties']['required_model']
    assert not Draft7Validator(model_schema).is_valid(value)
    assert not owner.supported_codex_model(value)


@pytest.mark.parametrize('change', ['fallback', 'extra_model_field', 'wrong_resources'])
def test_exported_logical_schema_and_native_reject_invalid_structure(route, monkeypatch, change):
    from jsonschema import Draft7Validator
    from tools.terminal_tool import TERMINAL_SCHEMA
    from tests.gateway.test_diggr_logical_admission import prepare

    r = route
    contract, _ = prepare(r, monkeypatch)
    proposal = manifest(r.task)
    proposal['todos'][0]['contract'] = contract
    if change == 'fallback':
        contract['model']['fallback_models_allowed'] = ['gpt-6-astra']
    elif change == 'extra_model_field':
        contract['model']['unconfirmed'] = True
    else:
        contract['resources'].pop('allowed_paths')
    schema = TERMINAL_SCHEMA['parameters']['properties']['continuation_proposal']
    assert not Draft7Validator(schema).is_valid(proposal)
    with pytest.raises((ValueError, KeyError)):
        owner.propose(r.guard, r.identity, proposal)


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
