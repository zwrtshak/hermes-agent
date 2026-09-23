"""Native maintenance integration: real handlers/guards, disposable state, fake OS only.

Recovery assertions replace incompatible donor CLI/change-action contracts.
The 59 original repair cases remain; their fixture now obtains a native owner grant.
"""
import copy
import json
import subprocess
from pathlib import Path
from unittest.mock import Mock

import pytest
from hermes_cli import diggr_continuation as dc
from tests.hermes_cli.test_diggr_launch_reservation import rig, report, hashed, fence, send


REAL_CHECK_OUTPUT = subprocess.check_output
REAL_ROUTE_CHECK = dc.route_check


@pytest.fixture
def native_recovery(rig):
    r = rig
    worktree = r.tmp / 'work'
    worktree.mkdir()
    route = dict(worktree=str(worktree), branch='hermes/WP-SYSTEM-167-fixture', plane_id='SYSTEM-167',
                 visible_target=r.binding['target'], receipt=str(r.tmp / 'old-receipt.json'))
    packet = dict(routing_mode='non_cmm', operator_scope_authorized=True, plane_id='SYSTEM-167',
                  task_id='SYSTEM-167', scope_id='SYSTEM-167', worktree=str(worktree), branch=route['branch'],
                  base='a'*40, requested_actions=['inspect', 'test'], coding_route=dict(executor='codex',
                      required_model='gpt-6-astra', fallback_models_allowed=[], merge_authorized=False,
                      routine_actions_authorized=['inspect', 'test']))
    pin = hashed(r.tmp / 'old-packet.json', packet)
    route.update(packet=pin['path'], packet_sha256=pin['sha256'], argv=['/fixture/codex', 'exec',
        '--model', 'gpt-6-astra', '--sandbox', 'workspace-write', '--cd', str(worktree),
        '--output-last-message', r.task['artifact'], 'inspect fixture only'])
    with r.guard.transaction() as rows:
        rows[r.task['task']]['worker_route'] = route
    r.guard.observe(r.task['task'], 1, 'unknown')
    r.clock[0] += 20
    r.wake = r.guard.tick(r.identity)
    assert r.guard.begin(r.identity, r.wake)
    fresh = copy.deepcopy(route)
    pin = hashed(r.tmp / 'fresh-packet.json', packet)
    fresh.update(packet=pin['path'], packet_sha256=pin['sha256'], receipt=str(r.tmp / 'fresh-receipt.json'))
    artifact = str(r.tmp / 'fresh-result.md')
    fresh['argv'][fresh['argv'].index('--output-last-message')+1] = artifact
    r.strategy = dict(kind='technical', reason='bounded fixture retry', hypothesis='fresh inspection', artifact=artifact, worker_route=fresh)
    r.evidence = report(r)
    # Concrete changed approach: old surface/shell gone, exact new target verified.
    target = dict(r.binding['target'], surface='44444444-4444-4444-8444-444444444444')
    fresh['visible_target'] = target
    old_process = r.processes.pop(101)
    r.processes[111] = dict(old_process, pid=111, pgid=111, tpgid=111, start='replacement-shell')
    r.surface.update(id=target['surface'], top_level_pids=[111], root_pids=[111],
                     tty_process_pids=[111], foreground_pgids=[111])
    r.monkeypatch.setattr(dc, 'cmux_tree', lambda: dict(kind='workspace', id=target['workspace'], children=[r.surface]))
    # Actual route validators, but no real git/TTY/process probing.
    def git_identity(cmd, **kw):
        cmd = [part for part in cmd if part != '--no-optional-locks']
        assert cmd[:3] == ['git', '-C', str(worktree)]
        return {('branch', '--show-current'): route['branch'],
                ('symbolic-ref', '--quiet', '--short', 'HEAD'): route['branch'], ('rev-parse', 'HEAD'): 'a'*40,
                ('rev-parse', '--show-toplevel'): str(worktree), ('status', '--porcelain'): ''}[tuple(cmd[3:])] + '\n'
    r.monkeypatch.setattr(subprocess, 'check_output', git_identity)
    return r


def request(r, operation='recover', **extra):
    return dict(operation=operation, wake=r.wake, evidence=r.evidence, strategy=r.strategy, **extra)


def test_native_recovery_preserves_action_and_launcher_without_dispatch(native_recovery):
    r = native_recovery
    before = r.row()
    ticket = dc.maintenance_control(request(r))
    after = r.row()
    assert after['action'] == before['worker_action']
    assert after['process_id'] == before['process_id']
    assert after['launcher_identity'] == before['launcher_identity']
    assert after['history'][0]['worker_route'] == before['worker_route']
    assert after['action_id'] != before['action_id'] and after['effect_id'] != before['effect_id']
    assert Path(after['artifact'] + '.reservation').is_file()
    assert not after.get('visible_sent')
    assert not Path(after['artifact']).exists()
    assert dc.worker_ticket(after) == ticket
    dc.cmux_send.assert_not_called()
    with pytest.raises(ValueError): dc.maintenance_control(request(r))


@pytest.mark.parametrize('missing_identity', [False, True])
def test_native_recovery_after_restart_requires_persisted_absent_processes(native_recovery, missing_identity):
    import psutil
    r = native_recovery
    process = psutil.Process
    def absent_process(pid):
        if pid in r.processes:
            return process(pid)
        raise psutil.NoSuchProcess(pid)
    r.monkeypatch.setattr(psutil, 'Process', absent_process)
    with r.guard.transaction() as rows:
        row = rows[r.task['task']]
        row['launcher_identity']['created'] = 500.0
        if missing_identity:
            row['launcher_identity'].pop('created')
    r.registry.get = lambda key: None
    r.evidence = report(r)
    before = r.row()['policy'].copy()
    if missing_identity:
        with pytest.raises(ValueError, match='persisted native launcher identity'):
            dc.maintenance_control(request(r))
        assert r.row()['policy'] == before
        assert not Path(r.strategy['artifact'] + '.reservation').exists()
    else:
        ticket = dc.maintenance_control(request(r))
        assert ticket['task'] == r.task['task']
        after = r.row()['policy']
        assert after['recoveries'] == before['recoveries'] + 1
        assert {k: v for k, v in after.items() if k != 'recoveries'} == {k: v for k, v in before.items() if k != 'recoveries'}
        assert not r.row().get('visible_sent')
    dc.cmux_send.assert_not_called()


@pytest.mark.parametrize('change', ['action', 'argv', 'packet', 'old-target', 'launcher', 'hash', 'legacy-effects'])
def test_recovery_rejects_incompatible_or_incomplete_donor_contract(native_recovery, change):
    r = native_recovery
    if change == 'action': r.strategy['action'] = 'different work'
    if change == 'argv': r.strategy['worker_route']['argv'][-1] = 'different prompt'
    if change == 'packet':
        route = r.strategy['worker_route']
        packet = json.loads(Path(route['packet']).read_text()); packet['requested_actions'].append('deploy')
        route['packet_sha256'] = hashed(Path(route['packet']), packet)['sha256']
    if change == 'old-target':
        r.strategy['worker_route']['visible_target'] = dict(r.binding['target'], surface='33333333-3333-4333-8333-333333333333')
        r.monkeypatch.setattr(dc, 'cmux_tree', lambda: dict(kind='workspace', id=r.binding['target']['workspace'], children=[r.surface]))
    if change == 'launcher': r.registry.get = lambda key: None
    if change == 'hash': (r.tmp / 'observation.json').write_text('changed')
    if change == 'legacy-effects':
        with r.guard.transaction() as rows:
            rows[r.task['task']]['action_id'] = None
        r.evidence = report(r)
    before = r.guard.path.read_bytes()
    with pytest.raises(ValueError): dc.maintenance_control(request(r))
    assert r.guard.path.read_bytes() == before
    assert not Path(r.strategy['artifact'] + '.reservation').exists()
    dc.cmux_send.assert_not_called()


def test_exact_one_use_maintenance_grant(native_recovery):
    r = native_recovery
    grant = dc.maintenance_control(request(r, 'authorize', expires=r.clock[0]+30))
    dc.EVENT_CONTEXT.set(None)
    ticket = dc.maintenance_control(request(r, maintenance=grant, identity=r.identity))
    assert ticket == r.ticket()
    with pytest.raises(ValueError):
        dc.maintenance_control(request(r, maintenance=grant, identity=r.identity))
    dc.cmux_send.assert_not_called()


@pytest.mark.parametrize('change', ['revoked', 'expired', 'strategy', 'nonce', 'identity'])
def test_maintenance_grant_never_expands_authority(native_recovery, change):
    r = native_recovery
    grant = dc.maintenance_control(request(r, 'authorize', expires=r.clock[0]+30))
    if change == 'revoked': dc.maintenance_control(request(r, 'revoke'))
    if change == 'expired': r.clock[0] += 30
    if change == 'strategy': r.strategy['hypothesis'] = 'changed'
    if change == 'nonce': grant['nonce'] = 'foreign'
    identity = dict(r.identity, session='foreign') if change == 'identity' else r.identity
    dc.EVENT_CONTEXT.set(None)
    before = r.guard.path.read_bytes()
    with pytest.raises(ValueError):
        dc.maintenance_control(request(r, maintenance=grant, identity=identity))
    assert r.guard.path.read_bytes() == before


def test_recovery_crash_leaves_exclusive_reservation_not_new_authority(native_recovery):
    r = native_recovery
    before = r.guard.path.read_bytes()
    with r.monkeypatch.context() as patch:
        patch.setattr(dc.os, 'replace', Mock(side_effect=OSError('fixture commit failure')))
        with pytest.raises(OSError): dc.maintenance_control(request(r))
    assert r.guard.path.read_bytes() == before
    assert Path(r.strategy['artifact'] + '.reservation').exists()
    with pytest.raises(FileExistsError): dc.maintenance_control(request(r))


def test_cli_recovery_rejected_before_payload_config_or_state(rig):
    r = rig
    r.monkeypatch.setattr(dc, 'runtime_guard', Mock(side_effect=AssertionError('must not access state')))
    with pytest.raises(SystemExit) as exc:
        dc.main(['--home', str(r.tmp), 'recover', '--payload', str(r.tmp / 'nonexistent.json')])
    assert exc.value.code == 2
    dc.runtime_guard.assert_not_called()


def test_legacy_normalization_does_not_invent_effect_provenance(rig):
    r = rig
    with r.guard.transaction() as rows:
        for field in ('schema_version', 'action_id', 'effect_id'):
            rows[r.task['task']].pop(field)
    row = r.row()
    assert row['schema_version'] == 2 and row['action_id'] is None and row['effect_id'] is None


def test_real_terminal_schema_to_retirement_handler(rig):
    r = rig
    from tools import terminal_tool as terminal
    from tools.registry import registry
    send(r)
    wake = fence(r)
    payload = dict(command='SYSTEM168_CONTINUATION_CONTROL', continuation_control=dict(
        operation='retire', wake=wake, evidence=report(r)))
    schema = registry.get_schema('terminal')['parameters']
    assert set(payload) <= set(schema['properties'])
    control = schema['properties']['continuation_control']
    assert control['type'] == 'object'
    assert 'retire' in control['properties']['operation']['enum']
    assert set(control['required']) <= set(payload['continuation_control'])
    assert set(control['properties']['wake']['required']) <= set(wake)
    native = Mock(side_effect=AssertionError('no shell execution'))
    r.monkeypatch.setattr(terminal, '_diggr_terminal_native', native)
    assert json.loads(registry.get_entry('terminal').handler(payload)) is True
    assert not dc.ownership_pending(r.row())
    native.assert_not_called()


def test_terminal_handler_rejects_contextless_or_freeform_control(rig):
    r = rig
    from tools import terminal_tool as terminal
    from tools.registry import registry
    native = Mock(side_effect=AssertionError('no shell execution'))
    r.monkeypatch.setattr(terminal, '_diggr_terminal_native', native)
    payload = dict(command='arbitrary command', continuation_control=dict(operation='retire', wake={}))
    with pytest.raises(ValueError, match='structured maintenance'):
        registry.get_entry('terminal').handler(payload)
    payload['command'] = 'SYSTEM168_CONTINUATION_CONTROL'
    dc.EVENT_CONTEXT.set(None)
    with pytest.raises(ValueError, match='native Main context'):
        registry.get_entry('terminal').handler(payload)
    native.assert_not_called()


def test_inactive_native_config_is_passive(tmp_path, monkeypatch):
    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    assert dc.runtime_guard(tmp_path) is None
    assert not (tmp_path / 'state').exists()


@pytest.mark.parametrize('placement', ['inside', 'symlink-inside', 'outside', 'dirty'])
def test_recovery_output_placement_real_git_preflight(native_recovery, placement):
    r = native_recovery
    work = Path(r.strategy['worker_route']['worktree'])
    r.monkeypatch.setattr(subprocess, 'check_output', REAL_CHECK_OUTPUT)
    r.monkeypatch.setattr(dc, 'route_check', REAL_ROUTE_CHECK)
    # Commits are confined to this disposable fixture repository; no project commit.
    def git(*args):
        return subprocess.check_output(['git', '-C', str(work), *args], text=True).strip()
    git('init', '-b', r.strategy['worker_route']['branch'])
    git('-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid',
        '-c', 'commit.gpgsign=false', 'commit', '--allow-empty', '-m', 'fixture')
    base = git('rev-parse', 'HEAD')
    with r.guard.transaction() as rows:
        old = rows[r.task['task']]['worker_route']
        packet = json.loads(Path(old['packet']).read_text()); packet['base'] = base
        old['packet_sha256'] = hashed(Path(old['packet']), packet)['sha256']
    route = r.strategy['worker_route']
    route['packet_sha256'] = hashed(Path(route['packet']), packet)['sha256']
    if placement in {'inside', 'symlink-inside'}:
        parent = work
        if placement == 'symlink-inside':
            parent = r.tmp / 'work-alias'; parent.symlink_to(work, target_is_directory=True)
        r.strategy['artifact'] = str(parent / 'new-result.md')
        route['argv'][route['argv'].index('--output-last-message')+1] = r.strategy['artifact']
    if placement == 'dirty': (work / 'unrelated.txt').write_text('must not ignore')
    r.evidence = report(r)
    before = r.guard.path.read_bytes()
    status = git('status', '--porcelain')
    if placement != 'outside':
        error = {'inside': 'outside authorized worktree',
                 'symlink-inside': 'canonical absolute non-symlink', 'dirty': 'clean'}[placement]
        with pytest.raises(ValueError, match=error): dc.maintenance_control(request(r))
        assert r.guard.path.read_bytes() == before
        assert git('status', '--porcelain') == status
        assert not Path(r.strategy['artifact'] + '.reservation').exists()
        assert not Path(route['receipt']).exists()
    else:
        ticket = dc.maintenance_control(request(r))
        assert git('status', '--porcelain') == ''
        assert Path(r.strategy['artifact'] + '.reservation').is_file()
        assert dc.main(['--home', str(r.tmp), 'route-preflight', '--payload-json',
                        json.dumps(dict(ticket=ticket, route=route))]) == 0
        assert r.row()['receipt_sha256'] == dc.digest(route['receipt'])
        assert git('status', '--porcelain') == ''
    dc.cmux_send.assert_not_called()


@pytest.mark.parametrize('kind,counter', [('technical', 'recoveries'), ('review', 'corrections')])
@pytest.mark.parametrize('used,allowed', [(1, True), (2, False)])
def test_total_recovery_budgets_survive_restart(native_recovery, kind, counter, used, allowed):
    r = native_recovery
    with r.guard.transaction() as rows:
        row = rows[r.task['task']]
        row['policy'][counter] = used
        row['recovery_kind'] = kind
    r.strategy['kind'] = kind
    r.evidence = report(r)
    before = r.row()['policy'].copy()
    restarted = dc.Guard(r.guard.path)
    if allowed:
        restarted.recover(r.identity, r.wake, r.evidence, r.strategy)
        after = restarted.get(r.task['task'])['policy']
        assert after[counter] == 2
        for field in ('first_started_at', 'deadline', 'batch_deadline', 'wakes'):
            assert after[field] == before[field]
    else:
        with pytest.raises(ValueError, match='budget exhausted'):
            restarted.recover(r.identity, r.wake, r.evidence, r.strategy)
        assert restarted.get(r.task['task'])['policy'] == before
        assert not Path(r.strategy['artifact'] + '.reservation').exists()


def test_executor_cannot_relabel_technical_retry_as_review(native_recovery):
    r = native_recovery
    r.strategy['kind'] = 'review'
    with pytest.raises(ValueError, match='kind required'):
        dc.maintenance_control(request(r))
    assert not Path(r.strategy['artifact'] + '.reservation').exists()


def test_changed_hypothesis_words_without_changed_native_binding_refused(native_recovery):
    r = native_recovery
    # The already verified original binding is supplied at the actual comparison boundary.
    with r.guard.transaction() as rows:
        rows[r.task['task']]['visible_binding'] = dc.bind_visible_target(r.strategy['worker_route']['visible_target'])
    r.strategy['hypothesis'] = 'entirely different words but same physical approach'
    r.evidence = report(r)
    before = r.guard.path.read_bytes()
    with pytest.raises(ValueError, match='changed wording'):
        dc.maintenance_control(request(r))
    assert r.guard.path.read_bytes() == before
    assert not Path(r.strategy['artifact'] + '.reservation').exists()


def test_review_counter_classification_comes_from_existing_main_gate(native_recovery):
    r = native_recovery
    with r.guard.transaction() as rows:
        rows[r.task['task']]['gate'] = 'main_validation'
    assert r.guard.retry(r.identity, r.wake, dict(reason='changes_requested', detail='fixture review'))
    assert r.row()['recovery_kind'] == 'review'
    r.clock[0] = r.row()['due']
    r.wake = r.guard.tick(r.identity)
    assert r.guard.begin(r.identity, r.wake)
    r.strategy['kind'] = 'review'
    r.evidence = report(r)
    dc.maintenance_control(request(r))
    assert r.row()['policy']['corrections'] == 1
    assert r.row()['policy']['recoveries'] == 0


def test_worker_wait_is_capped_by_remaining_absolute_authority(rig):
    from tests.hermes_cli.test_diggr_launch_reservation import claim_without_spawn
    r = rig
    with r.guard.transaction() as rows:
        rows[r.task['task']]['authorization_expires_at'] = r.clock[0] + 1
    ticket = r.ticket()
    send(r)
    spawn = claim_without_spawn(r, ticket)
    spawn.return_value.wait = Mock(return_value=1)
    dc.main(['--home', str(r.tmp), 'worker', '--payload-json', json.dumps(ticket)])
    spawn.return_value.wait.assert_called_once_with(timeout=1)


def test_worker_timeout_retains_child_for_reconciliation_without_kill(rig):
    from tests.hermes_cli.test_diggr_launch_reservation import claim_without_spawn
    r = rig
    with r.guard.transaction() as rows:
        rows[r.task['task']]['authorization_expires_at'] = r.clock[0] + 1
    ticket = r.ticket()
    send(r)
    spawn = claim_without_spawn(r, ticket)
    def timeout(*, timeout):
        assert timeout == 1
        r.clock[0] += 1
        raise subprocess.TimeoutExpired('synthetic child', timeout)
    spawn.return_value.wait = timeout
    with pytest.raises(subprocess.TimeoutExpired):
        dc.main(['--home', str(r.tmp), 'worker', '--payload-json', json.dumps(ticket)])
    row = r.row()
    assert row['status'] == 'paused'
    assert row['child_identity']['pid'] == 404 and not row.get('child_exited')
    assert r.guard.tick(r.identity) is None
