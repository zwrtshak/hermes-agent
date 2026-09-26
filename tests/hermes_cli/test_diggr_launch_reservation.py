"""SYSTEM-167: disposable native state; OS, TTY, clock and transport boundaries only."""
from tests.diggr_owner_fixtures import authorize

import copy
import json
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from hermes_cli import diggr_continuation as dc


@pytest.fixture
def rig(tmp_path, monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr(dc.time, 'time', lambda: clock[0])
    monkeypatch.setenv('HOME', str(tmp_path))
    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    identity = dict(home=str(tmp_path), profile='diggr-main', session='session',
                    session_key='session-key', platform='cli', chat_id='chat', user_id='user', thread_id='')
    shell = dict(pid=101, ppid=1, pgid=101, tty='ttys001', start='shell-start', executable='/bin/zsh')
    binding = dict(target=dict(workspace='11111111-1111-4111-8111-111111111111',
                               surface='22222222-2222-4222-8222-222222222222'), shell=shell)
    surface = dict(kind='surface', id=binding['target']['surface'], type='terminal', tty=shell['tty'],
                   top_level_pids=[101], root_pids=[101], tty_process_pids=[101], foreground_pgids=[101])
    processes = {101: dict(shell, tpgid=101)}
    monkeypatch.setattr(dc, 'cmux_snapshot', lambda target: dict(kind='workspace', id=target['workspace'], children=[surface]))
    monkeypatch.setattr(dc, 'process_identity', lambda pid: copy.deepcopy(processes[pid]))
    import psutil
    monkeypatch.setattr(psutil, 'pid_exists', lambda pid: pid in processes)
    monkeypatch.setattr(psutil, 'Process', lambda pid: SimpleNamespace(create_time=lambda: 500.0))
    session = SimpleNamespace(id='launcher', session_key=identity['session_key'], started_at=999,
        pid=202, pid_scope='host', exited=True, exit_code=0, completion_reason='exited',
        process=SimpleNamespace(pid=202, poll=lambda: 0), _pty=None)
    registry = SimpleNamespace(get=lambda key: session if key == 'launcher' else None,
                               _reconcile_local_exit=lambda session: None)
    monkeypatch.setitem(sys.modules, 'tools.process_registry', SimpleNamespace(process_registry=registry))
    guard = dc.Guard(tmp_path / 'state.json')
    task = dict(task='SYSTEM-167-fixture', scope='offline regression', identity=identity, owner='coding',
                gate='coding', action='bounded fixture work', artifact=str(tmp_path / 'result.md'),
                deadline=2000, wake_budget=3, authorization='fixture-authorization', ticket_nonce='fixture-nonce',
                producer='cmux', visible_binding=binding, launcher_expected=True, process_id='launcher',
                process_started_at=999, launcher_pid=202,
                launcher_identity=dict(process_id='launcher', session_key=identity['session_key'],
                                       started_at=999, pid=202, exited=True),
                worker_route=dict(argv=['codex', 'exec'], worktree=str(tmp_path)))
    guard.register(authorize(guard, task))
    context = dc.EVENT_CONTEXT.set(dict(identity=identity))
    monkeypatch.setattr(dc, 'runtime_guard', lambda home=None: guard)
    # Route validation is orthogonal; native ticket/claim/retire and TTY checks remain real.
    monkeypatch.setattr(dc, 'route_check', lambda *a, **kw: None)
    monkeypatch.setattr(dc, 'cmux_send', Mock())
    monkeypatch.setattr(dc, 'visible_worker_command', lambda ticket: 'fixture-wrapper')
    with guard.transaction() as rows:
        rows[task['task']]['receipt_sha256'] = 'fixture-route-seal'
    r = SimpleNamespace(guard=guard, task=task, identity=identity, clock=clock, session=session,
                        registry=registry, binding=binding, surface=surface, processes=processes,
                        tmp=tmp_path, monkeypatch=monkeypatch)
    r.row = lambda: guard.get(task['task'])
    r.ticket = lambda: dc.worker_ticket(r.row())
    yield r
    dc.EVENT_CONTEXT.reset(context)


def hashed(path, data):
    path.write_text(json.dumps(data))
    return dict(path=str(path), sha256=dc.digest(path))


def report(r, **overrides):
    row = r.row()
    observation = hashed(r.tmp / 'observation.json', dict(fixture='immutable observations'))
    effect = hashed(r.tmp / 'effect.json', dict(task=row['task'], action_id=row['action_id'],
                    effect_id=row['effect_id'], result='reconciled', effects=[]))
    data = {k: row[k] for k in ('task', 'generation', 'identity', 'action', 'action_id', 'effect_id')}
    data.update(row_sha256=dc.object_hash(row), authorization=row['authorization'],
                observations=[observation], effect_proof=effect, outcome='no_effect',
                worker_result='missing', reason='wrapper rejected without a claim')
    data.update(overrides)
    return hashed(r.tmp / 'report.json', data)


def send(r):
    return dc.terminal_dispatch(dict(command='SYSTEM167_REGISTERED_WORKER', continuation_ticket=r.ticket()),
                                lambda args: pytest.fail('no launcher permitted'))


def fence(r):
    r.guard.observe(r.task['task'], r.row()['generation'], 'unknown')
    r.clock[0] += 20
    wake = r.guard.tick(r.identity)
    assert r.guard.begin(r.identity, wake)
    assert r.guard.failure(r.identity, wake, report(r))
    return dict(task=r.task['task'], generation=r.row()['generation'])


def retire(r, evidence=None, wake=None):
    return dc.maintenance_control(dict(operation='retire', wake=wake or dict(task=r.task['task'],
                         generation=r.row()['generation']), evidence=evidence or report(r)))


def recover(r):
    # Exercise actual recovery and its new reservation, mock only route/worktree checks.
    r.guard.observe(r.task['task'], 1, 'unknown')
    r.clock[0] += 20
    wake = r.guard.tick(r.identity)
    assert r.guard.begin(r.identity, wake)
    old_packet = hashed(r.tmp / 'old-packet.json', dict(fixture=True))
    new_packet = hashed(r.tmp / 'new-packet.json', dict(fixture=True))
    with r.guard.transaction() as rows:
        rows[r.task['task']]['worker_route'].update(packet=old_packet['path'], packet_sha256=old_packet['sha256'])
    route = dict(r.row()['worker_route'], packet=new_packet['path'], packet_sha256=new_packet['sha256'],
                 receipt=str(r.tmp / 'new-receipt.json'), visible_target=r.binding['target'])
    r.monkeypatch.setattr(dc, 'validate_recovery_route', lambda *a: None)
    r.monkeypatch.setattr(dc, 'verify_recovery_worktree', lambda *a: None)
    ticket = r.guard.recover(r.identity, wake, report(r), dict(worker_route=route,
        kind='technical', artifact=str(r.tmp / 'new-result.md'), reason='claim timeout', hypothesis='new bounded preflight'))
    with r.guard.transaction() as rows:
        rows[r.task['task']]['receipt_sha256'] = 'fixture-new-seal'
    return ticket


def claim_without_spawn(r, ticket):
    # Native CLI claim path, foreground boundary supplied; Popen stops before any child.
    pid = 303
    r.processes[pid] = dict(pid=pid, ppid=101, pgid=pid, tpgid=pid, tty='ttys001', start='worker-start', executable='python')
    r.processes[101]['tpgid'] = pid
    r.surface.update(tty_process_pids=[101, pid], foreground_pgids=[pid])
    r.monkeypatch.setattr(dc.os, 'getpid', lambda: pid)
    r.monkeypatch.setattr(dc.os, 'isatty', lambda fd: True)
    r.monkeypatch.setattr(dc.os, 'ttyname', lambda fd: '/dev/ttys001')
    r.monkeypatch.setattr(dc.os, 'tcgetpgrp', lambda fd: pid)
    r.monkeypatch.setattr(dc.os, 'getpgrp', lambda: pid)
    import subprocess
    # Finish a synthetic child normally through the real wrapper, without dispatch.
    child = SimpleNamespace(pid=404, wait=lambda timeout: 1)
    r.processes[404] = dict(r.processes[pid], pid=404)
    spawn = Mock(return_value=child)
    r.monkeypatch.setattr(subprocess, 'Popen', spawn)
    return spawn


def test_native_terminal_rejection_keeps_unknown_effect_and_bounded_diagnostic(rig):
    r = rig
    with r.guard.transaction() as rows:
        row = rows[r.task['task']]
        for key in ('launcher_expected', 'process_id', 'process_started_at',
                    'launcher_pid', 'launcher_identity'):
            row.pop(key, None)
    r.monkeypatch.setattr(dc, 'register_native', lambda task: (r.guard, r.row()))
    response = dict(error='rejected; secret should not persist', exit_code=-1, status='error')
    args = dict(command=r.task.get('launcher_command', 'fixture-launcher'), background=True,
                continuation=dict(r.task, launcher_command=r.task.get('launcher_command', 'fixture-launcher')))
    with pytest.raises(ValueError, match='launch effects unknown'):
        dc.terminal_dispatch(args, lambda launch: json.dumps(response))
    row = r.row()
    assert row['launcher_expected'] is True
    assert row.get('process_id') is None
    assert row.get('worker_pid') is None
    assert row['native_launch_diagnostic'] == dict(
        response_sha256=dc.object_hash(response), outcome='terminal_error',
        status='error', exit_code=-1)
    assert 'secret should not persist' not in json.dumps(row)


def test_recovered_send_uses_fresh_claim_window(rig):
    r = rig
    ticket = recover(r)
    r.clock[0] += 31
    send(r)
    dc.observe_processes(r.guard)
    assert r.row()['status'] == 'running'
    spawn = claim_without_spawn(r, ticket)
    dc.main(['--home', str(r.tmp), 'worker', '--payload-json', json.dumps(ticket)])
    assert spawn.call_count == 1
    with pytest.raises(ValueError, match='UNREGISTERED'):
        dc.main(['--home', str(r.tmp), 'worker', '--payload-json', json.dumps(ticket)])
    assert spawn.call_count == 1


def test_observer_snapshot_cannot_fence_new_claim(rig):
    r = rig
    r.clock[0] += 31
    def get_after_claim(key):
        with r.guard.transaction() as rows:
            rows[r.task['task']].update(worker_pid=303, worker_identity=dict(pid=303, start='worker', executable='python'))
        return r.session
    r.registry.get = get_after_claim
    dc.observe_processes(r.guard)
    assert r.row()['status'] == 'running'
    assert r.row()['generation'] == 1


def test_retirement_releases_distinct_task_and_keeps_old_ticket_fenced(rig):
    r = rig
    old = r.ticket()
    send(r)
    wake = fence(r)
    assert dc.ownership_pending(r.row())  # failure is not release
    evidence = report(r)
    assert retire(r, evidence, wake)
    row = r.row()
    assert row['status'] == 'blocked' and row['effect_status'] == 'unknown'
    assert row['failure_evidence'] and row['reservation_retirement']
    assert not dc.ownership_pending(dc.Guard(r.guard.path).get(row['task']))
    r.guard.register(authorize(r.guard, dict(r.task, task='SYSTEM-167-distinct', artifact=str(r.tmp / 'distinct.md'))))
    with pytest.raises(ValueError, match='UNREGISTERED'):
        dc.validate_ticket({row['task']: row}, old)
    with pytest.raises(ValueError):
        retire(r, evidence, wake)


def test_exact_prelaunch_gateway_rejection_retires_without_invented_launcher(rig):
    r = rig
    with r.guard.transaction() as rows:
        row = rows.pop(r.task['task'])
        row.update(task='APP-104-album-group-new-20260926', generation=3,
            status='blocked', gate='reconcile', effect_status='unknown',
            owner_batch='ff6f91394b8c50844db6a06735f76378e26bdccc135e484b906dd76d555e469f',
            action_id='cac52908fccb449ebf46b44a3a079b7d',
            effect_id='0de8fcd3771545ba9a0f68922ff2c65e',
            native_launch_sha256='ed811def019e6f36ca33c81dd386b95e6a549815e8b4f5f6b3fe20d6b64ca68c',
            native_launch_diagnostic=dict(exit_code=1, outcome='terminal_error',
                response_sha256='8bb817e125655c67191c0c38d8e5384736a71441e04367c7a811a093e18a49f9',
                status='error'), attempt_history=[], launcher_expected=True)
        for key in ('process_id', 'process_started_at', 'launcher_pid', 'launcher_identity',
                    'worker_pid', 'worker_identity', 'visible_sent', 'origin'):
            row.pop(key, None)
        row['worker_route']['receipt'] = str(r.tmp / 'never-sent.json')
        rows[row['task']] = row
    r.task['task'] = 'APP-104-album-group-new-20260926'
    assert dc.prelaunch_rejection_case(r.row())
    assert dc.ownership_pending(r.row())
    evidence = hashed(r.tmp / 'prelaunch-rejection.json', dc.prelaunch_rejection_report(r.row()))
    assert retire(r, evidence)
    row = r.row()
    assert dc.prelaunch_retirement_matches(row)
    assert not dc.ownership_pending(row)
    assert dc.worktree_reserved(row, row['worker_route']['worktree'])
    assert not dc.ownership_pending(dc.Guard(r.guard.path).get(row['task']))
    with pytest.raises(ValueError, match='replay'):
        retire(r, evidence)
    with r.guard.transaction() as rows:
        rows[row['task']]['action'] = 'changed after retirement'
    assert dc.ownership_pending(r.row())


def test_prelaunch_retirement_refuses_changed_diagnostic_or_effect(rig):
    r = rig
    row = r.row()
    assert not dc.prelaunch_rejection_case(row)
    forged = dict(row, reservation_retirement=dict(kind='gateway_lifecycle_rejection_before_execute',
        row_sha256=dc.object_hash(row), evidence=hashed(r.tmp / 'false-report.json',
            dict(kind='gateway_lifecycle_rejection_before_execute'))))
    assert not dc.prelaunch_retirement_matches(forged)


@pytest.mark.parametrize('change', ['owner', 'generation', 'action_id', 'effect_id', 'hash', 'missing',
    'busy', 'shell-reused', 'launcher-alive', 'launcher-reused', 'launcher-missing', 'launcher-exit-missing',
    'worker', 'child', 'worker-identity', 'effect-proof', 'row-hash', 'authorization', 'no-context'])
def test_retirement_refuses_ambiguous_or_changed_proof(rig, change):
    r = rig
    send(r)
    wake = fence(r)
    overrides = {}
    if change in ('generation', 'action_id', 'effect_id', 'row-hash', 'authorization'):
        overrides[{'row-hash': 'row_sha256'}.get(change, change)] = 'wrong'
    if change == 'owner': overrides['identity'] = dict(r.identity, session='foreign')
    if change == 'effect-proof': overrides['effect_proof'] = None
    if change in ('worker', 'child', 'worker-identity'):
        with r.guard.transaction() as rows:
            rows[r.task['task']][{'worker': 'worker_pid', 'child': 'child_identity', 'worker-identity': 'worker_identity'}[change]] = 303
    evidence = report(r, **overrides)
    if change == 'hash': (r.tmp / 'observation.json').write_text('changed')
    if change == 'missing': (r.tmp / 'observation.json').unlink()
    if change == 'busy': r.surface['tty_process_pids'].append(303)
    if change == 'shell-reused': r.processes[101]['start'] = 'replacement'
    if change == 'launcher-alive': r.session.process.poll = lambda: None
    if change == 'launcher-reused': r.processes[202] = dict(pid=202)
    if change == 'launcher-missing': r.registry.get = lambda key: None
    if change == 'launcher-exit-missing': r.session.completion_reason = None
    if change == 'no-context': dc.EVENT_CONTEXT.set(None)
    before = r.guard.path.read_bytes()
    with pytest.raises((ValueError, OSError, KeyError)):
        retire(r, evidence, wake)
    assert r.guard.path.read_bytes() == before


def test_genuine_claim_timeout_and_unsent_reservation_expire(rig):
    r = rig
    ticket = recover(r)
    r.clock[0] += 31
    send(r)
    r.clock[0] += 30
    with pytest.raises(ValueError, match='UNREGISTERED'):
        dc.validate_ticket({r.task['task']: r.row()}, ticket)
    dc.observe_processes(r.guard)
    assert r.row()['gate'] == 'reconcile'
    assert r.row()['status'] == 'pending'


def test_unsent_recovery_is_bounded_without_observer(rig):
    r = rig
    ticket = recover(r)
    r.clock[0] += 60
    with pytest.raises(ValueError, match='UNREGISTERED'):
        send(r)
    dc.cmux_send.assert_not_called()
    dc.observe_processes(r.guard)
    assert r.row()['gate'] == 'reconcile'
    with pytest.raises(ValueError, match='UNREGISTERED'):
        dc.validate_ticket({r.task['task']: r.row()}, ticket)


def test_claim_deadline_cannot_extend_authorization(rig):
    r = rig
    with r.guard.transaction() as rows:
        rows[r.task['task']]['authorization_expires_at'] = r.clock[0] + 5
    ticket = r.ticket()
    send(r)
    r.clock[0] += 5
    with pytest.raises(ValueError, match='UNREGISTERED'):
        dc.validate_ticket({r.task['task']: r.row()}, ticket)


@pytest.mark.parametrize('change', ['row', 'report', 'observation', 'effect'])
def test_retirement_receipt_invalidates_on_changed_evidence(rig, change):
    r = rig
    send(r)
    fence(r)
    retire(r)
    assert not dc.ownership_pending(r.row())
    if change == 'row':
        with r.guard.transaction() as rows:
            rows[r.task['task']]['reason'] = 'changed'
    else:
        (r.tmp / (change + '.json')).write_text('changed')
    assert dc.ownership_pending(r.row())


@pytest.mark.parametrize('reuse', ['task', 'artifact', 'historical-artifact'])
def test_retirement_never_authorizes_reuse(rig, reuse):
    r = rig
    send(r)
    fence(r)
    retire(r)
    new = dict(r.task, task='distinct', artifact=str(r.tmp / 'new.md'))
    if reuse == 'task': new['task'] = r.task['task']
    if reuse == 'artifact': new['artifact'] = r.task['artifact']
    if reuse == 'historical-artifact':
        with r.guard.transaction() as rows:
            row = rows[r.task['task']]
            row['history'] = [dict(artifact=str(r.tmp / 'historic.md'))]
        new['artifact'] = str(r.tmp / 'historic.md')
    with pytest.raises(ValueError):
        r.guard.register(new)


def test_terminal_retirement_requires_current_native_owner(rig):
    r = rig
    send(r)
    fence(r)
    evidence = report(r)
    token = dc.EVENT_CONTEXT.set(dict(identity=dict(r.identity, session='foreign')))
    try:
        with pytest.raises(ValueError):
            retire(r, evidence)
    finally:
        dc.EVENT_CONTEXT.reset(token)
    with pytest.raises(ValueError):
        retire(r, evidence, dict(task=r.task['task'], generation=r.row()['generation'] - 1))


@pytest.mark.parametrize('status', ['running', 'pending', 'queued', 'executing'])
def test_retirement_never_releases_active_status(rig, status):
    r = rig
    send(r)
    with r.guard.transaction() as rows:
        rows[r.task['task']]['status'] = status
    before = r.guard.path.read_bytes()
    with pytest.raises(ValueError):
        retire(r)
    assert r.guard.path.read_bytes() == before


def test_retirement_preserves_claimed_worker_exit_requirements(rig):
    r = rig
    send(r)
    with r.guard.transaction() as rows:
        rows[r.task['task']].update(worker_pid=303,
            worker_identity=dict(pid=303, start='worker-start', executable='python'),
            child_identity=dict(pid=404, start='child-start', executable='codex'), child_exited=False)
    fence(r)
    r.processes[303] = dict(pid=303)
    assert dc.ownership_pending(r.row())
    with pytest.raises(ValueError):
        retire(r)
    del r.processes[303]
    r.processes[404] = dict(pid=404)
    with pytest.raises(ValueError):
        retire(r)
    del r.processes[404]
    assert retire(r)
    assert r.row()['worker_pid'] == 303
    assert r.row()['child_identity']['pid'] == 404
    assert r.row()['status'] == 'blocked'


def test_late_send_after_retirement_is_rejected_before_transport(rig):
    r = rig
    from contextlib import contextmanager
    transaction = r.guard.transaction
    calls = [0]
    @contextmanager
    def interleave():
        calls[0] += 1
        # send has durably reserved, but has not reacquired its I/O lock yet.
        if calls[0] == 2:
            r.guard.transaction = transaction
            fence(r)
            retire(r)
        with transaction() as rows:
            yield rows
    ticket = r.ticket()
    r.guard.transaction = interleave
    with pytest.raises(ValueError, match='UNREGISTERED'):
        dc.terminal_dispatch(dict(command='SYSTEM167_REGISTERED_WORKER', continuation_ticket=ticket),
                             lambda args: pytest.fail('dispatch forbidden'))
    dc.cmux_send.assert_not_called()
    assert not dc.ownership_pending(r.row())


def test_delayed_claim_waits_for_retirement_lock_and_cannot_spawn(rig):
    r = rig
    import subprocess
    import threading
    from contextlib import contextmanager
    ticket = r.ticket()
    send(r)
    fence(r)
    evidence = report(r)
    attempted = threading.Event()
    results = []
    transaction = r.guard.transaction
    @contextmanager
    def observed_transaction():
        if threading.current_thread().name == 'delayed-claim':
            attempted.set()
        with transaction() as rows:
            yield rows
    r.guard.transaction = observed_transaction
    spawn = Mock(side_effect=AssertionError('no child permitted'))
    r.monkeypatch.setattr(subprocess, 'Popen', spawn)
    def worker():
        try:
            dc.main(['--home', str(r.tmp), 'worker', '--payload-json', json.dumps(ticket)])
        except ValueError as exc:
            results.append(str(exc))
    thread = threading.Thread(target=worker, name='delayed-claim')
    original = dc.verify_visible_target
    def while_locked(binding, idle=False):
        result = original(binding, idle=idle)
        thread.start()
        assert attempted.wait(5)
        return result
    r.monkeypatch.setattr(dc, 'verify_visible_target', while_locked)
    assert retire(r, evidence)
    thread.join(5)
    assert not thread.is_alive()
    assert len(results) == 1 and 'UNREGISTERED' in results[0]
    spawn.assert_not_called()


def test_send_failure_does_not_fence_concurrent_claim(rig):
    r = rig
    # Transport error may arrive after its lock is released. Simulate the claim
    # just before the error handler submits its copied unclaimed observation.
    original = r.guard.observe
    def after_claim(*args, **kwargs):
        with r.guard.transaction() as rows:
            rows[r.task['task']].update(worker_pid=303)
        return original(*args, **kwargs)
    r.monkeypatch.setattr(r.guard, 'observe', after_claim)
    dc.cmux_send.side_effect = OSError('uncertain transport')
    with pytest.raises(OSError):
        send(r)
    assert r.row()['status'] == 'running'


def test_retire_bridge_has_no_freeform_dispatch_authority(rig):
    r = rig
    send(r)
    fence(r)
    payload = dict(operation='retire', wake=dict(task=r.task['task'], generation=r.row()['generation']),
                   evidence=report(r))
    invoke = Mock(side_effect=AssertionError('no dispatch'))
    with pytest.raises(ValueError):
        dc.terminal_dispatch(dict(command='arbitrary shell', continuation_control=payload), invoke)
    assert json.loads(dc.terminal_dispatch(dict(command='SYSTEM168_CONTINUATION_CONTROL',
                           continuation_control=payload), invoke)) is True
    invoke.assert_not_called()


def test_observer_preserves_ordinary_non_cmux_completion(rig):
    r = rig
    with r.guard.transaction() as rows:
        rows[r.task['task']]['producer'] = 'codex'
    artifact = r.tmp / 'result.md'
    artifact.write_text('synthetic worker result')
    dc.observe_processes(r.guard)
    row = r.row()
    assert row['gate'] == 'main_validation' and row['status'] == 'pending'
    assert row['evidence']['sha256'] == dc.digest(artifact)


def test_sent_claim_window_does_not_turn_launcher_exit_into_success(rig):
    r = rig
    send(r)
    (r.tmp / 'result.md').write_text('unclaimed artifact cannot prove completion')
    dc.observe_processes(r.guard)
    assert r.row()['status'] == 'running'
    r.clock[0] += 30
    dc.observe_processes(r.guard)
    assert r.row()['gate'] == 'reconcile'
    assert r.row()['evidence'] is None


def test_retirement_cannot_satisfy_done_or_acceptance(rig):
    r = rig
    send(r)
    wake = fence(r)
    failure = copy.deepcopy(r.row()['failure_evidence'])
    retire(r)
    assert r.row()['failure_evidence'] == failure
    assert not r.guard.ack(r.identity, wake, {}, 'done', 'complete')
    assert not r.guard.renew(r.row(), 3000)
    assert r.row()['status'] == 'blocked'
    with pytest.raises(ValueError):
        r.guard.recover(r.identity, wake, {}, {})


def test_retirement_detects_evidence_change_during_exit_check(rig):
    r = rig
    send(r)
    fence(r)
    evidence = report(r)
    before = r.guard.path.read_bytes()
    def changed():
        (r.tmp / 'effect.json').write_text('changed')
        return 0
    r.session.process.poll = changed
    with pytest.raises(ValueError, match='hash mismatch'):
        retire(r, evidence)
    assert r.guard.path.read_bytes() == before


def test_target_reservation_is_released_only_by_valid_retirement(rig):
    r = rig
    send(r)
    fence(r)
    other = dict(r.task, task='other-session', identity=dict(r.identity, session='other'),
                 artifact=str(r.tmp / 'other.md'))
    with pytest.raises(ValueError, match='visible target'):
        r.guard.register(authorize(r.guard, other))
    retire(r)
    r.guard.register(authorize(r.guard, other))


def test_recovery_retains_original_launcher_proof_and_history(rig):
    r = rig
    before = r.row()
    recover(r)
    row = r.row()
    for key in ('process_id', 'process_started_at', 'launcher_identity', 'launcher_pid'):
        assert row[key] == before[key]
        assert row['history'][0][key] == before[key]
    assert row['claim_phase'] == 'reserved'
    assert row['claim_deadline'] == r.clock[0] + 60


@pytest.mark.parametrize('change', ['owner', 'start', 'pid', 'code', 'scope', 'handle'])
def test_retirement_requires_exact_launcher_handle_and_registry(rig, change):
    r = rig
    send(r)
    fence(r)
    evidence = report(r)
    if change == 'owner': r.session.session_key = 'foreign'
    if change == 'start': r.session.started_at = 1
    if change == 'pid': r.session.pid = 999
    if change == 'code': r.session.exit_code = None
    if change == 'scope': r.session.pid_scope = 'container'
    if change == 'handle': r.session.process = None
    before = r.guard.path.read_bytes()
    with pytest.raises(ValueError):
        retire(r, evidence)
    assert r.guard.path.read_bytes() == before


@pytest.mark.parametrize('change', ['effect-id', 'effect-result', 'report-hash', 'effect-hash'])
def test_retirement_does_not_trust_plain_no_effect_assertion(rig, change):
    r = rig
    send(r)
    fence(r)
    evidence = report(r)
    data = json.loads((r.tmp / 'report.json').read_text())
    if change in ('effect-id', 'effect-result'):
        effects = json.loads((r.tmp / 'effect.json').read_text())
        effects['effect_id' if change == 'effect-id' else 'result'] = 'unverified'
        data['effect_proof'] = hashed(r.tmp / 'effect.json', effects)
        evidence = hashed(r.tmp / 'report.json', data)
    elif change == 'report-hash':
        evidence['sha256'] = 'wrong'
    else:
        (r.tmp / 'effect.json').write_text('changed')
    before = r.guard.path.read_bytes()
    with pytest.raises(ValueError):
        retire(r, evidence)
    assert r.guard.path.read_bytes() == before


def test_claimed_attempt_without_child_exit_proof_remains_owned(rig):
    r = rig
    send(r)
    with r.guard.transaction() as rows:
        rows[r.task['task']].update(worker_pid=303,
            worker_identity=dict(pid=303, start='worker', executable='python'))
    fence(r)
    assert dc.ownership_pending(r.row())
    with pytest.raises(ValueError, match='child exit unknown'):
        retire(r)
