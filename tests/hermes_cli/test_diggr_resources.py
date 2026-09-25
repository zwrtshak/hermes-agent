"""Physical executor release must not unlock an unreviewed candidate writer."""
import copy
import json

import pytest
from hermes_cli import diggr_continuation as dc
from tests.diggr_owner_fixtures import authorize
from tests.hermes_cli.test_diggr_launch_reservation import (
    rig, send, claim_without_spawn, fence, retire,
)
from tests.hermes_cli.test_diggr_native_maintenance import native_recovery, request


def completed(r):
    ticket = r.ticket()
    send(r)
    child = claim_without_spawn(r, ticket)
    child.return_value.wait = lambda timeout: 0
    assert dc.main(['--home', str(r.tmp), 'worker', '--payload-json', json.dumps(ticket)]) == 0
    r.processes.pop(303)
    r.processes.pop(404)
    r.processes[101]['tpgid'] = 101
    r.surface.update(tty_process_pids=[101], foreground_pgids=[101])
    assert r.row()['gate'] == 'main_validation'


def new_task(r, worktree):
    task = copy.deepcopy(r.task)
    task.update(task='separate-task', artifact=str(r.tmp / 'separate-result.md'),
                identity=dict(r.identity, session='separate', session_key='separate-key'),
                worker_route=dict(r.task['worker_route'], worktree=str(worktree)))
    return authorize(r.guard, task)


def test_completed_worker_releases_slot_but_keeps_candidate_and_session(rig):
    r = rig
    completed(r)
    assert dc.ownership_pending(r.row())
    assert not dc.target_reserved(r.row(), r.binding['target'])
    assert dc.worktree_reserved(r.row(), r.task['worker_route']['worktree'])
    other = new_task(r, r.tmp / 'separate-worktree')
    r.guard.register(other)
    assert r.row()['gate'] == 'main_validation'
    assert r.guard.get(other['task'])['status'] == 'running'


def test_same_candidate_worktree_writer_is_rejected_on_free_slot(rig):
    r = rig
    completed(r)
    other = new_task(r, r.task['worker_route']['worktree'])
    with pytest.raises(ValueError, match='candidate worktree'):
        r.guard.register(other)
    assert r.guard.get(other['task']) is None


@pytest.mark.parametrize('hazard', ['live_child', 'unknown_child', 'live_wrapper', 'launcher', 'busy'])
def test_executor_release_requires_all_process_and_idle_proofs(rig, hazard):
    r = rig
    completed(r)
    if hazard == 'live_child':
        r.processes[404] = dict(pid=404)
    elif hazard == 'unknown_child':
        with r.guard.transaction() as rows:
            rows[r.task['task']]['child_identity'] = None
    elif hazard == 'live_wrapper':
        r.processes[303] = dict(pid=303)
    elif hazard == 'launcher':
        r.session.process.poll = lambda: None
    else:
        r.surface['tty_process_pids'].append(777)
    assert dc.target_reserved(r.row(), r.binding['target'])
    with pytest.raises(ValueError, match='visible target'):
        r.guard.register(new_task(r, r.tmp / 'separate-worktree'))


@pytest.mark.parametrize('release', ['done', 'retired'])
def test_existing_terminal_release_frees_candidate_and_executor(rig, release):
    r = rig
    if release == 'done':
        completed(r)
        # This fixture supplies the already accepted final lifecycle state;
        # the separate native ack tests exercise admission to done.
        with r.guard.transaction() as rows:
            rows[r.task['task']]['status'] = 'done'
    else:
        send(r)
        fence(r)
        retire(r)
    assert not dc.target_reserved(r.row(), r.binding['target'])
    assert not dc.worktree_reserved(r.row(), r.task['worker_route']['worktree'])
    r.guard.register(new_task(r, r.task['worker_route']['worktree']))


def test_recovery_cannot_write_another_open_candidate(native_recovery):
    r = native_recovery
    # Simulate a pre-existing overlapping candidate from the older reader.
    with r.guard.transaction() as rows:
        other = copy.deepcopy(rows[r.task['task']])
        other.update(task='older-candidate', status='pending', gate='main_validation',
                     identity=dict(r.identity, session='different'))
        other['visible_binding']['target']['surface'] = '99999999-9999-4999-8999-999999999999'
        for key in ('origin', 'deliveries', 'coordinator_delivery'):
            other.pop(key, None)
        rows[other['task']] = other
    with pytest.raises(ValueError, match='candidate worktree'):
        dc.maintenance_control(request(r))
    dc.cmux_send.assert_not_called()


def test_resource_observation_does_not_block_stop_and_revalidates(rig):
    import concurrent.futures
    import contextvars
    import threading
    r = rig
    completed(r)
    task = new_task(r, r.tmp / 'separate-worktree')
    entered, release = threading.Event(), threading.Event()
    original = dc.executor_released
    def slow(row):
        entered.set()
        assert release.wait(5)
        return original(row)
    r.monkeypatch.setattr(dc, 'executor_released', slow)
    context = contextvars.copy_context()
    with concurrent.futures.ThreadPoolExecutor(2) as pool:
        registration = pool.submit(context.run, r.guard.register, task)
        try:
            assert entered.wait(5)
            stop = pool.submit(r.guard.control, r.identity, 'cancelled')
            stop.result(timeout=5)
        finally:
            release.set()
        with pytest.raises(ValueError, match='registration state changed'):
            registration.result(timeout=5)
    assert r.guard.get(task['task']) is None
