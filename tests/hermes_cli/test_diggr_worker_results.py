"""Actual wrapper boundaries with disposable state and synthetic native processes."""
import builtins
import json
from pathlib import Path

import pytest
from hermes_cli import diggr_continuation as dc
from tests.hermes_cli.test_diggr_launch_reservation import rig, send, claim_without_spawn


def run_worker(r, ticket):
    return dc.main(['--home', str(r.tmp), 'worker', '--payload-json', json.dumps(ticket)])


@pytest.mark.parametrize('exit_code,artifact', [(0, 'blocked: required input absent'),
                                               (7, 'partial changes'), (0, None), (7, None)])
def test_actual_exit_and_artifact_reach_main_without_invented_success(rig, exit_code, artifact):
    r = rig
    ticket = r.ticket()
    send(r)
    spawn = claim_without_spawn(r, ticket)
    def wait(*, timeout):
        if artifact is not None:
            Path(r.task['artifact']).write_text(artifact)
        return exit_code
    spawn.return_value.wait = wait
    assert run_worker(r, ticket) == exit_code
    row = r.row()
    result = row['worker_result']
    assert result['generation'] == 1 and row['generation'] == 2
    assert result['exit_code'] == exit_code and result['result'] == 'unclassified'
    assert result['artifact_status'] == ('present' if artifact is not None else 'missing')
    assert row['gate'] == 'main_validation' and row['status'] == 'pending'
    if artifact is not None:
        assert row['evidence']['sha256'] == dc.digest(r.task['artifact'])
        assert row['evidence']['result'] == 'unclassified'
    else:
        assert row['evidence'] is None
    snapshot = r.guard.path.read_bytes()
    dc.observe_processes(dc.Guard(r.guard.path))
    assert r.guard.path.read_bytes() == snapshot


def test_persisted_exit_survives_crash_before_classification(rig):
    r = rig
    ticket = r.ticket()
    send(r)
    spawn = claim_without_spawn(r, ticket)
    # Both normal completion and exception cleanup are interrupted, as by a crash.
    r.monkeypatch.setattr(r.guard, 'observe', lambda *a, **kw: (_ for _ in ()).throw(KeyboardInterrupt()))
    with pytest.raises(KeyboardInterrupt):
        run_worker(r, ticket)
    assert spawn.call_count == 1
    restarted = dc.Guard(r.guard.path)
    assert restarted.get(r.task['task'])['worker_result']['exit_code'] == 1
    dc.observe_processes(restarted)
    row = restarted.get(r.task['task'])
    assert row['gate'] == 'main_validation' and row['worker_result']['exit_code'] == 1
    assert spawn.call_count == 1


def test_stop_between_claim_and_spawn_prevents_child(rig):
    r = rig
    ticket = r.ticket()
    send(r)
    spawn = claim_without_spawn(r, ticket)
    original = builtins.print
    def stop_after_claim(*args, **kwargs):
        if args and str(args[0]).startswith('SYSTEM167_VISIBLE_WORKER_START'):
            r.guard.control(r.identity, 'cancelled')
        original(*args, **kwargs)
    r.monkeypatch.setattr(builtins, 'print', stop_after_claim)
    with pytest.raises(ValueError, match='revoked before spawn'):
        run_worker(r, ticket)
    spawn.assert_not_called()
    assert r.row()['status'] == 'cancelled'


def test_exit_after_stop_is_retained_without_main_wake(rig):
    r = rig
    ticket = r.ticket()
    send(r)
    spawn = claim_without_spawn(r, ticket)
    def wait(*, timeout):
        r.guard.control(r.identity, 'cancelled')
        Path(r.task['artifact']).write_text('partial work before stop')
        return 9
    spawn.return_value.wait = wait
    assert run_worker(r, ticket) == 9
    row = r.row()
    assert row['status'] == 'cancelled' and row['worker_result']['exit_code'] == 9
    assert row['late_evidence']['sha256'] == dc.digest(r.task['artifact'])
    assert r.guard.tick(r.identity) is None
