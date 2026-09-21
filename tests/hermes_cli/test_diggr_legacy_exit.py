"""Legacy recovery requires owning-handle exit, never a registry EOF flag."""
import copy
import subprocess
import sys
import threading
import time

import pytest

from hermes_cli import diggr_continuation as dc
from tools.process_registry import ProcessSession, process_registry
from tests.hermes_cli.test_diggr_recovery_restart import recovery, prepare

pytestmark = pytest.mark.macos_only


def legacy_row(session):
    return dict(process_id=session.id, process_started_at=session.started_at,
                identity=dict(session_key=session.session_key))


def test_native_stdout_eof_blocks_recovery_until_actual_exit(recovery, monkeypatch):
    proc = subprocess.Popen([sys.executable, '-B', '-c',
        'import os,sys; os.close(1); os.close(2); sys.stdin.read()'],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    session = ProcessSession(id='legacy_eof', command='harmless EOF fixture',
        session_key='fixture', pid=proc.pid, process=proc, started_at=time.time())
    # Isolate the native reader's registry transition as well as registration.
    monkeypatch.setattr(process_registry, '_running', {session.id: session})
    monkeypatch.setattr(process_registry, '_finished', {})
    reader = threading.Thread(target=process_registry._reader_loop,
                              args=(session,), daemon=True)
    try:
        reader.start()
        reader.join(10)
        assert not reader.is_alive()
        assert session.exited and proc.poll() is None
        assert process_registry.get(session.id) is session
        guard = recovery['guard']
        with guard.transaction() as tasks:
            row = tasks['fixture']
            row.update(process_id=session.id, process_started_at=session.started_at)
            row.pop('launcher_identity', None)
            row.pop('launcher_expected', None)
        payload = prepare(recovery)
        before = copy.deepcopy(guard.get('fixture'))
        with pytest.raises(ValueError, match='prior actual process exit not reconciled'):
            guard.recover(before['identity'], payload, payload['report_path'],
                          payload['report_sha256'], payload['strategy'])
        assert not dc.prior_process_exited(before)
        assert guard.get('fixture') == before
        proc.stdin.close()
        assert proc.wait(timeout=5) == 0
        assert dc.prior_process_exited(before)
        ticket = guard.recover(before['identity'], payload, payload['report_path'],
                               payload['report_sha256'], payload['strategy'])
        fresh = guard.get('fixture')
        assert ticket['generation'] > before['generation']
        assert fresh['status'] == 'running'
        assert fresh['action_id'] != before['action_id']
        assert fresh['effect_id'] != before['effect_id']
        assert len(fresh['attempt_history']) == 1
    finally:
        proc.stdin.close()
        proc.wait(timeout=5)
        reader.join(10)
        proc.stdout.close()


def test_legacy_real_pty_requires_waitable_exit(monkeypatch):
    from ptyprocess import PtyProcess
    proc = PtyProcess.spawn([sys.executable, '-B', '-c',
                            'import sys; sys.stdin.readline()'], echo=False)
    session = ProcessSession(id='legacy_pty', command='harmless PTY fixture',
        session_key='fixture', pid=proc.pid, _pty=proc, exited=True)
    monkeypatch.setattr(process_registry, '_running', {session.id: session})
    row = legacy_row(session)
    try:
        assert proc.isalive()
        assert not dc.prior_process_exited(row)
        proc.write(b'done\n')
        assert proc.wait() == 0
        assert dc.prior_process_exited(row)
    finally:
        if proc.isalive():
            proc.write(b'done\n')
            proc.wait()
        proc.close()


@pytest.mark.parametrize('edge', [
    'missing_session', 'no_handle', 'sandbox', 'wrong_pid', 'wrong_key', 'wrong_start',
])
def test_legacy_registry_flag_without_matching_local_handle_is_not_exit(edge, monkeypatch):
    proc = subprocess.Popen([sys.executable, '-B', '-c', 'pass'])
    assert proc.wait(timeout=5) == 0
    session = ProcessSession(id='legacy_edge', command='harmless exited fixture',
        session_key='fixture', pid=proc.pid, process=proc, exited=True)
    row = legacy_row(session)
    if edge == 'no_handle':
        session.process = None
    elif edge == 'sandbox':
        session.pid_scope = 'sandbox'
    elif edge == 'wrong_pid':
        session.pid += 1
    elif edge == 'wrong_key':
        row['identity']['session_key'] = 'other'
    elif edge == 'wrong_start':
        row['process_started_at'] += 1
    monkeypatch.setattr(process_registry, '_running',
                        {} if edge == 'missing_session' else {session.id: session})
    monkeypatch.setattr(process_registry, '_finished', {})
    assert not dc.prior_process_exited(row)
