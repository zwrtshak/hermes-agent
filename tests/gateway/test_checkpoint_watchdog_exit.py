"""Hard-exit backstops must not wait behind a wedged checkpoint writer."""
import threading
import pytest
from gateway import status, shutdown_watchdog


@pytest.mark.parametrize('caller', ['shutdown_watchdog', 'graceful_hard_exit'])
def test_hard_exit_reaches_os_exit_without_unlocking_busy_commit(tmp_path, monkeypatch, caller):
    from gateway import run
    import hermes_logging
    import gateway.lifecycle_ledger
    monkeypatch.setattr(status, '_get_process_hermes_home', lambda: tmp_path)
    assert status.acquire_gateway_runtime_lock()
    original_handle = status._gateway_lock_handle
    held, release, exited = threading.Event(), threading.Event(), threading.Event()
    observed = []
    def writer():
        with status._gateway_checkpoint_commit_lock:
            held.set()
            release.wait(timeout=5)
    worker = threading.Thread(target=writer, daemon=True)
    worker.start()
    assert held.wait(timeout=1)
    monkeypatch.setattr(status, 'remove_pid_file', lambda: None)
    monkeypatch.setattr(hermes_logging, 'drain_log_queue', lambda **kw: None)
    monkeypatch.setattr(gateway.lifecycle_ledger, 'mark_exited', lambda *a, **kw: None)
    monkeypatch.setattr(shutdown_watchdog, '_write_watchdog_dump', lambda *a, **kw: None)
    def fake_exit(code):
        observed.append((code, status._gateway_lock_handle is original_handle, original_handle.closed))
        exited.set()
    monkeypatch.setattr(run.os, '_exit', fake_exit)
    exit_thread = None
    try:
        if caller == 'shutdown_watchdog':
            shutdown_watchdog.arm_shutdown_watchdog(.01, exit_code=7, dump_path=tmp_path/'fixture.dump')
        else:
            exit_thread = threading.Thread(target=run._exit_after_graceful_shutdown, args=(7,), daemon=True)
            exit_thread.start()
        reached_while_busy = exited.wait(timeout=1)
    finally:
        release.set()
        worker.join(timeout=2)
        assert not worker.is_alive()
        assert exited.wait(timeout=2)
        if exit_thread is not None:
            exit_thread.join(timeout=2)
        status.release_gateway_runtime_lock()
    assert reached_while_busy, 'hard exit waited on checkpoint commit gate'
    assert observed == [(7, True, False)], 'busy runtime lock must stay held until OS process exit'
