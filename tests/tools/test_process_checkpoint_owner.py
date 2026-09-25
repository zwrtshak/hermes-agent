"""Gateway-only shared checkpoint ownership; no host processes or deliveries."""
import asyncio
import json
import os
from pathlib import Path
from unittest.mock import MagicMock
import pytest
from gateway import status
import tools.process_registry as pr


@pytest.fixture(params=["diggr-main", "diggr-coding"])
def owned_home(monkeypatch, tmp_path, request):
    tmp_path = tmp_path / "profiles" / request.param
    tmp_path.mkdir(parents=True)
    assert status._gateway_lock_handle is None
    monkeypatch.setattr(status, '_get_process_hermes_home', lambda: tmp_path)
    monkeypatch.setattr(pr, 'CHECKPOINT_PATH', tmp_path / 'processes.json')
    yield tmp_path
    status.release_gateway_runtime_lock()


def receipt(registry):
    session = pr.ProcessSession(id='pending', command='', exited=True, started_at=12,
                                completion_receipt={'task': 'fixture'},
                                receipt_state={'receipt': 'pending', 'main': 'pending'})
    registry._finished[session.id] = session
    return session


def test_cli_cannot_erase_gateway_receipts_or_claim_durability(owned_home, monkeypatch):
    assert status.acquire_gateway_runtime_lock()
    gateway, cli = pr.ProcessRegistry(), pr.ProcessRegistry()
    gateway.bind_gateway_checkpoint(owned_home)
    receipt(gateway)
    assert gateway._write_checkpoint()
    before = pr.CHECKPOINT_PATH.read_bytes()
    for key in ['_HERMES_GATEWAY', 'INVOCATION_ID', 'HERMES_S6_SUPERVISED_CHILD']:
        monkeypatch.setenv(key, '1')
    assert cli._write_checkpoint() is False
    assert cli.recover_from_checkpoint() == 0
    local = receipt(cli)
    assert cli.save_receipt_state(local.id, local.started_at, {'receipt': 'sending'}) is False
    assert local.receipt_state['receipt'] == 'pending'
    assert pr.CHECKPOINT_PATH.read_bytes() == before
    assert gateway.save_receipt_state('pending', 12, {'receipt': 'sent'})
    assert json.loads(pr.CHECKPOINT_PATH.read_text())[0]['receipt_state']['receipt'] == 'sent'


def test_binding_requires_actual_lock_and_exact_home(owned_home, monkeypatch):
    registry = pr.ProcessRegistry()
    monkeypatch.setenv('_HERMES_GATEWAY', '1')
    with pytest.raises(RuntimeError):
        registry.bind_gateway_checkpoint(owned_home)
    assert status.acquire_gateway_runtime_lock()
    with pytest.raises(RuntimeError):
        registry.bind_gateway_checkpoint(owned_home / 'other-profile')
    registry.bind_gateway_checkpoint(owned_home)
    receipt(registry)
    assert registry._write_checkpoint()
    before = pr.CHECKPOINT_PATH.read_bytes()
    monkeypatch.setattr(pr, 'CHECKPOINT_PATH', owned_home / 'other-profile' / 'processes.json')
    assert registry._write_checkpoint() is False
    assert not pr.CHECKPOINT_PATH.exists()
    assert (owned_home / 'processes.json').read_bytes() == before


def test_lock_release_revokes_checkpoint_writes(owned_home):
    assert status.acquire_gateway_runtime_lock()
    registry = pr.ProcessRegistry()
    registry.bind_gateway_checkpoint(owned_home)
    receipt(registry)
    assert registry._write_checkpoint()
    before = pr.CHECKPOINT_PATH.read_bytes()
    status.release_gateway_runtime_lock()
    assert registry._write_checkpoint() is False
    assert pr.CHECKPOINT_PATH.read_bytes() == before


@pytest.mark.skipif(not hasattr(os, 'fork'), reason='requires POSIX fork')
def test_actual_fork_cannot_inherit_or_rebind_checkpoint_owner(owned_home):
    assert status.acquire_gateway_runtime_lock()
    registry = pr.ProcessRegistry()
    registry.bind_gateway_checkpoint(owned_home)
    receipt(registry)
    assert registry._write_checkpoint()
    before = pr.CHECKPOINT_PATH.read_bytes()
    child = os.fork()
    if child == 0:
        try:
            assert registry._write_checkpoint() is False
            assert registry.recover_from_checkpoint() == 0
            with pytest.raises(RuntimeError):
                registry.bind_gateway_checkpoint(owned_home)
            os._exit(0)
        except BaseException:
            os._exit(1)
    _, code = os.waitpid(child, 0)
    assert os.waitstatus_to_exitcode(code) == 0
    assert pr.CHECKPOINT_PATH.read_bytes() == before
    assert registry._write_checkpoint()


@pytest.mark.asyncio
@pytest.mark.parametrize('lost_claim', [None, 'lock', 'pid'])
async def test_real_gateway_startup_binds_only_after_singleton_claim(owned_home, monkeypatch, lost_claim):
    import gateway.run as run
    from gateway.config import GatewayConfig
    registry = pr.ProcessRegistry()
    monkeypatch.setattr(pr, 'process_registry', registry)
    monkeypatch.setattr(run, '_hermes_home', owned_home)
    monkeypatch.setenv('HERMES_HOME', str(owned_home))
    monkeypatch.setattr('hermes_cli.resource_limits.apply_nofile_soft_limit', lambda: None)
    monkeypatch.setattr('gateway.code_skew.record_boot_fingerprint', lambda: None)
    monkeypatch.setattr(status, 'get_running_pid', lambda: None)
    monkeypatch.setattr('tools.skills_sync.sync_skills', lambda **kw: None)
    monkeypatch.setattr('hermes_logging.setup_logging', lambda **kw: None)
    monkeypatch.setattr('hermes_cli.security_audit_startup.log_startup_security_warnings', lambda **kw: None)
    monkeypatch.setattr(run, 'GatewayRunner', MagicMock())
    monkeypatch.setattr(run.threading, 'Thread', MagicMock())
    monkeypatch.setattr(asyncio.get_running_loop(), 'add_signal_handler', lambda *args: None)
    monkeypatch.setattr('atexit.register', lambda *args: None)
    original = registry.bind_gateway_checkpoint
    bound = []
    class StopAtBinding(Exception):
        pass
    def observe(home):
        assert status._get_pid_path().exists()
        original(home)
        bound.append(True)
        raise StopAtBinding
    monkeypatch.setattr(registry, 'bind_gateway_checkpoint', observe)
    if lost_claim == 'lock':
        monkeypatch.setattr(status, 'acquire_gateway_runtime_lock', lambda: False)
    if lost_claim == 'pid':
        def fail_pid():
            raise FileExistsError
        monkeypatch.setattr(status, 'write_pid_file', fail_pid)
    if lost_claim:
        assert await run.start_gateway(config=GatewayConfig(), verbosity=None) is False
        assert not bound and registry._checkpoint_owner is None
    else:
        with pytest.raises(StopAtBinding):
            await run.start_gateway(config=GatewayConfig(), verbosity=None)
        assert bound and registry._owns_gateway_checkpoint()


def test_other_profile_keeps_legacy_checkpoint_semantics(monkeypatch, tmp_path):
    home = tmp_path / 'profiles' / 'unrelated-fixture'
    home.mkdir(parents=True)
    monkeypatch.setattr(pr, 'CHECKPOINT_PATH', home / 'processes.json')
    registry = pr.ProcessRegistry()
    assert not registry.requires_gateway_checkpoint()
    assert registry._checkpoint_owner is None
    receipt(registry)
    assert registry._write_checkpoint()
    assert json.loads(pr.CHECKPOINT_PATH.read_text())[0]['session_id'] == 'pending'
    assert registry.save_receipt_state('pending', 12, {'receipt': 'sent'})
