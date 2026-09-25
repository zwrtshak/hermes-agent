"""Boot-time clock adjustment is not evidence that a live owned process exited."""
from types import SimpleNamespace
from unittest.mock import Mock
import subprocess
import psutil
import pytest
from hermes_cli import diggr_continuation as dc

@pytest.mark.parametrize('current_created',[500.0,501.0,499.0])
def test_legacy_living_pid_never_released_by_clock_difference(monkeypatch,current_created):
    monkeypatch.setattr(psutil,'pid_exists',lambda pid:True)
    monkeypatch.setattr(psutil,'Process',lambda pid:SimpleNamespace(create_time=lambda:current_created))
    assert not dc.identity_exited(dict(pid=101,created=500.0))

def test_legacy_absent_pid_still_has_exit_evidence(monkeypatch):
    monkeypatch.setattr(psutil,'pid_exists',lambda pid:False)
    assert dc.identity_exited(dict(pid=101,created=500.0))

@pytest.mark.parametrize('start,executable,expected',[
    ('original','/bin/bash',False),('original','/bin/zsh',False),('replacement','/bin/bash',True)])
def test_native_start_identity_and_pid_reuse(monkeypatch,start,executable,expected):
    monkeypatch.setattr(psutil,'pid_exists',lambda pid:True)
    monkeypatch.setattr(dc,'process_identity',lambda pid:dict(pid=pid,start=start,executable=executable))
    proof=dict(pid=101,created=500.0,start='original',executable='/bin/bash')
    assert dc.identity_exited(proof) is expected
    assert not dc.process_absent(proof)  # Original strict reservation/surface semantics remain.

@pytest.mark.parametrize('value',[None,{},dict(pid=101),dict(pid=101,created=None),dict(pid=True,created=500.0),dict(pid=101,created=500.0,start=None)])
def test_missing_identity_is_not_exit(value):
    assert not dc.identity_exited(value)

@pytest.mark.parametrize('result',[{},dict(pid=101),dict(pid=101,start=123,executable='/bin/bash'),OSError('unavailable'),subprocess.CalledProcessError(1,['ps'])])
def test_unknown_current_os_proof_blocks_release(monkeypatch,result):
    monkeypatch.setattr(psutil,'pid_exists',lambda pid:True)
    def inspect(pid):
        if isinstance(result,Exception):raise result
        return result
    monkeypatch.setattr(dc,'process_identity',inspect)
    assert not dc.identity_exited(dict(pid=101,start='original',executable='/bin/bash'))

def test_new_launcher_binds_os_start_but_wait_handle_exit_remains_valid(monkeypatch):
    poll=Mock(return_value=None)
    session=SimpleNamespace(pid_scope='host',pid=101,id='launcher',session_key='native',started_at=10,
        process=SimpleNamespace(pid=101,poll=poll),_pty=None)
    monkeypatch.setattr(psutil,'Process',lambda pid:SimpleNamespace(create_time=lambda:501.0))
    inspect=Mock(return_value=dict(pid=101,start='original',executable='/bin/bash'))
    monkeypatch.setattr(dc,'process_identity',inspect)
    proof=dc.launcher_identity(session)
    assert proof['start']=='original' and proof['created']==501.0
    poll.return_value=0
    assert dc.launcher_identity(session)['exited'] is True
    assert inspect.call_count==1

def test_new_bound_shell_captures_verified_os_proof(monkeypatch):
    monkeypatch.setattr(psutil,'Process',lambda pid:SimpleNamespace(create_time=lambda:501.0))
    monkeypatch.setattr(dc,'verify_visible_target',lambda *a,**kw:({},dict(pid=101,start='original',executable='/bin/bash')))
    assert dc.bound_shell_identity({'shell':{'pid':101}})==dict(pid=101,created=501.0,start='original',executable='/bin/bash')
