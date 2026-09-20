"""Hermetic registered-route proofs: never dispatch a productive command."""
import copy
import json
import time
from pathlib import Path
from unittest.mock import Mock
import pytest
from hermes_cli import diggr_continuation as dc

REAL_GIT_ROUTE_IDENTITY = dc._git_route_identity


@pytest.fixture
def bound(tmp_path, monkeypatch):
    work = tmp_path/'work'; work.mkdir()
    packet = tmp_path/'packet.json'
    target = {'workspace':'11111111-1111-4111-8111-111111111111', 'surface':'22222222-2222-4222-8222-222222222222'}
    argv = ['/tools/codex','exec','--model','gpt-6-astra','--sandbox','workspace-write','--cd',str(work),'--output-last-message',str(tmp_path/'result.md'),'fix only this scope']
    route = dict(packet=str(packet), receipt=str(tmp_path/'receipt.json'), worktree=str(work), branch='hermes/SYSTEM-167-test', plane_id='SYSTEM-167', visible_target=target, argv=argv)
    payload = dict(routing_mode='non_cmm', plane_id='SYSTEM-167', task_id='SYSTEM-167', scope_id='SYSTEM-167', operator_scope_authorized=True, worktree=str(work), branch=route['branch'], base='a'*40, requested_actions=['inspect','edit','test'], coding_route=dict(executor='codex',required_model='gpt-6-astra',fallback_models_allowed=[],merge_authorized=False,routine_actions_authorized=['inspect','edit','test']))
    packet.write_text(json.dumps(payload)); route['packet_sha256']=dc.digest(packet)
    row=dict(task='scope-fix',generation=1,authorization='owner routing',deadline=time.time()+600,artifact=str(tmp_path/'result.md'),worker_route=route,visible_binding={'target':target,'shell':{'pid':12}},identity={'home':str(tmp_path),'profile':'diggr-main','session':'fixture','session_key':'fixture','platform':'telegram','chat_id':'564628210','user_id':'564628210','thread_id':''})
    monkeypatch.setattr(dc,'verify_visible_target',Mock(return_value=({},{})))
    # Source repair must expose this read-only git identity boundary.
    monkeypatch.setattr(dc,'_git_route_identity',lambda path: (str(work), route['branch'], 'a'*40, ''),raising=False)
    return row, route, payload


def change_packet(row, payload):
    Path(row['worker_route']['packet']).write_text(json.dumps(payload))
    row['worker_route']['packet_sha256']=dc.digest(row['worker_route']['packet'])


def test_valid_non_cmm_without_telemetry(bound):
    row, route, _ = bound
    result = dc.preflight_non_cmm(row,route)
    assert result['verdict']=='allowed'
    assert result['mode']=='non_cmm'
    assert 'budget' not in result and 'telemetry' not in result
    dc.verify_visible_target.assert_called_once_with(row['visible_binding'],idle=True)


@pytest.mark.parametrize('mutation', ['action','model','branch','worktree','scope','unauthorized','fallback','merge','argv_override','argv_cd','target','expired','packet_tamper'])
def test_non_cmm_denials(bound, mutation):
    row, route, payload = bound
    if mutation=='action': payload['requested_actions']=['deploy']
    if mutation=='model': payload['coding_route']['required_model']='other'
    if mutation=='branch': payload['branch']='main'
    if mutation=='worktree': payload['worktree']='/wrong'
    if mutation=='scope': payload['scope_id']='APP-49'
    if mutation=='unauthorized': payload['operator_scope_authorized']=False
    if mutation=='fallback': payload['coding_route']['fallback_models_allowed']=['other']
    if mutation=='merge': payload['coding_route']['merge_authorized']=True
    if mutation=='argv_override': route['argv'][2:2]=['-c','model="other"']
    if mutation=='argv_cd': route['argv'][route['argv'].index('--cd')+1]='/wrong'
    if mutation=='target': route['visible_target']={'surface':'unsafe','workspace':'unsafe'}
    if mutation=='expired': row['deadline']=time.time()-1
    change_packet(row,payload)
    if mutation=='packet_tamper': Path(route['packet']).write_text('{}')
    with pytest.raises(ValueError): dc.preflight_non_cmm(row,route)


def test_receipt_integrity_and_replay(bound):
    row,route,_=bound
    receipt=dc.preflight_non_cmm(row,route)
    Path(route['receipt']).write_text(json.dumps(receipt))
    row['non_cmm_receipt_sha256']=dc.digest(route['receipt'])  # fixture: native issuance
    dc.route_check(row,route,seal=True)
    dc.route_check(row,route)
    receipt['generation']+=1
    Path(route['receipt']).write_text(json.dumps(receipt))
    with pytest.raises(ValueError): dc.route_check(row,route)
    row.pop('receipt_sha256')
    with pytest.raises(ValueError): dc.route_check(row,route,seal=True)


def test_plain_allowed_receipt_not_sufficient(bound):
    row,route,_=bound
    Path(route['receipt']).write_text(json.dumps(dict(verdict='allowed',route_packet=route['packet'],coding_route_contract={'plane_id':'SYSTEM-167','operator_scope_authorized':True})))
    with pytest.raises(ValueError): dc.route_check(row,route,seal=True)


def test_dirty_or_wrong_git_identity(bound, monkeypatch):
    row,route,_=bound
    for value in [(route['worktree'],'main','a'*40,''),(route['worktree'],route['branch'],'b'*40,''),(route['worktree'],route['branch'],'a'*40,' M x'),('/wrong',route['branch'],'a'*40,'')]:
        monkeypatch.setattr(dc,'_git_route_identity',lambda path: value)
        with pytest.raises(ValueError): dc.preflight_non_cmm(row,route)


def test_unsafe_shell_blocks(bound,monkeypatch):
    row,route,_=bound
    monkeypatch.setattr(dc,'verify_visible_target',Mock(side_effect=ValueError('occupied shell')))
    with pytest.raises(ValueError): dc.preflight_non_cmm(row,route)


def native_fixture(bound, monkeypatch):
    row,route,_=bound
    row.update(status='running',ticket_nonce='test-native-nonce')
    guard=dc.Guard(Path(route['worktree']).parent/'registry.json')
    with guard.transaction() as tasks: tasks[row['task']]=row
    monkeypatch.setattr(dc,'runtime_guard',lambda home: guard)
    return row,route,guard


def test_registered_cli_preflight_seals_without_dispatch(bound,monkeypatch):
    import subprocess
    row,route,guard=native_fixture(bound,monkeypatch)
    executor=Mock(side_effect=AssertionError('executor forbidden'))
    monkeypatch.setattr(subprocess,'run',executor)
    args=['--home',str(Path(route['worktree']).parent),'route-preflight','--payload-json',json.dumps(dict(ticket=dc.worker_ticket(row),route=route))]
    assert dc.main(args)==0
    assert guard.get(row['task'])['receipt_sha256']==dc.digest(route['receipt'])
    assert json.loads(Path(route['receipt']).read_text())['mode']=='non_cmm'
    with pytest.raises(ValueError): dc.main(args)
    executor.assert_not_called()


@pytest.mark.parametrize('reason',['expired','wrong_ticket','claimed','unsafe_shell'])
def test_registered_cli_denial_never_executes(bound,monkeypatch,reason):
    import subprocess
    row,route,guard=native_fixture(bound,monkeypatch)
    ticket=dc.worker_ticket(row)
    with guard.transaction() as tasks:
        if reason=='expired': tasks[row['task']]['deadline']=time.time()-1
        if reason=='claimed': tasks[row['task']]['worker_pid']=123
    if reason=='wrong_ticket': ticket['generation']+=1
    if reason=='unsafe_shell': monkeypatch.setattr(dc,'verify_visible_target',Mock(side_effect=ValueError('unsafe target')))
    executor=Mock(side_effect=AssertionError('executor forbidden'))
    monkeypatch.setattr(subprocess,'run',executor)
    with pytest.raises(ValueError):
        dc.main(['--home',str(Path(route['worktree']).parent),'route-preflight','--payload-json',json.dumps(dict(ticket=ticket,route=route))])
    assert not Path(route['receipt']).exists()
    executor.assert_not_called()


def seal_fixture(row, route):
    return dc.main(['--home', row['identity']['home'], 'route-preflight',
                    '--payload-json', json.dumps(dict(ticket=dc.worker_ticket(row), route=route))])


def test_registered_transport_sends_once_not_completion(bound, monkeypatch):
    row,route,guard=native_fixture(bound,monkeypatch)
    seal_fixture(row,route)
    send=Mock()
    executor=Mock(side_effect=AssertionError('native executor forbidden'))
    monkeypatch.setattr(dc,'cmux_send',send)
    args=dict(command='SYSTEM167_REGISTERED_WORKER',continuation_ticket=dc.worker_ticket(row))
    assert dc.terminal_dispatch(args,executor)['sent'] is True
    send.assert_called_once()
    assert guard.get(row['task'])['status']=='running'
    with pytest.raises(ValueError): dc.terminal_dispatch(args,executor)
    send.assert_called_once()
    executor.assert_not_called()


@pytest.mark.parametrize('reason',['unsealed','receipt_tamper','packet_tamper','expired','claimed','unsafe_shell'])
def test_transport_denial_never_sends_or_executes(bound,monkeypatch,reason):
    row,route,guard=native_fixture(bound,monkeypatch)
    if reason!='unsealed': seal_fixture(row,route)
    if reason=='receipt_tamper': Path(route['receipt']).write_text('{}')
    if reason=='packet_tamper': Path(route['packet']).write_text('{}')
    with guard.transaction() as tasks:
        if reason=='expired': tasks[row['task']]['deadline']=time.time()-1
        if reason=='claimed': tasks[row['task']]['worker_pid']=123
    if reason=='unsafe_shell': monkeypatch.setattr(dc,'verify_visible_target',Mock(side_effect=ValueError('unsafe shell')))
    send=Mock(side_effect=AssertionError('send forbidden'))
    executor=Mock(side_effect=AssertionError('executor forbidden'))
    monkeypatch.setattr(dc,'cmux_send',send)
    with pytest.raises(ValueError):
        dc.terminal_dispatch(dict(command='SYSTEM167_REGISTERED_WORKER',continuation_ticket=dc.worker_ticket(row)),executor)
    send.assert_not_called()
    executor.assert_not_called()


def test_native_registration_still_requires_main_event(bound,monkeypatch):
    row,route,guard=native_fixture(bound,monkeypatch)
    task=dict(row,task='new-task',scope='SYSTEM-167 only',owner='main',gate='implementation',action='fix/tests/PR',wake_budget=3,producer='cmux',artifact=str(Path(route['worktree']).parent/'fresh.md'))
    monkeypatch.setattr(dc,'bind_visible_target',lambda target: row['visible_binding'])
    token=dc.EVENT_CONTEXT.set(None)
    try:
        with pytest.raises(ValueError,match='native Main event'): dc.register_native(task)
        dc.EVENT_CONTEXT.set(dict(identity=dict(row['identity'],platform='cli')))
        with pytest.raises(ValueError,match='unsupported authorized'): dc.register_native(task)
        # Isolated fake registry, not a native live event or launch.
        dc.EVENT_CONTEXT.set(dict(identity=dict(row['identity'],session='second',session_key='second')))
        _,registered=dc.register_native(task)
        assert registered['worker_route']['packet_sha256']==dc.digest(route['packet'])
        assert 'receipt_sha256' not in registered
    finally:
        dc.EVENT_CONTEXT.reset(token)


def test_cli_real_temporary_git_worktree(bound, monkeypatch, tmp_path):
    """No remote or cmux: actual Git identity, fake execution boundary."""
    import subprocess
    row,route,guard=native_fixture(bound,monkeypatch)
    seed=tmp_path/'seed'
    seed.mkdir()

    def git(*args):
        return subprocess.check_output(['git','-C',str(seed),*args],text=True,stderr=subprocess.PIPE).strip()

    git('init','--initial-branch=fixture-base')
    git('-c','user.name=Fixture','-c','user.email=fixture@example.invalid',
        '-c','commit.gpgsign=false','commit','--allow-empty','-m','isolated fixture')
    Path(route['worktree']).rmdir()  # empty fixture directory only
    git('worktree','add','-b',route['branch'],route['worktree'],'HEAD')
    packet=json.loads(Path(route['packet']).read_text())
    packet['base']=git('rev-parse','HEAD')
    change_packet(row,packet)
    with guard.transaction() as tasks: tasks[row['task']]['worker_route']=route
    monkeypatch.setattr(dc,'_git_route_identity',REAL_GIT_ROUTE_IDENTITY)
    assert seal_fixture(row,route)==0
    assert REAL_GIT_ROUTE_IDENTITY(route['worktree'])==(route['worktree'],route['branch'],packet['base'],'')
    send=Mock()
    executor=Mock(side_effect=AssertionError('executor forbidden'))
    monkeypatch.setattr(dc,'cmux_send',send)
    assert dc.terminal_dispatch(dict(command='SYSTEM167_REGISTERED_WORKER',continuation_ticket=dc.worker_ticket(row)),executor)['sent']
    send.assert_called_once()
    executor.assert_not_called()
    dirty=Path(route['worktree'])/'untracked'
    dirty.write_text('fixture only')
    with pytest.raises(ValueError,match='clean exact-base'): dc.preflight_non_cmm(row,route)
    dirty.unlink()
    # A real branch rename and real new HEAD must not match the old packet.
    subprocess.check_output(['git','-C',route['worktree'],'branch','-m','hermes/other'],text=True)
    with pytest.raises(ValueError,match='clean exact-base'): dc.preflight_non_cmm(row,route)
    subprocess.check_output(['git','-C',route['worktree'],'branch','-m',route['branch']],text=True)
    subprocess.check_output(['git','-C',route['worktree'],'-c','user.name=Fixture','-c','user.email=fixture@example.invalid','-c','commit.gpgsign=false','commit','--allow-empty','-m','changed fixture head'],text=True)
    with pytest.raises(ValueError,match='clean exact-base'): dc.preflight_non_cmm(row,route)
