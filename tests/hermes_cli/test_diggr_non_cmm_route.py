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
    row=dict(task='scope-fix',status='running',generation=1,authorization='owner routing',deadline=time.time()+600,artifact=str(tmp_path/'result.md'),worker_route=route,visible_binding={'target':target,'shell':{'pid':12}},identity={'home':str(tmp_path),'profile':'diggr-main','session':'fixture','session_key':'fixture','platform':'telegram','chat_id':'564628210','user_id':'564628210','thread_id':''})
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
    if mutation=='expired': row['hard_stop']=time.time()-1
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
        if reason=='expired': tasks[row['task']]['hard_stop']=time.time()-1
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
        if reason=='expired': tasks[row['task']]['hard_stop']=time.time()-1
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


def test_sealed_ticket_deadline_renewal_preserves_route(bound, monkeypatch):
    row, route, guard = native_fixture(bound, monkeypatch)
    seal_fixture(row, route)
    ticket = dc.worker_ticket(row)
    with guard.transaction() as tasks:
        tasks[row['task']]['deadline'] = time.time() - 1
        current = dc.validate_ticket(tasks, ticket)
        dc.route_check(current, route)
        assert current['deadline'] > time.time()
        assert dc.worker_ticket(current) == ticket


def recovery_fixture(bound, monkeypatch, tmp_path):
    import subprocess, sys, psutil
    row, route, guard = native_fixture(bound, monkeypatch)
    with guard.transaction() as tasks:
        current = tasks[row['task']]
        current.update(scope='SYSTEM-167 only',owner='coding',gate='coding',action='fix',
                       wake_budget=1,wakes=0,due=0,lease=0,registered_at=time.time())
    # Real process identity and exit, no worker or provider launch.
    process = subprocess.Popen([sys.executable, '-c', 'pass'])
    identity = dict(pid=process.pid, created=psutil.Process(process.pid).create_time())
    process.wait(timeout=5)
    with guard.transaction() as tasks:
        tasks[row['task']].update(worker_pid=process.pid, worker_identity=identity)
    guard.observe(row['task'], 1, 'unknown')
    wake = guard.tick(row['identity'])
    assert guard.begin(row['identity'], wake)
    current = guard.get(row['task'])
    observation = tmp_path/'observation.txt'; observation.write_text('Reconciled fixture: no writes; process exited.')
    report = {k:current[k] for k in ('task','generation','identity','action_id','effect_id')}
    report.update(effects=[dict(effect_id=current['effect_id'],status='no_effect')],
                  observations=[dict(path=str(observation),sha256=dc.digest(observation))])
    report_path = tmp_path/'reconciliation.json'; report_path.write_text(json.dumps(report))
    new_route = copy.deepcopy(route)
    new_route['receipt'] = str(tmp_path/'fresh-receipt.json')
    new_route['packet'] = str(tmp_path/'fresh-packet.json')
    Path(new_route['packet']).write_text(Path(route['packet']).read_text())
    new_route['packet_sha256'] = dc.digest(new_route['packet'])
    artifact = str(tmp_path/'fresh-result.md')
    new_route['argv'][new_route['argv'].index('--output-last-message')+1] = artifact
    new_route['argv'][-1] = 'fresh bounded strategy: inspect fixture and verify'
    strategy = dict(action='fresh fixture strategy',artifact=artifact,worker_route=new_route)
    return guard, wake, report_path, strategy


def test_recovery_unique_effect_and_no_duplicate_dispatch(bound, monkeypatch, tmp_path):
    guard,wake,path,strategy = recovery_fixture(bound,monkeypatch,tmp_path)
    old = guard.get(wake['task'])
    ticket = guard.recover(old['identity'], wake, str(path), dc.digest(path), strategy)
    fresh = guard.get(wake['task'])
    assert fresh['action_id'] != old['action_id'] and fresh['effect_id'] != old['effect_id']
    assert fresh['artifact'] != old['artifact']
    assert fresh['generation'] > old['generation']
    assert fresh['status'] == 'running'
    assert not fresh.get('visible_sent')
    with pytest.raises(ValueError): guard.recover(old['identity'],wake,str(path),dc.digest(path),strategy)
    assert not guard.observe(wake['task'],old['generation'],'completed')
    seal_fixture(fresh,fresh['worker_route'])
    send=Mock(); monkeypatch.setattr(dc,'cmux_send',send)
    assert dc.terminal_dispatch(dict(command='SYSTEM167_REGISTERED_WORKER',continuation_ticket=ticket),Mock())['sent']
    with pytest.raises(ValueError): dc.terminal_dispatch(dict(command='SYSTEM167_REGISTERED_WORKER',continuation_ticket=ticket),Mock())
    send.assert_called_once()


@pytest.mark.parametrize('reason',['unknown_effect','live_process','stale_report','artifact_reuse','foreign_identity','occupied_terminal'])
def test_recovery_denies_unreconciled_or_unbound(bound,monkeypatch,tmp_path,reason):
    import os, psutil
    guard,wake,path,strategy = recovery_fixture(bound,monkeypatch,tmp_path)
    row = guard.get(wake['task'])
    report=json.loads(path.read_text())
    identity=row['identity']
    if reason=='unknown_effect': report['effects'][0]['status']='unknown'
    if reason=='stale_report': report['generation']-=1
    if reason=='artifact_reuse': strategy['artifact']=row['artifact']
    if reason=='foreign_identity': identity=dict(identity,session='foreign')
    if reason=='occupied_terminal': monkeypatch.setattr(dc,'verify_visible_target',Mock(side_effect=ValueError('busy')))
    if reason=='live_process':
        with guard.transaction() as tasks:
            tasks[row['task']].update(worker_pid=os.getpid(),worker_identity=dict(pid=os.getpid(),created=psutil.Process().create_time()))
    path.write_text(json.dumps(report))
    with pytest.raises(ValueError): guard.recover(identity,wake,str(path),dc.digest(path),strategy)
    assert guard.get(row['task'])['generation']==row['generation']


def test_migrated_legacy_seal_survives_epoch_deadline(bound,monkeypatch):
    """A trusted PR9 seal remains valid; its old deadline is execution time only."""
    import hashlib
    row,route,guard=native_fixture(bound,monkeypatch)
    proof=dict(task=row['task'],generation=row['generation'],authorization=row['authorization'],
        identity=row['identity'],artifact=row['artifact'],argv=route['argv'],binding=row['visible_binding'],
        packet_sha256=route['packet_sha256'],worktree=route['worktree'],branch=route['branch'],
        head='a'*40,expires_at=row['deadline'])
    contract=json.loads(Path(route['packet']).read_text())['coding_route']
    receipt=dict(schema_version='diggr.native.non_cmm_route.v1',mode='non_cmm',verdict='allowed',
        route_packet=route['packet'],packet_sha256=route['packet_sha256'],task=row['task'],
        generation=row['generation'],expires_at=row['deadline'],
        binding_sha256=hashlib.sha256(json.dumps(proof,sort_keys=True).encode()).hexdigest(),
        coding_route_contract=dict(contract,plane_id=route['plane_id'],operator_scope_authorized=True))
    Path(route['receipt']).write_text(json.dumps(receipt))
    with guard.transaction() as tasks:
        current=tasks[row['task']]
        current.update(receipt_sha256=dc.digest(route['receipt']),non_cmm_receipt_sha256=dc.digest(route['receipt']),deadline=time.time()-1)
        current=dc.validate_ticket(tasks,dc.worker_ticket(current))
        dc.route_check(current,route)


def test_concurrent_recovery_issues_one_ticket(bound,monkeypatch,tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    guard,wake,path,strategy=recovery_fixture(bound,monkeypatch,tmp_path)
    identity=guard.get(wake['task'])['identity']
    def attempt(_):
        try:
            return dc.Guard(guard.path).recover(identity,wake,str(path),dc.digest(path),strategy)
        except ValueError:
            return None
    with ThreadPoolExecutor(max_workers=4) as pool:
        tickets=list(pool.map(attempt,range(8)))
    assert sum(ticket is not None for ticket in tickets)==1
    assert len(guard.get(wake['task'])['attempt_history'])==1


def test_recovery_crash_before_commit_keeps_old_authority(bound,monkeypatch,tmp_path):
    guard,wake,path,strategy=recovery_fixture(bound,monkeypatch,tmp_path)
    identity=guard.get(wake['task'])['identity']
    with monkeypatch.context() as context:
        context.setattr(dc.os,'replace',Mock(side_effect=OSError('simulated crash before atomic replace')))
        with pytest.raises(OSError): guard.recover(identity,wake,str(path),dc.digest(path),strategy)
    assert guard.get(wake['task'])['generation']==wake['generation']
    assert Path(strategy['artifact']+'.reservation').exists()
    # The uncertain reservation is never silently reused.
    with pytest.raises(FileExistsError): guard.recover(identity,wake,str(path),dc.digest(path),strategy)


def test_second_no_progress_strategy_retains_all_effect_history(bound,monkeypatch,tmp_path):
    guard,wake,path,strategy=recovery_fixture(bound,monkeypatch,tmp_path)
    identity=guard.get(wake['task'])['identity']
    first=guard.recover(identity,wake,str(path),dc.digest(path),strategy)
    guard.observe(wake['task'],first['generation'],'unknown')
    second_wake=guard.tick(identity)
    assert guard.begin(identity,second_wake)
    row=guard.get(wake['task'])
    report=json.loads(path.read_text())
    report.update({k:row[k] for k in ('generation','action_id','effect_id')})
    report['effects']=[dict(effect_id=row['effect_id'],status='no_effect')]
    path2=tmp_path/'reconciliation-2.json';path2.write_text(json.dumps(report))
    strategy2=copy.deepcopy(strategy)
    strategy2['action']='second fresh bounded strategy'
    strategy2['artifact']=str(tmp_path/'result-2.md')
    route=strategy2['worker_route']
    route['receipt']=str(tmp_path/'receipt-2.json')
    route['packet']=str(tmp_path/'packet-2.json')
    Path(route['packet']).write_text(Path(strategy['worker_route']['packet']).read_text())
    route['packet_sha256']=dc.digest(route['packet'])
    route['argv'][route['argv'].index('--output-last-message')+1]=strategy2['artifact']
    route['argv'][-1]='second diagnosis with narrower fixture verification'
    second=guard.recover(identity,second_wake,str(path2),dc.digest(path2),strategy2)
    latest=guard.get(wake['task'])
    assert first!=second
    assert len({item['effect_id'] for item in latest['attempt_history']}|{latest['effect_id']})==3
    assert not guard.observe(wake['task'],first['generation'],'unknown')
