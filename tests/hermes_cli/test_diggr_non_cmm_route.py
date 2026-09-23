"""Hermetic registered-route proofs: never dispatch a productive command."""
from tests.diggr_owner_fixtures import authorize

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
    row.update(scope='fixture', action='inspect', owner='coding', gate='coding', wake_budget=10, ticket_nonce='test-native-nonce')
    initial = dc.Guard(tmp_path/'registry.json')
    initial.register(authorize(initial, row))
    row = initial.get(row['task'])
    monkeypatch.setattr(dc,'verify_visible_target',Mock(return_value=({},{})))
    monkeypatch.setattr(dc,'bound_shell_identity',lambda binding: dict(pid=12,created=1))
    # Source repair must expose this read-only git identity boundary.
    monkeypatch.setattr(dc,'_git_route_identity',lambda path: (str(work), route['branch'], 'a'*40, ''),raising=False)
    return row, row['worker_route'], payload


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
    assert json.loads(dc.terminal_dispatch(args,executor))['sent'] is True
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
    monkeypatch.setattr(dc,'bind_visible_target',lambda target: dict(row['visible_binding'],target=target))
    token=dc.EVENT_CONTEXT.set(None)
    try:
        with pytest.raises(ValueError,match='native Main event'): dc.register_native(task)
        dc.EVENT_CONTEXT.set(dict(identity=dict(row['identity'],platform='cli')))
        with pytest.raises(ValueError,match='unsupported authorized'): dc.register_native(task)
        # Isolated fake registry, not a native live event or launch.
        dc.EVENT_CONTEXT.set(dict(identity=dict(row['identity'],session='second',session_key='second')))
        with pytest.raises(ValueError,match='confirmed coordinator identity'):
            dc.register_native(task)
        # A different session still needs an unowned exact surface.
        task['worker_route']=dict(route,visible_target=dict(route['visible_target'],
            surface='33333333-3333-4333-8333-333333333333'))
        task['identity'] = dc.EVENT_CONTEXT.get()['identity']
        authorize(guard, task)
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
    assert json.loads(dc.terminal_dispatch(dict(command='SYSTEM167_REGISTERED_WORKER',continuation_ticket=dc.worker_ticket(row)),executor))['sent']
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



@pytest.mark.parametrize('native_home', [False, True])
def test_codex_home_binds_native_environment_not_caller(bound, monkeypatch, tmp_path, native_home):
    import shlex
    row, route, _ = bound
    guard = dc.Guard(tmp_path / 'fresh-native-state.json')
    monkeypatch.setattr(dc, 'runtime_guard', lambda home: guard)
    monkeypatch.setattr(dc, 'bind_visible_target', lambda target: row['visible_binding'])
    monkeypatch.delenv('CODEX_HOME', raising=False)
    if native_home:
        monkeypatch.setenv('CODEX_HOME', str(tmp_path / 'native-codex'))
    task = dict(row, task='native-binding', scope='fixture only', owner='coding', gate='coding',
                action='inspect', wake_budget=1, producer='cmux', codex_home=str(tmp_path / 'untrusted'))
    token = dc.EVENT_CONTEXT.set(dict(identity=row['identity']))
    try:
        authorize(guard, task)
        _, registered = dc.register_native(task)
    finally:
        dc.EVENT_CONTEXT.reset(token)
    ticket = dc.worker_ticket(registered)
    command = shlex.split(dc.visible_worker_command(ticket))
    if native_home:
        assert ticket['codex_home'] == str(tmp_path / 'native-codex')
        assert 'CODEX_HOME=' + ticket['codex_home'] in command
    else:
        assert 'codex_home' not in ticket
        assert not any(word.startswith('CODEX_HOME=') for word in command)
    assert not (tmp_path / 'native-codex').exists()  # No auth/config contents are accessed.


@pytest.mark.parametrize('operation', ['route-preflight', 'route-check', 'route-bind'])
@pytest.mark.parametrize('intervening_claim', [False, True])
def test_route_failure_observation_respects_intervening_claim(bound, monkeypatch, operation, intervening_claim):
    row, route, guard = native_fixture(bound, monkeypatch)
    ticket = dc.worker_ticket(row)
    reject = Mock(side_effect=ValueError('fixture route rejection'))
    monkeypatch.setattr(dc, 'preflight_non_cmm', reject)
    monkeypatch.setattr(dc, 'route_check', reject)
    observe = guard.observe

    def after_lock_release(*args, **kwargs):
        # A separate transaction commits the valid send/claim between failure and observe.
        if intervening_claim:
            with guard.transaction() as tasks:
                tasks[row['task']].update(visible_sent=True, visible_sent_at=1000,
                    worker_pid=303, worker_identity=dict(pid=303, created=1000),
                    worker_started_ns=1000000, claim_phase='claimed')
        return observe(*args, **kwargs)

    monkeypatch.setattr(guard, 'observe', after_lock_release)
    with pytest.raises(ValueError, match='fixture route rejection'):
        dc.main(['--home', row['identity']['home'], operation, '--payload-json',
                 json.dumps(dict(ticket=ticket, route=route))])
    after = guard.get(row['task'])
    if intervening_claim:
        assert after['generation'] == ticket['generation']
        assert after['status'] == 'running' and after['claim_phase'] == 'claimed'
        assert after['worker_pid'] == 303 and after['visible_sent']
    else:
        assert after['generation'] == ticket['generation'] + 1
        assert after['gate'] == 'reconcile'
