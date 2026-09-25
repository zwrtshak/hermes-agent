"""New logical native grants constrain technical binding; exact grants stay exact."""
import copy
import json
import subprocess
from pathlib import Path

import pytest
from hermes_cli import diggr_continuation as dc, diggr_owner as owner
from tests.gateway.test_diggr_owner_grant import route, confirm, manifest


def git(path, *args):
    return subprocess.check_output(['git', '-C', str(path), *args], text=True, stderr=subprocess.PIPE).strip()


def prepare(r, monkeypatch):
    repo = r.tmp / 'repo'; repo.mkdir()
    git(repo, 'init', '-b', 'main')
    git(repo, 'config', 'user.email', 'fixture@example.invalid')
    git(repo, 'config', 'user.name', 'Fixture')
    (repo / 'src').mkdir(); (repo / 'src' / 'code.txt').write_text('initial\n')
    git(repo, 'add', '.'); git(repo, '-c', 'commit.gpgsign=false', 'commit', '-m', 'fixture')
    worktrees = r.tmp / 'worktrees'; worktrees.mkdir()
    work = worktrees / 'attempt'
    git(repo, 'worktree', 'add', '-b', 'hermes/SYSTEM-167-fixture', str(work))
    dispatcher = r.tmp / 'system' / 'ops' / 'scripts' / 'loop_control_dispatch_coding.sh'
    dispatcher.parent.mkdir(parents=True); dispatcher.write_text('#!/bin/bash\n')
    monkeypatch.setattr(dc, 'DISPATCHER', str(dispatcher))
    monkeypatch.setattr(dc, 'CODEX_EXECUTABLE', '/tools/codex')
    artifacts = dispatcher.parents[2] / 'ops/state/loop-control'
    (artifacts / 'routing_packets').mkdir(parents=True)
    contract = dict(version='logical-v1', scope=r.task['scope'], action=r.task['action'],
        owner='coding', gate='coding', producer='cmux', plane_id='SYSTEM-167',
        resources=dict(repo=str(repo), worktrees=str(worktrees), artifacts=str(artifacts),
                       allowed_paths=['src'], no_touch=['src/private']),
        actions=dict(coding=['inspect','edit','test'], main=['review','merge','smoke','plane_update','nextcloud_update']),
        acceptance=dict(owner='main', criteria=['fixture tests pass'], main_live_required=True),
        model=dict(executor='codex', required_model='gpt-6-astra', provider='openai',
                   fallback_models_allowed=[], reasoning_effort='ultra', sandbox='workspace-write', approval_policy='never'))
    route = dict(packet=str(artifacts / 'routing_packets/first.json'), receipt=str(artifacts / 'receipt.json'),
        worktree=str(work), branch='hermes/SYSTEM-167-fixture', plane_id='SYSTEM-167',
        visible_target={'workspace':'11111111-1111-4111-8111-111111111111','surface':'22222222-2222-4222-8222-222222222222'})
    artifact = str(artifacts / 'result.txt')
    route['argv'] = ['/tools/codex','exec','--model','gpt-6-astra','--sandbox','workspace-write',
        '--cd',str(work),'--output-last-message',artifact,'-c','model_provider="openai"',
        '-c','model_reasoning_effort="ultra"','-c','approval_policy="never"','implement within approved scope']
    packet = dict(routing_mode='non_cmm', operator_scope_authorized=True, plane_id='SYSTEM-167',
        task_id='SYSTEM-167', scope_id='SYSTEM-167', worktree=str(work), branch=route['branch'],
        base=git(work,'rev-parse','HEAD'), requested_actions=['inspect','edit'],
        logical_resources=copy.deepcopy(contract['resources']), acceptance=copy.deepcopy(contract['acceptance']),
        role_actions=copy.deepcopy(contract['actions']), coding_route=dict(contract['model'],
        routine_actions_authorized=contract['actions']['coding'], merge_authorized=False))
    Path(route['packet']).write_text(json.dumps(packet)); route['packet_sha256']=dc.digest(route['packet'])
    r.task.update(producer='cmux', worker_route=route, artifact=artifact,
                  launcher_command=dc.logical_launcher_command(contract,route))
    monkeypatch.setattr(dc,'bind_visible_target',lambda target:dict(target=target))
    monkeypatch.setattr(dc,'bound_shell_identity',lambda binding:dict(pid=101))
    monkeypatch.setattr(dc,'verify_visible_target',lambda *a,**kw:None)
    return contract, packet


async def authorize_logical(r, contract):
    proposal=manifest(r.task); proposal['todos'][0]['contract']=contract
    bid, event, command=await confirm(r, proposal)
    with r.guard.transaction(read_only=True) as rows:
        r.identity=copy.deepcopy(rows.grants['batches'][bid]['coordinator_identity'])
    from tests.diggr_owner_fixtures import bind_native_transport_fixture
    bind_native_transport_fixture(r.identity)
    r.task.update(identity=r.identity,owner_batch=bid,owner_issue=owner.issue_key(proposal['todos'][0]))
    return proposal


@pytest.mark.asyncio
async def test_technical_choices_after_one_native_logical_confirmation(route,monkeypatch):
    r=route; contract, packet=prepare(r,monkeypatch)
    await authorize_logical(r,contract)
    before=r.guard.path.read_bytes()
    first=copy.deepcopy(r.task)
    dc.validate_logical_route(first,contract)
    second=copy.deepcopy(first)
    second['artifact']=str(Path(second['artifact']).with_name('second-result.txt'))
    second['worker_route']['packet']=str(Path(second['worker_route']['packet']).with_name('second.json'))
    second['worker_route']['receipt']=str(Path(second['worker_route']['receipt']).with_name('second-receipt.json'))
    argv=second['worker_route']['argv']; argv[argv.index('--output-last-message')+1]=second['artifact']; argv[-1]='different concrete prompt, same rights'
    Path(second['worker_route']['packet']).write_text(json.dumps(packet))
    second['launcher_command']=dc.logical_launcher_command(contract,second['worker_route'])
    dc.validate_logical_route(second,contract)
    assert r.guard.path.read_bytes()==before
    token=dc.EVENT_CONTEXT.set(dict(identity=r.identity))
    try:
        _, admitted=dc.register_native(second)
        assert admitted['logical_contract']==contract
        assert admitted['policy']['batch']==r.task['owner_batch']
        assert len(r.guard.get(second['task'])['worker_route']['argv'])>0
        registered=r.guard.path.read_bytes()
        assert dc.register_native(second)[1]['ticket_nonce']==admitted['ticket_nonce']
        assert r.guard.path.read_bytes()==registered
        with pytest.raises(ValueError): dc.register_native(first)
    finally: dc.EVENT_CONTEXT.reset(token)
    with r.guard.transaction(read_only=True) as rows:
        assert len(rows.grants['batches'])==1 and len(rows.grants['confirmation_events'])==1


@pytest.mark.asyncio
@pytest.mark.parametrize('change',['scope','repo','worktree','actions','no_touch','allowed_paths','acceptance',
    'model','provider','fallback','launcher','result_escape','receipt_escape','packet_escape','symlink',
    'budget','wake_budget','recipient','forged_binding','argv_provider','executor','prompt_escape'])
async def test_logical_expansion_rejected_before_admission(route,monkeypatch,change):
    r=route; contract,packet=prepare(r,monkeypatch)
    await authorize_logical(r,contract)
    task=copy.deepcopy(r.task); current=task['worker_route']
    if change=='prompt_escape': packet['coding_prompt_source']=str(r.tmp/'outside-prompt.txt')
    if change=='scope': task['scope']='expanded'
    if change=='repo': packet['logical_resources']['repo']=str(r.tmp)
    if change=='worktree': current['worktree']=str(r.tmp)
    if change=='actions': packet['requested_actions']=['merge']
    if change=='no_touch': packet['logical_resources']['no_touch']=[]
    if change=='allowed_paths': packet['logical_resources']['allowed_paths']=['.']
    if change=='acceptance': packet['acceptance']['main_live_required']=False
    if change=='model': packet['coding_route']['required_model']='other'
    if change=='provider': packet['coding_route']['provider']='other'
    if change=='fallback': packet['coding_route']['fallback_models_allowed']=['other']
    if change=='launcher': task['launcher_command']+='; true'
    if change=='result_escape': task['artifact']=str(r.tmp/'outside.txt')
    if change=='receipt_escape': current['receipt']=str(r.tmp/'outside-receipt.json')
    if change=='packet_escape': current['packet']=str(r.tmp/'outside-packet.json')
    if change=='symlink':
        alias=Path(current['receipt']).with_name('alias'); alias.symlink_to(r.tmp,target_is_directory=True)
        current['receipt']=str(alias/'receipt.json')
    if change=='budget': task['deadline']=r.clock[0]+owner.BUDGETS['todo_seconds']+1
    if change=='wake_budget': task['wake_budget']=owner.BUDGETS['wakes']+1
    if change=='forged_binding': task['logical_contract']=contract
    if change=='executor': current['argv'][0]='/elsewhere/codex'
    if change=='argv_provider': current['argv'][current['argv'].index('model_provider="openai"')]='model_provider="other"'
    Path(current['packet']).write_text(json.dumps(packet));current['packet_sha256']=dc.digest(current['packet'])
    identity=dict(r.identity,chat_id='other') if change=='recipient' else r.identity
    token=dc.EVENT_CONTEXT.set(dict(identity=identity)); before=r.guard.path.read_bytes()
    try:
        with pytest.raises((ValueError,subprocess.CalledProcessError)): dc.register_native(task)
        assert r.guard.path.read_bytes()==before
    finally: dc.EVENT_CONTEXT.reset(token)


@pytest.mark.asyncio
async def test_raw_guard_cannot_assert_logical_proof(route,monkeypatch):
    r=route;contract,_=prepare(r,monkeypatch);await authorize_logical(r,contract)
    with pytest.raises(ValueError,match='native validated'): r.guard.register(r.task)


@pytest.mark.asyncio
async def test_old_exact_contract_rejects_technical_prompt_change(route,monkeypatch):
    r=route;contract,_=prepare(r,monkeypatch)
    bid,_,_=await confirm(r)
    with r.guard.transaction(read_only=True) as rows:
        r.identity=copy.deepcopy(rows.grants['batches'][bid]['coordinator_identity'])
    from tests.diggr_owner_fixtures import bind_native_transport_fixture
    bind_native_transport_fixture(r.identity)
    r.task.update(identity=r.identity,owner_batch=bid,owner_issue=owner.issue_key(manifest(r.task)['todos'][0]))
    r.task['worker_route']['argv'][-1]='changed prompt'
    token=dc.EVENT_CONTEXT.set(dict(identity=r.identity))
    try:
        with pytest.raises(ValueError,match='differs from owner-confirmed'): dc.register_native(r.task)
    finally: dc.EVENT_CONTEXT.reset(token)

@pytest.mark.asyncio
@pytest.mark.parametrize('path,staged', [('src/code.txt',False),('src/private/secret.txt',False),('src/PRIVATE/secret.txt',False),('other.txt',True)])
async def test_actual_diff_paths_enforced_before_main_ack(route,monkeypatch,path,staged):
    r=route; contract,_=prepare(r,monkeypatch); await authorize_logical(r,contract)
    token=dc.EVENT_CONTEXT.set(dict(identity=r.identity))
    try: _,row=dc.register_native(r.task)
    finally: dc.EVENT_CONTEXT.reset(token)
    work=Path(row['worker_route']['worktree']); changed=work/path
    changed.parent.mkdir(parents=True,exist_ok=True); changed.write_text('changed\n')
    if staged: git(work,'add','.')
    artifact=Path(row['artifact']);artifact.write_text('fixture result')
    with r.guard.transaction() as rows:
        rows[row['task']].update(status='executing',lease=1100)
    evidence=dict(task=row['task'],generation=row['generation'],action=row['action'],artifact=row['artifact'],
                  sha256=dc.digest(artifact),result='validated')
    if path=='src/code.txt':
        assert r.guard.ack(r.identity,row,evidence,'main_live','review fixture',now=1000)
    else:
        before=r.guard.path.read_bytes()
        with pytest.raises(ValueError,match='No-Touch'):
            r.guard.ack(r.identity,row,evidence,'main_live','review fixture',now=1000)
        assert r.guard.path.read_bytes()==before

@pytest.mark.asyncio
@pytest.mark.parametrize('during',['lease_expiry','stop'])
async def test_main_ack_rechecks_time_and_stop_after_diff(route,monkeypatch,during):
    r=route;contract,_=prepare(r,monkeypatch);await authorize_logical(r,contract)
    token=dc.EVENT_CONTEXT.set(dict(identity=r.identity))
    try: _,row=dc.register_native(r.task)
    finally:dc.EVENT_CONTEXT.reset(token)
    artifact=Path(row['artifact']);artifact.write_text('result')
    with r.guard.transaction() as rows:rows[row['task']].update(status='executing',lease=1100)
    evidence=dict(task=row['task'],generation=row['generation'],action=row['action'],artifact=row['artifact'],
        sha256=dc.digest(artifact),result='validated')
    inspect=dc.validate_logical_changes
    def delayed(task):
        inspect(task)
        if during=='lease_expiry':r.clock[0]=1101
        else:r.guard.control(r.identity,'paused')
    monkeypatch.setattr(dc,'validate_logical_changes',delayed)
    assert not r.guard.ack(r.identity,row,evidence,'main_live','review')
    after=r.guard.get(row['task'])
    assert after['status']!='done' and after['gate']!='main_live'
    if during=='stop':assert after['status']=='paused'


@pytest.mark.asyncio
async def test_personal_acceptance_cannot_be_replaced_by_main_json(route,monkeypatch):
    r=route;contract,packet=prepare(r,monkeypatch)
    contract['acceptance']['owner']='user';packet['acceptance']=copy.deepcopy(contract['acceptance'])
    Path(r.task['worker_route']['packet']).write_text(json.dumps(packet))
    r.task['worker_route']['packet_sha256']=dc.digest(r.task['worker_route']['packet'])
    await authorize_logical(r,contract)
    token=dc.EVENT_CONTEXT.set(dict(identity=r.identity))
    try:_,row=dc.register_native(r.task)
    finally:dc.EVENT_CONTEXT.reset(token)
    artifact=Path(row['artifact']);artifact.write_text('result')
    with r.guard.transaction() as rows:rows[row['task']].update(status='executing',gate='main_live',lease=1100)
    final=Path(row['artifact']).with_name('main-final.json')
    final.write_text(json.dumps(dict(task=row['task'],generation=row['generation'],action=row['action'],
                                    authorization=row['authorization'],result='passed',checks=['Main claims accepted'])))
    evidence=dict(task=row['task'],generation=row['generation'],action=row['action'],artifact=row['artifact'],
        sha256=dc.digest(artifact),result='validated',authorization=row['authorization'],final_gate='passed',
        final_artifact=str(final),final_sha256=dc.digest(final))
    before=r.guard.path.read_bytes()
    assert not r.guard.ack(r.identity,row,evidence,'done','complete',now=1000)
    assert r.guard.path.read_bytes()==before
