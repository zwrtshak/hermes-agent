"""F1/F2 real adapter and gateway boundaries; synthetic data only."""
from types import SimpleNamespace
from unittest.mock import AsyncMock
import pytest
from tests.gateway.test_telegram_thread_fallback import _inject_fake_telegram, _make_adapter, FakeTimedOut, FakeBadRequest
from tests.gateway.test_diggr_origin_delivery import setup, result
from hermes_cli import diggr_continuation as dc

def bot_adapter(outcomes):
    a=_make_adapter(); a._should_attempt_rich=lambda *a,**kw:False
    a._bot=SimpleNamespace(send_message=AsyncMock(side_effect=outcomes)); a.send_typing=AsyncMock()
    return a

@pytest.mark.asyncio
@pytest.mark.parametrize('kind,expected,attempts',[('permanent','failed',1),('forbidden','failed',1),('safe_retry','failed',3),('rate_limit','failed',3),('timeout','uncertain',1),('network','uncertain',1),('partial','uncertain',1)])
async def test_real_adapter_outcomes_survive_reload(tmp_path,monkeypatch,kind,expected,attempts):
    r=await setup(tmp_path,monkeypatch); result(r)
    import httpx
    import telegram.error as errors
    from tests.gateway.test_telegram_thread_fallback import FakeRetryAfter, FakeNetworkError
    class Forbidden(Exception): pass
    monkeypatch.setattr(errors,"Forbidden",Forbidden,raising=False)
    monkeypatch.setattr(errors,"RetryAfter",FakeRetryAfter,raising=False)
    error=FakeTimedOut('Timed out')
    if kind=='safe_retry': error.__cause__=httpx.ConnectTimeout('not connected')
    if kind=='permanent': error=FakeBadRequest('chat not found')
    if kind=='forbidden': error=Forbidden('bot blocked')
    if kind=='rate_limit': error=FakeRetryAfter(1)
    if kind=='network': error=FakeNetworkError('connection lost after write')
    a=bot_adapter([SimpleNamespace(message_id=7),FakeBadRequest('chat not found')] if kind=='partial' else [error]*9)
    if kind=='partial': a.truncate_message=lambda *a,**kw:['first','second']
    r.runner._adapter_for_source=lambda s:a
    for _ in range(5):
        r.guard=dc.Guard(r.guard.path); await r.runner._diggr_deliver(r.guard,r.identity); r.clock[0]+=100
    item=r.guard.get(r.task['task'])['deliveries'][0]
    assert item['status']==expected
    assert item['attempts']==attempts
    assert a._bot.send_message.call_count==(2 if kind=='partial' else attempts)
    if kind=='partial': assert item['message_ids']==['7']

class Boundary(Exception): pass

@pytest.mark.asyncio
@pytest.mark.parametrize('profile',['diggr-main','mira'])
@pytest.mark.parametrize('topic',['','1','73'])
async def test_native_source_before_session_selection(tmp_path,monkeypatch,profile,topic):
    r=await setup(tmp_path,monkeypatch,profile,topic); result(r)
    jobs=[]; r.adapter.get_pending_message=lambda k:r.adapter._pending_messages.pop(k,None)
    r.adapter._start_session_processing=lambda e,k:jobs.append(e)
    await r.runner._diggr_tick(r.source,r.identity['session'],idle=True)
    event=jobs[0]; assert await r.runner._diggr_accept(event)
    r.runner._telegram_topic_mode_enabled=lambda s:True
    r.runner._session_db=SimpleNamespace(list_telegram_topic_bindings_for_chat=lambda **kw:[{'user_id':r.identity['user_id'],'thread_id':'99'}])
    seen=[]
    async def lookup(source): seen.append(source); raise Boundary()
    monkeypatch.setattr(r.runner.async_session_store,'get_or_create_session',lookup)
    with pytest.raises(Boundary): await r.runner._handle_message_with_agent(event,event.source,'fake',1)
    assert (seen[0].thread_id or '')==topic

@pytest.mark.asyncio
async def test_ordinary_user_keeps_topic_recovery(tmp_path,monkeypatch):
    r=await setup(tmp_path,monkeypatch)
    r.runner._telegram_topic_mode_enabled=lambda s:True
    r.runner._session_db=SimpleNamespace(list_telegram_topic_bindings_for_chat=lambda **kw:[{'user_id':r.identity['user_id'],'thread_id':'99'}])
    event=SimpleNamespace(source=r.source,text='ordinary user'); seen=[]
    async def lookup(source): seen.append(source); raise Boundary()
    monkeypatch.setattr(r.runner.async_session_store,'get_or_create_session',lookup)
    with pytest.raises(Boundary): await r.runner._handle_message_with_agent(event,event.source,'fake',1)
    assert seen[0].thread_id=='99'

@pytest.mark.asyncio
@pytest.mark.parametrize('profile',['diggr-main','mira'])
@pytest.mark.parametrize('lineage',['compression','reset','unrelated','missing','stale_parent'])
async def test_session_lineage_before_agent_context(tmp_path,monkeypatch,profile,lineage):
    from hermes_state import SessionDB, AsyncSessionDB
    r=await setup(tmp_path,monkeypatch,profile); result(r)
    jobs=[]; r.adapter.get_pending_message=lambda k:r.adapter._pending_messages.pop(k,None)
    r.adapter._start_session_processing=lambda e,k:jobs.append(e)
    await r.runner._diggr_tick(r.source,r.identity['session'],idle=True)
    event=jobs[0]
    db=SessionDB(db_path=tmp_path/'lineage.db'); r.runner._session_db=AsyncSessionDB(db)
    parent=r.identity['session']
    db.create_session(parent,source='telegram')
    db.end_session(parent,end_reason='compression' if lineage!='reset' else 'session_reset')
    if lineage!='missing': db.create_session('child',source='telegram',parent_session_id=parent if lineage!='unrelated' else None)
    entry=SimpleNamespace(session_id=parent if lineage=='stale_parent' else 'child',session_key=r.identity['session_key'])
    monkeypatch.setattr(r.runner.async_session_store,'get_or_create_session',AsyncMock(return_value=entry))
    try:
        assert await r.runner._diggr_accept(event)==(lineage=='compression')
        if lineage=='compression':
            def boundary(*args): raise Boundary('validated before context')
            r.runner._cache_session_source=boundary
            with pytest.raises(Boundary): await r.runner._handle_message_with_agent(event,event.source,'synthetic',1)
        else:
            with pytest.raises(ValueError): await r.runner._handle_message_with_agent(event,event.source,'synthetic',1)
        assert r.guard.get(r.task['task'])['origin']['identity']==r.identity
    finally: db.close()

@pytest.mark.asyncio
@pytest.mark.parametrize('profile',['diggr-main','mira'])
@pytest.mark.parametrize('topic',['','1','73'])
async def test_bound_local_pipeline(tmp_path,monkeypatch,profile,topic):
    """Real grants/dispatch/queue/outbox; OS, launcher and model are simulated."""
    import json
    from unittest.mock import Mock
    import psutil
    packet=tmp_path/'packet.json'; packet.write_text(json.dumps({'operator_scope_authorized':True,'plane_id':'synthetic','coding_route':{'required_model':'fixture'}}))
    receipt=tmp_path/'receipt.json'
    receipt.write_text(json.dumps({'verdict':'allowed','coding_route_contract':{'operator_scope_authorized':True,'plane_id':'synthetic'},'route_packet':str(packet)}))
    target={'workspace':'11111111-1111-4111-8111-111111111111','surface':'22222222-2222-4222-8222-222222222222'}
    shell=dict(pid=101,ppid=1,pgid=101,tpgid=101,tty='ttys001',start='synthetic',executable='/bin/zsh')
    surface=dict(kind='surface',id=target['surface'],type='terminal',tty='ttys001',top_level_pids=[101],root_pids=[101],tty_process_pids=[101],foreground_pgids=[101])
    monkeypatch.setattr(dc,'cmux_snapshot',lambda target:dict(kind='workspace',id=target['workspace'],children=[surface]))
    monkeypatch.setattr(dc,'process_identity',lambda pid:dict(shell))
    monkeypatch.setattr(psutil,'Process',lambda pid:SimpleNamespace(create_time=lambda:500.0))
    route=dict(argv=['codex','exec','--model','fixture'],worktree=str(tmp_path),receipt=str(receipt),packet=str(packet),packet_sha256=dc.digest(packet),plane_id='synthetic',branch='fixture',visible_target=target)
    r=await setup(tmp_path,monkeypatch,profile,topic,register=False,task_overrides=dict(producer='cmux',worker_route=route,launcher_command='synthetic-launcher'))
    from tools.process_registry import process_registry
    session=SimpleNamespace(id='fake-launch',session_key=r.identity['session_key'],started_at=1000,pid=202,pid_scope='host',process=SimpleNamespace(pid=202,poll=lambda:0),_pty=None,exited=True,exit_code=0)
    monkeypatch.setattr(process_registry,'get',lambda key:session if key=='fake-launch' else None)
    launcher=Mock(return_value={'session_id':'fake-launch'})
    sends=Mock(); monkeypatch.setattr(dc,'cmux_send',sends)
    ctx=dc.EVENT_CONTEXT.set({'identity':r.identity})
    try:
        admitted=dc.terminal_dispatch(dict(command='synthetic-launcher',background=True,continuation=r.task),launcher)
        ticket=admitted['continuation']['worker_ticket']
        with r.guard.transaction() as rows: dc.route_check(rows[r.task['task']],route,seal=True)
        dc.terminal_dispatch(dict(command='SYSTEM167_REGISTERED_WORKER',continuation_ticket=ticket),lambda args:pytest.fail('no second wrapper'))
        with pytest.raises(ValueError): dc.terminal_dispatch(dict(command='SYSTEM167_REGISTERED_WORKER',continuation_ticket=ticket),lambda args:pytest.fail('duplicate'))
    finally: dc.EVENT_CONTEXT.reset(ctx)
    assert launcher.call_count==1 and sends.call_count==1
    assert sends.call_args.args[0]['target']==target
    r.guard=dc.Guard(r.guard.path); result(r)
    jobs=[]; r.adapter.get_pending_message=lambda k:r.adapter._pending_messages.pop(k,None)
    r.adapter._start_session_processing=lambda e,k:jobs.append(e)
    # Persist before coordinator admission, then reload after it.
    await r.runner._diggr_tick(r.source,r.identity['session'],idle=True)
    assert len(jobs)==1 and await r.runner._diggr_accept(jobs[0])
    r.guard=dc.Guard(r.guard.path)
    assert not await r.runner._diggr_accept(jobs[0])
    assert await r.runner._diggr_finish(jobs[0],{'final_response':'SIMULATED coordinator answer'})
    r.guard=dc.Guard(r.guard.path)
    assert r.guard.get(r.task['task'])['deliveries'][-1]['content']=='SIMULATED coordinator answer'
    adapters={p:bot_adapter([SimpleNamespace(message_id=88)]) for p in ['diggr-main','mira']}
    r.runner._adapter_for_source=lambda source:adapters[source.profile]
    await r.runner._diggr_deliver(r.guard,r.identity)
    await r.runner._diggr_deliver(dc.Guard(r.guard.path),r.identity)
    sent=adapters[profile]._bot.send_message
    assert sent.call_count==1
    assert str(sent.call_args.kwargs['chat_id'])==r.identity['chat_id']
    assert str(sent.call_args.kwargs.get('direct_messages_topic_id') or '')==(topic if topic not in {'','1'} else '')
    if topic in {'','1'}: assert sent.call_args.kwargs.get('message_thread_id') is None
    assert adapters['mira' if profile=='diggr-main' else 'diggr-main']._bot.send_message.call_count==0
    assert r.guard.get(r.task['task'])['deliveries'][-1]['status']=='delivered'

@pytest.mark.asyncio
async def test_unclassified_negative_result_is_uncertain(tmp_path,monkeypatch):
    r=await setup(tmp_path,monkeypatch); result(r)
    r.adapter.send.return_value=SimpleNamespace(success=False,retryable=True)
    await r.runner._diggr_deliver(r.guard,r.identity)
    r.clock[0]+=100
    await r.runner._diggr_deliver(dc.Guard(r.guard.path),r.identity)
    assert r.adapter.send.call_count==1
    assert r.guard.get(r.task['task'])['deliveries'][0]['status']=='uncertain'

@pytest.mark.asyncio
async def test_restart_after_actual_send_claim_is_not_replayed(tmp_path,monkeypatch):
    import asyncio
    r=await setup(tmp_path,monkeypatch); result(r)
    a=bot_adapter([asyncio.CancelledError()]); r.runner._adapter_for_source=lambda s:a
    with pytest.raises(asyncio.CancelledError): await r.runner._diggr_deliver(r.guard,r.identity)
    assert dc.Guard(r.guard.path).get(r.task['task'])['deliveries'][0]['status']=='sending'
    r.clock[0]+=31
    await r.runner._diggr_deliver(dc.Guard(r.guard.path),r.identity)
    assert a._bot.send_message.call_count==1
    assert r.guard.get(r.task['task'])['deliveries'][0]['status']=='uncertain'

@pytest.mark.asyncio
async def test_changed_native_route_rejected_before_session_lookup(tmp_path,monkeypatch):
    r=await setup(tmp_path,monkeypatch); result(r)
    jobs=[]; r.adapter.get_pending_message=lambda k:r.adapter._pending_messages.pop(k,None)
    r.adapter._start_session_processing=lambda e,k:jobs.append(e)
    await r.runner._diggr_tick(r.source,r.identity['session'],idle=True)
    jobs[0].source.thread_id='99'
    lookup=AsyncMock(side_effect=AssertionError('must reject before selection'))
    monkeypatch.setattr(r.runner.async_session_store,'get_or_create_session',lookup)
    assert not await r.runner._diggr_accept(jobs[0])
    lookup.assert_not_called()
