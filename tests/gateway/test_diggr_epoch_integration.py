"""Real imported native queue and profile config; no network or worker launch."""
from tests.diggr_owner_fixtures import authorize, authorize_async

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock
from tests.gateway.test_diggr_origin_delivery import FixtureTransport

import pytest
from gateway.config import GatewayConfig, Platform
from gateway.run import GatewayRunner
from gateway.session import SessionSource, SessionStore
from hermes_cli import diggr_continuation as dc


@pytest.mark.asyncio
async def test_native_fifo_multiple_epochs_busy_and_control(tmp_path, monkeypatch):
    home = tmp_path.resolve()
    (home/'config.yaml').write_text('diggr_continuation:\n  enabled: true\n  profile: diggr-main\n  home: '+str(home)+'\n')
    monkeypatch.setenv('HERMES_HOME',str(home))
    guard = dc.runtime_guard(home)
    assert guard.path == home/'state/diggr-continuation.json'
    runner = object.__new__(GatewayRunner)
    runner.config = GatewayConfig()
    source = SessionSource(platform=Platform.TELEGRAM,profile='diggr-main',chat_id='564628210',user_id='564628210')
    runner._resolve_profile_home_for_source = lambda source: home
    runner.session_store = SessionStore(home/'sessions', runner.config)
    session = await runner.async_session_store.get_or_create_session(source)
    session_id = session.session_id
    adapter = FixtureTransport(_bot=SimpleNamespace(id='fixture-bot'),
        send=AsyncMock(return_value=SimpleNamespace(success=True, message_id='epoch-notice')),
        _pending_messages={},_active_sessions={})
    runner.adapters = {}
    runner._profile_adapters = {'diggr-main': {Platform.TELEGRAM: adapter}}
    identity = runner._diggr_identity(source,session_id)
    guard.register(await authorize_async(guard, dict(task='fixture',scope='temp file only',identity=identity,owner='coding',
        gate='coding',action='verify',artifact=str(home/'artifact'),deadline=1000,wake_budget=1)),now=1)
    guard.observe('fixture',1,'unknown',now=2)
    clock=[guard.get('fixture')['due']]
    monkeypatch.setattr(dc.time,'time',lambda: clock[0])
    seen=[]
    async def consume(event):
        assert await runner._diggr_accept(event)
        wake=dc.token(event.text); row=guard.get('fixture'); seen.append(wake)
        (home/'artifact').write_text(str(len(seen)))
        proof={k:row[k] for k in ('task','generation','action','artifact')}
        proof.update(sha256=dc.digest(home/'artifact'),result='validated')
        assert guard.ack(identity,wake,proof,'main_live','verify')
    jobs=[]
    adapter.get_pending_message=lambda key: adapter._pending_messages.pop(key,None)
    adapter._start_session_processing=lambda event,key: jobs.append(asyncio.create_task(consume(event)))
    key=identity['session_key']
    adapter._active_sessions[key]=True
    assert await runner._diggr_tick(source,session_id,idle=True)
    assert not adapter._pending_messages
    adapter._active_sessions.clear()
    for _ in range(4):
        await runner._diggr_tick(source,session_id,idle=True)
        await asyncio.gather(*jobs)
        clock[0]+=10
    assert len(seen)==4
    row=guard.get('fixture')
    assert row['wakes']==4 and row['epoch']==4
    assert not await runner._diggr_accept(SimpleNamespace(source=source,text=dc.prompt(dict(row,generation=seen[0]['generation']))))
    assert await runner._diggr_accept(SimpleNamespace(source=source,text='/stop'))
    assert guard.get('fixture')['status']=='paused'
    assert not await runner._diggr_tick(source,session_id,idle=True)


def test_cli_retry_receipt_on_temporary_home(tmp_path,monkeypatch):
    import json, time
    home=tmp_path.resolve()
    (home/'config.yaml').write_text('diggr_continuation:\n  enabled: true\n  profile: diggr-main\n  home: '+str(home)+'\n')
    monkeypatch.setenv('HERMES_HOME',str(home))
    guard=dc.runtime_guard(home)
    identity=dict(home=str(home),profile='diggr-main',session='fixture',session_key='fixture',
                  platform='telegram',chat_id='564628210',user_id='564628210',thread_id='')
    guard.register(authorize(guard, dict(task='fixture',scope='temp only',identity=identity,owner='coding',
        gate='coding',action='verify',artifact=str(home/'artifact'),deadline=time.time()+60,wake_budget=1)))
    guard.observe('fixture',1,'unknown')
    due=guard.get('fixture')['due']
    assert guard.tick(identity,now=due-1) is None
    wake=guard.tick(identity,now=due);assert guard.begin(identity,wake,now=due)
    payload=dict(task='fixture',generation=wake['generation'],identity=identity,
                 report=dict(reason='rate_limit',detail='fixture reset',retry_at=time.time()+120))
    assert dc.main(['--home', str(home), 'retry', '--payload-json', json.dumps(payload)]) == 0
    row=guard.get('fixture')
    assert row['gate']=='reconcile' and row['status']=='pending'
    assert row['due']>=payload['report']['retry_at']
    assert guard.tick(identity) is None
