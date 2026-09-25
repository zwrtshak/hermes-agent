"""Real PTB adapter -> Guard -> native FIFO -> admission; only transport/effects are fake.

No Telegram connection, worker, installed state or credentials. This is isolated
integration evidence, not live Telegram acceptance.
Kept outside tests/gateway because its conftest replaces PTB with a module mock.
"""
import asyncio
import copy
import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
import pytest_asyncio
from telegram import CallbackQuery, Chat, Message, Update, User

from gateway.config import GatewayConfig, Platform, PlatformConfig
from gateway.platforms.base import MessageEvent
from gateway.run import GatewayRunner
from gateway.session import SessionSource, SessionStore, build_session_context
from hermes_state import SessionDB, AsyncSessionDB
from hermes_cli import diggr_continuation as dc, diggr_delivery as delivery, diggr_owner as owner
from plugins.platforms.telegram.adapter import TelegramAdapter
from tests.gateway.test_diggr_owner_grant import manifest
from tests.gateway.test_diggr_logical_admission import prepare


@pytest_asyncio.fixture
async def native(tmp_path, monkeypatch):
    home = tmp_path / 'diggr-main'
    home.mkdir()
    monkeypatch.setenv('HERMES_HOME', str(home))
    monkeypatch.setenv('CODEX_HOME', str(home / 'codex'))
    monkeypatch.setenv('TELEGRAM_ALLOWED_USERS', owner.OWNER)
    (home / 'config.yaml').write_text(f'diggr_continuation:\n  enabled: true\n  profile: diggr-main\n  home: {home}\n')
    clock = [1000.0]
    monkeypatch.setattr(dc.time, 'time', lambda: clock[0])
    guard = dc.runtime_guard(home)
    db = SessionDB(db_path=home / 'sessions.db')
    runner = object.__new__(GatewayRunner)
    runner.config = GatewayConfig()
    runner._resolve_profile_home_for_source = lambda source: home
    runner._load_background_notifications_mode = lambda: 'all'
    runner.session_store = SessionStore(home / 'sessions', runner.config)
    runner._session_db = AsyncSessionDB(db)
    runner.adapters = {}
    adapter = TelegramAdapter(PlatformConfig(enabled=True, token='offline-fixture', extra={}))
    adapter.gateway_runner = runner
    runner._profile_adapters = {'diggr-main': {Platform.TELEGRAM: adapter}}
    source = SessionSource(platform=Platform.TELEGRAM, profile='diggr-main', chat_id=owner.OWNER,
                           user_id=owner.OWNER, thread_id='73', chat_type='dm', message_id='101')
    entry = await runner.async_session_store.get_or_create_session(source)
    db.create_session(entry.session_id, source='telegram')
    identity = runner._diggr_identity(source, entry.session_id)
    bot_user = User(123, 'Offline Bot', True)
    messages = []

    async def send_message(**kwargs):
        if kwargs['text'].startswith('Begrenzter Coding-Auftrag'):
            # The production send must see a durable reservation and no authority.
            with guard.transaction(read_only=True) as rows:
                if rows.grants.get('proposals'):
                    assert any(p.get('preview', {}).get('send') == 'sending' for p in rows.grants['proposals'].values())
                    assert not rows.grants.get('batches')
            assert kwargs.get('reply_markup') is None
        message = Message(message_id=100 + len(messages), date=datetime.fromtimestamp(clock[0], timezone.utc),
            chat=Chat(int(kwargs['chat_id']), 'private'), from_user=bot_user, text=kwargs['text'],
            message_thread_id=kwargs.get('message_thread_id'))
        messages.append(message)
        return message

    bot = SimpleNamespace(id=123, username='offline_bot', send_message=AsyncMock(side_effect=send_message),
        edit_message_text=AsyncMock(return_value=True), edit_message_reply_markup=AsyncMock(return_value=True),
        answer_callback_query=AsyncMock(return_value=True))
    adapter._bot = bot
    jobs = []
    # Stop at the actual native turn boundary; tests explicitly execute acceptance/tools.
    adapter._start_session_processing = lambda event, key: jobs.append(event)
    task = dict(task='button-task', scope='inspect disposable fixture', action='inspect', owner='coding',
        gate='coding', artifact=str(home / 'result'), deadline=1100, wake_budget=10,
        authorization='disposable native grant', identity=identity)
    r = SimpleNamespace(tmp=tmp_path, home=home, guard=guard, runner=runner, adapter=adapter, bot=bot,
        source=source, entry=entry, identity=identity, clock=clock, db=db, task=task, messages=messages, jobs=jobs)
    yield r
    db.close()


def proposal_result(r, proposal=None):
    from gateway.session_context import completion_launch_context
    proposal = proposal or manifest(r.task)
    previous_transport = completion_launch_context.get()
    token = dc.EVENT_CONTEXT.set(dict(identity=r.identity))
    tokens = r.runner._set_session_env(build_session_context(r.source, r.runner.config, r.entry))
    try:
        invoke = Mock(side_effect=AssertionError('proposal must never dispatch'))
        result = json.loads(dc.terminal_dispatch(dict(command='SYSTEM167_PROPOSE_OWNER_BATCH',
            continuation_proposal=proposal), invoke))
        invoke.assert_not_called()
        return result
    finally:
        r.runner._clear_session_env(tokens)
        completion_launch_context.set(previous_transport)
        dc.EVENT_CONTEXT.reset(token)


def propose(r, proposal=None):
    return proposal_result(r, proposal)['proposal_sha256']


def repeat_proposal(r, digest, status, proposal=None):
    before = r.guard.path.read_bytes()
    sends, jobs = r.bot.send_message.call_count, len(r.jobs)
    result = proposal_result(r, proposal)
    assert result['proposal_sha256'] == digest
    assert result['status'] == status
    assert result['next_step']
    assert r.guard.path.read_bytes() == before
    assert (r.bot.send_message.call_count, len(r.jobs)) == (sends, jobs)
    return result


@pytest.mark.asyncio
@pytest.mark.parametrize('state', ['pending', 'open', 'declined', 'expired', 'revoked',
                                  'uncertain', 'failed', 'ui_failed', 'legacy_unbound'])
async def test_repeat_proposal_reports_state_without_mutation_or_dispatch(native, state):
    r = native
    digest = (owner.propose(r.guard, r.identity, manifest(r.task)) if state == 'legacy_unbound'
              else propose(r))
    if state == 'uncertain':
        r.bot.send_message.side_effect = TimeoutError('private send details')
        await r.runner._diggr_idle_tick()
    elif state == 'failed':
        # Still stored as pending: reporting must evaluate the deadline without
        # waiting for a tick, writing state or attempting a late original send.
        r.clock[0] += delivery.DELIVERY_SECONDS + 1
    elif state not in {'pending', 'legacy_unbound'}:
        if state == 'ui_failed':
            r.bot.edit_message_reply_markup.side_effect = TimeoutError('private markup details')
        await r.runner._diggr_idle_tick()
        if state == 'declined':
            await r.adapter._handle_callback_query(click(r, digest, 'd'), None)
        elif state == 'expired':
            r.clock[0] += owner.PREVIEW_SECONDS + 1  # No refresh before reporting.
            assert proposal_row(r, digest)['preview']['state'] == 'open'
        elif state == 'revoked':
            assert await r.runner._diggr_accept(MessageEvent(source=r.source, text='/stop'))
        elif state == 'ui_failed':
            for _ in range(delivery.MAX_ATTEMPTS):
                r.clock[0] += 60
                await r.runner._diggr_idle_tick()
    expected = ('native_preview_' + state if state in {'pending', 'failed', 'uncertain'}
                else 'open' if state == 'ui_failed' else state)
    result = repeat_proposal(r, digest, expected)
    assert not batch_rows(r) and not r.jobs
    if state in {'legacy_unbound', 'failed', 'uncertain', 'ui_failed'}:
        assert '/continuation show ' + digest in result['next_step']
        assert 'erscheint automatisch' not in result['message']
    if state == 'ui_failed':
        assert result['preview_ui'] == 'failed'
    if state == 'expired':
        assert 'erneuern' in result['next_step'].lower()
    if state in {'uncertain', 'failed', 'declined', 'expired', 'revoked', 'legacy_unbound'}:
        sends = r.bot.send_message.call_count
        for _ in range(3):
            await r.runner._diggr_idle_tick()
        assert r.bot.send_message.call_count == sends
        assert not batch_rows(r) and not r.jobs


@pytest.mark.asyncio
@pytest.mark.parametrize('confirmed', [False, True])
@pytest.mark.parametrize('change', ['session', 'session_key', 'profile', 'bot', 'transport_profile', 'namespace', 'reply'])
async def test_repeat_proposal_preserves_origin_and_reply_drift_exemption(native, monkeypatch, confirmed, change):
    from dataclasses import replace
    r = native
    digest = await publish(r)
    if confirmed:
        await r.adapter._handle_callback_query(click(r, digest), None)
    before = r.guard.path.read_bytes()
    if change == 'session':
        r.db.create_session('new-session', source='telegram')
        await r.runner.async_session_store.switch_session(r.identity['session_key'], 'new-session')
        r.entry = await r.runner.async_session_store.get_or_create_session(r.source)
    elif change == 'session_key':
        # Same Telegram owner/topic, different configured session partition.
        r.runner.config.multiplex_profiles = True
        r.entry = await r.runner.async_session_store.get_or_create_session(r.source)
        assert r.entry.session_key != r.identity['session_key']
        await r.runner.async_session_store.switch_session(r.entry.session_key, r.identity['session'])
    elif change == 'profile':
        r.source = replace(r.source, profile='mira')
        r.runner._profile_adapters['mira'] = {Platform.TELEGRAM: r.adapter}
        r.entry = await r.runner.async_session_store.get_or_create_session(r.source)
    elif change == 'bot':
        r.bot.id = 456
    elif change == 'transport_profile':
        r.runner._profile_adapters = {'mira': {Platform.TELEGRAM: r.adapter}}
        r.runner.adapters = {Platform.TELEGRAM: r.adapter}
    elif change == 'namespace':
        import gateway.run as run
        monkeypatch.setattr(run, '_hermes_home', r.home / 'foreign')
    else:
        r.source = replace(r.source, message_id='909')
    r.identity = r.runner._diggr_identity(r.source, r.entry.session_id)
    if change == 'reply':
        repeat_proposal(r, digest, 'confirmed' if confirmed else 'open')
    else:
        with pytest.raises(ValueError, match='proposal.*(origin|owner|session|transport)') as error:
            proposal_result(r)
        for private in (digest, proposal_row(r, digest)['preview']['ref'], *batch_rows(r)):
            assert private not in str(error.value)
    assert r.guard.path.read_bytes() == before


@pytest.mark.asyncio
async def test_repeat_proposal_during_original_send_does_not_retry_uncertain_reservation(native):
    r = native
    digest = propose(r)
    entered, release = asyncio.Event(), asyncio.Event()
    original = r.bot.send_message.side_effect

    async def blocked_send(**kwargs):
        message = await original(**kwargs)
        entered.set()
        await release.wait()
        return message

    r.bot.send_message.side_effect = blocked_send
    pending = asyncio.create_task(delivery.deliver_previews(r.runner, r.guard, r.identity))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        assert repeat_proposal(r, digest, 'native_preview_pending')['preview_send'] == 'sending'
        r.clock[0] = proposal_row(r, digest)['preview']['send_due'] + 1
        result = repeat_proposal(r, digest, 'native_preview_uncertain')
        assert result['preview_send'] == 'uncertain'
        await delivery.deliver_previews(r.runner, r.guard, r.identity)
    finally:
        release.set()
        await pending
    for _ in range(3):
        await r.runner._diggr_idle_tick()
    assert r.bot.send_message.call_count == 1
    assert len(r.messages) == 1
    r.bot.edit_message_reply_markup.assert_not_called()
    assert not batch_rows(r) and not r.jobs
    repeat_proposal(r, digest, 'native_preview_uncertain')


@pytest.mark.asyncio
@pytest.mark.parametrize('confirmed', [False, True])
async def test_legacy_text_binding_is_reported_but_never_adopted_or_disclosed_to_new_origin(native, confirmed):
    r = native
    digest = owner.propose(r.guard, r.identity, manifest(r.task))
    await r.runner._diggr_accept(text_event(r, '/continuation show ' + digest, 202))
    shown = proposal_row(r, digest)['shown']
    if confirmed:
        await r.runner._diggr_accept(text_event(r, '/continuation confirm ' + digest + ' ' + shown['code'], 303))
    result = repeat_proposal(r, digest, 'confirmed' if confirmed else 'legacy_unbound')
    assert result['preview_send'] == 'unbound'
    assert not proposal_row(r, digest).get('preview')
    assert shown['code'] not in json.dumps(result)
    before = r.guard.path.read_bytes()
    r.db.create_session('new-session', source='telegram')
    await r.runner.async_session_store.switch_session(r.identity['session_key'], 'new-session')
    r.entry = await r.runner.async_session_store.get_or_create_session(r.source)
    r.identity = r.runner._diggr_identity(r.source, r.entry.session_id)
    with pytest.raises(ValueError, match='different native session or transport') as error:
        proposal_result(r)
    assert digest not in str(error.value) and shown['code'] not in str(error.value)
    assert r.guard.path.read_bytes() == before


@pytest.mark.asyncio
@pytest.mark.parametrize('merge', [False, True])
async def test_confirmed_preview_payload_keeps_full_neutral_contract(native, monkeypatch, merge):
    r = native
    contract, _ = prepare(r, monkeypatch)
    if not merge:
        contract['actions']['main'].remove('merge')
    proposal = manifest(r.task)
    proposal['todos'][0]['contract'] = contract
    digest = await publish(r, proposal)
    original = r.bot.send_message.call_args.kwargs['text']
    await r.adapter._handle_callback_query(click(r, digest), None)
    payload = r.bot.edit_message_text.call_args.kwargs
    text = payload['text']
    assert 'Noch keine Freigabe' not in text
    assert 'kein Worker gestartet' not in text
    assert 'vorgemerkt' not in text
    assert 'Bestätigt' in text and payload['parse_mode'] is None
    assert 'Merge: ' + ('JA (nur Main)' if merge else 'NEIN') in text
    for field in ('scope', 'action', 'plane_id', 'owner', 'gate', 'producer'):
        assert contract[field] in text
    for value in contract['resources'].values():
        assert owner.canonical(value) in text
    for role in ('coding', 'main'):
        assert ', '.join(contract['actions'][role]) in text
    for value in (contract['model'], contract['acceptance'], proposal['budgets'], proposal['replaces'],
                  proposal['todos'][0]['depends_on'], proposal['todos'][0]['gates']):
        assert owner.canonical(value) in text
    assert owner.issue_key(proposal['todos'][0]) in text
    assert text.startswith(proposal_row(r, digest)['preview']['text'] + '\n\n')
    assert len(text.encode('utf-16-le')) // 2 <= 4096
    assert len(original.encode('utf-16-le')) // 2 <= 4096


def proposal_row(r, digest):
    with r.guard.transaction(read_only=True) as rows:
        return copy.deepcopy(rows.grants['proposals'][digest])


def batch_rows(r):
    with r.guard.transaction(read_only=True) as rows:
        return copy.deepcopy(rows.grants.get('batches', {}))


def click(r, digest, action='a', query_id='click-1', **changes):
    preview = proposal_row(r, digest)['preview']
    message = Message(message_id=int(changes.pop('message_id', preview.get('message_id', 100))),
        date=datetime.fromtimestamp(r.clock[0], timezone.utc), chat=Chat(int(changes.pop('chat', owner.OWNER)), changes.pop('chat_type', 'private')),
        from_user=User(changes.pop('bot_id', 123), 'Offline Bot', True),
        text=preview['text'], message_thread_id=changes.pop('thread', 73))
    query = CallbackQuery(query_id, User(changes.pop('user', int(owner.OWNER)), 'Owner', False),
        'offline-instance', message=message, data=changes.pop('data', f"og:{preview['version']}:{action}:{preview['ref']}"))
    query.set_bot(changes.pop('bot', r.bot))
    assert not changes
    return Update(10, callback_query=query)


async def publish(r, proposal=None):
    digest = propose(r, proposal)
    await r.runner._diggr_idle_tick()
    assert proposal_row(r, digest)['preview']['send'] == 'bound'
    return digest


@pytest.mark.asyncio
async def test_button_to_native_main_to_real_logical_admission_and_once_only_send(native, monkeypatch):
    r = native
    contract, _ = prepare(r, monkeypatch)
    proposal = manifest(r.task)
    proposal['todos'][0]['contract'] = contract
    digest = await publish(r, proposal)
    pv = proposal_row(r, digest)['preview']
    assert pv['recipient']['bot_id'] == str(r.bot.id)
    assert not batch_rows(r) and not r.jobs
    markup = r.bot.edit_message_reply_markup.call_args.kwargs['reply_markup']
    assert all(1 <= len(b.callback_data.encode()) <= 64 for row in markup.inline_keyboard for b in row)
    assert 'Merge: JA (nur Main)' in pv['text']
    assert all(word in pv['text'] for word in ['Aufgabe:', 'Repo:', 'No-Touch:', 'Coding-Rechte:', 'Main-Rechte:',
        'gpt-6-astra', 'fallback_models_allowed', 'Budgets:', 'Abnahme:'])
    update = click(r, digest)
    await r.adapter._handle_callback_query(update, None)
    bid, batch = next(iter(batch_rows(r).items()))
    result = repeat_proposal(r, digest, 'confirmed', proposal)
    assert result['batch_id'] == bid and result['admitted_tasks'] == 0
    assert result['coordinator_status'] == 'pending'
    confirmed_text = r.bot.edit_message_text.call_args.kwargs['text']
    assert confirmed_text.startswith(pv['text'] + '\n\n')
    assert 'Bestätigt' in confirmed_text
    assert 'Noch keine Freigabe' not in confirmed_text
    assert 'kein Worker gestartet' not in confirmed_text
    assert 'vorgemerkt' not in confirmed_text
    assert batch['tasks'] == {} and batch['coordinator_delivery']['status'] == 'pending'
    assert not r.jobs  # Callback never runs Main or Codex.
    await r.runner._diggr_idle_tick()
    assert len(r.jobs) == 1
    event = r.jobs[0]
    assert event.internal and event._diggr_wake.batch == bid and event._diggr_wake.task == ''
    assert await r.runner._diggr_accept(event)
    assert not await r.runner._diggr_accept(event)
    tokens = await delivery.set_tool_context(r.runner, event,
        build_session_context(r.source, r.runner.config, r.entry), r.entry)
    try:
        r.task.update(owner_batch=bid, owner_issue=owner.issue_key(proposal['todos'][0]))
        _, admitted = dc.register_native(r.task)
        assert admitted['logical_contract'] == contract
        result = repeat_proposal(r, digest, 'confirmed', proposal)
        assert result['admitted_tasks'] == 1
        before = r.guard.path.read_bytes()
        assert dc.register_native(r.task)[1]['_native_admission_replayed']
        assert r.guard.path.read_bytes() == before
        # Real packet/model/Git/resource route checks; only the physical cmux send is replaced.
        ticket = dc.worker_ticket(admitted)
        assert dc.main(['--home', str(r.home), 'route-preflight', '--payload-json',
            json.dumps(dict(ticket=ticket, route=admitted['worker_route']))]) == 0
        send = Mock()
        monkeypatch.setattr(dc, 'cmux_send', send)
        invoke = Mock(side_effect=AssertionError('no external worker here'))
        args = dict(command='SYSTEM167_REGISTERED_WORKER', continuation_ticket=ticket)
        assert json.loads(dc.terminal_dispatch(args, invoke))['sent']
        assert json.loads(dc.terminal_dispatch(args, invoke))['already_registered']
        result = repeat_proposal(r, digest, 'confirmed', proposal)
        assert result['admitted_tasks'] == 1
        await r.adapter.edit_owner_preview(r.identity['chat_id'], proposal_row(r, digest)['preview'])
        assert r.bot.edit_message_text.call_args.kwargs['text'] == confirmed_text
        send.assert_called_once()
        invoke.assert_not_called()
    finally:
        r.runner._clear_session_env(tokens)
    assert await r.runner._diggr_finish(event, {'final_response': 'Native Admission geprüft; Transport im Test ersetzt.'})
    await r.adapter._handle_callback_query(update, None)
    await r.adapter._handle_callback_query(click(r, digest, query_id='second-click'), None)
    assert len(batch_rows(r)) == 1
    assert len(batch_rows(r)[bid]['tasks']) == 1
    assert r.bot.edit_message_reply_markup.call_args.kwargs['reply_markup'] is None
    assert await r.runner._diggr_accept(MessageEvent(source=r.source, text='/stop'))
    await r.runner._diggr_idle_tick()
    assert repeat_proposal(r, digest, 'revoked', proposal)['batch_id'] == bid
    stopped_text = r.bot.edit_message_text.call_args.kwargs['text']
    assert stopped_text.startswith(pv['text'] + '\n\n') and 'Widerrufen' in stopped_text
    assert 'kein Worker gestartet' not in stopped_text


@pytest.mark.asyncio
@pytest.mark.parametrize('change', ['owner', 'bot_sender', 'bot_object', 'chat', 'group', 'topic', 'message',
    'profile', 'namespace', 'selector', 'version', 'digest', 'display_recipient', 'json', 'absent', 'inaccessible', 'allowlist'])
async def test_foreign_or_untrusted_callback_never_authorizes(native, monkeypatch, change):
    r = native
    digest = await publish(r)
    kwargs = {'owner': {'user': 88}, 'bot_sender': {'bot_id': 456}, 'bot_object': {'bot': SimpleNamespace(id=123)},
              'chat': {'chat': 88}, 'group': {'chat_type': 'supergroup'}, 'topic': {'thread': 74},
              'message': {'message_id': 101}, 'selector': {'data': 'og:1:a:unknown_reference'},
              'version': {'data': 'og:2:a:' + proposal_row(r, digest)['preview']['ref']}}.get(change, {})
    update = click(r, digest, **kwargs)
    if change == 'profile':
        from gateway.profile_routing import ProfileRoute
        r.runner.config.multiplex_profiles = True
        r.runner.config.profile_routes = [ProfileRoute('wrong', 'telegram', 'mira')]
    if change == 'namespace':
        import gateway.run as run
        monkeypatch.setattr(run, '_hermes_home', r.home / 'foreign')
    if change in {'digest', 'display_recipient'}:
        with r.guard.transaction() as rows:
            p = rows.grants['proposals'][digest]
            if change == 'digest': p['manifest']['todos'][0]['contract']['scope'] = 'mutated'
            else: p['preview']['recipient']['user_id'] = '88'
    if change == 'json':
        update = SimpleNamespace(callback_query=update.callback_query)
    if change in {'absent', 'inaccessible'}:
        from telegram import InaccessibleMessage
        query = CallbackQuery('other', User(int(owner.OWNER), 'Owner', False), 'offline',
            message=None if change == 'absent' else InaccessibleMessage(Chat(int(owner.OWNER), 'private'), 100),
            data=update.callback_query.data)
        query.set_bot(r.bot)
        update = Update(11, callback_query=query)
    if change == 'allowlist': monkeypatch.setenv('TELEGRAM_ALLOWED_USERS', '88')
    await r.adapter._handle_callback_query(update, None)
    assert not batch_rows(r)
    assert not r.jobs


@pytest.mark.asyncio
@pytest.mark.parametrize('actions', [('a', 'a'), ('a', 'd'), ('d', 'a')])
async def test_guard_serializes_callback_decision_races(native, actions):
    r = native
    digest = await publish(r)
    await asyncio.gather(*(r.adapter._handle_callback_query(click(r, digest, action, 'race-' + str(i)), None)
                           for i, action in enumerate(actions)))
    p = proposal_row(r, digest)
    assert bool(p.get('batch')) != bool(p.get('declined'))
    assert len(batch_rows(r)) == int(bool(p.get('batch')))
    if p.get('batch'):
        assert next(iter(batch_rows(r).values()))['coordinator_delivery']['status'] == 'pending'


@pytest.mark.asyncio
async def test_expiry_renewal_needs_new_click_and_old_ref_cannot_replay(native):
    r = native
    digest = await publish(r)
    expired_click = click(r, digest)
    r.clock[0] += owner.PREVIEW_SECONDS + 1
    await r.adapter._handle_callback_query(expired_click, None)
    assert not batch_rows(r)
    assert proposal_row(r, digest)['preview']['state'] == 'expired'
    assert r.bot.edit_message_reply_markup.call_args.kwargs['reply_markup'].inline_keyboard[0][0].text == 'Vorschau erneuern'
    renewal = click(r, digest, 'r', 'renew')
    await r.adapter._handle_callback_query(renewal, None)
    assert not batch_rows(r)
    await r.adapter._handle_callback_query(renewal, None)
    await r.adapter._handle_callback_query(expired_click, None)
    assert not batch_rows(r)
    pv = proposal_row(r, digest)['preview']
    assert pv['version'] == 2 and pv['state'] == 'open'
    await r.adapter._handle_callback_query(click(r, digest, query_id='fresh-approval'), None)
    before = batch_rows(r)
    await r.adapter._handle_callback_query(click(r, digest, 'r', 'after-confirm'), None)
    assert batch_rows(r) == before
    assert proposal_row(r, digest)['preview']['version'] == 2


@pytest.mark.asyncio
@pytest.mark.parametrize('control', ['/stop', '/reset', '/new'])
@pytest.mark.parametrize('confirmed', [False, True])
async def test_native_controls_suppress_proposal_and_batch_wake(native, control, confirmed):
    r = native
    digest = await publish(r)
    approval = click(r, digest)
    if confirmed:
        await r.adapter._handle_callback_query(approval, None)
    assert await r.runner._diggr_accept(MessageEvent(source=r.source, text=control))
    await r.adapter._handle_callback_query(approval, None)
    await r.runner._diggr_idle_tick()
    assert not r.jobs
    assert proposal_row(r, digest)['revoked']
    assert all(b['revoked'] and b['coordinator_delivery']['status'] == 'suppressed' for b in batch_rows(r).values())
    repeat_proposal(r, digest, 'revoked')


@pytest.mark.asyncio
async def test_restart_after_display_and_busy_main_fifo(native):
    r = native
    digest = await publish(r)
    r.guard = dc.Guard(r.guard.path)
    # Restore the actual routing index, not an invented target from callback JSON.
    r.runner.session_store = SessionStore(r.home / 'sessions', r.runner.config)
    await r.adapter._handle_callback_query(click(r, digest), None)
    key = r.identity['session_key']
    r.adapter._active_sessions[key] = True
    await r.runner._diggr_idle_tick()
    assert not r.jobs
    pending = r.adapter._pending_messages[key]
    assert pending._diggr_wake.batch
    r.clock[0] += 100
    await r.runner._diggr_idle_tick()
    assert r.adapter._pending_messages[key] is pending  # Busy is never an interrupt or another wake.
    r.adapter._active_sessions.pop(key)
    await r.runner._diggr_idle_tick()
    assert len(r.jobs) == 1 and await r.runner._diggr_accept(r.jobs[0])


@pytest.mark.asyncio
async def test_uncertain_original_send_never_retries_or_has_buttons(native):
    r = native
    digest = propose(r)
    r.bot.send_message.side_effect = TimeoutError('sensitive transport details')
    for _ in range(4):
        await r.runner._diggr_idle_tick()
        r.clock[0] += 30
    assert r.bot.send_message.call_count == 1
    r.bot.edit_message_reply_markup.assert_not_called()
    assert proposal_row(r, digest)['preview']['send'] == 'uncertain'
    await r.adapter._handle_callback_query(click(r, digest), None)
    assert not batch_rows(r)
    assert 'sensitive transport' not in r.guard.path.read_text()


@pytest.mark.asyncio
async def test_commit_survives_ack_and_markup_failure_without_second_batch(native):
    r = native
    digest = await publish(r)
    r.bot.answer_callback_query.side_effect = TimeoutError('private callback')
    r.bot.edit_message_reply_markup.side_effect = TimeoutError('private markup')
    update = click(r, digest)
    await r.adapter._handle_callback_query(update, None)
    before = batch_rows(r)
    assert len(before) == 1
    await r.adapter._handle_callback_query(update, None)
    assert batch_rows(r) == before
    r.bot.edit_message_reply_markup.side_effect = None
    r.clock[0] += 31
    await r.runner._diggr_idle_tick()
    assert len(r.jobs) == 1 and await r.runner._diggr_accept(r.jobs[0])
    assert proposal_row(r, digest)['preview']['ui_applied'] == proposal_row(r, digest)['preview']['ui_revision']
    assert 'private callback' not in r.guard.path.read_text()


@pytest.mark.asyncio
async def test_stop_during_original_send_cannot_publish_active_buttons(native):
    r = native
    digest = propose(r)
    entered, release = asyncio.Event(), asyncio.Event()
    original = r.bot.send_message.side_effect
    async def delayed(**kwargs):
        result = await original(**kwargs)
        entered.set()
        await release.wait()
        return result
    r.bot.send_message.side_effect = delayed
    sending = asyncio.create_task(r.runner._diggr_idle_tick())
    await asyncio.wait_for(entered.wait(), 5)
    r.guard.control(r.identity, 'paused')
    release.set()
    await sending
    assert proposal_row(r, digest)['preview']['state'] == 'revoked'
    r.bot.edit_message_reply_markup.assert_not_called()
    assert not batch_rows(r)


@pytest.mark.asyncio
async def test_started_coordinator_is_not_replayed_on_restart(native):
    r = native
    digest = await publish(r)
    await r.adapter._handle_callback_query(click(r, digest), None)
    await r.runner._diggr_idle_tick()
    assert await r.runner._diggr_accept(r.jobs[0])
    del r.runner._diggr_batch_runtime  # A new process cannot inherit this runtime-only owner.
    r.clock[0] += 31
    await r.runner._diggr_idle_tick()
    assert len(r.jobs) == 1
    batch = next(iter(batch_rows(r).values()))
    assert batch['coordinator_delivery']['status'] == 'uncertain'
    assert batch['started_at'] is None and batch['tasks'] == {}
    assert not await r.runner._diggr_accept(r.jobs[0])
    result = repeat_proposal(r, digest, 'confirmed')
    assert result['coordinator_status'] == 'uncertain' and result['admitted_tasks'] == 0
    assert 'keine automatische Wiederholung' in result['next_step']


@pytest.mark.asyncio
@pytest.mark.parametrize('change', ['reset', 'wrong_session', 'missing_parent', 'synthetic'])
async def test_batch_wake_cannot_cross_session_or_json_boundary(native, change):
    r = native
    digest = await publish(r)
    await r.adapter._handle_callback_query(click(r, digest), None)
    await r.runner._diggr_idle_tick()
    event = r.jobs[0]
    if change == 'reset': r.db.end_session(r.identity['session'], end_reason='session_reset')
    if change == 'wrong_session':
        r.entry.session_id = 'unrelated'
        r.db.create_session('unrelated', source='telegram')
    if change == 'missing_parent': r.db.delete_session(r.identity['session'])
    if change == 'synthetic':
        event = MessageEvent(text=event.text, source=r.source, internal=True)
        event._diggr_wake = json.loads(json.dumps(vars(r.jobs[0]._diggr_wake)))
    assert not await r.runner._diggr_accept(event)
    assert next(iter(batch_rows(r).values()))['tasks'] == {}


@pytest.mark.asyncio
async def test_reused_callback_id_cannot_approve_another_proposal(native):
    r = native
    first = await publish(r)
    await r.adapter._handle_callback_query(click(r, first, 'd', 'same-id'), None)
    second = await publish(r, manifest(r.task, number=2))
    await r.adapter._handle_callback_query(click(r, second, 'a', 'same-id'), None)
    assert not batch_rows(r)


@pytest.mark.asyncio
async def test_new_resource_prevalidation_does_not_revalidate_existing_grants(native, monkeypatch):
    r = native
    contract, _ = prepare(r, monkeypatch)
    proposal = manifest(r.task)
    proposal['todos'][0]['contract'] = contract
    invalid = copy.deepcopy(proposal)
    invalid['todos'][0]['contract']['resources']['artifacts'] = str(r.tmp / 'disjoint')
    with pytest.raises(ValueError, match='common possible subtree'):
        propose(r, invalid)
    with r.guard.transaction(read_only=True) as rows:
        assert not rows.grants.get('proposals')
    digest = await publish(r, proposal)
    await r.adapter._handle_callback_query(click(r, digest), None)
    before = r.guard.path.read_bytes()
    # A later stricter preview check must not retroactively veto/alter a confirmed grant.
    monkeypatch.setattr(owner, 'prevalidate_resources', Mock(side_effect=AssertionError('existing grant revalidation')))
    assert propose(r, proposal) == digest
    assert r.guard.path.read_bytes() == before
    bid = next(iter(batch_rows(r)))
    await r.runner._diggr_idle_tick()
    event = r.jobs[0]
    assert await r.runner._diggr_accept(event)
    tokens = await delivery.set_tool_context(r.runner, event,
        build_session_context(r.source, r.runner.config, r.entry), r.entry)
    try:
        r.task.update(owner_batch=bid, owner_issue=owner.issue_key(proposal['todos'][0]))
        assert dc.register_native(r.task)[1]['logical_contract'] == contract
    finally:
        r.runner._clear_session_env(tokens)


@pytest.mark.asyncio
async def test_legacy_proposal_is_not_automatically_adopted(native):
    r = native
    digest = owner.propose(r.guard, r.identity, manifest(r.task))
    assert propose(r) == digest
    await r.runner._diggr_idle_tick()
    assert not proposal_row(r, digest).get('preview')
    r.bot.send_message.assert_not_called()
    assert not batch_rows(r)


def text_event(r, text, message_id):
    from dataclasses import replace
    message = Message(message_id=message_id, date=datetime.fromtimestamp(r.clock[0], timezone.utc),
        chat=Chat(int(owner.OWNER), 'private'), from_user=User(int(owner.OWNER), 'Owner', False),
        text=text, message_thread_id=73)
    event = MessageEvent(text=text, source=replace(r.source, message_id=str(message_id)),
                         message_id=str(message_id), platform_update_id=message_id)
    owner.stamp_telegram(event, message, message_id)
    return event


@pytest.mark.asyncio
@pytest.mark.parametrize('native_proposal', [False, True])
async def test_legacy_text_p_a_b_retains_real_bound_route(native, native_proposal):
    r = native
    digest = propose(r) if native_proposal else owner.propose(r.guard, r.identity, manifest(r.task))
    await r.runner._diggr_accept(text_event(r, '/continuation show ' + digest, 202))
    shown = proposal_row(r, digest)['shown']
    await r.runner._diggr_accept(text_event(r, '/continuation confirm ' + digest + ' ' + shown['code'], 303))
    batch = next(iter(batch_rows(r).values()))
    binding = batch['native_transport']
    assert binding['bot_id'] == '123'
    assert binding['parent_session_id'] == r.entry.session_id
    assert binding['metadata']['telegram_reply_to_message_id'] == '202'
    await r.runner._diggr_idle_tick()
    event = r.jobs[0]
    assert event.source.message_id == '101'
    assert await r.runner._diggr_accept(event)
    tokens = await delivery.set_tool_context(r.runner, event,
        build_session_context(event.source, r.runner.config, r.entry), r.entry)
    try:
        from gateway.session_context import completion_launch_context
        assert completion_launch_context.get()[0] == binding
    finally:
        r.runner._clear_session_env(tokens)


def advance_while_waiting_for_write_lock(r, monkeypatch, expires):
    """Hold the real flock until the writer attempts it, then advance the clock."""
    import fcntl
    import threading
    from contextlib import contextmanager
    actual_transaction, actual_flock = dc.Guard.transaction, fcntl.flock
    armed = [True]

    @contextmanager
    def delayed_transaction(guard, *, read_only=False):
        if read_only or not armed[0]:
            with actual_transaction(guard, read_only=read_only) as rows:
                yield rows
            return
        armed[0] = False
        attempted = threading.Event()
        with open(str(guard.path) + '.lock', 'a') as blocker:
            actual_flock(blocker, fcntl.LOCK_EX)

            def release():
                assert attempted.wait(5)
                r.clock[0] = expires + 1
                actual_flock(blocker, fcntl.LOCK_UN)

            def waiting_flock(fd, mode):
                # Establish actual contention before allowing the holder to release.
                with pytest.raises(BlockingIOError):
                    actual_flock(fd, mode | fcntl.LOCK_NB)
                attempted.set()
                return actual_flock(fd, mode)

            thread = threading.Thread(target=release)
            thread.start()
            with monkeypatch.context() as local:
                local.setattr(fcntl, 'flock', waiting_flock)
                with actual_transaction(guard) as rows:
                    yield rows
            thread.join(5)
            assert not thread.is_alive()
    monkeypatch.setattr(dc.Guard, 'transaction', delayed_transaction)


@pytest.mark.asyncio
@pytest.mark.parametrize('path', ['callback', 'text'])
async def test_expiry_is_decided_after_waiting_for_guard_lock(native, monkeypatch, path):
    r = native
    digest = await publish(r)
    if path == 'text':
        # Same message anchor here isolates time-of-check from the P/A/B regression.
        await r.runner._diggr_accept(text_event(r, '/continuation show ' + digest, 101))
        preview = proposal_row(r, digest)['shown']
    else:
        preview = proposal_row(r, digest)['preview']
    r.clock[0] = preview['expires'] - 1
    advance_while_waiting_for_write_lock(r, monkeypatch, preview['expires'])
    if path == 'callback':
        await r.adapter._handle_callback_query(click(r, digest), None)
        assert proposal_row(r, digest)['preview']['state'] == 'expired'
    else:
        event = text_event(r, '/continuation confirm ' + digest + ' ' + preview['code'], 101)
        event.platform_update_id += 1
        event._diggr_native_input = owner.NativeInput(event.text, owner.OWNER, owner.OWNER, '101',
                                                     event.platform_update_id, '73')
        with pytest.raises(ValueError, match='expired') as error:
            await r.runner._diggr_accept(event)
        assert preview['code'] not in str(error.value)
    assert not batch_rows(r)


def handler_effects(r, monkeypatch):
    """Keep gateway admission/session/FIFO logic; replace model and network effects."""
    import gateway.run as run
    monkeypatch.setattr(run, '_hermes_home', r.home)
    r.runner.hooks = SimpleNamespace(emit=AsyncMock(), emit_collect=AsyncMock(return_value=[]))
    r.runner._persist_active_agents = Mock()
    r.runner._voice_mode = {}
    r.runner._run_agent = AsyncMock(return_value=dict(final_response='bounded answer', messages=[]))
    r.adapter.send_typing = AsyncMock()
    r.adapter._bot.send_chat_action = AsyncMock()


@pytest.mark.asyncio
@pytest.mark.parametrize('gate', ['pause', 'drain'])
async def test_batch_new_work_defers_at_real_handler_and_resumes(native, monkeypatch, gate):
    r = native
    handler_effects(r, monkeypatch)
    digest = await publish(r)
    await r.adapter._handle_callback_query(click(r, digest), None)
    await r.runner._diggr_idle_tick()
    event = r.jobs.pop()
    before = next(iter(batch_rows(r).values()))
    if gate == 'pause':
        (r.home / 'ESTOP').touch()
    else:
        r.runner._external_drain_active = True
    assert await r.runner._handle_message(event) is None
    r.runner._run_agent.assert_not_awaited()
    after = next(iter(batch_rows(r).values()))
    assert after['coordinator_delivery']['status'] in {'queued', 'pending'}
    assert after['coordinator_delivery']['deadline'] == before['coordinator_delivery']['deadline']
    assert 'accepted_at' not in after['coordinator_delivery']
    for _ in range(4):
        r.clock[0] += 31
        await r.runner._diggr_idle_tick()
    assert not r.jobs
    assert next(iter(batch_rows(r).values()))['coordinator_delivery']['attempts'] == before['coordinator_delivery']['attempts']
    if gate == 'pause':
        (r.home / 'ESTOP').unlink()
    else:
        r.runner._external_drain_active = False
    await r.runner._diggr_idle_tick()
    resumed = r.jobs.pop()
    assert await r.runner._handle_message(resumed) is None
    r.runner._run_agent.assert_awaited_once()
    assert next(iter(batch_rows(r).values()))['coordinator_delivery']['status'] == 'response_produced'


@pytest.mark.asyncio
@pytest.mark.parametrize('admit_task', [False, True])
async def test_outer_hook_keeps_native_parent_after_real_cleanup_and_compression(native, monkeypatch, admit_task):
    from hermes_cli.goals import GoalManager
    from gateway.session_context import completion_launch_context
    r = native
    handler_effects(r, monkeypatch)
    digest = await publish(r)
    await r.adapter._handle_callback_query(click(r, digest), None)
    bid = next(iter(batch_rows(r)))
    await r.runner._diggr_idle_tick()
    event = r.jobs.pop()
    parent = r.identity['session']
    child = 'compressed-native-child'
    GoalManager(child).set('unrelated standing goal')
    judge = Mock(return_value=('continue', 'needs another turn', False, None, False))
    monkeypatch.setattr('hermes_cli.goals.judge_goal', judge)

    async def model(**kwargs):
        assert dc.EVENT_CONTEXT.get()['identity'] == r.identity
        assert completion_launch_context.get()[0]['parent_session_id'] == parent
        if admit_task:
            r.task.update(owner_batch=bid, owner_issue=owner.issue_key(manifest(r.task)['todos'][0]))
            dc.register_native(r.task)
        r.db.end_session(parent, end_reason='compression')
        r.db.create_session(child, source='telegram', parent_session_id=parent)
        return dict(final_response='bounded answer', messages=[], session_id=child)

    r.runner._run_agent.side_effect = model
    actual_hook = r.runner._post_turn_goal_continuation
    reached = []

    async def check_cleanup(**kwargs):
        assert dc.EVENT_CONTEXT.get() is None
        assert completion_launch_context.get() is None
        assert kwargs['session_entry'].session_id == child
        reached.append(True)
        await actual_hook(**kwargs)

    r.runner._post_turn_goal_continuation = check_cleanup
    assert await r.runner._handle_message(event) is None
    assert reached == [True]
    assert r.runner._run_agent.await_count == 1
    judge.assert_not_called()
    assert r.entry.session_id == child
    restored = SessionStore(r.home / 'sessions', r.runner.config)
    restored._ensure_loaded()
    assert restored._entries[r.identity['session_key']].session_id == child
    batch = batch_rows(r)[bid]
    assert batch['coordinator_identity']['session'] == parent
    assert batch['deliveries'][0]['content'] == 'bounded answer'
    assert batch['deliveries'][0]['status'] == 'delivered'
    assert not any(e.text.startswith('[Continuing toward') for e in r.adapter._pending_messages.values())


@pytest.mark.asyncio
async def test_batch_behind_q1_q2_drains_real_adapter_and_agent_fifo_once(native, monkeypatch):
    import gateway.run as run
    r = native
    handler_effects(r, monkeypatch)
    del r.runner._run_agent  # Exercise the real recursive drain as well as the adapter consumer.
    digest = await publish(r)
    await r.adapter._handle_callback_query(click(r, digest), None)
    key = r.identity['session_key']
    q1, q2 = (text_event(r, name, n) for name, n in [('Q1', 202), ('Q2', 303)])
    r.runner._enqueue_fifo(key, q1, r.adapter)
    r.runner._enqueue_fifo(key, q2, r.adapter)
    r.adapter._active_sessions[key] = asyncio.Event()
    await r.runner._diggr_idle_tick()
    assert r.adapter._pending_messages[key] is q1
    overflow = r.runner._peek_session_state(key).conversation.queued_events
    assert overflow[0] is q2
    native_event = overflow[1]
    assert type(native_event._diggr_wake) is delivery.NativeWake
    model_turns, handler_events = [], []

    def model(turn):
        ctx = turn._ctx
        native_wake = (dc.EVENT_CONTEXT.get() or {}).get('native_wake')
        if ctx.message.startswith(dc.PREFIX):
            assert native_wake is native_event._diggr_wake
            assert next(iter(batch_rows(r).values()))['coordinator_delivery']['status'] == 'accepted'
            assert delivery.native_turn()
            model_turns.append('batch')
        else:
            assert native_wake is None
            model_turns.append(ctx.message)
        result = dict(final_response='answer ' + model_turns[-1], messages=[],
                      pending_steer='MUST NOT RUN' if native_wake else None)
        ctx.result_holder[0] = result
        return result

    monkeypatch.setattr(run.TurnRunner, 'run_sync', model)
    finished = asyncio.Event()

    async def handle(event):
        handler_events.append(event)
        answer = await r.runner._handle_message(event)
        if event is native_event:
            finished.set()
        return answer

    r.adapter.set_message_handler(handle)
    del r.adapter._start_session_processing
    r.adapter._active_sessions.pop(key)
    r.adapter._start_session_processing(text_event(r, 'Q0', 404), key)
    await asyncio.wait_for(finished.wait(), 10)
    while r.adapter._background_tasks:
        await asyncio.wait_for(asyncio.gather(*list(r.adapter._background_tasks)), 10)
    assert model_turns == ['Q0', 'Q1', 'Q2', 'batch']
    assert handler_events[-1] is native_event
    assert len(handler_events) == 2  # Q1/Q2 use the existing in-band agent drain.
    assert not r.runner._queue_depth(key, adapter=r.adapter)
    assert key not in r.adapter._active_sessions
    batch = next(iter(batch_rows(r).values()))
    assert batch['coordinator_delivery']['status'] == 'response_produced'
    assert batch['deliveries'][0]['content'] == 'answer batch'
    assert batch['deliveries'][0]['status'] == 'delivered'
    assert not await r.runner._diggr_accept(native_event)
    await r.runner._diggr_idle_tick()
    assert model_turns == ['Q0', 'Q1', 'Q2', 'batch']


@pytest.mark.asyncio
@pytest.mark.parametrize('during', ['stop', 'expiry', 'refresh'])
async def test_stale_preview_edit_cannot_apply_new_revision_or_authority(native, during):
    r = native
    digest = propose(r)
    entered, release = asyncio.Event(), asyncio.Event()
    async def delayed(**kwargs):
        entered.set()
        await release.wait()
        return True
    r.bot.edit_message_text.side_effect = delayed
    sending = asyncio.create_task(r.runner._diggr_idle_tick())
    await asyncio.wait_for(entered.wait(), 5)
    stale_click = click(r, digest)
    initial = proposal_row(r, digest)['preview']
    if during == 'stop':
        r.guard.control(r.identity, 'paused')
    else:
        r.clock[0] = initial['expires'] + 1
        if during == 'refresh':
            # Decision only: the already-running UI lease owns its edit.
            await owner.handle_callback(r.runner, owner.telegram_callback(r.adapter, click(r, digest, 'r')))
    release.set()
    await sending
    pv = proposal_row(r, digest)['preview']
    assert pv['ui_applied'] != pv['ui_revision']
    assert not batch_rows(r)
    await r.adapter._handle_callback_query(stale_click, None)
    assert not batch_rows(r)
    r.bot.edit_message_text.side_effect = None
    await r.runner._diggr_idle_tick()
    pv = proposal_row(r, digest)['preview']
    assert pv['ui_applied'] == pv['ui_revision']
    assert pv['state'] == {'stop': 'revoked', 'expiry': 'expired', 'refresh': 'open'}[during]
    if during == 'refresh':
        await r.adapter._handle_callback_query(click(r, digest, query_id='fresh-after-edit'), None)
        assert len(batch_rows(r)) == 1


@pytest.mark.asyncio
async def test_idempotent_ptb_edits_and_preview_delivery_deadline(native):
    from telegram.error import BadRequest
    r = native
    r.bot.edit_message_text.side_effect = BadRequest('Message is not modified')
    r.bot.edit_message_reply_markup.side_effect = BadRequest('Message is not modified')
    digest = await publish(r)
    pv = proposal_row(r, digest)['preview']
    assert pv['ui_applied'] == pv['ui_revision']
    assert pv['ui_attempts'] == 1
    for _ in range(3):
        await r.runner._diggr_idle_tick()
    assert r.bot.edit_message_text.call_count == r.bot.edit_message_reply_markup.call_count == 1
    second = propose(r, manifest(r.task, number=2))
    r.runner._profile_adapters = {}
    r.clock[0] += delivery.DELIVERY_SECONDS + 1
    await r.runner._diggr_idle_tick()
    assert proposal_row(r, second)['preview']['send'] == 'failed'
    r.runner._profile_adapters = {'diggr-main': {Platform.TELEGRAM: r.adapter}}
    await r.runner._diggr_idle_tick()
    assert r.bot.send_message.call_count == 1
    assert not batch_rows(r)


@pytest.mark.asyncio
async def test_queued_restart_keeps_deadline_and_old_generation_cannot_start(native):
    r = native
    digest = await publish(r)
    await r.adapter._handle_callback_query(click(r, digest), None)
    r.guard = dc.Guard(r.guard.path)
    r.runner.session_store = SessionStore(r.home / 'sessions', r.runner.config)
    await r.runner._diggr_idle_tick()
    old = r.jobs.pop()
    before = next(iter(batch_rows(r).values()))['coordinator_delivery']
    r.runner.session_store = SessionStore(r.home / 'sessions', r.runner.config)
    r.clock[0] += 31
    await r.runner._diggr_idle_tick()
    fresh = r.jobs.pop()
    after = next(iter(batch_rows(r).values()))['coordinator_delivery']
    assert after['deadline'] == before['deadline']
    assert after['attempts'] == before['attempts'] + 1
    assert not await r.runner._diggr_accept(old)
    assert await r.runner._diggr_accept(fresh)
    assert not await r.runner._diggr_accept(fresh)


@pytest.mark.asyncio
@pytest.mark.parametrize('missing', ['guard', 'text', 'both'])
async def test_typed_batch_never_becomes_an_ordinary_event(native, missing):
    r = native
    digest = await publish(r)
    await r.adapter._handle_callback_query(click(r, digest), None)
    await r.runner._diggr_idle_tick()
    event = r.jobs.pop()
    if missing in {'guard', 'both'}:
        (r.home / 'config.yaml').write_text('diggr_continuation:\n  enabled: false\n')
    if missing in {'text', 'both'}:
        event.text = 'unbound ordinary text'
    assert not await r.runner._diggr_accept(event)


@pytest.mark.asyncio
async def test_batch_finish_durable_exact_duplicate_conflict_and_restart_delivery(native):
    r = native
    digest = await publish(r)
    await r.adapter._handle_callback_query(click(r, digest), None)
    await r.runner._diggr_idle_tick()
    event = r.jobs.pop()
    assert await r.runner._diggr_accept(event)
    r.bot.send_message.reset_mock()
    assert await r.runner._diggr_finish(event, {'final_response': 'exact native answer'})
    r.bot.send_message.assert_not_called()
    result = repeat_proposal(r, digest, 'confirmed')
    assert result['coordinator_status'] == 'response_produced'
    before = r.guard.path.read_bytes()
    assert await r.runner._diggr_finish(event, 'exact native answer')
    assert r.guard.path.read_bytes() == before
    with pytest.raises(ValueError, match='conflicting'):
        await r.runner._diggr_finish(event, 'different answer')
    assert r.guard.path.read_bytes() == before
    del r.runner._diggr_batch_runtime
    r.runner.session_store = SessionStore(r.home / 'sessions', r.runner.config)
    await r.runner._diggr_idle_tick()
    assert r.bot.send_message.call_count == 1
    assert r.bot.send_message.call_args.kwargs['text'] == 'exact native answer'
    assert r.bot.send_message.call_args.kwargs['message_thread_id'] == 73
    await r.runner._diggr_idle_tick()
    assert r.bot.send_message.call_count == 1


@pytest.mark.asyncio
async def test_existing_task_wake_outer_cleanup_retains_compression_parent(native, monkeypatch):
    from tests.gateway.test_diggr_origin_delivery import result
    from hermes_cli.goals import GoalManager
    r = native
    handler_effects(r, monkeypatch)
    digest = await publish(r)
    await r.adapter._handle_callback_query(click(r, digest), None)
    await r.runner._diggr_idle_tick()
    batch_event = r.jobs.pop()
    assert await r.runner._diggr_accept(batch_event)
    tokens = await delivery.set_tool_context(r.runner, batch_event,
        build_session_context(r.source, r.runner.config, r.entry), r.entry)
    try:
        r.task.update(owner_batch=batch_event._diggr_wake.batch,
                      owner_issue=owner.issue_key(manifest(r.task)['todos'][0]))
        dc.register_native(r.task)
    finally:
        r.runner._clear_session_env(tokens)
    await r.runner._diggr_finish(batch_event, 'task registered')
    result(r)
    await r.runner._diggr_idle_tick()
    event = r.jobs.pop()
    assert event._diggr_wake.task == r.task['task'] and not event._diggr_wake.batch
    GoalManager('task-child').set('unrelated goal')
    judge = Mock()
    monkeypatch.setattr('hermes_cli.goals.judge_goal', judge)
    async def model(**kwargs):
        assert dc.EVENT_CONTEXT.get()['identity'] == r.identity
        r.db.end_session(r.identity['session'], end_reason='compression')
        r.db.create_session('task-child', source='telegram', parent_session_id=r.identity['session'])
        return dict(final_response='task coordinator answer', messages=[], session_id='task-child')
    r.runner._run_agent.side_effect = model
    assert await r.runner._handle_message(event) is None
    assert dc.EVENT_CONTEXT.get() is None
    judge.assert_not_called()
    replies = [d for d in r.guard.get(r.task['task'])['deliveries'] if d['phase'] == 'coordinator_reply']
    assert len(replies) == 1 and replies[0]['content'] == 'task coordinator answer'
    assert replies[0]['status'] == 'delivered'


@pytest.mark.asyncio
@pytest.mark.parametrize('boundary', ['accept', 'tools'])
@pytest.mark.parametrize('change', ['stop', 'expiry', 'bot'])
async def test_batch_rechecks_authority_after_real_async_lineage(native, monkeypatch, boundary, change):
    from gateway.session_context import completion_launch_context
    r = native
    digest = await publish(r)
    await r.adapter._handle_callback_query(click(r, digest), None)
    await r.runner._diggr_idle_tick()
    event = r.jobs.pop()
    if boundary == 'tools':
        assert await r.runner._diggr_accept(event)
    actual = delivery.native_session_matches
    async def delayed(*args):
        matched = await actual(*args)
        assert matched
        if change == 'stop':
            r.guard.control(r.identity, 'paused')
        elif change == 'expiry':
            r.clock[0] = next(iter(batch_rows(r).values()))['coordinator_delivery']['deadline']
        else:
            r.bot.id = 456
        return matched
    monkeypatch.setattr(delivery, 'native_session_matches', delayed)
    if boundary == 'accept':
        assert not await r.runner._diggr_accept(event)
        assert next(iter(batch_rows(r).values()))['coordinator_delivery']['status'] != 'accepted'
    else:
        with pytest.raises(ValueError, match='bound origin'):
            await delivery.set_tool_context(r.runner, event,
                build_session_context(r.source, r.runner.config, r.entry), r.entry)
        assert dc.EVENT_CONTEXT.get() is None
        assert completion_launch_context.get() is None
    assert next(iter(batch_rows(r).values()))['tasks'] == {}


@pytest.mark.asyncio
async def test_batch_deadline_expires_while_paused_without_renewed_budget(native, monkeypatch):
    r = native
    handler_effects(r, monkeypatch)
    digest = await publish(r)
    await r.adapter._handle_callback_query(click(r, digest), None)
    await r.runner._diggr_idle_tick()
    event = r.jobs.pop()
    before = next(iter(batch_rows(r).values()))['coordinator_delivery']
    (r.home / 'ESTOP').touch()
    assert await r.runner._handle_message(event) is None
    r.clock[0] = before['deadline']
    await r.runner._diggr_idle_tick()
    (r.home / 'ESTOP').unlink()
    await r.runner._diggr_idle_tick()
    assert not await r.runner._diggr_accept(event)
    after = next(iter(batch_rows(r).values()))['coordinator_delivery']
    assert after['status'] == 'suppressed'
    assert after['deadline'] == before['deadline'] and after['attempts'] == before['attempts']
    result = repeat_proposal(r, digest, 'confirmed')
    assert result['coordinator_status'] == 'suppressed'
    assert 'keine automatische Wiederholung' in result['next_step']
    r.runner._run_agent.assert_not_awaited()
    assert not r.jobs


@pytest.mark.asyncio
async def test_callback_write_failure_rolls_back_grant_and_ui_without_error_details(native, monkeypatch, caplog):
    r = native
    digest = await publish(r)
    before = r.guard.path.read_bytes()
    actual = dc.os.replace
    def failed_write(src, dst):
        if str(dst) == str(r.guard.path):
            raise OSError('dummy-private-error')
        return actual(src, dst)
    update = click(r, digest)
    with monkeypatch.context() as local:
        local.setattr(dc.os, 'replace', failed_write)
        await r.adapter._handle_callback_query(update, None)
    assert not batch_rows(r)
    assert r.guard.path.read_bytes() == before
    assert proposal_row(r, digest)['preview']['state'] == 'open'
    assert 'dummy-private-error' not in caplog.text
    assert 'dummy-private-error' not in str(r.bot.answer_callback_query.call_args_list)
    await r.adapter._handle_callback_query(update, None)
    assert len(batch_rows(r)) == 1


@pytest.mark.asyncio
async def test_stop_during_main_cannot_fall_back_to_unbound_answer(native, monkeypatch):
    r = native
    handler_effects(r, monkeypatch)
    digest = await publish(r)
    await r.adapter._handle_callback_query(click(r, digest), None)
    await r.runner._diggr_idle_tick()
    event = r.jobs.pop()
    async def model(**kwargs):
        r.guard.control(r.identity, 'paused')
        return dict(final_response='stale unbound answer', messages=[])
    r.runner._run_agent.side_effect = model
    with pytest.raises(ValueError, match='bound origin'):
        await r.runner._handle_message(event)
    assert dc.EVENT_CONTEXT.get() is None
    assert not any('stale unbound answer' in msg.text for msg in r.messages)
    assert next(iter(batch_rows(r).values()))['coordinator_delivery']['status'] == 'suppressed'


@pytest.mark.asyncio
@pytest.mark.parametrize('change', ['bot', 'topic', 'profile', 'session'])
async def test_legacy_text_drift_exception_does_not_allow_route_substitution(native, change):
    from dataclasses import replace
    r = native
    digest = propose(r)
    await r.runner._diggr_accept(text_event(r, '/continuation show ' + digest, 202))
    shown = proposal_row(r, digest)['shown']
    event = text_event(r, '/continuation confirm ' + digest + ' ' + shown['code'], 303)
    if change == 'bot':
        r.bot.id = 456
    elif change == 'session':
        r.db.create_session('new-session', source='telegram')
        await r.runner.async_session_store.switch_session(r.identity['session_key'], 'new-session')
    else:
        event.source = replace(event.source, **({'thread_id': '74'} if change == 'topic' else {'profile': 'mira'}))
    with pytest.raises(ValueError) as error:
        await r.runner._diggr_accept(event)
    assert shown['code'] not in str(error.value)
    assert not batch_rows(r)


@pytest.mark.asyncio
async def test_f2_proposal_allows_canonical_packets_and_worktrees_within_system_repo(native, monkeypatch):
    from pathlib import Path
    r = native
    contract, _ = prepare(r, monkeypatch)
    system_repo = Path(contract['resources']['artifacts']).parents[2]
    contract['resources'].update(repo=str(system_repo), worktrees=str(system_repo / 'worktrees'))
    proposal = manifest(r.task)
    proposal['todos'][0]['contract'] = contract
    digest = propose(r, proposal)
    assert proposal_row(r, digest)['manifest'] == proposal
    assert not batch_rows(r)


@pytest.mark.asyncio
async def test_native_batch_real_turn_runner_disables_streams_and_propagates_tool_context(native, monkeypatch):
    import run_agent
    from gateway.session_context import completion_launch_context
    r = native
    handler_effects(r, monkeypatch)
    del r.runner._run_agent
    r.runner._provider_routing = {}
    r.runner._prefill_messages = []
    r.runner._ephemeral_system_prompt = ''
    r.runner._reasoning_config = {'effort': 'ultra'}
    r.runner.hooks.loaded_hooks = []
    # Only provider authentication and the expensive model implementation are replaced.
    r.runner._resolve_session_agent_runtime = lambda **kwargs: ('gpt-6-astra', {'provider': 'openai'})
    r.runner.config.streaming.enabled = True
    digest = await publish(r)
    await r.adapter._handle_callback_query(click(r, digest), None)
    await r.runner._diggr_idle_tick()
    event = r.jobs.pop()
    calls = []

    class OfflineAgent:
        def __init__(self, **kwargs):
            assert kwargs['model'] == 'gpt-6-astra' and kwargs['provider'] == 'openai'
            assert kwargs['fallback_model'] is None
            self.session_id = kwargs['session_id']
            self.tools = []

        def run_conversation(self, message, **kwargs):
            assert self.stream_delta_callback is None
            assert self.interim_assistant_callback is None
            assert dc.EVENT_CONTEXT.get()['native_wake'] is event._diggr_wake
            assert dc.EVENT_CONTEXT.get()['identity'] == r.identity
            assert completion_launch_context.get()[0]['metadata']['telegram_reply_to_message_id'] == '101'
            assert not any(m.text == 'only durable answer' for m in r.messages)
            calls.append(message)
            return dict(final_response='only durable answer', messages=[], pending_steer='must not run')

    monkeypatch.setattr(run_agent, 'AIAgent', OfflineAgent)
    assert await r.runner._handle_message(event) is None
    assert len(calls) == 1 and calls[0].startswith(dc.PREFIX)
    replies = next(iter(batch_rows(r).values()))['deliveries']
    assert len(replies) == 1 and replies[0]['content'] == 'only durable answer'
    assert replies[0]['status'] == 'delivered'
    assert sum(m.text == 'only durable answer' for m in r.messages) == 1
    assert dc.EVENT_CONTEXT.get() is None
