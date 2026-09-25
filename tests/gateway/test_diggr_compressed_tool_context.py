"""Real gateway tool-context setup after native wake compression."""
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from gateway.session import build_session_context
from gateway.session_context import completion_launch_context
from hermes_cli import diggr_continuation as dc, diggr_delivery as delivery
from hermes_state import SessionDB, AsyncSessionDB
from tests.gateway.test_diggr_origin_delivery import setup, result


@pytest.mark.asyncio
@pytest.mark.parametrize('lineage,admission', [('compression', 'replay'), ('compression', 'new_todo'),
    ('reset', 'replay'), ('unrelated', 'replay'), ('missing', 'replay')])
async def test_compressed_native_tool_admission(tmp_path, monkeypatch, lineage, admission):
    from tests.gateway import test_diggr_origin_delivery as fixtures
    from hermes_cli import diggr_owner as owner
    original_manifest = fixtures.manifest
    def two_todos(task):
        proposal = original_manifest(task)
        second_task = dict(task, task='second-task', artifact=str(tmp_path / 'second-result'))
        second = original_manifest(second_task, 2)['todos'][0]
        second['depends_on'] = [owner.issue_key(proposal['todos'][0])]
        proposal['todos'].append(second)
        return proposal
    if admission == 'new_todo':
        monkeypatch.setattr(fixtures, 'manifest', two_todos)
    r = await setup(tmp_path, monkeypatch)
    result(r)
    jobs = []
    r.adapter.get_pending_message = lambda key: r.adapter._pending_messages.pop(key, None)
    r.adapter._start_session_processing = lambda event, key: jobs.append(event)
    await r.runner._diggr_tick(r.source, r.identity['session'], idle=True)
    event = jobs[0]
    # Accept while the original session is still current, then compress before tools.
    assert await r.runner._diggr_accept(event)
    db = SessionDB(db_path=tmp_path / 'lineage.db')
    r.runner._session_db = AsyncSessionDB(db)
    parent = r.identity['session']
    db.create_session(parent, source='telegram')
    db.end_session(parent, end_reason='session_reset' if lineage == 'reset' else 'compression')
    if lineage != 'missing':
        db.create_session('child', source='telegram', parent_session_id=None if lineage == 'unrelated' else parent)
    entry = SimpleNamespace(session_id='child', session_key=r.identity['session_key'], created_at=1000, updated_at=1000)
    context = build_session_context(r.source, r.runner.config, entry)
    before = r.guard.path.read_bytes()
    try:
        if lineage != 'compression':
            with pytest.raises(ValueError, match='lineage'):
                await delivery.set_tool_context(r.runner, event, context, entry)
        else:
            tokens = await delivery.set_tool_context(r.runner, event, context, entry)
            try:
                _, replay = dc.register_native(r.task)
                assert replay['_native_admission_replayed']
                assert dc.EVENT_CONTEXT.get()['identity'] == r.identity
                assert completion_launch_context.get()[0] == r.guard.get(r.task['task'])['native_transport']
                assert context.session_id == 'child'
                assert r.guard.path.read_bytes() == before
                if admission == 'new_todo':
                    row = r.guard.get(r.task['task'])
                    evidence = {k: row[k] for k in ('task', 'generation', 'action', 'artifact')}
                    evidence.update(sha256=dc.digest(row['artifact']), result='validated')
                    assert r.guard.ack(r.identity, row, evidence, 'main_live', 'validate')
                    r.clock[0] += 2
                    wake = r.guard.tick(r.identity)
                    assert r.guard.begin(r.identity, wake)
                    row = r.guard.get(r.task['task'])
                    final = tmp_path / 'final.json'
                    final.write_text(json.dumps(dict(task=row['task'], generation=row['generation'],
                        action=row['action'], authorization=row['authorization'], result='passed', checks=['fixture'])))
                    evidence = {k: row[k] for k in ('task', 'generation', 'action', 'artifact')}
                    evidence.update(sha256=dc.digest(row['artifact']), result='validated',
                        authorization=row['authorization'], final_gate='passed', final_artifact=str(final),
                        final_sha256=dc.digest(final))
                    assert r.guard.ack(r.identity, row, evidence, 'done', 'complete')
                    second = dict(r.task, task='second-task', artifact=str(tmp_path / 'second-result'),
                        owner_issue=owner.issue_key(two_todos(r.task)['todos'][1]))
                    _, admitted = dc.register_native(second)
                    assert admitted['origin']['identity'] == r.identity
                    assert admitted['native_transport'] == replay['native_transport']
                    assert admitted['owner_batch'] == r.task['owner_batch']
            finally:
                r.runner._clear_session_env(tokens)
        if lineage == 'compression' and admission == 'replay':
            # Drive the actual handler to this boundary as well, so a detached
            # helper test cannot hide omission from the real agent/tool setup.
            monkeypatch.setattr(r.runner.async_session_store, 'get_or_create_session', AsyncMock(return_value=entry))
            r.runner._cache_session_source = lambda *args: None
            r.runner.hooks = SimpleNamespace(emit=AsyncMock())
            actual_setup = delivery.set_tool_context
            class ContextReached(Exception):
                pass
            async def checked_setup(*args):
                tokens = await actual_setup(*args)
                try:
                    assert dc.register_native(r.task)[1]['_native_admission_replayed']
                finally:
                    r.runner._clear_session_env(tokens)
                raise ContextReached()
            monkeypatch.setattr(delivery, 'set_tool_context', checked_setup)
            with pytest.raises(ContextReached):
                await r.runner._handle_message_with_agent(event, event.source, 'synthetic', 1)
        if admission != 'new_todo':
            assert r.guard.path.read_bytes() == before
    finally:
        db.close()


@pytest.mark.asyncio
async def test_ordinary_compressed_turn_uses_its_current_session(tmp_path, monkeypatch):
    r = await setup(tmp_path, monkeypatch)
    entry = SimpleNamespace(session_id='child', session_key=r.identity['session_key'], created_at=1000, updated_at=1000)
    context = build_session_context(r.source, r.runner.config, entry)
    tokens = await delivery.set_tool_context(r.runner, SimpleNamespace(source=r.source), context, entry)
    try:
        assert dc.EVENT_CONTEXT.get()['identity']['session'] == 'child'
        assert completion_launch_context.get()[0]['parent_session_id'] == 'child'
        with pytest.raises(ValueError, match='coordinator identity'):
            dc.register_native(r.task)
    finally:
        r.runner._clear_session_env(tokens)


@pytest.mark.asyncio
@pytest.mark.parametrize('field', ['bot_id', 'chat_id', 'profile_home', 'session_key',
    'metadata.thread_id', 'metadata.direct_messages_topic_id', 'metadata.telegram_dm_topic_reply_fallback'])
async def test_native_tool_context_refuses_transport_substitution(tmp_path, monkeypatch, field):
    r = await setup(tmp_path, monkeypatch, topic='73')
    result(r)
    jobs = []
    r.adapter.get_pending_message = lambda key: r.adapter._pending_messages.pop(key, None)
    r.adapter._start_session_processing = lambda event, key: jobs.append(event)
    await r.runner._diggr_tick(r.source, r.identity['session'], idle=True)
    event = jobs[0]
    assert await r.runner._diggr_accept(event)
    entry = await r.runner.async_session_store.get_or_create_session(r.source)
    context = build_session_context(r.source, r.runner.config, entry)
    actual_setup = r.runner._set_session_env
    def changed_transport(context):
        tokens = actual_setup(context)
        binding, schedule = completion_launch_context.get()
        if field.startswith('metadata.'):
            changed = dict(binding, metadata=dict(binding['metadata'], **{field.split('.', 1)[1]: 'foreign'}))
        else:
            changed = dict(binding, **{field: 'foreign'})
        completion_launch_context.set((changed, schedule))
        return tokens
    monkeypatch.setattr(r.runner, '_set_session_env', changed_transport)
    before = r.guard.path.read_bytes()
    with pytest.raises(ValueError, match='transport changed'):
        await delivery.set_tool_context(r.runner, event, context, entry)
    assert dc.EVENT_CONTEXT.get() is None
    assert completion_launch_context.get() is None
    assert r.guard.path.read_bytes() == before


@pytest.mark.asyncio
@pytest.mark.parametrize('compressed', [False, True])
@pytest.mark.parametrize('topic', ['1', '73'])
async def test_message_b_admission_wake_keeps_reply_anchor_with_message_a_origin(tmp_path, monkeypatch, compressed, topic):
    from dataclasses import replace
    from tests.gateway import test_diggr_origin_delivery as fixtures
    source_type = fixtures.SessionSource
    monkeypatch.setattr(fixtures, 'SessionSource', lambda **kwargs: source_type(**kwargs, message_id='101'))
    r = await setup(tmp_path, monkeypatch, topic=topic, register=False)
    db = SessionDB(db_path=tmp_path / 'message-lineage.db')
    r.runner._session_db = AsyncSessionDB(db)
    parent = r.identity['session']
    db.create_session(parent, source='telegram')
    message_b = replace(r.source, message_id='202')
    entry = await r.runner.async_session_store.get_or_create_session(message_b)
    assert entry.origin.message_id == '101'
    tokens = await delivery.set_tool_context(r.runner, SimpleNamespace(source=message_b),
        build_session_context(message_b, r.runner.config, entry), entry)
    try:
        dc.register_native(r.task)
    finally:
        r.runner._clear_session_env(tokens)
    original = r.guard.get(r.task['task'])['native_transport']
    assert original['metadata']['telegram_reply_to_message_id'] == '202'
    result(r)
    try:
        if compressed:
            db.end_session(parent, end_reason='compression')
            db.create_session('child', source='telegram', parent_session_id=parent)
            await r.runner.async_session_store.switch_session(r.identity['session_key'], 'child')
        jobs = []
        r.adapter.get_pending_message = lambda key: r.adapter._pending_messages.pop(key, None)
        r.adapter._start_session_processing = lambda event, key: jobs.append(event)
        await r.runner._diggr_tick(r.source, parent, idle=True)
        assert len(jobs) == 1
        event = jobs[0]
        assert event.source.message_id == '101'  # Actual route_scope retained session origin A.
        assert await r.runner._diggr_accept(event)
        entry = await r.runner.async_session_store.get_or_create_session(event.source)
        context = build_session_context(event.source, r.runner.config, entry)
        before = r.guard.path.read_bytes()
        tokens = await delivery.set_tool_context(r.runner, event, context, entry)
        try:
            binding = completion_launch_context.get()[0]
            assert binding == original
            assert binding['metadata']['telegram_reply_to_message_id'] == '202'
            assert binding['thread_id'] == topic
            assert dc.register_native(r.task)[1]['_native_admission_replayed']
            assert r.guard.path.read_bytes() == before
        finally:
            r.runner._clear_session_env(tokens)
        # Reject genuine source rerouting, not merely injected binding dictionaries.
        for field, value in [('profile', 'foreign'), ('thread_id', '999'), ('chat_id', 'foreign'), ('user_id', 'foreign')]:
            forged_source = replace(event.source, **{field: value})
            bad_event = SimpleNamespace(source=forged_source, _diggr_wake=event._diggr_wake)
            with pytest.raises(ValueError):
                await delivery.set_tool_context(r.runner, bad_event,
                    build_session_context(forged_source, r.runner.config, entry), entry)
            assert r.guard.path.read_bytes() == before
    finally:
        db.close()
