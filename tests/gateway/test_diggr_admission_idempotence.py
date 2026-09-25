"""Native retries preserve admission, budgets and one-shot launcher/send ownership."""
import copy
import json
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import Mock
import pytest
from gateway.config import Platform
from gateway.session import SessionContext
from hermes_cli import diggr_continuation as dc
from tests.gateway.test_diggr_origin_delivery import setup

@contextmanager
def native_context(r):
    token = dc.EVENT_CONTEXT.set({'identity': r.identity})
    tokens = r.runner._set_session_env(SessionContext(source=r.source,
        connected_platforms=[Platform.TELEGRAM], home_channels={},
        session_key=r.identity['session_key'], session_id=r.identity['session']))
    try: yield
    finally:
        r.runner._clear_session_env(tokens)
        dc.EVENT_CONTEXT.reset(token)

async def fixture(tmp_path, monkeypatch):
    packet = tmp_path / 'packet.json'
    packet.write_text(json.dumps(dict(operator_scope_authorized=True, plane_id='synthetic',
                                     coding_route={'required_model': 'fixture'})))
    route = dict(argv=['codex', 'exec', '--model', 'fixture'], worktree=str(tmp_path),
        receipt=str(tmp_path / 'receipt.json'), packet=str(packet), plane_id='synthetic',
        branch='fixture', visible_target={'workspace': 'fixture', 'surface': 'fixture'})
    binding = dict(target=route['visible_target'], tty='fixture')
    monkeypatch.setattr(dc, 'bind_visible_target', lambda target: binding)
    monkeypatch.setattr(dc, 'bound_shell_identity', lambda binding: {'pid': 101})
    monkeypatch.setattr(dc, 'verify_visible_target', lambda *a, **kw: None)
    monkeypatch.setattr(dc, 'observe_processes', lambda guard: None)
    monkeypatch.setattr(dc, 'launcher_identity', lambda session: {'pid': 202})
    r = await setup(tmp_path, monkeypatch, register=False, task_overrides=dict(
        producer='cmux', worker_route=route, launcher_command='synthetic-launcher'))
    from tools.process_registry import process_registry
    session = SimpleNamespace(session_key=r.identity['session_key'], started_at=1000, pid=202)
    monkeypatch.setattr(process_registry, 'get', lambda key: session if key == 'launcher' else None)
    r.args = dict(command='synthetic-launcher', background=True, continuation=r.task)
    return r

def response(value): return json.loads(value) if isinstance(value, str) else value

def forbidden(_): pytest.fail('duplicate external effect')

@pytest.mark.asyncio
async def test_reentrant_reload_and_budget_preservation(tmp_path, monkeypatch):
    r = await fixture(tmp_path, monkeypatch)
    reentrant = []
    def launch(args):
        reentrant.append(response(dc.terminal_dispatch(r.args, forbidden)))
        return {'session_id': 'launcher'}
    with native_context(r):
        dc.terminal_dispatch(r.args, launch)
        before = r.guard.path.read_bytes()
        r.clock[0] += 10
        replay = response(dc.terminal_dispatch(r.args, forbidden))
        assert r.guard.path.read_bytes() == before
    assert reentrant[0]['launcher_expected'] is True
    assert replay['session_id'] == 'launcher' and replay['already_registered'] is True

@pytest.mark.asyncio
async def test_uncertain_launcher_never_replayed(tmp_path, monkeypatch):
    r = await fixture(tmp_path, monkeypatch)
    with native_context(r):
        with pytest.raises(RuntimeError):
            dc.terminal_dispatch(r.args, Mock(side_effect=RuntimeError('uncertain start')))
        before = r.guard.path.read_bytes()
        replay = response(dc.terminal_dispatch(r.args, forbidden))
        assert r.guard.path.read_bytes() == before
    assert replay['effect_status'] == 'unknown' and replay['session_id'] is None

@pytest.mark.asyncio
@pytest.mark.parametrize('conflict', ['task', 'argv', 'packet', 'transport', 'launch_args', 'legacy', 'forged', 'identity', 'owner_batch', 'owner_issue'])
async def test_conflicting_or_legacy_admission(tmp_path, monkeypatch, conflict):
    r = await fixture(tmp_path, monkeypatch)
    with native_context(r):
        dc.terminal_dispatch(r.args, lambda _: {'session_id': 'launcher'})
        args = copy.deepcopy(r.args)
        if conflict == 'identity': args['continuation']['identity']['session'] = 'changed'
        if conflict in {'owner_batch', 'owner_issue'}: args['continuation'][conflict] = 'changed'
        if conflict == 'task': args['continuation']['scope'] = 'changed'
        if conflict == 'argv': args['continuation']['worker_route']['argv'] += ['changed']
        if conflict == 'packet':
            from pathlib import Path
            Path(r.task['worker_route']['packet']).write_text('{}')
        if conflict == 'launch_args': args['cwd'] = str(tmp_path)
        if conflict == 'transport':
            from gateway.session_context import completion_launch_context
            binding, callback = completion_launch_context.get()
            completion_launch_context.set((dict(binding, bot_id='another-bot'), callback))
        if conflict == 'legacy':
            with r.guard.transaction() as rows: rows[r.task['task']].pop('native_admission_sha256')
        if conflict == 'forged': args['continuation']['native_admission_sha256'] = 'caller-supplied'
        before = r.guard.path.read_bytes()
        with pytest.raises(ValueError): dc.terminal_dispatch(args, forbidden)
        assert r.guard.path.read_bytes() == before

@pytest.mark.asyncio
async def test_admission_race_and_direct_guard(tmp_path, monkeypatch):
    r = await fixture(tmp_path, monkeypatch)
    original = dc.Guard.register
    def racing_register(guard, task, **kwargs):
        original(guard, task, **kwargs)
        original(guard, task, **kwargs)
    monkeypatch.setattr(dc.Guard, 'register', racing_register)
    with native_context(r):
        _, row = dc.register_native(r.task)
        before = r.guard.path.read_bytes()
        assert dc.register_native(r.task)[1]['ticket_nonce'] == row['ticket_nonce']
        with pytest.raises(ValueError): original(r.guard, r.task)
        assert r.guard.path.read_bytes() == before

@pytest.mark.asyncio
@pytest.mark.parametrize('outcome', ['sent', 'uncertain', 'claimed', 'legacy', 'changed_ticket'])
async def test_worker_send_replay_is_status_only(tmp_path, monkeypatch, outcome):
    r = await fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(dc, 'route_check', lambda *a, **kw: None)
    sends = Mock(side_effect=RuntimeError('unknown transport') if outcome == 'uncertain' else None)
    monkeypatch.setattr(dc, 'cmux_send', sends)
    with native_context(r):
        admitted = dc.terminal_dispatch(r.args, lambda _: {'session_id': 'launcher'})
        ticket = admitted['continuation']['worker_ticket']
        with r.guard.transaction() as rows: rows[r.task['task']]['receipt_sha256'] = 'fixture'
        args = dict(command='SYSTEM167_REGISTERED_WORKER', continuation_ticket=ticket)
        if outcome == 'uncertain':
            with pytest.raises(RuntimeError): dc.terminal_dispatch(args, forbidden)
        else: dc.terminal_dispatch(args, forbidden)
        if outcome == 'claimed':
            with r.guard.transaction() as rows: rows[r.task['task']]['worker_pid'] = 303
        if outcome == 'legacy':
            with r.guard.transaction() as rows: rows[r.task['task']].pop('send_request_sha256')
        if outcome == 'changed_ticket': args['continuation_ticket'] = dict(ticket, ticket_nonce='changed')
        before = r.guard.path.read_bytes()
        if outcome in {'legacy', 'changed_ticket'}:
            with pytest.raises(ValueError): dc.terminal_dispatch(args, forbidden)
        else: assert response(dc.terminal_dispatch(args, forbidden))['already_registered'] is True
        assert r.guard.path.read_bytes() == before
        assert sends.call_count == 1

@pytest.mark.asyncio
@pytest.mark.parametrize('transition', ['main_validation', 'stop', 'expired', 'recovery'])
async def test_old_request_after_transition_is_observation_only(tmp_path, monkeypatch, capsys, transition):
    r = await fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(dc, 'route_check', lambda *a, **kw: None)
    sends = Mock()
    monkeypatch.setattr(dc, 'cmux_send', sends)
    with native_context(r):
        admitted = dc.terminal_dispatch(r.args, lambda _: {'session_id': 'launcher'})
        ticket = admitted['continuation']['worker_ticket']
        with r.guard.transaction() as rows: rows[r.task['task']]['receipt_sha256'] = 'fixture'
        args = dict(command='SYSTEM167_REGISTERED_WORKER', continuation_ticket=ticket)
        dc.terminal_dispatch(args, forbidden)
        if transition == 'main_validation':
            from tests.gateway.test_diggr_origin_delivery import result
            result(r)
        elif transition == 'stop': r.guard.control(r.identity, 'paused')
        elif transition == 'expired': r.clock[0] += 100000
        else:
            # A genuine recovery stores the previous attempt and clears its send
            # claim. Model that already-committed boundary without invoking OS recovery.
            with r.guard.transaction() as rows:
                row = rows[r.task['task']]
                row['history'] = [copy.deepcopy(row)]
                row.update(generation=2, ticket_nonce='new-attempt')
                row.pop('send_request_sha256'); row.pop('visible_sent')
        before = r.guard.path.read_bytes()
        replay = response(dc.terminal_dispatch(r.args, forbidden))
        worker_replay = response(dc.terminal_dispatch(args, forbidden))
        assert r.guard.path.read_bytes() == before
        dc.main(['--home', str(r.home), 'register', '--payload-json', json.dumps(r.task)])
        cli_replay = json.loads(capsys.readouterr().out)['result']
        assert 'ticket_nonce' not in cli_replay and cli_replay['generation'] == 1
        assert r.guard.path.read_bytes() == before
        assert replay['generation'] == worker_replay['generation'] == 1
        if transition != 'expired':
            assert replay['attempt_stale'] and worker_replay['attempt_stale']
            assert replay['worker_pid'] is None
        assert sends.call_count == 1

@pytest.mark.asyncio
async def test_concurrent_identical_requests_have_one_launcher(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    r = await fixture(tmp_path, monkeypatch)
    barrier = Barrier(2)
    bind = dc.bind_visible_target
    def competing_bind(target):
        barrier.wait(timeout=10)
        return bind(target)
    monkeypatch.setattr(dc, 'bind_visible_target', competing_bind)
    launch = Mock(return_value={'session_id': 'launcher'})
    from contextvars import copy_context
    with native_context(r), ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(copy_context().run, dc.terminal_dispatch, r.args, launch) for _ in range(2)]
        replies = [response(f.result(timeout=15)) for f in futures]
    assert launch.call_count == 1
    assert sum(bool(reply.get('already_registered')) for reply in replies) == 1
