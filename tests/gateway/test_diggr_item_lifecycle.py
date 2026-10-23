"""Native owner events and real disposable Git preparation, no live services."""
import asyncio
import copy
from contextlib import nullcontext
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import subprocess
import time
from types import SimpleNamespace


import pytest
from hermes_cli import diggr_continuation as dc, diggr_owner as owner, diggr_item as item, diggr_delivery as delivery
from tests.gateway.test_diggr_owner_grant import route, confirm, manifest
from tests.gateway.test_diggr_logical_admission import prepare, authorize_logical, git
from tests.gateway.test_telegram_reply_quote import _make_message


def _atomic_test_pid_code(path, pid):
    """Publish a complete test-owned PID before the readiness path appears."""
    return (f'pending={path}.with_name({path}.name+".pending"); '
            f'pending.write_text(str({pid})); pending.replace({path}); ')


def test_atomic_test_pid_publication_hides_partial_readiness(tmp_path, monkeypatch):
    ready = tmp_path / 'helper-ready'
    pending = ready.with_name(ready.name + '.pending')
    pending.write_text('')
    assert not ready.exists()
    original_replace = Path.replace

    def observe_commit(pending, target):
        assert target == ready and not ready.exists()
        assert pending.read_text() == str(os.getpid())
        return original_replace(pending, target)

    monkeypatch.setattr(Path, 'replace', observe_commit)
    exec(_atomic_test_pid_code('ready', 'os.getpid()'), {'ready': ready, 'os': os})
    assert int(ready.read_text()) == os.getpid()


def renewal_prompt_evidence(root, bid, active, contract):
    source = Path(root) / 'plane-source.md'
    source.write_bytes(b'Authoritative Plane fixture, supplied by Main.\n')
    prompt = Path(root) / 'executor-prompt.txt'
    prompt.write_bytes('Implement the confirmed fixture.\nGrüße 🎧\n'.encode())
    descriptor = dict(batch=bid, issue=active['issue'], plane_id=contract['plane_id'],
                      contract_sha256=owner.binding_hash(contract),
                      **{k: copy.deepcopy(active[k]) for k in ('worktree', 'branch', 'base', 'target')},
                      source=dict(path=str(source), sha256=dc.digest(source)),
                      prompt=dict(path=str(prompt), sha256=dc.digest(prompt)))
    evidence = Path(root) / 'prompt-binding.json'
    evidence.write_text(json.dumps(descriptor))
    return dict(path=str(evidence), sha256=dc.digest(evidence)), descriptor


def test_second_renewal_prompt_binding_preserves_exact_bytes_and_checks_source(tmp_path):
    active = dict(issue='workspace/project/issue', worktree='/fixture/worktree',
                  branch='hermes/WP-APP-112-fixture', base='a' * 40,
                  target=dict(workspace='w', surface='s'))
    contract = dict(plane_id='APP-112', resources=dict(artifacts=str(tmp_path)))
    evidence, descriptor = renewal_prompt_evidence(tmp_path, 'batch', active, contract)
    binding, prompt = item.renewal_prompt_binding('batch', active, contract, evidence)
    assert binding == dict(descriptor, evidence=evidence)
    assert prompt.encode() == Path(descriptor['prompt']['path']).read_bytes()
    Path(descriptor['source']['path']).write_bytes(b'changed source')
    with pytest.raises(ValueError, match='hash'):
        item.renewal_prompt_binding('batch', active, contract, evidence)


def test_gateway_loads_required_skills_with_native_loader_and_profile_scope(tmp_path, monkeypatch):
    from hermes_constants import set_hermes_home_override, reset_hermes_home_override
    home = tmp_path / 'diggr-main'
    home.mkdir()
    manifest = tmp_path / 'required_skills.py'
    manifest.write_text("def required_skill_ids(profile): return ('diggr-coding-loop-onboarding',)\n")
    monkeypatch.setattr(item, 'REQUIRED_SKILLS_MANIFEST', manifest)
    skill = home / 'skills' / 'diggr-coding-loop-onboarding' / 'SKILL.md'
    skill.parent.mkdir(parents=True)
    skill.write_text('---\nname: diggr-coding-loop-onboarding\ndescription: test loop loading\n---\nTest before PR.\n')
    token = set_hermes_home_override(home)
    try:
        assert 'Test before PR.' in item.required_gateway_skills()
        skill.write_text(skill.read_text().replace('Test before PR.', 'Changed test instruction.'))
        assert 'Changed test instruction.' in item.required_gateway_skills()
        skill.unlink()
        with pytest.raises(ValueError, match='skills missing'):
            item.required_gateway_skills()
    finally:
        reset_hermes_home_override(token)


def test_runtime_attestation_reads_loaded_schema_and_onboarding(monkeypatch):
    from types import SimpleNamespace
    from agent import subagent_lifecycle
    from tools.terminal_tool import TERMINAL_SCHEMA
    monkeypatch.setattr(item, 'require_onboarding_context', lambda: None)
    monkeypatch.setattr(subagent_lifecycle, 'get_active_subagent_parent',
                        lambda: SimpleNamespace(_diggr_onboarding_request=('home', 'session', 'skill', 'a' * 64)))
    receipt = item.runtime_attestation()
    assert receipt['terminal_schema_sha256'] == dc.object_hash(TERMINAL_SCHEMA)
    assert receipt['onboarding_sha256'] == 'a' * 64
    assert receipt['pid'] > 0


def test_runtime_attestation_without_current_provider_receipt_fails(monkeypatch):
    from types import SimpleNamespace
    from agent import subagent_lifecycle
    monkeypatch.setattr(item, 'require_onboarding_context', lambda: None)
    monkeypatch.setattr(subagent_lifecycle, 'get_active_subagent_parent',
                        lambda: SimpleNamespace(_diggr_onboarding_request=None))
    with pytest.raises(ValueError, match='onboarding receipt unavailable'):
        item.runtime_attestation()


def test_native_terminal_schema_exposes_runtime_receipt_flag():
    from tools.terminal_tool import TERMINAL_SCHEMA
    controls = TERMINAL_SCHEMA['parameters']['properties']['continuation_control']['anyOf']
    assert any(control.get('properties', {}).get('include_runtime', {}).get('type') == 'boolean'
               for control in controls)


async def setup(r, monkeypatch, *, publication=None, result_flow=False, mode=None,
                required_test_kinds=None):
    contract, packet = prepare(r, monkeypatch)
    # Disposable Git/cMux fixture has no real Coding coordinator. Dedicated
    # tests exercise the early native rejection and the real resolver separately.
    monkeypatch.setattr(dc, 'preflight_coding_coordinator',
                        lambda: {'workspace': 'fixture', 'surface': 'fixture', 'process': {'pid': 11}})
    contract['version'] = owner.ITEM_VERSION
    contract['acceptance']['owner'] = 'user'
    if publication is not None:
        contract['acceptance']['publication'] = publication
    if mode is not None:
        contract['acceptance']['mode'] = mode
        if mode == 'automatic':
            contract['acceptance']['required_test_kinds'] = required_test_kinds or ['ui']
    contract['actions']['main'] += ['test', 'commit', 'push', 'pr_update']
    repo = contract['resources']['repo']
    (Path(repo) / '.gitignore').write_text('build/\n')
    git(repo, 'add', '.gitignore')
    git(repo, '-c', 'commit.gpgsign=false', 'commit', '-m', 'ignore disposable build')
    base = git(repo, 'rev-parse', 'HEAD')
    git(repo, 'update-ref', 'refs/remotes/origin/main', base)
    await authorize_logical(r, contract)
    if not result_flow:
        # Existing lifecycle cases exercise batches created before flow version 3.
        with r.guard.transaction() as rows:
            batch = rows.grants['batches'][r.task['owner_batch']]
            batch.pop('flow_version', None)
            batch.pop('merge_approval_required', None)
    path = str(Path(contract['resources']['worktrees']) / 'item')
    payload = dict(operation='item_prepare', batch=r.task['owner_batch'], issue=r.task['owner_issue'],
                   worktree=path, branch='hermes/WP-' + contract['plane_id'] + '-item', base=base,
                   target=r.task['worker_route']['visible_target'])
    route_data = r.task['worker_route']
    old = route_data['worktree']
    route_data.update(worktree=path, branch=payload['branch'])
    route_data['argv'] = [path if s == old else s for s in route_data['argv']]
    # This helper upgrades the logical-v1 fixture to v2. Supply v2's complete
    # onboarding before binding argv; keep the production content gate active.
    route_data['argv'][-1] += '\n' + item.onboarding_payload()[0]
    packet.update(worktree=path, branch=payload['branch'], base=base,
                  acceptance=contract['acceptance'], role_actions=contract['actions'], coding_lane='audit_gated')
    Path(route_data['packet']).write_text(json.dumps(packet))
    route_data['packet_sha256'] = dc.digest(route_data['packet'])
    r.task['launcher_command'] = dc.logical_launcher_command(contract, route_data)
    return contract, payload


def register(r):
    token = dc.EVENT_CONTEXT.set(dict(identity=r.identity))
    try:
        return dc.register_native(r.task)[1]
    finally:
        dc.EVENT_CONTEXT.reset(token)


@pytest.mark.asyncio
async def test_registered_prompt_reference_preserves_long_unicode_bytes_and_rejects_change(route, monkeypatch):
    r = route
    contract, payload = await setup(r, monkeypatch)
    item.control(r.guard, r.identity, payload)
    bound = r.task['worker_route']
    prompt = bound['argv'][-1] + '\nUnicode: Grüße 🎧\nQuotes: "one" and \'two\'\n' + ('long line\n' * 4000)
    source = Path(contract['resources']['artifacts']) / 'executor-prompt.txt'
    source.write_bytes(prompt.encode('utf-8'))
    packet_path = Path(bound['packet'])
    packet = json.loads(packet_path.read_text())
    packet.update(coding_prompt_source=str(source), coding_prompt_sha256=dc.digest(source))
    packet_path.write_text(json.dumps(packet))
    bound['packet_sha256'] = dc.digest(packet_path)
    bound['argv'][-1] = '@coding_prompt_source'
    before = r.guard.path.read_bytes()
    source.write_bytes((prompt + '!').encode('utf-8'))
    with pytest.raises(ValueError, match='coding prompt source hash mismatch'):
        register(r)
    assert r.guard.path.read_bytes() == before
    source.write_bytes(prompt.encode('utf-8'))
    row = register(r)
    assert row['worker_route']['argv'][-1].encode('utf-8') == source.read_bytes()
    assert register(r)['native_admission_sha256'] == row['native_admission_sha256']
    source.write_bytes((prompt + '!').encode('utf-8'))
    with pytest.raises(ValueError, match='coding prompt source hash mismatch'):
        register(r)


@pytest.mark.asyncio
async def test_missing_coding_coordinator_blocks_before_registration(route, monkeypatch):
    r = route
    _, payload = await setup(r, monkeypatch)
    item.control(r.guard, r.identity, payload)
    before = r.guard.path.read_bytes()
    observed = []

    def absent():
        observed.append('checked')
        raise ValueError('canonical Coding coordinator unavailable or preload stale')

    monkeypatch.setattr(dc, 'preflight_coding_coordinator', absent, raising=False)
    with pytest.raises(ValueError, match='canonical Coding coordinator unavailable'):
        register(r)
    assert observed == ['checked']
    assert r.guard.path.read_bytes() == before
    monkeypatch.setattr(dc, 'preflight_coding_coordinator',
                        lambda: {'workspace': 'w', 'surface': 's', 'process': {'pid': 11}})
    assert register(r)['generation'] == 1


@pytest.mark.asyncio
async def test_changed_mandatory_skill_blocks_registered_old_prompt_before_send(route, monkeypatch):
    r = route
    _, payload = await setup(r, monkeypatch)
    item.control(r.guard, r.identity, payload)
    row = register(r)
    monkeypatch.setattr(item, 'onboarding_payload',
                        lambda: ('updated mandatory onboarding', '/synthetic/onboarding'))
    with pytest.raises(ValueError, match='complete diggr-coding-loop-onboarding'):
        dc.main(['--home', r.identity['home'], 'route-preflight', '--payload-json',
                 json.dumps(dict(ticket=dc.worker_ticket(row), route=row['worker_route']))])
    current = r.guard.get(row['task'])
    assert not current.get('visible_sent') and not current.get('worker_pid')
    assert not Path(row['worker_route']['receipt']).exists()


async def documentation_resolution_fixture(r, monkeypatch):
    contract, payload = await setup(r, monkeypatch)
    repo = Path(contract['resources']['repo'])
    (repo / 'ARCHITECTURE.md').write_text('Current architecture.\n')
    (repo / 'src' / 'consumer.txt').write_text('Current consumer.\n')
    git(repo, 'add', 'ARCHITECTURE.md', 'src/consumer.txt')
    git(repo, '-c', 'commit.gpgsign=false', 'commit', '-m', 'fixture documentation')
    payload['base'] = git(repo, 'rev-parse', 'HEAD')
    git(repo, 'update-ref', 'refs/remotes/origin/main', payload['base'])
    item.control(r.guard, r.identity, payload)
    blocker = evidence(contract, 'documentation-blocked.json', dict(
        batch=payload['batch'], issue=payload['issue'], reason='old docs missing',
        observations=['old architecture pack absent']))
    item.control(r.guard, r.identity, dict(payload, operation='item_preparation_blocked', evidence=blocker))
    with r.guard.transaction() as rows:
        prepared = rows.grants['batches'][payload['batch']]
        prepared['item']['preparation_wakes'] = 1
        prepared['native_transport'] = dict(
            platform='telegram', chat_id=r.identity['chat_id'], user_id=r.identity['user_id'],
            session_key=r.identity['session_key'], parent_session_id=r.identity['session'])
    decision = ('Manni, für ' + contract['plane_id'] + ' genehmige ich die Repo-Architektur '
                'und Producer-/Consumer-Datenwege als Dokumentationsgrundlage.')
    db = Path(r.identity['home']) / 'state.db'
    with sqlite3.connect(db) as conn:
        conn.execute('CREATE TABLE sessions (id TEXT, source TEXT, user_id TEXT, session_key TEXT, profile_name TEXT)')
        conn.execute('CREATE TABLE messages (id INTEGER, session_id TEXT, role TEXT, content TEXT, timestamp REAL)')
        conn.execute('INSERT INTO sessions VALUES (?,?,?,?,?)', (
            r.identity['session'], 'telegram', owner.OWNER, r.identity['session_key'], r.identity['profile']))
        conn.execute('INSERT INTO messages VALUES (?,?,?,?,?)', (
            7, r.identity['session'], 'user', decision, r.clock[0] + 1))
    worktree = Path(payload['worktree'])
    sources = [dict(role=role, path=name) for role, name in (
        ('architecture', 'ARCHITECTURE.md'), ('producer', 'src/code.txt'),
        ('consumer', 'src/consumer.txt'))]
    report = dict(batch=payload['batch'], issue=payload['issue'],
        decision='replace_missing_documentation_with_repo_evidence',
        blocker_evidence=blocker, owner_message_id=7,
        sources=sources, observations=['replacement sources reviewed'])
    return contract, payload, report


@pytest.mark.asyncio
async def test_documentation_resolution_keeps_same_prepared_item_and_is_idempotent(route, monkeypatch):
    r = route
    contract, payload, report = await documentation_resolution_fixture(r, monkeypatch)
    ref = evidence(contract, 'documentation-resolved.json', report)
    command = (str(Path(sys.executable).resolve()) + " -I -S - <<'PY'\n"
               "from pathlib import Path\nimport hashlib\n"
               "print(hashlib.sha256(Path(" + repr(ref['path']) + ").read_bytes()).hexdigest())\nPY")
    result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
    assert result.stdout.strip() == ref['sha256']
    request = dict(payload, operation='item_preparation_resolve', evidence=ref)
    token = dc.EVENT_CONTEXT.set(dict(identity=r.identity, direct_owner_event='fixture-owner-event'))
    try:
        receipt = item.control(r.guard, r.identity, request)
        assert receipt['contract_sha256'] == owner.binding_hash(contract)
        assert receipt['owner_message_sha256'] == hashlib.sha256(
            ('Manni, für ' + contract['plane_id'] + ' genehmige ich die Repo-Architektur '
             'und Producer-/Consumer-Datenwege als Dokumentationsgrundlage.').encode()).hexdigest()
        assert receipt['sources'] == [dict(source, sha256=dc.digest(Path(payload['worktree']) / source['path']))
                                      for source in report['sources']]
        before = r.guard.path.read_bytes()
        assert item.control(r.guard, r.identity, request) == receipt
        assert r.guard.path.read_bytes() == before
        other = evidence(contract, 'different-resolution.json', report)
        with pytest.raises(ValueError, match='already resolved'):
            item.control(r.guard, r.identity, dict(request, evidence=other))
        assert r.guard.path.read_bytes() == before
    finally:
        dc.EVENT_CONTEXT.reset(token)
    with r.guard.transaction(read_only=True) as rows:
        batch = rows.grants['batches'][payload['batch']]
        assert len(rows.grants['batches']) == 1
        assert batch['item']['phase'] == 'prepared' and batch['tasks'] == {}
        assert batch['item']['work_seconds'] == 0
        assert batch['item']['preparation_wakes'] == 1
        assert batch['item']['worktree'] == payload['worktree']
        assert batch['manifest']['todos'][0]['contract'] == contract
        assert 'preparation_blocker' not in batch['item']
        assert batch['resolved_preparation_blockers'][0]['blocker']['evidence'] == report['blocker_evidence']
    assert git(payload['worktree'], 'status', '--porcelain') == ''


@pytest.mark.asyncio
async def test_documentation_resolution_rejects_owner_and_contract_drift(route, monkeypatch):
    r = route
    contract, payload, report = await documentation_resolution_fixture(r, monkeypatch)
    db = Path(r.identity['home']) / 'state.db'
    with sqlite3.connect(db) as conn:
        current = conn.execute('SELECT session_id,role,content FROM messages WHERE id=7').fetchone()
        conn.execute('INSERT INTO messages VALUES (?,?,?,?,?)', (8, *current, 0))
    token = dc.EVENT_CONTEXT.set(dict(identity=r.identity, direct_owner_event='fixture-owner-event'))
    try:
        for label, change in (
            ('wrong-owner-hash', dict(owner_message_sha256='0' * 64)),
            ('wrong-owner-id', dict(owner_message_id=8)),
            ('stale-contract', dict(contract_sha256='0' * 64)),
            ('wrong-blocker', dict(blocker_evidence=dict(report['blocker_evidence'], sha256='0' * 64))),
            ('wrong-source', dict(sources=[dict(source, sha256='0' * 64)
                if source['role'] == 'producer' else source for source in report['sources']]))):
            ref = evidence(contract, label + '.json', dict(report, **change))
            before = r.guard.path.read_bytes()
            with pytest.raises(ValueError):
                item.control(r.guard, r.identity, dict(payload, operation='item_preparation_resolve', evidence=ref))
            assert r.guard.path.read_bytes() == before
        ref = evidence(contract, 'no-direct-context.json', report)
        with sqlite3.connect(db) as conn:
            conn.execute("UPDATE messages SET role='assistant' WHERE id=7")
        before = r.guard.path.read_bytes()
        with pytest.raises(ValueError, match='Owner decision is absent'):
            item.control(r.guard, r.identity, dict(payload, operation='item_preparation_resolve', evidence=ref))
        assert r.guard.path.read_bytes() == before
        with r.guard.transaction() as rows:
            rows.grants['batches'][payload['batch']]['manifest']['todos'][0]['contract']['scope'] = 'changed'
        with pytest.raises(ValueError, match='confirmed preparation contract changed'):
            item.control(r.guard, r.identity, dict(payload, operation='item_preparation_resolve', evidence=ref))
    finally:
        dc.EVENT_CONTEXT.reset(token)
    with pytest.raises(ValueError, match='direct original Owner/Main context'):
        item.control(r.guard, r.identity, dict(payload, operation='item_preparation_resolve', evidence=ref))


@pytest.mark.asyncio
async def test_direct_telegram_owner_stamp_reaches_native_tool_context(route):
    r = route
    from gateway.session import build_session_context
    from hermes_cli import diggr_delivery as delivery
    event = await r.event('Manni, fortsetzen')
    entry = await r.runner.async_session_store.get_or_create_session(event.source)
    context = build_session_context(event.source, r.runner.config, entry)
    tokens = await delivery.set_tool_context(r.runner, event, context, entry)
    try:
        assert dc.EVENT_CONTEXT.get()['direct_owner_event']
        assert dc.EVENT_CONTEXT.get()['native_wake'] is None
    finally:
        r.runner._clear_session_env(tokens)


@pytest.mark.asyncio
async def test_plain_telegram_text_batch_preserves_direct_owner_tool_context(route):
    r = route
    text = 'Manni, prüfe die bestehende APP-105-Vorbereitung.'
    msg = _make_message(text=text)
    msg.chat.id = int(owner.OWNER)
    msg.from_user.id = int(owner.OWNER)
    msg.from_user.is_bot = False
    r.adapter._text_batch_delay_seconds = 0
    r.adapter._text_batch_split_delay_seconds = 0
    await r.adapter._handle_text_message(
        SimpleNamespace(effective_message=msg, message=msg, update_id=123), None)
    await asyncio.gather(*r.adapter._pending_text_batch_tasks.values())
    event = r.adapter.handle_message.call_args.args[0]
    assert event.text == text
    from gateway.session import build_session_context
    from hermes_cli import diggr_delivery as delivery
    entry = await r.runner.async_session_store.get_or_create_session(event.source)
    context = build_session_context(event.source, r.runner.config, entry)
    tokens = await delivery.set_tool_context(r.runner, event, context, entry)
    try:
        assert dc.EVENT_CONTEXT.get()['direct_owner_event'] == '123:' + str(msg.message_id)
        assert dc.EVENT_CONTEXT.get()['native_wake'] is None
    finally:
        r.runner._clear_session_env(tokens)


@pytest.mark.asyncio
async def test_merged_telegram_text_does_not_reuse_one_owner_proof(route):
    r = route
    r.adapter._text_batch_delay_seconds = .05
    first = _make_message(text='Erster Teil')
    second = _make_message(text='Zweiter Teil')
    second.message_id = first.message_id + 1
    for update_id, msg in ((123, first), (124, second)):
        msg.chat.id = int(owner.OWNER)
        msg.from_user.id = int(owner.OWNER)
        msg.from_user.is_bot = False
        await r.adapter._handle_text_message(
            SimpleNamespace(effective_message=msg, message=msg, update_id=update_id), None)
    await asyncio.gather(*r.adapter._pending_text_batch_tasks.values())
    event = r.adapter.handle_message.call_args.args[0]
    assert event.text == 'Erster Teil\nZweiter Teil'
    from gateway.session import build_session_context
    from hermes_cli import diggr_delivery as delivery
    entry = await r.runner.async_session_store.get_or_create_session(event.source)
    context = build_session_context(event.source, r.runner.config, entry)
    tokens = await delivery.set_tool_context(r.runner, event, context, entry)
    try:
        assert dc.EVENT_CONTEXT.get()['direct_owner_event'] is None
    finally:
        r.runner._clear_session_env(tokens)


@pytest.mark.asyncio
async def test_plain_telegram_text_resolves_preparation_through_native_tool_context(route, monkeypatch):
    r = route
    msg = _make_message(text='Manni, APP-105 mit belegter Owner-Entscheidung fortsetzen')
    msg.chat.id = int(owner.OWNER)
    msg.from_user.id = int(owner.OWNER)
    msg.from_user.is_bot = False
    r.adapter._text_batch_delay_seconds = 0
    await r.adapter._handle_text_message(
        SimpleNamespace(effective_message=msg, message=msg, update_id=127), None)
    await asyncio.gather(*r.adapter._pending_text_batch_tasks.values())
    event = r.adapter.handle_message.call_args.args[0]
    entry = await r.runner.async_session_store.get_or_create_session(event.source)
    r.identity = r.runner._diggr_identity(event.source, entry.session_id)
    r.task['identity'] = r.identity
    contract, payload, report = await documentation_resolution_fixture(r, monkeypatch)
    ref = evidence(contract, 'text-owner-resolution.json', report)
    from gateway.session import build_session_context
    from hermes_cli import diggr_delivery as delivery
    context = build_session_context(event.source, r.runner.config, entry)
    tokens = await delivery.set_tool_context(r.runner, event, context, entry)
    try:
        assert dc.EVENT_CONTEXT.get()['direct_owner_event'] == '127:' + str(msg.message_id)
        receipt = item.control(r.guard, r.identity, dict(payload,
            operation='item_preparation_resolve', evidence=ref))
        assert receipt['evidence'] == ref
    finally:
        r.runner._clear_session_env(tokens)


@pytest.mark.asyncio
@pytest.mark.parametrize('change', ['foreign', 'forwarded', 'reply', 'internal'])
async def test_plain_telegram_text_rejects_non_direct_owner_context(route, change):
    r = route
    msg = _make_message(text='Manni, APP-105 fortsetzen')
    msg.chat.id = int(owner.OWNER)
    msg.from_user.id = int(owner.OWNER)
    msg.from_user.is_bot = False
    if change == 'foreign':
        msg.from_user.id += 1
    elif change == 'forwarded':
        msg.forward_origin = object()
    elif change == 'reply':
        msg.reply_to_message = _make_message(text='frühere Nachricht')
    r.adapter._text_batch_delay_seconds = 0
    await r.adapter._handle_text_message(
        SimpleNamespace(effective_message=msg, message=msg, update_id=124), None)
    await asyncio.gather(*r.adapter._pending_text_batch_tasks.values())
    event = r.adapter.handle_message.call_args.args[0]
    if change == 'internal':
        event.internal = True
    from gateway.session import build_session_context
    from hermes_cli import diggr_delivery as delivery
    entry = await r.runner.async_session_store.get_or_create_session(event.source)
    context = build_session_context(event.source, r.runner.config, entry)
    tokens = await delivery.set_tool_context(r.runner, event, context, entry)
    try:
        assert dc.EVENT_CONTEXT.get()['direct_owner_event'] is None
    finally:
        r.runner._clear_session_env(tokens)


@pytest.mark.asyncio
async def test_combined_telegram_text_cannot_reuse_first_owner_proof(route):
    r = route
    r.adapter._text_batch_delay_seconds = .05
    for update_id, text in ((125, 'Manni, APP-105'), (126, 'bitte fortsetzen')):
        msg = _make_message(text=text)
        msg.chat.id = int(owner.OWNER)
        msg.from_user.id = int(owner.OWNER)
        msg.from_user.is_bot = False
        await r.adapter._handle_text_message(
            SimpleNamespace(effective_message=msg, message=msg, update_id=update_id), None)
    await asyncio.gather(*r.adapter._pending_text_batch_tasks.values())
    event = r.adapter.handle_message.call_args.args[0]
    assert event.text == 'Manni, APP-105\nbitte fortsetzen'
    from gateway.session import build_session_context
    from hermes_cli import diggr_delivery as delivery
    entry = await r.runner.async_session_store.get_or_create_session(event.source)
    context = build_session_context(event.source, r.runner.config, entry)
    tokens = await delivery.set_tool_context(r.runner, event, context, entry)
    try:
        assert dc.EVENT_CONTEXT.get()['direct_owner_event'] is None
    finally:
        r.runner._clear_session_env(tokens)


@pytest.mark.asyncio
async def test_prepared_scope_correction_keeps_batch_worktree_and_budget(route, monkeypatch):
    r = route
    contract, payload = await setup(r, monkeypatch)
    prepared = item.control(r.guard, r.identity, payload)
    old_batch = payload['batch']
    with r.guard.transaction() as rows:
        batch = rows.grants['batches'][old_batch]
        old_manifest = copy.deepcopy(batch['manifest'])
        batch['item']['preparation_blocker'] = dict(reason='original scope excludes source file')
        original_batches = set(rows.grants['batches'])
    corrected = copy.deepcopy(old_manifest)
    corrected['replaces'] = old_batch
    corrected['todos'][0]['contract']['scope'] = 'Exact prepared source file only'
    corrected['todos'][0]['contract']['resources']['allowed_paths'] = ['src/code.txt']
    proposal = owner.propose(r.guard, r.identity, corrected)
    with r.guard.transaction(read_only=True) as rows:
        assert rows.grants['proposals'][proposal]['scope_amendment'] is True
    confirmed, _, _ = await confirm(r, corrected)
    assert confirmed == old_batch
    with r.guard.transaction(read_only=True) as rows:
        batch = rows.grants['batches'][old_batch]
        assert set(rows.grants['batches']) == original_batches
        assert batch['item']['worktree'] == prepared['worktree']
        assert batch['item']['budget'] == prepared['budget']
        assert batch['item']['work_seconds'] == 0
        assert batch['item']['phase'] == 'prepared' and batch['tasks'] == {}
        assert batch['manifest']['todos'][0]['contract']['resources']['allowed_paths'] == ['src/code.txt']
        assert batch['resolved_preparation_blockers'][0]['reason'] == 'original scope excludes source file'
        assert 'preparation_blocker' not in batch['item']
        assert batch['scope_amendments'][0]['native_owner_event']
        previous = batch['scope_amendments'][0]['previous_proposal']
        assert rows.grants['proposals'][previous]['manifest'] == old_manifest
    assert item.status(r.guard, r.identity, old_batch, payload['issue'])['phase'] == 'prepared'
    from hermes_cli import diggr_delivery as delivery
    with r.guard.transaction() as rows:
        batch = rows.grants['batches'][old_batch]
        batch['native_transport'] = delivery.native_transport(r.identity)
        batch['coordinator_delivery'] = dict(status='response_produced')
    async def origin(*_args):
        return object()
    monkeypatch.setattr(delivery, 'origin_scope', origin)
    monkeypatch.setattr(delivery, 'batch_work_blocked', lambda _runner: False)
    monkeypatch.setattr(delivery, 'preparation_observer_pending', lambda *_args: False)
    r.runner._running_agents = {}
    r.runner._completion_receipt_adapter = lambda *_args: r.adapter
    await delivery.reconcile_prepared(r.runner, r.guard, r.identity)
    await delivery.reconcile_prepared(r.runner, r.guard, r.identity)
    with r.guard.transaction(read_only=True) as rows:
        batch = rows.grants['batches'][old_batch]
        assert batch['item']['preparation_wakes'] == 1
        assert batch['preparation_delivery']['status'] == 'pending'
        assert batch['preparation_delivery']['generation'] == 1


@pytest.mark.asyncio
async def test_prepared_scope_correction_rejects_budget_and_no_touch_drift(route, monkeypatch):
    r = route
    contract, payload = await setup(r, monkeypatch)
    item.control(r.guard, r.identity, payload)
    with r.guard.transaction(read_only=True) as rows:
        original = copy.deepcopy(rows.grants['batches'][payload['batch']]['manifest'])
    corrected = copy.deepcopy(original)
    corrected['replaces'] = payload['batch']
    corrected['todos'][0]['contract']['resources']['allowed_paths'] = ['src/code.txt']
    before = r.guard.path.read_bytes()
    with pytest.raises(ValueError, match='documented preparation blocker'):
        owner.propose(r.guard, r.identity, corrected)
    assert r.guard.path.read_bytes() == before
    with r.guard.transaction() as rows:
        rows.grants['batches'][payload['batch']]['item']['preparation_blocker'] = dict(reason='scope is wrong')
    bad_budget = copy.deepcopy(corrected)
    bad_budget['budgets']['wakes'] += 1
    before = r.guard.path.read_bytes()
    with pytest.raises(ValueError):
        owner.propose(r.guard, r.identity, bad_budget)
    assert r.guard.path.read_bytes() == before
    corrected['todos'][0]['contract']['resources']['no_touch'] = ['src']
    with pytest.raises(ValueError, match='outside No-Touch'):
        owner.propose(r.guard, r.identity, corrected)
    assert r.guard.path.read_bytes() == before


@pytest.mark.asyncio
async def test_reserve_before_git_and_consume_same_binding(route, monkeypatch):
    r = route
    contract, payload = await setup(r, monkeypatch)
    original = item.git
    reservations = []

    def checked(repo, *args):
        if args[:2] == ('worktree', 'add'):
            with r.guard.transaction(read_only=True) as rows:
                reservations.append(rows.grants['batches'][payload['batch']]['item']['phase'])
        return original(repo, *args)

    monkeypatch.setattr(item, 'git', checked)
    before = r.guard.path.read_bytes()
    with pytest.raises((ValueError, FileNotFoundError, subprocess.CalledProcessError)):
        register(r)
    assert r.guard.path.read_bytes() == before
    prepared = item.control(r.guard, r.identity, payload)
    assert prepared['phase'] == 'prepared' and reservations == ['preparing']
    assert item.control(r.guard, r.identity, payload) == prepared
    assert reservations == ['preparing']
    row = register(r)
    assert row['policy']['item_lifecycle']
    assert row['hard_stop'] is None and row['authorization_expires_at'] is None
    assert item.status(r.guard, r.identity, payload['batch'], payload['issue'])['phase'] == 'coding'
    r.clock[0] += 86400
    current = r.guard.get(row['task'])
    assert dc.Guard.renew(current, r.clock[0])
    assert current['policy']['recoveries'] == 0
    assert current['policy']['work_seconds'] == 0
    assert not dc.Guard.renew(dict(current, policy=dict(current['policy'])), r.clock[0])


@pytest.mark.asyncio
async def test_occupied_lane_never_creates_worktree(route, monkeypatch):
    r = route
    _, payload = await setup(r, monkeypatch)
    with r.guard.transaction() as rows:
        rows.grants['batches']['other'] = dict(item=dict(phase='awaiting_test'))
    with pytest.raises(ValueError, match='another item'):
        item.control(r.guard, r.identity, payload)
    assert not Path(payload['worktree']).exists()
    with r.guard.transaction(read_only=True) as rows:
        assert not rows.grants['batches'][payload['batch']].get('item')


@pytest.mark.asyncio
async def test_crash_after_git_creation_recovers_by_readback_not_resend(route, monkeypatch):
    r = route
    _, payload = await setup(r, monkeypatch)
    original = item.git

    def crash(repo, *args):
        result = original(repo, *args)
        if args[:2] == ('worktree', 'add'):
            raise OSError('lost success response')
        return result

    monkeypatch.setattr(item, 'git', crash)
    with pytest.raises(ValueError, match='reservation retained'):
        item.control(r.guard, r.identity, payload)
    assert item.control(r.guard, r.identity, payload)['phase'] == 'preparation_unknown'
    confirmed = item.control(r.guard, r.identity, dict(payload, operation='item_prepare_readback'))
    assert confirmed['phase'] == 'prepared'
    assert git(payload['worktree'], 'rev-parse', 'HEAD') == payload['base']


def evidence(contract, name, value):
    path = Path(contract['resources']['artifacts']) / name
    path.write_text(json.dumps(value))
    return dict(path=str(path), sha256=dc.digest(path))


async def preparation_assignment_recovery_fixture(r, monkeypatch):
    contract, payload = await setup(r, monkeypatch, result_flow=True)
    prepared = item.control(r.guard, r.identity, payload)
    old_assignment = prepared['coding_assignment']['id']
    with r.guard.transaction() as rows:
        batch = rows.grants['batches'][payload['batch']]
        batch['native_transport'] = delivery.native_transport(r.identity)
        batch['preparation_delivery'] = dict(
            status='response_produced', accepted_at=time.time() - 900,
            deadline=time.time() - 300, generation=1, attempts=1,
            run_id='fixture-preparation')
        batch['item']['coding_assignment'].update(
            status='blocked', reason='confirmed assignment deadline expired')
    report = dict(
        batch=payload['batch'], issue=payload['issue'],
        reason=('Accepted Coding preparation produced no origin-bound response or artifacts '
                'before its native deadline; canonical coordinator origin is misbound.'),
        observations=[
            'item_status remained phase=prepared, task=null, execution.claimed=false and work_seconds=0 after deadline',
            'No message after cursor 10 exists in the process-bound session fixture by observed time 20',
            'The approved artifact directory fixture did not exist and no APP-112 preparation artifacts were found',
            ('Coordinator PID 999999999 is invoked with --profile diggr-coding but its effective '
             'HERMES_HOME is /profiles/diggr-main and it is bound to the old Main Telegram session old-main'),
        ])
    blocker = evidence(contract, 'preparation-recovery-blocker.json', report)
    item.control(r.guard, r.identity, dict(
        payload, operation='item_preparation_blocked', evidence=blocker))
    return contract, payload, blocker, old_assignment


@pytest.mark.asyncio
async def test_preparation_assignment_recovery_is_single_and_preserves_reservation(route, monkeypatch):
    r = route
    _, payload, blocker, old_assignment = await preparation_assignment_recovery_fixture(r, monkeypatch)
    with r.guard.transaction(read_only=True) as rows:
        before_batches = set(rows.grants['batches'])
        before_budget = copy.deepcopy(rows.grants['batches'][payload['batch']]['item']['budget'])
        before_worktree = rows.grants['batches'][payload['batch']]['item']['worktree']
    token = dc.EVENT_CONTEXT.set(dict(
        identity=r.identity, direct_owner_event='fixture-owner-event', native_wake=None))
    try:
        receipt = item.control(r.guard, r.identity, dict(
            payload, operation='item_preparation_assignment_recover', evidence=blocker))
        sealed = r.guard.path.read_bytes()
        assert item.control(r.guard, r.identity, dict(
            payload, operation='item_preparation_assignment_recover', evidence=blocker)) == receipt
        assert r.guard.path.read_bytes() == sealed
    finally:
        dc.EVENT_CONTEXT.reset(token)
    with r.guard.transaction(read_only=True) as rows:
        batch = rows.grants['batches'][payload['batch']]
        active = batch['item']
        assert set(rows.grants['batches']) == before_batches
        assert batch['tasks'] == {} and batch['started_at'] is None and batch['deadline'] is None
        assert active['worktree'] == before_worktree and active['work_seconds'] == 0
        assert active['budget'] == before_budget == batch['manifest']['budgets']
        assert active['coding_assignment']['status'] == 'pending'
        assert active['coding_assignment']['id'] == receipt['new_assignment']
        assert active['coding_assignment']['preparation_recovery_of'] == old_assignment
        assert [entry['id'] for entry in active['coding_assignment_history']] == [old_assignment]
        assert 'preparation_blocker' not in active
        assert len(batch['recovered_preparation_blockers']) == 1
        assert dc.coding_assignment_deadline(batch) == receipt['assignment_deadline']
        assert not any(row.get('owner_batch') == payload['batch'] for row in rows.values())


@pytest.mark.asyncio
@pytest.mark.parametrize('remove_recovery', [False, True])
async def test_preparation_recovery_without_current_marker_keeps_structural_deadline_gate(
        route, monkeypatch, remove_recovery):
    r = route
    _, payload, blocker, _ = await preparation_assignment_recovery_fixture(r, monkeypatch)
    token = dc.EVENT_CONTEXT.set(dict(
        identity=r.identity, direct_owner_event='fixture-owner-event', native_wake=None))
    try:
        receipt = item.control(r.guard, r.identity, dict(
            payload, operation='item_preparation_assignment_recover', evidence=blocker))
    finally:
        dc.EVENT_CONTEXT.reset(token)
    with r.guard.transaction() as rows:
        batch = rows.grants['batches'][payload['batch']]
        active = batch['item']
        assert active['coding_assignment']['id'] != active['coding_assignment_history'][-1]['id']
        active['coding_assignment'].pop('preparation_recovery_of')
        if remove_recovery:
            active.pop('preparation_assignment_recovery')
        batch['deadline'] = receipt['assignment_deadline'] + 1200
        assert dc.coding_assignment_deadline(batch) == float('-inf')


@pytest.mark.asyncio
async def test_preparation_assignment_recovery_accepts_exact_transferred_flow3_reservation(route, monkeypatch):
    r = route
    _, payload, blocker, old_assignment = await preparation_assignment_recovery_fixture(r, monkeypatch)
    predecessor = 'f' * 64
    with r.guard.transaction() as rows:
        batch = rows.grants['batches'][payload['batch']]
        archived = copy.deepcopy(batch)
        archived['revoked'] = True
        archived['replaced_by'] = payload['batch']
        archived['item'].update(phase='archived', transferred_to=payload['batch'])
        archived['tasks'] = {}
        rows.grants['batches'][predecessor] = archived
        batch.pop('flow_version')
        batch['replaces'] = predecessor
        batch['preparation_transfer'] = dict(
            from_batch=predecessor, to_batch=payload['batch'],
            native_owner_event='fixture-transfer')
    token = dc.EVENT_CONTEXT.set(dict(
        identity=r.identity, direct_owner_event='fixture-owner-event', native_wake=None))
    try:
        receipt = item.control(r.guard, r.identity, dict(
            payload, operation='item_preparation_assignment_recover', evidence=blocker))
    finally:
        dc.EVENT_CONTEXT.reset(token)
    with r.guard.transaction(read_only=True) as rows:
        batch = rows.grants['batches'][payload['batch']]
        assert receipt['core']['flow3_source_batch'] == predecessor
        assert batch['flow_version'] == 3
        assert batch['item']['coding_assignment']['preparation_recovery_of'] == old_assignment
        assert rows.grants['batches'][predecessor]['item']['phase'] == 'archived'


@pytest.mark.asyncio
@pytest.mark.parametrize('change', ['dirty', 'work', 'task', 'live_coordinator', 'changed_evidence'])
async def test_preparation_assignment_recovery_fails_closed(route, monkeypatch, change):
    r = route
    _, payload, blocker, _ = await preparation_assignment_recovery_fixture(r, monkeypatch)
    if change == 'dirty':
        Path(payload['worktree'], 'dirty.txt').write_text('dirty\n')
    elif change == 'work':
        with r.guard.transaction() as rows:
            rows.grants['batches'][payload['batch']]['item']['work_seconds'] = 1
    elif change == 'task':
        with r.guard.transaction() as rows:
            rows.grants['batches'][payload['batch']]['tasks'][payload['issue']] = 'unexpected-task'
    elif change == 'live_coordinator':
        monkeypatch.setattr(dc, 'pid_is_absent', lambda _pid: False)
    else:
        blocker = dict(blocker, sha256='0' * 64)
    before = r.guard.path.read_bytes()
    token = dc.EVENT_CONTEXT.set(dict(
        identity=r.identity, direct_owner_event='fixture-owner-event', native_wake=None))
    try:
        with pytest.raises(ValueError):
            item.control(r.guard, r.identity, dict(
                payload, operation='item_preparation_assignment_recover', evidence=blocker))
    finally:
        dc.EVENT_CONTEXT.reset(token)
    assert r.guard.path.read_bytes() == before


@pytest.mark.asyncio
async def test_preparation_assignment_recovery_requires_direct_owner_context(route, monkeypatch):
    r = route
    _, payload, blocker, _ = await preparation_assignment_recovery_fixture(r, monkeypatch)
    before = r.guard.path.read_bytes()
    with pytest.raises(ValueError, match='direct original Owner/Main context'):
        item.control(r.guard, r.identity, dict(
            payload, operation='item_preparation_assignment_recover', evidence=blocker))
    assert r.guard.path.read_bytes() == before


async def accepted_recovery_assignment(r, monkeypatch):
    contract, payload, blocker, _ = await preparation_assignment_recovery_fixture(r, monkeypatch)
    token = dc.EVENT_CONTEXT.set(dict(
        identity=r.identity, direct_owner_event='fixture-owner-event', native_wake=None))
    try:
        receipt = item.control(r.guard, r.identity, dict(
            payload, operation='item_preparation_assignment_recover', evidence=blocker))
    finally:
        dc.EVENT_CONTEXT.reset(token)
    process = dict(pid=99999999, start='old-process-start', executable='/usr/bin/hermes')
    with r.guard.transaction() as rows:
        assignment = rows.grants['batches'][payload['batch']]['item']['coding_assignment']
        assignment.update(status='accepted', accepted_at=time.time(), session='coding-session',
                          binding=dict(process=dict(identity=process)))
    r.clock[0] = receipt['assignment_deadline'] + 1
    return contract, payload, receipt


async def sent_recovery_renewal(r, monkeypatch, *, retry=False):
    _, payload, _ = await accepted_recovery_assignment(r, monkeypatch)
    with r.guard.transaction(read_only=True) as rows:
        transport = copy.deepcopy(rows.grants['batches'][payload['batch']]['native_transport'])
    monkeypatch.setattr(delivery, 'command_transport', lambda *args: transport)
    preview = item.control(r.guard, r.identity, dict(payload, operation='item_assignment_renew_preview'))
    assert await r.runner._diggr_accept(await r.event(preview['next_owner_command'])) is False
    with r.guard.transaction(read_only=True) as rows:
        r.clock[0] = rows.grants['batches'][payload['batch']]['item']['assignment_renewals'][0]['assignment_deadline'] + 1
    binding = dict(workspace='fixture', surface='fixture',
                   process=dict(identity=dict(pid=os.getpid())))
    if retry:
        checks = [0]

        def lane():
            checks[0] += 1
            return dict(binding, surface='changed') if checks[0] == 2 else binding

        monkeypatch.setattr(dc, 'preflight_coding_coordinator', lane)
    else:
        monkeypatch.setattr(dc, 'preflight_coding_coordinator', lambda: binding)
    sends = mock_assignment_send(monkeypatch)
    delivery.deliver_coding_assignments(r.guard, r.identity)
    if retry:
        assert sends == []
        delivery.deliver_coding_assignments(r.guard, r.identity)
    assert len(sends) == 1
    monkeypatch.setattr(dc, 'preflight_coding_coordinator', lambda: binding)
    with r.guard.transaction(read_only=True) as rows:
        active = rows.grants['batches'][payload['batch']]['item']
        assignment = active['coding_assignment']
        assert assignment['status'] == 'sent' and assignment['binding'] == binding
        assert active['assignment_renewals'][0]['lane_retry_used'] is retry
        assert active['assignment_renewals'][0]['lane_retry_used_at_send'] is retry
        return payload, binding, assignment['id']


async def second_renewal_fixture(r, monkeypatch):
    original_prepare = prepare

    def app_prepare(*args):
        contract, packet = original_prepare(*args)
        contract['plane_id'] = 'APP-112'
        contract['model'].update(required_model='gpt-6-sol', provider='openai-codex', reasoning_effort='high')
        packet.update(plane_id='APP-112', task_id='APP-112', scope_id='APP-112')
        r.task['worker_route']['plane_id'] = 'APP-112'
        return contract, packet

    monkeypatch.setattr(sys.modules[__name__], 'prepare', app_prepare)
    payload, binding, reference = await sent_recovery_renewal(r, monkeypatch)
    home = r.tmp / 'diggr-coding'
    home.mkdir()
    assert dc.accept_coding_assignment('SYSTEM171_ASSIGN ' + reference, 'first-session', home)
    with r.guard.transaction() as rows:
        batch = rows.grants['batches'][payload['batch']]
        active = batch['item']
        # The fixture's former coordinator has exited; no executor was registered.
        active['coding_assignment']['binding']['process']['identity'] = dict(
            pid=99999998, start='exited-coordinator', executable='/usr/bin/hermes')
        r.clock[0] = dc.coding_assignment_deadline(batch) + 1
        contract = batch['manifest']['todos'][0]['contract']
        source, descriptor = renewal_prompt_evidence(
            contract['resources']['artifacts'], payload['batch'], active, contract)
        prompt = Path(descriptor['prompt']['path'])
        prompt.write_bytes(('Executor instructions\n' + item.onboarding_payload()[0] +
                            '\nImplement the confirmed APP-112 fixture.\nGrüße 🎧\n').encode())
        descriptor['prompt']['sha256'] = dc.digest(prompt)
        Path(source['path']).write_text(json.dumps(descriptor))
        source['sha256'] = dc.digest(source['path'])
    return payload, home, source, descriptor


@pytest.mark.asyncio
@pytest.mark.parametrize('same_coordinator', [True, False])
async def test_second_renewal_distinguishes_live_coordinator_from_old_executor(route, monkeypatch, same_coordinator):
    r = route
    payload, _, source, _ = await second_renewal_fixture(r, monkeypatch)
    binding = dict(workspace='fixture', surface='fixture', process=dict(identity=dict(
        pid=os.getpid(), start='canonical-coordinator', executable=sys.executable)))
    with r.guard.transaction() as rows:
        rows.grants['batches'][payload['batch']]['item']['coding_assignment']['binding'] = binding
    real_absent = dc.process_absent
    monkeypatch.setattr(dc, 'process_absent', lambda identity, **kw:
                        False if identity == binding['process']['identity'] else real_absent(identity, **kw))
    monkeypatch.setattr(dc, 'preflight_coding_coordinator', lambda:
                        binding if same_coordinator else dict(binding, surface='different'))
    if same_coordinator:
        preview = item.control(r.guard, r.identity, dict(
            payload, operation='item_assignment_renew_preview', evidence=source))
        assert preview['core']['renewal_number'] == 2
    else:
        with pytest.raises(ValueError):
            item.control(r.guard, r.identity, dict(
                payload, operation='item_assignment_renew_preview', evidence=source))


@pytest.mark.asyncio
async def test_second_renewal_owner_preview_keeps_final_window_unspent(route, monkeypatch):
    r = route
    payload, _, source, descriptor = await second_renewal_fixture(r, monkeypatch)
    with r.guard.transaction(read_only=True) as rows:
        before = copy.deepcopy(rows.grants['batches'][payload['batch']])
    preview = item.control(r.guard, r.identity, dict(
        payload, operation='item_assignment_renew_preview', evidence=source))
    assert preview['core']['renewal_number'] == 2
    assert preview['core']['prompt_binding'] == dict(descriptor, evidence=source)
    assert await r.runner._diggr_accept(await r.event(preview['next_owner_command'])) is False
    with r.guard.transaction(read_only=True) as rows:
        batch = rows.grants['batches'][payload['batch']]
        active = batch['item']
        assert len(active['assignment_renewals']) == 2
        assert active['assignment_renewals'][0] == before['item']['assignment_renewals'][0]
        assert active['coding_assignment_history'][-1] == before['item']['coding_assignment']
        assert active['coding_assignment']['status'] == 'pending'
        assert 'activated_at' not in active['assignment_renewals'][-1]
        assert batch['tasks'] == {} and batch['deadline'] is None
        assert all(active[k] == before['item'][k] for k in
                   ('budget', 'work_seconds', 'worktree', 'branch', 'base', 'target'))
    with pytest.raises(ValueError):
        item.control(r.guard, r.identity, dict(
            payload, operation='item_assignment_renew_preview', evidence=source))


async def confirmed_second_renewal(r, monkeypatch):
    payload, home, source, descriptor = await second_renewal_fixture(r, monkeypatch)
    preview = item.control(r.guard, r.identity, dict(
        payload, operation='item_assignment_renew_preview', evidence=source))
    assert await r.runner._diggr_accept(await r.event(preview['next_owner_command'])) is False
    return payload, home, source, descriptor


@pytest.mark.asyncio
async def test_second_renewal_delivery_activates_once_then_native_accepts(route, monkeypatch):
    r = route
    payload, home, _, _ = await confirmed_second_renewal(r, monkeypatch)
    r.clock[0] += 900  # Waiting for delivery does not spend the 600-second window.
    binding = dict(workspace='fixture', surface='fixture', process=dict(identity=dict(pid=os.getpid())))
    monkeypatch.setattr(dc, 'preflight_coding_coordinator', lambda: binding)
    sends = mock_assignment_send(monkeypatch)
    delivery.deliver_coding_assignments(r.guard, r.identity)
    assert len(sends) == 1

    with r.guard.transaction(read_only=True) as rows:
        batch = rows.grants['batches'][payload['batch']]
        active = batch['item']
        reference = active['coding_assignment']['id']
        assert active['assignment_renewals'][-1]['activated_at'] == r.clock[0]
        assert dc.coding_assignment_deadline(batch) == r.clock[0] + 600
    assert dc.accept_coding_assignment('SYSTEM171_ASSIGN ' + reference, 'second-session', home)
    assert not dc.accept_coding_assignment('SYSTEM171_ASSIGN ' + reference, 'second-session', home)
    delivery.deliver_coding_assignments(r.guard, r.identity)
    assert len(sends) == 1


async def sent_second_renewal(r, monkeypatch):
    payload, home, source, descriptor = await confirmed_second_renewal(r, monkeypatch)
    binding = dict(workspace='fixture', surface='fixture', process=dict(identity=dict(pid=os.getpid())))
    monkeypatch.setattr(dc, 'preflight_coding_coordinator', lambda: binding)
    sends = mock_assignment_send(monkeypatch)
    delivery.deliver_coding_assignments(r.guard, r.identity)
    assert len(sends) == 1
    with r.guard.transaction(read_only=True) as rows:
        reference = rows.grants['batches'][payload['batch']]['item']['coding_assignment']['id']
    return payload, home, reference, source, descriptor


@pytest.mark.asyncio
@pytest.mark.parametrize('consumer', ['explicit', 'automatic'])
async def test_second_renewal_registration_uses_exact_approved_prompt(route, monkeypatch, consumer):
    r = route
    _, home, reference, _, descriptor = await sent_second_renewal(r, monkeypatch)
    expected = Path(descriptor['prompt']['path']).read_bytes()
    if consumer == 'automatic':
        observed = []
        monkeypatch.setattr(dc, 'start_assigned_worker', lambda guard, row: observed.append(row))
        dc.continue_received_coding_assignment('SYSTEM171_ASSIGN ' + reference, 'second-session', home)
        assert len(observed) == 1
        registered = observed[0]
    else:
        dc.accept_coding_assignment('SYSTEM171_ASSIGN ' + reference, 'second-session', home)
        _, registered = dc.register_assigned_worker(reference, 'second-session', home, expected.decode())
    assert registered['worker_route']['argv'][-1].encode() == expected
    assert registered['assignment_prompt_sha256'] == descriptor['prompt']['sha256']


@pytest.mark.asyncio
async def test_second_renewal_rejects_caller_prompt_before_artifacts(route, monkeypatch):
    r = route
    _, home, reference, _, descriptor = await sent_second_renewal(r, monkeypatch)
    dc.accept_coding_assignment('SYSTEM171_ASSIGN ' + reference, 'second-session', home)
    before = r.guard.path.read_bytes()
    root = Path(descriptor['prompt']['path']).parent
    files = set(root.rglob('*'))
    with pytest.raises(ValueError, match='prompt'):
        dc.register_assigned_worker(reference, 'second-session', home, 'unapproved replacement')
    assert r.guard.path.read_bytes() == before
    assert set(root.rglob('*')) == files


@pytest.mark.asyncio
@pytest.mark.parametrize('boundary', ['start', 'ticket', 'after_preflight'])
async def test_second_renewal_registered_source_drift_blocks_start(route, monkeypatch, boundary):
    r = route
    _, home, reference, _, descriptor = await sent_second_renewal(r, monkeypatch)
    prompt = Path(descriptor['prompt']['path']).read_text()
    dc.accept_coding_assignment('SYSTEM171_ASSIGN ' + reference, 'second-session', home)
    _, registered = dc.register_assigned_worker(reference, 'second-session', home, prompt)
    source = Path(descriptor['source']['path'])
    sends = []
    monkeypatch.setattr(dc, 'cmux_send', lambda *args: sends.append(args))
    if boundary == 'after_preflight':
        original = dc.preflight_non_cmm
        def changed(*args, **kwargs):
            result = original(*args, **kwargs)
            source.write_bytes(b'changed after preflight')
            return result
        monkeypatch.setattr(dc, 'preflight_non_cmm', changed)
    else:
        source.write_bytes(b'changed after registration')
    before = r.guard.path.read_bytes()
    with pytest.raises(ValueError, match='hash|prompt'):
        if boundary == 'ticket':
            with r.guard.transaction() as rows:
                dc.validate_ticket(rows, dc.worker_ticket(registered))
        else:
            dc.start_assigned_worker(r.guard, registered)
    assert not sends
    assert r.guard.path.read_bytes() == before


@pytest.mark.asyncio
@pytest.mark.parametrize('effect', ['worker_route', 'claim', 'unknown_effect', 'worker_pid', 'visible_sent'])
async def test_second_renewal_preview_rejects_any_worker_effect(route, monkeypatch, effect):
    r = route
    payload, _, source, _ = await second_renewal_fixture(r, monkeypatch)
    with r.guard.transaction() as rows:
        active = rows.grants['batches'][payload['batch']]['item']
        if effect == 'worker_route':
            active['worker_route'] = dict(worktree=active['worktree'])
        elif effect == 'claim':
            active['coding_assignment']['claim_phase'] = 'claimed'
        elif effect == 'unknown_effect':
            active['coding_assignment']['effect_status'] = 'unknown'
        else:
            active['coding_assignment'][effect] = 42 if effect == 'worker_pid' else True
    before = r.guard.path.read_bytes()
    with pytest.raises(ValueError):
        item.control(r.guard, r.identity, dict(
            payload, operation='item_assignment_renew_preview', evidence=source))
    assert r.guard.path.read_bytes() == before


@pytest.mark.asyncio
@pytest.mark.parametrize('boundary', ['owner', 'delivery', 'automatic', 'explicit'])
@pytest.mark.parametrize('changed', ['source', 'prompt', 'evidence'])
async def test_second_renewal_byte_drift_fails_closed(route, monkeypatch, boundary, changed):
    r = route
    if boundary == 'owner':
        payload, home, source, descriptor = await second_renewal_fixture(r, monkeypatch)
        preview = item.control(r.guard, r.identity, dict(
            payload, operation='item_assignment_renew_preview', evidence=source))
    elif boundary == 'delivery':
        payload, home, source, descriptor = await confirmed_second_renewal(r, monkeypatch)
    else:
        payload, home, reference, source, descriptor = await sent_second_renewal(r, monkeypatch)
        if boundary == 'explicit':
            dc.accept_coding_assignment('SYSTEM171_ASSIGN ' + reference, 'second-session', home)
    original_prompt = Path(descriptor['prompt']['path']).read_text()
    path = Path((source if changed == 'evidence' else descriptor[changed])['path'])
    path.write_bytes(path.read_bytes() + b'\n')
    before = r.guard.path.read_bytes()
    if boundary == 'delivery':
        sends = mock_assignment_send(monkeypatch)
        delivery.deliver_coding_assignments(r.guard, r.identity)
        assert not sends
    else:
        with pytest.raises(ValueError):
            if boundary == 'owner':
                await r.runner._diggr_accept(await r.event(preview['next_owner_command']))
            elif boundary == 'automatic':
                dc.continue_received_coding_assignment('SYSTEM171_ASSIGN ' + reference, 'second-session', home)
            else:
                dc.register_assigned_worker(reference, 'second-session', home, original_prompt)
    assert r.guard.path.read_bytes() == before


@pytest.mark.asyncio
@pytest.mark.parametrize('boundary', ['owner', 'delivery'])
@pytest.mark.parametrize('change', ['budget', 'scope', 'target', 'work', 'task', 'candidate', 'uncertain', 'live', 'occupied'])
async def test_second_renewal_state_drift_fails_closed(route, monkeypatch, boundary, change):
    r = route
    if boundary == 'owner':
        payload, _, source, _ = await second_renewal_fixture(r, monkeypatch)
        preview = item.control(r.guard, r.identity, dict(
            payload, operation='item_assignment_renew_preview', evidence=source))
    else:
        payload, _, _, _ = await confirmed_second_renewal(r, monkeypatch)
    with r.guard.transaction() as rows:
        batch = rows.grants['batches'][payload['batch']]
        active = batch['item']
        if change == 'budget': active['budget']['todo_seconds'] += 1
        if change == 'scope': batch['manifest']['todos'][0]['contract']['scope'] += ' expanded'
        if change == 'target': active['target']['surface'] = 'different'
        if change == 'work': active['work_seconds'] = 1
        if change == 'task': batch['tasks'][active['issue']] = 'unexpected'
        if change == 'candidate': active['candidate'] = dict(id='unexpected')
        if change == 'uncertain':
            previous = (active['coding_assignment'] if boundary == 'owner' else active['coding_assignment_history'][-1])
            previous.update(status='uncertain', session=None, accepted_at=None)
    if change == 'live': monkeypatch.setattr(dc, 'process_absent', lambda *a, **kw: False)
    if change == 'occupied':
        def occupied(*a, **kw): raise ValueError('occupied executor')
        monkeypatch.setattr(dc, 'verify_visible_target', occupied)
    before = r.guard.path.read_bytes()
    if boundary == 'owner':
        with pytest.raises(ValueError):
            await r.runner._diggr_accept(await r.event(preview['next_owner_command']))
    else:
        sends = mock_assignment_send(monkeypatch)
        delivery.deliver_coding_assignments(r.guard, r.identity)
        assert not sends
    assert r.guard.path.read_bytes() == before


@pytest.mark.asyncio
@pytest.mark.parametrize('change', ['missing', 'expired', 'replaced', 'not_direct', 'reused_event'])
async def test_second_renewal_requires_fresh_direct_preview(route, monkeypatch, change):
    r = route
    payload, _, source, _ = await second_renewal_fixture(r, monkeypatch)
    preview = item.control(r.guard, r.identity, dict(
        payload, operation='item_assignment_renew_preview', evidence=source))
    event = await r.event(preview['next_owner_command'])
    if change == 'missing':
        with r.guard.transaction() as rows:
            rows.grants['batches'][payload['batch']]['item'].pop('assignment_renew_preview')
    if change == 'expired': r.clock[0] += 300
    if change == 'replaced':
        item.control(r.guard, r.identity, dict(payload, operation='item_assignment_renew_preview', evidence=source))
    if change == 'not_direct': event.internal = True
    before = r.guard.path.read_bytes()
    with pytest.raises(ValueError):
        if change == 'reused_event':
            with r.guard.transaction(read_only=True) as rows:
                old_event = rows.grants['batches'][payload['batch']]['item']['assignment_renewals'][0]['native_owner_event']
            await item.owner_second_assignment_renew(r.runner, event, r.guard, r.identity, old_event,
                                                     payload['batch'], payload['issue'], preview['sha256'])
        else:
            await r.runner._diggr_accept(event)
    assert r.guard.path.read_bytes() == before


@pytest.mark.asyncio
@pytest.mark.parametrize('change', ['budget', 'work', 'scope', 'target', 'deadline'])
async def test_second_renewal_start_rechecks_bounds(route, monkeypatch, change):
    r = route
    payload, home, reference, _, descriptor = await sent_second_renewal(r, monkeypatch)
    dc.accept_coding_assignment('SYSTEM171_ASSIGN ' + reference, 'second-session', home)
    _, registered = dc.register_assigned_worker(reference, 'second-session', home,
                                                Path(descriptor['prompt']['path']).read_text())
    with r.guard.transaction() as rows:
        batch = rows.grants['batches'][payload['batch']]
        active = batch['item']
        if change == 'budget': active['budget']['todo_seconds'] += 1
        if change == 'work': active['work_seconds'] = 1
        if change == 'scope': batch['manifest']['todos'][0]['contract']['scope'] += ' expanded'
        if change == 'target': active['target']['surface'] = 'different'
        if change == 'deadline': r.clock[0] = dc.coding_assignment_deadline(batch)
    before = r.guard.path.read_bytes()
    sends = []
    monkeypatch.setattr(dc, 'cmux_send', lambda *a: sends.append(a))
    with pytest.raises(ValueError):
        dc.start_assigned_worker(r.guard, registered)
    assert not sends and r.guard.path.read_bytes() == before


@pytest.mark.asyncio
async def test_second_renewal_preview_requires_complete_final_onboarding(route, monkeypatch):
    r = route
    payload, _, source, descriptor = await second_renewal_fixture(r, monkeypatch)
    path = Path(descriptor['prompt']['path'])
    path.write_text('Implement without the mandatory onboarding.')
    descriptor['prompt']['sha256'] = dc.digest(path)
    Path(source['path']).write_text(json.dumps(descriptor))
    source['sha256'] = dc.digest(source['path'])
    before = r.guard.path.read_bytes()
    with pytest.raises(ValueError, match='onboarding'):
        item.control(r.guard, r.identity, dict(
            payload, operation='item_assignment_renew_preview', evidence=source))
    assert r.guard.path.read_bytes() == before


@pytest.mark.asyncio
async def test_second_renewal_post_registration_candidate_blocks_ticket(route, monkeypatch):
    r = route
    _, home, reference, _, descriptor = await sent_second_renewal(r, monkeypatch)
    dc.accept_coding_assignment('SYSTEM171_ASSIGN ' + reference, 'second-session', home)
    _, registered = dc.register_assigned_worker(reference, 'second-session', home,
                                                Path(descriptor['prompt']['path']).read_text())
    with r.guard.transaction(read_only=True) as rows:
        rows.grants['batches'][registered['owner_batch']]['item']['candidate'] = dict(id='unexpected')
        with pytest.raises(ValueError, match='second-renewal'):
            dc.validate_ticket(rows, dc.worker_ticket(registered))


@pytest.mark.asyncio
@pytest.mark.parametrize('boundary', ['registered', 'dispatch', 'claimed'])
@pytest.mark.parametrize('change', [
    'candidate', 'candidate_history', 'item_effect', 'assignment_effect', 'assignment_claim',
    'row_claim', 'child', 'phase', 'competing_worktree', 'competing_target', 'competing_item',
    'repo', 'scope', 'target', 'budget', 'row_budget', 'work', 'row_route', 'row_scope',
    'source', 'prompt', 'missing_seal', 'receipt_missing', 'claim_deadline', 'row_effect', 'row_result',
    'renewal_markers',
])
async def test_second_renewal_post_registration_drift_at_every_start_boundary(route, monkeypatch, boundary, change):
    from contextlib import contextmanager
    r = route
    _, home, reference, _, descriptor = await sent_second_renewal(r, monkeypatch)
    dc.accept_coding_assignment('SYSTEM171_ASSIGN ' + reference, 'second-session', home)
    _, registered = dc.register_assigned_worker(reference, 'second-session', home,
                                                Path(descriptor['prompt']['path']).read_text())
    original_transaction = r.guard.transaction

    def drift(rows):
        row = rows[registered['task']]
        batch = rows.grants['batches'][registered['owner_batch']]
        active, assignment = batch['item'], batch['item']['coding_assignment']
        if change == 'candidate': active['candidate'] = dict(id='unexpected')
        if change == 'candidate_history': active['candidate_history'] = [dict(id='unexpected')]
        if change == 'item_effect': active['effect_status'] = 'unknown'
        if change == 'assignment_effect': assignment['effect_status'] = 'unknown'
        if change == 'assignment_claim': assignment.update(claim_phase='claimed', worker_pid=os.getpid() + 1)
        if change == 'row_claim': row['worker_pid'] = os.getpid() + 1
        if change == 'child': row['child_identity'] = dict(pid=99999, start='unexpected')
        if change == 'phase': active['phase'] = 'archived'
        if change in {'competing_worktree', 'competing_target'}:
            route = dict(worktree=active['worktree'], visible_target=active['target'])
            route['visible_target' if change == 'competing_worktree' else 'worktree'] = 'different'
            rows['conflicting-task'] = dict(task='conflicting-task', deadline=r.clock[0] + 1000,
                                            worker_route=route)
        if change == 'competing_item':
            rows.grants['batches']['conflicting-batch'] = dict(item=dict(phase='prepared'))
        if change == 'repo': active['repo'] += '-different'
        if change == 'scope': batch['manifest']['todos'][0]['contract']['scope'] += ' expanded'
        if change == 'target': active['target']['surface'] = 'different'
        if change == 'budget': active['budget']['todo_seconds'] += 1
        if change == 'row_budget': row['policy']['corrections'] += 1
        if change == 'work': active['work_seconds'] = 1
        if change == 'row_route': row['worker_route']['branch'] += '-different'
        if change == 'row_scope': row['scope'] += ' expanded'
        if change in {'source', 'prompt'}: Path(descriptor[change]['path']).write_text('changed bytes')
        if change == 'missing_seal': row.pop('second_renewal_start_sha256', None)
        if change == 'receipt_missing':
            row.pop('receipt_sha256', None)
            row.pop('non_cmm_receipt_sha256', None)
            if boundary == 'registered':
                Path(row['worker_route']['receipt']).write_text('{}')
                row['receipt_sha256'] = '0' * 64
        if change == 'claim_deadline': row['claim_deadline'] = row.get('claim_deadline', r.clock[0]) + 1
        if change == 'row_effect': row['effect_status'] = 'observed'
        if change == 'row_result': row['worker_result'] = dict(exit_code=0, generation=row['generation'])
        if change == 'renewal_markers':
            row.pop('assignment_renewal_sha256')
            active.pop('assignment_renewals')
            assignment.pop('second_renewal')
            assignment.pop('prompt_binding')

    if boundary == 'registered':
        with original_transaction(read_only=True) as rows:
            dc.validate_ticket(rows, dc.worker_ticket(registered))  # Own registered task is valid.
            drift(rows)
            with pytest.raises(ValueError):
                dc.validate_ticket(rows, dc.worker_ticket(registered))
        return

    injected = []
    @contextmanager
    def transaction(*, read_only=False):
        with original_transaction(read_only=read_only) as rows:
            yield rows
        # Inject only AFTER the genuine send reservation / native claim committed,
        # before the next transaction. Never mutate the worker's captured own claim.
        if not read_only and not injected:
            with original_transaction() as current:
                own = current[registered['task']]
                assignment = current.grants['batches'][registered['owner_batch']]['item']['coding_assignment']
                reached = (assignment['status'] == 'sending_worker' if boundary == 'dispatch'
                           else own.get('claim_phase') == 'claimed')
                if reached:
                    injected.append(True)
                    drift(current)

    monkeypatch.setattr(r.guard, 'transaction', transaction)
    sends = []
    monkeypatch.setattr(dc, 'cmux_send', lambda *args: sends.append(args))
    if boundary == 'dispatch':
        with pytest.raises(ValueError):
            dc.start_assigned_worker(r.guard, registered)
        assert injected == [True] and sends == []
        return

    assert dc.start_assigned_worker(r.guard, registered)['sent'] is True
    assert len(sends) == 1
    monkeypatch.setattr(dc, 'verify_visible_claim', lambda row: None)  # Disposable cMux boundary.
    monkeypatch.setattr(dc, 'process_identity', lambda pid: dict(pid=pid, start='fixture'))
    spawned = []
    def reject_spawn(argv, *args, **kwargs):
        spawned.append(argv)
        raise AssertionError('executor must not spawn after conflicting evidence')
    monkeypatch.setattr(item, 'spawn_item_child', reject_spawn)
    with pytest.raises(ValueError):
        dc.main(['--home', r.identity['home'], 'worker', '--payload-json',
                 json.dumps(dc.worker_ticket(registered))])
    assert injected == [True] and spawned == []


@pytest.mark.asyncio
async def test_second_renewal_exact_prompt_reaches_single_native_spawn(route, monkeypatch):
    r = route
    _, home, reference, _, descriptor = await sent_second_renewal(r, monkeypatch)
    exact = Path(descriptor['prompt']['path']).read_bytes()
    _bind_disposable_codex_executable(r, monkeypatch, output='isolated executor fixture result')
    monkeypatch.setenv('HERMES_HOME', str(r.tmp))
    cmux_attempts = _guard_real_cmux_subprocesses(monkeypatch)
    dc.accept_coding_assignment('SYSTEM171_ASSIGN ' + reference, 'second-session', home)
    _, registered = dc.register_assigned_worker(reference, 'second-session', home, exact.decode())
    assert registered['worker_route']['argv'][-1].encode() == exact
    sends = []
    monkeypatch.setattr(dc, 'cmux_send', lambda binding, command: sends.append(command))
    assert dc.start_assigned_worker(r.guard, registered)['sent'] is True
    assert len(sends) == 1
    row = r.guard.get(registered['task'])
    ticket = dc.worker_ticket(row)
    monkeypatch.setattr(dc, 'verify_visible_claim', lambda row: None)  # Disposable terminal boundary.
    result, spawned = _run_sealed_fixture_worker(
        r.guard, ticket, row['worker_route']['argv'], row['artifact'])
    assert result == 0
    assert len(spawned) == 1
    assert Path(row['artifact']).read_text() == 'isolated executor fixture result'
    finished = r.guard.get(registered['task'])
    proof = finished['worker_supervision']
    assert proof['state'] == 'exited' and proof['accounted_seconds'] > 0
    assert finished['child_exited'] is True and finished['worker_result']['exit_code'] == 0
    assert finished['worker_result']['evidence']['sha256'] == dc.digest(row['artifact'])
    assert dc.worker_operational_exit(finished, ticket['generation'])
    with r.guard.transaction(read_only=True) as rows:
        assert rows.grants['batches'][registered['owner_batch']]['item']['work_seconds'] == proof['accounted_seconds']
    with pytest.raises(ValueError):
        dc.main(['--home', r.identity['home'], 'worker', '--payload-json', json.dumps(ticket)])
    assert len(spawned) == 1
    assert cmux_attempts == []


@pytest.mark.asyncio
@pytest.mark.parametrize('boundary', ['delivery', 'acceptance', 'third'])
async def test_second_renewal_expiry_never_opens_a_third_window(route, monkeypatch, boundary):
    r = route
    if boundary == 'delivery':
        payload, home, source, _ = await confirmed_second_renewal(r, monkeypatch)
        r.clock[0] += owner.BUDGETS['batch_seconds']
        sends = mock_assignment_send(monkeypatch)
        delivery.deliver_coding_assignments(r.guard, r.identity)
        assert not sends
        return
    payload, home, reference, source, _ = await sent_second_renewal(r, monkeypatch)
    if boundary == 'third':
        dc.accept_coding_assignment('SYSTEM171_ASSIGN ' + reference, 'second-session', home)
    with r.guard.transaction(read_only=True) as rows:
        r.clock[0] = dc.coding_assignment_deadline(rows.grants['batches'][payload['batch']])
    before = r.guard.path.read_bytes()
    with pytest.raises(ValueError):
        if boundary == 'acceptance':
            dc.accept_coding_assignment('SYSTEM171_ASSIGN ' + reference, 'second-session', home)
        else:
            item.control(r.guard, r.identity, dict(
                payload, operation='item_assignment_renew_preview', evidence=source))
    assert r.guard.path.read_bytes() == before


@pytest.mark.asyncio
@pytest.mark.parametrize('boundary', ['delivery', 'acceptance'])
async def test_second_renewal_current_assignment_unknown_effect_blocks(route, monkeypatch, boundary):
    r = route
    if boundary == 'delivery':
        payload, home, _, _ = await confirmed_second_renewal(r, monkeypatch)
    else:
        payload, home, reference, _, _ = await sent_second_renewal(r, monkeypatch)
    with r.guard.transaction() as rows:
        rows.grants['batches'][payload['batch']]['item']['coding_assignment']['effect_status'] = 'unknown'
    before = r.guard.path.read_bytes()
    if boundary == 'delivery':
        sends = mock_assignment_send(monkeypatch)
        delivery.deliver_coding_assignments(r.guard, r.identity)
        assert not sends
    else:
        with pytest.raises(ValueError):
            dc.accept_coding_assignment('SYSTEM171_ASSIGN ' + reference, 'second-session', home)
    assert r.guard.path.read_bytes() == before


async def uncertain_recovery_assignment(r, monkeypatch, *, live_process=False):
    contract, payload, blocker, _ = await preparation_assignment_recovery_fixture(r, monkeypatch)
    token = dc.EVENT_CONTEXT.set(dict(
        identity=r.identity, direct_owner_event='fixture-owner-event', native_wake=None))
    try:
        receipt = item.control(r.guard, r.identity, dict(
            payload, operation='item_preparation_assignment_recover', evidence=blocker))
    finally:
        dc.EVENT_CONTEXT.reset(token)
    process = (dict(pid=os.getpid(), start='fixture-live-start', executable='/usr/bin/hermes')
               if live_process else
               dict(pid=99999999, start='old-process-start', executable='/usr/bin/hermes'))
    if live_process:
        monkeypatch.setattr(dc, 'process_identity', lambda pid: process)
    with r.guard.transaction() as rows:
        assignment = rows.grants['batches'][payload['batch']]['item']['coding_assignment']
        assignment.update(status='uncertain', offered_at=r.clock[0],
                          binding=dict(workspace='fixture', surface='fixture',
                                       process=dict(identity=process)))
    r.clock[0] = receipt['assignment_deadline'] + 1
    return contract, payload, receipt


def mock_assignment_send(monkeypatch):
    sends = []
    real_run = subprocess.run

    def run(args, *positional, **keywords):
        if args[:3] == [dc.CMUX, 'rpc', 'terminal.paste']:
            sends.append(args)
            return SimpleNamespace(stdout=json.dumps(dict(
                workspace_id='fixture', surface_id='fixture', submitted=True, delivery='sent')))
        return real_run(args, *positional, **keywords)

    monkeypatch.setattr(subprocess, 'run', run)
    return sends


@pytest.mark.asyncio
async def test_uncertain_recovered_assignment_renews_after_process_exit(route, monkeypatch):
    r = route
    _, payload, recovery = await uncertain_recovery_assignment(r, monkeypatch)
    with r.guard.transaction(read_only=True) as rows:
        batch = rows.grants['batches'][payload['batch']]
        old_assignment = copy.deepcopy(batch['item']['coding_assignment'])
        transport = copy.deepcopy(batch['native_transport'])
        budget = copy.deepcopy(batch['manifest']['budgets'])
    monkeypatch.setattr(delivery, 'command_transport', lambda *args: transport)
    preview = item.control(r.guard, r.identity, dict(payload, operation='item_assignment_renew_preview'))
    assert preview['core']['recovery_sha256'] == owner.binding_hash(recovery)
    assert await r.runner._diggr_accept(await r.event(preview['next_owner_command'])) is False
    with r.guard.transaction(read_only=True) as rows:
        batch = rows.grants['batches'][payload['batch']]
        active = batch['item']
        new_assignment = active['coding_assignment']
        assert len(active['coding_assignment_history']) == 2
        assert active['coding_assignment_history'][-1] == old_assignment
        assert new_assignment['status'] == 'pending' and new_assignment['id'] != old_assignment['id']
        assert active['assignment_renewals'][0]['old_assignment'] == old_assignment['id']
        assert active['assignment_renewals'][0]['new_assignment'] == new_assignment['id']
        assert batch['tasks'] == {} and active.get('task') is None and active['work_seconds'] == 0
        assert batch['manifest']['budgets'] == budget and batch['native_transport'] == transport
        old_id = old_assignment['id']
    coding_home = r.tmp / 'diggr-coding'
    coding_home.mkdir()
    monkeypatch.setattr(dc, 'preflight_coding_coordinator', lambda: dict(
        workspace='fixture', surface='fixture', process=dict(identity=dict(pid=os.getpid()))))
    sealed = r.guard.path.read_bytes()
    with pytest.raises(ValueError, match='unknown or ambiguous'):
        dc.accept_coding_assignment('SYSTEM171_ASSIGN ' + old_id, 'late-session', coding_home)
    assert r.guard.path.read_bytes() == sealed
    with pytest.raises(ValueError):
        await r.runner._diggr_accept(await r.event(preview['next_owner_command']))
    assert r.guard.path.read_bytes() == sealed
    r.clock[0] = active['assignment_renewals'][0]['assignment_deadline'] + 1
    sends = mock_assignment_send(monkeypatch)
    delivery.deliver_coding_assignments(r.guard, r.identity)
    assert len(sends) == 1
    with r.guard.transaction(read_only=True) as rows:
        active = rows.grants['batches'][payload['batch']]['item']
        assert active['coding_assignment']['status'] == 'sent'
        assert active['assignment_renewals'][0]['activated_at'] == r.clock[0]


@pytest.mark.asyncio
async def test_uncertain_recovered_assignment_rejects_live_bound_process(route, monkeypatch):
    r = route
    _, payload, _ = await uncertain_recovery_assignment(r, monkeypatch, live_process=True)
    sealed = r.guard.path.read_bytes()
    with pytest.raises(ValueError, match='process still active or exit unproved'):
        item.control(r.guard, r.identity, dict(payload, operation='item_assignment_renew_preview'))
    assert r.guard.path.read_bytes() == sealed


@pytest.mark.asyncio
async def test_uncertain_recovered_renewal_rechecks_process_at_owner_command(route, monkeypatch):
    r = route
    _, payload, _ = await uncertain_recovery_assignment(r, monkeypatch)
    with r.guard.transaction(read_only=True) as rows:
        transport = copy.deepcopy(rows.grants['batches'][payload['batch']]['native_transport'])
    monkeypatch.setattr(delivery, 'command_transport', lambda *args: transport)
    preview = item.control(r.guard, r.identity, dict(payload, operation='item_assignment_renew_preview'))
    checks = []

    def process_exit_recheck(identity, *, original_process_only=False):
        checks.append((identity, original_process_only))
        return len(checks) == 1

    monkeypatch.setattr(dc, 'process_absent', process_exit_recheck)
    sealed = r.guard.path.read_bytes()
    with pytest.raises(ValueError, match='before renewal'):
        await r.runner._diggr_accept(await r.event(preview['next_owner_command']))
    assert len(checks) == 2 and all(original for _, original in checks)
    assert r.guard.path.read_bytes() == sealed


@pytest.mark.asyncio
async def test_uncertain_recovered_renewal_rechecks_process_at_send(route, monkeypatch):
    r = route
    _, payload, _ = await uncertain_recovery_assignment(r, monkeypatch)
    with r.guard.transaction(read_only=True) as rows:
        transport = copy.deepcopy(rows.grants['batches'][payload['batch']]['native_transport'])
    monkeypatch.setattr(delivery, 'command_transport', lambda *args: transport)
    preview = item.control(r.guard, r.identity, dict(payload, operation='item_assignment_renew_preview'))
    assert await r.runner._diggr_accept(await r.event(preview['next_owner_command'])) is False
    monkeypatch.setattr(dc, 'process_absent', lambda *args, **kwargs: False)
    sends = mock_assignment_send(monkeypatch)
    delivery.deliver_coding_assignments(r.guard, r.identity)
    assert sends == []
    with r.guard.transaction(read_only=True) as rows:
        assert rows.grants['batches'][payload['batch']]['item']['coding_assignment']['status'] == 'pending'


@pytest.mark.asyncio
@pytest.mark.parametrize('change', [
    'accepted_at', 'session', 'task', 'registered_worker', 'direct_worker',
    'work', 'candidate', 'dirty_worktree', 'wrong_recovery', 'missing_offer'])
async def test_uncertain_recovered_assignment_rejects_possible_writer(route, monkeypatch, change):
    r = route
    _, payload, _ = await uncertain_recovery_assignment(r, monkeypatch)
    if change == 'dirty_worktree':
        Path(payload['worktree'], 'dirty.txt').write_text('changed\n')
    else:
        with r.guard.transaction() as rows:
            batch = rows.grants['batches'][payload['batch']]
            active = batch['item']
            assignment = active['coding_assignment']
            if change in {'accepted_at', 'session', 'task'}:
                assignment[change] = 'unexpected'
            elif change == 'registered_worker':
                rows['unexpected'] = dict(task='unexpected', owner_batch=payload['batch'],
                                          deadline=r.clock[0] + 100, identity=r.identity)
            elif change == 'direct_worker':
                rows['unexpected'] = dict(task='unexpected', direct_worker=True,
                                          deadline=r.clock[0] + 100, identity=r.identity,
                                          policy=dict(batch=payload['batch']))
            elif change == 'work':
                active['work_seconds'] = 1
            elif change == 'candidate':
                active['candidate'] = dict(id='unexpected')
            elif change == 'wrong_recovery':
                assignment['preparation_recovery_of'] = 'wrong'
            elif change == 'missing_offer':
                assignment.pop('offered_at')
    sealed = r.guard.path.read_bytes()
    with pytest.raises(ValueError):
        item.control(r.guard, r.identity, dict(payload, operation='item_assignment_renew_preview'))
    assert r.guard.path.read_bytes() == sealed


@pytest.mark.asyncio
async def test_recovered_assignment_renewal_uses_native_owner_command_once(route, monkeypatch):
    r = route
    _, payload, recovery = await accepted_recovery_assignment(r, monkeypatch)
    before = item.status(r.guard, r.identity, payload['batch'], payload['issue'])['item']
    with r.guard.transaction(read_only=True) as rows:
        original_transport = copy.deepcopy(rows.grants['batches'][payload['batch']]['native_transport'])
    monkeypatch.setattr(delivery, 'command_transport', lambda *args: original_transport)
    preview = item.control(r.guard, r.identity, dict(
        payload, operation='item_assignment_renew_preview'))
    assert preview['core']['recovery_sha256'] == owner.binding_hash(recovery)
    assert await r.runner._diggr_accept(await r.event(preview['next_owner_command'])) is False
    with r.guard.transaction(read_only=True) as rows:
        batch = rows.grants['batches'][payload['batch']]
        active = batch['item']
        assert batch['native_transport'] == original_transport
        assert batch['tasks'] == {} and batch['started_at'] is None
        assert active['coding_assignment_history'][-1] == before['coding_assignment']
        assert active['coding_assignment']['status'] == 'pending'
        assert active['preparation_assignment_recovery'] == recovery
        assert all(active[key] == before[key] for key in
                   ('worktree', 'branch', 'base', 'budget', 'work_seconds'))
        assert len(active['assignment_renewals']) == 1
        assert not any(key in active['assignment_renewals'][0] for key in
                       ('activated_at', 'previous_deadline', 'lane_retry_used',
                        'lane_retry_used_at_send'))
        first_deadline = active['assignment_renewals'][0]['assignment_deadline']
    r.clock[0] = first_deadline + 1
    sends = mock_assignment_send(monkeypatch)
    delivery.deliver_coding_assignments(r.guard, r.identity)
    assert len(sends) == 1
    with r.guard.transaction(read_only=True) as rows:
        active = rows.grants['batches'][payload['batch']]['item']
        assert active['coding_assignment']['status'] == 'sent'
        assert active['assignment_renewals'][0]['previous_deadline'] == recovery['assignment_deadline']
        assert active['assignment_renewals'][0]['assignment_deadline'] == r.clock[0] + 600
        assert active['assignment_renewals'][0]['lane_retry_used'] is False
        assert active['preparation_assignment_recovery'] == recovery
    with pytest.raises(ValueError):
        item.control(r.guard, r.identity, dict(payload, operation='item_assignment_renew_preview'))


@pytest.mark.asyncio
async def test_recovered_renewal_refuses_changed_worktree_before_delivery(route, monkeypatch):
    r = route
    _, payload, _ = await accepted_recovery_assignment(r, monkeypatch)
    with r.guard.transaction(read_only=True) as rows:
        transport = copy.deepcopy(rows.grants['batches'][payload['batch']]['native_transport'])
    monkeypatch.setattr(delivery, 'command_transport', lambda *args: transport)
    preview = item.control(r.guard, r.identity, dict(payload, operation='item_assignment_renew_preview'))
    assert await r.runner._diggr_accept(await r.event(preview['next_owner_command'])) is False
    Path(payload['worktree'], 'dirty.txt').write_text('changed\n')
    sends = mock_assignment_send(monkeypatch)
    delivery.deliver_coding_assignments(r.guard, r.identity)
    assert sends == []
    with r.guard.transaction(read_only=True) as rows:
        assert rows.grants['batches'][payload['batch']]['item']['coding_assignment']['status'] == 'pending'


@pytest.mark.asyncio
@pytest.mark.parametrize('activated', [False, True])
@pytest.mark.parametrize('change', [
    'batch_worker', 'direct_worker', 'batch_task', 'item_task', 'assignment_task',
    'work', 'candidate', 'candidate_history', 'started', 'batch_deadline',
    'budget', 'missing_recovery', 'recovery_binding', 'recovery_core', 'recovery_history',
    'preparation_delivery', 'recovered_blockers', 'renewal_binding', 'renewal_preview',
    'assignment_acceptance',
    'missing_renewal', 'phase'])
async def test_recovered_renewal_rechecks_effect_free_chain_at_send_reservation(
        route, monkeypatch, activated, change):
    r = route
    _, payload, _ = await accepted_recovery_assignment(r, monkeypatch)
    with r.guard.transaction(read_only=True) as rows:
        transport = copy.deepcopy(rows.grants['batches'][payload['batch']]['native_transport'])
    monkeypatch.setattr(delivery, 'command_transport', lambda *args: transport)
    preview = item.control(r.guard, r.identity, dict(payload, operation='item_assignment_renew_preview'))
    assert await r.runner._diggr_accept(await r.event(preview['next_owner_command'])) is False
    with r.guard.transaction() as rows:
        batch = rows.grants['batches'][payload['batch']]
        active = batch['item']
        renewal = active['assignment_renewals'][0]
        r.clock[0] = renewal['assignment_deadline'] + 1
        if activated:
            renewal.update(previous_deadline=active['preparation_assignment_recovery']['assignment_deadline'],
                           activated_at=r.clock[0], assignment_deadline=r.clock[0] + 600,
                           lane_retry_used=False)
        if change in {'batch_worker', 'direct_worker'}:
            rows['late-worker'] = dict(task='late-worker', identity=r.identity,
                deadline=r.clock[0] + 100,
                owner_batch=payload['batch'] if change == 'batch_worker' else None,
                direct_worker=change == 'direct_worker',
                policy=dict(batch=payload['batch']) if change == 'direct_worker' else {})
        elif change == 'batch_task':
            batch['tasks'][payload['issue']] = 'late-worker'
        elif change == 'item_task':
            active['task'] = 'late-worker'
        elif change == 'assignment_task':
            active['coding_assignment']['task'] = 'late-worker'
        elif change == 'work':
            active['work_seconds'] = 1
        elif change == 'candidate':
            active['candidate'] = dict(id='late-candidate')
        elif change == 'candidate_history':
            active['candidate_history'] = [dict(id='late-candidate')]
        elif change == 'started':
            batch['started_at'] = r.clock[0]
        elif change == 'batch_deadline':
            batch['deadline'] = r.clock[0] + 100
        elif change == 'budget':
            active['budget']['todo_seconds'] += 1
        elif change == 'missing_recovery':
            active.pop('preparation_assignment_recovery')
        elif change == 'recovery_binding':
            active['preparation_assignment_recovery']['new_assignment'] = 'wrong'
        elif change == 'recovery_core':
            active['preparation_assignment_recovery']['core']['branch'] = 'wrong'
        elif change == 'recovery_history':
            active['coding_assignment_history'][0]['reason'] = 'wrong'
        elif change == 'preparation_delivery':
            batch['preparation_delivery']['status'] = 'wrong'
        elif change == 'recovered_blockers':
            batch['recovered_preparation_blockers'] = []
        elif change == 'renewal_binding':
            renewal['old_assignment'] = 'wrong'
        elif change == 'renewal_preview':
            renewal['preview_sha256'] = '0' * 64
        elif change == 'assignment_acceptance':
            active['coding_assignment']['accepted_at'] = r.clock[0]
        elif change == 'missing_renewal':
            active.pop('assignment_renewals')
        elif change == 'phase':
            active['phase'] = 'correction_needed'
    sends = mock_assignment_send(monkeypatch)
    delivery.deliver_coding_assignments(r.guard, r.identity)
    assert sends == []
    with r.guard.transaction(read_only=True) as rows:
        assert rows.grants['batches'][payload['batch']]['item']['coding_assignment']['status'] == 'pending'


@pytest.mark.asyncio
async def test_already_activated_effect_free_recovery_renewal_can_send(route, monkeypatch):
    r = route
    _, payload, _ = await accepted_recovery_assignment(r, monkeypatch)
    with r.guard.transaction(read_only=True) as rows:
        transport = copy.deepcopy(rows.grants['batches'][payload['batch']]['native_transport'])
    monkeypatch.setattr(delivery, 'command_transport', lambda *args: transport)
    preview = item.control(r.guard, r.identity, dict(payload, operation='item_assignment_renew_preview'))
    assert await r.runner._diggr_accept(await r.event(preview['next_owner_command'])) is False
    with r.guard.transaction() as rows:
        active = rows.grants['batches'][payload['batch']]['item']
        renewal = active['assignment_renewals'][0]
        r.clock[0] = renewal['assignment_deadline'] + 1
        renewal.update(previous_deadline=active['preparation_assignment_recovery']['assignment_deadline'],
                       activated_at=r.clock[0], assignment_deadline=r.clock[0] + 600,
                       lane_retry_used=False)
    sends = mock_assignment_send(monkeypatch)
    delivery.deliver_coding_assignments(r.guard, r.identity)
    assert len(sends) == 1
    with r.guard.transaction(read_only=True) as rows:
        assert rows.grants['batches'][payload['batch']]['item']['coding_assignment']['status'] == 'sent'


@pytest.mark.asyncio
async def test_activated_recovery_renewal_expires_during_worktree_validation(route, monkeypatch):
    r = route
    _, payload, _ = await accepted_recovery_assignment(r, monkeypatch)
    with r.guard.transaction(read_only=True) as rows:
        transport = copy.deepcopy(rows.grants['batches'][payload['batch']]['native_transport'])
    monkeypatch.setattr(delivery, 'command_transport', lambda *args: transport)
    preview = item.control(r.guard, r.identity, dict(payload, operation='item_assignment_renew_preview'))
    assert await r.runner._diggr_accept(await r.event(preview['next_owner_command'])) is False
    with r.guard.transaction() as rows:
        active = rows.grants['batches'][payload['batch']]['item']
        renewal = active['assignment_renewals'][0]
        r.clock[0] = renewal['assignment_deadline'] + 1
        activated_at = r.clock[0]
        renewal.update(previous_deadline=active['preparation_assignment_recovery']['assignment_deadline'],
                       activated_at=activated_at, assignment_deadline=activated_at + 600,
                       lane_retry_used=False)
    real_matches = item.worktree_matches

    def slow_matches(contract, active):
        result = real_matches(contract, active)
        r.clock[0] = activated_at + 600
        return result

    monkeypatch.setattr(item, 'worktree_matches', slow_matches)
    sends = mock_assignment_send(monkeypatch)
    delivery.deliver_coding_assignments(r.guard, r.identity)
    assert sends == []
    with r.guard.transaction(read_only=True) as rows:
        active = rows.grants['batches'][payload['batch']]['item']
        assert active['coding_assignment']['status'] == 'blocked'
        assert active['coding_assignment']['reason'] == 'confirmed assignment deadline expired'
        assert active['assignment_renewals'][0]['assignment_deadline'] == activated_at + 600


@pytest.mark.asyncio
@pytest.mark.parametrize('change', [
    'deadline', 'final_validation', 'history', 'recovery', 'renewal',
    'retry', 'assignment_id', 'binding'])
async def test_recovered_renewal_rechecks_after_second_lane_before_paste(route, monkeypatch, change):
    r = route
    _, payload, _ = await accepted_recovery_assignment(r, monkeypatch)
    with r.guard.transaction(read_only=True) as rows:
        transport = copy.deepcopy(rows.grants['batches'][payload['batch']]['native_transport'])
    monkeypatch.setattr(delivery, 'command_transport', lambda *args: transport)
    preview = item.control(r.guard, r.identity, dict(payload, operation='item_assignment_renew_preview'))
    assert await r.runner._diggr_accept(await r.event(preview['next_owner_command'])) is False
    with r.guard.transaction(read_only=True) as rows:
        r.clock[0] = rows.grants['batches'][payload['batch']]['item']['assignment_renewals'][0]['assignment_deadline'] + 1
    binding = dict(workspace='fixture', surface='fixture', process=dict(pid=11))
    checks = [0]
    if change == 'final_validation':
        real_matches = item.worktree_matches
        matches = [0]

        def slow_final_match(contract, active):
            result = real_matches(contract, active)
            matches[0] += 1
            if matches[0] == 2:
                r.clock[0] = active['assignment_renewals'][0]['activated_at'] + 600
            return result

        monkeypatch.setattr(item, 'worktree_matches', slow_final_match)

    def lane():
        checks[0] += 1
        if checks[0] == 2:
            with r.guard.transaction() as rows:
                active = rows.grants['batches'][payload['batch']]['item']
                if change == 'deadline':
                    r.clock[0] = active['assignment_renewals'][0]['activated_at'] + 600
                elif change == 'history':
                    active['coding_assignment_history'][0]['reason'] = 'changed'
                elif change == 'recovery':
                    active['preparation_assignment_recovery']['core']['branch'] = 'changed'
                elif change == 'renewal':
                    active['assignment_renewals'][0]['preview_sha256'] = '0' * 64
                elif change == 'retry':
                    active['assignment_renewals'][0]['lane_retry_used'] = True
                elif change == 'assignment_id':
                    active['coding_assignment']['id'] = 'changed'
                elif change == 'binding':
                    active['coding_assignment']['binding'] = dict(binding, workspace='changed')
        return binding

    monkeypatch.setattr(dc, 'preflight_coding_coordinator', lane)
    sends = mock_assignment_send(monkeypatch)
    delivery.deliver_coding_assignments(r.guard, r.identity)
    assert checks[0] == 2 and sends == []
    with r.guard.transaction(read_only=True) as rows:
        active = rows.grants['batches'][payload['batch']]['item']
        assignment = active['coding_assignment']
        renewal = active['assignment_renewals'][0]
        assert renewal['assignment_deadline'] == renewal['activated_at'] + 600
        if change in {'deadline', 'final_validation'}:
            assert assignment['status'] == 'blocked'
            assert assignment['reason'] == 'confirmed assignment deadline expired'
            assert renewal['lane_retry_used'] is False
        elif change in {'assignment_id', 'binding'}:
            assert assignment['status'] == 'sending'
            assert (assignment['id'] == 'changed' if change == 'assignment_id'
                    else assignment['binding']['workspace'] == 'changed')
        else:
            assert assignment['status'] == 'pending'
            assert assignment.get('binding') is None and assignment.get('offered_at') is None


@pytest.mark.asyncio
@pytest.mark.parametrize('retry_after_expiry', [False, True])
async def test_recovered_renewal_changed_lane_before_send_keeps_original_window(
        route, monkeypatch, retry_after_expiry):
    r = route
    _, payload, _ = await accepted_recovery_assignment(r, monkeypatch)
    with r.guard.transaction(read_only=True) as rows:
        transport = copy.deepcopy(rows.grants['batches'][payload['batch']]['native_transport'])
    monkeypatch.setattr(delivery, 'command_transport', lambda *args: transport)
    preview = item.control(r.guard, r.identity, dict(payload, operation='item_assignment_renew_preview'))
    assert await r.runner._diggr_accept(await r.event(preview['next_owner_command'])) is False
    with r.guard.transaction(read_only=True) as rows:
        renewal = rows.grants['batches'][payload['batch']]['item']['assignment_renewals'][0]
        r.clock[0] = renewal['assignment_deadline'] + 1
    binding = dict(workspace='fixture', surface='fixture', process=dict(pid=11))
    changed = dict(workspace='other', surface='fixture', process=dict(pid=12))
    lanes = iter((binding, changed))
    monkeypatch.setattr(dc, 'preflight_coding_coordinator', lambda: next(lanes))
    sends = mock_assignment_send(monkeypatch)
    delivery.deliver_coding_assignments(r.guard, r.identity)
    assert sends == []
    with r.guard.transaction(read_only=True) as rows:
        active = rows.grants['batches'][payload['batch']]['item']
        assignment = active['coding_assignment']
        renewal = copy.deepcopy(active['assignment_renewals'][0])
        assert assignment['status'] == 'pending'
        assert assignment.get('binding') is None and assignment.get('offered_at') is None
        assert renewal['activated_at'] == r.clock[0]
        assert renewal['assignment_deadline'] == renewal['activated_at'] + 600
    monkeypatch.setattr(dc, 'preflight_coding_coordinator', lambda: binding)
    r.clock[0] = renewal['activated_at'] + (601 if retry_after_expiry else 1)
    delivery.deliver_coding_assignments(r.guard, r.identity)
    assert len(sends) == (0 if retry_after_expiry else 1)
    with r.guard.transaction(read_only=True) as rows:
        active = rows.grants['batches'][payload['batch']]['item']
        expected = dict(renewal)
        if not retry_after_expiry:
            expected['lane_retry_used_at_send'] = True
        assert active['assignment_renewals'][0] == expected
        assert active['coding_assignment']['status'] == ('blocked' if retry_after_expiry else 'sent')
    if not retry_after_expiry:
        r.clock[0] = renewal['activated_at'] + 601
        delivery.deliver_coding_assignments(r.guard, r.identity)
        assert len(sends) == 1


@pytest.mark.asyncio
async def test_recovered_renewal_second_proven_no_send_cannot_reopen(route, monkeypatch):
    r = route
    _, payload, _ = await accepted_recovery_assignment(r, monkeypatch)
    with r.guard.transaction(read_only=True) as rows:
        transport = copy.deepcopy(rows.grants['batches'][payload['batch']]['native_transport'])
    monkeypatch.setattr(delivery, 'command_transport', lambda *args: transport)
    preview = item.control(r.guard, r.identity, dict(payload, operation='item_assignment_renew_preview'))
    assert await r.runner._diggr_accept(await r.event(preview['next_owner_command'])) is False
    with r.guard.transaction(read_only=True) as rows:
        r.clock[0] = rows.grants['batches'][payload['batch']]['item']['assignment_renewals'][0]['assignment_deadline'] + 1
    binding = dict(workspace='fixture', surface='fixture', process=dict(pid=11))
    changed = dict(workspace='other', surface='fixture', process=dict(pid=12))
    lanes = iter((binding, changed, binding, changed))
    monkeypatch.setattr(dc, 'preflight_coding_coordinator', lambda: next(lanes))
    sends = mock_assignment_send(monkeypatch)
    delivery.deliver_coding_assignments(r.guard, r.identity)
    with r.guard.transaction(read_only=True) as rows:
        active = rows.grants['batches'][payload['batch']]['item']
        renewal = copy.deepcopy(active['assignment_renewals'][0])
        assert renewal['lane_retry_used'] is True
        assert active['coding_assignment']['status'] == 'pending'
    r.clock[0] += 1
    delivery.deliver_coding_assignments(r.guard, r.identity)
    with r.guard.transaction(read_only=True) as rows:
        active = rows.grants['batches'][payload['batch']]['item']
        assert active['assignment_renewals'][0] == renewal
        assert active['coding_assignment']['status'] == 'blocked'
    monkeypatch.setattr(dc, 'preflight_coding_coordinator', lambda: binding)
    delivery.deliver_coding_assignments(r.guard, r.identity)
    assert sends == []


@pytest.mark.asyncio
@pytest.mark.parametrize('missing', [
    'activated_at', 'activated_at_none', 'lane_retry_used', 'retry_none',
    'retry_non_bool', 'both',
    'previous_deadline_only', 'renewed_deadline_only', 'retry_only',
    'sent_snapshot_only', 'assignment_only'])
async def test_recovered_renewal_missing_activation_or_retry_evidence_fails_closed(
        route, monkeypatch, missing):
    r = route
    _, payload, _ = await accepted_recovery_assignment(r, monkeypatch)
    with r.guard.transaction(read_only=True) as rows:
        transport = copy.deepcopy(rows.grants['batches'][payload['batch']]['native_transport'])
    monkeypatch.setattr(delivery, 'command_transport', lambda *args: transport)
    preview = item.control(r.guard, r.identity, dict(payload, operation='item_assignment_renew_preview'))
    assert await r.runner._diggr_accept(await r.event(preview['next_owner_command'])) is False
    with r.guard.transaction(read_only=True) as rows:
        renewal = rows.grants['batches'][payload['batch']]['item']['assignment_renewals'][0]
        initial_deadline = renewal['assignment_deadline']
        r.clock[0] = initial_deadline + 1
    binding = dict(workspace='fixture', surface='fixture', process=dict(pid=11))
    changed = dict(binding, workspace='changed')
    lanes = iter((binding, changed))
    monkeypatch.setattr(dc, 'preflight_coding_coordinator', lambda: next(lanes))
    sends = mock_assignment_send(monkeypatch)
    delivery.deliver_coding_assignments(r.guard, r.identity)
    assert sends == []
    with r.guard.transaction() as rows:
        active = rows.grants['batches'][payload['batch']]['item']
        renewal = active['assignment_renewals'][0]
        assert active['coding_assignment']['status'] == 'pending'
        assert renewal['lane_retry_used'] is True
        if missing in {'activated_at', 'both', 'previous_deadline_only',
                       'renewed_deadline_only', 'retry_only', 'sent_snapshot_only',
                       'assignment_only'}:
            renewal.pop('activated_at')
        elif missing == 'activated_at_none':
            renewal['activated_at'] = None
        if missing in {'lane_retry_used', 'both', 'previous_deadline_only',
                       'renewed_deadline_only', 'sent_snapshot_only', 'assignment_only'}:
            renewal.pop('lane_retry_used')
        elif missing == 'retry_none':
            renewal['lane_retry_used'] = None
        elif missing == 'retry_non_bool':
            renewal['lane_retry_used'] = 0
        if missing in {'lane_retry_used', 'retry_none', 'retry_non_bool'}:
            active['coding_assignment'].pop('binding', None)
            active['coding_assignment'].pop('offered_at', None)
        if missing in {'renewed_deadline_only', 'retry_only', 'sent_snapshot_only',
                       'assignment_only'}:
            renewal.pop('previous_deadline')
        if missing in {'previous_deadline_only', 'retry_only', 'sent_snapshot_only',
                       'assignment_only'}:
            renewal['assignment_deadline'] = initial_deadline
        if missing == 'sent_snapshot_only':
            renewal['lane_retry_used_at_send'] = True
        damaged = copy.deepcopy(renewal)
    r.clock[0] += 1
    monkeypatch.setattr(dc, 'preflight_coding_coordinator', lambda: binding)
    delivery.deliver_coding_assignments(r.guard, r.identity)
    assert sends == []
    with r.guard.transaction(read_only=True) as rows:
        active = rows.grants['batches'][payload['batch']]['item']
        assert active['coding_assignment']['status'] == 'pending'
        assert active['assignment_renewals'][0] == damaged
        assert active['assignment_renewals'][0]['assignment_deadline'] == damaged['assignment_deadline']


@pytest.mark.asyncio
async def test_recovered_renewal_no_send_at_deadline_blocks(route, monkeypatch):
    r = route
    _, payload, _ = await accepted_recovery_assignment(r, monkeypatch)
    with r.guard.transaction(read_only=True) as rows:
        transport = copy.deepcopy(rows.grants['batches'][payload['batch']]['native_transport'])
    monkeypatch.setattr(delivery, 'command_transport', lambda *args: transport)
    preview = item.control(r.guard, r.identity, dict(payload, operation='item_assignment_renew_preview'))
    assert await r.runner._diggr_accept(await r.event(preview['next_owner_command'])) is False
    with r.guard.transaction(read_only=True) as rows:
        r.clock[0] = rows.grants['batches'][payload['batch']]['item']['assignment_renewals'][0]['assignment_deadline'] + 1
    binding = dict(workspace='fixture', surface='fixture', process=dict(pid=11))

    def lane():
        with r.guard.transaction(read_only=True) as rows:
            renewal = rows.grants['batches'][payload['batch']]['item']['assignment_renewals'][0]
            if renewal.get('activated_at') is not None:
                r.clock[0] = renewal['assignment_deadline']
                return dict(binding, workspace='changed')
        return binding

    monkeypatch.setattr(dc, 'preflight_coding_coordinator', lane)
    sends = mock_assignment_send(monkeypatch)
    delivery.deliver_coding_assignments(r.guard, r.identity)
    with r.guard.transaction(read_only=True) as rows:
        active = rows.grants['batches'][payload['batch']]['item']
        assert active['coding_assignment']['status'] == 'blocked'
        assert active['coding_assignment']['reason'] == 'confirmed assignment deadline expired'
        assert active['assignment_renewals'][0]['lane_retry_used'] is False
    assert sends == []


@pytest.mark.asyncio
@pytest.mark.parametrize('outcome', ['retry', 'second_exception', 'expiry'])
async def test_recovered_renewal_second_lane_exception_is_proven_no_send(
        route, monkeypatch, outcome):
    r = route
    _, payload, _ = await accepted_recovery_assignment(r, monkeypatch)
    with r.guard.transaction(read_only=True) as rows:
        transport = copy.deepcopy(rows.grants['batches'][payload['batch']]['native_transport'])
    monkeypatch.setattr(delivery, 'command_transport', lambda *args: transport)
    preview = item.control(r.guard, r.identity, dict(payload, operation='item_assignment_renew_preview'))
    assert await r.runner._diggr_accept(await r.event(preview['next_owner_command'])) is False
    with r.guard.transaction(read_only=True) as rows:
        initial_item = rows.grants['batches'][payload['batch']]['item']
        initial_renewal = copy.deepcopy(initial_item['assignment_renewals'][0])
        initial_history = copy.deepcopy(initial_item['coding_assignment_history'])
        initial_recovery = copy.deepcopy(initial_item['preparation_assignment_recovery'])
        r.clock[0] = initial_renewal['assignment_deadline'] + 1
    binding = dict(workspace='fixture', surface='fixture', process=dict(pid=11))
    checks = [0]

    def lane():
        checks[0] += 1
        if checks[0] == 2 or (outcome == 'second_exception' and checks[0] == 4):
            if outcome == 'expiry':
                with r.guard.transaction(read_only=True) as rows:
                    renewal = rows.grants['batches'][payload['batch']]['item']['assignment_renewals'][0]
                    r.clock[0] = renewal['activated_at'] + 600
            raise ValueError('lane preflight failed before paste')
        return binding

    monkeypatch.setattr(dc, 'preflight_coding_coordinator', lane)
    sends = mock_assignment_send(monkeypatch)
    with pytest.raises(ValueError, match='lane preflight failed before paste'):
        delivery.deliver_coding_assignments(r.guard, r.identity)
    assert checks[0] == 2 and sends == []
    with r.guard.transaction(read_only=True) as rows:
        active = rows.grants['batches'][payload['batch']]['item']
        assignment = active['coding_assignment']
        renewal = copy.deepcopy(active['assignment_renewals'][0])
        assert renewal['activated_at'] is not None
        assert renewal['assignment_deadline'] == renewal['activated_at'] + 600
        assert renewal['native_owner_event'] == initial_renewal['native_owner_event']
        assert renewal['preview_sha256'] == initial_renewal['preview_sha256']
        assert renewal['old_assignment'] == initial_renewal['old_assignment']
        assert renewal['new_assignment'] == initial_renewal['new_assignment']
        assert active['coding_assignment_history'] == initial_history
        assert active['preparation_assignment_recovery'] == initial_recovery
        if outcome == 'expiry':
            assert assignment['status'] == 'blocked'
            assert assignment['reason'] == 'confirmed assignment deadline expired'
            assert renewal['lane_retry_used'] is False
        else:
            assert assignment['status'] == 'pending'
            assert assignment.get('binding') is None and assignment.get('offered_at') is None
            assert renewal['lane_retry_used'] is True
    if outcome == 'expiry':
        delivery.deliver_coding_assignments(r.guard, r.identity)
        assert sends == []
        with r.guard.transaction(read_only=True) as rows:
            active = rows.grants['batches'][payload['batch']]['item']
            assert active['coding_assignment']['status'] == 'blocked'
            assert active['assignment_renewals'][0] == renewal
        return
    r.clock[0] += 1
    if outcome == 'second_exception':
        with pytest.raises(ValueError, match='lane preflight failed before paste'):
            delivery.deliver_coding_assignments(r.guard, r.identity)
        assert checks[0] == 4 and sends == []
        with r.guard.transaction(read_only=True) as rows:
            active = rows.grants['batches'][payload['batch']]['item']
            assert active['coding_assignment']['status'] == 'blocked'
            assert active['coding_assignment']['reason'] == 'renewal lane retry exhausted'
            assert active['assignment_renewals'][0] == renewal
        delivery.deliver_coding_assignments(r.guard, r.identity)
        assert sends == [] and checks[0] == 4
    else:
        delivery.deliver_coding_assignments(r.guard, r.identity)
        assert len(sends) == 1 and checks[0] == 4
        with r.guard.transaction(read_only=True) as rows:
            active = rows.grants['batches'][payload['batch']]['item']
            assert active['coding_assignment']['status'] == 'sent'
            assert active['assignment_renewals'][0] == dict(renewal, lane_retry_used_at_send=True)


@pytest.mark.asyncio
@pytest.mark.parametrize('consumer', ['acceptance', 'registration'])
@pytest.mark.parametrize('change', [
    'recovery_old', 'recovery_new', 'first_id', 'previous_id',
    'renewal_old', 'renewal_new', 'core_batch', 'core_issue'])
async def test_sent_recovery_renewal_id_tamper_rejected_at_final_lock(
        route, monkeypatch, consumer, change):
    r = route
    payload, _, reference = await sent_recovery_renewal(r, monkeypatch)
    coding_home = r.tmp / 'diggr-coding'
    coding_home.mkdir()
    if consumer == 'registration':
        assert dc.accept_coding_assignment(
            'SYSTEM171_ASSIGN ' + reference, 'coding-session', coding_home)
    with r.guard.transaction() as rows:
        batch = rows.grants['batches'][payload['batch']]
        active = batch['item']
        recovery = active['preparation_assignment_recovery']
        renewal = active['assignment_renewals'][0]
        history = active['coding_assignment_history']
        if change == 'recovery_old':
            recovery['old_assignment'] = 'wrong'
        elif change == 'recovery_new':
            recovery['new_assignment'] = 'wrong'
        elif change == 'first_id':
            history[0]['id'] = 'wrong'
        elif change == 'previous_id':
            history[1]['id'] = 'wrong'
        elif change == 'renewal_old':
            renewal['old_assignment'] = 'wrong'
        elif change == 'renewal_new':
            renewal['new_assignment'] = 'wrong'
        elif change == 'core_batch':
            recovery['core']['batch'] = 'wrong'
        else:
            recovery['core']['issue'] = 'wrong'
    sealed = r.guard.path.read_bytes()
    with pytest.raises(ValueError):
        if consumer == 'acceptance':
            dc.accept_coding_assignment(
                'SYSTEM171_ASSIGN ' + reference, 'coding-session', coding_home)
        else:
            dc.register_assigned_worker(reference, 'coding-session', coding_home, 'Implement item')
    assert r.guard.path.read_bytes() == sealed


@pytest.mark.asyncio
@pytest.mark.parametrize('retry', [False, True])
async def test_sent_recovery_renewal_accepts_and_registers_exact_chain(route, monkeypatch, retry):
    r = route
    payload, _, reference = await sent_recovery_renewal(r, monkeypatch, retry=retry)
    coding_home = r.tmp / 'diggr-coding'
    coding_home.mkdir()
    assert dc.accept_coding_assignment(
        'SYSTEM171_ASSIGN ' + reference, 'coding-session', coding_home)
    _, worker = dc.register_assigned_worker(reference, 'coding-session', coding_home, 'Implement item')
    with r.guard.transaction(read_only=True) as rows:
        batch = rows.grants['batches'][payload['batch']]
        assert batch['item']['coding_assignment']['status'] == 'registered'
        assert batch['item']['coding_assignment']['task'] == worker['task']


@pytest.mark.asyncio
@pytest.mark.parametrize('contradiction', ['correction', 'migration'])
@pytest.mark.parametrize('indicator', ['complete', 'current', 'history', 'structural'])
async def test_recovered_renewal_contradiction_fails_closed_at_every_boundary(
        route, monkeypatch, contradiction, indicator):
    r = route
    payload, _, reference = await sent_recovery_renewal(r, monkeypatch)
    coding_home = r.tmp / 'diggr-coding'
    coding_home.mkdir()
    with r.guard.transaction() as rows:
        batch = rows.grants['batches'][payload['batch']]
        active = batch['item']
        assignment = active['coding_assignment']
        if contradiction == 'correction':
            assignment['correction_of'] = 1
        else:
            active['migration'] = dict(assignment_deadline=r.clock[0] + 1200)
        if indicator != 'complete':
            active.pop('assignment_renewals')
            active.pop('preparation_assignment_recovery')
            if indicator == 'current':
                assignment['preparation_recovery_of'] = active['coding_assignment_history'][0]['id']
                active['coding_assignment_history'] = []
            elif indicator == 'history':
                active['coding_assignment_history'][0].update(status='accepted', reason=None)
            else:
                active['coding_assignment_history'][-1].pop('preparation_recovery_of')
        batch['deadline'] = r.clock[0] + 1200
        assert delivery.recovered_preparation_origin(active)
        assert dc.coding_assignment_deadline(batch) == float('-inf')
    sealed = r.guard.path.read_bytes()
    with pytest.raises(ValueError):
        dc.accept_coding_assignment('SYSTEM171_ASSIGN ' + reference,
                                    'coding-session', coding_home)
    assert r.guard.path.read_bytes() == sealed

    with r.guard.transaction() as rows:
        assignment = rows.grants['batches'][payload['batch']]['item']['coding_assignment']
        assignment.update(status='accepted', session='coding-session', accepted_at=r.clock[0])
    sealed = r.guard.path.read_bytes()
    with pytest.raises(ValueError):
        dc.register_assigned_worker(reference, 'coding-session', coding_home, 'Implement item')
    assert r.guard.path.read_bytes() == sealed

    with r.guard.transaction() as rows:
        assignment = rows.grants['batches'][payload['batch']]['item']['coding_assignment']
        assignment.update(status='pending', binding=None, offered_at=None,
                          session=None, accepted_at=None)
    sends = mock_assignment_send(monkeypatch)
    delivery.deliver_coding_assignments(r.guard, r.identity)
    assert sends == []


@pytest.mark.asyncio
@pytest.mark.parametrize('remove_current_marker', [False, True])
async def test_recovered_renewal_phase_change_keeps_structural_gate(
        route, monkeypatch, remove_current_marker):
    r = route
    payload, _, reference = await sent_recovery_renewal(r, monkeypatch)
    coding_home = r.tmp / 'diggr-coding'
    coding_home.mkdir()
    with r.guard.transaction() as rows:
        batch = rows.grants['batches'][payload['batch']]
        active = batch['item']
        active['phase'] = 'correction_needed'
        active['coding_assignment']['correction_of'] = 731
        active['task'] = 'fabricated-worker'
        active.pop('assignment_renewals')
        active.pop('preparation_assignment_recovery')
        active['coding_assignment_history'][-1].update(
            status='worker_sent', task=active['task'])
        active['coding_assignment_history'][-1].pop('preparation_recovery_of')
        if remove_current_marker:
            active['coding_assignment'].pop('preparation_recovery_of', None)
        batch['deadline'] = r.clock[0] + 1200
        assert delivery.recovered_preparation_origin(active, rows)
        assert dc.coding_assignment_deadline(batch, rows) == float('-inf')
        assert dc.coding_assignment_deadline(batch) == float('-inf')
    sealed = r.guard.path.read_bytes()
    with pytest.raises(ValueError):
        dc.accept_coding_assignment('SYSTEM171_ASSIGN ' + reference,
                                    'coding-session', coding_home)
    assert r.guard.path.read_bytes() == sealed

    with r.guard.transaction() as rows:
        rows.grants['batches'][payload['batch']]['item']['coding_assignment'].update(
            status='accepted', session='coding-session', accepted_at=r.clock[0])
    sealed = r.guard.path.read_bytes()
    with pytest.raises(ValueError):
        dc.register_assigned_worker(reference, 'coding-session', coding_home, 'Implement item')
    assert r.guard.path.read_bytes() == sealed

    with r.guard.transaction() as rows:
        rows.grants['batches'][payload['batch']]['item']['coding_assignment'].update(
            status='pending', binding=None, offered_at=None, session=None, accepted_at=None)
    sends = mock_assignment_send(monkeypatch)
    delivery.deliver_coding_assignments(r.guard, r.identity)
    assert sends == []


@pytest.mark.asyncio
@pytest.mark.parametrize('consumer', ['acceptance', 'registration'])
@pytest.mark.parametrize('retry,change', [
    (False, 'missing_snapshot'), (False, 'missing_retry'),
    (False, 'retry_none'), (False, 'retry_set'),
    (False, 'retry_increment'), (False, 'snapshot_changed'),
    (True, 'missing_snapshot'), (True, 'retry_reset'),
    (True, 'retry_false'), (True, 'retry_increment'),
    (True, 'snapshot_changed'),
])
async def test_sent_recovery_renewal_rejects_retry_state_change(
        route, monkeypatch, consumer, retry, change):
    r = route
    payload, _, reference = await sent_recovery_renewal(r, monkeypatch, retry=retry)
    coding_home = r.tmp / 'diggr-coding'
    coding_home.mkdir()
    if consumer == 'registration':
        assert dc.accept_coding_assignment(
            'SYSTEM171_ASSIGN ' + reference, 'coding-session', coding_home)
    with r.guard.transaction() as rows:
        renewal = rows.grants['batches'][payload['batch']]['item']['assignment_renewals'][0]
        if change == 'missing_snapshot':
            renewal.pop('lane_retry_used_at_send')
        elif change == 'missing_retry':
            renewal.pop('lane_retry_used')
        elif change == 'retry_none':
            renewal['lane_retry_used'] = None
        elif change == 'retry_set':
            renewal['lane_retry_used'] = True
        elif change == 'retry_reset':
            renewal.pop('lane_retry_used')
        elif change == 'retry_false':
            renewal['lane_retry_used'] = False
        elif change == 'retry_increment':
            renewal['lane_retry_used'] = 2
        else:
            renewal['lane_retry_used_at_send'] = not retry
    sealed = r.guard.path.read_bytes()
    with pytest.raises(ValueError):
        if consumer == 'acceptance':
            dc.accept_coding_assignment(
                'SYSTEM171_ASSIGN ' + reference, 'coding-session', coding_home)
        else:
            dc.register_assigned_worker(reference, 'coding-session', coding_home, 'Implement item')
    assert r.guard.path.read_bytes() == sealed


@pytest.mark.asyncio
@pytest.mark.parametrize('consumer', ['acceptance', 'registration'])
@pytest.mark.parametrize('removed', ['renewal', 'recovery', 'both',
                                     'both_without_history_marker'])
async def test_sent_recovery_renewal_requires_history_records(
        route, monkeypatch, consumer, removed):
    r = route
    payload, _, reference = await sent_recovery_renewal(r, monkeypatch)
    coding_home = r.tmp / 'diggr-coding'
    coding_home.mkdir()
    if consumer == 'registration':
        assert dc.accept_coding_assignment(
            'SYSTEM171_ASSIGN ' + reference, 'coding-session', coding_home)
    with r.guard.transaction() as rows:
        batch = rows.grants['batches'][payload['batch']]
        if removed in {'renewal', 'both', 'both_without_history_marker'}:
            batch['item'].pop('assignment_renewals')
        if removed in {'recovery', 'both', 'both_without_history_marker'}:
            batch['item'].pop('preparation_assignment_recovery')
        if removed == 'both_without_history_marker':
            batch['item']['coding_assignment_history'][-1].pop('preparation_recovery_of')
        batch['deadline'] = r.clock[0] + 1200
        assert dc.coding_assignment_deadline(batch) == float('-inf')
    sealed = r.guard.path.read_bytes()
    with pytest.raises(ValueError):
        if consumer == 'acceptance':
            dc.accept_coding_assignment(
                'SYSTEM171_ASSIGN ' + reference, 'coding-session', coding_home)
        else:
            dc.register_assigned_worker(reference, 'coding-session', coding_home, 'Implement item')
    assert r.guard.path.read_bytes() == sealed


@pytest.mark.asyncio
@pytest.mark.parametrize('consumer', ['acceptance', 'registration'])
async def test_sent_recovery_renewal_lock_delay_crosses_deadline(route, monkeypatch, consumer):
    from contextlib import contextmanager
    r = route
    payload, _, reference = await sent_recovery_renewal(r, monkeypatch)
    coding_home = r.tmp / 'diggr-coding'
    coding_home.mkdir()
    if consumer == 'registration':
        assert dc.accept_coding_assignment(
            'SYSTEM171_ASSIGN ' + reference, 'coding-session', coding_home)
    with r.guard.transaction(read_only=True) as rows:
        deadline = rows.grants['batches'][payload['batch']]['item']['assignment_renewals'][0]['assignment_deadline']
    original = dc.Guard.transaction
    delayed = [False]

    @contextmanager
    def delay_final_lock(self, *args, **kwargs):
        with original(self, *args, **kwargs) as rows:
            if not kwargs.get('read_only') and not delayed[0]:
                delayed[0] = True
                r.clock[0] = deadline
            yield rows

    monkeypatch.setattr(dc.Guard, 'transaction', delay_final_lock)
    sealed = r.guard.path.read_bytes()
    with pytest.raises(ValueError):
        if consumer == 'acceptance':
            dc.accept_coding_assignment(
                'SYSTEM171_ASSIGN ' + reference, 'coding-session', coding_home)
        else:
            dc.register_assigned_worker(reference, 'coding-session', coding_home, 'Implement item')
    assert delayed[0]
    assert r.guard.path.read_bytes() == sealed


@pytest.mark.asyncio
async def test_activated_recovery_renewal_extended_deadline_cannot_send(route, monkeypatch):
    r = route
    _, payload, _ = await accepted_recovery_assignment(r, monkeypatch)
    with r.guard.transaction(read_only=True) as rows:
        transport = copy.deepcopy(rows.grants['batches'][payload['batch']]['native_transport'])
    monkeypatch.setattr(delivery, 'command_transport', lambda *args: transport)
    preview = item.control(r.guard, r.identity, dict(payload, operation='item_assignment_renew_preview'))
    assert await r.runner._diggr_accept(await r.event(preview['next_owner_command'])) is False
    with r.guard.transaction() as rows:
        active = rows.grants['batches'][payload['batch']]['item']
        renewal = active['assignment_renewals'][0]
        r.clock[0] = renewal['assignment_deadline'] + 1
        activated_at = r.clock[0]
        renewal.update(previous_deadline=active['preparation_assignment_recovery']['assignment_deadline'],
                       activated_at=activated_at, assignment_deadline=activated_at + 600,
                       lane_retry_used=False)
        renewal['assignment_deadline'] += 60
    r.clock[0] = activated_at + 601
    sends = mock_assignment_send(monkeypatch)
    delivery.deliver_coding_assignments(r.guard, r.identity)
    assert sends == []
    with r.guard.transaction(read_only=True) as rows:
        assert rows.grants['batches'][payload['batch']]['item']['coding_assignment']['status'] == 'pending'


@pytest.mark.asyncio
@pytest.mark.parametrize('consumer', ['delayed_intake', 'worker_registration'])
async def test_activated_recovery_renewal_extended_deadline_rejected_by_consumer(
        route, monkeypatch, consumer):
    r = route
    _, payload, _ = await accepted_recovery_assignment(r, monkeypatch)
    with r.guard.transaction(read_only=True) as rows:
        transport = copy.deepcopy(rows.grants['batches'][payload['batch']]['native_transport'])
    monkeypatch.setattr(delivery, 'command_transport', lambda *args: transport)
    preview = item.control(r.guard, r.identity, dict(payload, operation='item_assignment_renew_preview'))
    assert await r.runner._diggr_accept(await r.event(preview['next_owner_command'])) is False
    binding = dict(workspace='fixture', surface='fixture', process=dict(identity=dict(pid=os.getpid())))
    monkeypatch.setattr(dc, 'preflight_coding_coordinator', lambda: binding)
    with r.guard.transaction() as rows:
        batch = rows.grants['batches'][payload['batch']]
        active = batch['item']
        renewal = active['assignment_renewals'][0]
        r.clock[0] = renewal['assignment_deadline'] + 1
        activated_at = r.clock[0]
        renewal.update(previous_deadline=active['preparation_assignment_recovery']['assignment_deadline'],
                       activated_at=activated_at, assignment_deadline=activated_at + 660,
                       lane_retry_used=False)
        assignment = active['coding_assignment']
        assignment.update(status='sent' if consumer == 'delayed_intake' else 'accepted',
                          binding=binding)
        if consumer == 'worker_registration':
            assignment.update(session='coding-session', accepted_at=activated_at)
        reference = assignment['id']
    r.clock[0] = activated_at + 601
    coding_home = r.tmp / 'diggr-coding'
    coding_home.mkdir()
    sealed = r.guard.path.read_bytes()
    with r.guard.transaction(read_only=True) as rows:
        assert dc.coding_assignment_deadline(rows.grants['batches'][payload['batch']]) == float('-inf')
    if consumer == 'delayed_intake':
        with pytest.raises(ValueError, match='stale, stopped or bound elsewhere'):
            dc.accept_coding_assignment('SYSTEM171_ASSIGN ' + reference, 'coding-session', coding_home)
    else:
        with pytest.raises(ValueError, match='no longer startable'):
            dc.register_assigned_worker(reference, 'coding-session', coding_home, 'Implement item')
    assert r.guard.path.read_bytes() == sealed


@pytest.mark.asyncio
@pytest.mark.parametrize('consumer', ['delayed_intake', 'worker_registration'])
async def test_sent_recovered_renewal_without_recovery_binding_rejected_by_consumer(
        route, monkeypatch, consumer):
    r = route
    _, payload, _ = await accepted_recovery_assignment(r, monkeypatch)
    with r.guard.transaction(read_only=True) as rows:
        transport = copy.deepcopy(rows.grants['batches'][payload['batch']]['native_transport'])
    monkeypatch.setattr(delivery, 'command_transport', lambda *args: transport)
    preview = item.control(r.guard, r.identity, dict(payload, operation='item_assignment_renew_preview'))
    assert await r.runner._diggr_accept(await r.event(preview['next_owner_command'])) is False
    binding = dict(workspace='fixture', surface='fixture', process=dict(identity=dict(pid=os.getpid())))
    monkeypatch.setattr(dc, 'preflight_coding_coordinator', lambda: binding)
    with r.guard.transaction(read_only=True) as rows:
        renewal = rows.grants['batches'][payload['batch']]['item']['assignment_renewals'][0]
        r.clock[0] = renewal['assignment_deadline'] + 1
    sends = mock_assignment_send(monkeypatch)
    delivery.deliver_coding_assignments(r.guard, r.identity)
    assert len(sends) == 1
    with r.guard.transaction() as rows:
        batch = rows.grants['batches'][payload['batch']]
        active = batch['item']
        renewal = active['assignment_renewals'][0]
        activated_at = renewal['activated_at']
        reference = active['coding_assignment']['id']
        assert active['coding_assignment']['status'] == 'sent'
        assert active['coding_assignment']['binding'] == binding
        if consumer == 'worker_registration':
            active['coding_assignment'].update(status='accepted', session='coding-session',
                                               accepted_at=activated_at)
        active.pop('preparation_assignment_recovery')
        batch['deadline'] = activated_at + 1200
    r.clock[0] = activated_at + 601
    coding_home = r.tmp / 'diggr-coding'
    coding_home.mkdir()
    sealed = r.guard.path.read_bytes()
    with r.guard.transaction(read_only=True) as rows:
        assert dc.coding_assignment_deadline(rows.grants['batches'][payload['batch']]) == float('-inf')
    if consumer == 'delayed_intake':
        with pytest.raises(ValueError, match='stale, stopped or bound elsewhere'):
            dc.accept_coding_assignment('SYSTEM171_ASSIGN ' + reference, 'coding-session', coding_home)
    else:
        with pytest.raises(ValueError, match='no longer startable'):
            dc.register_assigned_worker(reference, 'coding-session', coding_home, 'Implement item')
    assert r.guard.path.read_bytes() == sealed


@pytest.mark.parametrize('change', [
    'original_deadline', 'previous_deadline', 'activated_at', 'assignment_deadline'])
def test_recovered_renewal_deadline_requires_original_and_activation_bounds(change):
    recovery = dict(created_at=1000, assignment_deadline=1600,
                    old_assignment='first', new_assignment='previous')
    renewal = dict(previous_deadline=1600, activated_at=1700,
                   assignment_deadline=2300, lane_retry_used=False,
                   old_assignment='previous', new_assignment='current')
    batch = dict(item=dict(phase='prepared',
                           coding_assignment_history=[dict(id='first'), dict(
                               id='previous', preparation_recovery_of='first')],
                           coding_assignment=dict(id='current'),
                           preparation_assignment_recovery=recovery,
                           assignment_renewals=[renewal]))
    assert dc.coding_assignment_deadline(batch) == 2300
    batch['item']['coding_assignment'] = dict(id='current', correction_of=1)
    assert dc.coding_assignment_deadline(batch) == float('-inf')
    batch['item'].pop('coding_assignment')
    if change == 'original_deadline':
        recovery['assignment_deadline'] += 1
    elif change == 'activated_at':
        renewal['activated_at'] = None
    else:
        renewal[change] += 60
    assert dc.coding_assignment_deadline(batch) == float('-inf')
    batch['item']['coding_assignment'] = dict(id='current', correction_of=1)
    assert dc.coding_assignment_deadline(batch) == float('-inf')


def test_plain_migration_and_correction_keep_their_deadlines():
    migration = dict(item=dict(
        phase='prepared', coding_assignment=dict(id='current'),
        coding_assignment_history=[dict(id='previous', status='accepted')],
        assignment_renewals=[dict(old_assignment='previous', new_assignment='current')],
        migration=dict(assignment_deadline=2300)))
    assert not delivery.recovered_preparation_origin(migration['item'])
    assert dc.coding_assignment_deadline(migration) == 2300

    correction = dict(item=dict(
        phase='correction_needed', coding_assignment=dict(id='current', correction_of=1),
        coding_assignment_history=[dict(id='previous', status='registered')]))
    assert not delivery.recovered_preparation_origin(correction['item'])
    assert dc.coding_assignment_deadline(correction) == float('-inf')


@pytest.mark.asyncio
async def test_plain_correction_requires_bound_result_and_completed_exit(route, monkeypatch):
    r = route
    payload, home, reference, _, descriptor = await sent_second_renewal(r, monkeypatch)
    prompt = Path(descriptor['prompt']['path']).read_text()
    dc.accept_coding_assignment('SYSTEM171_ASSIGN ' + reference, 'second-session', home)
    _, registered = dc.register_assigned_worker(reference, 'second-session', home, prompt)
    monkeypatch.setattr(dc, 'cmux_send', lambda *args: None)
    assert dc.start_assigned_worker(r.guard, registered)['sent'] is True
    row = _complete_item_attempt_os(r.guard, registered['task'], registered['artifact'])
    bid, issue, task = payload['batch'], payload['issue'], row['task']
    generation = row['generation']
    assert row['direct_worker'] is True
    row.update(status='blocked', gate='coding_correction')
    correction = dict(tasks={issue: task}, item=dict(
        phase='correction_needed', issue=issue, task=task,
        coding_assignment=dict(id='current', status='pending', correction_of=generation),
        coding_assignment_history=[
            dict(id='expired', status='blocked', reason='confirmed assignment deadline expired'),
            dict(id=reference, status='worker_sent', task=task)]))
    active = correction['item']
    assert delivery.recovered_preparation_origin(active)
    assert dc.coding_assignment_deadline(correction) == float('-inf')
    rows = {task: row}
    assert delivery.canonical_plain_correction(bid, correction, active, rows)
    assert not delivery.recovered_preparation_origin(
        active, rows, bid=bid, batch=correction)
    assert dc.coding_assignment_deadline(correction, rows, bid=bid) == float('inf')
    assert dc.coding_assignment_deadline(correction) == float('-inf')
    assert delivery.recovered_preparation_origin(active, rows)
    for field, value in [('generation', generation + 1), ('gate', 'coding'),
                         ('status', 'running'), ('direct_worker', False),
                         ('worker_result', None)]:
        damaged = copy.deepcopy(row)
        if field == 'generation':
            damaged['worker_result']['generation'] = value
        else:
            damaged[field] = value
        assert delivery.recovered_preparation_origin(
            active, {task: damaged}, bid=bid, batch=correction)
        assert dc.coding_assignment_deadline(
            correction, {task: damaged}, bid=bid) == float('-inf')
    damaged = copy.deepcopy(row)
    damaged['worker_result'].pop('exit_code')
    assert delivery.recovered_preparation_origin(
        active, {task: damaged}, bid=bid, batch=correction)
    assert dc.coding_assignment_deadline(
        correction, {task: damaged}, bid=bid) == float('-inf')
    for change in ('revoked', 'hard_stop'):
        damaged = copy.deepcopy(row)
        if change == 'revoked':
            damaged['policy']['revoked'] = True
        else:
            damaged['hard_stop'] = 0
        assert delivery.recovered_preparation_origin(
            active, {task: damaged}, bid=bid, batch=correction)
        assert dc.coding_assignment_deadline(
            correction, {task: damaged}, bid=bid) == float('-inf')
    for change in ('owner_batch', 'owner_issue', 'policy_batch', 'policy_issue',
                   'missing_policy_batch', 'missing_policy_issue',
                   'batch_task', 'item_task', 'history_task', 'history_status',
                   'history_id', 'history_current_id', 'row_generation',
                   'missing_row_generation', 'result_generation',
                   'missing_result_generation', 'correction_of'):
        damaged_batch = copy.deepcopy(correction)
        damaged_item = damaged_batch['item']
        damaged_row = copy.deepcopy(row)
        if change == 'owner_batch':
            damaged_row['owner_batch'] = 'foreign-batch'
        elif change == 'owner_issue':
            damaged_row['owner_issue'] = 'foreign-issue'
        elif change == 'policy_batch':
            damaged_row['policy']['batch'] = 'foreign-batch'
        elif change == 'policy_issue':
            damaged_row['policy']['issue'] = 'foreign-issue'
        elif change == 'missing_policy_batch':
            damaged_row['policy'].pop('batch')
        elif change == 'missing_policy_issue':
            damaged_row['policy'].pop('issue')
        elif change == 'batch_task':
            damaged_batch['tasks'][issue] = 'foreign-worker'
        elif change == 'item_task':
            damaged_item['task'] = 'foreign-worker'
        elif change == 'history_task':
            damaged_item['coding_assignment_history'][-1]['task'] = 'foreign-worker'
        elif change == 'history_status':
            damaged_item['coding_assignment_history'][-1]['status'] = 'accepted'
        elif change == 'history_id':
            damaged_item['coding_assignment_history'][-1]['id'] = 'fabricated'
        elif change == 'history_current_id':
            damaged_item['coding_assignment_history'][-1]['id'] = 'current'
        elif change == 'row_generation':
            damaged_row['generation'] = generation + 1
        elif change == 'missing_row_generation':
            damaged_row.pop('generation')
        elif change == 'result_generation':
            damaged_row['worker_result']['generation'] = generation + 1
        elif change == 'missing_result_generation':
            damaged_row['worker_result'].pop('generation')
        else:
            damaged_item['coding_assignment']['correction_of'] = generation + 1
        damaged_rows = {task: damaged_row}
        assert not delivery.canonical_plain_correction(
            bid, damaged_batch, damaged_item, damaged_rows)
        assert delivery.recovered_preparation_origin(
            damaged_item, damaged_rows, bid=bid, batch=damaged_batch)
        assert dc.coding_assignment_deadline(
            damaged_batch, damaged_rows, bid=bid) == float('-inf')


@pytest.mark.asyncio
@pytest.mark.parametrize('change', [
    'foreign_policy', 'row_generation', 'result_generation', 'correction_of'])
async def test_foreign_completed_worker_cannot_authorize_fabricated_correction(
        route, monkeypatch, change):
    r = route
    payload, binding, reference = await sent_recovery_renewal(r, monkeypatch)
    monkeypatch.setattr(dc, 'process_absent', lambda *args, **kwargs: True)
    with r.guard.transaction() as rows:
        batch = rows.grants['batches'][payload['batch']]
        active = batch['item']
        active['phase'] = 'correction_needed'
        active['task'] = 'foreign-worker'
        batch['tasks'][active['issue']] = 'foreign-worker'
        active.pop('preparation_assignment_recovery')
        active.pop('assignment_renewals')
        active['coding_assignment_history'][-1].update(
            status='worker_sent', task='foreign-worker')
        active['coding_assignment_history'][-1].pop('preparation_recovery_of', None)
        assignment = active['coding_assignment']
        assignment.pop('preparation_recovery_of', None)
        assignment.update(status='pending', correction_of=1, binding=None,
                          offered_at=None, session=None, accepted_at=None)
        rows['foreign-worker'] = dict(
            task='foreign-worker', identity=r.identity,
            owner_batch=payload['batch'], owner_issue=active['issue'],
            generation=1,
            direct_worker=True, status='blocked', gate='coding_correction',
            deadline=r.clock[0] + 600,
            artifact='/tmp/foreign-correction-result', action_id='action', effect_id='effect',
            worker_route=dict(argv=['foreign prompt']),
            native_admission_sha256=dc.object_hash(dict(
                assignment=active['coding_assignment_history'][-1]['id'],
                prompt_sha256=hashlib.sha256(b'foreign prompt').hexdigest())),
            visible_sent=True, worker_pid=123, worker_identity=dict(pid=123),
            child_exited=True, child_identity=dict(pid=124),
            policy=owner.Policy(batch=(
                                    'another-batch' if change == 'foreign_policy' else payload['batch']),
                                issue=(
                                    'another-issue' if change == 'foreign_policy' else active['issue']),
                                batch_deadline=batch['deadline'],
                                item_lifecycle=True, revoked=False,
                                work_seconds=1, work_limit=10),
            worker_result=dict(generation=1, exit_code=0, artifact_status='present',
                               task='foreign-worker', artifact='/tmp/foreign-correction-result',
                               action_id='action', effect_id='effect'))
        if change == 'row_generation':
            rows['foreign-worker']['generation'] = 2
        elif change == 'result_generation':
            rows['foreign-worker']['worker_result']['generation'] = 2
        elif change == 'correction_of':
            assignment['correction_of'] = 2
        assert not delivery.canonical_plain_correction(
            payload['batch'], batch, active, rows)
        assert delivery.recovered_preparation_origin(
            active, rows, bid=payload['batch'], batch=batch)
        assert dc.coding_assignment_deadline(batch, rows, bid=payload['batch']) == float('-inf')
    sends = mock_assignment_send(monkeypatch)
    delivery.deliver_coding_assignments(r.guard, r.identity)
    assert sends == []
    coding_home = r.tmp / 'diggr-coding'
    coding_home.mkdir()
    with r.guard.transaction() as rows:
        assignment = rows.grants['batches'][payload['batch']]['item']['coding_assignment']
        assignment.update(status='sent', binding=binding)
    with pytest.raises(ValueError):
        dc.accept_coding_assignment('SYSTEM171_ASSIGN ' + reference,
                                    'coding-session', coding_home)
    with r.guard.transaction() as rows:
        assignment = rows.grants['batches'][payload['batch']]['item']['coding_assignment']
        assignment.update(status='accepted', session='coding-session', accepted_at=r.clock[0])
    with pytest.raises(ValueError):
        dc.register_assigned_worker(reference, 'coding-session', coding_home, 'Implement item')


@pytest.mark.asyncio
@pytest.mark.parametrize('change', [
    'not_expired', 'missing_recovery', 'changed_recovery', 'dirty_worktree', 'task',
    'candidate', 'work', 'registered', 'assignment_task', 'uncertain',
    'live_process', 'changed_preview', 'changed_batch', 'wrong_transport'])
async def test_recovered_assignment_renewal_fails_closed(route, monkeypatch, change):
    r = route
    _, payload, _ = await accepted_recovery_assignment(r, monkeypatch)
    preview = item.control(r.guard, r.identity, dict(
        payload, operation='item_assignment_renew_preview')) if change in {
            'changed_preview', 'changed_batch', 'wrong_transport'} else None
    if change == 'not_expired':
        r.clock[0] -= 2
    elif change == 'dirty_worktree':
        Path(payload['worktree'], 'dirty.txt').write_text('changed\n')
    elif change == 'live_process':
        monkeypatch.setattr(dc, 'process_absent', lambda *args, **kwargs: False)
    elif change == 'wrong_transport':
        monkeypatch.setattr(delivery, 'command_transport', lambda *args: dict(wrong='transport'))
    elif change != 'changed_preview':
        with r.guard.transaction() as rows:
            batch = rows.grants['batches'][payload['batch']]
            active = batch['item']
            if change == 'missing_recovery':
                active.pop('preparation_assignment_recovery')
            elif change == 'changed_recovery':
                active['preparation_assignment_recovery']['new_assignment'] = 'wrong'
            elif change == 'task':
                active['task'] = 'unexpected-task'
            elif change == 'candidate':
                active['candidate'] = dict(id='unexpected')
            elif change == 'work':
                active['work_seconds'] = 1
            elif change == 'registered':
                batch['tasks'][payload['issue']] = 'unexpected-worker'
            elif change == 'assignment_task':
                active['coding_assignment']['task'] = 'unexpected-worker'
            elif change == 'uncertain':
                active['coding_assignment']['status'] = 'uncertain'
    sealed = r.guard.path.read_bytes()
    if preview is None:
        with pytest.raises(ValueError):
            item.control(r.guard, r.identity, dict(
                payload, operation='item_assignment_renew_preview'))
    else:
        if change == 'changed_preview':
            with r.guard.transaction() as rows:
                rows.grants['batches'][payload['batch']]['item']['coding_assignment']['session'] = 'changed'
            sealed = r.guard.path.read_bytes()
        elif change == 'changed_batch':
            with r.guard.transaction() as rows:
                rows.grants['batches'][payload['batch']]['native_transport']['bot_id'] = 'changed'
            sealed = r.guard.path.read_bytes()
        with pytest.raises(ValueError):
            await r.runner._diggr_accept(await r.event(preview['next_owner_command']))
    assert r.guard.path.read_bytes() == sealed


@pytest.mark.asyncio
@pytest.mark.parametrize('process_race', [False, True])
async def test_migration_assignment_renewal_keeps_existing_fenced_path(route, monkeypatch,
                                                                       process_race):
    r = route
    contract, payload = await setup(r, monkeypatch)
    item.control(r.guard, r.identity, payload)
    r.task['worker_route']['argv'][-1] += '\n' + item.onboarding_payload()[0]
    r.task['launcher_command'] = dc.logical_launcher_command(contract, r.task['worker_route'])
    old = register(r)
    retained = dc._worktree_snapshot(payload['worktree'])
    process = dict(pid=99999999, start='old-process-start', executable='/usr/bin/hermes')
    with r.guard.transaction() as rows:
        batch = rows.grants['batches'][payload['batch']]
        batch['flow_version'] = 3
        batch['native_transport'] = delivery.native_transport(r.identity)
        batch['tasks'] = {}
        active = batch['item']
        active.update(phase='prepared', task=None, migration=dict(
            old_task=old['task'], head=payload['base'],
            retained_worktree_sha256=retained, work_seconds=0,
            assignment_deadline=time.time() - 1))
        active['coding_assignment'] = dict(
            id='a' * 48, status='accepted', accepted_at=time.time() - 10,
            session='coding-session', binding=dict(process=dict(identity=process)))
        row = rows[old['task']]
        row.update(status='superseded', gate='migrated', direct_worker=False)
        row['migration_fence'] = dict(owner_event='fixture-migration', preview_sha256='f' * 64,
                                      row_sha256=dc.migration_fence_core(row))
    with r.guard.transaction(read_only=True) as rows:
        original_transport = copy.deepcopy(rows.grants['batches'][payload['batch']]['native_transport'])
    monkeypatch.setattr(delivery, 'command_transport', lambda *args: original_transport)
    process_checks = []

    def process_exit_proof(identity, *, original_process_only=False):
        process_checks.append((identity, original_process_only))
        return not process_race or len(process_checks) < 3

    monkeypatch.setattr(dc, 'process_absent', process_exit_proof)
    preview = item.control(r.guard, r.identity, dict(payload, operation='item_assignment_renew_preview'))
    assert preview['core']['old_row_sha256']
    sealed = r.guard.path.read_bytes()
    if process_race:
        with pytest.raises(ValueError, match='before renewal'):
            await r.runner._diggr_accept(await r.event(preview['next_owner_command']))
        assert process_checks == [(process, True)] * 3
        assert r.guard.path.read_bytes() == sealed
        return
    assert await r.runner._diggr_accept(await r.event(preview['next_owner_command'])) is False
    assert process_checks == [(process, True)] * 3
    with r.guard.transaction(read_only=True) as rows:
        batch = rows.grants['batches'][payload['batch']]
        assert batch['item']['migration']['assignment_deadline'] == r.clock[0] + 600
        assert batch['item']['coding_assignment']['status'] == 'pending'
        assert dc.migration_fence_matches(rows[old['task']])
        first_deadline = batch['item']['migration']['assignment_deadline']
    r.clock[0] = first_deadline + 1
    sends = mock_assignment_send(monkeypatch)
    delivery.deliver_coding_assignments(r.guard, r.identity)
    assert len(sends) == 1
    with r.guard.transaction(read_only=True) as rows:
        batch = rows.grants['batches'][payload['batch']]
        assert batch['item']['coding_assignment']['status'] == 'sent'
        assert batch['item']['migration']['assignment_deadline'] == r.clock[0] + 600


def native_computer_use_trace(r, candidate, checks):
    """Persist real-shaped action/result pairs in the disposable session DB."""
    from hermes_state import SessionDB
    capture_result = SessionDB._encode_content([
        {'type': 'text', 'text': 'capture mode=som 100x100'},
        {'type': 'image_url', 'image_url': {
            'url': 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScL/nwAAAABJRU5ErkJggg=='}}])
    db = r.tmp / 'state.db'
    with sqlite3.connect(db) as conn:
        conn.execute('CREATE TABLE IF NOT EXISTS messages ('
                     'id INTEGER PRIMARY KEY, session_id TEXT, role TEXT, '
                     'tool_name TEXT, tool_call_id TEXT, tool_calls TEXT, '
                     'content TEXT, timestamp REAL)')
        pairs = {}
        for check in checks:
            step = (check['action'], check['expected'])
            if step not in pairs:
                pair = []
                for action, result in (('click', '{"ok":true}'),
                                       ('capture', capture_result)):
                    call_id = 'call_' + str(conn.execute('SELECT count(*) FROM messages').fetchone()[0])
                    call = dict(id=call_id, function=dict(name='computer_use',
                                arguments=json.dumps(dict(action=action))))
                    conn.execute('INSERT INTO messages(session_id,role,tool_calls,timestamp) VALUES (?,?,?,?)',
                                 (r.identity['session'], 'assistant', json.dumps([call]),
                                  candidate['created_at'] + 1))
                    cursor = conn.execute('INSERT INTO messages('
                        'session_id,role,tool_name,tool_call_id,content,timestamp) VALUES (?,?,?,?,?,?)',
                        (r.identity['session'], 'tool', 'computer_use', call_id, result,
                         candidate['created_at'] + 2))
                    pair.append(cursor.lastrowid)
                pairs[step] = pair
            check['action_message_id'], check['capture_message_id'] = pairs[step]


def test_computer_use_trace_accepts_only_unique_compression_continuation(route):
    r = route
    candidate = dict(created_at=r.clock[0], steps=[dict(action='Open album', expected='Tracks visible')])
    check = dict(action='Open album', expected='Tracks visible')
    native_computer_use_trace(r, candidate, [check])
    report = dict(checks=[check])
    db = r.tmp / 'state.db'
    with sqlite3.connect(db) as conn:
        conn.execute('CREATE TABLE sessions (id TEXT PRIMARY KEY, parent_session_id TEXT, '
                     'source TEXT, model_config TEXT, ended_at REAL, end_reason TEXT)')
        conn.execute('INSERT INTO sessions(id,end_reason,ended_at) VALUES (?,?,?)',
                     (r.identity['session'], 'compression', r.clock[0] + 1))
        conn.execute('INSERT INTO sessions(id,parent_session_id,source,model_config) VALUES (?,?,?,?)',
                     ('child', r.identity['session'], 'telegram', '{}'))
        conn.execute('UPDATE messages SET session_id=?', ('child',))
    item.verify_computer_use_trace(report, candidate, r.identity)
    with sqlite3.connect(db) as conn:
        conn.execute('UPDATE sessions SET end_reason=? WHERE id=?',
                     ('session_reset', r.identity['session']))
    with pytest.raises(ValueError, match='current native session'):
        item.verify_computer_use_trace(report, candidate, r.identity)
    with sqlite3.connect(db) as conn:
        conn.execute('UPDATE sessions SET end_reason=? WHERE id=?',
                     ('compression', r.identity['session']))
        conn.execute('INSERT INTO sessions(id,parent_session_id,source,model_config) VALUES (?,?,?,?)',
                     ('sibling', r.identity['session'], 'telegram', '{}'))
    with pytest.raises(ValueError, match='current native session'):
        item.verify_computer_use_trace(report, candidate, r.identity)
    with sqlite3.connect(db) as conn:
        conn.execute('UPDATE sessions SET model_config=? WHERE id=?',
                     ('{"_branched_from":"fixture"}', 'sibling'))
    item.verify_computer_use_trace(report, candidate, r.identity)


def test_automatic_playback_requires_track_and_duration_evidence():
    candidate = dict(criteria=['Track playback'],
                     steps=[dict(action='Play track', expected='Playback starts', kind='track_playback')])
    report = dict(checks=[dict(criterion='Track playback', action='Play track', kind='track_playback',
                               expected='Playback starts', observed='Playback visible', result='passed')])
    with pytest.raises(ValueError, match='owner-approved playback cases'):
        item.validate_automatic_test(report, candidate, ['track_playback'])

    steps = [dict(action=action, expected='Playback starts', kind='track_playback')
             for action in ('Play track A', 'Play track B', 'Starte Titel C')]
    candidate = dict(criteria=['Track playback'], steps=steps)
    checks = [dict(criterion='Track playback', **step, observed='Playback visible',
                   result='passed', track=name, column=column, duration_seconds=5)
              for step, name, column in zip(steps[:2], ('A', 'B'), ('left', 'right'))]
    checks.append(dict(criterion='Track playback', **steps[2],
                       observed='Playback visible', result='passed'))
    with pytest.raises(ValueError, match='different columns and at most ten seconds'):
        item.validate_automatic_test(dict(checks=checks), candidate, ['track_playback'])
    checks[-1]['kind'] = 'ui'
    with pytest.raises(ValueError, match='cover every criterion and test-card step'):
        item.validate_automatic_test(dict(checks=checks), candidate, ['track_playback'])
    checks[-1]['kind'] = 'track_playback'
    checks[-1].update(track='C', column='right', duration_seconds=5)
    checks[1]['column'] = 'right'
    checks[-1]['action'] = steps[1]['action']
    checks[-1]['expected'] = steps[1]['expected']
    with pytest.raises(ValueError, match='cover every criterion and test-card step'):
        item.validate_automatic_test(dict(checks=checks), candidate, ['track_playback'])

    two_steps = dict(criteria=['Track playback'],
                     steps=steps[:2])
    duplicate = [dict(checks[0]), dict(checks[0], track='B', column='right')]
    duplicate.append(dict(checks[1]))
    with pytest.raises(ValueError, match='one consistent native action'):
        item.validate_automatic_test(dict(checks=duplicate), two_steps, ['track_playback'])


def _bind_disposable_codex_executable(r, monkeypatch, *, output='implementation report',
                                      omit_output=False, exit_code=0):
    """Bind a bounded test executable before native argv sealing and held exec."""
    executable = r.tmp / 'fixture-bin' / 'codex'
    executable.parent.mkdir(exist_ok=True)
    executable.write_text(f'''#!{sys.executable}
import json, os, sys, time
from pathlib import Path

args = sys.argv[1:]
assert args[0] == 'exec' and args.count('--output-last-message') == 1
artifact = Path(args[args.index('--output-last-message') + 1]).resolve()
home = Path(os.environ['HERMES_HOME']).resolve()
assert artifact.is_relative_to(home)
assert Path.cwd().resolve() == Path(args[args.index('--cd') + 1]).resolve()
marker = artifact.with_name(artifact.name + '.spawn')
with marker.open('x') as record:
    json.dump(dict(pid=os.getpid(), pgid=os.getpgrp()), record)
release = Path(str(artifact) + '.release')
deadline = time.monotonic() + 10
while not release.exists() and time.monotonic() < deadline:
    time.sleep(.02)
if not release.exists():
    sys.exit(2)
if not {omit_output!r}:
    artifact.write_text('scoped correction result' if artifact.name == 'correction-result.txt'
                        else {output!r})
sys.exit({exit_code!r})
''')
    executable.chmod(0o700)
    monkeypatch.setattr(dc, 'CODEX_EXECUTABLE', str(executable))
    return executable


async def completed_candidate(r, monkeypatch, *, prepared=None, observed_direct=False,
                              observed_v2=False):
    assert not (observed_direct and observed_v2)
    contract, payload = prepared if prepared is not None else await setup(
        r, monkeypatch, result_flow=observed_direct)
    if observed_v2:
        # The Item starter holds its real group leader before execvpe. Bind a
        # disposable executable into the route before native registration so
        # that the exact sealed argv can run without a live Codex installation.
        executable = _bind_disposable_codex_executable(r, monkeypatch)
        r.task['worker_route']['argv'][0] = str(executable)
        r.task['launcher_command'] = dc.logical_launcher_command(contract, r.task['worker_route'])
    item.control(r.guard, r.identity, payload)
    if observed_v2:
        row = _complete_native_v2_item_attempt(r, monkeypatch)
    elif observed_direct:
        binding = dict(workspace='fixture', surface='fixture',
                       process=dict(identity=dict(pid=os.getpid()), argv=['hermes', '--resume', 'coding']))
        monkeypatch.setattr(dc, 'preflight_coding_coordinator', lambda: binding)
        with r.guard.transaction() as rows:
            batch = rows.grants['batches'][payload['batch']]
            batch['native_transport'] = delivery.native_transport(r.identity)
            assignment = batch['item']['coding_assignment']
            reference = assignment['id']
            assignment.update(status='sent', binding=binding)
        coding_home = r.tmp / 'diggr-coding'
        coding_home.mkdir(exist_ok=True)
        assert dc.accept_coding_assignment('SYSTEM171_ASSIGN ' + reference,
                                           'coding-session', coding_home)
        _, row = dc.register_assigned_worker(reference, 'coding-session', coding_home,
                                             'Implement and test the approved fixture item.')
        monkeypatch.setattr(dc, 'cmux_send', lambda *args: None)
        assert dc.start_assigned_worker(r.guard, row)['sent'] is True
        row = _complete_item_attempt_os(r.guard, row['task'], row['artifact'],
                                        output='implementation report')
        native_result = row['worker_result']
        assert r.guard.observe(row['task'], row['generation'], 'completed', native_result['evidence'])
    else:
        row = register(r)
        # Legacy candidate fixtures carry an unstarted result; only the
        # dedicated direct-worker paths above qualify operational supervision.
        Path(row['artifact']).write_text('implementation report')
        native_result = dict(task=row['task'], generation=row['generation'], action=row['action'],
                             artifact=row['artifact'], action_id=row['action_id'],
                             effect_id=row['effect_id'], exit_code=0, artifact_status='present',
                             evidence=dict(sha256=dc.digest(row['artifact'])))
        with r.guard.transaction() as rows:
            rows[row['task']].update(worker_result=native_result, status='pending', gate='main_validation')
        monkeypatch.setattr(dc, 'verify_prior_exit', lambda row: None)
        monkeypatch.setattr(dc, 'verify_restarted_reservation_exit', lambda row: None)
    assessment = evidence(contract, 'assessment.json', dict(task=row['task'], result='passed',
        result_sha256=dc.digest(row['artifact']), checks=['scoped diff and tests']))
    command = [sys.executable, '-c', "from pathlib import Path; Path('build').mkdir(exist_ok=True); Path('build/app').write_text('candidate')"]
    steps = [dict(action='Open album', expected='Tracks grouped')]
    if contract['acceptance'].get('mode') == 'automatic':
        steps.append(dict(action='Switch source column', expected='Other tracks remain accessible'))
        for step in steps:
            step['kind'] = 'ui'
    candidate_payload = dict(payload, operation='item_candidate', steps=steps,
                             build_argv=command, build_path=str(Path(payload['worktree']) / 'build'), assessment=assessment)
    return contract, payload, candidate_payload


@pytest.mark.asyncio
async def test_real_owner_accepts_exact_candidate_once_and_no_idle_expiry(route, monkeypatch):
    r = route
    contract, payload, candidate_payload = await completed_candidate(r, monkeypatch)
    result = item.control(r.guard, r.identity, candidate_payload)
    assert item.control(r.guard, r.identity, candidate_payload) == result
    assert r.guard.get(r.task['task'])['status'] == 'awaiting_user'
    r.clock[0] += 86400
    assert r.guard.tick(r.identity) is None
    publish = dict(payload, operation='item_publish_check')
    with pytest.raises(ValueError, match='acceptance'):
        item.control(r.guard, r.identity, publish)
    event = await r.event('/continuation accept ' + result['candidate']['id'])
    assert await r.runner._diggr_accept(event) is False
    allowed = item.control(r.guard, r.identity, publish)
    assert allowed['may_publish'] and allowed['tree'] == result['candidate']['tree']
    (Path(payload['worktree']) / 'src/code.txt').write_text('changed after acceptance\n')
    with pytest.raises(ValueError):
        item.control(r.guard, r.identity, publish)


@pytest.mark.asyncio
async def test_owner_test_only_acceptance_holds_publication_without_wake(route, monkeypatch):
    r = route
    _, payload, candidate_payload = await completed_candidate(r, monkeypatch)
    candidate = item.control(r.guard, r.identity, candidate_payload)['candidate']
    row_before = r.guard.get(r.task['task'])
    event = await r.event('/continuation accept-test-only ' + candidate['id'])
    assert await r.runner._diggr_accept(event) is False
    current = item.status(r.guard, r.identity, payload['batch'], payload['issue'])
    assert current['item']['acceptance']['candidate'] == candidate['id']
    assert current['item']['publication_hold'] is True
    assert current['execution']['status'] == 'awaiting_user'
    assert current['execution']['gate'] == 'item_test_only'
    assert current['execution']['generation'] == row_before['generation']
    with pytest.raises(ValueError, match='publication hold'):
        item.control(r.guard, r.identity, dict(payload, operation='item_publish_check'))
    assert r.guard.tick(r.identity) is None
    assert await r.runner._diggr_accept(event) is False
    assert r.guard.get(r.task['task'])['generation'] == row_before['generation']


@pytest.mark.asyncio
async def test_contract_bound_test_only_holds_even_on_plain_accept(route, monkeypatch):
    r = route
    contract, payload = await setup(r, monkeypatch, publication='test_only')
    _, _, candidate_payload = await completed_candidate(r, monkeypatch, prepared=(contract, payload))
    candidate = item.control(r.guard, r.identity, candidate_payload)['candidate']
    row_before = r.guard.get(r.task['task'])
    assert await r.runner._diggr_accept(await r.event('/continuation accept ' + candidate['id'])) is False
    current = item.status(r.guard, r.identity, payload['batch'], payload['issue'])
    assert current['item']['publication_hold'] is True
    assert 'build_argv' not in current['item']['candidate']
    assert current['execution']['generation'] == row_before['generation']
    assert r.guard.tick(r.identity) is None
    with pytest.raises(ValueError, match='publication hold'):
        item.control(r.guard, r.identity, dict(payload, operation='item_publish_check'))


@pytest.mark.asyncio
async def test_failed_candidate_build_keeps_bound_output_and_exit(route, monkeypatch):
    r = route
    _, payload, candidate_payload = await completed_candidate(r, monkeypatch)
    prior_work = item.status(r.guard, r.identity, payload['batch'], payload['issue'])['item']['work_seconds']
    candidate_payload['build_argv'] = [sys.executable, '-c',
                                       "import sys; print('SECRET_TOKEN=abc123'); print('build diagnostic'); sys.exit(7)"]
    with pytest.raises(ValueError, match='candidate build or provenance failed'):
        item.control(r.guard, r.identity, candidate_payload)
    readback = item.control(r.guard, r.identity,
                            dict(payload, operation='item_build_readback'))
    assert readback['phase'] == 'candidate_failed'
    assert readback['may_rebuild'] is True
    assert readback['build']['exit_code'] == 7
    assert readback['build']['cwd'] == payload['worktree']
    assert 'argv' not in readback['build']
    assert len(readback['build']['error_message']) <= 160
    assert 'abc123' not in json.dumps(readback)
    assert 'abc123' not in json.dumps(item.status(r.guard, r.identity, payload['batch'], payload['issue']))
    log = Path(readback['build']['log'])
    assert log.stat().st_mode & 0o777 == 0o600
    assert 'build diagnostic\n' in log.read_text()
    assert 'SECRET_TOKEN=abc123' in log.read_text()
    assert readback['build']['log_sha256'] == dc.digest(log)
    assert item.status(r.guard, r.identity, payload['batch'], payload['issue'])['item']['work_seconds'] == prior_work


@pytest.mark.asyncio
async def test_missing_candidate_build_binary_keeps_launch_diagnosis(route, monkeypatch):
    r = route
    _, payload, candidate_payload = await completed_candidate(r, monkeypatch)
    candidate_payload['build_argv'] = [str(r.tmp / 'missing-builder')]
    with pytest.raises(ValueError, match='candidate build or provenance failed'):
        item.control(r.guard, r.identity, candidate_payload)
    readback = item.control(r.guard, r.identity,
                            dict(payload, operation='item_build_readback'))
    assert readback['build']['error_type'] == 'FileNotFoundError'
    assert readback['build']['exit_code'] is None
    assert readback['build']['state'] == 'failed'


@pytest.mark.asyncio
async def test_delegation_is_not_acceptance_and_raw_result_is_immutable(route, monkeypatch):
    r = route
    contract, payload, candidate_payload = await completed_candidate(r, monkeypatch)
    result = item.control(r.guard, r.identity, candidate_payload)
    cid = result['candidate']['id']
    await r.runner._diggr_accept(await r.event('/continuation delegate-test ' + cid))
    with pytest.raises(ValueError, match='passing result'):
        await r.runner._diggr_accept(await r.event('/continuation accept ' + cid))
    ref = evidence(contract, 'computer-use.json', dict(candidate_id=cid, tree=result['candidate']['tree'],
        result='passed', method='computer_use', checks=['all expected interactions']))
    item.control(r.guard, r.identity, dict(payload, operation='item_test_result', candidate_id=cid,
                                        result='passed', evidence=ref))
    with pytest.raises(ValueError, match='acceptance'):
        item.control(r.guard, r.identity, dict(payload, operation='item_publish_check'))
    await r.runner._diggr_accept(await r.event('/continuation accept ' + cid))
    assert item.control(r.guard, r.identity, dict(payload, operation='item_publish_check'))['may_publish']
    Path(r.task['artifact']).write_text('rewritten result')
    with pytest.raises(ValueError, match='raw result'):
        item.control(r.guard, r.identity, candidate_payload)


@pytest.mark.asyncio
async def test_confirmed_automatic_mode_accepts_only_complete_computer_use_evidence(route, monkeypatch):
    r = route
    prepared = await setup(r, monkeypatch, mode='automatic')
    contract, payload, candidate_payload = await completed_candidate(r, monkeypatch, prepared=prepared)
    untyped = copy.deepcopy(contract)
    untyped['acceptance'].pop('required_test_kinds')
    with pytest.raises(ValueError, match='owner-approved test kinds'):
        owner.validate_logical_contract(untyped)
    with r.guard.transaction(read_only=True) as rows:
        assert not rows.grants['batches'][payload['batch']].get('merge_approval_required', False)
    with pytest.raises(ValueError, match='short test card'):
        item.control(r.guard, r.identity, dict(candidate_payload, steps=[
            dict(action=step['action'], expected=step['expected'])
            for step in candidate_payload['steps']]))
    with pytest.raises(ValueError, match='two distinct UI test cases'):
        item.control(r.guard, r.identity, dict(candidate_payload, steps=candidate_payload['steps'][:1]))
    result = item.control(r.guard, r.identity, candidate_payload)
    candidate = result['candidate']
    assert 'Automatikmodus' in result['test_card']
    assert r.guard.get(r.task['task'])['status'] == 'pending'
    publish = dict(payload, operation='item_publish_check')
    with pytest.raises(ValueError, match='acceptance'):
        item.control(r.guard, r.identity, publish)
    step = candidate['steps'][0]
    checks = [dict(criterion=criterion, action=step['action'], expected=step['expected'], kind=step['kind'],
                   observed='Tracks remain visibly grouped', result='passed')
              for criterion in contract['acceptance']['criteria']]
    second = candidate['steps'][1]
    checks.append(dict(criterion=contract['acceptance']['criteria'][0],
                       action=second['action'], expected=second['expected'], kind=second['kind'],
                       observed='Other tracks remain visible', result='passed'))
    incomplete = evidence(contract, 'automatic-incomplete.json', dict(
        candidate_id=candidate['id'], tree=candidate['tree'], result='passed',
        method='computer_use', checks=[dict(checks[0], criterion='unrelated criterion')]))
    request = dict(payload, operation='item_test_result', candidate_id=candidate['id'],
                   result='passed', evidence=incomplete)
    with pytest.raises(ValueError, match='cover every criterion'):
        item.control(r.guard, r.identity, request)
    with pytest.raises(ValueError, match='acceptance'):
        item.control(r.guard, r.identity, publish)
    untraced = evidence(contract, 'automatic-untraced.json', dict(
        candidate_id=candidate['id'], tree=candidate['tree'], result='passed',
        method='computer_use', checks=checks))
    request['evidence'] = untraced
    with pytest.raises(ValueError, match='native action and capture'):
        item.control(r.guard, r.identity, request)
    native_computer_use_trace(r, candidate, checks)
    passed = evidence(contract, 'automatic-passed.json', dict(
        candidate_id=candidate['id'], tree=candidate['tree'], result='passed',
        method='computer_use', checks=checks))
    request['evidence'] = passed
    with sqlite3.connect(r.tmp / 'state.db') as conn:
        conn.execute('UPDATE messages SET session_id=? WHERE id=?',
                     ('other-session', checks[0]['capture_message_id']))
    with pytest.raises(ValueError, match='current native session'):
        item.control(r.guard, r.identity, request)
    with sqlite3.connect(r.tmp / 'state.db') as conn:
        conn.execute('UPDATE messages SET session_id=? WHERE id=?',
                     (r.identity['session'], checks[0]['capture_message_id']))
    receipt = item.control(r.guard, r.identity, request)
    state = r.guard.path.read_bytes()
    assert item.control(r.guard, r.identity, request) == receipt
    assert r.guard.path.read_bytes() == state
    current = item.status(r.guard, r.identity, payload['batch'], payload['issue'])
    assert current['item']['acceptance']['mode'] == 'automatic'
    assert current['execution']['status'] == 'pending'
    assert item.control(r.guard, r.identity, publish)['may_publish']


@pytest.mark.asyncio
async def test_automatic_track_test_rejects_long_or_single_column_playback(route, monkeypatch):
    r = route
    prepared = await setup(r, monkeypatch, mode='automatic',
                           required_test_kinds=['track_playback'])
    contract, payload, candidate_payload = await completed_candidate(r, monkeypatch, prepared=prepared)
    with pytest.raises(ValueError, match='owner-approved playback'):
        item.control(r.guard, r.identity, candidate_payload)
    candidate_payload['steps'] = [
        dict(action='Spiele Titel A', expected='Wiedergabe startet', kind='track_playback'),
        dict(action='Starte Titel B', expected='Wiedergabe startet', kind='track_playback'),
    ]
    candidate = item.control(r.guard, r.identity, candidate_payload)['candidate']
    steps = candidate['steps']
    for name, tracks in (
        ('long', [dict(track='A', column='Left', duration_seconds=11),
                  dict(track='B', column='Right', duration_seconds=4)]),
        ('same-column', [dict(track='A', column='Left', duration_seconds=4),
                         dict(track='B', column='left', duration_seconds=4)]),
    ):
        checks = [dict(criterion=contract['acceptance']['criteria'][0],
                       action=step['action'], expected=step['expected'], kind=step['kind'],
                       observed='Playback visible', result='passed', **track)
                  for step, track in zip(steps, tracks)]
        checks.extend(dict(criterion=criterion, action=steps[0]['action'],
                           expected=steps[0]['expected'], kind=steps[0]['kind'],
                           observed='Playback visible', result='passed', **tracks[0])
                      for criterion in contract['acceptance']['criteria'][1:])
        report = evidence(contract, 'automatic-' + name + '.json', dict(
            candidate_id=candidate['id'], tree=candidate['tree'], result='passed',
            method='computer_use', checks=checks))
        with pytest.raises(ValueError, match='different columns and at most ten seconds'):
            item.control(r.guard, r.identity, dict(payload, operation='item_test_result',
                candidate_id=candidate['id'], result='passed', evidence=report))
    assert not item.status(r.guard, r.identity, payload['batch'], payload['issue'])['item'].get('acceptance')
    valid = [dict(criterion=contract['acceptance']['criteria'][0], **step,
                  observed='Playback visible', result='passed', track=track,
                  column=column, duration_seconds=5)
             for step, track, column in zip(steps, ('A', 'B'), ('Left', 'Right'))]
    item.validate_automatic_test(dict(checks=valid), candidate,
                                 contract['acceptance']['required_test_kinds'])
    native_computer_use_trace(r, candidate, valid)
    item.verify_computer_use_trace(dict(checks=valid), candidate, r.identity)
    valid[1]['action_message_id'] = valid[0]['action_message_id']
    with pytest.raises(ValueError, match='distinct Computer Use actions'):
        item.verify_computer_use_trace(dict(checks=valid), candidate, r.identity)


@pytest.mark.asyncio
@pytest.mark.parametrize('expired', [False, True])
async def test_preconfirmed_second_item_waits_for_closeout_without_new_grant_or_deadline(route, monkeypatch, expired):
    r = route
    contract, first = await setup(r, monkeypatch, mode='automatic', result_flow=True)
    item.control(r.guard, r.identity, first)
    second = manifest(r.task, number=2)
    next_contract = copy.deepcopy(contract)
    next_contract['plane_id'] = 'SYSTEM-168'
    second['todos'][0]['contract'] = next_contract
    r.clock[0] += 1  # The two independent confirmations have an observable order.
    next_batch, _, _ = await confirm(r, second)
    with r.guard.transaction() as rows:
        first_batch = rows.grants['batches'][first['batch']]
        first_batch['coordinator_delivery'] = dict(status='admitted')
        queued = rows.grants['batches'][next_batch]
        queued['native_transport'] = dict(origin='fixture')
        queued['coordinator_delivery'] = dict(status='pending', generation=1,
                                              due=0, deadline=r.clock[0] + owner.BUDGETS['batch_seconds'])
        deadline = queued['coordinator_delivery']['deadline']
    for name in ('deliver_coding_assignments', 'observe_unstarted_coding_assignments'):
        monkeypatch.setattr(delivery, name, lambda *_args: None)
    for name in ('inspect_unstarted_coding_assignments', 'reconcile_prepared', 'reconcile_correction'):
        async def idle(*_args, **_kwargs):
            return None
        monkeypatch.setattr(delivery, name, idle)
    async def origin(*_args):
        return object()
    monkeypatch.setattr(delivery, 'origin_scope', origin)
    r.runner._completion_receipt_adapter = lambda *_args: r.adapter
    r.runner._peek_session_state = lambda _key: None
    r.runner._running_agents = {}
    r.adapter._pending_messages = {}
    r.adapter._active_sessions = {}
    events = []
    r.runner._enqueue_fifo = lambda _key, event, _adapter: events.append(event)
    await delivery.tick_batches(r.runner, r.guard, r.identity, idle=False)
    assert not events
    with r.guard.transaction(read_only=True) as rows:
        assert rows.grants['batches'][next_batch]['coordinator_delivery']['status'] == 'pending'
    with r.guard.transaction() as rows:
        rows.grants['batches'][first['batch']]['item']['phase'] = 'closed'
    if expired:
        r.clock[0] = deadline + 1
    else:
        with r.guard.transaction() as rows:
            prior = rows.grants['batches'][first['batch']]
            prior['coordinator_delivery']['status'] = 'uncertain'
            prior['coordinator_identity']['session'] = 'earlier-native-session'
        await delivery.tick_batches(r.runner, r.guard, r.identity, idle=False)
        assert not events  # An unresolved older wake cannot be skipped.
        with r.guard.transaction() as rows:
            rows.grants['batches'][first['batch']]['coordinator_delivery']['status'] = 'admitted'
    await delivery.tick_batches(r.runner, r.guard, r.identity, idle=False)
    if expired:
        assert not events
    else:
        assert len(events) == 1 and events[0]._diggr_wake.batch == next_batch
    with r.guard.transaction(read_only=True) as rows:
        assert rows.grants['batches'][next_batch]['coordinator_delivery']['deadline'] == deadline
        assert rows.grants['batches'][next_batch]['coordinator_delivery']['status'] == ('suppressed' if expired else 'queued')
        assert rows.grants['batches'][next_batch]['confirmation_event']
    if not expired:
        # The first item can reclaim the lane after queueing but before the wake is accepted.
        with r.guard.transaction() as rows:
            rows.grants['batches'][first['batch']]['item']['phase'] = 'reviewed'
        monkeypatch.setattr(delivery, 'batch_for_wake',
                            lambda _runner, _event, rows, _identity: rows.grants['batches'][next_batch])
        assert not delivery.accept_batch(r.runner, events[0], r.guard, r.identity)
        with r.guard.transaction(read_only=True) as rows:
            assert rows.grants['batches'][next_batch]['coordinator_delivery']['status'] == 'queued'


def signed_commit(r, worktree, suffix=''):
    key = r.tmp / ('signing' + suffix)
    subprocess.run(['ssh-keygen', '-q', '-t', 'ed25519', '-N', '', '-f', str(key)], check=True)
    allowed = r.tmp / 'allowed-signers'
    allowed.write_text('fixture@example.invalid ' + Path(str(key) + '.pub').read_text())
    git(worktree, 'config', 'gpg.format', 'ssh')
    git(worktree, 'config', 'user.signingkey', str(key))
    git(worktree, 'config', 'gpg.ssh.allowedSignersFile', str(allowed))
    git(worktree, 'commit', '--allow-empty', '-S', '-m', 'accepted fixture candidate')
    return git(worktree, 'rev-parse', 'HEAD')


def _installed_review_source_binding(payload, source):
    """Exact source check from loop_control_item_readback.py, review lines 106-109."""
    if (payload.get('source') != source or source['head'] != payload['head'] or
            source['base'] != payload['base'] or source['branch'] != payload['branch']):
        raise ValueError('direct review source binding missing or inconsistent')


@pytest.mark.parametrize('direct_evidence', [
    'complete', 'missing', 'invalid', 'missing_source', 'mismatched_source',
    'evidence_source_only'])
def test_item_review_forwards_manifest_direct_evidence_to_native_reader(monkeypatch, direct_evidence):
    head = 'a' * 40
    base = 'b' * 40
    source = {'head': head, 'base': base, 'branch': 'fixture-branch',
              'diff_hash': 'd' * 64}
    identity = {'role': 'main'}
    active = {'task': 'review-task', 'pr': {'number': 7}, 'branch': 'fixture-branch',
              'worktree': '/fixture/worktree', 'lane': 'fixture-lane',
              'plane_id': 'SYSTEM-171'}
    batch = {'item': active, 'flow_version': 3}
    contract = {'actions': {'main': ['review']},
                'model': {'required_model': 'fixture-model', 'provider': 'fixture-provider',
                          'reasoning_effort': 'high'},
                'resources': {'repo': '/fixture/repo'}}
    report_ref = {'sha256': 'b' * 64}
    run_ref = {'sha256': 'c' * 64}
    report = {'reviewed_head_sha': head, 'passed': True, 'score_5': 5,
              'merge_recommendation': 'merge', 'blocking_findings': [],
              'hard_gate_failures': [], 'acceptance_coverage': ['fixture check'],
              'deterministic_gates': ['fixture gate'], 'summary': 'reviewed'}
    genuine = {'evidence_kind': 'fixture-direct-kind', 'evidence_version': 2,
               'source': source,
               'child_cleanup': {'verified': True}}
    run = {'role': 'independent_reviewer', 'exit_code': 0, 'head': head, 'base': base,
           'model': contract['model']['required_model'],
           'provider': contract['model']['provider'],
           'reasoning_effort': contract['model']['reasoning_effort'],
           'reviewer_identity': {'pid': 202}, 'implementer_identity': {'pid': 101},
           'visible_target': {'surface': 'review'}, 'report_sha256': report_ref['sha256'],
           'completion_receipt': 'fixture-skills'}
    if direct_evidence != 'missing':
        run.update(genuine)
    if direct_evidence == 'invalid':
        run['child_cleanup'] = None
    elif direct_evidence == 'missing_source':
        run.pop('source')
    elif direct_evidence == 'mismatched_source':
        run['source'] = dict(source, branch='other-branch')
    elif direct_evidence == 'evidence_source_only':
        run['evidence_source'] = run.pop('source')
    rows = {'review-task': {'direct_worker': True, 'generation': 1}}
    guard = SimpleNamespace(transaction=lambda read_only=False: nullcontext(rows))
    monkeypatch.setattr(item, 'snapshot', lambda *_: (batch, contract))
    monkeypatch.setattr(item, 'publish_check', lambda *_: None)
    monkeypatch.setattr(item, 'current_pr', lambda *_: {'state': 'OPEN', 'headRefOid': head})
    monkeypatch.setattr(item, 'live_item', lambda *_: (batch, contract, active))

    def read_fixture(ref, _contract):
        if ref == report_ref:
            return report
        if ref == run_ref:
            return run
        assert ref == 'fixture-skills'
        return {'loaded': True}

    monkeypatch.setattr(item, 'read_evidence', read_fixture)
    calls = []

    def native_reader(request):
        calls.append(request)
        # Capture the caller payload. A stub cannot establish native review success.
        return {'passed': False, 'qualified': False}

    monkeypatch.setattr(item, 'external_readback', native_reader)
    payload = {'batch': 'fixture-batch', 'issue': 'fixture-issue',
               'evidence': report_ref, 'review_run': run_ref}
    with pytest.raises(ValueError, match='required-skills and semantic reviewer gate failed'):
        item.review(guard, identity, payload)
    assert 'review' not in active
    assert len(calls) == 1
    assert {key: calls[0][key] for key in genuine if key in calls[0]} == {
        key: run[key] for key in genuine if key in run}
    assert set(calls[0]) & set(genuine) == set(run) & set(genuine)
    for key in genuine:
        if key in run:
            assert calls[0][key] is run[key]
    assert 'evidence_source' not in calls[0]
    if direct_evidence in {'complete', 'invalid'}:
        _installed_review_source_binding(calls[0], source)
    else:
        with pytest.raises(ValueError, match='source binding missing or inconsistent'):
            _installed_review_source_binding(calls[0], source)
    if direct_evidence == 'invalid':
        assert not isinstance(calls[0]['child_cleanup'], dict)


@pytest.mark.asyncio
@pytest.mark.parametrize('followup,main_moved', [(False, False), (False, True), (True, False)])
async def test_current_head_review_merge_closeout_and_lane_release(route, monkeypatch, followup, main_moved):
    r = route
    contract, payload, candidate_payload = await completed_candidate(r, monkeypatch)
    candidate = item.control(r.guard, r.identity, candidate_payload)['candidate']
    await r.runner._diggr_accept(await r.event('/continuation accept ' + candidate['id']))
    head = signed_commit(r, payload['worktree'])
    pr = dict(number=7, url='https://example.invalid/repo/pull/7', headRefOid=head,
              headRefName=payload['branch'], baseRefName='main', state='OPEN', mergedAt=None,
              mergeCommit=None, isCrossRepository=False)
    plane = dict(issue=payload['issue'], plane_id='SYSTEM-167', state='Review')

    def readback(p):
        if p['operation'] == 'github':
            return copy.deepcopy(pr)
        if p['operation'] == 'review':
                return dict(passed=True, qualified=True, hard_gates_passed=True)  # Actual system gate has its own test.
        return dict(github=copy.deepcopy(pr), plane=copy.deepcopy(plane),
                    nextcloud=dict(path=p['nextcloud_path'], sha256=p['nextcloud_sha256']))

    monkeypatch.setattr(item, 'external_readback', readback)
    pr_request = dict(payload, operation='item_pr', pr_number=7)
    first_pr = item.control(r.guard, r.identity, pr_request)
    assert item.control(r.guard, r.identity, pr_request) == first_pr  # lost reply: remote readback, same PR
    with pytest.raises(ValueError, match='independent review'):
        item.control(r.guard, r.identity, dict(payload, operation='item_merge_check'))
    report = evidence(contract, 'review.json', dict(reviewed_head_sha=head, passed=True,
        score_5=5, merge_recommendation='merge', blocking_findings=[], hard_gate_failures=[],
        acceptance_coverage=['fixture invariant'], deterministic_gates=['fixture test'], summary='pass'))
    run = evidence(contract, 'review-run.json', dict(role='independent_reviewer', exit_code=0, head=head,
        model=contract['model']['required_model'], provider=contract['model']['provider'],
        reasoning_effort=contract['model']['reasoning_effort'], reviewer_identity={'pid':202},
        implementer_identity={'pid':101}, visible_target={'workspace':'review','surface':'review'},
        report_sha256=report['sha256'], run_id='fixture-review',
        completion_receipt=evidence(contract, 'skills.json', dict(fixture=True))))
    review_payload = dict(payload, operation='item_review', evidence=report, review_run=run)
    item.control(r.guard, r.identity, review_payload)
    check = item.control(r.guard, r.identity, dict(payload, operation='item_merge_check'))
    assert check['match_head_commit'] == head and check['may_merge']
    with pytest.raises(ValueError, match='not actually merged'):
        item.control(r.guard, r.identity, dict(payload, operation='item_closeout'))
    pr['headRefOid'] = '0' * 40
    with pytest.raises(ValueError, match='differs'):
        item.control(r.guard, r.identity, dict(payload, operation='item_merge_check'))
    pr.update(headRefOid=head, state='MERGED', mergedAt='fixture-time', mergeCommit={'oid':head})
    repo = contract['resources']['repo']
    remote = r.tmp / 'remote.git'
    subprocess.run(['git', 'init', '--bare', str(remote)], check=True, capture_output=True)
    git(repo, 'remote', 'add', 'origin', str(remote))
    git(repo, 'merge', '--ff-only', head)
    git(repo, 'push', 'origin', 'main')
    if followup:
        git(repo, 'fetch', 'origin')
        failure = evidence(contract, 'integrated-failure.json', dict(main_sha=head, merge_commit=head,
            result='failed', findings=['integrated playback regression']))
        next_path = str(Path(contract['resources']['worktrees']) / 'followup')
        prep = dict(payload, operation='item_followup_prepare', evidence=failure,
                    worktree=next_path, branch='hermes/WP-SYSTEM-167-followup', base=head)
        real_git = item.git
        def interrupted(path, *args):
            if args[:2] == ('worktree', 'add'):
                raise OSError('interrupted before Git created the path')
            return real_git(path, *args)
        monkeypatch.setattr(item, 'git', interrupted)
        with pytest.raises(ValueError, match='creation unresolved'):
            item.control(r.guard, r.identity, prep)
        monkeypatch.setattr(item, 'git', real_git)
        prepared = item.control(r.guard, r.identity, prep)
        assert item.control(r.guard, r.identity, prep) == prepared
        assert Path(payload['worktree']).exists()
        r.clock[0] += 20
        wake = r.guard.tick(r.identity)
        assert wake and r.guard.begin(r.identity, wake)
        row = r.guard.get(r.task['task'])
        effect = evidence(contract, 'postmerge-effect.json', dict(task=row['task'], action_id=row['action_id'],
            effect_id=row['effect_id'], result='reconciled', merged_head=head))
        report = evidence(contract, 'postmerge-reconcile.json', dict(
            **{k: row[k] for k in ('task', 'generation', 'identity', 'action', 'action_id', 'effect_id')},
            outcome='reconciled', observations=[failure], effect_proof=effect))
        fresh = copy.deepcopy(row['worker_route'])
        packet = json.loads(Path(fresh['packet']).read_text())
        packet.update(worktree=next_path, branch=prep['branch'], base=head,
                      retained_worktree_sha256=dc._worktree_snapshot(next_path))
        packet_path = Path(fresh['packet']).with_name('postmerge-packet.json')
        packet_path.write_text(json.dumps(packet))
        artifact = str(Path(contract['resources']['artifacts']) / 'postmerge-result.txt')
        fresh.update(worktree=next_path, branch=prep['branch'], packet=str(packet_path),
                     packet_sha256=dc.digest(packet_path), receipt=str(packet_path.with_name('postmerge-receipt.json')))
        fresh['argv'] = [next_path if s == payload['worktree'] else s for s in fresh['argv']]
        fresh['argv'][fresh['argv'].index('--output-last-message') + 1] = artifact
        fresh['argv'][-1] = item.onboarding_payload()[0] + '\n\nCorrect the integrated playback regression in the same item'
        monkeypatch.setattr(dc, 'verify_restarted_reservation_exit', lambda _: None)
        monkeypatch.setattr(dc, 'verify_prior_exit', lambda *a, **kw: None)
        token = dc.EVENT_CONTEXT.set(dict(identity=r.identity))
        try:
            ticket = dc.maintenance_control(dict(operation='recover', wake=wake, evidence=report,
                strategy=dict(kind='review', reason='integrated failure', hypothesis='repair integrated path',
                              artifact=artifact, worker_route=fresh)))
        finally:
            dc.EVENT_CONTEXT.reset(token)
        after = r.guard.get(r.task['task'])
        assert ticket == dc.worker_ticket(after)
        assert after['policy']['corrections'] == 1 and not after.get('worker_pid')
        state = item.status(r.guard, r.identity, payload['batch'], payload['issue'])
        assert state['item']['worktree'] == next_path and state['phase'] == 'coding'
        assert state['item']['candidate_history'][-1]['pr']['number'] == 7
        return
    smoke = evidence(contract, 'smoke.json', dict(main_sha=head, merge_commit=head, worktree=repo,
                                                result='passed', checks=['integrated fixture']))
    archive = evidence(contract, 'closeout.json', dict(issue=payload['issue'], candidate=candidate['id'],
        pr_head=head, main_sha=head, smoke_sha256=smoke['sha256']))
    close_payload = dict(payload, operation='item_closeout', main_worktree=repo, smoke=smoke,
                         archive=archive, nextcloud_path='/Projects/DIGGR/fixture/closeout.json')
    if main_moved:
        git(repo, '-c', 'commit.gpgsign=false', 'commit', '--allow-empty', '-m', 'later main change')
        git(repo, 'push', 'origin', 'main')
        with pytest.raises(ValueError, match='smoke evidence must match current main'):
            item.control(r.guard, r.identity, close_payload)
        integrated_head = git(repo, 'rev-parse', 'HEAD')
        smoke = evidence(contract, 'fresh-smoke.json', dict(main_sha=integrated_head, merge_commit=head,
            worktree=repo, result='passed', checks=['fresh integrated fixture']))
        archive = evidence(contract, 'fresh-closeout.json', dict(issue=payload['issue'],
            candidate=candidate['id'], pr_head=head, main_sha=integrated_head,
            smoke_sha256=smoke['sha256']))
        close_payload.update(smoke=smoke, archive=archive)
    with pytest.raises(ValueError, match='closeout is incomplete'):
        item.control(r.guard, r.identity, close_payload)
    assert item.status(r.guard, r.identity, payload['batch'], payload['issue'])['phase'] != 'closed'
    plane.update(state='Done', state_group='cancelled', completion_state='Done')
    with pytest.raises(ValueError, match='closeout is incomplete'):
        item.control(r.guard, r.identity, close_payload)
    plane.update(state_group='completed', completion_state='Accepted')
    with pytest.raises(ValueError, match='closeout is incomplete'):
        item.control(r.guard, r.identity, close_payload)
    plane.update(completion_state='Done')
    receipt = item.control(r.guard, r.identity, close_payload)
    assert receipt['main_sha'] == (integrated_head if main_moved else head)
    assert item.control(r.guard, r.identity, close_payload) == receipt
    assert item.status(r.guard, r.identity, payload['batch'], payload['issue'])['phase'] == 'closed'
    assert r.guard.get(r.task['task'])['status'] == 'done'
    if not main_moved:
        second = manifest(r.task, number=2)
        next_contract = copy.deepcopy(contract)
        next_contract['plane_id'] = 'SYSTEM-168'
        second['todos'][0]['contract'] = next_contract
        next_batch, _, _ = await confirm(r, second)
        next_payload = dict(operation='item_prepare', batch=next_batch,
            issue=owner.issue_key(second['todos'][0]),
            worktree=str(Path(contract['resources']['worktrees']) / 'next-item'),
            branch='hermes/WP-SYSTEM-168-next', base=git(repo, 'rev-parse', 'origin/main^{commit}'),
            target=payload['target'])
        next_item = item.control(r.guard, r.identity, next_payload)
        assert next_item['phase'] == 'prepared'
        assert next_item['coding_assignment']['status'] == 'pending'
        binding = dict(workspace='fixture', surface='fixture',
            process=dict(identity=dict(pid=os.getpid()), argv=['hermes', '--resume', 'coding']))
        monkeypatch.setattr(dc, 'preflight_coding_coordinator', lambda: binding)
        assignment_id = next_item['coding_assignment']['id']
        with r.guard.transaction() as rows:
            next_batch_row = rows.grants['batches'][next_batch]
            next_batch_row['native_transport'] = copy.deepcopy(
                rows[r.task['task']]['native_transport'])
            next_batch_row['item']['coding_assignment'].update(status='sent', binding=binding)
        coding_home = Path(r.identity['home']).parent / 'diggr-coding'
        assert dc.accept_coding_assignment('SYSTEM171_ASSIGN ' + assignment_id,
            'second-coding-session', coding_home)
        guard, second_worker = dc.register_assigned_worker(assignment_id,
            'second-coding-session', coding_home, 'Implement and test the second approved item.')
        sends = []
        monkeypatch.setattr(dc, 'cmux_send', lambda target, command: sends.append(command))
        dc.start_assigned_worker(guard, second_worker)
        assert len(sends) == 1
        second_worker = _complete_item_attempt_os(
            guard, second_worker['task'], second_worker['artifact'],
            output='Second item implemented and tested.\n')
        worker_result = second_worker['worker_result']
        assert guard.observe(second_worker['task'], second_worker['generation'],
            'completed', worker_result['evidence'])
        second_candidate = item.control(guard, r.identity, dict(next_payload,
            operation='item_candidate', checks=['second item scoped diff and tests'],
            steps=[dict(action='Open second item', expected='Approved behavior')],
            build_argv=[sys.executable, '-c',
                "from pathlib import Path; Path('build').mkdir(exist_ok=True); Path('build/app').write_text('second')"],
            build_path=str(Path(next_payload['worktree']) / 'build')))['candidate']
        await r.runner._diggr_accept(await r.event('/continuation accept ' + second_candidate['id']))
        assert item.control(guard, r.identity, dict(next_payload, operation='item_publish_check'))['may_publish']
        second_head = signed_commit(r, next_payload['worktree'], suffix='-2')
        second_pr = dict(number=8, url='https://example.invalid/repo/pull/8',
            headRefOid=second_head, headRefName=next_payload['branch'], baseRefName='main',
            state='OPEN', mergedAt=None, mergeCommit=None, isCrossRepository=False)
        second_plane = dict(issue=next_payload['issue'], plane_id='SYSTEM-168',
            state='Done', state_group='completed', completion_state='Done')
        def second_readback(request):
            if request['operation'] == 'github':
                return copy.deepcopy(second_pr)
            if request['operation'] == 'review':
                return dict(passed=True, qualified=True, hard_gates_passed=True)
            return dict(github=copy.deepcopy(second_pr), plane=copy.deepcopy(second_plane),
                nextcloud=dict(path=request['nextcloud_path'], sha256=request['nextcloud_sha256']))
        monkeypatch.setattr(item, 'external_readback', second_readback)
        item.control(guard, r.identity, dict(next_payload, operation='item_pr', pr_number=8))
        second_report = evidence(next_contract, 'second-review.json', dict(
            reviewed_head_sha=second_head, passed=True, merge_recommendation='merge',
            blocking_findings=[], hard_gate_failures=[],
            acceptance_coverage=['second fixture'], deterministic_gates=['second test'], summary='pass'))
        second_run = evidence(next_contract, 'second-review-run.json', dict(
            role='independent_reviewer', exit_code=0, head=second_head,
            model=next_contract['model']['required_model'],
            provider=next_contract['model']['provider'],
            reasoning_effort=next_contract['model']['reasoning_effort'],
            reviewer_identity={'pid': 204}, implementer_identity={'pid': 203},
            visible_target={'workspace': 'review', 'surface': 'review'},
            report_sha256=second_report['sha256'], run_id='second-fixture-review',
            completion_receipt=evidence(next_contract, 'second-skills.json', dict(fixture=True))))
        second_review = dict(next_payload, operation='item_review',
            evidence=second_report, review_run=second_run)
        reviewed = item.control(guard, r.identity, second_review)
        with guard.transaction(read_only=True) as rows:
            assert 'review_closeout_continuation' not in rows.grants['batches'][next_batch]['item']
            assert rows[second_worker['task']]['status'] == 'awaiting_user'
            assert rows[second_worker['task']]['gate'] == 'item_merge'
            awaiting_generation = rows[second_worker['task']]['generation']
        assert item.control(guard, r.identity, second_review) == reviewed
        assert guard.get(second_worker['task'])['generation'] == awaiting_generation
        with pytest.raises(ValueError, match='owner merge approval required'):
            item.control(guard, r.identity, dict(next_payload, operation='item_merge_check'))
        with pytest.raises(ValueError, match='current reviewed PR head'):
            await r.runner._diggr_accept(await r.event(
                '/continuation merge-approve ' + second_candidate['id'] + ' ' + '0' * 40))
        await r.runner._diggr_accept(await r.event(
            '/continuation merge-approve ' + second_candidate['id'] + ' ' + second_head))
        with guard.transaction(read_only=True) as rows:
            assert rows[second_worker['task']]['status'] == 'pending'
            assert rows[second_worker['task']]['gate'] == 'main_live'
            approved_generation = rows[second_worker['task']]['generation']
        await r.runner._diggr_accept(await r.event(
            '/continuation merge-approve ' + second_candidate['id'] + ' ' + second_head))
        assert guard.get(second_worker['task'])['generation'] == approved_generation
        assert item.control(guard, r.identity, dict(next_payload,
            operation='item_merge_check'))['may_merge']
        second_pr.update(state='MERGED', mergedAt='second-fixture-time',
            mergeCommit={'oid': second_head})
        git(repo, 'merge', '--ff-only', second_head)
        git(repo, 'push', 'origin', 'main')
        second_smoke = evidence(next_contract, 'second-smoke.json', dict(
            main_sha=second_head, merge_commit=second_head, worktree=repo,
            result='passed', checks=['integrated second fixture']))
        second_archive = evidence(next_contract, 'second-closeout.json', dict(
            issue=next_payload['issue'], candidate=second_candidate['id'],
            pr_head=second_head, main_sha=second_head,
            smoke_sha256=second_smoke['sha256']))
        item.control(guard, r.identity, dict(next_payload, operation='item_closeout',
            main_worktree=repo, smoke=second_smoke, archive=second_archive,
            nextcloud_path='/Projects/DIGGR/fixture/second-closeout.json'))
        assert item.status(guard, r.identity, next_batch, next_payload['issue'])['phase'] == 'closed'


@pytest.mark.asyncio
async def test_extension_is_one_owner_event_and_keeps_spent_work(route, monkeypatch):
    r = route
    _, payload = await setup(r, monkeypatch)
    item.control(r.guard, r.identity, payload)
    register(r)
    with r.guard.transaction() as rows:
        rows.grants['batches'][payload['batch']]['item']['work_seconds'] = 123
    status = item.status(r.guard, r.identity, payload['batch'], payload['issue'])
    event = await r.event('/continuation extend ' + payload['batch'] + ' ' + status['budget_hash'] + ' 600 2 1 0')
    await r.runner._diggr_accept(event)
    await r.runner._diggr_accept(event)
    status = item.status(r.guard, r.identity, payload['batch'], payload['issue'])
    assert status['item']['budget']['todo_seconds'] == owner.BUDGETS['todo_seconds'] + 600
    assert status['item']['work_seconds'] == 123
    assert len(status['item']['budget_extensions']) == 1


@pytest.mark.asyncio
@pytest.mark.live_system_guard_bypass
async def test_actual_work_budget_stops_only_owned_child(route, monkeypatch):
    r = route
    _, payload = await setup(r, monkeypatch)
    item.control(r.guard, r.identity, payload)
    register(r)
    with r.guard.transaction() as rows:
        rows.grants['batches'][payload['batch']]['item']['budget']['todo_seconds'] = .05
        rows[r.task['task']]['policy']['work_limit'] = .05
    row = r.guard.get(r.task['task'])
    child, handle, row = _spawn_bound_os_worker(
        r.guard, r.task['task'], [sys.executable, '-c', 'import time; time.sleep(3)'])
    unbound = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(3)'])
    try:
        started = time.monotonic()
        with pytest.raises(ValueError, match='^worker stopped by budget or revocation$'):
            item.wait_worker(r.guard, row, child, started, handle)
        assert child.poll() is not None
        assert unbound.poll() is None
        stopped = r.guard.get(row['task'])
        proof = stopped['worker_supervision']
        assert proof['state'] == 'unknown' and proof['accounted_seconds'] > 0
        with r.guard.transaction(read_only=True) as rows:
            spent = rows.grants['batches'][payload['batch']]['item']['work_seconds']
        assert spent == proof['accounted_seconds']
        with pytest.raises(ValueError, match='already recorded'):
            item.wait_worker(r.guard, row, child, started, handle)
        with r.guard.transaction(read_only=True) as rows:
            assert rows.grants['batches'][payload['batch']]['item']['work_seconds'] == spent
    finally:
        item.close_item_group(handle)
        for process in (child, unbound):
            if process.poll() is None:
                try:
                    process.terminate()  # each PID was started by this test
                except RuntimeError:
                    pass  # sandbox test guard; bounded child exits by itself
            process.wait(timeout=5)


@pytest.mark.asyncio
@pytest.mark.live_system_guard_bypass
@pytest.mark.parametrize('detached', [False, True], ids=['inherited-group', 'detached-session'])
async def test_worker_leader_exit_before_first_scan_keeps_live_helper_reserved(route, monkeypatch, tmp_path,
                                                                               detached):
    r = route
    _, payload = await setup(r, monkeypatch)
    item.control(r.guard, r.identity, payload)
    register(r)
    ready = tmp_path / 'helper-ready'
    release = tmp_path / 'helper-release'
    done = tmp_path / 'helper-done'
    pid_file = tmp_path / 'helper-pid'
    helper_code = (
        'import os,sys,time; from pathlib import Path; '
        'ready,release,done=map(Path,sys.argv[1:]); '
        f'{_atomic_test_pid_code("ready", "os.getpid()")}'
        'end=time.monotonic()+4; '
        'exec("while not release.exists() and time.monotonic()<end: time.sleep(.02)"); '
        'done.touch()'
    )
    leader_code = (
        'import subprocess,sys,time; from pathlib import Path; '
        'ready,release,done,pid_file=map(Path,sys.argv[1:]); '
        f'child=subprocess.Popen([sys.executable,"-c",{helper_code!r},str(ready),str(release),str(done)], '
        f'start_new_session={detached!r}); '
        f'{_atomic_test_pid_code("pid_file", "child.pid")}'
        'end=time.monotonic()+2; '
        'exec("while not ready.exists() and time.monotonic()<end: time.sleep(.02)"); '
        'sys.exit(0 if ready.exists() else 2)'
    )
    work_started = time.monotonic()
    leader, handle, attempt = _spawn_bound_os_worker(
        r.guard, r.task['task'], [sys.executable, '-c', leader_code,
        str(ready), str(release), str(done), str(pid_file)])
    helper_pid = None
    try:
        assert leader.wait(timeout=3) == 0
        helper_pid = int(pid_file.read_text())
        assert int(ready.read_text()) == helper_pid
        os.kill(helper_pid, 0)  # real OS process remains after proven leader exit
        if detached:
            assert os.getpgid(helper_pid) == helper_pid
        try:
            code = item.wait_worker(r.guard, attempt, leader, work_started, handle)
        except ValueError as exc:  # explicit incomplete proof is a safe blocked result
            assert any(word in str(exc).lower() for word in ('exit', 'unknown', 'incomplete', 'ownership'))
        else:
            assert code == 0
            elapsed = time.monotonic() - work_started
            native_result = r.guard.record_worker_result(attempt, code, work_seconds=elapsed)
            r.guard.observe(attempt['task'], attempt['generation'], 'completed', native_result['evidence'])
            current = r.guard.get(attempt['task'])
            assert done.exists() or current['status'] != 'pending', (
                'native Guard admitted the next step while the real helper was alive')
            assert done.exists(), 'leader exit released the worker while its real helper was alive'
            with r.guard.transaction(read_only=True) as rows:
                spent = rows.grants['batches'][payload['batch']]['item']['work_seconds']
            assert spent >= elapsed - .05, 'helper walltime was not charged to the item'
    finally:
        release.touch()
        item.close_item_group(handle)
        if leader.poll() is None:
            leader.wait(timeout=3)
        if helper_pid is not None:
            until = time.monotonic() + 5
            while not done.exists() and time.monotonic() < until:
                time.sleep(.02)
            assert done.exists(), 'test-owned helper did not complete its bounded lifetime'


@pytest.mark.asyncio
@pytest.mark.live_system_guard_bypass
async def test_worker_leader_exit_after_first_scan_keeps_captured_helper_reserved(route, monkeypatch, tmp_path):
    import threading

    r = route
    _, payload = await setup(r, monkeypatch)
    item.control(r.guard, r.identity, payload)
    register(r)
    ready = tmp_path / 'captured-helper-ready'
    leader_release = tmp_path / 'captured-leader-release'
    helper_release = tmp_path / 'captured-helper-release'
    helper_done = tmp_path / 'captured-helper-done'
    pid_file = tmp_path / 'captured-helper-pid'
    helper_code = (
        'import os,sys,time; from pathlib import Path; '
        'ready,release,done=map(Path,sys.argv[1:]); '
        f'{_atomic_test_pid_code("ready", "os.getpid()")}'
        'end=time.monotonic()+6; '
        'exec("while not release.exists() and time.monotonic()<end: time.sleep(.02)"); done.touch()'
    )
    leader_code = (
        'import subprocess,sys,time; from pathlib import Path; '
        'ready,leader_release,helper_release,helper_done,pid_file=map(Path,sys.argv[1:]); '
        f'child=subprocess.Popen([sys.executable,"-c",{helper_code!r},str(ready),str(helper_release),str(helper_done)]); '
        f'{_atomic_test_pid_code("pid_file", "child.pid")}'
        'end=time.monotonic()+4; '
        'exec("while not ready.exists() and time.monotonic()<end: time.sleep(.02)"); '
        'exec("while not leader_release.exists() and time.monotonic()<end: time.sleep(.02)"); '
        'sys.exit(0 if ready.exists() else 2)'
    )
    work_started = time.monotonic()
    leader, handle, attempt = _spawn_bound_os_worker(
        r.guard, r.task['task'], [sys.executable, '-c', leader_code, str(ready),
        str(leader_release), str(helper_release), str(helper_done), str(pid_file)])
    result = {}
    thread = None
    try:
        until = time.monotonic() + 2
        while not ready.exists() and time.monotonic() < until:
            time.sleep(.02)
        helper_pid = int(pid_file.read_text())
        assert int(ready.read_text()) == helper_pid
        def supervise():
            try:
                result['code'] = item.wait_worker(r.guard, attempt, leader, work_started, handle)
            except BaseException as exc:
                result['error'] = exc

        thread = threading.Thread(target=supervise, daemon=True)
        thread.start()
        until = time.monotonic() + 2
        while time.monotonic() < until:
            if 'error' in result:
                raise result['error']
            if any(entry['pid'] == helper_pid for entry in
                   r.guard.get(r.task['task']).get('worker_descendants', [])):
                break
            time.sleep(.02)
        else:
            pytest.fail('native supervisor did not capture the real helper before leader exit')
        leader_release.touch()
        assert leader.wait(timeout=3) == 0
        os.kill(helper_pid, 0)
        thread.join(timeout=2)
        if not thread.is_alive() and not helper_done.exists():
            assert isinstance(result.get('error'), ValueError) and any(
                word in str(result['error']).lower() for word in
                ('exit', 'unknown', 'incomplete', 'ownership')), (
                'captured helper was released when its leader exited')
    finally:
        leader_release.touch()
        helper_release.touch()
        item.close_item_group(handle)
        if leader.poll() is None:
            leader.wait(timeout=5)
        if thread is not None:
            thread.join(timeout=5)
            assert not thread.is_alive(), 'supervisor did not stop after bounded helper lifetime'
        until = time.monotonic() + 5
        while not helper_done.exists() and time.monotonic() < until:
            time.sleep(.02)
        assert helper_done.exists(), 'test-owned helper did not complete its bounded lifetime'


def _spawn_bound_os_worker(guard, task, argv, *, cwd=None, **popen_kwargs):
    """Exercise the same pre-exec group creation and root-TTY handoff as Item."""
    row = guard.get(task)
    leader, handle = item.spawn_item_child(argv, cwd or str(Path(guard.path).parent),
                                           row, **popen_kwargs)
    try:
        return leader, handle, _bind_started_os_worker(guard, task, leader, handle)
    except BaseException:
        item.close_item_group(handle)
        raise


def _bind_started_os_worker(guard, task, leader, handle):
    """Use actual process identities; ownership comes from the spawn handle."""
    wrapper = dc.process_identity(os.getpid())
    child = dc.process_identity(leader.pid)
    assert wrapper and child
    with guard.transaction() as rows:
        row = rows[task]
        row.update(worker_pid=os.getpid(), worker_identity=wrapper,
                   worker_started_ns=time.time_ns(), child_identity=child,
                   worker_group=handle['binding'], claim_phase='claimed', visible_sent=True)
    return guard.get(task)


def _run_sealed_fixture_worker(guard, ticket, argv, artifact):
    """Let the bound disposable executable run through the real held spawn."""
    import threading

    assert Path(argv[0]).is_file() and os.access(argv[0], os.X_OK)
    release = Path(artifact + '.release')
    release.unlink(missing_ok=True)

    def release_after_observation():
        until = time.monotonic() + 6
        while time.monotonic() < until:
            if (guard.get(ticket['task']).get('worker_supervision') or {}).get('state') == 'observing':
                release.touch()
                return
            time.sleep(.02)

    watcher = threading.Thread(target=release_after_observation, daemon=True)
    watcher.start()
    try:
        result = dc.main(['--home', ticket['identity']['home'], 'worker', '--payload-json',
                          json.dumps(ticket)])
        assert release.exists()
        row = guard.get(ticket['task'])
        marker = Path(artifact + '.spawn')
        assert marker.is_file(), 'held child never execed the sealed fixture argv'
        spawned = json.loads(marker.read_text())
        assert spawned == dict(pid=row['child_identity']['pid'],
                               pgid=row['worker_group']['pgid'])
        return result, [spawned]
    finally:
        release.touch()
        watcher.join(timeout=7)
        assert not watcher.is_alive()


def _guard_real_cmux_subprocesses(monkeypatch):
    """Fail at the process boundary if this opt-in fixture reaches real cMux."""
    import shlex

    attempts = []
    guarded_popen = subprocess.Popen  # Preserve the root conftest's process guard.

    class NoRealCmux(guarded_popen):
        def __init__(self, command, *args, **kwargs):
            parts = shlex.split(command) if isinstance(command, str) else list(command)
            executable = os.fspath(parts[0]) if parts else ''
            if executable == dc.CMUX or Path(executable).name == 'cmux':
                attempts.append(executable)
                raise AssertionError('real cmux subprocess forbidden in native V2 fixture: ' + executable)
            super().__init__(command, *args, **kwargs)

    monkeypatch.setattr(subprocess, 'Popen', NoRealCmux)
    return attempts


def _complete_native_v2_item_attempt(r, monkeypatch, *, exit_code=0, output_present=True):
    """Exercise native V2 gates with owned processes and external terminal seams.

    invoke, cmux_send, verify_visible_target and verify_visible_claim stop at the terminal boundary;
    the sealed argv executes a disposable bounded child in place of Codex. These
    seams do not prove shell parentage, TTY/PGID/foreground or cMux snapshots.
    """
    from tools.process_registry import ProcessSession, process_registry
    import yaml

    # Parent and fresh child must use the same already configured profile home.
    home = Path(r.identity['home']).resolve()
    assert home == r.tmp.resolve()
    profile = r.identity['profile']
    assert profile in delivery.PROFILES
    runtime_path = home / 'state' / 'diggr-continuation.json'
    assert r.guard.path == runtime_path and runtime_path.exists()
    config_path = home / 'config.yaml'
    assert not config_path.is_symlink()
    config = yaml.safe_load(config_path.read_text())
    assert config['diggr_continuation'] == dict(enabled=True, profile=profile, home=str(home))
    assert r.guard.profile == profile
    assert dc.runtime_guard(home).path == runtime_path
    dispatcher = r.tmp / 'system' / 'ops' / 'scripts' / 'loop_control_dispatch_coding.sh'
    assert dc.DISPATCHER == str(dispatcher) and dispatcher.is_file() and not dispatcher.is_symlink()
    assert dispatcher.resolve().is_relative_to(home)
    packet_root = dc.logical_packet_root()
    assert packet_root == (dispatcher.parents[2] / 'ops/state/loop-control/routing_packets').resolve()
    assert packet_root.is_relative_to(home)
    assert dc.CODEX_EXECUTABLE == r.task['worker_route']['argv'][0]
    onboarding, skill_dir = item.onboarding_payload()
    assert skill_dir == str((home / 'skills' / item.ONBOARDING_SKILL).resolve())
    assert onboarding in r.task['worker_route']['argv'][-1]
    item.require_onboarding_context()

    launcher_release = r.tmp / 'v2-launcher.release'
    launcher_release.unlink(missing_ok=True)
    launcher = None
    session = None
    def invoke(launch):
        nonlocal launcher, session
        # This is the external terminal seam. The native reservation, registry
        # ownership check and launcher identity capture remain in terminal_dispatch.
        code = ('import sys,time; from pathlib import Path; '
                'release=Path(sys.argv[1]); end=time.monotonic()+20; '
                'exec("while not release.exists() and time.monotonic()<end: time.sleep(.02)"); '
                'sys.exit(0 if release.exists() else 2)')
        launcher = subprocess.Popen([sys.executable, '-B', '-c', code, str(launcher_release)],
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        session = ProcessSession(id='fixture-v2-' + str(launcher.pid), command=launch['command'],
                                 session_key=r.identity['session_key'], started_at=time.time(),
                                 pid=launcher.pid, process=launcher, pid_scope='host')
        with process_registry._lock:
            assert session.id not in process_registry._running
            process_registry._running[session.id] = session
        return dict(session_id=session.id)

    # The worker runs in its own short-lived wrapper so the recorded worker PID
    # really exits before Main evaluates the candidate and recovery request.
    worker_script = '''
import json, os, subprocess, sys, threading, time
from pathlib import Path
from hermes_cli import diggr_continuation as dc
from hermes_cli import diggr_item as item
dc.DISPATCHER = sys.argv[6]
dc.CODEX_EXECUTABLE = sys.argv[7]
time.time = lambda: float(sys.argv[3])  # Same disposable fixture clock as Main.
guard = dc.Guard(Path(sys.argv[1]))
ticket = json.loads(sys.argv[2])
row = guard.get(ticket['task'])
home = Path(ticket['identity']['home']).resolve()
dispatcher = home / 'system' / 'ops' / 'scripts' / 'loop_control_dispatch_coding.sh'
assert dc.DISPATCHER == str(dispatcher) and dispatcher.is_file() and not dispatcher.is_symlink()
assert dispatcher.resolve().is_relative_to(home)
route = row['worker_route']
expected_binding = json.loads(sys.argv[8])  # Registered, sealed disposable fixture binding.
assert row['visible_binding'] == expected_binding
assert route['visible_target'] == expected_binding['target']
contract = row['logical_contract']
artifacts = Path(contract['resources']['artifacts']).resolve()
assert artifacts.is_relative_to(home)
assert dc.logical_packet_root() == artifacts / 'routing_packets'
packet, receipt, result = (Path(path).resolve() for path in
                           (route['packet'], route['receipt'], row['artifact']))
assert len({packet, receipt, result}) == 3
assert packet.is_relative_to(dc.logical_packet_root())
assert receipt.is_relative_to(artifacts) and result.is_relative_to(artifacts)
assert row['launcher_command'] == dc.logical_launcher_command(contract, route)
content, directory = item.onboarding_payload()
assert content == sys.argv[4]
assert directory == sys.argv[5]
assert content in row['worker_route']['argv'][-1]
assert dc.runtime_guard(ticket['identity']['home']).path == guard.path
release = Path(row['artifact'] + '.release')
release.unlink(missing_ok=True)
authorized = row['worker_route']['argv']
assert authorized[0] == dc.CODEX_EXECUTABLE
original_popen = subprocess.Popen
cmux_attempts = []
class NoRealCmux(original_popen):
    def __init__(self, command, *args, **kwargs):
        import shlex
        parts = shlex.split(command) if isinstance(command, str) else list(command)
        executable = os.fspath(parts[0]) if parts else ''
        if executable == dc.CMUX or Path(executable).name == 'cmux':
            cmux_attempts.append(executable)
            raise AssertionError('real cmux subprocess forbidden in fresh V2 worker: ' + executable)
        super().__init__(command, *args, **kwargs)
subprocess.Popen = NoRealCmux
target_checks = []
def verify_fixture_target(binding, idle=False):
    frame = sys._getframe()
    callers = []
    route_row = None
    while frame is not None:
        callers.append(frame.f_code.co_name)
        if frame.f_code.co_name == 'route_check':
            route_row = frame.f_locals.get('row')
        frame = frame.f_back
    assert 'route_check' in callers and 'preflight_non_cmm' in callers
    assert route_row is not None and not route_row.get('worker_pid'), 'target checked after worker claim'
    assert binding == expected_binding, 'receipt consumed a different visible binding'
    assert binding['target'] == route['visible_target'], 'receipt consumed a different target'
    target_checks.append(dict(binding=binding, idle=idle, before_claim=True))
    return binding['target'], binding.get('shell')  # Simulated terminal evidence only.
dc.verify_visible_target = verify_fixture_target
def release_after_first_scan():
    end = time.monotonic() + 6
    while time.monotonic() < end:
        if (guard.get(ticket['task']).get('worker_supervision') or {}).get('state') == 'observing':
            release.touch()
            return
        time.sleep(.02)
watcher = threading.Thread(target=release_after_first_scan, daemon=True)
dc.verify_visible_claim = lambda row: None  # Disposable external terminal seam.
watcher.start()
try:
    result = dc.main(['--home', ticket['identity']['home'], 'worker', '--payload-json', json.dumps(ticket)])
    assert result == int(sys.argv[9]) and release.exists()
    observed = guard.get(ticket['task'])
    marker = Path(row['artifact'] + '.spawn')
    assert marker.is_file(), 'held child never execed the sealed fixture argv'
    spawned = json.loads(marker.read_text())
    assert spawned == dict(pid=observed['child_identity']['pid'],
                           pgid=observed['worker_group']['pgid'])
    assert target_checks, 'fresh worker did not revalidate the sealed receipt before claim'
    assert cmux_attempts == [], 'fresh worker attempted real cmux: ' + repr(cmux_attempts)
    print('SYSTEM171_TEST_TERMINAL_ADAPTER ' + json.dumps(target_checks, sort_keys=True), flush=True)
finally:
    release.touch()
    watcher.join(timeout=7)
assert not watcher.is_alive()
'''
    env = dict(PATH=os.environ.get('PATH', ''), PYTHONPATH=str(Path(__file__).resolve().parents[2]),
               PYTHONDONTWRITEBYTECODE='1', HOME=str(home), HERMES_HOME=str(home),
               CODEX_HOME=str(home / 'codex'), HERMES_PLATFORM='telegram')
    wrapper = None
    sent = []
    try:
        request = dict(command=r.task['launcher_command'], background=True, continuation=r.task)
        token = dc.EVENT_CONTEXT.set(dict(identity=r.identity))
        try:
            admitted = dc.terminal_dispatch(request, invoke)
        finally:
            dc.EVENT_CONTEXT.reset(token)
        row = r.guard.get(r.task['task'])
        assert admitted['continuation']['worker_ticket'] == dc.worker_ticket(row)
        assert row['launcher_identity'], 'native launcher identity unavailable'
        assert row['launcher_identity']['pid'] == launcher.pid
        assert row['launcher_identity']['created'] > 0
        ticket = dc.worker_ticket(row)
        assert dc.main(['--home', r.identity['home'], 'route-preflight', '--payload-json',
                        json.dumps(dict(ticket=ticket, route=row['worker_route']))]) == 0
        with monkeypatch.context() as patch:
            patch.setattr(dc, 'cmux_send', lambda *args: sent.append(args))
            response = json.loads(dc.terminal_dispatch(
                dict(command='SYSTEM167_REGISTERED_WORKER', continuation_ticket=ticket),
                lambda _: pytest.fail('launcher must not start a second worker')))
        assert response['sent'] is True
        assert sent == [(row['visible_binding'], dc.visible_worker_command(ticket))]
        wrapper = subprocess.Popen([sys.executable, '-B', '-c', worker_script,
                                    str(r.guard.path), json.dumps(ticket), str(r.clock[0]),
                                    onboarding, skill_dir, dc.DISPATCHER, dc.CODEX_EXECUTABLE,
                                    json.dumps(row['visible_binding'], sort_keys=True), str(exit_code)], env=env,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                                   start_new_session=True)
        stdout, stderr = wrapper.communicate(timeout=16)
        assert wrapper.returncode == 0, (stdout, stderr)
        markers = [line.removeprefix('SYSTEM171_TEST_TERMINAL_ADAPTER ')
                   for line in stdout.splitlines()
                   if line.startswith('SYSTEM171_TEST_TERMINAL_ADAPTER ')]
        assert len(markers) == 1, (stdout, stderr)
        checks = json.loads(markers[0])
        assert checks and all(check == dict(binding=row['visible_binding'], idle=False,
                                            before_claim=True) for check in checks), checks
        assert wrapper.poll() == 0
        row = r.guard.get(r.task['task'])
        assert row['worker_supervision']['state'] == 'exited'
        assert row['worker_result']['exit_code'] == exit_code
        assert row['claim_phase'] == 'claimed'
        assert row['worker_identity']['pid'] == wrapper.pid
        assert row['child_identity']['pid'] > 0 and row['child_exited'] is True
        assert dc.worker_operational_exit(row)
        if output_present:
            assert row['worker_result']['evidence']['sha256'] == dc.digest(row['artifact'])
        else:
            assert row['worker_result']['artifact_status'] == 'missing'
            assert row['worker_result']['evidence'] is None
            assert not Path(row['artifact']).exists()
        return row
    finally:
        launcher_release.touch()
        if wrapper is not None and wrapper.poll() is None:
            # The wrapper and its one child own this private process group.
            import signal
            os.killpg(wrapper.pid, signal.SIGTERM)
            try:
                wrapper.communicate(timeout=3)
            except subprocess.TimeoutExpired:
                os.killpg(wrapper.pid, signal.SIGKILL)
                wrapper.communicate(timeout=3)
        if launcher is not None:
            try:
                assert launcher.wait(timeout=3) == 0
            except subprocess.TimeoutExpired:
                launcher.terminate()
                try:
                    launcher.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    launcher.kill()
                    launcher.wait(timeout=3)
        if session is not None:
            process_registry._reconcile_local_exit(session)
            with process_registry._lock:
                process_registry._running.pop(session.id, None)


def _complete_item_attempt_os(guard, task, artifact, *, output='observed item result'):
    """Finish a disposable Item attempt from a real wrapper that subsequently exits."""
    release = Path(artifact).with_name(Path(artifact).name + '.release')
    release.unlink(missing_ok=True)
    script = '''
import os, subprocess, sys, time
from pathlib import Path
from hermes_cli import diggr_continuation as dc, diggr_item as item
guard = dc.Guard(Path(sys.argv[1]))
task, artifact, release, output = sys.argv[2:]
child_code = ('import sys,time; from pathlib import Path; '
              'release=Path(sys.argv[1]); artifact=Path(sys.argv[2]); '
              'end=time.monotonic()+10; '
              'exec("while not release.exists() and time.monotonic()<end: time.sleep(.02)"); '
              'sys.exit(2) if not release.exists() else artifact.write_text(sys.argv[3])')
started = time.monotonic()
child, handle = item.spawn_item_child(
    [sys.executable, '-c', child_code, release, artifact, output],
    str(guard.path.parent), guard.get(task),
    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
with guard.transaction() as rows:
    row = rows[task]
    row.update(worker_pid=os.getpid(), worker_identity=dc.process_identity(os.getpid()),
               worker_started_ns=time.time_ns(), child_identity=dc.process_identity(child.pid),
               worker_group=handle['binding'], claim_phase='claimed', visible_sent=True)
attempt = guard.get(task)
try:
    code = item.wait_worker(guard, attempt, child, started, handle)
finally:
    item.close_item_group(handle)
guard.record_worker_result(attempt, code)
'''
    env = dict(PATH=os.environ.get('PATH', ''), PYTHONPATH=str(Path(__file__).resolve().parents[2]),
               PYTHONDONTWRITEBYTECODE='1', HOME=str(Path(guard.path).parent),
               HERMES_HOME=str(Path(guard.path).parent))
    wrapper = subprocess.Popen([sys.executable, '-B', '-c', script, str(guard.path),
                                task, str(artifact), str(release), output],
                               env=env, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, text=True)
    try:
        until = time.monotonic() + 6
        while time.monotonic() < until:
            proof = guard.get(task).get('worker_supervision') or {}
            if proof.get('state') == 'observing':
                release.touch()
                break
            if wrapper.poll() is not None:
                break
            time.sleep(.02)
        stdout, stderr = wrapper.communicate(timeout=13)
        assert wrapper.returncode == 0, (stdout, stderr)
        row = guard.get(task)
        assert row['worker_supervision']['state'] == 'exited'
        assert row['worker_result']['exit_code'] == 0
        return row
    finally:
        release.touch()
        if wrapper.poll() is None:
            wrapper.terminate()
            try:
                wrapper.communicate(timeout=3)
            except subprocess.TimeoutExpired:
                wrapper.kill()
                wrapper.communicate(timeout=3)


@pytest.mark.asyncio
@pytest.mark.live_system_guard_bypass
async def test_unobserved_os_exit_retains_unknown_and_books_elapsed_once(route, monkeypatch, tmp_path):
    import psutil

    r = route
    _, payload = await setup(r, monkeypatch)
    item.control(r.guard, r.identity, payload)
    register(r)
    ready = tmp_path / 'unobserved-ready'
    release = tmp_path / 'unobserved-release'
    code = ('import sys,time; from pathlib import Path; '
            'ready,release=map(Path,sys.argv[1:]); ready.touch(); '
            'end=time.monotonic()+8; '
            'exec("while not release.exists() and time.monotonic()<end: time.sleep(.02)")')
    started = time.monotonic()
    leader, handle, attempt = _spawn_bound_os_worker(
        r.guard, r.task['task'], [sys.executable, '-c', code, str(ready), str(release)])
    try:
        until = time.monotonic() + 2
        while not ready.exists() and time.monotonic() < until:
            time.sleep(.02)
        assert ready.exists() and leader.returncode is None
        release.touch()
        until = time.monotonic() + 3
        while time.monotonic() < until:
            if psutil.Process(leader.pid).status() == psutil.STATUS_ZOMBIE:
                break
            time.sleep(.01)
        assert psutil.Process(leader.pid).status() == psutil.STATUS_ZOMBIE
        assert leader.returncode is None
        with pytest.raises(ValueError, match='before first successful scan'):
            item.wait_worker(r.guard, attempt, leader, started, handle)
        row = r.guard.get(attempt['task'])
        proof = row['worker_supervision']
        assert proof['state'] == 'unknown' and proof['accounted_seconds'] > 0
        assert 'before first successful scan' in proof['reason']
        assert not dc.worker_operational_exit(row)
        assert not dc.executor_released(row) and not handle['released']
        with pytest.raises(ValueError, match='operational exit is unknown'):
            r.guard.record_worker_result(attempt, 0)
        with r.guard.transaction(read_only=True) as rows:
            spent = rows.grants['batches'][payload['batch']]['item']['work_seconds']
        assert spent == proof['accounted_seconds']
        with pytest.raises(ValueError, match='already recorded'):
            item.wait_worker(r.guard, attempt, leader, started, handle)
        with r.guard.transaction(read_only=True) as rows:
            assert rows.grants['batches'][payload['batch']]['item']['work_seconds'] == spent
    finally:
        release.touch()
        item.close_item_group(handle)


@pytest.mark.macos_only
@pytest.mark.asyncio
@pytest.mark.live_system_guard_bypass
async def test_early_fork_exit_waits_for_owned_group_and_cleans_helper(route, monkeypatch, tmp_path):
    """Delay the real first PGID lookup; the command may run only afterward."""
    import psutil
    import threading

    r = route
    _, payload = await setup(r, monkeypatch)
    item.control(r.guard, r.identity, payload)
    register(r)
    ready = tmp_path / 'early-helper-pid'
    leader_release = tmp_path / 'early-leader-release'
    helper_code = 'import time; time.sleep(30)'
    leader_code = (
        'import subprocess,sys,time; from pathlib import Path; '
        f'helper=subprocess.Popen([sys.executable,"-B","-c",{helper_code!r}]); '
        f'{_atomic_test_pid_code("Path(sys.argv[1])", "helper.pid")}'
        'release=Path(sys.argv[2]); end=time.monotonic()+8; '
        'exec("while not release.exists() and time.monotonic()<end: time.sleep(.01)")'
    )
    foreign = subprocess.Popen([sys.executable, '-B', '-c', helper_code],
                               start_new_session=True)
    foreign_created = psutil.Process(foreign.pid).create_time()
    first_lookup = threading.Event()
    release_lookup = threading.Event()
    original_getpgid = item.os.getpgid
    original_popen = item.subprocess.Popen
    worker_argv = [sys.executable, '-B', '-c', leader_code,
                   str(ready), str(leader_release)]
    result = {}
    leader = handle = helper = None
    helper_created = None

    def delayed_real_getpgid(pid):
        if not first_lookup.is_set():
            first_lookup.set()
            assert release_lookup.wait(3), 'first PGID observation was not released'
        return original_getpgid(pid)

    def synchronize_raw_spawn(args, *popen_args, **popen_kwargs):
        child = original_popen(args, *popen_args, **popen_kwargs)
        if args == worker_argv:
            # The previous implementation launched the real command directly.
            # Wait for its actual helper publication and zombie exit before
            # returning from Popen; the next getpgid remains a real OS call.
            until = time.monotonic() + 4
            while not ready.exists() and time.monotonic() < until:
                time.sleep(.01)
            assert ready.exists(), 'raw worker did not publish its helper'
            raw_helper = psutil.Process(int(ready.read_text()))
            result['raw_helper_identity'] = (raw_helper.pid, raw_helper.create_time())
            leader_release.touch()
            while psutil.Process(child.pid).status() != psutil.STATUS_ZOMBIE and time.monotonic() < until:
                time.sleep(.01)
            assert psutil.Process(child.pid).status() == psutil.STATUS_ZOMBIE
        return child

    def spawn():
        try:
            result['spawned'] = _spawn_bound_os_worker(
                r.guard, r.task['task'], worker_argv)
        except BaseException as exc:
            result['error'] = exc

    thread = threading.Thread(target=spawn)
    try:
        with monkeypatch.context() as patch:
            patch.setattr(item.os, 'getpgid', delayed_real_getpgid)
            patch.setattr(item.subprocess, 'Popen', synchronize_raw_spawn)
            thread.start()
            assert first_lookup.wait(3), 'spawn never reached its first group lookup'
            assert not ready.exists(), 'command ran before group ownership was established'
            release_lookup.set()
            thread.join(timeout=12)
        assert not thread.is_alive(), result
        if 'error' in result:
            raise result['error']
        leader, handle, attempt = result['spawned']
        until = time.monotonic() + 4
        while not ready.exists() and time.monotonic() < until:
            time.sleep(.01)
        assert ready.exists(), 'early helper was not started'
        helper = psutil.Process(int(ready.read_text()))
        helper_created = helper.create_time()
        assert original_getpgid(helper.pid) == leader.pid == handle['binding']['pgid']
        leader_release.touch()
        until = time.monotonic() + 4
        while psutil.Process(leader.pid).status() != psutil.STATUS_ZOMBIE and time.monotonic() < until:
            time.sleep(.01)
        assert psutil.Process(leader.pid).status() == psutil.STATUS_ZOMBIE
        assert leader.returncode is None
        with pytest.raises(ValueError, match='before first successful scan'):
            item.wait_worker(r.guard, attempt, leader, handle['started'], handle)
        proof = r.guard.get(attempt['task'])['worker_supervision']
        assert proof['state'] == 'unknown' and proof['accounted_seconds'] > 0
        assert not dc.worker_operational_exit(r.guard.get(attempt['task']))
        assert not handle['released']
        with pytest.raises(ValueError, match='operational exit is unknown'):
            r.guard.record_worker_result(attempt, 0)
        with r.guard.transaction(read_only=True) as rows:
            spent = rows.grants['batches'][payload['batch']]['item']['work_seconds']
        item.account_item_unknown(r.guard, attempt, handle)
        with r.guard.transaction(read_only=True) as rows:
            assert rows.grants['batches'][payload['batch']]['item']['work_seconds'] == spent
        assert psutil.Process(foreign.pid).create_time() == foreign_created
        assert psutil.Process(foreign.pid).status() != psutil.STATUS_ZOMBIE
        assert handle['kqueue'].closed
        until = time.monotonic() + 3
        while time.monotonic() < until:
            try:
                if helper.status() == psutil.STATUS_ZOMBIE:
                    break
            except psutil.NoSuchProcess:
                break
            time.sleep(.01)
        else:
            pytest.fail('same-identity inherited helper survived cleanup')
    finally:
        release_lookup.set()
        leader_release.touch()
        if thread.ident is not None:
            thread.join(timeout=12)
        if handle is None and 'spawned' in result:
            leader, handle, _ = result['spawned']
        if handle is not None:
            item.close_item_group(handle)
        if helper is None and 'raw_helper_identity' in result:
            try:
                pid, created = result['raw_helper_identity']
                candidate = psutil.Process(pid)
                if candidate.create_time() == created:
                    helper, helper_created = candidate, created
            except psutil.NoSuchProcess:
                pass
        if helper is not None and helper_created is not None:
            try:
                if helper.create_time() == helper_created and helper.status() != psutil.STATUS_ZOMBIE:
                    helper.terminate()
                    try:
                        helper.wait(timeout=2)
                    except psutil.TimeoutExpired:
                        if helper.create_time() == helper_created:
                            helper.kill()
                            helper.wait(timeout=2)
            except psutil.NoSuchProcess:
                pass
        if leader is not None and leader.returncode is None:
            leader.wait(timeout=3)
        if foreign.poll() is None:
            foreign.terminate()
        foreign.wait(timeout=3)


def test_item_start_failure_before_popen_has_no_work_or_open_kqueue(tmp_path, monkeypatch):
    class TrackedKqueue:
        closed = False
        def close(self):
            self.closed = True

    queue = TrackedKqueue()
    monkeypatch.setattr(item.select, 'kqueue', lambda: queue, raising=False)
    def fail_popen(*args, **kwargs):
        raise FileNotFoundError('no child started')
    monkeypatch.setattr(item.subprocess, 'Popen', fail_popen)
    attempt = dict(task='no-start', generation=1, action_id='a', effect_id='e')
    with pytest.raises(FileNotFoundError, match='no child started'):
        item.spawn_item_child([sys.executable], str(tmp_path), attempt)
    assert queue.closed


@pytest.mark.macos_only
def test_immediate_item_exit_without_helper_does_not_release_unknown_result(tmp_path):
    attempt = dict(task='immediate-exit', generation=1, action_id='a', effect_id='e')
    leader, handle = item.spawn_item_child(
        [sys.executable, '-B', '-c', 'pass'], str(tmp_path), attempt)
    try:
        leader.wait(timeout=3)
        assert not handle['released']
        assert item._group_state(handle['binding']['pgid']) == 'absent'
    finally:
        item.close_item_group(handle)
    if handle['kqueue'] is not None:
        assert handle['kqueue'].closed


@pytest.mark.asyncio
@pytest.mark.live_system_guard_bypass
async def test_started_item_outer_error_accounts_once_without_supervision(
        route, monkeypatch, tmp_path):
    r = route
    _, payload = await setup(r, monkeypatch)
    item.control(r.guard, r.identity, payload)
    register(r)
    ready = tmp_path / 'outer-error-ready'
    code = ('import sys,time; from pathlib import Path; '
            'Path(sys.argv[1]).touch(); time.sleep(8)')
    leader, handle, attempt = _spawn_bound_os_worker(
        r.guard, r.task['task'], [sys.executable, '-B', '-c', code, str(ready)])
    try:
        until = time.monotonic() + 3
        while not ready.exists() and time.monotonic() < until:
            time.sleep(.01)
        assert ready.exists()
        item.close_item_group(handle)
        item.account_item_unknown(r.guard, attempt, handle)
        row = r.guard.get(attempt['task'])
        proof = row['worker_supervision']
        assert proof['state'] == 'unknown' and proof['accounted_seconds'] > 0
        assert not dc.worker_operational_exit(row) and not handle['released']
        with r.guard.transaction(read_only=True) as rows:
            spent = rows.grants['batches'][payload['batch']]['item']['work_seconds']
        assert spent == proof['accounted_seconds']
        item.account_item_unknown(r.guard, attempt, handle)
        with r.guard.transaction(read_only=True) as rows:
            assert rows.grants['batches'][payload['batch']]['item']['work_seconds'] == spent
        with pytest.raises(ValueError, match='operational exit is unknown'):
            r.guard.record_worker_result(attempt, 0)
        if handle['kqueue'] is not None:
            assert handle['kqueue'].closed
    finally:
        item.close_item_group(handle)
        if leader.returncode is None:
            leader.wait(timeout=3)


@pytest.mark.asyncio
@pytest.mark.live_system_guard_bypass
async def test_observed_normal_os_worker_reaches_native_validation(route, monkeypatch, tmp_path):
    import threading

    r = route
    _, payload = await setup(r, monkeypatch)
    item.control(r.guard, r.identity, payload)
    register(r)
    ready = tmp_path / 'normal-ready'
    release = tmp_path / 'normal-release'
    artifact = r.guard.get(r.task['task'])['artifact']
    code = (
        'import sys,time; from pathlib import Path; '
        'ready,release,artifact=map(Path,sys.argv[1:]); ready.touch(); '
        'end=time.monotonic()+5; '
        'exec("while not release.exists() and time.monotonic()<end: time.sleep(.02)"); '
        'artifact.write_text("real worker result")'
    )
    started = time.monotonic()
    leader, handle, attempt = _spawn_bound_os_worker(
        r.guard, r.task['task'], [sys.executable, '-c', code, str(ready), str(release), artifact])
    result = {}
    thread = None
    try:
        until = time.monotonic() + 2
        while not ready.exists() and time.monotonic() < until:
            time.sleep(.02)
        assert ready.exists()
        def supervise():
            try:
                result['code'] = item.wait_worker(r.guard, attempt, leader, started, handle)
            except BaseException as exc:
                result['error'] = exc
        thread = threading.Thread(target=supervise, daemon=True)
        thread.start()
        until = time.monotonic() + 2
        while r.guard.get(attempt['task']).get('worker_supervision', {}).get('state') != 'observing' and time.monotonic() < until:
            if 'error' in result:
                raise result['error']
            time.sleep(.02)
        assert r.guard.get(attempt['task'])['worker_supervision']['state'] == 'observing'
        release.touch()
        thread.join(timeout=5)
        assert not thread.is_alive() and result.get('code') == 0 and not result.get('error')
        native = r.guard.record_worker_result(attempt, result['code'])
        assert native['artifact_status'] == 'present'
        assert r.guard.observe(attempt['task'], attempt['generation'], 'completed', native['evidence'])
        row = r.guard.get(attempt['task'])
        assert row['gate'] == 'main_validation' and row['status'] == 'pending'
        assert row['worker_supervision']['state'] == 'exited'
        assert row['worker_supervision']['accounted_seconds'] > 0
        with r.guard.transaction(read_only=True) as rows:
            spent = rows.grants['batches'][payload['batch']]['item']['work_seconds']
        assert spent == row['worker_supervision']['accounted_seconds']
        assert r.guard.record_worker_result(attempt, result['code']) == native
        with r.guard.transaction(read_only=True) as rows:
            assert rows.grants['batches'][payload['batch']]['item']['work_seconds'] == spent
    finally:
        release.touch()
        if leader.poll() is None:
            leader.wait(timeout=6)
        if thread is not None:
            thread.join(timeout=6)
        item.close_item_group(handle)


@pytest.mark.asyncio
@pytest.mark.live_system_guard_bypass
async def test_shared_inherited_group_is_not_adopted_or_signalled(route, monkeypatch):
    r = route
    _, payload = await setup(r, monkeypatch)
    item.control(r.guard, r.identity, payload)
    register(r)
    leader = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(2)'])
    foreign = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(2)'])
    try:
        assert os.getpgid(leader.pid) == os.getpgrp() == os.getpgid(foreign.pid)
        with pytest.raises(ValueError, match='group ownership unavailable'):
            item.wait_worker(r.guard, r.guard.get(r.task['task']), leader, time.monotonic())
        assert leader.poll() is None and foreign.poll() is None
        assert not r.guard.get(r.task['task']).get('worker_supervision')
    finally:
        for process in (leader, foreign):
            if process.poll() is None:
                process.terminate()
            process.wait(timeout=3)


@pytest.mark.asyncio
@pytest.mark.live_system_guard_bypass
@pytest.mark.parametrize('outcome', ['completion', 'revocation'])
async def test_late_inherited_helper_after_first_empty_os_scan_is_characterized(
        route, monkeypatch, tmp_path, outcome):
    """A real late fork must block completion and obey a subsequent native stop."""
    import threading
    import psutil

    r = route
    _, payload = await setup(r, monkeypatch)
    item.control(r.guard, r.identity, payload)
    register(r)
    ready = tmp_path / 'late-ready'
    release = tmp_path / 'late-release'
    helper_ready = tmp_path / 'late-helper-ready'
    helper_release = tmp_path / 'late-helper-release'
    foreign_release = tmp_path / 'late-foreign-release'
    artifact = r.guard.get(r.task['task'])['artifact']
    helper_code = (
        'import os,sys,time; from pathlib import Path; '
        'ready,release=map(Path,sys.argv[1:]); '
        f'{_atomic_test_pid_code("ready", "os.getpid()")}'
        'end=time.monotonic()+30; '
        'exec("while not release.exists() and time.monotonic()<end: time.sleep(.01)")'
    )
    leader_code = (
        'import subprocess,sys,time; from pathlib import Path; '
        'ready,release,helper_ready,helper_release,artifact=map(Path,sys.argv[1:]); '
        'ready.touch(); end=time.monotonic()+6; '
        'exec("while not release.exists() and time.monotonic()<end: time.sleep(.01)"); '
        f'subprocess.Popen([sys.executable,"-c",{helper_code!r},str(helper_ready),str(helper_release)]); '
        'end=time.monotonic()+2; '
        'exec("while not helper_ready.exists() and time.monotonic()<end: time.sleep(.01)"); '
        'artifact.write_text("real late helper result"); sys.exit(0)'
    )
    leader, handle, attempt = _spawn_bound_os_worker(
        r.guard, r.task['task'], [sys.executable, '-B', '-c', leader_code, str(ready),
        str(release), str(helper_ready), str(helper_release), artifact])
    foreign = subprocess.Popen([sys.executable, '-B', '-c',
        'import sys,time; from pathlib import Path; p=Path(sys.argv[1]); end=time.monotonic()+30; '
        'exec("while not p.exists() and time.monotonic()<end: time.sleep(.01)")',
        str(foreign_release)])
    thread = None
    helper_identity = None
    events = []
    result = {}
    started = time.monotonic()
    try:
        deadline = time.monotonic() + 2
        while not ready.exists() and time.monotonic() < deadline:
            time.sleep(.01)
        assert ready.exists()
        foreign_created = psutil.Process(foreign.pid).create_time()
        assert os.getpgid(leader.pid) == leader.pid != os.getpgrp()
        assert os.getpgid(foreign.pid) != leader.pid
        original_leader_exited = item._leader_exited
        original_children = psutil.Process.children
        supervisor_id = [None]
        first_empty = threading.Event()

        def observed_children(process, *args, **kwargs):
            children = original_children(process, *args, **kwargs)
            if (threading.get_ident() == supervisor_id[0] and process.pid == leader.pid
                    and not children and not first_empty.is_set()):
                events.append(('first_empty_scan', time.monotonic()))
                first_empty.set()
            return children

        def synchronized_real_exit(group_handle):
            nonlocal helper_identity
            if (threading.get_ident() == supervisor_id[0] and first_empty.is_set()
                    and not any(name == 'leader_exited' for name, _ in events)):
                deadline = time.monotonic() + 3
                while not helper_ready.exists() and time.monotonic() < deadline:
                    time.sleep(.005)
                assert helper_ready.exists(), 'late helper did not spawn after first empty scan'
                pid = int(helper_ready.read_text())
                helper_identity = (pid, psutil.Process(pid).create_time())
                assert os.getpgid(pid) == leader.pid
                events.append(('helper_spawned', time.monotonic()))
                while time.monotonic() < deadline:
                    if original_leader_exited(group_handle):
                        events.append(('leader_exited', time.monotonic()))
                        return True
                    time.sleep(.005)
                pytest.fail('real non-reaping leader exit was not observed')
            if any(name == 'leader_exited' for name, _ in events):
                return True  # Latch the consumed real kqueue event.
            return original_leader_exited(group_handle)

        # Synchronize the observation boundary; the kqueue/waitid answer is real.
        monkeypatch.setattr(psutil.Process, 'children', observed_children)
        monkeypatch.setattr(item, '_leader_exited', synchronized_real_exit)
        def supervise():
            supervisor_id[0] = threading.get_ident()
            try:
                result['code'] = item.wait_worker(r.guard, attempt, leader, started, handle)
            except BaseException as exc:
                result['error'] = exc
        thread = threading.Thread(target=supervise, daemon=True)
        thread.start()
        assert first_empty.wait(3), f'first empty scan absent; thread={thread.is_alive()}, error={result.get("error")!r}'
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            if result.get('error'):
                pytest.fail(f'supervisor failed after first scan: {result["error"]!r}; events={events!r}')
            if r.guard.get(attempt['task']).get('worker_supervision', {}).get('state') == 'observing':
                break
            time.sleep(.01)
        else:
            pytest.fail(f'first scan not committed; events={events!r}; thread={thread.is_alive()}, result={result!r}')
        release.touch()
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if result.get('error'):
                pytest.fail(f'supervisor failed at late helper boundary: {result["error"]!r}; events={events!r}')
            if any(name == 'leader_exited' for name, _ in events):
                break
            time.sleep(.01)
        else:
            pytest.fail(f'late helper handshake timed out; events={events!r}; '
                        f'helper_ready={helper_ready.exists()}, thread={thread.is_alive()}, result={result!r}')
        assert [name for name, _ in events] == ['first_empty_scan', 'helper_spawned', 'leader_exited']
        helper_pid = int(helper_ready.read_text())
        helper_process = psutil.Process(helper_pid)
        assert helper_identity == (helper_pid, helper_process.create_time())
        helper_live = helper_process.status() != psutil.STATUS_ZOMBIE
        row = r.guard.get(attempt['task'])
        observed = row.get('worker_descendants', [])
        captured = any(entry['pid'] == helper_pid and entry['created'] == helper_identity[1]
                       for entry in observed)
        events.append(('native_observation', time.monotonic()))
        print('LATE_CHILD_OS_EVIDENCE ' + json.dumps(dict(events=events,
            leader_pid=leader.pid, leader_exit=leader.returncode,
            helper_pid=helper_pid, helper_created=helper_identity[1], helper_live=helper_live,
            foreign_pid=foreign.pid, foreign_created=foreign_created,
            foreign_live=foreign.poll() is None,
            observed=observed, supervision=row['worker_supervision'],
            wait_code=result.get('code'), wait_error=str(result.get('error'))), sort_keys=True))
        assert not captured, 'probe did not exercise the uncaptured late helper'
        assert psutil.Process(leader.pid).status() == psutil.STATUS_ZOMBIE
        assert not handle['released']
        if helper_live:
            assert helper_live and thread is not None
            assert row['worker_supervision']['state'] != 'exited'
            assert not dc.worker_operational_exit(row)
            with pytest.raises(ValueError, match='operational exit') as rejected:
                r.guard.record_worker_result(attempt, 0)
            assert psutil.Process(helper_pid).create_time() == helper_identity[1]
            assert psutil.Process(helper_pid).status() != psutil.STATUS_ZOMBIE
            events.append(('rejected_helper_alive', time.monotonic()))
            rejected_row = r.guard.get(attempt['task'])
            assert rejected_row['gate'] != 'main_validation'
            assert not rejected_row.get('worker_result')
            assert not dc.executor_released(rejected_row)
            print('LATE_CHILD_NATIVE_CONSUMER ' + json.dumps(dict(
                accepted=False, reason=str(rejected.value)), sort_keys=True))
            if outcome == 'revocation':
                # The same initially uncaptured helper is still alive when the
                # native owner batch becomes revoked. Do not release its barrier.
                with r.guard.transaction() as rows:
                    rows.grants['batches'][payload['batch']]['revoked'] = True
                with r.guard.transaction(read_only=True) as rows:
                    assert rows[attempt['task']]['policy']['revoked'] is True
                assert psutil.Process(helper_pid).create_time() == helper_identity[1]
                assert psutil.Process(helper_pid).status() != psutil.STATUS_ZOMBIE
                events.append(('native_revocation_observed', time.monotonic()))
                thread.join(timeout=7)
                assert not thread.is_alive(), f'stop did not finish: {result!r}'
                assert isinstance(result.get('error'), ValueError)
                assert 'stopped by budget or revocation' in str(result['error'])
                try:
                    same_helper = psutil.Process(helper_pid)
                    assert same_helper.create_time() == helper_identity[1]
                    helper_terminal = same_helper.status() == psutil.STATUS_ZOMBIE
                except psutil.NoSuchProcess:
                    helper_terminal = True
                assert helper_terminal, 'revoked Item left its uncaptured inherited helper running'
                row = r.guard.get(attempt['task'])
                proof = row['worker_supervision']
                assert proof['state'] == 'unknown' and proof['accounted_seconds'] > 0
                assert not row.get('worker_result') and row['gate'] != 'main_validation'
                assert not dc.executor_released(row)
                assert foreign.poll() is None
                assert psutil.Process(foreign.pid).create_time() == foreign_created
                with r.guard.transaction(read_only=True) as rows:
                    spent = rows.grants['batches'][payload['batch']]['item']['work_seconds']
                assert spent == proof['accounted_seconds']
                with pytest.raises(ValueError, match='already recorded'):
                    item.wait_worker(r.guard, attempt, leader, started, handle)
                with r.guard.transaction(read_only=True) as rows:
                    assert rows.grants['batches'][payload['batch']]['item']['work_seconds'] == spent
                assert handle['closed'] and not handle['handed_off']
                print('LATE_CHILD_REVOKED ' + json.dumps(dict(events=events,
                    helper_pid=helper_pid, helper_terminal=helper_terminal,
                    foreign_pid=foreign.pid, spent=spent), sort_keys=True))
            else:
                helper_release.touch()
                events.append(('helper_released', time.monotonic()))
                thread.join(timeout=5)
                assert not thread.is_alive() and result.get('code') == 0
                events.append(('supervision_succeeded', time.monotonic()))
                native = r.guard.record_worker_result(attempt, result['code'])
                assert native['artifact_status'] == 'present'
                assert native['evidence']['sha256'] == dc.digest(artifact)
                assert r.guard.observe(attempt['task'], attempt['generation'], 'completed', native['evidence'])
                assert r.guard.get(attempt['task'])['gate'] == 'main_validation'
                events.append(('native_completed', time.monotonic()))
                row = r.guard.get(attempt['task'])
                assert row['worker_supervision']['owned_group_empty'] is True
                assert row['worker_supervision']['accounted_seconds'] >= events[-2][1] - started - .1
                spent = row['worker_supervision']['accounted_seconds']
                assert r.guard.record_worker_result(attempt, result['code']) == native
                with r.guard.transaction(read_only=True) as rows:
                    assert rows.grants['batches'][payload['batch']]['item']['work_seconds'] == spent
                print('LATE_CHILD_FINAL ' + json.dumps(dict(events=events, spent=spent,
                    helper_pid=helper_pid, foreign_pid=foreign.pid), sort_keys=True))
        else:
            pytest.fail('INCONCLUSIVE: helper capture or lifetime was not observed')
        assert foreign.poll() is None
        assert psutil.Process(foreign.pid).create_time() == foreign_created
        assert all(entry['pid'] != foreign.pid for entry in observed)
    finally:
        release.touch()
        helper_release.touch()
        foreign_release.touch()
        if thread is not None:
            thread.join(timeout=7)
            if thread.is_alive():
                item.close_item_group(handle)
                thread.join(timeout=3)
            assert not thread.is_alive(), 'supervisor did not end after barriers released'
        if leader.poll() is None:
            try:
                leader.wait(timeout=7)
            except subprocess.TimeoutExpired:
                leader.terminate()
                leader.wait(timeout=3)
        item.close_item_group(handle)
        if helper_identity is not None:
            try:
                process = psutil.Process(helper_identity[0])
                if process.create_time() == helper_identity[1] and process.status() != psutil.STATUS_ZOMBIE:
                    try:
                        process.wait(timeout=2)
                    except psutil.TimeoutExpired:
                        if process.create_time() == helper_identity[1]:
                            process.terminate()
                            try:
                                process.wait(timeout=2)
                            except psutil.TimeoutExpired:
                                if process.create_time() == helper_identity[1]:
                                    process.kill()
                                    process.wait(timeout=2)
            except psutil.NoSuchProcess:
                pass
        assert foreign.wait(timeout=10) == 0


@pytest.mark.live_system_guard_bypass
def test_owned_group_cleanup_keeps_exited_leader_identity_with_empty_capture(tmp_path, monkeypatch):
    """An unreaped leader pins its isolated group after getpgid ESRCH, without capture."""
    import psutil
    import signal

    ready = tmp_path / 'helper-ready'
    release = tmp_path / 'leader-release'
    helper_code = 'import time; time.sleep(8)'
    leader_code = (
        'import subprocess,sys,time; from pathlib import Path; '
        'ready,release=map(Path,sys.argv[1:]); '
        f'helper=subprocess.Popen([sys.executable,"-B","-c",{helper_code!r}]); '
        f'{_atomic_test_pid_code("ready", "helper.pid")}'
        'end=time.monotonic()+4; '
        'exec("while not release.exists() and time.monotonic()<end: time.sleep(.01)")'
    )
    attempt = dict(task='disposable-group', generation=1,
                   action_id='disposable-action', effect_id='disposable-effect')
    foreign = subprocess.Popen([sys.executable, '-B', '-c', helper_code],
                               start_new_session=True)
    foreign_created = psutil.Process(foreign.pid).create_time()
    leader = handle = helper = None
    helper_created = None
    evidence = []

    def observe(stage):
        row = dict(stage=stage, at=time.monotonic())
        for name, process in (('leader', leader), ('helper', helper)):
            if process is None:
                continue
            row[name + '_pid'] = process.pid
            try:
                observed = psutil.Process(process.pid)
                row[name + '_created'] = observed.create_time()
                row[name + '_status'] = observed.status()
                row[name + '_ppid'] = observed.ppid()
                try:
                    row[name + '_pgid'] = os.getpgid(process.pid)
                except OSError as exc:
                    row[name + '_pgid_error'] = repr(exc)
            except psutil.NoSuchProcess:
                row[name + '_status'] = 'absent'
            except (psutil.Error, OSError) as exc:
                row[name + '_observation_error'] = repr(exc)
        evidence.append(row)

    original_killpg = item.os.killpg
    def recorded_killpg(pgid, signum):
        if signum:
            observe('signal_attempt')
            evidence[-1].update(target_pgid=pgid, signal=signum)
        try:
            result = original_killpg(pgid, signum)
        except BaseException as exc:
            if signum:
                evidence.append(dict(stage='signal_error', at=time.monotonic(),
                                     target_pgid=pgid, signal=signum, error=repr(exc)))
            raise
        if signum:
            evidence.append(dict(stage='signal_return', at=time.monotonic(),
                                 target_pgid=pgid, signal=signum, result='returned'))
        return result

    try:
        leader, handle = item.spawn_item_child(
            [sys.executable, '-B', '-c', leader_code, str(ready), str(release)],
            str(tmp_path), attempt)
        deadline = time.monotonic() + 3
        while not ready.exists() and time.monotonic() < deadline:
            time.sleep(.01)
        assert ready.exists(), 'disposable helper did not start'
        helper = psutil.Process(int(ready.read_text()))
        helper_created = helper.create_time()
        assert os.getpgid(helper.pid) == handle['binding']['pgid'] == leader.pid
        assert os.getpgid(foreign.pid) != leader.pid
        observe('group_member_before_exit')
        release.touch()
        while psutil.Process(leader.pid).status() != psutil.STATUS_ZOMBIE and time.monotonic() < deadline:
            time.sleep(.01)
        assert psutil.Process(leader.pid).status() == psutil.STATUS_ZOMBIE
        observe('leader_exited')
        assert helper.create_time() == helper_created
        assert helper.status() != psutil.STATUS_ZOMBIE
        assert item._group_state(leader.pid) == 'present', evidence
        assert item._owned_group_pinned(handle), evidence
        handle['binding']['leader_created'] += 1
        try:
            assert not item._owned_group_pinned(handle), evidence
            with pytest.raises(ValueError, match='identity no longer pinned'):
                item._signal_owned_group(handle, signal.SIGTERM)
        finally:
            handle['binding']['leader_created'] -= 1
        handle['binding']['pgid'] += 1
        try:
            assert not item._owned_group_pinned(handle), evidence
            with pytest.raises(ValueError, match='identity no longer pinned'):
                item._signal_owned_group(handle, signal.SIGTERM)
        finally:
            handle['binding']['pgid'] -= 1
        with monkeypatch.context() as recording:
            recording.setattr(item.os, 'killpg', recorded_killpg)
            observe('cleanup_entry')
            item.close_item_group(handle)
        observe('cleanup_complete')
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            try:
                assert helper.create_time() == helper_created, evidence
                if helper.status() == psutil.STATUS_ZOMBIE:
                    break
            except psutil.NoSuchProcess:
                break
            time.sleep(.01)
        else:
            raise AssertionError('same-identity helper survived: ' + repr(evidence))
        observe('final_check')
        assert any(row.get('stage') == 'signal_return' and
                   row.get('target_pgid') == leader.pid and
                   row.get('signal') == signal.SIGTERM for row in evidence), evidence
        assert psutil.Process(foreign.pid).create_time() == foreign_created
        assert psutil.Process(foreign.pid).status() != psutil.STATUS_ZOMBIE
        assert not item._owned_group_pinned(handle), evidence
        print('OWNED_GROUP_CLEANUP_EVIDENCE ' + json.dumps(evidence, sort_keys=True))
    finally:
        (tmp_path / 'cleanup-evidence.json').write_text(json.dumps(evidence, sort_keys=True))
        release.touch()
        if handle is not None:
            item.close_item_group(handle)
        if helper is not None and helper_created is not None:
            try:
                if helper.create_time() == helper_created and helper.status() != psutil.STATUS_ZOMBIE:
                    helper.terminate()
            except psutil.NoSuchProcess:
                pass
        if leader is not None and leader.returncode is None:
            leader.wait(timeout=3)
        if foreign.poll() is None and psutil.Process(foreign.pid).create_time() == foreign_created:
            foreign.terminate()
        foreign.wait(timeout=3)


@pytest.mark.asyncio
@pytest.mark.live_system_guard_bypass
async def test_held_item_exec_preserves_owned_leader_identity(route, monkeypatch, tmp_path):
    """The supervisor accepts a real image replacement of its pinned child."""
    import psutil
    import select
    from datetime import datetime, timezone

    r = route
    _, payload = await setup(r, monkeypatch)
    item.control(r.guard, r.identity, payload)
    register(r)
    task = r.task['task']
    ready_read, ready_write = os.pipe()
    release_read, release_write = os.pipe()
    script = tmp_path / 'exec-image.sh'
    script.write_text(f'#!/bin/sh\nprintf R >&{ready_write}\nread line <&{release_read}\n')
    script.chmod(0o700)
    created_child = []
    captured = []
    real_popen = subprocess.Popen
    real_write = os.write

    def os_identity(pid):
        process = psutil.Process(pid)
        created = process.create_time()
        return dict(pid=pid, ppid=process.ppid(), pgid=os.getpgid(pid),
                    tpgid=-1, tty=process.terminal() or '??',
                    start=datetime.fromtimestamp(created, timezone.utc).strftime('%a %b %d %H:%M:%S %Y'),
                    executable=process.exe())

    def capture_popen(*args, **kwargs):
        child = real_popen(*args, **kwargs)
        created_child.append(child)
        return child

    def capture_release(fd, data):
        if data == b'\x01' and created_child and not captured:
            captured.append(os_identity(created_child[0].pid))
        return real_write(fd, data)

    child = handle = None
    try:
        with monkeypatch.context() as seam:
            seam.setattr(item.subprocess, 'Popen', capture_popen)
            seam.setattr(item.os, 'write', capture_release)
            child, handle = item.spawn_item_child([str(script)], str(tmp_path), r.guard.get(task),
                                                   pass_fds=(ready_write, release_read))
        os.close(ready_write)
        ready_write = -1
        os.close(release_read)
        release_read = -1
        assert select.select([ready_read], [], [], 3)[0], 'post-exec readiness timed out'
        assert os.read(ready_read, 1) == b'R', 'post-exec readiness missing'
        assert len(captured) == 1
        before = captured[0]
        after = os_identity(child.pid)
        assert before['pid'] == after['pid'] == handle['binding']['pgid']
        assert before['start'] == after['start']
        assert before['pgid'] == after['pgid']
        assert before['executable'] != after['executable']
        assert psutil.Process(child.pid).create_time() == handle['binding']['leader_created']
        (tmp_path / 'exec-identity.json').write_text(json.dumps(dict(
            before=before, after=after, leader_created=handle['binding']['leader_created'])))
        monkeypatch.setattr(dc, 'process_identity', os_identity)
        attempt = _bind_started_os_worker(r.guard, task, child, handle)
        with r.guard.transaction() as rows:
            rows[task]['child_identity'] = before
        attempt = r.guard.get(task)
        original_children = psutil.Process.children
        released = []

        def release_after_accepted_scan(process, *args, **kwargs):
            if process.pid == child.pid:
                # This disposable shell has no descendants. The sandbox denies
                # psutil's global PID enumeration, so keep only this scan seam.
                if not released:
                    released.append(real_write(release_write, b'\n'))
                return []
            return original_children(process, *args, **kwargs)

        with monkeypatch.context() as seam:
            seam.setattr(psutil.Process, 'children', release_after_accepted_scan)
            assert item.wait_worker(r.guard, attempt, child, handle['started'], handle) == 0
        assert released == [1]
        assert r.guard.get(task)['worker_supervision']['state'] == 'exited'
    finally:
        for fd in (ready_read, ready_write, release_read, release_write):
            if fd >= 0:
                try:
                    os.close(fd)
                except OSError:
                    pass
        if handle is not None:
            item.close_item_group(handle)
        if child is not None and child.returncode is None:
            child.wait(timeout=3)


@pytest.mark.asyncio
@pytest.mark.live_system_guard_bypass
@pytest.mark.parametrize('invalid', ['foreign-child', 'changed-start', 'wrong-group', 'missing'])
async def test_item_owned_leader_rejects_foreign_or_incomplete_identity(
        route, monkeypatch, tmp_path, invalid):
    """Neither a different child nor altered/incomplete ownership can supervise."""
    import psutil
    import select
    from datetime import datetime, timezone

    r = route
    _, payload = await setup(r, monkeypatch)
    item.control(r.guard, r.identity, payload)
    register(r)
    task = r.task['task']
    ready_read, ready_write = os.pipe()
    release_read, release_write = os.pipe()
    script = tmp_path / 'owned-image.sh'
    script.write_text(f'#!/bin/sh\nprintf R >&{ready_write}\nread line <&{release_read}\n')
    script.chmod(0o700)

    def os_identity(pid):
        process = psutil.Process(pid)
        return dict(pid=pid, ppid=process.ppid(), pgid=os.getpgid(pid),
                    tpgid=-1, tty=process.terminal() or '??',
                    start=datetime.fromtimestamp(process.create_time(), timezone.utc).strftime(
                        '%a %b %d %H:%M:%S %Y'), executable=process.exe())

    child = handle = foreign = None
    try:
        child, handle = item.spawn_item_child([str(script)], str(tmp_path), r.guard.get(task),
                                               pass_fds=(ready_write, release_read))
        os.close(ready_write)
        ready_write = -1
        os.close(release_read)
        release_read = -1
        assert select.select([ready_read], [], [], 3)[0], 'post-exec readiness timed out'
        assert os.read(ready_read, 1) == b'R'
        foreign = subprocess.Popen([sys.executable, '-B', '-c',
                                    'import time; time.sleep(10)'], start_new_session=True)
        foreign_created = psutil.Process(foreign.pid).create_time()
        foreign_pgid = os.getpgid(foreign.pid)
        assert foreign_pgid != handle['binding']['pgid']
        monkeypatch.setattr(dc, 'process_identity', os_identity)
        attempt = _bind_started_os_worker(r.guard, task, child, handle)
        if invalid != 'foreign-child':
            with r.guard.transaction() as rows:
                identity = rows[task]['child_identity']
                if invalid == 'changed-start':
                    identity['start'] = 'Thu Jan 01 00:00:00 1970'
                elif invalid == 'wrong-group':
                    identity['pgid'] = os.getpgid(foreign.pid)
                else:
                    rows[task]['child_identity'] = {'pid': child.pid}
            attempt = r.guard.get(task)
        expected = ('not bound' if invalid == 'foreign-child' else
                    'worker leader identity changed')
        signals = []
        real_killpg = os.killpg

        def observe_signal(pgid, signum):
            if signum:
                signals.append((pgid, signum))
            return real_killpg(pgid, signum)

        with monkeypatch.context() as seam:
            seam.setattr(item.os, 'killpg', observe_signal)
            with pytest.raises(ValueError, match=expected):
                item.wait_worker(r.guard, attempt,
                                 foreign if invalid == 'foreign-child' else child,
                                 handle['started'], handle)
        assert all(pgid == handle['binding']['pgid'] for pgid, _ in signals)
        assert all(pgid != foreign_pgid for pgid, _ in signals)
        assert psutil.Process(foreign.pid).create_time() == foreign_created
        assert foreign.poll() is None
        proof = r.guard.get(task).get('worker_supervision')
        assert proof is None if invalid == 'foreign-child' else proof['state'] == 'unknown'
    finally:
        for fd in (ready_read, ready_write, release_read, release_write):
            if fd >= 0:
                try:
                    os.close(fd)
                except OSError:
                    pass
        if handle is not None:
            item.close_item_group(handle)
        if child is not None and child.returncode is None:
            child.wait(timeout=3)
        if foreign is not None:
            if foreign.poll() is None:
                foreign.terminate()
            foreign.wait(timeout=3)


@pytest.mark.asyncio
@pytest.mark.live_system_guard_bypass
@pytest.mark.parametrize('outcome', ['success', 'stop', 'exception'])
async def test_root_pty_item_group_lifecycle(route, monkeypatch, tmp_path, outcome):
    """Exercise the bound Item child through a real controlling root PTY."""
    import errno
    import pty
    import select
    import signal
    import threading
    import psutil

    r = route
    _, payload = await setup(r, monkeypatch)
    item.control(r.guard, r.identity, payload)
    register(r)
    task = r.task['task']
    artifact = r.guard.get(task)['artifact']
    helper_ready = tmp_path / 'pty-helper-ready'
    helper_release = tmp_path / 'pty-helper-release'
    leader_release = tmp_path / 'pty-leader-release'
    report = tmp_path / 'pty-report.json'
    controller_report = tmp_path / 'pty-controller-report.json'
    child_consumed = tmp_path / 'pty-child-consumed'
    wrapper_identity = tmp_path / 'pty-wrapper-identity.json'
    token = 'item-pty-' + outcome
    foreign = subprocess.Popen([sys.executable, '-B', '-c',
        'import time; time.sleep(12)'], start_new_session=True)
    foreign_created = psutil.Process(foreign.pid).create_time()
    shell_pid = None
    master = None
    output = bytearray()
    stages = []

    def stamp(name, **details):
        stages.append(dict(stage=name, at=time.monotonic(), **details))

    def wrapper():
        own = os.getpgrp()
        wrapper_identity.write_text(json.dumps(dict(pid=os.getpid(),
            created=psutil.Process(os.getpid()).create_time())))
        assert os.tcgetpgrp(0) == own
        assert len({os.ttyname(fd) for fd in (0, 1, 2)}) == 1
        stamp('wrapper_foreground', pid=os.getpid(), pgid=own,
              foreground=os.tcgetpgrp(0), tty=os.ttyname(0))
        if outcome == 'stop':
            def process_snapshot(pid):
                if pid is None:
                    return dict(observation='pid unavailable')
                try:
                    process = psutil.Process(pid)
                    snapshot = dict(pid=pid, created=process.create_time(),
                                    status=process.status(), ppid=process.ppid())
                    try:
                        snapshot['pgid'] = os.getpgid(pid)
                    except OSError as exc:
                        snapshot['pgid_error'] = repr(exc)
                    return snapshot
                except psutil.NoSuchProcess:
                    return dict(pid=pid, observation='absent')
                except (psutil.Error, OSError) as exc:
                    return dict(pid=pid, observation_error=repr(exc))

            original_close = item.close_item_group
            original_signal = item._signal_owned_group
            def observed_close(group):
                record = not group.get('closed')
                if record:
                    leader_pid = group['child'].pid
                    helper_pid = int(helper_ready.read_text()) if helper_ready.exists() else None
                    stamp('cleanup_entry', leader=process_snapshot(leader_pid),
                          helper=process_snapshot(helper_pid),
                          binding=group['binding'], released=group['released'],
                          pinned=item._owned_group_pinned(group),
                          group_state=item._group_state(group['binding']['pgid']))
                try:
                    return original_close(group)
                finally:
                    if record:
                        stamp('cleanup_completion', leader=process_snapshot(group['child'].pid),
                              helper=process_snapshot(
                                  int(helper_ready.read_text()) if helper_ready.exists() else None),
                              closed=group.get('closed', False))

            def observed_signal(group, signum):
                pgid = group['binding']['pgid']
                stamp('signal_attempt', pgid=pgid, signal=signum,
                      leader=process_snapshot(group['child'].pid),
                      helper=process_snapshot(
                          int(helper_ready.read_text()) if helper_ready.exists() else None))
                try:
                    result = original_signal(group, signum)
                except BaseException as exc:
                    stamp('signal_error', pgid=pgid, signal=signum, error=repr(exc))
                    raise
                stamp('signal_return', pgid=pgid, signal=signum, result='returned')
                return result

            item.close_item_group = observed_close
            item._signal_owned_group = observed_signal
        helper_code = (
            'import os,sys,time; from pathlib import Path; '
            'ready,release=map(Path,sys.argv[1:]); '
            f'{_atomic_test_pid_code("ready", "os.getpid()")}'
            'end=time.monotonic()+8; '
            'exec("while not release.exists() and time.monotonic()<end: time.sleep(.01)")'
        )
        if outcome == 'exception':
            child_code = (
                'import sys,time; from pathlib import Path; '
                'print("C17_CHILD_READY",flush=True); '
                'line=sys.stdin.readline().strip(); '
                'print("C17_CHILD_REPLY "+line,flush=True); '
                'Path(sys.argv[5]).touch(); '
                'time.sleep(8)'
            )
        else:
            child_code = (
                'import os,subprocess,sys,time; from pathlib import Path; '
                'print("C17_CHILD_READY",flush=True); '
                'line=sys.stdin.readline().strip(); '
                'print("C17_CHILD_REPLY "+line,flush=True); '
                f'helper=subprocess.Popen([sys.executable,"-B","-c",{helper_code!r},'
                'sys.argv[1],sys.argv[2]]); '
                'end=time.monotonic()+6; '
                'exec("while not Path(sys.argv[3]).exists() and time.monotonic()<end: time.sleep(.01)"); '
                'Path(sys.argv[4]).write_text("root PTY result "+line); '
                'sys.exit(0)'
            )
        child = handle = attempt = None
        started = time.monotonic()
        try:
            child, handle, attempt = _spawn_bound_os_worker(r.guard, task,
                [sys.executable, '-B', '-c', child_code, str(helper_ready),
                 str(helper_release), str(leader_release), artifact, str(child_consumed)],
                stdin=None, stdout=None, stderr=None)
            stamp('handoff', child_pid=child.pid,
                  child_created=handle['binding']['leader_created'],
                  child_pgid=os.getpgid(child.pid), foreground=os.tcgetpgrp(0),
                  wrapper_pgid=own, foreign_pgid=os.getpgid(foreign.pid),
                  tty=os.ttyname(0))
            assert child.pid == os.tcgetpgrp(0) != own
            assert os.getpgid(foreign.pid) not in (child.pid, own)

            controller_error = []
            def control_helper():
                try:
                    stamp('controller_started')
                    deadline = time.monotonic() + 4
                    while not helper_ready.exists() and time.monotonic() < deadline:
                        time.sleep(.01)
                    assert helper_ready.exists(), 'PTY helper did not start'
                    pid = int(helper_ready.read_text())
                    process = psutil.Process(pid)
                    created = process.create_time()
                    assert os.getpgid(pid) == child.pid != own
                    stamp('helper_alive', pid=pid, created=created, pgid=os.getpgid(pid),
                          observation=process_snapshot(pid) if outcome == 'stop' else None)
                    while time.monotonic() < deadline:
                        row = r.guard.get(task)
                        if any(entry['pid'] == pid and entry['created'] == created
                               for entry in row.get('worker_descendants', [])):
                            break
                        time.sleep(.01)
                    else:
                        raise AssertionError('PTY helper was not captured before leader release')
                    stamp('helper_captured', observation=process_snapshot(pid)
                          if outcome == 'stop' else None)
                    leader_release.touch()
                    stamp('leader_released')
                    while time.monotonic() < deadline:
                        if psutil.Process(child.pid).status() == psutil.STATUS_ZOMBIE:
                            break
                        time.sleep(.01)
                    assert psutil.Process(child.pid).status() == psutil.STATUS_ZOMBIE
                    stamp('leader_exited')
                    if outcome == 'success':
                        time.sleep(.4)
                        assert process.create_time() == created
                        helper_release.touch()
                        stamp('helper_released')
                    elif outcome == 'stop':
                        assert process.create_time() == created
                        assert process.status() != psutil.STATUS_ZOMBIE
                        with r.guard.transaction() as rows:
                            rows.grants['batches'][payload['batch']]['revoked'] = True
                        stamp('revocation_committed')
                        with r.guard.transaction(read_only=True) as rows:
                            row = rows[task]
                            batch = rows.grants['batches'][payload['batch']]
                            assert batch['revoked'] is True and row['policy']['revoked'] is True
                            assert row['worker_supervision']['state'] == 'observing'
                            assert any(entry['pid'] == pid and entry['created'] == created
                                       for entry in row['worker_descendants'])
                            remaining = min(
                                row['policy']['work_limit'] - row['policy'].get('work_seconds', 0),
                                batch['item']['budget']['todo_seconds'] - batch['item']['work_seconds'])
                            assert remaining > 0 and time.monotonic() - started < remaining
                            stamp('native_revocation', batch_revoked=batch['revoked'],
                                  policy_revoked=row['policy']['revoked'],
                                  supervision=row['worker_supervision']['state'],
                                  status=row['status'], remaining=remaining,
                                  helper_pid=pid, helper_created=created,
                                  helper_status=process.status(),
                                  helper_observation=process_snapshot(pid),
                                  leader_observation=process_snapshot(child.pid))
                except BaseException as exc:
                    controller_error.append(repr(exc))
                    stamp('controller_error', error=repr(exc))
                finally:
                    controller_report.write_text(json.dumps(dict(
                        errors=controller_error, stages=stages)))

            controller = None
            if outcome != 'exception':
                controller = threading.Thread(target=control_helper, daemon=True)
                controller.start()
            try:
                if outcome == 'exception':
                    deadline = time.monotonic() + 4
                    while not child_consumed.exists() and time.monotonic() < deadline:
                        time.sleep(.01)
                    assert child_consumed.exists(), ('PTY child did not consume its token: '
                                                     + repr(stages))
                    stamp('child_consumed', child_pid=child.pid)
                    original_children = psutil.Process.children
                    def inspection_failure(process, *args, **kwargs):
                        if process.pid == child.pid:
                            raise PermissionError(errno.EPERM, 'test-owned PTY inspection denial')
                        return original_children(process, *args, **kwargs)
                    psutil.Process.children = inspection_failure
                    try:
                        with pytest.raises(ValueError, match='worker process inspection unavailable'):
                            item.wait_worker(r.guard, attempt, child, started, handle)
                    finally:
                        psutil.Process.children = original_children
                    stamp('exceptional_cleanup')
                elif outcome == 'stop':
                    with pytest.raises(ValueError, match='worker stopped by budget or revocation'):
                        item.wait_worker(r.guard, attempt, child, started, handle)
                    stamp('stopped')
                else:
                    code = item.wait_worker(r.guard, attempt, child, started, handle)
                    assert code == 0
                    stamp('supervision_succeeded')
                    native = r.guard.record_worker_result(attempt, code)
                    assert native['artifact_status'] == 'present'
                    assert Path(artifact).read_text() == 'root PTY result ' + token
                    assert native['evidence']['sha256'] == dc.digest(artifact)
                    assert r.guard.observe(task, attempt['generation'], 'completed', native['evidence'])
                    assert r.guard.get(task)['gate'] == 'main_validation'
                    assert r.guard.record_worker_result(attempt, code) == native
                    stamp('native_completed')
                row = r.guard.get(task)
                proof = row['worker_supervision']
                assert all(entry['pid'] != foreign.pid for entry in row.get('worker_descendants', []))
                if outcome == 'success':
                    assert proof['state'] == 'exited' and proof['owned_group_empty'] is True
                if helper_ready.exists():
                    helper_pid = int(helper_ready.read_text())
                    helper_created = next(stage['created'] for stage in stages
                                          if stage['stage'] == 'helper_alive')
                    deadline = time.monotonic() + 2
                    while time.monotonic() < deadline:
                        try:
                            helper = psutil.Process(helper_pid)
                            assert helper.create_time() == helper_created
                            if helper.status() == psutil.STATUS_ZOMBIE:
                                stamp('helper_final_zombie', observation=process_snapshot(helper_pid))
                                break
                        except psutil.NoSuchProcess:
                            stamp('helper_final_absent', pid=helper_pid)
                            break
                        time.sleep(.01)
                    else:
                        stamp('helper_final_survivor', observation=process_snapshot(helper_pid))
                        raise AssertionError('owned PTY helper survived supervision cleanup')
                    stamp('helper_exited', pid=helper_pid)
                assert proof['accounted_seconds'] > 0
                with r.guard.transaction(read_only=True) as rows:
                    assert rows.grants['batches'][payload['batch']]['item']['work_seconds'] == proof['accounted_seconds']
                if outcome != 'success':
                    assert proof['state'] == 'unknown' and not dc.worker_operational_exit(row)
                    assert row['gate'] != 'main_validation' and not handle['released']
                    with pytest.raises(ValueError):
                        r.guard.record_worker_result(attempt, 0)
                    stamp('native_result_refused', gate=row['gate'], released=handle['released'])
                stamp('accounted', seconds=proof['accounted_seconds'], state=proof['state'])
                if outcome == 'success':
                    released = next(stage['at'] for stage in stages if stage['stage'] == 'helper_released')
                    assert proof['accounted_seconds'] >= released - started - .1
            finally:
                leader_release.touch()
                helper_release.touch()
                if controller is not None:
                    controller.join(timeout=5)
                    assert not controller.is_alive()
                    assert not controller_error, controller_error
        finally:
            if handle is not None:
                item.close_item_group(handle)
            stamp('restored', foreground=os.tcgetpgrp(0), wrapper_pgid=own,
                  closed=handle is not None and handle['closed'])
            assert os.tcgetpgrp(0) == own
            if child is not None and child.returncode is None:
                child.wait(timeout=3)
            report.write_text(json.dumps(dict(outcome=outcome, stages=stages,
                controller=json.loads(controller_report.read_text()) if controller_report.exists() else None,
                foreign_pid=foreign.pid, foreign_created=foreign_created,
                helper_pid=int(helper_ready.read_text()) if helper_ready.exists() else None)))
        print('C17_WRAPPER_POST_READY', flush=True)
        assert sys.stdin.readline().strip() == token + '-wrapper'
        print('C17_WRAPPER_POST_REPLY ' + token, flush=True)

    def shell():
        start_read, start_write = os.pipe()
        wrapper_pid = os.fork()
        if wrapper_pid == 0:
            os.close(start_write)
            os.setpgid(0, 0)
            os.read(start_read, 1)
            try:
                wrapper()
            except BaseException as exc:
                import traceback
                traceback.print_exc()
                print('C17_WRAPPER_ERROR ' + repr(exc), flush=True)
                os._exit(2)
            os._exit(0)
        os.close(start_read)
        os.setpgid(wrapper_pid, wrapper_pid)
        os.tcsetpgrp(0, wrapper_pid)
        os.write(start_write, b'1')
        os.close(start_write)
        deadline = time.monotonic() + 11
        while time.monotonic() < deadline:
            ended, status = os.waitpid(wrapper_pid, os.WNOHANG)
            if ended:
                break
            time.sleep(.02)
        else:
            os.kill(wrapper_pid, signal.SIGINT)  # Direct test-owned child only.
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                ended, status = os.waitpid(wrapper_pid, os.WNOHANG)
                if ended:
                    break
                time.sleep(.02)
            else:
                os.kill(wrapper_pid, signal.SIGKILL)
                ended, status = os.waitpid(wrapper_pid, 0)
        assert ended == wrapper_pid and os.waitstatus_to_exitcode(status) == 0
        previous = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGTTOU})
        try:
            os.tcsetpgrp(0, os.getpgrp())
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, previous)
        print('C17_SHELL_POST_READY', flush=True)
        assert sys.stdin.readline().strip() == token + '-shell'
        print('C17_SHELL_POST_REPLY ' + token, os.tcgetpgrp(0), flush=True)

    try:
        shell_pid, master = pty.fork()
        if shell_pid == 0:
            # pytest's Python capture streams survive fork; only this disposable
            # PTY child owns these new text streams backed by its real slave fds.
            sys.stdin = os.fdopen(os.dup(0), 'r', buffering=1)
            sys.stdout = os.fdopen(os.dup(1), 'w', buffering=1)
            sys.stderr = os.fdopen(os.dup(2), 'w', buffering=1)
            try:
                shell()
            except BaseException as exc:
                print('C17_SHELL_ERROR ' + repr(exc), flush=True)
                os._exit(3)
            os._exit(0)
        sent = set()
        deadline = time.monotonic() + 14
        while time.monotonic() < deadline and 'shell' not in sent:
            readable, _, _ = select.select([master], [], [], .1)
            if readable:
                try:
                    chunk = os.read(master, 4096)
                except OSError as exc:
                    if exc.errno == errno.EIO:
                        break
                    raise
                if not chunk:
                    break
                output.extend(chunk)
                if b'C17_WRAPPER_ERROR' in output or b'C17_SHELL_ERROR' in output:
                    break
                for marker, label, line in (
                    (b'C17_CHILD_READY', 'child', token),
                    (b'C17_WRAPPER_POST_READY', 'wrapper', token + '-wrapper'),
                    (b'C17_SHELL_POST_READY', 'shell', token + '-shell')):
                    if marker in output and label not in sent:
                        os.write(master, (line + '\n').encode())
                        sent.add(label)
        deadline = time.monotonic() + 3
        while b'C17_SHELL_POST_REPLY' not in output and time.monotonic() < deadline:
            readable, _, _ = select.select([master], [], [], .1)
            if readable:
                try:
                    chunk = os.read(master, 4096)
                except OSError as exc:
                    if exc.errno == errno.EIO:
                        break
                    raise
                if not chunk:
                    break
                output.extend(chunk)
                if b'C17_WRAPPER_ERROR' in output or b'C17_SHELL_ERROR' in output:
                    break
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            ended, status = os.waitpid(shell_pid, os.WNOHANG)
            if ended:
                break
            time.sleep(.02)
        else:
            pytest.fail('PTY shell did not return within bounded deadline: '
                        + output.decode(errors='replace'))
        completed_shell_pid = shell_pid
        shell_pid = None
        assert os.waitstatus_to_exitcode(status) == 0, (
            output.decode(errors='replace') + '\nPTY report: ' +
            (report.read_text() if report.exists() else '<missing>') +
            '\nController report: ' +
            (controller_report.read_text() if controller_report.exists() else '<missing>'))
        transcript = output.decode(errors='replace')
        assert 'C17_WRAPPER_POST_REPLY ' + token in transcript
        assert f'C17_SHELL_POST_REPLY {token} {completed_shell_pid}' in transcript
        assert 'C17_CHILD_REPLY ' + token in transcript
        evidence = json.loads(report.read_text())
        assert evidence['outcome'] == outcome
        names = [stage['stage'] for stage in evidence['stages']]
        assert names.index('wrapper_foreground') < names.index('handoff') < names.index('restored')
        if outcome == 'success':
            assert [names.index(name) for name in ('helper_alive', 'helper_captured',
                'leader_released', 'leader_exited', 'helper_released', 'supervision_succeeded',
                'native_completed', 'helper_exited', 'restored')] == sorted(
                names.index(name) for name in ('helper_alive', 'helper_captured',
                'leader_released', 'leader_exited', 'helper_released', 'supervision_succeeded',
                'native_completed', 'helper_exited', 'restored'))
        elif outcome == 'stop':
            assert names.index('leader_exited') < names.index('native_revocation') < names.index('stopped')
            assert evidence['controller']['errors'] == []
            handoff = next(stage for stage in evidence['stages'] if stage['stage'] == 'handoff')
            assert any(stage['stage'] == 'signal_return' and
                       stage['pgid'] == handoff['child_pid'] and
                       stage['signal'] == signal.SIGTERM for stage in evidence['stages'])
            assert 'helper_final_absent' in names or 'helper_final_zombie' in names
            revocation = next(stage for stage in evidence['stages']
                              if stage['stage'] == 'native_revocation')
            assert revocation['batch_revoked'] and revocation['policy_revoked']
            assert revocation['helper_status'] != psutil.STATUS_ZOMBIE
        else:
            assert names.index('handoff') < names.index('child_consumed') < names.index('exceptional_cleanup') < names.index('restored')
        assert evidence['stages'][-1]['foreground'] == evidence['stages'][0]['pgid']
        assert foreign.poll() is None
        assert psutil.Process(foreign.pid).create_time() == foreign_created
        print('ROOT_PTY_ITEM_EVIDENCE ' + json.dumps(dict(evidence=evidence,
            transcript=transcript, foreign_alive=True), sort_keys=True))
    finally:
        leader_release.touch()
        helper_release.touch()
        if master is not None:
            os.close(master)
        if shell_pid is not None:
            if wrapper_identity.exists():
                identity = json.loads(wrapper_identity.read_text())
                try:
                    process = psutil.Process(identity['pid'])
                    if process.create_time() == identity['created'] and process.is_running():
                        process.send_signal(signal.SIGINT)
                except psutil.NoSuchProcess:
                    pass
            try:
                os.kill(shell_pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            os.waitpid(shell_pid, 0)
        if foreign.poll() is None and psutil.Process(foreign.pid).create_time() == foreign_created:
            foreign.terminate()
        foreign.wait(timeout=3)


@pytest.mark.asyncio
@pytest.mark.live_system_guard_bypass
async def test_authority_loss_between_scan_and_exit_publication_stays_unknown(route, monkeypatch, tmp_path):
    """The last ledger write must not publish an exit against an earlier authority read."""
    import threading
    from contextlib import contextmanager

    r = route
    _, payload = await setup(r, monkeypatch)
    item.control(r.guard, r.identity, payload)
    register(r)
    ready = tmp_path / 'publication-ready'
    release = tmp_path / 'publication-release'
    code = ('import sys,time; from pathlib import Path; '
            'ready,release=map(Path,sys.argv[1:]); ready.touch(); '
            'end=time.monotonic()+8; '
            'exec("while not release.exists() and time.monotonic()<end: time.sleep(.02)")')
    started = time.monotonic()
    leader, handle, attempt = _spawn_bound_os_worker(
        r.guard, r.task['task'], [sys.executable, '-c', code, str(ready), str(release)])
    result = {}
    thread = None
    try:
        until = time.monotonic() + 2
        while not ready.exists() and time.monotonic() < until:
            time.sleep(.02)
        assert ready.exists()
        original_transaction = r.guard.transaction
        injected = []
        publication_seen = []

        @contextmanager
        def transaction(*, read_only=False):
            with original_transaction(read_only=read_only) as rows:
                proof = rows[attempt['task']].get('worker_supervision') or {}
                if (not read_only and not injected and proof.get('state') == 'observing'
                        and leader.returncode is not None):
                    publication_seen.append((leader.returncode, proof['state']))
                    assert leader.returncode == 0
                    rows.grants['batches'][payload['batch']]['revoked'] = True
                    injected.append(True)
                yield rows

        monkeypatch.setattr(r.guard, 'transaction', transaction)
        def supervise():
            try:
                result['code'] = item.wait_worker(r.guard, attempt, leader, started, handle)
            except BaseException as exc:
                result['error'] = exc
        thread = threading.Thread(target=supervise, daemon=True)
        thread.start()
        until = time.monotonic() + 2
        while (r.guard.get(attempt['task']).get('worker_supervision') or {}).get('state') != 'observing' and time.monotonic() < until:
            if result.get('error'):
                raise result['error']
            time.sleep(.02)
        assert r.guard.get(attempt['task'])['worker_supervision']['state'] == 'observing'
        release.touch()
        thread.join(timeout=5)
        assert injected == [True]
        assert publication_seen == [(0, 'observing')]
        assert not thread.is_alive() and isinstance(result.get('error'), ValueError)
        assert 'authority ended before operational exit' in str(result['error'])
        row = r.guard.get(attempt['task'])
        assert row['worker_supervision']['state'] == 'unknown'
        assert row['worker_supervision']['accounted_seconds'] > 0
        assert not dc.executor_released(row)
        with pytest.raises(ValueError, match='operational exit is unknown'):
            r.guard.record_worker_result(attempt, 0)
        with r.guard.transaction(read_only=True) as rows:
            assert rows.grants['batches'][payload['batch']]['item']['work_seconds'] == row['worker_supervision']['accounted_seconds']
    finally:
        release.touch()
        if leader.poll() is None:
            leader.wait(timeout=9)
        if thread is not None:
            thread.join(timeout=6)
        item.close_item_group(handle)


@pytest.mark.asyncio
async def test_supervised_result_passes_exit_consumer_after_real_wrapper_exit(route, monkeypatch, tmp_path):
    r = route
    _, payload = await setup(r, monkeypatch)
    item.control(r.guard, r.identity, payload)
    register(r)
    ready = tmp_path / 'wrapped-leader-ready'
    release = tmp_path / 'wrapped-leader-release'
    with r.guard.transaction() as rows:
        # Exercise the direct exit consumer with a real wrapper process that
        # runs the native supervisor and then actually exits.
        rows[r.task['task']]['direct_worker'] = True
    wrapper_code = '''
import os, subprocess, sys, time
from pathlib import Path
from hermes_cli import diggr_continuation as dc, diggr_item as item
path, task, ready, release = sys.argv[1:]
guard = dc.Guard(path)
artifact = guard.get(task)['artifact']
leader_code = (
    'import sys,time; from pathlib import Path; '
    'ready,release,artifact=map(Path,sys.argv[1:]); ready.touch(); '
    'end=time.monotonic()+6; '
    'exec("while not release.exists() and time.monotonic()<end: time.sleep(.02)"); '
    'artifact.write_text("wrapped native result")'
)
started = time.monotonic()
leader, handle = item.spawn_item_child(
    [sys.executable, '-c', leader_code, ready, release, artifact],
    str(Path(path).parent), guard.get(task))
end = time.monotonic() + 2
while not Path(ready).exists() and time.monotonic() < end:
    time.sleep(.02)
if not Path(ready).exists():
    raise ValueError('real leader never became ready')
with guard.transaction() as rows:
    row = rows[task]
    row.update(worker_pid=os.getpid(), worker_identity=dc.process_identity(os.getpid()),
               child_identity=dc.process_identity(leader.pid), worker_started_ns=time.time_ns(),
               worker_group=handle['binding'], claim_phase='claimed', visible_sent=True)
attempt = guard.get(task)
try:
    code = item.wait_worker(guard, attempt, leader, started, handle)
finally:
    item.close_item_group(handle)
result = guard.record_worker_result(attempt, code)
guard.observe(task, attempt['generation'], 'completed', result['evidence'])
'''
    wrapper = subprocess.Popen([sys.executable, '-c', wrapper_code, str(r.guard.path),
                                r.task['task'], str(ready), str(release)],
                               cwd=str(Path(__file__).resolve().parents[2]))
    try:
        until = time.monotonic() + 3
        while time.monotonic() < until:
            if wrapper.poll() is not None:
                pytest.fail('native wrapper exited before observed leader scan')
            row = r.guard.get(r.task['task'])
            if row.get('worker_supervision', {}).get('state') == 'observing':
                break
            time.sleep(.02)
        else:
            pytest.fail('native wrapper never observed its real leader')
        release.touch()
        assert wrapper.wait(timeout=8) == 0
        row = r.guard.get(r.task['task'])
        assert row['gate'] == 'main_validation' and row['worker_result']['exit_code'] == 0
        dc.verify_direct_worker_exit(row, completed=True)
    finally:
        release.touch()
        wrapper.wait(timeout=8)


@pytest.mark.asyncio
@pytest.mark.live_system_guard_bypass
async def test_captured_os_helper_finishes_after_leader_before_native_result(route, monkeypatch, tmp_path):
    import threading

    r = route
    _, payload = await setup(r, monkeypatch)
    item.control(r.guard, r.identity, payload)
    register(r)
    ready = tmp_path / 'helper-ready'
    leader_release = tmp_path / 'leader-release'
    helper_release = tmp_path / 'helper-release'
    done = tmp_path / 'helper-done'
    pid_file = tmp_path / 'helper-pid'
    artifact = r.guard.get(r.task['task'])['artifact']
    helper_code = (
        'import os,sys,time; from pathlib import Path; '
        'ready,release,done=map(Path,sys.argv[1:]); '
        f'{_atomic_test_pid_code("ready", "os.getpid()")}'
        'end=time.monotonic()+7; '
        'exec("while not release.exists() and time.monotonic()<end: time.sleep(.02)"); done.touch()'
    )
    leader_code = (
        'import subprocess,sys,time; from pathlib import Path; '
        'ready,leader_release,helper_release,done,pid_file,artifact=map(Path,sys.argv[1:]); '
        f'helper=subprocess.Popen([sys.executable,"-c",{helper_code!r},str(ready),str(helper_release),str(done)]); '
        f'{_atomic_test_pid_code("pid_file", "helper.pid")}'
        'end=time.monotonic()+5; '
        'exec("while not ready.exists() and time.monotonic()<end: time.sleep(.02)"); '
        'exec("while not leader_release.exists() and time.monotonic()<end: time.sleep(.02)"); '
        'artifact.write_text("real captured helper result"); sys.exit(0)'
    )
    started = time.monotonic()
    leader, handle, attempt = _spawn_bound_os_worker(
        r.guard, r.task['task'], [sys.executable, '-c', leader_code, str(ready),
        str(leader_release), str(helper_release), str(done), str(pid_file), artifact])
    result = {}
    thread = None
    try:
        until = time.monotonic() + 2
        while not ready.exists() and time.monotonic() < until:
            time.sleep(.02)
        helper_pid = int(pid_file.read_text())
        assert int(ready.read_text()) == helper_pid
        def supervise():
            try:
                result['code'] = item.wait_worker(r.guard, attempt, leader, started, handle)
            except BaseException as exc:
                result['error'] = exc
        thread = threading.Thread(target=supervise, daemon=True)
        thread.start()
        until = time.monotonic() + 2
        while time.monotonic() < until:
            if 'error' in result:
                raise result['error']
            row = r.guard.get(attempt['task'])
            if row.get('worker_supervision', {}).get('state') == 'observing' and any(
                    entry['pid'] == helper_pid for entry in row.get('worker_descendants', [])):
                break
            time.sleep(.02)
        else:
            pytest.fail('real helper was not captured before leader exit')
        leader_release.touch()
        import psutil
        until = time.monotonic() + 3
        while psutil.Process(leader.pid).status() != psutil.STATUS_ZOMBIE and time.monotonic() < until:
            time.sleep(.01)
        assert psutil.Process(leader.pid).status() == psutil.STATUS_ZOMBIE
        leader_elapsed = time.monotonic() - started
        assert not done.exists() and thread.is_alive()
        with pytest.raises(ValueError, match='operational exit'):
            r.guard.record_worker_result(attempt, 0)
        assert not dc.executor_released(r.guard.get(attempt['task']))
        helper_release.touch()
        thread.join(timeout=7)
        assert not thread.is_alive() and result.get('code') == 0 and not result.get('error')
        native = r.guard.record_worker_result(attempt, 0)
        assert native['artifact_status'] == 'present'
        assert r.guard.observe(attempt['task'], attempt['generation'], 'completed', native['evidence'])
        row = r.guard.get(attempt['task'])
        assert row['gate'] == 'main_validation' and row['worker_supervision']['state'] == 'exited'
        assert row['worker_supervision']['accounted_seconds'] >= leader_elapsed
    finally:
        leader_release.touch()
        helper_release.touch()
        if leader.poll() is None:
            leader.wait(timeout=6)
        if thread is not None:
            thread.join(timeout=8)
        item.close_item_group(handle)


@pytest.mark.asyncio
@pytest.mark.live_system_guard_bypass
@pytest.mark.parametrize('stop_kind', ['budget', 'revocation'])
async def test_after_leader_exit_item_stop_accounts_once_and_spares_foreign_process(
        route, monkeypatch, tmp_path, stop_kind):
    import threading

    r = route
    _, payload = await setup(r, monkeypatch)
    item.control(r.guard, r.identity, payload)
    register(r)
    ready = tmp_path / 'stop-helper-ready'
    leader_release = tmp_path / 'stop-leader-release'
    helper_release = tmp_path / 'stop-helper-release'
    foreign_release = tmp_path / 'foreign-release'
    pid_file = tmp_path / 'stop-helper-pid'
    helper_code = (
        'import os,sys,time; from pathlib import Path; '
        'ready,release=map(Path,sys.argv[1:]); '
        f'{_atomic_test_pid_code("ready", "os.getpid()")}'
        'end=time.monotonic()+30; '
        'exec("while not release.exists() and time.monotonic()<end: time.sleep(.02)")'
    )
    leader_code = (
        'import subprocess,sys,time; from pathlib import Path; '
        'ready,leader_release,helper_release,pid_file=map(Path,sys.argv[1:]); '
        f'helper=subprocess.Popen([sys.executable,"-c",{helper_code!r},str(ready),str(helper_release)]); '
        f'{_atomic_test_pid_code("pid_file", "helper.pid")}'
        'end=time.monotonic()+5; '
        'exec("while not ready.exists() and time.monotonic()<end: time.sleep(.02)"); '
        'exec("while not leader_release.exists() and time.monotonic()<end: time.sleep(.02)")'
    )
    started = time.monotonic()
    leader, handle, attempt = _spawn_bound_os_worker(
        r.guard, r.task['task'], [sys.executable, '-c', leader_code,
        str(ready), str(leader_release), str(helper_release), str(pid_file)])
    foreign = subprocess.Popen([sys.executable, '-c',
        'import sys,time; from pathlib import Path; release=Path(sys.argv[1]); '
        'end=time.monotonic()+15; '
        'exec("while not release.exists() and time.monotonic()<end: time.sleep(.02)")',
        str(foreign_release)])
    result = {}
    thread = None
    helper_identity = None
    try:
        until = time.monotonic() + 2
        while not ready.exists() and time.monotonic() < until:
            time.sleep(.02)
        helper_pid = int(pid_file.read_text())
        def supervise():
            try:
                result['code'] = item.wait_worker(r.guard, attempt, leader, started, handle)
            except BaseException as exc:
                result['error'] = exc
        thread = threading.Thread(target=supervise, daemon=True)
        thread.start()
        until = time.monotonic() + 2
        while time.monotonic() < until:
            if 'error' in result:
                raise result['error']
            if any(entry['pid'] == helper_pid for entry in
                   r.guard.get(attempt['task']).get('worker_descendants', [])):
                break
            time.sleep(.02)
        else:
            pytest.fail('real helper not captured before stop')
        leader_release.touch()
        import psutil
        until = time.monotonic() + 3
        while psutil.Process(leader.pid).status() != psutil.STATUS_ZOMBIE and time.monotonic() < until:
            time.sleep(.01)
        assert psutil.Process(leader.pid).status() == psutil.STATUS_ZOMBIE
        assert thread.is_alive()
        import psutil
        helper_identity = next(entry for entry in r.guard.get(attempt['task'])['worker_descendants']
                               if entry['pid'] == helper_pid)
        assert psutil.Process(helper_pid).create_time() == helper_identity['created']
        assert psutil.Process(helper_pid).status() != psutil.STATUS_ZOMBIE
        with r.guard.transaction() as rows:
            batch = rows.grants['batches'][payload['batch']]
            if stop_kind == 'budget':
                batch['item']['budget']['todo_seconds'] = 0
            else:
                batch['revoked'] = True
        with r.guard.transaction(read_only=True) as rows:
            row = rows[attempt['task']]
            assert (row['policy']['work_limit'] == 0 if stop_kind == 'budget'
                    else row['policy']['revoked'] is True)
        thread.join(timeout=12)
        assert not thread.is_alive() and isinstance(result.get('error'), ValueError)
        assert 'stopped by budget or revocation' in str(result['error'])
        row = r.guard.get(attempt['task'])
        proof = row['worker_supervision']
        assert proof['state'] == 'unknown' and proof['accounted_seconds'] > 0
        assert foreign.poll() is None
        assert not dc.executor_released(row)
        with r.guard.transaction(read_only=True) as rows:
            spent = rows.grants['batches'][payload['batch']]['item']['work_seconds']
        assert spent == proof['accounted_seconds']
        with pytest.raises(ValueError, match='already recorded'):
            item.wait_worker(r.guard, attempt, leader, started, handle)
        with r.guard.transaction(read_only=True) as rows:
            assert rows.grants['batches'][payload['batch']]['item']['work_seconds'] == spent
    finally:
        leader_release.touch()
        helper_release.touch()
        foreign_release.touch()
        if leader.poll() is None:
            leader.wait(timeout=8)
        if thread is not None:
            thread.join(timeout=12)
        item.close_item_group(handle)
        if pid_file.exists() and helper_identity is not None:
            import psutil
            try:
                owned = psutil.Process(int(pid_file.read_text()))
                if owned.create_time() == helper_identity['created'] and owned.status() != psutil.STATUS_ZOMBIE:
                    owned.terminate()
                    try:
                        owned.wait(timeout=3)
                    except psutil.TimeoutExpired:
                        owned.kill()
                        owned.wait(timeout=3)
            except psutil.NoSuchProcess:
                pass
        foreign.wait(timeout=5)


@pytest.mark.asyncio
async def test_item_stop_authority_is_hydrated_from_persisted_grant(route, monkeypatch):
    r = route
    _, payload = await setup(r, monkeypatch)
    item.control(r.guard, r.identity, payload)
    register(r)
    with r.guard.transaction(read_only=True) as rows:
        original_limit = rows[r.task['task']]['policy']['work_limit']
    with r.guard.transaction() as rows:
        rows[r.task['task']]['policy'].update(work_limit=0, revoked=True)
    with r.guard.transaction(read_only=True) as rows:
        assert rows[r.task['task']]['policy']['work_limit'] == original_limit
        assert rows[r.task['task']]['policy']['revoked'] is False
    with r.guard.transaction() as rows:
        batch = rows.grants['batches'][payload['batch']]
        batch['item']['budget']['todo_seconds'] = 0
        batch['revoked'] = True
    with r.guard.transaction(read_only=True) as rows:
        assert rows[r.task['task']]['policy']['work_limit'] == 0
        assert rows[r.task['task']]['policy']['revoked'] is True


@pytest.mark.asyncio
async def test_started_item_unknown_cannot_use_integer_artifact_or_exit_consumers(route, monkeypatch):
    r = route
    _, payload = await setup(r, monkeypatch)
    item.control(r.guard, r.identity, payload)
    register(r)
    with r.guard.transaction() as rows:
        row = rows[r.task['task']]
        row.update(worker_pid=987654321, worker_identity=dict(pid=987654321, start='old', executable='python'),
                   worker_started_ns=1, child_identity=dict(pid=987654322, start='old', executable='python'),
                   child_exited=True, claim_phase='claimed', visible_sent=True)
    attempt = r.guard.get(r.task['task'])
    Path(attempt['artifact']).write_text('present but unobserved')
    evidence = {k: attempt[k] for k in ('task', 'generation', 'action', 'artifact')}
    evidence.update(sha256=dc.digest(attempt['artifact']), result='ready_for_main_review', exit_code=0)
    with pytest.raises(ValueError, match='operational exit'):
        r.guard.record_worker_result(attempt, 0)
    assert r.guard.observe(attempt['task'], attempt['generation'], 'unknown', evidence)
    row = r.guard.get(attempt['task'])
    assert row['gate'] == 'reconcile' and row['action'] != 'validate coding evidence'
    assert not dc.executor_released(row)
    for check in (dc.verify_prior_exit, dc.verify_restarted_reservation_exit, item.verify_completed_exit):
        with pytest.raises(ValueError, match='operational exit'):
            check(row)
    with pytest.raises(ValueError, match='operational exit'):
        dc.verify_direct_worker_exit(dict(row, direct_worker=True), completed=True)
    from hermes_cli import diggr_successor
    with pytest.raises(ValueError, match='operational exit'):
        diggr_successor.verify_exit(row)
    with pytest.raises(ValueError, match='operational exit'):
        r.guard.record_worker_result(attempt, 0)  # replay cannot create exit evidence


@pytest.mark.asyncio
async def test_observer_does_not_complete_item_with_forged_result(route, monkeypatch):
    r = route
    _, payload = await setup(r, monkeypatch)
    item.control(r.guard, r.identity, payload)
    register(r)
    with r.guard.transaction() as rows:
        row = rows[r.task['task']]
        row.update(worker_pid=987654321, worker_identity=dict(pid=987654321, start='old', executable='python'),
                   worker_started_ns=1, child_identity=dict(pid=987654322, start='old', executable='python'),
                   claim_phase='claimed', visible_sent=True, child_exited=True)
        row['worker_result'] = dict(task=row['task'], generation=row['generation'],
            artifact=row['artifact'], action_id=row['action_id'], effect_id=row['effect_id'],
            exit_code=0, artifact_status='present', evidence=None)
    dc.observe_processes(r.guard)
    row = r.guard.get(r.task['task'])
    assert row['gate'] == 'reconcile' and row['action'] != 'validate coding evidence'
    assert not dc.executor_released(row)


@pytest.mark.asyncio
async def test_supervision_proof_stays_bound_to_one_item_attempt(route, monkeypatch):
    r = route
    _, payload = await setup(r, monkeypatch)
    item.control(r.guard, r.identity, payload)
    register(r)
    with r.guard.transaction() as rows:
        row = rows[r.task['task']]
        row.update(worker_pid=987654321, worker_identity=dict(pid=987654321, start='old', executable='python'),
                   worker_started_ns=1, child_identity=dict(pid=987654322, start='old', executable='python'),
                   claim_phase='claimed', visible_sent=True)
        row['worker_supervision'] = dict(
            binding=dc.worker_supervision_binding(row), state='exited',
            leader_exit_code=0, accounted_seconds=1)
    attempt = r.guard.get(r.task['task'])
    assert not dc.worker_operational_exit(attempt)  # No native isolated-group spawn.
    assert not dc.worker_operational_exit(dict(attempt, child_identity=dict(pid=123, start='reused', executable='python')))
    assert not dc.worker_operational_exit(dict(attempt, effect_id='stale'))
    assert not dc.worker_operational_exit(dict(attempt, worker_supervision=dict(
        attempt['worker_supervision'], state='unknown', reason='scan unavailable')))
    with r.guard.transaction() as rows:
        rows[attempt['task']]['effect_id'] = 'new-attempt'
    with pytest.raises(ValueError, match='different attempt'):
        r.guard.record_worker_result(attempt, 0)


@pytest.mark.asyncio
async def test_acceptance_replay_does_not_enqueue_another_main_turn(route, monkeypatch):
    r = route
    _, payload, candidate_payload = await completed_candidate(r, monkeypatch)
    result = item.control(r.guard, r.identity, candidate_payload)
    event = await r.event('/continuation accept ' + result['candidate']['id'])
    await r.runner._diggr_accept(event)
    before = r.guard.get(r.task['task'])['generation']
    await r.runner._diggr_accept(event)
    assert r.guard.get(r.task['task'])['generation'] == before


@pytest.mark.asyncio
async def test_missing_build_output_keeps_item_repairable_without_test_invitation(route, monkeypatch):
    r = route
    _, payload, request = await completed_candidate(r, monkeypatch)
    broken = dict(request, build_argv=[sys.executable, '-c', 'pass'])
    with pytest.raises(ValueError, match='provenance failed'):
        item.control(r.guard, r.identity, broken)
    state = item.status(r.guard, r.identity, payload['batch'], payload['issue'])
    assert state['phase'] == 'candidate_failed'
    assert not state['item'].get('candidate')
    assert item.control(r.guard, r.identity, request)['candidate']['tree']


@pytest.mark.asyncio
async def test_budget_extension_resumes_only_coordinator_and_retains_worker_evidence(route, monkeypatch):
    r = route
    _, payload = await setup(r, monkeypatch)
    item.control(r.guard, r.identity, payload)
    register(r)
    with r.guard.transaction() as rows:
        current = rows.grants['batches'][payload['batch']]['item']
        current['work_seconds'] = current['budget']['todo_seconds']
        row = rows[r.task['task']]
        row.update(status='paused', reason='owner policy missing, revoked or exhausted',
                   worker_pid=987654321, worker_identity={'pid': 987654321, 'created': 1})
    before = r.guard.get(r.task['task'])
    state = item.status(r.guard, r.identity, payload['batch'], payload['issue'])
    event = await r.event('/continuation extend ' + payload['batch'] + ' ' + state['budget_hash'] + ' 600 2 1 0')
    await r.runner._diggr_accept(event)
    row = r.guard.get(r.task['task'])
    assert row['status'] == 'pending' and row['gate'] == 'reconcile'
    assert row['generation'] == before['generation'] + 1
    assert row['worker_identity'] == before['worker_identity']
    assert row['policy']['work_seconds'] == owner.BUDGETS['todo_seconds']
    await r.runner._diggr_accept(event)
    assert r.guard.get(r.task['task'])['generation'] == row['generation']


@pytest.mark.asyncio
async def test_preparation_failure_before_git_can_resume_only_original_reservation(route, monkeypatch):
    r = route
    _, payload = await setup(r, monkeypatch)
    original = item.git

    def fail(repo, *args):
        if args[:2] == ('worktree', 'add'):
            raise OSError('Git unavailable before creation')
        return original(repo, *args)

    monkeypatch.setattr(item, 'git', fail)
    with pytest.raises(ValueError, match='reservation retained'):
        item.control(r.guard, r.identity, payload)
    monkeypatch.setattr(item, 'git', original)
    assert not Path(payload['worktree']).exists()
    resumed = item.control(r.guard, r.identity, dict(payload, operation='item_prepare_resume'))
    assert resumed['phase'] == 'prepared'
    assert resumed['worktree'] == payload['worktree']
    assert not r.guard.get(r.task['task'])


@pytest.mark.asyncio
async def test_structured_status_reads_lost_claim_without_shell_or_new_dispatch(route, monkeypatch):
    r = route
    _, payload = await setup(r, monkeypatch)
    item.control(r.guard, r.identity, payload)
    register(r)
    with r.guard.transaction() as rows:
        rows[r.task['task']].update(claim_phase='claimed', worker_pid=1234,
                                   worker_identity=dict(pid=1234, created=42))
    request = dict(command='SYSTEM168_CONTINUATION_CONTROL',
                   continuation_control=dict(operation='item_status', batch=payload['batch'], issue=payload['issue']))
    token = dc.EVENT_CONTEXT.set(dict(identity=r.identity))
    try:
        before = r.guard.path.read_bytes()
        result = json.loads(dc.terminal_dispatch(request, lambda _: pytest.fail('shell must not run')))
        assert result['execution']['claimed'] is True
        assert result['execution']['worker_identity'] == dict(pid=1234, created=42)
        row = r.guard.get(r.task['task'])
        assert result['execution']['action'] == row['action']
        assert result['execution']['identity'] == row['identity']
        assert result['execution']['row_sha256'] == dc.object_hash(row)
        assert 'worker_route' not in result['execution']
        request['continuation_control']['include_worker_route'] = True
        with_route = json.loads(dc.terminal_dispatch(request, lambda _: pytest.fail('shell must not run')))
        assert with_route['execution']['worker_route'] == row['worker_route']
        assert with_route['execution']['row_sha256'] == result['execution']['row_sha256']
        from tools.terminal_tool import TERMINAL_SCHEMA
        item_schema = TERMINAL_SCHEMA['parameters']['properties']['continuation_control']['anyOf'][1]
        assert item_schema['properties']['include_worker_route']['type'] == 'boolean'
        request['continuation_control']['include_worker_route'] = 'true'
        with pytest.raises(ValueError, match='must be boolean'):
            dc.terminal_dispatch(request, lambda _: pytest.fail('shell must not run'))
        assert r.guard.path.read_bytes() == before
    finally:
        dc.EVENT_CONTEXT.reset(token)
    with pytest.raises(ValueError, match='native Main context'):
        dc.terminal_dispatch(request, lambda _: pytest.fail('shell must not run'))


@pytest.mark.asyncio
@pytest.mark.parametrize('change', [None, 'exact_argv', 'prompt', 'model', 'sandbox'])
async def test_technical_recovery_uses_exact_registered_route_readback(route, monkeypatch, change):
    r = route
    contract, payload = await setup(r, monkeypatch)
    item.control(r.guard, r.identity, payload)
    register(r)
    with r.guard.transaction() as rows:
        row = rows[r.task['task']]
        # A prepared prompt file may omit a trailing byte added at registration.
        row['worker_route']['argv'][-1] += 'scope evidence ' * 2100 + '\n'
        r.guard.schedule_recovery(row, r.clock[0], 'failed pre-worker dispatch')
    r.clock[0] += 20
    wake = r.guard.tick(r.identity)
    assert wake and r.guard.begin(r.identity, wake)
    request = dict(command='SYSTEM168_CONTINUATION_CONTROL', continuation_control=dict(
        operation='item_status', batch=payload['batch'], issue=payload['issue'], include_worker_route=True))
    token = dc.EVENT_CONTEXT.set(dict(identity=r.identity))
    try:
        current = json.loads(dc.terminal_dispatch(request, lambda _: pytest.fail('shell must not run')))
        row = r.guard.get(r.task['task'])
        assert current['execution']['worker_route'] == row['worker_route']
        assert len(current['execution']['worker_route']['argv'][-1]) > 30000
        assert current['execution']['worker_route']['argv'][-1].endswith('\n')
        assert current['execution']['row_sha256'] == dc.object_hash(row)
        observation = evidence(contract, 'technical-observation.json', dict(result='no worker sent'))
        report = evidence(contract, 'technical-reconcile.json', dict(
            **{k: row[k] for k in ('task', 'generation', 'identity', 'action', 'action_id', 'effect_id')},
            outcome='no_effect', observations=[observation]))
        fresh = copy.deepcopy(current['execution']['worker_route'])
        packet = json.loads(Path(fresh['packet']).read_text())
        packet_path = Path(fresh['packet']).with_name('technical-recovery-packet.json')
        packet_path.write_text(json.dumps(packet))
        artifact = str(Path(contract['resources']['artifacts']) / 'technical-recovery-result.txt')
        fresh.update(packet=str(packet_path), packet_sha256=dc.digest(packet_path),
                     receipt=str(packet_path.with_name('technical-recovery-receipt.json')))
        fresh['argv'][fresh['argv'].index('--output-last-message') + 1] = artifact
        if change == 'prompt':
            fresh['argv'][-1] = fresh['argv'][-1].removesuffix('\n')
        elif change in {'model', 'sandbox'}:
            option = '--' + change
            fresh['argv'][fresh['argv'].index(option) + 1] = 'different'
        elif change is None:
            # Native technical recovery reuses the sealed argv; Main only
            # supplies fresh packet, receipt and result paths.
            del fresh['argv']
        strategy = dict(kind='technical', reason='canonical lane repaired',
                        hypothesis='fresh exact-route preflight', artifact=artifact, worker_route=fresh)
        monkeypatch.setattr(dc, 'verify_restarted_reservation_exit', lambda _: None)
        monkeypatch.setattr(dc, 'verify_prior_exit', lambda *a, **kw: None)
        if change in {'prompt', 'model', 'sandbox'}:
            with pytest.raises(ValueError, match='changed authorized argv'):
                dc.maintenance_control(dict(operation='recover', wake=wake, evidence=report, strategy=strategy))
            after = r.guard.get(r.task['task'])
            assert after['policy']['recoveries'] == 0 and not after.get('worker_pid')
        else:
            ticket = dc.maintenance_control(dict(operation='recover', wake=wake,
                                                 evidence=report, strategy=strategy))
            after = r.guard.get(r.task['task'])
            assert ticket == dc.worker_ticket(after)
            assert after['policy']['recoveries'] == 1 and not after.get('worker_pid')
            assert after['worker_route']['argv'][-1] == row['worker_route']['argv'][-1]
            assert after['worker_route']['argv'][after['worker_route']['argv'].index('--output-last-message') + 1] == artifact
    finally:
        dc.EVENT_CONTEXT.reset(token)


@pytest.mark.asyncio
async def test_historical_provider_recovery_requires_bound_no_execution_and_preserves_old_route(route, monkeypatch):
    from tools.process_registry import process_registry

    r = route
    contract, payload = await setup(r, monkeypatch)
    item.control(r.guard, r.identity, payload)
    register(r)
    with r.guard.transaction() as rows:
        row = rows[r.task['task']]
        model = row['logical_contract']['model']
        model.update(required_model='gpt-6-sol', provider='openai-codex', reasoning_effort='high')
        rows.grants['batches'][payload['batch']]['manifest']['todos'][0]['contract']['model'] = copy.deepcopy(model)
        old_route = row['worker_route']
        argv = old_route['argv']
        argv[argv.index('--model') + 1] = 'gpt-6-sol'
        argv[argv.index('model_provider="openai"')] = 'model_provider="openai-codex"'
        argv[argv.index('model_reasoning_effort="ultra"')] = 'model_reasoning_effort="high"'
        old_packet = json.loads(Path(old_route['packet']).read_text())
        old_packet['coding_route'].update(required_model='gpt-6-sol', provider='openai-codex',
                                          reasoning_effort='high')
        Path(old_route['packet']).write_text(json.dumps(old_packet))
        old_route['packet_sha256'] = dc.digest(old_route['packet'])
        row.update(process_id='fixture-launcher', process_started_at=900, launcher_pid=54321,
                   launcher_identity=dict(process_id='fixture-launcher', session_key=r.identity['session_key'],
                                          started_at=900, pid=54321, exited=True))
        row['policy']['work_seconds'] = 37
        rows.grants['batches'][payload['batch']]['item']['work_seconds'] = 37
        r.guard.schedule_recovery(row, r.clock[0], 'historical CLI provider refused before send')
    old = r.guard.get(r.task['task'])
    old_packet_bytes = Path(old['worker_route']['packet']).read_bytes()
    old_packet_sha = old['worker_route']['packet_sha256']
    with r.guard.transaction(read_only=True) as rows:
        old_manifest = copy.deepcopy(rows.grants['batches'][payload['batch']]['manifest'])
    with pytest.raises(ValueError, match='provider|config'):
        dc.validate_codex_argv(old)
    r.clock[0] += 20
    wake = r.guard.tick(r.identity)
    assert wake and r.guard.begin(r.identity, wake)
    row = r.guard.get(r.task['task'])
    observation = evidence(contract, 'provider-observation.json', dict(result='fixture launcher exited before send'))
    proof = evidence(contract, 'provider-effect.json', dict(task=row['task'], action_id=row['action_id'],
        effect_id=row['effect_id'], result='no_effect', execution='not_started'))
    report = evidence(contract, 'provider-report.json', dict(
        **{key: row[key] for key in ('task', 'generation', 'identity', 'action', 'action_id', 'effect_id')},
        outcome='no_effect', worker_result='missing', observations=[observation], effect_proof=proof))
    fresh = copy.deepcopy(row['worker_route'])
    packet_path = Path(fresh['packet']).with_name('provider-fresh-packet.json')
    packet_path.write_bytes(old_packet_bytes)
    fresh.update(packet=str(packet_path), packet_sha256=dc.digest(packet_path),
                 receipt=str(packet_path.with_name('provider-fresh-receipt.json')))
    artifact = str(Path(contract['resources']['artifacts']) / 'provider-fresh-result.txt')
    fresh.pop('argv')  # native technical derivation owns the one-byte-category correction
    strategy = dict(kind='technical', reason='historical CLI provider correction',
                    hypothesis='fresh canonical provider route', artifact=artifact, worker_route=fresh)
    session = SimpleNamespace(id='fixture-launcher', session_key=r.identity['session_key'],
        started_at=900, pid=54321, pid_scope='host', exited=True, exit_code=0,
        completion_reason='exited')
    monkeypatch.setattr(process_registry, 'get', lambda key: session if key == 'fixture-launcher' else None)
    monkeypatch.setattr(process_registry, '_reconcile_local_exit', lambda observed: None)
    monkeypatch.setattr(dc, 'launcher_identity', lambda observed: dict(row['launcher_identity']))
    live = [False]
    monkeypatch.setattr(dc, 'pid_is_absent', lambda pid: pid == 54321 and not live[0])
    token = dc.EVENT_CONTEXT.set(dict(identity=r.identity))
    try:
        before = r.guard.path.read_bytes()
        live[0] = True
        with pytest.raises(ValueError, match='launcher|exit'):
            dc.maintenance_control(dict(operation='recover', wake=wake, evidence=report, strategy=strategy))
        assert r.guard.path.read_bytes() == before
        live[0] = False
        with pytest.raises(ValueError, match='stale'):
            dc.maintenance_control(dict(operation='recover', wake=dict(wake, generation=wake['generation'] - 1),
                                        evidence=report, strategy=strategy))

        def rejected(candidate_report=report, candidate_strategy=strategy):
            before_attempt = r.guard.path.read_bytes()
            with pytest.raises(ValueError):
                dc.maintenance_control(dict(operation='recover', wake=wake,
                                            evidence=candidate_report, strategy=candidate_strategy))
            assert r.guard.path.read_bytes() == before_attempt
            assert not Path(artifact + '.reservation').exists()

        unknown = evidence(contract, 'provider-unknown.json', dict(
            **{key: row[key] for key in ('task', 'generation', 'identity', 'action', 'action_id', 'effect_id')},
            outcome='unknown', worker_result='missing', observations=[observation]))
        rejected(candidate_report=unknown)
        unbound = evidence(contract, 'provider-unbound-effect.json', dict(task=row['task'],
            action_id='different', effect_id=row['effect_id'], result='no_effect', execution='not_started'))
        bad_report = evidence(contract, 'provider-unbound-report.json', dict(
            **{key: row[key] for key in ('task', 'generation', 'identity', 'action', 'effect_id')},
            action_id=row['action_id'], outcome='no_effect', worker_result='missing',
            observations=[observation], effect_proof=unbound))
        rejected(candidate_report=bad_report)
        no_proof = evidence(contract, 'provider-no-proof.json', dict(
            **{key: row[key] for key in ('task', 'generation', 'identity', 'action', 'action_id', 'effect_id')},
            outcome='no_effect', worker_result='missing', observations=[observation]))
        rejected(candidate_report=no_proof)
        with r.guard.transaction() as rows:
            rows[r.task['task']]['visible_sent'] = True  # aborted paste is uncertain, not no execution
            rows[r.task['task']]['visible_binding']['shell'] = dict(pid=87654, start='fixture', executable='sh')
        with monkeypatch.context() as patch:
            patch.setattr(dc, 'process_absent', lambda identity, **kwargs: True)
            rejected()
        with r.guard.transaction() as rows:
            rows[r.task['task']].pop('visible_sent')
            rows[r.task['task']]['visible_binding'].pop('shell')
        with r.guard.transaction() as rows:
            rows[r.task['task']]['logical_contract']['model']['provider'] = 'other'
        rejected()
        with r.guard.transaction() as rows:
            rows[r.task['task']]['logical_contract']['model']['provider'] = 'openai-codex'
        with r.guard.transaction() as rows:
            rows[r.task['task']]['policy']['recoveries'] = rows[r.task['task']]['policy']['limits']['recoveries']
        rejected()
        with r.guard.transaction() as rows:
            rows[r.task['task']]['policy']['recoveries'] = old['policy']['recoveries']

        canonical_argv = [artifact if word == row['artifact'] else word for word in row['worker_route']['argv']]
        canonical_argv[canonical_argv.index('model_provider="openai-codex"')] = 'model_provider="openai"'
        for change in ('model', 'reasoning', 'prompt', 'rights', 'embedded_route',
                       'extra_provider', 'double_provider'):
            candidate = copy.deepcopy(strategy)
            candidate['worker_route']['argv'] = list(canonical_argv)
            argv = candidate['worker_route']['argv']
            if change == 'model':
                argv[argv.index('--model') + 1] = 'gpt-6-astra'
            elif change == 'reasoning':
                argv[argv.index('model_reasoning_effort="high"')] = 'model_reasoning_effort="medium"'
            elif change == 'prompt':
                argv[-1] += ' changed'
            elif change in {'extra_provider', 'double_provider'}:
                argv[-1:-1] = ['-c', 'model_provider="other"' if change == 'extra_provider' else 'model_provider="openai"']
            elif change in {'rights', 'embedded_route'}:
                changed_packet = json.loads(packet_path.read_text())
                changed_path = packet_path.with_name('provider-' + change + '-packet.json')
                candidate['worker_route']['packet'] = str(changed_path)
                if change == 'rights':
                    changed_packet['requested_actions'].append('push')
                else:
                    changed_packet['worker_route'] = {
                        key: value for key, value in candidate['worker_route'].items() if key != 'packet_sha256'}
                changed_path.write_text(json.dumps(changed_packet))
                candidate['worker_route']['packet_sha256'] = dc.digest(changed_path)
            rejected(candidate_strategy=candidate)
        ticket = dc.maintenance_control(dict(operation='recover', wake=wake, evidence=report, strategy=strategy))
        with pytest.raises(ValueError, match='stale'):
            dc.maintenance_control(dict(operation='recover', wake=wake, evidence=report, strategy=strategy))
    finally:
        dc.EVENT_CONTEXT.reset(token)
    after = r.guard.get(r.task['task'])
    assert ticket == dc.worker_ticket(after)
    assert after['history'][0]['worker_route'] == old['worker_route']
    assert Path(old['worker_route']['packet']).read_bytes() == old_packet_bytes
    assert dc.digest(old['worker_route']['packet']) == old_packet_sha
    assert after['policy']['recoveries'] == old['policy']['recoveries'] + 1
    assert after['policy']['work_seconds'] == old['policy']['work_seconds']
    assert after['logical_contract'] == old['logical_contract']
    assert {key: after[key] for key in ('authorization', 'owner_batch', 'owner_issue', 'scope', 'identity')} == {
        key: old[key] for key in ('authorization', 'owner_batch', 'owner_issue', 'scope', 'identity')}
    with r.guard.transaction(read_only=True) as rows:
        assert rows.grants['batches'][payload['batch']]['manifest'] == old_manifest
        assert rows.grants['batches'][payload['batch']]['item']['work_seconds'] == 37
    assert after['worker_route']['argv'][-1] == old['worker_route']['argv'][-1]
    assert after['worker_route']['argv'].count('model_provider="openai"') == 1
    dc.validate_codex_argv(after)
    assert Path(artifact + '.reservation').is_file()
    assert not Path(artifact).exists()


async def status_only_reconciliation_fixture(r, monkeypatch):
    contract, payload = await setup(r, monkeypatch)
    item.control(r.guard, r.identity, payload)
    register(r)
    with r.guard.transaction() as rows:
        row = rows[r.task['task']]
        row.update(generation=3, status='blocked', gate='reconcile', recovery_kind='technical',
                   reason='coordinator answered without lifecycle transition',
                   action='read-only reconcile prior effects and process exit; prepare fresh bounded strategy',
                   checkpoint=dict(generation=2, gate='reconcile'),
                   coordinator_turns={'2': dict(status='response_produced')},
                   process_id='fixture-exited-launcher')
    current = item.status(r.guard, r.identity, payload['batch'], payload['issue'])
    execution = current['execution']
    observation = evidence(contract, 'status-only-observation.json', dict(
        result='dispatcher exited before send', worker_started=False))
    report = {k: execution[k] for k in ('generation', 'identity', 'action', 'action_id', 'effect_id', 'row_sha256')}
    report.update(task=current['task'], worker_result='missing',
                  reason='canonical Coding lane absent before send', observations=[observation])
    return contract, payload, report


@pytest.mark.asyncio
async def test_status_only_failed_reconciliation_requires_exit_and_keeps_worker_absent(route, monkeypatch):
    r = route
    contract, payload, report = await status_only_reconciliation_fixture(r, monkeypatch)
    ref = evidence(contract, 'status-only-reconciliation.json', report)
    from hermes_cli import diggr_successor as successor
    verified = []
    monkeypatch.setattr(successor, 'verify_exit', lambda row: verified.append(row['process_id']))
    token = dc.EVENT_CONTEXT.set(dict(identity=r.identity))
    try:
        result = r.guard.reconcile_failed(r.identity,
            dict(task=r.task['task'], generation=3), ref, now=r.clock[0])
    finally:
        dc.EVENT_CONTEXT.reset(token)
    assert verified == ['fixture-exited-launcher']
    assert result['worker_started'] is False and result['generation'] == 4
    row = r.guard.get(r.task['task'])
    assert row['status'] == 'pending' and row['gate'] == 'reconcile'
    assert row.get('worker_pid') is None and row['policy']['recoveries'] == 0
    assert row['failed_reconciliations'][0]['failure_evidence'] is None


@pytest.mark.asyncio
async def test_status_only_after_seal_before_send_reconciles_exact_receipt(route, monkeypatch):
    r = route
    contract, payload, report = await status_only_reconciliation_fixture(r, monkeypatch)
    row = r.guard.get(r.task['task'])
    receipt = Path(row['worker_route']['receipt'])
    receipt.write_text(json.dumps(dict(verdict='allowed', task=row['task'])))
    with r.guard.transaction() as rows:
        rows[row['task']]['receipt_sha256'] = dc.digest(receipt)
    report['row_sha256'] = dc.object_hash(r.guard.get(row['task']))
    ref = evidence(contract, 'sealed-status-reconciliation.json', report)
    from hermes_cli import diggr_successor as successor
    monkeypatch.setattr(successor, 'verify_exit', lambda _: None)
    token = dc.EVENT_CONTEXT.set(dict(identity=r.identity))
    try:
        result = r.guard.reconcile_failed(r.identity,
            dict(task=row['task'], generation=3), ref, now=r.clock[0])
    finally:
        dc.EVENT_CONTEXT.reset(token)
    assert result['worker_started'] is False
    assert r.guard.get(row['task'])['gate'] == 'reconcile'


@pytest.mark.asyncio
async def test_status_only_after_seal_refuses_changed_receipt(route, monkeypatch):
    r = route
    contract, payload, report = await status_only_reconciliation_fixture(r, monkeypatch)
    row = r.guard.get(r.task['task'])
    receipt = Path(row['worker_route']['receipt'])
    receipt.write_text('original')
    with r.guard.transaction() as rows:
        rows[row['task']]['receipt_sha256'] = dc.digest(receipt)
    receipt.write_text('changed')
    report['row_sha256'] = dc.object_hash(r.guard.get(row['task']))
    ref = evidence(contract, 'tampered-status-reconciliation.json', report)
    token = dc.EVENT_CONTEXT.set(dict(identity=r.identity))
    try:
        with pytest.raises(ValueError, match='route receipt'):
            r.guard.reconcile_failed(r.identity,
                dict(task=row['task'], generation=3), ref, now=r.clock[0])
    finally:
        dc.EVENT_CONTEXT.reset(token)


@pytest.mark.asyncio
async def test_existing_hashed_failure_reconciliation_remains_valid(route, monkeypatch):
    r = route
    contract, payload, report = await status_only_reconciliation_fixture(r, monkeypatch)
    previous = dict(report, generation=2)
    old_ref = evidence(contract, 'original-failure.json', previous)
    with r.guard.transaction() as rows:
        rows[r.task['task']]['failure_evidence'] = old_ref
    report['row_sha256'] = dc.object_hash(r.guard.get(r.task['task']))
    ref = evidence(contract, 'original-failure-reconciliation.json', report)
    from hermes_cli import diggr_successor as successor
    monkeypatch.setattr(successor, 'verify_exit', lambda _row: None)
    token = dc.EVENT_CONTEXT.set(dict(identity=r.identity))
    try:
        r.guard.reconcile_failed(r.identity,
            dict(task=r.task['task'], generation=3), ref, now=r.clock[0])
    finally:
        dc.EVENT_CONTEXT.reset(token)
    assert r.guard.get(r.task['task'])['failed_reconciliations'][0]['failure_evidence'] == old_ref


@pytest.mark.asyncio
@pytest.mark.parametrize('change', ['wrong_action', 'wrong_identity', 'wrong_effect',
                                    'visible_sent', 'route_receipt', 'unknown_exit'])
async def test_status_only_reconciliation_rejects_unbound_or_uncertain_effect(route, monkeypatch, change):
    r = route
    contract, payload, report = await status_only_reconciliation_fixture(r, monkeypatch)
    from hermes_cli import diggr_successor as successor
    if change == 'wrong_action':
        report['action'] = 'original Coding action'
    elif change == 'wrong_identity':
        report['identity'] = dict(report['identity'], session='foreign-session')
    elif change == 'wrong_effect':
        report['effect_id'] = '0' * 32
    elif change == 'visible_sent':
        with r.guard.transaction() as rows:
            rows[r.task['task']]['visible_sent'] = True
    elif change == 'route_receipt':
        Path(r.guard.get(r.task['task'])['worker_route']['receipt']).write_text('unexpected receipt')
    elif change == 'unknown_exit':
        def uncertain(_row):
            raise ValueError('actual launcher exit unavailable')
        monkeypatch.setattr(successor, 'verify_exit', uncertain)
    if change != 'unknown_exit':
        monkeypatch.setattr(successor, 'verify_exit', lambda _row: None)
    ref = evidence(contract, 'status-only-rejected.json', report)
    before = r.guard.path.read_bytes()
    token = dc.EVENT_CONTEXT.set(dict(identity=r.identity))
    try:
        with pytest.raises(ValueError):
            r.guard.reconcile_failed(r.identity,
                dict(task=r.task['task'], generation=3), ref, now=r.clock[0])
    finally:
        dc.EVENT_CONTEXT.reset(token)
    assert r.guard.path.read_bytes() == before


@pytest.mark.asyncio
async def test_lost_correction_response_does_not_schedule_second_recovery(route, monkeypatch):
    r = route
    contract, payload, request = await completed_candidate(r, monkeypatch)
    candidate = item.control(r.guard, r.identity, request)['candidate']
    ref = evidence(contract, 'failure.json', dict(candidate_id=candidate['id'], result='failed',
                                                 findings=['album track moved out of group']))
    request = dict(payload, operation='item_return', evidence=ref)
    first = item.control(r.guard, r.identity, request)
    before = r.guard.path.read_bytes()
    assert item.control(r.guard, r.identity, request) == first
    assert r.guard.path.read_bytes() == before
    row = r.guard.get(r.task['task'])
    assert row['status'] == 'pending' and row['gate'] == 'reconcile'
    assert row['policy']['corrections'] == 0  # only actual recover consumes a correction


@pytest.mark.asyncio
async def test_interrupted_build_readback_prevents_overlapping_correction(route, monkeypatch):
    r = route
    contract, payload, request = await completed_candidate(r, monkeypatch)
    child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(3)'], start_new_session=True)
    try:
        with r.guard.transaction() as rows:
            current = rows.grants['batches'][payload['batch']]['item']
            current.update(phase='candidate_build_unknown', candidate_build=dict(pgid=child.pid))
        readback = dict(payload, operation='item_build_readback')
        assert item.control(r.guard, r.identity, readback)['may_rebuild'] is False
        bad = evidence(contract, 'bad-build-return.json', dict(candidate_id=None, result='failed', findings=['build hung']))
        with pytest.raises(ValueError, match='completed candidate'):
            item.control(r.guard, r.identity, dict(payload, operation='item_return', evidence=bad))
    finally:
        if child.poll() is None:
            try:
                child.terminate()
            except RuntimeError:
                pass  # bounded child exits by itself under the sandbox test guard
        child.wait(timeout=5)
    assert item.control(r.guard, r.identity, readback)['may_rebuild'] is True
    assert item.control(r.guard, r.identity, request)['candidate']['id']


@pytest.mark.asyncio
async def test_interrupted_build_leader_exit_keeps_live_descendant_group_reserved(route, monkeypatch):
    r = route
    contract, payload, request = await completed_candidate(r, monkeypatch)
    leader = subprocess.Popen([sys.executable, '-c',
        'import subprocess,sys; child=subprocess.Popen([sys.executable,"-c","import time;time.sleep(2)"]); '
        'print(child.pid,flush=True)'], start_new_session=True, stdout=subprocess.PIPE, text=True)
    pgid = leader.pid
    try:
        descendant_pid = int(leader.stdout.readline().strip())
        assert descendant_pid > 0
        assert leader.wait(timeout=5) == 0
        with r.guard.transaction() as rows:
            current = rows.grants['batches'][payload['batch']]['item']
            current.update(phase='candidate_build_unknown', candidate_build=dict(pgid=pgid))
        readback = dict(payload, operation='item_build_readback')
        assert item.build_group_active(pgid)
        assert item.control(r.guard, r.identity, readback)['may_rebuild'] is False
    finally:
        if leader.poll() is None:
            leader.wait(timeout=5)
        until = time.monotonic() + 5
        while item.build_group_active(pgid) and time.monotonic() < until:
            time.sleep(.05)  # the self-terminating descendant is never signalled
    assert not item.build_group_active(pgid)
    assert item.control(r.guard, r.identity, readback)['may_rebuild'] is True
    assert item.control(r.guard, r.identity, request)['candidate']['id']


@pytest.mark.asyncio
async def test_build_readback_without_recorded_process_identity_stays_unknown(route, monkeypatch):
    r = route
    _, payload, _ = await completed_candidate(r, monkeypatch)
    with r.guard.transaction() as rows:
        current = rows.grants['batches'][payload['batch']]['item']
        current.update(phase='building_candidate', candidate_build=dict(state='launching',
            argv=[sys.executable, '-c', 'pass'], cwd=payload['worktree']))
    result = item.control(r.guard, r.identity, dict(payload, operation='item_build_readback'))
    assert result['phase'] == 'candidate_build_unknown'
    assert result['may_rebuild'] is False
    assert 'process identity' in result['reason']


@pytest.mark.asyncio
async def test_candidate_build_timeout_with_uncertain_descendant_stays_fenced(route, monkeypatch):
    r = route
    _, payload, request = await completed_candidate(r, monkeypatch)
    class TimedOutBuild:
        pid = 7777
        returncode = None
        calls = 0
        def communicate(self, timeout):
            self.calls += 1
            if self.calls == 1:
                raise subprocess.TimeoutExpired(request['build_argv'], timeout)
            return None
    original_popen = subprocess.Popen
    def build_popen(argv, *args, **kwargs):
        if argv == request['build_argv']:
            return TimedOutBuild()
        return original_popen(argv, *args, **kwargs)
    monkeypatch.setattr(item.subprocess, 'Popen', build_popen)
    monkeypatch.setattr(item.os, 'killpg', lambda pgid, sig: None)
    monkeypatch.setattr(item, 'build_group_active', lambda pgid: True)
    with pytest.raises(ValueError, match='candidate build or provenance failed'):
        item.control(r.guard, r.identity, request)
    result = item.control(r.guard, r.identity, dict(payload, operation='item_build_readback'))
    assert result['phase'] == 'candidate_build_unknown'
    assert result['may_rebuild'] is False
    assert result['build']['pgid'] == 7777
    assert result['build']['error_type'] == 'TimeoutExpired'


@pytest.mark.asyncio
async def test_v2_preflight_survives_time_spent_in_external_verification(route, monkeypatch):
    r = route
    _, payload = await setup(r, monkeypatch)
    item.control(r.guard, r.identity, payload)
    row = register(r)
    original = dc.route_check
    def checked(*args, **kwargs):
        original(*args, **kwargs)
        r.clock[0] += .25  # Actual validation advances the rolling coordination lease.
    monkeypatch.setattr(dc, 'route_check', checked)
    assert dc.main(['--home', r.identity['home'], 'route-preflight', '--payload-json',
                    json.dumps(dict(ticket=dc.worker_ticket(row), route=row['worker_route']))]) == 0
    current = r.guard.get(row['task'])
    assert current['receipt_sha256'] == dc.digest(row['worker_route']['receipt'])
    assert current['generation'] == row['generation'] and not current.get('worker_pid')


@pytest.mark.asyncio
@pytest.mark.parametrize("pre_candidate", [False, True])
async def test_native_v2_review_recovery_preserves_changes_and_passes_real_route_preflight(route, monkeypatch, pre_candidate):
    r = route
    cmux_attempts = _guard_real_cmux_subprocesses(monkeypatch)
    contract, payload, request = await completed_candidate(r, monkeypatch, observed_v2=True)
    source = Path(payload['worktree']) / 'src' / 'code.txt'
    source.write_text('implementation to retain\n')
    git(payload['worktree'], 'add', 'src/code.txt')
    if pre_candidate:
        row = r.guard.get(r.task['task'])
        failure = evidence(contract, 'failed-result.json', dict(task=row['task'],
            worker_generation=row['worker_result']['generation'], action_id=row['action_id'],
            effect_id=row['effect_id'], result_sha256=row['worker_result']['evidence']['sha256'],
            result='failed', findings=['needs narrow correction before candidate build']))
    else:
        candidate = item.control(r.guard, r.identity, request)['candidate']
        failure = evidence(contract, 'failed-test.json', dict(candidate_id=candidate['id'], result='failed',
                                                           findings=['needs narrow correction']))
    item.control(r.guard, r.identity, dict(payload, operation='item_return', evidence=failure))
    r.clock[0] += 20
    wake = r.guard.tick(r.identity)
    assert wake and r.guard.begin(r.identity, wake)
    row = r.guard.get(r.task['task'])
    effect = evidence(contract, 'effect.json', dict(task=row['task'], action_id=row['action_id'],
        effect_id=row['effect_id'], result='reconciled', retained_source_sha256=dc._worktree_snapshot(payload['worktree'])))
    report = evidence(contract, 'reconcile.json', dict(
        **{k: row[k] for k in ('task', 'generation', 'identity', 'action', 'action_id', 'effect_id')},
        outcome='reconciled', observations=[failure], effect_proof=effect))
    fresh = copy.deepcopy(row['worker_route'])
    packet = json.loads(Path(fresh['packet']).read_text())
    packet.update(base=git(payload['worktree'], 'rev-parse', 'HEAD'),
                  retained_worktree_sha256=dc._worktree_snapshot(payload['worktree']))
    packet_path = Path(fresh['packet']).with_name('correction.json')
    packet_path.write_text(json.dumps(packet))
    fresh.update(packet=str(packet_path), packet_sha256=dc.digest(packet_path),
                 receipt=str(Path(contract['resources']['artifacts']) / 'correction-receipt.json'))
    artifact = str(Path(contract['resources']['artifacts']) / 'correction-result.txt')
    fresh['argv'][fresh['argv'].index('--output-last-message') + 1] = artifact
    from tests.diggr_owner_fixtures import assert_child_onboarding
    correction_content, correction_dir = item.onboarding_payload()
    previous_prompt = correction_content + '\n\nCorrect only the album grouping finding; retain previous work'
    fresh['argv'][-1] = 'Executor instructions\n' + previous_prompt
    assert fresh['argv'][-1] == 'Executor instructions\n' + previous_prompt
    assert correction_content in fresh['argv'][-1]
    assert not fresh['argv'][-1].startswith('-')
    assert_child_onboarding(r.tmp, correction_content, correction_dir)
    strategy = dict(kind='review', reason='live test finding', hypothesis='preserve album grouping',
                    artifact=artifact, worker_route=fresh)
    token = dc.EVENT_CONTEXT.set(dict(identity=r.identity))
    try:
        ticket = dc.maintenance_control(dict(operation='recover', wake=wake, evidence=report, strategy=strategy))
    finally:
        dc.EVENT_CONTEXT.reset(token)
    after = r.guard.get(r.task['task'])
    assert ticket == dc.worker_ticket(after)
    assert after['policy']['corrections'] == 1
    assert source.read_text() == 'implementation to retain\n'
    assert git(payload['worktree'], 'show', ':src/code.txt') == 'implementation to retain'
    assert dc._worktree_snapshot(payload['worktree']) == packet['retained_worktree_sha256']
    assert not after.get('worker_pid') and not after.get('visible_sent')
    assert item.status(r.guard, r.identity, payload['batch'], payload['issue'])['phase'] == 'coding'
    # Complete the recovered ticket through native seal, send, claim and
    # supervision. Terminal I/O, visible claim verification and Codex argv
    # execution remain isolated external seams; no live TTY/cMux proof.
    route = after['worker_route']
    content, directory = item.onboarding_payload()
    assert directory == str((r.tmp / 'skills' / item.ONBOARDING_SKILL).resolve())
    assert content in route['argv'][-1]
    assert dc.main(['--home', r.identity['home'], 'route-preflight', '--payload-json',
                    json.dumps(dict(ticket=ticket, route=route))]) == 0
    sealed = r.guard.get(after['task'])
    assert sealed['receipt_sha256'] == dc.digest(route['receipt'])
    sent = []
    monkeypatch.setattr(dc, 'cmux_send', lambda binding, command: sent.append((binding, command)))
    response = json.loads(dc.terminal_dispatch(
        dict(command='SYSTEM167_REGISTERED_WORKER', continuation_ticket=ticket),
        lambda _: pytest.fail('launcher must not start a second worker')))
    assert response['sent'] is True
    assert sent == [(after['visible_binding'], dc.visible_worker_command(ticket))]
    monkeypatch.setattr(dc, 'verify_visible_claim', lambda row: None)
    result, spawned = _run_sealed_fixture_worker(
        r.guard, ticket, route['argv'], artifact)
    assert result == 0
    claimed = r.guard.get(after['task'])
    assert claimed['claim_phase'] == 'claimed' and claimed['worker_pid']
    assert claimed['worker_result']['generation'] == ticket['generation'] and len(spawned) == 1
    expected_generation = ticket['generation']
    assert claimed['worker_supervision']['state'] == 'exited'
    assert claimed['worker_supervision']['binding'] == dc.worker_supervision_binding(
        claimed, expected_generation)
    assert claimed['worker_result']['evidence']['generation'] == expected_generation
    assert claimed['generation'] == expected_generation + 1
    assert claimed['status'] == 'pending' and claimed['gate'] == 'main_validation'
    assert claimed['worker_result']['exit_code'] == 0 and claimed['child_exited'] is True
    assert claimed['worker_result']['evidence']['sha256'] == dc.digest(artifact)
    assert claimed['worker_result']['action_id'] == after['action_id']
    assert claimed['worker_result']['effect_id'] == after['effect_id']
    assert claimed['child_identity']['pid'] > 0 and dc.worker_operational_exit(claimed)
    assert dc.worker_operational_exit(claimed, expected_generation)
    assert not dc.worker_operational_exit(claimed, claimed['generation'])
    stale = copy.deepcopy(claimed)
    stale['worker_supervision']['binding']['generation'] = expected_generation - 1
    assert not dc.worker_operational_exit(stale, expected_generation)
    assert Path(artifact).read_text() == 'scoped correction result'
    with pytest.raises(ValueError):
        dc.main(['--home', r.identity['home'], 'worker', '--payload-json', json.dumps(ticket)])
    assert len(spawned) == 1
    assert cmux_attempts == [], 'parent/recovery attempted real cmux: ' + repr(cmux_attempts)


@pytest.mark.asyncio
@pytest.mark.parametrize('exit_code', [0, 1])
async def test_observed_missing_result_native_recovery_route(route, monkeypatch, exit_code):
    """A held, sealed child with no output must retain a usable native recovery route."""
    r = route
    cmux_attempts = _guard_real_cmux_subprocesses(monkeypatch)
    contract, payload = await setup(r, monkeypatch)
    executable = _bind_disposable_codex_executable(
        r, monkeypatch, omit_output=True, exit_code=exit_code)
    r.task['worker_route']['argv'][0] = str(executable)
    r.task['launcher_command'] = dc.logical_launcher_command(contract, r.task['worker_route'])
    item.control(r.guard, r.identity, payload)
    original_tree = dc._worktree_snapshot(payload['worktree'])
    observed = _complete_native_v2_item_attempt(
        r, monkeypatch, exit_code=exit_code, output_present=False)
    original_result = copy.deepcopy(observed['worker_result'])
    assert observed['worker_supervision']['state'] == 'exited'
    assert observed['worker_identity']['pid'] > 0 and dc.worker_operational_exit(observed)
    assert original_result['exit_code'] == exit_code
    assert original_result['artifact_status'] == 'missing' and original_result['evidence'] is None
    assert not Path(observed['artifact']).exists()
    assert dc._worktree_snapshot(payload['worktree']) == original_tree
    with r.guard.transaction(read_only=True) as rows:
        spent = rows.grants['batches'][payload['batch']]['item']['work_seconds']
    assert spent == observed['worker_supervision']['accounted_seconds'] and spent > 0
    with pytest.raises(ValueError, match='candidate needs the completed native worker and bound result'):
        item.control(r.guard, r.identity, dict(payload, operation='item_candidate'))

    assert observed['gate'] == 'reconcile' and observed['status'] == 'pending'
    assert observed['recovery_kind'] == 'technical'
    # A pending Main row is not an executing wake. Claim the native wake.
    r.clock[0] += 20
    wake = r.guard.tick(r.identity)
    assert wake and wake['gate'] == 'reconcile' and r.guard.begin(r.identity, wake)
    row = r.guard.get(r.task['task'])
    assert row['gate'] == 'reconcile' and row['status'] == 'executing'
    assert row['recovery_kind'] == 'technical'

    observation = evidence(contract, 'missing-output-observation.json', dict(
        task=row['task'], child_pid=observed['child_identity']['pid'],
        worker_pid=observed['worker_identity']['pid'], exit_code=exit_code,
        artifact=row['artifact'], artifact_absent=not Path(row['artifact']).exists(),
        worktree_sha256=dc._worktree_snapshot(payload['worktree'])))
    failure = evidence(contract, 'missing-output-failure.json', dict(
        **{k: row[k] for k in ('task', 'generation', 'identity', 'action', 'action_id', 'effect_id')},
        worker_result='missing', reason='owned worker exited without result',
        observations=[observation]))
    token = dc.EVENT_CONTEXT.set(dict(identity=r.identity))
    try:
        assert dc.maintenance_control(dict(operation='failure', wake=wake, evidence=failure)) is True
        blocked = r.guard.get(row['task'])
        assert blocked['status'] == 'blocked' and blocked['gate'] == 'reconcile'
        assert blocked['worker_result'] == original_result
        effect = evidence(contract, 'missing-output-effect.json', dict(
            task=row['task'], action_id=row['action_id'], effect_id=row['effect_id'],
            result='no_effect', retained_worktree_sha256=dc._worktree_snapshot(payload['worktree'])))
        diagnosis = evidence(contract, 'missing-output-diagnosis.json', dict(
            **{k: blocked[k] for k in ('task', 'generation', 'identity', 'action', 'action_id', 'effect_id')},
            row_sha256=dc.object_hash(blocked), reason='test child touched no worktree file',
            worker_result='missing', outcome='no_effect', observations=[observation],
            effect_proof=effect))
        assert dc.maintenance_control(dict(operation='reconcile_failed',
                                           wake=dict(task=row['task'], generation=blocked['generation']),
                                           evidence=diagnosis))['gate'] == 'reconcile'
        r.clock[0] += 20
        recovery_wake = r.guard.tick(r.identity)
        assert recovery_wake and r.guard.begin(r.identity, recovery_wake)
        current = r.guard.get(row['task'])
        report = evidence(contract, 'missing-output-recovery.json', dict(
            **{k: current[k] for k in ('task', 'generation', 'identity', 'action', 'action_id', 'effect_id')},
            outcome='no_effect', observations=[observation], effect_proof=effect))
        fresh = copy.deepcopy(current['worker_route'])
        packet_path = Path(fresh['packet']).with_name('missing-output-recovery-packet.json')
        packet_path.write_bytes(Path(fresh['packet']).read_bytes())
        artifact = str(Path(contract['resources']['artifacts']) / 'missing-output-recovery-result.txt')
        fresh.update(packet=str(packet_path), packet_sha256=dc.digest(packet_path),
                     receipt=str(Path(contract['resources']['artifacts']) / 'missing-output-recovery-receipt.json'))
        del fresh['argv']  # The technical route derives the sealed argv itself.
        strategy = dict(kind='technical', reason='observed missing result after actual exit',
                        hypothesis='retry sealed disposable child after verified no effects',
                        artifact=artifact, worker_route=fresh)
        unknown = evidence(contract, 'missing-output-effects-unknown.json', dict(
            **{k: current[k] for k in ('task', 'generation', 'identity', 'action', 'action_id', 'effect_id')},
            outcome='unknown', observations=[observation]))
        with pytest.raises(ValueError, match='expected no_effect or reconciled'):
            dc.maintenance_control(dict(operation='recover', wake=recovery_wake,
                                        evidence=unknown, strategy=strategy))
        assert r.guard.get(row['task']) == current
        ticket = dc.maintenance_control(dict(operation='recover', wake=recovery_wake,
                                             evidence=report, strategy=strategy))
        recovered = r.guard.get(row['task'])
        assert ticket == dc.worker_ticket(recovered)
        assert recovered['task'] == row['task'] and recovered['identity'] == row['identity']
        assert recovered['authorization'] == row['authorization']
        assert recovered['worker_route']['worktree'] == row['worker_route']['worktree']
        assert recovered['policy']['recoveries'] == row['policy']['recoveries'] + 1
        assert recovered['policy']['corrections'] == row['policy']['corrections']
        assert recovered['history'][-1]['worker_result'] == original_result
        assert not recovered.get('worker_pid') and not recovered.get('visible_sent')
        assert not Path(artifact).exists() and Path(artifact + '.reservation').is_file()
        with r.guard.transaction(read_only=True) as rows:
            assert rows.grants['batches'][payload['batch']]['item']['work_seconds'] == spent
        with pytest.raises(ValueError, match='stale recovery generation|stale.*lease|bounds'):
            dc.maintenance_control(dict(operation='recover', wake=recovery_wake,
                                        evidence=report, strategy=strategy))
    finally:
        dc.EVENT_CONTEXT.reset(token)
    assert cmux_attempts == []
