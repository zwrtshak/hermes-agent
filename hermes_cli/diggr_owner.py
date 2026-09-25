"""Narrow native Telegram owner grants; not a host-shell security boundary.

Only the adapter's authenticated direct-message path stamps NativeInput. Neither
JSON nor session environment reconstructs it. All durable changes use Guard's
existing transaction/lock. A proposal is data, never execution authority.
"""
import json
import hashlib
import secrets
import time
import uuid
from dataclasses import dataclass

BUDGETS = dict(todo_seconds=3600, batch_seconds=7200, wakes=10, recoveries=2, corrections=2)
OWNER = '564628210'


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True).replace('`', '\\u0060')


def stable_owner(identity):
    return {k: identity[k] for k in ('home', 'profile', 'platform', 'chat_id', 'user_id', 'thread_id')}


def issue_key(todo):
    return '/'.join(todo[k] for k in ('workspace_id', 'project_id', 'issue_id'))


def contract(task):
    return {k: task.get(k) for k in ('scope', 'action', 'owner', 'gate', 'producer', 'worker_route', 'launcher_command')}


CODING_ACTIONS = {'inspect', 'edit', 'test', 'review', 'document_work_item', 'commit', 'push', 'pr_update'}
MAIN_ACTIONS = CODING_ACTIONS | {'merge', 'smoke', 'plane_update', 'nextcloud_update'}
LOGICAL_VERSION = 'logical-v1'


def _strict_object(value, keys):
    return isinstance(value, dict) and set(value) == set(keys)


def validate_logical_contract(value):
    """A new explicit grant shape; unversioned exact grants are never upgraded."""
    from pathlib import Path, PurePosixPath
    import re
    if (not _strict_object(value, ('version', 'scope', 'action', 'owner', 'gate', 'producer',
                                  'plane_id', 'resources', 'actions', 'acceptance', 'model')) or
            value['version'] != LOGICAL_VERSION or value['producer'] != 'cmux' or
            value['owner'] != 'coding' or value['gate'] != 'coding' or
            any(not isinstance(value[k], str) or not value[k].strip() for k in ('scope', 'action')) or
            not isinstance(value['plane_id'], str) or
            not re.fullmatch(r'(?:APP|SYSTEM)-[0-9]+', value['plane_id'])):
        raise ValueError('explicit logical-v1 Coding scope required')
    resources = value['resources']
    if not _strict_object(resources, ('repo', 'worktrees', 'artifacts', 'allowed_paths', 'no_touch')):
        raise ValueError('exact logical resource boundaries required')
    for key in ('repo', 'worktrees', 'artifacts'):
        path = resources[key]
        if not isinstance(path, str) or not Path(path).is_absolute() or str(Path(path).resolve()) != path:
            raise ValueError('canonical logical resource roots required')
    for key in ('allowed_paths', 'no_touch'):
        paths = resources[key]
        if (not isinstance(paths, list) or (key == 'allowed_paths' and not paths) or
                any(not isinstance(path, str) for path in paths) or len(set(paths)) != len(paths)):
            raise ValueError('explicit unique relative scope paths required')
        for path in paths:
            if (not isinstance(path, str) or not path or PurePosixPath(path).is_absolute() or
                    str(PurePosixPath(path)) != path or '..' in PurePosixPath(path).parts or
                    any(c in path for c in ('*', '?', '[', '\\', '\x00'))):
                raise ValueError('literal relative scope paths required, no globs or escape')
    actions = value['actions']
    if not _strict_object(actions, ('coding', 'main')):
        raise ValueError('separate Coding/Main rights required')
    for role, allowed in (('coding', CODING_ACTIONS), ('main', MAIN_ACTIONS)):
        rights = actions[role]
        if (not isinstance(rights, list) or not rights or
                any(not isinstance(a, str) or a not in allowed for a in rights) or len(set(rights)) != len(rights)):
            raise ValueError('explicit supported role actions required')
    acceptance = value['acceptance']
    if (not _strict_object(acceptance, ('owner', 'criteria', 'main_live_required')) or
            acceptance['owner'] not in {'main', 'user'} or type(acceptance['main_live_required']) is not bool or
            not isinstance(acceptance['criteria'], list) or not acceptance['criteria'] or
            any(not isinstance(c, str) or not c.strip() for c in acceptance['criteria'])):
        raise ValueError('explicit acceptance ownership and criteria required')
    model = value['model']
    if (not _strict_object(model, ('executor', 'required_model', 'provider', 'fallback_models_allowed',
                                  'reasoning_effort', 'sandbox', 'approval_policy')) or
            model['executor'] != 'codex' or model['required_model'] != 'gpt-6-astra' or
            model['fallback_models_allowed'] != [] or model['sandbox'] != 'workspace-write' or
            model['approval_policy'] != 'never' or
            not isinstance(model['provider'], str) or not re.fullmatch(r'[a-zA-Z0-9_-]+', model['provider']) or
            model['reasoning_effort'] not in {'low', 'medium', 'high', 'xhigh', 'max', 'ultra'}):
        raise ValueError('explicit supported Astra/provider contract required')


@dataclass(frozen=True)
class LogicalBinding:
    """Internal validation result. JSON/task arguments cannot recreate this type."""
    contract_sha256: str
    task_sha256: str


def binding_hash(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


@dataclass(frozen=True)
class NativeInput:
    text: str
    chat: str
    user: str
    message: str
    update: int
    thread: str = ''


def stamp_telegram(event, message, update):
    """Called only after Telegram adapter allowlist checks, before dispatch."""
    user = getattr(message, 'from_user', None)
    if (not user or getattr(user, 'is_bot', True) or message.chat.type != 'private' or
            any(getattr(message, k, None) for k in ('forward_origin', 'forward_from', 'forward_from_chat',
                'quote', 'reply_to_message', 'external_reply', 'via_bot', 'sender_chat', 'edit_date')) or
            any(getattr(e, 'type', '') in {'blockquote', 'expandable_blockquote', 'code', 'pre'}
                for e in (getattr(message, 'entities', None) or [])) or
            type(update) is not int or update < 0 or not message.text or
            message.text != event.text):
        return
    event._diggr_native_input = NativeInput(message.text, str(message.chat.id), str(user.id),
                                           str(message.message_id), update,
                                           str(getattr(message, 'message_thread_id', None) or ''))


def verify_input(event, identity):
    proof = getattr(event, '_diggr_native_input', None)
    if (type(proof) is not NativeInput or getattr(event, 'internal', False) or
            identity['profile'] not in {'diggr-main', 'mira'} or identity['platform'] != 'telegram' or
            identity['chat_id'] != OWNER or identity['user_id'] != OWNER or proof.thread != identity['thread_id'] or
            proof.text != event.text or proof.chat != identity['chat_id'] or
            proof.user != identity['user_id'] or proof.message != str(event.message_id) or
            proof.update != event.platform_update_id):
        raise ValueError('direct authenticated native owner message required')
    return str(proof.update) + ':' + proof.message


def validate_manifest(manifest):
    if (not isinstance(manifest, dict) or set(manifest) != {'budgets', 'todos', 'replaces'} or
            manifest['budgets'] != BUDGETS or not isinstance(manifest['todos'], list) or
            not 1 <= len(manifest['todos']) <= 4 or len(canonical(manifest)) > 2800):
        raise ValueError('exact bounded owner manifest required (at most four Todos / 2800 bytes)')
    seen = set()
    for todo in manifest['todos']:
        if set(todo) != {'workspace_id', 'project_id', 'issue_id', 'contract', 'depends_on', 'gates'}:
            raise ValueError('exact Todo contract required')
        for key in ('workspace_id', 'project_id', 'issue_id'):
            if str(uuid.UUID(todo[key])) != todo[key]:
                raise ValueError('stable canonical Plane UUIDs required')
        key = issue_key(todo)
        if key in seen or todo['depends_on'] != sorted(seen):
            raise ValueError('unique issues in explicit sequential dependency order required')
        if todo['gates'] != ['owner_confirmed', 'native_checks', 'prior_done']:
            raise ValueError('owner, native and prior completion gates required')
        scope = todo['contract']
        if isinstance(scope, dict) and scope.get('version') == LOGICAL_VERSION:
            validate_logical_contract(scope)
        elif (not isinstance(scope, dict) or
                set(scope) != {'scope', 'action', 'owner', 'gate', 'producer', 'worker_route', 'launcher_command'} or
                not scope['scope'] or not scope['action']):
            raise ValueError('exact execution scope/action/route required')
        seen.add(key)
    if manifest['replaces'] is not None and not isinstance(manifest['replaces'], str):
        raise ValueError('explicit prior batch reference required for reauthorization')


def propose(guard, identity, manifest):
    validate_manifest(manifest)
    # Roundtrip detaches executor-owned mutable dictionaries.
    manifest = json.loads(canonical(manifest))
    digest = hashlib.sha256(canonical(manifest).encode()).hexdigest()
    with guard.transaction() as rows:
        proposals = rows.grants.setdefault('proposals', {})
        proposal = dict(owner=stable_owner(identity), manifest=manifest)
        if digest in proposals and any(proposals[digest][k] != v for k, v in proposal.items()):
            raise ValueError('proposal owner conflict')
        proposals.setdefault(digest, proposal)
    return digest


async def handle_command(runner, event, guard, identity):
    """Consumed before agent execution; separate explicit show and confirm messages."""
    from hermes_cli.diggr_continuation import object_hash, ownership_pending
    event_id = verify_input(event, identity)
    words = event.text.split()
    if len(words) not in (3, 4) or words[0] != '/continuation' or words[1] not in {'show', 'confirm'}:
        raise ValueError('use /continuation show HASH or /continuation confirm HASH CODE')
    digest = words[2]
    owner = stable_owner(identity)
    now = time.time()
    with guard.transaction() as rows:
        p = rows.grants.get('proposals', {}).get(digest)
        if not p or p['owner'] != owner or hashlib.sha256(canonical(p['manifest']).encode()).hexdigest() != digest:
            raise ValueError('unknown, altered or foreign owner proposal')
        if words[1] == 'show':
            if len(words) != 3:
                raise ValueError('show takes exactly the immutable proposal hash')
            if p.get('batch'):
                response = 'Already confirmed batch ' + p['batch']
                preview = None
            else:
                preview = dict(code=secrets.token_hex(12), event=event_id, expires=now + 300)
                response = ('Owner proposal (no execution yet):\n```json\n' + canonical(p['manifest']) + '\n```' +
                            '\nSHA256 ' + digest + '\nConfirm within 300 seconds with a NEW direct message:\n' +
                            '/continuation confirm ' + digest + ' ' + preview['code'])
        else:
            if len(words) != 4:
                raise ValueError('confirmation requires exact hash and one-use code')
            if p.get('batch'):
                batch = rows.grants['batches'][p['batch']]
                if batch['confirmation_event'] != event_id or batch['confirmation_code'] != words[3]:
                    raise ValueError('already confirmed; explicit new reauthorization proposal required')
                response = 'Already confirmed batch ' + p['batch']
            else:
                preview = p.get('shown')
                if (not preview or preview['code'] != words[3] or now >= preview['expires'] or
                        event_id == preview['event']):
                    raise ValueError('unshown, stale or replayed confirmation')
                used_events = rows.grants.setdefault('confirmation_events', {})
                event_key = object_hash(dict(owner=owner, event=event_id))
                if event_key in used_events:
                    raise ValueError('native confirmation event already consumed')
                batches = rows.grants.setdefault('batches', {})
                replaces = p['manifest']['replaces']
                keys = {issue_key(t) for t in p['manifest']['todos']}
                for bid, old in batches.items():
                    if old['owner'] != owner:
                        continue
                    overlap = keys & {issue_key(t) for t in old['manifest']['todos']}
                    if overlap and bid != replaces and not old.get('replaced_by'):
                        raise ValueError('same logical work requires explicit reauthorization of prior batch')
                if replaces:
                    old = batches.get(replaces)
                    if not old or old['owner'] != owner or old.get('replaced_by'):
                        raise ValueError('invalid reauthorization target')
                    if any(r.get('policy', {}).get('batch') == replaces and ownership_pending(r) for r in rows.values()):
                        raise ValueError('prior batch still owns unreconciled work')
                batch_id = object_hash(dict(owner=owner, proposal=digest, event=event_id))
                batches[batch_id] = dict(owner=owner, manifest=p['manifest'], proposal=digest,
                    confirmation_event=event_id, confirmation_code=words[3], confirmed_at=now,
                    coordinator_identity=json.loads(canonical(identity)),
                    started_at=None, deadline=None, tasks={}, revoked=False, replaces=replaces)
                if replaces:
                    batches[replaces].update(revoked=True, replaced_by=batch_id)
                used_events[event_key] = batch_id
                p['batch'] = batch_id
                response = 'Confirmed batch ' + batch_id + '. No work dispatched.'
            preview = None
    # No guard lock held across native delivery. An unsuccessful preview cannot authorize.
    from gateway.platforms.base import _thread_metadata_for_source
    result = await runner._adapter_for_source(event.source).send(
        event.source.chat_id, response, metadata=_thread_metadata_for_source(event.source, str(event.message_id)))
    if not getattr(result, 'success', False):
        raise ValueError('native owner display delivery failed')
    if words[1] == 'show' and preview:
        with guard.transaction() as rows:
            p = rows.grants['proposals'][digest]
            if not p.get('batch'):
                p['shown'] = preview
    return False  # fully handled, never forwarded to agent/tool execution


class Policy(dict):
    """Runtime-only verified row binding; JSON cannot recreate this type."""


class State(dict):
    def __init__(self, tasks, grants):
        super().__init__(tasks)
        self.grants = grants


def hydrate(rows):
    for name, row in rows.items():
        raw = row.get('policy')
        if not isinstance(raw, dict):
            continue
        batch = rows.grants.get('batches', {}).get(raw.get('batch'))
        if (batch and batch['owner'] == stable_owner(row['identity']) and
                batch['tasks'].get(raw.get('issue')) == name and batch['started_at'] is not None and
                raw.get('batch_deadline') == batch['deadline']):
            row['policy'] = Policy(raw, revoked=batch['revoked'])
        else:
            row['policy'] = dict(raw)  # legacy or forged binding fails closed


def bind(rows, task, now, *, logical_binding=None):
    batch = rows.grants.get('batches', {}).get(task.get('owner_batch'))
    key = task.get('owner_issue')
    if not batch or batch['owner'] != stable_owner(task['identity']) or batch['revoked']:
        raise ValueError('confirmed native owner batch required')
    todo = next((t for t in batch['manifest']['todos'] if issue_key(t) == key), None)
    actual = contract(task)
    # Native registration adds this seal; it is not an executor-selected route field.
    # Preserve exact comparison when an older confirmed proposal already pinned it.
    if (todo and todo['contract'].get('version') != LOGICAL_VERSION and actual['producer'] == 'cmux' and
            isinstance(todo['contract']['worker_route'], dict) and
            isinstance(actual['worker_route'], dict) and
            'packet_sha256' not in todo['contract']['worker_route']):
        actual['worker_route'] = {k: v for k, v in actual['worker_route'].items() if k != 'packet_sha256'}
    if todo and todo['contract'].get('version') == LOGICAL_VERSION:
        if (type(logical_binding) is not LogicalBinding or
                logical_binding.contract_sha256 != binding_hash(todo['contract']) or
                logical_binding.task_sha256 != binding_hash(task)):
            raise ValueError('native validated logical route binding required')
        paths = {task['artifact'], task['worker_route']['packet'], task['worker_route']['receipt']}
        for row in rows.values():
            for attempt in [row] + row.get('history', []) + row.get('attempt_history', []):
                used = {attempt.get('artifact'), attempt.get('worker_route', {}).get('packet'),
                        attempt.get('worker_route', {}).get('receipt')}
                if paths & used:
                    raise ValueError('logical attempt paths already bound; never reuse')
        if (task['deadline'] > now + BUDGETS['todo_seconds'] or
                any(task.get(k) is not None and task[k] > (batch['deadline'] or now + BUDGETS['batch_seconds'])
                    for k in ('hard_stop', 'authorization_expires_at'))):
            raise ValueError('logical request exceeds existing execution bounds')
    elif not todo or todo['contract'] != actual:
        raise ValueError('task differs from owner-confirmed logical Todo contract')
    if key in batch['tasks']:
        raise ValueError('logical Todo already admitted; alias/session cannot reset budget')
    for dep in todo['depends_on']:
        prior = rows.get(batch['tasks'].get(dep))
        if not prior or prior['status'] != 'done':
            raise ValueError('prior authorized Todo has not passed completion gates')
    if batch['started_at'] is None:
        batch.update(started_at=now, deadline=now + BUDGETS['batch_seconds'])
    if now >= batch['deadline']:
        raise ValueError('owner batch expired; new work forbidden')
    deadline = min(now + BUDGETS['todo_seconds'], batch['deadline'],
                   task.get('hard_stop') or float('inf'), task.get('authorization_expires_at') or float('inf'))
    if deadline <= now:
        raise ValueError('owner Todo expired')
    batch['tasks'][key] = task['task']
    return Policy(batch=task['owner_batch'], issue=key, first_started_at=now, deadline=deadline,
                  batch_deadline=batch['deadline'], wakes=0, recoveries=0, corrections=0, revoked=False)


def allowed(row, now):
    p = row.get('policy')
    return (type(p) is Policy and not p['revoked'] and now < p['deadline'] and
            now < p['batch_deadline'])


def stop(rows, identity):
    owner = stable_owner(identity)
    for batch in rows.grants.get('batches', {}).values():
        if batch['owner'] == owner:
            batch['revoked'] = True
    for row in rows.values():
        if row.get('policy') and stable_owner(row['identity']) == owner:
            row['policy']['revoked'] = True
