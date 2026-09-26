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


def prevalidate_resources(manifest):
    """New proposals only. Never tighten a confirmed grant during admission."""
    from pathlib import Path, PurePosixPath
    from hermes_cli.diggr_continuation import logical_packet_root
    for todo in manifest['todos']:
        scope = todo['contract']
        if scope.get('version') != LOGICAL_VERSION:
            continue
        resources = scope['resources']
        roots = {k: Path(resources[k]).resolve() for k in ('repo', 'worktrees', 'artifacts')}
        packet = logical_packet_root()
        artifacts = roots['artifacts']
        if not (artifacts.is_relative_to(packet) or packet.is_relative_to(artifacts)):
            raise ValueError('artifacts and canonical routing packet root need a common possible subtree')
        if any(path == Path(path.anchor) or path == Path.home().resolve() for path in roots.values()):
            raise ValueError('bounded task resource root required')
        if any(path.exists() and not path.is_dir() for path in (*roots.values(), packet)):
            raise ValueError('logical resource roots must permit directories')
        # Every permitted change below a forbidden ancestor is impossible.
        if all(any(PurePosixPath(path).is_relative_to(PurePosixPath(no)) for no in resources['no_touch'])
               for path in resources['allowed_paths']):
            raise ValueError('all allowed paths are excluded by No-Touch')


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
        raise ValueError('direct authenticated native owner message required: send unformatted direct text; '
                         'no code block, quote, reply or forward')
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


def propose(guard, identity, manifest, *, transport=None):
    validate_manifest(manifest)
    # Roundtrip detaches executor-owned mutable dictionaries.
    manifest = json.loads(canonical(manifest))
    digest = hashlib.sha256(canonical(manifest).encode()).hexdigest()
    with guard.transaction() as rows:
        proposals = rows.grants.setdefault('proposals', {})
        proposal = dict(owner=stable_owner(identity), manifest=manifest)
        if digest in proposals and any(proposals[digest][k] != v for k, v in proposal.items()):
            raise ValueError('proposal owner conflict')
        if digest in proposals and transport is not None:
            check_proposal_origin(rows, proposals[digest], identity, transport)
        if digest not in proposals:
            prevalidate_resources(manifest)
            if transport is not None:
                from hermes_cli import diggr_delivery as delivery
                if transport != delivery.native_transport(identity):
                    raise ValueError('native preview transport required')
                if (identity['platform'] != 'telegram' or identity['profile'] not in delivery.PROFILES or
                        identity['chat_id'] != OWNER or identity['user_id'] != OWNER or transport['chat_type'] != 'dm'):
                    raise ValueError('private native owner preview required')
                proposal.update(coordinator_identity=json.loads(canonical(identity)),
                                native_transport=json.loads(canonical(transport)), requested_at=time.time(),
                                preview=new_preview(digest, manifest))
        # Repeating a model call never adopts legacy rows, resets expiry or renews grants.
        proposals.setdefault(digest, proposal)
    return digest


def check_proposal_origin(rows, proposal, identity, transport):
    """A stable owner is not permission to inspect another native session."""
    from hermes_cli.diggr_delivery import same_transport
    if proposal['owner'] != stable_owner(identity):
        raise ValueError('proposal owner conflict')
    batch = rows.grants.get('batches', {}).get(proposal.get('batch'), {})
    shown = proposal.get('shown', {})
    for bound_identity, bound_transport in (
        (proposal.get('coordinator_identity'), proposal.get('native_transport')),
        (batch.get('coordinator_identity'), batch.get('native_transport')),
        (shown.get('identity'), shown.get('transport')),
    ):
        if ((bound_identity and bound_identity != identity) or
                (bound_transport and not same_transport(bound_transport, transport))):
            raise ValueError('proposal belongs to a different native session or transport')


def proposal_result(guard, identity, digest, transport):
    """Read-only snapshot; reporting never renews, adopts, sends or admits work."""
    from hermes_cli import diggr_delivery as delivery
    with guard.transaction(read_only=True) as rows:
        p = rows.grants['proposals'][digest]
        check_proposal_origin(rows, p, identity, transport)
        now = time.time()
        pv = p.get('preview', {})
        batch = rows.grants.get('batches', {}).get(p.get('batch'))
        state = current_preview_state(rows, p, now)
        send = pv.get('send', 'unbound')
        # Match delivery's deadlines even before its next tick, without changing
        # the durable reservation or risking another original send.
        if send == 'sending' and now >= pv['send_due']:
            send = 'uncertain'
        elif send == 'pending' and now >= p['requested_at'] + delivery.DELIVERY_SECONDS:
            send = 'failed'
        ui = 'pending' if pv else 'unbound'
        if pv and pv['state'] == state:
            if pv['ui_applied'] == pv['ui_revision']:
                ui = 'applied'
            elif pv['ui_attempts'] >= delivery.MAX_ATTEMPTS:
                ui = 'failed'
        result = dict(proposal_sha256=digest, proposal_state=state, preview_send=send, preview_ui=ui)
        fallback = ('Owner kann im selben nativen Chat ausdrücklich /continuation show ' + digest +
                    ' senden und danach die angezeigte Textbestätigung verwenden.')
        if batch:
            turn = batch.get('coordinator_delivery', {})
            result.update(batch_id=p['batch'], admitted_tasks=len(batch['tasks']),
                          coordinator_status=turn.get('status', 'unbound'), batch_deadline=batch['deadline'],
                          batch_expired=batch['deadline'] is not None and now >= batch['deadline'])
        if state == 'revoked':
            status, message, step = state, 'Vorschlag oder Batch widerrufen.', (
                'Keine weitere Ausführung. Bestehende Arbeit abgleichen; neue Autorisierung nur ausdrücklich.')
        elif state == 'confirmed':
            status, message, step = state, 'Bestätigt. Freigabe allein belegt weder Workerstart noch Zustellung.', (
                'Vorhandenen Batch und Taskstatus prüfen; nur innerhalb der bestehenden nativen Gates fortsetzen. '
                'Keine zweite Freigabe und keinen zusätzlichen Start aus dieser Wiederholung ableiten.')
            if (result['batch_expired'] or result['coordinator_status'] in {'uncertain', 'suppressed', 'exhausted', 'unbound'} or
                    (turn.get('status') in {'pending', 'queued', 'accepted'} and now >= turn['deadline'])):
                step = ('Bestehenden Batch und Tasks ausdrücklich abgleichen; keine automatische Wiederholung '
                        'oder Budgetverlängerung. Diese Antwort erteilt keine neue Ausführungsberechtigung.')
        elif state == 'declined':
            status, message, step = state, 'Abgelehnt. Keine Freigabe erteilt.', (
                'Entscheidung beibehalten; erneute Autorisierung nur nach ausdrücklicher Owner-Anweisung.')
        elif state == 'expired':
            status, message, step = state, 'Vorschau abgelaufen. Keine Freigabe erteilt.', (
                'Owner muss die bestehende Vorschau ausdrücklich erneuern und danach erneut freigeben.')
            if ui == 'failed':
                step = fallback
        elif not pv:
            status, message, step = 'legacy_unbound', 'Altvorschlag ohne automatische Button-Vorschau.', fallback
        elif send in {'uncertain', 'failed'}:
            status = 'native_preview_' + send
            message = ('Vorschauversand unklar; kein automatischer Neuversand.' if send == 'uncertain' else
                       'Vorschau konnte innerhalb der Versandfrist nicht zugestellt werden.')
            step = fallback
        elif state == 'open':
            status, message, step = state, 'Vorschau vorhanden; Freigabe offen.', (
                'Owner kann die vorhandene Vorschau mit Freigeben/Ablehnen entscheiden; keine neue Vorschau anfordern.')
            if ui == 'failed':
                message, step = 'Vorschau gebunden, Button-Aktualisierung fehlgeschlagen.', fallback
        else:
            status, message, step = 'native_preview_pending', 'Nativer Vorschauversand ausstehend; keine Freigabe erteilt.', (
                'Die native Vorschau abwarten. Bei Versandfehler oder unklarem Ausgang den expliziten Textpfad nutzen; '
                'keinen Originalsend wiederholen.')
        return dict(result, status=status, message=message, next_step=step)


PREVIEW_SECONDS = 300
MAX_CALLBACKS = 32
_CALLBACK_SEAL = object()


def preview_text(manifest):
    """Plain text, with every authority field visible; no executor Markdown."""
    lines = ['Begrenzter Coding-Auftrag', 'Unveränderlicher Freigabevertrag; aktueller Status separat.']
    for todo in manifest['todos']:
        scope = todo['contract']
        lines.append('Todo: ' + issue_key(todo))
        if scope.get('version') == LOGICAL_VERSION:
            lines += [f"Aufgabe: {scope['action']}", f"Scope: {scope['scope']} ({scope['plane_id']})",
                      f"Rolle/Gate: {scope['owner']}/{scope['gate']}; Executor: {scope['producer']}"]
            for key, label in (('repo', 'Repo'), ('worktrees', 'Worktrees'), ('artifacts', 'Artefakte'),
                               ('allowed_paths', 'Erlaubte Pfade'), ('no_touch', 'No-Touch')):
                lines.append(label + ': ' + canonical(scope['resources'][key]))
            lines += ['Coding-Rechte: ' + ', '.join(scope['actions']['coding']),
                      'Main-Rechte: ' + ', '.join(scope['actions']['main']),
                      'Merge: ' + ('JA (nur Main)' if 'merge' in scope['actions']['main'] else 'NEIN'),
                      'Modell / kein Fallback: ' + canonical(scope['model']),
                      'Abnahme: ' + canonical(scope['acceptance'])]
        else:
            lines += ['Exakter Vertrag: ' + canonical(scope),
                      'Merge: NEIN (keine zusätzliche Mergefreigabe)']
        lines += ['Abhängigkeiten: ' + canonical(todo['depends_on']), 'Gates: ' + canonical(todo['gates'])]
    lines += ['Budgets: ' + canonical(manifest['budgets']), 'Ersetzt Batch: ' + canonical(manifest['replaces'])]
    return '\n'.join(lines)


def new_preview(digest, manifest):
    text = preview_text(manifest)
    if len(text.encode('utf-16-le')) // 2 > 3800:
        raise ValueError('native owner preview exceeds one readable Telegram message')
    return dict(ref=secrets.token_urlsafe(12), version=1, digest=digest, text=text,
                state='pending', send='pending', ui_revision=0, ui_applied=-1, ui_attempts=0, ui_due=0)


def preview_state(preview, state):
    if preview['state'] != state:
        preview.update(state=state, ui_revision=preview['ui_revision'] + 1, ui_attempts=0, ui_due=0)


def current_preview_state(rows, p, now):
    preview = p.get('preview', {})
    batch = rows.grants.get('batches', {}).get(p.get('batch'))
    if p.get('revoked') or (batch and batch['revoked']):
        return 'revoked'
    if batch:
        return 'confirmed'
    if p.get('declined'):
        return 'declined'
    if preview.get('state') == 'open' and now >= preview['expires']:
        return 'expired'
    return preview.get('state', 'unconfirmed')


def refresh_preview_state(rows, p, now):
    if p.get('preview'):
        preview_state(p['preview'], current_preview_state(rows, p, now))


@dataclass(frozen=True)
class NativeCallback:
    """Adapter-only proof. Never serialized, exposed to tools or made from text."""
    source: object
    adapter: object
    query_id: str
    selector: str
    message: str
    bot: str
    seal: object


def telegram_callback(adapter, update):
    """Only the CallbackQueryHandler may cross this boundary, using real PTB objects."""
    from telegram import Update, CallbackQuery, Message
    if type(update) is not Update or type(update.callback_query) is not CallbackQuery:
        return None
    query = update.callback_query
    message, user = query.message, query.from_user
    if (type(message) is not Message or not message.date or message.date.timestamp() <= 0 or
            message.chat.type != 'private' or not user or user.is_bot or
            str(user.id) != OWNER or str(message.chat.id) != OWNER or
            not message.from_user or not message.from_user.is_bot or not query.id or
            not isinstance(query.data, str) or not 1 <= len(query.data.encode('utf-8')) <= 64):
        return None
    try:
        if query.get_bot() is not adapter._bot or str(message.from_user.id) != str(adapter._bot.id):
            return None
    except (AttributeError, RuntimeError):
        return None
    source = adapter.build_source(chat_id=str(message.chat.id), chat_type='dm', user_id=str(user.id),
        user_name=user.username, thread_id=str(message.message_thread_id or '') or None)
    runner = getattr(adapter, 'gateway_runner', None)
    if not runner or runner._registered_transport_adapter(source) is not adapter:
        return None
    transport_profile = runner._adapter_profile_for_source(source)
    # CallbackQuery bypasses the text-message wrapper that normally stamps the
    # receiving profile. Derive its default from the live registered adapter.
    source.profile = source.profile or transport_profile
    from contextlib import nullcontext
    from copy import copy
    from gateway.run import _profile_runtime_scope
    transport_source = copy(source)
    transport_source.profile = transport_profile
    scope = (_profile_runtime_scope(runner._resolve_profile_home_for_source(transport_source))
             if getattr(runner.config, 'multiplex_profiles', False) else nullcontext())
    with scope:
        if not runner._is_user_authorized(source):
            return None
    return NativeCallback(source, adapter, query.id, query.data, str(message.message_id),
                          str(adapter._bot.id), _CALLBACK_SEAL)


def display_recipient(identity, transport):
    return dict(stable_owner(identity), bot_id=transport['bot_id'],
                transport_profile=transport['transport_profile'], home_namespace=transport['home_namespace'])


async def handle_callback(runner, proof):
    """Selector -> persisted preview -> one Guard decision. Never dispatch a worker."""
    import re
    from hermes_cli import diggr_delivery as delivery
    from hermes_cli.diggr_continuation import runtime_guard
    if type(proof) is not NativeCallback or proof.seal is not _CALLBACK_SEAL:
        raise ValueError('real adapter callback required')
    match = re.fullmatch(r'og:([1-9][0-9]?):([adr]):([A-Za-z0-9_-]{16})', proof.selector)
    if not match:
        raise ValueError('unknown owner button')
    version, action, ref = int(match[1]), match[2], match[3]
    guard = runtime_guard(runner._resolve_profile_home_for_source(proof.source))
    if guard is None:
        raise ValueError('native owner grant unavailable')
    with guard.transaction(read_only=True) as rows:
        found = [(digest, p) for digest, p in rows.grants.get('proposals', {}).items()
                 if p.get('preview', {}).get('ref') == ref or
                 any(c['ref'] == ref for c in p.get('callbacks', {}).values())]
    if len(found) != 1:
        raise ValueError('unknown or replaced owner button')
    digest, snapshot = found[0]
    identity, transport = snapshot.get('coordinator_identity'), snapshot.get('native_transport')
    if not identity or not transport:
        raise ValueError('native preview binding missing')
    source = await delivery.origin_scope(runner, identity, transport, snapshot['requested_at'])
    if not source:
        raise ValueError('preview session closed or unavailable')
    current = runner._diggr_identity(proof.source, identity['session'])
    if (current != identity or proof.bot != transport['bot_id'] or
            (proof.source.profile or '') != transport['profile'] or
            (runner._adapter_profile_for_source(proof.source) or '') != transport['transport_profile'] or
            runner._completion_receipt_adapter(proof.source, transport) is not proof.adapter):
        raise ValueError('callback does not match native preview origin')
    callback_id = binding_hash(dict(namespace=display_recipient(identity, transport), query=proof.query_id))
    with guard.transaction() as rows:
        now = time.time()
        p = rows.grants['proposals'].get(digest)
        preview = p.get('preview') if p else None
        if (not preview or binding_hash(p['manifest']) != digest or preview['digest'] != digest or
                preview['text'] != preview_text(p['manifest']) or p['owner'] != stable_owner(current) or
                p.get('coordinator_identity') != identity or p.get('native_transport') != transport or
                preview.get('recipient') != display_recipient(current, transport) or
                preview.get('message_id') != proof.message or preview['send'] != 'bound'):
            raise ValueError('callback differs from durable displayed proposal')
        refresh_preview_state(rows, p, now)
        # Stop always wins over replayed UI success. No changed grant or renewed budget.
        if preview['state'] == 'revoked':
            return 'revoked', identity
        for other_digest, other in rows.grants.get('proposals', {}).items():
            prior = other.get('callbacks', {}).get(callback_id)
            if prior:
                if (other_digest != digest or prior['ref'] != ref or prior['version'] != version or
                        prior['action'] != action):
                    raise ValueError('callback already consumed for another decision')
                return prior['outcome'], identity
        if preview['ref'] != ref or preview['version'] != version:
            raise ValueError('owner button replaced; use the current preview')
        callbacks = p.setdefault('callbacks', {})
        if len(callbacks) >= MAX_CALLBACKS:
            raise ValueError('owner preview callback limit reached')
        outcome = preview['state']
        if outcome == 'open' and action in {'a', 'd'}:
            if action == 'a':
                confirm_batch(rows, p, digest, identity, 'callback:' + callback_id, None, now, transport)
                outcome = 'confirmed'
            else:
                p['declined'] = True
                preview_state(preview, 'declined')
                outcome = 'declined'
        elif outcome == 'expired' and action == 'r':
            # A renewed display is still unconfirmed; the new ref needs its own click.
            preview.update(ref=secrets.token_urlsafe(12), version=version + 1, expires=now + PREVIEW_SECONDS)
            preview_state(preview, 'open')
            outcome = 'renewed'
        callbacks[callback_id] = dict(ref=ref, version=version, action=action, outcome=outcome)
    runner._diggr_homes = getattr(runner, '_diggr_homes', set()) | {identity['home']}
    return outcome, identity


def confirm_batch(rows, p, digest, identity, event_id, code, now, transport=None):
    """Called under Guard's lock by either native proof; no effects outside state."""
    from hermes_cli.diggr_continuation import (
        object_hash, ownership_pending, operator_archive_matches, prelaunch_retirement_matches)
    owner = stable_owner(identity)
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
        if (overlap and bid != replaces and not old.get('replaced_by') and
                not all(operator_archive_matches(rows.get(old.get('tasks', {}).get(issue), {})) or
                        prelaunch_retirement_matches(rows.get(old.get('tasks', {}).get(issue), {}))
                        for issue in overlap)):
            raise ValueError('same logical work requires explicit reauthorization of prior batch')
    if replaces:
        old = batches.get(replaces)
        if not old or old['owner'] != owner or old.get('replaced_by'):
            raise ValueError('invalid reauthorization target')
        if any(r.get('policy', {}).get('batch') == replaces and ownership_pending(r) for r in rows.values()):
            raise ValueError('prior batch still owns unreconciled work')
    batch_id = object_hash(dict(owner=owner, proposal=digest, event=event_id))
    batch = dict(owner=owner, manifest=p['manifest'], proposal=digest,
        confirmation_event=event_id, confirmation_code=code, confirmed_at=now,
        coordinator_identity=json.loads(canonical(identity)),
        started_at=None, deadline=None, tasks={}, revoked=False, replaces=replaces)
    if transport:
        batch.update(native_transport=json.loads(canonical(transport)),
                     coordinator_delivery=dict(status='pending', generation=1, due=0,
                                               deadline=now + BUDGETS['batch_seconds']))
    batches[batch_id] = batch
    if replaces:
        batches[replaces].update(revoked=True, replaced_by=batch_id)
        pending = batches[replaces].get('coordinator_delivery')
        if pending:
            pending['status'] = 'suppressed'
    used_events[event_key] = batch_id
    p['batch'] = batch_id
    if p.get('preview'):
        preview_state(p['preview'], 'confirmed')
    return batch_id


async def handle_command(runner, event, guard, identity):
    """Consumed before agent execution; separate explicit show and confirm messages."""
    event_id = verify_input(event, identity)
    words = event.text.split()
    if len(words) >= 2 and words[1] in {'archive-show', 'archive-confirm'}:
        return await handle_operator_archive(runner, event, guard, identity, event_id, words)
    if len(words) not in (3, 4) or words[0] != '/continuation' or words[1] not in {'show', 'confirm'}:
        raise ValueError('use /continuation show HASH or /continuation confirm HASH CODE')
    digest = words[2]
    owner = stable_owner(identity)
    # Only an explicit, authenticated text request may bind a legacy display.
    # No transport is guessed for old fixtures/rows lacking a live native adapter.
    from hermes_cli.diggr_delivery import command_transport, same_transport
    transport = command_transport(runner, event, identity)
    with guard.transaction() as rows:
        now = time.time()
        p = rows.grants.get('proposals', {}).get(digest)
        if not p or p['owner'] != owner or hashlib.sha256(canonical(p['manifest']).encode()).hexdigest() != digest:
            raise ValueError('unknown, altered or foreign owner proposal')
        if p.get('revoked') or p.get('declined'):
            raise ValueError('proposal revoked or declined; a new scoped proposal is required')
        if p.get('native_transport') and (not same_transport(p['native_transport'], transport) or
                                          p['coordinator_identity'] != identity):
            raise ValueError('proposal belongs to a different native session or transport')
        if words[1] == 'show':
            if len(words) != 3:
                raise ValueError('show takes exactly the immutable proposal hash')
            if p.get('batch'):
                response = 'Already confirmed batch ' + p['batch']
                preview = None
            else:
                preview = dict(code=secrets.token_hex(12), event=event_id, expires=now + 300,
                               identity=identity, transport=transport)
                p['show_reservation'] = event_id
                response = ('Owner proposal (no execution yet):\n```json\n' + canonical(p['manifest']) + '\n```' +
                            '\nSHA256 ' + digest + '\nConfirm within 300 seconds with a NEW unformatted direct text message '
                            '(no code block, quote, reply or forward):\n' +
                            '/continuation confirm ' + digest + ' ' + preview['code'])
        else:
            if len(words) != 4:
                raise ValueError('confirmation requires exact hash and one-use code')
            if p.get('batch'):
                batch = rows.grants['batches'][p['batch']]
                if batch.get('revoked'):
                    raise ValueError('confirmed proposal revoked; no renewed authority')
                if batch['confirmation_event'] != event_id or batch['confirmation_code'] != words[3]:
                    raise ValueError('confirmation already consumed; batch remains confirmed')
                response = 'Already confirmed batch ' + p['batch']
            else:
                preview = p.get('shown')
                if not preview:
                    raise ValueError('preview missing; request /continuation show with the proposal hash first')
                if now >= preview['expires']:
                    raise ValueError('preview expired; request /continuation show again, then send a new unformatted confirmation')
                if preview['code'] != words[3]:
                    raise ValueError('wrong or replaced confirmation code; use the latest preview')
                if event_id == preview['event']:
                    raise ValueError('confirmation needs a new unformatted direct text message')
                bound = preview.get('transport')
                if bound and (not same_transport(bound, transport) or preview.get('identity') != identity):
                    raise ValueError('text preview native session or transport changed')
                batch_id = confirm_batch(rows, p, digest, identity, event_id, words[3], now, bound)
                response = 'Confirmed batch ' + batch_id + ('. Main continuation queued; admission checks still apply.'
                    if bound else '. No native delivery binding; no work dispatched.')
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
            if not p.get('batch') and not p.get('revoked') and not p.get('declined') and p.get('show_reservation') == event_id:
                p['shown'] = preview
    return False  # fully handled, never forwarded to agent/tool execution


def _app104_archive_candidate(rows, identity):
    """One historical identity-loss case, never a reusable task unlock."""
    row = rows.get('APP-104-stable-album-children')
    if (not row or row.get('task') != 'APP-104-stable-album-children' or
            row.get('generation') != 3 or row.get('status') != 'blocked' or
            row.get('effect_status') != 'unknown' or row.get('producer') != 'cmux' or
            row.get('owner_batch') != '13bd6f74a3e74ce084e43bbe823f59b444d4e76e89dbdffc2d31b8e75866ad62' or
            row.get('action_id') != 'fe6b4781ab5e4989a923ab65ab3aa6e3' or
            row.get('effect_id') != '8993957c2d584e418a42c4d5018d4bf6' or
            row.get('identity') != identity or not row.get('launcher_expected') or
            any(row.get(k) for k in ('process_id', 'launcher_pid', 'launcher_identity',
                                     'worker_pid', 'worker_identity', 'child_identity',
                                     'visible_sent', 'reservation_retirement', 'operator_archive'))):
        raise ValueError('APP-104 historical archive does not match the exact blocked attempt')
    return row


async def handle_operator_archive(runner, event, guard, identity, event_id, words):
    """Direct native owner acceptance of one documented unknown historical attempt."""
    from hermes_cli.diggr_continuation import object_hash, operator_archive_core
    from hermes_cli.diggr_delivery import command_transport, same_transport
    transport = command_transport(runner, event, identity)
    if words[1] == 'archive-show':
        if len(words) != 2:
            raise ValueError('archive-show takes no task alias or payload')
        with guard.transaction() as rows:
            row = _app104_archive_candidate(rows, identity)
            preview = dict(core_sha256=operator_archive_core(row), generation=row['generation'],
                identity=identity, transport=transport, event=event_id,
                code=secrets.token_hex(12), expires=time.time() + 300)
            rows.grants.setdefault('operator_archive_previews', {})[row['task']] = preview
        response = ('APP-104 old attempt, generation 3: start and effects remain UNKNOWN. '
            'This one-time owner archive releases its Main admission and visible target; '
            'the old worktree remains reserved. A second execution may duplicate past work. '
            'No success, no-effect or proven nonstart is asserted. To accept this risk, '
            'send a NEW direct unformatted message within five minutes:\n'
            '/continuation archive-confirm ' + preview['core_sha256'] + ' ' + preview['code'])
    else:
        if len(words) != 4:
            raise ValueError('archive-confirm requires the exact shown hash and one-use code')
        with guard.transaction() as rows:
            row = _app104_archive_candidate(rows, identity)
            preview = rows.grants.get('operator_archive_previews', {}).get(row['task'])
            if (not preview or time.time() >= preview['expires'] or
                    preview['identity'] != identity or
                    not same_transport(preview['transport'], transport) or
                    preview['event'] == event_id or
                    preview['core_sha256'] != words[2] or
                    preview['code'] != words[3] or
                    operator_archive_core(row) != preview['core_sha256']):
                raise ValueError('APP-104 archive preview expired, changed or not owner-confirmed')
            receipt = dict(kind='owner_accepted_historical_uncertainty',
                task=row['task'], generation=row['generation'], identity=dict(identity),
                action_id=row['action_id'], effect_id=row['effect_id'],
                core_sha256=preview['core_sha256'], native_owner_event=event_id,
                preview_event=preview['event'], accepted_at=time.time(),
                historical_effects='unknown', duplicate_work_risk_accepted=True,
                released=['main_session', 'visible_target'], retained=['old_worktree'])
            row['operator_archive'] = receipt
            rows.grants.setdefault('operator_archive_events', {})[object_hash(dict(
                owner=stable_owner(identity), event=event_id))] = receipt
            rows.grants['operator_archive_previews'].pop(row['task'], None)
        response = ('APP-104 old attempt archived by direct native owner confirmation. '
            'Historical effects remain unknown; the old worktree stays reserved. '
            'Main may propose a fresh separately confirmed APP-104 task; no worker was started.')
    from gateway.platforms.base import _thread_metadata_for_source
    result = await runner._adapter_for_source(event.source).send(
        event.source.chat_id, response, metadata=_thread_metadata_for_source(event.source, str(event.message_id)))
    if not getattr(result, 'success', False):
        raise ValueError('native archive display delivery failed')
    return False


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
    for p in rows.grants.get('proposals', {}).values():
        if p['owner'] == owner:
            p['revoked'] = True
            if p.get('preview'):
                preview_state(p['preview'], 'revoked')
    for batch in rows.grants.get('batches', {}).values():
        if batch['owner'] == owner:
            batch['revoked'] = True
            if batch.get('coordinator_delivery'):
                batch['coordinator_delivery']['status'] = 'suppressed'
    for row in rows.values():
        if row.get('policy') and stable_owner(row['identity']) == owner:
            row['policy']['revoked'] = True
