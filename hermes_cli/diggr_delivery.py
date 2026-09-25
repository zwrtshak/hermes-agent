"""Origin-bound status outbox in the existing Guard transaction.

No worker launch or execution authority. Same-UID state access is trusted just
like the existing owner ledger; this is not an OS isolation boundary.
"""
import json
import time
from dataclasses import dataclass


@dataclass(frozen=True)
class NativeWake:
    task: str
    generation: int
    session: str


PROFILES = {'diggr-main': 'main', 'mira': 'mira'}
# Preserve the existing three-send/20-second timeout and 15/30-second backoff
# budget, including while an adapter is unavailable or the gateway restarts.
SEND_TIMEOUT = 20
MAX_ATTEMPTS = 3
DELIVERY_SECONDS = MAX_ATTEMPTS * SEND_TIMEOUT + 15 + 30


def native_transport(identity):
    """Capture gateway-owned transport context; task arguments cannot mint it."""
    from gateway.session_context import completion_launch_context
    context = completion_launch_context.get()
    binding = context[0] if isinstance(context, tuple) and len(context) == 2 else None
    if not isinstance(binding, dict):
        raise ValueError('native transport binding required before registration')
    required = {'profile', 'transport_profile', 'home_namespace', 'profile_home',
                'bot_id', 'chat_type', 'platform', 'user_id', 'session_key',
                'parent_session_id', 'chat_id', 'thread_id'}
    if (not required.issubset(binding) or not binding['bot_id']
            or any(binding[key] != identity[target] for key, target in (
                ('profile_home', 'home'), ('platform', 'platform'), ('user_id', 'user_id'),
                ('session_key', 'session_key'), ('parent_session_id', 'session'),
                ('chat_id', 'chat_id'), ('thread_id', 'thread_id')))
            or binding['profile'] not in ('', identity['profile'])):
        raise ValueError('native transport binding differs from coordinator identity')
    return json.loads(json.dumps({key: binding[key] for key in required | {'metadata'} if key in binding}))


def transport_for(row):
    """Only launch-time provenance, never inferred from current configuration."""
    origin = row.get('origin') or {}
    transport = origin.get('transport')
    if (not isinstance(transport, dict) or not transport.get('bot_id')
            or transport != row.get('native_transport')
            or origin.get('identity') != row.get('identity')):
        return None
    return transport


async def route_scope(runner, row):
    """Reuse lifecycle checks, then require the persisted transport owner."""
    identity = row.get('origin', {}).get('identity')
    binding = transport_for(row)
    from gateway.run import _hermes_home
    if binding is None or not identity or binding.get('home_namespace') != str(_hermes_home):
        return None
    try:
        await runner.async_session_store._ensure_loaded()
    except Exception:
        return False
    entry = runner.session_store._entries.get(identity['session_key'])
    source = getattr(entry, 'origin', None)
    if source is None:
        return None
    if str(runner._resolve_profile_home_for_source(source).resolve()) != identity['home']:
        raise ValueError('delivery home no longer matches origin')
    if runner._diggr_identity(source, identity['session']) != identity:
        return None
    evt = dict(started_at=row['registered_at'], session_key=identity['session_key'])
    source = await runner._completion_receipt_scope(evt, binding, validate_binding=False)
    if source is None or source is False:
        return source
    if runner._completion_receipt_adapter(source, binding) is None:
        return False
    return source


def native_turn():
    from hermes_cli.diggr_continuation import EVENT_CONTEXT
    return type((EVENT_CONTEXT.get() or {}).get('native_wake')) is NativeWake


def hold_native_queue(adapter, session_key):
    # Native completions must traverse _handle_message/finish as their own turn,
    # never be folded into the previous turn's recursive in-band drain.
    head = getattr(adapter, '_pending_messages', {}).get(session_key)
    return native_turn() or type(getattr(head, '_diggr_wake', None)) is NativeWake


def coordinator(identity):
    return PROFILES[identity['profile']]


def origin_for(rows, row):
    batch = rows.grants.get('batches', {}).get(row.get('policy', {}).get('batch'))
    if not batch:
        raise ValueError('origin requires confirmed owner batch')
    return json.loads(json.dumps(dict(identity=row['identity'], request_id=batch['confirmation_event'],
                                     batch=row['policy']['batch'], issue=row['policy']['issue'],
                                     task=row['task'], coordinator=coordinator(row['identity']),
                                     **({'transport': row['native_transport']} if row.get('native_transport') else {}))))


def reconcile(rows, before):
    """Validate immutable routing and append meaningful state transitions atomically."""
    for name, row in rows.items():
        previous = before.get(name, {})
        origin = row.get('origin')
        if previous.get('native_transport') != row.get('native_transport') and previous:
            raise ValueError('immutable native transport changed')
        if previous.get('origin') and origin != previous['origin']:
            raise ValueError('immutable origin changed')
        if not origin:
            continue  # legacy rows require explicit reconciliation, never guessed delivery
        if origin != origin_for(rows, row):
            raise ValueError('origin no longer matches native grant and task')
        # No status notification for registration, transport send, queue or watcher.
        phase = None
        result = row.get('worker_result')
        if result and result != previous.get('worker_result'):
            phase = 'worker_process_exited'  # native wait result, never launcher exit
        elif row.get('late_evidence') != previous.get('late_evidence'):
            phase = 'late_result_retained_no_execution'
        elif row.get('gate') == 'reconcile' and row.get('generation') != previous.get('generation'):
            phase = 'reconcile'
        elif row.get('status') in {'done', 'awaiting_user', 'blocked', 'paused', 'cancelled', 'superseded'}:
            if row.get('status') != previous.get('status'):
                phase = row['status']
        elif (not result and row.get('gate') == 'main_validation'
              and previous.get('gate') != 'main_validation'):
            phase = 'worker_result_ready_for_coordinator'
        if phase:
            deliveries = row.setdefault('deliveries', [])
            generation = result['generation'] if phase == 'worker_process_exited' else row['generation']
            event_id = f"{row['task']}:{generation}:{phase}"
            if not any(d['id'] == event_id for d in deliveries):
                item = dict(id=event_id, phase=phase, status='pending', attempts=0, due=0,
                            deadline=time.time() + DELIVERY_SECONDS)
                if phase == 'worker_process_exited':
                    item['exit_code'] = result['exit_code']
                    item['content'] = (f"{row['task']}: Worker beendet (Exit {result['exit_code']}); "
                                       "Ergebnis noch nicht geprüft.")
                deliveries.append(item)
        if row.get('gate') == 'main_validation' and previous.get('gate') != 'main_validation':
            row['coordinator_delivery'] = dict(status='pending', generation=row['generation'])


async def deliver(runner, guard, identity):
    """Bounded Telegram notification; coordinator acceptance is tracked separately."""
    import asyncio
    import time
    import uuid
    from gateway.platforms.base import _thread_metadata_for_source

    # A caller cannot redirect a persisted origin or select another profile's bot.
    with guard.transaction() as rows:
        names = [name for name, row in rows.items() if row.get('origin', {}).get('identity') == identity]
    if not names:
        return
    if identity['platform'] != 'telegram':
        raise ValueError('delivery platform no longer matches origin')
    for name in names:
        snapshot = guard.get(name)
        source = await route_scope(runner, snapshot)
        adapter = runner._completion_receipt_adapter(source, transport_for(snapshot)) if source else None
        notify_mode = runner._load_background_notifications_mode()
        now = time.time()
        claim = uuid.uuid4().hex
        with guard.transaction() as rows:
            row = rows[name]
            for item in row.get('deliveries', []):
                if item['status'] == 'sending' and now >= item['due']:
                    item.update(status='uncertain', error='interrupted_send')
                if item['status'] == 'pending':
                    # Legacy pending records get one bounded reconciliation
                    # window; the persisted deadline is never renewed.
                    item.setdefault('deadline', now + DELIVERY_SECONDS)
                    if source is None:
                        item.update(status='suppressed', error='origin_session_closed')
                    elif (item['phase'] in {'worker_process_exited', 'worker_result_ready_for_coordinator'}
                          and (notify_mode == 'off' or
                               (notify_mode == 'error' and item.get('exit_code', 0) == 0))):
                        item.update(status='suppressed', error='notification_preference')
                    elif now >= item['deadline']:
                        item.update(status='failed', error='delivery_deadline')
            if adapter is None:
                continue  # No physical send, therefore no consumed attempt.
            item = next((d for d in row.get('deliveries', []) if d['status'] == 'pending' and d['due'] <= now), None)
            if item is None:
                continue
            item.update(status='sending', claim=claim, attempts=item['attempts'] + 1, due=now + 30)
            event_id, phase = item['id'], item['phase']
            content = item.get('content') or f"Coding {name}: {phase}"
        # A crash after claiming is deliberately NOT blindly replayed. Telegram
        # provides no idempotency key; operator reconciliation owns uncertainty.
        message_ids = []
        outcome = None
        retry_after = 0
        try:
            if adapter is None:
                status, message_id, error = 'pending', None, 'adapter_unavailable'
            else:
                metadata = dict(transport_for(snapshot).get('metadata') or _thread_metadata_for_source(source) or {},
                                diggr_durable_delivery=True, strict_topic=True)
                if identity['thread_id'] == '1':
                    metadata.pop('telegram_dm_topic_reply_fallback', None)
                result = await asyncio.wait_for(adapter.send(
                    identity['chat_id'], content,
                    metadata=metadata), timeout=SEND_TIMEOUT)
                details = getattr(result, 'raw_response', None) or {}
                outcome = details.get('delivery_outcome')
                retry_after = max(0, getattr(result, 'retry_after', None) or 0)
                message_ids = [str(m) for m in details.get('message_ids', [])]
                status = ('delivered' if getattr(result, 'success', False) is True and getattr(result, 'message_id', None) else
                          'uncertain' if getattr(result, 'success', False) is True else
                          'uncertain' if message_ids else
                          {'safe_retry': 'pending', 'rejected': 'failed'}.get(outcome, 'uncertain'))
                message_id = str(getattr(result, 'message_id', '') or '') if status == 'delivered' else None
                error = None if status == 'delivered' else 'transport_rejected'
        except Exception:
            status, message_id, error = 'uncertain', None, 'transport_exception'
        with guard.transaction() as rows:
            item = next(d for d in rows[name]['deliveries'] if d['id'] == event_id)
            if item.get('claim') != claim or item['status'] != 'sending':
                continue
            if status == 'pending' and item['attempts'] >= MAX_ATTEMPTS:
                status = 'failed'
            item.update(status=status, message_id=message_id, message_ids=message_ids,
                        transport_outcome=outcome, error=error,
                        due=time.time() + max(retry_after, 15 * 2 ** (item['attempts'] - 1)))


async def finish(runner, event, response):
    """Persist the actual native coordinator answer before any final send."""
    from hermes_cli.diggr_continuation import runtime_guard
    proof = getattr(event, '_diggr_wake', None)
    if type(proof) is not NativeWake:
        return False
    identity = runner._diggr_identity(event.source, proof.session)
    guard = runtime_guard(identity['home'])
    if guard is None:
        raise ValueError('origin guard unavailable for coordinator answer')
    content = str(response.get('final_response') or '') if isinstance(response, dict) else str(response or '')
    failed = isinstance(response, dict) and bool(response.get('failed'))
    if failed or not content.strip():
        content = 'Coordinator error; work is not accepted.' + ('\n' + content if content else '')
    with guard.transaction() as rows:
        row = rows.get(proof.task)
        if (not row or row.get('origin', {}).get('identity') != identity
                or transport_for(row) is None
                or runner._completion_receipt_adapter(event.source, transport_for(row)) is None):
            raise ValueError('coordinator answer does not match immutable origin')
        turn = row.get('coordinator_turns', {}).get(str(proof.generation))
        if not turn:
            raise ValueError('coordinator turn was never accepted')
        deliveries = row.setdefault('deliveries', [])
        event_id = f'{proof.task}:{proof.generation}:coordinator_reply'
        existing = next((d for d in deliveries if d['id'] == event_id), None)
        if existing:
            if existing['content'] != content:
                raise ValueError('conflicting duplicate coordinator answer')
            return True
        for item in deliveries:
            if item['status'] == 'pending' and item['phase'] == 'coordinator_validated_progress':
                item.update(status='superseded', superseded_by=event_id)
        deliveries.append(dict(id=event_id, phase='coordinator_reply', content=content,
                               status='pending', attempts=0, due=0, failed=failed,
                               deadline=time.time() + DELIVERY_SECONDS))
        turn.update(status='response_produced', failed=failed)
    return True


def native_identity(runner, event, *, accepted=True):
    """Validate the immutable route before any recovery/session selection."""
    from hermes_cli.diggr_continuation import runtime_guard
    proof = getattr(event, '_diggr_wake', None)
    if type(proof) is not NativeWake:
        return None
    identity = runner._diggr_identity(event.source, proof.session)
    guard = runtime_guard(identity['home'])
    row = guard.get(proof.task) if guard else None
    if (not row or row.get('origin', {}).get('identity') != identity or
            transport_for(row) is None or
            runner._completion_receipt_adapter(event.source, transport_for(row)) is None or
            row['generation'] != proof.generation or
            (accepted and (row['status'] != 'executing' or
             str(proof.generation) not in row.get('coordinator_turns', {})))):
        raise ValueError('native wake no longer owns its bound origin')
    return identity


async def native_session_matches(runner, identity, entry):
    """Only a proven live compression descendant may replace the physical session.

    Origin, grant and budget identities remain unchanged. A new/reset/unknown
    session is not a continuation, even when its chat key happens to match.
    """
    if entry.session_key != identity['session_key']:
        return False
    if getattr(entry, 'was_auto_reset', False):
        return False
    db = getattr(runner, '_session_db', None)
    if db is None:
        return entry.session_id == identity['session']
    try:
        parent = await db.get_session(identity['session'])
        if not parent:
            return False
        if entry.session_id == identity['session']:
            # A stale JSON route is not authority to revive a closed parent.
            return not parent.get('ended_at')
        if not parent.get('ended_at') or parent.get('end_reason') != 'compression':
            return False
        tip = await db.get_compression_tip(identity['session'])
        child = await db.get_session(tip) if tip == entry.session_id else None
        return bool(child and not child.get('ended_at'))
    except Exception:
        return False
