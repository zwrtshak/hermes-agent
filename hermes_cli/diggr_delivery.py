"""Origin-bound status outbox in the existing Guard transaction.

No worker launch or execution authority. Same-UID state access is trusted just
like the existing owner ledger; this is not an OS isolation boundary.
"""
import json
from dataclasses import dataclass


@dataclass(frozen=True)
class NativeWake:
    task: str
    generation: int
    session: str


PROFILES = {'diggr-main': 'main', 'mira': 'mira'}


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
                                     task=row['task'], coordinator=coordinator(row['identity']))))


def reconcile(rows, before):
    """Validate immutable routing and append meaningful state transitions atomically."""
    for name, row in rows.items():
        previous = before.get(name, {})
        origin = row.get('origin')
        if previous.get('origin') and origin != previous['origin']:
            raise ValueError('immutable origin changed')
        if not origin:
            continue  # legacy rows require explicit reconciliation, never guessed delivery
        if origin != origin_for(rows, row):
            raise ValueError('origin no longer matches native grant and task')
        # No status notification for registration, transport send, queue or watcher.
        phase = None
        if row.get('child_identity') and row.get('child_identity') != previous.get('child_identity'):
            phase = 'worker_process_started'  # process proof, not first tool-action proof
        elif row.get('late_evidence') != previous.get('late_evidence'):
            phase = 'late_result_retained_no_execution'
        elif row.get('gate') == 'reconcile' and row.get('generation') != previous.get('generation'):
            phase = 'reconcile'
        elif row.get('status') in {'done', 'awaiting_user', 'blocked', 'paused', 'cancelled', 'superseded'}:
            if row.get('status') != previous.get('status'):
                phase = row['status']
        elif row.get('gate') == 'main_validation' and previous.get('gate') != 'main_validation':
            phase = 'worker_result_ready_for_coordinator'
        elif row.get('progress_fingerprint') and row.get('progress_fingerprint') != previous.get('progress_fingerprint'):
            phase = 'coordinator_validated_progress'
        if phase:
            deliveries = row.setdefault('deliveries', [])
            event_id = f"{row['task']}:{row['generation']}:{phase}"
            if not any(d['id'] == event_id for d in deliveries):
                deliveries.append(dict(id=event_id, phase=phase, status='pending', attempts=0, due=0))
        if row.get('gate') == 'main_validation' and previous.get('gate') != 'main_validation':
            row['coordinator_delivery'] = dict(status='pending', generation=row['generation'])


async def deliver(runner, guard, identity):
    """Bounded Telegram notification; coordinator acceptance is tracked separately."""
    import asyncio
    import time
    import uuid
    from gateway.config import Platform
    from gateway.session import SessionSource
    from gateway.platforms.base import _thread_metadata_for_source

    # A caller cannot redirect a persisted origin or select another profile's bot.
    with guard.transaction() as rows:
        names = [name for name, row in rows.items() if row.get('origin', {}).get('identity') == identity]
    if not names:
        return
    source = SessionSource(platform=Platform.TELEGRAM, profile=identity['profile'],
                           chat_id=identity['chat_id'], user_id=identity['user_id'],
                           thread_id=identity['thread_id'] or None, chat_type='dm')
    if (identity['platform'] != 'telegram' or
            str(runner._resolve_profile_home_for_source(source).resolve()) != identity['home']):
        raise ValueError('delivery home/platform no longer matches origin')
    adapter = runner._adapter_for_source(source)
    metadata = dict(_thread_metadata_for_source(source) or {}, diggr_durable_delivery=True)
    if identity['thread_id'] == '1':
        # Telegram General has no topic parameter or synthetic reply anchor.
        # Keep origin=1 durable; do not recover a different last-active topic.
        metadata.pop('telegram_dm_topic_reply_fallback', None)
    for name in names:
        now = time.time()
        claim = uuid.uuid4().hex
        with guard.transaction() as rows:
            row = rows[name]
            for item in row.get('deliveries', []):
                if item['status'] == 'sending' and now >= item['due']:
                    item.update(status='uncertain', error='interrupted_send')
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
        try:
            if adapter is None:
                status, message_id, error = 'pending', None, 'adapter_unavailable'
            else:
                result = await asyncio.wait_for(adapter.send(
                    identity['chat_id'], content,
                    metadata=metadata), timeout=20)
                details = getattr(result, 'raw_response', None) or {}
                outcome = details.get('delivery_outcome')
                message_ids = [str(m) for m in details.get('message_ids', [])]
                status = ('delivered' if getattr(result, 'success', False) is True else
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
            if status == 'pending' and item['attempts'] >= 3:
                status = 'failed'
            item.update(status=status, message_id=message_id, message_ids=message_ids,
                        transport_outcome=outcome, error=error,
                        due=time.time() + 15 * 2 ** (item['attempts'] - 1))


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
        if not row or row.get('origin', {}).get('identity') != identity:
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
            if item['status'] == 'pending' and item['phase'] in {
                    'worker_result_ready_for_coordinator', 'coordinator_validated_progress'}:
                item.update(status='superseded', superseded_by=event_id)
        deliveries.append(dict(id=event_id, phase='coordinator_reply', content=content,
                               status='pending', attempts=0, due=0, failed=failed))
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
