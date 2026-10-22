"""Explicit successor disposition in the existing native owner transaction.

A proposal is inert. Only its direct native confirmation archives the exact
blocked predecessor and reserves one successor item, without filesystem effects.
"""
import copy
import re


def validate(manifest):
    from hermes_cli import diggr_owner as owner
    value = manifest['replaces']
    if not isinstance(value, dict):
        return
    if (set(value) != {'batch', 'task', 'generation', 'core_sha256', 'accept_unknown'} or
            not all(isinstance(value.get(k), str) and re.fullmatch('[0-9a-f]{64}', value[k])
                    for k in ('batch', 'core_sha256')) or
            not isinstance(value.get('task'), str) or not value['task'] or
            len(value['task']) > 160 or any(ord(c) < 32 for c in value['task']) or
            type(value.get('generation')) is not int or value['generation'] < 1 or
            value.get('accept_unknown') is not True or len(manifest['todos']) != 1 or
            manifest['todos'][0]['contract'].get('version') != owner.ITEM_VERSION):
        raise ValueError('explicit bounded logical-v2 successor disposition required')


def check(rows, manifest, identity):
    """Recheck the shown predecessor and actual exit evidence under ledger lock."""
    from hermes_cli import diggr_owner as owner, diggr_continuation as dc
    validate(manifest)
    value = manifest['replaces']
    if not isinstance(value, dict):
        return None
    old = rows.grants.get('batches', {}).get(value['batch'])
    row = rows.get(value['task'], {})
    todo = manifest['todos'][0]
    issue = owner.issue_key(todo)
    if (not old or old.get('owner') != owner.stable_owner(identity) or old.get('replaced_by') or
            old.get('tasks', {}).get(issue) != value['task'] or
            row.get('owner_batch') != value['batch'] or
            row.get('policy', {}).get('batch') != value['batch'] or
            row.get('policy', {}).get('issue') != issue or
            row.get('identity', {}).get('session_key') != identity.get('session_key') or
            owner.stable_owner(row.get('identity', {})) != owner.stable_owner(identity) or
            row.get('producer') != 'cmux' or row.get('status') != 'blocked' or
            row.get('gate') != 'reconcile' or row.get('effect_status') != 'unknown' or
            row.get('generation') != value['generation'] or
            not row.get('action_id') or not row.get('effect_id') or
            row.get('operator_archive') or row.get('reservation_retirement') or
            dc.operator_archive_core(row) != value['core_sha256']):
        raise ValueError('successor predecessor changed or not bound to this owner/item')
    previous_todo = next((t for t in old['manifest']['todos'] if owner.issue_key(t) == issue), None)
    if not previous_todo:
        raise ValueError('successor predecessor issue missing')
    previous_contract = previous_todo['contract']
    if (previous_contract.get('version') in owner.LOGICAL_VERSIONS and
            previous_contract['resources']['repo'] != todo['contract']['resources']['repo']):
        raise ValueError('successor cannot change predecessor repository')
    if any(task != value['task'] and other.get('identity') == identity and dc.ownership_pending(other)
           for task, other in rows.items()):
        raise ValueError('another attempt still owns the lane')
    if any(bid != value['batch'] and batch.get('item', {}).get('phase') not in {None, 'closed', 'archived'}
           for bid, batch in rows.grants.get('batches', {}).items()):
        raise ValueError('another item still owns the lane')
    previous_item = old.get('item')
    if previous_item and (previous_item.get('issue') != issue or previous_item.get('task') != row['task']):
        raise ValueError('predecessor item reservation differs')
    # This proves recorded execution/descendants cannot continue. It deliberately
    # does not claim that past work was absent. Missing identity stays blocked.
    verify_exit(row)
    return row


def verify_exit(row):
    """Select evidence by registry presence; never fall back after failed proof."""
    from hermes_cli import diggr_continuation as dc
    from tools.process_registry import process_registry
    for attempt in [row] + row.get('history', []):
        if attempt.get('task') != row['task'] or attempt.get('identity') != row['identity']:
            raise ValueError('historical attempt identity differs')
        if process_registry.get(attempt.get('process_id')) is not None:
            dc.verify_prior_exit(attempt, allow_idle_unclaimed=True)
        else:
            dc.verify_restarted_reservation_exit(dict(attempt, history=[]))


def apply(rows, row, manifest, identity, event_id, digest, batch_id, batch, now):
    from hermes_cli import diggr_owner as owner, diggr_continuation as dc
    value = manifest['replaces']
    receipt = dict(kind='owner_accepted_successor_uncertainty', task=row['task'],
        generation=row['generation'], identity=copy.deepcopy(row['identity']),
        confirming_identity=copy.deepcopy(identity), action_id=row['action_id'], effect_id=row['effect_id'],
        core_sha256=dc.operator_archive_core(row), native_owner_event=event_id, accepted_at=now,
        historical_effects='unknown', duplicate_work_risk_accepted=True,
        released=['main_session', 'visible_target'], retained=['old_worktree'],
        successor_batch=batch_id, successor_proposal=digest, predecessor_batch=value['batch'])
    row['operator_archive'] = receipt
    rows.grants.setdefault('operator_archive_events', {})[dc.object_hash(dict(
        owner=owner.stable_owner(identity), event=event_id))] = copy.deepcopy(receipt)
    old_item = rows.grants['batches'][value['batch']].get('item')
    if old_item:
        old_item.update(phase='archived', successor_batch=batch_id)
    todo = manifest['todos'][0]
    contract = todo['contract']
    batch['item'] = dict(issue=owner.issue_key(todo), plane_id=contract['plane_id'],
        repo=contract['resources']['repo'], phase='reserved', reserved_at=now,
        work_seconds=0, budget=copy.deepcopy(manifest['budgets']), predecessor=copy.deepcopy(value))


def receipt_matches(row, receipt):
    from hermes_cli import diggr_owner as owner, diggr_continuation as dc
    try:
        return bool(receipt.get('kind') == 'owner_accepted_successor_uncertainty' and
            row.get('producer') == 'cmux' and row.get('status') == 'blocked' and
            row.get('gate') == 'reconcile' and row.get('effect_status') == 'unknown' and
            receipt.get('task') == row['task'] and receipt.get('generation') == row['generation'] and
            receipt.get('identity') == row['identity'] and
            owner.stable_owner(receipt['confirming_identity']) == owner.stable_owner(row['identity']) and
            receipt['confirming_identity']['session_key'] == row['identity']['session_key'] and
            receipt.get('action_id') == row['action_id'] and receipt.get('effect_id') == row['effect_id'] and
            receipt.get('core_sha256') == dc.operator_archive_core(row) and
            receipt.get('native_owner_event') and receipt.get('accepted_at') is not None and
            receipt.get('historical_effects') == 'unknown' and receipt.get('duplicate_work_risk_accepted') is True and
            receipt.get('released') == ['main_session', 'visible_target'] and receipt.get('retained') == ['old_worktree'] and
            receipt.get('predecessor_batch') == row.get('owner_batch') and
            all(isinstance(receipt.get(k), str) and re.fullmatch('[0-9a-f]{64}', receipt[k])
                for k in ('successor_batch', 'successor_proposal')))
    except (KeyError, TypeError):
        return False
