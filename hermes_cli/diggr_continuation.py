"""SYSTEM-167 candidate: evidence gate for native Hermes continuation queues.

No scheduler or CMM state. One ticket-bound foreground worker via existing cmux;
all transitions serialized across processes. One active task per target session.
"""
import copy
import hashlib
import json
import os
import math
import uuid
from pathlib import Path
import tempfile
import time
from contextlib import contextmanager

from hermes_cli import diggr_owner as owner_policy
from hermes_cli import diggr_delivery as delivery

PREFIX = '[DIGGR evidence continuation] '
TERMINAL = {'paused', 'cancelled', 'superseded', 'blocked', 'awaiting_user', 'done'}
_NATIVE_PROVIDER_CORRECTION = object()
# The Coding agent may compact before its first tool call. Only a persisted,
# exact prompt received within the original 60-second admission window may
# obtain this bounded pre-send allowance (600-second compression ceiling + 60).
RECEIVED_PROMPT_CLAIM_SECONDS = 660


def worker_supervision_binding(row, generation=None):
    """The native Item supervisor binds observations to one started attempt."""
    return {key: row.get(key) for key in
            ('task', 'action_id', 'effect_id', 'worker_pid', 'worker_identity', 'child_identity')} | {
                'generation': row.get('generation') if generation is None else generation}


def requires_worker_supervision(row):
    # Historic non-Item workers and unsent/fenced attempts retain their own gates.
    return bool(row.get('policy', {}).get('item_lifecycle') and
                (row.get('claim_phase') == 'claimed' or any(row.get(key) for key in
                    ('worker_pid', 'worker_identity', 'worker_started_ns', 'child_identity',
                     'child_exited', 'worker_result', 'worker_supervision'))))


def worker_operational_exit(row, generation=None):
    if not requires_worker_supervision(row):
        return True
    proof = row.get('worker_supervision')
    result = row.get('worker_result') or {}
    if generation is None and isinstance(proof, dict):
        generation = (proof.get('binding') or {}).get('generation')
    return bool(isinstance(proof, dict) and proof.get('state') == 'exited' and
                proof.get('binding') == worker_supervision_binding(row, generation) and
                isinstance(row.get('worker_identity'), dict) and row.get('worker_pid') and
                isinstance(row.get('child_identity'), dict) and
                row['child_identity'].get('pid') and
                type(proof.get('leader_exit_code')) is int and
                type(proof.get('accounted_seconds')) in (int, float) and
                not proof.get('reason') and
                (not result or result.get('exit_code') == proof['leader_exit_code']))


def require_worker_operational_exit(row, generation=None):
    if not worker_operational_exit(row, generation):
        raise ValueError('started Item worker operational exit is unknown')


class Guard:
    def __init__(self, path, profile=None):
        self.profile = profile
        self.path = Path(path)
        if not self.path.is_absolute():
            raise ValueError('state path must be absolute')

    @contextmanager
    def transaction(self, *, read_only=False):
        import fcntl  # POSIX-only locking is loaded only for an active guard.
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(str(self.path) + '.lock', 'a', encoding='utf-8') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            state = json.loads(self.path.read_text(encoding='utf-8')) if self.path.exists() else {}
            before = json.dumps(state, sort_keys=True)
            state = owner_policy.State(state, state.pop('_owner_grants', {}))
            owner_policy.hydrate(state)
            for row in state.values():
                self.normalize(row)
            previous = json.loads(json.dumps(state))
            yield state
            if read_only:
                return  # Snapshot normalization never creates or updates the ledger.
            delivery.reconcile(state, previous)
            after = json.dumps(dict(state, _owner_grants=state.grants), sort_keys=True)
            if before != after:
                fd, name = tempfile.mkstemp(dir=self.path.parent)
                try:
                    with os.fdopen(fd, 'w', encoding='utf-8') as out:
                        out.write(after + '\n')
                        out.flush()
                        os.fsync(out.fileno())
                    os.replace(name, self.path)
                    directory = os.open(self.path.parent, os.O_RDONLY)
                    try:
                        os.fsync(directory)
                    finally:
                        os.close(directory)
                finally:
                    if os.path.exists(name):
                        os.unlink(name)

    def register(self, task, now=None, *, _native_admission=None, _logical_binding=None,
                 _coding_assignment=None):
        current_clock = now is None
        now = time.time() if now is None else now
        task = copy.deepcopy(task)
        if task.get('direct_worker') is not None and (
                _coding_assignment is None or task['direct_worker'] is not True):
            raise ValueError('direct worker route requires an accepted native assignment')
        if any(key in task for key in ('origin', 'deliveries', 'coordinator_delivery', 'operator_archive', 'reservation_retirement',
                                        'native_transport', 'transport', 'transport_profile', 'bot_id', 'chat_type',
                                        'logical_contract', 'native_admission_sha256', '_native_admission_replayed', 'native_launch_sha256', 'send_request_sha256',
                                        'second_renewal_start_sha256')):
            raise ValueError('origin and delivery fields are native-only')
        if task.get('task') == '_owner_grants':
            raise ValueError('reserved task name')
        required = ('task', 'scope', 'identity', 'owner', 'gate', 'action', 'artifact', 'deadline', 'wake_budget')
        if any(not task.get(key) for key in required):
            raise ValueError('explicit scope, identity, owner, gate, action, evidence and budget required')
        identity = task['identity']
        fields = {'home', 'profile', 'session', 'session_key', 'platform', 'chat_id', 'user_id', 'thread_id'}
        if set(identity) != fields or any(not identity[k] for k in fields - {'thread_id'}):
            raise ValueError('incomplete target identity')
        if identity['platform'] not in {'telegram', 'cli'} or identity['profile'] not in delivery.PROFILES:
            raise ValueError('unsupported responsible Main')
        if str(Path(identity['home']).resolve()) != identity['home'] or not Path(task['artifact']).is_absolute():
            raise ValueError('canonical absolute home and artifact required')
        for field in ('deadline', 'hard_stop', 'authorization_expires_at'):
            value = task.get(field)
            if value is not None and (type(value) not in (int, float) or not math.isfinite(value) or value <= now):
                raise ValueError('finite future execution and authorization bounds required')
        if (task['deadline'] <= now or type(task['wake_budget']) is not int or
                not 0 < task['wake_budget'] <= 10):
            raise ValueError('invalid bounded budget')
        if Path(task['artifact']).exists():
            raise ValueError('worker artifact must be a new task-specific path')
        if _coding_assignment is None:
            with self.transaction(read_only=True) as state:
                batch = state.grants.get('batches', {}).get(task.get('owner_batch'))
                if batch and batch.get('flow_version') == 3:
                    raise ValueError('result-flow batch requires accepted native Coding assignment')
            native_transport = delivery.native_transport(identity)
        else:
            with self.transaction(read_only=True) as state:
                batch = state.grants.get('batches', {}).get(task.get('owner_batch'))
                assignment = (batch or {}).get('item', {}).get('coding_assignment', {})
                if (not batch or batch.get('flow_version') != 3 or batch.get('revoked') or
                        batch.get('coordinator_identity') != identity or
                        assignment.get('id') != _coding_assignment or
                        assignment.get('status') != 'accepted' or
                        not batch.get('native_transport')):
                    raise ValueError('accepted native Coding assignment required')
                native_transport = copy.deepcopy(batch['native_transport'])
        with self.transaction(read_only=True) as state:
            revision = object_hash(dict(rows=dict(state), grants=state.grants))
            observed = copy.deepcopy(dict(state))
        if any(Path(a['artifact']).resolve() == Path(task['artifact']).resolve()
               for r in observed.values() for a in [r] + r.get('history', []) + r.get('attempt_history', [])):
            raise ValueError('artifact path already bound to another task; never reuse')
        if task['task'] in observed or any(r['identity'] == identity and ownership_pending(r) for r in observed.values()):
            raise ValueError('task already registered or session owned; explicitly supersede first')
        target = task.get('visible_binding', {}).get('target')
        if target and any(target_reserved(r, target) for r in observed.values()):
            raise ValueError('visible target already owned or prior exit unreconciled')
        worktree = task.get('worker_route', {}).get('worktree')
        if worktree and any(worktree_reserved(r, worktree) for r in observed.values()):
            raise ValueError('candidate worktree still owned or prior effects unreconciled')
        with self.transaction() as state:
            if object_hash(dict(rows=dict(state), grants=state.grants)) != revision:
                raise ValueError('registration state changed during resource verification')
            if _coding_assignment is not None:
                batch = state.grants.get('batches', {}).get(task.get('owner_batch'))
                item = (batch or {}).get('item', {})
                assignment = item.get('coding_assignment', {})
                if (not batch or batch.get('revoked') or
                        assignment.get('id') != _coding_assignment or
                        assignment.get('status') != 'accepted' or
                        batch.get('native_transport') != native_transport):
                    raise ValueError('Coding assignment changed before registration')
                if (assignment.get('correction_of') is not None and
                        not delivery.canonical_plain_correction(
                            task['owner_batch'], batch, item, state, now)):
                    raise ValueError('Coding correction changed before registration')
                if (delivery.recovered_preparation_origin(
                        item, state, now, bid=task['owner_batch'], batch=batch) and
                        (item.get('assignment_renewals') or
                         len(item.get('coding_assignment_history') or []) > 1 or
                         not assignment.get('preparation_recovery_of'))):
                    from hermes_cli import diggr_item
                    _, contract = diggr_item.bound(state, identity, task['owner_batch'], task['owner_issue'])
                    if (not delivery.recovery_renewal_effect_free(
                            state, task['owner_batch'], batch, contract, _coding_assignment, sent=True) or
                            not diggr_item.worktree_matches(contract, item)):
                        raise ValueError('recovered Coding renewal changed before registration')
                    diggr_item.assignment_bound_prompt(task['owner_batch'], batch, contract, row=task)
            now = time.time() if current_clock else now
            if _coding_assignment is not None and now >= coding_assignment_deadline(
                    batch, state, now, bid=task['owner_batch']):
                raise ValueError('Coding assignment deadline expired before registration')
            if any(task.get(k) is not None and now >= task[k]
                   for k in ('deadline', 'hard_stop', 'authorization_expires_at')):
                raise ValueError('registration bounds expired during resource verification')
            if Path(task['artifact']).exists() or Path(task['artifact']).is_symlink():
                raise ValueError('worker artifact appeared during resource verification')
            policy = owner_policy.bind(state, task, now, logical_binding=_logical_binding)
            if _coding_assignment is not None:
                now = time.time()
                if (now >= coding_assignment_deadline(batch, state, now, bid=task['owner_batch']) or
                        any(task.get(k) is not None and now >= task[k]
                            for k in ('deadline', 'hard_stop', 'authorization_expires_at'))):
                    raise ValueError('Coding assignment deadline expired before registration')
            state[task['task']] = dict(task, policy=policy, native_transport=native_transport, hard_stop=policy['deadline'],
                authorization_expires_at=policy['deadline'], worker_action=task['action'], generation=1, status='running', wakes=0,
                due=now, lease=0, evidence=None, registered_at=now, schema_version=2,
                epoch=1, epoch_wakes=0, recovery_attempt=0, progress_fingerprint=None,
                effect_status='unknown', checkpoint=None, action_id=uuid.uuid4().hex,
                effect_id=uuid.uuid4().hex, attempt_history=[],
                epoch_seconds=max(1, min(3600, task['deadline'] - now)))
            if policy.get('item_lifecycle'):
                state[task['task']].update(hard_stop=None, authorization_expires_at=None)
            if _logical_binding is not None:
                todo = next(t for t in state.grants['batches'][task['owner_batch']]['manifest']['todos']
                            if owner_policy.issue_key(t) == task['owner_issue'])
                state[task['task']]['logical_contract'] = copy.deepcopy(todo['contract'])
            if _native_admission is not None:
                state[task['task']]['native_admission_sha256'] = _native_admission
            state[task['task']]['origin'] = delivery.origin_for(state, state[task['task']])
            if _coding_assignment is not None:
                state.grants['batches'][task['owner_batch']]['item']['coding_assignment'].update(
                    status='registered', task=task['task'])
            self.normalize(state[task['task']])
            if _coding_assignment is not None and task.get('assignment_renewal_sha256'):
                state[task['task']]['second_renewal_start_sha256'] = assigned_start_core(
                    state.grants['batches'][task['owner_batch']], state[task['task']])

    @staticmethod
    def normalize(row):
        # Add fields only; never revive historic terminal records.
        row.setdefault('schema_version', 2)
        row.setdefault('epoch', 1)
        row.setdefault('epoch_wakes', row.get('wakes', 0))
        row.setdefault('recovery_attempt', 0)
        row.setdefault('progress_fingerprint', None)
        row.setdefault('effect_status', 'unknown')
        row.setdefault('checkpoint', None)
        row.setdefault('action_id', None)
        row.setdefault('effect_id', None)
        row.setdefault('attempt_history', [])
        row.setdefault('epoch_seconds', max(1, min(3600, row['deadline'] - row.get('registered_at', row['deadline'] - 300))))

    @staticmethod
    def checkpoint(row):
        row['checkpoint'] = {key: row.get(key) for key in (
            'scope', 'task', 'gate', 'action', 'owner', 'identity', 'authorization',
            'generation', 'epoch', 'epoch_wakes', 'wakes', 'evidence',
            'progress_fingerprint', 'effect_status', 'recovery_attempt', 'due',
            'action_id', 'effect_id', 'worker_route', 'visible_binding', 'worker_identity',
            'launcher_identity', 'visible_shell_identity')}

    @classmethod
    def renew(cls, row, now):
        cls.normalize(row)
        if row['status'] in TERMINAL:
            return False
        if not owner_policy.allowed(row, now):
            cls.checkpoint(row)
            row.update(status='paused', reason='owner policy missing, revoked or exhausted',
                       limit_generation=row['generation'], generation=row['generation'] + 1)
            return False
        if row['policy'].get('item_lifecycle'):
            # The item survives idle time. Only the coordination lease rolls;
            # claim deadlines and actual consumed work remain bounded separately.
            row['policy']['deadline'] = now + row['epoch_seconds']
        for field in ('hard_stop', 'authorization_expires_at'):
            if row.get(field) is not None and now >= row[field]:
                cls.checkpoint(row)
                row.update(status='paused', reason=field, limit_generation=row['generation'],
                           generation=row['generation'] + 1)
                return False
        if now >= row['deadline']:
            # A repaired failure still reconciles the original worker attempt.
            # Rolling only its coordination lease must not replace that proof.
            if not (row['gate'] == 'reconcile' and
                    (row.get('failed_reconciliations') or row.get('correction_reinspection') or
                     row.get('budget_reconciliation', {}).get('generation') == row['generation'])):
                cls.checkpoint(row)
            row.update(epoch=row['epoch'] + 1, epoch_wakes=0,
                       deadline=min(now + row['epoch_seconds'], row['policy']['deadline']))
        return True

    def get(self, task):
        with self.transaction() as state:
            return state.get(task)

    def blocks(self, identity):
        with self.transaction() as state:
            return any(r['identity'] == identity and r['status'] not in TERMINAL for r in state.values())

    def control(self, identity, status):
        if status not in {'paused', 'cancelled', 'superseded'}:
            raise ValueError('invalid control')
        with self.transaction() as state:
            owner_policy.stop(state, identity)
            for row in state.values():
                if (owner_policy.stable_owner(row['identity']) == owner_policy.stable_owner(identity)
                        and row['status'] not in TERMINAL):
                    row.update(status=status, limit_generation=row['generation'], generation=row['generation'] + 1)

    @staticmethod
    def evidence_matches(row, evidence):
        if not isinstance(evidence, dict):
            return False
        if any(evidence.get(k) != row[k] for k in ('task', 'generation', 'action', 'artifact')):
            return False
        try:
            path = Path(row['artifact'])
            return path.is_file() and hashlib.sha256(path.read_bytes()).hexdigest() == evidence.get('sha256')
        except OSError:
            return False

    def observe(self, task, generation, outcome, evidence=None, now=None, *, process_snapshot=None):
        now = time.time() if now is None else now
        with self.transaction() as state:
            row = state[task]
            worker_result = row.get('worker_result')
            known_exit = (isinstance(worker_result, dict) and
                          worker_result.get('generation') == generation and
                          type(worker_result.get('exit_code')) is int and
                          all(worker_result.get(k) == row.get(k) for k in
                              ('task', 'artifact', 'action_id', 'effect_id')))
            if known_exit:
                evidence = worker_result.get('evidence')
            if row['status'] in TERMINAL and row.get('limit_generation') == generation:
                if (outcome == 'completed' and self.evidence_matches(dict(row, generation=generation), evidence)):
                    row['late_evidence'] = evidence
                    return False  # retain only; never Done, renewal or dispatch
                return False
            if row['generation'] != generation or row['status'] != 'running':
                return False
            if process_snapshot is not None and process_observation_key(row) != process_snapshot:
                return False  # Send/claim advanced since the observer copied the row.
            if not self.renew(row, now):
                if outcome == 'completed' and self.evidence_matches(dict(row, generation=generation), evidence):
                    row['late_evidence'] = evidence
                return False
            if outcome == 'running':
                return True
            if outcome not in {'completed', 'unknown'}:
                raise ValueError('unknown outcome')
            valid = (known_exit or
                     (self.evidence_matches(row, evidence) and evidence.get('result') == 'ready_for_main_review'
                      and evidence.get('exit_code', 0) == 0))
            valid = valid and worker_operational_exit(row, generation)
            if not valid:
                row['evidence'] = evidence
                self.schedule_recovery(row, now, 'unknown or invalid worker outcome')
                return True
            self.checkpoint(row)
            row.update(owner=delivery.coordinator(row['identity']), gate='main_validation',
                       action='validate coding evidence',
                       evidence=evidence, generation=generation + 1, status='pending', due=now)
            return True

    def record_worker_result(self, attempt, exit_code, *, work_seconds=0):
        """Persist a native wait result before any coordinator classification.

        The wrapper's actual wait supplies the exit; a file alone never does.
        A stopped or expired attempt may retain its result without new authority.
        """
        if type(exit_code) is not int:
            raise ValueError('actual worker exit code required')
        if type(work_seconds) not in (int, float) or not math.isfinite(work_seconds) or work_seconds < 0:
            raise ValueError('finite nonnegative native work duration required')
        evidence = None
        path = Path(attempt['artifact'])
        artifact_status = 'missing'
        try:
            if (path.is_file() and not path.is_symlink() and
                    path.stat().st_mtime_ns >= attempt['worker_started_ns']):
                evidence = {k: attempt[k] for k in ('task', 'generation', 'action', 'artifact')}
                evidence.update(sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                                result='unclassified', exit_code=exit_code)
                artifact_status = 'present'
        except OSError:
            artifact_status = 'unavailable'
        result = {k: attempt[k] for k in
                  ('task', 'generation', 'action', 'artifact', 'action_id', 'effect_id')}
        result.update(exit_code=exit_code, observed_at=time.time(),
                      artifact_status=artifact_status, result='unclassified', evidence=evidence)
        with self.transaction() as rows:
            row = rows[attempt['task']]
            if any(row.get(k) != attempt.get(k) for k in
                   ('worker_pid', 'worker_identity', 'action_id', 'effect_id', 'artifact')):
                raise ValueError('worker result belongs to a different attempt')
            if requires_worker_supervision(row):
                require_worker_operational_exit(row, attempt['generation'])
                if exit_code != row['worker_supervision']['leader_exit_code']:
                    raise ValueError('worker result differs from supervised leader exit')
            previous = row.get('worker_result')
            if previous:
                if any(previous.get(k) != result.get(k) for k in result if k != 'observed_at'):
                    raise ValueError('conflicting worker result')
                return previous
            row.update(child_exited=True, worker_result=result)
            # Item time is charged by the supervisor, including its Unknown path.
            # Non-Item workers have no cumulative Item allowance.
        return result

    def tick(self, identity, busy=False, now=None):
        now = time.time() if now is None else now
        with self.transaction() as state:
            for row in state.values():
                if row['identity'] != identity or row['status'] in TERMINAL:
                    continue
                if not self.renew(row, now):
                    continue
                if (busy and row['status'] == 'executing' and now < row['lease'] and
                        row.get('coordinator_turns', {}).get(str(row['generation']), {}).get('status') == 'accepted'):
                    # Native watcher still observes this accepted coordinator turn.
                    # Never revive an expired/finished turn or extend owner bounds.
                    row['lease'] = min(now + 60, row['deadline'], row['policy']['deadline'])
                if busy or row['status'] == 'running':
                    continue
                if row['status'] in {'queued', 'executing'}:
                    if now < row['lease']:
                        continue
                    turn = row.get('coordinator_turns', {}).get(str(row['generation']), {})
                    if turn.get('status') == 'response_produced' and not turn.get('failed'):
                        # Delivery of a final answer is not a lifecycle receipt,
                        # but proves the coordinator finished. Do not classify
                        # this as a crash and spend another wake on the same gap.
                        self.checkpoint(row)
                        row.update(status='blocked',
                                   reason='coordinator answered without lifecycle transition',
                                   limit_generation=row['generation'], generation=row['generation'] + 1)
                        continue
                    if row.get('direct_worker') is True:
                        if row['status'] == 'queued' and not turn:
                            # No Main turn accepted this offer. Fence the old wake
                            # generation before a bounded redelivery of the result.
                            self.checkpoint(row)
                            row.update(status='pending', generation=row['generation'] + 1,
                                       due=now, lease=0)
                        else:
                            self.checkpoint(row)
                            row.update(status='blocked', reason='Main turn effect needs readback',
                                       limit_generation=row['generation'],
                                       generation=row['generation'] + 1)
                            continue
                    else:
                        self.schedule_recovery(row, row['lease'], 'expired Main lease')
                if now < row['due']:
                    continue
                if (row.get('direct_worker') is not True and
                        row['policy']['wakes'] >= row['policy'].get('limits', {}).get('wakes', 10)):
                    self.checkpoint(row)
                    row.update(status='paused', reason='total automatic wake budget exhausted',
                               limit_generation=row['generation'], generation=row['generation'] + 1)
                    continue
                if row.get('direct_worker') is True:
                    turns = row.setdefault('result_flow_wakes', {})
                    source = ('review:' + row['review_closeout_wake']
                              if row.get('review_closeout_wake') else
                              'result:' + row['worker_result']['effect_id']
                              if row.get('worker_result') else
                              'generation:' + str(row['generation']))
                    if turns.get(source, 0) >= 3:
                        row.update(status='blocked', reason='result successor delivery attempts exhausted')
                        continue
                    turns[source] = turns.get(source, 0) + 1
                else:
                    row['policy']['wakes'] += 1
                if row['epoch_wakes'] >= row['wake_budget']:
                    self.checkpoint(row)
                    row.update(epoch=row['epoch'] + 1, epoch_wakes=0)
                row.update(status='queued', wakes=row['wakes'] + 1,
                           epoch_wakes=row['epoch_wakes'] + 1,
                           lease=now + min(60, 15 * 2 ** min(row['recovery_attempt'], 2)))
                return dict(row)
        return None

    def begin(self, identity, wake, now=None):
        now = time.time() if now is None else now
        with self.transaction() as state:
            row = state.get(wake.get('task'))
            if not row or row['identity'] != identity or row['generation'] != wake.get('generation'):
                return False
            if row['status'] != 'queued' or not self.renew(row, now):
                return False
            if now >= row['lease']:
                if row.get('direct_worker') is True:
                    return False  # tick fences this undelivered generation before redelivery.
                self.schedule_recovery(row, row['lease'], 'expired Main lease')
                return False
            row.update(status='executing', lease=now + 60)
            row.setdefault('coordinator_turns', {})[str(row['generation'])] = dict(status='accepted', accepted_at=now)
            if row.get('coordinator_delivery', {}).get('generation') == row['generation']:
                row['coordinator_delivery'].update(status='accepted', accepted_at=now)
            return True

    def ack(self, identity, wake, evidence, next_gate, next_action, now=None):
        current_clock = now is None
        now = time.time() if now is None else now
        if next_gate not in {'main_live', 'awaiting_user', 'done'} or not next_action:
            raise ValueError('no shipping or Coding dispatch authority')
        with self.transaction(read_only=True) as state:
            observed = copy.deepcopy(state.get(wake['task']))
        logical_revision = None
        if (observed and observed.get('logical_contract') and observed['identity'] == identity and
                observed['status'] == 'executing' and observed['generation'] == wake['generation']):
            validate_logical_changes(observed)
            logical_revision = object_hash(observed)
        with self.transaction() as state:
            now = time.time() if current_clock else now
            row = state.get(wake['task'])
            if not row or row['identity'] != identity or row['status'] != 'executing' or row['generation'] != wake['generation'] or not self.renew(row, now):
                return False
            if logical_revision is not None and object_hash(row) != logical_revision:
                return False  # Authority/attempt changed while inspecting the actual diff.
            if now >= row['lease']:
                self.schedule_recovery(row, row['lease'], 'expired Main lease')
                return False
            if row['generation'] != wake['generation'] or not self.evidence_matches(row, evidence) or evidence.get('result') != 'validated':
                return False
            if next_gate == 'done' and (row['gate'] != 'main_live' or
                    evidence.get('authorization') != row.get('authorization') or
                    not row.get('authorization') or evidence.get('final_gate') != 'passed'):
                return False
            if (next_gate == 'done' and row.get('logical_contract', {}).get('acceptance', {}).get('owner') == 'user'):
                return False  # No native personal-acceptance proof API exists here; Main cannot substitute it.
            if next_gate == 'done':
                try:
                    final_path = Path(evidence['final_artifact'])
                    final_bytes = final_path.read_bytes()
                    final = json.loads(final_bytes)
                    if (not final_path.is_absolute() or final_path == Path(row['artifact']) or
                            hashlib.sha256(final_bytes).hexdigest() != evidence.get('final_sha256') or
                            any(final.get(k) != row.get(k) for k in ('task', 'generation', 'action', 'authorization')) or
                            final.get('result') != 'passed' or not final.get('checks')):
                        return False
                except (KeyError, ValueError, OSError, TypeError):
                    return False
            if next_gate == 'awaiting_user':
                blocker = evidence.get('blocker', {})
                if (not isinstance(blocker, dict) or blocker.get('category') not in
                        {'credentials', 'identity_authority', 'new_cost', 'destructive_out_of_scope', 'scope_decision'} or
                        not isinstance(blocker.get('evidence'), str) or not blocker['evidence'].strip()):
                    return False
            fingerprint = hashlib.sha256(json.dumps(
                [next_gate, evidence['sha256']], sort_keys=True).encode()).hexdigest()
            if next_gate == 'main_live' and fingerprint == row['progress_fingerprint']:
                self.schedule_recovery(row, now, 'no validated progress')
                return True
            if row.get('coordinator_delivery', {}).get('status') == 'accepted':
                row['coordinator_delivery'].update(status='handled', handled_at=now)
            self.checkpoint(row)
            row.update(progress_fingerprint=fingerprint, recovery_attempt=0)
            row.update(generation=row['generation'] + 1, gate=next_gate, action=next_action,
                       status=next_gate if next_gate in {'done', 'awaiting_user'} else 'pending',
                       due=now + 2, evidence=evidence)
            return True

    @classmethod
    def schedule_recovery(cls, row, now, reason, retry_at=0):
        cls.checkpoint(row)
        if row['gate'] != 'reconcile':
            row['recovery_kind'] = 'review' if row['gate'] in {'main_validation', 'main_live'} else 'technical'
        attempt = row['recovery_attempt'] + 1
        row.update(status='pending', owner=delivery.coordinator(row['identity']), gate='reconcile',
                   generation=row['generation'] + 1, recovery_attempt=attempt,
                   action='read-only reconcile prior effects and process exit; prepare fresh bounded strategy',
                   recovery_reason=reason,
                   due=max(now + min(300, 5 * 2 ** min(attempt, 6)), retry_at))

    def retry(self, identity, wake, report, now=None):
        now = time.time() if now is None else now
        retry_at = report.get('retry_at', 0)
        if (not isinstance(retry_at, (int, float)) or not math.isfinite(retry_at) or
                not report.get('reason') or not report.get('detail')):
            raise ValueError('concrete technical diagnosis and finite reset time required')
        with self.transaction() as tasks:
            row = tasks.get(wake['task'])
            if (not row or row['identity'] != identity or row['status'] != 'executing' or
                    row['generation'] != wake['generation'] or not self.renew(row, now)):
                return False
            if now >= row['lease']:
                self.schedule_recovery(row, row['lease'], 'expired Main lease')
                return False
            self.schedule_recovery(row, now, report, retry_at)
            return True

    def _recovery_row(self, state, identity, wake, now):
        row = state.get(wake.get('task'))
        if (not row or row['identity'] != identity or row['generation'] != wake.get('generation') or
                row['status'] != 'executing' or row['gate'] != 'reconcile' or
                now >= row['lease'] or not self.renew(row, now)):
            raise ValueError('stale recovery generation, owner, lease or bounds')
        return row

    def authorize_maintenance(self, identity, wake, evidence, strategy, expires, now=None):
        """One-use repair capability; native Main only, independent of the lost UI binding.

        No shell commands, dispatch, lifecycle reset, or generic repair privileges.
        The returned nonce delegates only this exact recovery request; every
        bound field is checked again under the same lock before consuming it.
        """
        import secrets
        now = time.time() if now is None else now
        require_native_main(identity)
        with self.transaction() as state:
            row = self._recovery_row(state, identity, wake, now)
            bounds = [now + 60, row['lease'], row['deadline']]
            bounds.extend(row[k] for k in ('hard_stop', 'authorization_expires_at') if row.get(k) is not None)
            if type(expires) not in (int, float) or not math.isfinite(expires) or not now < expires <= min(bounds):
                raise ValueError('maintenance expiry exceeds task lease or 60 seconds')
            if row.get('maintenance'):
                raise ValueError('maintenance already issued; revoke before replacement')
            capability = dict(nonce=secrets.token_hex(32), generation=row['generation'],
                              request_hash=object_hash(dict(evidence=evidence, strategy=strategy)),
                              expires=expires, authorization=row['authorization'])
            row['maintenance'] = capability
            row.setdefault('maintenance_audit', []).append(dict(event='authorized', at=now,
                generation=row['generation'], request_hash=capability['request_hash'], expires=expires))
            return dict(capability)

    def revoke_maintenance(self, identity, wake):
        require_native_main(identity)
        with self.transaction() as state:
            row = state[wake['task']]
            if row['identity'] != identity or row['generation'] != wake['generation']:
                raise ValueError('stale maintenance owner')
            row.pop('maintenance', None)
            row.setdefault('maintenance_audit', []).append(dict(event='revoked', at=time.time()))

    def failure(self, identity, wake, evidence, now=None):
        """Authentic Main blocker for a missing result or an unchanged failed review result."""
        now = time.time() if now is None else now
        require_native_main(identity)
        with self.transaction() as state:
            row = self._recovery_row(state, identity, wake, now)
            report = recovery_report(row, evidence)
            if not report.get('reason'):
                raise ValueError('evidence.reason: concrete failure required')
            if report.get('worker_result') == 'missing':
                if Path(row['artifact']).exists() or Path(row['artifact']).is_symlink():
                    raise ValueError('worker result is not absent')
            elif report.get('worker_result') == 'present':
                if row.get('recovery_kind') != 'review' or not row.get('policy', {}).get('item_lifecycle'):
                    raise ValueError('present worker result requires bound item review reconciliation')
                result = row.get('worker_result') or {}
                if (result.get('artifact_status') != 'present' or
                        any(result.get(k) != row.get(k) for k in ('task', 'artifact', 'action_id', 'effect_id')) or
                        result.get('evidence', {}).get('sha256') is None):
                    raise ValueError('unchanged native worker result required')
                verified_bytes(row['artifact'], result['evidence']['sha256'])
            else:
                raise ValueError('evidence.worker_result: expected missing or bound present')
            row.update(status='blocked', reason=report['reason'], failure_evidence=dict(evidence),
                       generation=row['generation'] + 1)
            row.pop('maintenance', None)
            return True

    def reconcile_failed(self, identity, wake, evidence, now=None):
        """Reopen only native failure reconciliation, never the worker or its ticket."""
        from hermes_cli.diggr_item import live_item, read_evidence
        from hermes_cli.diggr_successor import verify_exit
        now = time.time() if now is None else now
        require_native_main(identity)
        with self.transaction(read_only=True) as rows:
            row = rows.get(wake.get('task'))
            status_only_gap = bool(row and not row.get('failure_evidence') and
                row.get('reason') == 'coordinator answered without lifecycle transition' and
                (row.get('checkpoint') or {}).get('generation') == row.get('generation', 0) - 1 and
                (row.get('coordinator_turns') or {}).get(str(row['generation'] - 1), {}).get('status') == 'response_produced' and
                not any(row.get(k) for k in ('visible_sent', 'send_request_sha256', 'send_claim',
                                              'claim_phase', 'worker_pid',
                                              'worker_identity', 'worker_result')) and row.get('process_id'))
            review_present = bool(row and row.get('recovery_kind') == 'review' and
                                  row.get('policy', {}).get('item_lifecycle') and
                                  row.get('failure_evidence'))
            result = (row or {}).get('worker_result') or {}
            status_only_review_present = bool(row and
                row.get('recovery_kind') == 'review' and row.get('policy', {}).get('item_lifecycle') and
                row.get('reason') == 'coordinator answered without lifecycle transition' and
                row.get('limit_generation') == row.get('generation', 0) - 1 and
                (row.get('checkpoint') or {}).get('generation') == row.get('limit_generation') and
                (row.get('coordinator_turns') or {}).get(str(row.get('limit_generation')), {}).get('status') == 'response_produced' and
                not (row.get('coordinator_turns') or {}).get(str(row.get('limit_generation')), {}).get('failed') and
                result.get('artifact_status') == 'present' and type(result.get('exit_code')) is int and
                isinstance(result.get('evidence'), dict) and result['evidence'].get('sha256') and
                result.get('effect_id') == row.get('effect_id') and
                row.get('correction_reinspection') == result.get('effect_id') and
                row.get('status_only_review_reinspection') != result.get('effect_id') and
                not row.get('maintenance'))
            if status_only_review_present and row.get('failure_evidence'):
                status_only_review_present = historical_failure_carried_forward(row)
            if status_only_review_present:
                review_present = False
            present_review = review_present or status_only_review_present
            status_only_direct_unsent = bool(row and row.get('direct_worker') is True and
                row.get('recovery_kind') == 'technical' and
                row.get('reason') == 'coordinator answered without lifecycle transition' and
                row.get('limit_generation') == row.get('generation', 0) - 1 and
                (row.get('checkpoint') or {}).get('generation') == row.get('limit_generation') and
                (row.get('coordinator_turns') or {}).get(str(row.get('limit_generation')), {}).get('status') == 'response_produced' and
                not (row.get('coordinator_turns') or {}).get(str(row.get('limit_generation')), {}).get('failed') and
                not row.get('status_only_direct_reinspection'))
            if (not row or row['identity'] != identity or row['generation'] != wake.get('generation') or
                    row['status'] != 'blocked' or row['gate'] != 'reconcile' or
                    (row.get('recovery_kind') != 'technical' and not present_review) or
                    (not row.get('failure_evidence') and not status_only_gap and not status_only_review_present) or
                    row.get('reservation_retirement') or row.get('operator_archive')):
                raise ValueError('current native bound reconciliation failure required')
            batch, contract, item = live_item(rows, identity, row['owner_batch'], row['owner_issue'])
            policy = row['policy']
            if (item.get('task') != row['task'] or batch['tasks'].get(row['owner_issue']) != row['task'] or
                    not owner_policy.allowed(row, now) or
                    policy['wakes'] >= policy['limits']['wakes'] or
                    policy['corrections' if present_review else 'recoveries'] >=
                    policy['limits']['corrections' if present_review else 'recoveries'] or
                    (present_review and item.get('phase') != 'correction_needed')):
                raise ValueError('same live item and remaining reconciliation budget required')
            observed = copy.deepcopy(row)
            revision = object_hash(dict(rows=dict(rows), grants=rows.grants))
        # A prior failure is evidence of the blocked transition, not of no effects.
        if not status_only_gap and not status_only_review_present and not status_only_direct_unsent:
            previous = recovery_report(dict(observed, generation=observed['generation'] - 1),
                                       observed['failure_evidence'])
            if previous.get('worker_result') != ('present' if review_present else 'missing') or not previous.get('reason'):
                raise ValueError('bound native worker-result failure required')
            if present_review:
                result = observed.get('worker_result') or {}
                if result.get('artifact_status') != 'present' or not result.get('evidence', {}).get('sha256'):
                    raise ValueError('unchanged native worker result required')
                verified_bytes(observed['artifact'], result['evidence']['sha256'])
        if status_only_review_present:
            if not isinstance(item.get('correction'), dict):
                raise ValueError('current result-bound item correction required')
            correction = read_evidence(item['correction'], contract)
            result = observed['worker_result']
            if (correction.get('result') != 'failed' or not correction.get('findings') or
                    any(correction.get(k) != result.get(k) for k in ('task', 'action_id', 'effect_id')) or
                    correction.get('worker_generation') != result.get('generation') or
                    correction.get('result_sha256') != result['evidence']['sha256'] or
                    any(result.get(k) != observed.get(k) for k in ('task', 'artifact', 'action_id', 'effect_id'))):
                raise ValueError('current result-bound item correction required')
            verified_bytes(observed['artifact'], result['evidence']['sha256'])
        report = recovery_report(observed, evidence)
        read_evidence(evidence, contract)
        if not report.get('reason') or report.get('row_sha256') != object_hash(observed):
            raise ValueError('fresh diagnosis must bind the unchanged failed row')
        if present_review and report.get('worker_result') != 'present':
            raise ValueError('fresh review diagnosis must preserve present worker result')
        if status_only_direct_unsent:
            verify_status_only_direct_unsent(observed, batch, item, report)
        if status_only_review_present:
            result = observed['worker_result']
            result_ref = dict(path=observed['artifact'], sha256=result['evidence']['sha256'])
            if (report.get('outcome') != 'reconciled' or
                    not isinstance(report.get('effect_proof'), dict) or
                    result_ref not in report['observations'] or
                    item['correction'] not in report['observations']):
                raise ValueError('fresh reconciled effects and original result/correction observations required')
            effects = read_hashed_json(report['effect_proof']['path'], report['effect_proof']['sha256'])
            if effects.get('retained_worktree_sha256') != _worktree_snapshot(observed['worker_route']['worktree']):
                raise ValueError('retained worktree differs from reconciled effect proof')
        if status_only_gap:
            route = observed.get('worker_route') or {}
            artifact = Path(observed['artifact'])
            receipt = Path(route['receipt'])
            if (report.get('worker_result') != 'missing' or
                    artifact.exists() or artifact.is_symlink()):
                raise ValueError('status-only gap requires absent worker result')
            # A seal written before the coordinator's final answer is safe to
            # retain only when its exact native hash is still present. An
            # unsealed/changed receipt or any send reservation remains blocked.
            sealed = observed.get('receipt_sha256')
            if receipt.exists() or receipt.is_symlink() or sealed:
                if (receipt.is_symlink() or not receipt.is_file() or not sealed or
                        digest(receipt) != sealed):
                    raise ValueError('status-only gap has unsealed or changed route receipt')
        verify_exit(observed)  # Actual registry/OS proof, never a self-reported exit.
        with self.transaction() as rows:
            if object_hash(dict(rows=dict(rows), grants=rows.grants)) != revision:
                raise ValueError('failed reconciliation state changed during inspection')
            row = rows[wake['task']]
            recovery_report(row, evidence)
            if present_review:
                result = row['worker_result']
                verified_bytes(row['artifact'], result['evidence']['sha256'])
            if status_only_review_present:
                if read_evidence(item['correction'], contract) != correction:
                    raise ValueError('item correction changed during native reinspection')
                effects = read_hashed_json(report['effect_proof']['path'], report['effect_proof']['sha256'])
                if effects.get('retained_worktree_sha256') != _worktree_snapshot(row['worker_route']['worktree']):
                    raise ValueError('retained worktree changed during native reinspection')
            if status_only_direct_unsent:
                verify_status_only_direct_unsent(row, batch, item, report)
            checkpoint = copy.deepcopy(row.get('checkpoint'))
            self.schedule_recovery(row, now, 'native failure reinspection: ' + report['reason'])
            row['checkpoint'] = checkpoint  # Retain the original worker attempt and its evidence.
            if status_only_review_present:
                row['status_only_review_reinspection'] = result['effect_id']
            if status_only_direct_unsent:
                row['status_only_direct_reinspection'] = wake['generation']
            audit = dict(
                from_generation=wake['generation'], generation=row['generation'],
                evidence=dict(evidence), failure_evidence=copy.deepcopy(row.get('failure_evidence')),
                checkpoint=checkpoint, at=now)
            if status_only_review_present:
                audit['kind'] = 'status_only_review_present'
            if status_only_direct_unsent:
                audit['kind'] = 'status_only_direct_unsent'
            row.setdefault('failed_reconciliations', []).append(audit)
            return dict(task=row['task'], generation=row['generation'], status=row['status'],
                        gate=row['gate'], worker_started=False)

    def retire_reservation(self, identity, wake, evidence, now=None):
        """Native Main may discharge terminal ownership, never revive or dispatch."""
        now = time.time() if now is None else now
        require_native_main(identity)
        with self.transaction() as state:
            row = state.get(wake.get('task'))
            if (not row or owner_policy.stable_owner(row['identity']) != owner_policy.stable_owner(identity) or
                    row['identity']['session_key'] != identity['session_key'] or
                    row['generation'] != wake.get('generation') or
                    row['status'] not in TERMINAL or row.get('producer') != 'cmux' or
                    row.get('reservation_retirement')):
                raise ValueError('terminal current owner required; retirement replay refused')
            if prelaunch_rejection_case(row):
                # The persisted response digest was reconstructed from the exact
                # terminal gateway guard return, which occurs before env.execute.
                # This case does not invent a launcher PID or an exit receipt.
                report = read_hashed_json(evidence['path'], evidence['sha256'])
                if report != prelaunch_rejection_report(row):
                    raise ValueError('exact pre-execution rejection evidence required')
                route = row['worker_route']
                if any(Path(path).exists() or Path(path).is_symlink()
                       for path in (row['artifact'], route['receipt'])):
                    raise ValueError('worker artifact or route receipt exists; prelaunch retirement refused')
                before = prelaunch_rejection_core(row)
                row['reservation_retirement'] = dict(
                    kind='gateway_lifecycle_rejection_before_execute',
                    core_sha256=before, evidence=dict(evidence), at=now,
                    generation=row['generation'], identity=dict(row['identity']),
                    retired_by=dict(identity), action_id=row['action_id'],
                    effect_id=row['effect_id'])
                return True
            report = recovery_report(row, evidence)
            before = object_hash(row)
            if (report.get('row_sha256') != before or not row.get('authorization') or
                    report.get('authorization') != row['authorization'] or not report.get('reason') or
                    report.get('outcome') not in {'no_effect', 'reconciled'} or not report.get('effect_proof')):
                raise ValueError('exact authorized row and independently hashed effect reconciliation required')
            from tools.process_registry import process_registry
            if process_registry.get(row.get('process_id')) is None:
                # Only explicit, effect-reconciled retirement may use persisted
                # ownership after registry loss. It never authorizes recovery,
                # dispatch, acceptance or a replacement worker.
                verify_restarted_reservation_exit(row)
            elif row['identity'] != identity:
                raise ValueError('live registry requires original coordinator session')
            elif row.get('worker_pid'):
                verify_prior_exit(row)  # Claimed attempts retain every worker/child safeguard.
            else:
                if any(key in row for key in ('worker_pid', 'worker_identity', 'worker_started_ns',
                                              'child_identity', 'child_exited')):
                    raise ValueError('ambiguous worker or child claim; retirement refused')
                if row.get('visible_sent') is not True:
                    raise ValueError('retirement requires a recorded sent attempt')
                # Terminal status irreversibly fences the ticket. The same lock also
                # serializes send and worker claim, so a delayed wrapper cannot spawn.
                verify_visible_target(row['visible_binding'], idle=True)
                verify_launcher_exit(row)
            recovery_report(row, evidence)  # Detect evidence changes during OS verification.
            row.pop('maintenance', None)
            row['reservation_retirement'] = dict(
                row_sha256=object_hash(row), evidence=dict(evidence),
                request_row_sha256=before, at=now, generation=row['generation'],
                identity=dict(row['identity']), retired_by=dict(identity),
                action_id=row['action_id'], effect_id=row['effect_id'])
            return True

    def recover(self, identity, wake, evidence, strategy, maintenance=None, now=None):
        """Reconcile and reserve a fresh ticket, never execute it. Atomic registry transition."""
        import secrets
        now = time.time() if now is None else now
        evidence, strategy, maintenance = copy.deepcopy((evidence, strategy, maintenance))
        if maintenance is None:
            require_native_main(identity)
        if not isinstance(strategy, dict) or not isinstance(strategy.get('worker_route'), dict):
            raise ValueError('strategy.worker_route: object required')
        if not strategy['worker_route'].get('packet_sha256'):
            raise ValueError('strategy.worker_route.packet_sha256: required')
        if not isinstance(evidence, dict) or not evidence.get('path') or not evidence.get('sha256'):
            raise ValueError('evidence.path and evidence.sha256: required')
        preview = read_hashed_json(evidence['path'], evidence['sha256'])
        if preview.get('outcome') not in {'no_effect', 'reconciled'}:
            raise ValueError('evidence.outcome: expected no_effect or reconciled')
        if preview['outcome'] == 'reconciled' and not isinstance(preview.get('effect_proof'), dict):
            raise ValueError('evidence.effect_proof: separate reconciled-effects proof required')
        with self.transaction() as state:
            row = self._recovery_row(state, identity, wake, now)
            authority = row.get('maintenance')
            if authority or maintenance is not None:
                if (not authority or maintenance != authority or now >= authority['expires'] or
                        authority['generation'] != row['generation'] or
                        authority['authorization'] != row['authorization'] or
                        authority['request_hash'] != object_hash(dict(evidence=evidence, strategy=strategy))):
                    raise ValueError('revoked, expired or changed maintenance authorization')
            kind = strategy.get('kind')
            if kind not in {'technical', 'review'} or kind != row.get('recovery_kind'):
                raise ValueError('explicit technical or review recovery kind required')
            counter = 'recoveries' if kind == 'technical' else 'corrections'
            if row['policy'][counter] >= row['policy'].get('limits', {}).get(counter, 2):
                raise ValueError('total owner recovery/correction budget exhausted')
            # Freeze every reservation and the grant set, not just this task:
            # another task may acquire the replacement surface while we inspect.
            snapshot = copy.deepcopy(state)
            observed = copy.deepcopy(dict(state))
        state = observed
        row = state[wake['task']]
        report = recovery_report(row, evidence)
        if report.get('outcome') not in {'no_effect', 'reconciled'}:
            raise ValueError('prior effects unknown; recovery refused')
        if report['outcome'] == 'reconciled' and not report.get('effect_proof'):
            raise ValueError('reconciled effects require proof')
        from tools.process_registry import process_registry
        if process_registry.get(row.get('process_id')) is None:
            verify_restarted_reservation_exit(row)
        else:
            verify_prior_exit(row, allow_idle_unclaimed=True)
        worker_action = row.get('worker_action')
        checkpoint = row.get('checkpoint') or {}
        if not worker_action and checkpoint.get('gate') == 'coding':
            worker_action = checkpoint.get('action')
        if not worker_action or strategy.get('action', worker_action) != worker_action:
            raise ValueError('original authorized worker action required; changed action refused')
        if not strategy.get('reason') or not strategy.get('hypothesis'):
            raise ValueError('reason and changed hypothesis required')
        if any(h.get('hypothesis') == strategy['hypothesis'] for h in row.get('history', [])):
            raise ValueError('unchanged hypothesis; no identical recovery retries')
        route = dict(strategy['worker_route'])
        artifact = canonical_path(strategy['artifact'])
        old_route = row['worker_route']
        old_packet = read_hashed_json(old_route['packet'], old_route['packet_sha256'])
        packet = read_hashed_json(route['packet'], route['packet_sha256'])
        provider_correction = (kind == 'technical' and
            native_provider_correction(row, report, old_packet))
        if kind == 'technical' and 'argv' not in route:
            # Derive the entire technical argv from the sealed attempt. The
            # historical provider repair is admitted only by the native state
            # and effect/exit checks above, never by a request flag.
            route['argv'] = [artifact if word == row['artifact'] else word for word in old_route['argv']]
            if provider_correction:
                route['argv'] = corrected_provider_argv(route['argv'])
        item_correction = row.get('policy', {}).get('item_lifecycle') and kind == 'review'
        followup = None
        if item_correction:
            validate_logical_changes(row)
            with self.transaction(read_only=True) as current_state:
                current_item = current_state.grants['batches'][row['policy']['batch']]['item']
                followup = copy.deepcopy(current_item.get('followup_preparation'))
            if followup and (followup.get('phase') != 'prepared' or
                    any(route.get(k) != followup[k] for k in ('worktree', 'branch', 'plane_id')) or
                    packet.get('base') != followup['base']):
                raise ValueError('followup route differs from the same item preparation')
            if (packet.get('retained_worktree_sha256') != _worktree_snapshot(route['worktree'])):
                raise ValueError('correction must preserve the exact inspected current worktree')
        validate_recovery_route(old_route, route, old_packet, packet, row['artifact'], artifact, kind,
                                retained_correction=item_correction, followup=followup,
                                _provider_correction=_NATIVE_PROVIDER_CORRECTION if provider_correction else None)
        if row.get('logical_contract'):
            # Launcher path is not used by recovery, but keep the stored canonical binding accurate.
            logical_task = dict(row, worker_route=route, artifact=artifact,
                launcher_command=logical_launcher_command(row['logical_contract'], route))
            validate_logical_route(logical_task, row['logical_contract'], fresh=False)
        verify_recovery_worktree(route, packet)
        old_binding = row['visible_binding']
        target = route['visible_target']
        topology_hash = None
        if target != old_binding['target']:
            topology = cmux_tree()
            topology_hash = object_hash(topology)
            if exact_target_present(topology, old_binding['target']):
                raise ValueError('old exact target still present; migration refused')
            if not process_absent(old_binding['shell']):
                raise ValueError('prior shell alive or PID reused')
        binding = bind_visible_target(target)
        # A fresh bounded attempt may reuse the same verified idle target.
        # Changed wording is never authority; effect/exit/route checks above are.
        if binding != old_binding and not process_absent(old_binding['shell']):
            raise ValueError('prior shell exit not proven')
        verify_visible_target(binding, idle=True)
        coding_prompt_binding = (bound_coding_prompt(preflight_coding_coordinator())
                                 if row.get('logical_contract') else row.get('coding_prompt_binding'))
        for other in state.values():
            if other['task'] != row['task'] and target_reserved(other, target):
                raise ValueError('replacement surface already owned or prior exit unreconciled')
            if other['task'] != row['task'] and worktree_reserved(other, route['worktree']):
                raise ValueError('candidate worktree still owned or prior effects unreconciled')
        paths = [artifact, canonical_path(route['packet']), canonical_path(route['receipt'])]
        used = set()
        for other in state.values():
            for attempt in [other] + other.get('history', []) + other.get('attempt_history', []):
                used.add(attempt['artifact'])
                used.update(attempt.get('worker_route', {}).get(k) for k in ('packet', 'receipt'))
        if len(set(paths)) != len(paths) or any(p in used for p in paths):
            raise ValueError('reused recovery paths')
        if any(Path(p).exists() or Path(p).is_symlink() for p in (artifact, route['receipt'])):
            raise ValueError('fresh worker artifact and unsealed receipt required')
        # Preserve the current native preflight before minting any new authority.
        if packet.get('routing_mode') == 'non_cmm':
            fresh = dict(row, worker_route=route, artifact=artifact, visible_binding=binding)
            if row.get('logical_contract'):
                fresh['launcher_command'] = logical_launcher_command(row['logical_contract'], route)
            preflight_non_cmm(fresh, route)
        shell_identity = bound_shell_identity(binding)
        with self.transaction() as state:
            if not recovery_snapshot_matches(snapshot, state):
                raise ValueError('recovery state changed during external verification')
            row = state[wake['task']]
            commit_now = max(now, time.time())
            self._recovery_row(state, identity, wake, commit_now)
            if maintenance is None:
                require_native_main(identity)
            recovery_report(row, evidence)  # Evidence must still bind the observed attempt.
            if provider_correction:
                native_provider_correction(row, report, old_packet)
            read_hashed_json(old_route['packet'], old_route['packet_sha256'])
            read_hashed_json(route['packet'], route['packet_sha256'])
            if kind == 'review' and packet.get('coding_prompt_source') is not None:
                if verified_bytes(packet['coding_prompt_source'], packet['coding_prompt_sha256']).decode('utf-8') != route['argv'][-1]:
                    raise ValueError('packet.coding_prompt_source: changed before recovery reservation')
            if any(Path(p).exists() or Path(p).is_symlink() for p in (artifact, route['receipt'])):
                raise ValueError('recovery paths changed during external verification')
            if authority and commit_now >= authority['expires']:
                raise ValueError('maintenance expired during verification')
            # Exclusive durable reservation before state commit. A crash leaves an
            # orphan reservation and fails closed; it never permits a duplicate send.
            reservation = Path(artifact + '.reservation')
            with reservation.open('x', encoding='utf-8') as out:
                json.dump(dict(task=row['task'], generation=row['generation'] + 1,
                               request_hash=object_hash(strategy)), out)
                out.flush(); os.fsync(out.fileno())
            directory = os.open(reservation.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
            self.checkpoint(row)
            row['checkpoint'].update(effect_status=report['outcome'],
                reconciliation_path=evidence['path'], reconciliation_sha256=evidence['sha256'])
            snapshot = {k: v for k, v in row.items() if k not in {'history', 'attempt_history', 'checkpoint', 'maintenance', 'maintenance_audit'}}
            snapshot.update(reconciliation=dict(evidence), hypothesis=strategy['hypothesis'],
                            recovery_reason=strategy['reason'], recovered_at=now,
                            topology_sha256=topology_hash, replacement_binding_sha256=object_hash(binding))
            row.setdefault('history', []).append(snapshot)
            row.setdefault('attempt_history', []).append(dict(artifact=row['artifact'],
                worker_route=old_route, action_id=row['action_id'], effect_id=row['effect_id'],
                reconciliation_sha256=evidence['sha256'], effect_status=report['outcome']))
            for key in ('worker_pid', 'worker_identity', 'worker_started_ns', 'child_identity', 'child_exited',
                        'visible_sent', 'visible_sent_at', 'send_request_sha256', 'receipt_sha256', 'non_cmm_receipt_sha256', 'maintenance',
                        'worker_result', 'worker_descendants', 'worker_supervision'):
                row.pop(key, None)
            if row.get('logical_contract'):
                row['launcher_command'] = logical_launcher_command(row['logical_contract'], route)
            row['policy'][counter] += 1
            if row['policy'].get('item_lifecycle'):
                item = state.grants['batches'][row['policy']['batch']]['item']
                item.setdefault('candidate_history', []).append({k: item[k] for k in
                    ('candidate', 'acceptance', 'pr', 'review', 'worktree', 'branch', 'base') if k in item})
                if followup:
                    item.update({k: followup[k] for k in ('worktree', 'branch', 'base', 'target')})
                    item.pop('followup_preparation', None)
                for key in ('candidate', 'acceptance', 'test_delegation', 'test_result', 'pr', 'review'):
                    item.pop(key, None)
                item['phase'] = 'coding'
            row.update(generation=row['generation'] + 1, ticket_nonce=secrets.token_hex(32),
                       action_id=secrets.token_hex(16), effect_id=secrets.token_hex(16),
                       worker_route=route, artifact=artifact, visible_binding=binding,
                       visible_shell_identity=shell_identity,
                       status='running', owner='coding', gate='coding', action=worker_action, worker_action=worker_action,
                       evidence=None, effect_status='unknown', registered_at=now, due=now, lease=0,
                       claim_phase='reserved', claim_deadline=claim_deadline(row, commit_now, 60),
                       coding_prompt_binding=coding_prompt_binding)
            row.pop('coding_prompt_receipt', None)
            row.setdefault('maintenance_audit', []).append(dict(event='recovered', at=now,
                generation=row['generation'], request_hash=object_hash(strategy)))
            return worker_ticket(row)


def verified_bytes(path, sha256):
    candidate = Path(path)
    if not candidate.is_absolute() or candidate.is_symlink() or str(candidate.resolve()) != str(candidate):
        raise ValueError('canonical non-symlink evidence required')
    data = candidate.read_bytes()
    if hashlib.sha256(data).hexdigest() != sha256:
        raise ValueError('evidence hash mismatch')
    return data


def launcher_identity(session):
    """Capture native local ownership while the launcher handle is available.

    A short-lived child may already be reaped: its waitable handle still proves
    exit. A registry flag or a bare PID, including a recovered legacy PID, does
    not supply that proof. Never inspect a sandbox PID as a host PID.
    """
    import psutil
    if session.pid_scope != 'host' or not session.pid:
        return None
    handle = session.process or session._pty
    if handle is None or handle.pid != session.pid:
        return None

    def exited():
        return handle.poll() is not None if session.process is not None else not handle.isalive()

    proof = dict(process_id=session.id, session_key=session.session_key,
                 started_at=session.started_at, pid=session.pid)
    if exited():
        return dict(proof, exited=True)
    import subprocess
    try:
        native = process_identity(session.pid)
        created = psutil.Process(session.pid).create_time()
    except (psutil.NoSuchProcess, psutil.AccessDenied, OSError, ValueError, subprocess.SubprocessError):
        return dict(proof, exited=True) if exited() else None
    if (not isinstance(native, dict) or native.get('pid') != session.pid or
            not native.get('start') or not native.get('executable')):
        return dict(proof, exited=True) if exited() else None
    # Recheck the owning handle: don't capture a recycled PID after reaping.
    return (dict(proof, exited=True) if exited() else
            dict(proof, created=created, start=native['start'], executable=native['executable']))


def identity_exited(binding):
    if (not isinstance(binding, dict) or type(binding.get('pid')) is not int or binding['pid'] <= 0):
        return False
    if 'start' in binding or 'executable' in binding:
        if any(not isinstance(binding.get(key), str) or not binding[key] for key in ('start', 'executable')):
            return False
        return process_absent(binding, original_process_only=True)
    if (type(binding.get('created')) not in (float, int) or
            not math.isfinite(binding['created']) or binding['created'] <= 0):
        return False
    # Public psutil create_time can shift with boot-time clock adjustment across
    # processes/restarts. A living legacy PID is never exit evidence, even when
    # the recorded created value differs. Only absence can retire that identity.
    return pid_is_absent(binding['pid'])


def prior_process_exited(row):
    binding = row.get('worker_identity')
    if row.get('worker_pid') and (not binding or binding.get('pid') != row['worker_pid'] or
                                  not (process_absent(binding) if binding.get('start') else identity_exited(binding))):
        return False  # Legacy PID alone is not identity proof.
    process_id = row.get('process_id')
    if row.get('launcher_expected') and not process_id:
        return False
    if process_id:
        from tools.process_registry import process_registry
        session = process_registry.get(process_id)
        if session is not None:
            if (session.session_key != row['identity']['session_key'] or
                    session.started_at != row['process_started_at']):
                return False
            process_registry._reconcile_local_exit(session)
        proof = row.get('launcher_identity')
        if proof:
            if (proof.get('process_id') != process_id or
                    proof.get('session_key') != row['identity']['session_key'] or
                    proof.get('started_at') != row['process_started_at']):
                return False
            if proof.get('exited') is not True and not identity_exited(proof):
                return False
        elif session is None or (launcher_identity(session) or {}).get('exited') is not True:
            # Native stdout EOF may set session.exited while the child lives.
            # Legacy recovery needs the matching local handle's actual exit.
            return False
    # If no worker claimed, the old generation has already been fenced by
    # observe(). Any delayed terminal command can no longer acquire a claim.
    return True


# Native event binding: tools inherit this through the existing executor context.
from contextvars import ContextVar
EVENT_CONTEXT = ContextVar('diggr_continuation_event', default=None)


def object_hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def canonical_path(value):
    path = Path(value)
    if not path.is_absolute() or str(path.resolve()) != str(path) or path.is_symlink():
        raise ValueError('canonical absolute non-symlink path required')
    return str(path)


def read_hashed_json(path, sha256):
    path = Path(canonical_path(path))
    data = path.read_bytes()
    if hashlib.sha256(data).hexdigest() != sha256:
        raise ValueError('evidence or packet hash mismatch')
    result = json.loads(data)
    if not isinstance(result, dict):
        raise ValueError('JSON object required')
    return result


def require_native_main(identity):
    context = EVENT_CONTEXT.get()
    if not context or context.get('identity') != identity or identity.get('profile') not in delivery.PROFILES:
        raise ValueError('recovery requires originating native Main event authority')


def historical_failure_carried_forward(row):
    """Recognize a still-hashed failure from a reconciled prior attempt only."""
    ref = row['failure_evidence']
    failure = read_hashed_json(ref['path'], ref['sha256'])
    if (failure.get('task') == row.get('task') and
            failure.get('generation') == row.get('generation', 0) - 1 and
            all(failure.get(k) == row.get(k) for k in
                ('identity', 'action', 'action_id', 'effect_id'))):
        return False  # Current failure must use the ordinary bound path.
    if (failure.get('task') != row.get('task') or
            failure.get('identity') != row.get('identity') or
            type(failure.get('generation')) is not int or
            failure['generation'] >= row.get('generation', 0) - 1 or
            failure.get('worker_result') != 'missing'):
        raise ValueError('unexplained historical failure evidence')
    result = row['worker_result']
    matches = []
    for prior in row.get('history', []):
        if (prior.get('failure_evidence') != ref or
                prior.get('task') != row['task'] or
                prior.get('identity') != row['identity'] or
                any(prior.get(k) != failure.get(k) for k in
                    ('action', 'action_id', 'effect_id')) or
                type(prior.get('generation')) is not int or
                not failure['generation'] <= prior['generation'] < result['generation'] or
                not isinstance(prior.get('reconciliation'), dict)):
            continue
        recovery = recovery_report(prior, prior['reconciliation'])
        if recovery.get('outcome') not in {'no_effect', 'reconciled'}:
            continue
        if not any(attempt.get('action_id') == prior['action_id'] and
                   attempt.get('effect_id') == prior['effect_id'] and
                   attempt.get('reconciliation_sha256') == prior['reconciliation']['sha256'] and
                   attempt.get('effect_status') == recovery['outcome']
                   for attempt in row.get('attempt_history', [])):
            continue
        if not any(entry.get('failure_evidence') == ref and
                   type(entry.get('generation')) is int and
                   failure['generation'] <= entry['generation'] <= prior['generation']
                   for entry in row.get('failed_reconciliations', [])):
            continue
        matches.append(prior)
    if len(matches) != 1:
        raise ValueError('unexplained historical failure evidence')
    recovery_report(dict(matches[0], generation=failure['generation']), ref)
    return True


def verify_status_only_direct_unsent(row, batch, item, report):
    """Reinspect one answered flow-v3 turn after a proven pre-send failure."""
    generation = row['generation']
    reference = row.get('failure_evidence')
    attempts = row.get('failed_reconciliations') or []
    matching = [entry for entry in attempts if
        entry.get('from_generation') == generation - 2 and
        entry.get('generation') == generation - 1 and
        entry.get('failure_evidence') == reference]
    if (batch.get('flow_version') != 3 or batch.get('revoked') or
            item.get('phase') != 'coding' or item.get('task') != row['task'] or
            batch.get('tasks') != {row['owner_issue']: row['task']} or
            item.get('worktree') != row['worker_route']['worktree'] or
            item.get('branch') != row['worker_route']['branch'] or
            item.get('target') != row['worker_route']['visible_target'] or
            item.get('coding_assignment', {}).get('status') != 'registered' or
            item['coding_assignment'].get('task') != row['task'] or
            len(matching) != 1 or not isinstance(reference, dict) or
            row.get('status_only_direct_reinspection') or
            report.get('worker_result') != 'missing' or report.get('outcome') != 'no_effect' or
            any(row.get(key) for key in ('visible_sent', 'visible_sent_at', 'send_request_sha256',
                'send_claim', 'claim_phase', 'worker_pid', 'worker_identity', 'worker_result',
                'child_identity', 'child_exited', 'process_id', 'receipt_sha256',
                'non_cmm_receipt_sha256'))):
        raise ValueError('direct pre-send status-only proof unavailable')
    audit = matching[0]
    original = audit.get('checkpoint') or {}
    if (original.get('generation') != generation - 4 or
            original.get('task') != row['task'] or
            original.get('identity') != row['identity'] or
            original.get('worker_route') != row['worker_route'] or
            original.get('visible_binding') != row['visible_binding'] or
            original.get('action_id') != row['action_id'] or
            original.get('effect_id') != row['effect_id'] or
            not isinstance(audit.get('evidence'), dict)):
        raise ValueError('original direct-worker attempt not bound')
    failure = recovery_report(dict(row, generation=generation - 3), reference)
    prior = recovery_report(dict(row, generation=generation - 2), audit['evidence'])
    if (not failure.get('reason') or failure.get('worker_result') != 'missing' or
            not prior.get('reason') or prior.get('worker_result') != 'missing' or
            prior.get('outcome') != 'no_effect' or
            not isinstance(prior.get('row_sha256'), str)):
        raise ValueError('prior native no-effect reinspection missing')
    route = row['worker_route']
    packet_ref = dict(path=route['packet'], sha256=route['packet_sha256'])
    observations = prior['observations']
    receipt_refs = [ref for ref in observations if ref.get('path') == route['receipt']]
    snapshots = []
    for ref in observations:
        if ref in (reference, packet_ref) or ref in receipt_refs:
            continue
        try:
            candidate = read_hashed_json(ref['path'], ref['sha256'])
        except (ValueError, OSError, TypeError, KeyError):
            continue
        if isinstance(candidate, dict) and object_hash(candidate) == prior['row_sha256']:
            snapshots.append(candidate)
    if (reference not in observations or packet_ref not in observations or
            len(receipt_refs) != 1 or len(snapshots) != 1 or
            any(ref not in report['observations'] for ref in
                (reference, audit['evidence'], packet_ref, receipt_refs[0]))):
        raise ValueError('historical direct-worker evidence not linked')
    snapshot = snapshots[0]
    if (snapshot.get('generation') != generation - 2 or
            snapshot.get('status') != 'blocked' or snapshot.get('gate') != 'reconcile' or
            snapshot.get('failure_evidence') != reference or
            any(snapshot.get(key) != row.get(key) for key in
                ('task', 'identity', 'authorization', 'owner_batch', 'owner_issue',
                 'action', 'action_id', 'effect_id', 'artifact', 'worker_route',
                 'visible_binding', 'logical_contract')) or
            any(snapshot.get(key) for key in ('visible_sent', 'send_request_sha256',
                'claim_phase', 'worker_pid', 'worker_result', 'process_id'))):
        raise ValueError('prior direct-worker row changed or has effects')
    packet = read_hashed_json(packet_ref['path'], packet_ref['sha256'])
    receipt = read_hashed_json(receipt_refs[0]['path'], receipt_refs[0]['sha256'])
    artifact = Path(row['artifact'])
    actual_root, branch, head, dirty = _git_route_identity(route['worktree'])
    retained = packet.get('retained_worktree_sha256')
    migration = item.get('migration') or {}
    if (artifact.exists() or artifact.is_symlink() or
            actual_root != route['worktree'] or branch != route['branch'] or
            head != packet.get('base') or
            (migration and (head != migration.get('head') or
                retained != migration.get('retained_worktree_sha256'))) or
            ((dirty or retained is not None) and not _retained_worktree_matches(actual_root, packet)) or
            receipt.get('schema_version') != 'diggr.native.non_cmm_route.v2' or
            receipt.get('mode') != 'non_cmm' or receipt.get('verdict') != 'allowed' or
            receipt.get('route_packet') != route['packet'] or
            receipt.get('packet_sha256') != route['packet_sha256'] or
            receipt.get('task') != row['task'] or
            receipt.get('generation') != original['generation'] or
            receipt.get('coding_route_contract') != dict(packet.get('coding_route', {}),
                plane_id=route['plane_id'], operator_scope_authorized=True)):
        raise ValueError('direct-worker route, receipt or retained worktree changed')
    expires = min((row[key] for key in ('hard_stop', 'authorization_expires_at')
                   if row.get(key) is not None), default=None)
    proof = dict(task=row['task'], generation=original['generation'],
                 authorization=row['authorization'], identity=row['identity'],
                 artifact=row['artifact'], argv=route['argv'], binding=row['visible_binding'],
                 packet_sha256=route['packet_sha256'], worktree=actual_root,
                 branch=branch, head=head, expires_at=expires)
    binding_sha256 = hashlib.sha256(json.dumps(proof, sort_keys=True).encode()).hexdigest()
    if receipt.get('expires_at') != expires or receipt.get('binding_sha256') != binding_sha256:
        raise ValueError('unsealed direct-worker receipt is not the original native preflight')


def recovery_report(row, evidence):
    if canonical_path(evidence['path']) == row['artifact']:
        raise ValueError('Main reconciliation must be separate from worker artifact')
    report = read_hashed_json(evidence['path'], evidence['sha256'])
    fields = ('task', 'generation', 'identity', 'action', 'action_id', 'effect_id')
    if any(report.get(k) != row.get(k) for k in fields) or not report.get('observations'):
        raise ValueError('reconciliation not bound to current action, effects and identity')
    if not row.get('action_id') or not row.get('effect_id'):
        raise ValueError('legacy attempt has no effect binding; operator reconciliation required')
    observations = report['observations']
    if not isinstance(observations, list) or not observations:
        raise ValueError('hashed reconciliation observations required')
    for observation in observations:
        if not isinstance(observation, dict):
            raise ValueError('hashed reconciliation observations required')
        verified_bytes(observation['path'], observation['sha256'])
    if report.get('effect_proof'):
        proof = report['effect_proof']
        effects = read_hashed_json(proof['path'], proof['sha256'])
        # Older Main reports use a reconciled proof alongside no_effect.
        # The provider correction below requires the stricter matching result.
        allowed_results = {'reconciled', 'no_effect'} if report.get('outcome') == 'no_effect' else {'reconciled'}
        if (any(effects.get(k) != row[k] for k in ('task', 'action_id', 'effect_id')) or
                effects.get('result') not in allowed_results):
            raise ValueError('effect proof must bind the prior action and effect')
    return report


def corrected_provider_argv(argv):
    """Replace only the one historical Codex CLI provider override."""
    old = 'model_provider="openai-codex"'
    if (not isinstance(argv, list) or argv.count(old) != 1 or
            any(isinstance(word, str) and word.startswith('model_provider=') and word != old
                for word in argv)):
        raise ValueError('historical provider override must be unique')
    index = argv.index(old)
    if index == 0 or argv[index - 1] != '-c':
        raise ValueError('historical provider override must be a Codex config argument')
    result = list(argv)
    result[index] = 'model_provider="openai"'
    return result


def native_provider_correction(row, report, old_packet):
    """Recognize one obsolete sealed CLI spelling after native exit verification.

    A bare unclaimed state or report outcome never authorizes migration. The
    effect proof is separate, hashed, action-bound native Main evidence.
    """
    model = (row.get('logical_contract') or {}).get('model') or {}
    if (model.get('provider') != 'openai-codex' or
            old_packet.get('coding_route', {}).get('provider') != 'openai-codex' or
            old_packet['coding_route'].get('required_model') != model.get('required_model') or
            old_packet['coding_route'].get('reasoning_effort') != model.get('reasoning_effort') or
            old_packet['coding_route'].get('fallback_models_allowed') != [] or
            'model_provider="openai-codex"' not in row['worker_route']['argv']):
        return False
    corrected_provider_argv(row['worker_route']['argv'])
    if (not row.get('worker_pid') and any(row.get(key) for key in
            ('worker_identity', 'worker_started_ns', 'child_identity', 'child_exited', 'worker_descendants'))):
        raise ValueError('historical provider correction has ambiguous execution identity')
    proof_ref = report.get('effect_proof')
    if not isinstance(proof_ref, dict):
        raise ValueError('historical provider correction requires separate bound effect proof')
    proof = read_hashed_json(proof_ref['path'], proof_ref['sha256'])
    if (any(proof.get(k) != row.get(k) for k in ('task', 'action_id', 'effect_id')) or
            proof.get('result') != report.get('outcome')):
        raise ValueError('provider correction effect proof differs from old action')
    if report['outcome'] == 'no_effect':
        if (proof.get('execution') != 'not_started' or
                report.get('worker_result') != 'missing' or
                Path(row['artifact']).exists() or Path(row['artifact']).is_symlink() or
                row.get('visible_sent') or row.get('worker_pid') or
                row.get('worker_identity') or row.get('child_identity') or
                row.get('worker_result')):
            raise ValueError('provider correction no-execution is not proven')
    elif report['outcome'] != 'reconciled':
        raise ValueError('historical provider effect remains unknown')
    return True


def process_absent(identity, *, original_process_only=False):
    # Refuse reused PIDs as well as the original live process. Errors are not exit proof.
    if not isinstance(identity, dict) or any(not identity.get(k) for k in ('pid', 'start', 'executable')):
        raise ValueError('full prior process identity required')
    if not original_process_only:
        return pid_is_absent(identity['pid'])
    # Only a captured OS start proof can distinguish an exited original process
    # from PID reuse. Reservation/surface callers retain strict PID absence.
    import subprocess
    if (type(identity['pid']) is not int or identity['pid'] <= 0 or
            not isinstance(identity['start'], str) or not isinstance(identity['executable'], str)):
        return False
    try:
        if pid_is_absent(identity['pid']):
            return True
        current = process_identity(identity['pid'])
        if (not isinstance(current, dict) or current.get('pid') != identity['pid'] or
                any(not isinstance(current.get(key), str) or not current[key] for key in ('start', 'executable'))):
            return False
        return current['start'] != identity['start']
    except (OSError, ValueError, KeyError, subprocess.SubprocessError):
        return False  # Missing/inaccessible OS proof is not exit evidence.


def pid_is_absent(pid):
    import psutil
    if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
        raise ValueError('positive native process PID required')
    return not psutil.pid_exists(pid)


def completed_legacy_descendant_reused(binding, result):
    """Retire only a PID reused after a completed, result-bound Coding run.

    Older descendant records contain only a PID and psutil creation time.
    A small clock adjustment is not enough evidence of reuse. Require both
    psutil creation time and the separate native ps start proof to identify
    the same newer process after the native result. Ambiguity stays blocked.
    """
    if (not isinstance(binding, dict) or set(binding) != {'pid', 'created'} or
            type(binding.get('pid')) is not int or binding['pid'] <= 0 or
            type(binding.get('created')) not in (int, float) or
            not math.isfinite(binding['created']) or binding['created'] <= 0 or
            not isinstance(result, dict) or
            type(result.get('observed_at')) not in (int, float) or
            not math.isfinite(result['observed_at']) or
            result['observed_at'] <= binding['created'] or
            not isinstance(result.get('evidence'), dict) or
            not result.get('evidence', {}).get('sha256')):
        return False
    try:
        from datetime import datetime, timezone
        import psutil
        import subprocess
        first = psutil.Process(binding['pid']).create_time()
        native = process_identity(binding['pid'])
        second = psutil.Process(binding['pid']).create_time()
        if (not isinstance(native, dict) or native.get('pid') != binding['pid'] or
                not isinstance(native.get('start'), str) or
                not isinstance(native.get('executable'), str) or
                not native['executable']):
            return False
        native_created = datetime.strptime(
            native['start'], '%a %b %d %H:%M:%S %Y').replace(tzinfo=timezone.utc).timestamp()
        return (type(first) in (int, float) and math.isfinite(first) and
                first == second and abs(first - native_created) < 2 and
                first > result['observed_at'] + 1 and
                native_created > result['observed_at'] + 1)
    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess,
            OSError, ValueError, OverflowError, subprocess.SubprocessError):
        return False


def verify_direct_worker_exit(row, *, completed=False):
    """A flow-v3 worker has a native visible claim and wait, no launcher handle."""
    if row.get('direct_worker') is not True:
        raise ValueError('direct worker proof requires an assigned route')
    require_worker_operational_exit(row)
    result = row.get('worker_result') or {}
    completed_proof = (completed and row.get('visible_sent') is True and
        row.get('worker_pid') and row.get('child_exited') is True and
        row.get('child_identity') and type(result.get('exit_code')) is int and
        result.get('artifact_status') == 'present' and
        all(result.get(k) == row.get(k) for k in
            ('task', 'artifact', 'action_id', 'effect_id')))
    for descendant in row.get('worker_descendants', []):
        if not identity_exited(descendant) and not (
                completed_proof and completed_legacy_descendant_reused(descendant, result)):
            raise ValueError('recorded Coding descendant still alive')
    if row.get('worker_pid'):
        identity = row.get('worker_identity')
        if (not identity or identity.get('pid') != row['worker_pid'] or
                not process_absent(identity)):
            raise ValueError('direct worker exit identity missing or still alive')
        if row.get('child_identity'):
            if not process_absent(row['child_identity']):
                raise ValueError('direct child still alive or identity unavailable')
        elif row.get('child_exited') or completed:
            raise ValueError('direct child exit identity missing')
    elif row.get('visible_sent'):
        if not process_absent(row['visible_binding']['shell']):
            raise ValueError('unclaimed direct send still owns its terminal')
    if completed:
        if not completed_proof:
            raise ValueError('completed direct worker lacks bound native exit evidence')


def verify_prior_exit(row, *, allow_idle_unclaimed=False):
    if row.get('direct_worker') is True:
        return verify_direct_worker_exit(row)
    require_worker_operational_exit(row)
    for descendant in row.get('worker_descendants', []):
        if not identity_exited(descendant):
            raise ValueError('recorded Coding descendant still alive or identity unavailable')
    if row.get('worker_pid'):
        identity = row.get('worker_identity')
        if not identity or identity.get('pid') != row['worker_pid'] or not (process_absent(identity) if identity.get('start') else
                    identity_exited(identity) and pid_is_absent(identity['pid'])):
            raise ValueError('prior worker alive, PID reused or exit identity missing')
        if not row.get('child_exited'):
            if not row.get('child_identity') or not process_absent(row['child_identity']):
                raise ValueError('actual Coding child exit unknown or child alive')
    elif row.get('visible_sent'):
        # The old generation is fenced. A delayed wrapper cannot claim; reuse
        # requires the exact original shell to be idle, not merely PID absence.
        if not process_absent(row['visible_binding']['shell']):
            if not allow_idle_unclaimed:
                raise ValueError('unclaimed dispatch requires prior shell exit or bound recovery')
            verify_visible_target(row['visible_binding'], idle=True)
    verify_launcher_exit(row)


def verify_launcher_exit(row):
    if not row.get('process_id') or row.get('process_started_at') is None:
        raise ValueError('missing actual launcher exit evidence')
    from tools.process_registry import process_registry
    session = process_registry.get(row['process_id'])
    if (session is None or session.session_key != row['identity']['session_key'] or
            session.started_at != row['process_started_at']):
        raise ValueError('launcher ownership or start identity mismatch')
    process_registry._reconcile_local_exit(session)
    if (launcher_identity(session) or {}).get('exited') is not True:
        raise ValueError('actual launcher handle exit not proven')
    proof = row.get('launcher_identity')
    launcher_pid = row.get('launcher_pid')
    if proof:
        if (proof.get('process_id') != row['process_id'] or
                proof.get('session_key') != row['identity']['session_key'] or
                proof.get('started_at') != row['process_started_at'] or
                (launcher_pid is not None and proof.get('pid') != launcher_pid)):
            raise ValueError('launcher identity mismatch')
        launcher_pid = proof.get('pid')
        if proof.get('exited') is not True and not identity_exited(proof):
            raise ValueError('launcher handle exit not proven')
    if (not session.exited or not isinstance(session.exit_code, int) or
            getattr(session, 'completion_reason', None) not in {'exited', 'already_exited'} or
            getattr(session, 'pid_scope', None) != 'host' or
            not launcher_pid or session.pid != launcher_pid or
            not pid_is_absent(session.pid)):
        raise ValueError('prior launcher alive, reused PID or exit proof missing')


def verify_restarted_reservation_exit(row):
    """Prove recorded host processes absent without pretending to know exit codes.

    launcher_identity captures PID + creation time from an owned local handle
    before the registry can be lost. A bare PID or an exited flag is insufficient.
    Require the original terminal and all recorded prior children to be absent
    too; an occupied replacement terminal is neither signalled nor reused here.
    """
    if row.get('direct_worker') is True:
        return verify_direct_worker_exit(row, completed=row.get('status') == 'done')
    from tools.process_registry import process_registry
    import psutil

    attempts = [row] + row.get('history', [])
    for attempt in attempts:
        require_worker_operational_exit(attempt)
        for descendant in attempt.get('worker_descendants', []):
            if not identity_exited(descendant):
                raise ValueError('recorded Coding descendant still alive or identity unavailable')
        proof = attempt.get('launcher_identity')
        started = attempt.get('process_started_at')
        if (attempt.get('task') != row['task'] or attempt.get('identity') != row['identity'] or
                not attempt.get('process_id') or not isinstance(proof, dict) or
                type(started) not in (int, float) or not math.isfinite(started) or started <= 0 or
                any(proof.get(k) != v for k, v in dict(process_id=attempt['process_id'],
                    session_key=attempt['identity']['session_key'], started_at=started,
                    pid=attempt.get('launcher_pid')).items()) or
                type(proof.get('created')) not in (int, float) or
                not math.isfinite(proof['created']) or proof['created'] <= 0 or
                process_registry.get(attempt['process_id']) is not None):
            raise ValueError('persisted native launcher identity missing or registry still owns it')
        try:
            if not pid_is_absent(proof['pid']) or not identity_exited(proof):
                raise ValueError('prior launcher alive, reused PID or identity unknown')
            if not process_absent(attempt['visible_binding']['shell']):
                # A fenced reservation that never sent a wrapper owns no shell
                # execution. Preserve that exact shell only if it is still idle.
                exited_claim = (attempt.get('worker_pid') and
                    attempt.get('worker_identity', {}).get('pid') == attempt['worker_pid'] and
                    process_absent(attempt.get('worker_identity')) and
                    process_absent(attempt.get('child_identity')))
                if not exited_claim and ('visible_sent' in attempt or any(k in attempt for k in
                        ('worker_pid', 'worker_identity', 'worker_started_ns', 'child_identity', 'child_exited'))):
                    raise ValueError('prior terminal alive or PID reused')
                verify_visible_target(attempt['visible_binding'], idle=True)
            worker = attempt.get('worker_pid')
            if worker:
                identity = attempt.get('worker_identity')
                if (not isinstance(identity, dict) or identity.get('pid') != worker or
                        not process_absent(identity) or
                        not process_absent(attempt.get('child_identity'))):
                    raise ValueError('prior worker or child exit unknown')
            elif any(k in attempt for k in ('worker_pid', 'worker_identity', 'worker_started_ns',
                                           'child_identity', 'child_exited')):
                raise ValueError('ambiguous prior worker or child claim')
        except psutil.Error as exc:
            raise ValueError('prior process identity unavailable') from exc


def ownership_pending(row):
    if row['status'] not in TERMINAL:
        return True
    if row.get('producer') != 'cmux':
        return False
    if migration_fence_matches(row):
        return False
    if operator_archive_matches(row):
        return False
    if row.get('reservation_retirement'):
        return not retirement_matches(row)
    if row['status'] == 'done':
        from tools.process_registry import process_registry
        if process_registry.get(row.get('process_id')) is None:
            # A completed item can outlive the gateway's in-memory launcher
            # handle. Release its lane only when the persisted identities prove
            # every historical launcher, worker and child has exited.
            try:
                verify_restarted_reservation_exit(row)
            except (ValueError, KeyError, OSError):
                return True
            return False
    # Cancellation/failure invalidates tickets, not live process ownership.
    try:
        verify_prior_exit(row)
    except (ValueError, KeyError, OSError):
        return True
    return False


def migration_fence_matches(row):
    """Release only the exact stopped attempt proved absent at Owner migration."""
    fence = row.get('migration_fence')
    if not isinstance(fence, dict) or row.get('status') != 'superseded' or row.get('gate') != 'migrated':
        return False
    return bool(fence.get('owner_event') and fence.get('preview_sha256') and
                fence.get('row_sha256') == migration_fence_core(row))


def migration_fence_core(row):
    """Seal stable attempt fields; hydration may remove derived item policy fields."""
    original = {key: value for key, value in row.items()
                if key not in {'migration_fence', 'deliveries', 'coordinator_turns'}}
    policy = dict(original.get('policy') or {})
    for key in ('item_lifecycle', 'work_seconds', 'work_limit', 'limits', 'revoked'):
        policy.pop(key, None)
    original['policy'] = policy
    return object_hash(original)


def operator_archive_core(row):
    """Seal the historical attempt without delivery bookkeeping or grant hydration."""
    core = copy.deepcopy(row)
    for field in ('operator_archive', 'deliveries', 'coordinator_turns'):
        core.pop(field, None)
    if isinstance(core.get('policy'), dict):
        core['policy'].pop('revoked', None)
    return object_hash(core)


def operator_archive_matches(row):
    """An exact owner disposition releases admission, never rewrites history."""
    receipt = row.get('operator_archive')
    if isinstance(receipt, dict) and receipt.get('kind') == 'owner_accepted_successor_uncertainty':
        from hermes_cli.diggr_successor import receipt_matches
        return receipt_matches(row, receipt)
    return bool(isinstance(receipt, dict) and
        receipt.get('kind') == 'owner_accepted_historical_uncertainty' and
        (row.get('task') == 'APP-104-stable-album-children' or
         app104_next_archive_case(row)) and
        row.get('generation') == 3 and row.get('status') == 'blocked' and
        row.get('effect_status') == 'unknown' and
        receipt.get('task') == row['task'] and
        receipt.get('generation') == row['generation'] and
        receipt.get('identity') == row['identity'] and
        receipt.get('action_id') == row.get('action_id') and
        receipt.get('effect_id') == row.get('effect_id') and
        receipt.get('core_sha256') == operator_archive_core(row) and
        receipt.get('native_owner_event'))


def app104_next_archive_case(row):
    """Bind a second one-time archive to the recorded APP-104 handoff failure."""
    return bool(
        row.get('task') == 'APP-104-album-group-retired-next-20260926' and
        row.get('generation') == 3 and row.get('status') == 'blocked' and
        row.get('gate') == 'reconcile' and row.get('effect_status') == 'unknown' and
        row.get('producer') == 'cmux' and row.get('launcher_expected') is True and
        row.get('owner_batch') == '08888f8d40fa3cf91ddb5622124d46352e9c469961dc12056837d35169260a32' and
        row.get('action_id') == '8249b6b049f846e3a6790b74d3f6cffb' and
        row.get('effect_id') == 'd09a1d748819449792f17825723f9028' and
        row.get('process_id') == 'proc_0b2961fc999d' and
        row.get('launcher_pid') == 38914 and
        row.get('native_launch_sha256') == 'a5a5370259cf7a9167ca581f78244bd0f93fdd11a0cb5bb92ce80d438f396302' and
        row.get('receipt_sha256') == '60a4f518a623898fcf3e648e12d806917088682e808b433f54808986740ca358' and
        row.get('attempt_history') == [] and
        not row.get('reservation_retirement') and
        not any(row.get(key) for key in ('worker_pid', 'worker_identity',
            'worker_started_ns', 'child_identity', 'child_exited', 'worker_result',
            'visible_sent', 'send_claim')) and
        row.get('worker_route', {}).get('worktree') ==
            '/Users/diggr/Projects/diggr_app/worktrees/APP-104-album-group-retired-next-20260926' and
        row.get('artifact') ==
            '/Users/diggr/Projects/diggr_claw/ops/state/loop-control/routing_packets/APP-104-retired-next-20260926/coding-result.md')


def prelaunch_rejection_case(row):
    """The one APP-104 attempt rejected by the local gateway scan before execute."""
    return bool(
        row.get('task') == 'APP-104-album-group-new-20260926' and
        row.get('generation') == 3 and row.get('status') == 'blocked' and
        row.get('gate') == 'reconcile' and row.get('effect_status') == 'unknown' and
        row.get('producer') == 'cmux' and row.get('launcher_expected') is True and
        row.get('owner_batch') == 'ff6f91394b8c50844db6a06735f76378e26bdccc135e484b906dd76d555e469f' and
        row.get('action_id') == 'cac52908fccb449ebf46b44a3a079b7d' and
        row.get('effect_id') == '0de8fcd3771545ba9a0f68922ff2c65e' and
        row.get('native_launch_sha256') == 'ed811def019e6f36ca33c81dd386b95e6a549815e8b4f5f6b3fe20d6b64ca68c' and
        row.get('native_launch_diagnostic') == {
            'exit_code': 1, 'outcome': 'terminal_error',
            'response_sha256': '8bb817e125655c67191c0c38d8e5384736a71441e04367c7a811a093e18a49f9',
            'status': 'error'} and
        row.get('attempt_history') == [] and
        not row.get('operator_archive') and
        not any(row.get(key) for key in ('process_id', 'launcher_pid', 'launcher_identity',
            'worker_pid', 'worker_identity', 'child_identity', 'visible_sent',
            'send_claim', 'worker_result'))
    )


def prelaunch_rejection_core(row):
    """Seal launch identity and effects, excluding only mutable delivery bookkeeping."""
    core = copy.deepcopy(row)
    for field in ('reservation_retirement', 'deliveries', 'coordinator_turns',
                  'coordinator_delivery'):
        core.pop(field, None)
    if isinstance(core.get('policy'), dict):
        core['policy'].pop('revoked', None)
    return object_hash(core)


def prelaunch_rejection_report(row):
    """Exact evidence payload for this recorded pre-execution terminal response."""
    return dict(kind='gateway_lifecycle_rejection_before_execute', task=row['task'],
        generation=row['generation'], identity=row['identity'], action_id=row['action_id'],
        effect_id=row['effect_id'], core_sha256=prelaunch_rejection_core(row),
        native_response_sha256=row['native_launch_diagnostic']['response_sha256'],
        outcome='no_launcher_execution')


def prelaunch_retirement_matches(row):
    receipt = row.get('reservation_retirement')
    if not isinstance(receipt, dict) or receipt.get('kind') != 'gateway_lifecycle_rejection_before_execute':
        return False
    current = copy.deepcopy(row)
    current.pop('reservation_retirement', None)
    try:
        return bool(prelaunch_rejection_case(current) and
            not any(Path(path).exists() or Path(path).is_symlink() for path in
                (current['artifact'], current['worker_route']['receipt'])) and
            receipt.get('core_sha256') == prelaunch_rejection_core(current) and
            receipt.get('identity') == row['identity'] and
            receipt.get('action_id') == row['action_id'] and
            receipt.get('effect_id') == row['effect_id'] and
            read_hashed_json(receipt['evidence']['path'], receipt['evidence']['sha256']) ==
                prelaunch_rejection_report(current))
    except (KeyError, ValueError, OSError, TypeError):
        return False


def retirement_matches(row):
    if prelaunch_retirement_matches(row):
        return True
    receipt = row['reservation_retirement']
    current = {k: v for k, v in row.items() if k != 'reservation_retirement'}
    try:
        if (receipt['row_sha256'] != object_hash(current) and
                current.get('policy', {}).get('revoked') is True):
            # Hydration reflects later owner revocation. Tightening authorization
            # cannot revive retired process ownership; every other field stays sealed.
            current = dict(current, policy=dict(current['policy'], revoked=False))
        if (receipt['row_sha256'] != object_hash(current) or
                any(receipt[k] != row[k] for k in ('generation', 'identity', 'action_id', 'effect_id'))):
            return False
        report = recovery_report(row, receipt['evidence'])
        return (report.get('row_sha256') == receipt['request_row_sha256'] and
                report.get('authorization') == row.get('authorization') and
                report.get('outcome') in {'no_effect', 'reconciled'} and bool(report.get('effect_proof')))
    except (ValueError, KeyError, OSError, TypeError):
        return False


def claim_deadline(row, now, seconds):
    return min([now + seconds] + [row[k] for k in ('hard_stop', 'authorization_expires_at')
                                  if row.get(k) is not None])


def coding_handoff_pending(guard, observed, snapshot, now):
    """Bound successful launcher delivery separately from the worker claim."""
    with guard.transaction() as tasks:
        row = tasks.get(observed['task'])
        if (not row or row['generation'] != observed['generation'] or row['status'] != 'running' or
                process_observation_key(row) != snapshot or row.get('visible_sent') or
                row.get('worker_pid') or not guard.renew(row, now)):
            return False
        if row.get('handoff_generation') != row['generation']:
            row['handoff_generation'] = row['generation']
            row['handoff_deadline'] = min(claim_deadline(row, now, 120), row['registered_at'] + 300)
        if now >= row['handoff_deadline'] and row.get('coding_prompt_binding'):
            # The launcher has exited successfully. A durable prompt received
            # before this original handoff deadline may cover pre-API compaction.
            row['claim_phase'] = 'reserved'
            row['claim_deadline'] = row['handoff_deadline']
            if admit_received_coding_prompt(row, now):
                row['handoff_deadline'] = row['claim_deadline']
        return now < row['handoff_deadline']


def process_observation_key(row):
    # A copied observer must not invalidate a newer send/claim in the same generation.
    return object_hash({k: row.get(k) for k in (
        'worker_pid', 'worker_identity', 'worker_started_ns', 'child_identity', 'child_exited',
        'worker_supervision', 'worker_descendants', 'worker_result',
        'visible_sent', 'visible_sent_at', 'claim_phase', 'claim_deadline',
        'process_id', 'process_started_at', 'receipt_sha256', 'non_cmm_receipt_sha256',
        'handoff_generation', 'handoff_deadline')})


def target_reserved(row, target):
    owned = row.get('visible_binding', {}).get('target', {})
    return (owned.get('surface', '').upper() == target['surface'].upper()
            and ownership_pending(row) and not executor_released(row))


def executor_released(row):
    """Release only the physical slot, never the candidate or logical Todo."""
    result = row.get('worker_result')
    if not worker_operational_exit(row):
        return False
    if (row.get('producer') != 'cmux' or not isinstance(result, dict) or
            type(result.get('exit_code')) is not int or not row.get('worker_pid') or
            not row.get('child_exited') or not row.get('child_identity') or
            any(result.get(k) != row.get(k) for k in ('task', 'action_id', 'effect_id', 'artifact'))):
        return False
    try:
        # A waited child is not enough: its wrapper, launcher and any recorded
        # child must all be gone, and the bound shell must have regained idle.
        if not process_absent(row['child_identity']):
            return False
        from tools.process_registry import process_registry
        if process_registry.get(row.get('process_id')) is None:
            verify_restarted_reservation_exit(row)
        else:
            verify_prior_exit(row)
        verify_visible_target(row['visible_binding'], idle=True)
    except (ValueError, KeyError, OSError):
        return False
    return True


def worktree_reserved(row, worktree):
    """Derive the writer reservation from the existing candidate lifecycle."""
    owned = row.get('worker_route', {}).get('worktree')
    return bool(owned and Path(owned).resolve() == Path(worktree).resolve()
                and (ownership_pending(row) or operator_archive_matches(row) or
                     prelaunch_retirement_matches(row)))


def cmux_tree():
    import subprocess
    tree = json.loads(subprocess.check_output([CMUX, '--json', '--id-format', 'both',
        'top', '--all'], text=True, timeout=10))
    if not isinstance(tree, (dict, list)) or not tree:
        raise ValueError('missing topology evidence')
    return tree


def exact_target_present(tree, target):
    found = False
    workspaces = 0
    def walk(node, workspace=None):
        nonlocal found, workspaces
        if isinstance(node, dict):
            if node.get('kind') == 'workspace':
                workspace = node.get('id')
                if not isinstance(workspace, str) or not workspace:
                    raise ValueError('invalid workspace topology identity')
                workspaces += 1
            if (node.get('kind') == 'surface' and workspace and
                    workspace.upper() == target['workspace'].upper() and
                    node.get('id', '').upper() == target['surface'].upper()):
                found = True
            for child in node.values(): walk(child, workspace)
        elif isinstance(node, list):
            for child in node: walk(child, workspace)
    walk(tree)
    if not workspaces:
        raise ValueError('topology contains no verifiable workspace; unavailable is not lost')
    return found


def validate_recovery_route(old, route, old_packet, packet, old_artifact, artifact, kind='technical', *, retained_correction=False, followup=None, _provider_correction=None):
    # Adjacent durable bookkeeping must not dirty the required clean source tree.
    if Path(artifact).is_relative_to(Path(canonical_path(route['worktree']))):
        raise ValueError('recovery artifact and reservation must be outside authorized worktree')
    expected_route = followup if followup else old
    if any(route.get(k) != expected_route.get(k) for k in ('worktree', 'branch', 'plane_id')):
        raise ValueError('changed worktree, branch or Plane identity')
    # New packet path, same authorized content. No prompt/model/action drift hidden
    # behind a new strategy. Corrections of invalid original scope require Main.
    # Native Main review may correct implementation instructions within the
    # existing scope. Technical recovery cannot alter the prompt; neither path
    # can change model, permissions, base, retained state or Main's scope.
    prompt_keys = ({'coding_prompt_source', 'coding_prompt_sha256', 'result_path', 'worker_route'}
                   if kind == 'review' else set())
    if retained_correction and kind == 'review':
        prompt_keys |= {'base', 'retained_worktree_sha256'}
        if followup:
            prompt_keys |= {'worktree', 'branch'}
    for key in sorted(old_packet.keys() | packet.keys()):
        if key in prompt_keys:
            continue
        if key == 'worker_route' and _provider_correction is _NATIVE_PROVIDER_CORRECTION:
            old_embedded = {k: v for k, v in old.items() if k != 'packet_sha256'}
            new_embedded = {k: v for k, v in route.items() if k != 'packet_sha256'}
            if old_packet.get(key) != old_embedded or packet.get(key) != new_embedded:
                raise ValueError('packet.worker_route: changed embedded route binding')
            continue
        if key == 'required_checks' and kind == 'review' and _review_checks_unchanged_or_runner_fix(
                old_packet.get(key), packet.get(key)):
            continue
        if old_packet.get(key) != packet.get(key) or (key in old_packet) != (key in packet):
            raise ValueError(f'packet.{_first_changed_path(old_packet.get(key), packet.get(key), key)}: '
                             'changed authorized contract or unqualified preparation data')
    if kind == 'review':
        source = packet.get('coding_prompt_source')
        sha = packet.get('coding_prompt_sha256')
        if source is not None or sha is not None:
            if not isinstance(source, str) or not isinstance(sha, str) or not source or not sha:
                raise ValueError('packet.coding_prompt_source/coding_prompt_sha256: pair required')
            prompt = verified_bytes(source, sha).decode('utf-8')
            if route.get('argv', [None])[-1] != prompt:
                raise ValueError('packet.coding_prompt_source: route prompt differs from hashed source')
        if 'result_path' in packet and packet['result_path'] != artifact:
            raise ValueError('packet.result_path: must equal fresh worker artifact')
        if 'worker_route' in packet:
            embedded = {k: v for k, v in route.items() if k != 'packet_sha256'}
            if packet['worker_route'] != embedded:
                raise ValueError('packet.worker_route: must equal the proposed route')
    if (packet.get('operator_scope_authorized') is not True or
            any(packet.get(k) != route['plane_id'] for k in ('plane_id', 'task_id', 'scope_id')) or
            packet.get('worktree') != route['worktree'] or packet.get('branch') != route['branch'] or
            not packet.get('base') or not packet.get('requested_actions')):
        raise ValueError('explicit matching Plane-bound route contract required')
    contract = packet.get('coding_route', {})
    if (contract.get('executor') != 'codex' or not contract.get('required_model') or
            contract.get('fallback_models_allowed') != [] or contract.get('merge_authorized') is not False or
            not set(packet['requested_actions']).issubset(contract.get('routine_actions_authorized', []))):
        raise ValueError('invalid bounded Codex contract')
    expected = [artifact if word == old_artifact else word for word in old['argv']]
    if followup:
        expected[expected.index('--cd') + 1] = followup['worktree']
    if kind == 'review' and route.get('argv') and isinstance(route['argv'][-1], str) and route['argv'][-1].strip():
        expected[-1] = route['argv'][-1]
    if _provider_correction is _NATIVE_PROVIDER_CORRECTION:
        if (kind != 'technical' or old_packet.get('coding_route', {}).get('provider') != 'openai-codex' or
                packet.get('coding_route') != old_packet.get('coding_route')):
            raise ValueError('provider correction must retain the logical contract')
        expected = corrected_provider_argv(expected)
    if route.get('argv') != expected:
        raise ValueError('changed authorized argv')
    argv = route['argv']
    if (Path(argv[0]).name != 'codex' or argv[1:2] != ['exec'] or
            argv.count('--model') != 1 or argv[argv.index('--model') + 1] != contract['required_model'] or
            argv.count('--output-last-message') != 1 or
            argv[argv.index('--output-last-message') + 1] != artifact):
        raise ValueError('exact model and fresh output argument required')


def _first_changed_path(old, new, prefix):
    """Name the first mismatched field without including its possibly secret value."""
    if isinstance(old, dict) and isinstance(new, dict):
        for key in sorted(old.keys() | new.keys()):
            if old.get(key) != new.get(key) or (key in old) != (key in new):
                return _first_changed_path(old.get(key), new.get(key), prefix + '.' + str(key))
    return prefix


def _review_checks_unchanged_or_runner_fix(old, new):
    if old == new:
        return True
    if not isinstance(old, list) or not isinstance(new, list) or len(old) != len(new):
        return False
    changed = set()
    for before, after in zip(old, new):
        if not isinstance(before, dict) or not isinstance(after, dict) or before.keys() != after.keys():
            return False
        check_id = before.get('id')
        if (check_id != after.get('id') or
                {k: v for k, v in before.items() if k != 'argv'} !=
                {k: v for k, v in after.items() if k != 'argv'}):
            return False
        prior, current = before.get('argv'), after.get('argv')
        if prior == current:
            continue
        if (check_id not in {'focused', 'neighbor_profile_regressions'} or
                not isinstance(prior, list) or len(prior) < 4 or
                not isinstance(prior[1], str) or 'vitest@4.' not in prior[1] or
                prior[2:3] != ['run'] or
                prior[3:4] != ['--no-cache'] or
                current != prior[:3] + ['--configLoader', 'runner'] + prior[3:]):
            return False
        changed.add(check_id)
    return bool(changed)


def verify_recovery_worktree(route, packet):
    import subprocess
    def git(*args):
        return subprocess.check_output(['git', '-C', canonical_path(route['worktree']), *args],
            text=True, timeout=10, env=dict(os.environ, GIT_OPTIONAL_LOCKS='0')).strip()
    if (route['branch'] in {'main', 'master'} or not route['branch'].startswith('hermes/WP-') or
            git('branch', '--show-current') != route['branch'] or
            git('rev-parse', 'HEAD') != packet['base'] or
            ((git('status', '--porcelain') or packet.get('retained_worktree_sha256') is not None) and
             not _retained_worktree_matches(route['worktree'], packet))):
        raise ValueError('recovery requires exact clean authorized branch and base')


def maintenance_control(payload):
    """Native terminal-tool bridge; role and grant bindings come from runtime state."""
    operation = payload['operation']
    if operation == 'item_continue':
        from agent.subagent_lifecycle import get_active_subagent_parent
        from hermes_constants import get_hermes_home
        agent = get_active_subagent_parent()
        guard, row = register_assigned_worker(
            payload['assignment'], str(agent.session_id), get_hermes_home(), payload['prompt'])
        return start_assigned_worker(guard, row)
    context = EVENT_CONTEXT.get()
    capability = payload.get('maintenance')
    if operation == 'recover' and capability is not None:
        identity = payload['identity']  # bearer grant is checked against the stored row
    else:
        if not context:
            raise ValueError('maintenance operation requires native Main context')
        identity = context['identity']
        require_native_main(identity)
    guard = runtime_guard(identity['home'])
    if guard is None:
        raise ValueError('maintenance unavailable; native guard inactive')
    if operation.startswith('item_'):
        from hermes_cli.diggr_item import control
        return control(guard, identity, payload)
    wake = payload['wake']
    if operation == 'authorize':
        return guard.authorize_maintenance(identity, wake, payload['evidence'], payload['strategy'], payload['expires'])
    if operation == 'revoke':
        return guard.revoke_maintenance(identity, wake)
    if operation == 'retire':
        return guard.retire_reservation(identity, wake, payload['evidence'])
    if operation == 'failure':
        return guard.failure(identity, wake, payload['evidence'])
    if operation == 'reconcile_failed':
        return guard.reconcile_failed(identity, wake, payload['evidence'])
    if operation == 'recover':
        return guard.recover(identity, wake, payload['evidence'], payload['strategy'], maintenance=capability)
    raise ValueError('unsupported bounded maintenance operation')


def runtime_guard(home=None):
    from hermes_constants import get_hermes_home, set_hermes_home_override, reset_hermes_home_override
    from hermes_cli.config import load_config_readonly
    home = Path(home or get_hermes_home()).resolve()
    binding = set_hermes_home_override(home)
    try:
        cfg = load_config_readonly().get('diggr_continuation', {})
    finally:
        reset_hermes_home_override(binding)
    if not isinstance(cfg, dict):
        raise ValueError('diggr_continuation must be a mapping')
    if cfg.get('enabled') is not True:
        return None
    if cfg.get('profile') not in delivery.PROFILES or cfg.get('home') != str(home):
        raise ValueError('activation requires explicit matching Main profile and resolved home')
    return Guard(home / 'state' / 'diggr-continuation.json', profile=cfg['profile'])


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def worker_ticket(row):
    ticket = {k: row[k] for k in ('task', 'generation', 'identity', 'authorization', 'ticket_nonce')}
    if row.get('codex_home'):
        ticket['codex_home'] = row['codex_home']
    return ticket


CMUX = '/Applications/cmux.app/Contents/Resources/bin/cmux'


def cmux_snapshot(target):
    import subprocess
    # Fixed existing transport; never trust model/env-selected executables or titles.
    return json.loads(subprocess.check_output([CMUX, '--json', '--id-format', 'both',
        'top', '--all', '--processes'], text=True, timeout=10))


def process_identity(pid):
    import subprocess
    parts = subprocess.check_output(['/bin/ps', '-p', str(int(pid)), '-o',
        'pid=,ppid=,pgid=,tpgid=,tty=,lstart=,comm='], text=True, timeout=5,
        env={'LC_ALL': 'C', 'TZ': 'UTC'}).split(maxsplit=10)
    # lstart is localized and timezone-dependent; comm may contain spaces.
    # Compare the full stable identity, never display formatting from the caller.
    if len(parts) != 11:
        raise ValueError('missing/ambiguous OS process identity')
    return dict(zip(('pid', 'ppid', 'pgid', 'tpgid'), map(int, parts[:4])),
                tty=parts[4], start=' '.join(parts[5:10]), executable=parts[10].strip().lstrip('-'))


def visible_surface(target):
    from uuid import UUID
    if set(target) != {'workspace', 'surface'}:
        raise ValueError('explicit workspace and surface UUIDs required')
    for value in target.values():
        if str(UUID(value)).upper() != value.upper():
            raise ValueError('canonical cmux UUID required')
    found = []
    terminal_ttys = []
    def walk(node, workspace=None):
        if isinstance(node, dict):
            if node.get('kind') == 'workspace': workspace = node.get('id')
            if node.get('kind') == 'surface' and node.get('type') == 'terminal' and node.get('tty'):
                terminal_ttys.append(node['tty'])
            if node.get('kind') == 'surface' and node.get('id', '').upper() == target['surface'].upper():
                if workspace and workspace.upper() == target['workspace'].upper(): found.append(node)
            for child in node.values(): walk(child, workspace)
        elif isinstance(node, list):
            for child in node: walk(child, workspace)
    walk(cmux_snapshot(target))
    if len(found) != 1 or found[0].get('type') != 'terminal' or not found[0].get('tty'):
        raise ValueError('missing, ambiguous or non-terminal cmux target')
    if terminal_ttys.count(found[0]['tty']) != 1:
        raise ValueError('ambiguous cmux target TTY')
    return found[0]


def bind_visible_target(target):
    surface = visible_surface(target)
    roots = surface.get('top_level_pids', [])
    if len(roots) != 1:
        raise ValueError('target must have one live root shell')
    root = process_identity(roots[0])
    shell = root
    if root['executable'] == '/usr/bin/login':
        children = [process_identity(pid) for pid in surface.get('root_pids', [])
                    if pid != root['pid']]
        shells = [child for child in children if child['ppid'] == root['pid'] and
                  child['executable'] in ('/bin/zsh', '/bin/bash')]
        if len(shells) != 1 or root['tty'] != surface['tty'] or root['pgid'] != root['pid']:
            raise ValueError('target login/shell OS/TTY identity mismatch')
        shell = shells[0]
    if (shell['executable'] not in ('/bin/zsh', '/bin/bash') or
            shell['tty'] != surface['tty'] or shell['pgid'] != shell['pid'] or
            root['pid'] not in surface.get('root_pids', []) or
            shell['pid'] not in surface.get('root_pids', [])):
        raise ValueError('target shell OS/TTY identity mismatch')
    return dict(target=target, shell={k: shell[k] for k in
                ('pid', 'ppid', 'pgid', 'tty', 'start', 'executable')})


def verify_visible_target(binding, idle=False):
    actual = bind_visible_target(binding['target'])
    if actual != binding:
        # Only registered target and OS binding fields; no environment/argv/state dump.
        fields = ('pid', 'ppid', 'pgid', 'tty', 'start', 'executable')
        expected = dict(target={k: binding['target'].get(k) for k in ('surface', 'workspace')},
                        shell={k: binding['shell'].get(k) for k in fields})
        raise ValueError('stale cmux shell binding: expected=' + json.dumps(expected, sort_keys=True)
                         + ' actual=' + json.dumps(actual, sort_keys=True))
    surface = visible_surface(binding['target'])
    shell = process_identity(binding['shell']['pid'])
    root_pids = surface.get('top_level_pids', [])
    idle_pids = {shell['pid'], *root_pids}
    if idle and (shell['tpgid'] != shell['pgid'] or
                 surface.get('foreground_pgids') != [shell['pgid']] or
                 set(surface.get('tty_process_pids', [])) != idle_pids):
        raise ValueError('occupied terminal; idle root shell required')
    return surface, shell


def bound_shell_identity(binding):
    import psutil
    pid = binding['shell']['pid']
    proof = dict(pid=pid, created=psutil.Process(pid).create_time())
    _, current = verify_visible_target(binding, idle=True)
    if current.get('pid') != pid or not current.get('start') or not current.get('executable'):
        raise ValueError('bound shell OS start proof unavailable')
    return dict(proof, start=current['start'], executable=current['executable'])


def recovery_visible_binding(row):
    """Rebind only the same authorized surface after the old shell is gone.

    Called only after hashed effect reconciliation and launcher/worker exit.
    No killing, target discovery, dispatch or reuse of the old route seal.
    """
    import psutil
    binding = row['visible_binding']
    try:
        verify_visible_target(binding, idle=True)
        return binding
    except ValueError:
        shell = binding['shell']
        proof = row.get('visible_shell_identity')
        if proof:
            if proof.get('pid') != shell['pid'] or not identity_exited(proof):
                raise ValueError('prior cmux shell exit not reconciled')
        else:
            # Legacy bindings have a full OS start identity, never just a PID.
            if not shell.get('start'):
                raise ValueError('prior cmux shell identity missing')
            try:
                psutil.Process(shell['pid'])
                actual = process_identity(shell['pid'])
                if actual['pid'] != shell['pid'] or actual['start'] == shell['start']:
                    raise ValueError('prior cmux shell exit not reconciled')
            except psutil.NoSuchProcess:
                pass
        current = bind_visible_target(binding['target'])
        verify_visible_target(current, idle=True)
        return current



def verify_visible_claim(row):
    if not row.get('visible_sent'):
        raise ValueError('worker was not sent through registered visible transport')
    surface, shell = verify_visible_target(row['visible_binding'])
    current = process_identity(os.getpid())
    if (current['ppid'] != shell['pid'] or current['tty'] != shell['tty'] or
            current['pgid'] != current['pid'] or current['tpgid'] != current['pgid'] or
            shell['tpgid'] != current['pgid'] or
            current['pid'] not in surface.get('tty_process_pids', []) or
            surface.get('foreground_pgids') != [current['pgid']]):
        raise ValueError('claim is not the bound foreground terminal process')
    if any(not os.isatty(fd) or os.ttyname(fd) != '/dev/' + shell['tty'] for fd in (0, 1, 2)):
        raise ValueError('worker stdio is not attached to target TTY')
    if os.tcgetpgrp(0) != os.getpgrp():
        raise ValueError('worker does not own foreground TTY')


def visible_worker_command(ticket):
    import base64
    import shlex
    import sys
    payload = base64.urlsafe_b64encode(json.dumps(ticket).encode()).decode()
    source = Path(__file__).resolve()
    home = ticket['identity']['home']
    if not Path(home).is_absolute() or str(Path(home).resolve()) != home:
        raise ValueError('canonical absolute worker home required')
    # Set before Python startup/native imports. The visible shell's profile and
    # Python search path are never authority for this registered worker.
    argv = ['/usr/bin/env', '-u', 'PYTHONHOME', 'HOME=' + home, 'HERMES_HOME=' + home,
            'PYTHONPATH=' + str(source.parents[1]), 'PYTHONNOUSERSITE=1',
            'PYTHONDONTWRITEBYTECODE=1', sys.executable, '-B', str(source), '--home', home,
            'worker', '--payload-b64', payload]
    # Resolve before HOME becomes the Hermes profile. Legacy tickets also need
    # the originating runtime's default, never the target shell's credentials.
    codex_home = ticket.get('codex_home') or os.environ.get('CODEX_HOME') or str(Path.home().resolve() / '.codex')
    argv.insert(3, 'CODEX_HOME=' + canonical_path(codex_home))
    # cmux send interprets backslash escapes. Refuse them and control bytes entirely.
    if any('\\' in word or any(ord(ch) < 32 or ord(ch) == 127 for ch in word) for word in argv):
        raise ValueError('unsafe terminal transport bytes')
    return shlex.join(argv)


def cmux_send(binding, command):
    import subprocess
    verify_visible_target(binding, idle=True)
    target = binding['target']
    # One literal paste plus one submit. Keyboard injection can silently lose
    # the middle of a long registered command. The caller has already persisted
    # visible_sent, so any uncertain response stays unknown, never retried.
    if not command or any(ord(ch) < 32 or ord(ch) == 127 for ch in command):
        raise ValueError('single-line registered command required')
    params = dict(workspace_id=target['workspace'], surface_id=target['surface'],
                  text=command, submit_key='return')
    result = subprocess.run([CMUX, 'rpc', 'terminal.paste', json.dumps(params)],
                            check=True, capture_output=True, timeout=10)
    try:
        reply = json.loads(result.stdout)
        if (not isinstance(reply, dict) or reply.get('workspace_id') != target['workspace']
                or reply.get('surface_id') != target['surface'] or reply.get('submitted') is not True
                or reply.get('submit_error') or reply.get('error')
                or reply.get('delivery') not in (None, 'sent')):
            raise ValueError('unexpected paste result')
    except (ValueError, TypeError):
        raise ValueError('registered worker paste outcome uncertain; never resend') from None


def route_revision(row):
    """Compare authorization/state, not the derived rolling v2 coordination lease."""
    value = copy.deepcopy(row)
    if value.get('policy', {}).get('item_lifecycle'):
        value['policy'].pop('deadline', None)
    return object_hash(value)


def recovery_snapshot_matches(before, current):
    """Keep grant/resource fencing while allowing the native busy watcher to renew."""
    if before.keys() != current.keys() or before.grants != current.grants:
        return False
    for name, previous in before.items():
        row = current[name]
        comparable = copy.deepcopy(row)
        # Only the same accepted v2 coordinator may extend its still-live lease.
        # All turn, generation, ownership, budget and reservation fields still match.
        if (previous.get('policy', {}).get('item_lifecycle')
                and previous.get('status') == 'executing'
                and previous.get('coordinator_turns', {}).get(str(previous['generation']), {}).get('status') == 'accepted'
                and time.time() < previous.get('lease', 0)
                and previous['lease'] <= row.get('lease', 0)
                <= min(row['deadline'], row.get('policy', {}).get('deadline', 0))):
            comparable['lease'] = previous['lease']
        if route_revision(previous) != route_revision(comparable):
            return False
    return True


def route_snapshot_matches(before, current):
    if route_revision(before) == route_revision(current):
        return True
    # A background launcher may enter this CLI before its parent records the
    # returned process handle. Permit only that first complete native binding;
    # replacements, partial bindings and all other concurrent changes still fail.
    fields = ('process_id', 'process_started_at', 'launcher_identity', 'launcher_pid')
    binding = current.get('launcher_identity')
    if (not before.get('launcher_expected') or not before.get('native_launch_sha256') or
            any(before.get(k) is not None for k in fields) or not isinstance(binding, dict) or
            not all(current.get(k) is not None for k in fields) or
            binding.get('process_id') != current['process_id'] or
            binding.get('pid') != current['launcher_pid'] or
            binding.get('started_at') != current['process_started_at'] or
            binding.get('session_key') != current['identity']['session_key']):
        return False
    normalized = copy.deepcopy(current)
    for key in fields:
        if key in before:
            normalized[key] = before[key]
        else:
            normalized.pop(key, None)
    return route_revision(before) == route_revision(normalized)


def assigned_start_core(batch, row):
    """Seal registration; exclude only fields owned by the subsequent native send/claim."""
    batch, row = copy.deepcopy(batch), copy.deepcopy(row)
    batch.pop('deliveries', None)  # Status notifications carry no execution authority.
    batch['item']['coding_assignment'].pop('status', None)
    for key in ('second_renewal_start_sha256', 'deliveries', 'receipt_sha256',
                'non_cmm_receipt_sha256', 'visible_sent', 'visible_sent_at',
                'send_request_sha256', 'claim_phase', 'claim_deadline',
                'worker_pid', 'worker_identity', 'worker_started_ns'):
        row.pop(key, None)
    row['policy'].pop('deadline', None)  # Existing rolling coordination lease.
    row['policy'].pop('limits', None)  # Hydrated from the sealed item budget.
    return object_hash(dict(batch=batch, row=row))


def verify_assigned_prompt(rows, row, *, claimed=None):
    """Keep the final renewal source/bytes and start window bound through spawn."""
    batch = getattr(rows, 'grants', {}).get('batches', {}).get(row.get('owner_batch'), {})
    assignment = batch.get('item', {}).get('coding_assignment', {})
    if not (row.get('second_renewal_start_sha256') or row.get('assignment_renewal_sha256') or assignment.get('second_renewal') or
            len(batch.get('item', {}).get('assignment_renewals') or []) > 1):
        return
    from hermes_cli import diggr_item
    _, contract = diggr_item.bound(rows, row['identity'], row['owner_batch'], row['owner_issue'])
    diggr_item.assignment_bound_prompt(row['owner_batch'], batch, contract, row=row)
    item = batch['item']
    if (not row.get('second_renewal_start_sha256') or
            row['second_renewal_start_sha256'] != assigned_start_core(batch, row) or
            rows.get(row['task']) != row or item.get('phase') != 'coding' or
            item.get('task') != row['task'] or batch.get('tasks') != {item['issue']: row['task']} or
            row.get('policy', {}).get('limits', item['budget']) != item['budget']):
        raise ValueError('second-renewal registered item, assignment or task changed')
    for name, other in rows.items():
        if name == row['task']:
            continue
        if (other.get('owner_batch') == row['owner_batch'] or
                (other.get('policy') or {}).get('batch') == row['owner_batch'] or
                other.get('visible_binding', {}).get('target') == item['target'] or
                any(attempt.get('worker_route', {}).get('worktree') == item['worktree'] or
                    attempt.get('worker_route', {}).get('visible_target') == item['target']
                    for attempt in [other, *other.get('history', []), *other.get('attempt_history', [])])):
            raise ValueError('second-renewal has a competing task or resource reservation')
    if any(bid != row['owner_batch'] and other.get('item', {}).get('phase') not in {None, 'closed', 'archived'}
           for bid, other in rows.grants.get('batches', {}).items()):
        raise ValueError('second-renewal Coding lane has a competing item')
    claim = row.get('claim_phase')
    worker_fields = ('worker_pid', 'worker_identity', 'worker_started_ns')
    if claim is None:
        if (claimed is not None or assignment.get('status') != 'registered' or
                any(row.get(k) is not None for k in (*worker_fields, 'visible_sent', 'visible_sent_at',
                                                    'send_request_sha256', 'claim_deadline'))):
            raise ValueError('second-renewal has unexpected pre-send worker evidence')
    else:
        if (claim not in {'sent', 'claimed'} or assignment.get('status') not in {'sending_worker', 'worker_sent'} or
                row.get('visible_sent') is not True or
                row.get('send_request_sha256') != object_hash(worker_ticket(row)) or
                not all(type(row.get(k)) in (int, float) and math.isfinite(row[k])
                        for k in ('visible_sent_at', 'claim_deadline')) or
                row['claim_deadline'] != claim_deadline(row, row['visible_sent_at'], 30) or
                not row.get('receipt_sha256') or not row.get('non_cmm_receipt_sha256') or
                not row['registered_at'] <= row['visible_sent_at'] <= time.time() < row['claim_deadline']):
            raise ValueError('second-renewal native send reservation changed or expired')
        if claim == 'claimed':
            if (claimed is None or assignment.get('status') != 'worker_sent' or
                    row.get('worker_pid') != os.getpid() or not row.get('worker_identity') or
                    type(row.get('worker_started_ns')) is not int or
                    any(row.get(k) != claimed.get(k) for k in worker_fields)):
                raise ValueError('second-renewal does not match its own native claim')
        elif claimed is not None or any(row.get(k) is not None for k in worker_fields):
            raise ValueError('second-renewal has an additional worker claim')
    for key in ('receipt_sha256', 'non_cmm_receipt_sha256'):
        if row.get(key) is not None:
            verified_bytes(row['worker_route']['receipt'], row[key])
    if (batch.get('revoked') or assignment.get('task') != row.get('task') or
            time.time() >= coding_assignment_deadline(batch, rows, bid=row['owner_batch'])):
        raise ValueError('second-renewal prompt authority or start window expired')


def validate_ticket(tasks, ticket):
    row = tasks.get(ticket.get('task'))
    if row is not None:
        verify_assigned_prompt(tasks, row)
    if (row and worker_ticket(row) == ticket and row.get('claim_phase') == 'reserved' and
            not row.get('visible_sent') and
            row.get('claim_deadline') is not None and time.time() >= row['claim_deadline']):
        admit_received_coding_prompt(row, time.time())
    if (not row or row['status'] != 'running' or row.get('worker_pid') or
            worker_ticket(row) != ticket or row.get('reservation_retirement') or
            (row.get('claim_deadline') is not None and time.time() >= row['claim_deadline']) or
            not Guard.renew(row, time.time())):
        raise ValueError('UNREGISTERED, stale, paused, cancelled, expired or claimed worker ticket; never restart')
    return row


def admit_received_coding_prompt(row, now):
    """Extend only a timely, durable prompt in the exact canonical Coding session.

    A copied historical message ID is not delivery: timestamp, session,
    generation, nonce and live process/lane must all agree. Read failure leaves
    the original deadline in force. The receipt never authorizes a second send.
    """
    import sqlite3

    binding = row.get('coding_prompt_binding')
    if (not binding or row.get('status') != 'running' or row.get('claim_phase') != 'reserved' or
            row.get('visible_sent') or row.get('worker_pid') or row.get('coding_prompt_receipt') or
            row.get('claim_deadline') is None or now < row['claim_deadline']):
        return False
    original_deadline = row['claim_deadline']
    try:
        if bound_coding_prompt(preflight_coding_coordinator()) != binding:
            return False
        db = Path(row['identity']['home']).parent / 'diggr-coding' / 'state.db'
        with sqlite3.connect(f'file:{db}?mode=ro', uri=True, timeout=1) as connection:
            session = connection.execute(
                'SELECT source, profile_name, ended_at FROM sessions WHERE id=?',
                (binding['session'],)).fetchone()
            if session != ('cli', 'diggr-coding', None):
                return False
            messages = connection.execute(
                "SELECT id, timestamp, content FROM messages WHERE session_id=? AND role='user' "
                'AND timestamp>=? AND timestamp<? ORDER BY timestamp, id',
                (binding['session'], row['registered_at'], original_deadline)).fetchall()
        ticket = worker_ticket(row)
        marker = 'SYSTEM167_WORKER_TICKET '
        decoder = json.JSONDecoder()
        for message_id, received_at, content in messages:
            if not isinstance(content, str) or not content.startswith(
                    'Hermes Coding task: Loop Control visible Coding dispatch for '):
                continue
            if content.count(marker) != 1:
                continue
            candidate, _ = decoder.raw_decode(content.split(marker, 1)[1])
            if candidate != ticket:
                continue
            deadline = claim_deadline(row, received_at, RECEIVED_PROMPT_CLAIM_SECONDS)
            if now >= deadline:
                return False
            row['coding_prompt_receipt'] = dict(session=binding['session'], message_id=message_id,
                                                received_at=received_at,
                                                content_sha256=hashlib.sha256(content.encode()).hexdigest())
            row['claim_deadline'] = deadline
            return True
    except (OSError, sqlite3.Error, ValueError, TypeError, KeyError, IndexError):
        return False
    return False


def accept_coding_assignment(message, session_id, coding_home):
    """Persist an offered assignment before the Coding CLI calls the model.

    The terminal token identifies work; the existing confirmed batch and the
    exact current Coding process/lane authorize its acceptance. A copied token
    from another process or an old batch cannot create execution authority.
    """
    marker = 'SYSTEM171_ASSIGN '
    if not isinstance(message, str) or not message.startswith(marker):
        return None
    reference = message[len(marker):]
    if (len(reference) != 48 or any(ch not in '0123456789abcdef' for ch in reference)
            or not isinstance(session_id, str) or not session_id):
        raise ValueError('malformed native Coding assignment')
    home = Path(coding_home).resolve()
    if home.name != 'diggr-coding':
        raise ValueError('Coding assignment requires the diggr-coding profile')
    binding = preflight_coding_coordinator()
    if binding['process']['identity'].get('pid') != os.getpid():
        raise ValueError('Coding assignment belongs to another foreground process')
    guard = runtime_guard(home.parent / 'diggr-main')
    if guard is None:
        raise ValueError('native Main assignment store unavailable')
    with guard.transaction() as rows:
        now = time.time()
        matches = [(bid, batch, batch.get('item', {}))
                   for bid, batch in rows.grants.get('batches', {}).items()
                   if batch.get('item', {}).get('coding_assignment', {}).get('id') == reference]
        if len(matches) != 1:
            raise ValueError('Coding assignment is unknown or ambiguous')
        bid, batch, item = matches[0]
        assignment = item['coding_assignment']
        correction = (item.get('phase') == 'correction_needed' and
                      assignment.get('correction_of') is not None)
        if correction:
            if not delivery.canonical_plain_correction(bid, batch, item, rows, now):
                raise ValueError('Coding correction has no bound prior result')
        if (batch.get('flow_version') != 3 or batch.get('revoked') or
                item.get('phase') not in {'prepared', 'correction_needed'} or
                item.get('preparation_blocker') or
                (batch.get('tasks') and not correction) or assignment.get('binding') != binding
                or (assignment.get('rebind') and assignment['rebind'].get('target') != binding)
                or now >= coding_assignment_deadline(batch, rows, now, bid=bid)):
            raise ValueError('Coding assignment is stale, stopped or bound elsewhere')
        if assignment['status'] not in {'sending', 'sent', 'uncertain', 'accepted'}:
            raise ValueError('Coding assignment was not sent by the native runtime')
        if (delivery.recovered_preparation_origin(item, rows, now, bid=bid, batch=batch) and
                (item.get('assignment_renewals') or
                 len(item.get('coding_assignment_history') or []) > 1 or
                 not assignment.get('preparation_recovery_of'))):
            from hermes_cli import diggr_item
            _, contract = diggr_item.bound(rows, batch['coordinator_identity'], bid, item['issue'])
            if (not delivery.recovery_renewal_effect_free(
                    rows, bid, batch, contract, reference, sent=True) or
                    not diggr_item.worktree_matches(contract, item)):
                raise ValueError('recovered Coding renewal changed before acceptance')
        if assignment['status'] == 'accepted':
            if assignment.get('session') != session_id:
                raise ValueError('Coding assignment already accepted in another session')
            return False
        now = time.time()
        if (now >= coding_assignment_deadline(batch, rows, now, bid=bid) or
                (correction and not delivery.canonical_plain_correction(
                    bid, batch, item, rows, now))):
            raise ValueError('Coding assignment authority expired before acceptance')
        assignment.update(status='accepted', session=session_id, accepted_at=now)
    return True


def continue_received_coding_assignment(message, session_id, coding_home):
    """Advance one newly accepted native assignment without a model handoff.

    The exact terminal reference still authorizes only acceptance. Registration
    and send use their existing independent scope, process, budget and route
    checks. A replay never creates another worker.
    """
    if not accept_coding_assignment(message, session_id, coding_home):
        return None
    reference = message[len('SYSTEM171_ASSIGN '):]
    home = Path(coding_home).resolve()
    guard = runtime_guard(home.parent / 'diggr-main')
    with guard.transaction(read_only=True) as rows:
        matches = [(bid, batch) for bid, batch in rows.grants.get('batches', {}).items()
                   if batch.get('item', {}).get('coding_assignment', {}).get('id') == reference]
        if len(matches) != 1:
            raise ValueError('accepted Coding assignment is unknown or ambiguous')
        bid, batch = matches[0]
        item = batch['item']
        from hermes_cli.diggr_item import bound
        _, contract = bound(rows, batch['coordinator_identity'], bid, item['issue'])
    coding_prompt = (
        'Implement the confirmed item action: ' + contract['action'] + '\n'
        'Use the bound scope and acceptance criteria. For a correction, resolve '
        'every current bound finding. Run the required tests, typechecks and build. '
        'Return the result for Main validation. Stop before commit, push or PR.'
    )
    from hermes_cli.diggr_item import assignment_bound_prompt
    approved_prompt = assignment_bound_prompt(bid, batch, contract)
    if approved_prompt is not None:
        coding_prompt = approved_prompt
    guard, registered = register_assigned_worker(reference, session_id, home, coding_prompt)
    return start_assigned_worker(guard, registered)


def record_coding_start_failure(message, session_id, coding_home, error):
    """Retain a pre-registration refusal on its accepted assignment.

    Never infer a failed worker send: registered or sent assignments belong to
    the existing task observer and are deliberately left untouched here.
    """
    marker = 'SYSTEM171_ASSIGN '
    if not isinstance(message, str) or not message.startswith(marker):
        return False
    home = Path(coding_home).resolve()
    if home.name != 'diggr-coding':
        return False
    guard = runtime_guard(home.parent / 'diggr-main')
    if guard is None:
        return False
    reference = message[len(marker):]
    with guard.transaction() as rows:
        matches = [batch for batch in rows.grants.get('batches', {}).values()
                   if batch.get('item', {}).get('coding_assignment', {}).get('id') == reference]
        if len(matches) != 1:
            return False
        assignment = matches[0]['item']['coding_assignment']
        if (assignment.get('status') != 'accepted' or
                assignment.get('session') != session_id or assignment.get('task')):
            return False
        assignment.setdefault('start_failure', dict(
            at=time.time(), reason=' '.join(str(error).split())[:300],
            kind=type(error).__name__))
        return True


def verify_scope_amendment_correction(batch, item, prior, contract):
    """Bind the one approved addition to its original result across corrections."""
    from hermes_cli.diggr_item import APP105_TEST_SCOPE_PATH
    amendment = item['scope_amendment']
    original = batch['manifest']['todos'][0]['contract']
    if (amendment.get('added_path') != APP105_TEST_SCOPE_PATH or
            amendment.get('prior_contract_sha256') != owner_policy.binding_hash(original) or
            amendment.get('contract_sha256') != owner_policy.binding_hash(contract)):
        raise ValueError('approved scope amendment differs from original contract')

    def bound_result(entry, expected_contract):
        result = entry.get('worker_result') or {}
        route = entry.get('worker_route') or {}
        evidence = result.get('evidence') or {}
        if (entry.get('logical_contract') != expected_contract or
                any(result.get(key) != value for key, value in
                    dict(task=prior['task'], artifact=entry.get('artifact'),
                         action_id=entry.get('action_id'),
                         effect_id=entry.get('effect_id')).items()) or
                type(result.get('generation')) is not int or
                type(result.get('exit_code')) is not int or
                result.get('artifact_status') != 'present' or
                not isinstance(evidence.get('sha256'), str) or
                route.get('worktree') != item['worktree'] or
                route.get('branch') != item['branch']):
            raise ValueError('scope amendment correction result or route unbound')
        verified_bytes(entry['artifact'], evidence['sha256'])
        packet = read_hashed_json(route['packet'], route['packet_sha256'])
        if (packet.get('worktree') != item['worktree'] or
                packet.get('branch') != item['branch'] or
                packet.get('logical_resources') != expected_contract['resources']):
            raise ValueError('scope amendment correction packet differs from contract')
        return result, packet

    def reconciled_nonstart(entry, previous_generation):
        """Permit only a hashed, uniquely bound no-effect recovery between results."""
        if (entry.get('worker_result') is not None or
                entry.get('effect_status') != 'no_effect' or
                any(entry.get(key) is not None for key in ('logical_contract', 'visible_sent', 'worker_pid')) or
                Path(entry.get('artifact', '')).exists()):
            raise ValueError('scope amendment correction history has unverified attempt')
        matches = [snapshot for snapshot in prior.get('history', [])
                   if (snapshot.get('action_id') == entry.get('action_id') and
                       snapshot.get('effect_id') == entry.get('effect_id') and
                       snapshot.get('artifact') == entry.get('artifact') and
                       snapshot.get('worker_route') == entry.get('worker_route') and
                       (snapshot.get('reconciliation') or {}).get('sha256') ==
                           entry.get('reconciliation_sha256') and
                       type(snapshot.get('generation')) is int and
                       snapshot['generation'] > previous_generation)]
        if len(matches) != 1:
            raise ValueError('scope amendment correction history has unverified attempt')
        snapshot = matches[0]
        route = entry['worker_route']
        if (snapshot.get('logical_contract') != contract or
                route.get('worktree') != item['worktree'] or
                route.get('branch') != item['branch'] or
                snapshot.get('worker_result') is not None or snapshot.get('visible_sent') or
                snapshot.get('worker_pid') or
                recovery_report(snapshot, snapshot['reconciliation']).get('outcome') != 'no_effect'):
            raise ValueError('scope amendment correction history has unverified attempt')
        packet = read_hashed_json(route['packet'], route['packet_sha256'])
        if (packet.get('worktree') != item['worktree'] or
                packet.get('branch') != item['branch'] or
                packet.get('logical_resources') != contract['resources']):
            raise ValueError('scope amendment correction history packet differs from contract')
        return snapshot['generation']

    if prior.get('logical_contract') == original:
        original_result, _ = bound_result(prior, original)
        if (original_result['evidence']['sha256'] != amendment.get('result_sha256') or
                amendment.get('retained_worktree_sha256') != _worktree_snapshot(item['worktree'])):
            raise ValueError('approved scope amendment differs from completed prior result')
        return

    history = prior.get('attempt_history') or []
    anchors = [index for index, entry in enumerate(history)
               if entry.get('logical_contract') == original and
               (entry.get('worker_result') or {}).get('evidence', {}).get('sha256') ==
                   amendment.get('result_sha256')]
    if prior.get('logical_contract') != contract or len(anchors) != 1:
        raise ValueError('approved scope amendment has no unique original result')
    anchor = history[anchors[0]]
    original_result, _ = bound_result(anchor, original)
    chain = history[anchors[0] + 1:] + [prior]
    previous_generation = original_result['generation']
    first_completed = True
    for entry in chain:
        if entry is not prior and entry.get('worker_result') is None:
            previous_generation = reconciled_nonstart(entry, previous_generation)
            continue
        result, packet = bound_result(entry, contract)
        if (result['generation'] <= previous_generation or
                (first_completed and packet.get('retained_worktree_sha256') !=
                    amendment.get('retained_worktree_sha256'))):
            raise ValueError('approved scope amendment correction chain differs')
        previous_generation = result['generation']
        first_completed = False


def register_assigned_worker(reference, session_id, coding_home, prompt):
    """Build the technical route from one accepted grant, not from model fields."""
    import secrets

    if (not isinstance(prompt, str) or not prompt.strip() or '\x00' in prompt or
            not isinstance(reference, str) or len(reference) != 48 or
            any(ch not in '0123456789abcdef' for ch in reference)):
        raise ValueError('accepted assignment and nonempty Coding prompt required')
    home = Path(coding_home).resolve()
    if home.name != 'diggr-coding':
        raise ValueError('Coding worker requires the diggr-coding profile')
    binding = preflight_coding_coordinator()
    if binding['process']['identity'].get('pid') != os.getpid():
        raise ValueError('Coding worker belongs to another foreground process')
    guard = runtime_guard(home.parent / 'diggr-main')
    if guard is None:
        raise ValueError('native Main assignment store unavailable')
    with guard.transaction(read_only=True) as rows:
        matches = [(bid, batch) for bid, batch in rows.grants.get('batches', {}).items()
                   if batch.get('item', {}).get('coding_assignment', {}).get('id') == reference]
        if len(matches) != 1:
            raise ValueError('Coding assignment is unknown or ambiguous')
        bid, batch = matches[0]
        item = batch['item']
        assignment = item['coding_assignment']
        from hermes_cli.diggr_item import bound, assignment_bound_prompt
        _, prompt_contract = bound(rows, batch['coordinator_identity'], bid, item['issue'])
        approved_prompt = assignment_bound_prompt(bid, batch, prompt_contract, prompt=prompt)
        prompt_hash = hashlib.sha256((prompt if approved_prompt is not None else prompt.strip()).encode()).hexdigest()
        if assignment.get('status') == 'registered':
            existing = rows.get(assignment.get('task'))
            if (batch.get('flow_version') != 3 or batch.get('revoked') or
                    item.get('phase') not in {'prepared', 'coding'} or
                    item.get('preparation_blocker') or not batch.get('native_transport') or
                    assignment.get('session') != session_id or assignment.get('binding') != binding or
                    (assignment.get('rebind') and assignment['rebind'].get('target') != binding) or
                    time.time() >= coding_assignment_deadline(batch, rows, bid=bid) or
                    not existing or existing.get('direct_worker') is not True or
                    existing.get('owner_batch') != bid or existing.get('owner_issue') != item['issue'] or
                    existing.get('assignment_prompt_sha256') != prompt_hash or
                    existing.get('visible_sent') or existing.get('worker_pid') or
                    existing.get('status') != 'running' or
                    (assignment.get('correction_of') is None and
                     batch.get('tasks') != {item['issue']: existing['task']}) or
                    (assignment.get('correction_of') is not None and item.get('phase') != 'coding')):
                raise ValueError('registered Coding assignment cannot be resumed safely')
            assignment_bound_prompt(bid, batch, prompt_contract, prompt=prompt, row=existing)
            verify_assigned_prompt(rows, existing)
            return guard, copy.deepcopy(existing)
        correction = (item.get('phase') == 'correction_needed' and
                      assignment.get('correction_of') is not None)
        prior = copy.deepcopy(rows.get(item.get('task'))) if correction else None
        if (batch.get('flow_version') != 3 or batch.get('revoked') or
                item.get('phase') not in {'prepared', 'correction_needed'} or
                item.get('preparation_blocker') or
                (batch.get('tasks') and not correction) or assignment.get('status') != 'accepted' or
                not batch.get('native_transport') or
                assignment.get('session') != session_id or assignment.get('binding') != binding or
                (assignment.get('rebind') and assignment['rebind'].get('target') != binding) or
                time.time() >= coding_assignment_deadline(batch, rows, bid=bid)):
            raise ValueError('accepted Coding assignment is no longer startable')
        if correction:
            now = time.time()
            if (not delivery.canonical_plain_correction(bid, batch, item, rows, now) or
                    prior['policy']['corrections'] >= prior['policy'].get('limits', {}).get('corrections', 2)):
                raise ValueError('Coding correction lacks prior result or budget')
        identity = copy.deepcopy(batch['coordinator_identity'])
        from hermes_cli.diggr_item import bound
        _, contract = bound(rows, identity, bid, item['issue'])
        amendment = copy.deepcopy(item.get('scope_amendment'))
    owner_policy.validate_logical_contract(contract)
    if correction:
        verify_direct_worker_exit(prior, completed=True)
        validate_logical_changes(prior)
        if amendment:
            verify_scope_amendment_correction(batch, item, prior, contract)
    from hermes_cli.diggr_item import onboarding_payload, current_correction_findings
    correction_findings = (current_correction_findings(item, prior, contract)
                           if correction else None)
    skill, _ = onboarding_payload()
    root = Path(contract['resources']['artifacts'])
    packet_root = logical_packet_root()
    if packet_root.is_relative_to(root):
        packet_dir = packet_root
    elif root.is_relative_to(packet_root):
        packet_dir = root
    else:
        raise ValueError('confirmed packet and artifact roots do not intersect')
    packet_dir.mkdir(parents=True, exist_ok=True)
    root.mkdir(parents=True, exist_ok=True)
    attempt = secrets.token_hex(12)
    prefix = 'flow3-' + reference[:16] + '-' + attempt
    artifact = str(root / (prefix + '-result.txt'))
    route = dict(packet=str(packet_dir / (prefix + '-route.json')),
                 receipt=str(root / (prefix + '-receipt.json')),
                 worktree=item['worktree'], branch=item['branch'],
                 plane_id=contract['plane_id'], visible_target=copy.deepcopy(item['target']))
    model = contract['model']
    scoped_prompt = (skill + '\n\nConfirmed work boundary:\n' +
                     json.dumps(dict(plane_id=contract['plane_id'], scope=contract['scope'],
                                     allowed_paths=contract['resources']['allowed_paths'],
                                     no_touch=contract['resources']['no_touch'],
                                     acceptance=contract['acceptance']['criteria']),
                                sort_keys=True, ensure_ascii=False) +
                     '\n\nCoding plan:\n' + prompt.strip() +
                     ('\n\nRequired current correction findings (complete these before reporting success):\n' +
                      json.dumps(correction_findings, ensure_ascii=False)
                      if correction else ''))
    if approved_prompt is not None:
        scoped_prompt = approved_prompt  # Owner approved every byte, including the full onboarding.
    argv = [CODEX_EXECUTABLE, 'exec', '--model', model['required_model'],
            '--sandbox', model['sandbox'], '--cd', item['worktree'],
            '--output-last-message', artifact,
            '-c', 'model_provider=' + json.dumps(owner_policy.codex_cli_provider(model['provider'])),
            '-c', 'model_reasoning_effort=' + json.dumps(model['reasoning_effort']),
            '-c', 'approval_policy=' + json.dumps(model['approval_policy']),
            '--', scoped_prompt]
    route['argv'] = argv
    packet = dict(routing_mode='non_cmm', operator_scope_authorized=True,
                  plane_id=contract['plane_id'], task_id=contract['plane_id'],
                  scope_id=contract['plane_id'], worktree=item['worktree'],
                  branch=item['branch'], base=item['base'], coding_lane='audit_gated',
                  requested_actions=list(contract['actions']['coding']),
                  logical_resources=copy.deepcopy(contract['resources']),
                  acceptance=copy.deepcopy(contract['acceptance']),
                  role_actions=copy.deepcopy(contract['actions']),
                  coding_route=dict(copy.deepcopy(model),
                                    routine_actions_authorized=list(contract['actions']['coding']),
                                    merge_authorized=False))
    if approved_prompt is not None:
        source = assignment['prompt_binding']['prompt']
        packet.update(coding_prompt_source=source['path'], coding_prompt_sha256=source['sha256'])
    if correction:
        actual_root, actual_branch, current_head, _ = _git_route_identity(item['worktree'])
        if actual_root != item['worktree'] or actual_branch != item['branch']:
            raise ValueError('Coding correction worktree changed')
        packet['base'] = current_head
        packet['retained_worktree_sha256'] = _worktree_snapshot(item['worktree'])
    elif item.get('migration'):
        actual_root, actual_branch, current_head, _ = _git_route_identity(item['worktree'])
        retained = _worktree_snapshot(item['worktree'])
        if (actual_root != item['worktree'] or actual_branch != item['branch'] or
                current_head != item['migration']['head'] or
                retained != item['migration']['retained_worktree_sha256']):
            raise ValueError('migrated worktree changed after native Owner confirmation')
        packet['base'] = current_head
        packet['retained_worktree_sha256'] = retained
    # Technical copies are generated from the confirmed contract and sealed.
    # They are never supplied as a second authority by Coding or Main.
    with Path(route['packet']).open('x', encoding='utf-8') as output:
        json.dump(packet, output, sort_keys=True)
        output.flush()
        os.fsync(output.fileno())
    route['packet_sha256'] = digest(route['packet'])
    task = dict(task=prior['task'] if correction else prefix,
                scope=contract['scope'], identity=identity,
                owner=contract['owner'], gate=contract['gate'], action=contract['action'],
                producer=contract['producer'], artifact=artifact,
                deadline=time.time() + owner_policy.BUDGETS['todo_seconds'],
                wake_budget=owner_policy.BUDGETS['wakes'],
                authorization=(item.get('migration') or {}).get('owner_event') or
                    batch['confirmation_event'], owner_batch=bid,
                owner_issue=item['issue'], worker_route=route, direct_worker=True,
                launcher_command=logical_launcher_command(contract, route),
                codex_home=canonical_path(os.environ.get('CODEX_HOME') or
                                          str(Path.home().resolve() / '.codex')),
                ticket_nonce=secrets.token_hex(32),
                visible_binding=bind_visible_target(item['target']),
                coding_prompt_binding=bound_coding_prompt(binding))
    task['assignment_prompt_sha256'] = prompt_hash
    if approved_prompt is not None:
        task['assignment_renewal_sha256'] = owner_policy.binding_hash(item['assignment_renewals'][-1])
    task['visible_shell_identity'] = bound_shell_identity(task['visible_binding'])
    logical_binding = validate_logical_route(task, contract, fresh=not correction)
    admission = object_hash(dict(assignment=reference,
                                 prompt_sha256=hashlib.sha256(argv[-1].encode()).hexdigest()))
    if correction:
        with guard.transaction() as rows:
            current = rows[prior['task']]
            active = rows.grants['batches'][bid]
            assignment = active['item']['coding_assignment']
            if (current != prior or active.get('revoked') or
                    active['item'].get('phase') != 'correction_needed' or
                    assignment.get('id') != reference or assignment.get('status') != 'accepted' or
                    active['item'].get('scope_amendment') != amendment or
                    active['item'].get('correction') != item.get('correction') or
                    active['item'].get('candidate') != item.get('candidate') or
                    active.get('native_transport') != prior.get('native_transport') or
                    time.time() >= coding_assignment_deadline(active, rows, bid=bid) or
                    not delivery.canonical_plain_correction(
                        bid, active, active['item'], rows)):
                raise ValueError('Coding correction changed during validation')
            verify_direct_worker_exit(current, completed=True)
            validate_logical_changes(current)
            if current_correction_findings(active['item'], current, contract) != correction_findings:
                raise ValueError('Coding correction findings changed during validation')
            if amendment:
                verify_scope_amendment_correction(active, active['item'], current, contract)
            now = time.time()
            if (not delivery.canonical_plain_correction(
                    bid, active, active['item'], rows, now) or
                    current['policy']['corrections'] >= current['policy'].get('limits', {}).get('corrections', 2)):
                raise ValueError('confirmed correction authority or budget exhausted')
            current.setdefault('attempt_history', []).append(dict(
                artifact=current['artifact'], worker_route=copy.deepcopy(current['worker_route']),
                action_id=current['action_id'], effect_id=current['effect_id'],
                worker_result=copy.deepcopy(current['worker_result']),
                logical_contract=copy.deepcopy(current['logical_contract'])))
            for key in ('worker_pid', 'worker_identity', 'worker_started_ns', 'child_identity',
                        'child_exited', 'worker_result', 'worker_descendants', 'worker_supervision', 'visible_sent',
                        'visible_sent_at', 'send_request_sha256', 'receipt_sha256',
                        'non_cmm_receipt_sha256', 'coordinator_delivery', 'evidence',
                        'late_evidence', 'recovery_kind', 'reason'):
                current.pop(key, None)
            current['policy']['corrections'] += 1
            current.update(generation=current['generation'] + 1,
                           registered_at=now,
                           ticket_nonce=task['ticket_nonce'],
                           action_id=uuid.uuid4().hex, effect_id=uuid.uuid4().hex,
                           worker_route=route, artifact=artifact,
                           launcher_command=task['launcher_command'],
                           visible_binding=task['visible_binding'],
                           visible_shell_identity=task['visible_shell_identity'],
                           coding_prompt_binding=task['coding_prompt_binding'],
                           assignment_prompt_sha256=task['assignment_prompt_sha256'],
                           native_admission_sha256=admission,
                           status='running', owner='coding', gate='coding',
                           action=contract['action'], claim_phase=None, due=time.time())
            # A correction is a new unsent attempt on the same row. Refresh its
            # observer grace and coordination lease before exposing it as running.
            if not Guard.renew(current, now):
                raise ValueError('confirmed correction authority or budget exhausted')
            current['logical_contract'] = copy.deepcopy(contract)
            active['item'].setdefault('correction_history', []).append(
                active['item'].pop('correction'))
            active['item']['phase'] = 'coding'
            assignment.update(status='registered', task=current['task'])
    else:
        guard.register(task, _logical_binding=logical_binding,
                       _native_admission=admission, _coding_assignment=reference)
    return guard, guard.get(task['task'])


def start_assigned_worker(guard, registered):
    """Seal one existing route and send its visible worker command once."""
    with guard.transaction(read_only=True) as rows:
        row = copy.deepcopy(rows[registered['task']])
        verify_assigned_prompt(rows, row)
        batch = rows.grants['batches'][row['owner_batch']]
        assignment = batch['item']['coding_assignment']
        if (batch.get('flow_version') != 3 or batch.get('revoked') or
                assignment.get('task') != row['task'] or assignment.get('status') != 'registered' or
                row.get('visible_sent') or row.get('worker_pid')):
            raise ValueError('assigned worker is not in its fresh registered state')
    route = row['worker_route']
    receipt = preflight_non_cmm(copy.deepcopy(row), route)
    receipt_path = Path(route['receipt'])
    try:
        with receipt_path.open('x', encoding='utf-8') as output:
            json.dump(receipt, output, sort_keys=True)
            output.flush()
            os.fsync(output.fileno())
    except FileExistsError:
        # A crash after the exclusive write but before ledger sealing leaves
        # the same receipt on disk. Adopt only those exact bytes, never replace.
        if (receipt_path.is_symlink() or not receipt_path.is_file() or
                json.loads(receipt_path.read_text(encoding='utf-8')) != receipt):
            raise ValueError('existing route receipt differs; start effect unresolved') from None
    with guard.transaction() as rows:
        current = rows[row['task']]
        batch = rows.grants['batches'][row['owner_batch']]
        assignment = batch['item']['coding_assignment']
        same_row = current == row
        # The Gateway may renew only this item lease during external preflight.
        # Keep every route, identity, budget and send field byte-for-byte bound.
        if (not same_row and row.get('policy', {}).get('item_lifecycle') and
                isinstance(current.get('policy', {}).get('deadline'), (int, float)) and
                current['policy']['deadline'] >= row['policy']['deadline']):
            before = copy.deepcopy(row)
            before['policy']['deadline'] = current['policy']['deadline']
            same_row = before == current
        if (not same_row or batch.get('revoked') or
                assignment.get('task') != row['task'] or assignment.get('status') != 'registered'):
            raise ValueError('assigned worker changed during route preflight')
        verify_assigned_prompt(rows, current)
        if not Guard.renew(current, time.time()):
            raise ValueError('assigned worker authority expired during route preflight')
        current['non_cmm_receipt_sha256'] = digest(route['receipt'])
        route_check(current, route, seal=True)
        verify_visible_target(current['visible_binding'], idle=True)
        ticket = worker_ticket(current)
        command = visible_worker_command(ticket)
        sent_at = time.time()
        current.update(visible_sent=True, visible_sent_at=sent_at, claim_phase='sent',
                       send_request_sha256=object_hash(ticket),
                       claim_deadline=claim_deadline(current, sent_at, 30))
        assignment['status'] = 'sending_worker'
    # The send reservation is durable before I/O. A missing response remains
    # uncertain; a delayed launcher can only claim this same registered ticket.
    with guard.transaction() as rows:
        current = validate_ticket(rows, ticket)
        assignment = rows.grants['batches'][current['owner_batch']]['item']['coding_assignment']
        if (not current.get('visible_sent') or current.get('send_request_sha256') != object_hash(ticket)
                or assignment.get('status') != 'sending_worker'):
            raise ValueError('assigned worker send reservation changed')
        cmux_send(current['visible_binding'], command)
        assignment['status'] = 'worker_sent'
    return dict(task=row['task'], generation=row['generation'], sent=True,
                state=str(guard.path))


def coding_assignment_deadline(batch, rows=None, now=None, *, bid=None):
    """The confirmed batch bound, including batches without a delivery slot."""
    item = batch.get('item', {})
    history = item.get('coding_assignment_history') or []
    assignment = item.get('coding_assignment') or {}
    correction = assignment.get('correction_of') is not None
    if correction and not delivery.canonical_plain_correction(bid, batch, item, rows, now):
        return float('-inf')
    recovered_origin = delivery.recovered_preparation_origin(
        item, rows, now, bid=bid, batch=batch)
    preparation_recovery = item.get('preparation_assignment_recovery')
    renewals = item.get('assignment_renewals') or []
    if len(renewals) == 2:
        if (not recovered_origin or item.get('migration') or correction or not preparation_recovery or
                len(history) != 3 or assignment.get('second_renewal') is not True):
            return float('-inf')
        prior = copy.deepcopy(batch)
        prior['item']['coding_assignment'] = history[-1]
        prior['item']['coding_assignment_history'] = history[:-1]
        prior['item']['assignment_renewals'] = renewals[:-1]
        previous = coding_assignment_deadline(prior, rows, now, bid=bid)
        renewal = renewals[-1]
        core = renewal.get('core') or {}
        activated, deadline = renewal.get('activated_at'), renewal.get('assignment_deadline')
        if (not math.isfinite(previous) or renewal.get('old_assignment') != history[-1].get('id') or
                renewal.get('new_assignment') != assignment.get('id') or
                assignment.get('id') in {h.get('id') for h in history} or
                renewal.get('preview_sha256') != owner_policy.binding_hash(core) or
                core.get('expired_at') != previous or core.get('renewal_number') != 2 or
                not core.get('prompt_binding') or assignment.get('prompt_binding') != core['prompt_binding'] or
                type(renewal.get('lane_retry_used')) is not bool or
                not all(type(v) in (int, float) and math.isfinite(v) for v in (activated, deadline)) or
                renewal.get('previous_deadline') != previous or deadline != activated + 600):
            return float('-inf')
        return deadline
    if recovered_origin and (item.get('migration') or assignment.get('correction_of') is not None):
        return float('-inf')
    if recovered_origin:
        if not preparation_recovery:
            return float('-inf')
        if renewals:
            if (len(renewals) != 1 or len(history) != 2 or
                    assignment.get('preparation_recovery_of') is not None or
                    history[1].get('preparation_recovery_of') != history[0].get('id') or
                    preparation_recovery.get('old_assignment') != history[0].get('id') or
                    preparation_recovery.get('new_assignment') != history[1].get('id') or
                    renewals[0].get('old_assignment') != history[1].get('id') or
                    renewals[0].get('new_assignment') != assignment.get('id') or
                    assignment.get('id') == history[1].get('id')):
                return float('-inf')
        elif (len(history) != 1 or
              assignment.get('preparation_recovery_of') != history[0].get('id') or
              preparation_recovery.get('old_assignment') != history[0].get('id') or
              preparation_recovery.get('new_assignment') != assignment.get('id') or
              assignment.get('id') == history[0].get('id')):
            return float('-inf')
    elif renewals and not item.get('migration') and not preparation_recovery:
        return float('-inf')
    if preparation_recovery:
        original = preparation_recovery.get('assignment_deadline')
        created_at = preparation_recovery.get('created_at')
        if (not all(type(value) in (int, float) and math.isfinite(value)
                    for value in (created_at, original)) or original != created_at + 600):
            return float('-inf')
        if renewals:
            if len(renewals) != 1:
                return float('-inf')
            renewal = renewals[0]
            activated_at = renewal.get('activated_at')
            deadline = renewal.get('assignment_deadline')
            if (not all(type(value) in (int, float) and math.isfinite(value)
                        for value in (activated_at, deadline)) or
                    type(renewal.get('lane_retry_used')) is not bool or
                    renewal.get('previous_deadline') != original or
                    deadline != activated_at + 600):
                return float('-inf')
            if assignment.get('correction_of') is not None:
                return float('inf')  # Corrections retain their cumulative work allowance.
            return deadline
    if assignment.get('correction_of') is not None:
        return float('inf')  # Item work is bounded by its original cumulative work allowance.
    migration = item.get('migration')
    if migration:
        return migration['assignment_deadline']
    if preparation_recovery:
        return original
    return (batch.get('deadline') or
            batch.get('coordinator_delivery', {}).get('deadline') or
            batch['confirmed_at'] + owner_policy.BUDGETS['batch_seconds'])


def _git_route_identity(worktree):
    """Read only: never fetch, switch branches or change the index."""
    import subprocess

    def git(*args):
        return subprocess.check_output(
            ['git', '--no-optional-locks', '-C', worktree, *args],
            text=True, timeout=10, stderr=subprocess.PIPE,
        ).strip()

    return (git('rev-parse', '--show-toplevel'),
            git('symbolic-ref', '--quiet', '--short', 'HEAD'),
            git('rev-parse', 'HEAD'), git('status', '--porcelain'))


def _worktree_snapshot(worktree):
    """Seal retained tracked/index changes and untracked regular files, read-only."""
    import subprocess
    root = Path(worktree).resolve()
    def git(*args):
        return subprocess.check_output(['git', '--no-optional-locks', '-C', str(root), *args],
                                       timeout=10, stderr=subprocess.PIPE)
    parts = [git('status', '--porcelain=v1', '-z', '--untracked-files=all'),
             git('diff', '--binary', '--no-ext-diff', '--no-textconv', 'HEAD'),
             git('diff', '--cached', '--binary', '--no-ext-diff', '--no-textconv', 'HEAD')]
    for name in git('ls-files', '--others', '--exclude-standard', '-z').split(b'\0'):
        if not name:
            continue
        path = root / os.fsdecode(name)
        if path.is_symlink() or not path.is_file() or not path.resolve().is_relative_to(root):
            raise ValueError('retained worktree requires local regular files')
        parts.extend((name, path.read_bytes()))
    seal = hashlib.sha256()
    for part in parts:
        seal.update(len(part).to_bytes(8, 'big'))
        seal.update(part)
    return seal.hexdigest()


def _retained_worktree_matches(worktree, packet):
    expected = packet.get('retained_worktree_sha256')
    return (isinstance(expected, str) and len(expected) == 64 and
            all(c in '0123456789abcdef' for c in expected) and
            _worktree_snapshot(worktree) == expected)


def validate_codex_argv(row):
    bound = row['worker_route']
    argv = bound['argv']
    if (not isinstance(argv, list) or len(argv) < 4 or
            any(not isinstance(a, str) or not a or '\x00' in a for a in argv) or
            not Path(argv[0]).is_absolute() or Path(argv[0]).name != 'codex' or argv[1] != 'exec'):
        raise ValueError('explicit Codex exec argv required')
    # Deliberately narrow supported CLI grammar. No later model/config/profile
    # override, resume, full-auto or sandbox bypass can shadow the pinned flags.
    options = {}
    config = {}
    has_prompt_separator = argv[-2] == '--'
    if '--' in argv[2:-2] or (row.get('direct_worker') is True and
                              row.get('logical_contract', {}).get('version') == owner_policy.ITEM_VERSION and
                              not has_prompt_separator):
        raise ValueError('unsupported Codex argv')
    args = argv[2:-2] if has_prompt_separator else argv[2:-1]
    if len(args) % 2 or (not has_prompt_separator and argv[-1].startswith('-')):
        raise ValueError('unsupported Codex argv')
    for key, value in zip(args[::2], args[1::2]):
        if key == '-c':
            name, separator, setting = value.partition('=')
            allowed_config = {'approval_policy': {'"never"'},
                              'model_reasoning_effort': {'"low"', '"medium"', '"high"', '"xhigh"', '"max"', '"ultra"'}}
            if row.get('logical_contract'):
                allowed_config['model_provider'] = {json.dumps(owner_policy.codex_cli_provider(
                    row['logical_contract']['model']['provider']))}
            if not separator or name in config or setting not in allowed_config.get(name, set()):
                raise ValueError('duplicate or unsupported Codex config override')
            config[name] = setting
            continue
        if key in options or key not in {'--model', '--sandbox', '--cd', '--output-last-message', '--color', '-c'}:
            raise ValueError('duplicate or unsupported Codex option')
        options[key] = value
    expected_model = (row.get('logical_contract') or {}).get('model', {}).get('required_model')
    if expected_model is None:
        packet = read_hashed_json(bound['packet'], bound['packet_sha256'])
        expected_model = packet.get('coding_route', {}).get('required_model')
    if (options.get('--model') != expected_model or options.get('--sandbox') != 'workspace-write' or
            options.get('--cd') != bound['worktree'] or options.get('--output-last-message') != row['artifact'] or
            options.get('--color', 'never') != 'never' or
            options.get('-c', 'approval_policy="never"') != 'approval_policy="never"'):
        raise ValueError('Codex model/worktree/output/sandbox mismatch')
    if row.get('logical_contract'):
        model = row['logical_contract']['model']
        if config != dict(model_provider=json.dumps(owner_policy.codex_cli_provider(model['provider'])),
                          model_reasoning_effort=json.dumps(model['reasoning_effort']),
                          approval_policy=json.dumps(model['approval_policy'])):
            raise ValueError('argv differs from confirmed provider/reasoning/approval contract')

    if row.get('logical_contract', {}).get('version') == owner_policy.ITEM_VERSION:
        from hermes_cli.diggr_item import onboarding_payload, ONBOARDING_SKILL
        content, _ = onboarding_payload()
        if content not in argv[-1]:
            raise ValueError('registered Coding prompt must include the complete ' + ONBOARDING_SKILL +
                             ' before registration; do not change an already bound worker argv')


def preflight_non_cmm(row, route, *, check_idle=True, legacy_expiry=None):
    """Validate a native-ticket-bound route without CMM or context measurement.

    Returns evidence, not authority: only route-preflight under validate_ticket
    writes/seals it. Dispatch still requires the native visible claim.
    """
    bound = row['worker_route']
    fields = ('packet', 'receipt', 'worktree', 'branch', 'plane_id', 'argv', 'visible_target')
    if any(route.get(k) != bound.get(k) for k in fields):
        raise ValueError('non-CMM route differs from native ticket')
    if not row.get('authorization') or not Guard.renew(row, time.time()):
        raise ValueError('non-CMM authorization missing or expired')
    for field in ('packet', 'receipt', 'worktree'):
        path = Path(bound[field])
        if not path.is_absolute() or path.is_symlink() or str(path.resolve()) != str(path):
            raise ValueError('canonical non-symlink route paths required')
    packet_path = Path(bound['packet'])
    if not packet_path.is_file() or digest(packet_path) != bound['packet_sha256']:
        raise ValueError('packet integrity mismatch')
    packet = json.loads(packet_path.read_text(encoding='utf-8'))
    if (packet.get('routing_mode') != 'non_cmm' or
            packet.get('operator_scope_authorized') is not True or
            any(packet.get(k) != bound['plane_id'] for k in ('plane_id', 'task_id', 'scope_id')) or
            packet.get('worktree') != bound['worktree'] or packet.get('branch') != bound['branch']):
        raise ValueError('explicit non-CMM scope/Plane/worktree binding required')
    contract = packet.get('coding_route', {})
    safe = {'inspect', 'edit', 'test', 'review', 'document_work_item', 'commit', 'push', 'pr_update'}
    requested = packet.get('requested_actions')
    allowed = contract.get('routine_actions_authorized')
    if (not isinstance(requested, list) or not requested or
            not isinstance(allowed, list) or not allowed or
            any(not isinstance(a, str) or a not in safe for a in requested + allowed) or
            not set(requested) <= set(allowed) or contract.get('merge_authorized') is not False or
            contract.get('executor') != 'codex' or
            not owner_policy.supported_codex_model(contract.get('required_model')) or
            contract.get('fallback_models_allowed') != []):
        raise ValueError('non-CMM action/model authorization rejected')
    validate_codex_argv(row)
    if row.get('logical_contract'):
        validate_logical_route(row, row['logical_contract'], fresh=False)
    actual_root, branch, head, dirty = _git_route_identity(bound['worktree'])
    if (actual_root != bound['worktree'] or branch != bound['branch'] or
            branch in {'main', 'master'} or head != packet.get('base') or
            ((dirty or packet.get('retained_worktree_sha256') is not None) and
             not _retained_worktree_matches(bound['worktree'], packet))):
        raise ValueError('clean exact-base feature worktree required')
    if bound['visible_target'] != row['visible_binding']['target']:
        raise ValueError('visible target differs from registered binding')
    verify_visible_target(row['visible_binding'], idle=check_idle)
    expires_at = (legacy_expiry if legacy_expiry is not None else
                  min((row[k] for k in ('hard_stop', 'authorization_expires_at')
                       if row.get(k) is not None), default=None))
    proof = dict(task=row['task'], generation=row['generation'],
                 authorization=row['authorization'], identity=row['identity'],
                 artifact=row['artifact'], argv=bound['argv'], binding=row['visible_binding'],
                 packet_sha256=bound['packet_sha256'], worktree=actual_root,
                 branch=branch, head=head, expires_at=expires_at)
    fingerprint = hashlib.sha256(json.dumps(proof, sort_keys=True).encode()).hexdigest()
    return dict(schema_version=('diggr.native.non_cmm_route.v1' if legacy_expiry is not None
                                else 'diggr.native.non_cmm_route.v2'), mode='non_cmm',
                verdict='allowed', route_packet=bound['packet'],
                packet_sha256=bound['packet_sha256'], task=row['task'],
                generation=row['generation'], expires_at=expires_at,
                binding_sha256=fingerprint,
                coding_route_contract=dict(contract, plane_id=bound['plane_id'],
                                           operator_scope_authorized=True))


def route_check(row, route, seal=False):
    if delivery.transport_for(row) is None:
        raise ValueError('native transport provenance required before worker route')
    bound = row['worker_route']
    if any(route.get(k) != bound[k] for k in ('packet', 'receipt', 'worktree', 'branch', 'plane_id')):
        raise ValueError('routing target differs from native authorized ticket')
    if digest(bound['packet']) != bound['packet_sha256']:
        raise ValueError('routing packet changed since authorization')
    if seal or row.get('receipt_sha256'):
        receipt_path = Path(bound['receipt'])
        if receipt_path.is_symlink():
            raise ValueError('symlink receipt refused')
        receipt = json.loads(receipt_path.read_text(encoding='utf-8'))
        packet = json.loads(Path(bound['packet']).read_text(encoding='utf-8'))
        if packet.get('routing_mode') == 'non_cmm':
            if row.get('non_cmm_receipt_sha256') != digest(receipt_path):
                raise ValueError('non-CMM receipt not issued by native preflight')
            # PR9's sealed deadline was an execution deadline, not user expiry.
            # Only accept the old format when its bytes were already sealed.
            legacy = (receipt.get('expires_at') if
                      receipt.get('schema_version') == 'diggr.native.non_cmm_route.v1' and
                      row.get('receipt_sha256') == digest(receipt_path) else None)
            expected = preflight_non_cmm(row, bound, check_idle=False, legacy_expiry=legacy)
            if receipt != expected:
                raise ValueError('non-CMM receipt stale or mismatched')
        contract = receipt.get('coding_route_contract', {})
        if (receipt.get('verdict') != 'allowed' or contract.get('operator_scope_authorized') is not True or
                contract.get('plane_id') != bound['plane_id'] or
                receipt.get('route_packet') != bound['packet']):
            raise ValueError('routing receipt has no authorized passing contract')
        current = digest(bound['receipt'])
        if row.get('receipt_sha256') and row['receipt_sha256'] != current:
            raise ValueError('routing receipt changed after binding')
        if seal:
            row['receipt_sha256'] = current


DISPATCHER = '/Users/diggr/Projects/diggr_claw/ops/scripts/loop_control_dispatch_coding.sh'
CODEX_EXECUTABLE = '/Users/diggr/.npm-global/bin/codex'


def logical_packet_root():
    """Authoritative dispatcher packet root, shared with proposal prevalidation."""
    return (Path(DISPATCHER).parents[2] / 'ops/state/loop-control/routing_packets').resolve()


def logical_launcher_command(contract, route):
    """Only the existing dispatcher and data arguments, never arbitrary shell text."""
    import shlex
    system_root = str(Path(DISPATCHER).parents[2])
    values = dict(LOOP_CONTROL_ROUTING_PACKET=route['packet'],
        LOOP_CONTROL_PLANE_ID=route['plane_id'], LOOP_CONTROL_APP_REPO_PATH=contract['resources']['repo'],
        LOOP_CONTROL_APP_WORKTREE_PATH=route['worktree'], LOOP_CONTROL_APP_BRANCH=route['branch'],
        LOOP_CONTROL_SYSTEM_REPO_ROOT=system_root, LOOP_CONTROL_ROUTE_RECEIPT=route['receipt'])
    return shlex.join(['/usr/bin/env', '-u', 'LOOP_CONTROL_CMUX_BIN',
        '-u', 'LOOP_CONTROL_CODING_SURFACE', '-u', 'LOOP_CONTROL_CODING_WORKSPACE',
        '-u', 'BASH_ENV'] + [key + '=' + value for key, value in values.items()] + [DISPATCHER])


def validate_logical_changes(task):
    """Actual paths, including staged/untracked/renamed paths, before acceptance."""
    import subprocess
    from pathlib import PurePosixPath
    route = task['worker_route']
    worktree = Path(canonical_path(route['worktree']))
    resources = task['logical_contract']['resources']
    packet = read_hashed_json(route['packet'], route['packet_sha256'])
    def git(*args):
        return subprocess.check_output(['git', '--no-optional-locks', '-C', str(worktree), *args],
            timeout=10, stderr=subprocess.PIPE)
    repo_common = subprocess.check_output(['git', '--no-optional-locks', '-C', canonical_path(resources['repo']),
        'rev-parse', '--path-format=absolute', '--git-common-dir'], timeout=10, stderr=subprocess.PIPE)
    if (git('rev-parse', '--show-toplevel').decode().strip() != str(worktree) or
            git('rev-parse', '--path-format=absolute', '--git-common-dir') != repo_common or
            git('symbolic-ref', '--quiet', '--short', 'HEAD').decode().strip() != route['branch']):
        raise ValueError('logical acceptance worktree/branch changed')
    paths = git('diff', '--no-ext-diff', '--no-textconv', '--no-renames', '--name-only', '-z', packet['base'], '--')
    # The index can differ even when worktree bytes were restored to the base.
    # Disable rename folding so both source and destination obey the grant.
    paths += git('diff', '--cached', '--no-ext-diff', '--no-textconv', '--no-renames', '--name-only', '-z', packet['base'], '--')
    paths += git('ls-files', '--others', '--exclude-standard', '-z')
    for raw in paths.split(b'\0'):
        if not raw:
            continue
        path = PurePosixPath(os.fsdecode(raw))
        resolved = (worktree / path).resolve()
        if not resolved.is_relative_to(worktree):
            raise ValueError('actual change outside granted paths or inside No-Touch')
        actual = PurePosixPath(resolved.relative_to(worktree))
        # Reject aliases into No-Touch and conservative case/Unicode equivalents
        # (including the usual case-insensitive macOS worktree).
        import unicodedata
        folded = lambda value: PurePosixPath(unicodedata.normalize('NFC', str(value)).casefold())
        if (any(not any(candidate.is_relative_to(PurePosixPath(root)) for root in resources['allowed_paths'])
                for candidate in (path, actual)) or
                any(folded(candidate).is_relative_to(folded(root))
                    for candidate in (path, actual) for root in resources['no_touch'])):
            raise ValueError('actual change outside granted paths or inside No-Touch')


def validate_logical_route(task, contract, *, fresh=True):
    """Read-only technical binding inside a confirmed logical resource envelope."""
    import subprocess
    owner_policy.validate_logical_contract(contract)
    if any(task.get(k) != contract[k] for k in ('scope', 'producer')):
        raise ValueError('logical scope or executor changed')
    # Coordinator transitions change owner/gate/action, not original worker rights.
    if fresh and any(task.get(k) != contract[k] for k in ('action', 'owner', 'gate')):
        raise ValueError('logical initial action/role/gate changed')
    route = task['worker_route']
    resources = contract['resources']
    for key in ('repo', 'worktrees', 'artifacts'):
        if resources[key] in {str(Path(resources[key]).anchor), str(Path.home().resolve())}:
            raise ValueError('bounded task resource root required')
    repo = Path(canonical_path(resources['repo']))
    worktree = Path(canonical_path(route['worktree']))
    artifacts = Path(canonical_path(resources['artifacts']))
    if (not worktree.is_relative_to(Path(resources['worktrees'])) or
            artifacts.is_relative_to(worktree) or worktree == repo):
        raise ValueError('isolated authorized worktree and external artifact roots required')
    paths = [canonical_path(task['artifact']), canonical_path(route['packet']), canonical_path(route['receipt'])]
    if (len(set(paths)) != 3 or any(not Path(path).is_relative_to(artifacts) for path in paths) or
            not Path(route['packet']).is_relative_to(logical_packet_root())):
        raise ValueError('fresh logical packet/receipt/result paths must remain in bound roots')
    if fresh and any(Path(path).exists() or Path(path).is_symlink() for path in (task['artifact'], route['receipt'])):
        raise ValueError('logical result and receipt must be fresh')
    packet = read_hashed_json(route['packet'], route['packet_sha256'])
    if (contract['version'] == owner_policy.ITEM_VERSION and
            packet.get('coding_lane') not in {'fast', 'standard', 'audit_gated'}):
        raise ValueError('logical-v2 packet requires the selected coding_lane')
    if packet.get('coding_prompt_source') is not None:
        source = Path(canonical_path(packet['coding_prompt_source']))
        if not source.is_relative_to(artifacts) or not source.is_file():
            raise ValueError('logical prompt source must remain in bound artifact root')
    model = contract['model']
    coding = packet.get('coding_route', {})
    requested = packet.get('requested_actions')
    if (packet.get('routing_mode') != 'non_cmm' or packet.get('operator_scope_authorized') is not True or
            route['plane_id'] != contract['plane_id'] or
            any(packet.get(k) != contract['plane_id'] for k in ('plane_id', 'task_id', 'scope_id')) or
            packet.get('worktree') != route['worktree'] or packet.get('branch') != route['branch'] or
            packet.get('logical_resources') != resources or packet.get('acceptance') != contract['acceptance'] or
            packet.get('role_actions') != contract['actions'] or
            any(coding.get(k) != v for k, v in model.items()) or
            coding.get('routine_actions_authorized') != contract['actions']['coding'] or
            coding.get('merge_authorized') is not False or not isinstance(requested, list) or not requested or
            any(not isinstance(a, str) or a not in contract['actions']['coding'] for a in requested)):
        raise ValueError('packet differs from confirmed logical resources/rights/acceptance/model')
    if route['argv'][0] != CODEX_EXECUTABLE:
        raise ValueError('logical route requires the installed canonical Codex executable')
    if task['launcher_command'] != logical_launcher_command(contract, route):
        raise ValueError('logical route requires exact canonical dispatcher command')
    validate_codex_argv(dict(task, logical_contract=contract))
    def git(path, *args):
        return subprocess.check_output(['git', '--no-optional-locks', '-C', str(path), *args],
            text=True, timeout=10, stderr=subprocess.PIPE).strip()
    if (git(repo, 'rev-parse', '--show-toplevel') != str(repo) or
            git(repo, 'rev-parse', '--path-format=absolute', '--git-common-dir') !=
            git(worktree, 'rev-parse', '--path-format=absolute', '--git-common-dir') or
            str(worktree) not in [line[9:] for line in git(repo, 'worktree', 'list', '--porcelain').splitlines()
                                 if line.startswith('worktree ')]):
        raise ValueError('worktree is not registered to the exact granted repository')
    actual, branch, head, dirty = _git_route_identity(worktree)
    if (actual != str(worktree) or branch != route['branch'] or
            not any(branch.startswith(prefix + contract['plane_id'] + '-') for prefix in ('hermes/', 'hermes/WP-')) or
            head != packet.get('base') or
            ((dirty or packet.get('retained_worktree_sha256') is not None) and
             not _retained_worktree_matches(worktree, packet))):
        raise ValueError('logical route requires exact feature branch/base and retained-state proof')
    validate_logical_changes(dict(task, logical_contract=contract))
    return owner_policy.LogicalBinding(owner_policy.binding_hash(contract), owner_policy.binding_hash(task))


def existing_native_admission(rows, task, fingerprint, transport):
    """Exact authenticated replay only; legacy state never gains new authority."""
    row = rows.get(task.get('task'))
    if row is None:
        return None
    batch = rows.grants.get('batches', {}).get(task.get('owner_batch'))
    if (row.get('native_admission_sha256') != fingerprint or
            delivery.transport_for(row) != transport or
            row.get('origin') != delivery.origin_for(rows, row) or
            row.get('identity') != task['identity'] or not batch or
            batch.get('tasks', {}).get(task.get('owner_issue')) != row['task']):
        raise ValueError('conflicting or legacy native admission; never guess or reset')
    return dict(copy.deepcopy(row), _native_admission_replayed=True)


def existing_attempt_response(guard, row, generation=None):
    # This is a status receipt, never a fresh execution permit or success claim.
    generation = row['generation'] if generation is None else generation
    stale = generation != row['generation']
    return dict(already_registered=True, task=row['task'], generation=generation,
                current_generation=row['generation'], attempt_stale=stale,
                status=row['status'], effect_status=row.get('effect_status'),
                session_id=None if stale else row.get('process_id'), state=str(guard.path),
                launcher_expected=bool(row.get('launcher_expected')),
                visible_sent=bool(row.get('visible_sent')),
                worker_pid=None if stale else row.get('worker_pid'))


def materialize_prompt_reference(route, artifacts):
    """Import one hash-bound executor prompt before native registration."""
    argv = route.get('argv')
    if not isinstance(argv, list) or not argv or argv[-1] != '@coding_prompt_source':
        return None
    packet = read_hashed_json(route['packet'], digest(route['packet']))
    source_name = packet.get('coding_prompt_source')
    expected = packet.get('coding_prompt_sha256')
    if (not isinstance(source_name, str) or not isinstance(expected, str) or
            len(expected) != 64 or any(c not in '0123456789abcdef' for c in expected)):
        raise ValueError('hash-bound coding prompt source required')
    source = Path(canonical_path(source_name))
    if (not source.is_relative_to(Path(artifacts)) or source.is_symlink() or
            not source.is_file()):
        raise ValueError('coding prompt source outside granted artifacts')
    content = source.read_bytes()
    if hashlib.sha256(content).hexdigest() != expected:
        raise ValueError('coding prompt source hash mismatch')
    try:
        prompt = content.decode('utf-8')
    except UnicodeDecodeError as exc:
        raise ValueError('coding prompt source must be UTF-8') from exc
    if not prompt or '\x00' in prompt:
        raise ValueError('coding prompt source is empty or contains NUL')
    route['argv'] = [*argv[:-1], prompt]
    return expected


def preflight_coding_coordinator():
    """Read-only early check; the dispatcher rechecks before the real send."""
    import subprocess
    import sys

    resolver = Path(DISPATCHER).with_name('loop_control_coding_lane.py')
    if (not Path(DISPATCHER).is_file() or not resolver.is_file() or
            not Path(CODEX_EXECUTABLE).is_file() or not os.access(CODEX_EXECUTABLE, os.X_OK)):
        raise ValueError('registered Coding dispatcher or executor unavailable')
    try:
        result = subprocess.run([sys.executable, str(resolver), '--cmux', CMUX],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                text=True, timeout=20, check=True)
        binding = json.loads(result.stdout)
    except (OSError, subprocess.SubprocessError, ValueError) as exc:
        raise ValueError('canonical Coding coordinator unavailable or preload stale') from exc
    if (not isinstance(binding, dict) or not isinstance(binding.get('workspace'), str) or
            not isinstance(binding.get('surface'), str) or not isinstance(binding.get('process'), dict)):
        raise ValueError('canonical Coding coordinator binding incomplete')
    return binding


def bound_coding_prompt(binding):
    """Bind a resumed Coding session to its canonical process and cmux lane."""
    process = binding.get('process', {})
    argv = process.get('argv')
    if (not isinstance(argv, list) or len(argv) < 2 or argv[-2] != '--resume' or
            not isinstance(argv[-1], str) or not argv[-1] or
            not isinstance(process.get('identity'), dict)):
        return None  # A new empty session has no old context to compact.
    return dict(session=argv[-1], process=process['identity'],
                workspace=binding['workspace'], surface=binding['surface'])


def register_native(task):
    context = EVENT_CONTEXT.get()
    if not context:
        raise ValueError('registration requires native Main event context')
    identity = context['identity']
    if (identity['profile'] not in delivery.PROFILES or identity['platform'] != 'telegram' or
            identity['user_id'] != owner_policy.OWNER or identity['chat_id'] != owner_policy.OWNER):
        raise ValueError('unsupported authorized Main target')
    guard = runtime_guard(identity['home'])
    if guard is None:
        raise ValueError('candidate is not activated in native profile config')
    if guard.profile is not None and guard.profile != identity['profile']:
        raise ValueError('native profile differs from activated guard profile')
    native_home = canonical_path(os.environ.get('CODEX_HOME') or str(Path.home().resolve() / '.codex'))
    request = copy.deepcopy(task)
    request['identity'] = identity
    packet_hash = digest(task['worker_route']['packet']) if task.get('producer') == 'cmux' else None
    prompt_hash = None
    with guard.transaction(read_only=True) as rows:
        batch = rows.grants.get('batches', {}).get(task.get('owner_batch'))
        if not batch or batch.get('coordinator_identity') != identity:
            raise ValueError('confirmed coordinator identity required; no session rerouting')
        if batch.get('flow_version') == 3:
            raise ValueError('result-flow batch requires accepted native Coding assignment')
        # The caller cannot supply the persisted native fingerprint or transport.
        if any(key in task for key in ('logical_contract', 'native_admission_sha256', '_native_admission_replayed', 'native_launch_sha256', 'send_request_sha256', 'operator_archive', 'reservation_retirement',
                                      'origin', 'native_transport', 'deliveries', 'coordinator_delivery',
                                      'transport', 'transport_profile', 'bot_id', 'chat_type')):
            raise ValueError('native admission and origin fields are native-only')
        todo = next((t for t in batch['manifest']['todos']
                     if owner_policy.issue_key(t) == task.get('owner_issue')), None)
        logical = (copy.deepcopy(todo['contract']) if todo and
                   todo['contract'].get('version') in owner_policy.LOGICAL_VERSIONS else None)
        if logical and task.get('producer') == 'cmux':
            prompt_hash = materialize_prompt_reference(request['worker_route'],
                                                       logical['resources']['artifacts'])
        transport = delivery.native_transport(identity)
        fingerprint_input = dict(request=task, identity=identity, transport=transport,
            confirmation=batch['confirmation_event'], packet_sha256=packet_hash, codex_home=native_home)
        if prompt_hash is not None:
            fingerprint_input['prompt_sha256'] = prompt_hash
        fingerprint = object_hash(fingerprint_input)
        existing = existing_native_admission(rows, request, fingerprint, transport)
        if existing is not None:
            return guard, existing
    if not task.get('authorization'):
        raise ValueError('explicit task authorization reference required')
    task = dict(request, identity=identity)
    if task.get('producer') == 'cmux':
        import secrets
        route = dict(task['worker_route'])
        if (not isinstance(route.get('argv'), list) or not route['argv'] or
                any(not isinstance(a, str) or not a or '\x00' in a for a in route['argv']) or
                not Path(route['worktree']).is_absolute() or
                not Path(route['receipt']).is_absolute()):
            raise ValueError('explicit authorized worker argv and absolute route paths required')
        packet = json.loads(Path(route['packet']).read_text(encoding='utf-8'))
        if packet.get('operator_scope_authorized') is not True or packet.get('plane_id') != route['plane_id']:
            raise ValueError('packet scope authorization missing')
        argv = route['argv']
        model = packet.get('coding_route', {}).get('required_model')
        if (Path(argv[0]).name != 'codex' or argv[1:2] != ['exec'] or
                '--model' not in argv or argv.index('--model') + 1 >= len(argv) or
                argv[argv.index('--model') + 1] != model):
            raise ValueError('registered Codex argv must match route executor and model')
        task.pop('codex_home', None)  # Only the originating native environment may bind it.
        task['codex_home'] = native_home
        route['packet_sha256'] = digest(route['packet'])
        task.update(worker_route=route, ticket_nonce=secrets.token_hex(32),
                    visible_binding=bind_visible_target(route['visible_target']))
        task['visible_shell_identity'] = bound_shell_identity(task['visible_binding'])
    if logical and logical['version'] == owner_policy.ITEM_VERSION:
        task['coding_prompt_binding'] = bound_coding_prompt(preflight_coding_coordinator())
    logical_binding = validate_logical_route(task, logical) if logical else None
    try:
        guard.register(task, _native_admission=fingerprint, _logical_binding=logical_binding)
    except ValueError:
        # Another identical native call can win registration while we bind OS
        # resources. Recheck under the lock; do not turn other errors into success.
        with guard.transaction(read_only=True) as rows:
            existing = existing_native_admission(rows, request, fingerprint, transport)
            if existing is not None:
                return guard, existing
        raise
    return guard, guard.get(task['task'])


def observe_processes(guard):
    # Existing native registry owns the process; never launch/relaunch here.
    with guard.transaction() as tasks:
        rows = [dict(r) for r in tasks.values() if r['status'] == 'running']
    for row in rows:
        snapshot = process_observation_key(row)
        if row.get('worker_result'):
            guard.observe(row['task'], row['worker_result']['generation'],
                          'completed' if worker_operational_exit(row) else 'unknown',
                          process_snapshot=snapshot)
            continue
        if row.get('worker_pid'):
            import psutil
            if (not psutil.pid_exists(row['worker_pid']) or prior_process_exited(row)):
                guard.observe(row['task'], row['generation'], 'unknown', process_snapshot=snapshot)
            continue
        if row.get('claim_phase') in {'reserved', 'sent'} or row.get('visible_sent_at'):
            deadline = row.get('claim_deadline', row.get('visible_sent_at', 0) + 30)
            if row.get('claim_phase') == 'reserved' and time.time() >= deadline:
                with guard.transaction() as tasks:
                    current = tasks.get(row['task'])
                    if (current and current['generation'] == row['generation'] and
                            process_observation_key(current) == snapshot):
                        admit_received_coding_prompt(current, time.time())
                        row = dict(current)
                        snapshot = process_observation_key(row)
                        deadline = row.get('claim_deadline', deadline)
            if time.time() >= deadline:
                guard.observe(row['task'], row['generation'], 'unknown', process_snapshot=snapshot)
            # A recovered reservation/send is supervised independently of its old launcher.
            continue
        process_id = row.get('process_id')
        if not process_id:
            # A direct worker first performs bounded route preflight in Coding;
            # the Gateway must not classify that registered, unsent interval as
            # a missing launcher after the legacy 30-second grace period.
            grace = 300 if row.get('direct_worker') is True else 30
            if time.time() > row.get('registered_at', time.time()) + grace:
                guard.observe(row['task'], row['generation'], 'unknown', process_snapshot=snapshot)
            continue
        from tools.process_registry import process_registry
        session = process_registry.get(process_id)
        if session is None:
            guard.observe(row['task'], row['generation'], 'unknown', process_snapshot=snapshot)
            continue
        if session.session_key != row['identity']['session_key'] or session.started_at != row['process_started_at']:
            raise ValueError('native process owner mismatch')
        process_registry._reconcile_local_exit(session)
        if not session.exited:
            continue
        if (row.get('producer') == 'cmux' and session.exit_code == 0 and
                (launcher_identity(session) or {}).get('exited') is True):
            # Successful terminal delivery is a bounded handoff, not a worker claim.
            if coding_handoff_pending(guard, row, snapshot, time.time()):
                continue
            current = guard.get(row['task'])
            if (current['generation'] != row['generation'] or current['status'] != 'running' or
                    current.get('visible_sent') or current.get('worker_pid')):
                continue
            # An old observation may not fence a claim that races this read.
            snapshot = process_observation_key(current)
        evidence = None
        artifact = Path(row['artifact'])
        # A cmux send exit is dispatch evidence only, never Coding completion.
        if row.get('producer') != 'cmux' and session.exit_code == 0 and artifact.is_file():
            evidence = {k: row[k] for k in ('task', 'generation', 'action', 'artifact')}
            evidence.update(sha256=hashlib.sha256(artifact.read_bytes()).hexdigest(),
                            result='ready_for_main_review', exit_code=session.exit_code,
                            process_id=process_id)
        guard.observe(row['task'], row['generation'], 'completed' if evidence else 'unknown', evidence, process_snapshot=snapshot)


def terminal_dispatch(args, invoke):
    if args.get('continuation') is not None and args.get('completion_receipt') is not None:
        raise ValueError('guarded coding cannot request completion_receipt; use the durable coding outbox')
    if any(args.get(key) is not None for key in (
            'continuation_proposal', 'continuation_control', 'continuation_ticket', 'continuation')):
        from hermes_cli.diggr_item import require_onboarding_context
        require_onboarding_context()
    if args.get('continuation_proposal') is not None:
        context = EVENT_CONTEXT.get()
        if (not context or args.get('command') != 'SYSTEM167_PROPOSE_OWNER_BATCH' or
                any(args.get(k) for k in ('continuation', 'continuation_ticket', 'continuation_control'))):
            raise ValueError('native proposal-only request required')
        identity = context['identity']
        guard = runtime_guard(identity['home'])
        if guard is None:
            raise ValueError('native candidate inactive')
        transport = delivery.native_transport(identity)
        digest = owner_policy.propose(guard, identity, args['continuation_proposal'], transport=transport)
        return json.dumps(owner_policy.proposal_result(guard, identity, digest, transport))
    if args.get('continuation_control') is not None:
        if args.get('command') != 'SYSTEM168_CONTINUATION_CONTROL' or args.get('continuation') or args.get('continuation_ticket'):
            raise ValueError('structured maintenance accepts no shell command or dispatch')
        # Native registry handlers return strings, including null/boolean results.
        return json.dumps(maintenance_control(args['continuation_control']))
    if args.get('continuation_ticket') is not None:
        ticket = args['continuation_ticket']
        if args.get('command') != 'SYSTEM167_REGISTERED_WORKER' or args.get('continuation'):
            raise ValueError('structured worker launch accepts no freeform command')
        guard = runtime_guard(ticket['identity']['home'])
        if guard is None:
            raise ValueError('worker candidate inactive')
        generation = None
        snapshot = None
        try:
            with guard.transaction() as tasks:
                existing = tasks.get(ticket.get('task'))
                if existing:
                    attempts = [existing] + existing.get('history', [])
                    sent = next((attempt for attempt in attempts
                        if attempt.get('send_request_sha256') == object_hash(ticket)), None)
                    if sent is not None:
                        if (delivery.transport_for(existing) is None or
                                existing.get('origin') != delivery.origin_for(tasks, existing)):
                            raise ValueError('worker replay lacks original native provenance')
                        return json.dumps(existing_attempt_response(guard, existing, ticket['generation']))
                    if existing.get('visible_sent') or existing.get('worker_pid'):
                        raise ValueError('conflicting or legacy worker send; never resend')
                row = validate_ticket(tasks, ticket)
                snapshot = process_observation_key(row)
                if row.get('visible_sent'):
                    raise ValueError('visible worker already sent; never resend')
                generation = row['generation']
                route_check(row, row['worker_route'])
                if not row.get('receipt_sha256'):
                    raise ValueError('worker route receipt not sealed')
                verify_visible_target(row['visible_binding'], idle=True)
                command = visible_worker_command(ticket)
                # Durable reservation before I/O: uncertain delivery is never retried.
                sent_at = time.time()
                row.update(visible_sent=True, visible_sent_at=sent_at, claim_phase='sent',
                           send_request_sha256=object_hash(ticket),
                           claim_deadline=claim_deadline(row, sent_at, 30))
                snapshot = process_observation_key(row)
            # Reservation is durable before I/O. Revalidate after reacquiring the
            # lock and hold it through send: retirement cannot race a delayed send.
            with guard.transaction() as tasks:
                row = validate_ticket(tasks, ticket)
                if process_observation_key(row) != snapshot:
                    raise ValueError('send reservation changed')
                cmux_send(row['visible_binding'], command)
            result = dict(sent=True, task=row['task'], target=row['visible_binding']['target'])
        except Exception:
            if generation is not None:
                guard.observe(ticket['task'], generation, 'unknown',
                              process_snapshot=snapshot)
            raise
        # A response encoding error must not mutate a successfully reserved send.
        return json.dumps(result)
    task = args.get('continuation')
    if task is None:
        return invoke(args)
    if not isinstance(task, dict) or task.get('producer') not in {'codex', 'cmux', 'pilot'}:
        raise ValueError('explicit continuation producer required')
    if task['producer'] != 'cmux':
        raise ValueError('owner-bound dispatch requires the existing registered cmux route')
    if not args.get('background'):
        raise ValueError('guarded terminal launch requires native background supervision')
    if not task.get('launcher_command') or args.get('command') != task['launcher_command']:
        raise ValueError('launcher command must equal the exact owner-confirmed command')
    guard, row = register_native(task)
    with guard.transaction() as tasks:
        current = tasks[row['task']]
        if (current.get('launcher_expected') or current.get('process_id') or
                current.get('send_claim') or current.get('visible_sent') or
                current.get('worker_pid') or current.get('claim_phase') or
                current['status'] != 'running' or current['generation'] != 1):
            if current.get('native_launch_sha256') != object_hash(args):
                raise ValueError('conflicting or legacy launcher request; never replay')
            return json.dumps(existing_attempt_response(guard, current, 1))
        if not owner_policy.allowed(current, time.time()):
            raise ValueError('native admission expired before launcher reservation')
        # Durable one-shot reservation before invoke; an uncertain start is never replayed.
        current.update(launcher_expected=True, native_launch_sha256=object_hash(args))
        row = copy.deepcopy(current)
    try:
        launch = dict(args, notify_on_complete=False)
        if row.get('producer') == 'cmux':
            import shlex
            import sys
            # Quoted data only. Existing native terminal guards still inspect the command.
            launch['command'] = ' '.join([
                'LOOP_CONTROL_CONTINUATION_TICKET=' + shlex.quote(json.dumps(worker_ticket(row))),
                'LOOP_CONTROL_CONTINUATION_PYTHON=' + shlex.quote(sys.executable), args['command']])
        result = invoke(launch)
        data = json.loads(result) if isinstance(result, str) else result
        if not isinstance(data, dict):
            raise ValueError('native terminal response is not an object; launch effects unknown')
        process_id = data.get('session_id')
        if not process_id:
            # A terminal rejection is not evidence that no process was started.
            # Persist only bounded metadata: raw error text can contain secrets.
            diagnostic = dict(
                response_sha256=object_hash(data),
                outcome='terminal_error' if data.get('error') else 'missing_session_id',
                status=data.get('status') if isinstance(data.get('status'), str) and
                    data.get('status') in {'error', 'blocked'} else None,
                exit_code=data.get('exit_code') if type(data.get('exit_code')) is int else None,
            )
            with guard.transaction() as tasks:
                current = tasks[row['task']]
                if (current['generation'] == row['generation'] and
                        current.get('native_launch_sha256') == object_hash(args)):
                    current['native_launch_diagnostic'] = diagnostic
            raise ValueError('native terminal returned no process identity; launch effects unknown')
        from tools.process_registry import process_registry
        session = process_registry.get(process_id)
        if session is None or session.session_key != row['identity']['session_key']:
            raise ValueError('terminal process is not owned by registered native event')
        with guard.transaction() as tasks:
            current = tasks[row['task']]
            current.update(process_id=process_id, process_started_at=session.started_at,
                           launcher_identity=launcher_identity(session), launcher_pid=session.pid)
        observe_processes(guard)
        data.pop('hint', None)
        data['continuation'] = dict(task=row['task'], generation=row['generation'],
                                    state=str(guard.path), automatic_outcome_observation=True)
        if row.get('producer') == 'cmux':
            import shlex
            data['continuation']['worker_ticket'] = worker_ticket(row)
        return json.dumps(data) if isinstance(result, str) else data
    except Exception:
        guard.observe(row['task'], row['generation'], 'unknown')
        raise


def main(argv=None):
    import argparse
    parser = argparse.ArgumentParser(description='Evidence-bound native Main receipts')
    parser.add_argument('--home', required=True)
    parser.add_argument('operation', choices=['register', 'route-check', 'route-bind', 'route-preflight', 'worker', 'ack', 'complete', 'status', 'retry', 'recover'])
    payload_options = parser.add_mutually_exclusive_group(required=True)
    payload_options.add_argument('--payload', help='JSON file')
    payload_options.add_argument('--payload-json', help='Inline machine-readable JSON')
    payload_options.add_argument('--payload-b64', help='Trusted terminal transport JSON')
    args, command = parser.parse_known_args(argv)
    args.command = command
    if command and args.operation != 'worker':
        raise ValueError('unexpected command arguments')
    if args.operation == 'recover':
        # A new CLI process does not own the native launcher's in-memory registry.
        # Reject before loading payload/config/state; a bearer grant cannot restore it.
        parser.error('CLI recovery unsupported: use the owning native runtime '
                     'terminal SYSTEM168_CONTINUATION_CONTROL recover; '
                     'no authenticated owner-runtime bridge is configured by this CLI')
    import base64
    payload = json.loads(base64.urlsafe_b64decode(args.payload_b64) if args.payload_b64 is not None
                         else args.payload_json if args.payload_json is not None else Path(args.payload).read_text(encoding='utf-8'))
    guard = runtime_guard(args.home)
    if guard is None:
        raise ValueError('candidate inactive')
    if args.operation == 'register':
        # Environment variables are executor-controlled, never native authority.
        if not EVENT_CONTEXT.get():
            raise ValueError('CLI registration requires native runtime context; use terminal continuation')
        registered_guard, row = register_native(payload)
        if registered_guard.path != guard.path:
            raise ValueError('native registration home mismatch')
        result = (existing_attempt_response(guard, row, 1)
                  if row.get('_native_admission_replayed') else row)
    elif args.operation in {'route-preflight', 'route-check', 'route-bind'}:
        generation = None
        process_snapshot = None
        try:
            with guard.transaction() as tasks:
                row = validate_ticket(tasks, payload['ticket'])
                generation = row['generation']
                process_snapshot = process_observation_key(row)
                original = copy.deepcopy(row)
                observed = copy.deepcopy(row)
            route = copy.deepcopy(payload['route'])
            if args.operation == 'route-preflight':
                if observed.get('receipt_sha256'):
                    raise ValueError('route already sealed; preflight replay refused')
                receipt = preflight_non_cmm(observed, route)
                receipt_path = Path(observed['worker_route']['receipt'])
                # Exclusive evidence creation is not admission: a stop or changed
                # row before commit leaves an unsealed file, never a usable ticket.
                with receipt_path.open('x', encoding='utf-8') as output:
                    json.dump(receipt, output, sort_keys=True)
                    output.flush()
                    os.fsync(output.fileno())
                observed['non_cmm_receipt_sha256'] = digest(receipt_path)
                route_check(observed, route, seal=True)
            else:
                route_check(observed, route, seal=args.operation == 'route-bind')
            with guard.transaction() as tasks:
                row = validate_ticket(tasks, payload['ticket'])
                if not route_snapshot_matches(original, row):
                    raise ValueError('route state changed during external verification')
                bound = row['worker_route']
                if digest(bound['packet']) != bound['packet_sha256']:
                    raise ValueError('routing packet changed during external verification')
                if observed.get('receipt_sha256') and digest(bound['receipt']) != observed['receipt_sha256']:
                    raise ValueError('routing receipt changed during external verification')
                for key in ('receipt_sha256', 'non_cmm_receipt_sha256'):
                    if key in observed:
                        row[key] = observed[key]
            result = worker_ticket(row)
        except (ValueError, KeyError, OSError):
            if generation is not None:
                guard.observe(payload['ticket']['task'], generation, 'unknown',
                              process_snapshot=process_snapshot)
            raise
    elif args.operation == 'worker':
        import subprocess
        command = args.command[1:] if args.command[:1] == ['--'] else args.command
        generation = None
        try:
            with guard.transaction() as tasks:
                row = validate_ticket(tasks, payload)
                route_check(row, row['worker_route'])
                if not row.get('receipt_sha256'):
                    raise ValueError('worker route receipt not sealed')
                authorized = row['worker_route']['argv']
                if command and command != authorized:
                    raise ValueError('worker argv differs from authorized scope')
                command = authorized
                artifact = Path(row['artifact'])
                if artifact.exists() or artifact.is_symlink():
                    raise ValueError('worker artifact already exists; stale evidence refused')
                generation = row['generation']
                verify_visible_claim(row)
                row.update(worker_pid=os.getpid(), worker_identity=process_identity(os.getpid()),
                           worker_started_ns=time.time_ns(), claim_phase='claimed')
        except Exception:
            if generation is not None:
                guard.observe(payload['task'], generation, 'unknown')
            raise
        try:
            print('SYSTEM167_VISIBLE_WORKER_START ' + json.dumps(dict(task=row['task'], pid=os.getpid(),
                  binding=row['visible_binding'])), flush=True)
            # Stop and spawn share one short native critical section. All slow
            # route/TTY checks happened before the claim; never release the lock
            # between the final authority check and creating the child.
            with guard.transaction() as tasks:
                current = tasks[row['task']]
                if (current['generation'] != generation or current['status'] != 'running' or
                        current.get('worker_identity') != row['worker_identity'] or
                        not Guard.renew(current, time.time())):
                    raise ValueError('worker authority revoked before spawn')
                item_worker = current['policy'].get('item_lifecycle', False)
                verify_assigned_prompt(tasks, current, claimed=row)
                child = subprocess.Popen(command, cwd=row['worker_route']['worktree'])
                work_started = time.monotonic()
            # OS identity inspection may involve an external command: keep it out
            # of the stop lock. Missing identity after a crash blocks replay.
            try:
                child_identity = process_identity(child.pid)
            except (OSError, ValueError, subprocess.SubprocessError):
                child_identity = None
            with guard.transaction() as tasks:
                current = tasks[row['task']]
                if any(current.get(k) != row.get(k) for k in ('action_id', 'effect_id', 'worker_identity')):
                    raise ValueError('worker attempt changed after spawn')
                current['child_identity'] = child_identity
            if item_worker:
                from hermes_cli.diggr_item import wait_worker
                returncode = wait_worker(guard, row, child, work_started)
            else:
                wait_until = claim_deadline(row, time.time(), row['epoch_seconds'])
                returncode = child.wait(timeout=max(0, wait_until - time.time()))
            worker_result = guard.record_worker_result(row, returncode,
                work_seconds=max(0, time.monotonic() - work_started) if item_worker else 0)
            guard.observe(row['task'], row['generation'], 'completed', worker_result['evidence'])
            result = dict(exit_code=returncode, task=row['task'], outcome='completed',
                          artifact_status=worker_result['artifact_status'])
            print('SYSTEM167_VISIBLE_WORKER_END ' + json.dumps(result), flush=True)
        except BaseException:
            guard.observe(row['task'], row['generation'], 'unknown')
            raise
    elif args.operation == 'retry':
        result = guard.retry(payload['identity'], payload, payload['report'])
        if not result:
            raise ValueError('stale or unauthorized technical retry')
    elif args.operation == 'status':
        result = guard.get(payload['task'])
    else:
        row = guard.get(payload['task'])
        result = guard.ack(payload['identity'], payload, payload['evidence'],
                           'done' if args.operation == 'complete' else payload['next_gate'],
                           payload.get('next_action', 'technical work complete'))
        if not result:
            raise ValueError('receipt rejected: stale, unbound, unauthorized or gate incomplete')
    print(json.dumps({'ok': True, 'result': result}, sort_keys=True))
    if args.operation == 'worker':
        return result['exit_code'] if result['exit_code'] >= 0 else 128 - result['exit_code']
    return 0


def prompt(wake):
    if (wake.get('policy', {}).get('item_lifecycle') and
            wake.get('review_closeout_wake')):
        return PREFIX + json.dumps({'task': wake['task'], 'generation': wake['generation']}) + '\n' + (
            f"Native logical-v2 review closeout. Batch: {wake['policy']['batch']}; "
            f"issue: {wake['policy']['issue']}.\n"
            f"Expected review SHA-256: {wake['review_closeout_wake']}.\n"
            'Read item_status first and verify the current review receipt. If the item is already closed, stop. '
            'If a current owner stop or gate blocks, report that blocker. Otherwise call item_merge_check; '
            'a new manual grant requires the authenticated owner approval for this reviewed PR head. '
            'only may_merge=true for the current PR head plus passing external checks permits head-bound merge. '
            'Read back merged state, run integrated main smoke, preserve Nextcloud and Plane evidence, and call '
            'item_closeout. This is one budgeted Main continuation, never a new grant, worker or review round. '
            'Do not bypass a budget or retry an uncertain external side effect.\n')
    if wake.get('policy', {}).get('item_lifecycle'):
        automatic_test = wake.get('logical_contract', {}).get('acceptance', {}).get('mode') == 'automatic'
        return PREFIX + json.dumps({'task': wake['task'], 'generation': wake['generation']}) + '\n' + (
            f"Native logical-v2 item. Batch: {wake['policy']['batch']}; issue: {wake['policy']['issue']}.\n"
            f"Current gate: {wake['gate']}; pending action: {wake['action']}.\n"
            'Read the active optimized_coding_loop.md native item protocol. Use structured '
            'SYSTEM168_CONTINUATION_CONTROL item_status first; it returns the item and persisted execution claim/result. '
            'Assess the immutable completed worker result. If unusable before candidate build, use item_return with '
            'separate failed evidence binding task, worker_generation, action_id, effect_id, result_sha256 and findings; '
            'this schedules review reconciliation without inventing a candidate or starting another worker. '
            'Otherwise prepare the source/build-bound candidate and its test card. ' +
            ('This item has an explicitly confirmed automatic mode. Test the exact built candidate through real '
             'Computer Use before commit or PR. Cover every item criterion and test-card action with observed UI '
             'results; for track playback sample different columns and play each track at most ten seconds. '
             'Record the bound structured evidence with item_test_result. A failed or incomplete test cannot '
             'publish; return concrete findings or report a blocker. Do not invent owner events. '
             if automatic_test else
             'Andi tests by default; Computer Use requires explicit delegation. Native candidate acceptance precedes PR. ') +
            'Main alone signs, pushes and creates the exact accepted PR; independent current-head review is required. '
            'For new manual grants, request the native owner approval for this reviewed PR head before merge. '
            'Use item_closeout after actual merge, integrated smoke, Nextcloud and Plane Accepted. Legacy CLI complete '
            'is not the closeout path for this owner-accepted item. Never manufacture owner events or evidence. '
            'On reconcile use this executing wake in structured recover only after actual exit/effect proof. '
            'Recover returns one ticket, not a started worker: run route-preflight and registered transport once, '
            'then read the persisted claim. Lost notifications do not authorize resending. '
            f"Action/effect IDs: {wake.get('action_id')}/{wake.get('effect_id')}. "
        )
    return PREFIX + json.dumps({'task': wake['task'], 'generation': wake['generation']}) + '\n' + (
        f"Scope: {wake['scope']}\nOwner/gate: {wake['owner']}/{wake['gate']}\n"
        f"Pending action: {wake['action']}\nEvidence: {wake['artifact']}\n"
        f"Native worker result: {json.dumps(wake.get('worker_result'), sort_keys=True)}\n"
        'A known process exit is not technical acceptance. Missing/unavailable artifacts are result gaps; '
        'unclassified output, including exit 0, still requires Main assessment. '
        'Perform only the authorized action. Notification is not completion. '
        f'Receipt command: python -m hermes_cli.diggr_continuation --home {__import__("shlex").quote(wake["identity"]["home"])} ack --payload /absolute/receipt.json. '
        f'Final command: python -m hermes_cli.diggr_continuation --home {__import__("shlex").quote(wake["identity"]["home"])} complete --payload /absolute/final-receipt.json. '
        'Receipts bind task, generation, identity, evidence(task/generation/action/artifact/sha256/result), next_gate and next_action. '
        'DONE additionally requires final_gate=passed, registered authorization, final_artifact and final_sha256 in evidence. The separate final JSON binds task/generation/action/authorization, result=passed and nonempty checks; no shipping authority. '
        'Reconciliation is read-only: inspect prior effects and actual process exit. Unknown effects never replay. '
        'For technical failures use retry with report(reason/detail/retry_at optional), never awaiting_user. '
        'Only credentials, identity_authority, new_cost, destructive_out_of_scope or scope_decision with concrete blocker evidence permit awaiting_user. '
        'After reconciliation use SYSTEM168_CONTINUATION_CONTROL recover in the owning native runtime with wake/evidence/strategy(reason/hypothesis/artifact/worker_route). Standalone/background CLI recovery is unsupported even with a maintenance grant; '
        'The report binds task/generation/identity/action/action_id/effect_id, outcome and observations; reconciled effects require a separate bound effect_proof. '
        'Recover issues one fresh ticket; run route-preflight and existing registered terminal transport. No manual reset or provider fallback. '
        f"Action/effect IDs: {wake.get('action_id')}/{wake.get('effect_id')}. Epoch: {wake.get('epoch')}. "
    )


def token(text):
    if not isinstance(text, str) or not text.startswith(PREFIX):
        return None
    try:
        return json.loads(text[len(PREFIX):].split('\n', 1)[0])
    except (ValueError, TypeError):
        return {}  # malformed synthetic input must fail closed


if __name__ == '__main__':
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    try:
        raise SystemExit(main())
    except (ValueError, KeyError, OSError) as exc:
        print(json.dumps({'ok': False, 'error': str(exc)}))
        raise SystemExit(2)
