"""SYSTEM-167 candidate: evidence gate for native Hermes continuation queues.

No scheduler or CMM state. One ticket-bound foreground worker via existing cmux;
all transitions serialized across processes. One active task per target session.
"""
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


class Guard:
    def __init__(self, path, profile=None):
        self.profile = profile
        self.path = Path(path)
        if not self.path.is_absolute():
            raise ValueError('state path must be absolute')

    @contextmanager
    def transaction(self):
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

    def register(self, task, now=None):
        now = time.time() if now is None else now
        if 'origin' in task or 'deliveries' in task or 'coordinator_delivery' in task:
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
        with self.transaction() as state:
            if any(Path(a['artifact']).resolve() == Path(task['artifact']).resolve()
                   for r in state.values() for a in [r] + r.get('history', []) + r.get('attempt_history', [])):
                raise ValueError('artifact path already bound to another task; never reuse')
            if task['task'] in state or any(r['identity'] == identity and ownership_pending(r) for r in state.values()):
                raise ValueError('task already registered or session owned; explicitly supersede first')
            target = task.get('visible_binding', {}).get('target')
            if target and any(target_reserved(r, target) for r in state.values()):
                raise ValueError('visible target already owned or prior exit unreconciled')
            policy = owner_policy.bind(state, task, now)
            state[task['task']] = dict(task, policy=policy, hard_stop=policy['deadline'],
                authorization_expires_at=policy['deadline'], worker_action=task['action'], generation=1, status='running', wakes=0,
                due=now, lease=0, evidence=None, registered_at=now, schema_version=2,
                epoch=1, epoch_wakes=0, recovery_attempt=0, progress_fingerprint=None,
                effect_status='unknown', checkpoint=None, action_id=uuid.uuid4().hex,
                effect_id=uuid.uuid4().hex, attempt_history=[],
                epoch_seconds=max(1, min(3600, task['deadline'] - now)))
            state[task['task']]['origin'] = delivery.origin_for(state, state[task['task']])
            self.normalize(state[task['task']])

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
        for field in ('hard_stop', 'authorization_expires_at'):
            if row.get(field) is not None and now >= row[field]:
                cls.checkpoint(row)
                row.update(status='paused', reason=field, limit_generation=row['generation'],
                           generation=row['generation'] + 1)
                return False
        if now >= row['deadline']:
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
            valid = (self.evidence_matches(row, evidence) and evidence.get('result') == 'ready_for_main_review'
                     and evidence.get('exit_code', 0) == 0)
            if not valid:
                row['evidence'] = evidence
                self.schedule_recovery(row, now, 'unknown or invalid worker outcome')
                return True
            self.checkpoint(row)
            row.update(owner=delivery.coordinator(row['identity']), gate='main_validation',
                       action='validate coding evidence',
                       evidence=evidence, generation=generation + 1, status='pending', due=now)
            return True

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
                    self.schedule_recovery(row, row['lease'], 'expired Main lease')
                if now < row['due']:
                    continue
                if row['policy']['wakes'] >= 10:
                    self.checkpoint(row)
                    row.update(status='paused', reason='total automatic wake budget exhausted',
                               limit_generation=row['generation'], generation=row['generation'] + 1)
                    continue
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
                self.schedule_recovery(row, row['lease'], 'expired Main lease')
                return False
            row.update(status='executing', lease=now + 60)
            row.setdefault('coordinator_turns', {})[str(row['generation'])] = dict(status='accepted', accepted_at=now)
            if row.get('coordinator_delivery', {}).get('generation') == row['generation']:
                row['coordinator_delivery'].update(status='accepted', accepted_at=now)
            return True

    def ack(self, identity, wake, evidence, next_gate, next_action, now=None):
        now = time.time() if now is None else now
        if next_gate not in {'main_live', 'awaiting_user', 'done'} or not next_action:
            raise ValueError('no shipping or Coding dispatch authority')
        with self.transaction() as state:
            row = state.get(wake['task'])
            if not row or row['identity'] != identity or row['status'] != 'executing' or row['generation'] != wake['generation'] or not self.renew(row, now):
                return False
            if now >= row['lease']:
                self.schedule_recovery(row, row['lease'], 'expired Main lease')
                return False
            if row['generation'] != wake['generation'] or not self.evidence_matches(row, evidence) or evidence.get('result') != 'validated':
                return False
            if next_gate == 'done' and (row['gate'] != 'main_live' or
                    evidence.get('authorization') != row.get('authorization') or
                    not row.get('authorization') or evidence.get('final_gate') != 'passed'):
                return False
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
        """Authentic Main receipt for an absent result; cannot satisfy success/ship gates."""
        now = time.time() if now is None else now
        require_native_main(identity)
        with self.transaction() as state:
            row = self._recovery_row(state, identity, wake, now)
            report = recovery_report(row, evidence)
            if report.get('worker_result') != 'missing' or not report.get('reason'):
                raise ValueError('explicit missing-worker-result reason required')
            if Path(row['artifact']).exists() or Path(row['artifact']).is_symlink():
                raise ValueError('worker result is not absent')
            row.update(status='blocked', reason=report['reason'], failure_evidence=dict(evidence),
                       generation=row['generation'] + 1)
            row.pop('maintenance', None)
            return True

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
        if maintenance is None:
            require_native_main(identity)
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
            if row['policy'][counter] >= 2:
                raise ValueError('total owner recovery/correction budget exhausted')
            report = recovery_report(row, evidence)
            if report.get('outcome') not in {'no_effect', 'reconciled'}:
                raise ValueError('prior effects unknown; recovery refused')
            if report['outcome'] == 'reconciled' and not report.get('effect_proof'):
                raise ValueError('reconciled effects require proof')
            from tools.process_registry import process_registry
            if process_registry.get(row.get('process_id')) is None:
                verify_restarted_reservation_exit(row)
            else:
                verify_prior_exit(row)
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
            validate_recovery_route(old_route, route, old_packet, packet, row['artifact'], artifact)
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
            verify_changed_approach(old_binding, binding)
            if binding != old_binding and not process_absent(old_binding['shell']):
                raise ValueError('prior shell exit not proven')
            verify_visible_target(binding, idle=True)
            for other in state.values():
                if other['task'] != row['task'] and target_reserved(other, target):
                    raise ValueError('replacement surface already owned or prior exit unreconciled')
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
                preflight_non_cmm(fresh, route)
            shell_identity = bound_shell_identity(binding)
            commit_now = max(now, time.time())
            self._recovery_row(state, identity, wake, commit_now)
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
                        'visible_sent', 'visible_sent_at', 'receipt_sha256', 'non_cmm_receipt_sha256', 'maintenance'):
                row.pop(key, None)
            row['policy'][counter] += 1
            row.update(generation=row['generation'] + 1, ticket_nonce=secrets.token_hex(32),
                       action_id=secrets.token_hex(16), effect_id=secrets.token_hex(16),
                       worker_route=route, artifact=artifact, visible_binding=binding,
                       visible_shell_identity=shell_identity,
                       status='running', owner='coding', gate='coding', action=worker_action, worker_action=worker_action,
                       evidence=None, effect_status='unknown', registered_at=now, due=now, lease=0,
                       claim_phase='reserved', claim_deadline=claim_deadline(row, commit_now, 60))
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
    try:
        created = psutil.Process(session.pid).create_time()
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return dict(proof, exited=True) if exited() else None
    # Recheck the owning handle: don't capture a recycled PID after reaping.
    return dict(proof, exited=True) if exited() else dict(proof, created=created)


def identity_exited(binding):
    import psutil
    if (not isinstance(binding, dict) or type(binding.get('pid')) is not int or
            binding['pid'] <= 0 or type(binding.get('created')) not in (float, int) or
            not math.isfinite(binding['created']) or binding['created'] <= 0):
        return False
    try:
        return psutil.Process(binding['pid']).create_time() != binding['created']
    except psutil.NoSuchProcess:
        return True
    except psutil.AccessDenied:
        return False


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
        if any(effects.get(k) != row[k] for k in ('task', 'action_id', 'effect_id')) or effects.get('result') != 'reconciled':
            raise ValueError('effect proof must bind the prior action and effect')
    return report


def process_absent(identity):
    # Refuse reused PIDs as well as the original live process. Errors are not exit proof.
    if not isinstance(identity, dict) or any(not identity.get(k) for k in ('pid', 'start', 'executable')):
        raise ValueError('full prior process identity required')
    return pid_is_absent(identity['pid'])


def pid_is_absent(pid):
    import psutil
    if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
        raise ValueError('positive native process PID required')
    return not psutil.pid_exists(pid)


def verify_prior_exit(row):
    if row.get('worker_pid'):
        identity = row.get('worker_identity')
        if not identity or identity.get('pid') != row['worker_pid'] or not (process_absent(identity) if identity.get('start') else
                    identity_exited(identity) and pid_is_absent(identity['pid'])):
            raise ValueError('prior worker alive, PID reused or exit identity missing')
        if not row.get('child_exited'):
            if not row.get('child_identity') or not process_absent(row['child_identity']):
                raise ValueError('actual Coding child exit unknown or child alive')
    elif row.get('visible_sent'):
        # The ticket is already invalidated by observe -> reconcile. An unclaimed
        # delayed wrapper cannot execute; the bound shell must nevertheless be gone.
        if not process_absent(row['visible_binding']['shell']):
            raise ValueError('unclaimed dispatch requires prior shell exit')
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
    from tools.process_registry import process_registry
    import psutil

    attempts = [row] + row.get('history', [])
    for attempt in attempts:
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
                raise ValueError('prior terminal alive or PID reused')
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
    if row.get('reservation_retirement'):
        return not retirement_matches(row)
    # Cancellation/failure invalidates tickets, not live process ownership.
    try:
        verify_prior_exit(row)
    except (ValueError, KeyError, OSError):
        return True
    return False


def retirement_matches(row):
    receipt = row['reservation_retirement']
    current = {k: v for k, v in row.items() if k != 'reservation_retirement'}
    try:
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


def process_observation_key(row):
    # A copied observer must not invalidate a newer send/claim in the same generation.
    return object_hash({k: row.get(k) for k in (
        'worker_pid', 'worker_identity', 'worker_started_ns', 'child_identity', 'child_exited',
        'visible_sent', 'visible_sent_at', 'claim_phase', 'claim_deadline',
        'process_id', 'process_started_at')})


def target_reserved(row, target):
    owned = row.get('visible_binding', {}).get('target', {})
    return (owned.get('surface', '').upper() == target['surface'].upper()
            and ownership_pending(row))


def cmux_tree():
    import subprocess
    tree = json.loads(subprocess.check_output([CMUX, '--json', '--id-format', 'both',
        'tree', '--all'], text=True, timeout=10))
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


def verify_changed_approach(old_binding, binding):
    if binding == old_binding:
        raise ValueError('changed wording is not a changed approach; verified target migration required')


def validate_recovery_route(old, route, old_packet, packet, old_artifact, artifact):
    # Adjacent durable bookkeeping must not dirty the required clean source tree.
    if Path(artifact).is_relative_to(Path(canonical_path(route['worktree']))):
        raise ValueError('recovery artifact and reservation must be outside authorized worktree')
    if any(route.get(k) != old.get(k) for k in ('worktree', 'branch', 'plane_id')):
        raise ValueError('changed worktree, branch or Plane identity')
    # New packet path, same authorized content. No prompt/model/action drift hidden
    # behind a new strategy. Corrections of invalid original scope require Main.
    if packet != old_packet:
        raise ValueError('changed authorized packet scope/model/actions/base')
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
    if route.get('argv') != expected:
        raise ValueError('changed authorized argv')
    argv = route['argv']
    if (Path(argv[0]).name != 'codex' or argv[1:2] != ['exec'] or
            argv.count('--model') != 1 or argv[argv.index('--model') + 1] != contract['required_model'] or
            argv.count('--output-last-message') != 1 or
            argv[argv.index('--output-last-message') + 1] != artifact):
        raise ValueError('exact model and fresh output argument required')


def verify_recovery_worktree(route, packet):
    import subprocess
    def git(*args):
        return subprocess.check_output(['git', '-C', canonical_path(route['worktree']), *args],
            text=True, timeout=10, env=dict(os.environ, GIT_OPTIONAL_LOCKS='0')).strip()
    if (route['branch'] in {'main', 'master'} or not route['branch'].startswith('hermes/WP-') or
            git('branch', '--show-current') != route['branch'] or
            git('rev-parse', 'HEAD') != packet['base'] or git('status', '--porcelain')):
        raise ValueError('recovery requires exact clean authorized branch and base')


def maintenance_control(payload):
    """Native terminal-tool bridge; no subprocess and no identity from environment."""
    context = EVENT_CONTEXT.get()
    operation = payload['operation']
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
    wake = payload['wake']
    if operation == 'authorize':
        return guard.authorize_maintenance(identity, wake, payload['evidence'], payload['strategy'], payload['expires'])
    if operation == 'revoke':
        return guard.revoke_maintenance(identity, wake)
    if operation == 'retire':
        return guard.retire_reservation(identity, wake, payload['evidence'])
    if operation == 'failure':
        return guard.failure(identity, wake, payload['evidence'])
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
        'top', '--workspace', target['workspace'], '--processes'], text=True, timeout=10))


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
    def walk(node, workspace=None):
        if isinstance(node, dict):
            if node.get('kind') == 'workspace': workspace = node.get('id')
            if node.get('kind') == 'surface' and node.get('id', '').upper() == target['surface'].upper():
                if workspace and workspace.upper() == target['workspace'].upper(): found.append(node)
            for child in node.values(): walk(child, workspace)
        elif isinstance(node, list):
            for child in node: walk(child, workspace)
    walk(cmux_snapshot(target))
    if len(found) != 1 or found[0].get('type') != 'terminal' or not found[0].get('tty'):
        raise ValueError('missing, ambiguous or non-terminal cmux target')
    return found[0]


def bind_visible_target(target):
    surface = visible_surface(target)
    roots = surface.get('top_level_pids', [])
    if len(roots) != 1:
        raise ValueError('target must have one live root shell')
    shell = process_identity(roots[0])
    if (shell['executable'] not in ('/bin/zsh', '/bin/bash') or
            shell['tty'] != surface['tty'] or shell['pgid'] != shell['pid'] or
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
    if idle and (shell['tpgid'] != shell['pgid'] or
                 surface.get('foreground_pgids') != [shell['pgid']] or
                 set(surface.get('tty_process_pids', [])) != {shell['pid']}):
        raise ValueError('occupied terminal; idle root shell required')
    return surface, shell


def bound_shell_identity(binding):
    import psutil
    pid = binding['shell']['pid']
    proof = dict(pid=pid, created=psutil.Process(pid).create_time())
    verify_visible_target(binding, idle=True)
    return proof


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
    # One write including Enter; no separate send-key and no hidden fallback.
    subprocess.run([CMUX, 'send', '--workspace', target['workspace'], '--surface',
                    target['surface'], '--', command + '\n'], check=True, capture_output=True, timeout=10)


def validate_ticket(tasks, ticket):
    row = tasks.get(ticket.get('task'))
    if (not row or row['status'] != 'running' or row.get('worker_pid') or
            worker_ticket(row) != ticket or row.get('reservation_retirement') or
            (row.get('claim_deadline') is not None and time.time() >= row['claim_deadline']) or
            not Guard.renew(row, time.time())):
        raise ValueError('UNREGISTERED, stale, paused, cancelled, expired or claimed worker ticket; never restart')
    return row


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
            contract.get('required_model') != 'gpt-6-astra' or
            contract.get('fallback_models_allowed') != []):
        raise ValueError('non-CMM action/model authorization rejected')
    argv = bound['argv']
    if (not isinstance(argv, list) or len(argv) < 4 or
            any(not isinstance(a, str) or not a or '\x00' in a for a in argv) or
            not Path(argv[0]).is_absolute() or Path(argv[0]).name != 'codex' or argv[1] != 'exec'):
        raise ValueError('explicit Codex exec argv required')
    # Deliberately narrow supported CLI grammar. No later model/config/profile
    # override, resume, full-auto or sandbox bypass can shadow the pinned flags.
    options = {}
    args = argv[2:-1]
    if len(args) % 2 or argv[-1].startswith('-'):
        raise ValueError('unsupported Codex argv')
    for key, value in zip(args[::2], args[1::2]):
        if key in options or key not in {'--model', '--sandbox', '--cd', '--output-last-message', '--color', '-c'}:
            raise ValueError('duplicate or unsupported Codex option')
        options[key] = value
    if (options.get('--model') != 'gpt-6-astra' or options.get('--sandbox') != 'workspace-write' or
            options.get('--cd') != bound['worktree'] or options.get('--output-last-message') != row['artifact'] or
            options.get('--color', 'never') != 'never' or
            options.get('-c', 'approval_policy="never"') != 'approval_policy="never"'):
        raise ValueError('Codex model/worktree/output/sandbox mismatch')
    actual_root, branch, head, dirty = _git_route_identity(bound['worktree'])
    if (actual_root != bound['worktree'] or branch != bound['branch'] or
            branch in {'main', 'master'} or head != packet.get('base') or dirty):
        raise ValueError('clean exact-base feature worktree required')
    if bound['visible_target'] != row['visible_binding']['target']:
        raise ValueError('visible target differs from registered binding')
    verify_visible_target(row['visible_binding'], idle=check_idle)
    expires_at = (legacy_expiry if legacy_expiry is not None else
                  min((row[k] for k in ('hard_stop', 'authorization_expires_at')
                       if row.get(k) is not None), default=None))
    proof = dict(task=row['task'], generation=row['generation'],
                 authorization=row['authorization'], identity=row['identity'],
                 artifact=row['artifact'], argv=argv, binding=row['visible_binding'],
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
    with guard.transaction() as rows:
        batch = rows.grants.get('batches', {}).get(task.get('owner_batch'))
        if not batch or batch.get('coordinator_identity') != identity:
            raise ValueError('confirmed coordinator identity required; no session rerouting')
    if not task.get('authorization'):
        raise ValueError('explicit task authorization reference required')
    task = dict(task, identity=identity)
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
        task['codex_home'] = canonical_path(os.environ.get('CODEX_HOME') or str(Path.home().resolve() / '.codex'))
        route['packet_sha256'] = digest(route['packet'])
        task.update(worker_route=route, ticket_nonce=secrets.token_hex(32),
                    visible_binding=bind_visible_target(route['visible_target']))
        task['visible_shell_identity'] = bound_shell_identity(task['visible_binding'])
    guard.register(task)
    return guard, guard.get(task['task'])


def observe_processes(guard):
    # Existing native registry owns the process; never launch/relaunch here.
    with guard.transaction() as tasks:
        rows = [dict(r) for r in tasks.values() if r['status'] == 'running']
    for row in rows:
        snapshot = process_observation_key(row)
        if row.get('worker_pid'):
            import psutil
            if (not psutil.pid_exists(row['worker_pid']) or prior_process_exited(row)):
                guard.observe(row['task'], row['generation'], 'unknown', process_snapshot=snapshot)
            continue
        if row.get('claim_phase') in {'reserved', 'sent'} or row.get('visible_sent_at'):
            deadline = row.get('claim_deadline', row.get('visible_sent_at', 0) + 30)
            if time.time() >= deadline:
                guard.observe(row['task'], row['generation'], 'unknown', process_snapshot=snapshot)
            # A recovered reservation/send is supervised independently of its old launcher.
            continue
        process_id = row.get('process_id')
        if not process_id:
            # A launcher that died before binding has unknown outcome, not success.
            if time.time() > row.get('registered_at', time.time()) + 30:
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
        if row.get('producer') == 'cmux' and session.exit_code == 0:
            # Launcher delivery does not prove a successful preflight or worker claim.
            if time.time() <= row['registered_at'] + 30:
                continue
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
    if args.get('continuation_proposal') is not None:
        context = EVENT_CONTEXT.get()
        if (not context or args.get('command') != 'SYSTEM167_PROPOSE_OWNER_BATCH' or
                any(args.get(k) for k in ('continuation', 'continuation_ticket', 'continuation_control'))):
            raise ValueError('native proposal-only request required')
        identity = context['identity']
        guard = runtime_guard(identity['home'])
        if guard is None:
            raise ValueError('native candidate inactive')
        digest = owner_policy.propose(guard, identity, args['continuation_proposal'])
        return json.dumps(dict(proposal_sha256=digest, owner_command='/continuation show ' + digest))
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
        tasks[row['task']].update(registered_at=time.time(), launcher_expected=True)
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
        process_id = data.get('session_id')
        if not process_id:
            raise ValueError('terminal did not return native process identity')
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
        result = row
    elif args.operation in {'route-preflight', 'route-check', 'route-bind'}:
        generation = None
        process_snapshot = None
        try:
            if args.operation == 'route-preflight':
                with guard.transaction() as tasks:
                    row = validate_ticket(tasks, payload['ticket'])
                    generation = row['generation']
                    process_snapshot = process_observation_key(row)
                    if row.get('receipt_sha256'):
                        raise ValueError('route already sealed; preflight replay refused')
                    receipt = preflight_non_cmm(row, payload['route'])
                    receipt_path = Path(row['worker_route']['receipt'])
                    # Exclusive creation: never replace stale evidence or follow a link.
                    with receipt_path.open('x', encoding='utf-8') as output:
                        json.dump(receipt, output, sort_keys=True)
                        output.flush()
                        os.fsync(output.fileno())
                    row['non_cmm_receipt_sha256'] = digest(receipt_path)
                    route_check(row, payload['route'], seal=True)
            else:
                with guard.transaction() as tasks:
                    row = validate_ticket(tasks, payload['ticket'])
                    generation = row['generation']
                    process_snapshot = process_observation_key(row)
                    route_check(row, payload['route'], seal=args.operation == 'route-bind')
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
            child = subprocess.Popen(command, cwd=row['worker_route']['worktree'])
            # Missing identity during this crash window deliberately blocks recovery.
            try:
                child_identity = process_identity(child.pid)
            except (OSError, ValueError, subprocess.SubprocessError):
                child_identity = None
            with guard.transaction() as tasks:
                tasks[row['task']]['child_identity'] = child_identity
            wait_until = claim_deadline(row, time.time(), row['epoch_seconds'])
            returncode = child.wait(timeout=max(0, wait_until - time.time()))
            with guard.transaction() as tasks:
                tasks[row['task']]['child_exited'] = True
            completed = subprocess.CompletedProcess(command, returncode)
            artifact = Path(row['artifact'])
            evidence = None
            if (completed.returncode == 0 and artifact.is_file() and not artifact.is_symlink() and
                    artifact.stat().st_mtime_ns >= row['worker_started_ns']):
                evidence = {k: row[k] for k in ('task', 'generation', 'action', 'artifact')}
                evidence.update(sha256=hashlib.sha256(artifact.read_bytes()).hexdigest(),
                                result='ready_for_main_review', exit_code=completed.returncode)
            guard.observe(row['task'], row['generation'], 'completed' if evidence else 'unknown', evidence)
            result = dict(exit_code=completed.returncode, task=row['task'], outcome='completed' if evidence else 'unknown')
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
        return (result['exit_code'] if result['exit_code'] > 0 else 2) if result['outcome'] == 'unknown' else 0
    return 0


def prompt(wake):
    return PREFIX + json.dumps({'task': wake['task'], 'generation': wake['generation']}) + '\n' + (
        f"Scope: {wake['scope']}\nOwner/gate: {wake['owner']}/{wake['gate']}\n"
        f"Pending action: {wake['action']}\nEvidence: {wake['artifact']}\n"
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
