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

PREFIX = '[DIGGR evidence continuation] '
TERMINAL = {'paused', 'cancelled', 'superseded', 'blocked', 'awaiting_user', 'done'}


class Guard:
    def __init__(self, path):
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
            for row in state.values():
                self.normalize(row)
            yield state
            after = json.dumps(state, sort_keys=True)
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
        required = ('task', 'scope', 'identity', 'owner', 'gate', 'action', 'artifact', 'deadline', 'wake_budget')
        if any(not task.get(key) for key in required):
            raise ValueError('explicit scope, identity, owner, gate, action, evidence and budget required')
        identity = task['identity']
        fields = {'home', 'profile', 'session', 'session_key', 'platform', 'chat_id', 'user_id', 'thread_id'}
        if set(identity) != fields or any(not identity[k] for k in fields - {'thread_id'}):
            raise ValueError('incomplete target identity')
        if identity['platform'] not in {'telegram', 'cli'} or identity['profile'] != 'diggr-main':
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
            if any(Path(r['artifact']).resolve() == Path(task['artifact']).resolve() for r in state.values()):
                raise ValueError('artifact path already bound to another task; never reuse')
            if task['task'] in state or any(r['identity'] == identity and r['status'] not in TERMINAL for r in state.values()):
                raise ValueError('task already registered or session owned; explicitly supersede first')
            state[task['task']] = dict(task, generation=1, status='running', wakes=0,
                due=now, lease=0, evidence=None, registered_at=now, schema_version=2,
                epoch=1, epoch_wakes=0, recovery_attempt=0, progress_fingerprint=None,
                effect_status='unknown', checkpoint=None, action_id=uuid.uuid4().hex,
                effect_id=uuid.uuid4().hex, attempt_history=[],
                epoch_seconds=max(1, min(3600, task['deadline'] - now)))
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
        row.setdefault('action_id', uuid.uuid4().hex)
        row.setdefault('effect_id', uuid.uuid4().hex)
        row.setdefault('attempt_history', [])
        row.setdefault('epoch_seconds', max(1, min(3600, row['deadline'] - row.get('registered_at', row['deadline'] - 300))))

    @staticmethod
    def checkpoint(row):
        row['checkpoint'] = {key: row.get(key) for key in (
            'scope', 'task', 'gate', 'action', 'owner', 'identity', 'authorization',
            'generation', 'epoch', 'epoch_wakes', 'wakes', 'evidence',
            'progress_fingerprint', 'effect_status', 'recovery_attempt', 'due',
            'action_id', 'effect_id', 'worker_route', 'visible_binding', 'worker_identity')}

    @classmethod
    def renew(cls, row, now):
        cls.normalize(row)
        if row['status'] in TERMINAL:
            return False
        for field in ('hard_stop', 'authorization_expires_at'):
            if row.get(field) is not None and now >= row[field]:
                cls.checkpoint(row)
                row.update(status='paused', reason=field, generation=row['generation'] + 1)
                return False
        if now >= row['deadline']:
            cls.checkpoint(row)
            row.update(epoch=row['epoch'] + 1, epoch_wakes=0,
                       deadline=now + row['epoch_seconds'])
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
            for row in state.values():
                if row['identity'] == identity and row['status'] not in TERMINAL:
                    row.update(status=status, generation=row['generation'] + 1)

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

    def observe(self, task, generation, outcome, evidence=None, now=None):
        now = time.time() if now is None else now
        with self.transaction() as state:
            row = state[task]
            if row['generation'] != generation or row['status'] != 'running':
                return False
            if outcome == 'running':
                return True
            if outcome not in {'completed', 'unknown'}:
                raise ValueError('unknown outcome')
            valid = self.evidence_matches(row, evidence) and evidence.get('result') == 'ready_for_main_review'
            self.checkpoint(row)
            row.update(owner='main', gate='main_validation' if valid else 'reconcile',
                       action='validate coding evidence' if valid else 'read-only reconcile unknown outcome; do not repeat effects',
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
                if busy or row['status'] == 'running':
                    continue
                if row['status'] in {'queued', 'executing'}:
                    if now < row['lease']:
                        continue
                    self.schedule_recovery(row, row['lease'], 'expired Main lease')
                if now < row['due']:
                    continue
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
            self.checkpoint(row)
            row.update(progress_fingerprint=fingerprint, recovery_attempt=0)
            row.update(generation=row['generation'] + 1, gate=next_gate, action=next_action,
                       status=next_gate if next_gate in {'done', 'awaiting_user'} else 'pending',
                       due=now + 2, evidence=evidence)
            return True

    @classmethod
    def schedule_recovery(cls, row, now, reason, retry_at=0):
        cls.checkpoint(row)
        attempt = row['recovery_attempt'] + 1
        row.update(status='pending', owner='main', gate='reconcile',
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

    def recover(self, identity, wake, report_path, report_sha256, strategy, now=None):
        """Issue one fresh strategy ticket after read-only Main reconciliation.

        This never dispatches a worker. Existing preflight and terminal transport
        remain mandatory. Failed/uncertain effects are not replay permissions.
        """
        import copy
        now = time.time() if now is None else now
        with self.transaction() as tasks:
            row = tasks.get(wake['task'])
            if (not row or row['identity'] != identity or row['generation'] != wake['generation'] or
                    row['status'] != 'executing' or row['gate'] != 'reconcile' or
                    now >= row['lease'] or not self.renew(row, now)):
                raise ValueError('stale or unauthorized reconciliation')
            report = json.loads(verified_bytes(report_path, report_sha256))
            if any(report.get(k) != row.get(k) for k in
                   ('task', 'generation', 'identity', 'action_id', 'effect_id')):
                raise ValueError('reconciliation identity mismatch')
            effects = report.get('effects')
            if (not isinstance(effects, list) or len(effects) != 1 or
                    effects[0].get('effect_id') != row['effect_id'] or
                    effects[0].get('status') not in {'no_effect', 'reconciled'}):
                raise ValueError('unknown effects must never replay')
            observations = report.get('observations')
            if not isinstance(observations, list) or not observations:
                raise ValueError('concrete hashed reconciliation observations required')
            for observation in observations:
                verified_bytes(observation['path'], observation['sha256'])
            if not prior_process_exited(row):
                raise ValueError('prior actual process exit not reconciled')
            if not row.get('visible_binding') or not row.get('worker_route'):
                raise ValueError('fresh strategy requires the registered visible worker route')
            verify_visible_target(row['visible_binding'], idle=True)
            route = copy.deepcopy(strategy['worker_route'])
            previous = row['worker_route']
            if route.get('plane_id') != previous['plane_id'] or route.get('visible_target') != previous['visible_target']:
                raise ValueError('fresh strategy must preserve scope and exact visible target')
            old_packet = json.loads(verified_bytes(previous['packet'], previous['packet_sha256']))
            new_packet = json.loads(verified_bytes(route['packet'], route['packet_sha256']))
            if new_packet.get('coding_route') != old_packet.get('coding_route'):
                raise ValueError('fresh strategy must preserve model and action authorization')
            if not strategy.get('action') or route['argv'][-1] == previous['argv'][-1]:
                raise ValueError('explicit fresh strategy required; never replay prior action')
            used = set()
            for record in tasks.values():
                for attempt in [record] + record.get('attempt_history', []):
                    used.add(attempt['artifact'])
                    used.update(attempt.get('worker_route', {}).get(k) for k in ('packet', 'receipt'))
            for path in (strategy['artifact'], route['receipt'], route['packet']):
                target = Path(path)
                if (not target.is_absolute() or str(target.resolve()) != path or
                        target.is_symlink() or path in used):
                    raise ValueError('exclusive fresh canonical artifact/route paths required')
                if path != route['packet'] and target.exists():
                    raise ValueError('fresh artifact or receipt already exists')
            fresh = copy.deepcopy(row)
            fresh.update(generation=row['generation'] + 1, status='running', owner='coding', gate='coding',
                         action=strategy['action'], artifact=strategy['artifact'], worker_route=route,
                         action_id=uuid.uuid4().hex, effect_id=uuid.uuid4().hex,
                         ticket_nonce=uuid.uuid4().hex, evidence=None, effect_status='unknown',
                         registered_at=now, due=now, lease=0)
            for key in ('worker_pid', 'worker_identity', 'worker_started_ns', 'visible_sent', 'visible_sent_at',
                        'process_id', 'process_started_at', 'launcher_expected', 'receipt_sha256', 'non_cmm_receipt_sha256'):
                fresh.pop(key, None)
            # Full real route validation before committing new authority.
            preflight_non_cmm(fresh, route)
            self.checkpoint(row)
            fresh['checkpoint'] = row['checkpoint']
            fresh['checkpoint'].update(effect_status='reconciled', reconciliation_sha256=report_sha256,
                                       reconciliation_path=report_path)
            fresh['attempt_history'].append(dict(artifact=row['artifact'], worker_route=previous,
                action_id=row['action_id'], effect_id=row['effect_id'],
                reconciliation_sha256=report_sha256, effect_status='reconciled'))
            # Reservation survives a crash before state commit: never reuse uncertain paths.
            with open(strategy['artifact'] + '.reservation', 'x', encoding='utf-8') as reservation:
                json.dump(dict(task=row['task'], action_id=fresh['action_id'], effect_id=fresh['effect_id']), reservation)
                reservation.flush()
                os.fsync(reservation.fileno())
            tasks[row['task']] = fresh
            return worker_ticket(fresh)


def verified_bytes(path, sha256):
    candidate = Path(path)
    if not candidate.is_absolute() or candidate.is_symlink() or str(candidate.resolve()) != str(candidate):
        raise ValueError('canonical non-symlink evidence required')
    data = candidate.read_bytes()
    if hashlib.sha256(data).hexdigest() != sha256:
        raise ValueError('evidence hash mismatch')
    return data


def prior_process_exited(row):
    import psutil
    binding = row.get('worker_identity')
    if row.get('worker_pid'):
        if not binding or binding['pid'] != row['worker_pid']:
            return False  # Legacy PID alone is not identity proof.
        try:
            process = psutil.Process(binding['pid'])
            if process.create_time() == binding['created']:
                return False
        except psutil.NoSuchProcess:
            pass
        except psutil.AccessDenied:
            return False
    process_id = row.get('process_id')
    if row.get('launcher_expected') and not process_id:
        return False
    if process_id:
        from tools.process_registry import process_registry
        session = process_registry.get(process_id)
        if (session is None or session.session_key != row['identity']['session_key'] or
                session.started_at != row['process_started_at']):
            return False
        process_registry._reconcile_local_exit(session)
        if not session.exited:
            return False
    # If no worker claimed, the old generation has already been fenced by
    # observe(). Any delayed terminal command can no longer acquire a claim.
    return True


# Native event binding: tools inherit this through the existing executor context.
from contextvars import ContextVar
EVENT_CONTEXT = ContextVar('diggr_continuation_event', default=None)


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
    if cfg.get('profile') != 'diggr-main' or cfg.get('home') != str(home):
        raise ValueError('activation requires explicit matching Main profile and resolved home')
    return Guard(home / 'state' / 'diggr-continuation.json')


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def worker_ticket(row):
    return {k: row[k] for k in ('task', 'generation', 'identity', 'authorization', 'ticket_nonce')}


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
            worker_ticket(row) != ticket or not Guard.renew(row, time.time())):
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
    if (identity['platform'], identity['chat_id'], identity['user_id'], identity['thread_id']) != (
            'telegram', '564628210', '564628210', ''):
        raise ValueError('unsupported authorized Main target')
    guard = runtime_guard(identity['home'])
    if guard is None:
        raise ValueError('candidate is not activated in native profile config')
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
        route['packet_sha256'] = digest(route['packet'])
        task.update(worker_route=route, ticket_nonce=secrets.token_hex(32),
                    visible_binding=bind_visible_target(route['visible_target']))
    guard.register(task)
    return guard, guard.get(task['task'])


def observe_processes(guard):
    # Existing native registry owns the process; never launch/relaunch here.
    with guard.transaction() as tasks:
        rows = [dict(r) for r in tasks.values() if r['status'] == 'running']
    for row in rows:
        if row.get('worker_pid'):
            import psutil
            if (not psutil.pid_exists(row['worker_pid']) or prior_process_exited(row)):
                guard.observe(row['task'], row['generation'], 'unknown')
            continue
        if row.get('visible_sent_at') and time.time() - row['visible_sent_at'] > 30:
            guard.observe(row['task'], row['generation'], 'unknown')
            continue
        process_id = row.get('process_id')
        if not process_id:
            # A launcher that died before binding has unknown outcome, not success.
            if time.time() > row.get('registered_at', time.time()) + 30:
                guard.observe(row['task'], row['generation'], 'unknown')
            continue
        from tools.process_registry import process_registry
        session = process_registry.get(process_id)
        if session is None:
            guard.observe(row['task'], row['generation'], 'unknown')
            continue
        if session.session_key != row['identity']['session_key'] or session.started_at != row['process_started_at']:
            raise ValueError('native process owner mismatch')
        process_registry._reconcile_local_exit(session)
        if not session.exited:
            continue
        if row.get('producer') == 'cmux' and session.exit_code == 0:
            # The actual Coding worker must claim the issued ticket on its visible surface.
            continue
        evidence = None
        artifact = Path(row['artifact'])
        # A cmux send exit is dispatch evidence only, never Coding completion.
        if row.get('producer') != 'cmux' and session.exit_code == 0 and artifact.is_file():
            evidence = {k: row[k] for k in ('task', 'generation', 'action', 'artifact')}
            evidence.update(sha256=hashlib.sha256(artifact.read_bytes()).hexdigest(),
                            result='ready_for_main_review', exit_code=session.exit_code,
                            process_id=process_id)
        guard.observe(row['task'], row['generation'], 'completed' if evidence else 'unknown', evidence)


def terminal_dispatch(args, invoke):
    if args.get('continuation_ticket') is not None:
        ticket = args['continuation_ticket']
        if args.get('command') != 'SYSTEM167_REGISTERED_WORKER' or args.get('continuation'):
            raise ValueError('structured worker launch accepts no freeform command')
        guard = runtime_guard(ticket['identity']['home'])
        if guard is None:
            raise ValueError('worker candidate inactive')
        generation = None
        try:
            with guard.transaction() as tasks:
                row = validate_ticket(tasks, ticket)
                if row.get('visible_sent'):
                    raise ValueError('visible worker already sent; never resend')
                generation = row['generation']
                route_check(row, row['worker_route'])
                if not row.get('receipt_sha256'):
                    raise ValueError('worker route receipt not sealed')
                verify_visible_target(row['visible_binding'], idle=True)
                command = visible_worker_command(ticket)
                # Durable reservation before I/O: uncertain delivery is never retried.
                row.update(visible_sent=True, visible_sent_at=time.time())
            cmux_send(row['visible_binding'], command)
            return dict(sent=True, task=row['task'], target=row['visible_binding']['target'])
        except Exception:
            if generation is not None:
                guard.observe(ticket['task'], generation, 'unknown')
            raise
    task = args.get('continuation')
    if task is None:
        return invoke(args)
    if not isinstance(task, dict) or task.get('producer') not in {'codex', 'cmux', 'pilot'}:
        raise ValueError('explicit continuation producer required')
    if not args.get('background'):
        raise ValueError('guarded terminal launch requires native background supervision')
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
            current.update(process_id=process_id, process_started_at=session.started_at)
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
    import base64
    payload = json.loads(base64.urlsafe_b64decode(args.payload_b64) if args.payload_b64 is not None
                         else args.payload_json if args.payload_json is not None else Path(args.payload).read_text(encoding='utf-8'))
    guard = runtime_guard(args.home)
    if guard is None:
        raise ValueError('candidate inactive')
    if args.operation == 'register':
        # Native terminal subprocess bridge uses existing Hermes session variables.
        from gateway.session_context import get_session_env
        keys = dict(platform='PLATFORM', chat_id='CHAT_ID', user_id='USER_ID',
                    thread_id='THREAD_ID', session='ID', session_key='KEY', profile='PROFILE')
        identity = {k: get_session_env('HERMES_SESSION_' + v) for k, v in keys.items()}
        from hermes_constants import get_hermes_home
        identity['home'] = str(get_hermes_home().resolve())
        if identity['home'] != str(Path(args.home).resolve()):
            raise ValueError('CLI registration home differs from native subprocess home')
        if not EVENT_CONTEXT.get() and all(identity[k] for k in keys if k != 'thread_id'):
            EVENT_CONTEXT.set(dict(identity=identity))
        registered_guard, row = register_native(payload)
        if registered_guard.path != guard.path:
            raise ValueError('native registration home mismatch')
        result = row
    elif args.operation == 'route-preflight':
        # No dispatch and no CMM. The native ticket is authority; this operation
        # only validates its route and writes a fresh evidence receipt.
        with guard.transaction() as tasks:
            row = validate_ticket(tasks, payload['ticket'])
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
        result = worker_ticket(row)
    elif args.operation in {'route-check', 'route-bind'}:
        with guard.transaction() as tasks:
            row = validate_ticket(tasks, payload['ticket'])
            route_check(row, payload['route'], seal=args.operation == 'route-bind')
        result = worker_ticket(row)
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
                import psutil
                row.update(worker_pid=os.getpid(), worker_started_ns=time.time_ns(),
                           worker_identity=dict(pid=os.getpid(), created=psutil.Process().create_time()))
        except Exception:
            if generation is not None:
                guard.observe(payload['task'], generation, 'unknown')
            raise
        try:
            print('SYSTEM167_VISIBLE_WORKER_START ' + json.dumps(dict(task=row['task'], pid=os.getpid(),
                  binding=row['visible_binding'])), flush=True)
            completed = subprocess.run(command, check=False, cwd=row['worker_route']['worktree'],
                                       timeout=row['epoch_seconds'])
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
    elif args.operation == 'recover':
        result = guard.recover(payload['identity'], payload, payload['report_path'],
                               payload['report_sha256'], payload['strategy'])
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
        'After reconciliation use recover with report_path/report_sha256 and strategy(action/artifact/worker_route); '
        'the report binds task/generation/identity/action_id/effect_id, effects and hashed observations. '
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
