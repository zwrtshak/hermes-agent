"""Serial logical-v2 item lifecycle inside the existing native owner ledger.

No scheduler or second state file. Preparation is reserved before Git I/O;
acceptance is written only by an authenticated owner event. Legacy grants keep
their original semantics.
"""
import copy
from contextlib import contextmanager
import fcntl
import hashlib
import json
import math
import os
import re
import secrets
import signal
import sqlite3
import sys
from pathlib import Path, PurePosixPath
import subprocess
import time

from hermes_cli import diggr_owner as owner

REQUIRED_SKILLS_MANIFEST = Path('/Users/diggr/Projects/diggr_claw/ops/scripts/required_skills.py')

ONBOARDING_SKILL = 'diggr-coding-loop-onboarding'
ONBOARDING_BLOCK = 'BLOCKED_MISSING_CODING_LOOP_ONBOARDING'
APP105_TEST_SCOPE_PATH = 'apps/desktop/src/renderer/__tests__/workspace-release-context.test.ts'


def required_profile_skills():
    """One manifest for native CLI and gateway; no new role permissions."""
    from hermes_constants import get_hermes_home
    import importlib.util
    home = get_hermes_home()
    # These are the existing DIGGR participants and native coordinator profiles.
    if home.name not in {'diggr-main', 'diggr-coding', 'diggr-marketing',
                         'diggr-video', 'diggr-support-success', 'mira'}:
        return ()
    spec = importlib.util.spec_from_file_location('diggr_required_skills', REQUIRED_SKILLS_MANIFEST)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    wanted = tuple(module.required_skill_ids(home.name))
    if not wanted or wanted[0] != ONBOARDING_SKILL:
        raise ValueError(ONBOARDING_BLOCK + ': load ' + ONBOARDING_SKILL + ' first')
    return wanted


def onboarding_payload():
    """Resolve the current full payload with Hermes' own profile-scoped loader."""
    from agent.skill_commands import _load_skill_payload
    from agent.skill_utils import get_disabled_skill_names
    if ONBOARDING_SKILL in get_disabled_skill_names():
        raise ValueError('onboarding disabled')
    loaded = _load_skill_payload(ONBOARDING_SKILL)
    if not loaded:
        raise ValueError('onboarding unavailable')
    payload, directory, name = loaded
    content = payload.get('content')
    if name != ONBOARDING_SKILL or directory is None or not isinstance(content, str) or not content.strip():
        raise ValueError('onboarding incomplete')
    return content.strip(), str(directory.resolve())


def observe_onboarding_request(agent, request):
    """Record only host-observed full system context, never model attestation.

    Called on every final provider request, including retries and after
    compression. No context or receipt is persisted; no prompt is rewritten.
    """
    from hermes_constants import get_hermes_home
    from agent.message_content import flatten_message_text
    agent._diggr_onboarding_request = None
    try:
        if not required_profile_skills():
            return
        content, directory = onboarding_payload()
        systems = []
        for key in ('instructions', 'system'):
            value = request.get(key)
            if value:
                systems.append(flatten_message_text(value))
        for key in ('messages', 'input'):
            entries = request.get(key)
            if isinstance(entries, list):
                systems.extend(flatten_message_text(entry.get('content'))
                    for entry in entries if isinstance(entry, dict)
                    and entry.get('role') in {'system', 'developer'})
        if not any(content in system for system in systems):
            return
        agent._diggr_onboarding_request = (
            str(get_hermes_home().resolve()), str(agent.session_id), directory,
            hashlib.sha256(content.encode()).hexdigest())
    except Exception:
        # Ordinary chat remains available; a loop tool fails closed below.
        return


def require_onboarding_context():
    """Gate every agent-facing loop call before native state or effects."""
    from agent.subagent_lifecycle import get_active_subagent_parent
    from hermes_constants import get_hermes_home
    agent = get_active_subagent_parent()
    try:
        content, directory = onboarding_payload()
        expected = (str(get_hermes_home().resolve()), str(agent.session_id), directory,
                    hashlib.sha256(content.encode()).hexdigest())
        if getattr(agent, '_diggr_onboarding_request', None) == expected:
            return
    except Exception:
        pass
    raise ValueError(ONBOARDING_BLOCK + ': preload the complete ' + ONBOARDING_SKILL +
                     ' in the current session before calling the Coding Loop; native role authorization still applies')


def required_gateway_skills():
    """Preload the same primary and role skills as the native CLI."""
    from agent.skill_commands import build_preloaded_skills_prompt
    wanted = list(required_profile_skills())
    if not wanted:
        return ''
    prompt, loaded, missing = build_preloaded_skills_prompt(wanted)
    if missing or loaded != wanted:
        raise ValueError('required Coding Loop skills missing: ' + ', '.join(missing or wanted))
    return prompt

@contextmanager
def preparation_lock(guard):
    """Serialize only Git preparation, using no second persistent state store."""
    with guard.path.with_suffix('.prepare.lock').open('a') as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError('another native preparation is still active; read back afterward') from None
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def wait_worker(guard, attempt, child, started):
    """Supervise the native child while preserving its foreground terminal.

    Idle item time is free. Actual child execution consumes the cumulative
    allowance; stop/revocation terminates only captured child identities.
    """
    import psutil
    observed = {}
    while child.poll() is None:
        try:
            children = psutil.Process(child.pid).children(recursive=True)
            additions = {p.pid: p.create_time() for p in children}
        except psutil.NoSuchProcess:
            additions = {}
        if any(observed.get(pid) != created for pid, created in additions.items()):
            observed.update(additions)
            with guard.transaction() as rows:
                row = rows[attempt['task']]
                row['worker_descendants'] = [dict(pid=pid, created=created) for pid, created in observed.items()]
        with guard.transaction(read_only=True) as rows:
            row = rows[attempt['task']]
            policy = row['policy']
            same = all(row.get(k) == attempt.get(k) for k in ('action_id', 'effect_id', 'worker_identity'))
            remaining = policy['work_limit'] - policy.get('work_seconds', 0)
            permitted = same and not policy['revoked'] and time.monotonic() - started < remaining
        if not permitted:
            owned = []
            for pid, created in observed.items():
                try:
                    process = psutil.Process(pid)
                    if process.create_time() == created:
                        owned.append(process)
                        process.terminate()
                except psutil.NoSuchProcess:
                    pass
            child.terminate()
            _, alive = psutil.wait_procs(owned, timeout=5)
            for process in alive:
                process.kill()  # psutil verifies PID reuse against its cached identity
            try:
                return child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                child.kill()
                return child.wait(timeout=5)
        try:
            return child.wait(timeout=min(1, max(.01, remaining - (time.monotonic() - started))))
        except subprocess.TimeoutExpired:
            continue
    return child.returncode


def git(repo, *args):
    return subprocess.check_output(
        ['git', '--no-optional-locks', '-C', str(repo), *args],
        text=True, stderr=subprocess.PIPE, timeout=30).strip()


def bound(rows, identity, batch_id, issue):
    batch = rows.grants.get('batches', {}).get(batch_id)
    if (not batch or batch.get('coordinator_identity') != identity or
            batch['owner'] != owner.stable_owner(identity)):
        raise ValueError('item requires its authenticated original Main context')
    todo = next((t for t in batch['manifest']['todos'] if owner.issue_key(t) == issue), None)
    if not todo or todo['contract'].get('version') != owner.ITEM_VERSION:
        raise ValueError('explicit logical-v2 item grant required; no legacy upgrade')
    contract = copy.deepcopy(todo['contract'])
    amendment = batch.get('item', {}).get('scope_amendment')
    if amendment:
        original = owner.binding_hash(contract)
        path = amendment.get('added_path')
        if (amendment.get('prior_contract_sha256') != original or
                not isinstance(path, str) or path in contract['resources']['allowed_paths'] or
                any(PurePosixPath(path).is_relative_to(PurePosixPath(root))
                    for root in contract['resources']['no_touch'])):
            raise ValueError('item scope amendment differs from original contract')
        contract['resources']['allowed_paths'].append(path)
        owner.validate_logical_contract(contract)
        if owner.binding_hash(contract) != amendment.get('contract_sha256'):
            raise ValueError('item scope amendment changed')
    return batch, contract


def live_item(rows, identity, batch_id, issue):
    batch, contract = bound(rows, identity, batch_id, issue)
    item = batch.get('item')
    if batch['revoked'] or not item or item['issue'] != issue or item['phase'] in {'closed', 'archived'}:
        raise ValueError('active unrevoked item reservation required')
    return batch, contract, item


def snapshot(guard, identity, batch_id, issue):
    with guard.transaction(read_only=True) as rows:
        batch, contract = bound(rows, identity, batch_id, issue)
        return copy.deepcopy(batch), copy.deepcopy(contract)


def runtime_attestation():
    """Attest the schema and onboarding loaded in this responding Gateway turn."""
    from agent.subagent_lifecycle import get_active_subagent_parent
    from tools.terminal_tool import TERMINAL_SCHEMA
    from hermes_cli import diggr_continuation as dc
    import psutil
    require_onboarding_context()
    agent = get_active_subagent_parent()
    observed = getattr(agent, '_diggr_onboarding_request', None)
    if (not isinstance(observed, tuple) or len(observed) != 4 or
            not isinstance(observed[3], str) or len(observed[3]) != 64):
        raise ValueError('current provider onboarding receipt unavailable')
    return dict(pid=os.getpid(), started_at=psutil.Process(os.getpid()).create_time(),
                terminal_schema_sha256=dc.object_hash(TERMINAL_SCHEMA),
                onboarding_sha256=observed[3])


def status(guard, identity, batch_id, issue, *, include_worker_route=False, include_runtime=False):
    from hermes_cli import diggr_continuation as dc
    runtime = runtime_attestation() if include_runtime else None
    with guard.transaction(read_only=True) as rows:
        batch, contract = bound(rows, identity, batch_id, issue)
        item = batch.get('item', {})
        task_id = batch['tasks'].get(issue)
        row = rows.get(task_id, {})
        phase = 'stopped' if batch['revoked'] else item.get('phase', 'authorized')
        claimed = bool(row.get('claim_phase') == 'claimed' and row.get('worker_pid') and row.get('worker_identity'))
        if include_worker_route and not row.get('worker_route'):
            raise ValueError('registered worker route unavailable')
        execution = dict(claimed=claimed, row_sha256=dc.object_hash(row) if task_id else None,
            **{k: row.get(k) for k in (
            'generation', 'status', 'gate', 'action', 'identity', 'reason', 'failure_evidence',
            'claim_phase', 'worker_pid', 'worker_identity', 'action_id', 'effect_id', 'worker_result')})
        policy = row.get('policy') or {}
        limit_generation = row.get('limit_generation')
        turn = (row.get('coordinator_turns') or {}).get(str(limit_generation), {})
        execution['recovery_readback'] = dict(
            recovery_kind=row.get('recovery_kind'), limit_generation=limit_generation,
            checkpoint_generation=(row.get('checkpoint') or {}).get('generation'),
            prior_turn=dict(generation=limit_generation, status=turn.get('status'),
                            failed=turn.get('failed')),
            correction_reinspection_effect_id=row.get('correction_reinspection'),
            status_only_review_reinspection_effect_id=row.get('status_only_review_reinspection'),
            budget={k: policy.get(k) for k in ('wakes', 'corrections', 'recoveries',
                                               'work_seconds', 'work_limit', 'revoked', 'limits')})
        if include_worker_route:
            execution['worker_route'] = row['worker_route']
        result = dict(plane_id=contract['plane_id'], phase=phase, task=task_id,
            budget_hash=owner.binding_hash(item.get('budget', {})), item=item,
            preparation_delivery=batch.get('preparation_delivery'), execution=execution)
        result = copy.deepcopy(result)
        for candidate_record in ([result['item'].get('candidate')] +
                                 result['item'].get('candidate_history', [])):
            if isinstance(candidate_record, dict):
                candidate_record.pop('build_argv', None)
        if runtime is not None:
            result['runtime'] = runtime
        return result


def worktree_matches(contract, item):
    path = Path(item['worktree'])
    if not path.is_dir():
        return False
    entries = git(contract['resources']['repo'], 'worktree', 'list', '--porcelain')
    return (f'worktree {path}\n' in entries + '\n' and
            git(path, 'branch', '--show-current') == item['branch'] and
            git(path, 'rev-parse', 'HEAD') == item['base'] and
            not git(path, 'status', '--porcelain'))


def prepared_replacement(rows, manifest, identity):
    """Validate an unused preparation for a bounded owner correction.

    Called at proposal and confirmation under the ledger lock. Never release a
    running attempt, change a target, or reset consumed budgets.
    """
    from hermes_cli import diggr_continuation as dc
    bid = manifest['replaces']
    if not isinstance(bid, str):
        return None
    old = rows.grants.get('batches', {}).get(bid, {})
    prepared = old.get('item')
    if not prepared or prepared.get('phase') in {'closed', 'archived'}:
        return None
    if (old.get('revoked') or old.get('replaced_by') or
            old.get('coordinator_identity') != identity or
            old.get('owner') != owner.stable_owner(identity) or
            prepared.get('phase') != 'prepared' or old.get('tasks') != {} or
            old.get('started_at') is not None or old.get('deadline') is not None or
            prepared.get('work_seconds') != 0 or prepared.get('task') or
            any(r.get('owner_batch') == bid or r.get('policy', {}).get('batch') == bid
                for r in rows.values())):
        raise ValueError('replacement requires an unused prepared reservation')
    before, after = copy.deepcopy(old['manifest']), copy.deepcopy(manifest)
    if (len(before['todos']) != 1 or len(after['todos']) != 1 or
            before['todos'][0]['contract'].get('version') != owner.ITEM_VERSION or
            after['todos'][0]['contract'].get('version') != owner.ITEM_VERSION):
        raise ValueError('prepared replacement requires the same logical-v2 item')
    before['replaces'] = after['replaces']
    before['todos'][0]['contract']['model']['reasoning_effort'] = after['todos'][0]['contract']['model']['reasoning_effort']
    contract = after['todos'][0]['contract']
    prior_contract = before['todos'][0]['contract']
    scope_amendment = (prior_contract['resources']['allowed_paths'] !=
                       contract['resources']['allowed_paths'] or
                       prior_contract['resources']['no_touch'] != contract['resources']['no_touch'])
    if scope_amendment:
        from pathlib import PurePosixPath
        allowed = contract['resources']['allowed_paths']
        forbidden = contract['resources']['no_touch']
        if not prepared.get('preparation_blocker'):
            raise ValueError('prepared scope amendment requires a documented preparation blocker')
        if (not all(PurePosixPath(path).suffix for path in allowed) or
                any(PurePosixPath(path).is_relative_to(PurePosixPath(no))
                    for path in allowed for no in forbidden)):
            raise ValueError('prepared scope amendment requires concrete files outside No-Touch')
        prior_contract['scope'] = contract['scope']
        prior_contract['resources']['allowed_paths'] = copy.deepcopy(allowed)
        prior_contract['resources']['no_touch'] = copy.deepcopy(forbidden)
    if (before != after or prepared['issue'] != owner.issue_key(after['todos'][0]) or
            prepared['budget'] != manifest['budgets'] or
            prepared['repo'] != contract['resources']['repo'] or
            prepared['plane_id'] != contract['plane_id']):
        raise ValueError('prepared replacement may only correct scope or reasoning; all other terms stay bound')
    if any(other_id != bid and b.get('item', {}).get('phase') not in {None, 'closed', 'archived'}
           for other_id, b in rows.grants.get('batches', {}).items()):
        raise ValueError('another item still owns the lane')
    if any(dc.target_reserved(r, prepared['target']) or dc.worktree_reserved(r, prepared['worktree'])
           for r in rows.values()):
        raise ValueError('prepared resources belong to an execution attempt')
    if not worktree_matches(contract, prepared):
        raise ValueError('prepared replacement requires the same clean registered worktree')
    binding = dc.bind_visible_target(prepared['target'])
    shell_identity = dc.bound_shell_identity(binding)
    return dict(item=copy.deepcopy(prepared), binding=binding, shell_identity=shell_identity,
                scope_amendment=scope_amendment)


def prepare(guard, identity, payload):
    from hermes_cli import diggr_continuation as dc
    bid, issue = payload['batch'], payload['issue']
    batch, contract = snapshot(guard, identity, bid, issue)
    if batch['revoked']:
        raise ValueError('owner stopped the item')
    repo = contract['resources']['repo']
    root = Path(contract['resources']['worktrees'])
    path = Path(payload['worktree'])
    branch = payload['branch']
    if (not path.is_absolute() or str(path.resolve()) != str(path) or
            not path.is_relative_to(root) or path == root or
            not branch.startswith('hermes/WP-' + contract['plane_id'] + '-') or
            git(repo, 'check-ref-format', '--branch', branch) != branch):
        raise ValueError('canonical item feature branch and bounded worktree required')
    base = git(repo, 'rev-parse', '--verify', 'origin/main^{commit}')
    if payload['base'] != base:
        raise ValueError('preparation base differs from freshly checked origin/main')
    binding = dc.bind_visible_target(payload['target'])
    dc.bound_shell_identity(binding)
    wanted = dict(issue=issue, plane_id=contract['plane_id'], repo=repo,
                  worktree=str(path), branch=branch, base=base, target=payload['target'])
    with guard.transaction() as rows:
        batch, _ = bound(rows, identity, bid, issue)
        if batch['revoked']:
            raise ValueError('item revoked during readiness check')
        reserved = batch.get('item', {}).get('phase') == 'reserved'
        if reserved and any(batch['item'].get(k) != wanted[k] for k in ('issue', 'plane_id', 'repo')):
            raise ValueError('successor reservation differs from prepared item')
        if batch.get('item') and not reserved:
            if any(batch['item'].get(k) != v for k, v in wanted.items()):
                raise ValueError('existing item preparation differs; reconcile same reservation')
            return copy.deepcopy(batch['item'])  # retry only via explicit prepare_readback
        for other_id, other in rows.grants.get('batches', {}).items():
            if other_id != bid and other.get('item', {}).get('phase') not in {None, 'closed', 'archived'}:
                raise ValueError('Coding lane belongs to another item; no worktree created')
        if any((row.get('identity') == identity and dc.ownership_pending(row)) or
               dc.target_reserved(row, payload['target']) or dc.worktree_reserved(row, str(path))
               for row in rows.values()):
            raise ValueError('earlier native attempt still owns the lane; no worktree created')
        if path.exists() or path.is_symlink():
            raise ValueError('new item requires an unused worktree path')
        # The existing ledger lock commits the reservation before branch creation.
        if reserved:
            batch['item'].update(wanted, phase='preparing')
        else:
            batch['item'] = dict(wanted, phase='preparing', reserved_at=time.time(),
                                 work_seconds=0, budget=dict(batch['manifest']['budgets']))
    try:
        git(repo, 'worktree', 'add', '-b', branch, str(path), base)
    except (OSError, subprocess.SubprocessError):
        with guard.transaction() as rows:
            batch, _ = bound(rows, identity, bid, issue)
            batch['item']['phase'] = 'preparation_unknown'
        raise ValueError('Git preparation failed; reservation retained for exact readback') from None
    return reconcile_preparation(guard, identity, payload)


def reconcile_preparation(guard, identity, payload, *, resume=False):
    bid, issue = payload['batch'], payload['issue']
    batch, contract = snapshot(guard, identity, bid, issue)
    item = batch.get('item')
    if not item or item['phase'] not in {'preparing', 'preparation_unknown', 'prepared'}:
        raise ValueError('no pending preparation to reconcile')
    if not worktree_matches(contract, item):
        if not resume:
            raise ValueError('preparation incomplete; inspect and use item_prepare_resume for the same resources')
        path = Path(item['worktree'])
        if path.exists() or path.is_symlink():
            raise ValueError('preparation has no exact clean registered worktree; ownership retained')
        # Git atomically protects the same branch/path. This is preparation only,
        # never a repeated worker dispatch. Do not delete partial Git state.
        refs = git(contract['resources']['repo'], 'for-each-ref', '--format=%(objectname)',
                   'refs/heads/' + item['branch'])
        if refs and refs != item['base']:
            raise ValueError('reserved branch changed; retain preparation for inspection')
        args = ['worktree', 'add'] + ([] if refs else ['-b', item['branch']])
        args += [item['worktree'], item['branch'] if refs else item['base']]
        git(contract['resources']['repo'], *args)
        if not worktree_matches(contract, item):
            raise ValueError('Git preparation readback still differs; ownership retained')
    with guard.transaction() as rows:
        current, _ = bound(rows, identity, bid, issue)
        if current['item'] != item or current['revoked']:
            raise ValueError('preparation changed during readback')
        current['item']['phase'] = 'prepared'
        if current.get('flow_version') == 3 and not current['item'].get('coding_assignment'):
            import secrets
            current['item']['coding_assignment'] = dict(
                id=secrets.token_hex(24), status='pending', created_at=time.time())
        return copy.deepcopy(current['item'])


def bind_task(batch, task):
    from hermes_cli.diggr_continuation import read_hashed_json
    item = batch.get('item')
    route = task['worker_route']
    if (not item or item['phase'] != 'prepared' or item['issue'] != task['owner_issue'] or
            any(route.get(k) != item[k] for k in ('worktree', 'branch', 'plane_id')) or
            route.get('visible_target') != item['target']):
        raise ValueError('registration must consume the same pre-worktree item reservation')
    packet = read_hashed_json(route['packet'], route['packet_sha256'])
    item.update(phase='coding', task=task['task'], lane=packet['coding_lane'])


def source_tree(worktree):
    # Main explicitly stages the intended source before inviting the tester.
    # No ignored build/dependency output is automatically staged here.
    if git(worktree, 'diff', '--name-only') or git(worktree, 'ls-files', '--others', '--exclude-standard'):
        raise ValueError('candidate contains unstaged or untracked source; prepare explicit index first')
    return git(worktree, 'write-tree')


def build_digest(path, worktree):
    root = Path(path)
    if (not root.is_absolute() or str(root.resolve()) != str(root) or
            not root.is_relative_to(Path(worktree)) or not root.exists()):
        raise ValueError('candidate build must be a real path inside the bound worktree')
    files = sorted(root.rglob('*')) if root.is_dir() else [root]
    h = hashlib.sha256()
    count = 0
    for file in files:
        if file.is_symlink():
            raise ValueError('test build contains an unbound symlink')
        if file.is_file():
            h.update(str(file.relative_to(root.parent)).encode() + b'\0')
            h.update(hashlib.sha256(file.read_bytes()).digest())
            count += 1
    if not count:
        raise ValueError('empty build cannot be handed to a tester')
    return h.hexdigest()


def verify_candidate(item):
    candidate = item.get('candidate')
    if (not candidate or source_tree(item['worktree']) != candidate['tree'] or
            build_digest(candidate['build_path'], item['worktree']) != candidate['build_sha256']):
        raise ValueError('tested source or build changed; new candidate and acceptance required')
    return candidate


def build_group_active(pgid):
    """A dedicated build session may still own children after its leader exits."""
    if type(pgid) is not int or pgid <= 0:
        raise ValueError('build session identity missing; readback cannot infer absence')
    try:
        os.killpg(pgid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def build_readback(guard, identity, payload):
    bid, issue = payload['batch'], payload['issue']
    with preparation_lock(guard):
        batch, _ = snapshot(guard, identity, bid, issue)
        item = batch.get('item', {})
        if item.get('phase') == 'candidate_failed':
            return dict(phase='candidate_failed', may_rebuild=True,
                        build=copy.deepcopy(item.get('candidate_build')))
        if item.get('phase') not in {'building_candidate', 'candidate_build_unknown'}:
            raise ValueError('no interrupted candidate build to reconcile')
        pgid = item.get('candidate_build', {}).get('pgid')
        if type(pgid) is not int or pgid <= 0:
            # A crash between spawn and PGID persistence cannot prove that no
            # child is still building. Keep the build fenced for inspection.
            with guard.transaction() as rows:
                _, _, active = live_item(rows, identity, bid, issue)
                if active != item:
                    raise ValueError('build changed during readback')
                active['phase'] = 'candidate_build_unknown'
            return dict(phase='candidate_build_unknown', may_rebuild=False,
                        reason='build process identity missing; inspect effects before any rebuild',
                        build=copy.deepcopy(item.get('candidate_build')))
        if build_group_active(pgid):
            return dict(phase=item['phase'], may_rebuild=False, reason='same build session still active',
                        build=copy.deepcopy(item['candidate_build']))
        with guard.transaction() as rows:
            _, _, active = live_item(rows, identity, bid, issue)
            if active != item:
                raise ValueError('build changed during readback')
            active.update(phase='candidate_failed', build_reconciled_at=time.time())
        # This proves only the recorded build session ended, never a passing
        # build. A new build must seal source/output again before any test card.
        return dict(phase='candidate_failed', may_rebuild=True,
                    build=copy.deepcopy(item.get('candidate_build')))


def verify_completed_exit(row):
    """Use the same native exit paths as recovery, including after restart."""
    from hermes_cli import diggr_continuation as dc
    if row.get('direct_worker') is True:
        dc.verify_direct_worker_exit(row, completed=True)
        return
    from tools.process_registry import process_registry
    if process_registry.get(row.get('process_id')) is None:
        dc.verify_restarted_reservation_exit(row)
    else:
        dc.verify_prior_exit(row)


def verified_app105_scope_message(identity, message_id, after):
    """Read the earlier direct Owner decision from its original Telegram session.

    The persisted message is evidence, not a substitute for the current native
    Telegram Main context required by item_scope_amend.
    """
    import sqlite3
    if type(message_id) is not int or message_id < 1:
        raise ValueError('original Owner message ID required')
    db = Path(identity['home']) / 'state.db'
    with sqlite3.connect(db.as_uri() + '?mode=ro', uri=True, timeout=2) as conn:
        conn.execute('PRAGMA query_only=ON')
        session = conn.execute(
            'SELECT source,user_id,session_key,profile_name,chat_id,chat_type,thread_id '
            'FROM sessions WHERE id=?', (identity['session'],)).fetchone()
        message = conn.execute(
            'SELECT role,content,timestamp FROM messages WHERE id=? AND session_id=?',
            (message_id, identity['session'])).fetchone()
        context = conn.execute(
            "SELECT content FROM messages WHERE session_id=? AND id<? AND role='assistant' "
            "AND content!='' ORDER BY id DESC LIMIT 2", (identity['session'], message_id)).fetchall()
    if (session != ('telegram', owner.OWNER, identity['session_key'], identity['profile'],
                    owner.OWNER, 'dm', None) or not message or message[0] != 'user' or
            message[1].strip().casefold() != 'freigabe' or message[2] <= after or
            len(context) != 2 or not all(isinstance(row[0], str) for row in context)):
        raise ValueError('original direct Owner scope decision unavailable')
    latest, specific = (row[0] for row in context)
    if (not all(word in latest.casefold() for word in ('testdatei', 'discography', 'freigabe')) or
            APP105_TEST_SCOPE_PATH not in specific or 'Catalog' not in specific or
            'Discography' not in specific or 'ohne Anzahl' not in specific):
        raise ValueError('Owner decision does not follow the exact test-scope request')
    return dict(message_id=message_id, at=message[2],
                message_sha256=hashlib.sha256(message[1].encode()).hexdigest(),
                context_sha256=hashlib.sha256((latest + '\n' + specific).encode()).hexdigest())


def amend_app105_test_scope(guard, identity, payload):
    """Add one Owner-approved adjacent test only after a completed flow-v3 run."""
    from hermes_cli import diggr_continuation as dc
    if set(payload) != {'operation', 'batch', 'issue', 'added_path',
                        'owner_message_id', 'row_sha256'}:
        raise ValueError('exact APP-105 scope amendment fields required')
    context = dc.EVENT_CONTEXT.get()
    if (not context or context.get('identity') != identity or
            not context.get('direct_owner_event') or context.get('native_wake') is not None or
            identity.get('platform') != 'telegram' or identity.get('chat_id') != owner.OWNER or
            identity.get('user_id') != owner.OWNER):
        raise ValueError('original direct Telegram Owner/Main context required')
    path = payload.get('added_path')
    if path != APP105_TEST_SCOPE_PATH:
        raise ValueError('only the exact approved APP-105 test file may be added')
    with guard.transaction(read_only=True) as rows:
        batch, contract, item = live_item(rows, identity, payload['batch'], payload['issue'])
        existing = item.get('scope_amendment')
        if existing:
            if (existing['added_path'] == path and
                    existing['owner_message_id'] == payload.get('owner_message_id')):
                return copy.deepcopy(existing)
            raise ValueError('a different item scope amendment already exists')
        row = copy.deepcopy(rows.get(item.get('task'), {}))
        original = copy.deepcopy(batch['manifest']['todos'][0]['contract'])
        item_before = copy.deepcopy(item)
        if (batch.get('flow_version') != 3 or contract != original or
                batch.get('proposal') != owner.binding_hash(batch['manifest']) or
                contract['plane_id'] != 'APP-105' or
                batch.get('tasks') != {payload['issue']: row.get('task')} or
                row.get('owner_batch') != payload['batch'] or
                row.get('owner_issue') != payload['issue'] or
                item.get('task') != row.get('task') or
                item.get('phase') != 'coding' or item.get('candidate') or
                row.get('direct_worker') is not True or row.get('status') != 'blocked' or
                row.get('gate') != 'main_validation' or row.get('visible_sent') is not True or
                row.get('claim_phase') != 'claimed' or
                row.get('logical_contract') != original or
                row.get('policy', {}).get('revoked') or row.get('hard_stop') or
                row.get('authorization_expires_at') or
                path in original['resources']['allowed_paths'] or
                any(PurePosixPath(path).is_relative_to(PurePosixPath(root))
                    for root in original['resources']['no_touch']) or
                payload.get('row_sha256') != dc.object_hash(row)):
            raise ValueError('same stopped APP-105 result and exact current row required')
        result = row.get('worker_result') or {}
        if (result.get('artifact_status') != 'present' or
                result.get('generation') is None or
                not result.get('evidence', {}).get('sha256') or
                any(result.get(k) != row.get(k) for k in
                    ('task', 'artifact', 'action_id', 'effect_id'))):
            raise ValueError('bound completed worker result required')
    proof = verified_app105_scope_message(identity, payload.get('owner_message_id'),
                                          result['observed_at'])
    if proof['at'] <= item_before['reserved_at']:
        raise ValueError('Owner scope decision predates this item reservation')
    if (row['worker_route']['worktree'] != item_before['worktree'] or
            row['worker_route']['branch'] != item_before['branch']):
        raise ValueError('completed worker route differs from the reserved APP-105 worktree')
    verify_completed_exit(row)
    dc.verified_bytes(row['artifact'], result['evidence']['sha256'])
    dc.validate_logical_changes(row)
    actual, branch, head, _ = dc._git_route_identity(item_before['worktree'])
    if (actual != item_before['worktree'] or branch != item_before['branch'] or
            head != item_before['base'] or row['worker_route']['worktree'] != actual or
            not (Path(actual) / path).is_file()):
        raise ValueError('same APP-105 worktree and existing adjacent test required')
    retained = dc._worktree_snapshot(actual)
    amended = copy.deepcopy(original)
    amended['resources']['allowed_paths'].append(path)
    owner.validate_logical_contract(amended)
    receipt = dict(added_path=path, prior_contract_sha256=owner.binding_hash(original),
                   contract_sha256=owner.binding_hash(amended),
                   original_row_sha256=dc.object_hash(row), owner_message_id=proof['message_id'],
                   owner_message_sha256=proof['message_sha256'],
                   owner_context_sha256=proof['context_sha256'],
                   result_sha256=result['evidence']['sha256'],
                   retained_worktree_sha256=retained,
                   native_main_event=context['direct_owner_event'], at=time.time())
    with guard.transaction() as rows:
        batch, current_contract, active = live_item(rows, identity, payload['batch'], payload['issue'])
        if (active != item_before or current_contract != original or
                rows.get(row['task']) != row or batch.get('revoked') or
                dc._worktree_snapshot(actual) != retained):
            raise ValueError('item, worker or worktree changed during scope verification')
        verified_app105_scope_message(identity, payload['owner_message_id'],
                                      result['observed_at'])
        verify_completed_exit(rows[row['task']])
        dc.verified_bytes(row['artifact'], result['evidence']['sha256'])
        dc.validate_logical_changes(row)
        for other in rows.grants.get('batches', {}).values():
            prior = other.get('item', {}).get('scope_amendment') or {}
            if prior.get('owner_message_id') == proof['message_id']:
                raise ValueError('Owner scope decision already consumed')
        active['scope_amendment'] = receipt
    return copy.deepcopy(receipt)


def rebind_unstarted_app105_assignment(guard, identity, payload):
    """Move one accepted correction assignment to a new canonical Coding process."""
    import subprocess
    from hermes_cli import diggr_continuation as dc
    required = {'operation', 'batch', 'issue', 'assignment', 'row_sha256'}
    if set(payload) not in (required, required | {'target'}):
        raise ValueError('exact accepted APP-105 assignment rebind fields required')
    context = dc.EVENT_CONTEXT.get()
    if (not context or context.get('identity') != identity or
            not context.get('direct_owner_event') or context.get('native_wake') is not None or
            identity.get('platform') != 'telegram' or identity.get('chat_id') != owner.OWNER or
            identity.get('user_id') != owner.OWNER):
        reason = ((context or {}).get('owner_input_failure') or
                  'tool call is not bound to the direct original Owner/Main turn')
        raise ValueError('direct original Owner/Main context required for assignment rebind: ' + reason)
    with guard.transaction(read_only=True) as rows:
        batch, contract, item = live_item(rows, identity, payload['batch'], payload['issue'])
        assignment = item.get('coding_assignment') or {}
        if assignment.get('id') != payload['assignment']:
            raise ValueError('different APP-105 assignment')
        if assignment.get('rebind'):
            prior = assignment['rebind']
            if (assignment.get('status') == 'pending' and
                    prior.get('row_sha256') == payload['row_sha256'] and
                    payload.get('target', item['target']) == item['target']):
                return copy.deepcopy(prior)
        row = copy.deepcopy(rows.get(item.get('task'), {}))
        before = copy.deepcopy(item)
        old = copy.deepcopy(assignment)
        if (contract['plane_id'] != 'APP-105' or batch.get('flow_version') != 3 or
                not item.get('scope_amendment') or item.get('phase') != 'correction_needed' or
                assignment.get('status') != 'accepted' or not assignment.get('accepted_at') or
                not assignment.get('session') or assignment.get('task') or
                not isinstance(assignment.get('binding'), dict) or
                assignment.get('correction_of') != row.get('worker_result', {}).get('generation') or
                row.get('status') != 'blocked' or row.get('gate') != 'coding_correction' or
                row.get('direct_worker') is not True or
                row.get('owner_batch') != payload['batch'] or
                row.get('owner_issue') != payload['issue'] or
                payload['row_sha256'] != dc.object_hash(row) or
                not owner.allowed(row, time.time()) or
                row['policy']['corrections'] >= row['policy']['limits']['corrections'] or
                any(row.get(field) is not None and time.time() >= row[field]
                    for field in ('hard_stop', 'authorization_expires_at'))):
            raise ValueError('same unstarted, budgeted APP-105 correction required')
    dc.verify_direct_worker_exit(row, completed=True)
    dc.validate_logical_changes(row)
    dc.verify_scope_amendment_correction(batch, item, row, contract)
    current_correction_findings(item, row, contract)
    old_binding = old['binding']
    old_process = old_binding.get('process', {}).get('identity')
    if not dc.process_absent(old_process, original_process_only=True):
        raise ValueError('accepted Coding process still active or exit unproved')
    new_binding = dc.preflight_coding_coordinator()
    new_process = new_binding.get('process', {}).get('identity', {})
    if (new_binding.get('workspace') != old_binding.get('workspace') or
            not isinstance(new_process, dict) or
            type(new_process.get('pid')) is not int or new_process['pid'] <= 0 or
            new_process['pid'] == old_process['pid'] or
            dc.process_absent(new_process, original_process_only=True)):
        raise ValueError('new canonical Coding process and same workspace required')
    actual, branch, head, _ = dc._git_route_identity(item['worktree'])
    retained = dc._worktree_snapshot(item['worktree'])
    packet = dc.read_hashed_json(row['worker_route']['packet'],
                                 row['worker_route']['packet_sha256'])
    pr = item.get('pr')
    expected_head = packet['base']
    if pr:
        if (pr.get('state') != 'OPEN' or pr.get('headRefName') != item['branch'] or
                not isinstance(pr.get('headRefOid'), str)):
            raise ValueError('open APP-105 PR and bound branch required for assignment rebind')
        expected_head = pr['headRefOid']
    if (actual != item['worktree'] or branch != item['branch'] or
            head != expected_head or row['worker_route']['worktree'] != actual):
        raise ValueError('same APP-105 correction worktree required')
    target = payload.get('target', item['target'])
    target_binding = None
    if target != item['target']:
        old_target = item['target']
        old_shell = row.get('visible_binding', {}).get('shell')
        if (not isinstance(target, dict) or set(target) != {'workspace', 'surface'} or
                target['surface'] != old_target['surface'] or
                target['workspace'] == old_target['workspace'] or
                row.get('visible_binding', {}).get('target') != old_target or
                row.get('worker_route', {}).get('visible_target') != old_target or
                not isinstance(old_shell, dict) or
                not dc.process_absent(old_shell, original_process_only=True)):
            raise ValueError('same exited APP-105 executor surface required')
        try:
            dc.visible_surface(old_target)
        except (ValueError, OSError, subprocess.SubprocessError):
            pass  # Only an unavailable old target may be rebound.
        else:
            raise ValueError('old APP-105 executor target still available')
        target_binding = dc.bind_visible_target(target)
        dc.verify_visible_target(target_binding, idle=True)
    receipt = dict(assignment=old['id'], old_acceptance=old,
                   target=copy.deepcopy(new_binding), row_sha256=payload['row_sha256'],
                   retained_worktree_sha256=retained,
                   native_owner_event=context['direct_owner_event'], at=time.time())
    if target_binding is not None:
        receipt['executor_target'] = copy.deepcopy(target_binding)
    with guard.transaction() as rows:
        if target_binding is not None:
            dc.verify_visible_target(target_binding, idle=True)
        batch, current_contract, active = live_item(rows, identity, payload['batch'], payload['issue'])
        current = rows.get(row['task'])
        if (active != before or current != row or current_contract != contract or
                dc.preflight_coding_coordinator() != new_binding or
                not dc.process_absent(old_process, original_process_only=True) or
                dc.process_absent(new_process, original_process_only=True) or
                dc._worktree_snapshot(actual) != retained or
                (target_binding is not None and dc.bind_visible_target(target) != target_binding)):
            raise ValueError('Coding lane, item or worktree changed during assignment rebind')
        assignment = active['coding_assignment']
        if assignment.get('rebind'):
            assignment.setdefault('rebind_history', []).append(copy.deepcopy(assignment['rebind']))
        for key in ('binding', 'offered_at', 'session', 'accepted_at'):
            assignment.pop(key, None)
        assignment.update(status='pending', rebind=receipt)
        if target_binding is not None:
            active['target'] = copy.deepcopy(target)
    return copy.deepcopy(receipt)


def candidate(guard, identity, payload):
    from hermes_cli import diggr_continuation as dc
    bid, issue = payload['batch'], payload['issue']
    batch, contract = snapshot(guard, identity, bid, issue)
    item = batch.get('item', {})
    with guard.transaction(read_only=True) as rows:
        _, _, item = live_item(rows, identity, bid, issue)
        item = copy.deepcopy(item)
        row = copy.deepcopy(rows.get(item.get('task'), {}))
        before = copy.deepcopy(rows)
    result = row.get('worker_result', {})
    if (type(result.get('exit_code')) is not int or result.get('artifact_status') != 'present' or
            any(result.get(k) != row.get(k) for k in ('task', 'artifact', 'action_id', 'effect_id'))):
        raise ValueError('candidate needs the completed native worker and bound result')
    verify_completed_exit(row)
    dc.validate_logical_changes(row)
    raw = Path(row['artifact']).read_bytes()
    if hashlib.sha256(raw).hexdigest() != result.get('evidence', {}).get('sha256'):
        raise ValueError('worker result changed; never overwrite the raw result')
    assessment = read_evidence(payload['assessment'], contract)
    if (assessment.get('task') != row['task'] or
            assessment.get('result_sha256') != result['evidence']['sha256'] or
            assessment.get('result') != 'passed' or not assessment.get('checks')):
        raise ValueError('Main assessment must bind and validate the existing worker result')
    reassessing = item['phase'] == 'correction_needed'
    if reassessing:
        # A missing evidence check can be completed without another writer.
        # Main must explicitly resolve the exact failed assessment of this result.
        failure_ref = item.get('correction')
        if (item.get('candidate') or row.get('gate') != 'reconcile' or
                row.get('status') not in {'pending', 'executing', 'blocked'} or
                not failure_ref or assessment.get('reassessment_of') != failure_ref):
            raise ValueError('reassessment requires the same pre-candidate correction and completed result')
        failure = read_evidence(failure_ref, contract)
        if (any(failure.get(k) != row[k] for k in ('task', 'action_id', 'effect_id')) or
                failure.get('worker_generation') != result.get('generation') or
                failure.get('result_sha256') != result['evidence']['sha256'] or
                failure.get('result') != 'failed' or not failure.get('findings') or
                assessment.get('resolved_findings') != failure['findings']):
            raise ValueError('reassessment must resolve every bound finding of the same worker result')
    tree = source_tree(item['worktree'])
    steps = payload['steps']
    automatic = contract['acceptance'].get('mode') == 'automatic'
    if (not isinstance(steps, list) or not steps or
            any(not isinstance(s, dict) or
                set(s) != ({'action', 'expected', 'kind'} if automatic else {'action', 'expected'}) or
                not all(isinstance(v, str) and v.strip() for v in s.values()) or
                (automatic and s['kind'] not in {'ui', 'track_playback'}) for s in steps)):
        raise ValueError('short test card requires actions and expected results')
    if automatic and len({
            (step['action'], step['expected']) for step in steps}) < 2:
        raise ValueError('automatic Computer Use needs at least two distinct UI test cases')
    if automatic:
        required_kinds = set(contract['acceptance']['required_test_kinds'])
        step_kinds = [step['kind'] for step in steps]
        if (not required_kinds <= set(step_kinds) or
                ('track_playback' in required_kinds and step_kinds.count('track_playback') < 2)):
            raise ValueError('test card omits owner-approved playback or UI cases')
    if (not isinstance(payload.get('build_argv'), list) or not payload['build_argv'] or
            not all(isinstance(a, str) and a for a in payload['build_argv'])):
        raise ValueError('explicit build argv required for candidate provenance')
    # Build is a scoped Main test action; no shell parsing and no hidden fallback.
    if 'test' not in contract['actions']['main']:
        raise ValueError('Main build/test right missing from the confirmed item')
    if item.get('candidate') and item['phase'] in {'awaiting_test', 'accepted_candidate'}:
        existing = verify_candidate(item)
        if all(existing[k] == payload[k] for k in ('build_argv', 'build_path', 'steps')):
            return dict(candidate=existing, test_card=test_card(
                item, existing, contract['acceptance'].get('mode') == 'automatic'))
    if not reassessing and item['phase'] not in {'coding', 'awaiting_test', 'accepted_candidate', 'candidate_failed'}:
        raise ValueError('candidate already building or item has advanced; read back first')
    build_log = Path(contract['resources']['artifacts']) / ('candidate-build-' + secrets.token_hex(12) + '.log')
    build_record = dict(tree=tree, argv_sha256=owner.binding_hash(payload['build_argv']), cwd=item['worktree'],
                        log=str(build_log), state='launching', started_at=time.time())
    with guard.transaction() as rows:
        _, _, active = live_item(rows, identity, bid, issue)
        if active != item or (reassessing and not dc.recovery_snapshot_matches(before, rows)):
            raise ValueError('item changed before candidate build')
        if reassessing:
            if (read_evidence(payload['assessment'], contract) != assessment or
                    read_evidence(failure_ref, contract) != failure):
                raise ValueError('reassessment changed before candidate build')
            dc.verified_bytes(row['artifact'], result['evidence']['sha256'])
            # Fence the old recovery wake before a build can create effects.
            rows[item['task']]['gate'] = 'main_validation'
        active.update(phase='building_candidate', candidate_build_tree=tree,
                      candidate_build=build_record)
    child = None
    try:
        # The approved artifact root contains the complete private output. The
        # native readback exposes only the path and bounded result metadata.
        fd = os.open(build_log, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'wb') as log:
            log.write((json.dumps({'argv': payload['build_argv'], 'cwd': item['worktree']}) + '\n').encode())
            log.flush()
            child = subprocess.Popen(payload['build_argv'], cwd=item['worktree'], start_new_session=True,
                                     stdout=log, stderr=subprocess.STDOUT)
            with guard.transaction() as rows:
                _, _, active = live_item(rows, identity, bid, issue)
                active['candidate_build'].update(pgid=child.pid, state='running')
            try:
                child.communicate(timeout=1800)
            except subprocess.TimeoutExpired:
                # Fence only the dedicated build group. A surviving descendant
                # keeps the build in unknown and bars a second build.
                try:
                    os.killpg(child.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                child.communicate(timeout=10)
                raise
            log.flush()
            os.fsync(log.fileno())
        build_record.update(pgid=child.pid, exit_code=child.returncode,
                            log_sha256=hashlib.sha256(build_log.read_bytes()).hexdigest())
        if child.returncode != 0:
            raise ValueError('build returned a nonzero exit')
        if build_group_active(child.pid):
            raise ValueError('build descendants remain active')
        if source_tree(item['worktree']) != tree:
            raise ValueError('source changed while building; candidate not offered')
        value = dict(plane_id=item['plane_id'], tree=tree, base=git(item['worktree'], 'rev-parse', 'HEAD'),
                     build_path=payload['build_path'], build_sha256=build_digest(payload['build_path'], item['worktree']),
                     build_argv=payload['build_argv'], steps=steps,
                     assessment=payload['assessment'], result_sha256=result['evidence']['sha256'],
                     criteria=contract['acceptance']['criteria'], created_at=time.time())
    except (OSError, subprocess.SubprocessError, ValueError) as exc:
        # No raw stderr or private prompt is copied into a tool response.
        build_record.update(state='failed', error_type=type(exc).__name__,
                            error_message=('build executable unavailable' if isinstance(exc, FileNotFoundError)
                                           else 'build timed out' if isinstance(exc, subprocess.TimeoutExpired)
                                           else 'build or provenance check failed'),
                            exit_code=child.returncode if child is not None else None)
        if build_log.is_file():
            build_record['log_sha256'] = hashlib.sha256(build_log.read_bytes()).hexdigest()
        with guard.transaction() as rows:
            _, _, active = live_item(rows, identity, bid, issue)
            if active.get('phase') == 'building_candidate' and active.get('candidate_build_tree') == tree:
                # A timeout does not prove descendants have exited. Preserve
                # uncertainty rather than permit an overlapping rebuild.
                active['phase'] = ('candidate_build_unknown' if child is not None and build_group_active(child.pid)
                                   else 'candidate_failed')
                active['candidate_build'].update(build_record)
        raise ValueError('candidate build or provenance failed; inspect this item before inviting a tester') from None
    with guard.transaction() as rows:
        _, _, active = live_item(rows, identity, bid, issue)
        active['candidate_build'].update(build_record, state='passed')
    value['id'] = owner.binding_hash(dict(batch=bid, issue=issue, candidate=value))
    with guard.transaction() as rows:
        current, _, active = live_item(rows, identity, bid, issue)
        if active.get('phase') != 'building_candidate' or active.get('candidate_build_tree') != tree:
            raise ValueError('item changed during candidate build')
        if active.get('candidate'):
            active.setdefault('candidate_history', []).append(active['candidate'])
        active.update(phase='awaiting_test', candidate=value)
        task = rows[item['task']]
        for key in ('acceptance', 'test_delegation', 'test_result', 'pr', 'review',
                    'merge_approval', 'closeout'):
            active.pop(key, None)
        if contract['acceptance'].get('mode') == 'automatic':
            active['test_delegation'] = dict(candidate=value['id'], tree=value['tree'],
                confirmation_event=current['confirmation_event'], mode='automatic', at=time.time())
            task.update(status='pending', gate='main_live', generation=task['generation'] + 1,
                        due=time.time(), action='Computer-Use test the exact candidate before publication')
        else:
            task.update(status='awaiting_user', gate='item_test', generation=task['generation'] + 1,
                        action='wait for candidate-bound owner acceptance')
    return dict(candidate=value, test_card=test_card(
        item, value, contract['acceptance'].get('mode') == 'automatic'))


def test_card(item, value, automatic=False):
    lines = [item['plane_id'] + ': bereit zum Funktionstest.', 'Quellbaum: ' + value['tree']]
    lines += [f"{i}. {('[' + s['kind'] + '] ') if automatic else ''}{s['action']} → {s['expected']}"
              for i, s in enumerate(value['steps'], 1)]
    if automatic:
        lines.append('Automatikmodus: Main testet mit Computer Use; vollständiger gebundener PASS ist vor Commit/PR Pflicht.')
        lines.append('Jeder beobachtete Check belegt die IDs des nativen Computer-Use-Aktionsresultats (action_message_id) und der folgenden Bildschirmaufnahme (capture_message_id) aus derselben Main-Sitzung.')
    else:
        lines += ['Nach bestandenem Test freigeben: /continuation accept ' + value['id'],
                  'Testvertretung durch Main: /continuation delegate-test ' + value['id']]
    return '\n'.join(lines)


def read_evidence(ref, contract):
    path = Path(ref['path'])
    if (not path.is_absolute() or str(path.resolve()) != str(path) or
            not path.is_relative_to(Path(contract['resources']['artifacts']))):
        raise ValueError('evidence must be a canonical file in the approved artifact root')
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != ref['sha256']:
        raise ValueError('evidence changed')
    return json.loads(raw)


def validate_automatic_test(report, candidate, required_kinds):
    """Require observed UI evidence for every criterion and test-card step."""
    checks = report.get('checks')
    if (not isinstance(required_kinds, list) or not required_kinds or
            any(not isinstance(kind, str) or kind not in {'ui', 'track_playback'}
                for kind in required_kinds)):
        raise ValueError('automatic test lacks owner-approved test kinds')
    if ('track_playback' in required_kinds and
            sum(step.get('kind') == 'track_playback' for step in candidate['steps']) < 2):
        raise ValueError('test card omits owner-approved playback cases')
    required = {'criterion', 'action', 'expected', 'observed', 'result', 'kind'}
    if (not isinstance(checks, list) or not checks or any(
            not isinstance(check, dict) or not required <= set(check) or
            any(not isinstance(check[key], str) or not check[key].strip()
                for key in required - {'result'}) or check['result'] != 'passed'
            or check['kind'] not in {'ui', 'track_playback'} for check in checks)):
        raise ValueError('automatic Computer Use requires passing observed checks')
    if (not set(required_kinds) <= {check['kind'] for check in checks} or
            any(not any(check['criterion'] == criterion for check in checks)
            for criterion in candidate['criteria']) or
            any(not any(all(check[key] == step[key] for key in ('action', 'expected', 'kind'))
                        for check in checks) for step in candidate['steps']) or
            any(not any(all(check[key] == step[key] for key in ('action', 'expected', 'kind'))
                        for step in candidate['steps']) for check in checks)):
        raise ValueError('automatic Computer Use did not cover every criterion and test-card step')
    track_fields = ('track', 'column', 'duration_seconds')
    tracks = [check for check in checks if check['kind'] == 'track_playback']
    if any(check['kind'] == 'ui' and any(key in check for key in track_fields) for check in checks):
        raise ValueError('track evidence requires a track_playback test-card step')
    if tracks:
        if (any(not isinstance(check.get('track'), str) or not check['track'].strip() or
                    not isinstance(check.get('column'), str) or not check['column'].strip() or
                    type(check.get('duration_seconds')) not in (int, float) or
                    not 0 < check['duration_seconds'] <= 10 for check in tracks) or
                len({check['column'].strip().casefold() for check in tracks}) < 2 or
                len({check['track'].strip().casefold() for check in tracks}) < 2):
            raise ValueError('track playback needs different columns and at most ten seconds per track')
        for step in candidate['steps']:
            if step['kind'] != 'track_playback':
                continue
            matching = [check for check in tracks if all(check[key] == step[key]
                        for key in ('action', 'expected', 'kind'))]
            if len({(check['track'], check['column'], check['duration_seconds'])
                    for check in matching}) != 1:
                raise ValueError('each track playback step needs one consistent native action')


def compression_trace_session(conn, session_id, root_session):
    """Accept only the unique native compression continuation of the grant session."""
    seen = set()
    for _ in range(100):
        if session_id == root_session:
            return True
        if not session_id or session_id in seen:
            return False
        seen.add(session_id)
        try:
            child = conn.execute(
                'SELECT parent_session_id FROM sessions WHERE id=?', (session_id,)).fetchone()
            if not child or not child[0]:
                return False
            parent = conn.execute(
                'SELECT end_reason FROM sessions WHERE id=?', (child[0],)).fetchone()
            if not parent or parent[0] != 'compression':
                return False
            continuations = conn.execute(
                "SELECT id FROM sessions WHERE parent_session_id=? "
                "AND json_extract(COALESCE(model_config,'{}'),'$._branched_from') IS NULL "
                "AND json_extract(COALESCE(model_config,'{}'),'$._delegate_from') IS NULL "
                "AND COALESCE(source,'')!='tool' "
                "AND (ended_at IS NULL OR end_reason='compression')",
                (child[0],)).fetchall()
        except sqlite3.Error:
            return False
        if len(continuations) != 1 or continuations[0][0] != session_id:
            return False
        session_id = child[0]
    return False


def verify_computer_use_trace(report, candidate, identity):
    """Bind each reported UI check to completed native Computer Use calls."""
    from hermes_state import SessionDB
    checks = report['checks']
    refs = [(check.get('action_message_id'), check.get('capture_message_id')) for check in checks]
    if any(type(action) is not int or type(capture) is not int or action < 1 or capture < 1
           for action, capture in refs):
        raise ValueError('automatic test needs native action and capture message IDs for every check')
    ids = sorted({message_id for pair in refs for message_id in pair})
    db = Path(identity['home']) / 'state.db'
    with sqlite3.connect(db.as_uri() + '?mode=ro', uri=True, timeout=2) as conn:
        conn.execute('PRAGMA query_only=ON')
        rows = conn.execute(
            'SELECT id,session_id,role,tool_name,tool_call_id,content,timestamp FROM messages '
            'WHERE id IN (' + ','.join('?' for _ in ids) + ')', ids).fetchall()
        native = {row[0]: row for row in rows}
        calls = {}
        for message_id in ids:
            row = native.get(message_id)
            if (not row or row[2:4] != ('tool', 'computer_use') or
                    not isinstance(row[4], str) or not row[4] or
                    type(row[6]) not in (int, float) or row[6] < candidate['created_at'] or
                    not compression_trace_session(conn, row[1], identity['session'])):
                raise ValueError('Computer Use result missing from current native session and candidate')
            stored_content = row[5]
            if not isinstance(stored_content, str) or not stored_content.strip():
                raise ValueError('Computer Use result has no persisted observation')
            content = SessionDB._decode_content(stored_content)
            if isinstance(content, list):
                summaries = [part.get('text') for part in content if isinstance(part, dict) and
                             part.get('type') == 'text' and isinstance(part.get('text'), str)]
                images = [part.get('image_url', {}).get('url') for part in content
                          if isinstance(part, dict) and part.get('type') == 'image_url' and
                          isinstance(part.get('image_url'), dict)]
                if (not summaries or not any(isinstance(url, str) and
                        url.startswith('data:image/') and ';base64,' in url and
                        url.split(';base64,', 1)[1] for url in images)):
                    raise ValueError('Computer Use capture has no persisted image and summary')
                content = '\n'.join(summaries)
            if not isinstance(content, str) or not content.strip():
                raise ValueError('Computer Use result has no persisted observation')
            try:
                result = json.loads(content)
            except (TypeError, json.JSONDecodeError):
                result = None
            if (isinstance(result, dict) and result.get('error') or
                    content.lstrip().casefold().startswith(('error', 'tool blocked', 'operation interrupted'))):
                raise ValueError('Computer Use result failed')
            assistant = conn.execute(
                "SELECT tool_calls FROM messages WHERE session_id=? AND role='assistant' "
                'AND id<? ORDER BY id DESC LIMIT 1',
                (row[1], message_id)).fetchone()
            try:
                tool_calls = json.loads(assistant[0]) if assistant and assistant[0] else []
            except (TypeError, json.JSONDecodeError):
                tool_calls = []
            call = next((call for call in tool_calls if isinstance(call, dict) and
                         (call.get('id') == row[4] or call.get('call_id') == row[4])), None)
            if not call or (call.get('function') or {}).get('name', call.get('name')) != 'computer_use':
                raise ValueError('Computer Use result has no matching native call')
            raw_args = (call.get('function') or {}).get('arguments', call.get('arguments'))
            try:
                args = json.loads(raw_args) if isinstance(raw_args, str) else raw_args
            except (TypeError, json.JSONDecodeError):
                args = None
            if not isinstance(args, dict):
                raise ValueError('Computer Use call arguments unavailable')
            calls[message_id] = (row, args, content)
    for check, (action_id, capture_id) in zip(checks, refs):
        action, action_args, _ = calls[action_id]
        capture, capture_args, capture_content = calls[capture_id]
        if (action_id >= capture_id or action[1] != capture[1] or action[6] > capture[6] or
                action_args.get('action') in (None, '', 'capture', 'wait', 'list_apps', 'list_windows') or
                capture_args.get('action') != 'capture' or
                not (capture_content.startswith('capture mode=') or
                     (capture_content.startswith('{') and '"summary"' in capture_content))):
            raise ValueError('each automatic check needs a UI action followed by a native capture')
    step_actions = {}
    for check in checks:
        step = (check['action'], check['expected'])
        previous = step_actions.setdefault(step, check['action_message_id'])
        if previous != check['action_message_id']:
            raise ValueError('one test-card step cannot cite different Computer Use actions')
    if len(set(step_actions.values())) < len(candidate['steps']):
        raise ValueError('distinct test-card steps need distinct Computer Use actions')


def current_correction_findings(item, row, contract):
    """Read the current failed assessment and bind it to its actual source."""
    report = read_evidence(item['correction'], contract)
    result = row['worker_result']
    findings = report.get('findings')
    if (report.get('result') != 'failed' or not isinstance(findings, list) or
            not findings or any(not isinstance(value, str) or not value.strip()
                                for value in findings)):
        raise ValueError('current correction requires concrete failed findings')
    if item.get('candidate') or report.get('candidate_id') is not None:
        if item.get('candidate'):
            candidate = verify_candidate(item)
        else:
            # item_return archives the rejected candidate before Coding reads
            # this correction. Bind only the most recently archived candidate;
            # an older rejection cannot authorize a new attempt.
            history = item.get('candidate_history') or []
            if item.get('phase') != 'correction_needed' or not history or not isinstance(history[-1], dict):
                raise ValueError('current correction has no bound rejected candidate')
            candidate = history[-1]
        assessment = read_evidence(candidate['assessment'], contract)
        if (candidate.get('result_sha256') != result['evidence']['sha256'] or
                assessment.get('task') != row['task'] or
                assessment.get('result_sha256') != result['evidence']['sha256'] or
                assessment.get('result') != 'passed' or not assessment.get('checks') or
                report.get('candidate_id') != candidate['id']):
            raise ValueError('current correction findings differ from bound candidate')
    elif (any(report.get(k) != row[k] for k in ('task', 'action_id', 'effect_id')) or
            report.get('worker_generation') != result['generation'] or
            report.get('result_sha256') != result['evidence']['sha256']):
        raise ValueError('current correction findings differ from bound Worker result')
    return findings


def migration_facts(guard, identity, bid, issue):
    """Read a legacy item and prove that its last writer cannot still start."""
    from hermes_cli import diggr_continuation as dc
    with guard.transaction(read_only=True) as rows:
        batch, contract, item = live_item(rows, identity, bid, issue)
        row = rows.get(item.get('task'))
        if (batch.get('flow_version') == 3 or not row or
                batch.get('tasks') != {issue: row['task']} or
                item.get('phase') not in {'coding', 'correction_needed'} or
                item.get('candidate') or row.get('direct_worker') or
                row.get('status') not in {'blocked', 'paused', 'superseded'} or
                not row.get('policy', {}).get('item_lifecycle') or
                any(row.get(field) is not None for field in
                    ('hard_stop', 'authorization_expires_at')) or
                item.get('work_seconds', 0) >= item['budget']['todo_seconds'] or
                row['policy'].get('corrections', 0) >= item['budget']['corrections'] or
                not batch.get('native_transport')):
            raise ValueError('migration requires one stopped legacy item with remaining authority')
        for attempt in [row] + row.get('history', []) + row.get('attempt_history', []):
            if attempt.get('visible_sent') and not isinstance(attempt.get('worker_result'), dict):
                raise ValueError('migration blocked by unknown prior send effect')
        before = copy.deepcopy(row)
        prior_item = copy.deepcopy(item)
        contract_hash = owner.binding_hash(contract)
    dc.verify_restarted_reservation_exit(before)
    actual, branch, head, status = dc._git_route_identity(prior_item['worktree'])
    if actual != prior_item['worktree'] or branch != prior_item['branch']:
        raise ValueError('migration worktree or branch changed')
    dc.validate_logical_changes(before)
    retained = dc._worktree_snapshot(actual)
    diff_stat = git(actual, 'diff', '--stat', 'HEAD')
    core = dict(batch=bid, issue=issue, contract_sha256=contract_hash,
                row_sha256=dc.object_hash(before),
                item_sha256=owner.binding_hash({k: v for k, v in prior_item.items()
                                                if k != 'migration_preview'}),
                worktree=actual, branch=branch, head=head,
                retained_worktree_sha256=retained,
                worktree_status=status, diff_stat=diff_stat,
                owner=contract['owner'], contract_scope=contract['scope'],
                actions=copy.deepcopy(contract['actions']),
                model=copy.deepcopy(contract['model']),
                work_seconds=prior_item['work_seconds'],
                remaining_work_seconds=prior_item['budget']['todo_seconds'] - prior_item['work_seconds'],
                budget=copy.deepcopy(prior_item['budget']),
                corrections=before['policy'].get('corrections', 0),
                recoveries=before['policy'].get('recoveries', 0),
                wakes=before['policy'].get('wakes', 0),
                prior_coordination_deadline=before.get('deadline'),
                assignment_window_seconds=600,
                acceptance=copy.deepcopy(contract['acceptance']),
                scope=copy.deepcopy(contract['resources']))
    return core, before, prior_item


def migration_preview(guard, identity, payload):
    """Bind an Owner-visible one-time switch, without touching execution."""
    bid, issue = payload['batch'], payload['issue']
    core, row, item = migration_facts(guard, identity, bid, issue)
    digest = owner.binding_hash(core)
    with guard.transaction() as rows:
        batch, _, active = live_item(rows, identity, bid, issue)
        if (rows.get(row['task']) != row or active != item or batch.get('flow_version') == 3):
            raise ValueError('migration changed during preview; read status again')
        active['migration_preview'] = dict(core=core, sha256=digest,
                                           created_at=time.time(), shown=None)
    return dict(sha256=digest, batch=bid, issue=issue, core=core,
                next_owner_command='/continuation migrate-show ' + digest)


def migration_rollback_facts(guard, identity, bid, issue):
    """A rollback is safe only before native assignment delivery or new work."""
    from hermes_cli import diggr_continuation as dc
    with guard.transaction(read_only=True) as rows:
        batch, contract, item = live_item(rows, identity, bid, issue)
        migration = item.get('migration') or {}
        assignment = item.get('coding_assignment') or {}
        old = rows.get(migration.get('old_task'))
        unsent = (assignment.get('status') == 'pending' or
                  (assignment.get('status') == 'blocked' and
                   assignment.get('reason') == 'confirmed assignment deadline expired'))
        if (batch.get('flow_version') != 3 or batch.get('tasks') or
                item.get('phase') != 'prepared' or item.get('task') or
                not unsent or any(assignment.get(key) for key in
                                  ('binding', 'offered_at', 'session', 'accepted_at', 'task')) or
                not old or not dc.migration_fence_matches(old)):
            raise ValueError('rollback requires an undelivered migration assignment and fenced old row')
        before = copy.deepcopy(old)
        prior_item = copy.deepcopy(item)
    actual, branch, head, status = dc._git_route_identity(prior_item['worktree'])
    if (actual != prior_item['worktree'] or branch != prior_item['branch'] or
            head != migration['head'] or
            dc._worktree_snapshot(actual) != migration['retained_worktree_sha256']):
        raise ValueError('migration worktree changed; rollback requires reconciliation')
    core = dict(batch=bid, issue=issue, contract_sha256=owner.binding_hash(contract),
                old_row_sha256=dc.object_hash(before),
                item_sha256=owner.binding_hash({k: v for k, v in prior_item.items()
                                                if k != 'migration_rollback_preview'}),
                worktree=actual, branch=branch, head=head, status=status,
                retained_worktree_sha256=migration['retained_worktree_sha256'],
                migration_owner_event=migration['owner_event'],
                work_seconds=migration['work_seconds'],
                corrections=migration['corrections'],
                recoveries=migration['recoveries'], wakes=migration['wakes'])
    return core, before, prior_item


def renewal_prompt_binding(bid, item, contract, evidence):
    """Read exact source/prompt bytes; the native Owner preview supplies authority.

    Main supplies a hashed JSON descriptor through the existing evidence field.
    No Plane credentials, inferred prompt, or caller-selected contract is used.
    """
    from hermes_cli import diggr_continuation as dc
    root = Path(contract['resources']['artifacts'])

    def read(ref):
        if (not isinstance(ref, dict) or set(ref) != {'path', 'sha256'} or
                not isinstance(ref['path'], str) or not isinstance(ref['sha256'], str) or
                not re.fullmatch(r'[0-9a-f]{64}', ref['sha256']) or
                not Path(ref['path']).is_relative_to(root)):
            raise ValueError('renewal prompt evidence requires a path inside the approved artifact root')
        return dc.verified_bytes(ref['path'], ref['sha256'])

    descriptor = json.loads(read(evidence))
    expected = dict(batch=bid, issue=item['issue'], plane_id=contract['plane_id'],
                    contract_sha256=owner.binding_hash(contract),
                    **{k: item[k] for k in ('worktree', 'branch', 'base', 'target')})
    if (not isinstance(descriptor, dict) or set(descriptor) != set(expected) | {'source', 'prompt'} or
            any(descriptor.get(k) != v for k, v in expected.items())):
        raise ValueError('renewal prompt source identity or confirmed contract changed')
    source = read(descriptor['source'])
    prompt = read(descriptor['prompt'])
    if (len({evidence['path'], descriptor['source']['path'], descriptor['prompt']['path']}) != 3 or
            not source.strip() or not prompt.strip() or b'\0' in source or b'\0' in prompt):
        raise ValueError('distinct nonempty source and executor prompt required')
    source.decode('utf-8')
    return dict(descriptor, evidence=copy.deepcopy(evidence)), prompt.decode('utf-8')


def renewal_batch_snapshot(batch, bid):
    """Exclude only the native preview and this assignment's start-failure notice.

    The failure notice is emitted after acceptance without registering a worker.
    It is not part of the predecessor's Owner preview. All other batch facts stay
    bound, including older notices, history, target and budgets.
    """
    result = copy.deepcopy(batch)
    active = result['item']
    active.pop('assignment_renew_preview', None)
    reference = active.get('coding_assignment', {}).get('id')
    if 'deliveries' in result:
        result['deliveries'] = [d for d in result['deliveries'] if not (
            d.get('id') == bid + ':coding_start_unconfirmed:' + str(reference) and
            d.get('phase') == 'coding_start_unconfirmed')]
    return result


def assignment_bound_prompt(bid, batch, contract, *, prompt=None, row=None):
    """Recheck the Owner-sealed final prompt even after a task has been registered."""
    active = batch['item']
    assignment = active.get('coding_assignment') or {}
    renewals = active.get('assignment_renewals') or []
    required = (len(renewals) > 1 or assignment.get('second_renewal') or
                assignment.get('prompt_binding') or (row or {}).get('assignment_renewal_sha256'))
    if not required:
        return None
    renewal = renewals[-1] if len(renewals) == 2 else {}
    core = renewal.get('core') or {}
    binding = core.get('prompt_binding')
    if (not binding or core.get('renewal_number') != 2 or
            active.get('budget') != core.get('budget') or
            batch.get('manifest', {}).get('budgets') != core.get('budget') or
            active.get('work_seconds') != 0 or
            assignment.get('second_renewal') is not True or
            assignment.get('prompt_binding') != binding or assignment.get('rebind') or
            renewal.get('new_assignment') != assignment.get('id') or
            renewal.get('preview_sha256') != owner.binding_hash(core) or
            not renewal.get('native_owner_event')):
        raise ValueError('second-renewal prompt authorization missing or changed')
    checked, exact = renewal_prompt_binding(bid, active, contract, binding.get('evidence'))
    if checked != binding or (prompt is not None and prompt != exact):
        raise ValueError('second-renewal prompt differs from approved source bytes')
    if row is not None and (
            row.get('assignment_renewal_sha256') != owner.binding_hash(renewal) or
            row.get('assignment_prompt_sha256') != binding['prompt']['sha256'] or
            (row.get('policy') is not None and (
                row['policy'].get('work_limit') != core['budget']['todo_seconds'] or
                row['policy'].get('work_seconds') != 0)) or
            row.get('worker_route', {}).get('argv', [None])[-1] != exact):
        raise ValueError('registered second-renewal prompt or Owner binding changed')
    return exact


def second_renewal_core(rows, bid, batch, contract, evidence):
    """Only an expired, accepted first renewal of the recovered APP-112 chain."""
    from hermes_cli import diggr_continuation as dc, diggr_delivery as delivery
    active = batch['item']
    assignment = active.get('coding_assignment') or {}
    renewals = active.get('assignment_renewals') or []
    deadline = dc.coding_assignment_deadline(batch)
    effect_fields = ('task', 'worker_route', 'worker_pid', 'worker_identity', 'worker_result',
                     'worker_descendants', 'child_identity', 'claim_phase', 'claimed',
                     'visible_sent', 'visible_sent_at', 'send_request_sha256', 'effect_status')
    if any(entry.get(key) is not None
           for entry in [active, assignment, *(active.get('coding_assignment_history') or [])]
           for key in effect_fields):
        raise ValueError('second renewal has registered, claimed or unknown worker effect')
    if any(row.get('worker_route', {}).get('worktree') == active['worktree'] or
           row.get('worker_route', {}).get('visible_target') == active['target'] for row in rows.values()):
        raise ValueError('second renewal has a competing registered route')
    if (contract['plane_id'] != 'APP-112' or batch.get('revoked') or
            batch.get('flow_version') != 3 or active.get('migration') or
            len(renewals) != 1 or len(active.get('coding_assignment_history') or []) != 2 or
            assignment.get('status') != 'accepted' or assignment.get('rebind') or
            not isinstance(deadline, (int, float)) or not math.isfinite(deadline) or
            time.time() < deadline or
            not delivery.recovery_renewal_effect_free(
                rows, bid, renewal_batch_snapshot(batch, bid), contract, assignment.get('id'), sent=True)):
        raise ValueError('second and final renewal requires an expired effect-free first APP-112 renewal')
    binding = assignment.get('binding') or {}
    # Acceptance belongs to the Coding coordinator, not to an APP executor.
    # The older uncertain process still requires exit proof above. A retained
    # live coordinator must be exactly the independently resolved native lane;
    # all task/claim/effect guards and the idle executor check remain mandatory.
    if ((not dc.process_absent(binding.get('process', {}).get('identity'), original_process_only=True) and
         dc.preflight_coding_coordinator() != binding) or not worktree_matches(contract, active)):
        raise ValueError('second renewal predecessor process or worktree is unresolved')
    dc.verify_visible_target(dc.bind_visible_target(active['target']), idle=True)
    prompt_binding, prompt = renewal_prompt_binding(bid, active, contract, evidence)
    if onboarding_payload()[0] not in prompt or prompt.startswith('---'):
        raise ValueError('second-renewal executor prompt requires complete onboarding under a normal heading')
    snapshot = renewal_batch_snapshot(batch, bid)
    return dict(batch=bid, issue=active['issue'], renewal_number=2,
                contract_sha256=owner.binding_hash(contract),
                batch_sha256=owner.binding_hash(snapshot),
                assignment_sha256=owner.binding_hash(assignment),
                item_sha256=owner.binding_hash(snapshot['item']),
                expired_at=deadline, assignment_window_seconds=600,
                remaining_work_seconds=active['budget']['todo_seconds'],
                budget=copy.deepcopy(active['budget']),
                prompt_binding=prompt_binding)


def assignment_renew_facts(guard, identity, bid, issue, evidence=None):
    """Prove an expired migration or recovered preparation assignment had no work."""
    from hermes_cli import diggr_continuation as dc
    with guard.transaction(read_only=True) as rows:
        batch, contract, item = live_item(rows, identity, bid, issue)
        assignment = item.get('coding_assignment') or {}
        migration = item.get('migration') or {}
        recovery = item.get('preparation_assignment_recovery') or {}
        old = rows.get(migration.get('old_task')) if migration else None
        if item.get('assignment_renewals') and not migration:
            core = second_renewal_core(rows, bid, batch, contract, evidence)
            return core, copy.deepcopy(item), None, copy.deepcopy(batch.get('native_transport'))
        if (batch.get('flow_version') != 3 or batch.get('tasks') != {} or
                item.get('phase') != 'prepared' or item.get('task') is not None or
                item.get('candidate') or item.get('candidate_history') or
                item.get('preparation_blocker') or
                item.get('assignment_renewals') or
                assignment.get('task') is not None or
                not isinstance(assignment.get('binding'), dict) or
                time.time() < dc.coding_assignment_deadline(batch)):
            raise ValueError('renewal requires one expired assignment with no registered worker')
        if migration:
            if (assignment.get('status') != 'accepted' or
                    not assignment.get('accepted_at') or not assignment.get('session') or
                    recovery or not old or not dc.migration_fence_matches(old) or
                    any(row.get('direct_worker') is True and row.get('owner_batch') == bid
                        for row in rows.values())):
                raise ValueError('migration renewal requires the fenced old attempt')
        else:
            history = item.get('coding_assignment_history') or []
            core = recovery.get('core') or {}
            accepted = (assignment.get('status') == 'accepted' and
                        bool(assignment.get('accepted_at')) and bool(assignment.get('session')))
            # Native acceptance is the only entry to worker registration. An
            # An uncertain send has no accepted intake, but its bound process
            # must also be proven gone before another offer can be authorized.
            unaccepted = (assignment.get('status') == 'uncertain' and
                          assignment.get('accepted_at') is None and
                          assignment.get('session') is None and
                          type(assignment.get('offered_at')) in (int, float) and
                          recovery.get('created_at', float('inf')) <= assignment['offered_at'] <
                          recovery.get('assignment_deadline', float('-inf')))
            if (not (accepted or unaccepted) or not recovery or len(history) != 1 or
                    any(row.get('owner_batch') == bid or (row.get('policy') or {}).get('batch') == bid
                        for row in rows.values()) or
                    batch.get('started_at') is not None or batch.get('deadline') is not None or
                    item.get('work_seconds') != 0 or
                    item.get('budget') != batch.get('manifest', {}).get('budgets') or
                    batch.get('recovered_preparation_blockers') != [recovery] or
                    recovery.get('core_sha256') != owner.binding_hash(core) or
                    recovery.get('new_assignment') != assignment.get('id') or
                    assignment.get('preparation_recovery_of') != recovery.get('old_assignment') or
                    history[0].get('id') != recovery.get('old_assignment') or
                    history[0].get('status') != 'blocked' or
                    history[0].get('reason') != 'confirmed assignment deadline expired' or
                    any(history[0].get(key) is not None for key in
                        ('binding', 'offered_at', 'session', 'accepted_at', 'task')) or
                    core.get('batch') != bid or core.get('issue') != issue or
                    core.get('contract_sha256') != owner.binding_hash(contract) or
                    core.get('assignment_sha256') != owner.binding_hash(history[0]) or
                    core.get('preparation_delivery_sha256') != owner.binding_hash(
                        batch.get('preparation_delivery')) or
                    core.get('worktree') != item.get('worktree') or
                    core.get('branch') != item.get('branch') or
                    core.get('base') != item.get('base') or
                    core.get('work_seconds') != 0 or
                    recovery.get('assignment_deadline') != dc.coding_assignment_deadline(batch) or
                    (accepted and (assignment['accepted_at'] < recovery.get('created_at', float('inf')) or
                                   assignment['accepted_at'] >= recovery['assignment_deadline']))):
                raise ValueError('renewal requires the unchanged effect-free preparation recovery')
        before = copy.deepcopy(item)
        prior = copy.deepcopy(old) if old else None
        contract_hash = owner.binding_hash(contract)
        transport = copy.deepcopy(batch.get('native_transport'))
        batch_hash = owner.binding_hash(batch) if recovery else None
    if assignment['status'] == 'accepted':
        process = assignment['binding'].get('process', {}).get('identity')
        if not dc.process_absent(process, original_process_only=True):
            raise ValueError('accepted Coding process still active or exit unproved')
    elif recovery and assignment['status'] == 'uncertain':
        process = assignment['binding'].get('process', {}).get('identity')
        if not dc.process_absent(process, original_process_only=True):
            raise ValueError('previous Coding process still active or exit unproved')
    if migration:
        actual, branch, head, _ = dc._git_route_identity(item['worktree'])
        if (actual != item['worktree'] or branch != item['branch'] or
                head != migration['head'] or
                dc._worktree_snapshot(actual) != migration['retained_worktree_sha256']):
            raise ValueError('retained migration worktree changed')
    elif not worktree_matches(contract, item):
        raise ValueError('recovered preparation worktree changed')
    core = dict(batch=bid, issue=issue, contract_sha256=contract_hash,
                assignment_sha256=owner.binding_hash(assignment),
                expired_at=dc.coding_assignment_deadline(batch),
                item_sha256=owner.binding_hash(before),
                remaining_work_seconds=item['budget']['todo_seconds'] - item['work_seconds'],
                assignment_window_seconds=600)
    if migration:
        core.update(old_row_sha256=dc.object_hash(prior),
                    retained_worktree_sha256=migration['retained_worktree_sha256'])
    else:
        core.update(batch_sha256=batch_hash, recovery_sha256=owner.binding_hash(recovery),
                    worktree=item['worktree'], branch=item['branch'], base=item['base'])
    if core['remaining_work_seconds'] <= 0 or not transport:
        raise ValueError('remaining item budget and native Owner transport required')
    return core, before, prior, transport


async def owner_assignment_renew(runner, event, guard, identity, event_id, words):
    """A new direct Owner message opens one fresh technical assignment window."""
    from hermes_cli.diggr_delivery import command_transport, same_transport
    if len(words) != 4 or words[0:2] != ['/continuation', 'assignment-renew']:
        raise ValueError('use /continuation assignment-renew BATCH PREVIEW_HASH')
    bid, expected = words[2:]
    with guard.transaction(read_only=True) as rows:
        batch = rows.grants.get('batches', {}).get(bid, {})
        issue = batch.get('item', {}).get('issue')
        second = bool(batch.get('item', {}).get('assignment_renewals')) and not batch.get('item', {}).get('migration')
    if second:
        return await owner_second_assignment_renew(runner, event, guard, identity, event_id, bid, issue, expected)
    core, before, prior, bound_transport = assignment_renew_facts(guard, identity, bid, issue)
    if owner.binding_hash(core) != expected:
        raise ValueError('assignment renewal preview changed; read status again')
    transport = command_transport(runner, event, identity)
    if not transport or not same_transport(bound_transport, transport):
        raise ValueError('assignment renewal requires the original native Owner transport')
    with guard.transaction() as rows:
        batch, contract, active = live_item(rows, identity, bid, issue)
        if (active != before or (prior is not None and rows.get(prior['task']) != prior) or
                batch.get('tasks') != {} or batch.get('revoked') or
                batch.get('native_transport') != bound_transport or
                owner.binding_hash(contract) != core['contract_sha256'] or
                (prior is None and owner.binding_hash(batch) != core['batch_sha256']) or
                (prior is None and any(
                    row.get('owner_batch') == bid or (row.get('policy') or {}).get('batch') == bid
                    for row in rows.values()))):
            raise ValueError('assignment changed before native renewal')
        from hermes_cli import diggr_continuation as dc
        if prior is not None:
            actual, branch, head, _ = dc._git_route_identity(active['worktree'])
            if (actual != active['worktree'] or branch != active['branch'] or
                    head != active['migration']['head'] or
                    dc._worktree_snapshot(actual) !=
                    active['migration']['retained_worktree_sha256']):
                raise ValueError('retained migration worktree changed before renewal')
        elif not worktree_matches(contract, active):
            raise ValueError('recovered preparation worktree changed before renewal')
        process = active['coding_assignment']['binding'].get('process', {}).get('identity')
        if not dc.process_absent(process, original_process_only=True):
            raise ValueError('previous Coding process still active or exit unproved before renewal')
        old_assignment = copy.deepcopy(active['coding_assignment'])
        active.setdefault('coding_assignment_history', []).append(old_assignment)
        active['coding_assignment'] = dict(id=secrets.token_hex(24), status='pending',
                                           created_at=time.time())
        if prior is not None:
            active['migration']['assignment_deadline'] = time.time() + 600
        active['assignment_renewals'] = [dict(native_owner_event=event_id,
                                             preview_sha256=expected,
                                             old_assignment=old_assignment['id'],
                                             new_assignment=active['coding_assignment']['id'],
                                             assignment_deadline=time.time() + 600,
                                             at=time.time())]
    from gateway.platforms.base import _thread_metadata_for_source
    sent = await runner._adapter_for_source(event.source).send(
        event.source.chat_id, before['plane_id'] + ': Zuweisungsfenster für dasselbe Item einmalig erneuert; '
        'alter Start bleibt gesperrt, kein Worker gestartet.',
        metadata=_thread_metadata_for_source(event.source, str(event.message_id)))
    if not getattr(sent, 'success', False):
        raise ValueError('assignment renewal persisted; notification failed, read item status')
    return False


async def owner_second_assignment_renew(runner, event, guard, identity, event_id, bid, issue, expected):
    from hermes_cli.diggr_delivery import command_transport, same_transport
    if owner.verify_input(event, identity) != event_id:
        raise ValueError('fresh direct native Owner event required')
    transport = command_transport(runner, event, identity)
    with guard.transaction() as rows:
        batch, contract, active = live_item(rows, identity, bid, issue)
        preview = active.get('assignment_renew_preview') or {}
        core = preview.get('core') or {}
        renewals = active.get('assignment_renewals') or []
        if (not transport or not same_transport(batch.get('native_transport'), transport) or
                len(renewals) != 1 or event_id in {batch.get('confirmation_event'), renewals[0].get('native_owner_event')} or
                preview.get('sha256') != expected or owner.binding_hash(core) != expected or
                not core.get('preview_nonce') or type(core.get('preview_created_at')) not in (int, float) or
                not core['preview_created_at'] <= time.time() < core['preview_created_at'] + 300):
            raise ValueError('fresh second-renewal Owner preview and original transport required')
        current = second_renewal_core(rows, bid, batch, contract,
                                      core.get('prompt_binding', {}).get('evidence'))
        if dict(current, preview_nonce=core['preview_nonce'],
                preview_created_at=core['preview_created_at']) != core:
            raise ValueError('second-renewal preview changed before Owner confirmation')
        now = time.time()
        if now >= core['preview_created_at'] + 300:
            raise ValueError('second-renewal preview expired during verification')
        previous = copy.deepcopy(active['coding_assignment'])
        active['coding_assignment_history'].append(previous)
        active['coding_assignment'] = dict(id=secrets.token_hex(24), status='pending', created_at=now,
                                           second_renewal=True, prompt_binding=copy.deepcopy(core['prompt_binding']))
        renewals.append(dict(native_owner_event=event_id, preview_sha256=expected,
                             core=copy.deepcopy(core), old_assignment=previous['id'],
                             new_assignment=active['coding_assignment']['id'],
                             assignment_deadline=now + 600, at=now))
        active.pop('assignment_renew_preview')
    from gateway.platforms.base import _thread_metadata_for_source
    sent = await runner._adapter_for_source(event.source).send(
        event.source.chat_id, contract['plane_id'] + ': zweite und letzte Zuweisung bestätigt; kein Worker gestartet.',
        metadata=_thread_metadata_for_source(event.source, str(event.message_id)))
    if not getattr(sent, 'success', False):
        raise ValueError('second renewal persisted; notification failed, read item status')
    return False


def preparation_assignment_recovery_facts(guard, identity, bid, issue, evidence):
    """Prove that one blocked Flow-v3 preparation assignment had no effect."""
    from hermes_cli import diggr_continuation as dc

    with guard.transaction(read_only=True) as rows:
        batch, contract, item = live_item(rows, identity, bid, issue)
        assignment = item.get('coding_assignment') or {}
        blocker = item.get('preparation_blocker') or {}
        delivery = batch.get('preparation_delivery') or {}
        flow3_source = bid
        if batch.get('flow_version') != 3:
            transfer = batch.get('preparation_transfer') or {}
            predecessor = rows.grants.get('batches', {}).get(transfer.get('from_batch'), {})
            prior_item = predecessor.get('item') or {}
            if (transfer.get('to_batch') != bid or batch.get('replaces') != transfer.get('from_batch') or
                    predecessor.get('flow_version') != 3 or not predecessor.get('revoked') or
                    predecessor.get('replaced_by') != bid or prior_item.get('phase') != 'archived' or
                    prior_item.get('transferred_to') != bid or prior_item.get('issue') != issue or
                    prior_item.get('worktree') != item.get('worktree') or
                    prior_item.get('branch') != item.get('branch') or prior_item.get('base') != item.get('base') or
                    prior_item.get('work_seconds') != 0 or
                    prior_item.get('coding_assignment', {}).get('id') != assignment.get('id')):
                raise ValueError('preparation assignment recovery requires a native Flow-v3 reservation')
            flow3_source = transfer['from_batch']
        if (batch.get('tasks') != {} or
                batch.get('started_at') is not None or batch.get('deadline') is not None or
                item.get('phase') != 'prepared' or item.get('task') is not None or
                item.get('work_seconds') != 0 or item.get('candidate') or
                assignment.get('status') != 'blocked' or
                assignment.get('reason') != 'confirmed assignment deadline expired' or
                any(assignment.get(key) is not None for key in
                    ('binding', 'offered_at', 'session', 'accepted_at', 'task')) or
                blocker.get('evidence') != evidence or
                delivery.get('status') not in {'accepted', 'response_produced'} or
                type(delivery.get('accepted_at')) not in (int, float) or
                type(delivery.get('deadline')) not in (int, float) or
                blocker.get('at', 0) < delivery['deadline'] or
                time.time() < delivery['deadline'] or
                item.get('preparation_assignment_recovery') or
                any(row.get('owner_batch') == bid or row.get('policy', {}).get('batch') == bid
                    for row in rows.values())):
            raise ValueError('preparation assignment recovery requires one expired unstarted Flow-v3 reservation')
        transport = batch.get('native_transport') or {}
        if any(transport.get(k) != identity.get(v) for k, v in (
                ('platform', 'platform'), ('chat_id', 'chat_id'), ('user_id', 'user_id'),
                ('session_key', 'session_key'), ('parent_session_id', 'session'))):
            raise ValueError('preparation assignment recovery differs from confirmed native transport')
        before = copy.deepcopy(batch)
        contract_sha256 = owner.binding_hash(contract)

    report = read_evidence(evidence, contract)
    observations = report.get('observations')
    if (not isinstance(report, dict) or report.get('batch') != bid or
            report.get('issue') != issue or report.get('reason') != blocker.get('reason') or
            not isinstance(observations, list) or not observations or
            any(not isinstance(observation, str) or not observation.strip()
                for observation in observations)):
        raise ValueError('preparation recovery blocker evidence differs from the reserved item')
    if (not any(observation.startswith('No message after cursor ') for observation in observations) or
            not any('did not exist and no ' in observation and ' preparation artifacts were found'
                    in observation for observation in observations)):
        raise ValueError('preparation recovery requires bound no-response and no-artifact evidence')
    coordinator = [re.fullmatch(
        r'Coordinator PID ([1-9][0-9]*) is invoked with --profile diggr-coding but its effective '
        r'HERMES_HOME is \S+ and it is bound to the old Main Telegram session \S+', observation)
        for observation in observations]
    coordinator_pids = [int(match.group(1)) for match in coordinator if match]
    if len(coordinator_pids) != 1 or not dc.pid_is_absent(coordinator_pids[0]):
        raise ValueError('old preparation Coordinator exit is not proven')
    if not worktree_matches(contract, before['item']):
        raise ValueError('preparation assignment recovery requires the unchanged clean registered worktree')
    core = dict(batch=bid, issue=issue, contract_sha256=contract_sha256,
                item_sha256=owner.binding_hash(before['item']),
                assignment_sha256=owner.binding_hash(assignment),
                preparation_delivery_sha256=owner.binding_hash(delivery),
                blocker_evidence=copy.deepcopy(evidence), coordinator_pid=coordinator_pids[0],
                worktree=before['item']['worktree'], branch=before['item']['branch'],
                base=before['item']['base'], work_seconds=0, assignment_window_seconds=600,
                flow3_source_batch=flow3_source)
    return core, before


def recover_preparation_assignment(guard, identity, payload):
    """Create one fresh assignment for the same proven-unused reservation."""
    from hermes_cli import diggr_continuation as dc

    context = dc.EVENT_CONTEXT.get()
    if (not context or context.get('identity') != identity or
            context.get('native_wake') is not None or
            not context.get('direct_owner_event') or
            identity.get('platform') != 'telegram' or
            identity.get('chat_id') != owner.OWNER or identity.get('user_id') != owner.OWNER):
        raise ValueError('direct original Owner/Main context required for preparation assignment recovery')
    with guard.transaction(read_only=True) as rows:
        batch, _, item = live_item(rows, identity, payload['batch'], payload['issue'])
        prior = item.get('preparation_assignment_recovery')
        if prior:
            if prior.get('evidence') != payload['evidence']:
                raise ValueError('preparation assignment was already recovered with different evidence')
            return copy.deepcopy(prior)
    core, before = preparation_assignment_recovery_facts(
        guard, identity, payload['batch'], payload['issue'], payload['evidence'])
    with guard.transaction() as rows:
        batch, contract, item = live_item(rows, identity, payload['batch'], payload['issue'])
        if batch != before or owner.binding_hash(contract) != core['contract_sha256']:
            raise ValueError('preparation reservation changed before assignment recovery')
        if not worktree_matches(contract, item):
            raise ValueError('prepared worktree changed before assignment recovery')
        old_assignment = copy.deepcopy(item['coding_assignment'])
        blocker = item.pop('preparation_blocker')
        created_at = time.time()
        new_assignment = dict(id=secrets.token_hex(24), status='pending', created_at=created_at,
                              preparation_recovery_of=old_assignment['id'])
        receipt = dict(evidence=copy.deepcopy(payload['evidence']), core=core,
                       core_sha256=owner.binding_hash(core), blocker=blocker,
                       old_assignment=old_assignment['id'], new_assignment=new_assignment['id'],
                       assignment_deadline=created_at + 600, created_at=created_at)
        item.setdefault('coding_assignment_history', []).append(old_assignment)
        item['coding_assignment'] = new_assignment
        item['preparation_assignment_recovery'] = receipt
        batch['flow_version'] = 3
        batch.setdefault('recovered_preparation_blockers', []).append(copy.deepcopy(receipt))
        if (item['budget'] != before['item']['budget'] or
                item['work_seconds'] != 0 or batch['manifest']['budgets'] != before['manifest']['budgets'] or
                batch['tasks'] != {} or item['worktree'] != before['item']['worktree'] or
                item['branch'] != before['item']['branch'] or item['base'] != before['item']['base']):
            raise ValueError('preparation assignment recovery changed reservation or budget')
        return copy.deepcopy(receipt)


def migration_rollback_preview(guard, identity, payload):
    bid, issue = payload['batch'], payload['issue']
    core, old, item = migration_rollback_facts(guard, identity, bid, issue)
    digest = owner.binding_hash(core)
    with guard.transaction() as rows:
        batch, _, active = live_item(rows, identity, bid, issue)
        if active != item or rows.get(old['task']) != old or batch.get('tasks'):
            raise ValueError('migration changed during rollback preview')
        active['migration_rollback_preview'] = dict(core=core, sha256=digest,
                                                    created_at=time.time(), shown=None)
    return dict(sha256=digest, batch=bid, issue=issue, core=core,
                next_owner_command='/continuation migrate-rollback-show ' + digest)


async def owner_migration_command(runner, event, guard, identity, event_id, words):
    """Existing native Owner ingress supplies the one-use migration decision."""
    from hermes_cli import diggr_continuation as dc
    from hermes_cli.diggr_delivery import command_transport, same_transport
    rollback = words[1] in {'migrate-rollback-show', 'migrate-rollback-confirm'}
    if len(words) not in {3, 4} or words[1] not in {'migrate-show', 'migrate-confirm',
                                                    'migrate-rollback-show', 'migrate-rollback-confirm'}:
        raise ValueError('use a migration show HASH then confirm HASH CODE')
    digest = words[2]
    transport = command_transport(runner, event, identity)
    if not transport:
        raise ValueError('authenticated native Owner transport required')
    preview_key = 'migration_rollback_preview' if rollback else 'migration_preview'
    with guard.transaction(read_only=True) as rows:
        matches = [(bid, b['item']['issue'], copy.deepcopy(b['item'][preview_key]))
                   for bid, b in rows.grants.get('batches', {}).items()
                   if b.get('coordinator_identity') == identity and
                   b.get('item', {}).get(preview_key, {}).get('sha256') == digest]
    if len(matches) != 1:
        raise ValueError('unknown or superseded migration preview')
    bid, issue, preview = matches[0]
    with guard.transaction(read_only=True) as rows:
        if not same_transport(rows.grants['batches'][bid].get('native_transport'), transport):
            raise ValueError('migration must use the confirmed Owner transport')
    core, row, item = (migration_rollback_facts if rollback else migration_facts)(
        guard, identity, bid, issue)
    if owner.binding_hash(core) != digest or preview['core'] != core:
        raise ValueError('migration state changed; create a fresh preview')
    now = time.time()
    if now >= preview['created_at'] + 300:
        raise ValueError('migration preview expired; inspect current status again')
    if words[1] in {'migrate-show', 'migrate-rollback-show'}:
        if len(words) != 3:
            raise ValueError('migrate-show takes exactly the preview hash')
        shown = dict(code=secrets.token_hex(12), event=event_id, expires=now + 300,
                     identity=identity, transport=transport)
        with guard.transaction() as rows:
            active = rows.grants['batches'][bid]['item']
            if active.get(preview_key) != preview or rows.get(row['task']) != row:
                raise ValueError('migration preview changed before display')
            active[preview_key]['shown'] = shown
        response = (('Rollback vor Zustellung: alter gestoppter Pfad wird wieder aktiv; '
                     'kein Worker und keine Budgeterweiterung.\n') if rollback else
                    ('Migration: vorhandenen Diff und Verbrauch behalten; alter Start wird gesperrt. '
                    'Persönlicher Teststopp bleibt bestehen. Die technische Koordinationsfrist '
                    'wird für das verbleibende Arbeitsbudget neu geöffnet; die Zuweisung gilt '
                    '600 Sekunden ab Bestätigung. Explizite harte Stops sind ausgeschlossen. '
                    'Keine Budget-, Scope-, '
                    'Modell- oder Publish-Erweiterung.\n'))
        response = (response
                    + owner.canonical(core) + '\nSHA256 ' + digest +
                    '\nMit neuer direkter Nachricht bestätigen: /continuation ' +
                    ('migrate-rollback-confirm ' if rollback else 'migrate-confirm ')
                    + digest + ' ' + shown['code'])
    else:
        if len(words) != 4 or not preview.get('shown'):
            raise ValueError('migration requires a shown preview and one-use code')
        shown = preview['shown']
        if (words[3] != shown['code'] or now >= shown['expires'] or
                event_id == shown['event'] or shown['identity'] != identity or
                not same_transport(shown['transport'], transport)):
            raise ValueError('migration confirmation differs from the fresh native preview')
        with guard.transaction() as rows:
            batch, _, active = live_item(rows, identity, bid, issue)
            current = rows.get(row['task'])
            if (active != item or current != row or
                    (batch.get('flow_version') == 3) != rollback):
                raise ValueError('migration changed before the atomic switch')
            if rollback:
                migration = active['migration']
                assignment = active['coding_assignment']
                if (assignment.get('status') not in {'pending', 'blocked'} or
                        (assignment.get('status') == 'blocked' and
                         assignment.get('reason') != 'confirmed assignment deadline expired') or
                        any(assignment.get(key) for key in
                            ('binding', 'offered_at', 'session', 'accepted_at', 'task')) or
                        batch.get('tasks') or
                        not dc.migration_fence_matches(current)):
                    raise ValueError('rollback assignment was delivered or old fence changed')
                current.pop('migration_fence')
                current.update(status=migration['old_status'], gate=migration['old_gate'])
                if migration['old_reason_present']:
                    current['reason'] = migration['old_reason']
                else:
                    current.pop('reason', None)
                batch['tasks'] = {issue: current['task']}
                batch.pop('flow_version', None)
                active.pop('migration_rollback_preview', None)
                active.pop('coding_assignment', None)
                active.pop('migration', None)
                active.update(phase=migration['old_phase'], task=current['task'],
                              base=migration['old_base'])
            else:
                old_task = current['task']
                old_status, old_gate, old_reason = (current['status'], current['gate'],
                                                    current.get('reason'))
                old_reason_present = 'reason' in current
                old_phase = active['phase']
                current.update(status='superseded', gate='migrated',
                           reason='native Owner migration fenced old start',
                           generation=current['generation'] + 1)
                for delivery_slot in (current.get('coordinator_delivery'),
                                  batch.get('preparation_delivery'), batch.get('coordinator_delivery')):
                    if isinstance(delivery_slot, dict) and delivery_slot.get('status') in {'pending', 'queued'}:
                        delivery_slot['status'] = 'suppressed'
                for queued in batch.get('deliveries', []):
                    if queued.get('status') in {'pending', 'queued'}:
                        queued['status'] = 'suppressed'
                current['migration_fence'] = dict(owner_event=event_id, preview_sha256=digest,
                                               row_sha256=dc.migration_fence_core(current))
                batch['tasks'] = {}
                batch['flow_version'] = 3
                active.pop('migration_preview', None)
                active.update(phase='prepared', task=None, base=core['head'],
                          coding_assignment=dict(id=secrets.token_hex(24),
                                                 status='pending', created_at=now),
                          migration=dict(owner_event=event_id, preview_sha256=digest,
                                         assignment_deadline=now + 600,
                                         old_task=old_task, old_generation=row['generation'],
                                         old_status=old_status, old_gate=old_gate,
                                         old_reason=old_reason,
                                         old_reason_present=old_reason_present,
                                         old_phase=old_phase,
                                         old_base=item['base'], head=core['head'],
                                         retained_worktree_sha256=core['retained_worktree_sha256'],
                                         work_seconds=core['work_seconds'],
                                         corrections=core['corrections'],
                                         recoveries=core['recoveries'], wakes=core['wakes']))
        response = ('Rollback gespeichert: alter gestoppter Pfad wieder aktiv; keine neue Ausführung.'
                    if rollback else
                    'Migration gespeichert: alter Start gesperrt, vorhandener Worktree und Verbrauch '
                    'erhalten. Noch kein Worker gestartet; Coding erhält dieselbe gebundene Zuweisung.')
    from gateway.platforms.base import _thread_metadata_for_source
    sent = await runner._adapter_for_source(event.source).send(event.source.chat_id, response,
        metadata=_thread_metadata_for_source(event.source, str(event.message_id)))
    if not getattr(sent, 'success', False):
        raise ValueError('migration decision persisted; notification failed, read item status')
    return False


def flow_assessment(guard, identity, payload, verdict):
    """Persist Main's short judgment with native result/candidate bindings."""
    details = payload.get('checks' if verdict == 'passed' else 'findings')
    if (not isinstance(details, list) or not details or
            any(not isinstance(value, str) or not value.strip() for value in details)):
        raise ValueError('concrete Main checks or findings required')
    with guard.transaction(read_only=True) as rows:
        batch, contract, item = live_item(rows, identity, payload['batch'], payload['issue'])
        if batch.get('flow_version') != 3 or not item.get('task'):
            raise ValueError('runtime assessment requires a new result-driven item')
        if verdict == 'failed' and item.get('phase') == 'correction_needed' and item.get('correction'):
            previous = item['correction']
            if read_evidence(previous, contract).get('findings') == details:
                return previous
            raise ValueError('correction already pending with different findings')
        row = rows[item['task']]
        result = row.get('worker_result') or {}
        if verdict == 'passed':
            if (item.get('phase') != 'coding' or result.get('artifact_status') != 'present'
                    or row.get('gate') != 'main_validation'):
                raise ValueError('completed worker result required for Main checks')
            report = dict(task=row['task'], result='passed',
                          result_sha256=result['evidence']['sha256'], checks=details)
        elif item.get('phase') in {'awaiting_test', 'accepted_candidate', 'review', 'reviewed'} and item.get('candidate'):
            report = dict(candidate_id=item['candidate']['id'], result='failed', findings=details)
            if payload.get('review_evidence'):
                read_evidence(payload['review_evidence'], contract)
                report['independent_review'] = payload['review_evidence']
        else:
            if (item.get('phase') != 'coding' or result.get('artifact_status') != 'present'
                    or row.get('gate') != 'main_validation'):
                raise ValueError('completed worker result required for Main findings')
            report = dict(task=row['task'], action_id=row['action_id'], effect_id=row['effect_id'],
                          worker_generation=result['generation'],
                          result_sha256=result['evidence']['sha256'], result='failed', findings=details)
        previous = item.get('correction') if verdict == 'failed' else None
        if previous and read_evidence(previous, contract) == report:
            return previous
        root = Path(contract['resources']['artifacts'])
    path = root / ('flow3-main-assessment-' + secrets.token_hex(12) + '.json')
    with path.open('x', encoding='utf-8') as output:
        json.dump(report, output, sort_keys=True)
        output.flush()
        os.fsync(output.fileno())
    return dict(path=str(path), sha256=hashlib.sha256(path.read_bytes()).hexdigest())


def verify_documentation_resolution(batch_id, batch, contract, item, report, identity):
    """Verify the existing Owner decision and immutable replacement sources."""
    import sqlite3
    from hermes_cli import diggr_continuation as dc

    context = dc.EVENT_CONTEXT.get()
    if (not context or context.get('identity') != identity or context.get('native_wake') is not None or
            not context.get('direct_owner_event') or
            identity.get('platform') != 'telegram' or identity.get('chat_id') != owner.OWNER or
            identity.get('user_id') != owner.OWNER):
        raise ValueError('direct original Owner/Main context required for preparation resolution')
    transport = batch.get('native_transport') or {}
    if any(transport.get(k) != identity.get(v) for k, v in (
            ('platform', 'platform'), ('chat_id', 'chat_id'), ('user_id', 'user_id'),
            ('session_key', 'session_key'), ('parent_session_id', 'session'))):
        raise ValueError('preparation resolution differs from confirmed native transport')
    blocker = item['preparation_blocker']
    blocked_report = read_evidence(blocker['evidence'], contract)
    if (blocked_report.get('batch') != batch_id or blocked_report.get('issue') != item['issue']):
        raise ValueError('recorded preparation blocker evidence differs')
    if (not isinstance(report, dict) or report.get('batch') != batch_id or
            report.get('issue') != item['issue'] or
            report.get('decision') != 'replace_missing_documentation_with_repo_evidence' or
            report.get('blocker_evidence') != blocker['evidence'] or
            report.get('contract_sha256') not in (None, owner.binding_hash(contract)) or
            type(report.get('owner_message_id')) is not int or report['owner_message_id'] < 1 or
            not isinstance(report.get('observations'), list) or not report['observations']):
        raise ValueError('preparation resolution must bind current blocker, contract and Owner decision')
    # A session database row is evidence, not authority by itself. The current
    # call also needs the original direct Telegram Main context above.
    db = Path(identity['home']) / 'state.db'
    with sqlite3.connect(db.as_uri() + '?mode=ro', uri=True, timeout=2) as conn:
        conn.execute('PRAGMA query_only=ON')
        session = conn.execute(
            'SELECT source,user_id,session_key,profile_name FROM sessions WHERE id=?',
            (identity['session'],)).fetchone()
        message = conn.execute(
            'SELECT session_id,role,content,timestamp FROM messages WHERE id=?',
            (report['owner_message_id'],)).fetchone()
    message_sha256 = hashlib.sha256((message[2] or '').encode()).hexdigest() if message else None
    if (session != ('telegram', owner.OWNER, identity['session_key'], identity['profile']) or
            not message or message[0] != identity['session'] or message[1] != 'user' or
            message[3] <= blocker['at'] or
            report.get('owner_message_sha256') not in (None, message_sha256)):
        raise ValueError('Owner decision is absent or differs from original native session')
    decision = message[2].casefold()
    if not all(term in decision for term in (
            contract['plane_id'].casefold(), 'genehmige', 'repo-architektur',
            'producer-/consumer-datenwege', 'dokumentationsgrundlage')):
        raise ValueError('Owner message does not authorize this documentation replacement')
    sources = report.get('sources')
    if (not isinstance(sources, list) or len(sources) != 3 or
            not all(isinstance(source, dict) for source in sources) or
            {source.get('role') for source in sources} !=
            {'architecture', 'producer', 'consumer'}):
        raise ValueError('architecture, producer and consumer sources required')
    root = Path(item['worktree'])
    seen = set()
    verified_sources = []
    for source in sources:
        relative = source.get('path')
        if not isinstance(relative, str) or not relative or relative in seen:
            raise ValueError('distinct relative source paths required')
        if (source['role'] == 'architecture' and Path(relative).suffix != '.md') or (
                source['role'] != 'architecture' and not any(
                    Path(relative).is_relative_to(Path(allowed))
                    for allowed in contract['resources']['allowed_paths'])):
            raise ValueError('replacement sources must match architecture and approved code scope')
        path = root / relative
        if (Path(relative).is_absolute() or '..' in Path(relative).parts or
                not path.is_relative_to(root) or path.resolve() != path or not path.is_file()):
            raise ValueError('replacement source differs from clean prepared worktree')
        source_sha256 = hashlib.sha256(path.read_bytes()).hexdigest()
        if source.get('sha256') not in (None, source_sha256):
            raise ValueError('replacement source differs from clean prepared worktree')
        verified_sources.append(dict(role=source['role'], path=relative, sha256=source_sha256))
        seen.add(relative)
    if not worktree_matches(contract, item):
        raise ValueError('preparation resolution requires same clean registered worktree')
    return dict(owner_message_id=report['owner_message_id'],
                owner_message_sha256=message_sha256, sources=verified_sources)


async def owner_decision(runner, event, guard, identity, event_id, words):
    if words[1] == 'extend':
        return await extend_budget(runner, event, guard, identity, event_id, words)
    policy_decision = words[1] == 'review-policy'
    merge_approval = words[1] == 'merge-approve'
    if policy_decision and (len(words) != 4 or words[3] != 'bounded-three'):
        raise ValueError('review-policy requires exact candidate ID and bounded-three')
    if merge_approval and (len(words) != 4 or not re.fullmatch(r'[0-9a-f]{40}', words[3])):
        raise ValueError('merge-approve requires exact candidate ID and current PR head')
    if not policy_decision and not merge_approval and len(words) != 3:
        raise ValueError('accept/accept-test-only/delegate-test requires the exact candidate ID from the test card')
    with guard.transaction(read_only=True) as rows:
        matches = [(bid, b['item']['issue'], copy.deepcopy(b['item']))
                   for bid, b in rows.grants.get('batches', {}).items()
                   if b.get('owner') == owner.stable_owner(identity) and
                   b.get('item', {}).get('candidate', {}).get('id') == words[2]]
    if len(matches) != 1:
        raise ValueError('unknown or superseded candidate')
    bid, issue, item = matches[0]
    value = verify_candidate(item)
    reviewed_pr = None
    if merge_approval:
        _, contract = snapshot(guard, identity, bid, issue)
        if not item.get('pr'):
            raise ValueError('merge approval requires the accepted, independently reviewed PR')
        reviewed_pr = current_pr(item, contract, item['pr']['number'])
    with guard.transaction() as rows:
        _, confirmed_contract, active = live_item(rows, identity, bid, issue)
        if active != item:
            raise ValueError('candidate changed during owner decision')
        receipt = dict(candidate=value['id'], tree=value['tree'], identity=identity,
                       native_owner_event=event_id, at=time.time())
        if merge_approval:
            if not rows.grants['batches'][bid].get('merge_approval_required'):
                raise ValueError('this grant does not require a separate PR merge approval')
            if active.get('phase') != 'reviewed' or not active.get('review') or not active.get('pr'):
                raise ValueError('merge approval requires the accepted, independently reviewed PR')
            pr = reviewed_pr
            if (pr['state'] != 'OPEN' or pr['headRefOid'] != words[3] or
                    active['review']['head'] != words[3]):
                raise ValueError('merge approval requires the current reviewed PR head')
            approval = dict(receipt, issue=issue, pr_number=pr['number'], head=words[3],
                            review_sha256=active['review']['evidence']['sha256'])
            if active.get('merge_approval'):
                if active['merge_approval'] != approval:
                    old = active['merge_approval']
                    if any(old.get(k) != approval[k] for k in
                           ('candidate', 'tree', 'issue', 'pr_number', 'head', 'review_sha256', 'identity')):
                        raise ValueError('different merge approval already present')
                response = f"{item['plane_id']}: Merge für diesen geprüften PR ist bereits freigegeben."
            else:
                active['merge_approval'] = approval
                row = rows[active['task']]
                if row.get('status') != 'awaiting_user' or row.get('gate') != 'item_merge':
                    raise ValueError('merge approval cannot resume an item outside its owner gate')
                row.update(status='pending', gate='main_live', generation=row['generation'] + 1,
                           due=time.time(), action='merge approved reviewed PR and complete closeout')
                response = f"{item['plane_id']}: Merge für diesen geprüften PR ist freigegeben."
        elif policy_decision:
            if not active.get('acceptance') or not active.get('pr') or active['phase'] not in {'review', 'reviewed'}:
                raise ValueError('review policy requires an accepted candidate and verified current PR')
            if active.get('review_policy'):
                if not bounded_review_policy(active, identity):
                    raise ValueError('different review policy already present')
            else:
                active['review_policy'] = dict(receipt, kind='bounded-three', issue=issue,
                                               max_rounds=3, target_score=5,
                                               pr_number=active['pr']['number'], branch=active['branch'])
            response = (f"{item['plane_id']}: Höchstens drei begründete Reviewrunden. Nach Runde drei "
                        'blockiert die Note allein nicht mehr; alle harten Gates bleiben. '
                        'Bestehende Zähler, Abnahmen und Rechte bleiben unverändert.')
        elif active.get('acceptance'):
            if words[1] not in {'accept', 'accept-test-only'}:
                raise ValueError('candidate already accepted; test delegation cannot replace acceptance')
            response = f"{item['plane_id']}: Dieser Kandidat ist bereits angenommen."
        elif active['phase'] != 'awaiting_test':
            raise ValueError('candidate is not awaiting a test decision')
        elif words[1] == 'delegate-test':
            active['test_delegation'] = receipt
            response = f"{item['plane_id']}: Main übernimmt den Computer-Use-Test dieses Kandidaten."
        else:
            if active.get('test_delegation') and active.get('test_result', {}).get('result') != 'passed':
                raise ValueError('delegated test has no passing result yet')
            test_only = (words[1] == 'accept-test-only' or
                         confirmed_contract['acceptance'].get('publication') == 'test_only')
            active.update(acceptance=receipt, phase='accepted_candidate',
                          publication_hold=test_only)
            row = rows[active['task']]
            if test_only:
                row.update(status='awaiting_user', gate='item_test_only',
                           action='accepted candidate held before publication')
                response = f"{item['plane_id']}: Getesteter Kandidat angenommen; Veröffentlichung bleibt angehalten."
            else:
                row.update(status='pending', gate='main_live', generation=row['generation'] + 1,
                           due=time.time(), action='publish accepted candidate without changing its tree')
                response = f"{item['plane_id']}: Getesteter Kandidat angenommen; Main darf daraus den PR erstellen."
    from gateway.platforms.base import _thread_metadata_for_source
    sent = await runner._adapter_for_source(event.source).send(event.source.chat_id, response,
        metadata=_thread_metadata_for_source(event.source, str(event.message_id)))
    if not getattr(sent, 'success', False):
        raise ValueError('owner decision persisted; notification failed, read existing item status')
    return False


async def extend_budget(runner, event, guard, identity, event_id, words):
    if len(words) != 8:
        raise ValueError('extend requires batch, current budget hash, extra work seconds, wakes, recoveries, corrections')
    bid, expected = words[2:4]
    keys = ('todo_seconds', 'wakes', 'recoveries', 'corrections')
    extras = dict(zip(keys, map(int, words[4:])))
    if (any(v < 0 for v in extras.values()) or not any(extras.values()) or
            any(extras[k] > owner.BUDGETS[k] for k in keys)):
        raise ValueError('each explicit increment must be within one standard allowance')
    with guard.transaction() as rows:
        batch = rows.grants.get('batches', {}).get(bid, {})
        issue = batch.get('item', {}).get('issue')
        _, _, item = live_item(rows, identity, bid, issue)
        history = item.setdefault('budget_extensions', [])
        if not any(e['event'] == event_id for e in history):
            if owner.binding_hash(item['budget']) != expected:
                raise ValueError('budget changed; read the current item, never reset counters')
            before = copy.deepcopy(item['budget'])
            for k, v in extras.items():
                item['budget'][k] += v
            history.append(dict(event=event_id, identity=identity, before=before, extras=extras, at=time.time()))
            row = rows.get(item.get('task'))
            # Resume only coordinator work that was paused for a known budget
            # boundary. Never revive an old worker or invalidate its fencing.
            limits = item['budget']
            turn = row.get('coordinator_turns', {}).get(str(row.get('limit_generation')), {}) if row else {}
            correction_budget_block = bool(row and row['status'] == 'blocked'
                and row.get('reason') == 'coordinator answered without lifecycle transition'
                and row.get('gate') == 'reconcile' and row.get('recovery_kind') == 'review'
                and item.get('phase') == 'correction_needed'
                and row['generation'] == row.get('limit_generation', -2) + 1
                and turn.get('status') == 'response_produced' and not turn.get('failed')
                and row['policy']['corrections'] >= before['corrections']
                and row['policy']['corrections'] < limits['corrections'])
            paused_budget = bool(row and row['status'] == 'paused' and row.get('reason') in {
                'owner policy missing, revoked or exhausted', 'total automatic wake budget exhausted'})
            if (row and (paused_budget or correction_budget_block) and
                    item.get('work_seconds', 0) < limits['todo_seconds'] and
                    row['policy']['wakes'] < limits['wakes']):
                from hermes_cli.diggr_continuation import Guard
                checkpoint = copy.deepcopy(row.get('checkpoint'))
                Guard.schedule_recovery(row, time.time(), 'owner extended exhausted item budget')
                row['checkpoint'] = checkpoint
                row['budget_reconciliation'] = dict(generation=row['generation'], native_owner_event=event_id)
                row['reason'] = 'budget extended; reconcile the fenced attempt before any new start'
    from gateway.platforms.base import _thread_metadata_for_source
    sent = await runner._adapter_for_source(event.source).send(event.source.chat_id,
        'Zusatzbudget für dieses Item gespeichert; verbrauchte Arbeit bleibt angerechnet.',
        metadata=_thread_metadata_for_source(event.source, str(event.message_id)))
    if not getattr(sent, 'success', False):
        raise ValueError('budget extension persisted; notification failed, read item status')
    return False


def publish_check(guard, identity, payload):
    bid, issue = payload['batch'], payload['issue']
    batch, contract = snapshot(guard, identity, bid, issue)
    with guard.transaction(read_only=True) as rows:
        _, _, item = live_item(rows, identity, bid, issue)
        item = copy.deepcopy(item)
    value = verify_candidate(item)
    receipt = item.get('acceptance', {})
    if (item.get('publication_hold') or contract['acceptance'].get('publication') == 'test_only' or
            receipt.get('candidate') != value['id'] or receipt.get('tree') != value['tree'] or
            not {'commit', 'push', 'pr_update'} <= set(contract['actions']['main'])):
        raise ValueError('exact candidate owner acceptance, no publication hold and Main publishing rights required')
    if contract['acceptance'].get('mode') == 'automatic':
        if (receipt.get('mode') != 'automatic' or
                receipt.get('confirmation_event') != batch['confirmation_event'] or
                item.get('test_result', {}).get('evidence') != receipt.get('test_evidence')):
            raise ValueError('automatic publication requires confirmed candidate-bound Computer Use evidence')
        report = read_evidence(receipt['test_evidence'], contract)
        if (report.get('candidate_id') != value['id'] or report.get('tree') != value['tree'] or
                report.get('result') != 'passed' or report.get('method') != 'computer_use'):
            raise ValueError('automatic Computer Use evidence changed before publication')
        validate_automatic_test(report, value, contract['acceptance']['required_test_kinds'])
        verify_computer_use_trace(report, value, identity)
    return dict(plane_id=item['plane_id'], tree=value['tree'], candidate=value['id'],
                branch=item['branch'], worktree=item['worktree'], may_publish=True)


def external_readback(payload):
    from hermes_cli import diggr_continuation as dc
    from tools.environments.local import hermes_subprocess_env
    helper = Path(dc.DISPATCHER).with_name('loop_control_item_readback.py')
    # Use the same profile/credential boundary as other native child processes.
    # A gateway provider token must not shadow gh's established CLI credentials.
    try:
        result = subprocess.run([sys.executable, str(helper)], input=json.dumps(payload),
                                env=hermes_subprocess_env(), text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=90)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise ValueError('external readback failed (' + type(exc).__name__ +
                         '); retain item and retry only the readback') from None
    if result.returncode:
        category = 'HelperFailure'
        try:
            error = json.loads(result.stdout)
            allowed = {'CalledProcessError', 'FileNotFoundError', 'PermissionError',
                       'TimeoutExpired', 'ValueError', 'JSONDecodeError', 'ModuleNotFoundError'}
            if (isinstance(error, dict) and error.get('error') == 'external_readback_failed'
                    and error.get('category') in allowed):
                category = error['category']
        except (ValueError, TypeError):
            pass
        # Never surface raw child output: gh/client errors may contain secrets.
        raise ValueError(f'external readback failed ({category}, exit {result.returncode}); '
                         'retain item and retry only the readback')
    return json.loads(result.stdout)


def current_pr(item, contract, number):
    value = external_readback(dict(operation='github', repo=contract['resources']['repo'], number=number))
    if (value.get('number') != number or value.get('headRefName') != item['branch'] or
            value.get('baseRefName') != 'main' or value.get('isCrossRepository')):
        raise ValueError('PR does not target the owner-bound branch and repository main')
    head = value['headRefOid']
    if (git(item['worktree'], 'rev-parse', 'HEAD') != head or
            git(item['worktree'], 'rev-parse', head + '^{tree}') != item['candidate']['tree']):
        raise ValueError('PR head differs from the accepted candidate tree')
    # Verify trust in the owner-bound repository, where its existing relative
    # signer/revocation paths resolve. A linked worktree's .git is a file.
    # The exact worktree HEAD and accepted tree were checked above; do not
    # replace trust files, accept a worktree-local trust override, or skip verify.
    git(contract['resources']['repo'], 'verify-commit', head)
    return value


def record_pr(guard, identity, payload):
    publish_check(guard, identity, payload)
    batch, contract = snapshot(guard, identity, payload['batch'], payload['issue'])
    item = batch['item']
    pr = current_pr(item, contract, payload['pr_number'])
    if pr['state'] != 'OPEN':
        raise ValueError('first PR receipt requires an open candidate PR')
    with guard.transaction() as rows:
        _, _, active = live_item(rows, identity, payload['batch'], payload['issue'])
        if active != item:
            raise ValueError('candidate changed during PR verification')
        if active.get('pr') and active['pr']['number'] != pr['number']:
            raise ValueError('a different PR already belongs to this candidate')
        active.update(pr=pr, phase='review')
    return pr


def bounded_review_policy(item, identity):
    policy = item.get('review_policy', {})
    return bool(policy.get('kind') == 'bounded-three' and policy.get('max_rounds') == 3
                and policy.get('target_score') == 5 and policy.get('issue') == item['issue']
                and policy.get('pr_number') == (item.get('pr') or {}).get('number')
                and policy.get('branch') == item.get('branch')
                and policy.get('identity') == identity and policy.get('native_owner_event'))


def review_progress(item, report, run, ref, contract):
    """Count distinct qualified runs; the reviewer must explain real progress."""
    for key in ('blocking_findings', 'hard_gate_failures', 'improvement_findings'):
        findings = report.get(key, [])
        if not isinstance(findings, list) or any(
                not isinstance(f, (str, dict)) or not f for f in findings):
            raise ValueError('review findings must be lists of concrete findings')
    if 'score_rationale' in report and (not isinstance(report['score_rationale'], str) or not report['score_rationale'].strip()):
        raise ValueError('score rationale must be nonempty text')
    followup = report.get('review_followup')
    if followup is not None:
        if (not isinstance(followup, dict) or set(followup) != {
                'previous_report_sha256', 'kind', 'finding', 'resolution', 'evidence'} or
                not isinstance(followup['previous_report_sha256'], str) or len(followup['previous_report_sha256']) != 64 or
                followup['kind'] not in {'code_correction', 'evidence_clarification'} or
                not isinstance(followup['resolution'], str) or not followup['resolution'].strip() or
                not isinstance(followup['evidence'], list) or not followup['evidence'] or
                any(not isinstance(e, dict) or set(e) != {'path', 'sha256'} for e in followup['evidence'])):
            raise ValueError('complete typed review followup required')
        for progress_ref in followup['evidence']:
            read_evidence(progress_ref, contract)
    elif 'review_followup' in report:
        raise ValueError('null review followup is not a valid first round')
    history = item.get('review_rounds', [])
    for old in history:
        read_evidence(old['evidence'], contract)
        read_evidence(old['run'], contract)
    thread = run.get('resolved_run', {}).get('thread_id')
    if not thread or run.get('reviewer_identity') != {'codex_thread_id': thread}:
        raise ValueError('review round requires the actual independent session identity')
    for old in history:
        if old['evidence']['sha256'] == ref['sha256'] or old['thread_id'] == thread:
            if old['evidence'] != ref or old['thread_id'] != thread:
                raise ValueError('review report/session already counted; never inflate rounds')
            return old['round'], old
    if len(history) >= 3:
        raise ValueError('three qualified review rounds exhausted; no automatic fourth round')
    if not history and followup is not None:
        raise ValueError('first review round cannot refer to an unrecorded predecessor')
    if history:
        old = history[-1]
        prior = read_evidence(old['evidence'], contract)
        followup = report.get('review_followup', {})
        findings = prior.get('blocking_findings', []) + prior.get('hard_gate_failures', []) + prior.get('improvement_findings', [])
        # A historical report can omit an explanation for its deduction. Asking
        # the reviewer to resolve that specific omission is an evidence repair.
        if prior.get('score_5') != 5 and not prior.get('score_rationale') and not findings:
            findings = ['missing score rationale']
        if (followup.get('previous_report_sha256') != old['evidence']['sha256'] or
                followup.get('finding') not in findings or not followup.get('resolution') or
                followup.get('kind') not in {'code_correction', 'evidence_clarification'}):
            raise ValueError('next round must resolve a concrete prior finding, not just seek a higher score')
        if followup['finding'] == 'missing score rationale' and not report.get('score_rationale'):
            raise ValueError('score clarification must actually supply the missing rationale')
        refs = followup.get('evidence')
        used = {e['sha256'] for h in history for e in h.get('progress_evidence', [])}
        used.update(h['evidence']['sha256'] for h in history)
        if (not isinstance(refs, list) or not refs or
                any(not isinstance(e, dict) or set(e) != {'path', 'sha256'} for e in refs) or
                any(e['sha256'] in used for e in refs) or
                len({e['sha256'] for e in refs}) != len(refs)):
            raise ValueError('review followup requires new immutable correction or clarification evidence')
        for evidence_ref in refs:
            read_evidence(evidence_ref, contract)
        if followup['kind'] == 'code_correction' and old['tree'] == item['candidate']['tree']:
            raise ValueError('code correction must change the accepted source tree')
        if followup['kind'] == 'evidence_clarification' and old['tree'] != item['candidate']['tree']:
            raise ValueError('changed source requires a code-correction review and fresh acceptance')
    return len(history) + 1, None


def review(guard, identity, payload):
    batch, contract = snapshot(guard, identity, payload['batch'], payload['issue'])
    item = batch['item']
    if 'review' not in contract['actions']['main'] or not item.get('pr'):
        raise ValueError('Main review right and verified PR required')
    publish_check(guard, identity, payload)
    pr = current_pr(item, contract, item['pr']['number'])
    if pr['state'] != 'OPEN':
        raise ValueError('new review requires the still-open item PR')
    previous = item.get('review')
    if (previous and previous.get('head') == pr['headRefOid'] and
            previous.get('evidence') == payload['evidence'] and
            previous.get('run') == payload['review_run']):
        read_evidence(previous['evidence'], contract)
        read_evidence(previous['run'], contract)
        return previous
    report = read_evidence(payload['evidence'], contract)
    # This is Main's accountable inspection of the separate reviewer's result,
    # not an assertion that the native runtime itself performed the review.
    bounded = bounded_review_policy(item, identity)
    with guard.transaction(read_only=True) as rows:
        direct_worker = rows.get(item.get('task'), {}).get('direct_worker') is True
    scored = bounded or not (batch.get('flow_version') == 3 and direct_worker)
    hard_pass = (report.get('passed') is True and report.get('merge_recommendation') == 'merge'
                 and report.get('blocking_findings') == [] and report.get('hard_gate_failures') == [])
    if (report.get('reviewed_head_sha') != pr['headRefOid'] or
            (scored and (type(report.get('score_5')) not in (int, float) or
                         not 0 <= report['score_5'] <= 5)) or
            (not bounded and scored and (not hard_pass or report['score_5'] != 5)) or
            not report.get('acceptance_coverage') or
            not report.get('deterministic_gates') or not report.get('summary')):
        raise ValueError('independent review has findings, missing checks or a stale head')
    run = read_evidence(payload['review_run'], contract)
    if (run.get('role') != 'independent_reviewer' or run.get('exit_code') != 0 or
            run.get('head') != pr['headRefOid'] or run.get('model') != contract['model']['required_model'] or
            run.get('provider') != contract['model']['provider'] or
            run.get('reasoning_effort') != contract['model']['reasoning_effort'] or
            not run.get('reviewer_identity') or run.get('reviewer_identity') == identity or
            not run.get('implementer_identity') or run['reviewer_identity'] == run['implementer_identity'] or
            not run.get('visible_target') or run.get('report_sha256') != payload['evidence']['sha256']):
        raise ValueError('separate visible reviewer run with actual model and result binding required')
    skill_receipt = read_evidence(run['completion_receipt'], contract)
    validation = external_readback(dict(operation='review', repo=contract['resources']['repo'],
        worktree=item['worktree'], base=run.get('base'), branch=item['branch'], head=pr['headRefOid'],
        lane=item['lane'], work_item='WP-' + item['plane_id'], run_id=run.get('run_id'),
        report=payload['evidence'], completion_receipt=skill_receipt,
        resolved_run=run.get('resolved_run'), raw_jsonl=run.get('raw_jsonl'),
        model=run['model'], provider=run['provider'], reasoning_effort=run['reasoning_effort'],
        score_required=scored))
    direct_result_flow = batch.get('flow_version') == 3 and direct_worker and not bounded
    if (validation.get('qualified') if bounded or direct_result_flow else validation.get('passed')) is not True:
        raise ValueError('existing required-skills and semantic reviewer gate failed: ' +
                         ', '.join(validation.get('reasons', ['missing verdict'])))
    if direct_result_flow and not hard_pass:
        findings = [json.dumps(f, sort_keys=True, ensure_ascii=False) if isinstance(f, dict) else f
                    for f in report.get('blocking_findings', []) + report.get('hard_gate_failures', [])]
        if not findings or any(not isinstance(f, str) or not f.strip() for f in findings):
            raise ValueError('negative independent review requires concrete blocking findings')
        correction = flow_assessment(guard, identity,
                                     dict(payload, findings=findings,
                                          review_evidence=payload['evidence']), 'failed')
        result = return_for_correction(guard, identity, dict(payload, evidence=correction))
        receipt = dict(head=pr['headRefOid'], evidence=payload['evidence'], run=payload['review_run'],
                       checked_by=identity, at=time.time(), merge_ready=False)
        with guard.transaction() as rows:
            _, _, active = live_item(rows, identity, payload['batch'], payload['issue'])
            if active.get('correction') != correction:
                raise ValueError('review correction changed before evidence archival')
            active.setdefault('review_history', []).append(receipt)
        return dict(result, review=receipt)
    round_number, previous = review_progress(item, report, run, payload['evidence'], contract) if bounded else (None, None)
    ready = hard_pass and (validation.get('hard_gates_passed') if bounded else validation.get('passed')) is True
    ready = ready and (not scored or report['score_5'] == 5 or round_number == 3)
    receipt = dict(head=pr['headRefOid'], evidence=payload['evidence'], run=payload['review_run'],
                   checked_by=identity, at=time.time())
    if batch.get('flow_version') == 3 and direct_worker:
        receipt['merge_ready'] = ready
    if bounded:
        receipt.update(round=round_number, merge_ready=ready, score=report['score_5'],
                       thread_id=run['resolved_run']['thread_id'], tree=item['candidate']['tree'],
                       pr_number=pr['number'],
                       progress_evidence=report.get('review_followup', {}).get('evidence', []),
                       policy_event=item['review_policy']['native_owner_event'])
        if previous:
            if previous['run'] != payload['review_run'] or previous['head'] != pr['headRefOid']:
                raise ValueError('counted review binding changed')
            return previous
    with guard.transaction() as rows:
        _, _, active = live_item(rows, identity, payload['batch'], payload['issue'])
        if active != item:
            raise ValueError('item changed during review readback')
        if (read_evidence(payload['evidence'], contract) != report or
                read_evidence(payload['review_run'], contract) != run):
            raise ValueError('review evidence changed before persistence')
        if bounded:
            review_progress(active, report, run, payload['evidence'], contract)
            active.setdefault('review_rounds', []).append(receipt)
        if ready:
            active.pop('merge_approval', None)
            active.update(review=receipt, phase='reviewed')
            if batch.get('merge_approval_required'):
                row = rows[item['task']]
                row.update(status='awaiting_user', gate='item_merge',
                           generation=row['generation'] + 1,
                           action='wait for owner approval of reviewed PR head')
            if (batch.get('flow_version') == 3 and
                    rows[item['task']].get('direct_worker') is True and
                    not batch.get('merge_approval_required')):
                active.setdefault('review_closeout_continuation', dict(
                    status='pending', source_generation=rows[item['task']]['generation'],
                    report_sha256=payload['evidence']['sha256']))
        else:
            active.pop('review', None)
            active['phase'] = 'review'
    return receipt


def merge_check(guard, identity, payload):
    publish_check(guard, identity, payload)
    batch, contract = snapshot(guard, identity, payload['batch'], payload['issue'])
    item = batch['item']
    if ('merge' not in contract['actions']['main'] or item.get('phase') != 'reviewed' or
            not item.get('acceptance') or not item.get('review')):
        raise ValueError('Main merge right and independent review required')
    pr = current_pr(item, contract, item['pr']['number'])
    if item['review']['head'] != pr['headRefOid']:
        raise ValueError('review no longer matches the current PR head')
    if batch.get('merge_approval_required'):
        approval = item.get('merge_approval') or {}
        if any(approval.get(k) != v for k, v in (
                ('candidate', item['candidate']['id']), ('tree', item['candidate']['tree']),
                ('issue', item['issue']), ('pr_number', pr['number']),
                ('head', pr['headRefOid']),
                ('review_sha256', item['review']['evidence']['sha256']),
                ('identity', identity))) or not approval.get('native_owner_event'):
            raise ValueError('owner merge approval required: /continuation merge-approve '
                             + item['candidate']['id'] + ' ' + pr['headRefOid'])
    read_evidence(item['review']['evidence'], contract)
    read_evidence(item['review']['run'], contract)
    if item['review'].get('round'):
        if (not bounded_review_policy(item, identity) or
                item.get('review_rounds', [])[-1:] != [item['review']] or
                item['review'].get('merge_ready') is not True or
                item['review'].get('policy_event') != item['review_policy']['native_owner_event']):
            raise ValueError('native bounded review history no longer matches its owner policy')
    return dict(pr=pr, match_head_commit=pr['headRefOid'], may_merge=pr['state'] == 'OPEN')


def return_for_correction(guard, identity, payload):
    from hermes_cli import diggr_continuation as dc
    with guard.transaction(read_only=True) as rows:
        _, contract, item = live_item(rows, identity, payload['batch'], payload['issue'])
        item = copy.deepcopy(item)
        state_before = copy.deepcopy(rows)
        observed = copy.deepcopy(rows[item['task']])
    report = read_evidence(payload['evidence'], contract)
    if item.get('phase') == 'correction_needed':
        if item.get('correction') != payload['evidence']:
            raise ValueError('correction already pending; reconcile its current wake before changing findings')
        return dict(phase='correction_needed', task=item['task'], evidence=payload['evidence'])
    before_candidate = item.get('phase') == 'coding'
    if before_candidate:
        result = observed.get('worker_result', {})
        if (observed['gate'] != 'main_validation' or
                observed['status'] not in {'pending', 'executing', 'blocked'} or
                type(result.get('exit_code')) is not int or result.get('artifact_status') != 'present' or
                any(result.get(k) != observed.get(k) for k in ('task', 'artifact', 'action_id', 'effect_id'))):
            raise ValueError('correction requires a completed native result at Main validation')
        if (any(report.get(k) != observed[k] for k in ('task', 'action_id', 'effect_id')) or
                report.get('worker_generation') != result.get('generation') or
                report.get('result_sha256') != result.get('evidence', {}).get('sha256') or
                report.get('result') != 'failed' or not isinstance(report.get('findings'), list) or
                not report['findings'] or not all(isinstance(f, str) and f.strip() for f in report['findings'])):
            raise ValueError('correction needs result-bound failed assessment and concrete findings')
        dc.verified_bytes(observed['artifact'], report['result_sha256'])
    elif item.get('phase') not in {'awaiting_test', 'accepted_candidate', 'review', 'reviewed'} or not item.get('candidate'):
        raise ValueError('correction requires a completed candidate; reconcile any build first')
    elif (report.get('candidate_id') != item.get('candidate', {}).get('id') or
            report.get('result') != 'failed' or not report.get('findings')):
        raise ValueError('correction needs candidate-bound failed checks and concrete findings')
    if item.get('pr'):
        pr = current_pr(item, contract, item['pr']['number'])
        if pr['state'] == 'MERGED':
            raise ValueError('merged PR needs item_followup_prepare with integrated failure evidence')
    verify_completed_exit(observed)
    with guard.transaction() as rows:
        batch, _, active = live_item(rows, identity, payload['batch'], payload['issue'])
        if active != item or not dc.recovery_snapshot_matches(state_before, rows):
            raise ValueError('item changed during correction check')
        # Re-read the same immutable files after external process verification.
        if read_evidence(payload['evidence'], contract) != report:
            raise ValueError('correction evidence changed during verification')
        if before_candidate:
            dc.verified_bytes(observed['artifact'], report['result_sha256'])
        if active.get('review'):
            active.setdefault('review_history', []).append(active.pop('review'))
        active.pop('review_closeout_continuation', None)
        active.pop('merge_approval', None)
        active.update(phase='correction_needed', correction=payload['evidence'])
        active.pop('acceptance', None)
        row = rows[item['task']]
        if batch.get('flow_version') == 3 and row.get('direct_worker') is True:
            if row['policy']['corrections'] >= row['policy'].get('limits', {}).get('corrections', 2):
                context = dc.EVENT_CONTEXT.get() or {}
                direct_feedback = (
                    ((not before_candidate and item['phase'] == 'awaiting_test') or
                     (before_candidate and item.get('candidate') and
                      item.get('correction_history'))) and
                    not item.get('acceptance') and
                    context.get('identity') == identity and
                    context.get('native_wake') is None and
                    bool(context.get('direct_owner_event')) and
                    identity.get('platform') == 'telegram' and
                    identity.get('chat_id') == owner.OWNER and
                    identity.get('user_id') == owner.OWNER and
                    batch.get('coordinator_identity') == identity and
                    owner.allowed(row, time.time()) and
                    row['policy']['corrections'] == row['policy']['limits']['corrections'] ==
                        active['budget']['corrections'] and
                    not any(row.get(field) is not None and time.time() >= row[field]
                            for field in ('hard_stop', 'authorization_expires_at')) and
                    all(entry.get('owner_event') != context['direct_owner_event']
                        for entry in active.get('feedback_extensions', [])))
                if not direct_feedback:
                    raise ValueError('confirmed correction budget exhausted')
                # The authenticated candidate rejection itself authorizes one
                # same-scope correction. Work time and all other rights stay fixed.
                active['budget']['corrections'] += 1
                row['policy']['limits']['corrections'] += 1
                active.setdefault('feedback_extensions', []).append(dict(
                    owner_event=context['direct_owner_event'],
                    candidate_id=item['candidate']['id'], evidence=payload['evidence']))
            if active.get('candidate'):
                active.setdefault('candidate_history', []).append(active.pop('candidate'))
            prior = active.get('coding_assignment')
            if prior:
                active.setdefault('coding_assignment_history', []).append(prior)
            active['coding_assignment'] = dict(id=secrets.token_hex(24), status='pending',
                                               created_at=time.time(),
                                               correction_of=row['worker_result']['generation'])
            row.update(status='blocked', gate='coding_correction',
                       reason='result-bound findings awaiting one Coding correction')
        else:
            dc.Guard.schedule_recovery(row, time.time(), 'result correction: see bound findings' if before_candidate
                                       else 'candidate correction: see bound findings')
            row['recovery_kind'] = 'review'
    return dict(phase='correction_needed', task=item['task'], evidence=payload['evidence'])


def prepare_followup(guard, identity, payload):
    """Keep the same item and budgets after a failed integrated smoke.

    The old merged PR and worktree stay immutable. Only the next branch is
    prepared here; evidence-gated recover reserves the next ticket, whose native
    preflight and transport perform the start.
    """
    from hermes_cli import diggr_continuation as dc
    batch, contract = snapshot(guard, identity, payload['batch'], payload['issue'])
    item = batch['item']
    previous = item.get('followup_preparation')
    if previous and item.get('phase') == 'correction_needed':
        # Idempotent readback of the already reserved post-merge followup.
        # This is not a fresh merge authorization for a corrected candidate.
        if (not item.get('acceptance') or not item.get('review') or
                previous.get('evidence') != payload['evidence']):
            raise ValueError('followup readback differs from the merged item')
        pr = current_pr(item, contract, item['pr']['number'])
        if item['review']['head'] != pr['headRefOid']:
            raise ValueError('followup review head changed')
    else:
        pr = merge_check(guard, identity, payload)['pr']
    report = read_evidence(payload['evidence'], contract)
    if (pr['state'] != 'MERGED' or not pr.get('mergedAt') or
            report.get('result') != 'failed' or not report.get('findings') or
            report.get('merge_commit') != pr['mergeCommit']['oid']):
        raise ValueError('followup requires actual merge and integrated failure evidence')
    repo = contract['resources']['repo']
    path, branch = Path(payload['worktree']), payload['branch']
    base = git(repo, 'rev-parse', 'origin/main^{commit}')
    remote_main = git(repo, 'ls-remote', '--exit-code', 'origin', 'refs/heads/main').split()[0]
    if (base != remote_main or payload['base'] != base or report.get('main_sha') != base or
            not path.is_absolute() or str(path.resolve()) != str(path) or
            not path.is_relative_to(Path(contract['resources']['worktrees'])) or
            path == Path(contract['resources']['worktrees']) or str(path) == item['worktree'] or
            not branch.startswith('hermes/WP-' + item['plane_id'] + '-') or branch == item['branch'] or
            git(repo, 'check-ref-format', '--branch', branch) != branch or payload['target'] != item['target']):
        raise ValueError('followup must use a fresh bounded branch at current remote main')
    binding = dc.bind_visible_target(payload['target'])
    dc.bound_shell_identity(binding)
    with guard.transaction(read_only=True) as rows:
        row = copy.deepcopy(rows[item['task']])
    verify_completed_exit(row)
    wanted = dict(issue=item['issue'], plane_id=item['plane_id'], repo=repo, branch=branch,
                  worktree=str(path), base=base, target=item['target'])
    if previous:
        if any(previous.get(k) != v for k, v in wanted.items()):
            raise ValueError('different followup already reserved')
        if not path.exists() and not path.is_symlink() and previous['phase'] == 'preparing':
            # Resume the exact reserved preparation after a pre-Git interruption.
            # An already created branch must still point to its reserved base.
            try:
                branch_head = git(repo, 'rev-parse', '--verify', 'refs/heads/' + branch)
            except subprocess.CalledProcessError:
                branch_head = None
            if branch_head and branch_head != base:
                raise ValueError('reserved followup branch moved; retain for reconciliation')
            if branch_head:
                git(repo, 'worktree', 'add', str(path), branch)
            else:
                git(repo, 'worktree', 'add', '-b', branch, str(path), base)
        if not worktree_matches(contract, wanted):
            raise ValueError('followup preparation unresolved; no second creation')
        if previous['phase'] == 'prepared':
            return wanted
    else:
        if path.exists() or path.is_symlink():
            raise ValueError('followup path already exists')
        with guard.transaction() as rows:
            _, _, active = live_item(rows, identity, payload['batch'], payload['issue'])
            if active != item:
                raise ValueError('item changed before followup preparation')
            active['followup_preparation'] = dict(wanted, evidence=payload['evidence'], phase='preparing')
        try:
            git(repo, 'worktree', 'add', '-b', branch, str(path), base)
        except (OSError, subprocess.SubprocessError):
            raise ValueError('followup creation unresolved; read back same preparation') from None
        if not worktree_matches(contract, wanted):
            raise ValueError('followup worktree differs from reserved base')
    with guard.transaction() as rows:
        _, _, active = live_item(rows, identity, payload['batch'], payload['issue'])
        active['followup_preparation']['phase'] = 'prepared'
        active['phase'] = 'correction_needed'
        dc.Guard.schedule_recovery(rows[item['task']], time.time(), 'integrated smoke requires followup PR')
        rows[item['task']]['recovery_kind'] = 'review'
    return wanted


def closeout(guard, identity, payload):
    batch, contract = snapshot(guard, identity, payload['batch'], payload['issue'])
    item = batch.get('item', {})
    if item.get('phase') == 'closed':
        return copy.deepcopy(item['closeout'])  # never release or merge twice
    check = merge_check(guard, identity, payload)
    if not {'smoke', 'plane_update', 'nextcloud_update'} <= set(contract['actions']['main']):
        raise ValueError('closeout rights missing')
    pr = check['pr']
    if pr.get('state') != 'MERGED' or not pr.get('mergedAt') or not pr.get('mergeCommit', {}).get('oid'):
        raise ValueError('PR not actually merged; queue entry keeps the lane reserved')
    path = Path(payload['main_worktree'])
    repo = contract['resources']['repo']
    if (not path.is_absolute() or str(path.resolve()) != str(path) or
            not (str(path) == repo or path.is_relative_to(Path(contract['resources']['worktrees'])))):
        raise ValueError('integrated smoke worktree outside approved resources')
    main_sha = git(repo, 'ls-remote', '--exit-code', 'origin', 'refs/heads/main').split()[0]
    if (git(path, 'rev-parse', 'HEAD') != main_sha or git(path, 'status', '--porcelain') or
            git(path, 'rev-parse', '--path-format=absolute', '--git-common-dir') !=
            git(item['worktree'], 'rev-parse', '--path-format=absolute', '--git-common-dir')):
        raise ValueError('integrated smoke needs the clean actual remote main in the same repository')
    git(path, 'merge-base', '--is-ancestor', pr['mergeCommit']['oid'], main_sha)
    smoke = read_evidence(payload['smoke'], contract)
    if (smoke.get('main_sha') != main_sha or smoke.get('merge_commit') != pr['mergeCommit']['oid'] or
            smoke.get('result') != 'passed' or not smoke.get('checks') or smoke.get('worktree') != str(path)):
        raise ValueError('passing integrated smoke evidence must match current main')
    archive = read_evidence(payload['archive'], contract)
    if (archive.get('issue') != payload['issue'] or archive.get('candidate') != item['candidate']['id'] or
            archive.get('pr_head') != pr['headRefOid'] or archive.get('main_sha') != main_sha or
            archive.get('smoke_sha256') != payload['smoke']['sha256']):
        raise ValueError('closeout archive does not bind the accepted item and integrated smoke')
    external = external_readback(dict(operation='closeout', repo=repo, number=pr['number'],
        issue=payload['issue'], plane_id=item['plane_id'], nextcloud_path=payload['nextcloud_path'],
        nextcloud_sha256=payload['archive']['sha256']))
    if (external['github']['headRefOid'] != pr['headRefOid'] or
            external['github'].get('mergeCommit') != pr['mergeCommit'] or
            external['github'].get('state') != 'MERGED' or
            external['plane'].get('issue') != payload['issue'] or
            external['plane'].get('plane_id') != item['plane_id'] or
            external['plane'].get('state') not in {'Accepted', 'Done'} or
            external['plane'].get('state_group') != 'completed' or
            external['plane'].get('state') != external['plane'].get('completion_state')):
        raise ValueError('GitHub or Plane closeout is incomplete; lane retained')
    from hermes_cli import diggr_continuation as dc
    with guard.transaction(read_only=True) as rows:
        observed = copy.deepcopy(rows[item['task']])
    verify_completed_exit(observed)
    receipt = dict(candidate=item['candidate']['id'], tree=item['candidate']['tree'],
        github=external['github'], plane=external['plane'], nextcloud=external['nextcloud'],
        main_sha=main_sha, smoke=payload['smoke'], archive=payload['archive'], at=time.time())
    with guard.transaction() as rows:
        _, _, active = live_item(rows, identity, payload['batch'], payload['issue'])
        if active != item or rows[item['task']] != observed:
            raise ValueError('item changed during closeout verification')
        active.update(phase='closed', closeout=receipt)
        rows[item['task']].update(status='done', gate='done', effect_status='reconciled',
                                 item_closeout=copy.deepcopy(receipt))
    return receipt


def control(guard, identity, payload):
    operation = payload['operation']
    if operation == 'item_status':
        if 'include_worker_route' in payload and type(payload['include_worker_route']) is not bool:
            raise ValueError('include_worker_route must be boolean')
        if 'include_runtime' in payload and type(payload['include_runtime']) is not bool:
            raise ValueError('include_runtime must be boolean')
        return status(guard, identity, payload['batch'], payload['issue'],
                      include_worker_route=payload.get('include_worker_route', False),
                      include_runtime=payload.get('include_runtime', False))
    if operation == 'item_assignment_renew_preview':
        core, before, _, _ = assignment_renew_facts(
            guard, identity, payload['batch'], payload['issue'], payload.get('evidence'))
        if core.get('renewal_number') == 2:
            core.update(preview_nonce=secrets.token_hex(24), preview_created_at=time.time())
            with guard.transaction() as rows:
                _, _, active = live_item(rows, identity, payload['batch'], payload['issue'])
                if active != before:
                    raise ValueError('second-renewal item changed during preview')
                active['assignment_renew_preview'] = dict(core=copy.deepcopy(core), sha256=owner.binding_hash(core))
        digest = owner.binding_hash(core)
        return dict(core=core, sha256=digest,
                    next_owner_command='/continuation assignment-renew ' +
                    payload['batch'] + ' ' + digest)
    if operation == 'item_preparation_assignment_recover':
        return recover_preparation_assignment(guard, identity, payload)
    if operation == 'item_preparation_resolve':
        from hermes_cli import diggr_continuation as dc
        context = dc.EVENT_CONTEXT.get()
        if (not context or context.get('identity') != identity or
                context.get('native_wake') is not None or
                not context.get('direct_owner_event') or
                identity.get('platform') != 'telegram' or
                identity.get('chat_id') != owner.OWNER or identity.get('user_id') != owner.OWNER):
            raise ValueError('direct original Owner/Main context required for preparation resolution')
        with guard.transaction() as rows:
            batch, contract, item = live_item(rows, identity, payload['batch'], payload['issue'])
            if (batch.get('proposal') != owner.binding_hash(batch['manifest']) or
                    rows.grants.get('proposals', {}).get(batch['proposal'], {}).get('manifest') != batch['manifest']):
                raise ValueError('confirmed preparation contract changed')
            prior = item.get('preparation_resolution')
            if prior:
                if prior['evidence'] != payload['evidence']:
                    raise ValueError('preparation was already resolved with different evidence')
                return copy.deepcopy(prior)
            if (item['phase'] != 'prepared' or batch['tasks'] or item.get('task') or
                    not item.get('preparation_blocker')):
                raise ValueError('documented unregistered preparation blocker required')
            report = read_evidence(payload['evidence'], contract)
            # Bind the helper to the requested batch without trusting a report field.
            verified = verify_documentation_resolution(
                payload['batch'], batch, contract, item, report, identity)
            blocker = item.pop('preparation_blocker')
            receipt = dict(evidence=copy.deepcopy(payload['evidence']), blocker=blocker,
                           contract_sha256=owner.binding_hash(contract), **verified)
            batch.setdefault('resolved_preparation_blockers', []).append(copy.deepcopy(receipt))
            item['preparation_resolution'] = receipt
            return copy.deepcopy(receipt)
    if operation == 'item_scope_amend':
        return amend_app105_test_scope(guard, identity, payload)
    if operation == 'item_assignment_rebind':
        return rebind_unstarted_app105_assignment(guard, identity, payload)
    if operation == 'item_preparation_blocked':
        # This holds only automatic preparation continuation, never frees the
        # lane, changes the grant or proves nonstart. Native Main owns the report.
        with guard.transaction() as rows:
            batch, contract, item = live_item(rows, identity, payload['batch'], payload['issue'])
            if item['phase'] != 'prepared' or batch['tasks']:
                raise ValueError('preparation blocker requires the same unregistered item')
            report = read_evidence(payload['evidence'], contract)
            if (report.get('batch') != payload['batch'] or report.get('issue') != payload['issue'] or
                    not isinstance(report.get('reason'), str) or not report['reason'].strip() or
                    not isinstance(report.get('observations'), list) or not report['observations']):
                raise ValueError('preparation blocker requires bound reason and concrete observations')
            item['preparation_blocker'] = dict(evidence=payload['evidence'], reason=report['reason'], at=time.time())
            return copy.deepcopy(item['preparation_blocker'])
    if operation == 'item_prepare':
        with preparation_lock(guard):
            return prepare(guard, identity, payload)
    if operation == 'item_migration_preview':
        return migration_preview(guard, identity, payload)
    if operation == 'item_migration_rollback_preview':
        return migration_rollback_preview(guard, identity, payload)
    if operation == 'item_prepare_readback':
        with preparation_lock(guard):
            return reconcile_preparation(guard, identity, payload)
    if operation == 'item_prepare_resume':
        with preparation_lock(guard):
            return reconcile_preparation(guard, identity, payload, resume=True)
    if operation == 'item_candidate':
        if 'assessment' not in payload and 'checks' in payload:
            payload = dict(payload, assessment=flow_assessment(guard, identity, payload, 'passed'))
        with preparation_lock(guard):
            return candidate(guard, identity, payload)
    if operation == 'item_build_readback':
        return build_readback(guard, identity, payload)
    if operation == 'item_publish_check':
        return publish_check(guard, identity, payload)
    if operation == 'item_pr':
        return record_pr(guard, identity, payload)
    if operation == 'item_review':
        return review(guard, identity, payload)
    if operation == 'item_merge_check':
        return merge_check(guard, identity, payload)
    if operation == 'item_closeout':
        return closeout(guard, identity, payload)
    if operation == 'item_return':
        if 'evidence' not in payload and 'findings' in payload:
            payload = dict(payload, evidence=flow_assessment(guard, identity, payload, 'failed'))
        return return_for_correction(guard, identity, payload)
    if operation == 'item_followup_prepare':
        with preparation_lock(guard):
            return prepare_followup(guard, identity, payload)
    if operation == 'item_test_result':
        with guard.transaction() as rows:
            batch, contract, item = live_item(rows, identity, payload['batch'], payload['issue'])
            value = verify_candidate(item)
            if (item.get('test_delegation', {}).get('candidate') != value['id'] or
                    payload['candidate_id'] != value['id'] or payload['result'] not in {'passed', 'failed'}):
                raise ValueError('delegated test must bind the current candidate')
            previous = item.get('test_result')
            if previous:
                if (previous['candidate'] == value['id'] and previous['result'] == payload['result']
                        and previous['evidence'] == payload['evidence']):
                    return copy.deepcopy(previous)
                raise ValueError('conflicting test result for the same candidate')
            if item.get('phase') != 'awaiting_test':
                raise ValueError('candidate is not awaiting a test result')
            report = read_evidence(payload['evidence'], contract)
            if (report.get('candidate_id') != value['id'] or report.get('tree') != value['tree'] or
                    report.get('result') != payload['result'] or report.get('method') != 'computer_use' or
                    not report.get('checks')):
                raise ValueError('Computer Use evidence must bind candidate, tree and checks')
            automatic = contract['acceptance'].get('mode') == 'automatic'
            if automatic and payload['result'] == 'passed':
                if (item['test_delegation'].get('mode') != 'automatic' or
                        item['test_delegation'].get('confirmation_event') != batch['confirmation_event']):
                    raise ValueError('automatic test lacks the confirmed item-bound mode')
                validate_automatic_test(report, value, contract['acceptance']['required_test_kinds'])
                verify_computer_use_trace(report, value, identity)
            item['test_result'] = dict(candidate=value['id'], result=payload['result'],
                                       evidence=payload['evidence'], tester=identity, at=time.time())
            if automatic and payload['result'] == 'passed':
                item.update(acceptance=dict(candidate=value['id'], tree=value['tree'],
                    mode='automatic', confirmation_event=batch['confirmation_event'],
                    test_evidence=payload['evidence'], at=time.time()),
                    phase='accepted_candidate', publication_hold=False)
                row = rows[item['task']]
                row.update(status='pending', gate='main_live', generation=row['generation'] + 1,
                           due=time.time(), action='publish the exact Computer-Use-tested candidate')
            return copy.deepcopy(item['test_result'])
    raise ValueError('unsupported item lifecycle operation')
