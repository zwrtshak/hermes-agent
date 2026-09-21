"""Recovery across real CLI processes; cmux/TTY inspection is a fixture transport.

All processes, Git data and state belong to disposable fixtures. Shell/TTY
shape is simulated; psutil process identities are real. No Codex command or
cmux send is executed.
"""
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import time

import psutil
import pytest

from hermes_cli import diggr_continuation as dc

pytestmark = pytest.mark.macos_only


def child():
    return subprocess.Popen([sys.executable, '-B', '-c', 'import sys; sys.stdin.read()'],
                            stdin=subprocess.PIPE)


@pytest.fixture
def recovery(tmp_path, monkeypatch, request):
    root = tmp_path.resolve()
    shells = []

    def shell():
        proc = child()
        shells.append(proc)
        return proc

    def process_identity(pid):
        return dict(pid=pid, ppid=os.getpid(), pgid=pid, tpgid=pid, tty='fixture',
                    start=str(psutil.Process(pid).create_time()), executable='/bin/bash')
    monkeypatch.setattr(dc, 'process_identity', process_identity)

    target = dict(workspace='11111111-1111-4111-8111-111111111111',
                  surface='22222222-2222-4222-8222-222222222222')
    snapshot_path = root / 'cmux-fixture.json'

    def inventory(proc):
        identity = dc.process_identity(proc.pid)
        snapshot = dict(kind='workspace', id=target['workspace'], children=[dict(
            kind='surface', id=target['surface'], type='terminal', tty=identity['tty'],
            top_level_pids=[proc.pid], root_pids=[proc.pid],
            foreground_pgids=[proc.pid], tty_process_pids=[proc.pid])])
        snapshot_path.write_text(json.dumps(snapshot), encoding='utf-8')

    original_shell = shell()
    inventory(original_shell)
    monkeypatch.setattr(dc, 'cmux_snapshot', lambda target: json.loads(snapshot_path.read_text(encoding='utf-8')))
    work = root / 'work'
    work.mkdir()
    def git(*args):
        return subprocess.check_output(['git', '-C', str(work), *args], text=True, encoding='utf-8',
                                       stderr=subprocess.PIPE).strip()
    git('init', '--initial-branch=hermes/fixture')
    git('-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid',
        '-c', 'commit.gpgsign=false', 'commit', '--allow-empty', '-m', 'fixture')
    packet = dict(routing_mode='non_cmm', operator_scope_authorized=True,
                  plane_id='SYSTEM-167', task_id='SYSTEM-167', scope_id='SYSTEM-167',
                  worktree=str(work), branch='hermes/fixture', base=git('rev-parse', 'HEAD'),
                  requested_actions=['inspect', 'test'], coding_route=dict(
                      executor='codex', required_model='gpt-6-astra', fallback_models_allowed=[],
                      merge_authorized=False, routine_actions_authorized=['inspect', 'test']))
    packet_path = root / 'packet.json'
    packet_path.write_text(json.dumps(packet), encoding='utf-8')
    artifact = str(root / 'result')
    route = dict(packet=str(packet_path), packet_sha256=dc.digest(packet_path),
                 receipt=str(root / 'receipt'), worktree=str(work), branch='hermes/fixture',
                 plane_id='SYSTEM-167', visible_target=target, argv=[
                     '/fixture/codex', 'exec', '--model', 'gpt-6-astra', '--sandbox',
                     'workspace-write', '--cd', str(work), '--output-last-message', artifact,
                     'inspect disposable fixture'])
    identity = dict(home=str(root), profile='diggr-main', session='fixture',
                    session_key='fixture', platform='telegram', chat_id='564628210',
                    user_id='564628210', thread_id='')
    (root / 'config.yaml').write_text('diggr_continuation:\n  enabled: true\n'
                                    '  profile: diggr-main\n  home: ' + str(root) + '\n', encoding='utf-8')
    task = dict(task='fixture', scope='fixture only', owner='coding', gate='coding',
                action='inspect', artifact=artifact, deadline=time.time()+3600,
                wake_budget=1, authorization='fixture only', producer='cmux', worker_route=route)
    token = dc.EVENT_CONTEXT.set(dict(identity=identity))
    launcher = child()
    if getattr(request, 'param', None) == 'exited':
        launcher.stdin.close()
        launcher.wait(timeout=5)
    from tools.process_registry import ProcessSession, process_registry
    session = ProcessSession(id='proc_fixture', command='harmless fixture', session_key='fixture',
                             pid=launcher.pid, process=launcher, started_at=time.time())
    monkeypatch.setitem(process_registry._running, session.id, session)
    try:
        dc.terminal_dispatch(dict(command='fixture only', continuation=task, background=True),
                             lambda args: dict(session_id=session.id))
        guard = dc.runtime_guard(root)
        yield dict(root=root, guard=guard, identity=identity, launcher=launcher,
                   shell=shell, original_shell=original_shell, inventory=inventory,
                   snapshot=snapshot_path)
    finally:
        dc.EVENT_CONTEXT.reset(token)
        launcher.stdin.close()
        launcher.wait(timeout=5)
        for proc in shells:
            if proc.poll() is None:
                proc.stdin.close()
                proc.wait(timeout=5)


def prepare(fixture, now=None):
    guard = fixture['guard']
    row = guard.get('fixture')
    now = time.time() if now is None else now
    assert guard.observe('fixture', row['generation'], 'unknown', now=now)
    row = guard.get('fixture')
    assert guard.tick(fixture['identity'], now=row['due'] - .01) is None
    assert guard.tick(fixture['identity'], busy=True, now=row['due']) is None
    wake = guard.tick(fixture['identity'], now=row['due'])
    assert guard.begin(fixture['identity'], wake, now=row['due'])
    row = guard.get('fixture')
    root = fixture['root']
    observation = root / ('observation-' + str(row['generation']))
    observation.write_text('Fixture effects inspected: no work performed.', encoding='utf-8')
    report = {key: row[key] for key in ('task', 'generation', 'identity', 'action_id', 'effect_id')}
    report.update(effects=[dict(effect_id=row['effect_id'], status='no_effect')],
                  observations=[dict(path=str(observation), sha256=dc.digest(observation))])
    path = root / ('report-' + str(row['generation']))
    path.write_text(json.dumps(report), encoding='utf-8')
    route = copy.deepcopy(row['worker_route'])
    packet = root / ('packet-' + str(row['generation']))
    packet.write_bytes(Path(route['packet']).read_bytes())
    route.update(packet=str(packet), packet_sha256=dc.digest(packet),
                 receipt=str(root / ('receipt-' + str(row['generation']))))
    artifact = str(root / ('result-' + str(row['generation'])))
    route['argv'][route['argv'].index('--output-last-message') + 1] = artifact
    route['argv'][-1] = 'fresh inspection ' + str(row['generation'])
    return dict(identity=row['identity'], task=row['task'], generation=row['generation'],
                report_path=str(path), report_sha256=dc.digest(path),
                strategy=dict(action=route['argv'][-1], artifact=artifact, worker_route=route))


def cli(fixture, operation, payload):
    # Fresh interpreter, fresh native registry; only inventory transport replaced.
    bootstrap = ('import json,sys; from hermes_cli import diggr_continuation as dc; '
                 'from tools.process_registry import process_registry; '
                 'assert process_registry.get("proc_fixture") is None; '
                 'snapshot=json.load(open(sys.argv.pop(1),encoding="utf-8")); '
                 'dc.cmux_snapshot=lambda target:snapshot; '
                 'binding=json.loads(sys.argv.pop(1)); '
                 'dc.process_identity=lambda pid:dict(binding["shell"],tpgid=pid); '
                 'raise SystemExit(dc.main(sys.argv[1:]))')
    return subprocess.run([sys.executable, '-B', '-c', bootstrap, str(fixture['snapshot']),
                           json.dumps(dc.bind_visible_target(fixture['guard'].get('fixture')['visible_binding']['target'])),
                           '--home', str(fixture['root']), operation, '--payload-json',
                           json.dumps(payload)], text=True, encoding='utf-8', capture_output=True,
                          env=dict(os.environ, HERMES_HOME=str(fixture['root']),
                                   PYTHONDONTWRITEBYTECODE='1'), timeout=20)


@pytest.mark.parametrize('recovery', ['live', 'exited'], indirect=True)
def test_separate_cli_recovers_exited_native_launcher(recovery):
    recovery['launcher'].stdin.close()
    recovery['launcher'].wait(timeout=5)
    worker = child()
    binding = dict(pid=worker.pid, created=psutil.Process(worker.pid).create_time())
    worker.stdin.close()
    worker.wait(timeout=5)
    with recovery['guard'].transaction() as tasks:
        tasks['fixture'].update(worker_pid=worker.pid, worker_identity=binding)
    payload = prepare(recovery)
    result = cli(recovery, 'recover', payload)
    assert result.returncode == 0, result.stdout + result.stderr
    fresh = recovery['guard'].get('fixture')
    assert fresh['generation'] > payload['generation']
    assert len(fresh['attempt_history']) == 1
    assert cli(recovery, 'recover', payload).returncode != 0


def test_concurrent_cli_recovery_issues_exactly_one_fresh_ticket(recovery):
    from concurrent.futures import ThreadPoolExecutor
    recovery['launcher'].stdin.close()
    recovery['launcher'].wait(timeout=5)
    payload = prepare(recovery)
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda _: cli(recovery, 'recover', payload), range(4)))
    assert sum(result.returncode == 0 for result in results) == 1
    assert len(recovery['guard'].get('fixture')['attempt_history']) == 1


def test_hard_stop_dominates_late_worker_observation(recovery):
    guard = recovery['guard']
    with guard.transaction() as tasks:
        tasks['fixture']['hard_stop'] = time.time() - 1
    row = guard.get('fixture')
    assert not guard.observe('fixture', row['generation'], 'unknown')
    assert guard.get('fixture')['status'] == 'paused'
    assert guard.get('fixture')['recovery_attempt'] == 0


@pytest.mark.parametrize('control', ['paused', 'cancelled', 'superseded', 'hard_stop'])
def test_restarted_recovery_respects_user_control(recovery, control):
    recovery['launcher'].stdin.close()
    recovery['launcher'].wait(timeout=5)
    payload = prepare(recovery)
    guard = recovery['guard']
    if control == 'hard_stop':
        with guard.transaction() as tasks:
            tasks['fixture']['hard_stop'] = time.time() - 1
    else:
        guard.control(recovery['identity'], control)
    assert cli(recovery, 'recover', payload).returncode != 0
    assert not Path(payload['strategy']['artifact'] + '.reservation').exists()
    assert guard.tick(recovery['identity'], now=time.time() + 10000) is None


@pytest.mark.parametrize('defect', ['foreign_session', 'wrong_start', 'unknown_effect', 'changed_model'])
def test_restart_rejects_mismatched_authority_and_effects(recovery, defect):
    recovery['launcher'].stdin.close()
    recovery['launcher'].wait(timeout=5)
    payload = prepare(recovery)
    guard = recovery['guard']
    if defect in ('foreign_session', 'wrong_start'):
        with guard.transaction() as tasks:
            proof = tasks['fixture']['launcher_identity']
            if defect == 'foreign_session':
                proof['session_key'] = 'foreign'
            else:
                proof['started_at'] -= 1
    elif defect == 'unknown_effect':
        path = Path(payload['report_path'])
        report = json.loads(path.read_text(encoding='utf-8'))
        report['effects'][0]['status'] = 'unknown'
        path.write_text(json.dumps(report), encoding='utf-8')
        payload['report_sha256'] = dc.digest(path)
    else:
        route = payload['strategy']['worker_route']
        path = Path(route['packet'])
        packet = json.loads(path.read_text(encoding='utf-8'))
        packet['coding_route']['required_model'] = 'other'
        path.write_text(json.dumps(packet), encoding='utf-8')
        route['packet_sha256'] = dc.digest(path)
    assert cli(recovery, 'recover', payload).returncode != 0
    assert not Path(payload['strategy']['artifact'] + '.reservation').exists()


def test_separate_cli_never_assumes_live_or_missing_launcher_exited(recovery):
    payload = prepare(recovery)
    assert cli(recovery, 'recover', payload).returncode != 0

    with recovery['guard'].transaction() as tasks:
        tasks['fixture'].pop('launcher_identity', None)
    recovery['launcher'].stdin.close()
    recovery['launcher'].wait(timeout=5)
    assert cli(recovery, 'recover', payload).returncode != 0


def test_unknown_worker_failures_back_off_across_epochs_and_restarts(recovery):
    recovery['launcher'].stdin.close()
    recovery['launcher'].wait(timeout=5)
    guard = recovery['guard']
    now = time.time()
    delays, effects = [], set()
    for attempt in range(1, 41):
        worker = child()
        binding = dict(pid=worker.pid, created=psutil.Process(worker.pid).create_time())
        worker.stdin.close()
        worker.wait(timeout=5)
        with guard.transaction() as tasks:
            tasks['fixture'].update(worker_pid=worker.pid, worker_identity=binding)
        payload = prepare(recovery, now=now)
        row = guard.get('fixture')
        delays.append(row['due'] - now)
        assert 0 < delays[-1] <= 300, 'unknown worker outcomes require bounded backoff'
        assert delays == sorted(delays)
        assert row['recovery_attempt'] == attempt
        assert row['effect_id'] not in effects
        effects.add(row['effect_id'])
        assert row['epoch'] >= attempt
        restarted = dc.Guard(guard.path)
        assert restarted.tick(row['identity'], now=row['due']) is None
        ticket = restarted.recover(row['identity'], payload, payload['report_path'],
                                   payload['report_sha256'], payload['strategy'], now=row['due'])
        assert not restarted.observe('fixture', payload['generation'], 'unknown', now=row['due'])
        with pytest.raises(ValueError):
            restarted.recover(row['identity'], payload, payload['report_path'],
                              payload['report_sha256'], payload['strategy'], now=row['due'])
        assert not restarted.get('fixture').get('visible_sent')
        assert ticket['generation'] > row['generation']
        now = row['due'] + 1
    assert delays[-1] == 300
    assert len(guard.get('fixture')['attempt_history']) == 40
    assert guard.get('fixture')['status'] == 'running'


@pytest.mark.parametrize('legacy', [False, True])
def test_stale_shell_recovers_only_after_old_shell_exited(recovery, legacy):
    recovery['launcher'].stdin.close()
    recovery['launcher'].wait(timeout=5)
    payload = prepare(recovery)
    if legacy:
        with recovery['guard'].transaction() as tasks:
            tasks['fixture'].pop('visible_shell_identity', None)
    old = recovery['guard'].get('fixture')
    replacement = recovery['shell']()
    recovery['inventory'](replacement)
    # A second target inventory must not bypass a still live original shell.
    assert cli(recovery, 'recover', payload).returncode != 0
    recovery['original_shell'].stdin.close()
    recovery['original_shell'].wait(timeout=5)
    snapshot = json.loads(recovery['snapshot'].read_text(encoding='utf-8'))
    snapshot['children'][0]['foreground_pgids'] = []
    recovery['snapshot'].write_text(json.dumps(snapshot), encoding='utf-8')
    assert cli(recovery, 'recover', payload).returncode != 0
    recovery['inventory'](replacement)
    result = cli(recovery, 'recover', payload)
    assert result.returncode == 0, result.stdout + result.stderr
    fresh = recovery['guard'].get('fixture')
    assert fresh['visible_binding']['shell']['pid'] == replacement.pid
    assert fresh['identity'] == old['identity']
    assert fresh['visible_binding']['target'] == old['visible_binding']['target']
    assert fresh['checkpoint']['visible_binding'] == old['visible_binding']
    assert fresh['worker_route']['receipt'] != old['worker_route']['receipt']
    assert cli(recovery, 'recover', payload).returncode != 0
    assert cli(recovery, 'route-preflight', dict(ticket=dc.worker_ticket(old),
                                                route=old['worker_route'])).returncode != 0
    result = cli(recovery, 'route-preflight', dict(ticket=dc.worker_ticket(fresh),
                                                 route=fresh['worker_route']))
    assert result.returncode == 0, result.stdout + result.stderr
    assert recovery['guard'].get('fixture')['receipt_sha256'] == dc.digest(fresh['worker_route']['receipt'])
