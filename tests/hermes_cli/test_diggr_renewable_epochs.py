"""Durable renewable continuation contracts; all state is disposable."""
from pathlib import Path
from hermes_cli import diggr_continuation as dc


def registered(tmp_path, **overrides):
    tmp_path = tmp_path.resolve()
    identity = dict(home=str(tmp_path), profile='diggr-main', session='test',
                    session_key='test', platform='telegram', chat_id='fixture',
                    user_id='fixture', thread_id='')
    task = dict(task='test', scope='disposable evidence', identity=identity,
                owner='coding', gate='coding', action='inspect',
                artifact=str(tmp_path / 'result'), deadline=1000,
                wake_budget=1, authorization='fixture-only')
    task.update(overrides)
    guard = dc.Guard(tmp_path / 'state.json')
    guard.register(task, now=-9)
    guard.observe('test', 1, 'unknown', now=-8)
    return guard, identity


def test_exhausted_one_wake_renews_instead_of_blocks(tmp_path):
    guard, identity = registered(tmp_path)
    first = guard.tick(identity, now=3)
    assert first is not None
    second = guard.tick(identity, now=100)
    assert second is not None, 'exhausted wake_budget=1 must schedule continuation'
    assert second['status'] == 'queued'
    assert second['epoch'] == first['epoch'] + 1
    assert second['epoch_wakes'] == 1
    assert second['wakes'] == 2
    assert second['checkpoint']['identity'] == identity


import tempfile
import unittest
from unittest.mock import patch


class RenewableEpochTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()

    def test_budget(self):
        test_exhausted_one_wake_renews_instead_of_blocks(self.root)

    def test_deadline_renews_at_begin_and_ack(self):
        guard, identity = registered(self.root, deadline=10)
        wake = guard.tick(identity, now=3)
        self.assertTrue(guard.begin(identity, wake, now=11))
        row = guard.get('test')
        self.assertGreater(row['deadline'], 11)
        self.assertEqual(row['generation'], wake['generation'])
        Path(row['artifact']).write_text('validated')
        evidence = {k: row[k] for k in ('task', 'generation', 'action', 'artifact')}
        evidence.update(sha256=dc.digest(row['artifact']), result='validated')
        self.assertTrue(guard.ack(identity, wake, evidence, 'main_live', 'validate', now=21))
        self.assertGreater(guard.get('test')['deadline'], 21)

    def test_hard_stop_never_renews(self):
        guard, identity = registered(self.root, deadline=10, hard_stop=12)
        wake = guard.tick(identity, now=3)
        self.assertFalse(guard.begin(identity, wake, now=12))
        self.assertEqual(guard.get('test')['status'], 'paused')
        self.assertIsNone(guard.tick(identity, now=100))

    def test_live_ticket_survives_renewal(self):
        guard, identity = registered(self.root, deadline=10)
        with guard.transaction() as tasks:
            row = tasks['test']
            row.update(status='running', ticket_nonce='fixture')
            ticket = dc.worker_ticket(row)
        with patch.object(dc.time, 'time', return_value=11):
            with guard.transaction() as tasks:
                row = dc.validate_ticket(tasks, ticket)
                self.assertGreater(row['deadline'], 11)
                self.assertEqual(dc.worker_ticket(row), ticket)
        self.assertIsNone(guard.tick(identity, now=100))
        self.assertEqual(guard.get('test')['generation'], ticket['generation'])

    def test_terminal_legacy_rows_never_revive(self):
        guard, identity = registered(self.root)
        for status in dc.TERMINAL:
            with guard.transaction() as tasks:
                tasks['test']['status'] = status
                tasks['test'].pop('schema_version', None)
            self.assertIsNone(guard.tick(identity, now=9999))
            self.assertEqual(guard.get('test')['status'], status)

    def receipt(self, row, content='validated'):
        Path(row['artifact']).write_text(content)
        proof = {k: row[k] for k in ('task', 'generation', 'action', 'artifact')}
        return dict(proof, sha256=dc.digest(row['artifact']), result='validated')

    def test_progress_epochs_and_no_progress_backoff(self):
        guard, identity = registered(self.root)
        for index in range(12):
            now = 3 + index * 400
            wake = guard.tick(identity, now=now)
            self.assertIsNotNone(wake)
            self.assertTrue(guard.begin(identity, wake, now=now + 1))
            receipt = self.receipt(wake, str(index))
            self.assertTrue(guard.ack(identity, wake, receipt, 'main_live', 'validate', now=now + 2))
            row = guard.get('test')
            self.assertEqual(row['recovery_attempt'], 0)
            self.assertIsNotNone(row['progress_fingerprint'])
        wake = guard.tick(identity, now=5000)
        guard.begin(identity, wake, now=5001)
        self.assertTrue(guard.ack(identity, wake, self.receipt(wake, '11'), 'main_live', 'validate', now=5002))
        row = guard.get('test')
        self.assertEqual(row['gate'], 'reconcile')
        self.assertGreater(row['recovery_attempt'], 0)
        self.assertGreater(row['due'], 5002)
        self.assertIsNone(guard.tick(identity, now=5002))

    def test_structured_external_blockers_only(self):
        guard, identity = registered(self.root)
        wake = guard.tick(identity, now=3)
        guard.begin(identity, wake, now=4)
        receipt = self.receipt(wake)
        receipt['human_decision'] = 'quota exhausted'
        self.assertFalse(guard.ack(identity, wake, receipt, 'awaiting_user', 'wait', now=5))
        for category in ('quota', 'timeout', 'tests', 'merge_conflict'):
            receipt['blocker'] = dict(category=category, evidence='technical error')
            self.assertFalse(guard.ack(identity, wake, receipt, 'awaiting_user', 'wait', now=5))
        receipt['blocker'] = dict(category='credentials', evidence='Required credential unavailable in authorized scope')
        self.assertTrue(guard.ack(identity, wake, receipt, 'awaiting_user', 'request credential authority', now=5))

    def test_technical_retry_respects_reset_and_never_terminal(self):
        guard, identity = registered(self.root)
        wake = guard.tick(identity, now=3)
        guard.begin(identity, wake, now=4)
        report = dict(reason='quota', retry_at=500, detail='fixture rate limit')
        self.assertTrue(guard.retry(identity, wake, report, now=5))
        self.assertIsNone(guard.tick(identity, now=499))
        for now in range(500, 5000, 400):
            wake = guard.tick(identity, now=now)
            self.assertIsNotNone(wake)
            guard.begin(identity, wake, now=now+1)
            self.assertTrue(guard.retry(identity, wake, dict(reason='timeout', detail='bounded attempt timed out'), now=now+2))
            self.assertLessEqual(guard.get('test')['due'], now+302)
        self.assertNotIn(guard.get('test')['status'], dc.TERMINAL)

    def test_expired_receipt_and_begin_are_fenced_without_tick(self):
        guard, identity = registered(self.root)
        wake = guard.tick(identity, now=3)
        self.assertFalse(guard.begin(identity, wake, now=200))
        self.assertEqual(guard.get('test')['gate'], 'reconcile')
        wake = guard.tick(identity, now=400)
        self.assertTrue(guard.begin(identity, wake, now=401))
        self.assertFalse(guard.ack(identity, wake, self.receipt(wake), 'main_live', 'verify', now=500))

    def test_concurrent_ticks_and_restart_fence_one_claim(self):
        import concurrent.futures
        guard, identity = registered(self.root)
        def tick(_):
            return dc.Guard(guard.path).tick(identity, now=3)
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            results=list(pool.map(tick, range(16)))
        wakes=[wake for wake in results if wake]
        self.assertEqual(len(wakes),1)
        restarted=dc.Guard(guard.path)
        self.assertIsNone(restarted.tick(identity,now=4))
        replacement=restarted.tick(identity,now=100)
        self.assertIsNotNone(replacement)
        self.assertFalse(restarted.begin(identity,wakes[0],now=101))
        self.assertTrue(restarted.begin(identity,replacement,now=101))

    def test_independent_final_done_and_controls_dominate(self):
        import json
        for status in ('paused','cancelled','superseded'):
            home=self.root/status; home.mkdir()
            guard,identity=registered(home)
            wake=guard.tick(identity,now=3)
            guard.control(identity,status)
            self.assertFalse(guard.begin(identity,wake,now=4))
            self.assertIsNone(guard.tick(identity,now=9999))
        home=self.root/'done';home.mkdir()
        guard,identity=registered(home)
        wake=guard.tick(identity,now=3);guard.begin(identity,wake,now=4)
        self.assertFalse(guard.ack(identity,wake,self.receipt(wake),'done','done',now=5))
        guard.ack(identity,wake,self.receipt(wake),'main_live','final check',now=5)
        wake=guard.tick(identity,now=8);guard.begin(identity,wake,now=9)
        proof=self.receipt(wake)
        proof.update(final_gate='passed',authorization=wake['authorization'])
        final={k:wake[k] for k in ('task','generation','action','authorization')}
        final.update(result='passed',checks=['independent artifact verification'])
        final_path=home/'final.json';final_path.write_text(json.dumps(final))
        proof.update(final_artifact=str(final_path),final_sha256=dc.digest(final_path))
        self.assertTrue(guard.ack(identity,wake,proof,'done','done',now=10))
        self.assertIsNone(guard.tick(identity,now=9999))

    def test_registration_persists_version_and_rejects_nonfinite_bounds(self):
        import json
        guard,identity=registered(self.root)
        raw=json.loads(guard.path.read_text())['test']
        self.assertEqual(raw['schema_version'],2)
        for field,value in [('deadline',float('inf')),('hard_stop',float('nan')),('authorization_expires_at',float('inf'))]:
            home=self.root/field;home.mkdir()
            with self.assertRaises(ValueError): registered(home,**{field:value})

    def test_unknown_launcher_exit_is_not_recovery_permission(self):
        self.assertFalse(dc.prior_process_exited(dict(launcher_expected=True)))

    def test_exited_legacy_pid_enters_readonly_reconciliation(self):
        import subprocess, sys
        child=subprocess.Popen([sys.executable,'-c','pass'])
        child.wait(timeout=5)
        guard,identity=registered(self.root)
        with guard.transaction() as tasks:
            tasks['test'].update(status='running',worker_pid=child.pid)
        dc.observe_processes(guard)
        row=guard.get('test')
        self.assertEqual(row['gate'],'reconcile')
        self.assertEqual(row['status'],'pending')
        self.assertFalse(dc.prior_process_exited(row))

    def test_registration_cannot_supply_internal_epoch_authority(self):
        guard,_=registered(self.root,epoch_seconds=float('inf'),epoch_wakes=-100,
                           action_id='supplied',effect_id='supplied',recovery_attempt=-100)
        row=guard.get('test')
        self.assertLessEqual(row['epoch_seconds'],3600)
        self.assertEqual(row['epoch_wakes'],0)
        self.assertEqual(row['recovery_attempt'],1)
        self.assertNotEqual(row['action_id'],'supplied')
        self.assertNotEqual(row['effect_id'],'supplied')


if __name__ == '__main__':
    unittest.main(verbosity=2)
