from tests.diggr_owner_fixtures import authorize
import concurrent.futures
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from hermes_cli.diggr_continuation import Guard


def race(args):
    path, identity = args
    return Guard(path).tick(identity, now=20)




class GuardTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(Guard, 'evidence-bound continuation guard is not implemented')
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()
        self.path = self.root / 'guard.json'
        self.identity = dict(home=str(self.root), profile='diggr-main', session='disposable',
                             session_key='telegram:test', platform='telegram', chat_id='test',
                             user_id='tester', thread_id='')
        self.g = Guard(self.path)
        self.task = dict(task='SYSTEM167-smoke', scope='write disposable artifact only',
                         identity=self.identity, owner='coding', gate='coding',
                         action='inspect coding evidence', artifact=str(self.root / 'result.txt'),
                         deadline=1000, wake_budget=3)
        self.g.register(authorize(self.g, self.task), now=1)

    def evidence(self, generation=1, result='ready_for_main_review'):
        artifact = Path(self.task['artifact'])
        artifact.write_text('harmless artifact\n')
        return dict(task=self.task['task'], generation=generation, action='inspect coding evidence',
                    artifact=str(artifact), sha256=hashlib.sha256(artifact.read_bytes()).hexdigest(), result=result)

    def completed(self):
        self.g.observe(self.task['task'], 1, 'completed', self.evidence(), now=2)

    def test_abandoned_completion_transfers_to_main_validation(self):
        self.completed()
        wake = self.g.tick(self.identity, now=20)
        self.assertEqual(wake['gate'], 'main_validation')
        self.assertEqual(wake['owner'], 'main')
        self.assertTrue(self.g.blocks(self.identity))

    def test_artifact_missing_result_requires_reconcile(self):
        self.g.observe(self.task['task'], 1, 'completed', self.evidence(result=''), now=2)
        self.assertEqual(self.g.tick(self.identity, now=20)['gate'], 'reconcile')

    def test_stale_result(self):
        self.assertFalse(self.g.observe(self.task['task'], 0, 'completed', self.evidence(0), now=2))
        self.assertIsNone(self.g.tick(self.identity, now=20))

    def test_process_crash_reconciles_not_reruns(self):
        self.g.observe(self.task['task'], 1, 'unknown', None, now=2)
        self.assertEqual(self.g.tick(self.identity, now=20)['gate'], 'reconcile')

    def test_abandoned_claim_requires_reconciliation(self):
        self.completed()
        wake = self.g.tick(self.identity, now=20)
        self.assertTrue(self.g.begin(self.identity, wake, now=21))
        # Simulated owner loss: reload only disposable durable state, no process.
        recovered = Guard(self.path).tick(self.identity, now=100)
        self.assertEqual(recovered['gate'], 'reconcile')
        self.assertFalse(self.g.begin(self.identity, wake, now=101))

    def test_changed_artifact_cannot_ack(self):
        self.completed()
        wake = self.g.tick(self.identity, now=20)
        self.assertTrue(self.g.begin(self.identity, wake, now=21))
        receipt = self.evidence(wake['generation'], 'validated')
        receipt['action'] = wake['action']
        Path(self.task['artifact']).write_text('changed after receipt')
        self.assertFalse(self.g.ack(self.identity, wake, receipt, 'main_live', 'authorized gate', now=22))

    def test_slow_provider_not_complete(self):
        self.g.observe(self.task['task'], 1, 'running', None, now=2)
        self.assertIsNone(self.g.tick(self.identity, now=200))
        self.assertTrue(self.g.blocks(self.identity))

    def test_busy_main_pending_wake(self):
        self.completed()
        self.assertIsNone(self.g.tick(self.identity, busy=True, now=20))
        self.assertIsNotNone(self.g.tick(self.identity, now=21))

    def test_concurrent_locked_ticks_one_wake(self):
        self.completed()
        with concurrent.futures.ThreadPoolExecutor(4) as pool:
            wakes = list(pool.map(race, [(str(self.path), self.identity)] * 8))
        self.assertEqual(sum(w is not None for w in wakes), 1)

    def test_restart_queued_generation_not_duplicate(self):
        self.completed()
        wake = self.g.tick(self.identity, now=20)
        other = Guard(self.path)
        self.assertIsNone(other.tick(self.identity, now=21))
        recovered = other.tick(self.identity, now=100)
        self.assertEqual(recovered['gate'], 'reconcile')
        self.assertGreater(recovered['generation'], wake['generation'])
        self.assertFalse(other.begin(self.identity, wake, now=101))

    def test_stop_pause_cancel_supersede_dominate(self):
        for state in ('paused', 'cancelled', 'superseded'):
            with self.subTest(state=state):
                self.g.control(self.identity, state)
                self.assertIsNone(self.g.tick(self.identity, now=20))
                self.assertFalse(self.g.observe(self.task['task'], 1, 'completed', self.evidence(), now=21))

    def test_no_work_silent_and_foreign_identity_refused(self):
        foreign = dict(self.identity, session='foreign')
        self.assertIsNone(self.g.tick(foreign, now=20))
        self.assertFalse(self.g.blocks(foreign))
        self.completed()
        wake = self.g.tick(self.identity, now=20)
        self.assertFalse(self.g.begin(foreign, wake, now=21))

    def test_budget_deadline(self):
        self.completed()
        self.assertIsNotNone(self.g.tick(self.identity, now=1001))
        self.assertGreater(self.g.get(self.task['task'])['deadline'], 1001)

    def test_notification_not_progress_abandoned_live_gate(self):
        self.completed()
        wake = self.g.tick(self.identity, now=20)
        self.assertTrue(self.g.begin(self.identity, wake, now=21))
        self.assertFalse(self.g.ack(self.identity, wake, None, 'main_live', 'perform authorized live gate', now=22))
        receipt = self.evidence(wake['generation'])
        receipt['action'] = wake['action']
        receipt['result'] = 'validated'
        self.assertTrue(self.g.ack(self.identity, wake, receipt, 'main_live', 'perform authorized live gate', now=22))
        live = self.g.tick(self.identity, now=40)
        self.assertEqual(live['gate'], 'main_live')
        self.assertTrue(self.g.begin(self.identity, live, now=41))
        self.assertIsNone(self.g.tick(self.identity, busy=True, now=200))
        self.assertEqual(self.g.tick(self.identity, now=201)['gate'], 'reconcile')

    def test_done_requires_separate_bound_final_artifact(self):
        self.completed()
        wake = self.g.tick(self.identity, now=20)
        self.assertTrue(self.g.begin(self.identity, wake, now=21))
        receipt = self.evidence(wake['generation'], 'validated')
        receipt['action'] = wake['action']
        self.assertFalse(self.g.ack(self.identity, wake, receipt, 'done', 'done', now=22))
        self.assertFalse(self.g.ack(self.identity, wake, receipt, 'awaiting_user', 'wait', now=22))
        self.assertTrue(self.g.ack(self.identity, wake, receipt, 'main_live', 'final check', now=22))
        wake = self.g.tick(self.identity, now=40)
        self.assertTrue(self.g.begin(self.identity, wake, now=41))
        with self.g.transaction() as state:
            state[wake['task']]['authorization'] = 'isolated authorized final check'
        row = self.g.get(wake['task'])
        receipt = self.evidence(row['generation'], 'validated')
        receipt.update(action=row['action'], authorization=row['authorization'], final_gate='passed')
        self.assertFalse(self.g.ack(self.identity, wake, receipt, 'done', 'done', now=42))
        import json
        final = {k: row[k] for k in ('task', 'generation', 'action', 'authorization')}
        final.update(result='passed', checks=['harmless check passed'])
        final_path = self.root / 'final.json'
        final_path.write_text(json.dumps(final))
        receipt.update(final_artifact=str(final_path), final_sha256=hashlib.sha256(final_path.read_bytes()).hexdigest())
        self.assertTrue(self.g.ack(self.identity, wake, receipt, 'done', 'done', now=43))
        self.assertEqual(self.g.get(wake['task'])['status'], 'done')
        self.assertFalse(self.g.blocks(self.identity))

    def test_wake_budget_renews_across_generations(self):
        self.completed()
        self.assertIsNotNone(self.g.tick(self.identity, now=20))
        self.assertIsNotNone(self.g.tick(self.identity, now=100))
        self.assertIsNotNone(self.g.tick(self.identity, now=200))
        self.assertIsNotNone(self.g.tick(self.identity, now=400))
        self.assertEqual(self.g.get(self.task['task'])['wakes'], 4)
        self.assertEqual(self.g.get(self.task['task'])['epoch'], 2)

if __name__ == '__main__':
    unittest.main(verbosity=2)
