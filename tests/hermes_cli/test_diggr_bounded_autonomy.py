"""Owner-policy admission contract; missing trusted bindings must fail closed.

These tests do not invent a grant issuer, seed fabricated budget provenance, or
launch any worker. All registration/state is disposable. A caller-supplied
policy object is deliberately NOT treated as native owner authority.
"""
from pathlib import Path

import pytest
from hermes_cli import diggr_continuation as dc


@pytest.fixture
def admission(tmp_path, monkeypatch):
    now = 1000.0
    monkeypatch.setattr(dc.time, 'time', lambda: now)
    identity = dict(home=str(tmp_path), profile='diggr-main', session='fixture',
                    session_key='fixture', platform='telegram', chat_id='564628210',
                    user_id='564628210', thread_id='')
    guard = dc.Guard(tmp_path / 'state.json')
    monkeypatch.setattr(dc, 'runtime_guard', lambda home: guard)
    token = dc.EVENT_CONTEXT.set(dict(identity=identity))
    task = dict(task='fixture-todo', scope='fixture inspection only', identity=identity,
                owner='coding', gate='coding', action='inspect fixture', producer='pilot',
                artifact=str(tmp_path / 'result.txt'), deadline=now + 60, wake_budget=10,
                authorization='executor-provided-reference')
    try:
        yield guard, task, now
    finally:
        dc.EVENT_CONTEXT.reset(token)


@pytest.mark.parametrize('supplied_policy', [False, True])
def test_identity_context_is_not_logical_todo_and_batch_authorization(admission, supplied_policy):
    guard, task, now = admission
    if supplied_policy:
        # Finite caller claims cannot become trusted simply by matching policy numbers.
        task.update(hard_stop=now + 3600, authorization_expires_at=now + 3600,
                    logical_todo_id='caller-todo', batch_id='caller-batch',
                    batch_deadline=now + 7200)
    with pytest.raises(ValueError):
        dc.register_native(task)
    assert not guard.path.exists()


def test_direct_guard_admission_cannot_bypass_missing_owner_binding(admission):
    guard, task, now = admission
    with pytest.raises(ValueError):
        guard.register(task, now=now)
    assert not guard.path.exists()


def test_legacy_without_trustworthy_budget_history_cannot_renew(admission):
    guard, task, now = admission
    # An existing legacy record, not an invented new owner authorization.
    legacy = dict(task, generation=1, status='running', wakes=0)
    before = legacy['deadline']
    assert dc.Guard.renew(legacy, before) is False
    assert legacy['deadline'] == before


def test_explicit_stop_cannot_be_bypassed_by_new_executor_task_name(admission):
    guard, task, now = admission
    # Reproduce a pre-policy record, so the stop gate is tested independently of admission.
    with guard.transaction() as rows:
        rows[task['task']] = dict(task, generation=1, status='running', wakes=0)
    guard.control(task['identity'], 'paused')
    before = guard.path.read_bytes()
    renamed = dict(task, task='renamed-fixture-todo', artifact=str(Path(task['artifact']).with_name('other.txt')))
    with pytest.raises(ValueError):
        dc.register_native(renamed)
    assert guard.path.read_bytes() == before


def test_existing_native_context_requirement_remains_closed(admission):
    guard, task, now = admission
    dc.EVENT_CONTEXT.set(None)
    with pytest.raises(ValueError, match='native Main event'):
        dc.register_native(task)
    assert not guard.path.exists()


def test_existing_terminal_legacy_record_does_not_revive(admission):
    guard, task, now = admission
    legacy = dict(task, generation=1, status='blocked', wakes=0)
    assert dc.Guard.renew(legacy, now + 7200) is False
    assert legacy['status'] == 'blocked'
