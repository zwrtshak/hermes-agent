"""Review claims are role-pure; technical rework uses fresh same-card runs."""

import json

import pytest

from hermes_cli import kanban_db as kb
from tools import kanban_tools as kt


@pytest.fixture
def review(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    for key in (
        "HERMES_KANBAN_DB",
        "HERMES_KANBAN_BOARD",
        "HERMES_KANBAN_TASK",
        "HERMES_KANBAN_RUN_ID",
    ):
        monkeypatch.delenv(key, raising=False)
    conn = kb.connect()
    tid = kb.create_task(conn, title="implementation", assignee="implementer")
    impl = kb.claim_task(conn, tid)
    assert kb.request_review(
        conn,
        tid,
        reviewer="reviewer",
        summary="tests pass",
        expected_run_id=impl.current_run_id,
    )
    claimed = kb.claim_review_task(conn, tid)
    monkeypatch.setenv("HERMES_KANBAN_TASK", tid)
    monkeypatch.setenv("HERMES_KANBAN_RUN_ID", str(claimed.current_run_id))
    yield conn, tid, claimed.current_run_id
    conn.close()


def test_review_cannot_request_nested_review(review):
    conn, tid, rid = review
    ok, reason = kb.request_review(
        conn, tid, reviewer="another-reviewer", expected_run_id=rid, with_reason=True
    )
    assert not ok
    assert "review-only" in reason
    assert kb.get_task(conn, tid).current_run_id == rid
    assert kb.get_task(conn, tid).assignee == "reviewer"


@pytest.mark.parametrize(
    "operation,args",
    [
        (kt._handle_create, {"title": "repair", "assignee": "implementer"}),
        (
            kt._handle_request_review,
            {"summary": "review again", "reviewer": "reviewer"},
        ),
        (kt._handle_link, {"parent_id": "other", "child_id": "other2"}),
        (kt._handle_block, {"reason": "technical defects", "kind": "transient"}),
    ],
)
def test_review_tools_reject_role_escape(review, operation, args):
    conn, tid, rid = review
    count = conn.execute("SELECT count(*) FROM tasks").fetchone()[0]
    result = json.loads(operation(args))
    assert "review-only" in result.get("error", "")
    assert kb.get_task(conn, tid).current_run_id == rid
    assert conn.execute("SELECT count(*) FROM tasks").fetchone()[0] == count


@pytest.mark.parametrize(
    "name,args",
    [
        ("write_file", {"path": "forbidden.py", "content": "repair"}),
        (
            "patch",
            {
                "mode": "replace",
                "path": "forbidden.py",
                "old_string": "a",
                "new_string": "b",
            },
        ),
        ("delegate_task", {"goal": "repair defects"}),
        ("execute_code", {"code": "print('repair')"}),
    ],
)
def test_dispatch_rejects_implementation_tools(review, name, args, monkeypatch):
    from model_tools import handle_function_call, registry

    monkeypatch.setattr(
        registry, "dispatch", lambda *a, **kw: json.dumps({"dispatched": True})
    )
    result = json.loads(handle_function_call(name, args))
    assert "review-only" in result.get("error", "")


def test_agent_loop_delegate_cannot_bypass_review_guard(review, monkeypatch):
    from types import SimpleNamespace
    from run_agent import AIAgent
    import tools.delegate_tool as dt

    monkeypatch.setattr(dt, "delegate_task", lambda **kw: json.dumps({"spawned": True}))
    result = AIAgent._dispatch_delegate_task(SimpleNamespace(), {"goal": "repair"})
    assert "review-only" in json.loads(result).get("error", "")


def test_rework_round_trip_preserves_evidence_and_gates_children(review, monkeypatch):
    import base64

    conn, tid, first_review = review
    child = kb.create_task(conn, title="downstream QA", assignee="qa", parents=[tid])
    assert kb.get_task(conn, child).status == "todo"
    evidence = b"repro: fallback branch returns the wrong status"
    attached = json.loads(
        kt._handle_attach({
            "filename": "repro.txt",
            "content_type": "text/plain",
            "content_base64": base64.b64encode(evidence).decode(),
        })
    )
    assert attached.get("ok"), attached
    reason = "Correct the fallback status; repro.txt demonstrates the defect."
    assert json.loads(kt._handle_request_changes({"reason": reason})).get("ok")
    assert kb.latest_run(conn, tid).summary == reason
    assert kb.get_task(conn, tid).assignee == "implementer"
    assert kb.get_task(conn, tid).status == "ready"
    # The old reviewer must not take up repair/orchestration after handoff.
    assert (
        "review-only"
        in json.loads(
            kt._handle_create({"title": "repair", "assignee": "implementer"})
        )["error"]
    )
    kb.recompute_ready(conn)
    assert kb.claim_task(conn, child) is None
    repaired = kb.claim_task(conn, tid)
    monkeypatch.setenv("HERMES_KANBAN_RUN_ID", str(repaired.current_run_id))
    assert json.loads(
        kt._handle_request_review({"summary": "Fallback fixed; regression passes"})
    ).get("ok")
    assert kb.get_task(conn, tid).assignee == "reviewer"
    kb.recompute_ready(conn)
    assert kb.claim_task(conn, child) is None
    second = kb.claim_review_task(conn, tid)
    assert second.current_run_id != first_review
    monkeypatch.setenv("HERMES_KANBAN_RUN_ID", str(second.current_run_id))
    attachments = json.loads(kt._handle_attachments({}))
    assert "repro.txt" in json.dumps(attachments)
    assert json.loads(
        kt._handle_complete({"summary": "PASS: verified fallback regression"})
    ).get("ok")
    kb.recompute_ready(conn)
    assert kb.claim_task(conn, child) is not None


def test_exhausted_stale_run_cannot_close_successor(review):
    conn, tid, rid = review
    assert kb.request_changes(conn, tid, reason="Fix status", expected_run_id=rid)[0]
    successor = kb.claim_task(conn, tid)
    assert not kb.record_iteration_exhaustion(
        conn,
        tid,
        expected_run_id=rid,
        summary="old checkpoint",
        budget_used=150,
        budget_max=150,
    )
    assert kb.get_task(conn, tid).current_run_id == successor.current_run_id
    assert kb.get_task(conn, tid).status == "running"


@pytest.mark.parametrize("kind", ["dependency", "needs_input", "capability"])
def test_review_may_block_for_explicit_external_prerequisite(review, kind):
    result = json.loads(
        kt._handle_block({"kind": kind, "reason": "External service unavailable"})
    )
    assert result.get("ok"), result


@pytest.mark.parametrize("name", ["read_file", "search_files", "terminal"])
def test_review_keeps_read_and_test_tools(review, monkeypatch, name):
    from model_tools import handle_function_call, registry

    monkeypatch.setattr(
        registry, "dispatch", lambda *a, **kw: json.dumps({"dispatched": True})
    )
    assert json.loads(handle_function_call(name, {})).get("dispatched")


def test_review_cannot_escape_by_selecting_another_board(review):
    result = json.loads(
        kt._handle_create({
            "title": "repair",
            "assignee": "implementer",
            "board": "elsewhere",
        })
    )
    assert "review-only" in result.get("error", "")
