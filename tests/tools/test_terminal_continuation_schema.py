"""The model-visible contract must express a guarded native launch."""
from unittest.mock import Mock
import pytest
from tools.terminal_tool import TERMINAL_SCHEMA
from hermes_cli import diggr_continuation as dc


def payload():
    return dict(owner_batch='confirmed-batch', owner_issue='workspace/project/issue',
                launcher_command='registered-launcher', producer='cmux', task='bounded-task',
                scope='one issue', owner='main', gate='coding', action='inspect',
                artifact='/tmp/unique-result.md', deadline=2000000000, wake_budget=1,
                authorization='existing-owner-grant', worker_route=dict(
                    packet='/tmp/packet.json', receipt='/tmp/receipt.json',
                    worktree='/tmp/work', branch='fixture', plane_id='APP-109',
                    visible_target=dict(workspace='workspace', surface='surface'),
                    argv=['codex', 'exec']))


def test_declared_launch_reaches_native_authority_check_without_execution():
    schema = TERMINAL_SCHEMA['parameters']['properties']['continuation']
    task = payload()
    # Fields absent from properties cannot be reliably supplied by model tools.
    assert set(task) <= set(schema['properties'])
    assert set(task['worker_route']) <= set(schema['properties']['worker_route']['properties'])
    assert set(schema["required"]) <= set(task)
    invoke = Mock()
    token = dc.EVENT_CONTEXT.set(None)
    try:
        with pytest.raises(ValueError, match='requires native Main event context'):
            dc.terminal_dispatch(dict(command=task['launcher_command'],
                                      background=True, continuation=task), invoke)
    finally:
        dc.EVENT_CONTEXT.reset(token)
    invoke.assert_not_called()


@pytest.mark.parametrize('field', ['producer', 'authorization', 'worker_route', 'launcher_command'])
def test_incomplete_launch_rejected_by_tool_contract(field):
    task = payload()
    del task[field]
    schema = TERMINAL_SCHEMA['parameters']['properties']['continuation']
    assert field in schema['required']
    assert not set(schema['required']) <= set(task)
