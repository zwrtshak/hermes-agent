"""Runtime/Return-1 merge boundaries. All transports and processes are fakes."""
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from tests.gateway.test_telegram_thread_fallback import (
    _inject_fake_telegram, _make_adapter, FakeBadRequest,
)


@pytest.mark.asyncio
@pytest.mark.parametrize('strict,durable', [(True, False), (False, True), (True, True)])
async def test_strict_and_durable_routes_both_bypass_rich_and_root_fallback(strict, durable):
    adapter = _make_adapter()
    adapter._should_attempt_rich = Mock(return_value=True)
    adapter._try_send_rich = AsyncMock(side_effect=AssertionError('must retain bound legacy route'))
    adapter._bot = SimpleNamespace(send_message=AsyncMock(
        side_effect=FakeBadRequest('Message thread not found')))
    result = await adapter.send('-100123', 'synthetic completion', metadata={
        'thread_id': '77', 'strict_topic': strict, 'diggr_durable_delivery': durable,
    })
    assert result.success is False
    adapter._try_send_rich.assert_not_awaited()
    adapter._bot.send_message.assert_awaited_once()
    assert adapter._bot.send_message.await_args.kwargs['message_thread_id'] == 77
    if durable:
        assert result.raw_response['delivery_outcome'] == 'rejected'


def test_generated_terminal_wrapper_preserves_local_control_forwarding():
    from tools.code_execution_tool import generate_hermes_tools_module
    scope = {}
    exec(generate_hermes_tools_module(['terminal']), scope)
    call = Mock(return_value={'accepted': 'synthetic'})
    scope['_call'] = call
    control = {'action': 'stop', 'task': 'fixture'}
    assert scope['terminal']('true', continuation_control=control) == {'accepted': 'synthetic'}
    assert call.call_args.args[0] == 'terminal'
    assert call.call_args.args[1]['continuation_control'] is control


@pytest.mark.parametrize('prior_exited', [False, True])
def test_preserved_local_surface_rebinding_requires_proven_prior_exit(monkeypatch, prior_exited):
    from hermes_cli import diggr_continuation as dc
    binding = {'target': {'workspace': 'fixture-workspace', 'surface': 'fixture-surface'},
               'shell': {'pid': 101, 'start': 'fixture-start'}}
    current = {'target': dict(binding['target']), 'shell': {'pid': 102}}
    verify = Mock(side_effect=[ValueError('old shell absent'), None])
    bind = Mock(return_value=current)
    monkeypatch.setattr(dc, 'verify_visible_target', verify)
    monkeypatch.setattr(dc, 'identity_exited', lambda proof: prior_exited)
    monkeypatch.setattr(dc, 'bind_visible_target', bind)
    row = {'visible_binding': binding, 'visible_shell_identity': {'pid': 101, 'created': 10}}
    if prior_exited:
        assert dc.recovery_visible_binding(row) == current
        bind.assert_called_once_with(binding['target'])
        assert verify.call_args.kwargs == {'idle': True}
    else:
        with pytest.raises(ValueError, match='exit not reconciled'):
            dc.recovery_visible_binding(row)
        bind.assert_not_called()


@pytest.mark.parametrize('transport', ['uds', 'file'])
def test_generated_terminal_wrapper_forwards_proposal_receipt_and_control(transport):
    from tools.code_execution_tool import generate_hermes_tools_module
    scope = {}
    exec(generate_hermes_tools_module(['terminal'], transport=transport), scope)
    rpc = Mock(return_value={'accepted': 'synthetic'})
    scope['_call'] = rpc
    proposal = {'task': 'fixture', 'purpose': 'synthetic'}
    control = {'action': 'stop', 'task': 'fixture'}
    receipt = 'fixture-receipt'
    assert scope['terminal']('true', continuation_proposal=proposal,
                             completion_receipt=receipt,
                             continuation_control=control) == {'accepted': 'synthetic'}
    rpc.assert_called_once()
    name, arguments = rpc.call_args.args
    assert name == 'terminal'
    assert arguments['continuation_proposal'] is proposal
    assert arguments['completion_receipt'] == receipt
    assert arguments['continuation_control'] is control
    assert 'background' not in arguments
