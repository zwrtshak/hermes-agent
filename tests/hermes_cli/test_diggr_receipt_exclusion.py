"""R3: guarded coding must not acquire a second, lifecycle-local receipt path."""
from unittest.mock import Mock
import pytest
from hermes_cli import diggr_continuation as dc


@pytest.mark.parametrize('receipt', ['fixture-receipt', ''])
def test_coding_receipt_rejected_before_any_registration_or_launch(tmp_path, monkeypatch, receipt):
    state = tmp_path / 'existing-ledger.json'
    state.write_text('{"existing_budget": 7}')
    before = state.read_bytes()
    snapshot = lambda: {p.relative_to(tmp_path): p.read_bytes() if p.is_file() else None
                        for p in tmp_path.rglob("*")}
    before_tree = snapshot()
    register = Mock(side_effect=AssertionError('registration boundary reached'))
    guard = Mock(side_effect=AssertionError('state boundary reached'))
    invoke = Mock(side_effect=AssertionError('launcher boundary reached'))
    send = Mock(side_effect=AssertionError('visible send boundary reached'))
    monkeypatch.setattr(dc, 'register_native', register)
    monkeypatch.setattr(dc, 'runtime_guard', guard)
    monkeypatch.setattr(dc, 'cmux_send', send)
    args = dict(command='fixture-launcher', background=True,
                continuation={'producer': 'cmux', 'launcher_command': 'fixture-launcher'},
                completion_receipt=receipt)
    with pytest.raises(ValueError, match='guarded coding cannot request completion_receipt'):
        dc.terminal_dispatch(args, invoke)
    register.assert_not_called()
    guard.assert_not_called()
    invoke.assert_not_called()
    send.assert_not_called()
    assert state.read_bytes() == before
    assert snapshot() == before_tree


def test_ordinary_receipt_still_reaches_native_terminal_unchanged(monkeypatch):
    register = Mock(side_effect=AssertionError('ordinary receipt is not coding admission'))
    monkeypatch.setattr(dc, 'register_native', register)
    args = dict(command='synthetic-command', background=True, completion_receipt='fixture-receipt')
    native = Mock(return_value='synthetic native result')
    assert dc.terminal_dispatch(args, native) == 'synthetic native result'
    native.assert_called_once_with(args)
    assert native.call_args.args[0] is args
    register.assert_not_called()
