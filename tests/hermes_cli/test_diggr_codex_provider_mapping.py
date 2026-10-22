"""Logical Hermes provider names must bind to the exact Codex CLI provider."""

import copy
from pathlib import Path

import pytest

from hermes_cli import diggr_continuation as dc
from hermes_cli import diggr_owner as owner
from tests.gateway.test_diggr_item_lifecycle import sent_second_renewal
from tests.gateway.test_diggr_owner_grant import route


def route_row(*, logical_provider='openai-codex', cli_provider='openai',
              model='gpt-6-sol', effort='high', sandbox='workspace-write',
              approval='never'):
    contract_model = dict(required_model=model, provider=logical_provider,
                          reasoning_effort=effort, sandbox='workspace-write',
                          approval_policy='never')
    argv = ['/tools/codex', 'exec', '--model', model, '--sandbox', sandbox,
            '--cd', '/fixture/worktree', '--output-last-message', '/fixture/result.txt',
            '-c', 'model_provider="' + cli_provider + '"',
            '-c', 'model_reasoning_effort="' + effort + '"',
            '-c', 'approval_policy="' + approval + '"', 'fixture prompt']
    return dict(worker_route=dict(argv=argv, worktree='/fixture/worktree'),
                artifact='/fixture/result.txt',
                logical_contract=dict(version=owner.LOGICAL_VERSION, model=contract_model))


@pytest.mark.asyncio
async def test_native_generator_maps_sol_contract_and_roundtrips(route, monkeypatch):
    _, home, reference, _, descriptor = await sent_second_renewal(route, monkeypatch)
    dc.accept_coding_assignment('SYSTEM171_ASSIGN ' + reference, 'second-session', home)
    prompt = Path(descriptor['prompt']['path']).read_text()
    _, row = dc.register_assigned_worker(reference, 'second-session', home, prompt)
    model = row['logical_contract']['model']
    assert model['provider'] == 'openai-codex'
    assert model['required_model'] == 'gpt-6-sol'
    assert model['reasoning_effort'] == 'high'
    assert model['fallback_models_allowed'] == []
    assert model['sandbox'] == 'workspace-write'
    assert model['approval_policy'] == 'never'
    assert 'model_provider="openai"' in row['worker_route']['argv']
    dc.validate_codex_argv(row)


def test_codex_alias_is_accepted_only_as_cli_openai():
    row = route_row()
    dc.validate_codex_argv(row)
    tampered = copy.deepcopy(row)
    tampered['worker_route']['argv'][11] = 'model_provider="openai-codex"'
    with pytest.raises(ValueError, match='provider|config'):
        dc.validate_codex_argv(tampered)


def test_existing_openai_provider_stays_openai():
    dc.validate_codex_argv(route_row(logical_provider='openai'))


@pytest.mark.parametrize('override', ['model_provider="openai-codex"',
                                      'model_provider="other"'])
def test_duplicate_provider_override_cannot_shadow_pinned_cli_name(override):
    row = route_row()
    row['worker_route']['argv'][-1:-1] = ['-c', override]
    with pytest.raises(ValueError, match='duplicate|unsupported'):
        dc.validate_codex_argv(row)


@pytest.mark.parametrize('change', [
    {'cli_provider': 'other'}, {'model': 'gpt-6-astra'},
    {'effort': 'medium'}, {'sandbox': 'danger-full-access'},
    {'approval': 'on-request'},
])
def test_cli_route_rejects_changed_provider_or_execution_boundary(change):
    row = route_row()
    good = row['worker_route']['argv']
    bad = copy.deepcopy(row)
    argv = bad['worker_route']['argv']
    if 'cli_provider' in change:
        argv[11] = 'model_provider="' + change['cli_provider'] + '"'
    if 'model' in change:
        argv[3] = change['model']
    if 'effort' in change:
        argv[13] = 'model_reasoning_effort="' + change['effort'] + '"'
    if 'sandbox' in change:
        argv[5] = change['sandbox']
    if 'approval' in change:
        argv[15] = 'approval_policy="' + change['approval'] + '"'
    assert argv != good
    with pytest.raises(ValueError):
        dc.validate_codex_argv(bad)


def test_unknown_logical_provider_fails_closed():
    with pytest.raises(ValueError, match='provider'):
        dc.validate_codex_argv(route_row(logical_provider='unknown-provider',
                                         cli_provider='unknown-provider'))
