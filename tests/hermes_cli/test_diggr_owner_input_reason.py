"""Owner commands stay fail-closed and explain the actual Telegram rejection."""

from types import SimpleNamespace

import pytest

from hermes_cli import diggr_owner as owner


def _input(*, entities=(), thread=None):
    text = '/continuation show ' + 'a' * 64
    message = SimpleNamespace(
        text=text, chat=SimpleNamespace(id=int(owner.OWNER), type='private'),
        from_user=SimpleNamespace(id=int(owner.OWNER), is_bot=False),
        message_id=42, message_thread_id=thread, entities=entities,
    )
    event = SimpleNamespace(text=text, message_id='42', platform_update_id=43,
                            internal=False)
    identity = dict(profile='diggr-main', platform='telegram', chat_id=owner.OWNER,
                    user_id=owner.OWNER, thread_id=str(thread or ''))
    owner.stamp_telegram(event, message, 43)
    return event, identity


def test_code_formatted_command_explains_rejection_without_authorizing():
    event, identity = _input(entities=[SimpleNamespace(type='code')])
    with pytest.raises(ValueError, match='code or quote formatting is not direct owner input'):
        owner.verify_input(event, identity)
    assert not hasattr(event, '_diggr_native_input')


def test_thread_mismatch_explains_rejection_without_authorizing():
    event, identity = _input(thread=7)
    identity['thread_id'] = '8'
    with pytest.raises(ValueError, match='Telegram topic differs from the tool session'):
        owner.verify_input(event, identity)


def test_plain_direct_message_remains_valid():
    event, identity = _input()
    assert owner.verify_input(event, identity) == '43:42'


def test_overlapping_owner_card_is_rejected_before_display():
    class Rows(dict):
        pass

    identity = dict(home='/tmp/native-owner-test', profile='diggr-main',
                    platform='telegram', chat_id=owner.OWNER, user_id=owner.OWNER,
                    thread_id='')
    todo = dict(workspace_id='11111111-1111-4111-8111-111111111111',
                project_id='22222222-2222-4222-8222-222222222222',
                issue_id='33333333-3333-4333-8333-333333333333',
                contract=dict(scope='first repair', action='inspect', owner='main',
                              gate='main', producer='main', worker_route=None,
                              launcher_command=None),
                depends_on=[], gates=['owner_confirmed', 'native_checks', 'prior_done'])
    old_manifest = dict(budgets=dict(owner.BUDGETS), replaces=None, todos=[todo])
    new_todo = dict(todo, contract=dict(todo['contract'], scope='later repair'))
    new_manifest = dict(budgets=dict(owner.BUDGETS), replaces=None, todos=[new_todo])
    rows = Rows()
    rows.grants = dict(proposals={}, batches={'old-batch': dict(
        owner=owner.stable_owner(identity), manifest=old_manifest, tasks={},
        revoked=False, replaced_by=None)})

    with pytest.raises(ValueError, match='explicit reauthorization of prior batch'):
        owner._store_proposal(rows, identity, new_manifest)
    assert rows.grants['proposals'] == {}

    new_manifest['replaces'] = 'old-batch'
    digest = owner._store_proposal(rows, identity, new_manifest)
    assert rows.grants['proposals'][digest]['manifest']['replaces'] == 'old-batch'
