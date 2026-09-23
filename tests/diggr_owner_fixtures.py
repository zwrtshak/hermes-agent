"""Disposable real adapter/gateway confirmation for pre-existing lifecycle tests."""
import asyncio
from pathlib import Path
import uuid
import itertools

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from gateway.config import GatewayConfig
from gateway.run import GatewayRunner
from gateway.session import SessionStore
from hermes_cli import diggr_continuation as dc, diggr_owner as owner
from tests.gateway.test_telegram_reply_quote import _make_adapter, _make_message


EVENT_IDS = itertools.count(1)

async def authorize_async(guard, task):
    # Reused row fixtures describe new explicit requests, not native routing state.
    for key in ('origin', 'deliveries', 'coordinator_delivery'):
        task.pop(key, None)
    identity = task['identity']
    identity.update(platform='telegram', chat_id=owner.OWNER, user_id=owner.OWNER)
    todo = dict(workspace_id='11111111-1111-4111-8111-111111111111',
                project_id='22222222-2222-4222-8222-222222222222', issue_id=str(uuid.uuid4()),
                contract=owner.contract(task), depends_on=[],
                gates=['owner_confirmed', 'native_checks', 'prior_done'])
    digest = owner.propose(guard, identity, dict(budgets=dict(owner.BUDGETS), todos=[todo], replaces=None))
    runner = object.__new__(GatewayRunner); runner.config = GatewayConfig()
    runner._diggr_identity = lambda source, session_id: dict(identity)
    runner._resolve_profile_home_for_source = lambda source: Path(identity['home'])
    runner.session_store = SessionStore(guard.path.parent / 'fixture-sessions', runner.config)
    adapter = _make_adapter()
    adapter.gateway_runner = SimpleNamespace(_profile_name_for_source=lambda s: 'diggr-main')
    adapter._should_process_message = lambda *a, **kw: True
    adapter._is_user_authorized_from_message = lambda m: True
    adapter._ensure_forum_commands = AsyncMock(); adapter._cache_replied_media = AsyncMock()
    adapter._apply_telegram_group_observe_attribution = lambda e: e
    adapter.send = AsyncMock(return_value=SimpleNamespace(success=True))
    runner._adapter_for_source = lambda source: adapter
    adapter.handle_message = runner._diggr_accept
    for index in (1, 2):
        text = '/continuation show ' + digest if index == 1 else adapter.send.call_args.args[1].splitlines()[-1]
        msg = _make_message(text=text)
        msg.chat.id = int(owner.OWNER); msg.from_user.id = int(owner.OWNER); msg.from_user.is_bot = False
        event_id = next(EVENT_IDS)
        msg.message_id = event_id
        with patch.object(dc, 'runtime_guard', return_value=guard):
            await adapter._handle_command(SimpleNamespace(effective_message=msg, update_id=event_id), None)
    with guard.transaction() as rows:
        bid = rows.grants['proposals'][digest]['batch']
    task.update(owner_batch=bid, owner_issue=owner.issue_key(todo))
    return task


def authorize(guard, task):
    return asyncio.run(authorize_async(guard, task))
