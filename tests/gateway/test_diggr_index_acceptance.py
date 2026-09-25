"""Real Git index changes remain inside native logical acceptance boundaries."""
import json
from pathlib import Path
import pytest
from hermes_cli import diggr_continuation as dc
from tests.gateway.test_diggr_owner_grant import route
from tests.gateway.test_diggr_logical_admission import prepare, authorize_logical, git


@pytest.mark.asyncio
@pytest.mark.parametrize('change', ['private_edit', 'private_delete', 'rename_from_private',
                                  'rename_into_private', 'outside_edit', 'allowed_edit'])
async def test_index_only_changes_checked_before_native_ack(route, monkeypatch, change):
    r = route
    contract, packet = prepare(r, monkeypatch)
    work = Path(r.task['worker_route']['worktree'])
    private = work / 'src/private/tracked.txt'
    private.parent.mkdir()
    private.write_text('base private\n')
    (work / 'outside.txt').write_text('base outside\n')
    git(work, 'add', '.')
    git(work, '-c', 'commit.gpgsign=false', 'commit', '-m', 'tracked boundary fixture')
    packet['base'] = git(work, 'rev-parse', 'HEAD')
    packet_path = Path(r.task['worker_route']['packet'])
    packet_path.write_text(json.dumps(packet))
    r.task['worker_route']['packet_sha256'] = dc.digest(packet_path)
    await authorize_logical(r, contract)
    token = dc.EVENT_CONTEXT.set(dict(identity=r.identity))
    try:
        _, row = dc.register_native(r.task)
    finally:
        dc.EVENT_CONTEXT.reset(token)
    if change == 'private_delete':
        git(work, 'rm', 'src/private/tracked.txt')
    elif change == 'rename_from_private':
        git(work, 'mv', 'src/private/tracked.txt', 'src/moved.txt')
    elif change == 'rename_into_private':
        git(work, 'mv', 'src/code.txt', 'src/private/moved.txt')
    else:
        path = {'private_edit': private, 'outside_edit': work / 'outside.txt',
                'allowed_edit': work / 'src/code.txt'}[change]
        path.write_text('index-only change\n')
        git(work, 'add', '.')
    # Restore only worktree bytes; preserve the prohibited index change.
    git(work, 'restore', '--source=' + packet['base'], '--worktree', '.')
    if change.endswith('_edit'):
        assert git(work, 'diff', '--name-only', packet['base'], '--') == ''
    assert git(work, 'diff', '--cached', '--name-only', packet['base'], '--')
    artifact = Path(row['artifact'])
    artifact.write_text('fixture result')
    with r.guard.transaction() as rows:
        rows[row['task']].update(status='executing', lease=1100)
    evidence = dict(task=row['task'], generation=row['generation'], action=row['action'],
                    artifact=row['artifact'], sha256=dc.digest(artifact), result='validated')
    before = r.guard.path.read_bytes()
    if change == 'allowed_edit':
        assert r.guard.ack(r.identity, row, evidence, 'main_live', 'review fixture', now=1000)
    else:
        with pytest.raises(ValueError, match='No-Touch'):
            r.guard.ack(r.identity, row, evidence, 'main_live', 'review fixture', now=1000)
        assert r.guard.path.read_bytes() == before
