# SYSTEM-167: explicit non-CMM registered preflight

This is a source-level candidate, not an activation or installation instruction.
Review and deployment approval remain separate. Existing CMM routes are unchanged.
No context measurement, CMM config overlay or synthetic budget is used by this path.

## Authority and sequence

1. Main prepares an explicitly authorized packet with `routing_mode: non_cmm`
   and a clean, isolated feature worktree. All paths must be canonical absolute
   paths (not symlink aliases); packet and receipt must be outside the worktree
   if writing them would make it dirty. The receipt/output paths must be fresh.
2. Main registers through the existing `register_native` / `register` operation
   in its real authorized Telegram event context, using producer `cmux`.
   Registration still binds exact argv/model, packet hash, native identity,
   expiry, authorization and workspace/surface UUID plus OS shell identity.
   No manually supplied identity, test ContextVar or fabricated ticket is a
   supported operational substitute.
3. Using that actual immutable ticket, Main invokes the native module's new
   `route-preflight` operation with payload `{"ticket": <native ticket>,
   "route": <exact registered worker_route>}`. Existing `--home` and
   `--payload` / `--payload-json` transport options apply. This operation only
   reads source/worktree/shell identity and exclusively writes the registered
   receipt; it never launches a worker. It validates the ticket under the
   existing registry transaction, then validates and seals a real non-CMM
   receipt. Existing receipt files are never overwritten. Failure leaves the
   route unlaunchable; ambiguous evidence must not be replaced to force a retry.
4. Coding can use only the existing structured terminal command
   `SYSTEM167_REGISTERED_WORKER` with that native `continuation_ticket`.
   No manual argv send, new dispatch wrapper, alternate producer or guard
   bypass is part of this path. Preflight does not dispatch. Existing one-send
   reservation, foreground claim, ticket expiry and replay checks remain.
5. Actual worker evidence returns to Main via existing completion machinery.
   A send acknowledgment is not completion; the specialist cannot grant Main's
   acceptance, merge or activation. This change grants no installation/restart.

## Explicit packet contract

Required top-level fields:
- `routing_mode`: exactly `non_cmm` (no implicit fallback from CMM).
- `operator_scope_authorized`: literal true.
- `plane_id`, `task_id`, `scope_id`: each exactly the registered Plane ID.
- `worktree`, `branch`: exactly the registered path and feature branch.
- `base`: exact current clean worktree HEAD SHA. Detached HEAD and main/master
  are rejected; dirty/untracked work is rejected, not reset or stashed.
- `requested_actions`: nonempty list, each in the explicit authorized list.
- `coding_route`: `executor: codex`, `required_model: gpt-6-astra`,
  `fallback_models_allowed: []`, `merge_authorized: false`, plus a nonempty
  `routine_actions_authorized` list.

Supported action vocabulary: `inspect`, `edit`, `test`, `review`,
`document_work_item`, `commit`, `push`, `pr_update`. Requested actions must be
an authorized subset; deploy/merge/install/restart and unknown actions are
rejected. This verifies the routing contract, not semantic execution of an
arbitrary natural-language prompt; owner scope and executor sandbox still apply.

Supported exact argv grammar is deliberately narrow: absolute `codex`, `exec`,
option/value pairs, final prompt. Required options: `--model gpt-6-astra`,
`--sandbox workspace-write`, `--cd <registered worktree>`,
`--output-last-message <registered new artifact>`. Optional options:
`--color never`, `-c approval_policy="never"`. Duplicate, unknown, alternate
model/profile/config, full-auto and bypass options are rejected. No credentials
are read, copied or provisioned; auth/commit/push failures remain real blockers.

## Evidence integrity

Receipt schema `diggr.native.non_cmm_route.v1` binds task/generation,
packet hash, expiry and a digest over authorization/native identity, exact argv,
artifact, visible UUID/shell binding, worktree/branch and HEAD. It contains no
budget or telemetry. Its bytes must have been issued by the native preflight:
the registry separately records that digest. A hand-authored `allowed` receipt,
including one copying the expected fields, cannot be sealed without issuance.
The existing receipt seal also remains enforced. Revalidation before dispatch
and worker claim checks the current packet, worktree, model and visible binding;
claims use the original foreground ownership checks, not an idle-worker fiction.

## Tests / limits

Use the canonical `scripts/run_tests.sh` with an approved existing
`HERMES_PYTHON` when the worktree has no dev venv. Focused coverage is in
`tests/hermes_cli/test_diggr_non_cmm_route.py` and
`tests/hermes_cli/test_lifecycle_multiline_quotes.py`, alongside
`test_gateway_restart_loop.py` and `test_diggr_continuation_import.py`.
All registration/receipt/dispatch fixtures use isolated registry files and mocked
OS/transport boundaries. A passing test is not a live worker launch or proof of
production activation. No system-repo receipt-producer delta is required: the
producer and consumer share this existing native module and ticket boundary.
