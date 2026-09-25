# SYSTEM-169: native owner buttons

Source candidate only. No deployment, restart, live Telegram acceptance or real
Coding worker launch is claimed. Independent review of the final source and a
separately authorized live acceptance remain necessary.

## Normal flow

1. `terminal_dispatch` accepts `SYSTEM167_PROPOSE_OWNER_BATCH` only with the
   gateway's native identity and transport. A proposal grants no authority.
2. The existing Guard stores the immutable manifest and reserves one original
   Telegram send. The adapter sends the complete plain-text contract without
   buttons, persists the returned message ID, then attaches Freigeben/Ablehnen.
   The contract includes resources, roles, actions, explicit merge yes/no,
   model/no-fallback, acceptance, dependencies, gates and budgets. Oversized
   previews fail before approval; authority is never truncated.
3. A real PTB callback must match owner, bot, profile/home, private chat, topic,
   session, message, digest and preview reference/version. One Guard transaction
   records the decision and, on approval, the confirmed batch plus Main wake.
   The callback itself never runs Main or Coding.
4. The existing idle/FIFO path queues the typed native wake behind prior work.
   Pause/drain, session lineage, transport and admission checks still apply.
   Coding registration, route preflight and one-send worker dispatch remain
   separate guarded steps.

## Status and repetition

The immutable contract body is state-neutral. Telegram edits render the current
approval state separately; “Bestätigt” does not claim a worker has or has not run.

Proposal retries report a locked state snapshot and next step: pending, open,
confirmed, declined, revoked, expired, failed/uncertain send or legacy-unbound.
Confirmed responses include the existing batch, admitted-task count and Main
continuation status. Confirmation is distinct from admission, dispatch, worker
completion, Main acceptance and delivery of the resulting answer. A dispatch
acknowledgment is not completion; producing an answer is not successful delivery.

Repeating the same manifest never renews grants, deadlines or budgets, sends
another original preview, or silently adopts old records. A different bound
session, profile, bot or transport is rejected without disclosing its state.
Only the per-message Telegram reply anchor may drift; the stored origin stays
unchanged. Expired previews require an explicit renew click and then a new
approval click. Stop/reset/new suppress further authority; replay cannot undo it.

## Legacy text and uncertain delivery

The explicit text fallback remains available, including for legacy-unbound
proposals and exhausted preview edits. The authenticated owner sends
`/continuation show HASH`, reads the complete displayed contract, then sends
`/continuation confirm HASH CODE` as a **new, unformatted direct message** within
the displayed deadline. No code block, quote, reply or forward. Use the latest
one-use code; an expired display requires a new explicit show. Model text or a
copied callback selector is not native confirmation.

An original send with unknown outcome is never blindly retried, including after
restart. It may have reached Telegram without a usable response/message binding.
The model reports uncertainty and the explicit text fallback instead of promising
an automatic preview. Idempotent edits have bounded retries and cannot change
grants. A confirmed decision survives acknowledgment/markup failure. An accepted
Main turn whose runtime owner is lost also becomes uncertain; it is not restarted
automatically and receives no fresh budget.

## F1: source and test alignment

The earlier F1 setup failure was traced to stale installed test fixtures paired
with newer runtime source. The missing native transport ContextVar binding in
the old fixtures correctly triggered `native transport binding required before
registration`. The matching source already binds transport in the async helper
and again in its synchronous caller after `asyncio.run`; weakening that gate is
not a fix.

Any later authorized delivery must align runtime, tests and helpers from the
same reviewed revision, particularly `tests/diggr_owner_fixtures.py`,
`tests/hermes_cli/test_diggr_non_cmm_route.py` and their helper dependencies.
Do not combine older installed tests with this candidate or treat a setup error
as a passing run.

Run isolated checks through `scripts/run_tests.sh -j 2 --file-retries 0` with an
approved existing `HERMES_PYTHON`. `tests/test_diggr_owner_buttons_ptb.py` uses real
PTB and lives outside the gateway test subtree, whose conftest replaces PTB.
The declared PTB development dependency must match the lockfile. Tests use
temporary Guard/SessionDB state and real auth/admission/FIFO decisions; Telegram,
model and physical worker effects are offline boundaries, not live acceptance.
