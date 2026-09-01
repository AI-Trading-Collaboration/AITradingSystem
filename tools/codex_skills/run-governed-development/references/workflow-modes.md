# Governed Workflow Modes

## Authority

Use these sources in descending execution authority:

1. system/developer/user instructions;
2. repository `AGENTS.md`;
3. reviewed task requirement and policies;
4. executable checkout/lease/validation guards;
5. this skill.

The skill never independently authorizes strategy, production, broker, PR,
force-push, history rewrite, remote-divergence repair, or task-source cutover.
Apply the repository's default ordinary-push rule only after its final-tree,
ancestry, attribution, and remote-main gates pass.

## Preflight Commands

Run from the repository root. Resolve `<skill-root>` to the installed or
canonical skill directory.

Read-only:

```powershell
python <skill-root>/scripts/preflight.py --repo . --mode READ_ONLY
```

Single lane after task registration:

```powershell
python <skill-root>/scripts/preflight.py --repo . --mode SINGLE_LANE `
  --task-id <TASK_ID> --role coordinator --stage LANE `
  --coordinator-path docs/task_register.md `
  --claim task=src/example.py --claim task=tests/test_example.py
```

If the task lane still descends from `<FROZEN_BASE>` but local `main` has
advanced, `--stage LANE` reports
`BASE_DRIFT_DEFERRED_TO_INTEGRATION_PLAN` and does not require a replacement
branch. Before `INTEGRATION`, build the read-only plan:

```powershell
python scripts/architecture_arch005_integration_revalidation.py plan `
  --repository . --manifest <CHANGE_MANIFEST_JSON> `
  --frozen-base <FROZEN_BASE> --lane-head <LANE_HEAD> `
  --latest-main <LOCAL_MAIN> --output <PLAN_JSON>
```

Before coordinator shared/generated mutation, acquire one publication transaction
from the clean exact latest-main coordinator candidate base and expected local main.
When a validated base-drift plan is present, the plan continues to bind the frozen
task lane while the transaction binds this separate candidate base. Declare every task,
coordinator, generated, and formal-artifact path up front:

```powershell
python scripts/architecture_arch005_publication_fence.py acquire `
  --transaction-id <TRANSACTION_ID> --task-id <TASK_ID> `
  --change-id <CHANGE_ID> --thread-id <THREAD_ID> `
  --frozen-base <FROZEN_BASE> --lane-head <CANDIDATE_BASE> `
  --expected-main <LOCAL_MAIN> `
  --owned-path <TASK_PATH> --shared-path <COORDINATOR_PATH> `
  --generator-id canonical-task-source
```

Do not replace the plan's frozen `lane_head` with `<CANDIDATE_BASE>`; preflight
independently verifies the plan id/SHA, frozen lane, latest main, transaction
candidate base, and reviewed reconciliation id. The returned
`integration_publication_fence.v1` path is immutable. Advance it to
`TASK_SOURCE_PRE_WRITE` before `task_source.py register|update|build|refresh-consumers`,
then wrap the declared official generator order with `GENERATED_REBUILD_PRE` and
`GENERATED_REBUILD_POST`. Plan bytes, expected main, lease state, dirty attribution,
generator order, and evidence hashes are revalidated at each phase.

Then rerun preflight on the clean committed lane:

```powershell
python <skill-root>/scripts/preflight.py --repo . --mode SINGLE_LANE `
  --task-id <TASK_ID> --role coordinator --stage INTEGRATION `
  --expected-base <FROZEN_BASE> `
  --integration-revalidation-plan <PLAN_JSON> `
  --change-manifest <CHANGE_MANIFEST_JSON> `
  --publication-transaction <TRANSACTION_JSON> `
  --coordinator-path docs/task_register.md `
  --claim task=src/example.py
```

The validator rebuilds `integration_revalidation_plan.v1` from the same
repository. Only `READY_FOR_SINGLE_INTEGRATION_CANDIDATE` permits one
latest-main coordinator candidate. `RECONCILIATION_REQUIRED` keeps the lane and
requires one reviewed coordinator reconciliation; after reviewing the exact
overlap rows, rerun the same integration preflight with
`--reviewed-reconciliation-plan-id <PLAN_ID>`. The id must exactly match the
validated plan and is recorded in the preflight result.
`SERIAL_CONTRACT_WAVE_REQUIRED` requires the smallest reviewed contract wave;
`BLOCKED` or any binding/tamper failure stops integration.

Dual lane:

```powershell
python <skill-root>/scripts/preflight.py --repo . --mode DUAL_LANE `
  --task-id <INTEGRATION_TASK_ID> --role coordinator --stage START `
  --claim engineering=src/example_engineering.py `
  --claim strategy-evidence=src/example_research.py `
  --coordinator-path docs/task_register.md
```

Add `--contract-change` when a global policy, shared schema, public API,
DQ/PIT/cache identity, research window, threshold, or consumer-visible contract
changes. Expect `SERIAL_CONTRACT_WAVE_REQUIRED`.

The repository default is an ordinary push after local-main integration. Fetch
remote main first, then rerun the same governed mode and claims with:

```powershell
python <skill-root>/scripts/preflight.py --repo . `
  --mode <SINGLE_LANE_OR_DUAL_LANE> --task-id <TASK_ID> `
  --role coordinator --stage CLOSEOUT --remote-action `
  --publication-transaction <TRANSACTION_JSON> `
  <THE_SAME_LANE_AND_COORDINATOR_PATH_CLAIMS>
```

This is a read-only publication gate. It requires a clean local `main`, a
present `origin/main`, and `origin_only=0`; it never fetches, pushes, merges,
rebases, rewrites history, or repairs divergence.

## Mode Protocols

### READ_ONLY

- Do not create a branch, worktree, write lease, artifact, or task mutation.
- Use governed audit and lease replay when repository safety matters.
- Report local-main/remote divergence without changing either ref.

### SINGLE_LANE

1. Register the task and supporting requirement.
2. Run start preflight on local `main`.
3. Create a task branch from the reported exact local-main SHA.
4. Rerun lane preflight on the task branch.
5. Implement only declared task/coordinator paths.
6. Run focused and applicable formal validation.
7. If main advanced, keep the frozen task branch and obtain a validated
   `integration_revalidation_plan.v1`; do not create v2/v3 replacement lanes.
8. Acquire one `integration_publication_fence.v1` from the exact expected main,
   then form one latest-main candidate, reconcile reviewed domain overlap once,
   and regenerate coordinator-refreshable views once under that transaction.
9. At `TASK_SOURCE_PRE_WRITE`, update task status through the canonical writer;
   record official generator input/output hashes at the generated rebuild phases.
10. Commit the clean candidate, record `FORMAL_VALIDATION_PRE`, and run formal
   tiers. Executed Full requires `--publication-transaction`, atomically claims
   `FULL_DISPATCHED`, and records the exact summary at `FORMAL_VALIDATION_RESULT`.
11. Record `LOCAL_MAIN_FF_PRE`, verify local `main` is its ancestor, and
   fast-forward local `main`.
12. Fetch remote main, record `REMOTE_PUSH_PRE`, rerun `SINGLE_LANE` coordinator preflight with the same
    claims plus `--stage CLOSEOUT --remote-action`, ordinary-push, and verify
    both SHAs. A task moved to `docs/task_register_completed.md` by the validated
    final commit is recognized only at `CLOSEOUT`; earlier stages still require
    the active register.
13. Ordinary-push, verify `local main = origin/main = candidate`, audit/clean,
   record `CLEANUP_PRE`, and release the lease through the transaction receipt.

### DUAL_LANE

1. Register both domain tasks and the integration scope.
2. Run claim preflight before creating worktrees.
3. If a consumer-visible contract changes, complete a serial contract wave.
4. Create engineering and strategy-evidence branches/worktrees from the same
   exact local-main commit.
5. Give each lane disjoint owned paths and lane-focused validation.
6. Keep coordinator-only files out of both workers.
7. Create a coordinator integration branch from the frozen common base.
   If main has advanced, first obtain one validated base-drift plan and create
   the coordinator candidate from latest main; do not rebuild both domain lanes.
8. Acquire one coordinator publication transaction and absorb changes in this order:
   `contract -> adapter -> domain -> tests/fragments -> shared wiring/docs ->
   generated views`.
9. Record the generated rebuild once, bind the clean candidate, and run combined
   focused, generated freshness, architecture/contract, and the required
   integration/Full tiers. Full receives the exact transaction path.
10. Fast-forward local `main` once. Never fast-forward sibling lanes directly in
    sequence.
11. Fetch remote main, rerun `DUAL_LANE` coordinator preflight with the same
    claims plus `--stage CLOSEOUT --remote-action`, ordinary-push local main,
    and verify `local main = remote main = candidate`.
12. Audit and clean each lane independently.

## Coordinator-Only Defaults

Treat at least these paths as coordinator-only:

- `AGENTS.md`;
- `docs/task_register.md`;
- `docs/task_register_completed.md`;
- `docs/system_flow.md`;
- `docs/artifact_catalog.md`;
- `config/report_registry.yaml`;
- `registry/development_tasks_shadow/**`;
- `inputs/architecture/**`;
- formal validation artifacts and root/shared CLI wiring.

If a domain task needs one of these paths, declare it as coordinator work rather
than lane ownership.

## Stop Conditions

Stop and report on:

- missing task registration;
- governed worktree audit failure;
- unexpected active lease;
- stale or mismatched exact base;
- lane path overlap, including ancestor/descendant overlap;
- lane claim on coordinator-only state;
- unresolved public-contract or semantic conflict;
- concurrent heavyweight Full runs;
- missing, expired, terminal, stale-main, plan-tampered, or wrong-phase
  `integration_publication_fence.v1`;
- unattributed or unique worktree residue;
- non-fast-forward local-main integration;
- remote closeout from a non-main or dirty checkout;
- missing remote/upstream, remote divergence, or non-fast-forward push;
- candidate history containing unrelated user changes or commits;
- any push that would require merge, rebase, history rewrite, or force-push.
