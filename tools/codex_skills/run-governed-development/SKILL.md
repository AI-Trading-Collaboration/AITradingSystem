---
name: run-governed-development
description: Run the governed development workflow only in the AITradingSystem repository. Use there for non-trivial tracked mutations or read-only checkout, lease, worktree, and base-divergence audits. Do not use this skill in other repositories; their own AGENTS.md and project workflows apply instead.
---

# Run Governed Development

Treat repository rules and executable guards as authority. Use this skill to
select and sequence them; never replace them with skill-local state.

## Repository scope gate

Before reading this skill's repository-specific references or running any of its
commands:

1. Resolve the current Git top level with `git rev-parse --show-toplevel`.
2. Resolve the common Git directory with `git rev-parse --git-common-dir`.
3. Require `origin` to identify
   `github.com/AI-Trading-Collaboration/AITradingSystem` (HTTPS, SCP-style SSH,
   or `ssh://` form). Accept its main checkout and linked worktrees, but not a
   similarly named independent repository.
4. Require both repository sentinels at the resolved top level:
   - `docs/requirements/DEVX-002_Governed_Development_Workflow_Skill.md`
   - `scripts/architecture_arch005_checkout_guard.py`

The bundled `scripts/preflight.py` enforces the same identity and sentinel
checks before any project workflow probe. If repository identity or either
sentinel does not match, stop using this skill immediately. Do not create task
rows, branches, worktrees, leases, or governance artifacts. Follow the active
repository's `AGENTS.md` and ordinary workflow.

## Start

1. Read the repository `AGENTS.md` completely.
2. Read the relevant task row and supporting requirement.
3. Read
   `docs/requirements/DEVX-002_Governed_Development_Workflow_Skill.md`.
4. Select exactly one mode:
   - `READ_ONLY` for no tracked mutation.
   - `SINGLE_LANE` for one task-owned mutation scope.
   - `DUAL_LANE` for independent engineering and strategy-evidence scopes.
5. Run `scripts/preflight.py` before implementation.

If a non-trivial mutation has no task row, create only the coordinator-owned task
row and supporting requirement, then rerun preflight before implementation.

## Interpret Preflight

- Continue only on `PASS`.
- On `SERIAL_CONTRACT_WAVE_REQUIRED`, freeze the smallest reviewed shared
  contract/policy change first, then restart both lanes from its exact local-main
  base.
- On `BLOCKED`, stop mutation and resolve the typed reasons. Do not bypass
  worktree audit, active lease, task registration, base, path, or coordinator
  ownership failures.
- When a clean task lane still descends from its frozen base but local `main`
  advances, continue lane-focused work without creating a replacement branch.
  At `INTEGRATION`, build and validate an `integration_revalidation_plan.v1`.
  Only `READY_FOR_SINGLE_INTEGRATION_CANDIDATE` permits one latest-main
  coordinator candidate. Reconciliation, serial-contract, undeclared-path, and
  invalid-evidence decisions remain typed stop conditions. A domain-only
  `RECONCILIATION_REQUIRED` plan may continue without rebuilding the lane only
  when the coordinator explicitly supplies its exact reviewed plan id at the
  integration stage.

Read [workflow-modes.md](references/workflow-modes.md) for command forms,
ownership rules, integration topology, validation, and cleanup.

## Execute

- Keep local `main` unchanged during task implementation.
- Use a task branch for `SINGLE_LANE`.
- Use isolated sibling branches/worktrees from one exact local-main commit for
  `DUAL_LANE`.
- Keep task register, system flow, catalogs/registries, root/shared wiring,
  generated manifests/views, and formal validation under coordinator ownership.
- Run lane-focused validation in workers. Run heavyweight formal/Full validation
  only at the natural integration boundary.
- Preserve DQ/PIT, research-window, threshold, evidence-lineage, production, and
  broker boundaries.
- Base-drift planning is read-only. It never authorizes automatic rebase,
  merge, cherry-pick, commit, push, cleanup, or task mutation. Discard
  coordinator-refreshable lane bytes and regenerate those views once on the
  final candidate; bind heavyweight formal validation only to that final tree.
- Before coordinator-owned task-source mutation, generated-state rebuild, final
  candidate binding, or executed Full validation, acquire one repository
  `integration_publication_fence.v1` transaction. Pass its exact
  `--publication-transaction` to task-source, validation, `INTEGRATION`, and
  `CLOSEOUT` preflight commands. The transaction reuses the S4D
  `FileExecutionLeaseStore`; do not add or emulate a second lock.
- For a reviewed base-drift plan, acquire the transaction from the clean exact
  latest-main coordinator candidate base. `INTEGRATION` preflight binds the
  plan's frozen lane identity separately from the transaction's latest-main
  candidate identity; never rewrite the plan's lane head to make those values
  appear equal.

## Integrate and Close

- Advance the publication transaction through the reviewed phase order. Recheck
  expected local main and any integration-plan hash before each shared mutation;
  rebuild generated authorities once, bind the clean committed candidate at
  `FORMAL_VALIDATION_PRE`, and let the Full runner atomically record
  `FULL_DISPATCHED` before pytest and `FORMAL_VALIDATION_RESULT` afterward.
- For `SINGLE_LANE`, commit the validated task branch and fast-forward local
  `main` only after final-tree checks pass.
- For `DUAL_LANE`, form one coordinator integration candidate from the common
  base, absorb lanes in the reviewed order, refresh shared/generated state,
  validate the combined tree, and fast-forward local `main` once.
- After local-main integration, fetch remote main, then rerun the same governed
  mode and claims as coordinator with `--stage CLOSEOUT --remote-action`.
  The active transaction must already be at `REMOTE_PUSH_PRE` and is supplied
  with `--publication-transaction`.
  A task archived in `docs/task_register_completed.md` is eligible only at this
  `CLOSEOUT` stage; `START`, `LANE`, and `INTEGRATION` still require the task in
  the active register.
  Require a clean local `main` and `origin/main` to be the candidate's ancestor,
  perform the repository-default ordinary non-force push, and verify both SHAs.
  Skip only for an explicit no-push request or a governed no-push condition.
- Treat PR, force-push, history rewrite, and remote-divergence repair as separate
  actions requiring explicit authorization.
- Use the governed worktree audit at closeout.
- After ordinary push and exact SHA equality, record `CLEANUP_PRE`; release the
  S4D lease only through the publication command so the append-only closeout
  receipt remains replayable. A failed attempt is terminal evidence and any
  retry uses a new transaction with the failed Full parent bound explicitly.
- Clean branches/worktrees only after ancestry, unique-content, canonical
  evidence, process dependency, and recoverability checks pass.
- Report task commit, local-main SHA, validation, retained state, cleanup, and
  remote-action result.

## Resources

- Run `scripts/preflight.py` for deterministic repository and claim checks.
- Run `scripts/architecture_arch005_publication_fence.py acquire|checkpoint|validate|replay|release`
  for coordinator publication and Full exclusivity.
- Run `scripts/architecture_arch005_integration_revalidation.py plan|validate`
  through the repository when a frozen lane reaches an integration boundary
  after local main has advanced.
- Run `scripts/verify_bundle_parity.py` to compare the Git canonical bundle with
  the installed `$CODEX_HOME` bundle.
- Load [workflow-modes.md](references/workflow-modes.md) when executing a task.
