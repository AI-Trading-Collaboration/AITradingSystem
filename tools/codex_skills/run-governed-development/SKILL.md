---
name: run-governed-development
description: Run the governed AITradingSystem development workflow. Use for any non-trivial tracked repository mutation, including implementation, bug fixes, refactors, data/scoring/backtest/report changes, architecture or governance work, branch/worktree setup, parallel engineering and strategy lanes, integration, validation, or cleanup. Also use in READ_ONLY mode when a status or diagnosis depends on current checkout safety, leases, worktrees, or base divergence.
---

# Run Governed Development

Treat repository rules and executable guards as authority. Use this skill to
select and sequence them; never replace them with skill-local state.

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

## Integrate and Close

- For `SINGLE_LANE`, commit the validated task branch and fast-forward local
  `main` only after final-tree checks pass.
- For `DUAL_LANE`, form one coordinator integration candidate from the common
  base, absorb lanes in the reviewed order, refresh shared/generated state,
  validate the combined tree, and fast-forward local `main` once.
- After local-main integration, fetch remote main, then rerun the same governed
  mode and claims as coordinator with `--stage CLOSEOUT --remote-action`.
  A task archived in `docs/task_register_completed.md` is eligible only at this
  `CLOSEOUT` stage; `START`, `LANE`, and `INTEGRATION` still require the task in
  the active register.
  Require a clean local `main` and `origin/main` to be the candidate's ancestor,
  perform the repository-default ordinary non-force push, and verify both SHAs.
  Skip only for an explicit no-push request or a governed no-push condition.
- Treat PR, force-push, history rewrite, and remote-divergence repair as separate
  actions requiring explicit authorization.
- Use the governed worktree audit at closeout.
- Clean branches/worktrees only after ancestry, unique-content, canonical
  evidence, process dependency, and recoverability checks pass.
- Report task commit, local-main SHA, validation, retained state, cleanup, and
  remote-action result.

## Resources

- Run `scripts/preflight.py` for deterministic repository and claim checks.
- Run `scripts/architecture_arch005_integration_revalidation.py plan|validate`
  through the repository when a frozen lane reaches an integration boundary
  after local main has advanced.
- Run `scripts/verify_bundle_parity.py` to compare the Git canonical bundle with
  the installed `$CODEX_HOME` bundle.
- Load [workflow-modes.md](references/workflow-modes.md) when executing a task.
