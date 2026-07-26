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

## Integrate and Close

- For `SINGLE_LANE`, commit the validated task branch and fast-forward local
  `main` only after final-tree checks pass.
- For `DUAL_LANE`, form one coordinator integration candidate from the common
  base, absorb lanes in the reviewed order, refresh shared/generated state,
  validate the combined tree, and fast-forward local `main` once.
- After local-main integration, run the closeout preflight with
  `--remote-action`, fetch remote main, require it to be an ancestor of the
  candidate, perform the repository-default ordinary non-force push, and verify
  both SHAs. Skip only for an explicit no-push request or a governed no-push
  condition.
- Treat PR, force-push, history rewrite, and remote-divergence repair as separate
  actions requiring explicit authorization.
- Use the governed worktree audit at closeout.
- Clean branches/worktrees only after ancestry, unique-content, canonical
  evidence, process dependency, and recoverability checks pass.
- Report task commit, local-main SHA, validation, retained state, cleanup, and
  remote-action result.

## Resources

- Run `scripts/preflight.py` for deterministic repository and claim checks.
- Run `scripts/verify_bundle_parity.py` to compare the Git canonical bundle with
  the installed `$CODEX_HOME` bundle.
- Load [workflow-modes.md](references/workflow-modes.md) when executing a task.
