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

Add `--remote-action` to the coordinator closeout preflight. The repository
default is an ordinary push after local-main integration; this flag validates
that a remote exists and makes the intended publication visible in the output.

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
7. Update task status and generated governance state.
8. Commit the validated final tree.
9. Verify local `main` is its ancestor and fast-forward local `main`.
10. Run closeout preflight with `--remote-action`, fetch remote main, require it
    to be the candidate's ancestor, ordinary-push, and verify both SHAs.
11. Audit, then delete the merged task branch when recovery is available.

### DUAL_LANE

1. Register both domain tasks and the integration scope.
2. Run claim preflight before creating worktrees.
3. If a consumer-visible contract changes, complete a serial contract wave.
4. Create engineering and strategy-evidence branches/worktrees from the same
   exact local-main commit.
5. Give each lane disjoint owned paths and lane-focused validation.
6. Keep coordinator-only files out of both workers.
7. Create a coordinator integration branch from the frozen common base.
8. Absorb changes in this order:
   `contract -> adapter -> domain -> tests/fragments -> shared wiring/docs ->
   generated views`.
9. Run combined focused, generated freshness, architecture/contract, and the
   required integration/Full tiers on the final candidate.
10. Fast-forward local `main` once. Never fast-forward sibling lanes directly in
    sequence.
11. Run the coordinator remote gate, ordinary-push local main, and verify
    `local main = remote main = candidate`.
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
- unattributed or unique worktree residue;
- non-fast-forward local-main integration;
- missing remote/upstream, remote divergence, or non-fast-forward push;
- candidate history containing unrelated user changes or commits;
- any push that would require merge, rebase, history rewrite, or force-push.
