---
name: run-web-pro-git-review
description: Run an auditable planning handoff to a logged-in ChatGPT Web Pro session using a public or explicitly authorized Git exact commit. Use when the user asks Codex to consult Web Pro or GPT-5.6 Pro for large-module, architecture, strategy-research, governance, or roadmap planning; verify the selected Pro UI, ask for model and fallback evidence, require exact-commit file retrieval, recover interrupted sessions without duplicate submission, and reconcile the advisory answer against local repository authority.
---

# Run Web Pro Git Review

Use the browser as an external planning surface while keeping Git, local policy, and executable
guards authoritative. Never treat the webpage answer as implementation authorization.

## 1. Establish authority and outbound scope

1. Read the repository instructions, task register, relevant requirements, and applicable workflow
   skill before constructing the handoff.
2. Use read-only repository checks unless the user separately asks for implementation.
3. Resolve one immutable snapshot:
   - repository URL;
   - exact commit SHA;
   - exact tree URL;
   - exact blob URLs for every required file.
4. Prefer the user-selected commit. If the user asks for the current state, freeze the current
   audited commit and state which ref supplied it.
5. Confirm the repository is public or the user explicitly authorizes sending the selected context
   outside the local environment.
6. Send only minimal necessary facts. Exclude secrets, tokens, cookies, private local paths,
   known-unrelated exclusions, and unscoped private content.

Stop before browser submission when the exact commit cannot be resolved, outbound authority is
missing, authentication is unavailable, or the selected content contains unapproved private data.

## 2. Build the review packet

Read [prompt-template.md](references/prompt-template.md) and fill every required placeholder.

Require the webpage reviewer to:

- use only the exact commit, never moving `main`;
- list every successful and failed file retrieval;
- distinguish repository facts from prompt-supplied claims;
- start with `MODEL_IDENTITY_AND_ROUTING_RISK`;
- write `CANNOT_VERIFY_EXACT_BACKEND_ROUTE` when backend route metadata is unavailable;
- state that model self-report is not authoritative route evidence;
- provide ordered tasks, owner decisions, allowed/prohibited actions, artifacts, validation, exit
  conditions, falsification, downstream gates, and near-term sequencing;
- preserve the repository's safety and production boundaries.

Do not add facts that were not locally verified. Label unresolved or time-sensitive claims as items
the webpage reviewer must verify.

## 3. Submit through ChatGPT Web Pro

1. Use the `chrome:control-chrome` skill and the user's existing logged-in Chrome session.
2. Open a new ChatGPT conversation or the user-designated conversation.
3. Record separately:
   - account plan label;
   - selected composer model label;
   - any response-generation model label.
4. Select `Pro` when the UI offers it. If `Pro` is unavailable, stop and report
   `WEB_PRO_SELECTION_UNAVAILABLE`.
5. Enable web retrieval when the review requires public Git access.
6. Fill the unique composer with the complete packet.
7. Submit only after the user has explicitly asked for the external webpage review.
   When that current request is explicit and the packet contains only non-sensitive public Git or
   explicitly authorized context within the reviewed scope, submit directly after the checks above;
   do not ask for a second "send now" confirmation.
8. A prior explicit review request does not waive higher-priority confirmation or stop requirements
   for secrets, personal or sensitive data, private or unscoped content, file uploads, account or
   permission changes, paid-resource use, other external actions, or a second submission after an
   error. Treat material packet expansion as new outbound scope.
9. Save the conversation URL immediately after submission.

Do not use response latency, writing quality, subscription level, or a `Pro` UI label as proof of
the exact serving backend.

## 4. Wait and recover without duplicate submission

- Poll the visible generation state at intervals that keep the user informed.
- Do not click an “answer now” or equivalent early-return control unless the user requests it.
- Treat new retrieval/reasoning steps as progress.
- If a tab binding becomes stale, reopen the saved conversation URL in the same logged-in browser.
- Inspect the saved conversation before acting. Never resubmit when the original prompt or answer
  is already present.
- If the webpage returns a terminal error, preserve the conversation and report it as workflow
  evidence; do not replace it with local speculation.

Keep the completed result tab open for the user.

## 5. Classify model and routing evidence

Keep three evidence layers separate:

1. `UI_SELECTION`: what the account and composer visibly selected.
2. `MODEL_SELF_REPORT`: what the response environment says it is.
3. `BACKEND_ROUTE_ATTESTATION`: authoritative model identifier, route trace, fallback event, or
   equivalent platform metadata.

Use these outcomes:

- `UI_PRO_AND_SELF_REPORT_PRO_ROUTE_UNVERIFIED`: UI and self-report are consistent with Pro, but
  exact backend route is not attested.
- `ROUTING_MISMATCH_SIGNAL`: UI drifted from Pro or the response self-reported a non-Pro model.
- `ROUTING_ATTESTATION_UNAVAILABLE`: authoritative backend or no-fallback evidence is unavailable.
- `BACKEND_ROUTE_VERIFIED`: use only when the platform actually exposes authoritative route
  evidence; quote the exact field, not an inference.

The common webpage result is both
`UI_PRO_AND_SELF_REPORT_PRO_ROUTE_UNVERIFIED` and
`ROUTING_ATTESTATION_UNAVAILABLE`. Do not relabel that as a proven fallback or proven backend.

## 6. Validate repository retrieval and planning quality

For every required blob, record:

- requested exact URL;
- success, partial, or failure;
- evidence quoted or summarized by the webpage answer;
- whether the answer used an exact blob, connector-resolved blob, or an unverified branch page.

Fail the Git-retrieval portion when the answer silently substitutes moving `main`, claims unread
files were read, or omits required failures. Do not hide partial retrieval behind a general
“repository reviewed” statement.

Check that the plan:

- distinguishes completed work, newly removed blockers, remaining owner gates, and prohibited
  downstream stages;
- has typed early-stop outcomes;
- freezes shared contracts before parallel work;
- separates capability evidence from decision value and implementation authority;
- does not weaken frozen thresholds or expand the experiment after seeing results.

## 7. Reconcile locally before adoption

Compare every material recommendation with local:

- repository instructions;
- task register and supporting requirements;
- current exact Git state;
- executable preflight and validation guards;
- data-quality, PIT, production, and broker boundaries.

Local authority wins. Mark conflicts, stale assumptions, and useful advisory additions explicitly.
Do not create tasks, edit files, run research, or change external systems unless the user separately
authorizes that mutation. When implementation is authorized, enter the repository's governed
development workflow before writing.

## 8. Report the result

Lead with:

- whether the Web Pro workflow completed;
- the conversation URL;
- the exact commit reviewed;
- required-file retrieval success/failure;
- UI/self-report/backend-route classification;
- the recommended next task and its first stop condition;
- any mismatch between the webpage plan and repository authority.

State clearly that the webpage result is advisory and whether any local file or external system was
changed.
