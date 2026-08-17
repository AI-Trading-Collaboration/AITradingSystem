# TRADING-2532 — QQQ Options daily transport session-finalization V2 zero-order external validation admission and execution V1

- priority: `P0`
- status: `BASELINE_DONE`
- owner: Project Owner（一次性 external authority）；Codex capability coordinator（admission 后执行）
- production effect: `none`
- broker action: `none`

## 1. Why this successor exists

TRADING-2530 的一次真实零订单采集得到 `182 PRESENT / 1020 MISSING`，但
TRADING-2531 已证明 v1 collector 存在两个诊断混淆：当天第一条无 chain 的
`Slice` 会提前把整天终结为 `MISSING`，同时 contract-level zero 被错误当作
underlying 来源。TRADING-2531 已离线修复这两个合同缺陷并发布 proposal-only v2，
但没有产生新的外部事实。

本任务的唯一目的，是在新的一次性 Owner authority 下运行已冻结的 v2 zero-order
candidate，回答以下事实问题：

1. 整日合并并在 run finalization 后，真实的 option-chain present/missing 日数是多少；
2. 在 chain-present session 中，同 session RAW QQQ Equity bar 是否能提供 canonical
   underlying；
3. 2530 的 `1020 MISSING` 中有多少被 `SESSIONS_RECOVERED_AFTER_CHAINLESS` 明确识别为
   first-Slice event-order confounder；
4. 修复后仍未通过的轴属于 transport absence、underlying source absence/invalid，还是
   cross-field failure。

该运行仍不能证明 DQ/PIT admission、策略有效、收益稳健、selection/engine 解锁或可以交易。

## 2. Frozen authority inputs

- registration base:
  `bb6e43eff2dabfaa12d3f50354451075542380de`;
- current ordinary-pushed proposal main before this offline-admission lane:
  `c3e593b0e0739ca5f2494f3d55d52af019b0fc47`;
- target QuantConnect project id: `34808569`;
- requested range: `2021-02-22..2025-12-02`;
- expected session count: `1202`;
- policy file SHA-256:
  `cea137e0cb17b1c9594c359926015189f6fcfc2f472c4b6db72357d67a5d0cf5`;
- policy canonical SHA-256:
  `adc2e9cc0c889b814a97a5b8c4841c0890ef73c27dc07eddddc98ed2bed26f22`;
- contract content SHA-256:
  `f3c3918dd5dfd6fc1c6e84b63471c652d34090c9d50fab25d77dc58f9190b378`;
- contract canonical SHA-256:
  `97557122d50f6a82fe68f57286f7008bbe8bbdb511886f62f936d9fc1b6bb7e4`;
- project code LF byte count: `26901`;
- project code LF SHA-256:
  `0665a759a9db9bcae100133da9dd950e7f66597d4f19d00f01b26afb6a478f45`;
- predecessor evidence content SHA-256:
  `d47f3234f58e1a7114984a7a79a5090082f923b7e02c65a66dfa8b761321f792`;
- predecessor Results SHA-256:
  `2233b20a900c76cbb6938a96c635c5dabc5855349ac74ff684c8f1c657b752b7`;
- TRADING-2531 package manifest content SHA-256:
  `1f018f42b1149f5c04b559e3ca1b35e0418c841a75da6a6099dbff7ec67d1b4b`.

Any mismatch, missing artifact, repository drift after admission, or token expiry fails closed
before project mutation.

## 3. Exact external-effect boundary

Only after the exact Owner decision in section 4 is admitted:

- maximum QuantConnect project mutations: `1`;
- maximum Cloud backtests: `1`;
- maximum orders: `0`;
- maximum fills: `0`;
- candidate must remain zero-order;
- only bounded export-safe aggregate counters may be collected;
- raw option rows, contract identifiers, logs-as-data and Object Store export remain forbidden;
- no second attempt after the first project-mutation/run attempt, whether PASS or FAIL;
- authorization is single-use and invalidates on first run attempt;
- browser/API/CLI action outside the exact admitted execution path remains forbidden.

The TRADING-2530 counters remain immutable at `1 / 1 / 0 / 0`; this successor will maintain
its own independent counters beginning at `0 / 0 / 0 / 0` until admission.

## 4. Owner decision contract

Proposal publication does **not** authorize external action. Admission requires one new Owner
message whose fields bind the ordinary-pushed proposal SHA and all frozen inputs above:

```text
owner_decision:TRADING-2532:<YYYY-MM-DD>:authorize_single_zero_order_session_finalization_v2_external_validation_v1
ordinary_pushed_main_sha:<ORDINARY_PUSHED_PROPOSAL_MAIN_SHA>
registration_base_repository_code_sha:bb6e43eff2dabfaa12d3f50354451075542380de
policy_file_sha256:cea137e0cb17b1c9594c359926015189f6fcfc2f472c4b6db72357d67a5d0cf5
policy_canonical_sha256:adc2e9cc0c889b814a97a5b8c4841c0890ef73c27dc07eddddc98ed2bed26f22
contract_content_sha256:f3c3918dd5dfd6fc1c6e84b63471c652d34090c9d50fab25d77dc58f9190b378
contract_canonical_sha256:97557122d50f6a82fe68f57286f7008bbe8bbdb511886f62f936d9fc1b6bb7e4
project_code_lf_byte_count:26901
project_code_lf_sha256:0665a759a9db9bcae100133da9dd950e7f66597d4f19d00f01b26afb6a478f45
predecessor_evidence_content_sha256:d47f3234f58e1a7114984a7a79a5090082f923b7e02c65a66dfa8b761321f792
predecessor_results_sha256:2233b20a900c76cbb6938a96c635c5dabc5855349ac74ff684c8f1c657b752b7
package_manifest_content_sha256:7b49e82498c0ede285e93265007c5cbf89c42485eab5d5853018032eb95f80ce
target_project_id:34808569
requested_range:2021-02-22..2025-12-02
expected_session_count:1202
maximum_project_mutations:1
maximum_cloud_backtests:1
maximum_orders:0
maximum_fills:0
collector:codex_capability_coordinator
independent_reviewer:project_owner
authorization_expires_at_utc:<OWNER_SELECTED_EXPIRY_NOT_MORE_THAN_168_HOURS>
authorization_single_use:true
authorization_invalidates_on_first_run_attempt:true
```

`package_manifest_content_sha256` above is the final proposal-only package content seal.
It is not admission evidence until the strict proposal validator proves it and the exact
ordinary-pushed proposal SHA plus an unexpired Owner-selected expiry are substituted.

## 5. Execution and evidence sequence

1. Publish this proposal-only registration with no QuantConnect action.
2. Generate and validate a strict TRADING-2532 admission package that binds the final
   ordinary-pushed proposal main SHA and all exact hashes.
3. Admit the exact, unexpired, single-use Owner message; otherwise remain `PROPOSED`.
4. Recheck no prior 2532 project mutation/run attempt exists.
5. Perform at most one project mutation and one zero-order Cloud run.
6. Collect only the bounded aggregate payload and strict runtime provenance.
7. Independently verify totals, per-axis partitions, source code hash, run identity and
   `orders=0 / fills=0`.
8. Update Atlas only with facts admitted by the new result; keep DQ/PIT/strategy conclusions
   closed unless a separate governed evidence-admission task later authorizes them.

## 6. Acceptance criteria

1. Proposal and admitted authorization bind exact ordinary-pushed SHA plus every frozen hash.
2. Pre-run counters are `0 / 0 / 0 / 0`; post-attempt counters cannot exceed `1 / 1 / 0 / 0`.
3. One attempt only; failure never authorizes an automatic retry.
4. Runtime aggregate has exactly `1202` sessions on every public axis and passes all partition
   invariants.
5. `SESSIONS_RECOVERED_AFTER_CHAINLESS`, final missing count, canonical QQQ underlying
   provenance and ignored contract-zero diagnostics are exported without raw rows.
6. Result attribution explicitly separates collector-fix effect from remaining transport/data
   absence and does not rewrite TRADING-2530 evidence.
7. No order/fill path exists and runtime evidence confirms `0 / 0`.
8. Focused, authority and applicable formal validation pass before ordinary publication.

## 7. Current status

`BASELINE_DONE`: the offline admission and result-attribution baseline is implemented. It
strictly validates the exact Owner-token bytes and publication identity, consumes a
single-use authorization on the first run attempt, validates the v2 `1202`-session
aggregate partitions, rejects raw/log/Object Store/order carriers, and seals the
normalized evidence, action ledger and execution manifest. The parallel focused suite
passes `27` tests after one fail-closed seal-validation defect was found and corrected;
the first run remains recorded as `26 passed / 1 failed` rather than being hidden by a
serial rerun.

The combined admission, predecessor-contract, proposal, task-source, DevEx,
deprecation, report-flow and compatibility-authority suite passes `109` parallel
tests on the refreshed generated tree.

The first formal pass on that tree recorded Architecture `865 PASS`, Contract
`276 PASS`, Integration `995 PASS / 642 warnings`, and Reproducibility `24 PASS`.
Full then recorded `9174 passed / 7 failed / 3 skipped / 644 warnings`. All seven
failures had one presentation-governance root cause: Atlas had no explicit
page-effectiveness classification for the new TRADING-2532 successor, so the page
failed closed with `UNCLASSIFIED_SUCCESSOR_REVIEW_REQUIRED`; the admission parser,
transport contract, and trading paths were not the failing surfaces.

The successor classification and first-layer current-blocker narrative are now
updated. The Atlas focused suite passes `39` tests, and the regenerated local
canonical page is written to
`outputs/atlas/strategy_research_cited_query/trading_2470_v1/index.html`; its
final SHA-256 is recorded in the publication handoff and generated sidecar rather
than recursively embedded in a tracked source that itself changes the page.
The browser plugin refused to reload the local `file://` URL under its URL policy;
no bypass was attempted, and renderer/DOM contracts remain the auditable page QA.
The retained Full failure is the required parent evidence for the governed
failure-fix rerun after the final tracked tree is generated.
An expanded focused run then recorded `132 passed / 1 failed` because the local
ignored page sidecar still bound the prior task-event identity; the canonical
writer is required to refresh that sidecar, and no rendered or governed file is
hand-edited to mask the freshness failure.

This offline baseline is not external authority. The repository policy still has
`owner_token_status=PENDING_EXACT_OWNER_TOKEN`, no TRADING-2532 Owner decision has been
admitted, no QuantConnect action has occurred, and the task-local external counters remain
`0 / 0 / 0 / 0`. Applicable final formal gates must pass before ordinary publication.
