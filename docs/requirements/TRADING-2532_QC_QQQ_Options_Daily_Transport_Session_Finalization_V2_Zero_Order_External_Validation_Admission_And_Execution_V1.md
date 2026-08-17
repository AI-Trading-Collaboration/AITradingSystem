# TRADING-2532 — QQQ Options daily transport session-finalization V2 zero-order external validation admission and execution V1

- priority: `P0`
- status: `DONE`
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
message whose fields separately bind the immutable proposal publication and the exact
ordinary-pushed admission implementation. This separation is required because the admission
parser did not exist at the earlier proposal commit; treating those two commits as one identity
would make admission impossible.

```text
owner_decision:TRADING-2532:<YYYY-MM-DD>:authorize_single_zero_order_session_finalization_v2_external_validation_v2
proposal_publication_main_sha:c3e593b0e0739ca5f2494f3d55d52af019b0fc47
ordinary_pushed_admission_main_sha:<EXACT_CURRENT_LOCAL_AND_ORIGIN_MAIN_SHA>
admission_identity_contract_content_sha256:93671fc9c4f4251826d80e62f5e790bd18b453dc08780df93ecbedbcfbf2644c
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
ordinary-pushed admission implementation SHA plus an unexpired Owner-selected expiry are
substituted. The published proposal package remains immutable; a separate v2 admission-identity
contract records this correction and its own exact content seal.

The Owner message is admitted directly from `PROJECT_OWNER_CURRENT_CODEX_DIALOG`. No tracked
policy mutation is permitted after receiving it: such a mutation would change the very main SHA
the token must bind. Instead, the parser validates the exact token bytes and v2 identity-contract
seal, requires `local main = origin/main = ordinary_pushed_admission_main_sha`, and emits a sealed
unused admission receipt before any external attempt. The first attempted project mutation/run
then consumes that receipt once.

## 5. Execution and evidence sequence

1. Publish this proposal-only registration with no QuantConnect action.
2. Generate and validate a strict TRADING-2532 admission-identity v2 contract that preserves the
   proposal SHA and lets the later Owner message bind the exact already-published implementation
   main SHA without a recursive tracked mutation.
3. Admit the exact, unexpired, single-use Owner message directly into a sealed local receipt;
   otherwise remain `AWAITING_EXACT_OWNER_TOKEN_DIRECT_ADMISSION`.
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

`DONE`: exact v2 Owner authority has been admitted and consumed by the sole permitted
project mutation and Cloud run. Backtest `acf111f24d09a41870f9a23e93fcbe3b` completed; its
manually downloaded Results JSON is `814999` bytes with SHA-256
`5d3220342c96217f2c4a4d624b0dc7fbbcad98427de728e749dc2e4f3168d50d`. The strict result
validator passes and seals export-safe evidence content SHA-256
`ffa9faafd1d480282bcfe1c07c896f538f26d2b23d7d7d8356460bc881e0bc49`. External counters
are permanently `1 / 1 / 0 / 0`; the consumed token does not authorize a retry.
The terminal tracked package, evidence-binding regression and Atlas projection pass `58`
parallel focused tests. Applicable final formal results and ordinary publication identity are
recorded in runtime artifacts and the publication handoff so this tracked requirement does not
create a self-referential commit identity. Any non-PASS formal result must block publication;
because canonical terminal task events are immutable, the failure and correction are appended as
same-status `DONE` evidence while ordinary push remains forbidden until corrected final formal
passes.

The first final-tree Architecture run is retained as failure evidence at
`outputs/validation_runtime/architecture-fitness_20260817T151852Z/test_runtime_summary.json`:
`864 passed / 1 failed`. Its only failed surface reported stale deterministic DevEx module,
test and aggregate-shadow manifests after the tracked renderer, tests and system-flow changes.
The canonical `scripts/architecture_devex.py generate` writer refreshed exactly those generated
artifacts and now reports architecture fitness `PASS`; publication remains blocked until the
failure-fix validation chain passes on the corrected final candidate.

That corrected candidate passed Architecture `865`, Contract `276`, Integration `995` with
`642` warnings, and Reproducibility `24`. Its Full run is retained at
`outputs/validation_runtime/full_20260817T155116Z/test_runtime_summary.json` with
`9181 passed / 6 failed / 3 skipped / 644 warnings`. All six failures share the same generated
authority root cause: `docs/system_flow.md` had changed from the last sealed report-flow and
compatibility authority snapshots. The canonical report-flow writer now binds the live
`2230062` bytes and SHA-256
`41451feb05a17e0e33373af56ffa651d374e33e3f338af2d643e88b002065dac`; the compatibility
authority and DevEx manifests were then rebuilt in their required order. No strategy, evidence,
DQ/PIT or external-action behavior was involved in these failures. Publication remains blocked
until the new exact candidate passes the complete failure-fix chain, including one Full rerun
bound to the retained failed Full parent artifact.

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

The paragraphs above record the pre-execution offline baseline and its retained validation
history. They are not the current external state: section 7.2 is now the canonical execution
and result-attribution summary. Applicable final formal gates must still pass before ordinary
publication.

### 7.1 Publication-identity defect found after baseline publication

Post-publication read-only audit found that v1 required both runtime Git refs to equal proposal
commit `c3e593b0e0739ca5f2494f3d55d52af019b0fc47`, while the admission parser itself was first
published at `1002cfd21de4f9ca33f816f9a418c4a256b7d1bd`. The condition was therefore
unreachable: checking out the proposal removes the parser, while running the parser makes the
Git equality check fail. No Owner token or external action was attempted against this defective
contract.

The required repair is the v2 two-layer identity contract described in section 4. It must retain
the original proposal seal, bind the final executable main dynamically in the Owner token,
require local/remote equality at admission, avoid any post-token tracked mutation, and preserve
all `1 / 1 / 0 / 0`, single-use, export-safe and no-trading boundaries. Focused and applicable
formal validation must pass again before ordinary publication.

The repair is now implemented offline. The frozen v2 contract is
`inputs/research/qqq_options/trading_2532_session_finalization_v2_external_validation_admission_identity_v2/contract.json`
with content SHA-256
`93671fc9c4f4251826d80e62f5e790bd18b453dc08780df93ecbedbcfbf2644c`;
the copy-ready request artifact has file SHA-256
`051a27ad90f65daee5f6962a07ec21ca2f92f8f8b406177e9a11bd1fac89b77d`.
The focused v2 suite passes `32` parallel tests, including contract/request tamper,
proposal/admission-main distinction, dynamic ref mismatch, direct sealed admission and rejection
of post-token tracked policy mutation. At that repair-publication checkpoint, external counters
were still `0 / 0 / 0 / 0`; section 7.2 records the later separately authorized execution.

After regenerating DevEx, report-flow, compatibility and Atlas sidecar authority, the expanded
admission/proposal/predecessor/task-source/authority/Atlas suite passes `138` parallel tests.
The local canonical page is regenerated from the current task projection; its final hash is kept
in the generated sidecar and publication handoff rather than embedded in this tracked source.

### 7.2 Admitted single execution and result attribution

The exact Owner token was admitted at `2026-08-17T11:28:22.916240Z` against
`local main = origin/main = 49e8a0aa2d918fc4cf53b8085dde217bf4c22405`. It bound token SHA-256
`d38a68d98be1593923e81fd27b7e786da0f78b52d3c70f66bc751ee3474e7202`, the frozen
`26901`-byte project code and all proposal/predecessor hashes. The first run attempt at
`2026-08-17T11:48:34.340Z` consumed and invalidated the authorization. There was one actual
project mutation, one Cloud backtest, no order and no fill.

The v2 result answers the collector question without changing TRADING-2530's immutable bytes:

- option-chain presence is `1201 PRESENT / 1 MISSING / 0 INVALID / 0 NOT_EVALUATED`;
- underlying price, bid/ask quote, Greeks, implied volatility, open interest, volume and
  cross-field consistency are each `1201 PRESENT / 0 MISSING / 0 INVALID / 1 NOT_EVALUATED`;
- `1020` sessions contained at least one chainless Slice event, but `1019` later recovered a
  non-empty chain in the same session; only `1` session remained never-chain;
- all `1201` chain-present sessions had a canonical same-session QQQ Equity source, and all
  `1201` contract-level zero values were ignored as required by the v2 source contract;
- every public axis partitions to exactly `1202` sessions, while orders, fills, fees, holdings
  and trading volume remain zero.

Therefore, `1019` of the old `1020 MISSING` observations are attributable to v1's premature
first-Slice terminalization rather than final whole-session chain absence. The single remaining
session is a final transport absence under this run. The former underlying/cross-field invalid
result is also removed by the reviewed same-session RAW QQQ Equity source rule. These are
collector and transport facts only: canonical DQ/PIT admission, policy calibration, strategy
validity, selection, engine activation and investment conclusions all remain unauthorized and
`POLICY_BLOCKED_CASH_PRESERVATION` remains in force.

The raw Results JSON stays outside Git. The tracked execution package contains only its hash,
byte count, strict normalized aggregates and sealed provenance. The browser client did not
persist separate timestamps for login observation, mutation start and mutation verification;
the action ledger therefore uses the exact run-consumption timestamp as their shared conservative
observation boundary and preserves causal order with explicit ordinals. This coarsens only
sub-run chronology, not counters, identities or result content; hash/seal and ordering validation
cover the retained evidence, and no retry is permitted merely to recover finer UI timing.

### 7.3 Local ownership correction and retained replay workspace

During local closeout, the first write lease omitted the Atlas renderer and three focused test
paths. No unleased implementation bytes were retained: those edits were reverted, the original
lease was released, an expanded lease was acquired, and the same reviewed changes were then
reapplied inside its declared scope. This correction did not modify the external result, token,
receipts, seals or counters.

The ignored replay workspace
`outputs/external_validation/trading_2532_session_finalization_v2_once_20260817/` is retained
after publication because it contains the unique raw Results payload needed to replay the tracked
hash-only evidence. The original browser download at
`G:/Download/Upgraded Magenta Gorilla.json` is also left intact. The tracked JSON files are
byte-for-byte sealed in the canonical package; the raw payload remains untracked. The next owner
is the coordinator of a separately governed DQ/PIT evidence-admission task. The exit condition is
an independently verified source hash plus an explicit permanent-retention decision; only then
may the ignored workspace be audited and removed with an exact absolute-path allowlist. Until
that point the raw evidence is recoverable from both retained locations and must not be cleaned.
