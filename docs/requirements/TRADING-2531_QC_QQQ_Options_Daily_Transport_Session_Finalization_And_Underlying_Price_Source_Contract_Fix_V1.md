# TRADING-2531 — QQQ Options daily transport session finalization and underlying-price source contract fix V1

- priority: `P0`
- status: `BASELINE_DONE`
- owner: Codex capability coordinator
- production effect: `none`
- broker action: `none`

## 1. Why this successor exists

TRADING-2530 sealed one authorized zero-order Cloud result over the exact
`2021-02-22..2025-12-02` research window. Its export-safe counters total exactly
`1202` sessions per axis, but report:

- option-chain presence: `182 PRESENT / 1020 MISSING`;
- underlying price: `182 INVALID / 1020 NOT_EVALUATED`;
- cross-field consistency: `182 INVALID / 1020 NOT_EVALUATED`.

Those counts are valid evidence of what the v1 collector emitted, but inspection of the
ordinary-pushed candidate code shows two collector-contract defects that prevent the
counts from identifying the provider/data failure:

1. `on_data` adds the date to `_seen` before checking for a non-empty option chain.
   QuantConnect `Slice` delivery does not guarantee that every time point contains an
   option chain. A chain-less first Slice therefore terminalizes the whole date as
   `MISSING` and any later same-date Slice is ignored. The existing aggregate cannot
   distinguish true all-day chain absence from this event-order confounder.
2. v1 reads `OptionContract.underlying_last_price` independently for every contract.
   In all 182 chain-present sessions the exported classification is `INVALID`, while
   quote, Greeks, IV, OI and volume are present. The collector must instead bind the
   underlying observation to the subscribed raw QQQ Equity bar for the same session;
   it must not silently substitute a stale Security price or treat a contract-level
   zero as a valid underlying observation.

This task fixes the diagnostic contract and candidate implementation. It does not
reinterpret the immutable TRADING-2530 result and does not claim that all 1020 missing
sessions were caused by event ordering.

## 2. Authority and external-action boundary

This is an offline `SINGLE_LANE` contract-fix task based on exact main
`4c4c108bb0af990833b325ca11cce5d21d8505c9` and the immutable TRADING-2530 source
result SHA-256
`2233b20a900c76cbb6938a96c635c5dabc5855349ac74ff684c8f1c657b752b7`.

It authorizes tracked code, tests, policy/contract documentation, generated authority
refresh and local validation only. It does not authorize:

- a second QuantConnect project mutation or Cloud backtest;
- API/CLI/HTTP/browser interaction with QuantConnect;
- raw option rows, logs-as-data, Object Store or contract identifiers;
- orders, fills, investment conclusions, DQ/PIT admission, selection or engine unlock.

The TRADING-2530 external counters remain fixed at project mutation / Cloud attempt /
orders / fills = `1 / 1 / 0 / 0`.

## 3. Required v2 contract

### 3.1 Session finalization

- A session may receive zero, one or multiple `Slice` events.
- A chain-less or empty-chain Slice is non-terminal and must not increment a session
  status counter.
- The first non-empty option chain may classify the chain-derived axes exactly once.
- A session with no non-empty chain across the whole run is classified `MISSING` only
  during finalization.
- Duplicate and reordered same-session events must be idempotent.

### 3.2 Underlying-price provenance

- The canonical observation is the finite positive close of the subscribed QQQ Equity
  `TradeBar` in the same research session, with raw normalization.
- A contract-level `underlying_last_price` may be measured only as a diagnostic of the
  v1 defect; it must not override the canonical same-session Equity observation.
- Missing same-session Equity evidence remains explicitly missing/not-evaluated. There
  is no silent previous-close, `Security.Price`, forward-fill or renderer fallback.
- Cross-field consistency must use the same canonical underlying observation and remain
  invalid/not-evaluated when that observation is unavailable.

### 3.3 Versioning and evidence

- Preserve the immutable v1 runtime/evidence schema and hashes.
- Introduce a distinct v2 runtime identity and policy with bounded aggregate keys.
- Add export-safe diagnostic counts that separate non-terminal chain-less Slice events,
  later-same-session chain recovery, sessions never receiving a chain, canonical Equity
  underlying presence/missing/invalid, and ignored contract-level zero values.
- Every public count must have an exact total/invariant and strict parser coverage.
- Candidate code remains zero-order and exports no raw rows or identifiers.

## 4. Implementation stages

1. Build a pure deterministic session reducer and event model independent of
   `AlgorithmImports`.
2. Reproduce the v1 confounder with ordered and permuted multi-Slice fixtures.
3. Generate the v2 zero-order QuantConnect candidate from the reviewed reducer contract.
4. Build strict proposal/parser artifacts with exact file and canonical SHA-256 values.
5. Refresh system flow, task authority and generated compatibility/report authorities.
6. Run focused validation and the applicable final formal tiers on one frozen tree.

## 5. Acceptance criteria

1. Tests prove that `no-chain -> chain` and `chain -> duplicate/no-chain` event orders
   produce the same single session classification.
2. A session is counted missing only at finalization, never on its first empty Slice.
3. Tests prove that a positive same-session QQQ bar repairs the v1 zero-underlying
   confounder, while a missing/zero bar fails closed without stale fallback.
4. v2 aggregate totals and provenance counts are deterministic, bounded and strictly
   validated; v1 evidence remains byte-immutable.
5. Candidate code has exact hashes, zero order paths and no raw/log/Object Store export.
6. No Cloud, project, order, fill or other external action occurs in this task.
7. Focused and applicable formal validation pass; local main is ordinary-pushed only
   after governed integration and closeout gates pass.

## 6. Current evidence status

`BASELINE_DONE`: the offline v2 reducer, policy, zero-order candidate and strict
proposal/package parsers are implemented. The reducer/package focused suite passed
`19` tests; the reducer plus Atlas readability/current-blocker suite passed `58`
tests; and the task-source/deprecation/DevEx/report-flow/compatibility authority suite
passed `79` tests. Generated authority validators are `PASS`.

Exact proposal/package identities are:

- `main.py`: `26901` bytes, SHA-256
  `0665a759a9db9bcae100133da9dd950e7f66597d4f19d00f01b26afb6a478f45`;
- `owner_decision_request.md`: `1085` bytes, SHA-256
  `c466c77a31e53dc3099eb10cab579023b690de90e7d5553b66adc5831673c1b8`;
- `proposal.json`: `1794` bytes, SHA-256
  `e4ed039271c3d0ec498c471641e52c9efab25d6fab29a95885719c0b8fc58b4f`;
- `session_finalization_contract.json`: `3681` bytes, SHA-256
  `97557122d50f6a82fe68f57286f7008bbe8bbdb511886f62f936d9fc1b6bb7e4`;
- package manifest content SHA-256:
  `1f018f42b1149f5c04b559e3ca1b35e0418c841a75da6a6099dbff7ec67d1b4b`.

These hashes identify an offline proposal; they are not execution authority. Runtime
attribution between collector error and genuine provider absence remains unproven until
a separately governed and explicitly authorized v2 zero-order validation is completed.
The immutable TRADING-2530 result and external counters remain unchanged at `1/1/0/0`.
