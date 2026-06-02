# TRADING-071 ETF Allocation Dual-Track Weight Calibration

## Context

TRADING-071 builds a dual-track ETF weight calibration workflow:

- Track A uses bounded historical backtest search to generate candidate initial ETF
  allocation weights.
- Track B keeps real forward evidence in observe-only / candidate-only mode so the
  historical shortlist is not treated as a production conclusion.

The workflow must produce candidate initial weights, shadow-enrollment
recommendations, overfit diagnostics, comparison against forward evidence, proposal-only
manual review packages, Reader Brief visibility, and a final validation gate. It must
not mutate production baseline weights or trigger broker action.

## Safety Boundary

All TRADING-071 config, search results, registry records, forward enrollment records,
evidence aggregation, diagnostics, proposals, reports, Reader Brief summaries, and
validation outputs must keep:

```text
observe_only = true
candidate_only = true
production_effect = none
broker_action = none
manual_review_required = true
```

Disallowed behavior:

- overwrite baseline ETF weights;
- apply candidate weights to production config;
- place orders or enable broker action;
- automatically promote a candidate to production;
- use unbounded search by default.

## Default Scope

Initial universe:

```text
SPY, QQQ, SMH, SOXX, CASH
```

Default market regime:

```text
ai_after_chatgpt
```

Default requested historical conclusion window:

```text
2022-12-01 onward
```

Pre-2022 data may be used for warm-up or comparison, but reports must state why it is
not the primary AI-cycle conclusion window.

## Subtasks

|ID|Name|Status|Acceptance|
|---|---|---|---|
|TRADING-071A|Historical Weight Search Config|DONE|Config exists, bounded search constraints validate, safety fields are mandatory, invalid configs fail fast|
|TRADING-071B|Historical Weight Iteration Engine|DONE|Bounded candidates are generated, backtested, scored, ranked, persisted, and never applied|
|TRADING-071C|Walk-Forward / Regime Robustness Evaluation|DONE|Candidates are evaluated across full period, walk-forward windows, and regime slices|
|TRADING-071D|Candidate Initial Weight Set Registry|IN_PROGRESS|Selected candidate weights are stored as candidate-only records with safe statuses|
|TRADING-071E|Dual-Track Forward Enrollment|READY|Selected safe candidates can be explicitly enrolled into forward observation|
|TRADING-071F|Backtest vs Forward Evidence Aggregator|READY|Historical expectation and real forward evidence are linked and gap metrics are computed|
|TRADING-071G|Overfit Risk and Stability Diagnostics|READY|High-return but unstable candidates receive explainable overfit risk bands|
|TRADING-071H|Candidate Weight Proposal Generator|READY|Only evidence-linked proposal/review actions are generated; unsafe proposal types are rejected|
|TRADING-071I|Dual-Track Calibration Report|READY|JSON/Markdown report summarizes search, robustness, forward comparison, overfit diagnostics, proposals, and source links|
|TRADING-071J|Reader Brief Dual-Track Calibration Section|READY|Reader Brief surfaces top candidate, forward status, overfit risk, candidate status, safety, and report link|
|TRADING-071K|Dual-Track Calibration Validation Gate|READY|Final gate fails closed on unsafe states, unbounded search, unsafe proposals, or missing workflow pieces|

## Acceptance Criteria

- TRADING-071A through TRADING-071K are implemented with focused tests.
- Historical search config exists at `config/etf_portfolio/weight_search.yaml`.
- Historical search is bounded and config-driven.
- Candidate weights sum to 1.0 and satisfy asset/sleeve constraints.
- Candidate ranking uses risk-adjusted and robustness components, not total return alone.
- Walk-forward and regime-specific weakness is visible in outputs.
- Candidate initial weight registry remains candidate-only.
- Forward enrollment is explicit and safe.
- Backtest vs forward evidence aggregation handles insufficient data as
  `needs_more_forward_data` rather than producing false conclusions.
- Overfit risk diagnostics can flag historically attractive but fragile candidates.
- Proposals are evidence-linked and proposal-only.
- Reader Brief exposes the weight calibration state without running upstream commands.
- Validation gate passes only when the workflow is complete and safe.
- Runtime artifacts remain untracked.
- Documentation, artifact catalog, system flow, operations runbook, README, and task
  register are updated.

## Validation Commands

```bash
python -m pytest tests -q
python -m ruff check config src tests scripts docs
python -m compileall -q src tests scripts
git diff --check
python -m ai_trading_system.cli etf weight-calibration validate
```

If the final CLI changes, this document and the task register must record the actual
command.

## Progress Notes

- 2026-06-02: TRADING-071 新增为 P0 `IN_PROGRESS`。根据 owner 提供的 dual-track
  ETF weight calibration 计划，开始建立 historical search + forward evidence
  validation workflow；当前阶段只允许 candidate initial weights 和 proposal-only
  manual review package，不允许 production baseline replacement、broker action 或自动
  promotion。
- 2026-06-02: TRADING-071A 完成。新增 `config/etf_portfolio/weight_search.yaml`、
  weight search pydantic schema/loader、cross-config validation、`aits etf
  weight-calibration validate-config` 和 focused tests；验证通过 full pytest、ruff、
  compileall、diff check 和 config CLI。TRADING-071B 进入 `IN_PROGRESS`，下一步实现
  bounded historical candidate generation/backtest/scoring engine。
- 2026-06-02: TRADING-071B 完成。新增 bounded grid candidate generator、static
  candidate historical backtest engine、HistoricalWeightScore component scoring、hard
  blockers、deterministic ranking、search run JSON/Markdown/CSV writers、`aits etf
  weight-calibration search` 和 focused tests；outputs 明示 candidate cap、market
  regime、requested date range、baseline/benchmark comparison、`production_weights_mutated=false`
  和 `applied_weight_set=null`。TRADING-071C 进入 `IN_PROGRESS`，下一步扩展 walk-forward
  和 regime robustness evaluation。
- 2026-06-02: TRADING-071C 完成。Search payload 新增 `robustness_evaluation`、
  `robustness.json`、full-period/walk-forward/regime-slice metrics、insufficient-slice
  handling 和 stability score；HistoricalWeightScore 的 `regime_robustness_score`
  现在来自 slice stability summary。TRADING-071D 进入 `IN_PROGRESS`，下一步实现
  candidate initial weight set registry。
