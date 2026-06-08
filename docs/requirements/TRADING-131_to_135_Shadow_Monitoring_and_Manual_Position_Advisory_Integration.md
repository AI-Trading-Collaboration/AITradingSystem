# TRADING-131 to TRADING-135 Shadow Monitoring and Manual Position Advisory Integration

最后更新：2026-06-08

## 1. 背景

TRADING-126_to_130 已把 rebuilt observe pool 压缩为 shadow shortlist，并生成
candidate cluster、shadow shortlist、TARGET_ONLY / snapshot position advisory 和
position review pack。当前链路仍是一次性 review artifact，尚未接入持续 shadow
monitoring、daily position advisory、candidate disagreement tracking 和 owner review
journal。

本阶段把 `shadow_shortlist_id=4378b3ed3fc1be41` 代表的 shadow shortlist 模式升级为
持续观察入口。所有输出仍固定：

- `production_effect=none`
- `broker_action_allowed=false`
- `broker_action_taken=false`
- `owner_approval_required=true`
- `manual_review_required=true`
- 不自动生成 `production_candidate`
- 不写 official target weights、baseline config、production state 或 broker state

## 2. 子任务

|ID|标题|状态|验收重点|
|---|---|---|---|
|TRADING-131|Shadow Shortlist Daily / Weekly Monitoring Activation|VALIDATING|从 shadow shortlist 生成 daily / weekly monitor artifact，计算 promotion clock、weight delta、drift 和 recommendation。|
|TRADING-132|Manual Portfolio Snapshot Ingestion & Validation|VALIDATING|支持 manual YAML snapshot validation / normalize / report，fail closed 处理权重和值不一致、重复 symbol、负权重、broker import。|
|TRADING-133|Position Advisory Daily Report Integration|VALIDATING|从 shadow monitor run 生成 TARGET_ONLY / SNAPSHOT_DELTA daily advisory，输出 Reader Brief section。|
|TRADING-134|Candidate Disagreement & Consensus Drift Tracking|VALIDATING|从 monitor run 计算 symbol dispersion、pairwise disagreement、risk/cash exposure disagreement 和 HIGH_DISAGREEMENT manual_review gate。|
|TRADING-135|Owner Review Journal & Paper Action Log|VALIDATING|记录 daily advisory owner review、decision 和 paper-only action，保持 no broker action。|

## 3. 新增 CLI

Shadow monitor:

```bash
aits etf dynamic-v3-rescue shadow-monitor activate --shadow-shortlist-id <shadow_shortlist_id>
aits etf dynamic-v3-rescue shadow-monitor run --shadow-shortlist-id <shadow_shortlist_id> --as-of YYYY-MM-DD
aits etf dynamic-v3-rescue shadow-monitor report --latest
aits etf dynamic-v3-rescue validate-shadow-monitor-run --monitor-run-id <monitor_run_id>
```

Portfolio snapshot:

```bash
aits etf dynamic-v3-rescue portfolio-snapshot validate --snapshot <snapshot.yaml>
aits etf dynamic-v3-rescue portfolio-snapshot report --snapshot <snapshot.yaml>
aits etf dynamic-v3-rescue portfolio-snapshot normalize --snapshot <snapshot.yaml>
```

Position advisory daily:

```bash
aits etf dynamic-v3-rescue position-advisory daily-run --shadow-monitor-run-id <monitor_run_id> --config <position_advisory_v1.yaml>
aits etf dynamic-v3-rescue position-advisory daily-run --shadow-monitor-run-id <monitor_run_id> --config <position_advisory_v1.yaml> --portfolio-snapshot <snapshot.yaml>
aits etf dynamic-v3-rescue position-advisory daily-report --latest
aits etf dynamic-v3-rescue validate-position-advisory-daily --daily-advisory-id <daily_advisory_id>
```

Consensus drift:

```bash
aits etf dynamic-v3-rescue consensus-drift run --shadow-monitor-run-id <monitor_run_id>
aits etf dynamic-v3-rescue consensus-drift report --latest
aits etf dynamic-v3-rescue validate-consensus-drift --drift-id <drift_id>
```

Owner review:

```bash
aits etf dynamic-v3-rescue owner-review create --daily-advisory-id <daily_advisory_id>
aits etf dynamic-v3-rescue owner-review list
aits etf dynamic-v3-rescue owner-review report --latest
aits etf dynamic-v3-rescue owner-review record-decision --review-id <review_id> --decision monitor
aits etf dynamic-v3-rescue validate-owner-review --review-id <review_id>
```

## 4. Artifact Contract

Shadow monitor runs:

```text
reports/etf_portfolio/dynamic_v3_rescue/shadow_monitor_runs/<monitor_run_id>/
  shadow_monitor_manifest.json
  shadow_candidate_daily_results.jsonl
  shadow_candidate_weekly_summary.jsonl
  shadow_monitor_summary.json
  shadow_monitor_report.md
  reader_brief_section.md
```

Portfolio snapshots:

```text
reports/etf_portfolio/dynamic_v3_rescue/portfolio_snapshot/<snapshot_id>/
  snapshot_manifest.json
  normalized_positions.json
  portfolio_exposure_summary.json
  snapshot_validation_report.md
```

Position advisory daily:

```text
reports/etf_portfolio/dynamic_v3_rescue/position_advisory_daily/<daily_advisory_id>/
  daily_advisory_manifest.json
  daily_candidate_targets.jsonl
  daily_consensus_weights.csv
  daily_position_deltas.jsonl
  daily_advisory_actions.json
  daily_position_advisory_report.md
  reader_brief_section.md
```

Consensus drift:

```text
reports/etf_portfolio/dynamic_v3_rescue/consensus_drift/<drift_id>/
  consensus_drift_manifest.json
  candidate_pairwise_disagreement.csv
  symbol_weight_dispersion.csv
  consensus_drift_summary.json
  consensus_drift_report.md
```

Owner review:

```text
reports/etf_portfolio/dynamic_v3_rescue/owner_review_journal/
  owner_review_journal.jsonl
  paper_action_log.jsonl
  latest_owner_review.json
  owner_review_report.md
```

## 5. Snapshot Policy

Manual snapshot input may be real or simulated owner-provided data, but it is not a broker
import. Required validation:

1. `positions` weight plus `cash.weight` must be within `weight_sum_tolerance=0.005` of 1.0.
2. Position and cash value must match `total_equity` when values are supplied.
3. Duplicate symbols fail.
4. Negative weights or values fail.
5. Unsupported currencies fail.
6. Missing `as_of` fails.
7. `metadata.broker_imported=true` fails until broker import is explicitly designed.
8. `metadata.owner_reviewed=false` forces downstream advisory `manual_review_required=true`.

## 6. Advisory Interpretation

`TARGET_ONLY` means no current portfolio snapshot was provided; the report may show candidate
targets and consensus weights but cannot compute current-position delta.

`SNAPSHOT_DELTA` means a validated manual snapshot was provided; the report may show theoretical
delta from current manual weights to candidate/consensus target weights. It is still review-only
and cannot trigger a broker action.

Consensus drift directly gates daily advisory. If `disagreement_status=HIGH_DISAGREEMENT`,
`recommended_action` must be `manual_review` even when deltas appear small or within adjustment
limits.

## 7. Owner Review And Paper Action

Owner review records capture what the system recommended, what the project owner decided, and
whether a paper-only action was logged. `paper_adjustment` only writes a paper action log entry;
it does not mutate portfolio snapshot, broker state, official target weights, baseline config, or
production state.

## 8. Validation Plan

Focused tests:

- `test_shadow_monitor_activation.py`
- `test_portfolio_snapshot_validation.py`
- `test_position_advisory_daily.py`
- `test_consensus_drift.py`
- `test_owner_review_journal.py`

Required gates:

```bash
python -m pytest tests/test_shadow_monitor_activation.py tests/test_portfolio_snapshot_validation.py tests/test_position_advisory_daily.py tests/test_consensus_drift.py tests/test_owner_review_journal.py -q
python -m ruff check src tests
python -m compileall -q src tests
git diff --check
aits etf dynamic-v3-rescue validate
aits etf dynamic-v3-rescue artifacts validate --family dynamic_v3_rescue
```

## 9. Progress Notes

- 2026-06-08: 新增需求文档并进入 `IN_PROGRESS`。本阶段目标是从一次性 shadow shortlist /
  position review pack，升级为持续 shadow monitoring、daily manual position advisory、
  consensus drift gate 和 owner review journal。
- 2026-06-08: baseline implementation added. 新增 `shadow-monitor`、`portfolio-snapshot`、
  `position-advisory daily-run/daily-report`、`consensus-drift` 和 `owner-review` CLI；新增
  report registry、artifact catalog、system flow、README、operations runbook 和 Reader Brief
  integration；真实验收链路使用 `shadow_shortlist_id=4378b3ed3fc1be41` 生成 monitor run、
  TARGET_ONLY advisory、SNAPSHOT_DELTA advisory、consensus drift 和 owner review journal。
- 2026-06-08: 转入 `VALIDATING`。真实验收输出：activation `6df9887ad3ea5a5e`、
  monitor run `531f27b63a4c199d`、TARGET_ONLY advisory `45d89cf5b6315131`、
  SNAPSHOT_DELTA advisory `a3a1baf604a03ee9`、consensus drift `a365c32cc64b8005`、
  owner review `5e73e18e63e5920a`，latest snapshot validation `e5a86f456fe71738`。
  验证通过 focused pytest、existing dynamic-v3 tests、documentation contract、全量 pytest
  （2244 passed）、ruff、compileall、git diff check、dynamic-v3 root validation 和
  dynamic-v3 family artifact validation。剩余工作是 owner 复核真实 artifacts。
