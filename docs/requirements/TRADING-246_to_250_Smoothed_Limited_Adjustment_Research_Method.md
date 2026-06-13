# TRADING-246 to 250 Smoothed Limited Adjustment Research Method

最后更新：2026-06-13

## 状态

VALIDATING

## 背景

TRADING-239～245 的 experiment factory 显示，`smooth_weights_3d` 是当前最值得 promotion 的 variant，`smooth_weights_5d` 适合作为 secondary / sensitivity candidate。`risk_capped_limited_adjustment` 已完成 research-only 实现但 review 结论为 `REJECT`，主要问题是收益保留不足且 rolling consistency 没有改善。

本阶段把 `smooth_weights_3d` / `smooth_weights_5d` 从 experiment variant 正式实现为 `limited_adjustment` 的 research-only target method。平滑只变换 `limited_adjustment` 的 target weight path，不重新生成信号、不改变候选选择、不写 official target weights。

## 子任务

|ID|阶段|状态|验收标准|
|---|---|---|---|
|TRADING-246|Smoothed Limited Adjustment Spec & Config|VALIDATING|新增 `smoothed_limited_adjustment_v1.yaml`，配置校验、normalized config 和 config report 可生成；safety 字段锁定 no broker / no production / no auto apply。|
|TRADING-247|Smooth Weights 3d / 5d Target Method Implementation|VALIDATING|`smooth_weights_3d_limited_adjustment` 和 `smooth_weights_5d_limited_adjustment` 可从 model target 生成；输出 target weights、smoothing events、lag events 和 jump reduction summary；权重和硬约束保持有效。|
|TRADING-248|Smoothed Method Paper Shadow Backfill|VALIDATING|paper shadow backfill 纳入 3d / 5d；生成 state history、trade ledger、summary；披露 data quality 和 research-only safety。|
|TRADING-249|Smoothed vs Limited / Risk-Capped / Baseline Comparison|VALIDATING|输出 return、drawdown、turnover、rolling、regime、stability 和 lag cost 对比，覆盖 3d / 5d、limited、risk-capped 和 baseline/reference methods。|
|TRADING-250|Smoothed Research Method Review & Promotion Decision|VALIDATING|生成 review pack、decision、owner checklist、Reader Brief section，并明确仍需 forward confirmation。|

## 设计决策

- `smooth_weights_3d_limited_adjustment` 是 primary candidate，`smooth_weights_5d_limited_adjustment` 是 stronger smoothing / lag-risk 对照。
- 平滑公式使用配置化 exponential smoothing：`previous + alpha * (base - previous)`，并受 `max_daily_total_weight_change` 和 `max_single_symbol_daily_change` 限制。
- `sideways_choppy` 可降低 effective alpha 并收紧 total daily change，用于减少 signal churn / weight jumps。
- `strong_recovery` 可提高 effective alpha，并记录 lag diagnostics，用于暴露 fast regime change 中可能的滞后成本。
- 硬约束来自 smoothed config 和 model target constraints：总权重保持 1.0、非负、min cash、max single symbol、max semiconductor、max total risk asset。
- 所有 artifact 固定 `research_target_only=true`、`paper_shadow_only=true`、`not_official_target_weights=true`、`broker_action_allowed=false`、`broker_action_taken=false`、`order_ticket_generated=false`、`production_effect=none`、`auto_apply=false`。

## 不做范围

- 不接 broker API。
- 不读取或修改 owner real portfolio。
- 不写 official target weights。
- 不生成 order ticket。
- 不修改 `position_advisory_v1.yaml`。
- 不自动 owner approval。
- 不把 review decision 解释为 production promotion。
- 不实现 P2 的 `regime_gated_limited_adjustment`、cooldown hybrid 或图表化 report。

## 验收命令

```bash
aits etf dynamic-v3-rescue smoothed-limited config-validate --config config/etf_portfolio/dynamic_v3_rescue/smoothed_limited_adjustment_v1.yaml
aits etf dynamic-v3-rescue validate-smoothed-limited-config --config config/etf_portfolio/dynamic_v3_rescue/smoothed_limited_adjustment_v1.yaml
aits etf dynamic-v3-rescue smoothed-limited generate --target-id <model_target_id>
aits etf dynamic-v3-rescue validate-smoothed-limited --smoothed-id <smoothed_id>
aits etf dynamic-v3-rescue smoothed-backfill run --config config/etf_portfolio/dynamic_v3_rescue/paper_shadow_backfill_v1.yaml
aits etf dynamic-v3-rescue validate-smoothed-backfill --backfill-id <smoothed_backfill_id>
aits etf dynamic-v3-rescue smoothed-comparison run --smoothed-backfill-id <smoothed_backfill_id> --baseline-backfill-id <baseline_backfill_id> --risk-capped-backfill-id <risk_capped_backfill_id>
aits etf dynamic-v3-rescue validate-smoothed-comparison --comparison-id <comparison_id>
aits etf dynamic-v3-rescue smoothed-review pack --comparison-id <comparison_id> --smoothed-backfill-id <smoothed_backfill_id>
aits etf dynamic-v3-rescue validate-smoothed-review --review-id <review_id>
```

必需工程验证：

```bash
python -m pytest tests/test_smoothed_limited_config.py tests/test_smoothed_limited_generation.py tests/test_smoothed_backfill.py tests/test_smoothed_comparison.py tests/test_smoothed_review.py -q
python -m ruff check src tests
python -m compileall -q src tests
git diff --check
```

## 开放问题

- `smooth_weights_3d` 在本次 latest data backfill 中相对 `limited_adjustment` 保留收益并降低 turnover，但 rolling consistency 仍为 `WORSE`，因此 review decision 保持 `CONTINUE_OBSERVATION`。
- `smooth_weights_5d` 在本次 latest data backfill 中 lag risk 为 `LOW`，但仍需 owner 判断是否作为 secondary / sensitivity method 继续 forward confirmation。
- 即使 `smooth_weights_3d` 通过 review，也必须先保持 recommended research method，并继续 forward confirmation。

## 验证记录

2026-06-13 latest research chain:

- config report: `smoothed-limited-config_03cfff47dec7d82c`
- model target: `model-target_d462516663f56ba8`
- smoothed target: `smoothed-limited_eae4d8aa3efe7669`
- smoothed backfill: `smoothed-backfill_27939e31bfdf54c6`
- baseline backfill: `paper-shadow-backfill_a694e8cf129d6c39`
- risk-capped reference backfill: `risk-capped-backfill_3d41bb93e038bbe4`
- comparison: `smoothed-comparison_6e51482964e50fab`
- review: `smoothed-review_3275f9ae7fde2ebb`

Backfill date range: `2022-12-01` 到 `2026-06-12`，market regime=`ai_after_chatgpt`，data_quality=`PASS_WITH_WARNINGS`。

Key comparison vs `limited_adjustment`: total_return_delta=`0.003435815`，max_drawdown_delta=`0.0008252445`，turnover_delta=`-0.4557667881`，conclusion=`smoothed_better`。

Review decision: `CONTINUE_OBSERVATION`，recommended_method=`smooth_weights_3d_limited_adjustment`，secondary_method=`smooth_weights_5d_limited_adjustment`，decision_confidence=`LOW`，requires_forward_confirmation=`true`。

Validation passed:

- `aits etf dynamic-v3-rescue validate-smoothed-limited-config`
- `aits etf dynamic-v3-rescue validate-smoothed-limited`
- `aits etf dynamic-v3-rescue validate-smoothed-backfill`
- `aits etf dynamic-v3-rescue validate-smoothed-comparison`
- `aits etf dynamic-v3-rescue validate-smoothed-review`
- `aits etf dynamic-v3-rescue validate`
- `aits etf dynamic-v3-rescue artifacts validate --family dynamic_v3_rescue`
- `aits docs report-contract --as-of 2026-06-13`
- `aits reports reader-brief --latest`
- `aits reports validate-reader-brief --latest`
- `python -m ruff check src tests`
- `python -m compileall -q src tests`
- `git diff --check`
- `python -m pytest tests -q` -> `2410 passed, 640 warnings`

`aits reports index --latest` 结果为 `PASS_WITH_WARNINGS`，原因是当前 registry 中既有 missing/stale artifacts；本任务新增 smoothed report ids 已通过 documentation contract 覆盖检查，Reader Brief 本身为 `OK` 且 warnings=`0`。
