# TRADING-246 to 250 Smoothed Limited Adjustment Research Method

最后更新：2026-07-14

## 状态

BASELINE_DONE（ARCH-004G2.4CN canonical migration `COMPLETE_G2_4_CONTINUES`；等待独立 forward/PIT/DQ 证据）

## 背景

TRADING-239～245 最初把 `smooth_weights_3d` / `smooth_weights_5d` 提为待检验方向，但 ARCH-004G2.4CM 的可重算审计已确认：在当前 source-backed fixture 中，两种 smoothing transform 都没有改变 source rebalance target，不能沿用“3d 最值得 promotion”这一旧结论。本阶段保留两种 formal research-only method，是为了验证“平滑是否能降低 turnover / jump、以及代价是否可接受”这一预先提出的假设，不预设 primary / secondary 或胜者。`risk_capped_limited_adjustment` 只作为同一 Backfill lineage 下的研究对照，不把其旧 review label 当作当前比较事实。

本阶段把 `smooth_weights_3d` / `smooth_weights_5d` 从 experiment variant 正式实现为 `limited_adjustment` 的 research-only target method。平滑只变换 `limited_adjustment` 的 target weight path，不重新生成信号、不改变候选选择、不写 official target weights。

## 子任务

|ID|阶段|状态|验收标准|
|---|---|---|---|
|TRADING-246|Smoothed Limited Adjustment Spec & Config|BASELINE_DONE|新增 `smoothed_limited_adjustment_v1.yaml`，配置校验、normalized config 和 config report 可生成；safety 字段锁定 no broker / no production / no auto apply。|
|TRADING-247|Smooth Weights 3d / 5d Target Method Implementation|BASELINE_DONE|`smooth_weights_3d_limited_adjustment` 和 `smooth_weights_5d_limited_adjustment` 可从 model target 生成；输出 target weights、smoothing events、lag events 和 jump reduction summary；权重和硬约束保持有效。|
|TRADING-248|Smoothed Method Paper Shadow Backfill|BASELINE_DONE|paper shadow backfill 纳入 3d / 5d；生成 state history、trade ledger、summary；披露 data quality 和 research-only safety。|
|TRADING-249|Smoothed vs Limited / Risk-Capped / Baseline Comparison|BASELINE_DONE|输出 return、drawdown、turnover、rolling、regime、stability 和 lag cost 对比，覆盖 3d / 5d、limited、risk-capped 和 baseline/reference methods。|
|TRADING-250|Smoothed Research Method Review & Promotion Decision|BASELINE_DONE|生成 review pack、decision、owner checklist、Reader Brief section，并明确仍需 forward confirmation。|

## 设计决策

- `smooth_weights_3d_limited_adjustment` 与 `smooth_weights_5d_limited_adjustment` 都是候选；3d/5d 只描述 smoothing window，不预设 primary / secondary。推荐角色必须由同一 lineage、满足样本门槛的完整证据生成。
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

- 当前 source-backed fixture 中，3d/5d 都降低 turnover，但 drawdown delta 均未通过 reviewed non-worsening floor；3d 的 large-jump delta 还为正，因此两者均不可推荐。
- 当前 rolling window 有真实样本但 top/bottom rank frequency delta 都为 0，语义为 `MIXED`，不是缺失，也不是改善。
- 需要独立 forward/PIT、cost、regime 与 holdout 证据后再判断任何方法角色；不得根据本次 fixture 反向放宽 threshold。

## ARCH-004G2.4CN 迁移与正确性修复

2026-07-14 审计确认，2026-06-13 baseline 只能证明旧产物可生成，不能继续支撑“3d最值得promotion”这一当前结论：旧producer没有冻结并重验上游artifact/config，comparison允许跨Backfill混线，zero-sample与missing会经数值默认值被解释为0，review固定写入3d/5d角色且使用未治理hardcoded阈值，validator也不能从来源逐byte重建报告。

本slice将15个config/target/backfill/comparison/review callback迁至独立canonical模块，并要求：

- 五阶段bounded v2 input snapshots，写件前完成live validator、timezone cutoff、source/config checksum与exact lineage；
- Smoothed/Baseline/Risk必须来自同一个canonical Paper Backfill，日期区间一致且不得重复累计同一method observations；
- 缺样本保持`null`/`INSUFFICIENT_DATA`，不得补0后参与结论；
- return/drawdown/turnover/jump/lag/sample floor全部进入reviewed `evaluation_policy`；recommended/secondary由完整可用证据排序，无promotion-eligible证据时method必须为空，decision只能保持`CONTINUE_OBSERVATION`或按缺证据语义`DEFER`；
- validator从snapshot与live inputs重算JSON/JSONL/YAML/Markdown全部bytes，tamper/source drift/cross-lineage/future/non-finite均FAIL；
- 仍固定research-only、paper-shadow/manual-only、no official/no auto/no order/no broker、`production_effect=none`。

## 验证记录

2026-06-13 historical baseline（已被 G2.4CN correctness audit supersede，不得作为当前结论）：

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

当时的 Review decision 为 `CONTINUE_OBSERVATION`，但固定写入 recommended_method=`smooth_weights_3d_limited_adjustment` / secondary_method=`smooth_weights_5d_limited_adjustment`；由于当时允许跨 Backfill lineage、missing-to-zero 且 selection role 不是证据驱动，这两个 method role 现已撤销。

2026-07-14 G2.4CN source-backed contract fixture：requested/actual state range=`2022-12-01..2024-02-29`，63 rebalances，3d/5d 各 315 smoothing events、11 lag events；共同 return observations=326，DQ=`PASS_WITH_WARNINGS`。3d 相对 `limited_adjustment` 的 return/drawdown/turnover/large-jump delta 为 `-0.0012239743/-0.0000650563/-0.0962240287/+1`；5d 为 `-0.0017790195/-0.0000172701/-0.1151699146/-1`。3d/5d rolling 均为 `MIXED`，stability 分别为 `MIXED/IMPROVED`，lag 均为 `LOW`。Reviewed evaluation policy 下两者都因 drawdown 未改善而不具备 promotion eligibility，3d 还因 jump 变差被阻断；最终 decision=`CONTINUE_OBSERVATION/LOW`，recommended_method=`null`，secondary_method=`null`，observation candidates 仅表示继续收集证据。

这里的 `2022-12-01` 是 AI-cycle requested conclusion window 起点，不是 2021 长期 context。若未来单独使用 `2021-02-22` context/stress window，必须另行标记并与 AI-cycle headline 分开；本 fixture 的 workflow PASS 只证明计算、血缘和 validator 可复算，不证明策略优越。

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
