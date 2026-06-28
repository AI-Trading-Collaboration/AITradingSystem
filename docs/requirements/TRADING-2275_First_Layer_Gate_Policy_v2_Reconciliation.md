# TRADING-2275 First-Layer Gate Policy v2 Reconciliation

最后更新：2026-06-28

## 背景

TRADING-2274 证明 current first-layer gate 体系不是全部过严：
`no_major_regression_in_defensive_probe` 有正边际贡献，应保留为强 performance gate；
但 `not_2023_plus_only` 过度阻断高 utility 候选，应从 binary block 语义降级。
本批把 first-layer gate policy 从 binary block 模式调整为分层 gate policy，同时保持
research / production 边界。

## 范围

- 基于 TRADING-2274 `recommended_gate_policy.yaml` 和 ablation artifacts 生成
  gate policy v2 reconciliation。
- 固化 hard research gates：PIT / no-lookahead、data quality、actual-path only、
  no broker action、owner approval 和 production boundary。
- 保留 `no_major_regression_in_defensive_probe` 为强 performance gate，不允许收益改善自动豁免。
- 将 `not_2023_plus_only` 从 binary block 改为 owner-review risk flag。
- 将 beta / TQQQ dependency gates 标记为 inconclusive diagnostic gates，并要求保留
  exposure attribution 字段。
- 将 probability threshold 0.55 / 0.60 移入 threshold sensitivity / calibration 语义。
- 将 all-slices style gate 改为 review gate：严重 slice regression 可 blocked，轻微
  regression 进入 owner review。
- 单独记录 active selection rule 当前 accept=0，且 gate policy v2 不允许自动 promotion；
  下一步必须执行 TRADING-2276 active selection rule audit。

## 输出

- `outputs/research_trends/first_layer_gate_policy_v2/recommended_gate_policy_v2.yaml`
- `docs/research/gate_policy_v2_reconciliation_report.md`
- `docs/research/owner_review_gate_semantics.md`
- `docs/research/active_selection_rule_audit_plan.md`

## 非目标

- 不修改 active first-layer selection rule。
- 不训练或重跑 challenger model。
- 不把 owner-review state 解释为 promotion-ready。
- 不恢复 promotion、paper-shadow、production 或 broker action。

## 验收标准

- 每个 TRADING-2274 mandatory gate 都映射到 v2 分层语义。
- Hard research gates 明确为不可豁免 hard block。
- `no_major_regression_in_defensive_probe` 输出 `keep_as_strong_performance_gate` 或等价强门禁动作。
- `not_2023_plus_only` 输出 `OWNER_REVIEW_REQUIRED`，保留 risk flag 和 tradeoff explanation。
- Beta / TQQQ gates 输出 diagnostic / inconclusive，并列出 exposure attribution requirement。
- Probability 0.55 / 0.60 输出 threshold sensitivity / calibration artifact requirement。
- all-slices gate 输出 severe vs minor slice regression 处理语义和 slice tradeoff summary requirement。
- active selection rule plan 明确 current accept=`0`、不允许自动 promotion、下一步为 TRADING-2276。

## 进展

- 2026-06-28：新增并进入 `IN_PROGRESS`；本批只生成 gate policy v2 reconciliation
  artifacts，不改变 active policy，不进入 promotion/paper-shadow/production/broker。
- 2026-06-28：实现完成并转入 `VALIDATING`；新增
  `aits research trends first-layer-gate-policy-v2-reconciliation`，生成
  `recommended_gate_policy_v2.yaml`、`gate_policy_v2_reconciliation_report.md`、
  `owner_review_gate_semantics.md` 和 `active_selection_rule_audit_plan.md`。
  真实 run：current performance gates 后候选数=`1`，active selection accept=`0`；
  `no_major_regression_in_defensive_probe` 保留为 strong performance gate；
  `not_2023_plus_only` 降为 `OWNER_REVIEW_REQUIRED` risk flag；beta/TQQQ 降为
  diagnostic attribution；0.55/0.60 进入 threshold sensitivity；all-slices style
  gates 进入 severity-based review semantics。promotion/paper-shadow/production
  继续 false，broker_action=`none`。
- 2026-06-28：验证通过 Ruff、compileall、focused parallel pytest（46 passed）、
  docs freshness、`git diff --check` 和 `contract-validation` tier（193 passed）；
  runtime artifact=`outputs/validation_runtime/contract-validation_20260628T141300Z/test_runtime_summary.json`。
