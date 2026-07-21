# TRADING-2453：Constraint-hit 全量拒绝诊断与 Owner Review

最后更新：2026-07-21

状态：`READY`

稳定任务 ID：`TRADING-2453_CONSTRAINT_HIT_REJECTION_DIAGNOSIS_AND_OWNER_REVIEW`

## 背景

TRADING-2452 正式有效 run
`trading2452-historical-seen_20260721T053621Z_144f31edee91` 在统一
`2021-02-22..2025-12-31` historical-seen 六 fold 上完成 1,800 个 train evaluations。
全部 evaluation 的 evidence 都是 `COMPLETE`，但 1,800 行全部为 `gate=reject`、
`selection_score=null`：所有行命中 `constraint_hit_rate_exceeds_policy`，第 6 fold 另有 96 行
命中 `constraint_hits_delta_exceeds_policy`。因此没有 train-only selected candidate，test 与
recent-known diagnostic 均未执行。

这是需要解释的负面研究证据，不是授权放宽 gate、扩候选或重新搜索参数的理由。

## 目标

1. 从冻结的 TRADING-2452 artifacts 重算并核对每个 fold/candidate/template 的 constraint-hit
   rate、hit count、delta、row count、gate reason 与相关 robustness fields。
2. 区分三类可能原因：候选本身普遍违反约束；约束命中统计口径与 policy intended effect 不一致；
   统一 2021 窗口暴露了旧 2022 窗口未覆盖的结构性风险。
3. 形成不改变现有结果的诊断报告和 owner decision pack，列出继续保持 KILL、修复明确的计算/语义
   缺陷后同 package 重跑，或另行预注册新假设等可审计路径。

## 阶段与验收

|阶段|内容|验收|
|---|---|---|
|S0|冻结输入与重算合同|绑定有效 run/package/policy/source hashes；重算 1,800 行 gate reason 与原 artifact exact match|
|S1|分层归因|按 fold、candidate template、policy hash、constraint type 输出分布和集中度；null 不转 0|
|S2|语义审计|逐项对照 reviewed constraint policy rationale、计算代码与报告字段，区分正确拒绝和实现缺陷|
|S3|Owner review pack|给出选项、风险、所需新授权与下一预注册边界；默认保持 KILL/PAUSE|

## 安全边界

- 不修改当前 constraint threshold、score、position cap、候选参数或 selection rule；
- 不读取 `2026-07-22` 之后 prospective holdout；
- 不执行 candidate expansion、parameter search、paper-shadow、promotion、production 或 broker/order；
- 如果发现计算或语义缺陷，先登记独立修复任务并由现有 artifact fail-closed，不直接在诊断中改值；
- 全部输出固定 `research_only=true`、`manual_review_required=true`、
  `production_effect=none`、`broker_action=none`。

## 依赖与下一责任方

- 依赖 TRADING-2452 S4 formal validation 与提交完成；
- Strategy lane 负责 S0～S3；Engineering lane 可并行继续 ARCH-004 既定工作，但不得同时修改
  central gate/policy、共享 task/system-flow/catalog；
- owner 只在 S3 review pack 后决定是否产生新的策略实现或预注册任务。
