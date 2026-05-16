# Shadow 参数验证收紧计划

状态：VALIDATING

最后更新：2026-05-16

关联任务：`CALIBRATION-014`、`CALIBRATION-012`、`CALIBRATION-013`、`CALIBRATION-015`、`CALIBRATION-016`、`CALIBRATION-017`

## 背景

当前 shadow 参数搜索已经能枚举 weight/gate 组合，并按 gate 后仓位计算
position-weighted return、回撤、换手和成本。2026-05-04 至 2026-05-14 的当前样本
显示，放松 valuation/risk/thesis/confidence/data-confidence gate 后，shadow return
可高于 production；但样本只有 available=8、pending=1、missing=2，且 Top trials
出现大量并列。

这说明当前搜索器适合作为 validation-only 诊断工具，但不能把短样本最优结果解释
为生产调权证据。下一步需要把“权重贡献”和“gate 放松贡献”拆开，同时收紧
objective 和 approved hard overlay 的生产边界。

## 目标

- 在 shadow parameter search 报告中输出 factorial attribution：
  `production weight x production observed gate`、`candidate weight x production gate`、
  `production weight x candidate gate`、`candidate weight x candidate gate`。
- 当 best trial 的收益主要由 gate candidate 解释时，报告必须可见，避免把 beta/暴露
  增加误读为权重 alpha。
- 默认 shadow parameter objective 从 pilot 搜索改为验证门槛：要求达到当前
  prediction diagnostic floor，且 excess return 必须为正；短样本只能输出
  `PASS_WITH_LIMITATIONS` 和诊断领先项。
- 在 hard overlay 执行层未接入前，`approved_hard` hard effect 必须 fail closed，不得
  以配置批准但下游未执行的状态进入生产 resolver。
- 清理已过期的 production weight profile metadata，避免审计字段继续表达
  “未接入 score-daily”的旧状态。

## 阶段拆解

|阶段|状态|验收|
|---|---|---|
|1. 任务登记与需求文档|VALIDATING|新增本需求文档和 `CALIBRATION-014` 任务登记，明确验收标准和生产边界。|
|2. Factorial attribution|VALIDATING|`search-shadow-parameters` 报告、manifest 和测试能显示 weight-only、gate-only、combined 与 baseline 的差异。|
|3. Objective 收紧|VALIDATING|默认 objective 配置要求验证级样本和正 excess；当前 8 个 available 样本不得被写成 eligible best trial。|
|4. Approved hard fail-closed|VALIDATING|带 hard effect 的 `approved_hard` overlay 在 production resolver 中报错，测试覆盖错误信息。|
|5. 流图和验证|VALIDATING|系统流图、目标测试、ruff、diff check 和当前样本 CLI smoke 通过，或记录真实数据阻塞。|

## 生产边界

- 本任务不修改 `scoring_rules.yaml`、`portfolio.yaml`、正式 `position_gate`、正式
  prediction ledger、rule card 或日报结论。
- Shadow 搜索结果仍为 `production_effect=none`。
- 没有 owner approval、promotion floor、forward shadow 和 rollback condition 时，不生成
  approved overlay。

## 状态记录

- 2026-05-16：新增并进入实现。原因：owner 要求把本轮权重/gate 回测评估结论登记，
  并继续推进实现和测试整条 validation-only 流程。
- 2026-05-16：从 IN_PROGRESS 改为 VALIDATING。原因：已实现 search factorial
  attribution、diagnostic-leading trial 降级展示、默认 objective 验证门槛、
  `approved_hard` hard effect fail-closed 和 production weight profile metadata 清理；
  完整当前样本搜索 `current_20260504_20260514_validation_v3` 为
  `PASS_WITH_LIMITATIONS`，51,612 trials，无 eligible best trial，diagnostic-leading
  为 `grid_weight_0118__grid_gate_0217`，primary driver 为 `gate`。
- 2026-05-16：验证补充。`python -m pytest -q` 通过 548 项，`python -m ruff check
  src tests` 通过，`git diff --check` 仅提示 `docs/task_register.md` 行尾将由
  CRLF 转 LF；未发现 whitespace error。
- 2026-05-16：后续强化已拆到
  `docs/requirements/shadow_parameter_attribution_promotion_contract_2026-05-16.md`。
  该后续任务补 cap-level attribution、独立 promotion contract、objective regularization
  和 search lineage，仍保持 validation-only。
