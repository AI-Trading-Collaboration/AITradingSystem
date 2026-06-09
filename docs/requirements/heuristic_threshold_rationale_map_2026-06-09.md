# 启发式阈值逐项 rationale map 深化

状态：DONE

最后更新：2026-06-09

关联任务：`GOV-005`

## 背景

`GOV-004` 已经把投资解释路径中的 numeric literal 审计工具化，并要求关键 policy
配置具备 owner、status、rationale、validation 和 review metadata。并发复核指出，
当前 metadata 主要停留在 manifest 或 section 层；对于 `position_bands`、
`daily_conclusion`、`confidence_policy`、`robustness`、`promotion` 等内部 numeric
leaf，后续可以进一步建立逐阈值 rationale / validation 对照表。

该增强不阻断 `GOV-004` 归档。它的目标是提高配置审计深度，不改变任何生产评分、
仓位、回测、promotion、日报或 approved overlay 数值。

## 范围

优先覆盖以下已配置化但仍需要更细粒度 rationale 映射的 policy 区域：

- `config/scoring_rules.yaml` 的 `position_bands`、`daily_conclusion`、
  `confidence_policy` 和 source-type confidence 相关 numeric leaf。
- `config/backtest_validation_policy.yaml` 的 data credibility、robustness、
  PIT coverage readiness、gate/event attribution、promotion gate 和默认执行成本
  numeric leaf。
- `config/feedback_sample_policy.yaml` 的 reporting / pilot / diagnostic /
  promotion sample floor。

## 边界

- 本任务只新增或校验 policy rationale / validation metadata，不调整现有阈值。
- `aits docs heuristic-audit` 可以扩展校验规则，但仍必须是只读治理审计，
  `production_effect=none`。
- 如果发现某个阈值缺少依据，本任务应先产出 audit failure 或任务登记项，
  不得在没有 evidence 的情况下临时放宽或静默白名单。
- 若需要改变任何投资解释阈值，应另建校准或 policy change 任务，并保留回测、
  forward shadow 或 owner approval 证据。

## 验收标准

1. 关键 policy config 提供 threshold/section-level rationale map，至少覆盖本文件
   范围列出的 numeric leaf。
2. `aits docs heuristic-audit` 或同等只读校验能检查关键 rationale map 的存在性、
   字段完整性和 stale path / stale key。
3. 审计报告披露 policy/config version、map coverage、missing rationale、missing
   validation 和 `production_effect=none`。
4. 目标测试覆盖缺 rationale map、缺 validation、stale key、默认仓库 PASS 和 CLI /
   direct dispatcher 行为。
5. 更新 `docs/artifact_catalog.md`、`docs/system_flow.md` 或相关报告文档；若实现只
   改治理配置和审计报告语义，也要明确不改变评分、回测、日报和 production 输出。

## 状态记录

- 2026-06-09：从 `GOV-004` 后续增强拆分为独立 READY 任务，原因：逐阈值
  rationale map 会增加审计深度，但不阻断当前 heuristic audit 工具和已配置阈值
  迁移范围归档。
- 2026-06-09：从 READY 进入 IN_PROGRESS，原因：开始实现只读
  threshold/section-level rationale map 校验；本轮不得改变任何评分、仓位、回测、
  promotion、日报或 approved overlay 阈值。
- 2026-06-09：从 IN_PROGRESS 改为 DONE，原因：`config/scoring_rules.yaml`、
  `config/backtest_validation_policy.yaml` 和 `config/feedback_sample_policy.yaml`
  已新增 `threshold_rationale_map`，`aits docs heuristic-audit` 已校验 missing
  rationale、missing validation、stale target path 和 map coverage；真实审计
  `heuristic-audit --fail-on-warning` 为 PASS，map coverage=100.0%，
  `production_effect=none`，未改变任何 policy 数值。验证通过 focused pytest
  53 passed、文档新鲜度、documentation contract、Ruff、repo-wide Black check、
  `compileall` 和 `git diff --check`。
