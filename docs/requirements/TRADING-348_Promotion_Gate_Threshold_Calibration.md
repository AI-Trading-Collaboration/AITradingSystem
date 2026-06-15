# TRADING-348 Promotion Gate Threshold Calibration

最后更新：2026-06-15

## 1. 背景

TRADING-346 formal research method contract 目前把 filtered candidate chain 的离散状态
组合为 `FORMAL_RESEARCH_READY`。这些 gate 包含 stress strength、drawdown mismatch
reduction、flip/rotation reduction、A/B review confidence 和 confirmation target count。
TRADING-348 的目标是把这些 promotion-facing thresholds 迁移到 reviewed policy/config
中，并生成 calibration report，便于后续 owner 审计。

## 2. 目标

1. 新增 `config/research/promotion_gate_thresholds.yaml`。
2. 定义 threshold bands：
   - stress strength；
   - drawdown mismatch reduction；
   - flip/rotation reduction；
   - A/B review confidence；
   - confirmation target count。
3. 每个 threshold 记录 owner、status、rationale、intended effect、validation evidence 和
   review condition。
4. 新增 report CLI，读取 latest 或指定 formal research method contract，并输出当前候选在
   policy 下的解释。
5. 新增 validate CLI，检查 policy consistency 和 report artifact safety。
6. 新增 Reader Brief section 与 Reader Brief 汇总字段。
7. 更新 report registry、artifact catalog、README、system flow、operations runbook、task
   register 和 focused tests。

## 3. 非目标

- 不改变 TRADING-346 formal research method contract 的现有决策逻辑。
- 不为了让当前 candidate pass 而调参。
- 不回测、下载数据、刷新 evidence、重跑 upstream chain 或生成 target weights。
- 不创建 official target weights、broker action、order ticket、production mutation 或
  automatic position control。

## 4. Artifact Contract

Config:

- `config/research/promotion_gate_thresholds.yaml`

Runtime root:

- `run/review/register/promotion-gate-threshold-calibration/<calibration_id>/`

Artifacts:

- `promotion_gate_threshold_calibration_manifest.json`
- `promotion_gate_threshold_calibration_report.json`
- `promotion_gate_threshold_calibration_report.md`
- `reader_brief_section.md`
- `promotion_gate_threshold_validation.json/md`

Reader Brief fields:

- `promotion_threshold_calibration_id`
- `promotion_threshold_policy_id`
- `promotion_threshold_policy_version`
- `promotion_threshold_status`
- `promotion_threshold_current_interpretation`
- `promotion_threshold_stress_required`
- `promotion_threshold_confirmation_minimum`
- `promotion_threshold_validation_status`
- `promotion_threshold_next_action`

## 5. Threshold Governance

The first version is a `pilot_baseline` discrete-status policy. It documents conservative gate bands
from the already-reviewed filtered candidate chain, but does not fit thresholds to realized trading
outcomes or automatically change contract decisions. Any later numeric/statistical calibration must
update this policy with evidence and owner review.

## 6. 验收标准

- `promotion-gate-threshold-calibration report` 生成 JSON、Markdown、Reader Brief section 和
  validation artifact。
- `promotion-gate-threshold-calibration validate` 返回 PASS。
- Config 覆盖 5 个 required threshold families，且每个 family 有 rationale / intended effect /
  review condition。
- Reader Brief 显示 calibration id、policy id/version、threshold status、current interpretation、
  stress required band、confirmation target minimum 和 validation status。
- README、operations runbook、system flow、artifact catalog、report registry、requirements 和
  task register 同步更新。
- focused pytest、contract-validation suite、ruff、compileall、git diff check、documentation
  contract、report index 和 Reader Brief quality 通过。

## 7. 进展记录

- 2026-06-15：新增任务并进入 IN_PROGRESS；本阶段只实现 governance-only threshold
  policy/report/validation，不改变 formal contract decision logic，不生成 official target
  weights、不接 broker、不修改 production state。
- 2026-06-15：实现 `promotion-gate-threshold-calibration report/validate`、policy
  config、owner-readable threshold calibration doc、artifact family、Reader Brief 摘要字段、
  report registry、artifact catalog、README、operations runbook、system flow 和 focused tests。
  已生成 calibration `promotion-gate-threshold-calibration_81379cf981004dad`，report/validate
  CLI 和 policy validate 均 PASS，状态进入 VALIDATING。
- 2026-06-15：验证完成并归档 DONE。Focused pytest 5 passed；contract-validation suite
  38 passed / 28.32s；Ruff、compileall、documentation contract、report index、Reader Brief、
  Reader Brief quality 和 git diff check 通过。Report index 保持 `PASS_WITH_WARNINGS`，仅披露
  既有 missing/stale artifact visibility，不改变 TRADING-348 governance-only safety boundary。
