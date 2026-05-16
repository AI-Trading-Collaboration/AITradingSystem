# Shadow 与 production 边界复核

状态：VALIDATING

最后更新：2026-05-16

关联任务：`CALIBRATION-013`、`CALIBRATION-007`、`CALIBRATION-008`、`CALIBRATION-011`、`CALIBRATION-012`

## 背景

2026-05-15 的真实 `aits ops daily-run` 已完成正式 `score-daily`，但在
`parameter_governance` 阶段失败。直接原因是
`config/parameter_governance.yaml` 已登记 `source_level: validation_shadow`，
而参数治理解析器的合法 source level 枚举尚未包含该值。

这暴露了 shadow 参数链路和 production 日报链路之间的契约漂移风险。当前系统
已具备 parameter shadow、shadow weight profiles、shadow gate profiles 和 shadow
parameter search。它们都应保持 validation-only / `production_effect=none`，不得
通过默认路径、报告汇总或 daily-run 编排影响正式投资结论。

## Review 结论

| 环节 | 结论 | 处理 |
|---|---|---|
| 参数治理 source level | `validation_shadow` 是合理的 validation-only 参数来源等级，但代码枚举漏配 | 加入合法枚举，并用测试确认报告仍为 `production_effect=none` |
| Parameter shadow ledger 默认路径 | `run-parameter-shadow` 语义是 validation-only，但默认写入正式 `prediction_ledger.csv`，存在隔离边界不清 | 默认改为 `data/processed/prediction_ledger_flow_validation.csv`，显式传参仍可指定其他 ledger |
| Shadow weight / gate 观察 | 已默认写独立 observation ledger；prediction ledger 需要显式提供路径 | 保持现状 |
| Production score-daily | 只读取正式 weight profile、approved overlay、正式数据门禁和正式输入 | 不修改 |
| Approved overlay / rule card | Shadow 搜索和治理报告不写 approved overlay 或 rule card | 不修改 |

## 验收标准

- `aits feedback evaluate-parameter-governance` 接受 `validation_shadow`，并在
  current manifest 下不因未知枚举失败。
- `validation_shadow` 条目仍只能输出 keep / collect / forward-shadow / owner-required
  / blocked 类治理动作，不写 production 参数。
- `aits feedback run-parameter-shadow` 未显式传入 `--prediction-ledger-path` 时默认写
  `data/processed/prediction_ledger_flow_validation.csv`。
- 正式 `score-daily`、最终仓位、approved overlay、production weight profile、
  `scoring_rules.yaml`、`portfolio.yaml` 和 rule card 不因本修复改变。
- 目标测试、ruff、`git diff --check` 通过，并重新运行默认 `aits ops daily-run`。

## 状态记录

- 2026-05-16：新增任务并进入实现。复核发现两个边界问题：`validation_shadow`
  枚举漏配阻断 daily-run 后半段；parameter shadow 默认 ledger 指向正式
  `prediction_ledger.csv`，与 validation-only 语义不一致。
- 2026-05-16：从实现推进到验证。已补齐 `validation_shadow` 合法枚举，并将
  `run-parameter-shadow` 未显式传入 `--prediction-ledger-path` 时的默认输出改为
  `data/processed/prediction_ledger_flow_validation.csv`；正式 `score-daily`、approved
  overlay、生产权重、正式仓位 gate、rule card 和日报结论链路未改。
- 2026-05-16：验证通过 `ruff check src tests`、目标 pytest 45 passed、
  `git diff --check`，并完成真实 `aits ops daily-run`。默认 as_of 解析为
  2026-05-15，run id `daily_ops_run:2026-05-15:20260516T010851Z`，
  15/15 步 PASS；parameter_governance 现在为 `PASS_WITH_LIMITATIONS`，其中
  `validation_shadow` 条目仍为 `production_effect=none`。
