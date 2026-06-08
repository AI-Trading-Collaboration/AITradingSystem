# TRADING-146 Historical Replay PIT Safety Hardening

最后更新：2026-06-09

## 状态

- 任务登记：`docs/task_register.md` 中 `TRADING-146`
- 当前状态：VALIDATING
- 下一责任方：系统验证
- 安全边界：no broker API、no automatic order、no production candidate、no owner auto approval、no official target weight mutation、no production config auto calibration

## 背景

复验 `TRADING-141_to_145` latest artifacts 时，`replay_inventory` 显示：

- `total_replay_events=2`
- `pit_warning_count=2`
- `pit_unsafe_count=0`
- top limitations 包含 `ADVISORY_GENERATED_AFTER_AS_OF_DATE` 和 `MISSING_PRICE_DATA`

随后 latest `historical_replay` 通过显式 `--include-pit-warning` 纳入了这 2 个 warning events。按 TRADING-141_to_145 的 PIT safety 规则，无法证明 source advisory 在 replay `as_of` 已存在、或缺少价格覆盖导致 outcome window 不可验证时，不能作为可重放 decision input 进入 historical replay。

本任务收紧分类和 validation，防止 hard PIT limitations 被降级为 `PIT_WARNING` 后通过 opt-in replay。

## 范围

1. 把 `ADVISORY_GENERATED_AFTER_AS_OF_DATE` 归为 `PIT_UNSAFE` / `INELIGIBLE`。
2. 把 `MISSING_PRICE_DATA` 归为 `PIT_UNSAFE` / `INELIGIBLE`，因为 backfilled outcome 至少需要可审计的后续价格覆盖；缺覆盖时不能把 event 放进 replay 后再输出全 `PENDING` / zero-like simulation 结果。
3. `validate-replay-inventory` 增加 hard limitation consistency checks：hard limitations 不得出现在 `PIT_WARNING`、`PIT_SAFE` 或 replay eligible rows。
4. `historical-replay run --include-pit-warning` 仍只能纳入 non-hard `PIT_WARNING`，不得纳入 `PIT_UNSAFE`。
5. Markdown reports 披露 hard limitation count、`include_pit_warning` 状态和 `PIT_UNSAFE` exclusion 语义。

## 非目标

- 不重写 historical replay outcome algorithm。
- 不自动重跑 latest production-like artifact。
- 不修改 `position_advisory_v1.yaml`、official target weights、paper portfolio state、real portfolio、baseline config、production state 或 broker state。
- 不把任何 replay / backfill / performance review 结果解释为 production approval。

## 验收标准

- focused tests 覆盖 after-as-of advisory 和 missing price data 会变成 `PIT_UNSAFE` / `INELIGIBLE`。
- focused tests 覆盖 `--include-pit-warning` 不能纳入 hard PIT limitations。
- `validate-replay-inventory` 对 hard limitation 分类不一致 fail closed。
- report 文案清晰说明 `PIT_WARNING` opt-in 不覆盖 `PIT_UNSAFE`。
- `tests/test_dynamic_v3_historical_replay.py`、相关 TRADING-141_to_145 validate smoke、ruff 和 compileall 通过。
- README、`docs/system_flow.md`、`docs/artifact_catalog.md` 和本需求文档同步更新。

## 进展记录

- 2026-06-09：新增并进入 `IN_PROGRESS`。本轮目标是 P0 审计语义硬化，不改变 production 或 broker 边界。
- 2026-06-09：已实现 hard PIT limitations 常量、`validate-replay-inventory`
  consistency check、report hard limitation disclosure、`replay_action_summary`
  skipped event audit fields，并补充 focused tests 覆盖生成逻辑和 corrupt
  artifact fail-closed validation。
- 2026-06-09：从 `IN_PROGRESS` 改为 `VALIDATING`。重建 latest artifacts：
  `replay_inventory=358d353576f86a47`、`historical_replay=86b56e67308cdc50`、
  `backfilled_outcome=38f3b24513fc8229`、`historical_paper_sim=278aaea770c9d106`、
  `replay_performance_review=f5a0d6edae45994f`。新 inventory 将 2 个历史
  events 全部归为 `PIT_UNSAFE`；下游 replay/backfill/sim/review 保持
  `INSUFFICIENT_DATA` 或 `continue_forward_tracking`，没有伪造 outcome 或
  calibration 结论。验证通过 focused pytest、五个 artifact validate、ruff、
  compileall、documentation contract 和 docs freshness。
