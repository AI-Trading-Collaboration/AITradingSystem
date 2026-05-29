# SEC PIT Shadow Daily-Run Baseline Contract

任务 ID：`TRADING-047`

最后更新：2026-05-29

## 背景

2026-05-29 修复 `RISK-016` 后重新执行 `aits ops daily-run`，`score_daily`
已通过，但后续 `sec_pit_shadow_monitor` 失败。失败不是新的行情、PIT、SEC metrics、
valuation 或日报评分问题，而是 `sec-pit shadow-observe --latest` 与
`sec-pit shadow-monitor --latest` 的默认 baseline 输入契约不一致：

- `shadow-monitor --latest` 默认读取 research-only baseline：
  `data/processed/research/scores_daily_backfill_sec_pit_2023_2026.csv`。
- `shadow-observe --latest` 未显式指定 baseline 时仍先尝试默认 baseline dir，
  然后 fallback 到 production `data/processed/scores_daily.csv`。
- production score cache 只覆盖少量当日/近期评分行，导致 observe summary 记录
  `baseline_coverage_ratio` 约 2.1%、`LIMITED_BASELINE_MISSING` 和
  `Baseline score fallback path was used.`。
- 同日 baseline coverage audit 使用 research-only baseline，覆盖率约 99.88% 且为 `OK`，
  因此 monitor 按研究 baseline 校验 observe 产物时 fail closed。

## 目标

1. `aits sec-pit shadow-observe --latest` 在默认 baseline dir 下优先使用
   research-only historical baseline CSV，与 `shadow-monitor --latest` 和
   TRADING-045 baseline coverage audit 保持一致。
2. 显式传入 `--baseline-score-path` 时仍完全尊重调用方指定路径。
3. production `data/processed/scores_daily.csv` 仅作为兼容 fallback，并且不得在
   research baseline 存在时被默认 daily-run shadow observe 使用。
4. 保持 observe-only 边界：不修改 production scoring、production weights、
   active shadow weights、decision snapshot、prediction ledger 或交易行为。
5. 修复后重跑完整 `aits ops daily-run`，确认不再因 baseline fallback drift 阻断。

## 非目标

- 不重算或覆盖 research baseline backfill。
- 不放宽 shadow monitor coverage gate。
- 不把 `LIMITED_BASELINE_MISSING` 改成 PASS。
- 不修改 SEC PIT candidate、observe weight、rollback threshold 或 promotion 规则。

## 验收标准

- 单元测试覆盖默认 baseline dir 下优先选择
  `scores_daily_backfill_sec_pit_2023_2026.csv`，而不是 production fallback。
- `shadow-observe` summary 中 `baseline_score` 指向 research-only baseline，
  coverage gate 使用该文件计算。
- `shadow-monitor --latest` 可消费最新 observe 产物；若仍失败，失败原因应为新的真实门禁。
- `docs/system_flow.md` 和 artifact 目录说明该默认契约。
- 完整 `aits ops daily-run` 重新执行并通过，或按真实新 blocker 报告。

## 进展记录

- 2026-05-29：新增并进入 `IN_PROGRESS`。原因：修复 `RISK-016` 后的完整
  daily-run 在 `sec_pit_shadow_monitor` 阶段 fail closed；根因是 shadow observe
  默认 baseline fallback 到 production score cache，与 monitor / coverage audit 的
  research-only baseline 契约不一致。
- 2026-05-29：实现完成并进入 `VALIDATING`。验证通过
  `tests/trading_engine/test_sec_pit_shadow_observe.py`、
  `tests/trading_engine/test_sec_pit_shadow_monitor.py`、相关 ruff/Black/diff check，
  以及真实 `aits ops daily-run` as-of 2026-05-28；`sec_pit_shadow_observe` 和
  `sec_pit_shadow_monitor` 均 PASS。
