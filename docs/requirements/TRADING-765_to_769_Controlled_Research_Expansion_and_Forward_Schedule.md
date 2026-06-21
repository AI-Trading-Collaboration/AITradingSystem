# TRADING-765 to TRADING-769: Controlled Research Expansion and Forward Schedule

## 背景

TRADING-760～764 已把第一批 controlled benchmark、forward dry-run、Marketstack /
FMP source gap review、reverse diagnostics 和 batch review 跑通到
`CONTROLLED_RESEARCH_RUNNING` baseline。当前仍不能进入 promotion、paper-shadow、
official target weight 或 broker/order 逻辑。

本批任务的目标是把 benchmark/control 扩展成更真实的研究批次，关闭
Marketstack / FMP 对主研究的阻塞解释，开始每日 forward evidence dry-run 留存，
并在条件满足时才打开 reverse diagnostics activation gate。

## 市场 Regime

- regime：`ai_after_chatgpt`
- anchor event：ChatGPT public launch on 2022-11-30
- default backtest start：2022-12-01

所有输出必须披露实际 requested date range。pre-2022 数据只能用于 warm-up、
stress test 或 regime comparison，不得作为默认 AI-cycle 结论窗口。

## 安全边界

所有输出固定：

- `production_effect=none`
- `broker_action=none`
- `promotion_gate_allowed=false`
- `paper_shadow_change_allowed=false`
- `production_weight_change_allowed=false`
- `lookahead_violation_count=0`

本批任务只能证明研究机器能识别简单 benchmark 与伪策略，不能证明任何策略可
promotion、paper-shadow、production review 或 broker/order。

## 阶段拆解

| Task | 阶段 | 目标 | 状态 |
|---|---|---|---|
| TRADING-765 | Controlled Benchmark Execution Expansion | 输出更真实的 benchmark/control gross/net return、turnover、drawdown、cost-aware metrics，并按 asset/horizon/regime 分解 | VALIDATING |
| TRADING-766 | Marketstack DATA_REQUIRED Closure | 解释 row snapshot coverage 0.125 与 price/split/dividend discrepancy root cause；必要时降级为 limited second-source only | CLOSED_FOR_CONTROLLED_RESEARCH |
| TRADING-767 | FMP WATCHLIST Owner Review Closure | 明确 FMP 是否可继续作为 controlled research 主价格源，哪些 gap 只阻断 promotion，以及 delisted_companies 的 asset master / tradability 支持边界 | CLOSED_FOR_CONTROLLED_RESEARCH |
| TRADING-768 | Forward Evidence Daily Dry-Run Schedule | 把 daily dry-run archive 纳入统一 daily scheduler path 和 evidence ledger；不触发 broker，outcome 后续 append-only | VALIDATING |
| TRADING-769 | Reverse Diagnostics Activation Gate | 在 benchmark/control、FMP source status 和 baseline-vs-simple benchmark 差异满足后，才允许 teacher/oracle controlled activation | READY_FOR_CONTROLLED_ACTIVATION |

## Implementation Plan

1. 新增 controlled benchmark execution expansion runner：
   - 读取通过 `validate_data_cache` 的 cached price/rate data；
   - 运行 cash、buy-and-hold、static allocation、simple trend、risk-off、volatility、
     drawdown、masking benchmark definitions；
   - 输出 gross/net return、turnover、drawdown、cost-aware metrics；
   - 输出 by asset / by horizon / by regime breakdown；
   - controls 必须保持 fail-closed。
2. 新增 Marketstack closure report：
   - 将 TRADING-759 的 0.125 解释为只覆盖 SPY 的 probe-scope artifact；
   - 用 TRADING-761 expanded row-cache probe 区分 symbol mapping、endpoint 参数、
     plan limit、provider coverage 和探测逻辑原因；
   - 若 split/dividend source 仍缺少可审计 snapshot，结论降级为
     `LIMITED_SECOND_SOURCE_ONLY`，不阻塞主 controlled research。
3. 新增 FMP watchlist closure report：
   - 明确 FMP 可继续作为 controlled research 主价格源；
   - provider timestamp、conservative available-time、as-of/lineage owner review 和
     delisted validation gap 继续阻断 promotion；
   - delisted_companies 只能支持 diagnostic / asset-master candidate review，不能单独
     支持 tradable universe promotion。
4. 新增 forward evidence daily dry-run schedule command：
   - `aits forward-evidence capture-dry-run-daily --as-of {as_of}`；
   - 每日写 date-stamped archive；
   - idempotently append evidence ledger；
   - 通过 `config/scheduled_tasks.yaml` 接入 `aits ops daily-run` 统一 daily path。
5. 新增 reverse diagnostics activation gate：
   - 读取 benchmark expansion、FMP closure 和 review artifacts；
   - 条件满足才输出 controlled activation allowed；
   - teacher/oracle 仍只能 hypothesis generation。
6. 更新 `docs/system_flow.md`、`docs/artifact_catalog.md`、`config/report_registry.yaml`、
   `config/scheduled_tasks.yaml`、focused tests 和 CLI direct dispatcher。

## Acceptance Criteria

- Benchmark expansion 报告包含 `benchmark_run_count`、`control_run_count`、
  `negative_control_promotion_count=0`、`future_leakage_trap_blocked=true`、
  `random_signal_not_promoted=true`。
- 每个 benchmark 记录 gross/net return、turnover、drawdown 和 cost-aware metrics。
- 输出 by asset、by horizon、by regime breakdown。
- Marketstack closure 明确 0.125 coverage root cause，并给出
  `LIMITED_SECOND_SOURCE_ONLY` 或更严格结论；`marketstack_primary_source_allowed=false`。
- FMP closure 明确 controlled research primary price source allowed 与 promotion blockers；
  delisted_companies 不得被升级为 tradable universe promotion proof。
- Daily dry-run archive 每日记录 baseline / benchmark / candidate placeholder，
  不触发 broker，outcome 后续 append-only，并进入 evidence ledger。
- Reverse diagnostics activation gate 必须同时检查 benchmark/control batch、FMP controlled
  research source status 和至少一组 baseline vs simple benchmark 可解释差异；条件不满足
  时保持 `WAITING_FOR_PREREQUISITES`。

## Progress Notes

- 2026-06-21：新增本需求文档并进入 `IN_PROGRESS`；owner 要求推进
  TRADING-765～769。本批继续 validation-only / observe-only，不允许 promotion、
  paper-shadow、production 或 broker/order side effect。
- 2026-06-21：实现 controlled benchmark expansion、Marketstack DATA_REQUIRED
  closure、FMP WATCHLIST closure、forward evidence daily dry-run archive / ledger
  和 reverse diagnostics activation gate。真实 CLI run 结果：benchmark_run_count=9、
  control_run_count=5、negative_control_promotion_count=0、
  future_leakage_trap_blocked=true、random_signal_not_promoted=true、
  baseline_vs_simple_interpretable_difference_count=3；Marketstack 结论为
  `LIMITED_SECOND_SOURCE_ONLY`，不再阻塞主 controlled research，但
  price/split/dividend discrepancy 仍阻断 promotion；FMP 可作为 controlled
  research 主价格源，provider timestamp、conservative available-time、
  as-of/lineage owner review 和 delisted validation 仍只阻断 promotion；
  delisted_companies 只能支持 asset-master candidate review，不能单独支持
  tradable universe promotion。Forward daily dry-run 已生成 date-stamped archive
  并 append evidence ledger；activation gate 为
  `READY_FOR_CONTROLLED_ACTIVATION`，只允许 small controlled teacher/oracle batch，
  large-scale reverse diagnostics 仍关闭。
- 2026-06-21：验证已通过 focused parallel pytest、Ruff、Black check、
  compileall、`git diff --check`、fast-unit、contract-validation 和
  report-validation。Runtime artifacts：
  `outputs/validation_runtime/fast-unit_20260621T080029Z/test_runtime_summary.json`、
  `outputs/validation_runtime/contract-validation_20260621T080124Z/test_runtime_summary.json`、
  `outputs/validation_runtime/report-validation_20260621T080441Z/test_runtime_summary.json`。
