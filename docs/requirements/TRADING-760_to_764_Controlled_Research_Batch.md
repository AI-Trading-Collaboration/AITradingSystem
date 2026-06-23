# TRADING-760 to TRADING-764: Controlled Research Batch
最后更新：2026-06-23

## 背景

TRADING-759 已完成第一轮 current-subscription source qualification batch，输出
`CONTROLLED_RESEARCH_READY`，但仍保留 FMP PIT / lineage / delisted gap、
Marketstack coverage / discrepancy `DATA_REQUIRED`，且所有策略研究只能处于
validation-only / controlled-research-only。

本批任务推进第一批真实受控研究运行和数据源 gap 复核。目标状态是
`CONTROLLED_RESEARCH_RUNNING`，不是 `PROMOTION_READY`。

## 市场 Regime

- regime：`ai_after_chatgpt`
- anchor event：ChatGPT public launch on 2022-11-30
- default backtest start：2022-12-01

所有 batch/report 必须披露实际 requested date range。pre-2022 数据仅可用于
warm-up、stress test 或 regime comparison，不得作为默认 AI-cycle 结论窗口。

## 安全边界

所有输出固定：

- `production_effect=none`
- `broker_action=none`
- `promotion_gate_allowed=false`
- `paper_shadow_change_allowed=false`
- `production_weight_change_allowed=false`
- `lookahead_violation_count=0`

禁止把 endpoint accessible、current-view-only、research-label-only、oracle/teacher、
diagnostic-only、benchmark/control、reverse diagnostics 或 regret casebook 解释为
promotion、paper-shadow、production review 或 broker/order evidence。

## 阶段拆解

| Task | 阶段 | 目标 | 状态 |
|---|---|---|---|
| TRADING-760 | Controlled benchmark + forward evidence batch | 运行 benchmark / controls，并建立 forward evidence dry-run archive | VALIDATING |
| TRADING-761 | Marketstack reconciliation coverage expansion | 扩大 Marketstack representative universe probe，解释 coverage=0.125 与 discrepancy | DATA_REQUIRED |
| TRADING-762 | FMP PIT owner review + delisted validation | 生成 FMP PIT / lineage owner review package 和 delisted validation report | WATCHLIST |
| TRADING-763 | Reverse diagnostics + regret casebook controlled pilot | 消费 TRADING-760 输出，生成 controlled pilot diagnostics 和 casebook | VALIDATING |
| TRADING-764 | Controlled research batch review | 汇总 760～763，输出 continue/watchlist/data-required/pause 等 review board 决策 | VALIDATING |

## Implementation Plan

1. 复用 TRADING-739～759 的 `current_subscription_qualification` validation-only
   runner、artifact writer 和 safety metadata。
2. 新增 TRADING-760 artifacts：
   `controlled_benchmark_batch_report.*`、`control_audit_report.*` 和
   `forward_evidence_dry_run_archive.json`。
3. 新增 TRADING-761 artifacts：
   `marketstack_coverage_expansion_report.*` 和
   `fmp_marketstack_discrepancy_report.json`。
4. 新增 TRADING-762 artifacts：
   `fmp_pit_owner_review_package.*`、`fmp_delisted_validation_report.json` 和
   `fmp_allowed_uses_update.json`。
5. 新增 TRADING-763 artifacts：
   `reverse_diagnostics_controlled_pilot.*` 和
   `regret_casebook_controlled_pilot.*`。
6. 新增 TRADING-764 artifacts：
   `controlled_research_batch_review.*`，只做 review board，不新增策略逻辑。
7. 接入 CLI：
   - `aits research controlled-pilot benchmark-batch`
   - `aits forward-evidence capture-dry-run`
   - `aits data source-qualification marketstack-coverage-expansion`
   - `aits data source-qualification fmp-pit-owner-review`
   - `aits research acceleration reverse-diagnostics-controlled-pilot`
   - `aits research acceleration regret-casebook-controlled-pilot`
   - `aits research ops controlled-batch-review`
8. 更新 `docs/system_flow.md`、`docs/artifact_catalog.md`、`config/report_registry.yaml`
   和 focused tests。

## Acceptance Criteria

- Benchmark/control batch 至少覆盖 configured benchmark zoo 和 required controls。
- `negative_control_promotion_count=0`、`future_leakage_trap_blocked=true`、
  `random_signal_not_promoted=true`。
- Forward dry-run archive 存在，且 `outcome_status=pending`、
  `outcome_append_only=true`、`broker_action=none`。
- Marketstack representative universe probe complete，coverage ratio 有解释，
  discrepancy reason enum 只使用受控枚举，且 `marketstack_primary_source_allowed=false`。
- FMP provider timestamp gap 明确，不得静默升级；owner review package 包含
  request/response/snapshot/config/code lineage hashes；promotion 仍阻断。
- Delisted validation 报告明确 diagnostic / tradable universe candidate support 状态。
- Reverse diagnostics 和 regret casebook 都继承
  `CONTROLLED_RESEARCH_READY` / controlled-research-only，oracle/teacher 不能进入
  promotion evidence。
- Controlled batch review 对所有模块给出
  `CONTINUE|WATCHLIST|DATA_REQUIRED|PAUSE|KILL|PIVOT|INFRA_REVIEW` 决策，并输出
  next batch recommendation。

## Progress Notes

- 2026-06-21：新增本需求文档并进入 `IN_PROGRESS`；本批为 validation-only /
  observe-only controlled research batch，不允许 promotion、paper-shadow、production
  或 broker/order side effect。
- 2026-06-21：baseline 实现完成并进入 `VALIDATING`。新增 7 个 CLI 入口和
  TRADING-760～764 指定 artifacts；真实缓存运行结果显示 benchmark/control 与
  forward dry-run archive 可生成，Marketstack 仍为 `DATA_REQUIRED`，FMP PIT /
  delisted review 仍为 `WATCHLIST`，reverse diagnostics / regret casebook 仅用于
  hypothesis generation，review board 结论保持 `production_effect=none`、
  `broker_action=none`、`promotion_gate_allowed=false`、
  `paper_shadow_change_allowed=false`、`production_weight_change_allowed=false`。
- 2026-06-21：验证通过：`python -m compileall src tests scripts`、
  `python -m pytest -n 16 --dist loadfile tests/test_current_subscription_source_qualification.py`、
  `python scripts/run_validation_tier.py fast-unit --write-runtime-artifact`、
  `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`、
  `python scripts/run_validation_tier.py report-validation --write-runtime-artifact`、
  scoped Ruff、scoped Black check 和 `git diff --check`。runtime artifacts：
  `outputs/validation_runtime/fast-unit_20260621T051510Z/test_runtime_summary.json`、
  `outputs/validation_runtime/contract-validation_20260621T051558Z/test_runtime_summary.json`、
  `outputs/validation_runtime/report-validation_20260621T051644Z/test_runtime_summary.json`。
