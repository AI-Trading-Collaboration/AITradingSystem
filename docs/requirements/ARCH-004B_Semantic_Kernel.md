# ARCH-004B Semantic Kernel

最后更新：2026-07-11

## 任务信息

- task id：`ARCH-004B_SEMANTIC_KERNEL`
- parent：`ARCH-004`
- priority：`P0`
- status：`DONE`
- owner：architecture coordinator
- dependency：ARCH-004A complete；full parallel `5358 passed / 0 failed`
- production effect：`none`

## 目标

把 ARCH-004A 冻结的研究语义词典实现为唯一 typed semantic kernel，使新的 investment-facing artifact 不再用松散字典或单一 `default_backtest_start` 表达多种日期含义。

Phase B 必须建立：

1. `ResearchEvaluationContext`；
2. canonical context/status vocabulary；
3. `EvidenceRole`、`PolicyRef` 和 data-quality contract ref；
4. 完整、blocked 两种显式 context 状态；
5. deterministic serialization/context id；
6. 从现有 market-regime config、research-window registry 和 legacy flat payload 到新 contract 的兼容 adapter；
7. 2021/2022、requested/actual/effective/evaluation/as-of 冲突的 fail-closed validation；
8. 一个现有 read-only/research-only consumer 的 additive context 接入与旧字段 parity。

## 现状证据

- `BacktestRegimeContext` 只包含 regime id/name/start/anchor，不包含 research window、effective coverage 或 policy provenance；
- `config/market_regimes.yaml` 的 `ai_after_chatgpt.start_date=2022-12-01` 是项目 regime start；
- `config/research/research_window_registry.yaml` 的 `exact_three_asset_validated.start=2021-02-22` 是 scoped QQQ/SGOV/TQQQ primary validated window；
- 当前 report modules 大量各自拼装 `market_regime`、`requested_date_range`、`as_of`、`data_quality_status`；
- data-quality failure 时 actual/effective range 可能不可得，因此“完整 context”和“blocked context”必须在类型上可区分，不能用虚构日期补齐。

## 包边界

```text
contracts/
  status.py
  research_context.py

legacy/
  research_context_adapter.py
```

规则：

- `contracts` 只依赖 Python 标准库和 `core` 基础类型，不读取文件、不导入 CLI、renderer、domain module 或 `config.py`；
- `legacy` adapter 可以读取现有 typed config/registry，并把来源、hash 和 legacy 缺口显式转换成 `PolicyRef`/caveat；
- domain/report consumer 只能消费 context 或 adapter 结果，不自行重新解析 regime/window；
- Phase B 不引入 `ArtifactEnvelope`、workflow ledger 或 report plugin，它们属于 ARCH-004C/F。

## Contract

### 完整 context

至少包含：

- schema/version 和 deterministic `context_id`；
- `market_regime_id`、`regime_anchor`、`regime_start`；
- `research_window_id`、`research_window_start`、`window_role`、`evidence_role`；
- `requested_start/end`；
- `actual_data_start/end`；
- `effective_feature_start`；
- `effective_prediction_start`；
- `actual_portfolio_start`；
- `evaluation_start/end`；
- `as_of` 和 `trading_calendar`；
- per-input `effective_coverage`；
- data-quality contract id/status/passed/as-of/report ref；
- caveats 和 typed policy refs。

### Blocked context

当 data quality、source coverage 或 required metadata 不足时：

- 仍必须包含 regime/window/request/as-of 和 provenance；
- 不可得的 actual/effective/evaluation 字段保持 `null`；
- 必须给出稳定 blocker code；
- `assert_complete()` 必须失败；
- 不允许用 requested range 复制成 actual/effective range。

## 核心不变量

1. `regime_anchor <= regime_start`；
2. research-window start 来自匹配的 governed window id；
3. `actual_data_range` 必须包含在 requested range 内；
4. effective feature/prediction/portfolio start 必须位于 actual data range 内；
5. evaluation start 不得早于 feature、prediction 或 portfolio effective start；
6. evaluation end 不得晚于 actual data end 或 `as_of`；
7. primary/legacy/sensitivity/proxy/metadata window role 必须匹配对应 evidence role；
8. sensitivity/proxy/metadata context 必须携带 caveat；
9. 完整 context 至少有 market-regime、research-window 和 data-quality policy refs；
10. context id 只由 canonical semantic content 决定，不受 dict 顺序或 generated-at 影响；
11. unknown window、unknown role、未知 legacy status mapping 和已知字段冲突全部 fail closed。

注意：`2021-02-22` research window start 早于 `2022-12-01` market-regime start 本身是合法的。真正需要阻断的是把 `exact_three_asset_validated` 的 window start 错写为 `2022-12-01`，或把 regime start 当作 research-window start。

## 分阶段实施

### B1：纯 contract

- canonical status/evidence/window/policy enums；
- immutable range、coverage、policy/data-quality refs；
- complete/blocked context；
- deterministic serialization、round-trip 和 context id；
- pure invariant tests。

### B2：governed resolver

- typed regime/window specs；
- exact role/evidence compatibility；
- requested/actual/effective/evaluation/as-of validation；
- stable error codes；
- 2021/2022 conflict tests。

### B3：legacy adapter

- 读取现有 `config/market_regimes.yaml`、research-window registry/primary policy 和 data-quality policy；
- 输出带 path/hash/version/status 的 `PolicyRef`；
- 兼容 flat payload，但不从 requested range 猜 actual/effective range；
- unknown/mismatched legacy fields fail closed。

### B4：reference consumer

- 选择已有 read-only/research-only、`production_effect=none` consumer；
- additive 写入 `research_evaluation_context`；
- 保留旧 flat fields 并做 golden parity；
- blocked path 输出 blocked context，不伪造 coverage；
- 更新 `docs/system_flow.md`。

### B5：closeout

- architecture/import-boundary tests；
- focused/contract/full parallel validation；
- compatibility snapshot 更新；
- 明确 legacy sunset 和 ARCH-004C handoff。

## 验收标准

- pure contract 无 config/CLI/report/domain import；
- valid 2021 primary research context + 2022 AI regime context 可共存并 round-trip；
- exact-three-asset window 被声明为 2022 start 时明确抛出 stable conflict code；
- evaluation 早于 effective feature/prediction/portfolio coverage 时 fail closed；
- DQ/source blocked context 不伪造 actual/effective range且不能作为 complete conclusion context；
- unknown legacy mapping fail closed；
- reference consumer 的旧 CLI、exit、path、status、安全字段和计算结果 parity；
- 新增 context 字段包含 policy refs/checksums 和 deterministic id；
- Ruff、Black、mypy scoped、focused xdist、documentation/task consistency、contract-validation 和 full parallel pytest PASS；
- `production_effect=none`，不改变 strategy、threshold、weight、research result、promotion、paper-shadow、production 或 broker。

## 明确不做

- 不把所有历史 artifact 一次迁完；
- 不改变现有策略默认 window；
- 不把 2021 window 强制用于不相关资产或历史报告；
- 不修改 scoring/backtest algorithm、阈值、权重或数据源；
- 不为 blocked context 创建 placeholder date；
- 不新增 report family、CLI command 或 task-shaped research module；
- 不实现 ARCH-004C 的 envelope/workflow/report contracts。

## 状态记录

- 2026-07-11：ARCH-004A entry gate 完成后登记并进入 `IN_PROGRESS`。现状盘点确认 regime、window、requested range、as-of 与 DQ 字段分散在多个 module，尚无统一 actual/effective/evaluation coverage contract；开始 B1/B2 contract-first 实现。
- 2026-07-11：B1～B4 实现完成。新增纯 contracts、canonical status/evidence/window/policy refs、complete/blocked context、deterministic id/round-trip、explicit legacy mapping、governed config/hash adapter 和 `first_layer_v2_effective_coverage_audit` additive reference consumer；2021/2022、actual/requested、effective/evaluation、DQ as-of、unknown role/status 和 flat-field parity 均 fail closed。验证通过 scoped Ruff/Black/compileall/mypy、focused/documentation 74、contract-validation 197 和 full parallel `5375 passed / 0 failed / 642 warnings`，runtime artifact=`outputs/validation_runtime/full_20260710T171001Z/test_runtime_summary.json`。任务归档 `DONE`，ARCH-004C entry gate 解锁；未改变策略、阈值、权重、promotion、paper-shadow、production 或 broker。
