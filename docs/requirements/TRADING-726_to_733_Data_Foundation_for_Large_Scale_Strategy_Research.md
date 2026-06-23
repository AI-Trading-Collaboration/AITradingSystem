# TRADING-726 to TRADING-733 Data Foundation for Large-Scale Strategy Research
最后更新：2026-06-23

## 状态

- 状态：VALIDATING
- 日期：2026-06-21
- 范围：validation-only / observe-only data foundation baseline
- Owner：系统实现 + 项目 owner 后续复核

## 背景

附件 `AITradingSystem_TRADING_726_733_data_foundation_plan.md` 要求在 TRADING-702～725
研究治理、研究加速、组合决策与研究运维基础上，补齐大范围、多资产、多窗口、多模型族策略研究所需的数据底座：

- PIT feature store and snapshot registry。
- Tradable universe and asset master。
- Cost, liquidity, and tradability model。
- Regime, event, and cluster label store。
- Experiment result warehouse and run registry。
- Parallel backfill cache and checkpoint engine。
- Forward evidence capture and daily decision archive。
- Research case library for forward and reverse diagnostics。

本阶段只实现可审计 baseline，不把任何数据底座检查解释为 promotion evidence。

## 总安全边界

- validation-only / observe-only。
- `production_effect=none`。
- `broker_action=none`。
- 不修改 production、paper-shadow、official weights。
- 不触发 broker/order/live trading。
- 不放宽 PIT、data-quality、feature availability、lineage 或 no-lookahead gate。
- 不用 current-view / oracle / synthetic / bridge-only artifact 作为 promotion evidence。
- 缺真实 data source、PIT source manifest 或 forward evidence 时输出 gap / warning，不补造数据。

## 阶段拆解

### Phase 1：数据证明底座

- TRADING-726：PIT Feature Store and Snapshot Registry。
- TRADING-727：Tradable Universe and Asset Master。
- TRADING-728：Cost, Liquidity, and Tradability Model。

### Phase 2：研究语义和实验管理

- TRADING-729：Regime, Event, and Cluster Label Store。
- TRADING-730：Experiment Result Warehouse and Run Registry。

### Phase 3：规模化执行

- TRADING-731：Parallel Backfill Cache and Checkpoint Engine。

### Phase 4：前向证据和用例库

- TRADING-732：Forward Evidence Capture and Daily Decision Archive。
- TRADING-733：Research Case Library for Forward and Reverse Diagnostics。

## 验收标准

- PIT snapshot 可构建、审计和查询；manifest 包含 snapshot hash、source manifests、available_time 和 no-lookahead status。
- Asset master 可校验；tradability calendar 和 universe as-of audit 可生成。
- Cost/liquidity model 可校验；cost estimate/audit 披露 net return fields、cost model version 和 liquidity violation count。
- Regime/event/cluster labels 可构建和审计，标签版本和 as-of 有效性可见。
- Run registry 可登记、查询、比较和审计；run_id 唯一，reproducibility fields 完整。
- Research execution 可计划、batch dry-run、resume、cache audit 和 prune，输出 checkpoint/cache 状态。
- Forward evidence daily archive 可生成，outcome 更新为 append-only 语义，不触发 broker。
- Case library 可 register/query/audit，可从 regret casebook / oracle diagnostic set 构造，oracle case 不能进入 promotion gate。
- CLI、schema/config、report registry、artifact catalog、system flow、task register 和 focused tests 同步。
- focused pytest、Ruff、Black/format check、compileall、`git diff --check` 和适用 validation tier 通过或明确记录阻塞。

## 开放问题

- 第一版使用结构化 baseline config 和空/样例 records 验证 schema、lineage、as-of 与安全边界；真实多源数据接入仍需后续数据源 owner 决策。
- PIT feature snapshot 中 SEC/EDGAR、analyst estimates、macro/rates 等高可信 source 的完整 as-reported coverage 仍受供应商和 source manifest 可得性限制。
- Forward evidence 需要未来每日运行持续积累，当前只能验证 archive contract 和 append-only outcome mechanics。

## 进度记录

- 2026-06-21：按 owner 附件新增总计划并进入 IN_PROGRESS；实现前固定 validation-only、no production/paper-shadow/official weight mutation、no broker/order、no PIT/data-quality gate relaxation、no oracle/bridge-only promotion evidence 的边界。
- 2026-06-21：完成 validation-only baseline 并进入 VALIDATING；新增 data foundation config/schema、`src/ai_trading_system/data_foundation.py`、`aits data pit-feature-store|asset-master|universe`、`aits trading-costs`、`aits research labels|runs|execution|cases`、`aits forward-evidence`、report registry、artifact catalog、system flow 和 focused tests；已通过 compileall、Black check、Ruff、focused pytest、fast-unit、contract-validation、report-validation 和 `git diff --check` 前置验证。
