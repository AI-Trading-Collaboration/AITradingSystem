# TRADING-399 Decision Snapshot Lifecycle Policy

最后更新：2026-06-17

## 背景

Owner review 明确指出：缺失的 `decision_snapshot_YYYY-MM-DD.json` 不得被补造。
本任务为 decision snapshot 增加独立 lifecycle policy artifact，使 Reader Brief、
report index 和后续 recovery governance 能区分快照已存在、尚未到期、缺失且阻断、
缺失但不阻断等状态。

## 目标

- 定义 decision snapshot 的创建时机、必需输入、as-of 语义、source report 依赖、
  validation rules 和缺失行为。
- 新增状态枚举：
  - `SNAPSHOT_AVAILABLE`
  - `SNAPSHOT_NOT_DUE`
  - `SNAPSHOT_MISSING_BLOCKING`
  - `SNAPSHOT_MISSING_NON_BLOCKING`
- 新增可运行的 report / validate CLI 和 Reader Brief section。
- 明确 policy report 只读检查既有 artifact，不运行 `score-daily`、不刷新数据、
  不补造 decision snapshot、不修改 production 或 broker/order 状态。

## 生命周期策略

1. 创建时机：decision snapshot 只能由完成数据质量门禁后的 canonical daily
   scoring/decision workflow 创建，当前路径为 `score-daily` 写入
   `data/processed/decision_snapshots/decision_snapshot_YYYY-MM-DD.json`。
2. 必需输入：`score-daily` 所需的已验证 cached market/macro/fundamental inputs、
   daily score/evidence inputs、position gates、manual review fields 和同一 as-of
   的 decision context。Lifecycle policy 不读取这些上游明细，只验证 snapshot
   artifact 是否存在、可解析且日期一致。
3. as-of 语义：`decision_snapshot_YYYY-MM-DD.json` 的日期必须代表该 snapshot
   对应的 signal/decision date。显式 `--as-of` 检查只针对同一日期的 canonical
   path，不允许使用更早 snapshot 冒充当日 snapshot。
4. source report 依赖：Reader Brief、evidence dashboard、score change attribution、
   calculation explainers 和 governance packs 可以消费 decision snapshot；它们不能在
   snapshot 缺失时生成替代 snapshot。
5. 缺失行为：
   - 目标日期为未来日期或明确非交易到期日时输出 `SNAPSHOT_NOT_DUE`。
   - 目标日期为 weekday 且不晚于本地 today、但 canonical snapshot 缺失时输出
     `SNAPSHOT_MISSING_BLOCKING`。
   - 目标日期不需要 strict same-day conclusion，且 report 明确以 latest available
     snapshot 作为受限阅读上下文时，可输出 `SNAPSHOT_MISSING_NON_BLOCKING`，但必须
     披露被检查的目标日期和实际 latest snapshot 日期。
   - 任何 missing status 都不得创建、复制、回填或改写 snapshot 文件。
6. 验证规则：验证器必须检查 report type、状态枚举、snapshot path existence 与
   status 一致性、as-of/date alignment、Reader Brief core fields 和 safety boundary。

## 实施计划

1. 新增 `src/ai_trading_system/reports/decision_snapshot_lifecycle_policy.py`。
2. 新增 `aits reports decision-snapshot-lifecycle-policy` 和
   `aits reports validate-decision-snapshot-lifecycle-policy`。
3. Reader Brief 从 report index 读取 latest policy artifact 并展示状态、checked
   snapshot path、latest available snapshot、blocking impact 和 next action。
4. 更新 report registry、artifact catalog、README、operations runbook 和
   `docs/system_flow.md`。
5. 添加 focused tests 覆盖 available、not due、missing blocking、latest-context
   non-blocking、CLI 和 Reader Brief summary。

## 验收标准

- `decision_snapshot_lifecycle_policy` JSON/Markdown 和 validation JSON/Markdown 可生成。
- 状态枚举覆盖四种要求，且缺失快照不会被创建。
- 显式 2026-06-17 缺失 snapshot 时输出 blocking 状态，并在报告/Reader Brief 中说明。
- Reader Brief latest 能展示 lifecycle section。
- Focused tests、ruff、compileall、documentation contract、report index、Reader Brief
  quality、data quality gate 和 git diff check 通过。

## 进度记录

|日期|状态|记录|
|---|---|---|
|2026-06-17|IN_PROGRESS|新增需求文档和任务登记；实现只读 lifecycle policy，不补造缺失 snapshot。|
|2026-06-17|DONE|实现完成并转为 DONE。真实 artifact `outputs/reports/decision_snapshot_lifecycle_policy_2026-06-17.json/md` 输出 `SNAPSHOT_MISSING_BLOCKING`、target=`2026-06-17`、context_mode=`strict_same_day`、snapshot_exists=false、latest_available_snapshot_date=`2026-06-15`、market_session=`TRADING_DAY`、blocking_impact=`blocks_same_day_reader_brief_and_decision_conclusion`，未创建 `data/processed/decision_snapshots/decision_snapshot_2026-06-17.json`。Validation `outputs/reports/decision_snapshot_lifecycle_policy_validation_2026-06-17.json/md` 输出 `PASS_WITH_WARNINGS`、checks=7、failed=0、warnings=1。Reader Brief latest 使用 2026-06-15 decision snapshot + 2026-06-17 report index 展示 lifecycle section；focused pytest 6 passed，ruff PASS，compileall PASS，documentation contract PASS，report index `PASS_WITH_WARNINGS` / reports=418 / unwaived=9，Reader Brief quality OK，report quality gate `PASS_WITH_WARNINGS` / blocking=0，data quality gate PASS。该 policy 不运行 `score-daily`、不刷新数据、不补造 snapshot、不批准 paper-shadow、extended shadow、official target、broker/order、live trading 或 production mutation。|
