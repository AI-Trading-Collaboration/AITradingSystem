# DATA-GOV-001 统一 Data Foundation 治理与长期数据平台加固

最后更新：2026-07-12

## 任务信息

- task id：`DATA-GOV-001_UNIFIED_DATA_FOUNDATION_GOVERNANCE`
- related task：`STORAGE-001`
- priority：`P0`；物理存储迁移子任务为 `P1`
- status：`PROPOSED`
- owner：project owner / data platform owner / architecture coordinator
- architecture parent：`ARCH-004`
- production effect：`none`（本次仅登记需求，不改变运行时）

## Owner 决策与目标

后续将数据能力作为独立的逻辑系统长期治理，但当前不拆 repository、部署单元或服务。Data Foundation 负责生产和证明事实；Knowledge & Insight Core、报告和网页只能消费其机器可读 contract，不能反向覆盖数据、质量状态或 lineage。

目标数据链路为：

```text
provider / request cache
  -> immutable raw snapshot
  -> normalized versioned dataset
  -> PIT / bitemporal feature state
  -> run-bound manifest + instance lineage
  -> current CSV / report / knowledge / visualization views
```

这是一份 `ARCH-004` addendum，不建立第二套 architecture、status、contract 或 control plane。既有 `ArtifactEnvelope`、`DataQualityEvidence`、atomic writer、Workflow/RunLedger 和 architecture fitness 应被复用并推进为 canonical runtime contract。

## 当前证据

2026-07-12 的只读审计确认：

- `config/data_sources.yaml` 已有较成熟的 source role、endpoint、audit field、validation 和 allowed-use 声明；
- 核心 `score-daily` 和正式 backtest 已有 fail-closed 数据质量门禁，PIT/source qualification 也有 validation-only baseline；
- canonical CSV 与 download manifest 仍缺少统一事务边界，历史 manifest 多次指向同一可变路径；
- manifest 缺失、不可读、字段不全或当前 checksum 未覆盖时，部分路径仍可成为 `PASS_WITH_WARNINGS`；
- 部分 research/shadow/ETF 路径仍存在 gate enforcement 不一致或可注入弱 DQ summary 的情况；
- canonical freshness 仍未统一采用 market calendar，也未全面检查研究区间逐 ticker 内部缺口；
- request cache 尚缺 status-aware negative TTL、retention/GC、entitlement scope 与完整 header sanitation；
- 本地 `data/cache` 约 19 GiB、10 万级文件，其中 weight-calibration cache 占主要部分，已推翻 `STORAGE-001` 在 2026-05-06 所依据的“尚无实际规模瓶颈”前提；
- forward-only、manual input 和 ignored local data 的备份、恢复与校验演练尚未形成已完成闭环。

这些证据说明当前是“治理控制面较强、统一数据运行底座仍不完整”，不能把已有 validation-only Data Foundation baseline 等同于长期数据平台完成。

## 范围与阶段

### D0：P0 正确性与可恢复性

1. **不可变 snapshot 与事务发布**
   - staged write、完整 validation、immutable run manifest、atomic current pointer；
   - 并发写入使用 lock 或 CAS；
   - source event manifest 与 published artifact manifest 分离，并由 `run_id` 关联；
   - current artifact 没有有效 checksum/provenance 时 fail closed，不再以 warning 继续正式数据依赖流程。

2. **统一 DataQualityEvidence preflight**
   - 所有 feature、score、backtest、research replay、daily report 消费同一 canonical evidence contract；
   - evidence 至少绑定 report path/checksum、as-of、policy/config version、输入 snapshot/checksum、schema 和适用窗口；
   - DQ FAIL 必须在任何特征、回测结果或投资解释生成前停止；生产 CLI 不允许通用 skip gate；
   - market-calendar freshness、逐 ticker 研究区间 coverage、内部缺口、规范化 duplicate 和 finite-value 检查进入 canonical gate；
   - validator 自身异常必须写出结构化 FAIL evidence，不能只留下未审计 exception。

3. **生命周期、备份和恢复**
   - 为 raw、normalized、request cache、research cache、run artifact 定义 owner、TTL/retention、quota、archive 和 deletion proof；
   - 实际执行 cache prune/GC，并生成删除清单与保留原因；
   - forward-only PIT、manual inputs、manifests 和关键配置建立 checksum backup 与 restore rehearsal；
   - 任何清理不得删除仍被 run manifest、lineage 或审计 retention 引用的对象。

### D1：P1 统一运行时与可重放 lineage

1. 由统一 registry 驱动 source adapter、asset master、qualification、allowed-use 和 runtime health，收敛重复 source id 与旁路 connector；
2. request cache 增加按 HTTP status/API family 的缓存政策：401/402 不持久缓存，429/5xx 使用短 negative TTL，成功响应按 immutable/revision 语义管理；
3. 建立 instance-level lineage DAG，使每次 run 能绑定精确 snapshot、config、code、policy、DQ evidence 和下游 artifact checksum；
4. 将当前 validation-only PIT snapshot skeleton 推进为真实 bitemporal store，至少表达 `event_time`、`release_time`、`available_time`、`ingested_at`、revision 与有效期；
5. cache catalog 对当前文件重新校验，不得用历史 PASS summary 代表当前 cache health。

### D2：P1/P2 物理存储演进

`STORAGE-001` 在本任务下重新打开。先用真实 workload 基准决定哪些数据族迁移到 Parquet/DuckDB 或等价 typed/columnar store：

- 优先处理 weight-calibration、candidate backtest、regime robustness 和高重复内部表；
- 保留 CSV 作为透明 export、人工审计和兼容读取层，不再默认作为所有 canonical mutable state；
- 迁移必须证明 schema、checksum、snapshot id、lineage、回滚和结果 parity；
- 物理格式选择不能替代 D0 的不可变性、事务性、retention 和备份要求。

## 与既有任务的关系

- `TRADING-726～728`、`TRADING-734～759`：继续作为 PIT、asset master、source qualification 和 real-data acceptance 的 validation-only baseline；本任务不复制其功能，而是补 canonical adoption、事务发布和长期运行治理。
- `TRADING-729/730/732/733`：其中 labels、experiment/run、forward evidence 和 case library 的研究语义归 Knowledge & Insight Core；其底层 snapshot/storage/lineage 仍由 Data Foundation 提供。
- `ARCH-004C`：已有 contract 与 atomic writer 被复用，不新建平行 contract。
- `ARCH-004F2/F3`：分别消费 Data Foundation 的事实与 evidence，不在 research/reporting 层重写 DQ 或 lineage 真值。
- `STORAGE-001`：原 `DEFERRED/P3` 结论被最新容量证据部分取代，改为 `PROPOSED/P1`；是否采用 DuckDB/Parquet 仍由基准与迁移证明决定。

## 不在本任务中的事项

- 当前不拆 Git repository、独立服务、远程数据库或云部署；
- 不因架构整理改变 score、threshold、position、promotion、paper-shadow、production 或 broker 行为；
- 不用网页、Markdown、LLM 或人工字段覆盖 DataQualityEvidence；
- 不把 current-view、reconstructed PIT 或 diagnostic-only source 自动升级为 promotion-grade；
- 不把 CSV 替换本身当作数据平台完成。

## 验收标准

- canonical 数据发布可证明 staging、validation、immutable manifest 和 atomic pointer 的单一事务边界；
- 任意正式数据依赖 workflow 都能绑定并验证同一 `DataQualityEvidence`，FAIL 时零下游结果；
- historical run 可通过 snapshot/config/code/policy/checksum 重放，旧 manifest 不再只指向已覆盖的 mutable bytes；
- lifecycle policy 在真实 cache 上执行，容量、对象数、last access、retention、prune 和 deletion proof 可审计；
- backup/restore rehearsal 能恢复关键 forward-only 与 manual data，并通过 checksum；
- registry、source adapter、asset master、qualification 和 runtime health 使用一致 source identity；
- `STORAGE-001` 的格式决策有 workload 基准、兼容期、parity 与 rollback evidence；
- 实现阶段同步更新 `docs/system_flow.md`、operations runbook、registry/catalog、相关 schema 和测试；
- 全程保持 AI regime、DQ/PIT、投资解释和 production boundary 不被静默改变。

## 状态记录

- 2026-07-12：根据 owner 对当前数据管理、数据与知识系统分离及长期治理的讨论登记为 `PROPOSED`。本次只冻结目标、阶段、边界与冲突处置，不修改 cache、gate、score、backtest、report、scheduler 或 production runtime。
