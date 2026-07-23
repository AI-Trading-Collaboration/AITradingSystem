# DATA-GOV-001 统一 Data Foundation 治理与长期数据平台加固

最后更新：2026-07-23

## 任务信息

- task id：`DATA-GOV-001_UNIFIED_DATA_FOUNDATION_GOVERNANCE`
- related task：`STORAGE-001`
- priority：`P0`；物理存储迁移子任务为 `P1`
- status：`IN_PROGRESS`
- current phase：`D0B1_S2_COMPLETE_D0B2_AFTER_WAVE14_S0`
- owner：project owner / data platform owner / architecture coordinator
- architecture parent：`ARCH-004`
- production effect：`none`（D0 仅建设 fail-closed 数据发布与验证能力；在单独迁移和验收前不切换生产消费者）

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
   - D0C 同步补齐 bound-directory descriptor close 的结构化 cleanup 可观测性；该 P2 缺口可能造成
     资源泄漏或目录 rename/delete 可用性问题，但不改变 D0A 已提交 current 的 bytes 或 commit state；
   - 任何清理不得删除仍被 run manifest、lineage 或审计 retention 引用的对象。

#### D0A / D0B 边界

D0A 只建立 immutable publication transaction。它严格验证 caller-supplied DQ report 的 schema、
candidate bytes/schema、窗口、policy/status/count binding，并把 report 冻结到 store；但 D0A 不重新执行
`aits validate-data`，也不独立证明 caller 提供的 policy id/version 已由 canonical registry 审核。因此
D0A 的 manifest/envelope/result 必须机器可读地声明：

- `dq_execution_provenance_verified=false`；
- `filesystem_security_profile=acl_isolated_writer.v1`、`trusted_writer_principal_required=true`；
- `same_principal_adversarial_mutation_resistance=NOT_GUARANTEED`、
  `same_principal_post_ack_mutation_protection=false`；
- `store_acl_verified=false`、`consumer_cutover_allowed=false`、`crash_durability_verified=false`。

因此任何 feature/score/backtest/report consumer 不得把 D0A PASS 当作 required data-quality gate 已执行，
也不得把逻辑原子性解释为已验证 store ACL、同 principal 对抗性 CAS 或断电持久性。D0A 在 rename 前立即
重验旧 current exact bytes/首代 absence 和 source fd/path/nlink；replace 后继续重验 identity、nlink、
size、version 与 digest。检测到的 race 不能按 profile 降级为 warning：旧 current 必须精确恢复、首代
current 必须精确删除；成功回滚返回 `commit_state=ROLLED_BACK`，不能证明回滚结果则返回
`commit_state=INDETERMINATE`，replace 前拒绝保持 `commit_state=NOT_REPLACED`。只有全部 post-replace
attestation 已通过后的 descriptor/unlock/stage cleanup 故障才能作为 committed warning 返回。

D0B 才负责把 reviewed DQ policy path/version/SHA、canonical validator identity 与运行结果绑定到
publication，并选择首批真实 consumer 走同一 fail-closed preflight。只有 D0B 的 provenance、negative
tests、system-flow 与 formal gates 全部通过后，才能逐 consumer 授权 cutover。这个拆分避免 D0A
伪造比实际更强的安全保证，同时保留事务发布与 DQ 执行职责的清晰边界。

D0B 按 `docs/requirements/ARCH-004G4_D0B_Shared_DQ_Preflight_and_Periodic_Consumer_Migration.md`
进一步拆分，避免把 execution provenance、DQ rule completeness 与 consumer cutover 混成一个无法
审计的开关：

- D0B1：新增独立 `data_quality_execution_receipt.v1` 与严格 verifier，绑定 reviewed policy、
  canonical validator、exact invocation、input/manifest/report bytes 和现有 evidence；即使 FAIL 也写
  immutable receipt，且 `consumer_cutover_allowed=false`；
- D0B2：补齐 market-calendar freshness、逐 ticker coverage/internal gap、finite checks，并把
  download/publish/composite-manifest 更新收敛为单一事务；当前现场文件checksum已经可在manifest中匹配，
  但该偶然一致不能证明下一次下载不会留下data/manifest中间态；
- D0B3：把 verified receipt 与 publication companion attestation、reviewed per-consumer profile 绑定，
  只逐 consumer 授权，不翻转全局 cutover。

D0C 负责运行耐久性与可恢复性：跨进程 writer/reader/lock matrix、power-loss/crash-point rehearsal、
file + parent-directory durable commit、retention/reference-safe GC、forward-only/manual/config backup 与
restore 演练。D0A 的逻辑原子性与线程级测试不能替代 D0C；在 D0C 通过前不得声称 power-loss durable
或 backup/restore 已闭环。

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

- 2026-07-23：D0B1 与 Wave12 S2 formal exit PASS。最终 architecture/contract/reproducibility/
  integration=`525/266/23/983 passed`，failure-fix Full=`6825 passed / 3 skipped / 643 warnings /
  1147.04s`；canonical receipt/verifier、same-byte capture、actual window、profile/as-of pointer 与 daily
  typed preflight 已闭合。DATA-GOV overall 保持 `IN_PROGRESS`；Wave13 GOV-006 N1 formal gate已PASS，
  closeout commit/push后先基于最终HEAD通过Wave14 S0 contract/readiness，随后D0B2才与bounded G3并行，修复真实
  download/publish/manifest事务并补calendar freshness、coverage/internal gap、finite gate；
  `consumer_cutover_allowed=false`、production/broker 均保持关闭。
- 2026-07-23：Wave13 N1 closeout 前重新核对本地真实数据，prices/rates/Marketstack当前SHA均可在
  `download_manifest.csv`中匹配；Wave12记录的checksum缺失实例已由后续合法refresh消失，但D0B2仍为
  P0：当前下载实现会先覆盖数据CSV、最后再更新manifest，且composite row-count/source binding、
  market-calendar、requested-window coverage、internal-gap与finite gate尚未闭合。不得把现场一致状态
  解释为事务边界已完成。
- 2026-07-23：D0B1/S2 pre-formal hardening 已关闭 same-byte capture、actual evaluated window、typed
  execution profile 以及 root-bound report/receipt/pointer/universe read-write 四类 P1；独立复核确认无
  剩余 P0/P1，combined focused=`246 passed / 1 skipped`。当前只进入 generated freshness 与 formal
  gates；历史checksum缺失实例已经消失，但结构性transaction/composite binding缺口仍保留给D0B2，
  `consumer_cutover_allowed=false`。
- 2026-07-23：D0B1 canonical runner/verifier 与 chronology 加固通过 `20 passed`，并与 G4A 联合进入
  `W12_S2_SHARED_INTEGRATION`。S2 采用 profile/as-of 隔离的 atomic discovery pointer；pointer 只负责
  发现 exact receipt，公开 verifier 才提供证明。D0B2必须证明下一次download/publish/manifest更新的
  原子性与composite binding，而不是依赖当前cache恰好匹配；本阶段不得提前授权consumer cutover。
- 2026-07-23：Wave12 S0 shared contract/readiness 与 formal contract-validation PASS，D0B1 已获
  coordinator manual assignment，可与 G4A 在不相交路径并行实现。自动 dispatch、consumer cutover、
  production 与 broker 权限继续关闭；D0B worker 只能编辑 data-side execution module/focused tests。
- 2026-07-23：Wave12 S0 开始，D0B 与 G4 进入共享 contract/readiness 冻结。D0B1 只新增可重算的
  execution receipt/verifier，不改写 D0A schema 或历史 false safety flags；D0B2/D0B3 分别负责规则完整性
  与逐 consumer 授权。该时点真实prices checksum缺失是不可变历史审计事实；当前refresh后SHA已匹配，
  但严格门禁仍必须在任一未来不匹配或composite binding失败时阻断，禁止降级或绕过。G4只能消费verifier返回的
  typed preflight，不能继续信任裸 `PASS + evidence_id`，`production_effect=none`。
- 2026-07-23：D0A 多轮审计加固的 implementation 已完成。bound-directory authority 已覆盖 POSIX
  parent `dir_fd` 相对 mutation 与 Windows root→parent 不共享 delete 的目录 handle；stage/current/link
  三类 junction race 均证明 store 外零文件。后续独立复核又精确复现并关闭三项 P1：post-attest→hash
  hardlink 窗口、rename 前 current 复绑定距离过远、committed descriptor close 被静默吞掉。当前实现对
  pre/post-replace fd/path/nlink/version/digest 做 fail-closed attestation，旧 current 精确恢复、首代精确删除，
  rollback 不可证明时返回 `INDETERMINATE`；安全 profile 与 DQ/cutover/crash 限制按本节机器字段冻结，
  envelope 对 8 个 governed limitation key 的 missing/duplicate/contradictory/malformed 均 fail closed。
  协调线程 component-focused=`48 passed / 1 skipped`，Ruff、Black、mypy strict PASS；原子提交与
  contract 两项独立只读复审均为 no P0/P1。随后共同 formal gate 收口为 focused=
  `183 passed / 1 skipped`、architecture=`482 passed`、contract=`266 passed`；Full append-only ledger
  保留 attempt 1=`6701 passed / 2 failed` 与 attempt 2=`6706 passed / 1 failed`，最终 attempt=
  `6710 passed / 0 failed / 3 skipped / 643 warnings`，runner=`1106.60s`。前两次 FAIL 作为不可覆盖证据
  保留，最终 PASS 后 D0A 标记 formal complete，DATA-GOV-001 overall 仍为 `IN_PROGRESS`，D0B 为下一
  阶段且尚未自动 dispatch。DQ 边界仍是“验证并冻结上游 evidence，不证明 canonical gate 执行
  provenance”；`dq_execution_provenance_verified=false`、`store_acl_verified=false`、
  `crash_durability_verified=false`、`consumer_cutover_allowed=false` 继续生效。D0B 负责 policy
  SHA/registry、canonical validator 与首批 consumer cutover；在其独立授权和验收前不得切换 consumer，
  `production_effect=none`。
- 2026-07-23：owner 授权按双线推进长期任务，DATA-GOV-001 转 `IN_PROGRESS`。首个原子切片冻结为 D0A：复用现有 `ArtifactEnvelope`、atomic writer 与 DQ contract，建立 staged immutable snapshot、validated manifest、atomic current pointer 及 fail-closed validator；不在本切片迁移所有消费者、不选择 Parquet/DuckDB、不改变 score/backtest/report/production。数据 worker 只持有 data-foundation implementation/tests/本需求，`docs/task_register.md`、`docs/system_flow.md`、operations runbook、registry/catalog 与最终验证由 coordinator 集成。
- 2026-07-12：根据 owner 对当前数据管理、数据与知识系统分离及长期治理的讨论登记为 `PROPOSED`。本次只冻结目标、阶段、边界与冲突处置，不修改 cache、gate、score、backtest、report、scheduler 或 production runtime。
