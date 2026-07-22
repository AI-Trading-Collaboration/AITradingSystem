# 双线研发 Operating Model 与冲突处理协议

最后更新：2026-07-23

状态：`ADOPTED_S4C_VALIDATED_MAIN_INTEGRATION`（基于 ARCH-005 S4/S4A；S5 未授权）

## 1. 决策与适用范围

项目后续研发默认采用以下拓扑：

1. `engineering`：工程、数据平台、架构、运行效率与基础设施；
2. `strategy-evidence`：策略研究、数据证据、预注册、回测验证与结论复核；
3. `integration-coordinator`：任务登记、共享合同、中央接线、generated views、正式验证、提交与集成。

“双线”表示默认同时寻找一项可执行的工程任务和一项可执行的策略证据任务，不表示必须让两条
lane 始终占满。没有满足依赖、owner 授权、数据质量、PIT、预注册或证据门禁的高价值任务时，
对应 lane 必须保持空闲或只做只读审计，不得用低价值加法、补造输入、复制逻辑或降低门禁制造吞吐。

本 operating model 复用 ARCH-005 已完成的 dependency/readiness/conflict/lease/replay、isolated
worktree 和 human-gated integration 能力。它不授权：

- ARCH-005 S5 canonical task source cutover；
- worker 自动 commit/merge/push、自动 PR、task status mutation，或 coordinator 在门禁未完全
  PASS 时集成；
- ARCH-004 G2.5 自动恢复；
- 自动调参、候选扩展、promotion、paper-shadow、production 或 broker/order 行为。

## 2. 为什么采用双线

工程与策略证据通常具有不同的主要瓶颈：前者多受代码合同、并发、缓存、验证资源和架构依赖约束，
后者多受数据成熟度、PIT、预注册、holdout、owner 决策和研究结论边界约束。把两者放在同一个串行
队列会让一个外部数据或 owner blocker 阻塞另一条可执行工程链；不加控制地并发，则会在任务表、
root CLI、report registry、system flow、架构清单和全量验证上重新形成中央冲突。

双线 operating model 的目标不是“同时改更多文件”，而是：

- 让两个独立 domain slice 同时推进；
- 把共享写入集中到一个较短的 integration wave；
- 在启动前识别语义、合同、资源和路径冲突；
- 只运行一次必要的中央 architecture/contract/full gate；
- 保留可重放的选择、阻塞、失败、返工和集成证据。

## 3. 每条 lane 的输入与输出

|角色|启动输入|允许输出|禁止事项|
|---|---|---|---|
|`engineering`|task/requirement、exact base、owned paths、module/contract/resource claims、focused tests、safety boundary|独占工程模块、lane-local policy/contract、focused tests、lane evidence|直接编辑 coordinator-only 文件；借工程优化改变 DQ、PIT 或投资语义|
|`strategy-evidence`|预注册或明确的 legacy-diagnostic contract、source commitments、research window、DQ/PIT/cost/holdout、owned paths、focused tests|独占研究模块、证据 validator、lane-local artifacts/tests、诚实的 BLOCKED/INCOMPLETE 结论|事后选择候选、补造证据、复用污染 holdout、无授权运行 clean search/promotion|
|`integration-coordinator`|两条 lane 的 manifest、diff、validation evidence、base freshness 与 unresolved conflicts|共享接线、任务登记、system flow、catalog/registry、generated manifests/views、formal validation、validated commit、fast-forward main与普通push|替 worker 改写业务结论；把集成 PASS 推断为策略 PASS；rebase/merge commit/force-push|

每条 worker manifest 至少声明：

- `task_id`、`change_id`、`lane_id`、`base_commit`；
- `owned_paths`、`shared_paths_requested`、`coordinator_only_paths_touched=false`；
- `module_ids`、`contract_versions`、`generated_outputs`、`removal_targets`；
- `validation_tiers`、CPU/内存/网络/provider quota 等 resource claims；
- `production_effect=none`、`broker_action=none` 和本切片安全边界；
- 启动条件、完成条件、blocked/abort 条件和下一责任方。

## 4. 路径与所有权分区

### 4.1 Worker-owned

worker 只能写入 manifest 明确独占的 domain module、domain config、domain tests 和不与另一 lane 共用的
requirement/supporting document。两个 active manifest 的 owned paths、module IDs 和 write contract 不得重叠。

### 4.2 Coordinator-only

下列中央文件和 generated outputs 默认只有 integration coordinator 可写：

- `docs/task_register.md`、`docs/task_register_completed.md`；
- `registry/development_tasks_shadow/**` 与 task shadow index/baseline；
- `docs/system_flow.md`、`docs/artifact_catalog.md`；
- `config/report_registry.yaml`；
- root CLI 或被两条 lane 共用的 CLI registration/wiring；
- shared schema、global policy 和跨 domain public contract；
- module/test manifests、compatibility baseline、deprecation/reporting inventories；
- aggregate/generated views、正式 validation artifacts 和最终提交边界。

同一文件中“改不同段落”仍视为共享写冲突，不把文本合并能力误当作语义可并行。

### 4.3 Shared read-only

两条 lane 可同时读取同一 policy、cache、历史 artifact 或 canonical contract，但必须固定 source path、
SHA-256、schema/version 和 `as_of`。如果任一 lane 会更新该 source，它就不再是 shared read-only，必须
转为 contract wave 或建立资源 lease。

## 5. 冲突分类与处理决策

|冲突类型|典型表现|处理方式|是否继续并行|
|---|---|---|---|
|Path conflict|两 lane 写同一文件/目录|把中央文件收回 coordinator；能拆成独占 leaf module 时先拆 module|拆分后可并行|
|Module/API conflict|同时修改同一 public API 或 adapter|先串行冻结 contract wave；两 lane 从新 base 实现各自 adapter/domain|contract 后可并行|
|Semantic/policy conflict|一条 lane 改阈值、窗口、DQ/PIT、cache identity，另一条消费它|停止消费方；先完成 owner-reviewed policy/contract 和兼容性裁决|裁决前不可并行|
|Generated-view conflict|两 lane 刷新 task shadows、manifests、catalog 或 registry|worker 不生成；coordinator 在集成末尾统一生成一次|业务实现可并行|
|Runtime resource conflict|full pytest、重回测、provider quota、同一 cache key、内存峰值竞争|声明 resource claim；轻量 focused 可并行，重资源 gate/联网操作按 lease 串行|受资源容量约束|
|Base drift|一条 lane 的 source/contract 已被另一提交改变|停止集成，重跑 manifest/readiness；不得把 stale diff 直接合入|刷新后再决定|
|Evidence lineage conflict|不同 run、window、holdout 或 policy 的 artifact 被混用|fail closed；恢复 exact lineage 或另立安全重跑任务|不可并行拼接|
|Validation conflict|两 lane 各自反复运行 architecture/full|lane 仅跑 focused/impact tests；coordinator 自然集成边界统一跑 formal gates|避免重复重资源验证|

冲突处理顺序固定为：

1. 判断是否为真实语义/合同冲突，而不仅是 Git 文本冲突；
2. 能通过抽出稳定 contract、leaf module 或 adapter 分离时，先完成最小 contract wave；
3. 将 shared docs、root wiring 和 generated outputs 收回 coordinator；
4. 更新 manifest、base 和 lease 后重新计算 readiness；
5. 无法安全拆分时显式串行，不以复制 helper、alias、fallback 或跳过 validator 换并行度。

## 6. 提升并行力度的方法

并行度应通过缩小共享写集合获得：

- 需求阶段先冻结输入、输出、schema、reason code 和安全边界，再分 worker；
- domain implementation、validator 和 focused tests放在 leaf module，root CLI 只保留薄接线；
- report family 使用 lane-local producer/validator，catalog/registry 在 integration wave 一次更新；
- task worker 不更新全局任务表，coordinator 按真实结果一次更新并统一生成 shadow views；
- 对只读 source 使用 exact path/hash/as-of commitment，避免“同名最新 artifact”漂移；
- 对 cache、provider quota、CPU、内存和 full validation 建立 resource claim，而不只检查文件重叠；
- 两条 lane 在独立 branch/worktree、同一 exact base 上开发；超出 manifest 立即停止并重新排程；
- 一条 lane 失败只释放/expire 自身 lease；无依赖 lane 继续，coordinator 不提前集成半成品；
- merge 顺序固定为 `contract -> adapter -> domain implementation -> tests/fragments -> shared wiring/docs -> generated aggregates -> compatibility removal`。

当前只采用两条 domain worker。只有至少两个真实批次证明 shared-path conflict、返工、lease expiry、
coordinator wait 和 full validation 资源均可控，才讨论增加第三条 domain lane；不能仅凭 CPU 空闲扩容。

## 7. 验证与性能规则

### Lane 内

- 默认运行 `python -m pytest -n 16 --dist loadfile` 的 focused/impact-selected tests；
- 运行 Ruff、Black/scoped type check 和 lane-local tamper/compatibility tests；
- 记录 wall time、slowest nodes、CPU/内存或 provider request count；
- 若耗时明显超出同类基线，先确认 fixture 重建、递归 validator、锁等待、provider 重试或资源竞争，
  不直接扩大 timeout 或改为串行 pytest 掩盖问题。

### 集成边界

- coordinator 复验 base/manifest/path/module/contract/source freshness；
- 合并两 lane 后运行 combined focused；
- 统一刷新 task shadow、module/test manifest、compatibility/deprecation/reporting inventory；
- architecture/contract 必须在最终 tracked state 通过；
- full 只在 formal trigger/natural integration boundary 运行一次，失败修复重跑必须遵守 provenance policy；
- 新增测试进入 slowest tail、wall/P95/P99、scheduler fallback、tail idle 或峰值内存异常时，暂停下一批扩容并登记性能任务。

### S4C 验证通过后的 main 自动集成门禁

Owner 已授权 coordinator 在每个 integration batch 的适用门禁全部 PASS 后自动收口到 main。顺序固定：

1. 冻结候选最终 tree，确认工作区归属清楚、无越界写、active shared-path lease=0；
2. 确认 lane focused、required architecture/contract/full、generated manifests/views、compatibility/
   deprecation/source hashes 对该 tree 新鲜；仅文档/generated closeout可复用最近代码 Full，且不得改变
   runtime/data/strategy/report语义；
3. fetch `origin/main`，要求它是候选祖先；否则停止并重新做 base/conflict/readiness，不自动 rebase；
4. coordinator commit后确认 tree bytes未变，切换main并执行`git merge --ff-only <candidate>`；
5. 普通push `origin main`，再确认`main`、`origin/main`、candidate SHA完全相同；
6. 以新main作为下一双线batch的exact base。

任何 dirty/unattributed worktree、stale validation/hash/base、活动lease、分叉、非fast-forward或push
rejection均停止自动集成并报告。禁止用merge commit、rebase、force-push、删除用户改动或降级验证继续。
该自动集成权限本身不延伸到worker、PR、task status自动变更、S5、G2.5、策略promotion或
production/broker行为；后续任务即使由独立 owner 指令解锁，也仍须先满足本节 final-tree 门禁，不能把
“任务已授权”解释为“允许绕过验证自动合入”。

## 8. 双线周期与复盘指标

每个 integration batch 保存：

- 两 lane 的 selected/not-selected reason、queue age 和 blocker；
- preflight path/module/contract/resource conflict 数；
- replan、abort、lease expiry/takeover、base drift 和越界写次数；
- lane focused time、coordinator wait、architecture/contract/full time；
- merge conflict、返工文件、失败隔离是否有效；
- 从 READY 到 integrated 的 cycle time，以及 full 相对最近有效基线的变化。

至少每两个真实批次复盘一次。优化优先级为：减少 shared-write wait、重复 fixture/validator 计算和重复
formal gate，其次才是增加 worker 数量。任何容量、TTL、retry、aging 或 fairness 阈值必须进入 reviewed policy。

## 9. 近期双线任务队列

### Wave 1：已完成

|Lane|任务|状态与目标|主要 owned scope|退出/停止条件|
|---|---|---|---|---|
|Engineering|`OPS-065_EXTERNAL_REQUEST_CACHE_REVALIDATION_SINGLEFLIGHT`|`DONE`；per-key lease、winner double-check、bounded waiter reuse、stale-owner takeover 与严格 publish fencing|external request cache coordination policy/module、multiprocess tests；共享 cache 接线由 coordinator|同 key 一次 live、不同 key 并行、TTL=0 串行、crash/timeout/invalidation/tamper/fencing PASS；cache key/body/DQ 语义不变|
|Strategy evidence|`TRADING-2449` canonical R0/R1/R2 artifact recovery audit|`DONE`；可信 worktree exact recovery，R0/R1/R2 validators 与真实 gate PASS|只读来源盘点、exact ignored evidence、gate run artifacts；不改候选/阈值|真实 gate=`BLOCKED_CONTAMINATED_LEGACY_SOURCE`；R2不变；未运行 backtest/evaluator/候选/新搜索|
|Coordinator|Wave 1 integration|`DONE`；共享写入、formal gates、提交与性能复盘|task register/system flow/generated registry/manifests/baselines|focused/architecture/contract/full PASS；共享文件单写；`production_effect=none`|

已知 legacy evidence identity 包括：

- R1 walk-forward：`r1-wf_6447beb5464bad37`；
- R1 robustness：`r1-robustness_8c93b0e2615d0ace`；
- R2 decision：`r2-decision_c761da11538fc58c`。

恢复任务不得接受同名但 checksum/lineage 不一致的 artifact，也不得从 synthetic fixture 生成“真实”证据。

Wave 1 telemetry：base=`8bf2b86c`，两条 lane owned-path 冲突=0、越界写=0、base drift=0、lane abort=0；
integration review 发现并在 formal gate 前修复 stale-owner cache-pointer fencing gap，architecture 首轮又按设计
阻断一个 stale compatibility hash，未降低门禁。工程 focused module/cache/provider=`13/38/118 passed`；
architecture/contract/full=`446/265/6470 passed`，full=`2 skipped / 642 warnings / 975.18s`，scheduler
applied=true、fallback=false、tail idle max=`15.57s`。相对最近 969.84s full 基线约 +0.55%，新增测试未
进入 slowest 50。策略线恢复约 167.7MB exact bytes，四级 validator 与 gate 均 `PASS/0`。该批证明
“domain 并行 + shared integration 单写”可工作，但只有一个真实批次，不增加第三条 domain lane。

### Wave 2：已完成

Base=`ca9dea5e`，scope freeze如下：

|Lane|任务/切片|Owned scope|共享/冲突处理|停止条件|
|---|---|---|---|---|
|Engineering|`ARCH-004G2.../W2E1`|仅`tests/test_paper_shadow_weekly_review.py`；test-only immutable fixture reuse|不得触碰portable lineage、shared docs/manifests；保留5 nodeids及真实tamper/live validators|isolated before/after不正确、restore不可靠或无有意义降幅即撤回|
|Strategy evidence|`TRADING-2450`|locator policy/module、sidecar builder、R0/R1/R2 opt-in adapters、focused tests|legacy bytes/IDs/checksums只读；不得触碰weekly test或shared docs；path/content冲突fail closed|clean-clone、missing legacy、conflict/tamper/traversal/exact replay任一未闭合即不集成|
|Coordinator|Wave 2 integration|task/system-flow/catalog、compatibility/deprecation/manifests/generated registry、formal/full gates|共享路径单写；若两lane需同一public helper，先暂停并建立最小contract wave|归属、base freshness、focused/formal/full与telemetry全部PASS|

1. Engineering 优先从现有
   `ARCH-004G2_VALIDATION_RUNTIME_BUDGET_AND_FIXTURE_REUSE` 读取本次自然 Full profile，选择一个
   bounded smoothed long-tail leaf slice；首选 immutable fixture/DAG reuse 或单测内部重复 validator
   消除，不改 nodeid、DQ/PIT/tamper/策略语义，也不以增加 worker 数掩盖单文件长尾。
2. Strategy-evidence platform 推进 `TRADING-2450_LEGACY_RESEARCH_ARTIFACT_PORTABLE_LINEAGE`：冻结
   content-addressed/project-relative locator + sidecar resolver contract，使历史 worktree 删除后仍可按
   exact content 验证，同时保持 legacy bytes、run IDs、R2 与 TRADING-2449 gate 不变。
3. 两项主要 owned scope 分别为 validation runtime/fixtures 与 portable lineage resolver/R0-R2 adapters；
   task register、system flow、compatibility baseline、generated manifests 和 formal gates继续由 coordinator
   单写。若实现前发现两者同时修改同一 validator cache/public contract，先做最小 contract wave再并行。
4. Research owner 提供结果不可见时冻结的新 preregistration 后，另建 `TRADING-2449 S1`；只有 gate=
   `ELIGIBLE_FOR_OWNER_AUTHORIZED_CLEAN_RUN` 且 owner 显式授权，才能运行 TRADING-106 fold-local evaluator。
5. TRADING-106 clean fold evidence 完成后，再推进 TRADING-107 neighbor/rank/regime/extreme-day 与
   multiple-testing/overfit closure；没有 eligible candidate 时必须保持 `INCOMPLETE`。
6. `event_risk_high=15<20`、20d/60d maturity=0 和 5 个 archive gap 继续作为观察/owner 治理支线，
   不占主动开发 WIP，不降样本 floor。

Wave 2 telemetry：两条 lane owned-path 冲突=0、越界写=0、base drift=0、workaround=0；integration
coordinator 仍是 shared docs/config/manifests/generated views 的唯一写者。Engineering 的首版实现把
build-time missing source 误成 build 后删除 live source，被真实 lineage validator fail closed 拦截；该
`4 pass / 1 fail` 运行作废，未放松门禁。修订版同机isolated=`340.62s -> 286.28s`（`-15.95%`），
5 nodeids 与 CLI/live/tamper/coverage/decision 语义不变。Strategy lane 固化4个artifact/108个source的
portable sidecar；R0/WF/robustness/R2均PASS，旧artifacts byte-identical，R2与TRADING-2449结论不变。
architecture/contract/reproducibility/full=`446/265/23/6487 passed`，Full=`2 skipped / 642 warnings /
1169.47s`，scheduler applied=true、fallback=false、profile/telemetry/performance/provenance均PASS。

本次 Full 相比 Wave 1 wall 增加约19.91%，但633个共同文件耗时中位数约`1.264x`且worker busy中位数
同步上升，判定为广泛机器负载而非W2E1局部回归；目标 weekly file worker-s仍下降约2.87%。因此只接受
局部收益，不声明稳定Full提速；v17 duration seed以本次完整`1084 files / 6489 nodes`刷新。

### Historical checkpoint：Wave 2 后的下一双线 Wave 选择

以下内容保留 Wave 2 收口时的真实选择依据，已由后续 Wave 3～11 的执行事实 supersede，不再是当前
dispatch queue。其中“G2.5 仍需新显式授权”是该 checkpoint 当时的正确状态；owner 已在 2026-07-23
通过独立 resume event 满足该条件，不得反向改写本段历史证据。

1. Engineering lane 从新 Full profile 选择一个 bounded leaf，优先评估
   `tests/test_evidence_staleness_monitor.py`（6 nodes，约378.64 worker-s，且在contract tier也形成长尾）；
   `tests/test_smoothed_forward_weekly_run.py`虽约575.17 worker-s，但单node链更深、语义风险更高，只有前者
   无安全复用点或收益不足时才升级为候选。每次只治理一个leaf并做同机before/after。
2. Strategy lane 只有在输入可用时推进：首选 research owner 提供不可见结果时冻结的新 preregistration 后
   启动`TRADING-2449 S1`；gate达到`ELIGIBLE_FOR_OWNER_AUTHORIZED_CLEAN_RUN`且owner显式授权后才进入
   `TRADING-106`，随后才是`TRADING-107`。当前这些条件未满足，因此策略lane可处于
   `BLOCKED_INPUT`，不得为维持“形式双线”伪造任务、复用污染selection或降低样本floor。
3. 若策略输入仍未就绪，第二条lane可选取与Engineering完全分离的strategy-evidence platform
   housekeeping/reproducibility slice，但必须先登记稳定task id与验收标准；不得把另一个中央架构变更
   塞入同一wave。在该 checkpoint 当时，`ARCH-004 G2.5`与`ARCH-005 S5`仍分别需要新显式授权。
4. 两条lane继续遵循 owned-path lease、shared-path单写、public contract冲突先拆contract wave、generated
   views最后统一刷新，以及“任一lane失败不污染另一lane产物”的集成顺序。只有连续多个真实wave证明
   overlap/base-drift/abort和Full性能稳定，才评估第三条domain lane。

### Wave 3：完成，含一次迟到策略输入与共享路径冲突收口

Base=`3156a4b9`。本轮继续采用双线控制面，但不把“双线”误解为两个lane必须同时产生代码。
Strategy lane在scope freeze时没有合规输入；2026-07-21 owner随后为既有`TRADING-098`提供保守迁移方案1，
因此它只在coordinator安全集成点作为迟到解锁的策略子切片纳入本wave，不扩大到backtest、search、
shadow enrollment或promotion：

|Lane|状态|任务/owned scope|冲突与停止条件|
|---|---|---|---|
|Engineering|`COMPLETE`|`ARCH-004G2.../W3E1`；仅`tests/test_evidence_staleness_monitor.py`，复用既有module-scoped真实source DAG|6 nodeids/真实latest discovery/fallback/live validators/DQ/PIT/decision/safety全部保持；isolated=`125.36s -> 85.44s`，通过`<=100.29s`门槛|
|Strategy evidence|`COMPLETE_OBSERVE_ONLY_MIGRATION`|`TRADING-098` tracked requirement + gitignored runtime registry|三条历史记录保留并显式降级为incomplete；validator=`PASS/0`，focused/expanded/governance=`1/2/19 passed`；不绑定partial evidence、不产生投资结论或生产效果|
|Coordinator|`COMPLETE`|任务/需求/operating-model、generated manifests、compatibility、formal/full与integration|首轮Full失败按失败保留；direct fix后architecture/contract/Full闭合；shared docs最终只有coordinator写入|

只读baseline=`6 passed / 125.36s`，其中latest discovery/fallback分别约`35.54/35.33s`；完整DAG共建3次，
目标是消除后两次重复构建。Strategy输入到达时，复用了既有稳定task id与既有验收，没有新增策略逻辑；
runtime registry按项目约定保持gitignored，仅提交可审计的需求、状态归档和验证证据。

Engineering isolated after=`6 passed / 85.44s`（`-39.92s/-31.84%`），latest/fallback分别降至
`15.78/15.52s`。首轮Full因v17 source-bound test残留v16冻结值而FAIL；direct fix后architecture=`446`
、contract=`265`、Full=`6487 passed / 2 skipped / 643 warnings`。Full runner wall=`1169.47s ->
1019.79s`，但1084个common files的median ratio=`0.8456`且worker busy median也整体下降，因此只接受
W3E1的isolated收益，`stable_full_improvement_claimed=false`。

本wave还暴露并验证了一次真实冲突流程：Strategy worker在coordinator发现前已写入`task_register`与本
operating model两个shared paths；发现后没有回退其业务成果，而是立即停止worker的shared writes、
stage/commit/push和formal gates，由coordinator保留owned requirement/runtime结果并串行重建shared docs、
task shadow和source hashes。冲突未造成中断或丢失，`workaround_used=false`。后续wave应在dispatch前把
shared-path denylist注入worker验收，而不是只依赖文字约束。

### Wave 4：TRADING-2451 preregistration + shadow-continuation runtime

Base=`872d7ccb`，branch=`codex/dual-lane-wave4-prereg-runtime`。本轮从启动即同时具备两个有效lane，
并冻结以下owned scopes：

|Lane|状态|任务/owned scope|冲突与停止条件|
|---|---|---|---|
|Engineering|`COMPLETE`|`ARCH-004G2.../W4E1`；仅`tests/test_shadow_continuation_readiness.py`，把既有artifact validation session由per-test提升到module scope|5 nodeids、真实module source DAG、CLI、missing/fallback/cache blockers、DQ/lineage/validator与安全断言保持；isolated=`198.89s -> 155.18s`，Full worker-s=`479.3692 -> 363.3858`|
|Strategy evidence|`COMPLETE_PREREGISTRATION`|`TRADING-2451_DYNAMIC_V3_CLEAN_SELECTION_S1_PREREGISTRATION`；新requirement、policy/package inputs、独立builder/validator与focused test|package=`dynamic-v3-clean-s1_cf88e2fc1cee51406b6b`，validator=`PASS/0`、focused=11；0 result inputs，未运行evaluator/backtest/search/holdout；clean run仍需owner独立授权|
|Coordinator|`COMPLETE`|`task_register`、本operating model、system flow、generated manifests、compatibility与formal integration|generated=`878/429/449 tasks, 992 modules, 1143 tests`；architecture/contract/full全部PASS|

W4E1的Full profile目标文件为`479.37 worker-s`，但同机isolated仅`198.89s`，说明Full数值受全局负载
影响，验收只使用空闲isolated A/B。baseline中module source setup=`45.22s`，五个call依次约
`40.07/35.41/29.50/23.87/19.31s`；现有module fixture已经避免重复producer，剩余候选只允许让
content-fingerprint保护的PASS-only validation cache跨本module五个测试复用。所有per-test output、DQ、
fallback/cache输入仍各自位于独立`tmp_path`；任何shared source mutation或FAIL缓存都会使切片撤回。

Wave启动时再次发生了一次控制面竞态：Strategy worker在收到最新协调消息前创建requirement并短暂写入
shared task row/尝试切换临时branch。它随后自行删除shared row、回到coordinator branch并等待登记；
coordinator保留owned requirement、正式登记`TRADING-2451`后才恢复实现。未丢失成果，但这证明同checkout
并行仅靠消息仍存在时序窗口；后续自动调度必须把branch lease和shared-path denylist作为执行前硬门禁。

Wave 4最终formal结果：architecture=`446 passed / 51.90s`，contract=`265 passed / 128.13s`，Full=
`6498 passed / 2 skipped / 642 warnings / 940.47s`。Full runner相对Wave 3的`1019.79s`下降`7.78%`，
共同1084文件duration median ratio=`0.9516`，worker busy median=`1008.69 -> 929.22s`；W4E1目标文件
Full worker-s下降`24.20%`。由于仅一个自然Full样本且全局共同文件也整体变快，继续固定
`stable_full_improvement_claimed=false`。首次architecture因deprecation inventory新增引用计数陈旧而
FAIL，完整刷新后复验PASS；首次Full调用在pytest前被provenance gate拒绝，补齐真实run provenance后执行，
两项均未使用workaround。

### Wave 5：paper-shadow drift runtime + strategy authorization hold

Base=`1fa742d3`，branch=`codex/dual-lane-wave5-runtime`。本轮按“双线允许一条lane因真实输入阻塞”的
约束推进，不为填满并行槽位制造策略结果：

|Lane|状态|任务/owned scope|冲突与停止条件|
|---|---|---|---|
|Engineering|`COMPLETE`|`ARCH-004G2.../W5E1`；仅`tests/test_paper_shadow_drift_monitor.py`，一次构造immutable formal contract/protocol/signal-completeness前缀|3 nodeids、clean/missing variant、per-test drift/CLI outputs、真实validator与CLI链保持；isolated=`66.57s -> 29.16/28.58s`，较慢值`-56.20%`；Full worker-s=`155.8019 -> 59.6908`（`-61.69%`）|
|Strategy evidence|`BLOCKED_INPUT`|保持`TRADING-2451` preregistration package、授权边界与输入状态可见|一般续推指令不替代owner对`TRADING-106` clean evaluator/backtest的独立显式授权；不运行search、prospective holdout、promotion或broker|
|Coordinator|`COMPLETE`|task/requirement/operating-model、v19 duration seed/source-bound test、generated manifests、compatibility与formal gates|shared paths单写；本wave只有工程owned change进入集成，策略lane无伪造产物；`production_effect=none`|

Full profile首位`dynamic_v3_system_target_smoothed_operations_hardening`的Full/isolated约为
`260.80/84.21s`，判断为并发负载放大、缺少可归属局部重复，故未修改。W5E1 baseline三个call约
`17.51/18.08/25.40s`，均重复完整上游构造；实现把shared prefix及validation session限定在module/
worker生命周期，clean与missing daily observation仍分别构造，两个clean consumer只读共享同一source，
drift与CLI outputs仍使用各自`tmp_path`。v19 advisory seed已绑定Wave 4唯一自然Full
`full_20260720T175548Z`的`1085 files / 6500 nodes`。最终focused/architecture/contract=
`36/446/265 passed`；唯一自然Full=`6498 passed / 2 skipped / 643 warnings / 929.82s`，scheduler/profile/
telemetry/performance/provenance全部PASS、fallback=false、tail idle max=`0.0080s`。整体runner较Wave 4下降
`1.13%`，但只有一份自然样本，`stable_full_improvement_claimed=false`。

### Wave 6：paper-shadow daily runtime + strategy authorization hold

Base=`1e31ede1`，branch=`codex/dual-lane-wave6-runtime`。最新策略任务已确认Wave 4集成等待结束，但没有
新增`TRADING-106` clean evaluator/backtest独立授权；因此双线继续运行时允许Engineering进入有界优化，
Strategy evidence保持真实`BLOCKED_INPUT`，不把一般续推指令解释为研究执行许可。

|Lane|状态|任务/owned scope|冲突与停止条件|
|---|---|---|---|
|Engineering|`COMPLETE`|`ARCH-004G2.../W6E1`；仅`tests/test_paper_shadow_daily.py`，复用immutable formal contract/protocol/signal-completeness与market/signal source前缀|3 nodeids、每test独立daily output、missing source、真实producer/validator及CLI run/report/validate保持；isolated=`41.93s -> 20.70/20.37s`，Full worker-s=`82.7893 -> 30.1328`（`-63.60%`）|
|Strategy evidence|`BLOCKED_INPUT`|保持`TRADING-2451` preregistration package、source freshness与授权边界可见|未获owner对`TRADING-106`的独立显式授权；不运行evaluator/backtest/search/prospective holdout/promotion/broker|
|Coordinator|`COMPLETE`|v20 duration seed、task/requirement/operating-model、generated manifests/hash与formal integration|focused/architecture/contract/Full均PASS；shared paths单写；自然Full只运行一次；`production_effect=none`|

筛选时三组Full高位smoothed候选的combined isolated由单一`146.09/155.48/158.71s`真实链路主导，
不具备安全的跨consumer复用形态，本wave明确不修改。W6E1 baseline三个call约`10.65/10.11/15.86s`，
共同重复的只读前缀可提升到module/worker scope，而daily outputs与所有失败/CLI路径继续按test隔离。
两次after=`20.70/20.37s`，较慢值降低`21.23s / 50.63%`；v20 seed已绑定Wave 5唯一自然Full，
匹配`1085 files / 6500 nodes`。focused/architecture/contract=`36/446/265 passed`；唯一自然Full=
`6498 passed / 2 skipped / 643 warnings / 926.10s`，fallback=false、tail idle max=`0.0154s`。
目标文件Full worker-s降低`63.60%`，共同文件duration median ratio=`1.0036`，整体runner仅下降`0.40%`，
故仍不声明stable Full improvement。策略lane没有生成新结果。

### Wave 7：unified 2021 clean research + simulation interpretation runtime

Base=`0fc316e5`，branch=`codex/dual-lane-wave7-window-migration`。Owner 明确将 active strategy
research、主回测和主要结论统一到`2021-02-22`，并授权新 preregistration package PASS 后运行
historical-seen evaluator；prospective holdout 仍未授权。

|Lane|状态|任务/owned scope|冲突与停止条件|
|---|---|---|---|
|Engineering|`COMPLETE`|W7E1仅`tests/test_sim_interpretation.py`，复用immutable真实DAG|10 nodeids与PIT/missing/live drift/tamper保持；`105.71s -> 45.15s`，减少`57.29%`；weekly候选负优化已撤回|
|Strategy evidence|`COMPLETE_KILL_PAUSE`|`TRADING-2452` versioned package、validator与historical-seen evaluator；独占新inputs/module/tests/output root|policy冻结后才重建；DQ与2021 alignment必须PASS；300→train-only top20、六个fold、recent diagnostic；prospective access=false|
|Coordinator|`COMPLETE_MAIN_INTEGRATED`|active policy/runtime/CLI/glossary迁移、shared docs/manifests/hash/formal gates与commit/push|保留旧TRADING-2451和Phase A glossary bytes；共享路径单写；不得把historical-seen结果描述为无偏OOS|

本wave把“策略授权”与“工程性能优化”分开：策略lane可执行的最大边界是已知历史 clean evaluator，
工程lane仍只做 test-only runtime 优化。任何 DQ 失败、2021 consistency misalignment、package/source
drift、prospective holdout读取、搜索空间扩张或策略结果参与selection都会使策略lane fail closed；任何
nodeid/producer/validator/tamper覆盖损失或 isolated门槛失败都会撤回工程改动。

策略结果按预注册边界形成 A=`KILL_PAUSE`，没有据此自动启动 B/C、clean search 或 prospective
holdout；工程 W11 完成 Typer command-tree fixture reuse，formal Full failure-fix rerun 为
`6553 passed/2 skipped/1089.21s`。S4C 首次自动集成在全部适用门禁与 freshness PASS 后将
`codex/dual-lane-wave7-window-migration@80ffc28c` fast-forward 到 `main` 并普通 push，远端 SHA
复核一致。该结果证明 validated coordinator closeout 可执行，不代表策略 PASS、S5 cutover 或 G2.5
恢复，`production_effect=none`。

### Wave 8：historical source archive + simulation defensive runtime

Base=`4b6b6ee6`，branch=`codex/dual-lane-wave8-runtime-archive`。本 wave 只处理一项 test-only
runtime leaf 和一项 strategy-evidence portability debt；不恢复 Strategy B/C，不运行 backtest/search，
也不恢复 ARCH-004 G2.5。

|Lane|启动状态|任务/owned scope|冲突与停止条件|
|---|---|---|---|
|Engineering|`COMPLETE_RETAINED_W12E1`|`ARCH-004G2_VALIDATION_RUNTIME_BUDGET_AND_FIXTURE_REUSE/W12E1`；仅 `tests/test_sim_defensive_validation.py`|13 nodeids 保持；`79.52s -> 14.82/15.12s`，worst `-64.40s/-80.98%`；Full 中为 `16.2301 worker-s`，tamper/fail-closed、byte-exact restore 与隔离 latest pointer 均闭合|
|Strategy evidence|`BASELINE_DONE_BLOCKED_INPUT`|TRADING-2454 versioned historical archive policy/manifest/2 exact config blobs、archive resolver、portable-lineage adapters 与专属 tests|实际有7个drift source；两个config已解析，prices/rates exact历史bytes不可得，故四级 replay仍fail closed，退出条件为可信data archive|
|Coordinator|`VALIDATED_MAIN_INTEGRATION`|task/requirements、catalog/system flow、generated views/manifests/hashes、formal gates、S4C main integration|expanded focused=`164 passed/1 skipped`；architecture/contract/reproducibility=`446/265/23 passed`；唯一 Full=`6575 passed/2 skipped`，门禁全部 PASS 后才允许 fast-forward main|

两 lane 的 source paths、module ownership 与 runtime resources 不重叠。若策略 locator 不能严格绑定
sidecar/original locator/disposition，或工程 fixture 出现顺序依赖、跨测试写入、teardown/resource 异常，
对应 lane 独立撤回；不得用另一 lane 的成功掩盖失败。`production_effect=none`、`broker_action=none`。

策略 focused 首轮 `31 passed/1 failed` 暴露原“只有两个配置 drift”的审计假设不完整；修正后的全量
inventory 为 101 exact + 7 drift。实现没有扩大 archive 去猜历史数据：只保留两个 Git 可证明的配置
overlay，并将 S2 记录为 prices/rates exact bytes blocker；最终 focused=`32 passed`。Engineering expanded
focused=`112 passed/1 skipped`，skip 为 Windows 不支持 `os.fork` 的既有条件用例。Coordinator 只在共享
文档、manifests 与 formal gates 闭合后按 S4C 决定是否集成该 fail-closed baseline。

独立集成审查同时修复了两个 P1：archive resolver 必须返回实际校验过的 archive path，而不是继续返回
active locator；`minimum_win_rate_vs_no_trade` 必须成为 Simulation Defensive 的真实 policy gate。
修正后 archive overlay 的四级 adapters 均被 exact path/bytes 测试覆盖，manifest-without-sidecar、rooted/
UNC/path escape、非法 disposition 全部 fail closed；Simulation Defensive 的 invalid/boundary/低胜率语义均有
独立测试。唯一自然 Full=`6,575 passed/2 skipped/1,079.35s`，scheduler、profile、telemetry、performance、
provenance 全部 PASS，tail-idle max=`0.48s`；v23 advisory seed 精确覆盖 `6,577 nodes/1,097 files`。
Wave 8 因此满足 S4C validated-main integration 条件，但 TRADING-2454 仍按真实 exact-data blocker 保持
`BASELINE_DONE`，不得将工程门禁 PASS 解释为历史 replay 已恢复。

### Wave 9：active-window 文档一致性 + runtime candidate falsification

Base=`b2a1e3af`，branch=`codex/dual-lane-wave9-window-consistency-runtime`。本 wave 的策略线只修复
权威研究执行链的 active-window 解释，不运行被Owner关闭的B/C、新backtest/search或prospective；
工程线继续按预冻结门槛筛选长尾，但允许真实证据否决候选，不为维持“双线都有代码”保留无收益实现。

|Lane|当前状态|任务/owned scope|冲突与停止条件|
|---|---|---|---|
|Engineering|`CLOSED_REVERTED_NO_CHANGE`|W13先审计Smoothed data/freshness，再试点Layer1 archive scoreboard duplicate-builder elimination|Smoothed真实链无安全裁剪点；Layer1 `116.56s -> 115.68s`仅`-0.75%`，未达`<=96.56s`并byte-exact撤回；不得原机制重试|
|Strategy governance|`COMPLETE`|TRADING-2456；仅权威研究链文档、专属合同、requirement/task状态|2021 active primary与2022 immutable historical-only同时成立；历史artifact/requested/evaluated bytes不改；focused=`3 passed`|
|Coordinator|`VALIDATED_MAIN_INTEGRATION`|generated task views/manifests/hashes、formal gates与S4C自动集成|最终tree的required gates与freshness已PASS；提交前继续执行lease/owner、归属、upstream与fast-forward检查|

两条lane的保留文件不重叠：工程实现已撤回，仅runtime requirement保留否决证据；策略线修改
research document/contract/requirement。`strategy_logic_changed=false`、`cached_data_mutated=false`、
`production_effect=none`、`broker_action=none`。

Wave 9集成门禁为expanded focused=`52 passed`、architecture=`447 passed`、contract=`265 passed`、
reproducibility=`23 passed`；唯一自然Full=`6576 passed/2 skipped/1082.57s`。Full provenance、scheduler、
telemetry、performance与safety boundary均PASS，tail-idle max=`0.016s`。相对Wave 8的`1079.35s`
墙钟仅增加约`0.30%`，不构成性能退化信号；smoothed链仍占主要长尾，作为下一工程wave候选池继续
要求调用级归因和预冻结收益门槛，不在本wave引入未验证优化。

### Wave 10：uncontaminated selection foundation + opportunity-cost runtime

Base=`ef42c1e7`，branch=`codex/dual-lane-wave10-selection-protocol-runtime`。本wave继续两条私有lane，
但不把“继续策略线”解释为重开B/C或执行新研究结果。

|Lane|启动状态|任务/owned scope|冲突与停止条件|
|---|---|---|---|
|Engineering|`RETAINED_W14E1_EXIT_GATE_PASS`|仅`layer1_simple_rule_meta_policy.py`与专属equivalence test；预计算constrained-search既有20d opportunity component schedule|专属9 PASS；两次after wall=`52.91s/58.11s`，worst相对`104.50s`降低44.39%，通过`<=83.60s`门槛；243 grid、公式/顺序/rounding、DQ/PIT、CLI/schema/status不变|
|Strategy foundation|`DONE_FOUNDATION_ONLY`|TRADING-2457 policy、typed selection protocol contract与专属tests|专属19 PASS；只处理lineage/visibility/window/policy-role/holdout-access admission；不运行B/C、evaluator/backtest/search/DQ/prospective，不生成策略结果|
|Coordinator|`FORMAL_GATES_PASS_S4C_READY`|task/requirements、current chain/system flow、generated views/manifests/hashes、formal gates与S4C|共享路径单写；focused/architecture/contract/reproducibility=`28/447/265/23 passed`；Full=`6604 passed/2 skipped`；post-Full freshness与formal复验均PASS，进入S4C归属/upstream/fast-forward preflight|

工程lane不修改策略阈值或输出，策略lane不修改Layer1实现/测试；两者都不触碰既有TRADING-2452
package/artifacts。当前已知无关工作区文件仍为
`docs/research/growth_tilt_owner_diagnosis_pack.md`的EOL-only状态，blob与index相同，禁止纳入提交。
`strategy_logic_changed=false`、`cached_data_mutated=false`、`production_effect=none`、
`broker_action=none`。

本wave证明双线失败隔离可实际工作：W14只优化production-private重复计算，TRADING-2457只增加研究进入
协议，二者没有共享源码、配置或测试路径；coordinator在两lane通过私有门后才开始写共享事实。自动合入
仍只在最终tree的required tiers、generated freshness、无active lease、可归属worktree与fast-forward
条件全部PASS后触发。

Wave 10自然集成Full wall=`1179.75s`，比Wave 9增加`8.98%`，因此不声明全局稳定提速。W14目标node
Formal Full call由`174.15s`降为`103.41s`，但49个共同slowest节点中位数整体慢`6.18%`，多条旧
Smoothed链同步慢`14%～26%`，worker busy time也近乎同步上移；profile的scheduler/telemetry/
performance PASS、tail-idle max=`12.85s`。处置是保留通过isolated与真实Full双证据的W14，继续把
Smoothed链列为下一工程候选，同时禁止用本次总wall宣称全局改善或立即追加第二次昂贵Full。

### 后续架构方向

- Owner 已于 2026-07-23 通过“结合我们的并行能力推进这些任务”的新显式指令恢复
  `ARCH-004G2_PARALLEL_READINESS_GATE` / G2.5；历史 bootstrap handoff 的
  `next_slice_unblocked=false` 保持不可变，该授权作为后续独立 resume event 记录。Wave 11 已完成
  G2.5 formal closeout，状态为 `DONE`，只证明 readiness/rehearsal 与集成合同，不曾 dispatch、取 lease
  或自动 merge。
- 当前实际下一波只有 `ARCH-004G4 Operations + DATA-GOV-001 D0B`，状态为 `READY` 但尚未 dispatch。
  G3 Reporting 与 G5 Research Wrapper 均为 `PROPOSED`、尚未 dispatch；capacity 仍是最多两个 domain
  worker，不直接同时开放三域。G4 进入 cadence 等待后可把开发 slot 交给 G3，G5 属于后续第二批。
- ARCH-005 S5 source-of-truth cutover 后置，必须单独授权并在真实双线 telemetry 足够后评估；不得与
  另一项中央 shared-architecture 变更并行。

### Wave 11：G2.5 readiness + Data Foundation D0A + GOV-006 N0（formal PASS）

Base=`6ee5903a`。本 wave 使用两个互不重叠的 domain scope，并由 coordinator 独占共享事实：

|Lane|任务/owned scope|本波次退出边界|
|---|---|---|
|Architecture readiness|`DONE`；G2.5 policy、architecture-specific change manifests、ownership snapshot、fragment preview、rehearsal tests|复用 ARCH-005 parser/conflict/planner/scheduler；最多两个 active worker；frozen handoff、真实 base、coordinator manifest、20 个 fail-closed rehearsal 与 deterministic preview 全部验证；未 dispatch、未取 lease、未自动 merge。|
|Data Foundation|`D0A DONE`；immutable publish module 与专属 tests|strict candidate-bound DQ report、immutable snapshot/manifest/history、atomic current pointer、monotonic publish、per-dataset OS lock 和 no-follow containment fail closed；未迁消费者、未执行 provider refresh、未选择物理存储。`dq_execution_provenance_verified`、`store_acl_verified`、`crash_durability_verified`、`consumer_cutover_allowed` 均保持 `false`，下一数据切片为 D0B。|
|Coordinator|`GOV-006 N0 DONE`；task/requirements、system flow、artifact catalog、generated manifests/views、formal gates、commit/push|完成只读 normalization 与 30 条 disposition dry-run，`automatic_apply_allowed=false`；未自动 apply lifecycle 迁移。共享事实仍由 coordinator 单写并在最终 tree 刷新 source hashes。|

GOV-006 是 coordinator governance 工作，不构成第三个 domain worker。Strategy A 已关闭，B/C 未授权；
因此本 wave 的第二条 lane 用于 data/strategy-evidence foundation，而不制造新的研究结果。

Wave 11 formal closeout 已通过：focused=`183 passed / 1 skipped`；architecture=`482 passed / 60.07s`；
contract=`266 passed / 140.76s`；最终 Full=`6710 passed / 3 skipped / 643 warnings`，pytest=`1105.37s`、
runner=`1106.60s`，artifact=`outputs/validation_runtime/full_20260722T201357Z/test_runtime_summary.json`，
SHA-256=`6e324617d82455e9af185aa80fa8f237054fc4a69d17e63853c928f19a606546`，size=`26538 bytes`。
前两次 Full FAIL 作为 append-only attempt evidence 保留；同批三次 runner wall 为 `1109.83s / 1111.95s /
1106.60s`，波动小于 `0.5%`，最终值也较 Wave 10 的 `1179.75s` 低约 `6.20%`，没有 global runtime
regression 信号。module/test manifests、compatibility baseline、deprecation inventory 与 source hashes 已在
最终 shared-doc integration 后按 S4C 顺序刷新并验证；本 wave `production_effect=none`。

## 10. 本轮明确不做

- 不运行真实 periodic operation、策略 backtest、candidate/search 或 provider refresh；weekly 仅执行隔离测试；
- 不把“默认双线”解释成 S5、worker自主合并或 autonomous task mutation；coordinator 仅按S4C
  validated-main门禁自动集成；
- 不因 G2.5/D0A/N0 formal PASS 自动进入 G3/G4/G5、ARCH-004H 或 ARCH-005 S5；当前只把
  `G4 + D0B` 设为下一 `READY` 波次，G3/G5 保持未 dispatch；
- 不运行周期 operations、联网 provider refresh 或 cache 删除；
- 不改变研究窗口、阈值、策略结论、权重、promotion、paper-shadow、production 或 broker。
