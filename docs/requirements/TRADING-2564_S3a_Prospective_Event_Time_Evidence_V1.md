# TRADING-2564 S3a：前瞻事件时间证据

日期：2026-09-07（Asia/Tokyo）。状态：IN_PROGRESS。
所属任务：TRADING-2564_LONG_TERM_RESEARCH_CAPABILITY_IMPROVEMENT_V1；P1。
Owner：Project Owner / integration-coordinator；SINGLE_LANE，最小串行合同波。
冻结 main：f60153cb962d7b80013be258a4708af4012406fb。
Owner 指令：S2c.2 发布并说明下一步为 S3 后，要求「继续吧」。

## 1. 前置验收与本波范围

S2c.2 已完成普通 main 发布；local/remote/candidate 均为上述 SHA。Full 为
11159 passed / 5 skipped / 641 warnings，跳过节点及警告与基线相同；四类正式验证与
36 项 actual candidate E2E PASS。验收与关闭事务见
outputs/architecture/trading_2564_s2c2_five_candidate_preview/closeout_verified_v2.json。

本波补齐真实前瞻必需的事件时间记录能力，复用既有不可变文件和 canonical XNYS 日历。
它记录本地何时完成保存，并独立核验时间关系；不把日期、hash、caller JSON、历史 preview
或 download timestamp 冒充真实冻结或输入已知时间。不修改冻结 producer、策略 registry、
旧观察 ledger、旧收益函数、Named DQ 合同及其 55/57/59 源码集合。

本波只执行工程实现与 synthetic tests；不选择真实 activation 日期，不读取真实市场输入，
不执行真实 DQ、manifest replay、preview、capture、observation、maturity、scoreboard、
下载、cache mutation、provider、QuantConnect、Options、paper/live、broker、order、fill、position。
上述真实动作计数全部为 0。软件测试中的磁盘事件必须单独标记 synthetic，不进入研究证据链。
现有 heartbeat 不因本波启用；不增加 scheduler、外部 trigger、生产 CLI 或第二套锁。

## 2. 已核实的时钟差异与新版本

五候选原 target F 经 targets.shift(1) 乘下一 session 的 pct_change，首段为
Close(F) 到 Close(next(F))。S2c.2 在 F 收盘后检查完整 as_of F 输入，再产生的 preview
不能证明 Close(F) 前已经存在。旧 legacy_applied_session 只描述原算法，原样保留。

Composer 的 frozen signal_plan 则为 signal F 在 D=next(F) 生效，控制 Close(D) 到
Close(next(D))。S3a 采用单独命名的 NEXT_XNYS_CLOSE_FORWARD_V1 工程时间合同：

- feature_session=F；必须为已完成的 canonical XNYS session；
- decision_effective_session=D=next_XNYS(F)；截止时刻为 D 的 canonical close；
- recorder 在任何本次 payload 写入前内部采样 started_at，并要求 started_at 严格晚于 F close；
  writer 返回后的确认上界必须严格早于 D close，以双边顺序证明 payload 写入/完成窗口；
- 对应未来收益起点只能为 Close(D)，第一段终点为 Close(next(D))；本波不读取或计算收益；
- 五候选使用新 forward timing version，明确较 legacy 收益起点多延后一个 session，不能
  宣称与历史回测的收益等价；Composer 可以继承既有收益时钟；
- 月频候选保留 preview 的 target_session F、target_rebalance_session R 和权重，D 由 F
  求得，不从旧 R 推导，不在 D 重算权重；同月 carried target 不等于新调仓。

这些是防止把信号生成前收益纳入结果的时间约束，不是可调收益门槛。配置须记录 owner、
版本、工程审查状态、原因、影响、synthetic 验证与首次真实 capture 前复核条件。
本波不签发 owner 的真实 activation/capture 授权，不选择 sample/episode 数字或提升策略结论。

## 3. 最小事件合同与信任边界

新增独立 sidecar，记录 ACTIVATION、INPUTS_OBSERVED、SIGNAL_RECORDED 三类事件。
每个事件绑定明确业务 key、payload bytes/SHA/size、policy/plan identity 及前驱事件。
输入事件只证明所保存的确切本地 bytes 在记录时已知，不证明 provider 当年的 available_at。
定义、数据、策略和 DQ 的真实性仍由各自原 verifier 负责；本层不能从 caller 的 hash 声明
签发这些 authority，也不能反序列化一个说明 DTO 为 VerifiedNamedInputs。

复用 write_contained_artifact_bytes(immutable=True) 与 read_contained_artifact_bytes，
必要互斥复用 exclusive_store_maintenance，不新造锁。生产入口不接受 caller 可填写的 now。
必须在 payload writer 完成文件 fsync、namespace 同步和 post-write 核验后，由 recorder
内部 UTC clock 采样 payload_durable_completed_at，再落不可变 completion witness。
调用开始时刻、DTO generated_at、mtime 或写入前采样不得替代完成时刻。

以下段落保留 S3a v1 历史合同；其中跨时钟下界推论已在 S3b Full 中发现不成立，新写入的
修正方案见 [S3b 时钟证据修正](TRADING-2564_S3b_Clock_Evidence_Correction_V1.md)。旧 v1
bytes 和 retained verifier 不以新语义重新判断；修正完成前不得宣称当前 S3b 已通过验收。

UTC start 后紧邻 monotonic start；全部 payload writer 返回后先采 monotonic end，再采
UTC completion。这个内侧 monotonic interval 是 UTC elapsed 的下界，runtime 和 retained
verifier 均拒绝不一致；仅按 datetime 的 1µs 表示精度处理量化误差，不允许策略化 clock drift。
后续 lease recheck、witness start 和最终核验采时也不能倒退。

实际 recorder 还须持有并重验覆盖该输出根的现有 S4D write lease；immutable=True 本身不是
并发 compare-and-swap。store maintenance 只复用已存在的文件写入互斥，不建立新的全局发布
authority。事件固定槽位由完整 plan SHA 派生 stream，再按 feature session/stage 定位；
stage 前驱绑定 activation→inputs→signal。不新增 mutable head 或平行观察 ledger。
同一 plan 的 activation 不可覆盖；单个日期的 incomplete 槽位不妨碍后续合法日期独立记录。

现有 immutable publisher 的 root authority 禁止持锁期间跨根读取。记录器因此采用永久的
两阶段协议：外部预验 policy/calendar、前驱和 S4D snapshot；第一段 store maintenance 内
检查业务槽位并写入 intent、lease snapshot、全部 payload，采样原始完成时间；退出后复核
外部 authority；第二段 maintenance 内复核原 lease 未到期、逐字重读全部本次 bytes，
completion 仍不存在时写入原调用持有的 witness；退出再复核 live lease 和 retained chain。
这是输出提交协议，不绕过 root 保护，也不增加锁。两段之间崩溃或并发重试均保留 INCOMPLETE；
完成的同 key/同 bytes 必须返回原 witness。外部复核失败不能签发 witness。

record 和 retained verifier 均在读取前限制 event、intent、member、lease 和前驱 locator 的
固定 stream/session/stage 范围。Coverage 保留 input/signal 固定 intent/lease 残片；不枚举
未知 payload 来推导成功，不把孤立残片降级成普通 GAP。

completion witness 中的时间只见证 payload 已持久化，不见证 witness 自己的写入结束。
如果未来 admission 要求整个 witness bundle 也在 deadline 前完成，可信父执行器须在 witness
writer 返回后另记确认时间并核验；本波不能把未接入的父确认冒充已经存在。

同一已完成业务 key/相同 bytes 只读幂等返回原 witness；不得重签更早或更新的时刻。
同 key 不同 bytes 拒绝。intent/payload 已存在但 completion witness 缺失的崩溃现场必须
保留并返回 INCOMPLETE_EVENT，不能事后补签成功。跨 UTC 倒退、前驱/内容/计划错绑、
迟到和不完整事件不能成为可准入时间证据。异常后已有 bytes 保留，不清理伪装成未发生。

本地时钟和运行 recorder 是明确的受信假设；本层不是外部签名时间戳、恶意管理员防篡改
证明或 provider PIT 证明。retained verifier 只能复核原事件及 bytes/链/时间约束，不能
独立证明主机时钟从未被恶意调整。结果固定 temporal_evidence_only=true、
observation_authorized=false、provider_available_at_status=NOT_ESTABLISHED，
oos_admission_status=NOT_ESTABLISHED、production_effect=none、broker_action=none。

## 4. Activation、会话及缺口

本地 activation witness 的写后采样时间是定义 payload durable completion 的时间上界。
首个 feature session 选在 activation 的 America/New_York 日期之后的第一个 canonical
XNYS session；不以 caller 的旧日期启动，不把当天已经看过的片段补成结果盲前瞻。
实际 activation 仅由后续 reviewed bounded manifest 调用；本波合成冻结不设置真实起点。

Expected feature sessions 从 canonical calendar 枚举，不从价格行或成功事件反推。
日历继续使用 reviewed special-closure policy 和既有正常/半日 close，拒绝休市 feature date，
不硬编码 UTC close、不假设 next date=tomorrow，也不新增未经定义的 open 时刻。
旧缺口永久列出：后续合法 F 可按时间前进恢复，不能因旧缺口永久阻断未来，也不能把迟到
补录标成当时已记录。未完成事件保留 INCOMPLETE，过期未记录 session 保留 GAP。

## 5. 顺序、职责及验收

1. Coordinator 追加 task 与本需求；完成 SINGLE_LANE/contract-change preflight 和 fence。
2. 先冻结版本化事件/时间合同和策略边界；独立只读审查身份与收益时钟，再实现 recorder。
3. 实现 contained immutable sidecar、幂等/崩溃拒绝、canonical chronology 与 gap 检查。
   各生产 API 不调用 DQ/producer/旧 ledger，不计算收益；无真实捕获入口接线。
4. 16-worker tests 覆盖真实 writer 返回后采时、迟到、缺 witness、并发同 key、不同 payload、
   内容/前驱篡改、UTC naive/倒退、DST、周末/节假日/半日/特殊休市、F/R/D 区分、月频 carry、
   缺口后的合法恢复、同事件幂等保持原时间及全部零业务副作用。
5. source/final 官方生成流程、精确 candidate 正式验证/Full、普通 main 发布及审计收口。
   模块和测试清单的机械 ratchet 随真实 inventory 更新，不降低旧门禁。

S3a 工程 PASS 仅完成时间能力。S3b 仍须绑定实际 source snapshot/新代码、严格 Named DQ、
同 context preview、受审 bounded manifest 与真实 activation，才可采集；旧一次性授权不复用。
S4 的 hypothesis/comparator/cost/stop/sample/episode 规则须在首次查看相应 prospective outcome
前冻结。无成熟样本时只披露未成熟，不自动打开 scoreboard 或 OOS verdict。

## 6. 工作区与进度

复用 D:/Work/AITradingSystem_devx014_source_preservation，分支
codex/trading-2564-s3a-prospective-event-time-v1；不新增 worktree、clone 或真实 cache。
本波证据位于 outputs/architecture/trading_2564_s3a_prospective_event_time。
Owner/next owner 为 integration-coordinator；root 是 canonical main 执行根，仍保存前波唯一
actual/formal evidence 并承接 S3b。分支发布并确认无唯一实现后删除；root 待证据进入可验证
canonical 归档且无后续执行依赖才清理，不能仅凭 merge 删除。

- 2026-09-07：READ_ONLY/START PASS，main/origin f60153cb9、无活动 lease、审计 clean。
  已取得 source v1 publication fence；两路独立只读审查确认 sidecar 可独立实现以及上述
  两条收益时钟差异。真实研究、数据、capture 与交易动作仍为 0。
- 2026-09-07：132 项 DTO 合同并行测试 PASS；69 项真实 immutable writer、真实 S4D lease
  synthetic recorder 测试 PASS。独立源审查发现并已关闭：读取前路径范围、部分 UTC 倒退、
  maintenance 跨 root、input orphan 和完成事件并发幂等问题。新增反例与最终联合验证继续执行。
  初次误用 PATH Python 3.14 的 fixture lease 失败不作为 PASS；后续统一项目 .venv 3.11.9，
  维持 16-worker loadfile，未用串行或 mock lease 替代门禁。
- 当前工程源准备正式验证；新增兼容性 successor 精确继承 S2c.2 的 33 个源加本波 6 个源，
  restricted historical source 交集仍只有 system_flow 和 3 个架构测试；未知后继与缺绑定拒绝。
  Atlas、task、架构 inventory 和 report-flow seals 由本协调变更更新；历史源 hash 不重写。
- 最终联合 focused：204 passed / 0 skipped / 0 warnings，16-worker loadfile，251.79 秒；
  XML：outputs/architecture/trading_2564_s3a_prospective_event_time/event_time_focused_v1.xml。
  其中 132 项纯合同、72 项真实文件协议及 lease 合成测试。正式候选验证与 Full 仍须独立通过。
- 共享检查首轮 200 passed / 5 failed；五项 Atlas 失败均为 canonical task update 的 notes
  未保留完整 requirement_refs，导致总需求绑定被正确拒绝。source v1 在 Full 前结束并保留失败
  证据；source v2 经 canonical writer 恢复总需求、S2b、S2c、S2c.2、S3a 五项引用。
  未修改 Atlas 检查或页面绑定路径；复验与后续正式验证使用新事务。
- 共享兼容性复验 205 passed / 0 skipped，16-worker loadfile，226.37 秒；XML 为同一输出根的
  source_shared_focused_v2.xml。源提交后按精确 SHA 重建 Atlas 和四类正式验证，再执行 Full。
  最终验收、local/remote/candidate SHA 及清理状态由该根的 closeout_verified_v1.json 记录；
  没有该最终 PASS receipt 时不得把本节的 focused PASS 当作已发布。
