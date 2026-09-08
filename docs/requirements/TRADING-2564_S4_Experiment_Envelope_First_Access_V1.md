# TRADING-2564 S4：实验封套与首次结果访问门禁

状态：IN_PROGRESS；负责人：Codex；Owner 于 2026-09-08 同意继续实现上一轮提出的最小 S4。

## 目标与范围

把实验方案、独立受审冻结准入和持久化访问历史接成一个可执行的有界 Python gateway。
本波仅本地工程和合成验证，不调用真实行情、DQ、provider、下载、cache、研究采集、
maturity、scoreboard 或 Composer；order/fill 均为 0，production_effect=none。
主研究窗口仍为 2021-02-22 起；实验必须声明 requested/evaluated range。

现有 ResearchPreregistration.frozen_at/result_visibility 和 SelectionDataBinding 是声明；
S3b capture 的 outcome_access_authorized=False 不能作为首次查看许可。本合同不修改这些历史合同。

## 分步、依赖与验收

1. **最小串行合同**：冻结 exact envelope，绑定 hypothesis、candidate/parameters、implementation、
   input information set、data role/window、comparator、cost/holding accounting、main metric、
   sample/episode、first/repeated look、stop policy 和 seen/failed variant history。所有影响研究解释
   的规则均绑定受审政策内容及 SHA，不引入新的数字 floor/threshold。独立 admission 绑定完整
   envelope SHA、owner review reference、时间证据及固定 exposure domain。严格 JSON、字段和类型
   检查；不把自报 reviewed=True、NONE 或调用方 frozen_at 当作验证证据。
2. **有界持久化服务**：受审 authority 固定 canonical ledger root 与 outcome information domain，
   domain 不能随 candidate 名称、参数、源码、attempt、数据 revision 或 output path 重置。
   freeze/attempt/view 采用不可变、完整前驱哈希事件；replay 派生 trial/attempt/access 数。
   同一 S4D FileExecutionLeaseStore arbiter 内执行 replay/check/append，不创建第二套锁。
   VIEW_PENDING 必须持久化并复验后才调用 loader；pending 即 POSSIBLY_EXPOSED。
   loader 抛错、进程死亡、部分输出、terminal write 失败不回滚历史，不允许重试变回 unseen。
   成功结果绑定实际 bytes SHA/size；replay 只返回 metadata；再次查看必须登记新 access。
   显式 bootstrap 必须匹配 authority 固定 genesis SHA。业务事件与 canonical S4D store 内的
   独立不可变 checkpoint 链在同一 arbiter 下按 PREPARED→事件→COMPLETED 写入；缺失事件尾、
   丢失 ledger、半写 checkpoint 均拒绝继续，不从空目录自动重建历史。不把 checkpoint 放进
   lease events 目录，运行 lease 必须声明这两条资源。该冗余核验能发现单侧缺失或截断，
   不覆盖两条本地历史同时被完整删除：即使 authority 保持不变，同一已受审 genesis 也无法
   区分首次 bootstrap 与全部历史被外部删除。能够一致改写两条历史的本地攻击者同样超出本波
   信任边界；本波没有外部不可删除 anchor，不把本地 append-only 写入约束描述成外部存证。
3. **负例与集成**：合成测试覆盖 strict parsing、改名/改参数、旧与未知历史、前驱篡改、root 替换、
   lease identity、并发 pending、loader 异常/崩溃、terminal write 失败、重复 access 与 policy 不匹配。
   模块独立审查；更新 system flow/catalog/source closure；16-worker focused 验证、适用 formal tiers
   与自然边界 Full；exact candidate 普通 main fast-forward/push 后核对 SHA 并清理临时内容。

合同先由 coordinator 审阅固定，runtime 才依其执行。共享文档、任务源、generated authority、formal
validation 和最终集成由 coordinator 管理，worker 仅在指定模块/测试路径协作。

## 有界 API 与固定资源

`ResearchOutcomeAccessGateway(execution_root, candidate_commit)` 从固定
`config/research/research_experiment_authority.json` 读取受控本地 authority；本波仅测试 fixture
安装该文件，仓库不新增真实 authority。authority 绑定 canonical_execution_root、canonical_git_common_dir、
ledger_relative_path、genesis SHA、domain/history 及 exact envelope/policy allowlist。
服务提供 `bootstrap(lease)`、`freeze(envelope, admission, time_evidence_bytes, lease)`、
`begin_attempt(envelope_sha256, attempt_id, lease)`、
`view(attempt_id, access_id, loader, lease, scope=SYNTHETIC_ENGINEERING_ONLY)` 和 `replay_metadata()`。
具体 Python 参数约束以严格签名为准。

Checkpoint 位于 canonical `CheckoutLeaseGuard.store.root/research_outcome_access_checkpoints`，
不位于 lease `events_root`。本波全部 family/domain alias 共用一个 runtime 常量固定的物理 ledger，
authority 的 ledger_relative_path 必须精确匹配该常量。服务每次均盘点它，不只盘点 checkpoint
中仍可达的路径；不提供多 ledger bootstrap，从而避免删除旧 alias 尾 checkpoint 后留下不可达历史。
Global genesis 只绑定物理存储身份与 scope，具体 domain 定义在各 FREEZE 的受审 authority 中保留。
运行 lease 必须覆盖固定 ledger 和该 checkpoint 资源；普通 replay 不返回结果 bytes。
固定 ledger 路径为 `outputs/research/experiment_outcome_ledger_v1`，本波只在合成 fixture 内创建。
身份继承使用全部受审 FREEZE 的带类型 domain-definition / input-information-set 连通图。
例如已暴露 A=(domain4,input3)，B=(domain9,input3) 已建立别名关系，即使 B 尚未成功查看，
C=(domain9,input8) 也必须继承 A 的暴露；只比较当前两项 SHA 的 direct OR 会遗漏这种传递关系。
同一连通分量的历史库存整体继承，实际 access 按 requested interval 重叠判断。

## 信任边界与开放依赖

Owner review、canonical domain 注册及其历史盘点是受控本地信任输入，不是密码学身份签名。
门禁保证本 API 的写前次序、范围绑定与暴露继承，不保证操作系统外所有人从未见过行情。
未知历史不得被空 ledger 升级为 untouched；可能暴露不得恢复 NONE。时间证据的级别须显式保留，
本地记账时间不等于外部可靠时间 witness，也不能据此宣称 OOS。
封套中的 implementation/input SHA 是受审声明；本波 Python loader 是受控合成依赖，
固定标注 TRUSTED_SYNTHETIC_CALLBACK_NOT_ATTESTED，不能宣称已验证 arbitrary callback 的实现字节。
服务仍核验实际 checkout commit 和 S4D scope；未来真实 adapter 必须另外验证实际 producer/source、
DQ/PIT 与时间证据。政策文字只冻结规则内容和审查身份，本波不把文字自动解释成数值 stopping、
sample/episode 或会计执行引擎。

本波不接入 legacy maturity 价格读取/计算、scoreboard observation 读取、indicator casebook/ablation、
report 直接读取或 Composer labels。这些接入仍是本任务 S4 后继；要在首次行情/结果读取前接门禁，
仅包装最终 writer 不满足要求。真实实验数值 sample/episode/stop、重复查看政策、可靠冻结时间 witness、
跨消费者历史调查与 DQ/PIT 完整性须在真实运行前按受审方案补齐。工程 PASS 不解除这些依赖。

Accounting 本波冻结规则身份和内容，不重写 return attribution；现金贡献与复合收益归因修正、
exposure-matched posthoc 比较限制仍由 umbrella S4 后继推进。

## 工作区生命周期与治理

基线：e651a663f01a688f1e31ea76caa82cd3945b9228；SINGLE_LANE 最小串行公共合同波。
复用 D:/Work/AITradingSystem_devx014_source_preservation，task branch
codex/trading-2564-s4-experiment-first-access-v1；不创建新 clone/worktree。
本任务治理与合成证据保留在
D:/Work/AITradingSystem_devx014_source_preservation/outputs/architecture/trading_2564_s4_experiment_first_access。
formal runtime 放在 outputs/validation_runtime；任何后续临时 source fixture 必须先登记确切路径、
用途与删除条件，保留 canonical evidence 并核对 SHA 后才能删除。已排除用户文档不读取、不散列。
发布后删除已合并 task branch，canonical证据保留供复验；如存在未清理路径，逐项记载原因与退出条件。

## 进展

- 2026-09-08：S5 即时诊断已在 e651a663 发布，Full 12203 passed / 5 skipped，事务已 RELEASED。
- 2026-09-08：本波 START/LANE 初检 PASS；已创建独立 source publication transaction 并进入
  TASK_SOURCE_PRE_WRITE。登记本需求后重跑 LANE，PASS 后开始合同实现。真实业务动作仍为 0。
- 2026-09-08：合同 API 经 coordinator 阅读审查固定，runtime 依此开始实现；新增必填政策语义
  覆盖 overlap、missing、maturity、entry/terminal liquidation/rebalance/self-financing/carry。
  源身份采用 74 条明确路径的后继 authority，保留原有冻结 authority；restricted intersection
  仍为原有四条共享路径。Source implementation 尚未验收，formal/Full 尚未开始。
- 2026-09-08：实现审查要求修正 loader 持锁范围、控制类异常传播、事件时间回退及域历史继承。
  旧 authority 的历史库存不受旧封套选用区间限制；具体 access 才按 requested interval 重叠比较。
  单物理 ledger 方案替换未发布初稿的多 ledger 路由，不添加新的目录注册器或锁。相关负例通过后
  才进行正式验证；这不是已接受的临时绕过。
- 2026-09-08：独立纯内存反例确认“先换别名、再换 input revision”可绕过初稿的 direct-OR
  身份判断；v2 改为全 FREEZE 的连通闭包并加入三步链负例和独立域正例。原 v1 测试字节及结果
  保留，最终验收依修复后的联合合成验证与正式候选。
- 2026-09-08：v1 强制终止子进程负例在测试收尾阶段阻塞；现场已记录业务链保留
  VIEW_PENDING、S4D arbiter 已 RELEASED，以及测试 worker 身份。保留源版本、日志和
  runtime_hang_diagnostic_v1.json，v2 用 Pipe 握手消除被 terminate 的共享 Event 锁风险。
  v1 的失败或不完整结果单独保留，不以修复后 PASS 覆盖；仍采用并行 pytest 验证。

- 2026-09-08：修复后的合同/runtime 联合合成验证与独立实现审查 PASS，exact 源绑定及原始结果见
  outputs/architecture/trading_2564_s4_experiment_first_access/implementation_validation_v1.json 和
  implementation_independent_review_v1.json。v1 清理阻塞与不完整运行独立保留；首次访问前持久
  pending、跨别名传递历史、进程终止保守暴露及不持锁 callback 已按修复后测试审查。
  当前为最小工程基线，随后执行来源生成、正式候选四层与 Full 验证、普通发布和清理；
  完成状态以同目录 final_validation_acceptance_v1.json、main_publication_v1.json、
  closeout_verified_v1.json 的实际候选与终态为准，未产生的回执不构成 PASS。
- 2026-09-08：首次共享回归 495 passed / 1 failed；唯一失败为 current deprecation inventory ID
  仍绑定前版计数。官方扫描结果仅将本波两个 module、两个 test file 计数逆回，即精确重建旧 ID；
  9 个 legacy surface、856 个现有 writer、移除状态及 gate 均未改变。只更新当前 ID pin，
  不修改历史冻结 inventory 或移除许可；保留原失败 XML，重建四套来源生成物并重测整个
  deprecation 测试文件。原始 495 项与更正后结果的适用关系在 source_validation_composition_v1.json
  记录，最终候选仍必须执行完整 Full。
