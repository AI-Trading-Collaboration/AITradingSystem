# TRADING-2564：阶段任务唯一选择机制试行 V1

## 身份、授权与边界

- owning task：`TRADING-2564_LONG_TERM_RESEARCH_CAPABILITY_IMPROVEMENT_V1`；复用现有 umbrella，不新增治理 successor。
- owner：Project Owner；执行与回放：Codex coordinator。
- owner instruction：2026-09-09「好的，你可以尝试推进这个方向看看是否能解决我们最近出现的这类飘逸」。
- status：`IN_PROGRESS`；本轮为有退出条件的 developer-workflow pilot，不改变投资政策。
- frozen local-main：`a5b82f14f97a898d8c25951560f6e77d15aca295`。
- mode：`SINGLE_LANE`；本轮只交付书面裁决规则、来源绑定的案例回放与当前下一步记录。
- 不开发新 selector、scheduler、锁、数据库、页面或分数模型；不修改历史冻结 portfolio/AGENTS 字节。
- 真实研究、行情读取、DQ 执行、capture、provider、下载、缓存与交易动作均不在本轮范围。
- `production_effect=none`；`broker_action=none`。

## 本轮步骤与验收

后继决定 SDP-20260909-002（2026-09-09）：Owner 在确认 Composer 等待计划后明确要求推进其他
任务；已发布输入连接不再属于在途依赖。当前选用既有 S4 的最小会计算术核验，作为 equal-risk
首次结果消费的必要依赖；具体事实、来源、选择、排除项和返回条件见
[TRADING-2564 S4 前瞻会计](TRADING-2564_S4_Forward_Accounting_V1.md)。这不改变 Composer
主问题、真实采集窗口或既有外部/交易授权，不启动新 scheduler 或随机 backlog 工程。

1. 在 canonical task row 关联本需求，保持 umbrella 的未完成依赖真实可见。
2. 冻结阶段选择、全优先级任务准入、证据状态、必要依赖证明、同级消歧、停止与返回规则。
3. 对近期已发布材料做回放，区分当时可知事实、今日解释与未发布在途工作；不以事后结果评价旧决策。
4. 记录当前唯一主动作及 owner/依赖，明确不能选的工程扩展和重新选择触发条件。
5. 验证来源、交叉引用和规则反例，完成既有 publication 流程；退出本次建设，进入实际选择观察。

验收：给定相同冻结输入，逐条裁决必须得到同一动作或显式 `OWNER_DECISION_REQUIRED` / `WAIT`，
不能靠任务 P0/P1、容易实现、顺手修复或沉没成本改变结果。回放只证明可解释性与规则一致性，
不宣称已证明长期防漂移效果；真实效果需后续任务选择记录。

## 裁决协议 V1：先决定阶段，再决定动作

本规则补充 TRADING-2549 的 evidence-first 执行纪律，试用于 TRADING-2564 后续自主选项及其
TRADING-2560/2563 研究依赖。覆盖 P0/P1/P2、umbrella 内的小阶段、维护候选和已有任务的继续推进，
不只检查新 task。它是 owner 授权的书面试行，不是已接入所有 CLI/automation 的运行时门禁。
既有自动化、其他任务和外部授权不会由本文件自动暂停、扩大或取消。

### A. 固定输入，禁止执行者悄悄换目标

每次选择先在本需求或相关任务的 supporting requirement 写一个 decision record，至少包含：

|字段|必须表达的内容|
|---|---|
|identity|decision id、记录时间、exact published code/source refs、受影响 task，以及上一个决定|
|stage|owner 选择的一个首要问题、decision_enabled、合法证据类型、已有退出规则引用、非目标|
|authority|目标的 owner 指令/受审方案；允许执行什么与优先执行什么分别记录|
|facts|每个条件为 TRUE / FALSE / UNKNOWN，附来源；UNKNOWN 不能默认为 FALSE 或 READY|
|candidates|本轮实际考虑的动作、task/owner、依赖、是否在途、授权边界、完成验收及返回动作|
|selection|规则编号、唯一 action 或 WAIT / OWNER_DECISION_REQUIRED、排除其它候选的理由|
|resume|什么新事实触发复核；在触发前不得因新发现可优化项而重排|

首先继承最近明确指定的阶段问题；只有 owner 明确换目标或既有退出规则命中才结束/更换阶段。
宽泛的“继续建设长期能力”授权不自行抹掉 primary question。若两条 owner 指令对当前首要目标
确实冲突且不能按时间/明确范围解释，唯一输出是 `OWNER_DECISION_REQUIRED:STAGE_CONFLICT`。
缺少目标或退出规则时先提供具体选择与缺口供 owner 决定，不由执行者发明策略收益门槛。

运行中已确认的安全事件按既有事故流程处理，不要求先完成阶段计划；但 hypothetical risk、
无受影响消费路径的审计建议或“可能更稳健”不能冒充正在发生的事故。

证据冲突按对象判断：已发布 canonical event/receipt 可说明已完成事实；未发布的 dirty 文件与
active lease 只证明有人在做，不能升级为完成。较晚 supporting requirement 提及尚未同步的旧
task row 时应先核对它引用的 exact receipt；在核对前标 UNKNOWN，不能重复执行旧 row 所建议的动作。
源码/运行回执未被本轮读取时，不宣称已经独立验证其研究结论。

### B. 顺序裁决，命中即停

R0 是输入完整性检查，R1–R6 依次执行。这里的顺序是 developer-workflow pilot 规则，不是投资
阈值；理由是保护有效性、及时结束已完成阶段、优先取证、只补必要依赖，最后允许显式等待。

|规则|条件|唯一下一步|
|---|---|---|
|R0|会改变选择的目标、事实、授权或证据身份为 UNKNOWN/冲突|按下面 R0 分流表输出唯一动作。未知不授权新工程。|
|R1|已确认当前运行安全受影响，或当前阶段必需证据被具体正确性错误污染|按已有事故规则控制影响，或修复该错误的最小完整范围。完成后返回原阶段，不自动扩成通用重构。|
|R2|当前阶段的既有退出条件已经满足|作出继续/停止/转向决定并记录；负面结论也可完成阶段。不得继续补工程来延后裁决。|
|R3|缺少决定性证据，且对应取证动作所有执行前提与授权已满足|选该取证动作；工程 PASS 不能替代实际证据，已读历史不能升级为 pristine OOS。|
|R4|R3 的取证动作存在已证明的必要依赖，依赖可在既有授权下解决|只选依赖图中当前可执行的必要叶节点；冻结验收和返回的 experiment/action id。依赖在途时按 C 输出等待，不能重复实现。|
|R5|关键依赖只能等待 owner、provider/数据到达或真实交易日成熟|明确等待对象和恢复触发；可选已获授权的阶段内独立取证动作。没有这类动作就 WAIT，不能默认转做工程维护。|
|R6|没有符合以上条件的候选|OWNER_DECISION_REQUIRED:STAGE_PLAN_GAP，并提供具体缺口；不从 backlog 随机挑可做项。|

R0 不阻止对一个独立且已确认安全事故做已授权的立即控制；这种控制仍使用事故 authority。
已知错误必须证明影响当前证据/运行，而不是只因用了“correctness”标签就命中 R1。
未到实际回报读取时，不因为未来可能需要所有 outcome reader 便把它们都列为今天的 R4。
相反，当前具名动作确实会首次读取 outcome，则其封套、历史暴露与相关 DQ/PIT 前提必须先满足。

R0 的分流也按顺序命中即停：

1. 阶段目标/退出规则缺失或 owner 指令冲突：`OWNER_DECISION_REQUIRED:STAGE_CONTRACT`。
2. 缺口能用已授权的只读核查直接消歧：按 C 消歧后选唯一核查；C 无法消歧则 `OWNER_DECISION_REQUIRED:TIE`。
3. 唯一剩余缺口来自已具名的交付/真实时间等待，并且已有责任方与恢复触发：`WAIT_FOR_NAMED_EVIDENCE`。
4. 其它情况：`OWNER_DECISION_REQUIRED:UNRESOLVED_INPUT`，列出缺少的具体事实或授权，不自行推定。

R1 中尚未控制的实际安全影响先按既有事故 authority 处置；影响已受控而证据错误仍在，才选择
最小修复。若多个事故/错误之间无已有事故等级或阶段顺序可比较，同样使用 C，而不临时设风险分数。

### C. 必要依赖与同级消歧

工程候选获得 R1/R4 资格必须同时列明：受影响的具名实验/动作、已观察失败或可检查的合同缺口、
从该动作到本修复的直接依赖、拒绝现有合法路径的具体原因、最小验收、修复后的返回动作。
多个前置可以形成有向无环图；不得要求“只能有一个 blocker”。每次只选择该图的一个可执行叶节点。
发现依赖环、无限递归的“先完善平台”，或无法证明某条边时返回 R0/R6，不无期限展开。

同一规则命中多个动作，严格依次：

1. 采用本阶段已预先指定的证据缺口/动作顺序；不能看完结果再改这个顺序。
2. 同一证据缺口存在多个实现方式时，只有有效性和范围等价可证明，才允许成本支配比较：某方案
   的必要依赖集合是另一方案的子集，且有证据证明其资源、用时均不更差，并至少一项严格改善，
   才排除被支配方案；最后只剩一个方案时选择它。
   “大概更容易”不算证明；未知耗时不填零；成本/时间互有优劣属于不可比较。
3. 若剩下多个不同动作，输出 `OWNER_DECISION_REQUIRED:TIE`，列候选及具体取舍。
   不按 task ID、创建时间、P0 标签、任意数字权重或实施方便程度伪造最优次序。

若唯一选中的动作已有其它执行者持有效 lease/在途任务，本执行者输出
`WAIT:OWNED_ACTION_IN_PROGRESS`，同时保留该全局 primary task、交付验收与恢复触发。
这仍只有一个动作：等待已有交付，不再把“跟进”和“重复开发”当两个并列候选。

合法独立取证只能复用阶段已声明范围与顺序；不得因为 primary 被阻塞就暗中把 secondary 升级为
新的 primary。真实授权仍在 dispatch 前按既有 manifest/门禁重放；decision record 不提供新授权。

### D. 返回、重排与试行退出

- 工程依赖验收后立即重新从 R0 判断原实验；发现新的 blocker 必须重新证明依赖，不自动继承优先级。
- 允许重排的事件仅为：owner 明确改目标/顺序/范围；具名证据到达或被判无效；既有退出条件命中；
  依赖/授权/在途交付状态改变；具名安全事件发生。新建文档、测试 PASS、发现可优化项不单独触发重排。
- 必需的 final validation/publication/cleanup 属于已选任务的验收成本，不能跳过；其间发现的
  非阻塞优化不自动成为 successor。若其真实成本会使阶段目标不可达，显式交回阶段重估，不静默扩容。
- 本次建设完成后停止添加机制功能。后续选任务时复用 decision record，不新建 dashboard/评分器。
- 效果观察覆盖下一次工程依赖完成后的研究交接，以及首次授权/自然时间阻塞时的选择；这是按事件
  定义的试行出口，不是任意“完成 N 个任务”门槛。若这类事件尚未发生，效果保持 NOT_OBSERVED。
- 复核指标：是否有无具名依赖的工程插队；是否正确返回取证/明确等待；同一冻结输入能否重现选择；
  是否更改了阶段目标却没有 owner 决定。任何失败均回到原任务记录修正，不自动开长期治理 successor。

## 来源绑定与历史回放

回放范围为下列已发布 Git 材料。每行只检验当时记录能支持的选择；今天的规则是反事实试用，
不是宣称当时 owner 已批准此 V1，也不因后来研究失败否定当时合理的取证。

|来源|exact commit|文件|
|---|---|---|
|E1|7c687124801064a3878aa122a33f2d9b2e9a7fb5|docs/requirements/TRADING-2549_Evidence_First_Research_Portfolio_And_Reader_Entry_Reset_V1.md|
|E2|a5b82f14f97a898d8c25951560f6e77d15aca295|docs/requirements/TRADING-2560_First_Layer_Composer_V2_Prospective_OOS_Observation_V1.md|
|E3|a5b82f14f97a898d8c25951560f6e77d15aca295|docs/requirements/TRADING-2564_Long_Term_Research_Capability_Improvement_V1.md|
|E4|a5b82f14f97a898d8c25951560f6e77d15aca295|docs/requirements/TRADING-2564_S4_Experiment_Envelope_First_Access_V1.md|
|E5|a5b82f14f97a898d8c25951560f6e77d15aca295|docs/requirements/TRADING-2562_Dual_Track_Strategy_Research_Continuation_V1.md|
|E6|e651a663f01a688f1e31ea76caa82cd3945b9228|docs/requirements/TRADING-2564_S5_Immediate_Failure_Diagnostics_V1.md|

|案例|冻结事实与候选|规则与输出|能识别的偏移/局限|
|---|---|---|---|
|H1 研究优先已有规定|E1 明确唯一 primary、P0 准入和 blocker 解除后的交还；其它优先级/umbrella 内连续建设的选择未被同样覆盖|按 V1 每次继续 P1/S 小阶段也要过 B/C；无具名实验依赖的完善不准入|确认旧机制有适用范围缺口；不把现有所有 P1 一概判为错误|
|H2 旧 producer 无法产生当前 session|E2 明确旧 producer 历史截止、prospective=0；候选是 versioned current-session producer 或调旧信号参数|R4：已有具名 observation 的可核对 blocker，选最小 current-session producer；冻结信号规则保留|这是必要工程，不能因“研究优先”跳过；本文只回放其已记载的诊断，不重跑 producer|
|H3 研究输入身份不一致|E3 同一 as-of，原 root DQ FAIL、运营 root PASS；不能直接替换 consumer；候选是 scoped connection 或复制 PASS 标签|R4：选具名 consumer 的受审输入连接；不改变原 FAIL，不重跑无关全系统数据修复|能区分真正取证依赖与虚假就绪；需独立检验 receipt 才能认定后续 READY|
|H4 S3b 完成后转 S5 seed/即时诊断|E3 第16节及后续段落直接将下一波转到 S5；E6 的收益是验证诊断，未提供阻塞具名 prospective 动作的必要依赖证明|若只有长期建设授权，不能自动获得下一主动作资格；按 R0/R4 核对研究交接，S5 留在待办。若另有 owner 明确即时排序指令，则据其记录另行裁决|判定为自主排序的漂移风险，不声称这两项工程无价值或未经授权|
|H5 synthetic first-access PASS 后|E4 明确只支持 SYNTHETIC_ENGINEERING_ONLY，真实 adapter/时间/DQ/PIT/历史暴露尚有缺口|不能命中 R3 宣称可真实取证；仅为当前具名动作证明所需接入后才命中 R4，不自动连接全部 legacy readers|同时防止过早取证与无限铺开工程；不能把这份文件当真实 OOS PASS|
|H6 双线中的自然等待|E5 允许 Composer 与保留 evidence 的正交筛选，既有 equal-risk forward-aging 不等于新 empirical run|R5：未有已授权成熟更新/独立取证时 WAIT；自然成熟不授权新下载或重跑|等待是合法输出，不用工程工作掩盖没有新证据|

这六行是来源受限的 qualitative replay，不是量化节约时间或防漂移成功率。没有读取市场缓存、
研究结果价格或 excluded owner document；也未重新验证所引历史研究指标。

## 当前 decision record：SDP-20260909-001

- selection basis：上述 frozen main；记录日期 2026-09-09 Asia/Tokyo。精确审计时间和来源摘要放在
  本任务 runtime evidence；这不是 prospective signal 的时间证据。
- owner priority source：E1 的 `SIGNAL_VALUE_FIRST_LAYER_COMPOSER_V2` 仍为已明确指定的 primary；
  E5 授权双线并未把其它策略设为新 primary。若存在本轮未见的后续 owner 改序，必须回到 R0。
- 本轮建设目标：判断这个裁决协议是否能给近期偏移提供一致且有边界的选择；文档验收后退出建设。
- 返回的研究阶段问题：冻结 Composer 在合法前瞻观察中能否取得可用的新证据，以继续评价其增量价值？
  历史 `INSUFFICIENT/HOLD` 及原有样本/停止条件保留；不新增投资阈值，不把一次 observation 当策略胜出。
- 本次交接子阶段退出条件：具名输入连接交付被核验后即结束“建设输入连接”阶段，回到原
  prospective preregistration 的下一合法动作；真实观察/成熟/研究 verdict 的验收仍由
  `config/research/first_layer_composer_v2_prospective_oos_preregistration_v1.yaml` 及其已审执行合同决定。
  本 pilot 既不为这些合同补造数值，也不把开发交付冒充已取得观察样本。
- 当前缺口：E3/E4 证明真实前瞻输入/协议仍未全部接通；工程结果不能供应真实 observation。
- 新鲜在途信息：2026-09-09 对 `D:/Work/AITradingSystem_devx014_source_preservation` 的只读审计看到
  branch=`codex/trading-2560-composer-known-snapshot-v1`、active lease=`lease-afbe2bfbc279cf44d588`，
  以及具名 snapshot/DQ/producer/capture 的未提交 paths。只据此认定 OWNED_IN_PROGRESS；
  不读取 dirty 实现作可信研究证据，不认定其已完成或已经获真实 capture 授权。
- 候选：A=重复实现同一 Composer 输入连接；B=等待既有 TRADING-2560 的具名依赖交付；
  C=扩展 S5 诊断/提效；D=把 S4 接入所有 legacy readers；E=直接真实 capture。
- 决策：`WAIT:OWNED_ACTION_IN_PROGRESS`，关联 primary task=`TRADING-2560`。
  原因：R4 的必要依赖已有 owner，按 C 避免重复；C/D 没有当前具名动作的必要边证明；E 未证实准入。
  这明确下一最重要的工作仍是既有 Composer 取证依赖，由现有执行者完成，而不是再建工程子系统。
- 恢复触发：TRADING-2560 提供 final published candidate、named source/DQ/consumer 范围与验收，
  或报告一个新的 typed blocker；在真实动作前独立核实授权/协议/时间/输入。随后重新 R0→R3/R4/R5。
  已具备合法取证条件就转具名取证；只缺 owner/时间则明确等待；有新必要 blocker 则重新证明边。
- 本轮没有调度/中断其它任务，没有预约自动观察，没有替其它执行者声明完成。

## 规则反例与观察验收

|反例输入|必须得到的结果|
|---|---|
|只把一项非必要工程从 P1 改成 P0|选择不变；priority 不能补出依赖证据|
|把 blocked/unknown DQ 改写成“应该差不多”|R0，不能转 R3|
|两个不同实验同层级且没有 owner 排序|OWNER_DECISION_REQUIRED:TIE，不能按 ID 排序|
|两个实现的时间与成本互有优劣|不能声称支配；无预定顺序时交 owner|
|必要工程验收完成，另发现一个不阻塞的小优化|返回原实验重判；新优化留待办|
|必要前置有 A→B→A 依赖环|R0/R6，不能递归制造任务|
|已有人在实现唯一 blocker|WAIT:OWNED_ACTION_IN_PROGRESS，不能创建重复分支|
|自然时间未成熟且没有合法独立取证|WAIT，不能默认选 S5|
|已满足 REJECT 的已审退出条件，但还能优化报告|R2 结束阶段，不能以工程延缓负面结论|
|后续 owner 明确指定一个有界紧急工程任务|按新的明确范围执行并记录对阶段的影响，不把“研究优先”凌驾于 owner|

试行建设验收与效果验收分别记录：本轮完成材料/一致性核查后为 BASELINE_DONE（mechanism），
真实防漂移效果为 NOT_OBSERVED；umbrella 保持 IN_PROGRESS，不宣称长期能力或研究任务 DONE。

## 工作区生命周期与阻塞记录

2026-09-09 主 checkout 只读审计 PASS，START preflight PASS；从 exact local-main 创建 task branch。
首次 publication acquire 在取得 lease 前被 `LEASE_ARBITER_MIGRATION_REQUIRED` 拒绝：主 checkout
保留旧 arbiter directory，新主线要求已审协议迁移。没有迁移、删锁、改旧 owner、更新 task event 或
调用旧实现。该迁移不属于本轮；不把它扩展为新的工程修复任务。

已升级的 `D:/Work/AITradingSystem_devx014_source_preservation` 正由 TRADING-2560 使用，
只读审计看到 active lease 及该任务的未提交实现，不能复用或干扰。其它已列工作区有既有任务/运行身份。

使用正常 task isolation，创建唯一临时 worktree：
`D:/Work/AITradingSystem/run/ccra/trading2564-stage-decision-pilot`，
branch=`codex/trading-2564-stage-decision-pilot`；用途为书面试行、既有生成验证和集成，不新建 lease
实现或替换任何旧 root。创建前本需求先在主 checkout 保存，创建后同字节转移并核对 SHA。

临时证据：该 worktree 的 `outputs/architecture/trading_2564_stage_decision_pilot` 与
`outputs/validation_runtime`。canonical 保留位置为主 checkout 的
`outputs/architecture/trading_2564_stage_decision_pilot`，只迁入本任务证据并逐文件核对 SHA。

退出条件：候选进入 validated local/remote main，唯一证据已保留且校验一致，无活动进程依赖，
tracked/untracked/ignored 审计无未保留内容；用 `git worktree remove` 删除该精确路径并 prune。
失败或在途时保留，明确下一责任方、风险与退出条件。原有其它工作区及已排除用户文件不读取或清理。

首次隔离环境的 editable install 仍导入主 checkout 的旧包，导致 v1 acquire/task metadata 使用错误
runtime；发现后停止，v1 用原执行 authority 仅作 FAILED release，lease 已释放。首个 task event
还误填未来 UTC 时间。两者均保留为事件证据，不作可信来源/时间或正式验证；不修改原事件。
具体见 canonical `outputs/architecture/trading_2564_stage_decision_pilot/environment_incident_v1.json`。
本轮没有任何研究动作或策略实现。为避免把旧格式迁移扩进本任务，不复用已受该事件影响的 runtime。

唯一实际执行 worktree 改为 `D:/Work/AITradingSystem/run/ccra/trading2564-stage-decision-reviewed`，
branch=`codex/trading-2564-stage-decision-reviewed`，仍从同一 frozen base 创建；每个 Python 命令显式
绑定该 worktree 的 `src` 并核实 loaded origin。这是错误执行环境的替换，不是随 main 漂移重建。
旧 pilot worktree 的16项唯一字节及失败事件已归档并逐项校验，派生 pycache 已绑定未改的 Git 源码，
无 Python 进程依赖后清理该精确 worktree 和未含独有 commit 的分支；约190,892,169 bytes
（不含 excluded path）已释放。恢复包为 canonical `failed_environment_attempt_v1.zip`，
审计为 `failed_environment_preservation_v1.json` / `failed_environment_cleanup_pre_v1.json`；
primary 用户文件未读取、散列或修改。
新工作区证据仍保留到上文唯一 canonical 目录；两个目录的清理 allowlist 均在此明确登记。

新工作区首次显式源码调用使用 PATH 的 Python 3.14，现有文件身份 guard 因同一文件 path.stat 与
os.fstat 的 st_ctime_ns 不一致而拒绝 acquire 收尾；原始 lease/event 保留。随后使用项目既有
`.venv/Scripts/python.exe`（Python 3.11.9，pyproject 的类型检查目标为3.11），同一源码、相同
guard、同一 transaction id 重新 acquire，绑定原 lease 后 PASS。没有忽略字段、迁移锁、删事件或
修改代码；后续一律显式 project venv + task src，并披露实际解释器。Python 3.14 的平台差异仅作为
本次环境限制保存，不扩大本轮为跨版本修复。正式验证必须在这个明确的项目运行环境完成。

## 进展与流图影响

- 正确环境的 canonical task update 与 LANE preflight PASS；本轮修改只限本需求、umbrella 入口和
  既有 task/generated authority。最初错误环境的 task event 不进入本候选，原始 bytes 保留在事件档案。
- 六个来源受限回放和十个边界反例已写入；当前选择为等待已有 TRADING-2560 具名依赖交付。
- 没有 CLI、配置、模块、cache/report schema、DQ/PIT、评分、回测或研究消费数据流变化，因此
  `docs/system_flow.md` 不修改；没有新增程序和实现镜像测试。验证使用既有文档/registry 检查及必需正式 tiers。
- 本次试行不会以工程测试 PASS 声称降低了漂移。最终 publication 状态以本任务正式回执为准。
- 文档与 canonical task-source 既有聚焦验证：16 workers/loadfile，14 passed；六项 Git 来源身份
  与文档结构核查 PASS，十个反例由 coordinator 逐条裁决，非自动策略效力测试。
- 正式验证准备：readiness 在 Full 前发现新 worktree 缺少已有 immutable baseline 和 Atlas 页面，
  以及需重建当前 compatibility authority。v1 在任何 generator/Full 前以行政 scope correction
  终止并释放，v2 加入 exact generated fragment 与历史证据目的路径；不改 readiness/断言。
- baseline 副本仅来自现有受控 worktree 的以下目录：
  `outputs/research/first_layer_composer_v2_foundational_falsification_v1`、
  `outputs/research/first_layer_composer_v2_foundational_falsification_failure_fix_v1`、
  `outputs/research/first_layer_composer_v2_matched_placebo_v1`、
  `outputs/research/first_layer_composer_v2_temporal_influence_v1`、
  `outputs/research/first_layer_composer_v2_temporal_influence_failure_fix_v1`、
  `outputs/validation_runtime/trading_2464_o1_dq_20260729T183000Z`、
  `outputs/research_trends/operational_forecast/trading_2542i_real_v3`、
  `outputs/qqq_options/signal_packages/trading_2542i_operational_forecast_real_v3`。
  缺失副本逐文件 SHA 核对、拒绝 reparse/覆盖；来源路径和摘要保留到 hydration receipt。它们只供
  offline regression，不是新 DQ/research run，不作为 SDP 案例的新经验结论；随临时 worktree 清理。
- 复用 umbrella 已记录的 source/final 两阶段：先在 source transaction 按官方生成顺序提交被验证的
  文档与 task sources；再以同一 workspace 的 source commit 作为 final transaction lane_head，
  官方 Atlas renderer 绑定该 exact commit。不存在 tracked 变化时不创建空提交；正式验证、主线
  发布和 cleanup 只在 final transaction 执行。source commit 本身不代表完整验收。
