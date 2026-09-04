# TRADING-2564：长期研究能力建设与运行前就绪核查

最后更新：2026-09-05

- stable task id：`TRADING-2564_LONG_TERM_RESEARCH_CAPABILITY_IMPROVEMENT_V1`
- priority：`P1`
- status：`IN_PROGRESS`
- mode：`SINGLE_LANE`
- owner：Project Owner / integration-coordinator
- owner instruction：2026-09-05「好的，你来推进下这几个方面的长期能力建设把」
- frozen local-main：`7ca36350e02cfa455bf16f33e34784697b4afb2a`
- production effect：`none`；broker action：`none`

## 1. 目标与范围

在既有 evidence-first portfolio 下，提高获得可信新证据的连续性和单位研究结论效率。
投资认知质量与扣成本、控制市场暴露后的增量配置价值分别评估；工程 PASS 不提升策略 verdict。
本任务为一个长期 umbrella，不因 contract、DQ、执行、展示或发布阶段自动生成 successor。
已有 DATA-GOV-002、TRADING-2560/2563、ARCH-004G2、DEVX-011/012 继续分别拥有原领域 authority。

本次授权覆盖本地能力开发、离线工程验证、任务/文档同步和受治理的普通 main 发布。
它不重复消费 TRADING-2563 的已终止一次性授权；不执行真实 DQ、manifest replay、研究 rehearsal、
observation/maturity/scoreboard 写入、数据下载、cache mutation、provider、QuantConnect、Options、
paper/live、broker、order、fill、position 或任何交易行为。上述本次实际动作计数均须为 0。
合成 fixtures 的软件测试与真实研究运行分别计数。

## 2. 已核实基线与主要阻塞

- Composer 2557/2558/2559 的历史证据维持 `INSUFFICIENT/HOLD`，不得后验参数救援。
- 2560 已有 single-session producer，但仅 `SAFE_PREVIEW_READY`，真实 prospective observation=0。
- Equal-risk 仅有 2026-06-22、2026-06-24 两个既有 observation；补成熟不等于恢复连续采集。
- 原开发目录 corrected DQ `as_of=2026-09-03 / manual.v1` 已完成一次且 FAIL：requested
  `2021-02-22..2026-09-03`，evaluated 至 `2026-07-23`；receipt id 为
  `dq_execution_d4229d2a50e008715a99b46e1f3d17077b1d6936e6bbb034130a0f43c48ffecb`。
  该事实晚于 2563 当前发布的「等待 retry 授权」记录；历史事件和 manifest 不改写。
- Permanent runtime 同 as-of 已保留 canonical DQ PASS 和 34-step daily PASS；DQ receipt 为
  `dq_execution_861f345ec637d87cb7eaedd69bacc6b0d8603899a6ebff123c53e25ebe9ad802`。
  价格至 09-03、rates 至 09-02；这不能直接替换原研究输入身份或自动授予 consumer cutover。
- 2563 首次 Full 的 25 个失败中，23 个为 retained evidence 缺失、2 个为 Atlas 数量/commit
  绑定；应在昂贵执行前发现，不降低 DQ、PIT、完整性或防篡改要求。

## 3. 分阶段建设、依赖与验收

|阶段|交付与既有任务映射|验收与停止条件|
|---|---|---|
|S1 当前波|只读 research input readiness；Full 前只读 evidence/generated readiness；对应 DATA-GOV-002、2560/2563、ARCH-004G2|显式 root/as-of/profile/receipt/dependency；可解释 blocker；不复制、不修复、不签发新 DQ/执行 authority；fixture 负例及正式验证通过|
|S2 输入连接|复用运营 immutable publication、canonical DQ 和 scoped consumer 合同，建立指定快照到研究消费者的受审连接|绑定 source/code/policy/calendar/window；源只读；未知 scope/缺失必需数据继续阻断；切换及真实运行范围明确后再执行|
|S3 前瞻连续采集|复用2560 producer/append-only ledger与既有equal-risk流程|真实冻结时刻与首个合法XNYS session明确；信号在决策前记录；旧缺口永久披露，不补成OOS；实际capture和成熟更新单独准入|
|S4 实验决策质量|复用2549 portfolio与既有trial/holdout合同；equal-risk静态暴露匹配及carry归因|事先冻结假设、基准、成本和停止条件；记录已看窗口/失败变体；首次查看对应prospective outcome前冻结sample/episode规则；本波不选择数字阈值或启动实验|
|S5 效率与原始产品目标|落实DEVX健康候选、ARCH-004G2实测提效；再选择一个狭窄AI产业链可证伪问题|用可信后续周报评价失败验证时间和研究周期；只复用已有遥测；产业假设先证明PIT来源和相对简单指标的增量信息，不直接增加模型复杂度|

排序遵循真实依赖与信息收益；整个 umbrella 不升级 P0。单个直接阻塞实验的最小步骤按已有研究
准入规则处理。S1完成不等于S2–S5完成，长期剩余工作保留在本任务，不自动标 DONE。

## 4. S1 实现合同

### 4.1 研究输入核查

新增薄诊断模块及 stdout-only script，复用公开 canonical receipt verifier 与 contained reader。
显式提供 execution/source root、as-of、profile、window、依赖和 receipt，不使用 latest、glob或root fallback。
区分 requested/evaluated/实际必需ticker覆盖；不得把合法rates滞后机械当作DQ失败，也不得用全文件
max date掩盖必需ticker缺失。已有PASS必须绑定真实字节和消费请求；诊断不会新运行validator。
禁止调用会重跑 full/scoped DQ 的 capability verifier 或会写账本的研究入口。
输出始终 `dispatch_allowed=false`、`dq_validation_executed=false`、`production_effect=none`；
就绪只代表所列只读检查完整，不能替代执行时 DQ/PIT、scope、授权或capture门禁。
合法rates滞后只按已验证canonical policy/receipt原样披露，不由diagnostic自行豁免。
本波不改变 `quality_execution.py` 或任何冻结策略源文件。

后续S3必须区分 `feature_session`、after-close `signal_recorded_at` 与 next-XNYS decision-effective
session；timezone-aware不可变activation evidence必须证明真实freeze时间及信号在决策前已经存在。
Expected sessions来自canonical XNYS，不从实际price行反推；缺口永久披露，但不永久阻断未来合法日期
的顺序恢复。S4的sample/episode裁决规则必须在首次查看对应prospective outcome前冻结，而不只是
在首次正式scoreboard前冻结。旧日期、hash格式、事后preview均不能证明真实OOS。

### 4.2 工程验证前置

复用既有 task/Atlas/report-flow/compatibility validators 检查最终候选与明确证据依赖；不新增锁、
发布阶段或scheduler。核查必须在 `FULL_DISPATCHED` 和 pytest 前完成，失败保持 typed BLOCKED，
不自动hydrate、render、修复或终结未消费事务。固定数量的独立测试断言仍由 focused tests覆盖，
不得声称仅canonical validator即可发现，也不得为消除失败删除 reviewed ratchet。

## 5. 验证计划

- 合成正/负例：路径越界、reparse、缺失和tamper、请求/profile/as-of错配、旧数据、重复依赖、
  真实PASS但错误输入、Atlas旧commit和缺失sidecar、未知检查器、零副作用。
- Full readiness失败必须在pytest/Full claim前停止；正常路径保持既有workers、command、provenance。
- 使用16-worker focused pytest；最终候选才运行所需formal tiers及Full。
- 登记新增task时同步 reviewed task count、Atlas分类及对应生成物；只在自然边界按官方顺序重建。
- 不将软件测试或只读诊断写成实证收益、prospective observation或运营部署验收。

## 6. 工作区与生命周期

复用已审计且clean的 `D:\Work\AITradingSystem_trading2559_integration`，不创建新worktree或cache。
任务分支 `codex/trading-2564-research-capability-v1` 从上述 exact local main创建。
现有runtime、原研究cache与其他worktree不修改/删除；known-unrelated owner文档不读取、hash或复制。
收口回到main；普通push需满足现有fence/ancestry/final-tree检查；任务分支仅在合并与唯一内容审计后清理。

## 7. 进度

- 2026-09-05：Owner接受整体review并授权长期能力建设。三路只读设计完成；已定位隐式DQ/账本写入
  入口，S1限定无真实业务副作用。开始登记及SINGLE_LANE预检。
- 2026-09-05：v1在Full前静态检查发现既有runner测试实际路径为
  `tests/test_validation_tier_script.py`，初始scope误写为不存在的runner测试路径。
  暂停worker写入并以行政scope纠正终止v1，新v2显式声明正确路径；不改写事务，不绕过测试，
  没有Full/研究动作被消费。v1注册事件与所有当前task-owned字节继续保留。
- 2026-09-05：S1聚焦验证：research input readiness 32 passed、validation readiness 21 passed、
  Full runner接线78 passed，均为16 workers/loadfile；新代码Ruff/Black/mypy通过。
  首次真实目录只读盘点在Full前发现5份留存输入缺失，并指出当前生成物/Atlas尚未重建。
  v2在生成前结束，v3补齐report/catalog/flow官方generator的transitive输出scope及以下5个精确
  evidence destination。此前scope只声明monolith和index而遗漏fragment root，未执行越界生成。

### S1 验证依赖补齐记录

这是工程验证环境准备，不是研究replay或cache更新。只从已经保留的
`D:\Work\AITradingSystem_trading2563_equal_risk_catchup`读取以下明确文件，先核对committed admission/
package receipt中的SHA，再在本任务checkout同相对路径中仅创建缺失副本；不覆盖未知字节、不改source。
前四项位于 `outputs/research/first_layer_composer_v2_foundational_falsification_failure_fix_v1/`：

|文件|已登记SHA-256|
|---|---|
|run_attempt_consumption_receipt.json|f76cbab177a409e5e0da9976602a7833bc2a62c0df862281028f4f8f8ff15902|
|manifest_replay_receipt.json|b517947725a569189e383d49a5ddc435d14778250783e3f9994199153500f851|
|canonical_dq_receipt.json|d6ef507b977827c924c91b43ff7addec4a20ecdcfa003729add8d05c2386011c|
|independent_replay_receipt.json|2bf69476ce96c6f23f46fc82ae0e35125fc6822b0e25e374747a50b1a0a5d35f|
|outputs/research_trends/operational_forecast/trading_2542i_real_v3/normalized_signal_source.json|4d26b56bcfc1b21764cb90373fb2da9134838e6c42709b80ba7cbbf0856703f1|

副本仍是过去运行的immutable evidence，所有业务动作计数保持0；readiness工具本身仍禁止copy/hydrate。
2026-09-05已完成上述5份missing-only复制，逐项destination SHA与已登记SHA相同；没有覆盖已有文件。
这些文件保留在已复用的canonical集成checkout供离线validation，源副本保留，未新增临时目录。
未来该checkout清理前按对应historical evidence owner/可移植归档门禁处理，不由本任务删除源证据。

### S1 独立审查与最终源身份

独立审查发现runner在foreign PYTHONPATH先于自身src时可能加载其他checkout模块，以及standalone
readiness可能用A checkout的validator检查B checkout。修复限定为自身src首位导入、实际loaded module
root与target root不等时fail closed，并增加合成负例；不支持未经审查的跨checkout代码等价推断。
修复后readiness与runner两测试文件合计101 passed（16 workers/loadfile，39.43秒），Ruff及
readiness source/script strict mypy PASS；先前data readiness的32项测试不受该导入修正影响。

Atlas官方renderer要求所有task/policy/requirement sources先存在于exact commit；publication fence又要求
generator PRE/POST阶段HEAD保持lane_head。因此本波采用既有两阶段source/final模式，而不修改任一
门禁：v3在generator与Full前结束；v4仅声明canonical-task-source→architecture-manifests→
report-flow-authority→compatibility-authority形成已聚焦验证的source commit，随后结束该source事务。
v5以该source commit为lane_head，保持原frozen main，按完整五项顺序重建（含Atlas），验证清洁exact
candidate后才启动formal tiers/Full并发布。source提交不等于验收，不进入main；无空提交、历史改写、
旧SHA页面冒充或额外研究授权消费。行政事务的终止不伪装成已完成的Full。

本阶段任务保持IN_PROGRESS：S1代码与聚焦验证完成，final publication receipt才是正式工程验收证据；
S2-S5继续保留为后续依赖，既有观测数量与研究结论不变。S5需评估source/final两阶段的实际成本，
如需统一generator与exact-commit顺序，必须独立review最小workflow-contract变更，不在本波绕过。

Source集成聚焦回归首次为43 passed / 4 failed（16 workers，93.02秒）：4项均为当前report-flow
source identity断言仍保留修改前值。官方generator已证明catalog由560增为563 blocks、flow由1214增为
1216 blocks，总entry_count=3152，shadow render与source逐字节相同；据此更新当前hash/count断言，
保留历史DEVX-006D的3000与frozen legacy 306项及其hash不变。失败未串行重跑、未跳过、未消费Full。
修正后的18项report-flow/current-authority聚焦回归全部通过（16 workers，28.87秒）。

新增四个source/script的项目strict mypy PASS。将既有runner纳入额外显式strict检查时有56条诊断；
独立review用Git读取frozen base和当前源码到内存，以同一pyproject与当前src解析执行mypy：两者均
56条，message/errorcode多重集完全相同，新增0、消除0，其他模块错误0。未隐藏或忽略这些历史诊断；
S5保留runner类型债审查，具体修复另定窄范围，不将当前runner整体宣称mypy PASS。

只读诊断还必须在任何canonical checker之前拒绝非40hex candidate及未提交的实际核查代码。
代码检查仅使用src和两个validation脚本的显式literal allowlist，禁用Git外部diff/textconv；不扫描
owner文档或市场cache。正式runner的clean-candidate gate继续独立保留，standalone不能借同root旧HEAD
误报新代码为已提交候选，也不能把非法candidate字符串传作Git选项。
最终source聚焦验证三文件合计157 passed（16 workers/loadfile，43.16秒），覆盖data readiness、
Full readiness及runner接线；四个新增source/script strict mypy PASS，八个实现/测试文件Ruff与
Black check PASS。v4 LANE preflight PASS，HEAD/main/origin缓存ref仍为frozen 7ca36350，无越界dirty。
此处不预报正式Full成功；后续正式tier与publication事件绑定最终exact候选，失败须保留并关闭。

## 8. S2 下一波的只读实现定位（未启动迁移）

- 复用 `data/download_publication.py` 的完整publication验证链，补显式named snapshot/transaction
  解析；现有resolver只解析current，研究端不得随current前进自动换输入，也不得自行解析transaction JSON。
- 在既有consumer authorization合同中审查immutable-direct与source/执行根分离；同root legacy projection
  授权不能直接移借到另一runtime根。此共享合同需要最小serial review，DATA-GOV-002 Phase D准入仍独立。
- 优先以薄adapter接2560的 `build_current_session_preview()` DataFrame纯计算边界，不改冻结producer。
  输入闭包按既有operational forecast policy：QQQ/TQQQ/SHY/SGOV及DGS10/DGS2/DTWEXBGS；保留其已审
  2018-01-02训练初始化角色，不将2021-02-22以前结果纳入primary绩效结论，不能把依赖简化成QQQ/SGOV。
- `verify_consumer_data_capability_preflight()`会实际运行full/scoped DQ，不属于只读连接；未来真实执行需
  单独scope及计数。先用synthetic equivalence证明相同已验证bytes产生相同preview，缺字段/训练历史、
  future rows、code/policy/calendar/window错绑均阻断。
- Equal-risk先产只读execution plan，使用显式observation ID allowlist；原2563 manifest、固定source/code/input
  identity和已终止授权不改写。现有maturity入口会DQ、glob与写结果，不能直接当只读connector。
- Owner后续需决定明确source snapshot/root、consumer/version、是否准入scoped migration与真实动作上限。
  S2设计和synthetic PASS不签发这些权限；S3仍要求真实activation与决策前recorded time，旧缺口不补成OOS。

本定位仅阅读源码/政策，不证明现存runtime快照满足上述完整依赖；所有真实连接、DQ、capture、maturity、
scoreboard、下载及交易计数继续为0。下一责任方为本任务coordinator与DATA-GOV-002合同owner。
