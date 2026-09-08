# TRADING-2564：长期研究能力建设与运行前就绪核查

最后更新：2026-09-07

- stable task id：`TRADING-2564_LONG_TERM_RESEARCH_CAPABILITY_IMPROVEMENT_V1`
- priority：`P1`
- status：`IN_PROGRESS`
- mode：`SINGLE_LANE`
- owner：Project Owner / integration-coordinator
- owner instruction：2026-09-05「好的，你来推进下这几个方面的长期能力建设把」
- S1 frozen local-main：`7ca36350e02cfa455bf16f33e34784697b4afb2a`
- S2a frozen local-main：`2124aff36802e0a85e0566c2306365659e8ee4d1`
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

## 9. S1 正式交付与 S2a 最小串行合同波

### 9.1 已完成的 S1 发布

S1 final candidate `2124aff36802e0a85e0566c2306365659e8ee4d1` 已完成 architecture-fitness
888 passed、contract-validation 281 passed、integration 995 passed、reproducibility 24 passed，
Full 10324 passed / 5 skipped（16 workers/loadfile，2649.06秒）。Full仅实际dispatch一次，证据为
`outputs/validation_runtime/full_20260905T003206Z/test_runtime_summary.json`，SHA-256
`38d1690f1f9d54c97a3d8b8492857e877ea5378a228e2dbebc2cd6e452397e5b`。

Owner随后明确批准普通push；local main与实际remote main均为上述candidate。v6事务已RELEASED，
`outputs/architecture/arch_005_integration_publication_fence/transactions/trading-2564-research-capability-20260905-v6/closeout_receipt.json`
SHA-256为`a43fa52223ddbdb55d20d9e65c03471d6d302455b5e4824aa7516f91e3187f49`。
已合并S1本地分支删除，可由main恢复；集成checkout保留canonical evidence，无文件或工作区删除。
此前记录为历史过程，以上receipt补充最终验收，不改写既有失败事务或研究结论。

### 9.2 S2a 合同、职责与非目标

Owner继续推进长期建设；本波选`SINGLE_LANE --contract-change`，从S1 exact main冻结，先完成共享
输入解析合同，后续消费者不得从尚未完成的合同分支启动。两路独立静态审查与coordinator复核后，
选择以下最小兼容设计；不新增发布格式、DQ例外、consumer权限或临时绕行路径。

1. `ValidatedSnapshot`保存原有snapshot身份字段；`ValidatedCurrentSnapshot`与新增
   `ValidatedNamedSnapshot`为同级类型，历史快照不得以Current类型冒充。Named新增
   `SnapshotCommitAnchor`，记录dataset/pointer id、SHA、path及generation。
2. `validate_named_snapshot(store_root, dataset_id, pointer_id, expected_pointer_sha256)`只接受
   显式合法标识和精确SHA，按确定的history路径寻址，复用contained reader、pointer/history/reference
   全链验证。必须证明所选pointer是已验证current commit anchor的自身或祖先；current仅作已提交
   成员资格证明，绝不选择或替换输入。缺失current、目标不在链中、任一tamper均fail closed。
   该边界不证明对同一可信writer的恶意回滚抵抗，也不新增独立外部commit witness。
3. 新增`resolve_named_download_publication(output_dir, pointer_id, expected_pointer_sha256,
   expected_transaction_id, expected_transaction_sha256)`；transaction只能从上述已验证snapshot
   payload取得，继续复用outer pointer/manifest、transaction与全部member/source/window核验。
   不从transaction id、glob、latest或任意JSON反向寻址。
4. Download结果使用独立`ValidatedNamedDownloadPublication` wrapper，保留现有Current API与
   `ValidatedDownloadPublication`构造兼容。wrapper包含`publication`与`snapshot`，显式
   `validation_scope=STRUCTURAL_PUBLICATION_ONLY`、`legacy_projection_status=NOT_EVALUATED`、
   `consumer_cutover_allowed=false`、`dispatch_allowed=false`、`production_effect=none`。
   内层`legacy_projection_verified=false`仅是结构核验的未检查值，不表示legacy核验失败；新报告/
   adapter只能按外层NOT_EVALUATED解释。wrapper拒绝身份不一致或权限升级值。Named路径不读取
   mutable legacy projections；current前进、legacy变更或消失均不得换掉显式输入。
5. 本波只改两个publication模块及合成测试；不实现真实preview、maturity、收益计算或等价授权。
   Root拥有任务/文档/生成物/正式验证/发布；source与test worker仅在同一冻结base、已登记scope和
   活动事务下独立实现。工程fixture发布不计为真实publication、DQ或研究执行。

### 9.3 必须证明的边界与后续依赖

- Publisher先安装history、后执行pre-commit validator、最后提交current。完整但未提交的orphan
  也可能通过单独hash检查；必须用合成拒绝提交实例证明其不能被Named接纳。另覆盖当前/祖先正例、
  缺失目标不fallback、hash/ID错绑、链断裂/跳号/前驱SHA/cycle、member/source/manifest改动、
  traversal/reparse/hardlink与校验中替换等负例，复用现有文件系统安全测试而不降低门禁。
- Canonical DQ receipt绑定实际input path和manifest output_path。同SHA immutable member不是原路径
  的DQ receipt；跨root也不能借同root legacy consumer grant。`immutable_publish.py`本身在canonical
  validator源码哈希闭包内，新增函数会改变validator identity，旧receipt在新代码上可能返回
  `DQ_VALIDATOR_SHA_MISMATCH`。历史receipt与code原样保留，不改receipt、不豁免源码/路径身份。
  独立审查另确认`foundation_consumer_migration.py`的migration validator源码闭包也包含该文件，
  更新后的checkout重验旧migration capability会因`CONSUMER_MIGRATION_VALIDATOR_SOURCE_DRIFT`
  正确阻断；本波不重新签发capability或把旧部署自动升级到新代码。
  后续DQ消费合同迁移和真实DQ仍由DATA-GOV-002审查及单独范围准入。
- 冻结2560 producer要求prices和rates各自max_date等于feature_session；运营rates落后一天虽可能
  满足其DQ政策，仍不能直接满足producer。未来adapter必须阻断，不能补造rates或偷偷改producer。
- Equal-risk只读plan必须限定exact observation file ID/path/SHA及逐策略row复合身份，并显式
  as-of/input cutoff和canonical XNYS horizon。既有maturity入口会DQ、glob与写文件，且as-of未对
  downstream prices裁切，不能当只读helper。不得复用2563已终止额度或把历史gap补成OOS。
- S2后续adapter/合成等价/只读plan、真实snapshot/root/consumer选择及S3-S5仍未完成；S2a结构PASS
  不提升SAFE_PREVIEW_READY、研究verdict或任何执行权限。

### 9.4 验证与生命周期

复用`D:\Work\AITradingSystem_trading2559_integration`，不建新worktree/cache；S2a任务分支为
`codex/trading-2564-s2-named-snapshot-v1`，frozen base为上述2124aff。source/final事务分阶段遵守
既有Atlas exact-commit与fence顺序，不在本波修改治理合同。source事务
`trading-2564-s2a-source-20260905-v1`已获取唯一lease；首次acquire参数重复声明内建validation路径
被校验拒绝，未获取lease/写事务；按内建路径去重后范围相同，未绕过scope。

先运行16-worker focused tests与适用静态检查，最终单一candidate才执行formal tiers/Full与普通发布。
保留真实readiness/evidence缺失的typed blocker；不为测试自动补市场数据。所有真实DQ、replay、研究、
capture、maturity、scoreboard、下载、cache mutation、provider及交易动作均为0。
任务分支仅在main集成/普通push/SHA一致及唯一内容审计后删除；集成checkout的历史证据继续按S1
生命周期保留。未完成步骤保留在本umbrella；正式结果和清理以本波publication receipt为准。

### 9.5 S2a 实现与独立复核记录

两个只读resolver及独立Named类型已实现。初轮独立复核发现返回DTO可构造同代不一致anchor，
以及测试尚未覆盖transaction验证完成后的末端替换；已补纯内存anchor/Named身份、同store确定路径、
同代pointer ID/SHA与envelope一致性检查，并补8项DTO负例、4项末端竞态负例。DTO不是不可伪造
capability，只有resolver证明其输入字节与已观察提交链一致；不增加外部commit witness或权限。

第二次独立静态复核确认上述两项已解决，没有发现新的阻断问题。新增synthetic DQ用例只修改tmp
fixture内immutable源码，验证旧receipt收到`DQ_VALIDATOR_SHA_MISMATCH`，receipt bytes不变，
verifier不重新运行DQ。所有fixture与真实市场cache隔离，不据此声称旧migration capability已完成实测。

初轮三文件聚焦回归166 passed / 1平台条件skip（63.96秒）；现有DQ/consumer/current/readiness等
150项合成回归PASS（159.12秒）。这两批发生在最终DTO补强之前，保留为阶段证据，不冒称最终验收。
补强后的两source Ruff、Black及strict mypy PASS；最终源码SHA-256分别为：
`immutable_publish.py=e9c97a9863d6a74bee908d8ea56d930e86c9f25fe7e800b161b00e85e14d757f`、
`download_publication.py=d0044ec7aa59c6b75325ffc25808e9ec975b3ed5adfc97d5e0405d8ecc41fb64`。
最终三文件focused为179 passed / 1 skipped（16 workers/loadfile，42.67秒）；唯一skip是Windows上
既有`test_posix_in_root_symlink_alias_is_rejected`的POSIX语义用例，新增Windows reparse用例通过。
新测试文件SHA-256为`7f8d5b3a565996119ae265cfe552d184e006c17d527df9e6d4795746a7f8f23b`，
Ruff/Black PASS，source SHA与上述冻结一致。source/final正式验证继续执行；本任务和S2整体均未完成。

### 9.6 S2a 首次正式失败与窄修复

候选`d1d1b0e85bb7f0109c8311bce68d8dbb0bdc8040`的readiness为7 checks PASS、1242项依赖；
随后architecture-fitness为776 passed / 112 failed（16 workers/loadfile，517.11秒）。流水线立即
停止，contract/integration/reproducibility/Full均未启动。失败收据
`outputs/validation_runtime/architecture-fitness_20260905T061221Z/test_runtime_summary.json`
SHA-256为`636e444b77892fcb48709b234eeb7d32c3847dda56ea4ca9063058da737d3a73`。
原final v1事务已通过publication命令终止为FAILED并释放lease，原始日志和收据原样保留。

独立只读诊断确认112项均来自同一个缺口：两个publication模块已变更，但compatibility generator
尚无TRADING-2564当前源码继任authority。修复新增本任务独立的S2a fragment、精确source集合和
historical-test adapter接线，不将新路径塞入OPS-078历史职责，不改legacy prefix的306项与seal，
不删除失败断言或改策略/DQ规则。现有官方builder继续负责当前fragment/index，新增负例证明任意
未登记或被篡改的source仍被拒绝。业务flow与Named接口保持9.2定义。

修复source v1事务沿用原范围，诊断发现还需声明compatibility generator路径，故在任何tracked写入前
行政终止；v2显式加入`src/ai_trading_system/platform/architecture/compatibility_authority.py`后
追加任务纠正事件并重做SINGLE_LANE预检。保持原branch、checkout及frozen main，不建替代worktree。
架构失败不是failed Full，不伪造Full parent；修正后先做受影响聚焦回归，再冻结source/final候选，
正式验证与普通发布收据才构成验收。全部真实研究、DQ、数据与交易动作仍为0。

修复初次两文件focused为229 passed / 2 failed（16 workers/loadfile，532.36秒），原XML保留在
`outputs/validation_runtime/trading-2564-s2a-authority-fix-focused.xml`。此前112项历史追溯失败均已
消除；本次两项仅为新增篡改负例期望错误：source变更使content-addressed fragment路径变化，只读
builder不创建新文件，现有validator先返回`AUTHORITY_FILE_MISSING`，尚未到hash比较的
`AUTHORITY_GENERATED_STALE`。仅修正精确错误码断言，validator不改、不改为宽泛异常；随后仅重跑
该受影响测试文件，最终formal仍覆盖完整候选。generator源码项目strict mypy及三文件Ruff/Black PASS。
GEN_PRE首次漏显式generator顺序参数被阻断，补齐原声明后通过；重复refresh-consumers在GEN_PRE被
阶段门禁拒绝。任务更新已在TASK_SOURCE_PRE_WRITE完成，其消费者库存随后只读validate PASS，故不
绕过阶段、不重复任务写入，继续剩余官方generator。上述检查错误没有触发研究或Full。

### 9.7 后续合同审查发现（设计待审，未实施）

S2b最小方向是独立named-immutable DQ request/receipt/verifier分支，保留legacy v1原义，不全局
重解释`project_root`。须分别绑定只读source root、显式publication目录、execution code/policy root、
evidence output root；member只由已验证transaction导出。原manifest output_path与完整行hash、
transaction role/member、实际捕获bytes必须三段一致，不重写原manifest、不读取legacy projection
来冒充immutable输入。来源根不明确的relocation应阻断，支持范围需DATA-GOV-002合同owner审查。
复用canonical DQ规则及同一`DataFileSnapshot.content`；新verifier不重跑DQ，返回已核验的只读bytes
供后续adapter解析，避免PASS后再按路径读取换输入。新receipt不能改标签重用旧v1，不自动授予consumer。
真实snapshot/root、as-of、consumer/profile及动作上限仍需明确准入，机器hash由coordinator提取。

冻结producer的rates末日门禁是整个DataFrame index，并非每个必需series都有当日新值；已有内部
`ffill`也不证明发布可得性。未来adapter须逐series披露最后有效观察日、carry来源和真实available-at，
不得人为增加空末日行。S3还缺timezone-aware activation、输入cutoff、实际收盘、signal_recorded_at
及decision deadline证据；现有日期/六hash不能证明PIT/OOS。默认保留producer阻断；若连续采集需要
“决策截止已知rates”或更晚决策时点，先审查新信息集/时点合同，不能作为薄adapter暗改。S2b-S5继续
保留在本任务；本节为只读设计发现，不证明真实输入ready，不启用任何新DQ或研究运行。

S5新增实测候选：本波readiness自洽PASS仍漏掉新supersession覆盖，代价为112次重复失败、517.11秒、
461616字节日志。拟在首个昂贵formal tier前增加只读live-source覆盖预检，绑定exact base/candidate和
独立于generator的reviewed变更scope，复用canonical loader及LF hash规则；按路径归并missing/stale/
wrong-lineage，不能自动生成或批准authority。须最小shared-contract审查，不复制巨型历史测试helper，
不替代历史回归。负例须覆盖generator与生成物共同漏项、只覆盖一个source、旧SHA、缺supersession、
错误继承/较早section、非法base/candidate、excluded/越界读取及零dispatch；未变源码与纯EOL差异
不得误报。先测真实提前发现时间和日志量，不引入新性能阈值。此项仍为待审设计，当前波不实施。

### 9.8 S2a 首次 Full 失败与末尾断言修复

候选 `e1aa1c0cfc87d879257ff1ed5e0367e7587d4fc0` 的原 final v1 在 architecture 输出约80%后
进程中断；核对 handle 不存在且实际验证进程已消失，未生成该轮正式结果，Full 未派发。中断原因
未知，不把部分进度记为 PASS。旧事务和中断证据已封存；同候选 final v2 恢复后，readiness 7 checks /
1242 dependencies PASS，architecture 889 passed、contract 281 passed、integration 995 passed /
639 warnings、reproducibility 24 passed。没有改候选或创建新 worktree。

随后首次实际 Full 为 `1 failed / 10397 passed / 5 skipped / 640 warnings`，pytest 2698.48秒，
runner 2699.38秒；收据 `outputs/validation_runtime/full_20260905T075500Z/test_runtime_summary.json`
SHA-256=`e6132fd0173b1a582812cfacf2017a94bd92fba6fa7bf46b89db5d52b63df0c5`，profile SHA-256=
`4fc25fbb29613b00d79e8564d3e08d85a9a085264640908726ef9275a2f59edb`。唯一失败为
`test_devx_006d_report_catalog_flow_authority.py::test_compatibility_authority_carries_the_inactive_shadow_contract`：
第153行仍把 OPS-078 断言为最新末尾，实际已追加本任务 S2a。final v2 已以真实 Full FAIL 终止并释放
lease，main/origin 未前移；失败与此前“未派发 Full”的行政中断分别保留，不相互冒充。

本次窄修复按以下顺序执行：

1. 沿原 branch/checkout 和 frozen main `2124aff`，获取
   `trading-2564-s2a-tail-fix-source-20260905-v1`，绑定上述真实 failed Full，先追加 canonical 纠正事件。
2. 独立只读检索 tests/src/scripts 的 OPS-078 literal/常量、next(reversed)、末项与 latest/order 组合；
   未发现第二处同类过期末尾期望。修正唯一 exact-tail 为 S2a，并保留 OPS-078 前序关系及全部历史
   3000、successor 3152、192、LEGACY_MONOLITH、inactive 和 cutover=false 断言，不删除门禁。
3. 将本次修改的 DEVX-006D 测试显式纳入 S2a 精确 source/superseded scope，由9路径变10；对应独立
   exact-source 集断言同步，官方 generator 刷新当前 fragment/index。历史 legacy seal 和保留链不改。
4. focused 使用16 workers/loadfile，覆盖 DEVX-006D、DEVX-006C 全文件及 OPS-078/S2a 架构边界。
   复核静态检查、归属与生成物后形成 source/final 候选；最终正式重跑使用 failure_fix_rerun 并绑定
   上述 failed Full parent，不沿用旧候选的正式 PASS。全部通过后才普通 main 发布。

本修复不改两个 publication resolver、DQ/策略/阈值、页面研究 verdict 或数据流，故 system_flow
保持原样；真实 DQ/replay/研究/capture/maturity/scoreboard/下载/cache/provider/交易仍全部0。
后续 S2b-S5 未完成，不因本次测试修复或 source commit 标记完成。

### 9.9 等待正式验收期间的后续合同补充（待审、未实施）

两路独立静态审查与 coordinator 复核保存在
`outputs/validation_runtime/trading-2564-s2b-s4-design-review-20260905.md`，SHA-256=
`d5d9b68889e31c34b7effa1150b0c56209d0c64829dcbc112c7e323a7b0f4995`。后续仍由本 umbrella 和
DATA-GOV-002 / DEVX-009 等既有领域 owner 承接；这里登记具体依赖，不启动新执行授权：

- S2b：canonical DQ 的数值规则可复用，但 quality.py 的 publication 路径校验，以及 runner/verifier
  两处 manifest matcher 均仍按 legacy 物理路径绑定，必须新增同一套严格 named-row 身份分支。
  不得伪写 snapshot.path 或退入 checksum-only legacy 分支。保持真实 member path，输出前逐项核对
  summary/typed-attribution role/path/SHA；primary 与 rates 六个已批准 site 的 COMPLETE 不得退化为
  UNKNOWN。独立 request/receipt/verified bytes 类型与纯 provenance helper 需避免 quality/execution
  导入环，最小抽取范围另审；新增依赖必须进入真实 validator source closure，旧凭证正常 source drift。
  execution root 必须匹配实际 loaded code/policy/calendar；旧 tmp-root fixture 和 fake report spy
  不能证明这一点，须真实 canonical synthetic 正例。真实 source、consumer adoption 和动作上限另审。
- S3：现有下载/manifest 日期及 hash 不证明 provider available_at；capture 声明时刻也不等于完成。
  未来只能以真实事件绑定 freeze、输入已知时刻、signal持久化和 decision deadline；历史空白不补成
  OOS。决策时点、rates信息集与合法顺序恢复由受审新合同定义，不改冻结producer来消除滞后阻断。
- S4：ResearchPreregistration/SelectionDataBinding 可复用，indicator trial service 目前仅声明 payload、
  trial_count=0，结果后 candidate ledger 也不能当首看前登记。需薄的 exact envelope、append-only
  trial/attempt/访问事件及所有 outcome 释放前门禁；失败/部分暴露/未知历史不可通过改名清零。
  崩溃后不能证明未释放时保留可能已暴露。2560旧“首次formal scoreboard前”措辞原样保留，新 envelope
  落实本任务更严格的“首次查看 outcome 前”；sample/episode、停止条件与时间 witness 仍须审查。
- Equal-risk归因：旧 `_attribution_row` 的 SGOV 项是平均权重乘策略年收益，并非现金真实贡献；另一
  日度贡献接口用算术年化，不能直接与 CAGR 分量相加。未来先冻结可对账的逐日净差/成本及复利 linking。
  全样本暴露匹配只可作为预声明事后 comparator；现有 QQQ/SGOV comparator 仍为 DRAFT_FOR_OWNER_REVIEW，
  不自动借用其数字或替换 Composer benchmark。归因方法、sample/episode及真实访问仍由Owner审查。
- S5：scope 的编辑权限不等于 supersession 审批；轻量覆盖预检需独立 reviewed source scope，不能
  自举批准。tracked manifest 不嵌入包含自身的最终 commit SHA，应由现有 runtime transaction 绑定。
  source/final 顺序冲突的长期候选是同一 fence/lease 下区分提交前 tracked generator 与提交后
  exact-commit runtime generator，增加明确 candidate/tree seal 和版本化阶段，不放宽 HEAD 检查。
  现行 transaction 无 policy path，Full前后/task-source/preflight默认构造 fence；新版本还必须统一
  transaction→exact policy/handler 选路，历史 v1 按准确 version/SHA 与保留 bytes 重放，缺失时阻断，
  不覆盖旧 policy、不用 latest/default。此 shared workflow 波只能在当前事务发布后另行登记实施，
  同步 skill/preflight/runner/CLI/system flow 与历史兼容反例。

## 10. S2a 正式发布与 S2b 接续

S2a exact candidate `293813e5e2e7b88886b79fc22cf77e2d57f1f346` 已完成 readiness、architecture
889 passed、contract 281 passed、integration 995 passed、reproducibility 24 passed，Full
10400 passed / 5 skipped / 640 warnings（16 workers/loadfile，2576.43秒）。实际 Full 为
`outputs/validation_runtime/full_20260905T092755Z/test_runtime_summary.json`，SHA256
`1e56b63a250edb406b5edf5fc6284304a885583ea75d8105b54ecc2b238047f8`。不是重贴旧候选的 PASS。

首次普通推送申请被自动权限检查拒绝，没有发生外发；Owner随后针对该 exact candidate、指定
GitHub 仓库 `AI-Trading-Collaboration/AITradingSystem` 的 main 及代码/文档载荷明确回复「批准」。
本次 publication authorization_state=`EXACT_PREAUTHORIZED`，technical_validation_state=`PASS`；
不将该批准解释为真实研究或交易授权。普通 non-force push 已成功，实际 remote/main/local main
与 candidate 相等。原 source/final失败、中断、待授权和验证历史记录均保留，不改写历史。

最终收据为
`outputs/architecture/arch_005_integration_publication_fence/transactions/trading-2564-s2a-tail-fix-final-20260905-v1/closeout_receipt.json`，
SHA256=`888dbe53f9050d2a8c2486407f6262109761d496ccc01e830193e84e257483f5`，
`PASS/COMPLETED/RELEASED`，原 lease 已释放。已合入 S2a branch 删除，全部提交保留在 main 可恢复；
集成 checkout 和全部历史/合成验证证据继续按 §6/§9.4 保留，没有文件、目录或 cache 删除。

S2b 从上述 exact main 开始，复用同一 checkout，任务分支
`codex/trading-2564-s2b-named-dq-v1`。START 预检确认 clean、main/origin 一致、无活动旧 lease；
新 source transaction `trading-2564-s2b-named-dq-source-20260905-v1` 采用既有 SINGLE_LANE
串行合同波，scope 和正式验证仍由 coordinator 负责；不修改 publication fence 工作流。
详细步骤、Git-bytes/进程内验证对象边界、测试分层和后续消费依赖见
[S2b 指定快照 DQ 执行合同](TRADING-2564_S2b_Named_DQ_Execution_Contract_V1.md)。
S2a 的结构验收不替代 S2b 的 DQ 身份验收，也不等于 S2、S3–S5 或总任务完成。

### 10.1 S2b 源冻结前进度

同日统一focused382 passed/1既有warning；核心源码static PASS，实际candidate E2E/Full尚待。
父终态proof要求正常结束与postguard通过，failed/partial/unknown不能由残留PASS receipt解锁。
actual-candidate测试固定要求真实正式事务，不提供dirty或仅提交源的降级路径；父控制流失败与reader
补测92 passed，实际E2E尚未执行。LANE/静态检查通过，进入原定source/final工程验收顺序。
本波仅更新当前system_flow/artifact_catalog内容及相应RCF live seal，最终29路径S2b source closure
包含policy/catalog与当前deprecation计数测试；历史authority与研究receipt保留。详见S2b合同§11；
S2后续adapter与S3-S5仍未完成，
全部真实DQ/研究/下载/cache/provider/交易动作保持0，合成工程测试单列。

### 10.2 S2b 首次实际候选的进程身份阻断

源码与生成focused修正后80/80 PASS，形成source/candidate5615e27c454cbd872553b04a809b4f6edd676f6c；
最终五项生成、clean audit和只读readiness七项/1242依赖PASS。首次真实candidate synthetic E2E
8failed/4passed，原因是Windows venv启动器PID与执行解释器PID不同；严格父关联正确阻断，未放宽。
实际11个child=8synthetic DQ+3pre-DQ阻断，verify0；未派发任何正式tier/Full，main/origin仍293813。

旧final FAILED/RELEASED及原XML/11个父收据保持不可变，纯synthetic证据33根345文件909318字节已
逐文件SHA相等归档。以同范围新source事务修正测试启动映射，采用CPython官方direct-venv方式，
保持PID/lease/-I/-B与原venv依赖，不改生产DTO/55模块源身份/DQ/策略规则。详细根因、官方依据、
失败SHA、归档生命周期及验收见S2b合同§12；canonical追加纠正事件cycle457，LANE PASS。
后续仍需新精确candidate完整E2E和正式发布；没有failed Full parent，不将本轮失败冒充Full失败。
S2b、S2后续及S3-S5继续未完成，所有真实数据/研究/外部/交易动作保持0。

PID窄修复三文件Black/Ruff PASS；106项聚焦回归在保留100pass/6个fixture准备顺序失败后，
修正同fixture期待值捕获时机并重跑为106/106 PASS（54.88秒）。标准库PID/venv/-I/-B已验证，
生产路径字节未变；仍须修复后精确candidate的12项E2E及全部正式验收，不能借旧readiness提前发布。

### 10.3 S2b 首次 Full：派生一致性失败与窄修复

candidate06140c52ca4e5be718075b7f436b820b927637c4的12项actual E2E和四正式tiers全部通过，
首次Full为10654 passed/11 failed/5 skipped/641 warnings，3893.89秒；summary SHA256
ccef4a6bd377e560c62803db953f530bf45a148f6c30d255572d26d887454e04。Full内12项named E2E全部PASS，
但不能覆盖兼容性helper2项、C1清单2项及其rate review pack级联7项失败。原final FAILED/RELEASED，
main/origin仍293813，未发布。

按既有DATA-GOV-002/TRADING2542I受审等价派生重绑定路径修复：原69site语义不变，新3项仍未批准；
六rate候选、emitter AST、原Owner决定/条件、阈值与scope全部不变，官方重建C1→rate derived pack，
只同步derived ID/SHA绑定，补精确successor与负例。新source事务trading-2564-s2b-derived-fix-source-20260906-v1
绑定本失败Full，完整source closure29→40，先等价性和focused后新candidate验收。详情见S2b合同§13；
任何语义差异需source-owner/serial contract review，不能自动重批或扩大隔离权限。

S5明确增加两项未完成能力：关联验证必须覆盖C1→review pack→decision和独立successor测试；
合成证据需区分临时测试与canonical replay并在运行期间保留必要bytes。本Full结束后pytest-18406目录已
不存在，归档前检阻断且零复制/删除，不能重造原件；正式报告与16个父收据仍保留。对应next owner为
integration-coordinator，退出条件是依赖漏检回归与证据保留策略验收通过，不在本窄修复修改Full runner。
S2b、adapter及S3-S5均未完成，真实DQ/研究/下载/cache/provider/交易继续0。

### 10.4 保全后的S2b恢复与研究主线接续（2026-09-06）

DEVX014已正式发布`64e1d3da3cd41ae842fd7cc3f14802c550ceb9ef`；旧S2b工作区单次迁移1/1、原始源码保全1、
独立校验1完成，snapshot=`d5e8b26f2a4ac3c66ccb941b77e6f7c983423786`。它不是S2b技术或消费PASS。
完整66路径增量与main86路径经正式plan和独立review，在clean exact main上已取得一次真实INTEGRATION PASS。
plan=`integration-revalidation-f8a6fd3c9c5d05f5a27e`；原8条追加task event经官方writer逐条保留body/id，
原14-event fragment完全一致后才新增恢复事件。相关历史失败、已消费授权和原source18项dirty均不改写。

复用`D:/Work/AITradingSystem_devx014_source_preservation`作为唯一coordinator，branch
`codex/trading-2564-s2b-preserved-integration`；DEVX014与TRADING2564按各自精确task-id source事务串行交接，
最终一个候选统一验收。后继LANE不冒充重新批准dirty plan。旧`D:/Work/AITradingSystem_trading2559_integration`
继续保留原源码和独有保全链；两root退出条件为S2b发布、unique证据规范保留/hash核验及无未审源码/运行依赖后
审计清理，当前不删除或另建替代工作区。详细合同、原failed Full parent与验证顺序见S2b合同§14。

研究目标保持不变：先交付可信的指定输入DQ合同，再接实际consumer/PIT范围；其后推进S3合法前瞻记录与S4
预注册/机制归因。工程工作只用于解除这些依赖，不把工程PASS当作信号有效性结论，不扩展到无关基础设施。
真实DQ、研究、observation/maturity/scoreboard、下载/cache/provider与交易动作仍全0，heartbeat保持PAUSED。

## 11. S2b 已发布与 S2c 研究消费范围接续（2026-09-07）

S2b恢复最终candidate为5c20c8fc3655e8a6e62a494122b785337a6c81de；首个源码提交为
a2f92209de2e7fb6c6610ae98d12d1ff1011ae9b。实际Full full_20260906T133132Z为
10990 passed/0 failed/5 skipped/641 warnings，16 workers/loadfile，runner4335.35秒；
summary SHA256=6002b20611f23d1cc46d19db8bbb9a529032d1812f939ff0b9546911b0fba0cd，
profile SHA256=ad45ad6fc2fdcc2144064b0002f2bd5451fd317815efa34b496d64256044ff34。
architecture/contract/integration/reproducibility为1056/281/995/24 PASS，均绑定该candidate。
原12项实际candidate合成E2E在focused和Full均PASS；Full新增16个parent经独立复核，无阻断项。
parent PASS仅是关联/进程观察成功，内含预期BLOCKED/WARN/FAIL，不表示16次DQ都PASS。

普通main push成功，local main、origin/main与live remote main已核对相等；事务
trading-2564-s2b-preserved-recovery-final-20260906-v2于2026-09-06T14:55:57.522735Z
COMPLETED/RELEASED，租约释放，replay12事件PASS。完整终态及清理证据在
outputs/architecture/trading_2564_s2b_preserved_recovery/publication_reader_brief_v2.md，
以及对应publication transaction的closeout_receipt.json。原失败Full parent、行政失败和第一次
CLOSEOUT旧base参数拒绝都保留；参数修正不改冻结plan或Full。只删除已合并的临时branch引用，
两root和旧18项dirty保留，退出条件继续按§10.4及workspace_retention_audit_v2.json执行。

下一步仍优先解除真实研究输入阻塞。两路独立静态复核确认equal-risk及冻结5候选均prices-only，
但旧S2b共同evaluated window仍会在prices=T/rates=T−1时阻断T日读取，不能用历史preview替代
当前采样。先执行S2c.1固定价格消费范围串行合同，再接纯五候选preview/显式只读plan；不改变
旧receipt/common-window API、DQ数值/时效规则、策略、成本、窗口或准入权限。详细步骤、角色
语义、必要反例、来源闭包和生命周期见
[S2c Equal-risk 价格消费范围](TRADING-2564_S2c_Equal_Risk_Price_Consumer_Scope_V1.md)。
Composer的2018分段训练、rates可知时点及S3/S4首看前规则另行受审；不借新价格范围移用权限。

## 12. S2c.1发布与S2c.2五候选预览接续（2026-09-07）

S2c.1最终candidate b79974d6531c70deb18db153b3bd16c8f9e6e6ac 已经actual E2E、全部formal tiers与
Full PASS并普通推送；final v2 closeout为PASS/COMPLETED/RELEASED，main/origin/live remote SHA
在发布收据中相等。登记中此前架构失败摘要由本次追加状态接续，不改写旧FAIL证据。

Owner在下一步说明后要求「好的，继续推进吧」，本波从该exact main复用DEVX014工作区推进
[S2c.2固定五候选只读预览](TRADING-2564_S2c2_Five_Candidate_Read_Only_Preview_V1.md)。先审查
同seal captured registry与新consumer源码闭包合同，再严格解析完整主窗口并复用冻结算法，
交付内存preview及显式只读plan；不新增scheduler/执行器或真实研究动作。真实DQ、manifest replay、
capture、observation、maturity、scoreboard、下载/cache/provider和交易继续0；heartbeat仍PAUSED。
S3前瞻/PIT时点、S4首看前实验规则与归因、S5实测提效仍按原顺序保留，不新建successor任务。

当前仅本地工程开发与合成验证，所有真实研究/DQ/采样/下载/cache/provider/交易动作0；umbrella
保持IN_PROGRESS。S2b工程完成不等于S2c、S3–S5或信号有效性完成，不新增successor任务。

## 13. S2c.2 发布与 S3a 时间证据接续（2026-09-07）

S2c.2 已以 f60153cb962d7b80013be258a4708af4012406fb 完成普通 main 发布：Full
11159 passed / 5 skipped，四类正式验证及 36 项 actual candidate E2E PASS。
local/remote SHA 相等、事务 COMPLETED/RELEASED、分支已清理，canonical root/evidence 保留；
验收见 outputs/architecture/trading_2564_s2c2_five_candidate_preview/closeout_verified_v2.json。

Owner 随后要求「继续吧」。当前从该 exact main 推进
[S3a 前瞻事件时间证据](TRADING-2564_S3a_Prospective_Event_Time_Evidence_V1.md)：
只实现独立 recorder/verifier 与新 forward-only timing 工程合同，先审查时间与身份边界。
五候选的旧收益首段是 Close(F)→Close(next(F))；收盘后生成的信号必须使用单独版本，在
next(F) close 生效后才开始计收益，不能把旧终点标签当作执行证据。Composer 已有对应收益时钟。
真实 durable completion、本地输入已知时间与 provider available_at 分别披露；旧缺口永久保留。

S3a 不启动真实 activation/capture，不接旧 writer、不运行收益或成熟结果，不改变冻结 producer、
DQ/registry、scheduler 或策略结论；真实研究、数据、provider 与交易动作继续为 0。
首次真实接入保留在 S3b，须复核新时间版本及 bounded manifest；S4 首次 outcome 访问前的实验
规则和 S5 实测提效依赖仍未完成。Umbrella 继续 IN_PROGRESS，不自动登记 successor。

## 14. S3a 发布与 S3b 生产采集能力接续（2026-09-08）

S3a 已发布 9489d807f2fb6fd8795ccbadb62e7983b96a3709，Full 11385 passed / 5 skipped /
641 warnings，四正式 tiers PASS；local/remote 相等、事务 COMPLETED/RELEASED、merged branch
已清理。验收为 outputs/architecture/trading_2564_s3a_prospective_event_time/closeout_verified_v1.json。

Owner 要求继续推进，现执行[S3b 五候选生产采集链](TRADING-2564_S3b_Prospective_Capture_Execution_V1.md)
工程实现与合成验证。新 profile 连接可信 DQ 父/子执行、同 context preview、完整输入记录及父 ACK，
旧 55/57/59 权限与时间层声明不扩权。真实 activation/capture/DQ/结果访问/数据/交易仍全 0。
Composer 主线的分段训练/PIT、S4 首看前协议及净收益会计、S5 提效均保留未完成，详见新需求。

## 15. S3b Full 失败与时钟合同直接修正（2026-09-08）

未发布候选 `12ec8c386` 的四类正式验证通过，但 Full 为 1 failed / 11811 passed / 5 skipped。
真实时钟 synthetic activation 被 `TEMPORAL_CLOCK_BACKWARD` 阻断；原始失败与 INCOMPLETE
字节已保全，main 仍为 `9489d807`。旧 UTC/monotonic 下界假设已由独立纯时钟采样证伪。
按[S3b 主机时钟证据修正](TRADING-2564_S3b_Clock_Evidence_Correction_V1.md)执行 C1–C4：
共享 outer clock envelope、raw/derived 分离、D/lease/manifest 双截止、版本兼容及正式重验。
不增加主观时钟宽限、不替换真实时钟 E2E、不重试旧 activation key；真实业务动作仍为 0。
S5 另外保留即时失败诊断输出和最新耗时 seed 的审查，当前不在冻结 Full 或本时间合同中修改。

同日接续：C1–C3 已实现并修复独立终审的原始纳秒前驱/父子下界与失败诊断归属发现；
116 项针对性验证及两名 reviewer 精确源码复验通过，扩大 source/shared 验证进行中。
验收绑定在 `outputs/architecture/trading_2564_s3b_prospective_capture/clock_independent_review_v1.json`；
C4 actual candidate、正式 tiers、失败 parent Full、main 普通发布仍待完成，S3真实采集/S4/S5
不因本工程进展转为完成。
