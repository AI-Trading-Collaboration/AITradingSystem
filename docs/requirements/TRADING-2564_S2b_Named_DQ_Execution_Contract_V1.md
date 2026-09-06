# TRADING-2564 S2b：指定不可变快照的 canonical DQ 执行合同

日期：2026-09-05。状态：IN_PROGRESS / 工程合同实施中，未签发任何真实消费权限。
2026-09-07终态补充：S2b工程已正式发布，详见§15；上行是历史起始状态，真实消费准入仍未签发。
所属 canonical task：TRADING-2564_LONG_TERM_RESEARCH_CAPABILITY_IMPROVEMENT_V1，P1。
Owner：Project Owner / integration-coordinator；DQ 领域和实际 consumer adoption 仍归 DATA-GOV-002。
本文件是 umbrella 的步骤文档，不另建 successor/task，不改既有 DQ/研究窗口/策略 policy。

## 1. 起点、目标和授权边界

已发布 S2a exact base：`293813e5e2e7b88886b79fc22cf77e2d57f1f346`。
复用 `D:/Work/AITradingSystem_trading2559_integration`，不创建新 worktree、clone 或市场 cache。
branch：`codex/trading-2564-s2b-named-dq-v1`；模式 SINGLE_LANE / coordinator / contract-change。
现有 source transaction：`trading-2564-s2b-named-dq-source-20260905-v1`，SHA256
`040fbf3a3aa833b0dfc06665650a544505c3650cb55e5a8741a59b67bbfc0d77`；
lease `lease-14c61eb81ed80a3dfc38`。source/final 两阶段遵守现有流程，不在此波修改 fence。

目标：指定 pointer/transaction 所确定的 immutable members，经实际代码身份绑定的 canonical DQ
执行后，得到独立 named receipt；零 DQ 的 verifier 重新核验同一证据，并交付进程内 captured bytes。
后续 adapter 必须消费这些 bytes，不能在 PASS 之后按路径重新读取数据。结构、DQ、消费准入、
研究准入和交易准入分别成立，任何一项不能冒充另一项。

Owner 的长期建设授权允许本地开发、合成工程验证、任务/文档同步和受治理发布。所有真实 DQ、
manifest replay、研究、capture、observation、maturity、scoreboard、下载、cache mutation、provider、
QuantConnect、Options、paper/live、broker、order、fill、position、交易动作在本波均为0。
不可复用2557/2563等已消费的一次性授权。合成 fixture 的软件测试单列，不计为真实研究运行。

## 2. 已审查的设计来源

以下本地只读笔记为具体设计输入，不是已经实现的证据；其 original S2a 等待文字保留为历史：

|runtime note（outputs/validation_runtime 下）|SHA256|
|---|---|
|trading-2564-s2b-s4-implementation-matrix-20260905.md|278837d4cec8eb996b325590669c4c35648968a803a2687a1ed8b58d122e27b8|
|trading-2564-s2b-git-bytes-bootstrap-review-20260905.md|975e32b5572731b5e6f38b96ccc1e5df3df5e9186128ab398f0f50d38bd5c901|
|trading-2564-s2b-process-boundary-review-20260905.md|a8b52ada29b1beb4260be8a0c968b3c67aeff59585666dd805c1d07f874770df|
|trading-2564-s2-adapter-training-scope-review-20260905.md|fee87a469de71279675ff13b6d1242f1e695579cfc6be453d3138d30da502b96|

独立 reviewer 对 DTO/verified seal 与 bootstrap 对抗测试分别复核；共享 wiring、源闭包 manifest、
任务、system flow、authority 和正式验收仅 coordinator 写入。worker 必须先获得明确不重叠路径。

## 3. 串行合同与步骤依赖

|步骤|内容|验收，不代表实际消费准入|
|---|---|---|
|S2b.1|独立 request/receipt/源身份 DTO、进程内 context/verified bytes、固定 fresh bootstrap|严格 schema、四根、Git源码身份、PID/context及不可序列化边界成立；toy Git反例通过|
|S2b.2|最小纯 manifest provenance 抽取及 canonical DQ named 分支|原 manifest 全行与 role/member/captured bytes三段绑定；legacy规则/错误语义不改变；数值和归因真实 synthetic 等价|
|S2b.3|named runner/verifier与固定worker入口|runner DQ恰好一次；verifier零DQ/零写；FAIL/WARN不签发strict PASS；消费不重读输入|
|S2b.4|提交后的真实candidate synthetic端到端与正式发布|完整真实模块闭包由Git bytes编译；真实policy/calendar对象身份核验；全部正式tier/Full和普通发布收据成立|

这是一个 S2b 输入合同波，未冻结完成前不得启动依赖其接口的真实 adapter/capture；不因某个纯合同
测试通过而将整个 S2b 或 umbrella 标为完成。各步骤可在同一 exact base 的不重叠代码范围协作，
但共享合同变动先由 coordinator 同步给全部 worker，再继续 dependent 实现。

## 4. 四根、输入来源与 DQ 语义

- 分别绑定 source root、显式 publication root、实际 execution code/policy/calendar root、evidence
  output root；不全局重解释 legacy project_root，不借同 SHA 或 basename 推断 relocation。
- 只使用 S2a named resolver，从精确 pointer id/SHA、transaction id/SHA 得到确定成员。
  不自行 glob/latest/current 选输入，不读取 legacy projection 冒充 immutable 输入。
- 原 download manifest 的 bytes SHA、完整 raw-string 行、ordinal及行 SHA，与原 output_path、
  transaction role/member、被捕获 bytes/path/SHA 必须一致。原路径必须由显式 source-root 关系解释，
  未知 relocation/重复行/缺行/role错配阻断；不伪造 DataFileSnapshot.path 或改写 manifest。
- 复用 canonical validator 的价格、rates、日期、窗口、policy、XNYS、warning/error判定规则。
  新 named 路径与 legacy publication/derived-window authority 互斥，不引入阈值或 DQ例外。
- 新 receipt 使用独立 schema/id，不继承或重新签发 legacy DataQualityExecutionReceipt、
  VerifiedDataQualityPreflight 或 consumer capability。可组合复用既有纯 window/policy/report值类型。
- 输出前核对 summary 和全部 COMPLETE attribution 的 role/path/SHA/row身份，保留 primary
  non-market-session 与 rates 六个已批准 site 的明确来源，不把 mismatch 降为 UNKNOWN，不扩大 secondary scope。
- manifest 是 provenance，不虚增 checked_input_count；实际 CSV 输入角色和统计保持 canonical 原义。
- 拆出 quality_provenance.py 仅限必要的纯解析、全行 hash、named matcher/CSV事实，避免导入环；
  不复制整个 legacy runner，也不改变旧异常映射或用历史 receipt 伪造新输入身份。

## 5. 实际执行身份与受信边界

初始支持 profile：GIT_COMMIT_BYTES_COMPILED。只有 execution root 要求是已治理的 exact Git
checkout；其余根不因该约束被自动迁入 Git。缺 Git/对象/闭包/source identity 时明确 BLOCKED。

1. 固定绝对路径 bootstrap 在任何项目 import 前仅使用 stdlib，拒绝预加载 ai_trading_system.*。
   不从 python -m 项目入口起步，-B仅防写pyc，不将其当作禁止读取旧pyc的证据。
2. 从 exact local commit 的原始 Git blob 捕获有限 reviewed module→path集合与源码 bytes，
   明确 blob/SHA身份；不使用 shell、textconv/filters、replace objects 或 lazy fetch。
   清除会改写 Git 目标的 ambient env，支持 linked-worktree .git 文件，不下载缺失对象。
3. 直接编译已捕获 bytes，执行原 package __init__，不伪造空包或改 leaf import 绕开包语义。
   包括真实 transitive closure；未知 ai_trading_system import fail closed，不自动发现扩大名单。
4. 分别记录 compile origin（candidate/blob/bytes SHA）与真实 execution-root物理映射；__file__/
   spec.origin/co_filename保留正确物理路径，使现有 PROJECT_ROOT 推导和归因读取成立。
5. 项目加载前依赖现有 checkout/reparse 审计与窄路径检查；加载后复用 contained reader，
   不在bootstrap复制复杂Windows文件系统捕获代码。DQ前和终端核验磁盘source/policy/review pack与绑定。
6. 核对实际使用的 calendar/policy缓存对象，而不是只 fresh-load 一个未被使用的对象；
   不以 cache_clear/reload、相同内容复制根或 __file__ 声明替代实际执行身份。
7. tracked source manifest 不嵌入包含自身的最终 commit SHA；candidate由请求/transaction绑定。
   manifest及加载源码均来自同一候选。LF/filter差异阻断，不静默归一化或改工作区。

信任受审 bootstrap、CPython/stdlib、Git及依赖环境与协作 lease；不宣称抵抗恶意管理员、
内存注入、同进程 monkeypatch 或绕过 lease 的恶意写入。本波不引入通用 import/RPC/sandbox框架。

## 6. 进程内消费对象

跨进程只传 request/receipt DTO、locator/hash与结果摘要；DTO没有执行或消费权限。
named runner/verifier要求真实 bootstrap context；VerifiedNamedInputs 只能由 verifier内部factory
创建，绑定当前context和PID，输入内容为不可变bytes，accessor再次校验当前上下文。
禁止pickle/reduce、from_dict_verified、already_verified及parent按child PASS重建seal。
新child如需消费，须零DQ重新 verifier后创建自己的seal；旧receipt source drift照常阻断。
factory导入只允许named verifier/明确合同测试，context初始化只允许受控bootstrap/明确合同测试；
legacy factory白名单不放宽。consumer在同child内直接解析verified bytes，不重新打开原数据路径。

## 7. 分层验证与自然集成边界

- 提交前：纯DTO/绑定/seal测试、toy fixture内已提交Git源码的bootstrap正负例、真实canonical
  validator的synthetic数值/归因测试。call spy仅计数，不伪造report来证明DQ语义。
- source commit后：在实际受审checkout/exact commit通过fresh bootstrap加载完整真实模块闭包，
  synthetic inputs执行runner→receipt→verifier，证明真实Git-byte身份与sealed bytes；不能因未提交
  而静默skip/允许dirty/复制源码根/回退磁盘import。此层必须进入正式验证。
- 对抗例至少涵盖：旧标签/未知字段/重复role/scope错绑；缺pointer/member/原manifest行篡改；
  traversal/reparse/捕获竞态；loaded A后disk B、旧pyc、未知import、ambient Git/replace/lazy fetch；
  错candidate/root/policy/calendar；结果/归因来源错绑；seal跨PID/context/序列化；verifier零DQ/零写。
- focused默认16 workers/loadfile；static与独立review通过后才冻结source/final。官方generator
  顺序、全source supersession和exact-tail测试一起更新，不能让generator与生成物共同漏项而自称通过。
- Full只在最终自然integration candidate运行；不消费旧S2a allowance或把旧Full冒充新候选。
  system_flow、task/requirement、catalog/authorities和实际receipt同步；保持历史失败与旧schema可追溯。

## 8. 明确未完成的后续依赖与生命周期

本波不改变2560 producer的rates末日门禁、2018训练角色或primary 2021-02-22默认。
训练历史的窗口、consistency_start_date和rates carry/PIT仍须单独adapter准入，primary-only DQ
不自动证明完整训练历史。实际source snapshot/root、as-of/profile、consumer/version和DQ动作上限
由Owner后续明确，不从现有运营PASS推断。S3真实时点/连续采集和S4首看/停止规则继续按umbrella登记。

此task branch仅在受治理main集成/普通push/SHA相等与唯一内容审计后删除；提交保留在main可恢复。
复用checkout继续保留本地canonical证据，无新临时目录；将来目录清理先按umbrella §6/§9.4归档，
不删除现有市场数据或他人工作区。若本波中断，保留 exact branch/transaction/进度与未发布实现，
恢复原范围，不创建替代 v2/v3 worktree来追赶main。

## 9. 进度

- 2026-09-05：S2a closeout PASS/COMPLETED/RELEASED已核验；S2b START PASS，source事务进入
  TASK_SOURCE_PRE_WRITE，先追加canonical接续事件及本合同，再执行LANE预检；尚无S2b实现或验收结论。
- 同日：provenance focused124 PASS、contract33 PASS、toy bootstrap58 PASS；真实candidate
  E2E尚未执行。新增helper影响两处旧source-copy fixture：test_qqq_options_signal_package.py与
  test_research_input_readiness.py。v1没有声明这些路径，先停止写入并保留全部bytes，行政FAILED/
  RELEASED（未派发Full），新source v2完整保留旧scope并精确增加两路径。当前事务为
  `trading-2564-s2b-named-dq-source-20260905-v2`，SHA256
  `7bb4cba2dde758ffd9d4fb8d15ac560f4b01e1be6ab93f8108bd189e9adc41df`，lease
  `lease-970fb9435eac9671d4d5`；同branch/HEAD，不创建worktree或改写历史。
  v2首次acquire因CLI重复传入两项自动追加资源而在创建前BLOCKED，确认transaction不存在后去重，
  最终声明包含原全部资源，acquire/TASK_SOURCE_PRE_WRITE均PASS，不省略锁或validation资源。

## 10. 接线时明确的协议边界

- source_root指原source project；source_output_relative_path必须显式指定原下载output目录。
  publication_root是实际immutable输出目录，可为合法搬迁后的目录。只用完整原路径匹配，不默认data/raw。
- 当前运行日历1.1.0与历史price attribution的1.0.0分别绑定。二者语义角色不同，不要求SHA相同。
- CLI source-lease-id仅记录可信父进程关联值，不是新的租约证明。父dispatch必须绑定既有guard/fence
  active PASS的不可变引用/hash、lease id、root/candidate、request SHA、child PID/起止/终态，并在
  child生命周期保持原lease有效。run记录原lease；verify记录当前lease且不改原receipt，不要求相等。
  child不另造lock/lease验证器。worker构造receipt前检查terminal source；父终态失败时不得用已写
  PASS receipt冒充成功dispatch。CLI JSON本身也不授予research/consumer权限。
- verifier还必须读取显式successful-dispatch locator/SHA，并对齐原receipt的request、代码稳定身份、
  PID、lease和成功终态。该独立proof绑定先落盘的父dispatch收据，父收据不反向包含proof hash，
  因而没有hash cycle。它是可信coordinator关联，不是签名、第二套lease或研究权限。
- actual-candidate synthetic测试只使用两个明确环境输入：
  `AITS_NAMED_DQ_PUBLICATION_TRANSACTION`、`AITS_NAMED_DQ_SOURCE_LEASE_ID`。
  coordinator在现有Full/focused命令显式设置它们；测试缺值typed FAIL，不查找latest、不skip。
  它们只传关联，parent仍须调用现有fence/checkout audit/ACTIVE-unexpired replay，记录原值、
  transaction raw SHA与内部binding SHA、pre/post guard证明、request/child PID/终态。
  父证据持久保留在已声明outputs/validation_runtime/named_dq_parent_dispatch；原Full runner、
  plugin和scheduler不在此波修改。CI正式Full同样必须有其真实coordinator事务，不能套本机路径。
  当前CI scheduled/manual Full未供publication-transaction是既有S5接线依赖，不在测试中绕过。
- 源码审查确认55模块闭包没有新的未声明项目import；Git/物理文件的LF字节核对已做代表性只读检查。
  新runner/bootstrap strict mypy/Ruff/Black通过不替代source-commit后的真实运行证据。

## 11. 源冻结前工程复核

2026-09-05统一focused：382 passed / 0 skipped / 1既有pandas日期解析warning，16 workers/loadfile，
186.31秒；XML为outputs/validation_runtime/trading-2564-s2b-unified-focused-20260905.xml，SHA256
bb6f4e5c029805737947713cd972edac2803ee9489fc921d108d21116bd3b8c9。
覆盖合同、bootstrap、named verifier、legacy canonical DQ及两处source-copy fixture；未执行真实candidate
E2E或Full。新parent失败控制流用明确CONTROL_FLOW_STUB_ONLY与内存sink补测，不签发真实guard证明。

实际candidate测试仅接受FORMAL_VALIDATION_PRE或FULL_DISPATCHED下现有事务绑定的clean exact
candidate；删除未使用的提交源诊断降级选项。12个E2E case预期16个synthetic child，其中8次canonical
synthetic DQ、5个零DQ verify/probe、3个pre-DQ阻断；父端拒绝不计入child。最终数字以实际收据为准。

RCF当前live source seal需同步本波system_flow及artifact_catalog文档字节；policy已在source v2
shared_paths内，无需换事务。S2b source authority同时显式纳入该policy与catalog，精确范围由26增为28；
不改历史3000/3152/192、legacy seal、inactive-shadow或cutover门禁。新增named报告/receipt及父proof为
显式locator的manual工程合同产物，不登记全局report discovery/latest，不进入Reader Brief自动消费。
先追加canonical事件、更新目录与当前seal，再按原定官方generator顺序重建，最终candidate另行验证。

新增5个父失败控制流及原reader负例统一92 passed，35.03秒、16 workers/loadfile；XML
outputs/validation_runtime/trading-2564-s2b-parent-control-and-reader-focused-20260905.xml，SHA256
fa4026e694e5dff7b0f896b0806c50026742ea7c6be0262a7601d722f8448910。测试仅内存sink且真实authority/
DQ/child禁止，不能当实际candidate终态证明。LANE预检PASS、blocker/warning为空、唯一有效v2 lease；
14项当前源码/test文件Black检查与Ruff PASS，核心runner/bootstrap mypy PASS。大历史架构测试的diff
仅S2b末项/继承接线，无附带格式改动；正式candidate尚未生成，实际E2E/Full尚未执行。

当前RCF live seal：system_flow=2354549 bytes / SHA256
f3a8b6de7baf0424250ad3c71f1d0a20ec19d9a73840efeb28b25f9218a8786b / 1216 blocks；
artifact_catalog=2011035 bytes / SHA256
02d5a795b2acdc56d73ec87cbb2eefec530137011709910d9b7b981a8e57aff1 / 563 blocks。
二者均LF原始bytes；report_registry未变。操作runbook已完整分段读取，当前为manual developer
validation/catalog同步，非周期业务dispatch；DQ状态不适用于本轮工程元数据，所有真实动作仍0。

冻结前全局数量复核：新增4个contract/data模块及4个test文件+1个support，当前清单应为1202 modules /
1363 Python test/support；writer仍856，历史Wave11不改。现有scanner从generated fitness取总数，
生成前返回1198/1358不是新树清单。当前测试预期ID按旧完整payload与已核实的新计数在内存计算为
arch_004g_deprecation_inventory_366e110900530e536b18，必须再由官方generator/scanner独立确认。
test_arch_004g_deprecation.py已在v2共享声明内，加入S2b精确来源后范围由28增为29；不扩大写权限。
独立复核没有发现其他生成后必失败的当前数量断言；旧S2a live fragment按既有builder正常重建，
其严格hash测试不放松，原Git历史与legacy prefix仍保留。提交源只服务Atlas exact-commit生成；
source事务行政释放后，同一source SHA获取final事务、完成五项generator和正式验证，不重复提交空树。

首次source四项generator PASS，官方fitness/scanner确认1202/1363/856及上述预期inventory ID，
RCF3152 entries/192 fragments，compatibility321 merged entries/15 fragments。随后generated focused
为78 passed/2 failed（154.90秒），XML trading-2564-s2b-generated-source-focused-20260905.xml，SHA256
ba99e2cae2efee43dfa1d2a7c32b706ac5f8dd8ad0897b1faa90e7c9dfd49d9e，原失败证据保留。
两项均为S2a-only源码漂移负例的旧错误码期望：最新S2b fragment不变，validator先在新expected index
处报AUTHORITY_GENERATED_STALE；共享S2a/S2b source仍先报AUTHORITY_FILE_MISSING。按三个路径逐项
固定准确错误码，并额外断言index的精确路径，不改validator、不用宽泛错误集合。此为既有来源负例
的测试断言纠正，canonical任务仍IN_PROGRESS且scope29不变；source事务仍GENERATED_REBUILD_PRE，
不是Full失败，也没有新研究动作。修正后按同一官方顺序重建受影响生成物并重跑同一80项focused。

## 12. 首次实际 candidate 验收与 Windows venv 进程修正

修正后的上述80项全部PASS（181.23秒），source commit为
5615e27c454cbd872553b04a809b4f6edd676f6c，parent/main/origin仍293813e5e2e7b88886b79fc22cf77e2d57f1f346。
source v2按既有协议行政释放后，final v1五项生成PASS、tracked clean，FORMAL_VALIDATION_PRE绑定
该候选；readiness七项/1242个明确依赖PASS，93.804167秒。尚未派发正式tier或Full。

实际candidate focused为8 failed / 4 passed / 215.39秒，16 workers/loadfile。原XML
outputs/validation_runtime/trading-2564-s2b-actual-candidate-focused-20260905.xml SHA256
eceb507fc8cd84f7c7f0d2e2aae36a6ebf52f8ca75f5bcb70cb03250ef7b63c3保留。
11个合成child均run：8次synthetic canonical DQ，3次pre-DQ阻断，verify=0；8个父收据因
NAMED_PARENT_CHILD_ASSOCIATION_MISMATCH为BLOCKED，successful-dispatch proof=0。
原child六个PASS/一个WARN/一个FAIL均不能覆盖父终态失败。另三个父PASS仅代表正常观察到预期
BLOCKED child，不是DQ PASS。全部真实动作仍0，没有信号有效性新结论。

根因是Windows venv redirector：Popen观察启动器PID，而真正Python执行PID不同。标准库诊断
重现原模式104980/93012；同一CPython直接启动为92576/92576，保持原venv executable/prefix、
isolated=1及dont_write_bytecode=true。不是通过取消PID检查修复。
采用[CPython v3.11.9官方Windows spawn实现](https://github.com/python/cpython/blob/v3.11.9/Lib/multiprocessing/popen_spawn_win32.py#L55-L62)
的持久平台适配：仅在Windows venv映射到同一sys._base_executable，向该child的env副本设置
__PYVENV_LAUNCHER__为原逻辑sys.executable；移除child副本中冲突的PYTHONEXECUTABLE及旧launcher值，
审计只记录处理键名与固定路径，不输出其他环境内容，不修改parent/global环境。

边界与验收：

- 仅测试parent helper及对应测试变更；production DTO、bootstrap、55模块源闭包、4根、canonical
  DQ规则、策略及所有PID/lease/终态检查不变。无效解释器或不支持runtime明确失败，不回退launcher。
- neutral parent launch audit区分OS executable与逻辑venv executable，记录profile和child-only override；
  run/verify/TEST_PROBE使用同一映射，不能以自报PID替代Popen PID。
- 纯映射覆盖direct/non-venv/Windows-venv/非法base和环境冲突；标准库probe核对实际PID、venv与-I/-B，
  不导入项目、不执行DQ。保留失败控制流负例，再由新正式candidate重跑原12项E2E及所有正式tier。
- 属于现有测试进程启动映射，不增加产品CLI、数据边界或生产报告合同，system_flow现有流程仍准确，
  本次不修改图或RCF内容seal；受影响S2b来源仍29项，官方authority按新字节重建。

失败final事务已FAILED/RELEASED，失败记录
outputs/validation_runtime/trading-2564-s2b-final-v1-failure-20260905.md SHA256
b96ca142339ac6db3f0157c42a4e45ab9cbd6836f1525fafc8016c35081bb8be。
11个parent收据与stdout保留在原candidate命名目录；33根345文件909318字节纯synthetic输入/证据已
逐文件SHA相等归档到outputs/validation_runtime/trading-2564-s2b-final-v1-synthetic-failure-archive，
未复制execution源码、未删除原件。该归档不是新的可执行workspace或准入来源；退出条件是任务归档
保留政策允许且无唯一证据，之后再审查清理。

当前source事务trading-2564-s2b-pid-fix-source-20260905-v1，SHA256
441babb1ea398025fb4e83b5e5ce0db5566ac3aa00cc942b40581a59d4588968，lease-1e3857d2053f86ee2933；
同branch/checkout/claims，TASK_SOURCE_PRE_WRITE，canonical追加纠正事件cycle457，LANE PASS。
没有failed Full parent，不使用failure_fix_rerun或借用S2a结果；S2b和umbrella继续IN_PROGRESS。

PID修复初轮focused为100 passed/6 failed（56.61秒），XML
outputs/validation_runtime/trading-2564-s2b-pid-fix-focused-20260905.xml SHA256
c196c08b43b3ab079ca10a06dbef9aa0debf8c5c6c2aea881b68cf6b6869887c。
真实标准库PID/venv/-I/-B探针已PASS；六项失败仅CONTROL_FLOW_STUB_ONLY在设置两个合成AITS环境键前
捕获expected_launch，而dispatcher使用设置后的env副本。将期望值捕获移到fixture环境准备完成后，
保留精确环境比较，并用无trace的固定失败说明避免未来比较失败打印ambient环境值；不改实际启动
实现或生产合同。原失败XML保留，同source事务继续重跑106项，未执行新的named candidate或Full。

修正后同一106项全部PASS，54.88秒，16 workers/loadfile；XML
outputs/validation_runtime/trading-2564-s2b-pid-fix-focused-corrected-20260905.xml SHA256
fd1742ab8e8b0cf575c5bbe124e61cc26dcb1436bb08acd9923b377225ee567b。三文件Black/Ruff PASS；
测试差异只含启动适配、neutral审计及14项新增回归（含1项标准库probe与1项PID mismatch控制流）。
生产bootstrap/DTO/runner/validator/source manifest对5615e27c4无diff。下一步同四项source generator
与受影响authority focused，再source commit、final五项生成和原12项actual candidate；不提前标S2b完成。

## 13. 首次 Full 失败与当前派生证据的等价修复

2026-09-06：PID修复后的authority focused47 PASS，candidate为
06140c52ca4e5be718075b7f436b820b927637c4。原12项actual candidate全部PASS，356.09秒；
只读readiness七项/1242依赖PASS。正式architecture/contract/integration/reproducibility分别为
889/281/995/24 PASS；首次Full为10654 passed / 11 failed / 5 skipped / 641 warnings，
3893.89秒，artifact=outputs/validation_runtime/full_20260905T155839Z/test_runtime_summary.json，
SHA256=ccef4a6bd377e560c62803db953f530bf45a148f6c30d255572d26d887454e04。
profile/telemetry PASS、10670项collection完整，不覆盖pytest FAIL。Full内同12项named E2E也全部PASS。
两轮各16个synthetic child=11 run/5 verify-probe、8 synthetic DQ；真实DQ始终0。

失败分组：TRADING2452历史source helper漏登记精确S2b successor（2项）；C1当前归因清单仍绑定旧
source/hash和69site预期（2项）；rate review pack因C1 freshness失败级联（7项）。正式失败记录为
outputs/validation_runtime/trading-2564-s2b-first-full-failure-20260906.md，SHA256
0dfd78e2f939ba9ab6e69f37b692f947ca74129c3b16089040d5b4b29ef25025。
原final事务FAILED/RELEASED，main/origin仍293813e5e2e7b88886b79fc22cf77e2d57f1f346，未发布。

现有DATA-GOV-002C2/C3和TRADING-2542I记录已明确区分原Owner-reviewed历史与current-derived pack。
当前derived pack可以在严格等价证明后重建并重绑，不代表新Owner审批，亦不扩大六项scope。修复按
以下顺序实施，任一语义断言失败立即转source-owner/serial contract review，不自动重绑：

1. 原69site按stable site_id比较，除导航位置外逐项不变；新增恰为named manifest绑定的3项
   static/template/dynamic emission，全部保持GLOBAL_OR_UNKNOWN_SCOPE、OWNER_REVIEW_REQUIRED、
   phase_c_migration_eligible=false；原policy-authorized code/site仍1，不新增isolation权限。
2. 官方C1 builder重建JSON/validation/Markdown，再官方rate builder重建三件套；比较新旧rate pack，
   除content-derived id、C1 id、input-binding SHA/size及source_line外，六candidate全部字段、
   emitter_function_ast_sha256、rate_policy_snapshot、proposal、authority、summary、recommendation及safety相等。
3. 只重绑decision.review_pack.pack_id/sha256、runtime exact pack-ID常量及其exact-binding test。
   decision id/version/date/owner、原批准历史、六sites、row digest、conditions、阈值与scope不改。
   原price frozen pack/decision、active capability policy、55module/7dependency路径集合不改。
4. 历史helper只显式登记精确S2b phase并保留原live SHA比较；新增wrong SHA、缺source/supersession、
   historical_hashes_rewritten、authority locator及unknown-future phase负例，不使用前缀或索引范围豁免。
5. S2b current-source闭包29→40：增加C1/rate三件套共6路径、rate decision/runtime/exact test/C1 test共4路径，
   以及TRADING2452测试1路径。所有生成物/共享wire由coordinator更新，历史seal不改写。
6. 先等价性和全相关focused，再官方source/final；新candidate实际E2E和parent-bound Full不能借旧PASS。
   本次只同步元数据及派生身份，无产品CLI、图示流程或阈值改变，system_flow无需为本次修复另改。

新source transaction为trading-2564-s2b-derived-fix-source-20260906-v1，SHA256
5bff8113afb761b5287c5ba90b6a553dd5a8942e2e9fe9968d9c4ac0605f1b2e，lease-06816c342414ec09daa5。
保持原branch/checkout/frozen-main，完整补声明精确路径，绑定上述失败Full为parent；不建新worktree。
canonical追加失败/修复事件，S2b/umbrella保持IN_PROGRESS，所有真实研究、数据及交易动作继续0。

证据生命周期补充：focused轮33根345文件已SHA归档；Full结束后的归档前检发现pytest-18406根已不存在，
因此未创建archive、未复制或删除任何文件。Full父收据、请求、stdout和pre/post proof及正式报告保留，
不重造临时输入或声称原临时成员完整可再字节重放。S5增加运行期合成证据保留/临时测试与canonical replay
边界，以及将C1→review pack→decision/独立successor断言列入相关focused选择的后续项；不得仅靠Full发现
已知依赖遗漏。本波不修改Full runner、临时清理规则或S1全局readiness，详细退出条件沿§8保留。

## 14. 原始源码保全后的单 candidate 恢复（2026-09-06）

DEVX014已发布C=`64e1d3da3cd41ae842fd7cc3f14802c550ceb9ef`，其Full10685/0/5通过但不替代S2b验收。
旧source工作区的单次锁迁移1/1、源码保全1、独立校验1已完成；不再执行这三个已消费动作。
原HEAD06140c52、18项dirty raw bytes/index及旧terminal/events保留。snapshot
`d5e8b26f2a4ac3c66ccb941b77e6f7c983423786`只是RAW_BYTES_SOURCE_ONLY_UNVALIDATED；恢复范围是
原base293813e5到snapshot的完整66路径，不限于最后18项dirty修复。

唯一coordinator复用`D:/Work/AITradingSystem_devx014_source_preservation`，branch
`codex/trading-2564-s2b-preserved-integration`，当前HEAD/main=C。原source
`D:/Work/AITradingSystem_trading2559_integration`不写入、不重跑、不清理。
两root的purpose、next owner及退出条件已在DEVX014与本umbrella登记：最终S2b发布后，unique证据规范保留并
逐hash核验，确认无未审源码与运行依赖，才按既有生命周期审计清理；不创建替代worktree追赶main。

正式plan `integration-revalidation-f8a6fd3c9c5d05f5a27e`绑定原base、真实snapshot lane与C，
内部SHA `f8a6fd3c9c5d05f5a27ecbbce8561f83886bc82c192c52c7136ae2551a8f4b3c`；66/86增量、30 overlap
（9 coordinator refresh、21 domain）经过独立review。clean C的真实INTEGRATION已PASS，收据在
`outputs/architecture/trading_2564_s2b_preserved_recovery/clean_integration_admission_v2.json`。
不改plan分类、不伪造lane head、不让保全receipt签发消费权限。

DEVX014 source-only事务已追加交接后行政FAILED/RELEASED（candidate=null、Full=0）；TRADING2564
source事务`trading-2564-s2b-preserved-recovery-source-20260906-v1`在同root/branch/HEAD接管精确归因bytes。
后继LANE PASS是原clean admission的继续，不是重批dirty plan；main/HEAD/branch/plan/lease/归因漂移即停。
canonical原6条event与snapshot前缀完全一致；原新增8条已通过官方update逐条重放，完整body/id与14-event
fragment对象全部等同snapshot，之后才追加恢复事件（cycle480）。不改原事件base/time，不覆盖主线其他任务历史。

调和规则：以main为底稿保留S2a→OPS079→DEVX014，最后追加S2b40-source；本candidate的inherited
supersession authority明确DEVX014，S2a业务依赖不变。保留DEVX01434-source/live SHA与全部安全负例；
未知/近似future phase、错误SHA、缺source或supersession仍失败。11个两边重命名的旧fragment和9项coordinator
view均在审查逻辑section后官方重建，不复制旧生成物覆盖主线，不改C/D/S5与legacy306冻结前缀。
module1207/test-Python1368、compat17/323、flow1226、RCF3162/192只是实际增量推导的待验证预期；
deprecation ID/live seal必须按最终源码与LF文档生成。针对DEVX014独有source poison的错误码按实际最新
fragment关系逐项固定，并核对index detail；不接受宽泛错误集合。

独立consumer review确认execution_lease.v1的字段/事件/hash/TTL/replay不变，OS arbiter协议单列WRITE；
legacy/mixed store及state/path错误不当BUSY重试。S2b parent只消费fence/ACTIVE/expiry/逻辑event bytes，
不读取旧arbiter owner结构；55模块child闭包不含architecture/kernel/fence，主线未改其中路径，不扩大闭包。
最终candidate必须再做原12项真实bootstrap/E2E及正式tiers，不能复用旧PASS。

失败Full parent仍为`full_20260905T155839Z`，summary/profile原件已按同相对路径逐字节复制至本root：
SHA分别`ccef4a6bd377e560c62803db953f530bf45a148f6c30d255572d26d887454e04`与
`d01fe359e9f47bbac76a348da7a1952aa8ec0584e92c833462372c5195e9c061`，共2文件20040168bytes，无覆盖。
原locator本就是repo-relative；optional absolute-locator import helper因不适用而拒绝，未生成mapping。
原Full runner正常parent-binding只读校验已PASS（PYTEST_FAIL），因此保留原provenance并直接绑定；
不改validator或把失败父结果改为PASS。详细成功/拒绝收据保留在serial handoff记录。

本恢复先完成源码/精确后继和C1→rate pack→decision等价focused，再source四生成、final五生成与最终正式验收。
S2 consumer adapter、训练PIT/rates carry、S3真实前瞻和S4机制归因仍待后续受审阶段；S2b工程PASS不等于
信号有效。真实研究、manifest replay、canonical DQ、observation/maturity/scoreboard、下载、cache/provider/
QuantConnect/Options、paper/live/broker/order/fill/position/交易全部0；heartbeat保持PAUSED。

### 14.1 恢复源码与针对性验证

21项非重叠源码/测试/config通过apply_patch恢复后逐文件SHA256与保全snapshot完全一致；
共享compatibility builder及5项测试以C为底稿调和，保留DEVX014全部34-source与负例，
S2b精确40-source独立常量、拒绝sources/superseded同时多加或删减、最新S2b失败不回退DEVX014，
以及未知future phase拒绝检查。没有扩大validator接受集合。

源码focused：456 passed / 0 failed / 0 skipped / 2 warnings，16 workers/loadfile，199.86秒。
收据为outputs/architecture/trading_2564_s2b_preserved_recovery/source_focused_v1.json；
XML SHA256为05b8df54352a5805964af995cd6f6b1d3da35d20c5130594b6932cb36478c4c1。
两项warning来自既有合成无效日期测试；不是实际candidate验收或真实DQ证据。

官方C1/rate静态生成与逐字段等价核验PASS：原69 sites仅line改变，6 rate rules仅source_line改变；
新增3 sites全部GLOBAL_OR_UNKNOWN_SCOPE/OWNER_REVIEW_REQUIRED且不具备迁移/隔离授权。
6项生成物raw bytes均与保全snapshot相同；decision只更新对应pack ID/SHA，不改归因规则。
详情记录在dq_derived_equivalence_v1.json。按核实的模块/测试计数在内存推导当前
deprecation inventory ID为arch_004g_deprecation_inventory_9f37718b96f3d47939c5，
仍要求后续官方完整生成独立确认。

提交源码只服务真实committed bootstrap与Atlas exact-commit生成。源码提交后，本阶段不再改变
tracked requirement/canonical语义；source事务通过既有publication release行政释放
（candidate=null、Full=0），同root/source SHA获取final事务、执行五生成和最终验收。
后续终态事实先写runtime receipts，再由受控后继阶段同步任务；不把计划写成已通过结果。

### 14.2 生成阶段顺序与事件时间纠正

source v1进入GENERATED_REBUILD_PRE后，官方refresh-consumers因只接受TASK_SOURCE_PRE_WRITE
返回PUBLICATION_PHASE_MISMATCH，刷新未发生。没有直接调用底层writer绕过门禁；
v1以candidate=null、Full=0行政FAILED/RELEASED，失败与43个精确脏路径bindings保留在
source_phase_retry_v1.json。source v2在相同root/branch/HEAD/main/plan与全部43路径hash核验后
接管，LANE继续预检PASS；此为原clean integration admission的连续执行，不是重批dirty plan。
正确顺序为TASK_SOURCE_PRE_WRITE完成task事件及refresh-consumers，然后GENERATED_REBUILD_PRE
内先只读validate已生成canonical，再执行architecture→report-flow→compatibility并封存POST。

cycle481的occurred_at误填2026-09-06T12:04:00Z；实际命令完成不晚于时钟观察11:59:23Z。
原事件原样保留，已追加显式时间纠正事件，新事件时间由系统UTC时钟产生。
该工程进展时间不供PIT/研究样本使用。事务重试、时间纠正均不改变研究/数据/交易全0边界。

### 14.3 独立复核的缺失绑定阻断修复

共享测试helper在最新S2b缺sources或superseded列表/匹配项时可能continue到旧DEVX014；
仅错SHA双phase负例不能覆盖该缺口。修正限定于S2b exact40与TRADING2480受限路径的真实4项交集：
docs/system_flow.md、tests/test_arch_004_refactor_policy.py、tests/test_arch_004g_deprecation.py、
tests/test_trading2452_architecture_contract.py。历史hash漂移且精确S2b phase存在时，
先要求mapping、sources唯一匹配和superseded membership完整，再沿原chronology与live SHA规则验收。
不把其他历史路径错误交给S2b，不改生产validator或future phase白名单。
原2特殊fixture保留错误SHA拒绝，新增4类缺失的双phase负例；验收为全部共享focused PASS，
原DEVX01434-source与22项合成回归不减弱，原历史对象与raw fixture bytes不变。

50项特殊来源合成回归全部PASS（6.86秒、16 workers/loadfile），独立静态复核确认缺失绑定阻断已关闭。
首次共享generated focused为130 PASS/1 FAIL/0 SKIP（190.53秒），失败仅为新增交集断言误把本项目
平铺pytest模块写成tests包导入，ModuleNotFoundError；按现有named_data_quality_support相同的
平铺导入约定修正，不跳过交集检查、不修改被检验的集合。原v1 XML保留，后续v2同选择器重跑。
本轮Black --check另外指出2项共享测试需格式化；按项目Black规范统一，再按官方四项顺序重建并验收。
官方首次生成已确认1207/1368/856/0 violations、RCF3162/192、compat17/323及预期deprecation ID；
source transaction仍GENERATED_REBUILD_PRE，未作candidate commit或Full，也未改变任务IN_PROGRESS状态。

### 14.4 首个恢复提交及需求引用纠正

共享focused v2全部131 PASS/0 FAIL/0 SKIP，153.70秒、16 workers/loadfile；
XML SHA256为9e4e0bf856b66e4b04066b373d15b7b38c243d42db8672427cf23b1729cec595。
Black/Ruff及5个新增执行源strict mypy PASS。source提交
a2f92209de2e7fb6c6610ae98d12d1ff1011ae9b，parent/main/origin缓存仍为
64e1d3da3cd41ae842fd7cc3f14802c550ceb9ef；71个rename-aware文件，84个no-renames归因路径。
原14-event前缀仍完全一致，当前18 events；原base/P/C plan原始SHA不变。

source v2按既有协议行政释放后，final v1在canonical/architecture PASS之后的Atlas前置失败：
PAGE_EFFECTIVENESS_TASK_REQUIREMENT_BINDING_INVALID:TRADING-2564_LONG_TERM_RESEARCH_CAPABILITY_IMPROVEMENT_V1。
原因是coordinator后续update --notes替换了当前备注而未保留完整docs/requirements路径，
使requirement_refs从cycle480的两项变为空；原事件与需求文件均在，不是来源文件丢失。
Atlas尚未写新页面，后续RCF/compat generator未执行，candidate=null、actual E2E=0、Full=0，
tracked clean；原source commit、失败收据及final v1行政FAILED/RELEASED全部保留。

source v3只纠正上述元数据：官方追加事件恢复umbrella/S2b两条完整需求引用并保留全部18事件，
同branch/root从a2f92209继续，expected main仍C。新增提交前只读build_page_task_coverage检查，
先确认canonical需求绑定可解析；这不替代source commit后的Atlas exact-commit检查，
也不改变任何validator、生产源或投资语义。current notes继续携带完整需求链接、研究方向和两root
退出条件。本metadata failure-fix需要新的source commit及final事务，不覆盖首个提交或伪造其PASS。
S5将使用这次真实失败记录评价提前检查的效率收益；本波不扩展任务writer或新增治理系统。

## 15. 恢复验收与普通发布终态

最终candidate 5c20c8fc3655e8a6e62a494122b785337a6c81de已完成四正式tiers及Full；
full_20260906T133132Z：10990 PASS/0 FAIL/5 SKIP/641 warnings，16 workers/loadfile，
runner4335.35秒。summary SHA256为6002b20611f23d1cc46d19db8bbb9a529032d1812f939ff0b9546911b0fba0cd，
profile SHA256为ad45ad6fc2fdcc2144064b0002f2bd5451fd317815efa34b496d64256044ff34。
原failed Full parent仍保持原件与精确绑定。12项actual candidate合成E2E全部通过；独立复核
Full新增16份parent的关联/完整性通过，但其子结果含预期拒绝、WARN及FAIL，不等于全部strictDQ。

local main=origin/main=live remote main=candidate，普通非强制push成功；final v2事务已
COMPLETED/RELEASED，lease释放且12事件replayPASS。正式终态、四tier和独立审查、两次收尾预检、
普通push与SHA证据集中于outputs/architecture/trading_2564_s2b_preserved_recovery/。
已合并branch引用删除，代码可由main恢复；两root、旧18项源码和unique runtime证据继续保留。

所有真实研究、manifest replay、canonical DQ、observation/maturity/scoreboard、下载、市场cache、
provider与交易动作0；heartbeat仍PAUSED。S2b工程验收不签发真实consumer/研究权限。
后继S2c先审查固定prices-only consumer scope，旧共同evaluated window/receipt/API保持原义；
其后才接equal-risk纯预览，Composer分段训练和S3/S4仍未完成。参见umbrella§11与S2c步骤文档。
