# TRADING-2564 S2c：equal-risk 价格消费范围与纯预览接续

日期：2026-09-07（Asia/Tokyo）。状态：IN_PROGRESS，先执行最小串行合同波。
所属任务：TRADING-2564_LONG_TERM_RESEARCH_CAPABILITY_IMPROVEMENT_V1；P1。
Owner：Project Owner / integration-coordinator；DQ/consumer adoption 领域仍归 DATA-GOV-002。
模式：SINGLE_LANE / coordinator / contract-change。
冻结 main：5c20c8fc3655e8a6e62a494122b785337a6c81de。

## 1. 研究目标与真实依赖

目标仍是更连续、更低成本地获得可证伪的新研究证据，不是增加治理组件数量。
S2b 已正式发布；equal-risk 的价格算法不依赖 Composer 的2018训练初始化，可先接续。
但静态审查发现：S2b evaluated_window 是 prices/rates 文件日期范围交集，旧 accessor 要求
该交集覆盖整个消费窗口。因此 prices=T、rates=T−1 即使 strict canonical DQ PASS，旧 API
也不能消费T日窗口。不得把 scope 写T−1却计算T，不补rates、不把历史预览称为当前OOS。

两路独立只读复核（devx014_retained_plan_review、devx014_migration_review）确认：冻结五候选
均为 prices-only；rates 仅用于原 DQ gate 和策略定义中的系列名称元数据。范围变化必须先
成为独立受审串行合同，不能隐藏在 adapter。coordinator 已核对下列实际源码：

- named_quality_execution.py::run_named_data_quality_execution 向 canonical validator 传完整
  requested_window，不是共同 evaluated_window 的切片。
- quality.py::_check_price_requested_window 对每个 expected ticker、每个 canonical XNYS
  session 检查边界和内部缺失；strict PASS 不允许 coverage warning/error。
- quality.py 的 rates freshness/future 检查按同一 as-of 和既有 reviewed policy 执行。
- simple_baseline_portfolio_control.py 的固定配置、equal-risk 逆波动、dynamic challenger
  均只读取 prices；simple_baseline_forward_aging.py 的 run_* 入口有 DQ/读路径/写入副作用。

## 2. 本阶段顺序与范围

|步骤|交付|验收与后续依赖|
|---|---|---|
|S2c.1 当前串行波|固定 equal-risk price-consumer typed scope 与 prices-only accessor；独立实际源码 manifest|旧 API/receipt 不改义；真实 canonical 合成 T/T−1 正负例、身份和副作用边界通过；正式发布后才能启动依赖其接口的 consumer 实现|
|S2c.2 后续 consumer|完整已验证价格字节与捕获 registry → 纯五候选预览、显式只读计划|逐值等价、完整主窗口、月初/lag、registry/alias/closure、重复/缺失/future 负例；不调用旧 writer|
|S3 后续真实前瞻|绑定真实 snapshot/PIT/activation/首合法 session 与实际持久化|Owner 准入每类动作；sample/episode 在首次 outcome 访问前冻结；旧 gap 永不补成OOS|

本波不实现固定 preview CLI、收益/排名、observation/maturity/scoreboard 或新的 scheduler；
不改 Composer、baseline 算法或 registry 数字。没有另建 successor/task。
S2c.1 是解除真实接入阻塞的最小共享合同，不作为 S2 或总研究目标完成的替代品。

## 3. 固定价格消费合同

新增独立 typed NamedEqualRiskPriceScope 与 VerifiedNamedInputs 的明确 prices-only accessor。
scope 是可序列化请求 DTO，不是权限；只有现有 verifier 签发的进程内 VerifiedNamedInputs
可以交付 bytes。不得新增 ignore_evaluated_window、already_verified 或私读 _captured 的外部通道。

- 固定 consumer：simple_baseline_forward_aging_preview@1.0.0，仅准备冻结五候选价格计算。
- prices=FEATURE_INPUT；rates=DQ_GUARD_ONLY。新 accessor 只返回原始 captured prices bytes；
  不提供 rates role 参数、carry、裁切、填补或重新打开市场路径。
- 明确 as_of 和 requested price window；as_of 与原 DQ request 相同，窗口完整从项目主窗口
  2021-02-22 至 as_of，不把训练/敏感性日期加入 primary。该起点是现行项目政策，不是新阈值。
- 新 consumer scope 的集合固定恰好为 QQQ/TQQQ/SGOV 和 DGS2/DGS10/DTWEXBGS，不允许 caller
  扩张。原 strict canonical DQ request 必须覆盖此完整 requested window、三 ticker、三 rates
  和 prices/rates，原请求允许覆盖超集；secondary 继续依原请求/政策，不新增强制或豁免。
- 价格覆盖依据为绑定完整 request、报告、validator、calendar 的真实逐 ticker/session 检查，
  不是单文件 min/max；本 API 不另跑 DQ。缺ticker、gap、warning、FAIL、过旧或未来rates仍拒绝。
- 完整 registry 的 EXECUTION path/SHA/size 必须已在原 receipt.execution_dependencies 中绑定。
  路径固定为 config/research/simple_baseline_strategy_registry.yaml；不接受仅策略行 hash、旧
  policy_definition_hash、basename 或 caller 自报 PASS。本波只校验 bootstrap 捕获的完整配置
  身份，不宣称内容、五候选集合、alias 或策略语义已经获准；这些校验明确留给 S2c.2。
- 消费源码在 DQ run 与独立 verifier child 中必须为同一完整 Git-byte identity。当前独立 manifest
  在原55模块基础上明确增加 data_foundation、simple_baseline_portfolio_control 两个既有模块和
  完整 registry dependency；原55/7 manifest 不变。未来 consumer 新模块必须重新纳入实际闭包，
  不能先做旧DQ再临时 import 未绑定算法。新增 manifest 的具体集合须独立精确断言和实际运行验证。
  accessor 固定检查受审新 manifest 完整路径和 bytes SHA，不以57/8数量代替身份；manifest
  本身仅列路径、不含源码hash，因此固定其SHA不产生源码自引用。其他manifest、同数量替换、
  旧55/7 receipt或新旧closure混用均不得获得新接口。
- 保留同 PID/context、不可 pickle、successful original dispatch、全部 input/report/source bytes
  和 terminal identity 条件；context、canonical validator、lease/fence 不放宽。
- 原 NamedDQScope、receipt.v1、manual.v1、evaluated_window、assert_scope_covered/bytes_for
  保持原义。同一 T/T−1 收据旧接口仍拒绝T，新固定价格接口才可据价格范围交付 bytes。
  expected_evaluated_window 仍需诚实为T−1或未预设，不能为测试改成T。

这是数据读取范围合同，不是研究/消费部署准入。原始 bytes 可包含窗口外历史，后续 parser 必须
显式核对实际消费范围、唯一键、必需列、有限正价格和未来行；不靠隐式 pivot(last)/ffill 修复。
窗口准入不证明有足够 lookback，也不证明 publication/provider available-at、decision cutoff 或OOS。
rates观察日期旧不代表其修订字节当时已知；未来 S3仍须审查包含guard在内的真实可知时刻。

## 4. 必须保留的算法与后续消费验收

原 _realized_vol 使用 pct_change、rolling(60).std 默认样本标准差、shift(1) 与sqrt(252)；
equal-risk 按原policy上下界、零波动转NaN和50/50初始化处理。月频只在完整矩阵月首行定权，
收益端另外shift(1)。完整主窗口不可替换成最后60行；不足历史不借初始化当准入成功，真实零波动
的冻结50/50也不擅改。dynamic challenger保持原daily行为，不统一改成monthly。
完整registry、三资产/三rates与原五候选/alias必须保持；更改rates且分别合法过DQ不改变权重。
未来实际纯预览记录pandas/numpy等计算环境版本；Git SHA本身不证明跨环境数值重现。
Composer的分段训练覆盖合同仍独立待审，不借此prices-only范围授权其rates/训练消费。

## 5. 本波验证与失败边界

1. 纯 DTO/当前context测试：合法DTO canonical round-trip成功；错误字段、固定consumer、registry
   path/SHA/size、as-of/window、必需ticker/rate集合、非strict报告均拒绝；context/VerifiedNamedInputs
   的pickle、跨PID/context、关闭后访问失败。DTO不是seal，也不是研究授权。
2. 合成 canonical：prices=T、rates=T−1 strict PASS；旧API拒绝T，新API仅交付同prices bytes，
   共同 evaluated_window 不变。SGOV缺末日/内部session、TQQQ缺失、rates缺失/过旧/未来/异常
   仍由原DQ阻断，不凭全文件max或新scope宣称通过。
   原DQ仅覆盖尾段/至T−1、仅声明两ticker，即使文件含完整历史/三ticker/T也不能扩张scope。
   registry错误role/完整路径/SHA/size或与捕获依赖不一致拒绝；as-of不一致拒绝。rates可接受
   滞后完全依原政策，不硬编码T−1；非交易日as-of通过范围检查不代表当日feature session就绪。
3. 实际提交候选：原12项55模块E2E完整保留；新增57模块/8依赖独立精确集合、真实Git编译、
   原run→独立verify价格scope probe，验证DQ次数1/0及旧新接口分离。TEST_PROBE不是生产preview。
4. 16 workers/loadfile；源码static、聚焦、独立复核后按source/final顺序生成、正式tiers/Full、
   main fast-forward、普通push及SHA复核。只在最终候选运行Full，不复用S2b的PASS。
5. 当前authority新增精确S2c继任段；原S2a/OPS079/DEVX014/S2b及legacy306历史保留，缺source/
   supersession不回退旧段。system_flow/catalog同步真实接口，不提前登记未实现preview。

## 6. 权限、工作区和进度

Owner长期能力建设与继续开发指令覆盖本地代码、离线合成工程验证、任务同步和受治理普通发布。
真实研究、manifest replay、canonical DQ、capture、observation/maturity/scoreboard、下载、市场
cache mutation、provider、QuantConnect、Options、paper/live、broker/order/fill/position/交易全0。
旧精确1次迁移/源码保全和2557/2563研究授权均不重复消费；heartbeat保持PAUSED。

继续复用 D:/Work/AITradingSystem_devx014_source_preservation；branch为
codex/trading-2564-s2c-price-consumer-scope-v1，不建新worktree/cache。source transaction为
trading-2564-s2c-price-scope-source-20260907-v1，lease-8ff0e9161c1c91a924ca。
当前root保留canonical历史/本波runtime证据；旧 D:/Work/AITradingSystem_trading2559_integration
保留18项dirty源码和unique inputs/outputs，不写入或清理。next owner为coordinator；退出条件是
规范保留/hash核验所有唯一证据、审清源码/进程/scheduler依赖后精确allowlist清理。任务branch
仅在正常发布与无独有提交后删除；已提交代码从main恢复，临时fixture成员不冒称永久可重放。

- 2026-09-07：START预检PASS，clean exact main且无旧活动lease。第一次acquire把runner suite名
  误当fence tier名被创建前拒绝，事务文件不存在/active leases为空；采用既有policy的默认必需
  tier集合后acquire与TASK_SOURCE_PRE_WRITE PASS，未省略任何验证，也未派发测试/研究。
- 当前先登记合同及S2b终态；实现前再LANE预检和独立合同审查，所有实际研究计数保持0。
- 两路 exact-document 独立复核均无架构性阻断；已澄清固定消费集合/原DQ超集、manifest精确
  身份、registry身份与策略语义分离、DTO与seal序列化边界，并补入尾段/缺资产等决定性负例。

## 7. 源码实现与阶段验证

已实现固定scope、原seal内prices-only accessor及精确57/8 manifest；其raw SHA为
b3dd9c781237fc71234f793832ff5d426d0244db792b177111569459f460e822。旧55/7 manifest、bootstrap、
canonical validator、named runner/verifier、旧scope/accessor、baseline及registry算法数值未改。
S2c.1兼容性继任明确26-source闭包，原S2b40/DEVX01434及legacy历史合同保留；受限历史路径
交集恰为4项，最新S2c缺绑定不能回退S2b，未知future phase不被自动准入。

源码core focused 251 PASS/0 FAIL（157.86秒），restricted 62 PASS/0 FAIL（6.98秒），均16 workers/
loadfile；contract strict mypy PASS。两个XML及源进度摘要位于
outputs/architecture/trading_2564_s2c_price_consumer_scope/。首次manifest排序负例1 FAIL/73 PASS，
修正为ordinal排序；新增13场景首次因fixture winning_row_keys未按dimension/date排序在发布前
失败，改为sorted且不去重/不改CSV后13 PASS（23.87秒）。失败记录不抹除、不替换成串行PASS。
合成canonical测试不代表actual Git bootstrap或真实市场DQ，仍不签发seal或策略准入。

两个只读implementation review未发现新生产阻断。已按建议修正负例：canonical FAIL的run仍
按原协议正常exit0并保留父调度关联，独立verify才strict BLOCKED/0DQ；manifest替换使用相同
JSON编码基线，旧API拒绝核对具体scope错误。新增14个actual candidate测试尚未执行，涵盖原
13场景及同路径错误request manifest SHA的bootstrap零DQ拒绝。预计新增27个parent dispatch/
13次synthetic DQ仅为静态计划，不是收据或实际计数；不可声称伪造有效seal后测试了错误pin。

当前源码只为下一正式candidate准备。完成shared generated/focused后提交source，再按原
source/final事务顺序绑定最终tree并运行actual E2E、正式tiers及Full；不继承S2b PASS，也不提前
宣称S2c.1已发布。source提交后终态先记录runtime receipts，由后继阶段同步canonical进展。
本波实际研究、真实DQ、capture/observation/maturity/scoreboard、数据和交易动作保持全0。

共享复核进一步收窄历史测试helper：S2c current phase仅允许exact4交集；未知后继测试补齐全部
历史markers并断言实际chronology拒绝，避免缺fixture提前失败。明确声明覆盖某source却没有
唯一current binding的后继立即拒绝，不能回退；无关section不受影响。新增outside4和未知后继
缺binding负例。原DEVX014/S2b测试未削弱；第一次67项选择器65 PASS/2 FAIL仅旧负例错误信息
匹配，保留原drift字串并新增明确原因后67 PASS/0 FAIL（7.16秒），v2/v3 XML原样保留。
新旧fixed probe字符串compile PASS；当前交易/真实研究计数不变。

共享151项首次146 PASS/5 FAIL（205.15秒，16/loadfile），保留source_shared_focused_v1.xml。
其中两项为新flow/catalog条目总数3164仍断言旧3162，一项为新增合同文档引用106仍断言105；
另两项为retained S2b-only source漂移现在精确触发index的AUTHORITY_GENERATED_STALE，
不是current S2c fragment的AUTHORITY_FILE_MISSING。已按各固定source分别收紧错误码及路径
断言，未接受任意错误、未修改validator/旧运行语义或deprecation政策；修正后重建和复验待完成。

预提交核对发现Atlas当前说明仍停在S2a，故在已声明的两项policy和精确测试内同步现状：
S1/S2a/S2b已工程发布，S2c.1尚在验证，S2c.2/S3待完成。历史task行与研究证据评估时钟保留，
不把当前工程检查时间当作新研究证据；不新增页面组件、schema或投资结论。源码提交后的实际
Atlas生成仍绑定该精确提交，不以当前说明冒称S2c.1正式发布终态。

## 8. 首次最终候选与单项架构断言修复

源码提交d94dd79675d902d3630820f108b84a4a7f0766d3已保留，parent/main仍为本文件冻结的
5c20c8fc3655e8a6e62a494122b785337a6c81de。提交前shared151 PASS、Atlas状态精确2 PASS、
Black/Ruff及contract strict mypy PASS；63个no-renames归因路径、49个rename-aware文件。
四生成与随后精确提交绑定的五生成通过；source事务按既有行政FAILED/RELEASED交接，Full=0。
final v1首次创建前因未登记的atlas-live-page名称被拒绝且未创建事务/lease，核对既有policy后
以正确atlas-authority创建，不改门禁。相关创建前拒绝已保留在final_admission_v1.json。

最终候选实际26项E2E全部PASS（602.26秒，16/loadfile），parent receipts43份，run25/verify18；
该批synthetic canonical DQ21，所有18次verify DQ0，真实市场DQ0。独立抽查T−1、T−2和缺TQQQ
三条完整链无阻断，57/8身份、旧接口拒绝/新价格接口成功、原共同窗口、bytes、context与strict
拒绝均保留。证据仅适用于该候选，不宣称五候选preview、feature readiness或OOS已实现。

首轮architecture-fitness为1055 PASS/1 FAIL（runner1682.60秒，16/loadfile），失败唯一为
test_devx_011_governed_workflow_health_authority_remains_historical：其live RCF successor
仍断言3162，而当前官方1373+564+1227=3164；fragment_count仍192。builder明确从当前RCF
index汇总该字段，非冻结历史合同。独立只读复核确认应只修正精确计数及对应两条新增entry注释，
保留workflow_health_contract、safety、Owner/status、192与所有历史合同，不改builder/策略/DQ。

失败summary、log与brief原样保留在outputs/validation_runtime/
trading-2564-s2c-final-v1-architecture-20260907/；summary SHA为
7ea9835b243df86e70cf07c90da0433793b1284b062a331203bd00013ad7b556。
final v1已按失败终态释放，未运行后续三tiers或Full，未移动main或push。它不是failed Full parent；
后续正式复验不能伪造Full parent或把本轮FAIL替换成PASS。

当前继续同root/branch，从d94dd796进入source v2：
trading-2564-s2c-price-scope-source-20260907-v2，lease-649ad271903cd7236086，LANE预检PASS。
canonical追加第22事件/cycle487，原21事件不改写且三条完整需求引用保留。
本次只修上述测试计数/注释与必要任务/需求及官方生成物。前置共享回归在原151项上明确加入
DEVX011、DEVX012 workflow-health和PROD004 cumulative-PIT authority三个精确测试，共154项。
这也作为既有S5实测提效的前置覆盖漏项记录，不改变tier组成、并行配置或Full门槛。

通过前置复验后继续既有source/final顺序，绑定新候选、实际E2E和全部正式验证；旧d94dd796
的actual PASS只作为保留证据。S2c.2接续只读笔记位于当前runtime root，尚未实施或授权真实采集。
本修复及当前工程波的真实研究/DQ/观察/数据/交易动作仍全0，heartbeat PAUSED，旧一次授权不复用。
