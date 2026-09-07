# TRADING-2564 S2c.2：固定五候选只读预览

日期：2026-09-07（Asia/Tokyo）。状态：IN_PROGRESS。
所属任务：TRADING-2564_LONG_TERM_RESEARCH_CAPABILITY_IMPROVEMENT_V1；P1。
Owner：Project Owner / integration-coordinator；模式：SINGLE_LANE，先审查最小接口合同，再串行接入 consumer。
冻结 main：b79974d6531c70deb18db153b3bd16c8f9e6e6ac。
Owner 本轮指令：在确认 S2c.1 已发布、S2c.2 为下一步之后要求「好的，继续推进吧」。

## 1. 已完成的前置与本波边界

S2c.1 的 exact candidate b79974d6531c70deb18db153b3bd16c8f9e6e6ac 已通过全部正式验证与普通
main push；final transaction trading-2564-s2c-price-scope-final-20260907-v2 的 closeout 为
PASS / COMPLETED / RELEASED。具体 SHA 和 Full 证据保留在
outputs/architecture/trading_2564_s2c_price_consumer_scope/closeout_verified_v2.json。
此前任务摘要停在架构断言修复阶段，本波追加终态与接续记录，不改写历史失败。

本波交付同一个零 DQ verifier 进程内的五候选纯预览函数与显式只读计划。它接收 verifier 签发的
VerifiedNamedInputs 和固定 typed scope，返回内存 DTO；不增加第二个 scheduler、执行器或
生产 CLI，不调用旧 forward-aging writer。后续受审 adapter 可在同一 verifier context 调用它。
真实市场 DQ、manifest replay、研究、capture、observation、maturity、scoreboard、收益/排名、
下载、cache mutation、provider、QuantConnect、Options、paper/live 和交易动作全部为0。
合成工程测试的 run/verify/preview 另计，不消费旧2563或任何真实研究的一次性授权。

## 2. 顺序与验收

1. 追加当前 task/本合同；先由独立只读审查复核身份、完整配置与算法边界。
2. 最小合同：在同一个 seal 中保留完整 captured execution dependencies；新固定 accessor 与
   新源码 manifest 只供五候选 consumer。合同聚焦验证通过后再接计算，不让并行消费者使用旧合同。
3. 纯 consumer：严格解析原始 prices/registry bytes、完整主窗口 session grid、历史长度判断、
   复用原算法、输出当前 target 与显式只读计划。不得从外部 path/mapping 补输入。
4. 决定性并行测试与独立实现复核；source/final 按既有发布流程形成精确候选，实际候选合成
   run→独立 verify→同 context preview 后才运行 formal tiers/Full。旧55/7及57/8 actual E2E保留。
5. 普通 main 发布、终态收据与归属清理。S2c.2工程完成后，S3真实前瞻、S4/S5仍未完成。

## 3. 身份与无 I/O 合同

- 旧 NamedEqualRiskPriceScope、receipt.v1、manual.v1、共同 evaluated_window 和
  prices_for_equal_risk_preview 的57/8精确 manifest 检查保持原义。
- 新 accessor 固定返回 captured prices 与完整 captured registry 两份 bytes，不接受任意角色或
  路径；只接受 config/data_governance/named_simple_baseline_preview_sources_v1.json 的精确
  path/SHA。新 manifest 在既有57模块/8依赖上加入两项纯 preview/result 合同模块，完整集合
  由独立测试枚举；paths-only manifest 的 SHA pin 不产生源码自引用。
- 零 DQ verifier 把 bootstrap 已捕获的 execution dependency bytes 交给原 seal，逐项核对
  receipt.execution_dependencies 的唯一完整 path、role、SHA、size。新接口对未捕获依赖、
  旧manifest、同数量替换、未绑定registry、跨PID/context、关闭context、普通对象及pickle均拒绝。
- 已有 named contracts/runner 不顶层 import 新模块，保持旧55/7与57/8闭包可在新候选验证。
  新模块只能依赖被新manifest绑定的源码，不临时导入 simple_baseline_forward_aging.py。
- consumer 不重新读文件、重跑DQ、访问网络、写输出或清理缓存。verifier在原calendar身份验证后
  用现有canonical XNYS函数生成完整scope session tuple、as_of是否session与下一session，随两份
  captured bytes封存在同一seal。consumer只读取该不可变witness，不调用global日历getter，
  不复制节假日算法；verifier完成后即使日历cache被清空，preview仍须零I/O成功。
- JSON result 是说明数据，不能反序列化成seal、运行许可或PIT/OOS证明。

## 4. 固定五候选与 registry

完整registry仍为 config/research/simple_baseline_strategy_registry.yaml，冻结本次受审原始bytes
SHA256=a4487d4477e7066943c3894e690d1dc9dbcb18bc28919eaa8cfb15371ef79843。
这是本版consumer的精确策略版本身份，不是可调阈值。今后registry变化需单独受审版本，不能
借caller提交同一path的新hash自动改变当前算法。

解析使用既有 load_strict_yaml_text，拒绝重复key、非字符串key、非有限数与循环alias。随后
核对固定顺序/角色、唯一alias、跨静态与动态来源的ID唯一性、完整三资产/三rates、当前主窗口、
所有计算窗口/频率/权重/安全字段与冻结registry身份；旧宽松默认、丢弃负权重后归一化不得承担准入。

|public candidate|role|resolution|原行为|
|---|---|---|---|
|equal_risk_qqq_sgov|PRIMARY_FORWARD_AGING|同名静态定义|60 returns样本波动率、输入shift1、逆波动/原限幅、monthly|
|qqq_50_sgov_50|STATIC_COMPARATOR|同名静态定义|50/50、monthly|
|qqq_60_sgov_40|STATIC_COMPARATOR|同名静态定义|60/40、monthly|
|100_qqq|RISK_REFERENCE|唯一alias→qqq_100_static|100%QQQ、monthly|
|dyn_tqqq_capped_trend|DYNAMIC_CHALLENGER|同名dynamic allocation|原trend/vol/drawdown与risk-on/off、daily|

复用 simple_baseline_portfolio_control.py::_target_weight_frame 和 _dynamic_candidate_strategies，
不修改原算法、registry、窗口、成本、阈值或已有策略结论；静态配置只作为比较基准，不称为新alpha。

## 5. 价格范围、历史长度与日期语义

- 使用完整2021-02-22..as_of主窗口，每个canonical XNYS session具有QQQ/TQQQ/SGOV三资产。
  as_of必须是真实session，不降级到不晚于请求日的最后一行。
- 先严格校验CSV唯一header/必需date,ticker,adj_close列、合法日期/ticker、唯一规范键和有限正价格，
  再普通pivot；拒绝重复/归一化碰撞、缺资产/内部或末端session、未来行与消费窗口内非交易日。
  窗口之前历史和其他合法ticker可明确排除并披露，不参与初始化。不调用pivot_table(last)或price ffill。
- 月频调仓在完整主窗口矩阵上确定真实定权月首，不截最后60行，也不在请求日/月中重新初始化。
- 历史长度由冻结算法依赖推导：equal-risk需60 returns加输入shift，即62价格行，且其实际定权
  月首必须已经满足；dynamic的20日realized vol再作252日rolling quantile，完整target需273价格行。
  这些是算法可计算性不变量，不是新的统计样本门槛；静态候选不附加统计lookback。
- 足够历史的真实零波动仍执行原50/50，不当作历史不足；任一冻结候选的必要历史不足，返回typed
  阻断且不发布部分候选建议。测试分别核对每候选的依赖边界与月首状态。
- 输出target_session、target_rebalance_session、实际signal_price_through与原收益端另一次shift1
  对应的下一session语义。不得计算收益或把after-the-fact preview称为当时已存在的信号。
  equal-risk的signal_price_through是实际定权月首前一个session，dynamic是as_of前一session；
  固定权重的统计信号日期为null。下一session只表示理论applied日期，不证明实际执行。
- 结果暴露requested/evaluated/实际消费范围、输入和registry hash、receipt/dispatch/code identity、
  Python/pandas/numpy版本及数据质量PASS；PIT/available-at/activation仍NOT_ESTABLISHED。
  rates仅为原DQ guard；分别合法通过DQ的rates变化不得改变五候选权重。

## 6. 决定性验证

1. 五候选完整矩阵逐值、月初/月中/跨月、lag与dynamic跨状态等价；真实零波动不混同历史不足。
2. equal-risk61/62与月中达到62但月首未就绪；dynamic272/273；非法as_of、未来/缺失/重复/非有限/非正
   价格、header冲突；registry候选/角色/alias/ID碰撞、窗口/频率/权重/安全字段变化。
3. 每次成功或失败consumer调用期间禁用文件/网络/DQ/旧writer；不输出收益、排名或OOS观察。
4. 原12项55/7、14项S2c.1实际候选E2E保留；新真实候选Git编译闭包下，synthetic run DQ1→
   独立verify DQ0→调用真实preview→terminal/postguard。同一T/T−1收据共同window不变。
5. 完整manifest精确集合/缺consumer/替换模块、registry path/role/hash/size/缺依赖、跨context与旧
   receipt混用负例；计数不代替身份，测试probe不称为生产执行器。
6. 所有pytest为16 workers/loadfile；Full只绑定最终候选。历史失败与其原始产物保留。

## 7. 生命周期与启动记录

复用 D:/Work/AITradingSystem_devx014_source_preservation，branch
codex/trading-2564-s2c2-five-candidate-preview-v1；不新建worktree/clone/cache。原始证据与旧
D:/Work/AITradingSystem_trading2559_integration 的18项dirty源码保持既有保全条件。
本波runtime记录进入 outputs/architecture/trading_2564_s2c2_five_candidate_preview；next owner为
integration-coordinator。退出条件：实现已正式发布、必要证据canonical保留/hash验证、无unique
未审源码/进程/运行依赖后清理；有后续S3依赖或唯一历史证据时保留root并记录具体理由。

- START首次未声明scope得到SINGLE_LANE_SCOPE_REQUIRED；补齐显式声明后PASS，未修改代码。
- source v1第一次acquire将工具自动添加的两项resource再次声明，创建前PUBLICATION_PATH_DUPLICATE。
  第二次使用共享venv时误载旧主checkout editable源码，旧arbiter要求owner.json而本工作区已采用
  OS锁，LEASE_ARBITER_STATE_INVALID；没有active lease/transaction，也未删除或修复锁。
  保留该intent；直接修正进程PYTHONPATH为当前worktree/src并核对实际loaded module路径。
- source v2基于同一exact main取得lease-057a666c69268a9bfefd并进入TASK_SOURCE_PRE_WRITE。
  后续工具统一显式使用当前worktree/src，禁止用旧checkout代码治理新工作区。
- 两路独立只读合同复核已完成：无阻断项；补齐原candidate role、同seal calendar witness与不同
  signal日期说明。LANE（SINGLE_LANE/contract-change、全部归属路径与精确lease）PASS后进入串行实现。

## 8. 实现审查与验证进展

独立实现审查指出结果DTO可能携带scope/registry/dispatch错配或内部可变容器，已加入纯cross-binding、
精确nested tuple/date/环境键检查。极大但有限价格可能使原rolling MA溢出为NaN，旧comparison会
自然转risk-off；本consumer在调用原算法前检查所有dynamic operands有限并typed阻断，不改变旧
策略算法。UTF-8、CSV语法及错误universe类型统一返回领域error code。

初始旧合同聚焦251项通过。新增计算测试首轮59 PASS/1 FAIL：测试错误要求equal-risk包含原算法
本来省略的TQQQ零权重列；已修正断言，不改变生产计算。失败XML原样保留。完整聚焦重跑与exact
candidate合成E2E继续按16 workers/loadfile执行；真实市场DQ、研究、provider与交易动作均为0。
第二轮聚焦310 PASS/1 FAIL再次发现测试对flat dynamic的预期错误：原inclusive比较进入risk-on，
其完整权重是TQQQ=.33/SGOV=.67/QQQ=0；断言已按原registry与独立只读探针纠正。第三轮143
PASS/1 FAIL仅为mutable-dependency负例的error regex多了必需空格，实际拒绝行为正确；已精确修正。
补入未限幅的逆波动内部权重/月中保持/下月更新，以及极大整数权重的typed拒绝，避免只测试clip端点。

兼容性继承追加exact S2c2 33项源码闭包（旧26项加明确7项），保留S2b→S2c→S2c2顺序；历史受限
source交集仍只有docs/system_flow.md与三个既有architecture tests，未知future phase不得替代。

已完成聚焦修正复跑：`contract_preview_focused_v4.xml` 144 PASS；新两模块mypy与全部14项当前
Python变更的Ruff/Black检查通过。最终E2E、formal tiers与普通发布仍须绑定精确提交，不以这些
局部结果代替。官方source generator probe的模块/测试文件计数1209/1370，RCF为3167 entries，
均只更新可复核当前inventory/pins，不改变历史验证门槛或runtime writer允许数。

source 9f0fabb19bbb68e0ed0240931c3ddcad3387add0 的五项正式generator与36项actual candidate
E2E全部PASS（727.37秒，16 workers/loadfile）。独立只读复核逐项确认59模块/8依赖的path、SHA、
size、Git blob、bootstrap、request/receipt/dispatch/preview与calendar身份，run DQ1、verify DQ0。
完整review证据保留在本波runtime root。审查要求将该身份复核固化为永久断言，精确区分五种负例
拒绝码，并在consumer期间禁用canonical DQ函数及worker alias。生产计算不变。

final v1在Full前以行政superseded终结，无Full失败或Full dispatch；source v3继续复用同一工作区，
补强测试后重新冻结候选。LANE首条命令误用不存在的--expected-lease-id参数而在argparse退出，
未执行preflight或源码修改；使用正式--allow-active-lease重跑PASS，保留原命令结果。
S2c.2工程实现已完成，最终正式验证与发布结果由本波closeout收据承载；TRADING-2564仍为
IN_PROGRESS，S3/S4/S5、真实前瞻、PIT/OOS准入不由本波完成。
