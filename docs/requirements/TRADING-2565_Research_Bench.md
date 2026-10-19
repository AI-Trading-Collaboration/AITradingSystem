# TRADING-2565：研究评估台（试验登记和多重检验校正）

## Owner decision

- 依据：`owner_decision:TRADING-2567:2026-10-12:qqq_direction_1999_window`。评估台是 TRADING-2567 的 S3 阶段，本任务随它激活。Baker 定的研究节奏不限同时进行的问题数，靠这里的试验登记和多重检验校正控制假阳性。
- 第一批题目的顺序和成功标准：`owner_decision:TRADING-2567:2026-10-13:risk_first_question_order`（记在 TRADING-2567 需求文档）。评估台的主指标按它定：同等平均敞口下的回撤、左尾和复利分散在前，超额收益在后。
- 每一批都是 C 区，Baker 按结果 tree 事先批准。
- S4（信号筛选层）：owner_decision:TRADING-2565:2026-10-13:signal_screen_layer。Baker 10-13 19:49 JST 在研究线程的卡上选"先建"：第二题开题前先建开发窗口内的信号筛选层（数据入口、筛选和全局账、向量化证伪和减仓事件表三批）；19:55 JST 批准规则默认值：所有市场的筛选数据截止 2010-12-31；候选要 QQQ 方向一致、至少 2/3 的市场方向一致、美股大盘按全部筛过的信号做 BH 修正（q = 0.10）后仍显著、剔除任一个十年方向不变；FRED 日度序列晚 1 个交易日算已知；减仓事件表默认值见 config/research/research_screen.yaml。S4 的 C 区设置放在新文件 config/research/research_screen.yaml，research_bench.yaml 不变。
- 评估台配置的 C 区登记：owner_decision:TRADING-2565:2026-10-14:bench_config_zone。Baker 10-14 13:22 JST 在「减少粘贴和批准步骤」线程的卡上选"全部照做"，其中第 4 条是评估台配置的 C 区登记一次做完，按 tree 批最后一次。`config/gov008_ship.yaml` 1.2.2 把 `config/research/research_*.yaml` 按通配划进 C 区，评估台读的成本模型 `config/research/transaction_cost_model.yaml` 也划进 C 区（以前在 B 区）。以后评估台的新配置文件命名为 `config/research/research_<名>.yaml`，落在通配下，不用再改 `config/gov008_ship.yaml`，可以走决定记录。旧的研究治理政策 `config/research/research_governance_policy.yaml` 随通配从 B 区进 C 区。`config/research/qqq_long_history.yaml` 和 `research_data` 的代码仍在 B 区，数据由快照 id 和检查钉住。

- 研究吞吐（S4d 和研究记录快车道）：owner_decision:TRADING-2565:2026-10-13:research_throughput。Baker 10-13 20:12 JST 在研究线程的卡上选"两项都做"：评估台内置通用的"信号 → 仓位"策略，一道题的策略只是预注册里的配置（S4d）；研究记录走快车道，由第二个 pi 会话在单独的工作区跑研究命令、按天提交只追加的记录。两项都排在 S4 之后、第二题开题之前。
- 研究自动循环第 1 期章程：owner_decision:TRADING-2565:2026-10-15:research_loop_p1。Baker 2026-10-15 20:56 JST 在「研究自动生成和初筛」线程对章程草稿回复「同意」。章程原文是 `research/_loop/charter_p1.md`：期限两周；问题是 QQQ 的仓位规则和同平均敞口的固定仓位比回撤、左尾和几何年化；白名单是 4 个 FRED 日度序列（VIXCLS、VXNCLS、DGS10、DTB3，晚 1 个交易日算已知）；每期新筛信号不超过 10 个，每个候选的试跑配置不超过 8 个，验证窗口每期最多读 1 次，同一窗口第 k 次读按 0.05/k；试跑的门在试跑模式做好以后才用；筛选和试跑淘汰的想法只进筛选账和试跑账，不进负结果清单。这条决定在 C 区只批准一件事：在 `config/research/research_screen.yaml` 的 `sources` 加入这 4 个来源，连同钉住来源清单的不变量测试 `tests/invariants/test_research_sources_invariants.py`；由一条决定记录批准，有效 14 天。评估台、筛选规则、门槛和别的 C 区文件不在范围内。
- 试跑模式：owner_decision:TRADING-2565:2026-10-15:trial_mode。Baker 2026-10-15 23:39 JST（14:39:01Z）在「研究自动生成和初筛」线程的卡上选「先建试跑」（卡上的问题是 industry_corr_63 过了初筛，先建试跑模式再试它，还是现在开第四题）：先建试跑模式，建好后候选在开发窗口上自动试跑，主配置过门才叫 Baker 评估，不动验证窗口。试跑按第 1 期章程第 6 节的门做：门写在 `research/_trials/prereg.yaml`（C 区），QQQ 在 1999-03-10 到 2010-12-31 上用第三题的 11 条规则，美股大盘（Ken French 日度市场收益）用回撤、左尾、25 bps 成本和推迟两天的左尾 4 条规则；每个候选最多 8 个配置，只有主配置决定，每个配置都计入全局总试验数；快跌段只列不判，章程写的 4 段之外加上评估台原有的 2000–2002 和 2007–2009 两段。和章程不同的一处：美股大盘的起点由 1927-07-01 改为 1929-01-02，因为按扩张分位打分的信号要先攒 252 个交易日再加自己的回看，从 1926-07 的数据起算，1927-07 多数信号还没有值；1929 年 10 月的快跌仍在窗口里。这条决定在 C 区批准两批：美股大盘的数据和开发窗口（TRADING-2565-15）、试跑的门、计划、记录和命令（TRADING-2565-16），路径是 `src/ai_trading_system/research_bench/` 下的 `data.py`、`run.py`、`trial.py`、`__main__.py`，`config/research/research_window_registry.yaml`、`config/research/primary_research_window_policy.yaml`、`research/_trials/prereg.yaml` 和 `tests/invariants/test_research_ledger_invariants.py`；由一条决定记录批准，有效 14 天。验证窗口、筛选规则、`config/research/research_bench.yaml`、`config/research/research_screen.yaml` 和别的 C 区文件不在范围内。
- 查尺子和第 1 期章程的名额：owner_decision:TRADING-2565:2026-10-16:gate_power_check 和 owner_decision:TRADING-2565:2026-10-16:loop_quota。Baker 2026-10-16 09:58 JST（00:58:10Z）在「研究自动生成和初筛」线程的卡上选「先查尺子」（卡上原话："埋已知效果大小的假信号测门的检验力，同时写换问题的一页设计；暂停新择时卡，不读验证窗口、不买数据。"）；10:01 JST（01:01:06Z）在同一线程的卡上选「按建议改」（"新信号每期 10 改 20;从章程起算、没有候选过试跑门的信号每满 20 个复盘一次但不停;验证窗口和配置上限不变。"），改动写进 `research/_loop/charter_p1.md` 第 11 节，查尺子之后生效。
- 两段尺子：owner_decision:TRADING-2565:2026-10-16:ruler_two_stage。Baker 2026-10-16 19:46 JST（10:46:59Z）在「研究自动生成和初筛」线程的卡「选改尺子的方式:两段尺子、只减规则、前瞻记录,还是先收尾?」上选「两段尺子」（卡上原话："长历史上过核心规则当主检验,验证窗口只查方向;先用探针量到 L 有八成把握、误放不到 5% 再改门,C 区你批一次。"）；卡上写明新择时卡在尺子定下之前继续暂停。
- 只认 40 年：owner_decision:TRADING-2565:2026-10-17:ruler_40y。Baker 2026-10-17 01:37 JST（2026-10-16 16:37:55Z）在「研究自动生成和初筛」线程的卡「选两段尺子的下一步:只认 40 年、先换种子确认,还是收尾不改门?」上选「只认40年」（卡上这个选项的原话："第一段用 2010 年前至少 40 年，尺子 core4@0.95+S3；我写冻结说明和试运行门的 C 区改动包，你批一次，门改好后恢复时点卡。"）。两段尺子探针按写定的读法判为 below_80（30+10 年 L 两段都过 14/21，线是 17；40+10 年 16/16），只认 40 年是看完结果才拿掉 30 年那档的读法，由 Baker 定。
- 第一段加最小左尾效果：owner_decision:TRADING-2565:2026-10-19:tail_floor10。误放率探针（TRADING-2565-22）按写定的读法判为偏松以后，Baker 2026-10-19 01:57 JST（2026-10-18 16:57:33Z）在「研究自动生成和初筛」线程的卡「收紧门（左尾至少好 10%）再读一次这次结果，还是先只在后台筛？」上选「收紧重读」（卡上这个选项的原话："你把说明贴进研究线程一次，几分钟出数；够严我打包改门、你批一次，择时卡恢复；不够就继续停。"）。候选门和读法在重读之前写定：第一段四条照旧，再加 `comparison.cvar_5pct_relative_improvement >= 0.10`（左尾点估计至少改善 10%，即 `research/families.md` 写下的门的价），第二段照旧；读法的线和探针相同，另要对照 vol_noise_1 两段都过不少于 16/20。重读判为可用，所以这条决定批准把这一条加进试跑门 `research/_trials/prereg.yaml`（C 区，TRADING-2565-24，由一条决定记录批准）；门上 main 以后，第 1 期章程第 4 节的每周新择时卡和择时候选的试跑恢复，新信号每期 20 个。

## 为什么

2026-05 到 09 的约 860 个研究任务里，没有一条正面结论达到 A 或 B 级（`research/ledger.md`）。原因里和评估有关的有：
- 至少 7 个各自实现的模拟器，成本默认值有 0、4、5、5–15 bps 四种；
- 对照组各不相同，有的是看了结果才定；
- 常量冒充结论、参数没被评估器读到，几周后才发现；
- 试验次数从来不计；
- 验证窗口被反复读。

评估台把这些集中到一处，并用不变量测试守住。

## 设计

- **策略接口**：策略是 `(inputs, params) -> pd.Series`，对每个交易日给出目标敞口（对 Nasdaq-100，0 到 3 倍），只能用当天收盘及以前的数据。
- **执行时序**：决策日 t 的目标在 t+1 收盘成交，第一笔收益是 t+1 到 t+2；`delay_days` 再往后推。策略代码不许自己移位。
- **持仓**：
  - 敞口落到能买到的组合：0–1 倍是 QQQ 加现金，1–2 倍是 QQQ 加 2 倍，2–3 倍是 2 倍加 3 倍。
  - 持仓随价格漂移；调仓按日、周末或月末读决策，可设无交易带（偏离目标超过一定比例才调）。
- **成本**：单边换手乘统一成本，即 `config/research/transaction_cost_model.yaml` 各项之和，目前 4 bps。
- **基准**（固定，运行后不能加）：
  1. 平均敞口相同的固定敞口（主对照）；
  2. 买入持有 QQQ；
  3. 现金；
  4. 用杠杆的题目另加固定 2 倍、3 倍。
- **指标**：
  - 风险一侧：最大回撤、最长回撤天数、最差 1% 和 5% 日收益的平均（CVaR）、最差月、滚动 3 年几何年化的第 10 百分位、固定下跌段（按月份写死在配置里）的损失；
  - 收益和代价：几何年化、波动、对现金的夏普、平均敞口、年换手、年成本；
  - 相对主对照：少赚的上涨和躲过的下跌（负结果清单要求降风险方案同时报告）。
- **每次运行前的检查**：
  - 前视：在窗口里取 20 个日期，把之后的数据反转、缩放并分别上移和下移，目标不能变。
  - 参数消费：声明的数值和布尔参数，改了必须让窗口里某天的目标变化。
  
  不通过就不模拟。
- **数据**：只经 `research_data.qqq_long_history.load_daily` 读长历史快照。运行记录带快照 id、检查状态和 WARN 项；QQQ 用代理还是真实 QQQ 由运行指定。
- **窗口**：从 `primary_research_window_policy.yaml` 的 `question_windows` 和窗口登记读。窗口前的数据只作信号回看。
- **可复现**：运行结果带指纹，即规格、窗口、数据来源、配置哈希、成本和全部指标的 sha256。

## 分批和验收

| 批 | 内容 | 验收 |
|---|---|---|
| S3a | 模块 `src/ai_trading_system/research_bench/`、配置 `config/research/research_bench.yaml`；命令行 `python -m ai_trading_system.research_bench benchmarks`；只开开发窗口；评估台划进 C 区 | 不变量测试（`tests/invariants/test_research_bench_invariants.py`）：基准恒等、前视、参数消费、指标不是常量、成本、时序、可复现、和旧模拟器对得上；每条有反向对照，并有记录的变异证据（`tests/invariants/README.md`） |
| S3b | 运行记录 `research/<题号>/runs/`（失败的也记，带上一条的哈希）；按题和全局计数；正式的折算夏普（Bailey、López de Prado 2014），带手算的固定用例；验证窗口读取登记（每题一次，预注册的 owner decision 在 main 之后才能读，一致性窗口在验证之后）；预注册格式 `research/<题号>/prereg.yaml` 划进 C 区 | 原任务卡的验收：研究运行默认自动登记，未登记的结果不能进入结果准入；报告第一屏显示试验次数和校正后指标；校正实现有固定数值用例 |
| S3c | 标准证伪：成本 4、15、25 bps；推迟 1、2 天；逐年和逐段剔除；同敞口随机择时（21 天一块打乱 1,000 次）；平稳块 bootstrap（平均块长 21 天）。另有报告、`research/README.md` 的流程、pi 研究运行的 brief 模板 | 证伪项在固定样例上的数值用例；报告只含汇总 |

## S3a 的实现（2026-10-13）

- **模块**：
  - `settings`：配置、统一成本、窗口解析；
  - `data`：快照到资产收益和信号输入，T-bill 利率滞后一天；`ndx_return` 在 QQQ 序列开始以前接价格指数收益，让回看能伸到开发窗口之前（TRADING-2567-09 加）；
  - `simulate`：敞口映射、调仓规则、漂移、成本；
  - `metrics`；
  - `checks`：前视、参数消费；
  - `run`：检查、模拟、基准、指标、指纹；验证和一致性窗口抛 `WindowNotOpenError`（S3b 起要台账发的许可）；
  - `__main__`：`benchmarks` 命令在开发窗口上跑四个固定基准，只打印汇总和用时，用来核对数据和评估台，不算研究试验。
- **区**：`src/ai_trading_system/research_bench/**` 和 `config/research/research_bench.yaml` 加进 `config/gov008_ship.yaml` 的 C 区，pi 守卫读同一份配置。
- **和旧模拟器的关系**：时序和旧的 `simple_baseline_portfolio_control._strategy_return_series` 相同；同一份价格上，200 日均线全进全出规则逐日一致（不变量 10）。区别有两点：
  - 持仓在两次调仓之间漂移。旧模拟器隐含每天回到目标权重，又不计这部分换手。
  - 成本按乘法从调仓当天扣除。
- **用时**：云端合成数据上，一次完整运行约 0.3 秒，包括 40 次前视重算、参数检查、策略和 3 个基准的模拟。

## S3b 的实现（2026-10-13）

- **一题的目录** `research/<题号>/`：
  - `prereg.yaml`（C 区）和它指定的策略模块；
  - `runs/0001.json`、`0002.json`……：每次运行一条，失败的也记。每条带上一条的 sha256（换行符按 LF 算），改或删一条，后面就对不上；
  - `window_reads.jsonl`：验证和一致性窗口的每次读取一行，同样链起来。
  - 格式的样例在 `research/_template/`（`prereg.yaml` 带逐项说明，`strategy.py` 是策略接口的样例）。名字以 `_` 开头的目录不算题目。
- **预注册**（`prereg`）：pydantic 校验。字段有：
  - 题号、问题、问题类型、批准它的 owner decision；
  - 固定的快照 id（不能是 LATEST）和 QQQ 序列；
  - 策略模块，以及每个策略的函数、参数、调仓、推迟天数、是否加杠杆基准；
  - 验证哪些策略、每个策略的判定规则（`<部分>.<字段> <比较> <值>`，部分是 strategy、comparison、exposure_gap）；
  - 敏感性变体、已有研究的预期（效应、失效环境、来源）、样本地板、试验上限、停止条件。
- **运行和登记**（`ledger.run_and_record`，命令行 `run`、`validate`、`consistency`）：
  - 开发窗口随时可跑，可以改参数。每个不同的配置算一个试验：策略、策略模块的内容、参数、调仓、推迟、成本、QQQ 序列。重复的配置记录但不算新试验；新试验超过上限就拒绝，什么都不跑、不记录。
  - 验证窗口每题只读一次。要求 `prereg.yaml` 和策略模块与 main 上的一致，且 main 的 `docs/requirements` 里有 prereg 写的 owner decision（`config/research/research_bench.yaml` 的 `approval_ref`）。先登记读取，再跑；一次读取只许每个验证策略各跑一次，参数只能是预注册的。
  - 一致性窗口只能在验证窗口读过之后读，条件相同。
  - 评估台本身（`run_strategy`）没有台账发的许可就不开验证和一致性窗口。
  - 运行记录只有汇总指标和出处（commit、代码是否改动、快照、配置哈希、策略模块哈希），没有逐日序列。
- **试验计数和折算夏普**（`deflated`，命令行 `ledger`）：
  - 第一屏是每题和全局的开发试验数、上限、读取记录、链是否完好，以及每次运行的年化夏普和折算夏普。
  - 折算夏普按 Bailey、López de Prado（2014）：试验数用全局开发试验数，方差用各试验（每个配置取最近一次成功运行）日夏普的样本方差，偏度和峰度来自这次运行的日超额收益。和论文第 4 节的例子对得上（SR0 0.1132，DSR 0.9004），另有一个手算用例。
- **结果准入**：`research/<题号>/decision.md` 只能引用登记过的运行和读取，并且至少引用一次运行。仓库里所有题目的链、读取和引用由不变量 17 检查。
- **防删改的边界**：链只能让已提交的记录被改或被删时看得出来。不变量 17 查 git 历史：运行记录只许新增，读取记录只许追加。提交之前在本机删掉文件再重跑，链看不出来。S3c 的 pi 研究 brief 模板会把验证窗口的读取放在单独一次运行里，由运行报告列出改动的文件。
- **区**：`research/*/prereg.yaml` 加进 C 区（模板也是）。策略模块、运行记录和读取记录留在 A 区，靠哈希链和 git 历史检查防删改。更正（2026-10-13，TRADING-2566）：策略模块 `research/*/*.py` 也划进 C 区。留在 A 区时，验证读取只核对它和 main 上的一致，开题批准之后的 A 区改动可以改掉它而不需要新的 owner decision。
- **不变量**：`tests/invariants/test_research_ledger_invariants.py` 第 11 到 17 条，每条有反向对照；13 处变异都被抓到（`tests/invariants/README.md`）。

## S3c 的实现（2026-10-13）

- **标准证伪**（`falsify`）：每一项都和主结果一样，拿策略和同平均敞口的固定敞口基准比，只存汇总。
  - 成本：按 4、15、25 bps 各重跑一次。4 是统一成本，所以 `cost_4` 就是主结果。
  - 推迟：每笔交易再晚 1 天、2 天，各重跑一次。
  - 逐年和逐段剔除：把一个日历年或一个固定下跌段，从策略、基准、现金三条日收益里同时去掉，再比一次。记录每年、每段的结果，以及每个指标在这些剔除里的最小值和最大值。
  - 同敞口随机择时：把策略实际执行的目标敞口路径切成 21 天一块，随机重排 1,000 次，每次按策略同样的调仓、成本和推迟重新模拟。敞口分布和策略一样，只有时点是随机的。报告策略在这些随机择时里的排位（0 到 1，平局算一半）。
  - 平稳块 bootstrap（Politis、Romano 1994）：平均块长 21 天，10,000 次，把策略、基准、现金的日收益成对重抽，给出差值的双侧区间。置信度默认 0.95，预注册可以改（例如按子问题个数做 Bonferroni）。
  - 剔除、随机择时、bootstrap 都在日收益上算，几何年化按 252 个交易日。主结果的几何年化按日历年算，所以两者会差一点。
  - 种子固定（配置的 `falsification.seed`），同样的输入得到同样的结果。
- **什么时候跑**：
  - 验证和一致性窗口的运行总是跑，关不掉。
  - 开发窗口加 `run --falsify` 才跑，不算新试验。
  - 一次完整证伪在云端约 45 秒，大部分花在 1,000 次随机择时的模拟上。
- **判定规则能读证伪结果**：
  - 可用的部分：`paired`、`ci_low`、`ci_high`、`leave_year_min`/`_max`、`leave_episode_min`/`_max`、`placebo_rank`、`cost_<bps>`、`delay_<天>`。
  - 例如 `ci_low.max_drawdown > 0`：在 bootstrap 区间的下端，回撤仍比基准浅。
  - 规则写了运行不会产出的指标，登记验证读取时就拒绝，不会白白用掉唯一的一次读取。规则能写的指标和运行实际产出的字段由测试核对。
- **报告第一屏**（命令行 `report <题号>`，只用运行记录里的汇总）：
  - 题目、快照、owner decision；本题和全局的试验数、读取、链是否完好。
  - 每个策略在每个窗口最近一次成功的运行：折算夏普，以及和主对照的比较表。表按风险优先排：回撤、5% 和 1% 左尾、滚动 3 年低点、几何年化、夏普、波动。每行带 bootstrap 区间、逐年和逐段剔除的最差值、随机择时排位。
  - 成本和推迟的重跑结果，判定规则逐条是否成立。
- **pi 研究运行的 brief 模板**：`research/_template/brief.md`。
  - 开发和验证分两次运行。验证那次只运行 `ledger`、`validate`、`report`，运行报告里的改动只应有新增的运行记录和一行读取记录。
  - pi 不打开数据文件和运行记录，结果只看 `report`。
  - 每个要验证的策略，开发时至少用预注册参数跑一次 `--falsify`，先确认证伪能跑通。
- **和旧代码的关系**：思路来自 Composer v2 的证伪合同（TRADING-2555~2559，已归档）和 `backtest/robustness.py` 的同敞口随机、块 bootstrap。那两处接的是旧回测的结果类型，逐日打乱，只算总收益。评估台按自己的接口重写：按块打乱，保留敞口的短期持续性；bootstrap 成对重抽，覆盖全部风险指标。
- **不变量**：`tests/invariants/test_research_falsify_invariants.py` 第 18 到 21 条，`test_research_ledger_invariants.py` 第 22 条，每条有反向对照，例如知道下一天收益的策略、只在一年或一段里赚钱的策略、比基准每天多一点固定收益的策略。变异证据在 `tests/invariants/README.md`。

## S4c 的实现（2026-10-13）

- **范围和后续**：本批 TRADING-2565-05 只做向量化随机择时和减仓事件表；之后 S4a 做数据入口，S4b 做筛选和全局账。S4a/S4b 的开放问题和逐批验收待各批需求细化，本批不引入筛选或晋级判定。
- **向量模拟**：`simulate_many` 接收按完整收益日历对齐的目标矩阵，一行一条路径；只按交易日迭代，路径一起算。开仓目标、可执行日期、范围检查、持仓漂移、调度、无交易带、延迟和乘法成本与未改动的 `simulate` 一致。亏光和不可用目标报错带路径号。随机择时按原来的 `placebo_targets` 顺序分块送入它，再作成对比较；随机数顺序不变，与逐条模拟的差不超过 1e-12。速度用 3,000 个交易日、1,000 条路径、月度调度的合成数据对照，验收要求批模拟少于 5 秒；本机逐条 19.082099 秒、批模拟 0.153203 秒（约 124.55 倍），净收益最大绝对差为 0，完整条件记在运行报告。
- **事件定义**（`events`，配置 `screen_settings`）：
  - 参考敞口是窗口里实际持有敞口的均值。持有敞口不大于参考的 `derisk_ratio` 倍（数值容差 1e-12）算减仓日。
  - 连续减仓日成段；两段之间非减仓日少于 `merge_gap_days` 则合并，事件含中间的全部交易日。
  - 每个事件记 ISO 起止日期、交易日数（含首尾）、最小持有敞口、段内 QQQ 日收益连乘减 1；收益不大于 `protective_loss` 为 protective，不小于 `false_alarm_gain` 为 false_alarm，否则 neutral。
  - 汇总参考敞口、阈值、按时序的事件、事件数、每年事件数（按实际收益日数和 `trading_days_per_year`）、短事件数（严格短于 `short_event_days`）。三类各记次数和该类全部事件日上的 QQQ 连乘收益，无事件为 0.0。
  - 固定下跌段沿用 bench 的月份区间；数据无日期的段省略。每段记录交易日数、减仓日占比、非减仓日的 QQQ 连乘收益（`missed_loss`）、首次减仓相对段首的交易日延迟（无减仓为 None）、从段末当天起首次达到参考敞口的交易日延迟（直到数据结束仍未达到为 None）。合并事件内的非减仓日仍按非减仓日计入这些段内统计。
  - 默认值：0.75、5 天、-0.05、0.05、21 天；均为 owner 批准的描述性研究规则，不是生产或候选准入标准。设置冻结、拒绝未知字段并校验范围，单独文件计算 sha256，`research_bench.yaml` 保持字节不变。
- **记录与报告**：每次运行按自己的持有敞口和同日 QQQ 收益生成表。`RunResult.events` 和 ledger 的结果记录保存事件；和 falsification 一样，`events` 不进结果指纹或试验判定键。`report` 每个策略每个窗口的最近成功运行增加“减仓事件”小节，只用已记录汇总；旧记录没有该字段时写“无（S4c 之前的运行）”。
- **验收**：Claude 提供且不修改的 `tests/invariants/test_research_vector_events_invariants.py` 守住不变量 23（批模拟逐项等价）、24（亏光停止并点名）、25（随机择时结果不变）、26（手算事件及段内延迟）、27（全部设置生效、默认值与指纹隔离）。反向对照和四项变异检查记录在 `tests/invariants/README.md`；合成测试、静态检查、任务检查、全量测试和 zone C dry-run 的结果记在运行报告，Baker 按 tree 批后才发布。

## S4a 的实现

- **来源规格**：`research_bench.sources.SourceSpec` 冻结并拒绝未知字段，校验来源 id、daily 频率、非负交易日滞后；FRED 至少晚 1 个交易日，Ken French 只允许批准的日度因子和 49 行业组合。`screen_settings.sources` 拒绝重复 id；四个来源配置在 `config/research/research_screen.yaml`，原 `research_bench.yaml` 字节不变。
- **已知时点规则**：`known_at` 先去掉 NaN，再按第 i−lag 个交易日取不晚于参照日的最后观测；缺失值不重置观测年龄。观测日在交易日历中的位置用 `searchsorted(..., side="right")−1`，包括非交易日；早于参照日超过 5 个交易日的位置则丢弃，正好 5 日保留。这是本批批准的数据缺失规则，不是信号筛选或候选准入标准。
- **快照和检查**：`research_data.signal_sources fetch|check [--snapshot ID]` 使用 `state_dir`，代码根为 `PROJECT_ROOT`，研究工作区通过 `AITS_STATE_ROOT` 读取本机状态。每来源一个 CSV，保留抓取顺序、重复和缺失；Ken French 只读首个数据块，缺失码转 NaN，百分比转小数。规范 JSON 清单带规格、文件、sha256、行数及首末日期，以其 sha256 前 12 位生成 `sig-...`，抓取时间单列 `fetch.json`，不影响 id。
- **质量门禁**：检查文件哈希、日期递增且唯一非空、列非空比例；全空、文件不匹配、重复和乱序为 FAIL，相邻日期间缺失工作日严格超过 5 为 WARN，否则 PASS。`check.json` 和 `check.md` 只含 id、状态、计数、日期、比例。`load_snapshot` 拒绝未检查、FAIL 或校验和漂移，只返回按日期索引的原抓取值。快照清单内容地址也复核，WARN 可读但不是候选通过。
- **数据规则**：数据只在本机、不提交、不打印数据行、不发给外部模型。本批只用 Claude 原样提供的合成验收数据，不联网下载；真实 fetch 留待发布后单独执行。验收不变量 28–33 与五项变异证据见 `tests/invariants/README.md`。本批仅实现数据入口，不实现 S4b 的筛选和全局账，不改变 production/broker 边界。

## S4b 的实现

- **模块和规格**：`research_screens/signals/<id>.py` 和 `<id>.yaml`，函数参数为 `(returns, sources, params)`，返回同索引实数 Series（NaN 可留，无穷拒绝）。规格冻结、拒绝额外字段；id 等于文件名，机制和文献说明非空，参数和预期方向都写进规格。模块名包含模块哈希，不放进 `sys.modules`；模块和规格各按 LF 规范后的 SHA-256 钉住。模块在 B 区，本批不放真实信号。模块只能用传入的数据，不可自行读文件、联网或用外部状态；检查不是沙箱。
- **截止和检查**：设置在 `research_screen.yaml.screen`（C 区），最晚截止 2010-12-31，QQQ 评估从 1999-03-10 开始，长市场从 1927-07-01 开始。每个市场收益和声明的来源先截到截止日，再逐列 `known_at` 对齐，仅传声明来源。在配置的自助法市场上按 20 个切点跑 `check_no_lookahead`，在主市场跑参数消费检查；QQQ-only 用 QQQ 做参数检查。未知类型参数列为 unchecked；任一检查失败不记账、不进家族。市场日历须递增唯一，收益可 NaN、不得无穷或 ≤ -1；输出形状和数值类型也检查。
- **统计**：持有期默认 21、63、126 个交易日，t 的前向收益是 t+1..t+h 的对数收益和，风险是同段样本标准差乘 sqrt(252)，尾部不足 h 天为 NaN；任何缺失收益都会使该段前向值缺失。评估日要求起点以后且信号/目标有效。IC 是平均秩 Pearson，n_eff=days/h；MDE=(z_0.975+z_0.80)/sqrt(n_eff)，只是检测能力提示，不额外加入裁决门槛。少于 min_days 不给 IC、MDE、区间、p 或单调性。
- **扩张分组**：只计 t 为止有效值（含评估前回看），前 warmup_days 个值不分组；midrank 分位按 brief 的公式，组数来自设置。`expanding_percentile` 公开，使用坐标压缩和只加入过去值的 Fenwick tree（O(n log n)），S4d 可以复用。组表的收益年化按 252/h，风险为前向波动均值，回撤从 t 收盘的初始净值到后 h 天路径、按滚动峰值算最大回撤。空组数值为 None，有空组不给单调性。warmup 内有效评估日计入 days，但不计入任一组。
- **自助法**：只在 QQQ 和美股大盘上做，行业不做。评估日的成对秩用 `stationary_bootstrap_indices` 重抽，平均块长 h，不重新排秩，2,000 次，分批最多 100 次；种子为 `[seed, h]`。按 confidence 给分位区间，中心化双侧 p 按 brief 的加一公式。常量信号 IC=None；有退化抽样时区间/p=None，不静默删除抽样。结果仅有 JSON 汇总，非有限值为 None，日期仅起止，没有逐日值、运行时间或路径。
- **稳健性和基准**：QQQ 按年、其他市场按十年逐个剔除后重排秩算 IC；stable 要每次都严格同预期方向，零或退化 IC 不算稳定。小样本的 leave_out 为未评估（count=0、stable=None）。固定基准为 mom_12_1、ma_10m_distance、rv_21，严格按 brief 的窗口公式。基准相关用主持有期评估日上的有效配对值；partial_ic 仅在自助法市场做，在信号、前向值和三个基准同时有效的交集上排秩、带常数 OLS 取残差，残差标准差小于 1e-9 视为数值退化。
- **裁决**：先淘汰（区间完全错向、breadth < 0.5、或基准最大绝对相关 ≥ 0.9），再候选（非 QQQ-only、QQQ IC 严格同向、breadth ≥ 0.666667−1e-6、主市场全家族 BH 调整 p ≤ 0.10、主市场十年剔除 stable=True），其余 inconclusive。QQQ-only 的主市场区间、广度、剔除稳定和主 p 都 None，永不 candidate；淘汰区间看 QQQ。所有解释阈值来自 owner 批准配置，容差为配置舍入的不变量。
- **全局账和 BH**：`research/_screens/ledger/NNNN.json`，research_screen_record.v1，S0001 起，每条带前一条 LF 规范哈希；独占新增写入，重复 module/spec 哈希、data 和 settings 哈希组合先拒绝。家族成员为 module_sha256:spec_sha256，规格变更（含参数、方向）是新成员；每成员取最新记录（包括最新主 p=None），只对主 p 非 None 的成员做 BH。记录时的裁决保留，screen-ledger 用当前全家族 BH 和当前规则重算裁决；旧记录无 verdict_inputs 时当前裁决为 None。链验证检查文件名、连续编号、schema、number、screen_id 和 prev_sha256；git 历史不许筛选记录改、删、改名。链不能证明未提交的尾记录没有被删除，git 历史是已提交记录的补充控制。一次只许一个研究运行，独占写入防并发覆盖，不是多进程事务锁。
- **命令**：`screen SIGNAL_ID --signal-snapshot ID [--bench-snapshot ID] [--root ...]`，root 沿用评估台的长历史快照根，省略 bench id 用 LATEST 并在记录里写解析后的实际 id；信号来源经 `signal_sources.load_snapshot(PROJECT_ROOT, ID)` 的质量门禁，QQQ 经 `from_snapshot` 的长历史门禁。先查链/重复，再加载快照和计算。QQQ 用 ndx_return、actual；US 用 Mkt-RF+RF，行业名小写且非字母数字换 `_`，重名拒绝；行业收益已由 S4a 转为小数。记录包含快照 id、设置原字节哈希、commit/modified、规格与模块哈希。两个快照 id 可定位其本机质量检查；PASS/WARN 仅允许读数据，不是候选结论。屏幕只给规定汇总和中文裁决；screen-ledger 只读记录，不读真实快照。
- **和前面设计的两处不同**：前面“策略接口”是 `(inputs, params)` 给敞口，本批信号是 `(returns, sources, params)` 给信号值，仅在检查时包装成策略接口；前面的运行账按预注册题计试验，本批筛选在开题之前进入全局筛选家族、按最新成员主 p 做 BH，不使用 prereg 的 screens/screen_family_size。通用“信号 → 仓位”及预注册衔接留到 S4d，不在本批。
- **验收范围**：Claude 提供的不变量 34–40 原样保留，合成数据覆盖手算、截止日污染、检查拒绝、基准复制、链和 BH、裁决边界、汇总无逐日值和原 bench 配置哈希。七项本地变异、51 市场计时、静态检查和全量结果记在 TRADING-2565-07 运行报告及 `tests/invariants/README.md`；本批不读真实快照、不开展新研究试验。

## S4d 的实现

- **范围及批准**：TRADING-2565-08 实现通用“信号 → 仓位”策略，沿用 `owner_decision:TRADING-2565:2026-10-13:signal_screen_layer`，不引入候选或生产晋级规则。只用合成数据验收，不读真实快照。
- **参数**：`signals` 是非空、id 唯一的列表，每项正好含 id、模块/规格 SHA-256 和 score；映射及其字段严格校验，拒绝缺失和额外参数。数值要求有限但不限定分数或仓位范围，避免评估台乘 1.5 的参数消费检查误拒合法预注册；可执行仓位上下界仍由模拟器拒绝。
- **分数及映射**：方向是 expected_sign 乘 return/risk 的正负方向；percentile 复用筛选层的扩张 midrank 和 warmup_days，sign 对正、负、零给 1、0、0.5，不热身。多信号算术平均且任何 NaN 都传播。threshold 在 cut 上取 high，linear 在 p_low/p_high 间线性裁剪，steps 在边界取上一级。
- **保持**：首个有效目标之前保留 NaN，由评估台提示 extend the lookback；之后遇 NaN 只向前填充，保持上一目标，不偷读未来。
- **钉住信号**：每次调用读取 `SIGNALS_DIR`，用 `screen.load_signal` 核对模块和规格的 LF 规范化哈希。规格变动拒绝；信号参数只读规格，预注册不能覆盖。仅把声明来源的 `source:<来源>:` 列去前缀后传入；缺来源、非同索引 Series 拒绝。不打印、不写文件。
- **来源列**：`with_signal_sources` 按快照规格的 known_lag_trading_days 逐列 `known_at` 对齐交易日历，追加在原输入后，重复列拒绝；收益和原列顺序不变，source 增加 signal_snapshot_id。CLI 只有预注册指定信号快照才加载它，仍经过 S4a 质量门禁；ledger 拒绝快照不符及未声明的信号快照。
- **预注册及报告**：新增可选 data.signal_snapshot_id（固定 sig-12 位小写十六进制）、screens（S0001 等）、screen_family_size（>=1），默认 None/空。验证批准检查 screens 引用存在且链完整，不要求 candidate；报告第一屏显示引用和家族大小，通用策略逐信号显示 id、score 和同模块/规格哈希的筛选记录及记录时裁决，无匹配写“未筛”，只读汇总。
- **模板与验收**：新增 `_template/signal_strategy.py` 一行导入实现，prereg 模板只增注释示例；不变量 41–47、接线单测及五项变异守住语义，证据见 `tests/invariants/README.md` 和运行目录。任务保持 IN_PROGRESS，待决定记录批准；没有临时投资阈值或开放参数规则。
- **第一题不变**：`research/QQQ-Q1/` 的输入、运行、窗口读取与指纹以及三份指定配置保持原样；QQQ-Q1 的新字段取默认值，report 除新增“筛选”行外逐字相同。production_effect=none；broker_action=none。

## 进展

- 2026-10-12：随 TRADING-2567 激活（READY）。production_effect=none；broker_action=none。
- 2026-10-13：设计和分批定稿（项目文件 `knowledge/claude/qqq-options/s3-bench-and-q1-prereg-draft-2026-10-13.md`，含第一题预注册草稿）。S3a 为 TRADING-2565-01（C 区）。production_effect=none；broker_action=none。
- 2026-10-13：S3a（TRADING-2565-01）Baker 按结果 tree 1dcf95e3 事先批准，排在 OPS-085-01 之后发布。S3b 写好：运行登记、试验计数、折算夏普、验证窗口读取登记、预注册格式和模板，打包等 S3a 发布后的 main。production_effect=none；broker_action=none。
- 2026-10-13：S3b（TRADING-2565-02）Baker 按结果 tree d2f4d858 事先批准，排在 S3a 之后发布。S3c 写好：标准证伪、报告第一屏、pi 研究运行的 brief 模板，打包等 S3b 发布后的 main。production_effect=none；broker_action=none。
- 2026-10-14：S4c（TRADING-2565-05）：向量化随机择时、减仓事件表。production_effect=none；broker_action=none。
- 2026-10-14：TRADING-2565-09：ship 规则 1.2.2，评估台配置按 `config/research/research_*.yaml` 通配进 C 区，成本模型 `transaction_cost_model.yaml` 进 C 区；`tests/test_gov008_ship.py` 守住评估台设置和它点名的配置文件都在 C 区。production_effect=none；broker_action=none。
- 2026-10-14：S4a（TRADING-2565-06）：信号筛选的数据入口、发布滞后对齐、本机内容寻址快照和检查；待按决定记录批准，之后 S4b。production_effect=none；broker_action=none。
- 2026-10-14：S4b（TRADING-2565-07）：信号筛选、全局筛选账和 BH、screen/screen-ledger 命令；只用合成数据验证，待审阅和按决定记录批准，之后 S4d 和研究记录快车道。production_effect=none；broker_action=none。
- 2026-10-14：S4d（TRADING-2565-08）：通用的信号到仓位策略、钉住信号及来源快照、预注册与汇总报告接线；292 项合成聚焦检查通过，五项变异被捕获，静态检查和第一题不变检查通过。全仓 main 基线和 PR 检查超时，车道的 AITS_STATE_ROOT 路径断言失败及 secret-scan 测试未结束，尚无全仓绿灯；保留证据，不修范围外测试，不发布。待审阅和按决定记录批准，之后研究记录快车道和第二题开题。production_effect=none；broker_action=none。
- 2026-10-14：更正：S4d（TRADING-2565-08）已发布到 main 8b2ba7fe（Gate PASS，zone C），上一行里的“尚无全仓绿灯”和“不发布”写于全仓测试还没跑完的时候，已不是事实。S4e（TRADING-2565-10）：第一批真实信号（credit_spread_change_63、credit_spread_level、term_spread_level、high_distance_252），只用合成数据验收，不读真实快照；筛选要另按研究会话的指令跑。production_effect=none；broker_action=none。
- 2026-10-15：S4e 的第一批真实筛选已跑完，四条记录 S0001 到 S0004 已发布到 main 71f00bc1：credit_spread_level 是候选，credit_spread_change_63 淘汰，term_spread_level 和 high_distance_252 证据不足。S4f（TRADING-2565-11）：经典趋势信号 tsmom_21、tsmom_63、tsmom_126、tsmom_252 和 ma_distance_210 的模块与规格，供第二题（QQQ-Q2）的预注册按哈希钉住；它们是固定基准，不进筛选账。只用合成数据验收，不读真实快照。production_effect=none；broker_action=none。
- 2026-10-15：S4g（TRADING-2565-12）：研究自动循环第一批不用新数据的筛选信号 industry_breadth_210、industry_corr_63、dip_in_uptrend_10 的模块与规格（规格写在任何筛选之前，按哈希钉住）。只用合成数据验收，不读真实快照；筛选另按研究会话的指令跑。production_effect=none；broker_action=none。
- 2026-10-15：S4h（TRADING-2565-13）：按 owner_decision:TRADING-2565:2026-10-15:research_loop_p1 在 config/research/research_screen.yaml 加入研究自动循环第 1 期白名单的 4 个 FRED 日度序列（VIXCLS、VXNCLS、DGS10、DTB3，晚 1 个交易日算已知），钉住来源清单的不变量测试随之更新；新增筛选信号 vrp_21、rate10_change_126、bill3m_change_126 的模块与规格（规格写在任何筛选之前，按哈希钉住）。只用合成数据验收，不读真实快照；新信号快照和筛选另按研究会话的指令跑。production_effect=none；broker_action=none。
- 2026-10-16：试跑模式（S5）按 owner_decision:TRADING-2565:2026-10-15:trial_mode 开始：S5-0（TRADING-2565-14）把这条决定写进本文；S5a（TRADING-2565-15）美股大盘的数据和开发窗口；S5b（TRADING-2565-16）试跑的门、计划、记录和命令。production_effect=none；broker_action=none。
- 2026-10-16：S5a（TRADING-2565-15）：`research_bench.data.from_us_market` 把 Ken French 日度因子（`ff_factors_daily` 的 Mkt-RF 加 RF）变成评估台数据，2 倍、3 倍按日合成、不扣费；新问题类型 us_market_trial 只有开发窗口 us_market_1929_2010（1929-01-02 到 2010-12-31，`config/research/research_window_registry.yaml` 和 `config/research/primary_research_window_policy.yaml`）；`run_strategy` 可以给定减仓事件表的固定下跌段，不给时照旧用评估台设置里的。只用合成数据验收，不读真实快照。production_effect=none；broker_action=none。
- 2026-10-16：S5b（TRADING-2565-16）：试跑的门 `research/_trials/prereg.yaml`、第一份计划 T0001（industry_corr_63，筛选记录 S0009，4 个配置）、试跑记录 `research/_trials/runs/` 和 trial-run、trial-ledger、trial-report 三个命令（`research_bench.trial`）；试跑记录进账本不变量，走研究记录快车道。只用合成数据验收，不读真实快照；T0001 的运行另按研究会话的指令跑。production_effect=none；broker_action=none。
- 2026-10-16：查尺子（TRADING-2565-17，owner_decision:TRADING-2565:2026-10-16:gate_power_check）：`src/ai_trading_system/research_probes/gate_power.py`（B 区）量第二、三题和试跑用的 11 条规则，在一个和验证窗口一样长的窗口里认出真效果的比例。只用 2010-12-31 以前的数据（试跑计划 T0001 的美股大盘和 QQQ，先截断，拒绝更晚的数据），不读验证窗口和一致性窗口，不买数据，不改评估台、门和任何 C 区文件。美股大盘从 1930 到 2000 年每隔一年一个窗口（从当年第一个交易日到十年后的 2 月 19 日，和验证窗口的日历跨度相同，共 36 个）；QQQ 只看开发窗口，只描述。埋进去的假信号在每个月末用下个月的真实波动率加噪声（噪声是信号自身标准差的 0 到 3 倍，7 档，另加一档纯噪声）定仓，和 T0001 主配置一样按扩张百分位映射到 0..1、月末调仓；它们偷看未来，不过前视检查，不进任何账本，不算试验。第一题 q1a、第二题 trend4 和 ma10 按预注册原样一起跑，只描述。每个窗口按试跑的算法评估（成本、推迟、剔除一年、剔除固定行情段、同敞口随机择时、95% 自助法区间，另算 98.75% 的区间下限），美股的固定行情段用探针里写定的 17 段。读法在运行之前写定：整段美股历史上左尾改善至少 20% 的最弱一档记为 L，L 在美股窗口里 11 条全过的比例低于 50% 先改尺子，80% 及以上换问题，中间交 Baker 定；纯噪声过门超过 10% 报告门偏松。同一批按 owner_decision:TRADING-2565:2026-10-16:loop_quota 给 `research/_loop/charter_p1.md` 加第 11 节（新信号每期 20 个；没有候选过试跑门的信号每满 20 个复盘一次，不停），查尺子的结果交给 Baker 后生效。只用合成数据验收，不读真实快照；真实数据的运行发布后另按研究会话的指令跑。production_effect=none；broker_action=none。
- 2026-10-16：查尺子的结果：研究会话 19:24 JST 在 main 7f665986 上跑一次（407 个评估，没有出错）。L 为噪声 1（整段美股左尾改善 23.1%），在 36 个美股十年窗口里 11 条全过 19.4%（不重叠的 6 个里 1 个），纯噪声 0%；按写定的读法低于 50%：尺子对单个 10 年窗口太没力，第一题的「证据不足」不能当负结果读，先改尺子。
- 2026-10-16：两段尺子探针（TRADING-2565-18，owner_decision:TRADING-2565:2026-10-16:ruler_two_stage）：`src/ai_trading_system/research_probes/ruler_power.py`（B 区）在改试跑门之前量候选的两段尺子。数据和截断同查尺子，只用美股大盘。窗口：十年查方向窗口（和查尺子相同，36 个）、20、30、40 年的长窗口（从当年第一个交易日到最后一年的 12 月 31 日）和整段 1930–2010，起点都是 1930 年起每隔一年；配对是 30 或 40 年的长窗口加紧接着的十年窗口。假信号两族：波动率族和查尺子完全相同，用来挑尺子；回撤族（偷看下个月从高点的最大跌幅，另一个种子）只做验收。三条已检验的规则只描述。每个评估照试跑的算法，自助法区间另记 95%、99%、99.7% 三档。候选尺子：第一段在长窗口上过核心 4 条（区间下限 > 0、回撤改善 ≥ 5%、收益和 25 个基点成本下都不低于 -1%）或全部 11 条，三档置信度；第二段在下一个十年只查方向（S2：左尾和回撤点估计都改善；S3：左尾 95% 区间上限 > 0；都要收益不低于 -1%）。读法在运行之前写定：几乎没效果的波动率族规则（纯噪声和整段左尾改善不到 5% 的档）在两种配对里过两段都不超过 5% 的候选才可选；可选的里面取 L 两段都过比例（两种配对取低的）最高的，并列时核心 4 条在前、置信度高的在前、S2 在前；选中的到 80% 而且回撤族的 L 也到 80%、几乎没效果的不超过 5%，才写成试跑门的改动交 Baker 批（C 区）；其余情形原样报告，由 Baker 定。只用合成数据验收，不读真实快照；真实数据的运行发布后另按研究会话的指令跑。production_effect=none；broker_action=none。
- 2026-10-17：两段尺子探针的结果：研究会话在 main e019b10a 上跑一次（2185 个评估，没有出错，结果写于 00:22 JST）。按写定的读法判为 below_80：最好的候选 core4@0.95+S3，L 两段都过 66.7%（30+10 年 14/21，线是 17；40+10 年 16/16）；12 个候选里几乎没效果的规则在两种配对里一对也没过；回撤族在这把尺子上两种配对都全过。30+10 年没过的 7 对都卡在第一段：1938、1940、1944 年起的窗口回撤改善不到 5%，1964 到 1970 年起的窗口左尾改善的区间下限没离开 0。
- 2026-10-17：试跑门换成两段尺子（TRADING-2565-20，owner_decision:TRADING-2565:2026-10-17:ruler_40y）：`research/_trials/prereg.yaml`（C 区）以美股大盘开发窗口（1929-01-02..2010-12-31，82 年）为第一段，核心 4 条、95% 区间（左尾改善区间下限大于 0、回撤改善至少 5%、几何年化和 25 bps 成本下都不低于 -1%）；QQQ 开发窗口是第二段，只查方向（S3：左尾改善 95% 区间上限大于 0、几何年化不低于 -1%）。开题以后读验证窗口也按这两条只查方向，`research_bench.trial` 的 trial-report 把验证窗口一行从 0.05/k 改成列出这两条（C 区）。换门以前的门原样存为 `src/ai_trading_system/research_probes/trial_gate_v1.yaml`，查尺子和两段尺子两个探针改读它（B 区），重跑时量的还是同一套规则、记下的 gate_sha256 不变。`research/_loop/charter_p1.md` 第 11 节加一段：第 6 节的门和第 5 节的验证窗口读法按此改，第 4 节的每周新择时卡恢复。换门以前的试跑记录照当时的门判，不重算（T0001 在新门下美股大盘的回撤也不过，判定不变）。只用合成数据验收，不读真实快照。production_effect=none；broker_action=none。
- 2026-10-18：评估台加储备和固定比例（TRADING-2565-21，owner_decision:TRADING-2567:2026-10-18:portfolio_p1，设计见 `docs/requirements/TRADING-2567_QQQ_Research_Program_1999_Window.md` 里这条决定）：`research_bench.data` 用 FRED DGS10 合成 10 年期国债（当天已知的收益率给票息等于前一交易日收益率的十年期平价债定价，加按日历天数计的票息），运行规格加 `reserve`（默认 `cash`，不是现金时才写进规格，以前的指纹不变）：策略在储备代替现金的收益上模拟，基准、现金和敞口缺口照旧用现金；证伪检验里策略的重跑和安慰剂也用储备，同敞口基准用现金。新策略 `fixed_exposure`（固定敞口）。每次运行另算同波动率对照（QQQ 加现金，敞口取策略和买入持有的波动率之比，最高 1 倍），放在 `diagnostics`，不进指纹、不进题目账。试跑计划可以不写筛选候选而写 owner 决定（每条决定一份计划，只能用 `fixed_exposure`）；配置可写 `reserve`；试跑门在美股大盘下加 `reserve_question_types`，持有国债的配置用问题类型 `us_market_bond_trial`、开发窗口 `us_market_1962_2010`（49 年）。trial-report 对按决定写的计划显示决定号，标题写储备，每个成功的运行加「代价换效果」和「同波动率对照」两行。计划 `research/_trials/T0002.yaml`（主配置 60/40，敏感性 50/50、70/30）随这批进 main。只用合成数据验收，不读真实快照。production_effect=none；broker_action=none。
- 2026-10-18：误放率探针（TRADING-2565-22）：T0002 没过门以后，Baker 23:04 JST 在「研究自动生成和初筛」线程的卡上选「先做探针」。`src/ai_trading_system/research_probes/gate_false_pass.py`（B 区）量试跑门 `research/_trials/prereg.yaml` 的两段规则放过零效果规则的比例；探针读门的逐字节副本 `src/ai_trading_system/research_probes/trial_gate_v2.yaml`，记下它的 sha256。数据和截断同查尺子（试跑计划 T0001 的美股大盘和 QQQ，截到 2010-12-31）。只评估门的两个开发窗口：第一段美股大盘 1929-01-02..2010-12-31，第二段 QQQ 1999-03-10..2010-12-31。零效果三族各 150 个种子（从 20261018 起，三族共用）：纯噪声、查尺子的噪声 3 倍波动率族、两段尺子探针的噪声 3 倍回撤族；对照是噪声 1 倍和 0.5 倍的波动率族，各 20 个种子，只报检出率。热身 24 个月（美股数据从 1926-07-01 起，第一段开始前只有 29 个月末）。每个评估只算门读的部分：模拟、同敞口基准、比较、25 bps 重跑和门的置信度下的自助法区间，算法同证伪检验；不跑推迟、剔除和随机择时。读法在运行之前写定：三族合并、两段都过的比例的 95% Wilson 上限不超过 2.5% 为可用（循环恢复每期 20 个信号），2.5% 到 5% 为降额（恢复，每期 10 个，等金丝雀），超过 5% 为偏松（继续暂停，报 Baker，连同放得最多的规则）；单族上限超过 10% 单独点名；零效果的评估出错就不判读。只用合成数据验收，不读真实快照；真实数据的运行发布后另按研究会话的指令跑。production_effect=none；broker_action=none。
- 2026-10-19：误放率探针的结果：研究会话在 main 9ddb08d6 上跑一次（980 个评估，没有出错，10-19 01:22 JST 跑完）。按写定的读法判为偏松：零效果三族合并、两段都过 31/450（6.9%），95% Wilson 上限 9.6%；单族上限超过 10% 的是噪声 3 倍的波动率族（12/150，上限 13.5%）和回撤族（19/150，上限 18.9%）；纯噪声 0/150；对照两族各 20/20。对这两族，第一段只有左尾改善区间下限大于 0 一条在拦（成立 9.3%、12.7%），其余三条成立 75% 到 100%，第二段两条成立 93% 到 100%；它们在第一段的中位数是左尾改善 +3.1%、+4.7%，回撤浅 11.5、29.6 个点，几何年化多 1.35、3.9 个点（偷看未来的假信号，效果小但不是零）。按读法继续暂停，报 Baker。
- 2026-10-19：收紧重读（owner_decision:TRADING-2565:2026-10-19:tail_floor10）：不跑新评估，研究会话用一条代码重读同一份结果文件 `false_pass_result.json`（1180596 字节，SHA-256 F3A81E2B…），候选门在第一段加左尾点估计至少改善 10%。按同样的线判为可用：零效果三族两段都过 0/450，95% 上限 0.85%；对照噪声 1 倍 20/20（线是 16/20）、0.5 倍 20/20。两族近零信号原来第一段过门的 13 个和 19 个，左尾点估计都不到 10%。用的是同一批种子，不是新种子；10% 是 `research/families.md` 事先写下的门的价，不是看了结果挑的。试跑门的改动是 TRADING-2565-24（C 区）。production_effect=none；broker_action=none。
- 2026-10-19：试跑门第一段加最小左尾效果（TRADING-2565-24，owner_decision:TRADING-2565:2026-10-19:tail_floor10）：`research/_trials/prereg.yaml`（C 区）的美股大盘规则在核心 4 条之后加 `comparison.cvar_5pct_relative_improvement >= 0.10`（左尾改善的点估计至少 10%），`owner_decision` 改为这条决定；第二段、置信度、配置上限、储备问题类型和快跌段事件表不变。误放率探针继续读 `trial_gate_v2.yaml`（加这一条以前的门，不变），测试改成钉住它的 sha256，并检查现在的门正好是它加这一条。以前的试跑记录照当时的门判，不重算。上 main 以后研究自动循环恢复每周新择时卡和择时候选的试跑，新信号每期 20 个（第 1 期章程第 11、13 节）。只用合成数据验收，不读真实快照。production_effect=none；broker_action=none。
- 2026-10-19：研究自动循环第二批筛选信号（TRADING-2565-25，第二批假设卡 H07、H08，每周新择时卡在 TRADING-2565-24 上 main 以后恢复）：`bad_vol_share_63`（族：波动；过去 63 日下跌日收益平方和减上涨日收益平方和、除以两者之和，看波动的方向构成，不看水平）和 `reversal_profit_21`（族：反转；49 个行业按前 5 日复利收益做的短期反转组合，做多和做空各 1，过去 21 日日收益之和，所有市场共用一个值）的模块与规格，目标都是风险（规格写在任何筛选之前，按哈希钉住）。两个都只用 1926-07 起的 Ken French 日度数据，加上筛选的 252 天预热，在试跑门第一段（美股大盘 1929-01-02 起）开始以前就有值；测试在合成数据上核对这一点。`bad_vol_share_63` 选它的理由引用了 q1a 在验证窗口里的失败原因，验证窗口对它不是干净的检验。只用合成数据验收，不读真实快照；筛选另按研究会话的指令跑。production_effect=none；broker_action=none。
