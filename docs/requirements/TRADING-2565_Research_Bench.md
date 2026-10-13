# TRADING-2565：研究评估台（试验登记和多重检验校正）

## Owner decision

- 依据：`owner_decision:TRADING-2567:2026-10-12:qqq_direction_1999_window`。评估台是 TRADING-2567 的 S3 阶段，本任务随它激活。Baker 定的研究节奏不限同时进行的问题数，靠这里的试验登记和多重检验校正控制假阳性。
- 第一批题目的顺序和成功标准：`owner_decision:TRADING-2567:2026-10-13:risk_first_question_order`（记在 TRADING-2567 需求文档）。评估台的主指标按它定：同等平均敞口下的回撤、左尾和复利分散在前，超额收益在后。
- 每一批都是 C 区，Baker 按结果 tree 事先批准。

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

## 进展

- 2026-10-12：随 TRADING-2567 激活（READY）。production_effect=none；broker_action=none。
- 2026-10-13：设计和分批定稿（项目文件 `knowledge/claude/qqq-options/s3-bench-and-q1-prereg-draft-2026-10-13.md`，含第一题预注册草稿）。S3a 为 TRADING-2565-01（C 区）。production_effect=none；broker_action=none。
- 2026-10-13：S3a（TRADING-2565-01）Baker 按结果 tree 1dcf95e3 事先批准，排在 OPS-085-01 之后发布。S3b 写好：运行登记、试验计数、折算夏普、验证窗口读取登记、预注册格式和模板，打包等 S3a 发布后的 main。production_effect=none；broker_action=none。
- 2026-10-13：S3b（TRADING-2565-02）Baker 按结果 tree d2f4d858 事先批准，排在 S3a 之后发布。S3c 写好：标准证伪、报告第一屏、pi 研究运行的 brief 模板，打包等 S3b 发布后的 main。production_effect=none；broker_action=none。
