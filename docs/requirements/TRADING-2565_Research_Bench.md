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
  - `data`：快照到资产收益和信号输入，T-bill 利率滞后一天；
  - `simulate`：敞口映射、调仓规则、漂移、成本；
  - `metrics`；
  - `checks`：前视、参数消费；
  - `run`：检查、模拟、基准、指标、指纹；验证和一致性窗口抛 `WindowNotOpenError`，等 S3b；
  - `__main__`：`benchmarks` 命令在开发窗口上跑四个固定基准，只打印汇总和用时，用来核对数据和评估台，不算研究试验。
- **区**：`src/ai_trading_system/research_bench/**` 和 `config/research/research_bench.yaml` 加进 `config/gov008_ship.yaml` 的 C 区，pi 守卫读同一份配置。
- **和旧模拟器的关系**：时序和旧的 `simple_baseline_portfolio_control._strategy_return_series` 相同；同一份价格上，200 日均线全进全出规则逐日一致（不变量 10）。区别有两点：
  - 持仓在两次调仓之间漂移。旧模拟器隐含每天回到目标权重，又不计这部分换手。
  - 成本按乘法从调仓当天扣除。
- **用时**：云端合成数据上，一次完整运行约 0.3 秒，包括 40 次前视重算、参数检查、策略和 3 个基准的模拟。

## 进展

- 2026-10-12：随 TRADING-2567 激活（READY）。production_effect=none；broker_action=none。
- 2026-10-13：设计和分批定稿（项目文件 `knowledge/claude/qqq-options/s3-bench-and-q1-prereg-draft-2026-10-13.md`，含第一题预注册草稿）。S3a 为 TRADING-2565-01（C 区）。production_effect=none；broker_action=none。
