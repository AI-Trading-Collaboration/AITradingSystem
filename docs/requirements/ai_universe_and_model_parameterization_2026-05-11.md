# AI 产业链核心观察池扩展与模型参数化清单

状态：VALIDATING

最后更新：2026-05-11

关联任务：`UNIV-001`

## 背景

当前系统尚未正式稳定进入生产环境，owner 明确可以优先提升当前 AI
产业链覆盖，不需要为了保持早期 6 个 ticker 的历史输出一致性而延后
观察池扩展。

此前 production 路径主要围绕 `MSFT`、`GOOG`、`TSM`、`INTC`、`AMD`、
`NVDA` 六个 ticker。这个集合覆盖云、模型、GPU/ASIC、代工和封装主链路，
但对云 CapEx、半导体设备、HBM/存储、网络/ASIC 和软件商业化的代表性不足。

同时，当前 scoring、position gate、风险预算和报告解释里已有多处固定阈值。
这些阈值在基础版中是合理的保守规则，但后续应进入 replay、shadow、
rule governance 和 owner approval 约束下的参数迭代，而不是长期散落在代码中。

## 本轮决策

本轮直接把以下代表性 ticker 纳入当前核心 AI 产业链流程：

| 分组 | Ticker | 目的 |
|---|---|---|
| 云 CapEx / 平台 | `AMZN`, `META` | 补齐 hyperscaler CapEx 和自研 AI 基础设施需求。 |
| ASIC / 网络 / 加速器 | `AVGO`, `MRVL` | 观察定制 ASIC、网络和互联芯片对 AI 算力链的传导。 |
| HBM / 存储 | `MU` | 补齐 HBM 和存储供给约束。 |
| 半导体设备 | `ASML`, `AMAT`, `LRCX` | 补齐光刻、沉积、刻蚀和先进封装设备链。 |
| 应用商业化 | `PLTR`, `CRM`, `NOW` | 观察 AI 应用层付费转化和企业软件商业化。 |

新增 ticker 已进入：

- `config/universe.yaml` 的 `ai_chain.core_watchlist`；
- `config/watchlist.yaml` 的活跃观察池；
- `config/watchlist_lifecycle.yaml` 的 PIT 生命周期；
- `config/sec_companies.yaml` 的 SEC/ADR 公司映射；
- `score-daily`、`download-data`、`validate-data`、`backtest` 和 dashboard
  使用的默认核心标的集合。

新增 `AVGO` 和 `LRCX` 时同步补充 2024 年 10-for-1 拆股事件，避免默认
AI regime 窗口内的 adjusted close 跳变被误判为未知价格异常。

## 参数化候选清单

这些项目先登记为调优候选，不在本轮改 production 阈值。

| 类别 | 当前位置 | 现状 | 后续参数化方向 |
|---|---|---|---|
| 核心 universe 成员 | `config/universe.yaml` / `watchlist_lifecycle.yaml` | 当前核心 ticker 固定进入评分、特征和回测 | 建立 core/reference/candidate 分层、节点权重和加入/移除 promotion gate。 |
| 模块权重 | `config/scoring_rules.yaml:weights` | trend/fundamentals/macro/risk/valuation/policy 固定权重 | 用 regime、forward shadow、样本外表现约束权重扰动范围。 |
| 信号阈值和点数 | `config/scoring_rules.yaml` | MA、相对强弱、基本面、宏观、VIX、估值分位等阈值固定 | 迁入可版本化 parameter profile，支持 replay/shadow 对比。 |
| 评分到仓位映射 | `src/ai_trading_system/scoring/position_model.py` | 80/65/50/35 分映射到 80%-100%/60%-80%/40%-60%/20%-40%/0%-20% | 抽到配置并允许非线性或更平滑的仓位曲线。 |
| 估值健康状态 | `src/ai_trading_system/valuation.py` | `>=90` 为 `EXTREME_OVERHEATED`，`>=75` 为 `EXPENSIVE_OR_CROWDED` | 区分单票、行业中位数、拥挤比例和 PIT 可信度，避免单票过度触发整体 cap。 |
| valuation gate 上限 | `config/scoring_rules.yaml:position_gates.valuation` | expensive/crowded 上限 70%，extreme 上限 40% | 用 ticker 权重、节点扩散度、趋势强度和估值可信度决定 cap。 |
| thesis gate 上限 | `config/scoring_rules.yaml:position_gates.thesis` | warning 上限 70%，failure 上限 0% | 区分 active_trade、watch_only、单票 thesis 与组合 thesis。 |
| data confidence gate | `config/scoring_rules.yaml:position_gates.data_confidence` | quality warning 80%，insufficient data 60%，placeholder 80% | 让不同模块缺数按投资影响和可替代证据分层。 |
| 置信度仓位折扣 | `src/ai_trading_system/scoring/daily.py` | confidence >=75/60/45 使用固定 multiplier | 抽成配置，和模块置信度、样本成熟度联动。 |
| 宏观总风险资产预算 | `config/portfolio.yaml:macro_risk_asset_budget` | VIX、利率、美元阈值和风险资产区间固定 | 按 AI regime 和市场状态校准总风险资产预算边界。 |
| 风险预算 gate | `config/portfolio.yaml:risk_budget` | VIX stress、集中度、ETF beta 覆盖固定 | 接入真实持仓后对单票/节点/相关性簇做 replay 参数扫描。 |
| 官方风险来源 ticker 映射 | `src/ai_trading_system/official_policy_sources.py` | export control / Taiwan / ticker keyword 映射仍在代码中维护 | 迁入 `risk_events` 或 universe 派生配置，避免扩展观察池后遗漏风险匹配。 |
| 数据质量阈值 | `config/data_quality.yaml` | stale、return、adjustment、cross-source 阈值固定 | 保持 fail-closed，但可按 ticker 类别和来源历史质量分层。 |
| feature windows | `config/features.yaml` | MA 20/50/100/200，return 1/5/20，VIX 252 分位固定 | 通过 lag sensitivity 和 out-of-sample 验证窗口组合。 |
| execution policy | `config/execution_policy.yaml` | 动作阈值、低置信度、manual review gate 固定 | 与回测成本、换手和 cooldown 联动。 |
| catalyst windows | `src/ai_trading_system/catalyst_calendar.py` | 未来 5/20/60 天窗口固定 | 按事件类型和风险等级配置。 |

## 验收标准

- 新增 ticker 已进入核心观察池、watchlist、lifecycle 和 SEC 映射，配置校验通过。
- 默认价格下载 ticker 集合包含新增代表 ticker，`include_full_ai_chain` 不产生重复。
- watchlist 校验和 lifecycle 校验通过，活跃记录数反映扩展后的核心观察池。
- 文档明确哪些固定阈值后续应作为参数化候选，而不是在本轮直接调参。
- `docs/system_flow.md` 同步说明当前 core_watchlist 已扩展为 AI 产业链代表性观察池。
- 相关单测、ruff 通过。

## 进展记录

- 2026-05-11：新增任务并进入实现。owner 明确当前尚未进入稳定生产，可直接扩展代表 ticker 到流程；同时要求整理类似 valuation 40% cap 的硬编码限制条件，作为后续模型迭代参数候选。
- 2026-05-11：实现进入 `VALIDATING`。已把 11 个新增代表 ticker 纳入
  `core_watchlist`、watchlist、lifecycle、SEC 映射、风险事件相关 ticker 和官方政策来源 ticker 匹配；已补 AVGO/LRCX 拆股事件；更新系统流图和测试预期。验证通过 watchlist/lifecycle/risk-events CLI 校验、`python -m ruff check src tests` 和全量 `python -m pytest -q`。
- 2026-05-11：验证拉回 `IN_PROGRESS`。expanded universe 真实非交易日
  `ops daily-run --as-of 2026-05-10` 已通过官方政策来源、PIT 和 SEC
  companyfacts 下载，但 `fundamentals extract-sec-metrics` 阻断于 ASML
  taxonomy：SEC companyfacts 实际返回 `us-gaap + dei`，原配置预期
  `ifrs-full + dei`。下一步修正 ASML SEC taxonomy 配置并复跑 SEC metrics、
  非交易日 daily-run 与交易日 replay。
- 2026-05-11：重新进入 `VALIDATING`。ASML SEC taxonomy 已修正为
  `us-gaap + dei`，并补充 `us-gaap/EUR` 指标映射；`fundamentals
  extract-sec-metrics --as-of 2026-05-10` 覆盖 17 家公司。真实非交易日
  `ops daily-run --as-of 2026-05-10` 结果为 `PASS_WITH_SKIPS`，交易日
  strict `ops replay-day --as-of 2026-05-08 --full-universe` 为 `PASS`，
  `ops replay-window --start 2026-05-08 --end 2026-05-10` 为
  `PASS_WITH_SKIPS`（1 个交易日回放、2 个周末跳过）。当前剩余已知
  warning 是 AMZN R&D 指标在 SEC companyfacts 中缺披露口径，暂不阻断。
