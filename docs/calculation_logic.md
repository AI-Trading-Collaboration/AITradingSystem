# 输入到输出的计算逻辑

最后更新：2026-05-16

本文面向完全没有金融背景的读者，解释系统如何把输入数据变成每日评分、仓位区间和复核报告。本文只解释当前设计和计算思路，不引入新的投资规则，也不改变 production scoring、position gate、回测或 prediction ledger 语义。

系统的核心问题不是“明天一定涨还是跌”，而是：

> 在今天已知且可复核的数据下，AI 产业链的风险回报环境是否支持更高或更低的风险资产内 AI 仓位？

这里的“仓位”不是交易指令，也不是账户实际持仓。它只是投研语言：如果投资组合中有一部分被定义为风险资产，系统会建议这部分风险资产里 AI 相关资产的暴露区间应该偏高、偏低或保持中性。

## 先理解几个基本词

### 价格

价格是某个 ETF 或股票在某一天的市场收盘价。系统主要看 `adjusted_close`，也就是把拆股、分红等历史价格口径尽量调整到可比较状态后的价格。

为什么需要价格：

- 价格是市场已经投票出来的结果。
- 趋势、相对强弱、波动和回测都依赖价格。

设计思路：

- 价格不是“真相”，只是市场行为的观测值。
- 所以系统要求主行情源和第二行情源 reconciliation，不能静默相信单一来源。

### 收益率

收益率衡量价格变化比例，常用公式是：

```text
return_Nd = current_price / price_N_trading_days_ago - 1
```

例如 20 个交易日前价格是 100，今天是 110，则 `return_20d = 10%`。

为什么不用简单差值：

- 从 100 涨到 110 和从 10 涨到 20 的差值都可能是 10，但经济含义完全不同。
- 比例更适合比较不同价格水平的资产。

### 移动平均线

移动平均线是最近 N 个交易日价格的平均值：

```text
moving_average_N = average(last_N_adjusted_close)
```

例如 `ma_200` 是最近约 200 个交易日的平均价格。系统常用 `above_ma_100` 和 `above_ma_200`，含义是今天价格是否高于这条均线。

为什么要看均线：

- 单日价格噪声很大。
- 价格长期在均线上方，通常表示市场趋势较强。
- 价格长期在均线下方，通常表示市场风险偏防守。

设计思路：

- 均线只描述趋势，不解释原因。
- 趋势分数高不等于估值便宜，也不等于基本面健康。

### 相对强弱

相对强弱衡量一个资产是否跑赢另一个基准。系统会看半导体 ETF 或科技 ETF 相对 `SPY` 的表现，例如 `SMH/SPY`。

一个简化理解是：

```text
relative_strength = AI_or_semiconductor_asset_price / broad_market_price
relative_strength_return_Nd = current_relative_strength / relative_strength_N_days_ago - 1
```

为什么要看相对强弱：

- 大盘整体上涨时，AI 资产上涨不一定说明 AI 产业链更强。
- 如果 AI 或半导体资产跑赢大盘，才更像是市场在偏好这一主题。

设计思路：

- 系统不是只问“涨了吗”，而是问“相对大盘是否更受欢迎”。

### VIX、利率和美元

`^VIX` 常被用作市场恐慌或波动预期的代理。`DGS2`、`DGS10` 是美国 2 年和 10 年国债收益率。`DTWEXBGS` 是 Federal Reserve 广义美元指数代理。

为什么这些会影响 AI 仓位：

- VIX 高或快速上升时，市场通常更不愿意承担风险。
- 利率快速上升时，远期成长型资产的估值压力通常变大。
- 美元快速走强时，全球流动性和跨国企业收入折算可能承压。

设计思路：

- 宏观数据不直接判断某家公司好坏。
- 它只影响系统愿意承担多少总体风险。

### 基本面

基本面是企业经营质量的硬数据，例如毛利率、经营利润率、净利率、研发投入强度和资本开支强度。

为什么要看基本面：

- AI 产业链不是只有股价故事，还要看企业是否真的有收入质量、利润能力和投入强度。
- 价格趋势强但基本面弱，系统应降低确定性。

设计思路：

- SEC / TSM IR 这类来源比新闻摘要更适合做硬数据输入。
- 缺少基本面时不应假装知道，所以会进入低覆盖或中性处理。

### 估值

估值衡量“价格相对基本面是否昂贵”。系统当前用估值快照中的历史分位、拥挤或过热状态来辅助判断。

为什么高估值会限制仓位：

- 一家公司或一个主题可以很好，但如果市场已经给了很高价格，继续加仓的容错率会下降。
- 高估值不是看空信号，但它可能成为仓位上限。

设计思路：

- 估值更像刹车，不是方向盘。
- 它常常不改变 score 的方向判断，但会限制 final position。

### 风险事件

风险事件是政策、地缘、供应链、出口管制、监管等可能影响 AI 产业链的事件。

为什么要单独处理：

- 这类事件可能很少发生，但一旦发生会改变仓位风险。
- 新闻和 LLM 线索必须经过来源、等级和复核边界约束。

设计思路：

- 风险事件不能只靠模型自由发挥。
- 只有满足事件等级、证据等级、可见时间和复核规则的记录才能进入评分或 gate。

### 信心和 gate

信心回答“这次判断的数据和证据是否可靠”。gate 是仓位闸门，回答“即使分数不错，有没有理由不能给更高仓位”。

为什么要分开：

- 分数高代表环境偏支持。
- 信心低代表证据不够扎实。
- gate 触发代表某个风险约束要求系统先收手。

设计思路：

- 系统不能用一个总分掩盖所有问题。
- score、confidence 和 gate 分开，才方便复核。

## 总流程

每日主链路可以概括为：

```text
原始数据
  -> 数据质量门禁
  -> 市场和基本面特征
  -> 单个信号归一化
  -> 模块 component score
  -> effective weights
  -> overall score
  -> model position
  -> confidence adjusted position
  -> macro risk asset budget
  -> position gates
  -> final position
  -> 日报、snapshot、trace、ledger
```

每一步都保留可复核产物。这样做的目的不是让链路复杂，而是避免结论无法追溯。

## 第 1 步：输入数据先过质量门禁

主要输入：

- `data/raw/prices_daily.csv`
- `data/raw/prices_marketstack_daily.csv`
- `data/raw/rates_daily.csv`
- `data/raw/download_manifest.csv`
- `config/data_quality.yaml`

主要输出：

- `outputs/reports/data_quality_YYYY-MM-DD.md`
- `outputs/reports/data_quality_YYYY-MM-DD_marketstack_reconciliation.csv`

计算逻辑：

1. 检查文件和必需列是否存在。
2. 检查日期、ticker、价格、利率等字段是否能被解析。
3. 检查是否有重复键，例如同一个 `ticker + date` 出现多次。
4. 检查价格是否非正、异常跳变、缺少近期数据。
5. 检查主行情源和第二行情源是否存在无法解释的差异。
6. 输出质量报告；严重错误时后续特征、评分、回测和日报必须停止。

为什么要这样做：

- 投资链路里，错误输入会制造看似精确但完全错误的结论。
- 价格缓存是本地状态，不提交到代码仓库，CI 无法替你验证本地数据。
- 所以运行时必须 fail closed。

设计思路：

- 可解释的差异进入 `INFO` 或 warning，例如指数 volume 不适用、已知拆股窗口、第二源自身坏点。
- 无法解释的价格冲突不能自动平滑。
- 后续报告必须能看见 data quality status。

常见误解：

- 质量门禁通过不代表数据完美，只代表达到本系统当前生产使用最低要求。
- 第二来源不覆盖主来源，它只帮助发现主来源或第二来源的异常。

## 第 2 步：把原始数据变成 feature

主要输入：

- `config/features.yaml`
- `config/feature_availability.yaml`
- `data/raw/prices_daily.csv`
- `data/raw/rates_daily.csv`

主要输出：

- `data/processed/features_daily.csv`
- `outputs/reports/feature_summary_YYYY-MM-DD.md`
- `outputs/reports/feature_availability_YYYY-MM-DD.md`

计算逻辑：

### 2.1 只使用评估日可见的数据

系统会先过滤：

```text
date <= as_of
```

这叫 PIT，也就是 point-in-time。意思是站在 `as_of` 那一天，只能使用当时已经可见的数据。

为什么要这样做：

- 如果用未来数据解释过去，报告会看起来很聪明，但不可复现，也不能指导真实决策。

### 2.2 计算价格趋势

对于每个配置内的价格标的，系统会计算：

```text
return_1d
return_5d
return_20d
moving_average_20
moving_average_50
moving_average_100
moving_average_200
above_ma_100
above_ma_200
```

`above_ma_200` 的简化逻辑是：

```text
above_ma_200 = 1 if latest_adjusted_close > moving_average_200 else 0
```

为什么要这样做：

- 1 日、5 日、20 日收益率描述短中期变化。
- 100 日和 200 日均线描述中长期趋势。
- 用 0/1 表示是否站上均线，便于后续评分。

### 2.3 计算相对强弱

系统会计算 `SMH/SPY`、`SOXX/SPY`、`QQQ/SPY` 等相对强弱。

简化逻辑：

```text
ratio_today = numerator_price_today / denominator_price_today
ratio_then = numerator_price_N_days_ago / denominator_price_N_days_ago
relative_strength_return_Nd = ratio_today / ratio_then - 1
```

为什么要这样做：

- `SPY` 代表更宽的美股市场。
- AI、半导体或科技资产跑赢 `SPY`，说明主题相对更强。

### 2.4 计算 VIX、利率和美元特征

系统会计算：

- `^VIX` 当前值；
- `^VIX` 252 日分位；
- `^VIX` 5 日变化；
- `DGS2` / `DGS10` 的 5 日、20 日变化；
- `DTWEXBGS` 的 20 日变化。

分位的直觉是：

```text
vix_percentile_252 = 过去 252 个交易日里，有多少比例的 VIX 低于或等于今天
```

为什么要这样做：

- 当前 VIX 水平回答“现在紧张吗”。
- VIX 分位回答“相对过去一年紧张吗”。
- 利率和美元变化回答“宏观环境是否正在收紧”。

### 2.5 计算核心观察池广度

系统会看 AI 核心观察池中有多少比例的标的站上 200 日均线：

```text
above_ma_200_ratio = count(tickers_above_ma_200) / count(tickers_with_enough_data)
```

为什么要这样做：

- 只靠一两个龙头上涨，主题可能并不健康。
- 越多核心标的处于长期趋势上方，市场广度越好。

设计思路：

- feature 是事实转换，不是最终判断。
- feature 不直接决定仓位，后面还要经过评分、权重、信心和 gate。

常见误解：

- feature 高不等于建议买入。
- 单个 feature 只是一个证据点，不是结论。

## 第 3 步：把 feature 归一化成信号分数

主要输入：

- `data/processed/features_daily.csv`
- `data/processed/sec_features_YYYY-MM-DD.csv`
- `data/external/valuation_snapshots/*.yaml`
- `data/external/risk_event_occurrences/*.yaml`
- `config/scoring_rules.yaml`

主要输出：

- 模块评分明细；
- `data/processed/scores_daily.csv`；
- 日报中的模块评分和 `Score-to-Position Funnel`。

系统要把不同单位的指标统一成 0 到 1 的信号值：

```text
0 = 对 AI 风险暴露最不支持
1 = 对 AI 风险暴露最支持
```

### 3.1 线性区间规则

有些指标使用 `scale_min` 和 `scale_max`。

简化公式：

```text
normalized = (value - scale_min) / (scale_max - scale_min)
normalized = clamp(normalized, 0, 1)
```

例如毛利率越高越好，配置可能说 35% 到 70% 是评分区间。低于下限接近 0，高于上限接近 1，中间按比例计算。

为什么要这样做：

- 不同指标单位不同，不能直接相加。
- 归一化后才能进入同一个评分框架。

### 3.2 阈值方向规则

有些指标不是越大越好。

例如 VIX：

```text
bullish_below = 16
bearish_above = 25
```

直觉：

- VIX 低于 16，市场更平静，信号接近 1。
- VIX 高于 25，市场更紧张，信号接近 0。
- 16 到 25 之间线性过渡。

对于“越高越好”的指标，会使用相反方向，例如：

```text
bullish_above = 0
```

`SMH/SPY` 相对强弱收益率高于 0，表示半导体跑赢大盘，信号更好。

### 3.3 信号点数

每个信号都有 `points`。

```text
earned_points = normalized * points
```

为什么要有点数：

- 不是所有信号同等重要。
- 例如估值分位可能比某个短期变化更重要，配置可以给它更多点数。

设计思路：

- 阈值、点数和分数区间都放在配置里，并带有 policy metadata。
- 它们属于投资解释规则，不应变成无说明的代码常量。

常见误解：

- 0 分不一定代表资产一定下跌，只代表这个指标在当前规则下不支持提高 AI 风险暴露。
- 1 分不代表没有风险，只代表这个指标本身偏支持。

## 第 4 步：生成 component score

当前核心模块包括：

- `trend`：价格趋势和主题相对强弱；
- `fundamentals`：企业经营质量和投入强度；
- `macro_liquidity`：利率、美元和宏观流动性；
- `risk_sentiment`：VIX 和市场风险偏好；
- `valuation`：估值和拥挤程度；
- `policy_geopolitics`：政策、地缘和供应链风险事件。

每个模块有自己的信号列表。模块分数的简化公式是：

```text
component_score =
  (earned_points + missing_points * neutral_score / 100)
  / total_points
  * 100
```

其中：

- `earned_points` 是可用信号按归一化结果得到的点数；
- `missing_points` 是缺失信号的点数；
- `neutral_score` 当前通常是 50；
- `total_points` 是该模块所有信号点数总和。

为什么缺失信号用中性分：

- 缺失数据不能被当作好消息，也不能被当作坏消息。
- 直接删除缺失信号会让剩余少量信号过度决定模块分数。
- 用中性分能保持谨慎，但不会无故制造极端结论。

覆盖率逻辑：

```text
coverage = available_points / total_points
```

如果覆盖率低于 `minimum_signal_coverage`，模块会进入低覆盖或 `insufficient_data`，通常使用中性分并降低信心。

为什么要看覆盖率：

- 一个模块有 10 个信号，只拿到 1 个信号时，即使这个信号很好，也不应该给出很高确定性。

设计思路：

- component score 描述“这个模块从方向上支持多少”。
- source type 和 coverage 描述“这个模块有多可信”。
- 这两者不能混在一个数字里。

常见误解：

- 50 分不是“看涨 50%”，而是该模块在当前规则下接近中性。
- 模块分数高不等于最终仓位一定高，因为后面还有权重、信心和 gate。

## 第 5 步：用 effective weights 合成 overall score

主要输入：

- `config/weights/weight_profile_current.yaml`
- `config/weights/approved_weight_overlays.yaml`
- `outputs/current_context.json`
- 模块 component score

主要输出：

- `outputs/current_effective_weights.json`
- `scores_daily.csv` 的 overall 行；
- 日报中的 `Effective weights`。

基础权重大致表示当前系统认为各模块的长期重要性。例如当前生产权重是：

```text
trend: 0.25
fundamentals: 0.25
macro_liquidity: 0.15
risk_sentiment: 0.15
valuation: 0.10
policy_geopolitics: 0.10
```

overall score 的简化公式：

```text
overall_score = sum(component_score[module] * effective_weight[module])
```

权重通常会归一化到总和为 1。

为什么要用权重：

- 趋势、基本面、宏观、估值和政策风险不是同一类证据。
- 系统需要明确“哪个证据类别更重要”，而不是暗中平均。

approved overlay：

- approved overlay 是经过治理批准的上下文修正。
- 它可以影响 effective weights、confidence delta 或 soft position multiplier。
- 命中 overlay 会写入日报、CSV 和 trace。

shadow 权重：

- shadow weight profiles 只用于 validation 或观察。
- 未批准的 shadow 结果不得进入 production overall score。

设计思路：

- 权重变化必须可审计。
- 生产权重和 validation 权重必须隔离，避免参数搜索污染日报结论。

常见误解：

- overall score 不是所有输入的简单平均。
- 参数搜索里表现好的权重不等于已经批准为 production 权重。

## 第 6 步：把 overall score 映射成 model position

主要输入：

- `config/scoring_rules.yaml` 的 `position_bands`
- `overall_score`

主要输出：

- `model_position_band`
- `scores_daily.csv` 的 `model_risk_asset_ai_min/max`
- 日报中的评分映射仓位。

当前配置示例：

```text
score >= 80 -> 80%-100%
score >= 65 -> 60%-80%
score >= 50 -> 40%-60%
score >= 35 -> 20%-40%
score >= 0  -> 0%-20%
```

为什么是区间，不是单点：

- 投研判断不是精确到 1 个百分点的机械动作。
- 区间能表达不确定性，也方便后续 gate 施加上限。

设计思路：

- model position 是“只看 score 后”的初始仓位语言。
- 它还没有经过 confidence、macro budget、valuation、risk event、thesis、data confidence 等完整约束。

常见误解：

- `model_position = 60%-80%` 不代表系统已经建议最终持有 80%。
- 它只是进入 gate 前的模型原始仓位区间。

## 第 7 步：计算 confidence adjusted position

主要输入：

- component score 的 source type；
- component coverage；
- data quality / feature / fundamental / manual review warnings；
- `config/scoring_rules.yaml` 的 `confidence_policy`。

主要输出：

- `confidence_score`
- `confidence_level`
- `confidence_position_band`
- 日报中的判断置信度。

confidence 的简化逻辑：

```text
raw_confidence =
  weighted_average(component_confidence, effective_weights) * 100

confidence_score =
  raw_confidence
  - data_quality_penalty
  - feature_warning_penalty
  - fundamental_warning_penalty
  - manual_review_penalty
```

然后把结果限制在 0 到 100。

为什么 confidence 独立于 score：

- 分数可以高，但证据来源可能不足。
- 分数可以中性，但数据质量可能很高。
- 这两个维度表达的是不同问题。

仓位影响：

`confidence_policy.position_cap_bands` 会按 confidence score 给出 cap multiplier。例如低信心时，model position 的上限会被打折。

设计思路：

- 系统允许“方向看起来不错，但证据不够，所以仓位不能给满”。
- 这比把所有问题塞进 score 更容易复核。

常见误解：

- 低 confidence 不等于看空。
- 它更多表示“这次判断需要更保守地使用”。

## 第 8 步：计算 macro risk asset budget

主要输入：

- `config/portfolio.yaml`
- `^VIX` 特征；
- `DGS10` 利率变化；
- `DTWEXBGS` 美元变化。

主要输出：

- 总风险资产预算调整；
- 日报中的 `Macro risk budget`；
- position gate 的风险预算输入。

简化逻辑：

1. 从静态风险资产预算开始，例如总资产中 60%-80% 是风险资产。
2. 检查 VIX、VIX 分位、利率变化、美元变化是否达到 elevated 或 stress 阈值。
3. 如果触发 elevated，则把风险资产预算上限压到较低区间。
4. 如果触发 stress，则压到更低区间。

为什么要有宏观预算：

- AI 主题本身可能不错，但整个市场处于高压力环境时，总风险承受能力应该下降。
- 它像组合层面的总刹车，不是某个模块的看涨或看跌结论。

设计思路：

- 先决定总体风险预算，再讨论 AI 这部分在风险资产中的占比。
- 这样可以避免在市场压力很高时，单靠 AI 主题高分给出过高风险暴露。

常见误解：

- macro budget 不是预测 VIX 或利率。
- 它只根据当前观测到的宏观压力限制风险上限。

## 第 9 步：经过 position gates 得到 final position

主要输入：

- `model_position_band`
- `confidence_position_band`
- macro risk asset budget
- portfolio exposure
- valuation snapshot
- risk event occurrences
- thesis validation
- data quality and PIT coverage
- `config/scoring_rules.yaml`
- `config/portfolio.yaml`

主要输出：

- `final_position_band`
- `gate_summary`
- `binding_gate`
- 日报中的 `Binding Gate Ladder`
- decision snapshot 中的 gate 记录。

gate 的直觉是“刹车”。每个 gate 都给出一个仓位上限：

```text
final_position_max = min(all_applicable_caps)
```

如果某个 gate 的 cap 比 model position 上限更低，它就会触发。最低的有效上限通常就是 binding gate。

### 9.1 confidence gate

低 confidence 会限制仓位上限。

为什么：

- 证据不够可靠时，即使 score 高，也应该保守。

### 9.2 calibration overlay soft cap

如果 approved overlay 给出 position multiplier，系统会把 model position 上限乘以该 multiplier。

为什么：

- 历史校准可能发现某些上下文下原始分数需要更保守。
- 但只有 approved overlay 能影响 production。

### 9.3 portfolio limits

系统会检查 AI 暴露、单票集中度和组合限制。

为什么：

- 投资组合不能只看主题吸引力，还要控制集中风险。

### 9.4 risk budget gate

系统会根据市场压力和组合集中度限制 AI 风险资产仓位。

为什么：

- 高 VIX、组合过度集中、ETF beta 覆盖不足，都可能让同样的 AI 分数对应更高实际风险。

### 9.5 risk event gate

高等级政策或地缘风险事件可以降低仓位上限。

为什么：

- 出口管制、供应链中断、监管变化等事件可能让短期仓位风险超过 score 本身能表达的范围。

### 9.6 valuation gate

估值过热或拥挤时会限制仓位。

当前配置示例：

```text
EXPENSIVE_OR_CROWDED -> max_position 70%
EXTREME_OVERHEATED   -> max_position 40%
```

为什么：

- 好资产太贵时也需要留安全边际。
- 估值 gate 不等于看空，它只是不允许过高仓位。

### 9.7 thesis gate

交易 thesis 校验失败或警告会限制仓位。

为什么：

- 系统需要明确“为什么持有或观察”。
- 缺少 thesis、thesis 过期或关键假设失效时，不能只靠市场特征维持高仓位。

### 9.8 data confidence gate

数据质量失败、PIT 覆盖不足、placeholder 或 insufficient data 会限制仓位。

为什么：

- 数据不可信时，正确行为是限制输出，而不是让模型继续给高仓位。

设计思路：

- final position 使用最严格上限，而不是平均 gate。
- 平均会把严重风险稀释掉；最小值能保留“任一关键风险足够严重就先收手”的治理逻辑。

常见误解：

- binding gate 不是系统唯一关心的风险，只是当前最严格的风险。
- gate 触发不一定代表看空，很多时候只是估值过高、证据不足或组合暴露太集中。

## 第 10 步：生成日报、snapshot、trace 和 ledger

主要输出：

- `outputs/reports/daily_score_YYYY-MM-DD.md`
- `data/processed/scores_daily.csv`
- `data/processed/decision_snapshots/decision_snapshot_YYYY-MM-DD.json`
- `outputs/reports/evidence/daily_score_YYYY-MM-DD_trace.json`
- `data/processed/prediction_ledger.csv`
- `outputs/reports/evidence_dashboard_YYYY-MM-DD.html`

这些产物分工不同：

| 产物 | 面向谁 | 用途 |
| --- | --- | --- |
| 日报 Markdown | 人 | 阅读结论、原因、输入、gate 和复核问题 |
| `scores_daily.csv` | 人和程序 | 保存模块分数、overall score、仓位字段和权重 |
| decision snapshot | 程序和审计 | 保存某日生产判断上下文，供 replay、feedback、shadow 使用 |
| trace bundle | 审计 | 保存 claim、evidence、dataset、quality、run manifest、rule versions |
| prediction ledger | feedback | 记录当时的 prediction，后续对照 outcome |
| evidence dashboard | 人 | 本地静态下钻展示，不重新计算结论 |

设计思路：

- 日报负责解释。
- snapshot 负责复现。
- trace 负责审计。
- ledger 负责后验校准。
- dashboard 负责可视化阅读。

常见误解：

- 日报不是回测报告。
- trace 不是日报正文。
- ledger 后续 outcome 不能改写 signal-time 输入。
- dashboard 不替代 Markdown 日报和 trace bundle 的审计责任。

## 第 11 步：shadow 和参数搜索如何计算，但为什么不影响 production

主要输入：

- production decision snapshots；
- 历史价格缓存；
- shadow weight / gate profiles；
- parameter search space；
- parameter objective；
- promotion contract。

主要输出：

- `outputs/parameter_search/<run_id>/trials.csv`
- `outputs/parameter_search/<run_id>/pareto_front.csv`
- `outputs/parameter_search/<run_id>/best_profiles.yaml`
- `outputs/parameter_search/<run_id>/search_report.md`
- `data/processed/prediction_ledger_flow_validation.csv`

简化计算逻辑：

1. 读取历史 production snapshot，不改写原始生产结论。
2. 对每个候选参数组合重算 shadow position。
3. 用后续价格计算这个 shadow position 对应的 position-weighted return。
4. 比较收益、回撤、换手、胜率、样本缺失和 objective。
5. 输出 top trial、Pareto front、attribution 和 promotion readiness。

position-weighted return 的直觉：

```text
position_weighted_return = position * asset_return
```

如果某天资产收益是 2%，仓位是 40%，这天对组合的简化贡献就是 0.8%。真实回测还会考虑更多成本和约束。

为什么 validation-only：

- 参数搜索很容易过拟合历史。
- 即使某个 trial 历史表现好，也可能只是碰巧适配过去样本。
- 所以它必须先经过 forward shadow、样本门槛、治理复核和批准，才能影响 production。

设计思路：

- shadow 负责提出候选。
- promotion contract 负责判断是否进入下一阶段。
- production scoring 不读取未批准的 shadow trial。

常见误解：

- `diagnostic-leading` 不等于可上线。
- `best_profiles.yaml` 不等于生产权重。
- `prediction_ledger_flow_validation.csv` 不是正式 prediction ledger。

## 一个完整的简化例子

假设某天模块分数是：

| 模块 | component score | effective weight |
| --- | ---: | ---: |
| trend | 72 | 0.25 |
| fundamentals | 68 | 0.25 |
| macro_liquidity | 55 | 0.15 |
| risk_sentiment | 61 | 0.15 |
| valuation | 42 | 0.10 |
| policy_geopolitics | 70 | 0.10 |

overall score 计算：

```text
72 * 0.25
+ 68 * 0.25
+ 55 * 0.15
+ 61 * 0.15
+ 42 * 0.10
+ 70 * 0.10
= 63.6
```

score band：

```text
63.6 -> 40%-60%
```

假设 confidence 是 high，没有额外压缩，则：

```text
confidence adjusted position -> 40%-60%
```

假设宏观预算允许最高 60%，则：

```text
macro risk budget cap -> 60%
```

假设估值状态是 `EXTREME_OVERHEATED`，配置上限 40%：

```text
valuation gate cap -> 40%
```

其他 gate：

```text
risk event cap -> 70%
thesis cap -> 100%
data confidence cap -> 80%
```

最终取最严格上限：

```text
final_position_max = min(60%, 60%, 40%, 70%, 100%, 80%) = 40%
binding_gate = valuation
```

这一天的解释不是“系统看空 AI”，而是：

- 趋势和基本面仍有支持；
- overall score 给出中性偏建设性的模型仓位；
- 但估值过热让系统不允许更高仓位；
- final position 被 valuation gate 限制在 40%。

这就是日报里 `Score-to-Position Funnel` 和 `Binding Gate Ladder` 要展示的内容。

## 每一步为什么不直接合并成一个黑箱分数

系统刻意拆成多步，是为了满足四个要求：

1. 可解释：使用者能看到每个输入如何影响输出。
2. 可审计：每个结论都能反查来源、配置、证据和运行上下文。
3. 可治理：阈值、权重、gate 和 promotion 不能被无记录地修改。
4. 可复核：当结论不合理时，能定位是数据、特征、评分、权重、gate 还是报告展示的问题。

如果把所有东西合成一个分数，短期看起来简单，长期会出现三个问题：

- 不知道结果为什么变了；
- 不知道哪个数据源或规则造成了变化；
- 不知道 shadow / validation 结果是否污染了 production。

当前设计宁愿多输出几个可读 artifact，也不让关键投资解释藏在黑箱里。

## 看到异常时应该先查哪里

如果最终仓位很低：

1. 看日报的 `Binding Gate Ladder`。
2. 找最低 cap 的 gate。
3. 再看该 gate 的证据来源，例如 valuation snapshot、risk occurrences、thesis 或 data quality report。

如果分数和直觉不一致：

1. 看 `Score-to-Position Funnel` 的 component score。
2. 找最低或最高的模块。
3. 查 `docs/schema/fields.yaml` 和 trace bundle 中对应字段。

如果报告说 validation-only：

1. 先确认 `production_effect`。
2. 不要把 shadow trial、parameter search 或 dashboard 诊断解读为 production 建议。
3. 看 promotion contract 是否允许进入下一阶段。

如果数据缺失或信心低：

1. 看 `data_quality_YYYY-MM-DD.md`。
2. 看 `feature_availability_YYYY-MM-DD.md`。
3. 看日报的 Data Lineage Card 和 trace bundle 的 quality refs。

## 与其他文档的关系

- `docs/learning_path.md`：告诉你按什么顺序学习系统。
- `docs/artifact_catalog.md`：告诉你每个文件由谁生成、被谁消费、是否影响 production。
- `docs/schema/fields.yaml`：告诉你关键字段的含义、来源和常见误解。
- `docs/system_flow.md`：维护工程事实和全链路数据流。
- `outputs/reports/daily_score_YYYY-MM-DD.md`：展示某一天实际算出来的结果。
