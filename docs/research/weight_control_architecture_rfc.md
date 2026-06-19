# Weight Control Architecture RFC

最后更新：2026-06-19

状态：`RFC_READY_FOR_RESEARCH_ONLY_IMPLEMENTATION`

## 架构

```text
Strategic baseline weight
+ Slow relative tilt
+ Fast asymmetric risk overlay
+ Confidence shrinkage
+ Execution / turnover control
```

## 分层接口

|层|输入|输出|禁止输出|
|---|---|---|---|
|Signal layer|price/market/risk/fundamental features、source refs|normalized signal state、confidence、data quality status|target weight、order、broker action|
|Allocator layer|baseline weights、slow score、confidence|research-only hypothetical allocation、tilt explanation|official target weight、production mutation|
|Risk-control layer|drawdown/volatility/stress state|risk exposure scaler、reentry state|研究成功结论、交易指令|
|Execution layer|previous/candidate allocation、cost policy、turnover budget|simulated rebalance path、turnover/cost proxy、no-trade reason|order ticket、broker routing|

## 核心原则

- 不再用单一 regime-to-weight 规则承担 signal、allocation、risk 和 execution。
- risk-off 和 risk-on 支持非对称速度。
- 默认输出向 strategic baseline 收缩。
- 所有权重输出都必须标记为 `research_only_hypothetical_allocation`，不得写成
  official target weights。

当前状态：B0 static strategic baseline control 和 B1 execution-control runner 已完成；B1
证据为 mixed。B2-B6 仍缺独立 runner 和 signal robustness evidence，不能把 P0 动态策略总结果
解释为该分层架构的消融完成。
