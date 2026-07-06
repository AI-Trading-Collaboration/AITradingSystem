# 动态策略 TRADING-2393 路由

- status：`DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_AND_GATE_EVIDENCE_PLAN_READY`
- 推荐下一路由：`TRADING-2393_Dynamic_Strategy_Component_Attribution_Targeted_Ablation_Retest`
- 下一步：component attribution targeted ablation retest
- TRADING-2392 是否执行 ablation retest：`False`
- observation approved：`False`
- paper-shadow enabled：`False`
- production enabled：`False`
- broker action enabled：`False`

TRADING-2393 只有在读取 cached market data 时先执行必需的数据质量门禁后，才允许运行 targeted ablation retest。