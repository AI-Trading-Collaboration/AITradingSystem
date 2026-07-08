# Dynamic strategy TRADING-2418 route

- source task：`TRADING-2417`
- source status：`GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_AND_UPSTREAM_ARTIFACT_CLOSURE_READY`
- 下一任务：`TRADING-2418_Valid_Until_Window_Dependency_Evidence_Closure`
- PIT recheck route：`TRADING-2419_Growth_Tilt_Engine_PIT_Gate_Readiness_Recheck`

TRADING-2418 应关闭 `valid_until_window` dependency evidence。只有在
TRADING-2418 完成后，TRADING-2419 才能重新检查 growth tilt engine 的
PIT gate readiness。TRADING-2417 不授权 candidate search、observation、
paper-shadow、scheduler、event append、outcome binding、production 或 broker/order。
