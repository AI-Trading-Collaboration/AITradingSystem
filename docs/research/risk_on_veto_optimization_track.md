# Risk-On Veto Optimization Track

状态：`RISK_ON_VETO_TRACK_READY`

本 track 先研究如何减少 false add-risk，而不是主动寻找更高风险仓位。

Veto 类型：

- `risk_off_veto`
- `volatility_veto`
- `event_risk_veto`
- `trend_break_veto`
- `tqqq_veto`

通过条件：

- `false_add_risk_cost` 下降。
- defensive probe regression 下降。
- TQQQ stress risk 不恶化。
- captured upside 没有被完全消除。

Veto active 时，`growth_overlay=0`、`TQQQ_delta=0`、`add_risk=blocked`。当前不允许 promotion、paper-shadow、production 或 broker。
