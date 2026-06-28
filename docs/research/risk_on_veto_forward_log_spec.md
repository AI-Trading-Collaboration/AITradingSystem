# Risk-On Veto Forward Log Spec

`risk_on_veto` forward log 只能记录 observe-only diagnostic observation。

允许字段：

- `date`
- `risk_on_veto_active`
- `veto_reasons`
- `confidence`
- `diagnostic_note`
- future realized outcome fields
- `owner_note`

禁止字段：

- `target_weights`
- `portfolio_weights`
- `trade_action`
- `paper_shadow_action`
- `broker_action`
- `recommended_allocation`
- `target_allocation`
- `qqq_weight`
- `sgov_weight`
- `tqqq_weight`

Forward realized outcomes 可以在尚未成熟时为空。日志不是 paper-shadow action list，不是交易建议，也不能触发 broker 或生产配置修改。
