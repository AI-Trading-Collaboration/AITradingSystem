# Base + Overlay + Veto Policy Design

状态：`BASE_OVERLAY_VETO_FRAMEWORK_READY`

本设计只建立 schema 和 compiler contract，不表示 growth overlay 可用于 allocation candidate。

## 结构

- Base portfolio：`QQQ=0.60`、`SGOV=0.40`、`TQQQ=0.00`。
- Defensive overlay：只能消费 defensive channel，例如 `risk_off` 和 `defensive_hold`。
- Growth overlay：schema 中保留 `stay_constructive`、`add_risk`、`risk_on_diagnostic` 的 delta，但当前 return-seeking signals 仍 diagnostic-only，因此 compiler 必须按 usage matrix 阻断。
- Risk veto：`risk_off_veto`、`volatility_veto`、`event_risk_veto`、`trend_break_veto`、`tqqq_veto` 优先级最高。

## 约束

- 输出 long-only、sum-to-one。
- `TQQQ_max_weight=0.05`。
- `QQQ_equivalent_exposure_max=0.75`，其中 `TQQQ` 按 3 倍 QQQ-equivalent 处理。
- `turnover_max=0.25` 只作为 research compiler guardrail，不是 production constraint。
- 第二层不得读取 raw indicators。

## 安全边界

所有 target weights 都是 framework / research-only 输出，不是交易建议。Promotion、paper-shadow、production、broker 继续 disabled。
