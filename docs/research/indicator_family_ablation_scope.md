# Indicator Family Ablation Scope

状态：`INDICATOR_FAMILY_ABLATION_SCOPE_READY`

本批只做 indicator family evidence、channel-specific action-value diagnosis 和 selection rule gating。

## 本批不做

- `dynamic promotion`
- `paper-shadow`
- `production`
- `broker`
- `universal first-layer training`
- `second-layer weight optimization`
- `portfolio weight output`

所有输出固定 `promotion_allowed=false`、`paper_shadow_allowed=false`、`production_allowed=false`、`broker_action=none`。
