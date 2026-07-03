# High-Intensity Actual-Path Evidence Contract

本 contract 定义每个 high-intensity observe event 后续需要绑定的 actual-path evidence。未来 outcome 只能在事件发生后填充，不得在 event creation 时使用未来数据。

- horizon_1d_required: `True`
- horizon_5d_required: `True`
- horizon_10d_required: `True`
- horizon_20d_required: `True`
- allowed_horizons: `['1d', '5d', '10d', '20d']`
- pit_outcome_binding: `OUTCOME_BOUND_AFTER_EVENT_ONLY`
- manual_review_only: `True`
- promotion_allowed: `False`
- paper_shadow_allowed: `False`
- production_allowed: `False`
- broker_action: `none`
