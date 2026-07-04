# High-Intensity Scheduler Disabled Wiring Policy

- scheduler_enabled: `False`
- scheduler_default_enabled: `False`
- activation_requires_owner_review: `True`
- activation_not_allowed_in_2346: `True`
- broker_action: `none`

Scheduler config 必须 disabled-by-default。任何 activation 都必须进入后续
owner-approved observe-only activation plan，2346 和 2347 均不能启用 scheduler。