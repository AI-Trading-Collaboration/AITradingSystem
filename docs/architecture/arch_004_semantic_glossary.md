# ARCH-004 研究语义词典

最后更新：2026-07-21

当前机器源：`config/architecture/research_semantic_glossary_v2.yaml`；原
`config/architecture/research_semantic_glossary.yaml` 作为 ARCH-004 Phase A 冻结历史保留。

## 结论

Owner 已统一后续 active 策略研究和主要结论从 `2021-02-22` 开始；
`2022-12-01` 只保留为历史 ChatGPT-cycle comparison：

| 术语 | 当前值 | 作用 |
|---|---:|---|
| active project market regime | `unified_primary_2021` | 新研究、默认回测与主要结论的解释 regime |
| active regime / research anchor | `2021-02-22` | QQQ/SGOV/TQQQ 共同验证历史起点 |
| QQQ/SGOV/TQQQ primary research-window start | `2021-02-22` | first-layer、second-layer、actual-path、main leaderboard 与 owner-review 的主验证起点 |
| legacy comparison start | `2022-12-01` | `ai_after_chatgpt` 历史比较，不提供 active default 或 primary evidence |
| sensitivity start | `2020-05-28` | 带 SGOV secondary-source gap caveat 的稳健性检查 |

因此 active default 与 primary research window 都从 `2021-02-22` 开始，但字段仍不得混用；
报告至少要说明 market regime、research window、legacy comparison、sensitivity，以及某次命令的
requested/actual range。

## 日期与覆盖率必须分开

- `requested_date_range`：用户、CLI 或 workflow 要求评估的范围；
- `actual_date_range`：经过共同可交易日和验证约束后真正计算的范围；
- `effective_coverage`：各项输入实际可用的 PIT/freshness/completeness 覆盖；
- `as_of`：本次评估最多允许看到哪个时点的信息；
- `data_quality_as_of`：数据质量结论对哪个日期有效；
- `generated_at`：文件何时写出，不代表当时可见的市场信息。

这些字段可以不同，任何一个都不得静默替代另一个。

## 冲突处理

解析优先级固定为：显式 workflow/CLI 值、governed domain profile、scope 匹配时的 project default。没有匹配或存在冲突时必须 fail closed，不能猜测。

每份 investment-facing report 最终都应披露 market regime、research window id、requested/actual range、effective coverage、as-of、data-quality status 和 policy refs。typed `ResearchEvaluationContext` 与 runtime enforcement 已在 ARCH-004B 实现；TRADING-2452 以 versioned v2 glossary 迁移当前 active 语义，不改写 Phase A 历史事实。

## Safety Boundary

本词典不改变 strategy、threshold、weight、research result、promotion 或 production 行为。`production_effect=none`，paper-shadow、production 和 broker 均未解锁。
