# Paper Shadow Protocol

最后更新：2026-06-15

本文定义 formal research method contract 之后的 paper-shadow observation protocol。
它只服务研究观察和人工复核，不生成 official target weights，不接 broker，不生成 order
ticket，不修改 production state。

## Purpose

Paper-shadow protocol 的目的，是在候选方法进入任何更长期 paper-shadow 或 formal promotion
讨论之前，记录一段可比较、可审计、可人工复核的每日行为证据。它回答：

- 候选是否按 formal contract 中的预期信号行为运行；
- 风险状态、drawdown handling、rotation/mismatch behavior 是否稳定；
- hypothetical weight recommendation 是否只作为 observation-only 记录；
- 是否应该继续 paper-shadow、返回 research 或 reject。

## Eligibility

候选进入 protocol design 必须同时满足：

- formal research method contract 的 `promotion_state=FORMAL_RESEARCH_READY`；
- formal contract decision 的 `paper_shadow_eligibility=ELIGIBLE_FOR_PROTOCOL_DESIGN`；
- contract validation 为 `PASS`，或还未运行但 source contract safety boundary 为 `PASS`；
- `not_official_target_weights=true`、`broker_action_allowed=false`、
  `order_ticket_generated=false`、`production_effect=none`。

不满足任一条件时，protocol artifact 必须输出 `PROTOCOL_BLOCKED`，下一步返回 research
contract review。

## Observation Period

当前 pilot baseline 要求至少 `20` 个 trading days 的每日 observation 记录。这个值只用于
保证 paper-shadow evidence 不少于约一个交易月，不是 production approval 阈值。后续若进入
extended paper-shadow 或 promotion milestone，需要另开任务把样本期迁移到 reviewed
config/policy manifest，并基于真实 observation evidence 校准。

## Daily Review Fields

每日 paper-shadow observation 至少记录以下字段：

- `signal_output`
- `hypothetical_weight_recommendation`
- `risk_off_risk_on_state`
- `drawdown_state`
- `rotation_event`
- `mismatch_event`
- `benchmark_comparison`
- `manual_reviewer_notes`

`hypothetical_weight_recommendation` 必须明确标记为 paper-shadow-only，不能写入 official
target weights，不能被 broker/order 系统消费。

## Exit Conditions

可用 exit condition 只有：

- `promote_to_extended_paper_shadow`
- `return_to_research`
- `reject`

所有 exit condition 都需要人工复核。`promote_to_extended_paper_shadow` 仍然不是 production
approval，也不能触发 broker、order ticket、portfolio mutation 或 baseline mutation。

## Artifact Contract

`aits etf dynamic-v3-rescue paper-shadow-protocol build` 输出：

- `paper_shadow_protocol_manifest.json`
- `paper_shadow_protocol.json`
- `paper_shadow_protocol_report.md`
- `reader_brief_section.md`
- `paper_shadow_protocol_validation.json/md`

`validate-paper-shadow-protocol` 必须检查 source contract、eligibility conditions、daily
fields、exit conditions、observation period、Reader Brief fields 和 safety boundary。

## Safety Boundary

- `production_effect=none`
- `manual_review_only=true`
- `paper_shadow_protocol_only=true`
- `observation_only=true`
- `not_official_target_weights=true`
- `broker_action_allowed=false`
- `broker_action_taken=false`
- `order_ticket_generated=false`
- `production_state_mutated=false`
- `broker_order_system_consumable=false`

本 protocol 不实现 TRADING-351 daily runner，不初始化 paper shadow account，不修改已有
paper-shadow state。
