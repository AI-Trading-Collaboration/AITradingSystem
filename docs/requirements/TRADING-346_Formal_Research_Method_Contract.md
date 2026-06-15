# TRADING-346 Formal Research Method Contract

最后更新：2026-06-15

## 状态

`VALIDATING`

Owner 要求完成附件中的 TRADING-346。本任务承接 TRADING-336～345 的 filtered candidate chain，把 `median_plus_regime_mismatch_filter` 的 evidence、stress、drawdown mismatch reduction、flip/rotation reduction、A/B review、confirmation targets、owner review 和 next decision 映射为可审计 Research Method Contract。

## 背景

TRADING-336～345 当前真实链路结果为：

- evidence=`PROMISING`
- median regime filter spec contract=`PASS`
- stress=`STRONG`
- drawdown mismatch reduction=`IMPROVED`
- flip/rotation reduction=`IMPROVED`
- A/B review=`PROMISING`
- confirmation target count=3
- formalization readiness=`READY_FOR_FORMAL_RESEARCH_IMPLEMENTATION`
- owner action=`formalize_research_method`
- next decision=`FORMALIZE_RESEARCH_METHOD`

这些结果可以支持 formal research method implementation，但仍不得被解释为 official target weights、production mutation、broker integration、order tickets 或 automatic position control。

## Safety Boundary

- `research_screening_only=true`
- `paper_shadow_only=true`
- `manual_review_only=true`
- `formal_research_contract_only=true`
- `not_formal_research_method=true`
- `not_official_target_weights=true`
- `broker_action_allowed=false`
- `broker_action_taken=false`
- `order_ticket_generated=false`
- `auto_apply=false`
- `production_effect=none`

## Promotion States

Research Method Contract 必须定义以下状态：

|State|含义|
|---|---|
|`REJECTED`|核心 evidence 或 safety boundary 失败，候选不应继续作为 formal research method。|
|`NEEDS_MORE_EVIDENCE`|部分证据不足、冲突或缺失，只能继续 research evidence collection。|
|`PROMISING`|核心 evidence 有正向信号，但 owner review、confirmation 或稳定性条件仍不足。|
|`PAPER_SHADOW_ELIGIBLE`|达到观察资格，只能进入 manual-review-only / paper-shadow-only 路径。|
|`FORMAL_RESEARCH_READY`|达到 research-only formal method implementation 条件，仍不等于 production approval。|

## Objective Gates

Contract gate 使用以下输入，不引入新的投资阈值调参：

1. filtered candidate evidence result。
2. stress result。
3. drawdown mismatch reduction status。
4. flip/rotation reduction status。
5. A/B review status。
6. confirmation target count。
7. owner review action。
8. next decision。
9. safety boundary status。

当前 TRADING-346 是治理 contract：它把上一阶段的离散状态组合为审计结论，不调整或拟合 promotion thresholds。后续 TRADING-348 才负责 threshold calibration。

## Required Outputs

- `docs/research_method_contract.md`
- `formal_research_method_contract.json`
- `formal_research_method_decision.json`
- `reader_brief_section.md`
- `formal_research_method_contract_report.md`
- `formal_research_method_contract_validation.json`

Expected decision fields:

- `formal_research_method_status`
- `promotion_state`
- `blocking_reasons`
- `paper_shadow_eligibility`
- `safety_boundary_status`
- `next_required_action`

## Implementation Plan

|Step|状态|验收标准|
|---|---|---|
|Task register and requirements|DONE|任务登记和本文档先于实现更新。|
|Contract builder|DONE|读取 latest 或指定 upstream artifacts，输出 machine-readable contract、decision、report 和 Reader Brief。|
|Validate CLI|DONE|Fail-closed 校验 schema、required gates、safety boundary、paper-shadow conditions 和 source links。|
|Report CLI|DONE|输出 owner-readable 摘要和 artifact path。|
|Documentation and registry|DONE|README、system flow、operations runbook、artifact catalog 和 report registry 同步。|
|Tests and validation|VALIDATING|Focused pytest、CLI smoke、ruff、compileall、git diff check 通过。|

## Progress Notes

- 2026-06-15: 新增并进入 `IN_PROGRESS`。实现范围为 research-only contract artifact、report/validate CLI、Reader Brief section、registry/catalog/docs 更新和 focused tests。
- 2026-06-15: 实现完成并转入 `VALIDATING`。真实 CLI artifact
  `formal-research-method-contract_dee4a8ccd3159771` 写入 ignored runtime path
  `reports/etf_portfolio/dynamic_v3_rescue/formal_research_method_contract/formal-research-method-contract_dee4a8ccd3159771/`；
  decision 为 `formal_research_method_status=READY_FOR_RESEARCH_ONLY_IMPLEMENTATION`、
  `promotion_state=FORMAL_RESEARCH_READY`、`paper_shadow_eligibility=ELIGIBLE_FOR_PROTOCOL_DESIGN`、
  `safety_boundary_status=PASS`；`validate-research-method-contract` 返回 `status=PASS`
  且 `failed_check_count=0`。Latest pointer 写入 ignored
  `reports/etf_portfolio/dynamic_v3_rescue/latest/latest_formal_research_method_contract.json`。
- 2026-06-15: 验证通过 focused pytest `tests/test_formal_research_method_contract.py`
  和 Reader Brief focused summary test，ruff focused check、compileall、`git diff --check`、
  contract build/report/validate CLI、`aits reports index --latest`、`aits docs report-contract --latest`、
  `aits reports reader-brief --latest` 和 `aits reports validate-reader-brief --latest`。Report
  index 为 `PASS_WITH_WARNINGS`，原因是既有 missing/stale visibility；documentation contract
  为 `PASS`；Reader Brief 为 `OK`；Reader Brief quality 为 `LIMITED_READER_CONTEXT`
  且 failed=0。
