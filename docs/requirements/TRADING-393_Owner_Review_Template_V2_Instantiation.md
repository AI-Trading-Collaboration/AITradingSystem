# TRADING-393 Owner Review Template V2 Instantiation

## 背景

TRADING-364 建立了 reusable owner review template v2 和 filled review validation
contract。TRADING-384 的 owner review 已明确 recommended owner action 为 `hold`。
TRADING-391/392 已补齐 recovery evidence pack 和 append-only owner hold decision。

本任务把 latest owner review 结构化为 filled owner review template v2 JSON，使后续
monthly review、promotion board 和 recovery pack 可以引用同一套 owner interpretation 字段。
该 artifact 只表达 manual owner review 解释，不追加新的 owner decision，不改变 audit log 或
candidate/paper-shadow/production state。

## 范围

- 读取/引用 `outputs/reports/TRADING-384_owner_review_2026-06-17.md` 的 owner hold
  结论。
- 创建 filled owner review JSON，字段覆盖 template v2 required fields。
- 链接 recovery evidence pack、owner decision audit log、research safety boundary、report
  index、cost review 和 benchmark baseline control evidence。
- 运行现有 `aits reports owner-review-template-v2 --as-of 2026-06-17`。
- 运行现有 `aits reports validate-owner-review-template-v2 --latest --review-json-path ...`。

## 安全边界

- Final owner action 固定为 `hold`。
- 不 append owner decision audit log。
- 不运行上游 evidence collection。
- 不刷新 data/cache。
- 不修改 strategy output、candidate state、paper-shadow state 或 production state。
- 不批准 normal paper-shadow resumption、promotion、extended shadow 或 live trading。
- 不生成 official target weights，不触发 broker action 或 order ticket。

## 验收标准

- Filled owner review JSON 包含 `candidate_id`、`evidence_interpretation`、
  `main_reason_to_continue`、`main_reason_to_reject`、`uncertainty`、
  `required_follow_up`、`final_owner_action`、`linked_input_artifacts` 和
  `safety_status`。
- `final_owner_action=hold`，`safety_status=SAFETY_PASS_WITH_WARNINGS`。
- Filled review validation `PASS`，failed checks=0。
- Template report `TEMPLATE_READY`。
- Focused tests、ruff、compileall、documentation contract、report index、Reader Brief
  quality 和 git diff check 通过。

## 进展记录

|日期|状态|记录|
|---|---|---|
|2026-06-17|IN_PROGRESS|新增 requirements 和 task-register 行；准备创建 filled owner review JSON 并用现有 template v2 validation CLI 校验。|
|2026-06-17|DONE|新增 filled owner review JSON `docs/owner_reviews/TRADING-393_owner_review_template_v2_2026-06-17.json`，final owner action=`hold`、safety=`SAFETY_PASS_WITH_WARNINGS`。真实 template `outputs/reports/owner_review_template_v2_2026-06-17.json/md` 输出 `TEMPLATE_READY`；validation `outputs/reports/owner_review_template_v2_validation_2026-06-17.json/md` 输出 `PASS`、checks=22、failed=0、review_record_provided=true、review_record_failed=0。Focused fixture regression 通过。该 artifact 不 append audit log、不授权 normal shadow resume、promotion、extended shadow、official target、broker/order、live trading 或 production mutation。|
