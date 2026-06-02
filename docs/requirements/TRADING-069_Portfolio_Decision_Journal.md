# TRADING-069 Portfolio Decision Journal and Human Review Notes

状态：IN_PROGRESS

最后更新：2026-06-02

## 背景

TRADING-068 已把 ETF baseline、shadow candidates、AI confirmation、satellite
replacement、risk/watchlist 和 manual review actions 汇总为周度人工复核包。
TRADING-069 在该周度复核包之后新增人工决策日志，把 owner/reviewer 对每个
action item 的接受、拒绝、延期、继续观察、watch、归档、实验或数据请求记录成
结构化 journal。

本阶段只记录人工判断和后续研究动作，不自动改变 production weights、shadow
weights、broker routing 或 candidate lifecycle state。

## 安全边界

所有 journal entry、report、analytics 和 validation 输出必须固定：

```text
observe_only=true
candidate_only=true
production_effect=none
broker_action=none
manual_review_required=true
```

允许记录：

```text
human review notes
accepted/rejected/deferred recommendations
rationale
confidence
follow-up tasks
linked candidates
linked reports
candidate state update proposals
```

禁止记录或执行：

```text
place_order
enable_broker_action
promote_to_production_without_governance
production_weight_update
broker_order
automatic_candidate_promotion
automatic_candidate_rejection
```

## Journal Schema

每个 active entry 至少包含：

```text
review_id
decision_id
review_date
source_weekly_review
action_item_id
human_decision
decision_status
rationale
confidence
follow_up_task
linked_candidate
linked_report
created_at
```

`decision_status` 只能是：

```text
accept_recommendation
reject_recommendation
defer_decision
continue_observation
mark_watch
archive_candidate_after_review
start_new_experiment
request_more_data
```

Implementation may add audit fields such as `updated_at`, `audit_trail`,
`source_section`, `source_action_type`, `action_item_snapshot`,
`source_evidence`, and safety fields, provided the required fields remain
present.

## 阶段拆解

|阶段|范围|状态|验收|
|---|---|---|---|
|TRADING-069A|Decision Journal Schema|DONE|Schema validates required fields, decision_status enum, confidence, safety fields and disallowed actions|
|TRADING-069B|Manual Review Notes CLI|IN_PROGRESS|`aits etf decision-journal add/update/list/remove` can persist and edit local journal entries without production mutation|
|TRADING-069C|Weekly Review Action Linking|READY|Each entry references an existing TRADING-068 weekly review and action item, including source section and evidence|
|TRADING-069D|Candidate Decision State Update Proposal|READY|Journal can generate candidate state proposals without mutating shadow registry or production state|
|TRADING-069E|Journal Report Generator|READY|Generate JSON/Markdown/HTML journal summary with metadata, decisions, links, rationale, follow-ups and audit trail|
|TRADING-069F|Reader Brief Decision Journal Summary|READY|Reader Brief displays key decision counts/statuses and links to journal report|
|TRADING-069G|Review Outcome Analytics|READY|Analytics summarize decision status distribution, confidence and follow-up workload from journal entries|
|TRADING-069H|Decision Journal Validation Gate|READY|`aits etf decision-journal validate` fail-closed checks links, action items, safety fields and disallowed actions|

## 验收标准

- Schema tests cover required fields, enum values, confidence bounds, safety
  fields and disallowed actions.
- CLI tests cover add, update, list, remove and links to weekly review action
  items.
- Journal entries must reference a TRADING-068 weekly review report and an
  existing `manual_review_actions[].action_id`.
- Report generator outputs JSON, Markdown and HTML with review metadata, human
  decision summary, linked candidates/reports, confidence/rationale notes,
  follow-up tasks and audit trail.
- Candidate state proposal output is observe-only and does not mutate
  `data/simulation/etf_shadow_candidates.json` or any production config.
- Reader Brief summary section reads latest journal report from report index,
  shows key decision statuses and links to the detailed journal.
- Validation gate passes only when all entries satisfy source links, action item
  existence, safety fields and disallowed-action checks.
- README、`docs/system_flow.md`、`docs/artifact_catalog.md`、
  `config/report_registry.yaml`、operations runbook 和 task register 同步。

## 进展记录

- 2026-06-02：TRADING-069 新增为 P0 `IN_PROGRESS`。根据 owner 提供的
  Portfolio Decision Journal 计划开始按 A-H 顺序实现；本阶段只持久化人工
  weekly review notes 和 candidate-only follow-up，不写 production weights、
  不触发 broker action、不自动 promotion/rejection。
- 2026-06-02：TRADING-069A 完成。新增
  `src/ai_trading_system/etf_portfolio/decision_journal.py` 基础 schema、
  safety constants、decision_status enum、disallowed action blocking、journal
  load/write 验证和 schema tests。验证通过 `python -m pytest
  tests/test_etf_decision_journal.py -q`、目标 ruff 和 `git diff --check`。
