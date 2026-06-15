# TRADING-349 Candidate Decision Ledger

最后更新：2026-06-15

## 1. 背景

Filtered candidate chain 已经形成 evidence、stress、drawdown mismatch reduction、flip/rotation
reduction、A/B review、signal gate confirmation、owner review、formal research contract 和
paper-shadow protocol artifacts。当前缺口是这些 decision 仍分散在多个 artifact 中，没有一个
append-only ledger 记录候选决策轨迹。

## 2. 目标

1. 新增 candidate decision ledger schema。
2. 记录 candidate id、evidence status、stress result、mismatch result、rotation result、
   A/B result、confirmation count、owner action、final decision、next required action。
3. 提供 append-only JSONL ledger behavior。
4. 新增 validate CLI。
5. 新增 report CLI 和 Reader Brief summary。
6. 文档化 ledger 用途和 safety boundary。
7. 新增 focused tests。

## 3. 非目标

- 不创建 target weights。
- 不写 production strategy state。
- 不生成 order ticket。
- 不触发 broker action。
- 不把 ledger entry 当作 owner approval 或 production promotion。

## 4. Ledger Contract

Ledger canonical file:

- `reports/etf_portfolio/dynamic_v3_rescue/candidate_decision_ledger/candidate_decision_ledger.jsonl`

每次 `record` 追加一条 record，并创建一个 run-specific artifact 目录：

- `candidate_decision_ledger_manifest.json`
- `candidate_decision_record.json`
- `candidate_decision_ledger_snapshot.jsonl`
- `candidate_decision_ledger_report.md`
- `reader_brief_section.md`
- `candidate_decision_ledger_validation.json/md`

Append-only 规则：

- `record` 只追加，不重写既有 ledger JSONL。
- run-specific snapshot 只复制当次读取到的 ledger state，便于 report index 和 Reader Brief 下钻。
- validation 必须确认当前 record 存在于 ledger snapshot 和 canonical ledger。

## 5. Safety Boundary

所有 ledger payload 固定：

- `production_effect=none`
- `manual_review_only=true`
- `candidate_decision_ledger_only=true`
- `not_official_target_weights=true`
- `broker_action_allowed=false`
- `broker_action_taken=false`
- `order_ticket_generated=false`
- `production_state_mutated=false`
- `production_candidate_generated=false`
- `automatic_candidate_promotion=false`

## 6. 验收标准

- `candidate-decision-ledger record/report` 可运行。
- `validate-candidate-decision-ledger` 返回 PASS。
- Reader Brief 显示 ledger id、candidate、final decision、owner action、next action。
- README、operations runbook、system flow、artifact catalog、report registry、task register 和本文同步更新。
- focused pytest、contract-validation suite、ruff、compileall、git diff check、documentation contract、report index 和 Reader Brief quality 通过。

## 7. 进展记录

- 2026-06-15：新增任务并进入 IN_PROGRESS；本阶段只实现 research-only append ledger，不写 target weights、不接 broker。
- 2026-06-15：实现完成并转入 VALIDATING；真实链路生成 ledger `candidate-decision-ledger_35b2b3c53ce03111`，`final_decision=FORMALIZE_RESEARCH_METHOD`、`next_required_action=start_daily_paper_shadow_runner_design`、validator `status=PASS` / failed=0；Reader Brief JSON 已显示 candidate decision ledger fields；focused pytest 4 passed，contract-validation suite 21 passed / 17.82s，documentation contract PASS，report index `PASS_WITH_WARNINGS` 仅保留既有 missing/stale visibility，Reader Brief OK，Reader Brief quality OK；保持 append-only / no official target / no broker / no order / no production。
