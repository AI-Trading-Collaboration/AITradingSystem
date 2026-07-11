# TRADING-204_to_208 Real Manual Snapshot Dry Run and Owner Decision Loop

状态：VALIDATING

最后更新：2026-06-12

## 背景

TRADING-199_to_203 已完成 manual portfolio snapshot hardening、exposure / drift / guardrail 和 manual execution review pack。当前缺口是把 owner 维护的真实手动持仓快照接入 dry-run 闭环，同时继续避免写入账户号、券商凭证、订单号、税务批次、broker statement path 或任何真实交易动作。

本阶段的输入是 owner-maintained manual snapshot，不是 broker import，也不是 official account state。所有输出都是 advisory / paper / weekly review evidence，固定 `production_effect=none`。

## 子任务

|ID|目标|状态|验收标准|
|---|---|---|---|
|TRADING-204|Real Manual Portfolio Snapshot Intake & Redaction-safe Template|VALIDATING|模板可生成；lint/intake 可运行；敏感字段 fail closed；`validate-real-snapshot` PASS|
|TRADING-205|Real Snapshot Advisory Dry Run|VALIDATING|real snapshot 串联 exposure、drift、guardrail、manual review；输出 dry-run summary 和 Reader Brief section；不生成 order ticket|
|TRADING-206|Owner Decision Recording for Manual Execution Review|VALIDATING|从 dry run 创建 owner review；记录 `monitor/no_trade/paper_adjustment_review_only/reject_advisory/needs_more_data/defer`；不改变真实持仓|
|TRADING-207|Paper / Manual Action Tracking from Real Snapshot|VALIDATING|根据 owner decision 生成 paper-only 或 no-action tracking；不修改真实 snapshot；遵守 guardrail capped deltas|
|TRADING-208|Weekly Real Snapshot Advisory Review|VALIDATING|汇总 latest real snapshot dry run、owner decision 和 paper action；输出 weekly summary、owner decision summary 和 Reader Brief section|

## CLI 范围

新增命令：

```bash
aits etf dynamic-v3-rescue real-snapshot template
aits etf dynamic-v3-rescue real-snapshot lint --snapshot config/etf_portfolio/dynamic_v3_rescue/current_portfolio_snapshot.yaml
aits etf dynamic-v3-rescue real-snapshot intake --snapshot config/etf_portfolio/dynamic_v3_rescue/current_portfolio_snapshot.yaml
aits etf dynamic-v3-rescue real-snapshot report --latest
aits etf dynamic-v3-rescue validate-real-snapshot --snapshot-intake-id <snapshot_intake_id>

aits etf dynamic-v3-rescue real-snapshot-dry-run run --snapshot-intake-id <snapshot_intake_id> --shadow-shortlist-id <shadow_shortlist_id>
aits etf dynamic-v3-rescue real-snapshot-dry-run report --latest
aits etf dynamic-v3-rescue validate-real-snapshot-dry-run --dry-run-id <dry_run_id>

aits etf dynamic-v3-rescue real-execution-owner-review create --dry-run-id <dry_run_id>
aits etf dynamic-v3-rescue real-execution-owner-review record --review-id <review_id> --decision monitor
aits etf dynamic-v3-rescue real-execution-owner-review report --latest
aits etf dynamic-v3-rescue validate-real-execution-owner-review --review-id <review_id>

aits etf dynamic-v3-rescue real-snapshot-paper-action apply --owner-review-id <review_id>
aits etf dynamic-v3-rescue real-snapshot-paper-action report --latest
aits etf dynamic-v3-rescue validate-real-snapshot-paper-action --paper-action-id <paper_action_id>

aits etf dynamic-v3-rescue weekly-real-snapshot-review run --week-ending YYYY-MM-DD
aits etf dynamic-v3-rescue weekly-real-snapshot-review report --latest
aits etf dynamic-v3-rescue validate-weekly-real-snapshot-review --weekly-real-review-id <weekly_real_review_id>
```

## Artifacts

新增 runtime roots：

```text
reports/etf_portfolio/dynamic_v3_rescue/real_snapshot_intake/<snapshot_intake_id>/
reports/etf_portfolio/dynamic_v3_rescue/real_snapshot_dry_run/<dry_run_id>/
reports/etf_portfolio/dynamic_v3_rescue/real_execution_owner_review/<review_id>/
reports/etf_portfolio/dynamic_v3_rescue/real_snapshot_paper_action/<paper_action_id>/
reports/etf_portfolio/dynamic_v3_rescue/weekly_real_snapshot_review/<weekly_real_review_id>/
```

## 安全边界

- `broker_imported=false`
- `broker_action_allowed=false`
- `broker_action_taken=false`
- `order_ticket_generated=false`
- `owner_approval_required=true`
- `production_effect=none`
- 不修改真实持仓、不写 broker state、不生成 order ticket、不自动 owner approval、不自动 production candidate。

## Redaction-safe Snapshot 规则

模板和 intake 必须避免敏感字段：

- 不保存真实账户号、券商账号、订单号、交易凭证、statement path、税务批次、身份证件或资金来源。
- 允许保留手动抽象账户 id，例如 `manual_primary`。
- `total_equity` 必须为正。
- 权重合计必须接近 1.0。
- value 合计必须接近 `total_equity`。
- `snapshot.as_of` 必须存在。
- `owner_reviewed=false` 时，下游 advisory 必须保持 manual review required。

## Weekly Review 运行边界

Weekly real snapshot review 是 weekly/manual cadence，不新增独立 daily scheduler entry。运行前应按 `docs/operations/operations_runbook.md` 确认 cadence、上游 artifact、质量门禁和 production boundary。

## 进展记录

- 2026-07-12：ARCH-004G2.4AG接口迁移与TRADING-207 source-binding hardening完成。Paper action callbacks已有独立canonical owner；pending在任何写入前被拒绝，owner/dry-run/manual snapshot/drift/guardrail validations、id lineage、九类source path/checksum和content-derived recomputation均已实现。Focused 96与architecture-fitness 216通过；paper-only/no-action语义及no real snapshot/existing paper portfolio/official weights/order/production/broker边界保持。
- 2026-07-12：ARCH-004G2.4AG开始迁移TRADING-207接口并修复source-binding缺口。Paper action apply必须拒绝pending/非法owner decision及任一上游validation FAIL，在任何output mutation前验证owner review、dry-run、manual snapshot、drift和guardrail；artifact必须冻结source path/checksum/id lineage，validator从sources重算action type、before/proposed/applied capped deltas、after weights与paper state。该强化不改变paper-only/no-action业务语义，也不扩大到真实snapshot、既有paper portfolio、official weights、production、broker或order。
- 2026-06-12：需求从附件导入并登记为 P0 `IN_PROGRESS`。实现范围限定为 P0 dry-run、owner decision、paper tracking、weekly review 与 no broker/no order-ticket/no production 安全闭环。
- 2026-06-12：baseline 实现完成并转入 `VALIDATING`。真实验收链路使用
  `config/etf_portfolio/dynamic_v3_rescue/current_portfolio_snapshot.example.yaml`
  和 shadow shortlist `4378b3ed3fc1be41`，生成 real snapshot intake
  `329a413d95ba6d66`、dry run `6d5a08c630f05053`、owner review
  `2a16c992cfa06e60`、paper action `e42112a6a34bd7c0`、weekly real review
  `4bf5bd4bd36a0cd7`。Owner decision 验收路径记录为 `monitor`，paper action
  为 `no_action`，`broker_action_taken=false`、`order_ticket_generated=false`。
  对应五个 validate CLI、`aits etf dynamic-v3-rescue validate`、artifact family
  validation、report index、Reader Brief、Reader Brief quality、focused pytest、ruff、
  compileall 和 full pytest 均通过；下一步为 owner 复核 weekly real snapshot review
  及是否继续使用更细 redaction 字段或真实 owner snapshot 样本做运营观察。
