# TRADING-354A Evidence Staleness Weekly Review Recovery

最后更新：2026-06-16

## 1. 背景

TRADING-354 已把 paper-shadow weekly review 纳入 evidence staleness monitor 的 required
freshness source。真实 monitor `evidence-staleness-monitor_39cc2b41171c8b6d` 正确
fail-closed 为 `BLOCKING`，因为 latest `paper_shadow_weekly_review` 缺失。

这不是 waiver 场景。最佳解法是用既有 TRADING-353 weekly review CLI，从真实可用的 daily
paper-shadow observation 和 drift monitor artifacts 生成或接入最新有效 weekly review artifact，
再重新运行 evidence staleness monitor。

## 2. 目标

1. 定位 TRADING-353 paper-shadow weekly review CLI 和 artifact schema。
2. 定位最新可用 daily paper-shadow artifacts 和 drift monitor artifacts。
3. 为最新完整可用 review window 生成有效 `paper_shadow_weekly_review` artifact。
4. 如果最新市场周不完整，显式记录选用的最新完整可用 artifact window。
5. 不补造不可用数据；缺失输入必须由 weekly review 如实披露。
6. 确保 staleness monitor resolver 能发现 latest weekly review artifact。
7. 重新运行真实 TRADING-354 evidence staleness monitor。

## 3. 非目标

- 不生成 official target weights。
- 不接入 broker、不生成 order ticket、不自动控制仓位。
- 不修改 production state、candidate decision ledger 或 paper account state。
- 不伪造 daily observation、drift monitor、market data 或 weekly review 输入。
- 不为 staleness monitor 增加 waiver 或放宽 required weekly review 规则。

## 4. Recovery Flow

受控恢复顺序：

1. `paper-shadow-daily run/report` 已生成或可发现 daily observations。
2. `paper-shadow-drift-monitor report` 已生成或可发现 drift monitors。
3. `paper-shadow-weekly-review build/report` 使用最新完整可用窗口聚合 existing artifacts。
4. `validate-paper-shadow-weekly-review` 返回 PASS。
5. `evidence-staleness-monitor run/report` 读取 latest weekly review。
6. `validate-evidence-staleness-monitor` 返回 PASS。
7. 只有当没有 blocking / missing artifacts 时，`safe_to_continue_shadow=true`。

## 5. 验收标准

- weekly review CLI PASS。
- evidence staleness monitor run/report/validate CLI PASS。
- `paper_shadow_weekly_review` 不再出现在 `missing_artifacts`。
- 若存在其他 blocker，报告必须明确列出；不得把 `safe_to_continue_shadow` 误置为 true。
- focused tests 覆盖 latest weekly review discovery、weekly review 存在时 staleness 不再因其 blocking、weekly review 缺失时 fail-closed。
- operations runbook 记录 daily paper-shadow -> drift monitor -> weekly review -> evidence staleness monitor -> continuation decision 的恢复流。
- documentation contract、report index、Reader Brief quality、ruff、compileall、git diff check 通过。

## 6. 进展记录

- 2026-06-16：新增并进入 IN_PROGRESS；当前 blocker 是 latest `paper_shadow_weekly_review`
  缺失。不得通过 waiver 或降低 required rule 解决，必须生成或接入真实 weekly review artifact。
- 2026-06-16：完成真实 recovery run；当前只有 daily observation
  `paper-shadow-daily_c7945bcfdf91cd53` 和 drift monitor
  `paper-shadow-drift-monitor_c8546a7d24dc68c2` 可用，二者 observation date 均为
  `2026-06-12`，因此选择最新完整可用 artifact window `2026-06-12..2026-06-12`，
  不是完整市场周。生成 weekly review
  `paper-shadow-weekly-review_f21aec8c5f94ea48`，`weekly_decision=CONTINUE`，
  `missing_input_artifacts=none`，weekly report/latest/validate 均 PASS。最终重跑 evidence
  staleness monitor 生成 `evidence-staleness-monitor_9cb9357aade5728d`，
  `missing_artifacts=[]`、`blocking_artifacts=[]`，`paper_shadow_weekly_review` finding
  为 FRESH / missing=false；`safe_to_continue_shadow=false`，原因是 `price_data` 和
  `market_panel_data` 仍为 STALE，下一步应刷新或再生成 stale evidence 后重新评估。
- 2026-06-16：验收完成并归档为 DONE。`validate-paper-shadow-weekly-review --latest`
  和 `validate-evidence-staleness-monitor --monitor-id
  evidence-staleness-monitor_9cb9357aade5728d` 均 PASS；`reports index --latest`
  为 `PASS_WITH_EXPLICIT_WAIVERS` 且 unwaived=0；`docs report-contract` PASS；
  `reports validate-reader-brief --latest` 为 OK；focused pytest 24 passed；Ruff、
  compileall 和 git diff check 均通过。`validate-data --as-of 2026-06-12`
  返回 `PASS_WITH_WARNINGS`，errors=0、warnings=1，该 warning 不改变本任务的
  weekly review blocker 已解除结论。
