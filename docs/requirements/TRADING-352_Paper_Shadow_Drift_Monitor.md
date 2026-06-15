# TRADING-352 Paper Shadow Drift Monitor

最后更新：2026-06-15

## 1. 背景

TRADING-351 已能记录单日 paper-shadow observation，但还没有机制比较当前 observation 是否
偏离此前已验证的 candidate behavior。TRADING-352 建立只读 drift monitor，让 owner 在继续
paper-shadow、转人工复核、退回 research 或拒绝 candidate 前看到可审计的偏离原因。

## 2. 目标

1. 比较 current paper-shadow behavior 与 historical validation behavior。
2. 检测以下 drift families：
   - unexpected turnover increase
   - excessive risk-off frequency
   - drawdown mismatch regression
   - flip/rotation regression
   - benchmark underperformance
   - missing signal inputs
3. 输出 drift severity：`NONE`、`WATCH`、`WARNING`、`BLOCKING`。
4. 输出 next action：`continue_shadow`、`needs_manual_review`、`return_to_research`、
   `reject_candidate`。
5. 新增 report CLI、validate CLI、Reader Brief section、artifact registry/report registry 可见性和
   focused tests。

## 3. 非目标

- 不刷新 market data、signal artifact 或 validation evidence。
- 不重新运行 stress backfill、A/B review、paper-shadow daily runner 或 backtest。
- 不修改 candidate decision ledger、paper account state、official target weights、broker/order
  state、baseline config 或 production state。
- 不自动 promote、reject 或切换 candidate；所有输出只作为 manual review input。

## 4. Artifact Contract

Runtime root:

- `reports/etf_portfolio/dynamic_v3_rescue/paper_shadow_drift_monitor/<monitor_id>/`

Artifacts:

- `paper_shadow_drift_manifest.json`
- `paper_shadow_drift_report.json`
- `paper_shadow_drift_findings.jsonl`
- `paper_shadow_drift_report.md`
- `reader_brief_section.md`
- `paper_shadow_drift_validation.json/md`

Expected summary fields:

- `paper_shadow_drift_monitor_id`
- `paper_shadow_drift_candidate`
- `paper_shadow_drift_observation_id`
- `paper_shadow_drift_severity`
- `paper_shadow_drift_blocking_count`
- `paper_shadow_drift_warning_count`
- `paper_shadow_drift_next_action`
- `paper_shadow_drift_validation_status`

## 5. Drift Policy

第一版使用 named code constants for pilot baseline boundaries，并在 module comments 中声明：
这些阈值只用于 paper-shadow drift reporting，不是 production trading thresholds、position sizing
rules 或 promotion gates。后续如果进入 extended paper-shadow，应迁移到 reviewed policy manifest
并用真实 observation 样本校准。

## 6. Safety Boundary

所有输出固定：

- `manual_review_only=true`
- `paper_shadow_drift_monitor_only=true`
- `observation_only=true`
- `read_only_monitor=true`
- `not_official_target_weights=true`
- `broker_action_allowed=false`
- `broker_action_taken=false`
- `order_ticket_generated=false`
- `production_state_mutated=false`
- `paper_account_state_mutated=false`
- `automatic_candidate_promotion=false`
- `auto_apply=false`
- `production_effect=none`

## 7. 验收标准

- `paper-shadow-drift-monitor report` 能读取 latest 或指定 daily observation 与 historical
  validation artifacts，并生成 drift JSON/Markdown/Reader Brief。
- `validate-paper-shadow-drift-monitor` 返回 PASS，且能阻断缺失文件、不完整 findings、非法
  severity/action 或 unsafe payload。
- Reader Brief 显示 drift monitor id、candidate、observation id、severity、blocking/warning
  count、next action 和 validation status。
- README、operations runbook、system flow、artifact catalog、report registry、requirements 和
  task register 同步更新。
- focused pytest、contract-validation suite、ruff、compileall、git diff check、documentation
  contract、report index 和 Reader Brief quality 通过。

## 8. 进展记录

- 2026-06-15：新增任务并进入 IN_PROGRESS；本阶段只实现 advisory/read-only drift visibility，
  不刷新数据、不运行上游、不修改 paper account/production state、不触发 broker/order。
- 2026-06-15：实现 report/validate CLI、drift artifact schema、Reader Brief summary fields、
  report registry、artifact catalog、README、system flow、operations runbook 和 focused tests；
  转入 VALIDATING，等待真实 runtime artifact 与 validation suite 结果。
- 2026-06-15：DONE；真实 monitor `paper-shadow-drift-monitor_c8546a7d24dc68c2`
  读取 daily observation `paper-shadow-daily_c7945bcfdf91cd53`，输出
  `drift_severity=NONE`、`blocking_count=0`、`warning_count=0`、
  `next_action=continue_shadow`，artifact validate 与 standalone validate CLI 均 PASS。
  focused pytest 7 passed，contract-validation suite 41 passed / 9.42s，documentation
  contract PASS，report index `PASS_WITH_WARNINGS`，Reader Brief OK，Reader Brief quality OK，
  ruff、compileall 和 git diff check 通过。
