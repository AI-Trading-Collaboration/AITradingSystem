# TRADING-353A Weekly Review Coverage Sufficiency

最后更新：2026-06-16

## 1. 背景

TRADING-353 已建立 paper-shadow weekly review。TRADING-354A 为解除 missing weekly review
blocker，使用最新完整可用 artifact window `2026-06-12..2026-06-12` 生成 weekly review。
该 artifact 对 recovery 有效，但不是完整 market-week review；如果 downstream 只看到
`weekly_decision=CONTINUE`，可能把一日 recovery artifact 误读为完整周度 continuation
证据。

## 2. 目标

1. 为 paper-shadow weekly review 增加 coverage classification。
2. 输出 `selected_window_start`、`selected_window_end`、`expected_market_days`、
   `covered_market_days`、`missing_market_days`、`coverage_ratio`、
   `coverage_classification` 和 `coverage_safe_for_continuation`。
3. 明确区分 `FULL_WEEK_REVIEW`、`PARTIAL_ARTIFACT_WINDOW_REVIEW`、
   `RECOVERY_MODE_REVIEW` 和 `INSUFFICIENT_REVIEW`。
4. Evidence staleness monitor 消费 weekly coverage status；freshness 可通过，但 coverage
   不足时 `safe_to_continue_shadow=false`。
5. 支持显式 manual coverage override，并要求记录 override reason。

## 3. 非目标

- 不补造 missing daily observation 或 drift monitor。
- 不重跑 paper-shadow daily / drift 上游。
- 不把 recovery artifact 自动提升为 full-week continuation evidence。
- 不修改 candidate decision ledger、official target weights、paper account、broker/order 或
  production state。

## 4. Coverage Policy

Policy id: `TRADING-353A_WEEKLY_REVIEW_COVERAGE_SUFFICIENCY`。

- `FULL_WEEK_REVIEW`：selected window 覆盖 week_end 所在 U.S. equity market week 的全部
  expected market days，且 daily observations 覆盖每个 expected market day。
- `RECOVERY_MODE_REVIEW`：selected artifact window 短于完整 market week，但 selected window
  中每个 market day 都有 daily observation。
- `PARTIAL_ARTIFACT_WINDOW_REVIEW`：存在 daily observation，但 selected window 或 covered
  days 不满足完整 market-week coverage。
- `INSUFFICIENT_REVIEW`：没有可用 selected market-day daily observation。

默认只有 `FULL_WEEK_REVIEW` 可直接支持 weekly continuation。若 owner 要在 partial/recovery
coverage 下继续，必须显式使用 manual coverage override，并写入
`manual_coverage_override_reason`。

## 5. 验收标准

- One-day recovery artifact validation PASS，但分类为 `RECOVERY_MODE_REVIEW`，且
  `coverage_safe_for_continuation=false`。
- Full week artifact 覆盖全部 expected market days 时分类为 `FULL_WEEK_REVIEW`，且
  `coverage_safe_for_continuation=true`。
- Evidence staleness monitor finding 暴露 weekly coverage fields。
- Evidence staleness monitor 在 coverage unsafe 时保持 freshness status，但
  `safe_to_continue_shadow=false`，`next_refresh_action` 指向 full weekly review 或 explicit
  manual override。
- README、operations runbook、system flow、artifact catalog、report registry、requirements 和
  task register 同步。
- weekly review CLI、staleness monitor integration、focused pytest、ruff、compileall 和
  git diff check 通过。

## 6. 进展记录

- 2026-06-16：新增并进入 IN_PROGRESS；root cause 是一日 recovery weekly artifact 缺少
  coverage sufficiency 标签，容易被 downstream 误解为 full weekly review。
- 2026-06-16：实现完成并转 DONE；真实 recovery weekly artifact
  `paper-shadow-weekly-review_67b1b8ae09e18fab` 输出
  `coverage_classification=RECOVERY_MODE_REVIEW`、`coverage_ratio=0.2`、
  `coverage_safe_for_continuation=false`、`coverage_status=MANUAL_REVIEW_REQUIRED`，
  validation PASS。Evidence staleness monitor
  `evidence-staleness-monitor_3762a6e11c13bbe2` 输出
  `evidence_freshness_status=ACCEPTABLE`、stale/blocking/missing artifacts 为空，但
  `coverage_status=MANUAL_REVIEW_REQUIRED`、`safe_to_continue_shadow=false`、
  `next_refresh_action=complete_full_weekly_review_or_record_manual_coverage_override`。
  Focused weekly/staleness/Reader Brief pytest 26 passed，Ruff PASS，documentation contract
  PASS，report index `PASS_WITH_EXPLICIT_WAIVERS` / unwaived=0，Reader Brief quality OK，
  compileall PASS，git diff check PASS。
