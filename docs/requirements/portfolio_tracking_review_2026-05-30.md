# TRADING-058 Shadow Candidate Performance Review

最后更新：2026-06-09

状态：DONE

## 背景

TRADING-057A 已把 market freshness 恢复到 `OK`，并使
`lower_rebalance_threshold_2pct` candidate 重新进入 `active_tracking`。当前
系统已有 price repair、registry / manifest 一致性、freshness gate、refresh
recovery、candidate review、candidate tracking，以及 baseline vs candidate shadow
tracking。

TRADING-058 的目标是在不修改 production 参数、不启用 candidate、不自动 promotion
的前提下，对 active shadow candidate 做滚动表现复核，回答“这个 candidate 跟踪表现
如何”。

## 范围

本任务新增 portfolio tracking review 层：

1. 读取 `artifacts/portfolio_candidate_tracking/state/active_shadow_candidates.json`。
2. 汇总 candidate 自 tracking start 以来的 baseline vs candidate 表现。
3. 支持 `latest_day`、`rolling_5d`、`rolling_20d`、`since_tracking_start` 窗口。
4. 输出 performance、relative performance、signal transmission、risk guardrails。
5. 给出 advisory-only recommendation：
   `continue_tracking`、`watch`、`pause_tracking`、`retire_candidate`、
   `needs_more_data`、`eligible_for_extended_review`。
6. 生成 JSON/Markdown artifact、report alias、Dashboard 摘要、Reader Brief 摘要，并
   让 `aits parameters shadow-backtest --latest --dry-run` 只读引用 review artifact。

## 非目标和安全边界

- 不修改 `config/parameters/production/current.yaml`。
- 不写入 production 参数、weights、gates、broker 或真实交易 artifact。
- 不自动 promotion，不解除 `auto_promotion=false`。
- 不因为单日收益好而输出 production approval。
- 不绕过 `aits validate-data` 或 freshness gate。
- 不降低 signal quality `LIMITED` 的 promotion 禁令。

所有输出必须保持：

```text
production_effect=none
manual_review_required=true
auto_promotion=false
```

## 输入

- `artifacts/portfolio_candidate_tracking/state/active_shadow_candidates.json`
- `artifacts/portfolio_candidate_tracking/YYYY-MM-DD/portfolio_candidate_tracking_summary.json`
- `artifacts/portfolio_candidate_reviews/YYYY-MM-DD/portfolio_candidate_review_decision.json`
- `artifacts/portfolio_candidate_reviews/YYYY-MM-DD/portfolio_candidate_review_package.json`
- `artifacts/portfolio_candidates/YYYY-MM-DD/portfolio_candidates_summary.json`
- `artifacts/data_freshness/YYYY-MM-DD/market_data_freshness_summary.json`
- `artifacts/data_refresh/YYYY-MM-DD/market_data_refresh_summary.json`
- `artifacts/backtest_snapshots/YYYY-MM-DD/backtest_input_manifest.json`
- `config/portfolio/portfolio_tracking_review.yaml`
- `config/parameters/production/current.yaml` hash

## 输出

- `artifacts/portfolio_tracking_reviews/YYYY-MM-DD/portfolio_tracking_review_summary.json`
- `artifacts/portfolio_tracking_reviews/YYYY-MM-DD/portfolio_tracking_review_summary.md`
- `outputs/reports/portfolio_tracking_review_YYYY-MM-DD.json`
- `outputs/reports/portfolio_tracking_review_YYYY-MM-DD.md`

## 阶段拆解

### 阶段 1：核心 review artifact

- 新增 config 和 `ai_trading_system.trading_engine.portfolio_tracking_review`。
- 新增 CLI：
  - `aits portfolio review-tracking --latest`
  - `aits portfolio review-tracking --date YYYY-MM-DD`
  - `aits portfolio review-tracking --candidate <profile>`
  - `aits portfolio review-tracking --latest --window 5d|20d|since-start`
  - `aits portfolio review-tracking --latest --dry-run`
  - `aits portfolio validate-tracking-review --latest`
  - `aits reports portfolio-tracking-review --latest`
- JSON schema 和 Markdown 报告覆盖安全字段、recommendation、promotion impact。

### 阶段 2：只读集成

- Dashboard 新增 Portfolio Tracking Review 卡片。
- Reader Brief 展示 tracking review recommendation 和简洁摘要。
- Shadow backtest promotion decision 引用 review artifact，但仍保持 rejected。
- `config/report_registry.yaml`、`docs/artifact_catalog.md`、`docs/system_flow.md` 同步。

### 阶段 3：验证

- 新增专项测试覆盖 blocked、needs_more_data、continue_tracking、
  eligible_for_extended_review、retire_candidate、freshness stale、tracking not active、
  signal transmission、turnover warning、schema stability、Markdown/report alias、
  dashboard、Reader Brief、shadow backtest reference 和 production config unchanged。
- 运行验收命令：
  - `aits portfolio review-tracking --latest`
  - `aits portfolio validate-tracking-review --latest`
  - `aits reports portfolio-tracking-review --latest`
  - `aits parameters shadow-backtest --latest --dry-run`
  - `python -m pytest -q`
  - `python -m ruff check scripts src tests`
  - `python -m compileall src scripts`
  - `git diff --check`

## 状态记录

- 2026-05-30：新增并进入 `IN_PROGRESS`。原因：TRADING-057A 已恢复 freshness 和
  active shadow tracking，下一阶段需要建立 rolling performance review 框架，而不是
  直接从短期 tracking 结果得出 promotion 结论。
- 2026-05-30：从 `IN_PROGRESS` 改为 `VALIDATING`。原因：已完成 review artifact、
  CLI、report alias、Dashboard、Reader Brief、shadow backtest 只读引用、registry、
  artifact catalog 和 system flow 更新；真实 latest 输出 `LIMITED` /
  `needs_more_data`，`candidate_profile=lower_rebalance_threshold_2pct`，
  `tracking_days=1`，安全字段仍为 `production_effect=none`、
  `manual_review_required=true`、`auto_promotion=false`。

## 验证记录

- `aits portfolio review-tracking --latest`：通过，写出 2026-05-29 review artifact，
  recommendation 为 `needs_more_data`。
- `aits portfolio validate-tracking-review --latest`：通过，status 为 `LIMITED`。
- `aits reports portfolio-tracking-review --latest`：通过，写出 report alias。
- `aits parameters shadow-backtest --latest --dry-run`：通过，promotion 仍为
  `rejected`，supporting artifacts 包含 `portfolio_tracking_review`。
- `python -m pytest -q`：1533 passed，276 warnings。
- `python -m ruff check scripts src tests`：通过。
- `python -m compileall -q src scripts`：通过。
- `git diff --check`：通过。
- `config/parameters/production/current.yaml`：未修改。
- 2026-06-09：从 VALIDATING 改为 DONE，原因：latest
  `portfolio_tracking_reviews/2026-06-08` 已达到 `tracking_days=7`、
  `stage=short_window_review`、`recommendation=continue_tracking`、
  `done_condition_met=true`；`portfolio validate-tracking-review --latest` 和
  `reports portfolio-tracking-review --latest` 均为 OK，shadow backtest dry-run
  继续 `promotion_status=rejected`，所有 production/broker/trading action 安全字段
  保持 false/none。验证通过 candidate review / tracking / tracking review
  focused pytest 52 passed、文档检查、Ruff、repo-wide Black check、`compileall`
  和 `git diff --check`。
