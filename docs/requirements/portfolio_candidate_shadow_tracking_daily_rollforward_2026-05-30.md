# TRADING-056 Shadow Candidate Tracking & Daily Roll-forward

最后更新：2026-05-30

## 背景

TRADING-055 已完成 portfolio candidate manual review workflow，真实 latest review decision
为 `watch`，candidate profile 为 `lower_rebalance_threshold_2pct`。同时 latest dry-run
已解析到 2026-05-29，而 review decision artifact 和 latest valid manifest 仍在
2026-05-28，说明 manual-reviewed candidate 需要正式进入 shadow tracking，并明确处理
review date、tracking date 和 effective data date 不一致的 roll-forward 场景。

## 目标

1. 读取 latest portfolio candidate review decision，并只允许 `watch` 或
   `approved_for_shadow_candidate` 进入 tracking。
2. 生成 daily tracking JSON / Markdown 和跨日 state artifact。
3. 支持 latest tracking date 与 review decision date、candidate artifact date、manifest
   effective date 不一致时的可审计 roll-forward。
4. 在 latest data incomplete / degraded 时输出 `degraded_tracking` 或
   `tracking_blocked` reason，不绕过 data gate。
5. 接入 CLI、Dashboard、Reader Brief、report alias 和 shadow backtest supporting artifact。
6. 保持 `production_effect=none`、`manual_review_required=true`、`auto_promotion=false`，
   不修改 `config/parameters/production/current.yaml`，不自动 promotion。

## 非目标

- 不修改 production 参数。
- 不启用真实交易或 portfolio candidate。
- 不新增信号、调整 signal weights 或解除 promotion 禁止。
- 不用 review decision 覆盖 backtest 结果。
- 不使用 mock 数据或伪造 latest market data。

## 设计要点

- 新增 `ai_trading_system.trading_engine.portfolio_candidate_tracking`，复用
  TRADING-055 review decision/package 与 TRADING-054 candidate artifacts。
- 新增 CLI：
  - `aits portfolio track-candidate --latest`
  - `aits portfolio track-candidate --date YYYY-MM-DD`
  - `aits portfolio track-candidate --review <path>`
  - `aits portfolio track-candidate --latest --dry-run`
  - `aits portfolio validate-tracking --latest`
  - `aits portfolio tracking-status --latest`
  - `aits reports portfolio-candidate-tracking --latest`
- 输出：
  - `artifacts/portfolio_candidate_tracking/YYYY-MM-DD/portfolio_candidate_tracking_summary.json`
  - `artifacts/portfolio_candidate_tracking/YYYY-MM-DD/portfolio_candidate_tracking_summary.md`
  - `artifacts/portfolio_candidate_tracking/YYYY-MM-DD/portfolio_candidate_tracking_state.json`
  - `artifacts/portfolio_candidate_tracking/state/active_shadow_candidates.json`
- Roll-forward 必须记录 `tracking_date`、`effective_data_date`、
  `review_decision_date`、`candidate_artifact_date`、`latest_manifest_date`、
  `roll_forward_status` 和 reason。
- production hash 与 review decision 记录不一致时必须 block tracking。

## 验收标准

- `aits portfolio track-candidate --latest` 生成 tracking summary 和 state artifacts。
- `aits portfolio validate-tracking --latest` 输出
  `candidate_profile=lower_rebalance_threshold_2pct`、
  `tracking_status=active_tracking` 或 `degraded_tracking`、
  `production_effect=none`、`manual_review_required=true`、`auto_promotion=false`。
- `aits reports portfolio-candidate-tracking --latest` 可读取 latest tracking report。
- `aits parameters shadow-backtest --latest --dry-run` 仍为 `promotion_status=rejected`，
  且 supporting artifacts 引用 `portfolio_candidate_tracking_summary.json`。
- Dashboard 和 Reader Brief 只读展示 tracking 摘要，不触发重跑或 production 修改。
- `config/parameters/production/current.yaml` hash 保持不变。
- 目标测试和全量验证通过：
  - `python -m pytest -q`
  - `python -m ruff check scripts src tests`
  - `python -m compileall src scripts`
  - `git diff --check`

## 进展记录

- 2026-05-30: 新增并进入 `IN_PROGRESS`。原因：TRADING-055 已完成 manual watch
  decision，下一阶段需要把 candidate watched 正式推进为 candidate tracked，同时保留
  production 禁止边界和 data quality gate。
- 2026-05-30: 实现完成并进入 `VALIDATING`。已新增
  `config/portfolio/portfolio_candidate_tracking.yaml`、
  `ai_trading_system.trading_engine.portfolio_candidate_tracking`、CLI
  `track-candidate / validate-tracking / tracking-status`、report alias、
  Dashboard/Reader Brief 摘要、shadow backtest supporting artifact、report registry、
  artifact catalog 和 system flow 更新，以及专项测试。
- 2026-05-30: 真实 latest 验收完成。`aits portfolio track-candidate --latest`
  生成 `artifacts/portfolio_candidate_tracking/2026-05-29/portfolio_candidate_tracking_summary.json/md`
  和 `state/active_shadow_candidates.json`，`candidate_profile=lower_rebalance_threshold_2pct`、
  `tracking_status=degraded_tracking`、`data_gate=OK`、`effective_data_date=2026-05-28`。
  `aits parameters shadow-backtest --latest --dry-run` 仍为 `promotion_status=rejected`，
  并引用 `portfolio_candidate_tracking_summary.json`；`config/parameters/production/current.yaml`
  SHA256 前后一致。
- 2026-05-30: 验证通过 `python -m pytest -q`（1496 passed）、
  `python -m ruff check scripts src tests`、`python -m compileall src scripts` 和
  `git diff --check`。
