# TRADING-055 Portfolio Candidate Manual Review Workflow

最后更新：2026-05-30

## 背景

TRADING-054 已完成 portfolio construction candidate profile evaluation，并在
`artifacts/portfolio_candidates/2026-05-28/` 生成 advisory recommended candidate。
真实 latest 推荐为 `lower_rebalance_threshold_2pct`，但 `signal_snapshot` 质量仍为
`LIMITED`，`production_effect=none`、`manual_review_required=true`、
`auto_promotion=false`，且 `config/parameters/production/current.yaml` 未被修改。

当前缺口是 recommended candidate 只能作为 artifact 存在，尚不能进入正式人工
review / watch / shadow tracking 审计流程。

## 目标

1. 新增 `config/portfolio/portfolio_candidate_review.yaml`，明确人工 review 状态、
   safety 字段、decision rules 和 hard rejection policy。
2. 新增 `ai_trading_system.trading_engine.portfolio_candidate_review`，读取 latest
   `recommended_portfolio_candidate.yaml` 和 supporting artifacts，生成 review package
   与 decision artifact。
3. 新增 CLI：
   - `aits portfolio review-candidate --latest`
   - `aits portfolio decide-candidate --latest --decision ...`
   - `aits portfolio validate-review --latest`
   - `aits reports portfolio-candidate-review --latest`
4. 新增 artifacts：
   - `artifacts/portfolio_candidate_reviews/YYYY-MM-DD/portfolio_candidate_review_package.json`
   - `artifacts/portfolio_candidate_reviews/YYYY-MM-DD/portfolio_candidate_review_package.md`
   - `artifacts/portfolio_candidate_reviews/YYYY-MM-DD/portfolio_candidate_review_decision.json`
   - `artifacts/portfolio_candidate_reviews/YYYY-MM-DD/portfolio_candidate_review_decision.md`
5. Dashboard、Reader Brief 和 `aits parameters shadow-backtest --latest --dry-run` 只读引用
   latest review decision。

## 安全边界

- `approved_for_shadow_candidate` 只表示允许继续 shadow tracking，不是 production approval。
- 不写 `config/parameters/production/current.yaml`。
- 不自动 promotion。
- 不解除 `signal_snapshot` 的 `LIMITED` 限制。
- 不降低 data quality gate。
- 所有 review / decision / report artifacts 必须保持：
  - `production_effect=none`
  - `manual_review_required=true`
  - `auto_promotion=false`

## 状态定义

- `pending_review`: 已生成 review package，尚未人工确认。
- `approved_for_shadow_candidate`: 人工批准进入 shadow candidate tracking，不代表 production。
- `rejected`: 人工拒绝候选。
- `watch`: 继续观察，不升级。
- `needs_more_data`: 需要更多数据或报告支持。

## Decision Rules

若 `signal_snapshot` 仍为 `LIMITED`，production promotion 必须为 false；但在 hard rejection
不存在时，人工可选择 `approved_for_shadow_candidate` 作为 shadow tracking 状态。

以下情况必须拒绝 `approved_for_shadow_candidate`：

- `data_gate_not_ok`
- `production_config_modified`
- `missing_candidate_artifact`
- `missing_portfolio_candidates_summary`
- `auto_promotion_true`
- `production_effect_not_none`

默认建议为 `watch`：data gate 为 OK，candidate summary 为 LIMITED，存在 best profile，
且 signal quality 为 LIMITED。

## 验收标准

- `aits portfolio review-candidate --latest` 生成 review package JSON/Markdown，并在首次生成时
  写出 `pending_review` decision。
- `aits portfolio decide-candidate --latest --decision watch --reason "..."`
  写出 decision JSON/Markdown。
- `aits portfolio validate-review --latest` 输出 `status=watch`、
  `production_effect=none`、`manual_review_required=true`、`auto_promotion=false`、
  `production_config_modified=false`。
- `aits reports portfolio-candidate-review --latest` 可读取 latest review decision 并写
  `outputs/reports` alias。
- `aits parameters shadow-backtest --latest --dry-run` 仍保持
  `promotion_status=rejected`，但 promotion decision supporting artifacts 引用
  `portfolio_candidate_review_decision.json`。
- Dashboard 展示 Portfolio Candidate Review 卡片。
- Reader Brief 展示 3-5 行 review 摘要。
- `config/parameters/production/current.yaml` SHA256 前后一致。
- 专项测试、全量 pytest、ruff、compileall 和 `git diff --check` 通过。

## 进展记录

- 2026-05-30: 新增需求文档并进入 `IN_PROGRESS`。实现范围限定为
  recommended candidate -> manual review package -> explicit review decision artifact；
  不做 production 参数写入、不做自动 promotion。
- 2026-05-30: 实现 TRADING-055 v0.1 并进入 `VALIDATING`。已完成
  `config/portfolio/portfolio_candidate_review.yaml`、
  `ai_trading_system.trading_engine.portfolio_candidate_review`、
  `aits portfolio review-candidate / decide-candidate / validate-review`、
  `aits reports portfolio-candidate-review`、JSON/Markdown review package、decision
  artifact、Dashboard、Reader Brief、shadow promotion supporting artifact、report registry、
  system flow / artifact catalog 和专项测试。
- 2026-05-30: 真实 latest 验收完成。`aits portfolio review-candidate --latest`
  生成 `artifacts/portfolio_candidate_reviews/2026-05-28/portfolio_candidate_review_package.json/md`
  和 pending decision；`aits portfolio decide-candidate --latest --decision watch`
  将 decision 设为 `watch`，`candidate_profile=lower_rebalance_threshold_2pct`、
  `allowed_next_step=continue_shadow_tracking`、`production_effect=none`、
  `manual_review_required=true`、`auto_promotion=false`、
  `production_config_modified=false`。`aits parameters shadow-backtest --latest --dry-run`
  仍为 `promotion_status=rejected`，并引用
  `portfolio_candidate_review_decision.json`；`config/parameters/production/current.yaml`
  SHA256 前后一致。
