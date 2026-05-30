# TRADING-054 Portfolio Construction Candidate Profiles

## 状态

- task id: `TRADING-054`
- priority: `P1`
- status: `VALIDATING`
- owner: system
- started: 2026-05-30

## 背景

`TRADING-053B` 已把 `GOOGL`、`BRK.B via BRK-B`、`SGOV` 从 audited FMP raw cache 注册回主价格缓存，并刷新 latest backtest manifest / price cache registry。真实 latest resolution 已收敛到 required-asset common date `2026-05-28`，`aits data inspect-registry --latest` 通过。

随后 `aits portfolio sensitivity --latest` 显示 `data_gate=OK`、`status=LIMITED`、`primary_bottleneck=rebalance_threshold`。当前主要 blocker 已从数据层转移到 portfolio construction 层：rebalance threshold 可能过宽，导致 signal score 变化无法充分传导到 actual portfolio weights。

## 目标

- 新增 portfolio construction candidate profile 配置。
- 基于 latest valid backtest input manifest、signal snapshot、signal calibration、portfolio sensitivity 和 shadow backtest artifacts 评估候选方案。
- 重点测试 rebalance threshold，并同时测试 score sensitivity、score-to-weight mapping、position caps、sector cap 和 cash floor。
- 对每个 candidate profile 运行 shadow-backtest-like portfolio simulation 和 remove-one-signal ablation summary。
- 输出 performance、risk、turnover/cost、signal transmission 和 signal contribution 指标。
- 基于非收益唯一目标 ranking 选出 recommended candidate profile。
- 生成 JSON / Markdown / recommended candidate YAML artifact。
- 将 latest candidate summary 只读接入 Dashboard、Reader Brief 和 shadow backtest promotion decision supporting artifacts。
- 保持 `production_effect=none`、`manual_review_required=true`、`auto_promotion=false`。

## 非目标

- 不自动修改 production 参数。
- 不修改 `config/parameters/production/current.yaml`。
- 不解除 candidate promotion。
- 不做真实交易。
- 不新增 macro、valuation、event、news 或期权数据源。
- 不引入 ML optimizer。
- 不以单一收益最高作为最终选择标准。
- 不改变 signal snapshot quality 的 `LIMITED` 状态。
- 不绕过 manual review 或 data quality gate。

## 输入依赖

- `artifacts/data_registry/price_cache_registry.json`
- `artifacts/backtest_snapshots/YYYY-MM-DD/backtest_input_manifest.json`
- `artifacts/signal_snapshots/YYYY-MM-DD/signal_snapshot.json`
- `artifacts/signal_calibration/YYYY-MM-DD/signal_calibration_summary.json`
- `artifacts/portfolio_sensitivity/YYYY-MM-DD/portfolio_sensitivity_summary.json`
- `artifacts/shadow_backtest/YYYY-MM-DD/shadow_backtest_summary.json`

Portfolio sensitivity summary 必须可读取，并应显示 `primary_bottleneck=rebalance_threshold`。如果缺失，CLI 应提示先运行 `aits portfolio sensitivity --latest`。

## 实施步骤

1. 新增 `config/portfolio/portfolio_candidate_profiles.yaml`，记录 candidate profiles、guardrails、ranking policy、review condition 和 safety fields。
2. 新增 `ai_trading_system.trading_engine.portfolio_candidates`，复用 portfolio sensitivity 的 data gate、signal snapshot、score-to-target、target-to-actual、constraint 和 performance 计算路径。
3. 新增 CLI：`aits portfolio candidates --latest`、`aits portfolio validate-candidates --latest` 和 `aits reports portfolio-candidates --latest`。
4. 输出 `artifacts/portfolio_candidates/YYYY-MM-DD/portfolio_candidates_summary.json/md` 和 `recommended_portfolio_candidate.yaml`。
5. 将 latest candidate summary 只读接入 `aits parameters shadow-backtest --latest --dry-run` promotion reason/supporting artifacts，但不得改变 promotion status。
6. 将 latest candidate summary 接入 Daily Task Dashboard 和 Reader Brief。
7. 更新 `docs/system_flow.md`、`docs/artifact_catalog.md`、`config/report_registry.yaml` 和测试。

## 验收标准

- `aits portfolio candidates --latest` 生成 `artifacts/portfolio_candidates/YYYY-MM-DD/portfolio_candidates_summary.json`、`.md` 和 `recommended_portfolio_candidate.yaml`。
- `aits reports portfolio-candidates --latest` 可读取 latest candidate summary 并写 `outputs/reports` alias。
- `aits portfolio validate-candidates --latest` 输出 `status=LIMITED`、`production_effect=none`、`manual_review_required=true`、`auto_promotion=false`。
- 每个 candidate profile 输出 `signal_transmission`、`delta_vs_baseline`、`performance`、`risk_guardrails`、`signal_contribution`、`ranking_score` 和 `warnings`。
- `aits parameters shadow-backtest --latest --dry-run` 仍保持 `promotion_status=rejected`、`production_effect=none`、`manual_review_required=true`、`auto_promotion=false`，且 promotion decision supporting artifacts 可引用 `portfolio_candidates_summary.json`。
- Dashboard 展示 Portfolio Candidate Profiles 卡片。
- Reader Brief 展示 3-5 行 portfolio candidate 摘要。
- 不修改 `config/parameters/production/current.yaml`。
- 专项测试、`python -m pytest -q`、`python -m ruff check scripts src tests`、`python -m compileall src scripts` 和 `git diff --check` 通过。

## 开放问题

- 如果 recommended candidate 明显改善 transmission 且 guardrails 通过，下一步进入 `TRADING-055 Portfolio Candidate Manual Review Workflow`，仍不自动 production。
- 如果 candidate 改善有限或 guardrails 不通过，下一步回到 signal quality，优先评估 valuation risk 或 macro liquidity proxy 升级。
- 需要继续观察更多真实窗口；在 signal snapshot quality 仍为 `LIMITED` 时，candidate profile 不得支持 promotion。

## 进展记录

- 2026-05-30: 新增需求文档并进入 `IN_PROGRESS`。范围限定为只读 portfolio construction candidate profile evaluation，不修改 production 参数，不解除 candidate promotion。
- 2026-05-30: 实现 TRADING-054 v0.1 并进入 `VALIDATING`。已完成 `config/portfolio/portfolio_candidate_profiles.yaml`、`ai_trading_system.trading_engine.portfolio_candidates`、`aits portfolio candidates / validate-candidates`、`aits reports portfolio-candidates`、JSON/Markdown/recommended candidate artifact、Dashboard、Reader Brief、shadow promotion supporting artifact、report registry、system flow / artifact catalog 和专项测试。
- 2026-05-30: 真实 latest 验收完成。`aits portfolio candidates --latest` 生成 `artifacts/portfolio_candidates/2026-05-28/portfolio_candidates_summary.json/md` 和 `recommended_portfolio_candidate.yaml`，`status=LIMITED`、`data_gate=OK`、`best_profile=lower_rebalance_threshold_2pct`、`production_effect=none`、`manual_review_required=true`、`auto_promotion=false`。`aits parameters shadow-backtest --latest --dry-run` 已引用 `portfolio_candidates` supporting artifact，promotion 仍 `rejected`。
- 2026-05-30: 验证通过 `python -m pytest -q`、`python -m ruff check scripts src tests`、`python -m compileall src scripts` 和 `git diff --check`；`config/parameters/production/current.yaml` SHA256 前后保持 `CBF180EC0607BBB2B804CE7C388E0BF89789968B2082D1BC11F0953EE7A0D830`。
