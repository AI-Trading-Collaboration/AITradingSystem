# TRADING-053 Portfolio Sensitivity Diagnostics

最后更新：2026-06-09

## 状态

- task id: `TRADING-053`
- priority: `P1`
- status: `DONE`
- owner: system
- started: 2026-05-30

## 背景

`TRADING-051A` 已确认 `trend_momentum` 和 `sector_strength` 会进入 score calculation，ablation 会改变 score 和 portfolio。`TRADING-052` 进一步确认 `trend_long_bias` 是当前最佳 trend/sector profile，但整体仍为 `LIMITED`，candidate promotion 仍被禁止。

当前需要判断：score 改善未能形成可晋升证据，是否是因为 portfolio construction 对 score 变化过于迟钝，导致 score -> target allocation -> actual weight -> performance 的传导链路被 rebalance threshold、position limits、sector cap 或 cash floor 压制。

## 目标

- 诊断 score dispersion 是否足够产生 allocation 差异。
- 诊断 score -> target weight 的传导强度。
- 诊断 target weight -> actual weight 的传导强度和 rebalance suppression。
- 诊断 max single asset weight、sector cap 和 cash floor 是否频繁 binding。
- 诊断 sensitivity profile 的 turnover/cost 影响和 performance 影响。
- 支持多组 sensitivity profile 对比和 ranking。
- 生成 JSON / Markdown 报告和 recommended profile artifact。
- 将 latest sensitivity summary 只读接入 Dashboard、Reader Brief 和 shadow promotion reason。
- 保持 `production_effect=none`、`manual_review_required=true`、`auto_promotion=false`。

## 非目标

- 不优化或修改 production 参数。
- 不修改 `config/parameters/production/current.yaml`。
- 不解除 candidate promotion。
- 不新增 macro、valuation、event 或 news 数据源。
- 不接入真实交易或自动交易。
- 不引入 ML optimizer。
- 不把单一收益最高 profile 作为最终判断。

## 实施步骤

1. 新增 `config/portfolio/portfolio_sensitivity_profiles.yaml`，记录 sensitivity profiles、诊断阈值、ranking policy、review condition 和 safety fields。
2. 新增 `ai_trading_system.trading_engine.portfolio_sensitivity`，读取 shadow backtest config、production baseline、latest signal snapshot 和通过同一路径的 data quality gate，构建 sensitivity diagnostics。
3. 新增 CLI：`aits portfolio sensitivity --latest`、`aits portfolio validate-sensitivity --latest` 和 `aits reports portfolio-sensitivity --latest`。
4. 输出 `artifacts/portfolio_sensitivity/YYYY-MM-DD/portfolio_sensitivity_summary.json/md` 和 `recommended_portfolio_sensitivity_profile.yaml`。
5. 将 latest sensitivity summary 只读接入 `aits parameters shadow-backtest --latest --dry-run` promotion reason/supporting artifacts，但不得改变 promotion status。
6. 将 latest sensitivity summary 接入 Daily Task Dashboard 和 Reader Brief。
7. 更新 `docs/system_flow.md`、`docs/artifact_catalog.md`、`config/report_registry.yaml` 和测试。

## 验收标准

- `aits portfolio sensitivity --latest` 生成 `artifacts/portfolio_sensitivity/YYYY-MM-DD/portfolio_sensitivity_summary.json` 和 `.md`。
- `aits reports portfolio-sensitivity --latest` 可读取 latest sensitivity summary 并写 `outputs/reports` alias。
- `aits portfolio validate-sensitivity --latest` 输出 `status=LIMITED`、`production_effect=none`、`manual_review_required=true`、`auto_promotion=false`。
- `aits parameters shadow-backtest --latest --dry-run` 仍保持 `promotion_status=rejected`、`production_effect=none`、`manual_review_required=true`、`auto_promotion=false`，且 promotion reason/supporting artifacts 可引用 portfolio sensitivity summary。
- Dashboard 展示 Portfolio Sensitivity Summary 卡片。
- Reader Brief 展示 3-5 行 portfolio sensitivity 摘要。
- 不修改 `config/parameters/production/current.yaml`。
- 专项测试、`python -m pytest -q`、`python -m ruff check scripts src tests`、`python -m compileall src scripts` 和 `git diff --check` 通过。

## 当前验证状态

- 2026-06-09 真实 latest 数据门禁已通过：`python -m ai_trading_system.cli validate-data` 返回 `PASS_WITH_WARNINGS`，错误数为 0。
- 真实 latest CLI 链路已写入并读取 artifact：`aits portfolio sensitivity --latest` 生成 `artifacts/portfolio_sensitivity/2026-06-05/portfolio_sensitivity_summary.json`、`.md` 和 `recommended_portfolio_sensitivity_profile.yaml`。
- 2026-06-05 portfolio sensitivity summary 为 `status=LIMITED`、`data_gate=OK`、`latest_resolution=OK`、`data_registry_consistency=OK`、`best_profile=lower_rebalance_threshold`、`primary_bottleneck=rebalance_threshold`、`portfolio_is_too_insensitive=True`。
- `promotion_impact.can_support_candidate_promotion=false`，原因是 signal snapshot quality 仍为 `LIMITED`；sensitivity diagnostics 仅作为 advisory evidence，不能解除 candidate promotion gate。
- `aits portfolio validate-sensitivity --latest` 与 `aits reports portfolio-sensitivity --latest` 均可读取同一 latest summary，并保留 `production_effect=none`、`manual_review_required=true`、`auto_promotion=false`。
- `aits parameters shadow-backtest --latest --dry-run` 继续输出 `promotion_status=rejected`，promotion reason/supporting artifacts 引用 2026-06-05 portfolio sensitivity summary，且 production/broker/trading action 均为 false。
- `aits reports daily-tasks --as-of 2026-06-05` 的 Dashboard JSON 展示 Portfolio Sensitivity Summary，Reader Brief 的 `parameter_shadow_review` 展示 3-5 行 portfolio sensitivity 摘要。
- 本任务验收范围已闭合；后续 portfolio construction profile 评估由 `TRADING-054` 承接，更多真实窗口观察不阻塞本任务归档。

## 后续跟踪

- `rebalance_threshold` 已被确认为当前 portfolio sensitivity 的 primary bottleneck；调整 production portfolio construction 前仍需人工复核和候选 profile 验证。
- 在 signal quality 仍为 `LIMITED` 时，不得把 sensitivity 或 candidate profile 结果转成 production candidate promotion。

## 进展记录

- 2026-05-30: 新增需求文档并进入 `IN_PROGRESS`。本任务是 `TRADING-052` 后的分叉诊断，不修改 production 参数，不解除 candidate promotion。
- 2026-05-30: 实现 TRADING-053 v0.1 并进入 `VALIDATING`。已完成配置、CLI、JSON/Markdown/recommended profile artifact、Dashboard、Reader Brief、shadow promotion supporting artifact、report registry、system flow / artifact catalog 和测试。真实 latest 运行被 data quality gate 阻断，已记录为数据缓存 blocker，未实施临时 workaround。
- 2026-05-30: TRADING-053B 后复验通过 data gate，`aits portfolio sensitivity --latest` 生成 `2026-05-28` summary，status 为 `LIMITED`，primary bottleneck 为 `rebalance_threshold`；candidate promotion 仍 disabled。
- 2026-06-09: 从 `VALIDATING` 改为 `DONE`。真实 latest 复验确认 `validate-data` 为 `PASS_WITH_WARNINGS` / 错误数 0，`aits portfolio sensitivity --latest` 生成 2026-06-05 summary 和 recommended profile，`data_gate=OK`、`latest_resolution=OK`、`best_profile=lower_rebalance_threshold`、`primary_bottleneck=rebalance_threshold`、`portfolio_is_too_insensitive=True`；`validate-sensitivity`、`reports portfolio-sensitivity`、Dashboard、Reader Brief 和 shadow-backtest dry-run 只读引用均闭合，production safety fields 保持不变。
