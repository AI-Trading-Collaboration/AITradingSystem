# SEC PIT Real Run Diagnostics Runbook

本 runbook 对应 `TRADING-042`，用于对真实 SEC PIT evaluation / baseline comparison
结果做只读诊断。它不重跑 evaluation，不重跑 comparison，不下载市场数据，不修改 production
或 shadow 权重。

## 运行命令

```bash
aits sec-pit diagnose-run \
  --start 2023-01-01 \
  --end 2026-05-26 \
  --tickers NVDA,MSFT,AMD,AVGO,GOOGL,META,AMZN \
  --feature-panel data/processed/sec_edgar/sec_pit_feature_panel.csv \
  --evaluation-dir outputs/sec_pit_evaluation \
  --comparison-dir outputs/sec_pit_baseline_comparison \
  --baseline-score-path data/processed/scores_daily.csv \
  --output-dir outputs/sec_pit_diagnostics
```

也可自动发现 latest artifacts：

```bash
aits sec-pit diagnose-run --latest
```

## 必须检查

- `sec_pit_real_run_diagnostics_YYYY-MM-DD.json`
- `sec_pit_real_run_diagnostics_YYYY-MM-DD.md`
- `sec_pit_provenance_gap_YYYY-MM-DD.csv`
- `sec_pit_coverage_audit_YYYY-MM-DD.csv`
- `sec_pit_alias_resolution_audit_YYYY-MM-DD.csv`
- `sec_pit_label_coverage_audit_YYYY-MM-DD.csv`
- `sec_pit_candidate_sensitivity_YYYY-MM-DD.csv`

## 解释顺序

1. 先看 summary 的 `diagnostics_status`、`provenance.first_loss_stage` 和 limitations。
2. 用 provenance gap CSV 确认 SEC provenance 第一次在哪个 stage 明显下降。
3. 用 alias audit 确认输入 ticker 与 canonical ticker 是否一致，重点复核 `GOOGL -> GOOG`。
4. 用 baseline summary 字段确认实际 baseline artifact、fallback status、row count 和 date range。
5. 用 label audit 确认 `forward_return_20d`、`relative_return_vs_QQQ_20d` 和
   `max_drawdown_forward_20d` 是否可用于 evaluation label。
6. 用 coverage audit 确认修正后 `coverage_ratio_after <= 1`，并复核 duplicate observations。
7. 用 candidate sensitivity 只识别 near-promotion feature；不得自动 promotion。

尾部 decision date 若没有未来 20 个交易日价格，`forward_return_20d` 和
`relative_return_vs_QQQ_20d` 会按 label coverage audit 标记为未来价格窗口限制；这不是
feature 或 provenance 缺失。`max_drawdown_forward_20d` 应为非正数，单调上涨窗口为 `0.0`。

## 安全边界

- 所有输出固定 `manual_review_required=true`、`production_effect=none`。
- Candidate sensitivity 的 `hypothetical_recommendation_if_provenance_fixed` 不是 shadow
  promotion 决策。
- 当前真实 run 若 provenance、coverage 或 label 有 remediation blocker，不进入 TRADING-043；
  若仅剩 baseline fallback 或尾部未来 20D label 窗口限制，进入 review 前必须显式披露。
- remediation 后若 diagnostics status 为 `OK`，且 candidate sensitivity / evaluation
  显示某个 feature 已进入 manual-review shadow candidate 状态，才运行
  `docs/runbooks/sec_pit_candidate_review.md` 生成 evidence pack；该 review 仍不是自动
  promotion。
- Candidate review 后若 owner 明确批准 `APPROVE_OBSERVE_ONLY_SHADOW`，再运行
  `docs/runbooks/sec_pit_shadow_observe.md`。Observe-only lane 仍必须保留
  `production_effect=none`，不得写 production 或 active shadow 权重。

## Dashboard

Daily task dashboard 只读读取 latest
`outputs/sec_pit_diagnostics/sec_pit_real_run_diagnostics_YYYY-MM-DD.json`，展示 diagnostics
status、missing provenance rows、first loss stage、alias unresolved count、baseline artifact
status、drawdown label coverage、coverage ratio > 1 feature count、near-promotion count 和
promotion readiness。Dashboard 不运行 diagnostics。
