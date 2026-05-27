# SEC PIT Candidate Review Runbook

本 runbook 对应 `TRADING-043`，用于在 TRADING-040/041/042 remediation 后，为
`capex_intensity` 等 SEC PIT shadow candidate 生成人工复核 evidence pack。它不运行
evaluation、baseline comparison 或 diagnostics，也不修改 production weights 或 active
shadow weights。

## 运行命令

```bash
aits sec-pit review-candidates \
  --start 2023-01-01 \
  --end 2026-05-26 \
  --evaluation-dir outputs/sec_pit_evaluation \
  --comparison-dir outputs/sec_pit_baseline_comparison \
  --diagnostics-dir outputs/sec_pit_diagnostics \
  --candidate-feature capex_intensity \
  --output-dir outputs/sec_pit_candidate_review
```

也可自动发现 latest artifacts：

```bash
aits sec-pit review-candidates --latest
```

## 必须检查

- `sec_pit_candidate_review_summary_YYYY-MM-DD.json`
- `sec_pit_candidate_review_summary_YYYY-MM-DD.md`
- `sec_pit_candidate_evidence_YYYY-MM-DD.csv`
- `sec_pit_candidate_by_ticker_YYYY-MM-DD.csv`
- `sec_pit_candidate_by_period_YYYY-MM-DD.csv`
- `sec_pit_candidate_overlap_with_baseline_YYYY-MM-DD.csv`
- `sec_pit_candidate_shadow_proposal_YYYY-MM-DD.csv`

## 解释顺序

1. 先看 summary 的 `review_status`、`diagnostics_status`、`provenance_complete` 和
   `drawdown_label_coverage`。
2. 用 candidate evidence CSV 复核 RankIC、IC、hit rate、coverage、data quality、
   stability、drawdown impact 和 incremental alpha。
3. 用 by-ticker CSV 判断候选是否只依赖一个 ticker；出现
   `candidate_concentration_risk` 时不能把证据解释为 broad signal。
4. 用 by-period CSV 判断候选是否只在单一月份或市场状态下有效。
5. 用 baseline overlap CSV 判断候选是否提供新信息，还是重复 baseline score 行为；
   缺 baseline 字段时会在 `overlap_interpretation` 中标记
   `LIMITED_BASELINE_FIELDS_MISSING`。
6. 最后看 shadow proposal CSV。`READY_FOR_MANUAL_REVIEW` 只表示可以进入人工讨论，
   不是自动 observe-only shadow iteration，更不是 production promotion。

## 状态解释

- `OK`：核心 evaluation / comparison / diagnostics artifact 可读取，至少一个候选有可复核
  evidence。
- `LIMITED_MISSING_ARTIFACTS`：缺少或无法读取关键 TRADING-040/041/042 artifact，仍输出
  schema 完整的降级报告。
- `INSUFFICIENT_EVIDENCE`：artifact 可读取，但候选没有足够证据进入人工复核。
- `FAILED_VALIDATION`：输入 schema 或解析失败。

## 安全边界

- 所有 evidence 行固定 `manual_review_required=true`、`production_effect=none`。
- 所有 proposal 行固定 `review_required=true`、`production_effect=none`。
- `suggested_observe_only_weight` 只来自 TRADING-040 shadow candidate artifact，并且不得超过
  `max_allowed_initial_weight`。
- 本命令不写 `config/weights/*`、production score config、approved overlay、active shadow
  weight config 或 score-daily 输出。
- Dashboard 只读读取 latest summary JSON；不会运行 review pipeline。

## Dashboard

Daily task dashboard 只读读取 latest
`outputs/sec_pit_candidate_review/sec_pit_candidate_review_summary_YYYY-MM-DD.json`，展示 latest
review date、review status、candidate count、ready for manual review count、primary candidate、
diagnostics status、drawdown label coverage、top candidate feature、proposal status 和
`production_effect`。Dashboard 不运行 `aits sec-pit review-candidates`，不运行
evaluation/comparison/diagnostics，不修改 production 或 shadow 权重。
