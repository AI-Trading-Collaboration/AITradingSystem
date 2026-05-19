# Shadow Promotion Proposal Runbook

## 1. 手动生成 promotion proposal

```powershell
python scripts/run_shadow_promotion_proposal.py --date 2026-05-19 --lookback-days 7
```

默认读取：

- `data/derived/weight_iterations/shadow/current_shadow_weights.json`
- `config/weights/weight_profile_current.yaml`
- latest `data/derived/weight_iterations/comparison/reviews/shadow_vs_production_review_*.json`
- 最近 N 日 `data/derived/weight_iterations/comparison/daily_shadow_vs_production_*.json`
- 最近 TRADING-018B shadow candidate/history
- `config/shadow_promotion_proposal_policy.yaml`

输出：

- `data/derived/weight_iterations/promotion/proposals/shadow_promotion_proposal_YYYY-MM-DD.json`
- `data/derived/weight_iterations/promotion/proposals/shadow_promotion_proposal_YYYY-MM-DD.md`
- `data/derived/weight_iterations/promotion/logs/shadow_promotion_proposal_run_YYYY-MM-DD.json`
- `data/derived/weight_iterations/promotion/logs/shadow_promotion_proposal_run_YYYY-MM-DD.md`

## 2. 判断 proposal 是否可信

可信的 proposal 至少应满足：

- `proposal_decision=PROPOSE_FOR_MANUAL_REVIEW`
- `promotion_proposed=true`
- `promotion_executed=false`
- `production_effect=none`
- `manual_review_only=true`
- `safe_for_production=false`
- `readiness_checks` 全部关键项为 `PASS`
- production/shadow weight keys 完全一致
- `shadow_weight_sum_status=PASS`
- `impact_summary.available_comparison_days` 达到 policy 最小天数
- `impact_summary.risk_flag_delta_total <= 0`

即使满足这些条件，也只能进入人工审核，不代表可以上线或交易。

## 3. 检查 safety boundary

打开 proposal JSON，确认：

```json
{
  "production_effect": "none",
  "manual_review_only": true,
  "promotion_executed": false,
  "safe_for_production": false
}
```

同时检查 `pipeline_contract`：

- `runs_shadow_iteration_pipeline=false`
- `runs_comparison_pipeline=false`
- `runs_multi_day_review_pipeline=false`
- `runs_promotion_apply=false`
- `runs_scoring_pipeline=false`
- `runs_broker_runner=false`
- `runs_paper_runner=false`
- `runs_replay_runner=false`
- `writes_production_profile=false`
- `writes_production_weights=false`
- `writes_approved_profile=false`
- `promotes_shadow_to_production=false`
- `triggers_trade=false`

## 4. 确认 production 未被修改

运行前后对比：

```powershell
git diff -- config/weights/weight_profile_current.yaml
git diff -- config/weights
```

预期：没有因为 TRADING-018D 产生的 diff。

也可以检查 proposal JSON：

- `proposed_production_weights` 只是人工审核建议。
- `production_weights` 只是 snapshot。
- 没有任何字段表示 apply 已执行。

## 5. 处理 INSUFFICIENT_DATA

常见原因：

- 缺少 `current_shadow_weights.json`
- 缺少 latest TRADING-018C2 review
- production profile 无法读取或没有 `base_weights`
- production/shadow weight keys 不一致
- shadow weights sum 不接近 1.0
- policy file 缺失

处理方式：

1. 先补齐上游 artifact 或修复 schema。
2. 不要手工 alias weight key。
3. 不要为了通过 gate 改写 production profile。
4. 重新运行 proposal script。

## 6. 处理 INSUFFICIENT_HISTORY

说明多日 comparison 样本还不够。继续运行：

```powershell
python scripts/run_daily_shadow_vs_production_comparison.py --date YYYY-MM-DD
python scripts/run_shadow_vs_production_multi_day_review.py --date YYYY-MM-DD --lookback-days 7
python scripts/run_shadow_promotion_proposal.py --date YYYY-MM-DD --lookback-days 7
```

TRADING-018D 不应补造历史 comparison，也不应把少样本判断升级为 proposal。

## 7. 处理 SAFETY_BLOCKED

常见原因：

- 018C2 review 本身 safety blocked。
- `safety_blocked_days` 超过 policy 阈值。
- 上游 review 的 `production_effect`、`manual_review_only` 或 pipeline contract 异常。
- `current_shadow_weights.json` 的安全字段异常。

处理方式：

1. 先修复上游 safety 问题。
2. 重新生成 018C2 review。
3. 再重新生成 proposal。

不要绕过 safety blocker 生成 proposal。

## 8. 处理 REJECT_SHADOW

说明 shadow 明显更差或风险上升。常见触发：

- `average_score_delta < 0`
- `shadow_risk_flag_delta_total > 0`
- 018C2 `review_decision=SHADOW_LOOKS_WORSE`

处理方式：

- 保留 proposal/report 作为审计证据。
- 继续观察或调整 shadow iteration 输入。
- 不进入 apply 讨论。

## 9. 为什么本任务不执行 promotion

TRADING-018D 的作用是把 shadow evidence 汇总为人工审核材料。它故意不写：

- production profile
- production weights
- approved profile
- broker order
- replay output

这样可以把“证据是否足够进入人工审核”和“是否真正应用到 production”分成两次明确决策。

## 10. 后续 explicit apply task 设计要求

未来 TRADING-018E 如果实现 apply，必须至少要求：

- 人工 approval artifact
- 明确目标 production profile
- apply 前 diff
- apply 后 snapshot
- rollback snapshot
- rollback plan
- 禁止 scheduler 自动运行
- apply 后重新生成 audit report

TRADING-018D 不能实现上述 apply 逻辑。
