# TRADING-091 Dynamic v0.3 Rescue Real Evaluation and Promotion Gate

最后更新：2026-06-06

## 背景

TRADING-090 已建立 candidate-only dynamic v0.3 constraint-aware rescue workflow，但该阶段主要证明 v0.3 模板、报告和安全门禁可用。TRADING-091 的目标是用真实历史价格驱动评估验证 v0.3 rescue 是否具备进入生产候选复核的资格。

本任务仍不代表生产批准、shadow enrollment 或 baseline replacement。`promote_candidate` 只是 promotion gate 的人工复核建议，表示可进入后续 owner review / production-candidate package。

## 必须回答的问题

- v0.3 是否确实减少 constraint hit，而不是单纯降低 turnover。
- v0.3 是否改善 dynamic vs static 的长期差距。
- false risk-off 是否可控。
- drawdown protection 是否没有被明显牺牲。
- 是否存在过拟合某一段市场区间的问题。

## 安全边界

所有输出必须固定：

- `observe_only=true`
- `candidate_only=true`
- `production_effect=none`
- `broker_action=none`
- `manual_review_required=true`
- `production_state_mutated=false`
- `baseline_config_mutated=false`
- `official_target_weights_mutated=false`
- `automatic_candidate_promotion=false`
- `auto_enrollment_without_owner_approval=false`
- `shadow_enrollment_allowed=false`
- `automatic_enrollment_allowed=false`
- `owner_approval_executed=false`

禁止输出 owner approval、shadow enrollment、production weight update、baseline config mutation、official target weights write、broker order 或 automatic promotion。

## 阶段拆解

|阶段|状态|验收标准|
|---|---|---|
|TRADING-091A Real Evaluation Policy Config|BASELINE_DONE|新增 `config/etf_portfolio/dynamic_v3_real_evaluation.yaml`，配置 promotion gate thresholds、comparison baselines、overfit thresholds、output policy 和 safety；unsafe config fail closed。|
|TRADING-091B Real v0.3 Policy Materialization|BASELINE_DONE|从 v0.4 lower_turnover 和 TRADING-090 v0.3 templates 生成真实可回测 DynamicAllocationPolicyConfig，不修改 source production config。|
|TRADING-091C Price-Driven Candidate Evaluation|BASELINE_DONE|复用 TRADING-086 robustness pipeline，对 baseline / v0.2 / v0.4 / v0.3 candidates 运行真实历史评估；从 cached data 运行时必须先通过 `aits validate-data` 等价门禁。|
|TRADING-091D Comparison and Diagnostic Analysis|BASELINE_DONE|生成 v0.3 vs baseline / v0.2 / v0.4 对比表，并分析 constraint hit、false risk-off、drawdown preservation、turnover、static gap 和 overfit concentration。|
|TRADING-091E Promotion Gate|BASELINE_DONE|输出 `promote_candidate` / `observe_only` / `reject`，同时保留 reason codes、blocking conditions、manual review requirement 和 no-production-mutation safety。|
|TRADING-091F Reader Brief Summary|BASELINE_DONE|Reader Brief 增加面向读者的 `Dynamic v0.3 Real Evaluation` 摘要，解释 gate 判定、核心证据和限制；不得暗示已批准或已上线。|
|TRADING-091G Validation Gate|BASELINE_DONE|新增 `aits etf dynamic-v3-rescue validate-real`，校验配置、真实评估 builder、promotion gate、Reader Brief/report registry visibility、安全字段和 no approval/enrollment。|

## 设计决策

- 使用 TRADING-086 `build_dynamic_robustness_report` 的真实价格驱动回测作为基础评估引擎，避免新写并行回测逻辑。
- v0.3 policy materialization 只生成内存中的 candidate DynamicAllocationPolicyConfig；不写回 `dynamic_allocation_policy.yaml`，不写 official target weights。
- Promotion gate 阈值全部写入 `dynamic_v3_real_evaluation.yaml`，避免在评分、gate 或报告路径中出现不可审计硬编码。
- `promote_candidate` 是后续人工复核候选资格，不是 production promotion。

## 验证计划

```bash
python -m pytest tests/test_etf_dynamic_v3_real_evaluation.py tests/test_reader_brief.py -q
python -m ai_trading_system.cli etf dynamic-v3-rescue real-evaluate --as-of 2026-06-04 --end 2026-06-04
python -m ai_trading_system.cli etf dynamic-v3-rescue validate-real
ruff check src tests
python -m compileall src tests
git diff --check
```

最终若时间允许运行：

```bash
python -m pytest tests -q
```

## 状态记录

- 2026-06-06：新增任务并进入 `IN_PROGRESS`。原因：TRADING-090 已完成 deterministic v0.3 rescue baseline，但必须用真实历史价格评估决定是否具备生产候选复核资格。
- 2026-06-06：TRADING-091A-G baseline 实现完成并转入 `VALIDATING`。真实评估使用 cached price data 至 2026-06-04，`validate_data_status=PASS_WITH_WARNINGS`，生成 `dynamic-v3-real-evaluation-report_922c4bccd3e4dff1.json/md`；best v0.3 candidate 为 `dynamic_regime_overlay_v0_3a_constraint_smooth`，constraint hits 从 v0.4 的 568 降至 564，false risk-off delta 为 0，turnover 降至 0.7983，dynamic-vs-static gap 改善至 0.2005，drawdown degradation vs v0.4 为 -0.30pp；但 constraint hit rate 仍超过 gate，full robustness overfit status 为 `REVIEW_REQUIRED`，promotion gate 判定 `reject`。验证为 focused + Reader Brief tests 10 passed，`aits etf dynamic-v3-rescue validate-real` PASS，`real-report --latest` 正常，ruff、compileall、diff check 和全量 pytest 2174 passed / 330 warnings 通过；剩余条件是 owner 复核 reject 结论并决定是否新开后续 rescue/research task。
