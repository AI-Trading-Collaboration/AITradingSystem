# TRADING-090 Dynamic v0.3 Constraint-Aware Rescue Policy

最后更新：2026-06-05

## 背景

TRADING-089 已完成 v0.4 lower_turnover 的 review-only 复核。v0.4 显著降低 false risk-off 和 turnover，并把 dynamic vs static 从明显落后改善为正贡献；但同时暴露两个 shadow blocker：

- `CONSTRAINT_HIT_WORSENED`：constraint hits 增加 143。
- `DRAWDOWN_PRESERVATION_FAILED`：drawdown preservation 为 -5.95pp。

TRADING-090 的目标不是批准 v0.4，也不是开启 shadow enrollment，而是生成 constraint-aware v0.3 rescue candidates，验证是否能保留 v0.4 的有效改进，同时降低 constraint hits 并恢复 drawdown protection。

## 安全边界

所有配置、模板、batch runner、报告和 validation gate 必须固定：

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

本阶段禁止输出 owner approval、shadow enrollment、production weight update、baseline config mutation、broker order 或 automatic promotion。

## 阶段拆解

|阶段|状态|验收标准|
|---|---|---|
|TRADING-090A Constraint-Aware Rescue Policy Config|BASELINE_DONE|新增 `config/etf_portfolio/dynamic_v3_constraint_aware_rescue.yaml`，配置包含 blocker targets、preservation targets、normalization、soft penalty、smoothing、drawdown guardrail、emergency risk-off、candidate templates、evaluation thresholds 和 safety；unsafe config fail closed。|
|TRADING-090B Constraint Root-Cause Loader|BASELINE_DONE|从 TRADING-089 v0.4 review package 读取 constraint decomposition、drawdown review、dynamic rescue / robustness source links 和 safety；缺少必需证据时 fail closed，optional artifact 只产生 warning。|
|TRADING-090C Pre-Constraint Target Normalization Engine|BASELINE_DONE|对 raw target weights 做 sum-to-one、category cap pre-reduction、cash floor/cap 处理，并输出 reason codes。|
|TRADING-090D Soft Constraint Penalty and Allocation Smoothing|BASELINE_DONE|在接近 caps 时向 feasible interior 收敛，使用 previous weights smoothing，并限制 single-step delta；emergency override 可按 policy 绕过。|
|TRADING-090E Drawdown Guardrail Overlay|BASELINE_DONE|多信号确认后提升 CASH、优先降低 SMH/SOXX、其次降低 QQQ，并输出 guardrail reason codes。|
|TRADING-090F Emergency Risk-Off Exception|BASELINE_DONE|至少 N 个独立确认信号才触发 emergency risk-off；触发后允许更快提升 CASH 和降低 QQQ/SMH/SOXX，同时保留 safety。|
|TRADING-090G v0.3 Candidate Template Generator|BASELINE_DONE|生成 v0.3a/v0.3b/v0.3c/v0.3d candidate templates；每个模板链接 root-cause evidence，candidate-only，且不修改 production policy。|
|TRADING-090H v0.3 Candidate Batch Runner|BASELINE_DONE|新增 `aits etf dynamic-v3-rescue run`，使用 v0.4 review evidence 和 v0.3 templates 生成 candidate comparison，显式比较 v0.1/v0.4/static/baseline/QQQ/SPY/SMH 指标。|
|TRADING-090I v0.3 Rescue Evaluation Report|BASELINE_DONE|生成 JSON/Markdown 评估报告，包含 safety banner、v0.4 blocker summary、candidate table、constraint/drawdown/false signal/turnover/benchmark 比较、eligibility recommendation、remaining blockers 和 source links。|
|TRADING-090J Reader Brief Dynamic v0.3 Rescue Section|BASELINE_DONE|Reader Brief 显示 v0.3 rescue status、best candidate、constraint/drawdown 状态、safety 和详细报告链接；不得暗示 enrollment。|
|TRADING-090K Dynamic v0.3 Rescue Validation Gate|BASELINE_DONE|新增 `aits etf dynamic-v3-rescue validate`，检查配置、loader、engines、templates、runner、report、Reader Brief、report registry、safety 和 no approval/enrollment。|

## 设计决策

- 采用 deterministic template rescue，不引入无界 optimizer、ML model 或 native numerical optimization。
- batch runner 的 baseline 实现以 TRADING-089 v0.4 review package 为证据源，先生成可审计 candidate comparison；真实价格重跑仍由既有 dynamic robustness / rescue 链路提供 source artifacts。
- 所有投资解释阈值写入 v0.3 policy config，并通过 policy metadata 记录 owner、version/status、rationale、validation 和 review condition。
- runtime artifacts 继续输出到 `reports/etf_portfolio/dynamic_v3_rescue/`，不提交生成结果。

## 验证计划

本任务完成前至少运行：

```bash
python -m pytest tests/test_etf_dynamic_v3_rescue.py tests/test_reader_brief.py -q
python -m ai_trading_system.cli etf dynamic-v3-rescue validate
python -m ruff check config src tests scripts docs
python -m compileall -q src tests scripts
git diff --check
```

最终若时间允许运行全量：

```bash
python -m pytest tests -q
```

## 状态记录

- 2026-06-05：新增任务并进入 `IN_PROGRESS`。原因：TRADING-089 已确认 v0.4 lower_turnover 改善了 false risk-off 和 turnover，但 constraint hit worsening 与 drawdown preservation failure 仍阻断 shadow readiness；本阶段开始构建 candidate-only v0.3 rescue workflow。
- 2026-06-05：TRADING-090A-K baseline 实现完成，总任务转入 `VALIDATING`。原因：新增 dynamic v0.3 rescue policy、root-cause loader、pre-constraint normalization、soft constraint penalty/smoothing、drawdown guardrail、emergency risk-off、v0.3a-v0.3d templates、`aits etf dynamic-v3-rescue run/report/validate`、evaluation report、Reader Brief `Dynamic v0.3 Rescue` section、report registry/artifact catalog/system flow/runbook/README integration 和 focused tests；验证为 focused + Reader Brief tests 12 passed，`aits etf dynamic-v3-rescue validate` PASS，ruff、compileall、diff check 和全量 pytest 2171 passed / 330 warnings 通过；剩余条件是 owner 复核 v0.3 candidate report，且本阶段仍不得 approval、enroll 或生产写入。
