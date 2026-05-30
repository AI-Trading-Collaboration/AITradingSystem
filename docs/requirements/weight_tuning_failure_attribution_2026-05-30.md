# TRADING-059A Weight Tuning Failure Attribution & Guardrail Diagnostics

最后更新：2026-05-30

## 背景

TRADING-059 restricted backtest weight tuning v0.1 已跑通，但最新真实结果为：

- `result = NO_CANDIDATE`
- `candidate_status = rejected`
- `guardrail_status = FAIL`
- `production_effect = none`
- `auto_promotion = false`

这说明受限调参框架已形成闭环，但当前数据、信号质量和 guardrail 约束下没有可接受的 shadow weight candidate。TRADING-059A 的目标不是降低 guardrail 或生成新 candidate，而是解释为什么没有候选通过。

## 范围

TRADING-059A 读取 latest 或指定日期的 `weight_tuning_summary.json` 与 `weight_tuning_candidates.json`，输出结构化失败归因、guardrail 分解、walk-forward 失败摘要、near-miss candidates、root cause 和下一步建议。

正式产物：

- `artifacts/weight_tuning_failure/YYYY-MM-DD/weight_tuning_failure_summary.json`
- `artifacts/weight_tuning_failure/YYYY-MM-DD/weight_tuning_failure_summary.md`
- debug 模式可写 `weight_tuning_failure_candidates_debug.json`

新增命令：

- `aits parameters explain-weight-tuning-failure --latest`
- `aits parameters explain-weight-tuning-failure --date YYYY-MM-DD`
- `aits parameters explain-weight-tuning-failure --summary PATH`
- `aits parameters explain-weight-tuning-failure --latest --debug`
- `aits parameters validate-weight-tuning-failure --latest`
- `aits reports weight-tuning-failure --latest`

## 非目标与安全边界

- 不重新调参。
- 不扩大搜索空间。
- 不降低 guardrail。
- 不修改 `config/parameters/production/current.yaml`。
- 不放开 fallback signal 自由调参。
- 不生成伪 candidate。
- 不把 rejected candidate 改写为 watch。
- 不自动 promotion。

必须保持：

- `production_effect: none`
- `manual_review_required: true`
- `auto_promotion: false`
- `production_config_modified: false`

## 实施步骤

|阶段|状态|验收标准|
|---|---|---|
|1. Governance/docs|DONE|task register、需求文档、artifact catalog、system flow、report registry 更新。|
|2. Candidate diagnostics|DONE|TRADING-059 candidate 输出包含 `constraint_status`、`rejection_reasons`、`walk_forward_summary` 等字段，旧 artifact 缺字段时 attribution 模块可降级解释。|
|3. Attribution core|DONE|缺 summary/candidates 时输出 `BLOCKED`；存在 artifact 时统计 candidate rejection、failure ranking、near miss、root cause 和 next action。|
|4. CLI/report|DONE|新增 explain/validate/report CLI，生成 JSON/Markdown 和 report alias。|
|5. Dashboard/Reader Brief/shadow backtest|DONE|Dashboard 与 Reader Brief 展示 failure attribution 摘要；shadow backtest promotion decision 引用 failure artifact 且保持 rejected。|
|6. Validation|VALIDATING|专项测试覆盖 missing artifact、guardrail/performance/walk-forward/search root cause、Markdown、Dashboard、Reader Brief、shadow backtest safety；全量 pytest、ruff、compileall、diff check 正在最终复验。|

## Root Cause 分类

允许输出：

- `signal_quality_limited`
- `search_space_too_narrow`
- `guardrails_too_strict`
- `portfolio_turnover_too_high`
- `drawdown_control_insufficient`
- `no_alpha_detected`
- `walk_forward_unstable`
- `data_insufficient`
- `mixed`

## 验收标准

运行：

```bash
aits parameters explain-weight-tuning-failure --latest
aits parameters validate-weight-tuning-failure --latest
aits reports weight-tuning-failure --latest
aits reports reader-brief --latest
aits parameters shadow-backtest --latest --dry-run
```

应满足：

- weight tuning failure JSON/Markdown 生成；
- `status=NO_CANDIDATE_EXPLAINED` 或 `BLOCKED`；
- `production_effect=none`；
- `manual_review_required=true`；
- `auto_promotion=false`；
- `production_config_modified=false`；
- Reader Brief 展示 root cause；
- shadow backtest supporting artifacts 引用 `weight_tuning_failure_summary.json`；
- `config/parameters/production/current.yaml` 不变。

最终验证：

```bash
python -m pytest -q
python -m ruff check scripts src tests
python -m compileall src scripts
git diff --check
```

## 进展记录

- 2026-05-30：新增 TRADING-059A 并进入 `IN_PROGRESS`。目标是把 TRADING-059 的 `NO_CANDIDATE / guardrail_status=FAIL` 转为可审计、可排序、可用于下一步任务选择的失败归因报告。
- 2026-05-30：从 `IN_PROGRESS` 改为 `VALIDATING`。已完成 failure attribution core、TRADING-059 candidate diagnostics 增强、CLI/report alias、Dashboard、Reader Brief、shadow backtest supporting artifact 和专项测试；真实 latest 2026-05-28 failure attribution 为 `NO_CANDIDATE_EXPLAINED`，root cause 为 `portfolio_turnover_too_high`，top failure reason 为 `cost_drag_too_high`，most common guardrail failure 为 `turnover_guardrail_failed`，recommended next action 为 `review_portfolio_turnover_constraints`；production 参数 hash 不变。
