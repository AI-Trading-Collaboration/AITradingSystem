# TRADING-101: Dynamic v3 Rescue Real Evaluator Sweep Integration

## 背景

TRADING-093 到 TRADING-100 已完成 parameter sweep platform MVP，但默认 evaluator 为
`tiny_fixture_proxy`，只用于 CI、artifact contract 和平台闭环验证，不构成真实投资结论。
TRADING-101 将 sweep runner 接入 TRADING-091 的 dynamic v0.3 real evaluation pipeline，
让人工 research run 可以用真实 ETF price-driven evaluation 生成 candidate metrics。

## 范围

1. 将 evaluator mode 固定为 `tiny_fixture_proxy` 和 `real_dynamic_v3_rescue`。
2. 默认 CI / focused tests 保持 `tiny_fixture_proxy`。
3. Research/manual run 可通过 config 或 CLI 指定 `real_dynamic_v3_rescue`。
4. Sweep runner 将 candidate parameters 注入 real evaluation materialization。
5. `candidate_results.jsonl` 增加 `evaluator_mode`、`evaluator_version`、
   `real_evaluation_artifact_path`、`data_quality` 和 `metrics_source`。
6. Tiny fixture reports 必须显示 `not_for_investment_decision`；promotion pack 不允许 tiny
   mode 进入 `promote_candidate`。
7. Real mode metrics 必须来自真实 evaluation artifact；`validate-sweep` 必须检查 artifact
   path 存在；leaderboard 只能进入 `observe_only` / `promote_candidate`，仍不得自动
   `production_candidate`。
8. 新增 small real sweep smoke，`max_candidates <= 20`、`workers=1`，输出 sample real sweep
   artifact。

## 实施步骤

|步骤|状态|验收|
|---|---|---|
|Evaluator contract|DONE|配置 schema、CLI override、manifest、leaderboard 和 reports 均披露 evaluator mode / version。|
|Real adapter|DONE|每个 real candidate 写入独立 real evaluation artifact，candidate metrics 从该 artifact 派生。|
|Safety gate|DONE|Tiny fixture 标记 not-for-investment；promotion pack tiny mode 最多 `review_required` / `reject`；real mode 不生成 production candidate。|
|Validation|DONE|`validate-sweep` 区分 tiny vs real，real mode 缺 artifact path fail closed。|
|Docs / Reader Brief|DONE|README、runbook、system flow、artifact catalog、report registry、Reader Brief 摘要同步 evaluator fields。|
|Validation run|VALIDATING|focused tests、ruff、compileall、git diff --check 通过；全量 pytest 已尝试但超时。|

## 参数注入 Pilot Baseline

`real_dynamic_v3_rescue` 的首版参数注入是 pilot baseline：它把 sweep axes 映射到既有
TRADING-091 materialization controls，而不是新增第二套 production policy。映射规则使用
代码中的命名常量，目的只是让真实 evaluator 对候选参数产生可复现的差异化 real artifact。

- `constraint_buffer_bps` 转换为 target buffer 权重。
- `rescue_intensity` 缩放 soft penalty、drawdown guardrail 和 emergency cash step。
- `smooth_window_days` 缩放 single rebalance delta。
- `turnover_penalty` 缩放 weekly turnover cap 和 trend overlay scale。
- `rebalance_cooldown_days` 增加 minimum rebalance delta。
- `risk_off_confirmation_days` 提高 emergency risk threshold，并同步 v3 guardrail confirmation floor。
- `drawdown_guard` 使用 `none` / `soft` / `hard` multiplier 控制 drawdown guardrail 强度。

退出条件：当 owner 认可更明确的 real evaluator policy manifest 后，将该 pilot mapping 迁移到
reviewed configuration，并用真实 sensitivity / walk-forward evidence 校准映射范围。

## 安全边界

该集成仍属于 research / observe-only / manual review workflow。`real_dynamic_v3_rescue`
只提升 metrics 来源可信度，不代表 production approval。所有自动输出继续固定
`production_effect=none`、`broker_action=none`、`manual_review_required=true`、
`production_candidate_generated=false`，不得写 official target weights、baseline config、
production state、owner approval、shadow enrollment approval 或 broker action。

## 进展记录

- 2026-06-06：新增需求文档并登记 TRADING-101；进入实现。目标是保留 tiny fixture CI
  contract，同时为 manual research sweep 接入真实 price-driven dynamic v3 rescue evaluation。
- 2026-06-06：实现完成并进入 VALIDATING。新增 `real_dynamic_v3_rescue` evaluator、CLI/config
  override、per-candidate real evaluation artifact、`candidate_results.jsonl` evaluator provenance
  fields、tiny fixture not-for-investment 标记、tiny promotion cap、real artifact validation、small
  real smoke config 和 Reader Brief/docs 更新。Small real sweep sample
  `sweep_20260606T034508Z_ae5ae1d8` 以 `workers=1` 跑通 2 个 real candidates，
  `validate-sweep` PASS，data quality 为 `PASS_WITH_WARNINGS`。
- 2026-06-06：验证通过 focused tests
  `tests/test_etf_dynamic_v3_parameter_research.py tests/test_etf_dynamic_v3_real_evaluation.py tests/test_reader_brief.py -q`
  （16 passed）、`ruff check .`、`compileall src tests` 和 `git diff --check`。全量
  `pytest tests -q` 已尝试，15 分钟超时且未返回失败明细。
