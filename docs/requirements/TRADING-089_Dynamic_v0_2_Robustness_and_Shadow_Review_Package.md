# TRADING-089 Dynamic v0.2 Robustness and Shadow Review Package

最后更新：2026-06-05

## 状态

`VALIDATING`

## 背景

TRADING-088 的 dynamic rescue evaluation 显示 `dynamic_regime_overlay_v0_4_lower_turnover`
是当前最强 rescue candidate：false risk-off、turnover 和 dynamic vs static 表现明显改善。
同时，该候选增加 constraint hits，并且 drawdown preservation 为负。因此它只能进入
review-only 复核包，不能进入 shadow enrollment。

本阶段目标是回答：

```text
v0.4 lower_turnover 是否足够稳健，值得未来 owner 继续复核；
还是因为 constraint hit deterioration 和 drawdown preservation failure 继续阻断？
```

## 安全边界

所有 TRADING-089 outputs 必须固定：

```text
observe_only=true
candidate_only=true
production_effect=none
broker_action=none
manual_review_required=true
production_state_mutated=false
baseline_config_mutated=false
official_target_weights_mutated=false
automatic_candidate_promotion=false
auto_enrollment_without_owner_approval=false
shadow_enrollment_allowed=false
automatic_enrollment_allowed=false
owner_approval_executed=false
```

禁止输出或执行：

```text
approved_for_shadow
shadow_enrollment_record
production_weight_update
baseline_config_mutation
broker_order
auto_promotion
```

## 阶段拆分

|阶段|内容|状态|
|---|---|---|
|TRADING-089A|Dynamic v0.2 review policy config|BASELINE_DONE|
|TRADING-089B|v0.4 candidate evidence loader|BASELINE_DONE|
|TRADING-089C|Rescue improvement attribution|BASELINE_DONE|
|TRADING-089D|Constraint hit decomposition|BASELINE_DONE|
|TRADING-089E|Drawdown preservation failure review|BASELINE_DONE|
|TRADING-089F|Regime-level v0.4 robustness review|BASELINE_DONE|
|TRADING-089G|v0.4 vs static/base/QQQ/SMH comparison|BASELINE_DONE|
|TRADING-089H|Shadow review eligibility gate|BASELINE_DONE|
|TRADING-089I|Dynamic v0.2 review package generator|BASELINE_DONE|
|TRADING-089J|Reader Brief dynamic v0.2 review section|BASELINE_DONE|
|TRADING-089K|Dynamic v0.2 review validation gate|BASELINE_DONE|

## 设计决策

- 新增独立 `dynamic_v2_review` 模块，而不是把 TRADING-089 逻辑并入
  `dynamic_rescue` 或 `dynamic_shadow`。原因是本阶段有自己的 review policy、
  report schema、eligibility gate 和 validation gate。
- `dynamic_v2_review` 只读取 existing rescue / robustness / optional shadow artifacts。
  它不重新执行 market backtest，不写 production weights，也不创建 shadow enrollment。
- 若 optional shadow package 或 operations artifact 缺失，review package 记录 warning；
  rescue report 或 candidate robustness report 缺失则 package CLI fail closed。
- 当前 v0.4 预期分类为 `review_candidate / not_shadow_ready`，blockers 至少包含
  `CONSTRAINT_HIT_WORSENED` 和 `DRAWDOWN_PRESERVATION_FAILED`。

## 验收标准

- `config/etf_portfolio/dynamic_v2_review.yaml` 可加载，阈值和 safety boundary 可验证。
- v0.4 evidence loader 可抽取 static delta、false risk-off、turnover、constraint hit delta、
  drawdown preservation、data quality status 和 source links。
- Improvement attribution 同时显示 positive drivers 与 remaining negative drivers。
- Constraint hit decomposition 按 constraint type 分组，constraint worsening 生成 blocker。
- Drawdown failure review 可识别 negative preservation，输出 root cause 和 guardrail 建议。
- Regime robustness review 覆盖 required regimes，缺失 regime 明确标为 `MISSING`。
- Benchmark comparison 覆盖 failed v0.1、static base、current baseline、QQQ、SPY 和 SMH。
- Eligibility gate 能识别 v0.4 改善，但在 blocker 未解除时保持 `not_shadow_ready`。
- Review package 输出 JSON 和 Markdown，包含 safety banner、blockers、warnings 和 source links。
- Reader Brief 显示 Dynamic v0.2 Review section，缺失 package 时只读显示 `MISSING`。
- `aits etf dynamic-v2-review validate` fail closed 校验 config、module availability、
  package generator、Reader Brief/report registry visibility、安全边界和 no enrollment。
- 文档、task register、report registry、system flow、artifact catalog、README 和 runbook 同步。

## 进展记录

- 2026-06-05: 任务新增并进入 `IN_PROGRESS`；开始实现 TRADING-089A-K baseline。
- 2026-06-05: TRADING-089A-K baseline 实现完成并转入 `VALIDATING`。新增
  `config/etf_portfolio/dynamic_v2_review.yaml`、`dynamic_v2_review` 模块、
  `aits etf dynamic-v2-review package/report/validate`、review package JSON/Markdown、
  Reader Brief `Dynamic v0.2 Review` section、report registry、README、system flow、
  artifact catalog、`docs/runbook_daily_ops.md` 和 focused tests。当前实现会识别
  v0.4 lower-turnover positive evidence，但在 constraint hit worsening 和 negative
  drawdown preservation 未解除时保持 `review_candidate / not_shadow_ready`，且不生成
  approval、shadow enrollment、production mutation 或 broker action。
- 2026-06-05: 验证通过：dynamic related tests 13 passed，`aits etf dynamic-v2-review
  validate` PASS，ruff、compileall、diff check 通过，全量 `python -m pytest tests -q`
  为 2166 passed、330 warnings、耗时 634.90s。真实 latest package smoke 生成
  `dynamic-v2-review-package_8405ad6c02dc.json/md`，状态为 `not_shadow_ready` /
  `review_candidate`，blockers 为 `CONSTRAINT_HIT_WORSENED`、
  `DRAWDOWN_PRESERVATION_FAILED`、`DRAWDOWN_REVIEW_REQUIRED` 和
  `SHADOW_ENROLLMENT_NOT_ALLOWED`。剩余条件是 owner 复核。
