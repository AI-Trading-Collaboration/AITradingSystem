# TRADING-2315 Regime State Machine Design Audit

最后更新：2026-07-01

## 状态

`VALIDATING`

## 背景

Owner post-2302 roadmap 将 regime state machine 放在 P2 辅助层，目标是作为
diagnostic segmentation / validation stratification / interpretation layer，而不是
新的 direct strategy signal。

TRADING-2315 只做 design audit。它不得生成 regime label series，不得把 label
写入 scoring、position sizing、daily report recommendation、paper-shadow 或
broker path。

## 目标

新增 CLI：

```bash
aits research trends regime-state-machine-design-audit
```

设计 diagnostic-only regime labels 和 state-machine 边界，覆盖：

- `uptrend`
- `late_uptrend`
- `drawdown`
- `panic`
- `rebound`
- `failed_rebound`
- `range_bound`
- `high_volatility`
- `low_volatility`

## 输入

默认读取：

```text
config/research/regime_state_machine_design_policy.yaml
owner post-2302 roadmap / TRADING-2301 backlog context
```

本命令不读取 cached market / macro data，不调用 `validate_data_cache`，因为它不计算
features、scores、backtests、daily reports 或 label series。任何后续 TRADING-2316
生成 label series 时必须重新评估 data-quality gate 和 PIT / known-at contract。

## 产物

- `regime_state_machine_design_audit_summary.json`
- `regime_label_taxonomy.json`
- `regime_label_taxonomy.csv`
- `regime_transition_rule_matrix.json`
- `regime_transition_rule_matrix.csv`
- `regime_anti_lookahead_guardrail_matrix.json`
- `regime_anti_lookahead_guardrail_matrix.csv`
- `regime_candidate_segmentation_use_case_matrix.json`
- `regime_candidate_segmentation_use_case_matrix.csv`
- `regime_label_generator_poc_route.json`
- `regime_state_machine_safety_boundary.json`
- `docs/research/regime_state_machine_design_audit.md`

## 实施边界

1. Label taxonomy。
   - 每个 label 必须记录 diagnostic interpretation、allowed usage、blocked usage、
     PIT requirement 和 lookahead risk。
   - 不得使用 future return、future drawdown 或 realized post-event outcome 定义
     runtime label。

2. Transition / anti-lookahead guardrails。
   - Transition rule 只能描述 allowed ex-ante state transition design，不计算真实
     transition。
   - 必须记录 delayed confirmation / no future outcome / no hindsight relabeling /
     label versioning / missing input fail-closed 等 guardrails。

3. Candidate segmentation use cases。
   - 只能用于 validation stratification、risk-cap interpretation、candidate failure
     attribution 和 owner review context。
   - 不允许 direct strategy signal、position sizing、portfolio weight、broker action。

4. 安全边界。
   - `diagnostic_only=true`
   - `candidate_signal_generated=false`
   - `regime_label_series_generated=false`
   - `generator_implemented=false`
   - `actual_path_validation_executed=false`
   - `promotion_allowed=false`
   - `paper_shadow_allowed=false`
   - `production_allowed=false`
   - `broker_action=none`

## 验收标准

- CLI implemented: `aits research trends regime-state-machine-design-audit`。
- Summary 披露 selected market regime=`ai_after_chatgpt` 和 data_quality_status=
  `NOT_APPLICABLE_STATIC_DESIGN_AUDIT`。
- Label taxonomy 覆盖 owner roadmap 的 9 个 labels。
- Guardrail matrix 明确阻断 future outcome / hindsight relabeling / direct signal use。
- Candidate segmentation matrix 覆盖 volatility risk-cap、breadth proxy、AI leadership
  和 liquidity pressure 的 diagnostic-only use。
- 输出不得生成 label series、candidate signal、promotion、paper-shadow、production 或
  broker-ready 结论。

## 进展记录

- 2026-07-01: 根据 owner post-2302 roadmap 和 TRADING-2314 next task 新增并进入
  `IN_PROGRESS`。当前 worktree 已有两个无关 research 文档未提交改动，本任务必须
  selective staging，不能混入无关改动。
- 2026-07-01: 实现完成并进入 `VALIDATING`。真实 run status 为
  `REGIME_STATE_MACHINE_DESIGN_AUDIT_READY_DIAGNOSTIC_ONLY`，data_quality_status 为
  `NOT_APPLICABLE_STATIC_DESIGN_AUDIT`，label_count=9，transition_rule_count=8，
  guardrail_count=6，candidate_segmentation_use_case_count=4。
- 2026-07-01: Label taxonomy 覆盖 `uptrend`、`late_uptrend`、`drawdown`、
  `panic`、`rebound`、`failed_rebound`、`range_bound`、`high_volatility`、
  `low_volatility`。候选分段用途只覆盖 volatility risk-cap、breadth proxy、
  AI leadership 和 liquidity rates 的 diagnostic interpretation。
- 2026-07-01: 验证通过 Ruff、compileall、TRADING-2315 focused parallel pytest
  7 passed、相邻 registry/docs focused parallel pytest 43 passed、contract-validation
  193 passed（runtime artifact:
  `outputs/validation_runtime/contract-validation_20260701T055619Z/test_runtime_summary.json`）
  和 `git diff --check`。本命令不读取 cached data，因此不运行 `aits validate-data`
  作为门禁。
