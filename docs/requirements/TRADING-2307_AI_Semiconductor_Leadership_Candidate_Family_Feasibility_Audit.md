# TRADING-2307 AI Semiconductor Leadership Candidate Family Feasibility Audit

最后更新：2026-07-01

## 状态

`VALIDATING`

## 背景

Owner post-2302 roadmap 将 AI / semiconductor leadership 作为 breadth proxy 后的第二阶段 P1 candidate family。当前已知状态：

- `baseline_plus_trend_structure_scope_narrowed_confirmation_v1` 当前形态已 reject recommended；
- `risk_appetite_refined_confidence_v1` 当前形态已 archive；
- breadth proxy 已完成 TRADING-2303 / TRADING-2304，但因 current constituents snapshot 缺失仍 source-blocked；
- 下一步应研究更贴近 QQQ / SMH / AI 半导体暴露的 leadership signal，而不是重开泛化 `risk_appetite` 当前形态。

本任务只做 feasibility audit 和 design sketch，不生成 candidate-bound artifacts、不生成 signal series、不执行 actual-path validation。

## 目标

新增 CLI：

```bash
aits research trends ai-semiconductor-leadership-feasibility-audit
```

该命令生成 research-only artifacts：

- `ai_semiconductor_leadership_feasibility_audit.md`
- `ai_leadership_input_inventory.json`
- `ai_leadership_candidate_design_sketch.json`
- `ai_leadership_validation_route.json`

## 输入候选

- `SMH vs QQQ` relative strength；
- `NVDA vs SMH` leadership；
- `AMD / TSM / AVGO / ASML` relative strength；
- AI core basket vs QQQ；
- semiconductor basket breadth；
- mega-cap AI leadership concentration；
- earnings / capex / event context as future PIT source audit item。

## 安全边界

```yaml
promotion_allowed: false
paper_shadow_allowed: false
production_allowed: false
broker_action: none
generator_implemented: false
candidate_artifact_generated: false
candidate_signal_series_generated: false
actual_path_validation_executed: false
```

## 实施拆解

1. Static feasibility inventory。
   - 不读取 cached market / macro data、不联网、不下载外部数据。
   - 把 price-proxy inputs 标为 `CACHE_VALIDATION_REQUIRED_BEFORE_USE`。
   - 把 basket breadth、earnings、capex 和 event context 标为需要 PIT/source audit。

2. Candidate design sketch。
   - 设计 `ai_semiconductor_leadership_quality_v1`、`smh_relative_strength_leadership_v1`、`ai_core_basket_leadership_v1`。
   - 明确用途：SMH overweight confirmation、AI chain confirmation、semiconductor leadership weakening warning、exposure cap modifier。
   - 明确不得作为 standalone buy/sell / broker signal。

3. Validation route。
   - 只提出 TRADING-2308 generator POC 前置条件。
   - future generator / validation 必须先运行 `aits validate-data` 或同一 data quality code path。
   - event / earnings / capex inputs 必须先做 PIT source audit。

4. 文档和 registry。
   - 更新 `docs/research/ai_semiconductor_leadership_feasibility_audit.md`。
   - 更新 `config/report_registry.yaml`、`docs/artifact_catalog.md` 和 `docs/system_flow.md`。

5. 验证。
   - focused parallel pytest 覆盖 CLI 注册、输出 artifacts、inventory / route / safety boundary、错误 mode。
   - 运行 Ruff、compileall、docs / registry / task-register checks 和 `git diff --check`。

## 验收标准

- CLI implemented: `aits research trends ai-semiconductor-leadership-feasibility-audit`。
- 输出四个 owner-requested artifacts，并披露 selected market regime=`ai_after_chatgpt` 和 actual requested date range。
- 不读取 cached market / macro data；data quality status 为 `NOT_APPLICABLE_STATIC_FEASIBILITY_AUDIT`。
- 不生成 candidate-bound executable artifacts、不生成 signal series、不执行 actual-path validation、不启动 forward observe runtime。
- 所有 outputs 固定 promotion、paper-shadow、production、broker false/none。

## 进展记录

- 2026-07-01: 根据 owner post-2302 roadmap 新增并进入 `IN_PROGRESS`。当前 worktree 已有两个无关 research 文档未提交改动，本任务必须 selective staging，不能混入无关改动。
- 2026-07-01: 实现完成并转入 `VALIDATING`。真实 run status=`AI_SEMICONDUCTOR_LEADERSHIP_FEASIBILITY_AUDIT_READY_PRICE_PROXY_ONLY`，input_count=7，price_proxy_ready_after_dq_count=3，source_audit_required_count=2，route_count=4，generator_poc_ready_now=false，data_quality_status=`NOT_APPLICABLE_STATIC_FEASIBILITY_AUDIT`。验证通过 focused parallel pytest 44 passed、Ruff、compileall、contract-validation 193 passed（runtime artifact=`outputs/validation_runtime/contract-validation_20260701T030336Z/test_runtime_summary.json`）和 `git diff --check`。
