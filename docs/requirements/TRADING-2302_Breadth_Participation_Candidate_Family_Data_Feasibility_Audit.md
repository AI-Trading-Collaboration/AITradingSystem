# TRADING-2302 Breadth Participation Candidate Family Data Feasibility Audit

最后更新：2026-07-01

## 状态

`VALIDATING`

## 背景

TRADING-2301 已把 `breadth_participation` 排为最高优先级的新 first-layer candidate family。该方向直接回应 `baseline_plus_trend_structure` 当前形态暴露的问题：只看指数自身趋势结构，不足以稳定确认趋势质量。

本任务不实现 generator，也不生成 candidate signal series。核心目标是先回答 QQQ / SPY / SMH breadth 所需输入是否可审计、是否具备 PIT 历史成分可行性、current constituents proxy 是否只能作为 diagnostics，以及后续 TRADING-2303 应进入 generator POC、proxy diagnostics，还是数据源决策。

## CLI

新增命令：

```bash
aits research trends breadth-participation-candidate-family-feasibility-audit \
  --target-etfs QQQ,SPY,SMH \
  --target-assets QQQ,SPY,SMH \
  --candidate-family breadth_participation \
  --output-dir outputs/research_trends/breadth_participation_candidate_family_feasibility_audit \
  --mode feasibility_audit
```

`--mode` 只允许 `feasibility_audit`。不得支持 generator、actual-path validation、promotion、paper-shadow、production 或 broker action。

## 实施拆解

1. Research-only feasibility builder。
   - 构造 ETF / index price history、current constituents、historical ETF constituents、constituent price history 和 alternative proxy input 的 data inventory。
   - 该实现只读取静态任务定义，不读取本地 market / macro cache，不联网，不自动下载外部数据。
   - data quality status 固定为 `NOT_APPLICABLE_STATIC_FEASIBILITY_AUDIT`；后续任何 cached-data-dependent generator、scoring、backtest 或 report 必须先跑 `aits validate-data` 或同源 validation code path。

2. PIT / bias / proxy risk audit。
   - 输出 historical constituent PIT gap matrix。
   - 输出 current constituents proxy risk matrix。
   - 明确 strict PIT breadth 当前被 historical constituent gap 阻断，current constituents proxy 默认只可 diagnostics / POC，不得作为 promotion evidence。

3. Candidate design sketch。
   - 只记录候选 family 用途、candidate IDs、signal concepts、direction mapping 和 5d / 10d / 20d horizon。
   - 不生成 candidate-bound signal spec、signal series、prediction artifact，也不执行 actual-path validation。

4. Validation route and 2303 task route。
   - strict PIT 或 PIT approximation 可行时，后续才允许 `TRADING-2305_Breadth_Proxy_Candidate_Generator_POC`。
   - 若只有 current constituents proxy 可行，则进入 `TRADING-2303_Current_Constituents_Proxy_Diagnostics_Only`。
   - 若无可靠数据，则进入 data source decision。

5. 文档和 registry。
   - 生成 breadth feasibility audit、data inventory、PIT/bias risk、design sketch、2303 route 文档。
   - 更新 `config/report_registry.yaml`、`docs/artifact_catalog.md`、`docs/system_flow.md` 和 `docs/task_register.md`。

## 安全边界

所有输出强制：

```yaml
research_only: true
generator_implemented: false
candidate_signal_series_generated: false
prediction_artifact_generated: false
actual_path_validation_executed: false
candidate_artifact_generated: false
promotion_allowed: false
paper_shadow_allowed: false
production_allowed: false
broker_action: none
```

不得输出 `PROMOTION_READY`、`PAPER_SHADOW_READY`、`PRODUCTION_READY` 或 `BROKER_READY`。

## 验收标准

- CLI implemented: `aits research trends breadth-participation-candidate-family-feasibility-audit`。
- 生成附件要求的 JSON / CSV runtime artifacts 和 Markdown research docs。
- historical constituents gap、current constituents proxy risk、candidate design sketch、validation route 和 TRADING-2303 route 都可由 focused tests 覆盖。
- 所有 outputs 固定 promotion、paper-shadow、production、broker false / none。
- 不生成 generator、不生成 signal series、不生成 prediction artifact、不执行 actual-path validation。
- 更新 report registry、artifact catalog、system flow 和 task register。

## 进展记录

- 2026-07-01: 根据 owner 附件进入 `IN_PROGRESS`。当前 worktree 已存在 TRADING-1087 / ops / data download / docs 相关未提交改动；本任务必须 selective staging，不能混入无关改动。实现口径为静态 feasibility audit，不读取 cached market / macro data，不联网，不下载新外部数据。
- 2026-07-01: 实现完成并转入 `VALIDATING`。新增 `aits research trends breadth-participation-candidate-family-feasibility-audit`、静态 feasibility builder、input inventory、PIT gap matrix、current proxy risk matrix、candidate design sketch、signal concept matrix、validation route、TRADING-2303 route、safety boundary、report registry / artifact catalog / system flow 更新和 focused tests。真实 run status=`BREADTH_FEASIBILITY_AUDIT_READY_PROXY_ONLY`，recommended_next_action=`TRADING-2303_Current_Constituents_Proxy_Diagnostics_Only`，data quality status=`NOT_APPLICABLE_STATIC_FEASIBILITY_AUDIT`；所有 outputs 固定 promotion / paper-shadow / production / broker false / none，且 `generator_implemented=false`、`candidate_artifact_generated=false`、`actual_path_validation_executed=false`。验证通过 Ruff、compileall、focused parallel pytest 28 passed、full parallel pytest 3776 passed、docs freshness 481 docs / 0 issues、documentation contract PASS 1201 reports、contract-validation 193 passed、task-register consistency run/validate PASS 和 `git diff --check`。
