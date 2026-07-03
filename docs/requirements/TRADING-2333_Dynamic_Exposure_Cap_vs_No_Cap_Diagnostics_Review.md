# TRADING-2333 Dynamic Exposure-Cap vs No-Cap Diagnostics Review

最后更新：2026-07-03

## 状态

`VALIDATING`

## 背景

TRADING-2332 已完成 dynamic target baseline 下的 source-bound exposure-cap dry-run，真实 run 输出 `record_count=2490`、`cap_binding_rate=0.455422`、`return_proxy_delta=-0.187258`、`drawdown_proxy_delta=0.045294`，并 route 到 `TRADING-2333_Dynamic_Exposure_Cap_vs_No_Cap_Diagnostics_Review`。TRADING-2327 static baseline diagnostics 也显示 high opportunity cost 和 possible over-binding。本任务承接这些 prior artifacts，系统化判断 dynamic baseline 下 risk-cap / exposure-cap 是否仍有边际保护价值，还是主要构成 redundant binding / missed upside cost。

本任务只做 diagnostics review，不重新执行 simulation，不修改 exposure-cap policy，不进入 paper-shadow、production 或 broker action。

## 实施范围

1. 新增 CLI `aits research trends dynamic-exposure-cap-vs-no-cap-diagnostics-review`。
2. Fail-closed 读取 TRADING-2332 dynamic dry-run outputs。
3. 读取 TRADING-2327 diagnostics 和 TRADING-2326 static dry-run reference 作为 static baseline reference。
4. 读取 TRADING-2331 readiness / PIT caveat context、TRADING-2330 timestamp remediation context 和 TRADING-2323 simulation policy context。
5. 生成 dynamic cap-binding、overbinding、exposure-reduction、return/drawdown tradeoff、false-cost / missed-upside、downside-protection、turnover / cooldown、strategy-overlap diagnostics。
6. 生成 static-vs-dynamic evidence comparison、cap binding period attribution、policy sensitivity recommendation、decision matrix、TRADING-2334 route 和 interpretation boundary。
7. 输出 runtime artifacts、research docs，并更新 report registry、artifact catalog、system flow 和 task register。

## 边界

- 不重新执行 TRADING-2332 dynamic dry-run。
- 不重新执行 TRADING-2326 static dry-run。
- 不修改 exposure-cap policy、cooldown / decay policy、risk-cap trigger series 或 dynamic target baseline wrapper。
- 不生成 target weight、rebalance instruction、buy / sell signal、paper-shadow-ready、production-ready 或 broker-ready artifact。
- 不读取真实券商账户或真实持仓。
- 不将 `NEXT_SESSION_DECISION_POLICY` / PIT caveat wrapper 误标记为 strict PIT。

## Data Quality Policy

TRADING-2333 只读取 TRADING-2332 prior validated dynamic dry-run artifacts，不重新读取 cached market data。因此本任务的 data-validation policy 为：

```text
NOT_APPLICABLE_PRIOR_VALIDATED_DYNAMIC_DRY_RUN_ARTIFACTS_ONLY
```

实现必须确认 2332 `dynamic_target_data_quality_report.json` 存在，并把其中 `data_quality_status` 带入全部 summary / decision outputs。如果该状态为 `FAIL`，diagnostics status 必须为 `DATA_QUALITY_BLOCKED`，overall recommendation 必须为 `DATA_QUALITY_REMEDIATION_REQUIRED`，next task 必须 route 到 `TRADING-2334_Dynamic_Target_Baseline_Data_Remediation`。

## Heuristic Governance

本任务的 binding frequency、overbinding、tradeoff、false-cost、missed-upside、downside-protection、overlap 和 route labels 都是 diagnostics review pilot labels。它们只用于 research-only interpretation 和下一步任务路由，不是生产 policy、position cap、promotion gate 或 owner final approval。若后续进入 policy refinement，相关阈值必须迁移到受审配置或替换为 evidence-backed calibration。

## 验收标准

- CLI 可运行并生成所有 TRADING-2333 runtime artifacts 和 research docs。
- 缺少 required dynamic / static / PIT context artifact 时 fail closed。
- input 或 output 打开 promotion、paper-shadow、production、broker action、target weight、rebalance instruction、buy / sell signal 时 fail closed。
- 输出 dynamic cap-binding、overbinding、exposure-reduction、return/drawdown、false-cost / missed-upside、downside-protection、turnover/cooldown、strategy-overlap diagnostics JSON / CSV。
- 输出 static-vs-dynamic evidence comparison、cap binding period attribution、policy sensitivity recommendation、decision matrix、2334 task route 和 interpretation boundary。
- 所有 outputs 固定 `promotion_allowed=false`、`paper_shadow_allowed=false`、`production_allowed=false`、`broker_action=none`。
- Summary 明确 `data_quality_gate_executed=false`，并说明未重跑 `aits validate-data`，因为本任务只读取 prior validated TRADING-2332 artifacts。

## 验证计划

- `ruff check .`
- `python -m compileall -q src tests`
- TRADING-2333 focused pytest files
- `python -m pytest tests -q` 或项目并行 validation tier
- docs freshness
- documentation contract
- contract-validation tier
- task-register consistency run / validate
- `git diff --check`

## 进展记录

- 2026-07-03：根据 owner 附件新增并进入 `IN_PROGRESS`。当前 worktree 存在两个既有无关 research docs 改动，本任务必须 selective staging，不得混入本次 commit。
- 2026-07-03：实现完成并进入 `VALIDATING`。新增 `dynamic-exposure-cap-vs-no-cap-diagnostics-review` CLI、2332/2327/2326/2331/2330/2323 fail-closed loader、dynamic cap-binding / overbinding / exposure-reduction / return-drawdown / false-cost-missed-upside / downside-protection / turnover-cooldown / strategy-overlap diagnostics、static-vs-dynamic evidence comparison、cap binding period attribution、policy sensitivity recommendation、decision matrix、2334 route、interpretation boundary、registry / catalog / system-flow 文档更新和 TRADING-2333 focused tests。真实 run status=`DYNAMIC_EXPOSURE_CAP_DIAGNOSTICS_REVIEW_READY_PROMOTION_BLOCKED`，data_quality_status=`PASS_WITH_WARNINGS`，record_count=`2490`，cap_binding_rate=`0.455422`，overbinding_label=`OVERBINDING_BLOCKING`，return_proxy_delta=`-0.187258`，drawdown_proxy_delta=`0.045294`，overall_recommendation=`HIGH_INTENSITY_ONLY_FORWARD_OBSERVE`，next_task=`TRADING-2334_High_Intensity_Only_Risk_Cap_Forward_Observe_Plan`。
- 2026-07-03：验证通过 Ruff、compileall、TRADING-2333 focused parallel pytest 22 passed、真实 CLI run、docs freshness 511 docs PASS、documentation contract 1231 reports PASS、task-register consistency run / validate PASS、contract-validation 193 passed、full parallel pytest 4104 passed / 643 warnings 和 `git diff --check`。`contract-validation` runtime artifact=`outputs/validation_runtime/contract-validation_20260703T015615Z/test_runtime_summary.json`；full runtime artifact=`outputs/validation_runtime/full_20260703T015929Z/test_runtime_summary.json`。
