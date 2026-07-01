# TRADING-2303 Current Constituents Proxy Diagnostics Only

最后更新：2026-07-01

## 状态

`VALIDATING`

## 背景

TRADING-2302 已完成 breadth / participation data feasibility audit，真实结论为：

- strict PIT breadth 被 historical constituents、historical weights、delisted coverage、symbol mapping 和 known-at / reported-at 缺口阻断；
- current constituents proxy 只允许 diagnostics / POC，不允许作为 actual-path validation、promotion、paper-shadow、production 或 broker evidence；
- 推荐下一步为 `TRADING-2303_Current_Constituents_Proxy_Diagnostics_Only`。

本仓库当前没有可审计的 QQQ / SPY / SMH current constituents snapshot，也没有基于 snapshot 的 constituent price coverage audit。因此本任务的首版实现必须 fail closed：生成 source-blocked diagnostics package，明确缺失来源、bias 风险和下一步，而不是用 ETF ratio proxy 冒充 constituent breadth。

## 目标

新增 CLI：

```bash
aits research trends current-constituents-breadth-proxy-diagnostics
```

该命令承接 TRADING-2302 输出，生成 current constituents breadth proxy diagnostics-only 包：

- `current_constituents_breadth_proxy_diagnostics_report.md`
- `breadth_proxy_signal_distribution_matrix.json`
- `breadth_proxy_asset_horizon_drilldown.json`
- `breadth_proxy_bias_warning_report.json`
- `breadth_proxy_next_step_recommendation.json`

## 安全边界

```yaml
pit_status: current_constituents_proxy_only
strict_pit_ready: false
promotion_allowed: false
paper_shadow_allowed: false
production_allowed: false
broker_action: none
candidate_artifact_generated: false
candidate_signal_series_generated: false
actual_path_validation_executed: false
```

## 实施拆解

1. Source-blocked diagnostics builder。
   - 默认读取 TRADING-2302 feasibility output directory。
   - 检查 current constituents snapshot directory 是否存在且包含目标 ETF snapshot。
   - 当前默认无 snapshot 时输出 `CURRENT_CONSTITUENTS_PROXY_DIAGNOSTICS_SOURCE_BLOCKED`。
   - 不读取 price cache、不联网、不下载外部数据。

2. Signal distribution and drilldown。
   - 从 TRADING-2302 signal concept matrix 继承 breadth signal concepts。
   - 对每个 concept 输出 `NOT_COMPUTABLE_CURRENT_CONSTITUENTS_SNAPSHOT_MISSING`。
   - 对 QQQ / SPY / SMH x 5d / 10d / 20d 输出 source-blocked drilldown rows。

3. Bias warning and next step。
   - 明确 survivorship bias、lookahead bias、current winner backfill 和 weight concentration 风险。
   - 推荐下一步为 owner 提供 frozen current constituent snapshot，或继续 historical constituent data-source due diligence。

4. 文档和 registry。
   - 更新 `docs/research/current_constituents_breadth_proxy_diagnostics_report.md`。
   - 更新 `config/report_registry.yaml`、`docs/artifact_catalog.md` 和 `docs/system_flow.md`。

5. 验证。
   - focused parallel pytest 覆盖 CLI 注册、source-blocked outputs、安全边界、错误 mode。
   - 运行 Ruff、compileall、docs / registry / task-register checks 和 `git diff --check`。

## 验收标准

- CLI implemented: `aits research trends current-constituents-breadth-proxy-diagnostics`。
- 无 current constituent snapshot 时，真实 run status 为 source-blocked diagnostics，而不是 ready / generator / validation。
- 输出五个 required artifacts，并披露 selected market regime=`ai_after_chatgpt` 和 actual requested date range。
- 不生成 candidate-bound executable artifacts、不生成 signal series、不执行 actual-path validation、不启动 forward observe runtime。
- 所有 outputs 固定 promotion、paper-shadow、production、broker false/none。

## 进展记录

- 2026-07-01: 根据 TRADING-2302 recommended_next_action 和 owner post-2302 roadmap 新增并进入 `IN_PROGRESS`。当前 worktree 已有两个无关 research 文档未提交改动，本任务必须 selective staging，不能混入无关改动。
- 2026-07-01: 实现完成并转入 `VALIDATING`。新增 `aits research trends current-constituents-breadth-proxy-diagnostics`、source-blocked diagnostics builder、TRADING-2302 上游契约校验、source coverage matrix、signal distribution matrix、asset/horizon drilldown、bias warning、next-step recommendation、safety boundary、report registry / artifact catalog / system flow 更新和 focused tests。真实 run status=`CURRENT_CONSTITUENTS_PROXY_DIAGNOSTICS_SOURCE_BLOCKED`，source_snapshot_status=`ALL_TARGET_CURRENT_CONSTITUENTS_SNAPSHOTS_MISSING`，signal_concept_count=7，computable_signal_concept_count=0，asset_horizon_row_count=27，data quality status=`NOT_APPLICABLE_SOURCE_BLOCKED_STATIC_DIAGNOSTICS`；所有 outputs 固定 promotion / paper-shadow / production / broker false / none，且 `strict_pit_ready=false`、`candidate_artifact_generated=false`、`candidate_signal_series_generated=false`、`actual_path_validation_executed=false`。验证通过 focused parallel pytest 16 passed、docs/registry/task-register focused parallel pytest 54 passed、Ruff、compileall 和 contract-validation 193 passed。
