# TRADING-2304 Breadth Proxy Signal Concept Selection

最后更新：2026-07-01

## 状态

`VALIDATING`

## 背景

TRADING-2303 已生成 current constituents breadth proxy diagnostics-only 包，真实状态为：

- `CURRENT_CONSTITUENTS_PROXY_DIAGNOSTICS_SOURCE_BLOCKED`
- `source_snapshot_status=ALL_TARGET_CURRENT_CONSTITUENTS_SNAPSHOTS_MISSING`
- `signal_concept_count=7`
- `computable_signal_concept_count=0`
- `asset_horizon_row_count=27`

因此 TRADING-2304 的最优实现不是凭概念偏好选择 signal，而是 fail closed：生成 no-selection/source-blocked scorecard，说明选择标准无法评估，并阻止进入 TRADING-2305 generator POC。

## 目标

新增 CLI：

```bash
aits research trends breadth-proxy-signal-concept-selection
```

该命令读取 TRADING-2303 diagnostics outputs，生成 selection package：

- `breadth_proxy_signal_selection_report.md`
- `breadth_signal_concept_scorecard.json`
- `selected_breadth_signal_concepts.json`
- `rejected_breadth_signal_concepts.json`

## 选择标准

标准来自 owner roadmap，但当前只能记录为不可评估：

- 信号分布是否有足够区分度；
- 是否过度 neutral；
- 是否集中在单一 asset；
- 是否能解释 trend fragility；
- 是否和现有 price trend 信号不完全重复；
- bias 风险是否可接受。

## 安全边界

```yaml
selection_status: source_blocked_no_selection
selected_concept_count: 0
advance_to_generator_allowed: false
promotion_allowed: false
paper_shadow_allowed: false
production_allowed: false
broker_action: none
candidate_artifact_generated: false
candidate_signal_series_generated: false
actual_path_validation_executed: false
```

## 实施拆解

1. TRADING-2303 source validation。
   - 要求 upstream status 为 `CURRENT_CONSTITUENTS_PROXY_DIAGNOSTICS_SOURCE_BLOCKED`。
   - 要求 upstream safety fields 全部关闭。
   - 若 TRADING-2303 artifacts 缺失或 safety fields 被打开，fail closed。

2. Source-blocked scorecard。
   - 为每个 signal concept 生成 scorecard row。
   - `distribution_discrimination_status`、`neutrality_status`、`asset_concentration_status`、`trend_fragility_status`、`overlap_status` 和 `bias_acceptability_status` 均标记为 `NOT_EVALUATED_SOURCE_BLOCKED`。
   - 不使用主观 numeric threshold 或打分。

3. Selection outputs。
   - `selected_breadth_signal_concepts.json` 为空 rows，`selected_concept_count=0`。
   - `rejected_breadth_signal_concepts.json` 标记为 `REJECTED_SOURCE_BLOCKED_NOT_SIGNAL_QUALITY_REJECTION`，保留未来重新评估条件。

4. 文档和 registry。
   - 更新 `docs/research/breadth_proxy_signal_selection_report.md`。
   - 更新 `config/report_registry.yaml`、`docs/artifact_catalog.md` 和 `docs/system_flow.md`。

5. 验证。
   - focused parallel pytest 覆盖 CLI 注册、source-blocked outputs、安全边界、错误 mode / unsafe upstream。
   - 运行 Ruff、compileall、docs / registry / task-register checks 和 `git diff --check`。

## 验收标准

- CLI implemented: `aits research trends breadth-proxy-signal-concept-selection`。
- 当前 TRADING-2303 source-blocked 输入下，真实 run status 为 `BREADTH_PROXY_SIGNAL_SELECTION_SOURCE_BLOCKED_NO_SELECTION`。
- 输出 required artifacts，且 selected concepts 为空、rejected concepts 说明 source blocker 和 reconsideration condition。
- 不生成 candidate-bound executable artifacts、不生成 signal series、不执行 actual-path validation、不启动 forward observe runtime。
- 所有 outputs 固定 promotion、paper-shadow、production、broker false/none。

## 进展记录

- 2026-07-01: 根据 TRADING-2303 source-blocked diagnostics 和 owner post-2302 roadmap 新增并进入 `IN_PROGRESS`。当前 worktree 已有两个无关 research 文档未提交改动，本任务必须 selective staging，不能混入无关改动。
- 2026-07-01: 实现完成并转入 `VALIDATING`。新增 `aits research trends breadth-proxy-signal-concept-selection`、TRADING-2303 source validation、source-blocked scorecard、selected / rejected concept artifacts、selection safety boundary、report registry / artifact catalog / system flow 更新和 focused tests。真实 run status=`BREADTH_PROXY_SIGNAL_SELECTION_SOURCE_BLOCKED_NO_SELECTION`，source_status=`CURRENT_CONSTITUENTS_PROXY_DIAGNOSTICS_SOURCE_BLOCKED`，source_snapshot_status=`ALL_TARGET_CURRENT_CONSTITUENTS_SNAPSHOTS_MISSING`，signal_concept_count=7，selected_concept_count=0，rejected_concept_count=7，advance_to_generator_allowed=false，data quality status=`NOT_APPLICABLE_SOURCE_BLOCKED_STATIC_SELECTION`；所有 outputs 固定 promotion / paper-shadow / production / broker false / none，且 `candidate_artifact_generated=false`、`candidate_signal_series_generated=false`、`actual_path_validation_executed=false`。验证通过新 focused parallel pytest 10 passed、docs/registry/task-register focused parallel pytest 41 passed、Ruff、compileall、docs/task-register focused 31 passed、contract-validation 193 passed 和 `git diff --check`。
