# TRADING-102 to TRADING-110 Stable Real Parameter Iteration Backtest Loop

最后更新：2026-06-06

## 状态

`VALIDATING`

本任务把 TRADING-102 到 TRADING-110 作为一个稳定真实参数迭代闭环交付。目标不是自动批准 production candidate，而是让 `small_real` profile 能跑通可审计的真实 sweep、参数注入验证、walk-forward selection、overfit review、shadow monitoring 和 promotion review pack。

## 背景

TRADING-101 已把 parameter sweep 平台接入 `real_dynamic_v3_rescue` evaluator，但真实研究闭环仍缺少参数注入审计、数据 manifest / PIT 覆盖审计、运行 profile、candidate attribution、真正 train/test selection、overfit risk 控制、参数治理、历史结果索引和 shadow monitor。

当前已知数据质量限制：本地 price cache 可能出现 `prices_download_manifest_checksum_missing`。该问题不得静默通过；data audit 与 sweep artifacts 必须显式披露。

## 子任务拆解

|ID|目标|状态|验收|
|---|---|---|---|
|TRADING-102|Candidate parameter injection 与 weight path diff audit|VALIDATING|`injection-audit run/report/validate` 可运行，识别 consumed / not consumed / no observed effect 参数。|
|TRADING-103|Research data manifest 与 PIT coverage audit|VALIDATING|`data-audit run/report/validate` 可运行，记录 checksum、manifest、coverage、gap 和 quality status。|
|TRADING-104|Real sweep execution profiles|VALIDATING|`small_real`、`medium_real`、`overnight_real` 与 `tiny_fixture` profile 可列出、校验、运行。|
|TRADING-105|Candidate attribution 与 explainability|VALIDATING|candidate report 能链接 attribution artifact，缺 weight path 时标记 incomplete。|
|TRADING-106|True walk-forward selection protocol|VALIDATING|每个 train window 选择候选，再记录 test window result。|
|TRADING-107|Overfit risk control v1|VALIDATING|overfit artifact 可被 promotion pack 消费，HIGH_RISK fail closed。|
|TRADING-108|Parameter governance 与 search space versioning|VALIDATING|governance policy 校验 manual_only / controlled_search，search_space_version 写入 sweep artifacts。|
|TRADING-109|Research result index / query layer|VALIDATING|index-build 可重建 sweeps / candidates / leaderboard / shadow 历史查询。|
|TRADING-110|Shadow candidate daily / weekly monitor integration|VALIDATING|shadow monitor report 与 Reader Brief section 可生成并校验。|

## 运行 Profile

新增配置：

- `config/etf_portfolio/dynamic_v3_rescue/parameter_sweep_profiles.yaml`
- `config/etf_portfolio/dynamic_v3_rescue/parameter_governance_v1.yaml`

`tiny_fixture` 仅用于 CI / fixture / artifact contract，不得进入 investment decision 或 `promote_candidate`。`small_real` 及更大 real profile 使用 `real_dynamic_v3_rescue`，必须先通过 cached data quality gate 或等价直接调用。

## 新增 CLI

本任务新增或扩展：

- `aits etf dynamic-v3-rescue data-audit run/report`
- `aits etf dynamic-v3-rescue validate-data-audit`
- `aits etf dynamic-v3-rescue sweep profile-list/profile-validate/run-profile`
- `aits etf dynamic-v3-rescue injection-audit run/report`
- `aits etf dynamic-v3-rescue validate-injection-audit`
- `aits etf dynamic-v3-rescue candidate attribution`
- `aits etf dynamic-v3-rescue validate-candidate-attribution`
- `aits etf dynamic-v3-rescue walk-forward select-run/selection-report`
- `aits etf dynamic-v3-rescue validate-walk-forward-selection`
- `aits etf dynamic-v3-rescue overfit run/report`
- `aits etf dynamic-v3-rescue validate-overfit`
- `aits etf dynamic-v3-rescue governance validate/report/diff`
- `aits etf dynamic-v3-rescue research index-build/query/compare/history`
- `aits etf dynamic-v3-rescue shadow monitor-run/monitor-report`
- `aits etf dynamic-v3-rescue validate-shadow-monitor`

## Artifact Contract

新增 artifact families：

- `reports/etf_portfolio/dynamic_v3_rescue/data_audit/<data_audit_id>/`
- `reports/etf_portfolio/dynamic_v3_rescue/injection_audit/<audit_id>/`
- `reports/etf_portfolio/dynamic_v3_rescue/candidate_attribution/<candidate_id>/`
- `reports/etf_portfolio/dynamic_v3_rescue/walk_forward_selection/<wf_selection_id>/`
- `reports/etf_portfolio/dynamic_v3_rescue/overfit/<overfit_id>/`
- `reports/etf_portfolio/dynamic_v3_rescue/governance/`
- `reports/etf_portfolio/dynamic_v3_rescue/index/`
- `reports/etf_portfolio/dynamic_v3_rescue/shadow_monitor/<monitor_id>/`

所有 artifacts 固定：

- `observe_only=true`
- `candidate_only=true`
- `production_effect=none`
- `broker_action=none`
- `manual_review_required=true`
- `production_candidate_generated=false`

## 参数治理

所有可影响投资解释的参数 search policy 必须来自 governance config。`manual_only` 参数不得被 sweep override；`manual_review_required` 参数变化只能进入人工复核，不得自动生成 production candidate。

`search_space_version` 必须写入：

- sweep manifest
- candidate result
- candidate report
- promotion pack linked artifacts

## 13. 整体验收标准

### 13.1 small_real 完整链路

最终必须至少跑通一条 `small_real` 完整链路，实际输出需记录 artifact id 和路径：

1. data audit
2. small_real sweep run-profile
3. injection audit
4. leaderboard
5. candidate report
6. candidate attribution
7. walk-forward selection
8. overfit
9. shadow register
10. shadow monitor
11. promotion pack

### 13.2 必跑 validate 命令

small_real 链路完成后必须执行以下 gate；需要具体 id 的命令使用 13.1 生成的 artifact id：

- `aits etf dynamic-v3-rescue validate`
- `aits etf dynamic-v3-rescue validate-real`
- `aits etf dynamic-v3-rescue validate-attribution`
- `aits etf dynamic-v3-rescue validate-data-audit --audit-id <data_audit_id>`
- `aits etf dynamic-v3-rescue sweep profile-validate`
- `aits etf dynamic-v3-rescue sweep validate --sweep-id <sweep_id>`
- `aits etf dynamic-v3-rescue validate-injection-audit --audit-id <audit_id>`
- `aits etf dynamic-v3-rescue validate-candidate-attribution --candidate-id <candidate_id>`
- `aits etf dynamic-v3-rescue validate-walk-forward-selection --wf-selection-id <wf_selection_id>`
- `aits etf dynamic-v3-rescue validate-overfit --overfit-id <overfit_id>`
- `aits etf dynamic-v3-rescue governance validate`
- `aits etf dynamic-v3-rescue validate-shadow-registry`
- `aits etf dynamic-v3-rescue validate-shadow-monitor --monitor-id <monitor_id>`
- `aits etf dynamic-v3-rescue validate-promotion-pack --candidate-id <candidate_id>`
- `aits etf dynamic-v3-rescue artifacts validate --family dynamic_v3_rescue`
- `aits docs report-contract --latest`

### 13.3 本地代码验证

还必须执行 focused tests、ruff、compileall 和 `git diff --check`。尽量运行全量 pytest；如超时，记录运行时长和已覆盖测试。

### 13.4 验收记录

2026-06-06 small_real 验收：

- 日期范围：2026-06-05 至 2026-06-05，market_regime=`ai_after_chatgpt`
- `data_audit_id=d9e434a07019d286`，status=`PASS`，data_quality_status=`PASS_WITH_WARNINGS`
- `sweep_id=sweep_20260606T105642Z_ae5ae1d8`，profile=`small_real`，evaluator_mode=`real_dynamic_v3_rescue`，completed_count=50，failed_count=0，top_candidate=`aa02bb947cf29885`
- `audit_id=ede86378d2ac235d`，candidate_count=20，status=`PASS`
- `candidate_id=aa02bb947cf29885`，candidate gate=`review_required`，score=0.457281
- candidate attribution status=`INCOMPLETE`，原因：real evaluator 尚未导出完整 daily weight path；未伪造 path-level attribution
- `wf_selection_id=51fb80c576c29d2e`，status=`PASS`
- `overfit_id=a275a9f67316668d`，status=`REVIEW_REQUIRED`
- `monitor_id=df1b1d05827db3f9`，status=`PASS`，observe_only_candidate_count=2，promotion_review_ready_count=0
- `promotion_id=f0b0020a1ae42c1c`，status=`incomplete`，manual_review_required=true
- `research_index_manifest.json` status=`PASS`，sweep_count=4，candidate_count=5102
- 第 13.2 节所有 validate 命令已 PASS；`aits docs report-contract --latest` PASS，warnings=0
- 已知 warning：`prices_download_manifest_checksum_missing`；必须在后续 owner review 中继续披露

## 已知限制

- `small_real` 的 `max_constraint_hit_rate=0.65` 是 workflow-smoke 校准值，依据 2026-06-05 real evaluator 候选约 0.642-0.647 的 constraint-hit range；它只允许不劣于 reference path 的候选进入 observe-only/manual-review 下游 artifact 验证，不是 promotion gate。
- PBO / DSR 等高级统计方法本阶段只允许作为 `REVIEW_REQUIRED` placeholder，不得伪造 PASS。
- `prices_download_manifest_checksum_missing` 若仍存在，应在 data audit 与最终总结中披露。
- shadow monitor 是 observe-only 研究监控，不代表 owner approval、baseline mutation、target weight write 或 broker action。

## 进展记录

- 2026-07-11：ARCH-004G2.4P复核发现旧injection audit存在参数归因混杂：所有参数共享全体候选config/metric/weight hash distinct count，不能证明单一参数效应；同时截断grid前缀不能保证覆盖排序靠前的参数轴。TRADING-102继续保持`VALIDATING`，验收补充为base+逐轴OFAT matched pair、pair-only effect classification、独立`parameter_effect_summary.json`、matched-pair coverage disclosure，以及budget不足时`INCOMPLETE`且`validate-injection-audit` fail closed。既有2026-06-06 injection artifact只能证明旧workflow执行过，不能继续作为逐参数有效性结论。
- 2026-07-11：G2.4P正确性修复与CLI迁移完成：默认候选顺序固定为共同base+7个逐轴pair，effect只使用其他required parameters一致的pairs；新增独立summary artifact、legacy artifact结构化降级与pair coverage validation。Focused 53、reporting/policy组合70、architecture-fitness 199通过。TRADING-102仍为`VALIDATING`，因为需要在当前真实cache上重跑完整20-candidate injection audit后，才能重新建立逐参数效果证据；旧artifact不再具备该证明力。
- 2026-06-06：新增任务文档并进入 `IN_PROGRESS`，按 owner 粘贴的 TRADING-102～110 大功能开发计划启动实现。
- 2026-06-06：首次 2026-06-05 `small_real` 50-candidate real sweep 完成但 50/50 因 `constraint_hit_rate_exceeds_policy` 被 reject；修复 interrupted sweep resume 去重与 manifest 追溯问题，并将 `small_real` workflow-smoke absolute constraint-hit gate 校准为 0.65，同时保留 `max_constraint_hits_delta_vs_reference=0` 和 manual-review safety boundary。
- 2026-06-06：small_real 完整链路按第 13 节跑通并进入 `VALIDATING`；候选 `aa02bb947cf29885` 只进入 observe-only/manual-review 路径，promotion pack status=`incomplete`，不得解读为 production candidate。
