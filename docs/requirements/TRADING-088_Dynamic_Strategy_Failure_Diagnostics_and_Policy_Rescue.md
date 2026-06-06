# TRADING-088 Dynamic Strategy Failure Diagnostics and Policy Rescue

最后更新：2026-06-05

- 父任务：TRADING-088
- 优先级：P0
- 状态：VALIDATING
- owner：system + project owner
- 创建日期：2026-06-05
- 来源计划：`G:/Download/TRADING-088_Dynamic_Strategy_Failure_Diagnostics_and_Policy_Rescue_Development_Plan.md`

## 背景

TRADING-083 到 TRADING-087 已建立 two-layer dynamic ETF allocation workflow：
Layer 1 生成 trend signal calibration，Layer 2 将 trend/regime/risk scores 映射为
candidate-only dynamic allocation，随后进入 dynamic calibration、dynamic robustness 和
owner-approved-only dynamic shadow review。

真实 dynamic shadow review package 已生成，但未执行 owner approval。失败证据显示：
当前 `dynamic_regime_overlay_v0_1` 虽然改善 drawdown，但相对 static base、baseline、
QQQ 和 SMH 明显牺牲 upside，false risk-off 过多，turnover 超过 overfit threshold，
constraint hit rate 高，并且 trend signal quality score 为 0.0 且 signal redundancy 高。

TRADING-088 的目标不是批准 v0.1，也不是放松 gate，而是诊断失败来源并生成受控
v0.2 rescue policy candidates，供后续 TRADING-089 或 owner review 使用。

## 安全边界

所有 TRADING-088 输出必须固定：

```text
observe_only = true
candidate_only = true
production_effect = none
broker_action = none
manual_review_required = true
production_state_mutated = false
baseline_config_mutated = false
official_target_weights_mutated = false
automatic_candidate_promotion = false
auto_enrollment_without_owner_approval = false
shadow_enrollment_allowed = false
automatic_enrollment_allowed = false
owner_approval_executed = false
```

禁止：

```text
production_weight_update
baseline_config_mutation
official_target_weights_write
broker_order
automatic_candidate_promotion
auto_approval
auto_enrollment
```

TRADING-088 的 rescue candidates 只用于 diagnostic / candidate-only evaluation，不写
`data/etf_portfolio/target_weights.csv`，不修改 `dynamic_allocation_policy.yaml` 的 v0.1
baseline，不生成 owner approval，不 enroll shadow，不触发 broker。

## 子任务拆解

|子任务|状态|验收摘要|
|---|---|---|
|TRADING-088A Dynamic Failure Diagnostics Policy Config|BASELINE_DONE|新增 `config/etf_portfolio/dynamic_failure_diagnostics.yaml`，显式定义 failure thresholds、false signal definitions、signal buckets、turnover/constraint thresholds、rescue templates、improvement requirements 和 mandatory safety fields。|
|TRADING-088B Dynamic Failure Dataset Builder|BASELINE_DONE|从 dynamic robustness report 的 full daily path / comparison paths 构建 failure dataset，包含 decision、weights、scores、constraint hits、forward outcomes、false risk-off/on、underperformance 和 evaluation-only safety。|
|TRADING-088C Layer-1 Trend Signal Failure Attribution|BASELINE_DONE|输出 trend score bucket vs forward outcome、quality score、return/drawdown lift、false-signal contributors、redundancy count 和 candidate-only signal adjustment recommendations。|
|TRADING-088D False Risk-Off / False Risk-On Attribution|BASELINE_DONE|输出 false risk-off/on counts、opportunity/drawdown costs、by-regime/by-score/by-trigger 分解和 top false signal periods。|
|TRADING-088E Layer-2 Allocation Underperformance Attribution|BASELINE_DONE|量化 total underperformance vs static、cash drag、growth/semiconductor underweight cost、SPY overweight effect、drawdown improvement value 和 net risk-adjusted tradeoff。|
|TRADING-088F Turnover and Constraint Hit Breakdown|BASELINE_DONE|输出 turnover by source、constraint hit count/rate/by type、top periods 和 slow-switch/lower-turnover/constraint-review recommendations。|
|TRADING-088G Dynamic v0.2 Rescue Policy Templates|BASELINE_DONE|生成 v0.2 less defensive、v0.3 slow switch、v0.4 lower turnover、v0.5 AI trend confirmed only 四个 evidence-linked candidate-only templates。|
|TRADING-088H Rescue Candidate Batch Runner|BASELINE_DONE|新增 `aits etf dynamic-rescue run --base-candidate ...` / `--latest-failed-package`，复用 TRADING-086 robustness pipeline 对 rescue templates 做 v0.1/static/baseline/benchmark comparison。|
|TRADING-088I Rescue Candidate Evaluation Report|BASELINE_DONE|生成 rescue evaluation JSON/Markdown，包含 safety banner、failed v0.1 summary、Layer 1/2 attribution、false signal、turnover/constraint、template list、candidate comparison、improvement summary、remaining blockers、recommended next action 和 source links。|
|TRADING-088J Reader Brief Dynamic Rescue Section|BASELINE_DONE|Reader Brief 新增 `Dynamic Strategy Rescue` 只读区块，展示 v0.1 status、main failures、best rescue candidate、improvement、remaining blockers、safety 和 detailed report link。|
|TRADING-088K Dynamic Rescue Validation Gate|BASELINE_DONE|新增 `aits etf dynamic-rescue validate`，fail-closed 校验 A-J workflow、report registry、Reader Brief integration 和 no production/approval/enrollment safety boundary。|

## 设计约束

1. `config/etf_portfolio/dynamic_failure_diagnostics.yaml` 是 TRADING-088 heuristic / threshold / template policy manifest；失败阈值、score buckets、turnover/constraint thresholds、rescue template changes 和 improvement requirements 不得隐藏在代码字面量中。
2. Dynamic failure dataset 使用 TRADING-086 robustness full daily path 和 comparison daily paths；如果旧 report 缺少 full paths，只能基于可用 sample rows 生成受限 dataset，并在 source artifacts 中保留 upstream limitation。
3. Rescue templates 在内存复制 v0.1 dynamic allocation policy 后生成 candidate policies；不得直接改写 `config/etf_portfolio/dynamic_allocation_policy.yaml`。
4. `dynamic-rescue run` 依赖 cached market / macro data 时必须先执行 `aits validate-data` 等价门禁，并在 downstream report 中披露 `data_quality_status` 和 quality report path。
5. Validation gate 使用 synthetic validation prices，不依赖本地未跟踪 market cache；真实 rescue interpretation 仍必须运行 `dynamic-rescue run` 并由 owner 复核。
6. `rescue_success_candidate_found` 不是 approval、shadow enrollment 或 production promotion，只表示存在可进入下一阶段深度 robustness/shadow review 的 candidate evidence。
7. Runtime artifacts 写入 ignored `reports/etf_portfolio/dynamic_rescue/`；源码变更只包含 policy/config、module、CLI、Reader Brief/report registry、docs 和 tests。

## 验收命令

完成后至少运行：

```bash
python -m pytest tests/test_etf_dynamic_rescue.py tests/test_etf_dynamic_robustness.py tests/test_reader_brief.py tests/test_report_index.py -q
python -m ruff check config src tests scripts docs
python -m compileall -q src tests scripts
git diff --check
python -m ai_trading_system.cli etf dynamic-rescue validate
```

最终验收还需要全量：

```bash
python -m pytest tests -q
```

## 进展记录

- 2026-06-05: 新增并进入 IN_PROGRESS。基于真实 dynamic v0.1 failed package 观察结果，开始实现 failure diagnostics dataset、Layer 1/Layer 2 attribution、false signal decomposition、turnover/constraint breakdown、bounded v0.2 rescue templates、batch evaluation、Reader Brief section 和 final validation gate；本阶段不得 approval、enroll、修改 production config 或触发 broker。
- 2026-06-05: A-K baseline 实现完成并转入 VALIDATING。新增 dynamic failure diagnostics policy、`ai_trading_system.etf_portfolio.dynamic_rescue`、`aits etf dynamic-rescue run/report/validate`、dynamic robustness full daily/comparison path exposure、rescue dataset/report/validation writers、Reader Brief `Dynamic Strategy Rescue` section、report registry/artifact catalog/system flow/runbook/README integration 和 focused tests。验证：`tests/test_etf_dynamic_rescue.py -q` 4 passed；focused ruff/compileall 通过。剩余条件是真实 latest failed package 上运行 `dynamic-rescue run` 后由 owner 复核 rescue candidate 是否足以进入 TRADING-089。
