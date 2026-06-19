# TRADING-505 to 520 Weight Research Program v1

最后更新：2026-06-19

## 背景

TRADING-500 到 TRADING-504 已在 `docs/research/weight_research_turn_2026-06-19.md`
中完成基础研究转向：失败 taxonomy、分层权重控制架构、B0-B6 消融协议、统计验证 /
holdout 原则和下一阶段 2-3 个简单方向。该基础 RFC 没有实现新候选，没有运行 backfill，
也没有创建 paper-shadow 或 official target weights。

本批次承接附件中的 TRADING-505 到 TRADING-520。目标是把 Weight Research Program v1
从合并 RFC 拆成独立、可审计、可被后续 runner 消费的 source contracts。除非硬停止条件
全部解除，本批次不得进入更高阶段或伪造 B0-B6 实验结果。

2026-06-19 追加：B0 static strategic baseline 已用现有 `etf_assets_v0_1` 默认权重作为
research-only control 完成一次 mini-backfill。该结果只解除 “B0 不可复现” blocker，不解除
B1-B6 独立 runner、signal robustness、untouched holdout 或 v3 candidate blocker。

2026-06-19 再追加：TRADING-511A-D unblock batch 已冻结 B1-B6 runner scope、signal
robustness entry contract 和 untouched holdout/final gate policy，并只实现 B1
execution-control runner。B1 结果为 mixed research-only evidence，不能自动授权 B2。

## 范围

### Phase A source artifacts

- `docs/research/weight_research_retrospective.json/md`
- `docs/research/weight_control_architecture_rfc.json/md`
- `docs/research/ablation_protocol.json`
- `docs/research/ablation_and_baseline_protocol.md`
- `docs/research/statistical_validation_policy.json`
- `docs/research/statistical_validation_and_holdout_policy.md`
- `docs/research/next_research_program_roadmap.json/md`

### Phase B source artifacts

- `docs/research/research_program_control_plane.json/md`
- `docs/research/research_window_catalog.json/md`
- `docs/research/portfolio_utility_scorecard_contract.json/md`
- `docs/research/ablation_runner_contract.json/md`
- `docs/research/research_result_comparison_harness.json/md`
- `docs/research/weight_research_program_v1_reader_brief.md`
- `docs/research/weight_research_program_v1_snapshot.json/md`

### Phase C partial artifact

- `docs/research/b0_static_strategic_baseline_result.json/md`
- `docs/research/ablation_runner_scope_freeze.json/md`
- `docs/research/signal_robustness_entry_contract.json/md`
- `docs/research/untouched_holdout_final_gate_policy.json/md`
- `docs/research/b1_execution_control_result.json/md`

### Phase C/D hard-stop boundary

TRADING-510 到 TRADING-520 需要真实 B0-B6 mini/full 消融结果。当前只完成 B0 控制组
mini-backfill 和 B1 execution-control mini-backfill。B2-B6 没有独立 runner 和真实分层结果，
不能把 P0 动态策略总结果写成分层消融完成。

## 安全边界

本批次所有 artifacts 必须固定：

```text
research_only=true
manual_review_only=true
hypothetical_only=true
official_target_weights=false
broker_action_allowed=false
order_ticket_generated=false
paper_shadow_activation=false
extended_shadow_allowed=false
live_trading_allowed=false
production_effect=none
owner_decision_appended=false
```

## 硬停止条件

进入 TRADING-510 B0 mini-backfill 前必须满足：

1. B0 strategic baseline source 被 owner 或现有 policy 明确认可为 research-only control。
2. `research_window_catalog.json` 明确区分 development、mini-backfill、diagnostic 和
   untouched/final holdout；已反复使用的 casebook 不得作为最终 holdout。
3. `portfolio_utility_scorecard_contract.json` 的参数在实验前冻结，并带 policy metadata。
4. ablation runner 必须调用 `aits validate-data` 同一路径或同一 validation code path。
5. runner 输出不得包含 official target weights、broker/order、paper-shadow activation 或
   production mutation 语义。

如果任一条件不满足，program snapshot 必须保持
`WEIGHT_RESEARCH_PROGRAM_NEEDS_MORE_EVIDENCE` 或 blocked 状态。

进入 TRADING-511 B1 前必须额外满足：

1. B1 runner 能单独表达 execution / no-trade / turnover-control 机制，不能混入 trend、
   momentum、relative strength、risk/regime 等其他新增机制。
2. B1 输出必须与 B0 使用同一 frozen mini window、scorecard contract 和 validate-data gate。
3. B1 必须披露 signal robustness 状态；若为 `BLOCKED`，必须 fail closed。

## 阶段状态

|阶段|任务|当前状态|说明|
|---|---|---|---|
|Phase A|TRADING-500~504|BASELINE_DONE|基础 RFC 已存在；本批次拆成独立 JSON/Markdown source artifacts。|
|Phase B|TRADING-505~509|BASELINE_DONE|合同层已实现，不运行回测。|
|Phase C|TRADING-510~516|PARTIAL_B0_B1_DONE|B0 control 与 B1 execution-control mini-backfill 已完成；B2-B6 仍缺独立 runner、signal robustness evidence 和逐层比较。|
|Phase D|TRADING-517~520|BLOCKED_BY_EVIDENCE|必须等待 B0~B6 真实消融结果；不得提前选择 v3 candidate。|

## 验收标准

- 所有 Phase A/B JSON 为有效 JSON，含 `schema_version`、`task_id`、`status`、
  `market_regime=ai_after_chatgpt`、source refs、safety boundary 和 Reader Brief 字段。
- Failure taxonomy 覆盖附件要求的 10 个类别。
- B0-B6 ablation layer 每层只引入一个主要机制，并显式写明 required outputs。
- Scorecard 参数具备 owner、version/status、rationale、intended effect、planned validation
  和 review condition；不得出现无解释投资阈值。
- Window catalog 明确把已用 stress/diagnostic windows 标为 development/diagnostic，
  不允许 final holdout 使用。
- Program snapshot 在缺真实 B2~B6 结果时保持 needs-more-evidence，不生成 v3 spec。
- B0 result 必须披露 `aits validate-data` 状态、窗口、run id、B000 source weights、runtime
  artifacts、hash、return/drawdown/turnover/cost/benchmark proxies 和 safety boundary。
- B1 result 必须披露 511A-C contract validation、`aits validate-data` 状态、holdout access、
  forbidden logic check、B1 vs B0 comparison、mixed evidence interpretation 和 safety boundary。

## 状态记录

- 2026-06-19：新增任务和需求文档；当前实现目标是 Phase A/B source contract baseline
  和 Phase C/D hard-stop snapshot，不运行 backfill、不生成 official target weights、
  不激活 paper-shadow、不触发 broker/order、不 append owner decision。
- 2026-06-19：Phase A/B source artifacts 已补齐：
  `weight_research_retrospective`、`weight_control_architecture_rfc`、
  `ablation_protocol`、`statistical_validation_policy`、
  `next_research_program_roadmap`、`research_program_control_plane`、
  `research_window_catalog`、`portfolio_utility_scorecard_contract`、
  `ablation_runner_contract`、`research_result_comparison_harness` 和
  `weight_research_program_v1_snapshot`；snapshot 状态为
  `WEIGHT_RESEARCH_PROGRAM_NEEDS_MORE_EVIDENCE`。TRADING-510~520 因缺 B0
  research-only control owner/system 冻结、untouched holdout 和 B0 runner scope
  保持 blocked。验证通过 focused tests、task-register consistency、documentation contract
  和 `git diff --check`。
- 2026-06-19：新增 `B000/static_default_portfolio` benchmark registry 修复，使
  `static_default_portfolio` baseline 在配置存在显式 B001-B008 时仍可复现；B0 使用
  `normal_market_regime` mini window（2023-01-03 至 2023-07-31）运行完成，`aits
  validate-data` 为 `PASS_WITH_WARNINGS`，runtime ETF summary data quality 为 `PASS`。
  B0 result 已写入 `docs/research/b0_static_strategic_baseline_result.json/md`。Phase C
  仍只到 `PARTIAL_B0_DONE`，B1-B6 继续 blocked，原因是缺独立 runner 和 signal
  robustness evidence，不能用 P0 动态策略总结果替代逐层消融。
- 2026-06-19：按 owner 指示新增并完成 TRADING-511A-D unblock batch：冻结
  `ablation_runner_scope_freeze`、`signal_robustness_entry_contract` 和
  `untouched_holdout_final_gate_policy`，新增 `aits etf weight-research validate-contracts`
  和 `run-b1`。真实 B1 run `b1_execution_control_result_20260619T082549` 输出
  `B1_MINI_BACKFILL_COMPLETE_RESEARCH_ONLY`，data quality=`PASS_WITH_WARNINGS`，
  B1 vs B0 return_delta=0.002862、drawdown_reduction=-0.001205、turnover_delta=0.124318；
  解释为 mixed research-only evidence。B2-B6 仍 blocked，未经 owner/system 复核不得继续。
