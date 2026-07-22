# TRADING-2457：无污染 Selection Protocol Foundation

最后更新：2026-07-22

状态：`DONE`

稳定任务 ID：`TRADING-2457_UNCONTAMINATED_SELECTION_PROTOCOL_FOUNDATION`

## 背景与优先级

TRADING-096/097 与 R0～R2 已证明当前最关键的研究缺口不是缺少 evaluator，而是 selection 与
evidence role 未被统一约束：既有 source top-N 来自 full-period leaderboard并与 locked holdout
重叠，不能作为无偏 OOS selection；TRADING-2453 还证明 small-real/observe-only policy 可能被错误
消费成 hard eligibility。Owner 已选择 A 关闭当前 TRADING-2452 package，B/C 未授权，因此本任务只
建立未来新研究进入 preregistration 前的通用 admission contract，不启动任何新策略 run。

优先级为 P0：candidate lineage、result visibility、window role、policy intended role 与 holdout access
直接决定研究结论是否可被解释为无偏证据，优先于新增 candidate、参数搜索或报告扩展。

## 与既有能力的边界

- 复用 canonical `ResearchEvaluationContext`、`ResearchPreregistration` 与现有 2449/2451/2452
  eligibility/preregistration evidence；不新建第二套 evaluator、score、walk-forward 或 campaign runner。
- 2449 是 Dynamic-v3 specific gate，2451/2452 是已冻结的具体 package；本任务补的是跨未来研究可复用的
  candidate-universe derivation、data-role/visibility timeline、policy-consumer role 与 single-access
  prospective holdout约束。
- B 是新的 reviewed hard-eligibility policy + 新 package；C 是 per-template/per-axis causal diagnostic
  replay。两者都需要未来显式 owner 指令，本任务不能隐式触发、替代或预先批准 B/C。

## 阶段与输入输出

|阶段|输入|计算/约束|输出与验收|
|---|---|---|---|
|S0 threat model|TRADING-2446～2448、096/097、2452/2453 closeout与canonical lifecycle/context metadata|冻结 `DISCOVERY_HISTORICAL_KNOWN`、`TRAIN_SELECTION`、`HISTORICAL_SEEN_VALIDATION`、`PROSPECTIVE_UNTOUCHED` 四类角色及单向允许边；不读取市场结果|本需求、角色矩阵、禁止边和边界完成|
|S1 typed spec|hypothesis/baseline/candidate-generator commitment、2021 active context、metric/kill criteria、policy refs、窗口角色、freeze/visibility/access chronology|canonical serialization、semantic id、依赖闭包、candidate origin与policy-consumer role compatibility|versioned `SelectionProtocolSpec`/policy；foundation状态只能是 `FOUNDATION_ONLY` 或 `NOT_PREREGISTERED`|
|S2 pure validator|typed spec及其source/hash commitments|检查freeze早于结果可见/访问、role/date不冲突、full-period top-N污染、prospective access、policy-role mismatch与source/hash drift|content-derived validation checks/blockers；任何污染或缺件fail closed|
|S3 admission handoff|S2 validation|只根据checks生成admission状态，不推导策略收益|`READY_FOR_OWNER_PROTOCOL_AUTHORING`或明确blocker；execution/search/holdout均false|
|S4 integration|最终tracked tree|focused、architecture、contract、manifests/hashes；Full只在自然双线集成边界一次|task/docs/system-flow/generated evidence一致，`production_effect=none`|

## 必须执行的合同

1. Active primary research window 必须从 `2021-02-22` 开始；`2022-12-01` 只允许作为明确标记的
   immutable historical/legacy role，不能成为 active default、required comparator 或 minimum start。
2. Candidate universe 不得由与评价/test/holdout重叠的 full-period leaderboard、top-N 或结果可见区间派生。
3. `freeze_at` 必须早于任何 selection/test/prospective result visibility 或 access event；时间线相等、
   缺失、倒序或未经授权的 prospective access 均阻断。
4. Hard eligibility policy 必须声明 owner、version、rationale、intended consumer role 与 review condition；
   smoke、observe-only、reporting-only policy 不能被 hard gate 消费。
5. Known historical data只能用于 discovery 或显式 historical-seen validation，不能标记为 unbiased OOS。
6. Validator 只解析 contract/policy bytes并重算语义，不调用 market provider、cached-data DQ、evaluator、
   backtest、candidate search、paper-shadow、promotion、production 或 broker/order。

## 实施文件与并行 ownership

策略 worker 独占：

- `config/research/uncontaminated_selection_protocol_policy.yaml`；
- `src/ai_trading_system/contracts/research_selection_protocol.py`；
- `tests/test_trading2457_uncontaminated_selection_protocol.py`。

Integration coordinator 单写：本需求、`docs/task_register*.md`、
`docs/research/current_research_strategy_execution_chain.md`、`docs/system_flow.md`、双线 operating model、
generated manifests/registry views与compatibility hashes。策略 worker 不修改共享文件。

## 测试矩阵

- valid generic fixture deterministic round-trip/semantic id/validation PASS；
- 2021 active window PASS，2022 active/default/minimum-start表述 FAIL；
- freeze-after-visibility、full-period top-N lineage、selection/test/holdout overlap、prospective access FAIL；
- observe-only/reporting-only policy 被 hard eligibility消费 FAIL；缺owner/version/rationale/review/hash FAIL；
- source/hash、foundation status、nested typed payload或serialized id tamper FAIL；
- known historical fixture 不得形成 unbiased claim；
- safety fields固定为 false/none，测试证明没有 evaluator/backtest/search/market-data调用。

## 完成与后续阻塞

S0～S4 foundation 完成后本任务可 `DONE`；这只表示通用 admission contract 可用。具体新研究仍等待：

- owner 独立选择新 hypothesis/candidate generator；
- 若走 B，另行批准 reviewed hard-gate policy；
- 结果出现前冻结 candidate universe/selection rule；
- prospective holdout 在新授权前保持未访问，单次访问/到期规则另行登记；
- TRADING-097 event-risk sample floor 与 forward 20d/60d maturity 继续按真实时间积累。

全过程固定 `strategy_logic_changed=false`、`cached_data_mutated=false`、
`promotion_gate_allowed=false`、`paper_shadow_change_allowed=false`、`production_effect=none`、
`broker_action=none`。

## 2026-07-22 完成证据

S0～S4 foundation 已完成。新增 reviewed
`uncontaminated_selection_protocol_foundation_policy.v1`、typed
`UncontaminatedSelectionProtocol` 与 pure content-derived validator；valid contract 的
`foundation_status=FOUNDATION_ONLY`、admission=`READY_FOR_OWNER_PROTOCOL_AUTHORING`，但
`execution_unblocked=false`。`NOT_PREREGISTERED` 保留为typed fail-closed状态，不能得到authoring
资格。policy semantic SHA-256=`99a84c35308a0fe7f3294057032c0903c3ea1fe0a8eb348db6e0d3d259168e89`。

Coordinator focused=`28 passed`，其中策略专属矩阵19项、工程专属矩阵9项；Ruff、compile与diff-check
PASS。最终 architecture/contract/reproducibility=`447/265/23 passed`，自然集成Full=
`6604 passed/2 skipped/643 warnings/1179.75s`；provenance、scheduler、telemetry、performance、safety
均PASS，`strategy_logic_changed=false`、`production_effect=none`。本任务的`DONE`只表示未来研究可以先
进入owner protocol authoring，不表示B/C、新策略运行、unbiased OOS、prospective access或任何
promotion/production能力已获授权。
