# TRADING-2544：Conditional Source Value Audit Serial Contract And Feasibility V1

最后更新：2026-08-23

稳定任务 ID：
`TRADING-2544_CONDITIONAL_SOURCE_VALUE_AUDIT_SERIAL_CONTRACT_AND_FEASIBILITY_V1`

Owner 方向记录：
`owner_direction:TRADING-2544:2026-08-23:record_contextual_source_value_audit_v1`

状态：`PROPOSED`

优先级：`P1`

## 1. 方向与目标

项目拟研究：是否能够在严格 point-in-time、out-of-fold 和上下文可观测边界内，估计某个
信息渠道或数据来源对指定 target、horizon 与市场 context 的条件增量参考价值。

本任务把该方向登记为受治理的 serial contract 与 feasibility audit。当前 Owner 指令只授权
记录方向，不授权采集新数据、执行训练或回测、冻结阈值、改变评分/仓位/报告结论、调用外部
付费资源、进入 production 或触发 broker 行为。

首版目标不是从 attention、gate 或 feature weight 直接解释“参考价值”，而是定义并验证可审计的
OOF 增量损失差、tail-risk 信息增量、provider 可靠性与未来 decision utility 的不同证据层。

## 2. 概念边界

### 2.1 Information channel 与 provider 分离

- `information_channel` 表示归一化后的信息语义，例如 price/volume、macro、fundamental、
  valuation、policy/geopolitics 或 news-derived feature group；
- `provider` 表示提供同一或相近信息渠道的具体来源；
- 渠道预测价值不得与 provider 的 freshness、coverage、revision、schema 稳定性、PIT 合规性、
  failure rate 或异常差异混为同一分数；
- provider 替换或等价性判断只允许在相同 normalized feature contract、common support、
  `available_at` 与 DQ/PIT 边界内进行。

### 2.2 四类证据分别报告

1. `predictive_information_relevance`：对指定 target/horizon 的样本外预测损失增量；
2. `tail_risk_information_relevance`：对下行、极端状态或风险条件的增量信息；
3. `provider_equivalence_and_reliability`：同一信息合同下 provider 的可靠性与可替代性；
4. `decision_utility`：纳入交易成本、行动规则与风险约束后的决策效用，只能作为后续独立阶段。

任何一层通过都不得自动推出下一层通过。

## 3. 首选 estimand

首选主估计量为严格 outer-fold OOF、model/coalition-relative 的条件预测损失下降：

```text
V_g(c, t, h) = E[
  loss(y, yhat_without_g_OOF) - loss(y, yhat_full_OOF)
  | context=c, target=t, horizon=h
]
```

其中 `g` 可以是一个 information channel、provider-normalized channel 或预先登记的 coalition。
正值只表示该来源在指定模型族、时间范围、context 与 loss 下提供增量样本外信息；它不是因果效应、
独立 alpha、仓位建议或 production authorization。

为降低冗余和交互导致的误判，评估设计至少考虑 `full/drop/add`、group ablation、pairwise
interaction，并在组合规模与有效样本量允许时使用受控的 approximate Shapley。单一 drop-one
结果不得单独形成来源价值结论。

## 4. 分阶段路径

### S0：Serial contract wave

在任何 empirical result 可见前，先冻结并 review：

- information channel、provider、normalized feature、source lineage 和 checksum identity；
- `event_time`、`as_of`、`available_at`、revision policy、DQ/PIT 与缺失/异常语义；
- target、horizon、context definition，且所有 context 必须在预测时点可观测；
- primary research window 默认从 `2021-02-22` 开始，并在输出中同时报告 requested/evaluated
  ranges；
- purged/embargoed temporal split、outer-fold OOF artifact、overlapping horizon 与依赖结构；
- primary/secondary loss、uncertainty、multiple-comparison、coalition 与 conclusion taxonomy；
- `PASS / FAIL / INSUFFICIENT / INVALID` 的机器可判定边界；
- 所有 sample floor、capacity gate、显著性或 promotion threshold 必须进入 reviewed policy
  manifest，不得在代码或报告中临时决定。

### S1：M0/M1 feasibility

- `M0`：预先登记 context buckets 的简单 OOF 增量估计与 uncertainty；
- `M1`：regularized/hierarchical model，作为首个正式条件价值模型；
- 重新计算 session、label、fold、event cluster 与 effective sample size；
- 若数据覆盖、PIT、common support、fold 稳定性或有效样本量不足，输出 `INSUFFICIENT` 或
  `INVALID`，不得强制排名来源。

### S2：M2 neural challenger

只有 S0 contract 与 S1 pre-outcome capacity gate 通过后，才允许评估小型 gated MLP challenger。
M2 必须与 M0/M1 使用相同 OOF lineage 和预登记 comparison；gate/attention weight 只用于模型内部，
不得充当价值证据。若 M2 通过而 M1 失败，最高只能登记为 replication hypothesis，不得直接升级
为正式结论。

首版不采用 Transformer、RNN 或其他高容量 sequence model；后续如拟引入，必须建立新的
capacity、leakage、overfit 与 reproducibility requirement。

### S3：Future decision utility

只有信息相关性结论可复现后，才另行登记含 transaction cost、turnover、position/risk policy、
action value 与 holdout 的决策效用任务。本任务不实现该阶段。

## 5. 初始可行性观察

2026-08-23 Web Pro advisory review 基于当时 exact commit 的现有清单，提到约 `1,362` 个 session、
`5,412` 个 label 与 `118,300` 条 prediction record。prediction record 高度相关，不能等价为独立
训练样本；overlapping horizon 会进一步降低 effective sample size。

这些数字只用于说明为什么先走 M0/M1 与 capacity gate，不是本任务冻结的输入证据。正式启动时
必须从选定 exact commit 和通过 `aits validate-data` 的数据重新生成、校验并记录 requested/evaluated
date ranges、row counts、checksums 与依赖结构。

Web Pro advisory 的主要结论为：先执行 serial contract wave；以 OOF incremental loss reduction
而非模型权重作为价值证据；M0/M1 先行，small gated MLP 只作为 capacity-gated challenger；最高
结论保持 research-only conditional information relevance。该意见是 advisory，不替代项目 task、
policy、DQ/PIT、validation 或 Owner authority。UI 与回答自报为 Pro 路径，但 exact backend/fallback
route 无法独立验证。

## 6. 验收标准

若未来 Owner 明确启动本任务，至少满足：

1. reviewed S0 contract/policy manifest 固化 estimand、context、target/horizon、split、loss、
   uncertainty、coalition、capacity gate 和四态 conclusion；
2. source/provider/feature lineage、PIT `available_at`、DQ status、requested/evaluated ranges、row count
   与 checksum 可审计；
3. M0/M1 在同一 outer-fold OOF lineage 上完成 full/drop/add/group/pairwise 评估，并报告依赖调整后的
   uncertainty 与 effective sample size；
4. 只有预登记 capacity gate 通过才允许 M2；M2 结果不能以 gate/attention weight 代替消融证据；
5. 输出严格区分 predictive、tail-risk、provider reliability 和 decision utility，不跨层推断；
6. 所有结果限定为 research-only，除非后继任务另行获得 production/broker exact-scope authorization；
7. 更新届时实际受影响的 `docs/system_flow.md`、task 状态、tests、generated authority 与正式验证证据。

## 7. 启动前开放问题

- 首批 target 与 horizon 的精确定义；
- context 是固定 regime taxonomy、连续状态变量，还是两者的预登记组合；
- information channel/provider/coalition 的规范化映射；
- primary loss、tail-risk loss 与 uncertainty/multiple-comparison 方法；
- temporal split、embargo 与 overlapping-label cluster 的 exact 规则；
- capacity gate 与 minimum effective sample size 的 reviewed policy；
- provider cost/freshness/revision 是否只进入 reliability 层，还是建立独立 constrained utility 层。

以上问题未冻结前不得运行正式 neural training 或形成投资解释。

## 8. 当前边界与工作区生命周期

- 当前变更只新增 task registry 记录与本 supporting requirement；
- 当前登记 worktree：`D:\Work\AITradingSystem_trading2544_direction_record`；
- purpose：从 exact local `main` 隔离登记 TRADING-2544，不接触主工作区的其他任务；
- exit condition：聚焦验证、task commit、local-main 受治理集成、ordinary push/SHA 复核完成，且
  tracked/untracked/ignored/进程审计确认无唯一内容后删除该 worktree 与临时分支；
- 本轮不生成研究数据、模型、backtest、score 或 investment report，因此不更新
  `docs/system_flow.md`；未来启动 S0 或任何数据流实现时必须按实际影响同步更新；
- `production_effect=none`、`broker_action=none`、`external_research_run=none`。

## 9. 进度记录

- 2026-08-23：Owner 要求“可以先记录下这个方向”。任务以 `PROPOSED` 登记；该指令不解释为
  启动 serial contract、数据处理、训练、回测、外部资源消耗或生产授权。
- 2026-08-23：登记变更已进入 local/remote `main`，但 closeout 审计发现受
  `known_unrelated_exclusions` 保护的
  `docs/research/growth_tilt_owner_diagnosis_pack.md` 出现在登记 worktree。Codex 未读取、哈希、
  复制、移动、stage 或删除该文件，因此 worktree 与已合并 task branch 暂时保留。任务 tracked
  内容与 Architecture PASS evidence 已进入 canonical main；其余 1,168 个 ignored cache/runtime
  文件分布在 `.pytest_cache`、`outputs`、`scripts`、`src`、`tests`，没有发现依赖进程，也不作为
  唯一实现或研究证据。保留风险限于临时磁盘占用与 worktree/branch residue，
  `production_effect=none`、`broker_action=none`。下一责任方为 Project Owner 或该受保护文件的
  内容所有者：独立决定文件处置；当该 path 已不再存在且无需 Codex 接触其内容时，Codex 可重新
  执行 governed audit，确认无唯一内容/进程依赖，再运行 `git worktree remove`、`git worktree prune`
  并删除已合并 task branch。
