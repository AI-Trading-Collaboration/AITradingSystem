# 当前研究策略与定期优化复核说明

最后更新：2026-07-11  
项目级 AI market regime：`ai_after_chatgpt`，起点 `2022-12-01`  
当前 QQQ/SGOV/TQQQ primary validated research window：`2021-02-22` 至最近完整且通过数据质量门禁的美股交易日  
文档性质：研究治理与现状说明，不构成投资建议、策略晋升或交易授权

## 执行结论

系统需要定期根据新增结果复核研究策略，但不应按固定周期自动改参数、换模型或修改权重。

推荐机制是“固定周期观察 + 证据触发调整”：

- daily 只负责数据刷新、`aits validate-data`、评分和 evidence artifacts，不做策略寻优；
- weekly 复核回测、robustness、parameter replay/candidates、weight candidate、promotion gate 和 research governance；
- biweekly 复核 feedback loop、shadow lane、投资 thesis 与风险事件；
- monthly 复核长窗口回测、PIT/data-source coverage、report registry 和文档契约；
- 只有发现预先定义的退化、样本成熟、数据/契约修复或新独立证据时，才启动 ad hoc research；
- 任何候选变更必须先冻结假设、指标、阈值、窗口和成本口径，再运行 PIT/holdout 验证并由 owner 审批。

因此，“定期”约束的是证据检查频率，不是强制修改频率。没有足够新证据时，正确动作可以是保持基准、关闭候选族或继续观察。

## 为什么采用这一研究思路

### 1. 区分项目级 market regime 与策略研究 primary window

项目以 ChatGPT 于 2022-11-30 公开发布为锚点，`ai_after_chatgpt` market regime 从 2022-12-01 开始；该日期仍用于 AI 周期归因和 regime-specific comparison，但已经不是当前 QQQ/SGOV/TQQQ 策略研究的默认 primary window。

TRADING-1646～1665 的 window extension 研究发现，单独使用 2022-12-01 之后的数据过短，并可能高估 2023 年以后 AI/科技趋势的有效性。因此后续 first-layer、second-layer、actual-path、主 leaderboard 和 owner-review research 已正式采用：

- `2021-02-22`：`exact_three_asset_validated`，当前 primary validated window；
- `2022-12-01`：`legacy_research_window_2022_12`，仅作 legacy/AI-cycle comparison，不得作为新的主 leaderboard 或 owner decision primary evidence；
- `2020-05-28`：`exact_three_asset_primary_only_extension`，仅作带 SGOV secondary-source gap caveat 的 sensitivity；
- `2020-05-26`：SGOV inception requested range metadata，实际组合起点仍为 2020-05-28，不得计算此前组合收益。

因此当前研究解释应采用“2021 primary first、2022 AI/legacy comparison、2020 sensitivity caveated”，并在报告中同时披露 market regime、research window id 和实际请求日期范围。

### 2. 单一 regime-to-weight 规则承担过多职责，难以解释失败

当前研究架构拆为“战略基准权重 + 慢速相对倾斜 + 快速非对称风险 overlay + confidence shrinkage + execution/turnover control”。Signal、allocator、risk-control 和 execution 分层后，才能区分失败来自数据、信号、权重映射、风险控制、交易成本还是验证窗口，而不是看到净收益不佳就直接调参数。

### 3. 优化应证明增量价值，而不是只找到更好的历史曲线

研究采用 B0～B6 逐层消融：static baseline、execution control、fast risk scaler、slow relative tilt、交互、confidence shrinkage、regime incremental value。每一步只增加一个主要假设，并要求相对上一层证明独立净增益。验证还应覆盖 untouched holdout、purged walk-forward、leave-one-regime-out、参数邻域、block bootstrap、medium/high cost 和 worst-window。

### 4. 结果必须区分“策略失败”和“尚不可执行/不可验证”

缺 PIT lineage、baseline consumption、hard-veto aggregate、exposure unit、预注册阈值或有效样本时，结果应为 `BLOCKED`，不能把 null metric 写成 `FAIL`，也不能用治理通过替代投资有效性通过。这一原则可以避免为了推进流程而补造候选行为或事后选择阈值。

## 当前结果如何

### 历史权重研究主线

`Weight Research Program V1` 当前为 `WEIGHT_RESEARCH_PROGRAM_NEEDS_MORE_EVIDENCE`：B0 是 control-only baseline；B1 attribution 为 mixed；B2、B3、B4 已完成 research-only mini-backfill，但 B4 interaction 为 `INCONCLUSIVE`；B5/B6 blocked；untouched holdout 尚未被使用，不能宣称形成 production candidate。

后续 two-lane 研究显示：defensive lane 没有 material improvement；return-seeking diagnostic 的 7/7 probes 虽有正收益差，但 7/7 同时出现 drawdown regression，并依赖 TQQQ/beta 与 2023+ AI trend。因此最终状态是 `DIAGNOSTIC_ONLY_INSUFFICIENT_EVIDENCE_PROMOTION_BLOCKED`。

### 2026-07-10 最新 growth-tilt 研究

最新候选族结论为 `GROWTH_TILT_CANDIDATE_FAMILY_CLOSED_NO_EXECUTABLE_PIT_CANDIDATE`：

- approved candidate：0；M2 eligible candidate：0；实际 PIT candidate tested：0；
- real replay 未运行，runtime metrics 未生成；
- baseline adapters：0 ready / 4 blocked；
- replacement candidate prerequisites：2 PASS / 8 BLOCKED；
- candidate disposition：两个 `REDEFINE`、一个 `WITHDRAW`、一个 `KEEP_REDEFINED_BLOCKED`；
- promotion、paper-shadow、production 均为 false，broker action 为 `none`。

这一结果不是“候选回测亏损”，而是“候选相对 baseline 的可执行契约尚不存在或不完整”。关闭旧 candidate family 是合理止损：不为挽救候选而创造 baseline behavior，不从概念标签推断 soft confirmation，不把配置声明顺序当作 performance rank，也不在看到结果后补阈值。

下一研究路径是 `TRADING-2438N2_GROWTH_TILT_BASELINE_CAPABILITY_GRAPH`：先只读盘点真实 baseline capability。只有 capability 同时具备可调用实现、PIT lineage、消费路径、语义、依赖和治理状态，才允许生成 typed mutation candidate；如果 mutation-ready count 为 0，零候选也是有效结论。

## 建议的定期优化决策框架

| Cadence | 复核内容 | 允许动作 | 禁止动作 |
|---|---|---|---|
| Daily | 数据质量、freshness、artifact completeness、异常漂移 | 修数据与证据链；记录异常 | 自动调参、自动晋升 |
| Weekly | AI-regime backtest、robustness、成本、回撤、参数/权重候选 | 标记 keep / investigate / retire / preregister research | 根据单周收益改权重 |
| Biweekly | feedback loop、shadow vs baseline、thesis、风险事件 | 归因退化；决定是否开启受控研究 | 把 shadow 短样本当生产证据 |
| Monthly | 长窗口/PIT coverage、数据源、阈值与报告治理 | 冻结新研究问题或关闭无效方向 | 无新证据也强制换模型 |
| Event-driven | 数据契约修复、样本成熟、结构突变、持续越界 | owner-approved ad hoc PIT/holdout 验证 | 事后改阈值、复用 holdout 调参 |

启动策略调整至少应满足以下条件之一：

1. 连续多个独立观察窗口出现同方向退化，且归因不是数据质量或成本口径问题；
2. 原先 blocked 的 PIT、baseline contract、样本量或数据源证据已真正补齐；
3. 新市场结构使原假设失效，并有明确、可证伪的新假设；
4. 既有候选在预注册 gate 下通过 untouched holdout、稳健性、成本和风险检查。

策略不得因单次收益落后、单一窗口改善、参数单点最优、短期 shadow 正收益或 owner 直觉而自动调整。

## 当前建议

当前不应继续对旧 growth-tilt candidate family 做参数优化，也不应进入真实 PIT replay、promotion 或 paper-shadow。应先完成 baseline capability graph，确认系统实际拥有哪些 mutation-ready 能力；随后只针对 READY capability 生成结构正交、契约完整的新候选。

同时，应保持现有 weekly/biweekly/monthly review cadence，但把每次复核结果固定输出为：数据质量状态、market regime 与实际日期范围、baseline/candidate 对比、成本与回撤、最差窗口、证据成熟度、blocker、决策（keep/investigate/retire/research）和下一次触发条件。这样才能让“为什么这么做、结果如何、为何调整或不调整”持续可审计。

## 依据

- `config/scheduled_tasks.yaml`
- `config/research/primary_research_window_policy.yaml`
- `config/research/research_window_registry.yaml`
- `docs/operations/operations_runbook.md`
- `docs/research/research_window_extension_adoption_closeout.md`
- `docs/research/post_window_extension_research_discipline_closeout.md`
- `docs/research/weight_research_turn_2026-06-19.md`
- `docs/research/weight_research_program_v1_snapshot.md`
- `docs/research/two_lane_optimization_master_closeout.md`
- `docs/research/growth_tilt_candidate_family_closure.md`
- `outputs/research_strategies/growth_tilt_candidate_family_closure/growth_tilt_candidate_family_closure.json`
