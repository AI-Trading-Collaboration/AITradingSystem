# 研究台账

- 来源：2026-10-12 研究结论可靠性复核。复核读了 2026-05 到 09 约 860 个研究类任务的任务记录、需求文档、`docs/research` 和代码，关键证据逐条抽查过。
- 结论词和评级的含义见 `README.md`。`@archive:<路径>` 指 tag `research-archive-2026-10-12` 下的路径。
- 研究产物（`outputs/`、`data/raw`）不在 git 里，所以多数数字来自需求文档和任务记录的转述，没有重算。
- 这张表只由人审后更新。以后新的研究问题写在 `research/<问题ID>/decision.md`，并在这里加一行。

## 总表

| # | 研究线（任务，时间） | 结论 | 评级 | 去向 |
|---|---|---|---|---|
| 1 | 日报评分与仓位规则（SCORE、PROD-005，5/1 起） | 未测成 | D | 生产在用；第一个研究问题的候选 |
| 2 | 估值闸门决定最终仓位（VALUATION、SCORE-006） | 未测成 | D | 生产在用；并入第 1 行的问题 |
| 3 | LLM 风险评估进入评分（RISK-013，5/12） | 未测成 | D | 生产在用 |
| 4 | 权重与闸门 shadow 搜索（CALIBRATION-010~017，5 月） | 证据不足 | D（不晋级这一点可信） | 关闭 |
| 5 | ETF 权重搜索与 dynamic v3（TRADING-064~504，6/1–17） | 未测成 | 正面 D | 代码已于 2026-10-10 删除 |
| 6 | B0–B4 与 B2 风险叠加（505~660，6/19） | 证据不足 | C | 关闭 |
| 7 | 指标到信号、value surface、controlled batch（665~809，6/20–21） | 未测成 | C / D | 代码删除 |
| 8 | 尾部风险回退（810~864） | 未测成 | 正面 D；发现标签泄漏 A | 代码删除；泄漏案例做成评估台回归测试 |
| 9 | 简单基线与 equal-risk（865~932、1024~1048、2561） | 证据不足 | 选择 C；前瞻 D | 简单基线模拟器留作评估台核心；equal-risk 改作基准 |
| 10 | growth tilt 与 balanced core（1049~1118，6/26） | 拒绝 | D；"收益来自加 beta"多线一致 C | 代码删除 |
| 11 | Layer-1/2 meta policy（957~1023） | 未测成 | C | 代码删除；暂停 |
| 12 | first-layer v1/v2 与两条 lane（1547~1885） | 拒绝 | 负面 B-；正面 D | 代码删除 |
| 13 | 指标族消融、再消融（1946~2245） | 未测成 | D | 代码删除 |
| 14 | 闸门审计、候选生成器、风险上限（2270~2363） | 证据不足 | C / D | 代码删除 |
| 15 | dynamic strategy 优化循环与 growth-tilt 引擎（2364~2438N，7/5–7/10） | 证据不足 | 正面 D；负面 C | 代码删除 |
| 16 | 研究重启与 clean selection（2446~2458，7/20–26） | 未测成 | R1 负面 C；"家族无价值" D | 代码删除 |
| 17 | 决策目标能力审计（2460/2461，7/26） | 拒绝 | B | 代码删除 |
| 18 | 尾部能力、O1、盲重入（2462~2467） | 未测成 | C / D | 代码删除；2027-02 的一次性重看取消 |
| 19 | QQQ 期权与 QuantConnect（2478~2554，8 月） | 未测成；部分不可执行 | D | 代码删除；2026-10-12 按 TRADING-2567 重开 |
| 20 | growth action value（2540~2542I） | 未测成 | D | 代码删除 |
| 21 | Composer v2（2550、2555~2559，9/1–3） | 拒绝 | B | 代码删除；不得做参数救援 |
| 22 | Composer 与 equal-risk 前瞻（2560、2563） | 证据不足 | 事实 A | 由评估台的前瞻记录接替 |
| 23 | QQQ-Q1 按已实现波动调敞口和杠杆（TRADING-2567，2026-10-13..14） | Q1a 证据不足；Q1b 拒绝 | A | 关闭；见 `QQQ-Q1/decision.md` |
| 24 | QQQ-Q2 经典趋势规则与信用利差门（TRADING-2567，2026-10-15） | Q2a 证据不足；Q2b 拒绝 | A | 关闭；见 `QQQ-Q2/decision.md` |
| 25 | QQQ-Q3 快慢趋势信号分歧时减半（TRADING-2567，2026-10-15） | Q3 证据不足；参照 slow_only 拒绝 | A | 关闭；见 `QQQ-Q3/decision.md` |
| 26 | QQQ-Q4 行业收益离散度高时降敞口（TRADING-2567，2026-10-19） | Q4 证据不足；参照 rv21 证据不足 | A | 关闭；见 `QQQ-Q4/decision.md` |
| 27 | QQQ-Q5 快慢趋势信号分歧时减半，每周调仓（TRADING-2567，2026-10-21） | Q5 证据不足 | B | 关闭；见 `QQQ-Q5/decision.md` |

## 各线

### 1. 日报评分与仓位规则

- 记录的结论：production / BASELINE_APPROVED。
- 为什么是"未测成"：
  - 权重、仓位区间和闸门阈值是 2026-05-01 首次提交时定的先验规则，`config/scoring_rules.yaml` 开头写明"不代表已完成收益校准"；
  - 没有在主窗口上和买入持有、静态配置对比过；`config/rule_cards.yaml` 的"验证"是 `pytest -q`；PROD-005 是 9/24 按默认值批量批准的；
  - 运行副本 5/4–9/4 的 69 个有日报的交易日里，最终 AI 仓位只出现过 40% 和 60%，评分在 54–85 之间变动，仓位基本没跟着动；
  - `decision_outcomes.csv` 停在 5/15；唯一一次对照（5/4，1 个月样本）是固定 60% 跑赢动态模型。
- 证据：`config/scoring_rules.yaml:1-20,43-73,287-296`；`config/portfolio.yaml:12-53`；`config/rule_cards.yaml:19-32`；`docs/requirements/decision_support_improvements_2026-05-04.md:318-321`。
- 重开条件：预注册后，用运行副本的决策快照（73 个，2026-04-30..10-09）对比 100% QQQ、静态 40%、静态 60%。期间模型代码一直在改，只能算半前瞻。

### 2. 估值闸门决定最终仓位

- 记录的结论：连续数月 40%。
- 为什么是"未测成"：0.40/0.70 两个阈值没有校准记录；估值分位输入是 FMP 回填，PIT 可信度低；5/8 曾因读到未来 thesis 把仓位算成 0%。
- 证据：`config/scoring_rules.yaml`、`config/portfolio.yaml`；任务 VALUATION-001/002、SCORE-006。
- 重开条件：作为第 1 行问题的分解项。

### 3. LLM 风险评估进入评分

- 记录的结论：当作可信结论使用。
- 为什么是"未测成"：置信度从 55% 提到 65%，刚好越过 0.60 的低置信线；没有事件标签，也没有人工复核；风险事件闸门从未触发。
- 证据：`docs/requirements/risk_event_llm_formal_assessment_2026-05-10.md:43-49`；任务 RISK-013。
- 重开条件：有事件标签和人工复核样本之后。

### 4. 权重与闸门 shadow 搜索

- 记录的结论："放松估值闸门提升收益"，NOT_PROMOTABLE。
- 为什么是"证据不足"：在 8 个 1 日观测上累计约 15.5 万次试验，用 SMH 收益当代理。不晋级这一点可信，正面结论不能用。
- 证据：`docs/requirements/shadow_parameter_validation_hardening_2026-05-16.md`、`shadow_parameter_attribution_promotion_contract_2026-05-16.md`；任务 CALIBRATION-010~017。
- 重开条件：不单独重开；"放松估值闸门"并入第 1 行的问题。

### 5. ETF 权重搜索与 dynamic v3

- 记录的结论：`median_plus_regime_mismatch_filter` 判为 PROMISING，再到 FORMAL_RESEARCH_READY；后来整条线退役。
- 为什么是"未测成"：
  - 7 月重审发现建立在合成未来收益、缺失值补 0、全周期指标复制到各窗口上；修正后 0 个事件、INSUFFICIENT_DATA；
  - 退役依据是"七个参数轴效应全为 0"，有可能是评估器根本没读候选参数（见第 16 行）。
- 证据：`@archive:docs/requirements/TRADING-093_to_100_Dynamic_v3_Rescue_Parameter_Research_Platform.md:116,136`；`@archive:docs/requirements/TRADING-102_to_110_Stable_Real_Parameter_Iteration_Backtest_Loop.md:190,202`；`@archive:docs/requirements/TRADING-326_to_335_Signal_Feature_Diagnosis_and_Candidate_Quality_Filter_Pipeline.md:126-129`。代码在 tag `legacy-governance-final` 下（`src/ai_trading_system/etf_portfolio/`）。
- 重开条件：评估台有"每个参数都真的影响输出"的不变量测试后，在长历史上重跑一次。

### 6. B0–B4 与 B2 风险叠加

- 记录的结论：B2 回到设计，fast-risk 角色作废。
- 为什么是"证据不足"：B2 在 1 个月样本里一次都没触发；8 个窗口只有 6 个不同、靠手选；"owner 决定"和 agent 在同一批次写入（推断是代写）。整条线在 6/19 一天内完成。
- 证据：`@archive:docs/research/b2_fast_risk_no_trigger_audit.json`；`@archive:data/research_campaigns/b2-risk-overlay-current-form/owner_decisions.jsonl`；`@archive:docs/requirements/TRADING-654_to_660_B2_Owner_Decision_And_Redesign_Plan.md`。
- 重开条件：有触发样本的窗口（含 2022 年）并预注册。

### 7. 指标到信号、value surface、controlled batch

- 记录的结论：KILL value surface；KILL selector。
- 为什么是"未测成"：代码里实际只评估最近 72 个交易日；"holdout"是在同一批样本上切片；selector 的 KILL 规则是"跑不赢 10 个简单策略里事后最好的那个"。
- 证据：`@archive:src/ai_trading_system/controlled_strategy_batch.py:862-877,7220-7228,10682-10760`；`@archive:docs/requirements/TRADING-775_to_779_Controlled_Strategy_Research_Next_Stage.md`；`@archive:docs/requirements/TRADING-800_to_804_Value_Surface_V2_Cost_Horizon_Holdout.md`。
- 重开条件：在评估台上用全窗口和真正的留出样本重做。

### 8. 尾部风险回退

- 记录的结论：先 CONTINUE（precision=recall=1.0），后 BLOCKED。
- 为什么是"未测成"：触发器直接由实现后的超额收益生成，是标签泄漏；前视违例数在代码里写死为 0。发现泄漏这件事本身是 A 级。
- 证据：`@archive:src/ai_trading_system/controlled_strategy_batch.py:11730-11758`（泄漏）、`:594-598`（写死 0）；`@archive:docs/requirements/TRADING-821_to_825_Tail_Risk_Fallback_Falsification_Audit.md`；`@archive:docs/requirements/TRADING-827_Tail_Risk_Trigger_Label_Independence_Audit.md`。
- 重开条件：评估台的"标签和特征时间分离"测试通过后。泄漏案例（precision=recall=1、FP=FN=0）做成回归测试。

### 9. 简单基线与 equal-risk

- 记录的结论：equal-risk 为防御主候选。
- 为什么是"证据不足"：
  - QQQ 权重被下限 0.10 卡住，实际约等于 10% QQQ + 90% SGOV；
  - 对照不是风险匹配的；
  - 前瞻 2 条、成熟 0 条。
- 证据：`src/ai_trading_system/simple_baseline_portfolio_control.py:1387-1401`；`config/research/simple_baseline_strategy_registry.yaml`（`equal_risk: min_weight 0.10, max_weight 0.90`）；`@archive:docs/requirements/TRADING-865_to_878_Simple_Baseline_Portfolio_Control.md`。
- 去向：`simple_baseline_portfolio_control.py` 的 `_strategy_return_series`、`_metrics_for_strategy` 信号滞后经核实正确，留作评估台核心；equal-risk 改作基准定义（约 10/90），不再是研究线。

### 10. growth tilt 与 balanced core

- 记录的结论：同一天两个相反结论，最后列为 balanced core 候选。
- 为什么是"拒绝"：候选是 238 个里的最大值；报告自己承认有更好的邻居，beta 调整后不显著。"追求收益类变体的收益来自加 beta"在第 12、15 行也一样。
- 证据：`@archive:docs/research/growth_tilt_focused_diagnosis_master_review.md:14-19`；`@archive:docs/requirements/TRADING-1110_to_1118_Growth_Tilt_Focused_Diagnosis_And_Watchlist_Reconsideration.md`。
- 重开条件：新问题先做 beta 调整再谈。

### 11. Layer-1/2 meta policy

- 记录的结论：SELECTOR_FINAL_GATE_FAIL，归档。
- 为什么是"未测成"：窗口没有 2022 年熊市；200DMA 开头 19 行有前视；可能过早关闭。
- 证据：`@archive:src/ai_trading_system/layer1_simple_rule_meta_policy.py:5046-5047`；`@archive:docs/requirements/TRADING-986_to_1000_Layer1_Simple_Rule_MetaPolicy_Research.md`。
- 重开条件：评估台建好后，在含 2022 年的长历史上重跑一次再定。

### 12. first-layer v1/v2 与两条 lane

- 记录的结论：通用 first-layer 被拒；return-seeking 收益来自加 beta。
- 为什么是"拒绝"：v2 的"4/4、8/8 改善"实际从 2023-02-22 起算，补上 2022 年后全部失败。负面结论 B-。
- 证据：`@archive:docs/research/first_layer_v2_effective_coverage_audit.md:22-25`；`@archive:docs/requirements/TRADING-1806_to_1820_First_Layer_V2_Closeout_And_Lane_Separation_Policy.md`。
- 重开条件：不重开。

### 13. 指标族消融、再消融

- 记录的结论：relative_strength 只能诊断、依赖 beta/TQQQ；加风险的特征族为空；拒绝重开。
- 为什么是"未测成"：各族的入选通道、"依赖 beta/TQQQ"等写死在 `FAMILY_PROFILES` 常量里，没有测量；再消融的关键指标是常量 0 或 False。
- 证据：`git show 57b0ba70:src/ai_trading_system/indicator_family_ablation.py`（第 205 行起）；`git show fe3bdcab:src/ai_trading_system/free_feature_family_reablation.py`（347-356 行）；`@archive:docs/requirements/TRADING-1946_to_1975_Indicator_Family_Ablation_Channel_Signal_Evidence.md`。
- 重开条件：在评估台上重新测量。

### 14. 闸门审计、候选生成器、风险上限

- 记录的结论：多个 FORWARD_OBSERVE / CONTINUE。
- 为什么是"证据不足"：看了结果后删闸门（被删的正是防"只在 2023 年后有效"的那道）、收窄范围；"前瞻结果"其实是历史。宽口径风险上限在 44–46% 的交易日触发，收益约 −17pp。
- 证据：`@archive:docs/requirements/TRADING-2274_First_Layer_Performance_Gate_Acceptance_Audit.md`；`@archive:docs/requirements/TRADING-2363_Observe_Only_Owner_Decision_And_Pause_Checkpoint.md`。
- 重开条件：不单独重开。

### 15. dynamic strategy 优化循环与 growth-tilt 引擎

- 记录的结论：8 轮"优化、重测、owner 复核"，没有候选；家族关闭。
- 为什么是"证据不足"：头号候选来自约 358 个配置的网格；回撤更大、分段通过率 0；放宽门槛发生在上一步提交后 28 分钟；之后 PIT 审计发现引擎有 3 个阻断级前视和 as-of 缺口，结果从未重算。
- 证据：`@archive:src/ai_trading_system/dynamic_strategy_calibrated_gate_owner_review_decision.py:57`；`@archive:docs/research/dynamic_strategy_growth_tilt_engine_pit_risk_audit.md`；`@archive:docs/requirements/TRADING-2438N_Growth_Tilt_Candidate_Family_Closure_And_Contract_First_Discovery_Pivot.md`。
- 重开条件：不重开；如果重开，先修三个前视。

### 16. 研究重启与 clean selection

- 记录的结论：R1 无候选；KILL；家族退役。
- 为什么是"未测成"：1,800 次训练评估全被一个本属 smoke 的 0.65 门槛拒掉；300 个候选的约束命中率完全相同；参数注入审计从未重做，不能排除评估器没读候选参数。
- 证据：`@archive:docs/requirements/TRADING-2453_Constraint_Hit_Rejection_Diagnosis_and_Owner_Review.md:57-71`；`@archive:docs/requirements/TRADING-2458_Constraint_Causal_Diagnostic.md:252-258`。
- 重开条件：同第 5 行。

### 17. 决策目标能力审计

- 记录的结论：QQQ−SGOV 等三种相对收益在 1–20 日上都没有可测技能。
- 为什么是"拒绝"（B）：跑前冻结、门槛宽松、7 折 purged walk-forward。只限"三种资产价格特征加线性模型"，不能推广成"收益不可预测"。
- 证据：`@archive:docs/research/decision_target_capability_audit_model_ladder.md:14-22`；`@archive:config/research/decision_target_capability_audit_model_ladder_v1.yaml:282-289`；`@archive:docs/requirements/TRADING-2461_Decision_Target_Capability_Audit_Model_Ladder.md`。
- 重开条件：不重开；换特征集或模型算新问题。

### 18. 尾部能力、O1、盲重入

- 记录的结论：INSUFFICIENT 后关闭；O1 INSUFFICIENT；2027-02 一次性重看。
- 为什么是"未测成"：样本地板几乎等于理论上限（O1 训练地板 100 = 504/5），注定 INSUFFICIENT；O1 的标签就是第 17 行已经失败的那个。
- 证据：`@archive:docs/requirements/TRADING-2462_Tail_Risk_Capability_Robustness_Falsification_Audit.md`；`@archive:docs/requirements/TRADING-2464_O1_Relative_Opportunity_Spread_Capability_Audit.md:286-296`；`@archive:docs/requirements/TRADING-2467_O1_Blind_Calendar_Reentry_Policy_Slot_Freeze.md`。
- 重开条件：样本地板可达性检查（不读结果，只数样本）通过后。

### 19. QQQ 期权与 QuantConnect

- 记录的结论：唯一一次损益回测 +4.48%。
- 为什么是"未测成"：54 个任务里只有 1 个产出过策略损益，且后续任务自己声明不可解释；期权层面的 DQ/PIT 从未通过。平台限制见 `negative_results.md`"工程上不可执行"。
- 证据：`@archive:docs/requirements/TRADING-2552_QQQ_Options_Conditional_Paired_Comparison_Owner_Review_V1.md:64-86`；`@archive:docs/requirements/TRADING-2542I_QQQ_Options_Exact_Signal_And_Implementation_Policy_Draft_V1.md:286-304,425-434`；`@archive:docs/requirements/TRADING-2480_QC_QQQ_Options_Capability_License_Evidence_Spike_V1.md`。
- 重开条件：某个方向信号先在前瞻上站住，且有带 bid/ask 的期权数据。
- 2026-10-12 重开（TRADING-2567，`owner_decision:TRADING-2567:2026-10-12:qqq_direction_1999_window`）：方向信号先在 1999 年起的历史上通过预注册检验，再用 QuantConnect 分钟报价（带 bid/ask，2012 年起）回测期权表达；对照组和估计量在运行前冻结。
- 2026-10-15 期权代理测量（TRADING-2567-20 加代码，TRADING-2567-21 在开发窗口运行；不是预注册的题，没有读验证窗口，没有用真实期权价格）：卖 put 的几何年化比同平均敞口的固定仓位高，但回撤和左尾更差，定价越保守越差（读法落在事先写下的“溢价在、尾部也在，交 Baker 判断”）。Baker 在决定卡上选“收掉，不花钱”：不买 ORATS，不为卖波动起草预注册。登记在 `negative_results.md`（评级 C）。本节的结论和评级不变。

### 20. growth action value

- 记录的结论：没有经验结果。
- 为什么是"未测成"：派发前审查发现 `tqqq_veto` 是常量 true，增长状态永远不会激活。这个发现本身有用。
- 证据：`@archive:docs/requirements/TRADING-2542E_Growth_Action_Value_Real_DQ_And_Locked_Backtest_Review_V1.md:55-62,241-246,340-346`。
- 重开条件：随第 19 行。

### 21. Composer v2

- 记录的结论：先 RETAIN（+13.75pp），后 INSUFFICIENT/HOLD。
- 为什么是"拒绝"（B）：优势集中在 2022 年和 2023 年上半年一段；去掉 2022 年变为 −5.05pp；与随机择时无法区分（p=0.20）；晚 2 天执行就变负；成本 15–20 bps 处盈亏平衡。
- 证据：`@archive:config/research/first_layer_composer_v2_foundational_falsification_failure_fix_result_admission_v1.yaml:75-118`；`@archive:docs/requirements/TRADING-2555_First_Layer_Composer_V2_Foundational_Falsification_Contract_V1.md:84-97`；`@archive:docs/requirements/TRADING-2559_First_Layer_Composer_V2_Temporal_Influence_Falsification_V1.md:167-180`。
- 重开条件：不重开，不得做参数救援。

### 22. Composer 与 equal-risk 前瞻

- 记录的事实：Composer 观测 0 条；equal-risk 2 条（6/22、6/24）、成熟 0 条。采集全靠逐次手工授权，没接进调度；9/5–10/8 日报停了 23 个交易日，研究侧没有察觉。
- 证据：`@archive:config/research/first_layer_composer_v2_prospective_oos_preregistration_v1.yaml:27-28,75`；`@archive:docs/requirements/TRADING-2563_Equal_Risk_Catchup_V1.md:87-92`；`@archive:docs/requirements/TRADING-2560_Composer_Known_Snapshot_Prospective_Capture_V1.md`。
- 去向：由评估台的前瞻记录接替（日常调度里自动写只增记录）。手工采集那套代码删除，可复用部分见 `assets.md`。

### 23. QQQ-Q1 按已实现波动调敞口和杠杆

- 结论：Q1a（不加杠杆）证据不足，Q1b（最高 3 倍）拒绝。这是评估台上第一道预注册、只读一次验证窗口的题。
- 证据：`QQQ-Q1/decision.md`；运行 `QQQ-Q1-r0001`..`r0012`；读取 `QQQ-Q1-w0001`。
- 去向：关闭。重开条件见 decision.md。和第 18 行（波动聚集预测左尾，未测成）的关系：Q1a 说明按波动降仓对左尾有信息（随机择时排名 0.995），但幅度不足以胜过同敞口的固定仓位；第 18 行的问题是预测左尾本身，结论不变。
- 补注（2026-10-18）：见 `QQQ-Q1/decision.md` 的补注；「证据不足」不作负结果读（查尺子测得的检出率 19.4%）。

### 24. QQQ-Q2 经典趋势规则与信用利差门

- 结论：Q2a（1、3、6、12 个月四个回看期的趋势规则 trend4）证据不足；同一族的参照规则 ma10（10 个月均线）拒绝，它只作参照，不改变 trend4 的结论；Q2b（信用利差门 credit_gate）拒绝。这是评估台上第二道预注册、只读一次验证窗口的题，也是信号筛选层（TRADING-2565 S4）筛出的第一个候选的检验。
- 证据：`QQQ-Q2/decision.md`；运行 `QQQ-Q2-r0001`..`r0013`；读取 `QQQ-Q2-w0001`；筛选记录 S0002（候选）和 S0001（淘汰）。
- 去向：关闭。重开条件见 decision.md。和第 23 行的关系：Q1 和 Q2 合起来，五条降风险规则（Q1a、Q1b、trend4、ma10、credit_gate）在开发窗口都有效，在验证窗口 2011–2021 年没有一条胜过同平均敞口的固定仓位。推断：这个窗口的下跌少而短，只靠降仓的规则吃亏；这是对窗口和这类规则的看法，不是已证明的结论。
- 筛选里被淘汰的 S0001（`credit_spread_change_63`）见 `negative_results.md`；S0003、S0004 证据不足，没有开题。
- 补注（2026-10-18）：见 `QQQ-Q2/decision.md` 的补注；「证据不足」不作负结果读（查尺子测得的检出率 19.4%）；方向定于第一题的验证读取之后，记为软泄漏。

### 25. QQQ-Q3 快慢趋势信号分歧时减半

- 结论：Q3（21 日和 252 日趋势符号，一正一负半仓，fast_slow）证据不足；同一族的参照规则 slow_only（只看十二个月信号，全进或全出）拒绝，它只作参照，不改变 fast_slow 的结论。这是评估台上第三道预注册、只读一次验证窗口的题；fast_slow 是 Q2 trend4 的子集，两题的证据不独立。
- 证据：`QQQ-Q3/decision.md`；运行 `QQQ-Q3-r0001`..`r0009`；读取 `QQQ-Q3-w0001`。
- 去向：关闭。重开条件见 decision.md。和第 24 行的关系：七条降风险规则（Q1a、Q1b、trend4、ma10、credit_gate、fast_slow、slow_only，其中 ma10 和 slow_only 是参照）在验证窗口 2011–2021 年没有一条胜过同平均敞口的固定仓位。fast_slow 离门槛最近（左尾改善 4.4%、回撤浅 5.3 个点，区间都含 0，几何年化每年少 1.8 个点）。推断：效果小；这是对这个窗口和这类规则的看法，不是已证明的结论。
- 补注（2026-10-18）：见 `QQQ-Q3/decision.md` 的补注；「证据不足」不作负结果读（查尺子测得的检出率 19.4%）；方向定于第二题的验证读取之后，记为软泄漏。

### 26. QQQ-Q4 行业收益离散度高时降敞口

- 结论：Q4（49 个行业日收益截面离散度的 21 日均值，按扩张百分位降敞口，dispersion）证据不足；参照规则 rv21（同样的映射用在 QQQ 自己的 21 日已实现波动上）证据不足，它只作参照，不改变 dispersion 的结论。这是研究自动循环第一个过试跑门、转成正式题的候选（试跑 T0004），评估台上第四道只读一次验证窗口的题，只查方向。
- 证据：`QQQ-Q4/decision.md`；运行 `QQQ-Q4-r0001`..`r0007`；读取 `QQQ-Q4-w0001`；试跑 `_trials/T0004.yaml`（trial-r0023..r0030）。
- 去向：关闭。重开条件见 decision.md。和第 23–25 行的关系：九条降风险规则（Q1a、Q1b、trend4、ma10、credit_gate、fast_slow、slow_only、dispersion、rv21，其中 ma10、slow_only 和 rv21 是参照）在验证窗口 2011–2021 年没有一条以每年少不到 1 个点的代价改善左尾。dispersion 左尾改善 7.6%、回撤浅 4.0 个点、几何年化每年少 1.6 个点，和 Q1a 几乎相同。推断：在这个窗口里，离散度规则表现得像换了名字的波动择时；这是对这个窗口和这类规则的看法，不是已证明的结论。
- 补注（2026-10-19）：失败标签 dispersion、rv21 都是「有效果但代价太高」，Baker 同意；「波动」家族关闭，「广度和相关」停车，见 `families.md`。

### 27. QQQ-Q5 快慢趋势信号分歧时减半，每周调仓

- 结论：Q5（QQQ-Q3 的 fast_slow 规则不变，改成每周最后一个交易日调仓，fast_slow_weekly）证据不足，评级 B（方向在读过 `QQQ-Q3-w0001` 之后提出，按预注册降一档）。这是研究自动循环第 2 期第一批卡 H14，没有试跑计划，评估台上第五道只读一次验证窗口的题，只查方向；月频 fast_slow 用已发布的 `QQQ-Q3-r0008` 作参照，不重跑。
- 证据：`QQQ-Q5/decision.md`；运行 `QQQ-Q5-r0001`、`r0002`；读取 `QQQ-Q5-w0001`；开题依据是回填检验 TRADING-2567-33（美股大盘 1929–2010，开发窗口探针）和 QQQ-Q3 的开发敏感性变体。
- 去向：关闭。重开条件见 decision.md。和第 23–26 行的关系：十条降风险规则（Q1a、Q1b、trend4、ma10、credit_gate、fast_slow、slow_only、dispersion、rv21、fast_slow_weekly，其中 ma10、slow_only 和 rv21 是参照）在验证窗口 2011–2021 年没有一条以每年少不到 1 个点的代价改善左尾。fast_slow_weekly 左尾改善 8.0%（月频 4.4%）、回撤浅 3.7 个点、几何年化每年少 2.9 个点（月频 1.8）。推断：更快的调仓把慢信号带到了更多急跌日，但在长牛市里多出来的减仓多是中性的来回，付不起代价；这是对这个窗口和这类规则的看法，不是已证明的结论。按预注册写的失败之后，经典趋势的实现层到此为止，第 2 期剩 P1。
- 补注（2026-10-21）：失败标签「有效果但代价太高」，另加「软泄漏可疑」，Baker 同意；经典趋势家族保持关闭（家族价 2.8%/点），见 `families.md`。
