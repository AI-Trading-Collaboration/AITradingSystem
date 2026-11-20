# TRADING-2572 日报仓位满仓为底，闸门只提示（需求）

- 状态：IN_PROGRESS。TRADING-2572-00 登记了决定号和任务卡；-01（C 区）改核心行为和日报正文（第 3 节第 1–8 条、认知状态的新键和第 10 条，验收测试 `tests/test_trading2572_full_exposure_default.py`）；-02（B 区）改其余报告文字（Reader Brief、计算说明、分数变化归因、可追溯引用、证据面板、决策因果链在满仓快照上给最终仓位接按原规则的区间，binding 按原规则的区间找、标「只提示」；可追溯引用和证据面板的总风险资产也接按原规则的预算；指标研究的闸门遮蔽统计和影子权重对比读 gated 区间；共用读法在 `position_policy_view.py`，验收测试 `tests/test_trading2572_report_wording.py`；日报区间成绩单这批不改，见第 5 节第 3 条）；前瞻账本的满仓行和分段另走一批（TRADING-2565-139，B 区：周汇总加满仓 1.0 一行，日报线按持有快照的 `position_policy.mode` 分换包前后，账本的行不改）。新发布包在 2026-11-20 13:00 JST 日报稳定性判定以后、-01 和 -02 都发布以后再部署。
- 性质：改生产日报给出的建议仓位（只是建议，不下单）。分数映射、各道仓位闸门和宏观预算照算照写，只作提示；回测、回填照旧。broker_action=none。

## 1. Owner decision

`owner_decision:TRADING-2572:2026-11-19:full_exposure_default`：Baker 2026-11-19 定目标：「目标是收益最大化」，「最深我希望不超过QQQ同期最大回撤比例」。按这个目标，同期买入持有 QQQ 没有被打败，没测过的压仓规则都是代价。他 17:18 JST 选「落地满仓，选股先估」，要求查清日报为什么多数日子是 0.40 并提改法；17:31:36 JST（08:31:36Z）在决定卡「日报仓位怎么改成以满仓为底？」上选「满仓为底，闸门只提示」，选项原文：「日报默认给 1.0，评分和闸门照印但不压仓；我写需求、pi 写代码，11-20 后换包，账本加满仓线。」

总资产口径另问 Baker：「总资产也满仓 1.0，还是只把风险资产内的 AI 仓位满仓？」卡上推荐「总资产也满仓 1.0」。本需求按推荐写，标【M5】的地方随他的选择改；选后一种则总资产区间照现行规则（风险资产内 1.0 × 总风险资产预算）。Baker 2026-11-19 20:09 JST 选了「总资产也满仓 1.0」。

## 2. 为什么多数日子是 0.40

最终仓位区间取各道闸门上限的最小值（`scoring/position_model.py`）。估值闸门（`config/scoring_rules.yaml` 的 `position_gates.valuation`）在 17 只 AI 核心股任一只自身历史分位达到 90（forward PE、EV/Sales、PEG 三个分位的中位数）时把上限压到 0.40，达到 75 时 0.70。日报成绩单里 80 天有 56 天是估值闸门起作用，78 天最终上限低于模型上限；前瞻账本 10-14..11-11 读到的 13 份快照全是 0.40–0.40。其它闸门：组合上限 0.75、风险预算（VIX）0.70/0.45、数据置信度 0.80/0.60/0、thesis 0.70/0、置信度乘数，总资产预算 0.60–0.80（宏观压力时更低）。这些上限都没有在「回撤不深于 QQQ 同期」这个目标下测过。

## 3. 行为

1. 配置：`config/scoring_rules.yaml` 加 `position_policy.mode`（`full_exposure_default` 或 `gated_min`，后者是改前的行为）和 `owner_decision`，生产写 `full_exposure_default`；别的值加载报错，缺这一块按 `gated_min`。理由表加 `target_path: position_policy`。
2. 只看参数：`build_daily_score_report` 加参数 `position_policy_mode`（缺省 `gated_min`），不从 rules 读；`aits score-daily` 加选项 `--position-policy-mode`，缺省读配置。回测和回填不传，照旧 `gated_min`。
3. 历史回放：回放第 D 天时给 `score-daily` 子进程传第 D 天生产快照的 `positions.position_policy.mode`；快照里没有这一块按 `gated_min`；第 D 天没有快照时用 D 以前最近一份的。
4. 满仓模式：最终风险资产内 AI 区间 1.0–1.0（「满仓（默认）」）；【M5】总风险资产区间和总资产内 AI 区间 1.0–1.0。模型区间、置信度调整后的区间、静态总风险资产区间和宏观预算照写。新键 `gated_risk_asset_ai_band`、`gated_total_asset_ai_band` 记按现行规则算出的区间，两种模式都写，以后不改名、不去掉（前瞻账本以后要靠它补一条「闸门线」）。每道闸门加 `effect`：满仓模式 `info_only`，现行模式 `cap`；`triggered` 算法不变。快照加 `position_policy`（`mode`、`owner_decision`）。`scores_daily.csv` 的 overall 行加 `gated_risk_asset_ai_min/_max` 和 `position_policy_mode`。
5. 数据质量：生产里数据门禁 FAIL 时 `score-daily` 在评分前退出，不改。函数层面 FAIL 时最终仓位照旧 1.0，gated 区间 0。
6. 执行建议：满仓模式只把 `effect` 是 `cap` 的闸门交给执行建议（即没有），不再因闸门禁止加仓或转人工复核；低置信度转人工复核照旧。`config/execution_policy.yaml` 不改。上一期 `position_policy_mode` 和当期不同的那天（换包或换回），执行建议和「本期仓位变化」一行加一句「仓位政策变更（决定号），不是信号。」
7. 预警：满仓模式下触发的闸门照发，标题「{闸门} 触发（只提示，不压仓）」，严重度 info。`position_cap_drop` 两边都用 gated 上限（上一期读 csv 的 `gated_risk_asset_ai_max`，旧行为空时用那行的最终上限），标题「按原规则：AI 仓位上限大幅下降」，严重度照旧 high。
8. 规则卡：`position_gate.shared_caps.v1` 只适用于回测；新加 production 卡 `position_policy.full_exposure_default.v1`（`rule_type: position_gate`，适用 score-daily，回滚是把 mode 改回 `gated_min`）。
9. 认知状态写新键，正文印「满仓 1.0（默认）；按原规则会给 x–y，只提示」。
10. 日报正文（`scoring/daily.py` 渲染，C 区，随 -01 改）：现行模式一个字不变；满仓模式下最终仓位后面接「（满仓为底，闸门只提示；按原规则会给 x–y）」，今日结论卡加一行「按原规则仓位（只提示）」，说闸门压住最终仓位的句子（「中高配但受限」「不得超过已触发闸门」「将最终仓位上限压到」、binding 闸门等）只看还在压仓的闸门（满仓模式下没有），触发的闸门改成「按原规则会把上限压到 x（只提示，不压仓）」这类说法。
11. 其它报告模块（Reader Brief、计算说明等）的最终仓位那行不再写「仓位受限」，另列 gated 区间，由 -02 改。

## 4. 不做

- 不改分数映射、分档、闸门阈值，不删闸门代码；`config/portfolio.yaml`、`config/execution_policy.yaml` 不改。
- 不碰券商、下单、纸面交易。
- 不改前瞻账本已写的行。

## 5. 批次和验收

1. **TRADING-2572-00（记录）**：本需求、任务卡、`roadmap.yaml` 一条。验收：`tools/tasks.py check` 过。
2. **TRADING-2572-01（C 区，pi 写代码）**：第 3 节第 1–8 条、认知状态的新键和第 10 条日报正文。验收测试 `tests/test_trading2572_full_exposure_default.py`（Claude 先写）：回测、回填和现行模式行为不变、只加键；满仓模式最终 1.0、gated 区间等于现行规则的最终区间；数据 FAIL 和 WARNING；【M5】宏观压力下总资产区间；配置缺块和非法值；`score-daily` 读配置、换包那天印一句、连续满仓不印、`--position-policy-mode gated_min` 回到旧行为；日报正文满仓模式印 gated 区间、不出现说闸门压仓的句子，现行模式正文不变；前瞻账本和加的线读得动满仓快照；回放取第 D 天的模式且核心字段对得上；预警；规则卡；两种模式都有 gated 区间。门禁是整套测试。决定记录的 `c_paths`：`config/scoring_rules.yaml`、`src/ai_trading_system/scoring/position_model.py`、`src/ai_trading_system/scoring/daily.py`。
3. **TRADING-2572-02（B 区，pi 写代码）**：其余报告文字和读最终区间的模块（Reader Brief、计算说明、分数变化归因、可追溯引用、证据面板、决策因果链、指标研究的遮蔽统计、影子权重对比）。日报区间成绩单（`research_probes/daily_band_scorecard.py`）这批不改，2026-11-27 续约第 4 项再定；换包以后它的 final 线读到的是政策 1.0。`daily_shadow_vs_production_comparison` 自己用生产权重和闸门上限算主线区间，不读快照的最终区间，不用改。
4. 部署：-01、-02 都发布以后，2026-11-20 13:00 JST 日报稳定性判定以后，照日报发布包现行流程换包。

详细需求（含复盘线程两轮审查）在项目文件 `knowledge/claude/research-review/full-exposure-default-requirements-2026-11-19.md` v1.3.1。

broker_action=none。
