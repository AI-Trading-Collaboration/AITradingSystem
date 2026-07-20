# Next Research Program Roadmap

最后更新：2026-07-20

状态：`R2_CONTINUE_EVIDENCE_CLOSURE`

## 2026-07-20 R2 优先级覆盖

TRADING-2446～2448 真实结果优先于下方旧 H1～H3 启动描述：当前不得开启新的 candidate
expansion 或 parameter search。先完成以下证据条件：

1. dynamic-v3 `event_risk_high` 样本由 15 自然积累到 reviewed floor 20；
2. owner 明确 5 个 forward daily archive gap 的治理方式，本批不补造；
3. 20d/60d forward outcomes 按 append-only 路径成熟；
4. 若未来恢复候选比较，另建与 locked holdout 分离的无污染 selection protocol。

当前 R2=`CONTINUE_EVIDENCE_CLOSURE`，walk-forward OOS 已出现负面结果；即使上述 evidence
补齐，也应先重新决策是否 `PAUSE_CANDIDATE_EXPANSION`，不能自动恢复下方 hypothesis。

## 方向

|Hypothesis|研究问题|先验 kill criteria|
|---|---|---|
|H1|B0 + no-trade / turnover control 是否能降低 cost drag？|turnover 未下降；medium/high cost 净效用不优于 B0；数据质量 gate fail。|
|H2|B0 + fast asymmetric risk overlay 是否改善 stress/drawdown？|V 型恢复损失不可接受；false risk-off 增多；stress 仍 WEAK。|
|H3|B0 + slow relative tilt + shrinkage prep 是否有稳定 allocation value？|benchmark margin 不足；window FRAGILE；参数邻域不稳定。|

复杂 regime filter 组合暂缓。只有 B1-B5 的简单模块无法证明增益，或已经证明基础模块仍缺
regime information 增量价值时，才允许研究 B6。

历史状态：B0 control 和 H1/B1 execution/no-trade/turnover-control mini-backfill 已完成；
B1 证据为 mixed。H2/H3 与 B2-B6 仍需 owner/system 复核、独立 runner 和 signal
robustness evidence 后才能继续。
