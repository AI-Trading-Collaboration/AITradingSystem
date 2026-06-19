# 权重研究复盘与下一阶段协议

最后更新：2026-06-19

本文覆盖 TRADING-500 到 TRADING-504：失败复盘、权重控制架构 RFC、消融协议、
统计验证 / holdout 策略和下一阶段 2-3 个简单研究方向。本文只定义 research-only
协议，不实现新候选，不批准 paper-shadow，不生成 official target weights，不触发 broker/order
或 production mutation。

## 当前研究结论

TRADING-485 后，最新研究状态是：

```text
V2_RESEARCH_CYCLE_RETURN_TO_BACKLOG
recommended option: revise_hypothesis
```

已知弱点包括：

- backfill partial / mini backfill weak；
- stress weak；
- cost / benchmark weak；
- signal robustness blocked；
- window fragile。

这些是研究证据限制，不能通过文档、registry 或 validation workaround 清掉。

## Failure Taxonomy

| 类别 | 定义 | 当前例子 | 下一步处理 |
|---|---|---|---|
| `data_failure` | 输入缺失、stale、schema 不满足或 point-in-time 不可信 | signal series coverage gap、market coverage gap | 先修数据 / cache / manifest，不调策略 |
| `binding_failure` | spec 到 executable binding 的覆盖或语义不一致 | `v_shaped_recovery` source window missing | 修 binding contract 和 coverage diagnostics |
| `signal_failure` | 信号方向、稳定性或解释力不足 | signal robustness blocked、false risk-off cluster | 回到信号假设和事件 casebook |
| `allocator_failure` | 权重分配机制不能把有效信号转成净收益 | 单一 regime-to-weight 规则承担过多任务 | 拆成 baseline、tilt、overlay、shrinkage |
| `risk_control_failure` | drawdown / stress / recovery 行为不达标 | slow drawdown failure、v-shaped recovery missing | 独立风险 overlay 消融 |
| `execution_cost_failure` | 换手和交易成本吞噬收益 | cost / benchmark weak | 加 execution control baseline |
| `validation_window_failure` | 样本窗口脆弱、代表性不足或被反复使用 | window fragile、under-observed windows | 固定 holdout 和 leave-one-regime-out |
| `overfit_failure` | 参数附近不稳定或只在开发样本有效 | high overfit risk | parameter neighborhood stability / block bootstrap |
| `governance_failure` | gate、owner decision、paper-shadow 边界不清 | 当前未发生，系统已 fail-closed | 保持 gate，不用治理通过替代研究通过 |

## Weight-Control Architecture RFC

下一阶段不再让单一 regime-to-weight 规则同时承担所有任务。研究架构拆成五个组件：

```text
战略基准权重
+ 慢速相对倾斜
+ 快速非对称风险 overlay
+ confidence shrinkage
+ execution / turnover control
```

接口分层：

| 层 | 输入 | 输出 | 禁止事项 |
|---|---|---|---|
| Signal layer | price/market/fundamental/risk features、casebook labels | normalized signal state + confidence | 不直接输出目标权重 |
| Allocator layer | baseline weights、slow signal、confidence | research-only hypothetical weights | 不绕过 cost/risk control |
| Risk-control layer | drawdown/stress/volatility state | overlay multiplier / cap rationale | 不把风险缩放写成研究成功 |
| Execution layer | turnover、cost、rebalance constraints | feasible simulated rebalance path | 不生成 order ticket 或 broker action |

每层必须有独立 input contract、schema version、source artifact refs 和 validation status。

## Ablation Baseline Protocol

消融序列只允许每层增加一个主要假设：

| Baseline | 增量机制 | 必须证明的净增益 |
|---|---|---|
| `B0` | static baseline | 建立 AI regime 默认基准 |
| `B1` | `B0 + execution control` | 在不损害核心收益的情况下减少换手 / cost |
| `B2` | `B0 + fast risk scaler` | stress / drawdown 改善且不过度错失 recovery |
| `B3` | `B0 + slow relative tilt` | 相对基准收益改善且窗口稳定 |
| `B4` | `slow tilt + fast risk scaler` | 慢速 alpha 与快速风险控制可叠加 |
| `B5` | `B4 + confidence shrinkage` | 参数邻域更稳定，弱信号时自动降杠杆 |
| `B6` | `B5 + regime information` | regime 信息提供独立增益，不只是过拟合标签 |

任何一次性加入多个机制的候选不得进入 full backfill admission。

## Statistical Validation And Holdout Policy

默认研究窗口是 `ai_after_chatgpt`，锚点为 2022-11-30，默认起点 2022-12-01。
更早数据只能用于 warm-up、压力测试和 regime comparison，不能作为 AI-cycle 主结论。

验证策略：

- Development windows：允许使用已反复分析的窗口调试机制。
- Diagnostic set：现有 stress casebook 标记为 development / diagnostic，不再作为最终独立验证集。
- Untouched temporal holdout：每个候选进入 full backfill 前固定，不可在失败后重选。
- Purged walk-forward：训练 / 选择窗口与评估窗口之间留出 purge gap。
- Leave-one-regime-out：至少按高波动、慢跌、V 型恢复、横盘高波动等 regime 留一验证。
- Parameter neighborhood stability：候选参数邻域必须保持方向一致，不能只靠单点。
- Block bootstrap：对窗口顺序和局部相关性做稳健性检查。
- Medium / high cost：成本假设至少覆盖 medium 和 high。
- Worst-window penalty：最差窗口不能被平均收益掩盖。
- Stop rules：mini backfill weak、cost/benchmark weak、holdout fail、safety boundary fail 均阻止 full backfill 或 paper-shadow。

## Next Research Program Roadmap

### Candidate 1：B1 Execution-Control Baseline

| 字段 | 内容 |
|---|---|
| Hypothesis | 在 static baseline 上加入 turnover / rebalance control，可降低 cost drag 且不显著损害 AI regime return proxy |
| Expected gain | medium/high cost 下 net improvement proxy 优于 B0；turnover 明显下降 |
| Required input | B0 weights、historical prices、cost policy、rebalance calendar |
| Mini-backfill window | 2022-12-01 到最近完整交易日，另含 2-3 个 stress diagnostic windows |
| Kill criteria | net improvement 不优于 B0、turnover 未下降、任一 data quality gate fail |
| Full-backfill admission | B1 相对 B0 在 medium/high cost 均有独立净增益，且 worst-window 未恶化 |
| Budget | 先实现 protocol/spec 和 mini backfill，禁止 full backfill 自动继续 |
| Research gate | `B1_EXECUTION_CONTROL_MINI_GATE`，失败回到 taxonomy |

### Candidate 2：B2 Fast Asymmetric Risk Scaler

| 字段 | 内容 |
|---|---|
| Hypothesis | 快速风险 overlay 能改善 slow drawdown / high volatility stress，同时在 V 型恢复中保持有限损失 |
| Expected gain | max drawdown proxy 和 worst-window penalty 改善，recovery miss 不超过预设阈值 |
| Required input | risk regime features、drawdown event casebook、flip/rotation event casebook、B0 weights |
| Mini-backfill window | slow_drawdown、high_volatility_sideways_market、v_shaped_recovery diagnostic windows |
| Kill criteria | risk overlay 只靠降低暴露改善 drawdown，或 V 型恢复损失不可接受 |
| Full-backfill admission | 对 B0 有独立 stress 改善，且 cost / turnover 不显著恶化 |
| Budget | 只做 overlay 消融，不同时加入 slow tilt |
| Research gate | `B2_FAST_RISK_SCALER_MINI_GATE` |

### Candidate 3：B3 Slow Relative Tilt With Shrinkage Prep

| 字段 | 内容 |
|---|---|
| Hypothesis | 慢速相对倾斜可以提供基准相对净增益，但必须保留后续 confidence shrinkage 接口 |
| Expected gain | 相对 QQQ/SMH/SOXX/SPY 组合有稳定超额 proxy，window fragility 下降 |
| Required input | relative strength features、AI universe mapping、benchmark baseline control、B0 weights |
| Mini-backfill window | AI regime 全窗口 + leave-one-regime-out mini set |
| Kill criteria | outperformance margin 不足、window fragile 仍主导、parameter neighborhood 不稳定 |
| Full-backfill admission | B3 相对 B0 和 B1 的增益来源可解释，且不依赖单一窗口 |
| Budget | 不加入 fast risk scaler；只预留 shrinkage 字段 |
| Research gate | `B3_SLOW_TILT_MINI_GATE` |

## Safety Boundary

- 所有 weights 都是 research-only hypothetical weights。
- 不允许 paper-shadow activation、extended shadow、live trading、official target weights、
  broker integration、order ticket、production mutation 或 automatic position control。
- Owner review 可以选择下一轮研究方向，但不能把本 RFC 直接视为 implementation approval。
- 任何候选进入 TRADING-505+ 前必须先有 spec、消融协议、数据质量门禁、runtime artifact、
  Reader Brief 和 research gate。

## Open Decisions

- Clean holdout 的具体日期段需要在第一个候选 spec 前冻结。
- Cost assumption policy 应引用已有治理 artifact；如要改阈值，必须走 heuristic governance。
- B0 static baseline 的资产集合和 rebalance calendar 需要 owner 确认。
- 如果 Stage B schema/manifest/error taxonomy 缺口未收口，候选实现只能作为 blocked design，
  不得进入 release-blocking research run。
