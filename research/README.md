# 研究入口

这里是研究的唯一入口。讨论研究方向、写研究 brief、开新的研究问题，都先读这里的文件，不再翻旧的研究文档。

## 现在的状态（2026-10-15）

- 2026-05 到 09 的约 860 个研究任务已按 2026-10-12 的可靠性复核重新评级，结果见 `ledger.md`。
- 结论：没有一条正面结论达到 A 或 B 级；能当真的只有少数负面结论，见 `negative_results.md`。
- 2026-10-12 研究方向已定（TRADING-2567，`owner_decision:TRADING-2567:2026-10-12:qqq_direction_1999_window`）：QQQ 期权路径，先在 1999 年起的历史上找站得住的方向信号，再用 QuantConnect 回测期权表达；QQQ 类问题以 1999-03-10 起为开发验证窗口，2021-02-22 起只做一致性检查。
- 长历史数据和研究窗口（TRADING-2567 S1、S2）的代码和政策已就位：QQQ 方向问题在 1999-03-10 到 2010-12-31 上开发，2011-01-03 到 2021-02-19 每个问题预注册后只读一次，2021-02-22 起只做一致性检查（`config/research/primary_research_window_policy.yaml` 的 `question_windows`）。第一份数据快照已在主仓库下载（2026-10-13），TRADING-2567-03 修正后重跑检查再用，见 `assets.md`。
- 第一批题目的顺序（2026-10-13，`owner_decision:TRADING-2567:2026-10-13:risk_first_question_order`）：先管风险、再组合信号。依次是按波动调敞口和杠杆、3–5 个固定符号的经典信号等权组合、期权层卖波动、FOMC 和隔夜。主指标是同等平均敞口下的回撤、左尾和复利分散，超额收益在后。
- 共享评估台（TRADING-2565）分三批建。S3a：模块 `src/ai_trading_system/research_bench/`（C 区）；`python -m ai_trading_system.research_bench benchmarks` 在开发窗口上跑固定基准核对数据。S3b：运行登记、试验计数、折算夏普、验证窗口读取登记和预注册格式（下面第 2–5 步）。S3c：标准证伪（成本、推迟、逐年和逐段剔除、同敞口随机择时、块 bootstrap）、报告第一屏和 pi 研究运行的 brief 模板。pi 可以按 `_template/brief.md` 跑研究运行：评估台读数据、控制窗口，pi 只看屏幕上的汇总，守卫不让 pi 打开研究数据；预注册和策略模块都在 C 区（TRADING-2566）。
- 第一题 `QQQ-Q1/` 已结束（2026-10-14）：Q1a 证据不足，Q1b 拒绝，见 `QQQ-Q1/decision.md`。按波动降仓有信息，但在 2011–2021 年不足以胜过同平均敞口的固定仓位。
- 第二题 `QQQ-Q2/` 已结束（2026-10-15）：Q2a（四个回看期趋势）证据不足，参照 10 个月均线和 Q2b（信用利差门）拒绝，见 `QQQ-Q2/decision.md`。三条降风险规则在 2011–2021 年都没有胜过同平均敞口的固定仓位；加上第一题，五条降风险规则在开发窗口都有效，在验证窗口都不成立。
- 卖波动代理测量（2026-10-15，TRADING-2567-20、TRADING-2567-21）：只读开发窗口，用 VXN 当波动率的 Black-Scholes 代理给月度平值现金担保卖 put 定价。几何年化比同平均敞口的固定仓位高，但最大回撤和左尾更差（缩放 0.9：回撤 −38.8% 对 −29.4%，CVaR 5% 差 62.2%），定价越保守越差。Baker 选择收掉：不买 ORATS 数据，不起草预注册；评级 C，登记在 `negative_results.md`。这是描述性测量，不是台账里的一条研究线。
- 第三题 `QQQ-Q3/` 已结束（2026-10-15）：Q3（21 日和 252 日趋势信号，分歧时减半）证据不足，参照只看十二个月信号拒绝，见 `QQQ-Q3/decision.md`。它是 Q2 `trend4` 的子集，结果不是独立于 Q2a 的证据；七条降风险规则在 2011–2021 年都没有胜过同平均敞口的固定仓位。
- 查尺子的结果补注到第一到三题（2026-10-18，各题 `decision.md` 和 `ledger.md` 第 23–25 节）：「证据不足」不作负结果读；第二、三题的方向定于上一题的验证读取之后，记为软泄漏。
- 下一道题定为组合层面 P1（2026-10-18，`owner_decision:TRADING-2567:2026-10-18:portfolio_p1`）：QQQ 加合成 10 年期国债按固定比例，和平均敞口相同的 QQQ 加现金比；先按两段尺子试跑（计划 `_trials/T0002.yaml` 随评估台改造一批加入），主配置过门才开题。研究自动循环降为后台，只做筛选。
- 2026-10-18 起结题打失败标签、开题读失败条款，见 `families.md`、`scorecard.md`、`_loop/charter_p1.md` 第 12 节；第一到三题和 T0001 的 10 个失败已回填标签，Baker 同意。
- T0002（股债固定比例，P1）试跑没过门（2026-10-18）：主配置 QQQ 60%、国债 40% 在美股大盘 1962–2010 年的左尾和回撤两条没过，不开题。失败标签「机制不存在（按左尾）；软泄漏可疑」，Baker 2026-10-19 同意，见 `families.md`。
- T0003（bad_vol_share_63，研究自动循环第 1 期第 2 周的候选）试跑没过门（2026-10-19）：美股大盘 1929–2010 年左尾和回撤都比同平均敞口的固定敞口差。失败标签「机制不存在；软泄漏可疑」，Baker 2026-10-19 同意，见 `families.md`。同一天 Baker 去掉研究自动循环的产量上限（每族每期 2 张卡、每周 4–8 张、每期新信号数），改成定期复盘，见 `_loop/charter_p1.md` 第 14 节。
- 第三批试跑 T0004–T0006（研究自动循环第 1 期，2026-10-19，试跑 trial-r0023..r0046）：T0004（`industry_dispersion_21`，行业收益离散度）主配置两段都过，是循环第一次有候选过试跑门；T0005（`defensive_lead_63`）、T0006（`absorption_shift_63`）没过，失败标签「机制不存在（按左尾）」和「机制不存在」，Baker 2026-10-19 同意，见 `families.md`。Baker 同一天选 T0004 开题为第四题 QQQ-Q4（`owner_decision:TRADING-2567:2026-10-19:qqq_q4_prereg`）：离散度规则为主，QQQ 自己的 21 日已实现波动同法作对照，读一次验证窗口，只查方向；预注册和策略模块另作一批（C 区）。
- 第四题 `QQQ-Q4/` 已结束（2026-10-19）：离散度规则（试跑 T0004 转正式题）证据不足，参照 rv21（QQQ 自己的 21 日已实现波动，同法）证据不足，见 `QQQ-Q4/decision.md`。左尾方向对，几何年化每年少 1.6 个点，和第一题 q1a 几乎相同；九条降风险规则在 2011–2021 年都没有以每年少不到 1 个点的代价改善左尾。失败标签都是「有效果但代价太高」，Baker 2026-10-19 同意；「波动」家族关闭，「广度和相关」停车，见 `families.md`。
- 旧的研究代码、文档、配置和测试按 GOV-009（`owner_decision:GOV-009:2026-10-12:research_cleanup`）分批删除。删除前的完整内容保存在 tag `research-archive-2026-10-12`。

## 文件

| 文件 | 内容 |
|---|---|
| `ledger.md` | 26 条研究线的台账：结论、评级、证据、去向、重开条件 |
| `negative_results.md` | 负结果清单：已证伪、未测成、工程上不可执行。研究 brief 必读 |
| `families.md` | 机制家族台账：每个家族试过几个变体、最好结果、家族价、状态、重开条件；失败标签的定义 |
| `scorecard.md` | 记分表：预注册写的预期对结题结果（正式题）、假设卡写的方向对筛选结果（筛选层） |
| `assets.md` | 可以复用的代码、检验和记录，以及它们在哪里 |
| `_template/` | 一个研究问题的样例：`prereg.yaml`（逐项说明）和 `strategy.py`（策略接口）；`brief.md` 是 pi 研究运行的 brief 模板 |
| `_trials/` | 试跑：门 `prereg.yaml`（C 区）、每个候选一份试跑计划（`T0001.yaml` 等）、试跑记录 `runs/`；见 `_trials/README.md` |
| `signal_names.yaml`、`<问题ID>/zh.yaml` | 研究进展页面用的中文名和中文说明（TRADING-2568）：信号的中文名；每题的中文标题、一句话摘要、判定摘要和每个策略的名字与角色。只给页面显示用，不进评估台、不影响任何运行；格式见 `_template/zh.yaml` |
| `QQQ-Q1/` | 第一题（已结束）：结论 `decision.md`；预注册 `prereg.yaml` 和策略模块 `strategy.py`（都在 C 区）；运行记录在 `runs/`，验证窗口的读取记录在 `window_reads.jsonl` |
| `QQQ-Q2/` | 第二题（已结束）：结论 `decision.md`；预注册 `prereg.yaml` 和策略模块 `strategy.py`（都在 C 区）；运行记录在 `runs/`，验证窗口的读取记录在 `window_reads.jsonl` |
| `QQQ-Q3/` | 第三题（已结束）：结论 `decision.md`；预注册 `prereg.yaml` 和策略模块 `strategy.py`（都在 C 区）；运行记录在 `runs/`，验证窗口的读取记录在 `window_reads.jsonl` |
| `QQQ-Q4/` | 第四题（已结束，试跑 T0004 转正式题）：结论 `decision.md`；预注册 `prereg.yaml` 和策略模块 `strategy.py`（都在 C 区）；运行记录在 `runs/`，验证窗口的读取记录在 `window_reads.jsonl` |

研究进展页面（TRADING-2568）由 `python -m ai_trading_system.research_site --out <目录>` 从这里的记录生成：只读，不另存事实。main 上的研究记录更新后，GitHub Actions 的 Research site 自动重建并发布到 GitHub Pages：https://ai-trading-collaboration.github.io/AITradingSystem/

## 结论只用四个词

| 词 | 含义 |
|---|---|
| 保留 | 通过预注册的判定规则，进入前瞻记录 |
| 拒绝 | 可信地证伪，写明适用范围 |
| 证据不足 | 测了，但证据既不能支持也不能否定 |
| 未测成 | 测量本身有问题（常量冒充结论、参数没被读到、样本地板达不到等），不算证伪，可以重开 |

任务卡的 DONE、DROPPED 只表示工程状态，不表示研究结论。研究结论以 `ledger.md` 为准。

## 评级

| 级 | 含义 |
|---|---|
| A 可信 | 看结果前预注册；DQ 通过且 PIT；计成本；有真正的留出样本或足量前瞻样本；试验次数公开；可从记录的 commit 复现 |
| B 有条件可用 | 历史检验做得规范（有预注册、试验可数），但窗口被反复用过或样本小 |
| C 弱 | 事后设计、变体多且没计数、门槛看结果再定、代理或非 PIT 数据、DQ 不明或失败 |
| D 不能当结论 | 只有工程或合同 PASS；数据被污染；后来发现有 bug；或已被推翻 |

## 查看已删除的内容

台账里写成 `@archive:<路径>` 的，是 tag `research-archive-2026-10-12` 下的路径：

```
git show research-archive-2026-10-12:<路径>
git checkout research-archive-2026-10-12 -- <路径>     # 取回到工作区
```

## 以后一个研究问题怎么走

评估台建好后启用，细节以 TRADING-2565 的需求文档为准。命令都是 `python -m ai_trading_system.research_bench ...`，输出只有汇总。

1. Owner 定问题。同时进行的问题数不设上限（TRADING-2567）；假阳性靠评估台跨问题累计试验次数、抬高门槛来控制。
2. 新信号先写进 `src/ai_trading_system/research_screens/signals/`（模块和规格），用 `screen <id> --signal-snapshot <ID> [--bench-snapshot <ID>]` 筛选并进全局账，只有候选才开题；经典信号（均线、动量、实际波动）是基准，不筛。`screen-ledger` 看全家族 BH 修正后的当前裁决。策略是“一个或几个信号按固定规则变成仓位”时，复制 `_template/signal_strategy.py` 为 `strategy.py`，在预注册里写信号和映射，不再写策略代码。候选先在 `_trials/` 试跑（见 `_trials/README.md`）：主配置在 QQQ 和美股大盘的开发窗口都过门，才交 owner 评估是否开题。新信号的中文名可以顺手写进 `research/signal_names.yaml`（页面用，不写也行，页面会回退到规格里的说明）。
3. 把 `_template/` 复制成 `research/<问题ID>/`，写 `prereg.yaml` 和 `strategy.py`（都在 C 区）：问题、问题类型（决定窗口角色）、固定的数据快照、策略和参数、要验证的策略和判定规则、已有研究的预期、样本地板、试验上限、停止条件。基准和成本由评估台固定，不在这里写。再写一份 `research/<问题ID>/zh.yaml`（格式见 `_template/zh.yaml`）：中文标题、一句话摘要、判定摘要和每个策略的中文名与角色，研究进展页面用；它只管显示，和预注册有出入时以预注册为准。
4. 开发窗口：`run <问题ID> --strategy <id> [--set 参数=值] [--falsify]`。策略代码只给出每日目标敞口；执行滞后、成本、指标、基准都由评估台负责。每次运行登记在 `research/<问题ID>/runs/`，失败的也算；不同的配置算新试验，超过上限就拒绝。`--falsify` 同时跑标准证伪，不算新试验；要验证的策略都要先这样跑一次。`ledger` 看试验数和折算夏普，`report <问题ID>` 看第一屏。
5. Owner 确认预注册，记为一个 `Owner-Decision`；`prereg.yaml` 和 `strategy.py` 随一次 ship 进 main，决定写进 `docs/requirements`。
6. 验证窗口：`validate <问题ID>`，每题一次，pi 在单独一次运行里做（`_template/brief.md`）。预注册或策略模块和 main 上的不一致、或判定规则写了不存在的指标，就拒绝。验证和一致性窗口的运行总是跑标准证伪，判定规则可以读证伪结果（如 `ci_low.max_drawdown`）。之后才能 `consistency <问题ID>`。从第二题起，开题批准同时同意读验证窗口（`owner_decision:TRADING-2567:2026-10-14:opening_covers_validation_read`）：开发运行和实现核对都没有问题时，研究线程先告诉 owner，再读，不等回复。
7. 核验：`report <问题ID>` 的第一屏；没碰评估台和基准；试验数没超上限；结论符合预注册规则。
8. 写 `research/<问题ID>/decision.md`，结论用上面四个词，引用登记过的运行号（`<问题ID>-r0001`）和读取号（`<问题ID>-w0001`）。保留的进前瞻记录；拒绝的进 `negative_results.md`（人审后更新）。
