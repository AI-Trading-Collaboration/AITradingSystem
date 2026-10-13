# 研究入口

这里是研究的唯一入口。讨论研究方向、写研究 brief、开新的研究问题，都先读这里的文件，不再翻旧的研究文档。

## 现在的状态（2026-10-13）

- 2026-05 到 09 的约 860 个研究任务已按 2026-10-12 的可靠性复核重新评级，结果见 `ledger.md`。
- 结论：没有一条正面结论达到 A 或 B 级；能当真的只有少数负面结论，见 `negative_results.md`。
- 2026-10-12 研究方向已定（TRADING-2567，`owner_decision:TRADING-2567:2026-10-12:qqq_direction_1999_window`）：QQQ 期权路径，先在 1999 年起的历史上找站得住的方向信号，再用 QuantConnect 回测期权表达；QQQ 类问题以 1999-03-10 起为开发验证窗口，2021-02-22 起只做一致性检查。
- 长历史数据和研究窗口（TRADING-2567 S1、S2）的代码和政策已就位：QQQ 方向问题在 1999-03-10 到 2010-12-31 上开发，2011-01-03 到 2021-02-19 每个问题预注册后只读一次，2021-02-22 起只做一致性检查（`config/research/primary_research_window_policy.yaml` 的 `question_windows`）。第一份数据快照已在主仓库下载（2026-10-13），TRADING-2567-03 修正后重跑检查再用，见 `assets.md`。
- 第一批题目的顺序（2026-10-13，`owner_decision:TRADING-2567:2026-10-13:risk_first_question_order`）：先管风险、再组合信号。依次是按波动调敞口和杠杆、3–5 个固定符号的经典信号等权组合、期权层卖波动、FOMC 和隔夜。主指标是同等平均敞口下的回撤、左尾和复利分散，超额收益在后。
- 共享评估台（TRADING-2565）分三批建。S3a：模块 `src/ai_trading_system/research_bench/`（C 区）；`python -m ai_trading_system.research_bench benchmarks` 在开发窗口上跑固定基准核对数据。S3b：运行登记、试验计数、折算夏普、验证窗口读取登记和预注册格式（下面第 2–5 步）。S3c：标准证伪（成本、推迟、逐年和逐段剔除、同敞口随机择时、块 bootstrap）、报告第一屏和 pi 研究运行的 brief 模板。pi 可以按 `_template/brief.md` 跑研究运行：评估台读数据、控制窗口，pi 只看屏幕上的汇总，守卫不让 pi 打开研究数据；预注册和策略模块都在 C 区（TRADING-2566）。
- 第一题 `QQQ-Q1/` 已开题（2026-10-13，`owner_decision:TRADING-2567:2026-10-13:qqq_q1_prereg`）：按已实现波动调 Nasdaq-100 敞口，Q1a 不加杠杆，Q1b 最高 3 倍，和平均敞口相同的固定敞口比回撤、左尾和复利分散。下一步跑开发窗口。
- 旧的研究代码、文档、配置和测试按 GOV-009（`owner_decision:GOV-009:2026-10-12:research_cleanup`）分批删除。删除前的完整内容保存在 tag `research-archive-2026-10-12`。

## 文件

| 文件 | 内容 |
|---|---|
| `ledger.md` | 22 条研究线的台账：结论、评级、证据、去向、重开条件 |
| `negative_results.md` | 负结果清单：已证伪、未测成、工程上不可执行。研究 brief 必读 |
| `assets.md` | 可以复用的代码、检验和记录，以及它们在哪里 |
| `_template/` | 一个研究问题的样例：`prereg.yaml`（逐项说明）和 `strategy.py`（策略接口）；`brief.md` 是 pi 研究运行的 brief 模板 |
| `QQQ-Q1/` | 第一题：预注册 `prereg.yaml` 和策略模块 `strategy.py`（都在 C 区）；运行记录在 `runs/`，验证窗口的读取记录在 `window_reads.jsonl` |

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
2. 把 `_template/` 复制成 `research/<问题ID>/`，写 `prereg.yaml` 和 `strategy.py`（都在 C 区）：问题、问题类型（决定窗口角色）、固定的数据快照、策略和参数、要验证的策略和判定规则、已有研究的预期、样本地板、试验上限、停止条件。基准和成本由评估台固定，不在这里写。
3. 开发窗口：`run <问题ID> --strategy <id> [--set 参数=值] [--falsify]`。策略代码只给出每日目标敞口；执行滞后、成本、指标、基准都由评估台负责。每次运行登记在 `research/<问题ID>/runs/`，失败的也算；不同的配置算新试验，超过上限就拒绝。`--falsify` 同时跑标准证伪，不算新试验；要验证的策略都要先这样跑一次。`ledger` 看试验数和折算夏普，`report <问题ID>` 看第一屏。
4. Owner 确认预注册，记为一个 `Owner-Decision`；`prereg.yaml` 和 `strategy.py` 随一次 ship 进 main，决定写进 `docs/requirements`。
5. 验证窗口：`validate <问题ID>`，每题一次，pi 在单独一次运行里做（`_template/brief.md`）。预注册或策略模块和 main 上的不一致、或判定规则写了不存在的指标，就拒绝。验证和一致性窗口的运行总是跑标准证伪，判定规则可以读证伪结果（如 `ci_low.max_drawdown`）。之后才能 `consistency <问题ID>`。
6. 核验：`report <问题ID>` 的第一屏；没碰评估台和基准；试验数没超上限；结论符合预注册规则。
7. 写 `research/<问题ID>/decision.md`，结论用上面四个词，引用登记过的运行号（`<问题ID>-r0001`）和读取号（`<问题ID>-w0001`）。保留的进前瞻记录；拒绝的进 `negative_results.md`（人审后更新）。
