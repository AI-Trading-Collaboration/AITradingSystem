# TRADING-2556：First-layer composer v2 基础可证伪实证运行 V1

最后更新：2026-09-03

稳定任务 ID：`TRADING-2556_FIRST_LAYER_COMPOSER_V2_FOUNDATIONAL_FALSIFICATION_EMPIRICAL_RUN_V1`

Owner 指令：`owner_instruction:TRADING-2556:2026-09-03:continue_foundational_empirical_validation`

授权状态：`STANDING_OWNER_SCOPE`

状态：`BASELINE_DONE_TERMINAL_INVALID`

## 1. 目标与解释边界

本任务承接已发布的 TRADING-2555 F0 结果盲合同，在看到任何新增实证诊断之前，冻结一次本地
`R1_BOUNDED_RESEARCH_SANDBOX` 运行的代码、输入、统计量、计数器和退出条件。目标是判断已知
TRADING-2550 `RETAIN` 的窄义 QQQ/现金择时收益，是否能够通过年份、连续 episode、序列相关、成本、
现金 carry、状态迁移和源版本稳定性检验。

本任务不是 pristine out-of-sample 研究，不授权参数搜索、阈值重估、模型重训或事后补救。任何
`PASS` 只把 QQQ Options Wave B 留在 `OWNER_REVIEW_REQUIRED`；Wave C、paper/live、production、broker、
orders、fills、positions 均保持未授权。

## 2. 冻结权威

- F0 exact-main commit：`fcb2a420ed1489189ea1ec9a323724943dcaee52`；
- F0 policy：
  `config/research/first_layer_composer_v2_foundational_falsification_preregistration_v1.yaml`；
- F0 policy file/canonical/authority-set SHA-256：
  `54dc349be1ec5670f9e02fc74e9467b668b2311a7dadbdc22680c8c605a824ad`、
  `ea6b51baf7d8bdfec2454fb037131a199736e6cacb1eecdc35e01701f5357818`、
  `a07e63c9f3ba035d94cfdbf18bc096b69380e4baf1b003540390b66d4ec44fe3`；
- 主窗口：XNYS `2021-02-22..2025-12-02`，1202 个信号 session、1201 个收益 interval；
- 信号 lag：1 session；`constructive/risk_on -> QQQ weight 1`，其余状态为 0；
- 主 comparator：`long_interval_count / 1201` 的静态 QQQ 权重，其余为零收益现金；
- 主成本：candidate 与 comparator 同为单边 5 bps。

运行授权和执行 manifest 必须精确绑定实现 commit、模块 SHA、F0 权威及所有输入 SHA/byte count。
一旦实证 aggregate 可见，F0/F1 V1 的窗口、模型、阈值、comparator、成本、bootstrap、reducer 和输出
schema 均不得修改来挽救结果。

## 3. 有界运行清单

最多允许：

- manifest replay：1；
- canonical DQ：1；
- local foundational run：1；
- independent replay：1；
- data download、cache mutation、QuantConnect、option backtest、provider action、orders、fills、
  positions：全部 0。

只读输入限于执行 manifest 明确列出的既有 signal index/daily signal package、first-layer 五态预测、
canonical QQQ/SGOV/TQQQ price、rates、secondary prices、download manifest、DQ policy、XNYS calendar policy、
TRADING-2550 receipts 与 F0/F1 权威。不得导出逐行市场数据或逐行信号；terminal artifact 只保留 aggregate
统计、输入身份、计数器和安全边界。

## 4. 冻结统计定义

1. 主路径使用与 TRADING-2550 相同的 fully-funded 账户和交易成本公式，并逐字段 reconciliation。
2. 年度归因按收益 interval 左端 session 的 calendar year 分桶；2021、2025 明示为 partial year。
3. 连续 episode 是 `interval_target=1` 的最大连续区间；按 episode 报告 interval 数、候选复合收益、
   comparator 复合收益和 paired excess，不输出逐日值。
4. Leave-one-year-out 直接删除该年 interval 后，按原顺序复合剩余已冻结的同 session candidate/comparator
   net-return pair；不重训、不重选阈值、不重算状态。
5. Paired circular moving-block bootstrap 以同 session candidate/comparator net-return pair 为抽样单位，
   block length 固定 21/63，seed=2555，每个长度 10,000 次。每次统计量为两条重采样路径的复合总收益差
   percentage points；输出 2.5/50/97.5 percentile 和 `P(excess<=0)`。
6. 成本敏感性在 5/10/15/20 bps 下对 candidate/comparator 全量重算；break-even 只报告离散 bracket。
7. SGOV carry 仅为 diagnostic：candidate 空仓部分和 comparator 现金部分使用同 session SGOV adjusted-close
   return；QQQ 交易和 5 bps 成本保持不变，SGOV 只作为现金收益指数、不模拟 SGOV 成交或另收交易成本；
   不替换主零收益现金结论。
8. 主路径的逐 interval net-return pair 从完整 self-financing equity curve 相邻观测导出：左端发生的建仓或
   平仓成本归入该 interval，最终清仓成本归入最后一个 interval；两条逐 interval 路径分别复合必须与
   TRADING-2550 candidate/comparator final value 在 `1e-8` 内一致。年度、leave-one-year-out 和 bootstrap
   复用这组冻结 pair，不另改成本归属。
9. 连续 long episode 独立按 episode 起点建仓、末端清仓全额复算 candidate；comparator 在相同 session
   slice 上使用全窗口冻结静态权重和相同单边成本。episode 结果不用于 reducer，只用于集中度披露。
10. 状态迁移来源固定为生成 signal package 的同一份
    `first_layer_composer_v2_operational_predictions.csv`。对相邻预测行定义有序 pair
    `state[t] -> state[t+1]`，在 `t+1` session 已知 transition 后，forward horizon `h` 的市场结果定义为
    QQQ adjusted-close `close[t+1] -> close[t+1+h]`，`h` 固定 1/5/20；尾部不足 `h` 的 observation 计入
    `MISSING`，不得解释为 0 或进入成熟样本均值。
11. Policy-consumption 与 selection-history 只核对既有静态权威；禁止把 dormant 字段接入旧模型。
12. Source-revision diff 比较本次 manifest 与 TRADING-2550 输入身份；任何未解释 byte drift 使结果
    `INVALID`。

Reducer 严格复用 F0：`INVALID > FAIL > INSUFFICIENT > PASS`。主 5 bps paired excess `<=0` 或任一
bootstrap 97.5% 上界 `<=0` 为 `FAIL`；任一 bootstrap 2.5% 下界 `<=0`、任一 leave-one-year-out excess
`<=0` 或必需诊断不完整，最多为 `INSUFFICIENT`。

## 5. 实施阶段

1. 登记任务并冻结本 requirement；
2. 实现严格 authorization/manifest loader、纯计算诊断、write-once aggregate artifact 与 validator；
3. synthetic/golden/negative 测试验证成本、lag、bootstrap、年份、episode、SGOV、状态迁移和 reducer；
4. 在实现 commit 后生成 exact execution manifest，并在 dispatch 前自动 replay；
5. 运行一次 canonical DQ；非 PASS 立即写 terminal `INVALID`，不得继续收益计算；
6. 运行一次 aggregate empirical diagnostics 与一次独立重放；
7. 写入 result admission、更新 task/Atlas/system flow，并完成 focused/formal/Full；
8. 仅在发布 fence、local-main fast-forward、ordinary push 与 SHA 复核均 PASS 后收口。

## 6. 验收标准

- F1 task、authorization、manifest、实现、输入和输出均精确绑定且可重放；
- canonical DQ 显式 PASS，requested/evaluated window 均为 `2021-02-22..2025-12-02`；
- 10 个必需诊断全部完成，独立重放与主计算在冻结容差内一致；
- 运行计数不超过授权 maxima，所有外部/交易/生产计数保持 0；
- terminal verdict 由 F0 reducer 自动得出，不接受手工覆盖；
- aggregate artifact 不包含逐日价格、逐日信号或可反推账户/交易的原始 payload；
- 结论明确披露 reused-development-window、partial-year 和非 pristine-OOS 边界；
- focused、Architecture、Contract、Integration、Reproducibility、Full 与 ordinary publication PASS。

## 7. 工作区与退出条件

- governed mode：`SINGLE_LANE`；
- branch：`codex/trading-2556-foundational-falsification-f1`；
- 复用当前 checkout，不创建额外 worktree/clone；
- aggregate runtime 目录：
  `outputs/research/first_layer_composer_v2_foundational_falsification_v1/`；
- 该目录在 result admission 和正式验证完成前不得删除或覆盖；唯一运行 attempt 消耗后不得重跑；
- task 分支只在 main/remote 已含相同提交、证据已进入 canonical 位置且审计无独有内容后删除；
- `production_effect=none`、`broker_action=none`。

## 8. 进度记录

- 2026-09-03：Owner 在 F0 发布后指示“继续”，按 F0 successor envelope 解释为有界本地 F1 研究的
  `STANDING_OWNER_SCOPE`；不扩展为外部平台、数据下载、缓存修改、期权、paper/live、production 或 broker
  授权。任务登记阶段尚未读取市场数据、运行 DQ、计算 bootstrap 或查看新增实证结果。
- 2026-09-03：唯一 F1 V1 dispatch 已消耗。Manifest replay、canonical DQ（PASS，0 error/0 warning）、
  主路径与 TRADING-2550 独立会计重放均完成；但在把 bootstrap aggregate 转换为严格 reducer input 时，
  审计字段 `replicates`/`random_seed` 被直接传入不接受额外字段的 `BootstrapInterval`，导致 terminal
  `INVALID`。`aggregate_result.json` 未接纳任何收益诊断，因此本次失败不能用于判断信号有效或无效。
- 2026-09-03：V1 output 保留且禁止覆盖，同一 manifest 不得重跑。正确修复是新建版本化任务与新模块路径，
  仅过滤 reducer 所需的五个字段；窗口、信号、输入、成本、bootstrap draws、诊断和 reducer 均不得改变，
  并显式绑定本次失败 admission。不得在 V1 文件上做破坏历史 replay 的原地修改。
