# TRADING-2456：当前研究执行链窗口语义一致性修复

最后更新：2026-07-22

状态：`DONE`

稳定任务 ID：`TRADING-2456_CURRENT_RESEARCH_EXECUTION_CHAIN_WINDOW_SEMANTIC_CONSISTENCY`

## 背景

项目 active strategy research、primary backtest 与投资面向结论已经统一使用
`2021-02-22`。`docs/research/current_research_strategy_execution_chain.md` 顶部和窗口总表已经
正确声明该规则，但三个后追加的 Smoothed 小节仍把
`ai_after_chatgpt=2022-12-01` 写成“当前/仍是主结论窗口”。这些文字与当前 policy、项目规则和
owner 已确认的窗口语义冲突，也会让读者把历史 contract fixture 的实际 evaluated range 误读成
未来 run 的 active default。

历史 artifact、fixture 和已完成 run 的 requested/actual range 必须原样保留；本任务只修复它们的
证据角色，不改写历史结果。

## 目标与范围

1. 将三个错误的 active-primary 表述统一改为：active primary conclusion window 从
   `2021-02-22` 开始。
2. 保留 `2022-12-01` 历史 fixture/run 的真实 requested/evaluated range，并明确其角色仅为
   immutable historical/legacy comparison evidence。
3. 增加文档契约测试，禁止已识别的三类错误短语回归，并要求 2021 active primary、2022
   historical-only 边界同时存在。
4. 同步任务登记、详细进展与必要的 generated/hash evidence；不修改 runtime config、策略阈值、
   历史 artifact bytes 或研究结论。

## 实施步骤与验收

|阶段|内容|验收|
|---|---|---|
|S0|逐处窗口语义审计|区分 active default、historical requested/actual range、comparison role；不批量删除合法历史日期|
|S1|权威文档修复|三个 Smoothed 小节不再把 2022-12-01 称为当前/默认/主结论窗口；2021-02-22 active primary 明确|
|S2|防回归合同|focused test 对三个旧错误短语 fail closed，并验证 historical evidence 仍被保留和正确标记|
|S3|集成验证|docs/task generated views、architecture/contract/Full 及 source/hash freshness 按最终影响范围 PASS|

## 安全边界

- 不修改 `config/research/primary_research_window_policy.yaml` 或任何 runtime default；
- 不回写、删除或伪造历史 artifact 的 requested/actual range；
- 不运行 B/C、新 candidate/search、backtest、prospective holdout 或 provider refresh；
- 不改变 threshold、score、weights、promotion、paper-shadow、production 或 broker/order；
- `strategy_logic_changed=false`、`production_effect=none`、`broker_action=none`。

## 进展记录

- 2026-07-22：登记并进入 `IN_PROGRESS`。审计确认总表已正确使用 2021 active primary，错误集中在
  G2.4CQ/CR/CS 三个后追加小节；合法的 historical comparison、immutable fixture actual range 和旧
  artifact 描述继续保留。
- 2026-07-22：S0～S2完成并进入`VALIDATING`。三个错误短语均已移除；G2.4CQ/CR/CS明确使用
  `2021-02-22` active primary，同时保留`2022-12-01` immutable historical fixture实际范围及
  historical/legacy comparison角色。Focused首轮因Markdown换行使原始字符串计数为2而失败，测试改为
  规范化空白后仍要求三处边界，复验=`3 passed`；没有降低语义门槛。
- 2026-07-22：S3完成并转`DONE`。Expanded focused=`52 passed`，architecture=`447 passed`，
  contract=`265 passed`，reproducibility=`23 passed`；唯一自然集成边界Full=
  `6576 passed / 2 skipped / 1082.57s`，provenance、scheduler、telemetry、performance与safety
  boundary均PASS，tail-idle max=`0.016s`。本任务没有改变策略逻辑、历史artifact、缓存数据、生产状态或
  broker/order；后续由权威文档合同持续阻止旧active-primary表述回归。
