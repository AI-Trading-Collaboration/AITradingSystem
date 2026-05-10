# 输入可见性与 PIT 复现契约

状态：VALIDATING

最后更新：2026-05-11

## 背景

2026-05-10 用 `aits ops daily-run --as-of 2026-05-08` 验证 dashboard 自动触发时，流程在 `score_daily` 的 OpenAI 风险事件预审处 fail closed：

```text
risk_event_prereview_request_in_future
```

这个阻断本身是合理的：2026-05-10 发起的 live OpenAI 请求不可能是 2026-05-08 评估时点已经可见的输入。但它也暴露了一个系统边界问题：`daily-run`、历史复现和研究重建的入口语义还不够显式，导致流程先跑了多个 live/provider 写入步骤，最后才由 OpenAI 模块发现未来请求。

本任务先修复入口级边界，后续再扩展到更细的 artifact/row 级输入可见性。

## 核心定义

每个会影响评分、复现或审计解释的输入，长期应区分以下时间戳：

|字段|含义|
|---|---|
|`event_time`|事件或市场事实发生时间。|
|`source_published_at`|原始来源公开发布时间。|
|`provider_available_at`|供应商 API/页面可读取时间；供应商无法证明时不得伪造。|
|`ingested_at`|本系统实际抓取或写入本地缓存时间。|
|`available_time`|下游允许使用的保守可见时间，通常取可证明时间中的最晚者。|
|`visibility_cutoff`|本次任务声明的可见性截止时间。|

严格 PIT 规则：

```text
available_time <= visibility_cutoff
```

缺少可证明 `available_time` 的输入不能被当作严格复现输入，只能进入限制披露、研究重建或人工复核路径。

## 运行模式边界

|模式|入口|live provider / OpenAI|输出路径|用途|
|---|---|---|---|---|
|生产日常运行|`aits ops daily-run --as-of <当前生产日>`|允许，但必须归档 manifest、checksum 和 request/cache 审计|`outputs/runs/` + legacy mirror|生成当日生产投研结论。|
|严格历史复现|`aits ops replay-day --mode cache-only`|禁止；OpenAI 只能 `disabled` 或 `cache-only` 复制历史缓存|`outputs/replays/` 隔离 bundle|复现某个历史可见窗口下的分析产出。|
|研究重建|后续单独定义|可显式允许，但必须标记非 PIT、`production_effect=none`|不得覆盖生产 canonical artifacts|诊断、补充解释或非生产研究。|

本阶段不新增研究重建入口。历史 `as_of` 使用 `daily-run` 时应在执行任何子命令前被阻断，并提示改用 `ops replay-day`。

## 阶段拆解

|阶段|状态|验收标准|
|---|---|---|
|1. daily-run 历史 as-of 前置阻断|VALIDATING|`run_daily_ops_plan` 对历史 `as_of` 返回 `BLOCKED_VISIBILITY`，不执行 download/PIT/SEC/valuation/score/dashboard 等子命令；报告说明阻断原因和 replay 入口。|
|2. replay-day 接入 dashboard|VALIDATING|cache-only replay 在 `score_daily` 成功后生成 replay-scoped `evidence_dashboard_YYYY-MM-DD.html/json`，所有输入路径指向 replay bundle，不读取生产 canonical 输出。|
|3. 文档和测试|VALIDATING|README、runbook、system flow 说明入口边界；测试覆盖历史 daily-run 前置阻断、replay dashboard 命令和原最近交易日 replay 验证。|
|4. OpenAI cache-only row 级可见性过滤|VALIDATING|`replay-day --openai-replay-policy cache-only` 只复用 `request_timestamp/cache_created_at` 可证明不晚于有效 replay cutoff 的 prereview 记录；晚于 cutoff 或缺少可证明时间戳的记录进入排除审计，不调用 live OpenAI。|
|5. 手工输入 replay 隔离视图|VALIDATING|`replay-day` 生成 `input/data/external/trade_theses` 和 `input/data/external/trades` 过滤视图；`score-daily` 通过 path override 只读取 replay 路径；future thesis/trade 记录进入 input freeze manifest 的排除审计，不再由生产目录或下游校验泄漏到 replay。|
|6. market/macro raw cache replay 过滤|VALIDATING|`replay-day` 生成 as-of 过滤后的 `prices_daily.csv`、`prices_marketstack_daily.csv`、`rates_daily.csv` 和 replay download manifest；`score-daily` / `ops health` 只读取 replay raw 路径，不能因生产缓存含未来行情而在历史日期误触发 `prices_future_dates` / `rates_future_dates`。|
|7. artifact/row 级通用可见性 schema|READY|后续把更多输入族的 `available_time`、`source_published_at`、`ingested_at` 纳入统一 manifest 或质量报告，不在本阶段一次性改完。|

## 决策

- 不用 `--skip-risk-event-openai-precheck` 绕过 2026-05-08 的阻断来证明 dashboard 能跑通。
- 不把 2026-05-10 新抓取或新 LLM 处理的结果写成 2026-05-08 严格复现输入。
- `daily-run` 历史日期阻断不是失败绕行，而是把原本晚到 OpenAI 处的 PIT 错误前移到调度入口。
- `replay-day` 才是历史复现测试用例的正确入口；它必须继续隔离输出，不改写生产路径。
- `replay-day --openai-replay-policy cache-only` 可以复用本地历史 OpenAI prereview 缓存，但复用条件是缓存记录的可证明生成/请求时间不晚于有效 `visibility_cutoff`；不能证明时间的缓存记录不得进入严格复现输入。
- `trade_theses` 在 replay 中按 thesis 及其验证指标、证伪条件、风险事件和状态字段的可见日期整条过滤；任一未来状态会排除该 thesis，而不是让下游校验先读取 production 手工输入。
- `trades` 在 replay 中必须以交易记录可见时间为边界；缺少可证明记录时间的交易记录不能被当作严格 PIT 输入。未来平仓信息不得进入早于平仓日的 replay。

## 进展记录

- 2026-05-10：新增 `VIS-001`，进入实现。目标先收紧运行入口和 replay dashboard，不一次性迁移全部输入 schema。
- 2026-05-10：阶段 1-3 进入 `VALIDATING`。新增 `daily-run` 输入可见性预检查、metadata `input_visibility_status/issues`、replay-scoped dashboard 步骤和文档说明；`daily-run --as-of 2026-05-08` 已前置返回 `BLOCKED_VISIBILITY`，`replay-day --as-of 2026-05-08 --mode cache-only --openai-replay-policy cache-only --compare-to-production` 已 PASS 并生成 dashboard HTML/JSON。
- 2026-05-10：阶段 4 进入 `IN_PROGRESS`。owner 确认 replay 中能复用的历史 OpenAI 请求结果应尽量复用，但必须先增加 row 级可见性过滤和排除审计。
- 2026-05-10：阶段 4 进入 `VALIDATING`。`replay-day --openai-replay-policy cache-only` 已生成 replay 专用过滤 queue 和过滤报告；真实 `2026-05-08` replay PASS，源 OpenAI 预审队列 5 条均因 2026-05-10 请求时间晚于 cutoff 被排除，dashboard、pipeline health 和 secret scan 均通过。
- 2026-05-10：调查 `2026-05-08` replay dashboard 的 `final AI position=0` 后确认，直接原因是 `trade_theses` 文件均为 2026-05-10 创建或更新，相对 2026-05-08 严格复现属于未来手工输入，触发 24 个 thesis future-date 错误和 `thesis` position gate 0% 上限。当前行为是正确 fail closed，但 replay 输入冻结还应后续扩展到手工 thesis/trade 输入隔离视图。
- 2026-05-10：阶段 5 进入 `IN_PROGRESS`。owner 要求 `trade_theses`、`trades` 等手工数据也做好 replay 隔离；本轮实现限定为 cache-only replay 的过滤视图、命令 path override、manifest 审计和原日期复测。
- 2026-05-10：阶段 5 进入 `VALIDATING`。`replay-day` 已生成手工输入隔离视图并把 `score-daily` 的 `--thesis-path/--trades-path` 指向 replay bundle；真实 `2026-05-08` replay PASS，6 条 2026-05-10 thesis 被排除，最终 AI 仓位由旧 run 的 0%-0% 恢复为估值 gate 约束下的 40%-40%。
- 2026-05-11：阶段 6 进入 `IN_PROGRESS`。`replay-window --start 2026-05-01 --end 2026-05-10 --full-universe --openai-replay-policy cache-only --continue-on-failure` 暴露 2026-05-05 至 2026-05-07 的 `score-daily` 仍读取完整生产 `prices_daily.csv` / `rates_daily.csv`，数据质量门禁因 2026-05-08 未来行 fail closed；需要把市场和宏观 raw cache 纳入 replay as-of 过滤视图。
- 2026-05-11：阶段 6 进入 `VALIDATING`。`replay-day` 已生成 replay-scoped `prices_daily.csv`、`prices_marketstack_daily.csv`、`rates_daily.csv` 和 replay download manifest，并把 `score-daily` / `ops health` 指向该隔离 raw cache；复跑 2026-05-01 至 2026-05-10 后，2026-05-05、2026-05-06、2026-05-07、2026-05-08 均 PASS，周末 2026-05-02/03/09/10 正确跳过；剩余 2026-05-01 和 2026-05-04 为归档输入缺口，非未来行情过滤逻辑阻塞。
