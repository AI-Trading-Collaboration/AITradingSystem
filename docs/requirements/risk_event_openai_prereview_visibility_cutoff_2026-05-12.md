# RISK-015 风险事件 OpenAI 预审请求可见时间

状态：IN_PROGRESS

最后更新：2026-05-12

关联任务：`OPS-011`、`VIS-001`、`RISK-007`

## 背景

2026-05-12 日本时间上午执行最新交易日 `daily-run` 时，默认 `as_of` 已按美股市场时区修正为 2026-05-11，但 `score-daily` 在风险事件 OpenAI 自动预审处失败。失败原因是预审记录的 `request_timestamp` 发生在 2026-05-12 UTC，而校验逻辑使用 `request_timestamp.date() > as_of` 判定未来输入。

这个规则对历史回放和历史 as-of live 请求是必要的，但对生产日报不合理：5/11 美股收盘后生成 5/11 日报时，OpenAI 预审请求自然会出现在 5/12 UTC/JST。它不是未来函数，只要来源发布日期/采集日期不晚于 `as_of`，且请求时间不晚于本轮生产运行的 `visibility_cutoff`。

## 设计边界

- `published_at` 和 `captured_at` 仍必须不晚于 `as_of`，不能把未来官方来源候选送入当日评分。
- 历史 as-of 或没有显式生产 cutoff 的导入/预审路径继续按 `as_of` 当日 UTC 末尾 fail closed，避免用 live OpenAI 请求重写历史。
- 仅当 `score-daily` 的评估日等于当前最新已完成美股交易日时，live OpenAI 预审可使用本轮运行时间作为 `visibility_cutoff`。
- `request_timestamp` 只可晚于 `as_of` 但不得晚于 `visibility_cutoff`；超过 cutoff 仍视为未来输入。
- 预审输出仍只能进入 `llm_extracted / pending_review`，以及 owner 已批准的 LLM formal assessment 路径；不得直接触发 position gate。

## 验收标准

- 最新已完成美股交易日的生产 `score-daily` / `daily-run` 可以在收盘后 UTC 次日生成 OpenAI 预审记录并通过校验。
- 历史 `as_of` 未提供生产 cutoff 时仍拒绝 `request_timestamp` 晚于评估日的预审记录。
- 测试覆盖有 cutoff 与无 cutoff 两种分支。
- README 和 `docs/system_flow.md` 说明 `request_timestamp` 与 `visibility_cutoff` 的关系。

## 进展

- 2026-05-12：新增任务并进入实现，原因：最新交易日 `daily-run` 已越过 PIT、SEC 和 valuation，但在 `score_daily` 的 `risk_event_prereview_request_in_future` 阻塞；需要区分生产日报的盘后决策时间和历史回放的 as-of 日期。
