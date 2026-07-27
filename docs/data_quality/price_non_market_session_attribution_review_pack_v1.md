# DATA-GOV-002C2P Price Non-Market-Session Attribution Source-Owner Review Pack

- Review pack ID：`dq_price_issue_attribution_review_dff1943fa21f6aeaf9f15714`
- 状态：`SOURCE_OWNER_DECISION_PENDING`
- Exact site count：`1`
- Exact site：`dq_issue_site_312625a26da21428b763 / prices_non_market_session_date`
- Source-owner decision：`PENDING_SOURCE_OWNER_DECISION`
- 当前新增 runtime/schema/isolation/consumer 授权：`0`
- Production effect：`none`；broker action：`none`

## 工程结论

本 pack 只把一个已有 instrument-level pilot site 的六维归因建议整理成可审计评审输入。它不是 source-owner 批准记录，也不修改 `DataQualityIssue`、full/scoped DQ、capability classifier 或任何 consumer。

现有 issue 的 `rows` 是 requested window 内 distinct non-session date 数，不是触发 source row 数；`sample` 也只有前 10 个 distinct dates，不能作为完整 scope。

## Proposed six-dimensional attribution

|Dimension|Proposed rule|
|---|---|
|Price tickers|`DISTINCT_NORMALIZED_NON_EMPTY_TICKERS_FROM_ALL_TRIGGER_ROWS`|
|Rate series|`[]`|
|Source roles|`primary_market_prices`|
|Dates|`DISTINCT_NON_SESSION_DATES_WITHIN_REQUESTED_WINDOW`|
|Fields|`date`|
|Rows|`ALL_TRIGGER_ROWS_WITH_SOURCE_ORDINAL_AND_CANONICAL_ROW_DIGEST`|

## Fail-closed incomplete conditions

- `REQUESTED_WINDOW_IS_MISSING_OR_INVALID`
- `TRIGGER_DATE_SET_IS_EMPTY`
- `ANY_TRIGGER_DATE_IS_UNPARSEABLE_OR_OUTSIDE_REQUESTED_WINDOW`
- `ANY_TRIGGER_ROW_TICKER_IS_MISSING_OR_BLANK`
- `ANY_TRIGGER_ROW_SOURCE_ORDINAL_IS_MISSING_OR_DUPLICATED`
- `ANY_TRIGGER_ROW_CANONICAL_DIGEST_IS_MISSING_OR_INVALID`
- `TRIGGER_ROW_SET_IS_NOT_COMPLETE_FOR_EVERY_TRIGGER_DATE`
- `CALENDAR_AUTHORITY_OR_SPECIAL_CLOSURE_BINDING_IS_MISSING_OR_STALE`

## Price source-owner questions

- Confirm that primary_market_prices is the only canonical source role for this issue.
- Confirm that every source row on every distinct non-session date inside the requested window belongs to the affected row set.
- Confirm that the affected date scope is the exact distinct trigger-date set rather than a contiguous interval.
- Confirm that date is the only defective field while ticker, source_ordinal, and canonical_row_digest are identity evidence.
- Confirm that missing ticker, row identity, requested-window, or calendar authority must keep the issue GLOBAL_OR_UNKNOWN_SCOPE.

## 后续边界

- 当前 decision slot 仍为 `PENDING_SOURCE_OWNER_DECISION`。
- 未完整归因时继续保持 `GLOBAL_OR_UNKNOWN_SCOPE`。
- window/row-level isolation 仍未授权。
- 获批后仍须另建最小 serial C3 runtime contract wave；本 pack 不自动启动 C3。
- 不迁移 daily/periodic/research consumer，不生成权重、production 或 broker action。
