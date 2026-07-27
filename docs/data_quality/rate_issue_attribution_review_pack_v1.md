# DATA-GOV-002C2 Rate Row Issue Attribution Source-Owner Review Pack

- Review pack ID：`dq_rate_issue_attribution_review_b598da480819c534fbb9d471`
- 状态：`SOURCE_OWNER_DECISION_PENDING`
- 候选 site：`6`
- Scope taxonomy：single-row=`4`，row-pair=`2`
- Source-owner decisions pending：`6`
- 当前新增隔离授权：`0`
- Production effect：`none`；broker action：`none`

## 工程结论

本 pack 建议 source owner 逐项审查以下 6 个 site。它不是审批记录，不修改 `DataQualityIssue`、capability classifier 或任何 full/scoped DQ 结果。

若后续逐项获批，C3 的首个保守规则建议为 `ALL_AFFECTED_RATE_SERIES_OUTSIDE_REQUIRED_SCOPE`。只有 affected rate series 完整、非空，且与 consumer required rate series 完全不相交时才可讨论隔离；window/row-level isolation 仍未授权。

## Exact candidates

|Issue code|Site ID|Severity|Scope taxonomy|Affected window rule|Disposition|Decision|
|---|---|---|---|---|---|---|
|`rates_extreme_daily_change`|`dq_issue_site_85549de0f1e9ab739a74`|`ERROR`|`CURRENT_AND_PREVIOUS_VALID_OBSERVATION`|`PREVIOUS_TO_TRIGGER_DATE_INCLUSIVE`|`CONTRACT_WAVE_CANDIDATE`|`PENDING_SOURCE_OWNER_DECISION`|
|`rates_invalid_date`|`dq_issue_site_0e7f3d74bfa489801c83`|`ERROR`|`SINGLE_SOURCE_ROW`|`UNAVAILABLE_FOR_INVALID_DATE`|`CONTRACT_WAVE_CANDIDATE`|`PENDING_SOURCE_OWNER_DECISION`|
|`rates_invalid_value`|`dq_issue_site_f337897b3d0d0b8e2842`|`ERROR`|`SINGLE_SOURCE_ROW`|`EXACT_TRIGGER_DATE_WHEN_PARSEABLE`|`CONTRACT_WAVE_CANDIDATE`|`PENDING_SOURCE_OWNER_DECISION`|
|`rates_non_finite_value`|`dq_issue_site_dcc6dcab7a17c225b404`|`ERROR`|`SINGLE_SOURCE_ROW`|`EXACT_TRIGGER_DATE_WHEN_PARSEABLE`|`CONTRACT_WAVE_CANDIDATE`|`PENDING_SOURCE_OWNER_DECISION`|
|`rates_out_of_range`|`dq_issue_site_6421117ee905a6da1438`|`ERROR`|`SINGLE_SOURCE_ROW`|`EXACT_TRIGGER_DATE_WHEN_PARSEABLE`|`CONTRACT_WAVE_CANDIDATE`|`PENDING_SOURCE_OWNER_DECISION`|
|`rates_suspicious_daily_change`|`dq_issue_site_df1c184d09e3c55d3e71`|`WARNING`|`CURRENT_AND_PREVIOUS_VALID_OBSERVATION`|`PREVIOUS_TO_TRIGGER_DATE_INCLUSIVE`|`CONTRACT_WAVE_CANDIDATE`|`PENDING_SOURCE_OWNER_DECISION`|

## 逐项 source-owner questions

### `rates_extreme_daily_change`

- Predicate：`ABSOLUTE_CHANGE_EXCEEDS_EXTREME_POLICY_THRESHOLD`
- Row dependencies：`PREVIOUS_VALID_SAME_SERIES_ROW, TRIGGER_ROW`
- Defect fields：`value`
- Incomplete when：`RATE_SERIES_MISSING_OR_BLANK, PREVIOUS_VALID_OBSERVATION_UNAVAILABLE, TRIGGER_OR_PREVIOUS_ROW_IDENTITY_UNAVAILABLE`
- 待决定：是否确认 move issue 的完整 row scope 必须同时包含当前行和前一有效观测？
- 待决定：是否要求 C3 保存命中的 series-specific threshold 与 predecessor identity？

### `rates_invalid_date`

- Predicate：`PARSED_DATE_IS_MISSING`
- Row dependencies：`TRIGGER_ROW`
- Defect fields：`date`
- Incomplete when：`RATE_SERIES_MISSING_OR_BLANK, SOURCE_ROW_IDENTITY_UNAVAILABLE`
- 待决定：是否确认 invalid date 不阻止按 non-empty series 做保守 series-level attribution？
- 待决定：是否接受该类 issue 永不参与 window-level isolation？

### `rates_invalid_value`

- Predicate：`NUMERIC_VALUE_IS_MISSING`
- Row dependencies：`TRIGGER_ROW`
- Defect fields：`value`
- Incomplete when：`RATE_SERIES_MISSING_OR_BLANK, SOURCE_ROW_IDENTITY_UNAVAILABLE`
- 待决定：是否确认 value 缺失或非数值只影响触发行及其 series？
- 待决定：date 无法解析时是否固定降级为 series-level attribution？

### `rates_non_finite_value`

- Predicate：`NUMERIC_VALUE_IS_NON_FINITE`
- Row dependencies：`TRIGGER_ROW`
- Defect fields：`value`
- Incomplete when：`RATE_SERIES_MISSING_OR_BLANK, SOURCE_ROW_IDENTITY_UNAVAILABLE`
- 待决定：是否确认 Infinity/non-finite 只影响触发行及其 series？
- 待决定：date 无法解析时是否固定降级为 series-level attribution？

### `rates_out_of_range`

- Predicate：`VALUE_OUTSIDE_SERIES_POLICY_RANGE`
- Row dependencies：`TRIGGER_ROW`
- Defect fields：`value`
- Incomplete when：`RATE_SERIES_MISSING_OR_BLANK, SOURCE_ROW_IDENTITY_UNAVAILABLE`
- 待决定：是否确认 configured plausible range 的越界只影响触发行及其 series？
- 待决定：是否要求 C3 把实际命中的 series-specific threshold 一并写入 evidence？

### `rates_suspicious_daily_change`

- Predicate：`ABSOLUTE_CHANGE_EXCEEDS_SUSPICIOUS_BUT_NOT_EXTREME_THRESHOLD`
- Row dependencies：`PREVIOUS_VALID_SAME_SERIES_ROW, TRIGGER_ROW`
- Defect fields：`value`
- Incomplete when：`RATE_SERIES_MISSING_OR_BLANK, PREVIOUS_VALID_OBSERVATION_UNAVAILABLE, TRIGGER_OR_PREVIOUS_ROW_IDENTITY_UNAVAILABLE`
- 待决定：是否确认 warning 也使用与 extreme move 相同的完整 row-pair attribution？
- 待决定：是否确认 C3 不因 severity=WARNING 自动扩大任何 isolation 权威？

## 后续边界

- 所有 decision slots 仍为 `PENDING_SOURCE_OWNER_DECISION`。
- 未完整归因的 issue 继续保持 `GLOBAL_OR_UNKNOWN_SCOPE`。
- 获批后仍必须另建 C3 serial contract wave；本 pack 不自动启动 C3。
- 不迁移 daily/periodic consumer，不生成 strategy、weight、production 或 broker action。
