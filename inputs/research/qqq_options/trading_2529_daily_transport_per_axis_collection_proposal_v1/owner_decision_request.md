# TRADING-2529 Owner Decision Request

状态：`OWNER_FINAL_TOKEN_REQUIRED`；当前 `external_action=none`。

这只是逐轴 export-safe session/count aggregate 采集提案，不是 Cloud、raw rows、
交易、订单、DQ/PIT、策略有效性或投资结论授权。

## 为什么需要这一步

2528 只能确认 option chain 到达，但 underlying、bid/ask、Greeks、IV、OI、volume
与 cross-field consistency 七轴都没有独立计数，因此 1201 个 combined-gate rejects
仍无法定位。候选运行只补齐每轴 PRESENT/MISSING/INVALID/NOT_EVALUATED 会话计数。

## Exact scope

- repository base：`4366092a2284557a659daa3bd497250ea0ce1052`
- target project：`34808569`
- range / XNYS sessions：`2021-02-22..2025-12-02` / `1202`
- axes：`OPTION_CHAIN_PRESENCE, UNDERLYING_PRICE, BID_ASK_QUOTE, GREEKS, IMPLIED_VOLATILITY, OPEN_INTEREST, VOLUME, CROSS_FIELD_CONSISTENCY`
- run scope content / file SHA-256：`6c10f143fa542505b4696f255303510015e6b2318f22d6ac83e1c0933a974c33` / `23b9a1fa2aef4973b6c1e8892e6245a99f37587554de02fcb32609b4c0dd0a13`
- proposal content / file SHA-256：`2c41024a72229245290599da58056d5b0fd31da9cce7a562e9b7fe9e411081c9` / `09d62938680a2d2190e03787de3cf44c7d0097c7e004956ad56235f809320656`
- project code LF SHA-256 / bytes：`adfc060fff3cfd840565fb000ac4a1759b6f54f847568dd46c5418912d0b1421` / `24420`

## 后续 Owner token 最多允许的动作

- `INTERACTIVE_LOGIN_TO_EXISTING_QUANTCONNECT_ACCOUNT`
- `MUTATE_EXISTING_PROJECT_34808569_ONCE`
- `RUN_ONE_ZERO_ORDER_CLOUD_BACKTEST`
- `MANUALLY_DOWNLOAD_ONE_RESULTS_JSON`
- `COLLECT_PER_AXIS_SESSION_COUNT_AGGREGATES_ONLY`

## 持续禁止

- `ANY_ACTION_BEFORE_EXACT_OWNER_TOKEN_ADMISSION`
- `RAW_OPTION_ROWS_OR_INDIVIDUAL_CONTRACT_VALUES`
- `LOGS_AS_DATA_OR_OBJECT_STORE_EXPORT`
- `API_CLI_HTTP_OR_BACKGROUND_NETWORK_PATH`
- `SECOND_PROJECT_MUTATION_OR_SECOND_CLOUD_RUN`
- `ANY_ORDER_FILL_PAPER_LIVE_BROKER_OR_PRODUCTION_ACTION`
- `DQ_PIT_SELECTION_ENGINE_STRATEGY_OR_INVESTMENT_CONCLUSION`

## Owner token template（当前未签署）

```text
owner_decision:TRADING-2529:<YYYY-MM-DD>:authorize_single_zero_order_daily_transport_per_axis_export_safe_aggregate_collection_v1
ordinary_pushed_main_sha:<ORDINARY_PUSHED_MAIN_SHA>
registration_base_repository_code_sha:4366092a2284557a659daa3bd497250ea0ce1052
proposal_policy_file_sha256:05f45abfc296cb9e622559fde0602f4274ac9a52a42cacb92b1d6cca86707cc9
proposal_policy_canonical_sha256:417af9f94d81f83c44feb4dde3b663a7f67122abbda99ceb77bffd416c351f73
source_diagnostic_content_sha256:e8125e165f8acf6147f15fbd64701832ba6f602bbc98d69863d65ae942b8b7aa
source_diagnostic_canonical_sha256:b2382b928a860685412add5ac091ac458d08ab9d246351a4a5a516d050eca9ac
run_scope_content_sha256:6c10f143fa542505b4696f255303510015e6b2318f22d6ac83e1c0933a974c33
run_scope_canonical_sha256:23b9a1fa2aef4973b6c1e8892e6245a99f37587554de02fcb32609b4c0dd0a13
proposal_content_sha256:2c41024a72229245290599da58056d5b0fd31da9cce7a562e9b7fe9e411081c9
proposal_canonical_sha256:09d62938680a2d2190e03787de3cf44c7d0097c7e004956ad56235f809320656
project_code_lf_sha256:adfc060fff3cfd840565fb000ac4a1759b6f54f847568dd46c5418912d0b1421
target_project_id:34808569
requested_range:2021-02-22..2025-12-02
expected_session_count:1202
maximum_project_mutations:1
maximum_cloud_backtests:1
maximum_orders:0
maximum_fills:0
collector:codex_capability_coordinator
independent_reviewer:project_owner
authorization_expires_at_utc:<OWNER_SELECTED_EXPIRY_NOT_MORE_THAN_168_HOURS>
authorization_single_use:true
authorization_invalidates_on_first_run_attempt:true
```

只有 ordinary-pushed exact main、全部 hashes 与 expiry 均匹配后，Owner 对完整文本的
再次明确确认才可能进入独立后继 admission；本 package 自身没有执行能力。
