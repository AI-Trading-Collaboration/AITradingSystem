# TRADING-2513 Owner Decision Request

状态：`OWNER_AUTHORIZATION_REQUIRED`

这是一份 zero-order、export-safe derived aggregate collection 授权请求，不是策略、阈值、交易或投资结论授权。

## Exact run scope

- repository code SHA：`83d4f9680c4f78c7c1414659d51738ba7f615a7a`
- target project id：`34808569`
- requested/evaluated range：`2021-02-22..2025-12-02`
- XNYS session count：`1202`
- first/last session：`2021-02-22` / `2025-12-02`
- project code LF SHA-256：`d7f96fbb14e03a1f248b0a14b3ebdaa1bbeeada2d15f87fb3277b98b9c6641a6`
- proposal content SHA-256：`f48732afc0d69656fbe5c62b1965296feccda30caa3279c80b9d1c20ce272240`
- run scope content SHA-256：`80c11d7073dcc86f1297a34b3497fe705069619d6f1f51927ab9b673172db15e`
- proposal policy file/canonical SHA-256：`dc64eae45a3581089af1223c8bc6da005c0962d17906ad447cf72f8a9a5fbbaf` / `4c80425fae656c573ca74d44e5d738bc78307619c0471f2c852446430fefdbc6`
- collector policy file/canonical SHA-256：`48511cc64cab07b091787e2b0cb23354424248da66e7dba8866cd9ce9a766a8f` / `3ebdd8a4dd89aad4584fbe8bffeeabb30d9b7bd2c28cd394c0fbc346939e999f`
- transport map SHA-256：`60c970b71d3c47337fb76452d1384f2463079ef5026239e875e78b8c37d3eab5`

## Allowed actions（仅在 Owner 另行签署后）

- `QUANTCONNECT_LOGIN`
- `MODIFY_EXISTING_DEDICATED_PROJECT_ONCE`
- `RUN_ONE_ZERO_ORDER_CLOUD_BACKTEST`
- `EXPORT_SAFE_MANUAL_DOWNLOAD_RESULTS_COLLECTION`

## Prohibited actions

- `API`
- `BROKER`
- `CLI`
- `HTTP`
- `INVESTMENT_INTERPRETATION`
- `LIVE`
- `OBJECT_STORE`
- `PAPER`
- `PRODUCTION`
- `PURCHASE_OR_SUBSCRIPTION`
- `RAW_OPTIONS_DATA_DOWNLOAD`
- `RAW_OPTION_ROW_LOGGING_OR_EXPORT`
- `SECOND_CLOUD_BACKTEST`

## Review checklist

1. 在已 ordinary-push 的 exact main 上复核本 package 五文件 inventory 与 hashes。
2. 复核 target project、1202 sessions、一次 project mutation / 一次 cloud backtest / 零订单零成交上限。
3. 复核 `main.py` 不包含 threshold、order、raw-row logging/export、Object Store 或 network 行为。
4. 选择不超过 168 小时的 expiry，并确认 single-use 与 evidence collection 后失效。
5. 指定 collector 与 independent reviewer；reviewer 必须独立复核外部动作次数、结果下载与 prohibited-action absence。
6. 授权不等于 DQ PASS、policy reviewed、selection/engine enabled 或 investment interpretation。

## Owner token template（当前未签署）

```text
owner_decision:TRADING-2513:<YYYY-MM-DD>:authorize_single_zero_order_primary_window_derived_aggregate_collection_v1
ordinary_pushed_main_sha:<ORDINARY_PUSHED_MAIN_SHA>
repository_code_sha:83d4f9680c4f78c7c1414659d51738ba7f615a7a
proposal_content_sha256:f48732afc0d69656fbe5c62b1965296feccda30caa3279c80b9d1c20ce272240
run_scope_content_sha256:80c11d7073dcc86f1297a34b3497fe705069619d6f1f51927ab9b673172db15e
project_code_lf_sha256:d7f96fbb14e03a1f248b0a14b3ebdaa1bbeeada2d15f87fb3277b98b9c6641a6
proposal_policy_file_sha256:dc64eae45a3581089af1223c8bc6da005c0962d17906ad447cf72f8a9a5fbbaf
proposal_policy_canonical_sha256:4c80425fae656c573ca74d44e5d738bc78307619c0471f2c852446430fefdbc6
collector_policy_file_sha256:48511cc64cab07b091787e2b0cb23354424248da66e7dba8866cd9ce9a766a8f
collector_policy_canonical_sha256:3ebdd8a4dd89aad4584fbe8bffeeabb30d9b7bd2c28cd394c0fbc346939e999f
transport_map_sha256:60c970b71d3c47337fb76452d1384f2463079ef5026239e875e78b8c37d3eab5
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
authorization_invalidates_after_evidence_collection:true
```
