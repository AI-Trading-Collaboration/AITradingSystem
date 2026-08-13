# TRADING-2516 Owner Decision Request

状态：`OWNER_AUTHORIZATION_REQUIRED_FRESH_TOKEN`

本请求只刷新 QQQ Options PRIMARY 主窗口的 zero-order、export-safe derived aggregate collection 授权。
它不授权 policy values、selection、engine、订单、投资解释、paper/live/broker/production。

## Exact scope

- selected lane：`QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_AGGREGATE_EVIDENCE`
- registration base：`65b2bc1c88bf98132b7f6d58359ae3f18cea85f9`
- target project id：`34808569`
- requested/evaluated range：`2021-02-22..2025-12-02`
- XNYS session count：`1202`
- maximum project mutations / cloud backtests：`1` / `1`
- maximum orders / fills：`0` / `0`
- result carrier：Owner manual `Download Results` JSON only

## Allowed actions（仅在 Owner exact token 后）

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

## Review rules

1. 使用 ordinary-pushed 2516 exact main 与 package manifest hashes 填充占位符。
2. expiry 必须晚于 2026-08-13 且不超过 168 小时；不得使用 2513/2514 的旧 token 倒签。
3. token single-use，并在 evidence collection 后失效。
4. 授权不等于 run/evidence/DQ/PIT PASS，也不解除 `KEEP_CLOSED + PREREGISTRATION_ONLY`。

## Owner token template（未签署）

```text
owner_decision:TRADING-2516:2026-08-13:authorize_single_zero_order_primary_window_derived_aggregate_collection_v2
ordinary_pushed_main_sha:<ORDINARY_PUSHED_2516_MAIN_SHA>
refresh_policy_file_sha256:4aa2983a6cb6c0ac02d03d18a807ea3bdf553770ac545130011911bf83caca77
refresh_policy_canonical_sha256:acd849fd8189256d4908cc162eb0c9bfe4162c669760577f21d6c960919b4882
refresh_package_manifest_file_sha256:<REFRESH_PACKAGE_MANIFEST_FILE_SHA256>
refresh_package_manifest_content_sha256:<REFRESH_PACKAGE_MANIFEST_CONTENT_SHA256>
proposal_content_sha256:f48732afc0d69656fbe5c62b1965296feccda30caa3279c80b9d1c20ce272240
run_scope_content_sha256:80c11d7073dcc86f1297a34b3497fe705069619d6f1f51927ab9b673172db15e
project_code_lf_sha256:d7f96fbb14e03a1f248b0a14b3ebdaa1bbeeada2d15f87fb3277b98b9c6641a6
proposal_policy_file_sha256:dc64eae45a3581089af1223c8bc6da005c0962d17906ad447cf72f8a9a5fbbaf
proposal_policy_canonical_sha256:4c80425fae656c573ca74d44e5d738bc78307619c0471f2c852446430fefdbc6
collector_policy_file_sha256:48511cc64cab07b091787e2b0cb23354424248da66e7dba8866cd9ce9a766a8f
collector_policy_canonical_sha256:3ebdd8a4dd89aad4584fbe8bffeeabb30d9b7bd2c28cd394c0fbc346939e999f
transport_map_sha256:60c970b71d3c47337fb76452d1384f2463079ef5026239e875e78b8c37d3eab5
admission_policy_file_sha256:8e7103680884288574b5cc0c0813085e47396f244c4fce9db275477013760a91
admission_policy_canonical_sha256:a4e399ea022c04b579bbaaeb12bdc922e332ceb1badb0e4ba9740f17e11f824a
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
