# DATA-GOV-002 Phase C：DQ Issue Attribution Readiness Inventory

- inventory_id：`dq_issue_attribution_inventory_46d72c5be356c737467d8ef5`
- status：`SOURCE_OWNER_REVIEW_REQUIRED`
- 当前结论：本 inventory 不是新 issue 隔离授权；未 review 项继续 `GLOBAL_OR_UNKNOWN_SCOPE`。
- message/sample scope inference：`false`
- production_effect：`none`
- broker_action：`none`

## 汇总

|指标|值|
|---|---:|
|`canonical_site_count`|69|
|`direct_constructor_site_count`|63|
|`factory_call_site_count`|6|
|`static_site_count`|56|
|`template_site_count`|11|
|`dynamic_site_count`|2|
|`unique_static_code_count`|53|
|`policy_authorized_code_count`|1|
|`policy_authorized_site_count`|1|
|`legacy_affected_instruments_site_count`|1|
|`owner_review_required_site_count`|68|
|`factory_implementation_constructor_count`|1|
|`noncanonical_constructor_site_count`|2|

## 当前 reviewed policy code

- `prices_non_market_session_date`

## Canonical emission sites

|site_id|code kind / expression|emitter|scope status|owner review|legacy typed field|
|---|---|---|---|---|---|
|`dq_issue_site_be23df7146e06194de11`|`TEMPLATE_EXPRESSION` / `f'{role}_download_publication_binding_mismatch'`|`src/ai_trading_system/data/quality.py::_check_canonical_download_binding`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_b58edd8289bf54a7ea82`|`STATIC_LITERAL` / `download_manifest_canonical_binding_mismatch`|`src/ai_trading_system/data/quality.py::_check_canonical_download_binding`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_b268ab9e03d8dd8bd13e`|`TEMPLATE_EXPRESSION` / `f'{label}_duplicate_keys'`|`src/ai_trading_system/data/quality.py::_check_duplicate_keys`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_440e2b840ebfcd10edca`|`DYNAMIC_EXPRESSION` / `code`|`src/ai_trading_system/data/quality.py::_check_expected_price_tickers`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_0e9673b916d7044dadc4`|`TEMPLATE_EXPRESSION` / `f'{label}_missing_expected_values'`|`src/ai_trading_system/data/quality.py::_check_expected_values`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_77c8b873a1d08b943704`|`TEMPLATE_EXPRESSION` / `f'{label}_download_manifest_checksum_missing'`|`src/ai_trading_system/data/quality.py::_check_manifest_covers_file`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_312625a26da21428b763`|`STATIC_LITERAL` / `prices_non_market_session_date`|`src/ai_trading_system/data/quality.py::_check_price_market_calendar_dates`|`EXISTING_POLICY_AUTHORIZED_INSTRUMENT_SCOPE`|`EXISTING_OWNER_REVIEWED_PILOT`|`affected_instruments=true`|
|`dq_issue_site_6e2e6b8c204d145bb655`|`STATIC_LITERAL` / `prices_extreme_adj_close_move`|`src/ai_trading_system/data/quality.py::_check_price_moves`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_e35ba0673fdf3010f082`|`STATIC_LITERAL` / `prices_suspicious_adj_close_move`|`src/ai_trading_system/data/quality.py::_check_price_moves`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_b7cafd7c8796661ba203`|`STATIC_LITERAL` / `prices_known_split_adjustment_ratio_jump`|`src/ai_trading_system/data/quality.py::_check_price_moves`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_17d19386e111ec0d5aa8`|`STATIC_LITERAL` / `prices_adjustment_ratio_jump`|`src/ai_trading_system/data/quality.py::_check_price_moves`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_222dd39b31a3e2c1bf06`|`TEMPLATE_EXPRESSION` / `f'prices_non_finite_{column}'`|`src/ai_trading_system/data/quality.py::_check_price_numeric_rules`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_2c538718420446db496f`|`TEMPLATE_EXPRESSION` / `f'prices_invalid_{column}'`|`src/ai_trading_system/data/quality.py::_check_price_numeric_rules`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_45d05d6bfd7977a6322a`|`TEMPLATE_EXPRESSION` / `f'prices_non_positive_{column}'`|`src/ai_trading_system/data/quality.py::_check_price_numeric_rules`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_a1b1afc98349f62876b9`|`STATIC_LITERAL` / `prices_negative_volume`|`src/ai_trading_system/data/quality.py::_check_price_numeric_rules`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_6c5bb699a12e0c7a43e4`|`STATIC_LITERAL` / `prices_index_volume_not_applicable`|`src/ai_trading_system/data/quality.py::_check_price_numeric_rules`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_39aa038e581c541ef132`|`STATIC_LITERAL` / `prices_missing_volume`|`src/ai_trading_system/data/quality.py::_check_price_numeric_rules`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_080ecf81c78a87159f89`|`STATIC_LITERAL` / `prices_invalid_ohlc`|`src/ai_trading_system/data/quality.py::_check_price_numeric_rules`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_0680e34144c4e2bc847a`|`STATIC_LITERAL` / `prices_requested_window_coverage_missing`|`src/ai_trading_system/data/quality.py::_check_price_requested_window`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_a232ff999157501d15a3`|`STATIC_LITERAL` / `prices_internal_trading_day_gap`|`src/ai_trading_system/data/quality.py::_check_price_requested_window`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_d5a88647f240b62f088d`|`STATIC_LITERAL` / `prices_future_dates`|`src/ai_trading_system/data/quality.py::_check_price_staleness`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_17003a411be1131d0482`|`STATIC_LITERAL` / `prices_stale`|`src/ai_trading_system/data/quality.py::_check_price_staleness`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_85549de0f1e9ab739a74`|`STATIC_LITERAL` / `rates_extreme_daily_change`|`src/ai_trading_system/data/quality.py::_check_rate_moves`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_df1c184d09e3c55d3e71`|`STATIC_LITERAL` / `rates_suspicious_daily_change`|`src/ai_trading_system/data/quality.py::_check_rate_moves`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_6421117ee905a6da1438`|`STATIC_LITERAL` / `rates_out_of_range`|`src/ai_trading_system/data/quality.py::_check_rate_ranges`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_1866f7d114f3a4b085ad`|`STATIC_LITERAL` / `rates_future_dates`|`src/ai_trading_system/data/quality.py::_check_rate_staleness`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_c3b32d6f7cf5604d2260`|`STATIC_LITERAL` / `rates_stale`|`src/ai_trading_system/data/quality.py::_check_rate_staleness`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_6c632789c8734f557c99`|`TEMPLATE_EXPRESSION` / `f'{label}_missing_columns'`|`src/ai_trading_system/data/quality.py::_check_required_columns`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_d98c310ae55aa20c9685`|`STATIC_LITERAL` / `secondary_prices_no_reconciliation_overlap`|`src/ai_trading_system/data/quality.py::_check_secondary_price_reconciliation`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_34684dc102be0bdfe770`|`STATIC_LITERAL` / `secondary_prices_overlap_below_threshold`|`src/ai_trading_system/data/quality.py::_check_secondary_price_reconciliation`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_97b279e4c2b1cb01704e`|`STATIC_LITERAL` / `secondary_prices_close_mismatch`|`src/ai_trading_system/data/quality.py::_check_secondary_price_reconciliation`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_35827c040abf32cfbfeb`|`STATIC_LITERAL` / `secondary_prices_close_warning`|`src/ai_trading_system/data/quality.py::_check_secondary_price_reconciliation`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_1ce0fb764d35396f0433`|`STATIC_LITERAL` / `secondary_prices_known_split_close_basis`|`src/ai_trading_system/data/quality.py::_check_secondary_price_reconciliation`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_04a11e80c78951e6abe5`|`STATIC_LITERAL` / `secondary_prices_adj_close_mismatch`|`src/ai_trading_system/data/quality.py::_check_secondary_price_reconciliation`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_4265c5093f07c3c0f3cb`|`STATIC_LITERAL` / `secondary_prices_adjustment_basis_warning`|`src/ai_trading_system/data/quality.py::_check_secondary_price_reconciliation`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_b07d89c863c2158992db`|`STATIC_LITERAL` / `secondary_prices_adj_close_warning`|`src/ai_trading_system/data/quality.py::_check_secondary_price_reconciliation`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_8554afab573b2a941c76`|`STATIC_LITERAL` / `secondary_prices_adjustment_basis_info`|`src/ai_trading_system/data/quality.py::_check_secondary_price_reconciliation`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_5a25940cbcff57b48705`|`TEMPLATE_EXPRESSION` / `f'{label}_file_missing'`|`src/ai_trading_system/data/quality.py::_read_csv`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_80323f6f24680bc2c96f`|`TEMPLATE_EXPRESSION` / `f'{label}_file_unreadable'`|`src/ai_trading_system/data/quality.py::_read_csv`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_bb5c744f49b4ec8cba3b`|`TEMPLATE_EXPRESSION` / `f'{label}_file_unreadable'`|`src/ai_trading_system/data/quality.py::_read_csv`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_7f1cee4de1ae47e4e0f0`|`STATIC_LITERAL` / `secondary_prices_file_missing`|`src/ai_trading_system/data/quality.py::_read_secondary_prices_csv`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_ed6a6dca4aced55f547d`|`STATIC_LITERAL` / `secondary_prices_file_unreadable`|`src/ai_trading_system/data/quality.py::_read_secondary_prices_csv`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_97c9b23f5ad0847d719e`|`STATIC_LITERAL` / `secondary_prices_file_unreadable`|`src/ai_trading_system/data/quality.py::_read_secondary_prices_csv`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_2c96c159fee803553d0e`|`STATIC_LITERAL` / `download_manifest_missing`|`src/ai_trading_system/data/quality.py::_validate_download_manifest`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_c5d5ce35beddb69fd464`|`STATIC_LITERAL` / `download_manifest_unreadable`|`src/ai_trading_system/data/quality.py::_validate_download_manifest`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_478104d2792eb12ac0b8`|`STATIC_LITERAL` / `download_manifest_unreadable`|`src/ai_trading_system/data/quality.py::_validate_download_manifest`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_c597573968ab617fecf8`|`STATIC_LITERAL` / `manifest_missing_columns`|`src/ai_trading_system/data/quality.py::_validate_download_manifest`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_b33088a4000596e739b1`|`STATIC_LITERAL` / `download_manifest_provenance_reconstructed`|`src/ai_trading_system/data/quality.py::_validate_download_manifest`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_f4a908c0b9179da357b0`|`STATIC_LITERAL` / `prices_empty`|`src/ai_trading_system/data/quality.py::_validate_prices`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_41c56a119757eba4b1e7`|`STATIC_LITERAL` / `prices_invalid_date`|`src/ai_trading_system/data/quality.py::_validate_prices`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_08083cda85da879e6d77`|`STATIC_LITERAL` / `rates_empty`|`src/ai_trading_system/data/quality.py::_validate_rates`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_0e7f3d74bfa489801c83`|`STATIC_LITERAL` / `rates_invalid_date`|`src/ai_trading_system/data/quality.py::_validate_rates`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_f337897b3d0d0b8e2842`|`STATIC_LITERAL` / `rates_invalid_value`|`src/ai_trading_system/data/quality.py::_validate_rates`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_dcc6dcab7a17c225b404`|`STATIC_LITERAL` / `rates_non_finite_value`|`src/ai_trading_system/data/quality.py::_validate_rates`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_f2b42e78f99c3cc8a8f7`|`STATIC_LITERAL` / `DQ_WINDOW_INVALID`|`src/ai_trading_system/data/quality.py::_validate_requested_window`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_2ed5a14062cb8324f3c3`|`STATIC_LITERAL` / `DQ_WINDOW_INVALID`|`src/ai_trading_system/data/quality.py::_validate_requested_window`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_31659052988be60edab3`|`STATIC_LITERAL` / `DQ_WINDOW_NO_TRADING_SESSION`|`src/ai_trading_system/data/quality.py::_validate_requested_window`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_58c59a2cf6302269aa79`|`STATIC_LITERAL` / `requested_window_authority_manifest_conflict`|`src/ai_trading_system/data/quality.py::validate_data_cache`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_777a7638cf1cf58cb2b7`|`STATIC_LITERAL` / `requested_window_authority_mismatch`|`src/ai_trading_system/data/quality.py::validate_data_cache`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_06549b1beb061fe4cca9`|`STATIC_LITERAL` / `download_publication_invalid`|`src/ai_trading_system/data/quality.py::validate_data_cache`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_e11c6b4c80a2e3bd2e3a`|`STATIC_LITERAL` / `download_publication_required_for_requested_window`|`src/ai_trading_system/data/quality.py::validate_data_cache`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_7c449bf0a217ec6967e8`|`STATIC_LITERAL` / `download_manifest_required_for_requested_window`|`src/ai_trading_system/data/quality.py::validate_data_cache`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_d14a1be2ef88448b5f0e`|`STATIC_LITERAL` / `download_manifest_requested_window_mismatch`|`src/ai_trading_system/data/quality.py::validate_data_cache`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_b183f156da26b1194731`|`STATIC_LITERAL` / `DQ_MANIFEST_MISSING`|`src/ai_trading_system/data/quality_execution.py::_bindings_from_report`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_8d966c1e23d16a75ee5d`|`STATIC_LITERAL` / `DQ_INPUT_MISSING`|`src/ai_trading_system/data/quality_execution.py::_bindings_from_report`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_cf627128d7dec14a4bee`|`STATIC_LITERAL` / `DQ_INPUT_SET_MISMATCH`|`src/ai_trading_system/data/quality_execution.py::_bindings_from_report`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_5358fd636f76e282017f`|`STATIC_LITERAL` / `DQ_INPUT_SHA_MISMATCH`|`src/ai_trading_system/data/quality_execution.py::_bindings_from_report`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_b2cc27de17b9a65164cd`|`DYNAMIC_EXPRESSION` / `match.error_code`|`src/ai_trading_system/data/quality_execution.py::_bindings_from_report`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|
|`dq_issue_site_28d86d8f2b4887182d87`|`STATIC_LITERAL` / `DQ_WINDOW_MISMATCH`|`src/ai_trading_system/data/quality_execution.py::run_canonical_data_quality_execution`|`GLOBAL_OR_UNKNOWN_SCOPE`|`OWNER_REVIEW_REQUIRED`|`affected_instruments=false`|

## Non-canonical constructors

这些调用不在 capability attribution 权威内；数量可见用于防止扫描边界被误读。

|path|function|line|code expression|
|---|---|---:|---|
|`src/ai_trading_system/scoring/baseline_score_backfill.py`|`_backfill_data_quality_report`|322|`research_backfill_source_warning`|
|`src/ai_trading_system/scoring/baseline_score_backfill.py`|`_backfill_data_quality_report`|330|`research_backfill_source_failed`|

## 下一步

Source owner 必须逐 exact site/code 审查 attribution domain、source-wide/row-scoped taxonomy 和完整性生成方式。只有独立 reviewed contract wave 可以新增 typed schema 字段或扩大 allowed issue code。
