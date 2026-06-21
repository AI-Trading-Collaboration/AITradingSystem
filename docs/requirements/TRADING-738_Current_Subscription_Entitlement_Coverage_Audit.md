# TRADING-738 Current Subscription Entitlement & Data Coverage Audit

## 状态

- 状态：VALIDATING
- 日期：2026-06-21
- 范围：validation-only / observe-only subscription entitlement and coverage audit
- Owner：系统实现 + 项目 owner 后续复核

## 背景

TRADING-737 已把 `P0_remaining_count=9` 的 source-level requirement 明确为
provider/source proof、timestamp、manifest、as-of snapshot、corporate action /
revision policy 等缺口。TRADING-738 只审计当前本地 subscription / API key 是否
可能覆盖这些缺口，不尝试升级任何数据源状态。

## 安全边界

- validation-only / observe-only。
- 不输出、不记录、不提交任何 API key 原文。
- 只允许记录 `provider`、`key_present`、`endpoint_accessible`、
  `plan_or_limit_info_if_available`、`sanitized_error_class` 和
  `allowed_uses_candidate`。
- `production_effect=none`。
- `broker_action=none`。
- `promotion_gate_allowed=false`。
- `paper_shadow_change_allowed=false`。
- `production_weight_change_allowed=false`。
- 不修改 production、paper-shadow、official weights。
- 不触发 broker、order、live trading、paper-shadow activation。
- 不放宽 PIT、data-quality、lineage gate。
- 不尝试升级任何数据源状态。

## 输入

- 环境变量存在性：
  - `FINANCIAL_MODELING_PREP_API_KEY`
  - `FMP_API_KEY`
  - `MARKETSTACK_API_KEY`
  - `EODHD_API_KEY`
  - `ALPHA_VANTAGE_API_KEY`
  - `FRED_API_KEY`
  - `CONGRESS_API_KEY`
  - `GOVINFO_API_KEY`
- `outputs/data_quality/data_source_requirements/data_source_requirement_matrix.json`

## Representative Universe

- `SPY`
- `QQQ`
- `SMH`
- `MSFT`
- `GOOGL`
- `NVDA`
- `AMD`
- `TSM`
- `cash`

## 输出

- `outputs/data_quality/current_subscription_data_coverage/current_subscription_data_coverage_matrix.json/md`

## Endpoint Coverage 字段

- `endpoint_name`
- `accessible`
- `coverage_for_representative_universe`
- `historical_depth_observed`
- `raw_price_supported`
- `adjusted_price_supported`
- `splits_supported`
- `dividends_supported`
- `delisted_supported`
- `fundamentals_supported`
- `event_calendar_supported`
- `available_time_supported`
- `source_manifest_possible`
- `current_view_only_risk`
- `PIT_qualification_gap`
- `likely_allowed_use`

## Requirement Matching 字段

- `requirement_id`
- `can_current_subscription_cover`
- `provider_candidate`
- `endpoint_candidate`
- `remaining_gap`
- `requires_new_paid_source`

## 进度记录

- 2026-06-21：任务登记进入 IN_PROGRESS；固定只读 entitlement / coverage /
  remaining-gap audit 范围，不记录 key 原文，不升级 source status。
- 2026-06-21：实现 `aits data source-qualification subscription-audit`，
  默认 run 生成 `current_subscription_data_coverage_matrix.json/md`；
  `provider_count=9`、`endpoint_probe_count=29`、`accessible_endpoint_count=17`、
  `key_present_provider_count=6`、`requirement_match_count=9`、
  `requirements_current_subscription_cover_true_count=6`、
  `requirements_current_subscription_cover_unknown_count=2`、
  `requirements_current_subscription_cover_false_count=1`、
  `requires_new_paid_source_count=1`、`api_key_material_recorded=false`、
  `status_upgrade_attempted=false`，并保持 `production_effect=none`、
  `broker_action=none`、`promotion_gate_allowed=false`。
- 2026-06-21：key-material scan 对输出 JSON 通过，未发现本地环境变量中的 API key
  原文写入产物。
- 2026-06-21：focused validation 通过：
  `python -m pytest -n 16 --dist loadfile tests/test_current_subscription_data_coverage_audit.py tests/test_data_source_requirement_matrix.py tests/test_documentation_contract.py tests/test_report_index.py tests/test_artifact_lineage.py -q --durations=20 --durations-min=1`
  输出 33 passed。
- 2026-06-21：validation tier 通过：`fast-unit` 100 passed、
  `contract-validation` 99 passed、`report-validation` 55 passed
  （62 warnings，来自既有 numpy divide runtime warning）。
