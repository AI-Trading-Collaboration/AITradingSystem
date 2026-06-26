# Manual External Record Input Guide

本指南用于记录 Portfolio Visualizer、testfol.io 或其他外部组合回测平台的静态 baseline 结果。只填写真实外部平台导出、截图或手工记录，不得把内部系统结果复制为外部证据。

- YAML template: `inputs/external_validation/manual_external_records/static_baseline_external_records.template.yaml`
- CSV template: `inputs/external_validation/manual_external_records/static_baseline_external_records.template.csv`
- Required date range: `2022-12-01` to current internal `as_of` / available date range
- Required baselines: `100_qqq`, `qqq_50_sgov_50`, `qqq_60_sgov_40`
- Required evidence: fill at least one of `screenshot_reference` or `export_file_path`.
- SGOV convention must be one of `unknown`, `price_only`, `adjusted`, `total_return`, `platform_default`.

## Metric Fields

For `annual_return`, `max_drawdown`, `sharpe`, `calmar`, and `turnover`, enter the platform value when available. If the platform does not provide that metric, enter `metric_unavailable_on_platform` exactly.

## Safety Boundary

- `paper_shadow_allowed=false`
- `production_allowed=false`
- `broker_action=none`
- `manual_review_required=true`

Do not upload broker account information or personal account screenshots.
