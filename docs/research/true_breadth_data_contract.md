# True Breadth Data Contract

配置：`config/research/true_breadth_data_contract.yaml`

## 硬要求

任何进入 model-ready breadth 的数据源必须同时满足：

- `historical_constituents`: required
- `daily_membership_query`: required
- `delisted_securities`: required
- `survivorship_bias_free`: required
- `primary_window_coverage.start`: `2021-02-22`
- `python_or_cli_access`: required
- `local_cache_allowed`: required

Holdings-based source 还必须能区分 `holding_date`、`reported_date` 和 `known_at`。
如果只能看到 holdings date 或当前 holdings，而不能确认报告日 / 可见日，则降级为
`PIT_WARNING_DIAGNOSTIC_ONLY`。

## 禁止方法

`current_constituents_backfill` 被显式禁止。当前成分股列表不能反推历史 membership，
否则会把已经退市或被剔除的证券排除在样本外，制造 survivorship bias。

Price-only source 也不能成为 true breadth source。它可以用于 ETF proxy price、
constituent price cross-check 或 data-quality validation，但不能提供 index
membership。

## 安全边界

本契约不批准任何策略研究重启。所有 downstream 使用在 trial 和 owner 审批前都固定：
`promotion_allowed=false`、`paper_shadow_allowed=false`、`production_allowed=false`、
`broker_action=none`。
