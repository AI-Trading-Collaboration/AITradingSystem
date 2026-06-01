# TRADING-057 Market Data Freshness & Tracking Readiness Gate

最后更新：2026-05-30

## 背景

TRADING-056 已完成 shadow candidate tracking 和 daily roll-forward。真实 latest 结果为：

- `tracking_date=2026-05-29`
- `effective_data_date=2026-05-28`
- `tracking_status=degraded_tracking`
- `data_gate=OK`
- `production_effect=none`

这说明 candidate tracking 已能运行，但系统还缺少一个专门解释 market data freshness 与
tracking readiness 的判断层，无法区分正常数据延迟、非交易日、cache/manifest 未刷新、
数据源延迟、真实缺失和需要阻断 tracking 的数据异常。

## 目标

1. 判断 tracking date 的 U.S. market calendar 状态。
2. 判断 latest required market data 是否应当已经可用。
3. 区分 `OK`、`ACCEPTABLE_LAG`、`NON_TRADING_DAY`、`STALE`、`MISSING`、
   `SOURCE_DELAYED`、`MARKET_CALENDAR_UNKNOWN` 和 `FAILED`。
4. 输出 `artifacts/data_freshness/YYYY-MM-DD/market_data_freshness_summary.json/md`。
5. 新增 CLI：
   - `aits data freshness --latest`
   - `aits data freshness --date YYYY-MM-DD`
   - `aits data validate-freshness --latest`
   - `aits reports data-freshness --latest`
6. Portfolio candidate tracking 读取 latest freshness report，并把 freshness status 写入
   tracking summary。
7. Shadow backtest dry-run 只读引用 market data freshness artifact，但 promotion 仍保持
   rejected / observe-only。
8. Dashboard 和 Reader Brief 展示 market data freshness / tracking readiness 摘要。

## 非目标

- 不修改 `config/parameters/production/current.yaml`。
- 不解除 candidate promotion 或启用真实交易。
- 不下载、补造或伪造缺失价格。
- 不降低 `aits validate-data` 数据质量门禁。
- 不因为 freshness OK 自动允许 production promotion。

## 设计要点

- 使用 `config/data/market_data_freshness.yaml` 记录 market timezone、close time、ready
  window、可接受 lag 和 required assets。
- 复用现有 U.S. equity trading calendar 与 backtest manifest / registry consistency
  逻辑，避免 freshness、tracking 和 backtest 对 latest/effective date 形成不同口径。
- `--latest` 使用 raw price cache 的 latest date 作为 tracking date；effective data date
  取 required-asset common date 与 latest valid backtest manifest date 的可审计交集。
- trading day 且运行时间早于 expected data ready time 时，前一交易日数据可判定为
  `ACCEPTABLE_LAG`；超过 ready window 后仍滞后则为 `STALE`。
- 非交易日使用最近前一交易日数据时判定为 `NON_TRADING_DAY`。
- required asset 在 effective data date 不可用或 manifest/registry 无法支撑 tracking 时
  判定为 `MISSING` 或 `FAILED`。

## 验收标准

- `aits data freshness --latest` 生成 JSON 和 Markdown freshness report。
- `aits data validate-freshness --latest` 输出 freshness status、安全字段和 tracking
  readiness，并在 blocking 状态下 fail closed。
- `aits portfolio track-candidate --latest` 输出 `market_data_freshness_status`，并按
  freshness readiness 给出 `active_tracking`、`degraded_tracking` 或 `tracking_blocked`。
- `aits reports data-freshness --latest` 能读取 latest freshness report。
- `aits parameters shadow-backtest --latest --dry-run` 仍为 `promotion_status=rejected`，
  `production_effect=none`、`manual_review_required=true`、`auto_promotion=false`，并可引用
  market data freshness 与 candidate tracking artifacts。
- Dashboard / Reader Brief 展示 market data freshness 摘要。
- 新增专项测试覆盖 OK、acceptable lag、weekend/non-trading day、stale、missing asset、
  unknown calendar、JSON/Markdown、candidate tracking、dashboard、Reader Brief、shadow
  backtest artifact 和 production config unchanged。
- 验证通过：
  - `python -m pytest -q`
  - `python -m ruff check scripts src tests`
  - `python -m compileall src scripts`
  - `git diff --check`

## 进展记录

- 2026-05-30: 新增并进入 `IN_PROGRESS`。原因：TRADING-056 latest tracking 可运行但因
  `tracking_date=2026-05-29`、`effective_data_date=2026-05-28` 进入 degraded，需要先把
  run date / tracking date / effective data date 的 readiness 判断正式纳入系统。
- 2026-05-30: 实现完成并进入 `VALIDATING`。已新增
  `config/data/market_data_freshness.yaml`、`market_data_freshness` 核心模块、CLI
  `data freshness / validate-freshness`、report alias `reports data-freshness`、
  portfolio candidate tracking readiness gate、shadow backtest supporting artifact、
  Dashboard/Reader Brief 摘要、report registry、artifact catalog、system flow 更新和专项测试。
- 2026-05-30: 真实 latest 验收完成。`aits data freshness --latest` 生成
  `artifacts/data_freshness/2026-05-29/market_data_freshness_summary.json/md`，状态为
  `STALE`，`tracking_date=2026-05-29`、`effective_data_date=2026-05-28`、
  `tracking_readiness=cannot_track`、suggested action 为刷新 market data cache 和 manifest。
  `aits portfolio track-candidate --latest` 按 readiness gate 输出
  `tracking_status=tracking_blocked`，production promotion 仍 disabled。
- 2026-05-30: 验证通过 `python -m pytest -q`（1509 passed）、
  `python -m ruff check scripts src tests`、`python -m compileall src scripts` 和
  `git diff --check`。`config/parameters/production/current.yaml` 无 diff。
