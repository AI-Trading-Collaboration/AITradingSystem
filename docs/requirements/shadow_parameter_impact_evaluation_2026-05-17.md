# TRADING-008：Shadow Parameter Impact Evaluation

最后更新：2026-05-17

关联任务：`TRADING-008`、`TRADING-008A`

## 背景

当前系统已有 shadow parameter search、shadow iteration、paper trading summary、
continuous replay 和 paper signal quality。TRADING-008 的目标不是接真实券商、
扩展订单类型或晋级参数，而是只读评估 shadow 参数相对 production 参数在
paper trading / continuous replay / signal quality 层面是否有可观察改善。

## 范围

1. 新增 observe-only impact report：
   - `outputs/reports/shadow_parameter_impact_YYYY-MM-DD.json`
   - `outputs/reports/shadow_parameter_impact_YYYY-MM-DD.md`
   - 顶层 `production_effect=none`。
   - 不修改 production 参数、仓位建议、参数晋级、正式 ledger 或交易执行。
2. 输入读取：
   - 最近 7 / 14 / 30 日 `order_intent_candidates_YYYY-MM-DD.json`。
   - 最近 7 / 14 / 30 日 `paper_trading_summary_YYYY-MM-DD.json`。
   - 最近 7 / 14 / 30 日 `paper_signal_quality_YYYY-MM-DD.json`。
   - 可选已有 `paper_trading_replay_START_END.json`。
   - 对输入中的 `source_profile` / `mode` / `strategy_version` 归类为
     `production`、`shadow` 或 `unknown`。
3. 窗口比较：
   - candidate_count、generated_intents、approved / rejected、submitted /
     filled / open / cancelled。
   - realized / unrealized paper PnL。
   - continuous replay 存在时展示 final_equity 和 max_drawdown。
   - synthetic_snapshot_ratio、historical_ohlc_coverage、
     reconciliation_pass_ratio。
   - blocked_by、reason_code 和 confidence bucket performance 分布。
4. Impact gate：
   - 不允许因为短期 paper PnL 为正就输出 shadow 更好。
   - gate reason 至少覆盖 `insufficient_shadow_sample`、
     `insufficient_production_baseline`、`low_data_quality`、
     `synthetic_snapshot_ratio_too_high`、`daily_independent_only` 和
     `unreliable_reconciliation`。
   - 顶层 `impact_status` 只能是 `INSUFFICIENT_DATA`、`OBSERVE_ONLY`、
     `SHADOW_PROMISING_BUT_LIMITED`、`NO_CLEAR_IMPROVEMENT` 或
     `SHADOW_UNRELIABLE`。
   - 禁止输出上线、实盘、交易批准或参数 promotion 语义。
5. Dashboard：
   - daily task dashboard 新增 Shadow Impact 轻量卡片。
   - 只展示 impact_status、主要 blocked_by / warnings、7/14/30 日样本数、
     production vs shadow filled_count、production vs shadow paper PnL summary、
     continuous replay availability 和 report link。
   - dashboard 只读读取 JSON，不触发 replay、paper runner、broker 或参数晋级。

## 边界

- 不接真实券商。
- 不扩展订单类型。
- 不读取 broker API key。
- 不调用 IBKR / Alpaca stub 或任何真实 broker adapter。
- 不改变 production scoring、position gate、正式 prediction ledger、
  approved overlay、参数晋级或生产仓位建议。
- paper PnL、continuous replay 和 signal quality 都只是诊断证据，不是上线依据。

## 验收标准

- `python -m pytest tests/trading_engine`
- `python -m pytest tests/test_daily_task_dashboard.py`
- `python -m pytest`
- `python -m ruff check scripts src tests`
- `python scripts/run_paper_trading_replay.py --start 2026-05-01 --end 2026-05-17 --mode continuous-portfolio`

## 实施步骤

1. 新增 shadow impact policy manifest 和 report 生成模块。
2. 新增 standalone script，输出 JSON / Markdown。
3. 扩展 daily task dashboard 的只读 Shadow Impact 卡片。
4. 更新系统流图、artifact catalog 和任务登记。
5. 增加 trading_engine 与 dashboard 测试。
6. 运行验收命令，记录结果。

## 状态记录

- 2026-05-17：新增并进入实现。原因：owner 要求推进 TRADING-008，评估
  shadow 参数相对 production 参数在 paper trading、continuous replay 和
  signal quality 上是否有可观察改善；范围明确为 observe-only，
  `production_effect=none`，不接真实券商、不扩展订单类型、不改变生产结论。
- 2026-05-17：实现完成并进入验证。已新增
  `config/shadow_parameter_impact_policy.yaml`、
  `scripts/run_shadow_parameter_impact.py`、`shadow_parameter_impact_YYYY-MM-DD.json/md`
  生成器、impact gate、7/14/30 日 profile 对比、dashboard Shadow Impact
  只读卡片、系统流图/产物目录和测试；本地验证通过 `python -m pytest
  tests/trading_engine`、`python -m pytest tests/test_daily_task_dashboard.py`、
  `python -m pytest`、`python -m ruff check scripts src tests`、`python
  scripts/run_paper_trading_replay.py --start 2026-05-01 --end 2026-05-17
  --mode continuous-portfolio`、`python scripts/run_shadow_parameter_impact.py
  --date 2026-05-17 --replay-json
  outputs/reports/paper_trading_replay_2026-05-01_2026-05-17.json` 和
  `python -m ai_trading_system.cli docs validate-freshness`。当前真实本地样本
  输出 `INSUFFICIENT_DATA`，主要原因是最近窗口没有 shadow 样本。

## TRADING-008A：Shadow Impact Hardening

### 背景

基础版已能生成 observe-only impact JSON/Markdown 并接入 dashboard，但需要进一步
收紧输出语义，避免短样本 paper PnL、daily-independent replay 或 dashboard 摘要被
误读为参数上线、真实交易、交易批准或 production 晋级证据。

### 验收标准

1. 前置 CI：GitHub Actions `CI` run `25986926245` 对 commit
   `a720b95436f8481011145553399c6ec0b01c2e4c` 已完成且通过。
2. `scripts/run_shadow_parameter_impact.py`、
   `src/ai_trading_system/trading_engine/reports/shadow_parameter_impact.py` 和
   `tests/trading_engine/test_shadow_parameter_impact.py` 必须通过 Black，且不得退化为
   超长单行。
3. `impact_status` 只能使用 `INSUFFICIENT_DATA`、`OBSERVE_ONLY`、
   `SHADOW_PROMISING_BUT_LIMITED`、`NO_CLEAR_IMPROVEMENT`、
   `SHADOW_UNRELIABLE`、`LOW_DATA_QUALITY`。
4. JSON、Markdown 和 dashboard 输出不得出现 production promotion、live trade、
   should-trade 或 approval 危险语义。
5. JSON 必须包含 `policy_id`、`policy_version`、thresholds snapshot 和
   `production_effect=none`；Markdown 展示 policy version 和关键 thresholds。
6. `impact_gate` 必须提供 human-readable explanation，且每个 blocked_by / warning
   都有解释。
7. 使用 continuous replay 时记录 replay artifact path、mode 和 date range；缺失时写入
   `continuous_replay_missing` warning；daily-independent replay 不得被当作 continuous
   portfolio 结果。
8. Dashboard 只展示摘要，不展示长表，不触发 replay、paper runner、broker、参数晋级，
   也不读取 broker API key。

### 状态记录

- 2026-05-17：新增并进入实现。前置 CI 已通过；本轮目标是收紧状态枚举、policy
  snapshot、impact gate 解释、continuous replay 来源披露、dashboard 只读边界和危险语义
  测试。生产权重、production gate、approved overlay、正式 ledger、交易执行和真实券商
  能力均不在范围内。
- 2026-05-17：实现完成并进入验证。已新增 `LOW_DATA_QUALITY` impact status、
  `continuous_replay_missing` warning、blocked_by / warning explanation、continuous replay
  `source_artifact` path/mode/date range、daily-independent replay 非 continuous 防护，以及
  JSON/Markdown/dashboard 危险语义扫描测试；dashboard 仍只读展示摘要，不触发 replay、
  broker 或参数晋级。验证通过 `python -m pytest
  tests/trading_engine/test_shadow_parameter_impact.py`、`python -m pytest
  tests/trading_engine`、`python -m pytest tests/test_daily_task_dashboard.py`、`python -m pytest`、
  `python -m ruff check scripts src tests` 和 `python -m black --check scripts src tests`。
