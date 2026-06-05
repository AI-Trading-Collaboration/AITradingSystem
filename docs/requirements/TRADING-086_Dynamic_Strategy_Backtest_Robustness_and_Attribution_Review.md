# TRADING-086 Dynamic Strategy Backtest, Robustness, and Attribution Review

- 父任务：TRADING-086
- 优先级：P0
- 状态：VALIDATING
- owner：system + project owner
- 创建日期：2026-06-05
- 来源计划：`G:/Download/TRADING-083_to_TRADING-087_Two_Layer_Dynamic_ETF_Allocation_Strategy_Roadmap.md`

## 背景

TRADING-083 建立 Layer 1 trend signal calibration，TRADING-084 建立 Layer 2
candidate-only dynamic allocation policy，TRADING-085 生成 two-layer dynamic
candidate packs 和 calibration proxy ranking。TRADING-086 需要用真实 ETF 价格数据
对 dynamic candidate 做完整 backtest、walk-forward、regime attribution、false signal
diagnostics、turnover sensitivity 和 overfit review，回答 dynamic allocation 是否比
static alternatives 更稳健。

TRADING-085 的 `dynamic_backtest` cache 只作为 calibration proxy，不能直接替代本任务的
robustness backtest。本任务必须显式披露价格数据来源、data quality gate 状态、默认
`ai_after_chatgpt` regime、requested / effective date range 和 no-lookahead timing。

## 安全边界

所有 TRADING-086 输出必须固定：

```text
observe_only = true
candidate_only = true
production_effect = none
broker_action = none
manual_review_required = true
production_state_mutated = false
baseline_config_mutated = false
official_target_weights_mutated = false
automatic_candidate_promotion = false
auto_enrollment_without_owner_approval = false
```

禁止：

```text
production_weight_update
baseline_config_mutation
official_target_weights_write
broker_order
automatic_candidate_promotion
auto_enrollment_without_owner_approval
```

## 子任务拆解

|子任务|状态|验收摘要|
|---|---|---|
|TRADING-086A Dynamic vs Static Backtest Comparison|BASELINE_DONE|比较 dynamic candidate、static base candidate、current ETF baseline、QQQ/SPY/SMH buy-and-hold 和 best static historical candidate。|
|TRADING-086B Walk-Forward Robustness Review|BASELINE_DONE|输出 walk-forward 窗口指标、pass/fail reason 和单窗口依赖风险。|
|TRADING-086C Regime-Specific Attribution|BASELINE_DONE|按 risk_on / neutral / risk_off / growth_underperformance / semiconductor_leadership / event_risk_high 等 regime 展示收益、drawdown、turnover 和 hit rate。|
|TRADING-086D False Risk-On / False Risk-Off Diagnostics|BASELINE_DONE|衡量 false risk-off opportunity cost 和 false risk-on drawdown cost。|
|TRADING-086E Turnover and Rebalance Sensitivity Review|BASELINE_DONE|分析 score threshold、minimum holding period、confirmation window 和 turnover cap 对结果的影响。|
|TRADING-086F AI / Semiconductor Contribution Attribution|BASELINE_DONE|拆分 QQQ、SMH、SOXX、CASH 等 sleeve contribution，披露 AI / semiconductor contribution source。|
|TRADING-086G Event Risk Overlay Attribution|BASELINE_DONE|衡量 event risk overlay 对 drawdown、cash exposure 和 opportunity cost 的影响。|
|TRADING-086H Dynamic Overfit Diagnostics|BASELINE_DONE|检查 single-period dependency、regime fragility、parameter sensitivity 和 allocation path instability。|
|TRADING-086I Dynamic Strategy Robustness Report|BASELINE_DONE|生成 JSON/Markdown report，包含 comparison、metrics、diagnostics、source artifacts 和 safety banner。|
|TRADING-086J Reader Brief Dynamic Robustness Section|BASELINE_DONE|Reader Brief 只读 latest robustness report，展示 status、top candidate、benchmark comparison、false-signal costs、overfit risk 和 safety。|
|TRADING-086K Dynamic Robustness Validation Gate|BASELINE_DONE|新增 `aits etf dynamic-robustness validate`，fail closed 校验 A-J workflow 和 safety boundary。|

## 设计约束

1. 从 cached price data 生成 robustness report 前必须运行 `aits validate-data` 等价质量门禁并停止于失败；报告必须披露 gate status 和 report path。
2. Backtest 必须使用 `signal_date < execution_date < return_date` 的 no-lookahead timing。
3. Dynamic candidate 不得写 `data/etf_portfolio/target_weights.csv`、production baseline config、broker state 或 shadow enrollment ledger。
4. 所有 thresholds、windows、score mapping、false-signal horizon、walk-forward window 和 sensitivity grid 必须来自 `config/etf_portfolio/dynamic_robustness.yaml`，并带 policy metadata/rationale。
5. Comparison 不能只按 total return 排名；必须同时披露 CAGR、max drawdown、volatility、Sharpe、Sortino、Calmar、turnover、capture、false-signal costs、risk-off drawdown reduction 和 overfit diagnostics。
6. Runtime artifacts 只能写入 ignored `reports/` 路径；源码变更只包含 policy/config、module、CLI、Reader Brief、registry、docs 和 tests。

## 验收命令

完成后至少运行：

```bash
python -m pytest tests/test_etf_dynamic_robustness.py tests/test_etf_dynamic_calibration.py tests/test_reader_brief.py tests/test_report_index.py -q
python -m ruff check config src tests scripts docs
python -m compileall -q src tests scripts
git diff --check
python -m ai_trading_system.cli etf dynamic-robustness validate
```

如最终 CLI 名称不同，必须同步更新本文件、`docs/task_register.md` 和
`docs/system_flow.md`。

## 进展记录

- 2026-06-05: 新增并进入 IN_PROGRESS。基于 TRADING-083_to_TRADING-087 roadmap 和 TRADING-085 handoff，开始实现 dynamic strategy robustness review：真实价格驱动 dynamic vs static backtest、walk-forward robustness、regime attribution、false risk-on/off diagnostics、turnover/rebalance sensitivity、AI/semiconductor contribution、event risk overlay attribution、overfit diagnostics、report、Reader Brief section 和 validation gate。本阶段不允许 production mutation、broker action、automatic promotion 或 auto-enrollment。
- 2026-06-05: A-K baseline 实现完成并转入 VALIDATING。新增 dynamic robustness policy、`aits etf dynamic-robustness report --candidate <candidate_id>` / `report --latest` / `validate`、真实价格驱动 daily dynamic path、dynamic/static/current/QQQ/SPY/SMH/best-static comparison、walk-forward、regime attribution、false risk-on/off diagnostics、turnover sensitivity、AI/semiconductor contribution、event risk overlay attribution、overfit diagnostics、JSON/Markdown report、Reader Brief `Dynamic Robustness Review` section、report registry/artifact catalog/system flow/runbook/README integration 和 focused tests。验证：`tests/test_etf_dynamic_robustness.py tests/test_etf_dynamic_calibration.py tests/test_reader_brief.py tests/test_report_index.py -q` 共 20 passed；`aits etf dynamic-robustness validate` 为 PASS；ruff、compileall、diff check 通过；真实 cached-data smoke 使用 `dynamic-candidate-pack_8904048b4dbd`、2022-12-01 至 2026-06-03，`validate_data_status=PASS_WITH_WARNINGS`，生成 `dynamic-robustness-report_98f2aa81bb60.json/md`，`status=REVIEW_REQUIRED`、`dynamic_total_return=1.1048362233046518`、`excess_vs_static_base=-0.42559745604739074`、`false_risk_off_count=217`、`false_risk_on_count=40`、`overfit_status=REVIEW_REQUIRED`、`shadow_enrollment_allowed=false`、`official_target_weights_mutated=false`。
- 2026-06-05: 全量 `python -m pytest tests -q` 长跑复核通过，结果为 2153 passed、330 warnings、耗时 641.51s；TRADING-086 继续保持 VALIDATING，剩余条件是真实 robustness report 的 owner 复核，并在 TRADING-087 前确认哪些 dynamic candidates 可进入 owner-approved shadow package。
