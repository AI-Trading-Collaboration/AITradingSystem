# TRADING-012：PaperBroker Fill Model Calibration Diagnostics

最后更新：2026-05-17

关联任务：`TRADING-012`

## 背景

`TRADING-011` / `TRADING-011A` 已完成 Local `PaperBroker` vs IBKR Paper
lifecycle / fill comparison，本机样本为：

- `comparison_status=PASS`
- `status_match=true`
- `fill_match=true`
- `cancel_match=true`
- `fills_seen=false`
- `difference_labels=[]`

该样本只证明基础 `LIMIT` / `DAY` open-cancel lifecycle 对齐，不能证明 fill
model 已被真实 broker 行为验证。本任务新增 calibration diagnostics，先收集和解释
差异，不修改 `PaperBroker` fill 行为。

## 范围

1. 新增只读报告：
   - `outputs/reports/paperbroker_fill_model_calibration_YYYY-MM-DD.json`
   - `outputs/reports/paperbroker_fill_model_calibration_YYYY-MM-DD.md`
2. 新增人工显式脚本：
   `python scripts/run_paperbroker_fill_model_calibration.py --date YYYY-MM-DD`
3. 报告固定声明：
   - `production_effect=none`
   - `calibration_mode=diagnostic_only`
   - 不影响 replay、paper signal quality、shadow impact、production conclusion、
     参数晋级或交易建议。
4. 第一版不接 dashboard 大表；如后续接入 dashboard，只允许展示轻量摘要。

## 输入

第一版只读读取本地 artifacts：

- 最近 N 个 `paperbroker_vs_ibkr_paper_comparison_*.json`
- 最近 N 个 `ibkr_paper_controlled_fill_*.json`，其中 `fill_seen=false` 且最终
  `Cancelled` 的样本只作为 no-fill lifecycle evidence
- 可选最近已有 `paper_trading_replay_*.json` 的 `quality_flags`
- 可选同日或最近已有 `paper_signal_quality_*.json`

读取上限、最小 comparison 样本数和状态集合由
`config/paperbroker_fill_model_calibration_policy.yaml` 记录。报告不得读取 broker
API key，不调用 IBKR、不调用 broker adapter、不触发 paper runner 或 replay runner。

## 诊断指标

报告至少输出：

- `comparison_count`
- `controlled_fill_count`
- `calibration_evidence_count`
- `lifecycle_match_count`
- `status_match_ratio`
- `fill_match_ratio`
- `cancel_match_ratio`
- `local_filled_but_ibkr_not_filled_count`
- `ibkr_rejected_but_local_accepted_count`
- `broker_rejected_count`
- `insufficient_market_data_count`
- `synthetic_snapshot_related_count`
- `no_fill_lifecycle_only_count`
- `controlled_fill_no_fill_lifecycle_validated_count`
- `no_fill_lifecycle_validated_count`

## Calibration Gate

`calibration_status` 只允许：

- `INSUFFICIENT_SAMPLE`
- `LIFECYCLE_ALIGNED_FILL_UNTESTED`
- `LOCAL_SIM_TOO_OPTIMISTIC`
- `BROKER_REJECTION_GAP`
- `OBSERVE_ONLY`

禁止输出 `READY_FOR_LIVE`、`SHOULD_TRADE`、`PROMOTE`、`APPROVED` 或任何等价生产 /
实盘 / 晋级语义。

本机当前 `fills_seen=false` 的 comparison 样本应输出
`LIFECYCLE_ALIGNED_FILL_UNTESTED` 或 `INSUFFICIENT_SAMPLE`，不得输出已验证 fill model
的结论。

Controlled fill no-fill 样本如果满足 `fill_seen=false`、`cancel_requested=true` 且
`final_order_status=Cancelled`，只能归类为 `NO_FILL_LIFECYCLE_VALIDATED`。该分类证明
submit/open/cancel 链路被观察到，仍必须保持 `fill_tested=false`，不得把 controlled
fill 脚本成功运行解释为 fill model 已验证。

## Recommendations

如果只有 open/cancel lifecycle 样本，recommendations 固定包含：

- `lifecycle aligned for basic LIMIT DAY cancel path`
- `fill model remains unvalidated`
- `collect near-market or controlled small-fill IBKR Paper samples later`
- `do not modify PaperBroker fill model yet`

出现 `LOCAL_SIM_TOO_OPTIMISTIC` 或 broker rejection gap 时，recommendations 只能提出后续
诊断、样本收集和映射补充，不得直接调整 `PaperBroker` fill model。

## 实施步骤

1. 新增 policy manifest、任务登记、需求文档和系统流图 / artifact catalog 更新。
2. 新增 `paperbroker_fill_model_calibration` report builder 和 CLI。
3. 新增目标测试，覆盖无样本、no-fill lifecycle、local optimistic、broker rejection、
   production boundary、危险语义缺失和无 broker/replay 调用。
4. 运行目标测试、`tests/trading_engine`、全量 pytest、ruff、black。
5. 提交并 push 后确认 GitHub Actions 对本 commit 通过。

## 状态记录

- 2026-05-17：新增并进入实现。原因：owner 要求在 TRADING-011/011A 已完成且
  当前 comparison 样本没有 fill 的情况下，先建立 diagnostic-only calibration
  report，用于后续判断是否需要调整 fill model；本阶段不修改 `PaperBroker` 行为。
- 2026-05-17：实现完成并进入验证。已新增
  `config/paperbroker_fill_model_calibration_policy.yaml`、
  `scripts/run_paperbroker_fill_model_calibration.py`、calibration JSON/Markdown
  writer、系统流图、artifact catalog 和目标测试。本机真实 artifact 运行输出
  `calibration_status=LIFECYCLE_ALIGNED_FILL_UNTESTED`、`comparison_count=1`、
  `lifecycle_match_ratio=100%`、`fill_tested=false`；recommendations 明确
  `do not modify PaperBroker fill model yet`。本地验证通过
  `python -m pytest tests/trading_engine/test_paperbroker_fill_model_calibration.py`、
  `python -m pytest tests/trading_engine`、`python -m pytest`、
  `python -m ruff check scripts src tests` 和
  `python -m black --check scripts src tests`。
- 2026-05-18：TRADING-013A 扩展 calibration 语义。Calibration 现在可只读读取
  `ibkr_paper_controlled_fill_*.json` no-fill/cancelled artifact，并将其分类为
  `NO_FILL_LIFECYCLE_VALIDATED`；该分类不改变 `PaperBroker` fill model，不调用
  broker，不触发 runner/replay，且继续保持 `fill_tested=false` 和
  `LIFECYCLE_ALIGNED_FILL_UNTESTED`。
