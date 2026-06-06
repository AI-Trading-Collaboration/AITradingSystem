# TRADING-083 Trend Signal Weight Calibration

最后更新：2026-06-05

- 父任务：TRADING-083
- 优先级：P0
- 状态：VALIDATING
- owner：system + project owner
- 创建日期：2026-06-05
- 来源计划：`G:/Download/TRADING-083_to_TRADING-087_Two_Layer_Dynamic_ETF_Allocation_Strategy_Roadmap.md`

## 背景

TRADING-082 已把 static shadow-ready weight shapes 转成 owner-reviewed forward
shadow enrollment workflow。下一阶段进入 two-layer dynamic ETF allocation roadmap：
先独立校准 trend-analysis information weights，再由后续 TRADING-084 把 trend /
regime / risk scores 映射成动态 ETF allocation。

TRADING-083 只回答 trend signal inputs 是否有 forward explanatory value，以及
如何合成 candidate-only `CompositeTrendScore`。它不得输出 target weights，不得
修改 baseline config，也不得触发 broker action。

## 安全边界

所有 TRADING-083 输出必须固定：

```text
observe_only = true
candidate_only = true
production_effect = none
broker_action = none
manual_review_required = true
evaluation_only = true for forward return fields
```

禁止：

```text
target_weights
production_weights
baseline_config_mutation
production_weight_update
broker_order
automatic_candidate_promotion
```

## 子任务拆解

|子任务|状态|验收摘要|
|---|---|---|
|TRADING-083A Trend Signal Input Registry|BASELINE_DONE|新增 config-driven signal registry，声明 source、direction、normalization、bounds、quality requirement 和 safety。|
|TRADING-083B Trend Signal Dataset Builder|BASELINE_DONE|从 ETF feature / price-derived signals 构建 evaluation-only historical dataset，含 forward return / drawdown windows。|
|TRADING-083C Trend Signal Weight Config Schema|BASELINE_DONE|定义 bounded trend signal weight set、score band、risk penalty 和 config hash。|
|TRADING-083D Trend Score Computation Engine|BASELINE_DONE|输出 `CompositeTrendScore`、`RiskRegimeScore`、`GrowthLeadershipScore`、`SemiconductorLeadershipScore`、`EventRiskAdjustedTrendScore`。|
|TRADING-083E Trend Signal Weight Search Runner|BASELINE_DONE|支持 preset templates / bounded coarse grid / top-N export，不做 unbounded continuous optimization。|
|TRADING-083F Trend Score Bucket Forward Attribution|BASELINE_DONE|按 score buckets 比较 QQQ / SMH / QQQ-SPY forward return、drawdown 和 volatility。|
|TRADING-083G Trend Signal Redundancy Diagnostics|BASELINE_DONE|输出 correlation / rank correlation / redundancy warnings。|
|TRADING-083H Trend Signal Regime Stability Review|BASELINE_DONE|比较 ai_after_chatgpt、risk_on/off、growth leadership、semiconductor leadership 等 regime slices。|
|TRADING-083I Trend Signal Config Registry|BASELINE_DONE|保存 candidate-only selected configs，不写 production config。|
|TRADING-083J Trend Calibration Report|BASELINE_DONE|生成 JSON/Markdown report，包含 safety banner、coverage、top configs、attribution、redundancy、regime stability 和 source links。|
|TRADING-083K Reader Brief Trend Calibration Section|BASELINE_DONE|Reader Brief 只读 latest report，展示 top config / evidence status / redundancy risk / regime stability / detail link。|
|TRADING-083L Trend Calibration Validation Gate|BASELINE_DONE|新增 `aits etf trend-calibration validate`，fail closed 校验 A-K availability 和 safety boundary。|

## 设计约束

1. Trend calibration 与 allocation calibration 分离；TRADING-083 不输出 ETF target weights。
2. Forward returns 只能用于 evaluation / attribution，不得进入 decision-time record。
3. 所有 heuristics、thresholds、score bands 和 search weights 必须在 policy config 中带 rationale、validation 和 review condition。
4. 从 cached price data 生成 dataset / report 的 CLI 必须先运行 `validate-data` 等价质量门禁并在输出中披露 status。
5. 默认市场 regime 为 `ai_after_chatgpt`，默认 evaluation start 不早于 2022-12-01；若使用更早数据，只能作为 warm-up 或 regime comparison。

## 验收命令

完成后至少运行：

```bash
python -m pytest tests/test_etf_trend_calibration.py tests/test_reader_brief.py tests/test_report_index.py -q
python -m ruff check config src tests scripts docs
python -m compileall -q src tests scripts
git diff --check
python -m ai_trading_system.cli etf trend-calibration validate
```

如最终 CLI 名称不同，必须同步更新本文件、`docs/task_register.md` 和 `docs/system_flow.md`。

## 进展记录

- 2026-06-05: 新增并进入 IN_PROGRESS。基于 TRADING-083_to_TRADING-087 roadmap，先做 two-layer dynamic ETF allocation 的 Layer 1：trend signal weight calibration。本阶段只做 signal dataset、score config、bounded weight search、forward attribution、redundancy / regime stability review、report / Reader Brief / validation gate，不允许 target weights、production mutation、broker action 或 automatic promotion。
- 2026-06-05: TRADING-083A-L baseline 实现完成并转入 VALIDATING。新增 trend calibration policy、`aits etf trend-calibration run/report/validate`、data quality gate enforcement、evaluation-only dataset、score computation engine、bounded preset search、bucket forward attribution、redundancy diagnostics、regime stability review、candidate config registry、JSON/Markdown report、Reader Brief `Trend Signal Calibration` section、report registry、artifact catalog/system flow/runbook/README integration 和 focused tests。验证：`tests/test_etf_trend_calibration.py tests/test_reader_brief.py tests/test_report_index.py` 共 14 passed，`aits etf trend-calibration validate` 为 PASS，ruff、compileall 和 `git diff --check` 通过；剩余验证条件是 owner 复核真实 trend calibration report，并在 TRADING-084 dynamic allocation policy 前确认 Layer 1 signal config 的可解释性。
- 2026-06-05: 全量 `python -m pytest tests -q` 长跑复核通过，结果为 2141 passed、330 warnings、耗时 644.64s；TRADING-083 继续保持 VALIDATING，剩余条件不变：owner 复核真实 trend calibration report，并在 TRADING-084 前确认 Layer 1 signal config。
- 2026-06-05: 真实 cached-data smoke 通过。`aits etf trend-calibration run --start 2022-12-01 --end 2026-06-03 --top 5` 先生成 `validate_data_status=PASS_WITH_WARNINGS` 的 data quality report，然后输出 dataset、report 和 candidate config registry，`status=PASS`、`top_config=trend_balanced_v0_1`、`evaluation_only=true`、`production_effect=none`、`broker_action=none`；`aits etf trend-calibration report --latest` 成功只读摘录 latest report，显示 `evidence_status=PASS`、`redundancy_risk=high`、`regime_stability=usable`。
