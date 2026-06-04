# TRADING-085 Two-Layer Dynamic Candidate Batch Runner and Cache

- 父任务：TRADING-085
- 优先级：P0
- 状态：VALIDATING
- owner：system + project owner
- 创建日期：2026-06-05
- 来源计划：`G:/Download/TRADING-083_to_TRADING-087_Two_Layer_Dynamic_ETF_Allocation_Strategy_Roadmap.md`

## 背景

TRADING-083 已建立 Layer 1 trend signal weight calibration，TRADING-084 已建立
Layer 2 dynamic allocation policy engine。TRADING-085 需要把二者组合成可批量迭代、
可缓存、可排序、可审计的 candidate packs，为后续 TRADING-086 robustness review 和
TRADING-087 dynamic shadow workflow 提供输入。

本任务可以生成 two-layer dynamic candidate pack、allocation decision path、dynamic
backtest summary 和 ranking evidence，但不得写 official ETF target weights、不得
修改 baseline config、不得触发 broker，也不得自动 enrollment。

## 安全边界

所有 TRADING-085 输出必须固定：

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
|TRADING-085A Two-Layer Candidate Pack Schema|BASELINE_DONE|新增 config-driven candidate pack schema，记录 trend signal config、dynamic allocation policy、data range preset、cache keys 和 safety。|
|TRADING-085B Two-Stage Search Protocol|BASELINE_DONE|实现 Stage 1 trend config evaluation、Stage 2 allocation policy evaluation 和 Stage 3 bounded local refinement 的可审计记录。|
|TRADING-085C Coarse-to-Fine Candidate Iteration|BASELINE_DONE|支持 coarse / top-N / local_refinement candidate pack metadata 和 ranking。|
|TRADING-085D Trend Score Cache Extension|BASELINE_DONE|缓存 trend score series、score bands 和 regime labels。|
|TRADING-085E Allocation Path Cache|BASELINE_DONE|缓存 daily candidate weights、rebalance decisions、constraint diagnostics 和 reason codes。|
|TRADING-085F Dynamic Backtest Cache|BASELINE_DONE|缓存 portfolio return、drawdown、turnover、benchmark metrics 和 regime metrics；当前为 calibration proxy，完整 robustness 属于 TRADING-086。|
|TRADING-085G Parallel Two-Layer Batch Runner|BASELINE_DONE|CLI 支持 `--workers auto/N` 和 cache mode，并在 report 中披露 worker/cache summary。|
|TRADING-085H Two-Layer Candidate Ranking|BASELINE_DONE|按 risk-adjusted return、drawdown reduction、turnover、regime robustness、false signal penalty、data quality 和 overfit risk 排序。|
|TRADING-085I Two-Layer Batch Report|BASELINE_DONE|生成 JSON/Markdown report，展示 top packs、static/benchmark comparison、cache performance、ranking reasons 和 blockers。|
|TRADING-085J Two-Layer Batch Validation Gate|BASELINE_DONE|新增 `aits etf dynamic-calibration validate`，fail closed 校验 A-I availability 和 safety boundary。|

## 设计约束

1. TRADING-085 必须复用 TRADING-083 trend score / registry evidence 和 TRADING-084 dynamic allocation decision builder，不复制 production allocation logic。
2. Cache keys 必须包含 data/config/model/engine/policy hashes，且 cache hit/miss/write 在 report 中可见。
3. Candidate ranking 不能只按 return 排序，必须披露 drawdown、turnover、regime robustness、false signal penalty、data quality 和 overfit risk component。
4. Runtime artifacts 只能写入 ignored `reports/` 和 `data/cache/` 路径，不写 production config、official target weights 或 broker state。
5. 默认市场 regime 仍为 `ai_after_chatgpt`，报告必须披露 requested / resolved date range。

## 验收命令

完成后至少运行：

```bash
python -m pytest tests/test_etf_dynamic_calibration.py tests/test_etf_dynamic_allocation.py tests/test_reader_brief.py tests/test_report_index.py -q
python -m ruff check config src tests scripts docs
python -m compileall -q src tests scripts
git diff --check
python -m ai_trading_system.cli etf dynamic-calibration validate
```

如最终 CLI 名称不同，必须同步更新本文件、`docs/task_register.md` 和
`docs/system_flow.md`。

## 进展记录

- 2026-06-05: 新增并进入 IN_PROGRESS。基于 TRADING-083_to_TRADING-087 roadmap，开始实现 two-layer dynamic candidate batch/cache baseline：candidate pack schema、two-stage search protocol metadata、coarse-to-fine iteration、trend score cache、allocation path cache、dynamic backtest cache、parallel runner visibility、ranking、report 和 validation gate。本阶段不允许 official target weights write、baseline mutation、broker action、automatic promotion 或 auto-enrollment。
- 2026-06-05: A-J baseline 实现完成并转入 VALIDATING。新增 dynamic calibration policy、`aits etf dynamic-calibration run/report/validate`、candidate pack schema、two-stage search metadata、coarse/local refinement candidate generation、trend score cache、allocation path cache、dynamic calibration proxy cache、cache manifest wrapper、worker/cache visibility、evidence-based ranking、JSON/Markdown report、Reader Brief `Dynamic Calibration Batch` section、report registry/artifact catalog/system flow/runbook/README integration 和 focused tests。验证：`tests/test_etf_dynamic_calibration.py tests/test_etf_dynamic_allocation.py tests/test_reader_brief.py tests/test_report_index.py -q` 共 19 passed；`aits etf dynamic-calibration validate` 为 PASS；ruff、compileall 初步通过；真实 latest-trend-report smoke 生成 24 个 candidate packs，`status=PASS`、`data_quality_status=PASS_WITH_WARNINGS`、`top_ranking_score=51.071078`、`calibration_proxy=true`、`full_robustness_backtest_required=true`、`official_target_weights_mutated=false`；warm rerun `cache_hit_rate=1.0`、`cache_write_count=0`。剩余条件是 owner 复核真实 batch report，并在 TRADING-086 前确认哪些 candidate packs 进入完整 robustness review。
- 2026-06-05: 全量 `python -m pytest tests -q` 长跑复核通过，结果为 2149 passed、330 warnings、耗时 620.63s；TRADING-085 继续保持 VALIDATING，剩余条件不变：owner 复核真实 dynamic calibration batch report，并在 TRADING-086 前确认 candidate pack 输入。
