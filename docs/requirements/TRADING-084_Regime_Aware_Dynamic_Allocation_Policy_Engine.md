# TRADING-084 Regime-Aware Dynamic Allocation Policy Engine

- 父任务：TRADING-084
- 优先级：P0
- 状态：VALIDATING
- owner：system + project owner
- 创建日期：2026-06-05
- 来源计划：`G:/Download/TRADING-083_to_TRADING-087_Two_Layer_Dynamic_ETF_Allocation_Strategy_Roadmap.md`

## 背景

TRADING-083 已实现 Layer 1 trend signal weight calibration，并保持
trend-analysis information weights 与 allocation weights 分离。TRADING-084 进入
Layer 2：把 trend / regime / risk / AI / semiconductor / event risk scores 映射成
candidate-only ETF dynamic allocation decision records。

本任务可以输出 `candidate_target_weights` 作为人工复核对象，但不得写官方
`data/etf_portfolio/target_weights.csv`，不得修改 baseline config，也不得触发 broker。

## 安全边界

所有 TRADING-084 输出必须固定：

```text
observe_only = true
candidate_only = true
production_effect = none
broker_action = none
manual_review_required = true
production_state_mutated = false
baseline_config_mutated = false
official_target_weights_mutated = false
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
|TRADING-084A Dynamic Allocation Policy Config|BASELINE_DONE|新增 `config/etf_portfolio/dynamic_allocation_policy.yaml`，定义 base weights、regime targets、overlay rules、constraints、rebalance policy 和 safety。|
|TRADING-084B Regime-to-Weight Mapping Engine|BASELINE_DONE|把 risk_on / neutral / risk_off / growth_underperformance / semiconductor_leadership / event_risk_high 映射到 candidate target weight templates。|
|TRADING-084C Trend Overlay Adjustment Engine|BASELINE_DONE|按 score thresholds 对 SPY / QQQ / SMH / SOXX / CASH 做 bounded overlay adjustment 并输出 reason codes。|
|TRADING-084D Event Risk and Cash Overlay|BASELINE_DONE|event risk high 时增加 CASH、降低 QQQ / SMH / SOXX，并受 cash cap / sleeve cap 约束。|
|TRADING-084E Exposure and Turnover Constraint Layer|BASELINE_DONE|执行 SMH+SOXX cap、QQQ cap、CASH cap、single rebalance max delta、weekly turnover cap、long-only 和 no-leverage 约束。|
|TRADING-084F Rebalance Decision Engine|BASELINE_DONE|确认 score change threshold、regime confirmation window、minimum holding period 和 turnover cap 后输出 rebalance decision。|
|TRADING-084G Dynamic Allocation Decision Records|BASELINE_DONE|生成 date、input_scores、regime_state、previous/candidate weights、constraints、reason codes 和 safety。|
|TRADING-084H Dynamic Allocation Policy Registry|BASELINE_DONE|保存 candidate-only policy registry，不写 production config。|
|TRADING-084I Dynamic Allocation Report|BASELINE_DONE|生成 JSON/Markdown report，包含 policy summary、regime mappings、sample decisions、constraints、reason codes 和 safety banner。|
|TRADING-084J Reader Brief Dynamic Allocation Section|BASELINE_DONE|Reader Brief 只读 latest report，展示 regime、candidate target weights、constraints、rebalance decision 和 detail link。|
|TRADING-084K Dynamic Allocation Validation Gate|BASELINE_DONE|新增 `aits etf dynamic-allocation validate`，fail closed 校验 A-J availability 和 safety boundary。|

## 设计约束

1. TRADING-084 可生成 `candidate_target_weights`，但不得写 official ETF target weights artifact。
2. Dynamic allocation policy 必须从 config 读取所有 thresholds、caps、score bands、confirmation windows 和 overlay sizes。
3. Decision records 必须保留 reason codes、constraint diagnostics、previous weights、candidate weights 和 source trend report link。
4. Reader Brief 只读 latest dynamic allocation report；缺失时显示 `MISSING`，不得补跑 dynamic-allocation CLI。
5. 默认市场 regime 仍为 `ai_after_chatgpt`；报告必须披露 selected market regime 和 requested decision date。

## 验收命令

完成后至少运行：

```bash
python -m pytest tests/test_etf_dynamic_allocation.py tests/test_reader_brief.py tests/test_report_index.py -q
python -m ruff check config src tests scripts docs
python -m compileall -q src tests scripts
git diff --check
python -m ai_trading_system.cli etf dynamic-allocation validate
```

如最终 CLI 名称不同，必须同步更新本文件、`docs/task_register.md` 和
`docs/system_flow.md`。

## 进展记录

- 2026-06-05: 新增并进入 IN_PROGRESS。基于 TRADING-083_to_TRADING-087 roadmap，先做 two-layer dynamic ETF allocation 的 Layer 2 policy engine baseline：config-governed regime mapping、bounded trend/event overlays、constraints、candidate decision records、policy registry、report、Reader Brief 和 validation gate。本阶段不允许 official target weights write、baseline mutation、broker action、automatic promotion 或 auto-enrollment。
- 2026-06-05: A-K baseline 实现完成并转入 VALIDATING。新增 policy config、regime-to-weight mapping、trend/event overlay、exposure/turnover constraints、rebalance decision、decision records、policy registry、JSON/Markdown report、Reader Brief `Dynamic Allocation Candidate` section、report registry/artifact catalog/system flow/runbook/README integration 和 `aits etf dynamic-allocation validate` gate；focused pytest、ruff、compileall、diff check、dynamic-allocation validation gate 和 sample real CLI smoke 已通过。剩余条件是真实 dynamic allocation report 的 owner 复核，以及 TRADING-085 batch/cache 前确认 candidate policy 是否可作为输入。
- 2026-06-05: 全量 `python -m pytest tests -q` 长跑复核通过，结果为 2144 passed、330 warnings、耗时 645.57s。真实 CLI smoke 通过：`aits etf dynamic-allocation decide --date 2026-06-03 --score-profile semiconductor_leadership --latest-trend-report` 生成 latest decision/report/registry，`report --latest` 显示 `status=PASS`、`selected_regime=semiconductor_leadership`、`rebalance_decision=rebalance_candidate`、`data_quality_status=PASS_WITH_WARNINGS`、`production_effect=none`、`broker_action=none`、`official_target_weights_mutated=false`。
