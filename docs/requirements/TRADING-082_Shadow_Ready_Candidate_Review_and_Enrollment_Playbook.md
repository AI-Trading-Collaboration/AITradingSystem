# TRADING-082 Shadow-Ready Candidate Review and Enrollment Playbook

- 父任务：TRADING-082
- 优先级：P0
- 状态：VALIDATING
- owner：system
- 创建日期：2026-06-04
- 来源计划：`G:/Download/TRADING-082_Shadow_Ready_Candidate_Review_and_Enrollment_Playbook_Development_Plan.md`

## 背景

TRADING-079 diagnostics 已发现 `shadow_ready` observations 和 stable weight shapes。TRADING-080 解决 cache / resume 重复计算问题。TRADING-081 production-like profiling 证明 cold run 主要耗时在 per-candidate regime robustness / backtest computation，当前 runtime 低于 1200s trigger，且不建议优先做 native rewrite。

本阶段把 `shadow_ready` observations 转成可审计的 owner-reviewed forward shadow enrollment workflow。它是 review / enrollment governance，不是 production allocation change。

## 安全边界

所有 TRADING-082 输出必须固定：

```text
observe_only=true
candidate_only=true
production_effect=none
broker_action=none
manual_review_required=true
```

禁止：

```text
production baseline replacement
production config mutation
broker execution
automatic candidate promotion
auto-enrollment without owner approval
```

## 阶段拆解

|阶段|状态|验收标准|
|---|---|---|
|TRADING-082A Shadow-Ready Review Policy Config|BASELINE_DONE|新增 `config/etf_portfolio/shadow_ready_review.yaml`，ranking weights、thresholds、hard blockers、owner approval policy、enrollment limits 和 safety 校验 fail closed。|
|TRADING-082B Diagnostics Artifact Loader|BASELINE_DONE|可读取 diagnostics JSON、stable shapes CSV、near-shadow CSV；缺 required artifact 阻断，缺 optional artifact warning，source paths preserved。|
|TRADING-082C Shadow-Ready Candidate Aggregator|BASELINE_DONE|按 `weight_shape` 聚合 observations，保留 representative weights、appearance counts、preset/search ids、source weight_set_ids 和 safety。|
|TRADING-082D Stable Shape Review Ranking|BASELINE_DONE|生成 deterministic review ranking，包含 review priority score、reason summary、supporting evidence、blocking evidence 和 review status。|
|TRADING-082E Near-Shadow Diagnostic Summary|BASELINE_DONE|汇总 near-shadow gaps、rescue suggestions 和 caution，不绕过 gate。|
|TRADING-082F Shadow Candidate Review Package Generator|BASELINE_DONE|输出 JSON/Markdown review package，含 safety banner、ranked shapes、top candidates、owner checklist、decision options 和 source links。|
|TRADING-082G Owner Approval Capture|BASELINE_DONE|只允许 `approved_for_shadow` / `continue_review` / `needs_more_data` / `reject_candidate` / `defer_decision`，拒绝 production/broker 决策；approval 记录链接 package 和 shape。|
|TRADING-082H Approved Candidate Enrollment|BASELINE_DONE|只有 owner-approved candidate 可以 enroll；输出 enrollment record 和 forward tracking status，不修改 production state。|
|TRADING-082I Forward Tracking Linkage|BASELINE_DONE|enrollment 输出包含 `shadow_candidate_id`、`weight_set_id`、`shape_id`、`approval_id`、`review_package_id`、dashboard/weekly/journal links 和 `next_review_due`。|
|TRADING-082J Reader Brief Shadow Candidate Review Section|BASELINE_DONE|Reader Brief 只读 latest package/enrollment，展示 top candidate、approval status、approved enrollment count、pending review count、安全边界和 detail link。|
|TRADING-082K Shadow Candidate Review Validation Gate|BASELINE_DONE|新增 `aits etf shadow-review validate`，fail closed 校验 A-J availability、安全字段、source evidence links 和 no auto-enrollment。|

## 优先排序政策

默认 review priority score 使用配置化权重：

```text
0.25 shadow_ready_appearance_score
0.20 cross_preset_stability_score
0.15 rank_consistency_score
0.15 weight_shape_similarity_score
0.10 low_regime_failure_score
0.10 overfit_medium_ratio_score
0.05 balanced_exposure_score
```

硬阻断项包括：

```text
NO_SHADOW_READY_APPEARANCE
EVIDENCE_DASHBOARD_BLOCKED
DATA_QUALITY_CRITICAL
OPS_VALIDATION_FAILED
UNSAFE_PRODUCTION_EFFECT
BROKER_ACTION_NOT_NONE
MISSING_DIAGNOSTICS_SOURCE
MISSING_WEIGHT_SET_ID
```

## 初始复核目标

最新 diagnostics 建议先人工复核：

```text
shape_id=weight_shape_010_8ce67406f0
representative_weights=SPY 35 / QQQ 35 / SMH 15 / SOXX 5 / CASH 10
```

该目标不是 enrollment recommendation；它只作为 first review target。

## 验收命令

完成后至少运行：

```bash
python -m pytest tests/test_etf_shadow_ready_review.py tests/test_reader_brief.py tests/test_report_index.py -q
python -m ruff check config src tests scripts docs
python -m compileall -q src tests scripts
git diff --check
python -m ai_trading_system.cli etf shadow-review validate
```

如最终 CLI 名称不同，必须同步更新本文件、`docs/task_register.md` 和 `docs/system_flow.md`。

## 进展记录

- 2026-06-04: 新增并进入 IN_PROGRESS。基于 TRADING-079/080/081 证据，优先推进 shadow-ready candidate review and enrollment playbook；本阶段固定 observe-only / candidate-only / manual-review-only，不允许 production mutation、broker action、automatic promotion 或 auto-enrollment without approval。
- 2026-06-04: TRADING-082A~K baseline 实现完成并转入 VALIDATING。新增 policy config、shadow-review module、`package/approve/enroll-approved/validate` CLI、report registry、Reader Brief `Shadow Candidate Review` section、system flow、artifact catalog、operations runbook、README 和 focused tests。验证通过：`tests/test_etf_shadow_ready_review.py tests/test_etf_baseline_review.py tests/test_etf_weight_calibration_profiling.py tests/test_etf_weight_calibration.py tests/test_reader_brief.py tests/test_report_index.py -q` 共 140 passed；文档契约相关 tests 19 passed；`python -m ruff check config src tests scripts docs`、`python -m compileall -q src tests scripts`、`git diff --check` 和 `python -m ai_trading_system.cli etf shadow-review validate` 通过。真实 owner approval 和 forward observation 仍待运行验证。
- 2026-06-05: 全量 `python -m pytest tests -q` 长跑复核通过，结果为 2138 passed、330 warnings、耗时 635.12s；TRADING-082 继续保持 VALIDATING，剩余验证条件是 owner 复核真实 review package，并在批准后观察 forward shadow tracking。
