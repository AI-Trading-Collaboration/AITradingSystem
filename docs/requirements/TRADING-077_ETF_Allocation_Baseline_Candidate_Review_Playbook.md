# TRADING-077 ETF Allocation Baseline Candidate Review Playbook

最后更新：2026-06-03

## 状态

- 父任务：TRADING-077
- 当前状态：VALIDATING
- 优先级：P0
- 下一责任方：系统实现
- 安全边界：`observe_only=true`、`candidate_only=true`、`production_effect=none`、`broker_action=none`、`manual_review_required=true`

## 背景

TRADING-076 已完成 Strategy Evidence Dashboard，把 ETF baseline、weight calibration、forward evidence、AI / satellite attribution、parameter review、weekly review、decision journal、data quality、operations health 和 validation gates 汇总成统一研究视图。

TRADING-077 的目标是建立人工 baseline review playbook，回答：

```text
When is an ETF allocation candidate ready for human baseline review?
```

本阶段只生成 manual review package、owner decision capture、decision journal linkage、proposal-only draft 和 outcome tracking，不自动修改 baseline weights，不写 production config，不执行 broker action。

## 非目标

- 不实现 automatic production baseline replacement。
- 不实现 automatic candidate promotion。
- 不实现 broker execution 或 real account trading。
- 不修改 production config 或 target weights。
- 不引入 unbounded search、新信号、ML replacement 或 LLM/news/EDGAR production weighting。

## 阶段拆解

|子任务|状态|验收标准|
|---|---|---|
|TRADING-077A Baseline Review Policy Config|VALIDATING|`config/etf_portfolio/baseline_review.yaml` 存在并可验证；required sections、thresholds、blocking policy 和 safety fields mandatory。|
|TRADING-077B Candidate Review Eligibility Gate|VALIDATING|`aits etf baseline-review eligibility --candidate ...` 生成 fail-closed eligibility report，解释 blockers、warnings、missing evidence 和 source links。|
|TRADING-077C Evidence Requirement Matrix|VALIDATING|为 candidate 生成 required evidence matrix，包含 required/status/source/freshness/sample/blocking/notes。|
|TRADING-077D Baseline Review Package Generator|VALIDATING|`aits etf baseline-review package --candidate ...` 输出 JSON / Markdown manual review package，包含 safety banner、candidate、evidence、matrix、blockers、checklist、decision options 和 source links。|
|TRADING-077E Owner Review Checklist and Decision Capture|VALIDATING|结构化捕获 owner decision；拒绝 unsafe / disallowed decision；不修改 production state。|
|TRADING-077F Decision Journal Integration|VALIDATING|baseline review decision 可链接到 decision journal，保留 review package、candidate、evidence matrix 和 audit trail。|
|TRADING-077G Baseline Change Proposal Draft Generator|VALIDATING|仅在 `approve_for_proposal_draft` owner decision 后生成 proposal draft；仍固定 `production_effect=none`。|
|TRADING-077H Candidate Review Outcome Tracker|VALIDATING|记录 latest review status、decision/proposal linkage、history、next review due 和 follow-up tasks。|
|TRADING-077I Reader Brief Baseline Review Section|VALIDATING|Reader Brief 只读展示 baseline review status、eligible/needs-more/blocked counts、latest decision、proposal count、安全边界和 detail link。|
|TRADING-077J Baseline Review Playbook Validation Gate|VALIDATING|`aits etf baseline-review validate` fail-closed 校验 A-I workflow、source links 和 safety boundary。|

## 设计决策

1. Baseline review 是 TRADING-076 之后的 governance stage，输入为既有 evidence dashboard / report index artifacts，不运行上游，不补造证据。
2. Eligibility gate 不能只看单次 backtest；必须同时检查 historical robustness、forward evidence、drawdown / turnover、data quality、ops freshness、validation gates、AI / satellite attribution、decision journal、parameter review 和 manual review readiness。
3. Critical blocker fail closed，包括 evidence dashboard blocked、critical data quality、ops validation failed、forward sample too small、unsafe production effect、broker action、stale validation gate 和 missing required journal link。
4. Proposal draft 只是草案，必须要求 explicit owner decision 和 decision journal linkage；不得写 baseline config 或 target weights。
5. Reader Brief 只读 latest baseline review artifacts，不运行 `baseline-review` CLI。

## 验收命令

最终运行：

```powershell
python -m pytest tests -q
python -m ruff check config src tests scripts docs
python -m compileall -q src tests scripts
git diff --check
python -m ai_trading_system.cli etf baseline-review validate
```

## 进展记录

- 2026-06-03: 新增任务文档并进入 IN_PROGRESS，原因：owner 提供 TRADING-077 开发计划，要求在 TRADING-076 evidence dashboard 之后建立 manual baseline candidate review playbook；本阶段固定 observe-only / candidate-only / manual-review-only，不应用 baseline change，不触发 broker action。
- 2026-06-03: TRADING-077A-J 基础闭环进入 VALIDATING，原因：新增 baseline review policy、eligibility gate、evidence matrix、review package generator、owner decision capture、decision journal baseline-review link validation、proposal-only draft generator、outcome tracker、Reader Brief section、report registry entries 和 validation gate；专项测试与 `aits etf baseline-review validate --as-of 2026-06-03` 已通过，下一步等待真实 owner review cycle 观察。
- 2026-06-03: 全量验证通过，命令包括 `python -m pytest tests -q`（2064 passed）、`python -m ruff check config src tests scripts docs`、`python -m compileall -q src tests scripts`、`git diff --check` 和 `python -m ai_trading_system.cli etf baseline-review validate`（PASS）。TRADING-077 保持 VALIDATING，原因是真实 candidate evidence、owner decision 和 proposal draft review cycle 仍需运行观察。
