# REPORT-056: Research Governance Summary Aggregator

最后更新：2026-06-09

## 背景

`REPORT-055` 已把 score change attribution 和 market panel 接入 Reader Brief。
当前 Reader Brief 中 remaining important missing artifact 主要集中在
`research_governance_summary`，同时 Report Navigation 对 registry 与 runtime
index 的同一 artifact 可重复展示。

本任务在既有 `REPORT-051` Research Governance Summary Cards 基础上增强为
统一治理聚合 artifact，并修复 Reader Brief navigation 去重。

## 目标

新增或增强只读 `research_governance_summary`，供 Reader Brief 汇总：

- backtest 和 robustness 状态；
- parameter governance；
- weight candidate / promotion gate；
- shadow observe 和 SEC PIT shadow monitor；
- SEC PIT backfill / evaluation / baseline / diagnostics / candidate review；
- documentation contract、report index、artifact catalog / registry 状态；
- manual review queue、limitations 和 source artifacts。

报告需要回答：

- 当前系统研究状态是否健康；
- 是否允许 promotion；
- 哪些 shadow / observe-only 任务正在运行；
- 哪些回测或治理 artifact 缺失；
- 是否需要人工复核。

## 边界

- 命令和 builder 只读扫描既有 artifact，不运行 backtest、SEC PIT、shadow、
  weight、docs、scoring 或 trading 上游任务。
- 固定 `production_effect=none`。
- 不修改 production weights、active shadow weights、score configs、position gates
  或 trading decisions。
- 默认 `promotion_status=NOT_PROMOTABLE`；缺少 `weight_promotion_gate` 时必须
  降为 `BLOCKED_BY_MISSING_ARTIFACTS`，除非显式 promotion gate artifact 给出
  更保守阻断状态。

## CLI

- `aits reports research-governance-summary --date YYYY-MM-DD`
- `aits reports research-governance-summary --latest`

输出：

- `outputs/reports/research_governance_summary_YYYY-MM-DD.json`
- `outputs/reports/research_governance_summary_YYYY-MM-DD.md`

## 最低 JSON Schema

必须包含：

- `generated_at`
- `as_of_date`
- `governance_status`
- `research_readiness`
- `promotion_status`
- `manual_review_required`
- `production_effect`
- `backtest`
- `weight_iteration`
- `shadow_observe`
- `sec_pit`
- `documentation`
- `manual_review_queue`
- `limitations`
- `source_artifacts`

允许 `governance_status`：

- `OK`
- `PASS_WITH_LIMITATIONS`
- `LIMITED_CONTEXT`
- `FAILED_VALIDATION`

允许 `promotion_status`：

- `PROMOTABLE`
- `NOT_PROMOTABLE`
- `BLOCKED_BY_MISSING_ARTIFACTS`
- `BLOCKED_BY_MANUAL_REVIEW`
- `BLOCKED_BY_DATA_QUALITY`

## Reader Brief 集成

- Reader Brief 的 Backtest / Shadow / Governance 摘要优先消费
  `research_governance_summary_YYYY-MM-DD.json`。
- Missing / Limited Artifact Impact 在 summary 已生成时不得再把
  `research_governance_summary` 计为 IMPORTANT。
- Executive Summary 增加一句：
  `research governance status = ...; promotion_status = ...`。
- Report Navigation 将 research governance summary 放入
  `Governance / documentation`。
- Manual Review Queue 合并 governance summary 的人工复核项。

## Report Navigation 去重

同一 navigation group 内，同一 `artifact_id` 只展示一次。

- registry 与 runtime index 同时提供同一 artifact 时，优先 runtime index。
- status 合并时取更具体状态，但不重复 visible row。
- audit details 保留来源信息。
- 输出顺序稳定。

## 验收标准

- `aits reports research-governance-summary --latest` 可生成 JSON/Markdown。
- JSON / Markdown 覆盖 backtest、weight iteration、shadow observe、SEC PIT、
  documentation、manual review queue、limitations、source artifacts。
- promotion 默认不允许；缺 promotion gate 阻断 promotion。
- SEC PIT observe-only 状态清晰标注，且 `production_effect=none`。
- Reader Brief 消费该 artifact，缺失影响层不再把已生成 summary 计为重要缺口。
- Report Navigation 不重复展示 `daily_decision_summary` 等同组 artifact。
- 测试覆盖 schema、Markdown、缺失 backtest 降级、缺 promotion gate、缺 candidate
  evaluation、SEC PIT observe-only、documentation contract、Reader Brief 消费、
  navigation 去重、read-only 和 deterministic output。

## 进展

- 2026-05-29：新增并进入 `IN_PROGRESS`。先补任务登记和需求拆解，再扩展既有
  `research_governance_summary` 与 Reader Brief 集成。
- 2026-05-29：实现完成并进入 `VALIDATING`。已在既有
  `research_governance_summary` 上新增治理聚合 schema、`--date` / `--latest`
  CLI、promotion 默认阻断规则、SEC PIT observe-only / `production_effect=none`
  披露、documentation / registry 汇总、manual review queue 和 source artifacts；
  Reader Brief 已消费该 artifact，Executive Summary 展示 governance/promotion
  状态，Manual Review Queue 合并治理复核项，Missing / Limited Artifact Impact
  在 summary 已生成时不再把 `research_governance_summary` 计为重要缺口；
  Report Navigation 已按同组 `artifact_id` 去重并保留 audit sources。验证通过
  专项 pytest、`tests/trading_engine`、全量 pytest、ruff、black、diff check，以及
  真实 `--latest` 生成；`reader_brief_2026-05-27.json` 状态为 `OK`，
  `important_count=0`，`daily_decision_summary` 导航单行合并为 `limited/FRESH`。
- 2026-06-09：从 `VALIDATING` 改为 `DONE`。最新
  `research_governance_summary_2026-06-05.md/json` 为
  `PASS_WITH_LIMITATIONS` / `PASS_WITH_WARNINGS`，`production_effect=none`，
  `promotion_status=BLOCKED_BY_MISSING_ARTIFACTS`，cards=19，source artifacts=19，
  manual review queue=13，limitations=50，card/source artifact 空 production effect=0，
  unsafe production effect=0。JSON 与 Markdown 覆盖 backtest、weight iteration、
  shadow observe、SEC PIT、documentation、manual review queue、limitations 和
  source artifacts；SEC PIT shadow observe / monitor 仍为 observe-only /
  `OK_MONITORING` 且 `production_effect=none`。Reader Brief latest 为 `OK`，
  `backtest_shadow_governance.source=research_governance_summary`，
  narrative 展示 `research governance status = PASS_WITH_LIMITATIONS;
  promotion_status = BLOCKED_BY_MISSING_ARTIFACTS`，Missing / Limited Artifact
  Impact 的 `important_count=0` 且未把已生成的 `research_governance_summary`
  计为缺口；Manual Review Queue 合并 14 个 research governance 相关复核项。
  Report Navigation 的 `Governance / documentation` 组包含
  `research_governance_summary`，同组 duplicate artifact_id=0，并保留 runtime /
  registry `navigation_sources`。`research-governance-summary --latest`、
  `reader-brief --latest`、`validate-reader-brief --latest` 和
  `docs report-contract --latest` 通过。
