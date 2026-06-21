# TRADING-703 to TRADING-725 Research Governance, Acceleration, and Portfolio Decision Roadmap

## 状态

- 状态：VALIDATING
- 日期：2026-06-21
- 范围：validation-only / observe-only research infrastructure
- Owner：系统实现 + 项目 owner 后续复核

## 背景

附件 `AITradingSystem_TRADING_702_725_master_research_roadmap.md` 将后续研究目标从
局部 `indicator -> signal -> weight` 扩展为统一的动态组合决策研究问题：

```text
PIT state -> action / target weights / horizon / review condition / confidence
```

TRADING-702 已完成 PIT data source readiness audit 的基础实现。本阶段推进 TRADING-703
到 TRADING-725，目标是在不改变 production、paper-shadow、official weights、broker/order
路径的前提下，建立研究协议、证据账本、状态评估、样本质量、promotion readiness、逆向诊断、
benchmark/controls、preflight/pivot、动态组合决策 contract、strategy adapter、多阶段评估、
research ops 队列、dashboard、paper-shadow cohort readiness 和最终文档迁移。

## 总安全边界

- validation-only / observe-only。
- `production_effect=none`。
- 不修改 production / paper-shadow / official weights。
- 不触发 broker/order/live trading。
- 不放宽 PIT、data quality、feature availability、lineage 或 no-lookahead gate。
- 不把 `backtest_trace_bridge`、component replay、oracle、synthetic 或 negative/positive
  control 输出作为 promotion evidence。
- 不新增未登记的高影响阈值；subjective gate 必须进入 policy/config。
- 缺少真实 full-advisory evidence 时必须 fail-closed 或输出 evidence-required，不得伪造 PASS。

## 阶段拆解

### Phase A：研究治理基础

- TRADING-703：Research Protocol Registry & Core Schemas。
- TRADING-704：Evidence Ledger & Evidence Source Policy。
- TRADING-705：Multi-Axis State Evaluator & Blocker Taxonomy。
- TRADING-706：Sample Quality & Effective Evidence Audit。
- TRADING-707：Threshold Dependency & Calibration Governance。
- TRADING-708：Promotion Readiness Policy & Decision Ledger。
- TRADING-709：Artifact Catalog & Unified Research Rollup。
- TRADING-710：Watchlist / Stop / Rerun / Pivot Trigger & Pilot Migration。

### Phase B：正向与逆向研究加速

- TRADING-711：Strategy Pair Reverse Diagnostics。
- TRADING-712：Regret Taxonomy / Failure Casebook / Negative Result Ledger。
- TRADING-713：Strategy Benchmark Zoo / Positive-Negative Controls / Falsification Harness。
- TRADING-714：Research Preflight / Kill-Pause-Pivot Policy / Portfolio Controller。
- TRADING-715：Hypothesis Compiler / Mutation / Orthogonal Direction Generator。

### Phase C：统一动态投资决策问题

- TRADING-716：Portfolio Decision Problem Contract。
- TRADING-717：PIT Action-Outcome Dataset。
- TRADING-718：Strategy Adapter & Multi-Fidelity Evaluation Harness。
- TRADING-719：Window/Horizon-Conditioned Value Surface Baseline。
- TRADING-720：Advanced Policy Research Sandbox。

### Phase D：研究连续性与并行提效

- TRADING-721：Research Workstream Orchestrator。
- TRADING-722：Batch Experiment Pack & Review Board。
- TRADING-723：Research Ops Dashboard & WIP Governance。
- TRADING-724：Forward Paper-Shadow Cohort Readiness。
- TRADING-725：Master Validation / Migration / Documentation Rollup。

## 设计决策

1. 新增独立研究模块，而不是继续扩大 `indicator_research.py`：
   - `ai_trading_system.research_governance`
   - `ai_trading_system.research_acceleration`
   - `ai_trading_system.portfolio_decision`
2. CLI 挂载在既有 `aits research` 下：
   - `aits research governance ...`
   - `aits research acceleration ...`
   - `aits research portfolio-decision ...`
   - `aits research strategy ...`
   - `aits research advanced-policy ...`
   - `aits research ops ...`
   - `aits research paper-shadow ...`
3. 所有 artifact 同时写 JSON 和 Markdown，默认输出中文结论，保留 English identifiers。
4. 输出采用 fail-closed baseline：有真实证据时汇总真实证据；缺少 full-advisory / PIT outcome /
   human review 时输出 `EVIDENCE_REQUIRED` 或 `NOT_READY`，不升级 promotion。
5. Research ops 可以生成队列、实验包、review board 和 dashboard，但不自动运行 broker、
   paper-shadow enrollment 或 production mutation。

## 验收标准

- TRADING-703 到 TRADING-725 的 CLI 均可运行并写出 JSON/Markdown artifact。
- Protocol registry 至少包含 `valuation_crowding_masking`、`dynamic_trend_thresholds` 和
  `portfolio_decision_problem_v1`。
- JSON schema 文件存在并覆盖 roadmap 指定 schema。
- Evidence ledger 对 source class E0-E5 进行 allowed-use 限制。
- State evaluator 输出多轴状态，engineering pass 不自动推导 research pass。
- Sample quality audit 不用 row_count 代替有效证据质量。
- Promotion readiness 单一来源于 policy + evidence ledger + human review requirement。
- Oracle/teacher diagnostics 只能用于诊断和假设生成，不能进入 promotion gate。
- Benchmark/control/falsification 输出 future leakage trap blocked 与 negative control fail-closed。
- Portfolio decision contract 校验通过且固定 `production_effect=none`。
- Strategy adapter 和 advanced policy sandbox 均不能跳过 stage gate。
- Research ops dashboard 显示 WIP、blocked、stalled、next recommended batch。
- Paper-shadow cohort readiness 不修改 official weights，不触发 broker。
- `docs/system_flow.md` 同步新增新数据流说明。
- focused pytest、compileall、Ruff/Black check、`git diff --check` 和适用 validation tier 通过或明确记录阻塞。

## 开放问题

- 真实 full-advisory PIT replay 样本仍受 TRADING-702 标记的 SEC fundamentals 可用时点限制。
- 高影响阈值仍有大量 `SENSITIVITY_TESTED` / uncalibrated 状态，不能作为 validated boundary。
- Advanced policy、offline RL、NN 等复杂策略只能进入 sandbox，除非后续 owner 明确批准更高阶段。

## 进度记录

- 2026-06-21：按 owner 附件新增总计划并进入 IN_PROGRESS；实现前固定 validation-only、
  no production/paper-shadow/official weight mutation、no broker/order、no gate relaxation、
  no bridge-only promotion evidence 的边界。
- 2026-06-21：实现 validation-only baseline 并进入 VALIDATING。新增 core schema、pilot
  protocols、research governance policy、portfolio decision contract、`research_governance`、
  `research_acceleration`、`portfolio_decision` 模块，以及 `aits research governance`、
  `acceleration`、`portfolio-decision`、`strategy`、`advanced-policy`、`ops`、
  `paper-shadow` CLI。所有新增命令均保持 `production_effect=none`，缺少真实 E3/E4、
  PIT action-outcome rows 或人工复核时输出 `EVIDENCE_REQUIRED` / `NOT_READY` /
  `SANDBOX_REGISTERED_NOT_PROMOTABLE`，不批准 promotion、paper-shadow、official weights、
  broker/order 或 production mutation。
- 2026-06-21：验证通过新增 focused 并行 pytest `5 passed`、CLI smoke 覆盖 governance /
  acceleration / portfolio-decision / strategy / advanced-policy / ops / paper-shadow 子命令、
  `fast-unit` tier `74 passed`、`contract-validation` tier `73 passed`、`report-validation`
  tier `55 passed / 62 warnings`、`python -m compileall src/ai_trading_system`、Black check、
  Ruff 和 `git diff --check`。后续仍需真实 full-advisory / forward paper-shadow evidence
  才能把研究结论升级到 promotion review。
- 2026-06-21：继续推进 TRADING-709 / 716 / 723 / 725 的闭环。`validate-contract`、
  `show-contract`、research ops status/plan/run/rollup/review-board/dashboard、acceleration
  portfolio/pivot/hypothesis/direction 和 paper-shadow cohort-status CLI 补齐 JSON/Markdown
  artifact 写入；`config/report_registry.yaml` 增加 TRADING-703～725 primary review entries；
  `docs/artifact_catalog.md` 增加 artifact family 行；`docs/system_flow.md` 记录 registry /
  catalog / validation-tier 集成；`tests/test_research_master_roadmap.py` 纳入 `fast-unit` 和
  `contract-validation`，并新增 report registry、artifact catalog、validation tier 与 artifact
  existence checks。该增量仍固定 validation-only / observe-only，不改变 production、paper-shadow、
  official weights、broker/order 或任何 promotion readiness 结论。
