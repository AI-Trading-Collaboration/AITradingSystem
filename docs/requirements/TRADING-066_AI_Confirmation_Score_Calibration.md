# TRADING-066 AI Confirmation Score Calibration

状态：IN_PROGRESS

最后更新：2026-06-01

## 背景

TRADING-062 已建立 ETF allocation baseline。TRADING-063 完成 ETF credibility
gate，TRADING-064 完成 `etf_calibration_v1` controlled experiment pack，TRADING-065
完成 forward shadow observation dashboard。TRADING-066 在这些基础上新增 AI /
semiconductor confirmation overlay，用于量化 AI mega-cap、semiconductor breadth、
ETF relative strength 和 event risk 是否支持或削弱 ETF candidate allocation thesis。

## 范围

- 新增 config-driven AI confirmation universe。
- 基于价格和事件日历生成 AI / semiconductor breadth、mega-cap confirmation、
  AI semiconductor relative strength、event risk 和 composite score。
- 生成 standalone AI confirmation JSON/Markdown report。
- 新增 shadow candidate overlay experiment，只输出 candidate/shadow/hypothetical
  weights，不修改 official ETF target weights。
- Reader Brief 只读展示 AI confirmation 摘要。
- 新增 final validation gate，确认安全边界、报告、overlay 和 Reader Brief 集成完整。

## 安全边界

所有 TRADING-066 输出必须固定：

```text
observe_only=true
candidate_only=true
production_effect=none
broker_action=none
manual_review_required=true
```

AI confirmation layer 不写 `data/etf_portfolio/target_weights.csv`，不替换 ETF baseline，
不触发 broker，不自动 promotion。任何 overlay-adjusted weights 只能命名为
`candidate_weights`、`shadow_weights` 或 `hypothetical_weights`。

## 非目标

- 不把 LLM/news/EDGAR 文本情绪作为 production weight input。
- 不接入 live broker execution、real account trading、options execution 或自动生产晋级。
- 不用 ML ranking 替代 rule-based ETF allocation。
- 不建立 small-cap AI stock alpha model 或未经验证的 alternative data provider integration。

## 阶段拆解

|任务|状态|验收要点|
|---|---|---|
|TRADING-066A AI Confirmation Universe Config|DONE|universe YAML、loader、validation、required/optional data semantics|
|TRADING-066B Semiconductor / AI Breadth Features|DONE|group breadth features、coverage、no-lookahead timing|
|TRADING-066C Mega-Cap AI Confirmation Score|DONE|MegaCapAIScore 0-100、components、drivers、safety fields|
|TRADING-066D AI ETF / Semiconductor Relative Strength Score|DONE|ETF relative pairs、optional ETF handling、score bands|
|TRADING-066E Event Risk Overlay|DONE|config/calendar-driven risk score and active/upcoming/recent events|
|TRADING-066F AI Confirmation Composite Score|DONE|AIConfirmationScore, action_hint, reason_codes, safety fields|
|TRADING-066G AI Confirmation Report|DONE|JSON/Markdown standalone report with safety banner and components|
|TRADING-066H Shadow Candidate Overlay Experiment|DONE|bounded candidate-only hypothetical weights, no production mutation|
|TRADING-066I Reader Brief AI Confirmation Section|READY|daily Reader Brief summary and detailed report link|
|TRADING-066J AI Confirmation Validation Gate|READY|fail-closed final gate and CLI validation output|

## 验收标准

- `python -m ai_trading_system.cli etf ai-confirmation validate` 输出 `PASS`。
- 全量 `python -m pytest tests -q`、`python -m ruff check config src tests scripts docs`、
  `python -m compileall -q src tests scripts` 和 `git diff --check` 通过。
- AI confirmation universe、features、scores、event risk、composite、report、overlay、
  Reader Brief 和 validation gate 均实现且测试覆盖。
- Runtime artifacts 保持在 ignored runtime 目录；确定性 fixtures 只放在
  `tests/fixtures/etf_portfolio/`。
- Future returns 仍只用于 evaluation，不进入 feature/score/overlay decision input。
- Documentation、artifact catalog、system flow、report registry 和 task register 同步。

## 进展记录

- 2026-06-01: 新增并进入 IN_PROGRESS。目标是在 ETF allocation baseline 之上建立
  observe-only / candidate-only AI confirmation overlay，先用于解释和 shadow calibration，
  不改变 production ETF allocation。
- 2026-06-01: TRADING-066A 完成。新增
  `config/etf_portfolio/ai_confirmation_universe.yaml`、AI confirmation universe loader、
  deterministic validation helper 和 config tests；文档同步 README、artifact catalog、
  system flow、task register。本阶段只建立 source config，不生成 runtime conclusion，
  不改变 official ETF target weights。
- 2026-06-01: TRADING-066B 完成。新增 price-derived AI / semiconductor breadth feature
  builder、`aits etf ai-confirmation features` CLI、JSON/CSV runtime output、coverage/warning
  schema 和 tests；feature timing 只使用 `date <= score_date`，strict required data 缺失时
  fail closed，optional missing data 只降低 coverage 并记录 warning。
- 2026-06-01: TRADING-066C 完成。新增
  `config/etf_portfolio/ai_confirmation_policy.yaml`、policy loader、score band mapping 和
  MegaCapAIScore builder；score 输出 component_scores、drivers、coverage、policy hash 和
  safety fields，仍只作为 observe-only / candidate-only payload。
- 2026-06-01: TRADING-066D 完成。新增 AISemiconductorRelativeStrengthScore builder 和
  policy-governed ETF pair definitions；覆盖 `QQQ/SPY`、`SMH/QQQ`、`SOXX/QQQ`、
  `SMH/SPY`、`SOXX/SPY`，optional proxy 缺失只记录 warning，不阻断 baseline。
- 2026-06-01: TRADING-066E 完成。新增 event risk overlay，按事件窗口输出
  active/upcoming/recent events、affected_groups、risk_band 和 reason_codes；它仅作为风险旗标，
  不预测事件方向、不修改 production weights。
- 2026-06-01: TRADING-066F 完成。新增 AIConfirmationScore composite，按 policy 权重合成
  semiconductor breadth、MegaCapAIScore、AISemiconductorRelativeStrengthScore 和 event risk
  adjustment，输出 component_scores、action_hint、reason_codes、coverage、policy hash 和 safety
  fields；验证通过 `python -m pytest tests -q`（1779 passed）、ruff、compileall 和 diff check。
- 2026-06-01: TRADING-066G 完成。新增
  `aits etf ai-confirmation report --date YYYY-MM-DD`、standalone JSON/Markdown report writer、
  report registry entry 和 stable report tests；report 汇总 safety banner、AIConfirmationScore、
  component table、breadth、mega-cap、relative strength、event risk、coverage、drivers 和
  candidate-only/shadow usage note。验证通过 `python -m pytest tests -q`（1782 passed）、ruff、
  compileall 和 diff check。
- 2026-06-01: TRADING-066H 完成。新增 AI confirmation shadow overlay policy、builder、
  Markdown/JSON writer、`aits etf ai-confirmation overlay` CLI 和 report registry entry；overlay
  只读取显式 base weights 和 AI confirmation report，输出 bounded `after_candidate_weights`、
  `candidate_weights`、`shadow_weights`、`hypothetical_weights`，high event risk 阻断新增
  overweight，且不写 official target weights。验证通过 `python -m pytest tests -q`
  （1788 passed）、ruff、compileall 和 diff check。
