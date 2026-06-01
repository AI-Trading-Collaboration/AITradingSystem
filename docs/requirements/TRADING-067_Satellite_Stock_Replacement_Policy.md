# TRADING-067 Satellite Stock Replacement Policy

最后更新：2026-06-02

## 背景

TRADING-067 在 TRADING-062 ETF Portfolio Allocation System、TRADING-063
credibility validation、TRADING-064 calibration experiment pack、TRADING-065 forward
simulation dashboard 和 TRADING-066 AI Confirmation Score Calibration 之后，建立
candidate-only 的个股 satellite replacement policy。

核心问题：

```text
When should the system keep ETF exposure such as QQQ / SMH / SOXX,
and when is an individual AI / semiconductor stock strong enough to replace
a small part of that ETF sleeve?
```

默认答案保持 ETF first。个股只能在持续强于 benchmark ETF、趋势为正、风险可接受、
AI Confirmation supportive 或 neutral、并满足 single-name / sleeve 约束后，才生成
candidate-only replacement plan。

## 安全边界

所有 TRADING-067 输出必须固定：

```text
observe_only=true
candidate_only=true
production_effect=none
broker_action=none
manual_review_required=true
fallback_to_etf=true when data is insufficient or gate fails
```

不得写 official ETF target weights，不得触发 broker action，不得自动 promotion。允许输出的
权重字段仅限：

```text
candidate_weights
shadow_weights
hypothetical_weights
replacement_plan
```

## 阶段拆解

|阶段|范围|状态|验收|
|---|---|---|---|
|TRADING-067A|Satellite universe config|BASELINE_DONE|`config/etf_portfolio/satellite_universe.yaml` 存在并通过 loader validation|
|TRADING-067B|Stock-to-ETF benchmark mapping|BASELINE_DONE|enabled stock 可确定性映射到 benchmark ETF、sleeve、role、replacement_source|
|TRADING-067C|Stock vs ETF relative strength features|BASELINE_DONE|生成 no-lookahead stock/benchmark/relative return、MA、drawdown、volatility、coverage features|
|TRADING-067D|Satellite candidate score|BASELINE_DONE|输出 score、score_band、component scores、drivers 和 safety fields|
|TRADING-067E|Replacement eligibility gate|BASELINE_DONE|输出 eligible/watch/fallback_to_etf/blocked/insufficient_data 和 blocker codes|
|TRADING-067F|ETF replacement plan generator|BASELINE_DONE|只生成 candidate/shadow/hypothetical replacement plan，保留 sleeve exposure|
|TRADING-067G|Satellite shadow portfolio experiment|BASELINE_DONE|比较 ETF baseline 与 ETF + satellite candidate weights|
|TRADING-067H|Satellite risk constraint layer|BASELINE_DONE|执行 total/single/sleeve/residual ETF/vol/drawdown/event risk constraints|
|TRADING-067I|Standalone satellite replacement report|BASELINE_DONE|JSON/Markdown report 包含 universe、score、gate、plan、risk、AI confirmation context|
|TRADING-067J|Reader Brief satellite section|BASELINE_DONE|Reader Brief 只读展示 satellite replacement 摘要和 detail report link|
|TRADING-067K|Satellite policy validation gate|BASELINE_DONE|`aits etf satellite validate` fail-closed 聚合 A-J 检查|

## 配置与政策

新增 source configuration：

```text
config/etf_portfolio/satellite_universe.yaml
config/etf_portfolio/satellite_policy.yaml
```

`satellite_policy.yaml` 作为启发式治理 manifest，记录 score weights、score bands、
eligibility thresholds、risk constraints、owner、version/status、rationale、validation 和
review condition。所有会影响投资解释的 threshold 都必须来自该配置或具备相邻 rationale。

## 时点契约

TRADING-067 继续遵守 TRADING-063 no-lookahead contract：

```text
feature_date <= score_date
score_date < earliest_execution_date
future returns are evaluation-only
```

Relative strength features 只能使用 `run_date` 及之前的价格。Shadow experiment 可记录
未来表现的评估字段，但不得进入 decision-time score/gate/plan。

## 验证命令

完成前必须通过：

```bash
python -m pytest tests -q
python -m ruff check config src tests scripts docs
python -m compileall -q src tests scripts
git diff --check
python -m ai_trading_system.cli etf satellite validate
```

如果最终 CLI 名称变更，必须在本文件和 `docs/task_register.md` 记录实际命令。

## 进展记录

- 2026-06-02：TRADING-067 新增为 P0 `IN_PROGRESS`。根据 owner 提供计划开始实现
  A-K；安全边界保持 observe-only / candidate-only / no broker action。
- 2026-06-02：TRADING-067A~K baseline implementation 完成；新增 satellite universe/policy
  config、stock-to-ETF benchmark mapping、relative strength features、candidate score、
  replacement gate、candidate-only replacement plan、shadow experiment、standalone report、
  Reader Brief section、report registry 和 final validation gate。所有输出保持 ETF-first、
  observe-only / candidate-only / no broker action，不写 official ETF target weights。
