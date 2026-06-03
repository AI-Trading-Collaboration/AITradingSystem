# TRADING-079 Historical Weight Search Diagnostics and Robust Candidate Discovery

最后更新：2026-06-03

## 状态

- 父任务：TRADING-079
- 当前状态：VALIDATING
- 优先级：P0
- 下一责任方：系统验证 + 项目 owner
- 安全边界：`observe_only=true`、`candidate_only=true`、`production_effect=none`、`broker_action=none`、`manual_review_required=true`

## 背景

TRADING-078 已把 historical ETF weight calibration 整理为可运行 workflow。真实
`ai_cycle_recent` 运行生成了 Top-N candidates，但 Top 10 全部被
`blocked_by_overfit_risk` 阻断，`enroll-top --latest --top 3` 正确 fail closed。

这不是要放宽风控，而是要让系统回答：

```text
历史权重搜索为什么没有产生 shadow-ready 候选？
有哪些合理方式可以扩大搜索或调整候选生成，让我们找到更稳健的初始权重？
```

## 非目标

- 不放宽 `OVERFIT_RISK_HIGH`、`blocked_by_overfit_risk` 或 shadow enrollment gate。
- 不把 near-shadow candidate 自动登记为 shadow candidate。
- 不写 production weights、baseline config、target weights 或 broker state。
- 不引入 unbounded optimizer、ML optimizer 或 paid data dependency。
- 不把单一历史窗口最高收益解释为初始权重推荐。

## 阶段拆解

|子任务|状态|验收标准|
|---|---|---|
|TRADING-079A Multi-preset diagnostics|VALIDATING|比较 `last_2y`、`last_3y`、`last_5y`、`post_2022_bear`、`ai_cycle_recent`、`full_available` 的 Top-N、overfit risk distribution、shadow-ready count 和稳定权重结构。|
|TRADING-079B Cross-preset stability score|VALIDATING|输出 `cross_preset_stability_score`、`rank_consistency`、`weight_shape_similarity`、`regime_failure_count`，识别跨 preset 稳定出现的候选形状。|
|TRADING-079C Near-shadow rescue diagnostics|VALIDATING|输出 `near_shadow_candidates`、主要 blocker、差距解释和 rescue suggestion，例如降低半导体上限、提高现金下限或扩大历史窗口。|
|TRADING-079D Robust search packs|VALIDATING|新增 bounded defensive / balanced / moderate AI search configs，收窄 semiconductor exposure，不改变 production 权重。|
|TRADING-079E Shadow minimum criteria report|VALIDATING|报告说明为什么没有 shadow-ready、哪些条件最常失败，以及下一步是否应扩大窗口、降低 semiconductor cap、提高 cash floor 或调整 objective policy。|

## 设计决策

1. TRADING-079 是 TRADING-078 的 diagnostic / candidate discovery layer，复用既有 search、Top-N export、comparison、regime robustness 和 overfit explanation artifacts。
2. 多 preset 比较只生成 candidate-only diagnostic artifacts；不会自动注册或 enroll candidate。
3. Cross-preset stability 按权重形状聚合，而不是按单次 `weight_set_id` 聚合，因为不同 preset / search pack 可能生成相同或近似权重结构。
4. Rescue suggestion 是人工复核提示，不是自动参数修改。稳健搜索包作为显式 reviewed config 进入 `config/etf_portfolio/weight_search.yaml`。
5. 如果所有候选继续被 overfit 阻断，报告应把这解释为有效风控结果，而不是系统失败。

## 验收命令

```powershell
python -m pytest tests\test_etf_weight_calibration.py -q
python -m ruff check config src tests scripts docs
python -m compileall -q src tests scripts
git diff --check
```

## 进展记录

- 2026-06-03: 新增并进入 IN_PROGRESS，原因：真实 TRADING-078 `ai_cycle_recent` run 的 Top 10 全部为 `blocked_by_overfit_risk`，需要新增多 preset 横向诊断、cross-preset stability、near-shadow rescue diagnostics、稳健搜索包和 shadow minimum criteria report，帮助寻找更可靠的 candidate-only ETF 初始权重。
- 2026-06-03: 从 IN_PROGRESS 改为 VALIDATING，原因：已新增 `aits etf weight-calibration diagnostics` CLI、diagnostics report schema/writer/Markdown renderer、cross-preset stability score、near-shadow rescue diagnostics、shadow minimum criteria report、三组 lower-semiconductor robust search pack config、report registry/artifact catalog/system-flow/README/task-register integration 和专项测试。验证通过 `python -m pytest tests -q`（2110 passed）、`python -m ruff check config src tests scripts docs`、`python -m compileall -q src tests scripts`、`git diff --check` 和 `aits etf weight-calibration usability-validate`。小规模真实 smoke `aits etf weight-calibration diagnostics --include-robust-packs --preset ai_cycle_recent --top 3 --max-candidates 4` 成功生成 diagnostics artifacts；该 smoke 只验证工程链路，不替代完整六 preset / owner manual review。
