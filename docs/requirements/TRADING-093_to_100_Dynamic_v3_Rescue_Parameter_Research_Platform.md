# TRADING-093 to TRADING-100: Dynamic v3 Rescue Parameter Research Platform

## 背景

TRADING-091 真实评估显示 dynamic v0.3 rescue gate 为 `reject`，constraint hit 改善不足，robustness overfit 为 `REVIEW_REQUIRED`，且 drawdown 相对 v0.4 存在退化。TRADING-092 已完成失败归因和 v0.4 promotion review，结论是不再手工迭代单个 v0.5 / v0.6，而是建设可复现、可审计、可恢复的参数研究与晋级治理平台。

本平台仍属于 research / review / observe-only workflow。任何自动命令都不得生成 `production_candidate`，不得写 production baseline、official target weights、broker state、owner approval 或 shadow enrollment approval。

## 范围

|任务|阶段|目标|状态|
|---|---|---|---|
|TRADING-093|Parameter Sweep Config Schema|新增 `parameter_sweep_v1.yaml`、配置加载、schema validation、参数网格、稳定 `candidate_id` 和 preview CLI|VALIDATING|
|TRADING-094|Batch Parameter Backtest Runner|新增 sweep run/status/validate，生成不可变 sweep artifacts、checkpoint、resume、candidate error isolation 和 latest pointer|VALIDATING|
|TRADING-095|Candidate Ranking / Leaderboard / Reports|实现 hard gate、soft score、leaderboard、sweep report、candidate report 和 Reader Brief sweep 摘要|VALIDATING|
|TRADING-096|Walk-forward / OOS Validation|对 top candidates 生成 walk-forward windows、window results、OOS summary、leaderboard/report/validation|VALIDATING|
|TRADING-097|Robustness / Sensitivity / Overfit Diagnostics|生成邻近参数敏感性、stress/regime bucket、overfit diagnostics、robustness report/validation|VALIDATING|
|TRADING-098|Shadow Candidate Registry|新增 observe-only shadow registry、register/list/report/validate CLI，拒绝 rejected candidate 和缺失 source artifact|VALIDATING|
|TRADING-099|Scheduled Evaluation / Artifact Retention / Latest Pointer|新增 artifact latest/validate/stale CLI，文档化 retention policy 和 scheduled observation runbook|VALIDATING|
|TRADING-100|Promotion Review Pack|生成 promotion review / pack / validation，缺失证据 fail closed，最多自动到 `promote_candidate + manual_review_required`|VALIDATING|

## 实施顺序

1. 建立配置和 artifact contract：schema、normalized config、candidate list、stable IDs、latest pointers。
2. 建立 tiny fixture sweep runner：可运行完整闭环，不把大规模参数搜索放入 CI。
3. 接入 hard gate 和 soft ranking：阈值全部来自 YAML，score 仅排序，不覆盖 hard gate。
4. 生成 sweep / leaderboard / candidate reports，并接入 Reader Brief 只读摘要。
5. 生成 walk-forward / OOS artifacts，validation 必须能发现缺失文件。
6. 生成 robustness / sensitivity / overfit artifacts；复杂统计 placeholder 必须标记 `REVIEW_REQUIRED`，不得伪造 PASS。
7. 建立 shadow registry：仅 observe-only，不允许 production 使用。
8. 建立 artifact latest / stale / validate 与 retention policy 文档。
9. 建立 promotion pack：缺少任一关键证据时 `incomplete` 或 `review_required`，不得自动产生 `production_candidate`。
10. 更新 README、operations runbook、system flow、artifact catalog、report registry、task register 和 Reader Brief。

## 验收标准

- 现有命令继续可用：`validate`、`real-evaluate`、`real-report --latest`、`validate-real`、`failure-attribution`、`failure-attribution-report --latest`、`validate-attribution`。
- 新增计划中的 CLI 均存在并可运行。
- Tiny sweep 能生成完整 artifact 链路：sweep、leaderboard、candidate report、walk-forward、robustness、shadow report、promotion pack。
- 所有业务阈值来自 `config/etf_portfolio/dynamic_v3_rescue/parameter_sweep_v1.yaml`。
- 所有历史 artifact 不覆盖；latest pointer 只保存指针。
- data quality 失败时 sweep fail closed；tiny fixture 可使用显式可审计 fixture mode。
- `production_candidate` 不得由自动命令产生。
- Focused tests 覆盖 config、candidate_id、grid、tiny sweep、checkpoint/resume、ranking、reports、walk-forward、robustness、shadow registry、artifact latest/stale、promotion pack 和 existing CLI regression。
- 必须运行 focused pytest、ruff、compileall、git diff check；大阶段后尽量运行全量 pytest，如未运行需说明原因。

## 进展记录

- 2026-06-06：新增需求文档并登记 TRADING-093 到 TRADING-100；进入实现。目标是以完整骨架、可运行闭环和 tiny fixture 验证作为 baseline，后续真实大规模 sweep 和更复杂统计方法继续按 artifact 和 validation contract 迭代。
- 2026-06-06：TRADING-093 到 TRADING-100 baseline 实现完成并转入 VALIDATING。新增 parameter sweep config、核心模块、CLI 子命令、hard gate / soft score、sweep artifacts、walk-forward/OOS、robustness/sensitivity/overfit diagnostics、observe-only shadow registry、latest pointer/artifact validation、promotion pack、Reader Brief、report registry、artifact catalog、operations runbook、system flow、README 和 focused tests。样例默认 sweep `sweep_20260606T024119Z_98fa3c81` 生成 5000 个 tiny fixture candidates，其中 observe_only=12、reject=4988、top candidate=`ce9db518659c8d68`；walk-forward sample `1c295ba52ed095df` PASS，robustness sample `8125b579107a88a4` 为 REVIEW_REQUIRED，promotion pack `622ee067448ae09c` 因 `walk_forward_failed` reject，未生成 `production_candidate`。验证通过 focused tests、旧 dynamic-v3 回归、Reader Brief/report index、旧 validation gates、ruff、compileall、diff check 和全量 pytest。
