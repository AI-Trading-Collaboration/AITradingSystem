# TRADING-093 to TRADING-100: Dynamic v3 Rescue Parameter Research Platform

最后更新：2026-06-09

## 背景

TRADING-091 真实评估显示 dynamic v0.3 rescue gate 为 `reject`，constraint hit 改善不足，robustness overfit 为 `REVIEW_REQUIRED`，且 drawdown 相对 v0.4 存在退化。TRADING-092 已完成失败归因和 v0.4 promotion review，结论是不再手工迭代单个 v0.5 / v0.6，而是建设可复现、可审计、可恢复的参数研究与晋级治理平台。

本平台仍属于 research / review / observe-only workflow。任何自动命令都不得生成 `production_candidate`，不得写 production baseline、official target weights、broker state、owner approval 或 shadow enrollment approval。

## 范围

|任务|阶段|目标|状态|
|---|---|---|---|
|TRADING-093|Parameter Sweep Config Schema|新增 `parameter_sweep_v1.yaml`、配置加载、schema validation、参数网格、稳定 `candidate_id` 和 preview CLI|DONE|
|TRADING-094|Batch Parameter Backtest Runner|新增 sweep run/status/validate，生成不可变 sweep artifacts、checkpoint、resume、candidate error isolation 和 latest pointer|DONE|
|TRADING-095|Candidate Ranking / Leaderboard / Reports|实现 hard gate、soft score、leaderboard、sweep report、candidate report 和 Reader Brief sweep 摘要|DONE|
|TRADING-096|Walk-forward / OOS Validation|对 top candidates 生成 walk-forward windows、window results、OOS summary、leaderboard/report/validation|DONE|
|TRADING-097|Robustness / Sensitivity / Overfit Diagnostics|生成邻近参数敏感性、stress/regime bucket、overfit diagnostics、robustness report/validation；real sweep 必须绑定真实 evaluator artifact 和邻近 real candidate evidence|VALIDATING|
|TRADING-098|Shadow Candidate Registry|新增 observe-only shadow registry、register/list/report/validate CLI，拒绝 rejected candidate 和缺失 source artifact|VALIDATING|
|TRADING-099|Scheduled Evaluation / Artifact Retention / Latest Pointer|新增 artifact latest/validate/stale CLI、daily scheduler lightweight observe gate，文档化 retention policy 和 scheduled observation runbook|VALIDATING|
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
- 所有历史 artifact 不覆盖；latest pointer 只保存指针；默认全局 latest pointer 必须指向 canonical `reports/etf_portfolio/dynamic_v3_rescue/` 下的 artifact，`artifacts validate` 必须拒绝测试临时目录或 canonical 根外目标，`artifacts repair-latest` 只能从 canonical root 重建指针。
- `aits etf dynamic-v3-rescue schedule observe --as-of YYYY-MM-DD` 只能执行 daily scheduler lightweight gate：周度 due 条件、latest pointer validation、stale 检查和可选 observe-only shadow monitor；不得自动运行 `run-profile`、真实 sweep、promotion pack 或生成 `production_candidate`。
- data quality 失败时 sweep fail closed；tiny fixture 可使用显式可审计 fixture mode。
- `production_candidate` 不得由自动命令产生。
- Focused tests 覆盖 config、candidate_id、grid、tiny sweep、checkpoint/resume、ranking、reports、walk-forward、robustness、shadow registry、artifact latest/repair-latest/stale、promotion pack 和 existing CLI regression。
- 必须运行 focused pytest、ruff、compileall、git diff check；大阶段后尽量运行全量 pytest，如未运行需说明原因。

## 进展记录

- 2026-06-06：新增需求文档并登记 TRADING-093 到 TRADING-100；进入实现。目标是以完整骨架、可运行闭环和 tiny fixture 验证作为 baseline，后续真实大规模 sweep 和更复杂统计方法继续按 artifact 和 validation contract 迭代。
- 2026-06-06：TRADING-093 到 TRADING-100 baseline 实现完成并转入 VALIDATING。新增 parameter sweep config、核心模块、CLI 子命令、hard gate / soft score、sweep artifacts、walk-forward/OOS、robustness/sensitivity/overfit diagnostics、observe-only shadow registry、latest pointer/artifact validation、promotion pack、Reader Brief、report registry、artifact catalog、operations runbook、system flow、README 和 focused tests。样例默认 sweep `sweep_20260606T024119Z_98fa3c81` 生成 5000 个 tiny fixture candidates，其中 observe_only=12、reject=4988、top candidate=`ce9db518659c8d68`；walk-forward sample `1c295ba52ed095df` PASS，robustness sample `8125b579107a88a4` 为 REVIEW_REQUIRED，promotion pack `622ee067448ae09c` 因 `walk_forward_failed` reject，未生成 `production_candidate`。验证通过 focused tests、旧 dynamic-v3 回归、Reader Brief/report index、旧 validation gates、ruff、compileall、diff check 和全量 pytest。
- 2026-06-06：TRADING-101 扩展本平台的 evaluator contract。默认 CI / focused tests 继续使用 `tiny_fixture_proxy` 验证 artifact contract；manual research run 可用 `real_dynamic_v3_rescue` 生成 per-candidate TRADING-091 real evaluation artifacts。Tiny fixture promotion pack 被限制为 `review_required` / `reject`，不得进入 `promote_candidate`。
- 2026-06-07：TRADING-097 从 VALIDATING 改回 IN_PROGRESS。审查确认当前 `robustness run` 即使读取 `real_dynamic_v3_rescue` sweep，也仍用 `_fixture_metrics` 生成邻近参数敏感性，manifest/report 未披露 real evaluation artifact、metrics source、data quality 或邻近真实候选覆盖。修复方向是复用同一 sweep 中已完成的 real candidate results 作为邻近敏感性证据；缺少真实邻近证据时保持 `REVIEW_REQUIRED`，不得把 proxy 结果提升为 PASS。
- 2026-06-07：TRADING-097 real artifact-aware robustness 实现完成并转回 VALIDATING。`robustness run` 继承 source sweep evaluator provenance；real mode sensitivity 只读取同一 sweep 中已完成、`metrics_source=real_evaluation_artifact` 且 linked artifact 存在的 neighbor candidate；缺少 neighbor 或 bucket 证据时报告 `REVIEW_REQUIRED`，不回退 `_fixture_metrics`。Manifest/report/validation 新增 evaluator、metrics source、data quality、source real evaluation artifact、real neighbor count、missing neighbor count、stress evidence 和 regime evidence 字段。验证通过 dynamic-v3 focused tests、real evaluation tests、ruff、black 和 compileall。
- 2026-06-07：TRADING-099 从 VALIDATING 改回 IN_PROGRESS。目标是把 Dynamic v3 rescue research latest/stale/validation 和 small_real 手动研究链登记进统一 `config/scheduled_tasks.yaml` 的非 daily cadence，并新增 `aits etf dynamic-v3-rescue schedule observe --as-of YYYY-MM-DD` 作为 daily-run 可调用的轻量门控节点；该节点只做 due/skip/block 审计、latest pointer validation、stale 检查和可选 observe-only shadow monitor，不自动运行真实 sweep、promotion pack 或生成 `production_candidate`。
- 2026-06-07：TRADING-099 实现完成并转回 VALIDATING。新增 `dynamic-v3-rescue schedule observe` CLI、daily-run `dynamic_v3_rescue_schedule_observe` 节点、direct CLI dispatcher、closed-market / not-due / due-no-pointer / broken-pointer 审计、dynamic-v3 scheduled_tasks 日期/条件/data-quality/manual-review 门控，以及 operations runbook、scheduled orchestration runbook、system flow、artifact catalog 和 README 同步。验证通过 `pytest tests/test_scheduled_tasks.py tests/test_ops_daily.py tests/test_cli_direct.py tests/test_etf_dynamic_v3_parameter_research.py -q`（67 passed）、`ruff check`、`black --check`、`compileall -q src`、`aits docs validate-freshness`、`aits docs report-contract --latest` 和 `git diff --check`。
- 2026-06-07：TRADING-099 latest pointer hardening 补充完成。新增 `aits etf dynamic-v3-rescue artifacts repair-latest`，只扫描 canonical dynamic-v3 report root 下已有 artifact 并重建 latest pointers；默认 `artifacts validate` 新增 canonical-root 检查，测试临时目录或 canonical 根外 pointer 不能 PASS。当前本机 `repair-latest` 重建 15 个 pointers，`artifacts validate` PASS，`schedule observe --force-due --skip-shadow-monitor` PASS。
- 2026-06-09：`TRADING-093` 从 `VALIDATING` 改为 `DONE`。默认
  `parameter_sweep_v1.yaml` 复核通过：`aits etf dynamic-v3-rescue
  sweep-config validate` 为 `PASS`，`candidate_preview_count=5000`、
  `failed_check_count=0`、`production_candidate_generated=false`、
  `production_effect=none`；`sweep-config preview --limit 3` 输出 5000 个候选并
  展示稳定候选参数，重复 `preview --limit 5` 的 candidate_id 序列一致
  `9864fc2ed46ed2e3,c382bc687da707c2,bfb41947bc1e7092,a3091ad304594cee,4c2ccf3a995fd903`。
  `tests/test_etf_dynamic_v3_parameter_research.py -q` 为 16 passed，覆盖
  config validation、candidate_id、空 parameter_space、缺 hard constraints /
  scoring 和 max_candidates fail-closed 行为。
- 2026-06-09：`TRADING-094` 从 `VALIDATING` 改为 `DONE`。当前 canonical
  real sweep `sweep_20260607T102300Z_ae5ae1d8` 通过 current validator：
  `aits etf dynamic-v3-rescue sweep validate --sweep-id sweep_20260607T102300Z_ae5ae1d8`
  为 `PASS`、`failed_check_count=0`、`evaluator_mode=real_dynamic_v3_rescue`、
  `production_candidate_generated=false`；`sweep status` 显示 status=`completed`、
  candidate_count=300、completed_count=300、failed_count=0。字段级复核确认
  `sweep_manifest.json`、`sweep_config.normalized.yaml`、`data_manifest.json`、
  `candidates.jsonl`、`candidate_results.jsonl`、`candidate_errors.jsonl`、
  `checkpoint.json`、`gate_summary.json`、`leaderboard.json/md`、`sweep_report.md`
  和 `run.log` 均存在，manifest `production_effect=none`。旧
  `sweep_20260606T024119Z_98fa3c81` 可作为历史样例，但因后续 validator
  增加 `evaluator_mode` 要求，不再作为当前收口证据。Focused tests 16 passed，
  覆盖 checkpoint/resume、candidate error isolation 和 validation contract。
- 2026-06-09：`TRADING-095` 验证发现 Reader Brief / report index 集成缺口。
  当前 canonical real sweep `sweep_20260607T102300Z_ae5ae1d8` 的 leaderboard、
  sweep report 和 candidate report 可运行，但 `aits reports index --latest` 以
  latest decision snapshot `2026-06-05` 作为 as_of，默认 as-of artifact 过滤会把
  2026-06-07 的 sweep leaderboard 视为未来 artifact 排除，导致 Reader Brief
  Dynamic Rescue Parameter Sweep 区块降级为 replay-only。该问题不是用手工
  `--as-of 2026-06-07` 绕过；修复方向是在 `config/report_registry.yaml` 为
  明确的 ad-hoc latest research entry 增加 `artifact_selection_policy=latest_available`，
  同时让 report index 输出 `artifact_selection_policy`、`artifact_temporal_relation`
  和 `artifact_after_as_of`，只对该 entry 放宽选择，不全局改变 daily / PIT
  artifact 过滤。
- 2026-06-09：`TRADING-095` 从 `VALIDATING` 改为 `DONE`。当前 canonical
  real sweep `sweep_20260607T102300Z_ae5ae1d8` 的 `sweep leaderboard`、
  `sweep report` 和 `candidate report` 均可运行；top candidate 为
  `a72139edcaef7d22`，gate=`review_required`，score=`0.460619`，
  `production_candidate_generated=false`。字段级复核确认 leaderboard 包含
  `top_eligible_candidates`、`most_common_reject_reasons`、`metric_distributions`
  和 `recommended_next_actions`；本轮真实结果没有 hard rejected candidates，
  因此 reject reason aggregation 为空但字段存在。Candidate report 包含 7 个参数、
  3 个 gate reasons、27 个 metrics、7 项 score breakdown、3 个 artifact links 和
  recommendation。`aits reports index --latest` 现在将该 leaderboard 标记为
  `FRESH`、`artifact_selection_policy=latest_available`、`artifact_temporal_relation=AFTER_AS_OF`；
  `aits reports reader-brief --latest` 展示 Dynamic Rescue Parameter Sweep 为
  `AVAILABLE/PASS`，candidate_count=300、top_candidate=`a72139edcaef7d22`。
  验证通过 `tests/test_etf_dynamic_v3_parameter_research.py tests/test_report_index.py
  tests/test_reader_brief.py -q`（31 passed）、Reader Brief quality、documentation
  contract、docs freshness 和 diff check。
- 2026-06-09：`TRADING-096` 从 `VALIDATING` 改为 `DONE`。当前 canonical
  real sweep `sweep_20260607T102300Z_ae5ae1d8` 已生成 walk-forward artifact
  `c49f65c76e2b9b73`：`walk-forward run --top-n 20` 输出 status=`PASS`、
  `production_candidate_generated=false`；manifest 绑定 current sweep，
  candidate_count=20、min_windows=2，candidate results 为 40 行，对应 20 个
  sweep leaderboard top candidates x 2 个 walk-forward windows。`walk-forward report`
  披露 holdout `2024-01-01` 到 `2026-06-05`、OOS pass_count=20 和
  recommendation=`continue_to_robustness`；`validate-walk-forward` 为 `PASS`、
  `failed_check_count=0`。Focused test 补充 top-N 来源、holdout/OOS recommendation
  和删除 `wf_report.md` 后 validation fail-closed 断言；后续 robustness /
  overfit 仍由 TRADING-097 继续验证。
