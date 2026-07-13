# TRADING-179 to 183 Confirmation Cycle Scheduling and Weekly Evidence Operations

最后更新：2026-07-13

## 状态

- task_register_id: `TRADING-179_to_183_CONFIRMATION_CYCLE_WEEKLY_OPS`
- status: `VALIDATING`
- priority: `P0`
- market_regime: `ai_after_chatgpt`
- default_date_range: `2022-12-01` onward for historical pressure tagging unless the operator requests a narrower range
- production_effect: `none`
- broker_action_allowed: `false`
- auto_apply: `false`
- owner_approval_required: `true`

## 背景

TRADING-174_to_178 已经把 forward confirmation plan 转成 registry、progress、evaluation、rule review cycle 和 owner decision journal。当前真实结论仍是 `continue_tracking`，主要 blocker 是 forward outcome 数量不足、pressure regime tagged outcomes 不足、`defensive_limited_adjustment_drawdown` 无法验证、`limited_adjustment_vs_no_trade` 未达到 required events，以及 `consensus_target_risk` 仍只能 watch only。

本阶段目标是把这些分散命令收敛为每周人工可执行、可验证、可审计的 confirmation evidence operations 闭环。

## 子任务

|任务|范围|P0 完成标准|
|---|---|---|
|TRADING-179|Confirmation cycle runbook and scheduled command pack|新增 schedule config、plan/runbook/validate-config CLI、scheduled command pack artifact，报告明确 dry-run/review-only/owner-review/no broker/no production/no auto apply|
|TRADING-180|Outcome due + confirmation progress combined weekly runner|新增 weekly-run/weekly-report/validate CLI，默认 dry-run，不执行 outcome update；只有显式 `--execute-ready-updates` 才运行 safe outcome update|
|TRADING-181|Pressure regime outcome tagging enhancement|新增 tagging config、run/report/validate CLI，threshold 全部来自配置，输出 window tags、outcome tags 和 defensive validation relevant counts|
|TRADING-182|Confirmation evidence weekly dashboard|新增 dashboard build/report/validate CLI，聚合 due/update/progress/evaluation/rule review/pressure samples 和 Reader Brief section|
|TRADING-183|Owner rule review queue management|新增 queue build/report/validate CLI，集中展示 pending/reviewed/deferred/not_ready/ready owner review items，默认不允许 policy change|

## 实施顺序

1. 新增 `confirmation_cycle_schedule_v1.yaml` 和 `pressure_regime_tagging_v1.yaml`。
2. 新增 shared operations module，复用现有 outcome due/update/review/refresh/progress/evaluation/rule review helpers。
3. 在 CLI 中增加薄封装和 validate commands。
4. 增加 focused tests，覆盖 config validation、weekly dry-run、explicit update flag、pressure thresholds、dashboard、queue 和 safety flags。
5. 更新 README、operations runbook、system flow、report registry、artifact catalog、Reader Brief 文档索引。
6. 运行 focused validation、ruff、compileall、git diff check；尽量运行 full pytest。

## 安全边界

- 不接入 broker API。
- 不自动下单。
- 不生成 automatic production candidate。
- 不自动 owner approval。
- 不修改 `config/etf_portfolio/dynamic_v3_rescue/position_advisory_v1.yaml`。
- 不自动应用 rule proposal。
- `policy_change_allowed=false`、`broker_action_allowed=false`、`broker_action_taken=false`、`production_effect=none` 必须写入所有新增 artifacts。

## Dry-run 设计

Weekly runner 默认只执行 outcome due scan、update review、rolling evidence refresh 的 dry-run placeholder、confirmation progress、confirmation evaluation、rule review cycle、owner queue 和 dashboard 汇总。默认不运行 `outcome-update`，因为 outcome update 会改变 forward outcome artifacts，必须先由 owner 复核 update review pack。

`--execute-ready-updates` 是显式开关。开启后 runner 才会调用既有 safe outcome update helper，并继续写出 updated/skipped audit。即使开启更新，也不改变 policy、target weights、portfolio、baseline config 或 broker state。

## Pressure Regime Tagging 设计

第一版使用 price behavior proxy，并且全部阈值从 `config/etf_portfolio/dynamic_v3_rescue/pressure_regime_tagging_v1.yaml` 读取：

- `tech_drawdown`: tech proxy rolling drawdown below configured threshold。
- `semiconductor_pullback`: semiconductor proxy rolling drawdown below configured threshold。
- `risk_off`: tech drawdown plus volatility percentile pressure。
- `sideways_choppy`: low absolute trend slope plus elevated volatility。
- `strong_recovery`: prior drawdown followed by configured positive rolling return。
- `ai_trend`: tech/semiconductor trend positive and drawdown not severe。

这些 tags 只用于 confirmation evidence accumulation，不授权 defensive label、policy mutation 或 production action。

## Dashboard 阅读口径

Dashboard 必须回答：

- 本周 due windows、updated windows。
- confirmation targets total、ready_for_evaluation、continue_tracking。
- `limited_adjustment_vs_no_trade` 当前 available/required events 和 progress_pct。
- `defensive_limited_adjustment_drawdown` 当前 pressure samples 和缺口。
- `consensus_target_risk` 是否仍为 watch only/reference only。
- rule review recommendation 和 owner action required。
- safety boundary 是否仍然 no broker / no production / no auto apply。

## Owner Queue 口径

Owner rule review queue 从 latest rule review cycle 和 owner decision journal 汇总：

- not_ready items 不要求 owner action。
- ready/failure items 才进入 owner review。
- reviewed items 只记录人工 journal 状态，不应用 policy。
- deferred items 代表 evidence mixed 或 owner decision defer/request_more_data。

## 进展记录

### ARCH-004G2.4CD contract hardening

2026-07-13 进入`IN_PROGRESS`。本slice不把旧baseline smoke的`PASS`视为content-derived保证，重新冻结以下契约：

- Weekly：schedule config、Registry与每个已执行step必须在写weekly artifact前通过对应validator并满足cutoff/lineage；input snapshot记录exact ids、bounded file commitments与validation。可选step只能对“确实不存在的可选输入”标`SKIPPED`，invalid artifact或非预期异常必须FAIL，不得用广义异常掩盖。
- Pressure：在创建output前完成timezone、date range、config和DQ前置检查；price/rates、DQ evidence与选中的Advisory Outcome必须冻结并验证，future/duplicate/invalid relevant outcome fail closed。Price behavior proxy仍只作研究标签，不得提升为已验证因果regime label。
- Dashboard：只投影同一条validated Progress→Evaluation→Rule Review→Queue lineage及可选validated Weekly/Pressure source；missing与invalid分开表达。`available_*`与readiness完全继承Progress，Pressure只作补充展示，不得回写或覆盖Progress count。
- Queue：Rule Review source必须PASS/cutoff；owner decision只有在event chain合法、source Cycle与当前Cycle完全相同、target在该Cycle decision scope且final event不晚于cutoff时才可标`reviewed`。其他Cycle相同target不得串扰。
- Validation：Plan、Weekly、Pressure、Dashboard、Queue五类artifact都保存versioned bounded input snapshot；validator重验live source commitments并从snapshot重算JSON/JSONL/Markdown。新producer不得产生legacy unsnapshotted artifact；旧artifact仅允许只读warning，不进入当前结论。

退出要求：16 callback完成迁移且legacy root definition/import为0；focused tamper/cutoff/lineage/missing-vs-invalid/cross-Cycle tests、CLI parity、architecture、contract与Ruff通过；研究执行链、system flow、runbook、report registry、artifact catalog、manifests/deprecation/attribution同步。该slice仅生成operations evidence，默认dry-run，`production_effect=none`，完成后G2.4仍继续且不触发phase-level handoff。

2026-07-13 完成并转为`COMPLETE`。16 callback已迁canonical interface module，legacy root降至`22,538 lines / 657 functions / 618 decorators`；generated inventory为`894 modules / 1,114 tests / 858 direct writers / 0 violations`，CLI保持`993 leaf / 0 duplicate`。Focused累计580通过；正式architecture-fitness 265通过（`outputs/validation_runtime/architecture-fitness_20260713T152448Z/test_runtime_summary.json`），contract-validation 203通过（`outputs/validation_runtime/contract-validation_20260713T152711Z/test_runtime_summary.json`）。Plan config/path/live drift、Weekly step-chain/tamper、Pressure PENDING/invalid outcome/DQ preflight、Dashboard invalid optional source与Progress readiness、Queue cross-Cycle target均有fail-closed覆盖。当前fixture保持weekly 0 updates/0 ready、Pressure 0 defensive evidence、Dashboard 1 target/0 ready、cross-Cycle queue不误标reviewed；这些是当前证据状态，不是策略有效性结论。深链回归的主要研发效率债是重复验证同一上游bundle，后续只允许按source hash+validator version缓存PASS证据并保留cutoff/lineage/live drift重验，不允许删减gate。默认dry-run，`production_effect=none`；G2.4 callback/migration matrix继续，未到phase exit或handoff。

2026-06-11 baseline 实现完成并转入 `VALIDATING`。真实 CLI smoke 生成 artifacts：

- confirmation cycle plan `46dbf3d3f799b570`，status=`PASS`。
- weekly dry-run cycle `efc5b421b9fe3f32`，status=`PASS`，week_ending=`2026-06-11`，due_windows=1，updated_windows=0，ready_for_evaluation=0，rule_review_recommendation=`continue_tracking`。
- pressure regime tag `1bc4775787a6606d`，status=`PASS_WITH_WARNINGS`，date range=`2022-12-01` to `2026-06-11`，tech_drawdown=185，risk_off=124，semiconductor_pullback=272，defensive_validation_relevant_outcomes=0。
- confirmation dashboard `583f2901ecec9d0c`，status=`PASS`，targets_total=3，ready_for_evaluation=0，dashboard_recommendation=`continue_tracking`。
- rule review queue `67ad255aeb9085b1`，status=`PASS`，pending_count=0，ready_for_owner_review_count=0。

Validation passed: schedule config validate, pressure tagging config validate, four artifact validators, dynamic-v3 root validation, dynamic-v3 family artifact validation, report index latest, Reader Brief latest, Reader Brief quality, focused pytest 5 passed, `ruff check src tests`, `compileall src tests`, `git diff --check`, and full validation tier 2325 passed / 641 warnings in 124.72s.

Remaining validation context: pressure tagging produced enough proxy pressure windows but zero defensive-validation-relevant outcomes, so `defensive_limited_adjustment_drawdown` remains an evidence blocker rather than an approved defensive rule. Weekly run was intentionally dry-run and did not mutate outcome artifacts.

## 验收命令

```bash
aits etf dynamic-v3-rescue confirmation-cycle plan --config config/etf_portfolio/dynamic_v3_rescue/confirmation_cycle_schedule_v1.yaml
aits etf dynamic-v3-rescue confirmation-cycle weekly-run --week-ending 2026-06-21 --config config/etf_portfolio/dynamic_v3_rescue/confirmation_cycle_schedule_v1.yaml
aits etf dynamic-v3-rescue pressure-regime-tag validate-config --config config/etf_portfolio/dynamic_v3_rescue/pressure_regime_tagging_v1.yaml
aits etf dynamic-v3-rescue pressure-regime-tag run --start 2022-12-01 --end 2026-06-21
aits etf dynamic-v3-rescue confirmation-dashboard build --week-ending 2026-06-21
aits etf dynamic-v3-rescue rule-review-queue build
```

Required validation:

```bash
aits etf dynamic-v3-rescue validate
aits etf dynamic-v3-rescue confirmation-cycle validate-config --config config/etf_portfolio/dynamic_v3_rescue/confirmation_cycle_schedule_v1.yaml
aits etf dynamic-v3-rescue pressure-regime-tag validate-config --config config/etf_portfolio/dynamic_v3_rescue/pressure_regime_tagging_v1.yaml
aits etf dynamic-v3-rescue validate-confirmation-cycle-weekly --weekly-cycle-id <weekly_cycle_id>
aits etf dynamic-v3-rescue validate-pressure-regime-tag --tag-id <tag_id>
aits etf dynamic-v3-rescue validate-confirmation-dashboard --dashboard-id <dashboard_id>
aits etf dynamic-v3-rescue validate-rule-review-queue --queue-id <queue_id>
aits etf dynamic-v3-rescue artifacts validate --family dynamic_v3_rescue
```

## 开放问题

- P1: 更精细的 pressure regime labels 需要更多样本和 owner 复核是否接受 proxy 口径。
- P1: Weekly runner 的 artifact delta 可以继续下钻到 target-level before/after。
- P2: 自动日历调度、owner reminder 和图表化 dashboard 暂不进入本阶段 P0。
