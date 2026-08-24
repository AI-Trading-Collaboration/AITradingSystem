# TRADING-2545：Atlas 当前状态支配与首屏决定卡修复 V1

最后更新：2026-08-24

- stable task id：`TRADING-2545_ATLAS_CURRENT_STATE_DOMINANCE_AND_READER_CARD_REPAIR_V1`
- priority：`P0`
- status：`DONE`
- owner：Codex Atlas coordinator
- governed mode：`SINGLE_LANE`
- exact base：`faea3a6a5dc561008cfae793a6caad0b239b4777`
- production effect：`none`
- broker action：`none`
- external action：`none`

## 1. 问题与目标

Project Owner 指出 Atlas 首屏仍显示“1 天全日未出现期权链”和“先解释唯一缺链交易日”，
但 `TRADING-2541` 已通过一次受治理 zero-order Cloud validation 完成 exact-date recovery：
normal sessions=`1201`、recovered sessions=`1`、unresolved sessions=`0`、sessions=`1202/1202`。

只读诊断确认当前页面混合了两条不一致的状态链：

1. live canonical snapshot、task coverage 与审计区已经覆盖 `TRADING-2541` 及后继任务；
2. 首屏 reader decision cards 与 why-first 因果回答仍在 renderer 中硬编码
   `TRADING-2533` 的历史 DQ/PIT admission 结果；
3. validator 能重放 snapshot、task coverage 和 reader state，却没有证明首屏可见主线、阻塞和下一步
   与同一 live authority 一致；
4. 相邻测试继续断言历史 `1 PASS / 1 FAIL / 13 NOT_EVALUATED` 文案必须出现在当前首屏，
   因而把语义漂移固化为预期行为。

目标是建立单一当前状态推导：历史 DQ/PIT admission 继续作为 immutable historical fact 展示，
但不得覆盖后继 transport recovery；当前页面必须分轴表达 transport completeness、DQ/PIT promotion、
threshold/Owner policy 与 strategy/engine authority。

## 2. 状态支配规则

- `TRADING-2533` 保留为历史 admission：当时结论为 DQ=`FAIL`、PIT=`NOT_EVALUATED`；
- `TRADING-2541` 支配“当前是否仍有缺链 session”这一 transport 事实：
  current transport completeness=`1202/1202`、unresolved=`0`；
- `TRADING-2541` 明确记录 `dq_pit_promoted=false`，因此 transport PASS 不得被提升为 canonical
  DQ/PIT PASS；
- threshold、Owner review、selection、engine、investment、production 与 broker 状态继续按最新
  live mainline/blocker authority fail closed；
- 当前下一步不得再写“解释唯一缺链交易日”或暗示需要重跑该 Cloud transport validation；
  应指向 `TRADING-2541` 证据的 canonical DQ/PIT 准入以及仍未批准的 DQ/threshold authority。

## 3. 实施步骤

### S0：任务与契约登记

- 登记本 canonical task event 与 supporting requirement；
- 冻结 predecessor/successor 支配规则、四轴状态边界和页面可见语义；
- 记录隔离 worktree 生命周期。

### S1：单一 reader decision projection

- 从 live canonical task coverage/policy 构建 typed reader decision projection；
- 首屏四张决定卡与 why-first 因果回答只消费该 projection，不再各自硬编码历史状态；
- 历史任务卡仍保留 2533、2537 等原始结论，且明确限定为历史时点。

### S2：验证与回归

- validator 独立重建 reader decision projection，并验证 rendered HTML 的可见卡片与 source refs；
- negative tests 必须拒绝 predecessor 覆盖 successor、transport PASS 冒充 DQ/PIT PASS、
  stale next action 和 current/historical 混淆；
- 更新 `docs/system_flow.md`，刷新受影响的 Atlas/page-effectiveness/compatibility authority；
- 运行 focused pytest-xdist 与适用 Architecture、Contract、Integration、Reproducibility、Full。

### S3：收口

- 在 final exact commit 上重建 canonical Atlas HTML 与 sidecars；
- 完成 final-tree validation、task terminal update、local-main integration 与普通 non-force push；
- 审计并删除隔离 worktree/branch，确认无唯一证据或活动进程依赖。

## 4. 验收标准

1. 首屏明确显示 transport 已补齐：normal=`1201`、recovered=`1`、unresolved=`0`、sessions=`1202/1202`；
2. 同一首屏以普通读者文案明确显示“整体数据可信性尚未提升为通过”，并以审计属性保留
   `dq_pit_promoted=false`，不得把 transport PASS 写成 DQ/PIT PASS；
3. 不再把“解释唯一缺链交易日”作为当前下一步；
4. reader decision cards、why-first 因果回答、边界提示与 source refs 由同一 typed current-state
   projection 生成；page-effectiveness validator 能从 live policy、task coverage 与 2541 terminal evidence
   独立重建并核验最终 HTML；
5. `TRADING-2533` 历史 DQ/PIT 结论仍可审计，但不冒充当前 transport 事实；
6. stale predecessor dominance、丢失 2541、错误 DQ/PIT promotion 与 rendered-card drift 全部 fail closed；
7. canonical page/sidecars 在 final commit 重建并显示 `CURRENT` 仅当语义和身份同时一致；
8. `primary_research_start=2021-02-22`、`investment_conclusion_generated=false`、
   `order_authorized=false`、`real_engine_authorized=false`、`production_effect=none`、
   `broker_action=none` 保持不变。

## 5. Path 与 ownership

Task-owned implementation paths：

- `src/ai_trading_system/atlas/cited_query_renderer.py`；
- `src/ai_trading_system/atlas/live_snapshot.py` 或窄范围 Atlas typed projection module；
- `src/ai_trading_system/atlas/page_effectiveness.py`；
- 必要的 `src/ai_trading_system/contracts/strategy_research_*` typed contract；
- `config/atlas/live_snapshot.yaml`、`config/atlas/page_effectiveness.yaml`；
- `scripts/render_atlas_strategy_research_page.py` 的 repository-local import identity；
- Atlas focused tests。
- `tests/test_arch_005_s5_task_source_cutover.py` 的 canonical task-count baseline。

Coordinator paths：

- 本 requirement、canonical task registry/index 与 generated task views；
- `docs/system_flow.md`、`docs/artifact_catalog.md`（仅在 artifact contract 变化时）；
- architecture/report-flow/compatibility/DevEx generated authority；
- `inputs/architecture/arch_004e_module_manifest.yaml`、`arch_004e_test_manifest.yaml`、
  `arch_004e_aggregate_shadow_index.yaml` 与 `arch_004e_architecture_fitness.yaml`；
- ignored canonical Atlas HTML/JSON/validation sidecars 与 formal runtime artifacts。

## 6. Workspace 生命周期

- task branch：`codex/trading-2545-atlas-current-state-dominance`；
- isolated worktree：`D:\Work\AITradingSystem_trading2545_atlas_state`；
- purpose：隔离当前主 checkout 中 DEVX-009 的未提交改动，并实施本任务；
- exit condition：task commit 已进入 validated local/remote main，canonical artifacts 与 formal evidence
  已在权威位置闭合，无 active process、tracked/untracked/ignored unique residue；
- cleanup：满足 exit condition 后运行 governed audit、`git worktree remove` 与 `git worktree prune`；
- recovery：合入前由 task branch 恢复，合入后由 local/remote main 与 canonical artifact hashes 恢复。

## 7. 进度记录

- 2026-08-24：Owner 要求修复 Atlas 期权状态逻辑。READ_ONLY 诊断确认首屏 reader cards 与
  why-first 因果回答硬编码 2533 历史语义，同时审计区已包含 2541 recovery；当前 checkout
  有 DEVX-009 未提交改动，故从 exact local main `faea3a6a5dc561008cfae793a6caad0b239b4777`
  创建上述隔离 worktree，只先登记本任务与需求，implementation 尚未开始。
- 2026-08-24：实现 `atlas_reader_decision_projection.v1`，严格重放 2541 terminal evidence，把
  transport=`1202/1202` 与 `dq_pit_promoted=false`、threshold/Owner review、strategy/engine boundary
  分轴投影到首屏；validator 新增 rendered-card 文本/source/hash 独立核验。33 项 Atlas focused
  pytest-xdist 回归通过，包含把 `1202/1202` 篡改为 `1201/1202` 时 fail closed 的负向测试。
- 2026-08-24：Ruff PASS；canonical task 状态进入 `VALIDATING`，开始 Architecture、Contract、
  Integration、Reproducibility 与 final Full 正式验证。
- 2026-08-24：Architecture 初次并行运行发现 canonical task-count baseline 与 DevEx generated
  manifests 随新增任务过期（2 failed / 864 passed）；未改用串行掩盖。扩展受治理 path claim，更新
  1023 task baseline，并通过官方 `architecture_devex.py generate` 刷新 module/test/aggregate authority。
  两项根因用例并行通过，Architecture 完整重跑 866 passed；Contract 276 passed、Integration
  995 passed、Reproducibility 24 passed。task 状态更新为 `DONE`；clean exact-commit Full、canonical
  Atlas artifact rebuild、local-main/remote integration 与 workspace cleanup 作为 closeout 继续执行。
- 2026-08-24：terminal update 的 notes 初次未继续携带 requirement path，canonical projector 因而把
  `requirement_refs` 投影为空，收口回归按预期 fail closed（27 failed / 7 passed）。通过同一 canonical
  writer 追加 DONE→DONE 修正事件，在 notes 中恢复本 requirement path；未手工编辑 fragment 或生成视图。
- 2026-08-24：在 task commit 上运行正式 Atlas writer 时发现当前 Python editable install 指向主 checkout，
  导致脚本返回 PASS 却导入旧 renderer/validator 并生成旧首屏。正式入口现将自身 repository-local `src`
  置于 import path 首位；新增 poisoned external install 回归，防止 worktree/exact-commit 发布再次加载别处代码。
- 2026-08-24：独占 final Full 为 `9471 passed / 16 failed / 5 skipped`。16 项失败归为三类：
  `docs/system_flow.md` 变更后的 DEVX-006C/006D compatibility shadow 未刷新；9 项 O1 ledger 测试所需的
  ignored TRADING-2464 DQ fixture 未在隔离 worktree 水合；历史 canonical page coverage 断言停在 2544。
  按 reviewed precedent 从主 checkout 逐字节水合 4,057-byte DQ fixture，并验证 SHA-256=
  `ca02b4310f99d664bb8d987debd4900f4367935b3938663c7a633400d988a1ca`；刷新官方 authority、补齐
  2545 coverage 后，使用该失败 Full runtime artifact 作为 `failure_fix_rerun` parent 完整重跑。
- 2026-08-24：16 个失败点的修复聚焦回归为 `42 passed`。诊断同时发现 compatibility authority
  writer 与 Atlas writer 存在相同的 editable-install 身份风险：脚本可能从其他 checkout 导入旧实现，
  生成与当前 worktree 不一致的片段后仍报告 PASS。现将该 writer 绑定 repository-local `src`，把
  `docs/task_register_completed.md` 纳入当前 supersession authority，并刷新 DEVX-006C/006D 与 DevEx
  生成证据；Ruff 和两套 authority validate 均 PASS。下一步只剩 final commit 上的 canonical Atlas
  rebuild 与 parent-bound 并行 Full。
- 2026-08-24：在 clean exact commit `ebd30e642d4cce236eed7e3fadac0f8b02cd2be4` 重建 canonical
  Atlas 后，page-effectiveness 为 `PASS/CURRENT`，独立核验 visible projection、transport/DQ-PIT
  分轴与 successor dominance；对应 identity 回归 `1 passed`。随后以失败运行
  `full_20260824T023713Z` 为 parent 的正式并行 Full 为 `9487 passed / 5 skipped`，runtime artifact 为
  `outputs/validation_runtime/full_20260824T032554Z/test_runtime_summary.json`。本条最终治理证据绑定会形成
  新 exact commit；发布仍要求该最终 commit 的 fresh Full PASS，运行证据保存在 ignored runtime authority，
  不再用新的 tracked 事件追写自身结果，避免形成无穷自引用验证循环。
