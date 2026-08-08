# TRADING-2503 Atlas QQQ Options Serial Projection Contract and Renderer Consumer V1

最后更新：2026-08-08

稳定任务 ID：`TRADING-2503_ATLAS_QQQ_OPTIONS_SERIAL_PROJECTION_CONTRACT_RENDERER_CONSUMER_V1`

优先级：`P1`

状态：`BASELINE_DONE`（工程 baseline 已完成；Owner manual visual 仍待验收）

governed mode：`SINGLE_LANE`

exact registration base：`154b12f2820f54441b7f7920465b469f31f52caf`

contract change：`true`

Owner authority：
`owner_decision:TRADING-2501:2026-08-08:accept_read_only_owner_review_pack_recommendations_v1`

production effect：`none`

broker action：`none`

## 1. 目标

把 TRADING-2501 已由 Owner 接受的 13 项 QQQ Options 审阅事实，串行固化为 typed、可重放、
fail-closed 的 Atlas projection contract，并由现有 cited-query renderer 消费。页面要让金融知识较少的
读者先看见“当前为什么仍是 NO-GO”，再理解四组模块分别做到哪里、还不能证明什么、仍由什么阻塞。

本任务只改变报告投影与页面解释，不改变 QQQ Options 研究模块、task lifecycle、DQ/PIT、投资阈值、
外部平台 authority、回测结果或策略结论。`engineering_baseline=BASELINE_DONE` 不能被转换成
`strategy_conclusion=PASS`。

## 2. 冻结 authority

直接继承 TRADING-2501：

- accepted set `A={2481,2482,2483,2492,2493}`；
- accepted set `B={2484,2489,2490,2491}`；
- accepted set `C={2485,2486,2487,2488}`；
- accepted set `D={}`；
- exact 13-source replay manifest 与 source-set SHA-256
  `29c97b0524c0ccf2ce1b215da9122bbfa875f45b08d682145a7409d6c1abd11f`；
- 2489 `SOURCE_STATUS_MISMATCH_REVIEW_REQUIRED` 必须披露；
- 2492 顺序必须是 `NO-GO → 原因 → 734127 > 250000 → 1 order / 1 fill`；
- 2493 aggregate `NO_GO_KEEP_BLOCKED` 支配 subordinate `CONDITIONAL_GO`；
- primary requested/evaluated start=`2021-02-22`，`2022-12-01` 仅是 legacy context；
- no-green、no-strategy-PASS、no-new-threshold、no-external-action hard stops。

任一 source path、Git blob、byte count、task id、accepted layer、dominance 或 Owner token 漂移都必须
fail closed；renderer 不得从 task status 或自由文本自行补写原因。

## 3. Typed projection contract

V1 bundle 固定包含：

1. bundle identity、schema/version、Owner token、source-set identity、primary window；
2. 当前 aggregate conclusion 与其支配性原因；
3. 四个 reader groups；
4. 恰好 13 个 task cards；
5. 每个 card 的五层状态：`engineering_baseline`、`evidence_quality`、`policy_readiness`、
   `external_authority`、`strategy_conclusion`；
6. 每个 card 的 reader-first 五句：一句话定位、已完成、未证明、仍阻塞、读者下一步；
7. accepted layer A/B/C 与 exact source reference；
8. projection validation summary 与 deterministic canonical bytes。

public contract 必须提供 canonical seal/replay 与 rebuild validation；缺项、重复、乱序、hash 漂移、
非法绿色/策略 PASS 语义、2492/2493 顺序或支配关系破坏均拒绝。

## 4. 页面信息架构

新增页面区块标题：`QQQ 期权研究链：做到哪里、还缺什么`。固定阅读顺序：

1. **当前总判定**：`NO_GO_KEEP_BLOCKED`，并用普通语言说明“工程底座持续推进，但还不能证明策略有效”；
2. **为什么仍阻塞**：优先展示 2492 cap violation 与 2493 signed NO-GO；
3. **四组能力地图**：底座合同、policy-blocked mechanics、证据脚手架、external evidence/governance；
4. **13 个节点详情**：默认先读结论和阻塞，技术 source evidence 折叠；
5. **读者能确认 / 不能推出 / 下一步看什么**：避免 status code 成为新的疑问。

视觉语义只允许 neutral/blue、amber、violet/gray 与 red/no-go；不得使用绿色主状态。A/B/C 是展示层，
不是成绩、优先级或完成度。

## 5. 实施顺序

1. S0：登记 task row 与 supporting requirement，完成 governed START/LANE；
2. S1：先实现 projection contract、Owner-reviewed policy 与 exact-source validator；
3. S2：加入 13-card typed builder、canonical replay 与 focused tests；
4. S3：让 cited-query renderer 只消费 validated bundle，并输出 projection/validation sidecar；
5. S4：更新 `docs/system_flow.md`、architecture fragments、DevEx/task shadow/current authority；
6. S5：focused/adjacent/static DOM/compatibility 与最终五级 gates；
7. S6：生成 canonical local page，用 in-app browser 做视觉与响应式检查；
8. S7：governed integration、ordinary non-force push、verify 与 cleanup。

## 6. 路径所有权

task-owned：

```text
docs/requirements/TRADING-2503_Atlas_QQQ_Options_Serial_Projection_Contract_Renderer_Consumer_V1.md
src/ai_trading_system/contracts/strategy_research_qqq_options_projection.py
config/atlas/qqq_options_projection.yaml
src/ai_trading_system/atlas/qqq_options_projection.py
tests/atlas/test_qqq_options_projection.py
config/architecture/fragments/modules/atlas_qqq_options_projection.yaml
config/architecture/fragments/modules/strategy_research_qqq_options_projection_contract.yaml
config/architecture/fragments/flows/atlas_qqq_options_projection_page.yaml
```

consumer/coordinator-owned：

```text
src/ai_trading_system/atlas/cited_query_renderer.py
src/ai_trading_system/atlas/__init__.py
src/ai_trading_system/contracts/__init__.py
tests/atlas/test_cited_query_renderer.py
tests/atlas/test_historical_projection_review.py
config/architecture/fragments/modules/atlas_reader_status_explanation_renderer.yaml
docs/system_flow.md
docs/artifact_catalog.md
docs/task_register.md
inputs/architecture/** current authority
registry/development_tasks_shadow/**
registry/development_tasks_shadow_v2/**
tests/test_arch_004_refactor_policy.py
tests/test_arch_004g_deprecation.py
tests/test_trading2452_architecture_contract.py
```

明确不拥有或不修改：

```text
src/ai_trading_system/qqq_options_research/**
config/research/qqq_options_*.yaml
docs/requirements/TRADING-2481..TRADING-2502 historical authority
outputs/atlas/** tracked content
registered known-unrelated exclusion
```

## 7. 验收标准

1. 13 个 exact source 一一对应且 deterministic replay；
2. 四组、五层状态、A/B/C layer 与每项五句解释完整；
3. 2492 cap violation 先于 order/fill，2493 aggregate NO-GO 支配 conditional axes；
4. 2489 mismatch、primary window、synthetic/manual/bounded limitations 明示；
5. 页面主层不出现策略 PASS、收益证明、投资建议、promotion 或绿色成功暗示；
6. renderer 只消费 validated projection，不从 raw requirement/task status 猜原因；
7. JSON sidecar 与 HTML deterministic，现有 cited-query response/validation bytes不回归；
8. desktop/mobile static DOM 与人工视觉检查通过；
9. system flow、architecture/generated/current authority 与 focused/formal gates PASS；
10. `investment_conclusion_generated=false`、`production_effect=none`、`broker_action=none`。

## 8. 生命周期记录

- 2026-08-08：TRADING-2501 Owner 接受 reader-first A/B/C/D、2489 mismatch、2492 顺序与 2493 dominance；
- 2026-08-08：TRADING-2502 registration boundary 推送并释放 shared paths，exact main=
  `154b12f2820f54441b7f7920465b469f31f52caf`；
- 2026-08-08：登记 TRADING-2503 serial projection contract/renderer consumer，开始 governed S0。
- 2026-08-08：typed contract、exact-source validator、四组/五层 projection、renderer consumer、
  两份新 sidecar、system flow 与 architecture fragments 已完成；focused contract/renderer=`17 PASS`，
  Atlas adjacent=`112 PASS`，Ruff/strict mypy PASS，DevEx=`1098 modules / 1261 tests / 856 writers /
  0 violations`，task shadow=`970 / 465 / 505`、v1/v2 byte-identical。
- 2026-08-08：canonical page 连续 writer 输出 7 个 artifacts；SHA-256：`index=1bbfb90f...ea337`、
  `qqq_options_projection=cf22c775...a26df`、`qqq_options_projection_validation=f05a577c...348e2`。
  in-app Browser 访问本地 `file://` 被 URL policy 拒绝；按 Browser skill 未用 localhost、其他浏览器或
  间接导航绕过，automatic visual=`NOT_EXECUTED_URL_POLICY`，Owner manual visual=
  `PENDING_OWNER_REVIEW`。这不影响 static DOM/contract 证据，但任务不提前写为 Owner visual PASS。
- 2026-08-08：Atlas adjacent 首轮发现 historical review 仍固定 2496 canonical identity；按 durable
  successor boundary 更新为严格核验 2503 page、13 个 QQQ task marker、aggregate NO-GO 与四份 sidecar
  exact identity，不改 2479/2496 历史 policy 或旧 compatibility payload。
- 2026-08-08：首次 final Full=`8641 passed / 2 failed / 3 skipped`，唯一两项失败来自
  `tests/test_trading2452_architecture_contract.py` 把 2501 写死为最后 successor authority；保留 parent
  `outputs/validation_runtime/full_20260808T115902Z/test_runtime_summary.json`。failure-fix 仅让 2503
  successor 进入 direct current-live-hash 核验，不放宽 exact hash，也不改历史 compatibility payload；
  修复后必须从 final bytes 重跑五级，并以 `failure_fix_rerun` 绑定该 parent。
