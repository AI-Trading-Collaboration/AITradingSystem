# TRADING-2496 Atlas Reader Status Explanation Renderer V1

## 1. 状态与决策

- task id：`TRADING-2496_ATLAS_READER_STATUS_EXPLANATION_RENDERER_V1`；
- priority：`P1`；
- status：`BASELINE_DONE`；
- governed mode：`SINGLE_LANE`；
- exact base：`d437ae7cf059f58a5cdd14cb61849166dd29b3b9`；
- Owner token：`owner_decision:TRADING-2496:2026-08-03:implement_reader_first_explanation_renderer_v1`；
- predecessor：`TRADING-2495_ATLAS_READER_STATUS_EXPLANATION_CONTRACT_V1`；
- production effect：`none`；broker action：`none`。

用户已接受 Web Pro 的 contract-first 顺序，并要求按 reader-first 顺序改进当前 canonical Atlas 页面。
本任务只做 2495 sidecar 的 renderer consumer 与静态页面重建，不更改研究状态、结果状态、
投资结论、DQ/PIT、研究窗口、模型、回测或交易行为。

## 2. 读者问题与现状缺口

现有节点解释把 `canonical snapshot`、`raw_status`、`display_status`、`source kind` 与 exact id
放在主要阅读层。它们适合审计，却不能先回答普通读者最关心的问题：

1. 这一步现在的结论是什么；
2. 团队此刻具体在做什么；
3. 已经完成了什么；
4. 还缺什么证据或条件；
5. 为什么这会影响我对结果的理解；
6. 什么可观察事件会改变当前状态；
7. 技术依据在哪里。

页面必须把审计信息保留在折叠层，而不是删除；主要阅读层不得再次从 `RUNNING`、`LIMITED`
或自由摘要推测具体原因。

## 3. 冻结的阅读顺序

每个流程节点使用同一顺序：

1. `一句话结论`：只显示 `StatusExplanationRecord.plain_summary`；
2. `正在做什么`：`CURRENT_WORK` fact；
3. `已完成什么`：全部 `COMPLETED_MILESTONE` facts；
4. `还缺什么`：`UNMET_CONDITION` 与 `EVIDENCE_GAP` facts；
5. `为什么重要`：`READER_IMPACT` fact；
6. `什么会改变`：typed `transition_conditions`；
7. `由谁负责 / 下一步怎么读`：`responsible_role` 与 `next_reader_action`；
8. `查看审计依据`：technical refs、authority bindings、checked scope、source refs 与 exact ids。

`PRESENT` 表示已有可核验事实；`NOT_RECORDED`、`NOT_APPLICABLE`、`NOT_YET_DUE`、
`SOURCE_UNAVAILABLE`、`OWNER_DECISION_PENDING` 必须显示为不同的 value-state badge，不能用默认句子
把未知项伪装为事实。

## 4. 数据与合同边界

- renderer 必须通过 `build_status_explanation_bundle` 与
  `validate_status_explanation_bundle` 构造、复验 sidecar；
- `AtlasCitedQueryShowcase` 持有 canonical explanation bundle 与 validation；
- stage set 必须与 `ATLAS_STATUS_EXPLANATION_STAGE_IDS` exact 相等并保持顺序；
- explanation target/status/snapshot fingerprint/source refs drift 必须 fail closed；
- writer 新增 `status_explanations.json` 与 `status_explanation_validation.json`；
- 原 `responses.json`、`validation.json` 与 cited-query public contract 不改变；
- 页面不读取、映射或展示 `TRADING-2481..2493`；
- primary research start 保持 `2021-02-22`；`2022-12-01` 只可作为历史语境。

## 5. 视觉与交互

- 流程卡 summary 保留节点名、状态色与当前位置；
- 展开后先显示 plain-language conclusion，不出现 raw field/path/hash；
- facts 按读者问题分组，缺失事实用低饱和、清晰的“未登记/不适用/待决定”样式；
- “还缺什么”在 LIMITED/RUNNING 节点比“技术依据”更醒目；
- technical evidence 使用嵌套 `<details>`，标题固定为“查看审计依据”；
- provenance ledger 保留，但默认折叠且定位为审计附录；
- 无 script、form、外链、网络请求或自动动作；移动端保持单列可读。

## 6. 实施阶段

### S0：登记与治理

- 建立 task row 与本 requirement；
- 运行 SINGLE_LANE START/LANE preflight；
- 从 exact local main 创建 `codex/trading-2496-atlas-reader-explanation-renderer`。

### S1：Typed consumer

- 在 showcase build 时生成并验证 explanation bundle；
- renderer 只从 typed record 构建 reader sections；
- writer 输出 canonical sidecar 与 validation JSON；
- tamper、status drift、stage mismatch、missing bundle 与 excluded-task leakage fail closed。

### S2：Reader-first page

- 重构流程节点 HTML/CSS；
- 删除主要阅读层中的“状态为什么是这样 / 可以确认 / 不能推出”旧顺序；
- 技术字段移入“查看审计依据”；
- 保持当前研究关注、当前位置、状态颜色与八节点流程图。

### S3：Deterministic artifact 与视觉 QA

- double-build byte-identical；
- static DOM/count/value-state/ordering tests；
- 重建 `outputs/atlas/strategy_research_cited_query/trading_2470_v1/`；
- 通过 in-app browser 检查桌面与窄屏视觉；
- Owner manual visual 仍由用户独立确认，不由自动化伪写 PASS。

### S4：Governed closeout

- 更新 `docs/system_flow.md`、architecture fragments、task shadow 与 append-only compatibility；
- focused、Architecture、Contract、Integration、Reproducibility、exclusive Full；
- ff-only local main、ordinary non-force push、cleanup 与后继 handoff。

## 7. 路径所有权

task-owned：

```text
docs/requirements/TRADING-2496_Atlas_Reader_Status_Explanation_Renderer_V1.md
src/ai_trading_system/atlas/cited_query_renderer.py
tests/atlas/test_cited_query_renderer.py
config/architecture/fragments/modules/atlas_reader_status_explanation_renderer.yaml
config/architecture/fragments/flows/atlas_reader_status_explanation_page.yaml
```

coordinator-owned：

```text
docs/task_register.md
docs/system_flow.md
docs/artifact_catalog.md
inputs/architecture/**
registry/development_tasks_shadow/**
registry/development_tasks_shadow_v2/**
tests/test_arch_004_refactor_policy.py
tests/test_arch_004g_deprecation.py
tests/test_trading2452_architecture_contract.py
tests/atlas/test_historical_projection_review.py
```

ignored artifact：

```text
outputs/atlas/strategy_research_cited_query/trading_2470_v1/index.html
outputs/atlas/strategy_research_cited_query/trading_2470_v1/responses.json
outputs/atlas/strategy_research_cited_query/trading_2470_v1/validation.json
outputs/atlas/strategy_research_cited_query/trading_2470_v1/status_explanations.json
outputs/atlas/strategy_research_cited_query/trading_2470_v1/status_explanation_validation.json
```

不读取、hash、stage 或修改 known-unrelated exclusion。

## 8. 验收标准

1. 八节点与 explanation records exact 一一对应；
2. 主要阅读顺序严格为结论、当前工作、完成项、缺口、影响、改变条件、责任/下一步、依据；
3. 非 PRESENT state 具有明确 badge，不被推断为事实；
4. technical refs 默认折叠且可完整审计；
5. sidecar/validation/page double-build byte-identical；
6. 原 responses/validation bytes 与 public schema 保持兼容；
7. status/fingerprint/source/excluded-task tamper fail closed；
8. 2481–2493 继续排除，primary start 不漂移；
9. static DOM、focused、adjacent 与五级正式门禁 PASS；
10. 页面完成自动视觉 QA，Owner manual visual 独立为 `PENDING` 或用户明确 token；
11. `investment_conclusion_generated=false`、`production_effect=none`、`broker_action=none`。

## 9. Stop conditions

- 2495 bundle validation 不是 PASS/INSUFFICIENT_AUTHORITY 的合法结果；
- 需要从自由摘要、状态颜色或字段名推测具体原因；
- stage/target/status/fingerprint/source ref drift；
- 页面需要读取 2481–2493；
- 需要改变 DQ/PIT、研究窗口、模型、回测、策略状态或投资结论；
- external platform、network、production 或 broker action；
- formal runner 与其他 heavyweight runner 并发。

命中时 fail closed，不以硬编码文案、手工 HTML 或弱化 validator 绕过。

## 10. 生命周期记录

- 2026-08-03：Owner 接受 Web Pro 的 reader-first 信息顺序并要求改进当前页面；任务登记为
  `IN_PROGRESS`，manual visual=`PENDING`。
- workspace：`D:/Work/AITradingSystem`；planned branch：
  `codex/trading-2496-atlas-reader-explanation-renderer`；
- exit condition：typed consumer、页面 artifact、自动视觉 QA、正式门禁、ordinary push、cleanup；
- recoverability：tracked implementation 由 Git/main/SHA 恢复，ignored artifact 可由 exact renderer 重建。
- 2026-08-03：typed consumer、reader-first HTML/CSS、五文件 writer 与 static DOM tests 已完成；
  renderer focused=`9 passed`，Ruff 与 strict mypy PASS。canonical snapshot/diff identity 保持
  `d8a26341... / 730c6bb3...`；连续两次重建五文件 SHA/size byte-identical。原
  `responses.json=c9eee6e0...` 与 `validation.json=8136aee...` 未漂移；新增
  `status_explanations.json=625c7a77...`、`status_explanation_validation.json=ec562ea4...`，
  validation=`PASS`。
- 2026-08-03：in-app browser 自动打开 `file://` canonical page 被 Browser URL policy 拒绝；
  已停止且未使用本地服务器、其他浏览器或间接导航绕过。自动 visual status=
  `NOT_EXECUTED_URL_POLICY`，Owner manual visual=`PENDING_OWNER_REVIEW`。这不等于页面视觉 PASS；
  renderer baseline 可继续进入静态/兼容性与正式门禁。
- 2026-08-03：全部 Atlas + cited-query/status-explanation contract adjacent=`126 passed`。
  compatibility/deprecation 首轮 `184 passed / 3 failed` 为 2487/2494/2495 三处 EOF successor
  仍固定在 2495；提升到 2496 后第二轮 `186 passed / 1 failed`，剩余项为 2495 source replay
  尚未转交 2496 supersession；按 exact successor set 修复后相同 187 项覆盖=`187 passed`。
  historical projection local page test 已改为 2496 exact identity，并同时核验两个新增 sidecar；
  immutable compatibility prefix 与 2496 EOF marker 已重新逐字节验证 PASS。
