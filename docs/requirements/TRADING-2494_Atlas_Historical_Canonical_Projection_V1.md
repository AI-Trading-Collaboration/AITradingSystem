# TRADING-2494：Atlas Historical Canonical Projection V1

最后更新：2026-08-02

稳定任务 ID：
`TRADING-2494_ATLAS_HISTORICAL_CANONICAL_PROJECTION_V1`

优先级：`P1`

状态：`BASELINE_DONE`

Owner 决定：

```text
owner_decision:TRADING-2494:2026-08-02:advance_atlas_page_and_hold_trading_2481_2493_for_owner_review_v1
```

production effect：`none`

broker action：`none`

## 1. 决策背景与精确边界

TRADING-2477 已把五份 Owner-approved historical JSON 注册为 Atlas
`PUBLISHED_ARTIFACT` 并建立 exact-Git typed adapters。TRADING-2479 已完成 review-only
projection pack，冻结六个 candidate nodes、六条 `CONTAINS` edges、五个 results、五条
`NEUTRAL` provenance attributions、状态映射和普通读者展示语言，但没有修改 canonical
snapshot/page。

Owner 现要求继续推进页面模块，同时明确 `TRADING-2481..2493` 仍需再检查。本任务据此只授权：

1. 把 TRADING-2479 已审阅的五份 historical records 投影进 canonical Atlas snapshot；
2. 重建同一 cited-query 页面，使历史研究支线和五个结果进入可审计结果台账；
3. 保持 original historical status、canonical raw status、reader display status 三层分离；
4. 保持当前系统流程焦点、当前研究状态和 Owner decision boundary 不被历史材料改写。

本任务不授权：

- 把 `TRADING-2481..2493` 的工程状态、研究状态或外部平台状态投影进页面；
- 读取或投影 `next_research_program_roadmap`；
- 新建 empirical research、运行 DQ/model/backtest/QuantConnect/cloud/paper/live/production；
- 把 historical `PASS` 解释为当前策略 PASS、投资建议或 promotion readiness；
- 修改 primary research default `2021-02-22`。

## 2. 输入 authority

canonical projection 只消费：

- `config/atlas/source_registry.yaml` 的 13 个 canonical sources 与当前 base graph；
- `config/atlas/historical_source_adapters.yaml` 和 exact Git blobs 重建的五个 typed records；
- `config/atlas/historical_projection_review.yaml` 中已审阅的 stable IDs、状态映射和展示语言；
- 本任务新增的 canonical projection policy 与 Owner token；
- 当前 cited-query builder、renderer 和 independent validators。

五个允许的 source refs 固定为：

```text
historical-b0-baseline
historical-b1-b4-attribution
historical-weight-program-snapshot
historical-monthly-program-review
historical-final-branch-decision
```

`atlas_historical_candidate_next_roadmap_v1`、known-unrelated exclusion 及
`TRADING-2481..2493` 全部不属于输入 authority。

## 3. Canonical projection contract

投影前后的 exact counts：

|Entity|Base|Projected|Delta|
|---|---:|---:|---:|
|sources|13|13|0|
|nodes|21|27|+6|
|edges|22|28|+6|
|results|8|13|+5|
|attributions|12|17|+5|

新增 graph identity 必须逐项等于 TRADING-2479 review pack：

- group node：`campaign-historical-weight-research`；
- 五个 historical node IDs；
- root + five child edges，且 `edge_kind=CONTAINS`；
- 五个 `result-historical-*` IDs；
- 五个 `attr-historical-*` IDs，且 `direction=NEUTRAL`。

所有五个 canonical result 固定：

```text
display_status=LIMITED
investment_facing=false
```

四个 artifact-ready/decision records 的 `raw_status=PASS` 只表示历史 artifact 已形成；
program snapshot 的 `raw_status=LIMITED`。任何页面位置都必须说明 raw/display status 不是投资评级，
历史 branch decision 不是当前 Owner decision。DQ、requested/evaluated window、source ref、limitations
和缺失 DQ `null` 必须原样保留或可从 exact source drilldown 到达，不得补造。

`ResearchResultCard` 以向后兼容的 optional fields 新增 `source_original_status` 与
`status_mapping_rationale`；非 historical results 均保持 `null`。两字段必须同时存在或同时缺失，
renderer 只在 historical cards 上显示第三层“来源原始状态”和映射理由，避免从 limitations prose
反向猜测机器语义。

## 4. 页面设计

canonical cited-query 页面继续保持无脚本、无表单、无外部资源的静态 HTML，并新增：

1. 在研究系统流程中显示隔离的“历史权重研究支线”，视觉上不占用“你在这里”；
2. 结果台账从 8 个扩展到 13 个，新增五张 historical result cards；
3. 历史卡片显式展示“历史材料 / display LIMITED / 非投资结论”；
4. source、DQ/window、limitations 和 `NEUTRAL` provenance 可展开审计；
5. 顶部覆盖说明更新为“代表性主线 + 五份已审阅历史记录”，仍声明不是全仓完整历史；
6. `TRADING-2481..2493` 不出现在页面 DOM、responses、snapshot graph 或状态计数中。

八阶段系统流程的当前焦点继续是 `CITATION_FIRST_QUERY`；本任务不会把历史材料设置为 active
focus，也不会改变 upstream DQ/backtest 是否执行的事实。

## 5. 实施步骤

### S0：登记与 governed contract wave

- 登记本 row/requirement；
- 从 exact local main `3e21bb33f56763f3fbea4539abddd5674817b5ee` 启动
  `SINGLE_LANE`；
- 因 canonical snapshot/page 是 consumer-visible boundary，声明 `contract_change=true`，以本任务
  作为最小串行 contract wave；QQQ options 线保持暂停，不与本任务共享 formal runner。

### S1：Projection policy 与 typed merge

- 新增 canonical projection policy，绑定 Owner token、review policy identity、五个 source refs、
  stable IDs、expected counts 与 safety boundary；
- projector 重验 exact typed records、source registry、review mapping、ID collision、counts、status、
  primary window 和 forbidden set；任一 drift fail closed；
- base snapshot 与 canonical projected snapshot 显式分离，TRADING-2479 review-only builder 仍能重建
  当时的 base/candidate 对比，不被当前激活状态污染。

### S2：Canonical page 与静态验证

- 默认 Atlas bundle 激活 reviewed projection；
- cited-query result ledger、historical lane、覆盖说明和 source drilldown 同步；
- snapshot/response/validation/page deterministic double-build byte-identical；
- static DOM 检查 13 result cards、5 historical cards、1 current marker、0 script/form/external、
  0 `TRADING-2481..2493` token。

### S3：Governance 与 closeout

- 更新 `docs/system_flow.md`、architecture fragments/generated state、task shadow 与 append-only
  compatibility/deprecation authority；
- focused、Architecture、Contract、Integration、Reproducibility、exclusive Full 串行 PASS；
- validated task branch 通过 governed drift/integration 后 ff-only local main、ordinary push、
  evidence/branch/workspace cleanup；
- 输出新 canonical 页面供 Owner 手工视觉与语义复核；该人工复核独立记录，不由自动测试伪造。

## 6. 路径与所有权

task-owned：

```text
config/atlas/historical_canonical_projection.yaml
src/ai_trading_system/atlas/historical_canonical_projection.py
tests/atlas/test_historical_canonical_projection.py
docs/requirements/TRADING-2494_Atlas_Historical_Canonical_Projection_V1.md
```

coordinator-owned：

```text
config/atlas/source_registry.yaml
src/ai_trading_system/atlas/snapshot_builder.py
src/ai_trading_system/atlas/cited_query_renderer.py
src/ai_trading_system/atlas/__init__.py
src/ai_trading_system/atlas/historical_projection_review.py
src/ai_trading_system/contracts/strategy_research_explorer.py
tests/atlas/test_snapshot_builder.py
tests/atlas/test_cited_query_renderer.py
tests/atlas/test_historical_projection_review.py
tests/atlas/test_source_registry_coverage.py
tests/test_strategy_research_explorer_contract.py
docs/task_register.md
docs/system_flow.md
docs/artifact_catalog.md
config/architecture/fragments/modules/**
config/architecture/fragments/flows/**
inputs/architecture/**
registry/development_tasks_shadow/**
registry/development_tasks_shadow_v2/**
tests/test_arch_004_refactor_policy.py
tests/test_arch_004g_deprecation.py
```

不创建外部 cache/clone，不读取 known-unrelated exclusion。canonical ignored page artifacts 写到既有
`outputs/atlas/strategy_research_cited_query/trading_2470_v1/`；保留到 Owner 复核与 canonical evidence
handoff 完成。

## 7. 验收标准

1. Owner token、五个 source refs、review policy identity 和 exact typed records 一一闭合；
2. base=`13/21/22/8/12`、projected=`13/27/28/13/17`；
3. 六个 nodes、六条 `CONTAINS`、五个 results、五个 `NEUTRAL` attributions 无 collision；
4. 五个 display 全为 `LIMITED`、`investment_facing=false`，四 PASS + 一 LIMITED raw 映射 exact；
5. DQ/window/limitations/source lineage 不丢失，missing DQ 保持 null；
6. primary default 仍为 `2021-02-22`，`2022-12-01` 只保留 historical role；
7. page deterministic，13 result cards / 5 historical cards / 1 current marker / no script/form/external；
8. `TRADING-2481..2493` 不进入 page/snapshot/responses/status authority；
9. review-only TRADING-2479 pack 仍可从 base projection 重建，historical artifacts 不被改写；
10. focused/generated/compatibility/formal gates PASS；
11. Owner manual visual status 独立记录，未复核前不得伪报 PASS；
12. `investment_conclusion_generated=false`、`production_effect=none`、`broker_action=none`。

## 8. Stop conditions

- source/blob/typed record/review policy identity drift；
- candidate ID collision 或 counts 不等于 reviewed projection；
- 需要新增因果/时序 edge、`SUPPORTS` attribution 或投资状态；
- 页面需要引入 `TRADING-2481..2493`；
- primary research default、DQ/PIT 或研究结论发生变化；
- external platform、network、production 或 broker action；
- formal runner 与其他 heavyweight runner 并发。

任一 stop condition 命中即 fail closed，不能通过临时 HTTP、手工改 artifact、降级校验或补造
status 绕过。

## 9. 生命周期与进度

- governed mode：`SINGLE_LANE`；
- frozen base：`3e21bb33f56763f3fbea4539abddd5674817b5ee`；
- planned branch：`codex/trading-2494-atlas-historical-canonical-projection`；
- workspace：`D:/Work/AITradingSystem`，复用当前 clean detached exact-main checkout；
- exit condition：实现、验证、ff-only main、ordinary push、branch cleanup 与 canonical page handoff；
- recoverability：tracked implementation 由 Git/main/SHA 恢复，ignored page 由 deterministic writer 重建。

- 2026-08-02：Owner 指示继续推进页面模块，并明确 `TRADING-2481..2493` 仍需再检查；本任务建立，
  只接管 Atlas historical canonical projection，不推进或投影 options 波次。
- 2026-08-02：canonical projection、typed-record validation、snapshot 与 cited-query renderer 已完成。
  canonical counts=`13/27/28/13/17`；冻结 lane（已 hydration 页面）扩展 Atlas + contract focused=
  `106 passed`；原始 50 项回归覆盖重跑
  `50 passed`；Ruff、compileall、新模块 strict mypy、DevEx 与 task-shadow 生成/验证均 PASS。
- 2026-08-02：canonical ignored page 已 deterministic 重建到
  `outputs/atlas/strategy_research_cited_query/trading_2470_v1/`：`index.html` SHA-256=
  `d29a3c3363c6bdbb443c33d8194f74ce261d283d4900f91439794da4e358a365`，
  `responses.json` SHA-256=`c9eee6e0e585d9f109cbb2fd36d99a42dfa48368dcd20d8deb20c24a612c4e13`，
  `validation.json` SHA-256=`8136aeecc56f306fd4453021de88b5cac01dfd1e06436cf3ca56b28c91605dd1`。
  静态检查为 13 result cards、5 historical cards、5 historical flow nodes、1 current marker、
  0 script、0 form、0 `TRADING-2481..2493` token。由于 governed browser policy 禁止以自动化方式
  打开 `file://` 页面，Owner manual visual status 继续独立保留为 `PENDING_OWNER_REVIEW`，不伪报 PASS。
- 2026-08-02：compatibility/deprecation 首轮同覆盖 183 项为 `99 passed / 84 failed`；失败只来自
  ARCH-004G frozen inventory stale 与尚未追加 TRADING-2494 current-hash authority 两个治理级联，
  不代表 84 个实现缺陷。当前只重建 deterministic inventory 并追加 compatibility authority，
  不改写任何历史 prefix，不放宽 exact-byte/removal policy。
- 2026-08-02：冻结 lane commit=`d92761b5cf3998400e425029788e84d5ae6631eb`；2487 完成后
  latest main=`3731ee04d83688c9d9bb321ab0496c77ae65c232`。governed drift plan=
  `integration-revalidation-6140afa51893da8f5696`，decision=`RECONCILIATION_REQUIRED`，无
  contract conflict、undeclared path 或 blocker。最终候选保留 2487 current-checkout relative locator
  与 absolute/`..` 拒绝，同时合入 2494 page identity、五历史节点和 `TRADING-2481..2493`
  exclusion 断言；shared manifests/task shadow/compatibility authority 只在 latest-main final tree 重建。
- 2026-08-02：latest-main candidate 扩展 Atlas + contract focused=`105 passed / 1 skipped`；唯一
  skip 是该 checkout 未 hydration ignored canonical page，符合 2487 durable current-checkout locator，未移动、
  复制或跨 checkout 读取 `D:/Work/AITradingSystem` 中待 Owner 视觉复核的页面 artifact。页面逻辑、
  deterministic writer、snapshot/renderer/contract 覆盖均 PASS。
