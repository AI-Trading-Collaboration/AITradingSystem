# TRADING-2466：Atlas Strategy Research Path And Attribution Explorer

最后更新：2026-07-30

稳定任务 ID：
`TRADING-2466_ATLAS_STRATEGY_RESEARCH_PATH_AND_ATTRIBUTION_EXPLORER`

优先级：`P1`

状态：`BASELINE_DONE`

Owner 决定：

```text
owner_decision:TRADING-2466:2026-07-30:register_atlas_strategy_research_path_and_attribution_explorer_v1
```

production effect：`none`

broker action：`none`

## 1. 产品定义与目标

Atlas 定义为只读的：

```text
Strategy Research Path & Attribution Explorer
```

它面向金融知识较少的读者，回答：

- 当前主要研究问题与研究主线是什么；
- 已经执行、尚未执行和禁止执行的研究动作分别是什么；
- 数据事实、规则判断、模型结果、研究者解释与 Owner 决策如何区分；
- 结果为何为 `PASS`、`FAIL`、`INSUFFICIENT`、`INVALID`、`BLOCKED` 或未评估；
- 哪些 evidence 支持、反对或限制当前结论；
- 为什么推进、停止、替换或重新开放；
- 如何回溯 exact task、policy、report、artifact、DQ/PIT 与 Git identity。

Atlas 不是 strategy calculator、automatic recommendation engine、weight generator、
production control plane、broker 或 trade entry。展示层不得重新计算 score、gate、model
result、backtest、promotion、Owner decision 或投资语义。

## 2. 决策上下文

Owner 要求先解除 shared/generated 冲突放大，再恢复策略线与 Atlas 两线并行。DEVX-006
Task Shadow v2 B 波已经完成，证明文件级并行门禁可恢复；但 shared schema、public contract、
status semantics、DQ/PIT、known_at/available_at、cache/publication identity 仍须先完成最小
serial contract wave。

2026-07-30 Web Pro exact-Git advisory：

```text
conversation_url=https://chatgpt.com/c/6a6ac917-8d38-83ee-9161-6e6fab018828
reviewed_commit=26e76d25b425957926a37ce8be5e55c58d356f37
specified_blob_sha_match=7/7
classification=UI_PRO_AND_SELF_REPORT_PRO_ROUTE_UNVERIFIED
CANNOT_VERIFY_EXACT_BACKEND_ROUTE=true
```

网页回答是 planning evidence，不是后端 route attestation 或仓库 authority。Owner 已显式采纳
Atlas task、read-only 产品边界和 serial→dual→single integration 顺序。

## 3. 权威依赖与复用边界

优先复用而不复制：

- `UI-001` evidence-first local static HTML；
- `TRADING-076` Strategy Evidence Dashboard；
- `KNOWLEDGE-001` carrier-independent knowledge/evidence contract；
- `PUBLISHING-001` deterministic multi-carrier renderer boundary；
- `PLATFORM-UX-001` read-only/no-recompute system understanding client；
- canonical task register、requirements、policies、reports、artifact catalog、DQ/PIT receipts
  与 Owner decision references。

当前 exact tree 未发现独立 JavaScript frontend 工程；不得假装存在既有 `frontend/`、`web/`
或 writable API。具体 implementation path 必须在 serial contract wave 的 repository
inspection 中冻结；MVP 默认采用 Python deterministic snapshot + local static HTML。

## 4. 信息架构

MVP 至少包含：

1. 研究主线首页：当前主线、当前节点、上一步、下一合法动作和安全边界；
2. Research Path Map：`hypothesis -> task -> experiment -> evidence -> decision`；
3. 任务/实验时间线：status、Owner token、commit、requested/evaluated window、执行/未执行；
4. Evidence Card：observed value、rule/threshold、source、DQ/PIT、sample、limitation；
5. Result Status Card：raw machine status、通俗解释、可推出与不可推出的结论；
6. Attribution Explanation：贡献、阻断、缺失 evidence、反证与解释；
7. Alternatives / Stop Reason：替代路线、否决原因、停止类型和重新开放条件；
8. Glossary：ESS、OOF、PIT、fold、purge、embargo、known_at、available_at 等；
9. Raw Evidence Trace：exact commit/blob/content SHA、task/policy/report/artifact/receipt identity；
10. Window & DQ/PIT Panel：primary/requested/evaluated range、cutoff、freshness 与 limitations。

所有内容必须使用以下互斥来源类型：

```text
DATA_FACT
RULE_JUDGMENT
MODEL_RESULT
RESEARCHER_INTERPRETATION
OWNER_DECISION
```

聊天或 LLM 摘要只能是 `UNVERIFIED_CONTEXT`，不得成为上述五类 authority。

## 5. 分步计划

### S0：任务登记与 Owner 决定

- 登记本任务、supporting requirement 与 Owner token；
- 状态转为 `IN_PROGRESS`；
- 不创建产品代码或 shared contract。

### S1：最小 serial contract wave

- inventory existing canonical sources、status 与 provenance contracts；
- 冻结 versioned Atlas snapshot/read-model schema；
- 冻结 raw status 与 reader-facing explanation 的分离规则；
- 冻结 source reference、DQ/PIT、known_at/available_at、window 与 limitation 字段；
- 输出 concrete path mapping report；
- 运行 schema/round-trip/tamper/no-recompute focused validation；
- 形成两条 lane 可共同消费的 exact new main。

### S2：Atlas isolated lane

- 从 S1 exact local-main SHA 创建 isolated worktree；
- 实现 read-only source adapters、snapshot projector、independent validator 与 static renderer；
- 只使用 controlled fixtures 和已发布 canonical references；
- lane 不修改 coordinator-only files 或 shared/generated authority。

### S3：Coordinator integration

- 与 TRADING-2467 从同一 frozen base 进入一个 integration candidate；
- coordinator 统一更新 task/requirement、system flow、report/artifact registry、
  compatibility/deprecation authority、task shadow/index、module/test manifests；
- final tree 只刷新一次 shared/generated state；
- 运行 combined focused、Architecture、Contract、Integration、Reproducibility 与 Full。

### S4：Owner closeout

- MVP 可转 `BASELINE_DONE`；
- interactive API、cross-snapshot diff、带引用问答等后续能力另行评审；
- LLM 只能检索/表达，不能成为计算或 authority plane。

## 6. Module / contract / resource / evidence claims

计划 module claims：

```text
AtlasSnapshotProjector
AtlasSnapshotValidator
AtlasStaticRenderer
AtlasSourceAdapters
```

serial wave 计划 contract claims：

```text
strategy_research_explorer_snapshot.v1
strategy_research_explorer_source_ref.v1
strategy_research_path_node.v1
strategy_research_path_edge.v1
strategy_research_result_card.v1
strategy_research_attribution.v1
```

serial wave concrete paths：

- `src/ai_trading_system/contracts/strategy_research_explorer.py`；
- `src/ai_trading_system/contracts/__init__.py`；
- `tests/test_strategy_research_explorer_contract.py`。

resource claims：

- read-only Git-authoritative task/requirement/policy；
- read-only published canonical report/artifact/DQ/PIT references；
- no raw market/macro cache by default；
- no secrets、cookies、browser history 或 private runtime path；
- no broker、scheduler、data refresh、research runner 或 write API。

每个 investment-facing claim 至少绑定：

```text
exact_commit
source_path
git_blob_sha1_or_content_sha256
artifact_report_task_policy_identity
as_of
requested_range
evaluated_range
DQ_PIT_freshness
known_at_available_at_if_applicable
limitation
owner_decision_ref_if_applicable
```

## 7. Coordinator-only boundary

Atlas lane 不得修改：

- `docs/task_register.md`；
- 本 supporting requirement；
- `docs/system_flow.md`；
- `docs/artifact_catalog.md`；
- `config/report_registry.yaml`；
- `inputs/architecture/**`；
- `registry/development_tasks_shadow/**`；
- compatibility/deprecation authority；
- module/test manifests 与 formal validation artifacts。

这些文件仅在 serial wave 或 final integration candidate 由 coordinator 更新。

## 8. Temporary workspace lifecycle

计划 worktree：

```text
D:/Work/AITradingSystem_t2466_atlas
```

- owner task：本任务；
- purpose：S1 完成后实现 Atlas leaf modules/tests；
- creation condition：S1 exact main、DUAL_LANE preflight 与 disjoint claims PASS；
- exit condition：lane commit 已进入 validated final candidate、canonical evidence 完整、
  无 active process、无 unique tracked/untracked/ignored content；
- cleanup：按 exact absolute path 审计后用 `git worktree remove` 删除；实现可由 Git history
  恢复。

计划 coordinator integration worktree：

```text
D:/Work/AITradingSystem_t2466_t2467_integration
```

只在两个 lane commit 可集成后创建；退出条件与清理规则同上。

2026-07-30 closeout 审计记录：

- `D:/Work/AITradingSystem_t2466_atlas` 的 tracked audit 为 PASS，lane 的两项 patch
  经 `git cherry main` 均显示已由 final candidate 等价吸收；删除白名单仅含该 exact
  absolute path，`git worktree remove` 已释放 4,045,129 content bytes。其 3 份 ignored
  preview（33,435 bytes）已由下述 canonical final preview 取代，不保留为独立证据；
- `D:/Work/AITradingSystem_t2466_t2467_integration` 的删除白名单仅含该 exact absolute
  path，待本 closeout 记录进入 `main` 后执行 `git worktree remove`，预计释放
  36,435,776 content bytes。清理前 32 份 ignored files（30,765,564 bytes）已逐文件
  SHA-256 验证存在相同 canonical 副本；9 份 daily-ops failure diagnostics
  （4,950 bytes）和 3 份 generated preview/report files（1,653,568 bytes）是 formal
  tests 的可复现临时输出，已由 final PASS artifacts、tracked task shadow/registry 与
  canonical reports 取代，不作为独立证据保留；
- canonical retained evidence 为
  `outputs/atlas/strategy_research_explorer/trading_2466_mvp/` 及
  `outputs/validation_runtime/{architecture-fitness_20260730T093615Z,architecture-fitness_20260730T094237Z,contract-validation_20260730T094432Z,integration_20260730T094635Z,reproducibility_20260730T094728Z,full_20260730T094756Z,full_20260730T100813Z}/`；
  实现由 `main` commit `3062442c71e1a3285c887c17e7c8c9f9be1efc89`
  及其历史恢复，无 active process 依赖上述 worktrees。

## 9. Acceptance criteria

1. snapshot 与所有 object 均有 `schema_version`；
2. 同一 source fact 在 reader mode 中只能改变顺序与解释层级，不能改变事实；
3. 五类 information source 不可混淆；
4. 100% investment-facing claims 可回溯 exact source identity；
5. missing provenance 显式显示 `LIMITED` 或 `BLOCKED`；
6. raw status 不被 display status 覆盖；
7. 双构建 byte-identical，source drift/tamper fail closed；
8. UI 不重算 score/gate/model/backtest/weight/promotion；
9. 无 write API、无 upstream command dispatch；
10. validation PASS 不显示为 strategy PASS 或 production ready；
11. 键盘、文字替代、非仅颜色状态与可见错误提示通过；
12. bounded performance fixture 与 snapshot regression 通过；
13. report/contract/architecture/formal validation 通过；
14. `production_effect=none`、`broker_action=none`。

## 10. Stop conditions

任一 shared schema、status semantics、DQ/PIT、known_at/available_at、publication identity 或
coordinator-only overlap 出现时，两条 lane 暂停并回到最小 serial contract wave。Atlas 不得
以 UI summary 绕过策略、数据或 Owner gate。

## 11. 进度记录

- 2026-07-30：Owner 采纳 Web Pro 建议并要求继续推进；任务登记为 `IN_PROGRESS`。
- 2026-07-30：当前只授权 task/requirement、serial read-model contract、deterministic
  read-only MVP 与 governed integration；不授权投资计算、production 或 broker action。
- 2026-07-30：S1 已实现 versioned shared read-model contract；冻结五种 assertion kind、
  graph/result/attribution/source-ref、raw/display status 分离、DQ/context readiness、
  `LEGACY_HISTORY_PARTIAL` 降级、closed references、deterministic snapshot identity 与
  read-only fail-closed 边界。focused validation 为 `17 passed`（与 TRADING-2465 历史交接
  validator/tests 合并运行）；等待串行候选进入 exact `main` 后启动 Atlas isolated lane。
- 2026-07-30：serial candidate 已以
  `adfd3d5817a9797c35f97d01b92ced2e01663373` 进入并发布到 `main`；Atlas lane 从该
  frozen base 实现 `source_registry -> source projection -> snapshot -> validation ->
  static HTML/JSON`，lane focused=`23 passed`、Ruff PASS、governed audit clean。
- 2026-07-30：与 TRADING-2467 的 coordinator candidate 按“strategy-evidence -> Atlas”
  顺序集成，combined focused=`63 passed`。MVP 状态转 `BASELINE_DONE`；页面不含脚本、表单、
  write API、外部资源或 investment-facing result，validation PASS 明示不等于 strategy PASS。
  interactive API、跨 snapshot diff、带引用问答和更多 canonical source adapters 仍需独立任务
  与 Owner review。
- 2026-07-30：最终静态预览绑定 source commit
  `5fd71702221f26c30d7f2a747e813f2f7b60da8a`，写入 ignored canonical path
  `outputs/atlas/strategy_research_explorer/trading_2466_mvp/`：`index.html`
  SHA-256=`554b1803d9278fe1fa38b0b6711fecb2cdb837dc04ce43b28552e83e48971062`，
  `snapshot.json` SHA-256=`1be1f13426fcc3d7f58fd8b7fb19b43fca4f355c20976139100727727e60b6cd`，
  `validation.json` SHA-256=`0c2c1499eb4b4376291f8dcc72fc110ff821dc904daacbf1e8d92065f0903050`。
  Browser 对 `file://`、localhost service 启动及 `data:` preview 均按 URL safety policy
  fail closed，且明确禁止替代浏览器或间接绕过；因此像素级浏览器验收未声称完成。HTML
  structure、escaping、no-script/no-form、ARIA 与 deterministic output 由自动化测试覆盖。
- 2026-07-30：isolated candidate 为 task-shadow generate/validate 临时复制 portable bundle
  已声明的 4 份历史 validation summaries，并逐份校验 tracked bundle SHA；这些 ignored
  inputs 只服务 clean-worktree validation，未作为新事实或投资证据，将随 integration
  worktree 清理。
