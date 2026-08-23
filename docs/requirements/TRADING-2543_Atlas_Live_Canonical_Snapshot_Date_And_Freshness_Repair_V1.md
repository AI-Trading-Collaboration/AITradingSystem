# TRADING-2543：Atlas Live Canonical Snapshot、日期与 Freshness 长期修复 V1

最后更新：2026-08-23

- stable task id：`TRADING-2543_ATLAS_LIVE_CANONICAL_SNAPSHOT_DATE_AND_FRESHNESS_REPAIR_V1`
- priority：`P0`
- status：`DONE`
- governed mode：`SINGLE_LANE`（consumer-visible serial contract wave）
- exact base：`b70fe3963988241b187bc0d30bbc422eed2b2160`
- production effect：`none`
- broker action：`none`
- external action：`none`

## 1. 问题与 Owner 目标

Project Owner 要求推进 Atlas 策略研究网页长期修复。只读审计确认当前 canonical 页面是混合投影：

1. `config/atlas/source_registry.yaml` 的核心 Atlas snapshot 仍固定在
   `2026-08-02T00:00:00+09:00`；
2. canonical 页面使用与 `tests/atlas/test_cited_query_renderer.py::_payloads` 等价的 synthetic
   before/after/diff，只把旧 snapshot 加一天并追加“已增加引用式问答入口”；
3. `cited_query_renderer` 把 snapshot `generated_at` 同时投影为 evidence evaluation 和 page
   generation，导致 2026-08-23 重建的页面仍显示 08-02/08-03；
4. `page_effectiveness` 的 `relevant_source_paths` 未绑定 source registry、canonical snapshot 或
   diff，且 `source_snapshot_commit` 默认等于 current repository commit，因此外壳可显示
   `CURRENT`，而核心研究快照仍旧；
5. TRADING-2481--2542 已进入 task coverage/QQQ projection，但没有进入同一个 live canonical
   snapshot、query citation 和 change chain；当前开发中的 successor 也只能等待进入 exact main 后再分类。

这不是浏览器缓存或一次 HTML rebuild 问题。长期目标是建立唯一、可重放、fail-closed 的 live page
source bundle，使当前研究状态、历史证据、日期、task coverage、snapshot/diff identity 和页面
freshness 使用同一 exact commit 与 canonical source lineage。

## 2. 冻结边界

- Atlas 继续只读，不重算 score、threshold、DQ/PIT、backtest、strategy result 或 Owner decision；
- live projection 只能消费 canonical task registry、reviewed page task policy、tracked requirements、
  reviewed evidence inputs 与历史 source registry；
- 历史 08-02 snapshot 可保留为明确标记的 historical comparison，不得继续冒充当前研究状态；
- 页面必须区分 `research_state_as_of`、`evidence_evaluated_at` 与
  `page_source_commit_at`，缺失 evidence date 时显示 `UNKNOWN`，不得互相代填；
- page build 必须从 repository CLI/Python API 进入，不得调用测试 fixture 或 synthetic helper；
- `CURRENT` 必须同时证明 live snapshot、diff、task coverage、semantic source、rendered artifact 与
  exact commit identity 一致；任一未分类 successor 或 source drift 必须 fail closed；
- `primary_research_start=2021-02-22` 不变；不产生投资建议、订单、engine 或外部动作授权。

## 3. 分阶段实施

### S0：registration 与 serial contract freeze

- 登记 canonical task event 和本 requirement；
- 冻结 live page source bundle、三类日期语义、historical/live separation、freshness replay 和 CLI
  入口；
- 对 source registry、task coverage、snapshot/diff、rendered artifact 建立 exact identity contract。

### S1：live canonical snapshot builder

- 新增 typed live projection builder，从 canonical task registry/page policy 构建当前 mainline、最大
  blocker、最新 result/next action 与 closed source refs；
- 保留 historical snapshot 作为 explicit comparison base；
- 生成 `current_snapshot.json`、`comparison_snapshot.json` 与 `current_diff.json`，所有对象 exact-bind
  repository commit、requirement hash、task status/event 和 source identity；
- 禁止 fixture-only title/summary mutation 进入 canonical page。

### S2：日期与 freshness 修复

- reader state 分离 research state、evidence evaluation 和 page source commit time；
- page effectiveness policy 纳入 source registry、live snapshot policy/builder 和生成的 snapshot/diff
  identity；
- validator 独立重建 snapshot/diff/task coverage，并拒绝 synthetic fixture、旧 identity、漏分类 successor、
  date substitution、hash drift 和 exact commit mismatch；
- 页面 trust strip 明确显示当前研究状态截至时间和 exact source commit；历史 08-02 日期只在历史层出现。

### S3：canonical writer、页面与验证

- 增加正式 renderer CLI/Python API，唯一生成当前 canonical HTML 和 sidecars；
- canonical writer 输出完整 source bundle，页面不再依赖测试 helper；
- focused pytest 使用 `-n 16 --dist loadfile`；随后运行适用 Architecture、Contract、Integration、
  Reproducibility 与 Full；
- 更新 `docs/system_flow.md`、`docs/artifact_catalog.md`、architecture fragments/manifests、compatibility
  authority、task source 与 canonical page；
- final candidate 通过 governed integration、ordinary push 与 workspace cleanup 后关闭任务。

## 4. Path、module 与 evidence claims

Task/contract paths：

- `config/atlas/live_snapshot.yaml`；
- `config/atlas/page_effectiveness.yaml`；
- `config/atlas/reader_state_semantics.yaml`；
- `config/atlas/reader_projection_contract.yaml`；
- `src/ai_trading_system/atlas/live_snapshot.py`；
- `src/ai_trading_system/atlas/cited_query_renderer.py`；
- `src/ai_trading_system/atlas/page_effectiveness.py`；
- `src/ai_trading_system/atlas/reader_state_projection.py`；
- `src/ai_trading_system/contracts/strategy_research_reader_projection.py`；
- `src/ai_trading_system/contracts/strategy_research_page_effectiveness.py`；
- `src/ai_trading_system/contracts/strategy_research_reader_state.py`；
- package exports；
- `scripts/render_atlas_strategy_research_page.py`；
- `tests/atlas/test_live_snapshot.py`、相邻 Atlas focused tests。

Coordinator paths：

- 本 requirement、canonical task registry/index 和 generated task views；
- `docs/system_flow.md`、`docs/artifact_catalog.md`；
- architecture/module/test/deprecation manifests、compatibility authority；
- report/catalog/flow lossless shadow policy、generated fragments/index 与 authority test fixture；
- ignored canonical Atlas HTML/JSON/validation sidecars 和 formal runtime artifacts。

Evidence/resource claim：tracked Git sources 与 local deterministic validation only；不读取 market cache、
provider、Cloud、browser session、secret、paper/live、production 或 broker resource。

## 5. 验收标准

1. canonical writer 不导入、调用或复制测试 fixture/synthetic helper；
2. 页面 current snapshot 包含 page policy 中所有 `CORE_PROJECTED`/`DISCLOSED_*` task 的 exact status、
   requirement hash 与 source event identity，未分类 successor fail closed；
3. historical 08-02 snapshot 与 live snapshot 的角色、日期和 identity 分离；
4. `research_state_as_of` 来自最新 canonical relevant task event；
   `evidence_evaluated_at` 只来自 admitted evidence；`page_source_commit_at` 来自 exact commit metadata；
5. canonical page 的 current snapshot/diff ids 不得等于 synthetic fixture ids；
6. source registry、comparison/current snapshot、diff、task coverage、page sources 和 final HTML 均进入
   effectiveness replay；任一漂移不得显示 `CURRENT`；
7. 页面可见主线、blocker、不能推出什么和下一合法动作与最新 task coverage 一致；
8. 2542A 或后续 task 只在 exact main 出现并完成 Atlas classification 后进入 live snapshot，不读取未提交
   working-tree bytes；
9. focused/formal/final-tree validation PASS；
10. `investment_conclusion_generated=false`、`order_authorized=false`、`real_engine_authorized=false`、
    `production_effect=none`、`broker_action=none`。

## 6. Workspace 生命周期

- workspace：复用已审计且无 active process 的
  `D:\Work\AITradingSystem_trading2525_reader_state`；不创建新 worktree/clone；
- 原 branch `codex/trading-2525-reader-state` 保留，未删除；其 lane 已由 TRADING-2524 记录为吸收，
  本任务不改写该历史 branch；
- task branch：`codex/trading-2543-atlas-live-snapshot-freshness`；
- purpose：仅实施本 requirement 的 Atlas live snapshot/date/freshness/CLI 和验证；
- ignored residue：切换前仅存在 mypy/pytest/ruff/CPython cache，不作为研究或实现证据；
- exit condition：task commit 进入 validated local/remote main，canonical artifacts 已重建并校验，无 active
  process、无 unique tracked/untracked/ignored evidence；随后清理 task branch，并按 owning records 决定该
  reused worktree 是否保留或移除；
- recovery：合入前由 task branch 恢复，合入后由 local/remote main 与 canonical artifact hashes 恢复。

## 7. 进度记录

- 2026-08-23：Owner 要求推进长期修复。READ_ONLY preflight PASS；当前主 checkout 的 2542A 有独立
  未提交工作，因此选择复用已存在、worktree-audit PASS、无进程依赖的 2525 reader worktree，从 exact
  `main=b70fe3963988241b187bc0d30bbc422eed2b2160` 开始 serial contract wave。
- 2026-08-23：live snapshot policy/builder、三类日期语义、正式 page writer、四份 canonical sidecar、
  page-effectiveness v3 exact replay、Atlas task/source/event binding、system flow、artifact catalog 与
  architecture/report-flow authority 已完成。Atlas focused=`39 passed`；Architecture 首轮
  `863 passed / 2 failed`，两项均为新增 task/module 后的冻结计数与 deprecation inventory，按正式
  generator 修复后复验=`865 passed`；Contract=`276 passed`、Integration=`995 passed / 643 warnings`、
  Reproducibility=`24 passed`，Ruff、DevEx、report-flow authority 均 PASS。
- 2026-08-23：首次 Full artifact=
  `outputs/validation_runtime/full_20260823T084619Z/test_runtime_summary.json`，结果为
  `9362 passed / 12 failed / 6 skipped / 644 warnings`。12 项分为两条生成权威漂移与 9 项隔离
  worktree 缺失既有 TRADING-2464 DQ gate；该 gate 在主 checkout 与 2522 integration worktree
  两份来源逐字节同为 SHA-256=`ca02b4310f99d664bb8d987debd4900f4367935b3938663c7a633400d988a1ca`，
  且与 reviewed policy seal 一致，复制到本 task worktree 仅用于 Full fixture，task worktree 清理时删除。
  compatibility authority 由 canonical writer 重建后，对应 focused=`14 passed`。
- 2026-08-23：parent-bound Full failure-fix artifact=
  `outputs/validation_runtime/full_20260823T093657Z/test_runtime_summary.json`，结果为
  `9371 passed / 3 failed / 6 skipped / 644 warnings`。其中一项是 report-flow compatibility
  consumer 冻结计数应从 `2991` 同步到 canonical `2992`；另外两项是运行期间 local/remote main 已从
  frozen base `b70fe3963988241b187bc0d30bbc422eed2b2160` 前进到
  `675b8841890b9c943d9e57ab9e99509426e00fa2`，Wave14/15 carrier 按设计以
  `CARRIER_PUSH_DRIFT` 拒绝 stale-head Full。不得在 frozen lane 修改 carrier 测试；下一步是完成
  lane commit，生成 `integration_revalidation_plan.v1`，在 latest-main coordinator candidate 重建共享
  authority 并运行 final-tree Full。
- 2026-08-23：lane commit=`a12599d7ba1aa7abbbac488d64e20c6fc713b335`；base-drift plan=
  `integration-revalidation-3f3249bb7f37afcd7799 / RECONCILIATION_REQUIRED`。Coordinator 已逐项复核
  overlap，并由 INTEGRATION preflight 记录 reviewed plan id 后，从 latest main
  `675b8841890b9c943d9e57ab9e99509426e00fa2` 创建唯一 candidate。2542A 的 V2 measurement / owner / DQ
  blocked 语义与 2543 live projection 同时保留；task registry 在 final tree 重放原 2543 registration 并
  追加 `VALIDATING` 事件，task count=`1018`。共享 task views、report-flow、compatibility、DevEx 只在该
  final tree 重建；当前 mainline/blocker/next action 已切换为 2542A，仍不授权 DQ、经验研究、回测、
  投资结论或任何外部/交易动作。
- 2026-08-23：latest-main candidate 的 Atlas/authority/2542A 共存回归首轮为
  `137 passed / 1 failed`；唯一失败是 live builder 修正后的 DevEx 生成清单 freshness gate，随后使用
  official DevEx 与 compatibility authority builders 重建。2542A 新增的 `CANONICAL_DQ_PIT`、
  `false-risk-off` 和 `slice` 原始审计术语只在 live reader projection 中转换为稳定读者文本，task
  event、requirement 与 sidecar source identity 保持原字节及 exact hash。最终聚焦复验为
  `138 passed`；report-flow authority validate、DevEx fitness 与 compatibility authority build 均 PASS。
- 2026-08-23：candidate `7e1d5ca09c37088b4f6cb7402548a014828a0c05` 已 exact 重建 canonical page，
  `research_state_as_of=2026-08-23T19:14:00.242746+09:00`、`evidence_evaluated_at=null`、
  `page_source_commit_at=2026-08-23T19:37:49+09:00`，历史 08-02 只保留在 comparison。Final-tree
  Architecture=`865 passed`、Contract=`276 passed`、Integration=`995 passed / 642 warnings`、
  Reproducibility=`24 passed`。Parent-bound Full artifact=
  `outputs/validation_runtime/full_20260823T105854Z/test_runtime_summary.json`，结果为
  `9405 passed / 3 failed / 5 skipped / 644 warnings`：两项为 Full 期间 local/remote main 又前进到
  `1ca8ccf95c2a93a1b50164345d3e101a59b50838` 后的 `CARRIER_PUSH_DRIFT`，一项为 local canonical
  page test 尚未把 2543 加入 expected task list。不得放宽 carrier；后者直接修复，前者使用新 plan
  `integration-revalidation-aee92b71764822bc8b3c` 在 2542B latest-main 上再次协调重建。
- 2026-08-23：2542B latest-main candidate=`6fddf0ddba821ef309cc8b601917340d10ea69c5`；
  canonical page exact rebuild PASS，current mainline/blocker/next action 均为 2542B 的 Owner 与独立审阅阻塞，
  2543 只完成发布链修复。Final-tree Architecture=`865 passed`、Contract=`276 passed`、
  Integration=`995 passed / 642 warnings`、Reproducibility=`24 passed`、Full=
  `9444 passed / 5 skipped / 644 warnings`；最终 Full artifact=
  `outputs/validation_runtime/full_20260823T120227Z/test_runtime_summary.json`。Canonical task 已迁移为
  `DONE`；terminal status/page coverage 的 final exact-commit rebuild 与聚焦复验作为无行为扩张的治理收口，
  仍保持 `evidence_evaluated_at=null`、`investment_conclusion_generated=false`、production/broker effect=none。
