# TRADING-2546：Atlas QQQ Options 结果准入状态投影修复 V1

最后更新：2026-08-30

- stable task id：`TRADING-2546_ATLAS_QQQ_OPTIONS_RESULT_ADMISSION_STATUS_PROJECTION_FIX_V1`
- priority：`P0`
- status：`DONE`
- owner：Codex Atlas coordinator
- governed mode：`SINGLE_LANE`
- exact base：`705ba295690e86e3de566629795da15fd4287cfc`
- production effect：`none`
- broker action：`none`
- external action：`none`

## 1. 问题与目标

Project Owner 已追认单次 QuantConnect backtest dispatch 内的两个平台自动 Cloud Build，
并接纳 backtest `f2879a3cee7ec4e0b68b4f943aafd1f8` 的 export-safe aggregate，
仅限 non-executable `DATA_RESEARCH`。`TRADING-2542I` 已进入 `DONE`，canonical task
projection 明确记录 `authorization_state=RETROSPECTIVELY_REVIEWED`、
`technical_validation_state=PASS_EXPORT_SAFE_AGGREGATE_ONLY`，并禁止任何新的
save/build/backtest/retry 或其他外部、生产、broker 动作。

Atlas tracked policy 仍保留更早状态：

1. `TRADING-2542H` 仍被描述为当前 baseline 且等待 signal/policy freeze；
2. `TRADING-2542I` coverage 仍为 `QC_AUTHORIZED_NOT_RUN`；
3. live snapshot 仍写 `AUTHORIZED_LATER_WAVE_NOT_RUN`，并把下一合法动作写成一次 QC run；
4. 对应 focused tests 把这些陈旧描述固化为当前预期。

本任务只修复 Atlas 当前状态投影和生成页，使页面与 canonical task authority 一致。它不重写
`TRADING-2542H/2542I` 历史事件、不改变已接纳 aggregate、不生成新的策略结论，也不授权任何
QuantConnect、provider、raw option、Object Store、public share、paper/live/production/broker 动作。

## 2. 权威状态与展示语义

- `TRADING-2542H` 作为 immutable scope-correction predecessor 保留，但不再冒充当前 blocker；
- `TRADING-2542I` 是当前 QQQ options baseline wave 的 terminal authority，状态为 `DONE`；
- exact research window 为 `2021-02-22..2025-12-02`、1202 sessions；
- backtest aggregate 仅显示已准入字段：end equity=`104479.60`、net profit=`4.48%`、
  fees=`75.40`、orders/entries/exits/cancels=`116/58/58/0`；
- Sharpe=`-1.872`、PSR=`0`，因此正收益不得解释为策略有效、稳健或可投资；
- authorization/technical states 分别为 `RETROSPECTIVELY_REVIEWED` /
  `PASS_EXPORT_SAFE_AGGREGATE_ONLY`；
- 下一合法研究步骤仅能是另行登记、结果边界明确的 non-executable `DATA_RESEARCH`
  comparison/design task；当前不得执行新的 QC action。

## 3. 实施步骤

### S0：任务登记与陈旧状态定位

- 登记 canonical task 与本 requirement；
- 绑定 `TRADING-2542I` terminal task event 和 immutable incident/result artifact；
- 记录旧 `QC_AUTHORIZED_NOT_RUN` / `AUTHORIZED_LATER_WAVE_NOT_RUN` 文案为 current-state drift。

### S1：Atlas policy 与 live snapshot 修复

- 将 2542H coverage 降为 historical predecessor；
- 将 2542I coverage 更新为 result-admitted research-only terminal；
- live snapshot 显示 baseline completed、aggregate limitations 和 no-new-QC boundary；
- 把本任务纳入 page task coverage，保持 `primary_research_start=2021-02-22`；
- 不把未来趋势到期权策略设计写成已批准 task 或已授权 backtest。

### S2：生成与验证

- 更新 focused tests，拒绝 current 页面再次出现 QC-not-run 或 next-action=run-backtest；
- 更新 `docs/system_flow.md`，明确 result admission 到 Atlas current-state projection 的只读边；
- 按官方 generator 顺序刷新 task、architecture、Atlas、report-flow 与 compatibility authority；
- 在 final exact commit 上生成 ignored canonical Atlas HTML/sidecars；
- 运行 focused pytest-xdist 与适用 Architecture、Contract、Integration、Reproducibility、Full。

### S3：收口

- terminal task update 与 generated authority 使用同一 publication transaction；
- final tree 通过后 fast-forward local `main`，执行普通 non-force push 并验证 SHA 相等；
- audit/cleanup 后释放 publication fence；不访问 QuantConnect。

## 4. 验收标准

1. Atlas 当前页面不再声称 QuantConnect 尚未运行或下一步应运行 backtest；
2. 2542H 明确限定为 historical predecessor，2542I terminal result admission 支配当前 baseline；
3. 页面准确显示 backtest id、研究窗、准入 aggregate、Sharpe/PSR 限制和两类状态；
4. 页面明确写明：任何 follow-on comparison 必须另行预注册，当前无新 QC authority；
5. raw option rows、contract identifiers 和未准入 comparator 值不得被页面补写或推断；
6. focused negative tests 能拒绝 stale coverage、stale live summary、错误 next action 和策略推广；
7. canonical Atlas page/sidecars 在 final exact commit 上为 `PASS/CURRENT`；
8. `investment_conclusion_generated=false`、`order_authorized=false`、
   `real_engine_authorized=false`、`production_effect=none`、`broker_action=none` 保持不变。

## 5. Path 与 ownership

Task-owned：

- 本 supporting requirement；
- `src/ai_trading_system/atlas/live_snapshot.py`；
- `src/ai_trading_system/atlas/cited_query_renderer.py`；
- `config/atlas/page_effectiveness.yaml`；
- `config/atlas/live_snapshot.yaml`；
- Atlas focused tests。

Coordinator-owned：

- canonical task fragment/index/views；
- `docs/system_flow.md`；
- architecture、Atlas、report-flow、compatibility generated authority；
- ignored canonical Atlas HTML/JSON/validation sidecars 与 formal runtime artifacts。

known-unrelated exclusion `docs/research/growth_tilt_owner_diagnosis_pack.md` 不读取、不 hash、
不 diff、不 stage、不修改。本任务复用主 checkout，不创建额外 worktree/clone/cache。

## 6. 后继讨论边界

本任务完成后，Project Owner 与 Codex 再单独讨论“如何把既有趋势信号转换为期权实现并进行回测”。
该讨论至少区分 signal、option implementation、paired comparator、risk/capital budget 与 result admission，
但本任务不冻结新策略参数、不创建 executable manifest，也不触发新回测。

## 7. 进度记录

- 2026-08-30：Owner 指示先修复 Atlas 陈旧状态，后续再讨论趋势到期权策略和回测设计。
  READ_ONLY preflight=`PASS`，local/origin main=
  `705ba295690e86e3de566629795da15fd4287cfc`，active lease=0，worktree audit=`PASS`。
  诊断确认 tracked Atlas policy/live snapshot 仍保留 pre-backtest 文案；本任务仅修复本地报告状态，
  QuantConnect/provider/backtest/orders/fills/positions/production/broker action 均为 0/none。
- 2026-08-30：publication transaction v1 在 task registration 后按 `FAILED` 释放；进一步只读定位确认
  陈旧语义还存在于 `atlas_reader_decision_projection.v1` 首屏卡片和 renderer system orientation，超出
  v1 最初声明的 config-only scope。v1 未形成 candidate、未运行 generator/formal validation、未修改
  QuantConnect。v2 显式声明 live projection、renderer 与相邻 tests 后继续，禁止用局部文案替代完整修复。
- 2026-08-30：v2 已实现 result-authority replay、2542H→2542I successor dominance、live summary、
  首屏决定卡和 system orientation 单一投影。首轮 focused 并行测试暴露 raw uppercase audit identifiers
  进入 reader surface，被 terminology gate 正确拒绝；修复采用普通中文读者文案，exact technical states
  只留在 canonical audit authority。相邻 focused 复核为 55 passed / 2 expected pre-regeneration failures：
  一项为旧 task coverage count 断言，已更新为 76；另一项为 ignored canonical page 尚未从 final candidate
  重建，不代表实现失败。governed `INTEGRATION` preflight 已在 task active 时以 transaction v2=`PASS`。
  后继按 transaction 顺序追加 terminal event、刷新全部 generated authority、形成 clean candidate 并运行
  正式验证；全过程无 QuantConnect 或其他外部研究动作。
- 2026-08-30：v2 在 `GENERATED_REBUILD_PRE` 后由 Atlas writer 以
  `ATLAS_PAGE_SOURCE_WORKTREE_DRIFT:config/atlas/live_snapshot.yaml` fail closed。原因是 writer 只接受
  exact committed page source，不能把当前 dirty implementation 归到 frozen base `705ba295...`。
  v2 未形成 candidate、未运行 formal validation、未修改 local main/remote，也未发生 QuantConnect 动作；
  后继先把已通过 focused/static 检查的 task-owned 与协调器变更形成 exact lane commit，再从该 SHA 重开
  transaction、恢复 active task、通过同一 transaction 的 `INTEGRATION` preflight 后重新 terminalize，
  最后按官方顺序生成页面与共享 authority。
- 2026-08-30：exact-source lane commit 前的并行 focused validation 为 `56 passed / 1 expected
  pre-regeneration failure`；唯一失败是 ignored canonical Atlas page/sidecar 尚未重建，全部 source、policy、
  reader projection、negative drift 与 task-source count 用例均通过。下一步只提交经验证的本任务来源，
  再从该 exact SHA 重建 canonical 页面；不会把 stale generated page 当成实现失败或用手工改页绕过 writer。
- 2026-08-30：来源已形成 exact lane commit
  `2f7ccc8513aafa06b9d7a50d8f609cdfaef8ef07`。transaction v3 尝试把已经 terminal 的 2546
  恢复为 active 时被 canonical task state machine 以 `DONE->IN_PROGRESS` 非法转换安全拒绝；没有 task
  event 或其他 tracked write。2546 的完成历史保持不可变，剩余 exact-source 页面重建、formal validation
  与 publication closeout 改由一个窄范围 successor task 承接，不篡改 2546 终态，也不新增外部研究动作。
