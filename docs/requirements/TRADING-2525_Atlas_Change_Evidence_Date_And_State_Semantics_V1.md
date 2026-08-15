# TRADING-2525：Atlas 变化、证据日期与状态语义 V1

最后更新：2026-08-16

- stable task id：`TRADING-2525_ATLAS_CHANGE_EVIDENCE_DATE_AND_STATE_SEMANTICS_V1`
- priority：`P1`
- status：`IN_PROGRESS`（engineering lane）
- proposed governed mode：2524 `DUAL_LANE` 的 engineering worker
- contract change：`true`（reader-facing state/date semantics）
- predecessor gate：2523 已关闭且 2524-S0 `reader_projection_contract.v1` 已进入 local `main`
- production effect：`none`
- broker action：`none`

## 1. 问题与目标

页面已经保留 commit、as-of、known-at、available-at、freshness 与 acceptance 信息，但默认投影仍可能把
`PASS`、`VALIDATED` 或“证据完整”理解为策略有效。普通读者还需要直接知道数据截至何时、证据何时形成、
页面何时生成、相对哪个 snapshot 发生了什么变化、为什么停止以及什么事实会重新触发。

本任务冻结对象限定的 reader state/date/change contract，不改变 canonical raw enum，也不新增未经治理的
confidence、重要性或变化阈值。

## 2. Reader state contract

每个状态按以下结构投影：

`对象 + 状态 + 原因 + 数据/证据日期 + 下一合法动作 + 不能推出什么`

- `UNKNOWN`：authority 或数据不足；显示缺失项与可补充责任方；
- `LIMITED`：证据存在但范围、时间、DQ、样本或上下文有限；不得写成轻度 PASS；
- `BLOCKED`：gate、Owner 决策、权限或依赖禁止继续；显示解除条件；
- `NOT_EVALUATED`：相应评估未执行；不等于 FAIL，也不等于安全；
- `NOT_STRATEGY_VALID`：不能支持策略有效性结论，即使工程或页面通过；
- `PASS`：必须带通过对象，例如“页面工程校验通过”；禁止孤立显示“已验证”。

现有 `ANSWERED` 可以投影为“本回答引用闭包完整”，但不得据此声明研究证据完整。若需要新增或重定义
canonical enum，必须先拆出最小 serial contract wave。

## 3. 日期与变化合同

默认层分别标明：

- `data_as_of`：输入数据截至时间；
- `evidence_evaluated_at`：研究证据形成/评估时间；
- `page_generated_at`：页面投影时间；
- `source_commit` 与比较基线 snapshot identity。

比较视图至少支持 `CHANGED / UNCHANGED / UNKNOWN / NOT_COMPARABLE`，并明确“相对于哪个 snapshot 和
日期”。变化是否重要不得由本任务新设数值 cutoff。停止视图必须显示已完成、未完成、停止原因、重新触发
事实与下一责任方；不得使用虚构的线性进度百分比。

## 4. Owner 决策、允许动作与禁止动作

Owner 必须冻结 reader-facing lexicon、always-visible 日期、比较基线、时区/显示规则，以及是否需要新的
canonical enum。

允许：raw-to-reader display mapping、对象限定标签、日期字段投影、snapshot comparison、missing/null/
timezone fail-closed 处理。

禁止：静默改写 raw enum、把 `LIMITED` 提升为 PASS、把 page/engineering PASS 投影为 strategy PASS、
新增 confidence/importance cutoff、改变研究结果、DQ/PIT 或研究窗口。

## 5. Engineering lane 边界与 path claims

本任务不等待完整 2524 结束；只在 2524-S0 的 reviewed exact commit 进入 local `main` 后，与
2526/2527-A evidence lane 从同一 base 启动。task-owned paths 预先冻结为：

- `config/atlas/reader_state_semantics.yaml`；
- `src/ai_trading_system/contracts/strategy_research_reader_state.py`；
- `src/ai_trading_system/atlas/reader_state_projection.py`；
- `tests/atlas/test_reader_state_projection.py`；
- 本任务独占的 fixtures/negative cases（最终 exact path 在 START preflight 中列明）。

engineering worker 不得写 `reader_projection_contract.v1`、`cited_query_renderer.py`、page config、package
root/shared exports、2524/2526/2527 requirements、canonical HTML/manifest/sidecars、task registry、
`docs/system_flow.md`、catalog 或 formal validation artifacts。worker 交付 typed projection API、fixtures、
focused tests 与 lane evidence；shared wiring、DOM/CSS、generated outputs 和 registry 更新由 2524 coordinator
在 I0 完成。

若实现需要新增/重定义 canonical enum、改变 state/date public interface、引入数值 cutoff，立即停止并回到
2524 最小 serial contract/policy wave。不得为了保持并行而在 lane 内复制 reader projection contract。

## 6. 制品、验证与退出条件

预期制品：status mapping contract、date model、comparison component、positive/negative examples、source
binding、mapping tests 与 exact HTML evidence。

验证：raw-to-reader replay、missing/null/timezone、comparison-base identity、no-silent-upgrade、策略通过数
保持为零、desktop/mobile/assistive-technology 呈现，以及人工复述“状态对象、日期、下一步和禁止推断”。

Exit criteria：每个 reader-facing 状态都明确对象、原因、日期、下一动作与禁止推断；变化都有 exact
comparison identity；没有引入新投资解释阈值或研究语义漂移。

STOP CONDITION：任一 display mapping 改变 canonical research meaning，或需要未经治理的数值 cutoff，
立即停止并拆出 reviewed policy/contract wave。

本 lane 通过 focused/impact validation 后交给 2524 coordinator；不自行生成 final HTML、不自行更新任务
状态，也不单独运行 heavyweight Full。2526-A 可与本任务并行，不以本任务完成为前置；2526-B final
candidate 验收必须等待 coordinator 吸收本 lane 并生成唯一 exact HTML。

## 7. 进度记录

- 2026-08-15：根据 Project Owner 要求登记为后续计划；状态为 `PROPOSED`，未修改任何 raw state、
  日期事实、阈值或研究结论。
- 2026-08-15：Project Owner 确认并行拓扑；本任务定位为 2524 coordinator 下的 engineering lane，
  依赖从“完整 2524 完成”收窄为“2524-S0 exact contract commit 已进入 local `main`”。状态仍为
  `PROPOSED`，尚未创建 lane/worktree。
- 2026-08-16：2524-S0 已从 exact main `ece8d97373c1a8a70949aa0ae445b79593ee09b3` 发布，2524
  DUAL_LANE START claims `PASS`。本 lane 的临时 Git worktree 计划为
  `D:\Work\AITradingSystem_trading2525_reader_state`，owner=`TRADING-2525`，purpose=实现本 requirement
  第 5 节四个 task-owned paths，exit condition=lane focused/impact PASS、commit 被 2524 coordinator
  reviewed absorption、unique tracked/untracked/ignored audit 完成且无活跃进程依赖。不得在该 worktree
  保留 canonical final HTML 或 shared coordinator bytes。
