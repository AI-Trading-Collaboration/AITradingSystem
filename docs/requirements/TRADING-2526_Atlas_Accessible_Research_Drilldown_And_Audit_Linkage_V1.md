# TRADING-2526：Atlas 可访问研究展开与审计链接 V1

最后更新：2026-08-16

- stable task id：`TRADING-2526_ATLAS_ACCESSIBLE_RESEARCH_DRILLDOWN_AND_AUDIT_LINKAGE_V1`
- priority：`P1`
- status：`IN_PROGRESS`（2526-A strategy-evidence lane）
- proposed governed mode：2524 `DUAL_LANE` 的 strategy-evidence worker + coordinator final gate
- contract change：`true`（accessible reader/research interaction contract）
- predecessor gate：2523 已关闭且 2524-S0 `reader_projection_contract.v1` 已进入 local `main`
- production effect：`none`
- broker action：`none`

## 1. 问题与目标

现有递归解释和 `<details>` 能保留复杂证据，但多层 disclosure 会把复杂性转化为折叠森林。结构存在、
无 overflow 或 accessibility tree 可见，都不能证明键盘、screen reader 和移动端用户能完成核心任务。

本任务移除嵌套 disclosure，建立可访问的 research drilldown 与独立 audit destination，同时保持 raw
ID、hash、locator、receipt、manifest、sidecar 与 lineage 完整可达、可重放。

## 2. Accessible interaction contract

- 一个页面一个清晰 `h1`，标题层级连续；
- 提供 skip link 与明确的 `main`、`nav`、`footer` landmarks；
- 优先原生 `<details>/<summary>`；自定义 disclosure 必须支持 Enter、Space、`aria-expanded`、可见焦点
  与关闭后的上下文恢复；
- 不允许 nested details；一个 reader card 最多一个主要 disclosure；
- disclosure label 明确写出“查看数据范围与限制”“打开 exact lineage”等目标；
- DOM 顺序等于移动端视觉阅读顺序，320 CSS px 附近 reflow 不丢任务语义；
- 状态不能只靠颜色，图表有等价文字摘要与数据表，tooltip 不是唯一信息来源；
- 表格具有 caption、header association、scope、单位和时间范围；
- audit destination 保持 exact identity 和返回原上下文的路径。

## 3. Owner 决策、允许动作与禁止动作

Owner 必须冻结 accessibility conformance target、browser/assistive-technology matrix、手工验收责任人，
以及 audit 使用同页 appendix 还是独立 artifact page。

允许：semantic headings/landmarks、skip links、focus management、target size、details 重构、chart/table
alternatives、mobile task order、audit linkage 与可回滚样式。

禁止：删除 raw lineage、隐藏关键风险、无语义自定义控件、视觉顺序与 DOM 顺序不一致、用 hover-only
解释、用自动化 accessibility PASS 代签人类理解。

## 4. 两阶段执行与 path claims

### 2526-A：与 2525 并行的 evidence lane

S0 后从与 2525 相同的 exact local-main base 启动，task-owned paths 预先冻结为：

- `src/ai_trading_system/atlas/reader_accessibility_validation.py`；
- `tests/atlas/test_reader_accessibility_validation.py`；
- 本任务独占的 keyboard/reflow/focus/audit-reachability fixtures 与可重放 evidence schema（最终 exact
  path 在 START preflight 中列明）。

2526-A 负责 validator、test harness、negative cases、browser/AT runbook 与 typed violation evidence；不
直接修改 shared renderer、DOM/CSS、page config、package root/shared exports、canonical HTML/manifest/
sidecars、task registry、`docs/system_flow.md`、catalog 或 formal validation artifacts。需要 shared remediation
时，向 coordinator 提交 exact locator、失败类型、期望 contract behavior 与复现证据，由 coordinator 在
2524-I0 单次实现。

2527-A protocol preparation 可以与 2526-A 位于同一 strategy-evidence worktree，但两者仍使用不同
task-owned paths 和 evidence lineage；不得把 accessibility automation 当作 human comprehension PASS。

### 2526-B：最终候选串行门

coordinator 完成 2525/2526-A/2527-A 吸收、shared renderer wiring 与唯一 exact HTML 后，再串行运行
desktop/mobile、keyboard、screen reader、text zoom、reduced motion、320px reflow、focus restoration、
audit identity replay 与 provenance reachability。任何 remediation 改变 HTML bytes，都必须重新生成
manifest/sidecars 并重跑 2526-B；通过前不得启动 2527-B。

## 5. 制品与验证

预期制品：accessibility contract、exact HTML、L0/L1/audit linkage map、automated evidence、manual
keyboard/screen-reader logs、desktop/mobile screenshots 与 rollback plan。

验证至少覆盖：automated accessibility、keyboard-only、screen reader、text zoom、reduced motion、
320px reflow、desktop/mobile visual regression、no nested details、focus restoration、chart/table alternatives、
audit identity replay 与 provenance reachability。

## 6. Exit、falsification 与 downstream gate

Exit criteria：核心任务可由键盘和辅助技术完成；无 nested details；移动端 DOM/视觉顺序一致；关键风险
默认可见；audit evidence 可达且 exact identity 未漂移。

STOP CONDITION：任何“简化”导致 provenance 不可达，或移动端视觉顺序与 DOM 顺序分离，立即停止；
不得以删除证据、复制事实或降低 auditability 继续。

2526-A 只依赖 2524-S0，可与 2525 和 2527-A 并行；2526-B 必须等待 2524 coordinator integration 的
唯一 exact candidate HTML。只有 2526-B 对该 exact identity 通过，才允许进入 2527-B 人类验收。

## 7. 进度记录

- 2026-08-15：根据 Project Owner 要求登记为后续计划；状态为 `PROPOSED`，未改动页面或签署任何
  accessibility、visual、reader-comprehension acceptance。
- 2026-08-15：Project Owner 确认两阶段拓扑：2526-A 在 strategy-evidence lane 并行开发 validator/
  harness/evidence，shared renderer remediation 归 2524 coordinator；2526-B 对唯一 final HTML 串行执行
  browser/AT/mobile 验收。状态仍为 `PROPOSED`，尚未创建 lane/worktree。
- 2026-08-16：2524-S0 exact main=`ece8d97373c1a8a70949aa0ae445b79593ee09b3`，DUAL_LANE START
  claims `PASS`。2526-A 与 2527-A 共用的临时 Git worktree 计划为
  `D:\Work\AITradingSystem_trading2526_2527_evidence`，owner=`TRADING-2526/TRADING-2527-A`，
  purpose=仅实现两项 requirement 已冻结且互不重叠的 evidence/protocol paths，exit condition=两项 lane
  focused/impact PASS、commits 被 2524 coordinator reviewed absorption、unique tracked/untracked/ignored
  audit 完成且无活跃进程依赖。不得在该 worktree 修改 renderer、生成 final HTML 或签署 accessibility。
