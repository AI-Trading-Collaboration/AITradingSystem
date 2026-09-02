# TRADING-2526：Atlas 可访问研究展开与审计链接 V1

最后更新：2026-09-02

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

### 3.1 长期受控 HTTPS preview 方案

Project Owner 于 2026-09-02 要求把本地 `file://` / loopback Browser policy 阻塞转为长期 TODO。本任务
承接该方案，不新建重复 task ID。目标是提供一个 browser-policy 允许、exact-byte 可验证且不扩大数据或
发布权限的 HTTPS review surface：

1. 由受治理的 preview writer 从一个 clean exact commit 生成 review bundle；bundle 只包含 allowlisted
   Atlas HTML、同源静态 assets 与验证 sidecars，不包含市场数据、raw payload、credentials、cookies、
   browser profile、repository 其他文件或目录索引；
2. preview manifest 必须绑定 task id、source commit、HTML SHA-256、每个 asset SHA-256、writer version、
   生成时间、TTL/expiry、authorization state、`production_effect=none` 与 `broker_action=none`；
3. review endpoint 使用 browser-policy 允许的 HTTPS origin，默认 private/authenticated、不可索引、不可
   public-share；任何真实 cloud/project write、外部托管、付费资源或公开链接在执行前单独按 R2 取得 exact
   Owner 授权。本 TODO 本身不授权部署；
4. endpoint 必须按 exact manifest 提供不可变 bytes，禁止运行时注入、第三方脚本、外部字体/analytics、
   跨 bundle 路径访问或从 HTTPS 页面回连 localhost/private network；identity mismatch、expired TTL、
   missing asset 或 unauthorized origin 均 fail closed；
5. browser evidence receipt 绑定 exact preview URL identity、HTML SHA、browser/version、OS、viewport matrix、
   desktop/mobile screenshots、keyboard/reflow/audit replay 与 accessibility sidecar；自动化 Browser、Owner
   visual、assistive-technology 和 reader-comprehension 继续作为独立 acceptance tracks；
6. preview 完成或 TTL 到期后必须撤销 endpoint 并生成 cleanup receipt，记录删除目标、retained canonical
   evidence、released size 与 recoverability。不得用临时 localhost、alternate browser、raw CDP、data URL
   或复制到未治理公开站点绕过安全策略。

实现前需另行冻结 hosting/provider、private access、TTL、cost ceiling、retention、browser/viewport/AT
matrix 与 cleanup authority；这些是工程和外部动作边界，不影响任何投资解释、DQ/PIT、research window、
策略 verdict 或生产资格。

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

长期 HTTPS preview 还必须产出 allowlist bundle manifest、authorization/TTL receipt、exact-byte browser
evidence receipt 与 cleanup receipt。验证需证明 manifest 中的 HTML/asset SHA-256 与 endpoint 实际响应
一致，expired/unauthorized/missing-asset 情形 fail closed，并对 endpoint 撤销与 retained canonical evidence
完成可重放核验。

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
- 2026-08-16：2526-A 与 2527-A lane commit `2f8f24031` 已由 coordinator 审阅吸收；确定性 validator
  已在真实渲染 HTML 上验证九段 DOM 顺序、标题层级、无嵌套 disclosure、术语首现描述目标、重复术语
  tab-stop 去重与 card budget，并由 artifact writer fail closed 生成绑定 exact HTML SHA-256 的
  `reader_accessibility_validation.json`。coordinator focused=`74 passed in 138.06s`。2526-B 的 final
  browser/mobile/assistive-technology 复核尚未执行，Owner visual 与 reader comprehension 仍为
  `PENDING_REVIEW`。
- 2026-08-16：候选 exact HTML SHA-256=`72ac2710f966015a586b9700765bb96ead6d76974813384de00214e003244c17`，
  automated accessibility sidecar=`PASS` 且 human tracks 均为 `PENDING_REVIEW`。2526-B 尝试接管用户已开
  `file://` 页面时被 in-app Browser URL 安全策略 fail closed，页面未被浏览器控制或重载；按 no-workaround
  纪律未改用 alternate browser、raw CDP 或临时 loopback 绕过。下一步需要一个由浏览器策略允许的审阅入口，
  再对同一 exact bytes 执行 desktop/mobile/keyboard/assistive-technology 与 audit destination replay。
- 2026-09-02：Project Owner 已对 TRADING-2550 exact HTML SHA-256=
  `bea2de7359efffcfb981a6067e58d297b821dc415be6f5078267ff5d9b36a609` 完成 desktop/mobile 人工视觉验收，
  结果记为 `OWNER_MANUAL_VISUAL_PASS`；agent Browser 自动化仍为 `NOT_EXECUTED_URL_POLICY`，且本次人工
  通过不代签 2526-B 的 keyboard、screen-reader、assistive-technology、audit replay 或 reader-
  comprehension tracks。Owner 同时要求把长期受控 HTTPS preview 记入 TODO，故本任务继续保持
  `IN_PROGRESS`；next owner=`Atlas/DevEx coordinator`，当前 blockers 为 hosting/provider、private access、
  TTL、cost ceiling、retention、browser/viewport/AT matrix 与任何 R2 外部写入的 exact authorization 尚未
  冻结。Exit condition 是同一 exact HTML SHA 通过受允许 HTTPS origin 完成 Browser/mobile/keyboard/AT/
  audit replay，并由 TTL cleanup receipt 证明 endpoint 已撤销且 canonical evidence 保留完整。
