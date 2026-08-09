# TRADING-2505 Atlas 页面有效性、Freshness 与视觉回归 V1

## 任务身份

- task id：`TRADING-2505_ATLAS_PAGE_EFFECTIVENESS_FRESHNESS_VISUAL_REGRESSION_V1`
- priority：`P1`
- status：`BASELINE_DONE`（工程有效性基线完成；两项独立人工验收仍为 `PENDING_REVIEW`）
- governed mode：`SINGLE_LANE`
- contract change：`true`（新增 consumer-visible 页面有效性合同与验收状态，不改变投资策略、DQ/PIT 或交易合同）
- exact registration base：`78487d609469342a2c41f2885ad93b7187666aa1`
- next owner：Atlas 页面 engineering owner；视觉与读者理解结论仍由 project owner 独立验收

## 背景与问题

当前 Atlas 策略研究页面已经能展示研究主线、流程节点、进展状态与结果解释，但“页面能打开”并不等于“页面仍然有效”。仓库中的策略合同、任务状态和研究证据持续推进后，旧页面可能继续渲染成功，却已经遗漏新 authority、沿用旧状态，或把工程能力误读成研究结论。纯 DOM 测试也不能证明窄屏没有遮挡、展开交互可用，或非金融读者能回答页面最重要的问题。

本任务建立四层独立有效性边界：

1. `SOURCE_TRUTH`：页面输入可追溯到 exact repository commit、canonical source 与 checksum；
2. `SEMANTIC_PROJECTION`：页面对 task、模块、阻塞、证据和结论边界的投影完整且未漂移；
3. `VISUAL_RENDERING`：真实浏览器在 desktop/tablet/mobile 下无关键遮挡、截断或失效交互；
4. `READER_COMPREHENSION`：读者能从首屏和展开说明中回答主线、阻塞、证据与下一步问题。

四层必须分别记录，任何一层 PASS 都不得替代其他层，也不得自动升级策略结论。

## 冻结设计

### 1. Freshness 合同

页面 effectiveness manifest/receipt 必须绑定：

- `repository_commit` 与生成页面时的 source snapshot commit；
- 页面输入 bundle 中每个 canonical artifact 的 repository-relative locator、schema、SHA-256 与 byte count；
- TRADING-2481–2493 核心策略链覆盖；
- TRADING-2494–2504 reviewed successor 的覆盖、显式排除或“不影响本页面”的分类与依据；
- 所有影响展示语义的 policy/module/renderer/config 的 LF-normalized SHA-256；
- 最终 HTML 的 repository-relative locator、SHA-256 与 byte count（通过 sidecar 绑定，HTML 不自包含自己的 hash）。

freshness 只允许以下状态：

- `CURRENT`：source snapshot 与当前仓库一致，相关 successor 已全部分类且所有 hash 重放一致；
- `REPOSITORY_AHEAD_NO_RELEVANT_DRIFT`：仓库领先，但 reviewed delta 证明没有相关 source/semantic drift；
- `STALE_REBUILD_REQUIRED`：任一相关 source、successor coverage、页面输入或 HTML identity 漂移；
- `UNCLASSIFIED_SUCCESSOR_REVIEW_REQUIRED`：存在尚未完成影响分类的 successor。

未知、缺失、重复、absolute locator、`..` path escape、非 canonical bytes、hash/schema/commit mismatch 必须 fail closed，不能降级为 warning 后继续宣称 CURRENT。

### 2. 三重验收状态

页面及 sidecar 必须并列展示并保存三个独立状态：

- `ENGINEERING_VALIDATION`：自动化 source/semantic/browser 验证；
- `OWNER_VISUAL_REVIEW`：project owner 对视觉层级、信息密度、响应式效果的人工验收；
- `READER_COMPREHENSION_REVIEW`：目标读者是否能正确回答关键问题的人工验收。

每个状态只允许 `PASS`、`FAIL`、`PENDING_REVIEW`、`NOT_EXECUTED`。自动化只可写入 engineering 状态，严禁代签后两项；本任务交付时后两项默认保持 `PENDING_REVIEW`，直到收到明确的真实验收事实。

### 3. Reader-first 问题合同

页面首屏与流程节点展开说明必须让非金融读者能够回答：

1. 当前研究主线是什么；
2. 当前最大的阻塞是什么；
3. 哪些只是工程能力，哪些已经有实际研究证据；
4. 现有证据不能推出什么；
5. 下一责任方与下一合法动作是什么；
6. 当前是否允许形成投资结论、下单或启动真实 engine。

节点解释采用“现在发生了什么 → 为什么 → 对读者意味着什么 → 证据边界 → 下一步”的顺序。内部字段名、hash 与 canonical locator 放入可展开证据区，不再作为面向读者的第一层解释。

V1 不引入阅读时长、点击率、得分阈值或任何会影响验收结论的未评审数值 heuristic。人工验收只记录事实、问题与决策，不伪造量化通过线。

## 实现范围

### Task-owned

- `config/atlas/page_effectiveness.yaml`
- `src/ai_trading_system/contracts/strategy_research_page_effectiveness.py`
- `src/ai_trading_system/atlas/page_effectiveness.py`
- `tests/atlas/test_page_effectiveness.py`
- `config/architecture/fragments/modules/atlas_page_effectiveness.yaml`
- `config/architecture/fragments/modules/strategy_research_page_effectiveness_contract.yaml`
- `config/architecture/fragments/flows/atlas_page_effectiveness_validation.yaml`
- 本 requirement

### Coordinator-owned

- canonical task registry fragment/index 与 generated `docs/task_register*.md`
- `docs/system_flow.md`、`docs/artifact_catalog.md`
- Atlas renderer/init 与对应测试
- architecture/module/test/deprecation manifests、compatibility authority 与 generated views

不修改 TRADING-2481–2504 的 shared QQQ contract/policy，不填写任何 Owner threshold，不执行 QuantConnect、cloud、API、CLI、HTTP 外部动作、raw export、paper/live/broker/production。

## 分步计划

1. 通过 ARCH-005 S5 canonical registry 登记任务并完成 START/LANE preflight；
2. 冻结 effectiveness policy、typed manifest/receipt、strict seal/replay 与 freshness 分类；
3. 将 2481–2504 coverage、reader-first explanation 与三重验收状态接入 renderer；
4. 生成 canonical 页面与 sidecar，先做 focused source/semantic 验证；
5. 通过 loopback HTTP 使用 Playwright CLI 在 desktop/tablet/mobile 验证首屏、响应式、展开、锚点、键盘焦点、accessibility snapshot、DOM snapshot 与 full-page screenshots；
6. 更新 system flow、artifact catalog 与 generated authority；
7. 在 final tree 串行运行 focused/compatibility/formal gates，完成 ordinary main push 与 cleanup。

浏览器自动化不得与 pytest formal/heavyweight gates 并发。

## 验收标准

- exact commit/source/bundle/HTML identity 可重放，相关 drift 与未知 successor fail closed；
- 2481–2493 核心链与 2494–2504 successor 全部有可审计覆盖或排除事实；
- engineering、owner visual、reader comprehension 三个状态独立保存且不会互相提升；
- 页面先给读者答案，内部 canonical 字段和证据 locator 延后到展开层；
- desktop/tablet/mobile 无关键水平溢出、重叠、截断或不可达控件，锚点与 details/keyboard 交互可用；
- accessibility/DOM/screenshot evidence 与页面/sidecar checksum 绑定；
- 页面明确保持 `not investment advice`、无真实下单/engine/promotion 结论；
- focused、architecture/contract/report/integration/reproducibility/full 及 governed closeout 通过，ordinary push 后 local main 与 origin/main SHA 相同。

## 开放问题与退出条件

- Owner visual 与 reader comprehension 只有真实人工验收后才能从 `PENDING_REVIEW` 迁移；
- 若 successor 影响无法分类，页面必须显示 `UNCLASSIFIED_SUCCESSOR_REVIEW_REQUIRED`，不得继续宣称 CURRENT；
- 若浏览器证据只能由 `file://` 产生而无法通过 loopback HTTP 重放，则保持 engineering validation 未通过；
- 若任何改动需要重定义策略、DQ/PIT、research window 或投资阈值，停止本任务并另开最小 serial contract wave。

## Progress notes

- 2026-08-10：DEVX-006C/D 与 ARCH-005 S5 已 ordinary push/cleanup；exact clean base=`78487d609469342a2c41f2885ad93b7187666aa1`，runner=0。2505 仅开始 canonical registration boundary，未复用旧冻结树 formal evidence。
- 2026-08-10：typed manifest/policy/renderer/sidecars 与 reader-first 页面完成；Atlas focused=`17 PASS`，desktop/tablet/mobile preliminary loopback Playwright 均为 `horizontalOverflow=false`、`clipped=[]`，details keyboard、anchor 与 console 检查通过。
- 2026-08-10：DEVX-006D authority、DEVX-006C compatibility fragments、DevEx 与 deprecation inventory 已按当前字节重建；compatibility/deprecation/C/D/S5 同覆盖首次 `242 PASS / 5 FAIL`，五项均为新增任务导致的旧 current-authority 计数/hash，最小修正后原样重跑 `247 PASS`。
- 2026-08-10：任务按边界收口为 `BASELINE_DONE`，表示页面有效性工程能力可进入 final-tree gates；`OWNER_VISUAL_REVIEW` 与 `READER_COMPREHENSION_REVIEW` 仍保持 `PENDING_REVIEW`，不得据此宣称策略、收益、风险或交易结论通过。
- 2026-08-10：首次 final-tree Full=`8708 PASS / 1 FAIL / 3 skipped`，唯一失败为历史 2503 local-canonical test 对 ignored 页面硬编码旧 HTML/sidecar identity；2505 新页面已合法取代这些 bytes。failure-fix 保留 2503 无 effectiveness sidecar 时的 immutable legacy exact 检查；检测到 2505 sidecar 时，改为 strict manifest replay、当前 source/hash/freshness 验证、2481–2504 coverage 与页面 identity 绑定。父证据=`outputs/validation_runtime/full_20260809T190700Z/test_runtime_summary.json`；修复后必须从 final bytes 重跑完整五级，Full 使用 `failure_fix_rerun`，不得以 focused PASS 代替。
