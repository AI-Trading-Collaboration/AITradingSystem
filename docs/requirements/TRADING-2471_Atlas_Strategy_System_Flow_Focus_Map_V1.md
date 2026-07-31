# TRADING-2471：Atlas Strategy System Flow Focus Map V1

最后更新：2026-07-31

稳定任务 ID：
`TRADING-2471_ATLAS_STRATEGY_SYSTEM_FLOW_FOCUS_MAP_V1`

优先级：`P1`

状态：`BASELINE_DONE`

Owner 决定：

```text
owner_decision:TRADING-2471:2026-07-31:add_atlas_system_flow_focus_map_v1
owner_decision:TRADING-2471:2026-07-31:accept_flow_focus_progress_status_visual_v1
```

production effect：`none`

broker action：`none`

## 1. 决策与目标

TRADING-2470 已生成面向金融知识较少读者的 citation-first 结果页面，但页面主要回答
“研究主线、结果、归因、变化和来源是什么”，尚未把这些回答放回整个策略系统的流程位置。
本任务在同一静态页面增加：

1. 一张从数据输入到 Owner 决策边界的全系统流程图；
2. “当前页面位于哪里”的明确位置标记；
3. “当前实际研究关注哪些节点”的 exact-ID 路径；
4. 每个流程节点的显式进展状态与颜色标识；
5. 对未由本页面执行的 DQ、backtest、promotion、production 与 broker 行为保持明确边界。

V1 是展示层增强，不改变 Atlas snapshot/diff/query contract，不重算研究、评分、回测或
归因，不读取 market/cache/external data，不调用 LLM，不启动 HTTP 服务。

## 2. 冻结流程阶段

页面按以下稳定顺序展示八个阶段：

1. `DATA_INPUTS`：市场、宏观、基本面及人工治理输入；
2. `DATA_QUALITY_GATE`：schema、完整性、新鲜度、PIT 与 `aits validate-data`；
3. `RESEARCH_MAINLINE`：研究问题、策略主线与候选方法；
4. `BACKTEST_AND_EVALUATION`：primary window、OOS、stress 与状态评估；
5. `RESULT_ATTRIBUTION`：结果、驱动、限制与失败原因；
6. `ATLAS_SNAPSHOT_DIFF`：validated snapshot、cross-snapshot diff 与 lineage；
7. `CITATION_FIRST_QUERY`：本页五个固定问题、claim 与 citation closure；
8. `OWNER_DECISION_BOUNDARY`：人工复核、后续任务或明确停止；不代表自动 promotion。

阶段顺序是系统说明，不代表本页面执行了前置阶段，也不代表每个阶段均为 PASS。

### 2.1 节点进展状态

流程角色与进展状态是两套独立视觉语义：节点边框/底色表示“上游、研究关注、当前页面、
Owner 边界”，节点内状态胶囊表示该节点在当前 evidence view 的进展。V1 冻结以下
display-only 映射：

|阶段|状态码|中文标签|颜色语义|
|---|---|---|---|
|`DATA_INPUTS`|`NOT_EXECUTED_BY_PAGE`|本页未执行|中性灰|
|`DATA_QUALITY_GATE`|`NOT_EXECUTED_BY_PAGE`|本页未执行|中性灰|
|`RESEARCH_MAINLINE`|`IN_PROGRESS`|研究进行中|蓝|
|`BACKTEST_AND_EVALUATION`|`LIMITED`|证据有限|琥珀|
|`RESULT_ATTRIBUTION`|`LIMITED`|证据有限|琥珀|
|`ATLAS_SNAPSHOT_DIFF`|`VALIDATED`|已验证|绿|
|`CITATION_FIRST_QUERY`|`VALIDATED`|已验证|绿|
|`OWNER_DECISION_BOUNDARY`|`PENDING_OWNER_REVIEW`|待人工复核|紫|

这些状态只描述当前静态页面对既有 evidence 的读取/展示进展，不是 strategy PASS、投资评级、
promotion readiness、production readiness 或数据质量结论。颜色旁必须同时显示中文标签和稳定
状态码，不能仅凭颜色表达状态。

## 3. 当前位置与关注路径

### 3.1 当前页面位置

“你在这里”只能标记 `CITATION_FIRST_QUERY`。页面是已验证 Atlas artifact 的只读消费层，
不得把自身标成 research、backtest、promotion 或 production executor。

### 3.2 当前实际研究关注

V1 只从 canonical preview 已显式指定的 target IDs 构建关注路径：

- `program-strategy-research`；
- `result-restart-r2`；
- `attr-restart-oos-limits-expansion`；
- diff change
  `5f2258c0f6eaa792cdba48b3e5dbc3c786df7411228f6e47b8575d7d33fb8dec`；
- `restart-r0-r2-requirement`。

不得使用“最相关”排序、fuzzy matching、rename inference 或从文案猜测当前节点。缺少任一
所需回答或 target ID 时，flow focus 必须显示 `LIMITED`/缺失说明，而不是自动选择替代节点。

## 4. 视觉与可访问性合同

- 流程图放在页面导语与五个问题卡片之间，读者先获得全局位置再阅读细节；
- 桌面宽度使用有向水平流程，窄屏转为垂直流程；
- 同时区分：
  - 上下文阶段；
  - 当前研究关注路径；
  - 当前页面位置；
  - 本页以外的 Owner 决策边界；
- 每个阶段另行显示状态胶囊；流程角色颜色不得替代进展状态颜色；
- 不仅依赖颜色，必须配套文字 badge、序号和 `aria` 语义；
- 每个关注阶段显示 exact target ID，便于审计；
- 1280px 不出现页面级横向滚动；长 hash/ID 必须安全换行；
- 保持 static HTML：无 script、form、external resource、write action。

## 5. 实现范围

Task-owned：

- `src/ai_trading_system/atlas/cited_query_renderer.py`；
- `tests/atlas/test_cited_query_renderer.py`。

Coordinator-owned：

- `docs/task_register.md`；
- 本 requirement；
- `docs/system_flow.md`；
- `docs/artifact_catalog.md`；
- ARCH-004 compatibility/deprecation authority；
- ARCH-004E / ARCH-005 generated views。

不修改 query/request/response/citation public schema；若实现证明必须改变公共合同，应停止并
另行执行最小 serial contract wave。

## 6. 验收标准

1. 页面展示八阶段全系统流程，顺序和边界与本 requirement 一致；
2. `CITATION_FIRST_QUERY` 唯一显示“你在这里”；
3. 当前关注路径只使用五个 explicit target IDs；
4. 八个节点分别显示冻结的状态码、中文标签和颜色，图例声明状态不等于策略 PASS/投资评级；
5. 图中明确声明本页不执行 DQ/backtest/promotion/production/broker；
6. 缺失或重复 question/target 时页面不生成并 typed fail closed；既有 LIMITED response
   保持原状态，不被流程图升级；
7. deterministic double-build byte-identical；
8. no script/form/external/write 与 escaping contract PASS；
9. renderer focused、Atlas focused、task shadow、DevEx、compatibility/deprecation 和适用
   formal tiers PASS；
10. 使用 reviewed browser surface 对 1280px 与窄屏布局完成视觉复核；
11. `production_effect=none`、`broker_action=none`。

## 7. 工作区与安全

- governed mode：`SINGLE_LANE`；
- frozen base：登记提交后的 exact local `main`；
- task branch：`codex/trading-2471-atlas-flow-focus-map`；
- known-unrelated exclusion：
  `docs/research/growth_tilt_owner_diagnosis_pack.md`，不得读取、hash、复制、stage、修改或删除；
- retained preview 继续位于
  `outputs/atlas/strategy_research_cited_query/trading_2470_v1/`；
- 本任务不创建额外 worktree、server、data cache 或 strategy artifact；
- 页面 visual QA 不授权改变 production/broker 状态。

## 8. 进度记录

- 2026-07-31：Owner 在 TRADING-2470 canonical preview 上要求增加“整个策略系统流程以及
  当前实际关注节点位置”的流程图。本任务登记为独立展示增强；未授权修改投资语义、公共
  query contract、DQ/backtest 结果、production 或 broker 行为。
- 2026-07-31：八阶段 snake flow、唯一 `aria-current=step`、五个 exact-ID focus rows 与
  read-only safety boundary 已实现。renderer focused=`4 passed`、完整 Atlas/citation
  focused=`60 passed`、Ruff PASS；canonical double-build byte-identical，新
  `index.html`=`30698 bytes / SHA-256 9eb9ad17992a31fdf0b7ccd3c0260c754d8c6c0fbc87d98e8d90d52f51ef498e`，
  `responses.json` 与 `validation.json` SHA 保持不变。静态 DOM audit 确认 8 stages、
  1 个 current step、5 focus rows、unique ids、900/620px breakpoints、无
  script/form/iframe/external resource。Browser skill 刷新本地 `file://` 时被 URL policy
  拒绝且禁止 workaround；browser visual 暂记 `NOT_EXECUTED_URL_POLICY`，等待 Owner 在
  已打开页面手动刷新验收。
- 2026-07-31：按 Owner 后续要求，为八个节点增加独立进展状态胶囊与五色图例。状态计数为
  `NOT_EXECUTED_BY_PAGE=2`、`IN_PROGRESS=1`、`LIMITED=2`、`VALIDATED=2`、
  `PENDING_OWNER_REVIEW=1`；每项同时显示中文标签与状态码，并明确“不代表策略 PASS 或投资
  评级”。renderer focused=`4 passed`、Ruff 与 deterministic double-build PASS；新
  `index.html`=`34751 bytes / SHA-256 c0f3eb5507063dbe4ded202a942dc6c0b9af250b99d91ddd720626941851680e`，
  `responses.json`/`validation.json` SHA 保持不变。static DOM audit 确认 8 stages、
  1 current、5 focus、进展状态分布精确匹配、unique ids、900/620px breakpoints、
  no script/form/iframe/external。等待 Owner 手动刷新进行视觉验收。
- 2026-07-31：Owner 在 canonical `file://` preview 手动刷新并明确回复“这个版本先验收
  通过”。browser automation 仍保持 `NOT_EXECUTED_URL_POLICY`，人工视觉验收单独记录为
  `OWNER_MANUAL_VISUAL_PASS`。最终候选状态设为 `BASELINE_DONE`；必须以
  `trading-2471-owner-visual-closeout-v1` 边界的 generated authority、formal tiers 与
  governed closeout 全部 PASS 后才允许提交、集成本地 `main` 与普通推送。
