# TRADING-2536 — Atlas provider/transport attribution successor classification serial contract wave V1

- priority: `P0`
- status: `BASELINE_DONE`（non-terminal；等待 final-tree validation、ordinary publication 与新 Owner token）
- owner: Codex capability coordinator
- governed mode: `SINGLE_LANE`
- contract change: `true`（Atlas consumer-visible successor classification）
- frozen base: `c290f1244bb81df789d3b95d29d894b657943ca8`
- production effect: `none`
- broker action: `none`

## 1. 背景

TRADING-2535 已形成 final never-chain session 的 sealed、zero-order、export-safe
provider/transport attribution proposal，但在 Full final-tree validation 前被 append-only task
authority 投影为 `DONE`。Full `full_20260819T022717Z` 随后以
`UNCLASSIFIED_SUCCESSOR_REVIEW_REQUIRED` 失败：Atlas 没有对 2535 的页面影响作出显式分类。

2535 的 terminal event 不得回退或改写。本任务是最小 serial contract wave：吸收同一 frozen
base 上尚未提交的 2535 candidate，补齐 Atlas page-effectiveness 分类和读者摘要，然后重新执行
final-tree validation 与 publication。它不改变 2535 proposal hashes、分类语义或外部授权边界。

## 2. 页面分类合同

Atlas 必须把 2535 表述为：

1. 独立 export-safe attribution proposal 已建立并通过离线 replay/tamper 检查；
2. 它只计划区分 provider catalog empty、provider available but subscribed Slice not delivered、
   provider probe error 与 indeterminate；
3. 尚未执行任何 Cloud/provider probe，因而唯一 never-chain session 仍未获得真实归因；
4. 不得把 proposal 写成 DQ/PIT PASS，不得解锁 research、selection、engine、order、fill 或交易。

页面 freshness 只有在 task identity、requirement hash、coverage code 与读者摘要全部进入
`config/atlas/page_effectiveness.yaml` 后才可恢复 `CURRENT`。本任务不会冒充 Owner visual 或
reader-comprehension review。

## 3. 归属与路径

本任务采用 `SINGLE_LANE` coordinator correction，明确吸收同一 frozen base 上的 2535
uncommitted candidate。Task-owned：

- 2535 supporting requirement、proposal policy、builder、tests、sealed package 与 task-specific
  architecture fragments；
- `config/atlas/page_effectiveness.yaml`；
- `tests/atlas/test_page_effectiveness.py`；
- `tests/atlas/test_cited_query_renderer.py`；
- `tests/atlas/test_historical_projection_review.py`；
- 本 supporting requirement。

Coordinator-owned：canonical task authority/views/index、`docs/system_flow.md`、generated
architecture/compatibility/deprecation/report-catalog authority，以及 ignored Atlas rendered
artifacts、`src/ai_trading_system/atlas/cited_query_renderer.py` 中的 successor display upper
bound，以及 formal runtime artifacts。

已登记的 unrelated exclusion
`docs/research/growth_tilt_owner_diagnosis_pack.md` 不得读取、hash、stage 或修改。

## 4. 验收

- governed contract-change preflight 对扩展后的真实路径声明 PASS；
- Atlas manifest 显式包含 TRADING-2535，coverage 与摘要保持 proposal/unexecuted 边界；
- canonical Atlas page 重新生成，freshness=`CURRENT`，页面没有声称 attribution 已完成；
- 2535 package hashes 与 replay 结果不变；
- focused、Architecture、Contract、Integration、Reproducibility、Full 在同一 final tree PASS；
- task terminal 后 commit、local-main ff-only、ordinary non-force push、SHA verify 与 cleanup 完成；
- publication 后仍停在 `OWNER_FINAL_TOKEN_REQUIRED`，不得自动访问 QuantConnect、Cloud、raw
  rows、Object Store、orders、fills 或 broker。

## 5. Failure evidence

- Full：`7 failed / 9218 passed / 3 skipped / 643 warnings`，`1327.73s`；
- runtime artifact：
  `outputs/validation_runtime/full_20260819T022717Z/test_runtime_summary.json`；
- 7 个失败均由同一 typed blocker `UNCLASSIFIED_SUCCESSOR_REVIEW_REQUIRED` 引起；
- 该失败不是 provider/transport attribution 结果，也不构成外部运行证据。

## 6. Pre-formal evidence

- 2535 terminal requirement binding 已通过 append-only `DONE→DONE` correction event 补齐；
- Atlas policy 同时分类 2535 proposal 与 2536 classification wave，renderer successor display
  upper bound 已由 2534 提升至 2536；
- canonical Atlas page 已重建为 freshness=`CURRENT`，但 Owner visual 与 reader-comprehension
  review 仍保持 `PENDING_REVIEW`；
- Atlas focused：`39 passed`；2529–2535 adjacent evidence chain：`128 passed`；
  architecture/generated/terminology focused：`45 passed`；
- external counters 仍为 project mutation/backtest/order/fill=`0/0/0/0`；不复用 2532 token。
