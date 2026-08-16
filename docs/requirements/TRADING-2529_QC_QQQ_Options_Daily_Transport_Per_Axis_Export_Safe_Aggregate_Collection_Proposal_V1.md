# TRADING-2529 — QQQ Options daily transport per-axis export-safe aggregate collection proposal V1

- status: `BASELINE_DONE`
- priority: `P0`
- governed mode: `SINGLE_LANE`
- predecessor: `TRADING-2528`
- production effect: `none`
- broker action: `none`
- external action: `none`
- Owner authorization boundary: `PROPOSAL_PREPARATION_ONLY`

## 1. Owner 授权与本任务目标

Project Owner 于 2026-08-16 授权建立新的 governed 后继任务，并生成“逐轴 export-safe aggregate
采集提案”和 exact hashes；提案完成前明确不授权 Cloud、raw rows、交易、订单或其他外部动作。

本任务只生成可供 Owner 逐字段复核的严格离线 proposal package。package 必须冻结未来候选采集的
repository/policy/code/project/range/session/action identity，提供 canonical seals、exact file/content SHA-256、
一次性 token template 与 allowed/prohibited action inventories。任务本身不得代签 token，不得登录
QuantConnect，不得修改项目，不得运行 Cloud backtest，不得下载 Results/log/raw rows，不得调用 API/CLI/HTTP/
Object Store，也不得产生策略、DQ/PIT、selection、engine、paper/live/broker/production 结论。

## 2. 继承事实与问题边界

- frozen predecessor main：`4366092a2284557a659daa3bd497250ea0ce1052`；
- 2522 backtest id：`60ce7e0bec3ad2d83a4d1341e0221492`；
- requested/evaluated range：`2021-02-22..2025-12-02`，expected XNYS sessions=`1202`；
- actual chain sessions=`1201`，valid candidates=`0`，transport rejected sessions=`1201`；
- 2522 v4 authorization 已消费，`further_cloud_run_authorized=false`；
- 2528 只可确认 `OPTION_CHAIN_PRESENCE=PRESENT`，其余七轴均为 `NOT_EVALUATED`，reject scope=
  `UNRESOLVED_COMBINATION`；
- 当前没有足够 export-safe aggregate 区分 underlying、bid/ask、Greeks、IV、OI、volume 与 cross-field
  consistency 中的具体失败轴；不得从 2522 汇总反演 raw rows 或猜测 root cause。

## 3. 待 Owner 审阅的候选 scope

proposal 可以提出但不得执行以下上限：

- target project id：`34808569`；
- requested/evaluated range：`2021-02-22..2025-12-02`；
- primary role / calendar：`PRIMARY / XNYS`；expected sessions=`1202`；
- maximum project mutations / Cloud backtests：`1 / 1`；
- maximum orders / fills：`0 / 0`；
- candidate lifecycle：login → one existing-project mutation → one zero-order Cloud run → one manual
  export-safe Results JSON collection；
- authorization：single-use、第一次 run attempt 即失效、最长 168 小时；
- output 只允许 session-level/count-level derived aggregates，不允许 individual option contract、strike、expiry、
  symbol、quote、Greeks 或其他 raw-row carrier。

逐轴 proposal 至少包含以下稳定轴：

1. `OPTION_CHAIN_PRESENCE`；
2. `UNDERLYING_PRICE`；
3. `BID_ASK_QUOTE`；
4. `GREEKS`；
5. `IMPLIED_VOLATILITY`；
6. `OPEN_INTEREST`；
7. `VOLUME`；
8. `CROSS_FIELD_CONSISTENCY`。

每轴只可输出 `present/missing/invalid/not_evaluated` 的 session/count aggregate 与 typed rejection reason；
不得输出或记录 individual contract values。

## 4. 明确禁止

本任务与本阶段 Owner 授权不允许：

- QuantConnect login、project create/update、Cloud backtest、Results download；
- API、CLI、HTTP、Object Store 或后台网络替代路径；
- raw option rows、Logs、individual option contract identifiers/values 的下载、记录、导出或重建；
- 第二次 project mutation、第二次 Cloud run、范围扩张、购买/订阅；
- 任何 order/fill、selection/engine 解锁、paper/live/broker/production 行为；
- DQ/PIT PASS、策略有效性、收益、风险、可交易性或投资解释。

若 package 尚未 ordinary-push、exact hashes 不匹配、Owner token 缺失/过期/重复/范围漂移，后继 admission
必须输出 typed failure 并保持 `POLICY_BLOCKED_CASH_PRESERVATION`。

## 5. 计划 package 与合同

canonical package 计划位于：
`inputs/research/qqq_options/trading_2529_daily_transport_per_axis_collection_proposal_v1/`。

exact inventory：

1. `run_scope.json`：project/range/session/action/axis/safety scope；
2. `proposal.json`：未授权 proposal 与 expected token template；
3. `main.py`：拟议的 deterministic zero-order、aggregate-only project code；
4. `owner_decision_request.md`：Owner 可读的 exact hashes、allowed/prohibited actions、expiry 与确认文本；
5. `package_manifest.json`：上述四项与 upstream authority 的 path/SHA-256/byte-count cross-binding。

strict loader 必须拒绝 symlink、路径逃逸、missing/extra file、duplicate JSON keys、noncanonical JSON、axis/range/
session/project/action/hash/code/policy drift、提前授权、raw-row carrier、orders/fills 与任何安全边界放宽。

## 6. Path claims

Task-owned：

- `docs/requirements/TRADING-2529_QC_QQQ_Options_Daily_Transport_Per_Axis_Export_Safe_Aggregate_Collection_Proposal_V1.md`；
- `config/research/qc_qqq_options_daily_transport_per_axis_collection_proposal_v1.yaml`；
- `src/ai_trading_system/qqq_options_research/daily_transport_per_axis_collection_proposal.py`；
- `tests/test_qqq_options_daily_transport_per_axis_collection_proposal.py`；
- `inputs/research/qqq_options/trading_2529_daily_transport_per_axis_collection_proposal_v1/**`。

Coordinator-owned：canonical task registry/index/views、`docs/system_flow.md`、architecture fragments/manifests、
report/compatibility authority 与 final formal validation。页面 effectiveness 的 reviewed successor consumer 还包括：

- `config/atlas/page_effectiveness.yaml`；
- `src/ai_trading_system/atlas/cited_query_renderer.py`；
- `tests/atlas/test_page_effectiveness.py`；
- `tests/atlas/test_cited_query_renderer.py`；
- `tests/atlas/test_historical_projection_review.py`。

这些路径只把 2528/2529 的当前真实状态投影为可读页面：2528 已完成离线诊断但未定位根因，2529 提案已
准备且等待 Owner 最终授权；不改变 page-effectiveness schema、研究结论或外部动作权限。

## 7. 实施顺序

1. 登记 canonical 2529 task 与本 requirement；
2. 在 exact local main 上运行 governed START/LANE preflight 并建立 task branch；
3. 实现 versioned policy、sealed proposal/run scope/package、deterministic code 与 strict loader；
4. negative/property/golden tests 证明 hash/axis/range/session/action/raw-row/authorization drift fail closed；
5. 生成 exact package，接线 system flow/generated authority，并在 final tree 完成适用 formal gates；
6. ordinary non-force push 后向 Owner 提交 exact hashes 与最终一次性 token 文本；
7. 在 Owner 最终确认前，`external_action=none` 且不启动任何 Cloud/browser/network writer。

## 8. 验收标准与 exit condition

1. package exact、canonical、可重放，绑定 2522 failure evidence 与 2528 per-axis diagnostic contract；
2. proposed `main.py` 仅产生逐轴 session/count aggregate，静态/动态测试证明没有 raw rows、orders/fills；
3. proposal 完整列出一次性上限、expiry、allowed/prohibited actions 与 exact hashes；
4. Owner token 缺失时任何 admission/execution 均 fail closed；
5. 页面与文档只能表述“proposal ready / Owner final token pending”，不能表述 Cloud run 或 evidence 已发生；
6. focused、generated/compatibility 与适用 formal validation PASS；
7. ordinary-pushed exact package 与 token template 交付 Owner 后，本任务可进入 `BASELINE_DONE`。

真实 token admission、Cloud action、Results collection、evidence admission、DQ/PIT 和 independent review 必须
另立后继任务；不属于 TRADING-2529。

## 9. 2026-08-16 proposal closeout candidate

离线 package 已生成并通过 exact replay。2529 的完成边界是“可审阅提案已 ordinary-push；Owner final token
pending”，不是外部采集已经发生。当前 `external_action=none`、Cloud=`0`、project mutation=`0`、raw rows=`0`、
orders/fills=`0/0`。

Exact authority / package identity：

- proposal policy file / canonical SHA-256：
  `05f45abfc296cb9e622559fde0602f4274ac9a52a42cacb92b1d6cca86707cc9` /
  `417af9f94d81f83c44feb4dde3b663a7f67122abbda99ceb77bffd416c351f73`；
- source 2528 diagnostic content / file SHA-256：
  `e8125e165f8acf6147f15fbd64701832ba6f602bbc98d69863d65ae942b8b7aa` /
  `b2382b928a860685412add5ac091ac458d08ab9d246351a4a5a516d050eca9ac`；
- run scope content / file SHA-256：
  `6c10f143fa542505b4696f255303510015e6b2318f22d6ac83e1c0933a974c33` /
  `23b9a1fa2aef4973b6c1e8892e6245a99f37587554de02fcb32609b4c0dd0a13`；
- proposal content / file SHA-256：
  `2c41024a72229245290599da58056d5b0fd31da9cce7a562e9b7fe9e411081c9` /
  `09d62938680a2d2190e03787de3cf44c7d0097c7e004956ad56235f809320656`；
- candidate `main.py` LF SHA-256 / bytes：
  `adfc060fff3cfd840565fb000ac4a1759b6f54f847568dd46c5418912d0b1421` / `24420`；
- Owner request file SHA-256：
  `0fcd5fe80d0d282b22c1d2713047a51883ee09b3231c200b0307eab1e1501677`；
- package manifest content / file SHA-256：
  `79e6d0ca0b2a0b9793e1dad1fbecb033e90f5dfb36c0e8cc4430fd316398c5ea` /
  `856d3fda6b5ffb6ac4d5bd56886d03c4a62868498d63fc9ae0a27a079b2f6d33`。

Validation candidate：proposal + predecessor focused `35 passed in 83.19s`；architecture/compatibility组合
`88 passed in 95.32s`；DevEx、report/catalog authority、compatibility authority validators 均 PASS。最终 formal
tiers 与 ordinary push 由同一 coordinator candidate 完成；若 final-tree identity 或 hashes 漂移则 fail closed。

Atlas successor reconciliation 已完成：初次 Full 的 7 个失败全部来自 2529 尚未纳入页面 reviewed successor
coverage，而不是 proposal/hash/采集逻辑失败。修复后 renderer 不再向普通读者暴露未解释的内部状态标识，
页面明确显示“2528 只确认期权链出现、2529 只是待授权提案”，并把 coverage/audit 上限从 2528 更新到 2529。
最终本地页面 `index.html` SHA-256=`1ffda0de2ad9f04ded7822559f75cd5de2c38956eb01d3438a48aafbf0516d6c`；
Atlas focused=`39 passed in 133.86s`，页面 manifest/validation sidecar 同步重建且验证 PASS。该本地页面是
ignored presentation artifact，不属于 Cloud、浏览器或外部动作证据。
