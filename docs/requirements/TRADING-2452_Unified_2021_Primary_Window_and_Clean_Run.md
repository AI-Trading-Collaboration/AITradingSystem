# TRADING-2452：统一 2021 主研究窗口与 Dynamic v3 clean run

最后更新：2026-07-21

状态：`DONE`

稳定任务 ID：`TRADING-2452_UNIFIED_2021_PRIMARY_WINDOW_AND_CLEAN_RUN`

## Owner 决策与目标

Project owner 于 2026-07-21 确认：后续活跃策略研究、主回测与主要结论统一从
`2021-02-22` 开始，不再把 `2022-12-01` 作为活跃研究边界或必选比较节点。
`2022-12-01` 只允许保留在 immutable historical artifacts、旧 schema/compatibility evidence
和历史结论说明中，不得继续成为新 run 的默认起点、primary conclusion boundary 或 gate。

本任务同时替代 TRADING-2451 已冻结但尚未运行的 S1 package。旧 package 继续保留为历史
preregistration evidence，不原地改写；新 package 必须使用新的 version/package id、冻结时间与
content hashes，验证通过后才允许继续 TRADING-106 historical-seen fold-local evaluator。

Active / version-and-rebuild / retained-historical 的迁移分类与初始引用盘点见
`docs/research/unified_2021_active_window_migration_inventory.md`。

## 统一窗口口径

- active primary research window：`2021-02-22..latest`，window id 保持
  `exact_three_asset_validated`；
- historical-seen main folds：`2021-02-22..2025-12-31`，按半年 test period 扩展；
- recent pre-freeze diagnostic：`2026-01-02..preregistration freeze 前最后完整交易日`，只作
  已知结果近期诊断，不进入完整半年 fold 主评分；
- prospective untouched holdout：保持 `2026-07-22..2027-07-21`，本任务不读取、不授权；
- 所有历史 replay 必须继续声明 `prior_market_outcome_visibility=KNOWN`、
  `historical_replay_investigator_blind=false` 和 `unbiased_oos_claim_allowed=false`。

`2024-12-31` 原截止点来自旧 `dynamic_walk_forward_policy` 将 2025 定义为 locked holdout，
不是数据可用性限制。由于 2025 结果已经可见且旧 R1 已披露 holdout contamination，继续排除
2025 不能增加无偏性；真正 outcome-blind 的证据仍只来自 freeze 后 prospective holdout。

## 实施拆解与文件归属

|阶段|内容|验收|
|---|---|---|
|S0|盘点 active default、legacy compatibility 与 immutable evidence|每个 `2022-12-01` 引用归类为 migrate / historical-retain；无静默批量替换|
|S1|迁移 active project/research policy 与语义契约|新 run default/primary start 均为 `2021-02-22`；报告不再要求 2022 节点|
|S2|生成 versioned TRADING-2452 preregistration package|300 candidates、train-only top 20、六个完整半年 folds、recent diagnostic、0 result inputs、content-derived validator PASS|
|S3|执行 owner 已授权的 TRADING-106 historical-seen clean evaluator|先过 runtime `aits validate-data`；不访问 prospective holdout，不扩候选、不搜索参数|
|S4|共享集成与正式验证|task/system flow/catalog/manifests/compatibility/deprecation 同步；focused/architecture/contract/full 按风险 PASS|

S4 同时要求 evaluator module 对 `--help`/未知参数先完成 CLI parse，不能把文档探测解释为正式
research execution；`--help` 必须在创建 run directory、DQ artifact 或 worker 前退出 0。

策略实现文件由 strategy lane 独占；Wave 7 工程线只允许修改选定的 bounded test leaf。任务表、
`docs/system_flow.md`、catalog、generated manifests、compatibility baseline 与最终提交由 coordinator
单写，避免共享路径竞态。

## Evaluator 性能门禁与优化记录

正式 evaluator 不是以“最终能跑完”为唯一验收。每次长跑都必须记录 phase timing、固定/候选
report cache hit/miss、worker 数与安全边界，并设置 30 分钟诊断预算；超过预算或外推明显超预算时，
应停止、定位热点、完成等价性验证后再重跑，不能让长尾计算无限占用双线研发容量。

- 首个未优化 diagnostic 在约 51 分钟时仍未产出 train/test/result，被主动停止；8 workers 持续
  满 CPU，判定为算法重复计算而非挂死。
- 第一轮优化把每 phase/window 的固定 report 从 per-candidate 重算改为只计算 3 份，并按 exact
  candidate policy hash 做 worker-affinity cache；专项等价性、tamper 与 train→select→test 屏障测试 PASS。
- 第一轮优化后的正式 run 第 1 个 train phase 约 6 分 20 秒、test 约 50 秒；按全 phase 外推仍约
  40～45 分钟，故未等到 30 分钟才确认风险，而是在第 2 个 train 开始后主动停止。
- 第二轮曾假设 `build_dynamic_v3_real_evaluation_report` 对已校验 robustness report 的重复
  `deepcopy` 是主热点；专项回归 PASS 后真实重跑，第 1 train=`6m11s`，相对第一轮约`6m20s`
  没有工程上显著收益。该改动及新增测试已完整撤回，保留原来的深复制隔离语义；后续优化必须先
  计算真实 unique-policy/cache miss 分布或 profiler 证据，不再凭对象大小推断主热点。
- 全局按 `(candidate label, policy hash)` 去重后，每个非空 train phase 只计算 `228` 份候选
  report，而不是 8-worker affinity 下的 `372` 份；候选 report 使用 phase-transient
  content-addressed cache，batch 消费与二次 checksum/binding/inventory 校验通过后删除大 payload，
  manifest 仅保留 commitments。正式 run 共删除 `1,368` 份、释放 `15,318,824,739` bytes，
  final candidate cache 为空。
- 第一次全局缓存 run 的 13 phases 已完成，但 JSONL writer 错用了多行 canonical serializer，
  content-derived validator 因首行不是完整 JSON object 正确 `FAIL`；该 run 被排除，未修补、未纳入
  lineage。修复后新增 one-object-per-line roundtrip test，并对空 test/recent phase 跳过无用 fixed
  report，同时将已做 1/8/16/24-worker 等价验证的默认并行度设为 24。
- 正式有效 run=`trading2452-historical-seen_20260721T053621Z_144f31edee91`，总耗时
  `1545.11s`（25m45s），低于 30 分钟预算；package、runtime DQ、output checksum、source binding、
  transient cache commitments 与独立 content-derived validator 全部 PASS。

## 安全边界

- `research_only=true`、`manual_review_required=true`、`production_effect=none`、
  `broker_action=none`；
- `candidate_expansion_allowed=false`、`new_parameter_search_allowed=false`；
- owner 本次授权仅覆盖新预注册包 PASS 后的 historical-seen TRADING-106 evaluator；
- `prospective_holdout_access_allowed=false`、`paper_shadow_change_allowed=false`、
  `production_weight_change_allowed=false`、`unbiased_oos_claim_allowed=false`；
- 不得修改旧 TRADING-2451 package bytes 或把旧结果重新标记为 clean evidence。

## 验收标准

- active source-of-truth 不再把 `2022-12-01` 作为 default backtest/research start；
- legacy/immutable 引用保留时必须机器可区分，不能被新 run 消费为 primary evidence；
- 新 preregistration 的 source、candidate、selection、window、cost、lag、purge/embargo、checksum
  和 safety flags 可从 tracked inputs 重算，任意 drift/tamper fail closed；
- 2025 folds 只使用各自 train evidence 排名，test metrics 不参与 selection；
- 2026 pre-freeze recent diagnostic 与 prospective holdout 不重叠，且不参与主 fold ranking；
- evaluator 运行前通过同源 cached-data quality gate，结果明确 historical-seen 限制；
- 不产生 paper-shadow、promotion、production、official weights 或 broker/order 副作用。

## 状态记录

- 2026-07-21：owner 确认统一采用 `2021-02-22`，接受 historical main folds 延伸至
  `2025-12-31`、2026 freeze 前只作近期诊断、`2026-07-22` 后保持 untouched prospective
  holdout；并要求策略线与工程线继续按双线计划推进。任务进入 `IN_PROGRESS`。
- 2026-07-21：active paper-shadow/account backfill 新建 v2 policy 并切换 resolver；v1 原文件
  保持历史只读。TRADING-2452 package/content validator 与 runtime DQ 已 PASS。
- 2026-07-21：两轮长跑均在未形成研究结果时主动停止。首轮 worker-affinity 版第1 train约
  `6m20s`；第二轮`deepcopy`假设实测约`6m11s`且已撤回。真实 policy-hash 分布证明当前每
  train phase 有228个全局唯一 reports，但局部cache仍构建372份；进入全局content-addressed
  phase cache优化与等价性验证，完成前不继续正式run。
- 2026-07-21：正式有效 run 在 24 workers 下用 `1545.11s` 完成，独立 validator PASS；DQ=
  `PASS_WITH_WARNINGS`、as_of=`2026-07-20`。六个 train folds 共 1,800 个 COMPLETE evaluations，
  但全部 `gate=reject`、`selection_score=null`；1,800 行均命中
  `constraint_hit_rate_exceeds_policy`，其中第 6 fold 另有 96 行命中
  `constraint_hits_delta_exceeds_policy`。因此 selected/test/recent counts 均为 0，最终状态为
  `INCOMPLETE_NO_ELIGIBLE_CANDIDATE`。这是真实负面结果，不授权放宽阈值、扩候选或搜索参数；
  下一策略步骤必须先做 constraint-hit 结构诊断与 owner review。
- 2026-07-21：formal gate 前的 CLI 探测发现 evaluator module 会忽略 `--help` 并启动 24-worker
  正式路径。本次误启动进程与 186,744,597 bytes incomplete run directory 已按精确 PID/run id 清理；
  S4 增加 parse-before-execute contract 和 focused regression，避免帮助/未知参数触发昂贵研究执行。
- 2026-07-21：首个 full 的 6 个迁移失败修复后，failure-fix full 为
  `6541 passed / 1 failed / 2 skipped / 643 warnings / 1028.74s`。唯一失败是 simple-baseline
  fixture 已扩到 883 行，但 data-repair proof 的三处历史期望仍为 420；统一为 883 后 focused
  node `1 passed / 32.37s`。这是 fixture contract 同步，不改变 production 计算或研究结论。
- 2026-07-21：Windows singleflight 初始化竞态由 OPS-066 修复后，最终 formal Full=
  `6543 passed / 2 skipped / 642 warnings`，pytest=`1009.31s`、runner=`1010.39s`；profile、
  telemetry、performance evidence、scheduler 与 provenance 全部 PASS，fallback=false、tail idle max=
  `10.32s`。TRADING-2452 S0～S4 完成并归档 `DONE`；下一策略任务为 TRADING-2453，仍不得
  放宽阈值、扩候选或访问 prospective holdout。
