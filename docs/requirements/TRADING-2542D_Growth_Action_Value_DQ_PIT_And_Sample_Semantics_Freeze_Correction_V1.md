# TRADING-2542D：Growth Action Value DQ/PIT 与样本语义冻结修正 V1

## 1. 状态与目标

- 状态：`IN_PROGRESS`；
- 优先级：`P0`；
- 上游：`TRADING-2542C_GROWTH_ACTION_VALUE_INDEPENDENT_REVIEW_REMEDIATION_AND_FREEZE_READINESS_V1`；
- governed mode：`SINGLE_LANE` serial contract wave；
- 目标：以新版本修正 DQ/PIT V2 与 exact sheet V3 中已证实的首日 prior、excluded invalid、contributor terminal、right-censored episode、run authority 和 keyed reconciliation 缺陷；
- 不可变边界：V3 exact sheet 与 DQ/PIT V2 的文件、canonical hash 和历史证据保持不变；本任务只新增 DQ/PIT V3 与 exact sheet V4 successor；
- 安全边界：不查询 provider，不读取或改写真实 cache，不执行真实 DQ、Cloud backtest、empirical evaluation、production、broker、order 或 fill 行为。

## 2. Owner 指令与第二轮独立复核

Project Owner 已采纳 Web Pro 结论并要求 Codex 继续完善和推进剩余修复。该指令授权本地工程整改、测试、正式门禁和普通 Git 发布，不授权真实数据或投资执行。

第二轮 advisory review 固定在：

- repository commit：`e5266c9aadfba067060b013d83ec26bd4f065604`；
- conversation：`https://chatgpt.com/c/6a8b95b1-30dc-83e8-8d49-4b74a696acc1`；
- retrieval：10 个请求文件均为 exact-commit blob retrieval `SUCCESS`；
- UI/product/composer evidence：`Pro`；response self-report：`GPT-5.6 Pro`；
- route evidence：`UI_PRO_AND_SELF_REPORT_PRO_ROUTE_UNVERIFIED`、`ROUTING_ATTESTATION_UNAVAILABLE`、`CANNOT_VERIFY_EXACT_BACKEND_ROUTE`；
- overall disposition：`REQUEST_NEW_VERSION_BEFORE_OWNER_FREEZE_DECISION`。

Reviewer advisory 仅作为整改输入；仓库合同、实现和测试仍由本地 exact tree 独立核验后成为 authority。

## 3. 已确认缺陷

### 3.1 DQ/PIT V2

1. `2021-02-22` 是 1202-session target inventory 的首日，但 V2 只在 target inventory 内寻找 prior，因而首日无法得到正确 prior。独立 pre-window authority 必须固定为 `2021-02-19`，且不能把 target inventory 扩成 1203 日，也不能跨日 fallback 或豁免首日。
2. excluded row 在 identity、PIT、timestamp、quote validity 之前提前返回 `EXCLUDED`，与“excluded invalid 必须传播”的 YAML 语义冲突。
3. `expected_contributor_count=0` 的 policy terminal 应为显式 `FAIL`；expected 非空但 observed 为零、缺失或不匹配应为 `INVALID`。V2 用通用 reasons 合并后不能稳定区分。
4. caller 可分别传入 sessions、identity 和 manifests，缺少一个 typed、canonical-hash-rooted run authority，可能把互不一致的合法片段拼成非法 run。
5. synthetic threshold 可由 caller 任意构造，不能证明 numeric test 使用了被审阅 YAML 的 exact values。

### 3.2 exact sheet V3 / sample semantics

1. `active_episode_intervals` 在 transitive merge 前丢弃 raw right-censored episode；若它与先前 completed episode 相连，旧 completed episode 会被错误保留。必须先形成完整连接分量，再对整个 cluster 应用 right-censor terminal。
2. cost reconciliation 使用位置对齐，未按 session key 对齐；输入顺序或缺失日可能造成错配。
3. 部分 exact values、outcome strings、单位和缺失映射未逐项 pin/tamper-test。

## 4. Successor 合同

### 4.1 DQ/PIT V3

- target inventory 精确绑定 `2021-02-22..2025-12-02`、`expected_session_count=1202`、首尾日期和 canonical session inventory SHA-256；
- pre-window prior 独立绑定 `2021-02-19`，只服务首个 target session；后续 session 使用前一 target session；
- 一个 typed `RunAuthorityV3` 同时绑定 target sessions、pre-window prior、完整 instrument identity、每 session contributor manifest、evidence scope 和 canonical authority SHA；
- evaluator/aggregator 只接受已验证 authority，不接受 loose sessions/identity/manifests；
- identity/PIT/timestamp/quote invalid 检查先于 noncontributing exclusion；`INVALID` 优先级高于 `EXCLUDED`；
- zero expected contributor 明确 `FAIL`；expected 非空但 observed/missing/mismatch 明确 `INVALID`；numeric `FAIL/UNKNOWN` 按既有 collect-all 语义保留；
- numeric thresholds 只能从加载并 hash 校验后的 V3 YAML authority 派生；synthetic evidence 不得构造另一套阈值；
- 四个 pilot 数值原样保留：`120 seconds`、`0.20`、`open interest 10`、`volume 1`。它们仅获 `NON_EXECUTABLE_PILOT_FREEZE_READY`，对真实/可执行证据仍为 `INSUFFICIENT_EVIDENCE_TO_APPROVE`。

### 4.2 exact sheet V4

以下六轴必须逐字段保持 V3：

- `NON_BETA_ACTION_VALUE`；
- `NET_OF_COST_RETURN`；
- `ACTUAL_PATH_DRAWDOWN_REGRESSION`；
- `FALSE_RISK_OFF_COST`；
- `ACTUAL_PATH_TURNOVER`；
- `LEVERAGE_BETA_ATTRIBUTION`。

仅为以下两轴建立 successor：

- `CANONICAL_DQ_PIT`：绑定 DQ/PIT V3 exact file/canonical authority；
- `SAMPLE_AND_WINDOW_DEPENDENCE`：先构造和 transitive-merge 完整 raw episode clusters，再把含 right-censored tail 的整个 cluster 排除；跨 slice inclusion 和 single assignment 保持显式。

cost reconciliation 改为按唯一 session key 对齐，拒绝重复 key、集合不等、缺失日和非 decimal-return unit。V4 loader、canonical replay、source/test 文件名必须显式版本化。

## 5. 分阶段实施与验收

### D1：authority 与 evaluator

- 新建 V3 YAML、typed loader、run authority builder 和 evaluator/aggregator；
- 覆盖首日 PASS、错误/缺失 pre-window prior、1201/1203 inventory、excluded identity/PIT/quote invalid、zero expected/observed 和 threshold tamper。

验收：首日与全部 1202 日可在合格 synthetic authority 下得到 `GLOBAL_PASS`；任何身份、PIT、session、manifest 或 threshold drift 均 fail closed。

### D2：sample/cost successor

- 新建 V4 YAML 和 versioned loader；
- 实现 keyed cost reconciliation 和 cluster-level right-censor；
- 逐项 pin 六个不变轴及两个 successor 轴的 exact values、units、outcomes 和 predecessor hashes。

验收：连接的 right-censored tail 不得遗留已完成 episode；成本输入顺序不影响结果，重复/缺失 session 必须 `INVALID`。

### D3：治理、验证与发布

- 更新 `docs/system_flow.md`，通过 canonical task source 和官方 generator 重建 task、architecture、Atlas、report-flow 与 compatibility authority；
- focused pytest 使用 `-n 16 --dist loadfile`；最终候选运行 Architecture、Contract、Integration、Reproducibility 和唯一 Full；
- validation PASS 只证明工程合同，不等于真实 DQ、策略价值或投资结论 PASS；
- 正常 fast-forward local `main`、fetch、ordinary non-force push、SHA 等值验证和受治理清理。

验收：所有正式门禁在同一 exact candidate 上通过；旧 V3/DQ V2 hash 不变；`local main = origin/main = candidate`。

## 6. 必须通过的回归

1. first target session `2021-02-22` 使用 exact pre-window prior `2021-02-19` 并可 PASS；
2. exact 1202/1202 synthetic inventory 得到 `GLOBAL_PASS`；
3. missing/wrong prior、1201/1203 inventory fail closed；
4. excluded identity/PIT/timestamp/quote invalid 传播为 `INVALID`；
5. zero expected=`FAIL`，expected nonempty but zero observed/missing=`INVALID`；
6. transitive connected right-censored tail 排除整个 cluster；
7. threshold、outcome、unit、predecessor、session hash tamper 被拒绝；
8. keyed cost reconciliation 对乱序稳定，对 duplicate/missing key fail closed；
9. V3 exact sheet 与 DQ/PIT V2 file/canonical SHA-256 完全不变。

## 7. 开放边界与下一步

- 本任务完成后 successor 状态仍为 `NEW_VERSION_DRAFT_COMPLETE_PENDING_INDEPENDENT_REVIEW_AND_OWNER_FREEZE_DECISION`；
- 再次 independent review 与 Owner freeze decision 之前，不建立 executable DQ authority；
- 真实 provider/cache/DQ/backtest 需要后续单独固定 R1 manifest；
- 当前缺失 options 日期 `2022-08-26` 的 provider 事实核验不在本工程波次内，本任务只消除导致日期/数据归因不可信的工程缺陷。

## 8. 生命周期与进度

- branch：`codex/trading-2542d-dq-pit-sample-semantics`；
- frozen base：`de3fe9cb039ead4023fe76864dde75cc42a9f541`；
- publication transactions：`v1` 因初始 shared-path claim 不完整失败释放；`v2` 在候选 `3b478af688dd7145a4f0e490d32bdeaa82c1ba9c` 的 Architecture 正式档发现两项冻结元数据漂移后失败释放；`v3` 在 Atlas rebuild 发现 successor 未分类后失败释放；`v4` 在 exact-commit 全页渲染发现未登记的组合术语后失败释放；`v5` 在 Architecture/Contract/Integration/Reproducibility 均 PASS 后，因 local `main` 已前进到 `6d4e0d8383328554e50eeb2ac86abb88f90d4384` 而在 Full dispatch 前 fail closed；`v6` 因 Atlas import-isolation 回归失败而释放；`v7` 修复后 343 项聚焦回归 PASS，但在 generated post 后发现 lifecycle 文本仍指向 v6，因此失败释放；当前收敛事务为 `trading-2542d-publication-20260824-v8`；
- base drift reconciliation：原 task lane head 为 `fe5079d7dd99f8ede013e0a276cf38fd9754800e`，最新 main 含 TRADING-2545；`integration-revalidation-669f97fdfb5c784f7e50` 判定 `RECONCILIATION_REQUIRED`，无 contract conflict、undeclared path 或 branch rebuild 要求。v6 从最新 main 建立单一 integration candidate，保留 TRADING-2545 reader projection，并把 2542D 设为当前策略工程后继；
- 不创建额外 worktree、clone、provider cache 或 credential 文件；
- known-unrelated exclusion `docs/research/growth_tilt_owner_diagnosis_pack.md` 不得读取、hash、diff、stage 或修改；
- 退出条件：工程 successor、验证、普通发布和临时资源清理完成；若正式门禁失败，保留失败 artifact 并用新 transaction/parent 重试。

- 2026-08-24：第二轮 Web Pro review 完成，本地复现并确认八项工程缺陷；Owner 已要求继续修复。取得 publication transaction，开始登记任务；未运行 provider、真实 DQ 或 backtest。
- 2026-08-24：完成 DQ/PIT V3 工程 successor。file/canonical SHA-256 分别为 `b84d8d3dbe2dded761e989c623469607c386297e59d61207bb478d3054523c2e` / `9140e68dce070ca5cd421fe05ab480c9d2d330fd21a7f7c6cff0bda0b00aca8b`；typed run authority 绑定 contract canonical SHA、exact 1202 target inventory、独立 `2021-02-19` prior、identity、per-session manifests 与 evidence scope。首日、1202/1202、invalid excluded、zero expected/observed、authority/threshold tamper 回归通过。
- 2026-08-24：完成 exact sheet V4 工程 successor。file/canonical SHA-256 分别为 `e525e40eab86e5a8fd748a38cbfde390eb12ffa5acd05211da21ec85719e1a58` / `04f43175ff915e97a0921b08168febdb3e16fbc89326b4876794eabbb4c14e29`；六个轴与 V3 model dump 精确一致，DQ/PIT 轴绑定 V3，sample 轴在 cluster merge 后应用 right censor，成本按唯一 session key 对齐。Ruff、strict mypy 与全部 208 项 growth-action-value 并行回归通过；正式 generated authority 与五层 final-candidate validation 待执行。
- 2026-08-24：候选 `3b478af688dd7145a4f0e490d32bdeaa82c1ba9c` 的 Architecture 正式档得到 `876 passed / 2 failed`。失败均为新增任务/模块后冻结元数据未同步：canonical task count 仍为 `1023`（实际 `1024`），deprecation inventory 仍绑定旧 inventory id 与 `1149/1311` 仓库计数（实际 `arch_004g_deprecation_inventory_2840110c582631d26591`、`1151/1313`）。已保留 `architecture-fitness_20260824T052211Z` 失败证据并在 v3 事务中修正；策略 successor 行为测试没有失败。
- 2026-08-24：v3 official Atlas rebuild 以 `ATLAS_LIVE_UNCLASSIFIED_SUCCESSOR_REVIEW_REQUIRED:TRADING-2542D...` fail closed，证明新 successor 尚未进入页面分类而不能发布。v4 将 2542D 加入 `page_effectiveness` task coverage，并把 `live_snapshot` 的 current mainline、largest blocker 与 next legal action 同步到 2542D；该页面同步只陈述 draft/blocked 边界，不提升 DQ、策略价值或投资结论。
- 2026-08-24：v4 exact-commit Atlas 全页渲染以 `RENDERED_TERM_UNKNOWN_IDENTIFIER` 拒绝摘要中的组合标识 `pilot-freeze-ready`。v5 改用已支持的中文读者表达“不可执行 pilot 的冻结就绪状态”，不改变合同值、status、DQ 或投资解释。
- 2026-08-24：v5 同一候选上的 Architecture `878 passed`、Contract `278 passed`、Integration `995 passed`、Reproducibility `24 passed`；Full 未 dispatch。由于 main 在门禁期间前进到 TRADING-2545，旧候选证据不得替代最终树验证。v6 按 plan id `integration-revalidation-669f97fdfb5c784f7e50` 在最新 main 上协调共享 Atlas、system flow、report-flow、compatibility authority 与 generated indexes，并将在最终 exact candidate 上重新运行全部正式档。
- 2026-08-24：v6 聚焦回归得到 `342 passed / 1 failed`。唯一失败是 Atlas canonical writer 在本地 `src` 已由 editable install 置于 `sys.path` 后位时不会重新前插，导致受污染 `PYTHONPATH` 可抢先导入外部同名包；这破坏 exact-commit renderer 的来源隔离。未通过清空环境绕过；v7 把 `scripts/render_atlas_strategy_research_page.py` 纳入共享路径并修为无条件本仓库优先，重跑得到 `343 passed`，Ruff 与 strict mypy 也 PASS。
- 2026-08-24：v7 的代码、测试与生成物均通过后，提交前审计发现生命周期摘要仍把 v6 标为当前事务。为避免最终证据自相矛盾，v7 在 candidate commit 前失败释放；v8 只收敛该元数据、canonical task event 及其派生哈希，不改变已经验证的策略或 Atlas 行为。
- 当前工程 terminal 为 `NEW_VERSION_DRAFT_COMPLETE_PENDING_OWNER_FREEZE_DECISION`。四个 numeric 仅为 non-executable pilot freeze-ready；真实/可执行证据仍是 `INSUFFICIENT_EVIDENCE_TO_APPROVE`，provider/cache/DQ/backtest 权限继续关闭。
