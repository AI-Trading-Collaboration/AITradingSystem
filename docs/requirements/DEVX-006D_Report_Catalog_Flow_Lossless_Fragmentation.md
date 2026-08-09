# DEVX-006D Report / Catalog / Flow Lossless Fragmentation

## 任务状态

- Task ID：`DEVX-006D_REPORT_CATALOG_FLOW_LOSSLESS_FRAGMENTATION`
- Priority：`P1`
- Status：`BASELINE_DONE`
- Governed mode：`SINGLE_LANE`
- Exact base：`57bec20b15a7b1d471bdb774a2966bb81f02a037`
- Owner decision：`owner_decision:DEVX-006C:2026-08-09:authorize_c_then_d_then_s5_serial_v1`
- Production effect：`none`
- Broker action：`none`

## 背景与目标

`config/report_registry.yaml`、`docs/artifact_catalog.md` 和 `docs/system_flow.md`
仍是跨任务共享的大型单体 authority。现有 shallow inactive fragments 不能完整重建原文件，
不得直接激活。本波在 DEVX-006C 已发布的 exact main 上建立 full-entry v2 fragment shadow：
对三类 authority 做完整、确定性、可审计的拆分与重放，在任何 source-of-truth cutover 前证明
byte-identical render、100% coverage 和 0 silent drop。

## 分步计划与依赖

### D1：冻结、盘点与消费者清单

- 冻结三个 monolith 的 exact bytes、LF SHA-256、Git blob 与结构计数；
- 盘点现有 fragment、生成器、direct reader/writer 和 current-authority consumer；
- 明确 entry identity、顺序、原始字节区间、分区和跨文件引用边界；
- 所有不能无损表达的 legacy 内容均保留在 sealed prefix/opaque entry，不做语义猜测。

### D2：Full-entry v2 fragment 与 index 合同

- 定义 content-addressed full-entry fragments 与小型 hash-chain index；
- index 记录 source seal、entry id、order、path、content hash、previous hash 和 coverage；
- duplicate、missing、unknown、reorder、tamper、path escape、symlink、hash-chain drift、
  non-canonical bytes 与 collision 全部 fail closed；
- repeat build 必须 deterministic，生成中间态不得成为 canonical tracked authority。

### D3：Byte-identical shadow render 与迁移清单

- 从 v2 index/fragments 重建三份 monolith，逐字节等于 frozen source；
- coverage 必须为 100%，missing/duplicate/silent drop 为 0；
- direct consumers 使用统一 loader/render contract，或显式登记 immutable-byte/history 例外；
- 默认仍以现有 monolith 为 source of truth；rollback 只关闭 fragment shadow，不回写或重排
  monolith。

### D4：最终树验证与发布

- 更新必要的 architecture fitness、task shadow、consumer inventory 和 system-flow 说明；
- focused、Architecture、Contract、Integration、Reproducibility、Full 使用最终树新证据；
- ordinary main push、exact SHA equality、runner/shared-path cleanup 后释放给 ARCH-005 S5；
- S5 不得复用本波 formal evidence，必须从 D exact released main 独立登记。

## 范围

### In scope

- 三份 monolith 的 seal、full-entry v2 fragments、index、loader、shadow renderer；
- 动态 direct-consumer inventory、迁移状态和机械防回归；
- 与本波新增模块相关的 generated architecture/task-shadow 刷新；
- `docs/system_flow.md` 对 authority/read/render/rollback 边界的同步说明。

### Out of scope

- 不激活现有 shallow inactive fragments；
- 不执行 report/catalog/flow source-of-truth cutover；
- 不删除或重写三个 monolith，不修改其业务语义；
- 不改变 daily scheduler、DQ/PIT、scoring、backtest、investment conclusion；
- 不启动 ARCH-005 S5，不执行 cloud/API/HTTP/raw/paper/live/broker/production 动作。

## 验收标准

- 三个 legacy source seal 在实现期间保持精确可验证；
- full-entry v2 render 对三份当前 monolith 全部 byte-identical；
- entry coverage 100%，duplicate/missing/silent drop 均为 0；
- index/fragment repeat build deterministic，hash/path/order/chain/tamper failure fail closed；
- direct consumers 有完整动态 inventory、明确迁移状态和 rollback；
- owner review 前 `source_of_truth` 不切换，monolith runtime writer 不因本任务增加；
- focused 与五级 formal/Full 在 exact final tree PASS；
- ordinary push 后 `HEAD = local main = origin/main`，任务分支与 runner/shared paths 清理完成。

## 开放问题与退出条件

- 若结构边界不能同时满足 byte parity 与稳定 entry identity，必须停在 inactive shadow，记录
  blocker；不得以规范化 Markdown/YAML、重新排版或 shallow fragment 规避。
- 仅当本任务 ordinary push/cleanup RELEASE 后，ARCH-005 S5 才可从 exact latest main 登记。

## Progress notes

- 2026-08-09：DEVX-006C 已 ordinary push/cleanup，exact released main=
  `57bec20b15a7b1d471bdb774a2966bb81f02a037`；本波按 Owner 指定顺序登记，尚未发生
  implementation write、pytest 或 formal evidence 复用。
- 2026-08-09：D1 base seal 已记录：`config/report_registry.yaml`=
  `1814070 bytes / c25f1dac...e1af / blob 696096ab...`，`docs/artifact_catalog.md`=
  `1990360 bytes / 805e300d...8097 / blob 590de4e6...`，`docs/system_flow.md`=
  `2178497 bytes / 9ac3da0e...a22b / blob 45eecdaa...`。由于 AGENTS.md 要求同 change 更新
  system flow 且新增 shadow artifact 需要目录解释，canonical v2 seal 将绑定本任务文档更新后的
  final monolith bytes；base seal 仅作 drift/attribution 对照，不冒充 final source seal。
- 2026-08-09：首次 LANE claims 未列出 `docs/artifact_catalog.md` 与 C compatibility carry-forward
  路径；artifact catalog 的本任务说明已在 claim 扩展前写入。exclusion-aware audit 当时仅包含
  本任务字节、无 stage/runner，随后在任何 compatibility/generated 共享写入前以扩展 claims
  重跑 LANE preflight PASS。该事件保留为 coordinator claim-expansion audit incident，不作为
  放宽治理或复用 evidence 的依据。
- 2026-08-09：D1-D3 `BASELINE_DONE`。Final source seals：report registry=
  `1814070 bytes / c25f1dac...e1af / 1371 entries`，artifact catalog=
  `1991577 bytes / f67cb12f...ef42 / 546 entries`，system flow=
  `2179534 bytes / 086c5f68...5fd4 / 933 entries`。三者各 64 个 content-addressed
  partitions，共 192 fragments；shadow render 3/3 byte-identical、coverage=100%、silent drop=0。
- 2026-08-09：compact index=`874673 bytes / feb16525...b050`，较首轮重复 path/hash 的
  2.15 MB index 明显收缩；entry order 不含级联 ordinal/chain，partition hash-chain 每 target
  上限 64 条。Dynamic consumer inventory=`159 consumers / 15 pending Owner cutover /`
  `c9bb8226...7395`；因此 `cutover_ready=false`、`source_of_truth=LEGACY_MONOLITH`，rollback
  保持 `IGNORE_INACTIVE_SHADOW`。focused cross-layer=`111 passed`；final-tree formal/Full 与
  ordinary push/cleanup 仍由本 coordinator 完成后再 RELEASE S5。
- 2026-08-09：首轮 final-tree Full=`8693 passed / 2 failed / 3 skipped`；两项失败均来自
  `tests/test_trading2452_architecture_contract.py` 的 append-only compatibility successor
  白名单只包含 DEVX-006C、尚未包含同合同的 DEVX-006D，导致 D 最新 section 被错误套用
  “不得晚于 TRADING-2501”的历史 fallback。该测试路径在修改前加入 expanded LANE claim；
  首轮 Full 保留为失败证据，不复用为 closeout PASS。
- 2026-08-09：首轮 targeted 修正后发现 D 的 compatibility `sources` 漏列本波已刷新的
  `arch_004e_aggregate_shadow_index.yaml`，使该路径仍落到旧历史 section 的过期 current hash；
  已把它纳入 D append-only supersession authority，并要求重新生成 compatibility fragments、
  focused 契约与全量 final-tree evidence。
- 2026-08-09：修正后完整 compatibility/fragment/deprecation/TRADING-2452 focused replay=
  `246 passed`，Ruff、strict mypy、compileall PASS。四份只属于失败/中间生成树且不被最终 index
  引用的 compatibility fragments 已按 exact path 删除；canonical evidence 仅保留 index 引用的
  C/D fragments。最终 formal boundary 冻结为 `DEVX-006D-FINAL-LOSSLESS-SHADOW-V2`；V1 Full
  失败证据永久保留，但不得用于 promotion/closeout。
