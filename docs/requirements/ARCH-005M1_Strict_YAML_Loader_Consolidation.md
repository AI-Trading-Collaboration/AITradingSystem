# ARCH-005M1：Strict YAML Loader Consolidation

最后更新：2026-07-27

稳定任务 ID：`ARCH-005M1_STRICT_YAML_LOADER_CONSOLIDATION`

Owner continuation：
`owner_continuation:ARCH-005M1:2026-07-27:continue_long_term_engineering_goal`

状态：`BASELINE_DONE_BATCH_2`

## 1. 问题与目标

项目已有通用 `safe_load_yaml_text/path`，但它只保证使用 PyYAML safe loader，不拒绝
duplicate mapping key。五个对治理或运行时配置要求 duplicate-key fail-closed 的模块各自复制
了 `_UniqueKeySafeLoader` 和 mapping constructor：

- `platform/architecture/wave_readiness.py`；
- `platform/architecture/arch_004_g2_5_readiness.py`；
- `platform/architecture/task_portfolio_normalization.py`；
- `platform/architecture/integration_revalidation.py`；
- `us_equity_special_closure_policy.py`。

这些实现的 key policy、YAML merge flatten、non-finite/cyclic-alias处理和模块错误 code 并不
完全相同。直接替换为一个“更严格”实现会改变已有输入合同；继续复制则会让新控制面反复实现
同类安全逻辑。本任务建立单一可配置的 strict parsing primitive，并在 characterization
保护下分批迁移。

普通 `safe_load_yaml_*` 的广泛调用面不属于本任务。它们不得被批量升级或改变行为。

## 2. 当前差异盘点

|调用面|Key policy|YAML merge flatten|Non-finite/cycle|公开错误边界|
|---|---|---|---|---|
|Wave readiness|string only|否|显式拒绝|`WaveReadinessError`|
|G2.5 readiness|hashable|是|保持PyYAML现状|`G25ReadinessError`|
|Task portfolio|hashable|是|保持PyYAML现状|`TaskPortfolioNormalizationError`|
|Integration revalidation|string only|否|保持PyYAML现状|`IntegrationRevalidationError`|
|US special closure|hashable|是|保持PyYAML现状|wrapped `ValueError`|

Batch 1 只迁移语义相同的 G2.5 readiness 与 task portfolio 两个 architecture loader。
Wave readiness、integration revalidation、US special closure和普通safe loader留在后续批次。

## 3. Batch 1 合同

在 `ai_trading_system.yaml_loader` 新增可复用 strict primitive：

- 仍基于 `yaml.SafeLoader`，不允许 unsafe tag；
- 可配置是否执行 `flatten_mapping`；
- 可配置只允许string key或允许任意hashable key；
- duplicate key和unhashable key输出typed generic error及1-based line；
- 可配置递归拒绝non-finite number/string和cyclic alias；
- 不读取路径、不吞掉上层路径/UTF-8错误；模块wrapper继续拥有自己的read/error contract。

Batch 1 的两个wrapper必须逐项保持：

- G2.5：`YAML_DUPLICATE_KEY`、`YAML_UNHASHABLE_KEY`、`YAML_PARSE`和`YAML_READ`；
- Task portfolio：`POLICY_DUPLICATE_KEY`、`POLICY_UNHASHABLE_KEY`、
  `POLICY_YAML`、`POLICY_NOT_UTF8`和`POLICY_READ`；
- duplicate detail继续包含`key=<repr>`和1-based line；
- YAML merge、non-string hashable key、malformed YAML、cyclic alias和non-finite接受/拒绝行为
  与迁移前相同；
- policy schema、semantic validation、hash、manifest或生成输出不变。

## 4. 实施步骤

1. 为canonical primitive增加duplicate、unhashable、string-only、merge、malformed、
   non-finite与cycle characterization tests；
2. G2.5 wrapper改为调用canonical primitive并映射generic error；
3. Task portfolio wrapper执行同样迁移；
4. 删除两个模块内的loader class/constructor复制；
5. 更新system flow中的architecture governance parsing边界；
6. 刷新architecture/task generated views和append-only compatibility authority；
7. 运行focused、DevEx、architecture、contract及required Full；heavyweight gate只在最终
   integration candidate自然边界执行。

## 5. 验收标准

- canonical strict primitive有独立typed合同和完整负例；
- 两个模块不再定义本地`_UniqueKeySafeLoader`或mapping constructor；
- 迁移前后合法payload解析结果逐字节/结构相等；
- 全部既有错误类型、错误 code、key/line detail和read/UTF-8边界保持；
- 任何schema、policy threshold、DQ/PIT、strategy、backtest、report conclusion、
  production或broker行为均不改变；
- remaining三个loader明确保留为后续批次，不把Batch 1误报为全任务完成。

## 6. 生命周期与集成边界

- 模式：`SINGLE_LANE` serial maintenance contract wave；
- frozen base：`bc8496b11039f3d6a8d2bc837e821c298e04c9cf`；
- branch：`codex/arch-005m1-strict-yaml-batch1`；
- 不创建新worktree、clone、stash或cache；
- 不读取、修改或提交
  `docs/research/growth_tilt_owner_diagnosis_pack.md`；
- TRADING-2462 v5 和 DEVX-006 branch均不属于本任务；
- 当前main若在任务期间前进，保留frozen branch并在集成边界使用DEVX-006 plan，不重建v2/v3；
- `production_effect=none`、`broker_action=none`。

## 7. 进度

- 2026-07-27：C3等待source-owner决定期间，从既有P2维护项中选择不触碰策略或数据语义的
  strict YAML consolidation。只读inventory确认5个本地strict loader具有三类不同语义，
  不适合一次性替换；Batch 1冻结为G2.5+Task Portfolio两个hashable-key、merge-flatten实现。
  SINGLE_LANE START preflight在exact main `bc8496b1` PASS，任务进入
  `IN_PROGRESS_BATCH_1`。
- 2026-07-27：canonical strict primitive、两个wrapper迁移和characterization tests完成。
  Ruff PASS，strict mypy对3个变更source file PASS；首轮3文件并行pytest为`83 passed`，
  扩展并行回归覆盖Wave readiness、US special closure与DevEx共`149 passed, 1 failed`。
  唯一失败为预期的generated architecture manifest freshness：新增module/test及source
  line变化尚未在frozen lane刷新generated views。该结果不是运行时或兼容性失败；为避免与
  TRADING-2462 v5之后的最新main重复生成和重复执行heavy gate，generated views、
  append-only compatibility authority、architecture/contract/required Full统一留到
  单一latest-main integration candidate。当前状态进入`VALIDATING_BATCH_1`，不得据此
  宣称Batch 1或整个ARCH-005M1完成。
- 2026-07-27：frozen候选`49b2ccf30`通过DEVX-006机制接入
  `main=origin/main=4e6eb8aa6`后的单一final candidate
  `codex/integration-20260727-devx006-arch005m1`；没有创建ARCH-005M1 v2/v3 worktree。
  task registry与DevEx generated views已在组合最终树各刷新一次，deprecation inventory
  仅因新增1个module与2个test file从`1030/1196`更新为`1031/1198`。状态进入
  `VALIDATING_FINAL_TREE`，最终architecture/contract/Full及compatibility authority仍须通过。
- 2026-07-27：在`6dc8a643a` latest-main integration tree上，Batch 1 focused=`278 passed`，
  strict mypy与Ruff PASS；组合Architecture/Contract/Integration/Reproducibility分别为
  `710/275/995/23 passed`。append-only compatibility authority历史前缀绑定
  `6dc8a643a:inputs/architecture/arch_004_compatibility_baseline.yaml` exact blob，31项current
  source hash逐项一致。Batch 1仍等待required Full与post-Full治理门禁，不扩张到其余3个loader。
- 2026-07-27：组合final tree的唯一required Full通过
  `7454 passed / 5 skipped / 643 warnings`，artifact=`full_20260726T193718Z`。Batch 1状态
  进入`VALIDATING_POST_FULL`；remaining三个loader继续是后续批次，不把本次Full解释为全量迁移。
- 2026-07-27：最终归档树的post-Full Architecture/Contract分别为`710/275 passed`，
  artifacts=`architecture-fitness_20260726T200007Z`、
  `contract-validation_20260726T200525Z`。Batch 1转`BASELINE_DONE_BATCH_1`；剩余3个strict
  loader仍保留在本任务后续批次中，必须逐个characterization与迁移，未宣称全量统一完成。

## 8. Batch 2：Integration Revalidation Loader

### 8.1 范围与依赖

Batch 2只迁移
`platform/architecture/integration_revalidation.py`的policy loader。该loader属于并行研发
集成控制面，当前语义为：

- key只允许string；
- 不执行YAML merge flatten；
- duplicate key输出`POLICY_YAML_DUPLICATE_KEY`且message仅保留重复key文本；
- non-string key输出`POLICY_YAML_NON_STRING_KEY`和1-based line；
- malformed YAML、文件读取、UTF-8解码统一映射为`POLICY_READ_FAILED`；
- non-finite value和cyclic alias保持PyYAML既有行为，不新增recursive post-traversal；
  recursive mapping仍在解析期失败，recursive sequence仍由后续schema/path contract拒绝。

Batch 2复用Batch 1已发布的`load_strict_yaml_text`，配置
`key_policy=STRING`、`flatten_mapping=false`、`reject_non_finite=false`。不得修改canonical
primitive、integration plan分类算法、policy schema、path overlap、contract claim、Git delta、
candidate creation或validation tier语义。

### 8.2 步骤

1. 先补齐duplicate detail、non-string line、merge、non-finite、cyclic alias、malformed、
   read和UTF-8 characterization；
2. wrapper调用canonical primitive并逐code映射
   `DUPLICATE_KEY/NON_STRING_KEY/INVALID`；
3. 删除本地`_UniqueKeySafeLoader`与constructor；
4. 证明reviewed policy解析结果与迁移前结构相同，更新system flow的remaining-loader边界；
5. 刷新generated/compatibility authority，并运行focused、Architecture、Contract及最终
   required Full；Full只在自然integration boundary运行一次。

### 8.3 验收与生命周期

- Batch 2前后公开error type/code/message、合法payload结构及read/UTF-8边界一致；
- `integration_revalidation.py`不再直接定义strict loader或调用`yaml.load`；
- Wave readiness与US special closure仍保留为后续独立批次；
- frozen base=`ebeb67f6d014d4037a2559093a8e2394d96fd9dd`，task branch=
  `codex/arch-005m1-strict-yaml-batch2-integration`；
- 不创建worktree/clone/stash，不触碰既有operations worktree或known-unrelated owner文档；
- 无DQ/PIT/strategy/backtest/report conclusion/production/broker变化。

### 8.4 Batch 2进度

- 2026-07-27：M3完成并推送后，READ_ONLY与SINGLE_LANE START preflight在exact
  `main=origin/main=ebeb67f6d`、active lease=0下PASS。选择Integration Revalidation作为
  第二批，是因为它直接服务base-drift classification和单一integration candidate规划；
  本批不迁移另外两个remaining loader。状态进入`IN_PROGRESS_BATCH_2`。
- 2026-07-27：Integration Revalidation wrapper已改用canonical strict primitive，本地
  `_UniqueKeySafeLoader`与constructor删除。新增characterization覆盖duplicate detail、
  non-string line、no-merge、non-finite、recursive mapping/sequence、malformed、unsafe tag、
  missing file与invalid UTF-8；`tests/test_yaml_loader.py`和Integration Revalidation合计
  `29 passed`，strict mypy与Ruff PASS。状态进入`VALIDATING_BATCH_2`，下一步刷新
  generated/compatibility authority并运行formal gates。
- 2026-07-27：append-only compatibility authority已绑定`ebeb67f6d`历史prefix exact
  Git blob；完整兼容性回归`90 passed`，累计focused evidence=`119 passed`。当前12项
  live source hash全部匹配；Architecture=`735 passed`、Contract=`275 passed`，下一步
  运行required Full。
- 2026-07-27：唯一required Full以`natural_integration_boundary` provenance通过：
  `7504 passed / 3 skipped / 643 warnings`。Batch 2验收满足，状态转
  `BASELINE_DONE_BATCH_2`；最终元数据树执行post-Full Architecture/Contract。Wave readiness
  与US special closure仍须后续独立批次，ARCH-005M1未归档。
