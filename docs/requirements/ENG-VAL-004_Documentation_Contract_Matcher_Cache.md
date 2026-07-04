# ENG-VAL-004 Documentation Contract Matcher Cache
最后更新：2026-07-05

## 背景

ENG-VAL-003 后，`contract-validation` runtime artifact 显示当前最慢项为：

- `tests/test_documentation_contract.py::test_default_documentation_contract_covers_current_report_registry`
  `50.45s`；
- `build_documentation_contract_payload()` 默认读取完整 `config/report_registry.yaml`
  和 `docs/artifact_catalog.md`，当前约 1244 reports、4645 artifact globs、979 catalog rows；
- 当前 matcher 对每个 report 逐行扫描 catalog，并在匹配过程中重复执行 path normalize 与
  Markdown code-span regex extraction。

## 目标

在不降低 documentation contract 覆盖、不改变 report registry / artifact catalog 语义、
不改变 validation tier 默认策略的前提下，降低 docs contract 全量扫描耗时：

1. 对纯字符串 normalize / Markdown code-span extraction 增加缓存；
2. 保持 `_catalog_row_matches()` 与 `_artifact_pattern_matches()` 的匹配语义不变；
3. 保持默认 registry/catalog 全量扫描、summary fields、issues、pass/fail 判定不变；
4. 用 focused pytest 与 `contract-validation --write-runtime-artifact` 记录优化后 runtime。

## 安全边界

- 不减少 documentation contract 检查范围；
- 不修改 report registry、artifact catalog、report outputs、data quality gate、cached data、
  strategy logic 或 production state；
- 不改变 `scripts/run_validation_tier.py` 默认 `DEFAULT_DIST=loadfile` /
  `DEFAULT_WORKERS=16`；
- 不改变 pass/fail/warning 判定或 generated payload schema；
- 不生成 paper-shadow、production、official target weight、broker/order 或 runtime mutation。

## 实施步骤

|步骤|状态|验收标准|
|---|---|---|
|登记任务与需求|DONE|`docs/task_register.md` 与本需求文档记录范围、边界和验收标准|
|缓存 matcher 纯函数|DONE|normalize/code-span extraction 缓存后 focused docs contract 测试仍 PASS|
|验证 runtime 收益|DONE|focused pytest、Ruff、compileall、contract-validation runtime artifact 和 docs/task gates 通过|
|归档|DONE|任务移动到 completed，并记录最终 runtime 对比|

## 进展记录

- 2026-07-05：根据 ENG-VAL-003 后的 slow-duration evidence 新增并进入 `IN_PROGRESS`。
  本任务只优化 documentation contract matcher 的纯函数重复计算，不改变 contract 覆盖、
  registry/catalog 内容、validation runner 默认策略或生产边界。
- 2026-07-05：实现完成并归档 `DONE`。`_normalize_path_text()` 与 `_code_spans()`
  使用进程内缓存，literal prefix 在不含 `?` / `[]` glob metachar 时等价短路；
  日期类 `????-??-??` artifact glob 保持 `fnmatch` fallback。完整 registry/catalog
  pairwise reference 对比覆盖 4,547,455 个 pattern/catalog 组合，mismatch=0。
  微基准中 `build_documentation_contract_payload()` payload 阶段从约 `44.48s`
  降到 `4.68s`；focused docs contract pytest `5 passed in 12.31s`，
  `test_default_documentation_contract_covers_current_report_registry` call duration
  从 ENG-VAL-003 artifact 的 `50.45s` 降到 `5.43s`。`contract-validation`
  通过 `197 passed`，runtime artifact=`outputs/validation_runtime/contract-validation_20260704T183540Z/test_runtime_summary.json`，
  tier elapsed 从 `191.61s` 降到 `181.10s`，top20 slow duration total 从
  `394.89s` 降到 `340.96s`；docs contract 已退出 top20，当前最慢项转为
  tail-risk independent setup、controlled strategy CLI smoke、current subscription
  CLI smoke 和 tail-risk fallback / policy fixture 群。
