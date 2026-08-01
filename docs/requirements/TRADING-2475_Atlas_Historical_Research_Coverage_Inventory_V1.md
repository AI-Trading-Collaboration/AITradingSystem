# TRADING-2475：Atlas Historical Research Coverage Inventory V1

最后更新：2026-08-01

稳定任务 ID：
`TRADING-2475_ATLAS_HISTORICAL_RESEARCH_COVERAGE_INVENTORY_V1`

优先级：`P1`

状态：`IN_PROGRESS`

Owner 决定：

```text
owner_decision:TRADING-2475:2026-08-01:advance_atlas_historical_coverage_inventory_v1
```

production effect：`none`

broker action：`none`

## 1. 决策背景与目标

TRADING-2474 已在 cited-query 页面完整展示当前 Atlas V1.1 representative campaigns 内的
8 个 result 与 12 条 attribution，并明确
`historical_repository_coverage_complete=false`。Owner 随后确认继续推进，因此本任务先建立
全仓历史研究证据的机器可读 coverage inventory，回答：

1. `config/report_registry.yaml` 中有多少 research report entries 与 artifact declarations；
2. Git 当前 exact commit 下，`docs/research/` 与 `inputs/research_reviews/` 有多少 tracked paths；
3. 哪些 tracked paths 已被 research report registry 精确引用；
4. 哪些路径已是 Atlas source、哪些已登记但尚未投影、哪些仍需人工确认语义；
5. 后续 adapter 应从哪些 exact registered authorities 开始，而不是从文件名或自由文本猜结论。

本任务只建立 path/report-level inventory，不读取研究 artifact 内容，不重算 result、attribution、
DQ、model、backtest、score 或 investment conclusion，也不把 inventory candidate 自动升级为
Atlas result。页面展示和真实 result adapter 是后续独立任务。

## 2. Read-only census 基线

2026-08-01 在 `main=0c3780c6c33a7619fe74978baf5df678954767f8` 的 READ_ONLY census 中观察到：

- report registry entries：`1368`；其中 `group=research`：`961`；
- research artifact declarations：`4468`；其中 exact paths：`3754`、wildcard declarations：`685`；
- 两个冻结 research roots 内 tracked paths：`1434`；
- 被 research registry exact path 引用：`1143`；tracked 但未被 exact 引用：`291`；
- 当前 Atlas source paths：`8`；其中与 research registry exact path 直接相交：`4`。

这些数字只是任务登记时的诊断基线，不是最终 authority。实现必须从 exact commit、registry bytes
与 deterministic tracked-path manifest 重新计算并验证，不能把本段数字硬编码为产品结论。

## 3. 冻结 inventory 合同

### 3.1 Authority inputs

V1 只允许读取：

```text
config/report_registry.yaml
config/atlas/source_registry.yaml
config/atlas/historical_coverage_inventory.yaml
git ls-tree @ exact commit（仅两个 declared roots；输出进入 manifest/hash 前按 exact literal path 排除）
```

research roots 固定为：

```text
docs/research
inputs/research_reviews
```

known-unrelated exclusion 固定为：

```text
docs/research/growth_tilt_owner_diagnosis_pack.md
```

实现不得打开、hash、复制、stage、修改或删除该文件。`git ls-tree` 不支持 exclude pathspec，
因此 V1 读取的只有 tree path metadata，并在任何 manifest/hash/record 构造前按 exact literal path
过滤；不得对该路径执行 `git cat-file` 或任何 blob/content 读取。

### 3.2 Mechanical classifications

每条 path/report relation 只允许以下机械状态：

- `ATLAS_SOURCE_BOUND`：exact path 已出现在当前 Atlas source registry；
- `REGISTERED_RESEARCH_ARTIFACT`：exact path 被 `group=research` report entry 声明；
- `TRACKED_UNREGISTERED_REVIEW_REQUIRED`：路径在 declared roots 内被 Git 跟踪，但没有 exact
  research registry declaration；
- `WILDCARD_DECLARATION_REVIEW_REQUIRED`：report registry 只提供 wildcard，V1 不展开、不猜匹配；
- `DECLARED_NON_TRACKED_OR_RUNTIME_ARTIFACT`：exact declaration 不在 declared tracked roots，通常是
  `outputs/` 或其他运行时/配置产物；V1 只保留 declaration，不声称文件当前存在。

不得按文件名中的 `result`、`final`、`decision`、`closeout` 等词推导研究状态，也不得读取 Markdown、
JSON、YAML 或 CSV 研究 artifact 内容来提取结论。

### 3.3 Typed outputs

canonical output 目录：

```text
outputs/atlas/historical_research_coverage_inventory/trading_2475_v1/
```

输出：

- `inventory.json`：schema、exact commit、输入 SHA、tracked-path manifest SHA、report/path records、
  分类计数和安全边界；
- `inventory.md`：面向 Owner 的中文摘要，只展示机械 coverage 与后续 review queue；
- `validation.json`：独立重算 totals、唯一性、分类闭合、输入绑定、known-exclusion 与 no-inference
  合同。

所有输出固定：

```text
research_artifact_content_read=false
result_projection_allowed=false
investment_conclusion_generated=false
production_effect=none
broker_action=none
```

## 4. 分步计划

### S0：登记与 preflight

- 新增本 requirement 与 task-register row；
- 从 exact local `main` 创建 `codex/trading-2475-atlas-history-inventory`；
- 以 `SINGLE_LANE` 声明 task/coordinator paths；
- 不创建额外 worktree，不运行 periodic operations。

### S1：Typed inventory core

- 新增冻结 policy 与 typed inventory module；
- 解析两个 registry，但不读取 research artifact 内容；
- 从 exact commit 构造显式排除后的 tracked-path manifest；
- 生成 report/path records、机械 classifications 与稳定排序；
- duplicate IDs/paths、未知 classification、known-exclusion 泄漏、count/hash drift fail closed。

### S2：Independent validation 与 canonical artifacts

- validator 从同一 authority inputs 独立重建，不信任序列化 totals；
- actual-input double-build byte-identical；
- canonical artifact identities 写入 requirement、artifact catalog 与 report registry；
- 保持当前 cited-query HTML 和 Atlas 8 results / 12 attributions 不变。

### S3：Governed closeout

- 更新 task shadow、generated architecture、append-only compatibility authority；
- focused、Architecture、Contract、Reproducibility 与风险相称的 Full PASS；
- ff-only local-main、ordinary remote push 与任务分支清理。

## 5. 路径与所有权

task-owned：

```text
config/atlas/historical_coverage_inventory.yaml
src/ai_trading_system/atlas/historical_coverage_inventory.py
tests/atlas/test_historical_coverage_inventory.py
```

coordinator-owned：

```text
src/ai_trading_system/atlas/__init__.py
config/report_registry.yaml
docs/task_register.md
docs/requirements/TRADING-2475_Atlas_Historical_Research_Coverage_Inventory_V1.md
docs/system_flow.md
docs/artifact_catalog.md
inputs/architecture/**
registry/development_tasks_shadow/**
registry/development_tasks_shadow_v2/**
tests/test_arch_004_refactor_policy.py
```

resource claim：Git metadata、三个 declared YAML authorities 与 canonical Atlas output directory only；
不读取 research artifact bytes、market/cache/runtime/external source，不启动 HTTP、browser、LLM、
data acquisition、DQ、model、backtest、production 或 broker resource。

## 6. 验收标准

1. exact commit、三个 input SHA 与 tracked-path manifest SHA 全部可重算；
2. `group=research` report IDs 唯一，artifact declarations 保留 exact/wildcard 区别；
3. declared roots 内每个非排除 tracked path 恰好进入一个 path record；
4. exact registered、tracked-unregistered、wildcard 与 non-tracked/runtime classification totals 闭合；
5. 当前 Atlas source crosswalk 使用 exact path equality，不做 fuzzy/keyword matching；
6. known-unrelated path 不被打开、hash、复制、stage、修改、删除，也不进入 manifest/records；
7. 研究 artifact content read count 固定为 0，输出不含 derived result/status/conclusion；
8. duplicate/unknown/missing input、count/hash/tamper 漂移 typed fail closed；
9. actual-input double-build byte-identical，Markdown 与 JSON 数字一致；
10. current cited-query page 与 8 results / 12 attributions identity 不变；
11. focused/generated/compatibility/formal gates PASS；
12. `production_effect=none`、`broker_action=none`。

## 7. Stop conditions 与后续边界

- 如果 inventory 必须读取 research artifact 内容才能分类，停止并另立 content adapter contract；
- 如果需要改变 Atlas snapshot/public query schema，停止并另立最小 serial contract wave；
- wildcard expansion、文件名语义、自由文本/LLM summary、result/status 推断均不属于 V1；
- V1 完成后，Owner 再基于 inventory 选择第一批 exact registered adapters；页面 historical coverage
  panel 与真实 result projection 均是后续独立任务。

## 8. 工作区生命周期

- governed mode：`SINGLE_LANE`；
- registration base：`0c3780c6c33a7619fe74978baf5df678954767f8`；
- registration commit / lane frozen base：`82a234ecc74e3a3275cbdcdee20d1165c6b2bb1c`；
- branch：`codex/trading-2475-atlas-history-inventory`；
- workspace：`D:/Work/AITradingSystem`，不创建临时 worktree/clone/cache；
- canonical outputs 保留到后续 adapter 选择完成，可由 exact commit 与 authority inputs 重建；
- exit condition：validation、ff-only main、ordinary push、branch cleanup 全部完成。

## 9. 进度记录

- 2026-08-01：TRADING-2474 完成 local-main/remote closeout 后，Owner 回复“好的，继续”。按前序
  推荐启动 path/report-level historical coverage inventory；READ_ONLY preflight PASS，
  `local main=origin/main=0c3780c6c33a7619fe74978baf5df678954767f8`，无 active lease 或工作区 blocker。
- 2026-08-01：登记前 census 仅读取 report/source registry 与 Git path metadata，不读取研究 artifact
  内容；观察到 `961` 个 research reports、`1434` 个非排除 tracked research paths、`291` 个
  tracked-unregistered review candidates。以上数字等待 typed implementation 独立重算。
- 2026-08-01：S1 typed core、冻结 policy、exact-commit Git adapter、known-exclusion 双侧过滤、
  deterministic renderer/validator 与 `5` 个 focused tests 已实现；Black/Ruff/Mypy PASS，parallel
  focused pytest=`5 passed`。下一步在 implementation checkpoint commit 上生成 actual-input
  canonical inventory；当前 Atlas HTML、8 results 与 12 attributions 未改动。
- 2026-08-01：implementation checkpoint=`3367d000367dc4a3517af844aec66a5dbd1fbe04`。
  actual-input double-build byte-identical，inventory_id=
  `atlas_historical_coverage_inventory_3a09ccafea85f96382db`，independent validation=`PASS / 15 checks`。
  最终机械计数为 `1369 registry reports / 962 research reports / 4470 declarations / 1434 tracked
  research paths / 1143 tracked exact-registered / 291 tracked-unregistered / 8 Atlas source paths`；
  Atlas source 中 `4` 个被 research registry exact 登记，`1` 个位于 declared tracked roots。
  current snapshot 复核仍为 `8 sources / 8 results / 12 attributions`。

## 10. Canonical evidence

|Identity|Value|
|---|---|
|Policy SHA-256|`002a0b767a57b9bb5f955a3864dc13abf8fd88cfca998f711a00665c945fdec1`|
|Report registry SHA-256|`6816a90b1ab726a86f708d1187a882a709d6fe2bdc8cc32f1b185c5f7e67fd6c`|
|Atlas source registry SHA-256|`b3845a19b6744c2e922bb5cdfd98f7777df58afc18c9f48e7977597ccff3ef0d`|
|Tracked path manifest SHA-256|`57d5f39ab0f46e8e2a387b146ce1e3716c5f2aae23abd2df0e359730a99c00c9`|
|`inventory.json`|`2660817 bytes / 02497cee0e708cc8f8f9ce345c7bf3fb382194b2eff59bbc85278538645c3e0d`|
|`inventory.md`|`18092 bytes / 84d6b13954edde7522e744564b88b3bb059d2eceac9ee0234bf623beeb3fb05f`|
|`validation.json`|`1528 bytes / 766cc09920feb5ca0084e368828c65f328ec80d82e743515162c977182bf2980`|

以上三份 canonical outputs 位于
`outputs/atlas/historical_research_coverage_inventory/trading_2475_v1/`；输出目录是可重建运行产物，
由 report registry 精确登记，不进入当前 cited-query page identity。
