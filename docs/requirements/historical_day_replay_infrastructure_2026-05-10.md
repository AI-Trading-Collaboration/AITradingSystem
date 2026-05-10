# 历史交易日归档回放基础设施

最后更新：2026-05-10

## 背景

后续模型调优、规则回归、事故复盘和日报解释对“重新生成某个历史交易日的分析产出”会有高频需求。
当前系统已经有生产每日入口 `aits ops daily-run`，也可以通过手工方式做归档输入回放，但这还不是稳定基础设施。

2026-05-10 针对 2026-05-08 的回放验证暴露了当前缺口：

- 5/8 PIT raw payload、normalized CSV 和 manifest 记录本身可用；
- 只复用 PIT 备份不足以完成回放，因为当前工作区已有 5/9、5/10 valuation snapshots，
  按 5/8 as-of 校验会触发 `valuation_date_in_future`；
- 即使隔离未来 valuation，`ops health --as-of 2026-05-08` 仍会因全局 PIT manifest
  已包含 5/10 `available_time` 而失败；
- 最终只有临时隔离 5/9、5/10 valuation YAML，并把 PIT manifest 限制到 5/8
  可见窗口后，缓存回放才通过。

这说明历史日回放必须冻结整套 as-of 输入窗口，并把输出写入隔离命名空间。它不能继续依赖
backup/filter/restore 这类人工操作。

## 目标

- 提供一等公民 CLI，用于基于归档输入重新生成某个历史交易日的日报分析产出。
- 默认 cache-only，不调用 live vendor、OpenAI 或其他外部服务。
- 防止未来数据泄漏：所有输入必须通过 as-of 可见窗口筛选和校验。
- 默认不改写生产 canonical artifacts，例如 `features_daily.csv`、`scores_daily.csv`、
  `daily_score_YYYY-MM-DD.md`、decision snapshot、prediction ledger。
- 输出完整 replay bundle，记录输入来源、row count、checksum、可见窗口、排除的未来数据、
  子命令状态、门禁结果和可比较的产出。
- 支持两类用途：
  - 用当前代码和当前规则在历史输入上重新评分，用于模型调优和候选规则比较；
  - 与当日生产归档产出做 diff，用于回归测试和事故复盘。

## 非目标

- 不把事后供应商请求伪装成当日 forward-only PIT。
- 不自动补写、回填或修复缺失的历史归档输入。
- 不覆盖生产日报、生产 decision snapshot 或生产 ledger。
- 第一阶段不解决长期存储迁移，例如 DuckDB 或 Parquet。
- 第一阶段不把 replay 结果自动推广为 production 规则或生产结论。

## 术语

|术语|含义|
|---|---|
|as-of date|被回放的逻辑交易日，例如 2026-05-08。|
|visibility cutoff|回放时允许看到的最晚时间戳。所有 PIT、valuation、SEC、政策来源和缓存输入都必须不晚于该时间。|
|input freeze manifest|本次回放实际使用的输入清单，包含路径、来源类型、row count、checksum、最早/最晚日期和可见性判定。|
|replay workspace|隔离工作区，包含过滤后的输入视图和本次回放输出。|
|replay bundle|最终可审计产物目录，包含报告、JSON 元数据、输出文件、健康检查和 diff。|
|production replay|尝试复现历史生产日当时的产出。需要历史代码、配置和规则快照，否则只能标记为不可完全复现。|
|candidate replay|用当前代码或候选规则在历史输入上重跑，用于模型调优。必须声明不是原始生产结论。|

## 推荐 CLI

第一版建议新增：

```powershell
aits ops replay-day --as-of 2026-05-08 --mode cache-only
```

推荐参数：

|参数|默认|说明|
|---|---|---|
|`--as-of YYYY-MM-DD`|必填|回放的逻辑交易日。|
|`--mode cache-only`|`cache-only`|默认只读本地归档，不允许外部请求。|
|`--visible-at TIMESTAMP`|自动推导或严格失败|显式可见时间上限。用于消除“日期”和“真实运行时间”的歧义。|
|`--cutoff-policy`|`original-run`|可选 `original-run`、`end-of-asof-utc`、`close-plus-lag`。第一版建议严格优先从历史 run/PIT 报告推导。|
|`--output-root PATH`|`outputs/replays`|replay bundle 根目录。|
|`--label TEXT`|空|人为标签，例如 `candidate_rules_v2`。|
|`--compare-to-production`|false|若存在生产日报和 decision snapshot，生成差异报告。|
|`--openai-replay-policy disabled|cache-only`|`disabled`|默认不调用 OpenAI 且不读取历史 OpenAI 队列；`cache-only` 只复制历史预审队列和报告，不调用 live OpenAI。|
|`--allow-incomplete`|false|默认缺关键输入即失败；打开后只能生成诊断报告，不得标记为完整交易结论。|
|`--inventory-only`|false|只生成 input freeze manifest 和缺口报告，不运行评分。|

阶段 5 扩展：

```powershell
aits ops replay-window --start 2026-05-01 --end 2026-05-08 --mode cache-only
aits ops replay-day --as-of 2026-05-08 --mode cache-only --compare-to-production
aits ops replay-day --as-of 2026-05-08 --mode cache-only --openai-replay-policy cache-only
```

`replay-window` 默认只运行 U.S. equity trading day，周末和 NYSE 常规整日休市日写入
窗口报告的 skipped date 列表。`--compare-to-production` 比较本地 production
artifact 与 replay artifact 的 checksum、row count 和当日行摘要；它用于回归诊断，
不自动把 candidate replay 解释为原始 production 结论。

## 回放流程

### 1. 解析交易日和可见窗口

- 使用现有 U.S. equity market session 逻辑判断 as-of 是否为交易日。
- 默认只允许交易日回放。非交易日需要显式 `--allow-non-trading-day`，并只能进入休市日语义。
- 默认 `visibility cutoff` 优先读取 production `daily_ops_run_metadata_YYYY-MM-DD.json`
  的 `visibility_cutoff`，缺失时退回 as-of 当日 UTC 末尾。
- `visibility cutoff` 必须被写入 replay report。

### 2. 构建 input freeze manifest

系统先只读扫描生产归档，生成输入冻结清单，不运行评分。

必须记录：

- artifact id；
- artifact class；
- provider 或 internal source；
- endpoint 或生成命令；
- 原始路径；
- replay 视图路径；
- row count；
- checksum；
- min/max record date；
- min/max captured_at、downloaded_at 或 available_time；
- 被纳入或排除的原因；
- 是否 production-critical。

建议第一版覆盖这些输入：

|输入类别|可见性规则|
|---|---|
|market prices|记录日期不得晚于 as-of；质量门禁按 as-of 运行。|
|macro rates|记录日期不得晚于 as-of；series 级 freshness 阈值按 as-of 计算。|
|FMP PIT manifest|只保留 `available_time <= visibility cutoff` 的行。|
|FMP PIT normalized CSV|必须是 as-of 文件，且内部 `available_time` 不得超过 cutoff。|
|PIT raw payload|manifest 中引用的 checksum 必须匹配。|
|SEC companyfacts raw|若有 accepted_at 或 filing date，必须不晚于 cutoff；缺结构化时间时在报告中降级说明。|
|SEC metrics CSV|必须匹配 as-of，覆盖率和缺口继续由现有校验器判定。|
|valuation snapshots|排除 snapshot date 或 captured_at 晚于 cutoff 的 YAML。|
|official policy candidates|published_at 或 collected_at 不得晚于 cutoff。|
|risk event occurrences|只纳入 occurrence date、reviewed_at 符合 cutoff 的记录。|
|rule cards/config|需要记录当前代码/配置 hash；若要做 production replay，需要历史规则快照。|

### 3. 创建隔离 replay workspace

不要在生产路径上临时删除或改写文件。建议创建：

```text
outputs/replays/2026-05-08/<replay_run_id>/
  input/
    data/raw/pit_snapshots/manifest.csv
    data/external/valuation_snapshots/
    data/processed/pit_snapshots/fmp_forward_pit_2026-05-08.csv
  output/
    data/processed/
    outputs/reports/
  logs/
  replay_run.json
  replay_run.md
  input_freeze_manifest.csv
  input_freeze_manifest.json
```

输入视图可以用复制、硬链接或只读引用实现。Windows 下 symlink 权限不稳定，第一版应优先用小文件复制和
raw payload checksum 引用；大 raw payload 可共享原路径，但必须只读校验 checksum。

### 4. 运行 replay-scoped scoring

需要让 `score-daily`、`ops health` 和相关校验器支持 replay-scoped path context。

可选实现路径：

1. 短期：给关键 CLI 增加输入/输出 path override，`replay-day` 负责传入过滤后的路径。
2. 长期：引入 `ProjectContext` 或 `RuntimeContext`，替代各 CLI 对全局 `PROJECT_ROOT` 的直接依赖。

长期方案更干净，但影响面更大。建议第一阶段先做 inventory-only 和最小 path override，避免一次性重构过多。

### 5. 生成 replay bundle

每次回放必须输出：

- `replay_run.md`：中文可读总结；
- `replay_run.json`：结构化元数据；
- `input_freeze_manifest.csv/json`；
- `daily_score_YYYY-MM-DD.md`；
- `alerts_YYYY-MM-DD.md`；
- `pipeline_health_YYYY-MM-DD.md`；
- `decision_snapshot_YYYY-MM-DD.json`；
- evidence trace；
- secret scan 或 replay-safe artifact hygiene；
- `diff_vs_production.md/json`，如果启用 comparison；
- `replay_window.md/json`，如果运行窗口批量回放；
- sanitized command log，记录命令、退出码、耗时、stdout/stderr 行数和脱敏错误摘要。

## 数据泄漏防线

回放系统必须 fail closed 于这些情况：

- cache-only 模式发生任何外部 HTTP/API 请求；
- PIT manifest 或 normalized CSV 包含 cutoff 之后的 `available_time`；
- valuation snapshot date 或 captured_at 晚于 cutoff；
- SEC metrics 缺失或覆盖率低于现有门禁；
- market/macro data quality 失败；
- rule card 校验失败；
- replay 输出路径指向生产 canonical artifact；
- 缺少必要 checksum 或 checksum mismatch；
- 无法判断关键输入可见时间，且未显式 `--allow-incomplete`。

`--allow-incomplete` 只能生成诊断输出，报告状态应类似 `INCOMPLETE_REPLAY`，
不得输出为完整投资结论。

## 生产 daily-run 需要补充的归档

为了让未来 production replay 更可靠，生产每日运行也应逐步归档更多元数据：

- run id：已由 `daily_ops_run_metadata_YYYY-MM-DD.json` 记录；
- git commit、dirty diff checksum：已记录 commit、dirty flag、status hash 和 diff hash；
- config hash：已记录 `config/*.yaml` checksum；
- rule card version/hash：已记录 `config/rule_cards.yaml` checksum；
- exact command list：已记录每个 plan step 的 command、enabled、required env 和 skip reason；
- env presence summary，不记录 secret 值：已记录必需 env 的 boolean presence；
- provider request summary：仍主要由各 provider manifest / fetch report 承担；
- input artifact checksums：已记录 production run 开始前的关键输入 checksum，包括市场/宏观缓存、PIT manifest、SEC metrics、valuation/risk/thesis/trade 目录、历史 features/scores、OpenAI 预审队列和 config；
- output artifact checksums：已记录每个 step declared produced path 的文件/目录 checksum、size 和 file count；
- `visibility cutoff` 推导来源：production daily-run metadata 已记录 `visibility_cutoff` 和 `visibility_cutoff_source`，replay 默认优先使用该值；
- 子命令 sanitized failure summary：已记录 step status、return code、stdout/stderr 行数和脱敏错误摘要。

这些元数据不需要阻塞第一版 candidate replay，但会决定 production replay 的可复现上限。

## 阶段拆解

|阶段|状态|范围|验收标准|
|---|---|---|---|
|1. 设计和任务登记|DONE|建立 OPS-007、设计文档和 owner 讨论点|设计文档覆盖 CLI、输入窗口、输出隔离、审计、门禁和阶段拆解。|
|2. Inventory-only|DONE|新增 `aits ops replay-day --inventory-only`，只生成 input freeze manifest 和缺口报告|对 2026-05-08 能识别 5/9、5/10 valuation 为未来输入，识别 PIT manifest 未来 available_time，并给出可冻结视图统计。|
|3. Replay workspace MVP|DONE|生成隔离 workspace 和过滤后的 PIT manifest、valuation snapshot 视图|不修改生产 manifest、valuation 目录或生产日报输出；重复运行生成独立 run id。|
|4. Score replay MVP|DONE|让 `score-daily`、`ops health`、secret scan 在 replay workspace 中运行|2026-05-08 cache-only replay 通过，生产路径 `git status` 不因 replay 输出变化。|
|5. Diff 和批量回放|DONE|支持 production diff、candidate label、窗口批量回放|可对多个历史交易日输出结构化比较结果，用于模型调优。|
|6. Production replay 强化|DONE|daily-run 归档代码/config/rule/input/output hash、production visibility cutoff，并明确 OpenAI replay 缓存策略|可以区分“原生产复现失败”和“当前模型候选回放成功”；replay 不调用 live OpenAI，cache-only 策略只读取历史预审队列和报告。|

## 验收标准

- `aits ops replay-day --as-of 2026-05-08 --mode cache-only` 不调用外部服务。
- 回放输出全部位于 `outputs/replays/<as-of>/<run-id>/` 或等价隔离路径。
- 回放不会改写生产 `features_daily.csv`、`scores_daily.csv`、日报、alerts、decision snapshot、
  evidence trace、prediction ledger、PIT manifest 或 valuation snapshots。
- 5/8 回放能自动排除 5/9、5/10 valuation snapshots，并在 input freeze manifest 中记录排除数量。
- 5/8 回放使用 filtered PIT manifest，`ops health` 不再被 5/10 available_time 干扰。
- 如果 PIT raw payload 缺失或 checksum mismatch，回放 fail closed。
- 如果 valuation/SEC/market/macro/risk/rule card 门禁失败，回放停止并报告技术原因。
- replay report 明确写出这是 candidate replay、production replay 或 incomplete diagnostic。
- 结构化 run log 不包含 API key、token 或付费内容原文。

## 开放问题

- 默认 `visibility cutoff` 已优先来自 production `daily_ops_run_metadata`；缺失时退回 as-of 当日 UTC 末尾。
- 已支持 single-day replay 和 `replay-window`。
- replay 输出保留多久，是否需要自动清理策略？
- OpenAI 风险事件预审在 replay 中默认 `disabled`；显式 `cache-only` 时只读历史缓存。
- production daily-run 已归档 config/rule hash；完整配置文件快照和历史代码 checkout 属于长期治理增强。
- 模型调优使用 candidate replay 时，哪些输出允许进入调参数据集，哪些只作为诊断参考？

## 进展记录

- 2026-05-10：新增设计文档草案。原因：owner 明确历史交易日分析产出回放将成为模型调优重点基础设施；
  当前系统只有生产 `daily-run` 和手工归档回放，不足以支撑高频、安全、可审计回放。
- 2026-05-10：owner 确认按该方向先实现。第一阶段实现边界收敛为单日
  cache-only replay MVP：支持 inventory、隔离 replay bundle、PIT manifest
  与 valuation snapshot 可见窗口冻结、`score-daily`/`ops health`/secret scan
  在 replay 视图中运行；批量窗口回放、production replay 元数据强化和 OpenAI
  replay 缓存策略后续阶段再推进。
- 2026-05-10：单日 cache-only MVP 实现完成。新增 `aits ops replay-day`，
  默认清空 live provider/OpenAI 环境变量，只读生产归档输入，输出隔离到
  `outputs/replays/YYYY-MM-DD/<run-id>/`。真实验证
  `aits ops replay-day --as-of 2026-05-08 --mode cache-only --run-id
  codex_verify_20260510 --label codex_verify` 通过，状态 `PASS`：PIT manifest
  151 行中过滤保留 115 行、排除 36 行未来记录；FMP PIT normalized 4975 行全保留；
  valuation snapshots 61 个中过滤保留 49 个、排除 12 个未来快照；SEC metrics
  66 行；`score_daily`、`ops health`、secret hygiene 均 PASS。验证：
  `ruff check src tests` 通过，`pytest -q` 426 passed。
- 2026-05-10：阶段 5 开始实现。范围限定为 `replay-day --compare-to-production`
  生成 production artifact diff、`replay-window` 按 U.S. equity trading day
  批量运行单日 cache-only replay，并输出窗口级结构化报告。Production daily-run
  元数据归档、历史代码/config/rule 快照和 OpenAI 历史缓存策略仍保留在阶段 6。
- 2026-05-10：阶段 6 开始实现。范围限定为 production `daily-run` 写入脱敏
  metadata sidecar，记录 run id、git/config/rule hash、命令清单、env presence
  和 artifact checksum；不归档 secret 值、stdout/stderr 原文或付费内容原文。
- 2026-05-10：阶段 5 完成、阶段 6 基础版完成。新增 `replay-window`、
  `replay-day --compare-to-production` 和 `daily_ops_run_metadata_YYYY-MM-DD.json`。
  真实验证：`replay-window --start 2026-05-07 --end 2026-05-10 --inventory-only
  --compare-to-production` 通过，运行 5/7、5/8 并跳过 5/9、5/10 周末；
  `replay-day --as-of 2026-05-08 --compare-to-production` 完整回放通过，生成
  `diff_vs_production.md/json`，`score_daily`、`ops health`、secret scan 均 PASS。
  验证：`ruff check src tests` 通过，`pytest -q` 430 passed，`git diff --check`
  仅有 README、system_flow、task_register 的 CRLF/LF 提示。
- 2026-05-10：剩余重点任务完成。`daily_ops_run_metadata_YYYY-MM-DD.json`
  新增 pre-run input artifact checksum、`visibility_cutoff` 和
  `visibility_cutoff_source`；`replay-day` 默认优先读取 production metadata
  cutoff，缺失时退回 as-of 当日 UTC 末尾；新增 `--openai-replay-policy`
  支持 `disabled` 和 `cache-only`，后者只复制历史预审队列和报告，不调用
  live OpenAI。
