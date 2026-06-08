# RUN-002 每日运行输出归档路径

状态：VALIDATING

最后更新：2026-06-09

关联任务：`RUN-002`

## 背景

当前 `daily-run` 已有 canonical run bundle，但路径以评估日期为第一层：

```text
outputs/runs/<as_of>/<safe_run_id>/
```

这个结构能把同一评估日的多次运行聚在一起，但对真实运维复盘不够直观：

- 同一执行日可能跑当前生产日、补跑历史日、休市日巡检或失败重跑；
- 以 `as_of` 为第一层时，执行顺序需要再从 `run_id` 或 metadata 中反查；
- dashboard、health、secret scan、run manifest 等本轮输出应按执行时间形成统一归档入口；
- `data/raw` 和 `data/processed` 是状态缓存，不应被误解为每次运行的完整归档副本。

## 决策

每日运行 canonical bundle 改为执行 UTC 时间戳优先：

```text
outputs/runs/daily/<executed_at_utc>/
  as_of_<YYYY-MM-DD>__<safe_run_id>/
    manifest.json
    reports/
    traces/
    metadata/
```

其中：

- `<executed_at_utc>` 使用 `YYYYMMDDTHHMMSSZ`；
- `as_of_<YYYY-MM-DD>` 明确评估日期，避免把执行日期误写成市场结论日期；
- `<safe_run_id>` 保留人工 run id 或默认 run id，便于定位重跑目的；
- `outputs/reports/` 继续作为 legacy mirror 过渡路径；
- `outputs/replays/` 继续保持 replay 隔离结构，不在本任务中迁移；
- `data/raw/` 和 `data/processed/` 继续作为可校验状态缓存，run bundle 只归档报告、trace、metadata、manifest 和 checksum 引用。

## 验收标准

- `aits ops daily-run` 默认写入执行时间戳优先的 canonical run bundle。
- manifest 记录 `execution_timestamp_utc`、`run_root`、`as_of`、输入和输出 artifact checksum。
- legacy mirror 仍保留，方便短期使用旧 `outputs/reports` 入口。
- 同一 as-of 的多次运行能按执行时间自然排序。
- 休市日 skipped dashboard 不会从 legacy 路径带入本轮 run bundle。
- 文档和系统流图说明 `data/raw`、`data/processed` 与 run bundle 的职责边界。

## 进展记录

- 2026-05-11：新增并进入实现。owner 建议每日输出产物按执行时间戳整理到统一路径，并进一步明确需要归档记录的数据管理方式。
- 2026-05-11：实现进入 `VALIDATING`。`daily-run` canonical bundle 已改为执行时间戳优先路径，manifest 增加 `execution_timestamp_utc`，runbook 和 system flow 已同步说明状态缓存与运行归档的职责边界；下一步观察真实 daily-run 产物是否符合新目录规范。
- 2026-06-07：从 `VALIDATING` 回到 `IN_PROGRESS`。最新真实
  `aits ops daily-run --as-of 2026-06-05 --run-id codex_20260605_20260607103901`
  已写入 `outputs/runs/daily/20260607T013907Z/as_of_2026-06-05__codex_20260605_20260607103901/`
  canonical bundle，manifest 记录 `execution_timestamp_utc`、`as_of`、`run_root`、
  输入/输出 checksum 和 legacy mirror；但 manifest 中 `sec_pit_shadow_observe` /
  `sec_pit_shadow_monitor` 声明的 2026-06-05 输出为 `exists=false`，而执行报告对应
  steps 为 PASS。实际最新 SEC PIT shadow artifact 仍为 2026-05-26，说明 daily-run
  的 `sec-pit shadow-* --latest` 未绑定本轮 `as_of`。归档契约新增要求：PASS step 的
  声明输出必须真实存在，或在报告/manifest 中显式降级，不得让本轮 bundle 留下
  PASS 但输出缺失的记录。
- 2026-06-07：从 `IN_PROGRESS` 回到 `VALIDATING`。`daily-plan` 和
  `config/scheduled_tasks.yaml` 现在对 SEC PIT shadow daily steps 显式传入
  `--end {as_of}` / `--as-of {as_of}`；`cli_direct` 不再丢弃这些参数；`shadow-monitor`
  在显式 `as_of` 模式下要求同日 `shadow-observe` summary，不能回退到旧 observe
  artifact。验证通过 focused pytest、`compileall`、`validate-data --as-of 2026-06-05`、
  真实 `sec-pit shadow-observe --latest --end 2026-06-05` 和
  `sec-pit shadow-monitor --latest --as-of 2026-06-05`；2026-06-05 observe/monitor
  summary JSON/Markdown 均已真实落盘。下一步仍需观察一次完整 `daily-run` manifest，
  确认 PASS step 的 `produced_paths` 不再出现 `exists=false`。
- 2026-06-09：从 `VALIDATING` 回到 `IN_PROGRESS`。默认完整 daily-run
  `aits ops daily-run --run-id ops-011-default-smoke-20260609` 已证明 SEC PIT
  shadow observe/monitor 同日输出存在，但 run manifest 和 metadata 仍把
  `outputs/reports/download_data_diagnostics_2026-06-05.md` 记录为
  `produced_artifacts` / legacy output 且 `exists=false`。该文件只应在
  `download_data` 失败时生成；本轮 `download_data` 为 PASS，`diagnostic_path=null`。
  下一步修正 daily-run 计划/metadata 语义，把失败专用诊断路径和正常实际产物路径分离，
  避免 PASS run 的实际产物清单继续出现失败诊断缺失项。
- 2026-06-09：从 `IN_PROGRESS` 回到 `VALIDATING`。`DailyOpsStep` 新增
  `failure_diagnostic_paths`，`download_data_diagnostics_YYYY-MM-DD.md` 保留在
  daily plan 的失败诊断说明中，但成功步骤不再把该失败专用路径写入
  metadata/manifest produced artifacts；若 `download_data` 失败且该诊断文件真实生成，
  metadata 仍会记录其 checksum。验证通过 `tests/test_ops_daily.py`
  / `tests/test_run_artifacts.py` focused tests、Ruff、Black check 和 `compileall`。
  下一步观察一次完整真实 `daily-run` manifest，确认 PASS run 不再出现失败诊断缺失项。
