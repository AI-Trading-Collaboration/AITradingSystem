# RUN-002 每日运行输出归档路径

状态：VALIDATING

最后更新：2026-05-11

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
