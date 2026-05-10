# 系统设计 Review 采纳计划

状态：BASELINE_DONE

最后更新：2026-05-10

关联任务：`OPS-006`、`RUN-001`、`REPORT-008`、`DOC-002`、`ARCH-001`

## 背景

2026-05-10 对系统结构、定期运行、数据管理和报告呈现做了一次静态 review。总体判断是：系统工程纪律和审计链路较强，但复杂度已经上升，下一阶段应优先改善产品导航、运行闭环、run artifact 组织、报告第一屏和文档同步，而不是继续堆叠新指标。

本计划只登记和实现低风险闭环：daily ops runbook、daily-run canonical run bundle、Decision Card v2、文档新鲜度检查，以及 CLI 模块化拆分路线。CLI 大规模移动留到后续单独阶段。

## 采纳结论

|建议|结论|原因|
|---|---|---|
|拆分 `cli.py`|采纳为 `ARCH-001`，本批只做路线登记|`cli.py` 已超过 400KB，长期维护风险真实存在；但直接拆大文件会扩大回归面，应先分阶段迁移低耦合命令组。|
|daily ops runbook|采纳为 `OPS-006`|`daily-plan/daily-run` 已具备基础能力，缺少可交接的运行时刻、失败处理、正式 artifact 和审计附录说明。|
|统一 `run_id` artifact 目录|采纳为 `RUN-001`，采用兼容迁移|owner 选择迁移输出路径；为避免打断 pipeline health、replay 和既有测试，本阶段让 daily-run 生成 canonical `outputs/runs/...` bundle，同时保留 legacy mirror。|
|Decision Card|采纳为 `REPORT-008`|现有 `REPORT-003/REPORT-007` 已有结论卡和 Base Signal / Risk Caps；本次只补 Data Gate、Run ID / Trace、Main Invalidator 和 Next Checks。|
|文档新鲜度检查|采纳为 `DOC-002`|任务登记和需求文档更新频繁，最小自动门禁可防止 `最后更新` 明显落后于状态记录。|
|GitHub Actions 定时跑生产链路|暂不采纳|生产链路依赖本地缓存、provider secrets、付费 API 和运行时数据盘；在云 VM/secret/备份策略明确前，不应把 cron 放进公共 CI。|
|重拆 `data/external`|暂不采纳|当前文档和代码已经按 `trade_theses`、`risk_event_occurrences`、`valuation_snapshots`、`trades`、`portfolio_positions`、`market_evidence` 等领域拆分；大规模路径迁移收益低、风险高。|
|DuckDB / Parquet 迁移|暂不采纳|`STORAGE-001` 已明确暂缓；当前 CSV 更利于本地审计，尚无规模、类型约束或查询性能瓶颈证据。|

## 阶段拆解

|阶段|状态|任务|验收标准|
|---|---|---|---|
|1. 任务登记与需求文档|BASELINE_DONE|新增本文件并更新 `docs/task_register.md`|采纳/不采纳原因、任务 ID、优先级、状态、验收标准齐全。|
|2. Daily ops runbook|BASELINE_DONE|新增 `docs/runbook_daily_ops.md` 并更新 `OPS-003` 说明|能说明盘前、盘后、周/月复核、阻断/降级、正式 artifact、审计附录和 systemd/cron 示例；不新增 GitHub Actions cron。|
|3. Canonical run bundle|BASELINE_DONE|实现 `outputs/runs/YYYY-MM-DD/<run_id>/`|`daily-run` 支持 `--run-output-root`、`--run-id`、`--legacy-output-mode`；manifest 记录输入、输出、checksum、legacy mirror、visibility cutoff。|
|4. Decision Card v2|BASELINE_DONE|扩展现有“今日结论卡”|顶部展示 Data Gate、Run ID / Trace、Main Invalidator 和 Next Checks；不改变正式评分、仓位或 execution policy。|
|5. 文档新鲜度门禁|BASELINE_DONE|新增 `aits docs validate-freshness` 并接入 CI|缺少 `最后更新` 或状态记录日期晚于最后更新时失败；测试覆盖通过和失败样本。|
|6. CLI 分包路线|BASELINE_DONE|登记 `ARCH-001` 分阶段路线|本批不移动命令函数；后续阶段从低耦合 Typer 子命令组迁移，保持 `ai_trading_system.cli:app` 入口兼容。|

## 实施边界

- `data/raw` 与 `data/processed` 继续作为状态缓存，不迁入 `outputs/runs`。
- `outputs/runs` 本阶段只覆盖 `aits ops daily-run` 管理的日报链路；回测和 replay 保持现有目录。
- Legacy mirror 仅用于兼容现有读者、pipeline health 和 replay；后续可在 `RUN-001` 验证稳定后逐步收窄。
- 文档新鲜度检查只验证可确定的日期元数据，不判断任务业务是否真的完成。

## 验证计划

- `python -m ruff check src tests`
- `python -m pytest -q tests/test_ops_daily.py tests/test_daily_scoring.py tests/test_pipeline_health.py tests/test_historical_replay.py`
- `python -m pytest -q tests/test_run_artifacts.py tests/test_docs_freshness.py`
- `python -m ai_trading_system.cli docs validate-freshness`

## 状态记录

- 2026-05-10：新增本计划并登记 `OPS-006`、`RUN-001`、`REPORT-008`、`DOC-002`、`ARCH-001`。本批采纳 low-risk closure，暂不执行 GitHub Actions 生产 cron、`data/external` 路径迁移或 DuckDB/Parquet 存储迁移。
- 2026-05-10：基础实现完成：新增 daily ops runbook、`outputs/runs` canonical run bundle、`score-daily --run-id` trace 串联、Decision Card v2、`aits docs validate-freshness` 和 CI 门禁；`ARCH-001` 仍仅登记路线，CLI 大文件拆分留到下一批。
