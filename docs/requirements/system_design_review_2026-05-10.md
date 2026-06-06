# 系统设计 Review 采纳计划

状态：IN_PROGRESS

最后更新：2026-06-07

关联任务：`OPS-008`、`RUN-001`、`REPORT-008`、`DOC-002`、`ARCH-001`

## 背景

2026-05-10 对系统结构、定期运行、数据管理和报告呈现做了一次静态 review。总体判断是：系统工程纪律和审计链路较强，但复杂度已经上升，下一阶段应优先改善产品导航、运行闭环、run artifact 组织、报告第一屏和文档同步，而不是继续堆叠新指标。

本计划只登记和实现低风险闭环：daily ops runbook、daily-run canonical run bundle、Decision Card v2、文档新鲜度检查，以及 CLI 模块化拆分路线。CLI 大规模移动留到后续单独阶段。

## 采纳结论

|建议|结论|原因|
|---|---|---|
|拆分 `cli.py`|采纳为 `ARCH-001`，本批只做路线登记|`cli.py` 已超过 400KB，长期维护风险真实存在；但直接拆大文件会扩大回归面，应先分阶段迁移低耦合命令组。|
|daily ops runbook|采纳为 `OPS-008`|`daily-plan/daily-run` 已具备基础能力，缺少可交接的运行时刻、失败处理、正式 artifact 和审计附录说明。|
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
|3. Canonical run bundle|BASELINE_DONE|先实现 `outputs/runs/YYYY-MM-DD/<run_id>/` 基础版；后续 `RUN-002` 已升级为 `outputs/runs/daily/<executed_at_utc>/as_of_<YYYY-MM-DD>__<run_id>/`|`daily-run` 支持 `--run-output-root`、`--run-id`、`--legacy-output-mode`；manifest 记录输入、输出、checksum、legacy mirror、visibility cutoff 和执行时间戳。|
|4. Decision Card v2|BASELINE_DONE|扩展现有“今日结论卡”|顶部展示 Data Gate、Run ID / Trace、Main Invalidator 和 Next Checks；不改变正式评分、仓位或 execution policy。|
|5. 文档新鲜度门禁|BASELINE_DONE|新增 `aits docs validate-freshness` 并接入 CI|缺少 `最后更新` 或状态记录日期晚于最后更新时失败；测试覆盖通过和失败样本。|
|6. CLI 分包路线|IN_PROGRESS|登记 `ARCH-001` 分阶段路线，并逐步迁移低耦合 Typer 子命令组|已迁移 `docs`、`security`、`scenarios`、`catalysts`、`execution`、`watchlist`、`industry-chain`、`trace`、`evidence`、`data-sources`、`thesis`、`valuation`、`pit-snapshots`、`llm`、ETF compatibility alias、`signals`、`fundamentals`、`portfolio`、`parameters` 和主系统 `data` 命令组；保持 `ai_trading_system.cli:app` 入口兼容，命令名、参数和退出码不变；后续继续评估其他低耦合命令组。|

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

- 2026-05-10：新增本计划并登记 `OPS-008`、`RUN-001`、`REPORT-008`、`DOC-002`、`ARCH-001`。本批采纳 low-risk closure，暂不执行 GitHub Actions 生产 cron、`data/external` 路径迁移或 DuckDB/Parquet 存储迁移。
- 2026-05-10：基础实现完成：新增 daily ops runbook、`outputs/runs` canonical run bundle、`score-daily --run-id` trace 串联、Decision Card v2、`aits docs validate-freshness` 和 CI 门禁；`ARCH-001` 仍仅登记路线，CLI 大文件拆分留到下一批。
- 2026-05-11：`RUN-002` 将 daily-run canonical run bundle 从 as-of 优先目录升级为执行时间戳优先目录，保留本文件对 `RUN-001` 基础版的历史记录。
- 2026-05-17：`ARCH-001` 第一批低耦合命令组迁移完成，`aits docs` 迁入 `src/ai_trading_system/cli_commands/docs.py`，主入口仍通过 `ai_trading_system.cli:app` 注册，`docs validate-freshness` 和 `docs report-contract` 行为保持兼容。
- 2026-06-07：`ARCH-001` 下一批低耦合命令组迁移完成，`aits security scan-secrets` 迁入 `src/ai_trading_system/cli_commands/security.py`，`cli_direct` 改为调用 `security_cli.security_scan_secrets_command`；命令路径、参数、退出码和 secret hygiene 报告语义保持兼容。
- 2026-06-07：`ARCH-001` 第三批低耦合命令组迁移完成，`aits scenarios validate/lookup` 迁入 `src/ai_trading_system/cli_commands/scenarios.py`；主 CLI 只注册 `scenarios_app`，命令路径、参数、退出码和情景库校验/查询语义保持兼容。
- 2026-06-07：`ARCH-001` 第四批低耦合命令组迁移完成，`aits catalysts validate/upcoming/lookup` 迁入 `src/ai_trading_system/cli_commands/catalysts.py`；主 CLI 只注册 `catalysts_app`，评分路径仍复用底层 catalyst calendar loader/validator，命令路径、参数、退出码和报告/查询语义保持兼容。
- 2026-06-07：`ARCH-001` 第五批低耦合命令组迁移完成，`aits execution validate/lookup` 迁入 `src/ai_trading_system/cli_commands/execution.py`；主 CLI 只注册 `execution_app`，评分路径仍复用底层 execution policy loader/validator/report writer，命令路径、参数、退出码和报告/查询语义保持兼容。
- 2026-06-07：`ARCH-001` 第六批低耦合命令组迁移完成，`aits watchlist list/validate/validate-lifecycle` 迁入 `src/ai_trading_system/cli_commands/watchlist.py`，`aits industry-chain list/validate` 迁入 `src/ai_trading_system/cli_commands/industry_chain.py`；主 CLI 只注册对应 Typer app，评分、thesis、risk、valuation 和回测路径仍复用底层 config loader/validator，命令路径、参数、退出码和报告语义保持兼容。
- 2026-06-07：`ARCH-001` 第七批低耦合命令组迁移完成，`aits trace lookup` 迁入 `src/ai_trading_system/cli_commands/trace.py`，`aits evidence validate/import-csv` 迁入 `src/ai_trading_system/cli_commands/evidence.py`；主 CLI 只注册对应 Typer app，日报/回测 trace bundle 生成和 dashboard 仍复用底层 trace/evidence builder，不改变报告审计语义。
- 2026-06-07：`ARCH-001` 第八批低耦合命令组迁移完成，`aits data-sources list/validate/health/yahoo-price-diagnostic` 迁入 `src/ai_trading_system/cli_commands/data_sources.py`；主 CLI 只注册 `data_sources_app`，其他评分/报告命令仍复用底层 data source loader、quality gate 和价格诊断 helper，命令路径、参数、退出码和报告语义保持兼容。
- 2026-06-07：`ARCH-001` 第九批低耦合命令组迁移完成，`aits thesis list/validate/review` 迁入 `src/ai_trading_system/cli_commands/thesis.py`；主 CLI 只注册 `thesis_app`，日报人工复核摘要仍复用底层 thesis loader/validator/review builder，不改变 thesis gate 或报告语义。
- 2026-06-07：`ARCH-001` 第十批低耦合命令组迁移完成，`aits valuation fetch-fmp/fetch-fmp-valuation-history/fetch-eodhd-trends/validate-fmp-history/import-csv/list/validate/review` 迁入 `src/ai_trading_system/cli_commands/valuation.py`；主 CLI 只注册 `valuation_app`，score-daily、回测和 PIT manifest 仍复用底层估值 loader/validator/review builder 与 raw cache 默认目录，命令路径、参数、退出码和报告语义保持兼容。
- 2026-06-07：`ARCH-001` 第十一批低耦合命令组迁移完成，`aits pit-snapshots validate/build-manifest/fetch-fmp-forward` 迁入 `src/ai_trading_system/cli_commands/pit_snapshots.py`；主 CLI 只注册 `pit_snapshots_app`，daily-run direct dispatcher 改为调用 `pit_snapshots_cli`，并同步修复上一批估值迁移后 `valuation fetch-fmp` direct dispatcher 仍指向主 CLI 的漂移；PIT manifest/raw cache、`--continue-on-failure`、命令路径、参数、退出码和报告语义保持兼容。
- 2026-06-07：`ARCH-001` 第十二批低耦合命令组迁移完成，`aits llm precheck-claims` 迁入 `src/ai_trading_system/cli_commands/llm.py`；主 CLI 只注册 `llm_app`，共享 LLM request profile helper、风险事件 OpenAI 预审、LLM formal assessment 和 `score-daily` 语义未修改；命令路径、参数、退出码、provider LLM permission fail-closed、request cache、中文报告和待复核队列语义保持兼容。
- 2026-06-07：`ARCH-001` 第十三批低耦合命令组迁移完成，ETF 根级 compatibility alias 注册迁入 `src/ai_trading_system/cli_commands/etf_compat.py`；`features/regime/simulation/report/run/experiments` standalone alias app 从新模块导入，`data ingest/validate`、`signals generate` 和 `portfolio allocate` 仍注册到主 CLI 既有 app；ETF 子命令实现、根级主系统 `backtest` 和投资解释语义未修改。
- 2026-06-07：`ARCH-001` 第十四批低耦合命令组迁移完成，`aits signals build-snapshot/validate-snapshot/ablation/calibrate/explain-ablation/validate-ablation` 迁入 `src/ai_trading_system/cli_commands/signals.py`；主 CLI 只注册 `signals_app` 并继续把 ETF compatibility alias `signals generate` 注册到同一 app，daily-run direct dispatcher 改为调用 `signals_cli`；signal snapshot、ablation、calibration artifact、质量门禁和 `production_effect=none` 语义保持兼容。
- 2026-06-07：`ARCH-001` 第十五批低耦合命令组迁移完成，`aits fundamentals list-sec-companies/download-sec-companyfacts/download-sec-submissions/download-sec-filing-archive/sec-accession-coverage/validate-sec-companyfacts/extract-sec-metrics/validate-sec-metrics/build-sec-features/extract-tsm-ir-pdf-text/import-tsm-ir-quarterly/import-tsm-ir-quarterly-batch/fetch-tsm-ir-quarterly/merge-tsm-ir-sec-metrics` 迁入 `src/ai_trading_system/cli_commands/fundamentals.py`；主 CLI 只注册 `fundamentals_app`，daily-run direct dispatcher 改为调用 `fundamentals_cli`；SEC/TSMC 数据源审计、质量门禁、PIT/日报 helper 和评分/回测语义保持兼容。
- 2026-06-07：`ARCH-001` 第十六批低耦合命令组迁移完成，`aits portfolio exposure/sensitivity/validate-sensitivity/candidates/validate-candidates/explain-turnover/validate-turnover-attribution/review-candidate/decide-candidate/validate-review/track-candidate/validate-tracking/tracking-status/review-tracking/tracking-window-status/validate-tracking-review` 迁入 `src/ai_trading_system/cli_commands/portfolio.py`；portfolio artifact resolver 抽入 `src/ai_trading_system/cli_commands/portfolio_artifacts.py`，供 portfolio 命令和既有 `aits reports portfolio-*` alias 共用；主 CLI 继续注册同一个 `portfolio_app` 并保留 ETF compatibility alias `portfolio allocate`；portfolio artifact、tracking state、review report、日报/评分和投资解释语义保持兼容。
- 2026-06-07：`ARCH-001` 第十七批低耦合命令组迁移完成，`aits parameters shadow-backtest/validate-shadow-backtest/tune-weights/validate-weight-tuning/tune-weights-stable/diagnose-weight-stability-inputs/recover-weight-stability-inputs/validate-weight-stability-readiness/validate-weight-stability/explain-weight-stability/explain-weight-tuning/explain-weight-tuning-failure/validate-weight-tuning-failure` 迁入 `src/ai_trading_system/cli_commands/parameters.py`；shadow/weight artifact resolver 抽入 `src/ai_trading_system/cli_commands/parameter_artifacts.py`，供参数命令和既有 `aits reports weight-*` / `shadow-parameter-backtest` alias 共用；主 CLI 只注册 `parameters_app`，daily-run direct dispatcher 改为调用 `parameters_cli`；shadow backtest、weight tuning、stability、readiness、failure attribution、promotion、评分、回测和日报投资解释语义保持兼容。
- 2026-06-07：`ARCH-001` 第十八批低耦合命令组迁移完成，主系统 `aits data diagnose-backtest-inputs/inspect-registry/validate-backtest-manifest/reconcile-price-cache/refresh-backtest-manifest/freshness/validate-freshness/refresh-market/validate-refresh/recover-freshness/repair-backtest-inputs` 迁入 `src/ai_trading_system/cli_commands/data.py`；market data freshness/refresh artifact resolver 抽入 `src/ai_trading_system/cli_commands/data_artifacts.py`，供 data 命令和既有 `aits reports data-*` alias 共用；root 级 `download-data` / `validate-data` 和 ETF compatibility alias `data ingest/validate` 保持不动；数据质量门禁、freshness/refresh side effect、评分、回测和日报投资解释语义保持兼容。
