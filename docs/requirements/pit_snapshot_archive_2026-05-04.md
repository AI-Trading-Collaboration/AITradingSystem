# 自建 forward-only PIT 快照归档方案

状态：BASELINE_DONE

最后更新：2026-05-05

关联任务：`DATA-003`、`BACKTEST-002`、`DATA-001`、`BTINPUT-001`、`VALUATION-002`、`DATA-002`、`SECURITY-001`、`STORAGE-001`

## 背景

机构级 historical PIT estimates archive 成本高，且个人系统很难低成本还原过去某个交易日市场实际可见的 analyst consensus、price target、rating revision 和预期修正。项目 owner 2026-05-04 决策：不为了补齐历史 PIT estimates archive 硬买昂贵数据，也不把供应商回填历史接口伪装成历史可见输入；改为从当前日期开始建设严格留痕的 forward-only as-of 快照层。

该决策不推翻 `DATA-001` 的低成本估值 baseline。`DATA-001` 继续负责 EODHD/FMP 当前可见估值和预期指标；本文件新增的是通用快照归档、可见时间约束、回测数据可信度标签和滞后敏感性治理。

## 非目标

- 不采购或承诺补齐 FactSet、I/B/E/S、Bloomberg 等机构级 historical PIT estimates archive。
- 不把 FMP、EODHD 或其他供应商的 historical endpoint 自动提升为严格 `true_point_in_time`。
- 不用采集日回填数据改写 2026-05-04 以前的历史回测结论。
- 不把 `vendor_date`、`period`、`updatedAt` 或接口返回日期直接等同于本系统 `available_time`。
- 不在报告、manifest、trace bundle 或日志中输出 API key、cookie、token 或付费内容原文。

## DATA-003

标题：forward-only PIT 快照归档层

价值判断：应列为 P1。它不会修复旧历史回测的 estimates 缺口，但能保证从 2026-05-04 起的每一次供应商输入都有可审计来源、请求参数、原始响应、checksum、parser version 和可见时间。自然积累 3 个月、12 个月后，系统就有真实自建 as-of 样本，可用于 shadow live、forward test 和后续更可信的规则校准。

### 输入范围

第一阶段聚焦美股 AI 观察池，优先覆盖：

- analyst estimates；
- price target；
- rating changes；
- earnings calendar；
- company profile；
- index constituents；
- 估值和拥挤度相关供应商快照；
- SEC filing/submissions/companyfacts 的原始或 accession-level 审计引用。

A 股公告、巨潮、交易所公告、新闻标题和更广泛宏观/产业数据可作为后续扩展，不阻塞第一阶段。

### 原始快照要求

每次抓取必须生成 raw snapshot manifest。第一版可以继续使用本地文件加 JSON/CSV/YAML manifest，不要求立即引入 PostgreSQL 或 DuckDB；如果 schema 演进或查询性能成为瓶颈，再由 `STORAGE-001` 评估升级。

manifest 至少记录：

- `snapshot_id`；
- `source_name`、`source_type`、`source_quality_tier`；
- `endpoint`、`request_params`、`provider_symbol`、`canonical_ticker`、`provider_symbol_alias`；
- `http_status`、`content_type`、可安全保存的 response headers；
- `raw_payload_path`、`raw_payload_sha256`、`raw_payload_bytes`；
- `snapshot_time`、`ingested_at`、`vendor_timestamp`；
- `row_count`、`parser_version`、`schema_version`；
- `license_use_class`、`redistribution_allowed`、`llm_processing_allowed`；
- `point_in_time_class`、`history_source_class`、`backtest_use`、`confidence_level`、`confidence_reason`；
- `validation_status`、`validation_report_path`。

### 标准化输出要求

第一阶段标准化 estimates 时至少保留：

- `snapshot_id`；
- `source_name`；
- `symbol`；
- `metric`；
- `fiscal_period`；
- `estimate_value`、`consensus_mean`、`consensus_high`、`consensus_low`、`analyst_count`；
- `vendor_date`、`vendor_timestamp`；
- `available_time`；
- `normalization_version`；
- `raw_snapshot_ref`。

`available_time` 的默认定义是本系统成功采集、校验并写入快照的时间。只有当供应商明确提供可审计 as-of archive，且校验能证明该记录在更早时间已可见时，才允许早于本系统 `ingested_at`。

### 查询规则

任何评分、回测、校准和报告只能通过 as-of 查询读取标准化快照：

```sql
WHERE available_time <= decision_time
```

如果没有满足条件的记录，系统必须返回缺口或降级，不得使用最近未来快照、当前供应商视图或回填历史分布填补。

### 数据质量门禁

第一阶段应新增或扩展校验命令，覆盖：

- manifest 必填字段；
- raw payload 文件存在性和 sha256；
- row count 与标准化输出一致性；
- provider symbol alias；
- 重复快照和重复 normalized key；
- `available_time <= ingested_at` 或明确例外证明；
- low-confidence 数据不得声明 `strict_point_in_time`；
- provider 授权、再分发和外部 LLM 处理权限；
- API key 不落盘、不进报告。

严重校验失败必须阻止下游评分、回测或报告使用该快照。

### 阶段拆解

|阶段|目标|验收|
|---|---|---|
|1|建立通用 raw snapshot manifest、校验命令和报告|能校验现有 FMP/EODHD 原始缓存或新目录，输出中文质量报告和 checksum 问题|
|2|接入 FMP analyst estimates/price target/ratings/earnings calendar 的 forward-only 归档|每日抓取写入 raw 和 normalized，`available_time` 从采集成功时间开始|
|3|把标准化 estimates 接入 valuation/revision 计算，但只允许采集日后可见|`eps_revision_90d_pct` 在自建历史不足 90 天时明确降级，不伪造旧历史|
|4|增加日常调度和 pipeline health 检查|缺跑、断更、row count 异常和 checksum 变化进入 data/system alert|
|5|评估 SEC accession-level filing raw archive 增强|companyfacts 聚合结果可追溯到 filing accession、accepted time 和原始 filing|

### 实施 TODO

|顺序|任务|状态|验收标准|
|---:|---|---|---|
|1|新增通用 PIT raw snapshot 数据模型与 manifest schema|DONE|定义 `snapshot_id`、provider、endpoint、request params、raw payload、checksum、row count、`ingested_at`、`available_time`、PIT 可信度和授权字段；schema 字段有单元测试|
|2|实现 raw snapshot manifest 读取、写入和校验 helper|DONE|校验必填字段、日期约束、payload 文件存在性、sha256、row count、重复 `snapshot_id` 和低可信数据误标 strict PIT|
|3|新增 `aits pit-snapshots validate` 命令和中文质量报告|DONE|默认读取 `data/raw/pit_snapshots/manifest.csv` 或显式路径；输出状态、错误/警告、provider 摘要、payload 校验问题和下游使用边界；严重错误返回非零退出码|
|4|把现有 FMP/EODHD raw cache 纳入阶段 1 校验入口|DONE|能对已存在 `data/raw/fmp_analyst_estimates/`、`data/raw/fmp_historical_valuation/`、`data/raw/eodhd_earnings_trends/` 生成或校验 manifest；不改变当前评分语义|
|5|更新系统流图、数据源目录和 README 日常流程|DONE|文档明确 PIT capture 位于 `score-daily` 之前，缺跑不能事后补成 strict PIT，报告默认中文|
|6|补充测试和最小样例数据|DONE|覆盖 manifest 通过、checksum mismatch、未来 `available_time`、重复快照、missing payload 和 CLI 退出码|
|7|阶段 2 接入 FMP forward-only 抓取归档|DONE|`analyst estimates`、`price target`、`ratings`、`earnings calendar` 写 raw payload 和 normalized 输出，`available_time` 从采集成功时间开始|
|8|阶段 3 接入 valuation/revision as-of 查询|PENDING|`eps_revision_90d_pct` 只使用 `available_time <= decision_time` 的自建快照；样本不足 90 天时明确降级|
|9|阶段 4 接入日常健康检查和告警|PENDING|`ops health` / alerts 检查 PIT 快照缺跑、断更、row count 异常和 checksum 异常|
|10|阶段 5 评估 SEC accession-level 增强|PENDING|形成是否接入 accession-level filing archive 的设计结论或后续任务|

## BACKTEST-002

标题：回测数据可信度标签与滞后敏感性检查

价值判断：应列为 P1。`DATA-003` 只能让未来数据变得可审计；历史回测仍需要明确哪些结论是 A 级 PIT、B 级保守近似、C 级探索性。否则报告会把真实 PIT 输入和回填历史视图混在一起，造成投资解释风险。

### 数据可信度等级

回测和策略报告应输出：

- A 级：完全 PIT 或自建 raw snapshot 可证明当时可见；
- B 级：非完整 PIT，但有公告时间、采集时间或保守滞后，并通过 lag sensitivity；
- C 级：当前口径历史、供应商历史 endpoint 无可审计 `available_time`、或只能作为研究线索。

报告必须列出：

- `Backtest Data Quality`；
- `Uses Vendor Historical Estimates`；
- `Uses Self-Archived Snapshots`；
- `Minimum Feature Lag`；
- `Universe PIT`；
- `Corporate Actions Handling`；
- 每类核心输入的 `point_in_time_class`、`backtest_use` 和覆盖率。

### 滞后敏感性

新增 lag sensitivity 实验，至少支持：

- feature lag：0、1、3、5、10、20 个交易日；
- universe lag：0、1、3、5、10、20 个交易日；
- rebalance delay：默认 1 个交易日。

如果策略只在 `lag=0` 有效，报告必须把该结果标记为高未来函数风险；如果加 3 到 5 个交易日滞后仍有效，才允许作为更可信研究线索。

### 验收标准

- `aits backtest` 或独立 CLI 能生成中文 lag sensitivity 报告和机器可读摘要。
- 报告声明 `ai_after_chatgpt` regime 和实际日期范围。
- C 级输入不得输出无条件 Sharpe 结论，必须标注探索性。
- lag sensitivity 使用同一交易成本、同一 universe PIT 规则和同一数据质量门禁。
- `backtest_audit_*.md` 能解释哪些模块因缺少 PIT estimates 被降级。

## Owner 依赖

- 确认第一阶段供应商和 endpoint 清单。
- 确认供应商条款是否允许本地缓存、个人研究使用、再分发和外部 LLM 处理。
- 确认每日抓取时间，建议美股收盘后或次日固定时间。
- 确认 raw payload 保留周期和本地备份策略。

## 状态记录

- 2026-05-04：新增本方案。结论是暂不采购昂贵 historical PIT estimates archive，先建设 forward-only 自建快照层，并用数据可信度标签和 lag sensitivity 约束历史回测解释。
- 2026-05-05：从 READY 改为 IN_PROGRESS，原因：owner 确认优先开发 `DATA-003`，先把 PIT 快照归档纳入日常执行前置链路；本次先实施阶段 1 的通用 raw snapshot manifest、校验命令、中文质量报告、文档和测试。
- 2026-05-05：从 IN_PROGRESS 改为 BASELINE_DONE，原因：阶段 1 已实现 `pit_snapshots` manifest schema/校验、`aits pit-snapshots validate`、`aits pit-snapshots build-manifest`、现有 FMP/EODHD raw cache 发现入口、中文质量报告、数据源目录、系统流图、README 和测试；真实本地运行 `aits pit-snapshots build-manifest --as-of 2026-05-05` 生成 13 条快照且质量状态为 PASS。阶段 2-5 继续保留为后续开发。
- 2026-05-05：从 BASELINE_DONE 改回 IN_PROGRESS，原因：继续推进阶段 2，接入 FMP analyst estimates、price target、ratings 和 earnings calendar 的 forward-only PIT 抓取归档。
- 2026-05-05：从 IN_PROGRESS 改为 BASELINE_DONE，原因：阶段 2 已实现 `aits pit-snapshots fetch-fmp-forward`、`data/raw/fmp_forward_pit/` 原始归档、`data/processed/pit_snapshots/fmp_forward_pit_YYYY-MM-DD.csv` 标准化 as-of 索引、自动刷新 PIT manifest、中文抓取报告、数据源目录、系统流图、README 和测试；全量 pytest 与 Ruff 通过。阶段 3-5 继续保留为后续开发。
