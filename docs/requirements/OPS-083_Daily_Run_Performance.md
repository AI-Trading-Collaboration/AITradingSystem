# OPS-083：日常运行耗时优化

最后更新：2026-10-13

稳定任务 ID：`OPS-083_DAILY_RUN_PERFORMANCE`

状态：`IN_PROGRESS`（第 1 步已发布；第 2 步：OpenAI 预审有限并发）

任务记录：`tasks/OPS-083_DAILY_RUN_PERFORMANCE.yaml`。运行副本与计划任务见 `docs/operations/operations_runbook.md` 第 9 节。

## 1. 问题

运行副本 `D:\Work\AITradingSystem_ops_runtime` 的每次 daily-run 都会在
`outputs/runs/daily/<bundle>/<run>/metadata/daily_ops_run_metadata_<as_of>.json` 的 `step_results[].duration_seconds`
记下每一步耗时；capture 组件的耗时在 `outputs/daily_input_capture/<as_of>/daily_input_capture_manifest_<as_of>.json`。
run ledger 里每一步记的都是整次运行的起止时间，不能用来看分步耗时。

2026-10-12 只读分析（JST）：

- 2026-10-11 12:17 的运行（as_of 2026-10-09，27 步串行）共 915 秒：`secret_hygiene` 399 秒（44%），
  `capture_daily_inputs` 165 秒（其中 market_macro 99 秒），`score_daily` 161 秒，其余 24 步约 189 秒，大多数每步约 5 秒。
- 2026-07-29 到 2026-09-05 的 24 次 PASS 运行：中位 995 秒（915–1445 秒）；secret_hygiene 中位 319 秒，score_daily 189 秒，
  capture 156 秒。
- `secret_hygiene` 每次全量读取 config、docs、整个 outputs 和 download manifest：5 月约 3.1 千个文件、10 秒；2026-10-11 是
  41,041 个文件、399 秒（约 10 毫秒/文件）。扫描量至少 10 GB，主要是研究产物（`outputs/research_trends` 约 4.6 GB，单个 JSON
  最大约 450 MB；`outputs/reports` 1.6 GB；`outputs/runs` 1.5 GB；`outputs/replays` 1.5 万个文件），它们并不每天变化。
  2026-10-11 03:00 UTC 以后（含运行副本更新和一次完整运行）新增或改动的可扫描文件约 216 个、74 MB。
- `score_daily` 逐个候选串行调用 10 次 OpenAI（gpt-5.5、medium），每次约 15 秒；缓存全命中时这一步约 17 秒。正常逐日运行
  每天都是 MISS=10、EXPIRED=0，请求内容每天不同，延长缓存 TTL 没有作用。
- 不做事的步骤的最低耗时随代码量变化：5 月 2.6 秒，8 月 8–9 秒，GOV-008 删除大量代码后 5 秒。每一步都是
  `python -m ai_trading_system.cli_direct` 子进程，`cli_direct.py` 在顶部导入全部命令模块；判断是导入开销（推断，未直接测量）。

## 2. Owner 决定

**`owner_decision:OPS-083:2026-10-12:daily_run_perf_plan`**

2026-10-12 09:44 JST，owner（Baker）在项目线程看过上述分析和方案后回复"好的，按照这个思路继续推进优化把"。按以下顺序推进：

1. secret hygiene 改成增量扫描，定期全量兜底；
2. OpenAI 风险事件预审改成有限并发；
3. 先在机器上测量 CLI 导入耗时，再决定是否改为按需导入；
4. market_macro 暂缓，等数据源决定（`owner_decision:GOV-008:2026-10-11:research_after_migration`）。

不改步骤列表、门禁语义、报告结论、模型或 reasoning effort。全量兜底周期 7 天是实施方选的默认值（见第 3 节），可以随时调整。

## 3. 步骤计划与验收

### 第 1 步：secret hygiene 增量扫描（已发布）

- `scan_secrets` 增加可选缓存：路径 → (size, mtime_ns)。只沿用上次无发现、且大小和修改时间都没变的文件的结果；有发现的文件
  不进缓存，每次重读。读文件前先 stat，读的过程中文件变了下次也会重读。
- 以下情况自动全量并重建缓存：缓存不存在、无法读取或格式不符；`secret_hygiene.py` 内容变化（规则指纹）；扫描入口（解析后的
  绝对路径）变化；上次全量扫描满 7 天（`SECRET_SCAN_FULL_RESCAN_INTERVAL`，运维常量，不影响投资解释）；上次全量时间晚于当前时间；
  `--full-scan`。
- CLI：默认扫描入口使用 `data/cache/secret_hygiene/scan_cache.json`（`data/cache/` 不进 git，也不在扫描入口内）；
  自定义 `--scan-paths`（例如历史回放）不用缓存，行为与以前相同，除非显式给 `--cache-path`。daily 步骤命令不变，规格 id 不变。
- 报告新增三行：扫描方式（增量/全量及原因）、本次读取文件数与沿用上次结果数、上次全量扫描时间。"扫描文件数"仍是覆盖的文件总数。
- 需要立即全量时，在运行副本根目录运行 `.venv\Scripts\aits.exe security scan-secrets --full-scan`（写当天日期的 secret hygiene 报告，并重建缓存）。
- 已知限制：内容改了但大小和修改时间都没变的文件，要等下一次全量扫描（最多 7 天）才会被读到；测试覆盖了这一点。
- 验收：PR 套件通过；ship 后按运维手册 9.1 更新运行副本；第一次真实运行报告为"全量（缓存不存在）"，之后的运行为"增量"，
  `secret_hygiene` 耗时低于 60 秒（预计 15–30 秒）；PASS/FAIL 语义不变。

### 第 2 步：OpenAI 预审有限并发（本次）

- `run_openai_risk_event_prereview_for_official_candidates` 新增 `max_concurrency`（默认 1，即原来的串行）。大于 1 时用线程池，
  同时在途的请求不超过上限，结果按候选优先级顺序汇总 records、issues、checksum 与各项计数，所以成功时报告和串行逐字段一致。
- 上限来自 LLM request profile 的 `max_concurrency`（1–8，默认 1）。`risk_event_daily_official_precheck` 设为 4，其他 profile 不变。
  `score-daily` 和 `risk-events precheck-triaged-official-candidates` 都读 profile。
- 出错时：串行和以前一样在第一个错误处停。并发时第一个错误出现后不再发新请求，已在途的请求（最多 3 个）照常完成，并计入报告
  的请求数和问题列表，报告照实反映实际发出的请求；出错时队列仍为空，整批 fail closed 不变。
- 请求缓存和审计归档按请求各写各的文件；`write_json_atomically` 的临时文件名加上进程和线程号，同一缓存键被两个线程同时写时
  不会互相覆盖临时文件。429/5xx 沿用现有重试。
- 不改：候选筛选与排序、请求内容、模型、reasoning effort、候选上限、缓存 TTL、报告格式和队列。
- 验收：测试覆盖并发报告与串行一致（完成顺序打乱、带缓存写入与回读）、出错后不再发新请求、上限校验、profile 取值和
  `score-daily` 传参；PR 套件通过；缓存未命中日 `score_daily` 低于 100 秒。
- 预计：之前 160–240 秒，其中 OpenAI 等待约 140–220 秒（10 个请求，每个 15–22 秒），其余约 17 秒。上限 4 时 10 个请求分 3 批，
  预计 70–90 秒。原计划写的"低于 60 秒"要分 2 批才能做到（上限 5 或以上），改上限只需改 profile 的一个值，按第一次真实耗时再定。

### 第 3 步：CLI 启动开销

- 先在机器上运行 `.venv\Scripts\python.exe -X importtime -m ai_trading_system.cli_direct --help`，记录总导入时间和前 20 个模块。
- 若导入占每步耗时的大半，再把 `cli_direct.py` 的命令模块导入移到各分发分支；验收为不做事步骤的耗时下降且 `test_cli_direct` 全部通过。

### 第 4 步：market_macro（暂缓）

已经是增量下载（复用规范缓存，只补尾部）。等数据源决定后，先为各 provider 加分项计时，再决定是否并发。

## 4. 进度

- 2026-10-12：完成耗时分析（第 1 节）；第 1 步代码与测试由 pi 运行 `OPS-083-01` 提交，等待验收与 ship。
- 2026-10-13：第 1 步已于 2026-10-12 发布（main `5802317d3`），运行副本 10-12 13:30 JST 随 DATA-022 更新。10-12 17:30 JST 的
  日报第一次运行报告为"全量（缓存不存在）"，41,139 个文件 347.5 秒，缓存已写；第一次增量在下一次日报。同一次日报的 score_daily
  因 DATA-022 遗漏的收据角色失败（DATA-022-02 修复），所以第 2 步的耗时基线仍用 2026-10-11 之前的数据。第 2 步代码与测试由 pi
  运行 `OPS-083-02` 提交。
