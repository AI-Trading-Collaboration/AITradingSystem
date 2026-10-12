# OPS-083：日常运行耗时优化

最后更新：2026-10-12

稳定任务 ID：`OPS-083_DAILY_RUN_PERFORMANCE`

状态：`IN_PROGRESS`（第 1 步：secret hygiene 增量扫描）

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

### 第 1 步：secret hygiene 增量扫描（本次）

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

### 第 2 步：OpenAI 预审有限并发

- `risk_event_prereview.run_openai_risk_event_prereview_for_official_candidates` 改为有限并发（上限可配置，建议 4），按原候选顺序
  汇总 records、issues 与计数；确认请求缓存与审计归档写入线程安全；429/5xx 沿用现有重试。
- 验收：报告与串行版本逐字段一致（顺序、计数、缓存统计）；新增并发与失败路径测试；缓存未命中日 `score_daily` 低于 60 秒。

### 第 3 步：CLI 启动开销

- 先在机器上运行 `.venv\Scripts\python.exe -X importtime -m ai_trading_system.cli_direct --help`，记录总导入时间和前 20 个模块。
- 若导入占每步耗时的大半，再把 `cli_direct.py` 的命令模块导入移到各分发分支；验收为不做事步骤的耗时下降且 `test_cli_direct` 全部通过。

### 第 4 步：market_macro（暂缓）

已经是增量下载（复用规范缓存，只补尾部）。等数据源决定后，先为各 provider 加分项计时，再决定是否并发。

## 4. 进度

- 2026-10-12：完成耗时分析（第 1 节）；第 1 步代码与测试由 pi 运行 `OPS-083-01` 提交，等待验收与 ship。
