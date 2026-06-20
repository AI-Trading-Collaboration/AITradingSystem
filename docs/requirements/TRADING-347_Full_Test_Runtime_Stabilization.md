# TRADING-347 Full Test Runtime Stabilization

最后更新：2026-06-20

## 1. 背景

TRADING-336～346 的最新链路中，focused tests、documentation contract、report index
和 Reader Brief 验证可作为局部证据，但 full pytest 曾在 604 秒后超时，不能写成
passing evidence。TRADING-174 已建立 validation tier runner 和 16 worker 默认并行
入口；本任务在此基础上把测试分层升级为 promotion-facing contract：明确哪些 suite
阻断 promotion，哪些 suite 是慢速研究回归，哪些 suite 可用于快速反馈，并把每次运行的
runtime / coverage / safety 边界写成可审计 artifact。

## 2. 目标

1. 把 pytest 验证拆成 `fast-unit`、`contract-validation`、`report-validation`、
   `integration` 和 `slow-research-regression` 五类正式 suite。
2. 保留旧 tier alias，避免破坏既有 TRADING-174 使用路径。
3. 为每次 tier run 生成 `test_runtime_summary.json` 和
   `test_runtime_reader_brief.md`，披露命令、worker、runtime、status、promotion
   blocking 规则和安全边界。
4. 文档化 CI-friendly 命令、promotion-blocking suite 和 slow suite 隔离要求。
5. 保持 focused tests 快速反馈，不改变 strategy logic、production state、broker action、
   cached market data 或投资解释路径。

## 3. 非目标

- 不修改任何 trading strategy、score、backtest、report conclusion、target weights、
  paper/real portfolio、broker 或 production state。
- 不把 slow research regression 默认隐藏为 PASS。
- 不以 runtime artifact 替代 data quality gate；涉及 cached data 的业务命令仍必须执行
  `aits validate-data` 或同等门禁。
- 不要求本轮完整跑完 slow research regression；它必须被显式隔离和可调用。

## 4. Suite Contract

|Suite|用途|Promotion Blocking|Slow Allowed|
|---|---|---|---|
|`fast-unit`|CLI wiring、核心纯函数、文档/report registry 轻量反馈|是，对普通代码交付阻断|否|
|`contract-validation`|docs/report registry/artifact/runner contract 和安全边界|是，对 promotion-facing 改动阻断|否|
|`report-validation`|Reader Brief、report index、报告导航和读者摘要|是，对 report/Reader Brief 改动阻断|否|
|`integration`|scheduler、trading_engine、portfolio tooling 和跨模块集成|按改动范围阻断|允许中等耗时|
|`slow-research-regression`|Dynamic v3、backtest simulation、research regression|对 research promotion 前置阻断；日常交付可明确延后|是，必须单独运行或记录未运行|

旧 alias 映射：`fast -> fast-unit`、`reader-brief -> report-validation`、
`dynamic-v3 -> slow-research-regression`、`trading-engine -> integration`、
`full -> full`。

## 5. Artifacts

运行 `python scripts/run_validation_tier.py <suite> --write-runtime-artifact` 时输出：

- `outputs/validation_runtime/<run_id>/test_runtime_summary.json`
- `outputs/validation_runtime/<run_id>/test_runtime_reader_brief.md`

artifact 必须披露：

- suite/tier、suite_family、promotion_blocking、slow_suite_allowed；
- command、workers、dist、extra pytest args、status、exit_code、elapsed_seconds；
- `strategy_logic_changed=false`、`production_effect=none`、`broker_action_allowed=false`；
- no production mutation / no cached data mutation / no strategy logic change safety boundary；
- 若为 `PRINT_ONLY`，必须明确不能作为 passed validation evidence。

## 6. CI-Friendly Commands

后续所有需要 pytest 验证证据的任务默认使用并行模式。首选入口是
`python scripts/run_validation_tier.py <suite> --write-runtime-artifact`，该 runner 默认
`-n 16 --dist loadfile`；focused one-off pytest 也应显式使用
`python -m pytest -n 16 --dist loadfile ...`。只有在复现串行行为或排查
parallelism-related failure 时才使用 `--workers 1` / 串行 pytest，并在交付说明中记录原因。

```powershell
python scripts/run_validation_tier.py fast-unit --write-runtime-artifact
python scripts/run_validation_tier.py contract-validation --write-runtime-artifact
python scripts/run_validation_tier.py report-validation --write-runtime-artifact
python scripts/run_validation_tier.py integration --write-runtime-artifact
python scripts/run_validation_tier.py slow-research-regression --write-runtime-artifact
python scripts/run_validation_tier.py full --write-runtime-artifact
```

低核或复现环境可加 `--workers 1`，但这属于显式例外而不是默认路径。CI 可以先并行执行前三个 blocking suite，
再按改动范围执行 `integration` 或 `slow-research-regression`；完整发布或跨模块 P0
变更仍应尽量跑 `full`。如果 `full` 或 slow suite 超时，交付说明必须列出已经通过的
suite、超时时间和 visible slow-test clues，不能写成 PASS。

## 7. 验收标准

- `scripts/run_validation_tier.py --list` 显示五类正式 suite、旧 alias 和 promotion
  blocking 标记。
- runner tests 覆盖正式 suite、旧 alias、runtime artifacts 和 slow suite 隔离。
- fast suite 和 contract validation suite 可成功完成。
- report registry、artifact catalog、operations runbook、system flow、README、task register
  同步更新。
- `ruff`、`compileall`、`git diff --check`、documentation contract / report index 相关
  验证通过。

## 8. 进展记录

- 2026-06-15：新增任务并进入 IN_PROGRESS，原因：owner 要求完成附件中的 TRADING-347；
  当前重点是建立 suite taxonomy、runtime artifact 和 promotion-blocking 规则，而不是改变
  strategy logic 或生产行为。
- 2026-06-15：实现完成并进入 VALIDATING。新增正式 suite taxonomy、旧 alias 映射、
  pytest markers、`--write-runtime-artifact`、`test_runtime_summary.json`、
  `test_runtime_reader_brief.md`、report registry entry、artifact catalog、system flow、
  operations runbook 和 README 使用说明。首次默认并行运行发现本地 `.venv` 缺少
  `pytest-xdist`，该依赖已经在 `pyproject.toml` dev extra 中声明；本轮安装声明依赖后
  重跑，没有把并行失败降级为串行 PASS。验证结果：runner tests 6 passed；
  `fast-unit` 43 passed / 31.93 秒；`contract-validation` 19 passed / 22.86 秒；
  documentation contract PASS；report index `PASS_WITH_WARNINGS` 仅保留既有 missing/stale
  visibility。Slow research regression 已作为独立 suite 和旧 `dynamic-v3` alias 隔离；
  本轮不把未运行 slow suite 写成 passing evidence。
- 2026-06-20：继续 runtime optimization 诊断并修复一个重复构建热点。原始串行
  `python -m pytest -q` 为 2961 passed / 1805.89 秒；既有 runner 并行 full
  `--dist loadfile` 为 2961 passed / 253.37 秒，`--dist load` 为 2961 passed /
  281.59 秒，确认默认 `loadfile` 更合适。最慢用例
  `test_campaign_control_plane_validation_pack_writes_expected_artifacts` 原先在 full
  durations 中约 137～161 秒；通过让 Campaign validation pack builder 复用 writer 已
  生成的 component payload，focused rerun call 降至约 60～73 秒，优化后并行 full 为 2961 passed /
  195.72 秒（runner elapsed 210.84 秒）。本优化只减少 validation artifact 重复构建，
  不改变 strategy logic、cached data、production state、broker/order 或投资解释路径。
- 2026-06-20：owner 决定后续所有需要 pytest 验证的任务默认使用并行模式。项目文档已
  明确：首选 validation tier runner；focused one-off pytest 使用
  `python -m pytest -n 16 --dist loadfile ...`；串行 pytest 仅用于显式复现或
  parallelism-related failure 诊断，且不能把并行失败静默改写成串行 PASS。
- 2026-06-20：追加瓶颈定位。`full -n 16 --dist loadfile` 采样为 2961 passed /
  241.41 秒，`full -n 16 --dist worksteal` 为 2961 passed / 242.73 秒，`full -n 24
  --dist loadfile` 为 2961 passed / 247.73 秒；增加 worker 或切换 `worksteal` 未改善
  wall time。Top durations 显示 `tests/test_research_campaign.py` 单文件在 top list 中
  累计约 221～228 秒，最大单测
  `test_campaign_control_plane_validation_pack_writes_expected_artifacts` 为 102～115 秒。
  临时目录函数级计时显示完整 writer 约 76 秒，其中主要来自 `b2_e2e_compute` 约 22 秒、
  `b2_compute_smoke` / `b2_full_compute` / `b2_final_gate` / `run_next_smoke` /
  `forced_budget` / `b2_final_repeatability` 各约 10～12 秒；`validation_pack_precomputed`
  约 0.03 秒，Markdown render 和 post-B2 文档生成不是主瓶颈。当前主瓶颈是少数
  Campaign/research artifact 集成测试的单测粒度和串行 compute builder，而不是并行数量不足。
- 2026-06-20：进入下一轮瓶颈优化实现；目标是只在 validation pack writer 内复用同一轮
  B2 Campaign compute payload，减少重复 `run_b2_control_window_research` /
  targeted / full diagnostic 计算，不改变 Campaign runtime production path、strategy
  logic、cached data、broker/order、official weights 或测试覆盖语义。
- 2026-06-20：B2 compute payload cache 优化完成并验证。新增 validation-run scoped
  `_B2ComputePayloadCache`，只在 Campaign validation artifact builder / focused tests 中复用
  B2 targeted、control-window 和 full diagnostic payload；每次复用仍重新写入当前 run 的
  output directory，默认 CLI/production Campaign runtime 不启用该 cache。focused 最慢
  validation-pack 测试 call time 从约 102～115 秒降至 19.30 秒；
  `tests/test_research_campaign.py` 从 127.68 秒降至 44.44 秒；全量并行 full tier
  通过，runtime artifact 写入
  `outputs/validation_runtime/trading-347-bottleneck-after-b2-cache`，2961 passed /
  643 warnings / pytest 173.30 秒（runner elapsed 174.02 秒）。新的 full slowest list
  显示主要瓶颈已转移到 Dynamic v3 / rescue / roadmap 研究回归测试，worker 数量不再是
  当前主因。
- 2026-06-20：进入下一轮 Dynamic v3 validation runtime 优化。focused 计时显示
  `tests/test_etf_dynamic_v3_failure_attribution.py` 为 3 passed / 52.52 秒，两个慢测
  分别约 25.56 秒和 23.50 秒，主要成本是重复构建同一 synthetic validation sample。
  本轮目标是在 failure-attribution validation sample 构建路径增加进程内只读缓存，并让
  focused tests 复用同一 sample；默认报告/CLI 仍输出 fresh validation artifact，不改变
  Dynamic v3 策略逻辑、cached data、paper-shadow/live/broker/order/official weights。
- 2026-06-20：Dynamic v3 validation sample cache 优化完成并进入 full-tier 复验。
  `dynamic_v3_failure_attribution` 新增 checksum-keyed validation sample cache，focused 文件从
  52.52 秒降至 29.05 秒；`dynamic_rescue` 新增同类 validation sample cache，focused 文件从
  58.70 秒降至 20.08 秒；`dynamic_v3_parameter_research` 将 real sweep smoke 的候选
  evaluation 改为 2 worker 并为 real-smoke input + fixed robustness reports 增加只读复用，
  focused 文件从 56.10 秒降至 40.85 秒。三组 Dynamic v3 focused 回归合并运行
  24 passed / 39.79 秒。1 候选 smoke 尝试因 `real_neighbor_count` 合同不足已回退，
  未降低 robustness 覆盖。
- 2026-06-20：Dynamic v3 cache full-tier 复验通过。全量并行 full tier 输出
  2961 passed / 643 warnings / pytest 167.65 秒（runner elapsed 168.27 秒），runtime
  artifact 写入 `outputs/validation_runtime/trading-347-dynamic-v3-cache-full`。相对
  B2 cache 后的 174.02 秒继续下降约 5.75 秒；新的 top durations 分散在
  failure attribution、parameter research、signal feature quality、owner roadmap、micro
  search、gate calibration、portfolio sensitivity 等多个 25～32 秒级单测，当前瓶颈已从
  单一文件尾部转为多条独立研究回归链路的单测内部计算成本。
