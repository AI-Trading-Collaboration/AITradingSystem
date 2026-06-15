# TRADING-347 Full Test Runtime Stabilization

最后更新：2026-06-15

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

```powershell
python scripts/run_validation_tier.py fast-unit --write-runtime-artifact
python scripts/run_validation_tier.py contract-validation --write-runtime-artifact
python scripts/run_validation_tier.py report-validation --write-runtime-artifact
python scripts/run_validation_tier.py integration --write-runtime-artifact
python scripts/run_validation_tier.py slow-research-regression --write-runtime-artifact
python scripts/run_validation_tier.py full --write-runtime-artifact
```

低核或复现环境可加 `--workers 1`。CI 可以先并行/串行执行前三个 blocking suite，
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
