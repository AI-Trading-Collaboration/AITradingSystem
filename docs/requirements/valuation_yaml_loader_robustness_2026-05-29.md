# 估值与配置 YAML 读取边界韧性

任务 ID：`DATA-021`

最后更新：2026-06-09

## 背景

在修复 `RISK-016`、`TRADING-047`、`TRADING-048` 和 `DATA-020` 后，完整
`aits ops daily-run` 已能通过 PIT、SEC metrics、valuation fetch/validation 和
SEC PIT shadow monitor。随后 `score_daily` 在读取估值快照时触发 Windows native
access violation：

```text
yaml.__init__.py line 125 in safe_load
src/ai_trading_system/valuation.py line 726 in _load_yaml
src/ai_trading_system/valuation.py line 383 in load_valuation_snapshot_store
src/ai_trading_system/cli.py line 11909 in score_daily
```

修复 valuation loader 后，目标测试又在 `config.load_watchlist()` 的同类
`yaml.safe_load(file)` 路径触发相同 native crash。当前多个 daily-run 会触达的 YAML
loader 直接把打开的文本流传给 PyYAML。这个形态会让 PyYAML 的 reader 在解析期间继续驱动
文件流读取；在本地 Windows 子进程长流程中，该路径出现了不可捕获的 native crash，导致
`score_daily` 没有机会进入既有 `ValuationLoadError` / `valuation_load_error` 审计路径。

## 目标

1. daily-run 会触达的估值和配置 YAML loader 在进入 PyYAML 前先读取稳定的 UTF-8 文本快照，
   避免解析器直接持有活跃文件流。
2. 使用 PyYAML 的安全 C loader（`CSafeLoader`，不可用时才退回 `SafeLoader`），避免本机
   长流程命中纯 Python scanner 的不稳定状态。
3. UTF-8 解码失败、文件读取失败和 YAML 解析失败都必须进入现有 load error / validation
   issue 路径，保持 fail closed 和可审计。
4. 不跳过估值校验，不放宽 `score-daily` 估值门禁，不回填或伪造估值快照。
5. 修复后重跑 `score-daily` 和完整 `aits ops daily-run`；若仍失败，应暴露新的真实门禁或
   provider/运行时原因。

## 非目标

- 不修改 FMP valuation endpoint、请求参数、缓存目录或快照 schema。
- 不删除或改写已有估值快照。
- 不在 `score-daily` 中补写缺失估值数据。
- 不捕获或吞掉校验错误来让日报继续。

## 验收标准

- 单元测试确认 `load_valuation_snapshot_store` 和核心配置 loader 将文本内容交给项目统一
  YAML loader，且 loader 使用安全 loader 类解析文本，而不是把 live file object 传给 parser。
- 单元测试覆盖非 UTF-8 YAML 文件会被记录为 `valuation_load_error`。
- `tests/test_valuation.py` 和 `tests/test_daily_scoring.py` 通过。
- 相关 ruff、Black 和 `git diff --check` 通过。
- 完整 `aits ops daily-run` 重新执行并通过，或报告新的真实门禁。

## 进展记录

- 2026-05-29：新增并进入 `IN_PROGRESS`。原因：完整 daily-run 在前序阻塞修复后推进到
  `score_daily`，但 valuation snapshot store 在 PyYAML 读取文件流阶段触发 Windows
  access violation，需把读取边界移到本项目代码并复用现有估值校验 fail-closed 路径。
- 2026-05-29：范围扩展到核心配置 YAML loader。原因：valuation loader 改为文本快照后，
  目标测试在 `config.load_watchlist()` 的 `yaml.safe_load(file)` 路径复现同类 native crash，
  说明 daily-run 需要统一避免 live file object 进入 PyYAML。
- 2026-05-29：新增 `ai_trading_system.yaml_loader`，项目 YAML 读取统一走文本快照 +
  `CSafeLoader`（可用时）。原因：完整 daily-run 的 `pit_snapshots_validate` 进一步暴露
  `config/data_sources.yaml` 在 PyYAML 纯 Python scanner 中出现 `TypeError: argument of type 'bool' is not iterable`，
  PIT manifest 报告本身为 PASS，问题不是数据缺口而是 parser 路径不稳定。
- 2026-05-29：实现完成并进入 `VALIDATING`。验证通过
  `tests/test_valuation.py`、`tests/test_daily_scoring.py`、覆盖核心配置/风险/SEC PIT 的
  234 项目标测试、全 `src/ai_trading_system` ruff、Black check、`git diff --check`，
  以及真实 `aits ops daily-run` as-of 2026-05-28；最终 run 中
  `pit_snapshots_validate`、`valuation_snapshots` 和 `score_daily` 均 PASS。
- 2026-06-09：从 `VALIDATING` 改为 `DONE`。当前代码复跑
  `tests/test_valuation.py tests/test_daily_scoring.py -q` 为 40 passed；
  `ruff check src/ai_trading_system/yaml_loader.py src/ai_trading_system/valuation.py
  src/ai_trading_system/config.py tests/test_valuation.py tests/test_daily_scoring.py`
  PASS；相关 compileall PASS。当前缓存上的
  `aits score-daily --as-of 2026-06-05 --skip-risk-event-openai-precheck` 也能完成
  估值、配置 YAML、数据质量和日报评分读取路径，状态为 `PASS_WITH_LIMITATIONS`，
  数据质量报告为 `PASS`；本次 smoke 未触发 OpenAI 预审，完整 daily-run 证据仍以
  2026-05-28 的 23/23 PASS 为归档依据。
