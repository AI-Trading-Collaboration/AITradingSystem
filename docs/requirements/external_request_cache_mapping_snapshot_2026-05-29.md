# 外部请求缓存映射快照韧性

任务 ID：`DATA-020`

最后更新：2026-06-07

## 背景

修复 `RISK-016`、`TRADING-047` 和 `TRADING-048` 后，完整 `aits ops daily-run`
已越过 `score_daily` 和 SEC PIT shadow monitor。随后一次重跑在第 1 步
`download_data` fail closed：

```text
RuntimeError: dictionary keys changed during iteration
```

该错误发生在本地 Python 映射迭代阶段，诊断显示 `FMP_API_KEY`、`MARKETSTACK_API_KEY`、
`OPENAI_API_KEY`、`SEC_USER_AGENT` 均存在，且没有结构化 provider HTTP 失败信息。
同一下载函数在独立复现中可成功，说明更可能是 HTTP/cache identity 或 response metadata
生成过程中遇到可变 mapping（例如 header / params 对象懒加载或底层库更新）时缺少稳定快照边界。

## 目标

1. 外部请求缓存生成 cache identity、metadata 和脱敏 headers/params 时，先获取稳定
   mapping 快照；遇到一次 `dictionary keys changed during iteration` 时重试。
2. 保持 cache key 和 metadata 的脱敏语义：API key、token、Cookie、Authorization、
   User-Agent 不写入原文。
3. 不改变 external request cache schema、不改变 provider endpoint、请求参数或下载数据语义。
4. 修复后重跑完整 `aits ops daily-run`；若失败，应暴露新的真实 provider 或质量门禁原因。

## 非目标

- 不跳过 `download-data`。
- 不复用旧 CSV 冒充本轮下载成功。
- 不放宽 `validate-data` 或 downstream fail-closed 门禁。
- 不修改供应商优先级、universe、FMP/Marketstack/FRED/Cboe endpoint 或 API key 读取方式。

## 验收标准

- 单元测试覆盖 mapping 首次迭代抛出 `dictionary keys changed during iteration` 时，
  external request cache helper 会重试并成功输出脱敏结果。
- `tests/test_external_request_cache.py` 和 `tests/test_data_download.py` 通过。
- 相关 ruff、Black 和 `git diff --check` 通过。
- 完整 `aits ops daily-run` 重新执行并通过，或报告新的真实门禁。

## 进展记录

- 2026-05-29：新增并进入 `IN_PROGRESS`。原因：完整 daily-run 在修复 SEC PIT shadow
  monitor 后，转而被 `download_data` 的本地 mapping 迭代 RuntimeError 阻断；该错误缺少
  provider diagnostic，需在 request cache helper 层增强可变 mapping 快照边界。
- 2026-05-29：实现完成并进入 `VALIDATING`。验证通过
  `tests/test_external_request_cache.py`、`tests/test_data_download.py`、`tests/test_ops_daily.py`、
  相关 ruff/Black/diff check，以及真实 `aits ops daily-run` as-of 2026-05-28；
  最终 run 中 `download_data` 和 `validate_data` 均 PASS。
- 2026-06-07：从 `VALIDATING` 改为 `DONE`。原因：验收证据已满足且无剩余 owner
  或时间窗口 blocker；真实 daily-run 已证明 `download_data` 和 `validate_data`
  在该修复后通过，后续 FRED tail refresh 与 latest daily-run 也未再复现
  `dictionary keys changed during iteration`。
