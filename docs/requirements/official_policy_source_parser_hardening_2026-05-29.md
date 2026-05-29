# 官方政策来源解析韧性

任务 ID：`RISK-016`

最后更新：2026-05-29

## 背景

2026-05-29 自动化执行 `aits ops daily-run`，实际评估日期为 2026-05-28。
前置市场数据、PIT、SEC companyfacts、SEC metrics、FMP valuation snapshots 均已通过
或按预期输出 warning，但 `score_daily` 在官方政策来源抓取阶段失败。

失败点位于 `official_policy_sources._row_count(..., parser_kind="ofac_xml")`。
当时已经写入 OFAC SDN raw XML：

- `data/raw/official_policy_sources/2026-05-28/official_ofac_sdn_xml_ecdeeaa3c247.xml`
- SHA256：`ecdeeaa3c247f0f9a8a5dbf719242e9fa072a2198c1ba452b20304083121efee`
- 大小：28,438,994 bytes

同一 raw XML、当前 `.venv` 和当前源码下重新执行 `_row_count` 可得到 18,959 行；
隔离实时抓取 OFAC 两个 XML 源和完整官方政策源也均可通过。因此本次不是供应商空结果、
坏 XML、API key 缺失或 FMP/SEC/valuation 数据问题，而是官方政策源 parser 阶段缺少
per-source fail-closed 边界，导致裸 traceback 直接阻断 `score_daily`，并且没有生成
`official_policy_sources_YYYY-MM-DD.md` 报告。

## 目标

1. OFAC XML row count 使用显式循环和健壮 tag 处理，避免 generator expression 在本机
   子进程异常状态下留下不可审计 traceback。
2. 官方政策源每个 source 的 row count 和候选抽取必须独立 fail closed：raw payload
   写入后若 parser 异常，记录结构化 `official_policy_source_parse_failed` 错误，保留
   source_id、parser_kind、checksum 和 raw payload 路径。
3. `fetch_official_policy_sources` 返回 FAIL 报告，而不是抛出裸异常；`score-daily` 仍应
   因官方来源抓取失败而停止，不得绕过风险事件预审。
4. 正常 OFAC payload 下官方政策源抓取仍应通过，并继续写 raw payload、候选 CSV、
   download manifest 和中文报告。
5. 修复后重跑完整 `aits ops daily-run`，若遇到新的真实门禁，按 fail-closed 报告并继续
   修复 owner 已授权范围内的阻塞。

## 非目标

- 不跳过官方政策源抓取或 OpenAI 风险事件预审。
- 不把 parser 失败降级为 PASS。
- 不修改官方政策来源列表、LLM request profile、风险事件评分规则或仓位闸门。
- 不补写、回填或伪造缺跑日期。

## 验收标准

- 单元测试覆盖 OFAC XML row_count 和候选抽取正常路径。
- 单元测试覆盖 row_count parser 异常时，fetch report 为 FAIL，含
  `official_policy_source_parse_failed`，raw payload 已写入，候选 CSV 和报告可生成。
- `tests/test_official_policy_sources.py` 通过。
- 相关格式和静态检查通过。
- 完整 `aits ops daily-run` 重新执行并完成最新交易日链路，或报告新的真实门禁原因。

## 进展记录

- 2026-05-29：新增并进入 `IN_PROGRESS`。原因：`daily-run` as-of 2026-05-28 在
  `score_daily` 官方政策源 OFAC XML row_count 阶段裸 traceback，导致日报和后续健康检查
  均未生成。
- 2026-05-29：实现完成并进入 `VALIDATING`。验证通过
  `tests/test_official_policy_sources.py`、相关 ruff/Black/diff check，以及真实
  `aits ops daily-run` as-of 2026-05-28；本轮 run id 为
  `daily_ops_run:2026-05-28:20260529T030036Z`，23/23 步骤 PASS。
