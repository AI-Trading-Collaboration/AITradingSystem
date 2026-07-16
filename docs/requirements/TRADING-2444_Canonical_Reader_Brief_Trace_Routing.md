# TRADING-2444：Canonical Reader Brief Trace 路由修复

最后更新：2026-07-16

## 状态

- Status: `DONE`
- Priority: `P1`
- Owner / next responsible party: system operations
- Created: 2026-07-16

## 背景

`aits ops daily-run` 先在 legacy `outputs/reports/evidence/` 生成 daily score trace，
随后把该文件归档到 canonical run bundle 的 `traces/`。Daily-run 末尾会基于
canonical bundle 再刷新 Reader Brief，但当前刷新 helper 仍按
`<canonical reports>/evidence/daily_score_<as_of>_trace.json` 查找 trace。

因此 2026-07-15 的真实 daily-run 虽然 36/36 steps PASS，trace 也已正确归档，
Reader Brief quality 仍把 trace 标为 `MISSING/IMPORTANT`，整体降级为
`LIMITED_READER_CONTEXT`。这不会重算 score，却削弱 canonical Reader Brief 的
source/evidence audit trail。

## 采用方案

1. Daily-run canonical refresh 显式接收 canonical trace path；
2. 调用方传入 `run_paths.traces_dir/daily_score_<as_of>_trace.json`；
3. 不复制第二份 trace 到 `reports/evidence/`，保持 canonical bundle 的
   `reports/` 与 `traces/` 分层；
4. 缺 trace 时继续按现有 Reader Brief 语义显示限制，不静默回退到 legacy 路径。

## 验收标准

1. Daily-run integration test 在 legacy evidence 目录生成 trace、归档到 canonical
   `traces/` 后，Reader Brief `source_inputs.trace_bundle` 为 `AVAILABLE`；
2. Reader Brief 记录的 full path 指向本轮 canonical `traces/`，不指向 legacy
   `outputs/reports/evidence/` 或不存在的 canonical `reports/evidence/`；
3. `validate-reader-brief` 不再仅因这一路由错误降级；真正缺 trace 时仍保持
   `LIMITED_READER_CONTEXT`；
4. focused parallel pytest、Ruff、compileall 和 diff check 通过；
5. 不改变 score、PIT、data quality、production/active shadow weights、broker、order
   或 trading action，`production_effect=none`。

## 进展记录

- 2026-07-16：本周缺跑恢复审计发现 2026-07-15 canonical trace 位于
  `traces/daily_score_2026-07-15_trace.json`，checksum 与 legacy trace 一致；
  Reader Brief 却查找不存在的 `reports/evidence/...`。任务登记并进入
  `IN_PROGRESS`，修复范围限定为 canonical refresh path routing、focused test、
  task register 和 system flow。
- 2026-07-16：daily-run final refresh 改为显式接收本轮 canonical
  `traces/daily_score_<as_of>_trace.json`；integration test 证明 legacy trace
  归档后 Reader Brief 读取 canonical path，真正缺 trace 的既有降级测试仍通过。
- 2026-07-16：真实 2026-07-15 bundle 的隔离复验得到 Reader Brief=`OK`、
  quality=`OK`、13/13 checks、warnings=0、trace=`AVAILABLE`、
  `production_effect=none`。Focused parallel pytest=`42 passed`，另有单测复验
  `1 passed`，Ruff、compileall 与 diff check 均通过。隔离命令默认重建的两份
  canonical Owner Daily Brief 已按 run manifest 恢复，JSON/HTML SHA256 均与原
  manifest 完全一致。任务转 `DONE` 并移入 completed register。
