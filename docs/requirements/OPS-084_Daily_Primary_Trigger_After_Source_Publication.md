# OPS-084：日报主触发改到 12:00，避开 Cboe VIX 发布延迟

最后更新：2026-10-15

稳定任务 ID：`OPS-084_DAILY_PRIMARY_TRIGGER_AFTER_SOURCE_PUBLICATION`

状态：`DONE`（2026-10-15；验收见第 5 节）

关联：`docs/requirements/OPS-082_Deterministic_Windows_Scheduler.md`（调度）、OPS-070（fail-closed）、
OPS-073（终态处置）；任务记录 `tasks/OPS-084_DAILY_PRIMARY_TRIGGER_AFTER_SOURCE_PUBLICATION.yaml`。

## 1. 问题

- 2026-10-13 09:30 JST 的运行 `daily_ops_run:2026-10-12:20261013T003005Z`（as_of 2026-10-12）`FAILED`，
  分类 `DATA_SOURCE_UNAVAILABLE`：capture 组件 `market_macro` 两次尝试（间隔 5 秒）都报
  `Cboe VIX response had no rows for 2026-10-12 to 2026-10-12`。FMP 价格正常。
- 运行副本外部请求缓存里这次取到的 Cboe 文件最后一行是 10/09，响应头 `Last-Modified: Sat, 10 Oct 2026 01:11:01 GMT`，
  抓取时间 00:31:24Z，CDN `Age: 215`：纽约约 20:27 时 Cboe 还没有 10-12 这一行。
- `^VIX` 只来自 Cboe（FMP 的 `^VIX` 映射为 None）。VIX 用于评分 `risk_sentiment` 的全部三个信号、
  `scoring/position_gates.py` 的市场压力仓位上限、市场面板和 reader brief，按 OPS-070 缺失即 fail-closed。
- 17:30 不补跑：同一 as_of 已有 `FAILED` 终态时运行控制返回 `RUN_CONTROL_BLOCKED_TERMINAL_RECOVERY_REQUIRED`，
  不执行步骤（OPS-082 第 5 节"去重"）；OPS-073 把抓取阶段失败归为 `WAIT_FOR_NEXT_PROVIDER_READY_AS_OF_ORDINARY`。
  所以一次晚发布会丢掉一整天的日报。
- 历史：缓存里 2026-07-24～09-05 在 00:33Z～00:49Z 抓取的每一份 Cboe 文件都含 as_of 当天一行；此前 `market_macro`
  的失败（08-18、09-01）都是到 cdn.cboe.com 的 TLS 断连，不是晚发布。余量只有几分钟。
- 美国 2026-11-01 结束夏令时，之后 09:30 JST 等于纽约 19:30，比现在早一小时（推断：届时多数早上取不到当天 VIX）。

## 2. 决定

**`owner_decision:OPS-084:2026-10-13:primary_trigger_1200`**：owner 2026-10-13 10:39 JST 在决策卡
"日报主跑时间要不要从 09:30 改晚，避开 VIX 晚发布？"上选择"改到 12:00"（选项说明：主跑改为 12:00 JST，
只改调度脚本、摘要标签、测试和文档；上线后在电脑上重新注册一次计划任务；日报晚 2.5 小时）。

未选的选项：保持 09:30 并在运行内等待重试；修改 OPS-073 让 17:30 重跑只在抓取阶段失败的日期（可与本任务叠加，
需另行决定）；不改。

## 3. 改动

| 位置 | 改动 |
|---|---|
| `scripts/ops/register_daily_scheduler_task.ps1` | `$PrimaryTime` 09:30 → 12:00；17:30 不变。12:00 JST 是纽约 23:00（夏令）/ 22:00（冬令），比观察到的最晚更新（纽约 21:11）至少晚 49 分钟 |
| `scripts/ops/daily_scheduler_run.ps1` | PRIMARY / RESCUE 标签分界 13:00 → 15:00（只影响日志文件名和摘要标签，电脑唤醒后的延迟启动仍记为主窗口） |
| `scripts/ops/daily_scheduler_summary.py` | 主窗口标签"12:00 主窗口" |
| `scripts/agent/agent_config.json` | pi 的日报避让窗口 09:20–10:30 → 11:50–13:00（2026-10-11 一次完整运行 915 秒） |
| `tests/test_ops082_daily_scheduler.py` | 注册脚本断言 12:00；新增一致性测试：主触发早于标签分界、分界不晚于补救触发，摘要标签与触发时间一致，两个触发时间都在 pi 避让窗口内 |
| 文档 | OPS-082 需求文档、运维手册第 1 与 9 节、`docs/system_flow.md` 第 1 节 |

不变：`aits ops daily-run`、as_of 解析（纽约最近已完成交易日加收盘后 3 小时）、运行控制、OPS-073 处置、数据源。

## 4. 验收

1. 发布到 main 后，运行副本按运维手册 9.1 更新，`daily-plan --fail-on-missing-env` 为 READY。
2. 在运行副本根目录运行 `powershell -NoProfile -File scripts\ops\register_daily_scheduler_task.ps1`（幂等，不需要管理员），
   `schtasks /query /tn "\AITradingSystem Daily Run" /xml` 显示每日 12:00 与 17:30 两个触发。
3. 第一次 12:00 运行：`market_macro` PASS（含 as_of 当天 VIX），摘要窗口为"12:00 主窗口"。然后转 `DONE`。

## 5. 进展

- 2026-10-13：在 main `25b11cf9d` 的云端 clone 上完成改动；`tests/test_ops082_daily_scheduler.py` 22 passed，
  新测试在任一处未改时失败（注册脚本、摘要标签、pi 避让窗口）；交给 pi 线程按补丁包发布。
- 2026-10-13 14:01Z：运行副本按运维手册 9.1 从 `cc548db19` 更新到 `34ab84b0b`（含 OPS-084-01、OPS-085-01），
  `daily-plan` READY（27 步，as_of 2026-10-12）；14:06Z 在运行副本根目录重新注册计划任务，触发为每日 12:00 与 17:30（第 4 节第 1、2 条）。
- 2026-10-14 12:00 JST（as_of 2026-10-13）：第一次 12:00 运行，摘要窗口 PRIMARY，`market_macro` 第 1 次尝试 PASS，
  Cboe VIX 最后日期 2026-10-13（第 4 节第 3 条）。那天日报在 score_daily 失败是 GOOGL 拆股未登记（OPS-080-02），与触发时间无关。
  2026-10-15 12:00 JST（as_of 2026-10-14）`market_macro` 同样 PASS，日报 27 步全 PASS。状态改为 `DONE`。

production_effect=none；broker_action=none。
