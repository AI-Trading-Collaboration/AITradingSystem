# OPS-085：主跑只因数据源临时故障失败时，17:30 从采集步骤自动重试一次

最后更新：2026-10-15

稳定任务 ID：`OPS-085_RESCUE_RETRY_AFTER_SOURCE_FAILURE`

状态：`VALIDATING`（运行副本已含本改动；第一次真实的 17:30 自动重试按第 6 节第 3 条核对后转 `DONE`）

关联：OPS-082（计划任务与 17:30 触发）、OPS-084（主触发 12:00）、OPS-073（终态处置）、OPS-071（报告尾链恢复）、
OPS-072（官方政策源传输重试）、OPS-070（fail-closed）；任务记录 `tasks/OPS-085_RESCUE_RETRY_AFTER_SOURCE_FAILURE.yaml`。

## 1. 问题

- 同一 as_of 的主跑一旦以 `FAILED`/`BLOCKED` 结束，17:30 的第二次触发只返回
  `RUN_CONTROL_BLOCKED_TERMINAL_RECOVERY_REQUIRED`，不执行任何步骤（OPS-082 第 5 节"去重"）；OPS-071 的恢复只允许从
  `artifact_lineage` 以后的报告尾链开始，救不了采集阶段。所以一次数据源临时故障会丢掉一整天的日报。
- 运行副本里 2026-07-24 以来的记录（只读核对）：采集阶段因数据源失败、而其余步骤只是被阻断的日期有

| as_of | 组件 | 原因 | blocker | 本任务是否重试 |
|---|---|---|---|---|
| 2026-07-31 | `official_policy_sources`（GovInfo） | TLS EOF | 由 OPS-072 修复为请求级有界重试 | 否（见第 3 节） |
| 2026-08-18 | `market_macro`（cdn.cboe.com） | TLS 断连 | `PROVIDER_UNAVAILABLE` ×2 | 是 |
| 2026-09-01 | `market_macro`（cdn.cboe.com） | TLS 断连 | `PROVIDER_UNAVAILABLE` ×2 | 是 |
| 2026-10-12 | `market_macro`（Cboe VIX） | 当天一行尚未发布 | `REQUEST_FAILED` ×2 | 是 |

  四次的运行状态相同：`capture_daily_inputs` 已关闭（`PARTIAL_CAPTURE`，校验 PASS），只有 always-run 的
  `pipeline_health` 因当日评分缺失而 FAIL，其余下游全是依赖阻断。

## 2. 决定

**`owner_decision:OPS-085:2026-10-13:rescue_retry_after_source_failure`**：owner 2026-10-13 11:33 JST 在决策卡
"早上因数据源失败的日报，要不要在 17:30 自动重试一次？"上选择"自动重试"（选项说明：新开一个小任务：早上只因数据源不可用而
失败时，17:30 从采集步骤自动恢复一次；其他原因的失败仍要人工处理）。

## 3. 规则

17:30（包装脚本的 `RESCUE` 窗口，15:00 以后）调用 `aits ops daily-run --retry-after-source-failure`。daily-run 读取同一
workflow/规格/as_of 的上一次运行，**全部**满足下列条件才重试，否则照旧返回原来的运行控制结论：

1. 上一次运行是 `FAILED` 或 `BLOCKED`，且是该 as_of 的第 1 次尝试（同一 as_of 只自动重试一次）。
2. `capture_daily_inputs` 已关闭；采集 manifest 为 `PARTIAL_CAPTURE`、as_of 一致、manifest 校验 `PASS`。
3. 除 always-run 检查（`pipeline_health`、`secret_hygiene`）外没有其他步骤失败；其他步骤只是被阻断或跳过。
4. 每个失败的采集组件都满足：`recovery_mode` 为 `IMMUTABLE_RAW_BACKFILL`（可以重新下载的历史数据，目前只有
   `market_macro`：FMP 价格、Cboe VIX、FRED 利率），最后一次尝试的 blocker 在该组件策略的 `retryable_blocker_codes`
   里（`PROVIDER_UNAVAILABLE`、`REQUEST_FAILED`），错误摘要不含 Python traceback。

不重试、仍需人工处理的情况：密钥缺失、权限拒绝（401/403）、额度用尽（429）、数据格式错误、代码异常（traceback）、
任何其他步骤失败；以及 `official_policy_sources`、`fmp_forward_pit`、`fmp_valuation`（`HISTORICAL_RECAPTURE_FORBIDDEN`，
只有一次尝试，事后重抓会改变时点语义）和 `sec_companyfacts`（`MANUAL_NON_PIT_RAW_REVIEW`）。GovInfo 这类官方源的
传输故障已由 OPS-072 在请求内重试；OPS-072 第 2 节"禁止组件级历史重抓"对这些组件继续有效。

`REQUEST_FAILED` 是分类器的兜底代码。没有 traceback 的非网络错误（例如 2026-07-24 迁移期间的
`DOWNLOAD_MANIFEST_CURRENT_GENERATION_MISMATCH`）也会得到一次重试；重试同样 fail-closed，条件不变时会再次失败并停在人工处理。

## 4. 重试怎么执行

- 运行控制新增决定 `SOURCE_RETRY`（`runtime_control.yaml` 1.3.0：`source_retry_enabled: true`，
  `source_retry_from_step_ids: [capture_daily_inputs]`）。请求 `operations_source_retry_request.v1` 绑定父运行 id、
  重试起点、原因代码 `OPS_085_TEMPORARY_SOURCE_FAILURE`、采集 manifest 路径与 SHA-256、要重试的组件及其 blocker。
- 运行控制再次核对：父运行一致、起点在白名单、manifest 字节未变且 as_of 一致、尝试次数未超过 `max_run_attempts`（2）、
  这个幂等键没有用过重试（`outputs/run_control/daily/source_retry/<key>/` 已存在即拒绝）、重放的步骤都声明为幂等。
- 父运行的状态与台账原样归档到 `source_retry/<key>/attempt_<n>/`，写不可变回执 `operations_source_retry_receipt.v1`，
  然后以第 2 次尝试从 `capture_daily_inputs` 起重放其后全部步骤。台账要求 PASS 步骤的依赖都 PASS，所以不能只补跑失败部分。
  第 1 次尝试里已 PASS 的 PIT、SEC、数据校验步骤会对同一 as_of 再跑一次：2026-10-12 17:30 因规格变化把 as_of 2026-10-09
  整条重跑时，采集、PIT、SEC、数据校验等 14 个步骤都 PASS（运行副本 state `operations_run_c0bbf508…`，失败的是当时 DATA-022 的评分授权问题），
  说明同一 as_of 已有产物时重跑这些步骤可行。评分及之后的步骤在第 1 次尝试里本来就被阻断，没有产物。
- 采集步骤带 `--rescue-session`：主会话已 PASS 的组件直接复用（不重新请求）；终态失败的可重试组件在
  `source_control/<as_of>/<component>/rescue/` 下开一个独立的补救会话，有自己的尝试预算（`market_macro` 2 次）与租约。
  主会话的状态文件不改。补救会话结束后（PASS 或终态 FAIL），之后的采集直接读取补救会话结果。
- 采集策略文件 `config/operations/daily_input_capture.yaml` 不变（manifest 校验会核对它的 SHA-256）。
- 命令输出 `Source-failure retry：<结果>`；摘要 JSON 新增 `source_failure_retry`，Markdown 写一行
  "数据源失败自动重试（OPS-085）：…"。结果为 `RETRIED` 或 `NOT_ELIGIBLE:<原因>`（例如 `NO_FAILED_RUN`、
  `ALREADY_RETRIED`、`OTHER_STEP_FAILED:<步骤>`、`COMPONENT_NOT_RETRYABLE:<组件>:<blocker>`、`RUN_CONTROL_<决定>`）。
- 12:00 主跑不带该参数，行为不变。人工运行也可以带 `--retry-after-source-failure`，规则相同。

## 5. 改动

| 位置 | 改动 |
|---|---|
| `src/ai_trading_system/contracts/operations.py` | `SOURCE_RETRY` 决定；`OperationsSourceRetryRequest`、`OperationsSourceRetryReceipt` |
| `src/ai_trading_system/platform/operations/runtime_control.py` | 策略字段 `source_retry_enabled`、`source_retry_from_step_ids`；`acquire(..., source_retry=...)`；`read_execution_state` |
| `config/operations/runtime_control.yaml` | 1.2.0 → 1.3.0，开启重试，起点只允许 `capture_daily_inputs` |
| `src/ai_trading_system/daily_input_capture.py` | `capture_daily_inputs(..., rescue_session=...)`，组件级补救会话 |
| `src/ai_trading_system/ops_daily.py` | `assess_daily_source_retry`（第 3 节条件）；重试时给采集步骤加 `--rescue-session`；报告字段 `source_retry` |
| `src/ai_trading_system/cli_commands/ops.py`、`cli_direct.py` | `daily-run --retry-after-source-failure`、`capture-daily-inputs --rescue-session` |
| `scripts/ops/daily_scheduler_run.ps1` | 只在 `RESCUE` 窗口加 `--retry-after-source-failure` |
| `scripts/ops/daily_scheduler_summary.py` | 解析并显示重试结果 |
| 测试 | `tests/test_daily_input_capture.py`（补救会话只重抓临时失败的历史源、只跑一次、不重抓额度与 PIT 失败）；`tests/test_arch_004f1_operations_control_plane.py`（重放范围、最多一次、5 种未绑定请求被拒、10-12 形状端到端、额度/PIT/其他步骤失败不重试）；`tests/test_ops082_daily_scheduler.py`（只有 RESCUE 窗口带参数、摘要、CLI 转发） |
| 文档 | 本文件；OPS-082 第 5 节"去重"；运维手册第 3 与 9.2 节；`docs/system_flow.md` 第 5 节 |

不变：as_of 解析、12:00/17:30 触发时间、数据源、DQ 与评分门禁、OPS-071 恢复入口、OPS-073 对其他终态的处置。
production_effect=none；broker_action=none。

## 6. 验收

1. 发布到 main 后，运行副本按运维手册 9.1 更新，`daily-plan --fail-on-missing-env` 为 READY。包装脚本的改动不需要重新注册
   计划任务（OPS-084 的触发时间改动需要）。
2. 平常日子：17:30 摘要显示 `NOT_ELIGIBLE:NO_FAILED_RUN`（主跑已 PASS）。
3. 第一次真实的数据源临时故障：12:00 运行 FAILED 且只有可重试组件失败；17:30 摘要显示 `RETRIED`，运行控制状态为第 2 次尝试，
   `source_retry/<key>/attempt_2/source_retry_receipt.json` 存在；主会话已 PASS 的组件状态为复用；如果源已恢复，日报 PASS。
   然后转 `DONE`。如果数据源当天仍未恢复，第 2 次尝试 FAIL，次日不再重试，这同样算通过。

## 7. 进展

- 2026-10-13：在 main `25b11cf9d`（叠加 DATA-023-01 与 OPS-084-01 的本地提交）上完成实现与测试；交给 pi 线程按补丁包发布，
  建议排在 OPS-084-01 之后，同一次运行副本更新。
- 2026-10-13 14:01Z：运行副本按运维手册 9.1 从 `cc548db19` 更新到 `34ab84b0b`（含 OPS-084-01、OPS-085-01），
  `daily-plan` READY（27 步，as_of 2026-10-12）；2026-10-14 05:09Z 再更新到 `556dba09`（OPS-080-02）。第 6 节第 1 条满足。
- 2026-10-14 17:30 JST（as_of 2026-10-13，12:00 主跑在 score_daily 失败，原因是 GOOGL 拆股未登记）：摘要
  `source_failure_retry` 为 `NOT_ELIGIBLE:OTHER_STEP_FAILED:score_daily`，CLI 为 `RUN_CONTROL_BLOCKED_TERMINAL_RECOVERY_REQUIRED`，
  运行状态仍是第 1 次尝试的 FAILED，没有开始第 2 次尝试，符合第 3 节第 3 条。仍等第一次真实的数据源临时故障（第 6 节第 3 条）。
