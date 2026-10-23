# DATA-025：日报取数失败时保留错误的结尾

状态：DONE（2026-10-23）。`production_effect=none`；`broker_action=none`。无需 owner 决定（B 区，只多记一个字段，不改任何行为）。

## 1. 背景

2026-10-20 12:00 JST，OPS-086 步骤 7 的第一个生产日（as_of 2026-10-19）在发布包上失败：`sec_companyfacts` 在 TLS 握手处 4.9 秒失败，`official_policy_sources` 392 秒后失败。排查时发现 capture 只保存了失败命令 stderr 的**前 600 个字符**（`error_summary`）：一份 132 行的 Python 回溯，前 600 个字符只到 urllib3 的调用栈，真正说明原因的最后一行异常（例如 `SSLCertVerificationError`、`SSLEOFError`、`ReadTimeout`）不在里面，也没有存在任何别的文件里。要看到它只能再联网重跑一次命令。

## 2. 做法

`src/ai_trading_system/daily_input_capture.py` 里，每次尝试（`attempt_history` 的每一项）和组件结果各多一个字段 `error_tail`：

- 取的文本和 `error_summary` 是同一份（异常文本，否则 stderr，否则失败时的 stdout），经过同样的清理和遮罩（换行换成空格、去首尾空白、名字含 KEY/TOKEN/SECRET/PASSWORD/AUTHORIZATION 的环境变量的值换成 `[REDACTED]`），**先遮罩再截取**，所以跨过截取边界的密钥也不会露出半截。
- 清理后的文本不超过 600 个字符时 `error_tail` 为 `null`（`error_summary` 已经是全文）；超过时 `error_tail` 是清理后文本的**最后 800 个字符**。

## 3. 不变的

- `error_summary` 仍是清理后文本的前 600 个字符，一字不差。
- 阻断分类（`_classify_source_blocker`）仍只看 `error_summary`，所以 `blocker_code`、`retry_allowed`、OPS-085 的 17:30 重试资格（`ops_daily.py` 里“`error_summary` 含 Python 回溯就不重试”的检查）都不变。
- 没有新文件，没有新的目录，状态文件和 manifest 的其他字段、校验规则、source_revision 都不变；旧读取方忽略新字段。

## 4. 验收

- 新增测试 `tests/test_data025_capture_error_tail.py`（Claude 写）全过，`tests/test_daily_input_capture.py` 与相关的 `tests/test_ops_daily.py`、`tests/test_daily_task_dashboard.py` 不受影响。
- 下一次真实的取数失败时，capture manifest 里能看到该组件的 `error_tail`，以最后一行异常结尾。

## 5. 影响

只在失败且错误文本超过 600 个字符的尝试里多记最多 800 个字符；成功的组件、短错误的失败组件 `error_tail` 为 `null`。

## 6. 进展

- 2026-10-20：需求和验收测试写好，交 pi 实现（DATA-025-01）。
- 2026-10-20：DATA-025-01 发布（main `f1a703a70`）。
- 2026-10-23：任务关闭（DONE）：发布包 `20261022-545e19985` 的第一个生产日 12:00 到 12:17，27 步 PASS，五个取数部分一次通过。production_effect=none；broker_action=none。
