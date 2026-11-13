# DATA-035：OpenAI 预审重试前先等一会儿，代理短暂断线不再停日报

任务卡：`tasks/DATA-035.yaml`。一批：DATA-035-01（B 区）。

production_effect=none；broker_action=none。

## 1. 依据

- Claude 2026-11-11 在线程「修复 SOXX 拆股导致日报停写」发卡片「日报卡在连不上 OpenAI，今晚要不要加等待重试再换包？」，Baker 2026-11-13 13:57 JST 选「今晚加重试并换包」（推荐项），选项原话：「我写小改动：连不上 OpenAI 时隔 15 秒到 2 分钟再试几次；今晚发布，明早 11:30 前换包，需要你打两句部署授权。」卡片是 11-11 写的，“今晚”“明早”按 Baker 选的那天（11-13）算，换包时间见第 5 节。另一项「先不改代码，明天照常跑」没有选。卡片写的是“15 秒到 2 分钟”，第 3 节定为 60 秒、180 秒：重试次数不改（见第 4 节），两次等待就要把一次请求的尝试拉开到约 4 分钟。B 区，不需要决定记录。

## 2. 背景

- 2026-11-11 12:00 JST 的日报没跑（电脑睡眠，计划任务记 1 次错过）。17:30 JST 的补跑（as_of 2026-11-10，发布包 20261110-3d5511e46）是新发布包第一次运行：五个取数部分 PASS，validate_data PASS，数据质量 PASS（DATA-034 生效）。随后 `score_daily` 运行 22.5 秒、返回 1：「风险事件 OpenAI 自动预审失败，已停止每日评分」。后面的步骤全被挡住，运行记为 CODE_DEFECT，as_of 2026-11-10 没有日报。
- 预审报告 `risk_event_prereview_openai_2026-11-10.md`：2 条请求都是 `openai_responses_api_request_failed`，各 3 次尝试，`ConnectionResetError(10054)` 或 `SSLEOFError [SSL: UNEXPECTED_EOF_WHILE_READING]`，每次约 5.0 秒，没有 HTTP 状态，也没有 OpenAI request id（没到 OpenAI 的应用层）。
- `llm_precheck.py` 的 `_post_openai_json_with_retries`：连接错误和可重试的 HTTP 状态（408/409/425/429/5xx）都是立刻重试，不等；各 profile 的 `max_retries` 都是 2，所以一次请求的 3 次尝试在约 15 秒内用完。
- 同日 18:20 JST 在电脑上测：`curl` 和发布包 venv 里的 `requests` 访问 `https://api.openai.com/v1/models` 都拿到 401（连得上，没带密钥）；域名解析到 198.18.x.x（本机透明代理常用的保留地址段），没有 HTTP 代理变量，WinHTTP 直连。结论：当时是电脑到 OpenAI 的代理线路短暂断了，不是 OpenAI 或代码的问题；立刻重试正好都落在同一次断线里。
- 诊断：项目文件 `knowledge/claude/runs/DATA-034-D1/rc-reports/diag-score-daily-2026-11-11.md`。
- 2026-11-12 12:00 JST 的日报（as_of 2026-11-11）PASS，预审连上了。2026-11-13 12:00 JST 的日报（as_of 2026-11-12）又停在同一处：4 条请求各 3 次尝试，12 次全是 `ConnectionResetError(10054)`，每次 5.0 秒，约 15 秒用完；同一次运行里五个取数部分和数据质量都 PASS。诊断：项目文件 `knowledge/claude/runs/DATA-034-D1/rc-reports/diag-score-daily-2026-11-13.md`。

## 3. 做法（DATA-035-01，B 区）

- `llm_precheck.py` 新的公开常量 `OPENAI_RETRY_WAIT_SECONDS = (60.0, 180.0)`：第 n 次重试之前等第 n 个值，超出元组长度就一直用最后一个值。连接错误和可重试的 HTTP 状态都先等再重试；不可重试的状态（比如 400）照旧不重试，最后一次尝试失败以后也不等。
- 等待通过模块钩子 `_retry_sleep`（默认 `time.sleep`）进行，`_post_openai_json_with_retries` 另加参数 `sleep=`，测试用它们记录等待、不真睡。
- 每次尝试的诊断加 `retry_wait_seconds`（之后还要重试的那次才有）；`llm_precheck.py` 和 `risk_event_prereview.py` 报告里的“请求诊断”表加一列 “Wait before retry”。
- 对日报的影响：预审最多 4 路并发；一条一直失败的请求从约 15 秒变成约 4 分钟后才判失败；4 分钟内线路恢复，这条请求就成功，报告里照旧有 WARNING `openai_responses_api_retry_succeeded`。日报的步骤没有超时限制，17:30 补跑窗口不受影响。

## 4. 不变的

- 重试次数：`config/llm_request_profiles.yaml` 的 `max_retries` 都还是 2（它是受管参数 `llm_request_profiles.daily_risk_event_cost_pilot` 的一部分，`config/data_sources.yaml` 也写着“单请求失败最多重试 2 次”）；超时 120 秒、模型、候选上限、并发数都不变。
- 失败语义：所有尝试都失败时预审照旧 FAIL，`score_daily` 照旧停下，不会在没有预审的情况下出日报。
- 日报的阻断分类（这种失败仍记 CODE_DEFECT）和 OPS-085 的补跑重试条件。

## 5. 验收

- `tests/test_data035_openai_retry_wait.py`：等待是 60、180 秒；连接错误每次重试前等、最后一次后不等，诊断和审计记录里有 `retry_wait_seconds`；一次断线后成功只等一次并给出 WARNING；可重试的 HTTP 状态也等；不可重试的状态不等；第一次就成功不等；重试次数多于等待个数时重复最后一个值；`max_retries=0` 不等；默认走模块钩子 `_retry_sleep`（默认是 `time.sleep`）。
- `tests/test_risk_event_prereview.py`：自动夹具把 `_retry_sleep` 换成记录器（不真睡）；超时那条测试另断言等待是 60、180 秒，报告有 “Wait before retry” 一列。
- 发布后从 main 打新发布包，Baker 打两句授权后部署，避开 11:50–13:00 和 17:20–18:30 JST，目标是 2026-11-14 11:30 JST 以前；之后第一次日报 `score_daily` PASS，或者预审仍然失败时报告里能看到 60、180 秒的等待。

## 6. 没有解决的

- OpenAI 预审的连接失败被记为 CODE_DEFECT，不算数据源临时故障，所以同一天的补跑不会自动重试；要不要改分类以后另议。
- 代理线路断开超过约 4 分钟，日报照样停；网络和代理设置在电脑那边。
- 电脑睡眠时计划任务不会把它叫醒（11-11 12:00 因此错过）；要不要给计划任务打开唤醒，单独问 Baker。
- as_of 2026-11-09 和 2026-11-10 没有日报，怎么记在 DATA-034 的收尾批里问 Baker。

## 7. 进展

- 2026-11-11：Claude 写好补丁和测试，在云端测过，发卡片问 Baker。
- 2026-11-12：12:00 JST 的日报（as_of 2026-11-11，发布包 20261110-3d5511e46）PASS。
- 2026-11-13：12:00 JST 的日报（as_of 2026-11-12）又停在预审的连接断开；Baker 13:57 JST 选「今晚加重试并换包」；补丁在当天的 main 上重新打包，由 pi 应用。
