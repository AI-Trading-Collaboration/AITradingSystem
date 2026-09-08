# TRADING-2564 S5：Full 即时失败诊断 v1

状态：IN_PROGRESS；优先级 P1；负责人 integration-coordinator / Codex。
2026-09-08 Owner 要求继续长期研究能力建设；本片属于 umbrella S5 验证效率，
以 `8166843157fa9f0b619539ae7a20e00eea16ee0b` 为冻结源，采用 SINGLE_LANE。

## 目标与合同

现有 runner 已逐行 flush，但 pytest 通常到 session 结束才显示详细失败原因。
在 `scripts/pytest_runtime_profile.py` 的 master `pytest_runtest_logreport` 中，
原 phase 记录完成后立即显示实际 failed report 的诊断。保持原 CLI、完整 collection、
16/loadfile 调度、文件内顺序、退出码、profile/summary/provenance 和 publication fence 语义。
不引入 fail-fast、过滤、重试、提前 PASS 或新的最终证据 schema。

每个 failed setup/call/teardown report 输出一个明确 `IN_PROGRESS` 非终态块，
含 nodeid、worker、phase、优先显示的 reprcrash 根因及有界 traceback；同一 node 的
call 与 teardown 分别显示。pass/skip/expected xfail/非 strict XPASS 不新增输出；
strict XPASS 和 xfail 异常不匹配的实际 failed report 必须显示。
所有新增行带固定前缀，防止 traceback 中形似耗时统计的行污染 runner 的最终解析。
控制字符转义；显示上限是操作性输出预算，与投资阈值无关。截断须明示，并指向最终完整报告。
worker 不重复输出。仅隔离诊断格式化和写入的 Exception；不吞遥测错误、KeyboardInterrupt
或 SystemExit。terminal reporter 显式 flush；无 reporter 或该 sink 失败时以 stderr
作同一合并管道的尽力告知；所有诊断 sink 不可用时保留 pytest 原始执行结果。

## 分步、依赖与验收

1. D1 登记/合同：START、source publication transaction、canonical task event、LANE
   预检通过后实施；两个独立只读审查已核对 hook 语义和来源闭包。
2. D2 实现/合成证明：生产改动仅 plugin，新独立测试文件。一次真正 16/loadfile
   子进程经现有 `_run_command` 转发，末尾测试等待外层 release。外层收到完整失败根因、
   确认子进程存活且最终结果尚未生成后释放；最后验证所有剩余测试完成、原失败退出码、
   collection/phase/outcome 和诊断精确计数。覆盖上述负例、格式化/写入/flush 错误、
   traceback 截断和伪耗时行解析。所有 pytest 验证默认 16/loadfile。
3. D3 来源：append 命名 `phase_trading_2564_s5_immediate_failure_diagnostics_v1`，
   精确继承 S5 duration seed 的 66 条闭包并加入 plugin、新测试和本文。历史 prefix/业务字段
   保留，restricted source intersection 不扩大。同步当前 consumer、生成计数与 system flow。
4. D4 集成：source/shared 影响验证通过后 source commit；官方 Atlas 在该 exact HEAD
   重建一次，再在最终候选执行适用正式 tiers 与自然边界 Full。独立审查通过后按 publication
   fence 集成本地 main、普通推送、核对 SHA 并完成清理。umbrella 仍 IN_PROGRESS。

开放事项：D2–D4 的真实验证结果尚未产生，不预先声称 PASS 或稳定提速。
无真实 DQ/research/activation/capture/outcome/cache/download/provider/order/fill；
不改变 2021-02-22 primary window、PIT/投资解释、heartbeat 或生产配置。

## 工作区与证据生命周期

复用 `D:/Work/AITradingSystem_devx014_source_preservation`，不创建临时 clone/worktree。
分支 `codex/trading-2564-s5-live-failure-diagnostics-v1`；证据与协调器 helper 存于
`outputs/architecture/trading_2564_s5_live_failure_diagnostics/`，归本任务所有并留存可审计结果。
合成子进程仅用 pytest 临时目录，finally 释放 gate 并回收子进程；正式验证结束后检查
残留，保全唯一证据再清理精确任务路径。分支在 main/remote 一致且无唯一内容、活动进程
依赖后删除，主工作区保留。不得读取 known-unrelated 排除文件。

## 进展

2026-09-08：START PASS；source fence 已取得并进入 TASK_SOURCE_PRE_WRITE。
首次 acquire 显式重复声明 fence 自动追加的 validation runtime resource，被门禁拒绝且
未创建事务；删除重复参数后原事务 ID 成功取得，不修改 fence 或已有历史证据。

首轮原 runtime/runner 回归 103 PASS（113.00秒）；新诊断 25 PASS（9.09秒），含真实
16 worker、25 nodes/74 phase reports、六失败阶段的进程结束前 handshake。
子 pytest 故意退出1，外层测试通过；不能把这个合成子进程计为正式 Full。
独立审查随后发现动态异常类型名的控制字符以及二次行截断超过字符预算两个边界，已直接
修正为类型名有界 JSON 单行转义、行与字符共同裁剪，补反例后重验。
首轮 child 原始 bytes 已从
`C:/Users/32739/AppData/Local/Temp/pytest-of-JACK/pytest-18725/popen-gw0/test_real_16_worker_diagnostic0`
保全到本波 runtime 的 `retained_live_fixture_v1/`，45 files/87893 bytes 逐项 hash 一致；
该临时源无工程实现，在确认进程依赖为零后清理，receipt 保留。
修复后 focused 的精确临时根预先登记为
`D:/Work/AITradingSystem_devx014_source_preservation/outputs/architecture/trading_2564_s5_live_failure_diagnostics/pytest_live_v2`；
只用于本波合成验证，结束后保全 child outputs/测试结果及哈希，再按同一退出条件清理。


修复后 30 PASS（9.37秒），保留原25例并新增5个公开行为反例；独立复核原counterexample、
控制字符密集文本与多短行均通过。D2 实现与合成证明完成，D3 命名69来源/consumer与23个
来源验证cases已实现，进入 D3/D4 自然集成验证。工程验收及发布的最终状态由本波
`implementation_validation_v2.json`、source/shared XML、正式 summaries 和 publication
transaction 的绑定终态记录；本需求不预报尚未执行的正式 Full 结果。
umbrella 仍 IN_PROGRESS，S3真实研究/Composer/S4与后续S5依赖不因本片自动解除。
