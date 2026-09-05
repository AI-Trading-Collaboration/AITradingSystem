# DEVX-013 持续研发改进、Token 测量与收益验收闭环 V1

## 任务身份与授权

- task id：`DEVX-013_CONTINUOUS_IMPROVEMENT_EXECUTION_AND_OUTCOME_LOOP_V1`
- priority：`P1`；初始状态：`IN_PROGRESS`。
- owner：Project Owner；next owner：Codex engineering coordinator。
- owner decision：`owner_instruction:DEVX-013:2026-09-06:continue-approved-long-term-improvement-plan`。
- 讨论来源：Codex task `01a072a0-708f-7c83-94ce-e47c01a9c47e`；owner 在长期方案后明确要求“你继续推进吧”。
- mode：`SINGLE_LANE`，从 exact local main `03f114b7edf73fbb15aad94dfff9515495940cc4` 冻结。
- 本轮授权：正式登记长期方案、实现可验证的首个闭环版本，并推进已有重复验证失败候选的最小工程试点；按既有流程验证、集成与普通推送。
- `production_effect=none`、`broker_action=none`；不修改投资阈值、DQ/PIT、研究窗口或交易策略。

## 背景与已有能力

复用 DEVX-006 的生成权威分片、DEVX-011 的工程 telemetry / 稳定候选 fingerprint、DEVX-012 的 existing daily automation post-stage / ISO-week 去重。不新增 scheduler、任务登记权威、锁或发布队列。

2026-09-03 retained workflow-health snapshot 记录 307 次 validation、42.67 runner hours、16.05 failed hours、56 次 Full（26 次失败）、122/162 authority-only commits、10 个同 SHA/tier 重复组。该 snapshot 的观测窗口与生成时刻均须披露，不能据此声称完整周趋势或因果关系。

本地长任务的独立 response usage records 证明缓存输入占比很高；raw token、cached token、uncached token、output token 与账户额度不得混为一谈。此样本不能替代项目总体统计。详细原始对话不进入 Git，也不作为 telemetry 报告正文。

## 长期决策

1. 用统一链路持续执行：采集 → 候选归并 → 准入 → canonical task 实施 → 工程验证 → 收益观察。
2. 数据统计使用确定性程序；模型读取紧凑摘要和异常证据，不定期重读全部日志。
3. 正式比较使用已结束且不重叠的完整周；当前周只能标为 preview。缺失数据显式计 gap，不用 mtime 或缺失值补零。
4. 候选保持稳定身份，关联已有 task、准确 implementation SHA 与 before/after evidence；不建立第二份可独立修改的执行状态。
5. 工程完成与效率收益分别记录；收益证据不足不得报 improved，也不得重写终态 task event。
6. 自动维护的初始执行范围为 owner 已准入且已登记的任务。本轮不把 review-only candidate 自动提升为写代码权限；扩大准入范围必须经过后续 reviewed policy / serial contract wave。
7. 周报只在新增可行动问题、回归、完成或需要 owner 决定时通知；不变状态保持安静。
8. 初始维护容量建议每周一个主项，属于 developer-workflow pilot；四个完整观察周后复核，不影响投资解释。
9. 定期复盘重构必要性，按重复根因、跨模块联动和真实维护成本选择局部范围，不以行数减少作为收益证明。

## 分阶段实施与依赖

|阶段|工作|依赖|验收|
|---|---|---|---|
|S0|登记任务、冻结长期方案、租约与路径声明|owner continue instruction|canonical row、supporting requirement、preflight PASS|
|S1|增量/有界 Token 观测、完整周口径、采集覆盖|S0；使用可用本地 usage records，不采集 prompt|响应去重、冲突/malformed/missing gap、缓存分列、可复核来源身份；合成正负例与真实只读样本|
|S2|候选到 canonical task 的关联与独立收益观察|S1；原 registry writer 不变|稳定 candidate identity、有效任务引用、工程/收益分轴、可比较 before/after evidence、无证据不宣称改善|
|S3|用最高成本重复失败簇完成最小工程试点|S2；先确定真实根因与作用范围|合适的 focused 检查前置、保留最终 Full、确切候选验证；收益仅由测量支持|
|S4|现有 weekly post-stage 接入闭环并观察|S1/S2 发布；现有 trigger gate|可去重、阻断可见、旧报告兼容、有效完整周基线、无新 scheduler|
|S5|月度热点复盘和有界维护政策校准|四个真实完整周及试点证据|复核维护容量、阈值、投入与收益，确定局部重构候选；不自动生成无证据优化任务|

S1/S2/S4 可以在一个兼容的 V1 实现候选内完成，shared schema/policy 先由 coordinator 串行冻结，再开展模块实现。S3 必须先证实根因，不能为追求收益反复盲跑 Full。S5 是时间依赖的观察阶段，不能以 synthetic PASS 提前完成。

## 实现契约与资源纪律

- Token collector 只读显式指定的本地日志根与已识别 project/thread 元数据；使用 `token_usage_record.payload.usage`，按唯一 response identity 去重。
- 不累加累计计数，不重复累加 reasoning output（它是 output 的子集）；不把缓存输入从 raw input 中漏报。
- 同一 response id 内容冲突、非整数/负数、cached > input、reasoning > output、时间非法、未知来源/归属等情况必须显式拒绝或计 telemetry gap，不静默计入。
- 日志内容、prompt、密钥、用户其它项目内容不写入报告；统计输出只包含必要标识、计数、窗口与来源绑定。工程用量不冒充账单金额。
- 比较应绑定命令/工作负载、代码、数据、环境与指标方向；不可比时记录 `INSUFFICIENT_EVIDENCE`。自然观测仅支持相关趋势，不自动断言因果。
- 根据政策限制每次扫描/解析资源，记录 cursor/progress 与 coverage；未扫描部分不能当成零用量。
- 复用现有 publication fence 和 validation runner；focused pytest 使用 `-n 16 --dist loadfile`，Full 只在最终集成候选自然边界执行。
- 不能用旧 SHA 的 PASS 替代当前候选必须执行的验证，也不能仅为降低成本跳过 DQ/PIT 或篡改校验。
- 现有 checkout identity 门禁可能阻断 telemetry。先披露 blocked receipt；任何读取主线证据/自动 post-stage 契约变更均独立评审，不自动切分支、stash 或创建替代 clone。

## 路径所有权

S1/S2 shared contract 于 2026-09-06 先串行冻结：`workflow_health_policy` 的 DEVX-013 可选段、`workflow_improvement_plan.v1` 和 `workflow_health_report.v2`；v1 历史报告继续可重验，新 policy 不能复用旧 policy 的当周 bundle。新报告使用 previous complete ISO week UTC，原 command/文件名/sole scheduler 均保留。Token collector 内部 API 为 `collect_workflow_token_usage(log_roots, project_roots, window_start, window_end, limits)` keyword-only；仅在已声明 owned 文件中并行实现 collector/tests，coordinator 负责其余 wiring。初版采用有界快照，达到预算显式 PARTIAL；增量 cursor 的实施依赖真实扫描成本观察，不能伪称已实现。

Coordinator 拥有 requirement、canonical task fragment/events/index/views、`config/architecture/workflow_health_policy.yaml`、相关 CLI wiring、`docs/system_flow.md`、runbook、report/catalog/architecture generated authorities、正式验证与集成发布。

拟新增独立模块位于 `src/ai_trading_system/reports/workflow_*.py`，聚焦测试位于 `tests/test_workflow_*.py`。实际新增文件及 generator/input/output 集在 publication transaction 中逐项声明；本文件的候选命名不扩大执行权限。

## 临时工作区生命周期

- owning task：DEVX-013；branch：`codex/devx-013-continuous-improvement`。
- absolute worktree：`D:\Work\AITradingSystem\run\ccra\devx013-continuous-improvement`。
- purpose：primary checkout 仍承载其它研究任务；从同一 exact main 在项目既有 ignored 临时 workspace 父目录中隔离开发、验证与收口。现有 task-owned sibling worktrees不用于本任务。
- 创建前先在 primary checkout 写入本 supporting requirement；创建后将完全相同的 requirement 转移到该 worktree，逐字节 SHA 复核后移除 primary 的本任务 untracked 副本。
- exit condition：最终候选进入 validated main 并普通推送；canonical evidence 已保留且 hash 核对；tracked/untracked/ignored 内容和进程依赖审计完成后，用 `git worktree remove` 清理并 prune。
- 所需运行证据保留到 primary checkout 的本任务 governed output 路径并验证。若仍存在唯一实现或未结束验证，则保留 worktree，并在本节记录具体下一负责人、风险和退出条件。
- 本轮不删除其它 task worktree，不读取、hash 或复制 known-unrelated exclusion。

## 状态与开放事项

- 2026-09-06：owner 授权推进；完成初步只读身份/租约审计。SINGLE_LANE preflight 仅因尚未登记任务和未声明路径 BLOCKED，按任务登记例外进入 S0。
- S0：完成。S1/S2/S4：代码及 78 项并行聚焦回归通过，待生成权威和正式候选验收。S3：已确认主线 TRADING-2564 提供 Full 前只读 readiness，复用并关联该 canonical task，不重复造检查器；本候选继续执行已有 readiness 和 ratchet 聚焦检查。S5：等待四个完整观察周，不得提前标记 DONE。
- 自动准入更多维护类别的具体 allowlist/资源上限、跨任务周期归因、真实收益门槛，需由首版观测与试点结果确定；当前不假定任意优化均有收益。

- 审查修复：拒绝 v2 内容降级到 v1、复制旧周 bundle 冒充当前周、其它任务/SHA 的测量错配，以及修改 outcome 后重算摘要冒充有效收益。验证保留 acceptance/observation 快照并独立复算。
- Scope 校正：implementation-v1 在任何生成/Full 前发现 task-source 测试路径误拼及 transitive registry fragment root 未声明。保留实现字节，以行政 scope correction 结束旧事务；新 source 事务声明正确测试、两个 fragment root、Atlas 关联路径与留存证据目的路径。没有越界生成或 Full 被执行。
- 按既有 Atlas exact-source / fence 顺序，先形成已聚焦验证的 source commit，再在同一 worktree 上以新事务完成 Atlas 与最终生成权威、正式验证、main 发布；不创建替代 worktree，不以 source commit 代替验收。
- 初版 token 采集是有界快照，增量 cursor 尚未实现。是否增加 cursor 由真实扫描成本决定；当前不得宣称已经降低 token 总消耗。

- 真实只读试跑（当前256MiB预算）：上一完整周08-24→08-31扫描59文件仅得PARTIAL/0响应，原因是新日志抢先消耗预算；09-05单日诊断得到618响应但同为PARTIAL。两次约1.2–1.6秒。已将窗口之后新建会话的元数据级排除、同身份重复metadata恢复纳入S1修复，不能将该样本当作完整周零消耗或项目总量。

- S1真实复核：首行真实aware创建时间已排除34个窗后来源，正式周扫描84文件，1.66秒；上周早期日志没有独立usage instrumentation且仍达到256MiB，所以保留PARTIAL而非零消耗。单日诊断629响应/1.47秒亦为PARTIAL，活动快照不可作因果比较。10项重复meta实为id变化，继续拒绝；同id/cwd恢复已有回归覆盖。57项collector并行回归与Ruff/Black/strict mypy通过。
- 收益验收额外修复：将run开始/结束、代码、工作负载和环境规范化为独立execution identity；同summary重新排版/换路径不能增加样本。实际执行的FAIL与非零exit保留为REGRESSION；PRINT_ONLY、身份错配、状态/exit矛盾仍拒绝。新plan或显式Token根变化不静默复用旧bundle；同日旧件保留，交由后续合法日期重新生成。
- 工程验证环境只补齐TRADING-2564 supporting requirement第S1验证依赖表已有的5份明确immutable证据；目的路径已声明，missing-only复制逐项SHA一致，源保留，无研究/DQ/provider/交易动作。
- 首版发布边界：功能与聚焦验证交付后使用BASELINE_DONE；四周真实观察、增量cursor/跨id日志准入、checkout阻断的长期解法和月度容量/局部重构复核继续留在本task。S3复用已有readiness，不能把本轮工程PASS或合成实验写成节省比例。

- 当前工程状态：`BASELINE_DONE`（source candidate）。88项并行聚焦回归47.55秒，七个实现/测试文件Ruff、Black和strict mypy通过。最终main准入仍取决于本候选正式tier/Full及publication closeout receipt；保留失败证据，不预报正式PASS。长期S5及真实覆盖缺口继续开放。
