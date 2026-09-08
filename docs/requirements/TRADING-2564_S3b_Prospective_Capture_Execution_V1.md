# TRADING-2564 S3b：五候选前瞻采集执行链

日期：2026-09-08（Asia/Tokyo）。状态：IN_PROGRESS；P1。
所属任务：TRADING-2564_LONG_TERM_RESEARCH_CAPABILITY_IMPROVEMENT_V1。
Owner：Project Owner / integration-coordinator；SINGLE_LANE，串行 consumer 合同波。
冻结 local main：9489d807f2fb6fd8795ccbadb62e7983b96a3709。
Owner 在 S3a 完成后要求继续推进长期建设。本波是本地工程实现与合成验证，真实业务动作全部为 0。

## 1. 前置验收与目标

S3a 已普通发布上述 exact commit；Full 11385 passed / 5 skipped / 641 warnings，四类正式验证
PASS。跳过节点及警告与 f60153c 基线一致。local main、origin/main、live remote 相等，事务
COMPLETED/RELEASED，已合并任务分支删除，执行根及 canonical evidence 保留。验收文件：
outputs/architecture/trading_2564_s3a_prospective_event_time/closeout_verified_v1.json。

当前生产缺少 DQ 子进程的可信父执行链：成功关联逻辑只在合成测试辅助代码存在。S3b 补齐
源码身份、严格 Named DQ、同 context 预览、完整输入保存、S3a 时间证据与父确认之间的生产连接。
沿用现有 bootstrap、S4D lease、contained immutable writer；不另建锁、发布队列或 scheduler。

## 2. 本波边界与依赖

只接固定 FIVE_CANDIDATE 家族：equal-risk、三个静态比较项、dynamic challenger。
复用原 target 算法与完整 2021-02-22..F 价格窗口；rates 参加严格 DQ，角色为 DQ_GUARD_ONLY。
原 55/57/59 manifest bytes、旧 run/verify 语义及旧价格/preview accessor、旧 preview DTO 的精确
pin 不扩权。新 profile 固定新 manifest path/SHA、worker 入口和完整 import closure。

Composer 仍是 2549 的主研究问题，五候选不替换该主线。Composer 的 2018-01-02 初始化、
QQQ/TQQQ/SHY 与 SGOV 分段、三 rates、504-session training、20-session label、rates 可知/修订
及 prequential 训练访问需要另行串行合同。本波不借价格权限覆盖；下一责任方是 source/PIT owner
与 coordinator。验收要求完整分段输入和执行时可知证据，2021-02-22 前初始化不进入 primary 结论。

不真实 activation/capture/DQ、manifest replay、preview、observation、maturity、scoreboard、下载/
cache mutation/provider、QuantConnect、Options、paper/live、broker/order/fill/position。
现有 heartbeat 保持原状态。真实启用前须复核新 bounded manifest 和运行范围，不消费 2563 已终止
授权，不要求 Owner 粘贴机器 SHA。授权与技术有效性分别记录；合成测试独立计数。

## 3. 最小执行合同

1. 新严格封套绑定 roots、exact candidate、固定 profile/policy、consumer family、RecordingPlan、
   定义、指定 immutable snapshot、动作上限、授权状态和终止条件。activation 不虚构 DQ as-of。
   SYNTHETIC 不进入真实研究；真实范围未审时阻断。
2. activate/capture 只在新 profile 下可用。父进程用现有 Git-byte bootstrap 全闭包编译，在该
   context 内严格恢复真实 S4D handle 并重验；CLI lease id 仅为寻址。源码、policy、calendar、
   lease 和输出归属动作前后通过。生产代码不得导入 tests fixture。
3. 固定 production parent 启动一个隔离 DQ child，canonical DQ 最多一次。记录原 request、
   解释器 launch、真实 PID、stdout/stderr、返回码、pre/post guard 与 terminal 时间。
   严格 PASS、exit 0、相同 source identity 才生成原 v1 成功关联；关联 PASS 不是 DQ PASS。
   失败/崩溃保留 counters/bytes，不自动重试、更换 snapshot。
4. 父在自身 NamedExecutionContext 内 zero-DQ verify，以新专属 accessor 及原纯 helper
   生成预览，结果采用新 DTO 并复用 candidate DTO。旧 accessor/DTO 仍只接受原 pin。
   不跨 PID/JSON 重建 VerifiedNamedInputs；新 factory 明确封存同 policy/calendar witness。
5. 完整记录闭包包括全部 receipt.inputs（rates/参加 DQ 的 secondary 不得漏）、pointer/
   transaction/manifest、policy/calendar/registry、request/receipt/成功关联及真实 preview。
   新记录 accessor 只复制已验证闭包，不改 common evaluated window 或签发 rates 特征权限。
6. S3a 原样建立 activation→inputs→signal；外层 adoption 新增准入，旧 S3a false 字段不改。
   F/R/D 分开；D=next_XNYS(F)，首收益 Close(D)→Close(next(D))，不在 D 重算 target。
7. signal recorder 返回后父内部采 UTC/monotonic 并写不可变 ACK；完整 witness 被观察完成且
   严格早于 D close 才能准入。ACK 不证明自身此前 durable；缺 ACK 不补签旧 time；UTC 倒退拒绝。
   activation 的实际首合法 F 按完整 activation ACK 的纽约日期之后首 XNYS 求得；若跨午夜，
   保留 S3a 原 first_feature 语义并由新 admission 加严。
8. 固定 key/slot 的幂等、INCOMPLETE/GAP 保留；半完成不清除、不重签。retained verifier 独立
   核验全部 bindings/ACK/closure。新 admission 必须有满足所有条件可实际准入的正例；不得永远 false。

仅证明指定本地 bytes、实际执行关联和及时记录，不证明 provider 历史 available_at、策略有效性、
未触碰 holdout 或 production 准入。可信初始 bootstrap/host 仍是明确假设。

## 4. S4 首次结果访问与会计依赖

真实启用前准备完整实验封套；首次对应 outcome 访问前冻结 hypothesis、候选、comparator、成本/
持仓会计、主指标、sample/episode、首次/重复查看、停止、已看历史和失败变体。数字门槛待审，
不搬旧 pilot 20 observations 或单 LONG episode；本波不访问新结果。

旧五候选 maturity 为 cost_bps=0 的整条策略路径重算，底层 targets.shift(1)*pct_change、费用
target 差 half-L1。Composer evaluator 为自筹资金 cash/shares、单边 notional 成本和 terminal
liquidation。统一 bps 不会统一净收益账本。S4 必须明确动态路径或单信号固定持有、漂移/调仓、
入场/期末/清算、暴露匹配 comparator 与 carry；下一责任方为 research policy owner/coordinator，
退出条件为首看前冻结及独立会计对账。

结果门禁应在 maturity 的市场读取/计算、scoreboard/报告读取、Composer training label 访问前。
现 indicator_research trial ledger 是说明 DTO，缺 append-only trial/attempt/access 服务，仍由本
umbrella S4 负责。Composer 自动 refit 应预注册为 prequential，不能将已用训练区间称为未触碰 holdout。

## 5. 顺序与验收

- task/需求、START/LANE preflight 和 fence 在实现之前。先冻结新 API，再分工实现。
- 独立 worker 仅拥有 production DQ parent helper/测试；coordinator 拥有 bootstrap/profile/DTO/
  接线、shared authority、文档、生成、正式验收与发布。只读 reviewer 核验身份/时间/权限。
- 16-worker focused tests：新旧 profile 拒绝矩阵、source/root/PID/context/lease drift、真实 child
  FAIL/WARN/exit/timeout/UNKNOWN、全闭包/secondary、T/T−1、完整窗口、算法逐值等价、F/R/D、
  DST/午夜/假期/半日、晚到/崩溃/并发/幂等/ACK 缺失及 retained 读取不得升级 authority。
- fresh exact-candidate 合成输入测试实际运行 Git-byte bootstrap、production DQ child、
  canonical validator、同 context 真 seal/preview/完整闭包 writer 与真实 lease；另以普通 CLI
  在真实主机时间运行 SYNTHETIC_ENGINEERING activation 与同 key 重复读取。保存实际 PID、
  launch/probe、请求、前后 source/fence/lease 和时间证据；不伪造 seal 或源码身份。
- activation→CAPTURED/LATE 的多日期正例使用独立合成 checkout 的真实 S4D/S3a/writer 和
  明示时钟/DQ/verification 结构替身，不 mint seal。真实时间完整 capture 至少要等 activation
  纽约日期之后的 F 收盘；当前工程测试不能立即证明该运营事实，不能回填原 lease/ACK 时间。
  因此 actual-source 组件链、实际 activation 和合成跨日准入分别验收，后续真实范围 review
  后才按实际时间另验完整 capture；该剩余依赖归本任务 S3，不冒充已完成真实前瞻。
- source/final 官方生成、exact candidate formal tiers/Full、普通 main 发布与清理；Full 仅在
  自然集成边界。旧 failed evidence 不改写，不降低派生 ratchet 或真实 DQ/PIT 门禁。

## 6. 工作区与保留

复用 D:/Work/AITradingSystem_devx014_source_preservation，分支
codex/trading-2564-s3b-prospective-capture-v1；不新建 worktree/clone/cache。
canonical 证据 outputs/architecture/trading_2564_s3b_prospective_capture；正式证据
outputs/validation_runtime。保留 S2b/S2c/S3a 及唯一源码保全链，不碰 known-unrelated 内容。
发布后审计 ignored/untracked/unique/process，再清理 merged branch；根继续为 canonical 执行/
证据目录。退出条件为依赖结束、所需 bytes 在治理归档可复验、无唯一实现/活动进程。

## 7. 进度

2026-09-08：两路独立只读审查确认生产父执行缺口、完整输入闭包、旧权限边界、Composer 分段/PIT
和 S4 会计依赖。START/LANE preflight PASS，source 事务 TASK_SOURCE_PRE_WRITE。
S3b prototype 已实现，正在联合验证；真实业务动作全部 0；umbrella 保持 IN_PROGRESS。

2026-09-08 进度：固定新 85/12 profile、strict manifest/owner-review/request/ACK、production
DQ parent、同 context seal 与完整输入保全、S3a adapter 和只读重验已接通。旧 55/57/59 profile
pins 不变。owner review 位于 manifest 专属 control 路径，由可信 operator 提供且不冒充签名。
输出根与原 publication/source-output 全树必须相互隔离，拒绝跨角色覆盖。

审查修复：闭包使用紧凑 canonical bytes，保留 rates/secondary 与原父 request/stdout/stderr/
前后 guard；原 intent/event raw bytes、PID、source identity 与 ACK 一致性独立重验。UTC/
monotonic 外包络从 DQ 前开始，完整 witness 与 source/lease postguard 后才发布 ACK/result。
父 proof 按原检查时间重验，因此合法 heartbeat extension 不被旧过期时间误拒绝；它仍是本地
可信 host attestation，无法证明外部不可篡改时间。signal 从已关闭价格/registry/日历全量重算，
比较全部 candidate weights、R/F/D、窗口、环境和身份字段。失败/LATE/成功的状态字段、所有
admission 和 counters 严格核验；未知子调用次数不以父进程零调用替代。已用 key 在恢复 live
lease 前只读检查，过期后返回原完整结果或 INCOMPLETE，永不重新 dispatch。

当前聚焦结果：strict capture DTO 229 passed；新/旧五候选算法 106 passed；DQ parent 首轮
49 passed，新增 retained lease 原时间/heartbeat/release/HEAD 漂移与篡改测试 23 passed。
均为 16-worker loadfile 合成测试。coordinator core、全闭包联合验证、fresh exact-candidate
bootstrap E2E、共享生成链和 formal/Full 尚待完成；不将这些局部结果宣称为实际采集验收。

后续联合结果：旧 bootstrap/新旧合同与 S3a 合同 498 passed；Named 执行/metadata 157 passed、
原 Named contract 79 passed；DQ parent 74 passed，core 12 个场景通过，包含真实 writer 的
CAPTURED、同 key replay、所有关联 hash 重签的信号攻击以及 payload TIMELY/ACK 在 D close
恰好到达导致 LATE。全部为 16-worker loadfile 合成验证；fresh exact-candidate 两类测试已写，
须 source commit 与新 exact-head lease 后运行，当前尚未执行。

最终故障审查补充：child 已结束而 parent/successful-dispatch 写入失败时，由原始 helper 调用栈
携带 typed terminal observation，保留已知次数与已成功写成的 parent binding；不重派 DQ，
不在异常后补发成功关联。result 的完整投影/证据核验先于不可变发布；无足够 parent 证据时
保留 INCOMPLETE 和异常中的已知计数，不先发布一个无法复核的终态。

本阶段最后验收以 canonical runtime 收据为准：同一 outputs/architecture/
trading_2564_s3b_prospective_capture 下保留 source_commit_handoff_v1.json、final_readiness_v1.json、
actual_candidate_e2e_v1.xml、formal_tiers_v1.json、final_validation_acceptance_v1.json、
main_publication_v1.json 和 closeout_verified_v1.json。这些文件须由实际通过的执行生成，本文
列出路径不代表文件已存在或验证已通过。最终普通发布应验证 candidate=local main=remote main，
仅删除已合并且无唯一实现的任务 branch；执行根及 canonical 合成/验证证据继续保留。

## 8. 真实时钟 Full 失败与修正接续

2026-09-08：上述候选 `12ec8c386` 的 actual-candidate v2 两项测试及四正式 tiers 通过，
Full 则为 1 failed / 11811 passed / 5 skipped / 641 warnings。失败 activation 原 key 为
`actual_source_activation_20bd65d147be43a5bcc937bb0e56766a_v1`，返回
`BLOCKED / TEMPORAL_CLOCK_BACKWARD`；保留记录，不重试或补签，main 未发布。

已登记[主机时钟证据修正](TRADING-2564_S3b_Clock_Evidence_Correction_V1.md)。共享时间
合同先行，再同步 recorder、parent ACK、lease/manifest/deadline 和 source closure。
旧 Full 以 FAILED 终态释放 lease，修复重验须明确绑定其 parent。前文列出的
`final_validation_acceptance_v1.json`、publication/closeout 路径不代表已通过；后续版本
收据必须清楚关联此次失败与修正后的 exact candidate。

2026-09-08 修正进展：C1–C3 的固定 raw UTC/QPC 合同、recorder v2、ACK/result v2 单一提交
已实现；三项独立终审发现修复后，116 项针对性测试与两名 reviewer 的精确源码复验通过。
独立结论及 SHA 见 `clock_independent_review_v1.json`，修正前 608-pass 合并批次保留独立身份。
source profile 为 87/14，旧 55/57/59 bytes 不变；当前 source/shared 全范围验证与 C4 尚未完成。
不得沿用旧候选的 actual/Full 结果作为新候选通过证明，真实业务动作仍全零。

2026-09-08 C4 正式验收：修正后的 `38a0a689` source/shared 为 1,037 / 435 passed，actual
candidate 2 passed、四正式 tiers PASS；新 Full 11,986 passed / 5 skipped / 641 warnings，
与旧失败 parent 绑定并保留原证据。`clock_final_validation_acceptance_v1.json` 核验 skip/warnings、
exact source 与全部证据；`clock_main_publication_v1.json` 和 `clock_closeout_verified_v1.json`
证明普通 push 后 candidate/local/remote 相等、事务 COMPLETED/RELEASED、merged branch 已清理。
上述收据均位于本节同一 canonical runtime 根。本阶段工程完成；真实采集、S4 和 S5 未完成。
