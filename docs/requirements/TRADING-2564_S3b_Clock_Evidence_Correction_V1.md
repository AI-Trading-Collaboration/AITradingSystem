# TRADING-2564 S3b：主机时钟证据与截止准入修正

最后更新：2026-09-08

- task：`TRADING-2564_LONG_TERM_RESEARCH_CAPABILITY_IMPROVEMENT_V1`；priority：`P1`；status：`IN_PROGRESS`。
- owner：integration-coordinator；mode：`SINGLE_LANE / contract-change`。
- frozen local main：`9489d807f2fb6fd8795ccbadb62e7983b96a3709`。
- 未发布的 S3b source：`12ec8c386dcac217673a85ec755b6d2314f9acfb`。
- 当前阻塞：该候选 Full 的真实时钟合成 activation 被旧跨时钟比较阻断；修复、独立复核和正式重验尚未完成。
- 下一责任方：coordinator 完成最小时间合同修正；真实使用仍由 owner 单独审查。

## 1. 已观察失败与不能推断的事实

`outputs/validation_runtime/trading-2564-s3b-final-v1-full-20260908/test_runtime_summary.json`
记录 1 failed、11811 passed、5 skipped、641 warnings，4857.97 秒；summary SHA-256 为
`c785157d5e49151491fe5dde28954edf5cdf0201db5c95e0b2fcf6b37eb5666f`。
唯一失败是 `test_exact_candidate_real_clock_synthetic_activation_and_read_only_duplicate`。
该次 `actual_source_activation_20bd65d147be43a5bcc937bb0e56766a_v1` 返回
`BLOCKED / TEMPORAL_CLOCK_BACKWARD`，保留 intent、payload 和 BLOCKED result，但没有 completion
witness 或 parent ACK。DQ、真实 activation、capture、provider、order、fill 均为零。
原 attempt 不重试、不补签、不清理。失败 Full 和 FAILED publication transaction 为不可变历史。

本机 CPython 3.11.9 的 `monotonic_ns` 使用 GetTickCount64，reported resolution 为 15.625ms；
`time` 使用 GetSystemTimeAsFileTime，`perf_counter` 使用 QPC。独立短时 stdlib 采样复现了
UTC 未倒退但旧 `UTC elapsed + 1µs >= monotonic elapsed` 判据失败。纳秒整数和微秒 datetime
是表示单位，不能证明底层分辨率或两个独立时钟的速率关系。仅改用 QPC 也不足以修正该推论。
原失败没有保留完整的开始/结束计时 tuple，因此不能把具体失败逐字节确定为量化误差，亦不能
据此宣称当时发生了真实 UTC 回拨。本次修复针对已复现的不成立的合同假设。

协调者的 bounded stdlib 复现保留在 `outputs/architecture/trading_2564_s3b_prospective_capture/host_clock_diagnosis_v1.json`，
SHA-256 `72ff2dd3bac190e5bd9341aad04287579abae68a79ed339226753a4f12b10793`。
128 个 monotonic 样本中 13 次、128 个 QPC 样本中 24 次触发旧比较，raw UTC 倒退均为 0。
该采样未导入业务模块、运行 DQ/activation 或调整系统时钟。

技术依据：[CPython 3.11.9 pytime.c](https://github.com/python/cpython/blob/v3.11.9/Python/pytime.c)、
[Microsoft QPC 与量化误差说明](https://learn.microsoft.com/en-us/windows/win32/sysinfo/acquiring-high-resolution-time-stamps)。

## 2. 修正后的受信主机模型

本协议继续信任记录器及本地主机时钟，不建立外部 UTC 签名、物理 UTC 绝对准确度、所有隐藏
调时的检出能力或 provider PIT。`hidden_host_adjustment_proof=NOT_ESTABLISHED` 必须可见。
真实业务启用仍 BLOCKED 于原 owner scope，不以本工程修复签发权限。

1. 原始 UTC 顺序与单调持续时间分开校验。UTC 起止、前驱、lease/witness/postguard 的原始
   UTC 不可倒退；counter 两端及其差值必须为严格整数、非负且同一已批准 provider。
2. 新测量用 outer bracket：counter-start → 原始 UTC-ns anchor → 全部受控工作 → 原始
   UTC-ns observation → counter-end。从父 DQ 前固定一次起点，后续 checkpoint 不重新锚定。
3. 固定批准的 Windows CPython UTC/QPC provider，记录 implementation、reported resolution、
   monotonic/adjustable 属性及两端原始值。未知 provider 不自动等价迁移。reported resolution
   不被解释为物理 UTC 准确度；不引入主观毫秒宽限、倍数容忍或经验采样校准。
4. 保留 UTC-ns 避免 datetime floor 截断丢失上界。counter 量化采用所记录 provider 模型的
   两端向外界限；纳秒转微秒只作确定的向上取整。常数 2 表示两个采样端点，1000 表示单位
   转换，均不是投资或时钟漂移阈值。具体公式先在共享纯合同中冻结并独立测试。
5. 派生 admission bound 为 observed UTC 上界、最初 UTC anchor 加完整 counter 包络、
   上一 checkpoint bound 和已验证子事件 bound 的最大值。它是该受信模型下的保守准入值，
   不覆盖任何原始 timestamp，也不宣称独立的物理 UTC 时间证明。
6. raw UTC 比较仅针对 raw UTC；后续 raw UTC 小于先前派生 bound 不能冒充 CLOCK_BACKWARD。
   bound 只用于上限准入并单调传递。须同时保留实际 live source/lease 验证。
7. capture 要求 bound 严格早于 D close；lease 和 manifest 的 expiry 同样使用 bound。
   权限生效下限仍按真实 raw start 验证，不把未来派生值作为当前时钟传给 lease API。
   若 start=D−10s、raw completion=D−5s、elapsed=20s，结果仍不得准入。
8. activation 的最早 F 从完整父确认 bound 的纽约日期之后取首个 XNYS session，并不得
   早于 recorder 的结果。父 ACK 至少包含子事件 bound，后续 capture 继承该最终 F。
9. ACK 只声明其所覆盖的 recorder/witness/postguard 范围；不宣称 ACK 自己在较早时刻持久化。
   每个阶段的采样终点、权限重查与保留验证必须一致。
10. v2 采用单一最终 result commit 点。所有决定准入的 source/lease/manifest/clock/projection
    检查在最终 immutable result 写入前完成，立即 precommit 检查须证明原 scoped lease active。
    不保留一个未绑定 retained replay、却能在 result 写入后推翻 admission 的 fatal guard。
    这明确修订旧“每次写后仍 active”的无限完成证明边界：本协议不声明 final result 自身
    写完时 lease 仍 active，也不声明其先前 durability。commit 后的读取/返回失败意味着
    交付未确认，不能冒充原调用成功；外层 bootstrap source terminal guard 继续保护交付完整性。
    已提交证据的后续只读 replay 不因其原 lease 后来过期而重做业务或改写原准入事实。
11. ACK 的 covered-through bound 覆盖 recorder/witness/postguard，用于 D 截止和 activation
    首 F；后续 terminal precommit bound 从同一 anchor 延伸，用于 lease/manifest 到期和提交
    权限。最终 projection 重验和 result 自身不冒充此前已在 D 前持久化。不能先冻结 timely
    ACK，再用未声明的更晚截止把 result 改为 LATE 而使二者矛盾。

## 3. 版本与兼容

新 completion、父 ACK 和 clock evidence 显式版本化；旧 v1 的 bytes、原 inner elapsed 语义
及保留验证规则不重新解释。旧 v1 失败不能借新规则变成 PASS。旧 timing policy 保留，新写入
使用显式新 policy；S3b source profile、运行依赖和封闭输入链同步绑定实际新增模块与政策。
原冻结研究 producer、收益窗口、DQ/PIT、投资阈值、既有 observation ledger 不变。

C2 completion 继续只证明其冻结的 payload durable / witness-write-start 范围，
`observation_authorized=false` 且不声明 completion 自身的持久化时间。recorder 的写后检查
可使原调用未确认返回；只读 replay 不能把该原调用改称成功。C3 只能在本次 recorder 实际
成功返回并完成父 postguard 后生成 ACK，不得用孤立 retained completion 补签。

## 4. 分步执行与验收

|步骤|交付与依赖|验收|
|---|---|---|
|C1 共享时间合同|纯 DTO、严格 replay、固定 provider sampler、新工程 policy|raw/derived 分离、outer bracket、整数取整与量化边界、冻结 anchor、metadata tamper、未知 provider 负例；先完成此合同再迁消费者|
|C2 recorder 迁移|依赖 C1；新 completion/policy，保留旧 v1 verifier|粗时钟交错可记录；真实倒退仍拒绝；20s/5s 跨截止不得准入；lease、前驱、原始残片和兼容测试|
|C3 父执行与 ACK|依赖 C1/C2；父起点在 DQ 前，bound 跨全链传播|严格 deadline/expiry 相等拒绝、raw 尚及时但预算耗尽、子 bound 传播、纽约午夜 activation、原 key 只读 replay、完整 source closure|
|C4 集成验收|登记/图/生成 authority 同步，精确新候选|16-worker focused、真实源码/真实时钟 synthetic E2E、正式 tiers 与 Full；Full 使用 failure_fix_rerun 并绑定上述失败 parent；之后普通 main 发布|

不得以 mock 时间替换真实时钟 E2E、删除其正向断言、反复运行直到偶然 PASS 或关闭时钟门禁。
本修正为已复现合同错误的直接修复，不是临时 workaround。若 provider 或完整权限界限不能
证明满足本合同，保持 typed BLOCKED 并记录具体原因。

## 5. 工作区、证据与剩余工作

复用 `D:/Work/AITradingSystem_devx014_source_preservation` 和当前 S3b task branch，不创建替代
worktree、clone 或 cache。该路径仍保存唯一受治理证据；沿用 S3b 主需求的保留 owner、退出
条件和清理审计。仅在修复验证并发布后删除已合并 branch 引用，证据不在本次删除范围。
旧 Full、失败 attempt、原先 actual E2E PASS/FAIL 均保留各自 exact source 绑定。

2026-09-08：失败 Full 已保全，旧 publication 为 FAILED / lease RELEASED；修复 source transaction
与 LANE preflight PASS。本需求登记后开始 C1，C2–C4 尚未完成。Umbrella 保持 IN_PROGRESS，
Composer 分段/PIT、真实 S3、S4 首次 outcome 访问合同和 S5 效率改进继续保留，不以此次修复完成
整个长期能力建设。所有本次真实业务动作仍为零。

2026-09-08 C1 进展：四个共享合同/runtime/test 文件已实现，16-worker 聚焦测试
`clock_c1_focused_v2.xml` 为 98 passed；Ruff 与 Black PASS。首轮 88 passed / 6 failed
保留在 v1 XML；五个失败为非规范 float 字符串测试样例，一个为诊断断言误用前一次 UTC。
修正测试预期后重验，未放宽准入公式。协调者复核并以真实主机采样 16 个 checkpoint，
原始证据与源码/测试哈希记录于 `outputs/architecture/trading_2564_s3b_prospective_capture/clock_c1_review_v1.json`
（SHA-256 `27d217ed177c6e74bf51654f8f77dd91b496024df538bf56edda05a0a0cb4a91`）。
该诊断不运行 DQ、activation 或 capture，也不是实际 source-attested 或投资证据。

两个子任务因账户用量限制中断；独立实施终审尚未完成，不将中断冒充 PASS。C1 的固定
API/政策/算法已由协调者复核并冻结供本 lane 的 C2/C3 迁移使用；最终独立复核、完整
consumer 边界测试、actual candidate 与 Full 均仍是发布前的未完成门禁。失败 sampler
保留 prior evidence 和已采到的完整/部分原始读数，其 diagnostic 明确不可准入、不可续跑；
消费者必须保留该诊断而不能另建 sampler 重试同一事件。

C2 接口审计补充：`exclusive_store_maintenance` 的 root authority 禁止持锁期间跨 root
读取政策；C1 初稿在每次采样中打开 source policy，与此必要保护冲突。直接修正为：sampler
初始化及显式 `recheck_policy()` 在锁外验证精确政策 bytes；checkpoint 只读固定硬件时钟
及 provider metadata，不读文件。消费者须在每个写入事务前后于锁外复核政策，且后续
checkpoint 始终延伸原 anchor 以覆盖这些检查与锁等待。clock DTO 本来就不授予 source、
lease 或 policy-live authority；消费者 retained proof 仍须绑定实际 policy 与源码。
不修改或绕过目录 authority，不添加第二把锁。C1 的接口修正及针对锁内无文件 I/O 的测试
完成前不开始 C2 实施；上述 98-pass 证据仅对应修正前的明确源码哈希。

该接口修正已通过 `clock_c1_focused_v3.xml`：100 passed / 16 workers，包含实际 store
transaction 内禁止 sampler 文件 I/O、锁外政策漂移拒绝、provider 中途漂移及原始残片。
更新的真实主机诊断和源码绑定在 `clock_c1_review_v2.json`，SHA-256 为
`31b5742a5ff877b6f927aba2786593cb4c8460a31e7c9438ab2442e492101499`；独立终审仍待补齐。

C2/C3 的完成范围另外分清两类数据：completion 中的 clock evidence 截止于 witness
precommit，payload checkpoint 的 bound 决定 payload 状态；成功的原 recorder 返回还带有
延伸同一完整 prefix 的 `return_clock_evidence`，覆盖实际 witness 写后检查。它不改写原
completion，普通 retained verifier 不重新采样或补造这一返回观察。C3 必须将该返回证据
绑定进父 ACK，验证它是原 completion clock 的严格延伸，并传播其中最终 bound；只有
成功原调用提供这一字段，已存在 witness 的只读 replay 不具备为新 ACK 补签的资格。

2026-09-08 C2 已通过 `clock_c2_focused_v2.xml`：215 passed / 16 workers，Ruff/Black PASS。
首轮 200 passed / 4 failed 保留；修正原返回观察与 replay 的比较，并把前驱 locator 校验
恢复到 existing-witness 分支之前。新增 v1 原 inner 数学边界、禁止 v1 新写和混用、真实 raw
回拨诊断、粗时钟、lease 预算耗尽、原 return prefix/expiry/只读复验。结果与源码哈希在
`clock_c2_review_v1.json`（SHA-256 `46f0e7ff590184bbd337d014ab7d21aa832c0602947d6714f700f124ca3a8a50`）。
canonical task event 已更新，C3/C4 和独立终审仍未完成。

C3 的 ACK 须保留每一次原 recorder 返回：activation 为一个 return observation，capture
为按 inputs、signal 顺序的两个 observation。各 observation 绑定原 event 与完整 return
clock prefix；父对应 checkpoint 必须继承该子 bound，其余 checkpoint 不接受任意外来
bound。`verify_recorder_return_evidence` 仅从已有 bytes 重放该前缀、阶段和原 lease 上界，
不获取 live lease、不重采样。S3b 源码清单更新为 87 模块 / 14 依赖，SHA-256
`9a11ed94e1c318aee3c44a7d01ba0f31556bd4de355eef1f75893245fe8bc1ce`；包含旧 timing policy
作为 retained-verifier 依赖，旧 55/57/59 清单与 capture v1 policy bytes 保留。

2026-09-08 C3 首轮父流程验证为 286 passed / 3 failed，保留
`clock_c3_runtime_focused_v1.xml`。两个旧断言仍要求写后 fatal clock guard，一个 rehash
攻击样例未同步新增 recorder-return binding；按已冻结 v2 单一提交合同修正测试，同时补充
粗 raw 时钟/正 QPC 时长、counter 越过 D、terminal 复验延迟与 lease 耗尽、最终 proof/clock
篡改及只读无采样复验。六个生产源文件 strict mypy PASS；额外测试文件探针发现样例类型
标注问题，须修正标注后单独重验，不能冒充该更广范围已通过。

独立终审已恢复，并定位三项修正：v2 recorder 前驱与新 anchor 必须直接比较 raw UTC ns，
不能先 floor 到微秒；每个子 recorder anchor 必须由紧邻之前的父 checkpoint 的 raw UTC/QPC
约束，而非只包在全局父 anchor 之后；未执行采样的线程归属拒绝不能把上次成功读数归到
本次失败，重入期间的读数须标明真正的原采样阶段。这些属于 §2 原始时序/失败审计要求的
实现遗漏，协调者修复并补负例，独立 reviewer 复验后才进入源码冻结与 C4。

2026-09-08 C1–C3 实施与独立终审已通过：修正前合并验证为 608 passed
（`clock_c1_c2_c3_focused_v1.xml`），三项独立发现修正后的针对性批次为 116 passed
（`clock_independent_findings_focused_v2.xml`，16 workers / loadfile）。对应首轮 115 passed /
1 failed 的 XML 保留；仅剩余旧诊断断言相互矛盾，修正后通过，未放宽生产校验。
此前测试文件类型标注探针的问题亦已修正，八个生产/入口测试文件 strict mypy PASS。
两名独立 reviewer 的最终源码 SHA 与复验结论保存在
`outputs/architecture/trading_2564_s3b_prospective_capture/clock_independent_review_v1.json`，
SHA-256 `769336c3f4ea9fdf47a1813656a33749c7c28e34d364b94bea577822b42a8654`。
静态 source closure 为 87 模块 / 14 依赖，无缺失项目 import；旧三个 profile bytes 均未改变。
扩大源码聚焦验证已完成 1037 passed / 16 workers；共享 authority 以未发布的 S3b successor
绑定本次 58 项源码闭包。不改写封存 legacy prefix 或既有 runtime 证据；current generated
各阶段按既有 authority 规则重建。共享验证首轮 434 passed / 1 failed，失败为新增两个时钟
模块及两个测试文件后，current deprecation inventory 的旧指纹/计数未同步；本次只更新
精确库存期望为 1216 模块、1379 测试文件，writer 数量 856 与 removal 门禁保持原合同。
首轮 XML 保留，修正后重跑共享验证。C4 actual candidate、正式 tiers、失败 parent Full 与
发布仍未完成。
