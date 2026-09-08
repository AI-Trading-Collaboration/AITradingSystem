# TRADING-2564 S5：可信 Full 耗时来源与 advisory seed 更新

最后更新：2026-09-08

- stable task id：`TRADING-2564_LONG_TERM_RESEARCH_CAPABILITY_IMPROVEMENT_V1`
- priority：`P1`；status：`IN_PROGRESS`；mode：`SINGLE_LANE`
- owner / next owner：integration-coordinator / validation_operations
- frozen local-main：`38a0a689f8c4890760f40c1fedc4bd648a3bbb92`
- authority：既有 umbrella S5、ARCH-004G2.4-EB4 的机械 `PARTIAL_SEED` refresh 规则。
- production effect：`none`；broker action：`none`

## 1. 问题、范围与已完成前置

S3b 的时钟修正已通过精确候选、四正式 tiers 和 Full，并普通发布到上述 commit。
Full 为 11,986 passed / 5 skipped / 641 warnings，覆盖 11,991 nodes、1,318 files；
执行耗时 3,467.54 秒。source/shared focused 为 1,037 / 435 passed，actual candidate 为
2 passed；skip nodeids 和 normalized warning lines 与保留基线相同。local/remote/candidate
相等，事务 COMPLETED/RELEASED、merged branch 已清理。验收位于
`outputs/architecture/trading_2564_s3b_prospective_capture/clock_closeout_verified_v1.json`。

当前 v24 seed 仅含 1,108 个旧文件。已保留的失败 Full 可以诊断长尾，但不能成为新 seed 的
通过来源。`refresh_partial_duration_profile.py` 当前仅检查部分 PASS 标签、计数及 sidecar
hash，尚未复用正式 runner 对原始 nodes/phases/files/workers、调度顺序和 provenance 的
完整重算；它还允许读取器明确拒绝的零耗时行。这些缺口须先直接修复，再更新 seed。

本波只修复证据准入、复用已有遥测并生成 advisory 文件耗时数据。保持 16 workers / loadfile、
全部 nodeids、文件内部顺序和 `--no-loadscope-reorder`。不另跑 benchmark Full，不声明稳定
提效，不改投资阈值、策略、DQ/PIT、研究窗口、运营 scheduler 或发布事务协议。
真实 DQ、manifest replay、activation/capture、outcome/maturity/scoreboard、下载、cache、
provider、QuantConnect、paper/live、order/fill 等动作全部为 0。S3 真实接入、Composer、
S4 首看前协议/归因及 S5 其余长期事项仍由原 umbrella 保留。

## 2. 精确来源与准入合同

唯一待准入来源为
`outputs/validation_runtime/trading-2564-s3b-clock-final-v1-full-20260908/`：

|输入|SHA-256|
|---|---|
|`test_runtime_summary.json`|`0b31c4bea268f63024ba69215665e3f2db5e9777dfe887bb1d1b41a6f8552ad4`|
|`test_runtime_profile.json`|`578586cb250697df54e16c684cf4d7986cb2c8355291f20329c66986b65ab619`|

1. 严格解析 JSON，拒绝重复 key、非有限数值、错误 schema/report_type、bool 冒充整数、非执行、
   benchmark、错误 Full tier、非零 pytest exit 或非 PASS summary/profile/telemetry/provenance。
2. summary 和固定同目录 profile 必须属于一个明确 run；检查路径归属、唯一 inventory、原始
   byte size/SHA、summary projection 及相同完整 validation provenance。不能从两个 run 拼接。
3. source commit 为 summary.git_commit，使用 Git 精确读取该 commit 的 duration manifest 和
   runner 固定 full-test manifest 到内存。duration bytes 还须匹配原 summary input inventory、
   input_checksums 和 profile scheduler 的 manifest SHA/metadata。
4. 原 summary 未把 full-test manifest 列入 input inventory，不能虚构该历史字段。其身份绑定
   固定路径的 source-commit Git bytes，再重算完整 collection file coverage。
5. 历史 locator 按原 summary working_directory 做纯词法核对，不对历史路径执行当前文件系统
   resolve/stat/read。Git source bytes 和原 locator 不得被当前 seed 或当前 generated 清单替代。
6. 正式 runner 的 live API 保留；读盘适配层一次捕获 raw bytes，纯校验核心从同一 bytes 解析
   和求 hash。新增历史字节入口强制接收 duration/full-test bytes，并复用同一套 node-derived
   完整性、聚合、worker、coverage、调度 metadata/order 和 provenance 重算。
   禁止通过 `duration_profile_path=None` 绕过历史 seed 绑定；不复制第二份弱校验器。
7. 每个来源文件恰好一行，正数、有限 duration 和合法 node_count；源计数、逐节点 phase、
   逐文件和 worker 聚合必须可重算。零耗时必须 fail closed，不注入 epsilon 或默认权重。
8. 新 seed 仍为 `arch_004g2_full_duration_profile.v1 / PARTIAL_SEED`，机械保留全部来源 rows，
   duration 降序、同 duration 维持来源 first-seen 顺序。记录 summary/profile/source Git 两份
   manifest 的可审计绑定；source commit 指向历史 Full 的代码，不嵌入本次最终 candidate SHA。
   不写 COMPLETE-only 证据，不将后续 collection 变化伪装为当前完整实测。
9. 默认 dry-run，显式 `--write` 才原子替换受治理的目标 manifest；所有准入失败必须发生在
   输出写入前。替换当前 seed 后，历史复核仍从 source Git bytes 进行且结果不变。

## 3. 分阶段实施与验收

|阶段|依赖与交付|验收|
|---|---|---|
|E0 当前登记|S3b closeout、只读设计审查、source transaction、canonical task update|范围/归属明确；LANE preflight PASS 后才实施|
|E1 共用字节校验|E0；live 读盘 adapter、strict bytes parser、单一 pure validator|旧 live 调用链保留；历史输入必填；错误 bytes/locator/coverage/provenance 反例阻断|
|E2 来源准入与生成|E1；严格 summary/profile/source Git 绑定、正耗时约束、审计投影|同 run 正例通过；重签 aggregate、拼接、错误 commit/seed、伪 PASS、零/NaN/bool、重复 key/path 等失败且不写 output|
|E3 实际 seed 更新|E1/E2 独立复核及 focused PASS；唯一真实 PASS Full 源|只读准入后 dry-run/write 一致；1,318 rows / 11,991 source nodes；新 manifest 后历史验真不依赖新 bytes|
|E4 正式验收与发布|E3；官方生成顺序、精确 source/final candidate|四正式 tiers 和一次自然 Full；普通 ff/push 与 SHA 相等；释放租约并审计清理|

用 16-worker loadfile focused pytest 验证生产校验路径及必要负例，不能以仅自检输出相等代替
tamper/lineage 覆盖。独立 reviewer 核对 exact source bindings、node-derived 重算、旧 live
行为及 dry-run 零写入。正式 Full 仅在最终 candidate；如失败保留原产物并按已审规则重验。
当前运行与之前运行集合、环境和代码并非控制实验，耗时差只作描述，不作稳定因果提效结论。

## 4. 工作区、运行方式与生命周期

复用 `D:/Work/AITradingSystem_devx014_source_preservation`，从上述 exact main 创建
`codex/trading-2564-s5-duration-seed-v1`，不新增 clone/worktree/cache。实现与 generator 使用
该根的 `src` 作为显式 PYTHONPATH，并核对实际导入来源；Python 为
`D:/Work/AITradingSystem/.venv/Scripts/python.exe`。canonical runtime evidence 保存在
`outputs/architecture/trading_2564_s5_validation_efficiency`，正式结果在 `outputs/validation_runtime`。
根同时承担既有源码保全和历史证据依赖；本波仅在成功发布且无唯一分支内容后删除 merged branch。
目录退出条件仍为原依赖结束、必要证据已归档并核验且无活动进程/唯一内容，不扩大清理范围。

2026-09-08 启动记录：首次 acquire 调用遗漏当前 src 绑定，editable installation 加载另一
checkout 的旧目录锁实现，被当前 OS-file arbiter 阻断；未创建 lease/transaction、未修改
task source 或实现。保留 `source_invocation_incident_v1.json`，核对源码来源后以正确运行环境
重新 START preflight 和 acquire，已进入 TASK_SOURCE_PRE_WRITE；未修改锁或旧源码。

## 5. 进度

2026-09-08：E0 登记、source transaction 与 LANE preflight 已 PASS；E1 共用字节校验和
E2 刷新器来源准入已实现，尚在针对性测试和独立审查阶段。E3 未写 seed，E4 未建立正式候选。
历史入口保留 live reader 调用链，额外强制非空 bytes、完整合法 expected provenance、
non-bool exit/worker count、显式 dist/eligibility，并拒绝 Windows root-relative locator。

来源准入只接收当前 canonical root 保留的、具备 publication fence 和 pre-dispatch readiness
记录的正式 Full；没有这些记录的旧历史产物保持原样，不经本入口刷新为新的可信 seed。
summary Git commit 必须为实际 commit object，且与 publication/readiness candidate 一致；
task/provenance、原 root、UTC 包含关系、原命令和 safety boundary 均交叉核验。
原 duration/full-test manifests 从固定 Git blob 路径读取，禁止用更新后的工作树清单复核历史。
固定写入目标为 canonical duration manifest；若传入 alias，校验后仍对同一 canonical path
进行原子替换，不会仅替换 alias 并误报成功。原 manifest 自身重定向则拒绝。

验证记录：旧 reader 79 项在项目默认 pytest tmp 设置下通过；新增历史 reader 59 项通过，
均为 `-n 16 --dist loadfile`。首轮误指定仓库内 `--basetemp`，3 项旧测试因 output locator
从绝对路径转为仓库相对路径而失败；保留 `e1_reader_focus_v1.xml`，标准调用结果为
`e1_reader_focus_v2.xml`，没有修改生产路径逻辑或原断言以消除失败。
该内部 fixture 目录 `outputs/architecture/trading_2564_s5_validation_efficiency/e1_reader_pytest_tmp`
仅归本波测试所有，退出条件为失败记录归档、无活动 pytest/唯一证据后审计清理。
59 项结果在 `e1_captured_focus_v1.xml`；随后增加 expected provenance 类型反例仍须重验。
E2 还须完成真实临时 Git source fixture 的同源正例、重签篡改、CLI dry-run/写入目标反例。
这些工程记录均不构成真实 S3 采集或研究、投资、production、broker 证据。

2026-09-08 E1–E3 完成：最终三文件 focused 为 **245 passed / 44.39s**，独立源码、测试闭包与
实际来源复核均 PASS。实际复核由 35,970 phases 重算 11,991 nodes 和 1,318 files，再逐行与
候选比较；全部有限正值，真实源无等耗时 ties，完整文件集合、顺序、原 Git blobs 及原始
summary/profile hashes 一致。合成等耗时与 alias 回归另有针对性覆盖。

tracked `PARTIAL_SEED v25` 已写入，profile id 为 `trading_2564_s5_full_duration_partial_seed`，
SHA-256 `bd02d68f7b72771d1131816b8df4f5da3555bf218454eeebc3cac563068d2d99`，146,046 bytes。
当前权重来自本需求 §1 的 exact S3b PASS Full；新 manifest 之后再次历史重验，仍读取 Git C
中的原 v24 manifest（`df1d10ae...`）并得到与写入前逐字节相同的 v25 输出，源 summary/profile
未变。证据在 `actual_source_seed_dry_run_v2.json`、`actual_source_independent_review_v1.json`
及 `actual_seed_write_verified_v1.json`；它们均位于本需求指定的 canonical runtime 目录。
v1 dry-run 因 coverage 说明不够准确而被 v2 取代，保留原始证据；数值和算法没有随说明修正而变。
已收录路径即使节点集合变化仍沿用历史 advisory weight，只有未收录路径保持首次出现顺序排在后面。
E4 生成状态、source/shared 与正式 tiers/自然 Full、普通发布仍待完成，umbrella 保持 IN_PROGRESS。

2026-09-08 E4 首轮 shared 验证为 459 passed / 127 failed，原 XML 保留为
`source_shared_focused_v1.xml`。主要失败为新 seed、refresher、runtime profile 测试及
ARCH-004G2 需求的历史/live hash 差异缺少命名 S5 后继；另有生成计数和本地 Atlas 页面绑定。
不得扩大通用 allowlist 或重写历史证据。直接修复为新增
`phase_trading_2564_s5_validated_duration_seed_v1`，明确列举 S5 来源及严格重验/advisory 边界，
保留原 phase 的业务字段和 immutable prefix。新增或修改的来源必须有当前 hash 绑定，
历史 consumer 仅通过此命名 phase 承认对应差异，并验证范围、hash 和安全边界。

原 source transaction 未声明 compatibility authority 模块，因此在修改该模块前，将原事务
以 FAILED 结束并保留全部失败及已通过证据；同一分支、同一 base 和目录申请新 source
transaction，新增 exact coordinator path
`src/ai_trading_system/platform/architecture/compatibility_authority.py`。
新事务经 TASK_SOURCE_PRE_WRITE、canonical task 更新和 LANE preflight 后才实施；不新增
工作区、不改旧 transaction、不绕过 lease。之后重建四类生成状态并重验，再建立最终候选。

新 source v2 transaction 与扩展范围的 LANE preflight 已 PASS。独立静态审查确认 S5 精确
继承 58 条并新增 8 条来源，restricted intersection 仍为原 4 条；原 S3b 和其他生产函数
AST 保持。已补命名后继的缺失/重复/错误 hash、未知阶段和越界来源反例。
Atlas 原失败 node 必须在 source commit 后运行：官方 renderer 要求 exact HEAD 且相关
来源已提交。source/shared 阶段显式暂后置该 node，source commit 后官方重建并单独重验，
最终正式验证仍包含该 node；不修改断言、删除页面或跳过最终验证。

source/shared v2 为 606 passed / 1 failed / 607 executed，606.12 秒；含新增 22 个用例，
原 XML 保留。唯一失败是 DEVX-006D 测试仍把最新 section 写为 S3b，已同步为新命名 S5，
并在重验前将同一断言链的直接前驱从 S3a 更新为 S3b；旧历史 section 的合同值不变。
其余 606 个测试涉及的生产逻辑、边界和断言未因该修正改变；修正后重建生成状态并对
DEVX-006C/D 两个文件作 16/loadfile 影响验证，覆盖该原失败 node、当前来源闭包和生成新鲜度。
源验证合并使用原 606 PASS 与该修复后结果，禁止把原 FAIL XML 改写成 PASS；最终正式 tiers
和 Full 仍验证完整候选，Atlas 原 node 在 source commit 后官方刷新时先行补验。


## 2026-09-08 S5 duration seed 正式关闭与即时失败诊断后继

S5 duration seed E4 已完成：最终 Full 12150 passed / 5 skipped，四类正式 tiers 与独立复核
通过；main、origin/main 和发布候选均为 `8166843157fa9f0b619539ae7a20e00eea16ee0b`。
正式事务已 RELEASED。关闭证据在
`outputs/architecture/trading_2564_s5_validation_efficiency/closeout_verified_v2.json`；
此前失败证据继续保留。任务临时 E1 文件和已合并分支已清理，canonical 证据保留。
这关闭耗时来源修复片，不代表稳定提速或 umbrella 完成。

下一片为 [S5 即时失败诊断](TRADING-2564_S5_Immediate_Failure_Diagnostics_V1.md)：
Full 仍运行时显示失败阶段和根因，保留完整执行与最终证据。原 S3 真实前瞻研究、Composer、
S4 与其他 S5 长期能力仍未关闭；真实业务调用计数继续为零。
