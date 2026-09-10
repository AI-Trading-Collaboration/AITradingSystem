# OPS-081：调度业务合同与助手偏好解耦

状态：VALIDATING；优先级：P0；Owner：operations owner / Codex operations coordinator。

2026-09-10 Owner 授权修复模型配置与业务运行许可耦合的根因，覆盖同类配置变更的复发场景。

## 根因与合同

旧 scheduler binding 对整个 automation.toml 做 live SHA/size 验证，并固定 model/reasoning。合法偏好调整使已部署数据流水线失去许可；修改配置与部署验收没有按业务语义分类。

新版本采用显式字段分类和 versioned business commitment。model、reasoning_effort、展示名称和保存时间是审计观察；id、kind/version、status、rrule、execution_environment、target、cwds、业务 prompt 核心是执行合同。未知字段、未知 nested target key、类型不符仍 fail closed。不能通过删除任意未知字段来制造相同投影。

自由文本无法可靠判断业务语义。故 prompt 只允许原 canonical 业务核心及 reviewed policy 中逐字登记的说明附录；换行规范化/末尾空白不改变核心，未登记补充和业务核心任何改动必须重新验收。不会把任意尾随指令当作无害说明。当前已授权效率说明纳入 reviewed 附录。

新观察与绑定使用版本化 schema，保存原 config SHA/size/时间用于审计，同时独立保存业务投影 SHA。旧绑定继续按旧规则验证，不通过缺字段自动升级。迁移经新的候选验证、promotion 和 deployment acceptance；旧 receipt 原样保留。偏好变化经 live projection 相等检查即可继续，业务变化或异常读保持 typed BLOCKED。

关键运行约束继续由既有 runtime preflight、receipt provenance、daily lease/dedup 和安全门禁执行。此任务不授权非 daily trigger、历史重采集、数据门禁放宽、weights 或 broker。新模型不能绕过业务入口约束。

## 实施与验收

1. S0：登记任务与冻结串行合同；SINGLE_LANE，基于 main 0507e4dd129d2a33cd61479d9226dab7ea3dd5cd。
2. S1：严格业务投影、偏好 schema、受审 prompt 附录与稳定读取；接通 observation、binding、live deployment verifier。
3. S2：偏好/格式变化正例；业务/root/target/trigger/release/safety/未知字段/tamper/并发/部分写入负例；旧 binding 兼容和迁移测试。
4. S3：mandatory focused/formal checks；更新运行图、目录、runbook；受治理 main 集成及普通 push。
5. S4：满足 exact release 与六类 validation 后正式 promotion/acceptance，随后零业务 preflight；只有后续新 provider-ready ordinary 全链 PASS 才是 OPERATIONALLY_ACCEPTED。

客户端 UI 配置保存不由本仓库控制；不能承诺跨产品原子写入。我们的边界是：非业务变更不破坏许可；业务变更预检失败不能发布新 active receipt；稳定读取和再次验证拒绝混合快照；原 receipt 保留。现有 promotion transaction 继续提供 release 激活原子性。

## 工作区生命周期

独立 worktree：D:\Work\AITradingSystem_ops081_scheduler_contract；branch：codex/ops-081-scheduler-contract。用途为合同/代码/验证/发布；不充当 scheduler。完成后保全证据、检查无进程依赖并受治理清理。现有 OPS-080 工作区和用户 checkout 均不覆盖；任何重叠必须按实际来源审计。

## 2026-09-10 实施进展

S0/S1/S2 已实现：v3 observation/binding 保存业务投影，live validator 重算；fresh observation 仍 exact；legacy 明确保留旧校验，禁止隐式升级。字段类型、未知字段、prompt 未登记尾部、业务变更、torn read、符号链接、未来 timestamp、active receipt preservation 均有回归覆盖。初步聚焦 88 tests PASS；新增旧版本与 parent-link 检查后进入最终验证。Ruff / strict mypy 对两份实现源文件 PASS。

对 actual automation.toml 的候选只读观察已 PASS：Terra/medium 与 owner 效率说明无须回滚；business SHA 为 bff1fbb130e91c05158e1c7183dc9b4c567f191aca9ef46bae9d2bc6563b8739。此证据未修改 runtime 或 active receipt，不构成 OPERATIONALLY_ACCEPTED。

初始 publication transaction 因 generator 声明不足，第二次因需显式声明兼容性源/测试，均以 FAILED 正常释放并保留（均未执行 Full）。最终 scope transaction 为 ops-081-scheduler-contract-final-scope-20260910；声明完整 task/architecture/report-flow/compatibility 范围后继续同一 worktree，未新建替代分支或改写旧事务。兼容性扩展诊断发现新增末节与目录条目数的旧断言需更新；未完成的诊断跑批不作为正式 PASS。

首个已提交候选 384df80589f057e0c7cecf7636b3376a4b6764d2 的 fast-unit 为 353 PASS / 1 FAIL：新增模块/测试使 deprecation inventory identity 与 counts 改变，锁定断言未同步。该事务 FAILED/RELEASED，保留原失败摘要，不继续发布。重验事务为 ops-081-scheduler-contract-inventory-rerun-20260910；只修正经实际清单核验的 inventory 身份、1222/1387 计数及另一 live catalog successor 的 3204 计数，不修改废弃生命周期、writer 许可或其他安全门禁。未执行过 Full，无 failed-Full parent。

候选 58fd09d3fe10b06058edd2f1336cac36bf6aade9 的 fast-unit 为 354 PASS。Architecture 出现失败；独立 canonical validator PASS，确认实际 1068 项与 `test_repository_canonical_registry_is_active_and_self_hosted` 的旧 1067 断言不符。该未完成跑批被中止，runner 写 FAIL，事务 FAILED/RELEASED；不记为完整 Architecture 或 Full。当前重验事务为 ops-081-scheduler-contract-registry-rerun-20260910。对本次新增 task/module/test/catalog 相关旧计数完成定向扫描；更新任务计数并断言 OPS-081 实际存在。新增的测试侧 OPS-081 来源闭包沿用已有冻结 checkout 的缓存方式，避免每个历史 source 重扫全链；每次 source 查询仍 live 验证 hash，并增加反例验证缓存不会掩盖 live hash 失败。运行时代码未因此变更。

候选 e9412935d14f163529a9ad4b7ed1e3816f9c6811 的五类分层验证均 PASS。Full 准备补齐固定历史依赖与 exact Atlas 绑定；两次不完整 Full 分别暴露 coordinator 未传 synthetic 父关联参数、未声明两处实际子进程测试输出范围，均已保留失败记录并换事务，未放宽 guard。补齐完整 scope 后相关两份测试文件 194 PASS。另一次因发现更早 DEVX-015 Full 并发而仅停止本任务并释放租约；等待对方退出、确认主线未变与独占后重新执行。

完整 Full `full_20260910T050620Z` 为 12762 PASS / 4 FAIL / 6 skipped，3265.8 秒；失败仅为目录/流程图各一个旧 SHA 和条目数参数，以及两个历史源消费者尚未登记 OPS-081 接续。原历史 hash 不改写；按已验证的当前渲染字节更新 582/1249 参数，在历史消费者中仅允许 OPS-081 的精确四源交集、完整唯一绑定、正确 current-hash authority 与 live SHA。未知 phase、缺失/重复/漂移绑定及交集外源仍拒绝。修复事务 `ops-081-scheduler-contract-full-regression-fix-20260910` 绑定该 Full FAIL，并显式增加 `tests/test_trading2452_architecture_contract.py` 所有权；完整相关测试文件和新候选正式验证仍须完成。下一次 actual-candidate/Full 的父租约必须重新绑定新候选 exact HEAD，不能把旧源码租约当作新候选身份。
