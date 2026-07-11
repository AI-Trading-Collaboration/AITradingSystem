# ARCH-004G Domain Migration 与减法波次

最后更新：2026-07-11

## 任务信息

- task id：`ARCH-004G_DOMAIN_MIGRATION_AND_SUBTRACTION`
- parent：`ARCH-004`
- priority：`P0`
- status：`IN_PROGRESS`
- owner：architecture coordinator / 各 domain owner
- dependency：ARCH-004F1、F2、F3 `DONE`
- production effect：`none`

## 目标

把 A～F 已建立的 canonical contracts、platform services、operations control plane、research lifecycle 和 typed reporting kernel接回真实 domain consumers，并对被替代实现执行可审计的冻结、退役和删除。G 的成功标准不是新增 façade，而是让 legacy 可达面、重复 helper、god module职责和 task-shaped wrapper数量持续下降，同时保持 CLI、artifact、投资解释和安全边界。

## 不可破坏边界

- 默认 behavior-preserving；不得在结构迁移中改变 strategy、threshold、weight、score、position gate、backtest结论、promotion、paper-shadow、production或broker行为。
- `DEPRECATED` 必须记录 replacement、owner、usage evidence、compatibility window、sunset condition和removal gates。
- 删除前必须证明 runtime/import/CLI/report/docs/test reachability为零或全部由明确有期限的 compatibility façade承接。
- code deprecation与historical artifact retention分离；不得删除失败研究、lineage、schema、commit或runner reference。
- 每个波次必须冻结或删除旧入口；新增永久双轨、无期限 TODO 或从文件名猜 replacement均为失败。
- scoring、position gate、backtest等投资解释敏感路径最后迁移，并要求更强 golden/parity和owner signoff。

## G0 基线定义

当前优先大表面：

|优先级|lane|surface|当前事实|G0 disposition|
|---|---|---|---|---|
|1|shared platform|重复 IO/checksum/DQ/runtime metadata/safety helpers|direct writer baseline=894，当前=893；仍需逐调用分类|inventory + ratchet|
|2|interfaces|`cli_commands/etf_portfolio.py`|37,604 lines / 1,579,334 bytes|拆 command groups；root保留薄注册 façade|
|3|reporting|`reports/reader_brief.py`|29,027 lines / 1,277,659 bytes|native typed providers逐段接管；旧 renderer有期限兼容|
|4|portfolio/research|`etf_portfolio/dynamic_v3_system_target.py`|大型 mixed responsibility module|先 characterization，再拆 pure/application/report职责|
|5|research|`controlled_strategy_batch.py`|19,802 lines / 836,611 bytes|迁 generic runner/plugin；冻结 task-shaped扩展|
|6|portfolio/research|`etf_portfolio/dynamic_v3_parameter_research.py`|大型 parameter research module|迁 lifecycle/spec/plugin；不改变参数政策|
|7|research|growth-tilt/dynamic-strategy task wrappers|root wrapper与`research_quality`实现大量成对存在|按reachability与artifact parity分批冻结/删除|
|8|decision|scoring/position gate/backtest|investment interpretation sensitive|最后迁移，G前段不得触碰语义|

## 分阶段实施

### G0 Inventory、Deprecation Policy 与 Removal Gate

- 建立reviewed lifecycle policy：`EXPERIMENTAL -> ACTIVE -> DEPRECATED -> FROZEN -> REMOVED`；
- 对目标surface记录lane、owner、replacement、compatibility window、sunset phase、usage/reachability证据和逐项removal gate；
- deterministic扫描文件规模、AST职责、inbound imports、CLI/report/docs/tests可达性、direct-writer ratchet、legacy adapter和task-wrapper对；
- 未知replacement或usage evidence必须`BLOCKED`，不得直接标记可删除。

退出：inventory可复算且drift fail closed；0个missing owner/replacement/window/sunset/gate；本阶段0 runtime deletion。

### G1 Shared Platform Helper Migration

- 逐类迁移重复 writer/checksum/runtime metadata/DQ/safety helper到既有canonical service；
- 每个slice必须减少或冻结旧helper，并保持bytes/schema/status/error parity；
- direct writer调用继续ratchet下降，禁止新增waiver掩盖。

退出：选定helper family的legacy caller=0；旧helper frozen或removed；architecture dependency和focused/full gate通过。

### G2 Interfaces 与 ETF CLI

- 按data、research、portfolio、shadow、operations、reporting command group拆`etf_portfolio.py`；
- command name/options/default/exit/help保持golden parity；
- root文件只做注册与兼容导入，不承载计算或artifact写入。

退出：command registry全覆盖、0 duplicate command、2组真实CLI fixture parity；旧实现不可继续接收新命令。

### G3 Reporting Native Migration

- Reader Brief核心改为typed section provider/view model/renderer消费；
- Owner Daily、Research Review和Audit Index成为各自canonical消费面；
- legacy Reader Brief只保留明确annex/compatibility职责并建立sunset。

退出：core native coverage、旧JSON/HTML semantic parity、无报告层重算、legacy line/responsibility ratchet下降。

### G4 Operations 与 Periodic Consumer Migration

- 将`ops_daily.py`和scheduled consumer迁到F1 canonical workflow/control/runtime；
- 保持唯一外部daily trigger，non-daily自动dispatch仍false；
- 旧executor和schedule adapter达到freeze/removal gate。

退出：2 daily、2 weekly、1 monthly parity证据准备完成；运行锁、retry、resume、DQ failure传播一致。

### G5 Research Wrapper Migration

- task-shaped growth-tilt/dynamic-strategy wrapper迁到ExperimentSpec/plugin/lifecycle；
- 新同类研究禁止新增task-id Python module/CLI/report family；
- 逐wrapper证明primary/section/Markdown/envelope/ledger/lifecycle parity后冻结或删除。

退出：选定wave wrapper数量净下降；0无期限dual track；历史artifact仍可复现。

### G6 Portfolio/Decision Sensitive Migration

- 迁dynamic-v3 system target、parameter research、controlled batch；
- scoring、position gate和backtest只在完整characterization/golden/PIT/DQ/cost/owner signoff后迁移；
- 结构变更不得与策略调优混合。

退出：投资解释、成本、PIT、DQ、production-effect与owner decision parity全通过。

### G7 Closeout 与 H Handoff

- 生成lane级deprecation/reachability/removal ledger；
- 更新generated manifests、compatibility baseline、system flow、artifact catalog和runbooks；
- 通过architecture、contract、report、integration、reproducibility、clean-clone准备和full parallel。

退出：所有G wave有明确`FROZEN|REMOVED|BLOCKED_WITH_OWNER`终态，0永久compatibility TODO；ARCH-004H才可解锁正式cutover/removal。

## G0 验收证据

- tracked inventory与source hash；
- lifecycle transition与removal-readiness pure contract tests；
- reachability scanner重复运行结果一致；
- inventory drift、missing replacement、缺usage evidence、未满足removal gate均fail closed；
- `production_effect=none`、`broker_action=none`、0删除、0策略语义变化。

## 状态记录

- 2026-07-11：G0完成，G1进入`IN_PROGRESS`。Architecture=`156 passed`、contract-validation=`203 passed`，G tests已纳入architecture与contract正式tier；inventory drift、missing replacement、非法transition、blocked gate removal全部fail closed。G0保持0 runtime deletion、0 investment semantics change；下一步从direct-writer/helper family中选择有两个以上真实caller且可bytes parity的G1 slice。
- 2026-07-11：G0实现进入`VALIDATING`。新增pure deprecation record/gate contract、reviewed policy、deterministic scanner与tracked inventory；当前inventory=`795 modules / 1,112 tests / 893 direct writers / 7 legacy files / 99 dynamic wrappers / 48 matching research_quality implementations`，9 targets=`6 ACTIVE / 3 DEPRECATED / 0 removal-ready`。ETF CLI=`37,604 lines / 993 command decorators`，Reader Brief=`29,027 lines`。Focused=6、Ruff/mypy、architecture generator PASS；等待architecture/contract gate后进入G1。
- 2026-07-11：F3完成后登记G并进入G0。当前只允许inventory/policy/characterization，不直接删除runtime surface；3份并行用户研究文档继续排除在ARCH-004提交范围外。
