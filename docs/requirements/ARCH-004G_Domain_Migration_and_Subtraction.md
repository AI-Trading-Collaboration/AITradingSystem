# ARCH-004G Domain Migration 与减法波次

最后更新：2026-07-12

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

G1分片：

- G1.1 `cache_catalog`、`data_refresh_audit`、`data_source_fallback_policy`的本地JSON/text writer实现迁到`platform.artifacts`；保留原私有函数签名作为短期frozen compatibility wrapper，JSON必须显式`trailing_newline=false`以保持旧bytes，原`None|Path`返回契约不变；
- G1.2 迁移三模块内部caller后删除上述私有wrapper，并以direct-writer/reachability ratchet证明旧入口为零；
- G1.3 从inventory选择下一helper family，只有两个以上真实caller且能证明schema/error/bytes parity才抽取。
  - G1.3a选择`trading_engine`中的`data_freshness_summary`、`pipeline_health_summary`、`parameter_governance_summary`、`parameter_governance_daily_digest`、`notification_delivery_audit_summary`。五个模块各有真实JSON/text产物caller，旧实现均为UTF-8、JSON `indent=2`、保留插入顺序、末尾换行，text不追加内容；迁移必须显式`sort_keys=false`并删除10个私有writer，`OSError`边界、bytes和产物schema保持兼容。
  - G1.3b选择notification/retry workflow中的`operator_brief_notification_approval_gate`、`notification_delivery_failure_classification`、`operator_brief_notification_delivery_preflight`、`operator_brief_notification_dispatch_preview`、`operator_brief_notification_draft`、`operator_brief_notification_draft_dispatch`、`retry_execution_dry_run`、`retry_candidate_queue`。八个模块的JSON/text writer语义与G1.3a相同且均有真实artifact caller和独立测试；迁移须直接删除16个private helper、显式`sort_keys=false`，保持latest/run-log/draft/retry artifact path、bytes、schema/status与fail-closed行为。
  - G1.3c选择`data_freshness_summary`、`pipeline_health_summary`、`notification_delivery_audit_summary`、`operator_brief_notification_approval_gate`、`operator_brief_notification_delivery_preflight`、`operator_brief_notification_dispatch_preview`、`operator_brief_notification_draft`、`operator_brief_notification_draft_dispatch`的文件checksum helper。8个本地实现均以1 MiB chunk读取并返回SHA-256 hex，合计13个真实caller；新增canonical `platform.artifacts.sha256_path`后必须迁完caller并删除8个private helper，保持checksum、流式读取、missing/unreadable `OSError`边界和上层artifact状态/决策不变。
  - G1.3d先对42个`_with_runtime_metadata`做AST与字段分组，禁止用任意`extra_fields`强行合并14种语义。首个可迁子族为10个PIT replay closure/recheck模块：函数结构、metadata字段及39个safety-false字段名称/顺序完全相同，每模块1个真实caller。抽取`research_framework.runtime_metadata`中的专用observe-only helper和固定safety constant；模块保留`SAFETY_FALSE_FIELDS` canonical alias供测试/审计，删除10个private helper，并保持field order、generated-at格式、AI regime、source error、task/report identity及no-production/no-broker字段不变。
  - G1.3e对106个DQ/source-validation helper分组后，选择15个AST完全一致的growth-tilt `validate-data` gate。它们均读取同一universe/DQ config，`include_full_ai_chain=false`，传入同目录manifest与Marketstack secondary价格，仅主`data/raw/prices_daily.csv`要求secondary，写同一`default_quality_report_path`并返回相同8字段摘要。抽取专用`research_framework.data_quality_gate.run_growth_tilt_data_quality_gate`，禁止catch/降级/伪造PASS；迁完15个caller并删除15个gate和15个Marketstack判断helper，保持validate/write调用、路径、as-of、异常传播与报告schema。

G1.1不是G1完成条件；若wrapper长期保留或允许新增caller，视为永久双轨失败。

G1于2026-07-11正式完成。六类canonical family共删除80个private helper；选定family的legacy caller为0，direct writer从893降至861。G1.3d～G1.3e额外使dynamic wrapper surface从89,805行/2,154个函数降至88,315行/2,114个函数。29组safety assertion经AST审计后因语义不同保留原位，没有为追求行数而引入跨语义抽象。最终focused=`254 passed`、architecture-fitness=`168 passed`、Ruff PASS、architecture violation=0；G2进入`IN_PROGRESS`，本closeout slice尚未改动ETF CLI runtime。

### G2 Interfaces 与 ETF CLI

- 按data、research、portfolio、shadow、operations、reporting command group拆`etf_portfolio.py`；
- command name/options/default/exit/help保持golden parity；
- root文件只做注册与兼容导入，不承载计算或artifact写入。

退出：command registry全覆盖、0 duplicate command、2组真实CLI fixture parity；旧实现不可继续接收新命令。

#### G2 phase-level exit 与 ARCH-005 bootstrap handoff

Owner 于 2026-07-12 批准以下停止条件；它是整个 G2 的 phase gate，不是任一 G2.4 单切片的 closeout：

1. 完成全部 G2.4 callback/migration matrix 和 G2 phase exit criteria，不得把单个 slice `COMPLETE` 写成 G2 完成。
2. 通过 required focused/architecture/contract/full validation，并刷新、验证 module/test manifests、compatibility baseline、deprecation inventory 与全部 source hashes/freshness。
3. 只提交和推送可归属的 ARCH-004 变更，保留用户其他工作，并记录 shared-path active owner/lease count 与 known unrelated worktree files。
4. 写入并验证`arch_005_bootstrap_handoff.v1`证据，至少包含source phase、HEAD/base commit、branch/push status、migration matrix completeness、validation artifacts、manifest hashes/freshness、shared-path owner/lease count、unrelated files、`production_effect=none`。
5. Handoff 必须明确`next_slice_unblocked=false`；完成后 ARCH-004 停在 G2.5 之前，不得自动进入 G2.5、G3/G4/G5 或选择任何下一 ARCH-004 slice。
6. `ARCH-005_PARALLEL_DEVELOPMENT_CONTROL_PLANE` 需求状态可为`READY`，但 S0 实现必须等待上述 phase-level handoff `PASS`；ARCH-005 S0/S1 完成后，只能由新的显式指令恢复 ARCH-004 G2.5。

该 handoff 是无 runtime/production/broker effect 的架构交接证据；不得用临时 workaround、口头说明或一个 slice 的 PASS 替代。

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

- 2026-07-12：G2.4AW Dynamic-v3 Replay Diagnosis `COMPLETE`，G2.4继续且尚未进入phase-level ARCH-005 handoff。3 callback迁至canonical module；legacy root=`26,200 lines / 774 functions / 735 decorators`，generated=`861 modules / 1,114 tests / 858 writers / 0 violations`；focused=`83`、architecture=`232`、contract=`203`均PASS。五source bundles、unit-aware reasons、AV comparison gate和全部views可重算；另修复Backfill Repair遗漏cost_rate。本层无repair/calibration/portfolio/execution effect。
- 2026-07-12：G2.4AW Dynamic-v3 Replay Diagnosis contract freeze并进入`IN_PROGRESS`。范围固定为`replay-diagnosis run/report`与`validate-replay-diagnosis`共3 callback，迁至`dynamic_v3_replay_diagnosis.py`。旧run直接读取inventory/replay/backfill/sim/review五条mutable链，不调用任何validator、不校验完整lineage/generated cutoff、不冻结source；coverage与health只相信manifest/file exists，validator只检查文件、known reason、exists与safety，不能重算coverage/reasons/health/manifest/report。无pending reason时旧helper还注入blocking `unknown`，使健康证据错误降级；`can_enter_variant_comparison`只要任意AVAILABLE window即true，绕过AV的distinct-event/window/sim evidence gate；missing-price count混合inventory-event与outcome-window单位。本slice必须在任何output前要求五个source full PASS、完整inventory→replay→backfill/sim→review lineage与timezone-aware time ordering；冻结五类全部source bundle到immutable diagnosis snapshot。Coverage按event/variant/window/sim-state/recommendation分单位，pending reason披露source scope/count units；无pending reason返回空列表而非unknown。Variant comparison readiness绑定reviewed AV evidence gate，不以单window解锁方向性比较。Health记录validator/snapshot/checksum状态。Validator重验live sources并重算coverage/reasons/health/manifest/Markdown，source/snapshot/output tamper FAIL；legacy unsnapshotted只读warning。仅写diagnosis evidence/latest pointer，不运行repair/comparison/calibration，不改source/config/policy/weights/portfolio/production/order/broker。
- 2026-07-12：G2.4AV Dynamic-v3 Replay Performance Review `COMPLETE`，G2.4继续且尚未进入phase-level ARCH-005 handoff。3 callback迁至canonical module；legacy root=`26,322 lines / 777 functions / 738 decorators`，generated=`860 modules / 1,114 tests / 858 writers / 0 violations`；focused=`91`、architecture=`231`、contract=`203`均PASS。Source/policy/snapshot/output可重算，unsupported metrics与missing保持null，pilot evidence gate使当前1-event证据正确降级为continue forward tracking；无自动config/promotion/portfolio/execution effect。
- 2026-07-12：G2.4AV Dynamic-v3 Replay Performance Review contract freeze并进入`IN_PROGRESS`。范围固定为`replay-performance-review run/report`与`validate-replay-performance-review`共3 callback，迁至`dynamic_v3_replay_performance_review.py`。审计发现旧run不验证Backfilled Outcome/Historical Paper Simulation、不校验共同replay lineage或generated cutoff、不冻结source/policy且先创建output；无样本均值/比率和missing variant指标被写成0，`false_alarm_rate`/`missed_opportunity_rate`仅由同一5日relative return的正负互补推导，名称不能被现有证据支持；calibration recommendation还由未治理的`available>0`与`delta>0`直接触发。旧validator只检查文件、id、owner approval和safety，不能从source重算effectiveness、recommendation、manifest、Markdown或Reader Brief。本slice必须在任何output前要求两个source artifact full PASS、同一replay id且review time不早于sources；冻结backfill/sim全部计算输入与validated policy到immutable snapshot。可支持的5日relative指标改为正/非正率，真正false alarm/missed opportunity保持null并解释not identified；missing metrics保持null。Directional recommendation只有同时满足policy中的distinct-event和available-window floor才可提出，否则只能`continue_forward_tracking/manual_review_required`；任何建议仅为owner review proposal，不自动更新config/promotion。Validator重验live source/checksum并重算全部views，source/snapshot/output/policy tamper均FAIL；legacy unsnapshotted只读warning。仅写historical review evidence/latest pointer，不改backfill/sim/config/policy/official/paper/real/production/order/broker。
- 2026-07-12：owner 批准 ARCH-004→ARCH-005 bootstrap handoff 停止条件。G2.4 继续直至全callback/migration matrix与phase-level exit gate完成；随后必须产出`arch_005_bootstrap_handoff.v1` PASS，记录clean attribution/validation/manifest/lease/unrelated-worktree证据，并保持`next_slice_unblocked=false`。ARCH-005需求READY不代表S0可立即实施；S0等待整个G2 phase handoff，ARCH-004在handoff后停在G2.5之前。
- 2026-07-11：G1正式`COMPLETE`，G2 Interfaces/ETF CLI进入`IN_PROGRESS`。G1六类canonical family共删除80个private helper，selected-family legacy caller=0；direct writer=`893 -> 861`，dynamic wrapper=`89,805 -> 88,315 lines / 2,154 -> 2,114 functions`。29组safety assertion因语义不同明确不做强行抽象。最终focused=`254 passed`、architecture-fitness=`168 passed`、Ruff PASS、violation=0；production/broker effect=none。G2先冻结command registry和golden CLI contract，再按data/research/portfolio/shadow/operations/reporting拆分，当前尚未改ETF CLI runtime。
- 2026-07-11：G1.3e实现完成并进入`VALIDATING`。新增专用growth-tilt required DQ gate，15个caller已迁移，15个private gate与15个secondary判断helper清零。Characterization覆盖validate全部kwargs、主/替代price的Marketstack条件、report write、8字段summary与异常传播；15模块回归=`242 passed`、Ruff PASS。Generator=`797 modules / 1,113 tests / 861 direct writers / 0 violations`，dynamic wrapper=`88,315 lines / 2,114 functions`；等待compatibility与正式architecture gate。
- 2026-07-11：G1.3e growth-tilt DQ gate family进入`IN_PROGRESS`。Inventory=`106 helpers / 51 groups`；15个gate函数和secondary-price判断完全同构，15个模块都有独立测试。Canonical helper必须直接调用`validate_data_cache/write_data_quality_report`、异常fail-closed，不得绕过required data quality gate；characterization须验证全部kwargs、Marketstack条件、8-field summary、write调用与异常传播。
- 2026-07-11：G1.3d `COMPLETE`，G1.3e DQ/safety helper inventory进入`IN_PROGRESS`。最终focused=`194 passed`（10模块+metadata characterization=`182 passed`）、architecture=`166 passed`、Ruff PASS；10个private metadata helper剩余0、10个caller canonical。Dynamic wrapper inventory从`89,805 lines / 2,154 functions`降到`89,155 / 2,144`，direct writer保持861、violation=0；metadata/safety/workflow/investment语义未改变，G1整体继续。
- 2026-07-11：G1.3d实现完成并进入`VALIDATING`。新增专用runtime metadata contract和39-field canonical tuple，10个模块的10个caller已迁移、10个private helper清零；模块级safety常量为共享alias。Ordered metadata/fixed timestamp/source errors/AI regime/no-production/no-broker characterization及10模块回归=`182 passed`，Ruff PASS；generator=`796 modules / 1,113 tests / 861 direct writers / 0 violations`，等待compatibility与正式architecture gate。
- 2026-07-11：G1.3d PIT replay observe-only runtime metadata family进入`IN_PROGRESS`。Inventory=`42 files / 14 AST+field groups`；选定10个完全同构模块、10个caller和共同39-field safety tuple。设计禁止generic `extra_fields`，采用专用canonical helper；模块级`SAFETY_FALSE_FIELDS`保留为共享常量alias，退出门禁包含ordered payload parity、fixed generated-at、AI regime/source errors、39 fields false、旧helper reachability=0、10模块回归、Ruff、architecture与compatibility。
- 2026-07-11：G1.3c `COMPLETE`，G1.3d runtime metadata helper inventory进入`IN_PROGRESS`。最终focused=`167 passed`（8模块/platform/checksum characterization=`155 passed`）、architecture=`164 passed`、Ruff PASS；8个private checksum helper剩余0、13个caller全部canonical，direct writer保持861、violation=0。checksum/path/schema/status/workflow decision与production effect未改变；G1整体仍未完成。
- 2026-07-11：G1.3c实现完成并进入`VALIDATING`。新增canonical streaming `sha256_path`，8个模块的13个caller全部迁移，8个private checksum helper已删除；跨默认1 MiB边界、自定义chunk、missing path `OSError`和invalid chunk characterization及8模块/platform回归=`155 passed`，Ruff PASS。Architecture generator保持`795 modules / 1,113 tests / 861 direct writers / 0 violations`；等待compatibility与正式architecture gate。
- 2026-07-11：G1.3c checksum helper family进入`IN_PROGRESS`。确认8个模块、8个private helper、13个真实caller和8份独立模块测试；canonical API必须使用1 MiB默认chunk、对跨chunk文件产生相同hex，并保留missing/unreadable path的`OSError`边界。部分模块的`hashlib`还用于payload stable hash，迁移不得误删该独立逻辑。
- 2026-07-11：G1.3b `COMPLETE`，G1.3c下一helper family筛选进入`IN_PROGRESS`。最终focused=`151 passed`（8模块+writer characterization=`139 passed`）、architecture=`162 passed`、Ruff PASS；direct writer=`877 -> 861`、violation=0，16个private writer剩余0。latest/run-log/draft/retry artifacts保持path/schema/status/bytes兼容，notification/retry决策与production effect未改变；G1整体仍未完成。
- 2026-07-11：G1.3b实现完成并进入`VALIDATING`。8个notification/retry模块的16个private writer已删除，真实caller全部直接使用canonical API；focused=`139 passed`、Ruff PASS、architecture generator=`795 modules / 1,113 tests / 861 direct writers / 0 violations`。保持JSON insertion order/trailing newline、text bytes和`OSError`边界；等待compatibility与正式architecture gate后closeout，G1整体继续。
- 2026-07-11：G1.3b notification/retry writer family进入`IN_PROGRESS`。确认8个模块、16个private writer、所有模块均有真实artifact caller和独立pytest；预计direct writer=`877 -> 861`。本slice禁止compatibility wrapper，必须覆盖JSON insertion order/trailing newline、text bytes、`OSError`边界、notification/retry模块回归、Ruff、architecture与compatibility baseline。
- 2026-07-11：G1.3a `COMPLETE`，G1.3b下一helper family筛选进入`IN_PROGRESS`。最终focused=`107 passed`（其中五模块+writer characterization=`95 passed`）、architecture=`161 passed`、Ruff PASS；direct writer=`887 -> 877`、architecture violation=0，10个private writer剩余0。该family未改变payload/schema/status、DQ/governance/notification结论或production effect；G1整体仍未完成。
- 2026-07-11：G1.3a实现完成并进入`VALIDATING`。五个模块的10个private writer已删除且所有caller直接调用canonical atomic API；JSON显式`sort_keys=false`、trailing newline=true，characterization覆盖insertion order、Unicode、text bytes、atomic结果和`OSError`边界。Focused=`95 passed`、Ruff PASS、architecture generator=`795 modules / 1,113 tests / 877 direct writers / 0 violations`；等待正式architecture gate与compatibility baseline后closeout该family，G1整体继续。
- 2026-07-11：G1.3a进入`IN_PROGRESS`。从inventory确认五个`trading_engine` summary/governance模块共享完全同构的JSON/text writer，计划直接迁移内部caller并删除10个private helper，不保留兼容wrapper；canonical JSON调用必须显式`sort_keys=false`保留旧插入顺序，focused模块回归、bytes/error characterization、Ruff和architecture ratchet为退出门禁。
- 2026-07-11：G1首个helper family完成并准备独立closeout，G1.3继续`IN_PROGRESS`。最终focused=`29 passed`、architecture=`159 passed`、Ruff PASS；direct writer=887、architecture violation=0，private wrapper=0。该family满足implementation migration、caller migration、旧入口删除和bytes parity；G1整体仍未完成，下一family必须重新按真实重复caller筛选。
- 2026-07-11：G1.2完成，G1.3进入`IN_PROGRESS`。三模块全部internal caller已直接调用`write_json_atomic_without_trailing_newline/write_text_atomic`，6个private compatibility wrapper已删除，旧入口reachability=0。新增canonical no-newline helper由三个真实模块共同消费；writer、三个模块和platform writer regression=`17 passed`，Ruff PASS。Direct-writer仍为887（G1.1已完成6-call reduction）；等待architecture/compatibility gate后归档该family并选择下一family。
- 2026-07-11：G1.1完成，G1.2进入`IN_PROGRESS`。`cache_catalog`、`data_refresh_audit`、`data_source_fallback_policy`的6个direct writer实现已委托`platform.artifacts.write_json_atomic/write_text_atomic`；JSON显式`trailing_newline=false`，原`None|Path`返回不变。Focused writer+module regression=`11 passed`、Ruff、architecture generator PASS；direct writer=`893 -> 887`、violation=0。6个私有wrapper仍在，状态仅为frozen compatibility，G1.2必须迁caller并删除。
- 2026-07-11：G1.1登记并进入`IN_PROGRESS`。选择三个共享report/data-governance模块中完全同构的`_write_json/_write_text`实现；本slice预计减少6个direct `Path.write_text` callsite，保留无独立逻辑的frozen private wrapper以维持调用/返回契约，G1.2必须迁caller并删除wrapper。必须验证Unicode、sort keys、indent、无trailing newline、text bytes、返回值与atomic write parity。
- 2026-07-11：G0完成，G1进入`IN_PROGRESS`。Architecture=`156 passed`、contract-validation=`203 passed`，G tests已纳入architecture与contract正式tier；inventory drift、missing replacement、非法transition、blocked gate removal全部fail closed。G0保持0 runtime deletion、0 investment semantics change；下一步从direct-writer/helper family中选择有两个以上真实caller且可bytes parity的G1 slice。
- 2026-07-11：G0实现进入`VALIDATING`。新增pure deprecation record/gate contract、reviewed policy、deterministic scanner与tracked inventory；当前inventory=`795 modules / 1,112 tests / 893 direct writers / 7 legacy files / 99 dynamic wrappers / 48 matching research_quality implementations`，9 targets=`6 ACTIVE / 3 DEPRECATED / 0 removal-ready`。ETF CLI=`37,604 lines / 993 command decorators`，Reader Brief=`29,027 lines`。Focused=6、Ruff/mypy、architecture generator PASS；等待architecture/contract gate后进入G1。
- 2026-07-11：F3完成后登记G并进入G0。当前只允许inventory/policy/characterization，不直接删除runtime surface；3份并行用户研究文档继续排除在ARCH-004提交范围外。
