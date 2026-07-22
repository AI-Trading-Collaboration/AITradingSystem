# ARCH-004F3 Reporting Architecture

最后更新：2026-07-11

## 基本信息

- task id：`ARCH-004F3_REPORTING_ARCHITECTURE`
- parent：`ARCH-004`
- priority：`P0`
- status：`DONE`
- owner：reporting governance / architecture governance
- dependency：ARCH-004F1、ARCH-004F2 `DONE`；最近 full parallel=`5480 passed / 0 failed / 642 warnings`
- production effect：`none`

## 当前事实与问题

2026-07-11 inventory：

- `src/ai_trading_system/reports/reader_brief.py` 为 29,027 行、366 个顶层函数，同时承担 source loading、summary calculation、owner queue、research/governance annex、navigation、HTML/CSS 与 quality gate；
- `config/report_registry.yaml` 有 1,358 个 report entry：cadence 以 ad-hoc/manual 为主，audience 使用 legacy vocabulary；689项没有显式 `production_effect`；
- pure `ReportSpec` 已定义 `reader_tier`、section provider、view model、renderer，但 legacy adapter仍要求调用者在外部补入这些语义，registry未成为typed runtime source；
- generated report fragment仅有growth-tilt reference一个，`generated_source_of_truth_active=false`；registry/catalog/flow仍由共享大文件维护；
- Reader Brief payload与HTML已有大量外部测试和消费者，不能以删除字段、重命名path或重算结论的方式完成拆分。

根因是 daily owner reading、research review 与 audit/navigation 三种消费目的被长期叠加进同一 builder/renderer。F3目标不是新增第四套报告，而是建立一个typed reporting kernel，让旧Reader Brief成为有sunset/parity的兼容façade。

## 权威边界

- 报告层只选择、排序、压缩和渲染已存在结论；不得重算score、signal、weight、backtest、promotion或owner decision。
- Owner Daily Brief最多10个固定core section；新增report/provider不能修改core allowlist或顺序。
- owner queue只允许`due=true && actionable=true`；缺due/actionable证据的legacy item进入Audit Index或显示blocked，不进入owner queue。
- Research Review Pack承载hypothesis/evidence/review/decision与优化proposal，不混入daily owner core。
- Audit Index承载完整registry、artifact、lineage、freshness、waiver和legacy-unclassified项。
- legacy entry缺显式tier/actionable/provider/view-model/renderer时不得靠标题或路径猜测；兼容处置固定为`AUDIT_INDEX_LIMITED_UNCLASSIFIED`，待fragment迁移。
- 旧`reader_brief.v1` JSON/HTML path、schema、status、关键字段与CLI在cut-in parity前保持；新sidecar先additive。
- report registry/catalog/system flow aggregate由architecture coordinator控制；domain新增只提交fragment。

## 分阶段实施

### F3.1 Inventory、Policy 与 Characterization

- 固化reader/registry/function/field inventory、legacy vocabulary和missing semantic counts；
- 建立reviewed reporting architecture policy：10个core section、tier/actionability规则、legacy disposition、cut-in flags与safety boundary；
- characterization覆盖legacy payload/HTML/quality bytes或semantic parity、CLI exit与关键consumer字段。

验收：inventory deterministic；1,358项全部有explicit typed或limited disposition；不修改legacy输出。

### F3.2 Pure Contract 与 Typed Catalog

- 扩展pure report contracts：`ReportSectionSpec`、typed section view model、owner action item、report bundle/index；
- legacy registry adapter输出typed catalog assessment，不补造missing production effect、tier或actionability；
- provider registry只接受versioned provider id与declared source keys，重复/unknown provider fail closed。

验收：round-trip、deterministic ids、最多10 core、due/actionable queue、unknown legacy semantics fixtures全部通过。

### F3.3 Owner Daily Brief Reference Cut-in

- 从既有Reader Brief payload只读投影10个core section；不重新读取源artifact、不调用summary builder；
- 建立section provider -> typed view model -> renderer；JSON/HTML sidecar additive；
- legacy Reader Brief core renderer委托同一provider/view-model path，annex保持兼容façade至G wave。

验收：2个daily fixture、missing/limited fixture与legacy core semantic parity；core section count<=10；新增provider不改变core。

### F3.4 Research Review Pack

- 从F2 lifecycle、research governance和显式research-tier fragments生成review pack；
- 区分observation/review/proposal/validation/owner decision，不把proposal当adoption；
- fixed cadence只生成due review/action queue，不自动调参或promotion。

验收：reference growth-tilt closure与blocked lifecycle均正确传播；旧研究结论不变。

### F3.5 Audit Index 与 Generated Fragments

- 将registry/catalog/flow annex汇总为typed Audit Index；legacy-unclassified、freshness、lineage、waiver和production-effect gap可审计；
- report/artifact/flow fragments经deterministic generator形成shadow aggregate，新增report不编辑Reader Brief core；
- 逐步将既有registry entry迁为fragment，未迁项保留明确debt count。

验收：全registry coverage=1,358；0 silent drop；fragment duplicate/unknown owner/unsafe effect fail closed。

### F3.6 Cut-in、Parity 与 Closeout

- Reader Brief legacy façade切入provider/view-model/renderer核心；明确ARCH-004G reporting lane sunset；
- 更新artifact catalog、system flow、runbook、compatibility baseline与generated manifests；
- 通过focused、report-validation、contract-validation、integration、reproducibility、architecture fitness与full parallel。

## 输出链

|层级|输入|计算边界|输出|失败语义|
|---|---|---|---|---|
|Owner Daily Brief|既有daily payload + typed due/action items|选择/排序/压缩，不重算|<=10 core sections + owner queue|缺due/actionable不进queue|
|Research Review Pack|F2 lifecycle/evidence/review artifacts|状态传播与证据引用|review bundle + proposal/decision refs|proposal不得映射为adopted|
|Audit Index|registry/catalog/flow/lineage/freshness/waiver|索引与分类，不解释投资结论|完整audit entries + legacy debt|unknown/unclassified保留LIMITED|
|Legacy façade|typed core + legacy annex|兼容渲染|旧Reader Brief JSON/HTML|parity不符则不cut-in|

## 非目标

- 不在F3重写1,358个报告生成器或一次性删除29k行legacy Reader Brief；真实减法属于G/H。
- 不新增投资结论、阈值、权重、回测、promotion、paper-shadow、production或broker行为。
- 不把registry presence当artifact freshness/pass，也不把report PASS当upstream investment conclusion PASS。
- 不用标题、路径token或audience字符串静默猜reader tier。

## 当前进展

- 2026-07-11：F3.1进入`IN_PROGRESS`。完成初始inventory：Reader Brief=29,027 lines/366 top-level functions；registry=1,358 entries，cadence=`ad_hoc 949/manual 236/daily 83/weekly 64/monthly 15/on_change 10/event_driven 1`，legacy audience=`project_owner 773/reviewer 371/operator 127/owner 77/investor 8/system 1/daily_reader 1`，689项缺显式production effect。ReportSpec存在但未成为registry runtime source，generated report fragment=1且inactive。下一步落policy、deterministic inventory和characterization tests。
- 2026-07-11：F3.1完成，F3.2进入`IN_PROGRESS`。新增reviewed`reporting_architecture_policy.v1`，冻结10个Owner Daily Brief core section、due+actionable owner queue、no-auto-tune/no-recompute、additive-only与legacy unclassified=`AUDIT_INDEX_LIMITED_UNCLASSIFIED`；新增tracked `reporting_architecture_inventory.v1`与AST/YAML scanner，当前Reader Brief/registry/hash/distribution/typed-field/fragment count可deterministic复算，任一漂移fail closed。Focused=4、Ruff/mypy PASS，legacy Reader Brief bytes/path/schema/status未修改。
- 2026-07-11：F3.2/F3.3完成，F3.4进入`IN_PROGRESS`。新增pure `ReportSectionSpec`/section view model/`OwnerActionItem`/`OwnerDailyBriefViewModel`与typed catalog assessment；1,358个legacy registry entry全部显式保留为`LIMITED_UNCLASSIFIED`，0 silent drop。Owner Daily provider只读既有Reader Brief payload，固定生成10个section，只有typed `DUE + actionable`事项进入owner queue；`aits reports reader-brief`与daily-run新增JSON/HTML sidecar并纳入canonical-to-legacy mirror，不替换旧path/schema/status。Focused owner/report/daily-run=`18 passed`，Ruff与scoped mypy PASS；下一步Research Review Pack。
- 2026-07-11：F3.4/F3.5完成，F3.6进入`VALIDATING`。新增typed Research Review Pack，把observation/evidence/review/preregistration/validation/owner decision分栏传播，`proposal_is_adoption=false`、auto-tune=false；growth-tilt terminal RETIRE与PIT-lineage INVESTIGATE/BLOCKED fixture正确分离。新增Report Audit Index，完整保留1,358个registry entry、typed=0/limited=1,358/0 silent drop；platform只接收typed catalog，legacy解析不会反向依赖platform。Owner/Research/Audit各自新增report/artifact/flow shadow fragment，aggregate fragments=13、report fragments=4，architecture fitness module=793/test=1,111/orphan=0/overlap=0/dependency violation=0 PASS。Native Reader Brief cut-in仍显式false，等待parity/closeout gate，不伪装已删除legacy。
- 2026-07-11：F3.6与ARCH-004F3完成，ARCH-004G解锁。最终focused=14、report-validation=55、contract=197、architecture=150、integration=983、reproducibility=23、full=`5,494 passed / 0 failed / 642 warnings`。旧Reader Brief tests、path/schema/status与daily-run镜像均通过；新Owner Daily sidecar只做加法。Native Reader Brief cut-in仍显式false，legacy façade删除/迁移进入G/H，未把shadow fragment误写为active source of truth。
