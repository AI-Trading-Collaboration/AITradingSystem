# ARCH-004C Platform Contracts

最后更新：2026-07-11

## 任务信息

- task id：`ARCH-004C_PLATFORM_CONTRACTS`
- parent：`ARCH-004`
- priority：`P0`
- status：`DONE`
- owner：architecture coordinator
- dependency：ARCH-004B complete；full parallel `5375 passed / 0 failed`
- production effect：`none`

## 目标

把 artifact、data-quality evidence、workflow、run ledger、report registration、配置解析和文件写入从松散字典与分散 helper 收敛为平台契约。Phase C 只建立公共底座、兼容 façade 和新增代码约束，不改变策略计算、研究结论、阈值、权重或生产边界。

## 现状证据

- `core.ArtifactRef`、`core.WorkflowStep` 和 `core.ProductionEffect` 是有用的最小基础，但没有 envelope、质量证据、生命周期、lineage、typed due/run 状态或 report presentation contract；
- `run_artifacts.py` 自行拼装 runtime/config/checksum metadata，其他 report/research modules 又各自直接调用 `write_text`、`json.dumps` 或 `yaml.safe_dump`；
- `data.quality.DataQualityReport` 表达一次校验结果，但 consumer 没有统一、可嵌入且 fail-closed 的 `DataQualityEvidence`；
- `scheduled_tasks.py` 仍以 command string 和 loose metadata 为主，不能作为通用 typed workflow/run ledger；
- `reports.report_index.load_report_registry()` 返回 loose dict，registry 尚无 reader tier、section provider、renderer、canonical source 与生命周期的 typed contract；
- `config.py` 继续承载大量不相关 config type、路径常量和 loader，Phase B 的 legacy adapter 仍需直接依赖它；
- 仓库已有 atomic writer、shared report writer 和大量 bespoke writer，但没有唯一公共入口，也没有阻止新增 direct writer 的 architecture ratchet。

## 设计边界

```text
contracts/                 pure schemas and invariants
  artifact_envelope.py
  data_quality.py
  workflow.py
  report_spec.py

platform/                  IO and governed resolution
  artifacts/writer.py
  config/resolver.py
  architecture/dependency_gate.py

legacy/                    time-bounded compatibility façades/adapters
domain + reporting         consume contracts; do not redefine them
```

规则：

1. `contracts` 不读文件、不导入 config、CLI、report renderer 或 domain calculator；
2. `platform` 负责原子写入、canonical bytes、checksum、runtime metadata 和 typed config resolution；
3. 旧入口只作为有 owner、sunset phase 和 parity test 的 façade；
4. 新代码不得新增 direct JSON/YAML artifact writer 或重新定义 quality/status/context；
5. Phase C 不将全部历史 artifact 一次迁完，历史迁移由 ARCH-004G 分 wave 完成；
6. compatibility façade 不得吞掉 validation error、补造 DQ pass 或改变旧 artifact bytes/path/schema。

## Contract 范围

### ArtifactEnvelope

至少包含：

- schema/version、artifact id/type、producer、run id、generated-at、as-of；
- canonical status、production effect、lifecycle/retention/visibility；
- payload/schema reference、input/output lineage 和 checksums；
- `ResearchEvaluationContext` reference 或完整 context（investment-facing 时强制）；
- `DataQualityEvidence`；
- policy/config/runtime refs、limitations、next action 和 owner；
- deterministic envelope id 与 round-trip。

### DataQualityEvidence

至少包含 governed policy id/version、status、passed、checked-at、as-of、report ref/checksum、issue counts 和 blockers。`passed=false` 或缺 required report 时，不能被 envelope/workflow 声明为 quality-ready。

### WorkflowSpec 与 RunLedger

`WorkflowSpec` 使用 typed callable entrypoint、cadence/due policy、dependency、required quality gate、expected artifact、timeout/retry/idempotency/lock 和 failure propagation；不得仅依赖裸 command string。`RunLedger` 区分 `NOT_DUE|DUE|RUNNING|PASS|LIMITED|SKIPPED|BLOCKED|FAILED`，记录每步输入、输出、时间、attempt、质量证据和 blocker，且状态迁移 fail closed。

### ReportSpec

至少包含 report id、canonical source、audience、reader tier、section provider、view-model/renderer、cadence/freshness、actionability、owner queue、artifact lifecycle 和 production effect。报告层只能呈现 canonical artifact，不得重新计算投资结论。

### 公共 writer 与 config resolver

- 公共 writer 输出 deterministic UTF-8 bytes，使用同目录临时文件 + `os.replace`，返回 path/checksum/size；
- JSON/YAML/Markdown 的 newline、key ordering 和 encoding 明确定义；
- typed config resolver 记录 path、schema/policy version、checksum、loaded-at 和 parsed type；
- Phase C 至少从 `config.py` 抽出一组真实职责，并由旧 import façade 保持兼容；
- architecture gate 对新 direct writer、反向 layer import、未登记 façade 和跨层定义建立 ratchet。

## 分阶段实施

### C1：纯平台契约

- 实现 `ArtifactEnvelope`、`DataQualityEvidence`、`WorkflowSpec/RunLedger`、`ReportSpec`；
- deterministic serialization/id、round-trip、状态转换和 fail-closed invariant tests；
- 对 Phase-B `ResearchEvaluationContext` 与 `core.ProductionEffect/ArtifactRef` 只复用，不复制。

### C2：统一 IO 与兼容 façade

- 实现 canonical atomic writer/checksum/result contract；
- 现有 shared JSON/Markdown writers 委托新入口并保持 exact-byte parity；
- direct-writer inventory 形成 baseline，禁止新增，历史项进入 G wave。

### C3：typed config resolver 与 `config.py` 拆责

- 实现 `ConfigRef`、`ResolvedConfig[T]` 和 YAML typed resolver；
- 抽出 market-regime config/loader 为平台配置模块；
- `config.py` 保留有 sunset metadata 的 re-export façade；
- Phase-B adapter 转用新 resolver，行为和 checksum parity。

### C4：workflow/report adapters

- 将 scheduled-task/report-registry 的一个代表性路径解析为 typed contract；
- 保留旧 dict/command/artifact output；
- 明确裸 command string 只作为兼容显示字段，不是 execution authority。

### C5：architecture dependency gate

- 声明 layer/import/direct-writer/façade policy；
- AST gate 覆盖新文件并以 Phase-A baseline ratchet 兼容历史债务；
- 违规输出 owner、rule、path 和可执行 remediation，不使用 waiver 清零。

### C6：reference integration 与 closeout

- 选择 read-only、`production_effect=none` 的 run manifest/shared writer 路径接入；
- 验证 path/schema/status/bytes/checksum/runtime metadata parity；
- 更新 `docs/system_flow.md`、compatibility snapshot 和 policy；
- focused、scoped mypy、contract-validation、full parallel pytest 全部通过后才解锁 ARCH-004D。

## 验收标准

- 四类 pure contract 均有 deterministic id/round-trip/invariant tests；
- investment-facing envelope 没有 complete context 或 passed DQ evidence 时 fail closed；
- RunLedger 非法状态跳转、缺依赖、缺 DQ、重复 non-idempotent execution 均 fail closed；
- ReportSpec 明确 canonical source 与 renderer，报告层不能声明 domain calculator；
- public writer 原子写入、checksum 正确、失败不留下半文件，并与选定旧 writer exact-byte parity；
- typed resolver 的 path/hash/version 与 legacy loader parity；
- `config.py` 至少一组职责已实体迁出，旧 import 有 owner、sunset phase 和测试；
- architecture gate 禁止新增 direct writer、contracts 反向依赖和未登记跨层 import；
- 代表性 workflow/report adapter 保持旧输出和 safety boundary；
- Ruff、Black、compileall、scoped mypy、focused xdist、documentation/task consistency、contract-validation 和 full parallel pytest PASS；
- 未改变 AI regime、research window、data source、strategy、threshold、weight、promotion、paper-shadow、production 或 broker。

## Legacy sunset

| façade / debt | owner | compatibility window | removal gate |
|---|---|---|---|
| `config.py` 中已迁出的 re-export | architecture coordinator | through ARCH-004G corresponding domain wave | all consumers use platform resolver and full parity passes |
| existing shared writer signatures | reporting/platform owner | through ARCH-004G reporting wave | registered artifacts migrated and byte/schema parity passes |
| bespoke direct artifact writers | owning domain lane | frozen baseline through ARCH-004G | each lane has canonical writer migration and lifecycle evidence |
| loose scheduled/report registry dict consumers | operations/reporting owner | through ARCH-004F1/F3 | typed DAG/report plugins become canonical and parity cadence passes |

## 明确不做

- 不新增策略、候选、阈值、回测或 report family；
- 不把 config 全仓一次性重写；
- 不在 Phase C 迁移 scoring、position gate 或 broker path；
- 不删除尚未完成 consumer parity 的 legacy import；
- 不把 architecture baseline debt 当作 PASS，也不新增 waiver；
- 不提前实现 ARCH-004D 的通用 ExperimentSpec/plugin vertical slice。

## 状态记录

- 2026-07-11：ARCH-004B full gate 完成后登记并进入 `IN_PROGRESS`。盘点确认 writer、runtime/checksum、DQ evidence、workflow/report registry 与 config resolution 仍分散；按 C1～C6 contract-first 推进，ARCH-004D 保持 blocked。
- 2026-07-11：C1～C5 实现完成，进入 C6 `VALIDATING`。四类 pure contract、canonical atomic writer、runtime/checksum result、typed config resolver 与 market-regime 实体拆责、scheduled/report/DQ adapters、dependency/direct-writer ratchet 和四个 reference writer integrations 已落地；focused=120 passed，scoped mypy PASS，architecture self-scan=770 files / baseline 894 / current 893 / 0 violations。contract/full gate 尚未完成，ARCH-004D 继续 blocked。
- 2026-07-11：C6 完成并归档 `DONE`。Documentation/task consistency PASS，contract-validation=`197 passed`，full parallel=`5404 passed / 0 failed / 642 warnings`，runtime artifact=`outputs/validation_runtime/full_20260710T181121Z/test_runtime_summary.json`。Public writer、quality evidence、research context 和 typed config resolver 已各有一个权威新入口；旧入口均为有 owner/sunset/parity 的 façade 或 frozen G-wave debt。ARCH-004D entry gate 解锁；未改变 strategy/threshold/weight/research conclusion/promotion/paper-shadow/production/broker。
