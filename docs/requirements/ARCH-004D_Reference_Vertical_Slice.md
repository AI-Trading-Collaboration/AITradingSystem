# ARCH-004D Reference Vertical Slice

最后更新：2026-07-11

## 任务信息

- task id：`ARCH-004D_REFERENCE_VERTICAL_SLICE`
- parent：`ARCH-004`
- priority：`P0`
- status：`DONE`
- owner：architecture coordinator / research platform owner
- dependency：ARCH-004C complete；full parallel `5404 passed / 0 failed`
- production effect：`none`

## 选择结论

Reference slice 选择已关闭的 `TRADING-2438N1 growth_tilt_candidate_family_closure`，不选择尚有 owner input、真实 replay、fresh cache 或 production/paper-shadow 风险的其他 2438N 路径。

选择依据：

1. family terminal status 已冻结为 `GROWTH_TILT_CANDIDATE_FAMILY_CLOSED_NO_EXECUTABLE_PIT_CANDIDATE`；
2. `pit_candidates_tested=0`、`replay_run=false`、`production_effect=none`、`broker_action=none`；
3. domain calculator 已位于 `research_quality.growth_tilt_candidate_family_closure`，CLI/task wrapper 只负责 source loading、runtime metadata、writer 和 Markdown；
4. 现有 builder、strict blocker、runner、CLI、registry/catalog/flow tests 可形成完整 characterization；
5. 该 slice 可以证明“相同 calculator/report plugin 下新增 experiment 只加 spec，不加 task-id Python module”。

## 目标架构

```text
ExperimentSpec (YAML/typed)
  -> Application Research Runner
    -> Calculator Plugin
      -> Evidence/Decision Payload
        -> Canonical Artifact Writer
          -> ArtifactEnvelope + RunLedger sidecars
            -> Report Plugin
```

旧入口：

```text
aits research strategies growth-tilt-candidate-family-close
  -> time-bounded legacy façade
    -> generic application runner
```

旧 CLI name/options/exit、primary/ledger/Markdown path、payload schema/status、安全字段和中文解释必须保持 parity。新增 sidecar 只能 additive，不改变旧 payload 的 `artifact_paths` 或 report registry identity。

## Contract

### ExperimentSpec

至少包含：

- spec schema/id/version/status/owner；
- calculator plugin id/version；
- report plugin id/version 与 `ReportSpec`；
- typed input slots（structured/text、required、default path、role）；
- output slots（primary/section/Markdown/envelope/run-ledger）；
- explicit legacy status -> `CanonicalStatus` mapping；
- data-quality requirement/classification；
- strict behavior、production effect、manual review、broker boundary；
- deterministic spec id 与 config provenance。

### Application Runner

Runner 负责：

- spec/config resolution；
- input override 与 required-source validation；
- JSON/YAML/text loading；
- source path/hash/size lineage；
- calculator/report plugin dispatch；
- runtime metadata、strict error propagation；
- canonical writer、`ArtifactEnvelope` 和 one-step `RunLedger`；
- old artifact path/schema/bytes parity。

Runner 不包含 growth-tilt 常量、task id、status substring inference 或 domain calculation。

### Plugin

- calculator plugin 只把 typed execution context 映射给已有 pure builder；
- report plugin 只 render Markdown 和 section artifact，不重算 conclusion；
- plugin registry 对未知 plugin id/version fail closed；
- plugin module 按 capability 命名，不按 task id 命名。

### Sidecars

- `ArtifactEnvelope` 引用 primary payload checksum、所有 required input checksums、canonical status、owner、lifecycle、production effect 与 limitations；
- 该 slice 不读取 fresh cached market/macro data，`data_quality_required=false`，不得伪造 PASS DQ evidence；
- `RunLedger` 记录 `NOT_DUE -> DUE -> RUNNING -> PASS|BLOCKED`，其 status 由 spec 的 explicit status map 得出；
- sidecar 不加入旧 payload `artifact_paths`，避免 schema drift。

## 分阶段实施

### D1：Characterization 与 typed spec

- 冻结旧 runner payload/path/Markdown/strict/CLI semantics；
- 实现 immutable `ExperimentSpec`、input/output specs、plugin refs；
- 新增 governed YAML spec 与 typed loader/hash。

### D2：Generic runner 与 plugin interfaces

- 实现 execution context、calculator/report protocols、registry；
- generic source loading/lineage/runtime/strict pipeline；
- 全程复用 ARCH-004C writer/config/contracts。

### D3：N1 plugin 与 legacy façade

- 把现有 N1 wrapper 的 calculator adapter/Markdown renderer 迁到 capability plugin；
- 旧 `run_growth_tilt_candidate_family_closure()` 只解析兼容参数并调用 generic runner；
- CLI wiring 不改 command/options。

### D4：Envelope/ledger/report integration

- additive 写出 envelope/run-ledger sidecars；
- ReportSpec/report plugin 不重算结论；
- old primary/negative-ledger/Markdown exact parity；
- 更新 `docs/system_flow.md`。

### D5：Proof 与 closeout

- 用第二份 in-memory/spec fixture 证明同 calculator/plugin 的 variant 无需 Python module 或 CLI；
- architecture gate 禁止 generic runner 反向依赖具体 plugin/task wrapper；
- focused/scoped mypy/contract/full parallel validation；
- legacy façade登记 ARCH-004G sunset；
- 全部 PASS 后解锁 ARCH-004E。

## 验收标准

- ExperimentSpec deterministic round-trip，未知 plugin/status/input/output fail closed；
- generic runner module 不包含 `TRADING-2438N1`、growth-tilt-specific status 或 output name；
- old direct runner 与 generic runner 在固定 generated-at 下 primary payload、ledger JSON、Markdown exact bytes；
- old CLI name/options/exit/stdout key conclusions parity；
- strict missing source 继续输出相同 source id/path error；
- ready 与 blocked path 都有正确 canonical envelope/ledger status；
- data-quality not applicable 不得写成 passed DQ；
- same-plugin second spec 只新增 YAML/in-memory spec，不新增 Python module、CLI 或 report family；
- architecture dependency/direct-writer gate PASS；
- documentation/task/registry/catalog/flow consistency PASS；
- focused、scoped mypy、contract-validation、full parallel pytest PASS；
- strategy、threshold、weight、candidate、replay、research conclusion、promotion、paper-shadow、production 和 broker 均不改变。

## Legacy sunset

| façade | owner | sunset phase | removal gate |
|---|---|---|---|
| `dynamic_strategy_growth_tilt_candidate_family_closure.py` | research platform owner | ARCH-004G research lane | CLI consumers moved to generic experiment entry, two-run parity, full gate |
| task-specific CLI handler | CLI/platform owner | ARCH-004G CLI lane | generic `research experiment run` compatibility/cutover approved；旧 alias deprecation window complete |
| task-specific report registry command text | reporting owner | ARCH-004F3/G | generated ReportSpec registry becomes canonical with report parity |

## 明确不做

- 不重算或重解释 N1 closure；
- 不运行 N2、replay、backtest、scoring 或 cached data validation；
- 不创建新 candidate/baseline behavior/threshold；
- 不删除旧 CLI 或 report registry entry；
- 不把 sidecar 解释为 promotion/paper-shadow/production readiness；
- 不提前迁移其他 task-shaped modules。

## 状态记录

- 2026-07-11：ARCH-004C full gate 完成后登记并进入 `IN_PROGRESS`。选择 N1 closed/read-only/no-effect slice，开始 D1 characterization 与 typed ExperimentSpec；ARCH-004E 保持 blocked。
- 2026-07-11：D1～D4 完成，进入 D5 `VALIDATING`。typed/deterministic ExperimentSpec、generic runner、versioned plugin registry、N1 calculator/report plugins、legacy CLI façade、canonical writer、additive ArtifactEnvelope/RunLedger sidecars 和 same-plugin second-spec proof 已落地；focused=77、scoped mypy PASS、architecture gate=775 files / baseline 894 / current 893 / 0 violations。旧 artifacts 与 strict/CLI semantics parity，contract/full 尚待完成，E 继续 blocked。
- 2026-07-11：D5 完成并归档 `DONE`。Documentation/task/registry/catalog/flow consistency PASS，contract-validation=`197 passed`，full parallel=`5411 passed / 0 failed / 643 warnings`，artifact=`outputs/validation_runtime/full_20260710T185928Z/test_runtime_summary.json`。Reference slice 完整证明同 plugin variant 只加 spec、不加 task-id module；ARCH-004E entry gate 解锁。未重算 N1 closure或改变任何 strategy/replay/weight/promotion/paper-shadow/production/broker 状态。
