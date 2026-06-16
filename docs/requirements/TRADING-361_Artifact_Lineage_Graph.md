# TRADING-361 Artifact Lineage Graph

最后更新：2026-06-16

## 状态

- 当前状态：DONE
- 优先级：P2
- 下一责任方：项目 owner 后续复核 lineage coverage / legacy report cleanup 优先级
- Production boundary：只读 artifact lineage 审计；不刷新数据、不补造 artifact、不运行上游、不修改 research decision、不触发 broker/order/production。

## 背景

附件要求为 candidate research chains 建立 artifact lineage graph。当前系统已经有 report index、artifact catalog、cache catalog、data refresh audit、PIT manifest、signal input completeness、paper-shadow daily / drift / weekly、staleness monitor、readiness 和 owner review 等 artifacts，但缺少一个统一的、机器可读的 dependency graph 来解释候选为何到达某个 paper-shadow decision。

## 目标

新增只读 artifact lineage graph，按 candidate research chain 汇总关键 artifact families、latest artifact 可用性、checksum/metadata、依赖边和缺口，输出 JSON / Markdown，并在 Reader Brief 展示 lineage 覆盖状态。

必须覆盖的 node families：

- data artifacts
- cache catalog
- refresh audit
- PIT manifest
- signal artifacts
- daily paper-shadow artifacts
- drift monitor artifacts
- weekly review artifacts
- staleness monitor artifacts
- readiness reports
- owner reviews

## 范围

1. 从 report index、report registry 和已知 artifact paths 只读发现 latest artifacts。
2. 为每个 required family 建立 lineage node，记录 artifact id、family、path、exists、status、production effect、checksum、source status 和 reader impact。
3. 建立关键 dependency edges：
   - data artifacts -> cache catalog -> refresh audit -> PIT manifest
   - PIT manifest / signal artifacts -> daily paper-shadow
   - daily paper-shadow -> drift monitor / weekly review
   - drift monitor / weekly review / staleness monitor -> readiness reports
   - readiness reports -> owner reviews
4. 输出 JSON / Markdown report。
5. 增加 validation CLI，检查 required families、required edges、unsafe production effect、missing required artifacts 和 graph consistency。
6. Reader Brief 只读展示 latest lineage graph 摘要。
7. daily scheduler 在 report index 前生成 lineage graph，使同一轮 Reader Brief 能通过 report index 看见 latest lineage artifact。

## 非目标

- 不运行 paper-shadow daily、weekly review、readiness 或 owner review 上游命令。
- 不刷新 market/macro/PIT/signal 数据。
- 不修复、补造或重写缺失 artifact。
- 不改变 candidate decision、paper account、official target weights、broker action 或 production state。
- 不把 graph 缺口自动转换为 owner approval 或 promotion decision。

## 实施步骤

1. 新增 `src/ai_trading_system/reports/artifact_lineage.py`，提供 payload builder、validation、JSON/Markdown writer 和默认输出路径。
2. 在 `src/ai_trading_system/cli_commands/reports.py` 增加 `aits reports artifact-lineage` 和 `aits reports validate-artifact-lineage`。
3. 在 `src/ai_trading_system/cli_direct.py` 增加 direct dispatcher 覆盖。
4. 在 daily ops 和 `config/scheduled_tasks.yaml` 中把 lineage report 插入 `report_index` 前。
5. 在 `config/report_registry.yaml` 登记 `artifact_lineage_graph` 和 `artifact_lineage_validation`。
6. 在 Reader Brief 加入 `artifact_lineage_graph` summary。
7. 更新 README、operations runbook、artifact catalog 和 system flow。
8. 增加 focused tests 并运行验证。

## 验收标准

- `aits reports artifact-lineage --date YYYY-MM-DD` 可写出 JSON / Markdown。
- JSON 包含 `lineage_status`、`nodes`、`edges`、`blocking_issues`、`warning_issues`、`summary` 和 `safety_boundary`。
- 所有 required node families 均有 node；缺 required artifact 或 required edge 由 validation CLI fail closed。
- Validation CLI 输出 JSON / Markdown，`status` 为 `PASS`、`PASS_WITH_WARNINGS` 或 `FAIL`。
- report index 可扫描 `artifact_lineage_graph` artifact。
- Reader Brief JSON 暴露 latest lineage coverage / blocking issue 摘要。
- daily ops plan 和 scheduled task 顺序保持一致，lineage graph 位于 `report_index` 前。
- focused pytest、Ruff、compileall、git diff check、documentation contract、report index 和 Reader Brief/quality 通过或给出显式有限上下文原因。

## 进展记录

- 2026-06-16: 新增需求文档并登记任务，准备实现只读 artifact lineage graph。
- 2026-06-16: DONE。新增 `src/ai_trading_system/reports/artifact_lineage.py`、`aits reports artifact-lineage`、`aits reports validate-artifact-lineage`、daily-run / scheduled task / direct dispatcher integration、report registry entries、Reader Brief `Artifact Lineage Graph` 区块、artifact catalog、README、operations runbook、system flow 和 focused tests。真实 artifact `outputs/reports/artifact_lineage_graph_2026-06-15.json/md` 输出 `lineage_status=PASS`、families=11/11、edges=11/11、blocking=0、warnings=0；`outputs/reports/artifact_lineage_validation_2026-06-15.json/md` 输出 `validation_status=PASS`、checks=25、failed=0。`validate-data --as-of 2026-06-15` 为 `PASS_WITH_WARNINGS` / errors=0 / warnings=1，lineage 在该门禁后重新生成并验证通过。Report index `PASS_WITH_EXPLICIT_WAIVERS` / unwaived=0；Reader Brief OK；Reader Brief quality OK；documentation contract PASS；focused related pytest 80 passed；validation tiers `report-validation` 50 passed、`contract-validation` 61 passed、`fast-unit` 62 passed；Ruff PASS；compileall PASS。保持 read-only / no upstream rerun / no data refresh beyond explicit validation gate / no artifact repair / no paper-shadow decision mutation / no owner approval mutation / no official target / no broker / no production mutation。
