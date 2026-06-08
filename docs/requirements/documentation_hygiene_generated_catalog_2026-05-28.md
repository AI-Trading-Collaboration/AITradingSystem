# Documentation Hygiene & Generated Catalog

最后更新：2026-06-09

关联任务：`REPORT-052`

## 背景

`REPORT-047` 至 `REPORT-051` 已新增多个只读报告 artifact 和 `config/report_registry.yaml`。
报告数量继续增加后，单靠人工维护 `docs/artifact_catalog.md` 和
`docs/system_flow.md` 容易出现遗漏：registry 中有报告，但产物目录没有记录
生成命令、上游输入、schema/status、`production_effect` 或常见误解。

本任务不把 `docs/artifact_catalog.md` 改造成自动生成文件，也不拆分
`docs/task_register_completed.md`。第一阶段先建立可运行的文档契约检查，让
registry、artifact catalog 和后续 Reader Brief/report index 之间的边界可审计。

## 范围

- 新增只读 documentation contract 生成器，读取 `config/report_registry.yaml`
  和 `docs/artifact_catalog.md`。
- 输出 `outputs/reports/documentation_contract_YYYY-MM-DD.md/json`，列出每个
  registry report 是否被产物目录覆盖，以及对应 command、source artifact、
  schema/status、`production_effect` 和 common misread 摘要。
- 缺少 artifact catalog 覆盖、`production_effect` 或 common misread 时输出
  `FAIL`；命令或 schema/status 术语不完整先输出 warning，避免把自然语言
  文档解析做成脆弱门禁。
- CLI 入口为 `aits docs report-contract`，默认只读扫描，不运行任何上游报告，
  不修改 production、score、weights、position gates、paper trading 或 shadow
  artifacts。

## 非目标

- 不自动改写 `docs/artifact_catalog.md`。
- 不自动拆分 `docs/system_flow.md`、`docs/task_register.md` 或
  `docs/task_register_completed.md`。
- 不把 `config/report_registry.yaml` 提升为完整 artifact catalog 的唯一事实源。
- 不用自然语言相似度判断文档质量；第一阶段只做可解释、可复现的结构化覆盖检查。

## 阶段拆解

|阶段|状态|内容|验收标准|
|---|---|---|---|
|1|DONE|Documentation contract 生成器与 CLI|`aits docs report-contract --as-of YYYY-MM-DD` 写出 Markdown/JSON；JSON 固定 `production_effect=none`；缺文档覆盖 fail closed；目标测试覆盖 PASS 和 FAIL。|
|2|PROPOSED|生成式 docs/artifacts 索引|在第一阶段稳定后评估是否从 registry 和 catalog 生成 `docs/artifacts/*`，并明确哪些字段人工维护、哪些字段生成。|
|3|PROPOSED|completed task 月度归档策略|评估 `docs/task_register_completed.md` 的月度归档，不改变当前任务登记规则。|
|4|PROPOSED|文档格式/新鲜度集成|把 documentation contract 与既有 `docs validate-freshness` 形成可选本地文档质量检查组合。|

## 开放问题

- registry 是否应新增 per-report `production_effect` 字段，还是继续由
  artifact catalog 负责更细的 production/advisory/read-only 解释。
- documentation contract warning 是否应在 CI 中升级为失败。第一阶段只让 missing
  catalog coverage、missing production effect 和 missing common misread 失败。
- 后续 `docs/artifacts/*` 若采用生成文件，是否提交生成结果，还是只提交生成器和
  release artifact。

## 进展记录

- 2026-05-28：REPORT-052 第一阶段进入 IN_PROGRESS。范围限定为只读
  documentation contract，不拆分现有大文档，不引入 artifact catalog 双源维护。
- 2026-05-28：第一阶段进入 VALIDATING。新增 `src/ai_trading_system/documentation_contract.py`
  和 `aits docs report-contract`，输出 `documentation_contract_YYYY-MM-DD.md/json`；
  补齐 `parameter_governance` 与 `backtest_robustness` 的 artifact catalog 行，并把
  documentation contract 自身纳入 `config/report_registry.yaml`。默认 registry/catalog
  契约检查为 PASS；验证通过 documentation/report/docs 相关目标 pytest 75 passed、
  ruff 和 black。
- 2026-06-09：第一阶段从 `VALIDATING` 改为 `DONE`。最新
  `aits docs report-contract --latest` 输出 `documentation_contract_2026-06-05.md/json`，
  状态 `PASS`、`production_effect=none`、registry reports 166、missing catalog 0、
  errors 0、warnings 0；JSON 字段级复核 `summary.report_count=166`、
  `catalog_documented_count=166`、`issues=0`、non-documented records 0。阶段 2-4
  仍保持 PROPOSED，作为后续增强，不阻塞 REPORT-052 第一阶段收口。
