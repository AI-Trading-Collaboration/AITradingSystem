# TRADING-487 to 504 Engineering Closeout and Weight Research Turn

最后更新：2026-06-19

## 背景

TRADING-471 到 TRADING-485 已完成 executable research evidence repair 和 v2
research-only cycle。最终状态为 `V2_RESEARCH_CYCLE_RETURN_TO_BACKLOG`，没有 append
新 owner decision，没有恢复 normal paper-shadow，没有批准 extended shadow/live trading，
没有生成 official target weights，没有 broker/order 或 production mutation。

当前阶段判断：

- 工程治理、报告、artifact 和 Reader Brief 基础能力已基本形成。
- 研究候选可被执行和评估，但证据不足，不能进入 paper-shadow。
- 后续优先级应从继续扩展治理/report/dashboard，转为工程收尾、平台冻结、权重研究复盘和下一轮研究方向定义。

## 编号映射

附件建议使用 `TRADING-486` 到 `TRADING-499` 作为工程收尾任务，并使用
`TRADING-500` 到 `TRADING-504` 做权重研究转向。

当前工作区已有未提交任务：

- `TRADING-486_SECRET_HYGIENE_GENERATED_REPORT_FALSE_POSITIVE`

为避免任务 ID 复用，本批次在 task register 中登记为：

- `TRADING-487_to_504_ENGINEERING_CLOSEOUT_AND_WEIGHT_RESEARCH_TURN`

附件中的 `TRADING-486 engineering-surface-inventory-and-freeze-scope` 作为本批次
Stage A1 处理。本轮已实现 A1 到 A4 的只读工程控制面 baseline、Stage B
readiness baseline 和独立 `reproducibility` validation tier：surface inventory、canonical
system status / doctor、report latest 查询、artifact lifecycle inventory、Stage B readiness
doctor 和 artifact reproducibility runtime evidence；Stage B schema/config/manifest/test/error
taxonomy readiness 已达到 `ENGINEERING_STAGE_B_READY` / validation `PASS`；Stage C 已建立
documentation IA baseline，并把 Reader Brief consistency 升级为 effective view model：
非 daily Reader Brief artifact 先用原生 `reader_brief` / 文本字段，缺标准区块时由
report index metadata 补齐 Summary、Key Result、Blocking Issues、Warnings、Safety
Boundary 和 Next Action；源 artifact 原生缺口保留为 `native_template_gap` 审计项，不改写
历史 artifact。真实 `reader_brief_consistency_pack_2026-06-19` 和 validation 均为 `PASS`；
legacy waiver cleanup 已把 30 个历史/可选 report family 迁移为 registry
`visibility_policy` 并将 waiver policy 收口为 `waivers: []`；真实 report index 为 `PASS`，
waiver inventory 为 0 active。clean-clone release acceptance 已在 clean worktree 上
输出 `CLEAN_CLONE_ACCEPTANCE_PASS`，release candidate 输出
`ENGINEERING_CLOSEOUT_READY` 且 validation=`PASS`；Stage D weight research RFC baseline
已补齐。
后续如需要拆分单项任务，应使用未占用的新 ID，或在本批次文档中维护阶段状态。

## 阶段边界

TRADING-487 to 499 等价于附件的工程收尾阶段：

- 不修改候选研究逻辑。
- 不激活 paper-shadow、extended shadow、live trading。
- 不生成 official target weights。
- 不触发 broker/order。
- 不执行 production mutation。

TRADING-500 to 504 等价于权重研究复盘与方向定义阶段：

- 先复盘失败 taxonomy、权重控制架构、消融协议、统计验证和 holdout 策略。
- 不急于实现新候选。
- 后续 TRADING-505+ 只能实施通过 RFC 和消融协议批准的研究实验。

## Stage A：功能入口与系统表面收口

### A1 Engineering surface inventory and freeze scope

目标：

- 盘点 CLI 命令、report/validate 命令、artifact families、schemas、registry entries、
  Reader Brief 模板、configuration files、task-specific modules、active/legacy/deprecated docs、
  scheduler / operations entry points。
- 每个 surface 标记为 `KEEP`、`MERGE`、`DEPRECATE`、`ARCHIVE` 或
  `REMOVE_AFTER_COMPATIBILITY_WINDOW`。
- 不修改策略逻辑。

实现状态：

- 2026-06-19：新增 `aits reports engineering-surface-inventory --as-of YYYY-MM-DD`。
- 2026-06-19：新增 `aits reports validate-engineering-surface-inventory --latest`。
- 2026-06-19：新增 report registry、artifact catalog 和 system flow 登记。
- 2026-06-19：开始 A2/A3，新增稳定 `aits system status` / `aits system doctor`
  控制面和 `aits reports latest` report artifact 查询入口，旧 task-specific 命令保留为
  兼容入口，不在本阶段删除。
- 2026-06-19：真实 inventory 输出
  `ENGINEERING_SURFACE_INVENTORY_READY_WITH_LIMITATIONS`，surfaces=2005，
  unknown=0，required surface types=7/7；validation 为 `PASS_WITH_WARNINGS`，
  warning 仅披露现存 merge scope / compatibility alias 限制。

验收标准：

- 每个扫描到的公开 surface 都有冻结分类。
- 必需 surface type 覆盖 CLI、report registry、artifact catalog、configuration、operations、docs。
- validation 对 unknown classification fail closed。
- 输出固定 `production_effect=none`，并声明不运行上游、不刷新数据、不修改策略/候选/production。

### A2 Canonical CLI and workflow consolidation

目标：

- 把 task-specific CLI 收口为少量稳定 workflow 入口。
- 旧命令保留兼容别名并显示 deprecation 提示。
- 新候选优先通过 spec/config 接入通用 runner。

候选稳定入口：

- `system doctor`
- `system status`
- `research run`
- `research validate`
- `reports latest/open`
- `artifact reproduce`

验收标准：

- 新用户不需要知道 task ID 即可完成标准流程。
- 历史脚本不立即失效。
- canonical workflow 有端到端测试。

实现计划：

- `aits system status --as-of YYYY-MM-DD` 生成只读 canonical system status bundle。
- `aits system doctor --as-of YYYY-MM-DD` 校验 status bundle 的安全边界、report index
  未豁免 warning、核心 artifact 可见性和 canonical workflow 定义。
- `aits reports latest --report-id REPORT_ID` 从 report registry / report index 查找当前
  artifact，避免手工猜测 latest 文件名。
- 旧命令通过 status bundle 的 `legacy_compatibility` 字段暴露迁移路径，本轮不删除旧入口。

实现状态：

- 2026-06-19：新增 `aits system status` 和 `aits system doctor`，统一输出
  `canonical_system_status_YYYY-MM-DD.{json,md}` 与
  `canonical_system_doctor_YYYY-MM-DD.{json,md}`。
- 2026-06-19：新增 `aits reports latest --report-id REPORT_ID`，输出 freshness、
  artifact status、visibility、latest artifact name/path、owner action 和只读
  production boundary。
- 2026-06-19：canonical workflow 已覆盖 `system_doctor`、`system_status`、
  `research_status`、`research_run`、`research_validate`、`reports_latest` 和
  `artifact_reproduce`；旧 task-specific CLI 尚未删除或隐藏。

### A3 Canonical status and report bundle

第一屏必须包含：

- Current system state
- Active research candidate
- Latest research gate
- Data health
- Validation health
- Current blockers
- Current warnings
- Safety boundary
- Recommended next action
- Source artifacts

验收标准：

- owner、researcher、operator 都能从一个入口理解当前状态。
- Reader Brief 不把 validation PASS 表述成 research result PASS。
- 没有多个互相冲突的 latest status。

实现状态：

- 2026-06-19：Stage C legacy waiver cleanup 后，真实 `aits system status --as-of
  2026-06-19` 输出 `ENGINEERING_CONTROL_PLANE_READY`，latest research gate 为
  `V2_RETURN_TO_HYPOTHESIS_BACKLOG`，data health=`PASS`，
  validation health=`PASS`，recommended next action 为
  `revise_v2_hypothesis_after_owner_review`。
- 2026-06-19：Stage C legacy waiver cleanup 后，真实 `aits system doctor --as-of
  2026-06-19` 输出 `PASS`，checks=9，failed=0，warnings=0。

### A4 Artifact lifecycle, retention and latest pointers

生命周期状态：

- `CURRENT`
- `SUPERSEDED`
- `ARCHIVED`
- `INVALID`
- `LEGACY`

验收标准：

- latest 不依赖文件名猜测。
- 历史报告可追溯。
- 默认界面不展示大量过期 artifact。
- archive 不影响复现。

实现状态：

- 2026-06-19：A2/A3 status bundle 已汇总 report index 中的 lifecycle visibility，
  并提供 `aits reports latest` 查询入口。
- 2026-06-19：新增 `aits reports artifact-lifecycle-inventory --as-of YYYY-MM-DD`
  和 `aits reports validate-artifact-lifecycle-inventory --latest`，把 report index /
  report registry artifact globs 解析为 `CURRENT|SUPERSEDED|ARCHIVED|INVALID|LEGACY`
  lifecycle inventory，并披露 latest pointer、retention/default visibility、candidate /
  superseded / archive counts。
- 2026-06-19：真实 A4 inventory 输出
  `ARTIFACT_LIFECYCLE_READY_WITH_LIMITATIONS`，validation 为 `PASS_WITH_WARNINGS`；
  限制来自 legacy / superseded artifact review；Stage C 后 report index missing/stale
  和 explicit waivers 均为 0，历史/可选导航面通过 registry `visibility_policy` 披露。
- 尚未执行真实 archive / deletion / pointer rewrite；A4 当前是只读 signoff baseline，
  不是物理清理。物理归档/删除若未来需要，必须单独登记并审计。

## Stage B：可维护性与可复现性收尾

覆盖附件中的 schema/config/version consolidation、reporting module boundary refactor、
reproducibility manifest and doctor、test-suite architecture/runtime budget、structured logging
and error taxonomy。

关键验收标准：

- 核心 artifact 有明确 schema version。
- run manifest 记录 git commit、command、resolved config、input artifacts/checksums、
  schema versions、as-of date、random seed、environment summary、output artifacts、elapsed time
  和 warnings。
- 测试分层为 fast unit、schema/contract、report validation、integration、
  reproducibility、slow research regression。
- CLI 普通 gate blocker 不表现为 internal exception。

实现计划：

- 新增 `config/engineering_closeout_policy.yaml`，把 Stage B 的 schema/config、
  reproducibility manifest、validation tier 和 error taxonomy 期待字段显式配置化。
- 新增 `aits reports engineering-stage-b-readiness --as-of YYYY-MM-DD` 和
  `aits reports validate-engineering-stage-b-readiness --latest`，只读扫描 report index、
  latest JSON artifacts、config files、run manifests、validation runtime artifacts、
  validation tier runner 和 source error taxonomy coverage。
- Validation 对缺 policy、缺 required validation tiers、report index unwaived warning、
  unsafe production effect 或缺安全边界 fail closed；schema/config/profile/manifest/error
  taxonomy 的不完整项作为 Stage B 限制披露，不冒充已完成重构。

实现状态：

- 2026-06-19：新增 `config/engineering_closeout_policy.yaml`，显式记录 Stage B
  required artifact fields、run manifest fields、validation tiers、error categories 和 log
  fields。
- 2026-06-19：新增 `aits reports engineering-stage-b-readiness --as-of YYYY-MM-DD`
  和 `aits reports validate-engineering-stage-b-readiness --latest`。
- 2026-06-19：真实 Stage B readiness 输出
  `ENGINEERING_STAGE_B_READY_WITH_LIMITATIONS`，validation 为 `PASS_WITH_WARNINGS`；
  当时限制包括 latest JSON schema_version 缺口、run manifest required field 缺口、
  error taxonomy 尚未 centralized、reports.py boundary pressure。
- 2026-06-19：新增独立 `reproducibility` validation tier 和 `artifact-reproduce` alias；
  真实运行 `python scripts/run_validation_tier.py reproducibility --write-runtime-artifact`
  通过，最终 23 tests PASS，runtime artifact 写入
  `outputs/validation_runtime/trading-487-stage-b-reproducibility/test_runtime_summary.json`；
  重新生成 Stage B readiness 后 `missing_tiers=0`。
- 2026-06-19：新增 `src/ai_trading_system/error_taxonomy.py` 中心模块，覆盖
  `INPUT_MISSING`、`INPUT_STALE`、`SCHEMA_INCOMPATIBLE`、`CONFIG_INVALID`、
  `ARTIFACT_NOT_FOUND`、`REPORT_SOURCE_INCOMPLETE`、`RESEARCH_GATE_BLOCKED`、
  `SAFETY_BOUNDARY_BLOCKED` 和 `INTERNAL_ERROR`，并提供 required log fields 与
  `build_error_record` helper；`tests/test_error_taxonomy.py` 校验中心模块与
  `config/engineering_closeout_policy.yaml` 一致。重新生成 Stage B readiness 后
  `missing_error_category_count=0`、`error_taxonomy_central_module_exists=true`，
  validation failed=0、warnings=4。
- 2026-06-19：拆分 `src/ai_trading_system/cli_commands/reports.py` 中的 engineering
  closeout / report-index CLI adapters 到 `engineering_reports.py` 和
  `report_index_commands.py`，保留原 `aits reports ...` 命令名不变。重新生成 Stage B
  readiness 后 `reports_cli_line_count=11920`、`reports_cli_within_budget=true`，
  CLI boundary budget 已达标。
- 2026-06-19：新增 `config/config_contract_registry.yaml` 覆盖 legacy config metadata，
  补齐 daily run manifest writer、validation runtime summary、core dashboard payload 和
  ETF dynamic_v3 runtime artifact schema/report/status fields；pre-policy historical
  manifests 进入 legacy 统计，不再计入当前 Stage B warning。重新生成
  `engineering_stage_b_readiness` 后状态为 `ENGINEERING_STAGE_B_READY`，
  validation 为 `PASS`、failed=0、warnings=0；`reproducibility` tier 真实运行 23 tests
  PASS。
- 2026-06-19：对本地 ignored runtime artifacts 做 metadata-only contract migration：
  重新生成 2026-06-18 core dashboard JSON，并给 5 个 pre-contract ETF dynamic_v3 latest
  JSON 只追加 `schema_version` / `report_type` / `status` 等 contract 字段。原因是
  report index latest 仍指向这些本地旧格式 runtime artifacts；行为影响仅限 metadata，
  不改权重、指标、研究结论或 safety 字段；风险是本地 artifacts 与原始生成时间后的
  contract 字段不同；验证覆盖为 Stage B readiness `PASS`、相关 generator tests 和
  `reproducibility` tier；退出条件是这些 artifacts 被新生成器产出的同类 latest artifact
  自然替代后，不再需要 metadata migration。
- Stage B readiness contract 已满足；Reader Brief effective view model、legacy waiver cleanup、
  clean-clone release acceptance 和 platform freeze release candidate 已完成，当前 release
  candidate 为 `ENGINEERING_CLOSEOUT_READY`。

## Stage C：阅读体验、发布与平台冻结

覆盖附件中的 documentation information architecture、Reader Brief usability pass、
legacy deprecation cleanup、clean-clone release acceptance 和 platform freeze release candidate。

最终状态只能是：

- `ENGINEERING_CLOSEOUT_READY`
- `ENGINEERING_CLOSEOUT_READY_WITH_DOCUMENTED_LIMITATIONS`

不接受：

- `ENGINEERING_CLOSEOUT_BLOCKED`

限制必须是研究限制，不能是安装、文档、registry 或 reproducibility 缺陷。

实现状态：

- 2026-06-19：新增 `docs/platform_user_guide.md`，把新用户入口收口为
  Researcher、Operator、Owner / Reviewer 三条路径，并覆盖 15 分钟 quickstart、
  系统架构图入口、artifact 生命周期、状态枚举、Reader Brief 标准结构、
  troubleshooting decision tree、validation PASS 不等于 candidate PROMISING 和
  从 spec 到 final snapshot 的完整例子。
- 2026-06-19：README 顶部新增当前主入口表，指向 platform user guide、canonical
  status / doctor、operations runbook、artifact catalog 和 system flow，降低先读
  task register 的依赖。
- 2026-06-19：Reader Brief consistency 改为 effective view model：对非 daily
  Reader Brief 的 report index 记录，先读取原生 `reader_brief` / `reader_brief_section` /
  文本区块，再用 report index 的 `title`、`artifact_status`、`freshness_status`、
  `visibility_status`、`owner_action` 和 `production_effect` 补齐六个标准字段。真实
  `reader_brief_consistency_pack_2026-06-19` 输出 `PASS`，missing sections=0，
  unclear decisions=0，native template gaps=1599，view-model derived sections=1599；
  validation 输出 `PASS`、failed=0、warnings=0。该改动不改写历史 artifact、不运行上游、
  不刷新数据、不修改研究结论或 production state；原生模板缺口仍作为后续模板迁移审计项。
- 2026-06-19：新增 `scripts/run_clean_clone_release_acceptance.py` 和
  `clean_clone_release_acceptance` report registry / artifact catalog / system flow 入口。
  runner 在 clean worktree 上 local clone 当前 repo、创建 editable venv、生成 minimal sample
  project，并运行 CLI help、engineering surface inventory、inventory validation、report index、
  latest report lookup、canonical system status、system doctor 和可选 `artifact-reproduce`
  tier；dirty worktree 默认 fail closed，显式 dirty snapshot 只能输出
  `CLEAN_CLONE_ACCEPTANCE_BLOCKED_UNCOMMITTED_CHANGES`，不能作为 release PASS。
- 2026-06-19：真实 clean worktree acceptance 已重跑通过，artifact
  `clean_clone_release_acceptance_2026-06-19` 输出
  `CLEAN_CLONE_ACCEPTANCE_PASS`，steps=11、failed=0、blocking=0；此前 Windows
  checkout 路径过长导致 `git_clone` fail closed，已通过缩短默认 work dir 到 `run/ccra`
  并为 clone 启用 `core.longpaths=true` 修复，补充回归测试后重新验证。
- 2026-06-19：新增 `aits reports engineering-closeout-release-candidate --as-of
  YYYY-MM-DD` 和 `aits reports validate-engineering-closeout-release-candidate --latest`。
  该 release candidate report 只读聚合 latest status/doctor、report index、Stage B
  readiness、Reader Brief consistency、task-register consistency、clean-clone acceptance 和
  release-blocking validation runtime artifacts，输出 release version/tag、changelog、stable
  CLI/schema、compatibility policy、post-freeze change admission rules 和 engineering
  closeout Reader Brief；clean clone 未 PASS、dirty worktree 或 release-blocking tier failure
  必须保持 `ENGINEERING_CLOSEOUT_BLOCKED`。
- 2026-06-19：release candidate 已在 clean worktree 上重跑通过，artifact
  `engineering_closeout_release_candidate_2026-06-19` 输出
  `ENGINEERING_CLOSEOUT_READY`，checks=11、blocking=0、warnings=0；validation 输出
  `PASS`，checks=4、failed=0、warnings=0。README 历史段落瘦身和 legacy command/schema
  cleanup 可作为后续普通整理项处理，不阻塞本轮 platform freeze release candidate。

## Stage D：权重研究复盘与方向定义

覆盖附件中的 TRADING-500 到 TRADING-504：

- failure taxonomy
- weight-control architecture RFC
- ablation and baseline protocol
- statistical validation and holdout policy
- next research program roadmap

下一阶段架构原则：

- 不再让单一 regime-to-weight 规则同时承担 signal、allocation、risk、execution。
- 不让平滑器同时处理 noise、急跌、V 型恢复和成本控制。
- 使用战略基准权重、慢速相对倾斜、快速非对称风险 overlay、confidence shrinkage、
  execution / turnover control 的分层接口。

实现状态：

- 2026-06-19：新增 `docs/research/weight_research_turn_2026-06-19.md`，覆盖
  TRADING-500 到 TRADING-504 的 failure taxonomy、weight-control architecture RFC、
  B0-B6 ablation baseline protocol、statistical validation / holdout policy 和 3 个下一阶段
  research-only 候选路线图。
- 该文档只定义下一阶段研究协议，不实现候选、不运行 backfill、不批准 paper-shadow、
  不生成 official target weights、不触发 broker/order 或 production mutation。

## 当前禁止事项

在整个批次中继续禁止：

- paper-shadow activation
- extended shadow
- live trading
- official target weights
- broker integration
- order ticket
- production mutation
- automatic position control

研究 backfill 和 hypothetical weight 只能用于 research-only 分析，不得解释为实盘建议。

## 验证计划

本轮 baseline 验证：

- `pytest tests/test_engineering_closeout.py -q`
- `pytest tests/test_canonical_system_status.py tests/test_engineering_closeout.py -q`
- `pytest tests/test_canonical_system_status.py tests/test_engineering_closeout.py tests/test_report_index.py tests/test_task_register_consistency.py tests/test_docs_freshness.py tests/test_documentation_contract.py tests/test_reader_brief_consistency.py -q`
- `pytest tests/test_engineering_closeout.py tests/test_canonical_system_status.py tests/test_artifact_lifecycle_inventory.py tests/test_engineering_stage_b_readiness.py tests/test_validation_tier_script.py tests/test_engineering_closeout_docs.py tests/test_task_register_consistency.py tests/test_docs_freshness.py tests/test_documentation_contract.py -q`
- `pytest tests/test_error_taxonomy.py tests/test_engineering_stage_b_readiness.py -q`
- `pytest tests/test_engineering_closeout_docs.py tests/test_validation_tier_script.py tests/test_engineering_stage_b_readiness.py -q`
- `python -m compileall -q src`
- `ruff check src tests`
- `ruff check src tests scripts`
- `git diff --check`
- `python scripts/run_validation_tier.py reproducibility --write-runtime-artifact --artifact-dir outputs/validation_runtime/trading-487-stage-b-reproducibility`
- `aits reports engineering-surface-inventory --as-of 2026-06-19`
- `aits reports validate-engineering-surface-inventory --latest`
- `aits system status --as-of 2026-06-19`
- `aits system doctor --as-of 2026-06-19`
- `aits reports reader-brief-consistency --as-of 2026-06-19`
- `aits reports validate-reader-brief-consistency --source-json-path outputs/reports/reader_brief_consistency_pack_2026-06-19.json`
- `aits reports latest --report-id candidate_v2_research_cycle_snapshot --as-of 2026-06-19`
- `aits reports artifact-lifecycle-inventory --as-of 2026-06-19`
- `aits reports validate-artifact-lifecycle-inventory --source-json-path outputs/reports/artifact_lifecycle_inventory_2026-06-19.json`
- `aits reports engineering-stage-b-readiness --as-of 2026-06-19`
- `aits reports validate-engineering-stage-b-readiness --source-json-path outputs/reports/engineering_stage_b_readiness_2026-06-19.json`
- `aits reports index --as-of 2026-06-19`
- `aits reports task-register-consistency run --as-of 2026-06-19`
- `aits reports task-register-consistency validate --source-json-path outputs/reports/task_register_consistency_2026-06-19.json`
- `aits reports task-register-consistency validate --latest`
- `aits docs validate-freshness`

后续阶段验证按实际触及范围扩展到 task-register consistency、documentation contract、
report index、Reader Brief quality、validation tiers 和 clean-clone smoke。

## 状态记录

- 2026-06-19：新增批次需求文档和 task-register 行；因 `TRADING-486` 已被 secret
  hygiene P0 占用，本批次登记为 `TRADING-487_to_504...`，附件中的工程表面盘点作为
  Stage A1 实现。
- 2026-06-19：Stage A1 实现并验证；inventory 为
  `ENGINEERING_SURFACE_INVENTORY_READY_WITH_LIMITATIONS`，validation 为
  `PASS_WITH_WARNINGS`，unknown classification=0。
- 2026-06-19：Stage A2/A3 baseline 实现并验证；canonical status 为
  `ENGINEERING_CONTROL_PLANE_READY_WITH_LIMITATIONS`，doctor 为 `PASS_WITH_WARNINGS`，
  report index 为 `PASS_WITH_EXPLICIT_WAIVERS` 且 unwaived=0；当时尚未完成 A4、
  Stage B、Stage C 和 Stage D。
- 2026-06-19：Stage A4 baseline 实现并验证；artifact lifecycle inventory 为
  `ARTIFACT_LIFECYCLE_READY_WITH_LIMITATIONS`，validation 为 `PASS_WITH_WARNINGS`，
  report index unwaived=0；尚未完成 Stage B、Stage C 和 Stage D。
- 2026-06-19：Stage B readiness baseline 实现并验证；`engineering_stage_b_readiness`
  为 `ENGINEERING_STAGE_B_READY_WITH_LIMITATIONS`，validation 为 `PASS_WITH_WARNINGS`，
  report index unwaived=0；独立 `reproducibility` tier 后续补齐并真实通过，Stage B
  真实 schema/config/manifest 迁移、error taxonomy 全量接入和 reports CLI boundary baseline
  当时仍未完成，Stage C 和 Stage D 尚未完成。
- 2026-06-19：Stage B contract 收口完成；新增 config contract registry，补齐 manifest
  / validation runtime / dashboard / ETF runtime artifact contract 字段，pre-policy run
  manifests 作为 legacy 指标披露；最终 readiness 为 `ENGINEERING_STAGE_B_READY`，
  validation 为 `PASS`、failed=0、warnings=0，`reproducibility` tier 23 tests PASS。
- 2026-06-19：Stage C documentation IA baseline 与 Stage D weight research turn baseline
  已建立；新增 platform user guide、README 三路径入口、weight research turn RFC 和
  docs contract tests。Stage C release acceptance / platform freeze 仍未完成。
- 2026-06-19：Stage C Reader Brief usability baseline 前进；Reader Brief consistency
  使用 effective view model 补齐非 daily Reader Brief artifact 的六个标准阅读字段，真实
  pack 和 validation 均为 `PASS`，missing sections=0、unclear decisions=0、warnings=0；
  `native_template_gap_count=1599` 继续作为源模板迁移审计项。
- 2026-06-19：Stage C legacy waiver cleanup 前进；新增 report registry
  `visibility_policy=current|legacy_optional|deprecated_optional|archived_optional`，把 30 个
  历史/可选 report family 从 active waiver 迁移为 `LEGACY_*` / `DEPRECATED_*` 可见性状态；
  `config/report_index_visibility_waivers.yaml` 收口为 `waivers: []`。真实 report index 为
  `PASS`，missing=0、stale=0、explicit_waiver_count=0、unwaived=0、
  non_current_visibility_count=30；waiver inventory 和 validation 均为 `PASS`，active=0；
  canonical system status 为 `ENGINEERING_CONTROL_PLANE_READY`，doctor 为 `PASS`、
  warnings=0。
- 2026-06-19：release-blocking validation tiers 已重跑并写入 runtime artifacts：
  `fast-unit`、`contract-validation`、`report-validation` 和 `reproducibility` 均为 `PASS`。
  `fast-unit` 首次发现 direct CLI 兼容回归（`reports_cli.report_index_command` 拆分后未
  re-export），已通过恢复兼容导出修复并复跑通过。该验证暴露出提交后的 clean clone
  不能依赖本地 ignored `outputs/reports` runtime artifacts，必须由 sample/minimal workflow
  先生成必要 source artifacts，再运行 doctor、report、validation 和 artifact reproduce。
- 2026-06-19：clean-clone sample/minimal workflow 已实现为
  `scripts/run_clean_clone_release_acceptance.py`，并登记
  `outputs/reports/clean_clone_release_acceptance_YYYY-MM-DD.json/md`。实现期间工作树仍有未提交
  工程收尾变更，因此先用 `--allow-dirty-snapshot` 生成 BLOCKED smoke artifact。真实
  dirty-snapshot artifact 为 `CLEAN_CLONE_ACCEPTANCE_BLOCKED_UNCOMMITTED_CHANGES`，
  steps=10、failed=0、blocking=1，且 `artifact_reproduce_validation_tier` PASS；真实
  `CLEAN_CLONE_ACCEPTANCE_PASS` 后续已在提交后的 clean worktree 重新运行通过。
- 2026-06-19：TRADING-499 release candidate artifact 已实现；新增
  `engineering_closeout_release_candidate` / validation report family，用于记录 release
  version/tag、changelog、stable CLI/schema、compatibility policy、post-freeze change
  admission rules 和 engineering closeout Reader Brief。dirty worktree / dirty-snapshot
  clean-clone evidence 会让该 report fail closed；提交后真实 clean-clone PASS 已完成，
  release candidate 输出 `ENGINEERING_CLOSEOUT_READY` 且 validation=`PASS`。
