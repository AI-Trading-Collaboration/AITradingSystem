# ARCH-005M2：Bootstrap Standalone Validator Portable Bundle Adoption

最后更新：2026-07-27

稳定任务 ID：`ARCH-005M2_BOOTSTRAP_STANDALONE_VALIDATOR_PORTABLE_BUNDLE_ADOPTION`

Owner continuation：
`owner_continuation:ARCH-005M2:2026-07-27:continue_long_term_engineering_goal`

状态：`VALIDATING_CLEAN_CLONE`

## 1. 问题与目标

ARCH-004 G2.5 已把 bootstrap handoff 所需四份历史 validation summary 以 exact raw bytes
写入 tracked `arch_005_bootstrap_validation_bundle.v1`，因此 G2.5 gate 可以在 clean clone 中
摆脱未跟踪 `outputs/`。但这套能力仍内嵌在 G2.5 readiness 模块：

- 其他 standalone validator 无法复用 bundle 的 path/SHA/content 校验；
- 当前 bundle loader 不直接校验每份 summary 的 `status`、`exit_code` 与 runtime tier；
- Git lineage、handoff frozen blobs 和 bundle bytes 的组合验证没有独立 CLI；
- clean-clone 可执行性只作为 G2.5 整体测试的隐含能力，不是单独可调用合同。

本任务把既有模式抽象为可复用的 portable validator，同时保持历史 handoff、四份 summary
bytes、artifact SHA、路径和 G2.5 结论不变。

## 2. 权威与不变量

- 唯一 bundle bytes authority 仍是
  `inputs/architecture/arch_005_bootstrap_validation_bundle.json`；
- bundle path 与 SHA authority 仍由
  `config/architecture/arch_004_g2_5_readiness.yaml` 提供；
- canonical handoff 仍是
  `inputs/architecture/arch_005_bootstrap_handoff.yaml`；
- 不创建 bundle 副本、sidecar 或第二套 artifact fact source；
- 不改写历史 handoff、四份内嵌 summary bytes、原始 path 或 SHA；
- standalone validator 必须依赖完整 Git history 来验证
  `handoff base -> handoff HEAD -> requested source base`；
- `production_effect=none`、`broker_action=none`，不修改 DQ/PIT、策略、研究窗口、阈值、
  backtest、report conclusion 或 periodic operations。

## 3. 设计

### S0：合同冻结与 characterization

- 保留 `arch_005_bootstrap_validation_bundle.v1`；
- 冻结四个 bundle tier 到 runtime tier 的映射：
  `focused -> fast-unit`、
  `architecture_fitness -> architecture-fitness`、
  `contract_validation -> contract-validation`、
  `full_validation -> full`；
- size 是对 exact decoded bytes 的确定性度量，不单独创建可漂移的 size authority；
- SHA 同时绑定 bundle row、handoff record 与 decoded bytes，因此 CRLF/LF 变化必须 fail closed。

### S1：可复用 validator primitive

- 新增独立模块，严格校验 bundle/handoff schema、portable path、bundle file SHA、handoff
  path/SHA/checksum、artifact count/tier/path/SHA/base64；
- 解析每份 exact summary 并校验 `status=PASS`、`exit_code=0`、requested/resolved/runtime
  tier 一致；
- 从 handoff HEAD 读取 6 份 frozen tracked Git blobs，并调用 canonical handoff validator；
- 验证 handoff base、handoff HEAD、requested source base 的存在性和祖先关系；
- 返回 content-derived、中文可解释的结构化 PASS summary，不写入 repository。

### S2：G2.5 adoption 与 standalone CLI

- G2.5 现有 loader 改为复用 portable primitive，保持原函数签名、返回结构和 error code；
- 新增只读 CLI，默认从现有 G2.5 policy 取得 bundle/handoff path 和 exact bundle SHA；
- CLI 输出 JSON 到 stdout，可显式指定 source base，默认解析当前 `HEAD`；
- clean local clone 在没有 `outputs/validation_runtime/**` 时仍可运行 PASS。

### S3：验证与收口

- focused tests覆盖 canonical PASS、G2.5 adapter parity、clean-clone CLI；
- 负例覆盖 bundle/handoff missing、bundle hash tamper、artifact base64/content tamper、
  CRLF normalization、path/SHA/status/exit/tier drift、重复/缺失 tier、handoff checksum drift、
  unknown/non-descendant source base 和缺失 Git history；
- 更新 system flow、artifact catalog、task register、generated views 与 append-only
  compatibility authority；
- 运行 Architecture、Contract、Reproducibility、Integration 与风险相称的 required Full。

## 4. 验收标准

- standalone validator 不读取 untracked `outputs/`；
- 四份 artifact 的 path、SHA、decoded size、status、exit code、bundle tier 与 runtime tier
  均在结果中可审计且机械验证；
- handoff canonical validation 与三段 Git lineage 全部 PASS；
- clean clone 删除/不存在 `outputs/` 时 CLI PASS；
- tamper、missing、CRLF、base drift 和 history missing 均 typed fail closed；
- G2.5 build/validate 与既有 tests 保持兼容；
- 没有第二套事实源，没有历史 evidence rewrite；
- 无 production、broker、数据、策略或投资结论变化。

## 5. 分支与生命周期

- frozen base：`b3ce8d70e3917522d9abdaf4d168f812ff47878e`；
- task branch：`codex/arch-005m2-portable-bootstrap-bundle`；
- 本任务不创建额外 Git worktree、clone 或长期外部 cache；
- tests 创建的临时 local clone 由 pytest temporary directory 管理并在测试结束后删除；
- 如 main 前进，保留当前 frozen lane，按 DEVX-006 生成一次真实 base-drift plan，不创建
  v2/v3 重建链；
- tracked implementation 可由 commit 恢复；正式 validation artifacts 在 closeout 前迁入
  canonical 主工作区并校验；
- known-unrelated owner 文档不得读取、修改或提交。

## 6. 进度

- 2026-07-27：当前 main/remote exact=`b3ce8d70e3917522d9abdaf4d168f812ff47878e`；
  governed `SINGLE_LANE START` preflight PASS，active lease=0，任务由`PROPOSED`进入
  `IN_PROGRESS`。本轮只处理 bootstrap validation portability，不推进 DATA-GOV C3、
  ARCH-005M1 remaining loaders、G5、S5、strategy、production 或 broker。
- 2026-07-27：portable primitive、G2.5 adapter、standalone CLI、typed negative tests、
  system flow与artifact catalog接线完成。canonical CLI输出4份artifact的exact
  path/SHA/decoded size/status/exit/bundle-runtime tier及三段Git lineage，且
  `untracked_outputs_read=false`。Ruff与strict mypy PASS；focused并行pytest=
  `56 passed`（clean-clone case待候选commit后运行，避免用未提交working-tree bytes冒充
  clean-clone证据）。状态进入`VALIDATING_CLEAN_CLONE`。
