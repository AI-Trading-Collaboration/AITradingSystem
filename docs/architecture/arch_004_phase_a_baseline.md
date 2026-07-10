# ARCH-004A 当前架构基线与前序任务复核

最后更新：2026-07-11

## 结论

`ARCH-004A` 已完成，`ARCH-004B Semantic Kernel` 的 entry gate 已解锁。

2026-06-19 的 `ENGINEERING_CLOSEOUT_READY` 是有效的历史 control-plane baseline，不是当前 architecture health 证明。复用既有命令重新生成当前证据后，canonical control plane 已明确 fail closed：

- `ENGINEERING_CONTROL_PLANE_BLOCKED`；
- system doctor：`FAIL`；
- artifact lifecycle validation：`FAIL`；
- engineering Stage B validation：`FAIL`；
- 未新增 visibility waiver，也未批量刷新 optional/history artifacts。

这里的“Phase B 已解锁”不等于系统整体 health 已恢复。report index、artifact lifecycle、Stage B、Reader Brief native migration 和 canonical doctor 的真实问题继续作为后续阶段输入；Phase A 的职责是把它们完整暴露、冻结兼容基线并恢复 full-suite correctness，而不是用 waiver 抹平。

## 前序关系

ARCH-004 正式继承：

- `ARCH-001`：CLI 模块化；
- `ARCH-002`：artifact/workflow 基础契约；
- `ARCH-003`：低风险结构 seam；
- `TRADING-487_to_504`：engineering control plane、reproducibility、Reader Brief consistency、clean-clone 和 platform-freeze baseline。

详细机器记录见 `inputs/architecture/arch_004_predecessor_reconciliation.yaml`。

## 当前证据

| 检查 | 当前结果 | 解释 |
|---|---|---|
| Report index | `PASS_WITH_WARNINGS`，1,358 reports，39 missing，116 stale，155 unwaived | 不能作为当前 freeze-ready 证据 |
| Engineering surface | 3,812 surfaces，`READY_WITH_LIMITATIONS` | 2026-06-19 为 2,005，增加 1,807，约增长 90.1% |
| Surface disposition | KEEP 3,669 / MERGE 71 / ARCHIVE 62 / DEPRECATE 5 / REMOVE 5 | 大部分 surface 尚未进入真正减法流程 |
| Artifact lifecycle | `BLOCKED`，validation `FAIL` | 175 invalid latest、8,730 superseded artifacts、0 archived artifacts |
| Stage B | `ENGINEERING_STAGE_B_BLOCKED`，validation `FAIL` | 主要 blocker 是 155 个 report-index unwaived issues；另有 16 个 config metadata gaps |
| Reader Brief consistency | `PASS` | 仍有 1,634 个 native template gaps，全部由 compatibility view model 派生补齐 |
| Canonical status | `ENGINEERING_CONTROL_PLANE_BLOCKED` | data/validation health 均需复核 |
| System doctor | `FAIL`，2 failed checks | report-index issues 和 blocked canonical status |
| Full validation（修复前） | `5305 passed / 46 failed / 643 warnings` | 两类 shared CLI adapter contract drift，已登记 ARCH-004A1 |
| 原 46 个失败节点（修复后） | `46 passed / 0 failed` | `-n 16 --dist loadfile`，101.19 秒 |
| Full validation（Phase A exit） | `5358 passed / 0 failed / 643 warnings` | PASS，876.65 秒；artifact=`outputs/validation_runtime/full_20260710T162418Z/test_runtime_summary.json` |

## 关键解释

Reader Brief consistency 的 `PASS` 只证明 effective view model 能补齐六个标准区块，不证明 529 个 report family 已完成原生模板迁移。`native_template_gap_count=1634` 必须作为 ARCH-004F3 的迁移基线，不能被 PASS 隐藏。

Report index 的 155 个问题也不能通过批量 waiver 或无差别刷新解决。ARCH-004 后续要先将它们分类为：

- daily/periodic 真正 required；
- due but actionable；
- historical；
- deprecated/frozen；
- invalid/unreachable；
- 需要真实上游恢复。

只有 required/due 项才进入 owner queue，historical/deprecated 项进入 Audit Index 和 lifecycle workflow。

### 本轮捕获的结构性回归

2026-07-09 的 CLI 模块拆分把两个共享 helper 的调用契约改错：`date_range_kwargs` 丢失 `as_of` 参数并把 `as_of_date/start_date/end_date` 改成 `start/end`；`as_of_kwargs` 把 builder 要求的 `as_of_date` 改成 `as_of`。结果是 46 个 CLI tests 同时失败。

ARCH-004A1 没有逐命令打补丁，而是恢复抽取前 exact contract，并新增直接 contract tests。该事件证明后续模块迁移必须使用 compatibility snapshot、characterization/golden evidence 和 full parallel validation，不能仅凭“代码成功搬到新文件”判断重构完成。

### Semantic 与 Compatibility Freeze

- `config/architecture/research_semantic_glossary.yaml` 冻结 market regime、primary research window、requested/actual range、effective coverage、as-of、data-quality as-of 和 generated-at 的不同含义；
- `docs/architecture/arch_004_semantic_glossary.md` 明确 `2022-12-01` 是项目 AI regime start，而 scoped QQQ/SGOV/TQQQ primary research start 是 `2021-02-22`；
- `inputs/architecture/arch_004_compatibility_baseline.yaml` 冻结 3,812 个 surface、核心 source hash、CLI adapter exact contract、artifact/report evidence 和 parity rules；
- `inputs/architecture/arch_004_worktree_attribution.yaml` 证明本计划文件可与三个并发/用户研究文档隔离，提交时必须排除它们。

## Feature Freeze

`config/architecture/arch_004_refactor_policy.yaml` 已将 feature freeze 激活。允许 P0 correctness、data quality/PIT、安全事故、ARCH-004 characterization/contract/behavior-preserving migration；禁止新 task-shaped research module、新候选族、无 replacement 的 report family、继续向 god module 增加命令，以及在同一变更中混合结构重构和策略调优。

## Phase A Remaining Gates

- [x] predecessor reconciliation；
- [x] 当前 control-plane evidence；
- [x] full parallel validation baseline 与 exit PASS；
- [x] existing failure root-cause ledger、P0 linked task 与修复；
- [x] semantic glossary；
- [x] command/artifact compatibility baseline；
- [x] shared integration ownership；
- [x] attributable isolation。

ARCH-004B 可以开始 `ResearchEvaluationContext` 的 contract-first 实现；domain migration 仍需等 B/C 契约和 reference vertical slice 的各自门禁，不能因 Phase A 完成而一次性搬迁。

## Safety Boundary

- 本阶段只做 architecture governance、read-only evidence capture 和 P0 CLI compatibility correctness restore；
- 不修改策略、阈值、权重、回测、market-regime policy 或 research-window policy；
- 不激活 promotion、paper-shadow、production 或 broker；
- 不绕过 `aits validate-data`；
- `production_effect=none`。
