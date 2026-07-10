# ARCH-004E DevEx、Ownership 与 Generated Indexes

最后更新：2026-07-11

## 任务信息

- task id：`ARCH-004E_DEVEX_OWNERSHIP_GENERATED_INDEXES`
- parent：`ARCH-004`
- priority：`P0`
- status：`DONE`
- owner：architecture coordinator / developer experience owner
- dependency：ARCH-004D complete；full parallel `5411 passed / 0 failed`
- production effect：`none`

## 目标

把 Phase A 的一次性 surface inventory 和手写 validation-tier list 扩展为持续、可生成、可验证的开发控制面：所有 source/test 文件有唯一 ownership 与分类，changed-file 可以选出 impacted tests/tier，共享 aggregate 通过 fragment/shadow index 可重现，新增 module/experiment/report 使用 scaffold 而不是复制 task-shaped wrapper。

## 现状证据

- engineering surface inventory 能统计 CLI/report/config/schema/docs/tests，但没有 module owner、layer、deprecation、public-contract 或 test impact 字段；
- `scripts/run_validation_tier.py` 的关键 suite 仍是手写 paths，新增测试可能未进入 focused tier；
- `config/report_registry.yaml`、`docs/artifact_catalog.md`、`docs/system_flow.md` 是 coordinator-owned shared aggregates，worker 仍缺 fragment contract和 reproducible shadow diff；
- `shared_integration_ownership` 只列 exclusive path，没有覆盖全部 775 个 source files 与 1,106 个 test/support files；
- ARCH-004D 已证明 experiment variant 可以只加 spec，因此可作为 experiment scaffold 的首个 reference；
- architecture dependency gate 已存在，可扩展为 ownership/test/aggregate fitness，而不是新建另一套 status/doctor。

## 设计

```text
ownership policy rules
  -> deterministic module manifest (all source .py)
  -> deterministic test manifest (all tests .py)
  -> changed-file impact selector
  -> architecture fitness report

module/report/experiment fragments
  -> deterministic shadow aggregate index/diff
  -> coordinator-reviewed compatibility aggregate
```

规则：

1. 每个 source/test 文件只有一个 effective rule；specific rule overlap、orphan、missing owner/classification 均 fail closed；
2. fallback rule 只接收未被 specific rule覆盖的文件，不与 specific ownership计为 overlap；
3. ownership 分开记录 code/policy/data/artifact/runtime owner；
4. test manifest 区分 test/support、category、suite/tier 与 covered module patterns；
5. impact selector 只缩小 focused feedback，不替代 phase full validation；
6. scaffold 只写 fragment/spec/module skeleton，不编辑 root CLI/report registry/catalog/system flow；
7. Phase E 生成 shadow aggregate，不立即改变现有 source-of-truth；正式切换留给 F3/G/H；
8. manifest/aggregate bytes、排序和 checksum 必须 deterministic。

## 分阶段实施

### E1：Ownership policy 与 manifests

- governed path rules、owner roles、layer/category、deprecation、public-contract、test selection；
- 生成 file-level module manifest 和 test manifest；
- orphan/overlap/duplicate/path drift fail closed。

### E2：Impact selection

- changed source/config/doc -> owner、impacted test files/globs、minimum validation tiers；
- shared/exclusive file触发 integration coordinator；
- unknown file fail closed 到 full/architecture review，不静默跳过。

### E3：Architecture fitness

- 复用 ARCH-004C dependency/direct-writer gate；
- 合并 ownership/test coverage、manifest freshness、aggregate reproducibility、facade sunset；
- 输出单一 PASS/FAIL 与可执行 violations。

### E4：Scaffold 与 aggregate fragments

- module/experiment/report scaffold；
- scaffold 默认创建 owner/test/fragment metadata，不修改 shared aggregate；
- module/report fragments 生成 deterministic shadow index；
- 同输入 rerun zero diff。

### E5：Control-plane integration 与 closeout

- validation tier runner 可从 test manifest解析 architecture-focused suite，同时保留旧 tiers parity；
- engineering surface inventory消费或链接 generated manifests，不重建 CLI family；
- docs/system flow、runbook/parallel constraints、compatibility snapshot更新；
- focused/scoped mypy/contract/full parallel PASS 后解锁 F1/F2/F3。

## 验收标准

- 当前 777 个 source `.py` 与 1,107 个 tests/support `.py` 全部唯一分类，无 orphan/specific overlap；后续数量变化由 freshness gate 自动捕获；
- 每个 module row 有五类 owner、layer/category、deprecation 与 public-contract分类；
- 每个 test/support row 有 category、owner、suite、covered pattern；
- changed path impact selection deterministic，shared file会升级 coordinator/full gate；
- manifest source hash/rule hash不一致时 fitness FAIL；
- dependency/direct-writer、ownership/test、aggregate reproducibility 汇总为单一 architecture fitness PASS；
- scaffold 在目标已存在时 fail closed，且不触碰 shared aggregate；
- aggregate shadow index rerun byte-identical；
- validation runner的新 architecture tier来自 generated test manifest，不再手写逐文件 list；
- 现有 validation tier command/parity保持；
- Ruff、mypy、focused、documentation/task consistency、contract-validation、full parallel PASS；
- strategy、research result、threshold、weight、report conclusion、scheduler、promotion、paper-shadow、production、broker 不改变。

## 明确不做

- 不在 E 阶段切换 report registry/catalog/system flow 的正式 source-of-truth；
- 不自动编辑 root CLI；
- 不批量迁移 domain modules；
- 不根据 owner规则自动批准并行 shared-file edits；
- 不用 impact selection替代 full gate；
- 不创建新 research hypothesis/report family/production workflow。

## 状态记录

- 2026-07-11：E5 exit gates 全部通过并归档 `DONE`：architecture-fitness=`78 passed`、contract-validation=`197 passed`、full parallel=`5,420 passed / 0 failed / 643 warnings`。F1/F2/F3 解锁；现有 aggregates 仍保持 source-of-truth，正式切换只能在后续兼容迁移阶段完成。
- 2026-07-11：E1～E4 实现完成，E5 进入 `VALIDATING`。生成 module/test manifests 覆盖 `777/1,107` 个文件，五类 owner 齐全，orphan/specific overlap 均为 0；impact selection 对 shared/unknown path fail closed；architecture fitness 汇总 dependency/direct-writer、manifest freshness 与 aggregate reproducibility 后为 `PASS`（direct writer `894 baseline / 893 current / 0 violation`）；3 个 aggregate target、4 个 fragment 的 shadow index 可重复生成，现有 aggregate source-of-truth 未切换；focused 31 tests PASS，等待 architecture/contract/full exit gates。
- 2026-07-11：ARCH-004D full gate 完成后登记并进入 `IN_PROGRESS`。确认现有 surface inventory/validation tier可复用但缺 file-level ownership/test/impact/scaffold/generated shadow aggregate；开始 E1/E2 contract-first 实现，F1/F2/F3 保持 blocked。
