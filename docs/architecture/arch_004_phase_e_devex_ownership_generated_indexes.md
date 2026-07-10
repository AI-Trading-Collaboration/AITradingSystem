# ARCH-004E DevEx、Ownership 与 Generated Indexes 实施记录

最后更新：2026-07-11

## 结论

ARCH-004E 已完成并通过 exit validation。项目现有 777 个 source Python 文件和 1,107 个 test/support Python 文件均进入唯一、可生成、可校验的 ownership manifest；未发现 orphan 或 specific-rule overlap。该阶段只建立研发治理控制面，不改变任何投资行为、研究结论或生产边界。

## 已实现链路

```text
ownership policy
  -> module manifest (777)
  -> test manifest (1,107)
  -> changed-file impact selection
  -> architecture fitness

module / experiment / report fragment
  -> deterministic aggregate shadow index
  -> coordinator review
  -> existing aggregate remains source-of-truth
```

### Ownership 与 manifests

`config/architecture/devex_ownership_policy.yaml` 将 code、policy、data、artifact、runtime owner 分开记录，并定义 layer、category、public contract、deprecation 与 test coverage。Specific rules 必须互斥；fallback 只吸收未匹配路径。任何 orphan、specific overlap、缺 owner 或 manifest freshness mismatch 都 fail closed。

生成物：

- `inputs/architecture/arch_004e_module_manifest.yaml`：777 个 source 文件；
- `inputs/architecture/arch_004e_test_manifest.yaml`：1,107 个 test/support 文件；
- 当前 orphan/specific overlap：`0/0`。

### Impact selection 与 validation

Changed-file impact selection 用于缩短开发反馈，但不替代 full validation。Shared integration path 会要求 architecture coordinator 并升级 full gate；unknown path 同样 fail closed。新增 `architecture-fitness` tier 从 generated test manifest 的 architecture category 解析测试文件，避免维护另一份逐文件清单，同时保留既有 tier 命令和语义。

### Architecture fitness

单一 fitness 聚合：

- module/test ownership coverage 与 freshness；
- ARCH-004C dependency gate；
- direct-writer ratchet：`894 baseline / 893 current / 0 violation`；
- aggregate shadow reproducibility；
- source-of-truth 和 full-gate safety assertions。

当前结果为 `PASS`，扫描 777 个 source 文件，violation 为 0。

### Scaffold 与 aggregate fragments

`module`、`experiment`、`report` scaffold 只创建受治理的 skeleton/spec/fragment；在任一目标已存在时先整体拒绝，且不编辑 root CLI、report registry、artifact catalog 或 system flow。当前 4 个 fragment 生成覆盖 3 个 aggregate target 的 deterministic shadow index；现有 aggregates 未切换，worker 仍不得并行编辑 shared integration files。

### Engineering surface inventory

既有 engineering surface inventory 保持 canonical report family，只 additive 链接 module manifest、test manifest 和 architecture fitness 的路径、状态与数量，没有创建第二套 inventory/status/doctor。

## 验证状态

- Ruff 与 scoped mypy：PASS；
- focused：31 passed；
- generated manifests：777 modules / 1,107 tests，0 orphan，0 overlap；
- architecture fitness：PASS；
- architecture-fitness tier：78 passed；
- contract-validation：197 passed；
- full parallel：5,420 passed / 0 failed / 643 warnings，runtime artifact=`outputs/validation_runtime/full_20260710T194002Z/test_runtime_summary.json`。

## 不变边界

本阶段不切换 aggregate source-of-truth，不调整 scheduler cadence，不重算研究结果，不修改报告结论、data-quality/PIT、market regime、research window、阈值、回测、权重、promotion、paper-shadow、production 或 broker。Impact selection 也不得被用作跳过 full gate 的理由。
