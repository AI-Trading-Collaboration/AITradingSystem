# DEVX-015A 主机登记锚点改为父键单值（去除 RegRenameKey）

- 任务：`DEVX-015A_HOST_REGISTRY_SINGLE_VALUE_ANCHOR_V1`
- 父任务：`DEVX-015_TASK_CHECKPOINT_AND_PUBLICATION_SEPARATION_V2`（GOV-007 阶段 1 P1-C）
- 优先级：P0（本机可复现蓝屏；阻塞 DEVX-015 的原生注册验收）
- Owner 决定：`owner_decision:DEVX-015A:2026-09-25:single_value_anchor_v1`
  （2026-09-25 对话中确认"用父键单值方案，先登记任务"）
- agent_harness=claude_code；production_effect=none；broker_action=none

## 1. 背景：两次蓝屏

2026-09-25 本机（Windows 11 25H2，build 26200.9457，已装 KB5129195）两次蓝屏：

| 时间（JST） | dump | Bugcheck |
|---|---|---|
| 00:10 | `C:\Windows\Minidump\092526-13687-01.dmp` | 0xBE ATTEMPTED_WRITE_TO_READONLY_MEMORY |
| 01:06 | `C:\Windows\Minidump\092526-13562-01.dmp` | 0x3B SYSTEM_SERVICE_EXCEPTION（C0000005） |

两次的故障指令和调用栈相同（第一份由 Claude Code 用 cdb 独立复核，第二份的结论来自 Codex 的排查记录）：

```
nt!NtRenameKey → nt!CmRenameKey → nt!CmpReferenceSecurityNode
  → nt!CmpKeySecurityIncrementReferenceCount+0x1c   (mov dword ptr [rcx+0Ch],edx)
```

两次都没有 WHEA 硬件错误。两个调用栈精确落在同一条指令上，不像随机的内存位翻转。

与项目的关联：

- `workflow_coordination.py` 的 `publish_registry_key` 通过 ctypes 调用 `advapi32!RegRenameKey`，
  `register()` 用它把 `WorkflowControl.prepared-<摘要>` 无覆盖地发布为 `WorkflowControl`。
- 测试 `test_enrollment_native_registry_key_publication_preserves_payload_and_destination`
  在 `HKCU\Software\AITS-DEVX015-Test-<uuid>` 下真实执行这一重命名（2026-09-21 加入）。
- 崩溃后留下两棵测试键 `AITS-DEVX015-Test-926d04cd…` 和 `…-927fd4f4…`，都**只有 `prepared` 子键**。
  这说明两次崩溃都发生在第一次 `prepared → published` 重命名上。这两个键的 DACL 是继承的（非 protected），
  所以即使是普通权限的键，重命名也会触发崩溃。
- 两个残留键已导出到 `D:/Work/AITradingSystem_backups/2026-09-25/`，不属于 DEVX-015 文档登记的两个历史根
  （`1d958865…`、`e3955df6…`）。

判断：根因在 Windows 内核注册表重命名路径（或某个注册表过滤驱动），不在 Python 代码。用户态 API 即使参数有误，也只应返回错误码。
但项目无法修复内核，只要锚点发布还依赖 `RegRenameKey`，真实的管理员登记就同样可能让主机蓝屏。

## 2. 为什么用注册表（原设计意图，保持不变）

`_trusted_host_registration_bytes()` 的约束如下：

1. 锚点位于 Git 与所有 checkout、lease 目录之外；
2. 只有管理员能写，参与者（agent，普通令牌）只能读，不能伪造或重定向共享控制存储的位置；
3. 不允许任何环境变量或单个 checkout 的覆盖。

`HKLM\SOFTWARE` 默认归 Administrators 所有、受保护，路径固定，也不存在文件系统的 reparse/符号链接重定向面。

原设计用重命名，是为了保证正式对象**不存在空的中间状态**：读取端遇到"键存在但缺值"时，
按 `HOST_REGISTRATION_INCOMPLETE` fail closed，因为无法区分这是被中断的安装还是被篡改。

## 3. 决定：父键单值锚点

- 把登记内容写成父键 `HKLM\SOFTWARE\AITradingSystem` 下的**单个 REG_SZ 值**
  （值名在实现时确定，例如 `WorkflowControl.RegistrationV1`），不再使用子键 `WorkflowControl`，
  也不再调用 `RegRenameKey`。
- 单个值的写入本身是原子的：值不存在就是未登记，值存在就是完整的登记。原来"不存在空正式对象"的性质保持不变。
- 父键仍由同一个管理员入口保护创建（protected DACL），`assert_protected` 门禁不变。
- 防覆盖：写入前确认该值不存在，写入后读回并逐字节比较。已存在且内容不同时报 `HOST_REGISTRATION_CHANGED`，
  存在但无法解析时 fail closed。
- 已知差异（接受的风险）：`RegRenameKey`"目标已存在即失败"是内核级的防覆盖保证；
  "先检查不存在再写入"不是跨进程原子操作。缓解措施：只有管理员能写；登记过程由 arbiter 串行化；
  写后读回，不一致就 fail closed。验收时需要评审这一点。
- 迁移：主机目前是 `NOT_ENROLLED`，HKLM 从未写入过，所以不需要迁移旧锚点。
  如果将来发现已存在旧子键 `WorkflowControl`，按"来源不明对象"处理：fail closed，不自动删除或转换。

## 4. 硬性约束

- **本机禁止再执行任何原生 `RegRenameKey`**，包括现有测试、诊断脚本和复现尝试。
  如需复现，只能在可丢弃的隔离虚拟机里进行，并事先登记范围。
- 删除 `publish_registry_key` 及其测试后，还要保证测试集中没有其他路径会走到 `NtRenameKey`。
- 不修改真实 HKLM 或 ACL，也不执行真实的主机登记（与 DEVX-015 现有边界一致）。

## 5. 步骤与依赖

| 步骤 | 内容 | 依赖 |
|---|---|---|
| S0 | 登记本任务与本文档 | 无 |
| S1 | 查清 `AITradingSystem_devx015_integration`（分支 `codex/devx-015-main6498-reconciliation`，HEAD `7ae909f4c`）中未提交改动的归属，并确定在哪个 DEVX-015 lane 上实施 | owner / DEVX-015 协调方 |
| S2 | 实现：读写端都改为父键单值；删除 `publish_registry_key` 与 prepared 键逻辑；更新 DEVX-015 V3 需求文档中登记算法的描述 | S1 |
| S3 | 测试：用内存注册表模型覆盖"无值 → 未登记""写入后完整可见""已存在且相同 → 幂等""已存在且不同 → 拒绝""遗留子键 → fail closed"；原生测试只做 HKCU 下的单值写入与删除，不做重命名 | S2 |
| S4 | 随 DEVX-015 下一次正式发布做集成验证和 Full | DEVX-015 发布 |

## 6. 验收标准

1. 生产代码与测试中不再调用 `RegRenameKey` / `NtRenameKey`，有静态检查或测试守护，防止重新引入。
2. 读取端语义：父键值不存在返回未登记；值存在就严格校验；遗留的 `WorkflowControl` 子键 fail closed。
3. 登记端：幂等、拒绝不同内容的覆盖、写后读回校验；管理员令牌与 ACL 门禁保持不变。
4. 第 5 节 S3 中列出的测试全部通过，本机没有新的注册表残留。
5. DEVX-015 V3 需求文档与本文档同步更新，并说明 `RegRenameKey` 被移除的原因。
6. 不修改真实 HKLM 或 ACL，production_effect=none。

## 7. 待决问题

- 值名最终用什么名称，以及是否需要在 policy 或 contract 里登记这一合同变更。
- DEVX-016（单 agent 默认治理）落地后，HKLM 主机登记本身是否还是必需的，留到 DEVX-016 评估，不在本任务范围内。

## 8. 进展记录

- 2026-09-25：登记（S0）。同日的蓝屏排查与清理记录：
  `D:/Work/AITradingSystem_backups/2026-09-25/crash_followup_cleanup_record.md`。
  其中记录了一次审计失误：`D:/Work/devx015-*` 共 264 个目录曾被整体移动到暂存目录，
  复核发现其中 260 个是 DEVX-015 文档登记的证据根，已全部原样移回，没有删除任何内容。
  这些目录随 DEVX-015 的证据归档一起处理。
