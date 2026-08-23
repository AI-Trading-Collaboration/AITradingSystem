# DEVX-007：Web Pro Git Strategy Planning Skill

最后更新：2026-08-23

稳定任务 ID：`DEVX-007_WEB_PRO_GIT_STRATEGY_PLANNING_SKILL`

状态：`BASELINE_DONE`

优先级：`P0`

Owner intent：

`owner_intent:DEVX-007:2026-07-29:extract_web_pro_git_planning_flow_as_skill_v1`

后续 Owner decision：

`owner_decision:DEVX-007:2026-08-23:explicit_web_pro_request_authorizes_non_sensitive_review_submission_without_repeat_confirmation_v2`

production effect：`none`

broker action：`none`

## 1. 目标

把已验证的网页工作流固化为个人 Codex skill：

```text
冻结公开 Git exact commit
  -> 构造最小必要审阅上下文
  -> 在用户已登录的 ChatGPT 网页选择 Pro
  -> 明确询问模型身份、fallback 与 route evidence
  -> 要求逐项读取 exact-commit 文件
  -> 回收规划、检索结果与风险
  -> 将网页建议与仓库 authority 独立核对
```

Skill 用于大模块规划、策略研究路线审阅、架构规划与治理审阅，不负责实施网页建议。

## 2. Skill 身份与位置

- skill name：`run-web-pro-git-review`
- Git canonical：
  `tools/codex_skills/run-web-pro-git-review/`
- installed：
  `$CODEX_HOME/skills/run-web-pro-git-review/`
- canonical 与 installed bundle 必须相对文件集合和 SHA-256 一致；
- 使用 `skill-creator/scripts/init_skill.py` 初始化；
- 使用 `skill-creator/scripts/quick_validate.py` 验证。

installed bundle 是期望保留的本机能力，不是临时目录；Git canonical 可恢复它。

## 3. 输入与外发边界

允许发送到网页的内容仅限：

- public Git repository URL 与 exact commit/tree/blob URL；
- 用户明确授权公开或外发的仓库上下文；
- 最小必要的任务事实、禁止项与输出 schema。

不得发送：

- secrets、tokens、cookies、private local paths；
- 未公开、未获用户明确授权的 private repository 内容；
- known-unrelated exclusions 或用户未纳入 scope 的内容；
- moving `main` 代替 exact commit；
- 未核验为仓库事实的模型推断或策略结论。

外部提交前必须确认用户明确要求网页 Pro 审阅。Skill 不把一般规划请求自动升级为外发。

用户在当前请求中明确要求“参考网页版 GPT Pro”“发送给 Pro”或等价 external webpage
review 时，该请求已经授权提交一份唯一、最小必要、非敏感且只包含 public Git 或用户明确
授权上下文的 exact-commit packet。完成 public/authorization scope、exact commit、Pro UI、
唯一 composer 与 packet 内容复核后，应直接提交，不再追加“现在是否发送”的重复确认。

该授权复用只覆盖本次 advisory review packet，不覆盖以下情况：

- secrets、tokens、cookies、personal/sensitive data、private 或 unscoped content；
- 新增文件上传、账号/权限变更、付费资源、外部消息、production 或 broker action；
- 原请求未明确要求 Web Pro external review，或准备后的 packet 相对已授权 scope 发生实质扩张；
- terminal error 后的第二次提交。恢复时仍须先检查已保存会话，禁止重复发送。

上述情况继续按上位 browser、安全和项目授权规则确认或停止；“不重复确认”不得解释为
放宽敏感数据、R2/R3、private repository 或重复提交边界。

## 4. 模型与路由证据分层

每次报告必须分开记录：

1. **UI selection evidence**：账户/模型控件是否显示 `Pro`；
2. **response-environment self-report**：模型回答自述的标签；
3. **authoritative backend evidence**：backend model identifier、route trace、fallback event
   或平台提供的等价 metadata。

网页 UI、自述、耗时和回答质量均不能证明 exact backend route 或 no fallback。缺少第 3 类
证据时，必须输出：

```text
ROUTING_ATTESTATION_UNAVAILABLE
CANNOT_VERIFY_EXACT_BACKEND_ROUTE
```

如果自述为非 Pro 或 UI 从 Pro 漂移，只能记录 `ROUTING_MISMATCH_SIGNAL`；仍需说明自述不是
authoritative route evidence。不得把命名差异自动解释为 fallback。

## 5. Git 检索与提示词合同

提示词必须：

- 给出 repository URL、exact commit 与 exact tree；
- 列出必须读取的 exact blob URLs；
- 要求成功/失败逐项披露，禁止假装读取成功；
- 禁止读取 moving main 或用猜测补齐；
- 要求先输出 `MODEL_IDENTITY_AND_ROUTING_RISK`；
- 再输出 `REPOSITORY_RETRIEVAL`、current state、recommended sequence、first task spec、
  execution topology、falsification/stop matrix、downstream gates 与时间排序；
- 明确安全边界，例如不生成投资建议、官方仓位或越权启用 production/broker。

详细模板放在 skill 的 `references/prompt-template.md`，SKILL.md 只保留执行顺序和停止条件。

## 6. 浏览器执行与恢复

- 使用用户指定或已登录的 Chrome；按 `chrome:control-chrome` skill 操作；
- 提交前确认唯一输入框、Pro 选择和发送按钮；
- 用户已在当前请求中明确要求 Web Pro 审阅，且 packet 已通过非敏感 public/authorized
  outbound scope 复核时，直接点击发送，不再询问第二次确认；
- packet 含敏感/private/unscoped 内容或请求将产生 review submission 之外的外部动作时，
  仍按上位规则在相应边界确认或停止；
- 长时研究期间轮询生成状态，不点击“立即回答”截断，除非用户要求；
- 保存 conversation URL；
- tab binding 失效时，从已保存 URL 恢复，不重复提交；
- 完成后保留结果页面供用户核查。

认证失效时必须要求用户在该浏览器登录；不得切换来源绕过认证。

## 7. 结果回收与采用规则

回收时至少记录：

- conversation URL 与完成状态；
- UI Pro evidence；
- self-report；
- backend-route 可验证性；
- exact commit；
- 每个 required file 的 retrieval status；
- 规划的 task sequence、Owner gates、stop conditions 与 prohibited downstream actions。

网页建议必须再次与本地 `AGENTS.md`、task register、supporting requirement、可执行 preflight
和当前 exact repository state 核对。冲突时以本地 authority 为准，并在结果中标出冲突；
不得直接实施网页建议。

## 8. 验收标准

1. skill 触发描述覆盖 Web Pro、Git exact commit、大模块/策略/架构规划与路由风险验证；
2. SKILL.md 少于 500 行，详细 prompt 单层引用；
3. `agents/openai.yaml` 由标准脚本生成且显式提及 `$run-web-pro-git-review`；
4. public/explicitly-authorized outbound boundary 与 secret/private-repo stop condition明确；
5. UI、自述、backend route evidence 分层且 fallback 不可验证风险可见；
6. exact commit/blob retrieval 逐项披露；
7. interruption recovery 不重复提交；
8. 网页建议不会自动变成 repo mutation、投资结论或下游授权；
9. `quick_validate.py` PASS；
10. canonical/installed byte parity PASS；
11. task-register consistency、适用 Architecture/Contract 验证 PASS；
12. `docs/system_flow.md` 不更新：本任务不改变数据输入、DQ、scoring、backtest、report 或
    投资结论流，只增加外部 advisory planning 工具；
13. task shadow 重生成产生的 live-source drift 通过新的 append-only DEVX-007
    compatibility section 接管 current-hash authority；不得改写既有 D0E 历史段，且须由
    `tests/test_arch_004_refactor_policy.py` 验证历史前缀、精确 supersession set 与 live hash。
14. 用户已明确要求 Web Pro external review 且 packet 只含非敏感 public/authorized
    exact-commit 内容时，skill 不追加重复发送确认；敏感/private/unscoped、scope 扩张与
    second submission 继续 fail closed 或按上位规则确认。
15. v2 规则引起的既有 DEVX-007 live-source hash drift 必须通过 ARCH-005 S5 之后的新
    append-only compatibility fragment 接管；既有 legacy prefix 与三个已发布 fragment
    均不可改写，authority builder、index replay 与消费者测试必须共同验证新 section 为
    唯一 latest current-hash authority。
16. compatibility authority module/test 变更引起的 ARCH-004E module/test manifest freshness
    必须由 canonical `architecture_devex generate` 重建；相关四个 generated artifacts 的
    live hashes 纳入同一个 v2 authority，不得手改生成物或豁免 freshness gate。

## 9. 生命周期

- governed mode：`SINGLE_LANE`；
- branch：`codex/devx-007-web-pro-git-review-skill`；
- latest-main coordinator worktree：`D:\Work\AITradingSystem_devx007_latest_main_integration`；
  purpose=在精确 latest main 上重放审核后的任务差异、重建共享生成物并形成候选；exit
  condition=候选进入 local/remote main 且无 unique evidence/process dependency 后移除；
- isolated validation clone：`D:\Work\AITradingSystem_devx007_validation_clone`；purpose=冻结已提交
  候选与其 `origin/main` tracking ref，避免共享仓库的并行 fast-forward 在长时 Full 期间制造
  `CARRIER_PUSH_DRIFT`；不 fetch、不 push、不产生 production/broker effect；exit condition=最终
  Architecture/Contract/Full/Integration/Reproducibility 证据已记录且 clone 审计无 unique bytes 后移除；
- installed skill 保留；
- v2 task worktree：`D:\Work\AITradingSystem_devx007_submit_authorization`；purpose=隔离当前
  TRADING-2542B 未提交工作并更新 explicit-submission authorization 语义；exit condition=canonical
  与 installed parity、适用验证、task closeout、local-main integration 和 remote-main gate 完成后，
  审计无 unique content/process dependency 并安全移除；
- 临时 validation cache 仅在无 unique evidence 时清理；
- tracked change 由 Git commit 恢复；
- `production_effect=none`、`broker_action=none`。

## 10. 进度记录

- 2026-07-29：Owner 确认网页响应环境自述 `GPT-5.6 Pro` 符合本轮预期，并要求参考该回答
  规划后续策略任务、提取工作流为 skill。任务登记为 `IN_PROGRESS`；尚未初始化 bundle，
  尚未改变策略、数据、production 或 broker 状态。
- 2026-07-29：canonical/installed bundle、模板、静态契约测试与 task shadow 已生成；
  `quick_validate`、bundle parity、5 个 skill 测试、26 个 task/docs/shadow 测试 PASS。
  首轮 `architecture-fitness` 为 `48 failed / 736 passed`，失败集中于 D0E current-hash
  authority 未覆盖本次 task-shadow live drift。直接解决方案是新增 append-only DEVX-007
  authority section 并扩展相应架构测试；不跳过门禁、不改写 D0E 历史段。
- 2026-07-29：append-only authority 修复后，历史 authority/deprecation 聚焦集
  `144 passed`，正式 `architecture-fitness` 为 `785 passed`，正式
  `contract-validation` 为 `276 passed`。技能基线进入 `BASELINE_DONE`；后续仅需在下一次
  用户明确授权的网页审阅中观察复用体验，不影响 `TRADING-2464` 的独立 Owner gate。
- 2026-08-23：Owner 明确要求“发送给 Pro 不需要与我确认”，并要求更新 skill。该决定只取消
  已明确请求、非敏感 public/authorized exact-commit review packet 的重复确认，不改变敏感数据、
  private/unscoped content、scope 扩张、重复提交或其他外部动作的上位安全边界。当前
  Conditional Source Value Audit packet 已提交到
  `https://chatgpt.com/c/6a8ac662-51ac-83e8-afc6-336a20e46f8c`；网页回答仍仅作 advisory。
- 2026-08-23：首轮 v2 正式 `contract-validation` 为 `276 passed`；正式
  `architecture-fitness` 为 `754 passed / 111 failed`。失败不是 skill 行为测试失败，而是
  requirement、canonical `SKILL.md` 与 focused test 的新 live hashes 尚未被 ARCH-005 S5
  后续 authority 接管。按 No Silent Workarounds，采用最小 serial contract wave：追加
  DEVX-007 v2 compatibility fragment、扩展 builder/index replay 与 current-hash 消费者测试，
  不改写任何历史 authority，也不降低 Architecture gate。
- 2026-08-23：第二轮正式 `architecture-fitness` 为 `865 passed / 1 failed`；此前 111 个
  current-hash failure 已全部消除，唯一失败来自 `module_manifest_fresh` 与
  `test_manifest_fresh` 聚合状态。直接修复为运行 canonical Architecture generator，刷新
  module/test/aggregate/fitness 四个 generated artifacts，并把其精确 live hashes 纳入同一
  DEVX-007 v2 fragment 后复跑正式门禁。
- 2026-08-23：latest-main integration 的首轮 Full 为 `9427 passed / 12 failed / 6 skipped`。
  其中 9 项是临时 worktree 缺少既有只读 O1 DQ runtime artifact，2 项是 v2 authority 漏纳
  canonical task projection 的 live hash，1 项是 DEVX-006D 消费者仍假定旧 section 永远位于
  authority 末尾。最小修复为：在候选 worktree 复用 checksum-identical retained artifact；把
  `docs/task_register.md`、task registry index、DEVX-007 canonical task fragment 与 DEVX-006D
  消费者测试纳入 v2 current-hash authority；更新消费者只接受新的 append-only latest section；
  再以该 Full runtime artifact 作为 `failure_fix_rerun` 父证据，不修改策略或 DQ/PIT 语义。
- 2026-08-23：后续主线已经把 `TRADING-2543` 分配给 Atlas live canonical snapshot 任务；
  Conditional Source Value Audit 仍未创建，如 Owner 后续授权，拟议编号顺延为
  `TRADING-2544_CONDITIONAL_SOURCE_VALUE_AUDIT_SERIAL_CONTRACT_AND_FEASIBILITY_V1`，避免复用
  已占用的稳定 task id。
