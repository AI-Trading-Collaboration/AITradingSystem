# DEVX-007：Web Pro Git Strategy Planning Skill

最后更新：2026-07-29

稳定任务 ID：`DEVX-007_WEB_PRO_GIT_STRATEGY_PLANNING_SKILL`

状态：`BASELINE_DONE`

优先级：`P0`

Owner intent：

`owner_intent:DEVX-007:2026-07-29:extract_web_pro_git_planning_flow_as_skill_v1`

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
- 用户已明确授权提交时才点击发送；
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

## 9. 生命周期

- governed mode：`SINGLE_LANE`；
- branch：`codex/devx-007-web-pro-git-review-skill`；
- 不创建额外 repository worktree；
- installed skill 保留；
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
