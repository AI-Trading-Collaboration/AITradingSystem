# DEVX-008：风险分级外部动作与实证证据准入

- priority: `P0`
- status: `DONE`
- owner: Project Owner（policy direction）；Codex capability coordinator（实施与验证）
- governed mode: `SINGLE_LANE` serial contract wave
- contract change: `true`
- production effect: `none`
- broker action: `none`

## 1. Owner 决定与问题

2026-08-22，Project Owner 明确指出：

> 当前逐次 exact-token 限制过重；正确性应从实际证据出发，而不是把设想中的流程合规当作结果真假的前提。

既有 QQQ Options 流程把两个不同问题绑定在一起：

1. 某个外部动作是否在可接受的风险范围内；
2. 该动作产生的结果在技术上是否真实、完整、可复现并足以支持结论。

逐次粘贴包含大量 SHA-256 的 token 可以记录意图，但这些字段本来就可由已提交 manifest 自动重放；
让 Owner 机械复制同一组机器生成字段不提供独立技术验证。更严重的是，旧规则会把“没有事前格式化
token”直接解释成“结果不可作为正式证据”，把动作权限与证据正确性混为一谈。

本任务建立 successor policy。历史 authorization、execution 和 evidence artifacts 保持 immutable；
新 policy 只约束其生效后的动作和证据准入，不回写历史 terminal。

## 2. 核心原则

### 2.1 动作授权与证据正确性分轴

- 外部动作授权用于限制资源、状态改变、不可逆影响和第三方/账户风险；
- 证据准入由 exact code/data identity、运行身份、时间范围、输出完整性、可复现性、DQ/PIT 和独立复核决定；
- 缺少事前格式化 token 不会使观察事实自动变假，也不得成为唯一 evidence rejection reason；
- authorization state 必须保留为独立审计字段，不得冒充 technical validity；
- 发生越界动作时，输出先隔离并记录 incident；事实字节不得销毁，但在完成范围核验和 owner review 前
  不得推动 DQ/PIT、selection、engine、production 或投资结论。

### 2.2 风险分级

| 等级 | 典型动作 | 授权规则 |
|---|---|---|
| `R0_LOCAL_READ_ONLY` | 本地读取、静态分析、离线验证、已有结果复核 | 用户已提出任务即足够；不要求逐次授权。 |
| `R1_BOUNDED_RESEARCH_SANDBOX` | 已登录免费研究 sandbox/clone 内的 bounded save/build/backtest/provider query；明确 `orders=0`、`fills=0`、不改原项目、不公开分享 | Owner 已要求继续该诊断且任务/manifest 明确上限时形成 standing owner scope；不要求长 token。执行前由 Codex 自动核验 exact manifest，执行后记录实际 counters 和结果。 |
| `R2_MATERIAL_EXTERNAL_CHANGE` | 原项目写入、显著付费资源、删除云项目、公开分享、外部消息或其他可能影响第三方/恢复成本的动作 | 需要简短、明确、与目标绑定的 owner 指令；不要求机械回贴机器生成 hash 列表。 |
| `R3_PRODUCTION_OR_BROKER` | paper/live、broker、订单、成交、资金/持仓改变、production promotion 或其他高后果动作 | 必须单独、逐动作、exact-scope 授权，并保持现有 fail-closed 安全门禁。 |

Git PR、force-push、history rewrite 和 remote-divergence repair 继续受 `AGENTS.md` 的独立授权规则约束；
本 policy 不扩大这些权限。

## 3. Evidence admission contract

任何 external run 的 evidence 至少记录：

- task id、target service/project、exact repository commit、candidate byte count/SHA-256；
- requested/evaluated range、provider/engine identity、dispatch/terminal timestamp、run/build id；
- declared maxima 与 actual counters；
- output schema/completeness、stable terminal、orders/fills、production/broker effect；
- `authorization_state` 与 `technical_validation_state` 两个独立字段。

允许的 authorization state：

- `EXACT_PREAUTHORIZED`：存在事前 exact action token；
- `STANDING_OWNER_SCOPE`：Owner 已要求继续该 bounded R1 研究任务，manifest 自动核验通过；
- `RETROSPECTIVELY_REVIEWED`：动作先发生，之后完成 exact identity/scope/incident review；
- `UNAUTHORIZED_ACTION_INCIDENT`：动作超出 owner intent 或风险等级，尚未完成接纳审查。

允许的 technical validation state 至少区分 `PASS / FAIL / INDETERMINATE`。只有技术状态满足下游合同，且
不存在未解决的范围/安全 incident，证据才可进入正式结论。不得仅因 authorization state 不是
`EXACT_PREAUTHORIZED` 而判 technical evidence 无效。

## 4. TRADING-2537 当前适用边界

Project Owner 先后要求继续排查缺失日、尽快实质修复，并在本轮明确采用实证优先、风险分级规则；这些
对下列一次 R1 动作构成 `STANDING_OWNER_SCOPE`，不再要求 Owner 粘贴 V2 长 token：

- 仅使用现有 QuantConnect clone project `35444189`；
- 原 project `34808569` mutation=`0`，new clone=`0`；
- V2 candidate 为 `26587` LF bytes，SHA-256=
  `06b26262823c8c56ebceb4c90356086e07b050f9192e087b5e35a3dc43c5eac2`；
- additional project mutation/save/automatic build/zero-order backtest/provider query 最大值均为 `1`；
- orders/fills=`0/0`，不得公开分享、迁移、paper/live、broker、portfolio 或 Object Store action；
- 不自动 retry；任何 carrier ambiguity、save/build/run failure 或结果不完整按实际 counters 封存并停止；
- provider probe 继续只输出 terminal statistics，不导出 raw option rows 或 individual contract fields。

V1 package、已执行 backtest `fbad84708af7aceee7b91922809f942f` 和既有 lifetime counters 保持 immutable。
V2 run 只纠正 `OptionUniverse.Time` source-date attribution，不自动改变 DQ/PIT、selection、engine 或投资结论。

## 5. 实施范围与顺序

Serial policy wave task-owned：

- `AGENTS.md`；
- `config/governance/risk_tiered_external_action_evidence_admission_v1.yaml`；
- `tests/test_risk_tiered_external_action_evidence_admission.py`；
- 本 requirement；
- TRADING-2537/2539 supporting requirement 的 successor-policy 说明。

Coordinator-owned：canonical task source/index/generated views、`docs/system_flow.md`、适用 generated
architecture/compatibility authorities 和 formal validation artifacts。

顺序固定为：登记 DEVX-008 → governed SINGLE_LANE preflight → 实施/验证 policy → ordinary publication →
从新 exact main 继续 TRADING-2537 R1 evidence run → 封存 run evidence → 更新 2537/2539 task projection。

## 6. 验收标准

1. 项目规则明确动作授权与 technical evidence admission 分轴；
2. R1 standing owner scope 不要求逐次长 token，但必须自动核验 manifest、记录 maxima/actual counters；
3. 未事前 exact-token 的结果不被自动作废，越界结果进入隔离/复核而不是冒充正式结论；
4. R2/R3、原项目、付费、公开、删除、production、broker、order/fill 边界不放宽；
5. TRADING-2537 V2 current scope 精确记录，历史 artifacts immutable；
6. focused、Architecture、Contract、Integration、Reproducibility 与 required Full validation PASS；
7. local-main fast-forward、ordinary non-force push、SHA verify 和 branch cleanup 完成后再执行 V2 run。

## 7. 生命周期

- task branch：`codex/devx-008-risk-tiered-external-evidence`；
- 不创建额外 Git worktree、clone、download 或 credential 文件；
- existing QuantConnect clone `35444189` 不是本 policy wave 新建的临时资源，继续受 TRADING-2539
  evidence/cleanup 生命周期约束；
- known-unrelated exclusion `docs/research/growth_tilt_owner_diagnosis_pack.md` 不得读取、hash、diff、stage
  或修改；
- `production_effect=none`、`broker_action=none`。

## 8. 进度

- 2026-08-22：Project Owner 确认逐次 exact-token 限制过重，要求正确性从实际证据出发，并要求把结论
  同步到项目系统后继续此前中断工作。DEVX-008 作为最小 serial policy wave 启动。
- 2026-08-22：policy/config/AGENTS/system-flow/TRADING-2537/2539 successor binding 已实现。focused
  首轮为 `10 passed / 1 failed`，唯一失败是 canonical task count 从 1013 增至 1014；同步冻结计数并
  修正 Ruff 后为 `11 passed`。扩展 deprecation/task/policy focused=`20 passed`。
- 2026-08-22：Architecture 首轮=`864 passed / 1 failed`，仅 test manifest 与 aggregate shadow stale；
  官方 generator 刷新后第二轮=`864 passed / 1 failed`，仅新测试使 deprecation inventory 从
  `1299` 变为 `1300`；同步 immutable current inventory 后最终 Architecture=`865 passed`。
  Contract=`276 passed`、Integration=`995 passed / 643 warnings`、Reproducibility=`24 passed`。
  final committed-tree Full 与 ordinary publication 仍是外部 V2 dispatch 前最后门禁。
- 2026-08-22：commit `252bcaa76caf9cbf9dd4a923468400dd1302273c` 的首次 Full=
  `9242 passed / 28 failed / 3 skipped / 643 warnings`，parent artifact=
  `outputs/validation_runtime/devx_008_risk_tiered_external_evidence_full_final_v1/test_runtime_summary.json`。
  28 项归并为三条生成/绑定根因：AGENTS/deprecation compatibility authority stale、system-flow
  report-catalog-flow authority stale、2537/2539 task update notes 缺少 Markdown requirement link 使 canonical
  structured `requirement_refs` 被清空并导致 Atlas fail closed。policy focused 行为和其余 9242 项均通过。
  修复只允许恢复 requirement binding、按官方顺序重建 report-flow/compatibility/ARCH-004E authority，创建
  新 committed final tree，并以本次失败 artifact 为 parent 完整运行 `failure_fix_rerun`。
- 2026-08-22：2537/2539 canonical `requirement_refs` 已通过 append-only task events 恢复；system-flow
  report authority 更新为 `2243340 bytes / SHA-256 c015baa58b6095d6c670786890252b46067f42cb2a1388e816538ebaaa6d8dc0`
  与 `1042` blocks，总 entry count=`2969`、fragment count=`192`。按
  `ARCH-004E -> report-flow -> compatibility` 官方顺序重建后，首次 Full 的 28 项失败覆盖聚焦=
  `60 passed`。下一门禁是 final fix commit、ignored Atlas exact-commit sidecar 和 parent-bound Full rerun。
