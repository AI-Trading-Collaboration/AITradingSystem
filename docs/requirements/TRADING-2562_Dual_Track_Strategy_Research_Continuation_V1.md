# TRADING-2562：双线策略研究继续推进协调 V1

最后更新：2026-09-04

稳定任务 ID：`TRADING-2562_DUAL_TRACK_STRATEGY_RESEARCH_CONTINUATION_V1`

Owner 指令：2026-09-04，“好的，那就这样继续推进两条线吧”。

状态：`BASELINE_DONE`

## 1. 协调目标

从同一 exact local `main` 冻结两条互不污染的研究线：

- strategy-evidence lane（TRADING-2560）：为当前 composer 建立 prospective OOS append-only 观察合同；
- engineering lane（TRADING-2561）：对其他策略进行不读新 outcome 的 retained-evidence 正交筛选。

本协调任务拥有 task registry、共享文档、`docs/system_flow.md`、catalog/registry、root/shared wiring、
generated authority、formal validation 和最终 publication；两个 lane 不写这些 coordinator-only paths。

## 2. 路径与资源声明

### TRADING-2560 lane

- `config/research/first_layer_composer_v2_prospective_oos_preregistration_v1.yaml`；
- `src/ai_trading_system/first_layer_composer_v2_prospective_oos.py`；
- `tests/test_first_layer_composer_v2_prospective_oos.py`；
- 不创建真实 observation output，不运行 DQ、数据下载、provider 或 scheduler。

### TRADING-2561 lane

- `config/research/orthogonal_strategy_retained_evidence_screen_v1.yaml`；
- `src/ai_trading_system/orthogonal_strategy_retained_evidence_screen.py`；
- `tests/test_orthogonal_strategy_retained_evidence_screen.py`；
- 可生成测试临时目录内 aggregate fixture，不生成投资结论或正式 empirical artifact。

### Coordinator

- 三个 supporting requirements 与 canonical task fragments；
- `inputs/architecture/**`、`docs/task_register*.md`、`docs/system_flow.md`；
- 适用 Atlas/report-flow/catalog/compatibility generated authority；
- final candidate、formal validation、local-main、ordinary push 与 cleanup receipts。

两条 lane 的 path/module/public-contract/runtime-resource/evidence-lineage claims 必须互斥。当前波次只新增
内部 versioned contracts，不改 global DQ/PIT/cache schema、主研究窗口、现有策略阈值或公共 CLI；如实现
审查发现必须改变 consumer-visible shared contract，则停止并先完成最小 serial contract wave。

## 3. 顺序与验收

1. registration-only publication transaction 登记 TRADING-2560/2561/2562；
2. 在 exact common base 运行 `DUAL_LANE` START preflight；
3. 创建两个 task-identifiable worktree，分别完成实现与 focused parallel tests；
4. coordinator 按 policy/contract -> domain -> tests -> shared docs -> generated authority 顺序整合；
5. 更新三个 task 状态与 progress，刷新 `docs/system_flow.md`；
6. combined focused、Architecture、Contract、Integration、Reproducibility 与 Full 通过；
7. local `main` 只 fast-forward 一次，fetch 后普通 non-force push 并复核 main/origin/candidate SHA；
8. 审计并清理 lane worktrees；失败事务和唯一 evidence 按治理规则保留。

## 4. 安全边界

- authorization_state=`STANDING_OWNER_SCOPE`；
- 本波次只允许 tracked implementation/docs/tests 与离线验证；
- market data read、canonical DQ、prospective capture、backtest、bootstrap、provider、data download、
  cache mutation、QuantConnect、option backtest、paper/live、production、broker、orders、fills、positions
  均为 0；
- PR、force-push、history rewrite、remote divergence repair 不授权；最终正常 ordinary push 仍受项目默认
  publication gate 约束。

## 5. 工作区生命周期

- registration/coordinator checkout：`D:\Work\AITradingSystem_trading2559_integration`，当前复用而不新增；
- coordinator branch：`codex/trading-2562-dual-track-research-continuation`；
- lane worktrees 仅在 registration 与 START preflight PASS 后创建；
- 所有 lane worktree 的 exact path、purpose 与 exit condition 已分别记录在 TRADING-2560/2561；
- 本协调 checkout 在 local/main/origin 一致、canonical evidence 完整、无进程依赖且 governed audit 无唯一
  内容后恢复或清理，不删除未审计内容。
- 临时集成证据固定在
  `D:\Work\AITradingSystem_trading2559_integration\outputs\architecture\trading_2562_dual_track_integration`；
  owner=`TRADING-2562`，purpose=保存 exact change manifest、integration revalidation plan 与 closeout
  receipt；在 final candidate 已进入 local/remote main、canonical evidence 完整且 cleanup receipt 固化后
  删除，若 transaction 失败或仍是唯一失败证据则保留并在本记录继续说明。

## 6. 进度记录

- 2026-09-04：scope gate 与 governed worktree audit PASS；起始 local/main/origin main 均为
  `aa853b99bce3971679faa558a842b1194cd36350`，active lease 为 0。选择 `DUAL_LANE`，先登记任务，
  再开始实现。
- 2026-09-04：首次尝试用 TRADING-2562 transaction 批量登记三个 task 时，canonical writer 以
  `PUBLICATION_TASK_MISMATCH` 在任何 task event 写入前拒绝；未绕过 task identity。随后分别使用
  TRADING-2562 v1、TRADING-2560 v2 与 TRADING-2561 v1 registration-only transactions 完成三个
  canonical task registration，并按 `FAILED` 释放这些不声明最终 publication scope 的事务。
  `DUAL_LANE` START preflight 在相同 base、互斥 claims 与 coordinator-only paths 下 PASS。
- 2026-09-04：三个新 task 使 canonical task count 从 1059 增至 1062；registration focused test
  首次按预期暴露旧静态 ratchet，已只机械更新该 task-count 断言，不改变 registry 语义。
- 2026-09-04：TRADING-2560 lane 完成 result-blind prospective contract 与 producer readiness audit；
  10 个 focused tests、Ruff、Black、strict mypy PASS。audit 证明旧 producer 只能覆盖历史截止日，真实
  capture 保持 0，下一步必须另建 versioned current-session producer。
- 2026-09-04：TRADING-2561 lane 完成 retained-evidence screen；7 个 focused tests、Ruff、Black、
  strict mypy PASS。结果只选择继续 `equal_risk_qqq_sgov` 的既有 forward-aging，没有启动新 empirical
  run，也不声称经验独立性。
- 2026-09-04：两条 lane 已按 contract/domain/tests 顺序进入 coordinator commits
  `8029f9c77`、`edb2e6fcc`、`984a046a1`。本轮研究产物没有读取市场数据、运行 DQ/backtest、下载或
  修改 cache，也没有 provider、QuantConnect、Options、paper/live、production、broker、order、fill、
  position 动作；全部计数为 0。
