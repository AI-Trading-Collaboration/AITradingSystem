# TRADING-2563：Equal-Risk Catch-up V1

最后更新：2026-09-04

稳定任务 ID：`TRADING-2563_EQUAL_RISK_CATCHUP_V1`

状态：`BLOCKED_OWNER_INPUT`

## 1. Owner 精确授权

Owner 于 2026-09-04 明确批准：

> 批准 TRADING-2563 equal-risk catch-up：以 2026-09-03 为 as-of，授权 manifest replay 1 次、canonical DQ 1 次、isolated bounded rehearsal 1 次；只更新 2026-06-22 和 2026-06-24 两个既有 observation 的 maturity 1 次，并生成 scoreboard 1 次、continuity audit 1 次。新 observation、数据下载、cache mutation、provider、QuantConnect、Options、paper/live、broker、order、fill、position 和任何交易行为均为 0。

本次 `authorization_state=EXACT_PREAUTHORIZED`。授权只覆盖本地 research-only evidence
catch-up，不授权新增 observation、改变策略定义或任何外部/交易行为。

## 2. 目标与固定输入

- strategy：`equal_risk_qqq_sgov`；
- DQ / research as-of：`2026-09-03`；
- 只允许读取既有 local cache，不允许下载、覆盖或补写 cache；
- 只允许更新以下两个 append-only observation artifact 的 maturity 字段：
  - `simple_baseline_forward_aging_observation_2026-06-22`；
  - `simple_baseline_forward_aging_observation_2026-06-24`；
- 策略、target weights、signal inputs、definition hash、窗口、成本和 comparator 不得改变；
- 不允许为 2026-06-25 至 2026-09-03 的缺口补写 observation；
- scoreboard 继续使用 reviewed `simple_baseline_strategy_registry.yaml` 中的现有样本门槛，
  不新增或修改阈值。

## 3. 精确动作上限

|动作|最大次数|
|---|---:|
|manifest replay|1|
|canonical DQ|1|
|isolated bounded rehearsal|1|
|canonical maturity update|1|
|scoreboard generation|1|
|continuity audit|1|

以下动作最大次数全部为 0：new observation、data download、cache mutation、provider、
QuantConnect、Options、paper/live、broker、order、fill、position、production mutation 和任何
交易行为。

## 4. 执行顺序与 fail-closed 条件

1. 在 exact Git code identity 上冻结 versioned run manifest，并完成一次 deterministic replay；
2. 对 `as_of=2026-09-03` 执行一次 canonical DQ；只有 strict admissible PASS 才可继续；
3. 把两个既有 observation 与所需缓存只读复制到任务隔离目录，执行一次 bounded rehearsal；
4. rehearsal 必须证明只更新 maturity/outcome 字段，不改变 observation identity、target weights、
   signal inputs 或 policy definition hash；
5. 只有 rehearsal PASS 才允许对 canonical observation 目录执行一次 maturity update；
6. 随后各生成一次 scoreboard 与 continuity audit；
7. 任一步骤发生身份漂移、DQ 非可接纳 PASS、输入缺失、目标文件集合不精确或次数超限，立即停止，
   不用临时数据、历史回填或宽松校验替代。

`authorization_state` 与 `technical_validation_state` 必须分别记录。授权存在不代表 DQ 或研究结论
自动有效。

## 5. 验收标准

- versioned manifest 固定 task、Git SHA、as-of、两份 observation、动作上限和禁止项；
- replay receipt 显示 manifest replay 恰好 1 次；
- canonical DQ receipt 显示 DQ 恰好 1 次及 requested/evaluated scope；
- isolated rehearsal receipt 显示 rehearsal 恰好 1 次，且 canonical files 在 rehearsal 阶段未变化；
- canonical maturity update、scoreboard、continuity audit 各恰好 1 次；
- 两份 observation 的 immutable core identity 保持一致；
- 结果明确披露 5d/10d/20d/60d/120d 成熟数量、20d sample floor 和 continuity gap；
- 所有禁止动作计数为 0；
- focused / applicable formal validation、task consistency、governed audit 和 publication closeout 通过；
- local `main` 与 `origin/main` 按普通 non-force push 门禁收口。

## 6. 工作区生命周期

- governed mode：`SINGLE_LANE`；
- frozen base：`f2be2f0e734f5e1cab0c5b56fa1c69b9086e3097`；
- branch：`codex/trading-2563-equal-risk-catchup-v1`；
- worktree：`D:\Work\AITradingSystem_trading2563_equal_risk_catchup`；
- purpose：冻结 manifest、执行隔离 rehearsal，并在全部门禁通过后完成 bounded canonical catch-up；
- exit condition：task commit 已进入验证后的 local/origin `main`，运行 receipts 已保存并核验，
  无活跃进程依赖，tracked/untracked/ignored audit 没有未保全的唯一内容；满足时用
  `git worktree remove` 清理，否则保留并记录原因与下一责任人。

## 7. 初始状态

- 2026-09-04：Owner 给出上述精确授权；计数尚未消费。
- 既有 evidence 只有 decision date `2026-06-22` 与 `2026-06-24`；历史账本最后一次 maturity
  update 生成于 2026-06-25，状态为 `NO_MATURED_WINDOWS`。
- 离线 XNYS 日历核对显示两份 observation 的 5d/10d/20d 已到期；60d 分别在
  2026-09-16/2026-09-18 到期，120d 分别在 2026-12-10/2026-12-14 到期。日历到期不替代
  canonical DQ 和真实可见价格证据。

## 8. 执行进度与当前阻塞

- 2026-09-04：versioned manifest 已绑定 source commit `8286392a5e5e5fa1ecd5aea6fb76fbd551854105`；
  exact manifest commit 为 `e93a48cab1be0be9507019c6fdac29ccfc29a945`。
- manifest replay 恰好执行 1 次并 PASS；receipt SHA-256 为
  `8a79dfa44fbe613af95a3b5943f52d335ac7d017c51d45c1688e15be9409c822`。
- 首次 canonical DQ 调用在 validator 启动前被 `DQ_RECEIPT_FIELDS_INVALID` 拒绝：隔离 worktree
  的 `PROJECT_ROOT` 不允许包含原 checkout 的 cache path。该调用产生 1 次 dispatch attempt，
  但 completed DQ execution、DQ receipt 与 DQ report 均为 0；因此没有数据质量结论。
- 失败后复核确认 prices、rates、secondary prices 与两份 observation 的 SHA-256/size 全部未变；
  rehearsal、maturity update、scoreboard、continuity 均为 0，所有禁止动作仍为 0。
- 正确修复是在 cache 所属 checkout `D:\Work\AITradingSystem` 内运行一次 corrected DQ；该 checkout
  的 DQ code/config bytes 与冻结 manifest 一致，但这将形成第二次 dispatch，因此不沿用原授权擅自
  重试。精确 proposal 见
  `config/research/equal_risk_qqq_sgov_catchup_dq_path_failure_fix_proposal_v1.yaml`。
- 当前状态转为 `BLOCKED_OWNER_INPUT`；next owner 为 Project Owner，仅需决定是否授权 proposal
  中的一次 corrected DQ retry。即使 retry PASS，也先停下报告，不自动执行后续四项动作。
