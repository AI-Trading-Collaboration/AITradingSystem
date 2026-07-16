# TRADING-2443：Marketstack 配额周期恢复与每日链路解阻

最后更新：2026-07-16

## 状态

- Status: `DONE`
- Priority: `P0`
- Owner / next responsible party: system operations（后续 provider billing-cycle governance）
- Created: 2026-07-16

## 背景

2026-07-16 的统一 scheduler 入口 `aits ops daily-run` 选择最新已完成
U.S. equity trading day `2026-07-15`，在 `download_data` 步骤 fail closed。
Marketstack cache 需要补齐 2026-07-13、2026-07-14、2026-07-15 三个交易日，
共 25 个 provider-supported tickers，预估增量用量为 75。

最近缓存 quota header 为 `quota_limit=10000`、`quota_remaining=-938`，
因此本次 projected shortfall 为 1,013，ratio 为 10.13%。这同时超过
TRADING-1088 reviewed policy 的三个边界：

- `max_estimated_increment_usage=50`；
- `max_fetch_window_count=2`；
- `max_quota_overage_ratio=0.10`。

现有实现的计算与 fail-closed 行为符合 reviewed policy，不能通过修改比较符、
静默提高阈值、永久 `--without-marketstack`、减少 required ticker、复用旧二源数据
或跳过 `aits validate-data` 来“修复”。

Marketstack 官方 FAQ 说明月度 request 按 ticker 计费，超额 request 可能产生费用，
实时 usage 需要在 Account Dashboard 查看：

- <https://marketstack.com/faq>
- <https://marketstack.com/billing-overages-documentation>

官方文档未提供可替代 Account Dashboard 的只读 usage API。Project owner 随后提供的
2026-07-16 Account Dashboard 截图显示：当前 billing period 为
`2026-07-12` 至 `2026-08-11`、renewal date 为 `2026-08-12`、当前周期尚未使用 API calls，
并显示 `Overage` 已包含。由此确认本地最近 quota observation 属于上一计费周期，而不是
当前周期仍为负额度；无需升级套餐或批准新增费用。

## 采用方案

采用账期感知的 stale-header recovery：

1. `quota_cycle_reset` policy 固定当前 provider reset day 为每月 12 日（UTC），并记录
   owner dashboard evidence、风险与 review condition；
2. 只有 quota observation 早于当前 billing-cycle start，且请求是既有 cache 的单一
   tail range window，estimated usage <= 50、window count <= 1、自然日跨度 <= 7，才标记
   `CURRENT_CYCLE_QUOTA_BOOTSTRAP_ALLOWED`；
3. 保留 2026-07-13 至 2026-07-15 的原始 range request，使 25 个 tickers 只估算 25 units，
   不按三个交易日拆成 75 units；
4. full-history、新 ticker/backfill、mixed stale windows、当前周期负 header 或超边界请求
   继续 fail closed；首个 live response 刷新本地 quota evidence；
5. 原有 daily/catch-up projected shortfall 10% policy 完全不变。

真实复验首次重触发后又发现独立的 canonical run-control blocker：scheduled task adapter
把所有 step 的 `max_attempts` 硬编码为 1，因此 `runtime_control.yaml` 已 reviewed 的
`max_run_attempts=2` 无法用于任何已失败步骤。采用的直接修复是把 per-step
`max_attempts` 纳入 `config/scheduled_tasks.yaml`，只对幂等 `download_data` 配置为 2，
其他步骤继续默认为 1。该 scheduler policy 变更会生成新的 workflow spec / idempotency key，
旧失败 state / ledger 原样保留；同一新 spec 的第二次失败后仍 hard stop，不允许手工删除状态。

## 验收标准

1. Owner evidence 记录 billing cycle reset date、overage 状态、当前 usage 和 renewal date；
   policy 记录请求上限、适用边界和退出/复核条件。
2. 若更新 policy，阈值必须由该决策直接推导，保持 full-history、新 ticker、mixed stale
   windows、缺 quota limit 和超批准金额 fail closed。
3. `aits download-data` 的 manifest / failure diagnostics 继续披露 provider、endpoint、
   request params、download timestamp、row count、checksum、quota header、estimated usage、
   projected shortfall/ratio、policy id/version 和 violation reasons。
4. `aits validate-data --as-of 2026-07-15` 必须通过后才能进入 daily PIT、score、
   dashboard 和 Reader Brief；不得把旧 Marketstack rows 解释为 current cross-source evidence。
5. 使用 `python scripts/run_validation_tier.py fast-unit --write-runtime-artifact` 或 focused
   `pytest -n 16 --dist loadfile` 验证 allowed/blocked/cycle-expiry/diagnostic 路径。
6. 最终只通过 `aits ops daily-run` 真实复验；36 个步骤全部完成，且报告明确
   `production_effect=none`，没有 production weights、active shadow weights、broker/order
   或 trading action。

## 当前影响与风险

- `2026-07-15` data quality、daily PIT、score、dashboard 和 Reader Brief 均被阻断；
  不得使用旧 artifact 补造当日结论。
- 当前四个核心 raw cache / manifest 的 pre/post checksum 相同，没有部分 cache mutation。
- 实现只使用当前已重置周期的正常 quota，不升级套餐、不扩大 overage policy；若 provider
  改变 reset day 或 dashboard evidence 失效，必须更新 reviewed policy 后才能继续。

## 进展记录

- 2026-07-16：初始 daily-run fail closed；完成 policy、测试、失败 artifact 和官方
  quota/overage 文档审计，确认 reviewed 10% cap 正常执行，raw cache 未发生部分写入。
  当时 Account Dashboard 无登录态，任务登记为 `BLOCKED_OWNER_INPUT`；未实施 policy
  override、provider disable、source switch、cache fabrication 或交易/权重动作。
- 2026-07-16：owner 提供 Account Dashboard 截图，确认 7 月 12 日进入新 billing cycle、
  当前 usage 为 0；blocker 解除，任务转为 `IN_PROGRESS`。
- 2026-07-16：实现账期感知 stale-header recovery、审计字段及 allowed/blocked tests；正在执行
  focused parallel pytest、文档一致性检查和唯一 scheduler 入口真实复验。
- 2026-07-16：真实入口返回 `RUN_CONTROL_BLOCKED_RETRY_EXHAUSTED`，确认 step attempt
  硬编码为 1 使 reviewed run budget 无效；不删除旧 state/lock，扩展本任务以实现
  config-governed `download_data max_attempts=2` 和“一次重试后再次失败即阻断”覆盖。
- 2026-07-16：实现和收口验证完成。Focused parallel suite=`101 passed`，Ruff、compileall、
  docs/task consistency 和 diff check 通过；真实 `aits ops daily-run` run id
  `daily_ops_run:2026-07-15:20260716T022111Z` 为 36/36 PASS。Marketstack 单一 range
  `2026-07-13..2026-07-15` 发起 1 次 live request、estimated/increment usage=25，响应
  `quota_limit=10000`、`quota_remaining=10000`；data quality=`PASS_WITH_WARNINGS`，PIT=`PASS`，
  Reader Brief validation 13/13 PASS。Run manifest `production_effect=none`，未写 production
  weights/active shadow weights，未触发 broker/order/trading action；任务归档 `DONE`。
