# Shadow 参数归因、晋级 contract 与可复现性强化

状态：VALIDATING

最后更新：2026-05-16

关联任务：`CALIBRATION-015`、`CALIBRATION-016`、`CALIBRATION-017`、`CALIBRATION-014`

## 背景

`CALIBRATION-014` 已把 shadow 参数搜索从单纯权重 profile 对比推进到
weight/gate factorial attribution，并把短样本最优降级为 diagnostic-leading。
当前主要剩余风险不是搜索能否跑通，而是：

- 搜索显示 primary driver 为 `gate`，但还不知道具体是哪一个 cap 贡献了收益。
- 搜索 objective 仍可能在大量并列 trial 中偏向更激进、更难解释的参数组合。
- 搜索排序和生产晋级仍主要靠报告文字边界区分，缺少独立 promotion contract。
- 搜索 bundle 的 lineage 还不足以精确回答“这次 trial 用了哪份快照、价格缓存和
  resolver 版本”。

## 目标

- `search-shadow-parameters` 报告新增 cap-level attribution：在 production 权重下只替换
  selected trial 的单个 gate cap，拆出 `valuation`、`risk_budget`、`thesis`、
  `confidence`、`data_confidence` 等 cap 的边际贡献。
- 报告新增最终仓位变化解释表：按 signal date 展示 production/candidate 最终仓位、
  binding gate、资产收益和 return impact。
- 新增 `config/weights/shadow_parameter_promotion_contract.yaml` 和
  `aits feedback evaluate-shadow-parameter-promotion`，把 search ranking 与生产晋级检查
  分离；contract 只输出 `NOT_PROMOTABLE`、`READY_FOR_FORWARD_SHADOW` 或
  `READY_FOR_OWNER_REVIEW`，不写生产配置。
- 默认 objective 增加 gate relaxation、weight distance 和 changed dimension regularization，
  并限制离 production 过远的权重候选。
- 搜索 manifest 增加 source weight checksum、price checksum、decision snapshot aggregate
  checksum、resolver version、git commit sha 和 dirty worktree 标记。

## 阶段拆解

|阶段|状态|验收|
|---|---|---|
|1. 任务登记和需求文档|VALIDATING|新增本需求文档和 `CALIBRATION-015/016/017` 任务登记。|
|2. Cap-level attribution|VALIDATING|搜索报告、manifest 和测试包含单 cap ablation、primary gate cap 和最终仓位变化解释。|
|3. Promotion contract|VALIDATING|新增 contract 配置和 CLI；无 eligible best、样本不足、gate 主导或缺 forward shadow 时不得进入 owner review。|
|4. Objective regularization 与 lineage|VALIDATING|objective 支持 gate relaxation、weight distance、changed dimension penalty 和 production-nearby 限制；manifest 记录新增 checksum/version/commit/dirty 状态。|
|5. 验证|VALIDATING|目标测试、ruff、diff check 和当前样本 CLI smoke 通过，或记录真实阻塞。|

## 生产边界

- 本任务不修改 `weight_profile_current.yaml` 的生产权重、不修改 production gate、不写
  approved overlay、不写正式 prediction ledger、不改变日报结论。
- `evaluate-shadow-parameter-promotion` 只读取 search bundle 和 contract；输出仍为
  `production_effect=none`。
- `READY_FOR_FORWARD_SHADOW` 只表示继续观察，不表示批准上线。
- `approved_hard_allowed=false`，hard effects 在执行链路未接入前保持不可用。

## 状态记录

- 2026-05-16：新增并进入实现。原因：owner 要求继续把评估结论落地并验证流程正确性；
  下一步应优先解决 gate 主导拆解、搜索/晋级分层和可复现性。
- 2026-05-16：从 IN_PROGRESS 改为 VALIDATING。已实现 cap-level attribution、最终仓位变化
  解释、独立 promotion contract CLI、objective regularization 和 search lineage 字段；
  目标测试 `tests/test_shadow_weight_profiles.py` 通过 14 项。后续需跑 full validation 和当前样本
  CLI smoke。
- 2026-05-16：当前样本 smoke 通过。`current_20260504_20260514_cap_promotion_v3`
  评估 51,612 trials，状态 `PASS_WITH_LIMITATIONS`，无 eligible best trial；
  diagnostic-leading 为 `source_current__grid_gate_0217`，shadow return 9.02%、
  production return 4.74%、excess 4.29%，primary driver 为 `gate`。Cap-level
  attribution 显示 primary gate cap 为 `valuation`，`valuation` cap-only excess delta
  约 2.42%，`thesis` cap-only excess delta 约 0.76%，其他 cap 约 0%。同一 bundle
  的 `evaluate-shadow-parameter-promotion` 输出 `NOT_PROMOTABLE`：无 eligible best、
  available=8 低于 contract floor 30，且缺 forward shadow outcome。
- 2026-05-16：验证补充。`python -m pytest -q` 通过 551 项，`python -m ruff check
  src tests` 通过，`git diff --check` 仅提示 `docs/task_register.md` 行尾将由
  CRLF 转 LF；未发现 whitespace error。
