# TRADING-018B：Daily Shadow Weight Iteration State and Reports

最后更新：2026-06-09

关联任务：`TRADING-018B`

## 背景

`TRADING-018` 已把 `TRADING-015/016/017` 串成每日只读权重调节 summary。
下一阶段开始维护一份独立的 shadow 权重状态，让系统可以在 production 之外做小步
学习和对比准备。

本阶段只实现：

- `observe only -> shadow learn`

本阶段不实现：

- production promotion；
- production 权重修改；
- approved profile 写入；
- 真实交易执行；
- TRADING-018C production vs shadow comparison；
- TRADING-018D manual promotion gate。

## 范围

1. 新增 `scripts/run_daily_shadow_weight_iteration.py`。
2. 新增 shadow-only artifact 目录：
   - `data/derived/weight_iterations/shadow/current_shadow_weights.json`
   - `data/derived/weight_iterations/shadow/candidates/`
   - `data/derived/weight_iterations/shadow/history/`
   - `data/derived/weight_iterations/shadow/logs/`
3. 新增 `config/daily_shadow_weight_iteration_policy.yaml` 记录 pilot baseline 阈值、
   confidence defaults 和 conservative default weights。
4. 每日读取既有 `TRADING-015/016/017/018/018A` artifacts。
5. 只在输入完整、scheduler dry-run safety PASS、summary/gate 支持更新、confidence 达标
   且有非零 delta 时更新 current shadow state。
6. 缺输入、safety blocked 或信号不足时仍生成 candidate/report/log，但不更新既有
   current shadow state。
7. Daily task dashboard 新增 `Shadow Weight Iteration` 只读卡片。
8. 更新 system flow、artifact catalog 和 runbook。

## 输入

默认从 `outputs/reports/` 读取：

- `weight_adjustment_candidates_YYYY-MM-DD.json`
- `weight_candidate_evaluation_YYYY-MM-DD.json`
- `weight_promotion_gate_YYYY-MM-DD.json`
- `daily_weight_adjustment_summary_YYYY-MM-DD.json`
- `daily_weight_adjustment_scheduler_dry_run_YYYY-MM-DD.json`

脚本也允许显式传入 TRADING-018A scheduler dry-run JSON。该 JSON 必须使用
`report_type=daily_weight_adjustment_scheduler_dry_run`；类型不匹配时视为输入不足。

## 输出 Schema 边界

所有 JSON/Markdown 输出必须固定：

```json
{
  "mode": "shadow_only",
  "production_effect": "none",
  "manual_review_only": true
}
```

Candidate `decision` 只允许：

- `UPDATE`
- `NO_UPDATE`
- `INSUFFICIENT_DATA`
- `SAFETY_BLOCKED`
- `ERROR`

`UPDATE` 才能更新 `current_shadow_weights.json` 和写入 history snapshot。

## Shadow 初始化

如果 `current_shadow_weights.json` 不存在：

1. 优先从 `config/weights/weight_profile_current.yaml` 读取 `base_weights`，复制为独立
   shadow state。
2. 如果 production profile 不可用，则使用 conservative default。
3. 初始化必须写明 `initialization_source`，且不得让 production 直接引用 shadow state。

初始化只建立 shadow 状态基线；缺输入时不得把它解释成每日 UPDATE。

## 更新规则

更新必须同时满足：

1. `config/daily_shadow_weight_iteration_policy.yaml` 存在且可读取。
2. TRADING-015/016/017/018/018A JSON artifacts 均存在且类型有效。
3. TRADING-018A scheduler dry-run safety checks 为 `PASS`。
4. TRADING-018 summary 不是 `INSUFFICIENT_DATA` / `LIMITED` / `ERROR`。
5. TRADING-017 promotion gate 已进入 `READY_FOR_MANUAL_REVIEW`。
6. adjustment confidence 不低于 policy minimum；缺显式 confidence 时只能使用
   policy 中的 `confidence_defaults`。
7. 存在非零 proposed delta。

更新限制：

- `abs(delta) <= max_abs_delta_per_day`
- `abs(delta) <= current_weight * max_relative_delta_per_day`
- 更新后总权重归一化到 1.0
- 每个权重保持在 policy min/max 内

## 安全边界

本任务禁止：

- 修改 `config/weights/weight_profile_current.yaml`；
- 写 approved profile；
- 自动 promotion shadow 到 production；
- 调用 IBKR、PaperBroker、paper runner 或 replay runner；
- 改变 daily dashboard 主投资结论；
- 把 shadow candidate 写成可交易建议。
- 在 policy 缺失或 scheduler dry-run `report_type` 不匹配时更新 current shadow state。

## 阶段拆解

|阶段|状态|验收|
|---|---|---|
|1. 登记和需求文档|DONE|task register 指向本文，本文记录范围、输入、输出和安全边界。|
|2. Policy 和核心 builder|DONE|生成 candidate/current/history/log，覆盖降级和安全 invariant。|
|3. CLI 脚本|DONE|`python scripts/run_daily_shadow_weight_iteration.py --date YYYY-MM-DD` 可运行。|
|4. Dashboard 只读卡片|DONE|展示 current weights、latest decision、delta、report link，不重跑 pipeline。|
|5. 文档更新|DONE|system flow、artifact catalog、runbook 更新。|
|6. 测试验证|DONE|目标 pytest、dashboard 测试、ruff、black 通过。|

## 验收命令

```powershell
python -m pytest tests/trading_engine/test_daily_shadow_weight_iteration.py
python -m pytest tests/trading_engine
python -m pytest tests/test_daily_task_dashboard.py
python -m pytest
python -m ruff check scripts src tests
python -m black --check scripts src tests
```

## 状态记录

- 2026-05-19：新增并进入 `IN_PROGRESS`。原因：owner 要求在 TRADING-018/018A 后
  实现 shadow learn 状态维护；当前阶段禁止 production promotion、production 权重修改、
  真实交易执行或 dashboard 主结论变更。
- 2026-05-19：从 `IN_PROGRESS` 改为 `VALIDATING`。原因：已完成 policy、builder、
  CLI、candidate/current/history/log 输出、dashboard 只读卡片、runbook、系统流图、
  产物目录和测试；本地验证通过目标 pytest、`tests/trading_engine`、dashboard 测试、
  全量 pytest、ruff 和 black check。
- 2026-05-20：提交前 hardening。原因：将 confidence 默认值和 conservative 初始化
  权重纳入 policy 输出；policy 缺失和 TRADING-018A scheduler dry-run `report_type`
  不匹配均阻断 shadow update；新增对应单元测试。
- 2026-06-09：从 VALIDATING 改为 BASELINE_DONE。原因：本轮已按 operations runbook
  复核 daily / scheduler 链路边界，`aits validate-data` 为 `PASS_WITH_WARNINGS` /
  错误数 0；默认缺输入 smoke 输出 `INSUFFICIENT_DATA`，缺失输入为
  `TRADING-015`、`TRADING-016`、`TRADING-017`、`TRADING-018`、`TRADING-018A`，
  `production_effect=none`、`manual_review_only=true`、`mode=shadow_only`、
  `current_state_updated=false`；首次 isolated smoke 只初始化独立 shadow current，
  `initialization_source=production_profile_snapshot`、权重和为 1.0，未写 history
  UPDATE。字段级复核确认 `pipeline_contract` 不写 production profile、不写 approved profile、
  不自动 promotion、不改变 dashboard 主结论、不触发交易、不运行上游 pipeline；目标 +
  dashboard pytest 33 passed，`tests/trading_engine` 939 passed，scoped safety scan 未命中
  敏感输出或 forbidden production promotion 语义，当前代码基线 GitHub Actions
  `7da9850e` public badge/checks success。真实 TRADING-015/016/017/018/018A artifact 下
  UPDATE / NO_UPDATE / SAFETY_BLOCKED 的多日观察尚无样本，本轮已拆分为 `TRADING-018B1`
  继续跟踪，避免把运行期观察伪装成已完成。
