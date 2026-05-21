# TRADING-018E1：Approved Promotion Apply Preflight Only

关联任务：`TRADING-018E1`

状态：`VALIDATING`

最后更新：2026-05-22

## 背景

TRADING-018D 已生成 manual shadow-to-production promotion proposal，但该阶段不执行
promotion。下一阶段需要在真正 apply 之前增加一个只读 preflight，用人工 approval artifact
约束 proposal、目标 production profile、current shadow weights 和未来 rollback 计划。

018E1 只授权 preflight，不授权 production modification。

## 范围

- 新增 `scripts/run_shadow_promotion_apply_preflight.py`。
- 新增核心模块
  `src/ai_trading_system/trading_engine/shadow_promotion_apply_preflight.py`。
- 读取：
  - 018D `shadow_promotion_proposal_YYYY-MM-DD.json`
  - `data/manual_approvals/shadow_promotion_approval_YYYY-MM-DD.json`
  - 当前 production profile
  - `data/derived/weight_iterations/shadow/current_shadow_weights.json`
- 输出：
  - `data/derived/weight_iterations/promotion/preflight/shadow_promotion_apply_preflight_YYYY-MM-DD.json`
  - `data/derived/weight_iterations/promotion/preflight/shadow_promotion_apply_preflight_YYYY-MM-DD.md`
  - `data/derived/weight_iterations/promotion/preflight/logs/shadow_promotion_apply_preflight_run_YYYY-MM-DD.json`
  - `data/derived/weight_iterations/promotion/preflight/logs/shadow_promotion_apply_preflight_run_YYYY-MM-DD.md`
- Daily task dashboard 只读展示 latest preflight artifact。

## 安全边界

所有输出必须固定：

- `production_effect=none`
- `manual_review_only=true`
- `promotion_executed=false`
- `apply_executed=false`
- `preflight_only=true`
- `safe_for_production=false`

本任务禁止：

- 修改 production profile 或 production weights
- 执行 promotion / apply
- 写 approved profile
- 调用 broker、paper runner、replay runner 或 scoring pipeline
- 让 scheduler 自动 apply

## 校验规则

1. Approval artifact 必须存在、`approved=true`，且 proposal file/hash/date/decision、
   `promotion_proposed`、target profile path/name 和 safety acknowledgement 与本次参数匹配。
2. Proposal 必须是 TRADING-018D 的 `PROPOSE_FOR_MANUAL_REVIEW`，且保持 018D 安全边界。
3. `production_weights.keys() == shadow_weights.keys() == proposed_production_weights.keys()`，
   不允许静默 alias mapping。
4. proposed weights 必须在 `[0, 1]` 内，且总和在 tolerance 内等于 1.0。
5. shadow weights 必须与 proposed weights 一致或在极小 tolerance 内。
6. production profile metadata 如果声明 profile name、schema version、environment 等，必须与
   approval target 兼容。

## 决策枚举

`preflight_decision` 只能是：

- `PASS`
- `WARNING`
- `INSUFFICIENT_DATA`
- `APPROVAL_INVALID`
- `PROPOSAL_INVALID`
- `WEIGHT_MISMATCH`
- `TARGET_PROFILE_MISMATCH`
- `SAFETY_BLOCKED`
- `ERROR`

## 阶段拆解

|阶段|状态|验收标准|
|---|---|---|
|1. 任务登记与需求文档|DONE|task register 指向本文件，记录安全边界和验收标准。|
|2. 核心 preflight builder|DONE|可生成 JSON payload、diff preview、rollback plan 和 run log；所有安全 invariant fail closed。|
|3. CLI 脚本|DONE|`python scripts/run_shadow_promotion_apply_preflight.py --date YYYY-MM-DD` 可运行，默认只读读取 artifacts。|
|4. Dashboard 只读卡片|DONE|展示 latest preflight decision、安全字段、changed_weight_keys、proposal/approval/Markdown path，不重跑 pipeline。|
|5. 文档更新|DONE|更新 runbook、system flow、artifact catalog。|
|6. 测试验证|VALIDATING|目标 pytest、dashboard pytest、`tests/trading_engine`、全量 pytest 和 ruff 通过；本次改动文件 Black check 通过，全仓 Black check 被既有无关 `tests/test_market_data.py` 格式 baseline 阻断。|

## 验证命令

```bash
python -m pytest tests/trading_engine/test_shadow_promotion_apply_preflight.py -q
python -m pytest tests/test_daily_task_dashboard.py -q
python -m pytest tests/trading_engine -q
python -m pytest -q
python -m ruff check scripts src tests
python -m black --check scripts src tests
```

## 进展记录

- 2026-05-22：新增并进入 `IN_PROGRESS`。Owner 要求在 TRADING-018D proposal
  后新增 approved apply preflight only 阶段；本任务只生成 apply 前检查报告、diff preview
  和 rollback plan，不授权或执行 production 修改。
- 2026-05-22：从 `IN_PROGRESS` 改为 `VALIDATING`。已新增 preflight builder、
  CLI、JSON/Markdown/run log、approval/proposal/weight/target validation、diff preview、
  rollback plan、dashboard 只读卡片、runbook、system flow / artifact catalog 更新和测试。
  验证通过：`python -m pytest tests/trading_engine/test_shadow_promotion_apply_preflight.py -q`、
  `python -m pytest tests/test_daily_task_dashboard.py -q`、`python -m pytest tests/trading_engine -q`、
  `python -m pytest -q`、`python -m ruff check scripts src tests`。本次改动文件
  Black check 通过；全仓 `python -m black --check scripts src tests` 被既有无关
  `tests/test_market_data.py` 格式 baseline 阻断，未混入该无关格式化 diff。
