# TRADING-018E2：Explicit Approved Shadow Promotion Apply Command

关联任务：`TRADING-018E2`

状态：`DONE`

最后更新：2026-05-23

## 背景

TRADING-018D 只生成 shadow-to-production promotion proposal。TRADING-018E1
只生成 approved apply preflight、diff preview 和 rollback plan，不修改 production。

018E2 是第一次允许在严格人工授权下写入 production profile 的阶段。核心原则是：

- proposal 不是 approval
- preflight 不是 apply
- apply 必须人工显式执行

## 范围

- 新增 `scripts/run_shadow_promotion_apply.py`。
- 新增核心模块 `src/ai_trading_system/trading_engine/shadow_promotion_apply.py`。
- 读取：
  - 018E1 `shadow_promotion_apply_preflight_YYYY-MM-DD.json`
  - 单独的 `data/manual_approvals/shadow_promotion_apply_approval_YYYY-MM-DD.json`
  - 目标 production profile
  - 可选 018D `shadow_promotion_proposal_YYYY-MM-DD.json`
- 只有全部安全条件满足时，才把
  `preflight.diff_preview.production_weights_after_preview` 写入目标 profile 的 weights。
- 输出：
  - `data/derived/weight_iterations/promotion/apply/shadow_promotion_apply_result_YYYY-MM-DD.json`
  - `data/derived/weight_iterations/promotion/apply/shadow_promotion_apply_result_YYYY-MM-DD.md`
  - `data/derived/weight_iterations/promotion/apply/logs/shadow_promotion_apply_run_YYYY-MM-DD.json`
  - `data/derived/weight_iterations/promotion/apply/logs/shadow_promotion_apply_run_YYYY-MM-DD.md`
  - `data/derived/weight_iterations/promotion/rollback/production_profile_before_shadow_promotion_YYYY-MM-DD.json`
  - `data/derived/weight_iterations/promotion/rollback/production_profile_before_shadow_promotion_YYYY-MM-DD.sha256`
- Daily task dashboard 只读展示 latest apply result artifact。

## 安全边界

成功 apply 输出必须包含：

- `production_effect=profile_updated_only_if_apply_executed`
- `manual_review_only=true`
- `promotion_executed=true`
- `apply_executed=true`
- `safe_for_scheduler=false`
- `broker_execution=false`
- `replay_execution=false`
- `trading_execution=false`

未执行 apply 输出必须包含：

- `production_effect=none`
- `manual_review_only=true`
- `promotion_executed=false`
- `apply_executed=false`
- `safe_for_scheduler=false`
- `broker_execution=false`
- `replay_execution=false`
- `trading_execution=false`

本任务禁止：

- scheduler 自动运行 apply
- 无 apply approval、无 preflight 或无 danger flag 时 apply
- preflight 非 `PASS` 时 apply
- target profile hash 与 preflight / approval 不一致时 apply
- apply approval 与 preflight / proposal / target profile 不匹配时 apply
- broker、paper runner、replay runner 或 trading execution
- 修改除目标 production profile weights、apply artifacts 和 rollback snapshot 之外的文件
- dashboard 触发 018B/018C/018C2/018D/018E1/018E2、scoring、broker、replay 或交易

## 决策枚举

`apply_decision` 只能是：

- `APPLIED`
- `INSUFFICIENT_DATA`
- `APPROVAL_INVALID`
- `PREFLIGHT_INVALID`
- `DANGER_FLAG_MISSING`
- `TARGET_PROFILE_CHANGED`
- `TARGET_PROFILE_MISMATCH`
- `ROLLBACK_SNAPSHOT_FAILED`
- `WRITE_FAILED`
- `POST_APPLY_VALIDATION_FAILED`
- `SAFETY_BLOCKED`
- `ERROR`

## 校验规则

1. Danger flag `--i-understand-this-writes-production` 必须显式提供。
2. Preflight artifact 必须存在，`task_id=TRADING-018E1`、
   `preflight_decision=PASS`、`preflight_only=true`、`apply_executed=false`、
   `promotion_executed=false`、`production_effect=none`，且包含 diff preview、
   expected weights、target profile path 和 rollback plan。
3. Apply approval 必须是 `approval_type=shadow_promotion_apply`、`approved=true`，
   preflight sha256、proposal sha256、target path 和 expected target profile sha256
   必须匹配，并明确授权 production modification、weights-only update、rollback required、
   manual command required，同时禁止 scheduler、broker、replay 和 trading execution。
4. 写入前重新读取 target profile 并计算 sha256，必须等于 preflight 和 approval 中记录的
   expected hash。
5. 只允许修改 production profile 的 weights 字段；broker、execution、replay、scheduler、
   credentials、account、risk_limits 等字段不得变化。
6. Expected weights 来自 preflight diff preview，keys 必须与 target profile 当前 weights
   一致，总和必须约等于 1.0，每个 weight 必须在 `[0, 1]`。
7. Rollback snapshot 必须在 production profile 写入前创建；创建失败不得 apply。
8. Production profile 必须用临时文件加 atomic replace 写入。
9. 写入后必须重新读取并校验 weights、keys、sum、非 allowed fields 和 hash 变化。

## 阶段拆解

|阶段|状态|验收标准|
|---|---|---|
|1. 任务登记与需求文档|DONE|task register 指向本文件，记录安全边界、决策枚举和验收标准。|
|2. 核心 apply builder|DONE|可生成 blocked / applied payload，安全 invariant fail closed。|
|3. CLI 脚本|DONE|`python scripts/run_shadow_promotion_apply.py --date YYYY-MM-DD ... --i-understand-this-writes-production` 可运行。|
|4. Dashboard 只读卡片|DONE|展示 latest apply result，不导入或触发任何 pipeline。|
|5. 文档更新|DONE|更新 runbook、system flow、artifact catalog。|
|6. 测试验证|DONE|目标 pytest、dashboard pytest、`tests/trading_engine`、全量 pytest、ruff、本次改动文件 black check 和成功/失败 smoke 完成；全仓 black check 仍被既有无关 `tests/test_market_data.py` baseline 阻断。|

## 验证命令

```bash
python -m pytest tests/trading_engine/test_shadow_promotion_apply.py -q
python -m pytest tests/test_daily_task_dashboard.py -q
python -m pytest tests/trading_engine -q
python -m pytest -q
python -m ruff check scripts src tests
python -m black --check scripts src tests
```

## 进展记录

- 2026-05-23：新增并进入 `IN_PROGRESS`。Owner 要求在 TRADING-018E1
  preflight-only 后实现 explicit approved apply command。本任务允许在所有人工授权和
  hash 校验通过后只更新 target production profile weights；不实现 rollback command，
  rollback 留给 TRADING-018E3。
- 2026-05-23：从 `IN_PROGRESS` 改为 `VALIDATING`。已新增 apply builder、CLI、
  JSON/Markdown/run log、rollback snapshot、approval/preflight/danger flag/target profile/post-apply
  validation、dashboard 只读卡片、runbook、system flow / artifact catalog 更新和测试。
  repo 外临时 fixture smoke 通过：带 danger flag 输出 `apply_decision=APPLIED`，target
  profile sha256 变化且 weights 等于 expected weights，rollback snapshot 和 `.sha256` 已生成；
  不带 danger flag 输出 `DANGER_FLAG_MISSING`，target profile sha256 不变。验证通过：
  `python -m pytest tests/trading_engine/test_shadow_promotion_apply.py -q`、
  `python -m pytest tests/test_daily_task_dashboard.py -q`、
  `python -m pytest tests/trading_engine -q`、`python -m pytest -q`、
  `python -m ruff check scripts src tests`。本次改动文件 black check 通过；全仓
  `python -m black --check scripts src tests` 仍被既有无关 `tests/test_market_data.py`
  格式 baseline 阻断，未混入无关格式化 diff。
- 2026-05-23：从 `VALIDATING` 改为 `DONE`。最终收尾验证再次使用 repo 外临时
  fixture 跑成功和失败 smoke。成功路径带 apply approval、preflight PASS 和 danger flag，
  输出 `apply_decision=APPLIED`，production profile sha256 发生变化，production weights
  等于 expected weights，rollback snapshot 已生成，rollback snapshot sha256 等于 apply
  前 production profile sha256，`post_apply_validation.only_allowed_fields_changed=true`，
  `broker_execution=false`、`replay_execution=false`、`trading_execution=false`。失败路径不带
  danger flag，输出 `apply_decision=DANGER_FLAG_MISSING`、`apply_executed=false`、
  `promotion_executed=false`、`production_effect=none`，production profile sha256 不变。
  Dashboard import guard 验证 Shadow Promotion Apply Result 卡片只读读取 apply result
  artifact，不触发 018B/018C/018C2/018D/018E1/018E2/scoring/broker/replay/trading。
  收尾命令再次验证通过：`python -m pytest tests/trading_engine/test_shadow_promotion_apply.py -q`、
  `python -m pytest tests/test_daily_task_dashboard.py -q`、
  `python -m pytest tests/trading_engine -q`、`python -m pytest -q`、
  `python -m ruff check scripts src tests`。全仓
  `python -m black --check scripts src tests` 仍只被既有无关
  `tests/test_market_data.py` 格式 baseline 阻断，未混入无关格式化 diff。
