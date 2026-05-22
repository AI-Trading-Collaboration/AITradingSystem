# TRADING-018E3：Explicit Approved Shadow Promotion Rollback Command

关联任务：`TRADING-018E3`

状态：`VALIDATING`

最后更新：2026-05-23

## 背景

TRADING-018E2 已实现 explicit approved apply command，并在 apply 前创建
`production_profile_before_shadow_promotion_YYYY-MM-DD.json` rollback snapshot。
018E3 补齐单独的人工显式 rollback command。核心原则是：

- apply result 不是 rollback approval
- rollback 必须由人工显式命令触发
- rollback 不允许 scheduler、broker、replay 或 trading execution 触发

## 范围

- 新增 `scripts/run_shadow_promotion_rollback.py`。
- 新增核心模块
  `src/ai_trading_system/trading_engine/shadow_promotion_rollback.py`。
- 读取：
  - TRADING-018E2 apply result artifact
  - apply 时创建的 rollback snapshot
  - 当前 target production profile
  - 单独的 `data/manual_approvals/shadow_promotion_rollback_approval_YYYY-MM-DD.json`
- 只有 apply result、rollback approval、rollback snapshot、current target hash、
  danger flag、current snapshot 和 post-rollback validation 全部通过时，才原子恢复
  target production profile 的 weights。
- 输出：
  - `data/derived/weight_iterations/promotion/rollback_results/shadow_promotion_rollback_result_YYYY-MM-DD.json`
  - `data/derived/weight_iterations/promotion/rollback_results/shadow_promotion_rollback_result_YYYY-MM-DD.md`
  - `data/derived/weight_iterations/promotion/rollback_results/logs/shadow_promotion_rollback_run_YYYY-MM-DD.json`
  - `data/derived/weight_iterations/promotion/rollback_results/logs/shadow_promotion_rollback_run_YYYY-MM-DD.md`
  - `data/derived/weight_iterations/promotion/rollback_current_snapshots/production_profile_before_rollback_YYYY-MM-DD.json`
  - `data/derived/weight_iterations/promotion/rollback_current_snapshots/production_profile_before_rollback_YYYY-MM-DD.sha256`
- Daily task dashboard 只读展示 latest rollback result artifact。

## 安全边界

成功 rollback 输出必须包含：

- `production_effect=profile_rolled_back_only_if_rollback_executed`
- `manual_review_only=true`
- `rollback_executed=true`
- `safe_for_scheduler=false`
- `broker_execution=false`
- `replay_execution=false`
- `trading_execution=false`

未执行 rollback 输出必须包含：

- `production_effect=none`
- `manual_review_only=true`
- `rollback_executed=false`
- `safe_for_scheduler=false`
- `broker_execution=false`
- `replay_execution=false`
- `trading_execution=false`

本任务禁止：

- scheduler 自动运行 rollback
- 无 rollback approval、无 apply result、无 rollback snapshot 或无 danger flag 时 rollback
- apply result 不是 `APPLIED` 或 `apply_executed=true` 不成立时 rollback
- rollback snapshot sha256 与 apply result / approval 不匹配时 rollback
- 当前 target production profile 已偏离 apply 后 expected hash 时 rollback
- 修改除目标 production profile weights、rollback result artifacts 和 rollback-current snapshot
  之外的文件
- broker、replay runner 或 trading execution
- dashboard 触发 018B/018C/018C2/018D/018E1/018E2/018E3、scoring、broker、
  replay 或 trading execution

## 决策枚举

`rollback_decision` 只能是：

- `ROLLED_BACK`
- `INSUFFICIENT_DATA`
- `APPROVAL_INVALID`
- `APPLY_RESULT_INVALID`
- `DANGER_FLAG_MISSING`
- `ROLLBACK_SNAPSHOT_INVALID`
- `TARGET_PROFILE_CHANGED`
- `TARGET_PROFILE_MISMATCH`
- `CURRENT_SNAPSHOT_FAILED`
- `WRITE_FAILED`
- `POST_ROLLBACK_VALIDATION_FAILED`
- `SAFETY_BLOCKED`
- `ERROR`

## 校验规则

1. Danger flag `--i-understand-this-rolls-back-production` 必须显式提供。
2. Apply result 必须存在，`task_id=TRADING-018E2`、`apply_decision=APPLIED`、
   `apply_executed=true`、`promotion_executed=true`、rollback snapshot 已创建、
   `post_apply_validation.status=PASS`，且 broker/replay/trading 均为 false。
3. Rollback approval 必须是 `approval_type=shadow_promotion_rollback`、
   `approved=true`，并匹配 apply result sha256、rollback snapshot sha256、
   target path、expected current profile sha256 和 expected rollback profile sha256。
4. Rollback approval 必须明确授权 rollback、production modification、weights-only
   restore、current snapshot required、manual command required，并禁止
   scheduler/broker/replay/trading execution。
5. Rollback snapshot 必须可解析，weights keys 合法，总和约等于 1.0，每个 weight 在
   `[0, 1]`，sha256 必须匹配 apply result 记录。
6. Rollback 前重新读取 target profile 并计算 sha256，必须等于 apply result 的
   apply 后 hash、approval 的 expected current hash，以及可选 CLI expected hash。
7. Rollback 前必须保存 current production snapshot；创建失败不得写 target profile。
8. 本阶段采用 weights-only restore：只恢复 weights，非 weights 字段保持 rollback 前当前值。
9. Target profile 必须用临时文件加 atomic replace 写入。
10. 写入后必须重新读取并校验 weights、keys、sum、非 allowed fields 和禁改字段。

## 阶段拆解

|阶段|状态|验收标准|
|---|---|---|
|1. 任务登记与需求文档|DONE|task register 指向本文件，记录安全边界、决策枚举和验收标准。|
|2. 核心 rollback builder|DONE|可生成 blocked / rolled-back payload，安全 invariant fail closed。|
|3. CLI 脚本|DONE|`python scripts/run_shadow_promotion_rollback.py --date YYYY-MM-DD ... --i-understand-this-rolls-back-production` 可运行。|
|4. Dashboard 只读卡片|DONE|展示 latest rollback result，不导入或触发任何 pipeline。|
|5. 文档更新|DONE|更新 runbook、system flow、artifact catalog。|
|6. 测试验证|DONE|目标 pytest、dashboard pytest、`tests/trading_engine`、全量 pytest、ruff、本次改动文件 black check 和成功/失败 smoke 完成；全仓 black check 仍被既有无关 `tests/test_market_data.py` baseline 阻断。|

## 验证命令

```bash
python -m pytest tests/trading_engine/test_shadow_promotion_rollback.py -q
python -m pytest tests/test_daily_task_dashboard.py -q
python -m pytest tests/trading_engine -q
python -m pytest -q
python -m ruff check scripts src tests
python -m black --check scripts src tests
```

## 进展记录

- 2026-05-23：新增并进入 `IN_PROGRESS`。Owner 要求在 TRADING-018E2 explicit
  approved apply 后实现单独的 explicit approved rollback command；本任务只允许在单独
  rollback approval、danger flag、current profile hash 和 rollback snapshot 均匹配时恢复
  target production profile weights，不触发 scheduler、broker、replay 或 trading execution。
- 2026-05-23：从 `IN_PROGRESS` 改为 `VALIDATING`。已新增 rollback builder、CLI、
  JSON/Markdown/run log、rollback-current snapshot、rollback approval/apply result/danger
  flag/target profile/post-rollback validation、dashboard 只读卡片、runbook、system flow /
  artifact catalog 更新和测试。repo 外临时 fixture smoke 通过：带 danger flag 输出
  `rollback_decision=ROLLED_BACK`，production weights 恢复为 rollback snapshot weights，
  current snapshot 已生成且 sha256 等于 rollback 前 target profile；不带 danger flag 输出
  `DANGER_FLAG_MISSING`、`rollback_executed=false`、`production_effect=none` 且 target
  profile sha256 不变。验证通过：
  `python -m pytest tests/trading_engine/test_shadow_promotion_rollback.py -q`、
  `python -m pytest tests/test_daily_task_dashboard.py -q`、
  `python -m pytest tests/trading_engine -q`、`python -m pytest -q`、
  `python -m ruff check scripts src tests`。本次改动文件 black check 通过；全仓
  `python -m black --check scripts src tests` 仍被既有无关 `tests/test_market_data.py`
  格式 baseline 阻断，未混入无关格式化 diff。
