# TRADING-034 Operator Brief Notification Draft Dispatch

最后更新：2026-05-25

## 背景

TRADING-030 已生成只读通知草稿，TRADING-031 已做 delivery preflight，
TRADING-032 已生成 dry-run dispatch preview，TRADING-033 已生成 approval gate。
当前缺口是：真实发送前仍缺少一个本地 draft dispatch artifact，统一回答最终
subject、body、channel、脱敏 target、approval gate 来源、hash 对齐和无副作用边界。

## 目标

- 读取 TRADING-033 approval gate artifact。
- 读取 TRADING-032 dispatch preview artifact。
- 当 approval gate 为 `APPROVED` 且 preview hash 对齐时，生成 `DRAFT_READY` 草稿。
- 当 gate 未通过或输入不可信时，生成 blocked/noop 状态 artifact。
- 输出 JSON、Markdown、latest JSON/Markdown 和 run.log。
- Daily task dashboard 新增只读卡片，只读取 TRADING-034 latest artifact。
- 为未来真实发送任务提供唯一输入 artifact。

## 非目标和安全边界

TRADING-034 不发送 email，不调用 Gmail API，不创建 Gmail draft，不调用 SMTP，
不调用 webhook，不访问网络，不读取 `.env` secrets，不读取真实邮箱，不绕过或自动
修改 approval gate，不修改 TRADING-032/TRADING-033 artifact，不写 `sent_at`，不把草稿
状态标记为 sent。

所有输出必须固定：

- `production_effect=none`
- `manual_review_only=true`
- `draft_dispatch_only=true`
- `read_only=true`
- `external_side_effects=false`
- `network_access_required=false`
- `secrets_required=false`
- sent / webhook / execution flags 均为 `false`

## 状态规则

- `DRAFT_READY`：TRADING-033 为 `APPROVED`、`allowed_to_enter_dispatch=true`，
  TRADING-032 为 `WOULD_SEND`，preview hash 与 gate 记录一致，至少一个 channel 可用，
  subject/body 可生成，且没有 safety block。
- `APPROVAL_REQUIRED`：TRADING-033 为 `APPROVAL_REQUIRED`，approval marker 缺失或未批准，
  或 dispatch preview 为 `NEEDS_APPROVAL`。
- `APPROVAL_EXPIRED`：TRADING-033 为 `APPROVAL_EXPIRED`。
- `APPROVAL_MISMATCH`：TRADING-033 为 `APPROVAL_MISMATCH`，或当前 TRADING-032 hash 与 gate
  记录不一致。
- `SAFETY_BLOCKED`：TRADING-033 或 TRADING-032 safety blocked，draft 生成发现 secret /
  private key / 未脱敏 target / repo 外输出路径。
- `BLOCKED`：输入 artifact 缺失或 schema 不合法，message/channel 缺失，或 hash 计算失败。
- `NOOP`：TRADING-033 和 TRADING-032 均为 `NOOP`。

## 输出

默认输出目录：

`data/derived/operator_briefs/notifications/draft_dispatch/`

输出文件：

- `operator_brief_notification_draft_dispatch_YYYY-MM-DD.json`
- `operator_brief_notification_draft_dispatch_YYYY-MM-DD.md`
- `latest.json`
- `latest.md`
- `run.log`

## 实施步骤

|阶段|状态|验收标准|
|---|---|---|
|1. 核心 builder|DONE|读取 approval gate 和 dispatch preview，输出完整 JSON/Markdown/latest/run.log，覆盖全部状态和安全字段。|
|2. CLI|DONE|`python scripts/run_operator_brief_notification_draft_dispatch.py` 支持 date/input/output 参数，业务阻断返回 0，fatal exception 返回 1。|
|3. Dashboard|DONE|新增只读卡片展示 final status、ready、approval gate status、channel counts、draft hash、generated_at、artifact path 和 next action。|
|4. 测试|DONE|新增专项测试并扩展 dashboard 只读测试；覆盖 hash 稳定性、安全边界、CLI 和 Markdown。|
|5. 文档|DONE|更新 system flow、artifact catalog、runbook、requirements 索引和 task register。|
|6. 验证收尾|DONE|最终 repo 外 smoke 覆盖正常 draft dispatch、缺少必要 artifact、draft hash 不匹配、safety blocked、latest 更新和 Dashboard 只读读取；目标 pytest、dashboard pytest、`tests/trading_engine`、全量 pytest、ruff 和 diff check 已通过；全仓 Black check 仍仅被既有无关 `tests/test_market_data.py` baseline 阻断。|

## 验证命令

```powershell
python -m pytest tests/trading_engine/test_operator_brief_notification_draft_dispatch.py -q
python -m pytest tests/test_daily_task_dashboard.py -q
python -m pytest tests/trading_engine -q
python -m pytest -q
python -m ruff check scripts src tests
python -m black --check scripts src tests
git diff --check
```

## 状态记录

- 2026-05-25：新增并进入 `IN_PROGRESS`。原因：真实发送前缺少唯一 draft dispatch
  artifact；本任务只生成本地可审阅草稿，不执行任何外部副作用。
- 2026-05-25：从 `IN_PROGRESS` 改为 `VALIDATING`。原因：已完成核心 builder、CLI、
  JSON/Markdown/latest/run.log、稳定 draft hash、TRADING-032 `body_markdown` schema 扩展、
  dashboard 只读卡片、runbook、system flow、artifact catalog、requirements 索引和专项测试；
  验证通过目标 pytest、dashboard pytest、`tests/trading_engine`、全量 pytest、ruff 和 diff check；
  全仓 Black check 仍仅被既有无关 `tests/test_market_data.py` baseline 阻断。
- 2026-05-25：从 `VALIDATING` 改为 `DONE`。原因：最终 repo 外 smoke 确认正常
  `DRAFT_READY`、缺少必要 artifact 的 `BLOCKED`、preview/gate hash 不匹配的
  `APPROVAL_MISMATCH`、TRADING-032 safety blocked 的 `SAFETY_BLOCKED`、`latest.json`
  更新和历史 dated artifact 未覆盖；Dashboard import guard 确认 TRADING-034 卡片只读读取
  `latest.json`，不触发 TRADING-034 script、approval gate、dispatch preview、operator brief、
  notification draft generator、email/Gmail/Slack/Discord/webhook/mobile push、market/backtest/
  scoring/data download/broker/replay/trading；收尾验证通过目标 pytest、dashboard pytest、
  `tests/trading_engine`、全量 pytest、ruff 和 diff check；全仓 Black check 仍仅被既有无关
  `tests/test_market_data.py` baseline 阻断。
