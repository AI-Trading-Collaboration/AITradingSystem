# TRADING-001：Paper Trading Engine MVP

最后更新：2026-05-17

关联任务：`TRADING-001`

## 背景

owner 提供 `F:/Download/trading_engine_codex_development_doc.md`，要求在现有 `AITradingSystem` repo 内新增第二套交易执行子系统。当前项目仍以趋势分析、风险评估、回测和报告输出为主，不能让趋势系统直接接触券商下单接口。

本轮目标是在同一 repo 内实现一个独立、paper-only、可审计、可测试的交易引擎 MVP，为后续 shadow vs production 对比、人工确认式交易和真实券商只读接入预留边界。

## 范围

1. 在现有主包 `src/ai_trading_system/` 下新增独立 `trading_engine` 子模块。
2. 定义交易边界 schema：
   - `OrderIntent`
   - `RiskCheckResult`
   - `BrokerOrder`
   - `ExecutionReport`
   - `PortfolioState`
   - `BrokerPosition`
3. 新增 paper-only 配置，默认 `mode=paper`、`real_trading_enabled=false`，并把投资解释相关阈值放入可审计配置。
4. 实现 `PreTradeRiskChecker`，至少覆盖：
   - kill switch
   - asset type
   - order type
   - side / short sell
   - confidence
   - order notional
   - per-symbol position limit
   - total exposure
   - cash
   - duplicate order
5. 实现 `PaperPortfolio`、`PaperBroker`、limit order 成交模拟和订单状态机。
6. 实现 `ExecutionService`，作为唯一执行入口，内部强制调用风控并写审计日志。
7. 实现 JSONL audit log：
   - order intent
   - risk check
   - order
   - fill
   - portfolio snapshot
8. 实现 Markdown 交易日报。
9. 添加 demo script，使用模拟趋势信号生成 `OrderIntent`，跑通风控、paper broker、成交、组合快照和日报。
10. 添加单元测试覆盖 schema、风控、paper broker、execution service、报告和 demo 关键路径。

## 不在本轮范围

- 不接真实券商。
- 不读取真实 API key。
- 不实现真实 `submit_order`。
- 不交易期权、融资、杠杆、盘前盘后或高频策略。
- 不把现有趋势评分、日报或回测默认结论改成真实交易指令。
- 不把 paper trading 结果写入正式 prediction ledger 或现有 production scoring。

## 设计边界

- 趋势系统只允许输出标准 `OrderIntent` 或未来的 decision candidate adapter。
- `trading_engine` 不反向依赖趋势系统内部评分、报告或回测实现。
- 订单执行链路固定为：

```text
OrderIntent -> PreTradeRiskChecker -> ExecutionService -> PaperBroker -> AuditLog / PortfolioState / TradingDailyReport
```

- `ExecutionService` 不接受外部传入的 approved flag，也不允许跳过风控。
- 非 paper mode 必须检查 `real_trading_enabled`，本阶段即使该值被误改，也没有真实 broker 实现。

## 验收标准

- `pytest tests/trading_engine` 通过。
- `tests/trading_engine/test_safety_boundaries.py` 覆盖并保持以下边界：
  - `ExecutionService` 必须强制 paper-only。
  - `config/trading_engine.yaml` 默认 `real_trading_enabled=false`。
  - IBKR / Alpaca adapter stub 调用 `submit_order` 必须抛出明确错误。
  - trading_engine 外部的趋势分析、日报和 dashboard 模块不得直接 import broker adapter。
  - 所有 trading daily report 必须包含 `production_effect=none`。
  - 测试不得读取真实券商 API key 或环境变量。
- `tests/trading_engine/test_audit_integrity.py` 覆盖 audit log 完整性：
  - approved intent 可用 `intent_id` 串起 order intent、risk result、order、
    execution report、fill 和 portfolio snapshot。
  - rejected intent 不得出现 broker order 或 fill。
  - JSONL 每行必须包含 timestamp、run_id、strategy_id、schema version 和
    intent lineage 字段。
  - `replay_intent_audit_trace` 可读取单个 `intent_id` 的完整审计轨迹。
  - 测试覆盖 approved / rejected / filled / open 四种情况。
- `python scripts/run_paper_trading_demo.py --date 2026-05-17` 可生成：
  - JSONL audit log
  - paper order / fill / portfolio snapshot
  - `reports/trading_daily/2026-05-17.md`
- demo 输出能说明：
  - 生成多少 `OrderIntent`
  - 风控通过/拒绝数量
  - 提交订单数量
  - 成交/未成交数量
  - 报告路径
- 代码中不存在真实下单路径；IBKR / Alpaca 只能是 stub。
- `config/trading_engine.yaml` 记录 owner、version/status、阈值 rationale、验证计划和复核条件。
- `docs/system_flow.md` 同步说明新增 trading engine 数据流、配置和产物。

## 开放问题

- 未来是否把现有 daily-run 接入 `OrderIntent` builder，需要等 MVP 闭环验证后单独登记。
- shadow vs production 同日双意图比较暂不进入本轮实现，只保留报告扩展点。
- 真实券商只读接入、人工确认式交易和小资金自动交易均需另行设计和 owner approval。

## 进展记录

- 2026-05-17：新增任务和需求文档，进入 paper trading MVP 实现。当前设计选择 `src/ai_trading_system/trading_engine`，保持与既有趋势预测系统模块隔离。
- 2026-05-17：MVP 实现完成并进入 VALIDATING。新增 `config/trading_engine.yaml`、独立 schema/risk/portfolio/execution/audit/report/broker stub 模块、`scripts/run_paper_trading_demo.py` 和 `tests/trading_engine`；默认 demo 生成 3 个 OrderIntent、风控通过 2 / 拒绝 1、提交 2 个 paper order、成交 1 / 未成交 1，并输出 `reports/trading_daily/2026-05-17.md` 与 `data/trading_engine/audit/*/2026-05-17.jsonl`。验证通过 `pytest -q`（595 passed）、`ruff check src tests scripts`、目标 mypy、`git diff --check` 和 demo smoke。真实 broker adapter 仍只保留 stub，不读取 API key，不实现真实下单。
- 2026-05-17：从验证回到实现。原因：owner 要求新增 trading_engine 安全边界测试，显式锁定 paper-only、stub 禁止下单、外部模块不得绕过 broker 边界、日报 `production_effect=none` 和测试不读取真实券商环境变量。
- 2026-05-17：进入验证。原因：已新增
  `tests/trading_engine/test_safety_boundaries.py`，并把 `ExecutionService`
  运行时 broker guard、IBKR/Alpaca stub 明确错误和 trading daily report
  `production_effect=none` 文本边界纳入测试；验证通过 trading_engine 测试、
  全量 pytest、ruff、目标 mypy、diff check 和 demo smoke。
- 2026-05-17：继续保持验证。原因：已新增 `replay_intent_audit_trace` 和
  `tests/trading_engine/test_audit_integrity.py`；audit JSONL 顶层写入
  `intent_id` / `related_intent_ids`，新增 `execution_report_log`，可按
  `intent_id` 回放 approved、rejected、filled、open 审计轨迹；验证通过 audit
  integrity 测试、trading_engine 测试、全量 pytest、ruff、目标 mypy、diff check
  和 demo smoke。
