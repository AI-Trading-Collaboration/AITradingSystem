# TRADING-828 to TRADING-842 Tail-Risk Fallback Independent Validation Governance

最后更新：2026-06-22

## 背景

TRADING-827 的 trigger/label independence audit 已输出 `BLOCKED`：当前 tail-risk
fallback trigger 与 label/case/tail-risk 定义存在直接字段重叠和派生耦合。因此本任务包只
能重建独立验证、隔离受污染 metric、增加泄漏/时间边界压力测试，并把所有 promotion、
paper-shadow、production weight 和 broker/order 行为继续硬阻断。

## 全局安全边界

- `production_effect=none`
- `broker_action=none`
- `promotion_gate_allowed=false`
- `paper_shadow_change_allowed=false`
- `production_weight_change_allowed=false`
- `production_allowed=false`
- 不新增 paper-shadow，不修改 production weights，不触发真实交易或 broker/order action。
- TRADING-827 仍为 BLOCKED 时，任何 precision/recall/return metrics 都不能用于
  promotion。

## 阶段拆解

| 任务 | 优先级 | 状态 | 目标 | 依赖 | 验收摘要 |
|---|---|---|---|---|---|
| TRADING-828 | P0 | VALIDATING | 建立 independent forward outcome validation | TRADING-827、现有 value-surface / fallback artifacts | 新增 CLI、JSON/MD artifact、只使用 `decision_time` 后真实 forward outcome 字段，禁止 label/case 字段进入 outcome |
| TRADING-829 | P0 | VALIDATING | 建立 forward outcome contract and lineage audit | TRADING-828 schema、TRADING-827 字段矩阵 | 输出 field/dependency/time-window/forbidden dependency matrices，发现重叠或 future leakage 时 BLOCKED |
| TRADING-830 | P0 | VALIDATING | 审计 decision-time boundary | TRADING-827、829 | 检查 trigger/feature/policy input 是否只用 decision_time 当时或之前可见数据 |
| TRADING-831 | P0 | VALIDATING | quarantine tainted metrics | TRADING-827、旧 label-based validation artifacts | 标记 precision/recall/f1/return/hit-rate 等旧 metrics 不可用于 promotion/paper-shadow/production |
| TRADING-832 | P0 | VALIDATING | fallback counterfactual baseline validation | TRADING-828 independent outcomes | 在 independent outcome 上比较 fallback policy 与 no fallback/static/best baseline |
| TRADING-833 | P1 | VALIDATING | regime-stratified independent outcome review | TRADING-828、832 | 分层输出 sample、return/drawdown improvement、FP/FN cost 与 concentration score |
| TRADING-834 | P1 | VALIDATING | threshold sensitivity robustness review | TRADING-828、832 | 覆盖 trigger/cutoff/lookback/intensity perturbations，输出 stability/fragility |
| TRADING-835 | P1 | VALIDATING | false-positive / false-negative cost ledger | TRADING-828 | 单独量化误触发与漏触发成本，输出 worst/best cases 和 asymmetry |
| TRADING-836 | P1 | VALIDATING | sample coverage and evidence maturity gate | TRADING-828、833 | 按总样本、triggered/non-triggered、5/10/20d、regime/tail/recent 样本给出 maturity 状态 |
| TRADING-837 | P1 | VALIDATING | forward aging observation tracker | TRADING-819、828 | 把 forward validation 做成 aging tracker，区分 new/matured/pending/rolling performance |
| TRADING-838 | P1.5 | VALIDATING | leakage stress test suite | TRADING-827、829、830 | 覆盖 signal lag、label permutation、timestamp、availability、forward overlap、trigger-outcome overlap 等测试 |
| TRADING-839 | P1.5 | VALIDATING | promotion gate hard block integration | TRADING-827、828、829、830、838 | 任何 blocking 状态都输出 promotion/paper-shadow/production/broker hard block |
| TRADING-840 | P2 | VALIDATING | independent trigger v2 candidate builder | TRADING-829、830、841 | 只用 decision_time 前可见 price/vol/trend/drawdown/breadth/proxy 输入，不用旧 label/future outcome |
| TRADING-841 | P2 | VALIDATING | trigger feature availability catalog | TRADING-840 input contract | 输出 source、availability、missing periods、PIT quality 和 trigger/outcome usage permissions |
| TRADING-842 | P2 | VALIDATING | tail-risk research master review | TRADING-827～841 | 聚合所有报告，给出 continue/rebuild/quarantine/pause/deprecate 建议和 owner next action |

## 实施顺序

1. P0：TRADING-828、829、830、831、832。
2. P1/P1.5：TRADING-833、834、835、836、837、838、839。
3. P2：TRADING-840、841、842。

## 产物契约

所有任务都必须生成同名 JSON/Markdown artifact，默认位于
`outputs/research_strategies/value_surface_review/`，并登记到
`config/report_registry.yaml`，`required_for_daily_reading=false`。

## 验证要求

- focused pytest 覆盖 builder 输出和安全字段。
- CLI / registry focused pytest 覆盖命令与 report registry 条目。
- `fast-unit`
- `contract-validation`
- `report-validation`
- `ruff check`
- changed-file Black check
- `compileall`
- `git diff --check`

## 状态记录

- 2026-06-22：新增任务包并登记。TRADING-827 已证明旧 trigger/label 口径 BLOCKED；
  本任务包进入 implementation，所有输出继续 controlled-only / read-only，不改变策略权限。

