# TRADING-219～223 Paper Shadow Selection Drilldown and Research Method Hardening

最后更新：2026-06-12

## 背景

TRADING-214～218 已将 paper shadow 从短窗口链路验证扩展到 `ai_after_chatgpt`
市场 regime 的完整历史回填，真实 artifacts 为：

- backfill: `paper-shadow-backfill_2138461d25e686e0`
- selection review: `system-target-selection-review_83a214b3223d937b`

当前 selection review 给出：

- `recommended_research_method=limited_adjustment`
- `decision_status=REVIEW_REQUIRED`
- `data_quality_status=PASS_WITH_WARNINGS`

本阶段要把推荐原因、风险代价、一致性、data warning 影响和 hardening 状态结构化解释给 owner。该链路仍是 research-only，不是 PIT-safe production backtest，不允许生成 official target weights、broker order、production candidate 或自动 owner approval。

## 子任务

### TRADING-219 Selection Review Reason Attribution

新增 `selection-attribution run/report` 和 `validate-selection-attribution`，读取既有 selection review，输出 method score attribution、recommendation reason breakdown 和 review-required reason breakdown。报告必须解释 `limited_adjustment` 为什么被选中、哪些 component 支持或拖累、为什么不是 `consensus_target` / `defensive_limited_adjustment` / baseline，以及 `REVIEW_REQUIRED` 中 warning 与 blocker 的区别。

### TRADING-220 Limited Adjustment Long-window Risk Review

新增 `limited-long-risk run/report` 和 `validate-limited-long-risk`，读取 paper shadow backfill，专门审查 `limited_adjustment` 在完整窗口中的 return、drawdown、volatility、turnover 和 exposure path。收益改善不得自动解释为规则胜出；报告必须披露风险资产暴露是否更高，以及 official target weights 仍不允许。

### TRADING-221 Rolling / Regime Consistency Check

新增 `limited-consistency run/report` 和 `validate-limited-consistency`，整合 rolling eval、regime review 和 stability diagnostics，输出 rolling consistency、regime consistency 和 stability consistency。pressure regime 弱点必须显式披露；证据不足时保持 `INSUFFICIENT_DATA` 或 `MIXED`，不补造稳定结论。

### TRADING-222 Data Quality Warning Impact Review

新增 `data-warning-impact run/report` 和 `validate-data-warning-impact`，解释 `PASS_WITH_WARNINGS` 对推荐结论和 hardening 的影响。若旧 backfill artifact 只记录 warning 状态但未保留 warning 明细，必须输出 `UNKNOWN` / `REVIEW_REQUIRED`，不得假装 warning 可量化或无影响。

### TRADING-223 Research Method Hardening Pack

新增 `research-method-hardening run/report` 和 `validate-research-method-hardening`，整合 TRADING-219～222，生成 `hardening_decision.json`、owner checklist、research method hardening report 和 Reader Brief section。Hardening 即使通过也只表示 `hardened_primary_research_method` 观察口径，不是 official target weights，不触发 broker 或 production。

## 实施顺序

1. 更新任务登记和本文档，记录 P0 scope 与安全边界。
2. 复用 `dynamic_v3_system_target.py` 的 artifact / latest pointer / validation 模式。
3. 增加五组 run/report payload/validate/render 函数和 CLI。
4. 更新 Reader Brief、report registry、artifact catalog、system flow、operations runbook 和 README。
5. 增加 focused tests。
6. 运行真实链路、focused tests、ruff、compileall、`git diff --check`、dynamic-v3 validation 和 artifact family validation。

## 验收标准

- 五组新增 CLI 可运行并写入 runtime artifacts。
- 所有新增 artifact 固定 `research_target_only=true`、`paper_shadow_only=true`、`not_official_target_weights=true`、`broker_action_allowed=false`、`broker_action_taken=false`、`order_ticket_generated=false`、`production_effect=none`。
- Selection attribution 解释推荐原因和 `REVIEW_REQUIRED` blocker。
- Long-window risk review 披露收益、回撤、波动、换手和 exposure path。
- Consistency check 同时覆盖 rolling、regime、stability。
- Data warning impact 对无法判断的 warning 明确 `UNKNOWN` / `REVIEW_REQUIRED`。
- Hardening pack 不写 official target weights、不触发 broker、不修改 production state。
- README、operations runbook、system flow、report registry、artifact catalog、Reader Brief 和本文档同步。
- Focused tests、ruff、compileall、`git diff --check` 和要求的 validate 命令通过，或记录阻塞原因。

## 状态记录

- 2026-06-12: 新增任务文档并进入 `IN_PROGRESS`，原因：owner 要求完成 TRADING-219～223 selection drilldown 与 research method hardening pack。
- 2026-06-12: baseline 实现完成并转入 `VALIDATING`。真实链路 artifacts：
  `selection-attribution_0206b2ebef7fa3f7`、
  `limited-long-risk_b648c3f9e6143ad6`、
  `limited-consistency_dd5b4c272ca5e24c`、
  `data-warning-impact_8902063d65aca354`、
  `research-method-hardening_be155e974fc8b08f`。
  当前 hardening decision 为 `REVIEW_REQUIRED`，confidence=`LOW`，
  candidate_method=`limited_adjustment`。主要原因：long-window risk
  为 `RETURN_IMPROVES_RISK_WORSENS`、rolling consistency 为 `UNSTABLE`、
  data warning impact 为 `REVIEW_REQUIRED`，且旧 backfill artifact 只有
  `PASS_WITH_WARNINGS` 状态、没有 warning 明细，因此 warning id 固定披露为
  `pass_with_warnings_detail_unavailable`，不能静默判断为无影响。
- 2026-06-12: 验证通过新增五组 `run/report/validate` CLI、`aits validate-data`
  等价质量门禁（状态 `PASS_WITH_WARNINGS`）、`dynamic-v3-rescue validate`、
  `artifacts validate --family dynamic_v3_rescue`、report index、Reader Brief 和
  Reader Brief quality、focused pytest `6 passed`、ruff、compileall 和
  `git diff --check`。单次 `python -m pytest tests -q` 因完整套件总耗时超过
  wrapper 超时两次未取得 monolithic 结果；随后按根目录测试文件和
  `tests/trading_engine` 测试文件分段覆盖全部 test files，合计 `2378 passed`、
  `640 warnings`，warnings 为既有 numpy / eventkit warning。
