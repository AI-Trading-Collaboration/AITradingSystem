# TRADING-1031 to 1048 Equal-Risk Forward-Aging Stabilization and Layer-2 Growth Restart

最后更新：2026-06-26

## 背景

TRADING-1024 to 1030 已将 Layer-1 simple-rule selector 归档为 dry-run only，并将
`equal_risk_qqq_sgov` 保持为 defensive primary forward-aging 主线。下一阶段分为两条
research-only 主线：

- Track A: 稳定 `equal_risk_qqq_sgov` forward-aging observation、maturity、scoreboard 和 Reader Brief 极简摘要。
- Track B: 重启 Layer-2 controlled growth component research，但使用比旧 QQQ-plus growth 更严格的 component-ready 标准。

所有新增输出必须继续保持：

- `paper_shadow_allowed=false`
- `production_allowed=false`
- `broker_action=none`
- `manual_review_required=true`

## 阶段拆解

|任务|阶段|状态|验收标准|
|---|---|---|---|
|TRADING-1031|Equal-risk scheduler integration|VALIDATING|新增 `aits research strategies equal-risk-forward-aging-scheduler-integration`，交易日每日最多写一次 observation，非交易日不写，重复运行返回 `OBSERVATION_ALREADY_EXISTS`。|
|TRADING-1032|Observation continuity check|VALIDATING|新增 continuity CLI，输出 latest/expected/actual/missing/duplicate/invalid/replacement/data-quality 字段。|
|TRADING-1033|First maturity monitor|VALIDATING|新增 maturity monitor CLI，统计 5d/10d/20d/60d/120d mature/pending/missing，确认不改写 target weights、signal inputs、definition hash。|
|TRADING-1034|Scoreboard safety gate|VALIDATING|新增 scoreboard safety CLI，样本不足时输出 `INSUFFICIENT` 并阻断 medium horizon / paper-shadow readiness。|
|TRADING-1035|Reader Brief live summary|VALIDATING|新增 equal-risk Reader Brief 极简 live summary CLI，不输出交易建议、目标实盘仓位或调仓语言。|
|TRADING-1036|Growth restart contract|VALIDATING|新增 Layer-2 growth restart contract，明确最低进入条件和禁止路径。|
|TRADING-1037|Controlled growth registry v2|VALIDATING|新增 `config/research/controlled_growth_component_candidate_registry_v2.yaml` 和 registry review CLI。|
|TRADING-1038|Beta-adjusted edge contract|VALIDATING|新增 beta-adjusted edge contract CLI，明确 beta attribution / penalty / net edge 字段。|
|TRADING-1039|Low-turnover controlled growth search|VALIDATING|新增 low-turnover growth search CLI，输出候选指标、beta-adjusted edge 和 dominance status。|
|TRADING-1040|Volatility-targeted growth search|VALIDATING|新增 vol-targeted search CLI，覆盖 target vol/window/TQQQ cap/SGOV min/rebalance grid。|
|TRADING-1041|Drawdown-guarded growth search|VALIDATING|新增 drawdown-guarded search CLI，输出 drawdown reduction、missed rebound、late risk-on/off、turnover、switch count、Calmar edge。|
|TRADING-1042|Beta exposure attribution|VALIDATING|新增 attribution CLI，拆解 QQQ beta、TQQQ overlay、timing、SGOV carry、cash/leverage drag。|
|TRADING-1043|Period/drawdown validation|VALIDATING|新增 period/drawdown validation CLI，覆盖 2022、2023、2024、2025-to-latest、最大 QQQ/TQQQ drawdown 和 SGOV carry period。|
|TRADING-1044|Cost/turnover sensitivity|VALIDATING|新增 sensitivity CLI，覆盖 zero/low/medium/high cost、1d/2d lag、monthly/threshold rebalance。|
|TRADING-1045|Growth readiness gate|VALIDATING|新增 readiness gate CLI，判断是否允许进入 component-ready review，默认仍 research-only。|
|TRADING-1046|Owner decision pack|VALIDATING|新增 owner decision pack CLI，回答 material edge、beta-adjusted edge、drawdown、AI rally concentration、definition hash、reviewability 和 blocked tracks。|
|TRADING-1047|Dual-track roadmap|VALIDATING|新增 dual-track roadmap CLI，明确 equal-risk 主线、growth research track、paused/blocked tracks 和 owner next action。|
|TRADING-1048|Roadmap v2 master review|VALIDATING|新增 master review CLI，汇总 1031～1047 并保持 no paper-shadow/no production/no broker。|

## 依赖与顺序

1. 先实现 task register、需求文档、policy/config、report registry、artifact catalog 和 system flow 更新。
2. 接入 Track A CLI，复用 `simple_baseline_forward_aging` 与现有 stabilization helpers。
3. 新增 controlled growth component v2 module/config/CLI/tests。
4. 新增 roadmap v2 聚合与 Reader Brief safety tests。
5. 执行附件要求的 Ruff、compileall、focused parallel pytest 和 `git diff --check`。

## 验收命令

```bash
python -m ruff check src tests
python -m compileall -q src tests
python -m pytest -n 16 --dist loadfile tests/test_layer1_meta_policy_readiness.py
python -m pytest -n 16 --dist loadfile tests/test_layer2_strategy_component_readiness.py
python -m pytest -n 16 --dist loadfile tests/test_task_register_consistency.py tests/test_report_index.py tests/test_documentation_contract.py
python -m pytest -n 16 --dist loadfile tests/test_equal_risk_growth_research_restart.py
git diff --check
```

## 进展记录

- 2026-06-25: 新增并进入 `IN_PROGRESS`，原因：owner 要求完成 TRADING-1031～1048。实施边界为 research-only；Layer-1 selector 不恢复，QQQ-plus growth 旧候选不直接 selectable，TQQQ-heavy、tail-risk fallback、LEAPS、Wheel、Options 继续暂停或阻塞。
- 2026-06-25: 实现完成并进入 `VALIDATING`。新增 18 个 research strategies CLI/artifacts、controlled growth component registry v2、report registry entries、artifact catalog/system flow 更新、focused tests 和 roadmap/owner docs；验证通过 Ruff、compileall、focused equal-risk/growth pytest、report/task/documentation pytest、Layer-1/Layer-2 readiness regression pytest 和 `git diff --check`。所有新增 artifacts 继续固定 no paper-shadow/no production/no broker。
