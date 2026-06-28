# TRADING-2006 to 2025 Risk-On Veto Observe-Only Diagnostic

## 背景

TRADING-1976～2005 已收口为 `CHANNEL_V3_RISK_ON_VETO_ONLY`：

- `do_not_de_risk_pass=false`，未通过 false risk-off / missed upside / defensive regression gate。
- `risk_on_veto_pass=true`，但只能作为 blocker / veto diagnostic。
- `candidate_count=0`，promotion、paper-shadow、production、broker 全部 disabled。

本批把该结论从 v3 selection result 固化为后续可审计的 observe-only diagnostic contract，并正式归档失败的 `do_not_de_risk v3` 分支。

## 非目标

- 不继续调 `do_not_de_risk v3`。
- 不训练新的 add-risk、growth overlay 或 TQQQ 模型。
- 不输出 portfolio weights、target allocation、trade action、recommended allocation 或 broker action。
- 不进入 owner review、promotion、paper-shadow、production 或 broker。
- 不把 `risk_on_veto` 解释为 risk-on / buy / allocation signal。

## 分阶段实施

1. TRADING-2006～2007：生成 channel v3 closeout reclassification 与 do-not-de-risk v3 archive artifact。
2. TRADING-2008～2009：新增 risk-on veto diagnostic contract 与 metric policy，明确 active cost、avoided cost、lost upside 和 net benefit 口径。
3. TRADING-2010～2012：实现 episode extractor 与 `aits research trends risk-on-veto-diagnostic`，生成 episode CSV、summary JSON、tracked YAML 和 review 文档。
4. TRADING-2013～2015：生成 2022/2023+ behavior review、veto tradeoff review 和 return-seeking diagnostic compatibility。
5. TRADING-2016～2017：新增 guardrail tests 和 owner brief，验证 forbidden fields 与 promotion/paper-shadow/production/broker 全部 blocked。

## 验收标准

- `outputs/research_trends/risk_on_veto_diagnostic/risk_on_veto_episodes.csv` 存在且不含 allocation/trade/broker 字段。
- `outputs/research_trends/risk_on_veto_diagnostic/summary.json` 披露 data quality status、raw active/inactive cost、avoided cost、captured-upside lost、net veto benefit、hit/false-positive/false-negative rates。
- tracked YAML 均包含 research audit metadata 和 fixed safety fields。
- `do_not_de_risk v3` 明确归档为 `DO_NOT_DERISK_V3_ARCHIVED_NO_MATERIAL_IMPROVEMENT`。
- `risk_on_veto v3` 明确为 `RISK_ON_VETO_V3_OBSERVE_ONLY_DIAGNOSTIC`，owner review / promotion / paper-shadow / production / broker 均 disabled。
- report registry、artifact catalog、system flow 和 task register 同步更新。
- 并行 focused pytest、Ruff、compileall、治理检查和 `git diff --check` 通过。

## 进展记录

- 2026-06-28：任务登记为 `IN_PROGRESS`；开始实现 observe-only diagnostic runner、配置、报告和 guardrail tests。
- 2026-06-28：实现完成并转入 `VALIDATING`；真实运行 `aits research trends risk-on-veto-diagnostic` 生成 episode CSV、summary JSON、tracked YAML 和 owner brief。Data quality status=`PASS_WITH_WARNINGS`；episode_count=2771、veto_active_count=531、blocked_add_risk_count=347；raw active/inactive false-add-risk cost=`0.007518/0.006566`；net_veto_benefit_total=`-2.343111`；compatibility=`VETO_TOO_STRICT_FOR_RETURN_SEEKING_DIAGNOSTIC`；`do_not_de_risk v3` archive status=`DO_NOT_DERISK_V3_ARCHIVED_NO_MATERIAL_IMPROVEMENT`。Focused parallel pytest、documentation/audit/channel pytest、Ruff、compileall、task-register consistency check 和 `git diff --check` 已通过。
