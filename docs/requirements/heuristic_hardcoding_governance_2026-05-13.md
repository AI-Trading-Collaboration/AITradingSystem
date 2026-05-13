# 启发式硬编码治理与初版审计

状态：VALIDATING

最后更新：2026-05-13

关联任务：`GOV-004`

## 背景

`CALIBRATION-003` 中原先把 feedback 样本下限写成 `30`，暴露了更一般的问题：投资评估系统中很多阈值、分档、样本下限和 promotion 条件如果没有配置、依据或回测验证，就会变成不可迭代的“想当然 if/else”。这类逻辑会影响评分解释、仓位 gate、反馈校准、回测晋级和日报结论，因此需要进入项目规则和持续审计。

本文件是初版审计，不直接改变 production 结论；它给后续分批迁移提供清单和优先级。

## 新增项目规则

`AGENTS.md` 已新增 `Heuristic and Threshold Governance`：

- 影响投资解释的 threshold、score band、confidence cutoff、sample floor、position cap、readiness rule、promotion gate、risk multiplier、report boundary 或 backtest acceptance rule，必须配置化或有明确 rationale。
- 允许低风险例外：0/1/100 这类纯尺度边界、数组索引、格式精度、单位换算、协议/schema 常量、HTTP timeout、retry count、UI 尺寸和测试 fixture，前提是不改变投资解释。
- pilot 阶段可以使用临时 baseline，但必须登记任务、写明用途、影响、风险、验证计划和退出条件。
- 报告应披露依赖的 policy/config version 或链接，避免结论无法追溯。

## 审计范围

初版静态扫描范围聚焦投资评估路径：

- `src/ai_trading_system/scoring/`
- `src/ai_trading_system/backtest/`
- `src/ai_trading_system/decision_outcomes.py`
- `src/ai_trading_system/prediction_ledger.py`
- `src/ai_trading_system/feedback_loop_review.py`
- `src/ai_trading_system/periodic_investment_review.py`
- `src/ai_trading_system/market_feedback_optimization.py`
- `config/*.yaml` 中与 scoring / portfolio / risk / data quality / feedback sample policy 相关的阈值

粗筛命令：

```powershell
rg -n "(>=|<=|<|>)\s*(5|10|20|30|40|50|55|60|65|70|75|80|90|95|100|0\.[0-9]+)" `
  src\ai_trading_system\scoring `
  src\ai_trading_system\backtest `
  src\ai_trading_system\decision_outcomes.py `
  src\ai_trading_system\prediction_ledger.py `
  src\ai_trading_system\feedback_loop_review.py `
  src\ai_trading_system\periodic_investment_review.py `
  src\ai_trading_system\market_feedback_optimization.py
```

初版粗筛命中 35 处比较型数字字面量，分布在 8 个投资评估相关文件。该数字不是最终缺陷数，因为它包含一部分尺度边界和已参数化输入；但足以说明需要治理。

## 初版发现摘要

|类别|代表位置|风险|处理方向|
|---|---|---|---|
|样本下限 / maturity floor|`decision_outcomes.py`、`prediction_ledger.py`、`feedback_loop_review.py`、`periodic_investment_review.py`、`backtest/promotion_gate.py`、`cli.py` 中的 `30`|P1：会决定是否可解释为稳定样本、是否可晋级或报告颜色|迁移到 `config/feedback_sample_policy.yaml` 或 model promotion policy；区分 reporting / pilot / diagnostic / promotion floor。|
|Score -> position band|`scoring/position_model.py` 中 `80/65/50/35` 和 `0.8/0.6/0.4/0.2` 等仓位带|P1：直接影响模型目标仓位和日报解释|迁移到 scoring/position policy 配置；每个 band 需要 rationale、有效期和回测/forward shadow 引用。|
|日报结论边界|`scoring/daily.py` 中 `45/55/65`、component score `<50` / `>=55` 等|P1：影响结论措辞、支持项/限制项分类和复盘解释|迁移到 report conclusion policy；报告输出 policy version。|
|Confidence cutoff / cap multiplier|`scoring/daily.py` 中 `0.60/0.75`、`75/60/45`、`1.0/0.85/0.70/0.50`|P1：影响 confidence gate 和最终仓位上限|迁移到 confidence policy；与 `confidence_position_gate` 文档和样本反馈对齐。|
|Backtest robustness / promotion threshold|`backtest/robustness.py` 中 `0.95/0.05/0.70`，`backtest/promotion_gate.py` 中 `30`|P1：影响模型 promotion 判断和样本外稳定性解释|迁移到 calibration protocol / promotion policy；记录 trial、OOS、DSR/PBO 或替代证据。|
|Feature coverage threshold|`backtest/daily.py` 中 `0.9`，`backtest/audit.py` 默认 `0.9`|P1：影响 backtest data quality 和可用性结论|迁移到 data quality / backtest coverage policy。|
|配置中的启发式阈值|`config/scoring_rules.yaml`、`config/portfolio.yaml`、`config/risk_events.yaml`、`config/scenario_library.yaml`、`config/data_quality.yaml`、`config/feedback_sample_policy.yaml`|P1/P2：已配置化但不一定都有 rationale、owner、status、validation 引用|补 metadata/rationale 字段或链接 requirement；报告引用 policy version。|

## 当前已改善项

- `CALIBRATION-003` 已把 feedback 样本门槛从硬编码 `30` 改为 `config/feedback_sample_policy.yaml`。
- 新政策区分 reporting、pilot、diagnostic、promotion 四层，使当前样本少时可启动 pilot 流程，但不能晋级 production。
- `market_feedback_optimization` 报告输出样本政策版本、配置路径和四层 floor。
- P1-A 第一批迁移已完成：`decision_outcomes.py`、`prediction_ledger.py`、`feedback_loop_review.py`、`periodic_investment_review.py`、`backtest/promotion_gate.py` 和相关 CLI 状态颜色改为读取 `config/feedback_sample_policy.yaml`；样本门槛不再由这些路径的裸 `30` 决定。
- P1-B 第一批迁移已完成：`config/scoring_rules.yaml` 增加 `policy_metadata`、`position_bands`、`daily_conclusion`、`confidence_policy` 和完整 `source_type_confidence`；`WeightedScoreModel` 不再内置 score->position band，`score-daily`、回测评分和 robustness 信号族基线读取同一套配置；日报 `score_architecture_audit` 输出 scoring policy metadata。
- P1-C 第一批迁移已完成：新增 `config/backtest_validation_policy.yaml`，管理 robustness 默认实验参数、解释阈值和 promotion gate 要求；`aits backtest` 未显式传入 robustness 参数时读取该 policy，robustness/promotion 报告输出 policy metadata；promotion 的 shadow outcome floor 继续读取 `config/feedback_sample_policy.yaml`。
- Feature coverage 第一批迁移已完成：`backtest/daily.py` 的 data credibility 覆盖率阈值和 `aits backtest --minimum-component-coverage` 默认值读取 `config/backtest_validation_policy.yaml` 的 `data_credibility.component_coverage_min`。

## 迁移优先级

|优先级|范围|原因|验收标准|
|---|---|---|---|
|P1-A|样本下限 / maturity floor|已经直接影响 feedback、shadow maturity、promotion 和投资复盘|所有样本门槛从统一 sample policy 或 promotion policy 读取；报告输出 policy version。|
|P1-B|Score band / confidence band / position band|直接影响仓位和日报动作语言|`position_model` 和 `scoring/daily` 不再用无解释数字字面量；配置记录 rationale 和适用市场阶段。|
|P1-C|Backtest promotion / robustness threshold|影响是否把策略判断视为可晋级|promotion gate 读取 calibration protocol 或 promotion policy；报告列出每项阈值来源。|
|P2|已配置化阈值补 rationale|风险低于代码硬编码，但仍影响解释|关键 YAML 增加 owner/status/rationale/validation_ref 或链接需求文档。|
|P2|自动审计工具|当前靠人工 `rg`，容易漏项|新增只读 audit 命令或测试，发现新投资解释 numeric literal 时提示。|

## 例外边界

以下内容初版不作为迁移目标：

- 0/1/100 这类纯比例或百分制尺度边界。
- `DEFAULT_OUTCOME_HORIZONS = (1, 5, 20, 60, 120)` 这类明确的观察窗口集合；但如改变默认 horizon，仍需文档记录。
- HTTP timeout、retry、UI 尺寸、文本格式精度。
- 测试 fixture 中为了构造样例而出现的数字。

## 状态记录

- 2026-05-13：新增任务和项目规则，原因：owner 要求 review 当前项目中类似 `30` 样本门槛的启发式硬编码，并把治理要求加入项目规则。
- 2026-05-13：完成初版静态审计。粗筛在投资评估相关路径命中 35 处比较型数字字面量，归类为样本门槛、score/position band、confidence/report boundary、robustness/promotion threshold、coverage threshold 和已配置但缺 rationale 的启发式阈值。
- 2026-05-13：完成 P1-A 第一批迁移。样本 floor 从多个 feedback/report/promotion 代码路径迁移到 `config/feedback_sample_policy.yaml`；相关报告开始披露 sample policy version 或使用 policy 的 promotion / diagnostic floor。
- 2026-05-13：完成 P1-B 第一批迁移。评分到仓位、日报结论边界、confidence cutoff/cap、source type confidence 迁移到 `config/scoring_rules.yaml`；正式评分路径不再依赖散落的阈值 if/else，报告审计输出 policy metadata。
- 2026-05-13：完成 P1-C 第一批迁移。新增 `config/backtest_validation_policy.yaml`，回测稳健性默认实验参数和 promotion gate 关键要求改为 policy 驱动；`backtest_robustness` 与 `model_promotion` 摘要输出 policy 信息。
- 2026-05-13：完成 feature coverage 第一批迁移。Backtest Data Quality 的模块覆盖率阈值和 CLI 审计覆盖率默认值改为读取 `config/backtest_validation_policy.yaml`。
