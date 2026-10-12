# 可复用资产

- 用途：建评估台（TRADING-2565）、前瞻记录和 agent 护栏（TRADING-2566）时，先从这里找现成的东西，不要从头写。
- 来源：2026-10-12 研究结论可靠性复核第 4.3 节。
- 位置：普通路径在 main 上；`@archive:<路径>` 是 tag `research-archive-2026-10-12` 下的路径，取法见 `README.md`。

## 评估

| 资产 | 位置 | 说明 |
|---|---|---|
| 简单基线模拟器 | `src/ai_trading_system/simple_baseline_portfolio_control.py`：`_strategy_return_series`（1272 行）、`_metrics_for_strategy`（1473 行） | 信号滞后经核实正确，是事实上的共享模拟器（删除前有 28 个模块调它的私有函数）。评估台以它为核心，提成公开模块，接口是"策略给出每日目标权重" |
| 稳健性场景 | `src/ai_trading_system/backtest/robustness.py`：`same_turnover_random_scenario`、`same_exposure_random_scenario`、`_block_bootstrap_total_return_delta_ci` | 同换手、同暴露的随机基线和 block bootstrap 区间。C 区 |
| 成本模型 | `config/research/transaction_cost_model.yaml` | 删除前研究代码里的成本默认值有 0、4、5、5–15 bps 四种，评估台统一用这一份 |
| 运行出处记录 | `src/ai_trading_system/core/provenance.py` | 记录 commit、代码是否改动、配置哈希。评估台的运行器以它为核心，补上快照 id、窗口和试验编号 |
| 研究窗口政策 | `config/research/primary_research_window_policy.yaml`、`research_window_registry.yaml` | C 区 |

## 证伪检验

Composer v2 的一整套证伪检验（TRADING-2555~2559），评估台要做成通用的"标准证伪"：

| 检验 | 位置 |
|---|---|
| 暴露匹配基准、逐年剔除、成本临界点 | `@archive:src/ai_trading_system/first_layer_foundational_falsification_contract.py`、`@archive:src/ai_trading_system/first_layer_foundational_falsification_execution_v2.py`；`@archive:docs/requirements/TRADING-2555_First_Layer_Composer_V2_Foundational_Falsification_Contract_V1.md` |
| matched placebo（随机择时对照） | `@archive:src/ai_trading_system/first_layer_matched_placebo_falsification.py`；`@archive:docs/requirements/TRADING-2558_First_Layer_Composer_V2_Matched_Placebo_Falsification_V1.md` |
| 时间平移（晚 N 天执行）、逐段剔除 | `@archive:src/ai_trading_system/first_layer_temporal_influence_falsification.py`；`@archive:docs/requirements/TRADING-2559_First_Layer_Composer_V2_Temporal_Influence_Falsification_V1.md` |

## 前瞻记录

| 资产 | 位置 | 说明 |
|---|---|---|
| known-snapshot 信息集合同 | `@archive:config/research/first_layer_composer_v2_known_snapshot_input_v1.yaml`；`@archive:docs/requirements/TRADING-2560_Composer_Known_Snapshot_Prospective_Capture_V1.md` | 历史训练访问和未来结果访问分开 |
| 结果首读网关 | `@archive:src/ai_trading_system/research_outcome_access.py` | 哈希链记录谁在什么时候第一次读了结果。只接过合成数据 |
| 精确记账 | `@archive:src/ai_trading_system/research_forward_accounting.py` | 前瞻收益的精确记账 |
| capture hold | `@archive:src/ai_trading_system/data/capture_hold.py`、`@archive:scripts/capture_hold.py` | 固定 commit、独占输出路径、有效期。只是本机自证，不是独立权威 |
| 日常前瞻采集的先例 | `aits forward-evidence capture-dry-run-daily`（在日报计划里） | 新的前瞻记录直接接进日常调度，不再手工授权 |

## 清单和案例

| 资产 | 位置 | 说明 |
|---|---|---|
| PIT 风险清单 | `@archive:docs/requirements/TRADING-2402_Dynamic_Strategy_Data_PIT_And_Signal_Quality_Gap_Review.md`、`@archive:docs/requirements/TRADING-2406_Growth_Tilt_Engine_PIT_And_Signal_Construction_Remediation_Plan.md` | 评估台不变量测试的检查项来源 |
| 未校准的高影响阈值 | `@archive:docs/requirements/TRADING-693_Long_Horizon_Evidence_Floor_Calibration_Audit.md`；`config/research/threshold_registry.yaml` | 36 个高影响阈值都没有校准 |
| 标签泄漏完整案例 | `@archive:docs/requirements/TRADING-810_to_815_Tail_Risk_Policy_Family.md` → `@archive:docs/requirements/TRADING-821_to_825_Tail_Risk_Fallback_Falsification_Audit.md` → `@archive:docs/requirements/TRADING-827_Tail_Risk_Trigger_Label_Independence_Audit.md` | 做成回归测试：precision=recall=1、FP=FN=0 就是红旗 |
| 常量冒充结论的案例 | `git show 57b0ba70:src/ai_trading_system/indicator_family_ablation.py`（`FAMILY_PROFILES`）；`@archive:docs/requirements/TRADING-2542E_Growth_Action_Value_Real_DQ_And_Locked_Backtest_Review_V1.md`（`tqqq_veto` 常量 true） | 做成回归测试：结论字段不能是常量 |
| QuantConnect 平台知识 | `@archive:docs/requirements/TRADING-2480_QC_QQQ_Options_Capability_License_Evidence_Spike_V1.md`、`@archive:docs/requirements/TRADING-2489_QC_QQQ_Options_Platform_Evidence_Manual_Bundle_V1.md` | 日级 `OptionUniverse` 不带 bid/ask，只有分钟报价有；期权数据从 2012 年起；2022-08-26 订阅缺失。25 万数据点是项目自己预审的上限，不是平台限制 |
