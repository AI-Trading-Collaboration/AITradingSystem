# QLD 增量工具价值评估

- 机械结论：`QLD_ELIGIBLE_FOR_OWNER_ACTION_UNIVERSE_REVIEW`
- 数据质量：`PASS_WITH_WARNINGS`
- Canonical full-cache data quality：`FAIL`
- 本报告采用 Owner 批准的 scoped 五资产 DQ；不声称 canonical full-cache PASS。
- 评估区间：`2021-02-22` 至 `2026-07-21`
- 共同交易日：1359
- SPY 角色：立即进入 reference / benchmark / regime-control，不进入当前权重动作空间。
- QLD 角色：Owner 已批准为 role-limited 2x execution / implementation instrument。
- 角色决策：`owner_decision:TRADING-2459:2026-07-25:approve_qld_role_limited_2x_implementation_instrument`
- 自动执行：未批准；本报告不生成或修改正式 target weights。

## Full-primary cadence 判定

| Cadence | QLD Pareto non-dominated | 严格优于两种 comparator 的 objective | 被谁支配 |
|---|---:|---|---|
| buy_and_hold | 是 | terminal_value_net, cagr | - |
| weekly | 是 | max_drawdown, worst_20d_loss, external_turnover, cost_drag | - |
| monthly | 是 | external_turnover, cost_drag | - |
| daily | 是 | worst_5d_loss, worst_20d_loss, external_turnover, cost_drag | - |

## 关键指标（full-primary）

```json
{
  "buy_and_hold": {
    "qld_100": {
      "total_return": 1.9820898696088225,
      "cagr": 0.22392921545952316,
      "annualized_volatility": 0.4518362710851382,
      "max_drawdown": -0.6366418889374725,
      "sharpe": 0.6744528306255484,
      "worst_5d_loss": -0.23206751054852315,
      "worst_20d_loss": -0.30428087465352627,
      "external_turnover": 1.0,
      "cost_drag": 0.0011933132731454243
    },
    "qqq_50_tqqq_50": {
      "total_return": 1.7297120057152315,
      "cagr": 0.20407630829379952,
      "annualized_volatility": 0.4293661906347547,
      "max_drawdown": -0.6262925818041523,
      "sharpe": 0.6490038524881132,
      "worst_5d_loss": -0.22011704124568432,
      "worst_20d_loss": -0.29504828848984377,
      "external_turnover": 1.0,
      "cost_drag": 0.0010923217309870559
    },
    "sgov_33_tqqq_67": {
      "total_return": 1.5233074741486647,
      "cagr": 0.18669480473107747,
      "annualized_volatility": 0.4402416878382972,
      "max_drawdown": -0.637340248299292,
      "sharpe": 0.6109845596274907,
      "worst_5d_loss": -0.23382596337251904,
      "worst_20d_loss": -0.3096970664170223,
      "external_turnover": 1.0,
      "cost_drag": 0.0010097268804170056
    }
  },
  "weekly": {
    "qld_100": {
      "total_return": 1.9820898696088225,
      "cagr": 0.22392921545952316,
      "annualized_volatility": 0.4518362710851382,
      "max_drawdown": -0.6366418889374725,
      "sharpe": 0.6744528306255484,
      "worst_5d_loss": -0.23206751054852315,
      "worst_20d_loss": -0.30428087465352627,
      "external_turnover": 1.0,
      "cost_drag": 0.0011933132731454243
    },
    "qqq_50_tqqq_50": {
      "total_return": 2.023584266001528,
      "cagr": 0.22706105190158143,
      "annualized_volatility": 0.4482463057774528,
      "max_drawdown": -0.6378778578739294,
      "sharpe": 0.682040255843651,
      "worst_5d_loss": -0.23061791436030787,
      "worst_20d_loss": -0.30674005854142017,
      "external_turnover": 4.211371352629453,
      "cost_drag": 0.005097923495032131
    },
    "sgov_33_tqqq_67": {
      "total_return": 1.902268461457572,
      "cagr": 0.2178033532582735,
      "annualized_volatility": 0.44692789165170954,
      "max_drawdown": -0.6436974182483038,
      "sharpe": 0.6658177816710437,
      "worst_5d_loss": -0.2290551098739234,
      "worst_20d_loss": -0.30920052637798934,
      "external_turnover": 5.281891643789899,
      "cost_drag": 0.006138526128245392
    }
  },
  "monthly": {
    "qld_100": {
      "total_return": 1.9820898696088225,
      "cagr": 0.22392921545952316,
      "annualized_volatility": 0.4518362710851382,
      "max_drawdown": -0.6366418889374725,
      "sharpe": 0.6744528306255484,
      "worst_5d_loss": -0.23206751054852315,
      "worst_20d_loss": -0.30428087465352627,
      "external_turnover": 1.0,
      "cost_drag": 0.0011933132731454243
    },
    "qqq_50_tqqq_50": {
      "total_return": 2.0172979609462125,
      "cagr": 0.22658884649587785,
      "annualized_volatility": 0.4436698481267582,
      "max_drawdown": -0.6359933678564755,
      "sharpe": 0.6838204149841484,
      "worst_5d_loss": -0.228912188821643,
      "worst_20d_loss": -0.29947795907536146,
      "external_turnover": 2.532038949946709,
      "cost_drag": 0.0030577698873166526
    },
    "sgov_33_tqqq_67": {
      "total_return": 1.8787620471011843,
      "cagr": 0.21597320081785543,
      "annualized_volatility": 0.43811034787731595,
      "max_drawdown": -0.6403068295106467,
      "sharpe": 0.6672861741596685,
      "worst_5d_loss": -0.2257456299745454,
      "worst_20d_loss": -0.29478398305479214,
      "external_turnover": 3.018415576247535,
      "cost_drag": 0.003478072750739525
    }
  },
  "daily": {
    "qld_100": {
      "total_return": 1.9820898696088225,
      "cagr": 0.22392921545952316,
      "annualized_volatility": 0.4518362710851382,
      "max_drawdown": -0.6366418889374725,
      "sharpe": 0.6744528306255484,
      "worst_5d_loss": -0.23206751054852315,
      "worst_20d_loss": -0.30428087465352627,
      "external_turnover": 1.0,
      "cost_drag": 0.0011933132731454243
    },
    "qqq_50_tqqq_50": {
      "total_return": 2.068765492633782,
      "cagr": 0.23043156660311692,
      "annualized_volatility": 0.44862143434641444,
      "max_drawdown": -0.634995724023194,
      "sharpe": 0.6879030630577314,
      "worst_5d_loss": -0.2323997454242257,
      "worst_20d_loss": -0.30502275911203225,
      "external_turnover": 7.957575737569602,
      "cost_drag": 0.00978379876684432
    },
    "sgov_33_tqqq_67": {
      "total_return": 1.9992058162272555,
      "cagr": 0.22522533814699597,
      "annualized_volatility": 0.4475417926292139,
      "max_drawdown": -0.637418382391032,
      "sharpe": 0.6789871579986819,
      "worst_5d_loss": -0.2325459998355246,
      "worst_20d_loss": -0.3054181588934307,
      "external_turnover": 10.315966024060804,
      "cost_drag": 0.012401721067590632
    }
  }
}
```

## Tracking diagnostics

```json
{
  "QLD": {
    "target_daily_multiplier": 2.0,
    "residual_bias_daily": -0.00020105861089401005,
    "residual_bias_annualized": -0.050666769945290534,
    "residual_mae": 0.0005066785323216143,
    "residual_rmse": 0.000721312369681887,
    "correlation": 0.9997036937105532,
    "realized_beta": 1.9990226705734921
  },
  "TQQQ": {
    "target_daily_multiplier": 3.0,
    "residual_bias_daily": -0.0003673431975985499,
    "residual_bias_annualized": -0.09257048579483457,
    "residual_mae": 0.0008310997832133311,
    "residual_rmse": 0.0011902432094562407,
    "correlation": 0.9996920298115026,
    "realized_beta": 2.9700878926923875
  }
}
```

## 解释边界

- 这是 historical-seen instrument implementation diagnostic，不是新策略搜索，也不是 unbiased OOS。
- 2026-07-22 之后的共享源行曾在审计终端中可见，但未进入本次计算。
- 独立趋势模型必须先确认可信 Nasdaq-100 上升趋势，组合层必须先形成接近 2x 的 QQQ-equivalent target，且风险门必须通过；QLD 不参与这些上游判断。
- QLD 不得作为 trend signal、独立 strategy style、自由 candidate dimension，也不得按本次历史收益动态切换工具。
- “接近 2x”的数值容差、执行 selector、forward shadow 验收和退出规则尚未治理；在这些政策完成前，automatic instrument selection、paper-shadow、production 和 broker action 均保持关闭。
