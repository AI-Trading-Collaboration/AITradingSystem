# Risk-On Veto Observe-Only Owner Brief

## 1. 为什么 do_not_de_risk v3 归档？

`do_not_de_risk v3` 未通过 false risk-off / missed upside / defensive regression gate。归档状态为 `DO_NOT_DERISK_V3_ARCHIVED_NO_MATERIAL_IMPROVEMENT`；defensive_probe_regression_count=`8`，2022_slice_not_worse=`False`。

## 2. 为什么 risk_on_veto 保留？

`risk_on_veto` 是 TRADING-1976～2005 唯一通过的 channel，final_status=`CHANNEL_V3_RISK_ON_VETO_ONLY`。它保留的理由是 false add-risk / compiler veto diagnostic 仍有解释价值。

## 3. risk_on_veto 是什么，不是什么？

它是 veto / blocker，用于说明何时不应轻易 add-risk、growth overlay 或 TQQQ exposure。它不是 allocation signal、risk-on signal、buy signal 或 TQQQ signal，也不能进入 owner review、promotion、paper-shadow、production 或 broker。

## 4. active false-add-risk cost 为什么不能单独解释？

Veto active 行本来就可能处在更危险环境，所以 raw active cost 高于 inactive mean 不等于 veto 失败。必须同时看 blocked add-risk reference 下的 avoided cost、captured-upside lost 和 net veto benefit。

## 当前诊断摘要

- raw active false-add-risk cost: `0.007518`
- raw inactive false-add-risk cost: `0.006566`
- avoided false-add-risk cost total: `1.07859`
- captured upside lost total: `3.421701`
- net veto benefit total: `-2.343111`
- compatibility status: `VETO_TOO_STRICT_FOR_RETURN_SEEKING_DIAGNOSTIC`

结论：本批只建立 observe-only diagnostic 能力。candidate_count=0，promotion / paper-shadow / production / broker 全部 disabled。
