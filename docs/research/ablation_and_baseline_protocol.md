# Ablation And Baseline Protocol

最后更新：2026-06-19

状态：`ABLATION_PROTOCOL_FROZEN`

|层|唯一新增机制|必须比较|
|---|---|---|
|B0|static strategic baseline|control|
|B1|execution/no-trade/turnover control|B1 vs B0|
|B2|fast asymmetric risk scaler|B2 vs B0|
|B3|slow relative tilt|B3 vs B0|
|B4|B2 + B3 组合|B4 vs B2；B4 vs B3|
|B5|confidence shrinkage|B5 vs B4|
|B6|regime information|B6 vs B5|

每层必须输出 return proxy、drawdown proxy、turnover、rotation count、cost-adjusted proxy、
benchmark-relative result、stress result、window stability 和 signal robustness status。

硬停止：B0 不可复现、window catalog 未冻结、scorecard 参数未冻结、任何 official target
weight 语义、signal robustness BLOCKED、stress/cost/benchmark/window weak 或 fragile、
相对 baseline 无独立净增益。
