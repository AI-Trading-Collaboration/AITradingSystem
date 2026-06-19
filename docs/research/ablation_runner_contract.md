# Ablation Runner Contract

最后更新：2026-06-19

状态：`RUNNER_CONTRACT_READY_B0_B1_IMPLEMENTED_ONLY`

Runner 输入包括 ablation layer id、candidate spec、research window catalog、input data
artifacts、scorecard contract 和 safety metadata。输出必须包括 ablation run result、utility
score、stress/cost/benchmark/window/signal robustness summaries 和 blocking reasons。

执行前必须运行 `aits validate-data` 或同一 validation code path；失败必须停止，不能生成
technical features、scoring outputs、backtest results 或 daily-style reports。

Runner 不得输出 official target weights、paper-shadow activation、broker/order artifact、
production mutation 或 automatic owner decision。

当前只完成 B0 static strategic baseline control result 和 B1 execution-control runner。
B1 证据为 mixed；B2-B6 仍需要独立 runner 和 signal robustness evidence，不能用 P0 动态策略
总结果替代分层消融。
