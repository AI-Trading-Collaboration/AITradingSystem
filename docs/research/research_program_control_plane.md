# Research Program Control Plane

最后更新：2026-06-19

状态：`CONTROL_PLANE_CONTRACT_READY`

该控制面只定义实验入口和 admission contract，不运行 backfill。它读取 roadmap、ablation
protocol、validation policy、window catalog 和 scorecard contract，生成 experiment run
manifest、experiment id、candidate id、mini/full admission decision 和 research program status。

禁止输出 paper-shadow candidate、official target weights、broker/order artifact、automatic owner
decision 或 production mutation。

当前 program status：`WEIGHT_RESEARCH_PROGRAM_NEEDS_MORE_EVIDENCE`。

当前 executable result：`B1_MINI_BACKFILL_COMPLETE_RESEARCH_ONLY_MIXED`。B1 只使用
execution/no-trade/turnover-control runner，证据为 mixed；B2-B6 仍需 owner/system 复核、
独立 runner 和 signal robustness evidence 后才能继续。
