# Weight Research Retrospective

最后更新：2026-06-19

状态：`RETROSPECTIVE_READY`

## Reader Brief

- Summary：上一轮候选失败不是单一工程问题，而是 data coverage、binding、signal robustness、
  allocator mapping、risk control、cost/benchmark 和 window fragility 的组合问题。
- Key Result：`RETROSPECTIVE_READY`
- Blocking Issues：B1 已有 mixed execution-control mini-backfill；B2-B6 真实消融结果尚不存在，
  不能进入 candidate v3。
- Warnings：已反复使用的 stress/casebook 只能作为 development 或 diagnostic set。
- Safety Boundary：`research_only=true`、`manual_review_only=true`、`production_effect=none`
- Next Action：复核 B1 mixed evidence；B2-B6 在独立 runner 和 signal evidence 满足前保持
  blocked。

## Failure Taxonomy

|类别|当前证据|下一步|
|---|---|---|
|`DATA_COVERAGE_FAILURE`|signal series coverage gap、market coverage gap、partial/stale inputs|先修数据覆盖，不调策略。|
|`BINDING_FAILURE`|`v_shaped_recovery` source window dates missing；partial/static proxy|扩展 historical signal/weight binding。|
|`SIGNAL_ROBUSTNESS_FAILURE`|signal robustness review 为 BLOCKED|把信号质量修复与 allocator redesign 分开。|
|`ALLOCATOR_MAPPING_FAILURE`|单一 regime-to-weight 同时承担 signal、risk、cost、execution|拆成 signal、allocator、risk-control、execution layers。|
|`RISK_CONTROL_FAILURE`|`slow_drawdown` failed，V-shaped recovery coverage missing|独立测试 fast asymmetric risk overlay。|
|`EXECUTION_COST_FAILURE`|medium/high cost 场景仍 weak|先测试 no-trade band 和 turnover control。|
|`BENCHMARK_FAILURE`|相对 QQQ/SPY/SMH/SOXX/equal-weight margin 不足|每个 ablation result 必须输出 benchmark-relative rows。|
|`WINDOW_FRAGILITY_FAILURE`|recent / stress-heavy splits fragile|冻结 development、diagnostic 和 holdout 用途。|
|`OVERFIT_RISK`|诊断窗口被反复使用，参数附近稳定性未证明|进入 full backfill 前要求 neighborhood stability 和 purged walk-forward。|
|`GOVERNANCE_HOLD`|当前没有治理绕行，但安全边界必须保持|validation PASS 不能写成 research PASS。|
