# Dynamic Strategy Closeout Reader Brief

Dynamic strategy closeout 的建议结论是：暂停 full allocation research，保留 defensive overlay / advisory diagnostic 的 observe-only 研究价值。当前 review 覆盖 `2022-12-01`～`2026-06-26` 的 `ai_after_chatgpt` regime，并只使用 actual-path evidence；target-path metrics 继续只能作为 diagnostic，旧 dynamic 结果标注为 `CLOSEOUT_REVIEWED_LEGACY_EVIDENCE`，不能解锁 promotion。

Dynamic promotion 仍然 `BLOCKED`，原因不是单一缺口。`limited_adjustment` 是最强 surviving candidate，但它只是略高于 `qqq_60_sgov_40`，仍低于 `100_qqq`，且 stress gate 仍 blocked。`dynamic_v0_5_ai_trend_confirmed_only` 受 false risk-off 和 regime overfit 限制；两个 staleness-aware repaired variants 没有 material improvement；event override variants 要么增加 turnover 太多，要么过于 noisy，并且 runtime trace 仍缺 event type / source taxonomy provenance。Cost/cash、stress、regime baseline expansion 与 artifact governance 共同说明：证据可审计，但不足以支持 full allocation strategy。

建议降级为 defensive overlay，是因为部分模块仍可帮助风控复核：event risk score、regime diagnosis、high-vol stress flag、cash/SGOV fallback advisory 和 manual review prompt。这些能力适合观察风险、提示降风险、支持人工复核，不适合直接控制完整仓位。后续若 owner 批准，可以进入 observe-only forward watch，记录 risk-off / risk-on advisory 后 1d、5d、10d、20d 的市场表现，积累前瞻证据，而不是继续在同一历史窗口调参。

仍被禁止的行为包括：恢复 dynamic promotion、进入 paper-shadow、写 production weights、自动 risk-on、自动交易、broker order、用 target-path metrics 重新打开 full allocation research、用 legacy dynamic evidence 当作 promotion evidence。重新打开 full allocation research 必须满足新的 actual-path locked-sample edge、多 regime 正贡献、正的 timing attribution、net-of-cost 有效、stress 不恶化、turnover 受控、PIT audit 通过、walk-forward / out-of-sample 通过，并由 owner 手动批准 paper-shadow preflight。
