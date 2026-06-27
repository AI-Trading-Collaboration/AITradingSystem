# Post-Window-Extension Research Discipline Closeout

- 状态：`PRIMARY_WINDOW_ADOPTED_LEGACY_OVERFIT_BLOCKED`
- primary window：`exact_three_asset_validated`
- default start：`2021-02-22`
- legacy window：`legacy_research_window_2022_12`
- sensitivity window：`exact_three_asset_primary_only_extension`
- promotion_allowed：`False`
- paper_shadow_allowed：`False`
- production_allowed：`False`
- broker_action：`none`

## Closeout

本批正式采用 `2021-02-22` primary validated window 作为后续 first-layer、second-layer、actual-path 和 owner-review research default。

`2022-12-01` legacy window 降级为 comparison-only。`2020-05-28` extension 只允许作为带 SGOV secondary-source caveat 的 sensitivity。Owner requested `2020-05-26` range 保留为 metadata-only，不得用于组合收益起点。

## Audit Discipline

Post-1665 research artifact 进入 owner review 前必须携带：

- research window metadata；
- `research_audit_metadata`；
- pre-registered selection rule；
- modified/frozen layer 信息；
- label / feature / model / threshold / probe registry version。

## Final Status

`WINDOW_EXTENSION_REVEALS_LEGACY_OVERFIT` 已被记录为 promotion blocker。Dynamic promotion、paper-shadow、production 和 broker 继续 blocked/false/none。
