# TRADING-1706 to 1715 Research Window Adoption Closeout

最后更新：2026-06-28

## 状态

- Task id: `TRADING-1706_to_1715_RESEARCH_WINDOW_ADOPTION_CLOSEOUT`
- Status: `VALIDATING`
- Owner: system implementation + project owner review
- Date opened: 2026-06-28
- Original owner attachment id: `TRADING-1666~1675`
- Renumbering reason: `TRADING-1666_to_1705_UPPER_STATE_LABEL_FEATURE_RESET` already exists in the project register.
- Market regime: `ai_after_chatgpt`
- Safety boundary: research-only, actual-path required, target-path diagnostic-only, dynamic promotion blocked, no paper-shadow, no production, no broker.

## 背景

`TRADING-1646_to_1665_RESEARCH_WINDOW_EXTENSION_VALIDATION` 已经把 QQQ / SGOV / TQQQ research windows 从单一 `2022-12-01` legacy window 升级为：

- `exact_three_asset_validated`: `2021-02-22` 起，primary validated window；
- `exact_three_asset_primary_only_extension`: `2020-05-28` 起，sensitivity only，带 SGOV secondary-source gap caveat；
- `legacy_research_window_2022_12`: `2022-12-01` 起，legacy comparison only；
- `requested_sgov_inception_range`: owner requested `2020-05-26`，metadata-only，actual portfolio start `2020-05-28`。

真实 closeout 状态为 `WINDOW_EXTENSION_REVEALS_LEGACY_OVERFIT_PROMOTION_BLOCKED`。本批目标是把这个结论从一次性验证结果升级为后续研究默认纪律、audit metadata contract 和 owner-readable closeout。

## 任务映射

| ID | Scope |
|---|---|
| TRADING-1706 | Post-window-extension closeout report |
| TRADING-1707 | Legacy result reclassification |
| TRADING-1708 | Primary research window policy |
| TRADING-1709 | Research audit metadata schema |
| TRADING-1710 | Window-aware selection rule templates |
| TRADING-1711 | Research window guardrail tests |
| TRADING-1712 | System flow / catalog / registry / task register updates |
| TRADING-1713 | Owner brief |
| TRADING-1714 | Validation |
| TRADING-1715 | Final closeout, commit and push |

## Expected Artifacts

- `docs/research/research_window_extension_adoption_closeout.md`
- `inputs/research_reviews/research_window_extension_adoption_closeout.yaml`
- `inputs/research_reviews/legacy_window_evidence_reclassification.yaml`
- `docs/research/legacy_window_evidence_reclassification_review.md`
- `config/research/primary_research_window_policy.yaml`
- `config/research/research_audit_metadata_schema.yaml`
- `tests/test_research_audit_metadata.py`
- `config/research/window_aware_selection_rule_templates.yaml`
- `docs/research/window_aware_selection_rule_guidelines.md`
- `docs/research/research_window_extension_owner_brief.md`
- `docs/research/post_window_extension_research_discipline_closeout.md`
- `inputs/research_reviews/post_window_extension_research_discipline_final_matrix.yaml`
- updates to `docs/system_flow.md`, `docs/artifact_catalog.md`, `config/report_registry.yaml`, and `docs/task_register.md`

## Acceptance Criteria

- `2021-02-22` is the adopted primary validated research window.
- `2022-12-01` legacy results are comparison-only and cannot be primary owner-decision or promotion evidence.
- `2020-05-28` extension is sensitivity-only and must carry the SGOV secondary-source caveat.
- Requested `2020-05-26` SGOV inception range remains metadata-only and cannot start portfolio returns before common tradable data.
- Post-window-extension artifacts require window metadata and `research_audit_metadata`.
- `WINDOW_EXTENSION_REVEALS_LEGACY_OVERFIT` is an explicit promotion blocker.
- Dynamic promotion, paper-shadow, production and broker remain blocked/false/none.

## Validation Plan

- `python -m ruff check src tests`
- `python -m compileall -q src tests`
- `python -m pytest -n 16 --dist loadfile tests/test_research_window_contracts.py`
- `python -m pytest -n 16 --dist loadfile tests/test_research_audit_metadata.py`
- `python -m pytest -n 16 --dist loadfile tests/test_research_artifact_governance.py`
- `python -m pytest -n 16 --dist loadfile tests/test_task_register_consistency.py tests/test_report_index.py tests/test_documentation_contract.py`
- `git diff --check`
- `git diff --cached --check`

## Progress Notes

- 2026-06-28: Registered task and requirement document. This batch is an adoption and governance closeout; it does not rerun strategy search, enable promotion, or change broker/production state.
- 2026-06-28: Implementation completed and moved to `VALIDATING`. Added primary research window policy, post-1665 research audit metadata schema, window-aware selection rule templates, adoption closeout, legacy evidence reclassification, owner brief, final discipline closeout, report registry/catalog/system-flow entries, and guardrail tests. Validation passed with Ruff, compileall, focused parallel pytest, artifact governance pytest, documentation contract pytest, and `git diff --check`; promotion, paper-shadow, production, and broker remain blocked/false/none.
