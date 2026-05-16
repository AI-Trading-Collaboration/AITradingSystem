from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Literal, cast

import pandas as pd
import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.shadow.lineage import git_commit_sha, sha256_file
from ai_trading_system.shadow_weight_profiles import (
    DEFAULT_SHADOW_PARAMETER_PROMOTION_CONTRACT_PATH,
    DEFAULT_SHADOW_PARAMETER_SEARCH_OUTPUT_ROOT,
    PRODUCTION_OBSERVED_GATE_PROFILE_ID,
    ShadowParameterPromotionCheck,
    ShadowParameterPromotionContractConfig,
    build_shadow_parameter_promotion_report,
    load_shadow_parameter_promotion_contract,
)

SCHEMA_VERSION = 1
DEFAULT_SHADOW_ITERATION_REGISTRY_PATH = (
    PROJECT_ROOT / "data" / "processed" / "shadow_iteration_registry.csv"
)
DEFAULT_SHADOW_ITERATION_REPORT_DIR = PROJECT_ROOT / "outputs" / "reports"
DEFAULT_SHADOW_ITERATION_RUN_ROOT = PROJECT_ROOT / "outputs" / "shadow_iterations"

CandidateType = Literal["weight_only", "gate_only", "weight_gate_bundle"]
LifecycleStatus = Literal[
    "OBSERVED",
    "CANDIDATE",
    "FORWARD_SHADOW_ACTIVE",
    "BLOCKED",
    "RETIRED",
]

REGISTRY_COLUMNS = (
    "iteration_id",
    "as_of",
    "source_search_run_id",
    "trial_id",
    "candidate_type",
    "status",
    "production_effect",
    "primary_driver",
    "target_weights_json",
    "gate_caps_json",
    "objective_score",
    "excess_return",
    "shadow_return",
    "production_return",
    "max_drawdown_delta",
    "turnover",
    "available_samples",
    "missing_samples",
    "weight_only_excess",
    "gate_only_excess",
    "combined_excess",
    "binding_gate_summary",
    "promotion_status",
    "blocked_reasons_json",
    "next_action",
    "first_seen_at",
    "last_seen_at",
    "active_days",
    "report_path",
    "retirement_evidence_json",
)

_EPSILON = 1e-9
_CAP_ATTRIBUTION_ORDER = (
    "valuation",
    "risk_budget",
    "thesis",
    "confidence",
    "data_confidence",
)
# Pilot retirement policy for shadow-only observation state. These counters are
# registry hygiene, not production promotion criteria.
RETIRE_AFTER_CONSECUTIVE_MISSING_TOP_GROUP = 3
RETIRE_AFTER_CONSECUTIVE_WORSE_RUNS = 3
P2_GUARDRAILS = (
    "do_not_modify_production_weight_profile",
    "do_not_generate_approved_calibration_overlay",
    "do_not_enable_approved_hard",
    "do_not_promote_gate_only_as_weight_candidate",
    "do_not_promote_weight_gate_bundle_directly",
    "do_not_generate_shrinkage_production_proposal",
)


@dataclass(frozen=True)
class ShadowIterationCandidate:
    iteration_id: str
    as_of: date
    source_search_run_id: str
    trial_id: str
    candidate_type: CandidateType
    status: LifecycleStatus
    production_effect: str
    primary_driver: str
    target_weights: dict[str, float]
    gate_caps: dict[str, float]
    changed_weights: dict[str, dict[str, float]]
    objective_score: float | None
    excess_return: float | None
    shadow_return: float | None
    production_return: float | None
    max_drawdown_delta: float | None
    turnover: float | None
    available_samples: int
    missing_samples: int
    weight_only_excess: float | None
    gate_only_excess: float | None
    combined_excess: float | None
    binding_gate_summary: str
    promotion_status: str
    blocked_reasons: tuple[str, ...]
    next_action: str
    first_seen_at: str
    last_seen_at: str
    active_days: int
    report_path: Path
    retirement_evidence: dict[str, Any]
    is_potential_weight_iteration_candidate: bool
    attribution_summary: str

    def to_registry_row(self) -> dict[str, Any]:
        return {
            "iteration_id": self.iteration_id,
            "as_of": self.as_of.isoformat(),
            "source_search_run_id": self.source_search_run_id,
            "trial_id": self.trial_id,
            "candidate_type": self.candidate_type,
            "status": self.status,
            "production_effect": self.production_effect,
            "primary_driver": self.primary_driver,
            "target_weights_json": _json_dumps(self.target_weights),
            "gate_caps_json": _json_dumps(self.gate_caps),
            "objective_score": _blank_if_none(self.objective_score),
            "excess_return": _blank_if_none(self.excess_return),
            "shadow_return": _blank_if_none(self.shadow_return),
            "production_return": _blank_if_none(self.production_return),
            "max_drawdown_delta": _blank_if_none(self.max_drawdown_delta),
            "turnover": _blank_if_none(self.turnover),
            "available_samples": self.available_samples,
            "missing_samples": self.missing_samples,
            "weight_only_excess": _blank_if_none(self.weight_only_excess),
            "gate_only_excess": _blank_if_none(self.gate_only_excess),
            "combined_excess": _blank_if_none(self.combined_excess),
            "binding_gate_summary": self.binding_gate_summary,
            "promotion_status": self.promotion_status,
            "blocked_reasons_json": json.dumps(
                list(self.blocked_reasons),
                ensure_ascii=False,
            ),
            "next_action": self.next_action,
            "first_seen_at": self.first_seen_at,
            "last_seen_at": self.last_seen_at,
            "active_days": self.active_days,
            "report_path": str(self.report_path),
            "retirement_evidence_json": json.dumps(
                self.retirement_evidence,
                ensure_ascii=False,
                sort_keys=True,
            ),
        }

    def to_card(self) -> dict[str, Any]:
        return {
            "trial_id": self.trial_id,
            "iteration_id": self.iteration_id,
            "candidate_type": self.candidate_type,
            "status": self.status,
            "production_effect": self.production_effect,
            "is_potential_weight_iteration_candidate": (
                self.is_potential_weight_iteration_candidate
            ),
            "changed_weights": self.changed_weights,
            "changed_gate_caps": self.gate_caps,
            "objective_score": self.objective_score,
            "excess_return": self.excess_return,
            "shadow_return": self.shadow_return,
            "production_return": self.production_return,
            "max_drawdown_delta": self.max_drawdown_delta,
            "turnover": self.turnover,
            "available_samples": self.available_samples,
            "missing_samples": self.missing_samples,
            "primary_driver": self.primary_driver,
            "attribution_summary": self.attribution_summary,
            "binding_gate_summary": self.binding_gate_summary,
            "promotion_status": self.promotion_status,
            "blocked_reasons": list(self.blocked_reasons),
            "next_action": self.next_action,
        }


@dataclass(frozen=True)
class ShadowIterationLineage:
    source_search_run_id: str
    source_search_output_path: Path
    objective_config: dict[str, Any]
    search_space_config: dict[str, Any]
    promotion_contract: dict[str, Any]
    generated_artifact_paths: dict[str, str]
    git_commit_sha: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_search_run_id": self.source_search_run_id,
            "source_search_output_path": str(self.source_search_output_path),
            "objective_config": self.objective_config,
            "search_space_config": self.search_space_config,
            "promotion_contract": self.promotion_contract,
            "generated_artifact_paths": self.generated_artifact_paths,
            "git_commit_sha": self.git_commit_sha,
        }


@dataclass(frozen=True)
class ShadowIterationReport:
    as_of: date
    generated_at: datetime
    run_id: str
    source_search_run_id: str
    search_output_dir: Path
    registry_path: Path
    report_path: Path
    json_path: Path
    run_output_dir: Path
    production_effect: str
    manifest: dict[str, Any]
    promotion_status: str
    promotion_checks: tuple[ShadowParameterPromotionCheck, ...]
    candidates: tuple[ShadowIterationCandidate, ...]
    retired_rows: tuple[dict[str, Any], ...]
    lineage: ShadowIterationLineage
    warnings: tuple[str, ...]

    @property
    def status(self) -> str:
        if self.warnings:
            return "PASS_WITH_LIMITATIONS"
        return "PASS"

    @property
    def best_weight_only(self) -> ShadowIterationCandidate | None:
        return _candidate_by_type(self.candidates, "weight_only")

    @property
    def best_gate_only(self) -> ShadowIterationCandidate | None:
        return _candidate_by_type(self.candidates, "gate_only")

    @property
    def best_weight_gate_bundle(self) -> ShadowIterationCandidate | None:
        return _candidate_by_type(self.candidates, "weight_gate_bundle")

    @property
    def active_candidates(self) -> tuple[ShadowIterationCandidate, ...]:
        return tuple(
            candidate
            for candidate in self.candidates
            if candidate.status
            in {"OBSERVED", "CANDIDATE", "FORWARD_SHADOW_ACTIVE", "BLOCKED"}
        )

    def to_dashboard_dict(self) -> dict[str, Any]:
        factorial = _factorial_attribution(self.manifest)
        cap_rows = _cap_attribution_rows(self.manifest)
        position_rows = _position_change_rows(self.manifest)
        return {
            "schema_version": SCHEMA_VERSION,
            "report_type": "shadow_iteration",
            "status": self.status,
            "production_effect": self.production_effect,
            "as_of": self.as_of.isoformat(),
            "generated_at": self.generated_at.isoformat(),
            "run_id": self.run_id,
            "source_search_run_id": self.source_search_run_id,
            "summary": {
                "conclusion": _overall_conclusion(self),
                "production_parameters_changed": False,
                "active_candidate_count": len(self.active_candidates),
                "primary_driver": _primary_driver(self.manifest),
                "next_action": _overall_next_action(self),
            },
            "best_candidates": {
                "weight_only": _card_or_none(self.best_weight_only),
                "gate_only": _card_or_none(self.best_gate_only),
                "weight_gate_bundle": _card_or_none(self.best_weight_gate_bundle),
            },
            "active_candidates": [candidate.to_card() for candidate in self.active_candidates],
            "promotion_contract_check": {
                "status": self.promotion_status,
                "checks": [check.to_dict() for check in self.promotion_checks],
            },
            "blocked_reasons": _blocked_reason_index(self.candidates),
            "attribution": {
                "factorial": factorial
                if factorial is not None
                else {"status": "unavailable"},
                "cap_level": {
                    "status": "available" if cap_rows else "unavailable",
                    "rows": cap_rows,
                },
                "position_change": {
                    "status": "available" if position_rows else "unavailable",
                    "rows": position_rows,
                },
            },
            "lineage": self.lineage.to_dict(),
            "warnings": list(self.warnings),
            "safety": {
                "production_parameters_changed": False,
                "forbidden_write_surfaces": [
                    "config/weights/weight_profile_current.yaml",
                    "config/scoring_rules.yaml",
                    "config/portfolio.yaml",
                    "approved calibration overlay",
                    "data/processed/prediction_ledger.csv",
                ],
                "p2_guardrails": list(P2_GUARDRAILS),
            },
        }


def default_shadow_iteration_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"shadow_iteration_{as_of.isoformat()}.md"


def default_shadow_iteration_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"shadow_iteration_{as_of.isoformat()}.json"


def build_shadow_iteration_report(
    *,
    as_of: date,
    search_output_dir: Path | None = None,
    search_output_root: Path = DEFAULT_SHADOW_PARAMETER_SEARCH_OUTPUT_ROOT,
    registry_path: Path = DEFAULT_SHADOW_ITERATION_REGISTRY_PATH,
    reports_dir: Path = DEFAULT_SHADOW_ITERATION_REPORT_DIR,
    run_output_root: Path = DEFAULT_SHADOW_ITERATION_RUN_ROOT,
    contract_path: Path = DEFAULT_SHADOW_PARAMETER_PROMOTION_CONTRACT_PATH,
    generated_at: datetime | None = None,
) -> ShadowIterationReport:
    effective_generated_at = generated_at or datetime.now(tz=UTC)
    effective_search_output_dir = search_output_dir or find_latest_search_output_dir(
        search_output_root,
    )
    manifest_path = effective_search_output_dir / "manifest.json"
    trials_path = effective_search_output_dir / "trials.csv"
    manifest = _read_json_object(manifest_path)
    trials = _read_trials(trials_path)
    source_search_run_id = str(manifest.get("run_id") or effective_search_output_dir.name)
    run_id = _shadow_iteration_run_id(as_of, source_search_run_id)
    report_path = default_shadow_iteration_report_path(reports_dir, as_of)
    json_path = default_shadow_iteration_json_path(reports_dir, as_of)
    run_output_dir = run_output_root / run_id
    contract = load_shadow_parameter_promotion_contract(contract_path)
    promotion_report = build_shadow_parameter_promotion_report(
        search_output_dir=effective_search_output_dir,
        contract_path=contract_path,
        generated_at=effective_generated_at,
    )
    existing_rows = _load_registry_rows(registry_path)
    existing_by_id = {
        str(row.get("iteration_id") or ""): row
        for row in existing_rows
        if row.get("iteration_id")
    }
    top_trials = _top_trials_by_candidate_type(trials)
    source_weights = _source_weights(trials)
    candidates = tuple(
        _candidate_from_trial(
            row=row,
            candidate_type=candidate_type,
            as_of=as_of,
            source_search_run_id=source_search_run_id,
            source_weights=source_weights,
            manifest=manifest,
            contract=contract,
            existing_row=existing_by_id.get(
                _iteration_id(source_search_run_id, str(row.get("trial_id") or ""))
            ),
            report_path=report_path,
            generated_at=effective_generated_at,
        )
        for candidate_type, row in top_trials.items()
    )
    current_ids = {candidate.iteration_id for candidate in candidates}
    retired_rows = _retire_missing_rows(
        existing_rows=existing_rows,
        current_ids=current_ids,
        as_of=as_of,
        generated_at=effective_generated_at,
    )
    lineage = _build_lineage(
        manifest=manifest,
        source_search_run_id=source_search_run_id,
        search_output_dir=effective_search_output_dir,
        contract_path=contract_path,
        report_path=report_path,
        json_path=json_path,
        registry_path=registry_path,
        run_output_dir=run_output_dir,
    )
    warnings = _build_warnings(candidates, manifest)
    return ShadowIterationReport(
        as_of=as_of,
        generated_at=effective_generated_at,
        run_id=run_id,
        source_search_run_id=source_search_run_id,
        search_output_dir=effective_search_output_dir,
        registry_path=registry_path,
        report_path=report_path,
        json_path=json_path,
        run_output_dir=run_output_dir,
        production_effect="none",
        manifest=manifest,
        promotion_status=promotion_report.status,
        promotion_checks=promotion_report.checks,
        candidates=candidates,
        retired_rows=tuple(retired_rows),
        lineage=lineage,
        warnings=tuple(warnings),
    )


def write_shadow_iteration_outputs(report: ShadowIterationReport) -> dict[str, Path]:
    report.report_path.parent.mkdir(parents=True, exist_ok=True)
    report.json_path.parent.mkdir(parents=True, exist_ok=True)
    report.run_output_dir.mkdir(parents=True, exist_ok=True)
    registry_rows = [*report.retired_rows]
    registry_rows.extend(candidate.to_registry_row() for candidate in report.candidates)
    _write_registry_rows(report.registry_path, registry_rows)
    report.report_path.write_text(render_shadow_iteration_report(report), encoding="utf-8")
    report.json_path.write_text(
        json.dumps(report.to_dashboard_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    cards_path = report.run_output_dir / "trial_cards.json"
    lineage_path = report.run_output_dir / "lineage.json"
    summary_path = report.run_output_dir / "summary.json"
    cards_path.write_text(
        json.dumps(
            [candidate.to_card() for candidate in report.candidates],
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    lineage_path.write_text(
        json.dumps(report.lineage.to_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    summary_path.write_text(
        json.dumps(
            {
                "schema_version": SCHEMA_VERSION,
                "report_type": "shadow_iteration_run_summary",
                "production_effect": "none",
                "run_id": report.run_id,
                "as_of": report.as_of.isoformat(),
                "status": report.status,
                "source_search_run_id": report.source_search_run_id,
                "candidate_count": len(report.candidates),
                "active_candidate_count": len(report.active_candidates),
                "report_path": str(report.report_path),
                "json_path": str(report.json_path),
                "registry_path": str(report.registry_path),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return {
        "registry": report.registry_path,
        "markdown_report": report.report_path,
        "json_report": report.json_path,
        "run_output_dir": report.run_output_dir,
        "trial_cards": cards_path,
        "lineage": lineage_path,
        "summary": summary_path,
    }


def register_forward_shadow_candidate(
    *,
    registry_path: Path = DEFAULT_SHADOW_ITERATION_REGISTRY_PATH,
    iteration_id: str,
    candidate_id: str,
    as_of: date,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    rows = _load_registry_rows(registry_path)
    if not rows:
        raise FileNotFoundError(f"shadow iteration registry not found or empty: {registry_path}")
    matched_index: int | None = None
    for index, row in enumerate(rows):
        if str(row.get("iteration_id") or "") == iteration_id:
            matched_index = index
            break
    if matched_index is None:
        raise ValueError(f"iteration_id not found in shadow iteration registry: {iteration_id}")
    row = _normalize_registry_row(rows[matched_index])
    trial_id = str(row.get("trial_id") or "")
    if candidate_id not in {trial_id, iteration_id}:
        raise ValueError(
            "candidate-id must match registry trial_id or iteration_id: "
            f"{candidate_id}"
        )
    now = generated_at or datetime.now(tz=UTC)
    reasons = _json_list(row.get("blocked_reasons_json"))
    reasons.append("registered_forward_shadow: observation-only registry status update")
    row["status"] = "FORWARD_SHADOW_ACTIVE"
    row["production_effect"] = "none"
    row["promotion_status"] = _forward_shadow_promotion_status(row)
    row["next_action"] = "已登记持续 forward shadow 观察；不得修改 production 参数。"
    row["last_seen_at"] = now.isoformat()
    if not str(row.get("first_seen_at") or ""):
        row["first_seen_at"] = now.isoformat()
    row["active_days"] = _active_days(str(row.get("first_seen_at") or ""), as_of)
    row["blocked_reasons_json"] = json.dumps(
        list(dict.fromkeys(reasons)),
        ensure_ascii=False,
    )
    rows[matched_index] = row
    _write_registry_rows(registry_path, rows)
    return row


def find_latest_search_output_dir(output_root: Path) -> Path:
    if not output_root.exists():
        raise FileNotFoundError(f"shadow parameter search output root not found: {output_root}")
    candidates = [
        path
        for path in output_root.iterdir()
        if path.is_dir() and (path / "manifest.json").exists() and (path / "trials.csv").exists()
    ]
    if not candidates:
        raise FileNotFoundError(
            f"no shadow parameter search output directories found under {output_root}"
        )
    return max(candidates, key=_search_output_sort_key)


def render_shadow_iteration_report(report: ShadowIterationReport) -> str:
    lines = [
        "# Shadow Parameter Iteration 报告",
        "",
        f"- 状态：{report.status}",
        "- production_effect：none",
        f"- As of：{report.as_of.isoformat()}",
        f"- Run ID：`{report.run_id}`",
        f"- Source search run id：`{report.source_search_run_id}`",
        f"- Source search output：`{report.search_output_dir}`",
        "",
        "## 本次结论",
        "",
        f"- {_overall_conclusion(report)}",
        "- Production 参数未改变：本流程没有写入 production weight、production gate、"
        "approved overlay 或正式 prediction ledger。",
        f"- Primary driver：`{_primary_driver(report.manifest)}`",
        f"- Next action：{_overall_next_action(report)}",
        "",
        "## Active Shadow Candidates",
        "",
    ]
    if not report.active_candidates:
        lines.append("- 本次没有可登记的 top shadow candidate。")
    else:
        lines.extend(
            [
                (
                    "| Type | Trial | Status | Potential weight iteration | "
                    "Objective | Excess | Primary driver | Next action |"
                ),
                "|---|---|---|---|---:|---:|---|---|",
            ]
        )
        for candidate in report.active_candidates:
            lines.append(
                "| "
                f"`{candidate.candidate_type}` | "
                f"`{candidate.trial_id}` | "
                f"`{candidate.status}` | "
                f"{'yes' if candidate.is_potential_weight_iteration_candidate else 'no'} | "
                f"{_format_score(candidate.objective_score)} | "
                f"{_format_pct(candidate.excess_return)} | "
                f"`{candidate.primary_driver}` | "
                f"{_escape_markdown_table(candidate.next_action)} |"
            )
    lines.extend(
        [
            "",
            "## Best Candidates",
            "",
            _best_candidate_line("best weight-only candidate", report.best_weight_only),
            _best_candidate_line("best gate-only candidate", report.best_gate_only),
            _best_candidate_line(
                "best weight-gate bundle candidate",
                report.best_weight_gate_bundle,
            ),
            "",
            "## Promotion Contract Check",
            "",
            f"- Promotion status：`{report.promotion_status}`",
            "- Contract 只用于状态标记和 blocked reason 输出，不触发 production mutation。",
            "",
            "| Check | Status | Evidence | Reason |",
            "|---|---|---|---|",
        ]
    )
    for check in report.promotion_checks:
        lines.append(
            "| "
            f"`{check.check_id}` | "
            f"`{check.status}` | "
            f"{_escape_markdown_table(check.evidence_ref)} | "
            f"{_escape_markdown_table(check.reason)} |"
        )
    lines.extend(["", "## Blocked Reasons", ""])
    blocked = _blocked_reason_index(report.candidates)
    if not blocked:
        lines.append("- 无。")
    else:
        for trial_id, reasons in blocked.items():
            lines.append(f"- `{trial_id}`：{'；'.join(reasons)}")
    lines.extend(["", "## Trial Cards", ""])
    if not report.candidates:
        lines.append("- 无 Trial Card。")
    else:
        for candidate in report.candidates:
            lines.extend(_render_trial_card(candidate))
    lines.extend(["", "## Attribution", ""])
    lines.extend(_render_factorial_attribution(report.manifest))
    lines.extend(["", "### Cap-Level Attribution", ""])
    lines.extend(_render_cap_attribution(report.manifest))
    lines.extend(["", "## Position Change Explanation", ""])
    lines.extend(_render_position_changes(report.manifest))
    lines.extend(["", "## Data Lineage", ""])
    lineage = report.lineage.to_dict()
    lines.extend(
        [
            f"- source_search_run_id：`{lineage['source_search_run_id']}`",
            f"- source search output path：`{lineage['source_search_output_path']}`",
            (
                "- objective config："
                f"`{_lineage_path(lineage['objective_config'])}`，"
                f"version=`{_lineage_version(lineage['objective_config'])}`，"
                f"checksum=`{_lineage_checksum(lineage['objective_config'])}`"
            ),
            (
                "- search space config："
                f"`{_lineage_path(lineage['search_space_config'])}`，"
                f"version=`{_lineage_version(lineage['search_space_config'])}`，"
                f"checksum=`{_lineage_checksum(lineage['search_space_config'])}`"
            ),
            (
                "- promotion contract："
                f"`{_lineage_path(lineage['promotion_contract'])}`，"
                f"version=`{_lineage_version(lineage['promotion_contract'])}`，"
                f"checksum=`{_lineage_checksum(lineage['promotion_contract'])}`"
            ),
            f"- git commit sha：`{lineage['git_commit_sha'] or 'unknown'}`",
            "",
            "| Artifact | Path |",
            "|---|---|",
        ]
    )
    for artifact, path in lineage["generated_artifact_paths"].items():
        lines.append(f"| `{artifact}` | `{_escape_markdown_table(path)}` |")
    lines.extend(["", "## P2 Guardrails", ""])
    lines.extend(f"- `{guardrail}`" for guardrail in P2_GUARDRAILS)
    lines.extend(["", "## Warnings", ""])
    if report.warnings:
        lines.extend(f"- {warning}" for warning in report.warnings)
    else:
        lines.append("- 无。")
    return "\n".join(lines).rstrip() + "\n"


def _read_json_object(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"JSON file not found: {path}")
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"JSON file must contain an object: {path}")
    return raw


def _read_trials(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"shadow parameter trials CSV not found: {path}")
    frame = pd.read_csv(path, keep_default_na=False)
    required = {
        "trial_id",
        "weight_candidate_id",
        "gate_candidate_id",
        "available_count",
        "missing_count",
        "production_total_return",
        "shadow_total_return",
        "excess_total_return",
        "production_max_drawdown",
        "shadow_max_drawdown",
        "shadow_turnover",
        "objective_score",
        "eligible",
        "ineligibility_reason",
        "weight_l1_distance_from_production",
        "target_weights_json",
        "gate_cap_overrides_json",
    }
    missing = required - set(frame.columns)
    if missing:
        raise ValueError("trials.csv missing columns: " + ", ".join(sorted(missing)))
    return frame


def _top_trials_by_candidate_type(
    trials: pd.DataFrame,
) -> dict[CandidateType, Mapping[str, Any]]:
    groups: dict[CandidateType, list[Mapping[str, Any]]] = {
        "weight_only": [],
        "gate_only": [],
        "weight_gate_bundle": [],
    }
    trial_rows = cast(list[dict[str, Any]], trials.to_dict(orient="records"))
    for row in trial_rows:
        candidate_type = _candidate_type(row)
        if candidate_type is None:
            continue
        groups[candidate_type].append(row)
    result: dict[CandidateType, Mapping[str, Any]] = {}
    for candidate_type, rows in groups.items():
        if rows:
            result[candidate_type] = max(rows, key=_candidate_sort_key)
    return result


def _candidate_type(row: Mapping[str, Any]) -> CandidateType | None:
    weights_changed = (
        _float_or_none(row.get("weight_l1_distance_from_production")) or 0.0
    ) > _EPSILON
    gate_caps = _json_mapping(row.get("gate_cap_overrides_json"))
    gates_changed = (
        bool(gate_caps)
        and str(row.get("gate_candidate_id") or "") != PRODUCTION_OBSERVED_GATE_PROFILE_ID
    )
    if weights_changed and gates_changed:
        return "weight_gate_bundle"
    if weights_changed:
        return "weight_only"
    if gates_changed:
        return "gate_only"
    return None


def _candidate_from_trial(
    *,
    row: Mapping[str, Any],
    candidate_type: CandidateType,
    as_of: date,
    source_search_run_id: str,
    source_weights: Mapping[str, float],
    manifest: Mapping[str, Any],
    contract: ShadowParameterPromotionContractConfig,
    existing_row: Mapping[str, Any] | None,
    report_path: Path,
    generated_at: datetime,
) -> ShadowIterationCandidate:
    trial_id = str(row.get("trial_id") or "")
    iteration_id = _iteration_id(source_search_run_id, trial_id)
    target_weights = _json_float_mapping(row.get("target_weights_json"))
    gate_caps = _json_float_mapping(row.get("gate_cap_overrides_json"))
    changed_weights = _changed_weights(source_weights, target_weights)
    primary_driver = _primary_driver(manifest)
    blocked_reasons, critical_reasons = _candidate_blocked_reasons(
        row=row,
        candidate_type=candidate_type,
        primary_driver=primary_driver,
        contract=contract,
    )
    retirement_evidence = _candidate_retirement_evidence(
        row=row,
        primary_driver=primary_driver,
        existing_row=existing_row,
        critical_reasons=critical_reasons,
    )
    retirement_reasons = _retirement_reasons(retirement_evidence)
    blocked_reasons.extend(retirement_reasons)
    previous_status = str((existing_row or {}).get("status") or "")
    first_seen_at = str((existing_row or {}).get("first_seen_at") or "")
    if not first_seen_at:
        first_seen_at = generated_at.isoformat()
    last_seen_at = generated_at.isoformat()
    status = _candidate_status(
        candidate_type=candidate_type,
        previous_status=previous_status,
        has_critical_reasons=bool(critical_reasons),
        has_retirement_reasons=bool(retirement_reasons),
        blocked_reasons=blocked_reasons,
    )
    potential_weight_candidate = (
        candidate_type == "weight_only"
        and not critical_reasons
        and primary_driver == "weight"
    )
    promotion_status = _candidate_promotion_status(
        candidate_type=candidate_type,
        potential_weight_candidate=potential_weight_candidate,
        has_critical_reasons=bool(critical_reasons),
        blocked_reasons=blocked_reasons,
    )
    next_action = _candidate_next_action(
        candidate_type=candidate_type,
        status=status,
        potential_weight_candidate=potential_weight_candidate,
    )
    factorial = _factorial_attribution(manifest)
    return ShadowIterationCandidate(
        iteration_id=iteration_id,
        as_of=as_of,
        source_search_run_id=source_search_run_id,
        trial_id=trial_id,
        candidate_type=candidate_type,
        status=status,
        production_effect="none",
        primary_driver=primary_driver,
        target_weights=target_weights,
        gate_caps=gate_caps,
        changed_weights=changed_weights,
        objective_score=_float_or_none(row.get("objective_score")),
        excess_return=_float_or_none(row.get("excess_total_return")),
        shadow_return=_float_or_none(row.get("shadow_total_return")),
        production_return=_float_or_none(row.get("production_total_return")),
        max_drawdown_delta=_max_drawdown_delta(row),
        turnover=_float_or_none(row.get("shadow_turnover")),
        available_samples=_int_or_zero(row.get("available_count")),
        missing_samples=_int_or_zero(row.get("missing_count")),
        weight_only_excess=_factorial_value(factorial, "weight_only_excess_delta"),
        gate_only_excess=_factorial_value(factorial, "gate_only_excess_delta"),
        combined_excess=_factorial_value(factorial, "combined_excess_delta"),
        binding_gate_summary=_binding_gate_summary(manifest, trial_id),
        promotion_status=promotion_status,
        blocked_reasons=tuple(blocked_reasons),
        next_action=next_action,
        first_seen_at=first_seen_at,
        last_seen_at=last_seen_at,
        active_days=_active_days(first_seen_at, as_of),
        report_path=report_path,
        retirement_evidence=retirement_evidence,
        is_potential_weight_iteration_candidate=potential_weight_candidate,
        attribution_summary=_attribution_summary(manifest),
    )


def _candidate_blocked_reasons(
    *,
    row: Mapping[str, Any],
    candidate_type: CandidateType,
    primary_driver: str,
    contract: ShadowParameterPromotionContractConfig,
) -> tuple[list[str], list[str]]:
    reasons: list[str] = []
    critical: list[str] = []
    if candidate_type == "gate_only":
        reasons.append("not_weight_promotion_candidate: gate_only 只能进入 gate policy review")
        if primary_driver != "gate":
            reason = f"primary_driver_mismatch: expected gate, actual {primary_driver}"
            reasons.append(reason)
            critical.append(reason)
    elif candidate_type == "weight_gate_bundle":
        reasons.append(
            "not_weight_promotion_candidate: weight_gate_bundle 只能作为 diagnostic"
        )
    elif primary_driver != "weight":
        reason = f"primary_driver_mismatch: expected weight, actual {primary_driver}"
        reasons.append(reason)
        critical.append(reason)

    eligible = _bool_value(row.get("eligible"))
    ineligibility = str(row.get("ineligibility_reason") or "not_eligible")
    if contract.require_search_eligible_best and not eligible:
        reasons.append(f"not_objective_eligible: {ineligibility}")

    available = _int_or_zero(row.get("available_count"))
    if available < contract.min_available_samples:
        reasons.append(
            f"available_samples_below_contract_floor: {available} < "
            f"{contract.min_available_samples}"
        )

    excess = _float_or_none(row.get("excess_total_return"))
    if contract.require_positive_excess and (excess is None or excess <= 0.0):
        reason = "non_positive_excess_return"
        reasons.append(reason)
        critical.append(reason)

    drawdown_delta = _max_drawdown_delta(row)
    if (
        contract.max_drawdown_degradation is not None
        and drawdown_delta is not None
        and drawdown_delta > contract.max_drawdown_degradation + _EPSILON
    ):
        reason = (
            "drawdown_degradation_above_contract_limit: "
            f"{drawdown_delta:.6f} > {contract.max_drawdown_degradation:.6f}"
        )
        reasons.append(reason)
        critical.append(reason)

    turnover = _float_or_none(row.get("shadow_turnover"))
    if (
        contract.max_shadow_turnover is not None
        and turnover is not None
        and turnover > contract.max_shadow_turnover + _EPSILON
    ):
        reason = (
            "shadow_turnover_above_contract_limit: "
            f"{turnover:.6f} > {contract.max_shadow_turnover:.6f}"
        )
        reasons.append(reason)
        critical.append(reason)

    if contract.required_forward_shadow_available_samples > 0:
        reasons.append(
            "forward_shadow_outcome_missing: contract requires available>="
            f"{contract.required_forward_shadow_available_samples}"
        )
    if contract.owner_approval_required:
        reasons.append("owner_approval_required")
    if contract.rollback_condition_required:
        reasons.append("rollback_condition_required")
    return reasons, critical


def _candidate_status(
    *,
    candidate_type: CandidateType,
    previous_status: str,
    has_critical_reasons: bool,
    has_retirement_reasons: bool,
    blocked_reasons: list[str],
) -> LifecycleStatus:
    if has_retirement_reasons:
        return "RETIRED"
    if previous_status == "FORWARD_SHADOW_ACTIVE":
        return "FORWARD_SHADOW_ACTIVE"
    if has_critical_reasons:
        return "BLOCKED"
    if not previous_status:
        return "OBSERVED"
    if candidate_type == "weight_only" and blocked_reasons:
        return "CANDIDATE"
    if candidate_type == "gate_only":
        return "CANDIDATE"
    return "OBSERVED"


def _candidate_retirement_evidence(
    *,
    row: Mapping[str, Any],
    primary_driver: str,
    existing_row: Mapping[str, Any] | None,
    critical_reasons: list[str],
) -> dict[str, Any]:
    previous = _json_object((existing_row or {}).get("retirement_evidence_json"))
    previous_excess = _float_or_none((existing_row or {}).get("excess_return"))
    current_excess = _float_or_none(row.get("excess_total_return"))
    worse_count = 0
    if previous_excess is not None and current_excess is not None:
        prior_count = _int_or_zero(previous.get("worse_performance_count"))
        worse_count = prior_count + 1 if current_excess < previous_excess - _EPSILON else 0
    previous_driver = str((existing_row or {}).get("primary_driver") or "")
    return {
        "missing_top_group_count": 0,
        "worse_performance_count": worse_count,
        "previous_excess_return": previous_excess,
        "current_excess_return": current_excess,
        "previous_primary_driver": previous_driver or None,
        "current_primary_driver": primary_driver,
        "critical_contract_reasons": critical_reasons,
        "policy": {
            "retire_after_consecutive_missing_top_group": (
                RETIRE_AFTER_CONSECUTIVE_MISSING_TOP_GROUP
            ),
            "retire_after_consecutive_worse_runs": (
                RETIRE_AFTER_CONSECUTIVE_WORSE_RUNS
            ),
        },
    }


def _retirement_reasons(evidence: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    if (
        _int_or_zero(evidence.get("missing_top_group_count"))
        >= RETIRE_AFTER_CONSECUTIVE_MISSING_TOP_GROUP
    ):
        reasons.append(
            "retired: consecutive missing top group count reached policy threshold"
        )
    if (
        _int_or_zero(evidence.get("worse_performance_count"))
        >= RETIRE_AFTER_CONSECUTIVE_WORSE_RUNS
    ):
        reasons.append(
            "retired: consecutive worse performance count reached policy threshold"
        )
    previous_driver = str(evidence.get("previous_primary_driver") or "")
    current_driver = str(evidence.get("current_primary_driver") or "")
    if previous_driver == "weight" and current_driver == "gate":
        reasons.append("retired: primary_driver changed from weight to gate")
    critical = evidence.get("critical_contract_reasons")
    if isinstance(critical, list) and any(
        isinstance(reason, str)
        and (
            reason.startswith("drawdown_degradation_above_contract_limit")
            or reason.startswith("shadow_turnover_above_contract_limit")
        )
        for reason in critical
    ):
        reasons.append("retired: drawdown or turnover violates promotion contract")
    return reasons


def _candidate_promotion_status(
    *,
    candidate_type: CandidateType,
    potential_weight_candidate: bool,
    has_critical_reasons: bool,
    blocked_reasons: list[str],
) -> str:
    if candidate_type == "gate_only":
        return "GATE_POLICY_REVIEW_ONLY"
    if candidate_type == "weight_gate_bundle":
        return "DIAGNOSTIC_ONLY"
    if any(reason.startswith("retired:") for reason in blocked_reasons):
        return "RETIRED"
    if has_critical_reasons:
        return "BLOCKED"
    if potential_weight_candidate:
        return "POTENTIAL_WEIGHT_ITERATION_CANDIDATE"
    if blocked_reasons:
        return "CONTRACT_PENDING"
    return "OBSERVED"


def _forward_shadow_promotion_status(row: Mapping[str, Any]) -> str:
    current = str(row.get("promotion_status") or "")
    if current in {"GATE_POLICY_REVIEW_ONLY", "DIAGNOSTIC_ONLY", "BLOCKED"}:
        return current
    return "FORWARD_SHADOW_ACTIVE"


def _candidate_next_action(
    *,
    candidate_type: CandidateType,
    status: LifecycleStatus,
    potential_weight_candidate: bool,
) -> str:
    if status == "FORWARD_SHADOW_ACTIVE":
        return "继续前向 shadow 观察；不修改 production 参数。"
    if status == "RETIRED":
        return "候选已按 shadow-only retirement 规则退出观察；不修改 production。"
    if status == "BLOCKED":
        return "不进入权重迭代；先处理 blocked reasons。"
    if candidate_type == "gate_only":
        return "进入 gate policy review；不得作为权重晋级候选。"
    if candidate_type == "weight_gate_bundle":
        return "仅用于 diagnostic；拆分 weight/gate 影响后再评估。"
    if potential_weight_candidate:
        return "登记为潜在权重迭代观察对象；继续 forward shadow。"
    return "继续观察；promotion contract 只输出阻断原因，不改 production。"


def _retire_missing_rows(
    *,
    existing_rows: list[dict[str, Any]],
    current_ids: set[str],
    as_of: date,
    generated_at: datetime,
) -> list[dict[str, Any]]:
    retired_rows: list[dict[str, Any]] = []
    for row in existing_rows:
        iteration_id = str(row.get("iteration_id") or "")
        if iteration_id in current_ids:
            continue
        updated = _normalize_registry_row(row)
        evidence = _json_object(updated.get("retirement_evidence_json"))
        missing_count = _int_or_zero(evidence.get("missing_top_group_count")) + 1
        evidence["missing_top_group_count"] = missing_count
        evidence.setdefault(
            "policy",
            {
                "retire_after_consecutive_missing_top_group": (
                    RETIRE_AFTER_CONSECUTIVE_MISSING_TOP_GROUP
                ),
                "retire_after_consecutive_worse_runs": (
                    RETIRE_AFTER_CONSECUTIVE_WORSE_RUNS
                ),
            },
        )
        reasons = _json_list(updated.get("blocked_reasons_json"))
        reasons.append(
            "missing_top_group_observation: "
            f"{missing_count}/{RETIRE_AFTER_CONSECUTIVE_MISSING_TOP_GROUP}"
        )
        retirement_reasons = _retirement_reasons(evidence)
        reasons.extend(retirement_reasons)
        if retirement_reasons:
            updated["status"] = "RETIRED"
            updated["next_action"] = "连续不在 top group，按 shadow-only 规则退休。"
        elif updated.get("status") != "RETIRED":
            updated["next_action"] = (
                "本次不在 top group；继续观察是否达到连续缺席退休阈值。"
            )
        updated["last_seen_at"] = generated_at.isoformat()
        updated["blocked_reasons_json"] = json.dumps(
            list(dict.fromkeys(reasons)),
            ensure_ascii=False,
        )
        updated["retirement_evidence_json"] = json.dumps(
            evidence,
            ensure_ascii=False,
            sort_keys=True,
        )
        updated["as_of"] = as_of.isoformat()
        updated["active_days"] = _active_days(str(updated.get("first_seen_at") or ""), as_of)
        retired_rows.append(updated)
    return retired_rows


def _build_lineage(
    *,
    manifest: Mapping[str, Any],
    source_search_run_id: str,
    search_output_dir: Path,
    contract_path: Path,
    report_path: Path,
    json_path: Path,
    registry_path: Path,
    run_output_dir: Path,
) -> ShadowIterationLineage:
    return ShadowIterationLineage(
        source_search_run_id=source_search_run_id,
        source_search_output_path=search_output_dir,
        objective_config=_config_lineage(
            manifest,
            path_key="objective_path",
            checksum_key="objective_checksum",
        ),
        search_space_config=_config_lineage(
            manifest,
            path_key="search_space_path",
            checksum_key="search_space_checksum",
        ),
        promotion_contract=_contract_lineage(contract_path),
        generated_artifact_paths={
            "registry": str(registry_path),
            "markdown_report": str(report_path),
            "json_report": str(json_path),
            "run_output_dir": str(run_output_dir),
            "trial_cards": str(run_output_dir / "trial_cards.json"),
            "lineage": str(run_output_dir / "lineage.json"),
            "summary": str(run_output_dir / "summary.json"),
        },
        git_commit_sha=str(manifest.get("git_commit_sha") or "") or git_commit_sha(),
    )


def _config_lineage(
    manifest: Mapping[str, Any],
    *,
    path_key: str,
    checksum_key: str,
) -> dict[str, Any]:
    path_value = str(manifest.get(path_key) or "")
    path = Path(path_value) if path_value else None
    version = None
    checksum = str(manifest.get(checksum_key) or "")
    if path is not None and path.exists():
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            version = raw.get("version")
        if not checksum:
            checksum = sha256_file(path)
    return {
        "path": path_value or None,
        "version": version,
        "checksum": checksum or None,
    }


def _contract_lineage(contract_path: Path) -> dict[str, Any]:
    contract = load_shadow_parameter_promotion_contract(contract_path)
    return {
        "path": str(contract_path),
        "version": contract.version,
        "checksum": sha256_file(contract_path) if contract_path.exists() else None,
    }


def _load_registry_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    for column in REGISTRY_COLUMNS:
        if column not in frame.columns:
            frame[column] = ""
    rows = cast(
        list[dict[str, Any]],
        frame.loc[:, list(REGISTRY_COLUMNS)].to_dict(orient="records"),
    )
    return [
        _normalize_registry_row(row)
        for row in rows
    ]


def _write_registry_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    normalized = [_normalize_registry_row(row) for row in rows]
    frame = pd.DataFrame(normalized, columns=list(REGISTRY_COLUMNS))
    frame.to_csv(path, index=False)


def _normalize_registry_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        column: "" if row.get(column) is None else row.get(column)
        for column in REGISTRY_COLUMNS
    }


def _source_weights(trials: pd.DataFrame) -> dict[str, float]:
    for row in trials.to_dict(orient="records"):
        if str(row.get("weight_candidate_id") or "") == "source_current":
            return _json_float_mapping(row.get("target_weights_json"))
    first = trials.iloc[0].to_dict() if not trials.empty else {}
    return _json_float_mapping(first.get("target_weights_json"))


def _changed_weights(
    source_weights: Mapping[str, float],
    target_weights: Mapping[str, float],
) -> dict[str, dict[str, float]]:
    changed: dict[str, dict[str, float]] = {}
    for key in sorted(set(source_weights) | set(target_weights)):
        source = float(source_weights.get(key, 0.0))
        target = float(target_weights.get(key, 0.0))
        if abs(target - source) > _EPSILON:
            changed[key] = {
                "production": source,
                "shadow": target,
                "delta": target - source,
            }
    return changed


def _binding_gate_summary(manifest: Mapping[str, Any], trial_id: str) -> str:
    factorial = _factorial_attribution(manifest)
    if not factorial or factorial.get("selected_trial_id") != trial_id:
        return "unavailable: position change data is only available for selected trial"
    rows = _position_change_rows(manifest)
    if not rows:
        return "unavailable"
    production: dict[str, int] = {}
    shadow: dict[str, int] = {}
    for row in rows:
        for key, target in (
            ("production_binding_gates", production),
            ("candidate_binding_gates", shadow),
        ):
            value = str(row.get(key) or "")
            for item in [part.strip() for part in value.split(",") if part.strip()]:
                gate_id = item.split(":", 1)[0]
                target[gate_id] = target.get(gate_id, 0) + 1
    return (
        "production="
        f"{_format_gate_counts(production)}; shadow={_format_gate_counts(shadow)}"
    )


def _attribution_summary(manifest: Mapping[str, Any]) -> str:
    factorial = _factorial_attribution(manifest)
    if not factorial:
        return "factorial attribution unavailable"
    weight_only_excess = _format_pct(
        _factorial_value(factorial, "weight_only_excess_delta")
    )
    gate_only_excess = _format_pct(
        _factorial_value(factorial, "gate_only_excess_delta")
    )
    combined_excess = _format_pct(
        _factorial_value(factorial, "combined_excess_delta")
    )
    parts = [
        f"primary_driver={factorial.get('primary_driver') or 'unknown'}",
        f"weight_only_excess={weight_only_excess}",
        f"gate_only_excess={gate_only_excess}",
        f"combined_excess={combined_excess}",
    ]
    cap_rows = _cap_attribution_rows(manifest)
    if cap_rows:
        primary_cap = max(
            cap_rows,
            key=lambda item: abs(_float_or_none(item.get("excess_delta_vs_baseline")) or 0.0),
        )
        parts.append(
            "primary_gate_cap="
            f"{primary_cap.get('gate_id')} "
            f"{_format_pct(_float_or_none(primary_cap.get('excess_delta_vs_baseline')))}"
        )
    return "; ".join(parts)


def _build_warnings(
    candidates: tuple[ShadowIterationCandidate, ...],
    manifest: Mapping[str, Any],
) -> list[str]:
    warnings = [str(item) for item in manifest.get("warnings") or [] if item]
    if not candidates:
        warnings.append("没有可登记的 weight_only/gate_only/weight_gate_bundle top candidate。")
    if not _cap_attribution_rows(manifest):
        warnings.append("cap-level attribution unavailable。")
    if not _position_change_rows(manifest):
        warnings.append("position change data unavailable。")
    return list(dict.fromkeys(warnings))


def _render_trial_card(candidate: ShadowIterationCandidate) -> list[str]:
    reasons = "；".join(candidate.blocked_reasons) if candidate.blocked_reasons else "无"
    return [
        f"### `{candidate.candidate_type}` / `{candidate.trial_id}`",
        "",
        "| 项目 | 内容 |",
        "|---|---|",
        f"| trial_id | `{candidate.trial_id}` |",
        f"| candidate_type | `{candidate.candidate_type}` |",
        f"| status | `{candidate.status}` |",
        "| production_effect | `none` |",
        (
            "| changed weights | "
            f"{_escape_markdown_table(_format_changed_weights(candidate.changed_weights))} |"
        ),
        (
            "| changed gate caps | "
            f"{_escape_markdown_table(_format_gate_caps(candidate.gate_caps))} |"
        ),
        f"| objective_score | {_format_score(candidate.objective_score)} |",
        f"| excess_return | {_format_pct(candidate.excess_return)} |",
        f"| max_drawdown_delta | {_format_pct(candidate.max_drawdown_delta)} |",
        f"| turnover | {_format_number(candidate.turnover)} |",
        f"| primary_driver | `{candidate.primary_driver}` |",
        f"| attribution summary | {_escape_markdown_table(candidate.attribution_summary)} |",
        f"| blocked reasons | {_escape_markdown_table(reasons)} |",
        f"| next action | {_escape_markdown_table(candidate.next_action)} |",
        "",
    ]


def _render_factorial_attribution(manifest: Mapping[str, Any]) -> list[str]:
    factorial = _factorial_attribution(manifest)
    if not factorial:
        return ["### Factorial Attribution", "", "- unavailable"]
    weight_only_excess = _format_pct(
        _factorial_value(factorial, "weight_only_excess_delta")
    )
    gate_only_excess = _format_pct(
        _factorial_value(factorial, "gate_only_excess_delta")
    )
    combined_excess = _format_pct(
        _factorial_value(factorial, "combined_excess_delta")
    )
    interaction_excess = _format_pct(
        _factorial_value(factorial, "interaction_excess_delta")
    )
    return [
        "### Factorial Attribution",
        "",
        f"- primary_driver：`{factorial.get('primary_driver') or 'unknown'}`",
        f"- weight-only excess：{weight_only_excess}",
        f"- gate-only excess：{gate_only_excess}",
        f"- combined excess：{combined_excess}",
        f"- interaction excess：{interaction_excess}",
    ]


def _render_cap_attribution(manifest: Mapping[str, Any]) -> list[str]:
    rows = _cap_attribution_rows(manifest)
    if not rows:
        return ["- unavailable"]
    by_gate = {str(row.get("gate_id") or ""): row for row in rows}
    lines = [
        (
            "| Gate cap | Selected value | Cap-only trial | Excess | "
            "Delta vs baseline | MDD | Turnover |"
        ),
        "|---|---:|---|---:|---:|---:|---:|",
    ]
    ordered_gate_ids = [
        *[gate_id for gate_id in _CAP_ATTRIBUTION_ORDER if gate_id in by_gate],
        *sorted(gate_id for gate_id in by_gate if gate_id not in _CAP_ATTRIBUTION_ORDER),
    ]
    for gate_id in ordered_gate_ids:
        row = by_gate[gate_id]
        lines.append(
            "| "
            f"`{gate_id}` | "
            f"{_format_number(_float_or_none(row.get('selected_cap_value')))} | "
            f"`{row.get('cap_only_trial_id') or 'unknown'}` | "
            f"{_format_pct(_float_or_none(row.get('cap_only_excess_total_return')))} | "
            f"{_format_pct(_float_or_none(row.get('excess_delta_vs_baseline')))} | "
            f"{_format_pct(_float_or_none(row.get('cap_only_shadow_max_drawdown')))} | "
            f"{_format_number(_float_or_none(row.get('cap_only_shadow_turnover')))} |"
        )
    return lines


def _render_position_changes(manifest: Mapping[str, Any]) -> list[str]:
    rows = _position_change_rows(manifest)
    if not rows:
        return ["- unavailable"]
    lines = [
        (
            "| Date | Production final position | Shadow final position | Delta | "
            "Production binding gate | Shadow binding gate | Return impact |"
        ),
        "|---|---:|---:|---:|---|---|---:|",
    ]
    for row in rows[:50]:
        lines.append(
            "| "
            f"{row.get('as_of') or ''} | "
            f"{_format_pct(_float_or_none(row.get('production_position')))} | "
            f"{_format_pct(_float_or_none(row.get('candidate_position')))} | "
            f"{_format_pct(_float_or_none(row.get('position_delta')))} | "
            f"{_escape_markdown_table(str(row.get('production_binding_gates') or ''))} | "
            f"{_escape_markdown_table(str(row.get('candidate_binding_gates') or ''))} | "
            f"{_format_pct(_float_or_none(row.get('return_impact')))} |"
        )
    if len(rows) > 50:
        lines.append(f"- 仅展示前 50 行；完整行数：{len(rows)}。")
    return lines


def _overall_conclusion(report: ShadowIterationReport) -> str:
    best = report.best_weight_only
    if best is not None and best.is_potential_weight_iteration_candidate:
        return (
            f"本次存在潜在 weight-only 观察候选 `{best.trial_id}`，"
            "仍需 forward shadow 和 owner review。"
        )
    gate = report.best_gate_only
    if gate is not None:
        return (
            f"本次最强信号偏向 gate policy review：`{gate.trial_id}`；"
            "不得作为权重晋级候选。"
        )
    if report.best_weight_gate_bundle is not None:
        return "本次只形成 bundle diagnostic 候选；不进入权重晋级。"
    return "本次没有形成可登记 shadow iteration candidate。"


def _overall_next_action(report: ShadowIterationReport) -> str:
    for candidate in report.candidates:
        if candidate.status != "BLOCKED":
            return candidate.next_action
    if report.candidates:
        return report.candidates[0].next_action
    return "检查 search output 是否包含可分类 trial。"


def _best_candidate_line(
    label: str,
    candidate: ShadowIterationCandidate | None,
) -> str:
    if candidate is None:
        return f"- {label}：unavailable"
    return (
        f"- {label}：`{candidate.trial_id}`，status=`{candidate.status}`，"
        f"excess={_format_pct(candidate.excess_return)}，"
        f"next_action={candidate.next_action}"
    )


def _blocked_reason_index(
    candidates: tuple[ShadowIterationCandidate, ...],
) -> dict[str, list[str]]:
    return {
        candidate.trial_id: list(candidate.blocked_reasons)
        for candidate in candidates
        if candidate.blocked_reasons
    }


def _card_or_none(
    candidate: ShadowIterationCandidate | None,
) -> dict[str, Any] | None:
    return None if candidate is None else candidate.to_card()


def _candidate_by_type(
    candidates: tuple[ShadowIterationCandidate, ...],
    candidate_type: CandidateType,
) -> ShadowIterationCandidate | None:
    for candidate in candidates:
        if candidate.candidate_type == candidate_type:
            return candidate
    return None


def _search_output_sort_key(path: Path) -> tuple[str, float]:
    manifest_path = path / "manifest.json"
    generated_at = ""
    try:
        manifest = _read_json_object(manifest_path)
        generated_at = str(manifest.get("generated_at") or "")
    except (OSError, ValueError, json.JSONDecodeError):
        generated_at = ""
    return generated_at, path.stat().st_mtime


def _shadow_iteration_run_id(as_of: date, source_search_run_id: str) -> str:
    safe_source = re.sub(r"[^A-Za-z0-9_.-]+", "_", source_search_run_id).strip("_")
    return f"shadow_iteration_{as_of.isoformat()}_{safe_source or 'unknown'}"


def _iteration_id(source_search_run_id: str, trial_id: str) -> str:
    return f"{source_search_run_id}::{trial_id}"


def _candidate_sort_key(row: Mapping[str, Any]) -> tuple[float, float, int]:
    objective = _float_or_none(row.get("objective_score"))
    excess = _float_or_none(row.get("excess_total_return"))
    available = _int_or_zero(row.get("available_count"))
    return (
        objective if objective is not None else float("-inf"),
        excess if excess is not None else float("-inf"),
        available,
    )


def _primary_driver(manifest: Mapping[str, Any]) -> str:
    factorial = _factorial_attribution(manifest)
    if not factorial:
        return "unknown"
    return str(factorial.get("primary_driver") or "unknown")


def _factorial_attribution(manifest: Mapping[str, Any]) -> dict[str, Any] | None:
    value = manifest.get("factorial_attribution")
    return value if isinstance(value, dict) else None


def _factorial_value(
    factorial: Mapping[str, Any] | None,
    key: str,
) -> float | None:
    if factorial is None:
        return None
    return _float_or_none(factorial.get(key))


def _cap_attribution_rows(manifest: Mapping[str, Any]) -> list[dict[str, Any]]:
    value = manifest.get("cap_attribution")
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _position_change_rows(manifest: Mapping[str, Any]) -> list[dict[str, Any]]:
    value = manifest.get("position_change_rows")
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _json_mapping(value: object) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if value is None:
        return {}
    text = str(value).strip()
    if not text:
        return {}
    raw = json.loads(text)
    if not isinstance(raw, dict):
        return {}
    return dict(raw)


def _json_float_mapping(value: object) -> dict[str, float]:
    return {
        str(key): float(parsed)
        for key, parsed in _json_mapping(value).items()
        if _float_or_none(parsed) is not None
    }


def _json_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    if value is None:
        return []
    text = str(value).strip()
    if not text:
        return []
    try:
        raw = json.loads(text)
    except json.JSONDecodeError:
        return [text]
    if not isinstance(raw, list):
        return [str(raw)]
    return [str(item) for item in raw]


def _json_object(value: object) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if value is None:
        return {}
    text = str(value).strip()
    if not text:
        return {}
    try:
        raw = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return raw if isinstance(raw, dict) else {}


def _json_dumps(value: Mapping[str, Any]) -> str:
    return json.dumps(dict(value), ensure_ascii=False, sort_keys=True)


def _float_or_none(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    if isinstance(value, str | int | float):
        try:
            parsed = float(value)
        except ValueError:
            return None
    else:
        return None
    if parsed != parsed:
        return None
    return parsed


def _int_or_zero(value: object) -> int:
    parsed = _float_or_none(value)
    if parsed is None:
        return 0
    return int(parsed)


def _bool_value(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def _max_drawdown_delta(row: Mapping[str, Any]) -> float | None:
    shadow = _float_or_none(row.get("shadow_max_drawdown"))
    production = _float_or_none(row.get("production_max_drawdown"))
    if shadow is None or production is None:
        return None
    return max(0.0, abs(shadow) - abs(production))


def _active_days(first_seen_at: str, as_of: date) -> int:
    first_date = _date_from_iso(first_seen_at)
    if first_date is None:
        return 1
    return max(1, (as_of - first_date).days + 1)


def _date_from_iso(value: str) -> date | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value).date()
    except ValueError:
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None


def _format_gate_counts(counts: Mapping[str, int]) -> str:
    if not counts:
        return "none"
    return ", ".join(f"{gate}:{count}" for gate, count in sorted(counts.items()))


def _format_changed_weights(value: Mapping[str, Mapping[str, float]]) -> str:
    if not value:
        return "none"
    return "; ".join(
        f"{key}: {row['production']:.2f}->{row['shadow']:.2f} ({row['delta']:+.2f})"
        for key, row in value.items()
    )


def _format_gate_caps(value: Mapping[str, float]) -> str:
    if not value:
        return "none"
    return "; ".join(f"{key}: {cap:.2f}" for key, cap in sorted(value.items()))


def _format_score(value: float | None) -> str:
    return "NA" if value is None else f"{value:.4f}"


def _format_number(value: float | None) -> str:
    return "NA" if value is None else f"{value:.2f}"


def _format_pct(value: float | None) -> str:
    return "NA" if value is None else f"{value:.2%}"


def _blank_if_none(value: object) -> object:
    return "" if value is None else value


def _escape_markdown_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")


def _lineage_path(value: object) -> str:
    return str(value.get("path") if isinstance(value, dict) else "unknown")


def _lineage_version(value: object) -> str:
    if not isinstance(value, dict):
        return "unknown"
    return str(value.get("version") or "unknown")


def _lineage_checksum(value: object) -> str:
    if not isinstance(value, dict):
        return "unknown"
    return str(value.get("checksum") or "unknown")
