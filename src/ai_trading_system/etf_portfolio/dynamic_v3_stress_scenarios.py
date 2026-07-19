from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st

DEFAULT_STRESS_SCENARIO_LIBRARY_CONFIG_PATH = (
    st.PROJECT_ROOT
    / "config"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "stress_scenario_library_v1.yaml"
)
DEFAULT_STRESS_SCENARIO_LIBRARY_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "stress_scenario_library"
)
STRESS_SCENARIO_LIBRARY_INPUT_SCHEMA = "stress_scenario_library_input_snapshot.v2"
REQUIRED_STRESS_SCENARIO_IDS = (
    "rapid_drawdown",
    "slow_drawdown",
    "v_shaped_recovery",
    "high_volatility_sideways_market",
    "false_risk_off_cluster",
    "rate_shock",
    "ai_sector_correction",
    "semiconductor_led_selloff",
    "liquidity_squeeze",
)
STRESS_SCENARIO_LIBRARY_SAFETY = {
    **st.SYSTEM_TARGET_SAFETY,
    "manual_review_only": True,
    "stress_scenario_library_only": True,
    "candidate_validation_only": True,
    "data_downloaded_by_library": False,
    "pipelines_executed_by_library": False,
    "not_probability_forecast": True,
}


def build_stress_scenario_library(
    *,
    config_path: Path = DEFAULT_STRESS_SCENARIO_LIBRARY_CONFIG_PATH,
    output_dir: Path = DEFAULT_STRESS_SCENARIO_LIBRARY_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    config = st._load_yaml_mapping(config_path)
    normalized = _normalized_stress_scenario_library(config, config_path=config_path)
    library_run_id = st._stable_id(
        "stress-scenario-library",
        normalized.get("library_id"),
        normalized.get("version"),
        generated.isoformat(),
    )
    root = st._unique_dir(output_dir / library_run_id)
    root.mkdir(parents=True, exist_ok=False)
    library = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_stress_scenario_library",
        "library_run_id": root.name,
        "stress_scenario_library_id": normalized.get("library_id"),
        "version": normalized.get("version"),
        "status": normalized.get("status"),
        "owner": normalized.get("owner"),
        "generated_at": generated.isoformat(),
        "config_path": str(config_path),
        "scenario_count": len(_records(normalized.get("scenarios"))),
        "required_scenarios_present": _required_scenarios_present(normalized),
        "missing_required_scenarios": _missing_required_scenarios(normalized),
        "candidate_validation_use": normalized.get("candidate_validation_use"),
        "next_validation_action": _next_validation_action(normalized),
        "selection_policy": normalized.get("selection_policy"),
        "scenarios": normalized.get("scenarios"),
        **STRESS_SCENARIO_LIBRARY_SAFETY,
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_stress_scenario_manifest",
        "library_run_id": root.name,
        "stress_scenario_library_id": normalized.get("library_id"),
        "version": normalized.get("version"),
        "status": "PASS" if library["required_scenarios_present"] else "FAIL",
        "generated_at": generated.isoformat(),
        "config_path": str(config_path),
        "stress_scenario_manifest_path": str(root / "stress_scenario_manifest.json"),
        "stress_scenario_input_snapshot_path": str(root / "stress_scenario_input_snapshot.json"),
        "stress_scenario_library_path": str(root / "stress_scenario_library.json"),
        "stress_scenario_reader_brief_path": str(root / "stress_scenario_reader_brief.md"),
        "stress_scenario_report_path": str(root / "stress_scenario_report.md"),
        **STRESS_SCENARIO_LIBRARY_SAFETY,
    }
    reader = render_stress_scenario_reader_brief(library)
    input_snapshot = {
        "schema_version": STRESS_SCENARIO_LIBRARY_INPUT_SCHEMA,
        "library_run_id": root.name,
        "generated_at": generated.isoformat(),
        "config_path": str(config_path),
        "normalized_config": normalized,
    }
    input_snapshot_path = root / "stress_scenario_input_snapshot.json"
    st._write_json(input_snapshot_path, input_snapshot)
    manifest["input_snapshot_sha256"] = st._file_sha256(input_snapshot_path)
    st._write_json(root / "stress_scenario_manifest.json", manifest)
    st._write_json(root / "stress_scenario_library.json", library)
    st._write_text(root / "stress_scenario_reader_brief.md", reader)
    st._write_text(
        root / "stress_scenario_report.md",
        render_stress_scenario_report(manifest, library),
    )
    st._write_latest_pointer(
        "latest_stress_scenario_library",
        root.name,
        root / "stress_scenario_manifest.json",
    )
    validation = validate_stress_scenario_library_artifact(
        library_run_id=root.name,
        output_dir=output_dir,
        write_output=True,
    )
    return {
        "library_run_id": root.name,
        "library_dir": root,
        "manifest": manifest,
        "stress_scenario_library": library,
        "stress_scenario_reader_brief": reader,
        "stress_scenario_validation": validation,
    }


def stress_scenario_library_report_payload(
    *,
    library_run_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_STRESS_SCENARIO_LIBRARY_DIR,
) -> dict[str, Any]:
    root = st._artifact_dir(
        artifact_id=library_run_id,
        latest_pointer="latest_stress_scenario_library",
        latest=latest,
        output_dir=output_dir,
        required_name="stress_scenario_manifest.json",
    )
    payload = {
        **st._read_json(root / "stress_scenario_manifest.json"),
        "stress_scenario_library": st._read_json(root / "stress_scenario_library.json"),
        "stress_scenario_reader_brief": (root / "stress_scenario_reader_brief.md").read_text(
            encoding="utf-8"
        ),
        "library_dir": str(root),
    }
    validation = st._read_optional_json(root / "stress_scenario_validation.json")
    if validation:
        payload["stress_scenario_validation"] = validation
    return payload


def validate_stress_scenario_library_artifact(
    *,
    library_run_id: str,
    output_dir: Path = DEFAULT_STRESS_SCENARIO_LIBRARY_DIR,
    write_output: bool = True,
) -> dict[str, Any]:
    root = output_dir / library_run_id
    manifest = st._read_optional_json(root / "stress_scenario_manifest.json") or {}
    library = st._read_optional_json(root / "stress_scenario_library.json") or {}
    input_snapshot = st._read_optional_json(root / "stress_scenario_input_snapshot.json") or {}
    reader = (
        (root / "stress_scenario_reader_brief.md").read_text(encoding="utf-8")
        if (root / "stress_scenario_reader_brief.md").exists()
        else ""
    )
    scenarios = _records(library.get("scenarios"))
    scenario_ids = {_text(row.get("scenario_id")) for row in scenarios}
    checks = st._required_file_checks(
        root,
        (
            "stress_scenario_manifest.json",
            "stress_scenario_input_snapshot.json",
            "stress_scenario_library.json",
            "stress_scenario_reader_brief.md",
            "stress_scenario_report.md",
        ),
    )
    checks.extend(
        [
            st._check(
                "input_snapshot_schema",
                input_snapshot.get("schema_version") == STRESS_SCENARIO_LIBRARY_INPUT_SCHEMA,
                "",
            ),
            st._check(
                "input_snapshot_id",
                input_snapshot.get("library_run_id") == library_run_id,
                "",
            ),
            st._check(
                "input_snapshot_sha256_matches",
                bool(_text(manifest.get("input_snapshot_sha256")))
                and manifest.get("input_snapshot_sha256")
                == st._file_sha256(root / "stress_scenario_input_snapshot.json"),
                "",
            ),
            st._check(
                "library_run_id_matches",
                manifest.get("library_run_id") == library_run_id,
                "",
            ),
            st._check("metadata_visible", _metadata_visible(library), ""),
            st._check(
                "scenario_count",
                len(scenarios) >= len(REQUIRED_STRESS_SCENARIO_IDS),
                str(len(scenarios)),
            ),
            st._check("scenario_ids_unique", len(scenario_ids) == len(scenarios), ""),
            st._check(
                "required_scenarios_present",
                set(REQUIRED_STRESS_SCENARIO_IDS).issubset(scenario_ids),
                ",".join(sorted(scenario_ids)),
            ),
            st._check(
                "scenario_metadata_complete",
                all(_scenario_metadata_complete(row) for row in scenarios),
                "",
            ),
            st._check(
                "selection_policy_visible",
                bool(_mapping(library.get("selection_policy")).get("default_selection_method")),
                "",
            ),
            st._check(
                "reader_brief_fields",
                "stress_scenario_library_id" in reader and "scenario_count" in reader,
                "",
            ),
            st._check(
                "library_read_only",
                library.get("data_downloaded_by_library") is False
                and library.get("pipelines_executed_by_library") is False,
                "",
            ),
            st._check(
                "not_probability_forecast",
                library.get("not_probability_forecast") is True,
                "",
            ),
            st._check("broker_forbidden", st._payload_safe(manifest, library), ""),
        ]
    )
    try:
        config_path = Path(_text(input_snapshot.get("config_path")))
        live_normalized = _normalized_stress_scenario_library(
            st._load_yaml_mapping(config_path),
            config_path=config_path,
        )
    except (OSError, ValueError):
        live_normalized = {}
    snapshot_normalized = _mapping(input_snapshot.get("normalized_config"))
    checks.extend(
        [
            st._check(
                "config_snapshot_matches_live",
                live_normalized == snapshot_normalized,
                "",
            ),
            st._check(
                "library_matches_config_snapshot",
                library.get("stress_scenario_library_id") == snapshot_normalized.get("library_id")
                and library.get("version") == snapshot_normalized.get("version")
                and library.get("status") == snapshot_normalized.get("status")
                and library.get("owner") == snapshot_normalized.get("owner")
                and library.get("selection_policy") == snapshot_normalized.get("selection_policy")
                and library.get("scenarios") == snapshot_normalized.get("scenarios"),
                "",
            ),
            st._check(
                "report_exact_rebuild",
                (root / "stress_scenario_report.md").read_text(encoding="utf-8")
                == render_stress_scenario_report(manifest, library),
                "",
            ),
            st._check(
                "reader_exact_rebuild",
                reader == render_stress_scenario_reader_brief(library),
                "",
            ),
        ]
    )
    validation = st._validation_payload(
        "etf_dynamic_v3_stress_scenario_library_validation",
        library_run_id,
        checks,
    )
    if write_output:
        st._write_json(root / "stress_scenario_validation.json", validation)
        st._write_text(
            root / "stress_scenario_validation.md",
            render_stress_scenario_validation_report(validation),
        )
    return validation


def render_stress_scenario_reader_brief(library: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Stress Scenario Library",
            "",
            f"- stress_scenario_library_id: {library.get('stress_scenario_library_id')}",
            f"- scenario_count: {library.get('scenario_count')}",
            f"- required_scenarios_present: {library.get('required_scenarios_present')}",
            f"- candidate_validation_use: {library.get('candidate_validation_use')}",
            f"- next_validation_action: {library.get('next_validation_action')}",
            "- safety_boundary: candidate validation only / no data refresh / "
            "no pipeline execution / no official target / no broker / no production",
            "",
        ]
    )


def render_stress_scenario_report(
    manifest: Mapping[str, Any],
    library: Mapping[str, Any],
) -> str:
    rows = [
        "| "
        f"`{row.get('scenario_id')}` | "
        f"{row.get('scenario_group')} | "
        f"{row.get('severity')} | "
        f"{row.get('candidate_validation_use')} | "
        f"{', '.join(_texts(row.get('expected_failure_modes')))} |"
        for row in _records(library.get("scenarios"))
    ]
    selection_policy = _mapping(library.get("selection_policy"))
    return "\n".join(
        [
            f"# Stress Scenario Library {manifest.get('library_run_id')}",
            "",
            "## Summary",
            f"- library: {library.get('stress_scenario_library_id')} / {library.get('version')}",
            f"- scenario_count: {library.get('scenario_count')}",
            f"- required_scenarios_present: {library.get('required_scenarios_present')}",
            f"- candidate_validation_use: {library.get('candidate_validation_use')}",
            f"- next_validation_action: {library.get('next_validation_action')}",
            "",
            "## Scenario Selection",
            f"- market_regime: {selection_policy.get('market_regime')}",
            f"- method: {selection_policy.get('default_selection_method')}",
            "",
            "## Scenarios",
            "| scenario_id | group | severity | validation use | expected failure modes |",
            "|---|---|---|---|---|",
            *rows,
            "",
            "## Safety Boundary",
            "- read-only scenario library",
            "- no data download or upstream rerun",
            "- no candidate ledger mutation",
            "- no official target weights",
            "- no broker action or order ticket",
            "- no production mutation",
            "",
        ]
    )


def render_stress_scenario_validation_report(validation: Mapping[str, Any]) -> str:
    checks = [
        f"- {row.get('check_id')}: passed={row.get('passed')} detail={row.get('detail')}"
        for row in _records(validation.get("checks"))
    ]
    return "\n".join(
        [
            f"# Stress Scenario Library Validation {validation.get('artifact_id')}",
            "",
            f"- status: {validation.get('status')}",
            f"- failed_check_count: {validation.get('failed_check_count')}",
            "- production_effect: none",
            "",
            "## Checks",
            *checks,
            "",
        ]
    )


def _normalized_stress_scenario_library(
    config: Mapping[str, Any],
    *,
    config_path: Path,
) -> dict[str, Any]:
    selection_policy = _mapping(config.get("selection_policy"))
    scenarios = [_normalized_scenario(row) for row in _records(config.get("scenarios"))]
    safety = {**STRESS_SCENARIO_LIBRARY_SAFETY, **_mapping(config.get("safety"))}
    return {
        "schema_version": st.SCHEMA_VERSION,
        "library_id": _text(
            config.get("library_id"),
            "dynamic_v3_rescue_stress_scenario_library_v1",
        ),
        "version": _text(config.get("version")),
        "status": _text(config.get("status"), "pilot_baseline"),
        "owner": _text(config.get("owner"), "system_validation"),
        "rationale": _text(config.get("rationale")),
        "intended_effect": _text(config.get("intended_effect")),
        "validation_evidence": _text(config.get("validation_evidence")),
        "review_condition": _text(config.get("review_condition")),
        "config_path": str(config_path),
        "candidate_validation_use": "standardized_dynamic_v3_candidate_stress_validation",
        "selection_policy": {
            **selection_policy,
            "required_scenario_ids": _texts(selection_policy.get("required_scenario_ids"))
            or list(REQUIRED_STRESS_SCENARIO_IDS),
        },
        "scenarios": scenarios,
        "safety": safety,
        **safety,
    }


def _normalized_scenario(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": st.SCHEMA_VERSION,
        "scenario_id": _text(row.get("scenario_id")),
        "name": _text(row.get("name")),
        "scenario_group": _text(row.get("scenario_group")),
        "severity": _text(row.get("severity")),
        "market_regime_relevance": _text(row.get("market_regime_relevance")),
        "selection_rationale": _text(row.get("selection_rationale")),
        "candidate_validation_use": _text(row.get("candidate_validation_use")),
        "expected_failure_modes": _texts(row.get("expected_failure_modes")),
        "required_evidence": _texts(row.get("required_evidence")),
        "review_questions": _texts(row.get("review_questions")),
        **STRESS_SCENARIO_LIBRARY_SAFETY,
    }


def _required_scenarios_present(library: Mapping[str, Any]) -> bool:
    return not _missing_required_scenarios(library)


def _missing_required_scenarios(library: Mapping[str, Any]) -> list[str]:
    scenario_ids = {_text(row.get("scenario_id")) for row in _records(library.get("scenarios"))}
    required = _texts(
        _mapping(library.get("selection_policy")).get("required_scenario_ids")
    ) or list(REQUIRED_STRESS_SCENARIO_IDS)
    return sorted(set(required) - scenario_ids)


def _next_validation_action(library: Mapping[str, Any]) -> str:
    if _missing_required_scenarios(library):
        return "complete_required_scenario_metadata_before_candidate_validation"
    return "use_library_ids_in_next_stress_backfill_or_case_review"


def _metadata_visible(library: Mapping[str, Any]) -> bool:
    return all(
        bool(_text(library.get(key)))
        for key in (
            "stress_scenario_library_id",
            "version",
            "status",
            "owner",
            "candidate_validation_use",
        )
    ) and bool(_mapping(library.get("selection_policy")).get("default_selection_method"))


def _scenario_metadata_complete(row: Mapping[str, Any]) -> bool:
    return all(
        [
            bool(_text(row.get("scenario_id"))),
            bool(_text(row.get("name"))),
            bool(_text(row.get("scenario_group"))),
            bool(_text(row.get("severity"))),
            bool(_text(row.get("market_regime_relevance"))),
            bool(_text(row.get("selection_rationale"))),
            bool(_text(row.get("candidate_validation_use"))),
            bool(_texts(row.get("expected_failure_modes"))),
            bool(_texts(row.get("required_evidence"))),
            bool(_texts(row.get("review_questions"))),
        ]
    )


_mapping = st._mapping
_records = st._records
_text = st._text
_texts = st._texts
