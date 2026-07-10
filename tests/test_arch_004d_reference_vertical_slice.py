from __future__ import annotations

import copy
import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from ai_trading_system import dynamic_strategy_growth_tilt_candidate_family_closure as legacy
from ai_trading_system.contracts import CanonicalStatus
from ai_trading_system.platform.artifacts import canonical_json_bytes
from ai_trading_system.platform.config import ResolvedConfig
from ai_trading_system.research_framework import (
    ExperimentRunRequest,
    ExperimentSpec,
    PluginRef,
    ResearchPluginError,
    resolve_experiment_spec,
    run_experiment,
)
from ai_trading_system.research_framework.plugins.growth_tilt_candidate_family_closure import (
    growth_tilt_candidate_family_closure_registry,
    render_growth_tilt_family_closure_markdown,
)
from ai_trading_system.research_quality import growth_tilt_candidate_family_closure as closure

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SPEC_PATH = PROJECT_ROOT / "config/research/experiments/growth_tilt_candidate_family_closure.yaml"
GENERATED_AT = datetime(2026, 7, 10, 22, 0, tzinfo=UTC)
AS_OF = date(2026, 7, 10)


def test_experiment_spec_is_typed_deterministic_and_reuses_report_contract() -> None:
    resolved = resolve_experiment_spec(SPEC_PATH)
    spec = resolved.value

    restored = ExperimentSpec.from_dict(spec.to_dict())

    assert restored == spec
    assert restored.spec_id == spec.spec_id
    assert spec.calculator_plugin == PluginRef(
        plugin_id="growth_tilt_candidate_family_closure_calculator",
        version="v1",
    )
    assert spec.canonical_status(closure.READY_STATUS) is CanonicalStatus.PASS
    assert spec.canonical_status(closure.BLOCKED_STATUS) is CanonicalStatus.BLOCKED
    assert spec.data_quality_required is False
    assert spec.production_effect.value == "none"
    assert spec.report_spec().report_id == closure.REPORT_TYPE
    assert resolved.reference.sha256


def test_experiment_spec_unknown_status_and_plugin_fail_closed() -> None:
    spec = resolve_experiment_spec(SPEC_PATH).value
    with pytest.raises(ValueError, match="UNKNOWN_EXPERIMENT_STATUS"):
        spec.canonical_status("READY_ENOUGH")

    registry = growth_tilt_candidate_family_closure_registry()
    with pytest.raises(ResearchPluginError, match="UNKNOWN_RESEARCH_PLUGIN"):
        registry.calculator(PluginRef(plugin_id="unknown_calculator", version="v1"))


def test_generic_runner_writes_legacy_artifacts_plus_envelope_and_ledger(
    tmp_path: Path,
) -> None:
    resolved = resolve_experiment_spec(SPEC_PATH)
    paths = _write_source_fixtures(tmp_path)
    result = _run_generic(
        resolved,
        paths=paths,
        output_root=tmp_path / "outputs",
        docs_root=tmp_path / "docs",
    )

    assert result.payload["status"] == closure.READY_STATUS
    assert set(result.payload["artifact_paths"]) == {
        "json_path",
        "negative_result_ledger_path",
        "markdown_path",
    }
    assert result.envelope.status is CanonicalStatus.PASS
    assert result.envelope.data_quality is None
    assert result.envelope.data_quality_required is False
    assert result.envelope.investment_facing is False
    assert result.ledger.entry("evaluate_and_render").status is CanonicalStatus.PASS
    assert result.output_paths["envelope"].exists()
    assert result.output_paths["run_ledger"].exists()
    assert result.output_paths["primary"].read_bytes() == canonical_json_bytes(
        result.payload,
        trailing_newline=False,
    )
    assert result.output_paths["reader_markdown"].read_text(encoding="utf-8") == (
        render_growth_tilt_family_closure_markdown(result.payload)
    )
    section = json.loads(result.output_paths["negative_result_ledger"].read_text(encoding="utf-8"))
    assert section == {
        "task_id": "TRADING-2438N1",
        "status": closure.READY_STATUS,
        "report_type": "negative_result_ledger",
        "schema_version": closure.LEDGER_SCHEMA_VERSION,
        "negative_result_ledger": result.payload["negative_result_ledger"],
        "production_effect": "none",
        "broker_action": "none",
    }


def test_legacy_facade_has_payload_and_markdown_parity_with_generic_runner(
    tmp_path: Path,
) -> None:
    paths = _write_source_fixtures(tmp_path)
    generic = _run_generic(
        resolve_experiment_spec(SPEC_PATH),
        paths=paths,
        output_root=tmp_path / "outputs",
        docs_root=tmp_path / "docs",
    )
    primary_before = generic.output_paths["primary"].read_bytes()
    markdown_before = generic.output_paths["reader_markdown"].read_bytes()
    section_before = generic.output_paths["negative_result_ledger"].read_bytes()

    legacy_payload = legacy.run_growth_tilt_candidate_family_closure(
        m1e_path=paths["m1e"],
        adapters_path=paths["adapters"],
        owner_resolution_path=paths["owner_resolution"],
        candidate_set_path=paths["candidate_set"],
        requirement_doc_path=paths["requirement_text"],
        report_registry_path=paths["report_registry"],
        artifact_catalog_path=paths["artifact_catalog_text"],
        system_flow_path=paths["system_flow_text"],
        output_root=tmp_path / "outputs",
        docs_root=tmp_path / "docs",
        as_of_date=AS_OF,
        strict=True,
        experiment_spec_path=SPEC_PATH,
        generated_at=GENERATED_AT,
    )

    assert legacy_payload == generic.payload
    assert generic.output_paths["primary"].read_bytes() == primary_before
    assert generic.output_paths["reader_markdown"].read_bytes() == markdown_before
    assert generic.output_paths["negative_result_ledger"].read_bytes() == section_before


def test_blocked_runner_preserves_null_evidence_and_blocked_sidecars(
    tmp_path: Path,
) -> None:
    paths = _write_source_fixtures(tmp_path)
    paths["m1e"] = tmp_path / "missing_m1e.json"

    result = _run_generic(
        resolve_experiment_spec(SPEC_PATH),
        paths=paths,
        output_root=tmp_path / "blocked_outputs",
        docs_root=tmp_path / "blocked_docs",
        strict=False,
    )

    assert result.payload["status"] == closure.BLOCKED_STATUS
    assert result.payload["source_validation_error_count"] == 1
    assert result.payload["pit_candidates_tested"] == 0
    assert result.payload["runtime_metrics_materialized"] is False
    assert result.envelope.status is CanonicalStatus.BLOCKED
    assert result.envelope.data_quality is None
    ledger_entry = result.ledger.entry("evaluate_and_render")
    assert ledger_entry.status is CanonicalStatus.BLOCKED
    assert ledger_entry.blocker_codes

    with pytest.raises(ValueError, match="m1e missing"):
        _run_generic(
            resolve_experiment_spec(SPEC_PATH),
            paths=paths,
            output_root=tmp_path / "strict_outputs",
            docs_root=tmp_path / "strict_docs",
            strict=True,
        )


def test_second_same_plugin_spec_requires_no_new_python_module_or_cli(
    tmp_path: Path,
) -> None:
    resolved = resolve_experiment_spec(SPEC_PATH)
    variant = resolved.value.model_copy(
        update={
            "experiment_id": "growth_tilt_candidate_family_closure_reproducibility_variant",
            "spec_version": "growth_tilt_candidate_family_closure.experiment.repro.v1",
        }
    )
    variant_resolved = ResolvedConfig(value=variant, reference=resolved.reference)
    paths = _write_source_fixtures(tmp_path)

    result = _run_generic(
        variant_resolved,
        paths=paths,
        output_root=tmp_path / "variant_outputs",
        docs_root=tmp_path / "variant_docs",
    )

    assert result.payload["status"] == closure.READY_STATUS
    assert result.envelope.artifact_id == variant.experiment_id
    assert result.ledger.workflow_id == f"experiment:{variant.experiment_id}"


def test_generic_runner_has_no_reference_slice_or_task_id_dependency() -> None:
    runner_text = (PROJECT_ROOT / "src/ai_trading_system/research_framework/runner.py").read_text(
        encoding="utf-8"
    )

    assert "TRADING-2438N1" not in runner_text
    assert "growth_tilt" not in runner_text
    assert closure.READY_STATUS not in runner_text


def _run_generic(
    resolved,
    *,
    paths: dict[str, Path],
    output_root: Path,
    docs_root: Path,
    strict: bool = True,
):
    return run_experiment(
        resolved_spec=resolved,
        plugins=growth_tilt_candidate_family_closure_registry(),
        request=ExperimentRunRequest(
            project_root=PROJECT_ROOT,
            output_root=output_root,
            docs_root=docs_root,
            as_of=AS_OF,
            input_overrides=paths,
            strict=strict,
            generated_at=GENERATED_AT,
        ),
    )


def _write_source_fixtures(tmp_path: Path) -> dict[str, Path]:
    sources = {
        "m1e": _m1e_fixture(),
        "adapters": {
            "schema_version": closure.EXPECTED_ADAPTER_SCHEMA,
            "status": closure.EXPECTED_ADAPTER_STATUS,
            "adapter_contract_ready_count": 0,
            "adapter_contract_blocked_count": 4,
        },
        "owner_resolution": {"schema_version": closure.EXPECTED_OWNER_SCHEMA},
        "candidate_set": {"schema_version": closure.EXPECTED_CANDIDATE_SET_SCHEMA},
        "report_registry": {"reports": [{"report_id": closure.REPORT_TYPE}]},
    }
    paths: dict[str, Path] = {}
    for source_id, value in sources.items():
        path = tmp_path / f"{source_id}.json"
        path.write_text(json.dumps(copy.deepcopy(value)), encoding="utf-8")
        paths[source_id] = path
    text_sources = {
        "requirement_text": "\n".join(
            (
                "TRADING-2438N1",
                closure.CLOSURE_STATUS,
                "candidate-independent baseline project",
                "zero placeholder candidates",
            )
        ),
        "artifact_catalog_text": "\n".join(closure.REQUIRED_CATALOG_REFERENCES),
        "system_flow_text": "\n".join(closure.REQUIRED_FLOW_REFERENCES),
    }
    for source_id, value in text_sources.items():
        path = tmp_path / f"{source_id}.md"
        path.write_text(value, encoding="utf-8")
        paths[source_id] = path
    return paths


def _m1e_fixture() -> dict[str, object]:
    rows = [
        {
            "prerequisite_id": prerequisite_id,
            "status": "PASS" if index < 2 else "BLOCKED",
            "blocker_code": None if index < 2 else closure.EXPECTED_BLOCKER_CODES[index - 2],
        }
        for index, prerequisite_id in enumerate(closure.EXPECTED_PREREQUISITE_IDS)
    ]
    return {
        "schema_version": closure.EXPECTED_M1E_SCHEMA,
        "status": closure.EXPECTED_M1E_STATUS,
        "disposition": "KEEP_REDEFINED_BLOCKED",
        "approved_candidate_count": 0,
        "m2_eligible_candidate_count": 0,
        "prerequisite_matrix": {
            "schema_version": "growth_tilt_replacement_candidate_prerequisite_matrix.v1",
            "replacement_candidate_id": "capped_recovery_permission_overlay",
            "rows": rows,
            "prerequisite_count": 10,
            "pass_count": 2,
            "blocked_count": 8,
            "all_prerequisites_ready": False,
            "blocker_codes": list(closure.EXPECTED_BLOCKER_CODES),
        },
    }
