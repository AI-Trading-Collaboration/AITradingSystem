from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from ai_trading_system.qqq_options_research.cross_layer_validation import (
    DEFAULT_QQQ_OPTIONS_CROSS_LAYER_VALIDATION_GOLDEN_PATH,
    DEFAULT_QQQ_OPTIONS_CROSS_LAYER_VALIDATION_POLICY_PATH,
    QQQOptionsCloudSmokeChecklist,
    QQQOptionsCrossLayerArtifactBinding,
    QQQOptionsCrossLayerObservation,
    QQQOptionsCrossLayerScenarioSpec,
    QQQOptionsCrossLayerValidationError,
    QQQOptionsCrossLayerValidationReport,
    build_qqq_options_cloud_smoke_checklist,
    build_qqq_options_cross_layer_validation_report,
    load_qqq_options_cross_layer_validation_harness,
    load_qqq_options_cross_layer_validation_policy,
    validate_qqq_options_cross_layer_observation,
)

ROOT = Path(__file__).resolve().parents[1]
AT = datetime(2026, 8, 3, 0, 0, tzinfo=UTC)
POLICY_SHA256 = "363f7596aa874b819b8377f278e6a503134fa87895553415b7636a5288a13f11"
CORPUS_SHA256 = "74325026a521bba6bd7b22bd8f9d9c600f1016e2fed2a5a8f97a27b89ad1819c"
SCENARIO_IDS = (
    "CORPORATE_ACTION_SCOPE_INVALID",
    "CROSSED_QUOTE_INVALID",
    "INSUFFICIENT_SETTLED_CASH",
    "ITM_EXPIRY_SCOPE_INVALID",
    "MISSING_QUOTE_INVALID",
    "NO_ELIGIBLE_CONTRACT_CASH",
    "PARTIAL_FILL_CANCELED",
    "STALE_QUOTE_REJECTED",
    "VALID_CROSS_LAYER_SYNTHETIC",
    "VENUE_REJECTED_CASH",
)
EXPECTED_FIXTURE_HASHES = {
    "CORPORATE_ACTION_SCOPE_INVALID": (
        "38cd329ee1b5a1ae5f3952566d2e613ee9abd59ebde8c587d1c2bc8d6aa626ab"
    ),
    "CROSSED_QUOTE_INVALID": (
        "a74d81053b445389b186a0034b8e1853035603f18a424c50c668a7bb4df9b216"
    ),
    "INSUFFICIENT_SETTLED_CASH": (
        "cda38868a24be1586a65370f6e84342cd38143b8e41f87e21055af1657c469d5"
    ),
    "ITM_EXPIRY_SCOPE_INVALID": (
        "ab615becb15047aea3f180d8982343a7202ccce2935f65593557ac651de798aa"
    ),
    "MISSING_QUOTE_INVALID": (
        "8002c1782625665dbd0f78f360b8dfe299a33aaaefccba2d00c654603549845c"
    ),
    "NO_ELIGIBLE_CONTRACT_CASH": (
        "2ac8bc62d07cb9ade4723d63310b81051a18ef603d7ffec8c0a1d5eecc943cdb"
    ),
    "PARTIAL_FILL_CANCELED": (
        "d371bb52eca0e8a358d4a1e05a10ce5187aceeeb8387ac1b6c6f2d7149d4fd8c"
    ),
    "STALE_QUOTE_REJECTED": (
        "024395ef576a3c1e2c88ae275d8e46798e8576904d729814985a97db7d27ada5"
    ),
    "VALID_CROSS_LAYER_SYNTHETIC": (
        "933296582f637f27d259ccdd4bb4b7746544138d5e2518e6cf545c335f8cda2d"
    ),
    "VENUE_REJECTED_CASH": (
        "0e7af602b134f34cb21d29219dcffdfa767d8b1587aa7cfe721a03d79df46585"
    ),
}


def _loaded():
    return load_qqq_options_cross_layer_validation_harness(project_root=ROOT)


def _spec(scenario_id: str) -> QQQOptionsCrossLayerScenarioSpec:
    return next(item for item in _loaded().policy.scenarios if item.scenario_id == scenario_id)


def _artifact_hash(role: str) -> str:
    return hashlib.sha256(f"synthetic:{role}".encode()).hexdigest()


def _observation(
    spec: QQQOptionsCrossLayerScenarioSpec,
    **overrides: object,
) -> QQQOptionsCrossLayerObservation:
    payload: dict[str, object] = {
        "schema_version": "qqq_options_cross_layer_observation.v1",
        "observation_id": f"obs-{spec.scenario_id.lower()}",
        "scenario_id": spec.scenario_id,
        "observed_at_utc": AT,
        "terminal_layer": spec.terminal_layer,
        "observed_status": spec.expected_status,
        "reason_codes": spec.expected_reason_codes,
        "order_count": spec.expected_order_count,
        "fill_count": spec.expected_fill_count,
        "cash_preservation_required": spec.cash_preservation_required,
        "run_valid": spec.run_valid,
        "dq_status": spec.expected_dq_status,
        "pit_status": spec.expected_pit_status,
        "artifact_bindings": tuple(
            QQQOptionsCrossLayerArtifactBinding(
                artifact_role=item.artifact_role,
                contract_id=item.contract_id,
                artifact_sha256=_artifact_hash(item.artifact_role),
            )
            for item in spec.required_artifacts
        ),
        "evidence_classification": "SYNTHETIC_TEST_ONLY_NOT_PLATFORM_EVIDENCE",
        "synthetic_fixture_is_platform_evidence": False,
        "investment_interpretation_allowed": False,
        "pilot_authorized": False,
        "range_expansion_allowed": False,
    }
    payload.update(overrides)
    return QQQOptionsCrossLayerObservation.seal(**payload)


def _all_observations() -> tuple[QQQOptionsCrossLayerObservation, ...]:
    return tuple(_observation(spec) for spec in _loaded().policy.scenarios)


def _write_modified(source: Path, target: Path, old: str, new: str) -> Path:
    content = source.read_text(encoding="utf-8")
    assert old in content
    target.write_text(content.replace(old, new, 1), encoding="utf-8")
    return target


def test_policy_and_golden_freeze_exact_authority_and_safety() -> None:
    loaded = _loaded()

    assert loaded.policy_sha256 == POLICY_SHA256
    assert loaded.golden.policy_sha256 == POLICY_SHA256
    assert loaded.golden.corpus_sha256 == CORPUS_SHA256
    assert loaded.policy.primary_research_start.isoformat() == "2021-02-22"
    assert loaded.policy.legacy_non_default_start.isoformat() == "2022-12-01"
    assert loaded.policy.legacy_non_default_start_is_default is False
    assert loaded.policy.decision == "QQQ_OPTIONS_VALIDATION_HARNESS_READY"
    assert len(loaded.policy.authority_bindings) == 20
    assert loaded.policy.safety.synthetic_fixture_is_platform_evidence is False
    assert loaded.policy.safety.synthetic_pass_may_authorize_pilot is False
    assert loaded.policy.safety.external_platform_action_allowed is False
    assert loaded.policy.safety.production_effect == "none"
    assert loaded.policy.safety.broker_action == "none"


def test_scenario_inventory_and_golden_hashes_are_complete_and_exact() -> None:
    loaded = _loaded()
    specs = loaded.policy.scenarios
    goldens = loaded.golden.scenario_goldens

    assert tuple(item.scenario_id for item in specs) == SCENARIO_IDS
    assert tuple(item.scenario_id for item in goldens) == SCENARIO_IDS
    assert {item.scenario_id: item.fixture_sha256 for item in specs} == EXPECTED_FIXTURE_HASHES
    assert {item.scenario_id: item.fixture_sha256 for item in goldens} == (
        EXPECTED_FIXTURE_HASHES
    )
    assert all(
        item.evidence_classification == "SYNTHETIC_TEST_ONLY_NOT_PLATFORM_EVIDENCE"
        for item in specs
    )


def test_every_predecessor_authority_hash_replays_from_current_checkout() -> None:
    loaded = load_qqq_options_cross_layer_validation_policy(project_root=ROOT)
    for binding in loaded.policy.authority_bindings:
        payload = (ROOT / binding.path).read_bytes().replace(b"\r\n", b"\n")
        assert hashlib.sha256(payload).hexdigest() == binding.sha256, binding.authority_id


def test_authority_hash_drift_fails_closed(tmp_path: Path) -> None:
    policy_path = _write_modified(
        ROOT / DEFAULT_QQQ_OPTIONS_CROSS_LAYER_VALIDATION_POLICY_PATH,
        tmp_path / "drifted.yaml",
        "84020b2c7b7263b65d6b437ba178a0f7b94d678dc47561418fca48fce7792830",
        "f" * 64,
    )
    with pytest.raises(QQQOptionsCrossLayerValidationError, match="authority hash drifted"):
        load_qqq_options_cross_layer_validation_policy(policy_path, project_root=ROOT)


def test_missing_and_escaping_authority_paths_fail_closed(tmp_path: Path) -> None:
    source = ROOT / DEFAULT_QQQ_OPTIONS_CROSS_LAYER_VALIDATION_POLICY_PATH
    missing = _write_modified(
        source,
        tmp_path / "missing.yaml",
        "src/ai_trading_system/qqq_options_research/contracts.py",
        "src/ai_trading_system/qqq_options_research/not_present.py",
    )
    with pytest.raises(QQQOptionsCrossLayerValidationError, match="required regular"):
        load_qqq_options_cross_layer_validation_policy(missing, project_root=ROOT)

    escaping = _write_modified(
        source,
        tmp_path / "escaping.yaml",
        "src/ai_trading_system/qqq_options_research/contracts.py",
        "../contracts.py",
    )
    with pytest.raises(QQQOptionsCrossLayerValidationError, match="portable repository-relative"):
        load_qqq_options_cross_layer_validation_policy(escaping, project_root=ROOT)


def test_symlink_authority_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = ROOT / "src/ai_trading_system/qqq_options_research/contracts.py"
    original = Path.is_symlink

    def fake_is_symlink(path: Path) -> bool:
        return path == target or original(path)

    monkeypatch.setattr(Path, "is_symlink", fake_is_symlink)
    with pytest.raises(QQQOptionsCrossLayerValidationError, match="symlink"):
        load_qqq_options_cross_layer_validation_policy(project_root=ROOT)


@pytest.mark.parametrize(
    ("old", "new", "message"),
    (
        (
            CORPUS_SHA256,
            "e" * 64,
            "corpus golden hash drifted",
        ),
        (
            EXPECTED_FIXTURE_HASHES["CROSSED_QUOTE_INVALID"],
            "d" * 64,
            "scenario golden hashes drifted",
        ),
        (
            POLICY_SHA256,
            "c" * 64,
            "golden policy hash does not match",
        ),
    ),
)
def test_golden_tamper_fails_closed(
    tmp_path: Path,
    old: str,
    new: str,
    message: str,
) -> None:
    golden_path = _write_modified(
        ROOT / DEFAULT_QQQ_OPTIONS_CROSS_LAYER_VALIDATION_GOLDEN_PATH,
        tmp_path / "golden.yaml",
        old,
        new,
    )
    with pytest.raises(QQQOptionsCrossLayerValidationError, match=message):
        load_qqq_options_cross_layer_validation_harness(
            golden_path=golden_path,
            project_root=ROOT,
        )


@pytest.mark.parametrize("scenario_id", SCENARIO_IDS)
def test_each_exact_scenario_observation_passes(scenario_id: str) -> None:
    spec = _spec(scenario_id)
    validation = validate_qqq_options_cross_layer_observation(
        _observation(spec),
        project_root=ROOT,
    )

    assert validation.status == "PASS"
    assert validation.mismatch_codes == ()
    assert validation.fixture_sha256 == EXPECTED_FIXTURE_HASHES[scenario_id]
    assert validation.platform_evidence_status == "NOT_EVALUATED_NO_AUTHORIZED_PILOT"
    assert validation.pilot_authorized is False


@pytest.mark.parametrize(
    ("override", "expected_code"),
    (
        ({"terminal_layer": "INPUT"}, "TERMINAL_LAYER_MISMATCH"),
        ({"observed_status": "BLOCKED"}, "STATUS_MISMATCH"),
        ({"reason_codes": ("WRONG_REASON",)}, "REASON_CODES_MISMATCH"),
        ({"order_count": 2}, "ORDER_COUNT_MISMATCH"),
        ({"fill_count": 0}, "FILL_COUNT_MISMATCH"),
        ({"cash_preservation_required": True}, "CASH_PRESERVATION_MISMATCH"),
        ({"run_valid": False}, "RUN_VALIDITY_MISMATCH"),
        ({"dq_status": "NOT_EVALUATED"}, "DQ_STATUS_MISMATCH"),
        ({"pit_status": "NOT_EVALUATED"}, "PIT_STATUS_MISMATCH"),
    ),
)
def test_semantic_mismatch_is_typed(
    override: dict[str, object],
    expected_code: str,
) -> None:
    spec = _spec("VALID_CROSS_LAYER_SYNTHETIC")
    validation = validate_qqq_options_cross_layer_observation(
        _observation(spec, **override),
        project_root=ROOT,
    )

    assert validation.status == "FAIL"
    assert expected_code in validation.mismatch_codes


def test_artifact_contract_mismatch_is_typed() -> None:
    spec = _spec("STALE_QUOTE_REJECTED")
    wrong = QQQOptionsCrossLayerArtifactBinding(
        artifact_role="SELECTION_RESULT",
        contract_id="WrongSelectionContract.v1",
        artifact_sha256="a" * 64,
    )
    validation = validate_qqq_options_cross_layer_observation(
        _observation(spec, artifact_bindings=(wrong,)),
        project_root=ROOT,
    )

    assert validation.status == "FAIL"
    assert validation.mismatch_codes == ("ARTIFACT_CONTRACT_MISMATCH",)


def test_complete_report_passes_without_claiming_platform_evidence() -> None:
    report = build_qqq_options_cross_layer_validation_report(
        _all_observations(),
        report_id="cross-layer-report-001",
        built_at_utc=AT,
        project_root=ROOT,
    )

    assert report.status == "PASS"
    assert report.synthetic_fixture_coverage_status == "PASS"
    assert report.missing_scenario_ids == ()
    assert len(report.scenario_validations) == 10
    assert report.platform_evidence_status == "NOT_EVALUATED_NO_AUTHORIZED_PILOT"
    assert report.synthetic_fixture_is_platform_evidence is False
    assert report.investment_interpretation_allowed is False
    assert report.pilot_authorized is False
    assert report.range_expansion_allowed is False


def test_missing_scenario_report_fails_without_relabeling_dq() -> None:
    observations = _all_observations()[1:]
    report = build_qqq_options_cross_layer_validation_report(
        observations,
        report_id="cross-layer-report-missing",
        built_at_utc=AT,
        project_root=ROOT,
    )

    assert report.status == "FAIL"
    assert report.missing_scenario_ids == ("CORPORATE_ACTION_SCOPE_INVALID",)
    assert report.platform_evidence_status == "NOT_EVALUATED_NO_AUTHORIZED_PILOT"
    assert report.pilot_authorized is False


def test_duplicate_observation_fails_closed() -> None:
    first = _all_observations()[0]
    with pytest.raises(
        QQQOptionsCrossLayerValidationError,
        match="unique by scenario_id",
    ):
        build_qqq_options_cross_layer_validation_report(
            (first, first),
            report_id="duplicate-report",
            built_at_utc=AT,
            project_root=ROOT,
        )


def test_observation_permutation_is_report_byte_identical() -> None:
    observations = _all_observations()
    first = build_qqq_options_cross_layer_validation_report(
        observations,
        report_id="permutation-report",
        built_at_utc=AT,
        project_root=ROOT,
    )
    second = build_qqq_options_cross_layer_validation_report(
        tuple(reversed(observations)),
        report_id="permutation-report",
        built_at_utc=AT,
        project_root=ROOT,
    )

    assert first.canonical_bytes() == second.canonical_bytes()
    assert first.canonical_sha256() == second.canonical_sha256()


def test_sealed_records_replay_and_reject_tamper_or_noncanonical_json() -> None:
    observation = _observation(_spec("PARTIAL_FILL_CANCELED"))
    replayed = QQQOptionsCrossLayerObservation.from_json_bytes(observation.canonical_bytes())
    assert replayed == observation

    tampered = json.loads(observation.canonical_bytes())
    tampered["reason_codes"] = ["VENUE_REJECTED"]
    with pytest.raises(QQQOptionsCrossLayerValidationError, match="content_sha256"):
        QQQOptionsCrossLayerObservation.from_json_bytes(
            json.dumps(tampered, sort_keys=True, separators=(",", ":")).encode()
        )

    noncanonical = json.dumps(
        json.loads(observation.canonical_bytes()),
        indent=2,
        sort_keys=True,
    ).encode()
    with pytest.raises(QQQOptionsCrossLayerValidationError, match="NONCANONICAL"):
        QQQOptionsCrossLayerObservation.from_json_bytes(noncanonical)


def test_report_and_checklist_canonical_replay() -> None:
    report = build_qqq_options_cross_layer_validation_report(
        _all_observations(),
        report_id="replay-report",
        built_at_utc=AT,
        project_root=ROOT,
    )
    checklist = build_qqq_options_cloud_smoke_checklist(
        checklist_id="cloud-smoke-checklist-001",
        created_at_utc=AT,
        project_root=ROOT,
    )

    assert QQQOptionsCrossLayerValidationReport.from_json_bytes(report.canonical_bytes()) == report
    assert QQQOptionsCloudSmokeChecklist.from_json_bytes(checklist.canonical_bytes()) == checklist


def test_cloud_smoke_checklist_is_complete_and_owner_blocked() -> None:
    checklist = build_qqq_options_cloud_smoke_checklist(
        checklist_id="cloud-smoke-checklist-blocked",
        created_at_utc=AT,
        project_root=ROOT,
    )

    assert checklist.status == "BLOCKED_OWNER_AUTHORIZATION"
    assert checklist.owner_authorization_token == "NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS"
    assert len(checklist.items) == 12
    assert all(item.status == "PENDING_OWNER_AUTHORIZATION" for item in checklist.items)
    assert all(item.evidence_status == "NOT_EVALUATED" for item in checklist.items)
    assert checklist.external_action_executed is False
    assert checklist.synthetic_pass_may_authorize_pilot is False
    assert checklist.pilot_authorized is False
    assert checklist.range_expansion_allowed is False


def test_forged_cloud_authorization_cannot_replay() -> None:
    checklist = build_qqq_options_cloud_smoke_checklist(
        checklist_id="cloud-smoke-checklist-forged",
        created_at_utc=AT,
        project_root=ROOT,
    )
    payload = json.loads(checklist.canonical_bytes())
    payload["owner_authorization_token"] = "owner_decision:TRADING-2492:forged"
    payload["status"] = "READY"
    payload["external_action_executed"] = True

    with pytest.raises(QQQOptionsCrossLayerValidationError, match="RECORD_INVALID"):
        QQQOptionsCloudSmokeChecklist.from_json_bytes(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        )


def test_structural_invalid_observation_fails_before_validation() -> None:
    spec = _spec("VALID_CROSS_LAYER_SYNTHETIC")
    with pytest.raises(ValidationError, match="fill count cannot exceed order count"):
        _observation(spec, order_count=0, fill_count=1)

    with pytest.raises(ValidationError, match="future"):
        _observation(spec, observed_at_utc=datetime(2099, 1, 1, tzinfo=UTC))


def test_policy_and_golden_paths_are_repository_defaults() -> None:
    assert DEFAULT_QQQ_OPTIONS_CROSS_LAYER_VALIDATION_POLICY_PATH.as_posix() == (
        "config/research/qqq_options_cross_layer_validation_harness_v1.yaml"
    )
    assert DEFAULT_QQQ_OPTIONS_CROSS_LAYER_VALIDATION_GOLDEN_PATH.as_posix() == (
        "config/research/qqq_options_cross_layer_validation_golden_v1.yaml"
    )
