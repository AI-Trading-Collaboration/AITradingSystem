from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from ai_trading_system.qqq_options_research import (
    growth_action_value_veto_option_signal_architecture as architecture_contract,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ARCHITECTURE_PATH = architecture_contract.DEFAULT_ARCHITECTURE_PATH
DEFAULT_LEGACY_COMPATIBILITY_MAP_PATH = (
    architecture_contract.DEFAULT_LEGACY_COMPATIBILITY_MAP_PATH
)
LegacyVetoCompatibilityMap = architecture_contract.LegacyVetoCompatibilityMap
VetoOptionSignalArchitecture = architecture_contract.VetoOptionSignalArchitecture
VetoOptionSignalArchitectureError = architecture_contract.VetoOptionSignalArchitectureError
load_legacy_veto_compatibility_map = (
    architecture_contract.load_legacy_veto_compatibility_map
)
load_veto_option_signal_architecture = (
    architecture_contract.load_veto_option_signal_architecture
)


def _architecture_payload() -> dict[str, object]:
    path = PROJECT_ROOT / DEFAULT_ARCHITECTURE_PATH
    return load_strict_yaml_text(path.read_text(encoding="utf-8"), label=str(path))


def _compatibility_payload() -> dict[str, object]:
    path = PROJECT_ROOT / DEFAULT_LEGACY_COMPATIBILITY_MAP_PATH
    return load_strict_yaml_text(path.read_text(encoding="utf-8"), label=str(path))


def test_architecture_loads_as_non_executable_owner_freeze_draft() -> None:
    result = load_veto_option_signal_architecture()

    assert result.terminal == (
        "DRAFT_READY_FOR_OWNER_EXACT_FREEZE_NO_EXECUTION_AUTHORITY"
    )
    assert result.architecture.policy_family_generation == "RESULT_BLIND_SUCCESSOR_V3"
    assert result.file_sha256 == hashlib.sha256(result.path.read_bytes()).hexdigest()
    assert result.canonical_sha256 == result.architecture.canonical_sha256
    assert result.architecture.safety.veto_series_generation_allowed is False
    assert result.architecture.safety.r1_manifest_generation_allowed is False
    assert result.architecture.safety.provider_query_authorized is False
    assert result.architecture.safety.real_dq_authorized is False
    assert result.architecture.safety.backtest_authorized is False
    assert result.architecture.safety.production_effect == "none"
    assert result.architecture.safety.broker_action == "none"


def test_immutable_v1_v2_dq_v3_and_exact_sheet_v4_identities_are_bound() -> None:
    bindings = load_veto_option_signal_architecture().architecture.authority_bindings

    assert tuple(row.file_sha256 for row in bindings[:4]) == (
        "03edd3868da276be69652cd9854f0201934a6cf2fa4eb5c40bfcfb4ff06206c1",
        "f02df23a4bd36069f5fe09354a3ce8480583fc451b71ec511bc3ba2da27780f2",
        "96eafe7525704a8e0e260c9ed344adf3420f7e1c977e877a557856258fee3144",
        "c90c4cc22b8918e90641bf0553416a68458433bea750bd2064fcf98df7886215",
    )
    assert all(row.immutable for row in bindings)


def test_dq_action_guard_market_veto_alpha_and_diagnostics_are_separate() -> None:
    architecture = load_veto_option_signal_architecture().architecture

    assert architecture.layers.dq_is_market_veto is False
    assert architecture.layers.action_guard_is_market_veto is False
    assert architecture.layers.option_alpha_is_market_veto is False
    assert architecture.layers.option_risk_diagnostics_mandatory is False
    assert architecture.data_qualification.precedence == (
        "INVALID",
        "FAIL",
        "INSUFFICIENT",
        "PASS",
    )
    assert architecture.data_qualification.non_pass_can_be_market_clear is False
    assert architecture.action_universe_constraints.allowed_assets == ("QQQ", "SGOV")


def test_successor_has_four_orthogonal_market_vetoes_and_no_tqqq_market_gate() -> None:
    vetoes = load_veto_option_signal_architecture().architecture.mandatory_market_state_vetoes

    assert tuple(row.veto_id for row in vetoes) == (
        "broad_market_risk_off_veto",
        "realized_volatility_veto",
        "scheduled_event_risk_veto",
        "underlying_trend_break_veto",
    )
    assert all(row.option_alpha_input_allowed is False for row in vetoes)
    assert all(row.series_generation_allowed_now is False for row in vetoes)
    assert "tqqq_veto" not in {row.legacy_field for row in vetoes}


def test_legacy_map_keeps_tqqq_as_action_guard_and_risk_off_blocked() -> None:
    compatibility = load_legacy_veto_compatibility_map().compatibility_map
    rows = {row.legacy_field: row for row in compatibility.mapping}

    assert rows["tqqq_veto"].successor_field == "NO_LEVERAGE_ETF_ACTION_GUARD"
    assert rows["tqqq_veto"].current_state == (
        "REMOVED_FROM_SUCCESSOR_MARKET_CLEAR_GATE"
    )
    assert rows["risk_off_veto"].current_state == "BLOCKED_OWNER_EXACT_FREEZE"
    assert all(row.legacy_bytes_retained for row in rows.values())
    assert all(not row.direct_successor_consumption_allowed for row in rows.values())


def test_alpha_to_veto_and_result_to_source_edges_are_mechanically_banned() -> None:
    architecture = load_veto_option_signal_architecture().architecture
    dependency = architecture.dependency_policy

    assert "selected_call_activity" in dependency.forbidden_mandatory_veto_inputs
    assert "selected_put_activity" in dependency.forbidden_mandatory_veto_inputs
    assert "selected_pair_checksum" in dependency.forbidden_mandatory_veto_inputs
    assert "candidate_return" in dependency.forbidden_mandatory_veto_inputs
    assert dependency.alpha_to_veto_edge_allowed is False
    assert dependency.veto_to_alpha_selection_edge_allowed is False
    assert dependency.result_to_source_or_bucket_edge_allowed is False
    assert architecture.option_alpha.selected_call_put_activity_role == "ALPHA_ONLY"


def test_option_surface_is_optional_independent_and_not_currently_admitted() -> None:
    option_risk = load_veto_option_signal_architecture().architecture.option_risk_diagnostics

    assert option_risk.role == "OPTIONAL_INDEPENDENT_DIAGNOSTIC_ONLY"
    assert option_risk.current_capability == (
        "NOT_ADMITTED_NO_EXACT_1202_PIT_VALID_SURFACE"
    )
    assert option_risk.contributor_universe == "PRE_SELECTION_RESULT_BLIND_FIXED_BUCKETS"
    assert option_risk.selected_pair_input_allowed is False
    assert option_risk.raw_option_rows_allowed is False
    assert option_risk.option_sid_allowed is False
    assert option_risk.mandatory_market_clear_input_allowed_now is False


def test_manifest_stop_policy_fails_before_r1_authority() -> None:
    stop = load_veto_option_signal_architecture().architecture.manifest_stop_policy

    assert stop.stop_before == "R1_MANIFEST_GENERATION"
    assert stop.missing_required_source_outcome == (
        "INSUFFICIENT_EVIDENCE_TO_BUILD_R1_MANIFEST"
    )
    assert stop.malformed_authority_outcome == "PRE_RUN_AUTHORITY_INVALID"
    assert stop.constant_false_fill_allowed is False
    assert stop.retained_series_truncation_allowed is False
    assert stop.cross_date_fallback_allowed is False


@pytest.mark.parametrize(
    ("mutate", "match"),
    [
        (
            lambda payload: payload["layers"].__setitem__("dq_is_market_veto", True),
            "Input should be False",
        ),
        (
            lambda payload: payload["dependency_policy"].__setitem__(
                "alpha_to_veto_edge_allowed", True
            ),
            "Input should be False",
        ),
        (
            lambda payload: payload["dependency_policy"][
                "forbidden_mandatory_veto_inputs"
            ].pop(),
            "forbidden alpha-to-veto input set drifted",
        ),
        (
            lambda payload: payload["mandatory_market_state_vetoes"][0].__setitem__(
                "legacy_field", "tqqq_veto"
            ),
            "mandatory market-state veto set drifted",
        ),
        (
            lambda payload: payload["safety"].__setitem__(
                "veto_series_generation_allowed", True
            ),
            "Input should be False",
        ),
        (
            lambda payload: payload["data_qualification"].__setitem__(
                "missing_unknown_or_non_pit_terminal", "PASS"
            ),
            "Input should be 'INVALID'",
        ),
    ],
)
def test_architecture_rejects_semantic_or_execution_drift(
    mutate: object, match: str
) -> None:
    payload = deepcopy(_architecture_payload())
    mutate(payload)  # type: ignore[operator]

    with pytest.raises(ValueError, match=match):
        VetoOptionSignalArchitecture.model_validate(payload)


def test_legacy_map_rejects_deletion_direct_consumption_and_role_drift() -> None:
    deletion = deepcopy(_compatibility_payload())
    deletion["legacy_taxonomy"].pop()
    with pytest.raises(ValueError, match="legacy veto taxonomy drifted"):
        LegacyVetoCompatibilityMap.model_validate(deletion)

    consumption = deepcopy(_compatibility_payload())
    consumption["mapping"][0]["direct_successor_consumption_allowed"] = True
    with pytest.raises(ValueError, match="Input should be False"):
        LegacyVetoCompatibilityMap.model_validate(consumption)

    role = deepcopy(_compatibility_payload())
    role["mapping"][0]["current_state"] = "READY"
    with pytest.raises(ValueError, match="legacy compatibility mapping drifted"):
        LegacyVetoCompatibilityMap.model_validate(role)


def test_loader_rejects_immutable_authority_hash_drift() -> None:
    payload = deepcopy(_architecture_payload())
    payload["authority_bindings"][0]["file_sha256"] = "0" * 64

    with TemporaryDirectory(dir=PROJECT_ROOT / "outputs") as directory:
        path = Path(directory) / "architecture.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        with pytest.raises(
            VetoOptionSignalArchitectureError,
            match="file SHA-256 mismatch",
        ):
            load_veto_option_signal_architecture(path=path)


def test_canonical_round_trip_contains_no_result_series_or_manifest() -> None:
    architecture = load_veto_option_signal_architecture().architecture
    replay = VetoOptionSignalArchitecture.model_validate(
        json.loads(architecture.canonical_bytes)
    )

    assert replay.canonical_bytes == architecture.canonical_bytes
    assert replay.canonical_sha256 == architecture.canonical_sha256
    text = architecture.canonical_bytes.decode("utf-8")
    assert "backtest_id" not in text
    assert "veto_series_sha256" not in text
    assert "strategy_return" not in text
    assert "r1_manifest_sha256" not in text


def test_missing_or_escaping_paths_are_typed_rejections(tmp_path: Path) -> None:
    with pytest.raises(
        VetoOptionSignalArchitectureError,
        match="VETO_OPTION_SIGNAL_ARCHITECTURE_REJECTED",
    ):
        load_veto_option_signal_architecture(
            path=Path("config/research/does_not_exist.yaml")
        )

    outside = tmp_path / "outside.yaml"
    outside.write_text("schema_version: invalid\n", encoding="utf-8")
    with pytest.raises(
        VetoOptionSignalArchitectureError,
        match="escapes project root",
    ):
        load_veto_option_signal_architecture(path=outside)

    isolated_root = tmp_path / "isolated_root"
    isolated_root.mkdir()
    relative_outside = Path("..") / outside.name
    with pytest.raises(
        VetoOptionSignalArchitectureError,
        match="escapes project root",
    ):
        load_veto_option_signal_architecture(
            path=relative_outside,
            project_root=isolated_root,
        )
