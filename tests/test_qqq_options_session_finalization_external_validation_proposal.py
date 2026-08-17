from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from ai_trading_system.qqq_options_research.daily_transport_session_finalization import (
    validate_session_finalization_package,
)

ROOT = Path(__file__).resolve().parents[1]
PACKAGE_ROOT = ROOT / (
    "inputs/research/qqq_options/"
    "trading_2532_session_finalization_v2_external_validation_proposal_v1"
)


def _object(raw: bytes) -> dict[str, Any]:
    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise AssertionError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    value = json.loads(raw.decode("utf-8"), object_pairs_hook=reject_duplicates)
    assert isinstance(value, dict)
    return value


def _content_sha256(payload: dict[str, Any]) -> str:
    body = dict(payload)
    body.pop("content_sha256")
    canonical = (
        json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def test_proposal_package_binds_exact_2531_source_and_has_no_external_effect() -> None:
    source = validate_session_finalization_package(project_root=ROOT)
    scope = _object((PACKAGE_ROOT / "execution_scope.json").read_bytes())
    manifest = _object((PACKAGE_ROOT / "package_manifest.json").read_bytes())

    assert scope["content_sha256"] == _content_sha256(scope)
    assert manifest["content_sha256"] == _content_sha256(manifest)
    assert scope["authorization_status"] == "OWNER_DECISION_NOT_ADMITTED"
    assert scope["external_action_performed"] is False
    assert scope["current_external_counters"] == {
        "cloud_backtests": 0,
        "fills": 0,
        "orders": 0,
        "project_mutations": 0,
    }
    assert scope["maximum_external_effects_after_exact_owner_admission"] == {
        "cloud_backtests": 1,
        "fills": 0,
        "orders": 0,
        "project_mutations": 1,
    }
    assert scope["ordinary_pushed_proposal_main_sha"] == (
        "PENDING_PROPOSAL_PUBLICATION"
    )
    assert scope["expected_session_count"] == 1202
    assert scope["target_project_id"] == 34808569
    assert scope["requested_range"] == "2021-02-22..2025-12-02"
    frozen = scope["frozen_inputs"]
    assert frozen == {
        "contract_canonical_sha256": (
            "97557122d50f6a82fe68f57286f7008bbe8bbdb511886f62f936d9fc1b6bb7e4"
        ),
        "contract_content_sha256": (
            "f3c3918dd5dfd6fc1c6e84b63471c652d34090c9d50fab25d77dc58f9190b378"
        ),
        "policy_canonical_sha256": (
            "adc2e9cc0c889b814a97a5b8c4841c0890ef73c27dc07eddddc98ed2bed26f22"
        ),
        "policy_file_sha256": "cea137e0cb17b1c9594c359926015189f6fcfc2f472c4b6db72357d67a5d0cf5",
        "predecessor_evidence_content_sha256": (
            "d47f3234f58e1a7114984a7a79a5090082f923b7e02c65a66dfa8b761321f792"
        ),
        "predecessor_results_sha256": (
            "2233b20a900c76cbb6938a96c635c5dabc5855349ac74ff684c8f1c657b752b7"
        ),
        "project_code_lf_byte_count": 26901,
        "project_code_lf_sha256": (
            "0665a759a9db9bcae100133da9dd950e7f66597d4f19d00f01b26afb6a478f45"
        ),
        "trading_2531_package_content_sha256": (
            "1f018f42b1149f5c04b559e3ca1b35e0418c841a75da6a6099dbff7ec67d1b4b"
        ),
    }
    assert frozen["policy_file_sha256"] == source.policy_file_sha256
    assert frozen["policy_canonical_sha256"] == source.policy_canonical_sha256
    assert frozen["contract_content_sha256"] == source.contract["content_sha256"]
    assert frozen["project_code_lf_sha256"] == hashlib.sha256(
        source.project_code_bytes
    ).hexdigest()
    assert manifest["external_action_authorized"] is False
    assert manifest["external_action_performed"] is False
    assert manifest["production_effect"] == manifest["broker_action"] == "none"


def test_manifest_covers_only_scope_and_non_authorizing_owner_request() -> None:
    manifest = _object((PACKAGE_ROOT / "package_manifest.json").read_bytes())
    artifacts = {item["relative_path"]: item for item in manifest["artifacts"]}
    assert set(artifacts) == {"execution_scope.json", "owner_decision_request.md"}
    assert manifest["artifact_count"] == len(artifacts) == 2
    for relative_path, identity in artifacts.items():
        raw = (PACKAGE_ROOT / relative_path).read_bytes()
        assert identity["byte_count"] == len(raw)
        assert identity["sha256"] == hashlib.sha256(raw).hexdigest()

    request = (PACKAGE_ROOT / "owner_decision_request.md").read_text(encoding="utf-8")
    assert "本文件不是 QuantConnect 执行授权" in request
    assert "<ORDINARY_PUSHED_PROPOSAL_MAIN_SHA>" in request
    assert "<FINAL_TRADING_2532_PROPOSAL_PACKAGE_CONTENT_SHA256>" in request
    assert "<OWNER_SELECTED_EXPIRY_NOT_MORE_THAN_168_HOURS>" in request
    assert "maximum_project_mutations:1" in request
    assert "maximum_cloud_backtests:1" in request
    assert "maximum_orders:0" in request
    assert "maximum_fills:0" in request


def test_proposal_package_contains_no_runtime_or_execution_receipt() -> None:
    assert {path.name for path in PACKAGE_ROOT.iterdir()} == {
        "execution_scope.json",
        "owner_decision_request.md",
        "package_manifest.json",
    }
    assert not any(
        name in {path.name for path in PACKAGE_ROOT.iterdir()}
        for name in (
            "authorization_admission.json",
            "external_action_ledger.json",
            "run_attempt_consumption_receipt.json",
            "export_safe_aggregate_evidence.json",
        )
    )
