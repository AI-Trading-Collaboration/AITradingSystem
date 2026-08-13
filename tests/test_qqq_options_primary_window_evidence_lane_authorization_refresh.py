from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest

from ai_trading_system.qqq_options_research import (
    primary_window_evidence_lane_authorization_refresh as refresh_v1,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_NAMES = ("owner_decision_request.md", "package_manifest.json")
PACKAGE_ROOT = Path(
    "inputs/research/qqq_options/"
    "trading_2516_primary_window_evidence_lane_authorization_refresh_v1"
)
EXPECTED_MAIN = "a" * 40


def _owner_decision_bytes(
    *,
    expiry: str = "2026-08-19T00:00:00Z",
    main_sha: str = EXPECTED_MAIN,
) -> bytes:
    package = (
        refresh_v1.load_qc_qqq_options_primary_window_evidence_lane_authorization_refresh_package(
            project_root=PROJECT_ROOT
        )
    )
    loaded = package.policy_load
    policy = loaded.policy
    upstream = policy.upstream_authority
    manifest_path = PROJECT_ROOT / PACKAGE_ROOT / "package_manifest.json"
    fields = (
        ("ordinary_pushed_main_sha", main_sha),
        ("refresh_policy_file_sha256", loaded.policy_file_sha256),
        ("refresh_policy_canonical_sha256", loaded.policy_canonical_sha256),
        (
            "refresh_package_manifest_file_sha256",
            hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
        ),
        (
            "refresh_package_manifest_content_sha256",
            package.manifest.content_sha256,
        ),
        ("proposal_content_sha256", upstream.proposal_content_sha256),
        ("run_scope_content_sha256", upstream.run_scope_content_sha256),
        ("project_code_lf_sha256", upstream.project_code_lf_sha256),
        ("proposal_policy_file_sha256", upstream.proposal_policy_file_sha256),
        (
            "proposal_policy_canonical_sha256",
            upstream.proposal_policy_canonical_sha256,
        ),
        ("collector_policy_file_sha256", upstream.collector_policy_file_sha256),
        (
            "collector_policy_canonical_sha256",
            upstream.collector_policy_canonical_sha256,
        ),
        ("transport_map_sha256", upstream.transport_map_sha256),
        ("admission_policy_file_sha256", upstream.admission_policy_file_sha256),
        (
            "admission_policy_canonical_sha256",
            upstream.admission_policy_canonical_sha256,
        ),
        ("target_project_id", str(policy.target_project_id)),
        (
            "requested_range",
            f"{policy.requested_start.isoformat()}..{policy.requested_end.isoformat()}",
        ),
        ("expected_session_count", str(policy.expected_session_count)),
        ("maximum_project_mutations", "1"),
        ("maximum_cloud_backtests", "1"),
        ("maximum_orders", "0"),
        ("maximum_fills", "0"),
        ("collector", policy.collector_id),
        ("independent_reviewer", policy.independent_reviewer_id),
        ("authorization_expires_at_utc", expiry),
        ("authorization_single_use", "true"),
        ("authorization_invalidates_after_evidence_collection", "true"),
    )
    return (
        "\n".join((policy.decision_token, *(f"{key}:{value}" for key, value in fields)))
        + "\n"
    ).encode("utf-8")


@pytest.fixture
def sandbox_package(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[
    tuple[
        Path,
        Path,
        refresh_v1.BuiltQCQQQOptionsEvidenceLaneAuthorizationRefreshPackage,
    ]
]:
    built = (
        refresh_v1.build_qc_qqq_options_primary_window_evidence_lane_authorization_refresh_package(
            project_root=PROJECT_ROOT
        )
    )
    root = tmp_path / "repository"
    package_root = root / PACKAGE_ROOT
    package_root.mkdir(parents=True)
    (package_root / "owner_decision_request.md").write_bytes(
        built.owner_decision_request_bytes
    )
    (package_root / "package_manifest.json").write_bytes(built.manifest.canonical_bytes)

    def expected_builder(
        *, project_root: Path = PROJECT_ROOT
    ) -> refresh_v1.BuiltQCQQQOptionsEvidenceLaneAuthorizationRefreshPackage:
        del project_root
        return built

    monkeypatch.setattr(
        refresh_v1,
        "build_qc_qqq_options_primary_window_evidence_lane_authorization_refresh_package",
        expected_builder,
    )
    yield root, package_root, built


def test_policy_loads_and_exactly_binds_2513_2514_authority() -> None:
    loaded = (
        refresh_v1.load_qc_qqq_options_primary_window_evidence_lane_authorization_refresh_policy(
            project_root=PROJECT_ROOT
        )
    )
    policy = loaded.policy

    assert loaded.policy_file_sha256 == (
        "4aa2983a6cb6c0ac02d03d18a807ea3bdf553770ac545130011911bf83caca77"
    )
    assert loaded.policy_canonical_sha256 == (
        "acd849fd8189256d4908cc162eb0c9bfe4162c669760577f21d6c960919b4882"
    )
    assert policy.registration_base_repository_code_sha == (
        "65b2bc1c88bf98132b7f6d58359ae3f18cea85f9"
    )
    assert policy.upstream_authority.proposal_package_manifest_content_sha256 == (
        "b44de8a0854cde6004f71ac2ed86cc619ab6c12c81b07f5efe790dad74219d58"
    )
    assert policy.upstream_authority.admission_policy_canonical_sha256 == (
        "a4e399ea022c04b579bbaaeb12bdc922e332ceb1badb0e4ba9740f17e11f824a"
    )


def test_refresh_package_is_deterministic_and_golden() -> None:
    first = (
        refresh_v1.build_qc_qqq_options_primary_window_evidence_lane_authorization_refresh_package(
            project_root=PROJECT_ROOT
        )
    )
    second = (
        refresh_v1.build_qc_qqq_options_primary_window_evidence_lane_authorization_refresh_package(
            project_root=PROJECT_ROOT
        )
    )

    assert first == second
    assert first.manifest.content_sha256 == (
        "0978dceaefb1acec33e2da2681075128c880d19ce4b01a194a7b38961f943381"
    )
    assert first.manifest.canonical_sha256 == (
        "7373474ee0279f70dcc678f6325935c82e96b90e5e46da82613bb8fcb106d924"
    )
    assert first.manifest.maximum_orders == first.manifest.maximum_fills == 0
    assert first.manifest.external_action_performed is False
    assert first.manifest.selection_authorized is False
    assert first.manifest.engine_status == "POLICY_BLOCKED_CASH_PRESERVATION"


def test_owner_request_uses_fresh_unsigned_token_and_exact_scope() -> None:
    built = (
        refresh_v1.build_qc_qqq_options_primary_window_evidence_lane_authorization_refresh_package(
            project_root=PROJECT_ROOT
        )
    )
    text = built.owner_decision_request_bytes.decode("utf-8")

    assert "owner_decision:TRADING-2516:2026-08-13:" in text
    assert "authorize_single_zero_order_primary_window_derived_aggregate_collection_v2" in text
    assert "ordinary_pushed_main_sha:<ORDINARY_PUSHED_2516_MAIN_SHA>" in text
    assert "authorization_expires_at_utc:<OWNER_SELECTED_EXPIRY_NOT_MORE_THAN_168_HOURS>" in text
    assert "requested/evaluated range：`2021-02-22..2025-12-02`" in text
    assert "maximum orders / fills：`0` / `0`" in text
    assert "owner_decision:TRADING-2513:2026-08-12" not in text
    assert "OWNER_AUTHORIZATION_REQUIRED_FRESH_TOKEN" in text


def test_repository_package_inventory_and_loader_are_exact() -> None:
    package_root = PROJECT_ROOT / PACKAGE_ROOT
    assert tuple(sorted(path.name for path in package_root.iterdir())) == PACKAGE_NAMES

    loaded = (
        refresh_v1.load_qc_qqq_options_primary_window_evidence_lane_authorization_refresh_package(
            project_root=PROJECT_ROOT
        )
    )
    assert loaded.manifest.canonical_sha256 == (
        "7373474ee0279f70dcc678f6325935c82e96b90e5e46da82613bb8fcb106d924"
    )
    artifact = loaded.manifest.artifacts[0]
    request = (package_root / artifact.relative_path).read_bytes()
    assert len(request) == artifact.byte_count
    assert hashlib.sha256(request).hexdigest() == artifact.sha256


@pytest.mark.parametrize("name", PACKAGE_NAMES)
def test_loader_rejects_each_tampered_artifact(
    sandbox_package: tuple[
        Path,
        Path,
        refresh_v1.BuiltQCQQQOptionsEvidenceLaneAuthorizationRefreshPackage,
    ],
    name: str,
) -> None:
    root, package_root, _ = sandbox_package
    target = package_root / name
    target.write_bytes(target.read_bytes() + b"\n")

    with pytest.raises(
        refresh_v1.QCQQQOptionsEvidenceLaneAuthorizationRefreshError,
        match="AUTHORIZATION_REFRESH_PACKAGE_REJECTED",
    ):
        refresh_v1.load_qc_qqq_options_primary_window_evidence_lane_authorization_refresh_package(
            project_root=root
        )


@pytest.mark.parametrize("mutation", ("extra", "missing"))
def test_loader_rejects_nonexact_inventory(
    sandbox_package: tuple[
        Path,
        Path,
        refresh_v1.BuiltQCQQQOptionsEvidenceLaneAuthorizationRefreshPackage,
    ],
    mutation: str,
) -> None:
    root, package_root, _ = sandbox_package
    if mutation == "extra":
        (package_root / "extra.txt").write_text("unexpected\n", encoding="utf-8")
    else:
        (package_root / "owner_decision_request.md").unlink()

    with pytest.raises(refresh_v1.QCQQQOptionsEvidenceLaneAuthorizationRefreshError):
        refresh_v1.load_qc_qqq_options_primary_window_evidence_lane_authorization_refresh_package(
            project_root=root
        )


def test_loader_rejects_symlink_entry(
    sandbox_package: tuple[
        Path,
        Path,
        refresh_v1.BuiltQCQQQOptionsEvidenceLaneAuthorizationRefreshPackage,
    ],
) -> None:
    root, package_root, built = sandbox_package
    target = package_root / "owner_decision_request.md"
    target.unlink()
    source = package_root / "request-source.md"
    source.write_bytes(built.owner_decision_request_bytes)
    try:
        os.symlink(source, target)
    except OSError:
        pytest.skip("symlink creation is unavailable")
    source.unlink()

    with pytest.raises(refresh_v1.QCQQQOptionsEvidenceLaneAuthorizationRefreshError):
        refresh_v1.load_qc_qqq_options_primary_window_evidence_lane_authorization_refresh_package(
            project_root=root
        )


def test_owner_decision_candidate_validates_without_consuming_or_unblocking() -> None:
    candidate = refresh_v1.validate_qc_qqq_options_authorization_refresh_owner_decision_candidate(
        owner_decision_bytes=_owner_decision_bytes(),
        expected_ordinary_pushed_main_sha=EXPECTED_MAIN,
        reviewed_at_utc=datetime(2026, 8, 14, tzinfo=UTC),
        project_root=PROJECT_ROOT,
    )

    assert candidate.decision == "OWNER_AUTHORIZATION_REVIEWED_NOT_CONSUMED"
    assert candidate.authorization_consumed is False
    assert candidate.external_action_performed is False
    assert candidate.selection_authorized is False
    assert candidate.engine_status == "POLICY_BLOCKED_CASH_PRESERVATION"
    assert candidate.owner_policy_value_count == 0
    assert type(candidate).from_json_bytes(candidate.canonical_bytes) == candidate


def test_owner_decision_candidate_rejects_old_2513_token() -> None:
    raw = _owner_decision_bytes().replace(
        b"owner_decision:TRADING-2516:2026-08-13:",
        b"owner_decision:TRADING-2513:2026-08-12:",
        1,
    )

    with pytest.raises(
        refresh_v1.QCQQQOptionsEvidenceLaneAuthorizationRefreshError,
        match="fresh exact 2516 Owner decision token was not supplied",
    ):
        refresh_v1.validate_qc_qqq_options_authorization_refresh_owner_decision_candidate(
            owner_decision_bytes=raw,
            expected_ordinary_pushed_main_sha=EXPECTED_MAIN,
            reviewed_at_utc=datetime(2026, 8, 14, tzinfo=UTC),
            project_root=PROJECT_ROOT,
        )


@pytest.mark.parametrize(
    ("expiry", "reviewed_at", "pattern"),
    (
        (
            "2026-08-21T00:00:00Z",
            datetime(2026, 8, 14, tzinfo=UTC),
            "outside the reviewed <=168h window",
        ),
        (
            "2026-08-19T00:00:00Z",
            datetime(2026, 8, 12, 23, 59, tzinfo=UTC),
            "review as-of is outside",
        ),
        (
            "2026-08-19T00:00:00Z",
            datetime(2026, 8, 19, 0, 1, tzinfo=UTC),
            "review as-of is outside",
        ),
    ),
)
def test_owner_decision_candidate_rejects_expiry_and_asof_drift(
    expiry: str, reviewed_at: datetime, pattern: str
) -> None:
    with pytest.raises(
        refresh_v1.QCQQQOptionsEvidenceLaneAuthorizationRefreshError,
        match=pattern,
    ):
        refresh_v1.validate_qc_qqq_options_authorization_refresh_owner_decision_candidate(
            owner_decision_bytes=_owner_decision_bytes(expiry=expiry),
            expected_ordinary_pushed_main_sha=EXPECTED_MAIN,
            reviewed_at_utc=reviewed_at,
            project_root=PROJECT_ROOT,
        )


def test_owner_decision_candidate_rejects_binding_and_duplicate_key_tamper() -> None:
    raw = _owner_decision_bytes()
    mismatched = raw.replace(
        b"expected_session_count:1202", b"expected_session_count:1201", 1
    )
    duplicated = raw.replace(
        b"maximum_fills:0\n",
        b"maximum_fills:0\nmaximum_fills:0\n",
        1,
    )

    for payload in (mismatched, duplicated):
        with pytest.raises(
            refresh_v1.QCQQQOptionsEvidenceLaneAuthorizationRefreshError
        ):
            refresh_v1.validate_qc_qqq_options_authorization_refresh_owner_decision_candidate(
                owner_decision_bytes=payload,
                expected_ordinary_pushed_main_sha=EXPECTED_MAIN,
                reviewed_at_utc=datetime(2026, 8, 14, tzinfo=UTC),
                project_root=PROJECT_ROOT,
            )


def test_manifest_duplicate_key_and_checksum_tamper_fail_closed(
    sandbox_package: tuple[
        Path,
        Path,
        refresh_v1.BuiltQCQQQOptionsEvidenceLaneAuthorizationRefreshPackage,
    ],
) -> None:
    root, package_root, _ = sandbox_package
    target = package_root / "package_manifest.json"
    payload = json.loads(target.read_text(encoding="utf-8"))
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    target.write_text(raw[:-1] + ',"maximum_orders":0}\n', encoding="utf-8")

    with pytest.raises(refresh_v1.QCQQQOptionsEvidenceLaneAuthorizationRefreshError):
        refresh_v1.load_qc_qqq_options_primary_window_evidence_lane_authorization_refresh_package(
            project_root=root
        )
