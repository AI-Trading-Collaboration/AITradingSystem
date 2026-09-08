"""Synthetic S4 contract checks; no market, provider, cache or research execution."""

from __future__ import annotations

import hashlib
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, date, datetime

import pytest

from ai_trading_system.contracts.prospective_event_time_evidence import (
    TemporalEvidenceError,
    canonical_json_bytes,
)
from ai_trading_system.contracts.research_experiment_envelope import (
    REQUIRED_POLICY_RULES,
    ExperimentContractError,
    ExperimentEnvelope,
    ExperimentPolicy,
    FreezeAdmission,
    LocalExperimentAuthority,
    OutcomeExposure,
    OutcomeInterval,
)


def make_envelope(*, look_mode: str = "FIRST_ONLY") -> ExperimentEnvelope:
    """Reusable fixture with explicit synthetic rules, never investment defaults."""
    hypothesis = "Synthetic outcome bytes match the predeclared fixture."
    policies = []
    for role, keys in sorted(REQUIRED_POLICY_RULES.items()):
        rules = {key: f"Synthetic {role}/{key}; fixture only." for key in keys}
        if role == "hypothesis":
            rules["statement"] = hypothesis
        if role == "look":
            rules["mode"] = look_mode
        policies.append(
            ExperimentPolicy(
                role=role,
                policy_id=f"synthetic-{role}",
                version="v1",
                owner="synthetic-owner",
                review_reference="fixture-policy-review",
                rationale="Synthetic contract verification.",
                rules=tuple(sorted(rules.items())),
            )
        )
    return ExperimentEnvelope(
        envelope_id="fixture-envelope",
        family_id="fixture-family",
        hypothesis=hypothesis,
        candidate_id="fixture-candidate",
        candidate_parameters_sha256="1" * 64,
        implementation_sha256="2" * 64,
        input_information_set_sha256="3" * 64,
        domain_id="synthetic-outcome-domain",
        requested_interval=OutcomeInterval(date(2026, 1, 1), date(2026, 1, 31)),
        evaluated_interval=OutcomeInterval(date(2026, 1, 2), date(2026, 1, 30)),
        data_role="PROSPECTIVE_UNTOUCHED",
        policies=tuple(policies),
    )


def make_authority(envelope: ExperimentEnvelope) -> LocalExperimentAuthority:
    return LocalExperimentAuthority(
        authority_id="synthetic-authority",
        version="v1",
        owner="synthetic-owner",
        owner_review_reference="fixture-exact-envelope-review",
        domain_id=envelope.domain_id,
        domain_definition_sha256="4" * 64,
        canonical_execution_root="D:/synthetic-workspace",
        canonical_git_common_dir="D:/synthetic-source/.git",
        ledger_relative_path="outputs/synthetic-outcome-ledger",
        genesis_sha256="5" * 64,
        history_status="KNOWN_COMPLETE",
        prior_exposures=(),
        approved_policy_sha256s=tuple(policy.canonical_sha256() for policy in envelope.policies),
        approved_envelope_sha256s=(envelope.canonical_sha256(),),
    )


def make_admission(
    envelope: ExperimentEnvelope, authority: LocalExperimentAuthority
) -> FreezeAdmission:
    return FreezeAdmission(
        envelope_sha256=envelope.canonical_sha256(),
        authority_sha256=authority.canonical_sha256(),
        domain_id=authority.domain_id,
        owner_review_reference=authority.owner_review_reference,
        reviewed_at=datetime(2025, 12, 31, tzinfo=UTC),
        time_evidence_sha256="6" * 64,
    )


def test_exact_envelope_policy_and_review_bindings_round_trip() -> None:
    envelope = make_envelope()
    authority = make_authority(envelope)
    admission = make_admission(envelope, authority)
    records = (
        envelope,
        authority,
        admission,
        envelope.policy("look"),
        envelope.requested_interval,
        OutcomeExposure(envelope.requested_interval, "PARTIAL", "fixture-prior-result"),
    )
    for record in records:
        restored = type(record).from_json_bytes(record.canonical_bytes())
        assert restored.canonical_bytes() == record.canonical_bytes()
        assert record.canonical_sha256() == hashlib.sha256(record.canonical_bytes()).hexdigest()
    admission.validate_bindings(envelope, authority)
    assert envelope.look_mode == "FIRST_ONLY"
    assert not authority.prior_history_blocks_first_access(envelope.requested_interval)


@pytest.mark.parametrize("extra", ["reviewed", "frozen_at", "result_visibility", "trial_count"])
def test_self_reported_review_and_history_fields_are_rejected(extra: str) -> None:
    envelope = make_envelope()
    authority = make_authority(envelope)
    for record in (
        envelope,
        authority,
        make_admission(envelope, authority),
        envelope.policy("look"),
    ):
        row = record.to_dict()
        row[extra] = True
        with pytest.raises(ExperimentContractError, match="missing or unknown"):
            type(record).from_dict(row)


@pytest.mark.parametrize(
    "record_name", ["envelope", "authority", "admission", "policy", "interval", "exposure"]
)
@pytest.mark.parametrize("schema", [None, True, 1, "legacy.v0"])
def test_unknown_schema_and_coercions_are_not_accepted(record_name: str, schema: object) -> None:
    envelope = make_envelope()
    authority = make_authority(envelope)
    record = {
        "envelope": envelope,
        "authority": authority,
        "admission": make_admission(envelope, authority),
        "policy": envelope.policy("look"),
        "interval": envelope.requested_interval,
        "exposure": OutcomeExposure(envelope.requested_interval, "KNOWN", "fixture-history"),
    }[record_name]
    row = record.to_dict()
    row["schema_version"] = schema
    with pytest.raises(ExperimentContractError):
        type(record).from_dict(row)


@pytest.mark.parametrize(
    "content",
    [
        b'{"a":1,"a":2}',
        b'{"a":{"nested":1,"nested":2}}',
        b'{"a":NaN}',
        b'{"a":Infinity}',
        b'{"a":1e999}',
        b'"\xff"',
        b'"\\ud800"',
        b"{} trailing",
    ],
)
def test_duplicate_nonfinite_and_invalid_json_rejected(content: bytes) -> None:
    with pytest.raises(TemporalEvidenceError):
        ExperimentEnvelope.from_json_bytes(content)


@pytest.mark.parametrize(
    "field,value",
    [
        ("envelope_id", True),
        ("family_id", 1),
        ("candidate_parameters_sha256", "A" * 64),
        ("input_information_set_sha256", None),
        ("data_role", "NONE"),
        ("primary_window_start", "2022-12-01"),
        ("scope", "REAL_RESEARCH"),
    ],
)
def test_envelope_closed_types_and_scope(field: str, value: object) -> None:
    row = make_envelope().to_dict()
    row[field] = value
    with pytest.raises(ExperimentContractError):
        ExperimentEnvelope.from_dict(row)


@pytest.mark.parametrize(
    "field,value",
    [
        ("envelope_id", "renamed-experiment"),
        ("family_id", "renamed-family"),
        ("candidate_id", "renamed-candidate"),
        ("candidate_parameters_sha256", "7" * 64),
        ("implementation_sha256", "8" * 64),
        ("input_information_set_sha256", "9" * 64),
    ],
)
def test_alias_parameter_code_and_revision_changes_need_exact_review(
    field: str, value: str
) -> None:
    original = make_envelope()
    authority = make_authority(original)
    row = original.to_dict()
    row[field] = value
    changed = ExperimentEnvelope.from_dict(row)
    with pytest.raises(ExperimentContractError, match="EXPERIMENT_ENVELOPE_NOT_APPROVED"):
        authority.validate_envelope(changed)


def test_opaque_policy_approval_cannot_replace_substantive_content() -> None:
    policy = make_envelope().policy("accounting")
    for rules in ({}, {"status": "REVIEWED"}, {"cost_rule": "some reviewed policy"}):
        row = policy.to_dict()
        row["rules"] = rules
        with pytest.raises(ExperimentContractError, match="substantive"):
            ExperimentPolicy.from_dict(row)
    with pytest.raises(ExperimentContractError, match="unique"):
        replace(policy, rules=policy.rules + (policy.rules[0],))


def test_policy_content_changes_require_independent_approval_even_with_envelope_approved() -> None:
    envelope = make_envelope()
    authority = make_authority(envelope)
    changed_policy = replace(
        envelope.policy("accounting"), rationale="Changed synthetic rationale."
    )
    changed = replace(
        envelope,
        policies=tuple(
            changed_policy if policy.role == "accounting" else policy
            for policy in envelope.policies
        ),
    )
    envelope_only_approval = replace(
        authority, approved_envelope_sha256s=(changed.canonical_sha256(),)
    )
    with pytest.raises(ExperimentContractError, match="EXPERIMENT_POLICY_NOT_APPROVED"):
        envelope_only_approval.validate_envelope(changed)


def test_required_policy_roles_and_hypothesis_cannot_drift() -> None:
    envelope = make_envelope()
    for policies in (envelope.policies[:-1], envelope.policies + (envelope.policies[0],)):
        with pytest.raises(ExperimentContractError, match="exactly once"):
            replace(envelope, policies=policies)
    with pytest.raises(ExperimentContractError, match="hypothesis"):
        replace(envelope, hypothesis="A different research question.")


def test_look_mode_is_explicit_reviewed_policy_without_numeric_defaults() -> None:
    envelope = make_envelope(look_mode="REPEAT_DECLARED")
    assert envelope.look_mode == "REPEAT_DECLARED"
    with pytest.raises(ExperimentContractError, match="look mode"):
        make_envelope(look_mode="UNLIMITED")


@pytest.mark.parametrize("visibility", ["KNOWN", "PARTIAL", "POSSIBLY_EXPOSED", "UNKNOWN"])
def test_all_prior_exposure_states_block_inclusive_first_access(visibility: str) -> None:
    envelope = make_envelope()
    prior = OutcomeExposure(
        OutcomeInterval(date(2025, 12, 1), date(2026, 1, 1)), visibility, "previous-failed-variant"
    )
    authority = replace(make_authority(envelope), prior_exposures=(prior,))
    assert authority.prior_history_blocks_first_access(envelope.requested_interval)
    assert not authority.prior_history_blocks_first_access(
        OutcomeInterval(date(2026, 2, 1), date(2026, 2, 28))
    )


def test_unknown_global_history_is_never_upgraded_by_empty_inventory() -> None:
    envelope = make_envelope()
    authority = replace(make_authority(envelope), history_status="UNKNOWN", prior_exposures=())
    assert authority.prior_history_blocks_first_access(envelope.requested_interval)
    assert authority.prior_history_blocks_first_access(
        OutcomeInterval(date(2099, 1, 1), date(2099, 12, 31))
    )
    with pytest.raises(ExperimentContractError):
        OutcomeExposure(envelope.requested_interval, "NONE", "unverified-untouched-claim")


@pytest.mark.parametrize(
    "field,value",
    [
        ("envelope_sha256", "a" * 64),
        ("authority_sha256", "b" * 64),
        ("domain_id", "alias-domain"),
        ("owner_review_reference", "arbitrary-review-string"),
    ],
)
def test_admission_must_bind_exact_independent_authority(field: str, value: str) -> None:
    envelope = make_envelope()
    authority = make_authority(envelope)
    row = make_admission(envelope, authority).to_dict()
    row[field] = value
    with pytest.raises(ExperimentContractError, match="EXPERIMENT_ADMISSION_MISMATCH"):
        FreezeAdmission.from_dict(row).validate_bindings(envelope, authority)


@pytest.mark.parametrize(
    "field,value",
    [
        ("reviewed_at", "2025-12-31T00:00:00"),
        ("reviewed_at", "2025-12-31T00:00:00-00:00"),
        ("reviewed_at", True),
        ("time_evidence_level", "EXTERNALLY_WITNESSED"),
        ("time_evidence_sha256", ""),
        ("scope", "OOS_AUTHORIZED"),
    ],
)
def test_local_time_never_claims_external_witness_or_execution_permission(
    field: str, value: object
) -> None:
    envelope = make_envelope()
    row = make_admission(envelope, make_authority(envelope)).to_dict()
    row[field] = value
    with pytest.raises((ExperimentContractError, TemporalEvidenceError)):
        FreezeAdmission.from_dict(row)


@pytest.mark.parametrize(
    "path",
    [
        "../fresh",
        "outputs/../fresh",
        "/tmp/fresh",
        "D:/fresh",
        "outputs\\fresh",
        "outputs//fresh",
        "outputs/fresh.",
        "outputs/con",
        "outputs/aux.txt",
        "outputs/ fresh",
        "outputs/fresh ",
    ],
)
def test_ledger_paths_cannot_escape_or_alias_windows_locations(path: str) -> None:
    with pytest.raises(ExperimentContractError):
        replace(make_authority(make_envelope()), ledger_relative_path=path)


@pytest.mark.parametrize(
    "path", ["relative/root", "D:\\root", "D:/root/../fresh", "//host/share", "/tmp/./fresh"]
)
def test_authority_root_identity_requires_exact_absolute_paths(path: str) -> None:
    authority = make_authority(make_envelope())
    for field in ("canonical_execution_root", "canonical_git_common_dir"):
        row = authority.to_dict()
        row[field] = path
        with pytest.raises(ExperimentContractError):
            LocalExperimentAuthority.from_dict(row)


def test_authority_root_genesis_and_history_are_part_of_admission_identity() -> None:
    envelope = make_envelope()
    authority = make_authority(envelope)
    admission = make_admission(envelope, authority)
    variants = (
        replace(authority, canonical_execution_root="D:/copied-workspace"),
        replace(authority, canonical_git_common_dir="D:/other-source/.git"),
        replace(authority, genesis_sha256="a" * 64),
        replace(authority, ledger_relative_path="outputs/fresh-ledger"),
        replace(authority, history_status="UNKNOWN"),
    )
    for changed in variants:
        with pytest.raises(ExperimentContractError, match="EXPERIMENT_ADMISSION_MISMATCH"):
            admission.validate_bindings(envelope, changed)


def test_dates_intervals_and_primary_window_are_exact() -> None:
    with pytest.raises(ExperimentContractError):
        OutcomeInterval(datetime(2026, 1, 1, tzinfo=UTC), date(2026, 1, 31))
    with pytest.raises(ExperimentContractError):
        OutcomeInterval(date(2026, 1, 31), date(2026, 1, 1))
    envelope = make_envelope()
    for interval in (
        OutcomeInterval(date(2026, 1, 1), date(2026, 2, 1)),
        OutcomeInterval(date(2025, 12, 31), date(2026, 1, 30)),
    ):
        with pytest.raises(ExperimentContractError, match="inside"):
            replace(envelope, evaluated_interval=interval)
    with pytest.raises(ExperimentContractError, match="primary-window"):
        replace(envelope, requested_interval=OutcomeInterval(date(2021, 2, 21), date(2026, 1, 31)))


def test_direct_construction_and_parsed_values_remain_immutable() -> None:
    envelope = make_envelope()
    with pytest.raises(FrozenInstanceError):
        envelope.candidate_id = "mutation"  # type: ignore[misc]
    with pytest.raises(ExperimentContractError, match="immutable tuple"):
        replace(envelope, policies=list(envelope.policies))  # type: ignore[arg-type]
    row = envelope.to_dict()
    detached = ExperimentEnvelope.from_json_bytes(canonical_json_bytes(row))
    row["candidate_id"] = "mutated-external-dict"
    assert detached.candidate_id == envelope.candidate_id
