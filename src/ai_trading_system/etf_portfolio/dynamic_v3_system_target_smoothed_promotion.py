from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime
from functools import wraps
from hashlib import sha256
from pathlib import Path
from typing import Any

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as legacy
from ai_trading_system.etf_portfolio import dynamic_v3_system_target_hardening as hardening
from ai_trading_system.etf_portfolio import dynamic_v3_system_target_portfolio as target_core
from ai_trading_system.etf_portfolio import dynamic_v3_system_target_smoothed_evidence as evidence
from ai_trading_system.etf_portfolio import dynamic_v3_system_target_smoothed_method as method
from ai_trading_system.etf_portfolio import dynamic_v3_system_target_smoothed_readiness as readiness
from ai_trading_system.etf_portfolio.dynamic_v3_confirmation_operations import (
    _file_bytes_match,
    _json_bytes,
)
from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    _check,
    _mapping,
    _read_json,
    _records,
    _stable_id,
    _text,
    _unique_dir,
    _update_latest_pointer,
)
from ai_trading_system.etf_portfolio.dynamic_v3_pressure_validation import (
    _write_views_atomic,
)
from ai_trading_system.platform.artifacts.writer import write_bytes_atomic

SMOOTHED_PROMOTION_REVIEW_SNAPSHOT_SCHEMA = "smoothed_promotion_review_input_snapshot.v2"
PRIMARY_RESEARCH_CANDIDATE_GATE_SNAPSHOT_SCHEMA = (
    "primary_research_candidate_gate_input_snapshot.v2"
)
SMOOTHED_FORWARD_BINDING_SNAPSHOT_SCHEMA = "smoothed_forward_binding_input_snapshot.v2"
PAPER_SHADOW_PRIMARY_SWITCH_SNAPSHOT_SCHEMA = "paper_shadow_primary_switch_input_snapshot.v2"
SMOOTHED_OWNER_PROMOTION_SNAPSHOT_SCHEMA = "smoothed_owner_promotion_input_snapshot.v2"

DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH = method.DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH
DEFAULT_SMOOTHED_PROMOTION_REVIEW_DIR = legacy.DEFAULT_SMOOTHED_PROMOTION_REVIEW_DIR
DEFAULT_PRIMARY_RESEARCH_CANDIDATE_GATE_DIR = legacy.DEFAULT_PRIMARY_RESEARCH_CANDIDATE_GATE_DIR
DEFAULT_SMOOTHED_FORWARD_BINDING_DIR = legacy.DEFAULT_SMOOTHED_FORWARD_BINDING_DIR
DEFAULT_PAPER_SHADOW_PRIMARY_SWITCH_DIR = legacy.DEFAULT_PAPER_SHADOW_PRIMARY_SWITCH_DIR
DEFAULT_SMOOTHED_OWNER_PROMOTION_DIR = legacy.DEFAULT_SMOOTHED_OWNER_PROMOTION_DIR
DEFAULT_SMOOTHED_READINESS_SCORECARD_DIR = readiness.DEFAULT_SMOOTHED_READINESS_SCORECARD_DIR
DEFAULT_SMOOTHED_OWNER_REVIEW_UPDATE_DIR = readiness.DEFAULT_SMOOTHED_OWNER_REVIEW_UPDATE_DIR
DEFAULT_SMOOTHED_WATCH_PACK_DIR = evidence.DEFAULT_SMOOTHED_WATCH_PACK_DIR
DEFAULT_SMOOTHED_FORWARD_CONFIRMATION_DIR = evidence.DEFAULT_SMOOTHED_FORWARD_CONFIRMATION_DIR
SYSTEM_TARGET_SAFETY = method.SYSTEM_TARGET_SAFETY
_VALIDATION_SESSION: ContextVar[dict[tuple[str, str, str], dict[str, Any]] | None] = ContextVar(
    "smoothed_promotion_validation_session", default=None
)


class DynamicV3SmoothedPromotionError(ValueError):
    """Raised when promotion lineage or policy cannot be reproduced exactly."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise DynamicV3SmoothedPromotionError(message)


def _generated_at(value: datetime | None) -> datetime:
    try:
        return target_core._generated_at(value)
    except Exception as exc:  # noqa: BLE001
        raise DynamicV3SmoothedPromotionError(str(exc)) from exc


def _validation_payload(
    report_type: str,
    artifact_id: str,
    checks: Sequence[Mapping[str, Any]],
    *,
    artifact_id_key: str,
) -> dict[str, Any]:
    return target_core._validation_payload(
        report_type,
        artifact_id,
        checks,
        artifact_id_key=artifact_id_key,
    )


def _write(root: Path, views: Mapping[str, bytes], pointer: str, manifest: str) -> None:
    _write_views_atomic(root, views)
    _update_latest_pointer(pointer, root.name, root / manifest)


def _rewrite_views(root: Path, views: Mapping[str, bytes]) -> None:
    _require(root.is_dir(), f"artifact directory missing: {root}")
    expected = set(views)
    actual = {path.name for path in root.iterdir() if path.is_file()}
    _require(actual == expected, "owner artifact file inventory drift")
    for name, payload in views.items():
        write_bytes_atomic(root / name, payload)


def _view_errors(root: Path, views: Mapping[str, bytes]) -> list[str]:
    return [name for name, payload in views.items() if not _file_bytes_match(root / name, payload)]


def _bundle_json(binding: Mapping[str, Any], name: str) -> dict[str, Any]:
    return hardening._bundle_json(binding, name)


def _artifact_fingerprint(root: Path) -> str:
    files = (
        {path.resolve() for path in root.iterdir() if path.is_file()} if root.is_dir() else set()
    )
    digest = sha256()
    for path in sorted(files, key=str):
        digest.update(str(path).encode("utf-8"))
        try:
            with path.open("rb") as handle:
                while chunk := handle.read(1024 * 1024):
                    digest.update(chunk)
        except OSError as exc:
            digest.update(f"MISSING:{exc}".encode())
    return digest.hexdigest()


@contextmanager
def smoothed_promotion_validation_session() -> Iterator[None]:
    current = _VALIDATION_SESSION.get()
    if current is not None:
        yield
        return
    token = _VALIDATION_SESSION.set({})
    try:
        yield
    finally:
        _VALIDATION_SESSION.reset(token)


def _with_validation_session(
    function: Callable[..., dict[str, Any]],
) -> Callable[..., dict[str, Any]]:
    @wraps(function)
    def wrapped(*args: Any, **kwargs: Any) -> dict[str, Any]:
        with smoothed_promotion_validation_session():
            return function(*args, **kwargs)

    return wrapped


def _cached_artifact_validation(
    *,
    validator: Callable[..., dict[str, Any]],
    validator_key: str,
    artifact_id: str,
    root: Path,
) -> dict[str, Any]:
    cache = _VALIDATION_SESSION.get()
    if cache is None:
        with smoothed_promotion_validation_session():
            return _cached_artifact_validation(
                validator=validator,
                validator_key=validator_key,
                artifact_id=artifact_id,
                root=root,
            )
    fingerprint = _artifact_fingerprint(root / artifact_id)
    name = f"{validator.__module__}.{validator.__qualname__}:{validator_key}"
    key = (name, str((root / artifact_id).resolve()), fingerprint)
    cached = cache.get(key)
    if cached is None:
        cached = validator(**{validator_key: artifact_id, "output_dir": root})
        cache[key] = dict(cached)
    return dict(cached)


def _source_binding(
    *,
    kind: str,
    artifact_id: str,
    root: Path,
    validator: Callable[..., dict[str, Any]],
    validator_key: str,
    json_views: Sequence[str],
    text_views: Sequence[str] = (),
) -> dict[str, Any]:
    # A downstream snapshot must not embed an upstream input snapshot.  Those
    # snapshots already contain their own source bundles, so copying them here
    # grows the chain exponentially.  The live upstream validator below still
    # validates the complete artifact (including its input snapshot), while the
    # bounded bundle freezes only the business views consumed by this stage.
    bounded_json_views = tuple(
        name for name in json_views if not name.endswith("_input_snapshot.json")
    )
    _require(bounded_json_views, f"{kind} bounded source views missing")

    def cached_validator(**_kwargs: Any) -> dict[str, Any]:
        return _cached_artifact_validation(
            validator=validator,
            validator_key=validator_key,
            artifact_id=artifact_id,
            root=root,
        )

    return readiness._source_binding(
        kind=kind,
        artifact_id=artifact_id,
        root=root,
        validator=cached_validator,
        validator_key=validator_key,
        json_views=bounded_json_views,
        text_views=text_views,
    )


def _validate_binding(
    binding: Mapping[str, Any],
    *,
    kind: str,
    validator: Callable[..., dict[str, Any]],
    validator_key: str,
) -> list[str]:
    artifact_id = _text(binding.get("artifact_id"))
    source_dir = Path(_text(_mapping(binding.get("bundle")).get("source_dir")))
    root = source_dir.parent
    validation = _cached_artifact_validation(
        validator=validator,
        validator_key=validator_key,
        artifact_id=artifact_id,
        root=root,
    )
    return readiness._validate_binding(
        binding,
        kind=kind,
        validator=lambda **_kwargs: validation,
        validator_key=validator_key,
    )


def _policy_binding(path: Path) -> dict[str, Any]:
    return target_core._config_binding(path, kind="smoothed_promotion_policy")


def _string_list(value: Any, *, name: str) -> list[str]:
    values = list(value) if isinstance(value, list) else []
    _require(values and all(isinstance(item, str) and item for item in values), f"{name} invalid")
    _require(len(values) == len(set(values)), f"{name} contains duplicates")
    return values


def _promotion_policy(config: Mapping[str, Any]) -> dict[str, Any]:
    target_core._policy_metadata(
        {"policy_metadata": _mapping(config.get("promotion_policy_metadata"))},
        name="smoothed promotion",
    )
    raw = _mapping(config.get("promotion_policy"))
    required = {
        "gate_scope",
        "current_primary_research_candidate",
        "rollback_method",
        "eligible_readiness_decisions",
        "forward_observation_statuses",
        "forward_pass_statuses",
        "owner_decisions",
        "candidate_required_for_owner_review",
        "candidate_required_for_switch",
    }
    _require(set(raw) == required, "smoothed promotion_policy fields must be exact")
    gate_scope = _text(raw.get("gate_scope"))
    current = _text(raw.get("current_primary_research_candidate"))
    rollback = _text(raw.get("rollback_method"))
    _require(gate_scope == "paper_shadow_research_only", "promotion gate_scope invalid")
    _require(bool(current) and bool(rollback), "promotion baseline methods invalid")
    eligible = _string_list(
        raw.get("eligible_readiness_decisions"), name="eligible_readiness_decisions"
    )
    observation = _string_list(
        raw.get("forward_observation_statuses"), name="forward_observation_statuses"
    )
    passed = _string_list(raw.get("forward_pass_statuses"), name="forward_pass_statuses")
    owner = _string_list(raw.get("owner_decisions"), name="owner_decisions")
    _require(eligible == ["PROMOTE_FOR_REVIEW"], "eligible readiness decisions invalid")
    _require(
        set(observation) == {"IN_PROGRESS", "WATCH_ONLY"}, "forward observation statuses invalid"
    )
    _require(passed == ["PASS"], "forward pass statuses invalid")
    _require(
        "pending" in owner and "promote_to_primary_research_candidate" in owner,
        "owner decisions incomplete",
    )
    _require(
        raw.get("candidate_required_for_owner_review") is True
        and raw.get("candidate_required_for_switch") is True,
        "promotion candidate requirements must be true",
    )
    return {
        "gate_scope": gate_scope,
        "current_primary_research_candidate": current,
        "rollback_method": rollback,
        "eligible_readiness_decisions": eligible,
        "forward_observation_statuses": observation,
        "forward_pass_statuses": passed,
        "owner_decisions": owner,
        "candidate_required_for_owner_review": True,
        "candidate_required_for_switch": True,
    }


def _policy(snapshot: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    binding = _mapping(snapshot.get("policy_binding"))
    config = _mapping(binding.get("payload"))
    return config, _promotion_policy(config)


def _validate_policy_binding(binding: Mapping[str, Any]) -> list[str]:
    errors = target_core._validate_config_binding(binding)
    try:
        _require(binding.get("kind") == "smoothed_promotion_policy", "policy binding kind invalid")
        config = _mapping(binding.get("payload"))
        method._evaluation_policy(config)
        evidence._evidence_policy(config)
        readiness._readiness_policy(config)
        _promotion_policy(config)
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def _scorecard_binding(artifact_id: str, root: Path) -> dict[str, Any]:
    return _source_binding(
        kind="smoothed_readiness_scorecard",
        artifact_id=artifact_id,
        root=root,
        validator=readiness.validate_smoothed_readiness_scorecard_artifact,
        validator_key="scorecard_id",
        json_views=(
            "smoothed_readiness_scorecard_input_snapshot.json",
            "smoothed_readiness_manifest.json",
            "smoothed_method_scorecard.json",
            "promotion_readiness_decision.json",
        ),
        text_views=("smoothed_readiness_scorecard_report.md",),
    )


def _owner_update_binding(artifact_id: str, root: Path) -> dict[str, Any]:
    return _source_binding(
        kind="smoothed_owner_review_update",
        artifact_id=artifact_id,
        root=root,
        validator=readiness.validate_smoothed_owner_review_update_artifact,
        validator_key="owner_update_id",
        json_views=(
            "smoothed_owner_review_update_input_snapshot.json",
            "smoothed_owner_update_manifest.json",
            "smoothed_owner_decision_options.json",
        ),
        text_views=(
            "smoothed_owner_review_checklist.md",
            "smoothed_owner_review_update_report.md",
            "reader_brief_section.md",
        ),
    )


def _watch_binding(artifact_id: str, root: Path) -> dict[str, Any]:
    return _source_binding(
        kind="smoothed_watch_pack",
        artifact_id=artifact_id,
        root=root,
        validator=evidence.validate_smoothed_watch_pack_artifact,
        validator_key="watch_pack_id",
        json_views=(
            "smoothed_watch_input_snapshot.json",
            "smoothed_watch_manifest.json",
            "smoothed_watch_summary.json",
        ),
        text_views=(
            "owner_smoothed_watch_checklist.md",
            "smoothed_watch_pack_report.md",
            "reader_brief_section.md",
        ),
    )


def _confirmation_binding(artifact_id: str, root: Path) -> dict[str, Any]:
    return _source_binding(
        kind="smoothed_confirmation",
        artifact_id=artifact_id,
        root=root,
        validator=evidence.validate_smoothed_confirmation_artifact,
        validator_key="confirmation_id",
        json_views=(
            "smoothed_confirmation_input_snapshot.json",
            "smoothed_confirmation_manifest.json",
            "smoothed_confirmation_targets.json",
        ),
        text_views=("smoothed_confirmation_report.md",),
    )


def _simple_payload(
    binding: Mapping[str, Any], manifest: str, views: Mapping[str, str]
) -> dict[str, Any]:
    return evidence._simple_payload(binding, manifest, views)


def _scorecard_payload(binding: Mapping[str, Any]) -> dict[str, Any]:
    return readiness._scorecard_payload(binding)


def _owner_update_payload(binding: Mapping[str, Any]) -> dict[str, Any]:
    return _simple_payload(
        binding,
        "smoothed_owner_update_manifest.json",
        {"smoothed_owner_decision_options": "smoothed_owner_decision_options.json"},
    )


def _watch_payload(binding: Mapping[str, Any]) -> dict[str, Any]:
    return readiness._watch_payload(binding)


def _confirmation_payload(binding: Mapping[str, Any]) -> dict[str, Any]:
    return _simple_payload(
        binding,
        "smoothed_confirmation_manifest.json",
        {"smoothed_confirmation_targets": "smoothed_confirmation_targets.json"},
    )


def _promotion_binding(artifact_id: str, root: Path) -> dict[str, Any]:
    return _source_binding(
        kind="smoothed_promotion_review",
        artifact_id=artifact_id,
        root=root,
        validator=validate_smoothed_promotion_review_artifact,
        validator_key="promotion_review_id",
        json_views=(
            "smoothed_promotion_review_input_snapshot.json",
            "smoothed_promotion_review_manifest.json",
            "promotion_evidence_summary.json",
            "promotion_blocking_issues.json",
        ),
        text_views=(
            "smoothed_promotion_review_report.md",
            "reader_brief_section.md",
        ),
    )


def _promotion_payload(binding: Mapping[str, Any]) -> dict[str, Any]:
    return _simple_payload(
        binding,
        "smoothed_promotion_review_manifest.json",
        {
            "promotion_evidence_summary": "promotion_evidence_summary.json",
            "promotion_blocking_issues": "promotion_blocking_issues.json",
        },
    )


def _gate_binding(artifact_id: str, root: Path) -> dict[str, Any]:
    return _source_binding(
        kind="primary_research_candidate_gate",
        artifact_id=artifact_id,
        root=root,
        validator=validate_primary_research_candidate_gate_artifact,
        validator_key="gate_id",
        json_views=(
            "primary_research_candidate_gate_input_snapshot.json",
            "primary_research_candidate_gate_manifest.json",
            "gate_decision.json",
            "gate_criteria_results.json",
        ),
        text_views=("primary_research_candidate_gate_report.md",),
    )


def _gate_payload(binding: Mapping[str, Any]) -> dict[str, Any]:
    return _simple_payload(
        binding,
        "primary_research_candidate_gate_manifest.json",
        {
            "gate_decision": "gate_decision.json",
            "gate_criteria_results": "gate_criteria_results.json",
        },
    )


def _binding_binding(artifact_id: str, root: Path) -> dict[str, Any]:
    return _source_binding(
        kind="smoothed_forward_binding",
        artifact_id=artifact_id,
        root=root,
        validator=validate_smoothed_forward_binding_artifact,
        validator_key="binding_id",
        json_views=(
            "smoothed_forward_binding_input_snapshot.json",
            "smoothed_forward_binding_manifest.json",
            "bound_confirmation_targets.json",
            "forward_progress_requirements.json",
        ),
        text_views=("smoothed_forward_binding_report.md", "reader_brief_section.md"),
    )


def _binding_payload(binding: Mapping[str, Any]) -> dict[str, Any]:
    return _simple_payload(
        binding,
        "smoothed_forward_binding_manifest.json",
        {
            "bound_confirmation_targets": "bound_confirmation_targets.json",
            "forward_progress_requirements": "forward_progress_requirements.json",
        },
    )


def _switch_binding(artifact_id: str, root: Path) -> dict[str, Any]:
    return _source_binding(
        kind="paper_shadow_primary_switch",
        artifact_id=artifact_id,
        root=root,
        validator=validate_paper_shadow_primary_switch_artifact,
        validator_key="switch_plan_id",
        json_views=(
            "paper_shadow_primary_switch_input_snapshot.json",
            "paper_shadow_primary_switch_manifest.json",
            "primary_switch_plan.json",
            "primary_switch_safety_checks.json",
        ),
        text_views=("paper_shadow_primary_switch_report.md", "reader_brief_section.md"),
    )


def _switch_payload(binding: Mapping[str, Any]) -> dict[str, Any]:
    return _simple_payload(
        binding,
        "paper_shadow_primary_switch_manifest.json",
        {
            "primary_switch_plan": "primary_switch_plan.json",
            "primary_switch_safety_checks": "primary_switch_safety_checks.json",
        },
    )


def _lineage(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: payload.get(key)
        for key in (
            "review_id",
            "comparison_id",
            "smoothed_backfill_id",
            "baseline_backfill_id",
            "confirmation_id",
        )
    }


def _promotion_evidence(
    scorecard: Mapping[str, Any],
    owner_update: Mapping[str, Any],
    watch: Mapping[str, Any],
) -> dict[str, Any]:
    decision = _mapping(scorecard.get("promotion_readiness_decision"))
    scorecard_body = _mapping(scorecard.get("smoothed_method_scorecard"))
    owner_options = _mapping(owner_update.get("smoothed_owner_decision_options"))
    watch_summary = _mapping(watch.get("smoothed_watch_summary"))
    candidate = decision.get("recommended_method")
    _require(
        candidate
        == scorecard_body.get("candidate_method")
        == owner_options.get("candidate_method")
        == watch_summary.get("candidate_method"),
        "promotion candidate lineage mismatch",
    )
    _require(
        candidate is None or candidate in readiness.SMOOTHED_METHOD_TO_VARIANT,
        "promotion candidate invalid",
    )
    method_rows = [
        row for row in _records(scorecard_body.get("methods")) if row.get("method") == candidate
    ]
    _require(
        (candidate is None and not method_rows)
        or (candidate is not None and len(method_rows) == 1),
        "promotion candidate scorecard row invalid",
    )
    row = method_rows[0] if method_rows else {}
    supporting: list[dict[str, Any]] = []
    for name, value in _mapping(row.get("source_statuses")).items():
        supporting.append(
            {
                "evidence_id": f"candidate_source_status_{name}",
                "source": "smoothed_method_scorecard.source_statuses",
                "metric": name,
                "actual": value,
                "supports_promotion_review": value
                not in {None, "", "INSUFFICIENT_DATA", "INSUFFICIENT_EVIDENCE", "NOT_REGISTERED"},
                **SYSTEM_TARGET_SAFETY,
            }
        )
    for name, value in row.items():
        if name.endswith("_score"):
            supporting.append(
                {
                    "evidence_id": f"candidate_score_{name}",
                    "source": "smoothed_method_scorecard",
                    "metric": name,
                    "actual": value,
                    "supports_promotion_review": value is not None,
                    **SYSTEM_TARGET_SAFETY,
                }
            )
    return {
        "schema_version": 2,
        "candidate_method": candidate,
        "secondary_method": decision.get("secondary_method"),
        "candidate_source": decision.get("candidate_source"),
        "readiness_decision": _text(decision.get("decision"), "CONTINUE_OBSERVATION"),
        "readiness_evidence_status": decision.get("evidence_status"),
        "decision_confidence": _text(decision.get("confidence"), "LOW"),
        "recommended_owner_action": owner_options.get("recommended_owner_action"),
        "forward_confirmation_status": _text(
            _mapping(row.get("source_statuses")).get("forward_confirmation"),
            _text(watch_summary.get("forward_confirmation_status"), "NOT_REGISTERED"),
        ),
        "overall_readiness_score": row.get("overall_readiness_score"),
        "missing_score_components": list(row.get("missing_score_components") or []),
        "supporting_evidence": supporting,
        **SYSTEM_TARGET_SAFETY,
    }


def _promotion_blocking(
    scorecard: Mapping[str, Any],
    evidence_summary: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    decision = _mapping(scorecard.get("promotion_readiness_decision"))
    candidate = evidence_summary.get("candidate_method")
    readiness_decision = _text(evidence_summary.get("readiness_decision"))
    issues: list[dict[str, Any]] = []

    def add(issue: str, *, blocks_candidate: bool, reason: str, severity: str) -> None:
        if issue not in {row.get("issue") for row in issues}:
            issues.append(
                {
                    "issue": issue,
                    "severity": severity,
                    "blocks_official_promotion": True,
                    "blocks_paper_shadow_primary_candidate": blocks_candidate,
                    "reason": reason,
                    **SYSTEM_TARGET_SAFETY,
                }
            )

    if candidate is None:
        add(
            "no_eligible_candidate",
            blocks_candidate=True,
            severity="BLOCKING",
            reason="Validated confirmation and readiness artifacts contain no candidate.",
        )
    if readiness_decision not in policy["eligible_readiness_decisions"]:
        add(
            "readiness_decision_not_eligible",
            blocks_candidate=True,
            severity="BLOCKING",
            reason=f"Readiness decision is {readiness_decision}.",
        )
    for raw_reason in list(decision.get("blocking_reasons") or []):
        reason = _text(raw_reason)
        if not reason:
            continue
        observation_only = reason == "forward_confirmation_in_progress"
        add(
            reason,
            blocks_candidate=not observation_only,
            severity="REVIEW_REQUIRED" if observation_only else "BLOCKING",
            reason="Content-derived readiness blocking or observation condition.",
        )
    if evidence_summary.get("decision_confidence") == "LOW":
        add(
            "decision_confidence_low",
            blocks_candidate=False,
            severity="WARNING",
            reason="Validated readiness confidence remains LOW.",
        )
    hard = [row for row in issues if row.get("blocks_paper_shadow_primary_candidate") is True]
    can_enter = (
        candidate is not None
        and readiness_decision in policy["eligible_readiness_decisions"]
        and not hard
    )
    return {
        "schema_version": 2,
        "blocking_issues": issues,
        "can_enter_owner_review": can_enter,
        "can_become_paper_shadow_primary_candidate": (
            "OWNER_DECISION_REQUIRED" if can_enter else "NOT_ELIGIBLE"
        ),
        "can_write_official_target_weights": False,
        "can_trigger_production": False,
        "automatic_promotion_allowed": False,
        **SYSTEM_TARGET_SAFETY,
    }


def _render_promotion_reader(
    evidence_summary: Mapping[str, Any], blocking: Mapping[str, Any]
) -> str:
    return "\n".join(
        [
            "## Dynamic Rescue Smoothed Promotion Review",
            "",
            f"- candidate_method: {evidence_summary.get('candidate_method')}",
            f"- readiness_decision: {evidence_summary.get('readiness_decision')}",
            f"- forward_confirmation_status: {evidence_summary.get('forward_confirmation_status')}",
            f"- can_enter_owner_review: {str(blocking.get('can_enter_owner_review')).lower()}",
            "- candidate_role_fixed: false",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def _render_promotion_report(
    manifest: Mapping[str, Any],
    evidence_summary: Mapping[str, Any],
    blocking: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Smoothed Promotion Review {manifest.get('promotion_review_id')}",
            "",
            f"- candidate_method: {evidence_summary.get('candidate_method')}",
            f"- readiness_decision: {evidence_summary.get('readiness_decision')}",
            f"- evidence_status: {evidence_summary.get('readiness_evidence_status')}",
            f"- can_enter_owner_review: {blocking.get('can_enter_owner_review')}",
            "- auto_apply: false",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "## Supporting Evidence",
            "",
            *[
                f"- {row.get('evidence_id')}: actual={row.get('actual')}, "
                f"supports={row.get('supports_promotion_review')}"
                for row in _records(evidence_summary.get("supporting_evidence"))
            ],
            "",
            "## Blocking Issues",
            "",
            *[
                f"- {row.get('issue')}: severity={row.get('severity')}, "
                f"blocks_candidate={row.get('blocks_paper_shadow_primary_candidate')}"
                for row in _records(blocking.get("blocking_issues"))
            ],
            "",
            "候选只继承 validated confirmation/readiness authority；本报告不能创造候选。",
            "",
        ]
    )


def _promotion_views(
    snapshot: Mapping[str, Any], *, promotion_review_id: str, root: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    config, policy = _policy(snapshot)
    scorecard = _scorecard_payload(_mapping(snapshot.get("scorecard_source")))
    owner_update = _owner_update_payload(_mapping(snapshot.get("owner_update_source")))
    watch = _watch_payload(_mapping(snapshot.get("watch_source")))
    evidence_summary = _promotion_evidence(scorecard, owner_update, watch)
    blocking = _promotion_blocking(scorecard, evidence_summary, policy)
    lineage = _lineage(scorecard)
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_smoothed_promotion_review_manifest",
        "promotion_review_id": promotion_review_id,
        "readiness_scorecard_id": scorecard.get("scorecard_id"),
        "owner_update_id": owner_update.get("owner_update_id"),
        "watch_pack_id": watch.get("watch_pack_id"),
        **lineage,
        "candidate_method": evidence_summary.get("candidate_method"),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "policy_id": _mapping(config.get("promotion_policy_metadata")).get("policy_id"),
        "smoothed_promotion_review_input_snapshot_path": str(
            root / "smoothed_promotion_review_input_snapshot.json"
        ),
        "smoothed_promotion_review_manifest_path": str(
            root / "smoothed_promotion_review_manifest.json"
        ),
        "promotion_evidence_summary_path": str(root / "promotion_evidence_summary.json"),
        "promotion_blocking_issues_path": str(root / "promotion_blocking_issues.json"),
        "smoothed_promotion_review_report_path": str(root / "smoothed_promotion_review_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    reader = _render_promotion_reader(evidence_summary, blocking)
    report = _render_promotion_report(manifest, evidence_summary, blocking)
    views = {
        "smoothed_promotion_review_input_snapshot.json": _json_bytes(dict(snapshot)),
        "smoothed_promotion_review_manifest.json": _json_bytes(manifest),
        "promotion_evidence_summary.json": _json_bytes(evidence_summary),
        "promotion_blocking_issues.json": _json_bytes(blocking),
        "smoothed_promotion_review_report.md": report.encode("utf-8"),
        "reader_brief_section.md": reader.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "promotion_evidence_summary": evidence_summary,
        "promotion_blocking_issues": blocking,
        "reader_brief_section": reader,
    }


def _validate_promotion_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == SMOOTHED_PROMOTION_REVIEW_SNAPSHOT_SCHEMA,
            "promotion snapshot schema invalid",
        )
        specs = (
            (
                "scorecard_source",
                "smoothed_readiness_scorecard",
                readiness.validate_smoothed_readiness_scorecard_artifact,
                "scorecard_id",
                "smoothed_readiness_manifest.json",
            ),
            (
                "owner_update_source",
                "smoothed_owner_review_update",
                readiness.validate_smoothed_owner_review_update_artifact,
                "owner_update_id",
                "smoothed_owner_update_manifest.json",
            ),
            (
                "watch_source",
                "smoothed_watch_pack",
                evidence.validate_smoothed_watch_pack_artifact,
                "watch_pack_id",
                "smoothed_watch_manifest.json",
            ),
        )
        manifests: dict[str, dict[str, Any]] = {}
        for field, kind, validator, key, manifest_name in specs:
            binding = _mapping(snapshot.get(field))
            errors.extend(
                _validate_binding(binding, kind=kind, validator=validator, validator_key=key)
            )
            manifests[field] = _bundle_json(binding, manifest_name)
        errors.extend(_validate_policy_binding(_mapping(snapshot.get("policy_binding"))))
        scorecard = manifests["scorecard_source"]
        owner = manifests["owner_update_source"]
        watch = manifests["watch_source"]
        _require(
            owner.get("scorecard_id") == scorecard.get("scorecard_id"),
            "promotion owner/scorecard mismatch",
        )
        _require(
            owner.get("watch_pack_id") == watch.get("watch_pack_id"),
            "promotion owner/watch mismatch",
        )
        for field in ("review_id", "comparison_id", "smoothed_backfill_id", "baseline_backfill_id"):
            _require(
                len({scorecard.get(field), owner.get(field), watch.get(field)}) == 1,
                f"promotion {field} mismatch",
            )
        _require(
            scorecard.get("confirmation_id") == watch.get("confirmation_id"),
            "promotion confirmation mismatch",
        )
        generated = target_core._datetime(
            snapshot.get("generated_at"), field="promotion generated_at"
        )
        evidence._chronology(generated, scorecard, owner, watch)
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


@_with_validation_session
def build_smoothed_promotion_review_pack(
    *,
    readiness_scorecard_id: str,
    owner_update_id: str,
    watch_pack_id: str,
    scorecard_dir: Path = DEFAULT_SMOOTHED_READINESS_SCORECARD_DIR,
    owner_update_dir: Path = DEFAULT_SMOOTHED_OWNER_REVIEW_UPDATE_DIR,
    watch_pack_dir: Path = DEFAULT_SMOOTHED_WATCH_PACK_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_PROMOTION_REVIEW_DIR,
    config_path: Path = DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    snapshot = {
        "schema_version": SMOOTHED_PROMOTION_REVIEW_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "scorecard_source": _scorecard_binding(readiness_scorecard_id, scorecard_dir),
        "owner_update_source": _owner_update_binding(owner_update_id, owner_update_dir),
        "watch_source": _watch_binding(watch_pack_id, watch_pack_dir),
        "policy_binding": _policy_binding(config_path),
        "production_effect": "none",
    }
    errors = _validate_promotion_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    artifact_id = _stable_id("smoothed-promotion-review", snapshot)
    root = _unique_dir(output_dir / artifact_id)
    views, payload = _promotion_views(snapshot, promotion_review_id=root.name, root=root)
    _write(
        root,
        views,
        "latest_smoothed_promotion_review",
        "smoothed_promotion_review_manifest.json",
    )
    return {"promotion_review_id": root.name, "promotion_review_dir": root, **payload}


def smoothed_promotion_review_report_payload(
    *,
    promotion_review_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_PROMOTION_REVIEW_DIR,
) -> dict[str, Any]:
    root = hardening._artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=promotion_review_id if not latest else None,
        pointer_name="latest_smoothed_promotion_review",
    )
    return {
        **_read_json(root / "smoothed_promotion_review_manifest.json"),
        "promotion_evidence_summary": _read_json(root / "promotion_evidence_summary.json"),
        "promotion_blocking_issues": _read_json(root / "promotion_blocking_issues.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "input_snapshot": _read_json(root / "smoothed_promotion_review_input_snapshot.json"),
        "promotion_review_dir": str(root),
    }


@_with_validation_session
def validate_smoothed_promotion_review_artifact(
    *,
    promotion_review_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_PROMOTION_REVIEW_DIR,
) -> dict[str, Any]:
    root = output_dir / promotion_review_id
    snapshot = (
        legacy._read_optional_json(root / "smoothed_promotion_review_input_snapshot.json") or {}
    )
    errors = _validate_promotion_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, payload = _promotion_views(
            snapshot, promotion_review_id=promotion_review_id, root=root
        )
        mismatches = _view_errors(root, views)
        evidence_summary = _mapping(payload.get("promotion_evidence_summary"))
        blocking = _mapping(payload.get("promotion_blocking_issues"))
        if evidence_summary.get("candidate_method") is None:
            _require(
                blocking.get("can_enter_owner_review") is False,
                "candidate-less promotion cannot enter owner review",
            )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return _validation_payload(
        "etf_dynamic_v3_smoothed_promotion_review_validation",
        promotion_review_id,
        [
            _check("snapshot_and_live_inputs", not errors, "; ".join(errors)),
            _check("content_derived_views", not mismatches, ",".join(mismatches)),
        ],
        artifact_id_key="promotion_review_id",
    )


def _gate_decision(
    gate_id: str, promotion: Mapping[str, Any], policy: Mapping[str, Any]
) -> dict[str, Any]:
    evidence_summary = _mapping(promotion.get("promotion_evidence_summary"))
    blocking = _mapping(promotion.get("promotion_blocking_issues"))
    candidate = evidence_summary.get("candidate_method")
    hard = [
        row
        for row in _records(blocking.get("blocking_issues"))
        if row.get("blocks_paper_shadow_primary_candidate") is True
    ]
    eligible = (
        candidate is not None
        and blocking.get("can_enter_owner_review") is True
        and evidence_summary.get("readiness_decision") in policy["eligible_readiness_decisions"]
        and not hard
    )
    return {
        "schema_version": 2,
        "gate_id": gate_id,
        "candidate_method": candidate,
        "secondary_method": evidence_summary.get("secondary_method"),
        "gate_scope": policy["gate_scope"],
        "gate_decision": "ELIGIBLE_FOR_OWNER_APPROVAL" if eligible else "CONTINUE_OBSERVATION",
        "decision_confidence": _text(evidence_summary.get("decision_confidence"), "LOW"),
        "owner_approval_required": True,
        "auto_apply": False,
        "can_update_paper_shadow_primary_candidate": (
            "OWNER_DECISION_REQUIRED" if eligible else "NOT_ELIGIBLE"
        ),
        "can_write_official_target_weights": False,
        "can_trigger_production": False,
        **SYSTEM_TARGET_SAFETY,
    }


def _gate_criteria(
    gate_id: str,
    promotion: Mapping[str, Any],
    decision: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    evidence_summary = _mapping(promotion.get("promotion_evidence_summary"))
    blocking = _mapping(promotion.get("promotion_blocking_issues"))
    candidate = evidence_summary.get("candidate_method")
    readiness_decision = _text(evidence_summary.get("readiness_decision"))
    forward = _text(evidence_summary.get("forward_confirmation_status"), "NOT_REGISTERED")
    hard = [
        _text(row.get("issue"))
        for row in _records(blocking.get("blocking_issues"))
        if row.get("blocks_paper_shadow_primary_candidate") is True
    ]
    warnings = [
        _text(row.get("issue"))
        for row in _records(blocking.get("blocking_issues"))
        if row.get("blocks_paper_shadow_primary_candidate") is False
    ]
    forward_ok = forward in set(policy["forward_observation_statuses"]) | set(
        policy["forward_pass_statuses"]
    )
    criteria = [
        {
            "criterion": "candidate_present",
            "required": True,
            "actual": candidate is not None,
            "status": "PASS" if candidate is not None else "FAIL",
            **SYSTEM_TARGET_SAFETY,
        },
        {
            "criterion": "promotion_review_decision",
            "required": list(policy["eligible_readiness_decisions"]),
            "actual": readiness_decision,
            "status": "PASS"
            if readiness_decision in policy["eligible_readiness_decisions"]
            else "FAIL",
            **SYSTEM_TARGET_SAFETY,
        },
        {
            "criterion": "no_hard_blockers",
            "required": [],
            "actual": hard,
            "status": "PASS" if not hard else "FAIL",
            **SYSTEM_TARGET_SAFETY,
        },
        {
            "criterion": "forward_confirmation",
            "required": list(policy["forward_pass_statuses"]),
            "observation_allowed": list(policy["forward_observation_statuses"]),
            "actual": forward,
            "status": (
                "PASS"
                if forward in policy["forward_pass_statuses"]
                else "PASS_WITH_WARNINGS"
                if forward in policy["forward_observation_statuses"]
                else "FAIL"
            ),
            **SYSTEM_TARGET_SAFETY,
        },
        {
            "criterion": "production_safety",
            "required": "NO_PRODUCTION",
            "actual": "NO_PRODUCTION"
            if decision.get("can_trigger_production") is False
            and decision.get("can_write_official_target_weights") is False
            else "PRODUCTION_ALLOWED",
            "status": "PASS"
            if decision.get("can_trigger_production") is False
            and decision.get("can_write_official_target_weights") is False
            else "FAIL",
            **SYSTEM_TARGET_SAFETY,
        },
    ]
    if not forward_ok and "forward_confirmation_not_registered" not in hard:
        hard.append("forward_confirmation_not_registered")
    return {
        "schema_version": 2,
        "gate_id": gate_id,
        "criteria": criteria,
        "hard_blockers": hard,
        "warnings": warnings,
        **SYSTEM_TARGET_SAFETY,
    }


def _render_gate_report(
    manifest: Mapping[str, Any],
    decision: Mapping[str, Any],
    criteria: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Primary Research Candidate Gate {manifest.get('gate_id')}",
            "",
            f"- candidate_method: {decision.get('candidate_method')}",
            f"- gate_decision: {decision.get('gate_decision')}",
            f"- owner_approval_required: {decision.get('owner_approval_required')}",
            "- auto_apply: false",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "## Criteria",
            "",
            *[
                f"- {row.get('criterion')}: actual={row.get('actual')}, status={row.get('status')}"
                for row in _records(criteria.get("criteria"))
            ],
            "",
            "FAIL 表示业务门未通过，不表示 artifact 校验失败。",
            "",
        ]
    )


def _gate_views(
    snapshot: Mapping[str, Any], *, gate_id: str, root: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    config, policy = _policy(snapshot)
    promotion = _promotion_payload(_mapping(snapshot.get("promotion_source")))
    decision = _gate_decision(gate_id, promotion, policy)
    criteria = _gate_criteria(gate_id, promotion, decision, policy)
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_primary_research_candidate_gate_manifest",
        "gate_id": gate_id,
        "promotion_review_id": promotion.get("promotion_review_id"),
        **_lineage(promotion),
        "candidate_method": decision.get("candidate_method"),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "policy_id": _mapping(config.get("promotion_policy_metadata")).get("policy_id"),
        "primary_research_candidate_gate_input_snapshot_path": str(
            root / "primary_research_candidate_gate_input_snapshot.json"
        ),
        "primary_research_candidate_gate_manifest_path": str(
            root / "primary_research_candidate_gate_manifest.json"
        ),
        "gate_decision_path": str(root / "gate_decision.json"),
        "gate_criteria_results_path": str(root / "gate_criteria_results.json"),
        "primary_research_candidate_gate_report_path": str(
            root / "primary_research_candidate_gate_report.md"
        ),
        **SYSTEM_TARGET_SAFETY,
    }
    report = _render_gate_report(manifest, decision, criteria)
    views = {
        "primary_research_candidate_gate_input_snapshot.json": _json_bytes(dict(snapshot)),
        "primary_research_candidate_gate_manifest.json": _json_bytes(manifest),
        "gate_decision.json": _json_bytes(decision),
        "gate_criteria_results.json": _json_bytes(criteria),
        "primary_research_candidate_gate_report.md": report.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "gate_decision": decision,
        "gate_criteria_results": criteria,
    }


def _validate_gate_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == PRIMARY_RESEARCH_CANDIDATE_GATE_SNAPSHOT_SCHEMA,
            "candidate gate snapshot schema invalid",
        )
        source = _mapping(snapshot.get("promotion_source"))
        errors.extend(
            _validate_binding(
                source,
                kind="smoothed_promotion_review",
                validator=validate_smoothed_promotion_review_artifact,
                validator_key="promotion_review_id",
            )
        )
        errors.extend(_validate_policy_binding(_mapping(snapshot.get("policy_binding"))))
        manifest = _bundle_json(source, "smoothed_promotion_review_manifest.json")
        generated = target_core._datetime(
            snapshot.get("generated_at"), field="candidate gate generated_at"
        )
        evidence._chronology(generated, manifest)
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


@_with_validation_session
def run_primary_research_candidate_gate(
    *,
    promotion_review_id: str,
    promotion_review_dir: Path = DEFAULT_SMOOTHED_PROMOTION_REVIEW_DIR,
    output_dir: Path = DEFAULT_PRIMARY_RESEARCH_CANDIDATE_GATE_DIR,
    config_path: Path = DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    snapshot = {
        "schema_version": PRIMARY_RESEARCH_CANDIDATE_GATE_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "promotion_source": _promotion_binding(promotion_review_id, promotion_review_dir),
        "policy_binding": _policy_binding(config_path),
        "production_effect": "none",
    }
    errors = _validate_gate_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    artifact_id = _stable_id("primary-research-candidate-gate", snapshot)
    root = _unique_dir(output_dir / artifact_id)
    views, payload = _gate_views(snapshot, gate_id=root.name, root=root)
    _write(
        root,
        views,
        "latest_primary_research_candidate_gate",
        "primary_research_candidate_gate_manifest.json",
    )
    return {"gate_id": root.name, "gate_dir": root, **payload}


def primary_research_candidate_gate_report_payload(
    *,
    gate_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_PRIMARY_RESEARCH_CANDIDATE_GATE_DIR,
) -> dict[str, Any]:
    root = hardening._artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=gate_id if not latest else None,
        pointer_name="latest_primary_research_candidate_gate",
    )
    return {
        **_read_json(root / "primary_research_candidate_gate_manifest.json"),
        "gate_decision": _read_json(root / "gate_decision.json"),
        "gate_criteria_results": _read_json(root / "gate_criteria_results.json"),
        "input_snapshot": _read_json(root / "primary_research_candidate_gate_input_snapshot.json"),
        "gate_dir": str(root),
    }


@_with_validation_session
def validate_primary_research_candidate_gate_artifact(
    *, gate_id: str, output_dir: Path = DEFAULT_PRIMARY_RESEARCH_CANDIDATE_GATE_DIR
) -> dict[str, Any]:
    root = output_dir / gate_id
    snapshot = (
        legacy._read_optional_json(root / "primary_research_candidate_gate_input_snapshot.json")
        or {}
    )
    errors = _validate_gate_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, payload = _gate_views(snapshot, gate_id=gate_id, root=root)
        mismatches = _view_errors(root, views)
        decision = _mapping(payload.get("gate_decision"))
        if decision.get("candidate_method") is None:
            _require(
                decision.get("gate_decision") == "CONTINUE_OBSERVATION",
                "candidate-less gate cannot be eligible",
            )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return _validation_payload(
        "etf_dynamic_v3_primary_research_candidate_gate_validation",
        gate_id,
        [
            _check("snapshot_and_live_inputs", not errors, "; ".join(errors)),
            _check("content_derived_views", not mismatches, ",".join(mismatches)),
        ],
        artifact_id_key="gate_id",
    )


def _bound_targets(
    binding_id: str,
    confirmation: Mapping[str, Any],
    gate: Mapping[str, Any],
) -> dict[str, Any]:
    source = _mapping(confirmation.get("smoothed_confirmation_targets"))
    decision = _mapping(gate.get("gate_decision"))
    candidate = source.get("candidate_method")
    _require(candidate == decision.get("candidate_method"), "forward binding candidate mismatch")
    source_rows = _records(source.get("targets"))
    _require(
        (candidate is None and not source_rows)
        or (
            candidate in readiness.SMOOTHED_METHOD_TO_VARIANT
            and bool(source_rows)
            and all(row.get("method") == candidate for row in source_rows)
        ),
        "forward binding candidate/target semantics invalid",
    )
    ids = [_text(row.get("target_id")) for row in source_rows]
    _require(all(ids) and len(ids) == len(set(ids)), "forward binding target ids invalid")
    targets = [
        {
            **dict(row),
            "source_target_registered": True,
            "bound_to_weekly_progress": True,
            "bound_to_confirmation_dashboard": True,
            "bound_to_rule_review_queue": True,
        }
        for row in source_rows
    ]
    return {
        "schema_version": 2,
        "binding_id": binding_id,
        "source_confirmation_id": confirmation.get("confirmation_id"),
        "gate_id": gate.get("gate_id"),
        "candidate_method": candidate,
        "binding_status": "OBSERVATION_BOUND" if targets else "NOT_REGISTERED",
        "targets": targets,
        **SYSTEM_TARGET_SAFETY,
    }


def _forward_requirements(bound: Mapping[str, Any]) -> dict[str, Any]:
    requirements: list[dict[str, Any]] = []
    readiness_conditions: list[str] = []
    for row in _records(bound.get("targets")):
        requirement = {
            key: row.get(key)
            for key in (
                "target_id",
                "method",
                "baseline",
                "status",
                "required_forward_events",
                "required_sideways_events",
                "required_recovery_events",
                "windows",
                "success_criteria",
            )
            if key in row
        }
        requirements.append({**requirement, **SYSTEM_TARGET_SAFETY})
        if row.get("required_forward_events") is not None:
            readiness_conditions.append("required_forward_events_met")
        if row.get("required_sideways_events") is not None:
            readiness_conditions.append("sideways_events_met")
        if row.get("required_recovery_events") is not None or row.get("status") == "WATCH_ONLY":
            readiness_conditions.append("no_high_lag_failure")
    return {
        "schema_version": 2,
        "binding_status": bound.get("binding_status"),
        "requirements": requirements,
        "rule_review_ready_when": list(dict.fromkeys(readiness_conditions)),
        **SYSTEM_TARGET_SAFETY,
    }


def _render_binding_reader(bound: Mapping[str, Any], requirements: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Dynamic Rescue Smoothed Forward Binding",
            "",
            f"- candidate_method: {bound.get('candidate_method')}",
            f"- binding_status: {bound.get('binding_status')}",
            f"- target_count: {len(_records(bound.get('targets')))}",
            "- rule_review_ready_when: "
            + ", ".join(str(item) for item in requirements.get("rule_review_ready_when", [])),
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def _render_binding_report(
    manifest: Mapping[str, Any],
    bound: Mapping[str, Any],
    requirements: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Smoothed Forward Binding {manifest.get('binding_id')}",
            "",
            f"- source_confirmation_id: {bound.get('source_confirmation_id')}",
            f"- candidate_method: {bound.get('candidate_method')}",
            f"- binding_status: {bound.get('binding_status')}",
            f"- target_count: {len(_records(bound.get('targets')))}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "## Bound Targets",
            "",
            *[
                f"- {row.get('target_id')}: method={row.get('method')}, status={row.get('status')}"
                for row in _records(bound.get("targets"))
            ],
            "",
            "只绑定 confirmation 实际登记的 target；零 target 时保持 NOT_REGISTERED。",
            "",
        ]
    )


def _binding_views(
    snapshot: Mapping[str, Any], *, binding_id: str, root: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    config, _policy_value = _policy(snapshot)
    confirmation = _confirmation_payload(_mapping(snapshot.get("confirmation_source")))
    gate = _gate_payload(_mapping(snapshot.get("gate_source")))
    bound = _bound_targets(binding_id, confirmation, gate)
    requirements = _forward_requirements(bound)
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_smoothed_forward_binding_manifest",
        "binding_id": binding_id,
        "confirmation_id": confirmation.get("confirmation_id"),
        "gate_id": gate.get("gate_id"),
        **_lineage(gate),
        "candidate_method": bound.get("candidate_method"),
        "binding_status": bound.get("binding_status"),
        "target_count": len(_records(bound.get("targets"))),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "policy_id": _mapping(config.get("promotion_policy_metadata")).get("policy_id"),
        "smoothed_forward_binding_input_snapshot_path": str(
            root / "smoothed_forward_binding_input_snapshot.json"
        ),
        "smoothed_forward_binding_manifest_path": str(
            root / "smoothed_forward_binding_manifest.json"
        ),
        "bound_confirmation_targets_path": str(root / "bound_confirmation_targets.json"),
        "forward_progress_requirements_path": str(root / "forward_progress_requirements.json"),
        "smoothed_forward_binding_report_path": str(root / "smoothed_forward_binding_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    reader = _render_binding_reader(bound, requirements)
    report = _render_binding_report(manifest, bound, requirements)
    views = {
        "smoothed_forward_binding_input_snapshot.json": _json_bytes(dict(snapshot)),
        "smoothed_forward_binding_manifest.json": _json_bytes(manifest),
        "bound_confirmation_targets.json": _json_bytes(bound),
        "forward_progress_requirements.json": _json_bytes(requirements),
        "smoothed_forward_binding_report.md": report.encode("utf-8"),
        "reader_brief_section.md": reader.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "bound_confirmation_targets": bound,
        "forward_progress_requirements": requirements,
        "reader_brief_section": reader,
    }


def _validate_binding_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == SMOOTHED_FORWARD_BINDING_SNAPSHOT_SCHEMA,
            "forward binding snapshot schema invalid",
        )
        confirmation_source = _mapping(snapshot.get("confirmation_source"))
        gate_source = _mapping(snapshot.get("gate_source"))
        errors.extend(
            _validate_binding(
                confirmation_source,
                kind="smoothed_confirmation",
                validator=evidence.validate_smoothed_confirmation_artifact,
                validator_key="confirmation_id",
            )
        )
        errors.extend(
            _validate_binding(
                gate_source,
                kind="primary_research_candidate_gate",
                validator=validate_primary_research_candidate_gate_artifact,
                validator_key="gate_id",
            )
        )
        errors.extend(_validate_policy_binding(_mapping(snapshot.get("policy_binding"))))
        confirmation = _bundle_json(confirmation_source, "smoothed_confirmation_manifest.json")
        gate = _bundle_json(gate_source, "primary_research_candidate_gate_manifest.json")
        _require(
            gate.get("confirmation_id") == confirmation.get("confirmation_id"),
            "forward binding confirmation mismatch",
        )
        _require(
            gate.get("candidate_method") == confirmation.get("candidate_method"),
            "forward binding manifest candidate mismatch",
        )
        for field in ("review_id", "comparison_id", "smoothed_backfill_id", "baseline_backfill_id"):
            _require(
                gate.get(field) == confirmation.get(field), f"forward binding {field} mismatch"
            )
        generated = target_core._datetime(
            snapshot.get("generated_at"), field="forward binding generated_at"
        )
        evidence._chronology(generated, confirmation, gate)
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


@_with_validation_session
def run_smoothed_forward_binding(
    *,
    confirmation_id: str,
    gate_id: str,
    confirmation_dir: Path = DEFAULT_SMOOTHED_FORWARD_CONFIRMATION_DIR,
    gate_dir: Path = DEFAULT_PRIMARY_RESEARCH_CANDIDATE_GATE_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_FORWARD_BINDING_DIR,
    config_path: Path = DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    snapshot = {
        "schema_version": SMOOTHED_FORWARD_BINDING_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "confirmation_source": _confirmation_binding(confirmation_id, confirmation_dir),
        "gate_source": _gate_binding(gate_id, gate_dir),
        "policy_binding": _policy_binding(config_path),
        "production_effect": "none",
    }
    errors = _validate_binding_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    artifact_id = _stable_id("smoothed-forward-binding", snapshot)
    root = _unique_dir(output_dir / artifact_id)
    views, payload = _binding_views(snapshot, binding_id=root.name, root=root)
    _write(root, views, "latest_smoothed_forward_binding", "smoothed_forward_binding_manifest.json")
    return {"binding_id": root.name, "binding_dir": root, **payload}


def smoothed_forward_binding_report_payload(
    *,
    binding_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_FORWARD_BINDING_DIR,
) -> dict[str, Any]:
    root = hardening._artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=binding_id if not latest else None,
        pointer_name="latest_smoothed_forward_binding",
    )
    return {
        **_read_json(root / "smoothed_forward_binding_manifest.json"),
        "bound_confirmation_targets": _read_json(root / "bound_confirmation_targets.json"),
        "forward_progress_requirements": _read_json(root / "forward_progress_requirements.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "input_snapshot": _read_json(root / "smoothed_forward_binding_input_snapshot.json"),
        "binding_dir": str(root),
    }


@_with_validation_session
def validate_smoothed_forward_binding_artifact(
    *, binding_id: str, output_dir: Path = DEFAULT_SMOOTHED_FORWARD_BINDING_DIR
) -> dict[str, Any]:
    root = output_dir / binding_id
    snapshot = (
        legacy._read_optional_json(root / "smoothed_forward_binding_input_snapshot.json") or {}
    )
    errors = _validate_binding_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, payload = _binding_views(snapshot, binding_id=binding_id, root=root)
        mismatches = _view_errors(root, views)
        bound = _mapping(payload.get("bound_confirmation_targets"))
        if bound.get("candidate_method") is None:
            _require(
                not _records(bound.get("targets"))
                and bound.get("binding_status") == "NOT_REGISTERED",
                "candidate-less binding must contain zero targets",
            )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return _validation_payload(
        "etf_dynamic_v3_smoothed_forward_binding_validation",
        binding_id,
        [
            _check("snapshot_and_live_inputs", not errors, "; ".join(errors)),
            _check("content_derived_views", not mismatches, ",".join(mismatches)),
        ],
        artifact_id_key="binding_id",
    )


def _switch_plan(
    switch_plan_id: str,
    gate: Mapping[str, Any],
    binding: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    gate_decision = _mapping(gate.get("gate_decision"))
    bound = _mapping(binding.get("bound_confirmation_targets"))
    candidate = gate_decision.get("candidate_method")
    _require(candidate == bound.get("candidate_method"), "switch candidate mismatch")
    eligible = (
        candidate is not None
        and gate_decision.get("gate_decision") == "ELIGIBLE_FOR_OWNER_APPROVAL"
        and bound.get("binding_status") == "OBSERVATION_BOUND"
        and bool(_records(bound.get("targets")))
    )
    reasons: list[str] = []
    if candidate is None:
        reasons.append("no_eligible_candidate")
    if gate_decision.get("gate_decision") != "ELIGIBLE_FOR_OWNER_APPROVAL":
        reasons.append("candidate_gate_not_eligible")
    if bound.get("binding_status") != "OBSERVATION_BOUND":
        reasons.append("forward_targets_not_registered")
    return {
        "schema_version": 2,
        "switch_plan_id": switch_plan_id,
        "switch_scope": policy["gate_scope"],
        "current_primary_research_candidate": policy["current_primary_research_candidate"],
        "proposed_primary_research_candidate": candidate if eligible else None,
        "candidate_method": candidate,
        "switch_decision": "OWNER_DECISION_REQUIRED" if eligible else "NO_ELIGIBLE_CANDIDATE",
        "switch_blocking_reasons": list(dict.fromkeys(reasons)),
        "auto_switch": False,
        "requires_owner_decision": True,
        "requires_forward_confirmation": candidate is not None,
        "rollback_method": policy["rollback_method"],
        "effective_only_for": [
            "paper_shadow_reports",
            "research_comparisons",
            "forward_observation",
        ],
        "actual_switch_executed": False,
        **SYSTEM_TARGET_SAFETY,
    }


def _switch_safety(plan: Mapping[str, Any]) -> dict[str, Any]:
    checks = {
        "research_scope_only": plan.get("switch_scope") == "paper_shadow_research_only",
        "auto_switch_disabled": plan.get("auto_switch") is False,
        "actual_switch_executed_false": plan.get("actual_switch_executed") is False,
        "not_official_target_weights": True,
        "does_not_modify_real_portfolio": True,
        "does_not_generate_order_ticket": True,
        "broker_action_allowed": False,
        "production_effect": "none",
    }
    passed = (
        checks["research_scope_only"] is True
        and checks["auto_switch_disabled"] is True
        and checks["actual_switch_executed_false"] is True
        and checks["not_official_target_weights"] is True
        and checks["does_not_modify_real_portfolio"] is True
        and checks["does_not_generate_order_ticket"] is True
        and checks["broker_action_allowed"] is False
        and checks["production_effect"] == "none"
    )
    return {
        "schema_version": 2,
        "status": "PASS" if passed else "FAIL",
        "safety_checks": checks,
        **SYSTEM_TARGET_SAFETY,
    }


def _render_switch_reader(plan: Mapping[str, Any], safety: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Dynamic Rescue Paper Shadow Primary Switch Plan",
            "",
            f"- current_candidate: {plan.get('current_primary_research_candidate')}",
            f"- proposed_candidate: {plan.get('proposed_primary_research_candidate')}",
            f"- switch_decision: {plan.get('switch_decision')}",
            f"- safety_status: {safety.get('status')}",
            "- auto_switch: false",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def _render_switch_report(
    manifest: Mapping[str, Any], plan: Mapping[str, Any], safety: Mapping[str, Any]
) -> str:
    return "\n".join(
        [
            f"# Paper Shadow Primary Switch Plan {manifest.get('switch_plan_id')}",
            "",
            "- current_primary_research_candidate: "
            f"{plan.get('current_primary_research_candidate')}",
            "- proposed_primary_research_candidate: "
            f"{plan.get('proposed_primary_research_candidate')}",
            f"- switch_decision: {plan.get('switch_decision')}",
            "- blockers: "
            + ", ".join(str(item) for item in plan.get("switch_blocking_reasons", [])),
            f"- safety_status: {safety.get('status')}",
            "- auto_switch: false",
            "- actual_switch_executed: false",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def _switch_views(
    snapshot: Mapping[str, Any], *, switch_plan_id: str, root: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    config, policy = _policy(snapshot)
    gate = _gate_payload(_mapping(snapshot.get("gate_source")))
    binding = _binding_payload(_mapping(snapshot.get("binding_source")))
    plan = _switch_plan(switch_plan_id, gate, binding, policy)
    safety = _switch_safety(plan)
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_paper_shadow_primary_switch_manifest",
        "switch_plan_id": switch_plan_id,
        "gate_id": gate.get("gate_id"),
        "binding_id": binding.get("binding_id"),
        **_lineage(gate),
        "candidate_method": plan.get("candidate_method"),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "policy_id": _mapping(config.get("promotion_policy_metadata")).get("policy_id"),
        "paper_shadow_primary_switch_input_snapshot_path": str(
            root / "paper_shadow_primary_switch_input_snapshot.json"
        ),
        "paper_shadow_primary_switch_manifest_path": str(
            root / "paper_shadow_primary_switch_manifest.json"
        ),
        "primary_switch_plan_path": str(root / "primary_switch_plan.json"),
        "primary_switch_safety_checks_path": str(root / "primary_switch_safety_checks.json"),
        "paper_shadow_primary_switch_report_path": str(
            root / "paper_shadow_primary_switch_report.md"
        ),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    reader = _render_switch_reader(plan, safety)
    report = _render_switch_report(manifest, plan, safety)
    views = {
        "paper_shadow_primary_switch_input_snapshot.json": _json_bytes(dict(snapshot)),
        "paper_shadow_primary_switch_manifest.json": _json_bytes(manifest),
        "primary_switch_plan.json": _json_bytes(plan),
        "primary_switch_safety_checks.json": _json_bytes(safety),
        "paper_shadow_primary_switch_report.md": report.encode("utf-8"),
        "reader_brief_section.md": reader.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "primary_switch_plan": plan,
        "primary_switch_safety_checks": safety,
        "reader_brief_section": reader,
    }


def _validate_switch_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == PAPER_SHADOW_PRIMARY_SWITCH_SNAPSHOT_SCHEMA,
            "primary switch snapshot schema invalid",
        )
        gate_source = _mapping(snapshot.get("gate_source"))
        binding_source = _mapping(snapshot.get("binding_source"))
        errors.extend(
            _validate_binding(
                gate_source,
                kind="primary_research_candidate_gate",
                validator=validate_primary_research_candidate_gate_artifact,
                validator_key="gate_id",
            )
        )
        errors.extend(
            _validate_binding(
                binding_source,
                kind="smoothed_forward_binding",
                validator=validate_smoothed_forward_binding_artifact,
                validator_key="binding_id",
            )
        )
        errors.extend(_validate_policy_binding(_mapping(snapshot.get("policy_binding"))))
        gate = _bundle_json(gate_source, "primary_research_candidate_gate_manifest.json")
        binding = _bundle_json(binding_source, "smoothed_forward_binding_manifest.json")
        _require(binding.get("gate_id") == gate.get("gate_id"), "switch gate/binding mismatch")
        _require(
            binding.get("candidate_method") == gate.get("candidate_method"),
            "switch manifest candidate mismatch",
        )
        for field in (
            "review_id",
            "comparison_id",
            "smoothed_backfill_id",
            "baseline_backfill_id",
            "confirmation_id",
        ):
            _require(binding.get(field) == gate.get(field), f"switch {field} mismatch")
        generated = target_core._datetime(
            snapshot.get("generated_at"), field="primary switch generated_at"
        )
        evidence._chronology(generated, gate, binding)
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


@_with_validation_session
def build_paper_shadow_primary_switch_plan(
    *,
    gate_id: str,
    binding_id: str,
    gate_dir: Path = DEFAULT_PRIMARY_RESEARCH_CANDIDATE_GATE_DIR,
    binding_dir: Path = DEFAULT_SMOOTHED_FORWARD_BINDING_DIR,
    output_dir: Path = DEFAULT_PAPER_SHADOW_PRIMARY_SWITCH_DIR,
    config_path: Path = DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    snapshot = {
        "schema_version": PAPER_SHADOW_PRIMARY_SWITCH_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "gate_source": _gate_binding(gate_id, gate_dir),
        "binding_source": _binding_binding(binding_id, binding_dir),
        "policy_binding": _policy_binding(config_path),
        "production_effect": "none",
    }
    errors = _validate_switch_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    artifact_id = _stable_id("paper-shadow-primary-switch", snapshot)
    root = _unique_dir(output_dir / artifact_id)
    views, payload = _switch_views(snapshot, switch_plan_id=root.name, root=root)
    _write(
        root,
        views,
        "latest_paper_shadow_primary_switch",
        "paper_shadow_primary_switch_manifest.json",
    )
    return {"switch_plan_id": root.name, "switch_plan_dir": root, **payload}


def paper_shadow_primary_switch_report_payload(
    *,
    switch_plan_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_PAPER_SHADOW_PRIMARY_SWITCH_DIR,
) -> dict[str, Any]:
    root = hardening._artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=switch_plan_id if not latest else None,
        pointer_name="latest_paper_shadow_primary_switch",
    )
    return {
        **_read_json(root / "paper_shadow_primary_switch_manifest.json"),
        "primary_switch_plan": _read_json(root / "primary_switch_plan.json"),
        "primary_switch_safety_checks": _read_json(root / "primary_switch_safety_checks.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "input_snapshot": _read_json(root / "paper_shadow_primary_switch_input_snapshot.json"),
        "switch_plan_dir": str(root),
    }


@_with_validation_session
def validate_paper_shadow_primary_switch_artifact(
    *, switch_plan_id: str, output_dir: Path = DEFAULT_PAPER_SHADOW_PRIMARY_SWITCH_DIR
) -> dict[str, Any]:
    root = output_dir / switch_plan_id
    snapshot = (
        legacy._read_optional_json(root / "paper_shadow_primary_switch_input_snapshot.json") or {}
    )
    errors = _validate_switch_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, payload = _switch_views(snapshot, switch_plan_id=switch_plan_id, root=root)
        mismatches = _view_errors(root, views)
        plan = _mapping(payload.get("primary_switch_plan"))
        safety = _mapping(payload.get("primary_switch_safety_checks"))
        _require(
            plan.get("auto_switch") is False and plan.get("actual_switch_executed") is False,
            "switch plan executed a change",
        )
        _require(safety.get("status") == "PASS", "switch safety failed")
        if plan.get("candidate_method") is None:
            _require(
                plan.get("proposed_primary_research_candidate") is None,
                "candidate-less switch proposed a candidate",
            )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return _validation_payload(
        "etf_dynamic_v3_paper_shadow_primary_switch_validation",
        switch_plan_id,
        [
            _check("snapshot_and_live_inputs", not errors, "; ".join(errors)),
            _check("content_derived_views", not mismatches, ",".join(mismatches)),
        ],
        artifact_id_key="switch_plan_id",
    )


def _owner_decision(
    decision_id: str,
    promotion: Mapping[str, Any],
    gate: Mapping[str, Any],
    switch: Mapping[str, Any],
    record_request: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    promotion_evidence = _mapping(promotion.get("promotion_evidence_summary"))
    promotion_blocking = _mapping(promotion.get("promotion_blocking_issues"))
    gate_decision = _mapping(gate.get("gate_decision"))
    switch_plan = _mapping(switch.get("primary_switch_plan"))
    candidate = promotion_evidence.get("candidate_method")
    _require(
        candidate == gate_decision.get("candidate_method") == switch_plan.get("candidate_method"),
        "owner promotion candidate mismatch",
    )
    owner_decision = _text(record_request.get("owner_decision"), "pending")
    _require(owner_decision in policy["owner_decisions"], "owner promotion decision invalid")
    eligible = (
        candidate is not None
        and promotion_blocking.get("can_enter_owner_review") is True
        and gate_decision.get("gate_decision") == "ELIGIBLE_FOR_OWNER_APPROVAL"
        and switch_plan.get("proposed_primary_research_candidate") == candidate
        and switch_plan.get("switch_decision") == "OWNER_DECISION_REQUIRED"
    )
    if owner_decision == "promote_to_primary_research_candidate":
        _require(eligible, "owner cannot promote without an eligible candidate and switch plan")
    if eligible:
        recommended = "review_for_manual_promotion_decision"
    elif candidate is None:
        recommended = "request_more_forward_data"
    elif gate_decision.get("gate_decision") != "ELIGIBLE_FOR_OWNER_APPROVAL":
        recommended = "continue_observation"
    else:
        recommended = "defer"
    change_requested = owner_decision == "promote_to_primary_research_candidate" and eligible
    return {
        "schema_version": 2,
        "decision_id": decision_id,
        "candidate_method": candidate,
        "current_primary_research_candidate": switch_plan.get("current_primary_research_candidate"),
        "proposed_primary_research_candidate": switch_plan.get(
            "proposed_primary_research_candidate"
        ),
        "owner_decision": owner_decision,
        "decision_reason": _text(record_request.get("decision_reason")),
        "recorded_at": record_request.get("recorded_at"),
        "recommended_owner_action": recommended,
        "paper_shadow_primary_candidate_change_allowed": change_requested,
        "paper_shadow_primary_candidate_change_requested": change_requested,
        "actual_switch_executed": False,
        "not_official_target_weights": True,
        **SYSTEM_TARGET_SAFETY,
    }


def _render_owner_checklist(decision: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Smoothed Owner Promotion Checklist",
            "",
            "- [ ] candidate 是否来自 validated confirmation/readiness lineage？",
            "- [ ] gate 是否为 ELIGIBLE_FOR_OWNER_APPROVAL？",
            "- [ ] forward target 是否为实际登记目标且未被伪造？",
            "- [ ] switch plan 是否保持 research-only / no auto-switch？",
            "- [ ] 是否确认 no official target weights / no broker / no production？",
            "",
            f"- candidate_method: {decision.get('candidate_method')}",
            f"- owner_decision: {decision.get('owner_decision')}",
            f"- recommended_owner_action: {decision.get('recommended_owner_action')}",
            "",
        ]
    )


def _render_owner_reader(decision: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Dynamic Rescue Smoothed Promotion Decision",
            "",
            f"- candidate_method: {decision.get('candidate_method')}",
            f"- owner_decision: {decision.get('owner_decision')}",
            f"- recommended_owner_action: {decision.get('recommended_owner_action')}",
            "- change_requested: "
            + str(decision.get("paper_shadow_primary_candidate_change_requested")).lower(),
            "- actual_switch_executed: false",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def _render_owner_report(manifest: Mapping[str, Any], decision: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            f"# Smoothed Owner Promotion Decision {manifest.get('decision_id')}",
            "",
            f"- candidate_method: {decision.get('candidate_method')}",
            "- current_primary_research_candidate: "
            f"{decision.get('current_primary_research_candidate')}",
            "- proposed_primary_research_candidate: "
            f"{decision.get('proposed_primary_research_candidate')}",
            f"- owner_decision: {decision.get('owner_decision')}",
            f"- decision_reason: {decision.get('decision_reason')}",
            f"- recommended_owner_action: {decision.get('recommended_owner_action')}",
            "- change_requested: "
            f"{decision.get('paper_shadow_primary_candidate_change_requested')}",
            "- actual_switch_executed: false",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "记录 promotion 只表达 owner 的 research-candidate 请求；本 artifact 不执行切换。",
            "",
        ]
    )


def _owner_views(
    snapshot: Mapping[str, Any], *, decision_id: str, root: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    config, policy = _policy(snapshot)
    promotion = _promotion_payload(_mapping(snapshot.get("promotion_source")))
    gate = _gate_payload(_mapping(snapshot.get("gate_source")))
    switch = _switch_payload(_mapping(snapshot.get("switch_source")))
    record_request = _mapping(snapshot.get("record_request"))
    decision = _owner_decision(decision_id, promotion, gate, switch, record_request, policy)
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_smoothed_owner_promotion_manifest",
        "decision_id": decision_id,
        "promotion_review_id": promotion.get("promotion_review_id"),
        "gate_id": gate.get("gate_id"),
        "switch_plan_id": switch.get("switch_plan_id"),
        **_lineage(promotion),
        "candidate_method": decision.get("candidate_method"),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "policy_id": _mapping(config.get("promotion_policy_metadata")).get("policy_id"),
        "smoothed_owner_promotion_input_snapshot_path": str(
            root / "smoothed_owner_promotion_input_snapshot.json"
        ),
        "smoothed_owner_promotion_manifest_path": str(
            root / "smoothed_owner_promotion_manifest.json"
        ),
        "owner_promotion_decision_path": str(root / "owner_promotion_decision.json"),
        "owner_promotion_checklist_path": str(root / "owner_promotion_checklist.md"),
        "smoothed_owner_promotion_report_path": str(root / "smoothed_owner_promotion_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    checklist = _render_owner_checklist(decision)
    reader = _render_owner_reader(decision)
    report = _render_owner_report(manifest, decision)
    views = {
        "smoothed_owner_promotion_input_snapshot.json": _json_bytes(dict(snapshot)),
        "smoothed_owner_promotion_manifest.json": _json_bytes(manifest),
        "owner_promotion_decision.json": _json_bytes(decision),
        "owner_promotion_checklist.md": checklist.encode("utf-8"),
        "smoothed_owner_promotion_report.md": report.encode("utf-8"),
        "reader_brief_section.md": reader.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "owner_promotion_decision": decision,
        "owner_promotion_checklist": checklist,
        "reader_brief_section": reader,
    }


def _validate_owner_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == SMOOTHED_OWNER_PROMOTION_SNAPSHOT_SCHEMA,
            "owner promotion snapshot schema invalid",
        )
        specs = (
            (
                "promotion_source",
                "smoothed_promotion_review",
                validate_smoothed_promotion_review_artifact,
                "promotion_review_id",
                "smoothed_promotion_review_manifest.json",
            ),
            (
                "gate_source",
                "primary_research_candidate_gate",
                validate_primary_research_candidate_gate_artifact,
                "gate_id",
                "primary_research_candidate_gate_manifest.json",
            ),
            (
                "switch_source",
                "paper_shadow_primary_switch",
                validate_paper_shadow_primary_switch_artifact,
                "switch_plan_id",
                "paper_shadow_primary_switch_manifest.json",
            ),
        )
        manifests: dict[str, dict[str, Any]] = {}
        for field, kind, validator, key, manifest_name in specs:
            source = _mapping(snapshot.get(field))
            errors.extend(
                _validate_binding(source, kind=kind, validator=validator, validator_key=key)
            )
            manifests[field] = _bundle_json(source, manifest_name)
        errors.extend(_validate_policy_binding(_mapping(snapshot.get("policy_binding"))))
        promotion = manifests["promotion_source"]
        gate = manifests["gate_source"]
        switch = manifests["switch_source"]
        _require(
            gate.get("promotion_review_id") == promotion.get("promotion_review_id"),
            "owner promotion/gate mismatch",
        )
        _require(switch.get("gate_id") == gate.get("gate_id"), "owner gate/switch mismatch")
        for field in (
            "review_id",
            "comparison_id",
            "smoothed_backfill_id",
            "baseline_backfill_id",
            "confirmation_id",
            "candidate_method",
        ):
            _require(
                len({promotion.get(field), gate.get(field), switch.get(field)}) == 1,
                f"owner {field} mismatch",
            )
        generated = target_core._datetime(
            snapshot.get("generated_at"), field="owner promotion generated_at"
        )
        evidence._chronology(generated, promotion, gate, switch)
        request = _mapping(snapshot.get("record_request"))
        _require(
            set(request) == {"owner_decision", "decision_reason", "recorded_at"},
            "owner record request fields invalid",
        )
        _config, policy = _policy(snapshot)
        _require(
            request.get("owner_decision") in policy["owner_decisions"],
            "owner record request decision invalid",
        )
        recorded_at = request.get("recorded_at")
        if recorded_at is not None:
            recorded = target_core._datetime(recorded_at, field="owner recorded_at")
            _require(recorded >= generated, "owner recorded_at precedes artifact generation")
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


@_with_validation_session
def create_smoothed_owner_promotion_decision(
    *,
    promotion_review_id: str,
    gate_id: str,
    switch_plan_id: str,
    promotion_review_dir: Path = DEFAULT_SMOOTHED_PROMOTION_REVIEW_DIR,
    gate_dir: Path = DEFAULT_PRIMARY_RESEARCH_CANDIDATE_GATE_DIR,
    switch_plan_dir: Path = DEFAULT_PAPER_SHADOW_PRIMARY_SWITCH_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_OWNER_PROMOTION_DIR,
    config_path: Path = DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    snapshot = {
        "schema_version": SMOOTHED_OWNER_PROMOTION_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "promotion_source": _promotion_binding(promotion_review_id, promotion_review_dir),
        "gate_source": _gate_binding(gate_id, gate_dir),
        "switch_source": _switch_binding(switch_plan_id, switch_plan_dir),
        "policy_binding": _policy_binding(config_path),
        "record_request": {
            "owner_decision": "pending",
            "decision_reason": "",
            "recorded_at": None,
        },
        "production_effect": "none",
    }
    errors = _validate_owner_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    artifact_id = _stable_id("smoothed-owner-promotion", snapshot)
    root = _unique_dir(output_dir / artifact_id)
    views, payload = _owner_views(snapshot, decision_id=root.name, root=root)
    _write(root, views, "latest_smoothed_owner_promotion", "smoothed_owner_promotion_manifest.json")
    return {"decision_id": root.name, "decision_dir": root, **payload}


@_with_validation_session
def record_smoothed_owner_promotion_decision(
    *,
    decision_id: str,
    decision: str,
    decision_reason: str = "",
    output_dir: Path = DEFAULT_SMOOTHED_OWNER_PROMOTION_DIR,
    recorded_at: datetime | None = None,
) -> dict[str, Any]:
    current = validate_smoothed_owner_promotion_artifact(
        decision_id=decision_id, output_dir=output_dir
    )
    _require(
        current.get("status") == "PASS", "owner promotion artifact must validate before recording"
    )
    recorded = _generated_at(recorded_at)
    root = output_dir / decision_id
    snapshot = _read_json(root / "smoothed_owner_promotion_input_snapshot.json")
    updated = {
        **snapshot,
        "record_request": {
            "owner_decision": decision,
            "decision_reason": decision_reason.strip(),
            "recorded_at": recorded.isoformat(),
        },
    }
    errors = _validate_owner_snapshot(updated)
    _require(not errors, "; ".join(errors))
    views, payload = _owner_views(updated, decision_id=decision_id, root=root)
    _rewrite_views(root, views)
    validation = validate_smoothed_owner_promotion_artifact(
        decision_id=decision_id, output_dir=output_dir
    )
    _require(validation.get("status") == "PASS", "recorded owner promotion artifact invalid")
    return {"decision_id": decision_id, "decision_dir": root, **payload}


def smoothed_owner_promotion_report_payload(
    *,
    decision_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_OWNER_PROMOTION_DIR,
) -> dict[str, Any]:
    root = hardening._artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=decision_id if not latest else None,
        pointer_name="latest_smoothed_owner_promotion",
    )
    return {
        **_read_json(root / "smoothed_owner_promotion_manifest.json"),
        "owner_promotion_decision": _read_json(root / "owner_promotion_decision.json"),
        "owner_promotion_checklist": (root / "owner_promotion_checklist.md").read_text(
            encoding="utf-8"
        ),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "input_snapshot": _read_json(root / "smoothed_owner_promotion_input_snapshot.json"),
        "decision_dir": str(root),
    }


@_with_validation_session
def validate_smoothed_owner_promotion_artifact(
    *, decision_id: str, output_dir: Path = DEFAULT_SMOOTHED_OWNER_PROMOTION_DIR
) -> dict[str, Any]:
    root = output_dir / decision_id
    snapshot = (
        legacy._read_optional_json(root / "smoothed_owner_promotion_input_snapshot.json") or {}
    )
    errors = _validate_owner_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, payload = _owner_views(snapshot, decision_id=decision_id, root=root)
        mismatches = _view_errors(root, views)
        decision = _mapping(payload.get("owner_promotion_decision"))
        _require(
            decision.get("actual_switch_executed") is False, "owner artifact executed a switch"
        )
        if decision.get("candidate_method") is None:
            _require(
                decision.get("paper_shadow_primary_candidate_change_requested") is False,
                "candidate-less owner artifact requested promotion",
            )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return _validation_payload(
        "etf_dynamic_v3_smoothed_owner_promotion_validation",
        decision_id,
        [
            _check("snapshot_and_live_inputs", not errors, "; ".join(errors)),
            _check("content_derived_views", not mismatches, ",".join(mismatches)),
        ],
        artifact_id_key="decision_id",
    )
