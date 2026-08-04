from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.contracts.qc_qqq_options_capability_admission import (
    QCCapabilityAdmissionReceipt,
)
from ai_trading_system.contracts.qc_qqq_options_capability_discovery_authorization import (
    QCCapabilityDiscoveryAuthorization,
    QCCapabilityDiscoveryAuthorizationContractError,
)
from ai_trading_system.platform.artifacts import sha256_path
from ai_trading_system.qqq_options_capability_admission import (
    verify_qc_qqq_options_capability_admission_receipt,
)
from ai_trading_system.trading_calendar import us_equity_market_session
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_QC_QQQ_OPTIONS_CAPABILITY_DISCOVERY_AUTHORIZATION_PATH = Path(
    "config/research/qc_qqq_options_capability_discovery_authorization_v1.yaml"
)


@dataclass(frozen=True)
class QCCapabilityDiscoveryAuthorizationLoadResult:
    authorization: QCCapabilityDiscoveryAuthorization
    authorization_policy_path: Path
    authorization_policy_sha256: str
    authorization_canonical_sha256: str
    prior_receipt: QCCapabilityAdmissionReceipt


def load_qc_qqq_options_capability_discovery_authorization(
    path: Path = DEFAULT_QC_QQQ_OPTIONS_CAPABILITY_DISCOVERY_AUTHORIZATION_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> QCCapabilityDiscoveryAuthorizationLoadResult:
    resolved_root = project_root.resolve()
    resolved_policy = resolved_root / path
    try:
        resolved_policy = _require_bound_regular_file(
            path,
            project_root=resolved_root,
            field="authorization policy",
        )
        payload = safe_load_yaml_path(resolved_policy)
        if not isinstance(payload, dict):
            raise TypeError("authorization policy root must be a mapping")
        authorization = QCCapabilityDiscoveryAuthorization.model_validate(payload)
        bound_paths = {
            "prior admission policy": (
                authorization.prior_admission_policy_path,
                authorization.prior_admission_policy_sha256,
            ),
            "prior evidence": (
                authorization.prior_evidence_path,
                authorization.prior_evidence_sha256,
            ),
            "prior receipt": (
                authorization.prior_receipt_path,
                authorization.prior_receipt_sha256,
            ),
            "calendar policy": (
                authorization.calendar_policy_path,
                authorization.calendar_policy_sha256,
            ),
            "research window policy": (
                authorization.research_window_policy_path,
                authorization.research_window_policy_sha256,
            ),
        }
        resolved_bindings: dict[str, Path] = {}
        for field, (relative_path, expected_sha256) in bound_paths.items():
            resolved = _require_bound_regular_file(
                Path(relative_path),
                project_root=resolved_root,
                field=field,
            )
            if sha256_path(resolved) != expected_sha256:
                raise ValueError(f"{field} SHA-256 mismatch")
            resolved_bindings[field] = resolved

        receipt = verify_qc_qqq_options_capability_admission_receipt(
            resolved_bindings["prior receipt"],
            policy_path=resolved_bindings["prior admission policy"],
            evidence_path=resolved_bindings["prior evidence"],
            project_root=resolved_root,
        )
        _validate_prior_receipt_binding(authorization=authorization, receipt=receipt)
        _validate_research_window_binding(
            authorization=authorization,
            policy_path=resolved_bindings["research window policy"],
        )
        session = us_equity_market_session(authorization.scope.requested_start)
        if (
            not session.is_trading_day
            or session.session_status != "TRADING_DAY"
            or session.session_kind != "NORMAL_TRADING_DAY"
        ):
            raise ValueError("requested capability-discovery date is not a normal XNYS session")
    except QCCapabilityDiscoveryAuthorizationContractError:
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise QCCapabilityDiscoveryAuthorizationContractError(
            "QC_CAPABILITY_DISCOVERY_AUTHORIZATION_INVALID",
            f"{resolved_policy}: {exc}",
        ) from exc

    return QCCapabilityDiscoveryAuthorizationLoadResult(
        authorization=authorization,
        authorization_policy_path=resolved_policy,
        authorization_policy_sha256=sha256_path(resolved_policy),
        authorization_canonical_sha256=authorization.canonical_sha256,
        prior_receipt=receipt,
    )


def _validate_prior_receipt_binding(
    *,
    authorization: QCCapabilityDiscoveryAuthorization,
    receipt: QCCapabilityAdmissionReceipt,
) -> None:
    expected = {
        "decision": authorization.prior_admission_decision,
        "bounded_pilot_preparation_allowed": (
            authorization.prior_bounded_pilot_preparation_allowed
        ),
        "confirmed_item_count": authorization.prior_confirmed_item_count,
        "required_item_count": authorization.prior_required_item_count,
        "confirmed_field_count": authorization.prior_confirmed_field_count,
        "required_field_count": authorization.prior_required_field_count,
    }
    for field, value in expected.items():
        if getattr(receipt, field) != value:
            raise ValueError(f"prior receipt {field} mismatch")


def _validate_research_window_binding(
    *,
    authorization: QCCapabilityDiscoveryAuthorization,
    policy_path: Path,
) -> None:
    payload = safe_load_yaml_path(policy_path)
    if not isinstance(payload, dict):
        raise TypeError("research window policy root must be a mapping")
    market_regime = payload.get("market_regime")
    if not isinstance(market_regime, dict):
        raise ValueError("research window policy market_regime is missing")
    if market_regime.get("default_backtest_start") != authorization.primary_research_start:
        raise ValueError("primary research start binding mismatch")
    splits = payload.get("validation_splits")
    if not isinstance(splits, list):
        raise ValueError("research window policy validation_splits is missing")
    matches = [
        item
        for item in splits
        if isinstance(item, dict) and item.get("split_id") == authorization.research_window_split_id
    ]
    if len(matches) != 1:
        raise ValueError("reviewed historical window split identity mismatch")
    split = matches[0]
    if (
        split.get("start_date") != authorization.research_window_start
        or split.get("end_date") != authorization.research_window_end
        or split.get("purpose") != "historical_seen_evaluation"
        or split.get("prior_market_outcome_visibility") != "KNOWN"
        or split.get("unbiased_oos_claim_allowed") is not False
    ):
        raise ValueError("reviewed historical window split facts mismatch")


def _require_bound_regular_file(
    path: Path,
    *,
    project_root: Path,
    field: str,
) -> Path:
    root = project_root.resolve()
    candidate = path if path.is_absolute() else root / path
    try:
        relative = candidate.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"{field} escapes the project root") from exc
    if ".." in relative.parts:
        raise ValueError(f"{field} escapes the project root")
    current = root
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise ValueError(f"{field} cannot use a symlink")
    resolved = candidate.resolve(strict=True)
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"{field} resolves outside the project root") from exc
    if not resolved.is_file():
        raise ValueError(f"{field} must be a regular file")
    return resolved
