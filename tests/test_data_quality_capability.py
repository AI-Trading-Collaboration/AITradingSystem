from __future__ import annotations

import copy
import json
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
import pytest

from ai_trading_system.contracts.data_quality_capability import (
    ConsumerDataCapabilityDependency,
    ConsumerDataCapabilityReceipt,
    DataQualityCapabilityContractError,
    VerifiedConsumerDataCapabilityPreflight,
)
from ai_trading_system.data.download_publication import (
    DownloadArtifactCandidate,
    DownloadSourceBinding,
    publish_download_transaction,
)
from ai_trading_system.data.quality_capability import (
    ConsumerDataCapabilityBuildResult,
    build_consumer_data_capability,
    load_reviewed_consumer_data_capability_policy,
    verify_consumer_data_capability_receipt,
)
from ai_trading_system.data.quality_capability_discovery import (
    build_consumer_data_capability_dependency,
    consumer_data_capability_discovery_path,
    publish_consumer_data_capability_discovery,
    verify_consumer_data_capability_preflight,
)
from ai_trading_system.trading_calendar import is_us_equity_trading_day

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CAPABILITY_POLICY_PATH = (
    PROJECT_ROOT / "config/data_quality/decision_target_label_core_capability_v1.yaml"
)
DATA_QUALITY_POLICY_PATH = PROJECT_ROOT / "config/data_quality.yaml"
AS_OF = date(2021, 5, 28)
GENERATED_AT = datetime(2026, 7, 26, 12, 0, tzinfo=UTC)


def test_out_of_scope_vix_calendar_error_is_isolated_but_global_fail_is_disclosed(
    tmp_path: Path,
) -> None:
    prices_path, rates_path = _write_sources(tmp_path, include_out_of_scope_vix_error=True)
    result = _build(tmp_path, prices_path=prices_path, rates_path=rates_path)

    assert result.receipt.full_quality.status == "FAIL"
    assert result.receipt.global_cache_pass_claimed is False
    assert result.receipt.isolated_global_error_codes == ("prices_non_market_session_date",)
    assert result.receipt.unisolated_global_error_codes == ()
    assert result.receipt.scoped_quality.status == "PASS"
    assert result.receipt.capability_passed is True
    assert result.receipt.requested_window_authority_id is not None
    issue = next(
        item
        for item in result.receipt.full_quality.issues
        if item.code == "prices_non_market_session_date"
    )
    assert issue.affected_instruments == ("^VIX",)
    assert issue.isolated_from_capability is True
    verified = verify_consumer_data_capability_receipt(
        result.receipt_path,
        capability_policy_path=CAPABILITY_POLICY_PATH,
        data_quality_policy_path=DATA_QUALITY_POLICY_PATH,
    )
    assert verified == result.receipt
    scoped = pd.read_csv(result.scoped_prices_path)
    assert set(scoped["ticker"]) == {"QQQ", "SPY", "SGOV"}
    assert "^VIX" not in set(scoped["ticker"])


def test_required_scope_calendar_error_and_unstructured_error_fail_closed(
    tmp_path: Path,
) -> None:
    prices_path, rates_path = _write_sources(
        tmp_path / "required",
        include_required_qqq_error=True,
    )
    required = _build(tmp_path / "required", prices_path=prices_path, rates_path=rates_path)

    assert required.receipt.capability_passed is False
    assert required.receipt.requested_window_authority_id is None
    assert required.receipt.unisolated_global_error_codes == ("prices_non_market_session_date",)
    qqq_issue = next(
        item
        for item in required.receipt.full_quality.issues
        if item.code == "prices_non_market_session_date"
    )
    assert qqq_issue.affected_instruments == ("QQQ",)
    assert qqq_issue.isolated_from_capability is False

    duplicate_prices, duplicate_rates = _write_sources(tmp_path / "unstructured")
    frame = pd.read_csv(duplicate_prices)
    frame.loc[frame["ticker"] == "^VIX", "close"] = 0.0
    frame.to_csv(duplicate_prices, index=False, lineterminator="\n")
    duplicate = _build(
        tmp_path / "unstructured",
        prices_path=duplicate_prices,
        rates_path=duplicate_rates,
    )

    assert duplicate.receipt.capability_passed is False
    assert "prices_non_positive_close" in duplicate.receipt.unisolated_global_error_codes
    duplicate_issue = next(
        item
        for item in duplicate.receipt.full_quality.issues
        if item.code == "prices_non_positive_close"
    )
    assert duplicate_issue.affected_instruments == ()


def test_receipt_is_deterministic_and_policy_source_panel_tamper_fail_closed(
    tmp_path: Path,
) -> None:
    prices_path, rates_path = _write_sources(
        tmp_path / "source",
        include_out_of_scope_vix_error=True,
    )
    first = _build(tmp_path / "build", prices_path=prices_path, rates_path=rates_path)
    first_bytes = first.receipt_path.read_bytes()
    canonical = {item.role: Path(item.path) for item in first.receipt.canonical_inputs}
    second = _build_bound_capability(
        tmp_path / "build",
        prices_path=canonical["canonical_prices"],
        rates_path=canonical["canonical_rates"],
        manifest_path=canonical["canonical_manifest"],
    )

    assert first.receipt == second.receipt
    assert first_bytes == second.receipt_path.read_bytes()

    original_receipt = first.receipt_path.read_bytes()
    payload = json.loads(original_receipt)
    payload["consumer_id"] = "ANOTHER_CONSUMER"
    first.receipt_path.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(
        DataQualityCapabilityContractError,
        match="DQ_CAPABILITY_RECEIPT_ID_MISMATCH",
    ):
        verify_consumer_data_capability_receipt(
            first.receipt_path,
            capability_policy_path=CAPABILITY_POLICY_PATH,
            data_quality_policy_path=DATA_QUALITY_POLICY_PATH,
        )

    first.receipt_path.write_bytes(original_receipt)
    panel_path = first.scoped_prices_path
    original_panel = panel_path.read_bytes()
    panel_path.write_bytes(original_panel.replace(b"QQQ", b"QXQ", 1))
    with pytest.raises(
        DataQualityCapabilityContractError,
        match="DQ_CAPABILITY_FILE_BINDING_MISMATCH",
    ):
        verify_consumer_data_capability_receipt(
            first.receipt_path,
            capability_policy_path=CAPABILITY_POLICY_PATH,
            data_quality_policy_path=DATA_QUALITY_POLICY_PATH,
        )
    panel_path.write_bytes(original_panel)

    policy = load_reviewed_consumer_data_capability_policy(CAPABILITY_POLICY_PATH)
    drift = copy.deepcopy(policy.model_dump(mode="python"))
    drift["required_price_tickers"] = ["QQQ", "QQQ", "SGOV"]
    with pytest.raises(ValueError, match="unique values"):
        type(policy).model_validate(drift)


def test_contract_rejects_cross_consumer_reuse_or_global_pass_overclaim(
    tmp_path: Path,
) -> None:
    prices_path, rates_path = _write_sources(
        tmp_path,
        include_out_of_scope_vix_error=True,
    )
    result = _build(tmp_path, prices_path=prices_path, rates_path=rates_path)
    payload = result.receipt.model_dump(mode="python")
    payload["cross_consumer_reuse_allowed"] = True
    with pytest.raises(ValueError, match="cannot authorize reuse"):
        ConsumerDataCapabilityReceipt.model_validate(payload)

    payload = result.receipt.model_dump(mode="python")
    payload["global_cache_pass_claimed"] = True
    with pytest.raises(ValueError, match="global cache claim"):
        ConsumerDataCapabilityReceipt.model_validate(payload)


def test_generic_dependency_discovery_and_verified_preflight_are_exact_and_idempotent(
    tmp_path: Path,
) -> None:
    root, result, dependency = _build_discoverable_capability(tmp_path)
    published_at = datetime(2026, 7, 26, 13, 0, tzinfo=UTC)
    first = publish_consumer_data_capability_discovery(
        dependency=dependency,
        source_receipt_path=result.receipt_path,
        project_root=root,
        published_at=published_at,
    )
    first_pointer_bytes = first.pointer_path.read_bytes()
    second = publish_consumer_data_capability_discovery(
        dependency=dependency,
        source_receipt_path=result.receipt_path,
        project_root=root,
        published_at=published_at,
    )

    assert first.pointer == second.pointer
    assert second.pointer_path.read_bytes() == first_pointer_bytes
    assert second.pointer.dependency_id == dependency.dependency_id
    assert second.pointer.receipt_id == result.receipt.receipt_id
    assert second.retained_receipt_path.read_bytes() == result.receipt.canonical_bytes
    assert second.pointer_path == consumer_data_capability_discovery_path(
        dependency,
        AS_OF,
        project_root=root,
    )

    preflight = verify_consumer_data_capability_preflight(
        dependency=dependency,
        as_of=AS_OF,
        project_root=root,
        verified_at=datetime(2026, 7, 26, 14, 0, tzinfo=UTC),
    )
    assert preflight.consumer_id == dependency.consumer_id
    assert preflight.consumer_version == dependency.consumer_version
    assert preflight.capability_id == dependency.capability_id
    assert preflight.as_of == AS_OF
    assert preflight.receipt == result.receipt
    assert preflight.receipt_path.endswith(f"/{result.receipt.receipt_id}.json")
    assert preflight.receipt.global_cache_pass_claimed is False
    assert preflight.receipt.scoped_quality.status == "PASS"


def test_generic_preflight_rejects_forgery_dependency_drift_and_pointer_tamper(
    tmp_path: Path,
) -> None:
    root, result, dependency = _build_discoverable_capability(tmp_path)
    published = publish_consumer_data_capability_discovery(
        dependency=dependency,
        source_receipt_path=result.receipt_path,
        project_root=root,
        published_at=datetime(2026, 7, 26, 13, 0, tzinfo=UTC),
    )

    with pytest.raises(
        DataQualityCapabilityContractError,
        match="DQ_CAPABILITY_NOT_VERIFIED",
    ):
        VerifiedConsumerDataCapabilityPreflight(
            dependency=dependency,
            receipt=result.receipt,
            pointer_path=published.pointer_path.relative_to(root).as_posix(),
            pointer_sha256="0" * 64,
            receipt_path=published.retained_receipt_path.relative_to(root).as_posix(),
            receipt_sha256="0" * 64,
            receipt_size_bytes=len(result.receipt.canonical_bytes),
            verified_at=datetime(2026, 7, 26, 14, 0, tzinfo=UTC),
            _verification_seal=object(),
        )

    drift_payload = dependency.model_dump(mode="python")
    drift_payload["consumer_version"] = "9.9.9"
    drift = ConsumerDataCapabilityDependency.model_validate(drift_payload)
    with pytest.raises(
        DataQualityCapabilityContractError,
        match="DQ_CAPABILITY_SECURE_READ_FAILED|DQ_CAPABILITY_DEPENDENCY_MISMATCH",
    ):
        verify_consumer_data_capability_preflight(
            dependency=drift,
            as_of=AS_OF,
            project_root=root,
        )

    pointer_payload = json.loads(published.pointer_path.read_text(encoding="utf-8"))
    pointer_payload["receipt_sha256"] = "f" * 64
    published.pointer_path.write_bytes(
        (json.dumps(pointer_payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode(
            "utf-8"
        )
    )
    with pytest.raises(
        DataQualityCapabilityContractError,
        match="DQ_CAPABILITY_FILE_BINDING_MISMATCH",
    ):
        verify_consumer_data_capability_preflight(
            dependency=dependency,
            as_of=AS_OF,
            project_root=root,
        )


def test_generic_publication_rejects_policy_drift_and_immutable_receipt_collision(
    tmp_path: Path,
) -> None:
    root, result, dependency = _build_discoverable_capability(tmp_path)
    published = publish_consumer_data_capability_discovery(
        dependency=dependency,
        source_receipt_path=result.receipt_path,
        project_root=root,
        published_at=datetime(2026, 7, 26, 13, 0, tzinfo=UTC),
    )

    retained_bytes = published.retained_receipt_path.read_bytes()
    published.retained_receipt_path.write_bytes(retained_bytes.replace(b"QQQ", b"QXQ", 1))
    with pytest.raises(
        DataQualityCapabilityContractError,
        match="DQ_CAPABILITY_SECURE_WRITE_FAILED",
    ):
        publish_consumer_data_capability_discovery(
            dependency=dependency,
            source_receipt_path=result.receipt_path,
            project_root=root,
            published_at=datetime(2026, 7, 26, 13, 0, tzinfo=UTC),
        )

    published.retained_receipt_path.write_bytes(retained_bytes)
    policy_path = root / dependency.capability_policy_path
    policy_path.write_bytes(policy_path.read_bytes() + b"\n")
    with pytest.raises(
        DataQualityCapabilityContractError,
        match="DQ_CAPABILITY_DEPENDENCY_MISMATCH",
    ):
        verify_consumer_data_capability_preflight(
            dependency=dependency,
            as_of=AS_OF,
            project_root=root,
        )


def test_generic_publication_requires_strict_pass_and_rejects_pointer_regression(
    tmp_path: Path,
) -> None:
    blocked_root = tmp_path / "blocked"
    blocked_root, blocked, blocked_dependency = _build_discoverable_capability(
        blocked_root,
        include_required_qqq_error=True,
    )
    assert blocked.receipt.capability_passed is False
    with pytest.raises(
        DataQualityCapabilityContractError,
        match="DQ_CAPABILITY_STRICT_PASS_REQUIRED",
    ):
        publish_consumer_data_capability_discovery(
            dependency=blocked_dependency,
            source_receipt_path=blocked.receipt_path,
            project_root=blocked_root,
            published_at=datetime(2026, 7, 26, 13, 0, tzinfo=UTC),
        )

    root, current, dependency = _build_discoverable_capability(
        tmp_path / "regression",
        generated_at=datetime(2026, 7, 26, 14, 0, tzinfo=UTC),
    )
    publish_consumer_data_capability_discovery(
        dependency=dependency,
        source_receipt_path=current.receipt_path,
        project_root=root,
        published_at=datetime(2026, 7, 26, 15, 0, tzinfo=UTC),
    )
    canonical = {item.role: Path(item.path) for item in current.receipt.canonical_inputs}
    older = build_consumer_data_capability(
        capability_policy=load_reviewed_consumer_data_capability_policy(
            root / dependency.capability_policy_path
        ),
        capability_policy_path=root / dependency.capability_policy_path,
        data_quality_policy_path=root / dependency.data_quality_policy_path,
        prices_path=canonical["canonical_prices"],
        rates_path=canonical["canonical_rates"],
        output_root=root / "evidence/older_capability",
        as_of=AS_OF,
        full_expected_price_tickers=["QQQ", "SPY", "SGOV", "^VIX"],
        full_expected_rate_series=["DGS3MO"],
        manifest_path=canonical["canonical_manifest"],
        generated_at=datetime(2026, 7, 26, 13, 0, tzinfo=UTC),
    )
    with pytest.raises(
        DataQualityCapabilityContractError,
        match="DQ_CAPABILITY_DISCOVERY_REGRESSION",
    ):
        publish_consumer_data_capability_discovery(
            dependency=dependency,
            source_receipt_path=older.receipt_path,
            project_root=root,
            published_at=datetime(2026, 7, 26, 16, 0, tzinfo=UTC),
        )


def _build_discoverable_capability(
    root: Path,
    *,
    include_required_qqq_error: bool = False,
    generated_at: datetime = GENERATED_AT,
) -> tuple[Path, ConsumerDataCapabilityBuildResult, ConsumerDataCapabilityDependency]:
    local_capability_policy = (
        root / "config/data_quality/decision_target_label_core_capability_v1.yaml"
    )
    local_data_quality_policy = root / "config/data_quality.yaml"
    local_capability_policy.parent.mkdir(parents=True, exist_ok=True)
    local_capability_policy.write_bytes(CAPABILITY_POLICY_PATH.read_bytes())
    local_data_quality_policy.write_bytes(DATA_QUALITY_POLICY_PATH.read_bytes())
    prices_path, rates_path = _write_sources(
        root / "source",
        include_out_of_scope_vix_error=True,
        include_required_qqq_error=include_required_qqq_error,
    )
    publication = _publish_sources(
        root / "publication",
        prices_path=prices_path,
        rates_path=rates_path,
    )
    result = build_consumer_data_capability(
        capability_policy=load_reviewed_consumer_data_capability_policy(local_capability_policy),
        capability_policy_path=local_capability_policy,
        data_quality_policy_path=local_data_quality_policy,
        prices_path=publication.legacy_prices_path,
        rates_path=publication.legacy_rates_path,
        output_root=root / "evidence/capability",
        as_of=AS_OF,
        full_expected_price_tickers=["QQQ", "SPY", "SGOV", "^VIX"],
        full_expected_rate_series=["DGS3MO"],
        manifest_path=publication.legacy_manifest_path,
        generated_at=generated_at,
    )
    dependency = build_consumer_data_capability_dependency(
        capability_policy_path=local_capability_policy,
        data_quality_policy_path=local_data_quality_policy,
        project_root=root,
    )
    return root, result, dependency


def _build(
    root: Path,
    *,
    prices_path: Path,
    rates_path: Path,
) -> ConsumerDataCapabilityBuildResult:
    publication = _publish_sources(
        root / "publication",
        prices_path=prices_path,
        rates_path=rates_path,
    )
    return _build_bound_capability(
        root,
        prices_path=publication.legacy_prices_path,
        rates_path=publication.legacy_rates_path,
        manifest_path=publication.legacy_manifest_path,
    )


def _build_bound_capability(
    root: Path,
    *,
    prices_path: Path,
    rates_path: Path,
    manifest_path: Path,
) -> ConsumerDataCapabilityBuildResult:
    return build_consumer_data_capability(
        capability_policy=load_reviewed_consumer_data_capability_policy(CAPABILITY_POLICY_PATH),
        capability_policy_path=CAPABILITY_POLICY_PATH,
        data_quality_policy_path=DATA_QUALITY_POLICY_PATH,
        prices_path=prices_path,
        rates_path=rates_path,
        output_root=root / "capability",
        as_of=AS_OF,
        full_expected_price_tickers=["QQQ", "SPY", "SGOV", "^VIX"],
        full_expected_rate_series=["DGS3MO"],
        manifest_path=manifest_path,
        generated_at=GENERATED_AT,
    )


def _publish_sources(
    root: Path,
    *,
    prices_path: Path,
    rates_path: Path,
):
    price_rows = len(pd.read_csv(prices_path))
    rate_rows = len(pd.read_csv(rates_path))
    artifacts = (
        DownloadArtifactCandidate(
            role="prices",
            filename="prices_daily.csv",
            content=prices_path.read_bytes(),
            row_count=price_rows,
            source_event_ids=("prices:capability_fixture",),
        ),
        DownloadArtifactCandidate(
            role="rates",
            filename="rates_daily.csv",
            content=rates_path.read_bytes(),
            row_count=rate_rows,
            source_event_ids=("rates:capability_fixture",),
        ),
    )
    sources = (
        DownloadSourceBinding(
            source_event_id="prices:capability_fixture",
            artifact_role="prices",
            source_kind="LIVE_PROVIDER",
            source_id="capability_prices",
            provider="capability_fixture",
            endpoint="prices",
            request_parameters={
                "start": "2021-02-22",
                "end": AS_OF.isoformat(),
            },
            winning_row_count=price_rows,
            allocation_mode="REMAINDER",
            winning_row_keys=_row_keys(prices_path, "ticker"),
        ),
        DownloadSourceBinding(
            source_event_id="rates:capability_fixture",
            artifact_role="rates",
            source_kind="LIVE_PROVIDER",
            source_id="capability_rates",
            provider="capability_fixture",
            endpoint="rates",
            request_parameters={
                "start": "2021-02-22",
                "end": AS_OF.isoformat(),
            },
            winning_row_count=rate_rows,
            allocation_mode="REMAINDER",
            winning_row_keys=_row_keys(rates_path, "series"),
        ),
    )
    return publish_download_transaction(
        output_dir=root,
        requested_start=date(2021, 2, 22),
        requested_end=AS_OF,
        artifacts=artifacts,
        source_bindings=sources,
        published_at=GENERATED_AT,
    )


def _row_keys(path: Path, identity_column: str) -> tuple[tuple[str, str], ...]:
    frame = pd.read_csv(path, dtype={"date": str, identity_column: str})
    return tuple(
        sorted(
            (str(row[identity_column]), str(row["date"])) for row in frame.to_dict(orient="records")
        )
    )


def _write_sources(
    root: Path,
    *,
    include_out_of_scope_vix_error: bool = False,
    include_required_qqq_error: bool = False,
) -> tuple[Path, Path]:
    root.mkdir(parents=True, exist_ok=True)
    dates = [
        value.date()
        for value in pd.date_range("2021-02-22", AS_OF.isoformat(), freq="D")
        if is_us_equity_trading_day(value.date())
    ]
    rows: list[dict[str, object]] = []
    for ticker in ("QQQ", "SPY", "SGOV", "^VIX"):
        for session in dates:
            value = 20.0 if ticker == "^VIX" else 100.0
            rows.append(
                {
                    "date": session.isoformat(),
                    "ticker": ticker,
                    "open": value,
                    "high": value,
                    "low": value,
                    "close": value,
                    "adj_close": value,
                    "volume": 1_000_000,
                    "source": "test_fixture",
                }
            )
    if include_out_of_scope_vix_error:
        rows.append(
            {
                "date": "2021-05-22",
                "ticker": "^VIX",
                "open": 20.0,
                "high": 20.0,
                "low": 20.0,
                "close": 20.0,
                "adj_close": 20.0,
                "volume": 1_000_000,
                "source": "test_fixture",
            }
        )
    if include_required_qqq_error:
        rows.append(
            {
                "date": "2021-05-22",
                "ticker": "QQQ",
                "open": 100.0,
                "high": 100.0,
                "low": 100.0,
                "close": 100.0,
                "adj_close": 100.0,
                "volume": 1_000_000,
                "source": "test_fixture",
            }
        )
    prices_path = root / "prices_daily.csv"
    pd.DataFrame(rows).to_csv(prices_path, index=False, lineterminator="\n")
    rates_path = root / "rates_daily.csv"
    pd.DataFrame(
        {
            "date": [value.isoformat() for value in dates],
            "series": "DGS3MO",
            "value": 5.0,
            "source": "test_fixture",
        }
    ).to_csv(rates_path, index=False, lineterminator="\n")
    return prices_path, rates_path
