"""Synthetic Composer runtime tests with real S4D/S3a immutable recording.

The source bootstrap, Named DQ verifier/seal and model are explicit doubles.
No test invokes canonical DQ, a real market source, provider or fitted model.
The existing S3b clock/Git fixtures supply the same host-clock and lease boundary.
"""

from __future__ import annotations

import math
import os
import subprocess
from collections.abc import Iterator
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pandas as pd
import pytest
from test_composer_prospective_capture_contract import _manifest, _request, _review
from test_named_data_quality_execution_contract import _receipt as synthetic_receipt
from test_named_quality_dispatch import _git
from test_prospective_capture_execution import _Clock

import ai_trading_system.composer_prospective_capture as capture
import ai_trading_system.data.named_quality_dispatch as dispatch
import ai_trading_system.host_clock_evidence as host_clock
import ai_trading_system.prospective_event_time_evidence as recorder
from ai_trading_system.contracts.composer_prospective_capture import (
    CAPTURE_POLICY_PATH,
    ComposerCaptureError,
    ComposerCaptureRequest,
    ComposerCompletionAcknowledgement,
)
from ai_trading_system.contracts.data_quality_execution import DataQualityDateWindow
from ai_trading_system.contracts.host_clock_evidence import (
    POLICY_PATH as CLOCK_POLICY_PATH,
)
from ai_trading_system.contracts.host_clock_evidence import (
    HostClockEvidence,
    datetime_to_utc_ns,
    require_clock_evidence_extension,
)
from ai_trading_system.contracts.named_data_quality_execution import (
    COMPOSER_INPUT_POLICY_PATH,
    COMPOSER_INPUT_POLICY_SHA256,
    COMPOSER_SCOPE_ORDER,
    NamedArtifactBinding,
    NamedComposerInputScope,
    NamedDQExecutionRequest,
    NamedDQRoots,
)
from ai_trading_system.contracts.prospective_event_time_evidence import (
    canonical_json_bytes,
    strict_json_loads,
)
from ai_trading_system.first_layer_composer_v2_current_session_producer import (
    CurrentSessionPreviewResult,
)
from ai_trading_system.first_layer_operational_forecast import _next_xnys_session, _xnys_sessions
from ai_trading_system.platform.architecture.checkout_guard import (
    CheckoutLeaseGuard,
    CheckoutLeaseHandle,
    CheckoutOperationClass,
)

ROOT = Path(__file__).resolve().parents[1]
ACTIVATED = datetime(2026, 11, 25, 22, tzinfo=UTC)
CAPTURED = datetime(2026, 11, 27, 18, 1, tzinfo=UTC)
_CURRENT_POLICY = "config/research/first_layer_composer_v2_current_session_producer_v1.yaml"


@dataclass
class _Harness:
    root: Path
    request: ComposerCaptureRequest
    bootstrap: Any
    lease: CheckoutLeaseHandle
    clock: _Clock

    def renew(self, at: datetime) -> None:
        if not self.lease.released:
            self.lease.release(outcome="synthetic_clock_transition", at=self.clock.value)
        self.clock.value = at
        decision, lease = self.lease.guard.acquire(
            intent_id="synthetic-composer-renewed",
            task_id="TRADING-2560-SYNTHETIC",
            thread_id="synthetic-composer-parent",
            actor="integration-coordinator",
            operation_class=CheckoutOperationClass.SHARED_MUTATION,
            shared_paths=self.request.required_write_paths,
            base_commit=self.request.candidate_commit,
            now=at - timedelta(seconds=1),
        )
        assert decision.status == "PASS" and lease is not None
        self.lease = lease
        self.bootstrap.source_lease_id = lease.lease_id

    def run(self, request: ComposerCaptureRequest | None = None) -> dict[str, Any]:
        request = request or self.request
        operation = "composer-" + request.operation
        self.bootstrap.operation = operation
        return capture.bootstrap_worker(
            request.to_dict(), operation=operation, bootstrap=self.bootstrap
        )

    def read(self, relative: str) -> dict[str, Any]:
        return cast(dict[str, Any], strict_json_loads((self.root / relative).read_bytes()))


@pytest.fixture
def harness(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[_Harness]:
    root = tmp_path / "synthetic-composer-parent"
    root.mkdir()
    (root / ".gitignore").write_text("outputs/\n", encoding="utf-8")
    dependencies = (
        CAPTURE_POLICY_PATH,
        COMPOSER_INPUT_POLICY_PATH,
        _CURRENT_POLICY,
        recorder.POLICY_PATH,
        CLOCK_POLICY_PATH,
    )
    for relative in (
        "config/architecture/arch_005_s4d_checkout_guard.yaml",
        "config/architecture/arch_005_parallel_control_policy.yaml",
        *dependencies,
        *recorder._CALENDAR_PATHS,
    ):
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes((ROOT / relative).read_bytes())
    _git(root, "init", "-b", "synthetic-composer-parent")
    _git(root, "config", "user.email", "composer@example.invalid")
    _git(root, "config", "user.name", "Synthetic Composer")
    _git(root, "config", "core.autocrlf", "false")
    _git(root, "config", "core.longpaths", "true")
    _git(root, "add", ".gitignore", "config", "src")
    _git(root, "commit", "-m", "synthetic Composer runtime fixture")
    commit = _git(root, "rev-parse", "HEAD")
    output = _manifest().output_relative_path
    manifest = _manifest(
        roots=NamedDQRoots(
            (tmp_path / "synthetic-source").as_posix(),
            (tmp_path / "synthetic-publication").as_posix(),
            root.as_posix(),
            (root / output / "dq").as_posix(),
        ),
        candidate_commit=commit,
    )
    request = _request("activate", manifest=manifest)
    review = _review(manifest)
    target = root / request.owner_review.relative_path
    target.parent.mkdir(parents=True)
    target.write_bytes(review.canonical_bytes)
    guard = CheckoutLeaseGuard(
        project_root=root,
        policy_path=root / "config/architecture/arch_005_s4d_checkout_guard.yaml",
        parallel_policy_path=root / "config/architecture/arch_005_parallel_control_policy.yaml",
    )
    decision, lease = guard.acquire(
        intent_id="synthetic-composer-parent",
        task_id="TRADING-2560-SYNTHETIC",
        thread_id="synthetic-composer-parent",
        actor="integration-coordinator",
        operation_class=CheckoutOperationClass.SHARED_MUTATION,
        shared_paths=request.required_write_paths,
        base_commit=commit,
        now=ACTIVATED - timedelta(minutes=1),
    )
    assert decision.status == "PASS" and lease is not None
    identity = replace(
        synthetic_receipt().execution,
        execution_root=root.as_posix(),
        candidate_commit=commit,
        source_manifest_path=request.source_manifest_path,
        source_manifest_sha256=request.source_manifest_sha256,
    )
    context = SimpleNamespace(
        identity=identity,
        process_id=os.getpid(),
        stable_identity_sha256=identity.stable_identity_sha256,
    )
    clock = _Clock(ACTIVATED)
    bootstrap = SimpleNamespace(
        operation="composer-activate",
        context=context,
        canonical_dq_call_count=0,
        source_lease_id=lease.lease_id,
        dependencies={
            path: SimpleNamespace(content=(root / path).read_bytes()) for path in dependencies
        },
        assert_execution_unchanged=lambda **_: clock.value.isoformat(),
    )
    monkeypatch.setattr(capture, "require_named_execution_context", lambda: context)
    monkeypatch.setattr(dispatch, "require_named_execution_context", lambda: context)
    monkeypatch.setattr(recorder, "SOURCE_ROOT", root)
    monkeypatch.setattr(capture, "_now", clock)
    monkeypatch.setattr(dispatch, "_now", clock)
    monkeypatch.setattr(recorder, "_utc_now", clock)
    monkeypatch.setattr(host_clock, "_utc_ns", lambda: datetime_to_utc_ns(clock.value))
    monkeypatch.setattr(host_clock, "_counter_ns", lambda: 0)
    monkeypatch.setattr(capture, "dispatch_named_quality_child", _forbidden)
    monkeypatch.setattr(capture, "build_known_snapshot_preview", _forbidden)
    value = _Harness(root, request, bootstrap, lease, clock)
    yield value
    if not value.lease.released:
        value.lease.release(
            outcome="synthetic_composer_complete", at=clock.value + timedelta(seconds=1)
        )


def _forbidden(*args: Any, **kwargs: Any) -> Any:
    raise AssertionError("unexpected DQ/model/recorder execution outside the synthetic test step")


@dataclass
class _SyntheticVerified:
    """Test-boundary replacement only; never constructs or exports a real seal."""

    receipt: Any
    segment: str
    prices: bytes
    rates: bytes
    sessions: tuple[date, ...]
    next_session: date
    policy_binding: NamedArtifactBinding

    def inputs_for_composer_segment(
        self, *, required_scope: NamedComposerInputScope
    ) -> tuple[bytes, bytes, tuple[date, ...], date]:
        assert required_scope == NamedComposerInputScope(
            self.receipt.request.scope.as_of, self.segment, self.policy_binding
        )
        return self.prices, self.rates, self.sessions, self.next_session

    def recording_closure_for_composer(self) -> tuple[tuple[str, bytes], ...]:
        return (
            ("input_prices", self.prices),
            ("input_rates", self.rates),
            ("synthetic_receipt", self.receipt.canonical_bytes),
        )


def _synthetic_input_bytes(feature: date) -> tuple[bytes, bytes]:
    sessions = _xnys_sessions(date(2018, 1, 2), feature)
    prices = "date,ticker,adj_close\n" + "".join(
        f"{day.isoformat()},{ticker},{100 + ordinal * 0.01:.4f}\n"
        for ordinal, day in enumerate(sessions)
        for ticker in ("QQQ", "TQQQ", "SHY", "SGOV")
        if ticker != "SGOV" or day >= date(2020, 5, 28)
    )
    rates = "date,series,value\n" + "".join(
        f"{day.isoformat()},{series},{value}\n"
        for day in sessions[:-1]
        for series, value in (("DGS2", 2.0), ("DGS10", 3.0), ("DTWEXBGS", 100.0))
    )
    return prices.encode(), rates.encode()


def _verified_segments(
    request: ComposerCaptureRequest,
    monkeypatch: pytest.MonkeyPatch,
    *,
    prices: bytes | None = None,
    rates: bytes | None = None,
) -> tuple[_SyntheticVerified, ...]:
    assert request.feature_session is not None
    default_prices, default_rates = _synthetic_input_bytes(request.feature_session)
    price_content = prices if prices is not None else default_prices
    rate_content = rates if rates is not None else default_rates
    policy_content = (ROOT / COMPOSER_INPUT_POLICY_PATH).read_bytes()
    binding = NamedArtifactBinding(
        "EXECUTION", COMPOSER_INPUT_POLICY_PATH, capture._sha(policy_content), len(policy_content)
    )
    rows = []
    for segment, dq_request in zip(COMPOSER_SCOPE_ORDER, request.named_dq_requests, strict=True):
        window = dq_request.scope.requested_window
        sessions = _xnys_sessions(window.start, window.end)
        receipt_bytes = canonical_json_bytes(
            {
                "synthetic_verifier_double": True,
                "request": dq_request.to_dict(),
                "prices_sha256": capture._sha(price_content),
                "rates_sha256": capture._sha(rate_content),
            }
        )
        receipt = SimpleNamespace(
            request=dq_request,
            execution_dependencies=(binding,),
            receipt_id="synthetic_receipt_" + capture._sha(receipt_bytes),
            canonical_bytes=receipt_bytes,
            canonical_sha256=capture._sha(receipt_bytes),
            evaluated_window=DataQualityDateWindow(window.start, sessions[-2]),
            report=SimpleNamespace(status="PASS"),
            price_consistency_start_date=date(2021, 2, 22),
            rate_consistency_start_date=date(2021, 2, 22),
            policy=NamedArtifactBinding("EXECUTION", "config/data_quality.yaml", "a" * 64, 1),
            inputs=tuple(
                SimpleNamespace(
                    role=role,
                    member=NamedArtifactBinding(
                        "PUBLICATION",
                        "members/" + role + ".csv",
                        capture._sha(content),
                        len(content),
                    ),
                )
                for role, content in (("prices", price_content), ("rates", rate_content))
            ),
        )
        rows.append(
            _SyntheticVerified(
                receipt,
                segment,
                price_content,
                rate_content,
                sessions,
                _next_xnys_session(request.feature_session),
                binding,
            )
        )
    monkeypatch.setattr(capture, "VerifiedNamedInputs", _SyntheticVerified)
    return tuple(rows)


def _aggregate(
    request: ComposerCaptureRequest, rows: tuple[_SyntheticVerified, ...]
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any], tuple[Any, ...]]:
    return capture._aggregate(request, cast(Any, rows))


def _parent_payload(
    harness: _Harness,
    request: ComposerCaptureRequest,
    segment: str,
    *,
    count: int | None,
    status: str,
    returncode: int = 0,
) -> dict[str, Any]:
    dq_request = request.named_dq_requests[COMPOSER_SCOPE_ORDER.index(segment)]
    return {
        "schema_version": "named_data_quality_parent_dispatch.v1",
        "profile": "COMPOSER_PROSPECTIVE_PRODUCTION_PARENT",
        "status_semantics": "PARENT_ASSOCIATION_AND_PROCESS_OBSERVATION_ONLY",
        "status": status,
        "request_id": dq_request.request_id,
        "candidate_commit": request.candidate_commit,
        "execution_root": request.roots.execution_root,
        "parent_pid": os.getpid(),
        "source_lease_id": harness.lease.lease_id,
        "operation": "run",
        "parent_operation": "composer-" + request.operation,
        "request": {
            "path": (
                harness.root
                / request.operation_relative_path
                / "dq_dispatch"
                / segment
                / "request.json"
            ).as_posix(),
            "sha256": dq_request.canonical_sha256,
            "size_bytes": len(dq_request.canonical_bytes),
        },
        "parent_canonical_dq_call_count": 0,
        "observed_canonical_dq_call_count": count,
        "counter_observation_state": "UNKNOWN" if count is None else "KNOWN",
        "returncode": returncode,
        "terminal_state": "EXITED",
    }


def _dispatch_result(
    harness: _Harness,
    request: ComposerCaptureRequest,
    segment: str,
    *,
    count: int | None = 1,
    status: str = "PASS",
    returncode: int = 0,
) -> dispatch.NamedQualityDispatchResult:
    prefix = request.operation_relative_path + "/dq_dispatch/" + segment + "/"
    dq_request = request.named_dq_requests[COMPOSER_SCOPE_ORDER.index(segment)]
    capture._write(harness.root, prefix + "request.json", dq_request.canonical_bytes)
    parent = _parent_payload(
        harness, request, segment, count=count, status=status, returncode=returncode
    )
    parent_binding = capture._write(
        harness.root, prefix + "parent_receipt.json", canonical_json_bytes(parent)
    )
    return dispatch.NamedQualityDispatchResult(
        status,
        "synthetic/receipt.json" if status == "PASS" else None,
        "a" * 64 if status == "PASS" else None,
        prefix + "successful_run_dispatch.json" if status == "PASS" else None,
        "b" * 64 if status == "PASS" else None,
        parent_binding,
        count,
        "UNKNOWN" if count is None else "KNOWN",
        returncode,
        "EXITED",
    )


def _install_synthetic_pipeline(
    harness: _Harness, request: ComposerCaptureRequest, monkeypatch: pytest.MonkeyPatch
) -> tuple[list[str], tuple[_SyntheticVerified, ...]]:
    verified = _verified_segments(request, monkeypatch)
    observed: list[str] = []

    def dispatch_double(dq_request: NamedDQExecutionRequest, **kwargs: Any) -> Any:
        ordinal = request.named_dq_requests.index(dq_request)
        segment = COMPOSER_SCOPE_ORDER[ordinal]
        observed.append("dq_" + segment)
        assert kwargs["output_relative_path"].endswith("/dq_dispatch/" + segment)
        assert Path(dq_request.roots.evidence_root).is_dir()
        return _dispatch_result(harness, request, segment)

    def verified_double(dq_request: NamedDQExecutionRequest, **kwargs: Any) -> Any:
        return verified[request.named_dq_requests.index(dq_request)]

    original_inputs, original_signal = (
        recorder.record_local_input_observation,
        recorder.record_signal_completion,
    )

    def inputs_double(**kwargs: Any) -> Any:
        event = original_inputs(**kwargs)
        observed.append("inputs_return")
        return event

    def producer_double(**kwargs: Any) -> CurrentSessionPreviewResult:
        assert observed == ["dq_training", "dq_exact_cash", "dq_primary", "inputs_return"]
        assert kwargs["data_quality_status"] == "PASS"
        assert kwargs["prices"].index.max().date() == request.feature_session
        assert kwargs["rates"].index.max().date() < request.feature_session
        observed.append("fit")
        return CurrentSessionPreviewResult(
            preview={
                "schema_version": "first_layer_composer_v2_known_snapshot_preview.v1",
                "status": "SAFE_PREVIEW_READY",
                "feature_session": str(request.feature_session),
                "decision_date": str(_next_xnys_session(request.feature_session)),
                "trend_state": "neutral",
                "confidence": 0.5,
                "action": "FLAT_CASH",
                "observation_identity_preview": {
                    "policy_sha256": COMPOSER_INPUT_POLICY_SHA256,
                    "dq_receipt_sha256": kwargs["dq_receipt_sha256"],
                    "source_sha256": kwargs["source_sha256"],
                    "feature_snapshot_sha256": "c" * 64,
                    "signal_sha256": "d" * 64,
                    "model_sha256": "e" * 64,
                },
                "historical_training_access": {
                    "accessed": True,
                    "role": "CURRENT_REVISION_PREQUENTIAL_TRAINING",
                },
                "input_policy_sha256": COMPOSER_INPUT_POLICY_SHA256,
                "rate_disclosures": [
                    {
                        "series": series,
                        "raw_last_valid_observation_date": "2026-11-25",
                        "effective_carry_source_date": "2026-11-25",
                        "raw_lag_calendar_days": 2,
                        "effective_lag_calendar_days": 2,
                        "carry_applied": True,
                        "input_content_sha256": "f" * 64,
                    }
                    for series in ("DGS10", "DGS2", "DTWEXBGS")
                ],
                "future_observation_outcome_access": False,
                "historical_pit_claim_allowed": False,
            },
            fit_audit=pd.DataFrame(
                [
                    {
                        "model_id": "synthetic_model",
                        "threshold": math.inf,
                        "sample_status": "SAMPLE_INSUFFICIENT",
                        "train_sample_count": 504,
                    }
                ]
            ),
            receipt={"status": "SAFE_PREVIEW_READY", "prospective_capture_count": 0},
        )

    def signal_double(**kwargs: Any) -> Any:
        assert observed[-1] == "fit"
        payload = cast(dict[str, Any], strict_json_loads(kwargs["signal"].content))
        assert payload["fit_audit"][0]["threshold"] == "Infinity"
        assert payload["future_observation_outcome_accessed"] is False
        event = original_signal(**kwargs)
        observed.append("signal_return")
        return event

    monkeypatch.setattr(capture, "dispatch_named_quality_child", dispatch_double)
    monkeypatch.setattr(capture, "verify_named_data_quality_execution_receipt", verified_double)
    monkeypatch.setattr(capture, "record_local_input_observation", inputs_double)
    monkeypatch.setattr(capture, "record_signal_completion", signal_double)
    monkeypatch.setattr(
        capture,
        "load_current_session_producer_policy",
        lambda **_: SimpleNamespace(
            file_sha256=capture._sha(harness.bootstrap.dependencies[_CURRENT_POLICY].content)
        ),
    )
    monkeypatch.setattr(capture, "build_known_snapshot_preview", producer_double)
    return observed, verified


def test_aggregate_preserves_full_prices_earlier_rates_and_three_original_windows(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    request = _request("capture", manifest=harness.request.manifest)
    verified = _verified_segments(request, monkeypatch)
    original = tuple((item.prices, item.rates) for item in verified)
    prices, rates, aggregate, closure = _aggregate(request, verified)
    assert prices.index.min().date() == date(2018, 1, 2)
    assert prices.index.max().date() == request.feature_session
    assert rates.index.max().date() == date(2026, 11, 25)
    assert math.isnan(float(cast(Any, prices.loc["2018-01-02", "SGOV"])))
    assert float(cast(Any, prices.loc["2020-05-28", "SGOV"])) > 0
    assert aggregate["scope_order"] == list(COMPOSER_SCOPE_ORDER)
    assert [row["requested_window"]["start"] for row in aggregate["segments"]] == [
        "2018-01-02",
        "2020-05-28",
        "2021-02-22",
    ]
    assert all(row["evaluated_window"]["end"] == "2026-11-25" for row in aggregate["segments"])
    assert all(row["price_consistency_start_date"] == "2021-02-22" for row in aggregate["segments"])
    assert aggregate["historical_pit_claim_allowed"] is False
    assert aggregate["future_observation_outcome_access_allowed"] is False
    assert {row.role for row in closure} >= {"segmented_dq_identity", "training_input_rates"}
    assert tuple((item.prices, item.rates) for item in verified) == original


@pytest.mark.parametrize("role", ["prices", "rates"])
def test_aggregate_rejects_mixed_segment_bytes_before_csv_parse(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch, role: str
) -> None:
    request = _request("capture", manifest=harness.request.manifest)
    verified = list(_verified_segments(request, monkeypatch))
    verified[1] = replace(verified[1], **{role: getattr(verified[1], role) + b"\n"})
    monkeypatch.setattr(pd, "read_csv", _forbidden)
    with pytest.raises(ComposerCaptureError, match="one price/rates snapshot"):
        _aggregate(request, tuple(verified))


@pytest.mark.parametrize("problem", ["missing_segment", "reordered", "wrong_request", "no_policy"])
def test_aggregate_rejects_incomplete_or_relabelled_verified_segments(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch, problem: str
) -> None:
    request = _request("capture", manifest=harness.request.manifest)
    verified = list(_verified_segments(request, monkeypatch))
    if problem == "missing_segment":
        verified.pop()
    elif problem == "reordered":
        verified[0], verified[1] = verified[1], verified[0]
    elif problem == "wrong_request":
        verified[0].receipt.request = request.named_dq_requests[1]
    else:
        verified[0].receipt.execution_dependencies = ()
    monkeypatch.setattr(pd, "read_csv", _forbidden)
    with pytest.raises(ComposerCaptureError):
        _aggregate(request, tuple(verified))


@pytest.mark.parametrize(
    "problem",
    [
        "future_price",
        "future_rate",
        "duplicate_price",
        "duplicate_rate",
        "invalid_date",
        "missing_training_price",
        "missing_exact_cash",
        "missing_series",
    ],
)
def test_aggregate_rejects_future_duplicate_or_incomplete_input_rows(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch, problem: str
) -> None:
    request = _request("capture", manifest=harness.request.manifest)
    assert request.feature_session is not None
    prices, rates = _synthetic_input_bytes(request.feature_session)
    if problem == "future_price":
        prices += b"2026-11-30,QQQ,120\n"
    elif problem == "future_rate":
        rates += b"2026-11-30,DGS2,2\n"
    elif problem == "duplicate_price":
        prices += b"2018-01-02,QQQ,100\n"
    elif problem == "duplicate_rate":
        rates += b"2018-01-02,DGS2,2\n"
    elif problem == "invalid_date":
        prices += b"invalid,QQQ,100\n"
    elif problem == "missing_training_price":
        prices = b"\n".join(
            line for line in prices.split(b"\n") if not line.startswith(b"2018-01-02,SHY,")
        )
    elif problem == "missing_exact_cash":
        prices = b"\n".join(
            line for line in prices.split(b"\n") if not line.startswith(b"2020-05-28,SGOV,")
        )
    else:
        rates = b"\n".join(line for line in rates.split(b"\n") if b",DGS2," not in line)
    verified = _verified_segments(request, monkeypatch, prices=prices, rates=rates)
    with pytest.raises(ComposerCaptureError):
        _aggregate(request, verified)


def test_fit_audit_serializes_insufficient_sample_infinity_without_changing_sentinel() -> None:
    finite_threshold = 0.12345678912345678
    audit = pd.DataFrame(
        [
            {
                "model_id": "synthetic",
                "threshold": math.inf,
                "sample_status": "SAMPLE_INSUFFICIENT",
                "train_sample_count": 504,
            },
            {
                "model_id": "finite",
                "threshold": finite_threshold,
                "sample_status": "PASS",
                "train_sample_count": 504,
            },
        ]
    )
    result = capture._fit_audit_json(audit)
    encoded = canonical_json_bytes(result)
    assert cast(list[dict[str, Any]], strict_json_loads(encoded))[0]["threshold"] == "Infinity"
    assert result[1]["threshold"] == finite_threshold
    assert (
        cast(list[dict[str, Any]], strict_json_loads(encoded))[1]["threshold"] == finite_threshold
    )
    assert math.isinf(audit.iloc[0]["threshold"])
    assert result[0]["sample_status"] == "SAMPLE_INSUFFICIENT"


@pytest.mark.parametrize(
    "mutation",
    [
        None,
        "source_sha256",
        "decision_date",
        "input_policy_sha256",
        "historical_pit_claim_allowed",
        "future_observation_outcome_access",
        "future_observation_outcome_accessed",
    ],
)
def test_signal_identity_binds_original_source_decision_and_disclosures(
    mutation: str | None,
) -> None:
    request = _request("capture")
    aggregate: dict[str, Any] = {
        "source_snapshot": request.named_dq_requests[0].selector.to_dict(),
        "decision_effective_session": "2026-11-30",
        "rates_content_sha256": "b" * 64,
    }
    event = NamedArtifactBinding("EXECUTION", "synthetic/input.json", "c" * 64, 1)
    # Explicit direct-validation double: this test does not mint a clock or recorder seal.
    returned = SimpleNamespace(
        event=event,
        clock_evidence=SimpleNamespace(admission_bound_ns=123456789),
    )
    preview: dict[str, Any] = {
        "feature_session": "2026-11-27",
        "decision_date": aggregate["decision_effective_session"],
        "input_policy_sha256": COMPOSER_INPUT_POLICY_SHA256,
        "historical_pit_claim_allowed": False,
        "future_observation_outcome_access": False,
        "observation_identity_preview": {
            "policy_sha256": COMPOSER_INPUT_POLICY_SHA256,
            "dq_receipt_sha256": capture._sha(canonical_json_bytes(aggregate)),
            "source_sha256": capture._sha(canonical_json_bytes(aggregate["source_snapshot"])),
        },
    }
    signal = {
        "schema_version": "composer_current_known_recorded_signal.v1",
        "input_event": event.to_dict(),
        "local_known_admission_bound_ns": returned.clock_evidence.admission_bound_ns,
        "preview": preview,
        "rates_content_sha256": aggregate["rates_content_sha256"],
        "future_observation_outcome_accessed": False,
    }
    if mutation == "source_sha256":
        preview["observation_identity_preview"][mutation] = "f" * 64
    elif mutation == "decision_date":
        preview[mutation] = "2026-12-01"
    elif mutation == "input_policy_sha256":
        preview[mutation] = "f" * 64
    elif mutation == "future_observation_outcome_accessed":
        signal[mutation] = True
    elif mutation is not None:
        preview[mutation] = True
    if mutation is None:
        capture._validate_signal_identity(request, signal, aggregate, cast(Any, returned))
    else:
        with pytest.raises(ComposerCaptureError, match="actual source identity"):
            capture._validate_signal_identity(request, signal, aggregate, cast(Any, returned))


def test_activation_uses_real_recording_and_replays_after_expiry_without_clock_or_writes(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    result = harness.run()
    assert result["status"] == "ACTIVATED"
    assert result["canonical_dq_call_count"] == 0 and result["model_fit_call_count"] == 0
    assert result["activation_admitted"] is True and result["real_observation_admitted"] is False
    ack_bytes = (harness.root / result["acknowledgement"]["relative_path"]).read_bytes()
    ack = ComposerCompletionAcknowledgement.from_json_bytes(ack_bytes)
    assert ack.first_feature_session == date(2026, 11, 27)
    assert ack.acknowledgement_own_durability_time_claimed is False
    terminal = HostClockEvidence.from_dict(result["terminal_clock_evidence"])
    require_clock_evidence_extension(ack.clock_evidence, terminal)
    harness.lease.release(outcome="synthetic_complete", at=harness.clock.value)
    harness.clock.value = harness.request.manifest.expires_at + timedelta(days=1)
    for name in (
        "restore_named_capture_lease",
        "record_activation",
        "dispatch_named_quality_child",
        "_write",
    ):
        monkeypatch.setattr(capture, name, _forbidden)
    monkeypatch.setattr(host_clock, "_utc_ns", _forbidden)
    monkeypatch.setattr(host_clock, "_counter_ns", _forbidden)
    replay = harness.run()
    assert replay == {
        **result,
        "idempotent_replay": True,
        "this_invocation_canonical_dq_call_count": 0,
    }
    assert (harness.root / result["acknowledgement"]["relative_path"]).read_bytes() == ack_bytes


def test_readiness_runs_three_synthetic_children_without_model_or_observation(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    request = _request("readiness", manifest=harness.request.manifest)
    assert all(not Path(row.roots.evidence_root).exists() for row in request.named_dq_requests)
    observed, _ = _install_synthetic_pipeline(harness, request, monkeypatch)
    monkeypatch.setattr(capture, "build_known_snapshot_preview", _forbidden)
    monkeypatch.setattr(capture, "record_local_input_observation", _forbidden)
    result = harness.run(request)
    assert result["status"] == "READINESS_PASS"
    assert observed == ["dq_training", "dq_exact_cash", "dq_primary"]
    assert result["canonical_dq_call_count"] == 3
    assert result["real_market_dq_call_count"] == result["model_fit_call_count"] == 0
    assert result["historical_training_accessed"] is False and result["recorder_returns"] == []
    assert not any(
        result[key]
        for key in ("activation_admitted", "capture_admitted", "real_observation_admitted")
    )
    monkeypatch.setattr(capture, "dispatch_named_quality_child", _forbidden)
    assert harness.run(request)["idempotent_replay"] is True


def test_readiness_rejects_declared_evidence_root_escape_before_dispatch(
    harness: _Harness, tmp_path: Path
) -> None:
    request = _request("readiness", manifest=harness.request.manifest)
    payload = request.to_dict()
    outside = tmp_path / "outside-dq-authority"
    named_requests = cast(list[dict[str, Any]], payload["named_dq_requests"])
    named_requests[0]["roots"]["evidence_root"] = outside.as_posix()
    harness.bootstrap.operation = "composer-readiness"
    with pytest.raises(ComposerCaptureError, match="isolated segment receipt root"):
        capture.bootstrap_worker(
            payload, operation="composer-readiness", bootstrap=harness.bootstrap
        )
    assert not outside.exists()
    assert not (harness.root / request.operation_relative_path / "attempt.json").exists()


@pytest.mark.parametrize("component", ["segment", "ancestor"])
def test_readiness_rejects_external_evidence_reparse_before_dispatch(
    harness: _Harness, tmp_path: Path, component: str
) -> None:
    request = _request("readiness", manifest=harness.request.manifest)
    outside = tmp_path / "outside-dq-target"
    outside.mkdir()
    marker = outside / "untouched.txt"
    marker.write_bytes(b"synthetic external target must remain untouched")
    evidence_root = Path(request.named_dq_requests[0].roots.evidence_root)
    link = evidence_root if component == "segment" else evidence_root.parent
    link.parent.mkdir(parents=True, exist_ok=True)
    if os.name == "nt":
        created = subprocess.run(
            ["cmd", "/c", "mklink", "/J", str(link), str(outside)],
            capture_output=True,
            text=True,
            check=False,
        )
        assert created.returncode == 0, created.stderr
    else:
        link.symlink_to(outside, target_is_directory=True)

    result = harness.run(request)
    assert result["status"] == "BLOCKED"
    assert result["reason_code"] == "ARTIFACT_PATH_REPARSE_POINT"
    assert result["canonical_dq_call_count"] == 0
    assert result["counter_observation_state"] == "KNOWN"
    assert result["dq_dispatches"] == []
    assert result["unobserved_dispatch"] is False
    assert result["model_fit_call_count"] == 0 and result["recorder_returns"] == []
    assert not result["activation_admitted"] and not result["capture_admitted"]
    assert list(outside.iterdir()) == [marker]
    assert marker.read_bytes() == b"synthetic external target must remain untouched"


@pytest.mark.parametrize("completed_segments", [1, 2])
def test_readiness_rechecks_live_lease_before_each_segment_directory(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch, completed_segments: int
) -> None:
    request = _request("readiness", manifest=harness.request.manifest)
    verified = _verified_segments(request, monkeypatch)
    observed: list[int] = []

    def child(dq_request: NamedDQExecutionRequest, **kwargs: Any) -> Any:
        ordinal = request.named_dq_requests.index(dq_request)
        assert ordinal < completed_segments
        assert Path(dq_request.roots.evidence_root).is_dir()
        observed.append(ordinal)
        result = _dispatch_result(harness, request, COMPOSER_SCOPE_ORDER[ordinal])
        if len(observed) == completed_segments:
            harness.lease.release(outcome="synthetic_lease_revoked", at=harness.clock.value)
        return result

    monkeypatch.setattr(capture, "dispatch_named_quality_child", child)
    monkeypatch.setattr(
        capture,
        "verify_named_data_quality_execution_receipt",
        lambda req, **_: verified[request.named_dq_requests.index(req)],
    )
    with pytest.raises(ComposerCaptureError) as caught:
        harness.run(request)
    assert caught.value.prospective_child_canonical_dq_call_count == completed_segments
    assert observed == list(range(completed_segments))
    for row in request.named_dq_requests[completed_segments:]:
        assert not Path(row.roots.evidence_root).exists()
    assert not (harness.root / request.operation_relative_path / "result.json").exists()


def test_complete_capture_orders_input_return_before_fit_and_records_original_known_time(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert harness.run()["status"] == "ACTIVATED"
    harness.renew(CAPTURED)
    request = _request("capture", manifest=harness.request.manifest)
    observed, _ = _install_synthetic_pipeline(harness, request, monkeypatch)
    result = harness.run(request)
    assert result["status"] == "CAPTURED"
    assert observed == [
        "dq_training",
        "dq_exact_cash",
        "dq_primary",
        "inputs_return",
        "fit",
        "signal_return",
    ]
    assert result["canonical_dq_call_count"] == 3 and result["model_fit_call_count"] == 1
    assert result["historical_training_accessed"] is True
    assert result["capture_admitted"] is True and result["real_observation_admitted"] is False
    assert result["future_observation_outcome_accessed"] is False
    observation = harness.read(result["observation"]["relative_path"])
    ack = ComposerCompletionAcknowledgement.from_json_bytes(
        (harness.root / result["acknowledgement"]["relative_path"]).read_bytes()
    )
    assert (
        observation["local_known_admission_bound_ns"]
        == ack.recorder_returns[0].clock_evidence.admission_bound_ns
    )
    assert observation["decision_effective_session"] == "2026-11-30"
    assert observation["outcome_status"] == "MATURITY_NOT_EVALUATED"
    assert observation["historical_training_access"]["accessed"] is True
    assert observation["input_policy_sha256"] == COMPOSER_INPUT_POLICY_SHA256
    assert all(row["effective_lag_calendar_days"] == 2 for row in observation["rate_disclosures"])
    assert observation["future_observation_outcome_accessed"] is False
    for name in (
        "dispatch_named_quality_child",
        "build_known_snapshot_preview",
        "record_local_input_observation",
        "record_signal_completion",
        "_write",
    ):
        monkeypatch.setattr(capture, name, _forbidden)
    assert harness.run(request)["idempotent_replay"] is True
    assert len(observed) == 6


def test_incomplete_attempt_is_not_retried_after_expiry(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert capture._reserve(
        harness.request, harness.bootstrap, harness.lease, _review(harness.request.manifest)
    )
    original = (
        harness.root / harness.request.operation_relative_path / "attempt.json"
    ).read_bytes()
    harness.lease.release(outcome="synthetic_partial", at=harness.clock.value)
    harness.clock.value = harness.request.manifest.expires_at + timedelta(days=1)
    monkeypatch.setattr(capture, "restore_named_capture_lease", _forbidden)
    monkeypatch.setattr(capture, "record_activation", _forbidden)
    result = harness.run()
    assert result["status"] == "INCOMPLETE" and result["idempotent_replay"] is True
    assert (
        result["canonical_dq_call_count"] is None
        and result["counter_observation_state"] == "UNKNOWN"
    )
    assert (
        harness.root / harness.request.operation_relative_path / "attempt.json"
    ).read_bytes() == original


@pytest.mark.parametrize("status", ["ACTIVATED", "BLOCKED"])
def test_retained_result_rejects_changed_authority_counters_pid_or_event(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch, status: str
) -> None:
    if status == "BLOCKED":

        def fail_activation(**kwargs: Any) -> Any:
            raise RuntimeError("synthetic activation failure")

        monkeypatch.setattr(capture, "record_activation", fail_activation)
    result = harness.run()
    assert result["status"] == status
    result_path = harness.root / harness.request.operation_relative_path / "result.json"
    for field, value in (
        ("real_observation_admitted", True),
        ("capture_admitted", True),
        ("parent_pid", True),
        ("parent_canonical_dq_call_count", 1),
        ("canonical_dq_call_count", 1),
        ("canonical_dq_call_count", False),
        ("real_market_dq_call_count", False),
        ("model_fit_call_count", False),
        ("counter_observation_state", "UNKNOWN"),
        ("future_observation_outcome_accessed", True),
        ("orders", 1),
        ("retry_allowed", True),
    ):
        result_path.write_bytes(canonical_json_bytes({**result, field: value}))
        with pytest.raises(ValueError):
            harness.run()
    result_path.write_bytes(canonical_json_bytes(result))
    assert harness.run()["status"] == status


@pytest.mark.parametrize(
    "problem", ["segment", "count", "state", "returncode", "terminal", "parent_bytes"]
)
def test_dispatch_rows_reject_tampering_with_successful_child_projection(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch, problem: str
) -> None:
    request = _request("readiness", manifest=harness.request.manifest)
    dq = _dispatch_result(harness, request, "training")
    row = capture._dq_row("training", dq)
    changes = {
        "segment": ("segment", "primary"),
        "count": ("canonical_dq_call_count", 0),
        "state": ("counter_observation_state", "UNKNOWN"),
        "returncode": ("returncode", 1),
        "terminal": ("terminal_state", "TIMED_OUT_AND_CHILD_REAPED"),
    }
    if problem == "parent_bytes":
        (harness.root / dq.parent_receipt.relative_path).write_bytes(b"{}")
    else:
        key, value = changes[problem]
        row[key] = value
    monkeypatch.setattr(capture, "verify_named_data_quality_execution_receipt", _forbidden)
    with pytest.raises(ComposerCaptureError):
        capture._verify_dispatch_rows(request, [row], harness.bootstrap)


def test_failed_dispatch_count_must_equal_original_parent_observation(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    request = _request("readiness", manifest=harness.request.manifest)
    dq = _dispatch_result(harness, request, "training", count=1, status="BLOCKED", returncode=1)
    row = capture._dq_row("training", dq)
    monkeypatch.setattr(capture, "verify_named_data_quality_execution_receipt", _forbidden)
    assert capture._verify_dispatch_rows(request, [row], harness.bootstrap) == ()
    for value in (0, None, True):
        with pytest.raises(ComposerCaptureError):
            capture._verify_dispatch_rows(
                request, [{**row, "canonical_dq_call_count": value}], harness.bootstrap
            )


@pytest.mark.parametrize("failure_index", [0, 1, 2])
def test_failed_child_stops_remaining_segments_and_preserves_actual_attempt_count(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch, failure_index: int
) -> None:
    request = _request("readiness", manifest=harness.request.manifest)
    verified = _verified_segments(request, monkeypatch)
    observed = []

    def child(dq_request: NamedDQExecutionRequest, **kwargs: Any) -> Any:
        ordinal = request.named_dq_requests.index(dq_request)
        observed.append(ordinal)
        return _dispatch_result(
            harness,
            request,
            COMPOSER_SCOPE_ORDER[ordinal],
            status="BLOCKED" if ordinal == failure_index else "PASS",
            returncode=1 if ordinal == failure_index else 0,
        )

    monkeypatch.setattr(capture, "dispatch_named_quality_child", child)
    monkeypatch.setattr(
        capture,
        "verify_named_data_quality_execution_receipt",
        lambda req, **_: verified[request.named_dq_requests.index(req)],
    )
    monkeypatch.setattr(capture, "record_local_input_observation", _forbidden)
    result = harness.run(request)
    assert result["status"] == "BLOCKED" and result["canonical_dq_call_count"] == failure_index + 1
    assert result["counter_observation_state"] == "KNOWN"
    assert result["model_fit_call_count"] == 0 and result["recorder_returns"] == []
    assert observed == list(range(failure_index + 1))
    monkeypatch.setattr(capture, "dispatch_named_quality_child", _forbidden)
    assert harness.run(request)["idempotent_replay"] is True


@pytest.mark.parametrize("parent_published", [True, False])
def test_partial_dispatch_publication_preserves_known_counter_without_retry(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch, parent_published: bool
) -> None:
    request = _request("readiness", manifest=harness.request.manifest)
    calls = []

    def fail_dispatch(*args: Any, **kwargs: Any) -> Any:
        calls.append(1)
        parent_binding = None
        if parent_published:
            parent_binding = _dispatch_result(harness, request, "training").parent_receipt
        raise dispatch.NamedQualityDispatchError(
            "NAMED_PARENT_TERMINAL_PUBLICATION_FAILED",
            "synthetic observed one call",
            terminal_observation=dispatch.NamedQualityTerminalObservation(
                canonical_dq_call_count=1,
                counter_observation_state="KNOWN",
                returncode=0,
                terminal_state="EXITED",
                parent_receipt=parent_binding,
            ),
        )

    monkeypatch.setattr(capture, "dispatch_named_quality_child", fail_dispatch)
    monkeypatch.setattr(capture, "record_local_input_observation", _forbidden)
    result_path = harness.root / request.operation_relative_path / "result.json"
    result = harness.run(request)
    assert result["status"] == "BLOCKED"
    assert result["canonical_dq_call_count"] == 1
    assert result["counter_observation_state"] == "KNOWN"
    assert result["failed_dispatch_terminal_observation"]["evidence_state"] == (
        "PARENT_BINDING_VERIFIED" if parent_published else "ORIGINAL_PARENT_OBSERVATION_ONLY"
    )
    assert not result["capture_admitted"] and not result["real_observation_admitted"]
    assert result["technical_validation_state"] == "BLOCKED"
    assert result_path.exists()
    monkeypatch.setattr(capture, "dispatch_named_quality_child", _forbidden)
    replay = harness.run(request)
    assert replay["status"] == "BLOCKED"
    assert replay["idempotent_replay"] is True and calls == [1]
    result_path.write_bytes(
        canonical_json_bytes({**result, "status": "CAPTURED", "capture_admitted": True})
    )
    with pytest.raises(ValueError):
        harness.run(request)


@pytest.mark.parametrize(
    "counts,terminal,unobserved,expected",
    [
        ([1, 1], 1, False, (3, "KNOWN")),
        ([1, 1], 2, False, (4, "KNOWN")),
        ([1, 1], None, True, (None, "UNKNOWN")),
        ([1, None], None, False, (None, "UNKNOWN")),
        ([True], None, False, (None, "UNKNOWN")),
        ([-1], None, False, (None, "UNKNOWN")),
        ([], None, False, (0, "KNOWN")),
    ],
)
def test_aggregate_counters_preserve_observed_excess_and_distinguish_unknown(
    counts: list[int | None],
    terminal: int | None,
    unobserved: bool,
    expected: tuple[int | None, str],
) -> None:
    result = {
        "dq_dispatches": [{"canonical_dq_call_count": value} for value in counts],
        "unobserved_dispatch": unobserved,
    }
    if terminal is not None:
        result["failed_dispatch_terminal_observation"] = {"canonical_dq_call_count": terminal}
    assert capture._actual_counter(result) == expected


def test_input_writer_failure_prevents_fit_and_second_capture_attempt(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert harness.run()["status"] == "ACTIVATED"
    harness.renew(CAPTURED)
    request = _request("capture", manifest=harness.request.manifest)
    observed, _ = _install_synthetic_pipeline(harness, request, monkeypatch)

    def failed_inputs(**kwargs: Any) -> Any:
        raise RuntimeError("synthetic incomplete input writer")

    monkeypatch.setattr(capture, "record_local_input_observation", failed_inputs)
    monkeypatch.setattr(capture, "build_known_snapshot_preview", _forbidden)
    result = harness.run(request)
    assert result["status"] == "BLOCKED" and result["model_fit_call_count"] == 0
    assert result["canonical_dq_call_count"] == 3 and result["recorder_returns"] == []
    assert observed == ["dq_training", "dq_exact_cash", "dq_primary"]
    monkeypatch.setattr(capture, "dispatch_named_quality_child", _forbidden)
    assert harness.run(request)["idempotent_replay"] is True


def test_preclose_capture_is_blocked_before_any_child_or_input_writer(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert harness.run()["status"] == "ACTIVATED"
    harness.renew(CAPTURED - timedelta(minutes=2))
    request = _request("capture", manifest=harness.request.manifest)
    monkeypatch.setattr(capture, "record_local_input_observation", _forbidden)
    result = harness.run(request)
    assert result["status"] == "BLOCKED"
    assert result["reason_code"] == "COMPOSER_OUTSIDE_FEATURE_WINDOW"
    assert result["canonical_dq_call_count"] == 0 and result["model_fit_call_count"] == 0


def test_terminal_projection_must_pass_before_immutable_result_publication(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = capture._verify_composer_projection

    def reject_candidate(
        request: ComposerCaptureRequest, result: dict[str, Any], **kwargs: Any
    ) -> Any:
        if kwargs.get("terminal_required") is False:
            raise ComposerCaptureError("SYNTHETIC_PRECOMMIT_REJECTED", "before result writer")
        return original(request, result, **kwargs)

    monkeypatch.setattr(capture, "_verify_composer_projection", reject_candidate)
    with pytest.raises(ComposerCaptureError):
        harness.run()
    assert not (harness.root / harness.request.operation_relative_path / "result.json").exists()
    assert harness.run()["status"] == "INCOMPLETE"
