"""S3b exact-checkout production parent/seal test with synthetic input bytes.

The fixed TEST_PROBE uses the real committed bootstrap, S4D lease, isolated DQ
child and verifier. It never fabricates a seal, clock, lease or temporal ACK.
Positive multi-session capture timing is covered by the separate synthetic
clock/recorder tests; this test does not claim an actual forward observation.
"""

from __future__ import annotations

import hashlib
import json
import math
import subprocess
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

from named_data_quality_support import (
    BOOTSTRAP_PATH,
    ROOT,
    _current_child_python_launch,
    _git,
    _live_parent_proof,
    _parent_environment,
    build_actual_candidate_fixture,
)

from ai_trading_system.contracts.data_quality_execution import DataQualityDateWindow
from ai_trading_system.contracts.named_data_quality_execution import (
    EQUAL_RISK_GUARD_RATE_SERIES,
    EQUAL_RISK_PRICE_TICKERS,
    PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_PATH,
    NamedArtifactBinding,
    NamedDQExecutionReceipt,
    NamedDQRoots,
)
from ai_trading_system.contracts.prospective_capture_execution import (
    ParentCompletionAcknowledgement,
    ProspectiveCaptureManifest,
    ProspectiveCaptureOwnerReview,
    ProspectiveCaptureRequest,
)
from ai_trading_system.prospective_event_time_evidence import (
    first_feature_session,
    load_time_evidence_policy,
)
from ai_trading_system.trading_calendar import is_us_equity_trading_day

# Finite test-owned code. Production has no arbitrary probe or seal-import API.
_PROSPECTIVE_PARENT_SEAL_PROBE = r"""
import hashlib
import importlib
import json
import os
from pathlib import Path
import runpy
import sys

bootstrap_path, request_path, request_sha, lease_id, output = sys.argv[1:]
entry = runpy.run_path(bootstrap_path)
raw = entry["_initial_file_bytes"](Path(request_path).parent, Path(request_path).name)
assert hashlib.sha256(raw).hexdigest() == request_sha
request_dict = entry["_json_object"](raw)
session = entry["NamedBootstrapSession"](
    request_dict, operation="capture", source_lease_id=lease_id
)
try:
    session.load()
    contracts = importlib.import_module("ai_trading_system.contracts.named_data_quality_execution")
    request = contracts.NamedDQExecutionRequest.from_dict(request_dict)
    dispatch = importlib.import_module("ai_trading_system.data.named_quality_dispatch")
    worker = importlib.import_module(entry["WORKER_MODULE"])
    preview_module = importlib.import_module("ai_trading_system.simple_baseline_named_preview")
    lease = dispatch.restore_named_capture_lease(
        execution_root=session.root, source_lease_id=lease_id,
        candidate_commit=request.candidate_commit, required_paths=(output,),
    )
    result = dispatch.dispatch_named_quality_child(
        request, bootstrap=session, lease=lease, output_relative_path=output + "/dq_dispatch",
    )
    assert result.status == "PASS", result
    assert result.canonical_dq_call_count == 1 and result.counter_observation_state == "KNOWN"
    verified = worker.verify_named_data_quality_execution_receipt(
        request, receipt_path=result.receipt_path, receipt_sha256=result.receipt_sha256,
        run_dispatch_path=result.run_dispatch_path, run_dispatch_sha256=result.run_dispatch_sha256,
        bootstrap=session,
    )
    receipt = verified.receipt
    registry = next(row for row in receipt.execution_dependencies
                    if row.relative_path == contracts.EQUAL_RISK_PRICE_REGISTRY_PATH)
    scope = contracts.NamedEqualRiskPriceScope(
        as_of=request.scope.as_of, requested_window=request.scope.requested_window,
        registry_binding=registry,
    )
    preview = preview_module.build_prospective_simple_baseline_preview(
        verified, required_scope=scope
    )
    prices, registry_bytes, sessions, next_session = (
        verified.inputs_for_prospective_five_candidate_preview(required_scope=scope)
    )
    rebuilt = preview_module.rebuild_prospective_simple_baseline_preview(
        receipt=receipt, successful_dispatch=verified.successful_dispatch,
        required_scope=scope, prices=prices, registry=registry_bytes,
        sessions=sessions, next_session=next_session,
    )
    assert preview == rebuilt
    closure = dict(verified.recording_closure_for_prospective())
    manifest = json.loads(closure["closure_manifest"])
    canonical = json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    assert canonical.encode() == closure["closure_manifest"]
    assert set(closure) == {row["role"] for row in manifest["members"]} | {"closure_manifest"}
    for row in manifest["members"]:
        content = closure[row["role"]]
        assert len(content) == row["binding"]["size_bytes"]
        assert hashlib.sha256(content).hexdigest() == row["binding"]["sha256"]
    assert closure["input_prices"] == prices
    assert contracts.NamedDQExecutionReceipt.from_json_bytes(closure["dq_receipt"]) == receipt
    for accessor in (
        verified.prices_for_equal_risk_preview, verified.inputs_for_five_candidate_preview
    ):
        try:
            accessor(required_scope=scope)
        except ValueError:
            pass
        else:
            raise AssertionError("old exact-profile access must stay closed")
    assert session.canonical_dq_call_count == 0
    output_root = session.root / output
    writer = importlib.import_module("ai_trading_system.data.immutable_publish")
    for role, content in closure.items():
        writer.write_contained_artifact_bytes(
            root=session.root, relative_path=output + "/closure/" + role + ".bin",
            content=content, immutable=True,
        )
    session.assert_execution_unchanged(stage="TERMINAL")
    dispatch.recheck_named_capture_lease(
        lease, candidate_commit=request.candidate_commit, required_paths=(output,)
    )
    result_dict = {
        "schema_version": "prospective_parent_seal_actual_candidate_test.v1",
        "profile": "TEST_PROBE_SYNTHETIC_INPUTS_WITH_REAL_SOURCE_AND_LEASE",
        "status": "PASS", "parent_pid": os.getpid(),
        "child_pid": receipt.execution_observation.execution_pid,
        "candidate_commit": request.candidate_commit,
        "parent_started_at": session.started_at,
        "parent_terminal_checked_at": session.terminal_checked_at,
        "execution_identity": receipt.execution.to_dict(),
        "parent_canonical_dq_call_count": session.canonical_dq_call_count,
        "child_canonical_dq_call_count": result.canonical_dq_call_count,
        "real_market_dq_call_count": 0, "real_observation_count": 0,
        "activation_or_capture_executed": False,
        "source_clock_or_lease_replaced": False,
        "receipt_path": result.receipt_path, "receipt_sha256": result.receipt_sha256,
        "dq_parent_receipt": result.parent_receipt.to_dict(),
        "closure_manifest": manifest, "preview": preview,
        "complete_recomputation_equal": True,
        "production_effect": "none", "broker_action": "none",
    }
    print(json.dumps(result_dict, ensure_ascii=False, sort_keys=True, allow_nan=False))
finally:
    session.close()
"""


def test_exact_candidate_production_parent_mints_new_profile_seal_and_complete_closure(
    tmp_path: Path,
) -> None:
    start, end = date(2021, 2, 22), date(2025, 1, 8)
    days = []
    day = start
    while day <= end:
        if is_us_equity_trading_day(day):
            days.append(day)
        day += timedelta(days=1)
    prices = ["date,ticker,open,high,low,close,adj_close,volume\n"]
    for index, day in enumerate(days):
        for ticker in EQUAL_RISK_PRICE_TICKERS:
            amplitude = 0.0001 if ticker == "SGOV" else 0.005
            price = 100 * math.exp(0.0001 * index + amplitude * math.sin(index / 7))
            prices.append(
                f"{day},{ticker},{price},{price * 1.01},{price * 0.99},{price},{price},1000\n"
            )
    rates = ["date,series,value\n"]
    for day in days[:-1]:
        for series in EQUAL_RISK_GUARD_RATE_SERIES:
            rates.append(f"{day},{series},{110 if series == 'DTWEXBGS' else 1.5}\n")
    fixture = build_actual_candidate_fixture(
        tmp_path,
        prices_content="".join(prices).encode(),
        rates_content="".join(rates).encode(),
        requested_start=start,
        requested_end=end,
        as_of=end,
        expected_price_tickers=EQUAL_RISK_PRICE_TICKERS,
        expected_rate_series=EQUAL_RISK_GUARD_RATE_SERIES,
        prospective_capture_profile=True,
        expected_evaluated_window=DataQualityDateWindow(start, days[-2]),
    )
    transaction, lease_id = _parent_environment()
    relative = (
        "outputs/architecture/trading_2564_s3b_prospective_capture/synthetic/exact-candidate/"
        + uuid4().hex
    )
    output = ROOT / relative
    evidence = output / "dq"
    request = replace(
        fixture.request, roots=replace(fixture.request.roots, evidence_root=evidence.as_posix())
    )
    before = _live_parent_proof(request=request, transaction_input=transaction, lease_id=lease_id)
    evidence.mkdir(parents=True)
    request_path = output / "request.json"
    request_path.write_bytes(request.canonical_bytes)
    (output / "test_parent_pre_guard.json").write_text(
        json.dumps(before, ensure_ascii=False), encoding="utf-8"
    )
    launch = _current_child_python_launch()
    command = [
        launch.executable,
        "-I",
        "-B",
        "-X",
        "utf8",
        "-c",
        _PROSPECTIVE_PARENT_SEAL_PROBE,
        str(ROOT / BOOTSTRAP_PATH),
        str(request_path),
        request.canonical_sha256,
        lease_id,
        relative,
    ]
    started = datetime.now(UTC)
    with subprocess.Popen(
        command, cwd=ROOT, env=launch.environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    ) as process:
        try:
            stdout, stderr = process.communicate(timeout=300)
        except subprocess.TimeoutExpired:
            process.kill()
            stdout, stderr = process.communicate()
        child_pid, returncode = process.pid, process.returncode
    terminal = datetime.now(UTC)
    (output / "stdout.json").write_bytes(stdout)
    (output / "stderr.txt").write_bytes(stderr)
    after = _live_parent_proof(request=request, transaction_input=transaction, lease_id=lease_id)
    assert before["request_sha256"] == after["request_sha256"] == request.canonical_sha256
    (output / "test_parent_post_guard.json").write_text(
        json.dumps(after, ensure_ascii=False), encoding="utf-8"
    )
    (output / "test_parent_receipt.json").write_text(
        json.dumps(
            {
                "schema_version": "prospective_exact_candidate_test_parent.v1",
                "request_sha256": request.canonical_sha256,
                "candidate_commit": request.candidate_commit,
                "child_pid": child_pid,
                "returncode": returncode,
                "spawn_requested_at": started.isoformat(),
                "terminal_observed_at": terminal.isoformat(),
                "launch_audit": launch.audit,
                "command": command,
                "fixed_probe_sha256": hashlib.sha256(
                    _PROSPECTIVE_PARENT_SEAL_PROBE.encode()
                ).hexdigest(),
                "stdout_sha256": hashlib.sha256(stdout).hexdigest(),
                "stderr_sha256": hashlib.sha256(stderr).hexdigest(),
                "synthetic_inputs_only": True,
                "real_observation_count": 0,
                "production_effect": "none",
                "broker_action": "none",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    assert returncode == 0, stderr.decode("utf-8", errors="replace")
    result = json.loads(stdout)
    assert result["status"] == "PASS"
    assert result["parent_pid"] != result["child_pid"]
    assert result["parent_pid"] == child_pid
    assert (
        started
        <= datetime.fromisoformat(result["parent_started_at"])
        <= datetime.fromisoformat(result["parent_terminal_checked_at"])
        <= terminal
    )
    assert result["parent_canonical_dq_call_count"] == 0
    assert result["child_canonical_dq_call_count"] == 1
    assert result["real_market_dq_call_count"] == result["real_observation_count"] == 0
    assert result["activation_or_capture_executed"] is False
    assert result["source_clock_or_lease_replaced"] is False
    assert result["candidate_commit"] == request.candidate_commit
    identity = result["execution_identity"]
    assert identity["source_manifest_path"] == PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_PATH
    assert len(identity["modules"]) == 85
    receipt_bytes = (evidence / result["receipt_path"]).read_bytes()
    assert hashlib.sha256(receipt_bytes).hexdigest() == result["receipt_sha256"]
    receipt = NamedDQExecutionReceipt.from_json_bytes(receipt_bytes)
    assert len(receipt.execution_dependencies) == 12
    assert receipt.report.status == "PASS"
    assert receipt.evaluated_window == DataQualityDateWindow(start, days[-2])
    assert result["preview"]["feature_session"] == end.isoformat()
    assert result["preview"]["decision_effective_session"] == "2025-01-10"
    assert result["complete_recomputation_equal"] is True
    assert len(result["closure_manifest"]["members"]) == 29
    assert {
        row["role"]
        for row in result["closure_manifest"]["members"]
        if row["semantic_role"] == "DQ_GUARD_ONLY"
    } == {"input_rates"}


def test_exact_candidate_real_clock_synthetic_activation_and_read_only_duplicate(
    tmp_path: Path,
) -> None:
    """Run the production CLI twice at actual host time, with no market inputs."""
    now = datetime.now(UTC)
    policy = load_time_evidence_policy(source_root=ROOT)
    first = first_feature_session(now, policy=policy)
    manifest_id = "actual_source_activation_" + uuid4().hex + "_v1"
    relative = "outputs/architecture/trading_2564_s3b_prospective_capture/synthetic/" + manifest_id
    output = ROOT / relative
    roots = NamedDQRoots(
        source_root=(tmp_path / "synthetic-source").as_posix(),
        publication_root=(tmp_path / "synthetic-publication").as_posix(),
        execution_root=ROOT.as_posix(),
        evidence_root=(output / "dq").as_posix(),
    )
    manifest = ProspectiveCaptureManifest(
        manifest_id=manifest_id,
        roots=roots,
        candidate_commit=_git(ROOT, "rev-parse", "HEAD"),
        source_output_relative_path="outputs/synthetic_source",
        output_relative_path=relative,
        allowed_feature_sessions=(first,),
        expires_at=now + timedelta(days=7),
        evidence_purpose="SYNTHETIC_ENGINEERING",
        require_secondary_prices=False,
    )
    review = ProspectiveCaptureOwnerReview(
        manifest_id=manifest_id,
        manifest_sha256=manifest.canonical_sha256,
        decision_ref="synthetic_review:TRADING-2564:actual-source-activation-test",
        reviewed_at=now,
        authorization_state="STANDING_OWNER_SCOPE",
        evidence_purpose="SYNTHETIC_ENGINEERING",
    )
    request = ProspectiveCaptureRequest(
        operation="activate",
        manifest=manifest,
        owner_review=NamedArtifactBinding(
            "EXECUTION",
            relative + "/control/owner_review.json",
            review.canonical_sha256,
            len(review.canonical_bytes),
        ),
        roots=roots,
        candidate_commit=manifest.candidate_commit,
        source_manifest_path=manifest.source_manifest_path,
        source_manifest_sha256=manifest.source_manifest_sha256,
        policy_path=manifest.policy_path,
        feature_session=None,
        named_dq_request=None,
    )
    transaction, lease_id = _parent_environment()
    _live_parent_proof(request=request, transaction_input=transaction, lease_id=lease_id)
    control = output / "control"
    control.mkdir(parents=True)
    (control / "owner_review.json").write_bytes(review.canonical_bytes)
    request_path = control / "activation_request.json"
    request_path.write_bytes(request.canonical_bytes)
    launch = _current_child_python_launch()
    results: list[dict[str, Any]] = []
    original_bytes = None
    for invocation in range(2):
        before = _live_parent_proof(
            request=request, transaction_input=transaction, lease_id=lease_id
        )
        command = [
            launch.executable,
            "-I",
            "-B",
            "-X",
            "utf8",
            str(ROOT / BOOTSTRAP_PATH),
            "--operation",
            "activate",
            "--request",
            str(request_path),
            "--request-sha256",
            request.canonical_sha256,
            "--source-lease-id",
            lease_id,
        ]
        started = datetime.now(UTC)
        with subprocess.Popen(
            command,
            cwd=ROOT,
            env=launch.environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        ) as process:
            try:
                stdout, stderr = process.communicate(timeout=300)
            except subprocess.TimeoutExpired:
                process.kill()
                stdout, stderr = process.communicate()
            pid, returncode = process.pid, process.returncode
        terminal = datetime.now(UTC)
        after = _live_parent_proof(
            request=request, transaction_input=transaction, lease_id=lease_id
        )
        (output / f"activation_cli_{invocation}_stdout.json").write_bytes(stdout)
        (output / f"activation_cli_{invocation}_stderr.txt").write_bytes(stderr)
        (output / f"activation_cli_{invocation}_parent.json").write_text(
            json.dumps(
                {
                    "schema_version": "prospective_activation_actual_candidate_test_parent.v1",
                    "candidate_commit": request.candidate_commit,
                    "request_sha256": request.canonical_sha256,
                    "child_pid": pid,
                    "returncode": returncode,
                    "command": command,
                    "launch_audit": launch.audit,
                    "started_at": started.isoformat(),
                    "terminal_at": terminal.isoformat(),
                    "pre_guard": before,
                    "post_guard": after,
                    "stdout_sha256": hashlib.sha256(stdout).hexdigest(),
                    "stderr_sha256": hashlib.sha256(stderr).hexdigest(),
                    "synthetic_inputs_only": True,
                    "real_market_dq_call_count": 0,
                    "production_effect": "none",
                    "broker_action": "none",
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        assert returncode == 0, (stdout, stderr)
        result = json.loads(stdout)
        assert result["status"] == "ACTIVATED", result
        assert result["activation_admitted"] is True
        assert result["capture_admitted"] is result["real_observation_admitted"] is False
        assert result["canonical_dq_call_count"] == result["parent_canonical_dq_call_count"] == 0
        assert result["idempotent_replay"] is bool(invocation)
        if invocation == 0:
            assert result["parent_pid"] == pid
            original_bytes = (output / "activation/result.json").read_bytes()
        else:
            assert result["parent_pid"] == results[0]["parent_pid"]
            assert result["this_invocation_canonical_dq_call_count"] == 0
            assert (output / "activation/result.json").read_bytes() == original_bytes
        ack_binding = NamedArtifactBinding.from_dict(result["acknowledgement"])
        ack_bytes = (ROOT / ack_binding.relative_path).read_bytes()
        assert hashlib.sha256(ack_bytes).hexdigest() == ack_binding.sha256
        ack = ParentCompletionAcknowledgement.from_json_bytes(ack_bytes)
        assert ack.first_feature_session == first_feature_session(
            ack.witness_bundle_observed_at, policy=policy
        )
        if invocation == 0:
            assert (
                started
                <= ack.recording_call_started_at
                <= ack.witness_bundle_observed_at
                <= terminal
            )
        results.append(result)
