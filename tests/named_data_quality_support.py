"""Actual-candidate synthetic fixtures and an explicitly associated parent.

These helpers never acquire/release a lease, discover a transaction, or authorize
research. The coordinator must explicitly set both documented environment inputs:
``AITS_NAMED_DQ_PUBLICATION_TRANSACTION`` and ``AITS_NAMED_DQ_SOURCE_LEASE_ID``.
They are association inputs, not authority: existing live fence/guard replay is
required immediately before dispatch and after the real child terminates.

The actual execution checkout is never copied or modified. Market-like rows and
their immutable publication exist only under pytest's synthetic ``tmp_path``.
Parent proofs are retained under the transaction's existing validation resource;
they do not grant consumer, research, provider, or trading permission.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import shutil
import subprocess
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any
from uuid import uuid4

import pytest

from ai_trading_system.contracts.data_quality_execution import DataQualityDateWindow
from ai_trading_system.contracts.named_data_quality_execution import (
    EQUAL_RISK_PRICE_SOURCE_MANIFEST_PATH,
    FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_PATH,
    NamedArtifactBinding,
    NamedDQExecutionReceipt,
    NamedDQExecutionRequest,
    NamedDQRoots,
    NamedDQScope,
    NamedDQSuccessfulDispatchBinding,
    NamedSnapshotSelector,
)
from ai_trading_system.data.download_publication import (
    DownloadArtifactCandidate,
    DownloadSourceBinding,
    ValidatedDownloadPublication,
    publish_download_transaction,
)
from ai_trading_system.data.immutable_publish import read_contained_artifact_bytes
from ai_trading_system.platform.architecture.integration_publication_fence import (
    IntegrationPublicationFence,
)

ROOT = Path(__file__).resolve().parents[1]
TRANSACTION_ENV = "AITS_NAMED_DQ_PUBLICATION_TRANSACTION"
LEASE_ENV = "AITS_NAMED_DQ_SOURCE_LEASE_ID"
SOURCE_MANIFEST_PATH = "config/data_governance/named_data_quality_execution_sources_v1.json"
BOOTSTRAP_PATH = "scripts/run_named_data_quality.py"
PUBLICATION_POLICY_PATH = "config/architecture/arch_005_integration_publication_fence.yaml"
AS_OF = date(2026, 9, 3)
START = date(2026, 9, 2)
PRICE_BYTES = (
    b"date,ticker,open,high,low,close,adj_close,volume\n"
    b"2026-09-02,QQQ,100,102,99,100,100,1000\n"
    b"2026-09-03,QQQ,100,102,99,101,101,1100\n"
)
RATE_BYTES = b"date,series,value\n2026-09-02,DGS10,4.2\n2026-09-03,DGS10,4.2\n"

# One fixed synthetic probe, not a production CLI or arbitrary-code dispatch API.
# It uses the real bootstrap class and exact Git-compiled verifier. No test seal,
# context factory, module patch, source copy, or identity substitution is used.
_SEALED_INPUTS_TEST_PROBE = r"""
import argparse
import hashlib
import importlib
import json
import os
import pickle
import runpy
import sys
from dataclasses import replace
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("bootstrap", type=Path)
parser.add_argument("--request", required=True, type=Path)
parser.add_argument("--request-sha256", required=True)
parser.add_argument("--source-lease-id", required=True)
parser.add_argument("--operation", choices=("verify",), required=True)
parser.add_argument("--receipt-path", required=True)
parser.add_argument("--receipt-sha256", required=True)
parser.add_argument("--run-dispatch-path", required=True)
parser.add_argument("--run-dispatch-sha256", required=True)
args = parser.parse_args()
session = None
try:
    if not sys.flags.isolated:
        raise ValueError("TEST_PROBE_ISOLATED_CHILD_REQUIRED")
    entry = runpy.run_path(str(args.bootstrap))
    raw = entry["_initial_file_bytes"](args.request.parent, args.request.name)
    if hashlib.sha256(raw).hexdigest() != args.request_sha256:
        raise ValueError("TEST_PROBE_REQUEST_SHA_MISMATCH")
    request = entry["_json_object"](raw)
    session = entry["NamedBootstrapSession"](
        request, operation="verify", source_lease_id=args.source_lease_id
    )
    session.load()
    contracts = importlib.import_module(
        "ai_trading_system.contracts.named_data_quality_execution"
    )
    typed = contracts.NamedDQExecutionRequest.from_dict(request)
    worker = importlib.import_module(entry["WORKER_MODULE"])
    verified = worker.verify_named_data_quality_execution_receipt(
        typed,
        receipt_path=args.receipt_path,
        receipt_sha256=args.receipt_sha256,
        run_dispatch_path=args.run_dispatch_path,
        run_dispatch_sha256=args.run_dispatch_sha256,
        bootstrap=session,
    )
    verified.assert_scope_covered(typed.scope)
    digests = {
        role: hashlib.sha256(verified.bytes_for(role, required_scope=typed.scope)).hexdigest()
        for role in typed.scope.input_roles
    }
    checks = {
        "physical_module_origins": all(
            Path(sys.modules[item.name].__file__)
            == session.root / item.artifact.relative_path
            and sys.modules[item.name].__spec__.origin
            == str(session.root / item.artifact.relative_path)
            for item in session.modules.values()
        ),
        "verifier_code_origin": (
            worker.verify_named_data_quality_execution_receipt.__code__.co_filename
            == str(session.root / "src/ai_trading_system/data/named_quality_execution.py")
        ),
    }
    try:
        pickle.dumps(verified)
    except TypeError as exc:
        checks["pickle_rejected"] = "cannot be serialized" in str(exc)
    else:
        checks["pickle_rejected"] = False
    expanded = replace(
        typed.scope,
        expected_price_tickers=typed.scope.expected_price_tickers + ("SYNTHETIC_OUT_OF_SCOPE",),
    )
    try:
        verified.bytes_for("prices", required_scope=expanded)
    except contracts.NamedDataQualityExecutionContractError as exc:
        checks["expanded_scope_rejected"] = "not covered" in str(exc)
    else:
        checks["expanded_scope_rejected"] = False
    result = {
        "schema_version": "named_data_quality_bootstrap_result.v1",
        "profile": "TEST_PROBE",
        "status": "PASS",
        "request_id": typed.request_id,
        "process_id": os.getpid(),
        "source_lease_id": session.source_lease_id,
        "receipt_id": verified.receipt.receipt_id,
        "original_dq_pid": verified.receipt.execution_observation.execution_pid,
        "verifier_pid": verified.verifier_pid,
        "input_sha256": digests,
        "actual_compiled_module_count": len(session.loader.loaded),
        "execution_identity_sha256": session.context.identity.stable_identity_sha256,
        "context_provenance_kind": session.context.provenance_kind,
        "canonical_dq_call_count": session.canonical_dq_call_count,
        "verified_input_seal_exported": False,
        "dispatch_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }
    if session.canonical_dq_call_count != 0:
        raise ValueError("TEST_PROBE_DQ_DISPATCH_FORBIDDEN")
    session.assert_execution_unchanged(stage="TERMINAL")
    result["child_started_at"] = session.started_at
    result["child_terminal_checked_at"] = session.terminal_checked_at
    # Check after normal context close; the retained object must not become a
    # parent-consumable capability merely because this child already verified it.
    session.close()
    try:
        verified.bytes_for("prices", required_scope=typed.scope)
    except session.context_module.NamedExecutionContextError as exc:
        checks["closed_context_accessor_rejected"] = exc.code == "NAMED_CONTEXT_REQUIRED"
    else:
        checks["closed_context_accessor_rejected"] = False
    if not all(checks.values()):
        raise ValueError("TEST_PROBE_SEAL_BOUNDARY_FAILED: " + repr(checks))
    result["probe_checks"] = checks
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, allow_nan=False))
except (ValueError, OSError, ImportError, RuntimeError, SyntaxError, TypeError) as exc:
    print(json.dumps({
        "schema_version": "named_data_quality_bootstrap_result.v1",
        "profile": "TEST_PROBE",
        "status": "BLOCKED",
        "reason_code": getattr(exc, "code", "TEST_PROBE_FAILED"),
        "detail": str(exc),
        "canonical_dq_call_count": 0 if session is None else session.canonical_dq_call_count,
        "verified_input_seal_exported": False,
        "dispatch_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }, ensure_ascii=False, sort_keys=True))
    raise SystemExit(2)
finally:
    if session is not None:
        session.close()
"""


# Separate finite contract probe: no five-candidate calculation or production CLI.
# Like the original probe, it uses only the actual Git bootstrap/verifier seal.
_EQUAL_RISK_PRICE_TEST_PROBE = r"""
import argparse
import hashlib
import importlib
import json
import os
import pickle
import runpy
import sys
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("bootstrap", type=Path)
parser.add_argument("--request", required=True, type=Path)
parser.add_argument("--request-sha256", required=True)
parser.add_argument("--source-lease-id", required=True)
parser.add_argument("--operation", choices=("verify",), required=True)
parser.add_argument("--receipt-path", required=True)
parser.add_argument("--receipt-sha256", required=True)
parser.add_argument("--run-dispatch-path", required=True)
parser.add_argument("--run-dispatch-sha256", required=True)
args = parser.parse_args()
session = None
try:
    if not sys.flags.isolated:
        raise ValueError("PRICE_PROBE_ISOLATED_CHILD_REQUIRED")
    entry = runpy.run_path(str(args.bootstrap))
    raw = entry["_initial_file_bytes"](args.request.parent, args.request.name)
    if hashlib.sha256(raw).hexdigest() != args.request_sha256:
        raise ValueError("PRICE_PROBE_REQUEST_SHA_MISMATCH")
    request = entry["_json_object"](raw)
    session = entry["NamedBootstrapSession"](
        request, operation="verify", source_lease_id=args.source_lease_id
    )
    session.load()
    contracts = importlib.import_module(
        "ai_trading_system.contracts.named_data_quality_execution"
    )
    window_type = importlib.import_module(
        "ai_trading_system.contracts.data_quality_execution"
    ).DataQualityDateWindow
    typed = contracts.NamedDQExecutionRequest.from_dict(request)
    worker = importlib.import_module(entry["WORKER_MODULE"])
    verified = worker.verify_named_data_quality_execution_receipt(
        typed, receipt_path=args.receipt_path, receipt_sha256=args.receipt_sha256,
        run_dispatch_path=args.run_dispatch_path,
        run_dispatch_sha256=args.run_dispatch_sha256, bootstrap=session,
    )
    receipt_before = verified.receipt.canonical_bytes
    # An absent registry remains an untrusted DTO, never a forged dependency.
    registry = next(
        (item for item in verified.receipt.execution_dependencies
         if item.relative_path == contracts.EQUAL_RISK_PRICE_REGISTRY_PATH),
        contracts.NamedArtifactBinding(
            "EXECUTION", contracts.EQUAL_RISK_PRICE_REGISTRY_PATH, "0" * 64, 1
        ),
    )
    scope = contracts.NamedEqualRiskPriceScope(
        as_of=typed.scope.as_of,
        requested_window=window_type(contracts.EQUAL_RISK_PRIMARY_START, typed.scope.as_of),
        registry_binding=registry,
    )
    prices = verified.prices_for_equal_risk_preview(required_scope=scope)
    checks = {
        "prices_exact_member": hashlib.sha256(prices).hexdigest() == next(
            item.member.sha256 for item in verified.receipt.inputs if item.role == "prices"
        ),
        "receipt_unchanged": verified.receipt.canonical_bytes == receipt_before,
        "physical_module_origins": all(
            Path(sys.modules[item.name].__file__) == session.root / item.artifact.relative_path
            and sys.modules[item.name].__spec__.origin
            == str(session.root / item.artifact.relative_path)
            for item in session.modules.values()
        ),
    }
    legacy_covered = verified.receipt.evaluated_window.contains(scope.requested_window)
    try:
        verified.bytes_for("prices", required_scope=typed.scope)
    except contracts.NamedDataQualityExecutionContractError as exc:
        checks["legacy_scope_unchanged"] = (
            not legacy_covered
            and "required consumption scope is not covered by canonical DQ" in str(exc)
        )
    else:
        checks["legacy_scope_unchanged"] = legacy_covered
    changed_registry = replace(
        registry, sha256=("0" * 64 if registry.sha256 != "0" * 64 else "1" * 64)
    )
    later = scope.as_of + timedelta(days=1)
    invalid_scopes = {
        "registry_sha_mismatch": replace(scope, registry_binding=changed_registry),
        "registry_size_mismatch": replace(
            scope, registry_binding=replace(registry, size_bytes=registry.size_bytes + 1)
        ),
        "asof_mismatch": replace(
            scope, as_of=later, requested_window=window_type(scope.requested_window.start, later)
        ),
    }
    for label, invalid in invalid_scopes.items():
        try:
            verified.prices_for_equal_risk_preview(required_scope=invalid)
        except contracts.NamedDataQualityExecutionContractError:
            checks[label + "_rejected"] = True
        else:
            checks[label + "_rejected"] = False
    try:
        verified.prices_for_equal_risk_preview("rates", required_scope=scope)
    except TypeError:
        checks["rates_role_rejected"] = True
    else:
        checks["rates_role_rejected"] = False
    try:
        pickle.dumps(verified)
    except TypeError:
        checks["pickle_rejected"] = True
    else:
        checks["pickle_rejected"] = False
    result = {
        "schema_version": "named_data_quality_bootstrap_result.v1",
        "profile": "EQUAL_RISK_PRICE_TEST_PROBE", "status": "PASS",
        "request_id": typed.request_id, "process_id": os.getpid(),
        "source_lease_id": session.source_lease_id,
        "receipt_id": verified.receipt.receipt_id,
        "original_dq_pid": verified.receipt.execution_observation.execution_pid,
        "verifier_pid": verified.verifier_pid,
        "input_sha256": {"prices": hashlib.sha256(prices).hexdigest()},
        "price_scope": scope.to_dict(),
        "original_evaluated_window": verified.receipt.evaluated_window.to_dict(),
        "actual_compiled_module_count": len(session.loader.loaded),
        "execution_identity_sha256": session.context.identity.stable_identity_sha256,
        "context_provenance_kind": session.context.provenance_kind,
        "canonical_dq_call_count": session.canonical_dq_call_count,
        "verified_input_seal_exported": False, "dispatch_allowed": False,
        "strategy_semantics_validated": False, "feature_readiness_claimed": False,
        "production_effect": "none", "broker_action": "none",
    }
    if session.canonical_dq_call_count != 0:
        raise ValueError("PRICE_PROBE_DQ_DISPATCH_FORBIDDEN")
    session.assert_execution_unchanged(stage="TERMINAL")
    result["child_started_at"] = session.started_at
    result["child_terminal_checked_at"] = session.terminal_checked_at
    session.close()
    try:
        verified.prices_for_equal_risk_preview(required_scope=scope)
    except session.context_module.NamedExecutionContextError as exc:
        checks["closed_context_accessor_rejected"] = exc.code == "NAMED_CONTEXT_REQUIRED"
    else:
        checks["closed_context_accessor_rejected"] = False
    if not all(checks.values()):
        raise ValueError("PRICE_PROBE_BOUNDARY_FAILED: " + repr(checks))
    result["probe_checks"] = checks
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, allow_nan=False))
except (ValueError, OSError, ImportError, RuntimeError, SyntaxError, TypeError) as exc:
    print(json.dumps({
        "schema_version": "named_data_quality_bootstrap_result.v1",
        "profile": "EQUAL_RISK_PRICE_TEST_PROBE", "status": "BLOCKED",
        "reason_code": getattr(exc, "code", "PRICE_PROBE_FAILED"), "detail": str(exc),
        "canonical_dq_call_count": 0 if session is None else session.canonical_dq_call_count,
        "verified_input_seal_exported": False, "dispatch_allowed": False,
        "production_effect": "none", "broker_action": "none",
    }, ensure_ascii=False, sort_keys=True))
    raise SystemExit(2)
finally:
    if session is not None:
        session.close()
"""


_FIVE_CANDIDATE_PREVIEW_TEST_PROBE = r"""
import argparse
import builtins
import hashlib
import importlib
import io
import json
import os
import pickle
import runpy
import socket
import sys
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

parser = argparse.ArgumentParser()
parser.add_argument("bootstrap", type=Path)
parser.add_argument("--request", required=True, type=Path)
parser.add_argument("--request-sha256", required=True)
parser.add_argument("--source-lease-id", required=True)
parser.add_argument("--operation", choices=("verify",), required=True)
parser.add_argument("--receipt-path", required=True)
parser.add_argument("--receipt-sha256", required=True)
parser.add_argument("--run-dispatch-path", required=True)
parser.add_argument("--run-dispatch-sha256", required=True)
args = parser.parse_args()
session = None
def forbidden(*args, **kwargs):
    raise AssertionError("PREVIEW_PROBE_FORBIDDEN_IO")
try:
    if not sys.flags.isolated:
        raise ValueError("PREVIEW_PROBE_ISOLATED_CHILD_REQUIRED")
    entry = runpy.run_path(str(args.bootstrap))
    raw = entry["_initial_file_bytes"](args.request.parent, args.request.name)
    if hashlib.sha256(raw).hexdigest() != args.request_sha256:
        raise ValueError("PREVIEW_PROBE_REQUEST_SHA_MISMATCH")
    request = entry["_json_object"](raw)
    session = entry["NamedBootstrapSession"](
        request, operation="verify", source_lease_id=args.source_lease_id
    )
    session.load()
    contracts = importlib.import_module("ai_trading_system.contracts.named_data_quality_execution")
    window_type = importlib.import_module(
        "ai_trading_system.contracts.data_quality_execution"
    ).DataQualityDateWindow
    typed = contracts.NamedDQExecutionRequest.from_dict(request)
    worker = importlib.import_module(entry["WORKER_MODULE"])
    verified = worker.verify_named_data_quality_execution_receipt(
        typed, receipt_path=args.receipt_path, receipt_sha256=args.receipt_sha256,
        run_dispatch_path=args.run_dispatch_path,
        run_dispatch_sha256=args.run_dispatch_sha256, bootstrap=session,
    )
    receipt_before = verified.receipt.canonical_bytes
    registry = next((item for item in verified.receipt.execution_dependencies
                     if item.relative_path == contracts.EQUAL_RISK_PRICE_REGISTRY_PATH),
                    contracts.NamedArtifactBinding(
                        "EXECUTION", contracts.EQUAL_RISK_PRICE_REGISTRY_PATH, "0" * 64, 1))
    scope = contracts.NamedEqualRiskPriceScope(
        as_of=typed.scope.as_of,
        requested_window=window_type(contracts.EQUAL_RISK_PRIMARY_START, typed.scope.as_of),
        registry_binding=registry,
    )
    # Old closures must fail at the accessor before attempting to import the new consumer.
    prices, registry_bytes, sessions, next_session = (
        verified.inputs_for_five_candidate_preview(required_scope=scope)
    )
    consumer = importlib.import_module("ai_trading_system.simple_baseline_named_preview")
    calendar = importlib.import_module("ai_trading_system.trading_calendar")
    calendar_policy = importlib.import_module("ai_trading_system.us_equity_special_closure_policy")
    checks = {}
    # Clear the actual policy cache after verification; no future date getter may run.
    calendar_policy.default_us_equity_special_closure_policy.cache_clear()
    with patch.object(builtins, "open", forbidden), patch.object(io, "open", forbidden), \
         patch.object(socket, "socket", forbidden), \
         patch.object(calendar, "is_us_equity_trading_day", forbidden):
        preview = consumer.build_named_simple_baseline_preview(verified, required_scope=scope)
        payload = preview.to_dict()
        checks["repeat_is_identical_without_io"] = (
            consumer.build_named_simple_baseline_preview(verified, required_scope=scope)
            .canonical_bytes == preview.canonical_bytes
        )
        for label, bad_scope in {
            "registry_sha": replace(scope, registry_binding=replace(registry, sha256="0" * 64)),
            "registry_size": replace(scope, registry_binding=replace(
                registry, size_bytes=registry.size_bytes + 1)),
        }.items():
            try:
                consumer.build_named_simple_baseline_preview(verified, required_scope=bad_scope)
            except contracts.NamedDataQualityExecutionContractError:
                checks[label + "_rejected"] = True
            else:
                checks[label + "_rejected"] = False
        try:
            verified.prices_for_equal_risk_preview(required_scope=scope)
        except contracts.NamedDataQualityExecutionContractError:
            checks["old_accessor_rejects_new_manifest"] = True
        else:
            checks["old_accessor_rejects_new_manifest"] = False
        try:
            pickle.dumps(verified)
        except TypeError:
            checks["pickle_rejected"] = True
        else:
            checks["pickle_rejected"] = False
    checks["receipt_unchanged"] = verified.receipt.canonical_bytes == receipt_before
    checks["captured_registry_exact"] = (
        hashlib.sha256(registry_bytes).hexdigest() == registry.sha256
    )
    checks["compiled_complete_closure"] = len(session.loader.loaded) == 59
    checks["consumer_dq_zero"] = session.canonical_dq_call_count == 0
    result = {
        "schema_version": "named_data_quality_bootstrap_result.v1",
        "profile": "FIVE_CANDIDATE_PREVIEW_TEST_PROBE", "status": "PASS",
        "request_id": typed.request_id, "process_id": os.getpid(),
        "source_lease_id": session.source_lease_id,
        "receipt_id": verified.receipt.receipt_id,
        "original_dq_pid": verified.receipt.execution_observation.execution_pid,
        "verifier_pid": verified.verifier_pid,
        "actual_compiled_module_count": len(session.loader.loaded),
        "execution_identity_sha256": session.context.identity.stable_identity_sha256,
        "canonical_dq_call_count": session.canonical_dq_call_count,
        "preview": payload,
        "verified_input_seal_exported": False, "dispatch_allowed": False,
        "production_effect": "none", "broker_action": "none",
    }
    session.assert_execution_unchanged(stage="TERMINAL")
    result["child_started_at"] = session.started_at
    result["child_terminal_checked_at"] = session.terminal_checked_at
    session.close()
    try:
        consumer.build_named_simple_baseline_preview(verified, required_scope=scope)
    except session.context_module.NamedExecutionContextError as exc:
        checks["closed_context_rejected"] = exc.code == "NAMED_CONTEXT_REQUIRED"
    else:
        checks["closed_context_rejected"] = False
    if not all(checks.values()):
        raise ValueError("PREVIEW_PROBE_BOUNDARY_FAILED: " + repr(checks))
    result["probe_checks"] = checks
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, allow_nan=False))
except (ValueError, OSError, ImportError, RuntimeError, SyntaxError, TypeError) as exc:
    print(json.dumps({
        "schema_version": "named_data_quality_bootstrap_result.v1",
        "profile": "FIVE_CANDIDATE_PREVIEW_TEST_PROBE", "status": "BLOCKED",
        "reason_code": getattr(exc, "code", "PREVIEW_PROBE_FAILED"), "detail": str(exc),
        "canonical_dq_call_count": 0 if session is None else session.canonical_dq_call_count,
        "verified_input_seal_exported": False, "dispatch_allowed": False,
        "production_effect": "none", "broker_action": "none",
    }, ensure_ascii=False, sort_keys=True))
    raise SystemExit(2)
finally:
    if session is not None:
        session.close()
"""


def _sha(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _json_bytes(payload: object) -> bytes:
    return (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n"
    ).encode("utf-8")


def _instant() -> str:
    return datetime.now(UTC).isoformat()


def _git(root: Path, *arguments: str) -> str:
    environment = {
        key: value for key, value in os.environ.items() if not key.upper().startswith("GIT_")
    }
    environment.update(
        GIT_CONFIG_NOSYSTEM="1",
        GIT_CONFIG_GLOBAL=os.devnull,
        GIT_CONFIG_SYSTEM=os.devnull,
        GIT_TERMINAL_PROMPT="0",
        GIT_NO_REPLACE_OBJECTS="1",
        GIT_NO_LAZY_FETCH="1",
        GIT_OPTIONAL_LOCKS="0",
    )
    result = subprocess.run(
        [
            "git",
            "--no-replace-objects",
            "--no-lazy-fetch",
            "-c",
            f"safe.directory={root.as_posix()}",
            "-c",
            "core.fsmonitor=false",
            "-c",
            "protocol.allow=never",
            "-C",
            str(root),
            *arguments,
        ],
        env=environment,
        capture_output=True,
        check=False,
    )
    if result.returncode:
        pytest.fail("NAMED_PARENT_LOCAL_GIT_FAILED: " + " ".join(arguments))
    return result.stdout.decode("utf-8").strip()


def _write_new(path: Path, content: bytes) -> dict[str, object]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(content)
    return {"path": path.as_posix(), "sha256": _sha(content), "size_bytes": len(content)}


@dataclass(frozen=True)
class NamedExecutionFixture:
    request: NamedDQExecutionRequest
    source_root: Path
    publication_root: Path
    evidence_root: Path
    publication: ValidatedDownloadPublication
    input_bytes: dict[str, bytes]


@dataclass(frozen=True)
class ParentDispatchResult:
    child_result: dict[str, Any]
    returncode: int | None
    parent_receipt_path: Path
    parent_receipt: dict[str, Any]
    run_dispatch_path: str | None = None
    run_dispatch_sha256: str | None = None


class NamedChildPythonLaunchError(ValueError):
    """An unsupported interpreter must fail before any child is dispatched."""


@dataclass(frozen=True)
class _ChildPythonLaunch:
    executable: str
    environment: dict[str, str]
    audit: dict[str, Any]


def _child_python_launch(
    *,
    platform_name: str,
    implementation: str,
    logical_executable: str,
    base_executable: str | None,
    prefix: str,
    base_prefix: str,
    environment: Mapping[str, str],
) -> _ChildPythonLaunch:
    """Pure launch selection, not a guard, executable inspection or authority.

    The Windows venv mapping is CPython's own spawn pattern, not a PID fallback:
    https://github.com/python/cpython/blob/v3.11.9/Lib/multiprocessing/popen_spawn_win32.py#L55-L62
    getpath consumes __PYVENV_LAUNCHER__ even under -I; PYTHONEXECUTABLE would
    take precedence, so both inherited overrides are removed from the child copy.
    Other CPython platforms retain the original direct logical executable.
    """
    if implementation != "cpython" or platform_name not in {"win32", "linux", "darwin"}:
        raise NamedChildPythonLaunchError("NAMED_PARENT_PYTHON_RUNTIME_UNSUPPORTED")
    path_type = PureWindowsPath if platform_name == "win32" else PurePosixPath
    for label, value in (
        ("LOGICAL_EXECUTABLE", logical_executable),
        ("BASE_EXECUTABLE", base_executable),
        ("PREFIX", prefix),
        ("BASE_PREFIX", base_prefix),
    ):
        if (
            not isinstance(value, str)
            or not value
            or "\0" in value
            or not path_type(value).is_absolute()
        ):
            raise NamedChildPythonLaunchError(f"NAMED_PARENT_PYTHON_{label}_INVALID")
    assert base_executable is not None
    in_venv = path_type(prefix) != path_type(base_prefix)
    windows_venv = platform_name == "win32" and in_venv
    if platform_name == "win32":
        if any(
            path_type(value).name.lower() != "python.exe"
            for value in (logical_executable, base_executable)
        ):
            raise NamedChildPythonLaunchError("NAMED_PARENT_PYTHON_WINDOWS_EXECUTABLE_UNSUPPORTED")
        if (path_type(logical_executable) != path_type(base_executable)) != in_venv:
            raise NamedChildPythonLaunchError("NAMED_PARENT_PYTHON_VENV_IDENTITY_INCONSISTENT")
    child_environment = dict(environment)
    removed_keys = sorted(
        key
        for key in child_environment
        if key.upper() in {"PYTHONEXECUTABLE", "__PYVENV_LAUNCHER__"}
    )
    for key in removed_keys:
        del child_environment[key]
    overrides = {"__PYVENV_LAUNCHER__": logical_executable} if windows_venv else {}
    child_environment.update(overrides)
    executable = base_executable if windows_venv else logical_executable
    return _ChildPythonLaunch(
        executable=executable,
        environment=child_environment,
        audit={
            "schema_version": "named_data_quality_python_launch.v1",
            "semantics": "PARENT_INTERPRETER_LAUNCH_SELECTION_ONLY",
            "profile": "CPYTHON_WINDOWS_DIRECT_VENV" if windows_venv else "CPYTHON_DIRECT",
            "implementation": implementation,
            "platform": platform_name,
            "logical_executable": logical_executable,
            "os_executable": executable,
            "base_executable": base_executable,
            "logical_prefix": prefix,
            "base_prefix": base_prefix,
            "venv_detected": in_venv,
            "windows_redirector_bypassed": windows_venv,
            "child_environment_removed_keys": removed_keys,
            "child_environment_overrides": overrides,
            "parent_environment_mutated": False,
            "dispatch_authority_granted": False,
        },
    )


def _current_child_python_launch() -> _ChildPythonLaunch:
    """Check the selected current runtime files without importing or launching code."""
    launch = _child_python_launch(
        platform_name=sys.platform,
        implementation=sys.implementation.name,
        logical_executable=sys.executable,
        base_executable=getattr(sys, "_base_executable", None),
        prefix=sys.prefix,
        base_prefix=sys.base_prefix,
        environment=os.environ,
    )
    for label, executable in (
        ("LOGICAL_EXECUTABLE", launch.audit["logical_executable"]),
        ("BASE_EXECUTABLE", launch.audit["base_executable"]),
    ):
        try:
            valid = Path(executable).is_file() and os.access(executable, os.X_OK)
        except (OSError, ValueError):
            valid = False
        if not valid:
            raise NamedChildPythonLaunchError(f"NAMED_PARENT_PYTHON_{label}_NOT_EXECUTABLE")
    return launch


def build_actual_candidate_fixture(
    tmp_path: Path,
    *,
    execution_root: Path = ROOT,
    prices_content: bytes | None = None,
    rates_content: bytes | None = None,
    requested_start: date = START,
    requested_end: date = AS_OF,
    as_of: date = AS_OF,
    expected_price_tickers: tuple[str, ...] = ("QQQ",),
    expected_rate_series: tuple[str, ...] = ("DGS10",),
    equal_risk_price_profile: bool = False,
    five_candidate_preview_profile: bool = False,
    expected_evaluated_window: DataQualityDateWindow | None = None,
) -> NamedExecutionFixture:
    """Publish synthetic bytes, then relocate only that tmp-path publication.

    The publisher's finite source-kind enum has no TEST kind. LIVE_PROVIDER is
    therefore a *fixture schema value*, with an explicit SYNTHETIC_FIXTURE name
    and memory-only endpoint; it does not represent an actual provider request.
    Original manifest output paths are preserved when copying the synthetic
    publication. No source, policy, or real market cache is copied from ROOT.
    """
    if (
        type(equal_risk_price_profile) is not bool
        or type(five_candidate_preview_profile) is not bool
        or (equal_risk_price_profile and five_candidate_preview_profile)
    ):
        pytest.fail("NAMED_PARENT_PRICE_PROFILE_MUST_BE_BOOL")
    source_manifest_path = (
        FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_PATH
        if five_candidate_preview_profile
        else (
            EQUAL_RISK_PRICE_SOURCE_MANIFEST_PATH
            if equal_risk_price_profile
            else SOURCE_MANIFEST_PATH
        )
    )
    execution_root = execution_root.resolve()
    if execution_root != ROOT:
        pytest.fail("NAMED_PARENT_ACTUAL_CHECKOUT_REQUIRED: no copied execution checkout")
    if Path(_git(execution_root, "rev-parse", "--show-toplevel")).resolve() != execution_root:
        pytest.fail("NAMED_PARENT_GIT_ROOT_MISMATCH")
    candidate = _git(execution_root, "rev-parse", "HEAD")
    source_root = tmp_path / "synthetic-source"
    source_output = source_root / "data/raw"
    publication_root = tmp_path / "synthetic-publication"
    evidence_root = tmp_path / "synthetic-evidence"
    evidence_root.mkdir(parents=True)
    inputs = {
        "prices": PRICE_BYTES if prices_content is None else prices_content,
        "rates": RATE_BYTES if rates_content is None else rates_content,
    }
    artifacts: list[DownloadArtifactCandidate] = []
    bindings: list[DownloadSourceBinding] = []
    for role, content in inputs.items():
        records = list(csv.DictReader(io.StringIO(content.decode("utf-8"))))
        dimension = "ticker" if role == "prices" else "series"
        event_id = f"synthetic-fixture:{role}"
        artifacts.append(
            DownloadArtifactCandidate(
                role=role,
                filename=f"{role}_daily.csv",
                content=content,
                row_count=len(records),
                source_event_ids=(event_id,),
            )
        )
        bindings.append(
            DownloadSourceBinding(
                source_event_id=event_id,
                artifact_role=role,
                source_kind="LIVE_PROVIDER",
                source_id=f"synthetic-fixture-{role}",
                provider="SYNTHETIC_FIXTURE",
                endpoint=f"memory:synthetic-fixture-{role}",
                request_parameters={"synthetic_only": True, "network_request_count": 0},
                winning_row_count=len(records),
                allocation_mode="REMAINDER",
                # Binding order is canonical (dimension, date); CSV bytes and
                # ordinals stay untouched. Do not deduplicate invalid fixtures.
                winning_row_keys=tuple(sorted((row[dimension], row["date"]) for row in records)),
            )
        )
    publication = publish_download_transaction(
        output_dir=source_output,
        requested_start=requested_start,
        requested_end=requested_end,
        published_at=datetime.combine(requested_end, time(21), tzinfo=UTC),
        artifacts=tuple(artifacts),
        source_bindings=tuple(bindings),
    )
    pointer = json.loads(publication.discovery_pointer_path.read_bytes())
    # Both ends are newly created fixture directories. This is not real cache
    # migration and does not write to, copy, or reinterpret the execution root.
    shutil.copytree(source_output, publication_root)
    manifest = (execution_root / source_manifest_path).read_bytes()
    window = DataQualityDateWindow(requested_start, requested_end)
    request = NamedDQExecutionRequest(
        roots=NamedDQRoots(
            source_root=source_root.as_posix(),
            publication_root=publication_root.as_posix(),
            execution_root=execution_root.as_posix(),
            evidence_root=evidence_root.as_posix(),
        ),
        selector=NamedSnapshotSelector(
            pointer_id=pointer["pointer_id"],
            pointer_sha256=publication.discovery_pointer_sha256,
            transaction_id=publication.transaction_id,
            transaction_sha256=publication.transaction_manifest_sha256,
        ),
        scope=NamedDQScope(
            as_of=as_of,
            requested_window=window,
            expected_price_tickers=expected_price_tickers,
            expected_rate_series=expected_rate_series,
            input_roles=("prices", "rates"),
            require_secondary_prices=False,
        ),
        source_output_relative_path="data/raw",
        policy_path="config/data_quality.yaml",
        execution_profile_id="manual.v1",
        candidate_commit=candidate,
        source_manifest_path=source_manifest_path,
        source_manifest_sha256=_sha(manifest),
        expected_evaluated_window=expected_evaluated_window or window,
    )
    return NamedExecutionFixture(
        request, source_root, publication_root, evidence_root, publication, inputs
    )


def _parent_environment() -> tuple[str, str]:
    transaction, lease = os.environ.get(TRANSACTION_ENV), os.environ.get(LEASE_ENV)
    if not transaction or not lease or transaction != transaction.strip() or lease != lease.strip():
        pytest.fail(
            "NAMED_PARENT_ASSOCIATION_REQUIRED: coordinator must explicitly supply "
            f"{TRANSACTION_ENV} and {LEASE_ENV}; no search, default, or skip"
        )
    return transaction, lease


def _live_parent_proof(
    *,
    request: NamedDQExecutionRequest,
    transaction_input: str,
    lease_id: str,
) -> dict[str, Any]:
    root = Path(request.roots.execution_root)
    raw_path = Path(transaction_input)
    transaction = raw_path if raw_path.is_absolute() else root / raw_path
    if ".." in raw_path.parts or not transaction.resolve().is_relative_to(root):
        raise ValueError("NAMED_PARENT_TRANSACTION_OUTSIDE_EXECUTION_ROOT")
    fence = IntegrationPublicationFence(
        project_root=root,
        policy_path=root / PUBLICATION_POLICY_PATH,
    )
    validated = fence.validate(transaction, require_candidate=True)
    if validated["lease_id"] != lease_id:
        raise ValueError("NAMED_PARENT_LEASE_ASSOCIATION_MISMATCH")
    if validated["phase"] not in {
        "FORMAL_VALIDATION_PRE",
        "FULL_DISPATCHED",
    }:
        raise ValueError("NAMED_PARENT_FORMAL_PHASE_REQUIRED")
    audit = fence.guard.audit_worktree()
    if audit.dirty_paths:
        raise ValueError("NAMED_PARENT_CANDIDATE_DIRTY")
    if (
        Path(audit.audited_identity.checkout_root) != root
        or audit.audited_identity.head_commit != request.candidate_commit
    ):
        raise ValueError("NAMED_PARENT_CANDIDATE_IDENTITY_MISMATCH")
    replay = fence.guard.replay()
    if replay.status != "PASS":
        raise ValueError("NAMED_PARENT_LEASE_REPLAY_INVALID")
    lease = next((item for item in replay.lease_heads if item.lease_id == lease_id), None)
    checked_at = datetime.now(UTC)
    if (
        lease is None
        or lease.state != "ACTIVE"
        or lease.expires_at is None
        or datetime.fromisoformat(lease.expires_at) <= checked_at
    ):
        raise ValueError("NAMED_PARENT_ACTIVE_UNEXPIRED_LEASE_REQUIRED")
    event_id = dict(replay.head_event_ids)[lease_id]
    event_path = fence.guard.store.events_root / lease_id / f"{event_id}.json"
    event_bytes = event_path.read_bytes()
    transaction_bytes = transaction.read_bytes()
    transaction_payload = json.loads(transaction_bytes)
    if transaction_payload["transaction_sha256"] != validated["transaction_sha256"]:
        raise ValueError("NAMED_PARENT_TRANSACTION_CAPTURE_DRIFT")
    publication_replay = fence.replay(transaction)
    if (
        publication_replay.status != "PASS"
        or publication_replay.phase != validated["phase"]
        or publication_replay.candidate_sha != validated["candidate_sha"]
    ):
        raise ValueError("NAMED_PARENT_PUBLICATION_REPLAY_CAPTURE_DRIFT")
    publication_head = publication_replay.events[-1]
    publication_event_path = (
        transaction.parent
        / "events"
        / (
            f"{publication_head['sequence']:04d}_{publication_head['phase'].lower()}_"
            f"{publication_head['event_id'][:12]}.json"
        )
    )
    publication_event_bytes = publication_event_path.read_bytes()
    audit_payload, lease_payload = audit.to_dict(), replay.to_dict()
    return {
        "schema_version": "named_dq_existing_parent_proof.v1",
        "status": "PASS",
        "checked_at": checked_at.isoformat(),
        "candidate_binding_role": "FORMAL_CANDIDATE",
        "environment_inputs": {TRANSACTION_ENV: transaction_input, LEASE_ENV: lease_id},
        "request_sha256": request.canonical_sha256,
        "execution_root": root.as_posix(),
        "candidate_commit": request.candidate_commit,
        "transaction": {
            "path": transaction.as_posix(),
            "raw_file_sha256": _sha(transaction_bytes),
            "internal_transaction_sha256": validated["transaction_sha256"],
        },
        "fence_binding": validated,
        "fence_binding_sha256": _sha(_json_bytes(validated)),
        "checkout_audit": audit_payload,
        "checkout_audit_sha256": _sha(_json_bytes(audit_payload)),
        "lease_replay": lease_payload,
        "lease_replay_sha256": _sha(_json_bytes(lease_payload)),
        "publication_replay": publication_replay.to_dict(),
        "publication_head_event": {
            "path": publication_event_path.as_posix(),
            "event_id": publication_head["event_id"],
            "raw_file_sha256": _sha(publication_event_bytes),
        },
        "active_lease": lease.to_dict(),
        "lease_head_event": {
            "path": event_path.as_posix(),
            "event_id": event_id,
            "raw_file_sha256": _sha(event_bytes),
        },
        "dispatch_authority_granted_by_environment": False,
        "lease_acquired_or_mutated": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _bound_bytes(root: Path, binding: NamedArtifactBinding) -> bytes:
    content = read_contained_artifact_bytes(root=root, relative_path=binding.relative_path)
    if _sha(content) != binding.sha256 or len(content) != binding.size_bytes:
        raise ValueError("NAMED_PARENT_BOUND_ARTIFACT_CHANGED")
    return content


def _parent_artifact_bytes(binding: dict[str, Any]) -> bytes:
    relative = Path(binding["path"]).relative_to(ROOT).as_posix()
    return _bound_bytes(
        ROOT,
        NamedArtifactBinding(
            root_role="EXECUTION",
            relative_path=relative,
            sha256=binding["sha256"],
            size_bytes=binding["size_bytes"],
        ),
    )


def _write_successful_run_proof(
    *,
    request: NamedDQExecutionRequest,
    child: dict[str, Any],
    parent_path: Path,
    parent: dict[str, Any],
    postguard: dict[str, Any],
) -> tuple[str, str]:
    """Only after the immutable parent receipt: no parent -> proof hash cycle."""
    if (
        parent["returncode"] != 0
        or parent["terminal_state"] != "EXITED"
        or parent["operation"] != "run"
        or parent["status"] != "PASS"
        or postguard["status"] != "PASS"
        or parent["failure"] is not None
        or type(child.get("canonical_dq_call_count")) is not int
        or child.get("canonical_dq_call_count") != 1
    ):
        raise ValueError("NAMED_PARENT_SUCCESSFUL_RUN_TERMINAL_REQUIRED")
    receipt_path = child["receipt_path"]
    receipt_bytes = read_contained_artifact_bytes(
        root=Path(request.roots.evidence_root),
        relative_path=receipt_path,
    )
    if _sha(receipt_bytes) != child["receipt_sha256"]:
        raise ValueError("NAMED_PARENT_RUN_RECEIPT_SHA_MISMATCH")
    receipt = NamedDQExecutionReceipt.from_json_bytes(receipt_bytes)
    if (
        receipt.request != request
        or receipt.receipt_id != child["receipt_id"]
        or receipt.execution_observation.execution_pid != parent["child_pid"]
        or receipt.execution_observation.source_lease_id != parent["source_lease_id"]
        or receipt.execution.candidate_commit != request.candidate_commit
        or receipt.execution.execution_root != request.roots.execution_root
        or receipt.report.status != child["status"]
    ):
        raise ValueError("NAMED_PARENT_RUN_RECEIPT_ASSOCIATION_MISMATCH")
    child_started = datetime.fromisoformat(child["child_started_at"])
    child_terminal = datetime.fromisoformat(child["child_terminal_checked_at"])
    if not (
        datetime.fromisoformat(parent["spawn_requested_at"])
        <= child_started
        <= child_terminal
        <= datetime.fromisoformat(parent["terminal_observed_at"])
    ):
        raise ValueError("NAMED_PARENT_CHILD_TERMINAL_ENCLOSURE_MISMATCH")
    parent_bytes = parent_path.read_bytes()
    if parent_bytes != _json_bytes(parent):
        raise ValueError("NAMED_PARENT_RECEIPT_CHANGED_BEFORE_DISPATCH_BINDING")
    proof = NamedDQSuccessfulDispatchBinding(
        receipt=NamedArtifactBinding(
            root_role="EVIDENCE",
            relative_path=receipt_path,
            sha256=_sha(receipt_bytes),
            size_bytes=len(receipt_bytes),
        ),
        receipt_id=receipt.receipt_id,
        request_id=request.request_id,
        request_sha256=request.canonical_sha256,
        execution_identity_sha256=receipt.execution.stable_identity_sha256,
        candidate_commit=request.candidate_commit,
        execution_root=request.roots.execution_root,
        execution_pid=parent["child_pid"],
        source_lease_id=parent["source_lease_id"],
        child_started_at=child_started,
        child_terminal_checked_at=child_terminal,
        parent_postchecked_at=datetime.fromisoformat(postguard["checked_at"]),
        parent_receipt=NamedArtifactBinding(
            root_role="EXECUTION",
            relative_path=parent_path.relative_to(ROOT).as_posix(),
            sha256=_sha(parent_bytes),
            size_bytes=len(parent_bytes),
        ),
    )
    proof.assert_matches_receipt(receipt, receipt_path=receipt_path)
    path = parent_path.parent / "successful_run_dispatch.json"
    _write_new(path, proof.canonical_bytes)
    return path.relative_to(ROOT).as_posix(), proof.canonical_sha256


def _verified_successful_run_arguments(
    *,
    selected: NamedDQExecutionRequest,
    successful_run: ParentDispatchResult | None,
    receipt_path: str | None,
    receipt_sha256: str | None,
) -> tuple[str, str, str, str]:
    """Re-read the original successful terminal proof, not a leftover receipt."""
    if (
        successful_run is None
        or type(successful_run.returncode) is not int
        or successful_run.returncode != 0
        or successful_run.run_dispatch_path is None
        or successful_run.run_dispatch_sha256 is None
    ):
        raise ValueError("NAMED_PARENT_EXPLICIT_SUCCESSFUL_RUN_REQUIRED")
    proof_bytes = read_contained_artifact_bytes(
        root=ROOT,
        relative_path=successful_run.run_dispatch_path,
    )
    if _sha(proof_bytes) != successful_run.run_dispatch_sha256:
        raise ValueError("NAMED_PARENT_RUN_DISPATCH_SHA_MISMATCH")
    proof = NamedDQSuccessfulDispatchBinding.from_json_bytes(proof_bytes)
    parent_bytes = _bound_bytes(ROOT, proof.parent_receipt)
    parent = json.loads(parent_bytes)
    if (
        parent != successful_run.parent_receipt
        or ROOT / proof.parent_receipt.relative_path != successful_run.parent_receipt_path
        or parent["returncode"] != 0
        or parent["terminal_state"] != "EXITED"
        or parent["operation"] != "run"
        or parent["status"] != "PASS"
        or parent["failure"] is not None
        or parent["child_pid"] != proof.execution_pid
        or parent["source_lease_id"] != proof.source_lease_id
        or parent["observed_canonical_dq_call_count"] != 1
    ):
        raise ValueError("NAMED_PARENT_ORIGINAL_RUN_TERMINAL_MISMATCH")
    post = json.loads(_parent_artifact_bytes(parent["post_dispatch_proof"]))
    child = json.loads(_parent_artifact_bytes(parent["child_stdout"]))
    if (
        post["status"] != "PASS"
        or post["active_lease"]["state"] != "ACTIVE"
        or post["active_lease"]["lease_id"] != proof.source_lease_id
        or post["fence_binding"]["status"] != "PASS"
        or post["fence_binding"]["lease_id"] != proof.source_lease_id
        or datetime.fromisoformat(post["checked_at"]) != proof.parent_postchecked_at
        or datetime.fromisoformat(post["active_lease"]["expires_at"]) <= proof.parent_postchecked_at
        or child != successful_run.child_result
        or child != parent["child_result"]
        or child["process_id"] != proof.execution_pid
        or child["source_lease_id"] != proof.source_lease_id
        or datetime.fromisoformat(child["child_started_at"]) != proof.child_started_at
        or datetime.fromisoformat(child["child_terminal_checked_at"])
        != proof.child_terminal_checked_at
    ):
        raise ValueError("NAMED_PARENT_ORIGINAL_POSTGUARD_OR_CHILD_MISMATCH")
    original_bytes = _bound_bytes(Path(selected.roots.evidence_root), proof.receipt)
    original = NamedDQExecutionReceipt.from_json_bytes(original_bytes)
    proof.assert_matches_receipt(original, receipt_path=proof.receipt.relative_path)
    if (
        original.request != selected
        or (receipt_path is not None and receipt_path != proof.receipt.relative_path)
        or (receipt_sha256 is not None and receipt_sha256 != proof.receipt.sha256)
    ):
        raise ValueError("NAMED_PARENT_VERIFY_RECEIPT_SCOPE_MISMATCH")
    return (
        proof.receipt.relative_path,
        proof.receipt.sha256,
        successful_run.run_dispatch_path,
        successful_run.run_dispatch_sha256,
    )


def dispatch_actual_candidate_child(
    fixture: NamedExecutionFixture,
    *,
    operation: str = "run",
    receipt_path: str | None = None,
    receipt_sha256: str | None = None,
    request: NamedDQExecutionRequest | None = None,
    successful_run: ParentDispatchResult | None = None,
    test_probe: bool = False,
    equal_risk_price_probe: bool = False,
    five_candidate_preview_probe: bool = False,
    parent_evidence_root: Path | None = None,
) -> ParentDispatchResult:
    """Dispatch one actual child under an existing, live, explicitly named lease.

    Unknown/partial child counters stay unknown; no successful result is invented
    after timeout or process failure. Pre/post proofs do not prove adversarial
    isolation against another process ignoring the cooperative repository lease.
    """
    selected = fixture.request if request is None else request
    if Path(selected.roots.execution_root) != ROOT:
        pytest.fail("NAMED_PARENT_ACTUAL_CHECKOUT_REQUIRED")
    if operation not in {"run", "verify"}:
        pytest.fail("NAMED_PARENT_OPERATION_INVALID")
    if type(test_probe) is not bool or (test_probe and operation != "verify"):
        pytest.fail("NAMED_PARENT_TEST_PROBE_VERIFY_ONLY")
    if (
        type(equal_risk_price_probe) is not bool
        or (equal_risk_price_probe and operation != "verify")
        or (equal_risk_price_probe and test_probe)
    ):
        pytest.fail("NAMED_PARENT_PRICE_PROBE_EXCLUSIVE_VERIFY_ONLY")
    if type(five_candidate_preview_probe) is not bool or (
        five_candidate_preview_probe
        and (operation != "verify" or test_probe or equal_risk_price_probe)
    ):
        pytest.fail("NAMED_PARENT_PREVIEW_PROBE_EXCLUSIVE_VERIFY_ONLY")
    fixed_probe = (
        _FIVE_CANDIDATE_PREVIEW_TEST_PROBE
        if five_candidate_preview_probe
        else (
            _EQUAL_RISK_PRICE_TEST_PROBE
            if equal_risk_price_probe
            else _SEALED_INPUTS_TEST_PROBE if test_probe else None
        )
    )
    run_dispatch_path = run_dispatch_sha256 = None
    if operation == "verify":
        try:
            receipt_path, receipt_sha256, run_dispatch_path, run_dispatch_sha256 = (
                _verified_successful_run_arguments(
                    selected=selected,
                    successful_run=successful_run,
                    receipt_path=receipt_path,
                    receipt_sha256=receipt_sha256,
                )
            )
        except (OSError, RuntimeError, ValueError, KeyError, TypeError) as exc:
            pytest.fail(f"NAMED_PARENT_SUCCESSFUL_RUN_REQUIRED: {exc}")
    elif successful_run is not None:
        pytest.fail("NAMED_PARENT_RUN_MUST_NOT_CLAIM_PREVIOUS_DISPATCH")
    transaction_input, lease_id = _parent_environment()
    try:
        launch = _current_child_python_launch()
    except NamedChildPythonLaunchError as exc:
        pytest.fail(str(exc))
    try:
        before = _live_parent_proof(
            request=selected,
            transaction_input=transaction_input,
            lease_id=lease_id,
        )
    except (OSError, RuntimeError, ValueError, KeyError) as exc:
        pytest.fail(f"NAMED_PARENT_PRE_DISPATCH_BLOCKED: {exc}")
    destination = (
        parent_evidence_root or ROOT / "outputs/validation_runtime/named_dq_parent_dispatch"
    )
    if not destination.resolve().is_relative_to(ROOT / "outputs/validation_runtime"):
        pytest.fail("NAMED_PARENT_EVIDENCE_OUTSIDE_VALIDATION_RESOURCE")
    directory = destination / selected.candidate_commit / f"{operation}-{uuid4().hex}"
    directory.mkdir(parents=True, exist_ok=False)
    pre_binding = _write_new(directory / "pre_dispatch_proof.json", _json_bytes(before))
    request_binding = _write_new(directory / "request.json", selected.canonical_bytes)
    command = [launch.executable, "-I", "-B", "-X", "utf8"]
    if fixed_probe is not None:
        command.extend(["-c", fixed_probe])
    command.extend(
        [
            str(ROOT / BOOTSTRAP_PATH),
            "--request",
            str(directory / "request.json"),
            "--request-sha256",
            selected.canonical_sha256,
            "--source-lease-id",
            lease_id,
            "--operation",
            operation,
        ]
    )
    if receipt_path is not None:
        command.extend(["--receipt-path", receipt_path])
    if receipt_sha256 is not None:
        command.extend(["--receipt-sha256", receipt_sha256])
    if run_dispatch_path is not None and run_dispatch_sha256 is not None:
        command.extend(
            ["--run-dispatch-path", run_dispatch_path, "--run-dispatch-sha256", run_dispatch_sha256]
        )
    started_at = _instant()
    process: subprocess.Popen[bytes] | None = None
    child_result: dict[str, Any] = {}
    stdout = stderr = b""
    child_pid: int | None = None
    observed_spawn_at: str | None = None
    terminal_state = "NOT_STARTED"
    failure: str | None = None
    try:
        if datetime.fromisoformat(before["active_lease"]["expires_at"]) <= datetime.fromisoformat(
            started_at
        ):
            raise ValueError("NAMED_PARENT_LEASE_EXPIRED_BEFORE_SPAWN")
        process = subprocess.Popen(
            command,
            executable=launch.executable,
            env=launch.environment,
            cwd=ROOT,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        child_pid, observed_spawn_at = process.pid, _instant()
        try:
            # A finite synthetic test-process bound, not a research threshold.
            stdout, stderr = process.communicate(timeout=120)
            terminal_state = "EXITED"
        except subprocess.TimeoutExpired:
            process.kill()
            stdout, stderr = process.communicate()
            terminal_state = "TIMED_OUT_AND_CHILD_REAPED"
        if terminal_state == "EXITED":
            value = json.loads(stdout.decode("utf-8"))
            if not isinstance(value, dict):
                raise ValueError("NAMED_PARENT_CHILD_RESULT_NOT_OBJECT")
            child_result = value
            if process.returncode == 0 and (
                value.get("process_id") != child_pid or value.get("source_lease_id") != lease_id
            ):
                raise ValueError("NAMED_PARENT_CHILD_ASSOCIATION_MISMATCH")
    except (OSError, ValueError) as exc:
        failure = str(exc)
    finally:
        if process is not None and process.poll() is None:
            # Only this Popen child is ours. Do not terminate unrelated Git,
            # pytest workers, processes, or another task's validation session.
            process.kill()
            stdout, stderr = process.communicate()
            terminal_state = "PARENT_ERROR_AND_CHILD_REAPED"
    terminal_at = _instant()
    try:
        after = _live_parent_proof(
            request=selected,
            transaction_input=transaction_input,
            lease_id=lease_id,
        )
    except (OSError, RuntimeError, ValueError, KeyError) as exc:
        after = {"status": "BLOCKED", "checked_at": _instant(), "detail": str(exc)}
        failure = f"NAMED_PARENT_POST_DISPATCH_BLOCKED: {exc}"
    if not (
        datetime.fromisoformat(before["checked_at"])
        <= datetime.fromisoformat(started_at)
        <= datetime.fromisoformat(observed_spawn_at or terminal_at)
        <= datetime.fromisoformat(terminal_at)
        <= datetime.fromisoformat(after["checked_at"])
    ):
        failure = "NAMED_PARENT_PROOF_TIME_ORDER_INVALID"
    post_binding = _write_new(directory / "post_dispatch_proof.json", _json_bytes(after))
    stdout_binding = _write_new(directory / "child_stdout.json", stdout)
    stderr_binding = _write_new(directory / "child_stderr.txt", stderr)
    count = child_result.get("canonical_dq_call_count")
    if type(count) is not int:
        count = None
    parent_receipt: dict[str, Any] = {
        "schema_version": "named_data_quality_parent_dispatch.v1",
        "profile": "ACTUAL_CANDIDATE_SYNTHETIC_E2E",
        "child_entrypoint_profile": (
            "FIVE_CANDIDATE_PREVIEW_TEST_PROBE"
            if five_candidate_preview_probe
            else (
                "EQUAL_RISK_PRICE_TEST_PROBE"
                if equal_risk_price_probe
                else "TEST_PROBE" if test_probe else "PRODUCTION_CLI"
            )
        ),
        "fixed_test_probe_sha256": (
            _sha(fixed_probe.encode("utf-8")) if fixed_probe is not None else None
        ),
        "status_semantics": "PARENT_ASSOCIATION_AND_PROCESS_OBSERVATION_ONLY",
        "status": "PASS" if failure is None and terminal_state == "EXITED" else "BLOCKED",
        "environment_inputs": {TRANSACTION_ENV: transaction_input, LEASE_ENV: lease_id},
        "candidate_commit": selected.candidate_commit,
        "execution_root": ROOT.as_posix(),
        "request": request_binding,
        "request_id": selected.request_id,
        "source_lease_id": lease_id,
        "operation": operation,
        "command": command,
        "launch_audit": launch.audit,
        "parent_pid": os.getpid(),
        "child_pid": child_pid,
        "spawn_requested_at": started_at,
        "spawn_observed_at": observed_spawn_at,
        "terminal_observed_at": terminal_at,
        "terminal_state": terminal_state,
        "returncode": None if process is None else process.returncode,
        "pre_dispatch_proof": pre_binding,
        "post_dispatch_proof": post_binding,
        "child_stdout": stdout_binding,
        "child_stderr": stderr_binding,
        "child_result": child_result,
        "observed_canonical_dq_call_count": count,
        "counter_observation_state": "KNOWN" if count is not None else "UNKNOWN",
        "failure": failure,
        "synthetic_inputs_only": True,
        "real_market_dq_call_count": 0,
        "source_checkout_copied": False,
        "lease_acquired_or_mutated": False,
        "verified_input_seal_exported": False,
        "dispatch_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }
    path = directory / "parent_receipt.json"
    _write_new(path, _json_bytes(parent_receipt))
    if failure is not None or terminal_state != "EXITED":
        pytest.fail(
            f"NAMED_PARENT_DISPATCH_BLOCKED: {path.as_posix()}: {failure or terminal_state}"
        )
    if operation == "run" and process is not None and process.returncode == 0:
        try:
            run_dispatch_path, run_dispatch_sha256 = _write_successful_run_proof(
                request=selected,
                child=child_result,
                parent_path=path,
                parent=parent_receipt,
                postguard=after,
            )
        except (OSError, RuntimeError, ValueError, KeyError, TypeError) as exc:
            pytest.fail(f"NAMED_PARENT_TERMINAL_PROOF_BLOCKED: {path.as_posix()}: {exc}")
    return ParentDispatchResult(
        child_result,
        process.returncode if process else None,
        path,
        parent_receipt,
        run_dispatch_path if operation == "run" else None,
        run_dispatch_sha256 if operation == "run" else None,
    )
