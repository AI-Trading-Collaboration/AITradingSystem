"""Synthetic capture/report tests, never proof of Git-bootstrap execution identity."""

from __future__ import annotations

import copy
import json
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import named_data_quality_support as parent_support
import pandas as pd
import pytest
from pydantic import TypeAdapter
from test_data_quality import _publish_quality_cache, _write_price_dates, _write_valid_cache
from test_named_data_quality_execution_contract import (
    _dispatch as _synthetic_dispatch,
)
from test_named_data_quality_execution_contract import (
    _receipt as _synthetic_receipt,
)
from test_named_data_quality_execution_contract import (
    _receipt_path as _synthetic_receipt_path,
)

from ai_trading_system.config import PROJECT_ROOT, load_data_quality
from ai_trading_system.contracts.data_quality_execution import DataQualityDateWindow
from ai_trading_system.contracts.named_data_quality_execution import (
    NamedArtifactBinding,
    NamedDQExecutionReceipt,
    NamedDQExecutionRequest,
    NamedDQRoots,
    NamedDQScope,
    NamedDQSuccessfulDispatchBinding,
    NamedSnapshotSelector,
)
from ai_trading_system.contracts.named_execution_context import NamedExecutionContextError
from ai_trading_system.data import named_quality_execution as execution
from ai_trading_system.data.download_publication import (
    DownloadPublicationError,
    ValidatedDownloadPublication,
)
from ai_trading_system.data.quality import DataQualityReport, validate_data_cache

_START = date(2026, 4, 29)
_END = date(2026, 4, 30)
_PUBLISHED = datetime(2026, 5, 1, tzinfo=UTC)
_DEPENDENCIES = (
    "config/data_quality/price_non_market_session_attribution_decision_v1.yaml",
    "config/data_quality/rate_row_issue_attribution_decision_v1.yaml",
    "config/data/archive/us_equity_special_closure_registry_1_0_0.yaml",
)


@pytest.mark.parametrize(
    "platform_name,in_venv",
    [("win32", True), ("win32", False), ("linux", True), ("darwin", False)],
)
def test_python_launch_mapping_preserves_logical_runtime_and_only_copies_child_environment(
    platform_name: str,
    in_venv: bool,
) -> None:
    # Pure path declarations: no executable, process, lease, context or DQ proof.
    base = "C:/synthetic/base" if platform_name == "win32" else "/synthetic/base"
    prefix = f"{base}/venv" if in_venv else base
    suffix = "/python.exe" if platform_name == "win32" else "/bin/python"
    logical, base_executable = prefix + suffix, base + suffix
    ambient = {
        "KEEP_UNRELATED": "synthetic-retained",
        "PYTHONEXECUTABLE": "synthetic-conflicting-executable",
        "__PYVENV_LAUNCHER__": "synthetic-stale-launcher",
        "pythonexecutable": "synthetic-case-alias",
    }
    original = ambient.copy()
    launch = parent_support._child_python_launch(
        platform_name=platform_name,
        implementation="cpython",
        logical_executable=logical,
        base_executable=base_executable,
        prefix=prefix,
        base_prefix=base,
        environment=ambient,
    )
    mapped = platform_name == "win32" and in_venv
    assert ambient == original
    assert launch.environment is not ambient
    assert launch.executable == (base_executable if mapped else logical)
    expected_overrides = {"__PYVENV_LAUNCHER__": logical} if mapped else {}
    assert launch.environment == {"KEEP_UNRELATED": "synthetic-retained", **expected_overrides}
    assert launch.audit["profile"] == (
        "CPYTHON_WINDOWS_DIRECT_VENV" if mapped else "CPYTHON_DIRECT"
    )
    assert launch.audit["logical_executable"] == logical
    assert launch.audit["os_executable"] == launch.executable
    assert launch.audit["logical_prefix"] == prefix
    assert launch.audit["base_prefix"] == base
    assert launch.audit["windows_redirector_bypassed"] is mapped
    assert launch.audit["venv_detected"] is in_venv
    assert launch.audit["child_environment_overrides"] == expected_overrides
    assert launch.audit["child_environment_removed_keys"] == sorted(
        original.keys() - {"KEEP_UNRELATED"}
    )
    assert launch.audit["parent_environment_mutated"] is False
    assert launch.audit["dispatch_authority_granted"] is False
    assert not any(value in json.dumps(launch.audit) for value in original.values())


@pytest.mark.parametrize(
    "field,value,code",
    [
        ("implementation", "pypy", "RUNTIME_UNSUPPORTED"),
        ("platform_name", "unreviewed-platform", "RUNTIME_UNSUPPORTED"),
        ("base_executable", None, "BASE_EXECUTABLE_INVALID"),
        ("base_executable", "relative/python.exe", "BASE_EXECUTABLE_INVALID"),
        ("base_executable", "C:/synthetic/base/pythonw.exe", "WINDOWS_EXECUTABLE_UNSUPPORTED"),
        ("base_executable", "C:/synthetic/venv/python.exe", "VENV_IDENTITY_INCONSISTENT"),
    ],
)
def test_python_launch_invalid_runtime_has_typed_failure_without_redirector_fallback(
    field: str,
    value: str | None,
    code: str,
) -> None:
    runtime: dict[str, Any] = {
        "platform_name": "win32",
        "implementation": "cpython",
        "logical_executable": "C:/synthetic/venv/python.exe",
        "base_executable": "C:/synthetic/base/python.exe",
        "prefix": "C:/synthetic/venv",
        "base_prefix": "C:/synthetic/base",
        "environment": {},
    }
    runtime[field] = value
    with pytest.raises(
        parent_support.NamedChildPythonLaunchError, match=f"NAMED_PARENT_PYTHON_{code}"
    ):
        parent_support._child_python_launch(**runtime)


@pytest.mark.parametrize("nonregular", [False, True])
def test_current_python_launch_rejects_missing_or_nonregular_base_without_spawn(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    nonregular: bool,
) -> None:
    base = tmp_path / "python.exe"
    if nonregular:
        base.mkdir()
    monkeypatch.setattr(
        parent_support,
        "sys",
        SimpleNamespace(
            platform=sys.platform,
            implementation=sys.implementation,
            executable=sys.executable,
            _base_executable=str(base),
            prefix=str(tmp_path / "venv"),
            base_prefix=str(tmp_path / "base"),
        ),
    )
    with pytest.raises(
        parent_support.NamedChildPythonLaunchError,
        match="NAMED_PARENT_PYTHON_BASE_EXECUTABLE_NOT_EXECUTABLE",
    ):
        parent_support._current_child_python_launch()


def test_stdlib_only_child_has_popen_pid_original_venv_and_isolated_no_pyc_flags() -> None:
    # This is only an interpreter launch regression, never a named bootstrap,
    # canonical DQ, real data, guard, lease, candidate identity or sealed input.
    parent_environment = dict(os.environ)
    launch = parent_support._current_child_python_launch()
    probe = (
        "import json,os,sys; "
        "print(json.dumps({'profile':'STDLIB_INTERPRETER_LAUNCH_PROBE',"
        "'pid':os.getpid(),'executable':sys.executable,'base_executable':sys._base_executable,"
        "'prefix':sys.prefix,'base_prefix':sys.base_prefix,"
        "'isolated':sys.flags.isolated,'dont_write_bytecode':sys.dont_write_bytecode,"
        "'project_imported':any(name=='ai_trading_system' or name.startswith('ai_trading_system.') "
        "for name in sys.modules)}))"
    )
    with subprocess.Popen(
        [launch.executable, "-I", "-B", "-X", "utf8", "-c", probe],
        executable=launch.executable,
        env=launch.environment,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ) as process:
        try:
            stdout, stderr = process.communicate(timeout=30)
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate()
    assert process.returncode == 0, stderr.decode("utf-8", errors="replace")
    actual = json.loads(stdout)
    assert actual["profile"] == "STDLIB_INTERPRETER_LAUNCH_PROBE"
    assert actual["pid"] == process.pid != os.getpid()
    assert Path(actual["executable"]) == Path(sys.executable)
    assert Path(actual["base_executable"]) == Path(sys._base_executable)
    assert Path(actual["prefix"]) == Path(sys.prefix)
    assert Path(actual["base_prefix"]) == Path(sys.base_prefix)
    assert actual["isolated"] == 1
    assert actual["dont_write_bytecode"] is True
    assert actual["project_imported"] is False
    assert dict(os.environ) == parent_environment


@dataclass(frozen=True)
class _Case:
    request: NamedDQExecutionRequest
    publication: ValidatedDownloadPublication


def _case(tmp_path: Path, *, issues: bool = False) -> _Case:
    source = tmp_path / "original-source"
    source.mkdir()
    prices, rates = _write_valid_cache(source, tickers=["MSFT"])
    end = _END
    if issues:
        end = date(2026, 5, 2)
        _write_price_dates(prices, ("2026-04-29", "2026-04-30", "2026-05-01", "2026-05-02"))
        frame = pd.read_csv(rates)
        frame.loc[0, "value"] = 30.0
        frame.to_csv(rates, index=False)
    output = source / "source-output"
    publication = _publish_quality_cache(
        output,
        prices_path=prices,
        rates_path=rates,
        requested_start=_START,
        requested_end=end,
        published_at=datetime.combine(end, datetime.min.time(), tzinfo=UTC) + timedelta(days=1),
    )
    pointer = json.loads(publication.discovery_pointer_path.read_bytes())
    request = NamedDQExecutionRequest(
        roots=NamedDQRoots(
            source_root=source.as_posix(),
            publication_root=output.as_posix(),
            execution_root=PROJECT_ROOT.as_posix(),
            evidence_root=(tmp_path / "evidence").as_posix(),
        ),
        selector=NamedSnapshotSelector(
            pointer_id=pointer["pointer_id"],
            pointer_sha256=publication.discovery_pointer_sha256,
            transaction_id=publication.transaction_id,
            transaction_sha256=publication.transaction_manifest_sha256,
        ),
        scope=NamedDQScope(
            as_of=end,
            requested_window=DataQualityDateWindow(_START, end),
            expected_price_tickers=("MSFT",),
            expected_rate_series=("DGS2", "DGS10"),
            input_roles=("prices", "rates"),
            require_secondary_prices=False,
        ),
        source_output_relative_path="source-output",
        policy_path="config/data_quality.yaml",
        execution_profile_id="manual.v1",
        # DTO placeholders only: no test initializes a context or claims that
        # these values establish actual loaded-code or committed Git identity.
        candidate_commit="a" * 40,
        source_manifest_path="config/data_governance/named_data_quality_execution_sources_v1.json",
        source_manifest_sha256="b" * 64,
    )
    return _Case(request, publication)


def _dependencies() -> tuple[NamedArtifactBinding, ...]:
    result = []
    for relative in _DEPENDENCIES:
        content = (PROJECT_ROOT / relative).read_bytes()
        result.append(
            NamedArtifactBinding("EXECUTION", relative, sha256(content).hexdigest(), len(content))
        )
    return tuple(result)


def _canonical_report(
    request: NamedDQExecutionRequest, capture: execution.CapturedNamedPublication
) -> DataQualityReport:
    return validate_data_cache(
        prices_path=capture.snapshots["prices"].path,
        rates_path=capture.snapshots["rates"].path,
        manifest_path=capture.snapshots["manifest"].path,
        expected_price_tickers=list(request.scope.expected_price_tickers),
        expected_rate_series=list(request.scope.expected_rate_series),
        quality_config=load_data_quality(),
        as_of=request.scope.as_of,
        requested_window=(request.scope.requested_window.start, request.scope.requested_window.end),
        file_snapshots=capture.snapshots,
        named_download_publication=capture.named,
        named_source_root=Path(request.roots.source_root),
        named_source_output_relative_path=request.source_output_relative_path,
    )


def _tree_bytes(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file()
    }


def test_capture_binds_actual_members_full_manifest_and_does_not_dispatch_or_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    case = _case(tmp_path)
    root = Path(case.request.roots.publication_root)
    before = _tree_bytes(root)

    def forbidden(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("capture must not dispatch DQ or write artifacts")

    monkeypatch.setattr(execution, "validate_data_cache", forbidden)
    monkeypatch.setattr(execution, "write_contained_artifact_bytes", forbidden)
    captured = execution.capture_named_publication(case.request)
    assert _tree_bytes(root) == before
    assert tuple(item.role for item in captured.inputs) == ("prices", "rates")
    assert set(captured.snapshots) == {"prices", "rates", "manifest"}
    assert len(captured.inputs) == 2  # Manifest is provenance, not a third CSV input.
    assert captured.manifest.member.sha256 == case.publication.manifest_sha256
    for item in captured.inputs:
        content = dict(captured.captured)[item.role]
        assert sha256(content).hexdigest() == item.member.sha256
        assert captured.snapshots[item.role].content == content
        assert captured.snapshots[item.role].path == root / item.member.relative_path
        row = captured.manifest.rows[item.manifest_row_ordinal]
        assert row.row_sha256 == item.manifest_row_sha256
        assert dict(row.fields)["output_path"] == item.original_output_path
        assert json.loads(dict(row.fields)["request_parameters"])["publication_transaction_id"] == (
            case.publication.transaction_id
        )


def test_capture_pins_old_selection_when_current_anchor_advances(tmp_path: Path) -> None:
    case = _case(tmp_path)
    first = execution.capture_named_publication(case.request)
    later = _publish_quality_cache(
        Path(case.request.roots.publication_root),
        prices_path=case.publication.legacy_prices_path,
        rates_path=case.publication.legacy_rates_path,
        published_at=_PUBLISHED + timedelta(minutes=1),
    )
    observed = execution.capture_named_publication(case.request)
    assert observed.publication.pointer_id == first.publication.pointer_id
    assert observed.publication.transaction_id != later.transaction_id
    assert observed.publication.anchor_generation > first.publication.anchor_generation
    assert observed.inputs == first.inputs
    assert observed.manifest == first.manifest
    assert observed.captured == first.captured


def test_capture_relocation_never_needs_original_source_or_legacy_projection(
    tmp_path: Path,
) -> None:
    case = _case(tmp_path)
    first = execution.capture_named_publication(case.request)
    destination = tmp_path / "relocated-publication"
    shutil.copytree(
        Path(case.request.roots.publication_root) / ".download_publications",
        destination / ".download_publications",
    )
    Path(case.request.roots.source_root).rename(tmp_path / "retired-synthetic-source")
    request = replace(
        case.request, roots=replace(case.request.roots, publication_root=destination.as_posix())
    )
    assert not Path(request.roots.source_root).exists()
    observed = execution.capture_named_publication(request)
    assert observed.inputs == first.inputs
    assert observed.captured == first.captured
    assert observed.publication == first.publication
    assert all(
        snapshot.path.is_relative_to(destination) for snapshot in observed.snapshots.values()
    )
    assert not (destination / "prices_daily.csv").exists()


def test_capture_missing_immutable_manifest_does_not_fall_back_to_legacy(tmp_path: Path) -> None:
    case = _case(tmp_path)
    case.publication.manifest_path.rename(case.publication.manifest_path.with_suffix(".retired"))
    assert case.publication.legacy_manifest_path.is_file()
    with pytest.raises(DownloadPublicationError, match="DOWNLOAD_MANIFEST_MISSING"):
        execution.capture_named_publication(case.request)


@pytest.mark.parametrize(
    ("role", "code"),
    [("manifest", "NAMED_DQ_MANIFEST_CHANGED"), ("prices", "NAMED_DQ_MEMBER_CHANGED")],
)
def test_capture_rejects_bytes_changed_after_named_resolution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, role: str, code: str
) -> None:
    case = _case(tmp_path)
    root = Path(case.request.roots.publication_root)
    selected = (
        case.publication.manifest_path if role == "manifest" else case.publication.prices_path
    )
    target = selected.relative_to(root).as_posix()
    original_read = execution.read_contained_artifact_bytes

    def changed_read(*, root: Path, relative_path: str) -> bytes:
        content = original_read(root=root, relative_path=relative_path)
        return content + b"\n" if relative_path == target else content

    monkeypatch.setattr(execution, "read_contained_artifact_bytes", changed_read)
    with pytest.raises(execution.NamedDataQualityExecutionError, match=code):
        execution.capture_named_publication(case.request)


@pytest.mark.parametrize("metadata_role", ["snapshot_manifest", "source_event"])
def test_capture_rejects_single_metadata_read_misbound_to_pinned_pointer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, metadata_role: str
) -> None:
    case = _case(tmp_path)
    captured = execution.capture_named_publication(case.request)
    binding = getattr(captured.publication, metadata_role)
    root = Path(case.request.roots.publication_root)
    original_read = execution.read_contained_artifact_bytes
    original_bytes = (root / binding.relative_path).read_bytes()
    reads = 0

    def changed_once(*, root: Path, relative_path: str) -> bytes:
        nonlocal reads
        content = original_read(root=root, relative_path=relative_path)
        if relative_path == binding.relative_path:
            reads += 1
            # Same-size substitution isolates hash identity, without changing
            # the immutable fixture or the resolver's earlier validated read.
            return b" " + content[1:] if reads == 1 else content
        return content

    def forbidden(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("metadata mismatch must not dispatch DQ or write")

    monkeypatch.setattr(execution, "read_contained_artifact_bytes", changed_once)
    monkeypatch.setattr(execution, "validate_data_cache", forbidden)
    monkeypatch.setattr(execution, "write_contained_artifact_bytes", forbidden)
    with pytest.raises(
        execution.NamedDataQualityExecutionError,
        match="NAMED_DQ_PUBLICATION_METADATA_MISMATCH",
    ):
        execution.capture_named_publication(case.request)
    assert reads == 1
    assert (root / binding.relative_path).read_bytes() == original_bytes
    assert sha256(case.publication.discovery_pointer_path.read_bytes()).hexdigest() == (
        case.request.selector.pointer_sha256
    )


@pytest.mark.parametrize("operation", ["run", "verify"])
def test_run_and_verify_require_real_context_before_any_capture_or_dq(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    case = _case(tmp_path)

    def forbidden(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("missing context must fail before capture/read/write/DQ")

    for name in (
        "capture_named_publication",
        "validate_data_cache",
        "read_contained_artifact_bytes",
        "write_contained_artifact_bytes",
    ):
        monkeypatch.setattr(execution, name, forbidden)
    bootstrap = cast(execution.NamedBootstrapAuthority, object())
    with pytest.raises(NamedExecutionContextError, match="NAMED_CONTEXT_REQUIRED"):
        if operation == "run":
            execution.run_named_data_quality_execution(case.request, bootstrap=bootstrap)
        else:
            execution.verify_named_data_quality_execution_receipt(
                case.request,
                receipt_path="unused/receipt.json",
                receipt_sha256="c" * 64,
                bootstrap=bootstrap,
            )


def test_report_bundle_uses_real_canonical_summaries(tmp_path: Path) -> None:
    case = _case(tmp_path)
    captured = execution.capture_named_publication(case.request)
    report = _canonical_report(case.request, captured)
    assert report.status == "PASS"
    raw = execution._report_bundle(report, capture=captured, request=case.request, dependencies=())
    payload = json.loads(raw)
    assert payload["input_summaries"] == execution._expected_summaries(captured)
    assert payload["canonical_report"] == TypeAdapter(DataQualityReport).dump_python(
        report, mode="json"
    )
    assert payload["complete_attributions"] == []
    assert payload["dispatch_allowed"] is False


@pytest.mark.parametrize("field", ["path", "sha", "rows", "ticker", "window", "missing_manifest"])
def test_report_bundle_rejects_summary_scope_or_window_mismatch(tmp_path: Path, field: str) -> None:
    case = _case(tmp_path)
    captured = execution.capture_named_publication(case.request)
    report = _canonical_report(case.request, captured)
    if field == "path":
        report = replace(
            report, price_summary=replace(report.price_summary, path=tmp_path / "other.csv")
        )
    elif field == "sha":
        report = replace(report, rate_summary=replace(report.rate_summary, sha256="0" * 64))
    elif field == "rows":
        report = replace(report, price_summary=replace(report.price_summary, rows=99))
    elif field == "ticker":
        report = replace(report, expected_price_tickers=("NVDA",))
    elif field == "window":
        report = replace(report, requested_window_start=_START - timedelta(days=1))
    else:
        report = replace(report, manifest_summary=None)
    with pytest.raises(
        execution.NamedDataQualityExecutionError, match="NAMED_DQ_REPORT_SUMMARY_MISMATCH"
    ):
        execution._report_bundle(report, capture=captured, request=case.request, dependencies=())


def test_real_complete_attributions_bind_immutable_source_rows_and_policy(tmp_path: Path) -> None:
    case = _case(tmp_path, issues=True)
    captured = execution.capture_named_publication(case.request)
    report = _canonical_report(case.request, captured)
    assert report.status == "FAIL"
    attributions = [
        issue.typed_attribution.to_dict()
        for issue in report.issues
        if issue.typed_attribution is not None
    ]
    assert {value["source"]["source_role"] for value in attributions} == {
        "primary_market_prices",
        "primary_macro_rates",
    }
    assert all(value["scope_status"] == "COMPLETE" for value in attributions)
    execution._check_attribution_payloads(
        attributions, capture=captured, request=case.request, dependencies=_dependencies()
    )
    bundle = execution._report_bundle(
        report, capture=captured, request=case.request, dependencies=_dependencies()
    )
    assert json.loads(bundle)["complete_attributions"] == attributions


@pytest.mark.parametrize(
    "mutation", ["source", "digest", "ordinal", "duplicate", "policy", "window"]
)
def test_complete_attribution_tamper_is_not_downgraded_to_unknown(
    tmp_path: Path, mutation: str
) -> None:
    case = _case(tmp_path, issues=True)
    captured = execution.capture_named_publication(case.request)
    report = _canonical_report(case.request, captured)
    original = next(
        issue.typed_attribution
        for issue in report.issues
        if issue.code == "prices_non_market_session_date"
    )
    assert original is not None
    payload = copy.deepcopy(original.to_dict())
    if mutation == "source":
        payload["source"]["sha256"] = "0" * 64
    elif mutation == "digest":
        payload["affected_rows"][0]["canonical_row_digest"] = "0" * 64
    elif mutation == "ordinal":
        payload["affected_rows"][0]["source_ordinal"] = 99
    elif mutation == "duplicate":
        payload["affected_rows"].append(copy.deepcopy(payload["affected_rows"][0]))
    elif mutation == "policy":
        payload["decision"]["sha256"] = "0" * 64
    else:
        payload["requested_window"]["start"] = "2021-02-22"
    with pytest.raises(execution.NamedDataQualityExecutionError, match="NAMED_DQ_ATTRIBUTION_"):
        execution._check_attribution_payloads(
            [payload], capture=captured, request=case.request, dependencies=_dependencies()
        )


@pytest.mark.parametrize(
    "field_path",
    [
        ("decision", "decision_id"),
        ("decision", "decision_version"),
        ("site_id",),
        ("issue_code",),
        ("scope_taxonomy",),
        ("calendar", "calendar_id"),
        ("calendar", "calendar_source"),
        ("calendar", "calendar_function"),
        ("calendar", "calendar_function_ast_sha256"),
        ("calendar", "special_closure_policy_id"),
        ("calendar", "special_closure_policy_version"),
        ("calendar", "special_closure_policy_sha256"),
    ],
)
def test_price_attribution_requires_full_reviewed_decision_and_calendar_identity(
    tmp_path: Path, field_path: tuple[str, ...]
) -> None:
    case = _case(tmp_path, issues=True)
    captured = execution.capture_named_publication(case.request)
    report = _canonical_report(case.request, captured)
    original = next(
        issue.typed_attribution
        for issue in report.issues
        if issue.code == "prices_non_market_session_date"
    )
    assert original is not None
    payload = copy.deepcopy(original.to_dict())
    assert payload["calendar"]["special_closure_policy_version"] == "1.0.0"
    target = payload
    for key in field_path[:-1]:
        target = target[key]
    key = field_path[-1]
    target[key] = "0" * 64 if key.endswith("sha256") else f"{target[key]}-tampered"
    # The decision file path/hash still names the real captured dependency;
    # membership of that hash alone must not bless a different policy identity.
    assert payload["decision"]["sha256"] == original.to_dict()["decision"]["sha256"]
    with pytest.raises(
        execution.NamedDataQualityExecutionError, match="NAMED_DQ_ATTRIBUTION_POLICY_MISMATCH"
    ):
        execution._check_attribution_payloads(
            [payload], capture=captured, request=case.request, dependencies=_dependencies()
        )


@pytest.mark.parametrize("mutation", ["missing_dates", "disjoint", "expected_mismatch"])
def test_evaluated_window_rejects_empty_or_misclaimed_coverage(
    tmp_path: Path, mutation: str
) -> None:
    case = _case(tmp_path)
    captured = execution.capture_named_publication(case.request)
    request, inputs = case.request, captured.inputs
    if mutation == "missing_dates":
        inputs = (replace(inputs[0], observed_min_date=None, observed_max_date=None), *inputs[1:])
    elif mutation == "disjoint":
        inputs = (
            replace(
                inputs[0], observed_min_date=date(2026, 5, 4), observed_max_date=date(2026, 5, 4)
            ),
            *inputs[1:],
        )
    else:
        request = replace(request, expected_evaluated_window=DataQualityDateWindow(_END, _END))
    with pytest.raises(
        execution.NamedDataQualityExecutionError,
        match="NAMED_DQ_(WINDOW_EMPTY|EVALUATED_WINDOW_MISMATCH)",
    ):
        execution._evaluated_window(request, inputs)


def test_evaluated_window_does_not_expand_to_wider_observed_history(tmp_path: Path) -> None:
    case = _case(tmp_path)
    captured = execution.capture_named_publication(case.request)
    inputs = tuple(
        replace(item, observed_min_date=date(2021, 2, 22), observed_max_date=date(2026, 9, 3))
        for item in captured.inputs
    )
    assert execution._evaluated_window(case.request, inputs) == case.request.scope.requested_window


@dataclass
class _DispatchCase:
    """Declaration bytes only: no child, lease, guard, DQ or execution context."""

    receipt: NamedDQExecutionReceipt
    proof: NamedDQSuccessfulDispatchBinding
    parent: dict[str, Any]
    stdout: dict[str, Any]
    postguard: dict[str, Any]
    request_content: bytes


def _dispatch_case() -> _DispatchCase:
    receipt = _synthetic_receipt()
    proof = _synthetic_dispatch(receipt)
    stdout = {
        "schema_version": "named_data_quality_bootstrap_result.v1",
        "status": receipt.report.status,
        "request_id": receipt.request.request_id,
        "process_id": proof.execution_pid,
        "source_lease_id": proof.source_lease_id,
        "receipt_id": receipt.receipt_id,
        "receipt_path": _synthetic_receipt_path(receipt),
        "receipt_sha256": receipt.canonical_sha256,
        "canonical_dq_call_count": 1,
        "child_started_at": proof.child_started_at.isoformat(),
        "child_terminal_checked_at": proof.child_terminal_checked_at.isoformat(),
        "verified_input_seal_exported": False,
        "dispatch_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }
    parent = {
        "schema_version": "named_data_quality_parent_dispatch.v1",
        "status_semantics": "PARENT_ASSOCIATION_AND_PROCESS_OBSERVATION_ONLY",
        "status": "PASS",
        "operation": "run",
        "terminal_state": "EXITED",
        "returncode": 0,
        "candidate_commit": receipt.request.candidate_commit,
        "execution_root": receipt.request.roots.execution_root,
        "request_id": receipt.request.request_id,
        "source_lease_id": proof.source_lease_id,
        "child_pid": proof.execution_pid,
        "observed_canonical_dq_call_count": 1,
        "counter_observation_state": "KNOWN",
        "failure": None,
        "child_result": copy.deepcopy(stdout),
        "dispatch_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }
    postguard = {
        "schema_version": "named_dq_existing_parent_proof.v1",
        "status": "PASS",
        "checked_at": proof.parent_postchecked_at.isoformat(),
        "candidate_commit": receipt.request.candidate_commit,
        "execution_root": receipt.request.roots.execution_root,
        "request_sha256": receipt.request.canonical_sha256,
        "active_lease": {"lease_id": proof.source_lease_id, "state": "ACTIVE"},
    }
    return _DispatchCase(receipt, proof, parent, stdout, postguard, receipt.request.canonical_bytes)


def _dispatch_bytes(
    case: _DispatchCase,
) -> tuple[NamedDQSuccessfulDispatchBinding, str, dict[str, bytes]]:
    # Rebinding parent/proof hashes lets each negative reach semantic correlation
    # checks; these synthetic declarations never authenticate a real coordinator.
    def encode(value: object) -> bytes:
        return (json.dumps(value, sort_keys=True, allow_nan=False) + "\n").encode()

    buffers = {
        "synthetic/request.json": case.request_content,
        "synthetic/stdout.json": encode(case.stdout),
        "synthetic/postguard.json": encode(case.postguard),
    }

    def parent_artifact(relative: str) -> dict[str, object]:
        content = buffers[relative]
        return {
            "path": (Path(case.receipt.request.roots.execution_root) / relative).as_posix(),
            "sha256": sha256(content).hexdigest(),
            "size_bytes": len(content),
        }

    parent = {
        **case.parent,
        "request": parent_artifact("synthetic/request.json"),
        "child_stdout": parent_artifact("synthetic/stdout.json"),
        "post_dispatch_proof": parent_artifact("synthetic/postguard.json"),
    }
    parent_content = encode(parent)
    parent_relative = case.proof.parent_receipt.relative_path
    buffers[parent_relative] = parent_content
    proof = replace(
        case.proof,
        parent_receipt=replace(
            case.proof.parent_receipt,
            sha256=sha256(parent_content).hexdigest(),
            size_bytes=len(parent_content),
        ),
    )
    dispatch_path = "synthetic/successful-dispatch.json"
    buffers[dispatch_path] = proof.canonical_bytes
    return proof, dispatch_path, buffers


def _install_dispatch_reader(
    monkeypatch: pytest.MonkeyPatch, case: _DispatchCase, buffers: dict[str, bytes]
) -> list[str]:
    reads: list[str] = []

    def read(*, root: Path, relative_path: str) -> bytes:
        assert root == Path(case.receipt.request.roots.execution_root)
        reads.append(relative_path)
        return buffers[relative_path]

    def forbidden(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("pure dispatch binding must not access context, dispatch or write")

    monkeypatch.setattr(execution, "read_contained_artifact_bytes", read)
    for name in (
        "require_named_execution_context",
        "validate_data_cache",
        "write_contained_artifact_bytes",
    ):
        monkeypatch.setattr(execution, name, forbidden)
    return reads


def test_successful_dispatch_correlates_declaration_bytes_without_execution_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _dispatch_case()
    proof, path, buffers = _dispatch_bytes(case)
    reads = _install_dispatch_reader(monkeypatch, case, buffers)
    observed = execution._verify_successful_run_dispatch(
        case.receipt,
        receipt_path=_synthetic_receipt_path(case.receipt),
        dispatch_path=path,
        dispatch_sha256=proof.canonical_sha256,
    )
    assert observed == proof
    assert observed.proof_semantics == "TRUSTED_COORDINATOR_CORRELATION_ONLY"
    assert not observed.dispatch_allowed
    assert reads == [
        path,
        proof.parent_receipt.relative_path,
        "synthetic/request.json",
        "synthetic/stdout.json",
        "synthetic/postguard.json",
    ]


@pytest.mark.parametrize(
    "path,digest", [(None, None), ("synthetic/proof.json", None), (None, "0" * 64)]
)
def test_naked_pass_receipt_requires_both_successful_dispatch_locators(
    monkeypatch: pytest.MonkeyPatch, path: str | None, digest: str | None
) -> None:
    case = _dispatch_case()
    reads = _install_dispatch_reader(monkeypatch, case, {})
    assert case.receipt.report.status == "PASS"
    with pytest.raises(
        execution.NamedDataQualityExecutionError, match="NAMED_DQ_SUCCESSFUL_DISPATCH_REQUIRED"
    ):
        execution._verify_successful_run_dispatch(
            case.receipt,
            receipt_path=_synthetic_receipt_path(case.receipt),
            dispatch_path=path,
            dispatch_sha256=digest,
        )
    assert reads == []


@pytest.mark.parametrize("mutation", ["proof_sha", "parent_sha", "receipt_path", "request_bytes"])
def test_successful_dispatch_rejects_locator_hash_or_request_byte_mismatch(
    monkeypatch: pytest.MonkeyPatch, mutation: str
) -> None:
    case = _dispatch_case()
    if mutation == "request_bytes":
        case.request_content = replace(
            case.receipt.request, source_output_relative_path="different/source-output"
        ).canonical_bytes
    proof, path, buffers = _dispatch_bytes(case)
    digest = "0" * 64 if mutation == "proof_sha" else proof.canonical_sha256
    receipt_path = _synthetic_receipt_path(case.receipt)
    if mutation == "receipt_path":
        receipt_path = "different/receipt.json"
    if mutation == "parent_sha":
        buffers[proof.parent_receipt.relative_path] += b"\n"
    _install_dispatch_reader(monkeypatch, case, buffers)
    code = {
        "proof_sha": "NAMED_DQ_DISPATCH_SHA_MISMATCH",
        "parent_sha": "NAMED_DQ_DISPATCH_PARENT_SHA_MISMATCH",
        "receipt_path": "does not match original receipt",
        "request_bytes": "NAMED_DQ_DISPATCH_PARENT_REQUEST_MISMATCH",
    }[mutation]
    with pytest.raises(ValueError, match=code):
        execution._verify_successful_run_dispatch(
            case.receipt, receipt_path=receipt_path, dispatch_path=path, dispatch_sha256=digest
        )


@pytest.mark.parametrize(
    "field,value",
    [
        ("schema_version", "unrelated_parent.v1"),
        ("status_semantics", "VERIFIED_RESEARCH_AUTHORITY"),
        ("status", "BLOCKED"),
        ("operation", "verify"),
        ("terminal_state", "TIMED_OUT"),
        ("returncode", 2),
        ("returncode", False),
        ("child_pid", 5678),
        ("source_lease_id", "other-lease"),
        ("request_id", "other-request"),
        ("observed_canonical_dq_call_count", 0),
        ("counter_observation_state", "UNKNOWN"),
        ("failure", {"reason": "post-DQ failure"}),
    ],
)
def test_failed_or_uncorrelated_parent_cannot_admit_its_leftover_pass_receipt(
    monkeypatch: pytest.MonkeyPatch, field: str, value: object
) -> None:
    case = _dispatch_case()
    case.parent[field] = value
    proof, path, buffers = _dispatch_bytes(case)
    _install_dispatch_reader(monkeypatch, case, buffers)
    assert case.receipt.report.status == "PASS"
    with pytest.raises(
        execution.NamedDataQualityExecutionError, match="NAMED_DQ_DISPATCH_PARENT_UNSUCCESSFUL"
    ):
        execution._verify_successful_run_dispatch(
            case.receipt,
            receipt_path=_synthetic_receipt_path(case.receipt),
            dispatch_path=path,
            dispatch_sha256=proof.canonical_sha256,
        )


@pytest.mark.parametrize(
    "field,value,copy_to_parent",
    [
        ("schema_version", "unrelated_child.v1", True),
        ("verified_input_seal_exported", True, True),
        ("receipt_id", "different-receipt", False),
        ("receipt_sha256", "0" * 64, True),
        ("request_id", "different-request", True),
        ("process_id", 5678, True),
        ("source_lease_id", "different-lease", True),
        ("canonical_dq_call_count", True, True),
        ("child_terminal_checked_at", "2026-09-05T01:00:03+00:00", True),
    ],
)
def test_successful_dispatch_requires_exact_original_child_stdout(
    monkeypatch: pytest.MonkeyPatch, field: str, value: object, copy_to_parent: bool
) -> None:
    case = _dispatch_case()
    case.stdout[field] = value
    if copy_to_parent:
        case.parent["child_result"] = copy.deepcopy(case.stdout)
    proof, path, buffers = _dispatch_bytes(case)
    _install_dispatch_reader(monkeypatch, case, buffers)
    with pytest.raises(
        execution.NamedDataQualityExecutionError, match="NAMED_DQ_DISPATCH_PARENT_RESULT_MISMATCH"
    ):
        execution._verify_successful_run_dispatch(
            case.receipt,
            receipt_path=_synthetic_receipt_path(case.receipt),
            dispatch_path=path,
            dispatch_sha256=proof.canonical_sha256,
        )


@pytest.mark.parametrize(
    "field,value",
    [
        ("schema_version", "unrelated_postguard.v1"),
        ("status", "FAIL"),
        ("checked_at", "2026-09-05T01:00:01+00:00"),
        ("candidate_commit", "2" * 40),
        ("execution_root", "D:/synthetic/other-root"),
        ("request_sha256", "0" * 64),
        ("active_lease", {"lease_id": "other-lease", "state": "ACTIVE"}),
        ("active_lease", {"lease_id": "synthetic_lease", "state": "RELEASED"}),
    ],
)
def test_successful_dispatch_requires_original_parent_postguard_correlation(
    monkeypatch: pytest.MonkeyPatch, field: str, value: object
) -> None:
    case = _dispatch_case()
    case.postguard[field] = value
    proof, path, buffers = _dispatch_bytes(case)
    _install_dispatch_reader(monkeypatch, case, buffers)
    with pytest.raises(
        execution.NamedDataQualityExecutionError,
        match="NAMED_DQ_DISPATCH_PARENT_POSTGUARD_MISMATCH",
    ):
        execution._verify_successful_run_dispatch(
            case.receipt,
            receipt_path=_synthetic_receipt_path(case.receipt),
            dispatch_path=path,
            dispatch_sha256=proof.canonical_sha256,
        )


@pytest.mark.parametrize("artifact", ["request", "stdout", "postguard"])
def test_parent_child_artifact_hashes_are_checked_before_semantic_admission(
    monkeypatch: pytest.MonkeyPatch, artifact: str
) -> None:
    case = _dispatch_case()
    proof, path, buffers = _dispatch_bytes(case)
    buffers[f"synthetic/{artifact}.json"] += b"\n"
    _install_dispatch_reader(monkeypatch, case, buffers)
    with pytest.raises(
        execution.NamedDataQualityExecutionError, match="NAMED_DQ_DISPATCH_PARENT_SHA_MISMATCH"
    ):
        execution._verify_successful_run_dispatch(
            case.receipt,
            receipt_path=_synthetic_receipt_path(case.receipt),
            dispatch_path=path,
            dispatch_sha256=proof.canonical_sha256,
        )


@pytest.mark.parametrize(
    "field,value",
    [
        ("path", "relative/request.json"),
        ("path", "D:/synthetic/outside/request.json"),
        ("path", "D:/synthetic/execution/../request.json"),
        ("size_bytes", True),
        ("size_bytes", -1),
        ("extra", "ignored-fields-are-forbidden"),
    ],
)
def test_parent_artifact_binding_requires_contained_path_and_strict_size_before_read(
    monkeypatch: pytest.MonkeyPatch, field: str, value: object
) -> None:
    case = _dispatch_case()
    reads = _install_dispatch_reader(monkeypatch, case, {})
    binding = {
        "path": "D:/synthetic/execution/synthetic/request.json",
        "sha256": "0" * 64,
        "size_bytes": 1,
        field: value,
    }
    with pytest.raises(
        execution.NamedDataQualityExecutionError, match="NAMED_DQ_DISPATCH_PARENT_INVALID"
    ):
        execution._read_parent_artifact(
            binding, root=Path(case.receipt.request.roots.execution_root)
        )
    assert reads == []


@pytest.mark.parametrize(
    "failure_mode,terminal_state,returncode,communicate_count,kill_count",
    [
        ("spawn_error", "NOT_STARTED", None, 0, 0),
        ("timeout", "TIMED_OUT_AND_CHILD_REAPED", -9, 2, 1),
        ("communicate_error", "PARENT_ERROR_AND_CHILD_REAPED", -9, 2, 1),
        ("invalid_json", "EXITED", 0, 1, 0),
        ("postguard_error", "EXITED", 0, 1, 0),
        ("pid_mismatch", "EXITED", 0, 1, 0),
    ],
)
def test_parent_failure_control_flow_preserves_memory_only_diagnostics_without_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_mode: str,
    terminal_state: str,
    returncode: int | None,
    communicate_count: int,
    kill_count: int,
) -> None:
    # This is NOT an actual-candidate fixture, child, guard, lease, DQ result or
    # persisted proof. All would-be artifact bytes are intercepted in memory.
    # The real candidate tests independently exercise these boundaries end to end.
    label = "CONTROL_FLOW_STUB_ONLY"
    sandbox_root = tmp_path / label
    original_request = _synthetic_receipt().request
    request = replace(
        original_request,
        roots=NamedDQRoots(
            source_root=(tmp_path / "unused-source").as_posix(),
            publication_root=(tmp_path / "unused-publication").as_posix(),
            execution_root=sandbox_root.as_posix(),
            evidence_root=(tmp_path / "unused-evidence").as_posix(),
        ),
    )
    # The dispatcher only accesses request; no market/publication fixture is
    # created or made authoritative to reach these failure-control branches.
    fixture = cast(parent_support.NamedExecutionFixture, SimpleNamespace(request=request))
    lease_id = f"{label}-lease"
    clock_base = datetime(2026, 9, 5, tzinfo=UTC)
    clock_tick = 0
    guard_calls = 0

    def instant() -> str:
        nonlocal clock_tick
        value = clock_base + timedelta(seconds=clock_tick)
        clock_tick += 1
        return value.isoformat()

    def control_flow_guard(**kwargs: Any) -> dict[str, object]:
        nonlocal guard_calls
        guard_calls += 1
        assert kwargs == {
            "request": request,
            "transaction_input": label,
            "lease_id": lease_id,
        }
        if guard_calls == 2 and failure_mode == "postguard_error":
            raise ValueError("SYNTHETIC_POSTGUARD_FAILURE")
        return {
            "profile": label,
            "status": label,  # Deliberately not a PASS guard or ACTIVE lease.
            "checked_at": instant(),
            "active_lease": {
                "state": label,
                "expires_at": (clock_base + timedelta(hours=1)).isoformat(),
            },
        }

    declared_stdout = json.dumps(
        {
            "profile": label,
            "status": label,
            "process_id": 4343 if failure_mode == "pid_mismatch" else 4242,
            "source_lease_id": lease_id,
            "canonical_dq_call_count": 1,  # A declaration, never an observed DQ call.
        }
    ).encode()
    raw_stdout = (
        b'{"profile":"CONTROL_FLOW_STUB_ONLY",'
        if failure_mode == "invalid_json"
        else declared_stdout
    )
    raw_stderr = b"CONTROL_FLOW_STUB_ONLY stderr\n"

    class ControlFlowProcess:
        pid = 4242

        def __init__(self) -> None:
            self.returncode: int | None = None
            self.communicate_count = 0
            self.kill_count = 0

        def communicate(self, timeout: int | None = None) -> tuple[bytes, bytes]:
            self.communicate_count += 1
            if self.communicate_count == 1 and failure_mode == "timeout":
                raise subprocess.TimeoutExpired(label, timeout)
            if self.communicate_count == 1 and failure_mode == "communicate_error":
                raise OSError("SYNTHETIC_COMMUNICATE_FAILURE")
            if self.returncode is None:
                self.returncode = 0
            return raw_stdout, raw_stderr

        def kill(self) -> None:
            self.kill_count += 1
            self.returncode = -9

        def poll(self) -> int | None:
            return self.returncode

    process = ControlFlowProcess()
    popen_calls: list[list[str]] = []

    def controlled_popen(command: list[str], **kwargs: Any) -> ControlFlowProcess:
        popen_calls.append(command)
        assert kwargs["cwd"] == sandbox_root
        assert command[1:3] == ["-I", "-B"]
        assert command[0] == kwargs["executable"] == expected_launch.executable
        if kwargs["env"] != expected_launch.environment:
            # Preserve the exact comparison without printing ambient values.
            pytest.fail("CONTROL_FLOW_CHILD_ENVIRONMENT_COPY_MISMATCH", pytrace=False)
        assert kwargs["env"] is not os.environ
        if failure_mode == "spawn_error":
            raise OSError("SYNTHETIC_POPEN_FAILURE")
        return process

    captured: dict[str, bytes] = {}

    def memory_sink(path: Path, content: bytes) -> dict[str, object]:
        assert path.is_relative_to(sandbox_root)
        assert path.name not in captured
        captured[path.name] = content
        return {
            "path": path.as_posix(),
            "sha256": sha256(content).hexdigest(),
            "size_bytes": len(content),
        }

    def forbidden(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("CONTROL_FLOW_STUB_ONLY cannot access real authority or DQ")

    original_popen = subprocess.Popen
    monkeypatch.setattr(parent_support, "ROOT", sandbox_root)
    monkeypatch.setattr(parent_support, "_instant", instant)
    monkeypatch.setattr(parent_support, "_live_parent_proof", control_flow_guard)
    monkeypatch.setattr(parent_support, "_write_new", memory_sink)
    # Replace only this helper's module reference, not the global subprocess
    # module used by pytest/xdist or unrelated processes in this worker.
    monkeypatch.setattr(
        parent_support,
        "subprocess",
        SimpleNamespace(
            Popen=controlled_popen,
            TimeoutExpired=subprocess.TimeoutExpired,
            DEVNULL=subprocess.DEVNULL,
            PIPE=subprocess.PIPE,
        ),
    )
    for name in (
        "_git",
        "IntegrationPublicationFence",
        "_write_successful_run_proof",
        "read_contained_artifact_bytes",
        "publish_download_transaction",
        "build_actual_candidate_fixture",
    ):
        monkeypatch.setattr(parent_support, name, forbidden)
    monkeypatch.setattr(execution, "validate_data_cache", forbidden)
    monkeypatch.setenv(parent_support.TRANSACTION_ENV, label)
    monkeypatch.setenv(parent_support.LEASE_ENV, lease_id)
    expected_launch = parent_support._current_child_python_launch()

    with pytest.raises(pytest.fail.Exception, match="NAMED_PARENT_DISPATCH_BLOCKED"):
        parent_support.dispatch_actual_candidate_child(fixture)

    assert guard_calls == 2
    assert len(popen_calls) == 1
    assert process.communicate_count == communicate_count
    assert process.kill_count == kill_count
    assert subprocess.Popen is original_popen
    assert list(captured) == [
        "pre_dispatch_proof.json",
        "request.json",
        "post_dispatch_proof.json",
        "child_stdout.json",
        "child_stderr.txt",
        "parent_receipt.json",
    ]
    assert json.loads(captured["pre_dispatch_proof.json"])["status"] == label
    parent = json.loads(captured["parent_receipt.json"])
    assert parent["status"] == "BLOCKED"
    assert parent["terminal_state"] == terminal_state
    assert parent["returncode"] == returncode
    assert parent["launch_audit"] == expected_launch.audit
    assert parent["child_pid"] == (None if failure_mode == "spawn_error" else process.pid)
    assert parent["dispatch_allowed"] is parent["verified_input_seal_exported"] is False
    assert captured["child_stdout.json"] == (b"" if failure_mode == "spawn_error" else raw_stdout)
    assert captured["child_stderr.txt"] == (b"" if failure_mode == "spawn_error" else raw_stderr)
    if failure_mode in {"postguard_error", "pid_mismatch"}:
        expected_failure = (
            "NAMED_PARENT_POST_DISPATCH_BLOCKED:"
            if failure_mode == "postguard_error"
            else "NAMED_PARENT_CHILD_ASSOCIATION_MISMATCH"
        )
        assert parent["failure"].startswith(expected_failure)
        assert parent["child_result"]["profile"] == label
        assert parent["observed_canonical_dq_call_count"] == 1
        assert parent["counter_observation_state"] == "KNOWN"
        assert json.loads(captured["post_dispatch_proof.json"])["status"] == (
            "BLOCKED" if failure_mode == "postguard_error" else label
        )
    else:
        # Even a parseable declaration returned while reaping a failed process
        # must not turn partial stdout into a known counter or successful run.
        assert parent["child_result"] == {}
        assert parent["observed_canonical_dq_call_count"] is None
        assert parent["counter_observation_state"] == "UNKNOWN"
        if failure_mode == "timeout":
            assert parent["failure"] is None
        else:
            assert parent["failure"]
    assert "successful_run_dispatch.json" not in captured
    assert not any(path.is_file() for path in tmp_path.rglob("*"))
