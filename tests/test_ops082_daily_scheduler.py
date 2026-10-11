"""OPS-082 deterministic Windows scheduler: summary classification, run-control dedupe, recovery.

owner_decision:OPS-082:2026-10-11:deterministic_scheduler_v1.
"""

from __future__ import annotations

import importlib.util
import json
import os
import re
import subprocess
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from types import ModuleType

import pytest

from ai_trading_system.cli_commands.ops import _build_daily_terminal_recovery_request
from ai_trading_system.ops_daily import build_daily_ops_plan, run_daily_ops_plan_controlled
from ai_trading_system.platform.operations.runtime_checkout import (
    RuntimeCheckoutError,
    inspect_runtime_checkout,
    require_clean_runtime_release,
)

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts" / "ops"
AS_OF = "2026-10-09"
RUN_ID = "daily_ops_run:2026-10-09:20261011T003000Z"


def _summary_module() -> ModuleType:
    path = SCRIPTS / "daily_scheduler_summary.py"
    spec = importlib.util.spec_from_file_location("ops082_daily_scheduler_summary", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


summary = _summary_module()


def _write_json(path: Path, payload: object) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def _component(component_id: str, status: str = "PASS", blocker: str = "NONE", error: str = ""):
    return {
        "component_id": component_id,
        "status": status,
        "blocker_code": blocker,
        "attempt_count": 1,
        "error_summary": error or None,
    }


def _runtime(
    tmp_path: Path,
    *,
    run_id: str = RUN_ID,
    state_status: str = "BLOCKED",
    steps: list[dict[str, object]] | None = None,
    components: list[dict[str, object]] | None = None,
    metadata_status: str = "BLOCKED_DEPENDENCY",
) -> Path:
    runtime = tmp_path / "runtime"
    states = runtime / "outputs" / "run_control" / "daily" / "states"
    key = "operations_run_0123456789abcdef01234567"
    _write_json(
        states / f"{key}.json",
        {
            "as_of": AS_OF,
            "attempt": 1,
            "blocker_codes": ["DAILY_STEP_BLOCKED:validate_data"],
            "idempotency_key": key,
            "run_id": run_id,
            "status": state_status,
            "updated_at": "2026-10-11T00:40:00+00:00",
        },
    )
    _write_json(
        states / f"{key}.run_ledger.json",
        {
            "as_of": AS_OF,
            "entries": [
                {"step_id": step["step_id"], "status": step["status"], "blocker_codes": []}
                for step in steps or []
            ],
        },
    )
    bundle = (
        runtime
        / "outputs"
        / "runs"
        / "daily"
        / "20261011T003000Z"
        / f"as_of_{AS_OF}__{summary.safe_run_id(run_id)}"
    )
    _write_json(
        bundle / "metadata" / f"daily_ops_run_metadata_{AS_OF}.json",
        {"status": metadata_status, "step_results": steps or [], "input_visibility_issues": []},
    )
    if components is not None:
        _write_json(
            runtime
            / "outputs"
            / "daily_input_capture"
            / AS_OF
            / f"daily_input_capture_manifest_{AS_OF}.json",
            {"status": "PARTIAL_CAPTURE", "component_results": components},
        )
    return runtime


def _summarize(
    runtime: Path,
    tmp_path: Path,
    log: str | None,
    *,
    exit_code: int | None = 1,
    precheck_blocker: str | None = None,
    env: dict[str, str] | None = None,
) -> dict[str, object]:
    log_path = None
    if log is not None:
        log_path = tmp_path / "run.log"
        log_path.write_text(log, encoding="utf-8")
    evidence = summary.collect_evidence(
        runtime_root=runtime,
        window="PRIMARY",
        exit_code=exit_code,
        precheck_blocker=precheck_blocker,
        log_path=log_path,
    )
    result = summary.build_summary(
        evidence,
        started_at="2026-10-11T09:30:00+09:00",
        finished_at="2026-10-11T09:45:00+09:00",
        runtime_head="c" * 40,
        log_path=log_path,
        env=env or {},
    )
    md_path, json_path = summary.write_summary(result, tmp_path / "out" / "2026-10-11_PRIMARY")
    assert (
        json.loads(json_path.read_text(encoding="utf-8"))["blocker_category"]
        == (result["blocker_category"])
    )
    result["_markdown"] = md_path.read_text(encoding="utf-8")
    return result


def _executed_log(status: str = "BLOCKED_DEPENDENCY") -> str:
    return f"每日运行执行：{status}\nRun ID：{RUN_ID}\n"


PAID_SOURCE_STEPS = [
    {"step_id": "capture_daily_inputs", "status": "LIMITED", "error": None},
    {
        "step_id": "validate_data",
        "status": "BLOCKED",
        "error": "CAPTURE_COMPONENT_NOT_PASS:market_macro:FAIL",
    },
    {"step_id": "score_daily", "status": "BLOCKED", "error": "UPSTREAM_NOT_PASS:validate_data"},
    {"step_id": "reader_brief", "status": "BLOCKED", "error": "UPSTREAM_NOT_PASS:score_daily"},
    {"step_id": "pipeline_health", "status": "FAIL", "error": "return_code=1"},
]


def _paid_source_components(secret: str) -> list[dict[str, object]]:
    return [
        _component(
            "market_macro",
            "FAIL",
            "PROVIDER_QUOTA_EXHAUSTED",
            "Marketstack quota exhausted: HTTP 429 for "
            "https://api.marketstack.com/v1/eod?access_key=abcdef123456",
        ),
        _component(
            "fmp_forward_pit",
            "FAIL",
            "PROVIDER_PERMISSION_DENIED",
            f"HTTP 401 Unauthorized apikey={secret} subscription inactive",
        ),
        _component("sec_companyfacts"),
        _component("fmp_valuation", "FAIL", "CREDENTIAL_MISSING", "invalid api key"),
        _component("official_policy_sources"),
    ]


def test_paid_source_outage_is_classified_as_data_source_unavailable(tmp_path: Path) -> None:
    secret = "fmp-secret-value-123"
    runtime = _runtime(
        tmp_path,
        steps=PAID_SOURCE_STEPS,
        components=_paid_source_components(secret),
        metadata_status="FAIL",
    )

    result = _summarize(runtime, tmp_path, _executed_log("FAIL"), env={"FMP_API_KEY": secret})

    assert result["blocker_category"] == summary.DATA_SOURCE_UNAVAILABLE
    assert result["blocker_category_label"] == "数据源不可用"
    assert result["as_of"] == AS_OF
    assert result["run_state"]["status"] == "BLOCKED"
    assert result["run_state"]["belongs_to_this_invocation"] is True
    assert result["data_quality"]["validate_data_status"] == "BLOCKED"
    categories = {item["component_id"]: item["category"] for item in result["capture_components"]}
    assert categories == {
        "market_macro": summary.DATA_SOURCE_UNAVAILABLE,
        "fmp_forward_pit": summary.DATA_SOURCE_UNAVAILABLE,
        "sec_companyfacts": None,
        "fmp_valuation": summary.DATA_SOURCE_UNAVAILABLE,
        "official_policy_sources": None,
    }
    # pipeline_health is the always-run closing check; its FAIL follows the upstream blocker.
    assert not any(item["category"] == summary.CODE_DEFECT for item in result["findings"])
    text = json.dumps(result, ensure_ascii=False)
    assert secret not in text and "abcdef123456" not in text
    assert "<redacted" in text
    assert "数据源不可用" in result["_markdown"]
    assert result["production_effect"] == "none" and result["llm_used"] is False


def test_second_trigger_for_same_as_of_reports_waiting_without_rerun(tmp_path: Path) -> None:
    parent_run = "daily_ops_run:2026-10-09:20261011T003000Z"
    runtime = _runtime(
        tmp_path,
        run_id=parent_run,
        steps=PAID_SOURCE_STEPS,
        components=_paid_source_components("unused-secret"),
        metadata_status="FAIL",
    )
    log = (
        "Canonical run control：RUN_CONTROL_BLOCKED_TERMINAL_RECOVERY_REQUIRED\n"
        "Run ID：daily_ops_run:2026-10-09:20261011T083000Z\n"
    )

    result = _summarize(runtime, tmp_path, log)

    assert result["blocker_category"] == summary.WAITING_FOR_NEW_AS_OF
    assert result["run_state"]["belongs_to_this_invocation"] is False
    assert result["run_state"]["run_id"] == parent_run
    reasons = [item["reason"] for item in result["findings"]]
    assert "本次未执行任何步骤" in reasons[0]
    assert any(reason.startswith("原终态：capture 组件 fmp_forward_pit") for reason in reasons)
    assert "不是本次执行" in result["_markdown"]


@pytest.mark.parametrize(
    ("log", "steps", "components", "expected"),
    [
        pytest.param(
            f"Canonical run control：RUN_CONTROL_ALREADY_COMPLETE\nRun ID：{RUN_ID}\n",
            [],
            None,
            summary.NONE,
            id="already-complete",
        ),
        pytest.param(
            _executed_log("PASS"),
            [{"step_id": "validate_data", "status": "PASS"}],
            [_component("market_macro")],
            summary.NONE,
            id="pass",
        ),
        pytest.param(
            _executed_log("FAIL"),
            [
                {"step_id": "validate_data", "status": "PASS"},
                {
                    "step_id": "score_daily",
                    "status": "FAIL",
                    "error": "Traceback (most recent call last): KeyError: 'x'",
                },
            ],
            [_component("market_macro")],
            summary.CODE_DEFECT,
            id="step-traceback",
        ),
        pytest.param(
            _executed_log("FAIL"),
            [{"step_id": "capture_daily_inputs", "status": "LIMITED"}],
            [
                _component(
                    "sec_companyfacts",
                    "FAIL",
                    "REQUEST_FAILED",
                    "Traceback (most recent call last): AttributeError",
                )
            ],
            summary.CODE_DEFECT,
            id="capture-traceback",
        ),
        pytest.param(
            _executed_log("FAIL"),
            [{"step_id": "validate_data", "status": "FAIL", "error": "prices_stale"}],
            [_component("market_macro")],
            summary.DATA_QUALITY_FAILED,
            id="dq-fail",
        ),
        pytest.param(
            _executed_log("BLOCKED_DEPENDENCY"),
            [
                {
                    "step_id": "score_daily",
                    "status": "BLOCKED",
                    "error": "MISSING_ENV:OPENAI_API_KEY",
                }
            ],
            [_component("market_macro")],
            summary.CONTROL_PLANE,
            id="missing-env",
        ),
        pytest.param(
            _executed_log("BLOCKED_DEPENDENCY"),
            [{"step_id": "capture_daily_inputs", "status": "LIMITED"}],
            [_component("market_macro", "FAIL", "SOURCE_LEASE_CONFLICT")],
            summary.CONTROL_PLANE,
            id="source-lease-conflict",
        ),
        pytest.param(
            f"Canonical run control：RUN_CONTROL_BLOCKED_CONCURRENT\nRun ID：{RUN_ID}\n",
            [],
            None,
            summary.CONTROL_PLANE,
            id="concurrent",
        ),
        pytest.param(
            "Traceback (most recent call last):\nImportError: x\n",
            [],
            None,
            summary.CODE_DEFECT,
            id="crash-before-run-id",
        ),
    ],
)
def test_summary_classification(
    tmp_path: Path,
    log: str,
    steps: list[dict[str, object]],
    components: list[dict[str, object]] | None,
    expected: str,
) -> None:
    runtime = _runtime(
        tmp_path,
        state_status="PASS" if expected == summary.NONE else "FAILED",
        steps=steps,
        components=components,
        metadata_status="PASS" if expected == summary.NONE else "FAIL",
    )
    exit_code = 0 if expected == summary.NONE else 1

    result = _summarize(runtime, tmp_path, log, exit_code=exit_code)

    assert result["blocker_category"] == expected
    assert result["blocker_category_label"] == summary.CATEGORY_LABELS[expected]


def test_precheck_blocker_is_control_plane_without_daily_run_log(tmp_path: Path) -> None:
    result = _summarize(
        tmp_path / "runtime",
        tmp_path,
        None,
        exit_code=None,
        precheck_blocker="RUNTIME_CHECKOUT_DIRTY",
    )

    assert result["blocker_category"] == summary.CONTROL_PLANE
    assert "RUNTIME_CHECKOUT_DIRTY" in result["findings"][0]["reason"]
    assert result["read_errors"] == []


def test_summary_main_writes_both_files(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path, steps=PAID_SOURCE_STEPS, components=[])
    log_path = tmp_path / "x.log"
    log_path.write_text(_executed_log(), encoding="utf-8")
    stem = tmp_path / "scheduler" / "2026-10-11_RESCUE"

    code = summary.main(
        [
            "--runtime-root",
            str(runtime),
            "--window",
            "RESCUE",
            "--output-stem",
            str(stem),
            "--log",
            str(log_path),
            "--exit-code",
            "1",
        ]
    )

    assert code == 0
    payload = json.loads((stem.parent / "2026-10-11_RESCUE.summary.json").read_text("utf-8"))
    assert payload["window"] == "RESCUE"
    assert (
        (stem.parent / "2026-10-11_RESCUE.summary.md")
        .read_text("utf-8")
        .startswith("# 每日调度运行摘要")
    )


def test_same_as_of_second_trigger_does_not_rerun_after_capture_failure(tmp_path: Path) -> None:
    """The production run-control policy refuses a second ordinary run for a terminal as_of."""

    as_of = date(2026, 5, 6)
    plan = build_daily_ops_plan(as_of=as_of, project_root=tmp_path)
    calls: list[tuple[str, ...]] = []

    def provider_down_runner(command, **kwargs):
        calls.append(tuple(command))
        failed = "capture-daily-inputs" in command
        return subprocess.CompletedProcess(
            command,
            1 if failed else 0,
            stdout="",
            stderr="HTTP 401 Unauthorized" if failed else "",
        )

    env = {
        "FMP_API_KEY": "present",
        "MARKETSTACK_API_KEY": "present",
        "SEC_USER_AGENT": "AITradingSystem test@example.com",
        "OPENAI_API_KEY": "present",
    }
    first = run_daily_ops_plan_controlled(
        plan,
        project_root=tmp_path,
        env=env,
        runner=provider_down_runner,
        run_id="scheduler_primary",
        visibility_check_date=as_of,
        visibility_latest_completed_trading_day=as_of,
        run_control_root=tmp_path / "control",
    )
    first_calls = list(calls)
    second = run_daily_ops_plan_controlled(
        plan,
        project_root=tmp_path,
        env=env,
        runner=provider_down_runner,
        run_id="scheduler_rescue",
        visibility_check_date=as_of,
        visibility_latest_completed_trading_day=as_of,
        run_control_root=tmp_path / "control",
    )

    assert first.status in {"FAIL", "BLOCKED_DEPENDENCY"}
    assert any("capture-daily-inputs" in command for command in first_calls)
    assert not any("score-daily" in command for command in first_calls)
    assert second.status == "RUN_CONTROL_BLOCKED_TERMINAL_RECOVERY_REQUIRED"
    assert calls == first_calls


def _git(root: Path, *args: str) -> str:
    completed = subprocess.run(
        (
            "git",
            "-C",
            str(root),
            "-c",
            "user.name=AITS Test",
            "-c",
            "user.email=test@example.com",
            "-c",
            "commit.gpgsign=false",
            *args,
        ),
        capture_output=True,
        text=True,
        check=True,
        env={
            name: value
            for name, value in os.environ.items()
            if name not in {"GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE"}
        },
    )
    return completed.stdout.strip()


def _clean_runtime_repo(tmp_path: Path) -> tuple[Path, str]:
    root = tmp_path / "runtime_repo"
    root.mkdir()
    _git(root, "init", "-q")
    (root / ".gitignore").write_text("outputs/\n", encoding="utf-8")
    _git(root, "add", ".gitignore")
    _git(root, "commit", "-q", "-m", "release")
    head = _git(root, "rev-parse", "HEAD")
    _git(root, "update-ref", "refs/remotes/origin/main", head)
    return root, head


def _parent_manifest(root: Path, *, git_commit: str) -> None:
    _write_json(
        root / "outputs" / "runs" / "daily" / "20261011T003000Z" / "bundle" / "manifest.json",
        {"run_id": "parent", "as_of": AS_OF, "git_commit": git_commit},
    )


def _request(root: Path):
    return _build_daily_terminal_recovery_request(
        as_of=date.fromisoformat(AS_OF),
        run_output_root=root / "outputs" / "runs",
        parent_run_id="parent",
        recovery_from_step="artifact_lineage",
        recovery_reason_code="OPS_082_TEST",
        requested_at=datetime(2026, 10, 11, tzinfo=UTC),
        project_root=root,
    )


def test_cli_recovery_binds_clean_runtime_head_without_deployment_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("AITS_OPS_DEPLOYMENT_RECEIPT", raising=False)
    root, head = _clean_runtime_repo(tmp_path)
    _parent_manifest(root, git_commit="a" * 40)

    request = _request(root)

    assert request is not None
    assert request.current_release_commit == head
    assert request.runtime_origin_main_commit == head
    assert Path(request.runtime_checkout_root) == root.resolve()
    assert request.to_dict()["schema_version"] == "operations_recovery_request.v2"
    assert "deployment_receipt_path" not in request.to_dict()


def test_cli_recovery_rejects_dirty_runtime_checkout(tmp_path: Path) -> None:
    root, _ = _clean_runtime_repo(tmp_path)
    _parent_manifest(root, git_commit="a" * 40)
    (root / "stray.py").write_text("x = 1\n", encoding="utf-8")

    with pytest.raises(ValueError, match="RUNTIME_CHECKOUT_DIRTY"):
        _request(root)


def test_cli_recovery_rejects_head_not_on_origin_main(tmp_path: Path) -> None:
    root, _ = _clean_runtime_repo(tmp_path)
    _parent_manifest(root, git_commit="a" * 40)
    (root / "local.txt").write_text("local\n", encoding="utf-8")
    _git(root, "add", "local.txt")
    _git(root, "commit", "-q", "-m", "local only")

    with pytest.raises(ValueError, match="RUNTIME_CHECKOUT_HEAD_NOT_ON_ORIGIN_MAIN"):
        _request(root)


def test_runtime_checkout_ignores_gitignored_outputs(tmp_path: Path) -> None:
    root, head = _clean_runtime_repo(tmp_path)
    _write_json(root / "outputs" / "run_control" / "state.json", {"status": "PASS"})

    state = require_clean_runtime_release(root)

    assert state.clean and state.head_on_origin_main and state.head_commit == head


def test_runtime_checkout_without_origin_main_is_rejected(tmp_path: Path) -> None:
    root, _ = _clean_runtime_repo(tmp_path)
    _git(root, "update-ref", "-d", "refs/remotes/origin/main")

    with pytest.raises(RuntimeCheckoutError) as raised:
        inspect_runtime_checkout(root)

    assert raised.value.code == "RUNTIME_CHECKOUT_GIT_FAILED"


def _ps1(name: str) -> str:
    raw = (SCRIPTS / name).read_bytes()
    assert all(byte < 128 for byte in raw), f"{name} must stay ASCII for Windows PowerShell 5.1"
    return raw.decode("ascii")


def _code_lines(text: str) -> str:
    without_block_comments = re.sub(r"<#.*?#>", "", text, flags=re.DOTALL)
    return "\n".join(
        line for line in without_block_comments.splitlines() if not line.lstrip().startswith("#")
    )


def test_wrapper_calls_daily_run_once_with_the_daily_run_environment_only(
    tmp_path: Path,
) -> None:
    code = _code_lines(_ps1("daily_scheduler_run.ps1"))

    assert code.count("'daily-run'") == 1
    assert "Start-Process -FilePath $Aits" in code
    env_block = re.search(r"\$DailyRunEnvironment = @\((.*?)\)", code, flags=re.DOTALL)
    assert env_block is not None
    names = set(re.findall(r"'([A-Z_]+)'", env_block.group(1)))
    assert "AITS_OPS_DEPLOYMENT_RECEIPT" not in code
    assert not any(name.startswith("AITS_") for name in names)
    required: set[str] = set()
    for as_of, kwargs in (
        (date(2026, 5, 6), {}),
        (date(2026, 5, 6), {"include_valuation_snapshots": False}),
        (date(2026, 5, 9), {}),
    ):
        plan = build_daily_ops_plan(as_of=as_of, project_root=tmp_path, **kwargs)
        required |= {name for step in plan.steps for name in step.required_env_vars}
    assert required <= names
    assert {"CONGRESS_API_KEY", "GOVINFO_API_KEY"} <= names
    # Values are only ever written to the process environment, never to the log.
    assert re.search(r"Add-LogText[^\n]*\$value", code) is None


def test_register_script_creates_one_interactive_unelevated_task_with_two_triggers() -> None:
    code = _code_lines(_ps1("register_daily_scheduler_task.ps1"))

    assert code.count("Register-ScheduledTask") == 1
    assert "-Force" in code.split("Register-ScheduledTask", 1)[1].split("| Out-Null", 1)[0]
    assert "$PrimaryTime = '09:30'" in code and "$RescueTime = '17:30'" in code
    assert code.count("New-ScheduledTaskTrigger -Daily") == 2
    assert "-LogonType Interactive -RunLevel Limited" in code
    assert "-StartWhenAvailable -MultipleInstances IgnoreNew" in code
    assert "Password" not in code
    assert "Only one scheduling source is allowed" in code
