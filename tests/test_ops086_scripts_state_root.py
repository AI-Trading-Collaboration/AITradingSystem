"""OPS-086-04: scripts/ take their state paths from ai_trading_system.paths.

Each test loads a script from its file, the way `python scripts/<name>.py` does, and stops it at
argument parsing or at the first call into the package, so no script does any work.
"""

from __future__ import annotations

import argparse
import importlib.util
import inspect
import itertools
import sys
from collections.abc import Callable
from datetime import date
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from ai_trading_system.paths import CODE_ROOT, STATE_ROOT_ENV

SCRIPTS = CODE_ROOT / "scripts"
REPORTS = ("outputs", "reports")
AUDIT = ("data", "trading_engine", "audit")
DATA = ("data",)

# (script, argparse dest, default path parts under the state root)
ARGUMENT_DEFAULTS = [
    ("run_daily_shadow_vs_production_comparison", "data_root", DATA),
    ("run_daily_shadow_vs_production_comparison", "reports_dir", REPORTS),
    ("run_daily_shadow_weight_iteration", "data_root", DATA),
    ("run_daily_shadow_weight_iteration", "reports_dir", REPORTS),
    ("run_daily_weight_adjustment", "reports_dir", REPORTS),
    ("run_daily_weight_adjustment_scheduler_dry_run", "reports_dir", REPORTS),
    ("run_ibkr_paper_controlled_fill_test", "output_dir", REPORTS),
    ("run_ibkr_paper_order_lifecycle", "output_dir", REPORTS),
    ("run_ibkr_paper_readonly_snapshot", "output_dir", REPORTS),
    ("run_paper_signal_quality", "reports_dir", REPORTS),
    ("run_paper_trading_demo", "audit_root", AUDIT),
    ("run_paper_trading_from_candidates", "audit_root", AUDIT),
    ("run_paper_trading_replay", "reports_dir", REPORTS),
    ("run_paper_trading_replay", "audit_root", AUDIT),
    ("run_paperbroker_fill_model_calibration", "reports_dir", REPORTS),
    ("run_shadow_parameter_impact", "reports_dir", REPORTS),
    ("run_shadow_promotion_apply", "data_root", DATA),
    ("run_shadow_promotion_apply_preflight", "data_root", DATA),
    ("run_shadow_promotion_lifecycle_audit", "data_root", DATA),
    ("run_shadow_promotion_proposal", "data_root", DATA),
    ("run_shadow_promotion_rollback", "data_root", DATA),
    ("run_shadow_vs_production_multi_day_review", "data_root", DATA),
    ("run_weight_adjustment_candidates", "reports_dir", REPORTS),
    ("run_weight_candidate_evaluation", "reports_dir", REPORTS),
    ("run_weight_promotion_gate", "reports_dir", REPORTS),
]

# (script, function, parameter, default path parts under the state root)
FUNCTION_DEFAULTS = [
    (
        "run_daily_weight_adjustment_scheduler_dry_run",
        "write_daily_weight_adjustment_scheduler_dry_run_report",
        "reports_dir",
        REPORTS,
    ),
    ("run_ibkr_paper_order_lifecycle", "run_order_lifecycle", "output_dir", REPORTS),
    ("run_ibkr_paper_readonly_snapshot", "run_snapshot", "output_dir", REPORTS),
    ("run_paper_trading_replay", "run_paper_trading_replay", "reports_dir", REPORTS),
    ("run_paper_trading_replay", "run_paper_trading_replay", "audit_root", AUDIT),
]

_module_ids = itertools.count()


class _Stopped(Exception):
    def __init__(self, captured: Any) -> None:
        super().__init__("stopped")
        self.captured = captured


@pytest.fixture
def separate_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    state = (tmp_path / "state").resolve()
    state.mkdir()
    monkeypatch.setenv(STATE_ROOT_ENV, str(state))
    return state


@pytest.fixture
def load_script(monkeypatch: pytest.MonkeyPatch) -> Callable[[str], ModuleType]:
    monkeypatch.setattr(sys, "path", list(sys.path))

    def load(name: str) -> ModuleType:
        path = SCRIPTS / f"{name}.py"
        spec = importlib.util.spec_from_file_location(
            f"_ops086_script_{name}_{next(_module_ids)}", path
        )
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    return load


def _parser(module: ModuleType, monkeypatch: pytest.MonkeyPatch) -> argparse.ArgumentParser:
    def stop(self: argparse.ArgumentParser, *args: Any, **kwargs: Any) -> None:
        raise _Stopped(self)

    monkeypatch.setattr(argparse.ArgumentParser, "parse_args", stop)
    with pytest.raises(_Stopped) as stopped:
        module.main()
    parser = stopped.value.captured
    assert isinstance(parser, argparse.ArgumentParser)
    return parser


def _stop_at(module: ModuleType, name: str, monkeypatch: pytest.MonkeyPatch) -> None:
    def stop(*args: Any, **kwargs: Any) -> None:
        raise _Stopped(kwargs)

    monkeypatch.setattr(module, name, stop)


@pytest.mark.parametrize(("script", "dest", "parts"), ARGUMENT_DEFAULTS)
def test_argument_default_comes_from_the_state_root(
    separate_state: Path,
    load_script: Callable[[str], ModuleType],
    monkeypatch: pytest.MonkeyPatch,
    script: str,
    dest: str,
    parts: tuple[str, ...],
) -> None:
    parser = _parser(load_script(script), monkeypatch)

    assert parser.get_default(dest) == str(separate_state.joinpath(*parts))


def test_argument_defaults_are_unchanged_without_a_state_root(
    load_script: Callable[[str], ModuleType], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv(STATE_ROOT_ENV, raising=False)

    for script, dest, parts in ARGUMENT_DEFAULTS:
        parser = _parser(load_script(script), monkeypatch)
        assert parser.get_default(dest) == str(CODE_ROOT.joinpath(*parts)), (script, dest)


@pytest.mark.parametrize(("script", "function", "parameter", "parts"), FUNCTION_DEFAULTS)
def test_function_default_comes_from_the_state_root(
    separate_state: Path,
    load_script: Callable[[str], ModuleType],
    script: str,
    function: str,
    parameter: str,
    parts: tuple[str, ...],
) -> None:
    module = load_script(script)
    signature = inspect.signature(getattr(module, function))

    assert signature.parameters[parameter].default == separate_state.joinpath(*parts)


def test_function_defaults_are_unchanged_without_a_state_root(
    load_script: Callable[[str], ModuleType], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv(STATE_ROOT_ENV, raising=False)

    for script, function, parameter, parts in FUNCTION_DEFAULTS:
        signature = inspect.signature(getattr(load_script(script), function))
        assert signature.parameters[parameter].default == CODE_ROOT.joinpath(*parts), (
            script,
            function,
            parameter,
        )


def test_acl_rehearsal_output_parent_is_in_the_state_root_and_policy_in_the_code_root(
    separate_state: Path, load_script: Callable[[str], ModuleType]
) -> None:
    module = load_script("data_foundation_acl_rehearsal")

    assert module.DEFAULT_OUTPUT_PARENT == separate_state / "outputs" / "validation_runtime"
    assert module.DEFAULT_POLICY_PATH == CODE_ROOT / "config" / "data" / "data_foundation_acl.yaml"
    assert module.DEFAULT_POLICY_PATH.is_file()


def test_durability_rehearsal_policy_stays_in_the_code_root(
    separate_state: Path, load_script: Callable[[str], ModuleType]
) -> None:
    module = load_script("data_foundation_durability_rehearsal")

    assert module.DEFAULT_POLICY_PATH == (
        CODE_ROOT / "config" / "data" / "data_foundation_durability.yaml"
    )
    assert module.DEFAULT_POLICY_PATH.is_file()


def test_paper_runner_default_candidates_path_is_in_the_state_root(
    separate_state: Path,
    load_script: Callable[[str], ModuleType],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_script("run_paper_trading_from_candidates")
    _stop_at(module, "run_from_candidates", monkeypatch)
    argv = ["run_paper_trading_from_candidates.py", "--date", "2026-10-09"]
    monkeypatch.setattr(sys, "argv", argv)

    with pytest.raises(_Stopped) as stopped:
        module.main()

    kwargs = stopped.value.captured
    assert kwargs["candidates_path"] == (
        separate_state / "outputs" / "reports" / "order_intent_candidates_2026-10-09.json"
    )
    assert kwargs["audit_root"] == separate_state / "data" / "trading_engine" / "audit"
    assert kwargs["project_root"] == CODE_ROOT


def test_paper_runner_decision_snapshot_follows_the_project_root(
    separate_state: Path, load_script: Callable[[str], ModuleType], tmp_path: Path
) -> None:
    module = load_script("run_paper_trading_from_candidates")
    as_of = date(2026, 10, 9)
    name = "decision_snapshot_2026-10-09.json"

    def snapshot_path(project_root: Path) -> str:
        payload = module._limited_daily_decision_summary_payload(
            as_of=as_of, reports_dir=tmp_path / "reports", project_root=project_root
        )
        records = [r for r in payload["source_artifacts"] if r["id"] == "decision_snapshot"]
        assert len(records) == 1
        return str(records[0]["path"])

    code_path = separate_state / "data" / "processed" / "decision_snapshots" / name
    assert snapshot_path(CODE_ROOT) == str(code_path)
    other = tmp_path / "other_project"
    other_path = other / "data" / "processed" / "decision_snapshots" / name
    assert snapshot_path(other) == str(other_path)


@pytest.mark.parametrize(
    ("script", "writer"),
    [
        ("data_quality_issue_attribution_inventory", "write_attribution_readiness_artifacts"),
        ("rate_issue_attribution_review_pack", "write_rate_issue_attribution_review_artifacts"),
    ],
)
def test_review_pack_default_outputs_stay_in_the_code_root(
    separate_state: Path,
    load_script: Callable[[str], ModuleType],
    monkeypatch: pytest.MonkeyPatch,
    script: str,
    writer: str,
) -> None:
    module = load_script(script)
    _stop_at(module, writer, monkeypatch)
    monkeypatch.setattr(sys, "argv", [f"{script}.py", "--repo-root", str(CODE_ROOT)])

    with pytest.raises(_Stopped) as stopped:
        module.main()

    kwargs = stopped.value.captured
    assert kwargs["json_path"] == CODE_ROOT / module.DEFAULT_JSON_PATH
    assert kwargs["markdown_path"] == CODE_ROOT / module.DEFAULT_MARKDOWN_PATH
    assert kwargs["validation_path"] == CODE_ROOT / module.DEFAULT_VALIDATION_PATH
