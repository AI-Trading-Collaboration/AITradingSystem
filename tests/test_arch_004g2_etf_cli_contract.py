from __future__ import annotations

import ast
import hashlib
from pathlib import Path

import pytest
import typer
from typer.testing import CliRunner

from ai_trading_system.cli_commands.etf_portfolio import etf_app
from ai_trading_system.interfaces.cli.etf_portfolio import etf_app as canonical_etf_app
from ai_trading_system.platform.architecture import (
    CLI_CONTRACT_SCHEMA_VERSION,
    CliContractError,
    assert_frozen_cli_contract,
    build_cli_contract,
    write_generated_architecture_artifact,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_PATH = PROJECT_ROOT / "src/ai_trading_system/cli_commands/etf_portfolio.py"
BASELINE_PATH = PROJECT_ROOT / "inputs/architecture/arch_004g2_etf_cli_contract.yaml"
REGISTRATION_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/registration.py"
)
DATA_COMMANDS_PATH = PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/data.py"
DATA_QUALITY_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/data_quality.py"
)
OPERATIONS_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/operations.py"
)
COMMON_PATH = PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/common.py"


def test_g2_1_etf_cli_contract_matches_frozen_runtime_tree() -> None:
    contract = build_cli_contract(
        etf_app,
        source_path=SOURCE_PATH,
        project_root=PROJECT_ROOT,
    )

    assert contract["schema_version"] == CLI_CONTRACT_SCHEMA_VERSION
    assert contract["counts"] == {
        "root_command_count": 41,
        "group_count": 291,
        "leaf_command_count": 993,
        "registered_leaf_count": 993,
        "unique_path_count": 1284,
        "duplicate_path_count": 0,
    }
    assert contract["tree_sha256"] == (
        "afa0760c82cf347bb135ecb12ae133bc16238fb53e28b7a0cf3c699f6ba1cec2"
    )
    assert contract["production_effect"] == "none"
    assert contract == safe_load_yaml_path(BASELINE_PATH)
    assert_frozen_cli_contract(contract, baseline_path=BASELINE_PATH)


def test_g2_1_cli_contract_blocks_duplicate_registration(tmp_path: Path) -> None:
    app = typer.Typer()

    @app.command("same")
    def first() -> None:
        pass

    @app.command("same")
    def second() -> None:
        pass

    with pytest.raises(CliContractError, match="CLI_CONTRACT_DUPLICATE_PATH"):
        build_cli_contract(app, source_path=__file_path(), project_root=tmp_path)


def test_g2_1_cli_contract_detects_option_default_and_help_drift(tmp_path: Path) -> None:
    before = typer.Typer()
    after = typer.Typer()

    @before.command("run", help="before help")
    def before_run(limit: int = typer.Option(5, "--limit")) -> None:
        pass

    @after.command("run", help="after help")
    def after_run(limit: int = typer.Option(6, "--limit")) -> None:
        pass

    source = __file_path()
    before_contract = build_cli_contract(before, source_path=source, project_root=PROJECT_ROOT)
    after_contract = build_cli_contract(after, source_path=source, project_root=PROJECT_ROOT)
    assert before_contract["tree_sha256"] != after_contract["tree_sha256"]

    frozen_path = tmp_path / "cli_contract.yaml"
    write_generated_architecture_artifact(frozen_path, before_contract)
    with pytest.raises(CliContractError, match="CLI_CONTRACT_BASELINE_DRIFT"):
        assert_frozen_cli_contract(after_contract, baseline_path=frozen_path)


def test_g2_2_registration_shell_owns_every_app_and_group_relationship() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    registration_tree = ast.parse(REGISTRATION_PATH.read_text(encoding="utf-8"))

    assert canonical_etf_app is etf_app
    assert _typer_app_count(legacy_tree) == 0
    assert _add_typer_count(legacy_tree) == 0
    assert _typer_app_count(registration_tree) == 291
    assert _add_typer_count(registration_tree) == 290
    assert len(SOURCE_PATH.read_text(encoding="utf-8").splitlines()) == 35554
    assert len(REGISTRATION_PATH.read_text(encoding="utf-8").splitlines()) == 1855


@pytest.mark.parametrize(
    ("args", "expected_sha256"),
    [
        (["data", "--help"], "a3699045160cf408407036e9d4b9d6433ad4b7518ccfdd9656a8082525109a3f"),
        (
            ["portfolio", "--help"],
            "5b6a33a94f50471ec8b4f811f8b3ba51060483b9e963a9f0d0e50dae8045d161",
        ),
    ],
)
def test_g2_2_real_cli_help_fixtures_preserve_bytes(
    args: list[str],
    expected_sha256: str,
) -> None:
    result = CliRunner().invoke(etf_app, args, terminal_width=120, color=False)

    assert result.exit_code == 0
    assert result.exception is None
    assert hashlib.sha256(result.stdout.encode("utf-8")).hexdigest() == expected_sha256


def test_g2_3_data_feature_callbacks_and_common_helpers_leave_legacy_root() -> None:
    legacy_names = _function_names(ast.parse(SOURCE_PATH.read_text(encoding="utf-8")))
    data_names = _function_names(ast.parse(DATA_COMMANDS_PATH.read_text(encoding="utf-8")))
    common_names = _function_names(ast.parse(COMMON_PATH.read_text(encoding="utf-8")))

    callbacks = {"data_ingest_command", "data_validate_command", "features_build_command"}
    helpers = {"parse_date", "resolve_date", "satellite_symbols"}
    assert legacy_names.isdisjoint(callbacks | {f"_{name}" for name in helpers})
    assert callbacks <= data_names
    assert helpers <= common_names


def test_g2_3_data_quality_callbacks_leave_legacy_root() -> None:
    legacy_names = _function_names(ast.parse(SOURCE_PATH.read_text(encoding="utf-8")))
    canonical_names = _function_names(
        ast.parse(DATA_QUALITY_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "data_quality_price_freshness_command",
        "data_quality_report_command",
        "data_quality_validate_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names


def test_g2_3_operations_callbacks_and_parser_leave_legacy_root() -> None:
    legacy_names = _function_names(ast.parse(SOURCE_PATH.read_text(encoding="utf-8")))
    canonical_names = _function_names(
        ast.parse(OPERATIONS_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "ops_dry_run_command",
        "ops_report_command",
        "ops_validate_command",
    }
    assert legacy_names.isdisjoint(callbacks | {"_parse_operations_graph_cadence"})
    assert callbacks | {"parse_operations_graph_cadence"} <= canonical_names


def __file_path() -> Path:
    return Path(__file__).resolve()


def _typer_app_count(tree: ast.Module) -> int:
    return sum(
        isinstance(node, (ast.Assign, ast.AnnAssign))
        and isinstance(node.value, ast.Call)
        and isinstance(node.value.func, ast.Attribute)
        and isinstance(node.value.func.value, ast.Name)
        and node.value.func.value.id == "typer"
        and node.value.func.attr == "Typer"
        for node in tree.body
    )


def _add_typer_count(tree: ast.Module) -> int:
    return sum(
        isinstance(node, ast.Expr)
        and isinstance(node.value, ast.Call)
        and isinstance(node.value.func, ast.Attribute)
        and node.value.func.attr == "add_typer"
        for node in tree.body
    )


def _function_names(tree: ast.Module) -> set[str]:
    return {
        node.name
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
