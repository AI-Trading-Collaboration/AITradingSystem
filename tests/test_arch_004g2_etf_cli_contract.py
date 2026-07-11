from __future__ import annotations

from pathlib import Path

import pytest
import typer

from ai_trading_system.cli_commands.etf_portfolio import etf_app
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


def __file_path() -> Path:
    return Path(__file__).resolve()
