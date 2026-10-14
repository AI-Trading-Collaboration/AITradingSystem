"""OPS-086 step 1: what git tracked under data/ and outputs/ now lives on the code side.

With a separate state root (AITS_STATE_ROOT, DATA-023) the state directories hold state only.
Trade theses, the owner decision audit log and the committed research results are reviewed and
committed with the code, so they are read from the code root whatever the state root is.
Requirement: docs/requirements/OPS-086_Daily_Release_Bundle.md section 3. Written before the
change; the implementation must not edit this file.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import os
import re
import subprocess
import sys
from datetime import date
from pathlib import Path

import pytest
import yaml

from ai_trading_system.paths import CODE_ROOT, STATE_ROOT_ENV

# sha256 of each file on main 62502d10 before the move; the move keeps every byte.
MOVED = {
    "inputs/trade_theses/amd_ai_accelerator_share_2026_q2.yaml": (
        "e42d0547dc229a23f7a66aa49bf0fed6fd9cf51ddf35c3ddc51c9e0c5fca5f4f"
    ),
    "inputs/trade_theses/goog_ai_search_cloud_2026_q2.yaml": (
        "954fb3adcc615d81c71d8fe3a04b847561a536339d4ed1677dad3474cf84277f"
    ),
    "inputs/trade_theses/intc_foundry_turnaround_2026_q2.yaml": (
        "861c809e44f3543fec769f78c7411a4cf3ab8bdcc30e5429e05390514a0e8e76"
    ),
    "inputs/trade_theses/msft_ai_platform_2026_q2.yaml": (
        "e813782b3dfdccb7a7659550bf022d9c125302e9d3ddec09cb05e1b67b0d4203"
    ),
    "inputs/trade_theses/nvda_ai_infra_2026_q2.yaml": (
        "6255b287545b5dd3bf372ae276587681a3d62d7f26ca71c375a962797aeea0e2"
    ),
    "inputs/trade_theses/tsm_ai_foundry_packaging_2026_q2.yaml": (
        "6b0870c3cac9771bb4a924fa394bc0628a6ef38175b264c689b8945345ce624f"
    ),
    "inputs/governance/owner_decision_audit_log.jsonl": (
        "ee543e9a44c6a4779f26281fe2ac2ad52ee4c24f59495dc86788af62a5ea9a03"
    ),
    "research/_results/indicator_family_ablation/action_value_by_family.csv": (
        "0dc6bab4c079d7181757f91a7261773f40620957872574da680998d91afe86f8"
    ),
    "research/_results/indicator_family_ablation/action_value_by_family_summary.json": (
        "27b6dd3643ba000a905e74a6720ed18d45785be50112a7804066d6d005db1b99"
    ),
    "research/_results/indicator_family_ablation/family_only_models/"
    "breadth_participation_metrics.json": (
        "ef481ae55f7b0abf729f7401b137e23da9dc8c293d88a86a24cd463a4b1afa00"
    ),
    "research/_results/indicator_family_ablation/family_only_models/"
    "drawdown_recovery_metrics.json": (
        "16f5faa568fc8c41305bc569a6fc3a02e4cbbd944c60df7f26ecbc32a5eff0cb"
    ),
    "research/_results/indicator_family_ablation/family_only_models/event_risk_metrics.json": (
        "4ea1c7fe53af90bb618e5fea78aba65ca496dd33754600b0797b1d45aec1dce6"
    ),
    "research/_results/indicator_family_ablation/family_only_models/rates_liquidity_metrics.json": (
        "cf87b5ea938d462270c38b1dd2af4d26e670548a9d94072ecdbc6af46a728316"
    ),
    "research/_results/indicator_family_ablation/family_only_models/"
    "relative_strength_metrics.json": (
        "53293b34da5dd300af5e032e54f129c8b7dd80a704beaa90a27f1ad726334c7d"
    ),
    "research/_results/indicator_family_ablation/family_only_models/"
    "trend_persistence_metrics.json": (
        "3deb0cb9854efb1f3022c432647c4c3d93f1d006c7021d23c5109ff42aa58742"
    ),
    "research/_results/indicator_family_ablation/family_only_models/"
    "volatility_compression_metrics.json": (
        "6e5249e1408c4481b8056614e420583995d60861d5eb48687c777407c5d5dfb9"
    ),
    "research/_results/indicator_family_ablation/indicator_family_ablation_summary.yaml": (
        "2ea2e2034fd7a4621b67d793ebea03b335e91644ec10a41a29d7f4c31f394455"
    ),
    "research/_results/norgate_trial/daily_membership_snapshot_summary.csv": (
        "aac46e6773ed2608258743fa0cb6716b85c42be80b5f956e52a62c21104119b5"
    ),
    "research/_results/norgate_trial/membership_probe.csv": (
        "f4b53312c54bfe879f620a9573abc0ed1b8291bca195695d0059e8b023d86a8c"
    ),
    "research/_results/norgate_trial/membership_probe_summary.json": (
        "7d7b9e74dfef17cf5ee588a3345872e0dd205b8a24f64509334f479512523a65"
    ),
    "research/_results/norgate_trial/nasdaq100_breadth_prototype_2y.csv": (
        "45583948d7513857ac92505d0757913433fb16034bcfc3c7531dce486f9432ad"
    ),
    "research/_results/norgate_trial/norgate_2y_benchmark_consistency.csv": (
        "39762e613e6a78e1b4c5edcaf11d5d002532a66ad95e3bf763455f6842782283"
    ),
    "research/_results/norgate_trial/norgate_2y_partial_evidence_conclusion_matrix.json": (
        "6ca13426b83e75335b1f3836d32fa4ed5ea21db518dcff0709275d63758dbf47"
    ),
    "research/_results/norgate_trial/norgate_2y_partial_evidence_review.json": (
        "9add3fd7e12c1f1ee7285894244af7561708c28c8a15769aa0eac204e65b9300"
    ),
    "research/_results/norgate_trial/norgate_trial_breadth_feature_report_2y.csv": (
        "bcaa721e27c01d5dfc0f87373206e775771357a4e6dc7723b2f31ce989e9b8db"
    ),
    "research/_results/norgate_trial/norgate_trial_breadth_feature_report_2y.json": (
        "406fd34912399c9093a7b6e0e311f7f11c5aadd0f32f46cdce0b1cd7a548c908"
    ),
    "research/_results/norgate_trial/norgate_trial_local_signal_bucket_report_2y.csv": (
        "3b81aeea0ea94eff794836af1be5e332122bf9ea51c438d7bed64428b1f07781"
    ),
    "research/_results/norgate_trial/norgate_trial_local_signal_deterioration_report_2y.csv": (
        "76d2803d6985998725fce28fc1b33778eba2750f149599dc5f1b29193f80e70c"
    ),
    "research/_results/norgate_trial/norgate_trial_local_signal_report_2y.json": (
        "dea8087dd21aaa8e1861b9eeeb596ee169bb0468b34fe04f32ed79d029684482"
    ),
    "research/_results/norgate_trial/norgate_trial_partial_effectiveness_conclusion_matrix.json": (
        "5939a5832d8483da85098e6dbf21f46f55b57c2eafb3b4d99bf51a6c0684a775"
    ),
    "research/_results/norgate_trial/norgate_trial_partial_effectiveness_coverage_by_date.csv": (
        "e6cdb4e8c3facb760d00363a9d40fe5ee2764fb8f07bc803916cd526c8a36f20"
    ),
    "research/_results/norgate_trial/norgate_trial_partial_effectiveness_coverage_report.json": (
        "1400cd6b7c6a6abc895c6d17acabbf950a3bee6ca2738624bc1f46fa81342a7e"
    ),
    "research/_results/norgate_trial/price_coverage_summary.json": (
        "1b2a1140eba505265c780559c516a0358242f71c39a5d25e85cb193d0121822d"
    ),
    "research/_results/norgate_trial/smoke_test_summary.json": (
        "2487023f929d3cd0be68b93b5d3d0435b03761e2962f6deb32b19cbff4d24962"
    ),
    "research/_results/paid_data_due_diligence/vendor_scoring_matrix.csv": (
        "f5f84f0d24b2febae02c53f71d9d707052ac91fbf2b4c1794bd8e9f0ef846387"
    ),
}
RESEARCH_RESULTS = sorted(path for path in MOVED if path.startswith("research/_results/"))
# The registry names 27 of them; vendor_scoring_matrix.csv is named by
# inputs/research_reviews/paid_data_due_diligence_final_matrix.yaml and its test.
REGISTRY_NAMED = [path for path in RESEARCH_RESULTS if "/paid_data_due_diligence/" not in path]
THESES_DIR = CODE_ROOT / "inputs" / "trade_theses"
AUDIT_LOG = CODE_ROOT / "inputs" / "governance" / "owner_decision_audit_log.jsonl"
NORGATE_OUTPUT_ROOT = CODE_ROOT / "research" / "_results" / "norgate_trial"

OLD_LOCATIONS = (
    "data/external/trade_theses",
    "data/governance/owner_decision_audit_log",
    "outputs/research_trends/indicator_family_ablation",
    "outputs/research_trends/norgate_trial",
    "outputs/research_trends/paid_data_due_diligence",
)
# Records of what happened before the move keep the old wording.
HISTORICAL_PREFIXES = (
    "docs/archive/",
    "docs/requirements/",
    "docs/owner_reviews/",
    "docs/task_register.md",
    "docs/task_register_completed.md",
    "docs/implementation_backlog.md",
    "docs/operations/operations_runbook_legacy_",
    "tasks/",
    "research/_results/",
    "tests/test_ops086_tracked_state.py",
)
TEXT_SUFFIXES = {
    ".py",
    ".md",
    ".yaml",
    ".yml",
    ".json",
    ".jsonl",
    ".toml",
    ".ps1",
    ".txt",
    ".csv",
    ".html",
    ".cfg",
    ".ini",
}


def _tracked(*pathspec: str) -> list[str]:
    try:
        done = subprocess.run(
            ["git", "-C", str(CODE_ROOT), "ls-files", "-z", "--", *pathspec],
            capture_output=True,
            check=False,
            timeout=60,
        )
    except OSError:
        pytest.skip("git is not available")
    if done.returncode != 0:
        pytest.skip("not a git checkout")
    return [name for name in done.stdout.decode("utf-8").split("\0") if name]


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _defaults_with_separate_state_root(tmp_path: Path) -> dict[str, str]:
    """Defaults are evaluated at import: read them in a fresh interpreter whose state root is
    somewhere else."""
    script = "\n".join(
        [
            "import inspect, json",
            "from ai_trading_system.thesis import DEFAULT_TRADE_THESES_DIR",
            "from ai_trading_system.cli_commands import score_daily, thesis",
            "from ai_trading_system.reports.owner_decision_audit_log import (",
            "    DEFAULT_OWNER_DECISION_AUDIT_LOG_PATH,",
            ")",
            "from ai_trading_system.vendor_adapters.norgate_connector import (",
            "    DEFAULT_NORGATE_TRIAL_OUTPUT_ROOT,",
            ")",
            "def default(function, name):",
            "    return str(inspect.signature(function).parameters[name].default)",
            "print(json.dumps({",
            "    'constant': str(DEFAULT_TRADE_THESES_DIR),",
            "    'score_daily': default(score_daily.score_daily, 'thesis_path'),",
            "    'thesis_list': default(thesis.list_theses, 'input_path'),",
            "    'thesis_validate': default(thesis.validate_theses, 'input_path'),",
            "    'thesis_review': default(thesis.review_theses, 'input_path'),",
            "    'audit_log': str(DEFAULT_OWNER_DECISION_AUDIT_LOG_PATH),",
            "    'norgate': str(DEFAULT_NORGATE_TRIAL_OUTPUT_ROOT),",
            "}))",
        ]
    )
    state = tmp_path / "separate_state"
    state.mkdir()
    env = {**os.environ, STATE_ROOT_ENV: str(state.resolve())}
    done = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=env,
        cwd=tmp_path,
        check=False,
        timeout=300,
    )
    assert done.returncode == 0, done.stderr[-3000:]
    return json.loads(done.stdout.strip().splitlines()[-1])


def test_git_tracks_only_gitkeep_under_the_state_directories() -> None:
    tracked = _tracked("data", "outputs", "artifacts")
    assert tracked, "the .gitkeep files keep the legacy layout's directories"
    assert [name for name in tracked if not name.endswith("/.gitkeep")] == []


def test_moved_files_are_tracked_at_their_new_place_byte_for_byte() -> None:
    tracked = set(_tracked("inputs", "research/_results"))
    missing = sorted(set(MOVED) - tracked)
    assert missing == []
    changed = sorted(path for path, digest in MOVED.items() if _sha256(CODE_ROOT / path) != digest)
    assert changed == []
    assert sorted(_tracked("research/_results")) == RESEARCH_RESULTS
    assert sorted(_tracked("inputs/trade_theses")) == sorted(
        path for path in MOVED if path.startswith("inputs/trade_theses/")
    )


def test_defaults_point_to_the_code_side_whatever_the_state_root(tmp_path: Path) -> None:
    defaults = _defaults_with_separate_state_root(tmp_path)

    expected_theses = str(THESES_DIR)
    assert defaults["constant"] == expected_theses
    assert defaults["score_daily"] == expected_theses
    assert defaults["thesis_list"] == expected_theses
    assert defaults["thesis_validate"] == expected_theses
    assert defaults["thesis_review"] == expected_theses
    assert defaults["audit_log"] == str(AUDIT_LOG)
    assert defaults["norgate"] == str(NORGATE_OUTPUT_ROOT)


def test_defaults_in_this_process_match_the_constants() -> None:
    from ai_trading_system.cli_commands import reports as reports_cli
    from ai_trading_system.cli_commands import score_daily, thesis
    from ai_trading_system.reports import owner_decision_audit_log
    from ai_trading_system.thesis import DEFAULT_TRADE_THESES_DIR

    assert DEFAULT_TRADE_THESES_DIR == THESES_DIR
    signature = inspect.signature(score_daily.score_daily)
    assert signature.parameters["thesis_path"].default == THESES_DIR
    for command in (thesis.list_theses, thesis.validate_theses, thesis.review_theses):
        assert inspect.signature(command).parameters["input_path"].default == THESES_DIR
    assert owner_decision_audit_log.DEFAULT_OWNER_DECISION_AUDIT_LOG_PATH == AUDIT_LOG
    append = inspect.signature(reports_cli.owner_decision_audit_log_append_command)
    assert AUDIT_LOG in [parameter.default for parameter in append.parameters.values()]


def test_the_theses_load_from_the_code_side() -> None:
    from ai_trading_system.thesis import DEFAULT_TRADE_THESES_DIR, load_trade_thesis_store

    store = load_trade_thesis_store(DEFAULT_TRADE_THESES_DIR)
    assert len(store.loaded) == 6
    assert not store.load_errors


def test_the_audit_log_still_holds_its_three_records() -> None:
    lines = AUDIT_LOG.read_text(encoding="utf-8").splitlines()
    records = [json.loads(line) for line in lines if line.strip()]
    assert len(records) == 3


def test_the_pre_run_input_digest_reads_the_code_side_theses(tmp_path: Path) -> None:
    from ai_trading_system import ops_daily

    plan = ops_daily.build_daily_ops_plan(as_of=date(2026, 5, 6), project_root=tmp_path)
    paths = ops_daily._pre_run_input_paths(plan, tmp_path)
    assert tmp_path / "inputs" / "trade_theses" in paths
    assert not [path for path in paths if path.as_posix().endswith("external/trade_theses")]


def test_src_reads_trade_theses_only_from_inputs_or_the_replay_mirror() -> None:
    """Every `/ "trade_theses"` in src sits under inputs/, or is the replay mirror below a replay's
    input root (historical_replay copies the theses there and points the replay at them)."""
    allowed_endings = ('"inputs"', 'state_dir(paths.input_root, "data") / "external"')
    offenders = []
    for path in sorted((CODE_ROOT / "src" / "ai_trading_system").rglob("*.py")):
        text = re.sub(r"\s+", " ", path.read_text(encoding="utf-8"))
        text = text.replace("( ", "(").replace(" )", ")")
        for match in re.finditer(r' ?/ "trade_theses"', text):
            before = text[max(0, match.start() - 60) : match.start()]
            if not before.endswith(allowed_endings):
                offenders.append(f"{path.relative_to(CODE_ROOT).as_posix()}: ...{before}")
    assert offenders == []


def test_registry_still_names_the_moved_research_results() -> None:
    registry = yaml.safe_load((CODE_ROOT / "config" / "report_registry.yaml").read_text("utf-8"))
    globs = [glob for entry in registry["reports"] for glob in entry.get("artifact_globs", [])]
    stale = [glob for glob in globs if glob.startswith(OLD_LOCATIONS)]
    assert stale == []
    matched: set[str] = set()
    for glob in globs:
        if glob.split("/", 1)[0] in ("data", "outputs", "artifacts"):
            continue
        matched |= {path.relative_to(CODE_ROOT).as_posix() for path in CODE_ROOT.glob(glob)}
    assert sorted(set(REGISTRY_NAMED) - matched) == []


def test_the_due_diligence_matrix_names_the_moved_scoring_matrix() -> None:
    matrix = (
        CODE_ROOT / "inputs" / "research_reviews" / "paid_data_due_diligence_final_matrix.yaml"
    ).read_text("utf-8")
    assert matrix.count("research/_results/paid_data_due_diligence/vendor_scoring_matrix.csv") == 2


def test_source_registry_and_rule_card_name_the_code_side_theses() -> None:
    sources = yaml.safe_load((CODE_ROOT / "config" / "data_sources.yaml").read_text("utf-8"))
    theses = [s for s in sources["sources"] if s["source_id"] == "local_trade_theses"]
    assert len(theses) == 1
    assert theses[0]["endpoint"] == "inputs/trade_theses/*.yaml"
    assert theses[0]["cache_paths"] == ["inputs/trade_theses"]

    cards = yaml.safe_load((CODE_ROOT / "config" / "rule_cards.yaml").read_text("utf-8"))
    thesis_cards = [
        c for c in cards["cards"] if "inputs/trade_theses" in c.get("source_config_paths", [])
    ]
    assert thesis_cards, "the thesis rule card names its source"
    for card in thesis_cards:
        for raw in card["source_config_paths"]:
            if raw.split("/", 1)[0] not in ("data", "outputs", "artifacts"):
                assert (CODE_ROOT / raw).exists(), raw


def test_score_report_and_position_gate_label_the_code_side_theses() -> None:
    gates = (CODE_ROOT / "src/ai_trading_system/scoring/position_gates.py").read_text("utf-8")
    assert 'source="inputs/trade_theses"' in gates
    daily = (CODE_ROOT / "src/ai_trading_system/scoring/daily.py").read_text("utf-8")
    assert '("交易 thesis", "`inputs/trade_theses/*.yaml`")' in daily


def test_no_current_file_names_an_old_location() -> None:
    offenders = []
    for name in _tracked():
        if name.startswith(HISTORICAL_PREFIXES) or Path(name).suffix not in TEXT_SUFFIXES:
            continue
        path = CODE_ROOT / name
        if not path.is_file():
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        for old in OLD_LOCATIONS:
            if old in text:
                offenders.append(f"{name}: {old}")
    assert offenders == []
