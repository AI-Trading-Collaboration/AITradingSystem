from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from dynamic_v3_research_helpers import (
    prepared_real_like_sweep,
    write_candidate_evidence,
    write_regime_price_cache,
)

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_POSITION_ADVISORY_CONFIG_PATH,
    build_observe_pool,
    build_shadow_shortlist,
    build_shadow_shortlist_monitoring_pack,
    run_candidate_clustering,
    run_position_advisory,
    run_regime_coverage,
)


def observe_pool_fixture(tmp_path: Path, *, top_n: int = 8) -> dict[str, Any]:
    sweep = prepared_real_like_sweep(tmp_path)
    evidence_dirs = write_candidate_evidence(tmp_path, sweep)
    run_regime_coverage(
        sweep_id=sweep["sweep_id"],
        sweep_output_dir=sweep["sweep_output_dir"],
        prices_path=write_regime_price_cache(tmp_path),
        output_dir=tmp_path / "regime_coverage",
    )
    pool = build_observe_pool(
        sweep_id=sweep["sweep_id"],
        top_n=top_n,
        sweep_output_dir=sweep["sweep_output_dir"],
        output_dir=tmp_path / "observe_pool",
        regime_coverage_dir=tmp_path / "regime_coverage",
        **evidence_dirs,
    )
    return {"sweep": sweep, "pool": pool}


def shortlist_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = observe_pool_fixture(tmp_path)
    shortlist = build_shadow_shortlist(
        observe_pool_id=fixture["pool"]["pool_id"],
        target_size=4,
        max_size=5,
        min_size=2,
        observe_pool_dir=tmp_path / "observe_pool",
        output_dir=tmp_path / "shortlist",
    )
    return {**fixture, "shortlist": shortlist}


def cluster_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = shortlist_fixture(tmp_path)
    cluster = run_candidate_clustering(
        shortlist_id=fixture["shortlist"]["shortlist_id"],
        shortlist_dir=tmp_path / "shortlist",
        output_dir=tmp_path / "candidate_cluster",
    )
    return {**fixture, "cluster": cluster}


def shadow_shortlist_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = cluster_fixture(tmp_path)
    shadow = build_shadow_shortlist_monitoring_pack(
        shortlist_id=fixture["shortlist"]["shortlist_id"],
        cluster_id=fixture["cluster"]["cluster_id"],
        shortlist_dir=tmp_path / "shortlist",
        cluster_dir=tmp_path / "candidate_cluster",
        output_dir=tmp_path / "shadow_shortlist",
    )
    return {**fixture, "shadow": shadow}


def position_advisory_config(tmp_path: Path) -> Path:
    raw = yaml.safe_load(DEFAULT_POSITION_ADVISORY_CONFIG_PATH.read_text(encoding="utf-8"))
    path = tmp_path / "position_advisory_v1.yaml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    return path


def position_advisory_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = shadow_shortlist_fixture(tmp_path)
    config_path = position_advisory_config(tmp_path)
    advisory = run_position_advisory(
        shadow_shortlist_id=fixture["shadow"]["shadow_shortlist_id"],
        config_path=config_path,
        shadow_shortlist_dir=tmp_path / "shadow_shortlist",
        output_dir=tmp_path / "position_advisory",
    )
    return {**fixture, "config_path": config_path, "advisory": advisory}
