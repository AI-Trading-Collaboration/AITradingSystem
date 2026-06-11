from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    build_manual_execution_review_pack,
    run_execution_guardrails_check,
    run_portfolio_exposure_validation,
    run_position_drift_analysis,
    write_manual_portfolio_snapshot_artifact,
)

RUN_DATE = "2026-06-11"


def manual_snapshot_payload(
    *,
    cash_weight: float = 0.20,
    qqq_weight: float = 0.50,
    smh_weight: float = 0.20,
    tlt_weight: float = 0.10,
    cash_currency: str = "USD",
    qqq_currency: str = "USD",
    smh_currency: str = "USD",
    tlt_currency: str = "USD",
) -> dict[str, Any]:
    total_equity = 100000.0
    return {
        "schema_version": 1,
        "snapshot": {
            "as_of": RUN_DATE,
            "source": "manual_owner_input",
            "broker_imported": False,
            "owner_reviewed": False,
            "base_currency": "USD",
            "total_equity": total_equity,
            "cash_symbol": "CASH",
        },
        "accounts": [
            {
                "account_id": "manual_primary",
                "account_type": "manual_snapshot",
                "currency": "USD",
                "total_equity": total_equity,
            }
        ],
        "cash": {
            "symbol": "CASH",
            "value": round(total_equity * cash_weight, 2),
            "weight": cash_weight,
            "currency": cash_currency,
        },
        "positions": [
            _position("QQQ", qqq_weight, total_equity, qqq_currency),
            _position("SMH", smh_weight, total_equity, smh_currency),
            _position("TLT", tlt_weight, total_equity, tlt_currency),
        ],
        "metadata": {
            "owner_notes": "Manual fixture for advisory review only.",
            "last_manual_update": RUN_DATE,
            "broker_action_allowed": False,
            "broker_action_taken": False,
        },
    }


def write_manual_snapshot(tmp_path: Path, payload: dict[str, Any] | None = None) -> Path:
    path = tmp_path / "current_portfolio_snapshot.yaml"
    path.write_text(
        yaml.safe_dump(payload or manual_snapshot_payload(), sort_keys=False),
        encoding="utf-8",
    )
    return path


def write_manual_snapshot_artifact(
    tmp_path: Path,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    snapshot_path = write_manual_snapshot(tmp_path, payload)
    return write_manual_portfolio_snapshot_artifact(
        snapshot_path=snapshot_path,
        output_dir=tmp_path / "manual_portfolio_snapshot",
    )


def write_shadow_shortlist(
    tmp_path: Path,
    candidate_weights: list[dict[str, float]],
) -> dict[str, Any]:
    shadow_id = "shadow-shortlist-fixture"
    shadow_dir = tmp_path / "shadow_shortlist" / shadow_id
    shadow_dir.mkdir(parents=True, exist_ok=True)
    candidates = []
    for index, weights in enumerate(candidate_weights, start=1):
        candidate_id = f"candidate-{index}"
        weight_dir = tmp_path / "weight_paths" / candidate_id
        weight_dir.mkdir(parents=True, exist_ok=True)
        weight_path = weight_dir / "daily_weights.csv"
        _write_daily_weights(weight_path, weights)
        candidates.append(
            {
                "candidate_id": candidate_id,
                "shortlist_rank": index,
                "cluster_id": "cluster-1",
                "cluster_label": "fixture",
                "source_weight_path_artifact": str(weight_path),
                "manual_review_required": True,
                "broker_action_allowed": False,
                "production_effect": "none",
            }
        )
    manifest = {
        "schema_version": 1,
        "report_type": "etf_dynamic_v3_shadow_shortlist_monitoring_manifest",
        "shadow_shortlist_id": shadow_id,
        "source_shortlist_id": "shortlist-fixture",
        "status": "PASS",
        "shadow_candidate_count": len(candidates),
        "broker_action_allowed": False,
        "production_effect": "none",
    }
    (shadow_dir / "shadow_shortlist_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    with (shadow_dir / "shadow_shortlist_candidates.jsonl").open(
        "w",
        encoding="utf-8",
    ) as handle:
        for row in candidates:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    return {"shadow_shortlist_id": shadow_id, "shadow_dir": shadow_dir}


def manual_review_pack_fixture(
    tmp_path: Path,
    *,
    candidate_weights: list[dict[str, float]],
    snapshot_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    snapshot = write_manual_snapshot_artifact(tmp_path, snapshot_payload)
    exposure = run_portfolio_exposure_validation(
        snapshot_id=snapshot["snapshot_id"],
        snapshot_dir=tmp_path / "manual_portfolio_snapshot",
        output_dir=tmp_path / "portfolio_exposure",
    )
    shadow = write_shadow_shortlist(tmp_path, candidate_weights)
    drift = run_position_drift_analysis(
        snapshot_id=snapshot["snapshot_id"],
        shadow_shortlist_id=shadow["shadow_shortlist_id"],
        snapshot_dir=tmp_path / "manual_portfolio_snapshot",
        shadow_shortlist_dir=tmp_path / "shadow_shortlist",
        output_dir=tmp_path / "position_drift",
    )
    guardrail = run_execution_guardrails_check(
        drift_id=drift["drift_id"],
        exposure_id=exposure["exposure_id"],
        drift_dir=tmp_path / "position_drift",
        exposure_dir=tmp_path / "portfolio_exposure",
        output_dir=tmp_path / "execution_guardrails",
    )
    review = build_manual_execution_review_pack(
        snapshot_id=snapshot["snapshot_id"],
        exposure_id=exposure["exposure_id"],
        drift_id=drift["drift_id"],
        guardrail_id=guardrail["guardrail_id"],
        snapshot_dir=tmp_path / "manual_portfolio_snapshot",
        exposure_dir=tmp_path / "portfolio_exposure",
        drift_dir=tmp_path / "position_drift",
        guardrail_dir=tmp_path / "execution_guardrails",
        output_dir=tmp_path / "manual_execution_review",
    )
    return {
        "snapshot": snapshot,
        "exposure": exposure,
        "shadow": shadow,
        "drift": drift,
        "guardrail": guardrail,
        "review": review,
    }


def report_index_for_manual_review(fixture: dict[str, Any]) -> dict[str, Any]:
    paths = {
        "etf_dynamic_v3_manual_portfolio_snapshot": (
            fixture["snapshot"]["snapshot_dir"] / "manual_portfolio_manifest.json"
        ),
        "etf_dynamic_v3_portfolio_exposure": (
            fixture["exposure"]["exposure_dir"] / "portfolio_exposure_manifest.json"
        ),
        "etf_dynamic_v3_position_drift": (
            fixture["drift"]["drift_dir"] / "position_drift_manifest.json"
        ),
        "etf_dynamic_v3_execution_guardrails": (
            fixture["guardrail"]["guardrail_dir"] / "guardrail_manifest.json"
        ),
        "etf_dynamic_v3_manual_execution_review": (
            fixture["review"]["review_dir"] / "manual_execution_review_manifest.json"
        ),
    }
    return {
        "reports": [
            {
                "report_id": report_id,
                "latest_artifact_path": str(path),
                "status": "FRESH",
                "freshness_status": "FRESH",
            }
            for report_id, path in paths.items()
        ]
    }


def consensus_candidate_weights() -> list[dict[str, float]]:
    return [
        {"QQQ": 0.43, "SMH": 0.20, "SPY": 0.25, "SOXX": 0.05, "TLT": 0.0, "CASH": 0.07},
        {"QQQ": 0.431, "SMH": 0.201, "SPY": 0.249, "SOXX": 0.049, "TLT": 0.0, "CASH": 0.07},
    ]


def high_disagreement_candidate_weights() -> list[dict[str, float]]:
    return [
        {"QQQ": 0.90, "SMH": 0.0, "SPY": 0.0, "SOXX": 0.0, "TLT": 0.0, "CASH": 0.10},
        {"QQQ": 0.0, "SMH": 0.0, "SPY": 0.0, "SOXX": 0.0, "TLT": 0.8, "CASH": 0.20},
    ]


def _position(symbol: str, weight: float, total_equity: float, currency: str) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "asset_type": "ETF",
        "quantity": None,
        "market_price": None,
        "value": round(total_equity * weight, 2),
        "weight": weight,
        "currency": currency,
        "account_id": "manual_primary",
    }


def _write_daily_weights(path: Path, weights: dict[str, float]) -> None:
    fields = ["date", "SPY", "QQQ", "SMH", "SOXX", "TLT", "CASH"]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerow({"date": RUN_DATE, **weights})
