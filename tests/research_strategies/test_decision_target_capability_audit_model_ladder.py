from __future__ import annotations

import copy
import json
import math
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.contracts import CanonicalStatus
from ai_trading_system.platform.artifacts import sha256_path, write_json_atomic
from ai_trading_system.research_framework import (
    ExperimentRunRequest,
    resolve_experiment_spec,
    run_experiment,
)
from ai_trading_system.research_framework.plugins.decision_target_capability_audit_label_foundation import (  # noqa: E501
    READY_STATUS as LABEL_READY_STATUS,
)
from ai_trading_system.research_framework.plugins.decision_target_capability_audit_label_foundation import (  # noqa: E501
    build_decision_target_source_package,
    build_label_payload,
)
from ai_trading_system.research_framework.plugins.decision_target_capability_audit_model_ladder import (  # noqa: E501
    BLOCKED_STATUS,
    READY_STATUS,
    build_capability_payload,
    capture_input_snapshot,
    decision_target_capability_model_ladder_registry,
    render_capability_markdown,
    validate_capability_payload,
)
from ai_trading_system.trading_calendar import is_us_equity_trading_day
from ai_trading_system.yaml_loader import safe_load_yaml_path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SPEC_PATH = (
    PROJECT_ROOT / "config/research/experiments/decision_target_capability_audit_model_ladder.yaml"
)
AUDIT_POLICY_PATH = (
    PROJECT_ROOT / "config/research/decision_target_capability_audit_model_ladder_v1.yaml"
)
LABEL_POLICY_PATH = (
    PROJECT_ROOT / "config/research/decision_target_capability_audit_label_foundation_v2.yaml"
)
DATA_QUALITY_POLICY_PATH = PROJECT_ROOT / "config/data_quality.yaml"
CAPABILITY_POLICY_PATH = (
    PROJECT_ROOT / "config/data_quality/decision_target_label_core_capability_v1.yaml"
)
AS_OF = date(2026, 7, 24)
GENERATED_AT = datetime(2026, 7, 26, 12, 0, tzinfo=UTC)
TASK_ID = "TRADING-2461_DECISION_TARGET_CAPABILITY_AUDIT_MODEL_LADDER"


def test_model_ladder_is_deterministic_purged_and_content_derived(
    tmp_path: Path,
) -> None:
    sources, _ = _sources(tmp_path)

    first = build_capability_payload(sources, as_of=AS_OF)
    second = build_capability_payload(copy.deepcopy(sources), as_of=AS_OF)

    assert first == second
    assert first["status"] == READY_STATUS
    assert first["candidate_family_created"] is False
    assert first["strategy_backtest_executed"] is False
    assert first["target_weights_generated"] is False
    assert first["qld_used_as_signal"] is False
    assert len(first["evaluation"]["fold_ledger"]) >= 6
    assert first["capability_summary"]["prediction_row_count"] > 0
    assert first["style_classification"]["candidate_family_creation_authorized"] is False
    assert validate_capability_payload(first, sources, as_of=AS_OF) == ()

    for row in first["evaluation"]["fold_metrics"]:
        assert row["train_row_count"] <= row["candidate_train_row_count"]
        assert row["test_start"] > first["evaluation"]["evaluated_range"]["start"]
        if row["valid"]:
            assert row["train_row_count"] >= 252
            assert row["test_row_count"] >= 20

    tampered = copy.deepcopy(first)
    tampered["evaluation"]["predictions"][0]["prediction"] = 99.0
    assert validate_capability_payload(tampered, sources, as_of=AS_OF) == (
        "CAPABILITY_EVALUATION_MISMATCH",
    )


def test_input_and_label_row_order_tamper_fail_closed(tmp_path: Path) -> None:
    sources, paths = _sources(tmp_path)
    snapshot = copy.deepcopy(sources["input_snapshot"])
    panel_record = snapshot["records"]["market_panel"]
    panel_path = Path(panel_record["path"])
    with panel_path.open("a", encoding="utf-8", newline="") as handle:
        handle.write("\n")

    blocked = build_capability_payload(
        {**sources, "input_snapshot": snapshot},
        as_of=AS_OF,
    )
    assert blocked["status"] == BLOCKED_STATUS
    assert blocked["strict_validation_errors"] == ["INPUT_MARKET_PANEL_COMMITMENT_MISMATCH"]
    assert blocked["evaluation"] is None

    sources, paths = _sources(tmp_path / "row-order")
    snapshot = copy.deepcopy(sources["input_snapshot"])
    policy = copy.deepcopy(sources["audit_policy"])
    label_path = Path(snapshot["records"]["label_payload"]["path"])
    label = json.loads(label_path.read_text(encoding="utf-8"))
    rows = label["evaluation"]["label_rows"]
    rows[0], rows[1] = rows[1], rows[0]
    write_json_atomic(label_path, label, trailing_newline=False)
    _refresh_record(snapshot, policy, "label_payload")

    row_order_blocked = build_capability_payload(
        {
            "audit_policy": policy,
            "input_snapshot": snapshot,
            "requirement_text": TASK_ID,
        },
        as_of=AS_OF,
    )
    assert row_order_blocked["status"] == BLOCKED_STATUS
    assert row_order_blocked["strict_validation_errors"] == ["MODEL_LADDER_BUILD_FAILED"]
    assert paths["requirement_text"].is_file()


def test_generic_runner_writes_non_actionable_pass_artifacts(tmp_path: Path) -> None:
    _, paths = _sources(tmp_path)
    result = run_experiment(
        resolved_spec=resolve_experiment_spec(SPEC_PATH),
        plugins=decision_target_capability_model_ladder_registry(),
        request=ExperimentRunRequest(
            project_root=PROJECT_ROOT,
            output_root=tmp_path / "outputs",
            docs_root=tmp_path / "docs",
            as_of=AS_OF,
            input_overrides=paths,
            strict=True,
            generated_at=GENERATED_AT,
        ),
    )

    assert result.payload["status"] == READY_STATUS
    assert result.envelope.status is CanonicalStatus.PASS
    assert result.envelope.investment_facing is False
    assert result.envelope.data_quality is not None
    assert result.envelope.data_quality.ready is True
    assert result.ledger.entry("evaluate_and_render").status is CanonicalStatus.PASS
    assert result.output_paths["reader_markdown"].read_text(encoding="utf-8") == (
        render_capability_markdown(result.payload)
    )
    assert (
        validate_capability_payload(
            result.payload,
            {
                "audit_policy": safe_load_yaml_path(paths["audit_policy"]),
                "input_snapshot": json.loads(paths["input_snapshot"].read_text(encoding="utf-8")),
                "requirement_text": paths["requirement_text"].read_text(encoding="utf-8"),
            },
            as_of=AS_OF,
        )
        == ()
    )


def test_experiment_contract_is_manual_research_only() -> None:
    spec = resolve_experiment_spec(SPEC_PATH).value

    assert spec.data_quality_required is True
    assert spec.investment_facing_envelope is False
    assert spec.production_effect.value == "none"
    assert spec.broker_action == "none"
    assert spec.canonical_status(READY_STATUS) is CanonicalStatus.PASS
    assert spec.canonical_status(BLOCKED_STATUS) is CanonicalStatus.BLOCKED


def _sources(tmp_path: Path) -> tuple[dict[str, Any], dict[str, Path]]:
    label_policy = safe_load_yaml_path(LABEL_POLICY_PATH)
    prices_path, rates_path = _write_market_sources(tmp_path, prices=_prices())
    label_source_root = tmp_path / "label-source"
    package = build_decision_target_source_package(
        policy=label_policy,
        prices_path=prices_path,
        rates_path=rates_path,
        output_root=label_source_root,
        as_of=AS_OF,
        expected_price_tickers=["QQQ", "SPY", "SGOV"],
        expected_rate_series=["DGS3MO"],
        captured_at=GENERATED_AT,
        capability_policy_path=CAPABILITY_POLICY_PATH,
        data_quality_policy_path=DATA_QUALITY_POLICY_PATH,
    )
    stored_package = json.loads(
        (label_source_root / "market_panel_package.json").read_text(encoding="utf-8")
    )
    label_sources = {
        "label_policy": label_policy,
        "market_panel_package": stored_package,
        "data_quality_policy": safe_load_yaml_path(DATA_QUALITY_POLICY_PATH),
        "requirement_text": "research-only; no model or weights",
    }
    label_payload = build_label_payload(label_sources, as_of=AS_OF)
    assert label_payload["status"] == LABEL_READY_STATUS
    label_payload["as_of"] = AS_OF.isoformat()
    label_payload["generated_at"] = GENERATED_AT.isoformat()
    label_path = tmp_path / "batch1-label-payload.json"
    write_json_atomic(label_path, label_payload, trailing_newline=False)

    snapshot_root = tmp_path / "snapshot"
    capture_input_snapshot(
        label_payload_path=label_path,
        source_package_path=label_source_root / "market_panel_package.json",
        market_panel_path=Path(str(package["panel"]["path"])),
        capability_receipt_path=Path(str(package["capability_receipt"]["path"])),
        output_root=snapshot_root,
        captured_at=GENERATED_AT,
    )
    snapshot_path = snapshot_root / "input_snapshot.json"
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    audit_policy = copy.deepcopy(safe_load_yaml_path(AUDIT_POLICY_PATH))
    authority = audit_policy["input_authority"]
    authority["label_row_count"] = len(label_payload["evaluation"]["label_rows"])
    authority["common_session_count"] = pd.read_csv(Path(str(package["panel"]["path"])))[
        "date"
    ].nunique()
    receipt = json.loads(
        Path(str(package["capability_receipt"]["path"])).read_text(encoding="utf-8")
    )
    authority["capability_receipt_id"] = receipt["receipt_id"]
    authority["full_canonical_status"] = receipt["full_quality"]["status"]
    authority["scoped_status"] = receipt["scoped_quality"]["status"]
    authority["global_cache_pass_claimed"] = receipt["global_cache_pass_claimed"]
    for role in ("label_payload", "source_package", "market_panel", "capability_receipt"):
        record = snapshot["records"][role]
        authority["expected_files"][role] = {
            "sha256": record["sha256"],
            "size_bytes": record["size_bytes"],
        }
    audit_policy_path = tmp_path / "audit-policy.json"
    write_json_atomic(audit_policy_path, audit_policy)
    requirement_path = tmp_path / "requirement.md"
    requirement_path.write_text(f"{TASK_ID}; research-only\n", encoding="utf-8")
    sources: dict[str, Any] = {
        "audit_policy": audit_policy,
        "input_snapshot": snapshot,
        "requirement_text": requirement_path.read_text(encoding="utf-8"),
    }
    paths = {
        "audit_policy": audit_policy_path,
        "input_snapshot": snapshot_path,
        "requirement_text": requirement_path,
    }
    return sources, paths


def _refresh_record(
    snapshot: dict[str, Any],
    policy: dict[str, Any],
    role: str,
) -> None:
    records = snapshot["records"]
    assert isinstance(records, dict)
    record = records[role]
    assert isinstance(record, dict)
    path = Path(str(record["path"]))
    record["sha256"] = sha256_path(path)
    record["size_bytes"] = path.stat().st_size
    authority = policy["input_authority"]
    assert isinstance(authority, dict)
    expected = authority["expected_files"]
    assert isinstance(expected, dict)
    expected[role] = {
        "sha256": record["sha256"],
        "size_bytes": record["size_bytes"],
    }


def _write_market_sources(
    root: Path,
    *,
    prices: pd.DataFrame,
) -> tuple[Path, Path]:
    root.mkdir(parents=True, exist_ok=True)
    prices_path = root / "prices_daily.csv"
    prices.to_csv(prices_path, index=False, lineterminator="\n")
    rates_path = root / "rates_daily.csv"
    dates = sorted(prices["date"].unique())
    pd.DataFrame(
        {
            "date": dates,
            "series": "DGS3MO",
            "value": 5.0,
            "source": "test_fixture",
        }
    ).to_csv(rates_path, index=False, lineterminator="\n")
    return prices_path, rates_path


def _prices() -> pd.DataFrame:
    dates = pd.DatetimeIndex(
        [
            value
            for value in pd.date_range("2021-02-22", AS_OF.isoformat(), freq="D")
            if is_us_equity_trading_day(value.date())
        ]
    )
    returns = {
        "QQQ": [
            (
                0.0
                if index == 0
                else 0.0004 + 0.005 * math.sin(index / 17.0) + 0.0015 * math.sin(index / 5.0)
            )
            for index in range(len(dates))
        ],
        "SPY": [
            0.0 if index == 0 else 0.0003 + 0.003 * math.sin(index / 17.0)
            for index in range(len(dates))
        ],
        "SGOV": [0.0 if index == 0 else 0.00008 for index in range(len(dates))],
    }
    rows: list[dict[str, object]] = []
    for ticker in ("QQQ", "SPY", "SGOV"):
        price = 100.0
        for session, asset_return in zip(dates, returns[ticker], strict=True):
            price *= 1.0 + asset_return
            rows.append(
                {
                    "date": session.date().isoformat(),
                    "ticker": ticker,
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "adj_close": price,
                    "volume": 1_000_000,
                    "source": "test_fixture",
                }
            )
    return pd.DataFrame(rows)
