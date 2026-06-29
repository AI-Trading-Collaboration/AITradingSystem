from __future__ import annotations

import csv
import json
from pathlib import Path

from ai_trading_system.post_2085_research_common import (
    write_csv_rows,
    write_matrix_artifacts,
)


def test_write_csv_rows_creates_parent_and_normalizes_rows(tmp_path: Path) -> None:
    csv_path = tmp_path / "nested" / "rows.csv"

    write_csv_rows(
        csv_path,
        [
            {"candidate_id": "baseline_plus_trend_structure", "score": 0.42},
            {"candidate_id": "volatility_regime", "score": None},
        ],
    )

    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert rows == [
        {"candidate_id": "baseline_plus_trend_structure", "score": "0.42"},
        {"candidate_id": "volatility_regime", "score": ""},
    ]


def test_write_matrix_artifacts_writes_json_and_matching_csv(tmp_path: Path) -> None:
    json_path = tmp_path / "matrix" / "payload.json"
    csv_path = tmp_path / "matrix" / "payload.csv"

    write_matrix_artifacts(
        json_path,
        csv_path,
        {"report_type": "candidate_matrix", "status": "PASS"},
        [
            {"candidate_id": "risk_appetite", "row_count": 3},
            {"candidate_id": "volatility_regime", "row_count": 5},
        ],
    )

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        csv_rows = list(csv.DictReader(handle))

    assert payload["report_type"] == "candidate_matrix"
    assert payload["status"] == "PASS"
    assert payload["rows"] == [
        {"candidate_id": "risk_appetite", "row_count": 3},
        {"candidate_id": "volatility_regime", "row_count": 5},
    ]
    assert csv_rows == [
        {"candidate_id": "risk_appetite", "row_count": "3"},
        {"candidate_id": "volatility_regime", "row_count": "5"},
    ]
