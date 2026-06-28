from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


def build_norgate_breadth_prototype_schema_rows(
    snapshot_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Build summary-only 2Y breadth prototype rows.

    Numeric values are intentionally empty until live Norgate membership and
    trial price data are available. The schema itself is useful for validating
    downstream governance and report contracts without committing vendor data.
    """

    rows: list[dict[str, Any]] = []
    for row in snapshot_rows:
        rows.append(
            {
                "date": row.get("date", ""),
                "index_id": row.get("index_id", "$NDX"),
                "20d_positive_return_ratio": "",
                "60d_positive_return_ratio": "",
                "above_50d_ma_ratio": "",
                "above_200d_ma_ratio": "",
                "outperform_qqq_20d_ratio": "",
                "outperform_qqq_60d_ratio": "",
                "median_member_20d_return": "",
                "median_member_60d_return": "",
                "equal_weight_member_20d_return": "",
                "equal_weight_member_60d_return": "",
                "qqq_20d_return": "",
                "qqq_60d_return": "",
                "equal_weight_minus_qqq_20d": "",
                "equal_weight_minus_qqq_60d": "",
                "warning": "numeric prototype requires live Norgate trial data",
            }
        )
    return rows
