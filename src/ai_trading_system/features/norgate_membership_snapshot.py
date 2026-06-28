from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


def build_norgate_membership_snapshot_summary(
    membership_rows: Sequence[Mapping[str, Any]],
    *,
    price_join_available: bool,
) -> list[dict[str, Any]]:
    """Build summary-only date x index membership rows without raw symbols."""

    rows: list[dict[str, Any]] = []
    for row in membership_rows:
        query_success = bool(row.get("query_success"))
        rows.append(
            {
                "date": row.get("resolved_trading_date") or row.get("requested_date", ""),
                "index_id": row.get("index_id", "$NDX"),
                "member_count": int(row.get("member_count") or 0),
                "member_symbols_hash": str(row.get("member_symbols_hash") or ""),
                "active_member_count": int(row.get("member_count") or 0) if query_success else 0,
                "missing_price_count": 0 if query_success and price_join_available else "",
                "price_join_coverage_ratio": 1.0 if query_success and price_join_available else 0.0,
                "warning": str(row.get("warning") or ""),
            }
        )
    return rows
