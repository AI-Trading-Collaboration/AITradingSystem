from datetime import timedelta

import pandas as pd

EVENT_GROUPS = {
    "fomc": ("fomc",),
    "cpi": ("cpi", "inflation"),
    "pce": ("pce",),
    "payrolls": ("employment", "payroll", "labor"),
    "gdp": ("gdp", "growth"),
}


def build_macro_event_calendar_free_features(
    events: pd.DataFrame,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    if events.empty:
        return _empty_frame()
    if "event_date" not in events.columns or "event_type" not in events.columns:
        raise ValueError("macro calendar events must include event_date and event_type")

    frame = events.copy()
    frame["event_date"] = pd.to_datetime(frame["event_date"], errors="coerce")
    frame = frame.loc[frame["event_date"].notna()].copy()
    if frame.empty:
        return _empty_frame()
    start = pd.Timestamp(start_date) if start_date else frame["event_date"].min()
    end = pd.Timestamp(end_date) if end_date else frame["event_date"].max()
    dates = pd.date_range(start=start, end=end, freq="D")
    out = pd.DataFrame({"date": dates})
    lower_types = frame["event_type"].astype(str).str.lower()
    for group, patterns in EVENT_GROUPS.items():
        bound_patterns = patterns
        group_dates = frame.loc[
            lower_types.apply(
                lambda value, local_patterns=bound_patterns: any(
                    pattern in value for pattern in local_patterns
                )
            ),
            "event_date",
        ].sort_values()
        bound_group_dates = group_dates.copy()
        out[f"is_{group}_day"] = out["date"].isin(set(group_dates))
        out[f"days_to_{group}"] = out["date"].apply(
            lambda value, dates=bound_group_dates: _days_to(value, dates)
        )
        out[f"days_since_{group}"] = out["date"].apply(
            lambda value, dates=bound_group_dates: _days_since(value, dates)
        )
    event_counts = frame.groupby("event_date").size().rename("event_count").reset_index()
    out = out.merge(event_counts, left_on="date", right_on="event_date", how="left")
    out["event_count"] = out["event_count"].fillna(0).astype(int)
    out["is_major_macro_release_day"] = out["event_count"] > 0
    out["event_cluster_score"] = out["date"].apply(lambda value: _cluster_score(value, frame))
    out["feature_family"] = "macro_event_calendar_free_v1"
    out["known_at"] = out["date"].dt.strftime("%Y-%m-%d")
    out["available_at"] = out["known_at"]
    out["decision_at"] = out["known_at"]
    out["PIT_status"] = (
        "PIT_WARNING_DIAGNOSTIC_ONLY"
        if (frame.get("PIT_status", pd.Series(dtype=str)).astype(str) == "PIT_WARNING").any()
        else "PIT_APPROVED"
    )
    out["allowed_usage"] = (
        "diagnostic_only"
        if (out["PIT_status"] == "PIT_WARNING_DIAGNOSTIC_ONLY").any()
        else "event_risk_diagnostic"
    )
    out["blocked_usage"] = "promotion,paper_shadow,production,broker"
    out = out.drop(columns=["event_date"], errors="ignore")
    out["date"] = out["date"].dt.strftime("%Y-%m-%d")
    return out.sort_values("date").reset_index(drop=True)


def _days_to(date_value: pd.Timestamp, event_dates: pd.Series) -> int | None:
    future = event_dates.loc[event_dates >= date_value]
    if future.empty:
        return None
    return int((future.iloc[0] - date_value).days)


def _days_since(date_value: pd.Timestamp, event_dates: pd.Series) -> int | None:
    past = event_dates.loc[event_dates <= date_value]
    if past.empty:
        return None
    return int((date_value - past.iloc[-1]).days)


def _cluster_score(date_value: pd.Timestamp, frame: pd.DataFrame) -> int:
    start = date_value - timedelta(days=2)
    end = date_value + timedelta(days=2)
    return int(((frame["event_date"] >= start) & (frame["event_date"] <= end)).sum())


def _empty_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "date",
            "feature_family",
            "known_at",
            "available_at",
            "decision_at",
            "PIT_status",
            "allowed_usage",
            "blocked_usage",
        ]
    )
