"""Deterministic outcome/probability evaluation; no I/O except reviewed config loading."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from math import fsum, sqrt
from pathlib import Path
from statistics import NormalDist
from typing import Any, Self

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ai_trading_system.research_judgments.schema import effective_prob
from ai_trading_system.research_judgments.settle import DEFAULT_EVAL_PATH, settle_ledger
from ai_trading_system.yaml_loader import safe_load_yaml_path

FORMAT = "judgment_report.v1"
# Serialization precision and the uninformative binary forecast are protocol/formula invariants.
ROUND = 10
COIN = 0.5
COIN_BRIER = COIN * COIN


class ReportParams(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    confidence: float = Field(gt=0, lt=1)
    power: float = Field(gt=0, lt=1)
    min_events: int = Field(ge=1)
    abandon_events: int = Field(ge=1)
    edge_brier: float = Field(gt=0, allow_inf_nan=False)
    min_events_for_mde: int = Field(ge=2)
    min_clusters: int = Field(ge=2)
    bh_q: float = Field(gt=0, lt=1)
    calibration_bins: tuple[float, ...]

    @field_validator("calibration_bins")
    @classmethod
    def valid_bins(cls, value: tuple[float, ...]) -> tuple[float, ...]:
        if (
            len(value) < 2
            or value[0] != COIN
            or value[-1] != 1
            or not all(a < b for a, b in zip(value[:-1], value[1:], strict=True))
        ):
            raise ValueError("invalid calibration bins")
        return value

    @model_validator(mode="after")
    def ordered_event_limits(self) -> Self:
        if self.abandon_events < self.min_events:
            raise ValueError("abandon_events must not precede min_events")
        return self


class ReportConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    schema_version: int
    report: ReportParams


def load_report_params(path: Path = DEFAULT_EVAL_PATH) -> ReportParams:
    try:
        raw = safe_load_yaml_path(path)
    except (OSError, yaml.YAMLError, RecursionError) as exc:
        raise ValueError("invalid report configuration") from exc
    return ReportConfig.model_validate(raw).report


def earnings_quarter(day: str) -> str:
    parsed = date.fromisoformat(day)
    return f"{parsed.year}Q{(parsed.month - 1) // 3 + 1}"


def wilson(successes: float, n: int, confidence: float) -> tuple[float, float]:
    z = NormalDist().inv_cdf((1 + confidence) / 2)
    phat = successes / n
    denom = 1 + z * z / n
    center = (phat + z * z / (2 * n)) / denom
    half = z * sqrt(phat * (1 - phat) / n + z * z / (4 * n * n)) / denom
    return max(0, center - half), min(1, center + half)


def cluster_stats(values: Sequence[float], clusters: Sequence[str]) -> dict[str, Any]:
    mean = fsum(values) / len(values)
    residuals: dict[str, list[float]] = defaultdict(list)
    for value, cluster in zip(values, clusters, strict=True):
        residuals[cluster].append(value - mean)
    g = len(residuals)
    se = (
        None
        if g < 2
        else sqrt(g / (g - 1) * fsum(fsum(u) ** 2 for u in residuals.values()) / len(values) ** 2)
    )
    return {"mean": mean, "se": se, "clusters": g}


def one_sided_p(mean: float, se: float | None) -> float | None:
    if se is None:
        return None
    if se == 0:
        return 0.0 if mean > 0 else 1.0
    return 1 - NormalDist().cdf(mean / se)


def benjamini_hochberg(p_values: dict[str, float | None], q: float) -> set[str]:
    ordered = sorted(
        p_values, key=lambda name: (p_values[name] if p_values[name] is not None else 1.0, name)
    )
    k = 0
    for rank, name in enumerate(ordered, 1):
        p = p_values[name]
        if p is not None and p <= rank / len(ordered) * q:
            k = rank
    return set(ordered[:k])


def classify(stats: dict[str, Any], params: ReportParams, bh_pass: bool) -> tuple[str, str]:
    if stats["n_events"] < params.min_events:
        return "insufficient", "events_below_minimum"
    if stats["mde"] is None:
        return "insufficient", "mde_unavailable"
    if stats["mde"] > params.edge_brier:
        return "insufficient", "mde_above_edge"
    if stats["n_events"] >= params.abandon_events and stats["lift_upper"] < params.edge_brier:
        return "abandon", "edge_excluded"
    if (
        stats["lift_mean"] >= params.edge_brier
        and stats["lift_lower"] > 0
        and stats["lift_first_half"] is not None
        and stats["lift_second_half"] is not None
        and stats["lift_first_half"] > 0
        and stats["lift_second_half"] > 0
        and bh_pass
    ):
        return "promising", "edge_reached"
    return "insufficient", "keep_collecting"


@dataclass(frozen=True)
class _Judgment:
    id: str
    event: tuple[str, str]
    day: str
    p: float
    q: float
    y: float
    methods: tuple[str, ...]
    ticker: str
    aiMode: str
    sawSystem: str


def _judgment(row: dict[str, Any], outcome: float) -> _Judgment:
    event_id = row.get("eventId")
    # Tag explicit and derived keys so even identical spellings cannot merge the two namespaces.
    event = (
        ("explicit", event_id) if event_id else ("derived", f"{row['ticker']}@{row['deadline']}")
    )
    saw = row.get("sawSystem")
    return _Judgment(
        row["id"],
        event,
        row["deadline"],
        effective_prob(row),
        float(row["prob"]),
        float(outcome),
        tuple(row["methods"]),
        row["ticker"],
        row["aiMode"],
        "unknown" if saw is None else "yes" if saw else "no",
    )


def _mean(values: Sequence[float]) -> float:
    return fsum(values) / len(values)


def _group(
    judgments: list[_Judgment], params: ReportParams, *, lifts: bool = True
) -> dict[str, Any]:
    by_event: dict[tuple[str, str], list[_Judgment]] = defaultdict(list)
    for judgment in judgments:
        by_event[judgment.event].append(judgment)
    events: list[dict[str, Any]] = []
    for key, members in by_event.items():
        events.append(
            {
                "key": key,
                "day": min(j.day for j in members),
                "members": members,
                "y": _mean([j.y for j in members]),
                "p": _mean([j.p for j in members]),
                "q": _mean([j.q for j in members]),
                "bp": _mean([(j.p - j.y) ** 2 for j in members]),
                "bq": _mean([(j.q - j.y) ** 2 for j in members]),
            }
        )
    events.sort(key=lambda e: (e["day"], e["key"]))
    n = len(events)
    clusters = [earnings_quarter(e["day"]) for e in events]
    low, high = wilson(fsum(e["y"] for e in events), n, params.confidence)
    stats: dict[str, Any] = {
        "n_judgments": len(judgments),
        "n_events": n,
        "n_clusters": len(set(clusters)),
        "hit_rate": _mean([e["y"] for e in events]),
        "hit_wilson_low": low,
        "hit_wilson_high": high,
        "brier_scored": _mean([e["bp"] for e in events]),
        "brier_original": _mean([e["bq"] for e in events]),
        "brier_coin": COIN_BRIER,
        "update_gain": _mean([e["bq"] - e["bp"] for e in events]),
        "n_updated": sum(any(j.q != j.p for j in e["members"]) for e in events),
        "n_flipped": sum((j.q - COIN) * (j.p - COIN) < 0 for j in judgments),
    }
    if not lifts:
        return stats
    baseline: list[float] = []
    if n >= 2:
        for i, event in enumerate(events):
            rate = fsum(e["y"] for k, e in enumerate(events) if k != i) / (n - 1)
            baseline.append(_mean([(rate - j.y) ** 2 for j in event["members"]]))
    stats["brier_baseline"] = _mean(baseline) if baseline else None
    stats["lift_coin_mean"] = _mean([COIN_BRIER - e["bp"] for e in events])
    for field in (
        "lift_mean",
        "lift_se",
        "lift_lower",
        "lift_upper",
        "lift_p",
        "lift_first_half",
        "lift_second_half",
        "mde",
    ):
        stats[field] = None
    if baseline:
        differences = [bb - e["bp"] for bb, e in zip(baseline, events, strict=True)]
        clustered = cluster_stats(differences, clusters)
        mean, se = clustered["mean"], clustered["se"]
        stats["lift_mean"] = mean
        h = n // 2
        stats["lift_first_half"] = _mean(differences[:h]) if h else None
        stats["lift_second_half"] = _mean(differences[h:])
        if se is not None:
            z = NormalDist().inv_cdf(params.confidence)
            stats.update(
                lift_se=se,
                lift_lower=mean - z * se,
                lift_upper=mean + z * se,
                lift_p=one_sided_p(mean, se),
            )
            if n >= params.min_events_for_mde and stats["n_clusters"] >= params.min_clusters:
                stats["mde"] = (z + NormalDist().inv_cdf(params.power)) * se
    return stats


def _calibration(judgments: list[_Judgment], params: ReportParams) -> list[dict[str, Any]]:
    folded = [(1 - j.p, 1 - j.y) if j.p < COIN else (j.p, j.y) for j in judgments]
    table = []
    bins = params.calibration_bins
    for low, high in zip(bins[:-1], bins[1:], strict=True):
        members = [
            (p, y) for p, y in folded if low <= p and (p < high or high == bins[-1] and p <= high)
        ]
        row: dict[str, Any] = {
            "low": low,
            "high": high,
            "n": len(members),
            "mean_p": None,
            "hit_rate": None,
            "wilson_low": None,
            "wilson_high": None,
        }
        if members:
            successes = fsum(y for _, y in members)
            wl, wh = wilson(successes, len(members), params.confidence)
            row.update(
                mean_p=_mean([p for p, _ in members]),
                hit_rate=successes / len(members),
                wilson_low=wl,
                wilson_high=wh,
            )
        table.append(row)
    return table


def _rounded(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, ROUND)
    if isinstance(value, dict):
        return {key: _rounded(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_rounded(item) for item in value]
    return value


def build_report(
    rows: list[dict[str, Any]], outcomes: list[dict[str, Any]], params: ReportParams, asof: date
) -> dict[str, Any]:
    by_id = {row["id"]: row for row in rows}
    research = sorted(
        [
            _judgment(by_id[o["id"]], o["outcome"])
            for o in outcomes
            if o["state"] == "settled" and by_id[o["id"]]["purpose"] == "research"
        ],
        key=lambda j: j.id,
    )
    learning = []
    for row in sorted(rows, key=lambda r: r["id"]):
        if row["purpose"] == "learning":
            result = settle_ledger(row, asof)
            if result.state == "settled":
                assert result.outcome is not None
                learning.append(_judgment(row, result.outcome))
    method_groups: dict[str, list[_Judgment]] = defaultdict(list)
    for judgment in research:
        for method_id in judgment.methods:
            method_groups[method_id].append(judgment)
    methods = [
        {"method": name, **_group(group, params), "calibration": _calibration(group, params)}
        for name, group in sorted(method_groups.items())
    ]
    passing = benjamini_hochberg(
        {m["method"]: m["lift_p"] if m["n_events"] >= params.min_events else None for m in methods},
        params.bh_q,
    )
    for method in methods:
        method["status"], method["reason"] = classify(method, params, method["method"] in passing)
    doc: dict[str, Any] = {
        "format": FORMAT,
        "asof": asof.isoformat(),
        "counts": {
            state: sum(o["state"] == state for o in outcomes)
            for state in ("settled", "pending", "void", "data_gap")
        },
        "bh": {"m": len(methods), "q": params.bh_q},
        "methods": methods,
        "research": _group(research, params, lifts=False) if research else None,
        "calibration": _calibration(research, params),
        "learning": _group(learning, params, lifts=False) if learning else None,
        "all_settled": _group(sorted(research + learning, key=lambda j: j.id), params, lifts=False)
        if research or learning
        else None,
    }
    for key, field in (
        ("by_ticker", "ticker"),
        ("by_ai_mode", "aiMode"),
        ("by_saw_system", "sawSystem"),
    ):
        groups: dict[str, list[_Judgment]] = defaultdict(list)
        for judgment in research:
            groups[getattr(judgment, field)].append(judgment)
        doc[key] = [
            {field: name, **_group(group, params)} for name, group in sorted(groups.items())
        ]
    rounded: dict[str, Any] = _rounded(doc)
    return rounded
