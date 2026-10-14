"""Append-only global screening family and current Benjamini-Hochberg verdicts."""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from ai_trading_system.research_bench.ledger import RECORD_NAME, _now, _plain, chain_digest
from ai_trading_system.research_bench.screen import bh_adjust, screen_verdict
from ai_trading_system.research_bench.screen_settings import ScreenRules

SCREEN_SCHEMA = "research_screen_record.v1"
REQUIRED = {
    "signal_id",
    "module_sha256",
    "spec_sha256",
    "params",
    "markets",
    "data",
    "code",
    "settings_sha256",
    "results",
    "primary_p_value",
    "verdict",
    "family_size",
}
RESERVED = {"schema_version", "number", "screen_id", "recorded_at", "prev_sha256"}


class ScreenLedgerError(ValueError):
    """A broken chain or a repeat on the same data and settings."""


def _directory(root: Path) -> Path:
    return root / "research" / "_screens" / "ledger"


def _paths(root: Path) -> list[Path]:
    directory = _directory(root)
    if not directory.exists():
        return []
    return sorted(
        (p for p in directory.iterdir() if p.is_file() and RECORD_NAME.fullmatch(p.name)),
        key=lambda p: int(p.stem),
    )


def verify_screen_ledger(root: Path) -> list[str]:
    directory = _directory(root)
    if not directory.exists():
        return []
    if not directory.is_dir():
        return ["screen ledger is not a directory"]
    problems = [
        f"{p.name} is not a screen record"
        for p in sorted(directory.iterdir())
        if not (p.is_file() and RECORD_NAME.fullmatch(p.name))
    ]
    previous = None
    for position, path in enumerate(_paths(root), 1):
        data = path.read_bytes()
        if int(path.stem) != position:
            problems.append(f"{path.name}: expected record {position:04d}; a record is missing")
        try:
            record = json.loads(data)
        except ValueError:
            problems.append(f"{path.name}: not JSON")
            previous = chain_digest(data)
            continue
        if not isinstance(record, dict) or record.get("schema_version") != SCREEN_SCHEMA:
            problems.append(f"{path.name}: schema_version is not {SCREEN_SCHEMA}")
        else:
            if record.get("number") != int(path.stem):
                problems.append(f"{path.name}: number does not match file name")
            if record.get("screen_id") != f"S{int(path.stem):04d}":
                problems.append(f"{path.name}: screen_id does not match file name")
            if record.get("prev_sha256") != previous:
                problems.append(f"{path.name}: prev_sha256 does not match the record before it")
        previous = chain_digest(data)
    return problems


def screen_records(root: Path) -> list[dict[str, Any]]:
    problems = verify_screen_ledger(root)
    if problems:
        raise ScreenLedgerError("; ".join(problems))
    return [json.loads(p.read_bytes()) for p in _paths(root)]


def family_key(record: Mapping[str, Any]) -> str:
    return f"{record['module_sha256']}:{record['spec_sha256']}"


def check_screen_repeat(records: list[dict[str, Any]], body: Mapping[str, Any]) -> None:
    """Preflight before loading any snapshot or calculating a signal."""
    for record in records:
        if (
            family_key(record) == family_key(body)
            and record["data"] == body["data"]
            and record["settings_sha256"] == body["settings_sha256"]
        ):
            raise ScreenLedgerError(
                f"{record['screen_id']}: same signal, data and settings already screened"
            )


def append_screen_record(root: Path, body: dict[str, Any]) -> dict[str, Any]:
    missing, reserved = REQUIRED - body.keys(), RESERVED & body.keys()
    if missing or reserved:
        raise ValueError(f"missing keys {sorted(missing)}; reserved keys {sorted(reserved)}")
    if body["verdict"] not in {"candidate", "eliminated", "inconclusive"}:
        raise ValueError("invalid screen verdict")
    for key, fields in (
        ("data", {"signal_snapshot_id", "bench_snapshot_id"}),
        ("code", {"commit", "modified"}),
    ):
        if not isinstance(body[key], Mapping) or not fields <= body[key].keys():
            raise ValueError(f"{key} must contain {sorted(fields)}")
    p = body["primary_p_value"]
    if p is not None:
        bh_adjust([p])
    records = screen_records(root)
    check_screen_repeat(records, body)
    paths = _paths(root)
    number = len(records) + 1
    record = _plain(
        {
            "schema_version": SCREEN_SCHEMA,
            "number": number,
            "screen_id": f"S{number:04d}",
            "recorded_at": _now(),
            "prev_sha256": chain_digest(paths[-1].read_bytes()) if paths else None,
            **body,
        }
    )
    text = json.dumps(record, ensure_ascii=False, indent=1, sort_keys=True, allow_nan=False)
    directory = _directory(root)
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / f"{number:04d}.json").open("xb") as handle:
        handle.write(text.encode("utf-8") + b"\n")
    return record  # type: ignore[no-any-return]


def screen_ledger_summary(root: Path, settings: ScreenRules) -> dict[str, Any]:
    records = screen_records(root)
    counts = Counter(family_key(r) for r in records)
    latest = {family_key(r): r for r in records}
    tested = [key for key, r in latest.items() if r["primary_p_value"] is not None]
    adjusted = dict(
        zip(tested, bh_adjust([latest[k]["primary_p_value"] for k in tested]), strict=True)
    )
    members = []
    for key, record in latest.items():
        p = adjusted.get(key)
        inputs = record["results"].get("verdict_inputs")
        members.append(
            {
                "signal_id": record["signal_id"],
                "family_key": key,
                "number": record["number"],
                "records": counts[key],
                "primary_p_value": record["primary_p_value"],
                "adjusted_p": p,
                "bh_pass": p <= settings.bh_q if p is not None else None,
                "verdict_at_record": record["verdict"],
                "verdict": screen_verdict(**inputs, primary_adjusted_p=p, settings=settings)
                if inputs is not None
                else None,
            }
        )
    return {"family_size": len(latest), "tested": len(tested), "members": members}
