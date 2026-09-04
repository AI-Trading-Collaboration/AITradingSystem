from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import subprocess
import tempfile
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

SCHEMA_VERSION = "equal_risk_qqq_sgov_catchup_run_authorization.v1"
TASK_ID = "TRADING-2563_EQUAL_RISK_CATCHUP_V1"
AUTHORIZED_COUNTS = {
    "manifest_replays": 1,
    "canonical_dq_runs": 1,
    "isolated_bounded_rehearsals": 1,
    "canonical_maturity_updates": 1,
    "scoreboard_generations": 1,
    "continuity_audits": 1,
}
ZERO_COUNTS = {
    "new_observations",
    "data_downloads",
    "cache_mutations",
    "provider_actions",
    "quantconnect_actions",
    "option_actions",
    "paper_actions",
    "live_actions",
    "broker_actions",
    "orders",
    "fills",
    "positions",
    "trading_actions",
}


class ManifestReplayError(ValueError):
    pass


def sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_manifest(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ManifestReplayError("manifest must be a mapping")
    return payload


def validate_manifest_shape(payload: Mapping[str, Any], *, require_frozen: bool) -> None:
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise ManifestReplayError("unexpected schema_version")
    if payload.get("task_id") != TASK_ID:
        raise ManifestReplayError("unexpected task_id")
    owner = _mapping(payload.get("owner_decision"), "owner_decision")
    if owner.get("authorization_state") != "EXACT_PREAUTHORIZED":
        raise ManifestReplayError("authorization_state must be EXACT_PREAUTHORIZED")
    scope = _mapping(payload.get("execution_scope"), "execution_scope")
    if scope.get("as_of") != "2026-09-03":
        raise ManifestReplayError("as_of drift")
    if scope.get("observation_decision_dates") != ["2026-06-22", "2026-06-24"]:
        raise ManifestReplayError("observation target drift")
    if scope.get("new_observation_allowed") is not False:
        raise ManifestReplayError("new observation must remain forbidden")
    envelope = _mapping(payload.get("run_envelope"), "run_envelope")
    for key, expected in AUTHORIZED_COUNTS.items():
        if envelope.get(key) != expected:
            raise ManifestReplayError(f"action maximum drift: {key}")
    for key in ZERO_COUNTS:
        if envelope.get(key) != 0:
            raise ManifestReplayError(f"forbidden action enabled: {key}")
    safety = _mapping(payload.get("safety"), "safety")
    for key in (
        "data_download_authorized",
        "cache_mutation_authorized",
        "provider_authorized",
        "quantconnect_authorized",
        "options_authorized",
        "paper_allowed",
        "live_allowed",
        "production_allowed",
        "broker_allowed",
    ):
        if safety.get(key) is not False:
            raise ManifestReplayError(f"forbidden safety flag enabled: {key}")
    if safety.get("production_effect") != "none" or safety.get("broker_action") != "none":
        raise ManifestReplayError("production/broker boundary drift")
    if require_frozen:
        identity = _mapping(payload.get("code_identity"), "code_identity")
        commit = identity.get("exact_source_commit")
        if payload.get("status") != "OWNER_EXACT_AUTHORIZED_NOT_YET_CONSUMED":
            raise ManifestReplayError("manifest is not in executable status")
        if identity.get("source_freeze_state") != "FROZEN":
            raise ManifestReplayError("source identity is not frozen")
        if (
            not isinstance(commit, str)
            or len(commit) != 40
            or any(char not in "0123456789abcdef" for char in commit)
        ):
            raise ManifestReplayError("exact_source_commit is invalid")


def replay_manifest(manifest_path: Path, repository: Path, output_path: Path) -> dict[str, Any]:
    if output_path.exists():
        raise ManifestReplayError("manifest replay receipt already exists")
    payload = load_manifest(manifest_path)
    validate_manifest_shape(payload, require_frozen=True)
    identity = _mapping(payload["code_identity"], "code_identity")
    source_commit = str(identity["exact_source_commit"])
    head = _git(repository, "rev-parse", "HEAD")
    if (
        subprocess.run(
            ["git", "merge-base", "--is-ancestor", source_commit, head],
            cwd=repository,
            check=False,
        ).returncode
        != 0
    ):
        raise ManifestReplayError("source commit is not an ancestor of execution HEAD")

    proofs: list[dict[str, Any]] = []
    for item in identity.get("bound_paths", []):
        binding = _mapping(item, "code_identity.bound_paths[]")
        proof = _verify_path(repository / str(binding["path"]), binding)
        committed = subprocess.run(
            ["git", "show", f"{source_commit}:{binding['path']}"],
            cwd=repository,
            check=True,
            stdout=subprocess.PIPE,
        ).stdout
        if hashlib.sha256(committed).hexdigest() != binding["sha256"]:
            raise ManifestReplayError(f"source commit binding drift: {binding['path']}")
        proofs.append(proof)

    manifest_in_head = subprocess.run(
        ["git", "show", f"HEAD:{manifest_path.relative_to(repository).as_posix()}"],
        cwd=repository,
        check=True,
        stdout=subprocess.PIPE,
    ).stdout
    if hashlib.sha256(manifest_in_head).hexdigest() != sha256_path(manifest_path):
        raise ManifestReplayError("live manifest differs from execution HEAD")

    inputs = _mapping(payload.get("input_allowlist"), "input_allowlist")
    for name, raw in inputs.items():
        binding = _mapping(raw, f"input_allowlist.{name}")
        path = Path(str(binding["path"]))
        if not path.is_absolute():
            path = repository / path
        proof = {"input_id": name, **_verify_path(path, binding)}
        if "row_count" in binding:
            observed = _csv_date_inventory(path)
            for key in ("row_count", "min_date", "max_date"):
                if observed[key] != binding[key]:
                    raise ManifestReplayError(f"CSV inventory drift: {name}.{key}")
            proof.update(observed)
        if "decision_date" in binding:
            observation = json.loads(path.read_text(encoding="utf-8"))
            if observation.get("decision_date") != binding["decision_date"]:
                raise ManifestReplayError(f"observation decision date drift: {name}")
            if observation.get("status") != "OBSERVATION_WRITTEN":
                raise ManifestReplayError(f"observation status drift: {name}")
            if not any(
                row.get("strategy_id") == "equal_risk_qqq_sgov"
                for row in observation.get("observations", [])
                if isinstance(row, dict)
            ):
                raise ManifestReplayError(f"primary strategy missing: {name}")
        proofs.append(proof)

    receipt = {
        "schema_version": "equal_risk_qqq_sgov_catchup_manifest_replay_receipt.v1",
        "task_id": TASK_ID,
        "status": "PASS",
        "authorization_state": "EXACT_PREAUTHORIZED",
        "technical_validation_state": "MANIFEST_REPLAY_PASS",
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "execution_head": head,
        "source_commit": source_commit,
        "manifest": {
            "path": manifest_path.relative_to(repository).as_posix(),
            "sha256": sha256_path(manifest_path),
            "size_bytes": manifest_path.stat().st_size,
        },
        "as_of": "2026-09-03",
        "target_observation_dates": ["2026-06-22", "2026-06-24"],
        "actual_counters": {
            key: (1 if key == "manifest_replays" else 0) for key in payload["run_envelope"]
        },
        "input_proofs": proofs,
        "production_effect": "none",
        "broker_action": "none",
    }
    _atomic_write_json(output_path, receipt)
    return receipt


def _verify_path(path: Path, binding: Mapping[str, Any]) -> dict[str, Any]:
    if not path.is_file():
        raise ManifestReplayError(f"bound path missing: {path}")
    size = path.stat().st_size
    digest = sha256_path(path)
    if size != binding.get("size_bytes") or digest != binding.get("sha256"):
        raise ManifestReplayError(f"bound path identity drift: {path}")
    return {"path": str(path), "sha256": digest, "size_bytes": size}


def _csv_date_inventory(path: Path) -> dict[str, Any]:
    dates: list[str] = []
    with path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            value = row.get("date") or row.get("Date")
            if value:
                dates.append(value)
    if not dates:
        raise ManifestReplayError(f"CSV has no dated rows: {path}")
    return {"row_count": len(dates), "min_date": min(dates), "max_date": max(dates)}


def _mapping(value: object, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ManifestReplayError(f"{label} must be a mapping")
    return value


def _git(repository: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args],
        cwd=repository,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    ).stdout.strip()


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        newline="\n",
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
        delete=False,
    ) as handle:
        handle.write(body)
        temporary = Path(handle.name)
    os.replace(temporary, path)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--repository", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    receipt = replay_manifest(
        args.manifest.resolve(),
        args.repository.resolve(),
        args.output.resolve(),
    )
    print(json.dumps(receipt, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
