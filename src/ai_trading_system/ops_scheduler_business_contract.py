"""OPS-081: strict scheduler semantics independent of assistant preferences.

This is a whitelist, not a natural-language safety classifier. Only explicitly
reviewed advisory suffixes can differ from the release-bound business prompt.
"""

from __future__ import annotations

import hashlib
import json
import tomllib
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path

BUSINESS_SCHEMA = "ops_scheduler_business_contract.v1"
OBSERVATION_SCHEMA = "ops_scheduler_observation.v3"
BINDING_SCHEMA = "ops_scheduler_binding.v3"
# Every accepted field belongs to exactly one class. Unknown fields must never
# become a silently ignored new execution route or credential override.
BUSINESS_FIELDS = frozenset(
    {"version", "kind", "id", "status", "rrule", "execution_environment", "target", "cwds"}
)
PREFERENCE_FIELDS = frozenset({"model", "reasoning_effort", "name", "notification_policy"})
AUDIT_FIELDS = frozenset({"created_at", "updated_at"})
REQUIRED_FIELDS = (BUSINESS_FIELDS | {"prompt", "updated_at"}) - {"version", "kind"}
ALLOWED_FIELDS = BUSINESS_FIELDS | PREFERENCE_FIELDS | AUDIT_FIELDS | {"prompt"}


class SchedulerBusinessContractError(ValueError):
    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        super().__init__(f"{code}: {detail}")


def stable_config_bytes(path: Path) -> bytes:
    """Reject symlinks, torn writes, and replacement during the bounded read."""
    if any(item.is_symlink() for item in (path, *path.parents)) or not path.is_file():
        raise SchedulerBusinessContractError("SCHEDULER_CONFIG_FILE_REQUIRED", str(path))
    before = path.stat()
    raw = path.read_bytes()
    again = path.read_bytes()
    after = path.stat()
    if (
        raw != again
        or (before.st_ino, before.st_size, before.st_mtime_ns)
        != (after.st_ino, after.st_size, after.st_mtime_ns)
        or any(item.is_symlink() for item in (path, *path.parents))
    ):
        raise SchedulerBusinessContractError("SCHEDULER_CONFIG_CONCURRENT_CHANGE", str(path))
    return raw


def prompt_core(actual: str, canonical: str, *, reviewed_advisory_suffixes: Sequence[str]) -> str:
    # Newlines and trailing blank space are serialization differences only.
    core = canonical.replace("\r\n", "\n").rstrip()
    text = actual.replace("\r\n", "\n").rstrip()
    variants = {core, *(core + "\n\n" + suffix.rstrip() for suffix in reviewed_advisory_suffixes)}
    if text not in variants:
        raise SchedulerBusinessContractError("SCHEDULER_PROMPT_DRIFT", "unreviewed business text")
    return core


def business_commitment(
    raw: bytes,
    *,
    canonical_prompt: str,
    reviewed_advisory_suffixes: Sequence[str] = (),
    now: datetime | None = None,
) -> dict[str, object]:
    try:
        config = tomllib.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise SchedulerBusinessContractError(
            "SCHEDULER_CONFIG_READ_FAILED", type(exc).__name__
        ) from exc
    unknown, missing = set(config) - ALLOWED_FIELDS, REQUIRED_FIELDS - set(config)
    if "cwds" in missing:
        raise SchedulerBusinessContractError(
            "SCHEDULER_DEVELOPMENT_CWD_FORBIDDEN", "neutral carrier required"
        )
    if unknown or missing:
        raise SchedulerBusinessContractError(
            "SCHEDULER_CONFIG_FIELDS", f"unknown={sorted(unknown)};missing={sorted(missing)}"
        )
    for field in (BUSINESS_FIELDS - {"version", "target", "cwds"}) | {"prompt"}:
        value = config.get(field, "cron" if field == "kind" else None)
        if not isinstance(value, str) or not value.strip():
            raise SchedulerBusinessContractError("SCHEDULER_CONFIG_TYPE", field)
    version = config.get("version", 1)
    if type(version) is not int or version != 1 or config.get("kind", "cron") != "cron":
        raise SchedulerBusinessContractError("SCHEDULER_CONFIG_VERSION_OR_KIND", "expected v1 cron")
    for field in PREFERENCE_FIELDS & set(config):
        value = config[field]
        if not isinstance(value, str) or not value.strip():
            raise SchedulerBusinessContractError("SCHEDULER_CONFIG_TYPE", field)
    if config.get("notification_policy", "failed_runs_only") != "failed_runs_only":
        raise SchedulerBusinessContractError("SCHEDULER_CONFIG_TYPE", "notification_policy")
    if config["target"] != {"type": "projectless"}:
        raise SchedulerBusinessContractError(
            "SCHEDULER_TARGET_MISMATCH", "exact projectless target required"
        )
    if config["cwds"] != ["~"]:
        raise SchedulerBusinessContractError(
            "SCHEDULER_DEVELOPMENT_CWD_FORBIDDEN", "neutral carrier required"
        )
    checked_at = now or datetime.now(UTC)
    if checked_at.tzinfo is None:
        raise SchedulerBusinessContractError("SCHEDULER_TIMESTAMP_INVALID", "aware time required")
    for field in AUDIT_FIELDS & set(config):
        value = config[field]
        if type(value) is not int or value < 0 or value > int(checked_at.timestamp() * 1000):
            raise SchedulerBusinessContractError("SCHEDULER_OBSERVATION_PREDATES_CONFIG", field)
    core = prompt_core(
        config["prompt"], canonical_prompt, reviewed_advisory_suffixes=reviewed_advisory_suffixes
    )
    projection = {field: config[field] for field in sorted(BUSINESS_FIELDS) if field in config}
    projection.update(version=version, kind=config.get("kind", "cron"))
    projection["prompt_core_sha256"] = hashlib.sha256(core.encode("utf-8")).hexdigest()
    encoded = json.dumps(
        projection, sort_keys=True, ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")
    return {
        "schema_version": BUSINESS_SCHEMA,
        "projection": projection,
        "sha256": hashlib.sha256(encoded).hexdigest(),
    }


def verify_business_commitment(
    retained: Mapping[str, object], current: Mapping[str, object]
) -> None:
    if dict(retained) != dict(current):
        raise SchedulerBusinessContractError(
            "SCHEDULER_BUSINESS_CONTRACT_DRIFT", "business projection differs"
        )


def validate_retained_business_commitment(retained: Mapping[str, object]) -> Mapping[str, object]:
    projection = retained.get("projection")
    if (
        set(retained) != {"schema_version", "projection", "sha256"}
        or retained.get("schema_version") != BUSINESS_SCHEMA
        or not isinstance(projection, Mapping)
        or set(projection) != BUSINESS_FIELDS | {"prompt_core_sha256"}
        or type(projection.get("version")) is not int
        or projection.get("version") != 1
        or projection.get("kind") != "cron"
        or projection.get("target") != {"type": "projectless"}
        or projection.get("cwds") != ["~"]
    ):
        raise SchedulerBusinessContractError("SCHEDULER_BUSINESS_CONTRACT_INVALID", "shape")
    encoded = json.dumps(
        dict(projection), sort_keys=True, ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")
    if retained.get("sha256") != hashlib.sha256(encoded).hexdigest():
        raise SchedulerBusinessContractError("SCHEDULER_BUSINESS_CONTRACT_INVALID", "checksum")
    return projection
