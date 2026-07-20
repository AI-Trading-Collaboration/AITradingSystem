from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path, PurePath
from typing import Any

import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.platform.artifacts.writer import write_json_atomic
from ai_trading_system.yaml_loader import safe_load_yaml_path

POLICY_SCHEMA_VERSION = "legacy_research_artifact_portable_lineage_policy.v1"
SIDECAR_SCHEMA_VERSION = "legacy_research_artifact_portable_lineage_sidecar.v1"
LOCATOR_KIND = "project_relative_content"
DEFAULT_POLICY_PATH = (
    PROJECT_ROOT / "config" / "research" / "legacy_research_artifact_portable_lineage_policy.yaml"
)
DEFAULT_TRADING2449_SIDECAR_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research"
    / "legacy_lineage"
    / "trading2449_r0_r1_r2_portable_lineage.v1.json"
)

REQUIRED_SIDECAR_CONTRACT = {
    "schema_version": SIDECAR_SCHEMA_VERSION,
    "allowed_locator_kinds": [LOCATOR_KIND],
    "require_sidecar_content_id": True,
    "require_policy_hash_binding": True,
    "require_legacy_artifact_hash_binding": True,
    "require_source_sha256": True,
    "require_source_size": True,
}
REQUIRED_RESOLUTION_CONTRACT = {
    "portable_mode_requires_explicit_opt_in": True,
    "require_portable_source": True,
    "allow_missing_historical_path": True,
    "require_historical_source_match_when_present": True,
    "historical_portable_conflict_action": "fail_closed",
    "missing_source_action": "fail_closed",
    "tamper_action": "fail_closed",
    "reject_absolute_locator": True,
    "reject_parent_traversal": True,
    "require_locator_under_project_root": True,
}
REQUIRED_DISTRIBUTION_CONTRACT = {
    "canonical_sidecar_path": (
        "inputs/research/legacy_lineage/" "trading2449_r0_r1_r2_portable_lineage.v1.json"
    ),
    "persistence": "tracked_repository_input",
    "archive_installation": (
        "Copy the immutable R0/R1/R2 artifact and source archive into the exact "
        "project-relative locator paths recorded by the canonical sidecar, then run "
        "the validators with explicit portable_lineage_sidecar_path.  Never rewrite "
        "the legacy artifacts or the sidecar during installation."
    ),
    "clean_clone_without_archive": "fail_closed_portable_source_missing",
}
REQUIRED_CONSUMERS = {
    "r0_preflight",
    "r1_walk_forward",
    "r1_robustness",
    "r2_decision",
}
REQUIRED_FAILURE_REASONS = {
    "POLICY_SCHEMA_INVALID",
    "SIDECAR_SCHEMA_INVALID",
    "SIDECAR_CONTENT_ID_MISMATCH",
    "POLICY_BINDING_MISMATCH",
    "LEGACY_ARTIFACT_BINDING_MISSING",
    "LEGACY_ARTIFACT_TAMPERED",
    "SOURCE_BINDING_MISSING",
    "SOURCE_EXPECTATION_MISMATCH",
    "SIDECAR_MISSING",
    "PORTABLE_SOURCE_MISSING",
    "PORTABLE_SOURCE_TAMPERED",
    "HISTORICAL_SOURCE_TAMPERED",
    "HISTORICAL_PORTABLE_CONFLICT",
    "LOCATOR_PATH_TRAVERSAL",
    "LOCATOR_OUTSIDE_PROJECT_ROOT",
}

SAFETY = {
    "immutable_legacy_artifact_rewrite_allowed": False,
    "research_recalculation_allowed": False,
    "candidate_generation_allowed": False,
    "parameter_search_allowed": False,
    "promotion_change_allowed": False,
    "production_effect": "none",
    "broker_action": "none",
}


class PortableLineageError(ValueError):
    """A stable fail-closed portable-lineage resolution failure."""

    def __init__(self, reason_code: str, detail: str) -> None:
        super().__init__(f"{reason_code}: {detail}")
        self.reason_code = reason_code
        self.detail = detail


@dataclass(frozen=True)
class PortableLineageBinding:
    """One immutable legacy path and its byte-identical portable source."""

    binding_id: str
    legacy_path: str
    portable_path: Path
    consumers: tuple[str, ...]


def load_portable_lineage_policy(
    path: Path = DEFAULT_POLICY_PATH,
) -> dict[str, Any]:
    try:
        payload = safe_load_yaml_path(path)
    except (OSError, yaml.YAMLError) as exc:
        raise PortableLineageError(
            "POLICY_SCHEMA_INVALID", f"policy cannot be loaded: {path}"
        ) from exc
    if not isinstance(payload, Mapping):
        raise PortableLineageError("POLICY_SCHEMA_INVALID", "policy must be a mapping")
    policy = dict(payload)
    if policy.get("schema_version") != POLICY_SCHEMA_VERSION:
        raise PortableLineageError("POLICY_SCHEMA_INVALID", "unexpected schema_version")
    if policy.get("policy_id") != "legacy_research_artifact_portable_lineage_v1":
        raise PortableLineageError("POLICY_SCHEMA_INVALID", "unexpected policy_id")
    if policy.get("policy_version") != 1:
        raise PortableLineageError("POLICY_SCHEMA_INVALID", "unexpected policy_version")
    if policy.get("status") != "owner_approved_portability_contract":
        raise PortableLineageError("POLICY_SCHEMA_INVALID", "unexpected policy status")
    if not str(policy.get("owner", "")).strip():
        raise PortableLineageError("POLICY_SCHEMA_INVALID", "policy owner is required")
    sidecar = _mapping(policy.get("sidecar"))
    if sidecar != REQUIRED_SIDECAR_CONTRACT:
        raise PortableLineageError("POLICY_SCHEMA_INVALID", "sidecar contract changed")
    if _mapping(policy.get("resolution")) != REQUIRED_RESOLUTION_CONTRACT:
        raise PortableLineageError("POLICY_SCHEMA_INVALID", "resolution contract changed")
    if _mapping(policy.get("distribution")) != REQUIRED_DISTRIBUTION_CONTRACT:
        raise PortableLineageError("POLICY_SCHEMA_INVALID", "distribution contract changed")
    if _mapping(policy.get("safety")) != SAFETY:
        raise PortableLineageError("POLICY_SCHEMA_INVALID", "safety boundary changed")
    validation = _mapping(policy.get("validation"))
    consumers = validation.get("required_consumers")
    reasons = validation.get("required_failure_reasons")
    if (
        not isinstance(consumers, list)
        or not all(isinstance(item, str) for item in consumers)
        or len(consumers) != len(set(consumers))
        or set(consumers) != REQUIRED_CONSUMERS
    ):
        raise PortableLineageError("POLICY_SCHEMA_INVALID", "required consumers changed")
    if (
        not isinstance(reasons, list)
        or not all(isinstance(item, str) for item in reasons)
        or len(reasons) != len(set(reasons))
        or set(reasons) != REQUIRED_FAILURE_REASONS
    ):
        raise PortableLineageError("POLICY_SCHEMA_INVALID", "failure reason contract changed")
    if not str(validation.get("review_condition", "")).strip():
        raise PortableLineageError("POLICY_SCHEMA_INVALID", "review_condition is required")
    return policy


def build_portable_lineage_sidecar(
    *,
    output_path: Path,
    legacy_artifacts: Sequence[PortableLineageBinding],
    sources: Sequence[PortableLineageBinding],
    project_root: Path = PROJECT_ROOT,
    policy_path: Path = DEFAULT_POLICY_PATH,
) -> dict[str, Any]:
    """Build a deterministic sidecar without changing any legacy artifact bytes."""

    policy = load_portable_lineage_policy(policy_path)
    if not legacy_artifacts:
        raise PortableLineageError(
            "LEGACY_ARTIFACT_BINDING_MISSING", "at least one legacy artifact is required"
        )
    artifact_records = [
        _binding_record(binding, project_root=project_root) for binding in legacy_artifacts
    ]
    source_records = [_binding_record(binding, project_root=project_root) for binding in sources]
    _require_unique_bindings(artifact_records, label="legacy_artifact")
    _require_unique_bindings(source_records, label="source")
    payload: dict[str, Any] = {
        "schema_version": SIDECAR_SCHEMA_VERSION,
        "policy_binding": {
            "policy_id": policy["policy_id"],
            "policy_version": policy["policy_version"],
            "policy_sha256": _file_sha256(policy_path),
        },
        "legacy_artifacts": artifact_records,
        "sources": source_records,
        "safety": dict(SAFETY),
    }
    payload["sidecar_id"] = "portable-lineage_" + _canonical_sha256(payload)[:20]
    write_json_atomic(output_path, payload)
    return payload


def build_research_restart_portable_lineage_sidecar(
    *,
    output_path: Path,
    r0_preflight_path: Path,
    walk_forward_dir: Path,
    robustness_dir: Path,
    r2_output_root: Path,
    historical_project_root: Path,
    portable_source_overrides: Mapping[str, Path] | None = None,
    project_root: Path = PROJECT_ROOT,
    policy_path: Path = DEFAULT_POLICY_PATH,
) -> dict[str, Any]:
    """Discover the immutable R0/R1/R2 path graph and build one replay sidecar."""

    r0_path = r0_preflight_path.resolve()
    wf_manifest_path = (walk_forward_dir / "r1_wf_manifest.json").resolve()
    robustness_manifest_path = (robustness_dir / "r1_robustness_manifest.json").resolve()
    r2_manifest_path = (r2_output_root / "strategy_research_restart_r2_manifest.json").resolve()
    r0 = _load_json_mapping(r0_path)
    wf_manifest = _load_json_mapping(wf_manifest_path)
    wf_index = _load_json_mapping(walk_forward_dir / "fold_evaluations_index.json")
    robustness_manifest = _load_json_mapping(robustness_manifest_path)
    r2_manifest = _load_json_mapping(r2_manifest_path)
    commitments = _mapping(r2_manifest.get("input_commitments"))

    artifact_historical_paths = {
        "r0_preflight": str(_mapping(commitments.get("r0_preflight")).get("path", "")),
        "r1_walk_forward": str(_mapping(commitments.get("walk_forward_manifest")).get("path", "")),
        "r1_robustness": str(_mapping(commitments.get("robustness_manifest")).get("path", "")),
        "r2_decision": str(
            historical_project_root.resolve() / r2_manifest_path.relative_to(project_root.resolve())
        ),
    }
    legacy_artifacts = [
        PortableLineageBinding(
            binding_id="artifact_r0_preflight",
            legacy_path=artifact_historical_paths["r0_preflight"],
            portable_path=r0_path,
            consumers=("r0_preflight",),
        ),
        PortableLineageBinding(
            binding_id="artifact_r1_walk_forward_manifest",
            legacy_path=artifact_historical_paths["r1_walk_forward"],
            portable_path=wf_manifest_path,
            consumers=("r1_walk_forward",),
        ),
        PortableLineageBinding(
            binding_id="artifact_r1_robustness_manifest",
            legacy_path=artifact_historical_paths["r1_robustness"],
            portable_path=robustness_manifest_path,
            consumers=("r1_robustness",),
        ),
        PortableLineageBinding(
            binding_id="artifact_r2_decision_manifest",
            legacy_path=artifact_historical_paths["r2_decision"],
            portable_path=r2_manifest_path,
            consumers=("r2_decision",),
        ),
    ]

    discovered: dict[str, tuple[Path, set[str]]] = {}
    overrides = dict(portable_source_overrides or {})

    def add(raw_path: Any, consumer: str) -> None:
        raw = str(raw_path or "").strip()
        if not raw:
            return
        portable = overrides.get(raw)
        if portable is None:
            portable = _portable_for_historical_path(
                raw,
                historical_project_root=historical_project_root,
                project_root=project_root,
            )
        existing = discovered.get(raw)
        if existing is not None and existing[0] != portable:
            raise PortableLineageError(
                "SIDECAR_SCHEMA_INVALID", f"ambiguous portable source for {raw}"
            )
        consumers = set() if existing is None else existing[1]
        consumers.add(consumer)
        discovered[raw] = (portable, consumers)

    for fingerprint in _mapping(r0.get("input_fingerprints")).values():
        add(_mapping(fingerprint).get("path"), "r0_preflight")
    add(_mapping(r0.get("artifact_paths")).get("markdown"), "r0_preflight")

    for key in ("restart_preflight_path", "restart_policy_path", "prices_path"):
        add(wf_manifest.get(key), "r1_walk_forward")
    for raw in _mapping(wf_manifest.get("source_artifacts")).values():
        add(raw, "r1_walk_forward")
    for evaluation in _records(wf_index.get("evaluations")):
        add(evaluation.get("evaluation_path"), "r1_walk_forward")

    for key in (
        "restart_preflight_path",
        "restart_policy_path",
        "prices_path",
        "source_real_evaluation_path",
    ):
        add(robustness_manifest.get(key), "r1_robustness")
    for raw in _mapping(robustness_manifest.get("source_artifacts")).values():
        add(raw, "r1_robustness")
    for neighbor in _records(robustness_manifest.get("derived_neighbors")):
        add(neighbor.get("path"), "r1_robustness")

    for commitment in commitments.values():
        add(_mapping(commitment).get("path"), "r2_decision")
    maturity_raw_path = str(_mapping(commitments.get("forward_maturity")).get("path", ""))
    maturity_path = overrides.get(maturity_raw_path)
    if maturity_path is None:
        maturity_path = _portable_for_historical_path(
            maturity_raw_path,
            historical_project_root=historical_project_root,
            project_root=project_root,
        )
    maturity = _load_json_mapping(maturity_path)
    add(maturity.get("config_path"), "r2_decision")
    add(maturity.get("ledger_path"), "r2_decision")
    for key in ("prices_path", "secondary_prices_path", "rates_path"):
        add(_mapping(maturity.get("data_quality_gate")).get(key), "r2_decision")

    sources = [
        PortableLineageBinding(
            binding_id="source_" + sha256(raw.encode("utf-8")).hexdigest()[:20],
            legacy_path=raw,
            portable_path=portable,
            consumers=tuple(sorted(consumers)),
        )
        for raw, (portable, consumers) in sorted(discovered.items())
    ]
    return build_portable_lineage_sidecar(
        output_path=output_path,
        legacy_artifacts=legacy_artifacts,
        sources=sources,
        project_root=project_root,
        policy_path=policy_path,
    )


class PortableLineageResolver:
    """Resolve immutable historical paths through a verified opt-in sidecar."""

    def __init__(
        self,
        *,
        sidecar_path: Path,
        subject_artifact_path: Path,
        consumer: str,
        project_root: Path = PROJECT_ROOT,
        policy_path: Path = DEFAULT_POLICY_PATH,
    ) -> None:
        self.sidecar_path = sidecar_path.resolve()
        self.subject_artifact_path = subject_artifact_path.resolve()
        self.consumer = consumer
        self.project_root = project_root.resolve()
        self.policy_path = policy_path.resolve()
        self.policy = load_portable_lineage_policy(self.policy_path)
        self.sidecar = _load_sidecar(self.sidecar_path)
        self._resolved: dict[str, dict[str, Any]] = {}
        self._artifact_binding: dict[str, Any] = {}
        self._validate_contract()
        self._verify_subject_binding()
        self.verify_consumer_sources()

    def verify_consumer_sources(self) -> None:
        records = _records(self.sidecar.get("sources"))
        selected = [record for record in records if self.consumer in _consumers(record)]
        for record in selected:
            self._verify_record(record)

    def resolve(
        self,
        legacy_path: Path | str,
        *,
        expected_sha256: str | None = None,
        expected_size: int | None = None,
    ) -> Path:
        key = _legacy_key(str(legacy_path), project_root=self.project_root)
        record = self._source_records().get(key)
        if record is None or self.consumer not in _consumers(record):
            raise PortableLineageError(
                "SOURCE_BINDING_MISSING",
                f"no {self.consumer} binding for legacy path {legacy_path}",
            )
        if expected_sha256 is not None and record.get("sha256") != expected_sha256:
            raise PortableLineageError(
                "SOURCE_EXPECTATION_MISMATCH",
                f"manifest hash disagrees with sidecar for {legacy_path}",
            )
        if expected_size is not None and int(record.get("size", -1)) != expected_size:
            raise PortableLineageError(
                "SOURCE_EXPECTATION_MISMATCH",
                f"manifest size disagrees with sidecar for {legacy_path}",
            )
        self._verify_record(record)
        return _locator_path(record, project_root=self.project_root)

    def evidence(self) -> dict[str, Any]:
        binding = self._artifact_binding
        return {
            "schema_version": "legacy_research_artifact_resolution_evidence.v1",
            "status": "PASS",
            "mode": "explicit_portable_lineage",
            "consumer": self.consumer,
            "sidecar_id": self.sidecar.get("sidecar_id"),
            "sidecar_path": str(self.sidecar_path),
            "sidecar_sha256": _file_sha256(self.sidecar_path),
            "policy_id": self.policy.get("policy_id"),
            "policy_version": self.policy.get("policy_version"),
            "policy_sha256": _file_sha256(self.policy_path),
            "legacy_artifact": {
                "binding_id": binding.get("binding_id"),
                "legacy_path": binding.get("legacy_path"),
                "sha256": binding.get("sha256"),
                "size": binding.get("size"),
                "resolved_path": str(self.subject_artifact_path),
            },
            "resolved_sources": [
                self._resolved[key] for key in sorted(self._resolved) if key.startswith("source:")
            ],
            "production_effect": "none",
            "broker_action": "none",
        }

    def _validate_contract(self) -> None:
        if self.sidecar.get("schema_version") != SIDECAR_SCHEMA_VERSION:
            raise PortableLineageError(
                "SIDECAR_SCHEMA_INVALID", "unexpected sidecar schema_version"
            )
        claimed_id = str(self.sidecar.get("sidecar_id", ""))
        unsigned = dict(self.sidecar)
        unsigned.pop("sidecar_id", None)
        expected_id = "portable-lineage_" + _canonical_sha256(unsigned)[:20]
        if claimed_id != expected_id:
            raise PortableLineageError(
                "SIDECAR_CONTENT_ID_MISMATCH", "sidecar content does not match sidecar_id"
            )
        binding = _mapping(self.sidecar.get("policy_binding"))
        if (
            binding.get("policy_id") != self.policy.get("policy_id")
            or binding.get("policy_version") != self.policy.get("policy_version")
            or binding.get("policy_sha256") != _file_sha256(self.policy_path)
        ):
            raise PortableLineageError(
                "POLICY_BINDING_MISMATCH", "sidecar policy binding is stale or invalid"
            )
        if _mapping(self.sidecar.get("safety")) != SAFETY:
            raise PortableLineageError("SIDECAR_SCHEMA_INVALID", "safety boundary changed")
        artifacts = _records(self.sidecar.get("legacy_artifacts"))
        sources = _records(self.sidecar.get("sources"))
        if not artifacts:
            raise PortableLineageError(
                "LEGACY_ARTIFACT_BINDING_MISSING", "legacy_artifacts is empty"
            )
        _require_unique_bindings(artifacts, label="legacy_artifact")
        _require_unique_bindings(sources, label="source")
        normalized_source_paths = [
            _legacy_key(str(record.get("legacy_path", "")), project_root=self.project_root)
            for record in sources
        ]
        if len(normalized_source_paths) != len(set(normalized_source_paths)):
            raise PortableLineageError(
                "SIDECAR_SCHEMA_INVALID", "duplicate normalized source legacy_path"
            )
        for record in [*artifacts, *sources]:
            _validate_record_schema(record, project_root=self.project_root)

    def _verify_subject_binding(self) -> None:
        candidates = [
            record
            for record in _records(self.sidecar.get("legacy_artifacts"))
            if self.consumer in _consumers(record)
            and _locator_path(record, project_root=self.project_root) == self.subject_artifact_path
        ]
        if len(candidates) != 1:
            raise PortableLineageError(
                "LEGACY_ARTIFACT_BINDING_MISSING",
                f"expected one {self.consumer} binding for {self.subject_artifact_path}",
            )
        record = candidates[0]
        self._verify_record(record, artifact=True)
        self._artifact_binding = record

    def _source_records(self) -> dict[str, dict[str, Any]]:
        return {
            _legacy_key(str(record.get("legacy_path", "")), project_root=self.project_root): record
            for record in _records(self.sidecar.get("sources"))
        }

    def _verify_record(self, record: Mapping[str, Any], *, artifact: bool = False) -> None:
        binding_id = str(record.get("binding_id", ""))
        cache_key = ("artifact:" if artifact else "source:") + binding_id
        if cache_key in self._resolved:
            return
        portable = _locator_path(record, project_root=self.project_root)
        historical = _historical_path(
            str(record.get("legacy_path", "")), project_root=self.project_root
        )
        expected_hash = str(record.get("sha256", ""))
        expected_size = int(record.get("size", -1))
        if not portable.is_file():
            raise PortableLineageError(
                "PORTABLE_SOURCE_MISSING", f"portable source missing for {binding_id}"
            )
        portable_hash = _file_sha256(portable)
        portable_size = portable.stat().st_size
        historical_exists = historical.is_file()
        historical_hash = _file_sha256(historical) if historical_exists else None
        historical_size = historical.stat().st_size if historical_exists else None
        same_path = historical_exists and historical.resolve() == portable.resolve()
        if historical_exists and not same_path and historical_hash != portable_hash:
            raise PortableLineageError(
                "HISTORICAL_PORTABLE_CONFLICT",
                f"historical and portable bytes conflict for {binding_id}",
            )
        if portable_hash != expected_hash or portable_size != expected_size:
            reason = "LEGACY_ARTIFACT_TAMPERED" if artifact else "PORTABLE_SOURCE_TAMPERED"
            raise PortableLineageError(reason, f"portable content mismatch for {binding_id}")
        if historical_exists and (
            historical_hash != expected_hash or historical_size != expected_size
        ):
            reason = "LEGACY_ARTIFACT_TAMPERED" if artifact else "HISTORICAL_SOURCE_TAMPERED"
            raise PortableLineageError(reason, f"historical content mismatch for {binding_id}")
        self._resolved[cache_key] = {
            "binding_id": binding_id,
            "legacy_path": str(record.get("legacy_path", "")),
            "locator_kind": LOCATOR_KIND,
            "locator_path": _mapping(record.get("locator")).get("path"),
            "resolved_path": str(portable),
            "sha256": expected_hash,
            "size": expected_size,
            "historical_path_status": (
                "SAME_AS_PORTABLE"
                if same_path
                else "MATCH" if historical_exists else "MISSING_ALLOWED"
            ),
        }


def portable_lineage_failure_evidence(
    *,
    error: PortableLineageError,
    consumer: str,
    sidecar_path: Path,
) -> dict[str, Any]:
    return {
        "schema_version": "legacy_research_artifact_resolution_evidence.v1",
        "status": "FAIL",
        "mode": "explicit_portable_lineage",
        "consumer": consumer,
        "reason_code": error.reason_code,
        "detail": error.detail,
        "sidecar_path": str(sidecar_path),
        "production_effect": "none",
        "broker_action": "none",
    }


def _binding_record(binding: PortableLineageBinding, *, project_root: Path) -> dict[str, Any]:
    if not binding.binding_id.strip():
        raise PortableLineageError("SIDECAR_SCHEMA_INVALID", "binding_id is required")
    if not binding.consumers:
        raise PortableLineageError("SIDECAR_SCHEMA_INVALID", "consumers is required")
    portable = binding.portable_path.resolve()
    if not portable.is_file():
        raise PortableLineageError(
            "PORTABLE_SOURCE_MISSING", f"portable source missing: {portable}"
        )
    locator = _project_relative_locator(portable, project_root=project_root)
    return {
        "binding_id": binding.binding_id,
        "legacy_path": binding.legacy_path,
        "locator": locator,
        "sha256": _file_sha256(portable),
        "size": portable.stat().st_size,
        "consumers": sorted(set(binding.consumers)),
    }


def _project_relative_locator(path: Path, *, project_root: Path) -> dict[str, str]:
    root = project_root.resolve()
    try:
        relative = path.resolve().relative_to(root)
    except ValueError as exc:
        raise PortableLineageError(
            "LOCATOR_OUTSIDE_PROJECT_ROOT", f"portable source is outside project root: {path}"
        ) from exc
    return {"kind": LOCATOR_KIND, "path": relative.as_posix()}


def _locator_path(record: Mapping[str, Any], *, project_root: Path) -> Path:
    locator = _mapping(record.get("locator"))
    if locator.get("kind") != LOCATOR_KIND:
        raise PortableLineageError("SIDECAR_SCHEMA_INVALID", "unsupported locator kind")
    raw = str(locator.get("path", ""))
    pure = PurePath(raw)
    if not raw or pure.is_absolute() or Path(raw).is_absolute():
        raise PortableLineageError("LOCATOR_PATH_TRAVERSAL", "locator must be relative")
    if any(part in {"", ".", ".."} for part in pure.parts):
        raise PortableLineageError("LOCATOR_PATH_TRAVERSAL", f"unsafe locator path: {raw}")
    root = project_root.resolve()
    resolved = (root / Path(*pure.parts)).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise PortableLineageError(
            "LOCATOR_OUTSIDE_PROJECT_ROOT", f"locator escaped project root: {raw}"
        ) from exc
    return resolved


def _validate_record_schema(record: Mapping[str, Any], *, project_root: Path) -> None:
    if not str(record.get("binding_id", "")).strip():
        raise PortableLineageError("SIDECAR_SCHEMA_INVALID", "binding_id is required")
    if not str(record.get("legacy_path", "")).strip():
        raise PortableLineageError("SIDECAR_SCHEMA_INVALID", "legacy_path is required")
    digest = str(record.get("sha256", ""))
    if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
        raise PortableLineageError("SIDECAR_SCHEMA_INVALID", "sha256 is invalid")
    if not isinstance(record.get("size"), int) or int(record["size"]) < 0:
        raise PortableLineageError("SIDECAR_SCHEMA_INVALID", "size is invalid")
    if not _consumers(record):
        raise PortableLineageError("SIDECAR_SCHEMA_INVALID", "consumers is empty")
    _locator_path(record, project_root=project_root)


def _require_unique_bindings(records: Sequence[Mapping[str, Any]], *, label: str) -> None:
    ids = [str(record.get("binding_id", "")) for record in records]
    paths = [str(record.get("legacy_path", "")) for record in records]
    if len(ids) != len(set(ids)):
        raise PortableLineageError("SIDECAR_SCHEMA_INVALID", f"duplicate {label} binding_id")
    if len(paths) != len(set(paths)):
        raise PortableLineageError("SIDECAR_SCHEMA_INVALID", f"duplicate {label} legacy_path")


def _load_sidecar(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise PortableLineageError("SIDECAR_MISSING", f"sidecar missing: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PortableLineageError("SIDECAR_SCHEMA_INVALID", "sidecar is not valid JSON") from exc
    if not isinstance(payload, Mapping):
        raise PortableLineageError("SIDECAR_SCHEMA_INVALID", "sidecar must be a mapping")
    return dict(payload)


def _historical_path(raw: str, *, project_root: Path) -> Path:
    path = Path(raw)
    return path if path.is_absolute() else project_root / path


def _portable_for_historical_path(
    raw: str, *, historical_project_root: Path, project_root: Path
) -> Path:
    path = Path(raw)
    current_root = project_root.resolve()
    historical_root = historical_project_root.resolve()
    if not path.is_absolute():
        return (current_root / path).resolve()
    resolved = path.resolve()
    for root in (historical_root, current_root):
        try:
            relative = resolved.relative_to(root)
        except ValueError:
            continue
        return (current_root / relative).resolve()
    raise PortableLineageError(
        "LOCATOR_OUTSIDE_PROJECT_ROOT",
        f"historical source cannot be rebased into project root: {raw}",
    )


def _legacy_key(raw: str, *, project_root: Path) -> str:
    return str(_historical_path(raw, project_root=project_root).resolve()).casefold()


def _canonical_sha256(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    return sha256(encoded).hexdigest()


def _load_json_mapping(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise PortableLineageError("PORTABLE_SOURCE_MISSING", f"JSON source missing: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PortableLineageError("PORTABLE_SOURCE_TAMPERED", f"invalid JSON: {path}") from exc
    if not isinstance(payload, Mapping):
        raise PortableLineageError("PORTABLE_SOURCE_TAMPERED", f"JSON is not a mapping: {path}")
    return dict(payload)


def _file_sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _records(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _consumers(record: Mapping[str, Any]) -> tuple[str, ...]:
    value = record.get("consumers")
    if not isinstance(value, list):
        return ()
    return tuple(str(item) for item in value if str(item).strip())


__all__ = [
    "DEFAULT_POLICY_PATH",
    "DEFAULT_TRADING2449_SIDECAR_PATH",
    "LOCATOR_KIND",
    "POLICY_SCHEMA_VERSION",
    "PortableLineageBinding",
    "PortableLineageError",
    "PortableLineageResolver",
    "SIDECAR_SCHEMA_VERSION",
    "build_portable_lineage_sidecar",
    "build_research_restart_portable_lineage_sidecar",
    "load_portable_lineage_policy",
    "portable_lineage_failure_evidence",
]
