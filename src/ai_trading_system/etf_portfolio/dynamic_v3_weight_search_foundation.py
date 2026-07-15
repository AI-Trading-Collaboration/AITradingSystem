from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.data.quality import render_data_quality_report, write_data_quality_report
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st
from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as _legacy

DEFAULT_WEIGHT_SEARCH_SPACE_CONFIG_PATH = _legacy.DEFAULT_WEIGHT_SEARCH_SPACE_CONFIG_PATH
DEFAULT_WEIGHT_SEARCH_SPACE_DIR = _legacy.DEFAULT_WEIGHT_SEARCH_SPACE_DIR
DEFAULT_WEIGHT_EXPERIMENT_BATCH2_DIR = _legacy.DEFAULT_WEIGHT_EXPERIMENT_BATCH2_DIR
DEFAULT_WEIGHT_BATCH_BACKFILL_DIR = _legacy.DEFAULT_WEIGHT_BATCH_BACKFILL_DIR
SEARCH_REQUIRED_FAMILIES = _legacy.SEARCH_REQUIRED_FAMILIES

_assert_weight_search_safety = _legacy._assert_weight_search_safety
_mapping = _legacy._mapping
_enabled_families = _legacy._enabled_families
_float = _legacy._float
_text = _legacy._text
_weight_search_safety_locked = _legacy._weight_search_safety_locked
_search_family_inventory = _legacy._search_family_inventory
_stable_id = _legacy._stable_id
_unique_dir = _legacy._unique_dir
_write_json = _legacy._write_json
_write_latest_pointer = _legacy._write_latest_pointer
_write_text = _legacy._write_text
render_weight_search_space_report = _legacy.render_weight_search_space_report
_artifact_dir = _legacy._artifact_dir
_read_json = _legacy._read_json
_read_optional_json = _legacy._read_optional_json
_records = _legacy._records
_required_file_checks = _legacy._required_file_checks
_texts = _legacy._texts
_payload_experiment_safe = _legacy._payload_experiment_safe
_payload_safe = _legacy._payload_safe
_validation_payload = _legacy._validation_payload
_batch2_family_coverage = _legacy._batch2_family_coverage
_generate_batch2_variants = _legacy._generate_batch2_variants
_write_jsonl = _legacy._write_jsonl
render_batch2_matrix_report = _legacy.render_batch2_matrix_report
_read_jsonl = _legacy._read_jsonl
_batch2_matrix_payload = _legacy._batch2_matrix_payload
_coerce_date = _legacy._coerce_date
_latest_common_price_date = _legacy._latest_common_price_date
_variant_churn_metrics = _legacy._variant_churn_metrics
_variant_lag_metrics = _legacy._variant_lag_metrics
render_batch_backfill_report = _legacy.render_batch_backfill_report

SEARCH_SPACE_INPUT_SCHEMA = "weight_search_space_input_snapshot.v2"
MATRIX_INPUT_SCHEMA = "weight_experiment_batch2_input_snapshot.v2"
BACKFILL_INPUT_SCHEMA = "weight_batch_backfill_input_snapshot.v2"


class DynamicV3WeightSearchFoundationError(ValueError):
    """Raised when the weight-search foundation chain is not reproducible."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise DynamicV3WeightSearchFoundationError(message)


def _json_bytes(payload: Mapping[str, Any]) -> bytes:
    text = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    return text.replace("\n", os.linesep).encode("utf-8")


def _jsonl_bytes(rows: Sequence[Mapping[str, Any]]) -> bytes:
    return "".join(
        json.dumps(row, ensure_ascii=False, sort_keys=True) + os.linesep for row in rows
    ).encode("utf-8")


def _text_file_bytes(text: str) -> bytes:
    return text.replace("\n", os.linesep).encode("utf-8")


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _file_binding(path: Path) -> dict[str, Any]:
    resolved = path.resolve()
    _require(resolved.is_file(), f"source file missing: {resolved}")
    payload = resolved.read_bytes()
    return {"path": str(resolved), "sha256": _sha256_bytes(payload), "size_bytes": len(payload)}


def _validate_file_binding(binding: Mapping[str, Any]) -> None:
    path = Path(str(binding.get("path", "")))
    _require(path.is_file(), f"bound source file missing: {path}")
    payload = path.read_bytes()
    _require(_sha256_bytes(payload) == binding.get("sha256"), f"bound source drift: {path}")
    _require(len(payload) == binding.get("size_bytes"), f"bound source size drift: {path}")


def _write_snapshot(path: Path, snapshot: Mapping[str, Any]) -> None:
    _write_json(path, snapshot)


def _view_hashes(root: Path, names: Sequence[str]) -> dict[str, str]:
    return {name: _sha256_bytes((root / name).read_bytes()) for name in names}


def _validate_view_hashes(root: Path, expected: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    for name, digest in expected.items():
        path = root / str(name)
        if not path.is_file():
            errors.append(f"missing view: {name}")
        elif _sha256_bytes(path.read_bytes()) != digest:
            errors.append(f"view drift: {name}")
    return errors


def _artifact_binding(
    *, kind: str, artifact_id: str, root: Path, names: Sequence[str]
) -> dict[str, Any]:
    resolved = root.resolve()
    return {
        "kind": kind,
        "artifact_id": artifact_id,
        "source_dir": str(resolved),
        "files": {name: _file_binding(resolved / name) for name in names},
    }


def _validate_artifact_binding(binding: Mapping[str, Any], *, kind: str) -> None:
    _require(binding.get("kind") == kind, f"artifact binding kind mismatch: {kind}")
    source_dir = Path(str(binding.get("source_dir", "")))
    _require(source_dir.is_dir(), f"artifact source dir missing: {source_dir}")
    for file_binding in _mapping(binding.get("files")).values():
        _validate_file_binding(_mapping(file_binding))


def load_weight_search_space_config(
    path: Path = DEFAULT_WEIGHT_SEARCH_SPACE_CONFIG_PATH,
) -> dict[str, Any]:
    payload = st._load_yaml_mapping(path)
    _assert_weight_search_safety(_mapping(payload.get("safety")))
    return payload


def validate_weight_search_space_config(
    path: Path = DEFAULT_WEIGHT_SEARCH_SPACE_CONFIG_PATH,
) -> dict[str, Any]:
    payload = st._load_yaml_mapping(path)
    families = _enabled_families(payload)
    checks = [
        st._check("schema_version", payload.get("schema_version") == 1, ""),
        st._check(
            "research_screening_only",
            _text(_mapping(payload.get("search")).get("mode")) == "research_screening_only",
            "",
        ),
        st._check(
            "required_families_covered",
            set(SEARCH_REQUIRED_FAMILIES).issubset(set(families)),
            ",".join(families),
        ),
        st._check(
            "initial_batch_size_bounded",
            50 <= int(_float(_mapping(payload.get("max_variants")).get("initial_batch"), 0)) <= 80,
            _text(_mapping(payload.get("max_variants")).get("initial_batch")),
        ),
        st._check(
            "expanded_batch_size_bounded",
            int(_float(_mapping(payload.get("max_variants")).get("expanded_batch"), 0)) <= 200,
            _text(_mapping(payload.get("max_variants")).get("expanded_batch")),
        ),
        st._check(
            "safety_locked", _weight_search_safety_locked(_mapping(payload.get("safety"))), ""
        ),
    ]
    return st._validation_payload(
        "etf_dynamic_v3_weight_search_space_config_validation",
        "weight_search_space_v2",
        checks,
        extra={"config_path": str(path)},
    )


def run_weight_search_space_validation(
    *,
    config_path: Path = DEFAULT_WEIGHT_SEARCH_SPACE_CONFIG_PATH,
    output_dir: Path = DEFAULT_WEIGHT_SEARCH_SPACE_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    config = load_weight_search_space_config(config_path)
    config = json.loads(json.dumps(config, ensure_ascii=False, sort_keys=True, default=str))
    validation = validate_weight_search_space_config(config_path)
    _require(validation.get("status") == "PASS", "weight search space config validation failed")
    inventory = _search_family_inventory(config)
    search = _mapping(config.get("search"))
    search_space_id = _stable_id(
        "weight-search-space",
        search.get("name"),
        config_path,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / search_space_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weight_search_space_manifest",
        "search_space_id": root.name,
        "search_name": search.get("name"),
        "source_backfill_id": search.get("source_backfill_id"),
        "generated_at": generated.isoformat(),
        "status": validation["status"],
        "config_path": str(config_path),
        "market_regime": "ai_after_chatgpt",
        "anchor_event": "ChatGPT public launch",
        "anchor_date": "2022-11-30",
        "default_backtest_start": st.AI_AFTER_CHATGPT_START.isoformat(),
        "families": _enabled_families(config),
        "max_variants": _mapping(config.get("max_variants")),
        "weight_search_space_manifest_path": str(root / "weight_search_space_manifest.json"),
        "normalized_search_space_path": str(root / "normalized_search_space.yaml"),
        "search_family_inventory_path": str(root / "search_family_inventory.json"),
        "weight_search_space_report_path": str(root / "weight_search_space_report.md"),
        "weight_search_space_input_snapshot_path": str(
            root / "weight_search_space_input_snapshot.json"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    _write_json(root / "weight_search_space_manifest.json", manifest)
    _write_text(
        root / "normalized_search_space.yaml",
        yaml.safe_dump(config, sort_keys=False, allow_unicode=True),
    )
    _write_json(root / "search_family_inventory.json", inventory)
    _write_text(
        root / "weight_search_space_report.md",
        render_weight_search_space_report(manifest, inventory),
    )
    snapshot = {
        "schema_version": SEARCH_SPACE_INPUT_SCHEMA,
        "generated_at": generated.isoformat(),
        "config_path": str(config_path),
        "config_source": _file_binding(config_path),
        "normalized_search_space": config,
        "config_validation": validation,
        "search_space_id": root.name,
        "view_hashes": _view_hashes(
            root,
            (
                "weight_search_space_manifest.json",
                "normalized_search_space.yaml",
                "search_family_inventory.json",
                "weight_search_space_report.md",
            ),
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    _write_snapshot(root / "weight_search_space_input_snapshot.json", snapshot)
    _write_latest_pointer(
        "latest_weight_search_space",
        root.name,
        root / "weight_search_space_manifest.json",
    )
    return {
        "search_space_id": root.name,
        "search_space_dir": root,
        "manifest": manifest,
        "normalized_search_space": config,
        "search_family_inventory": inventory,
        "validation": validation,
    }


def weight_search_space_report_payload(
    *,
    search_space_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_WEIGHT_SEARCH_SPACE_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=search_space_id,
        latest_pointer="latest_weight_search_space",
        latest=latest,
        output_dir=output_dir,
        required_name="weight_search_space_manifest.json",
    )
    return {
        **_read_json(root / "weight_search_space_manifest.json"),
        "normalized_search_space": yaml.safe_load(
            (root / "normalized_search_space.yaml").read_text(encoding="utf-8")
        ),
        "search_family_inventory": _read_json(root / "search_family_inventory.json"),
        "input_snapshot": _read_optional_json(root / "weight_search_space_input_snapshot.json"),
        "search_space_dir": str(root),
    }


def validate_weight_search_space_artifact(
    *,
    search_space_id: str,
    output_dir: Path = DEFAULT_WEIGHT_SEARCH_SPACE_DIR,
) -> dict[str, Any]:
    root = output_dir / search_space_id
    manifest = _read_optional_json(root / "weight_search_space_manifest.json") or {}
    inventory = _read_optional_json(root / "search_family_inventory.json") or {}
    snapshot = _read_optional_json(root / "weight_search_space_input_snapshot.json") or {}
    checks = _required_file_checks(
        root,
        (
            "weight_search_space_manifest.json",
            "normalized_search_space.yaml",
            "search_family_inventory.json",
            "weight_search_space_report.md",
            "weight_search_space_input_snapshot.json",
        ),
    )
    snapshot_errors: list[str] = []
    expected_inventory: dict[str, Any] = {}
    expected_manifest: dict[str, Any] = {}
    expected_report = ""
    try:
        _require(snapshot.get("schema_version") == SEARCH_SPACE_INPUT_SCHEMA, "snapshot schema")
        _require(snapshot.get("search_space_id") == search_space_id, "snapshot id mismatch")
        config_source = _mapping(snapshot.get("config_source"))
        _validate_file_binding(config_source)
        config_path = Path(str(config_source.get("path", "")))
        live_config = load_weight_search_space_config(config_path)
        _require(live_config == snapshot.get("normalized_search_space"), "live config drift")
        live_validation = validate_weight_search_space_config(
            Path(str(snapshot.get("config_path", "")))
        )
        _require(live_validation.get("status") == "PASS", "live config validation failed")
        _require(
            live_validation == snapshot.get("config_validation"),
            "config validation commitment drift",
        )
        _require(_payload_experiment_safe(snapshot), "snapshot safety fields invalid")
        config = _mapping(snapshot.get("normalized_search_space"))
        expected_inventory = json.loads(
            json.dumps(_search_family_inventory(config), ensure_ascii=False, default=str)
        )
        search = _mapping(config.get("search"))
        expected_manifest = {
            "schema_version": st.SCHEMA_VERSION,
            "report_type": "etf_dynamic_v3_weight_search_space_manifest",
            "search_space_id": search_space_id,
            "search_name": search.get("name"),
            "source_backfill_id": search.get("source_backfill_id"),
            "generated_at": snapshot.get("generated_at"),
            "status": "PASS",
            "config_path": snapshot.get("config_path"),
            "market_regime": "ai_after_chatgpt",
            "anchor_event": "ChatGPT public launch",
            "anchor_date": "2022-11-30",
            "default_backtest_start": st.AI_AFTER_CHATGPT_START.isoformat(),
            "families": _enabled_families(config),
            "max_variants": _mapping(config.get("max_variants")),
            "weight_search_space_manifest_path": str(root / "weight_search_space_manifest.json"),
            "normalized_search_space_path": str(root / "normalized_search_space.yaml"),
            "search_family_inventory_path": str(root / "search_family_inventory.json"),
            "weight_search_space_report_path": str(root / "weight_search_space_report.md"),
            "weight_search_space_input_snapshot_path": str(
                root / "weight_search_space_input_snapshot.json"
            ),
            **st.EXPERIMENT_FACTORY_SAFETY,
        }
        expected_report = render_weight_search_space_report(expected_manifest, expected_inventory)
        _require(
            yaml.safe_load((root / "normalized_search_space.yaml").read_text(encoding="utf-8"))
            == config,
            "normalized config view drift",
        )
        _require(
            not _validate_view_hashes(root, _mapping(snapshot.get("view_hashes"))),
            "view hash drift",
        )
    except Exception as exc:  # noqa: BLE001
        snapshot_errors.append(str(exc))
    families = _texts(manifest.get("families"))
    checks.extend(
        [
            st._check(
                "snapshot_and_live_config_valid", not snapshot_errors, "; ".join(snapshot_errors)
            ),
            st._check("manifest_content_derived", manifest == expected_manifest, ""),
            st._check("inventory_content_derived", inventory == expected_inventory, ""),
            st._check(
                "report_content_derived",
                (
                    (root / "weight_search_space_report.md").read_text(encoding="utf-8")
                    if (root / "weight_search_space_report.md").is_file()
                    else ""
                )
                == expected_report,
                "",
            ),
            st._check(
                "search_space_id_matches", manifest.get("search_space_id") == search_space_id, ""
            ),
            st._check(
                "required_families_visible",
                set(SEARCH_REQUIRED_FAMILIES).issubset(set(families)),
                "",
            ),
            st._check("family_inventory_present", bool(_records(inventory.get("families"))), ""),
            st._check("broker_forbidden", _payload_safe(manifest, inventory), ""),
            st._check(
                "experiment_safety_locked", _payload_experiment_safe(manifest, inventory), ""
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_weight_search_space_artifact_validation",
        search_space_id,
        checks,
    )


def build_weight_experiment_batch2(
    *,
    search_space_id: str | None = None,
    latest_search_space: bool = False,
    source_backfill_id: str | None = None,
    search_space_dir: Path = DEFAULT_WEIGHT_SEARCH_SPACE_DIR,
    output_dir: Path = DEFAULT_WEIGHT_EXPERIMENT_BATCH2_DIR,
    generated_at: datetime | None = None,
    expanded: bool = False,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    search_space = weight_search_space_report_payload(
        search_space_id=search_space_id,
        latest=latest_search_space,
        output_dir=search_space_dir,
    )
    resolved_search_space_id = str(search_space.get("search_space_id", ""))
    search_validation = validate_weight_search_space_artifact(
        search_space_id=resolved_search_space_id,
        output_dir=Path(str(search_space.get("search_space_dir", ""))).parent,
    )
    _require(search_validation.get("status") == "PASS", "source search space validation failed")
    search_source = _artifact_binding(
        kind="weight_search_space",
        artifact_id=resolved_search_space_id,
        root=Path(str(search_space.get("search_space_dir", ""))),
        names=(
            "weight_search_space_manifest.json",
            "normalized_search_space.yaml",
            "search_family_inventory.json",
            "weight_search_space_report.md",
            "weight_search_space_input_snapshot.json",
        ),
    )
    config = _mapping(search_space.get("normalized_search_space"))
    variants = _generate_batch2_variants(config, expanded=expanded)
    max_key = "expanded_batch" if expanded else "initial_batch"
    max_variants = int(_float(_mapping(config.get("max_variants")).get(max_key), len(variants)))
    variants = variants[:max_variants]
    if not expanded and len(variants) < 50:
        raise ValueError("Batch-2 initial matrix must contain at least 50 variants")
    coverage = _batch2_family_coverage(variants)
    search = _mapping(config.get("search"))
    resolved_source_backfill = source_backfill_id or _text(search.get("source_backfill_id"))
    matrix_id = _stable_id(
        "weight-experiment-batch2",
        search_space.get("search_space_id"),
        resolved_source_backfill,
        "expanded" if expanded else "initial",
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / matrix_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weight_experiment_batch2_manifest",
        "batch2_matrix_id": root.name,
        "matrix_id": root.name,
        "search_space_id": search_space.get("search_space_id"),
        "source_backfill_id": resolved_source_backfill,
        "generated_at": generated.isoformat(),
        "status": "PASS" if len(variants) >= (50 if not expanded else 1) else "FAIL",
        "market_regime": "ai_after_chatgpt",
        "requested_start_date": st.AI_AFTER_CHATGPT_START.isoformat(),
        "variant_count": len(variants),
        "expanded": expanded,
        "families_covered": coverage["families_covered"],
        "failure_modes_covered": coverage["failure_modes_covered"],
        "batch2_matrix_manifest_path": str(root / "batch2_matrix_manifest.json"),
        "batch2_variant_specs_path": str(root / "batch2_variant_specs.jsonl"),
        "batch2_family_coverage_path": str(root / "batch2_family_coverage.json"),
        "batch2_matrix_report_path": str(root / "batch2_matrix_report.md"),
        "weight_experiment_batch2_input_snapshot_path": str(
            root / "weight_experiment_batch2_input_snapshot.json"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    _write_json(root / "batch2_matrix_manifest.json", manifest)
    _write_jsonl(root / "batch2_variant_specs.jsonl", variants)
    _write_json(root / "batch2_family_coverage.json", coverage)
    _write_text(root / "batch2_matrix_report.md", render_batch2_matrix_report(manifest, coverage))
    snapshot = {
        "schema_version": MATRIX_INPUT_SCHEMA,
        "generated_at": generated.isoformat(),
        "search_source": search_source,
        "source_backfill_id": resolved_source_backfill,
        "expanded": expanded,
        "matrix_id": root.name,
        "view_hashes": _view_hashes(
            root,
            (
                "batch2_matrix_manifest.json",
                "batch2_variant_specs.jsonl",
                "batch2_family_coverage.json",
                "batch2_matrix_report.md",
            ),
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    _write_snapshot(root / "weight_experiment_batch2_input_snapshot.json", snapshot)
    pointer = "latest_weight_expanded_matrix" if expanded else "latest_weight_experiment_batch2"
    _write_latest_pointer(pointer, root.name, root / "batch2_matrix_manifest.json")
    return {
        "batch2_matrix_id": root.name,
        "matrix_id": root.name,
        "matrix_dir": root,
        "manifest": manifest,
        "variant_specs": variants,
        "family_coverage": coverage,
    }


def weight_experiment_batch2_report_payload(
    *,
    matrix_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_WEIGHT_EXPERIMENT_BATCH2_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=matrix_id,
        latest_pointer="latest_weight_experiment_batch2",
        latest=latest,
        output_dir=output_dir,
        required_name="batch2_matrix_manifest.json",
    )
    return {
        **_read_json(root / "batch2_matrix_manifest.json"),
        "variant_specs": _read_jsonl(root / "batch2_variant_specs.jsonl"),
        "family_coverage": _read_json(root / "batch2_family_coverage.json"),
        "input_snapshot": _read_optional_json(
            root / "weight_experiment_batch2_input_snapshot.json"
        ),
        "matrix_dir": str(root),
    }


def validate_weight_experiment_batch2_artifact(
    *,
    matrix_id: str,
    output_dir: Path = DEFAULT_WEIGHT_EXPERIMENT_BATCH2_DIR,
) -> dict[str, Any]:
    root = output_dir / matrix_id
    manifest = _read_optional_json(root / "batch2_matrix_manifest.json") or {}
    variants = _read_jsonl(root / "batch2_variant_specs.jsonl")
    coverage = _read_optional_json(root / "batch2_family_coverage.json") or {}
    snapshot = _read_optional_json(root / "weight_experiment_batch2_input_snapshot.json") or {}
    checks = _required_file_checks(
        root,
        (
            "batch2_matrix_manifest.json",
            "batch2_variant_specs.jsonl",
            "batch2_family_coverage.json",
            "batch2_matrix_report.md",
            "weight_experiment_batch2_input_snapshot.json",
        ),
    )
    snapshot_errors: list[str] = []
    expected_variants: list[dict[str, Any]] = []
    expected_coverage: dict[str, Any] = {}
    expected_manifest: dict[str, Any] = {}
    expected_report = ""
    try:
        _require(snapshot.get("schema_version") == MATRIX_INPUT_SCHEMA, "snapshot schema")
        _require(snapshot.get("matrix_id") == matrix_id, "snapshot id mismatch")
        _require(_payload_experiment_safe(snapshot), "snapshot safety fields invalid")
        search_source = _mapping(snapshot.get("search_source"))
        _validate_artifact_binding(search_source, kind="weight_search_space")
        source_id = str(search_source.get("artifact_id", ""))
        source_dir = Path(str(search_source.get("source_dir", "")))
        source_validation = validate_weight_search_space_artifact(
            search_space_id=source_id, output_dir=source_dir.parent
        )
        _require(source_validation.get("status") == "PASS", "live search source validation failed")
        source_payload = weight_search_space_report_payload(
            search_space_id=source_id, output_dir=source_dir.parent
        )
        config = _mapping(source_payload.get("normalized_search_space"))
        expanded = snapshot.get("expanded") is True
        expected_variants = _generate_batch2_variants(config, expanded=expanded)
        max_key = "expanded_batch" if expanded else "initial_batch"
        max_variants = int(
            _float(_mapping(config.get("max_variants")).get(max_key), len(expected_variants))
        )
        expected_variants = expected_variants[:max_variants]
        expected_coverage = _batch2_family_coverage(expected_variants)
        expected_manifest = {
            "schema_version": st.SCHEMA_VERSION,
            "report_type": "etf_dynamic_v3_weight_experiment_batch2_manifest",
            "batch2_matrix_id": matrix_id,
            "matrix_id": matrix_id,
            "search_space_id": source_id,
            "source_backfill_id": snapshot.get("source_backfill_id"),
            "generated_at": snapshot.get("generated_at"),
            "status": "PASS" if len(expected_variants) >= (1 if expanded else 50) else "FAIL",
            "market_regime": "ai_after_chatgpt",
            "requested_start_date": st.AI_AFTER_CHATGPT_START.isoformat(),
            "variant_count": len(expected_variants),
            "expanded": expanded,
            "families_covered": expected_coverage["families_covered"],
            "failure_modes_covered": expected_coverage["failure_modes_covered"],
            "batch2_matrix_manifest_path": str(root / "batch2_matrix_manifest.json"),
            "batch2_variant_specs_path": str(root / "batch2_variant_specs.jsonl"),
            "batch2_family_coverage_path": str(root / "batch2_family_coverage.json"),
            "batch2_matrix_report_path": str(root / "batch2_matrix_report.md"),
            "weight_experiment_batch2_input_snapshot_path": str(
                root / "weight_experiment_batch2_input_snapshot.json"
            ),
            **st.EXPERIMENT_FACTORY_SAFETY,
        }
        expected_report = render_batch2_matrix_report(expected_manifest, expected_coverage)
        _require(
            not _validate_view_hashes(root, _mapping(snapshot.get("view_hashes"))),
            "view hash drift",
        )
    except Exception as exc:  # noqa: BLE001
        snapshot_errors.append(str(exc))
    checks.extend(
        [
            st._check(
                "snapshot_and_live_source_valid", not snapshot_errors, "; ".join(snapshot_errors)
            ),
            st._check("manifest_content_derived", manifest == expected_manifest, ""),
            st._check("variants_content_derived", variants == expected_variants, ""),
            st._check("coverage_content_derived", coverage == expected_coverage, ""),
            st._check(
                "report_content_derived",
                (
                    (root / "batch2_matrix_report.md").read_text(encoding="utf-8")
                    if (root / "batch2_matrix_report.md").is_file()
                    else ""
                )
                == expected_report,
                "",
            ),
            st._check("matrix_id_matches", manifest.get("batch2_matrix_id") == matrix_id, ""),
            st._check("variants_present", bool(variants), ""),
            st._check("variant_count_bounded", 1 <= len(variants) <= 200, str(len(variants))),
            st._check(
                "initial_batch_minimum_met_or_expanded",
                manifest.get("expanded") is True or len(variants) >= 50,
                str(len(variants)),
            ),
            st._check(
                "covers_at_least_8_families",
                len(_texts(coverage.get("families_covered"))) >= 8,
                ",".join(_texts(coverage.get("families_covered"))),
            ),
            st._check(
                "variants_have_failure_modes",
                all(_texts(row.get("target_failure_modes")) for row in variants),
                "",
            ),
            st._check(
                "variants_have_expected_tradeoffs",
                all(
                    _texts(row.get("expected_benefit")) and _texts(row.get("expected_cost"))
                    for row in variants
                ),
                "",
            ),
            st._check(
                "not_formal_methods",
                all(row.get("not_formal_research_method") is True for row in variants),
                "",
            ),
            st._check("broker_forbidden", _payload_safe(manifest, coverage, *variants), ""),
            st._check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, coverage, *variants),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_weight_experiment_batch2_validation", matrix_id, checks
    )


def run_weight_batch_backfill(
    *,
    matrix_id: str,
    matrix_dir: Path = DEFAULT_WEIGHT_EXPERIMENT_BATCH2_DIR,
    baseline_backfill_dir: Path = st.DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
    output_dir: Path = DEFAULT_WEIGHT_BATCH_BACKFILL_DIR,
    price_cache_path: Path | None = None,
    rates_cache_path: Path = st.DEFAULT_RATES_CACHE_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    matrix = _batch2_matrix_payload(matrix_id=matrix_id, output_dir=matrix_dir)
    matrix_validation = validate_weight_experiment_batch2_artifact(
        matrix_id=matrix_id, output_dir=matrix_dir
    )
    _require(matrix_validation.get("status") == "PASS", "source matrix validation failed")
    matrix_source = _artifact_binding(
        kind="weight_experiment_batch2",
        artifact_id=matrix_id,
        root=matrix_dir / matrix_id,
        names=(
            "batch2_matrix_manifest.json",
            "batch2_variant_specs.jsonl",
            "batch2_family_coverage.json",
            "batch2_matrix_report.md",
            "weight_experiment_batch2_input_snapshot.json",
        ),
    )
    source_backfill_id = _text(matrix.get("source_backfill_id"))
    backfill = st.paper_shadow_backfill_report_payload(
        backfill_id=source_backfill_id,
        output_dir=baseline_backfill_dir,
    )
    source_backfill_validation = st.validate_paper_shadow_backfill_artifact(
        backfill_id=source_backfill_id, output_dir=baseline_backfill_dir
    )
    _require(
        source_backfill_validation.get("status") == "PASS",
        "source paper backfill validation failed",
    )
    source_backfill_source = _artifact_binding(
        kind="paper_shadow_backfill",
        artifact_id=source_backfill_id,
        root=baseline_backfill_dir / source_backfill_id,
        names=(
            "paper_shadow_backfill_input_snapshot.json",
            "paper_shadow_backfill_manifest.json",
            "backfill_rebalance_calendar.json",
            "backfill_method_states.jsonl",
            "backfill_trade_ledger.jsonl",
            "backfill_data_quality.json",
            "paper_shadow_backfill_report.md",
            "validate_data_quality_report.md",
        ),
    )
    baseline_states = _records(backfill.get("backfill_method_states"))
    config = st._load_backfill_config_from_manifest(backfill)
    start = max(
        _coerce_date(backfill.get("date_start"), st.AI_AFTER_CHATGPT_START),
        st.AI_AFTER_CHATGPT_START,
    )
    requested_end = _coerce_date(backfill.get("date_end"), generated.date())
    source = _mapping(config.get("source"))
    symbols = st._symbols_from_state_paths(baseline_states)
    prices_path = price_cache_path or st._resolve_project_path(
        source.get("price_cache_path"),
        st.DEFAULT_PRICE_CACHE_PATH,
    )
    price_source = _file_binding(prices_path)
    rates_source = _file_binding(rates_cache_path)
    pivot = st._load_price_pivot(prices_path, symbols, start)
    latest_valid_as_of = _latest_common_price_date(pivot, symbols)
    end = min(requested_end, latest_valid_as_of, generated.date())
    used_latest_valid_as_of = end < requested_end
    pivot = pivot.loc[(pivot.index.date >= start) & (pivot.index.date <= end)]
    quality_as_of = max(end, generated.date())
    quality = st._run_data_quality_gate(
        price_cache_path=prices_path,
        rates_cache_path=rates_cache_path,
        expected_symbols=symbols,
        as_of=quality_as_of,
    )
    if not quality.passed:
        raise RuntimeError(f"data quality gate failed for historical backfill: {quality.status}")
    returns = pivot.pct_change().fillna(0.0)
    labels = {
        idx.date().isoformat(): st._risk_capped_regime_context_for_return(row, config)
        for idx, row in returns.iterrows()
    }
    variant_specs = _records(matrix.get("variant_specs"))
    variant_states: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    for variant in variant_specs:
        try:
            variant_states.extend(
                st._run_variant_weight_path(
                    variant=variant,
                    baseline_states=baseline_states,
                    returns=returns,
                    labels=labels,
                    config=config,
                )
            )
        except Exception as exc:  # noqa: BLE001
            failed.append({"variant_id": _text(variant.get("variant_id")), "error": str(exc)})
    performance = st._variant_performance_metrics(variant_states, baseline_states)
    regime = st._variant_regime_metrics(variant_states, baseline_states, labels, config)
    stability = st._variant_stability_metrics(variant_states, baseline_states, config)
    churn = _variant_churn_metrics(variant_states, stability)
    lag = _variant_lag_metrics(regime)
    backfill_id = _stable_id(
        "weight-batch-backfill", matrix_id, end.isoformat(), generated.isoformat()
    )
    root = _unique_dir(output_dir / backfill_id)
    root.mkdir(parents=True, exist_ok=False)
    quality_report_path = root / "validate_data_quality_report.md"
    progress = {
        "schema_version": st.SCHEMA_VERSION,
        "batch_backfill_id": root.name,
        "variants_total": len(variant_specs),
        "variants_completed": len({row.get("variant_id") for row in performance}),
        "variants_failed": len(failed),
        "failed_variants": failed,
        "date_start": start.isoformat(),
        "date_end": end.isoformat(),
        "requested_date_end": requested_end.isoformat(),
        "latest_valid_as_of": latest_valid_as_of.isoformat(),
        "data_quality": quality.status,
        "data_quality_as_of": quality_as_of.isoformat(),
        "validate_data_quality_report_path": str(quality_report_path),
        "used_latest_valid_as_of": used_latest_valid_as_of,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weight_batch_backfill_manifest",
        "batch_backfill_id": root.name,
        "batch2_matrix_id": matrix_id,
        "matrix_id": matrix_id,
        "source_backfill_id": source_backfill_id,
        "generated_at": generated.isoformat(),
        "status": (
            "PASS"
            if not failed and performance
            else "PASS_WITH_WARNINGS"
            if performance
            else "FAIL"
        ),
        "market_regime": backfill.get("market_regime", "ai_after_chatgpt"),
        "date_start": start.isoformat(),
        "date_end": end.isoformat(),
        "requested_start_date": backfill.get("requested_start_date", start.isoformat()),
        "requested_end_date": requested_end.isoformat(),
        "latest_valid_as_of": latest_valid_as_of.isoformat(),
        "data_quality_status": quality.status,
        "data_quality_as_of": quality_as_of.isoformat(),
        "data_quality_checked_at": quality.checked_at.isoformat(),
        "validate_data_quality_report_path": str(quality_report_path),
        "used_latest_valid_as_of": used_latest_valid_as_of,
        "variants_total": len(variant_specs),
        "variants_completed": progress["variants_completed"],
        "variants_failed": len(failed),
        "batch_backfill_manifest_path": str(root / "batch_backfill_manifest.json"),
        "batch_backfill_progress_path": str(root / "batch_backfill_progress.json"),
        "variant_weight_paths_path": str(root / "variant_weight_paths.jsonl"),
        "variant_performance_metrics_path": str(root / "variant_performance_metrics.jsonl"),
        "variant_regime_metrics_path": str(root / "variant_regime_metrics.jsonl"),
        "variant_stability_metrics_path": str(root / "variant_stability_metrics.jsonl"),
        "variant_churn_metrics_path": str(root / "variant_churn_metrics.jsonl"),
        "variant_lag_metrics_path": str(root / "variant_lag_metrics.jsonl"),
        "batch_backfill_report_path": str(root / "batch_backfill_report.md"),
        "weight_batch_backfill_input_snapshot_path": str(
            root / "weight_batch_backfill_input_snapshot.json"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    _write_json(root / "batch_backfill_manifest.json", manifest)
    _write_json(root / "batch_backfill_progress.json", progress)
    write_data_quality_report(quality, quality_report_path)
    _write_jsonl(root / "variant_weight_paths.jsonl", variant_states)
    _write_jsonl(root / "variant_performance_metrics.jsonl", performance)
    _write_jsonl(root / "variant_regime_metrics.jsonl", regime)
    _write_jsonl(root / "variant_stability_metrics.jsonl", stability)
    _write_jsonl(root / "variant_churn_metrics.jsonl", churn)
    _write_jsonl(root / "variant_lag_metrics.jsonl", lag)
    _write_text(root / "batch_backfill_report.md", render_batch_backfill_report(manifest, progress))
    snapshot = {
        "schema_version": BACKFILL_INPUT_SCHEMA,
        "generated_at": generated.isoformat(),
        "backfill_id": root.name,
        "matrix_source": matrix_source,
        "paper_backfill_source": source_backfill_source,
        "price_source": price_source,
        "rates_source": rates_source,
        "quality_checked_at": quality.checked_at.isoformat(),
        "view_hashes": _view_hashes(
            root,
            (
                "batch_backfill_manifest.json",
                "batch_backfill_progress.json",
                "validate_data_quality_report.md",
                "variant_weight_paths.jsonl",
                "variant_performance_metrics.jsonl",
                "variant_regime_metrics.jsonl",
                "variant_stability_metrics.jsonl",
                "variant_churn_metrics.jsonl",
                "variant_lag_metrics.jsonl",
                "batch_backfill_report.md",
            ),
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    _write_snapshot(root / "weight_batch_backfill_input_snapshot.json", snapshot)
    _write_latest_pointer(
        "latest_weight_batch_backfill", root.name, root / "batch_backfill_manifest.json"
    )
    return {
        "batch_backfill_id": root.name,
        "backfill_id": root.name,
        "backfill_dir": root,
        "manifest": manifest,
        "progress": progress,
        "variant_weight_paths": variant_states,
        "variant_performance_metrics": performance,
        "variant_regime_metrics": regime,
        "variant_stability_metrics": stability,
        "variant_churn_metrics": churn,
        "variant_lag_metrics": lag,
    }


def resume_weight_batch_backfill(
    *,
    backfill_id: str,
    output_dir: Path = DEFAULT_WEIGHT_BATCH_BACKFILL_DIR,
) -> dict[str, Any]:
    validation = validate_weight_batch_backfill_artifact(
        backfill_id=backfill_id, output_dir=output_dir
    )
    _require(validation.get("status") == "PASS", "backfill validation failed before resume")
    payload = weight_batch_backfill_report_payload(backfill_id=backfill_id, output_dir=output_dir)
    progress = _mapping(payload.get("batch_backfill_progress"))
    return {
        "batch_backfill_id": backfill_id,
        "resume_status": (
            "ALREADY_COMPLETE"
            if int(_float(progress.get("variants_completed")))
            >= int(_float(progress.get("variants_total")))
            else "PARTIAL_COMPLETION_REVIEW_REQUIRED"
        ),
        "progress": progress,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def weight_batch_backfill_report_payload(
    *,
    backfill_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_WEIGHT_BATCH_BACKFILL_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=backfill_id,
        latest_pointer="latest_weight_batch_backfill",
        latest=latest,
        output_dir=output_dir,
        required_name="batch_backfill_manifest.json",
    )
    return {
        **_read_json(root / "batch_backfill_manifest.json"),
        "batch_backfill_progress": _read_json(root / "batch_backfill_progress.json"),
        "variant_weight_paths": _read_jsonl(root / "variant_weight_paths.jsonl"),
        "variant_performance_metrics": _read_jsonl(root / "variant_performance_metrics.jsonl"),
        "variant_regime_metrics": _read_jsonl(root / "variant_regime_metrics.jsonl"),
        "variant_stability_metrics": _read_jsonl(root / "variant_stability_metrics.jsonl"),
        "variant_churn_metrics": _read_jsonl(root / "variant_churn_metrics.jsonl"),
        "variant_lag_metrics": _read_jsonl(root / "variant_lag_metrics.jsonl"),
        "input_snapshot": _read_optional_json(root / "weight_batch_backfill_input_snapshot.json"),
        "backfill_dir": str(root),
    }


def _rebuild_weight_batch_backfill_views(
    snapshot: Mapping[str, Any], *, root: Path, backfill_id: str
) -> dict[str, bytes]:
    _require(snapshot.get("schema_version") == BACKFILL_INPUT_SCHEMA, "snapshot schema")
    _require(snapshot.get("backfill_id") == backfill_id, "snapshot id mismatch")
    _require(_payload_experiment_safe(snapshot), "snapshot safety fields invalid")
    matrix_source = _mapping(snapshot.get("matrix_source"))
    paper_source = _mapping(snapshot.get("paper_backfill_source"))
    _validate_artifact_binding(matrix_source, kind="weight_experiment_batch2")
    _validate_artifact_binding(paper_source, kind="paper_shadow_backfill")
    _validate_file_binding(_mapping(snapshot.get("price_source")))
    _validate_file_binding(_mapping(snapshot.get("rates_source")))
    matrix_id = str(matrix_source.get("artifact_id", ""))
    matrix_root = Path(str(matrix_source.get("source_dir", "")))
    matrix_validation = validate_weight_experiment_batch2_artifact(
        matrix_id=matrix_id, output_dir=matrix_root.parent
    )
    _require(matrix_validation.get("status") == "PASS", "live matrix validation failed")
    matrix = _batch2_matrix_payload(matrix_id=matrix_id, output_dir=matrix_root.parent)
    source_backfill_id = str(paper_source.get("artifact_id", ""))
    paper_root = Path(str(paper_source.get("source_dir", "")))
    paper_validation = st.validate_paper_shadow_backfill_artifact(
        backfill_id=source_backfill_id, output_dir=paper_root.parent
    )
    _require(paper_validation.get("status") == "PASS", "live paper backfill validation failed")
    backfill = st.paper_shadow_backfill_report_payload(
        backfill_id=source_backfill_id, output_dir=paper_root.parent
    )
    generated = datetime.fromisoformat(str(snapshot.get("generated_at", "")))
    baseline_states = _records(backfill.get("backfill_method_states"))
    config = st._load_backfill_config_from_manifest(backfill)
    start = max(
        _coerce_date(backfill.get("date_start"), st.AI_AFTER_CHATGPT_START),
        st.AI_AFTER_CHATGPT_START,
    )
    requested_end = _coerce_date(backfill.get("date_end"), generated.date())
    symbols = st._symbols_from_state_paths(baseline_states)
    prices_path = Path(str(_mapping(snapshot.get("price_source")).get("path", "")))
    rates_path = Path(str(_mapping(snapshot.get("rates_source")).get("path", "")))
    pivot = st._load_price_pivot(prices_path, symbols, start)
    latest_valid_as_of = _latest_common_price_date(pivot, symbols)
    end = min(requested_end, latest_valid_as_of, generated.date())
    used_latest_valid_as_of = end < requested_end
    pivot = pivot.loc[(pivot.index.date >= start) & (pivot.index.date <= end)]
    quality_as_of = max(end, generated.date())
    quality = st._run_data_quality_gate(
        price_cache_path=prices_path,
        rates_cache_path=rates_path,
        expected_symbols=symbols,
        as_of=quality_as_of,
    )
    _require(quality.passed, f"live data quality failed: {quality.status}")
    quality = replace(
        quality, checked_at=datetime.fromisoformat(str(snapshot.get("quality_checked_at", "")))
    )
    returns = pivot.pct_change().fillna(0.0)
    labels = {
        idx.date().isoformat(): st._risk_capped_regime_context_for_return(row, config)
        for idx, row in returns.iterrows()
    }
    variant_specs = _records(matrix.get("variant_specs"))
    variant_states: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    for variant in variant_specs:
        try:
            variant_states.extend(
                st._run_variant_weight_path(
                    variant=variant,
                    baseline_states=baseline_states,
                    returns=returns,
                    labels=labels,
                    config=config,
                )
            )
        except Exception as exc:  # noqa: BLE001
            failed.append({"variant_id": _text(variant.get("variant_id")), "error": str(exc)})
    performance = st._variant_performance_metrics(variant_states, baseline_states)
    regime = st._variant_regime_metrics(variant_states, baseline_states, labels, config)
    stability = st._variant_stability_metrics(variant_states, baseline_states, config)
    churn = _variant_churn_metrics(variant_states, stability)
    lag = _variant_lag_metrics(regime)
    quality_report_path = root / "validate_data_quality_report.md"
    progress = {
        "schema_version": st.SCHEMA_VERSION,
        "batch_backfill_id": backfill_id,
        "variants_total": len(variant_specs),
        "variants_completed": len({row.get("variant_id") for row in performance}),
        "variants_failed": len(failed),
        "failed_variants": failed,
        "date_start": start.isoformat(),
        "date_end": end.isoformat(),
        "requested_date_end": requested_end.isoformat(),
        "latest_valid_as_of": latest_valid_as_of.isoformat(),
        "data_quality": quality.status,
        "data_quality_as_of": quality_as_of.isoformat(),
        "validate_data_quality_report_path": str(quality_report_path),
        "used_latest_valid_as_of": used_latest_valid_as_of,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weight_batch_backfill_manifest",
        "batch_backfill_id": backfill_id,
        "batch2_matrix_id": matrix_id,
        "matrix_id": matrix_id,
        "source_backfill_id": source_backfill_id,
        "generated_at": generated.isoformat(),
        "status": "PASS"
        if not failed and performance
        else "PASS_WITH_WARNINGS"
        if performance
        else "FAIL",
        "market_regime": backfill.get("market_regime", "ai_after_chatgpt"),
        "date_start": start.isoformat(),
        "date_end": end.isoformat(),
        "requested_start_date": backfill.get("requested_start_date", start.isoformat()),
        "requested_end_date": requested_end.isoformat(),
        "latest_valid_as_of": latest_valid_as_of.isoformat(),
        "data_quality_status": quality.status,
        "data_quality_as_of": quality_as_of.isoformat(),
        "data_quality_checked_at": quality.checked_at.isoformat(),
        "validate_data_quality_report_path": str(quality_report_path),
        "used_latest_valid_as_of": used_latest_valid_as_of,
        "variants_total": len(variant_specs),
        "variants_completed": progress["variants_completed"],
        "variants_failed": len(failed),
        "batch_backfill_manifest_path": str(root / "batch_backfill_manifest.json"),
        "batch_backfill_progress_path": str(root / "batch_backfill_progress.json"),
        "variant_weight_paths_path": str(root / "variant_weight_paths.jsonl"),
        "variant_performance_metrics_path": str(root / "variant_performance_metrics.jsonl"),
        "variant_regime_metrics_path": str(root / "variant_regime_metrics.jsonl"),
        "variant_stability_metrics_path": str(root / "variant_stability_metrics.jsonl"),
        "variant_churn_metrics_path": str(root / "variant_churn_metrics.jsonl"),
        "variant_lag_metrics_path": str(root / "variant_lag_metrics.jsonl"),
        "batch_backfill_report_path": str(root / "batch_backfill_report.md"),
        "weight_batch_backfill_input_snapshot_path": str(
            root / "weight_batch_backfill_input_snapshot.json"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    return {
        "batch_backfill_manifest.json": _json_bytes(manifest),
        "batch_backfill_progress.json": _json_bytes(progress),
        "validate_data_quality_report.md": render_data_quality_report(quality).encode("utf-8"),
        "variant_weight_paths.jsonl": _jsonl_bytes(variant_states),
        "variant_performance_metrics.jsonl": _jsonl_bytes(performance),
        "variant_regime_metrics.jsonl": _jsonl_bytes(regime),
        "variant_stability_metrics.jsonl": _jsonl_bytes(stability),
        "variant_churn_metrics.jsonl": _jsonl_bytes(churn),
        "variant_lag_metrics.jsonl": _jsonl_bytes(lag),
        "batch_backfill_report.md": _text_file_bytes(
            render_batch_backfill_report(manifest, progress)
        ),
    }


def validate_weight_batch_backfill_artifact(
    *,
    backfill_id: str,
    output_dir: Path = DEFAULT_WEIGHT_BATCH_BACKFILL_DIR,
) -> dict[str, Any]:
    root = output_dir / backfill_id
    manifest = _read_optional_json(root / "batch_backfill_manifest.json") or {}
    progress = _read_optional_json(root / "batch_backfill_progress.json") or {}
    snapshot = _read_optional_json(root / "weight_batch_backfill_input_snapshot.json") or {}
    performance = _read_jsonl(root / "variant_performance_metrics.jsonl")
    variants = {str(row.get("variant_id")) for row in performance}
    regime = _read_jsonl(root / "variant_regime_metrics.jsonl")
    stability = _read_jsonl(root / "variant_stability_metrics.jsonl")
    checks = _required_file_checks(
        root,
        (
            "batch_backfill_manifest.json",
            "batch_backfill_progress.json",
            "variant_weight_paths.jsonl",
            "variant_performance_metrics.jsonl",
            "variant_regime_metrics.jsonl",
            "variant_stability_metrics.jsonl",
            "variant_churn_metrics.jsonl",
            "variant_lag_metrics.jsonl",
            "batch_backfill_report.md",
            "validate_data_quality_report.md",
            "weight_batch_backfill_input_snapshot.json",
        ),
    )
    rebuild_errors: list[str] = []
    rebuild_drift: list[str] = []
    try:
        rebuild_drift = _validate_view_hashes(root, _mapping(snapshot.get("view_hashes")))
        if not rebuild_drift:
            expected_views = _rebuild_weight_batch_backfill_views(
                snapshot, root=root, backfill_id=backfill_id
            )
            rebuild_drift = [
                name
                for name, expected in expected_views.items()
                if not (root / name).is_file() or (root / name).read_bytes() != expected
            ]
    except Exception as exc:  # noqa: BLE001
        rebuild_errors.append(str(exc))
    checks.extend(
        [
            st._check(
                "snapshot_and_live_sources_valid",
                not rebuild_errors,
                "; ".join(rebuild_errors),
            ),
            st._check(
                "all_views_content_derived",
                not rebuild_drift,
                ",".join(rebuild_drift),
            ),
            st._check("backfill_id_matches", manifest.get("batch_backfill_id") == backfill_id, ""),
            st._check("performance_metrics_present", bool(performance), ""),
            st._check(
                "data_quality_visible",
                manifest.get("data_quality_status") in {"PASS", "PASS_WITH_WARNINGS"},
                _text(manifest.get("data_quality_status")),
            ),
            st._check("latest_valid_as_of_visible", bool(manifest.get("latest_valid_as_of")), ""),
            st._check(
                "each_variant_has_regime_metrics",
                variants.issubset({str(row.get("variant_id")) for row in regime}),
                "",
            ),
            st._check(
                "each_variant_has_stability_metrics",
                variants.issubset({str(row.get("variant_id")) for row in stability}),
                "",
            ),
            st._check(
                "progress_counts_match",
                int(_float(progress.get("variants_completed"))) == len(variants),
                "",
            ),
            st._check("broker_forbidden", _payload_safe(manifest, progress, *performance), ""),
            st._check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, progress, *performance, *regime, *stability),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_weight_batch_backfill_validation", backfill_id, checks
    )
