from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import asdict
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any, cast

from ai_trading_system.config import (
    PROJECT_ROOT,
    configured_price_tickers,
    configured_rate_series,
    load_data_quality,
    load_universe,
)
from ai_trading_system.data.quality import DataQualityReport, validate_data_cache
from ai_trading_system.legacy_research_artifact_portable_lineage import (
    DEFAULT_HISTORICAL_SOURCE_ARCHIVE_POLICY_PATH,
    PortableLineageError,
    PortableLineageResolver,
    portable_lineage_failure_evidence,
    require_portable_lineage_archive_sidecar_pair,
)
from ai_trading_system.legacy_research_artifact_portable_lineage import (
    DEFAULT_POLICY_PATH as DEFAULT_PORTABLE_LINEAGE_POLICY_PATH,
)
from ai_trading_system.platform.artifacts.writer import (
    write_json_atomic,
    write_markdown_atomic,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_RESTART_POLICY_PATH = (
    PROJECT_ROOT / "config" / "research" / "strategy_research_restart_policy.yaml"
)
DEFAULT_PRIMARY_WINDOW_POLICY_PATH = (
    PROJECT_ROOT / "config" / "research" / "primary_research_window_policy.yaml"
)
DEFAULT_WINDOW_REGISTRY_PATH = (
    PROJECT_ROOT / "config" / "research" / "research_window_registry.yaml"
)
DEFAULT_PRICES_PATH = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv"
DEFAULT_SECONDARY_PRICES_PATH = PROJECT_ROOT / "data" / "raw" / "prices_marketstack_daily.csv"
DEFAULT_RATES_PATH = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv"
DEFAULT_RESTART_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_ops" / "strategy_restart"
DEFAULT_COST_POLICY_PATH = PROJECT_ROOT / "config" / "etf_portfolio" / "risk.yaml"
DEFAULT_EXECUTION_POLICY_PATH = PROJECT_ROOT / "config" / "etf_portfolio" / "strategy.yaml"

REPORT_TYPE = "strategy_research_restart_preflight"
SCHEMA_VERSION = "strategy_research_restart_preflight.v1"
SAFETY_BOUNDARY: dict[str, Any] = {
    "research_only": True,
    "validation_only": True,
    "observe_only": True,
    "production_effect": "none",
    "broker_action": "none",
    "promotion_gate_allowed": False,
    "paper_shadow_change_allowed": False,
    "production_weight_change_allowed": False,
    "shadow_enrollment_allowed": False,
    "automatic_candidate_generation_allowed": False,
    "manual_review_required": True,
}


class ResearchRestartError(ValueError):
    """Raised when the research restart contract cannot be verified."""


def load_restart_policy(path: Path = DEFAULT_RESTART_POLICY_PATH) -> dict[str, Any]:
    payload = safe_load_yaml_path(path)
    if not isinstance(payload, dict):
        raise ResearchRestartError(f"restart policy must be a mapping: {path}")
    if payload.get("schema_version") != "strategy_research_restart_policy.v1":
        raise ResearchRestartError("unsupported strategy research restart policy schema")
    if payload.get("status") != "owner_approved_pilot_baseline":
        raise ResearchRestartError("restart policy is not owner-approved pilot baseline")
    if payload.get("safety_boundary") != SAFETY_BOUNDARY:
        raise ResearchRestartError("restart policy safety boundary differs from required boundary")
    return payload


def run_research_restart_preflight(
    *,
    source_sweep_dir: Path,
    policy_path: Path = DEFAULT_RESTART_POLICY_PATH,
    primary_window_policy_path: Path = DEFAULT_PRIMARY_WINDOW_POLICY_PATH,
    window_registry_path: Path = DEFAULT_WINDOW_REGISTRY_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    secondary_prices_path: Path = DEFAULT_SECONDARY_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    download_manifest_path: Path | None = None,
    cost_policy_path: Path = DEFAULT_COST_POLICY_PATH,
    execution_policy_path: Path = DEFAULT_EXECUTION_POLICY_PATH,
    output_root: Path = DEFAULT_RESTART_OUTPUT_ROOT,
    as_of: date | None = None,
) -> dict[str, Any]:
    manifest_path = download_manifest_path or prices_path.parent / "download_manifest.csv"
    resolved_as_of = as_of or _max_csv_date(prices_path)
    if resolved_as_of is None:
        raise ResearchRestartError("cannot determine data-quality as-of date from prices")
    universe = load_universe()
    quality = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=configured_price_tickers(universe),
        expected_rate_series=configured_rate_series(universe),
        quality_config=load_data_quality(),
        as_of=resolved_as_of,
        manifest_path=manifest_path,
        secondary_prices_path=secondary_prices_path,
        require_secondary_prices=True,
    )
    payload = build_research_restart_preflight(
        source_sweep_dir=source_sweep_dir,
        policy_path=policy_path,
        primary_window_policy_path=primary_window_policy_path,
        window_registry_path=window_registry_path,
        prices_path=prices_path,
        secondary_prices_path=secondary_prices_path,
        rates_path=rates_path,
        download_manifest_path=manifest_path,
        cost_policy_path=cost_policy_path,
        execution_policy_path=execution_policy_path,
        data_quality_report=quality,
    )
    output_root.mkdir(parents=True, exist_ok=True)
    json_path = output_root / "strategy_research_restart_preflight.json"
    markdown_path = output_root / "strategy_research_restart_preflight.md"
    payload["artifact_paths"] = {
        "json": str(json_path),
        "markdown": str(markdown_path),
    }
    _write_json(json_path, payload)
    markdown = render_research_restart_preflight(payload)
    write_markdown_atomic(markdown_path, markdown)
    return payload


def build_research_restart_preflight(
    *,
    source_sweep_dir: Path,
    policy_path: Path,
    primary_window_policy_path: Path,
    window_registry_path: Path,
    prices_path: Path,
    secondary_prices_path: Path,
    rates_path: Path,
    download_manifest_path: Path,
    cost_policy_path: Path,
    execution_policy_path: Path,
    data_quality_report: DataQualityReport,
) -> dict[str, Any]:
    policy = load_restart_policy(policy_path)
    primary_policy = _load_mapping(primary_window_policy_path)
    registry = _load_mapping(window_registry_path)
    source_paths = {
        "sweep_manifest": source_sweep_dir / "sweep_manifest.json",
        "normalized_config": source_sweep_dir / "sweep_config.normalized.yaml",
        "candidate_results": source_sweep_dir / "candidate_results.jsonl",
    }
    source_manifest = _load_json_mapping(source_paths["sweep_manifest"])
    source_config = _load_mapping(source_paths["normalized_config"])
    cost_policy = _load_mapping(cost_policy_path)
    execution_policy = _load_mapping(execution_policy_path)
    transaction_costs = _mapping(cost_policy.get("transaction_costs"))
    signal_execution_lag_days = _mapping(execution_policy.get("model")).get(
        "signal_execution_lag_days"
    )
    window_semantics = _window_semantics_snapshot(policy, primary_policy, registry)
    dq = _data_quality_snapshot(data_quality_report)
    source_sweep_id = str(source_manifest.get("sweep_id", ""))
    r0 = _mapping(policy.get("r0_preflight"))
    allowed_dq = {str(value) for value in _sequence(r0.get("allowed_data_quality_status"))}
    checks = [
        _check("source_sweep_directory_matches_id", source_sweep_dir.name == source_sweep_id),
        _check(
            "source_evaluator_is_real",
            source_manifest.get("evaluator_mode") == r0.get("required_evaluator_mode"),
        ),
        _check("source_sweep_completed", source_manifest.get("status") == "completed"),
        _check(
            "source_candidate_results_nonempty",
            source_paths["candidate_results"].stat().st_size > 0,
        ),
        _check("data_quality_gate_passed", data_quality_report.passed),
        _check("data_quality_status_allowed", data_quality_report.status in allowed_dq),
        _check("window_semantics_consistent", window_semantics["status"] == "PASS"),
        _check(
            "source_window_is_explicit_primary_validated",
            _mapping(policy.get("research_lane")).get("source_window_role") == "primary_validated",
        ),
        _check(
            "cost_model_complete",
            all(key in transaction_costs for key in ("commission_bps", "slippage_bps")),
        ),
        _check(
            "execution_lag_explicit",
            isinstance(signal_execution_lag_days, int) and signal_execution_lag_days >= 1,
        ),
        _check("holdout_dates_valid", _holdout_dates_valid(source_config)),
        _check("safety_boundary_fixed", policy.get("safety_boundary") == SAFETY_BOUNDARY),
        _check(
            "new_candidate_generation_disabled",
            _mapping(policy.get("research_lane")).get("next_candidate_generation_allowed") is False,
        ),
    ]
    hard_checks_passed = all(bool(item["passed"]) for item in checks)
    fingerprints = _fingerprints(
        {
            "restart_policy": policy_path,
            "primary_window_policy": primary_window_policy_path,
            "window_registry": window_registry_path,
            "source_sweep_manifest": source_paths["sweep_manifest"],
            "source_normalized_config": source_paths["normalized_config"],
            "source_candidate_results": source_paths["candidate_results"],
            "prices": prices_path,
            "secondary_prices": secondary_prices_path,
            "rates": rates_path,
            "download_manifest": download_manifest_path,
            "cost_policy": cost_policy_path,
            "execution_policy": execution_policy_path,
        }
    )
    payload = {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "status": "PASS" if hard_checks_passed else "FAIL",
        "generated_at": datetime.now(UTC).isoformat(),
        "policy_id": policy["policy_id"],
        "policy_status": policy["status"],
        "research_execution_unblocked": hard_checks_passed,
        "source_sweep_id": source_sweep_id,
        "source_sweep_dir": str(source_sweep_dir),
        "window_semantics": window_semantics,
        "research_lane": policy["research_lane"],
        "data_quality_gate": dq,
        "pit_snapshot": {
            "prices": _summary_snapshot(data_quality_report.price_summary),
            "secondary_prices": _summary_snapshot(data_quality_report.secondary_price_summary),
            "rates": _summary_snapshot(data_quality_report.rate_summary),
            "download_manifest": _summary_snapshot(data_quality_report.manifest_summary),
        },
        "cost_and_execution_snapshot": {
            "transaction_costs": transaction_costs,
            "signal_execution_lag_days": signal_execution_lag_days,
            "cost_policy_path": str(cost_policy_path),
            "execution_policy_path": str(execution_policy_path),
        },
        "holdout_snapshot": _mapping(source_config.get("out_of_sample")),
        "walk_forward_policy_snapshot": _mapping(
            _mapping(policy.get("r1_evidence")).get("walk_forward")
        ),
        "robustness_policy_snapshot": _mapping(
            _mapping(policy.get("r1_evidence")).get("robustness")
        ),
        "forward_maturity_policy_snapshot": _mapping(
            _mapping(policy.get("r1_evidence")).get("forward_maturity")
        ),
        "input_fingerprints": fingerprints,
        "checks": checks,
        "failed_check_count": sum(1 for item in checks if not item["passed"]),
        "safety": dict(SAFETY_BOUNDARY),
        **SAFETY_BOUNDARY,
    }
    return cast(dict[str, Any], _jsonable(payload))


def validate_research_restart_preflight(
    *,
    artifact_path: Path,
    portable_lineage_sidecar_path: Path | None = None,
    portable_project_root: Path = PROJECT_ROOT,
    portable_lineage_policy_path: Path = DEFAULT_PORTABLE_LINEAGE_POLICY_PATH,
    historical_source_archive_manifest_path: Path | None = None,
    historical_source_archive_policy_path: Path = (DEFAULT_HISTORICAL_SOURCE_ARCHIVE_POLICY_PATH),
) -> dict[str, Any]:
    resolver: PortableLineageResolver | None = None
    require_portable_lineage_archive_sidecar_pair(
        portable_lineage_sidecar_path=portable_lineage_sidecar_path,
        historical_source_archive_manifest_path=historical_source_archive_manifest_path,
    )
    try:
        if portable_lineage_sidecar_path is not None:
            resolver = PortableLineageResolver(
                sidecar_path=portable_lineage_sidecar_path,
                subject_artifact_path=artifact_path,
                consumer="r0_preflight",
                project_root=portable_project_root,
                policy_path=portable_lineage_policy_path,
                historical_source_archive_manifest_path=(historical_source_archive_manifest_path),
                historical_source_archive_policy_path=historical_source_archive_policy_path,
            )
        payload = _load_json_mapping(artifact_path)
        checks = [
            _check("schema_version", payload.get("schema_version") == SCHEMA_VERSION),
            _check("report_type", payload.get("report_type") == REPORT_TYPE),
            _check("safety_boundary", payload.get("safety") == SAFETY_BOUNDARY),
            _check(
                "execution_gate_matches_checks",
                payload.get("research_execution_unblocked")
                is all(bool(item.get("passed")) for item in _records(payload.get("checks"))),
            ),
            _check(
                "status_matches_execution_gate",
                payload.get("status")
                == ("PASS" if payload.get("research_execution_unblocked") is True else "FAIL"),
            ),
            _check(
                "window_semantics_pass",
                _mapping(payload.get("window_semantics")).get("status") == "PASS",
            ),
            _check(
                "input_fingerprints_fresh",
                _fingerprints_fresh(payload.get("input_fingerprints"), resolver=resolver),
            ),
        ]
        markdown_path = _portable_path(
            Path(_mapping(payload.get("artifact_paths")).get("markdown", "")), resolver
        )
        checks.append(
            _check(
                "markdown_matches_payload",
                markdown_path.is_file()
                and markdown_path.read_text(encoding="utf-8")
                == render_research_restart_preflight(payload),
            )
        )
    except PortableLineageError as exc:
        assert portable_lineage_sidecar_path is not None
        return _portable_validation_failure(
            artifact_path=artifact_path,
            sidecar_path=portable_lineage_sidecar_path,
            error=exc,
        )
    passed = all(bool(item["passed"]) for item in checks)
    result: dict[str, Any] = {
        "schema_version": "strategy_research_restart_preflight_validation.v1",
        "report_type": "strategy_research_restart_preflight_validation",
        "status": "PASS" if passed else "FAIL",
        "artifact_path": str(artifact_path),
        "checks": checks,
        "failed_check_count": sum(1 for item in checks if not item["passed"]),
        "production_effect": "none",
        "broker_action": "none",
    }
    if resolver is not None:
        result["portable_lineage_resolution"] = resolver.evidence()
    return result


def render_research_restart_preflight(payload: Mapping[str, Any]) -> str:
    window = _mapping(payload.get("window_semantics"))
    dq = _mapping(payload.get("data_quality_gate"))
    lane = _mapping(payload.get("research_lane"))
    lines = [
        "# 策略研究重启 R0 Preflight",
        "",
        f"- 状态：`{payload.get('status')}`",
        f"- research_execution_unblocked：`{payload.get('research_execution_unblocked')}`",
        f"- source_sweep_id：`{payload.get('source_sweep_id')}`",
        f"- data_quality：`{dq.get('status')}`",
        f"- failed_check_count：`{payload.get('failed_check_count')}`",
        "",
        "## 窗口语义",
        "",
        f"- 项目 AI-cycle 结论窗口：`{window.get('project_ai_cycle_start')}`",
        f"- QQQ/SGOV/TQQQ primary validated：`{window.get('primary_validated_start')}`",
        f"- 本轮 source role：`{lane.get('source_window_role')}`",
        "- 两个窗口回答不同问题；不得把 legacy comparison 作为 multi-window primary evidence。",
        "",
        "## 研究假设",
        "",
        f"- lane：`{lane.get('lane_id')}`",
        f"- hypothesis：{lane.get('hypothesis')}",
        f"- falsification：{lane.get('primary_falsification_question')}",
        "",
        "## 检查",
        "",
    ]
    for item in _records(payload.get("checks")):
        lines.append(f"- {item.get('check_id')}：`{'PASS' if item.get('passed') else 'FAIL'}`")
    lines.extend(
        [
            "",
            "## 安全边界",
            "",
            "- production_effect=`none`",
            "- promotion/paper-shadow/production weight 均不允许改变",
            "- broker_action=`none`",
            "",
        ]
    )
    return "\n".join(lines)


def _window_semantics_snapshot(
    policy: Mapping[str, Any],
    primary_policy: Mapping[str, Any],
    registry: Mapping[str, Any],
) -> dict[str, Any]:
    semantics = _mapping(policy.get("window_semantics"))
    project = _mapping(semantics.get("project_ai_cycle_conclusion"))
    primary = _mapping(semantics.get("qqq_sgov_tqqq_primary_validation"))
    legacy = _mapping(semantics.get("qqq_sgov_tqqq_ai_cycle_comparison"))
    primary_contract = _mapping(primary_policy.get("primary_research_window_policy"))
    registry_windows = _mapping(registry.get("windows"))
    registry_primary = _mapping(registry_windows.get(str(primary.get("window_id", ""))))
    registry_legacy = _mapping(registry_windows.get(str(legacy.get("window_id", ""))))
    checks = [
        _check("project_primary_start", _iso(project.get("start")) == "2021-02-22"),
        _check("project_primary_anchor", _iso(project.get("anchor_date")) == "2021-02-22"),
        _check(
            "primary_policy_start",
            _iso(primary_contract.get("default_start")) == "2021-02-22",
        ),
        _check("primary_semantic_start", _iso(primary.get("start")) == "2021-02-22"),
        _check("primary_registry_role", registry_primary.get("role") == "primary_validated"),
        _check("primary_registry_start", _iso(registry_primary.get("start")) == "2021-02-22"),
        _check("legacy_registry_role", registry_legacy.get("role") == "legacy_comparison"),
        _check("legacy_registry_start", _iso(registry_legacy.get("start")) == "2022-12-01"),
        _check("window_roles_are_distinct", primary.get("role") != legacy.get("role")),
    ]
    return {
        "status": "PASS" if all(item["passed"] for item in checks) else "FAIL",
        "project_primary_start": _iso(project.get("start")),
        "primary_validated_start": _iso(primary.get("start")),
        "legacy_comparison_start": _iso(legacy.get("start")),
        "checks": checks,
    }


def _data_quality_snapshot(report: DataQualityReport) -> dict[str, Any]:
    return {
        "status": report.status,
        "passed": report.passed,
        "as_of": report.as_of.isoformat(),
        "checked_at": report.checked_at.isoformat(),
        "error_count": report.error_count,
        "warning_count": report.warning_count,
        "info_count": report.info_count,
        "issue_codes": sorted({item.code for item in report.issues}),
        "required_gate": "aits validate-data",
        "called_same_validation_code_path": True,
    }


def _summary_snapshot(summary: Any) -> dict[str, Any] | None:
    if summary is None:
        return None
    payload = asdict(summary)
    return {
        key: (
            value.isoformat()
            if isinstance(value, date)
            else str(value) if isinstance(value, Path) else value
        )
        for key, value in payload.items()
    }


def _holdout_dates_valid(source_config: Mapping[str, Any]) -> bool:
    holdout = _mapping(source_config.get("out_of_sample"))
    try:
        start = date.fromisoformat(str(holdout.get("holdout_start")))
        end = date.fromisoformat(str(holdout.get("holdout_end")))
    except ValueError:
        return False
    return holdout.get("enabled") is True and start <= end


def _fingerprints(paths: Mapping[str, Path]) -> dict[str, Any]:
    return {
        name: {"path": str(path), "sha256": _file_sha256(path), "exists": path.is_file()}
        for name, path in paths.items()
    }


def _fingerprints_fresh(value: Any, *, resolver: PortableLineageResolver | None = None) -> bool:
    records = _mapping(value)
    if not records:
        return False
    for item in records.values():
        record = _mapping(item)
        path = _portable_path(
            Path(str(record.get("path", ""))),
            resolver,
            expected_sha256=str(record.get("sha256", "")),
        )
        if not path.is_file() or record.get("sha256") != _file_sha256(path):
            return False
    return True


def _portable_path(
    path: Path,
    resolver: PortableLineageResolver | None,
    *,
    expected_sha256: str | None = None,
    expected_size: int | None = None,
) -> Path:
    if resolver is None:
        return path
    return resolver.resolve(
        path,
        expected_sha256=expected_sha256,
        expected_size=expected_size,
    )


def _portable_validation_failure(
    *, artifact_path: Path, sidecar_path: Path, error: PortableLineageError
) -> dict[str, Any]:
    return {
        "schema_version": "strategy_research_restart_preflight_validation.v1",
        "report_type": "strategy_research_restart_preflight_validation",
        "status": "FAIL",
        "artifact_path": str(artifact_path),
        "checks": [
            {
                "check_id": "portable_lineage_resolution",
                "passed": False,
                "reason_code": error.reason_code,
            }
        ],
        "failed_check_count": 1,
        "portable_lineage_resolution": portable_lineage_failure_evidence(
            error=error,
            consumer="r0_preflight",
            sidecar_path=sidecar_path,
        ),
        "production_effect": "none",
        "broker_action": "none",
    }


def _max_csv_date(path: Path) -> date | None:
    import csv

    latest: date | None = None
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            raw = row.get("date")
            if not raw:
                continue
            current = date.fromisoformat(raw)
            latest = current if latest is None else max(latest, current)
    return latest


def _load_mapping(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise ResearchRestartError(f"required mapping file missing: {path}")
    payload = safe_load_yaml_path(path)
    if not isinstance(payload, dict):
        raise ResearchRestartError(f"required mapping is invalid: {path}")
    return payload


def _load_json_mapping(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise ResearchRestartError(f"required JSON missing: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ResearchRestartError(f"required JSON is not an object: {path}")
    return payload


def _file_sha256(path: Path) -> str:
    if not path.is_file():
        return ""
    digest = sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    write_json_atomic(path, payload)


def _check(check_id: str, passed: bool) -> dict[str, Any]:
    return {"check_id": check_id, "passed": bool(passed)}


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _sequence(value: Any) -> list[Any]:
    return (
        list(value) if isinstance(value, Sequence) and not isinstance(value, (str, bytes)) else []
    )


def _records(value: Any) -> list[dict[str, Any]]:
    return [_mapping(item) for item in _sequence(value)]


def _iso(value: Any) -> str:
    return value.isoformat() if isinstance(value, date) else str(value or "")


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return value
