from __future__ import annotations

import argparse
import json
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal

from ai_trading_system.platform.artifacts import write_bytes_atomic
from ai_trading_system.yaml_loader import safe_load_yaml_path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_POLICY_PATH = Path(
    "config/research/qc_qqq_options_session_finalization_dq_pit_evidence_admission_v1.yaml"
)
DEFAULT_OUTPUT_ROOT = Path(
    "inputs/research/qqq_options/trading_2533_session_finalization_dq_pit_evidence_admission_v1"
)
DEFAULT_RETAINED_RAW_RESULTS_PATH = Path(
    "outputs/external_validation/"
    "trading_2532_session_finalization_v2_once_20260817/"
    "upgraded_magenta_gorilla_results.json"
)

_REPORT_FILE = "dq_pit_evidence_admission.json"
_MANIFEST_FILE = "package_manifest.json"
_SOURCE_EVIDENCE_FILE = "export_safe_aggregate_evidence.json"
_SOURCE_MANIFEST_FILE = "execution_evidence_manifest.json"
_REQUIRED_CHECK_IDS = tuple(
    sorted(
        (
            "cache_identity",
            "chain_presence",
            "engine_identity",
            "evidence_identity",
            "exchange_calendar_identity",
            "fill_forward_ambiguity",
            "local_cache_dq_scope_separation",
            "open_interest_freshness",
            "order_fill_chronology",
            "prior_day_model_freshness",
            "provider_raw_checksum",
            "quote_freshness",
            "quote_integrity",
            "signal_selection_chronology",
            "symbol_mapping_identity",
        )
    )
)
_PIT_CHECK_IDS = tuple(
    sorted(
        (
            "exchange_calendar_identity",
            "fill_forward_ambiguity",
            "open_interest_freshness",
            "order_fill_chronology",
            "prior_day_model_freshness",
            "quote_freshness",
            "signal_selection_chronology",
            "symbol_mapping_identity",
        )
    )
)
_FIXED_NOT_EVALUATED_RULES = {
    "NOT_EVALUATED_AGGREGATE_PARTIAL_ONLY",
    "NOT_EVALUATED_WITHOUT_CANONICAL_MATERIAL",
    "NOT_EVALUATED_ZERO_ORDER_RUN",
}
_ALLOWED_STATUSES = frozenset({"PASS", "FAIL", "NOT_EVALUATED"})


class SessionFinalizationDQPITAdmissionError(ValueError):
    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(code if not detail else f"{code}: {detail}")
        self.code = code
        self.detail = detail


def _fail(code: str, detail: str = "") -> None:
    raise SessionFinalizationDQPITAdmissionError(code, detail)


def _canonical_bytes(payload: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")


def _content_sha256(payload: Mapping[str, Any]) -> str:
    body = dict(payload)
    body.pop("content_sha256", None)
    return sha256(_canonical_bytes(body)).hexdigest()


def _seal(payload: Mapping[str, Any]) -> dict[str, Any]:
    sealed = dict(payload)
    sealed["content_sha256"] = _content_sha256(sealed)
    return sealed


def _verify_seal(payload: Mapping[str, Any], *, label: str) -> None:
    if payload.get("content_sha256") != _content_sha256(payload):
        _fail("TRADING_2533_CONTENT_SEAL_INVALID", label)


def _file_sha256(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()


def _mapping(value: object, *, field: str) -> dict[str, Any]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        _fail("TRADING_2533_MAPPING_REQUIRED", field)
    return value


def _sequence(value: object, *, field: str) -> list[Any]:
    if not isinstance(value, list):
        _fail("TRADING_2533_SEQUENCE_REQUIRED", field)
    return value


def _require_exact_keys(payload: Mapping[str, Any], *, expected: set[str], field: str) -> None:
    observed = set(payload)
    if observed != expected:
        missing = sorted(expected - observed)
        extra = sorted(observed - expected)
        _fail(
            "TRADING_2533_KEY_SET_DRIFT",
            f"{field}: missing={missing}; extra={extra}",
        )


def _load_policy(project_root: Path) -> tuple[dict[str, Any], str]:
    root = project_root.resolve()
    path = (root / DEFAULT_POLICY_PATH).resolve()
    raw = path.read_bytes()
    policy = _mapping(safe_load_yaml_path(path), field="policy")
    _require_exact_keys(
        policy,
        expected={
            "schema_version",
            "policy_id",
            "policy_version",
            "policy_status",
            "task_id",
            "predecessor_task_id",
            "owner",
            "owner_instruction_ref",
            "rationale",
            "intended_effect",
            "review_condition",
            "admission_as_of_utc",
            "source",
            "canonical_dq_pit",
            "check_rules",
            "decision",
            "safety",
        },
        field="policy",
    )
    if (
        policy["schema_version"]
        != "qc_qqq_options_session_finalization_dq_pit_evidence_admission_policy.v1"
        or policy["policy_id"] != "qc_qqq_options_session_finalization_dq_pit_evidence_admission_v1"
        or policy["policy_version"] != "1.0.0"
        or policy["policy_status"] != "REVIEWED_OFFLINE_FAIL_CLOSED"
    ):
        _fail("TRADING_2533_POLICY_IDENTITY_INVALID")

    source = _mapping(policy["source"], field="source")
    _require_exact_keys(
        source,
        expected={
            "execution_package_root",
            "retained_raw_results_locator",
            "raw_results_byte_count",
            "raw_results_sha256",
            "independent_verification_status",
            "backtest_id",
            "requested_range",
            "expected_session_count",
            "observed_session_count",
            "export_safe_evidence_content_sha256",
            "execution_manifest_content_sha256",
            "tracked_artifacts",
        },
        field="source",
    )
    if source["independent_verification_status"] != "PASS":
        _fail("TRADING_2533_RAW_RESULT_NOT_INDEPENDENTLY_VERIFIED")
    if source["expected_session_count"] != 1202 or source["observed_session_count"] != 1202:
        _fail("TRADING_2533_SESSION_AUTHORITY_DRIFT")

    canonical = _mapping(policy["canonical_dq_pit"], field="canonical_dq_pit")
    _require_exact_keys(
        canonical,
        expected={
            "policy_path",
            "policy_file_sha256",
            "shared_contract_sha256",
            "required_check_ids",
            "pit_check_ids",
        },
        field="canonical_dq_pit",
    )
    if tuple(_sequence(canonical["required_check_ids"], field="required_check_ids")) != (
        _REQUIRED_CHECK_IDS
    ):
        _fail("TRADING_2533_REQUIRED_CHECK_SET_DRIFT")
    if tuple(_sequence(canonical["pit_check_ids"], field="pit_check_ids")) != _PIT_CHECK_IDS:
        _fail("TRADING_2533_PIT_CHECK_SET_DRIFT")
    dq_policy_path = (root / str(canonical["policy_path"])).resolve()
    if _file_sha256(dq_policy_path) != canonical["policy_file_sha256"]:
        _fail("TRADING_2533_CANONICAL_DQ_POLICY_DRIFT")

    rules = _mapping(policy["check_rules"], field="check_rules")
    if tuple(sorted(rules)) != _REQUIRED_CHECK_IDS:
        _fail("TRADING_2533_RULE_CHECK_SET_DRIFT")
    decisions = _mapping(policy["decision"], field="decision")
    _require_exact_keys(
        decisions,
        expected={
            "dq_status",
            "pit_status",
            "admission_status",
            "selection_status",
            "engine_status",
        },
        field="decision",
    )
    if decisions["dq_status"] != "FAIL" or decisions["pit_status"] != "NOT_EVALUATED":
        _fail("TRADING_2533_FAIL_CLOSED_DECISION_DRIFT")
    safety = _mapping(policy["safety"], field="safety")
    if any(
        safety.get(field) is not False
        for field in (
            "external_action_authorized",
            "cloud_run_authorized",
            "raw_option_rows_authorized",
            "object_store_authorized",
            "selection_authorized",
            "investment_conclusion_authorized",
        )
    ):
        _fail("TRADING_2533_EXTERNAL_OR_INVESTMENT_BOUNDARY_OPEN")
    if safety.get("maximum_orders") != 0 or safety.get("maximum_fills") != 0:
        _fail("TRADING_2533_TRADING_BOUNDARY_OPEN")
    return policy, sha256(raw).hexdigest()


def _load_canonical_json(path: Path, *, label: str) -> dict[str, Any]:
    raw = path.read_bytes()
    try:
        payload = _mapping(json.loads(raw), field=label)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        _fail("TRADING_2533_JSON_INVALID", label)
        raise AssertionError from exc
    if raw != _canonical_bytes(payload):
        _fail("TRADING_2533_JSON_NOT_CANONICAL", label)
    return payload


def _load_source_package(
    *, project_root: Path, policy: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], dict[str, str]]:
    source = _mapping(policy["source"], field="source")
    root = (project_root.resolve() / str(source["execution_package_root"])).resolve()
    tracked = _mapping(source["tracked_artifacts"], field="tracked_artifacts")
    observed_hashes: dict[str, str] = {}
    for name, expected_value in sorted(tracked.items()):
        expected = _mapping(expected_value, field=f"tracked_artifacts.{name}")
        _require_exact_keys(
            expected,
            expected={"byte_count", "file_sha256"},
            field=f"tracked_artifacts.{name}",
        )
        path = root / name
        raw = path.read_bytes()
        if len(raw) != expected["byte_count"] or sha256(raw).hexdigest() != expected["file_sha256"]:
            _fail("TRADING_2533_SOURCE_ARTIFACT_DRIFT", name)
        observed_hashes[name] = str(expected["file_sha256"])

    evidence = _load_canonical_json(root / _SOURCE_EVIDENCE_FILE, label=_SOURCE_EVIDENCE_FILE)
    manifest = _load_canonical_json(root / _SOURCE_MANIFEST_FILE, label=_SOURCE_MANIFEST_FILE)
    _verify_seal(evidence, label=_SOURCE_EVIDENCE_FILE)
    _verify_seal(manifest, label=_SOURCE_MANIFEST_FILE)
    if evidence["content_sha256"] != source["export_safe_evidence_content_sha256"]:
        _fail("TRADING_2533_SOURCE_EVIDENCE_SEAL_DRIFT")
    if manifest["content_sha256"] != source["execution_manifest_content_sha256"]:
        _fail("TRADING_2533_SOURCE_MANIFEST_SEAL_DRIFT")
    if manifest.get("artifacts", {}).get(_SOURCE_EVIDENCE_FILE) != evidence["content_sha256"]:
        _fail("TRADING_2533_SOURCE_MANIFEST_BINDING_INVALID")
    if (
        evidence.get("backtest_id") != source["backtest_id"]
        or evidence.get("requested_range") != source["requested_range"]
        or evidence.get("expected_session_count") != source["expected_session_count"]
        or evidence.get("observed_session_count") != source["observed_session_count"]
    ):
        _fail("TRADING_2533_SOURCE_SCOPE_DRIFT")
    if evidence.get("orders") != 0 or evidence.get("fills") != 0:
        _fail("TRADING_2533_SOURCE_TRADING_EFFECT_PRESENT")
    if evidence.get("dq_pit_admission_authorized") is not False:
        _fail("TRADING_2533_SOURCE_DQ_BOUNDARY_OPEN")
    if (
        manifest.get("cloud_backtest_attempt_count") != 1
        or manifest.get("project_mutation_count") != 1
        or manifest.get("second_attempt_authorized") is not False
    ):
        _fail("TRADING_2533_SOURCE_ACTION_LEDGER_INVALID")
    return evidence, manifest, observed_hashes


def verify_retained_raw_results(
    raw_results_path: Path = DEFAULT_RETAINED_RAW_RESULTS_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> dict[str, Any]:
    policy, _ = _load_policy(project_root)
    source = _mapping(policy["source"], field="source")
    path = raw_results_path
    if not path.is_absolute():
        path = project_root.resolve() / path
    raw = path.resolve().read_bytes()
    observed = sha256(raw).hexdigest()
    if len(raw) != source["raw_results_byte_count"] or observed != source["raw_results_sha256"]:
        _fail("TRADING_2533_RAW_RESULTS_IDENTITY_MISMATCH")
    return {
        "status": "PASS",
        "locator": str(source["retained_raw_results_locator"]),
        "byte_count": len(raw),
        "sha256": observed,
        "verification_scope": "WHOLE_RESULTS_BYTES_ONLY_NO_RAW_OPTION_ROW_EXTRACTION",
        "verified_at_utc": str(policy["admission_as_of_utc"]),
    }


def _aggregate_status(statuses: Sequence[str]) -> Literal["PASS", "FAIL", "NOT_EVALUATED"]:
    if any(status == "FAIL" for status in statuses):
        return "FAIL"
    if any(status == "NOT_EVALUATED" for status in statuses):
        return "NOT_EVALUATED"
    return "PASS"


def _build_checks(
    *, policy: Mapping[str, Any], evidence: Mapping[str, Any]
) -> tuple[dict[str, Any], ...]:
    rules = _mapping(policy["check_rules"], field="check_rules")
    axis_counts = _mapping(
        evidence.get("per_axis_status_session_counts"),
        field="per_axis_status_session_counts",
    )
    checks: list[dict[str, Any]] = []
    for check_id in _REQUIRED_CHECK_IDS:
        rule_payload = _mapping(rules[check_id], field=f"check_rules.{check_id}")
        rule = rule_payload.get("rule")
        evidence_refs: list[str] = []
        missing_requirement = rule_payload.get("missing_requirement")
        if check_id == "chain_presence":
            key = str(rule_payload.get("evidence_key"))
            missing = axis_counts.get(key)
            if not isinstance(missing, int) or missing < 0:
                _fail("TRADING_2533_CHAIN_COUNT_INVALID")
            status = "FAIL" if missing > 0 else "PASS"
            reason_code = str(rule_payload["reason_code"]) if missing > 0 else None
            evidence_refs.append(f"{_SOURCE_EVIDENCE_FILE}#/per_axis_status_session_counts/{key}")
        elif check_id == "local_cache_dq_scope_separation":
            key = str(rule_payload.get("evidence_key"))
            status = "PASS" if evidence.get(key) is False else "FAIL"
            reason_code = None if status == "PASS" else "DQ_SCOPE_SUBSTITUTION_ATTEMPTED"
            evidence_refs.extend(
                (
                    f"{_SOURCE_EVIDENCE_FILE}#/{key}",
                    f"{_SOURCE_MANIFEST_FILE}#/{key}",
                )
            )
        elif rule in _FIXED_NOT_EVALUATED_RULES:
            status = "NOT_EVALUATED"
            reason_code = str(rule_payload["reason_code"])
            if check_id == "quote_integrity":
                evidence_refs.extend(
                    (
                        f"{_SOURCE_EVIDENCE_FILE}#/per_axis_status_session_counts/TRADING2531_BID_ASK_QUOTE_PRESENT_SESSIONS",
                        f"{_SOURCE_EVIDENCE_FILE}#/per_axis_status_session_counts/TRADING2531_BID_ASK_QUOTE_NOT_EVALUATED_SESSIONS",
                    )
                )
        else:
            _fail("TRADING_2533_RULE_KIND_INVALID", f"{check_id}:{rule}")
        if status not in _ALLOWED_STATUSES:
            _fail("TRADING_2533_CHECK_STATUS_INVALID", check_id)
        checks.append(
            {
                "check_id": check_id,
                "status": status,
                "reason_code": reason_code,
                "evidence_refs": evidence_refs,
                "missing_requirement": missing_requirement,
            }
        )
    return tuple(checks)


def _evidence_gaps() -> list[dict[str, str]]:
    return [
        {
            "gap_id": "FINAL_NEVER_CHAIN_ATTRIBUTION",
            "plain_language_zh": (
                "解释唯一一个全日未出现期权链的交易日究竟是provider缺失还是transport缺失。"
            ),
        },
        {
            "gap_id": "FRESHNESS_AS_OF_EVIDENCE",
            "plain_language_zh": (
                "提供quote新鲜度、Greeks/model与OI的as-of交易日，而不只是字段存在计数。"
            ),
        },
        {
            "gap_id": "CALENDAR_MAPPING_IDENTITY",
            "plain_language_zh": "绑定交易日历和symbol mapping的id、版本与哈希。",
        },
        {
            "gap_id": "CACHE_ENGINE_PLATFORM_IDENTITY",
            "plain_language_zh": (
                "绑定cache material、engine、tier、bundle和platform evidence identity。"
            ),
        },
        {
            "gap_id": "PROVIDER_RAW_CHECKSUM_AVAILABILITY",
            "plain_language_zh": (
                "确认provider是否暴露raw source checksum；Results文件哈希不能代替它。"
            ),
        },
        {
            "gap_id": "STRATEGY_CHRONOLOGY_NOT_OCCURRED",
            "plain_language_zh": (
                "zero-order transport run没有signal、selection、order或fill时序；"
                "未来只能由独立策略证据产生。"
            ),
        },
        {
            "gap_id": "NUMERIC_POLICY_REVIEW",
            "plain_language_zh": (
                "quote age、spread、min OI和volume阈值仍需独立review，不能从本次汇总反推。"
            ),
        },
    ]


@dataclass(frozen=True)
class DQPITEvidenceAdmissionBuild:
    policy_sha256: str
    report: Mapping[str, Any]
    manifest: Mapping[str, Any]


def build_dq_pit_evidence_admission(
    *,
    project_root: Path = PROJECT_ROOT,
    raw_results_path: Path | None = None,
) -> DQPITEvidenceAdmissionBuild:
    root = project_root.resolve()
    policy, policy_sha256 = _load_policy(root)
    evidence, source_manifest, observed_hashes = _load_source_package(
        project_root=root, policy=policy
    )
    source = _mapping(policy["source"], field="source")
    if raw_results_path is not None:
        raw_verification = verify_retained_raw_results(raw_results_path, project_root=root)
    else:
        raw_verification = {
            "status": str(source["independent_verification_status"]),
            "locator": str(source["retained_raw_results_locator"]),
            "byte_count": int(source["raw_results_byte_count"]),
            "sha256": str(source["raw_results_sha256"]),
            "verification_scope": "WHOLE_RESULTS_BYTES_ONLY_NO_RAW_OPTION_ROW_EXTRACTION",
            "verified_at_utc": str(policy["admission_as_of_utc"]),
        }

    checks = _build_checks(policy=policy, evidence=evidence)
    status_counts = Counter(str(check["status"]) for check in checks)
    dq_status = _aggregate_status([str(check["status"]) for check in checks])
    pit_status = _aggregate_status(
        [str(check["status"]) for check in checks if str(check["check_id"]) in _PIT_CHECK_IDS]
    )
    decision = _mapping(policy["decision"], field="decision")
    if dq_status != decision["dq_status"] or pit_status != decision["pit_status"]:
        _fail("TRADING_2533_DERIVED_DECISION_MISMATCH")

    axis_counts = _mapping(
        evidence["per_axis_status_session_counts"],
        field="per_axis_status_session_counts",
    )
    diagnostics = _mapping(evidence["diagnostic_counts"], field="diagnostic_counts")
    report = _seal(
        {
            "schema_version": "qqq_options_session_finalization_dq_pit_evidence_admission.v1",
            "task_id": str(policy["task_id"]),
            "predecessor_task_id": str(policy["predecessor_task_id"]),
            "policy_id": str(policy["policy_id"]),
            "policy_version": str(policy["policy_version"]),
            "policy_sha256": policy_sha256,
            "canonical_dq_pit_policy_sha256": str(
                _mapping(policy["canonical_dq_pit"], field="canonical_dq_pit")["policy_file_sha256"]
            ),
            "canonical_shared_contract_sha256": str(
                _mapping(policy["canonical_dq_pit"], field="canonical_dq_pit")[
                    "shared_contract_sha256"
                ]
            ),
            "admission_as_of_utc": str(policy["admission_as_of_utc"]),
            "source_identity": {
                "backtest_id": str(source["backtest_id"]),
                "requested_range": str(source["requested_range"]),
                "expected_session_count": int(source["expected_session_count"]),
                "observed_session_count": int(source["observed_session_count"]),
                "raw_results_verification": raw_verification,
                "export_safe_evidence_content_sha256": str(evidence["content_sha256"]),
                "execution_manifest_content_sha256": str(source_manifest["content_sha256"]),
                "tracked_artifact_file_sha256": observed_hashes,
            },
            "transport_facts": {
                "chain_present_sessions": int(
                    axis_counts["TRADING2531_OPTION_CHAIN_PRESENCE_PRESENT_SESSIONS"]
                ),
                "final_never_chain_sessions": int(
                    axis_counts["TRADING2531_OPTION_CHAIN_PRESENCE_MISSING_SESSIONS"]
                ),
                "recovered_after_chainless_sessions": int(
                    diagnostics["TRADING2531_SESSIONS_RECOVERED_AFTER_CHAINLESS"]
                ),
                "orders": int(evidence["orders"]),
                "fills": int(evidence["fills"]),
            },
            "required_checks": list(checks),
            "coverage_summary": {
                "required_check_count": len(checks),
                "pass_count": status_counts["PASS"],
                "fail_count": status_counts["FAIL"],
                "not_evaluated_count": status_counts["NOT_EVALUATED"],
            },
            "decision": {
                "dq_status": dq_status,
                "pit_status": pit_status,
                "admission_status": str(decision["admission_status"]),
                "selection_status": str(decision["selection_status"]),
                "engine_status": str(decision["engine_status"]),
                "investment_conclusion_authorized": False,
            },
            "evidence_gaps": _evidence_gaps(),
            "reader_summary_zh": (
                "现有证据已解决collector与transport归因，但仍有1个交易日全日未出现期权链；"
                "2482的15项检查中只有范围隔离通过、chain presence明确失败，其余13项因缺少"
                "时点、身份或逐观察证据而未评估。因此DQ=FAIL、PIT=NOT_EVALUATED，研究继续关闭。"
            ),
            "safety": _mapping(policy["safety"], field="safety"),
        }
    )
    report_bytes = _canonical_bytes(report)
    manifest = _seal(
        {
            "schema_version": (
                "qqq_options_session_finalization_dq_pit_evidence_admission_package.v1"
            ),
            "task_id": str(policy["task_id"]),
            "status": str(decision["admission_status"]),
            "artifact_count": 1,
            "artifacts": {
                _REPORT_FILE: {
                    "byte_count": len(report_bytes),
                    "file_sha256": sha256(report_bytes).hexdigest(),
                    "content_sha256": str(report["content_sha256"]),
                }
            },
            "source_artifacts": observed_hashes,
            "production_effect": "none",
            "broker_action": "none",
            "external_action_authorized": False,
            "orders": 0,
            "fills": 0,
        }
    )
    return DQPITEvidenceAdmissionBuild(
        policy_sha256=policy_sha256,
        report=report,
        manifest=manifest,
    )


def write_dq_pit_evidence_admission_package(
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    *,
    project_root: Path = PROJECT_ROOT,
    raw_results_path: Path | None = None,
) -> DQPITEvidenceAdmissionBuild:
    built = build_dq_pit_evidence_admission(
        project_root=project_root, raw_results_path=raw_results_path
    )
    destination = output_root
    if not destination.is_absolute():
        destination = project_root.resolve() / destination
    destination.mkdir(parents=True, exist_ok=True)
    write_bytes_atomic(destination / _REPORT_FILE, _canonical_bytes(built.report))
    write_bytes_atomic(destination / _MANIFEST_FILE, _canonical_bytes(built.manifest))
    return built


def validate_dq_pit_evidence_admission_package(
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    *,
    project_root: Path = PROJECT_ROOT,
    raw_results_path: Path | None = None,
) -> DQPITEvidenceAdmissionBuild:
    root = output_root
    if not root.is_absolute():
        root = project_root.resolve() / root
    report = _load_canonical_json(root / _REPORT_FILE, label=_REPORT_FILE)
    manifest = _load_canonical_json(root / _MANIFEST_FILE, label=_MANIFEST_FILE)
    _verify_seal(report, label=_REPORT_FILE)
    _verify_seal(manifest, label=_MANIFEST_FILE)
    expected = build_dq_pit_evidence_admission(
        project_root=project_root, raw_results_path=raw_results_path
    )
    if report != expected.report:
        _fail("TRADING_2533_REPORT_DRIFT")
    if manifest != expected.manifest:
        _fail("TRADING_2533_MANIFEST_DRIFT")
    return expected


def _main() -> int:
    parser = argparse.ArgumentParser(
        description="Build or validate the offline TRADING-2533 DQ/PIT evidence admission."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    build_parser = subparsers.add_parser("build")
    build_parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    build_parser.add_argument("--raw-results", type=Path, default=DEFAULT_RETAINED_RAW_RESULTS_PATH)
    validate_parser = subparsers.add_parser("validate")
    validate_parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    validate_parser.add_argument("--raw-results", type=Path)
    args = parser.parse_args()
    if args.command == "build":
        result = write_dq_pit_evidence_admission_package(
            args.output_root, raw_results_path=args.raw_results
        )
    else:
        result = validate_dq_pit_evidence_admission_package(
            args.output_root, raw_results_path=args.raw_results
        )
    print(
        json.dumps(
            {
                "status": "PASS",
                "policy_sha256": result.policy_sha256,
                "report_content_sha256": result.report["content_sha256"],
                "manifest_content_sha256": result.manifest["content_sha256"],
                "dq_status": result.report["decision"]["dq_status"],
                "pit_status": result.report["decision"]["pit_status"],
                "production_effect": "none",
                "broker_action": "none",
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())


__all__ = [
    "DEFAULT_OUTPUT_ROOT",
    "DEFAULT_POLICY_PATH",
    "DEFAULT_RETAINED_RAW_RESULTS_PATH",
    "DQPITEvidenceAdmissionBuild",
    "SessionFinalizationDQPITAdmissionError",
    "build_dq_pit_evidence_admission",
    "validate_dq_pit_evidence_admission_package",
    "verify_retained_raw_results",
    "write_dq_pit_evidence_admission_package",
]
