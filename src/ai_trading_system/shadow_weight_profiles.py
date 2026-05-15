from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Literal, Self

import pandas as pd
import yaml
from pydantic import BaseModel, Field, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.weight_calibration import (
    WeightProfile,
    load_weight_profile,
)

SCHEMA_VERSION = 1
DEFAULT_SHADOW_WEIGHT_PROFILE_MANIFEST_PATH = (
    PROJECT_ROOT / "config" / "weights" / "shadow_weight_profiles.yaml"
)
DEFAULT_SHADOW_WEIGHT_PROFILE_OBSERVATION_LEDGER_PATH = (
    PROJECT_ROOT / "data" / "processed" / "shadow_weight_profile_observations.csv"
)
DEFAULT_SHADOW_WEIGHT_PROFILE_REPORT_DIR = PROJECT_ROOT / "outputs" / "reports"

ShadowProfileStatus = Literal["shadow", "candidate", "retired"]

OBSERVATION_COLUMNS = (
    "as_of",
    "generated_at",
    "profile_id",
    "profile_version",
    "profile_status",
    "production_effect",
    "production_score",
    "shadow_score",
    "score_delta_vs_production",
    "production_model_band",
    "shadow_model_band",
    "production_final_band",
    "shadow_final_band",
    "shadow_model_target_position",
    "shadow_gated_target_position",
    "gate_cap_max_position",
    "gate_cap_sources",
    "target_weights_json",
    "source_snapshot_path",
)


class ShadowWeightProfile(BaseModel):
    profile_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_.:-]+$")
    version: str = Field(min_length=1)
    status: ShadowProfileStatus
    owner: str = Field(min_length=1)
    production_effect: Literal["none"] = "none"
    rationale: str = Field(min_length=1)
    review_after_reports: int = Field(gt=0)
    target_weights: dict[str, float]
    metadata: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_target_weights(self) -> Self:
        _validate_weight_mapping(self.target_weights)
        return self


class ShadowWeightProfileManifest(BaseModel):
    version: str = Field(min_length=1)
    status: str = Field(min_length=1)
    owner: str = Field(min_length=1)
    production_effect: Literal["none"] = "none"
    source_weight_profile_path: str = Field(min_length=1)
    label_horizon_days: int = Field(gt=0)
    rationale: str = Field(min_length=1)
    review_after_reports: int = Field(gt=0)
    profiles: tuple[ShadowWeightProfile, ...]

    @model_validator(mode="after")
    def validate_profiles(self) -> Self:
        if not self.profiles:
            raise ValueError("shadow weight profile manifest requires profiles")
        seen: set[str] = set()
        duplicates: list[str] = []
        for profile in self.profiles:
            if profile.profile_id in seen:
                duplicates.append(profile.profile_id)
            seen.add(profile.profile_id)
        if duplicates:
            raise ValueError(
                "shadow weight profile ids must be unique: " + ", ".join(duplicates)
            )
        return self


@dataclass(frozen=True)
class PositionBand:
    min_score: float
    min_position: float
    max_position: float
    label: str


@dataclass(frozen=True)
class ShadowWeightObservation:
    as_of: date
    generated_at: datetime
    profile_id: str
    profile_version: str
    profile_status: str
    production_effect: str
    production_score: float
    shadow_score: float
    production_model_band: dict[str, Any]
    shadow_model_band: dict[str, Any]
    production_final_band: dict[str, Any]
    shadow_final_band: dict[str, Any]
    gate_cap_max_position: float
    gate_cap_sources: tuple[str, ...]
    target_weights: dict[str, float]
    source_snapshot_path: Path

    @property
    def score_delta_vs_production(self) -> float:
        return self.shadow_score - self.production_score

    @property
    def shadow_model_target_position(self) -> float:
        return _band_midpoint(self.shadow_model_band)

    @property
    def shadow_gated_target_position(self) -> float:
        return _band_midpoint(self.shadow_final_band)

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of.isoformat(),
            "generated_at": self.generated_at.isoformat(),
            "profile_id": self.profile_id,
            "profile_version": self.profile_version,
            "profile_status": self.profile_status,
            "production_effect": self.production_effect,
            "production_score": self.production_score,
            "shadow_score": self.shadow_score,
            "score_delta_vs_production": self.score_delta_vs_production,
            "production_model_band": _band_label(self.production_model_band),
            "shadow_model_band": _band_label(self.shadow_model_band),
            "production_final_band": _band_label(self.production_final_band),
            "shadow_final_band": _band_label(self.shadow_final_band),
            "shadow_model_target_position": self.shadow_model_target_position,
            "shadow_gated_target_position": self.shadow_gated_target_position,
            "gate_cap_max_position": self.gate_cap_max_position,
            "gate_cap_sources": ",".join(self.gate_cap_sources),
            "target_weights_json": json.dumps(
                self.target_weights,
                ensure_ascii=False,
                sort_keys=True,
            ),
            "source_snapshot_path": str(self.source_snapshot_path),
        }


@dataclass(frozen=True)
class ShadowWeightProfileRunReport:
    as_of: date
    generated_at: datetime
    manifest_path: Path
    source_weight_profile_path: Path
    decision_snapshot_path: Path
    observation_ledger_path: Path | None
    prediction_ledger_path: Path | None
    manifest: ShadowWeightProfileManifest
    production_score: float
    production_model_band: dict[str, Any]
    production_final_band: dict[str, Any]
    observations: tuple[ShadowWeightObservation, ...]
    warnings: tuple[str, ...]

    @property
    def status(self) -> str:
        if self.warnings:
            return "PASS_WITH_LIMITATIONS"
        return "PASS"

    @property
    def production_effect(self) -> str:
        return "none"

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "report_type": "shadow_weight_profile_run",
            "status": self.status,
            "production_effect": self.production_effect,
            "as_of": self.as_of.isoformat(),
            "generated_at": self.generated_at.isoformat(),
            "manifest_path": str(self.manifest_path),
            "manifest_version": self.manifest.version,
            "source_weight_profile_path": str(self.source_weight_profile_path),
            "decision_snapshot_path": str(self.decision_snapshot_path),
            "observation_ledger_path": (
                None if self.observation_ledger_path is None else str(self.observation_ledger_path)
            ),
            "prediction_ledger_path": (
                None if self.prediction_ledger_path is None else str(self.prediction_ledger_path)
            ),
            "production_score": self.production_score,
            "production_model_band": self.production_model_band,
            "production_final_band": self.production_final_band,
            "profile_count": len(self.observations),
            "warnings": list(self.warnings),
            "observations": [observation.to_dict() for observation in self.observations],
        }


def default_shadow_weight_profile_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"shadow_weight_profiles_{as_of.isoformat()}.md"


def load_shadow_weight_profile_manifest(
    path: Path | str = DEFAULT_SHADOW_WEIGHT_PROFILE_MANIFEST_PATH,
    *,
    source_profile_path: Path | None = None,
) -> tuple[ShadowWeightProfileManifest, WeightProfile, Path]:
    manifest_path = Path(path)
    raw = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"shadow weight profile manifest must be a mapping: {path}")
    manifest = ShadowWeightProfileManifest.model_validate(raw)
    resolved_source_path = source_profile_path or _project_path(
        manifest.source_weight_profile_path
    )
    source_profile = load_weight_profile(resolved_source_path)
    for profile in manifest.profiles:
        _validate_profile_against_source(profile, source_profile)
    return manifest, source_profile, resolved_source_path


def build_shadow_weight_profile_run_report(
    *,
    as_of: date,
    decision_snapshot_path: Path,
    manifest_path: Path = DEFAULT_SHADOW_WEIGHT_PROFILE_MANIFEST_PATH,
    scoring_rules_path: Path | None = None,
    observation_ledger_path: Path | None = None,
    prediction_ledger_path: Path | None = None,
    generated_at: datetime | None = None,
) -> ShadowWeightProfileRunReport:
    manifest, _source_profile, source_profile_path = load_shadow_weight_profile_manifest(
        manifest_path
    )
    snapshot = _read_json_object(decision_snapshot_path)
    generated = generated_at or datetime.now(tz=UTC)
    component_scores = _component_scores(snapshot)
    production_score = _float_required((snapshot.get("scores") or {}).get("overall_score"))
    production_model_band = _dict_value(
        (snapshot.get("positions") or {}).get("model_risk_asset_ai_band")
    )
    production_final_band = _dict_value(
        (snapshot.get("positions") or {}).get("final_risk_asset_ai_band")
    )
    gate_cap_max_position, gate_cap_sources = _gate_cap(snapshot)
    position_bands = _load_position_bands(scoring_rules_path)
    warnings: list[str] = []
    observations: list[ShadowWeightObservation] = []
    for profile in manifest.profiles:
        if profile.status != "shadow":
            warnings.append(
                f"profile {profile.profile_id} status={profile.status}，本轮只作观察。"
            )
        shadow_score = sum(
            component_scores[signal] * weight
            for signal, weight in profile.target_weights.items()
        )
        shadow_model_band = _band_for_score(shadow_score, position_bands)
        shadow_final_band = _apply_gate_cap(shadow_model_band, gate_cap_max_position)
        observations.append(
            ShadowWeightObservation(
                as_of=as_of,
                generated_at=generated,
                profile_id=profile.profile_id,
                profile_version=profile.version,
                profile_status=profile.status,
                production_effect=profile.production_effect,
                production_score=production_score,
                shadow_score=shadow_score,
                production_model_band=production_model_band,
                shadow_model_band=shadow_model_band,
                production_final_band=production_final_band,
                shadow_final_band=shadow_final_band,
                gate_cap_max_position=gate_cap_max_position,
                gate_cap_sources=gate_cap_sources,
                target_weights=dict(profile.target_weights),
                source_snapshot_path=decision_snapshot_path,
            )
        )
    if not observations:
        warnings.append("shadow weight profile manifest 没有可观察 profile。")
    return ShadowWeightProfileRunReport(
        as_of=as_of,
        generated_at=generated,
        manifest_path=manifest_path,
        source_weight_profile_path=source_profile_path,
        decision_snapshot_path=decision_snapshot_path,
        observation_ledger_path=observation_ledger_path,
        prediction_ledger_path=prediction_ledger_path,
        manifest=manifest,
        production_score=production_score,
        production_model_band=production_model_band,
        production_final_band=production_final_band,
        observations=tuple(observations),
        warnings=tuple(dict.fromkeys(warnings)),
    )


def write_shadow_weight_observation_ledger(
    report: ShadowWeightProfileRunReport,
    output_path: Path = DEFAULT_SHADOW_WEIGHT_PROFILE_OBSERVATION_LEDGER_PATH,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    new_frame = pd.DataFrame([observation.to_dict() for observation in report.observations])
    for column in OBSERVATION_COLUMNS:
        if column not in new_frame.columns:
            new_frame[column] = ""
    new_frame = new_frame.loc[:, list(OBSERVATION_COLUMNS)]
    if output_path.exists():
        existing = pd.read_csv(output_path, dtype=str, keep_default_na=False)
        missing = set(OBSERVATION_COLUMNS) - set(existing.columns)
        if missing:
            raise ValueError(
                "existing shadow weight observation ledger is missing columns: "
                + ", ".join(sorted(missing))
            )
        current_keys = set(zip(new_frame["as_of"], new_frame["profile_id"], strict=True))
        existing = existing.loc[
            [
                (as_of, profile_id) not in current_keys
                for as_of, profile_id in zip(
                    existing["as_of"],
                    existing["profile_id"],
                    strict=True,
                )
            ]
        ]
        frame = pd.concat(
            [existing.loc[:, list(OBSERVATION_COLUMNS)], new_frame],
            ignore_index=True,
        )
    else:
        frame = new_frame
    frame = frame.sort_values(["as_of", "profile_id"])
    frame.to_csv(output_path, index=False)
    return output_path


def render_shadow_weight_profile_report(report: ShadowWeightProfileRunReport) -> str:
    lines = [
        "# Shadow Weight Profiles 观察报告",
        "",
        f"- 状态：{report.status}",
        "- production_effect：none",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- 生成时间：{report.generated_at.isoformat()}",
        f"- Manifest：`{report.manifest_path}`",
        f"- Manifest version：{report.manifest.version}",
        f"- Source weight profile：`{report.source_weight_profile_path}`",
        f"- Decision snapshot：`{report.decision_snapshot_path}`",
        f"- Observation ledger：`{report.observation_ledger_path}`",
        f"- Prediction ledger：`{report.prediction_ledger_path}`",
        f"- 主线评分：{report.production_score:.2f}",
        f"- 主线模型仓位：{_band_label(report.production_model_band)}",
        f"- 主线最终仓位：{_band_label(report.production_final_band)}",
        "",
        "## 治理边界",
        "",
        "- 本报告只读比较 shadow 权重参数，不修改生产 `weight_profile_current.yaml`、"
        "approved overlay、正式 `prediction_ledger.csv`、日报结论或仓位 gate。",
        "- shadow profile 的好坏只能进入长期 observation/outcome；是否替换生产权重需要"
        "另行定义 owner approval、promotion 和 rollback 条件。",
        "",
        "## Profile 对比",
        "",
        (
            "| Profile | Version | Shadow score | Δ vs production | Model band | "
            "Gated band | Gate cap |"
        ),
        "|---|---|---:|---:|---|---|---:|",
    ]
    for observation in report.observations:
        lines.append(
            "| "
            f"`{observation.profile_id}` | "
            f"`{observation.profile_version}` | "
            f"{observation.shadow_score:.2f} | "
            f"{observation.score_delta_vs_production:+.2f} | "
            f"{_band_label(observation.shadow_model_band)} | "
            f"{_band_label(observation.shadow_final_band)} | "
            f"{observation.gate_cap_max_position:.0%} |"
        )
    lines.extend(["", "## 警告", ""])
    if report.warnings:
        lines.extend(f"- {warning}" for warning in report.warnings)
    else:
        lines.append("- 无")
    return "\n".join(lines).rstrip() + "\n"


def write_shadow_weight_profile_report(
    report: ShadowWeightProfileRunReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_shadow_weight_profile_report(report), encoding="utf-8")
    return output_path


def build_shadow_weight_prediction_records(
    report: ShadowWeightProfileRunReport,
    *,
    snapshot: dict[str, Any],
    trace_bundle: dict[str, Any],
    trace_bundle_path: Path,
    features_path: Path,
    data_quality_report_path: Path,
) -> tuple[dict[str, Any], ...]:
    from ai_trading_system.prediction_ledger import (
        build_prediction_record_from_decision_snapshot,
    )

    records: list[dict[str, Any]] = []
    for observation in report.observations:
        candidate_id = (
            f"shadow_weight_profile:{observation.profile_id}:"
            f"{observation.profile_version}"
        )
        record = build_prediction_record_from_decision_snapshot(
            snapshot=snapshot,
            trace_bundle=trace_bundle,
            trace_bundle_path=trace_bundle_path,
            features_path=features_path,
            data_quality_report_path=data_quality_report_path,
            candidate_id=candidate_id,
            production_effect="none",
            label_horizon_days=report.manifest.label_horizon_days,
        )
        record["score"] = observation.shadow_score
        record["signal"] = _band_label(observation.shadow_final_band)
        record["model_target_position"] = observation.shadow_model_target_position
        record["gated_target_position"] = observation.shadow_gated_target_position
        record["execution_assumption"] = (
            "shadow_weight_profile_no_order_no_position_change"
        )
        records.append(record)
    return tuple(records)


def _validate_weight_mapping(weights: dict[str, float]) -> None:
    if not weights:
        raise ValueError("target_weights must not be empty")
    empty_keys = [key for key in weights if not key.strip()]
    if empty_keys:
        raise ValueError("target_weights keys must not be empty")
    negative_keys = [key for key, value in weights.items() if value < 0.0]
    if negative_keys:
        raise ValueError(
            "target_weights must be non-negative: " + ", ".join(sorted(negative_keys))
        )
    if abs(sum(weights.values()) - 1.0) > 1e-6:
        raise ValueError("target_weights must sum to 1.0")


def _validate_profile_against_source(
    profile: ShadowWeightProfile,
    source_profile: WeightProfile,
) -> None:
    source_signals = set(source_profile.base_weights)
    profile_signals = set(profile.target_weights)
    missing = sorted(source_signals - profile_signals)
    unknown = sorted(profile_signals - source_signals)
    if missing or unknown:
        details = []
        if missing:
            details.append("missing " + ", ".join(missing))
        if unknown:
            details.append("unknown " + ", ".join(unknown))
        raise ValueError(
            f"shadow profile {profile.profile_id} signal mismatch: "
            + "; ".join(details)
        )
    out_of_bounds = [
        signal
        for signal, weight in profile.target_weights.items()
        if weight < source_profile.bounds.min_weight or weight > source_profile.bounds.max_weight
    ]
    if out_of_bounds:
        raise ValueError(
            f"shadow profile {profile.profile_id} weights outside source bounds: "
            + ", ".join(sorted(out_of_bounds))
        )


def _read_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON file must contain an object: {path}")
    return payload


def _component_scores(snapshot: dict[str, Any]) -> dict[str, float]:
    raw_components = (snapshot.get("scores") or {}).get("components")
    if not isinstance(raw_components, list) or not raw_components:
        raise ValueError("decision snapshot missing scores.components")
    scores: dict[str, float] = {}
    for item in raw_components:
        if not isinstance(item, dict):
            continue
        name = str(item.get("component") or "")
        if not name:
            continue
        scores[name] = _float_required(item.get("score"))
    if not scores:
        raise ValueError("decision snapshot contains no component scores")
    return scores


def _gate_cap(snapshot: dict[str, Any]) -> tuple[float, tuple[str, ...]]:
    raw_gates = (snapshot.get("positions") or {}).get("position_gates")
    if not isinstance(raw_gates, list):
        return 1.0, ()
    cap = 1.0
    sources: list[str] = []
    for gate in raw_gates:
        if not isinstance(gate, dict):
            continue
        if str(gate.get("gate_id") or "") == "score_model":
            continue
        max_position = _float_or_none(gate.get("max_position"))
        if max_position is None:
            continue
        if max_position < cap:
            cap = max_position
            sources = [str(gate.get("gate_id") or gate.get("label") or "unknown")]
        elif abs(max_position - cap) <= 1e-9:
            sources.append(str(gate.get("gate_id") or gate.get("label") or "unknown"))
    return cap, tuple(dict.fromkeys(sources))


def _load_position_bands(scoring_rules_path: Path | None) -> tuple[PositionBand, ...]:
    path = scoring_rules_path or PROJECT_ROOT / "config" / "scoring_rules.yaml"
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict) or not isinstance(raw.get("position_bands"), list):
        raise ValueError(f"scoring rules missing position_bands: {path}")
    bands = []
    for item in raw["position_bands"]:
        if not isinstance(item, dict):
            continue
        bands.append(
            PositionBand(
                min_score=_float_required(item.get("min_score")),
                min_position=_float_required(item.get("min_position")),
                max_position=_float_required(item.get("max_position")),
                label=str(item.get("label") or "unknown"),
            )
        )
    if not bands:
        raise ValueError(f"scoring rules contain no valid position_bands: {path}")
    return tuple(sorted(bands, key=lambda band: band.min_score, reverse=True))


def _band_for_score(score: float, bands: tuple[PositionBand, ...]) -> dict[str, Any]:
    for band in bands:
        if score >= band.min_score:
            return {
                "label": band.label,
                "min_position": band.min_position,
                "max_position": band.max_position,
            }
    band = bands[-1]
    return {
        "label": band.label,
        "min_position": band.min_position,
        "max_position": band.max_position,
    }


def _apply_gate_cap(band: dict[str, Any], gate_cap_max_position: float) -> dict[str, Any]:
    max_position = min(_float_required(band.get("max_position")), gate_cap_max_position)
    min_position = min(_float_required(band.get("min_position")), max_position)
    label = str(band.get("label") or "unknown")
    if max_position < _float_required(band.get("max_position")):
        label = f"{label}/仓位受限"
    return {
        "label": label,
        "min_position": min_position,
        "max_position": max_position,
    }


def _band_midpoint(band: dict[str, Any]) -> float:
    min_position = _float_required(band.get("min_position"))
    max_position = _float_required(band.get("max_position"))
    return (min_position + max_position) / 2.0


def _band_label(band: dict[str, Any]) -> str:
    label = str(band.get("label") or "unknown")
    min_position = _float_or_none(band.get("min_position"))
    max_position = _float_or_none(band.get("max_position"))
    if min_position is None or max_position is None:
        return label
    return f"{label} {min_position:.0%}-{max_position:.0%}"


def _dict_value(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _float_required(value: Any) -> float:
    parsed = _float_or_none(value)
    if parsed is None:
        raise ValueError(f"expected numeric value, got {value!r}")
    return parsed


def _float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _project_path(value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path
