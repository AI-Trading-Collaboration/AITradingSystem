from __future__ import annotations

import json
from collections.abc import Mapping
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
DEFAULT_SHADOW_POSITION_GATE_PROFILE_MANIFEST_PATH = (
    PROJECT_ROOT / "config" / "weights" / "shadow_position_gate_profiles.yaml"
)
DEFAULT_SHADOW_WEIGHT_PROFILE_OBSERVATION_LEDGER_PATH = (
    PROJECT_ROOT / "data" / "processed" / "shadow_weight_profile_observations.csv"
)
DEFAULT_SHADOW_WEIGHT_PROFILE_REPORT_DIR = PROJECT_ROOT / "outputs" / "reports"
PRODUCTION_OBSERVED_GATE_PROFILE_ID = "production_observed_gates"

ShadowProfileStatus = Literal["shadow", "candidate", "retired"]

OBSERVATION_COLUMNS = (
    "as_of",
    "generated_at",
    "profile_id",
    "profile_version",
    "profile_status",
    "production_effect",
    "weight_profile_id",
    "weight_profile_version",
    "gate_profile_id",
    "gate_profile_version",
    "production_score",
    "shadow_score",
    "score_delta_vs_production",
    "production_model_band",
    "shadow_model_band",
    "production_final_band",
    "shadow_final_band",
    "production_model_target_position",
    "production_gated_target_position",
    "shadow_model_target_position",
    "shadow_gated_target_position",
    "gate_cap_max_position",
    "gate_cap_sources",
    "gate_cap_overrides_json",
    "target_weights_json",
    "source_snapshot_path",
)

PERFORMANCE_COLUMNS = (
    "as_of",
    "profile_id",
    "profile_version",
    "horizon_days",
    "outcome_end_date",
    "outcome_status",
    "outcome_reason",
    "asset_return",
    "production_gated_target_position",
    "shadow_gated_target_position",
    "production_turnover",
    "shadow_turnover",
    "production_position_return",
    "shadow_position_return",
    "excess_position_return",
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


class ShadowPositionGateProfile(BaseModel):
    profile_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_.:-]+$")
    version: str = Field(min_length=1)
    status: ShadowProfileStatus
    owner: str = Field(min_length=1)
    production_effect: Literal["none"] = "none"
    rationale: str = Field(min_length=1)
    review_after_reports: int = Field(gt=0)
    gate_cap_overrides: dict[str, float] = Field(default_factory=dict)
    metadata: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_gate_cap_overrides(self) -> Self:
        empty_keys = [key for key in self.gate_cap_overrides if not key.strip()]
        if empty_keys:
            raise ValueError("gate_cap_overrides keys must not be empty")
        if "score_model" in self.gate_cap_overrides:
            raise ValueError(
                "score_model cap comes from score-to-position bands; "
                "do not override it in shadow gate profiles"
            )
        out_of_bounds = [
            key
            for key, value in self.gate_cap_overrides.items()
            if value < 0.0 or value > 1.0
        ]
        if out_of_bounds:
            raise ValueError(
                "gate_cap_overrides must be between 0 and 1: "
                + ", ".join(sorted(out_of_bounds))
            )
        return self


class ShadowPositionGateProfileManifest(BaseModel):
    version: str = Field(min_length=1)
    status: str = Field(min_length=1)
    owner: str = Field(min_length=1)
    production_effect: Literal["none"] = "none"
    source_policy_paths: tuple[str, ...] = Field(default_factory=tuple)
    rationale: str = Field(min_length=1)
    review_after_reports: int = Field(gt=0)
    profiles: tuple[ShadowPositionGateProfile, ...]

    @model_validator(mode="after")
    def validate_profiles(self) -> Self:
        if not self.profiles:
            raise ValueError("shadow position gate profile manifest requires profiles")
        seen: set[str] = set()
        duplicates: list[str] = []
        for profile in self.profiles:
            if profile.profile_id in seen:
                duplicates.append(profile.profile_id)
            seen.add(profile.profile_id)
        if duplicates:
            raise ValueError(
                "shadow position gate profile ids must be unique: "
                + ", ".join(duplicates)
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
    weight_profile_id: str
    weight_profile_version: str
    gate_profile_id: str
    gate_profile_version: str
    production_score: float
    shadow_score: float
    production_model_band: dict[str, Any]
    shadow_model_band: dict[str, Any]
    production_final_band: dict[str, Any]
    shadow_final_band: dict[str, Any]
    gate_cap_max_position: float
    gate_cap_sources: tuple[str, ...]
    gate_cap_overrides: dict[str, float]
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

    @property
    def production_model_target_position(self) -> float:
        return _band_midpoint(self.production_model_band)

    @property
    def production_gated_target_position(self) -> float:
        return _band_midpoint(self.production_final_band)

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of.isoformat(),
            "generated_at": self.generated_at.isoformat(),
            "profile_id": self.profile_id,
            "profile_version": self.profile_version,
            "profile_status": self.profile_status,
            "production_effect": self.production_effect,
            "weight_profile_id": self.weight_profile_id,
            "weight_profile_version": self.weight_profile_version,
            "gate_profile_id": self.gate_profile_id,
            "gate_profile_version": self.gate_profile_version,
            "production_score": self.production_score,
            "shadow_score": self.shadow_score,
            "score_delta_vs_production": self.score_delta_vs_production,
            "production_model_band": _band_label(self.production_model_band),
            "shadow_model_band": _band_label(self.shadow_model_band),
            "production_final_band": _band_label(self.production_final_band),
            "shadow_final_band": _band_label(self.shadow_final_band),
            "production_model_target_position": self.production_model_target_position,
            "production_gated_target_position": self.production_gated_target_position,
            "shadow_model_target_position": self.shadow_model_target_position,
            "shadow_gated_target_position": self.shadow_gated_target_position,
            "gate_cap_max_position": self.gate_cap_max_position,
            "gate_cap_sources": ",".join(self.gate_cap_sources),
            "gate_cap_overrides_json": json.dumps(
                self.gate_cap_overrides,
                ensure_ascii=False,
                sort_keys=True,
            ),
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
    gate_manifest_path: Path | None
    gate_manifest_version: str | None
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
            "gate_manifest_path": (
                None if self.gate_manifest_path is None else str(self.gate_manifest_path)
            ),
            "gate_manifest_version": self.gate_manifest_version,
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


@dataclass(frozen=True)
class ShadowWeightPerformanceRow:
    as_of: date
    profile_id: str
    profile_version: str
    horizon_days: int
    outcome_end_date: date | None
    outcome_status: str
    outcome_reason: str
    asset_return: float | None
    production_gated_target_position: float | None
    shadow_gated_target_position: float | None
    production_turnover: float | None
    shadow_turnover: float | None
    production_position_return: float | None
    shadow_position_return: float | None

    @property
    def excess_position_return(self) -> float | None:
        if self.production_position_return is None or self.shadow_position_return is None:
            return None
        return self.shadow_position_return - self.production_position_return

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of.isoformat(),
            "profile_id": self.profile_id,
            "profile_version": self.profile_version,
            "horizon_days": self.horizon_days,
            "outcome_end_date": (
                "" if self.outcome_end_date is None else self.outcome_end_date.isoformat()
            ),
            "outcome_status": self.outcome_status,
            "outcome_reason": self.outcome_reason,
            "asset_return": _blank_if_none(self.asset_return),
            "production_gated_target_position": _blank_if_none(
                self.production_gated_target_position
            ),
            "shadow_gated_target_position": _blank_if_none(
                self.shadow_gated_target_position
            ),
            "production_turnover": _blank_if_none(self.production_turnover),
            "shadow_turnover": _blank_if_none(self.shadow_turnover),
            "production_position_return": _blank_if_none(
                self.production_position_return
            ),
            "shadow_position_return": _blank_if_none(self.shadow_position_return),
            "excess_position_return": _blank_if_none(self.excess_position_return),
        }


@dataclass(frozen=True)
class ShadowWeightPerformanceSummary:
    profile_id: str
    profile_version: str
    total_count: int
    available_count: int
    pending_count: int
    missing_count: int
    production_total_return: float | None
    shadow_total_return: float | None
    excess_total_return: float | None
    production_max_drawdown: float | None
    shadow_max_drawdown: float | None
    production_turnover: float
    shadow_turnover: float
    shadow_beats_production_rate: float | None


@dataclass(frozen=True)
class ShadowWeightPerformanceReport:
    as_of: date
    since: date | None
    observation_ledger_path: Path
    prices_path: Path
    strategy_ticker: str
    horizon_days: int
    cost_bps: float
    slippage_bps: float
    rows: tuple[ShadowWeightPerformanceRow, ...]
    summaries: tuple[ShadowWeightPerformanceSummary, ...]
    warnings: tuple[str, ...]

    @property
    def status(self) -> str:
        if self.warnings or not self.rows:
            return "PASS_WITH_LIMITATIONS"
        return "PASS"

    @property
    def best_profile(self) -> ShadowWeightPerformanceSummary | None:
        comparable = [
            summary
            for summary in self.summaries
            if summary.excess_total_return is not None and summary.available_count > 0
        ]
        if not comparable:
            return None
        return max(comparable, key=lambda summary: summary.excess_total_return or 0.0)

    @property
    def best_positive_profile(self) -> ShadowWeightPerformanceSummary | None:
        best = self.best_profile
        if best is None or best.excess_total_return is None:
            return None
        if best.excess_total_return <= 0.0:
            return None
        return best


def default_shadow_weight_profile_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"shadow_weight_profiles_{as_of.isoformat()}.md"


def default_shadow_weight_performance_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"shadow_weight_performance_{as_of.isoformat()}.md"


def default_shadow_weight_performance_csv_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"shadow_weight_performance_{as_of.isoformat()}.csv"


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


def load_shadow_position_gate_profile_manifest(
    path: Path | str = DEFAULT_SHADOW_POSITION_GATE_PROFILE_MANIFEST_PATH,
) -> ShadowPositionGateProfileManifest:
    manifest_path = Path(path)
    raw = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"shadow position gate profile manifest must be a mapping: {path}")
    return ShadowPositionGateProfileManifest.model_validate(raw)


def build_shadow_weight_profile_run_report(
    *,
    as_of: date,
    decision_snapshot_path: Path,
    manifest_path: Path = DEFAULT_SHADOW_WEIGHT_PROFILE_MANIFEST_PATH,
    gate_manifest_path: Path | None = None,
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
    if gate_manifest_path is None:
        gate_manifest = None
        gate_profiles = (_production_observed_gate_profile(),)
    else:
        gate_manifest = load_shadow_position_gate_profile_manifest(gate_manifest_path)
        gate_profiles = gate_manifest.profiles
    position_bands = _load_position_bands(scoring_rules_path)
    warnings: list[str] = []
    observations: list[ShadowWeightObservation] = []
    for profile in manifest.profiles:
        if profile.status != "shadow":
            warnings.append(
                f"profile {profile.profile_id} status={profile.status}，本轮只作观察。"
            )
        for gate_profile in gate_profiles:
            if gate_profile.status != "shadow":
                warnings.append(
                    f"gate profile {gate_profile.profile_id} "
                    f"status={gate_profile.status}，本轮只作观察。"
                )
            missing_gate_ids = _missing_gate_override_ids(
                snapshot,
                gate_profile.gate_cap_overrides,
            )
            if missing_gate_ids:
                warnings.append(
                    f"gate profile {gate_profile.profile_id} 覆盖了本次 snapshot "
                    f"未出现的 gate：{', '.join(missing_gate_ids)}。"
                )
            shadow_score = sum(
                component_scores[signal] * weight
                for signal, weight in profile.target_weights.items()
            )
            shadow_model_band = _band_for_score(shadow_score, position_bands)
            gate_cap_max_position, gate_cap_sources = _gate_cap(
                snapshot,
                gate_cap_overrides=gate_profile.gate_cap_overrides,
            )
            shadow_final_band = _apply_gate_cap(
                shadow_model_band,
                gate_cap_max_position,
            )
            observations.append(
                ShadowWeightObservation(
                    as_of=as_of,
                    generated_at=generated,
                    profile_id=_combined_shadow_profile_id(profile, gate_profile),
                    profile_version=_combined_shadow_profile_version(
                        profile,
                        gate_profile,
                    ),
                    profile_status=_combined_shadow_profile_status(
                        profile,
                        gate_profile,
                    ),
                    production_effect=profile.production_effect,
                    weight_profile_id=profile.profile_id,
                    weight_profile_version=profile.version,
                    gate_profile_id=gate_profile.profile_id,
                    gate_profile_version=gate_profile.version,
                    production_score=production_score,
                    shadow_score=shadow_score,
                    production_model_band=production_model_band,
                    shadow_model_band=shadow_model_band,
                    production_final_band=production_final_band,
                    shadow_final_band=shadow_final_band,
                    gate_cap_max_position=gate_cap_max_position,
                    gate_cap_sources=gate_cap_sources,
                    gate_cap_overrides=dict(gate_profile.gate_cap_overrides),
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
        gate_manifest_path=gate_manifest_path,
        gate_manifest_version=None if gate_manifest is None else gate_manifest.version,
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
        for column in sorted(missing):
            existing[column] = ""
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
        f"- Gate manifest：`{report.gate_manifest_path}`",
        f"- Gate manifest version：{report.gate_manifest_version or 'production_observed'}",
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
        "- 若传入 shadow gate manifest，本报告只在隔离 observation/prediction ledger "
        "中覆盖已观察 gate cap；不改生产 `scoring_rules.yaml` 或 `portfolio.yaml`。",
        "- shadow profile 的好坏只能进入长期 observation/outcome；是否替换生产权重需要"
        "另行定义 owner approval、promotion 和 rollback 条件。",
        "",
        "## Profile 对比",
        "",
        (
            "| Profile | Version | Weight profile | Gate profile | Shadow score | "
            "Δ vs production | Model band | Gated band | Gate cap | Gate overrides |"
        ),
        "|---|---|---|---|---:|---:|---|---|---:|---|",
    ]
    for observation in report.observations:
        lines.append(
            "| "
            f"`{observation.profile_id}` | "
            f"`{observation.profile_version}` | "
            f"`{observation.weight_profile_id}` | "
            f"`{observation.gate_profile_id}` | "
            f"{observation.shadow_score:.2f} | "
            f"{observation.score_delta_vs_production:+.2f} | "
            f"{_band_label(observation.shadow_model_band)} | "
            f"{_band_label(observation.shadow_final_band)} | "
            f"{observation.gate_cap_max_position:.0%} | "
            f"{_format_gate_overrides(observation.gate_cap_overrides)} |"
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


def build_shadow_weight_performance_report(
    *,
    as_of: date,
    since: date | None = None,
    observation_ledger_path: Path = DEFAULT_SHADOW_WEIGHT_PROFILE_OBSERVATION_LEDGER_PATH,
    prices_path: Path = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv",
    strategy_ticker: str = "SMH",
    horizon_days: int = 1,
    cost_bps: float = 5.0,
    slippage_bps: float = 0.0,
) -> ShadowWeightPerformanceReport:
    if horizon_days <= 0:
        raise ValueError("horizon_days must be positive")
    if cost_bps < 0 or slippage_bps < 0:
        raise ValueError("cost_bps and slippage_bps must be non-negative")
    observations = _load_observation_rows(observation_ledger_path, as_of, since=since)
    price_series = _strategy_price_series(prices_path, strategy_ticker)
    cost_rate = (cost_bps + slippage_bps) / 10_000.0
    rows: list[ShadowWeightPerformanceRow] = []
    warnings: list[str] = []
    previous_positions: dict[str, tuple[float, float]] = {}
    for observation in observations:
        profile_id = str(observation["profile_id"])
        profile_version = str(observation["profile_version"])
        signal_date = date.fromisoformat(str(observation["as_of"]))
        production_position = _float_or_none(
            observation.get("production_gated_target_position")
        )
        shadow_position = _float_or_none(observation.get("shadow_gated_target_position"))
        if production_position is None or shadow_position is None:
            row = ShadowWeightPerformanceRow(
                as_of=signal_date,
                profile_id=profile_id,
                profile_version=profile_version,
                horizon_days=horizon_days,
                outcome_end_date=None,
                outcome_status="MISSING_DATA",
                outcome_reason="observation 缺少 production/shadow gate 后仓位",
                asset_return=None,
                production_gated_target_position=production_position,
                shadow_gated_target_position=shadow_position,
                production_turnover=None,
                shadow_turnover=None,
                production_position_return=None,
                shadow_position_return=None,
            )
            rows.append(row)
            continue
        outcome = _horizon_return(
            price_series,
            signal_date=signal_date,
            as_of=as_of,
            horizon_days=horizon_days,
        )
        if outcome["status"] != "AVAILABLE":
            rows.append(
                ShadowWeightPerformanceRow(
                    as_of=signal_date,
                    profile_id=profile_id,
                    profile_version=profile_version,
                    horizon_days=horizon_days,
                    outcome_end_date=outcome["end_date"],
                    outcome_status=str(outcome["status"]),
                    outcome_reason=str(outcome["reason"]),
                    asset_return=None,
                    production_gated_target_position=production_position,
                    shadow_gated_target_position=shadow_position,
                    production_turnover=None,
                    shadow_turnover=None,
                    production_position_return=None,
                    shadow_position_return=None,
                )
            )
            continue
        previous_production, previous_shadow = previous_positions.get(
            profile_id,
            (0.0, 0.0),
        )
        production_turnover = abs(production_position - previous_production)
        shadow_turnover = abs(shadow_position - previous_shadow)
        asset_return = _float_required(outcome["return"])
        rows.append(
            ShadowWeightPerformanceRow(
                as_of=signal_date,
                profile_id=profile_id,
                profile_version=profile_version,
                horizon_days=horizon_days,
                outcome_end_date=outcome["end_date"],
                outcome_status="AVAILABLE",
                outcome_reason="",
                asset_return=asset_return,
                production_gated_target_position=production_position,
                shadow_gated_target_position=shadow_position,
                production_turnover=production_turnover,
                shadow_turnover=shadow_turnover,
                production_position_return=(
                    production_position * asset_return - production_turnover * cost_rate
                ),
                shadow_position_return=(
                    shadow_position * asset_return - shadow_turnover * cost_rate
                ),
            )
        )
        previous_positions[profile_id] = (production_position, shadow_position)
    summaries = _performance_summaries(rows)
    if not rows:
        warnings.append("shadow weight observation ledger 没有可评估记录。")
    if not any(row.outcome_status == "AVAILABLE" for row in rows):
        warnings.append("当前没有 available shadow weight performance 样本。")
    return ShadowWeightPerformanceReport(
        as_of=as_of,
        since=since,
        observation_ledger_path=observation_ledger_path,
        prices_path=prices_path,
        strategy_ticker=strategy_ticker,
        horizon_days=horizon_days,
        cost_bps=cost_bps,
        slippage_bps=slippage_bps,
        rows=tuple(rows),
        summaries=summaries,
        warnings=tuple(dict.fromkeys(warnings)),
    )


def write_shadow_weight_performance_csv(
    report: ShadowWeightPerformanceReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame([row.to_dict() for row in report.rows])
    for column in PERFORMANCE_COLUMNS:
        if column not in frame.columns:
            frame[column] = ""
    frame = frame.loc[:, list(PERFORMANCE_COLUMNS)]
    frame.to_csv(output_path, index=False)
    return output_path


def render_shadow_weight_performance_report(
    report: ShadowWeightPerformanceReport,
    *,
    csv_path: Path | None = None,
) -> str:
    best = report.best_positive_profile
    lines = [
        "# Shadow Weight Performance 评估报告",
        "",
        f"- 状态：{report.status}",
        "- production_effect：none",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- 起始日期：{report.since.isoformat() if report.since else '全部'}",
        f"- Strategy ticker：{report.strategy_ticker}",
        f"- Horizon：{report.horizon_days}D",
        f"- 单边成本：{report.cost_bps:.1f} bps",
        f"- 线性滑点：{report.slippage_bps:.1f} bps",
        f"- Observation ledger：`{report.observation_ledger_path}`",
        f"- Prices：`{report.prices_path}`",
        f"- 机器可读 performance：`{csv_path}`",
        "",
        "## 治理边界",
        "",
        "- 本报告只把 shadow 权重和 shadow gate profile 的观察仓位转成验证期收益、"
        "回撤、换手和成本对比；不修改生产 `weight_profile_current.yaml`、"
        "approved overlay、正式 prediction ledger、日报结论或仓位 gate。",
        "- 当前结论只能作为 validation-only 调参方向；production 替换仍需要 promotion floor、"
        "forward shadow、owner approval 和 rollback 条件。",
        "",
        "## 最优候选",
        "",
    ]
    if best is None:
        if report.best_profile is None:
            lines.append("- 当前没有可比较的 available 样本。")
        else:
            lines.append(
                "- 当前没有产生正向 position-weighted excess return 的 shadow profile。"
            )
            lines.append(
                "- 最高 excess total return："
                f"{_format_pct(report.best_profile.excess_total_return)}"
            )
    else:
        lines.extend(
            [
                f"- Return-leading profile：`{best.profile_id}` / `{best.profile_version}`",
                f"- Shadow total return：{_format_pct(best.shadow_total_return)}",
                f"- Production total return：{_format_pct(best.production_total_return)}",
                f"- Excess total return：{_format_pct(best.excess_total_return)}",
                f"- Shadow max drawdown：{_format_pct(best.shadow_max_drawdown)}",
                f"- Production max drawdown：{_format_pct(best.production_max_drawdown)}",
                f"- Shadow turnover：{best.shadow_turnover:.2f}",
                f"- Production turnover：{best.production_turnover:.2f}",
                (
                    "- Shadow beats production rate："
                    f"{_format_pct(best.shadow_beats_production_rate)}"
                ),
            ]
        )
    lines.extend(
        [
            "",
            "## Profile 对比",
            "",
            (
                "| Profile | Version | Available | Pending | Missing | Production return | "
                "Shadow return | Excess | Production MDD | Shadow MDD | "
                "Production turnover | Shadow turnover | Beat rate |"
            ),
            "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for summary in report.summaries:
        lines.append(
            "| "
            f"`{summary.profile_id}` | "
            f"`{summary.profile_version}` | "
            f"{summary.available_count} | "
            f"{summary.pending_count} | "
            f"{summary.missing_count} | "
            f"{_format_pct(summary.production_total_return)} | "
            f"{_format_pct(summary.shadow_total_return)} | "
            f"{_format_pct(summary.excess_total_return)} | "
            f"{_format_pct(summary.production_max_drawdown)} | "
            f"{_format_pct(summary.shadow_max_drawdown)} | "
            f"{summary.production_turnover:.2f} | "
            f"{summary.shadow_turnover:.2f} | "
            f"{_format_pct(summary.shadow_beats_production_rate)} |"
        )
    lines.extend(["", "## 警告", ""])
    if report.warnings:
        lines.extend(f"- {warning}" for warning in report.warnings)
    else:
        lines.append("- 无")
    return "\n".join(lines).rstrip() + "\n"


def write_shadow_weight_performance_report(
    report: ShadowWeightPerformanceReport,
    output_path: Path,
    *,
    csv_path: Path | None = None,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_shadow_weight_performance_report(report, csv_path=csv_path),
        encoding="utf-8",
    )
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


def _production_observed_gate_profile() -> ShadowPositionGateProfile:
    return ShadowPositionGateProfile(
        profile_id=PRODUCTION_OBSERVED_GATE_PROFILE_ID,
        version="production_observed",
        status="shadow",
        owner="system_validation",
        production_effect="none",
        rationale="使用 production decision snapshot 中已观察到的 gate cap。",
        review_after_reports=1,
        gate_cap_overrides={},
        metadata={"isolation": "shadow_observation_only"},
    )


def _combined_shadow_profile_id(
    weight_profile: ShadowWeightProfile,
    gate_profile: ShadowPositionGateProfile,
) -> str:
    if gate_profile.profile_id == PRODUCTION_OBSERVED_GATE_PROFILE_ID:
        return weight_profile.profile_id
    return f"{weight_profile.profile_id}__{gate_profile.profile_id}"


def _combined_shadow_profile_version(
    weight_profile: ShadowWeightProfile,
    gate_profile: ShadowPositionGateProfile,
) -> str:
    if gate_profile.profile_id == PRODUCTION_OBSERVED_GATE_PROFILE_ID:
        return weight_profile.version
    return f"{weight_profile.version}+{gate_profile.version}"


def _combined_shadow_profile_status(
    weight_profile: ShadowWeightProfile,
    gate_profile: ShadowPositionGateProfile,
) -> str:
    if weight_profile.status == "retired" or gate_profile.status == "retired":
        return "retired"
    if weight_profile.status == "candidate" or gate_profile.status == "candidate":
        return "candidate"
    return "shadow"


def _load_observation_rows(
    path: Path,
    as_of: date,
    *,
    since: date | None,
) -> tuple[dict[str, Any], ...]:
    if not path.exists():
        raise FileNotFoundError(f"shadow weight observation ledger not found: {path}")
    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    required = {"as_of", "profile_id", "profile_version"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(
            "shadow weight observation ledger missing columns: "
            + ", ".join(sorted(missing))
        )
    for column in ("production_gated_target_position", "shadow_gated_target_position"):
        if column not in frame.columns:
            frame[column] = ""
    frame["_as_of"] = pd.to_datetime(frame["as_of"], errors="coerce")
    frame = frame.loc[frame["_as_of"].notna()].copy()
    frame = frame.loc[frame["_as_of"].dt.date <= as_of].copy()
    if since is not None:
        frame = frame.loc[frame["_as_of"].dt.date >= since].copy()
    frame = frame.sort_values(["_as_of", "profile_id"])
    return tuple(frame.drop(columns=["_as_of"]).to_dict(orient="records"))


def _strategy_price_series(path: Path, ticker: str) -> pd.Series:
    prices = pd.read_csv(path)
    required = {"date", "ticker", "adj_close"}
    missing = required - set(prices.columns)
    if missing:
        raise ValueError("prices CSV missing columns: " + ", ".join(sorted(missing)))
    frame = prices.loc[prices["ticker"] == ticker].copy()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["_adj_close"] = pd.to_numeric(frame["adj_close"], errors="coerce")
    frame = frame.loc[frame["_date"].notna() & frame["_adj_close"].notna()].copy()
    if frame.empty:
        raise ValueError(f"prices CSV contains no rows for strategy ticker: {ticker}")
    return frame.sort_values("_date").set_index("_date")["_adj_close"]


def _horizon_return(
    series: pd.Series,
    *,
    signal_date: date,
    as_of: date,
    horizon_days: int,
) -> dict[str, Any]:
    if signal_date not in set(series.index.date):
        return {
            "status": "MISSING_DATA",
            "reason": f"signal_date 无策略 ticker 收盘价：{signal_date.isoformat()}",
            "end_date": None,
            "return": None,
        }
    start_index = _date_position(series, signal_date)
    end_index = start_index + horizon_days
    if end_index >= len(series):
        return {
            "status": "PENDING",
            "reason": "价格历史尚未覆盖完整观察窗口",
            "end_date": None,
            "return": None,
        }
    end_date = series.index[end_index].date()
    if end_date > as_of:
        return {
            "status": "PENDING",
            "reason": "观察窗口结束日在本次 as_of 之后",
            "end_date": end_date,
            "return": None,
        }
    window = series.iloc[start_index : end_index + 1]
    return {
        "status": "AVAILABLE",
        "reason": "",
        "end_date": end_date,
        "return": float(window.iloc[-1] / window.iloc[0] - 1.0),
    }


def _performance_summaries(
    rows: list[ShadowWeightPerformanceRow],
) -> tuple[ShadowWeightPerformanceSummary, ...]:
    grouped: dict[tuple[str, str], list[ShadowWeightPerformanceRow]] = {}
    for row in rows:
        grouped.setdefault((row.profile_id, row.profile_version), []).append(row)
    summaries: list[ShadowWeightPerformanceSummary] = []
    for (profile_id, profile_version), group_rows in sorted(grouped.items()):
        available = [row for row in group_rows if row.outcome_status == "AVAILABLE"]
        production_returns = [
            _float_required(row.production_position_return)
            for row in available
            if row.production_position_return is not None
        ]
        shadow_returns = [
            _float_required(row.shadow_position_return)
            for row in available
            if row.shadow_position_return is not None
        ]
        excess_returns = [
            _float_required(row.excess_position_return)
            for row in available
            if row.excess_position_return is not None
        ]
        summaries.append(
            ShadowWeightPerformanceSummary(
                profile_id=str(profile_id),
                profile_version=str(profile_version),
                total_count=len(group_rows),
                available_count=len(available),
                pending_count=sum(
                    row.outcome_status == "PENDING" for row in group_rows
                ),
                missing_count=sum(
                    row.outcome_status == "MISSING_DATA" for row in group_rows
                ),
                production_total_return=_compound_returns(production_returns),
                shadow_total_return=_compound_returns(shadow_returns),
                excess_total_return=(
                    None
                    if not production_returns or not shadow_returns
                    else _compound_returns(shadow_returns)
                    - _compound_returns(production_returns)
                ),
                production_max_drawdown=_max_drawdown_from_returns(production_returns),
                shadow_max_drawdown=_max_drawdown_from_returns(shadow_returns),
                production_turnover=sum(
                    row.production_turnover or 0.0 for row in available
                ),
                shadow_turnover=sum(row.shadow_turnover or 0.0 for row in available),
                shadow_beats_production_rate=(
                    None
                    if not excess_returns
                    else sum(value > 0.0 for value in excess_returns)
                    / len(excess_returns)
                ),
            )
        )
    return tuple(sorted(summaries, key=lambda item: item.profile_id))


def _compound_returns(values: list[float]) -> float | None:
    if not values:
        return None
    equity = 1.0
    for value in values:
        equity *= 1.0 + value
    return equity - 1.0


def _max_drawdown_from_returns(values: list[float]) -> float | None:
    if not values:
        return None
    equity = 1.0
    peak = 1.0
    max_drawdown = 0.0
    for value in values:
        equity *= 1.0 + value
        peak = max(peak, equity)
        max_drawdown = min(max_drawdown, equity / peak - 1.0)
    return max_drawdown


def _date_position(series: pd.Series, value: date) -> int:
    matches = series.index[series.index.date == value]
    if len(matches) == 0:
        raise ValueError(f"date not found in series: {value.isoformat()}")
    return int(series.index.get_loc(matches[0]))


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


def _missing_gate_override_ids(
    snapshot: dict[str, Any],
    gate_cap_overrides: Mapping[str, float],
) -> tuple[str, ...]:
    raw_gates = (snapshot.get("positions") or {}).get("position_gates")
    if not isinstance(raw_gates, list):
        return tuple(sorted(gate_cap_overrides))
    observed = {
        str(gate.get("gate_id") or "")
        for gate in raw_gates
        if isinstance(gate, dict) and str(gate.get("gate_id") or "")
    }
    return tuple(sorted(set(gate_cap_overrides) - observed))


def _gate_cap(
    snapshot: dict[str, Any],
    *,
    gate_cap_overrides: Mapping[str, float] | None = None,
) -> tuple[float, tuple[str, ...]]:
    raw_gates = (snapshot.get("positions") or {}).get("position_gates")
    overrides = gate_cap_overrides or {}
    if not isinstance(raw_gates, list):
        return 1.0, ()
    cap = 1.0
    sources: list[str] = []
    for gate in raw_gates:
        if not isinstance(gate, dict):
            continue
        if str(gate.get("gate_id") or "") == "score_model":
            continue
        gate_id = str(gate.get("gate_id") or gate.get("label") or "unknown")
        observed_position = _float_or_none(gate.get("max_position"))
        max_position = overrides.get(gate_id, observed_position)
        if max_position is None:
            continue
        source = gate_id
        if gate_id in overrides and observed_position is not None:
            source = f"{gate_id}:{observed_position:.0%}->{max_position:.0%}"
        if max_position < cap:
            cap = max_position
            sources = [source]
        elif abs(max_position - cap) <= 1e-9:
            sources.append(source)
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


def _blank_if_none(value: Any) -> Any:
    return "" if value is None else value


def _format_pct(value: float | None) -> str:
    if value is None:
        return "NA"
    return f"{value:.2%}"


def _format_gate_overrides(overrides: Mapping[str, float]) -> str:
    if not overrides:
        return "production observed"
    return ", ".join(
        f"{gate_id}={value:.0%}" for gate_id, value in sorted(overrides.items())
    )


def _project_path(value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path
