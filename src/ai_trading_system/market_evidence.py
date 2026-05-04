from __future__ import annotations

import csv
import hashlib
from dataclasses import dataclass, field
from datetime import date
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field, ValidationError, model_validator

EvidenceSourceType = Literal[
    "primary_source",
    "paid_vendor",
    "manual_input",
    "public_convenience",
    "llm_extracted",
]
EvidenceGrade = Literal["S", "A", "B", "C", "D", "X"]
EvidenceNovelty = Literal["new", "confirming", "duplicate", "conflicting", "unknown"]
EvidenceDirection = Literal["positive", "negative", "mixed", "neutral", "unknown"]
EvidenceImpactHorizon = Literal["short", "medium", "long", "unknown"]
EvidenceReviewStatus = Literal["confirmed", "pending_review", "rejected"]

REQUIRED_CSV_COLUMNS = frozenset(
    {
        "evidence_id",
        "source_name",
        "source_type",
        "captured_at",
        "topic",
        "raw_summary",
    }
)
OPTIONAL_CSV_COLUMNS = frozenset(
    {
        "source_url",
        "published_at",
        "tickers",
        "industry_chain_nodes",
        "evidence_grade",
        "novelty",
        "impact_horizon",
        "direction",
        "confidence",
        "manual_review_status",
        "manual_review_required",
        "linked_risk_event",
        "linked_thesis",
        "linked_valuation_snapshot",
        "linked_catalyst",
        "linked_claim",
        "llm_model",
        "llm_prompt_hash",
        "notes",
    }
)
ALLOWED_CSV_COLUMNS = REQUIRED_CSV_COLUMNS | OPTIONAL_CSV_COLUMNS


class MarketEvidenceIssueSeverity(StrEnum):
    ERROR = "ERROR"
    WARNING = "WARNING"


class MarketEvidence(BaseModel):
    evidence_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_.:-]+$")
    source_name: str = Field(min_length=1)
    source_type: EvidenceSourceType
    source_url: str = ""
    published_at: date | None = None
    captured_at: date
    tickers: list[str] = Field(default_factory=list)
    industry_chain_nodes: list[str] = Field(default_factory=list)
    topic: str = Field(min_length=1)
    evidence_grade: EvidenceGrade = "C"
    novelty: EvidenceNovelty = "unknown"
    impact_horizon: EvidenceImpactHorizon = "unknown"
    direction: EvidenceDirection = "unknown"
    confidence: float = Field(default=0.5, ge=0, le=1)
    manual_review_status: EvidenceReviewStatus = "pending_review"
    manual_review_required: bool = True
    linked_risk_event: str = ""
    linked_thesis: str = ""
    linked_valuation_snapshot: str = ""
    linked_catalyst: str = ""
    linked_claim: str = ""
    raw_summary: str = Field(min_length=1)
    llm_model: str = ""
    llm_prompt_hash: str = ""
    notes: str = ""

    @model_validator(mode="after")
    def normalize(self) -> MarketEvidence:
        self.tickers = [ticker.upper() for ticker in self.tickers]
        if self.source_type == "llm_extracted":
            self.manual_review_required = True
            if self.manual_review_status == "confirmed":
                self.manual_review_status = "pending_review"
        if self.source_type in {"primary_source", "paid_vendor", "public_convenience"} and (
            not self.source_url
        ):
            raise ValueError("source_url is required for non-manual evidence sources")
        return self


@dataclass(frozen=True)
class LoadedMarketEvidence:
    evidence: MarketEvidence
    path: Path


@dataclass(frozen=True)
class MarketEvidenceLoadError:
    path: Path
    message: str


@dataclass(frozen=True)
class MarketEvidenceStore:
    input_path: Path
    loaded: tuple[LoadedMarketEvidence, ...]
    load_errors: tuple[MarketEvidenceLoadError, ...]


@dataclass(frozen=True)
class MarketEvidenceIssue:
    severity: MarketEvidenceIssueSeverity
    code: str
    message: str
    evidence_id: str | None = None
    path: Path | None = None


@dataclass(frozen=True)
class MarketEvidenceValidationReport:
    as_of: date
    input_path: Path
    evidence: tuple[LoadedMarketEvidence, ...]
    issues: tuple[MarketEvidenceIssue, ...] = field(default_factory=tuple)

    @property
    def evidence_count(self) -> int:
        return len(self.evidence)

    @property
    def confirmed_count(self) -> int:
        return sum(
            1
            for loaded in self.evidence
            if loaded.evidence.manual_review_status == "confirmed"
        )

    @property
    def pending_review_count(self) -> int:
        return sum(
            1
            for loaded in self.evidence
            if loaded.evidence.manual_review_status == "pending_review"
        )

    @property
    def error_count(self) -> int:
        return sum(
            1 for issue in self.issues if issue.severity == MarketEvidenceIssueSeverity.ERROR
        )

    @property
    def warning_count(self) -> int:
        return sum(
            1 for issue in self.issues if issue.severity == MarketEvidenceIssueSeverity.WARNING
        )

    @property
    def passed(self) -> bool:
        return self.error_count == 0

    @property
    def status(self) -> str:
        if self.error_count:
            return "FAIL"
        if self.warning_count:
            return "PASS_WITH_WARNINGS"
        return "PASS"


@dataclass(frozen=True)
class MarketEvidenceCsvImportReport:
    input_path: Path
    row_count: int
    checksum_sha256: str
    evidence: tuple[MarketEvidence, ...]
    issues: tuple[MarketEvidenceIssue, ...] = field(default_factory=tuple)

    @property
    def imported_count(self) -> int:
        return len(self.evidence)

    @property
    def error_count(self) -> int:
        return sum(
            1 for issue in self.issues if issue.severity == MarketEvidenceIssueSeverity.ERROR
        )

    @property
    def warning_count(self) -> int:
        return sum(
            1 for issue in self.issues if issue.severity == MarketEvidenceIssueSeverity.WARNING
        )

    @property
    def passed(self) -> bool:
        return self.error_count == 0

    @property
    def status(self) -> str:
        if self.error_count:
            return "FAIL"
        if self.warning_count:
            return "PASS_WITH_WARNINGS"
        return "PASS"


def load_market_evidence_store(input_path: Path | str) -> MarketEvidenceStore:
    path = Path(input_path)
    loaded: list[LoadedMarketEvidence] = []
    errors: list[MarketEvidenceLoadError] = []
    for yaml_path in _evidence_yaml_paths(path):
        try:
            raw = _load_yaml(yaml_path)
        except (OSError, yaml.YAMLError) as exc:
            errors.append(MarketEvidenceLoadError(path=yaml_path, message=str(exc)))
            continue
        for raw_item in _raw_evidence_items(raw):
            try:
                evidence = MarketEvidence.model_validate(raw_item)
            except ValidationError as exc:
                errors.append(
                    MarketEvidenceLoadError(
                        path=yaml_path,
                        message=_compact_validation_error(exc),
                    )
                )
                continue
            loaded.append(LoadedMarketEvidence(evidence=evidence, path=yaml_path))
    return MarketEvidenceStore(input_path=path, loaded=tuple(loaded), load_errors=tuple(errors))


def validate_market_evidence_store(
    store: MarketEvidenceStore,
    *,
    as_of: date,
) -> MarketEvidenceValidationReport:
    issues: list[MarketEvidenceIssue] = []
    for load_error in store.load_errors:
        issues.append(
            MarketEvidenceIssue(
                severity=MarketEvidenceIssueSeverity.ERROR,
                code="market_evidence_load_error",
                path=load_error.path,
                message=load_error.message,
            )
        )
    _check_duplicate_evidence(store.loaded, issues)
    _check_duplicate_source_keys(store.loaded, issues)
    for loaded in store.loaded:
        evidence = loaded.evidence
        if evidence.captured_at > as_of or (
            evidence.published_at is not None and evidence.published_at > as_of
        ):
            issues.append(
                MarketEvidenceIssue(
                    severity=MarketEvidenceIssueSeverity.ERROR,
                    code="market_evidence_date_in_future",
                    evidence_id=evidence.evidence_id,
                    path=loaded.path,
                    message="证据发布日期或采集日期晚于评估日期。",
                )
            )
        if evidence.source_type == "llm_extracted":
            issues.append(
                MarketEvidenceIssue(
                    severity=MarketEvidenceIssueSeverity.WARNING,
                    code="llm_evidence_pending_review",
                    evidence_id=evidence.evidence_id,
                    path=loaded.path,
                    message="LLM 抽取证据只能进入待复核队列，不能直接改变评分或仓位。",
                )
            )
        if evidence.source_type == "public_convenience":
            issues.append(
                MarketEvidenceIssue(
                    severity=MarketEvidenceIssueSeverity.WARNING,
                    code="public_convenience_evidence_not_scoreable",
                    evidence_id=evidence.evidence_id,
                    path=loaded.path,
                    message="公开便利源只能作为辅助证据，不能单独进入自动评分。",
                )
            )
    return MarketEvidenceValidationReport(
        as_of=as_of,
        input_path=store.input_path,
        evidence=store.loaded,
        issues=tuple(issues),
    )


def import_market_evidence_csv(input_path: Path | str) -> MarketEvidenceCsvImportReport:
    path = Path(input_path)
    raw_bytes = path.read_bytes()
    checksum = hashlib.sha256(raw_bytes).hexdigest()
    issues: list[MarketEvidenceIssue] = []
    reader = csv.DictReader(raw_bytes.decode("utf-8-sig").splitlines())
    fieldnames = tuple(reader.fieldnames or ())
    _check_csv_schema(fieldnames, issues, path)
    if any(issue.severity == MarketEvidenceIssueSeverity.ERROR for issue in issues):
        return MarketEvidenceCsvImportReport(
            input_path=path,
            row_count=0,
            checksum_sha256=checksum,
            evidence=(),
            issues=tuple(issues),
        )
    imported: list[MarketEvidence] = []
    row_count = 0
    for row_number, raw_row in enumerate(reader, start=2):
        row_count += 1
        row = {key: _cell(value) for key, value in raw_row.items() if key is not None}
        try:
            imported.append(_evidence_from_csv_row(row))
        except (ValidationError, ValueError) as exc:
            issues.append(
                MarketEvidenceIssue(
                    severity=MarketEvidenceIssueSeverity.ERROR,
                    code="market_evidence_csv_row_invalid",
                    evidence_id=row.get("evidence_id") or None,
                    path=path,
                    message=f"row {row_number}: {_error_message(exc)}",
                )
            )
    has_error = any(
        issue.severity == MarketEvidenceIssueSeverity.ERROR for issue in issues
    )
    return MarketEvidenceCsvImportReport(
        input_path=path,
        row_count=row_count,
        checksum_sha256=checksum,
        evidence=tuple(imported if not has_error else ()),
        issues=tuple(issues),
    )


def write_market_evidence_yaml(
    evidence_items: tuple[MarketEvidence, ...] | list[MarketEvidence],
    output_dir: Path,
) -> tuple[Path, ...]:
    output_dir.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    for evidence in evidence_items:
        output_path = output_dir / f"{_safe_file_stem(evidence.evidence_id)}.yaml"
        output_path.write_text(
            yaml.safe_dump(
                evidence.model_dump(mode="json", exclude_none=False),
                allow_unicode=True,
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        paths.append(output_path)
    return tuple(paths)


def render_market_evidence_validation_report(report: MarketEvidenceValidationReport) -> str:
    lines = [
        "# Market Evidence 校验报告",
        "",
        f"- 状态：{report.status}",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- 输入路径：`{report.input_path}`",
        f"- 证据数：{report.evidence_count}",
        f"- 已确认：{report.confirmed_count}",
        f"- 待复核：{report.pending_review_count}",
        f"- 错误数：{report.error_count}",
        f"- 警告数：{report.warning_count}",
        "",
        "## 证据记录",
        "",
    ]
    if not report.evidence:
        lines.append("未发现可读取证据。")
    else:
        lines.extend(
            [
                "| Evidence | Source | Grade | Review | Topic | Tickers | Nodes | Links |",
                "|---|---|---|---|---|---|---|---|",
            ]
        )
        for loaded in sorted(report.evidence, key=lambda item: item.evidence.evidence_id):
            evidence = loaded.evidence
            lines.append(
                "| "
                f"{evidence.evidence_id} | "
                f"{evidence.source_type} | "
                f"{evidence.evidence_grade} | "
                f"{evidence.manual_review_status} | "
                f"{_escape_markdown_table(evidence.topic)} | "
                f"{', '.join(evidence.tickers)} | "
                f"{', '.join(evidence.industry_chain_nodes)} | "
                f"{_linked_summary(evidence)} |"
            )
    lines.extend(["", "## 问题", ""])
    if not report.issues:
        lines.append("未发现问题。")
    else:
        lines.extend(["| 级别 | Code | Evidence | 说明 |", "|---|---|---|---|"])
        for issue in report.issues:
            lines.append(
                "| "
                f"{_severity_label(issue.severity)} | "
                f"{issue.code} | "
                f"{issue.evidence_id or ''} | "
                f"{_escape_markdown_table(issue.message)} |"
            )
    lines.extend(
        [
            "",
            "## 方法说明",
            "",
            "- market_evidence 是新信息进入系统的证据账本，不直接改变评分、仓位或交易建议。",
            "- `llm_extracted` 和 `public_convenience` 默认只进入人工复核或辅助解释。",
        ]
    )
    return "\n".join(lines) + "\n"


def render_market_evidence_import_report(report: MarketEvidenceCsvImportReport) -> str:
    return "\n".join(
        [
            "# Market Evidence CSV 导入报告",
            "",
            f"- 状态：{report.status}",
            f"- 输入路径：`{report.input_path}`",
            f"- CSV 行数：{report.row_count}",
            f"- SHA256：`{report.checksum_sha256}`",
            f"- 导入证据数：{report.imported_count}",
            f"- 错误数：{report.error_count}",
            f"- 警告数：{report.warning_count}",
        ]
    ) + "\n"


def write_market_evidence_validation_report(
    report: MarketEvidenceValidationReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_market_evidence_validation_report(report), encoding="utf-8")
    return output_path


def write_market_evidence_import_report(
    report: MarketEvidenceCsvImportReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_market_evidence_import_report(report), encoding="utf-8")
    return output_path


def default_market_evidence_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"market_evidence_{as_of.isoformat()}.md"


def _evidence_yaml_paths(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    if path.is_dir():
        return sorted([*path.glob("*.yaml"), *path.glob("*.yml")])
    return []


def _load_yaml(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def _raw_evidence_items(raw: Any) -> list[Any]:
    if raw is None:
        return []
    if isinstance(raw, dict) and "evidence" in raw:
        value = raw["evidence"]
        return value if isinstance(value, list) else [value]
    return [raw]


def _check_duplicate_evidence(
    loaded_items: tuple[LoadedMarketEvidence, ...],
    issues: list[MarketEvidenceIssue],
) -> None:
    seen: set[str] = set()
    for loaded in loaded_items:
        evidence_id = loaded.evidence.evidence_id
        if evidence_id in seen:
            issues.append(
                MarketEvidenceIssue(
                    severity=MarketEvidenceIssueSeverity.ERROR,
                    code="duplicate_market_evidence_id",
                    evidence_id=evidence_id,
                    path=loaded.path,
                    message="evidence_id 重复，无法稳定追溯证据。",
                )
            )
        seen.add(evidence_id)


def _check_duplicate_source_keys(
    loaded_items: tuple[LoadedMarketEvidence, ...],
    issues: list[MarketEvidenceIssue],
) -> None:
    seen: dict[tuple[str, str, str], str] = {}
    for loaded in loaded_items:
        evidence = loaded.evidence
        key = (evidence.source_url, str(evidence.published_at), evidence.topic)
        if not evidence.source_url:
            continue
        existing = seen.get(key)
        if existing is not None:
            issues.append(
                MarketEvidenceIssue(
                    severity=MarketEvidenceIssueSeverity.WARNING,
                    code="possible_duplicate_market_evidence",
                    evidence_id=evidence.evidence_id,
                    path=loaded.path,
                    message=(
                        "source_url/published_at/topic "
                        f"与 {existing} 重复，需确认是否重复信息。"
                    ),
                )
            )
        seen[key] = evidence.evidence_id


def _check_csv_schema(
    fieldnames: tuple[str, ...],
    issues: list[MarketEvidenceIssue],
    path: Path,
) -> None:
    if not fieldnames:
        issues.append(
            MarketEvidenceIssue(
                severity=MarketEvidenceIssueSeverity.ERROR,
                code="missing_csv_header",
                path=path,
                message="CSV 缺少表头。",
            )
        )
        return
    columns = set(fieldnames)
    missing = sorted(REQUIRED_CSV_COLUMNS - columns)
    if missing:
        issues.append(
            MarketEvidenceIssue(
                severity=MarketEvidenceIssueSeverity.ERROR,
                code="missing_required_csv_columns",
                path=path,
                message=f"CSV 缺少必填列：{', '.join(missing)}。",
            )
        )
    unknown = sorted(columns - ALLOWED_CSV_COLUMNS)
    if unknown:
        issues.append(
            MarketEvidenceIssue(
                severity=MarketEvidenceIssueSeverity.WARNING,
                code="unknown_csv_columns",
                path=path,
                message=f"CSV 包含未使用列：{', '.join(unknown)}。",
            )
        )


def _evidence_from_csv_row(row: dict[str, str]) -> MarketEvidence:
    captured_at = date.fromisoformat(row["captured_at"])
    published_at = date.fromisoformat(row["published_at"]) if row.get("published_at") else None
    return MarketEvidence(
        evidence_id=row["evidence_id"],
        source_name=row["source_name"],
        source_type=row["source_type"],
        source_url=row.get("source_url", ""),
        published_at=published_at,
        captured_at=captured_at,
        tickers=_split_items(row.get("tickers", "")),
        industry_chain_nodes=_split_items(row.get("industry_chain_nodes", "")),
        topic=row["topic"],
        evidence_grade=row.get("evidence_grade") or "C",
        novelty=row.get("novelty") or "unknown",
        impact_horizon=row.get("impact_horizon") or "unknown",
        direction=row.get("direction") or "unknown",
        confidence=float(row["confidence"]) if row.get("confidence") else 0.5,
        manual_review_status=row.get("manual_review_status") or "pending_review",
        manual_review_required=_parse_bool(row.get("manual_review_required", ""), default=True),
        linked_risk_event=row.get("linked_risk_event", ""),
        linked_thesis=row.get("linked_thesis", ""),
        linked_valuation_snapshot=row.get("linked_valuation_snapshot", ""),
        linked_catalyst=row.get("linked_catalyst", ""),
        linked_claim=row.get("linked_claim", ""),
        raw_summary=row["raw_summary"],
        llm_model=row.get("llm_model", ""),
        llm_prompt_hash=row.get("llm_prompt_hash", ""),
        notes=row.get("notes", ""),
    )


def _split_items(value: str) -> list[str]:
    return [item.strip() for item in value.split(";") if item.strip()]


def _parse_bool(value: str, *, default: bool) -> bool:
    if not value:
        return default
    return value.lower() in {"1", "true", "yes", "y", "是"}


def _cell(value: str | None) -> str:
    return "" if value is None else value.strip()


def _compact_validation_error(exc: ValidationError) -> str:
    first_error = exc.errors()[0] if exc.errors() else None
    if not first_error:
        return "market evidence schema validation failed"
    location = ".".join(str(part) for part in first_error.get("loc", ()))
    message = str(first_error.get("msg", "schema validation failed"))
    return f"{location}: {message}" if location else message


def _error_message(exc: Exception) -> str:
    if isinstance(exc, ValidationError):
        return _compact_validation_error(exc)
    return str(exc)


def _safe_file_stem(value: str) -> str:
    return value.replace(":", "_").replace("/", "_").replace("\\", "_")


def _linked_summary(evidence: MarketEvidence) -> str:
    links = [
        evidence.linked_risk_event,
        evidence.linked_thesis,
        evidence.linked_valuation_snapshot,
        evidence.linked_catalyst,
        evidence.linked_claim,
    ]
    return ", ".join(link for link in links if link)


def _severity_label(severity: MarketEvidenceIssueSeverity) -> str:
    if severity == MarketEvidenceIssueSeverity.ERROR:
        return "错误"
    return "警告"


def _escape_markdown_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
