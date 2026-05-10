from __future__ import annotations

import csv
import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from enum import StrEnum
from pathlib import Path

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.official_policy_sources import (
    OFFICIAL_POLICY_CANDIDATE_COLUMNS,
    default_official_policy_candidates_path,
)

DEFAULT_RISK_EVENT_TRIAGE_REPORT_DIR = PROJECT_ROOT / "outputs" / "reports"
RISK_EVENT_CANDIDATE_TRIAGE_SCHEMA_VERSION = "official_policy_candidate_triage.v1"

TRIAGE_BUCKET_ORDER = {
    "must_review": 0,
    "review_next": 1,
    "sample_review": 2,
    "auto_low_relevance": 3,
    "duplicate_or_noise": 4,
}

RISK_EVENT_CANDIDATE_TRIAGE_COLUMNS = (
    "candidate_id",
    "as_of",
    "source_id",
    "provider",
    "source_type",
    "source_name",
    "source_url",
    "source_title",
    "published_at",
    "captured_at",
    "matched_topics",
    "matched_risk_ids",
    "affected_tickers",
    "affected_nodes",
    "triage_bucket",
    "ai_relevance_score",
    "ai_signals",
    "triage_reason",
    "review_policy",
    "evidence_grade_floor",
    "review_status",
    "raw_payload_path",
    "raw_payload_sha256",
    "row_count",
    "production_effect",
    "notes",
)

REQUIRED_INPUT_COLUMNS = frozenset(
    {
        "candidate_id",
        "as_of",
        "source_id",
        "provider",
        "source_type",
        "source_name",
        "source_url",
        "source_title",
        "matched_topics",
        "matched_risk_ids",
        "affected_tickers",
        "affected_nodes",
        "production_effect",
    }
)

HIGH_AI_PATTERNS = (
    ("export_controls", r"\bexport controls?\b|\bexport administration regulations\b|\bear\b"),
    ("entity_list", r"\bentity list\b|\bunverified list\b|\bmilitary end user\b"),
    ("advanced_computing", r"\badvanced computing\b|\bsupercomput(?:er|ing)\b"),
    ("semiconductor", r"\bsemiconductors?\b|\bmicroelectronics?\b|\bchips?\b"),
    ("ai_chip", r"\bai chips?\b|\bartificial intelligence chips?\b"),
    ("gpu_asic", r"\bgpus?\b|\basics?\b|\baccelerators?\b"),
    ("data_center", r"\bdata centers?\b|\bdatacenters?\b|\bcloud computing\b"),
    ("advanced_packaging", r"\badvanced packaging\b|\bhbm\b|\bhigh bandwidth memory\b"),
    (
        "core_ai_ticker",
        (
            r"\bnvda\b|\bnvidia\b|\bamd\b|\badvanced micro devices\b|\btsm\b|"
            r"\btsmc\b|\btaiwan semiconductor\b|\bintc\b|\bintel\b|\basml\b"
        ),
    ),
)

MEDIUM_AI_PATTERNS = (
    ("artificial_intelligence", r"\bartificial intelligence\b|\bai\b"),
    ("machine_learning", r"\bmachine learning\b|\blarge language models?\b|\bfrontier models?\b"),
    ("ai_policy", r"\bnational ai\b|\bai policy\b|\bai safety\b|\bautomated decision\b"),
)

BROAD_RISK_PATTERNS = (
    ("sanctions", r"\bsanctions?\b|\bofac\b|\bsdn\b|\bcmic\b"),
    ("trade_policy", r"\bsection 301\b|\btariffs?\b|\btrade investigation\b|\bustr\b"),
    (
        "china_geopolitics",
        r"\bchina\b|\bprc\b|\bpeople's republic of china\b|\bchinese\b",
    ),
    ("russia_geopolitics", r"\brussia\b|\brussian federation\b|\brussian\b"),
    ("taiwan_geopolitics", r"\btaiwan\b|\btaiwan strait\b"),
    ("defense_policy", r"\bdefense industrial base\b|\bmilitary\b|\bnational security\b"),
)

LOW_SIGNAL_PATTERNS = (
    ("financial_entity", r"\bbank\b|\bcredit\b|\bleasing\b|\bcapital\b|\bfinance\b"),
    ("personal_or_admin", r"\bpassport\b|\bvisa\b|\bminister\b|\bgovernment of\b"),
)


class RiskEventCandidateTriageIssueSeverity(StrEnum):
    ERROR = "ERROR"
    WARNING = "WARNING"


@dataclass(frozen=True)
class RiskEventCandidateTriageIssue:
    severity: RiskEventCandidateTriageIssueSeverity
    code: str
    message: str
    row_number: int | None = None
    candidate_id: str | None = None


@dataclass(frozen=True)
class RiskEventCandidateTriageRecord:
    source_row: dict[str, str]
    row_number: int
    triage_bucket: str
    ai_relevance_score: int
    ai_signals: tuple[str, ...]
    triage_reason: str
    review_policy: str

    @property
    def candidate_id(self) -> str:
        return self.source_row.get("candidate_id", "")

    @property
    def source_id(self) -> str:
        return self.source_row.get("source_id", "")

    @property
    def source_title(self) -> str:
        return self.source_row.get("source_title", "")


@dataclass(frozen=True)
class RiskEventCandidateTriageReport:
    input_path: Path
    as_of: date
    generated_at: datetime
    records: tuple[RiskEventCandidateTriageRecord, ...]
    issues: tuple[RiskEventCandidateTriageIssue, ...] = field(default_factory=tuple)
    production_effect: str = "none"

    @property
    def row_count(self) -> int:
        return len(self.records)

    @property
    def error_count(self) -> int:
        return sum(
            1
            for issue in self.issues
            if issue.severity == RiskEventCandidateTriageIssueSeverity.ERROR
        )

    @property
    def warning_count(self) -> int:
        return sum(
            1
            for issue in self.issues
            if issue.severity == RiskEventCandidateTriageIssueSeverity.WARNING
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

    @property
    def bucket_counts(self) -> Counter[str]:
        return Counter(record.triage_bucket for record in self.records)


def triage_official_policy_candidates(
    input_path: Path,
    *,
    as_of: date | None = None,
    generated_at: datetime | None = None,
) -> RiskEventCandidateTriageReport:
    input_path = Path(input_path)
    issues: list[RiskEventCandidateTriageIssue] = []
    generated_time = generated_at or datetime.now(tz=UTC)
    rows: list[dict[str, str]] = []

    try:
        with input_path.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            missing_columns = REQUIRED_INPUT_COLUMNS - set(reader.fieldnames or ())
            if missing_columns:
                issues.append(
                    RiskEventCandidateTriageIssue(
                        severity=RiskEventCandidateTriageIssueSeverity.ERROR,
                        code="risk_event_candidate_triage_missing_columns",
                        message=(
                            "官方候选 CSV 缺少必需字段："
                            + ", ".join(sorted(missing_columns))
                        ),
                    )
                )
            else:
                rows = [
                    {key: str(value or "") for key, value in row.items()}
                    for row in reader
                    if row
                ]
    except OSError as exc:
        issues.append(
            RiskEventCandidateTriageIssue(
                severity=RiskEventCandidateTriageIssueSeverity.ERROR,
                code="risk_event_candidate_triage_input_unreadable",
                message=f"无法读取官方候选 CSV：{exc}",
            )
        )

    report_date = as_of or _infer_as_of(rows, input_path) or date.today()
    if issues:
        return RiskEventCandidateTriageReport(
            input_path=input_path,
            as_of=report_date,
            generated_at=generated_time,
            records=(),
            issues=tuple(issues),
        )

    seen_titles: set[str] = set()
    records: list[RiskEventCandidateTriageRecord] = []
    for row_number, row in enumerate(rows, start=2):
        record = _classify_row(row, row_number=row_number, seen_titles=seen_titles)
        records.append(record)
        production_effect = row.get("production_effect", "")
        if production_effect and production_effect != "none":
            issues.append(
                RiskEventCandidateTriageIssue(
                    severity=RiskEventCandidateTriageIssueSeverity.WARNING,
                    code="risk_event_candidate_triage_input_has_production_effect",
                    message=(
                        "输入候选存在非 none production_effect；triage 输出仍强制为 none。"
                    ),
                    row_number=row_number,
                    candidate_id=row.get("candidate_id", ""),
                )
            )

    sorted_records = tuple(
        sorted(
            records,
            key=lambda record: (
                TRIAGE_BUCKET_ORDER.get(record.triage_bucket, 99),
                -record.ai_relevance_score,
                record.source_id,
                record.candidate_id,
            ),
        )
    )
    return RiskEventCandidateTriageReport(
        input_path=input_path,
        as_of=report_date,
        generated_at=generated_time,
        records=sorted_records,
        issues=tuple(issues),
    )


def write_risk_event_candidate_triage_csv(
    report: RiskEventCandidateTriageReport,
    output_path: Path,
) -> Path:
    if not report.passed:
        raise ValueError("risk event candidate triage report has errors")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=RISK_EVENT_CANDIDATE_TRIAGE_COLUMNS)
        writer.writeheader()
        for record in report.records:
            writer.writerow(_record_to_row(record))
    return output_path


def write_risk_event_candidate_triage_report(
    report: RiskEventCandidateTriageReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_risk_event_candidate_triage_report(report), encoding="utf-8")
    return output_path


def load_triaged_candidate_ids(
    input_path: Path,
    *,
    buckets: tuple[str, ...],
) -> tuple[str, ...]:
    bucket_set = {bucket for bucket in buckets if bucket}
    if not bucket_set:
        raise ValueError("triage bucket 集合不能为空。")
    input_path = Path(input_path)
    with input_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        missing_columns = {"candidate_id", "triage_bucket"} - set(reader.fieldnames or ())
        if missing_columns:
            raise ValueError(
                "triage CSV 缺少必需字段：" + ", ".join(sorted(missing_columns))
            )
        candidate_ids: list[str] = []
        for row in reader:
            if row.get("triage_bucket", "") in bucket_set:
                candidate_id = row.get("candidate_id", "").strip()
                if candidate_id:
                    candidate_ids.append(candidate_id)
    return tuple(candidate_ids)


def render_risk_event_candidate_triage_report(
    report: RiskEventCandidateTriageReport,
) -> str:
    counts = report.bucket_counts
    lines = [
        "# 风险事件官方候选 AI 模块 Triage 报告",
        "",
        f"- 状态：{report.status}",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- 生成时间：{report.generated_at.isoformat()}",
        f"- 输入 CSV：`{report.input_path}`",
        f"- 候选数：{report.row_count}",
        f"- 错误数：{report.error_count}",
        f"- 警告数：{report.warning_count}",
        f"- production_effect：`{report.production_effect}`",
        "",
        "## 方法边界",
        "",
        "- 本报告只按 AI 模块相关性给官方候选排序，降低无明显联系项的人工复核优先级。",
        "- 分类不会写入 `risk_event_occurrence`，不会进入评分、仓位闸门或回测标签。",
        (
            "- `auto_low_relevance` 不等于已确认无风险；它表示没有直接 AI 模块信号，"
            "后续仍可抽样或按新证据复核。"
        ),
        (
            "- 分类优先看标题、URL、来源名称和明确 metadata 的直接信号，"
            "不盲目继承宽泛 sanctions/geopolitics 自动映射出的 ticker 或 risk_id。"
        ),
        "",
        "## Bucket 统计",
        "",
        "| Bucket | 数量 | 复核策略 |",
        "|---|---:|---|",
    ]
    for bucket in TRIAGE_BUCKET_ORDER:
        lines.append(
            "| "
            f"`{bucket}` | "
            f"{counts.get(bucket, 0)} | "
            f"{_review_policy_for_bucket(bucket)} |"
        )

    lines.extend(["", "## 高优先级候选", ""])
    high_records = [
        record
        for record in report.records
        if record.triage_bucket in {"must_review", "review_next"}
    ]
    if high_records:
        lines.extend(_candidate_table(high_records[:40]))
    else:
        lines.append("没有直接 AI 模块高优先级候选。")

    lines.extend(["", "## 降低优先级候选摘要", ""])
    low_records = [
        record
        for record in report.records
        if record.triage_bucket in {"sample_review", "auto_low_relevance", "duplicate_or_noise"}
    ]
    if low_records:
        lines.extend(_candidate_table(low_records[:60]))
        if len(low_records) > 60:
            lines.append(f"\n另有 {len(low_records) - 60} 条低优先级候选保留在 CSV。")
    else:
        lines.append("没有低优先级候选。")

    lines.extend(["", "## 问题", ""])
    if report.issues:
        lines.extend(["| 级别 | Row | Candidate | Code | 说明 |", "|---|---:|---|---|---|"])
        for issue in report.issues:
            lines.append(
                "| "
                f"{issue.severity.value} | "
                f"{issue.row_number or ''} | "
                f"{issue.candidate_id or ''} | "
                f"`{issue.code}` | "
                f"{_escape_markdown_table(issue.message)} |"
            )
    else:
        lines.append("未发现问题。")
    return "\n".join(lines) + "\n"


def default_risk_event_candidate_triage_csv_path(processed_dir: Path, as_of: date) -> Path:
    return processed_dir / f"official_policy_candidate_triage_{as_of.isoformat()}.csv"


def default_risk_event_candidate_triage_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"risk_event_candidate_triage_{as_of.isoformat()}.md"


def default_risk_event_candidate_triage_input_path(processed_dir: Path, as_of: date) -> Path:
    return default_official_policy_candidates_path(processed_dir, as_of)


def _classify_row(
    row: dict[str, str],
    *,
    row_number: int,
    seen_titles: set[str],
) -> RiskEventCandidateTriageRecord:
    title = row.get("source_title", "")
    source_url = row.get("source_url", "")
    source_name = row.get("source_name", "")
    source_id = row.get("source_id", "")
    title_key = _normalization_key(title)

    direct_text = " ".join(part for part in (title, source_url, source_name, source_id) if part)
    expanded_text = " ".join(
        part
        for part in (
            direct_text,
            row.get("matched_topics", ""),
            row.get("matched_risk_ids", ""),
            row.get("affected_tickers", ""),
            row.get("affected_nodes", ""),
        )
        if part
    )
    direct_high = _signals_for(direct_text, HIGH_AI_PATTERNS)
    direct_medium = _signals_for(direct_text, MEDIUM_AI_PATTERNS)
    broad_signals = _signals_for(expanded_text, BROAD_RISK_PATTERNS)
    low_signals = _signals_for(direct_text, LOW_SIGNAL_PATTERNS)

    if title_key and title_key in seen_titles:
        return RiskEventCandidateTriageRecord(
            source_row=row,
            row_number=row_number,
            triage_bucket="duplicate_or_noise",
            ai_relevance_score=5,
            ai_signals=tuple(direct_high + direct_medium + broad_signals),
            triage_reason="同一标准化标题已在本批次出现；保留审计记录但不重复逐条复核。",
            review_policy=_review_policy_for_bucket("duplicate_or_noise"),
        )
    if title_key:
        seen_titles.add(title_key)

    if direct_high:
        return RiskEventCandidateTriageRecord(
            source_row=row,
            row_number=row_number,
            triage_bucket="must_review",
            ai_relevance_score=95,
            ai_signals=tuple(direct_high),
            triage_reason=(
                "标题、URL 或来源直接命中 AI 模块高相关信号："
                + ", ".join(direct_high)
                + "。"
            ),
            review_policy=_review_policy_for_bucket("must_review"),
        )

    if direct_medium:
        return RiskEventCandidateTriageRecord(
            source_row=row,
            row_number=row_number,
            triage_bucket="review_next",
            ai_relevance_score=70,
            ai_signals=tuple(direct_medium),
            triage_reason=(
                "直接涉及 AI/人工智能政策，但尚未命中半导体、出口管制、"
                "核心 ticker 或数据中心等市场触发信号。"
            ),
            review_policy=_review_policy_for_bucket("review_next"),
        )

    if broad_signals and _is_broad_sanctions_source(source_id):
        reason = (
            "候选来自 OFAC/CSL 等宽泛制裁或筛查列表，标题/URL 未显示直接 AI 模块信号。"
        )
        if low_signals:
            reason += " 标题更像金融/行政/个人实体： " + ", ".join(low_signals) + "。"
        return RiskEventCandidateTriageRecord(
            source_row=row,
            row_number=row_number,
            triage_bucket="auto_low_relevance",
            ai_relevance_score=20,
            ai_signals=tuple(broad_signals + low_signals),
            triage_reason=reason,
            review_policy=_review_policy_for_bucket("auto_low_relevance"),
        )

    if broad_signals:
        return RiskEventCandidateTriageRecord(
            source_row=row,
            row_number=row_number,
            triage_bucket="sample_review",
            ai_relevance_score=40,
            ai_signals=tuple(broad_signals),
            triage_reason=(
                "仅命中宽泛政策/地缘主题，未出现 AI、半导体、先进计算、"
                "出口管制或核心 ticker 的直接信号。"
            ),
            review_policy=_review_policy_for_bucket("sample_review"),
        )

    return RiskEventCandidateTriageRecord(
        source_row=row,
        row_number=row_number,
        triage_bucket="auto_low_relevance",
        ai_relevance_score=10,
        ai_signals=(),
        triage_reason="标题、URL、来源名称和候选 metadata 均未显示 AI 模块直接相关信号。",
        review_policy=_review_policy_for_bucket("auto_low_relevance"),
    )


def _signals_for(text: str, patterns: tuple[tuple[str, str], ...]) -> list[str]:
    signals: list[str] = []
    for label, pattern in patterns:
        if re.search(pattern, text, flags=re.IGNORECASE):
            signals.append(label)
    return signals


def _is_broad_sanctions_source(source_id: str) -> bool:
    source_id_lower = source_id.lower()
    return any(token in source_id_lower for token in ("ofac", "trade_csl", "consolidated"))


def _normalization_key(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()
    return re.sub(r"\s+", " ", normalized)


def _infer_as_of(rows: list[dict[str, str]], input_path: Path) -> date | None:
    for row in rows:
        raw_value = row.get("as_of", "")
        if not raw_value:
            continue
        try:
            return date.fromisoformat(raw_value[:10])
        except ValueError:
            continue
    match = re.search(r"(\d{4}-\d{2}-\d{2})", input_path.name)
    if match:
        try:
            return date.fromisoformat(match.group(1))
        except ValueError:
            return None
    return None


def _record_to_row(record: RiskEventCandidateTriageRecord) -> dict[str, object]:
    source_row = record.source_row
    row: dict[str, object] = {
        column: source_row.get(column, "") for column in OFFICIAL_POLICY_CANDIDATE_COLUMNS
    }
    row.update(
        {
            "triage_bucket": record.triage_bucket,
            "ai_relevance_score": record.ai_relevance_score,
            "ai_signals": ";".join(record.ai_signals),
            "triage_reason": record.triage_reason,
            "review_policy": record.review_policy,
            "production_effect": "none",
        }
    )
    return {column: row.get(column, "") for column in RISK_EVENT_CANDIDATE_TRIAGE_COLUMNS}


def _candidate_table(records: list[RiskEventCandidateTriageRecord]) -> list[str]:
    lines = [
        "| Bucket | Score | Source | Title | Signals | Reason |",
        "|---|---:|---|---|---|---|",
    ]
    for record in records:
        lines.append(
            "| "
            f"`{record.triage_bucket}` | "
            f"{record.ai_relevance_score} | "
            f"{record.source_id} | "
            f"{_escape_markdown_table(record.source_title)[:140]} | "
            f"{', '.join(record.ai_signals)} | "
            f"{_escape_markdown_table(record.triage_reason)[:180]} |"
        )
    return lines


def _review_policy_for_bucket(bucket: str) -> str:
    if bucket == "must_review":
        return "优先人工复核；可能影响 AI 产业链、核心 ticker、出口管制或仓位风险。"
    if bucket == "review_next":
        return "次优先人工复核；AI policy 相关，但需确认是否有投资含义。"
    if bucket == "sample_review":
        return "批量抽样复核；用于防漏检，通常不逐条优先处理。"
    if bucket == "auto_low_relevance":
        return "低优先级归档；没有直接 AI 模块信号，除非后续证据增强。"
    if bucket == "duplicate_or_noise":
        return "重复或低信息记录；保留审计，不重复人工逐条复核。"
    return "待复核。"


def _escape_markdown_table(value: object) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")
