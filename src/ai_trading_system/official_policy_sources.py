from __future__ import annotations

import csv
import json
import re
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from hashlib import sha256
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Protocol

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data.download import write_download_manifest

DEFAULT_OFFICIAL_POLICY_RAW_DIR = PROJECT_ROOT / "data" / "raw" / "official_policy_sources"
DEFAULT_OFFICIAL_POLICY_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
OFFICIAL_POLICY_CANDIDATE_SCHEMA_VERSION = "official_policy_source_candidates.v1"
OFFICIAL_POLICY_CANDIDATE_COLUMNS = (
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
    "evidence_grade_floor",
    "review_status",
    "review_questions",
    "raw_payload_path",
    "raw_payload_sha256",
    "row_count",
    "production_effect",
    "notes",
)

_USER_AGENT = "AITradingSystem policy source fetcher; contact=project-owner"
_DEFAULT_LIMIT = 50


class OfficialPolicyIssueSeverity(StrEnum):
    ERROR = "ERROR"
    WARNING = "WARNING"


@dataclass(frozen=True)
class OfficialPolicyIssue:
    severity: OfficialPolicyIssueSeverity
    code: str
    message: str
    source_id: str | None = None


@dataclass(frozen=True)
class OfficialPolicyHttpResponse:
    status_code: int
    headers: Mapping[str, str]
    body: bytes


class OfficialPolicyHttpClient(Protocol):
    def get(
        self,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        timeout: int = 30,
    ) -> OfficialPolicyHttpResponse:
        """Download one official source payload."""


class UrllibOfficialPolicyHttpClient:
    def get(
        self,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        timeout: int = 30,
    ) -> OfficialPolicyHttpResponse:
        request = urllib.request.Request(
            url,
            headers=dict(headers or {}),
            method="GET",
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return OfficialPolicyHttpResponse(
                    status_code=response.status,
                    headers=dict(response.headers.items()),
                    body=response.read(),
                )
        except urllib.error.HTTPError as exc:
            return OfficialPolicyHttpResponse(
                status_code=exc.code,
                headers=dict(exc.headers.items()),
                body=exc.read(),
            )


@dataclass(frozen=True)
class OfficialPolicySourceRequest:
    source_id: str
    provider: str
    endpoint: str
    request_parameters: Mapping[str, object]
    parser_kind: str
    output_extension: str
    source_type: str = "primary_source"
    required_api_key_env: str = ""


@dataclass(frozen=True)
class OfficialPolicyRawPayload:
    source_id: str
    provider: str
    endpoint: str
    request_parameters: Mapping[str, object]
    source_type: str
    parser_kind: str
    downloaded_at: datetime
    status_code: int
    output_path: Path
    checksum_sha256: str
    row_count: int
    candidate_count: int


@dataclass(frozen=True)
class OfficialPolicyCandidate:
    candidate_id: str
    as_of: date
    source_id: str
    provider: str
    source_type: str
    source_name: str
    source_url: str
    source_title: str
    published_at: date | None
    captured_at: date
    matched_topics: tuple[str, ...]
    matched_risk_ids: tuple[str, ...]
    affected_tickers: tuple[str, ...]
    affected_nodes: tuple[str, ...]
    evidence_grade_floor: str
    review_status: str
    review_questions: tuple[str, ...]
    raw_payload_path: Path
    raw_payload_sha256: str
    row_count: int
    production_effect: str = "none"
    notes: str = ""


@dataclass(frozen=True)
class OfficialPolicySourceFetchReport:
    as_of: date
    since: date
    generated_at: datetime
    raw_dir: Path
    processed_dir: Path
    payloads: tuple[OfficialPolicyRawPayload, ...]
    candidates: tuple[OfficialPolicyCandidate, ...]
    skipped_sources: tuple[str, ...] = ()
    issues: tuple[OfficialPolicyIssue, ...] = field(default_factory=tuple)
    production_effect: str = "none"

    @property
    def payload_count(self) -> int:
        return len(self.payloads)

    @property
    def candidate_count(self) -> int:
        return len(self.candidates)

    @property
    def error_count(self) -> int:
        return sum(
            1 for issue in self.issues if issue.severity == OfficialPolicyIssueSeverity.ERROR
        )

    @property
    def warning_count(self) -> int:
        return sum(
            1 for issue in self.issues if issue.severity == OfficialPolicyIssueSeverity.WARNING
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


def fetch_official_policy_sources(
    *,
    as_of: date,
    since: date | None,
    raw_dir: Path = DEFAULT_OFFICIAL_POLICY_RAW_DIR,
    processed_dir: Path = DEFAULT_OFFICIAL_POLICY_PROCESSED_DIR,
    api_keys: Mapping[str, str] | None = None,
    selected_source_ids: Sequence[str] | None = None,
    limit: int = _DEFAULT_LIMIT,
    http_client: OfficialPolicyHttpClient | None = None,
    download_manifest_path: Path | None = None,
) -> OfficialPolicySourceFetchReport:
    if limit <= 0:
        raise ValueError("limit must be positive")
    since_date = since or (as_of - timedelta(days=3))
    if since_date > as_of:
        raise ValueError("since must be earlier than or equal to as_of")

    raw_dir = Path(raw_dir)
    processed_dir = Path(processed_dir)
    raw_run_dir = raw_dir / as_of.isoformat()
    raw_run_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)
    client = http_client or UrllibOfficialPolicyHttpClient()
    keys = dict(api_keys or {})
    selected_ids = set(selected_source_ids or ())

    payloads: list[OfficialPolicyRawPayload] = []
    candidates: list[OfficialPolicyCandidate] = []
    issues: list[OfficialPolicyIssue] = []
    skipped_sources: list[str] = []

    requests = build_official_policy_source_requests(
        as_of=as_of,
        since=since_date,
        api_keys=keys,
        limit=limit,
    )
    available_source_ids = {request.source_id for request in requests}
    for source_id in sorted(selected_ids - available_source_ids):
        issues.append(
            OfficialPolicyIssue(
                severity=OfficialPolicyIssueSeverity.ERROR,
                code="official_policy_source_unknown_source_id",
                source_id=source_id,
                message=f"未知官方来源 source_id：{source_id}。",
            )
        )

    for request in requests:
        if selected_ids and request.source_id not in selected_ids:
            continue
        if request.required_api_key_env and not keys.get(request.required_api_key_env):
            skipped_sources.append(request.source_id)
            issues.append(
                OfficialPolicyIssue(
                    severity=OfficialPolicyIssueSeverity.WARNING,
                    code="official_policy_source_missing_api_key",
                    source_id=request.source_id,
                    message=(
                        f"{request.source_id} 需要环境变量 "
                        f"{request.required_api_key_env}，本次未抓取。"
                    ),
                )
            )
            continue

        try:
            response = client.get(request.endpoint, headers=_headers_for(request))
        except (OSError, TimeoutError, urllib.error.URLError) as exc:
            issues.append(
                OfficialPolicyIssue(
                    severity=OfficialPolicyIssueSeverity.ERROR,
                    code="official_policy_source_download_failed",
                    source_id=request.source_id,
                    message=(
                        f"{request.source_id} 下载失败："
                        f"{type(exc).__name__}: {exc}"
                    ),
                )
            )
            continue
        output_path = _raw_output_path(raw_run_dir, request, response.body)
        output_path.write_bytes(response.body)
        checksum = _sha256_bytes(response.body)
        row_count = 0
        request_candidates: list[OfficialPolicyCandidate] = []
        if response.status_code < 400:
            row_count = _row_count(response.body, request.parser_kind)
            request_candidates = _extract_candidates(
                request=request,
                body=response.body,
                output_path=output_path,
                checksum=checksum,
                as_of=as_of,
                row_count=row_count,
            )
        if response.status_code >= 400:
            issues.append(
                OfficialPolicyIssue(
                    severity=OfficialPolicyIssueSeverity.ERROR,
                    code="official_policy_source_http_error",
                    source_id=request.source_id,
                    message=(
                        f"{request.source_id} HTTP status={response.status_code}；"
                        "已保存响应用于排查，但本来源不能视为成功抓取。"
                    ),
                )
            )
        payload = OfficialPolicyRawPayload(
            source_id=request.source_id,
            provider=request.provider,
            endpoint=request.endpoint,
            request_parameters=dict(request.request_parameters),
            source_type=request.source_type,
            parser_kind=request.parser_kind,
            downloaded_at=datetime.now(tz=UTC),
            status_code=response.status_code,
            output_path=output_path,
            checksum_sha256=checksum,
            row_count=row_count,
            candidate_count=len(request_candidates),
        )
        payloads.append(payload)
        candidates.extend(request_candidates)

    report = OfficialPolicySourceFetchReport(
        as_of=as_of,
        since=since_date,
        generated_at=datetime.now(tz=UTC),
        raw_dir=raw_dir,
        processed_dir=processed_dir,
        payloads=tuple(payloads),
        candidates=tuple(sorted(candidates, key=lambda item: item.candidate_id)),
        skipped_sources=tuple(skipped_sources),
        issues=tuple(issues),
    )
    write_official_policy_candidates_csv(
        report,
        default_official_policy_candidates_path(processed_dir, as_of),
    )
    if download_manifest_path is not None and report.payloads:
        write_download_manifest(
            output_dir=download_manifest_path.parent,
            records=_download_manifest_records(report),
            filename=download_manifest_path.name,
        )
    return report


def build_official_policy_source_requests(
    *,
    as_of: date,
    since: date,
    api_keys: Mapping[str, str],
    limit: int = _DEFAULT_LIMIT,
) -> tuple[OfficialPolicySourceRequest, ...]:
    federal_fields = [
        "document_number",
        "title",
        "publication_date",
        "type",
        "abstract",
        "excerpts",
        "html_url",
        "pdf_url",
        "raw_text_url",
        "agencies",
    ]
    requests: list[OfficialPolicySourceRequest] = [
        _federal_register_request(
            source_id="official_federal_register_policy_documents",
            term=(
                "advanced computing OR artificial intelligence OR semiconductor "
                "OR export controls OR data center"
            ),
            as_of=as_of,
            since=since,
            fields=federal_fields,
            limit=limit,
            provider="Federal Register API",
        ),
        _federal_register_request(
            source_id="official_bis_federal_register_notices",
            term=(
                "\"Bureau of Industry and Security\" OR \"Entity List\" OR "
                "\"Unverified List\" OR \"Export Administration Regulations\""
            ),
            as_of=as_of,
            since=since,
            fields=federal_fields,
            limit=limit,
            provider="Federal Register API / BIS notices",
        ),
        OfficialPolicySourceRequest(
            source_id="official_ofac_sdn_xml",
            provider="OFAC Sanctions List Service",
            endpoint=(
                "https://sanctionslistservice.ofac.treas.gov/api/"
                "PublicationPreview/exports/SDN.XML"
            ),
            request_parameters={"format": "XML", "list": "SDN"},
            parser_kind="ofac_xml",
            output_extension="xml",
        ),
        OfficialPolicySourceRequest(
            source_id="official_ofac_consolidated_xml",
            provider="OFAC Sanctions List Service",
            endpoint=(
                "https://sanctionslistservice.ofac.treas.gov/api/"
                "PublicationPreview/exports/CONSOLIDATED.XML"
            ),
            request_parameters={"format": "XML", "list": "CONSOLIDATED"},
            parser_kind="ofac_xml",
            output_extension="xml",
        ),
        OfficialPolicySourceRequest(
            source_id="official_ustr_press_releases",
            provider="United States Trade Representative",
            endpoint="https://ustr.gov/about-us/policy-offices/press-office/press-releases",
            request_parameters={"since": since.isoformat(), "as_of": as_of.isoformat()},
            parser_kind="ustr_html",
            output_extension="html",
        ),
        OfficialPolicySourceRequest(
            source_id="official_trade_csl_json",
            provider="International Trade Administration",
            endpoint=(
                "https://data.trade.gov/downloadable_consolidated_screening_list/"
                "v1/consolidated.json"
            ),
            request_parameters={"format": "JSON", "source": "downloadable_csl"},
            parser_kind="trade_csl_json",
            output_extension="json",
        ),
    ]

    congress_key = api_keys.get("CONGRESS_API_KEY", "")
    requests.append(
        OfficialPolicySourceRequest(
            source_id="official_congress_bills",
            provider="Congress.gov API",
            endpoint=_url_with_query(
                "https://api.congress.gov/v3/bill",
                {
                    "format": "json",
                    "limit": str(min(limit, 250)),
                    "fromDateTime": f"{since.isoformat()}T00:00:00Z",
                    "toDateTime": f"{(as_of + timedelta(days=1)).isoformat()}T00:00:00Z",
                    "sort": "updateDate desc",
                    "api_key": congress_key,
                },
            ),
            request_parameters={
                "fromDateTime": f"{since.isoformat()}T00:00:00Z",
                "toDateTime": f"{(as_of + timedelta(days=1)).isoformat()}T00:00:00Z",
                "limit": min(limit, 250),
                "sort": "updateDate desc",
            },
            parser_kind="congress_json",
            output_extension="json",
            required_api_key_env="CONGRESS_API_KEY",
        )
    )

    govinfo_key = api_keys.get("GOVINFO_API_KEY", "")
    requests.append(
        OfficialPolicySourceRequest(
            source_id="official_govinfo_federal_register",
            provider="GovInfo API",
            endpoint=_url_with_query(
                f"https://api.govinfo.gov/collections/FR/{since.isoformat()}T00:00:00Z",
                {
                    "pageSize": str(min(limit, 100)),
                    "offsetMark": "*",
                    "api_key": govinfo_key,
                },
            ),
            request_parameters={
                "collection": "FR",
                "since": f"{since.isoformat()}T00:00:00Z",
                "pageSize": min(limit, 100),
                "offsetMark": "*",
            },
            parser_kind="govinfo_json",
            output_extension="json",
            required_api_key_env="GOVINFO_API_KEY",
        )
    )
    return tuple(requests)


def write_official_policy_fetch_report(
    report: OfficialPolicySourceFetchReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_official_policy_fetch_report(report), encoding="utf-8")
    return output_path


def render_official_policy_fetch_report(report: OfficialPolicySourceFetchReport) -> str:
    lines = [
        "# 官方政策/地缘来源抓取报告",
        "",
        f"- 状态：{report.status}",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- 抓取窗口：{report.since.isoformat()} 至 {report.as_of.isoformat()}",
        f"- 生成时间：{report.generated_at.isoformat()}",
        f"- Raw 目录：`{report.raw_dir}`",
        (
            "- 候选 CSV："
            f"`{default_official_policy_candidates_path(report.processed_dir, report.as_of)}`"
        ),
        f"- 官方 payload 数：{report.payload_count}",
        f"- 待人工复核候选数：{report.candidate_count}",
        f"- 跳过来源数：{len(report.skipped_sources)}",
        f"- 错误数：{report.error_count}",
        f"- 警告数：{report.warning_count}",
        f"- production_effect：`{report.production_effect}`",
        "",
        "## 方法边界",
        "",
        "- 本命令只抓取和归档官方/一手低成本来源，并生成待人工复核候选。",
        "- 候选记录不会写入 `risk_event_occurrence`，不会进入评分，也不会触发仓位闸门。",
        (
            "- `Congress.gov` 和 `GovInfo` 需要 owner 在本机配置 API key；"
            "缺 key 时显式跳过并保留警告。"
        ),
        (
            "- USTR 页面和 CSL 下载结果用于人工复核线索；正式评分仍需要 source policy、"
            "发生记录和复核声明通过校验。"
        ),
        "",
        "## 来源抓取结果",
        "",
    ]
    if report.payloads:
        lines.extend(
            [
                "| Source | Provider | HTTP | Row count | 候选 | Raw payload | SHA256 |",
                "|---|---|---:|---:|---:|---|---|",
            ]
        )
        for payload in report.payloads:
            lines.append(
                "| "
                f"{payload.source_id} | "
                f"{_escape_markdown_table(payload.provider)} | "
                f"{payload.status_code} | "
                f"{payload.row_count} | "
                f"{payload.candidate_count} | "
                f"`{payload.output_path}` | "
                f"`{payload.checksum_sha256[:16]}` |"
            )
    else:
        lines.append("没有成功抓取任何 payload。")

    lines.extend(["", "## 待人工复核候选", ""])
    if report.candidates:
        lines.extend(
            [
                "| Candidate | Source | 发布日期 | 标题 | Risk ids | Topics |",
                "|---|---|---|---|---|---|",
            ]
        )
        for candidate in report.candidates[:50]:
            lines.append(
                "| "
                f"{candidate.candidate_id} | "
                f"{candidate.source_id} | "
                f"{candidate.published_at.isoformat() if candidate.published_at else ''} | "
                f"{_escape_markdown_table(candidate.source_title)[:160]} | "
                f"{', '.join(candidate.matched_risk_ids)} | "
                f"{', '.join(candidate.matched_topics)} |"
            )
        if len(report.candidates) > 50:
            hidden_count = len(report.candidates) - 50
            lines.append(
                f"| ... | ... | ... | 另有 {hidden_count} 条候选 | ... | ... |"
            )
    else:
        lines.append("未生成关键词命中的待复核候选。")

    lines.extend(["", "## 问题", ""])
    if report.issues:
        lines.extend(["| 级别 | Source | Code | 说明 |", "|---|---|---|---|"])
        for issue in report.issues:
            lines.append(
                "| "
                f"{_issue_label(issue.severity)} | "
                f"{issue.source_id or ''} | "
                f"{issue.code} | "
                f"{_escape_markdown_table(issue.message)} |"
            )
    else:
        lines.append("未发现问题。")
    return "\n".join(lines) + "\n"


def write_official_policy_candidates_csv(
    report: OfficialPolicySourceFetchReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=OFFICIAL_POLICY_CANDIDATE_COLUMNS)
        writer.writeheader()
        for candidate in report.candidates:
            writer.writerow(_candidate_to_row(candidate))
    return output_path


def load_official_policy_candidates_csv(input_path: Path) -> tuple[OfficialPolicyCandidate, ...]:
    input_path = Path(input_path)
    with input_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        missing_columns = set(OFFICIAL_POLICY_CANDIDATE_COLUMNS) - set(reader.fieldnames or ())
        if missing_columns:
            raise ValueError(
                "官方候选 CSV 缺少必需字段：" + ", ".join(sorted(missing_columns))
            )
        return tuple(
            _candidate_from_row(row, row_number=index)
            for index, row in enumerate(reader, 2)
        )


def default_official_policy_fetch_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"official_policy_sources_{as_of.isoformat()}.md"


def default_official_policy_candidates_path(processed_dir: Path, as_of: date) -> Path:
    return processed_dir / f"official_policy_source_candidates_{as_of.isoformat()}.csv"


def _federal_register_request(
    *,
    source_id: str,
    term: str,
    as_of: date,
    since: date,
    fields: Sequence[str],
    limit: int,
    provider: str,
) -> OfficialPolicySourceRequest:
    params: dict[str, object] = {
        "per_page": min(limit, 1000),
        "order": "newest",
        "conditions[term]": term,
        "conditions[publication_date][gte]": since.isoformat(),
        "conditions[publication_date][lte]": as_of.isoformat(),
    }
    query_items = [(key, str(value)) for key, value in params.items()]
    query_items.extend(("fields[]", field_name) for field_name in fields)
    return OfficialPolicySourceRequest(
        source_id=source_id,
        provider=provider,
        endpoint=_url_with_query(
            "https://www.federalregister.gov/api/v1/documents.json",
            query_items,
        ),
        request_parameters={**params, "fields": tuple(fields)},
        parser_kind="federal_register_json",
        output_extension="json",
    )


def _headers_for(request: OfficialPolicySourceRequest) -> dict[str, str]:
    headers = {"User-Agent": _USER_AGENT}
    if request.output_extension == "json":
        headers["Accept"] = "application/json"
    elif request.output_extension == "xml":
        headers["Accept"] = "application/xml,text/xml,*/*"
    else:
        headers["Accept"] = "text/html,application/xhtml+xml,*/*"
    return headers


def _extract_candidates(
    *,
    request: OfficialPolicySourceRequest,
    body: bytes,
    output_path: Path,
    checksum: str,
    as_of: date,
    row_count: int,
) -> list[OfficialPolicyCandidate]:
    if request.parser_kind in {"federal_register_json", "congress_json", "govinfo_json"}:
        return _json_record_candidates(
            request=request,
            body=body,
            output_path=output_path,
            checksum=checksum,
            as_of=as_of,
            row_count=row_count,
        )
    if request.parser_kind == "trade_csl_json":
        return _trade_csl_candidates(
            request=request,
            body=body,
            output_path=output_path,
            checksum=checksum,
            as_of=as_of,
            row_count=row_count,
        )
    if request.parser_kind == "ofac_xml":
        return _ofac_xml_candidates(
            request=request,
            body=body,
            output_path=output_path,
            checksum=checksum,
            as_of=as_of,
            row_count=row_count,
        )
    if request.parser_kind == "ustr_html":
        return _ustr_html_candidates(
            request=request,
            body=body,
            output_path=output_path,
            checksum=checksum,
            as_of=as_of,
            row_count=row_count,
        )
    return []


def _json_record_candidates(
    *,
    request: OfficialPolicySourceRequest,
    body: bytes,
    output_path: Path,
    checksum: str,
    as_of: date,
    row_count: int,
) -> list[OfficialPolicyCandidate]:
    data = _load_json(body)
    if not isinstance(data, dict):
        return []
    records = _records_for_json_kind(data, request.parser_kind)
    candidates: list[OfficialPolicyCandidate] = []
    for index, record in enumerate(records):
        title = _record_title(record)
        source_url = _record_url(record)
        published_at = _parse_optional_date(_record_date(record))
        haystack = json.dumps(record, ensure_ascii=False, sort_keys=True)
        topics = _matched_topics(haystack)
        if not topics:
            continue
        risk_ids, tickers, nodes = _risk_mapping_for(topics, haystack)
        candidates.append(
            _candidate(
                request=request,
                as_of=as_of,
                output_path=output_path,
                checksum=checksum,
                row_count=row_count,
                source_url=source_url,
                source_title=title,
                published_at=published_at,
                topics=topics,
                risk_ids=risk_ids,
                affected_tickers=tickers,
                affected_nodes=nodes,
                record_key=str(record.get("document_number") or record.get("packageId") or index),
            )
        )
    return candidates


def _trade_csl_candidates(
    *,
    request: OfficialPolicySourceRequest,
    body: bytes,
    output_path: Path,
    checksum: str,
    as_of: date,
    row_count: int,
) -> list[OfficialPolicyCandidate]:
    data = _load_json(body)
    records = _flatten_json_records(data)
    candidates: list[OfficialPolicyCandidate] = []
    for index, record in enumerate(records[:2000]):
        haystack = json.dumps(record, ensure_ascii=False, sort_keys=True)
        topics = _matched_topics(haystack)
        if not topics:
            continue
        risk_ids, tickers, nodes = _risk_mapping_for(topics, haystack)
        title = _first_text(
            record,
            ("name", "alt_names", "source", "programs", "remarks", "title"),
        )
        source_url = _first_text(record, ("source_information_url", "url")) or request.endpoint
        candidates.append(
            _candidate(
                request=request,
                as_of=as_of,
                output_path=output_path,
                checksum=checksum,
                row_count=row_count,
                source_url=source_url,
                source_title=title or f"CSL record {index + 1}",
                published_at=None,
                topics=topics,
                risk_ids=risk_ids,
                affected_tickers=tickers,
                affected_nodes=nodes,
                record_key=str(record.get("id") or record.get("uid") or index),
            )
        )
        if len(candidates) >= 100:
            break
    return candidates


def _ofac_xml_candidates(
    *,
    request: OfficialPolicySourceRequest,
    body: bytes,
    output_path: Path,
    checksum: str,
    as_of: date,
    row_count: int,
) -> list[OfficialPolicyCandidate]:
    try:
        root = ET.fromstring(body)
    except ET.ParseError:
        return []
    candidates: list[OfficialPolicyCandidate] = []
    for index, element in enumerate(root.iter()):
        tag = _strip_namespace(element.tag).lower()
        if tag not in {"sdnentry", "entry", "sanctionsentry", "profile"}:
            continue
        text = " ".join(value.strip() for value in element.itertext() if value.strip())
        topics = _matched_topics(text)
        if not topics:
            continue
        risk_ids, tickers, nodes = _risk_mapping_for(topics, text)
        title = _ofac_title(element) or f"OFAC record {index + 1}"
        candidates.append(
            _candidate(
                request=request,
                as_of=as_of,
                output_path=output_path,
                checksum=checksum,
                row_count=row_count,
                source_url=request.endpoint,
                source_title=title,
                published_at=None,
                topics=topics,
                risk_ids=risk_ids,
                affected_tickers=tickers,
                affected_nodes=nodes,
                record_key=str(index),
            )
        )
        if len(candidates) >= 100:
            break
    return candidates


def _ustr_html_candidates(
    *,
    request: OfficialPolicySourceRequest,
    body: bytes,
    output_path: Path,
    checksum: str,
    as_of: date,
    row_count: int,
) -> list[OfficialPolicyCandidate]:
    parser = _AnchorParser()
    parser.feed(_decode_text(body))
    candidates: list[OfficialPolicyCandidate] = []
    for index, anchor in enumerate(parser.anchors):
        title = " ".join(anchor.text.split())
        if not title:
            continue
        topics = _matched_topics(title)
        if not topics:
            continue
        risk_ids, tickers, nodes = _risk_mapping_for(topics, title)
        href = urllib.parse.urljoin(request.endpoint, anchor.href)
        candidates.append(
            _candidate(
                request=request,
                as_of=as_of,
                output_path=output_path,
                checksum=checksum,
                row_count=row_count,
                source_url=href,
                source_title=title,
                published_at=None,
                topics=topics,
                risk_ids=risk_ids,
                affected_tickers=tickers,
                affected_nodes=nodes,
                record_key=str(index),
            )
        )
    return candidates[:100]


def _candidate(
    *,
    request: OfficialPolicySourceRequest,
    as_of: date,
    output_path: Path,
    checksum: str,
    row_count: int,
    source_url: str,
    source_title: str,
    published_at: date | None,
    topics: tuple[str, ...],
    risk_ids: tuple[str, ...],
    affected_tickers: tuple[str, ...],
    affected_nodes: tuple[str, ...],
    record_key: str,
) -> OfficialPolicyCandidate:
    key = _safe_token(f"{request.source_id}_{record_key}_{source_title}")[:80]
    candidate_id = f"official:{request.source_id}:{key}"
    return OfficialPolicyCandidate(
        candidate_id=candidate_id,
        as_of=as_of,
        source_id=request.source_id,
        provider=request.provider,
        source_type=request.source_type,
        source_name=request.provider,
        source_url=source_url or request.endpoint,
        source_title=source_title or request.source_id,
        published_at=published_at,
        captured_at=as_of,
        matched_topics=topics,
        matched_risk_ids=risk_ids,
        affected_tickers=affected_tickers,
        affected_nodes=affected_nodes,
        evidence_grade_floor="A",
        review_status="pending_review",
        review_questions=(
            "是否为官方/一手来源且内容已经生效？",
            "是否匹配现有 risk_event_id，还是需要新增风险规则？",
            "是否影响 AI 相关 ticker、产业链节点、thesis 或仓位闸门？",
        ),
        raw_payload_path=output_path,
        raw_payload_sha256=checksum,
        row_count=row_count,
        notes=(
            "官方来源候选仅供人工复核；未确认前不得写入 active occurrence、"
            "不得评分、不得触发 position gate。"
        ),
    )


def _row_count(body: bytes, parser_kind: str) -> int:
    if parser_kind.endswith("_json"):
        return len(_flatten_json_records(_load_json(body)))
    if parser_kind == "ofac_xml":
        try:
            root = ET.fromstring(body)
        except ET.ParseError:
            return 0
        return sum(
            1
            for element in root.iter()
            if _strip_namespace(element.tag).lower()
            in {"sdnentry", "entry", "sanctionsentry", "profile"}
        )
    if parser_kind == "ustr_html":
        parser = _AnchorParser()
        parser.feed(_decode_text(body))
        return len(parser.anchors)
    return 0


def _records_for_json_kind(data: dict[str, Any], parser_kind: str) -> list[dict[str, Any]]:
    if parser_kind == "federal_register_json":
        return [item for item in data.get("results", []) if isinstance(item, dict)]
    if parser_kind == "congress_json":
        return [item for item in data.get("bills", []) if isinstance(item, dict)]
    if parser_kind == "govinfo_json":
        return [item for item in data.get("packages", []) if isinstance(item, dict)]
    return _flatten_json_records(data)


def _flatten_json_records(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if not isinstance(data, dict):
        return []
    for key in ("results", "data", "records", "entities", "items"):
        value = data.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    if any(isinstance(value, (str, int, float, bool)) for value in data.values()):
        return [data]
    records: list[dict[str, Any]] = []
    for value in data.values():
        if isinstance(value, list):
            records.extend(item for item in value if isinstance(item, dict))
    return records


def _record_title(record: Mapping[str, Any]) -> str:
    latest_action = record.get("latestAction")
    action_text = ""
    if isinstance(latest_action, dict):
        action_text = str(latest_action.get("text") or "")
    title = _first_text(record, ("title", "shortTitle", "name"))
    return title or action_text or "untitled official record"


def _record_url(record: Mapping[str, Any]) -> str:
    return (
        _first_text(
            record,
            ("html_url", "url", "packageLink", "detailsLink", "pdf_url", "raw_text_url"),
        )
        or ""
    )


def _record_date(record: Mapping[str, Any]) -> str:
    latest_action = record.get("latestAction")
    if isinstance(latest_action, dict):
        action_date = latest_action.get("actionDate")
        if action_date:
            return str(action_date)
    return _first_text(
        record,
        ("publication_date", "updateDate", "dateIssued", "lastModified", "introducedDate"),
    )


def _matched_topics(text: str) -> tuple[str, ...]:
    lowered = text.lower()
    topics: list[str] = []
    topic_keywords = {
        "export_controls": (
            "export control",
            "export administration regulations",
            "ear",
            "entity list",
            "unverified list",
            "military end user",
            "advanced computing",
            "semiconductor",
            "ai chip",
            "gpu",
            "data center",
        ),
        "sanctions": ("sanction", "ofac", "sdn", "cmic", "sectoral sanctions"),
        "trade_policy": ("section 301", "tariff", "ustr", "trade investigation"),
        "taiwan_geopolitics": ("taiwan", "taiwan strait"),
        "china_technology": ("china", "prc", "people's republic of china"),
        "russia_geopolitics": ("russia", "russian federation"),
        "ai_policy": ("artificial intelligence", "advanced computing", "cloud computing"),
    }
    for topic, keywords in topic_keywords.items():
        if any(_contains_keyword(lowered, keyword) for keyword in keywords):
            topics.append(topic)
    return tuple(topics)


def _contains_keyword(lowered_text: str, keyword: str) -> bool:
    normalized = keyword.lower()
    if len(normalized) <= 3 and normalized.replace(" ", "").isalnum():
        return re.search(
            rf"(?<![a-z0-9]){re.escape(normalized)}(?![a-z0-9])",
            lowered_text,
        ) is not None
    return normalized in lowered_text


def _risk_mapping_for(
    topics: tuple[str, ...],
    text: str,
) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    risk_ids: list[str] = []
    nodes: list[str] = []
    tickers: list[str] = []
    topic_set = set(topics)
    if topic_set & {"export_controls", "sanctions", "china_technology", "ai_policy"}:
        risk_ids.append("ai_chip_export_control_upgrade")
        nodes.extend(("export_controls", "gpu_asic_demand", "semiconductor_equipment"))
        tickers.extend(("NVDA", "AMD", "TSM", "INTC"))
    if "taiwan_geopolitics" in topic_set:
        risk_ids.append("taiwan_geopolitical_escalation")
        nodes.extend(("foundry_demand", "advanced_packaging", "export_controls"))
        tickers.extend(("TSM", "NVDA", "AMD", "SMH", "SOXX"))
    if "trade_policy" in topic_set:
        nodes.append("export_controls")
    upper_text = text.upper()
    for ticker in ("NVDA", "AMD", "TSM", "INTC", "ASML", "SMH", "SOXX", "MSFT", "GOOG"):
        if ticker in upper_text:
            tickers.append(ticker)
    return (
        _dedupe_tuple(risk_ids),
        _dedupe_tuple(tickers),
        _dedupe_tuple(nodes),
    )


def _ofac_title(element: ET.Element) -> str:
    pieces: list[str] = []
    for child in element.iter():
        tag = _strip_namespace(child.tag).lower()
        if tag in {"lastname", "firstname", "name", "sdnname", "primaryname"} and child.text:
            pieces.append(child.text.strip())
    return " ".join(piece for piece in pieces if piece).strip()


def _candidate_to_row(candidate: OfficialPolicyCandidate) -> dict[str, object]:
    return {
        "candidate_id": candidate.candidate_id,
        "as_of": candidate.as_of.isoformat(),
        "source_id": candidate.source_id,
        "provider": candidate.provider,
        "source_type": candidate.source_type,
        "source_name": candidate.source_name,
        "source_url": candidate.source_url,
        "source_title": candidate.source_title,
        "published_at": candidate.published_at.isoformat() if candidate.published_at else "",
        "captured_at": candidate.captured_at.isoformat(),
        "matched_topics": ";".join(candidate.matched_topics),
        "matched_risk_ids": ";".join(candidate.matched_risk_ids),
        "affected_tickers": ";".join(candidate.affected_tickers),
        "affected_nodes": ";".join(candidate.affected_nodes),
        "evidence_grade_floor": candidate.evidence_grade_floor,
        "review_status": candidate.review_status,
        "review_questions": ";".join(candidate.review_questions),
        "raw_payload_path": str(candidate.raw_payload_path),
        "raw_payload_sha256": candidate.raw_payload_sha256,
        "row_count": candidate.row_count,
        "production_effect": candidate.production_effect,
        "notes": candidate.notes,
    }


def _candidate_from_row(
    row: Mapping[str, str],
    *,
    row_number: int,
) -> OfficialPolicyCandidate:
    candidate_id = row.get("candidate_id", "")
    try:
        row_count = int(row.get("row_count", "0") or 0)
    except ValueError as exc:
        raise ValueError(
            f"官方候选 CSV 第 {row_number} 行 row_count 不是整数：{candidate_id}"
        ) from exc
    return OfficialPolicyCandidate(
        candidate_id=candidate_id,
        as_of=_parse_required_date(row.get("as_of", ""), "as_of", row_number),
        source_id=row.get("source_id", ""),
        provider=row.get("provider", ""),
        source_type=row.get("source_type", ""),
        source_name=row.get("source_name", ""),
        source_url=row.get("source_url", ""),
        source_title=row.get("source_title", ""),
        published_at=_parse_optional_date(row.get("published_at", "")),
        captured_at=_parse_required_date(
            row.get("captured_at", ""),
            "captured_at",
            row_number,
        ),
        matched_topics=_split_semicolon_items(row.get("matched_topics", "")),
        matched_risk_ids=_split_semicolon_items(row.get("matched_risk_ids", "")),
        affected_tickers=_split_semicolon_items(row.get("affected_tickers", "")),
        affected_nodes=_split_semicolon_items(row.get("affected_nodes", "")),
        evidence_grade_floor=row.get("evidence_grade_floor", ""),
        review_status=row.get("review_status", ""),
        review_questions=_split_semicolon_items(row.get("review_questions", "")),
        raw_payload_path=Path(row.get("raw_payload_path", "")),
        raw_payload_sha256=row.get("raw_payload_sha256", ""),
        row_count=row_count,
        production_effect=row.get("production_effect", "none") or "none",
        notes=row.get("notes", ""),
    )


def _download_manifest_records(
    report: OfficialPolicySourceFetchReport,
) -> tuple[dict[str, object], ...]:
    return tuple(
        {
            "downloaded_at": payload.downloaded_at.isoformat(),
            "source_id": payload.source_id,
            "provider": payload.provider,
            "endpoint": _redact_api_key(payload.endpoint),
            "request_parameters": json.dumps(
                payload.request_parameters,
                ensure_ascii=False,
                sort_keys=True,
            ),
            "output_path": str(payload.output_path),
            "row_count": payload.row_count,
            "checksum_sha256": payload.checksum_sha256,
        }
        for payload in report.payloads
    )


def _raw_output_path(
    raw_run_dir: Path,
    request: OfficialPolicySourceRequest,
    body: bytes,
) -> Path:
    digest = _sha256_bytes(body)[:12]
    return raw_run_dir / f"{request.source_id}_{digest}.{request.output_extension}"


def _load_json(body: bytes) -> Any:
    try:
        return json.loads(_decode_text(body))
    except json.JSONDecodeError:
        return {}


def _decode_text(body: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return body.decode(encoding)
        except UnicodeDecodeError:
            continue
    return body.decode("utf-8", errors="replace")


def _url_with_query(
    base_url: str,
    params: Mapping[str, str] | Sequence[tuple[str, str]],
) -> str:
    return f"{base_url}?{urllib.parse.urlencode(params)}"


def _first_text(record: Mapping[str, Any], keys: Sequence[str]) -> str:
    for key in keys:
        value = record.get(key)
        if value is None:
            continue
        if isinstance(value, list):
            joined = "; ".join(str(item) for item in value if item)
            if joined:
                return joined
        else:
            text = str(value).strip()
            if text:
                return text
    return ""


def _parse_optional_date(value: str) -> date | None:
    if not value:
        return None
    value = value.strip()
    if "T" in value:
        value = value.split("T", 1)[0]
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


def _parse_required_date(value: str, field_name: str, row_number: int) -> date:
    parsed = _parse_optional_date(value)
    if parsed is None:
        raise ValueError(f"官方候选 CSV 第 {row_number} 行 {field_name} 不是有效日期。")
    return parsed


def _split_semicolon_items(value: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in value.split(";") if item.strip())


def _strip_namespace(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[1]
    return tag


def _safe_token(value: str) -> str:
    token = re.sub(r"[^A-Za-z0-9_.:-]+", "_", value).strip("_")
    return token or "record"


def _sha256_bytes(value: bytes) -> str:
    return sha256(value).hexdigest()


def _dedupe_tuple(values: Sequence[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            result.append(value)
    return tuple(result)


def _redact_api_key(url: str) -> str:
    parsed = urllib.parse.urlsplit(url)
    pairs = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    redacted = [
        (key, "***" if key.lower() == "api_key" and value else value)
        for key, value in pairs
    ]
    return urllib.parse.urlunsplit(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            urllib.parse.urlencode(redacted),
            parsed.fragment,
        )
    )


def _escape_markdown_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ").replace("\r", " ")


def _issue_label(severity: OfficialPolicyIssueSeverity) -> str:
    return "错误" if severity == OfficialPolicyIssueSeverity.ERROR else "警告"


@dataclass(frozen=True)
class _Anchor:
    href: str
    text: str


class _AnchorParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.anchors: list[_Anchor] = []
        self._current_href: str | None = None
        self._current_text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        href = dict(attrs).get("href")
        if href:
            self._current_href = href
            self._current_text = []

    def handle_data(self, data: str) -> None:
        if self._current_href is not None:
            self._current_text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() != "a" or self._current_href is None:
            return
        text = " ".join(part.strip() for part in self._current_text if part.strip())
        if text:
            self.anchors.append(_Anchor(href=self._current_href, text=text))
        self._current_href = None
        self._current_text = []
