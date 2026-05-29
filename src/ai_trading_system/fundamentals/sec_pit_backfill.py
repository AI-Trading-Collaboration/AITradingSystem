from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime
from hashlib import sha256
from importlib import import_module
from pathlib import Path
from typing import Any, Protocol, cast

import pandas as pd

from ai_trading_system.config import (
    DEFAULT_FUNDAMENTAL_FEATURES_CONFIG_PATH,
    DEFAULT_FUNDAMENTAL_METRICS_CONFIG_PATH,
    DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    PROJECT_ROOT,
    SecCompaniesConfig,
    SecCompanyConfig,
    dedupe_preserving_order,
    load_fundamental_features,
    load_fundamental_metrics,
    load_sec_companies,
)
from ai_trading_system.external_request_cache import (
    CachedHttpResponse,
    default_external_request_cache_dir,
    lookup_external_request_cache,
    write_external_request_cache_response,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

SEC_PIT_TASK_ID = "TRADING-039"
SEC_PIT_BACKTEST_DATA_GRADE = "B_RECONSTRUCTED_SEC_FILING_PIT"
SEC_PIT_CURRENT_HISTORY_GRADE = "C_CURRENT_HISTORY_APPROX"
DEFAULT_SEC_PIT_BACKFILL_CONFIG_PATH = PROJECT_ROOT / "config" / "sec_pit_backfill.yaml"
DEFAULT_SEC_EDGAR_RAW_DIR = PROJECT_ROOT / "data" / "raw" / "sec_edgar"
DEFAULT_SEC_EDGAR_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed" / "sec_edgar"
DEFAULT_SEC_PIT_REPORT_DIR = PROJECT_ROOT / "outputs" / "reports" / "sec_pit_backfill"
SEC_PIT_RAW_MANIFEST_COLUMNS = (
    "downloaded_at",
    "provider",
    "source_id",
    "source_endpoint",
    "request_parameters",
    "ticker",
    "cik",
    "payload_type",
    "output_path",
    "row_count",
    "checksum_sha256",
    "production_effect",
    "backtest_data_grade",
    "strict_vendor_archive",
    "external_side_effects",
    "broker_access_required",
    "paid_vendor_required",
    "retroactive_strict_pit",
    "manual_review_required_for_grade_upgrade",
)


@dataclass(frozen=True)
class SecPitBackfillConfig:
    daily_availability_policy: str
    intraday_policy_enabled: bool
    max_requests_per_second: float
    allowed_forms: tuple[str, ...]
    metric_panel_forms: tuple[str, ...]
    six_k_default_grade: str
    coverage_warning_threshold: float
    coverage_error_threshold: float
    stale_quarterly_days: int
    stale_annual_days: int
    policy_version: str = "sec_pit_backfill.v1"


@dataclass(frozen=True)
class SecPitRawFile:
    ticker: str
    cik: str
    payload_type: str
    output_path: Path
    row_count: int
    checksum_sha256: str


@dataclass(frozen=True)
class SecPitRawFetchSummary:
    raw_dir: Path
    manifest_path: Path
    files: tuple[SecPitRawFile, ...]

    @property
    def file_count(self) -> int:
        return len(self.files)

    @property
    def ticker_count(self) -> int:
        return len({file.ticker for file in self.files})


class SecPitRawProvider(Protocol):
    def download_submissions_raw(self, ticker: str, cik: str, *, use_cache: bool = True) -> bytes:
        """Download SEC submissions raw JSON bytes."""

    def download_companyfacts_raw(self, ticker: str, cik: str, *, use_cache: bool = True) -> bytes:
        """Download SEC companyfacts raw JSON bytes."""

    def submissions_endpoint_for(self, cik: str) -> str:
        """Return submissions endpoint for audit metadata."""

    def companyfacts_endpoint_for(self, cik: str) -> str:
        """Return companyfacts endpoint for audit metadata."""


class SecPitEdgarProvider:
    submissions_base_url = "https://data.sec.gov/submissions"
    companyfacts_base_url = "https://data.sec.gov/api/xbrl/companyfacts"

    def __init__(
        self,
        user_agent: str,
        *,
        max_requests_per_second: float = 5.0,
        requests_module: Any | None = None,
        request_cache_dir: Path | str | None = None,
        max_retries: int = 3,
    ) -> None:
        if not user_agent.strip():
            raise ValueError("SEC User-Agent must not be empty")
        if max_requests_per_second <= 0:
            raise ValueError("max_requests_per_second must be positive")
        if max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        self.user_agent = user_agent.strip()
        self.max_requests_per_second = max_requests_per_second
        self._requests_module = requests_module
        self._request_cache_dir = request_cache_dir
        self._max_retries = max_retries
        self._last_live_request_time = 0.0

    def download_submissions_raw(self, ticker: str, cik: str, *, use_cache: bool = True) -> bytes:
        del ticker
        response = self._request_raw(
            url=self.submissions_endpoint_for(cik),
            api_family="sec_pit_submissions",
            use_cache=use_cache,
        )
        response.raise_for_status()
        return bytes(response.content)

    def download_companyfacts_raw(self, ticker: str, cik: str, *, use_cache: bool = True) -> bytes:
        del ticker
        response = self._request_raw(
            url=self.companyfacts_endpoint_for(cik),
            api_family="sec_pit_companyfacts",
            use_cache=use_cache,
        )
        response.raise_for_status()
        return bytes(response.content)

    def submissions_endpoint_for(self, cik: str) -> str:
        return f"{self.submissions_base_url}/CIK{cik}.json"

    def companyfacts_endpoint_for(self, cik: str) -> str:
        return f"{self.companyfacts_base_url}/CIK{cik}.json"

    def _request_raw(
        self,
        *,
        url: str,
        api_family: str,
        use_cache: bool,
    ) -> CachedHttpResponse:
        requests = self._requests_module or cast(Any, import_module("requests"))
        cache_dir = default_external_request_cache_dir(
            requests_module=self._requests_module,
            explicit_cache_dir=self._request_cache_dir,
        )
        headers = {
            "User-Agent": self.user_agent,
            "Accept-Encoding": "gzip, deflate",
            "Accept": "application/json",
        }
        if use_cache and cache_dir is not None:
            lookup = lookup_external_request_cache(
                provider="SEC EDGAR",
                api_family=api_family,
                method="GET",
                url=url,
                headers=headers,
                cache_dir=Path(cache_dir),
            )
            if lookup.response is not None and lookup.response.status_code not in {403, 429}:
                return lookup.response

        retry_statuses = {403, 429}
        response: CachedHttpResponse | None = None
        for attempt in range(self._max_retries + 1):
            self._throttle()
            live_response = requests.get(url, headers=headers, timeout=30)
            status_code = int(getattr(live_response, "status_code", 200))
            response_headers = _headers_from_response(live_response)
            content = _content_from_response(live_response)
            response = _write_or_build_cached_response(
                provider="SEC EDGAR",
                api_family=api_family,
                url=url,
                headers=headers,
                status_code=status_code,
                response_headers=response_headers,
                content=content,
                cache_dir=Path(cache_dir) if cache_dir is not None else None,
            )
            if status_code not in retry_statuses or attempt == self._max_retries:
                return response
            time.sleep(min(2.0**attempt, 30.0))
        if response is None:
            raise RuntimeError("SEC EDGAR request did not produce a response")
        return response

    def _throttle(self) -> None:
        min_interval = 1.0 / self.max_requests_per_second
        now = time.monotonic()
        elapsed = now - self._last_live_request_time
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        self._last_live_request_time = time.monotonic()


def load_sec_pit_backfill_config(
    path: Path | str = DEFAULT_SEC_PIT_BACKFILL_CONFIG_PATH,
) -> SecPitBackfillConfig:
    raw_path = Path(path)
    raw = safe_load_yaml_path(raw_path) or {}
    section = raw.get("sec_pit_backfill", raw)
    if not isinstance(section, dict):
        raise ValueError("sec_pit_backfill config must be a mapping")
    return SecPitBackfillConfig(
        daily_availability_policy=str(section.get("daily_availability_policy", "next_trading_day")),
        intraday_policy_enabled=bool(section.get("intraday_policy_enabled", False)),
        max_requests_per_second=float(section.get("max_requests_per_second", 5)),
        allowed_forms=tuple(str(item).upper() for item in section.get("allowed_forms", ())),
        metric_panel_forms=tuple(
            str(item).upper()
            for item in section.get("metric_panel_forms", section.get("allowed_forms", ()))
        ),
        six_k_default_grade=str(section.get("six_k_default_grade", SEC_PIT_CURRENT_HISTORY_GRADE)),
        coverage_warning_threshold=float(section.get("coverage_warning_threshold", 0.70)),
        coverage_error_threshold=float(section.get("coverage_error_threshold", 0.40)),
        stale_quarterly_days=int(section.get("stale_quarterly_days", 180)),
        stale_annual_days=int(section.get("stale_annual_days", 540)),
        policy_version=str(section.get("policy_version", "sec_pit_backfill.v1")),
    )


def fetch_sec_pit_raw(
    *,
    sec_companies: SecCompaniesConfig,
    provider: SecPitRawProvider,
    raw_dir: Path = DEFAULT_SEC_EDGAR_RAW_DIR,
    tickers: list[str] | None = None,
    use_cache: bool = True,
) -> SecPitRawFetchSummary:
    companies = selected_sec_pit_companies(sec_companies, tickers)
    if not companies:
        raise ValueError("no active SEC companies selected for SEC PIT raw fetch")

    submissions_dir = raw_dir / "submissions"
    companyfacts_dir = raw_dir / "companyfacts"
    manifest_dir = raw_dir / "manifest"
    submissions_dir.mkdir(parents=True, exist_ok=True)
    companyfacts_dir.mkdir(parents=True, exist_ok=True)
    manifest_dir.mkdir(parents=True, exist_ok=True)

    downloaded_at = datetime.now(tz=UTC).isoformat()
    records: list[dict[str, object]] = []
    files: list[SecPitRawFile] = []
    for company in companies:
        submissions_payload = provider.download_submissions_raw(
            company.ticker,
            company.cik,
            use_cache=use_cache,
        )
        submissions_path = sec_pit_submissions_path(raw_dir, company.ticker, company.cik)
        submissions_file = _write_raw_payload(
            payload=submissions_payload,
            output_path=submissions_path,
            ticker=company.ticker,
            cik=company.cik,
            payload_type="submissions",
            row_count=_submissions_row_count(submissions_payload),
        )
        files.append(submissions_file)
        records.append(
            _raw_manifest_record(
                downloaded_at=downloaded_at,
                company=company,
                payload_type="submissions",
                endpoint=provider.submissions_endpoint_for(company.cik),
                file=submissions_file,
            )
        )

        companyfacts_payload = provider.download_companyfacts_raw(
            company.ticker,
            company.cik,
            use_cache=use_cache,
        )
        companyfacts_path = sec_pit_companyfacts_path(raw_dir, company.ticker, company.cik)
        companyfacts_file = _write_raw_payload(
            payload=companyfacts_payload,
            output_path=companyfacts_path,
            ticker=company.ticker,
            cik=company.cik,
            payload_type="companyfacts",
            row_count=_companyfacts_row_count(companyfacts_payload),
        )
        files.append(companyfacts_file)
        records.append(
            _raw_manifest_record(
                downloaded_at=downloaded_at,
                company=company,
                payload_type="companyfacts",
                endpoint=provider.companyfacts_endpoint_for(company.cik),
                file=companyfacts_file,
            )
        )

    manifest_path = write_sec_pit_raw_manifest(
        pd.DataFrame(records, columns=list(SEC_PIT_RAW_MANIFEST_COLUMNS)),
        manifest_dir / "sec_edgar_raw_manifest.csv",
    )
    return SecPitRawFetchSummary(
        raw_dir=raw_dir,
        manifest_path=manifest_path,
        files=tuple(files),
    )


def run_sec_pit_backfill(
    *,
    start: date,
    end: date,
    raw_dir: Path = DEFAULT_SEC_EDGAR_RAW_DIR,
    processed_dir: Path = DEFAULT_SEC_EDGAR_PROCESSED_DIR,
    report_dir: Path = DEFAULT_SEC_PIT_REPORT_DIR,
    sec_companies_path: Path = DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    metrics_path: Path = DEFAULT_FUNDAMENTAL_METRICS_CONFIG_PATH,
    features_path: Path = DEFAULT_FUNDAMENTAL_FEATURES_CONFIG_PATH,
    config_path: Path = DEFAULT_SEC_PIT_BACKFILL_CONFIG_PATH,
    provider: SecPitRawProvider | None = None,
    user_agent: str | None = None,
    tickers: list[str] | None = None,
    use_cache: bool = True,
    full_pipeline: bool = True,
) -> dict[str, Path]:
    if start > end:
        raise ValueError("start must be on or before end")
    sec_companies = load_sec_companies(sec_companies_path)
    policy = load_sec_pit_backfill_config(config_path)
    artifacts: dict[str, Path] = {}
    if full_pipeline:
        if provider is None:
            if not user_agent or not user_agent.strip():
                raise ValueError("SEC PIT backfill requires a SEC User-Agent")
            provider = SecPitEdgarProvider(
                user_agent=user_agent,
                max_requests_per_second=policy.max_requests_per_second,
            )
        raw_summary = fetch_sec_pit_raw(
            sec_companies=sec_companies,
            provider=provider,
            raw_dir=raw_dir,
            tickers=tickers,
            use_cache=use_cache,
        )
        artifacts["raw_manifest"] = raw_summary.manifest_path

    from ai_trading_system.fundamentals.sec_filing_timeline import build_filing_timeline_csv
    from ai_trading_system.fundamentals.sec_pit_metrics import build_mapped_metrics_csv
    from ai_trading_system.fundamentals.sec_pit_panel import (
        build_fundamental_pit_daily_panel_csv,
        build_fundamental_pit_intervals_csv,
        build_sec_pit_feature_panel_csv,
    )
    from ai_trading_system.fundamentals.sec_pit_validation import (
        validate_and_write_sec_pit_artifacts,
    )
    from ai_trading_system.fundamentals.sec_xbrl_facts import build_xbrl_facts_long_csv

    processed_dir.mkdir(parents=True, exist_ok=True)
    timeline_path = build_filing_timeline_csv(
        sec_companies=sec_companies,
        raw_dir=raw_dir,
        start=start,
        end=end,
        output_path=processed_dir / "filing_timeline.csv",
    )
    artifacts["filing_timeline"] = timeline_path
    facts_path = build_xbrl_facts_long_csv(
        sec_companies=sec_companies,
        raw_dir=raw_dir,
        filing_timeline_path=timeline_path,
        end=end,
        output_path=processed_dir / "xbrl_facts_long.csv",
    )
    artifacts["xbrl_facts_long"] = facts_path
    metrics_output = build_mapped_metrics_csv(
        facts_path=facts_path,
        metrics=load_fundamental_metrics(metrics_path),
        policy=policy,
        end=end,
        output_path=processed_dir / "mapped_metrics_long.csv",
    )
    artifacts["mapped_metrics_long"] = metrics_output
    intervals_path = build_fundamental_pit_intervals_csv(
        mapped_metrics_path=metrics_output,
        output_path=processed_dir / "fundamental_pit_intervals.csv",
    )
    artifacts["fundamental_pit_intervals"] = intervals_path
    daily_panel_path = build_fundamental_pit_daily_panel_csv(
        intervals_path=intervals_path,
        start=start,
        end=end,
        output_path=processed_dir / "fundamental_pit_daily_panel.csv",
    )
    artifacts["fundamental_pit_daily_panel"] = daily_panel_path
    feature_panel_path = build_sec_pit_feature_panel_csv(
        intervals_path=intervals_path,
        features=load_fundamental_features(features_path),
        sec_companies=sec_companies,
        start=start,
        end=end,
        output_path=processed_dir / "sec_pit_feature_panel.csv",
    )
    artifacts["sec_pit_feature_panel"] = feature_panel_path
    validation_artifacts = validate_and_write_sec_pit_artifacts(
        as_of=end,
        raw_manifest_path=raw_dir / "manifest" / "sec_edgar_raw_manifest.csv",
        filing_timeline_path=timeline_path,
        facts_path=facts_path,
        mapped_metrics_path=metrics_output,
        intervals_path=intervals_path,
        feature_panel_path=feature_panel_path,
        output_dir=report_dir,
        policy=policy,
    )
    artifacts.update(validation_artifacts)
    return artifacts


def selected_sec_pit_companies(
    config: SecCompaniesConfig,
    tickers: list[str] | None,
) -> tuple[SecCompanyConfig, ...]:
    active_companies = [company for company in config.companies if company.active]
    if not tickers:
        return tuple(active_companies)
    requested = dedupe_preserving_order([ticker.upper() for ticker in tickers])
    by_ticker = {company.ticker: company for company in active_companies}
    missing = [ticker for ticker in requested if ticker not in by_ticker]
    if missing:
        raise ValueError(f"unknown or inactive SEC PIT tickers: {', '.join(missing)}")
    return tuple(by_ticker[ticker] for ticker in requested)


def sec_pit_submissions_path(raw_dir: Path, ticker: str, cik: str) -> Path:
    return raw_dir / "submissions" / f"{ticker.upper()}_{cik}_submissions.json"


def sec_pit_companyfacts_path(raw_dir: Path, ticker: str, cik: str) -> Path:
    return raw_dir / "companyfacts" / f"{ticker.upper()}_{cik}_companyfacts.json"


def write_sec_pit_raw_manifest(frame: pd.DataFrame, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        existing = pd.read_csv(output_path, dtype=str).fillna("")
        frame = pd.concat([existing, frame], ignore_index=True)
    frame.to_csv(output_path, index=False)
    return output_path


def read_sec_pit_raw_manifest(raw_dir: Path) -> pd.DataFrame:
    path = raw_dir / "manifest" / "sec_edgar_raw_manifest.csv"
    if not path.exists():
        return pd.DataFrame(columns=list(SEC_PIT_RAW_MANIFEST_COLUMNS))
    return pd.read_csv(path, dtype=str).fillna("")


def sec_pit_safety_metadata() -> dict[str, object]:
    return {
        "production_effect": "none",
        "backtest_data_grade": SEC_PIT_BACKTEST_DATA_GRADE,
        "strict_vendor_archive": False,
        "external_side_effects": False,
        "broker_access_required": False,
        "paid_vendor_required": False,
        "retroactive_strict_pit": False,
        "manual_review_required_for_grade_upgrade": True,
    }


def _write_raw_payload(
    *,
    payload: bytes,
    output_path: Path,
    ticker: str,
    cik: str,
    payload_type: str,
    row_count: int,
) -> SecPitRawFile:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(payload.rstrip() + b"\n")
    checksum = _sha256_file(output_path)
    return SecPitRawFile(
        ticker=ticker,
        cik=cik,
        payload_type=payload_type,
        output_path=output_path,
        row_count=row_count,
        checksum_sha256=checksum,
    )


def _raw_manifest_record(
    *,
    downloaded_at: str,
    company: SecCompanyConfig,
    payload_type: str,
    endpoint: str,
    file: SecPitRawFile,
) -> dict[str, object]:
    return {
        "downloaded_at": downloaded_at,
        "provider": "SEC EDGAR",
        "source_id": "sec_edgar_reconstructed_pit_raw",
        "source_endpoint": endpoint,
        "request_parameters": json.dumps(
            {"ticker": company.ticker, "cik": company.cik, "payload_type": payload_type},
            ensure_ascii=False,
            sort_keys=True,
        ),
        "ticker": company.ticker,
        "cik": company.cik,
        "payload_type": payload_type,
        "output_path": str(file.output_path),
        "row_count": file.row_count,
        "checksum_sha256": file.checksum_sha256,
        **sec_pit_safety_metadata(),
    }


def _write_or_build_cached_response(
    *,
    provider: str,
    api_family: str,
    url: str,
    headers: dict[str, str],
    status_code: int,
    response_headers: dict[str, str],
    content: bytes,
    cache_dir: Path | None,
) -> CachedHttpResponse:
    if cache_dir is not None:
        return write_external_request_cache_response(
            provider=provider,
            api_family=api_family,
            method="GET",
            url=url,
            headers=headers,
            status_code=status_code,
            response_headers=response_headers,
            content=content,
            cache_dir=cache_dir,
            requested_at=datetime.now(tz=UTC),
        )
    return CachedHttpResponse(
        status_code=status_code,
        headers=response_headers,
        content=content,
        url=url,
        cache_key=sha256(url.encode("utf-8")).hexdigest(),
        cache_metadata_path=Path(""),
        from_cache=False,
    )


def _submissions_row_count(payload: bytes) -> int:
    data = _json_object(payload)
    recent = (
        data.get("filings", {}).get("recent", {}) if isinstance(data.get("filings"), dict) else {}
    )
    if not isinstance(recent, dict):
        return 0
    lengths = [len(value) for value in recent.values() if isinstance(value, list)]
    return max(lengths) if lengths else 0


def _companyfacts_row_count(payload: bytes) -> int:
    data = _json_object(payload)
    facts = data.get("facts")
    if not isinstance(facts, dict):
        return 0
    total = 0
    for taxonomy_value in facts.values():
        if not isinstance(taxonomy_value, dict):
            continue
        for concept_value in taxonomy_value.values():
            if not isinstance(concept_value, dict):
                continue
            units = concept_value.get("units")
            if not isinstance(units, dict):
                continue
            for unit_facts in units.values():
                if isinstance(unit_facts, list):
                    total += len(unit_facts)
    return total


def _json_object(payload: bytes) -> dict[str, Any]:
    data = json.loads(payload.decode("utf-8"))
    if not isinstance(data, dict):
        raise TypeError("SEC EDGAR response was not a JSON object")
    return data


def _headers_from_response(response: Any) -> dict[str, str]:
    headers = getattr(response, "headers", {}) or {}
    if hasattr(headers, "items"):
        return {str(key): str(value) for key, value in headers.items()}
    return {}


def _content_from_response(response: Any) -> bytes:
    content = getattr(response, "content", None)
    if isinstance(content, bytes):
        return content
    if isinstance(content, bytearray):
        return bytes(content)
    text = getattr(response, "text", None)
    if isinstance(text, str):
        return text.encode("utf-8")
    if hasattr(response, "json"):
        return json.dumps(response.json(), ensure_ascii=False).encode("utf-8")
    return b""


def _sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
