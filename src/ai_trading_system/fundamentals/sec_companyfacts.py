from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib import import_module
from pathlib import Path
from typing import Any, Protocol, cast

import pandas as pd

from ai_trading_system.config import SecCompaniesConfig, SecCompanyConfig, dedupe_preserving_order
from ai_trading_system.external_request_cache import (
    cached_requests_get,
    default_external_request_cache_dir,
)


@dataclass(frozen=True)
class SecCompanyFactsRequest:
    ticker: str
    cik: str


class SecCompanyFactsProvider(Protocol):
    def download_companyfacts(self, request: SecCompanyFactsRequest) -> dict[str, Any]:
        """Download SEC companyfacts JSON for a single CIK."""


class SecCompanyFactsRawProvider(Protocol):
    def download_companyfacts_raw(self, request: SecCompanyFactsRequest) -> bytes:
        """Download raw SEC companyfacts JSON bytes for one CIK."""


@dataclass(frozen=True)
class SecCompanyFactsFile:
    ticker: str
    cik: str
    company_name: str
    output_path: Path
    fact_count: int
    taxonomy_count: int
    checksum_sha256: str


@dataclass(frozen=True)
class SecCompanyFactsDownloadSummary:
    output_dir: Path
    manifest_path: Path
    files: tuple[SecCompanyFactsFile, ...]

    @property
    def company_count(self) -> int:
        return len(self.files)

    @property
    def total_fact_count(self) -> int:
        return sum(file.fact_count for file in self.files)


class SecEdgarCompanyFactsProvider:
    base_url = "https://data.sec.gov/api/xbrl/companyfacts"

    def __init__(
        self,
        user_agent: str,
        *,
        requests_module: Any | None = None,
        request_cache_dir: Path | str | None = None,
    ) -> None:
        if not user_agent.strip():
            raise ValueError("SEC User-Agent must not be empty")
        self.user_agent = user_agent.strip()
        self._requests_module = requests_module
        self._request_cache_dir = request_cache_dir

    def download_companyfacts_raw(self, request: SecCompanyFactsRequest) -> bytes:
        requests = self._requests_module or cast(Any, import_module("requests"))
        request_cache_dir = default_external_request_cache_dir(
            requests_module=self._requests_module,
            explicit_cache_dir=self._request_cache_dir,
        )
        response = cached_requests_get(
            provider="SEC EDGAR",
            api_family="companyfacts",
            url=self._endpoint(request.cik),
            headers={
                "User-Agent": self.user_agent,
                "Accept-Encoding": "gzip, deflate",
                "Accept": "application/json",
            },
            timeout=30,
            requests_module=requests,
            cache_dir=request_cache_dir,
        )
        response.raise_for_status()
        return bytes(response.content)

    def download_companyfacts(self, request: SecCompanyFactsRequest) -> dict[str, Any]:
        data = _load_companyfacts_json(self.download_companyfacts_raw(request))
        if not isinstance(data, dict):
            raise TypeError("SEC companyfacts response was not a JSON object")
        return data

    def endpoint_for(self, cik: str) -> str:
        return self._endpoint(cik)

    def _endpoint(self, cik: str) -> str:
        return f"{self.base_url}/CIK{cik}.json"


def download_sec_companyfacts(
    config: SecCompaniesConfig,
    output_dir: Path,
    provider: SecCompanyFactsProvider,
    tickers: list[str] | None = None,
) -> SecCompanyFactsDownloadSummary:
    companies = _selected_companies(config, tickers)
    if not companies:
        raise ValueError("no active SEC companies selected for download")

    output_dir.mkdir(parents=True, exist_ok=True)
    files: list[SecCompanyFactsFile] = []
    manifest_records: list[dict[str, object]] = []

    for company in companies:
        request = SecCompanyFactsRequest(ticker=company.ticker, cik=company.cik)
        raw_payload = _download_raw_companyfacts_if_available(provider, request)
        data = (
            _load_companyfacts_json(raw_payload)
            if raw_payload is not None
            else provider.download_companyfacts(request)
        )
        output_path = output_dir / f"{company.ticker.lower()}_companyfacts.json"
        _write_json(data, output_path, raw_payload=raw_payload)
        checksum = _sha256_file(output_path)
        fact_count = _count_company_facts(data)
        taxonomy_count = len(data.get("facts", {})) if isinstance(data.get("facts"), dict) else 0
        file_record = SecCompanyFactsFile(
            ticker=company.ticker,
            cik=company.cik,
            company_name=company.company_name,
            output_path=output_path,
            fact_count=fact_count,
            taxonomy_count=taxonomy_count,
            checksum_sha256=checksum,
        )
        files.append(file_record)
        manifest_records.append(_manifest_record(provider, request, file_record))

    manifest_path = _write_manifest(output_dir, tuple(manifest_records))
    return SecCompanyFactsDownloadSummary(
        output_dir=output_dir,
        manifest_path=manifest_path,
        files=tuple(files),
    )


def _selected_companies(
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
        raise ValueError(f"unknown or inactive SEC tickers: {', '.join(missing)}")
    return tuple(by_ticker[ticker] for ticker in requested)


def _manifest_record(
    provider: SecCompanyFactsProvider,
    request: SecCompanyFactsRequest,
    file_record: SecCompanyFactsFile,
) -> dict[str, object]:
    endpoint = _provider_endpoint(provider, request.cik)
    return {
        "downloaded_at": datetime.now(tz=UTC).isoformat(),
        "source_id": "sec_company_facts",
        "provider": "SEC EDGAR",
        "endpoint": endpoint,
        "request_parameters": json.dumps(
            {"ticker": request.ticker, "cik": request.cik},
            ensure_ascii=False,
            sort_keys=True,
        ),
        "ticker": request.ticker,
        "cik": request.cik,
        "output_path": str(file_record.output_path),
        "fact_count": file_record.fact_count,
        "taxonomy_count": file_record.taxonomy_count,
        "checksum_sha256": file_record.checksum_sha256,
    }


def _provider_endpoint(provider: SecCompanyFactsProvider, cik: str) -> str:
    endpoint_for = getattr(provider, "endpoint_for", None)
    if callable(endpoint_for):
        return str(endpoint_for(cik))
    return f"provider:{provider.__class__.__name__}"


def _write_manifest(
    output_dir: Path,
    records: tuple[dict[str, object], ...],
    filename: str = "sec_companyfacts_manifest.csv",
) -> Path:
    output_path = output_dir / filename
    new_frame = pd.DataFrame(records)
    if output_path.exists():
        existing = pd.read_csv(output_path)
        new_frame = pd.concat([existing, new_frame], ignore_index=True)
    new_frame.to_csv(output_path, index=False)
    return output_path


def _download_raw_companyfacts_if_available(
    provider: SecCompanyFactsProvider,
    request: SecCompanyFactsRequest,
) -> bytes | None:
    raw_fetcher = getattr(provider, "download_companyfacts_raw", None)
    if not callable(raw_fetcher):
        return None
    raw_payload = raw_fetcher(request)
    if not isinstance(raw_payload, bytes):
        raise TypeError("SEC companyfacts raw response was not bytes")
    return raw_payload


def _load_companyfacts_json(raw_payload: bytes) -> dict[str, Any]:
    data = json.loads(raw_payload.decode("utf-8"))
    if not isinstance(data, dict):
        raise TypeError("SEC companyfacts response was not a JSON object")
    return cast(dict[str, Any], data)


def _write_json(
    data: dict[str, Any],
    output_path: Path,
    *,
    raw_payload: bytes | None = None,
) -> None:
    if raw_payload is not None:
        output_path.write_bytes(raw_payload.rstrip() + b"\n")
        return
    with output_path.open("w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, separators=(",", ":"))
        file.write("\n")


def _count_company_facts(data: dict[str, Any]) -> int:
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


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
