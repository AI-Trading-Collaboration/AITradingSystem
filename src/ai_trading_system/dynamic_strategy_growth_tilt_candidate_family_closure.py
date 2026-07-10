from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_framework import ExperimentRunRequest, resolve_experiment_spec
from ai_trading_system.research_framework.plugins.growth_tilt_candidate_family_closure import (
    growth_tilt_candidate_family_closure_registry,
)
from ai_trading_system.research_framework.runner import run_experiment
from ai_trading_system.research_quality import growth_tilt_candidate_family_closure as closure

TASK_ID = "TRADING-2438N1"
TASK_REGISTER_ID = (
    "TRADING-2438N1_GROWTH_TILT_CANDIDATE_FAMILY_CLOSURE_AND_NEGATIVE_EVIDENCE_LEDGER"
)
REPORT_TYPE = closure.REPORT_TYPE
SCHEMA_VERSION = closure.SCHEMA_VERSION

DEFAULT_EXPERIMENT_SPEC_PATH = (
    PROJECT_ROOT
    / "config"
    / "research"
    / "experiments"
    / "growth_tilt_candidate_family_closure.yaml"
)
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"
DEFAULT_M1E_PATH = (
    PROJECT_ROOT
    / "outputs"
    / "research_strategies"
    / "growth_tilt_replacement_candidate_contract"
    / "growth_tilt_replacement_candidate_contract.json"
)
DEFAULT_ADAPTERS_PATH = (
    PROJECT_ROOT
    / "outputs"
    / "research_strategies"
    / "growth_tilt_baseline_contract_adapters_readiness"
    / "growth_tilt_baseline_contract_adapters_readiness.json"
)
DEFAULT_OWNER_RESOLUTION_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "growth_tilt_owner_decision_resolution.yaml"
)
DEFAULT_CANDIDATE_SET_PATH = (
    PROJECT_ROOT / "research" / "configs" / "growth_tilt" / "false_risk_off_missed_upside_2433.yaml"
)
DEFAULT_REQUIREMENT_DOC_PATH = (
    PROJECT_ROOT
    / "docs"
    / "requirements"
    / "TRADING-2438N_Growth_Tilt_Candidate_Family_Closure_And_"
    "Contract_First_Discovery_Pivot.md"
)
DEFAULT_REPORT_REGISTRY_PATH = PROJECT_ROOT / "config" / "report_registry.yaml"
DEFAULT_ARTIFACT_CATALOG_PATH = PROJECT_ROOT / "docs" / "artifact_catalog.md"
DEFAULT_SYSTEM_FLOW_PATH = PROJECT_ROOT / "docs" / "system_flow.md"


def run_growth_tilt_candidate_family_closure(
    *,
    m1e_path: Path = DEFAULT_M1E_PATH,
    adapters_path: Path = DEFAULT_ADAPTERS_PATH,
    owner_resolution_path: Path = DEFAULT_OWNER_RESOLUTION_PATH,
    candidate_set_path: Path = DEFAULT_CANDIDATE_SET_PATH,
    requirement_doc_path: Path = DEFAULT_REQUIREMENT_DOC_PATH,
    report_registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Path = DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Path = DEFAULT_SYSTEM_FLOW_PATH,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    as_of_date: date | None = None,
    strict: bool = False,
    experiment_spec_path: Path = DEFAULT_EXPERIMENT_SPEC_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    """Time-bounded CLI façade over the ARCH-004D generic research runner."""
    resolved_spec = resolve_experiment_spec(experiment_spec_path)
    result = run_experiment(
        resolved_spec=resolved_spec,
        plugins=growth_tilt_candidate_family_closure_registry(),
        request=ExperimentRunRequest(
            project_root=PROJECT_ROOT,
            output_root=output_root,
            docs_root=docs_root,
            as_of=as_of_date or date.today(),
            strict=strict,
            generated_at=generated_at,
            input_overrides={
                "m1e": m1e_path,
                "adapters": adapters_path,
                "owner_resolution": owner_resolution_path,
                "candidate_set": candidate_set_path,
                "requirement_text": requirement_doc_path,
                "report_registry": report_registry_path,
                "artifact_catalog_text": artifact_catalog_path,
                "system_flow_text": system_flow_path,
            },
        ),
    )
    return result.payload
