"""Time-bounded compatibility adapters for pre-ARCH-004 interfaces."""

from ai_trading_system.legacy.platform_contract_adapters import (
    PlatformContractAdapterError,
    data_quality_report_to_evidence,
    report_registry_entry_to_spec,
    scheduled_task_to_workflow_spec,
)

__all__ = [
    "PlatformContractAdapterError",
    "data_quality_report_to_evidence",
    "report_registry_entry_to_spec",
    "scheduled_task_to_workflow_spec",
]
