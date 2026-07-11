"""Time-bounded compatibility adapters for pre-ARCH-004 interfaces."""

from ai_trading_system.legacy.platform_contract_adapters import (
    PlatformContractAdapterError,
    data_quality_report_to_evidence,
    report_registry_entry_to_spec,
    scheduled_task_to_workflow_spec,
)
from ai_trading_system.legacy.research_campaign_adapter import (
    LegacyCampaignLifecycleBinding,
    ResearchCampaignCompatibilityAssessment,
    assess_legacy_campaign_lifecycle,
)
from ai_trading_system.legacy.scheduled_tasks_adapter import (
    DailyShadowParityAssessment,
    LegacyScheduledTaskDispatchBlocked,
    LegacyScheduledWorkflowBinding,
    ScheduledWorkflowCompatibilityAssessment,
    assess_daily_shadow_parity,
    assess_scheduled_cadence,
)

__all__ = [
    "PlatformContractAdapterError",
    "LegacyCampaignLifecycleBinding",
    "ResearchCampaignCompatibilityAssessment",
    "LegacyScheduledTaskDispatchBlocked",
    "LegacyScheduledWorkflowBinding",
    "DailyShadowParityAssessment",
    "ScheduledWorkflowCompatibilityAssessment",
    "assess_legacy_campaign_lifecycle",
    "assess_daily_shadow_parity",
    "assess_scheduled_cadence",
    "data_quality_report_to_evidence",
    "report_registry_entry_to_spec",
    "scheduled_task_to_workflow_spec",
]
