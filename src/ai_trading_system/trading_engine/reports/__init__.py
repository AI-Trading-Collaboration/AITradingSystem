"""Trading engine reports."""

from ai_trading_system.trading_engine.reports.daily_shadow_vs_production_comparison import (
    build_daily_shadow_vs_production_comparison_payload,
    default_shadow_vs_production_comparison_json_path,
    render_daily_shadow_vs_production_comparison_report,
    write_daily_shadow_vs_production_comparison_report,
)
from ai_trading_system.trading_engine.reports.daily_shadow_weight_iteration import (
    build_daily_shadow_weight_iteration_payload,
    default_current_shadow_weights_path,
    default_shadow_weight_candidate_json_path,
    render_daily_shadow_weight_iteration_report,
    write_daily_shadow_weight_iteration_report,
)
from ai_trading_system.trading_engine.reports.daily_weight_adjustment import (
    build_daily_weight_adjustment_summary_payload,
    default_daily_weight_adjustment_summary_json_path,
    render_daily_weight_adjustment_summary_report,
    write_daily_weight_adjustment_summary_report,
)
from ai_trading_system.trading_engine.reports.paper_signal_quality import (
    build_paper_signal_quality_payload,
    default_paper_signal_quality_json_path,
    render_paper_signal_quality_report,
    write_paper_signal_quality_report,
)
from ai_trading_system.trading_engine.reports.paperbroker_fill_model_calibration import (
    build_paperbroker_fill_model_calibration_payload,
    default_paperbroker_fill_model_calibration_json_path,
    render_paperbroker_fill_model_calibration_report,
    write_paperbroker_fill_model_calibration_report,
)
from ai_trading_system.trading_engine.reports.shadow_parameter_impact import (
    build_shadow_parameter_impact_payload,
    default_shadow_parameter_impact_json_path,
    render_shadow_parameter_impact_report,
    write_shadow_parameter_impact_report,
)
from ai_trading_system.trading_engine.reports.trading_daily_report import (
    TradingDailyReport,
    build_paper_trading_summary_payload,
    build_trading_daily_report,
    render_trading_daily_report,
    write_paper_trading_summary_json,
    write_trading_daily_report,
)
from ai_trading_system.trading_engine.reports.weight_adjustment_candidates import (
    build_weight_adjustment_candidates_payload,
    default_weight_adjustment_candidates_json_path,
    render_weight_adjustment_candidates_report,
    write_weight_adjustment_candidates_report,
)
from ai_trading_system.trading_engine.reports.weight_candidate_evaluation import (
    build_weight_candidate_evaluation_payload,
    default_weight_candidate_evaluation_json_path,
    render_weight_candidate_evaluation_report,
    write_weight_candidate_evaluation_report,
)

__all__ = [
    "TradingDailyReport",
    "build_paper_trading_summary_payload",
    "build_paper_signal_quality_payload",
    "build_paperbroker_fill_model_calibration_payload",
    "build_trading_daily_report",
    "build_shadow_parameter_impact_payload",
    "build_daily_weight_adjustment_summary_payload",
    "build_daily_shadow_weight_iteration_payload",
    "build_daily_shadow_vs_production_comparison_payload",
    "build_weight_adjustment_candidates_payload",
    "build_weight_candidate_evaluation_payload",
    "default_current_shadow_weights_path",
    "default_daily_weight_adjustment_summary_json_path",
    "default_paper_signal_quality_json_path",
    "default_paperbroker_fill_model_calibration_json_path",
    "default_shadow_parameter_impact_json_path",
    "default_shadow_weight_candidate_json_path",
    "default_shadow_vs_production_comparison_json_path",
    "default_weight_adjustment_candidates_json_path",
    "default_weight_candidate_evaluation_json_path",
    "render_daily_shadow_weight_iteration_report",
    "render_daily_shadow_vs_production_comparison_report",
    "render_paper_signal_quality_report",
    "render_paperbroker_fill_model_calibration_report",
    "render_shadow_parameter_impact_report",
    "render_daily_weight_adjustment_summary_report",
    "render_weight_adjustment_candidates_report",
    "render_weight_candidate_evaluation_report",
    "render_trading_daily_report",
    "write_paper_trading_summary_json",
    "write_paper_signal_quality_report",
    "write_paperbroker_fill_model_calibration_report",
    "write_shadow_parameter_impact_report",
    "write_daily_weight_adjustment_summary_report",
    "write_daily_shadow_weight_iteration_report",
    "write_daily_shadow_vs_production_comparison_report",
    "write_weight_adjustment_candidates_report",
    "write_weight_candidate_evaluation_report",
    "write_trading_daily_report",
]
