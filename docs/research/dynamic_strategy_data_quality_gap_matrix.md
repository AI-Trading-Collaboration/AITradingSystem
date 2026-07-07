# Dynamic strategy data quality gap matrix

- status：`DYNAMIC_STRATEGY_DATA_PIT_AND_SIGNAL_QUALITY_GAP_REVIEW_READY`
- validate-data：`PASS_WITH_WARNINGS`

## Data quality review

{
  "cached_market_data": {
    "corporate_action_handling": "known split events are recorded; one TQQQ adjustment-ratio warning remains reviewable",
    "coverage_end": "2026-07-02",
    "coverage_start": "2018-01-02",
    "macro_coverage_end": "2026-07-01",
    "macro_coverage_start": "2018-01-02",
    "macro_rate_row_count": 6365,
    "missing_date_count": "NOT_EXPOSED_BY_VALIDATE_DATA_AUDIT",
    "missing_symbol_count": "NOT_EXPOSED_BY_VALIDATE_DATA_AUDIT",
    "price_row_count": 56288,
    "secondary_coverage_end": "2026-07-02",
    "secondary_coverage_start": "2018-01-02",
    "secondary_price_row_count": 51769,
    "split_dividend_adjustment_risk": "MATERIAL_TQQQ_ADJUSTMENT_RATIO_WARNING",
    "stale_data_risk": "MINOR_WEEKEND_OR_HOLIDAY_AS_OF_WITH_LAST_PRICE_2026-07-02"
  },
  "error_count": 0,
  "info_count": 12,
  "latest_validate_data_status": "PASS_WITH_WARNINGS",
  "pass_with_warnings_interpretation": "PASS_WITH_WARNINGS 不阻断 2402 review；但 TQQQ adjustment-ratio warning 直接触及 dynamic strategy universe，后续候选解释必须保留 caveat。",
  "record_ready": true,
  "schema_version": "dynamic_strategy_data_quality_gap_review.v1",
  "warning_count": 2,
  "warning_detail_summary": [
    {
      "code": "prices_download_manifest_checksum_missing",
      "description": "价格数据当前文件 sha256 未出现在下载审计清单中；请确认缓存是否由 download-data 生成。",
      "dynamic_strategy_relevance": "MATERIAL",
      "gap_category": "DATA_QUALITY",
      "likely_impact": "cache provenance is incomplete; ranking math not directly changed but auditability is weakened",
      "recommended_fix": "reconcile price cache checksum with download_manifest or rerun audited download-data path",
      "row_count": "",
      "sample": "D:\\Work\\AITradingSystem\\data\\raw\\prices_daily.csv",
      "severity": "警告",
      "source": "下载审计清单"
    },
    {
      "code": "prices_adjustment_ratio_jump",
      "description": "价格数据的复权比例出现明显跳变",
      "dynamic_strategy_relevance": "MATERIAL",
      "gap_category": "DATA_QUALITY",
      "likely_impact": "TQQQ is part of the dynamic strategy universe; unresolved adjustment-ratio warning can affect leveraged exposure interpretation",
      "recommended_fix": "investigate TQQQ corporate-action / adjusted-close ratio and document whether it is vendor basis or cache error",
      "row_count": "1",
      "sample": "{'date': '2025-11-20', 'ticker': 'TQQQ', '_adjustment_ratio': 0.9946178686759957, '_adjustment_ratio_change': 1.0000294200104527}",
      "severity": "警告",
      "source": "价格主源"
    }
  ],
  "warnings_irrelevant_to_dynamic_strategy": [],
  "warnings_relevant_to_dynamic_strategy": [
    {
      "code": "prices_download_manifest_checksum_missing",
      "description": "价格数据当前文件 sha256 未出现在下载审计清单中；请确认缓存是否由 download-data 生成。",
      "dynamic_strategy_relevance": "MATERIAL",
      "gap_category": "DATA_QUALITY",
      "likely_impact": "cache provenance is incomplete; ranking math not directly changed but auditability is weakened",
      "recommended_fix": "reconcile price cache checksum with download_manifest or rerun audited download-data path",
      "row_count": "",
      "sample": "D:\\Work\\AITradingSystem\\data\\raw\\prices_daily.csv",
      "severity": "警告",
      "source": "下载审计清单"
    },
    {
      "code": "prices_adjustment_ratio_jump",
      "description": "价格数据的复权比例出现明显跳变",
      "dynamic_strategy_relevance": "MATERIAL",
      "gap_category": "DATA_QUALITY",
      "likely_impact": "TQQQ is part of the dynamic strategy universe; unresolved adjustment-ratio warning can affect leveraged exposure interpretation",
      "recommended_fix": "investigate TQQQ corporate-action / adjusted-close ratio and document whether it is vendor basis or cache error",
      "row_count": "1",
      "sample": "{'date': '2025-11-20', 'ticker': 'TQQQ', '_adjustment_ratio': 0.9946178686759957, '_adjustment_ratio_change': 1.0000294200104527}",
      "severity": "警告",
      "source": "价格主源"
    }
  ]
}

## DATA_QUALITY gaps

[
  {
    "affected_candidates": [
      "QQQ",
      "TQQQ",
      "SGOV"
    ],
    "affected_research_tasks": [
      "TRADING-2364",
      "TRADING-2386",
      "TRADING-2399"
    ],
    "gap_category": "DATA_QUALITY",
    "gap_description": "价格数据当前文件 sha256 未出现在下载审计清单中；请确认缓存是否由 download-data 生成。",
    "gap_id": "2402-DATA-01",
    "likely_impact": "cache provenance is incomplete; ranking math not directly changed but auditability is weakened",
    "owner_review_required": true,
    "recommended_fix": "reconcile price cache checksum with download_manifest or rerun audited download-data path",
    "recommended_next_task": "TRADING-2403_Dynamic_Strategy_PIT_Coverage_Matrix_And_Signal_Construction_Review",
    "severity": "MATERIAL"
  },
  {
    "affected_candidates": [
      "QQQ",
      "TQQQ",
      "SGOV"
    ],
    "affected_research_tasks": [
      "TRADING-2364",
      "TRADING-2386",
      "TRADING-2399"
    ],
    "gap_category": "DATA_QUALITY",
    "gap_description": "价格数据的复权比例出现明显跳变",
    "gap_id": "2402-DATA-02",
    "likely_impact": "TQQQ is part of the dynamic strategy universe; unresolved adjustment-ratio warning can affect leveraged exposure interpretation",
    "owner_review_required": true,
    "recommended_fix": "investigate TQQQ corporate-action / adjusted-close ratio and document whether it is vendor basis or cache error",
    "recommended_next_task": "TRADING-2403_Dynamic_Strategy_PIT_Coverage_Matrix_And_Signal_Construction_Review",
    "severity": "MATERIAL"
  }
]
