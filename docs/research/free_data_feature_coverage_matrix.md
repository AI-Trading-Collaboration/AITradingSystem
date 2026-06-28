# Free Data Feature Coverage Matrix

- 状态：`FREE_DATA_FEATURE_COVERAGE_MATRIX_READY_WITH_WARNINGS`

| Family | Earliest | Latest | Missing rate | Allowed | Blocked |
|---|---|---|---:|---|---|
| `rates_liquidity_free_v1` | 2018-01-02 | 2026-06-25 | 0.5714285714285714 | `risk_on_veto_research` | `promotion,paper_shadow,production,broker` |
| `volatility_compression_free_v1` | 2018-01-02 | 2026-06-26 | 0.0 | `risk_on_veto_research` | `promotion,paper_shadow,production,broker` |
| `macro_event_calendar_free_v1` | missing | missing | 1.0 | `diagnostic_only` | `model_ready_without_known_at,promotion,paper_shadow,production,broker` |
| `event_risk_free_v1` | missing | missing | 1.0 | `diagnostic_only` | `promotion,paper_shadow,production,broker` |
| `participation_proxy_free_v1` | source_dependent |  | None | `diagnostic_only` | `true_pit_breadth,promotion,paper_shadow,production,broker` |
