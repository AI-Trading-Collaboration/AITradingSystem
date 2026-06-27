# First-Layer Policy-Aware Calibration Scope

- Status: `pilot_baseline`
- Market regime: `ai_after_chatgpt`
- First-layer outputs: trend_state / confidence / validity_days / decay_profile
- Direct weight output allowed: `False`
- Dynamic second-layer probe count: `4`
- Trend-sensitive probe count: `4`
- Dynamic promotion: `BLOCKED`
- Paper-shadow / production / broker: `false / false / none`

本范围文件定义第一层趋势校准 research-only 路径；second-layer probe 在生成 labels 前冻结，不能与 first-layer 同时优化。
