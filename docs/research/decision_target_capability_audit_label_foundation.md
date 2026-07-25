# Decision Target Capability Audit：第一批 Label Foundation

- 结论：`BLOCKED_DATA_QUALITY_OR_SOURCE`
- Canonical data quality：`FAIL`
- 阻塞项：prices_non_market_session_date
- Scoped DQ exception：未使用、也不允许复用 QLD 的五资产例外。

标签生成已 fail closed。修复 canonical 数据质量并取得 strict PASS 后，才能形成真实 label dataset；当前状态不影响 clean fixture 对语义实现的验证。
