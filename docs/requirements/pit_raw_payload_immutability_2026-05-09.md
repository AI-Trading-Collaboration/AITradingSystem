# PIT raw payload immutability

## 背景

2026-05-09 在验证 `DTWEXBGS` freshness 修复后，`aits ops daily-run`
已通过 `score_daily`，但第 8 步 `pipeline_health` 失败。健康报告显示
`pit_manifest_checksum` 有 6 条 `fmp_analyst_estimates_*_2026_05_09`
checksum mismatch。

根因是 `aits pit-snapshots fetch-fmp-forward` 在 PIT manifest 中记录了
`data/raw/fmp_analyst_estimates/<ticker>/fmp_analyst_estimates_<ticker>_2026-05-09.json`
的 checksum；随后同一 daily-run 中 `aits valuation fetch-fmp` 再次写入同一路径。
文件内容被覆盖，manifest 中旧 checksum 不再匹配当前文件。

## 目标

- PIT manifest 指向的 raw payload 必须不可变。
- 同一 ticker、同一 captured date 多次运行 valuation fetch 时，不得覆盖旧 raw JSON。
- pipeline health 的 checksum mismatch 继续作为硬错误；不降低 health 检查。
- 保持旧文件读取兼容，历史单日期文件仍可被 loader 读取。

## 设计决策

1. `write_fmp_analyst_estimate_history_snapshots` 输出文件名加入
   `downloaded_at` UTC token 和 payload checksum 前缀。
2. 文件名格式：
   `fmp_analyst_estimates_<ticker>_<captured_at>_<downloaded_at>_<checksum12>.json`
3. loader 继续用 `fmp_analyst_estimates_*.json` glob 读取，兼容旧路径。
4. 不删除或改写已存在 raw payload；下一次 PIT manifest refresh 会从当前 raw cache
   重新发现可校验快照。

## 验收标准

- 单元测试覆盖同一 ticker / captured_at 不同 downloaded_at 会写入不同文件。
- `valuation fetch-fmp` CLI 测试不再假设单日期文件名。
- `aits ops daily-run` 重新通过 pipeline health。
- 不在报告、manifest 或错误中输出 API key 或付费内容原文。

## 状态记录

- 2026-05-09：新增并进入实现，原因：真实 daily-run 暴露 valuation raw cache
  覆盖导致 PIT manifest checksum mismatch。
- 2026-05-09：实现完成。`write_fmp_analyst_estimate_history_snapshots`
  输出文件名新增 `downloaded_at` UTC token 和 checksum 前缀，旧
  `fmp_analyst_estimates_*.json` glob 读取保持兼容。真实
  `aits ops daily-run` 9/9 步通过，`pipeline_health` 状态 PASS，
  `secret_hygiene` 状态 PASS；目标测试 53 passed，Ruff 通过。
