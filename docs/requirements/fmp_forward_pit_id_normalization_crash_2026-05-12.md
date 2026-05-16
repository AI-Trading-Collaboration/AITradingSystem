# DATA-017 FMP forward PIT normalized_id 稳定化

任务 ID：`DATA-017`

最后更新：2026-05-17

## 背景

2026-05-12 运行最新交易日 `aits ops daily-run` 时，默认 as-of 正确解析为
`2026-05-11`，`download_data` 通过，但 `pit_snapshots` 子命令在标准化 FMP
forward-only PIT payload 时以 Windows 原生访问冲突退出：

```text
Windows fatal exception: access violation
src/ai_trading_system/fmp_forward_pit.py line 802 in _id_token
src/ai_trading_system/fmp_forward_pit.py line 614 in _normalize_fmp_forward_pit_payloads
```

供应商请求已返回并写入部分 raw 目录 mtime，但命令未生成
`fmp_forward_pit_fetch_2026-05-11.md`、normalized CSV 或 PIT validation 报告。

## 目标

- `normalized_id` 生成不能读取供应商原始 record 字段，也不能对供应商原始字段逐字符依赖
  Python Unicode 分类方法，避免异常 Unicode、超长字符串或嵌套值触发解释器原生崩溃。
- ID 必须基于 `snapshot_id + endpoint + record_index` 保持确定性、可读前缀和冲突保护。
- 继续保留原始 record 内容在 `normalized_values_json` 和 raw payload 中，
  不丢失审计信息。
- PIT fetch 命令应能完成 raw payload、normalized CSV、fetch report 和 PIT
  validation report 写入。

## 非目标

- 不改变 FMP endpoint、请求参数、供应商选择或 PIT 可见性规则。
- 不补写未实际抓取的数据。

## 验收标准

- 单元测试覆盖非 ASCII、超长和嵌套/非标供应商字段的 normalized id 生成。
- 真实 `aits pit-snapshots fetch-fmp-forward --as-of 2026-05-11 --continue-on-failure`
  不再访问冲突，并生成 PIT 抓取报告、normalized CSV 和 PIT validation 报告。
- 最新交易日 `aits ops daily-run` 能继续越过 PIT 阶段；若后续步骤失败，必须
  报告新的阻断原因。

## 进展记录

- 2026-05-12：新增并进入 `IN_PROGRESS`。原因：最新交易日 daily-run 暴露
  PIT normalized id 生成在真实 FMP payload 上触发 Windows access violation。
- 2026-05-12：进一步收敛 `normalized_id` 输入，只使用 snapshot、endpoint 和
  record_index，不再读取供应商 record 内容；record 内容仍保留在 raw payload 和
  `normalized_values_json` 审计字段中。
- 2026-05-12：PIT artifact 阶段继续暴露 `_id_token()` 在 Windows 上对
  `str.encode(..., errors=...)` 关键字参数路径的非确定 TypeError；已改为位置参数调用，
  保持相同编码语义，配合 daily-run 隔离 pycache 后真实 PIT 子流程 PASS。
- 2026-05-12：daily-run 复跑时 raw payload 写入阶段又触发 stdlib JSON 编码器
  `first argument must be a string, not dict`；已将 FMP raw payload 写入改为递归流式 JSON
  writer，保留嵌套字段和值，避免对大型 payload 使用单次 `json.dumps()`。
- 2026-05-12：PIT 子流程继续在 `_safe_scalar_values()` 和 `dataclasses.replace()`
  热路径上出现 Windows access violation；已改为只接受原生 string key 和精确 scalar value，
  并手工构造 retargeted normalized row / fetch report，避免反射式 replace 和不必要的
  强制 `str()`。
- 2026-05-12：直接 PIT 再次复现 Windows access violation，栈定位到
  `retarget_fmp_forward_pit_normalized_rows()` 逐行复制 12411 条 normalized dataclass。
  修复为从已附加 raw path/checksum 的 payload 重新标准化，避免对旧 dataclass 做大批量字段复制，
  同时保持 raw payload path、checksum、snapshot id 和 normalized CSV 语义一致。
- 2026-05-12：真实 CLI 继续在首次/二次构造 normalized dataclass 时随机访问冲突。
  PIT CLI 改为 `include_normalized_rows=False`，只保留 raw payload 和 row count，
  normalized CSV 由 attached raw payload 流式写出，不再为 12411 行创建常驻 dataclass。
