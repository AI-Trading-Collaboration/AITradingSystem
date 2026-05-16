# FUND-002 SEC companyfacts JSON 写入稳定化

任务 ID：`FUND-002`

最后更新：2026-05-17

## 背景

2026-05-12 最新交易日 `daily-run` 已越过 FMP PIT 后，阻断在
`aits fundamentals download-sec-companyfacts`。faulthandler 显示崩溃点为：

```text
json.encoder._iterencode_dict
ai_trading_system/fundamentals/sec_companyfacts.py line 191 in _write_json
```

当前 `_write_json()` 使用 `json.dumps(..., indent=2, sort_keys=True)`，会对 SEC
companyfacts 大型嵌套 JSON 一次性构造完整字符串并递归排序。对 17 个扩展核心
ticker 的大 payload，本机 Windows/Python 3.11 出现 access violation。

## 目标

- SEC companyfacts raw JSON 写入应直接流式写入文件，避免构造巨型中间字符串。
- 保留 SEC 原始 JSON 字段和值，不改变 companyfacts 下载 endpoint、请求参数或后续
  指标抽取语义。
- checksum 继续基于实际写入文件计算，manifest 继续记录 provider、endpoint、请求参数、
  row count 和 checksum。

## 非目标

- 不压缩、不裁剪、不抽样 SEC companyfacts raw payload。
- 不改变 SEC 指标映射或评分逻辑。

## 验收标准

- 单元测试覆盖 `_write_json()` 输出仍为合法 JSON。
- `.venv\Scripts\python.exe -m ai_trading_system.cli fundamentals download-sec-companyfacts`
  可完成 17 个 active companyfacts 下载/写入。
- 最新交易日 `daily-run` 能越过 SEC companyfacts；若后续步骤失败，报告新的真实门禁原因。

## 进展记录

- 2026-05-12：新增并进入 `IN_PROGRESS`。原因：扩展核心观察池后，SEC companyfacts
  大型 JSON pretty/sorted dumps 在 Windows 本机触发原生崩溃。
