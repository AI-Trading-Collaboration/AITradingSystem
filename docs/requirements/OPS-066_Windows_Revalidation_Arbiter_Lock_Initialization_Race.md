# OPS-066：Windows Revalidation Arbiter Lock 初始化竞态

最后更新：2026-07-21

状态：`DONE`

稳定任务 ID：`OPS-066_WINDOWS_REVALIDATION_ARBITER_LOCK_INITIALIZATION_RACE`

## 背景与根因

TRADING-2452 最终 full 在 `tests/test_external_request_cache.py::
test_concurrent_wrapper_revalidation_uses_single_live_request` 暴露真实 Windows 竞态。
`_exclusive_file_lock` 先以 `O_CREAT|O_EXCL` 发布空 `arbiter.lock`，再经另一 handle 写入首字节。
并发 waiter 可在 creator 写入前打开文件并对 byte 0 取得 `msvcrt` range lock，随后 creator 的
`fdopen/flush` 以 `PermissionError` 失败。

前两轮 full 与既有 focused PASS 说明该竞态低概率，但不能据此忽略；它会让相同 cache key 的
revalidation 在 Windows 高并发下失败。

## 设计与验收

1. 所有 caller 用同一 `a+b` 路径打开/create lock file，不在 OS lock 外写初始化 byte；
2. caller 先按既有 bounded timeout/poll policy 获取 byte-range lock；
3. 只有持锁 caller 在文件为空时写入并 `flush/fsync` 首字节，随后执行原临界区；
4. 保持 per-key、跨进程、winner double-check、lease/takeover/fencing、timeout reason 和不同 key
   并行语义不变；
5. 新增多线程“首次创建同一 lock file”回归，并重复运行 wrapper singleflight；
6. external-cache focused、architecture/contract/full 按风险 PASS。

## 安全边界

- 不改变 cache identity、TTL、Retry-After、negative observation、CAS invalidation、provider retry 或
  request budget；
- 不删除或平滑 provider failure evidence，不绕过 `aits validate-data`；
- 不运行 periodic operation、不产生真实 provider 请求；
- `production_effect=none`、`broker_action=none`。

## 完成证据

- 原 wrapper singleflight 与 16-thread 首次创建回归 `2 passed`；完整 external-cache/coordination
  focused=`39 passed`；
- architecture=`446 passed`；最终 Full=`6543 passed / 2 skipped / 642 warnings / 1010.39s`；
- Full profile/telemetry/performance/provenance PASS，scheduler applied=true、fallback=false；
- cache/provider/DQ/TTL/lease/fencing 语义不变，`production_effect=none`、`broker_action=none`。
