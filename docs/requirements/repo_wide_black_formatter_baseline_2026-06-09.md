# DEV-001：Repo-wide Black Formatter Baseline

状态：`READY`

最后更新：2026-06-09

关联任务：`DEV-001`

## 背景

多个交易执行任务在验证时只对触达文件执行 Black check，因为全仓
`python -m black --check src tests scripts` 仍被既有无关格式 baseline 阻断。该问题不
影响当前 paper signal quality 的运行语义，但会持续污染后续任务的验证记录。

## 范围

1. 固定项目认可的 Python 与 Black 版本，优先使用项目 `.venv` / CI 对齐版本。
2. 运行全仓 Black check，记录实际 would-reformat 文件清单。
3. 将纯格式化变更作为独立提交处理，避免混入业务逻辑、报告语义或数据流修改。
4. 若存在无法自动格式化的文件，记录原因和后续拆分项。

## 边界

- 不改变业务逻辑。
- 不改变 scoring、backtest、report、data gate 或 trading 行为。
- 不修改 generated / ignored artifacts。
- 不借 formatter 任务重排无关 Markdown 表格或配置语义。

## 验收标准

- 固定版本下 `python -m black --check src tests scripts` 通过，或将剩余阻断拆成更小的
  已登记任务。
- 相关目标 pytest / Ruff / diff check 通过。
- 变更 diff 只包含格式化或有明确说明的 formatter 配置更新。

## 状态记录

- 2026-06-09：新增并进入 `READY`。原因：`TRADING-006/006A` 已验证触达文件 Black
  通过，但历史全仓 Black baseline 仍是跨任务遗留问题，应从 paper signal quality 任务中
  独立出来治理。
