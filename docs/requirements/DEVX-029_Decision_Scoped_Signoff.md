# DEVX-029 按决定签字、按补丁发布

## 1. 背景

- 2026-10-13 的耗时分析覆盖 10-11～10-13：流水线时间里约 28% 在等 owner。
- owner 出手约 56 次，其中约 27 次是机械操作：
  - 在新的电脑会话里写原话授权启动 pi、跑门禁；
  - 放 C 区验收记录；
  - 被自动检查拦下后手动下载。
- 一个 C 区包，owner 要出手三次：在决定卡片上点选，在 pi 线程写原话，在电脑上放按 tree 的验收记录。
- C 区批准和发布核对都绑定结果 tree。前面任何一个包先发布，后面的包都要重算 tree、重新批准：
  - DATA-023 两个包重建了 4 次；
  - TRADING-2567-02 的批准句作废、重发过一次。
- `owner_decision:DEVX-025:2026-10-12:signoff_policy` 已经定了"签决定、不签执行"：决定写清范围和预期结果，执行与之一致就不再问。但 `publish.ps1` 对 C 区的检查仍是"每次运行一份按 tree 的记录"，粒度是执行，不是决定。
- 方案全文见项目文件 `knowledge/claude/ops/signoff-reduction-2026-10-13.md`。电脑上 Claude Code 的设置由 owner 自己改，不在本任务里。

## 2. owner 决定

owner_decision:DEVX-029:2026-10-13:decision_scoped_signoff —— owner（Baker）的提出和选择如下：

- 2026-10-13 05:19Z，他在项目线程里提出："目前人工签字和授权感觉时间过大了，我希望能尽可能减少人工参与，看如何修改现有模式把，其他工程优化可以参考你的建议"。
- Claude 给出方案后，他在 06:23:31Z 的决定卡片上作答：
  - 问题："按新方案减少签字吗？C 区决定记录由谁写入验收目录？"
  - 选择："照做，选A"
  - 选项后果："开 DEVX-029；以后每个 C 区决定你在 pi 线程写一句，电脑会话照原话写入记录，手机上就能完成。"

决定的内容：

- C 区改为按决定批准。每个 owner 决定在验收根目录 `decisions/` 下放一份记录，写明允许改的 C 区路径和有效期。记录覆盖的 C 区改动不再需要按 tree 的验收记录。
- 规则和门禁本身、生产和券商边界仍然按 tree 逐个批准。这些路径列在 `config/gov008_ship.yaml` 的 `decision_record_excluded` 里。
- 发布可以按 Claude 审过的补丁核对，不再只能按预测的 tree。
- 决定记录的写入方式 A：owner 在 pi 线程写一句话，点名这个决定；电脑会话随后用 `approve.ps1 -Yes` 把云端起草、核对过哈希的草稿写入验收根目录，记录里逐字写上 owner 的原话和时间。pi 的账户写不了验收根目录，这条边界不变。

## 3. 步骤与验收

### 运行 01：登记决定和任务（B 区）

本需求文档和任务记录。

### 运行 02：决定记录和补丁核对（C 区，云端模型）

步骤：

1. **决定记录。** 文件放在 `<review_root>/decisions/<task>.<date>.<slug>.json`，schema 为 `aits.owner_decision/1`，字段如下：
   - `decision`：完整的 `owner_decision:` 编号；
   - `approved_at`：UTC 时间，写成 `YYYY-MM-DDTHH:MM:SSZ`；
   - `owner_words`：owner 的原话，逐字照录；
   - `c_paths`：非空列表，glob 写法与 zones 相同；
   - `expected_result`；
   - `expires`：日期，不早于 `approved_at`，也不晚于它之后 14 天。
2. **`config/gov008_ship.yaml` 新增 `decision_record_excluded`。** 列 AGENTS.md、ship 规则、`ship.py`、pi 守卫、`publish.ps1`、`approve.ps1`、`.github`，以及生产和券商边界。`scripts/agent/approve.ps1` 同时列入 C 区。
3. **`ship.py --publish` 新增两组参数。**
   - `--review-root`：区划为 C 时，base..HEAD 里每个 `Owner-Decision` 都要有 schema 正确、未过期的记录；每个改到的 C 区路径都要被某份记录的 `c_paths` 覆盖；不能有路径落在 `decision_record_excluded` 里。
   - `--expected-patch` 和 `--expected-message`：分支必须恰好比 base 多一个提交；`git patch-id --stable` 的结果要等于审过的值（计算时去掉两份任务总表和登记的排除文件，diff 选项固定）；提交信息去掉 `Gate:` 行后的 SHA-256 也要等于审过的值。
   - 新增 `--check-decision-record <文件>`，只校验一份记录。
   - 区划和排除清单一律按 base 上的规则算。
4. **`publish.ps1` 新增 `-ExpectedPatch` 和 `-ExpectedMessage`，与 `-ExpectedTree` 二选一。**
   - C 区有按 tree 的验收记录时照旧核对。在补丁模式下，记录的 tree 要等于分支的 tree。
   - C 区没有按 tree 的验收记录时，把 `--review-root` 交给 main 的 `ship.py`，由它核对决定记录。
5. **新增 `scripts/agent/approve.ps1 -Draft <文件> [-ExpectSha256 <哈希>] [-Yes]`。**
   - 草稿必须放在 `agent_config.json` 的 `decision_drafts_root` 里，不能在运行目录、agent 克隆或验收根目录里。
   - 用 main 的 `ship.py --check-decision-record` 校验草稿，然后显示决定编号、`c_paths`、预期结果、有效期和哈希。
   - 写入前要求输入 Y。`-Yes` 跳过确认，但必须同时给 `-ExpectSha256`。
   - 不覆盖已有记录，内容相同时视为已写入。撤销一个决定，就是删掉它的记录文件。
6. **AGENTS.md：** 在"How a change ships"和"Tasks and decisions"两节写进决定记录和补丁核对。
7. **ship-change skill：** 写进新的发布参数。

验收：

- 反向用例在修改前的脚本上失败：
  - C 区路径不在 `c_paths` 里；
  - 记录过期，或有效期超过 14 天；
  - `Owner-Decision` 没有对应的记录；
  - 排除清单里的路径即使写进 `c_paths` 也被拒绝；
  - 补丁多出一处改动；
  - 提交信息被改；
  - 分支有两个提交。
- 正向用例：
  - base 前进而补丁相同，用 `-ExpectedPatch` 可以发布；
  - 有决定记录的 C 区改动不需要按 tree 的记录就能发布。
- 按 tree 的老流程不变，现有用例全部通过。PowerShell 用例要在 Windows PowerShell 5.1 下通过。
- `ship.py --dry-run` 显示 C 区。
- 发布后，Claude 给出补丁模式的发布放行规则片段，由 owner 自己加到电脑设置里。

## 4. 开放问题

- `scripts/agent/agent_config.json` 定义 `review_root` 和 `decision_drafts_root`，目前在 B 区。发布和写入都只读 main 上的这个文件，所以分支上的修改影响不到自己这次发布；但它一旦合进 main，后面的发布都会读新值。是否把它移进 C 区，等 DEVX-026 的考官文件评估时一起定。
- 只新增 C 区条目、属于收紧的 `gov008_ship.yaml` 改动，以后是否也允许由决定记录放行。本次不做。

## 5. 进度

- 2026-10-13：owner 选定方式 A（`owner_decision:DEVX-029:2026-10-13:decision_scoped_signoff`）。运行 01 登记本需求和任务；运行 02（C 区）按 tree 请 owner 最后一次批准。
- 2026-10-13：运行 02（C 区）实现第 3 节步骤 1–7：
  - `ship.py` 增加决定记录核对、补丁指纹和 `--check-decision-record`；
  - `publish.ps1` 增加 `-ExpectedPatch`、`-ExpectedMessage`；C 区没有按 tree 的验收记录时，先确认每个 `Owner-Decision` 都有记录文件，再交给 `ship.py` 核对；
  - 新增 `approve.ps1`，只输出 ASCII，Windows PowerShell 5.1 不会读乱；
  - `config/gov008_ship.yaml` 升到 1.1.0；
  - `agent_config.json` 增加 `decision_drafts_root`（`D:\Work\AITradingSystem_decision_drafts`，pi 的账户无权访问）；
  - AGENTS.md 和 ship-change skill 同步。

  云端在 Linux 上用 PowerShell 7 跑了全部 PowerShell 用例；PowerShell 5.1 由 Windows 上的门禁覆盖。这次改的是规则本身，owner 按结果 tree 批准。

## 6. 运行影响边界

production_effect=none；broker_action=none。只改开发流程的签字和发布核对，不改投资逻辑、数据流、评分、回测或交易边界。
