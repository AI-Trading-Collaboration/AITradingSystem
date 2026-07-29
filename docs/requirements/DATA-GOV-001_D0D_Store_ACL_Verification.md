# DATA-GOV-001 D0D Store ACL 独立验收

最后更新：2026-07-29

## 任务信息

- task id：`DATA-GOV-001_D0D_STORE_ACL_VERIFICATION`
- parent：`DATA-GOV-001_UNIFIED_DATA_FOUNDATION_GOVERNANCE`
- priority：`P0`
- status：`DONE`
- owner：data platform owner / architecture coordinator
- production effect：`none`
- broker action：`none`

## 目标与顺序

Owner 已选择按
`D0C crash durability -> ACL -> per-consumer migration`
推进。D0C 已完成并进入 local/remote `main`；本任务只建设和验收 exact store /
principal / policy scoped 的 ACL capability。任何 generic consumer migration 必须等待本任务
形成独立、可复核的 scoped PASS 后另立任务。

D0D 解决的是“谁可以读取、写入、改变 ACL 或删除 canonical store bytes”的 OS enforcement
边界。它不把 D0A 历史 manifest 的 `store_acl_verified=false` 改写为 true，不把
`consumer_cutover_allowed` 改为 true，也不声称能够抵御同 principal 恶意写入、local
administrator/root、离线磁盘访问或尚未验收的 network/object store。

## S0 合同冻结

1. 新增 reviewed ACL policy manifest，至少声明：
   - policy id/version/status/owner/rationale/review condition；
   - writer、reader、operator/recovery 与 explicitly forbidden broad principals；
   - directory/file 权限、继承规则、ACL 变更权限和受管 store roots；
   - Windows/POSIX 支持 profile 与明确 limitations；
   - `production_effect=none`、generic cutover=false。
2. 新增独立 `data_store_acl_attestation.v1`，绑定 exact：
   - store identity、resolved root、platform/filesystem；
   - policy bytes/checksum；
   - principal stable identity（Windows SID 或 POSIX uid/gid）；
   - canonical permission/security-descriptor digest；
   - static effective-rights matrix和dynamic probe receipt；
   - evidence timestamp、validator version、status与limitations。
3. D0A publication/manifest bytes保持 immutable。ACL PASS 只能存在于独立 attestation；
   consumer 后续必须同时绑定 exact DQ、publication、durability 与 ACL evidence。

## 分阶段实现

### S1：fail-closed inspector 与 policy validator

- 所有 path 必须 resolved containment，拒绝 symlink/reparse escape、缺失 root、非目录和
  policy/identity drift；
- policy schema、未知字段、重复 principal、角色冲突、broad write、继承漂移、
  owner/ACL-change 权限漂移均 fail closed；
- Windows 使用 stable SID 和 canonical DACL/security descriptor；POSIX 使用 uid/gid、
  mode bits 与 capability limitation。不能解析或平台不受支持时不得给 PASS；
- inspector 是只读；apply 与 verify 分离，apply 只能作用于显式 isolated rehearsal root。

### S2：隔离 enforcement rehearsal

- 在任务专属临时目录创建空 store，禁止对 repository data cache、生产 store、scheduler
  runtime 或外部路径改变权限；
- apply reviewed ACL 后验证：
  - trusted writer 可以 create/replace/read/delete受管对象；
  - reader 只有 policy允许的 read/traverse；
  - broad/unapproved principal没有 write/delete/ACL-change权；
  - inheritance与新建 child对象不会重新引入 broad write；
  - ACL tamper、policy checksum drift、principal drift和证据 tamper均被 validator拒绝；
- 若宿主无法提供独立 reader/unapproved principal token，必须输出 typed
  `BLOCKED`/`UNSUPPORTED`，不得用同 principal 成功写入代替 negative enforcement evidence。

### S3：scoped attestation 与 consumer binding

- 生成 content-addressed rehearsal bundle和独立 ACL attestation；
- validator从 policy、live ACL与probe receipt重算结论，不能只信生成器状态；
- attestation只授权 exact store identity/policy/principal set，不授权其他 store或consumer；
- `store_acl_verified=false`和`consumer_cutover_allowed=false`继续保留在历史 D0A
  manifest/result；per-consumer migration必须消费独立 ACL attestation，并另行决定
  consumer-scoped cutover。

### S4：验证、关闭与交接

- focused ACL tests与相关 immutable publication/durability regression；
- Ruff、strict mypy、compileall；
- task registry、DevEx、system flow、compatibility/deprecation freshness；
- Architecture、Contract、Report、Reproducibility、Integration；若自然集成边界要求则运行
  parent-bound Full；
- governed task commit、latest-main revalidation、local main fast-forward、ordinary
  non-force push和 local/remote SHA equality；
- 清理临时 store/workspace；被宿主策略阻止时记录 exact path、风险、next owner与exit
  condition。

## 验收标准

1. policy/attestation schema有 owner、version、rationale、review condition且无未治理阈值；
2. exact isolated store上 native ACL apply、static rights、dynamic positive/negative probe和
   inheritance probe全部为 PASS；任一缺失不得降级为 warning PASS；
3. broad/unapproved principal不能 write/delete/change ACL，reader不能扩大权限；
4. attestation绑定 live ACL digest、policy checksum、stable identities、store identity与
   probe bytes，任何 drift/tamper fail closed；
5. historical D0A false flags与 generic cutover false不变；
6. 不访问或改变 production store、cache、scheduler、QLD automatic selection、strategy、
   weights或broker；
7. formal validation和 governed closeout全部 PASS 后才可转 `DONE`；否则保留明确 blocker，
   不得开始 generic per-consumer migration。

## 当前进度

- 2026-07-29：Owner要求继续按既定顺序推进；D0C 已以
  `debebc3c7e9f887a399f942e2f41215c2f75591e`
  进入 local/remote `main`。D0D 任务登记为 `IN_PROGRESS`，当前只完成 S0 requirement
  freeze；尚未 apply ACL、生成 PASS attestation或开放 consumer。
- 2026-07-29：S1-S3 已在 Windows local fixed NTFS 隔离 root 完成。Policy
  `data_foundation_store_acl_isolation.v1`使用 current process user SID作为writer、
  `BUILTIN\Users` SID作为restricted reader、`LOCAL_SYSTEM`与
  `BUILTIN\Administrators`作为recovery，并保护DACL、移除继承；没有使用deny ACE覆盖
  writer所属group。内核restricted-token rehearsal验证writer
  create/replace/read/delete、reader read、reader write/delete/ACL-change拒绝、
  unapproved token read/write拒绝和new-child inheritance。POSIX继续显式
  `BLOCKED_PENDING_DISTINCT_IDENTITY_REHEARSAL`，不被Windows PASS代替。
- 2026-07-29：final canonical bundle
  `outputs/validation_runtime/data_foundation_d0d_20260729T034500Z/rehearsal_bundle.json`
  为 `PASS`，bundle id=
  `data_foundation_acl_bundle_3f68c2174cf4ffe1753ef8b9f32de5ea`，SHA-256=
  `0e34f6f5e3cbcd273721651f49c74ac196548cf24df186396d68c328410c04cc`；
  attestation id=
  `store_acl_attestation_ca3499e0650954d9cde4962fb78a6fa1`，SHA-256=
  `5d11335a790b5dc3df8718ff2d329680615c6c10b083d28038e7815b47e7eda7`；
  validator module SHA-256=
  `02f89af3eaa9eef5c801ecaa4e748e031e7258643fc56faf28df09add07a5d8c`；
  cleanup receipt SHA-256=
  `a9741506c605eb9ed9a8c66920a45d3df293f1e796076be976e2692140669311`。
  Live validator在root存在时重跑全部probe；attestation持久化后trusted writer删除
  `live_rehearsal_store`，offline bundle validator确认exact pointer/checksum/id与
  `root_exists_after=false`。Focused ACL=`16 passed`，ACL+immutable publication+
  durability+download regression=`138 passed / 1 skipped`。任务转`VALIDATING`等待S4；
  D0A `store_acl_verified=false`、generic cutover=false、production/broker none不变。
  首轮`T033000Z` bundle在加入exact validator module checksum前生成，已由final bundle
  supersede且不再作为canonical evidence；其live root同样已清理，仅保留为ignored historical
  validation output。
- 2026-07-29：S4 formal exit完成。Architecture/Contract/Report/Reproducibility/
  Integration分别为`783/276/57/24/995 passed`，Integration=`643 warnings`；
  natural-boundary Full=
  `7668 passed / 3 skipped / 644 warnings`，artifact=
  `outputs/validation_runtime/full_20260729T035743Z/test_runtime_summary.json`，
  trigger provenance=`natural_integration_boundary`、task=
  `DATA-GOV-001_D0D_STORE_ACL_VERIFICATION`、boundary=
  `DATA-GOV-001-D0D-ACL-NATURAL-INTEGRATION-20260729`。任务转`DONE`；下一步只能另立
  per-consumer migration任务，逐项绑定exact DQ/publication/durability/ACL evidence。
- 2026-07-29：Full后台runner临时目录
  `outputs/validation_runtime/data_gov_001_d0d_full_runner_20260729/`
  只含`stdout.log`（20,599 bytes）和空`stderr.log`；canonical summary/profile/reader brief
  已在`full_20260729T035743Z/`保全，无unique evidence或运行进程依赖。按exact absolute
  allowlist执行原生PowerShell清理时被host command policy拒绝，故保留为ignored validation
  log；behavior/evidence risk为none，next owner为本地operator，exit condition为复核
  canonical Full artifact仍PASS后删除该exact目录。没有采用跨shell或不透明删除绕过。
