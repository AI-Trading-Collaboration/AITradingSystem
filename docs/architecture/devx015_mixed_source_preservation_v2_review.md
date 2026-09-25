# DEVX-015 mixed source preservation V2

Engineering scope: continue the owner-requested DEVX-015 and OPS-080 closure.
This review does not grant production, research, broker, host installation or
filesystem permission changes.

## Required behavior

OPS-080's retained source has 92 declared paths: 44 modifications, 31 additions
and 17 deletions. Its current present bytes fit the existing 16 MiB limit.
V1 permits only up to 64 tracked unstaged modifications and must remain usable.
Increasing its limit alone cannot preserve the actual input.

Keep `config/architecture/arch_005_source_preservation.yaml` and its V1 contract.
Add the finite reviewed locator
`config/architecture/arch_005_source_preservation_v2.yaml`, selected explicitly
through the existing CLI `--policy` argument. Bind the locator to V2 schema;
reject arbitrary policy locations and schema/locator substitution. The selected
policy must be committed and included in the original implementation binding.
This is a versioned contract in the same authority, not a second lease/store or
alternative execution entrypoint.

V2 permits exactly up to 92 declared paths and 16 MiB of present raw bytes.
Its recovery task is DEVX-015 and its owner reference is the existing recorded
DEVX-015 V3 closure decision. Require explicit ADD/MODIFY/DELETE semantics,
baseline mode/OID or proven absence, after bytes or proven deletion, and exact
whole dirty scope. Retain all terminal-source, repository identity, original
lease, configuration, canonical history, capture drift and independent snapshot
verification gates. No splitting to evade budgets; no task history deletion.

## Acceptance and activation

V1 regression and exact V2 path/schema negatives must pass. V2 needs actual
committed policy/module/CLI identity acceptance, including the 92-path shape,
before use on real OPS-080. Synthetic identity fixtures cannot prove this.
The original source/HEAD/index/branches stay unchanged by preservation.
The receipt remains RAW_BYTES_SOURCE_ONLY_UNVALIDATED, with no candidate,
validation, publication or deployment rights.

The current v4 transaction does not declare the new policy path. Retain its
results through the original failed/released handoff, acquire a new transaction
with this precise added path, and pass original preflight before mutation.
Required final candidate tiers/Full/publication, I05/L03/X05 and OPS-080 S4/S5
remain mandatory. Neither this review nor focused PASS activates a host or
completes an acceptance mapping.
