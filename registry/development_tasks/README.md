# Retired task registry (GOV-008 P5)

Tasks now live in `tasks/<ID>.yaml` and are rendered into `docs/task_register.md` and
`docs/task_register_completed.md` by `python tools/tasks.py render`. The event-chained fragments
that used to live here were removed in GOV-008 P5a; git history keeps them.

One fragment stays as frozen evidence because a kept research contract binds its exact bytes:
`2f/2f96dc5335fe6ba122c905841f6bcc0d25c252cb25828c9c06f0b65990486c7f.yaml` is the
`TERMINAL_RESULT_ADMISSION_EVENT` authority of `config/research/qc_qqq_options_paired_comparison_contract_v1.yaml`
(TRADING-2548). Do not edit or move it.
