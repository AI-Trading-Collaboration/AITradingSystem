# Norgate Trial Environment Contract

配置：`config/data/norgate_trial_environment_contract.yaml`

允许状态：

- `NORGATE_ENV_READY`
- `NORGATE_ENV_MISSING_PACKAGE`
- `NORGATE_ENV_MISSING_LOCAL_DB`
- `NORGATE_ENV_AUTH_REQUIRED`
- `NORGATE_ENV_BLOCKED`

Smoke test 只允许记录 package presence、package version、database count/hash、credential env
presence 和 fail-closed warnings。不得记录 `NORGATE_USERNAME` / `NORGATE_PASSWORD` 的值。

本地 Norgate database 应在 repo 外。Repo 内以下路径已 gitignore：

- `data/raw/norgate/`
- `data/vendor/norgate/`
- `data/cache/norgate/`
- `outputs/vendor/norgate_raw/`
- `*.norgate.raw.*`
