from __future__ import annotations

import hashlib
import json
import re
import subprocess
from copy import deepcopy
from functools import cache
from pathlib import Path
from typing import Any

import pytest

from ai_trading_system.yaml_loader import safe_load_yaml_path, safe_load_yaml_text

POLICY_PATH = Path("config/architecture/arch_004_refactor_policy.yaml")
RECONCILIATION_PATH = Path("inputs/architecture/arch_004_predecessor_reconciliation.yaml")
GLOSSARY_PATH = Path("config/architecture/research_semantic_glossary.yaml")
COMPATIBILITY_BASELINE_PATH = Path("inputs/architecture/arch_004_compatibility_baseline.yaml")
ATTRIBUTION_PATH = Path("inputs/architecture/arch_004_worktree_attribution.yaml")
DEPENDENCY_POLICY_PATH = Path("config/architecture/arch_004c_dependency_policy.yaml")
DIRECT_WRITER_BASELINE_PATH = Path("inputs/architecture/arch_004c_direct_writer_baseline.yaml")


WAVE11_SECTION = "phase_arch_004_g2_5_wave11"
WAVE11_BASE_COMMIT = "6ee5903a929da593746e1459c055c8226cc21157"
WAVE11_BASELINE_REPOSITORY_PATH = "inputs/architecture/arch_004_compatibility_baseline.yaml"
WAVE11_BASELINE_GIT_BLOB = "166901e26b8d2369f7ee22455e161c95be20d9a5"
WAVE11_HISTORICAL_PREFIX_BYTE_COUNT = 1_136_370
WAVE11_HISTORICAL_PREFIX_SHA256 = "f81225a6a10c56ee74bf9958e383a2c9ee5bc16c09c8b74317ce3aa12f8945ce"
DOCS_GOV_SECTION = "phase_docs_gov_001_freshness_closeout"
DOCS_GOV_BASE_COMMIT = "53c60c8e9351a0480987cd4ecdbd746a09e807cf"
DOCS_GOV_BASELINE_GIT_BLOB = "4c71f33b9e72fd9f61a7fc26534c5b4fee4dbf96"
DOCS_GOV_HISTORICAL_PREFIX_BYTE_COUNT = 1_151_797
DOCS_GOV_HISTORICAL_PREFIX_SHA256 = (
    "b0085b51a4c2ff19ffa7257e5d01a49fbdd85e300b3a43e379bf2737f67682f5"
)
DOCS_GOV_SUPPORTING_DOC = "docs/requirements/DOCS-GOV-001_Existing_Freshness_Metadata_Debt.md"
DOCS_GOV_SOURCE_PATHS = frozenset(
    {
        "docs/requirements/ARCH-004F1_Operations_Control_Plane.md",
        "docs/requirements/ARCH-004F3_Reporting_Architecture.md",
        DOCS_GOV_SUPPORTING_DOC,
        "docs/requirements/TRADING-102_to_110_Stable_Real_Parameter_Iteration_Backtest_Loop.md",
        "docs/requirements/TRADING-111_to_113_Real_Research_Evidence_Closure.md",
        "docs/requirements/"
        "TRADING-141_to_145_Historical_Advisory_Replay_and_Backfilled_Outcome_Evaluation.md",
        "docs/requirements/"
        "TRADING-146_to_150_Historical_Replay_Result_Diagnosis_and_Advisory_Rule_Calibration.md",
        "docs/requirements/"
        "TRADING-156_to_160_Outcome_Update_Loop_and_Rolling_Advisory_Evidence_Refresh.md",
        "docs/requirements/TRADING-161_to_168_Backtest_Simulation_Advisory_Evaluation.md",
        "docs/requirements/"
        "TRADING-169_to_173_Simulation_Result_Interpretation_and_Advisory_Rule_Review.md",
        "docs/requirements/"
        "TRADING-204_to_208_Real_Manual_Snapshot_Dry_Run_and_Owner_Decision_Loop.md",
        "docs/requirements/TRADING-2450_Legacy_Research_Artifact_Portable_Lineage.md",
        "docs/task_register.md",
        "docs/task_register_completed.md",
        "inputs/architecture/arch_004e_test_manifest.yaml",
        "inputs/architecture/arch_005_task_registry_baseline.yaml",
        "inputs/architecture/arch_005_task_shadow_index.yaml",
        "inputs/governance/gov_006_wave1_decision_manifest.json",
        "tests/test_arch_004_refactor_policy.py",
        "tests/test_trading2452_architecture_contract.py",
    }
)
DOCS_GOV_SUPERSEDED_LIVE_SOURCE_PATHS = DOCS_GOV_SOURCE_PATHS - {DOCS_GOV_SUPPORTING_DOC}
WAVE12_SECTION = "phase_arch_004_wave12_g4_d0b_s2"
WAVE12_BASE_COMMIT = "12b1fb86369f146c9ef1c7ac54872eb8150ed791"
WAVE12_CLOSEOUT_COMMIT = "02f9492a1975440240cb65f761d2cf544a5a875e"
WAVE12_BASELINE_GIT_BLOB = "43e39f378110325a3d40fdeebae2c1b2976bc40f"
WAVE12_HISTORICAL_PREFIX_BYTE_COUNT = 1_158_058
WAVE12_HISTORICAL_PREFIX_SHA256 = "98444bce4775733359a238a561be02e8e536418b33d6032af60abfddf7f2d512"
WAVE12_READINESS_POLICY_PATH = Path("config/architecture/arch_004_wave12_g4_d0b_readiness.yaml")
WAVE13_SECTION = "phase_arch_004_wave13_gov006_n1"
WAVE13_BASE_COMMIT = "58ee6a80b5a04ff68a97a96d36b575ae8391f657"
WAVE13_BASELINE_GIT_BLOB = "9dd3a91833ef4ff1eade620961d38a235f445ac3"
WAVE13_HISTORICAL_PREFIX_BYTE_COUNT = 1_180_701
WAVE13_HISTORICAL_PREFIX_SHA256 = "297b186ddfed93d1571d3c35343bf91eb5438872d83ad489c4f0bb917ae9b734"
WAVE13_APPLIED_CLOSEOUT_PATH = Path("inputs/governance/gov_006_wave1_applied_closeout.json")
WAVE13_DECISION_MANIFEST_PATH = Path("inputs/governance/gov_006_wave1_decision_manifest.json")
WAVE13_G2_5_READINESS_PATH = Path("inputs/architecture/arch_004g2_5_parallel_readiness.json")
WAVE13_APPLIED_CLOSEOUT_RAW_SHA256 = (
    "f8ea35968cfc0ad206904009ac0f78cf75b4eadba8acf87d326a4459469be386"
)
WAVE13_APPLIED_CLOSEOUT_CANONICAL_SHA256 = (
    "bdbfd433a72d5349ead904a95ebba484b6051d9cdc029f091395d1f0988dc111"
)
WAVE13_DECISION_MANIFEST_RAW_SHA256 = (
    "9269df2bfa35ca8e88f7b44d80a0cbb9abb64fc9f79119b3a17e8b0bfdd6cc7d"
)
WAVE13_DECISION_MANIFEST_CANONICAL_SHA256 = (
    "3fb5f2a038eca2361179601d03bd6688c313544b42ece9f608bf9da25a88a537"
)
WAVE14_S0_1_SECTION = "phase_arch_004_wave14_s0_1_readiness_infrastructure"
WAVE14_S0_1_BASE_COMMIT = "e2da21894ea8e8921a86c6c1b48d7b191f0f142c"
WAVE14_S0_1_BASELINE_GIT_BLOB = "d4965aaeb37b96c2892f8bc4ec7d7f906de92378"
WAVE14_S0_1_HISTORICAL_PREFIX_BYTE_COUNT = 1_201_626
WAVE14_S0_1_HISTORICAL_PREFIX_SHA256 = (
    "44064dc305b5de0359dde26462601073fc3193003917c4ea1ce1f86a85dab490"
)
WAVE14_S0_1_EXPECTED_SOURCE_COUNT = 85
WAVE14_S0_1_EXPECTED_SUPERSEDED_SOURCE_COUNT = 68
WAVE14_S2_SECTION = "phase_arch_004_wave14_s2_shared_integration_and_formal_exit"
WAVE14_S2_BASE_COMMIT = "39a3ea7306a3937beda835020df4d8419c1cbbdf"
WAVE14_S2_BASELINE_GIT_BLOB = "1c2f3980ccee17db62ab10feb886b5f45bf2e588"
WAVE14_S2_HISTORICAL_PREFIX_BYTE_COUNT = 1_222_368
WAVE14_S2_HISTORICAL_PREFIX_SHA256 = (
    "8a7b53764da4839ed0c06524ffde445c85befe10af588865c9cb93601b0a09c9"
)
WAVE14_S2_PROHIBITED_USER_PATH = "docs/research/growth_tilt_owner_diagnosis_pack.md"
OPS_067_SECTION = "phase_ops_067_reader_brief_quality_fail_closed_finalization"
OPS_067_BASE_COMMIT = "8bdac3465cacfc5beb546034507e845dc3295c21"
OPS_067_BASELINE_GIT_BLOB = "b947dbb859ed3c2a297fadf627b9d71df7791f70"
OPS_067_HISTORICAL_PREFIX_BYTE_COUNT = 1_274_261
OPS_067_HISTORICAL_PREFIX_SHA256 = (
    "bf683206146c0f0585ce292eafef06c6fe414231630be45c3589d1bb39776df4"
)
OPS_068_SECTION = "phase_ops_068_limited_non_pit_reconstruction"
OPS_068_BASE_COMMIT = "e2b264ba7f3b500c78a796c352327504b9408517"
OPS_068_BASELINE_GIT_BLOB = "fff2b77d51dd087f1c1e86f598bcc94b6f08a7d8"
OPS_068_HISTORICAL_PREFIX_BYTE_COUNT = 1_326_278
OPS_068_HISTORICAL_PREFIX_SHA256 = (
    "6f993118d3f142f2c14164355d70edb57744249c5c6ef5b5473d0dfe5c7eddbb"
)
ARCH_005S4D_SECTION = "phase_arch_005_s4d_shared_checkout_write_lease_guard"
ARCH_005S4D_BASE_COMMIT = "87bc1ae0d968f971bd12c2e6fc1a32a49a3d6f55"
ARCH_005S4D_BASELINE_GIT_BLOB = "d49bdc213bb562569c0f73dae32c6c4cb4d44020"
ARCH_005S4D_HISTORICAL_PREFIX_BYTE_COUNT = 1_367_723
ARCH_005S4D_HISTORICAL_PREFIX_SHA256 = (
    "410da90f3682ce7d3bd16ab4e15feb135769139e03ea02f6f406cb00ad947e75"
)
WAVE15_SECTION = "phase_arch_004_wave15_d0b3_g4b_g3_close"
WAVE15_BASE_COMMIT = "7ec6fd713b0e676607e38522be36b8e4d6c20d55"
WAVE15_BASELINE_GIT_BLOB = "3c8a5cb7839f2a20ca2d0484689149192648258f"
WAVE15_HISTORICAL_PREFIX_BYTE_COUNT = 1_384_553
WAVE15_HISTORICAL_PREFIX_SHA256 = "254fed6a21580289923cc8b9e7b0fc4d81e37df6156de65e576ce9d60e7cba3b"
D0B2B_SECTION = "phase_data_gov_001_d0b2b_operational_acceptance_registration"
D0B2B_BASE_COMMIT = "c908ef400d778e305c650b61124f6c6cccfe798b"
D0B2B_BASELINE_GIT_BLOB = "80065b8f558a6664bbc47f4487c70e7b57925fa8"
D0B2B_HISTORICAL_PREFIX_BYTE_COUNT = 1_431_740
D0B2B_HISTORICAL_PREFIX_SHA256 = "87fd32f6306d8b2ae0d73127fd79d540d167780d5429370cf4cce99eb9cb6a99"
OPS_069_SECTION = "phase_ops_069_daily_input_capture_and_session_gap_ledger"
OPS_069_BASE_COMMIT = "e5177870c2edc5b2dc7c439a5a5dba917fb866e1"
OPS_069_BASELINE_GIT_BLOB = "63846fd59e6affb59e84907887700a2342d6f7d8"
OPS_069_HISTORICAL_PREFIX_BYTE_COUNT = 1_477_973
OPS_069_HISTORICAL_PREFIX_SHA256 = (
    "10296a49ee1eb3a7628e2d06f012d019729e09f2326d18a0c332529c3efbd75a"
)
OPS_069_NEW_SOURCE_PATHS = frozenset(
    {
        "config/operations/daily_input_capture.yaml",
        "docs/requirements/OPS-069_Daily_Input_Capture_and_Session_Gap_Ledger.md",
        (
            "registry/development_tasks_shadow/active/e2/"
            "e243dd6b7a896e38db785f5f6f8a288efeecaada3c5bc24278ab62fe476b0ba9.yaml"
        ),
        "src/ai_trading_system/daily_input_capture.py",
        "tests/test_daily_input_capture.py",
    }
)
OPS_070_SECTION = "phase_ops_070_objective_blocker_and_consumer_dependency_dag"
OPS_070_BASE_COMMIT = "fc6313416d78f56a29519f41ca564eaa1f90e8ce"
OPS_070_BASELINE_GIT_BLOB = "431aa247873b42982c125537e4a62024bca8bdaf"
OPS_070_HISTORICAL_PREFIX_BYTE_COUNT = 1_521_808
OPS_070_HISTORICAL_PREFIX_SHA256 = (
    "4afcb86b621d45c0dbd2120167ef23009512d2a31937f9e7321c215810aee803"
)
ARCH_005S4E_SECTION = "phase_arch_005_s4e_checkout_handoff_and_source_reconciliation"
ARCH_005S4E_BASE_COMMIT = "925315059b88ee781e9dae7960d232714a610566"
ARCH_005S4E_BASELINE_GIT_BLOB = "d4c4b3e93d859a0213d1e23487aa444e700950c6"
ARCH_005S4E_HISTORICAL_PREFIX_BYTE_COUNT = 1_561_188
ARCH_005S4E_HISTORICAL_PREFIX_SHA256 = (
    "a16d34759611985147046e89ebc8e701b695fa40c706e787d3c702336cb8505f"
)
ARCH_005S4E_NEW_SOURCE_PATHS = frozenset(
    {
        "config/architecture/arch_005_s4e_checkout_reconciliation.yaml",
        ("docs/requirements/ARCH-005S4E_Checkout_Handoff_and_Source_Reconciliation.md"),
        ("docs/requirements/DEVX-001_Temporary_Workspace_Lifecycle_and_Cleanup.md"),
        (
            "registry/development_tasks_shadow/active/5f/"
            "5f2863af3be3b748326a830beac9e9046b678d403b968cc4671b85f5e92ab111.yaml"
        ),
        (
            "registry/development_tasks_shadow/active/ec/"
            "ece5b0e0c4bdf06659bac12fcee89e26a29932c6a547f791d0f6937e9c43a77c.yaml"
        ),
        "scripts/architecture_arch005_checkout_reconciliation.py",
        ("src/ai_trading_system/platform/architecture/checkout_reconciliation.py"),
        "tests/test_arch_005_s4e_checkout_reconciliation.py",
    }
)
ARCH_005S4D_S2_SECTION = "phase_arch_005_s4d_s2_read_only_telemetry"
ARCH_005S4D_S2_BASE_COMMIT = "77bc0742736d657e7294842bfba3bba5143b3b6b"
ARCH_005S4D_S2_BASELINE_GIT_BLOB = "df2da7b70613145319f0f372c3275f287ce3acec"
ARCH_005S4D_S2_HISTORICAL_PREFIX_BYTE_COUNT = 1_610_891
ARCH_005S4D_S2_HISTORICAL_PREFIX_SHA256 = (
    "418d8eb33bc1db2a429dc33fdbb42e38ca24a1a4bf22e6e807ae1e2e7dbfe3f0"
)
ARCH_005S4D_S2_NEW_SOURCE_PATHS = frozenset(
    {
        ("src/ai_trading_system/platform/architecture/checkout_telemetry.py"),
        "tests/test_arch_005_s4d_checkout_telemetry.py",
    }
)
TRADING_2458_2460_INTEGRATION_SECTION = "phase_trading_2458_2460_clean_main_integration"
TRADING_2458_2460_INTEGRATION_BASE_COMMIT = "3e58b2c6d74d38e14fe8e8c25e31ada8950eeb0e"
TRADING_2458_2460_INTEGRATION_BASELINE_GIT_BLOB = "8ca811637e56ad8793250d8bbcb38cfd679cac4d"
TRADING_2458_2460_INTEGRATION_HISTORICAL_PREFIX_BYTE_COUNT = 1_623_138
TRADING_2458_2460_INTEGRATION_HISTORICAL_PREFIX_SHA256 = (
    "41d3dea5743c4f114f525917fa362730195b4cf981a26ec54acaf8021af25a66"
)
TRADING_2458_2460_INTEGRATION_SOURCE_PATHS = frozenset(
    {
        "config/architecture/arch_004_refactor_policy.yaml",
        "config/report_registry.yaml",
        "config/research/decision_target_capability_audit_label_foundation_v1.yaml",
        ("config/research/experiments/decision_target_capability_audit_label_foundation.yaml"),
        ("config/research/experiments/leveraged_exposure_instrument_evaluation.yaml"),
        "config/research/portfolio_decision_problem.yaml",
        "config/research/protocols/portfolio_decision_problem_v1.yaml",
        "config/research/strategy_style_discovery_universe_v1.yaml",
        "docs/artifact_catalog.md",
        ("docs/requirements/DEVX-001_Temporary_Workspace_Lifecycle_and_Cleanup.md"),
        "docs/requirements/TRADING-2458_Constraint_Causal_Diagnostic.md",
        ("docs/requirements/TRADING-2459_Strategy_Style_Discovery_SPY_QLD_Universe_Evaluation.md"),
        ("docs/requirements/TRADING-2460_Decision_Target_Capability_Audit_Label_Foundation.md"),
        "docs/research/decision_target_capability_audit_label_foundation.md",
        "docs/research/leveraged_exposure_instrument_evaluation.md",
        "docs/system_flow.md",
        "docs/task_register.md",
        "inputs/architecture/arch_004e_aggregate_shadow_index.yaml",
        "inputs/architecture/arch_004e_architecture_fitness.yaml",
        "inputs/architecture/arch_004e_module_manifest.yaml",
        "inputs/architecture/arch_004e_test_manifest.yaml",
        "inputs/architecture/arch_004g_deprecation_inventory.yaml",
        "inputs/architecture/arch_005_task_registry_baseline.yaml",
        "inputs/architecture/arch_005_task_shadow_index.yaml",
        (
            "registry/development_tasks_shadow/active/18/"
            "187e589211bc90f393371ab013797585fe29cc7e5a8a76309721fee6ab2c0ae7.yaml"
        ),
        (
            "registry/development_tasks_shadow/active/28/"
            "2832c2647b0ebf371274429f2fda1908702e29ee428932ff137afb98871e3eca.yaml"
        ),
        (
            "registry/development_tasks_shadow/active/6d/"
            "6dac6c4d999ca12b3ffb0fb5ad1367b73628acb5e5545c41efdab33202f2ee8c.yaml"
        ),
        (
            "registry/development_tasks_shadow/active/ca/"
            "ca70315097648d8e19cb7498427fc2eab0fec34ebd092ada8937cfe518cfdfeb.yaml"
        ),
        (
            "registry/development_tasks_shadow/active/fa/"
            "fa0369fb8c3d8bd54932895e8aa2d98e9f5714e267d6db571abab8f860eb5b9d.yaml"
        ),
        (
            "registry/development_tasks_shadow/active/ec/"
            "ece5b0e0c4bdf06659bac12fcee89e26a29932c6a547f791d0f6937e9c43a77c.yaml"
        ),
        (
            "src/ai_trading_system/research_framework/plugins/"
            "decision_target_capability_audit_label_foundation.py"
        ),
        (
            "src/ai_trading_system/research_framework/plugins/"
            "leveraged_exposure_instrument_evaluation.py"
        ),
        "src/ai_trading_system/research_framework/runner.py",
        ("tests/research_strategies/test_decision_target_capability_audit_label_foundation.py"),
        ("tests/research_strategies/test_leveraged_exposure_instrument_evaluation.py"),
        "tests/test_arch_004_refactor_policy.py",
        "tests/test_arch_004f3_reporting_architecture.py",
        "tests/test_arch_004g_deprecation.py",
    }
)
DEVX_TRADING_CLEANUP_SECTION = "phase_devx_001_trading_workspace_cleanup"
DEVX_TRADING_CLEANUP_BASE_COMMIT = "0f585879650f3433008bbbfbbaf52f47dba1ae15"
DEVX_TRADING_CLEANUP_BASELINE_GIT_BLOB = "7ec44196f606d9f7a6c496f3e69b99cbe4a6e4b1"
DEVX_TRADING_CLEANUP_HISTORICAL_PREFIX_BYTE_COUNT = 1_637_593
DEVX_TRADING_CLEANUP_HISTORICAL_PREFIX_SHA256 = (
    "ae4ea07b89499ea40fbaa22e0a228d2566f2b07079d5d7c70acb6eb7c85ecda1"
)
DEVX_TRADING_CLEANUP_SOURCE_PATHS = frozenset(
    {
        "docs/requirements/DEVX-001_Temporary_Workspace_Lifecycle_and_Cleanup.md",
        "docs/task_register.md",
        "inputs/architecture/arch_004e_test_manifest.yaml",
        "inputs/architecture/arch_005_task_registry_baseline.yaml",
        "inputs/architecture/arch_005_task_shadow_index.yaml",
        (
            "registry/development_tasks_shadow/active/ec/"
            "ece5b0e0c4bdf06659bac12fcee89e26a29932c6a547f791d0f6937e9c43a77c.yaml"
        ),
        "tests/test_arch_004_refactor_policy.py",
    }
)
TRADING_2459_DOC_CLOSEOUT_SECTION = "phase_trading_2459_documentation_closeout"
TRADING_2459_DOC_CLOSEOUT_BASE_COMMIT = "9c7dc4d3b0cc83ce9845960acc6524379992470e"
TRADING_2459_DOC_CLOSEOUT_BASELINE_GIT_BLOB = "3cfc00d50553f53b210195186d53280e38fa1628"
TRADING_2459_DOC_CLOSEOUT_HISTORICAL_PREFIX_BYTE_COUNT = 1_642_192
TRADING_2459_DOC_CLOSEOUT_HISTORICAL_PREFIX_SHA256 = (
    "c0007c6e4e0c2f7875998314784336bb2e42b2438b64be7aa540b1c6ffe03638"
)
TRADING_2459_DOC_CLOSEOUT_SOURCE_PATHS = frozenset(
    {
        ("docs/requirements/TRADING-2459_Strategy_Style_Discovery_SPY_QLD_Universe_Evaluation.md"),
        "docs/task_register.md",
        "inputs/architecture/arch_004e_test_manifest.yaml",
        "inputs/architecture/arch_005_task_registry_baseline.yaml",
        "inputs/architecture/arch_005_task_shadow_index.yaml",
        (
            "registry/development_tasks_shadow/active/6d/"
            "6dac6c4d999ca12b3ffb0fb5ad1367b73628acb5e5545c41efdab33202f2ee8c.yaml"
        ),
        "tests/test_arch_004_refactor_policy.py",
    }
)
DATA_GOV_002_SECTION = "phase_data_gov_002_capability_receipt_phase_a"
DATA_GOV_002_BASE_COMMIT = "9c878722d7228ed81046034643361dfa2a92c3d2"
DATA_GOV_002_BASELINE_GIT_BLOB = "f5186ecb72aad2837ccec21cbe9f4870670771bd"
DATA_GOV_002_HISTORICAL_PREFIX_BYTE_COUNT = 1_646_009
DATA_GOV_002_HISTORICAL_PREFIX_SHA256 = (
    "ab360412d4a83fe544ce6cdb4df6ecbbe27ec2b66d89055627138aa9943991cf"
)
DEVX_002_SECTION = "phase_devx_002_governed_development_workflow_skill"
DEVX_002_BASE_COMMIT = "6935cdd622dcf7dad55a48685a79cc1856f39ee4"
DEVX_002_BASELINE_GIT_BLOB = "62fe82a041dbd77aef9276a2b871e97f52af2426"
DEVX_002_HISTORICAL_PREFIX_BYTE_COUNT = 1_665_104
DEVX_002_HISTORICAL_PREFIX_SHA256 = (
    "2913ffe1fce6d541b6fcd0276f26205fea7fb5039e911a8b1856ec5b2e169a5a"
)
DEVX_002_PUSH_V2_SECTION = "phase_devx_002_default_ordinary_push_v2"
DEVX_002_PUSH_V2_BASE_COMMIT = "211c934b6b4a33170b8b79b279aff267b57f6911"
DEVX_002_PUSH_V2_BASELINE_GIT_BLOB = "8c1f7462c27964ce4050727107de018d5dd7fb1e"
DEVX_002_PUSH_V2_HISTORICAL_PREFIX_BYTE_COUNT = 1_679_340
DEVX_002_PUSH_V2_HISTORICAL_PREFIX_SHA256 = (
    "6f9e336cddaa26edd0f723d432a46993bb5e56c48bab9145fe262482bd7d85e4"
)
ARCH_004G2_OBSERVABILITY_SECTION = "phase_arch_004g2_smoothed_validation_observability"
ARCH_004G2_OBSERVABILITY_BASE_COMMIT = "4bcdc69bbd2c7dd1c3c57610213201a47496d0c1"
ARCH_004G2_OBSERVABILITY_BASELINE_GIT_BLOB = "44aa4869574c1ce330a58f1ac097fb33f12c5a3f"
ARCH_004G2_OBSERVABILITY_HISTORICAL_PREFIX_BYTE_COUNT = 1_685_075
ARCH_004G2_OBSERVABILITY_HISTORICAL_PREFIX_SHA256 = (
    "7baa6d2b94ebcd4cf8ca9edded4ea94359f3ed1b4f5c703083032de7294a9df7"
)
ARCH_004G2_CLOSURE_THRESHOLD_SECTION = "phase_arch_004g2_smoothed_closure_threshold_miss"
ARCH_004G2_CLOSURE_THRESHOLD_BASE_COMMIT = "0f4bdb404bc72de5151af9f86a02061fc0e49835"
ARCH_004G2_CLOSURE_THRESHOLD_BASELINE_GIT_BLOB = "2ed23d02218aecd23da9fe72068a341e662fbf51"
ARCH_004G2_CLOSURE_THRESHOLD_HISTORICAL_PREFIX_BYTE_COUNT = 1_691_172
ARCH_004G2_CLOSURE_THRESHOLD_HISTORICAL_PREFIX_SHA256 = (
    "246c9ddd4114f419cf43b5529c2a4df7afe96811690ba7b843c76afb7d6a2bcd"
)
DATA_GOV_002_PHASE_B1_SECTION = "phase_data_gov_002_capability_receipt_phase_b1"
DATA_GOV_002_PHASE_B1_BASE_COMMIT = "463e38c6e88babb9df3d715ed8fb80ee58686cae"
DATA_GOV_002_PHASE_B1_BASELINE_GIT_BLOB = "6922d14c28ff0faaa8e0b4948eb1e333486d0286"
DATA_GOV_002_PHASE_B1_HISTORICAL_PREFIX_BYTE_COUNT = 1_695_520
DATA_GOV_002_PHASE_B1_HISTORICAL_PREFIX_SHA256 = (
    "cdb5308abdb66aa4f7bd59332b90ec6c3095dacdcc907c254bbfe5ebe43c384d"
)
LATEST_COMPATIBILITY_SECTION = DATA_GOV_002_PHASE_B1_SECTION
DATA_GOV_002_PHASE_B1_NEW_SOURCE_PATHS = frozenset(
    {
        ("config/architecture/fragments/modules/data_quality_capability_discovery.yaml"),
        "src/ai_trading_system/data/quality_capability_discovery.py",
    }
)
TRADING_2458_RETIREMENT_SECTION = "phase_trading_2458_candidate_family_retirement_v1"
TRADING_2458_RETIREMENT_BASE_COMMIT = "b8463faac3579f9b3084458f62a27d2a4f21b2b1"
TRADING_2458_RETIREMENT_BASELINE_GIT_BLOB = "4cca325113e225c9f6b8cd031d14bdec8c4b6999"
TRADING_2458_RETIREMENT_HISTORICAL_PREFIX_BYTE_COUNT = 1_703_744
TRADING_2458_RETIREMENT_HISTORICAL_PREFIX_SHA256 = (
    "475e75e34c2b38ed086dc3795aa7d03090e2834a248dbdc34a2d71de28c5b8db"
)
TRADING_2458_CLOSEOUT_SECTION = "phase_trading_2458_candidate_family_retirement_closeout"
TRADING_2458_CLOSEOUT_BASE_COMMIT = "42deab316de4eec678574647a27cd94ad43d7697"
TRADING_2458_CLOSEOUT_BASELINE_GIT_BLOB = "3b1974b825aceaf862d93f9169386987169998d9"
TRADING_2458_CLOSEOUT_HISTORICAL_PREFIX_BYTE_COUNT = 1_711_875
TRADING_2458_CLOSEOUT_HISTORICAL_PREFIX_SHA256 = (
    "6fed5c1364303061c92e9493a226464b4c2bfef6c8820a683d42ffba34dd241e"
)
LATEST_COMPATIBILITY_SECTION = TRADING_2458_CLOSEOUT_SECTION
DATA_GOV_002_PHASE_B2_SECTION = "phase_data_gov_002_capability_receipt_phase_b2"
DATA_GOV_002_PHASE_B2_BASE_COMMIT = "281c8236b3b4da103b3ccb665e19d5d51e1bba42"
DATA_GOV_002_PHASE_B2_BASELINE_GIT_BLOB = "e84ecbe30c6ba3d37335601590d2e839ba3d6754"
DATA_GOV_002_PHASE_B2_HISTORICAL_PREFIX_BYTE_COUNT = 1_716_489
DATA_GOV_002_PHASE_B2_HISTORICAL_PREFIX_SHA256 = (
    "29e403addf1d6909ccd50080c58b54b3f0a6efdf0edca4490df202a3a5a042d6"
)
LATEST_COMPATIBILITY_SECTION = DATA_GOV_002_PHASE_B2_SECTION
DATA_GOV_002_PHASE_B2_NEW_SOURCE_PATHS = frozenset(
    {
        "config/data_quality/regime_label_generator_capability_v1.yaml",
        "config/data_quality/regime_label_generator_dependency_v1.yaml",
        "config/research/regime_label_generator_policy.yaml",
        "docs/requirements/TRADING-2316_Regime_Label_Generator_Diagnostic_POC.md",
        (
            "registry/development_tasks_shadow/active/8a/"
            "8ac717bc6cb7975c7630428801f4a29995e9505bf34902f1c4266ccd2a3f9661.yaml"
        ),
        "src/ai_trading_system/regime_label_generator_diagnostic_poc.py",
        "tests/research_trends/test_regime_label_generator_diagnostic_poc.py",
    }
)
DEVX_003_SECTION = "phase_devx_003_governed_closeout_remote_preflight"
DEVX_003_BASE_COMMIT = "a309fa2f5bf5ef2205041c2ef7416c3e03487aab"
DEVX_003_BASELINE_GIT_BLOB = "6d6daae795647489d3931d84204198ce90c3784b"
DEVX_003_HISTORICAL_PREFIX_BYTE_COUNT = 1_726_631
DEVX_003_HISTORICAL_PREFIX_SHA256 = (
    "bf5484046a5931a0f89930a93fd9d4ade8a995fc831f4a018411c8d3c5f001e9"
)
LATEST_COMPATIBILITY_SECTION = DEVX_003_SECTION
DEVX_003_NEW_SOURCE_PATHS = frozenset(
    {
        "docs/requirements/DEVX-003_Governed_Closeout_Remote_Preflight.md",
        (
            "registry/development_tasks_shadow/active/7b/"
            "7b04799972c59b71610e961e1ef5b68bb12230cdbf7f125bd217d27f2a281504.yaml"
        ),
    }
)
DATA_GOV_002C1_SECTION = "phase_data_gov_002c1_dq_issue_attribution_readiness_inventory"
DATA_GOV_002C1_BASE_COMMIT = "8a319c2fe26088d414fe034478727d777ca54b84"
DATA_GOV_002C1_BASELINE_GIT_BLOB = "4c1c76ea9b7bce8cd48819d3798a298c260f1cbc"
DATA_GOV_002C1_HISTORICAL_PREFIX_BYTE_COUNT = 1_738_870
DATA_GOV_002C1_HISTORICAL_PREFIX_SHA256 = (
    "984a89c4f2eb330474e8bed3772d180eb43422937dcc9f8cfefc79f1ef79e52d"
)
LATEST_COMPATIBILITY_SECTION = DATA_GOV_002C1_SECTION
DATA_GOV_002C1_NEW_SOURCE_PATHS = frozenset(
    {
        "docs/data_quality/dq_issue_attribution_readiness_inventory_v1.md",
        ("docs/requirements/DATA-GOV-002C1_DQ_Issue_Attribution_Readiness_Inventory.md"),
        "inputs/data_quality/dq_issue_attribution_readiness_inventory_v1.json",
        ("inputs/data_quality/dq_issue_attribution_readiness_inventory_v1.validation.json"),
        (
            "registry/development_tasks_shadow/active/e6/"
            "e6356b93b6fa162226dc50ebb00b0ea60124fd52da4090f120dbd74c8afade08.yaml"
        ),
        "scripts/data_quality_issue_attribution_inventory.py",
        "src/ai_trading_system/data/quality_issue_attribution_inventory.py",
        "tests/test_data_quality_issue_attribution_inventory.py",
    }
)
DATA_GOV_002C2_SECTION = "phase_data_gov_002c2_rate_row_issue_attribution_source_owner_review_pack"
DATA_GOV_002C2_BASE_COMMIT = "fb18463e599fe404a60e99f0da718c454a03a829"
DATA_GOV_002C2_BASELINE_GIT_BLOB = "de7b3e78afde8fcd106a38c3cf272f04ff9f4bbf"
DATA_GOV_002C2_HISTORICAL_PREFIX_BYTE_COUNT = 1_748_058
DATA_GOV_002C2_HISTORICAL_PREFIX_SHA256 = (
    "1c7ae69b0d15bd74cb29b93b51f601eca115895d5591fc689b6c3785b9fcb2bc"
)
LATEST_COMPATIBILITY_SECTION = DATA_GOV_002C2_SECTION
DATA_GOV_002C2_NEW_SOURCE_PATHS = frozenset(
    {
        "config/data_quality/rate_row_issue_attribution_review_v1.yaml",
        "docs/data_quality/rate_issue_attribution_review_pack_v1.md",
        ("docs/requirements/DATA-GOV-002C2_Rate_Row_Issue_Attribution_Source_Owner_Review_Pack.md"),
        "inputs/data_quality/rate_issue_attribution_review_pack_v1.json",
        "inputs/data_quality/rate_issue_attribution_review_pack_v1.validation.json",
        (
            "registry/development_tasks_shadow/active/d8/"
            "d880fd14c51a49a1e1df382c0f862ec2d23b204b47bf606946f48a5a6bc28f33.yaml"
        ),
        "scripts/rate_issue_attribution_review_pack.py",
        "src/ai_trading_system/data/rate_issue_attribution_review_pack.py",
        "tests/test_rate_issue_attribution_review_pack.py",
    }
)
TRADING_2461_SECTION = "phase_trading_2461_decision_target_capability_audit_model_ladder"
TRADING_2461_BASE_COMMIT = "7b883b840783d35506876b07e3c512bd709f4d76"
TRADING_2461_BASELINE_GIT_BLOB = "909b9dbc745a5df588beba7f6d47d1b7d3505ede"
TRADING_2461_HISTORICAL_PREFIX_BYTE_COUNT = 1_757_969
TRADING_2461_HISTORICAL_PREFIX_SHA256 = (
    "e8a0a4a0bd6b208204204505578bb96790fdbfd9e49789a4e374ceacd065dd9e"
)
LATEST_COMPATIBILITY_SECTION = TRADING_2461_SECTION
TRADING_2461_CORE_NEW_SOURCE_PATHS = frozenset(
    {
        "config/research/decision_target_capability_audit_model_ladder_v1.yaml",
        ("config/research/experiments/decision_target_capability_audit_model_ladder.yaml"),
        ("docs/requirements/TRADING-2461_Decision_Target_Capability_Audit_Model_Ladder.md"),
        "docs/research/decision_target_capability_audit_model_ladder.md",
        (
            "src/ai_trading_system/research_framework/plugins/"
            "decision_target_capability_audit_model_ladder.py"
        ),
        ("tests/research_strategies/test_decision_target_capability_audit_model_ladder.py"),
    }
)
ARCH_004G2_PAPER_WEEKLY_SECTION = (
    "phase_arch_004g2_paper_shadow_weekly_validation_authority_candidate"
)
ARCH_004G2_PAPER_WEEKLY_BASE_COMMIT = "e4e262bba3fa35083cb88aaaaae88f6067dc74d2"
ARCH_004G2_PAPER_WEEKLY_BASELINE_GIT_BLOB = "96ea11bf997824701a067797f533d319d2449657"
ARCH_004G2_PAPER_WEEKLY_HISTORICAL_PREFIX_BYTE_COUNT = 1_766_348
ARCH_004G2_PAPER_WEEKLY_HISTORICAL_PREFIX_SHA256 = (
    "85ca0da8a70dba7d912c4cd2ca02aa0787e42ec11d3736f26d6dc4bdf018ea8c"
)
LATEST_COMPATIBILITY_SECTION = ARCH_004G2_PAPER_WEEKLY_SECTION
ARCH_004G2_PAPER_WEEKLY_NEW_SOURCE_PATHS = frozenset(
    {
        (
            "docs/requirements/"
            "ARCH-004G2_Paper_Shadow_Weekly_Immutable_Validation_Authority_Candidate.md"
        ),
    }
)
OPS_069_TERMINAL_ARCHIVE_SECTION = "phase_ops_069_daily_input_capture_terminal_archive"
OPS_069_TERMINAL_ARCHIVE_BASE_COMMIT = "0e4d5a862855b88dacb48a539985b77b41757479"
OPS_069_TERMINAL_ARCHIVE_BASELINE_GIT_BLOB = "92aca5c09f20d41b587a786ebe371c57928320d5"
OPS_069_TERMINAL_ARCHIVE_HISTORICAL_PREFIX_BYTE_COUNT = 1_771_189
OPS_069_TERMINAL_ARCHIVE_HISTORICAL_PREFIX_SHA256 = (
    "0b41765c7bb8d49a12416d4b1dfaa88a8912578b423cd80e54a65ca3a7291ea4"
)
LATEST_COMPATIBILITY_SECTION = OPS_069_TERMINAL_ARCHIVE_SECTION
OPS_069_ACTIVE_TASK_SHADOW_PATH = (
    "registry/development_tasks_shadow/active/e2/"
    "e243dd6b7a896e38db785f5f6f8a288efeecaada3c5bc24278ab62fe476b0ba9.yaml"
)
OPS_069_COMPLETED_TASK_SHADOW_PATH = (
    "registry/development_tasks_shadow/completed/e2/"
    "e243dd6b7a896e38db785f5f6f8a288efeecaada3c5bc24278ab62fe476b0ba9.yaml"
)
OPS_069_TERMINAL_ARCHIVE_NEW_SOURCE_PATHS = frozenset({OPS_069_COMPLETED_TASK_SHADOW_PATH})
OPS_069_TERMINAL_ARCHIVE_REMOVED_SOURCE_PATHS = frozenset({OPS_069_ACTIVE_TASK_SHADOW_PATH})
DEVX_004_SECTION = "phase_devx_004_completed_task_closeout_registration"
DEVX_004_BASE_COMMIT = "9baa0d59a204d01002a75d9c0c18516daedfaa35"
DEVX_004_BASELINE_GIT_BLOB = "721a566b99ec2f5c03d1e20e5ecbf3cefdcb5fb8"
DEVX_004_HISTORICAL_PREFIX_BYTE_COUNT = 1_775_827
DEVX_004_HISTORICAL_PREFIX_SHA256 = (
    "31ed072dd1272da8141324b130b50312a004c252a931670afbf7dd20bffe0235"
)
LATEST_COMPATIBILITY_SECTION = DEVX_004_SECTION
DEVX_004_NEW_SOURCE_PATHS = frozenset(
    {
        "docs/requirements/DEVX-004_Completed_Task_Closeout_Registration.md",
        (
            "registry/development_tasks_shadow/active/76/"
            "76c18e8bca6074a7c2a8006bf1f50fe4c2ce24ef304274dbda4fb1cef4cdbd3c.yaml"
        ),
    }
)
DEVX_001_RECONCILIATION_SECTION = "phase_devx_001_research_worktree_lifecycle_reconciliation"
DEVX_001_RECONCILIATION_BASE_COMMIT = "f8b32b24b1983afc39a4a1cfdfb4d31d584d82fb"
DEVX_001_RECONCILIATION_BASELINE_GIT_BLOB = "cd0034202297b0a4199f12641b14e42f521f503d"
DEVX_001_RECONCILIATION_HISTORICAL_PREFIX_BYTE_COUNT = 1_780_838
DEVX_001_RECONCILIATION_HISTORICAL_PREFIX_SHA256 = (
    "8f8f46dc9807dbce44a606d726847babb82dd6a726ec547d428c4bb07f273cc4"
)
LATEST_COMPATIBILITY_SECTION = DEVX_001_RECONCILIATION_SECTION
DEVX_001_RECONCILIATION_NEW_SOURCE_PATHS = frozenset(
    {
        "docs/requirements/DEVX-005_Target_Bound_Worktree_Audit.md",
        (
            "registry/development_tasks_shadow/active/e6/"
            "e60a070f4acc65852357170dc373ecc665003d8e3618a5437708a106cf9f9569.yaml"
        ),
    }
)
DEVX_005_SECTION = "phase_devx_005_target_bound_worktree_audit"
DEVX_005_BASE_COMMIT = "0cb8bf9be3e1f91044e6cb950f6b401b5e230fb3"
DEVX_005_BASELINE_GIT_BLOB = "28f5172f095bc8b7cc8626752729594f49e6adee"
DEVX_005_HISTORICAL_PREFIX_BYTE_COUNT = 1_785_830
DEVX_005_HISTORICAL_PREFIX_SHA256 = (
    "cdc4be90c932312bef7cae4cb0b58c1a0733947c639ff9fc63da27206a73a4c0"
)
LATEST_COMPATIBILITY_SECTION = DEVX_005_SECTION
DEVX_005_NEW_SOURCE_PATHS: frozenset[str] = frozenset()
TRADING_2462_SECTION = "phase_trading_2462_tail_risk_robustness_falsification_audit"
TRADING_2462_BASE_COMMIT = "bc8496b11039f3d6a8d2bc837e821c298e04c9cf"
TRADING_2462_BASELINE_GIT_BLOB = "074ca94a5ebec39e7007ba70fedcd802731dabc1"
TRADING_2462_HISTORICAL_PREFIX_BYTE_COUNT = 1_792_642
TRADING_2462_HISTORICAL_PREFIX_SHA256 = (
    "776bd60ed7edd2eb9fffc5d87f7134983e1bacbd38a5360c1b041b284281b2ef"
)
LATEST_COMPATIBILITY_SECTION = TRADING_2462_SECTION
TRADING_2462_CORE_NEW_SOURCE_PATHS = frozenset(
    {
        "config/research/decision_target_tail_risk_robustness_audit_v1.yaml",
        "config/research/experiments/decision_target_tail_risk_robustness_audit.yaml",
        ("docs/requirements/TRADING-2462_Tail_Risk_Capability_Robustness_Falsification_Audit.md"),
        "docs/research/decision_target_tail_risk_robustness_audit.md",
        (
            "src/ai_trading_system/research_framework/plugins/"
            "decision_target_tail_risk_robustness_audit.py"
        ),
        "tests/research_strategies/test_decision_target_tail_risk_robustness_audit.py",
    }
)
TRADING_2462_ACTIVE_TASK_SHADOW_PATH = (
    "registry/development_tasks_shadow/active/8a/"
    "8a124c3f1fb3cd10eddf178834b6a0d62026760cc274f49707e423203f45fa9d.yaml"
)
TRADING_2462_COMPLETED_TASK_SHADOW_PATH = (
    "registry/development_tasks_shadow/completed/8a/"
    "8a124c3f1fb3cd10eddf178834b6a0d62026760cc274f49707e423203f45fa9d.yaml"
)
DEVX_006_SECTION = "phase_devx_006_base_drift_aware_integration_revalidation"
DEVX_006_BASE_COMMIT = "6dc8a643ae02c2cda47b4572eec452fce80251ef"
DEVX_006_BASELINE_GIT_BLOB = "40741323a69ff5418a58633346de93cfb318af68"
DEVX_006_HISTORICAL_PREFIX_BYTE_COUNT = 1_801_243
DEVX_006_HISTORICAL_PREFIX_SHA256 = (
    "927e29db52bd81907307e710fe44d5d6455efc58a8b5543efab82944ede48dba"
)
DEVX_006_ACTIVE_TASK_SHADOW_PATH = (
    "registry/development_tasks_shadow/active/82/"
    "8208f6495030a28c1d528a7782e6ffe01e96146f35ba0efaa8dd72c47760a9fb.yaml"
)
DEVX_006_COMPLETED_TASK_SHADOW_PATH = (
    "registry/development_tasks_shadow/completed/82/"
    "8208f6495030a28c1d528a7782e6ffe01e96146f35ba0efaa8dd72c47760a9fb.yaml"
)
LATEST_COMPATIBILITY_SECTION = DEVX_006_SECTION
DEVX_006_NEW_SOURCE_PATHS = frozenset(
    {
        "config/architecture/arch_005_integration_revalidation.yaml",
        "docs/requirements/ARCH-005M1_Strict_YAML_Loader_Consolidation.md",
        "docs/requirements/DEVX-006_Base_Drift_Aware_Integration_and_Revalidation.md",
        DEVX_006_COMPLETED_TASK_SHADOW_PATH,
        "scripts/architecture_arch005_integration_revalidation.py",
        "src/ai_trading_system/platform/architecture/integration_revalidation.py",
        "src/ai_trading_system/yaml_loader.py",
        "tests/test_arch_005_integration_revalidation.py",
        "tests/test_yaml_loader.py",
    }
)
ARCH_005M2_SECTION = "phase_arch_005m2_portable_bootstrap_bundle_adoption"
ARCH_005M2_BASE_COMMIT = "b3ce8d70e3917522d9abdaf4d168f812ff47878e"
ARCH_005M2_BASELINE_GIT_BLOB = "e8a3a7977c981be1ee64f518f2e112296bc535a5"
ARCH_005M2_HISTORICAL_PREFIX_BYTE_COUNT = 1_813_310
ARCH_005M2_HISTORICAL_PREFIX_SHA256 = (
    "3ad1154dd078384bfbaf897036b8485d43d313952846deef61bc42ab9880048d"
)
ARCH_005M2_ACTIVE_TASK_SHADOW_PATH = (
    "registry/development_tasks_shadow/active/76/"
    "76274bb2bb89b8ff54bb486ddf8b5c1c43f87d66fbfebca3cf760326c864e415.yaml"
)
ARCH_005M2_COMPLETED_TASK_SHADOW_PATH = (
    "registry/development_tasks_shadow/completed/76/"
    "76274bb2bb89b8ff54bb486ddf8b5c1c43f87d66fbfebca3cf760326c864e415.yaml"
)
ARCH_005M2_CORE_NEW_SOURCE_PATHS = frozenset(
    {
        "docs/requirements/ARCH-005M2_Bootstrap_Standalone_Validator_Portable_Bundle_Adoption.md",
        "scripts/architecture_arch005_portable_validation_bundle.py",
        (
            "src/ai_trading_system/platform/architecture/"
            "portable_validation_bundle.py"
        ),
        "tests/test_arch_005m2_portable_validation_bundle.py",
    }
)
ARCH_005M3_SECTION = (
    "phase_arch_005m3_external_request_cache_multiprocess_harness_hardening"
)
ARCH_005M3_BASE_COMMIT = "970d5189f707a3e7b1fd62a7d96c24cbbda79d4b"
ARCH_005M3_BASELINE_GIT_BLOB = "4c3a959b584b322afc250396fe35d7d11a78a317"
ARCH_005M3_HISTORICAL_PREFIX_BYTE_COUNT = 1_826_176
ARCH_005M3_HISTORICAL_PREFIX_SHA256 = (
    "7c543793ffd697d70d08366ad46fb1e8a88ffb026b37cd8a14d0f3b0a41128db"
)
ARCH_005M3_ACTIVE_TASK_SHADOW_PATH = (
    "registry/development_tasks_shadow/active/c3/"
    "c381b25072ea135344a2576c2b622a9d2b3a891d53775df866277c77fada39f1.yaml"
)
ARCH_005M3_COMPLETED_TASK_SHADOW_PATH = (
    "registry/development_tasks_shadow/completed/c3/"
    "c381b25072ea135344a2576c2b622a9d2b3a891d53775df866277c77fada39f1.yaml"
)
ARCH_005M3_CORE_NEW_SOURCE_PATHS = frozenset(
    {
        "docs/requirements/ARCH-005M3_External_Request_Cache_Multiprocess_Harness_Hardening.md",
        "docs/task_register_completed.md",
    }
)
ARCH_005M3_FROZEN_PRODUCTION_PATHS = frozenset(
    {
        "config/data/external_request_cache_revalidation_coordination_policy.yaml",
        "src/ai_trading_system/external_request_cache_revalidation_coordination.py",
    }
)
ARCH_005M1_BATCH2_SECTION = (
    "phase_arch_005m1_batch2_integration_revalidation_loader"
)
ARCH_005M1_BATCH2_BASE_COMMIT = "ebeb67f6d014d4037a2559093a8e2394d96fd9dd"
ARCH_005M1_BATCH2_BASELINE_GIT_BLOB = "45ddf4bf3be0651b9ce86961a191e91dc5acff1a"
ARCH_005M1_BATCH2_HISTORICAL_PREFIX_BYTE_COUNT = 1_836_039
ARCH_005M1_BATCH2_HISTORICAL_PREFIX_SHA256 = (
    "5f4aeb45edd474a213feece9b743c533f621a06ccad0b97d20e27bda4afb4ad3"
)
ARCH_005M1_BATCH2_ACTIVE_TASK_SHADOW_PATH = (
    "registry/development_tasks_shadow/active/0a/"
    "0abb8a3ad247288bda28379233fd84e047b071d1ab19dbbe650d6c1c8bf49355.yaml"
)
ARCH_005M1_BATCH2_FROZEN_PATHS = frozenset(
    {
        "config/architecture/arch_005_integration_revalidation.yaml",
        "src/ai_trading_system/platform/architecture/wave_readiness.py",
        "src/ai_trading_system/us_equity_special_closure_policy.py",
        "src/ai_trading_system/yaml_loader.py",
    }
)
ARCH_005M1_BATCH3_SECTION = "phase_arch_005m1_batch3_wave_readiness_loader"
ARCH_005M1_BATCH3_BASE_COMMIT = "edb356bf9c038d7d2d1ba6056ac3783a763bbbab"
ARCH_005M1_BATCH3_BASELINE_GIT_BLOB = "802eaf47794a94d6481f987717821ee283adf5bf"
ARCH_005M1_BATCH3_HISTORICAL_PREFIX_BYTE_COUNT = 1_841_511
ARCH_005M1_BATCH3_HISTORICAL_PREFIX_SHA256 = (
    "a293bc06efb0c7e9176e2aed5174da12449110889830f98de176b633caf129ef"
)
ARCH_005M1_BATCH3_ACTIVE_TASK_SHADOW_PATH = (
    "registry/development_tasks_shadow/active/0a/"
    "0abb8a3ad247288bda28379233fd84e047b071d1ab19dbbe650d6c1c8bf49355.yaml"
)
ARCH_005M1_BATCH3_FROZEN_PATHS = frozenset(
    {
        "config/architecture/arch_004_wave12_g4_d0b_readiness.yaml",
        "config/architecture/arch_004_wave14_d0b2_g3_readiness.yaml",
        "config/architecture/arch_004_wave15_d0b3_g4b_g3_close_readiness.yaml",
        "config/architecture/arch_004_wave15_g3_close_readiness.yaml",
        "scripts/architecture_wave_readiness.py",
        "src/ai_trading_system/platform/architecture/integration_revalidation.py",
        "src/ai_trading_system/us_equity_special_closure_policy.py",
        "src/ai_trading_system/yaml_loader.py",
    }
)
ARCH_005M1_BATCH4_SECTION = (
    "phase_arch_005m1_batch4_us_equity_special_closure_loader"
)
ARCH_005M1_BATCH4_BASE_COMMIT = "ad22819f3aa666d7ac920b6b3583fff588279f73"
ARCH_005M1_BATCH4_BASELINE_GIT_BLOB = "3b97c4e575042ec9dacb14952b344afdbb306e70"
ARCH_005M1_BATCH4_HISTORICAL_PREFIX_BYTE_COUNT = 1_846_974
ARCH_005M1_BATCH4_HISTORICAL_PREFIX_SHA256 = (
    "bb1d82dad109a91461ee5d7de854b48582ef982e0403761f983c25197c895201"
)
ARCH_005M1_BATCH4_ACTIVE_TASK_SHADOW_PATH = (
    "registry/development_tasks_shadow/active/0a/"
    "0abb8a3ad247288bda28379233fd84e047b071d1ab19dbbe650d6c1c8bf49355.yaml"
)
ARCH_005M1_BATCH4_COMPLETED_TASK_SHADOW_PATH = (
    "registry/development_tasks_shadow/completed/0a/"
    "0abb8a3ad247288bda28379233fd84e047b071d1ab19dbbe650d6c1c8bf49355.yaml"
)
ARCH_005M1_BATCH4_FROZEN_PATHS = frozenset(
    {
        "config/data/us_equity_special_closure_registry.yaml",
        "src/ai_trading_system/platform/architecture/arch_004_g2_5_readiness.py",
        "src/ai_trading_system/platform/architecture/integration_revalidation.py",
        "src/ai_trading_system/platform/architecture/task_portfolio_normalization.py",
        "src/ai_trading_system/platform/architecture/wave_readiness.py",
        "src/ai_trading_system/trading_calendar.py",
        "src/ai_trading_system/yaml_loader.py",
    }
)
OPS_070_STABLE_RELEASE_SECTION = "phase_ops_070_stable_ops_deployment_release"
OPS_070_STABLE_RELEASE_BASE_COMMIT = "00d98ddaa2828852c1086ea9176935643e11e205"
OPS_070_STABLE_RELEASE_BASELINE_GIT_BLOB = (
    "2872030cfea709051e775d868797cae834d0185d"
)
OPS_070_STABLE_RELEASE_HISTORICAL_PREFIX_BYTE_COUNT = 1_857_331
OPS_070_STABLE_RELEASE_HISTORICAL_PREFIX_SHA256 = (
    "f6efbcf88720d27a233e6b42f185029aaf21afdd8895f671b48dc0da13508d98"
)
OPS_070_STABLE_RELEASE_NEW_SOURCE_PATHS = frozenset(
    {
        "config/operations/ops_release_promotion.yaml",
        (
            "docs/requirements/"
            "DEVX-006_Fragmented_Generated_Authority_and_Stable_Task_Shadow_v2.md"
        ),
        (
            "registry/development_tasks_shadow/active/98/"
            "989bc4bfe58706d37f7b749b47ba03259688afcb2cac1cdf1fafb35b290130af.yaml"
        ),
        "src/ai_trading_system/ops_release_promotion.py",
        "tests/test_ops_release_promotion.py",
    }
)
OPS_070_RUNTIME_EXCLUDE_SECTION = "phase_ops_070_runtime_git_exclusion_contract"
OPS_070_RUNTIME_EXCLUDE_BASE_COMMIT = "992734147b4e25a300694f07c1d7323d37641501"
OPS_070_RUNTIME_EXCLUDE_BASELINE_GIT_BLOB = (
    "7c830fb0840a890b619538af6b904f810e2703ce"
)
OPS_070_RUNTIME_EXCLUDE_HISTORICAL_PREFIX_BYTE_COUNT = 1_867_053
OPS_070_RUNTIME_EXCLUDE_HISTORICAL_PREFIX_SHA256 = (
    "fff12d89d6b61ab394e622257203baf28f461a1a61d472c343aa031470f5862e"
)
OPS_070_CROSS_RELEASE_POLICY_SECTION = (
    "phase_ops_070_cross_release_promotion_policy_authority"
)
OPS_070_CROSS_RELEASE_POLICY_BASE_COMMIT = (
    "f462836e3d599ad7e718a487045c3cc1d2ed20a8"
)
OPS_070_CROSS_RELEASE_POLICY_BASELINE_GIT_BLOB = (
    "7846080de69beca5900a4260f8eac27b13150a42"
)
OPS_070_CROSS_RELEASE_POLICY_HISTORICAL_PREFIX_BYTE_COUNT = 1_873_407
OPS_070_CROSS_RELEASE_POLICY_HISTORICAL_PREFIX_SHA256 = (
    "60474696d6e9a66dfc64149da02f557d526a07d27217a820f2720a5acfb4f0df"
)
LATEST_COMPATIBILITY_SECTION = OPS_070_CROSS_RELEASE_POLICY_SECTION
TRADING_2458_RETIREMENT_NEW_SOURCE_PATHS = frozenset(
    {
        "config/research/trading2458_candidate_family_retirement_v1.yaml",
        "docs/research/trading2458_candidate_family_retirement.md",
        "src/ai_trading_system/trading2458_candidate_family_retirement.py",
        "tests/test_trading2458_candidate_family_retirement.py",
    }
)
TRADING_2458_CLOSEOUT_NEW_SOURCE_PATHS = frozenset(
    {
        (
            "registry/development_tasks_shadow/completed/18/"
            "187e589211bc90f393371ab013797585fe29cc7e5a8a76309721fee6ab2c0ae7.yaml"
        ),
    }
)
TRADING_2461_ACTIVE_TASK_SHADOW_PATH = (
    "registry/development_tasks_shadow/active/8f/"
    "8f94232414037d95253e1089454c402ee929cfea494b338d55145a38cdb7a374.yaml"
)
TRADING_2461_COMPLETED_TASK_SHADOW_PATH = (
    "registry/development_tasks_shadow/completed/8f/"
    "8f94232414037d95253e1089454c402ee929cfea494b338d55145a38cdb7a374.yaml"
)
TRADING_2458_CLOSEOUT_REMOVED_SOURCE_PATHS = frozenset(
    {
        (
            "registry/development_tasks_shadow/active/18/"
            "187e589211bc90f393371ab013797585fe29cc7e5a8a76309721fee6ab2c0ae7.yaml"
        ),
    }
)
ARCH_004G2_OBSERVABILITY_NEW_SOURCE_PATHS = frozenset(
    {"tests/profile_smoothed_validation_chain.py"}
)
DEVX_002_NEW_SOURCE_PATHS = frozenset(
    {
        "docs/requirements/DEVX-002_Governed_Development_Workflow_Skill.md",
        (
            "registry/development_tasks_shadow/active/18/"
            "1822c0cf93a0237c022d88f5f201813efcdf3dd3d0b4cfbb1200ef2eac2d5d00.yaml"
        ),
        "tests/test_governed_development_skill.py",
        "tools/codex_skills/run-governed-development/SKILL.md",
        "tools/codex_skills/run-governed-development/agents/openai.yaml",
        "tools/codex_skills/run-governed-development/references/workflow-modes.md",
        "tools/codex_skills/run-governed-development/scripts/preflight.py",
        "tools/codex_skills/run-governed-development/scripts/verify_bundle_parity.py",
    }
)
DATA_GOV_002_NEW_SOURCE_PATHS = frozenset(
    {
        "config/architecture/fragments/flows/data_quality_capability.yaml",
        "config/architecture/fragments/modules/data_quality_capability.yaml",
        "config/data_quality/decision_target_label_core_capability_v1.yaml",
        "config/research/decision_target_capability_audit_label_foundation_v2.yaml",
        ("docs/requirements/DATA-GOV-002_Consumer_Capability_Scoped_Data_Quality_Receipts.md"),
        (
            "registry/development_tasks_shadow/active/ac/"
            "acfbf141d52be91423be6a52f49e44eabfbc7d41cd86941ed1bc87f8226a2252.yaml"
        ),
        "src/ai_trading_system/contracts/data_quality_capability.py",
        "src/ai_trading_system/data/quality_capability.py",
        "tests/test_data_quality_capability.py",
    }
)
OPS_070_NEW_SOURCE_PATHS = frozenset(
    {
        "config/operations/ops_scheduler_checkout.yaml",
        "docs/requirements/OPS-070_Objective_Blocker_and_Consumer_Dependency_DAG.md",
        (
            "registry/development_tasks_shadow/active/53/"
            "5384d046e12134a40fd6b11e3967cba8902da30174e414395ac2a09ba454fdab.yaml"
        ),
        "src/ai_trading_system/cli_commands/pit_snapshots.py",
        "src/ai_trading_system/fmp_forward_pit.py",
        "src/ai_trading_system/ops_scheduler_checkout.py",
        "src/ai_trading_system/pipeline_health.py",
        "src/ai_trading_system/pit_snapshots.py",
        "tests/test_arch_004c_platform_adapters.py",
        "tests/test_fmp_forward_pit.py",
        "tests/test_ops_scheduler_checkout.py",
        "tests/test_pipeline_health.py",
        "tests/test_pit_snapshots.py",
    }
)
D0B2B_NEW_SOURCE_PATHS = frozenset(
    {
        "config/data/canonical_price_session_policy.yaml",
        "config/data/us_equity_special_closure_registry.yaml",
        ("docs/requirements/DATA-GOV-001_D0B2B_Canonical_Daily_Acceptance_Remediation.md"),
        (
            "registry/development_tasks_shadow/active/bc/"
            "bcfdb006c5efbb9a62a5c2be93a7e9b323e9cde3a5e82ce9de2b434e42ac6350.yaml"
        ),
        "src/ai_trading_system/data/market_data.py",
        "src/ai_trading_system/us_equity_special_closure_policy.py",
        "tests/test_market_data.py",
        "tests/test_us_equity_special_closure_policy.py",
    }
)
WAVE15_NEW_SOURCE_PATHS = frozenset(
    {
        "config/architecture/arch_004_wave15_g3_close_readiness.yaml",
        "config/data_quality/arch_004_wave15_daily_score_consumer_authorization.yaml",
        "config/research/trading2458_constraint_causal_diagnostic.yaml",
        "src/ai_trading_system/contracts/data_quality_consumer_authorization.py",
        "src/ai_trading_system/data/quality_consumer_authorization.py",
        "src/ai_trading_system/platform/reporting/g3_close_readiness.py",
        "src/ai_trading_system/trading2458_constraint_causal_diagnostic.py",
        "tests/test_arch_004_wave15_g3_close_readiness.py",
        "tests/test_data_quality_consumer_authorization.py",
        "tests/test_trading2458_constraint_causal_diagnostic.py",
    }
)
OPS_068_NEW_SOURCE_PATHS = frozenset(
    {
        "docs/requirements/OPS-068_2026_07_21_Daily_Gap_Recovery.md",
        "docs/schema/limited_non_pit_reconstruction.v2.schema.json",
        (
            "registry/development_tasks_shadow/completed/9d/"
            "9dd21402a1abc63d94ac7a64ad1fac93c09d13564ab383ee4f9146ce7693fde7.yaml"
        ),
        "src/ai_trading_system/limited_non_pit_reconstruction.py",
        "tests/test_limited_non_pit_reconstruction.py",
    }
)
WAVE14_S2_PRE_FULL_REQUIRED_TIERS = (
    "combined_focused",
    "static",
    "report_validation",
    "architecture_fitness",
    "contract_validation",
    "integration",
    "reproducibility",
)
WAVE14_S2_POST_FULL_REQUIRED_TIERS = (
    "focused",
    "architecture_fitness",
    "contract_validation",
    "integration",
    "reproducibility",
)
WAVE14_S2_ACTIVE_TASK_SHADOW_PATH = (
    "registry/development_tasks_shadow/active/d4/"
    "d4fc4bd29b4d23452253c0bc5a7889e5dbd8195817ffd56ac7561a9897c1d9bf.yaml"
)
WAVE14_S2_COMPLETED_TASK_SHADOW_PATH = (
    "registry/development_tasks_shadow/completed/d4/"
    "d4fc4bd29b4d23452253c0bc5a7889e5dbd8195817ffd56ac7561a9897c1d9bf.yaml"
)
WAVE14_S2_APPROVED_POST_FULL_EVIDENCE_ONLY_PATHS = frozenset(
    {
        "config/architecture/arch_004_refactor_policy.yaml",
        "docs/architecture/dual_lane_development_operating_model.md",
        "docs/operations/operations_runbook.md",
        ("docs/requirements/ARCH-004G4_D0B_Shared_DQ_Preflight_and_Periodic_Consumer_Migration.md"),
        "docs/requirements/ARCH-004G_Domain_Migration_and_Subtraction.md",
        ("docs/requirements/ARCH-004_Post_2438N_System_Architecture_Refactor_Program.md"),
        "docs/requirements/ARCH-004_Wave14_D0B2_G3_Parallel_Readiness.md",
        "docs/requirements/ARCH-005S4D_Shared_Checkout_Write_Lease_Guard.md",
        "docs/requirements/ARCH-005_Parallel_Development_Control_Plane.md",
        "docs/requirements/DATA-GOV-001_Unified_Data_Foundation_Governance.md",
        "docs/system_flow.md",
        "docs/task_register.md",
        "docs/task_register_completed.md",
        WAVE11_BASELINE_REPOSITORY_PATH,
        "inputs/architecture/arch_004g_deprecation_inventory.yaml",
        "inputs/architecture/arch_005_task_registry_baseline.yaml",
        "inputs/architecture/arch_005_task_shadow_index.yaml",
        WAVE14_S2_ACTIVE_TASK_SHADOW_PATH,
        WAVE14_S2_COMPLETED_TASK_SHADOW_PATH,
    }
)


@cache
def _wave11_base_baseline_blob() -> bytes:
    object_name = f"{WAVE11_BASE_COMMIT}:{WAVE11_BASELINE_REPOSITORY_PATH}"
    object_id = subprocess.run(
        ["git", "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert object_id == WAVE11_BASELINE_GIT_BLOB
    return subprocess.run(
        ["git", "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout


@cache
def _docs_gov_base_baseline_blob() -> bytes:
    object_name = f"{DOCS_GOV_BASE_COMMIT}:{WAVE11_BASELINE_REPOSITORY_PATH}"
    object_id = subprocess.run(
        ["git", "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert object_id == DOCS_GOV_BASELINE_GIT_BLOB
    return subprocess.run(
        ["git", "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout


@cache
def _wave12_base_baseline_blob() -> bytes:
    object_name = f"{WAVE12_BASE_COMMIT}:{WAVE11_BASELINE_REPOSITORY_PATH}"
    object_id = subprocess.run(
        ["git", "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert object_id == WAVE12_BASELINE_GIT_BLOB
    return subprocess.run(
        ["git", "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout


@cache
def _wave13_base_baseline_blob() -> bytes:
    object_name = f"{WAVE13_BASE_COMMIT}:{WAVE11_BASELINE_REPOSITORY_PATH}"
    object_id = subprocess.run(
        ["git", "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert object_id == WAVE13_BASELINE_GIT_BLOB
    return subprocess.run(
        ["git", "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout


@cache
def _wave14_s0_1_base_baseline_blob() -> bytes:
    object_name = f"{WAVE14_S0_1_BASE_COMMIT}:{WAVE11_BASELINE_REPOSITORY_PATH}"
    object_id = subprocess.run(
        ["git", "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert object_id == WAVE14_S0_1_BASELINE_GIT_BLOB
    return subprocess.run(
        ["git", "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout


@cache
def _wave14_s2_base_baseline_blob() -> bytes:
    object_name = f"{WAVE14_S2_BASE_COMMIT}:{WAVE11_BASELINE_REPOSITORY_PATH}"
    object_id = subprocess.run(
        ["git", "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert object_id == WAVE14_S2_BASELINE_GIT_BLOB
    return subprocess.run(
        ["git", "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout


@cache
def _ops_067_base_baseline_blob() -> bytes:
    object_name = f"{OPS_067_BASE_COMMIT}:{WAVE11_BASELINE_REPOSITORY_PATH}"
    object_id = subprocess.run(
        ["git", "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert object_id == OPS_067_BASELINE_GIT_BLOB
    return subprocess.run(
        ["git", "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout


@cache
def _ops_068_base_baseline_blob() -> bytes:
    object_name = f"{OPS_068_BASE_COMMIT}:{WAVE11_BASELINE_REPOSITORY_PATH}"
    object_id = subprocess.run(
        ["git", "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert object_id == OPS_068_BASELINE_GIT_BLOB
    return subprocess.run(
        ["git", "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout


@cache
def _arch_005s4d_base_baseline_blob() -> bytes:
    object_name = f"{ARCH_005S4D_BASE_COMMIT}:{WAVE11_BASELINE_REPOSITORY_PATH}"
    object_id = subprocess.run(
        ["git", "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert object_id == ARCH_005S4D_BASELINE_GIT_BLOB
    return subprocess.run(
        ["git", "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout


@cache
def _wave15_base_baseline_blob() -> bytes:
    object_name = f"{WAVE15_BASE_COMMIT}:{WAVE11_BASELINE_REPOSITORY_PATH}"
    object_id = subprocess.run(
        ["git", "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert object_id == WAVE15_BASELINE_GIT_BLOB
    return subprocess.run(
        ["git", "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout


@cache
def _d0b2b_base_baseline_blob() -> bytes:
    object_name = f"{D0B2B_BASE_COMMIT}:{WAVE11_BASELINE_REPOSITORY_PATH}"
    object_id = subprocess.run(
        ["git", "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert object_id == D0B2B_BASELINE_GIT_BLOB
    return subprocess.run(
        ["git", "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout


@cache
def _ops_069_base_baseline_blob() -> bytes:
    object_name = f"{OPS_069_BASE_COMMIT}:{WAVE11_BASELINE_REPOSITORY_PATH}"
    object_id = subprocess.run(
        ["git", "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert object_id == OPS_069_BASELINE_GIT_BLOB
    return subprocess.run(
        ["git", "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout


@cache
def _ops_070_base_baseline_blob() -> bytes:
    object_name = f"{OPS_070_BASE_COMMIT}:{WAVE11_BASELINE_REPOSITORY_PATH}"
    object_id = subprocess.run(
        ["git", "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert object_id == OPS_070_BASELINE_GIT_BLOB
    return subprocess.run(
        ["git", "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout


@cache
def _arch_005s4e_base_baseline_blob() -> bytes:
    object_name = f"{ARCH_005S4E_BASE_COMMIT}:{WAVE11_BASELINE_REPOSITORY_PATH}"
    object_id = subprocess.run(
        ["git", "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert object_id == ARCH_005S4E_BASELINE_GIT_BLOB
    return subprocess.run(
        ["git", "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout


@cache
def _arch_005s4d_s2_base_baseline_blob() -> bytes:
    object_name = f"{ARCH_005S4D_S2_BASE_COMMIT}:{WAVE11_BASELINE_REPOSITORY_PATH}"
    object_id = subprocess.run(
        ["git", "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert object_id == ARCH_005S4D_S2_BASELINE_GIT_BLOB
    return subprocess.run(
        ["git", "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout


@cache
def _trading_2458_2460_integration_base_baseline_blob() -> bytes:
    object_name = f"{TRADING_2458_2460_INTEGRATION_BASE_COMMIT}:{WAVE11_BASELINE_REPOSITORY_PATH}"
    object_id = subprocess.run(
        ["git", "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert object_id == TRADING_2458_2460_INTEGRATION_BASELINE_GIT_BLOB
    return subprocess.run(
        ["git", "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout


@cache
def _devx_trading_cleanup_base_baseline_blob() -> bytes:
    object_name = f"{DEVX_TRADING_CLEANUP_BASE_COMMIT}:{WAVE11_BASELINE_REPOSITORY_PATH}"
    object_id = subprocess.run(
        ["git", "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert object_id == DEVX_TRADING_CLEANUP_BASELINE_GIT_BLOB
    return subprocess.run(
        ["git", "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout


@cache
def _trading_2459_doc_closeout_base_baseline_blob() -> bytes:
    object_name = f"{TRADING_2459_DOC_CLOSEOUT_BASE_COMMIT}:{WAVE11_BASELINE_REPOSITORY_PATH}"
    object_id = subprocess.run(
        ["git", "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert object_id == TRADING_2459_DOC_CLOSEOUT_BASELINE_GIT_BLOB
    return subprocess.run(
        ["git", "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout


@cache
def _data_gov_002_base_baseline_blob() -> bytes:
    object_name = f"{DATA_GOV_002_BASE_COMMIT}:{WAVE11_BASELINE_REPOSITORY_PATH}"
    object_id = subprocess.run(
        ["git", "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert object_id == DATA_GOV_002_BASELINE_GIT_BLOB
    return subprocess.run(
        ["git", "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout


@cache
def _devx_002_base_baseline_blob() -> bytes:
    object_name = f"{DEVX_002_BASE_COMMIT}:{WAVE11_BASELINE_REPOSITORY_PATH}"
    object_id = subprocess.run(
        ["git", "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert object_id == DEVX_002_BASELINE_GIT_BLOB
    return subprocess.run(
        ["git", "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout


@cache
def _devx_002_push_v2_base_baseline_blob() -> bytes:
    object_name = f"{DEVX_002_PUSH_V2_BASE_COMMIT}:{WAVE11_BASELINE_REPOSITORY_PATH}"
    object_id = subprocess.run(
        ["git", "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert object_id == DEVX_002_PUSH_V2_BASELINE_GIT_BLOB
    return subprocess.run(
        ["git", "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout


@cache
def _arch_004g2_observability_base_baseline_blob() -> bytes:
    object_name = f"{ARCH_004G2_OBSERVABILITY_BASE_COMMIT}:{WAVE11_BASELINE_REPOSITORY_PATH}"
    object_id = subprocess.run(
        ["git", "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert object_id == ARCH_004G2_OBSERVABILITY_BASELINE_GIT_BLOB
    return subprocess.run(
        ["git", "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout


@cache
def _arch_004g2_closure_threshold_base_baseline_blob() -> bytes:
    object_name = f"{ARCH_004G2_CLOSURE_THRESHOLD_BASE_COMMIT}:{WAVE11_BASELINE_REPOSITORY_PATH}"
    object_id = subprocess.run(
        ["git", "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert object_id == ARCH_004G2_CLOSURE_THRESHOLD_BASELINE_GIT_BLOB
    return subprocess.run(
        ["git", "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout


@cache
def _data_gov_002_phase_b1_base_baseline_blob() -> bytes:
    object_name = f"{DATA_GOV_002_PHASE_B1_BASE_COMMIT}:{WAVE11_BASELINE_REPOSITORY_PATH}"
    object_id = subprocess.run(
        ["git", "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert object_id == DATA_GOV_002_PHASE_B1_BASELINE_GIT_BLOB
    return subprocess.run(
        ["git", "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout


@cache
def _trading_2458_retirement_base_baseline_blob() -> bytes:
    object_name = f"{TRADING_2458_RETIREMENT_BASE_COMMIT}:{WAVE11_BASELINE_REPOSITORY_PATH}"
    object_id = subprocess.run(
        ["git", "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert object_id == TRADING_2458_RETIREMENT_BASELINE_GIT_BLOB
    return subprocess.run(
        ["git", "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout


@cache
def _trading_2458_closeout_base_baseline_blob() -> bytes:
    object_name = f"{TRADING_2458_CLOSEOUT_BASE_COMMIT}:{WAVE11_BASELINE_REPOSITORY_PATH}"
    object_id = subprocess.run(
        ["git", "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert object_id == TRADING_2458_CLOSEOUT_BASELINE_GIT_BLOB
    return subprocess.run(
        ["git", "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout


@cache
def _data_gov_002_phase_b2_base_baseline_blob() -> bytes:
    object_name = f"{DATA_GOV_002_PHASE_B2_BASE_COMMIT}:{WAVE11_BASELINE_REPOSITORY_PATH}"
    object_id = subprocess.run(
        ["git", "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert object_id == DATA_GOV_002_PHASE_B2_BASELINE_GIT_BLOB
    return subprocess.run(
        ["git", "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout


@cache
def _devx_003_base_baseline_blob() -> bytes:
    object_name = f"{DEVX_003_BASE_COMMIT}:{WAVE11_BASELINE_REPOSITORY_PATH}"
    object_id = subprocess.run(
        ["git", "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert object_id == DEVX_003_BASELINE_GIT_BLOB
    return subprocess.run(
        ["git", "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout


@cache
def _data_gov_002c1_base_baseline_blob() -> bytes:
    object_name = f"{DATA_GOV_002C1_BASE_COMMIT}:{WAVE11_BASELINE_REPOSITORY_PATH}"
    object_id = subprocess.run(
        ["git", "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert object_id == DATA_GOV_002C1_BASELINE_GIT_BLOB
    return subprocess.run(
        ["git", "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout


@cache
def _data_gov_002c2_base_baseline_blob() -> bytes:
    object_name = f"{DATA_GOV_002C2_BASE_COMMIT}:{WAVE11_BASELINE_REPOSITORY_PATH}"
    object_id = subprocess.run(
        ["git", "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert object_id == DATA_GOV_002C2_BASELINE_GIT_BLOB
    return subprocess.run(
        ["git", "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout


@cache
def _trading_2461_base_baseline_blob() -> bytes:
    object_name = f"{TRADING_2461_BASE_COMMIT}:{WAVE11_BASELINE_REPOSITORY_PATH}"
    object_id = subprocess.run(
        ["git", "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert object_id == TRADING_2461_BASELINE_GIT_BLOB
    return subprocess.run(
        ["git", "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout


@cache
def _arch_004g2_paper_weekly_base_baseline_blob() -> bytes:
    object_name = f"{ARCH_004G2_PAPER_WEEKLY_BASE_COMMIT}:{WAVE11_BASELINE_REPOSITORY_PATH}"
    object_id = subprocess.run(
        ["git", "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert object_id == ARCH_004G2_PAPER_WEEKLY_BASELINE_GIT_BLOB
    return subprocess.run(
        ["git", "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout


@cache
def _ops_069_terminal_archive_base_baseline_blob() -> bytes:
    object_name = f"{OPS_069_TERMINAL_ARCHIVE_BASE_COMMIT}:{WAVE11_BASELINE_REPOSITORY_PATH}"
    object_id = subprocess.run(
        ["git", "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert object_id == OPS_069_TERMINAL_ARCHIVE_BASELINE_GIT_BLOB
    return subprocess.run(
        ["git", "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout


@cache
def _devx_004_base_baseline_blob() -> bytes:
    object_name = f"{DEVX_004_BASE_COMMIT}:{WAVE11_BASELINE_REPOSITORY_PATH}"
    object_id = subprocess.run(
        ["git", "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert object_id == DEVX_004_BASELINE_GIT_BLOB
    return subprocess.run(
        ["git", "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout


@cache
def _devx_001_reconciliation_base_baseline_blob() -> bytes:
    object_name = f"{DEVX_001_RECONCILIATION_BASE_COMMIT}:{WAVE11_BASELINE_REPOSITORY_PATH}"
    object_id = subprocess.run(
        ["git", "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert object_id == DEVX_001_RECONCILIATION_BASELINE_GIT_BLOB
    return subprocess.run(
        ["git", "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout


@cache
def _devx_005_base_baseline_blob() -> bytes:
    object_name = f"{DEVX_005_BASE_COMMIT}:{WAVE11_BASELINE_REPOSITORY_PATH}"
    object_id = subprocess.run(
        ["git", "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert object_id == DEVX_005_BASELINE_GIT_BLOB
    return subprocess.run(
        ["git", "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout


@cache
def _trading_2462_base_baseline_blob() -> bytes:
    object_name = f"{TRADING_2462_BASE_COMMIT}:{WAVE11_BASELINE_REPOSITORY_PATH}"
    object_id = subprocess.run(
        ["git", "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert object_id == TRADING_2462_BASELINE_GIT_BLOB
    return subprocess.run(
        ["git", "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout


@cache
def _devx_006_base_baseline_blob() -> bytes:
    object_name = f"{DEVX_006_BASE_COMMIT}:{WAVE11_BASELINE_REPOSITORY_PATH}"
    object_id = subprocess.run(
        ["git", "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert object_id == DEVX_006_BASELINE_GIT_BLOB
    return subprocess.run(
        ["git", "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout


@cache
def _arch_005m2_base_baseline_blob() -> bytes:
    object_name = f"{ARCH_005M2_BASE_COMMIT}:{WAVE11_BASELINE_REPOSITORY_PATH}"
    object_id = subprocess.run(
        ["git", "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert object_id == ARCH_005M2_BASELINE_GIT_BLOB
    return subprocess.run(
        ["git", "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout


@cache
def _arch_005m3_base_baseline_blob() -> bytes:
    object_name = f"{ARCH_005M3_BASE_COMMIT}:{WAVE11_BASELINE_REPOSITORY_PATH}"
    object_id = subprocess.run(
        ["git", "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert object_id == ARCH_005M3_BASELINE_GIT_BLOB
    return subprocess.run(
        ["git", "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout


@cache
def _arch_005m1_batch2_base_baseline_blob() -> bytes:
    object_name = (
        f"{ARCH_005M1_BATCH2_BASE_COMMIT}:{WAVE11_BASELINE_REPOSITORY_PATH}"
    )
    object_id = subprocess.run(
        ["git", "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert object_id == ARCH_005M1_BATCH2_BASELINE_GIT_BLOB
    return subprocess.run(
        ["git", "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout


@cache
def _arch_005m1_batch3_base_baseline_blob() -> bytes:
    object_name = (
        f"{ARCH_005M1_BATCH3_BASE_COMMIT}:{WAVE11_BASELINE_REPOSITORY_PATH}"
    )
    object_id = subprocess.run(
        ["git", "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert object_id == ARCH_005M1_BATCH3_BASELINE_GIT_BLOB
    return subprocess.run(
        ["git", "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout


@cache
def _arch_005m1_batch4_base_baseline_blob() -> bytes:
    object_name = (
        f"{ARCH_005M1_BATCH4_BASE_COMMIT}:{WAVE11_BASELINE_REPOSITORY_PATH}"
    )
    object_id = subprocess.run(
        ["git", "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert object_id == ARCH_005M1_BATCH4_BASELINE_GIT_BLOB
    return subprocess.run(
        ["git", "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout


@cache
def _ops_070_stable_release_base_baseline_blob() -> bytes:
    object_name = (
        f"{OPS_070_STABLE_RELEASE_BASE_COMMIT}:"
        f"{WAVE11_BASELINE_REPOSITORY_PATH}"
    )
    object_id = subprocess.run(
        ["git", "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert object_id == OPS_070_STABLE_RELEASE_BASELINE_GIT_BLOB
    return subprocess.run(
        ["git", "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout


@cache
def _ops_070_runtime_exclude_base_baseline_blob() -> bytes:
    object_name = (
        f"{OPS_070_RUNTIME_EXCLUDE_BASE_COMMIT}:"
        f"{WAVE11_BASELINE_REPOSITORY_PATH}"
    )
    object_id = subprocess.run(
        ["git", "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert object_id == OPS_070_RUNTIME_EXCLUDE_BASELINE_GIT_BLOB
    return subprocess.run(
        ["git", "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout


@cache
def _ops_070_cross_release_policy_base_baseline_blob() -> bytes:
    object_name = (
        f"{OPS_070_CROSS_RELEASE_POLICY_BASE_COMMIT}:"
        f"{WAVE11_BASELINE_REPOSITORY_PATH}"
    )
    object_id = subprocess.run(
        ["git", "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert object_id == OPS_070_CROSS_RELEASE_POLICY_BASELINE_GIT_BLOB
    return subprocess.run(
        ["git", "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout


def _assert_wave11_historical_prefix_immutable(
    current_bytes: bytes,
    base_blob: bytes,
) -> None:
    assert len(base_blob) == WAVE11_HISTORICAL_PREFIX_BYTE_COUNT
    assert hashlib.sha256(base_blob).hexdigest() == WAVE11_HISTORICAL_PREFIX_SHA256
    historical_prefix = current_bytes[:WAVE11_HISTORICAL_PREFIX_BYTE_COUNT]
    assert historical_prefix == base_blob, (
        "Wave11 historical prefix differs from the immutable base blob"
    )
    assert hashlib.sha256(historical_prefix).hexdigest() == WAVE11_HISTORICAL_PREFIX_SHA256
    wave11_suffix = current_bytes[WAVE11_HISTORICAL_PREFIX_BYTE_COUNT:]
    expected_marker = f"\n{WAVE11_SECTION}:\n".encode()
    assert wave11_suffix.startswith(expected_marker), (
        "Wave11 must be appended after the exact base blob with one blank line"
    )
    assert wave11_suffix.count(expected_marker) == 1


def _assert_docs_gov_historical_prefix_immutable(
    current_bytes: bytes,
    base_blob: bytes,
) -> None:
    assert len(base_blob) == DOCS_GOV_HISTORICAL_PREFIX_BYTE_COUNT
    assert hashlib.sha256(base_blob).hexdigest() == DOCS_GOV_HISTORICAL_PREFIX_SHA256
    historical_prefix = current_bytes[:DOCS_GOV_HISTORICAL_PREFIX_BYTE_COUNT]
    assert historical_prefix == base_blob, (
        "DOCS-GOV historical prefix differs from the immutable Wave11 closeout blob"
    )
    assert hashlib.sha256(historical_prefix).hexdigest() == DOCS_GOV_HISTORICAL_PREFIX_SHA256
    docs_gov_suffix = current_bytes[DOCS_GOV_HISTORICAL_PREFIX_BYTE_COUNT:]
    expected_marker = f"\n{DOCS_GOV_SECTION}:\n".encode()
    assert docs_gov_suffix.startswith(expected_marker), (
        "DOCS-GOV closeout must be appended after the exact Wave11 closeout blob"
    )
    assert current_bytes.count(expected_marker) == 1


def _assert_wave12_historical_prefix_immutable(
    current_bytes: bytes,
    base_blob: bytes,
) -> None:
    assert len(base_blob) == WAVE12_HISTORICAL_PREFIX_BYTE_COUNT
    assert hashlib.sha256(base_blob).hexdigest() == WAVE12_HISTORICAL_PREFIX_SHA256
    historical_prefix = current_bytes[:WAVE12_HISTORICAL_PREFIX_BYTE_COUNT]
    assert historical_prefix == base_blob, "Wave12 historical prefix differs from its base blob"
    assert hashlib.sha256(historical_prefix).hexdigest() == WAVE12_HISTORICAL_PREFIX_SHA256
    wave12_suffix = current_bytes[WAVE12_HISTORICAL_PREFIX_BYTE_COUNT:]
    expected_marker = f"\n{WAVE12_SECTION}:\n".encode()
    assert wave12_suffix.startswith(expected_marker), (
        "Wave12 closeout must be appended after the exact prior baseline blob"
    )
    assert current_bytes.count(expected_marker) == 1


def _assert_wave13_historical_prefix_immutable(
    current_bytes: bytes,
    base_blob: bytes,
) -> None:
    assert len(base_blob) == WAVE13_HISTORICAL_PREFIX_BYTE_COUNT
    assert hashlib.sha256(base_blob).hexdigest() == WAVE13_HISTORICAL_PREFIX_SHA256
    historical_prefix = current_bytes[:WAVE13_HISTORICAL_PREFIX_BYTE_COUNT]
    assert historical_prefix == base_blob, "Wave13 historical prefix differs from its base blob"
    assert hashlib.sha256(historical_prefix).hexdigest() == WAVE13_HISTORICAL_PREFIX_SHA256
    wave13_suffix = current_bytes[WAVE13_HISTORICAL_PREFIX_BYTE_COUNT:]
    expected_marker = f"\n{WAVE13_SECTION}:\n".encode()
    assert wave13_suffix.startswith(expected_marker), (
        "Wave13 closeout must be appended after the exact application-commit baseline blob"
    )
    assert current_bytes.count(expected_marker) == 1


def _assert_wave14_s0_1_historical_prefix_immutable(
    current_bytes: bytes,
    base_blob: bytes,
) -> None:
    assert len(base_blob) == WAVE14_S0_1_HISTORICAL_PREFIX_BYTE_COUNT
    assert hashlib.sha256(base_blob).hexdigest() == WAVE14_S0_1_HISTORICAL_PREFIX_SHA256
    historical_prefix = current_bytes[:WAVE14_S0_1_HISTORICAL_PREFIX_BYTE_COUNT]
    assert historical_prefix == base_blob, (
        "Wave14 S0.1 historical prefix differs from the Wave13 closeout blob"
    )
    assert hashlib.sha256(historical_prefix).hexdigest() == WAVE14_S0_1_HISTORICAL_PREFIX_SHA256
    wave14_suffix = current_bytes[WAVE14_S0_1_HISTORICAL_PREFIX_BYTE_COUNT:]
    expected_marker = f"\n{WAVE14_S0_1_SECTION}:\n".encode()
    assert wave14_suffix.startswith(expected_marker), (
        "Wave14 S0.1 must be appended after the exact Wave13 closeout blob"
    )
    assert current_bytes.count(expected_marker) == 1


def _assert_wave14_s2_historical_prefix_immutable(
    current_bytes: bytes,
    base_blob: bytes,
) -> None:
    assert len(base_blob) == WAVE14_S2_HISTORICAL_PREFIX_BYTE_COUNT
    assert hashlib.sha256(base_blob).hexdigest() == WAVE14_S2_HISTORICAL_PREFIX_SHA256
    historical_prefix = current_bytes[:WAVE14_S2_HISTORICAL_PREFIX_BYTE_COUNT]
    assert historical_prefix == base_blob, (
        "Wave14 S2 historical prefix differs from the immutable S0 carrier blob"
    )
    assert hashlib.sha256(historical_prefix).hexdigest() == WAVE14_S2_HISTORICAL_PREFIX_SHA256
    wave14_s2_suffix = current_bytes[WAVE14_S2_HISTORICAL_PREFIX_BYTE_COUNT:]
    expected_marker = f"\n{WAVE14_S2_SECTION}:\n".encode()
    assert wave14_s2_suffix.startswith(expected_marker), (
        "Wave14 S2 must be appended after the exact S0 carrier blob"
    )
    assert current_bytes.count(expected_marker) == 1


def _assert_ops_067_historical_prefix_immutable(
    current_bytes: bytes,
    base_blob: bytes,
) -> None:
    assert len(base_blob) == OPS_067_HISTORICAL_PREFIX_BYTE_COUNT
    assert hashlib.sha256(base_blob).hexdigest() == OPS_067_HISTORICAL_PREFIX_SHA256
    historical_prefix = current_bytes[:OPS_067_HISTORICAL_PREFIX_BYTE_COUNT]
    assert historical_prefix == base_blob, "OPS-067 historical prefix differs from Wave14 S2"
    assert hashlib.sha256(historical_prefix).hexdigest() == OPS_067_HISTORICAL_PREFIX_SHA256
    ops_067_suffix = current_bytes[OPS_067_HISTORICAL_PREFIX_BYTE_COUNT:]
    expected_marker = f"\n{OPS_067_SECTION}:\n".encode()
    assert ops_067_suffix.startswith(expected_marker), (
        "OPS-067 must be appended after the exact Wave14 S2 baseline blob"
    )
    assert current_bytes.count(expected_marker) == 1


def _assert_ops_068_historical_prefix_immutable(
    current_bytes: bytes,
    base_blob: bytes,
) -> None:
    assert len(base_blob) == OPS_068_HISTORICAL_PREFIX_BYTE_COUNT
    assert hashlib.sha256(base_blob).hexdigest() == OPS_068_HISTORICAL_PREFIX_SHA256
    historical_prefix = current_bytes[:OPS_068_HISTORICAL_PREFIX_BYTE_COUNT]
    assert historical_prefix == base_blob, "OPS-068 historical prefix differs from OPS-067"
    assert hashlib.sha256(historical_prefix).hexdigest() == OPS_068_HISTORICAL_PREFIX_SHA256
    ops_068_suffix = current_bytes[OPS_068_HISTORICAL_PREFIX_BYTE_COUNT:]
    expected_marker = f"\n{OPS_068_SECTION}:\n".encode()
    assert ops_068_suffix.startswith(expected_marker), (
        "OPS-068 must be appended after the exact OPS-067 baseline blob"
    )
    assert current_bytes.count(expected_marker) == 1


def _assert_arch_005s4d_historical_prefix_immutable(
    current_bytes: bytes,
    base_blob: bytes,
) -> None:
    assert len(base_blob) == ARCH_005S4D_HISTORICAL_PREFIX_BYTE_COUNT
    assert hashlib.sha256(base_blob).hexdigest() == ARCH_005S4D_HISTORICAL_PREFIX_SHA256
    historical_prefix = current_bytes[:ARCH_005S4D_HISTORICAL_PREFIX_BYTE_COUNT]
    assert historical_prefix == base_blob, "ARCH-005S4D historical prefix differs from OPS-068"
    assert hashlib.sha256(historical_prefix).hexdigest() == (ARCH_005S4D_HISTORICAL_PREFIX_SHA256)
    suffix = current_bytes[ARCH_005S4D_HISTORICAL_PREFIX_BYTE_COUNT:]
    expected_marker = f"\n{ARCH_005S4D_SECTION}:\n".encode()
    assert suffix.startswith(expected_marker), (
        "ARCH-005S4D must be appended after the exact OPS-068 baseline blob"
    )
    assert current_bytes.count(expected_marker) == 1


def _assert_wave15_historical_prefix_immutable(
    current_bytes: bytes,
    base_blob: bytes,
) -> None:
    assert len(base_blob) == WAVE15_HISTORICAL_PREFIX_BYTE_COUNT
    assert hashlib.sha256(base_blob).hexdigest() == WAVE15_HISTORICAL_PREFIX_SHA256
    historical_prefix = current_bytes[:WAVE15_HISTORICAL_PREFIX_BYTE_COUNT]
    assert historical_prefix == base_blob, "Wave15 historical prefix differs from carrier D"
    assert hashlib.sha256(historical_prefix).hexdigest() == WAVE15_HISTORICAL_PREFIX_SHA256
    suffix = current_bytes[WAVE15_HISTORICAL_PREFIX_BYTE_COUNT:]
    expected_marker = f"\n{WAVE15_SECTION}:\n".encode()
    assert suffix.startswith(expected_marker), (
        "Wave15 must be appended after the exact carrier-D baseline blob"
    )
    assert current_bytes.count(expected_marker) == 1


def _assert_d0b2b_historical_prefix_immutable(
    current_bytes: bytes,
    base_blob: bytes,
) -> None:
    assert len(base_blob) == D0B2B_HISTORICAL_PREFIX_BYTE_COUNT
    assert hashlib.sha256(base_blob).hexdigest() == D0B2B_HISTORICAL_PREFIX_SHA256
    historical_prefix = current_bytes[:D0B2B_HISTORICAL_PREFIX_BYTE_COUNT]
    assert historical_prefix == base_blob, "D0B2B historical prefix differs from Wave15"
    assert hashlib.sha256(historical_prefix).hexdigest() == D0B2B_HISTORICAL_PREFIX_SHA256
    suffix = current_bytes[D0B2B_HISTORICAL_PREFIX_BYTE_COUNT:]
    expected_marker = f"\n{D0B2B_SECTION}:\n".encode()
    assert suffix.startswith(expected_marker), (
        "D0B2B must be appended after the exact Wave15 baseline blob"
    )
    assert current_bytes.count(expected_marker) == 1


def _assert_ops_069_historical_prefix_immutable(
    current_bytes: bytes,
    base_blob: bytes,
) -> None:
    assert len(base_blob) == OPS_069_HISTORICAL_PREFIX_BYTE_COUNT
    assert hashlib.sha256(base_blob).hexdigest() == OPS_069_HISTORICAL_PREFIX_SHA256
    historical_prefix = current_bytes[:OPS_069_HISTORICAL_PREFIX_BYTE_COUNT]
    assert historical_prefix == base_blob, "OPS-069 historical prefix differs from D0B2B"
    assert hashlib.sha256(historical_prefix).hexdigest() == OPS_069_HISTORICAL_PREFIX_SHA256
    suffix = current_bytes[OPS_069_HISTORICAL_PREFIX_BYTE_COUNT:]
    expected_marker = f"\n{OPS_069_SECTION}:\n".encode()
    assert suffix.startswith(expected_marker), (
        "OPS-069 must be appended after the exact D0B2B baseline blob"
    )
    assert current_bytes.count(expected_marker) == 1


def _assert_ops_070_historical_prefix_immutable(
    current_bytes: bytes,
    base_blob: bytes,
) -> None:
    assert len(base_blob) == OPS_070_HISTORICAL_PREFIX_BYTE_COUNT
    assert hashlib.sha256(base_blob).hexdigest() == OPS_070_HISTORICAL_PREFIX_SHA256
    historical_prefix = current_bytes[:OPS_070_HISTORICAL_PREFIX_BYTE_COUNT]
    assert historical_prefix == base_blob, "OPS-070 historical prefix differs from OPS-069"
    assert hashlib.sha256(historical_prefix).hexdigest() == OPS_070_HISTORICAL_PREFIX_SHA256
    suffix = current_bytes[OPS_070_HISTORICAL_PREFIX_BYTE_COUNT:]
    expected_marker = f"\n{OPS_070_SECTION}:\n".encode()
    assert suffix.startswith(expected_marker), (
        "OPS-070 must be appended after the exact OPS-069 baseline blob"
    )
    assert current_bytes.count(expected_marker) == 1


def _assert_arch_005s4e_historical_prefix_immutable(
    current_bytes: bytes,
    base_blob: bytes,
) -> None:
    assert len(base_blob) == ARCH_005S4E_HISTORICAL_PREFIX_BYTE_COUNT
    assert hashlib.sha256(base_blob).hexdigest() == ARCH_005S4E_HISTORICAL_PREFIX_SHA256
    historical_prefix = current_bytes[:ARCH_005S4E_HISTORICAL_PREFIX_BYTE_COUNT]
    assert historical_prefix == base_blob, "ARCH-005S4E historical prefix differs from OPS-070"
    assert hashlib.sha256(historical_prefix).hexdigest() == (ARCH_005S4E_HISTORICAL_PREFIX_SHA256)
    suffix = current_bytes[ARCH_005S4E_HISTORICAL_PREFIX_BYTE_COUNT:]
    expected_marker = f"\n{ARCH_005S4E_SECTION}:\n".encode()
    assert suffix.startswith(expected_marker), (
        "ARCH-005S4E must be appended after the exact OPS-070 baseline blob"
    )
    assert current_bytes.count(expected_marker) == 1


def _assert_arch_005s4d_s2_historical_prefix_immutable(
    current_bytes: bytes,
    base_blob: bytes,
) -> None:
    assert len(base_blob) == ARCH_005S4D_S2_HISTORICAL_PREFIX_BYTE_COUNT
    assert hashlib.sha256(base_blob).hexdigest() == (ARCH_005S4D_S2_HISTORICAL_PREFIX_SHA256)
    historical_prefix = current_bytes[:ARCH_005S4D_S2_HISTORICAL_PREFIX_BYTE_COUNT]
    assert historical_prefix == base_blob, (
        "ARCH-005S4D S2 historical prefix differs from ARCH-005S4E"
    )
    assert hashlib.sha256(historical_prefix).hexdigest() == (
        ARCH_005S4D_S2_HISTORICAL_PREFIX_SHA256
    )
    suffix = current_bytes[ARCH_005S4D_S2_HISTORICAL_PREFIX_BYTE_COUNT:]
    expected_marker = f"\n{ARCH_005S4D_S2_SECTION}:\n".encode()
    assert suffix.startswith(expected_marker), (
        "ARCH-005S4D S2 must be appended after the exact ARCH-005S4E baseline blob"
    )
    assert current_bytes.count(expected_marker) == 1


def _assert_trading_2458_2460_integration_historical_prefix_immutable(
    current_bytes: bytes,
    base_blob: bytes,
) -> None:
    assert len(base_blob) == TRADING_2458_2460_INTEGRATION_HISTORICAL_PREFIX_BYTE_COUNT
    assert hashlib.sha256(base_blob).hexdigest() == (
        TRADING_2458_2460_INTEGRATION_HISTORICAL_PREFIX_SHA256
    )
    historical_prefix = current_bytes[:TRADING_2458_2460_INTEGRATION_HISTORICAL_PREFIX_BYTE_COUNT]
    assert historical_prefix == base_blob, (
        "TRADING clean-main integration historical prefix differs from ARCH-005S4D S2"
    )
    assert hashlib.sha256(historical_prefix).hexdigest() == (
        TRADING_2458_2460_INTEGRATION_HISTORICAL_PREFIX_SHA256
    )
    suffix = current_bytes[TRADING_2458_2460_INTEGRATION_HISTORICAL_PREFIX_BYTE_COUNT:]
    expected_marker = f"\n{TRADING_2458_2460_INTEGRATION_SECTION}:\n".encode()
    assert suffix.startswith(expected_marker), (
        "TRADING clean-main integration must be appended after the exact "
        "ARCH-005S4D S2 baseline blob"
    )
    assert current_bytes.count(expected_marker) == 1


def _assert_devx_trading_cleanup_historical_prefix_immutable(
    current_bytes: bytes,
    base_blob: bytes,
) -> None:
    assert len(base_blob) == DEVX_TRADING_CLEANUP_HISTORICAL_PREFIX_BYTE_COUNT
    assert hashlib.sha256(base_blob).hexdigest() == DEVX_TRADING_CLEANUP_HISTORICAL_PREFIX_SHA256
    historical_prefix = current_bytes[:DEVX_TRADING_CLEANUP_HISTORICAL_PREFIX_BYTE_COUNT]
    assert historical_prefix == base_blob, (
        "DEVX TRADING cleanup historical prefix differs from the immutable integration blob"
    )
    assert hashlib.sha256(historical_prefix).hexdigest() == (
        DEVX_TRADING_CLEANUP_HISTORICAL_PREFIX_SHA256
    )
    suffix = current_bytes[DEVX_TRADING_CLEANUP_HISTORICAL_PREFIX_BYTE_COUNT:]
    expected_marker = f"\n{DEVX_TRADING_CLEANUP_SECTION}:\n".encode()
    assert suffix.startswith(expected_marker), (
        "DEVX TRADING cleanup must be appended after the exact integration blob"
    )
    assert current_bytes.count(expected_marker) == 1


def _assert_trading_2459_doc_closeout_historical_prefix_immutable(
    current_bytes: bytes,
    base_blob: bytes,
) -> None:
    assert len(base_blob) == TRADING_2459_DOC_CLOSEOUT_HISTORICAL_PREFIX_BYTE_COUNT
    assert (
        hashlib.sha256(base_blob).hexdigest() == TRADING_2459_DOC_CLOSEOUT_HISTORICAL_PREFIX_SHA256
    )
    historical_prefix = current_bytes[:TRADING_2459_DOC_CLOSEOUT_HISTORICAL_PREFIX_BYTE_COUNT]
    assert historical_prefix == base_blob, (
        "TRADING-2459 documentation closeout historical prefix differs from "
        "the immutable DEVX cleanup blob"
    )
    assert (
        hashlib.sha256(historical_prefix).hexdigest()
        == TRADING_2459_DOC_CLOSEOUT_HISTORICAL_PREFIX_SHA256
    )
    suffix = current_bytes[TRADING_2459_DOC_CLOSEOUT_HISTORICAL_PREFIX_BYTE_COUNT:]
    expected_marker = f"\n{TRADING_2459_DOC_CLOSEOUT_SECTION}:\n".encode()
    assert suffix.startswith(expected_marker), (
        "TRADING-2459 documentation closeout must be appended after the exact DEVX cleanup blob"
    )
    assert current_bytes.count(expected_marker) == 1


def _assert_data_gov_002_historical_prefix_immutable(
    current_bytes: bytes,
    base_blob: bytes,
) -> None:
    assert len(base_blob) == DATA_GOV_002_HISTORICAL_PREFIX_BYTE_COUNT
    assert hashlib.sha256(base_blob).hexdigest() == DATA_GOV_002_HISTORICAL_PREFIX_SHA256
    historical_prefix = current_bytes[:DATA_GOV_002_HISTORICAL_PREFIX_BYTE_COUNT]
    assert historical_prefix == base_blob, (
        "DATA-GOV-002 historical prefix differs from the immutable "
        "TRADING-2459 documentation-closeout blob"
    )
    assert hashlib.sha256(historical_prefix).hexdigest() == (DATA_GOV_002_HISTORICAL_PREFIX_SHA256)
    suffix = current_bytes[DATA_GOV_002_HISTORICAL_PREFIX_BYTE_COUNT:]
    expected_marker = f"\n{DATA_GOV_002_SECTION}:\n".encode()
    assert suffix.startswith(expected_marker), (
        "DATA-GOV-002 must be appended after the exact TRADING-2459 documentation-closeout blob"
    )
    assert current_bytes.count(expected_marker) == 1


def _assert_devx_002_historical_prefix_immutable(
    current_bytes: bytes,
    base_blob: bytes,
) -> None:
    assert len(base_blob) == DEVX_002_HISTORICAL_PREFIX_BYTE_COUNT
    assert hashlib.sha256(base_blob).hexdigest() == DEVX_002_HISTORICAL_PREFIX_SHA256
    historical_prefix = current_bytes[:DEVX_002_HISTORICAL_PREFIX_BYTE_COUNT]
    assert historical_prefix == base_blob, (
        "DEVX-002 historical prefix differs from the immutable DATA-GOV-002 blob"
    )
    assert hashlib.sha256(historical_prefix).hexdigest() == DEVX_002_HISTORICAL_PREFIX_SHA256
    suffix = current_bytes[DEVX_002_HISTORICAL_PREFIX_BYTE_COUNT:]
    expected_marker = f"\n{DEVX_002_SECTION}:\n".encode()
    assert suffix.startswith(expected_marker), (
        "DEVX-002 must be appended after the exact DATA-GOV-002 compatibility blob"
    )
    assert current_bytes.count(expected_marker) == 1


def _assert_devx_002_push_v2_historical_prefix_immutable(
    current_bytes: bytes,
    base_blob: bytes,
) -> None:
    assert len(base_blob) == DEVX_002_PUSH_V2_HISTORICAL_PREFIX_BYTE_COUNT
    assert hashlib.sha256(base_blob).hexdigest() == DEVX_002_PUSH_V2_HISTORICAL_PREFIX_SHA256
    historical_prefix = current_bytes[:DEVX_002_PUSH_V2_HISTORICAL_PREFIX_BYTE_COUNT]
    assert historical_prefix == base_blob, (
        "DEVX-002 push-v2 historical prefix differs from the immutable DEVX-002 v1 blob"
    )
    assert (
        hashlib.sha256(historical_prefix).hexdigest() == DEVX_002_PUSH_V2_HISTORICAL_PREFIX_SHA256
    )
    suffix = current_bytes[DEVX_002_PUSH_V2_HISTORICAL_PREFIX_BYTE_COUNT:]
    expected_marker = f"\n{DEVX_002_PUSH_V2_SECTION}:\n".encode()
    assert suffix.startswith(expected_marker), (
        "DEVX-002 push-v2 must be appended after the exact DEVX-002 v1 compatibility blob"
    )
    assert current_bytes.count(expected_marker) == 1


def _assert_arch_004g2_observability_historical_prefix_immutable(
    current_bytes: bytes,
    base_blob: bytes,
) -> None:
    assert len(base_blob) == ARCH_004G2_OBSERVABILITY_HISTORICAL_PREFIX_BYTE_COUNT
    assert (
        hashlib.sha256(base_blob).hexdigest() == ARCH_004G2_OBSERVABILITY_HISTORICAL_PREFIX_SHA256
    )
    historical_prefix = current_bytes[:ARCH_004G2_OBSERVABILITY_HISTORICAL_PREFIX_BYTE_COUNT]
    assert historical_prefix == base_blob, (
        "ARCH-004G2 observability historical prefix differs from the immutable "
        "DEVX-002 push-v2 blob"
    )
    assert (
        hashlib.sha256(historical_prefix).hexdigest()
        == ARCH_004G2_OBSERVABILITY_HISTORICAL_PREFIX_SHA256
    )
    suffix = current_bytes[ARCH_004G2_OBSERVABILITY_HISTORICAL_PREFIX_BYTE_COUNT:]
    expected_marker = f"\n{ARCH_004G2_OBSERVABILITY_SECTION}:\n".encode()
    assert suffix.startswith(expected_marker), (
        "ARCH-004G2 observability must be appended after the exact DEVX-002 push-v2 blob"
    )
    assert current_bytes.count(expected_marker) == 1


def _assert_arch_004g2_closure_threshold_historical_prefix_immutable(
    current_bytes: bytes,
    base_blob: bytes,
) -> None:
    assert len(base_blob) == ARCH_004G2_CLOSURE_THRESHOLD_HISTORICAL_PREFIX_BYTE_COUNT
    assert (
        hashlib.sha256(base_blob).hexdigest()
        == ARCH_004G2_CLOSURE_THRESHOLD_HISTORICAL_PREFIX_SHA256
    )
    historical_prefix = current_bytes[:ARCH_004G2_CLOSURE_THRESHOLD_HISTORICAL_PREFIX_BYTE_COUNT]
    assert historical_prefix == base_blob, (
        "ARCH-004G2 closure-threshold historical prefix differs from the immutable "
        "observability blob"
    )
    assert (
        hashlib.sha256(historical_prefix).hexdigest()
        == ARCH_004G2_CLOSURE_THRESHOLD_HISTORICAL_PREFIX_SHA256
    )
    suffix = current_bytes[ARCH_004G2_CLOSURE_THRESHOLD_HISTORICAL_PREFIX_BYTE_COUNT:]
    expected_marker = f"\n{ARCH_004G2_CLOSURE_THRESHOLD_SECTION}:\n".encode()
    assert suffix.startswith(expected_marker), (
        "ARCH-004G2 closure-threshold authority must be appended after the exact observability blob"
    )
    assert current_bytes.count(expected_marker) == 1


def _assert_data_gov_002_phase_b1_historical_prefix_immutable(
    current_bytes: bytes,
    base_blob: bytes,
) -> None:
    assert len(base_blob) == DATA_GOV_002_PHASE_B1_HISTORICAL_PREFIX_BYTE_COUNT
    assert hashlib.sha256(base_blob).hexdigest() == DATA_GOV_002_PHASE_B1_HISTORICAL_PREFIX_SHA256
    historical_prefix = current_bytes[:DATA_GOV_002_PHASE_B1_HISTORICAL_PREFIX_BYTE_COUNT]
    assert historical_prefix == base_blob, (
        "DATA-GOV-002 Phase B1 historical prefix differs from the immutable "
        "ARCH-004G2 closure-threshold blob"
    )
    assert (
        hashlib.sha256(historical_prefix).hexdigest()
        == DATA_GOV_002_PHASE_B1_HISTORICAL_PREFIX_SHA256
    )
    suffix = current_bytes[DATA_GOV_002_PHASE_B1_HISTORICAL_PREFIX_BYTE_COUNT:]
    expected_marker = f"\n{DATA_GOV_002_PHASE_B1_SECTION}:\n".encode()
    assert suffix.startswith(expected_marker), (
        "DATA-GOV-002 Phase B1 authority must be appended after the exact "
        "ARCH-004G2 closure-threshold blob"
    )
    assert current_bytes.count(expected_marker) == 1


def _assert_trading_2458_retirement_historical_prefix_immutable(
    current_bytes: bytes,
    base_blob: bytes,
) -> None:
    assert len(base_blob) == TRADING_2458_RETIREMENT_HISTORICAL_PREFIX_BYTE_COUNT
    assert hashlib.sha256(base_blob).hexdigest() == TRADING_2458_RETIREMENT_HISTORICAL_PREFIX_SHA256
    historical_prefix = current_bytes[:TRADING_2458_RETIREMENT_HISTORICAL_PREFIX_BYTE_COUNT]
    assert historical_prefix == base_blob, (
        "TRADING-2458 retirement historical prefix differs from the immutable "
        "DATA-GOV-002 Phase B1 blob"
    )
    assert (
        hashlib.sha256(historical_prefix).hexdigest()
        == TRADING_2458_RETIREMENT_HISTORICAL_PREFIX_SHA256
    )
    suffix = current_bytes[TRADING_2458_RETIREMENT_HISTORICAL_PREFIX_BYTE_COUNT:]
    expected_marker = f"\n{TRADING_2458_RETIREMENT_SECTION}:\n".encode()
    assert suffix.startswith(expected_marker), (
        "TRADING-2458 retirement must be appended after the exact DATA-GOV-002 Phase B1 blob"
    )
    assert current_bytes.count(expected_marker) == 1


def _assert_trading_2458_closeout_historical_prefix_immutable(
    current_bytes: bytes,
    base_blob: bytes,
) -> None:
    assert len(base_blob) == TRADING_2458_CLOSEOUT_HISTORICAL_PREFIX_BYTE_COUNT
    assert hashlib.sha256(base_blob).hexdigest() == TRADING_2458_CLOSEOUT_HISTORICAL_PREFIX_SHA256
    historical_prefix = current_bytes[:TRADING_2458_CLOSEOUT_HISTORICAL_PREFIX_BYTE_COUNT]
    assert historical_prefix == base_blob, (
        "TRADING-2458 closeout historical prefix differs from the immutable "
        "candidate-family retirement blob"
    )
    assert (
        hashlib.sha256(historical_prefix).hexdigest()
        == TRADING_2458_CLOSEOUT_HISTORICAL_PREFIX_SHA256
    )
    suffix = current_bytes[TRADING_2458_CLOSEOUT_HISTORICAL_PREFIX_BYTE_COUNT:]
    expected_marker = f"\n{TRADING_2458_CLOSEOUT_SECTION}:\n".encode()
    assert suffix.startswith(expected_marker), (
        "TRADING-2458 closeout must be appended after the exact retirement blob"
    )
    assert current_bytes.count(expected_marker) == 1


def _assert_data_gov_002_phase_b2_historical_prefix_immutable(
    current_bytes: bytes,
    base_blob: bytes,
) -> None:
    assert len(base_blob) == DATA_GOV_002_PHASE_B2_HISTORICAL_PREFIX_BYTE_COUNT
    assert hashlib.sha256(base_blob).hexdigest() == DATA_GOV_002_PHASE_B2_HISTORICAL_PREFIX_SHA256
    historical_prefix = current_bytes[:DATA_GOV_002_PHASE_B2_HISTORICAL_PREFIX_BYTE_COUNT]
    assert historical_prefix == base_blob, (
        "DATA-GOV-002 Phase B2 historical prefix differs from the immutable "
        "TRADING-2458 retirement closeout blob"
    )
    assert (
        hashlib.sha256(historical_prefix).hexdigest()
        == DATA_GOV_002_PHASE_B2_HISTORICAL_PREFIX_SHA256
    )
    suffix = current_bytes[DATA_GOV_002_PHASE_B2_HISTORICAL_PREFIX_BYTE_COUNT:]
    expected_marker = f"\n{DATA_GOV_002_PHASE_B2_SECTION}:\n".encode()
    assert suffix.startswith(expected_marker), (
        "DATA-GOV-002 Phase B2 authority must be appended after the exact "
        "TRADING-2458 retirement closeout blob"
    )
    assert current_bytes.count(expected_marker) == 1


def _assert_devx_003_historical_prefix_immutable(
    current_bytes: bytes,
    base_blob: bytes,
) -> None:
    assert len(base_blob) == DEVX_003_HISTORICAL_PREFIX_BYTE_COUNT
    assert hashlib.sha256(base_blob).hexdigest() == DEVX_003_HISTORICAL_PREFIX_SHA256
    historical_prefix = current_bytes[:DEVX_003_HISTORICAL_PREFIX_BYTE_COUNT]
    assert historical_prefix == base_blob, (
        "DEVX-003 historical prefix differs from the immutable DATA-GOV-002 Phase B2 blob"
    )
    assert hashlib.sha256(historical_prefix).hexdigest() == DEVX_003_HISTORICAL_PREFIX_SHA256
    suffix = current_bytes[DEVX_003_HISTORICAL_PREFIX_BYTE_COUNT:]
    expected_marker = f"\n{DEVX_003_SECTION}:\n".encode()
    assert suffix.startswith(expected_marker), (
        "DEVX-003 authority must be appended after the exact DATA-GOV-002 Phase B2 blob"
    )
    assert current_bytes.count(expected_marker) == 1


def _assert_data_gov_002c1_historical_prefix_immutable(
    current_bytes: bytes,
    base_blob: bytes,
) -> None:
    assert len(base_blob) == DATA_GOV_002C1_HISTORICAL_PREFIX_BYTE_COUNT
    assert hashlib.sha256(base_blob).hexdigest() == DATA_GOV_002C1_HISTORICAL_PREFIX_SHA256
    historical_prefix = current_bytes[:DATA_GOV_002C1_HISTORICAL_PREFIX_BYTE_COUNT]
    assert historical_prefix == base_blob, (
        "DATA-GOV-002C1 historical prefix differs from the immutable DEVX-003 blob"
    )
    assert hashlib.sha256(historical_prefix).hexdigest() == DATA_GOV_002C1_HISTORICAL_PREFIX_SHA256
    suffix = current_bytes[DATA_GOV_002C1_HISTORICAL_PREFIX_BYTE_COUNT:]
    expected_marker = f"\n{DATA_GOV_002C1_SECTION}:\n".encode()
    assert suffix.startswith(expected_marker), (
        "DATA-GOV-002C1 authority must be appended after the exact DEVX-003 blob"
    )
    assert current_bytes.count(expected_marker) == 1


def _assert_data_gov_002c2_historical_prefix_immutable(
    current_bytes: bytes,
    base_blob: bytes,
) -> None:
    assert len(base_blob) == DATA_GOV_002C2_HISTORICAL_PREFIX_BYTE_COUNT
    assert hashlib.sha256(base_blob).hexdigest() == DATA_GOV_002C2_HISTORICAL_PREFIX_SHA256
    historical_prefix = current_bytes[:DATA_GOV_002C2_HISTORICAL_PREFIX_BYTE_COUNT]
    assert historical_prefix == base_blob, (
        "DATA-GOV-002C2 historical prefix differs from the immutable C1 blob"
    )
    assert hashlib.sha256(historical_prefix).hexdigest() == DATA_GOV_002C2_HISTORICAL_PREFIX_SHA256
    suffix = current_bytes[DATA_GOV_002C2_HISTORICAL_PREFIX_BYTE_COUNT:]
    expected_marker = f"\n{DATA_GOV_002C2_SECTION}:\n".encode()
    assert suffix.startswith(expected_marker), (
        "DATA-GOV-002C2 authority must be appended after the exact C1 blob"
    )
    assert current_bytes.count(expected_marker) == 1


def _assert_trading_2461_historical_prefix_immutable(
    current_bytes: bytes,
    base_blob: bytes,
) -> None:
    assert len(base_blob) == TRADING_2461_HISTORICAL_PREFIX_BYTE_COUNT
    assert hashlib.sha256(base_blob).hexdigest() == TRADING_2461_HISTORICAL_PREFIX_SHA256
    historical_prefix = current_bytes[:TRADING_2461_HISTORICAL_PREFIX_BYTE_COUNT]
    assert historical_prefix == base_blob, (
        "TRADING-2461 model-ladder historical prefix differs from the immutable DATA-GOV-002C2 blob"
    )
    assert hashlib.sha256(historical_prefix).hexdigest() == TRADING_2461_HISTORICAL_PREFIX_SHA256
    suffix = current_bytes[TRADING_2461_HISTORICAL_PREFIX_BYTE_COUNT:]
    expected_marker = f"\n{TRADING_2461_SECTION}:\n".encode()
    assert suffix.startswith(expected_marker), (
        "TRADING-2461 model-ladder authority must be appended after the exact DATA-GOV-002C2 blob"
    )
    assert current_bytes.count(expected_marker) == 1


def _assert_arch_004g2_paper_weekly_historical_prefix_immutable(
    current_bytes: bytes,
    base_blob: bytes,
) -> None:
    assert len(base_blob) == ARCH_004G2_PAPER_WEEKLY_HISTORICAL_PREFIX_BYTE_COUNT
    assert hashlib.sha256(base_blob).hexdigest() == ARCH_004G2_PAPER_WEEKLY_HISTORICAL_PREFIX_SHA256
    historical_prefix = current_bytes[:ARCH_004G2_PAPER_WEEKLY_HISTORICAL_PREFIX_BYTE_COUNT]
    assert historical_prefix == base_blob, (
        "ARCH-004G2 paper-weekly historical prefix differs from the immutable TRADING-2461 blob"
    )
    assert (
        hashlib.sha256(historical_prefix).hexdigest()
        == ARCH_004G2_PAPER_WEEKLY_HISTORICAL_PREFIX_SHA256
    )
    suffix = current_bytes[ARCH_004G2_PAPER_WEEKLY_HISTORICAL_PREFIX_BYTE_COUNT:]
    expected_marker = f"\n{ARCH_004G2_PAPER_WEEKLY_SECTION}:\n".encode()
    assert suffix.startswith(expected_marker), (
        "paper-weekly authority must be appended after the exact TRADING-2461 blob"
    )
    assert current_bytes.count(expected_marker) == 1


def _assert_ops_069_terminal_archive_historical_prefix_immutable(
    current_bytes: bytes,
    base_blob: bytes,
) -> None:
    assert len(base_blob) == OPS_069_TERMINAL_ARCHIVE_HISTORICAL_PREFIX_BYTE_COUNT
    assert (
        hashlib.sha256(base_blob).hexdigest() == OPS_069_TERMINAL_ARCHIVE_HISTORICAL_PREFIX_SHA256
    )
    historical_prefix = current_bytes[:OPS_069_TERMINAL_ARCHIVE_HISTORICAL_PREFIX_BYTE_COUNT]
    assert historical_prefix == base_blob, (
        "OPS-069 terminal-archive historical prefix differs from the immutable paper-weekly blob"
    )
    assert (
        hashlib.sha256(historical_prefix).hexdigest()
        == OPS_069_TERMINAL_ARCHIVE_HISTORICAL_PREFIX_SHA256
    )
    suffix = current_bytes[OPS_069_TERMINAL_ARCHIVE_HISTORICAL_PREFIX_BYTE_COUNT:]
    expected_marker = f"\n{OPS_069_TERMINAL_ARCHIVE_SECTION}:\n".encode()
    assert suffix.startswith(expected_marker), (
        "OPS-069 terminal-archive authority must be appended after the exact paper-weekly blob"
    )
    assert current_bytes.count(expected_marker) == 1


def _assert_devx_004_historical_prefix_immutable(
    current_bytes: bytes,
    base_blob: bytes,
) -> None:
    assert len(base_blob) == DEVX_004_HISTORICAL_PREFIX_BYTE_COUNT
    assert hashlib.sha256(base_blob).hexdigest() == DEVX_004_HISTORICAL_PREFIX_SHA256
    historical_prefix = current_bytes[:DEVX_004_HISTORICAL_PREFIX_BYTE_COUNT]
    assert historical_prefix == base_blob, (
        "DEVX-004 historical prefix differs from the immutable OPS-069 blob"
    )
    assert hashlib.sha256(historical_prefix).hexdigest() == DEVX_004_HISTORICAL_PREFIX_SHA256
    suffix = current_bytes[DEVX_004_HISTORICAL_PREFIX_BYTE_COUNT:]
    expected_marker = f"\n{DEVX_004_SECTION}:\n".encode()
    assert suffix.startswith(expected_marker)
    assert current_bytes.count(expected_marker) == 1


def _assert_devx_001_reconciliation_historical_prefix_immutable(
    current_bytes: bytes,
    base_blob: bytes,
) -> None:
    assert len(base_blob) == DEVX_001_RECONCILIATION_HISTORICAL_PREFIX_BYTE_COUNT
    assert hashlib.sha256(base_blob).hexdigest() == DEVX_001_RECONCILIATION_HISTORICAL_PREFIX_SHA256
    historical_prefix = current_bytes[:DEVX_001_RECONCILIATION_HISTORICAL_PREFIX_BYTE_COUNT]
    assert historical_prefix == base_blob, (
        "DEVX-001 reconciliation historical prefix differs from immutable DEVX-004 blob"
    )
    suffix = current_bytes[DEVX_001_RECONCILIATION_HISTORICAL_PREFIX_BYTE_COUNT:]
    expected_marker = f"\n{DEVX_001_RECONCILIATION_SECTION}:\n".encode()
    assert suffix.startswith(expected_marker)
    assert current_bytes.count(expected_marker) == 1


def _assert_devx_005_historical_prefix_immutable(
    current_bytes: bytes,
    base_blob: bytes,
) -> None:
    assert len(base_blob) == DEVX_005_HISTORICAL_PREFIX_BYTE_COUNT
    assert hashlib.sha256(base_blob).hexdigest() == DEVX_005_HISTORICAL_PREFIX_SHA256
    historical_prefix = current_bytes[:DEVX_005_HISTORICAL_PREFIX_BYTE_COUNT]
    assert historical_prefix == base_blob, (
        "DEVX-005 historical prefix differs from immutable DEVX-001 blob"
    )
    suffix = current_bytes[DEVX_005_HISTORICAL_PREFIX_BYTE_COUNT:]
    expected_marker = f"\n{DEVX_005_SECTION}:\n".encode()
    assert suffix.startswith(expected_marker)
    assert current_bytes.count(expected_marker) == 1


def _assert_trading_2462_historical_prefix_immutable(
    current_bytes: bytes,
    base_blob: bytes,
) -> None:
    assert len(base_blob) == TRADING_2462_HISTORICAL_PREFIX_BYTE_COUNT
    assert hashlib.sha256(base_blob).hexdigest() == TRADING_2462_HISTORICAL_PREFIX_SHA256
    historical_prefix = current_bytes[:TRADING_2462_HISTORICAL_PREFIX_BYTE_COUNT]
    assert historical_prefix == base_blob, (
        "TRADING-2462 tail-risk robustness historical prefix differs from the "
        "immutable DEVX-005 blob"
    )
    assert hashlib.sha256(historical_prefix).hexdigest() == TRADING_2462_HISTORICAL_PREFIX_SHA256
    suffix = current_bytes[TRADING_2462_HISTORICAL_PREFIX_BYTE_COUNT:]
    expected_marker = f"\n{TRADING_2462_SECTION}:\n".encode()
    assert suffix.startswith(expected_marker), (
        "TRADING-2462 authority must be appended after the exact DEVX-005 blob"
    )
    assert current_bytes.count(expected_marker) == 1


def _assert_devx_006_historical_prefix_immutable(
    current_bytes: bytes,
    base_blob: bytes,
) -> None:
    assert len(base_blob) == DEVX_006_HISTORICAL_PREFIX_BYTE_COUNT
    assert hashlib.sha256(base_blob).hexdigest() == DEVX_006_HISTORICAL_PREFIX_SHA256
    historical_prefix = current_bytes[:DEVX_006_HISTORICAL_PREFIX_BYTE_COUNT]
    assert historical_prefix == base_blob, (
        "DEVX-006 historical prefix differs from immutable TRADING-2462 blob"
    )
    suffix = current_bytes[DEVX_006_HISTORICAL_PREFIX_BYTE_COUNT:]
    expected_marker = f"\n{DEVX_006_SECTION}:\n".encode()
    assert suffix.startswith(expected_marker)
    assert current_bytes.count(expected_marker) == 1


def _assert_arch_005m2_historical_prefix_immutable(
    current_bytes: bytes,
    base_blob: bytes,
) -> None:
    assert len(base_blob) == ARCH_005M2_HISTORICAL_PREFIX_BYTE_COUNT
    assert hashlib.sha256(base_blob).hexdigest() == ARCH_005M2_HISTORICAL_PREFIX_SHA256
    historical_prefix = current_bytes[:ARCH_005M2_HISTORICAL_PREFIX_BYTE_COUNT]
    assert historical_prefix == base_blob, (
        "ARCH-005M2 historical prefix differs from immutable DEVX-006 blob"
    )
    suffix = current_bytes[ARCH_005M2_HISTORICAL_PREFIX_BYTE_COUNT:]
    expected_marker = f"\n{ARCH_005M2_SECTION}:\n".encode()
    assert suffix.startswith(expected_marker)
    assert current_bytes.count(expected_marker) == 1


def _assert_arch_005m3_historical_prefix_immutable(
    current_bytes: bytes,
    base_blob: bytes,
) -> None:
    assert len(base_blob) == ARCH_005M3_HISTORICAL_PREFIX_BYTE_COUNT
    assert hashlib.sha256(base_blob).hexdigest() == ARCH_005M3_HISTORICAL_PREFIX_SHA256
    historical_prefix = current_bytes[:ARCH_005M3_HISTORICAL_PREFIX_BYTE_COUNT]
    assert historical_prefix == base_blob, (
        "ARCH-005M3 historical prefix differs from immutable ARCH-005M2 blob"
    )
    suffix = current_bytes[ARCH_005M3_HISTORICAL_PREFIX_BYTE_COUNT:]
    expected_marker = f"\n{ARCH_005M3_SECTION}:\n".encode()
    assert suffix.startswith(expected_marker)
    assert current_bytes.count(expected_marker) == 1


def _assert_arch_005m1_batch2_historical_prefix_immutable(
    current_bytes: bytes,
    base_blob: bytes,
) -> None:
    assert len(base_blob) == ARCH_005M1_BATCH2_HISTORICAL_PREFIX_BYTE_COUNT
    assert (
        hashlib.sha256(base_blob).hexdigest()
        == ARCH_005M1_BATCH2_HISTORICAL_PREFIX_SHA256
    )
    historical_prefix = current_bytes[:ARCH_005M1_BATCH2_HISTORICAL_PREFIX_BYTE_COUNT]
    assert historical_prefix == base_blob, (
        "ARCH-005M1 Batch 2 historical prefix differs from immutable ARCH-005M3 blob"
    )
    suffix = current_bytes[ARCH_005M1_BATCH2_HISTORICAL_PREFIX_BYTE_COUNT:]
    expected_marker = f"\n{ARCH_005M1_BATCH2_SECTION}:\n".encode()
    assert suffix.startswith(expected_marker)
    assert current_bytes.count(expected_marker) == 1


def _assert_arch_005m1_batch3_historical_prefix_immutable(
    current_bytes: bytes,
    base_blob: bytes,
) -> None:
    assert len(base_blob) == ARCH_005M1_BATCH3_HISTORICAL_PREFIX_BYTE_COUNT
    assert (
        hashlib.sha256(base_blob).hexdigest()
        == ARCH_005M1_BATCH3_HISTORICAL_PREFIX_SHA256
    )
    historical_prefix = current_bytes[:ARCH_005M1_BATCH3_HISTORICAL_PREFIX_BYTE_COUNT]
    assert historical_prefix == base_blob, (
        "ARCH-005M1 Batch 3 historical prefix differs from immutable Batch 2 blob"
    )
    suffix = current_bytes[ARCH_005M1_BATCH3_HISTORICAL_PREFIX_BYTE_COUNT:]
    expected_marker = f"\n{ARCH_005M1_BATCH3_SECTION}:\n".encode()
    assert suffix.startswith(expected_marker)
    assert current_bytes.count(expected_marker) == 1


def _assert_arch_005m1_batch4_historical_prefix_immutable(
    current_bytes: bytes,
    base_blob: bytes,
) -> None:
    assert len(base_blob) == ARCH_005M1_BATCH4_HISTORICAL_PREFIX_BYTE_COUNT
    assert (
        hashlib.sha256(base_blob).hexdigest()
        == ARCH_005M1_BATCH4_HISTORICAL_PREFIX_SHA256
    )
    historical_prefix = current_bytes[:ARCH_005M1_BATCH4_HISTORICAL_PREFIX_BYTE_COUNT]
    assert historical_prefix == base_blob, (
        "ARCH-005M1 Batch 4 historical prefix differs from immutable Batch 3 blob"
    )
    suffix = current_bytes[ARCH_005M1_BATCH4_HISTORICAL_PREFIX_BYTE_COUNT:]
    expected_marker = f"\n{ARCH_005M1_BATCH4_SECTION}:\n".encode()
    assert suffix.startswith(expected_marker)
    assert current_bytes.count(expected_marker) == 1


def _assert_ops_070_stable_release_historical_prefix_immutable(
    current_bytes: bytes,
    base_blob: bytes,
) -> None:
    assert len(base_blob) == OPS_070_STABLE_RELEASE_HISTORICAL_PREFIX_BYTE_COUNT
    assert (
        hashlib.sha256(base_blob).hexdigest()
        == OPS_070_STABLE_RELEASE_HISTORICAL_PREFIX_SHA256
    )
    historical_prefix = current_bytes[
        :OPS_070_STABLE_RELEASE_HISTORICAL_PREFIX_BYTE_COUNT
    ]
    assert historical_prefix == base_blob, (
        "OPS-070 stable release historical prefix differs from immutable "
        "ARCH-005M1 Batch 4 blob"
    )
    suffix = current_bytes[OPS_070_STABLE_RELEASE_HISTORICAL_PREFIX_BYTE_COUNT:]
    expected_marker = f"\n{OPS_070_STABLE_RELEASE_SECTION}:\n".encode()
    assert suffix.startswith(expected_marker)
    assert current_bytes.count(expected_marker) == 1


def _assert_ops_070_runtime_exclude_historical_prefix_immutable(
    current_bytes: bytes,
    base_blob: bytes,
) -> None:
    assert len(base_blob) == OPS_070_RUNTIME_EXCLUDE_HISTORICAL_PREFIX_BYTE_COUNT
    assert (
        hashlib.sha256(base_blob).hexdigest()
        == OPS_070_RUNTIME_EXCLUDE_HISTORICAL_PREFIX_SHA256
    )
    historical_prefix = current_bytes[
        :OPS_070_RUNTIME_EXCLUDE_HISTORICAL_PREFIX_BYTE_COUNT
    ]
    assert historical_prefix == base_blob, (
        "OPS-070 runtime Git exclusion historical prefix differs from immutable "
        "stable deployment release blob"
    )
    suffix = current_bytes[OPS_070_RUNTIME_EXCLUDE_HISTORICAL_PREFIX_BYTE_COUNT:]
    expected_marker = f"\n{OPS_070_RUNTIME_EXCLUDE_SECTION}:\n".encode()
    assert suffix.startswith(expected_marker)
    assert current_bytes.count(expected_marker) == 1


def _assert_ops_070_cross_release_policy_historical_prefix_immutable(
    current_bytes: bytes,
    base_blob: bytes,
) -> None:
    assert (
        len(base_blob)
        == OPS_070_CROSS_RELEASE_POLICY_HISTORICAL_PREFIX_BYTE_COUNT
    )
    assert (
        hashlib.sha256(base_blob).hexdigest()
        == OPS_070_CROSS_RELEASE_POLICY_HISTORICAL_PREFIX_SHA256
    )
    historical_prefix = current_bytes[
        :OPS_070_CROSS_RELEASE_POLICY_HISTORICAL_PREFIX_BYTE_COUNT
    ]
    assert historical_prefix == base_blob, (
        "OPS-070 cross-release policy historical prefix differs from immutable "
        "runtime Git exclusion contract blob"
    )
    suffix = current_bytes[
        OPS_070_CROSS_RELEASE_POLICY_HISTORICAL_PREFIX_BYTE_COUNT:
    ]
    expected_marker = f"\n{OPS_070_CROSS_RELEASE_POLICY_SECTION}:\n".encode()
    assert suffix.startswith(expected_marker)
    assert current_bytes.count(expected_marker) == 1


def _wave11_portable_artifact_identity(attempt: dict[str, Any]) -> tuple[str, str]:
    artifact = attempt.get("artifact")
    assert isinstance(artifact, dict), "executed attempt requires portable artifact evidence"
    artifact_path = artifact.get("path")
    artifact_sha256 = artifact.get("sha256")
    artifact_size = artifact.get("size_bytes")
    assert isinstance(artifact_path, str) and artifact_path.strip(), (
        "executed attempt artifact path must be non-empty"
    )
    assert (
        isinstance(artifact_sha256, str)
        and len(artifact_sha256) == 64
        and all(character in "0123456789abcdef" for character in artifact_sha256)
    ), "executed attempt artifact SHA256 must be lowercase 64-hex"
    assert type(artifact_size) is int and artifact_size > 0, (
        "executed attempt artifact size_bytes must be a positive integer"
    )
    return artifact_path, artifact_sha256


def _assert_wave11_full_attempt_chain(attempts: list[dict[str, Any]]) -> None:
    assert len(attempts) >= 2
    attempt_ids = [attempt.get("attempt_id") for attempt in attempts]
    assert all(isinstance(attempt_id, str) and attempt_id for attempt_id in attempt_ids)
    assert len(attempt_ids) == len(set(attempt_ids)), "Full attempt ids must be unique"
    assert attempts[0]["role"] == "INITIAL_FORMAL_GATE", (
        "first Full attempt must remain the initial formal gate"
    )
    assert "replaces_attempt_id" not in attempts[0], (
        "initial Full attempt cannot replace another attempt"
    )

    artifact_paths: set[str] = set()
    artifact_hashes: set[str] = set()
    for index, attempt in enumerate(attempts):
        is_latest = index == len(attempts) - 1
        if index > 0:
            previous = attempts[index - 1]
            assert attempt["role"] == "FAILURE_FIX_REPLACEMENT"
            assert previous["status"] == "FAIL", (
                "replacement must immediately follow a failed attempt"
            )
            assert attempt.get("replaces_attempt_id") == previous["attempt_id"], (
                "replacement must identify the immediately preceding failed attempt"
            )

        status = attempt["status"]
        if is_latest:
            assert status in {"PENDING", "PASS"}, "latest Full attempt must be PENDING or PASS"
        else:
            assert status == "FAIL", "every intermediate Full attempt must be FAIL"

        if status == "PENDING":
            assert attempt.get("required") is True
            assert "artifact" not in attempt
            continue

        assert status in {"FAIL", "PASS"}
        if status == "FAIL":
            assert attempt["failed"] > 0
        else:
            assert attempt["failed"] == 0
        artifact_path, artifact_sha256 = _wave11_portable_artifact_identity(attempt)
        assert artifact_path not in artifact_paths, (
            "executed Full attempt artifact paths must be unique"
        )
        assert artifact_sha256 not in artifact_hashes, (
            "executed Full attempt artifact SHA256 values must be unique"
        )
        artifact_paths.add(artifact_path)
        artifact_hashes.add(artifact_sha256)


def _assert_portable_repository_relative_path(path: object) -> str:
    assert isinstance(path, str) and path, "path must be a non-empty string"
    assert "\\" not in path, "path must use portable POSIX separators"
    assert "\n" not in path and "\r" not in path, "path must be one physical line"
    assert not path.startswith("/"), "path must be repository-relative"
    assert not re.match(r"^[A-Za-z]:", path), "path must not be drive-qualified"
    assert not any(token in path for token in ("*", "?", "[")), "path must be exact, not a glob"
    parts = path.split("/")
    assert all(part not in {"", ".", ".."} for part in parts), (
        "path must be normalized and cannot escape the repository"
    )
    return path


def _wave14_s2_portable_full_artifact(
    attempt: dict[str, Any],
) -> tuple[str, str]:
    artifact = attempt.get("artifact")
    assert isinstance(artifact, dict), "executed Full attempt requires artifact evidence"
    assert set(artifact) == {
        "path",
        "sha256",
        "size_bytes",
        "passed",
        "failed",
        "skipped",
    }
    artifact_path = _assert_portable_repository_relative_path(artifact["path"])
    assert artifact_path.startswith("outputs/validation_runtime/full_")
    assert artifact_path.endswith("/test_runtime_summary.json")
    artifact_sha256 = artifact["sha256"]
    assert isinstance(artifact_sha256, str)
    assert re.fullmatch(r"[0-9a-f]{64}", artifact_sha256)
    assert type(artifact["size_bytes"]) is int and artifact["size_bytes"] > 0
    assert type(artifact["passed"]) is int and artifact["passed"] > 0
    assert type(artifact["failed"]) is int and artifact["failed"] >= 0
    assert type(artifact["skipped"]) is int and artifact["skipped"] >= 0
    return artifact_path, artifact_sha256


def _assert_wave14_s2_full_attempt_chain(
    full_validation: dict[str, Any],
) -> list[dict[str, Any]]:
    assert full_validation["required"] is True
    assert full_validation["attempts_append_only"] is True
    assert full_validation["executed_attempts_may_be_removed_or_overwritten"] is False
    assert full_validation["post_pass_repeat_full_allowed"] is False
    run_count = full_validation["run_count"]
    assert type(run_count) is int and run_count >= 0

    attempts = full_validation["attempts"]
    assert isinstance(attempts, list) and attempts
    attempt_ids = [attempt.get("attempt_id") for attempt in attempts]
    assert attempt_ids == [
        f"wave14_s2_full_{ordinal}" for ordinal in range(1, len(attempts) + 1)
    ], "Full attempt ids must preserve their append-only ordinal chain"

    artifact_paths: set[str] = set()
    artifact_hashes: set[str] = set()
    executed_count = 0
    for index, attempt in enumerate(attempts):
        assert isinstance(attempt, dict)
        is_latest = index == len(attempts) - 1
        assert attempt["required"] is True
        if index == 0:
            assert attempt["role"] == "INITIAL_FORMAL_GATE"
            assert "replaces_attempt_id" not in attempt
        else:
            previous = attempts[index - 1]
            assert attempt["role"] == "FAILURE_FIX_REPLACEMENT"
            assert previous["status"] == "FAIL", (
                "a replacement attempt may only follow the immediately preceding failure"
            )
            assert attempt.get("replaces_attempt_id") == previous["attempt_id"]

        status = attempt["status"]
        if is_latest:
            assert status in {"PENDING", "PASS"}
        else:
            assert status == "FAIL", "all non-latest Full attempts must remain failures"

        if status == "PENDING":
            assert not {
                "artifact",
                "tested_commit",
                "tested_tree",
                "tested_section_status",
                "full_sensitive_source_manifest_sha256",
            }.intersection(attempt)
            continue

        executed_count += 1
        assert status in {"FAIL", "PASS"}
        artifact_path, artifact_sha256 = _wave14_s2_portable_full_artifact(attempt)
        if status == "PASS":
            assert attempt["artifact"]["failed"] == 0
        else:
            assert attempt["artifact"]["failed"] > 0
        assert artifact_path not in artifact_paths
        assert artifact_sha256 not in artifact_hashes
        artifact_paths.add(artifact_path)
        artifact_hashes.add(artifact_sha256)

        tested_commit = attempt.get("tested_commit")
        tested_tree = attempt.get("tested_tree")
        assert attempt.get("tested_section_status") == "VALIDATING_WAVE14_S2"
        source_manifest_sha256 = attempt.get("full_sensitive_source_manifest_sha256")
        assert isinstance(tested_commit, str) and re.fullmatch(r"[0-9a-f]{40}", tested_commit)
        assert isinstance(tested_tree, str) and re.fullmatch(r"[0-9a-f]{40}", tested_tree)
        assert isinstance(source_manifest_sha256, str) and re.fullmatch(
            r"[0-9a-f]{64}", source_manifest_sha256
        )

    assert run_count == executed_count, "run_count must equal executed, non-PENDING attempts"
    latest_status = attempts[-1]["status"]
    if latest_status == "PENDING":
        assert full_validation["status"] == "PENDING"
    elif executed_count == 1:
        assert full_validation["status"] == "PASS"
    else:
        assert full_validation["status"] == "PASS_AFTER_FAILURE_FIX"
    return attempts


@cache
def _assert_current_wave11_historical_prefix_immutable() -> None:
    _assert_wave11_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _wave11_base_baseline_blob(),
    )


@cache
def _assert_current_docs_gov_historical_prefix_immutable() -> None:
    _assert_docs_gov_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _docs_gov_base_baseline_blob(),
    )


@cache
def _assert_current_wave12_historical_prefix_immutable() -> None:
    _assert_wave12_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _wave12_base_baseline_blob(),
    )


@cache
def _assert_current_wave13_historical_prefix_immutable() -> None:
    _assert_wave13_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _wave13_base_baseline_blob(),
    )


@cache
def _assert_current_wave14_s0_1_historical_prefix_immutable() -> None:
    _assert_wave14_s0_1_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _wave14_s0_1_base_baseline_blob(),
    )


@cache
def _assert_current_wave14_s2_historical_prefix_immutable() -> None:
    _assert_wave14_s2_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _wave14_s2_base_baseline_blob(),
    )


@cache
def _assert_current_ops_067_historical_prefix_immutable() -> None:
    _assert_ops_067_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _ops_067_base_baseline_blob(),
    )


@cache
def _assert_current_ops_068_historical_prefix_immutable() -> None:
    _assert_ops_068_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _ops_068_base_baseline_blob(),
    )


@cache
def _assert_current_arch_005s4d_historical_prefix_immutable() -> None:
    _assert_arch_005s4d_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _arch_005s4d_base_baseline_blob(),
    )


@cache
def _assert_current_wave15_historical_prefix_immutable() -> None:
    _assert_wave15_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _wave15_base_baseline_blob(),
    )


@cache
def _assert_current_d0b2b_historical_prefix_immutable() -> None:
    _assert_d0b2b_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _d0b2b_base_baseline_blob(),
    )


@cache
def _compatibility_baseline() -> dict[str, Any]:
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    assert isinstance(baseline, dict)
    return baseline


@cache
def _wave11_superseded_live_source_paths() -> frozenset[str]:
    _assert_current_wave11_historical_prefix_immutable()
    baseline = _compatibility_baseline()
    wave11 = baseline[WAVE11_SECTION]
    paths = wave11["superseded_live_source_paths"]
    assert isinstance(paths, list)
    return frozenset(str(path) for path in paths)


@cache
def _docs_gov_superseded_live_source_paths() -> frozenset[str]:
    _assert_current_docs_gov_historical_prefix_immutable()
    baseline = _compatibility_baseline()
    docs_gov = baseline[DOCS_GOV_SECTION]
    paths = docs_gov["superseded_live_source_paths"]
    assert isinstance(paths, list)
    return frozenset(str(path) for path in paths)


@cache
def _wave12_superseded_live_source_paths() -> frozenset[str]:
    _assert_current_wave12_historical_prefix_immutable()
    baseline = _compatibility_baseline()
    wave12 = baseline[WAVE12_SECTION]
    paths = wave12["superseded_live_source_paths"]
    assert isinstance(paths, list)
    return frozenset(str(path) for path in paths)


@cache
def _wave13_superseded_live_source_paths() -> frozenset[str]:
    _assert_current_wave13_historical_prefix_immutable()
    baseline = _compatibility_baseline()
    wave13 = baseline[WAVE13_SECTION]
    paths = wave13["superseded_live_source_paths"]
    assert isinstance(paths, list)
    return frozenset(str(path) for path in paths)


@cache
def _wave14_s0_1_superseded_live_source_paths() -> frozenset[str]:
    _assert_current_wave14_s0_1_historical_prefix_immutable()
    baseline = _compatibility_baseline()
    wave14 = baseline[WAVE14_S0_1_SECTION]
    paths = wave14["superseded_live_source_paths"]
    assert isinstance(paths, list)
    return frozenset(str(path) for path in paths)


@cache
def _wave14_s2_superseded_live_source_paths() -> frozenset[str]:
    _assert_current_wave14_s2_historical_prefix_immutable()
    baseline = _compatibility_baseline()
    wave14 = baseline[WAVE14_S2_SECTION]
    paths = wave14["superseded_live_source_paths"]
    assert isinstance(paths, list)
    return frozenset(str(path) for path in paths)


@cache
def _ops_067_superseded_live_source_paths() -> frozenset[str]:
    _assert_current_ops_067_historical_prefix_immutable()
    baseline = _compatibility_baseline()
    ops_067 = baseline[OPS_067_SECTION]
    paths = ops_067["superseded_live_source_paths"]
    assert isinstance(paths, list)
    return frozenset(str(path) for path in paths)


@cache
def _ops_068_superseded_live_source_paths() -> frozenset[str]:
    _assert_current_ops_068_historical_prefix_immutable()
    baseline = _compatibility_baseline()
    ops_068 = baseline[OPS_068_SECTION]
    paths = ops_068["superseded_live_source_paths"]
    assert isinstance(paths, list)
    return frozenset(str(path) for path in paths)


@cache
def _arch_005s4d_superseded_live_source_paths() -> frozenset[str]:
    _assert_current_arch_005s4d_historical_prefix_immutable()
    baseline = _compatibility_baseline()
    phase = baseline[ARCH_005S4D_SECTION]
    paths = phase["superseded_live_source_paths"]
    assert isinstance(paths, list)
    return frozenset(str(path) for path in paths)


@cache
def _wave15_superseded_live_source_paths() -> frozenset[str]:
    _assert_current_wave15_historical_prefix_immutable()
    baseline = _compatibility_baseline()
    phase = baseline[WAVE15_SECTION]
    paths = phase["superseded_live_source_paths"]
    assert isinstance(paths, list)
    return frozenset(str(path) for path in paths)


@cache
def _d0b2b_superseded_live_source_paths() -> frozenset[str]:
    _assert_current_d0b2b_historical_prefix_immutable()
    baseline = _compatibility_baseline()
    phase = baseline[D0B2B_SECTION]
    paths = phase["superseded_live_source_paths"]
    assert isinstance(paths, list)
    return frozenset(str(path) for path in paths)


@cache
def _ops_069_superseded_live_source_paths() -> frozenset[str]:
    _assert_ops_069_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _ops_069_base_baseline_blob(),
    )
    baseline = _compatibility_baseline()
    phase = baseline[OPS_069_SECTION]
    paths = phase["superseded_live_source_paths"]
    assert isinstance(paths, list)
    return frozenset(str(path) for path in paths)


@cache
def _ops_070_superseded_live_source_paths() -> frozenset[str]:
    _assert_ops_070_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _ops_070_base_baseline_blob(),
    )
    baseline = _compatibility_baseline()
    phase = baseline[OPS_070_SECTION]
    paths = phase["superseded_live_source_paths"]
    assert isinstance(paths, list)
    return frozenset(str(path) for path in paths)


@cache
def _arch_005s4e_superseded_live_source_paths() -> frozenset[str]:
    _assert_arch_005s4e_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _arch_005s4e_base_baseline_blob(),
    )
    baseline = _compatibility_baseline()
    phase = baseline[ARCH_005S4E_SECTION]
    paths = phase["superseded_live_source_paths"]
    assert isinstance(paths, list)
    return frozenset(str(path) for path in paths)


@cache
def _arch_005s4d_s2_superseded_live_source_paths() -> frozenset[str]:
    _assert_arch_005s4d_s2_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _arch_005s4d_s2_base_baseline_blob(),
    )
    baseline = _compatibility_baseline()
    phase = baseline[ARCH_005S4D_S2_SECTION]
    paths = phase["superseded_live_source_paths"]
    assert isinstance(paths, list)
    return frozenset(str(path) for path in paths)


@cache
def _trading_2458_2460_integration_superseded_live_source_paths() -> frozenset[str]:
    _assert_trading_2458_2460_integration_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _trading_2458_2460_integration_base_baseline_blob(),
    )
    baseline = _compatibility_baseline()
    phase = baseline[TRADING_2458_2460_INTEGRATION_SECTION]
    paths = phase["superseded_live_source_paths"]
    assert isinstance(paths, list)
    return frozenset(str(path) for path in paths)


@cache
def _trading_2458_2460_integration_source_paths() -> frozenset[str]:
    baseline = _compatibility_baseline()
    phase = baseline[TRADING_2458_2460_INTEGRATION_SECTION]
    sources = phase["sources"]
    assert isinstance(sources, list)
    return frozenset(str(source["path"]) for source in sources)


@cache
def _devx_trading_cleanup_superseded_live_source_paths() -> frozenset[str]:
    _assert_devx_trading_cleanup_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _devx_trading_cleanup_base_baseline_blob(),
    )
    baseline = _compatibility_baseline()
    phase = baseline[DEVX_TRADING_CLEANUP_SECTION]
    paths = phase["superseded_live_source_paths"]
    assert isinstance(paths, list)
    return frozenset(str(path) for path in paths)


@cache
def _devx_trading_cleanup_source_paths() -> frozenset[str]:
    baseline = _compatibility_baseline()
    phase = baseline[DEVX_TRADING_CLEANUP_SECTION]
    sources = phase["sources"]
    assert isinstance(sources, list)
    return frozenset(str(source["path"]) for source in sources)


@cache
def _trading_2459_doc_closeout_superseded_live_source_paths() -> frozenset[str]:
    _assert_trading_2459_doc_closeout_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _trading_2459_doc_closeout_base_baseline_blob(),
    )
    baseline = _compatibility_baseline()
    phase = baseline[TRADING_2459_DOC_CLOSEOUT_SECTION]
    paths = phase["superseded_live_source_paths"]
    assert isinstance(paths, list)
    return frozenset(str(path) for path in paths)


@cache
def _trading_2459_doc_closeout_source_paths() -> frozenset[str]:
    baseline = _compatibility_baseline()
    phase = baseline[TRADING_2459_DOC_CLOSEOUT_SECTION]
    sources = phase["sources"]
    assert isinstance(sources, list)
    return frozenset(str(source["path"]) for source in sources)


@cache
def _data_gov_002_superseded_live_source_paths() -> frozenset[str]:
    _assert_data_gov_002_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _data_gov_002_base_baseline_blob(),
    )
    baseline = _compatibility_baseline()
    phase = baseline[DATA_GOV_002_SECTION]
    paths = phase["superseded_live_source_paths"]
    assert isinstance(paths, list)
    return frozenset(str(path) for path in paths)


@cache
def _data_gov_002_source_paths() -> frozenset[str]:
    baseline = _compatibility_baseline()
    phase = baseline[DATA_GOV_002_SECTION]
    sources = phase["sources"]
    assert isinstance(sources, list)
    return frozenset(str(source["path"]) for source in sources)


@cache
def _devx_002_superseded_live_source_paths() -> frozenset[str]:
    _assert_devx_002_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _devx_002_base_baseline_blob(),
    )
    baseline = _compatibility_baseline()
    phase = baseline[DEVX_002_SECTION]
    paths = phase["superseded_live_source_paths"]
    assert isinstance(paths, list)
    return frozenset(str(path) for path in paths)


@cache
def _devx_002_source_paths() -> frozenset[str]:
    baseline = _compatibility_baseline()
    phase = baseline[DEVX_002_SECTION]
    sources = phase["sources"]
    assert isinstance(sources, list)
    return frozenset(str(source["path"]) for source in sources)


@cache
def _devx_002_push_v2_superseded_live_source_paths() -> frozenset[str]:
    _assert_devx_002_push_v2_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _devx_002_push_v2_base_baseline_blob(),
    )
    baseline = _compatibility_baseline()
    phase = baseline[DEVX_002_PUSH_V2_SECTION]
    paths = phase["superseded_live_source_paths"]
    assert isinstance(paths, list)
    return frozenset(str(path) for path in paths)


@cache
def _devx_002_push_v2_source_paths() -> frozenset[str]:
    baseline = _compatibility_baseline()
    phase = baseline[DEVX_002_PUSH_V2_SECTION]
    sources = phase["sources"]
    assert isinstance(sources, list)
    return frozenset(str(source["path"]) for source in sources)


@cache
def _arch_004g2_observability_superseded_live_source_paths() -> frozenset[str]:
    _assert_arch_004g2_observability_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _arch_004g2_observability_base_baseline_blob(),
    )
    baseline = _compatibility_baseline()
    phase = baseline[ARCH_004G2_OBSERVABILITY_SECTION]
    paths = phase["superseded_live_source_paths"]
    assert isinstance(paths, list)
    return frozenset(str(path) for path in paths)


@cache
def _arch_004g2_observability_source_paths() -> frozenset[str]:
    baseline = _compatibility_baseline()
    phase = baseline[ARCH_004G2_OBSERVABILITY_SECTION]
    sources = phase["sources"]
    assert isinstance(sources, list)
    return frozenset(str(source["path"]) for source in sources)


@cache
def _arch_004g2_closure_threshold_superseded_live_source_paths() -> frozenset[str]:
    _assert_arch_004g2_closure_threshold_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _arch_004g2_closure_threshold_base_baseline_blob(),
    )
    baseline = _compatibility_baseline()
    phase = baseline[ARCH_004G2_CLOSURE_THRESHOLD_SECTION]
    paths = phase["superseded_live_source_paths"]
    assert isinstance(paths, list)
    return frozenset(str(path) for path in paths)


@cache
def _arch_004g2_closure_threshold_source_paths() -> frozenset[str]:
    baseline = _compatibility_baseline()
    phase = baseline[ARCH_004G2_CLOSURE_THRESHOLD_SECTION]
    sources = phase["sources"]
    assert isinstance(sources, list)
    return frozenset(str(source["path"]) for source in sources)


@cache
def _data_gov_002_phase_b1_superseded_live_source_paths() -> frozenset[str]:
    _assert_data_gov_002_phase_b1_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _data_gov_002_phase_b1_base_baseline_blob(),
    )
    baseline = _compatibility_baseline()
    phase = baseline[DATA_GOV_002_PHASE_B1_SECTION]
    paths = phase["superseded_live_source_paths"]
    assert isinstance(paths, list)
    return frozenset(str(path) for path in paths)


@cache
def _data_gov_002_phase_b1_source_paths() -> frozenset[str]:
    baseline = _compatibility_baseline()
    phase = baseline[DATA_GOV_002_PHASE_B1_SECTION]
    sources = phase["sources"]
    assert isinstance(sources, list)
    return frozenset(str(source["path"]) for source in sources)


@cache
def _trading_2458_retirement_superseded_live_source_paths() -> frozenset[str]:
    _assert_trading_2458_retirement_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _trading_2458_retirement_base_baseline_blob(),
    )
    baseline = _compatibility_baseline()
    phase = baseline[TRADING_2458_RETIREMENT_SECTION]
    paths = phase["superseded_live_source_paths"]
    assert isinstance(paths, list)
    return frozenset(str(path) for path in paths)


@cache
def _trading_2458_retirement_source_paths() -> frozenset[str]:
    baseline = _compatibility_baseline()
    phase = baseline[TRADING_2458_RETIREMENT_SECTION]
    sources = phase["sources"]
    assert isinstance(sources, list)
    return frozenset(str(source["path"]) for source in sources)


@cache
def _trading_2458_closeout_superseded_live_source_paths() -> frozenset[str]:
    _assert_trading_2458_closeout_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _trading_2458_closeout_base_baseline_blob(),
    )
    baseline = _compatibility_baseline()
    phase = baseline[TRADING_2458_CLOSEOUT_SECTION]
    paths = phase["superseded_live_source_paths"]
    assert isinstance(paths, list)
    return frozenset(str(path) for path in paths)


@cache
def _trading_2458_closeout_source_paths() -> frozenset[str]:
    baseline = _compatibility_baseline()
    phase = baseline[TRADING_2458_CLOSEOUT_SECTION]
    sources = phase["sources"]
    assert isinstance(sources, list)
    return frozenset(str(source["path"]) for source in sources)


@cache
def _data_gov_002_phase_b2_superseded_live_source_paths() -> frozenset[str]:
    _assert_data_gov_002_phase_b2_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _data_gov_002_phase_b2_base_baseline_blob(),
    )
    baseline = _compatibility_baseline()
    phase = baseline[DATA_GOV_002_PHASE_B2_SECTION]
    paths = phase["superseded_live_source_paths"]
    assert isinstance(paths, list)
    return frozenset(str(path) for path in paths)


@cache
def _data_gov_002_phase_b2_source_paths() -> frozenset[str]:
    baseline = _compatibility_baseline()
    phase = baseline[DATA_GOV_002_PHASE_B2_SECTION]
    sources = phase["sources"]
    assert isinstance(sources, list)
    return frozenset(str(source["path"]) for source in sources)


@cache
def _devx_003_superseded_live_source_paths() -> frozenset[str]:
    _assert_devx_003_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _devx_003_base_baseline_blob(),
    )
    baseline = _compatibility_baseline()
    phase = baseline[DEVX_003_SECTION]
    paths = phase["superseded_live_source_paths"]
    assert isinstance(paths, list)
    return frozenset(str(path) for path in paths)


@cache
def _devx_003_source_paths() -> frozenset[str]:
    baseline = _compatibility_baseline()
    phase = baseline[DEVX_003_SECTION]
    sources = phase["sources"]
    assert isinstance(sources, list)
    return frozenset(str(source["path"]) for source in sources)


@cache
def _data_gov_002c1_superseded_live_source_paths() -> frozenset[str]:
    _assert_data_gov_002c1_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _data_gov_002c1_base_baseline_blob(),
    )
    baseline = _compatibility_baseline()
    phase = baseline[DATA_GOV_002C1_SECTION]
    paths = phase["superseded_live_source_paths"]
    assert isinstance(paths, list)
    return frozenset(str(path) for path in paths)


@cache
def _data_gov_002c1_source_paths() -> frozenset[str]:
    baseline = _compatibility_baseline()
    phase = baseline[DATA_GOV_002C1_SECTION]
    sources = phase["sources"]
    assert isinstance(sources, list)
    return frozenset(str(source["path"]) for source in sources)


@cache
def _data_gov_002c2_superseded_live_source_paths() -> frozenset[str]:
    _assert_data_gov_002c2_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _data_gov_002c2_base_baseline_blob(),
    )
    baseline = _compatibility_baseline()
    phase = baseline[DATA_GOV_002C2_SECTION]
    paths = phase["superseded_live_source_paths"]
    assert isinstance(paths, list)
    return frozenset(str(path) for path in paths)


@cache
def _data_gov_002c2_source_paths() -> frozenset[str]:
    baseline = _compatibility_baseline()
    phase = baseline[DATA_GOV_002C2_SECTION]
    sources = phase["sources"]
    assert isinstance(sources, list)
    return frozenset(str(source["path"]) for source in sources)


@cache
def _trading_2461_superseded_live_source_paths() -> frozenset[str]:
    _assert_trading_2461_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _trading_2461_base_baseline_blob(),
    )
    baseline = _compatibility_baseline()
    phase = baseline[TRADING_2461_SECTION]
    paths = phase["superseded_live_source_paths"]
    assert isinstance(paths, list)
    return frozenset(str(path) for path in paths)


@cache
def _trading_2461_source_paths() -> frozenset[str]:
    baseline = _compatibility_baseline()
    phase = baseline[TRADING_2461_SECTION]
    sources = phase["sources"]
    assert isinstance(sources, list)
    return frozenset(str(source["path"]) for source in sources)


@cache
def _arch_004g2_paper_weekly_superseded_live_source_paths() -> frozenset[str]:
    _assert_arch_004g2_paper_weekly_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _arch_004g2_paper_weekly_base_baseline_blob(),
    )
    baseline = _compatibility_baseline()
    phase = baseline[ARCH_004G2_PAPER_WEEKLY_SECTION]
    paths = phase["superseded_live_source_paths"]
    assert isinstance(paths, list)
    return frozenset(str(path) for path in paths)


@cache
def _arch_004g2_paper_weekly_source_paths() -> frozenset[str]:
    baseline = _compatibility_baseline()
    phase = baseline[ARCH_004G2_PAPER_WEEKLY_SECTION]
    sources = phase["sources"]
    assert isinstance(sources, list)
    return frozenset(str(source["path"]) for source in sources)


@cache
def _ops_069_terminal_archive_superseded_live_source_paths() -> frozenset[str]:
    _assert_ops_069_terminal_archive_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _ops_069_terminal_archive_base_baseline_blob(),
    )
    baseline = _compatibility_baseline()
    phase = baseline[OPS_069_TERMINAL_ARCHIVE_SECTION]
    paths = phase["superseded_live_source_paths"]
    assert isinstance(paths, list)
    return frozenset(str(path) for path in paths)


@cache
def _ops_069_terminal_archive_source_paths() -> frozenset[str]:
    baseline = _compatibility_baseline()
    phase = baseline[OPS_069_TERMINAL_ARCHIVE_SECTION]
    sources = phase["sources"]
    assert isinstance(sources, list)
    return frozenset(str(source["path"]) for source in sources)


@cache
def _devx_004_superseded_live_source_paths() -> frozenset[str]:
    _assert_devx_004_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _devx_004_base_baseline_blob(),
    )
    paths = _compatibility_baseline()[DEVX_004_SECTION]["superseded_live_source_paths"]
    assert isinstance(paths, list)
    return frozenset(str(path) for path in paths)


@cache
def _devx_004_source_paths() -> frozenset[str]:
    sources = _compatibility_baseline()[DEVX_004_SECTION]["sources"]
    assert isinstance(sources, list)
    return frozenset(str(source["path"]) for source in sources)


@cache
def _devx_001_reconciliation_superseded_live_source_paths() -> frozenset[str]:
    _assert_devx_001_reconciliation_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _devx_001_reconciliation_base_baseline_blob(),
    )
    paths = _compatibility_baseline()[DEVX_001_RECONCILIATION_SECTION][
        "superseded_live_source_paths"
    ]
    assert isinstance(paths, list)
    return frozenset(str(path) for path in paths)


@cache
def _devx_001_reconciliation_source_paths() -> frozenset[str]:
    sources = _compatibility_baseline()[DEVX_001_RECONCILIATION_SECTION]["sources"]
    assert isinstance(sources, list)
    return frozenset(str(source["path"]) for source in sources)


@cache
def _devx_005_superseded_live_source_paths() -> frozenset[str]:
    _assert_devx_005_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _devx_005_base_baseline_blob(),
    )
    paths = _compatibility_baseline()[DEVX_005_SECTION]["superseded_live_source_paths"]
    assert isinstance(paths, list)
    return frozenset(str(path) for path in paths)


@cache
def _devx_005_source_paths() -> frozenset[str]:
    sources = _compatibility_baseline()[DEVX_005_SECTION]["sources"]
    assert isinstance(sources, list)
    return frozenset(str(source["path"]) for source in sources)


@cache
def _trading_2462_superseded_live_source_paths() -> frozenset[str]:
    _assert_trading_2462_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _trading_2462_base_baseline_blob(),
    )
    paths = _compatibility_baseline()[TRADING_2462_SECTION]["superseded_live_source_paths"]
    assert isinstance(paths, list)
    return frozenset(str(path) for path in paths)


@cache
def _trading_2462_source_paths() -> frozenset[str]:
    sources = _compatibility_baseline()[TRADING_2462_SECTION]["sources"]
    assert isinstance(sources, list)
    return frozenset(str(source["path"]) for source in sources)


@cache
def _devx_006_superseded_live_source_paths() -> frozenset[str]:
    _assert_devx_006_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _devx_006_base_baseline_blob(),
    )
    paths = _compatibility_baseline()[DEVX_006_SECTION]["superseded_live_source_paths"]
    assert isinstance(paths, list)
    return frozenset(str(path) for path in paths)


@cache
def _devx_006_source_paths() -> frozenset[str]:
    sources = _compatibility_baseline()[DEVX_006_SECTION]["sources"]
    assert isinstance(sources, list)
    return frozenset(str(source["path"]) for source in sources)


@cache
def _arch_005m2_superseded_live_source_paths() -> frozenset[str]:
    _assert_arch_005m2_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _arch_005m2_base_baseline_blob(),
    )
    paths = _compatibility_baseline()[ARCH_005M2_SECTION][
        "superseded_live_source_paths"
    ]
    assert isinstance(paths, list)
    return frozenset(str(path) for path in paths)


@cache
def _arch_005m2_source_paths() -> frozenset[str]:
    sources = _compatibility_baseline()[ARCH_005M2_SECTION]["sources"]
    assert isinstance(sources, list)
    return frozenset(str(source["path"]) for source in sources)


@cache
def _arch_005m3_superseded_live_source_paths() -> frozenset[str]:
    _assert_arch_005m3_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _arch_005m3_base_baseline_blob(),
    )
    paths = _compatibility_baseline()[ARCH_005M3_SECTION][
        "superseded_live_source_paths"
    ]
    assert isinstance(paths, list)
    return frozenset(str(path) for path in paths)


@cache
def _arch_005m3_source_paths() -> frozenset[str]:
    sources = _compatibility_baseline()[ARCH_005M3_SECTION]["sources"]
    assert isinstance(sources, list)
    return frozenset(str(source["path"]) for source in sources)


@cache
def _arch_005m1_batch2_superseded_live_source_paths() -> frozenset[str]:
    _assert_arch_005m1_batch2_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _arch_005m1_batch2_base_baseline_blob(),
    )
    paths = _compatibility_baseline()[ARCH_005M1_BATCH2_SECTION][
        "superseded_live_source_paths"
    ]
    assert isinstance(paths, list)
    return frozenset(str(path) for path in paths)


@cache
def _arch_005m1_batch2_source_paths() -> frozenset[str]:
    sources = _compatibility_baseline()[ARCH_005M1_BATCH2_SECTION]["sources"]
    assert isinstance(sources, list)
    return frozenset(str(source["path"]) for source in sources)


@cache
def _arch_005m1_batch3_superseded_live_source_paths() -> frozenset[str]:
    _assert_arch_005m1_batch3_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _arch_005m1_batch3_base_baseline_blob(),
    )
    paths = _compatibility_baseline()[ARCH_005M1_BATCH3_SECTION][
        "superseded_live_source_paths"
    ]
    assert isinstance(paths, list)
    return frozenset(str(path) for path in paths)


@cache
def _arch_005m1_batch3_source_paths() -> frozenset[str]:
    sources = _compatibility_baseline()[ARCH_005M1_BATCH3_SECTION]["sources"]
    assert isinstance(sources, list)
    return frozenset(str(source["path"]) for source in sources)


@cache
def _arch_005m1_batch4_superseded_live_source_paths() -> frozenset[str]:
    _assert_arch_005m1_batch4_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _arch_005m1_batch4_base_baseline_blob(),
    )
    paths = _compatibility_baseline()[ARCH_005M1_BATCH4_SECTION][
        "superseded_live_source_paths"
    ]
    assert isinstance(paths, list)
    return frozenset(str(path) for path in paths)


@cache
def _arch_005m1_batch4_source_paths() -> frozenset[str]:
    sources = _compatibility_baseline()[ARCH_005M1_BATCH4_SECTION]["sources"]
    assert isinstance(sources, list)
    return frozenset(str(source["path"]) for source in sources)


@cache
def _ops_070_stable_release_superseded_live_source_paths() -> frozenset[str]:
    _assert_ops_070_stable_release_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _ops_070_stable_release_base_baseline_blob(),
    )
    paths = _compatibility_baseline()[OPS_070_STABLE_RELEASE_SECTION][
        "superseded_live_source_paths"
    ]
    assert isinstance(paths, list)
    return frozenset(str(path) for path in paths)


@cache
def _ops_070_stable_release_source_paths() -> frozenset[str]:
    sources = _compatibility_baseline()[OPS_070_STABLE_RELEASE_SECTION]["sources"]
    assert isinstance(sources, list)
    return frozenset(str(source["path"]) for source in sources)


@cache
def _ops_070_runtime_exclude_superseded_live_source_paths() -> frozenset[str]:
    _assert_ops_070_runtime_exclude_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _ops_070_runtime_exclude_base_baseline_blob(),
    )
    paths = _compatibility_baseline()[OPS_070_RUNTIME_EXCLUDE_SECTION][
        "superseded_live_source_paths"
    ]
    assert isinstance(paths, list)
    return frozenset(str(path) for path in paths)


@cache
def _ops_070_runtime_exclude_source_paths() -> frozenset[str]:
    sources = _compatibility_baseline()[OPS_070_RUNTIME_EXCLUDE_SECTION]["sources"]
    assert isinstance(sources, list)
    return frozenset(str(source["path"]) for source in sources)


@cache
def _ops_070_cross_release_policy_superseded_live_source_paths() -> frozenset[str]:
    _assert_ops_070_cross_release_policy_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _ops_070_cross_release_policy_base_baseline_blob(),
    )
    paths = _compatibility_baseline()[OPS_070_CROSS_RELEASE_POLICY_SECTION][
        "superseded_live_source_paths"
    ]
    assert isinstance(paths, list)
    return frozenset(str(path) for path in paths)


@cache
def _ops_070_cross_release_policy_source_paths() -> frozenset[str]:
    sources = _compatibility_baseline()[OPS_070_CROSS_RELEASE_POLICY_SECTION][
        "sources"
    ]
    assert isinstance(sources, list)
    return frozenset(str(source["path"]) for source in sources)


@cache
def _arch_005s4d_s2_all_superseded_live_source_paths() -> frozenset[str]:
    paths = (
        _arch_005s4e_superseded_live_source_paths() | _arch_005s4d_s2_superseded_live_source_paths()
    )
    baseline = _compatibility_baseline()
    if TRADING_2458_2460_INTEGRATION_SECTION in baseline:
        paths |= _trading_2458_2460_integration_source_paths()
    if DEVX_TRADING_CLEANUP_SECTION in baseline:
        paths |= _devx_trading_cleanup_source_paths()
    if TRADING_2459_DOC_CLOSEOUT_SECTION in baseline:
        paths |= _trading_2459_doc_closeout_source_paths()
    if DATA_GOV_002_SECTION in baseline:
        paths |= _data_gov_002_source_paths()
    if DEVX_002_SECTION in baseline:
        paths |= _devx_002_source_paths()
    if DEVX_002_PUSH_V2_SECTION in baseline:
        paths |= _devx_002_push_v2_source_paths()
    if ARCH_004G2_OBSERVABILITY_SECTION in baseline:
        paths |= _arch_004g2_observability_source_paths()
    if TRADING_2458_RETIREMENT_SECTION in baseline:
        paths |= _trading_2458_retirement_source_paths()
    if TRADING_2458_CLOSEOUT_SECTION in baseline:
        paths |= _trading_2458_closeout_source_paths()
    if DATA_GOV_002_PHASE_B2_SECTION in baseline:
        paths |= _data_gov_002_phase_b2_source_paths()
    if DEVX_003_SECTION in baseline:
        paths |= _devx_003_source_paths()
    if DATA_GOV_002C1_SECTION in baseline:
        paths |= _data_gov_002c1_source_paths()
    if DATA_GOV_002C2_SECTION in baseline:
        paths |= _data_gov_002c2_source_paths()
    if TRADING_2461_SECTION in baseline:
        paths |= _trading_2461_source_paths()
    if ARCH_004G2_PAPER_WEEKLY_SECTION in baseline:
        paths |= _arch_004g2_paper_weekly_source_paths()
    if OPS_069_TERMINAL_ARCHIVE_SECTION in baseline:
        paths |= _ops_069_terminal_archive_source_paths()
    if DEVX_004_SECTION in baseline:
        paths |= _devx_004_source_paths()
    if DEVX_001_RECONCILIATION_SECTION in baseline:
        paths |= _devx_001_reconciliation_source_paths()
    if DEVX_005_SECTION in baseline:
        paths |= _devx_005_source_paths()
    if TRADING_2462_SECTION in baseline:
        paths |= _trading_2462_source_paths()
    if DEVX_006_SECTION in baseline:
        paths |= _devx_006_source_paths()
    if ARCH_005M2_SECTION in baseline:
        paths |= _arch_005m2_source_paths()
    if ARCH_005M3_SECTION in baseline:
        paths |= _arch_005m3_source_paths()
    if ARCH_005M1_BATCH2_SECTION in baseline:
        paths |= _arch_005m1_batch2_source_paths()
    if ARCH_005M1_BATCH3_SECTION in baseline:
        paths |= _arch_005m1_batch3_source_paths()
    if ARCH_005M1_BATCH4_SECTION in baseline:
        paths |= _arch_005m1_batch4_source_paths()
    if OPS_070_STABLE_RELEASE_SECTION in baseline:
        paths |= _ops_070_stable_release_source_paths()
    return paths


def _raw_source_sha256(source: dict[str, object]) -> str:
    path = str(source["path"])
    normalization = source.get("hash_normalization")
    if normalization is not None and normalization != "git_eol_lf":
        raise AssertionError(f"unsupported hash normalization: {normalization}")
    typed_normalization = str(normalization) if normalization is not None else None
    if Path(path).is_absolute():
        return _normalized_source_sha256(path, typed_normalization)
    return _cached_repository_source_sha256(path, typed_normalization)


@cache
def _cached_repository_source_sha256(
    path: str,
    normalization: str | None,
) -> str:
    return _normalized_source_sha256(path, normalization)


def _normalized_source_sha256(path: str, normalization: str | None) -> str:
    payload = Path(path).read_bytes()
    if normalization == "git_eol_lf":
        payload = payload.replace(b"\r\n", b"\n")
    return hashlib.sha256(payload).hexdigest()


def _source_sha256_at_commit(source: dict[str, object], commit: str) -> str:
    object_name = f"{commit}:{source['path']}"
    payload = subprocess.run(
        ["git", "show", object_name],
        check=True,
        capture_output=True,
    ).stdout
    normalization = source.get("hash_normalization")
    if normalization == "git_eol_lf":
        payload = payload.replace(b"\r\n", b"\n")
    elif normalization is not None:
        raise AssertionError(f"unsupported hash normalization: {normalization}")
    return hashlib.sha256(payload).hexdigest()


def _source_sha256s_at_commit(
    sources: list[dict[str, object]],
    commit: str,
) -> dict[str, str]:
    object_names: list[str] = []
    for source in sources:
        path = _assert_portable_repository_relative_path(source["path"])
        object_names.append(f"{commit}:{path}")
    batch_input = ("\n".join(object_names) + "\n").encode("utf-8")
    output = subprocess.run(
        ["git", "cat-file", "--batch"],
        input=batch_input,
        check=True,
        capture_output=True,
    ).stdout

    offset = 0
    hashes: dict[str, str] = {}
    for source in sources:
        header_end = output.index(b"\n", offset)
        header = output[offset:header_end]
        offset = header_end + 1
        header_parts = header.rsplit(b" ", 2)
        assert len(header_parts) == 3 and header_parts[1] == b"blob"
        size = int(header_parts[2])
        payload = output[offset : offset + size]
        assert len(payload) == size
        offset += size
        assert output[offset : offset + 1] == b"\n"
        offset += 1

        normalization = source.get("hash_normalization")
        if normalization == "git_eol_lf":
            payload = payload.replace(b"\r\n", b"\n")
        elif normalization is not None:
            raise AssertionError(f"unsupported hash normalization: {normalization}")
        hashes[str(source["path"])] = hashlib.sha256(payload).hexdigest()
    assert offset == len(output)
    assert len(hashes) == len(sources)
    return hashes


def _assert_wave14_s2_formal_tiers(
    validation: dict[str, Any],
    *,
    phase_complete: bool,
) -> None:
    required = validation["required_formal_tiers"]
    assert required == {
        "pre_full": list(WAVE14_S2_PRE_FULL_REQUIRED_TIERS),
        "post_full": list(WAVE14_S2_POST_FULL_REQUIRED_TIERS),
    }
    tier_groups = {
        "pre_full": validation["pre_full_formal_tiers"],
        "post_full": validation["post_full_formal_tiers"],
    }
    for group_name, required_names in (
        ("pre_full", WAVE14_S2_PRE_FULL_REQUIRED_TIERS),
        ("post_full", WAVE14_S2_POST_FULL_REQUIRED_TIERS),
    ):
        tiers = tier_groups[group_name]
        assert isinstance(tiers, dict)
        assert tuple(tiers) == required_names
        for tier_name, tier in tiers.items():
            assert isinstance(tier, dict)
            assert tier["required"] is True
            if phase_complete:
                assert tier["status"] == "PASS", f"COMPLETE requires {group_name}.{tier_name} PASS"
            else:
                assert tier["status"] in {"PENDING", "PASS"}
            if tier["status"] == "PASS":
                assert type(tier["passed"]) is int and tier["passed"] > 0
                assert type(tier["failed"]) is int and tier["failed"] == 0


def _removed_live_source_paths_after(section_id: str) -> frozenset[str]:
    baseline = _compatibility_baseline()
    section_seen = False
    removed_paths: set[str] = set()
    for current_section_id, section in baseline.items():
        if current_section_id == section_id:
            section_seen = True
            continue
        if not section_seen or not isinstance(section, dict):
            continue
        removed_paths.update(str(path) for path in section.get("removed_live_source_paths", []))
    assert section_seen, f"compatibility section not found: {section_id}"
    return frozenset(removed_paths)


def _assert_wave14_s2_all_sources_tracked(
    source_paths: list[str],
    *,
    allowed_removed_paths: frozenset[str] = frozenset(),
) -> None:
    for path in source_paths:
        portable_path = _assert_portable_repository_relative_path(path)
        result = subprocess.run(
            ["git", "ls-files", "--error-unmatch", "-z", "--", portable_path],
            check=False,
            capture_output=True,
        )
        if path in allowed_removed_paths:
            assert not Path(portable_path).exists(), (
                f"later removal authority requires source to be absent: {path}"
            )
            continue
        assert result.returncode == 0, f"Wave14 S2 source must be Git-tracked: {path}"
        assert result.stdout == portable_path.encode() + b"\0"


def _wave14_s2_source_manifest_sha256(
    sources: list[dict[str, object]],
) -> str:
    manifest_records: list[dict[str, object]] = []
    for source in sorted(sources, key=lambda item: str(item["path"])):
        record: dict[str, object] = {
            "path": str(source["path"]),
            "sha256": str(source["sha256"]),
        }
        normalization = source.get("hash_normalization")
        if normalization is not None:
            record["hash_normalization"] = normalization
        manifest_records.append(record)
    canonical_payload = json.dumps(
        manifest_records,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return hashlib.sha256(canonical_payload).hexdigest()


def _wave14_s2_git_tree(commit: str) -> str:
    object_check = subprocess.run(
        ["git", "cat-file", "-e", f"{commit}^{{commit}}"],
        check=False,
        capture_output=True,
    )
    assert object_check.returncode == 0, "tested_commit must resolve to a Git commit"
    return subprocess.run(
        ["git", "rev-parse", f"{commit}^{{tree}}"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _wave14_s2_section_at_commit(commit: str) -> dict[str, Any]:
    payload = subprocess.run(
        ["git", "show", f"{commit}:{WAVE11_BASELINE_REPOSITORY_PATH}"],
        check=True,
        capture_output=True,
    ).stdout.decode("utf-8")
    baseline = safe_load_yaml_text(payload)
    assert isinstance(baseline, dict)
    section = baseline.get(WAVE14_S2_SECTION)
    assert isinstance(section, dict), "tested candidate must contain the Wave14 S2 section"
    return section


def _wave14_s2_changed_paths_since(tested_commit: str) -> frozenset[str]:
    protected_exclude = ":(top,literal,exclude)" + WAVE14_S2_PROHIBITED_USER_PATH
    tracked = subprocess.run(
        [
            "git",
            "diff",
            "--name-only",
            "--no-renames",
            "-z",
            tested_commit,
            "--",
            ".",
            protected_exclude,
        ],
        check=True,
        capture_output=True,
    ).stdout
    untracked = subprocess.run(
        [
            "git",
            "ls-files",
            "--others",
            "--exclude-standard",
            "-z",
            "--",
            ".",
            protected_exclude,
        ],
        check=True,
        capture_output=True,
    ).stdout
    paths = {path.decode("utf-8") for path in (tracked + untracked).split(b"\0") if path}
    assert WAVE14_S2_PROHIBITED_USER_PATH not in paths
    return frozenset(paths)


def _assert_wave14_s2_final_full_evidence(
    wave14: dict[str, Any],
    sources: list[dict[str, object]],
    approved_post_full_paths: frozenset[str],
) -> None:
    validation = wave14["validation"]
    final_attempts = _assert_wave14_s2_full_attempt_chain(validation["full_validation"])
    final_attempt = final_attempts[-1]
    assert final_attempt["status"] == "PASS"
    tested_commit = final_attempt["tested_commit"]
    tested_tree = final_attempt["tested_tree"]
    assert _wave14_s2_git_tree(tested_commit) == tested_tree
    assert (
        subprocess.run(
            ["git", "merge-base", "--is-ancestor", tested_commit, "HEAD"],
            check=False,
            capture_output=True,
        ).returncode
        == 0
    ), "the Full-tested candidate must be an ancestor of the final state"

    candidate = _wave14_s2_section_at_commit(tested_commit)
    assert candidate["status"] == "VALIDATING_WAVE14_S2"
    assert candidate["source_hash_status"] == "PRE_FULL_TRACKED_STATE_FRESH"
    candidate_policy_payload = subprocess.run(
        ["git", "show", f"{tested_commit}:{POLICY_PATH.as_posix()}"],
        check=True,
        capture_output=True,
    ).stdout.decode("utf-8")
    candidate_policy = safe_load_yaml_text(candidate_policy_payload)
    assert isinstance(candidate_policy, dict)
    candidate_coordination = candidate_policy["phase_g_execution"]["current_coordination_wave"]
    assert candidate_coordination["compatibility_status"] == "VALIDATING_WAVE14_S2"
    assert candidate_coordination["status"] == "S2_IN_PROGRESS"
    candidate_validation = candidate["validation"]
    _assert_wave14_s2_formal_tiers(candidate_validation, phase_complete=False)
    assert all(
        tier["status"] == "PENDING"
        for tier in candidate_validation["pre_full_formal_tiers"].values()
    ), "the immutable candidate cannot self-claim validation that runs after its commit"
    assert all(
        tier["status"] == "PENDING"
        for tier in candidate_validation["post_full_formal_tiers"].values()
    )
    candidate_attempts = _assert_wave14_s2_full_attempt_chain(
        candidate_validation["full_validation"]
    )
    assert candidate_attempts[-1]["status"] == "PENDING"
    assert len(candidate_attempts) == len(final_attempts)
    assert candidate_attempts[:-1] == final_attempts[:-1], (
        "executed Full history before the tested attempt must remain byte-equivalent"
    )
    for identity_key in ("attempt_id", "role", "required", "replaces_attempt_id"):
        if identity_key in candidate_attempts[-1] or identity_key in final_attempt:
            assert candidate_attempts[-1].get(identity_key) == final_attempt.get(identity_key)
    assert (
        candidate_validation["full_validation"]["run_count"] + 1
        == (validation["full_validation"]["run_count"])
    )

    for tier in validation["pre_full_formal_tiers"].values():
        assert tier["tested_commit"] == tested_commit
        assert tier["tested_tree"] == tested_tree

    final_source_by_path = {str(source["path"]): source for source in sources}
    candidate_sources = candidate["sources"]
    assert isinstance(candidate_sources, list)
    candidate_source_by_path = {str(source["path"]): source for source in candidate_sources}
    assert len(candidate_source_by_path) == len(candidate_sources)
    candidate_source_paths = set(candidate_source_by_path)
    sensitive_paths = wave14["full_sensitive_sources"]
    assert candidate["full_sensitive_sources"] == sensitive_paths
    assert set(sensitive_paths) == candidate_source_paths - (
        approved_post_full_paths & candidate_source_paths
    )
    assert sensitive_paths
    sensitive_source_records = [final_source_by_path[path] for path in sensitive_paths]
    tested_source_sha256s = _source_sha256s_at_commit(
        sensitive_source_records,
        tested_commit,
    )
    for path in sensitive_paths:
        final_source = final_source_by_path[path]
        candidate_source = candidate_source_by_path[path]
        assert candidate_source == final_source, (
            f"Full-sensitive source record changed after Full: {path}"
        )
        assert tested_source_sha256s[path] == final_source["sha256"], path
    assert final_attempt["full_sensitive_source_manifest_sha256"] == (
        _wave14_s2_source_manifest_sha256(sensitive_source_records)
    )

    assert candidate["post_full_evidence_only_paths"] == sorted(approved_post_full_paths)
    assert candidate["removed_live_source_paths"] == []
    for removed_path in wave14["removed_live_source_paths"]:
        assert (
            subprocess.run(
                ["git", "cat-file", "-e", f"{tested_commit}:{removed_path}"],
                check=False,
                capture_output=True,
            ).returncode
            == 0
        ), "governed removal must have existed in the tested candidate"
    candidate_post_full = candidate_validation["post_full_closeout"]
    assert candidate_post_full["status"] == "PENDING"
    assert candidate_post_full["changed_paths"] == []
    assert candidate_post_full["full_sensitive_sources_unchanged"] is False
    assert candidate_post_full["changed_paths_are_evidence_only"] is False
    assert candidate_post_full["full_rerun_required"] is False

    final_post_full = validation["post_full_closeout"]
    observed_changed_paths = _wave14_s2_changed_paths_since(tested_commit)
    recorded_changed_paths = final_post_full["changed_paths"]
    assert recorded_changed_paths == sorted(observed_changed_paths)
    assert observed_changed_paths
    assert observed_changed_paths <= approved_post_full_paths
    assert final_post_full["status"] == "PASS"
    assert final_post_full["full_sensitive_sources_unchanged"] is True
    assert final_post_full["changed_paths_are_evidence_only"] is True
    assert final_post_full["full_rerun_required"] is False


@cache
def _normalization_migrations_before(
    stop_section: str,
) -> dict[str, tuple[dict[str, object], ...]]:
    baseline = _compatibility_baseline()
    normalization_migrations: dict[str, list[dict[str, object]]] = {}
    for section_id, section in baseline.items():
        if section_id == stop_section:
            break
        if not isinstance(section, dict):
            continue
        for source_key in ("frozen_sources", "sources"):
            records = section.get(source_key, [])
            if not isinstance(records, list):
                continue
            for source in records:
                if (
                    isinstance(source, dict)
                    and source.get("hash_normalization") == "git_eol_lf"
                    and isinstance(source.get("previous_worktree_sha256"), str)
                    and {"path", "sha256"} <= source.keys()
                ):
                    normalization_migrations.setdefault(str(source["path"]), []).append(source)
    return {path: tuple(records) for path, records in normalization_migrations.items()}


def _source_matches_checkout(
    source: dict[str, object],
    normalization_migrations: dict[str, tuple[dict[str, object], ...]],
) -> bool:
    try:
        source_sha256 = _raw_source_sha256(source)
    except FileNotFoundError:
        return False
    if source_sha256 == str(source["sha256"]):
        return True
    return any(
        str(source["sha256"]) == str(migration["previous_worktree_sha256"])
        and _raw_source_sha256(migration) == str(migration["sha256"])
        for migration in normalization_migrations.get(str(source["path"]), ())
    )


def _prior_active_source_mismatches(stop_section: str) -> frozenset[str]:
    baseline = _compatibility_baseline()
    normalization_migrations = _normalization_migrations_before(stop_section)
    mismatches: set[str] = set()
    for section_id, section in baseline.items():
        if section_id == stop_section:
            break
        if not isinstance(section, dict):
            continue
        section_superseded_paths = {
            str(path) for path in section.get("superseded_source_paths", [])
        }
        for source_key in ("frozen_sources", "sources"):
            records = section.get(source_key, [])
            if not isinstance(records, list):
                continue
            for source in records:
                if not isinstance(source, dict) or not {"path", "sha256"} <= source.keys():
                    continue
                path = str(source["path"])
                is_historical = any(
                    str(key).startswith("historical_") and bool(value)
                    for key, value in source.items()
                )
                if is_historical or path in section_superseded_paths:
                    continue
                if not _source_matches_checkout(source, normalization_migrations):
                    mismatches.add(path)
    return frozenset(mismatches)


def _latest_active_source_mismatches(stop_section: str) -> frozenset[str]:
    baseline = _compatibility_baseline()
    active: dict[str, dict[str, object]] = {}
    for section_id, section in baseline.items():
        if section_id == stop_section:
            break
        if not isinstance(section, dict):
            continue
        removed_paths = {
            str(path)
            for key in ("removed_live_source_paths", "superseded_source_paths")
            for path in section.get(key, [])
        }
        for path in removed_paths:
            active.pop(path, None)
        for source_key in ("frozen_sources", "sources"):
            records = section.get(source_key, [])
            if not isinstance(records, list):
                continue
            for source in records:
                if not isinstance(source, dict) or not {"path", "sha256"} <= source.keys():
                    continue
                is_historical = any(
                    str(key).startswith("historical_") and bool(value)
                    for key, value in source.items()
                )
                if not is_historical:
                    active[str(source["path"])] = source
    normalization_migrations = _normalization_migrations_before(stop_section)
    return frozenset(
        path
        for path, source in active.items()
        if not _source_matches_checkout(source, normalization_migrations)
    )


@cache
def _docs_gov_prior_active_source_mismatches() -> frozenset[str]:
    return _prior_active_source_mismatches(DOCS_GOV_SECTION)


@cache
def _wave12_prior_active_source_mismatches() -> frozenset[str]:
    return _prior_active_source_mismatches(WAVE12_SECTION)


@cache
def _wave13_prior_active_source_mismatches() -> frozenset[str]:
    return _prior_active_source_mismatches(WAVE13_SECTION)


@cache
def _wave14_s0_1_prior_active_source_mismatches() -> frozenset[str]:
    return _prior_active_source_mismatches(WAVE14_S0_1_SECTION)


@cache
def _wave14_s2_prior_active_source_mismatches() -> frozenset[str]:
    return _prior_active_source_mismatches(WAVE14_S2_SECTION)


@cache
def _ops_067_prior_active_source_mismatches() -> frozenset[str]:
    return _prior_active_source_mismatches(OPS_067_SECTION)


@cache
def _ops_068_prior_active_source_mismatches() -> frozenset[str]:
    return _prior_active_source_mismatches(OPS_068_SECTION)


@cache
def _arch_005s4d_prior_active_source_mismatches() -> frozenset[str]:
    return _prior_active_source_mismatches(ARCH_005S4D_SECTION)


@cache
def _wave15_prior_active_source_mismatches() -> frozenset[str]:
    return _prior_active_source_mismatches(WAVE15_SECTION)


@cache
def _d0b2b_prior_active_source_mismatches() -> frozenset[str]:
    return _prior_active_source_mismatches(D0B2B_SECTION)


@cache
def _ops_069_prior_active_source_mismatches() -> frozenset[str]:
    return _prior_active_source_mismatches(OPS_069_SECTION)


@cache
def _ops_070_prior_active_source_mismatches() -> frozenset[str]:
    return _prior_active_source_mismatches(OPS_070_SECTION)


@cache
def _arch_005s4e_prior_active_source_mismatches() -> frozenset[str]:
    return _prior_active_source_mismatches(ARCH_005S4E_SECTION)


@cache
def _arch_005s4d_s2_prior_active_source_mismatches() -> frozenset[str]:
    return _latest_active_source_mismatches(ARCH_005S4D_S2_SECTION)


@cache
def _trading_2458_2460_integration_prior_active_source_mismatches() -> frozenset[str]:
    return _latest_active_source_mismatches(TRADING_2458_2460_INTEGRATION_SECTION)


@cache
def _devx_trading_cleanup_prior_active_source_mismatches() -> frozenset[str]:
    return _latest_active_source_mismatches(DEVX_TRADING_CLEANUP_SECTION)


@cache
def _trading_2459_doc_closeout_prior_active_source_mismatches() -> frozenset[str]:
    return _latest_active_source_mismatches(TRADING_2459_DOC_CLOSEOUT_SECTION)


@cache
def _data_gov_002_prior_active_source_mismatches() -> frozenset[str]:
    return _latest_active_source_mismatches(DATA_GOV_002_SECTION)


@cache
def _devx_002_prior_active_source_mismatches() -> frozenset[str]:
    return _latest_active_source_mismatches(DEVX_002_SECTION)


@cache
def _devx_002_push_v2_prior_active_source_mismatches() -> frozenset[str]:
    return _latest_active_source_mismatches(DEVX_002_PUSH_V2_SECTION)


@cache
def _arch_004g2_observability_prior_active_source_mismatches() -> frozenset[str]:
    return _latest_active_source_mismatches(ARCH_004G2_OBSERVABILITY_SECTION)


@cache
def _arch_004g2_closure_threshold_prior_active_source_mismatches() -> frozenset[str]:
    return _latest_active_source_mismatches(ARCH_004G2_CLOSURE_THRESHOLD_SECTION)


@cache
def _data_gov_002_phase_b1_prior_active_source_mismatches() -> frozenset[str]:
    return _latest_active_source_mismatches(DATA_GOV_002_PHASE_B1_SECTION)


@cache
def _trading_2458_retirement_prior_active_source_mismatches() -> frozenset[str]:
    return _latest_active_source_mismatches(TRADING_2458_RETIREMENT_SECTION)


@cache
def _trading_2458_closeout_prior_active_source_mismatches() -> frozenset[str]:
    return _latest_active_source_mismatches(TRADING_2458_CLOSEOUT_SECTION)


@cache
def _data_gov_002_phase_b2_prior_active_source_mismatches() -> frozenset[str]:
    return _latest_active_source_mismatches(DATA_GOV_002_PHASE_B2_SECTION)


@cache
def _devx_003_prior_active_source_mismatches() -> frozenset[str]:
    return _latest_active_source_mismatches(DEVX_003_SECTION)


@cache
def _data_gov_002c1_prior_active_source_mismatches() -> frozenset[str]:
    return _latest_active_source_mismatches(DATA_GOV_002C1_SECTION)


@cache
def _data_gov_002c2_prior_active_source_mismatches() -> frozenset[str]:
    return _latest_active_source_mismatches(DATA_GOV_002C2_SECTION)


@cache
def _trading_2461_prior_active_source_mismatches() -> frozenset[str]:
    return _latest_active_source_mismatches(TRADING_2461_SECTION)


@cache
def _arch_004g2_paper_weekly_prior_active_source_mismatches() -> frozenset[str]:
    return _latest_active_source_mismatches(ARCH_004G2_PAPER_WEEKLY_SECTION)


@cache
def _ops_069_terminal_archive_prior_active_source_mismatches() -> frozenset[str]:
    return _latest_active_source_mismatches(OPS_069_TERMINAL_ARCHIVE_SECTION)


@cache
def _devx_004_prior_active_source_mismatches() -> frozenset[str]:
    return _latest_active_source_mismatches(DEVX_004_SECTION)


@cache
def _devx_001_reconciliation_prior_active_source_mismatches() -> frozenset[str]:
    return _latest_active_source_mismatches(DEVX_001_RECONCILIATION_SECTION)


@cache
def _devx_005_prior_active_source_mismatches() -> frozenset[str]:
    return _latest_active_source_mismatches(DEVX_005_SECTION)


@cache
def _trading_2462_prior_active_source_mismatches() -> frozenset[str]:
    return _latest_active_source_mismatches(TRADING_2462_SECTION)


@cache
def _devx_006_prior_active_source_mismatches() -> frozenset[str]:
    return _latest_active_source_mismatches(DEVX_006_SECTION)


@cache
def _arch_005m2_prior_active_source_mismatches() -> frozenset[str]:
    return _latest_active_source_mismatches(ARCH_005M2_SECTION)


@cache
def _arch_005m3_prior_active_source_mismatches() -> frozenset[str]:
    return _latest_active_source_mismatches(ARCH_005M3_SECTION)


@cache
def _arch_005m1_batch2_prior_active_source_mismatches() -> frozenset[str]:
    return _latest_active_source_mismatches(ARCH_005M1_BATCH2_SECTION)


@cache
def _arch_005m1_batch3_prior_active_source_mismatches() -> frozenset[str]:
    return _latest_active_source_mismatches(ARCH_005M1_BATCH3_SECTION)


@cache
def _arch_005m1_batch4_prior_active_source_mismatches() -> frozenset[str]:
    return _latest_active_source_mismatches(ARCH_005M1_BATCH4_SECTION)


@cache
def _ops_070_stable_release_prior_active_source_mismatches() -> frozenset[str]:
    return _latest_active_source_mismatches(OPS_070_STABLE_RELEASE_SECTION)


@cache
def _ops_070_runtime_exclude_prior_active_source_mismatches() -> frozenset[str]:
    return _latest_active_source_mismatches(OPS_070_RUNTIME_EXCLUDE_SECTION)


@cache
def _ops_070_cross_release_policy_prior_active_source_mismatches() -> frozenset[str]:
    return _latest_active_source_mismatches(OPS_070_CROSS_RELEASE_POLICY_SECTION)


def _source_sha256(source: dict[str, object]) -> str:
    # Historical source records retain their captured hashes. Live drift must be
    # owned by one of the append-only supersession ledgers; the newest section is
    # the current raw-live hash authority without rewriting any prior bytes.
    baseline = _compatibility_baseline()
    if OPS_070_CROSS_RELEASE_POLICY_SECTION in baseline:
        current_superseded_paths = (
            _ops_070_cross_release_policy_superseded_live_source_paths()
        )
        assert (
            _ops_070_cross_release_policy_prior_active_source_mismatches()
            == current_superseded_paths
        )
        superseded_paths = (
            _arch_005s4d_s2_all_superseded_live_source_paths()
            | _ops_070_stable_release_superseded_live_source_paths()
            | _ops_070_runtime_exclude_superseded_live_source_paths()
            | current_superseded_paths
        )
        authority_section = OPS_070_CROSS_RELEASE_POLICY_SECTION
    elif OPS_070_RUNTIME_EXCLUDE_SECTION in baseline:
        current_superseded_paths = (
            _ops_070_runtime_exclude_superseded_live_source_paths()
        )
        assert (
            _ops_070_runtime_exclude_prior_active_source_mismatches()
            == current_superseded_paths
        )
        superseded_paths = (
            _arch_005s4d_s2_all_superseded_live_source_paths()
            | _ops_070_stable_release_superseded_live_source_paths()
            | current_superseded_paths
        )
        authority_section = OPS_070_RUNTIME_EXCLUDE_SECTION
    elif OPS_070_STABLE_RELEASE_SECTION in baseline:
        current_superseded_paths = (
            _ops_070_stable_release_superseded_live_source_paths()
        )
        assert (
            _ops_070_stable_release_prior_active_source_mismatches()
            == current_superseded_paths
        )
        superseded_paths = (
            _arch_005s4d_s2_all_superseded_live_source_paths()
            | current_superseded_paths
        )
        authority_section = OPS_070_STABLE_RELEASE_SECTION
    elif ARCH_005M1_BATCH4_SECTION in baseline:
        current_superseded_paths = _arch_005m1_batch4_superseded_live_source_paths()
        assert (
            _arch_005m1_batch4_prior_active_source_mismatches()
            == current_superseded_paths
        )
        superseded_paths = (
            _arch_005s4d_s2_all_superseded_live_source_paths()
            | current_superseded_paths
        )
        authority_section = ARCH_005M1_BATCH4_SECTION
    elif ARCH_005M1_BATCH3_SECTION in baseline:
        current_superseded_paths = _arch_005m1_batch3_superseded_live_source_paths()
        assert (
            _arch_005m1_batch3_prior_active_source_mismatches()
            == current_superseded_paths
        )
        superseded_paths = (
            _arch_005s4d_s2_all_superseded_live_source_paths()
            | current_superseded_paths
        )
        authority_section = ARCH_005M1_BATCH3_SECTION
    elif ARCH_005M1_BATCH2_SECTION in baseline:
        current_superseded_paths = _arch_005m1_batch2_superseded_live_source_paths()
        assert (
            _arch_005m1_batch2_prior_active_source_mismatches()
            == current_superseded_paths
        )
        superseded_paths = (
            _arch_005s4d_s2_all_superseded_live_source_paths()
            | _devx_trading_cleanup_source_paths()
            | _trading_2459_doc_closeout_source_paths()
            | _data_gov_002_source_paths()
            | _devx_002_source_paths()
            | _devx_002_push_v2_source_paths()
            | _arch_004g2_observability_source_paths()
            | _arch_004g2_closure_threshold_source_paths()
            | _data_gov_002_phase_b1_source_paths()
            | _trading_2458_retirement_source_paths()
            | _trading_2458_closeout_source_paths()
            | _data_gov_002_phase_b2_source_paths()
            | _devx_003_source_paths()
            | _data_gov_002c1_source_paths()
            | _data_gov_002c2_source_paths()
            | _trading_2461_source_paths()
            | _arch_004g2_paper_weekly_source_paths()
            | _ops_069_terminal_archive_source_paths()
            | _devx_004_source_paths()
            | _devx_001_reconciliation_source_paths()
            | _devx_005_source_paths()
            | _trading_2462_source_paths()
            | _devx_006_source_paths()
            | _arch_005m2_source_paths()
            | _arch_005m3_source_paths()
            | _arch_005m1_batch2_source_paths()
            | current_superseded_paths
        )
        authority_section = ARCH_005M1_BATCH2_SECTION
    elif ARCH_005M3_SECTION in baseline:
        current_superseded_paths = _arch_005m3_superseded_live_source_paths()
        assert _arch_005m3_prior_active_source_mismatches() == current_superseded_paths
        superseded_paths = (
            _arch_005s4d_s2_all_superseded_live_source_paths()
            | _devx_trading_cleanup_source_paths()
            | _trading_2459_doc_closeout_source_paths()
            | _data_gov_002_source_paths()
            | _devx_002_source_paths()
            | _devx_002_push_v2_source_paths()
            | _arch_004g2_observability_source_paths()
            | _arch_004g2_closure_threshold_source_paths()
            | _data_gov_002_phase_b1_source_paths()
            | _trading_2458_retirement_source_paths()
            | _trading_2458_closeout_source_paths()
            | _data_gov_002_phase_b2_source_paths()
            | _devx_003_source_paths()
            | _data_gov_002c1_source_paths()
            | _data_gov_002c2_source_paths()
            | _trading_2461_source_paths()
            | _arch_004g2_paper_weekly_source_paths()
            | _ops_069_terminal_archive_source_paths()
            | _devx_004_source_paths()
            | _devx_001_reconciliation_source_paths()
            | _devx_005_source_paths()
            | _trading_2462_source_paths()
            | _devx_006_source_paths()
            | _arch_005m2_source_paths()
            | _arch_005m3_source_paths()
            | current_superseded_paths
        )
        authority_section = ARCH_005M3_SECTION
    elif ARCH_005M2_SECTION in baseline:
        current_superseded_paths = _arch_005m2_superseded_live_source_paths()
        assert _arch_005m2_prior_active_source_mismatches() == current_superseded_paths
        superseded_paths = (
            _arch_005s4d_s2_all_superseded_live_source_paths()
            | _devx_trading_cleanup_source_paths()
            | _trading_2459_doc_closeout_source_paths()
            | _data_gov_002_source_paths()
            | _devx_002_source_paths()
            | _devx_002_push_v2_source_paths()
            | _arch_004g2_observability_source_paths()
            | _arch_004g2_closure_threshold_source_paths()
            | _data_gov_002_phase_b1_source_paths()
            | _trading_2458_retirement_source_paths()
            | _trading_2458_closeout_source_paths()
            | _data_gov_002_phase_b2_source_paths()
            | _devx_003_source_paths()
            | _data_gov_002c1_source_paths()
            | _data_gov_002c2_source_paths()
            | _trading_2461_source_paths()
            | _arch_004g2_paper_weekly_source_paths()
            | _ops_069_terminal_archive_source_paths()
            | _devx_004_source_paths()
            | _devx_001_reconciliation_source_paths()
            | _devx_005_source_paths()
            | _trading_2462_source_paths()
            | _devx_006_source_paths()
            | _arch_005m2_source_paths()
            | current_superseded_paths
        )
        authority_section = ARCH_005M2_SECTION
    elif DEVX_006_SECTION in baseline:
        current_superseded_paths = _devx_006_superseded_live_source_paths()
        assert _devx_006_prior_active_source_mismatches() == current_superseded_paths
        superseded_paths = (
            _arch_005s4d_s2_all_superseded_live_source_paths()
            | _devx_trading_cleanup_source_paths()
            | _trading_2459_doc_closeout_source_paths()
            | _data_gov_002_source_paths()
            | _devx_002_source_paths()
            | _devx_002_push_v2_source_paths()
            | _arch_004g2_observability_source_paths()
            | _arch_004g2_closure_threshold_source_paths()
            | _data_gov_002_phase_b1_source_paths()
            | _trading_2458_retirement_source_paths()
            | _trading_2458_closeout_source_paths()
            | _data_gov_002_phase_b2_source_paths()
            | _devx_003_source_paths()
            | _data_gov_002c1_source_paths()
            | _data_gov_002c2_source_paths()
            | _trading_2461_source_paths()
            | _arch_004g2_paper_weekly_source_paths()
            | _ops_069_terminal_archive_source_paths()
            | _devx_004_source_paths()
            | _devx_001_reconciliation_source_paths()
            | _devx_005_source_paths()
            | _trading_2462_source_paths()
            | _devx_006_source_paths()
            | current_superseded_paths
        )
        authority_section = DEVX_006_SECTION
    elif TRADING_2462_SECTION in baseline:
        current_superseded_paths = _trading_2462_superseded_live_source_paths()
        assert _trading_2462_prior_active_source_mismatches() == current_superseded_paths
        superseded_paths = (
            _arch_005s4d_s2_all_superseded_live_source_paths()
            | _devx_trading_cleanup_source_paths()
            | _trading_2459_doc_closeout_source_paths()
            | _data_gov_002_source_paths()
            | _devx_002_source_paths()
            | _devx_002_push_v2_source_paths()
            | _arch_004g2_observability_source_paths()
            | _arch_004g2_closure_threshold_source_paths()
            | _data_gov_002_phase_b1_source_paths()
            | _trading_2458_retirement_source_paths()
            | _trading_2458_closeout_source_paths()
            | _data_gov_002_phase_b2_source_paths()
            | _devx_003_source_paths()
            | _data_gov_002c1_source_paths()
            | _data_gov_002c2_source_paths()
            | _trading_2461_source_paths()
            | _arch_004g2_paper_weekly_source_paths()
            | _ops_069_terminal_archive_source_paths()
            | _devx_004_source_paths()
            | _devx_001_reconciliation_source_paths()
            | _devx_005_source_paths()
            | _trading_2462_source_paths()
            | current_superseded_paths
        )
        authority_section = TRADING_2462_SECTION
    elif DEVX_005_SECTION in baseline:
        current_superseded_paths = _devx_005_superseded_live_source_paths()
        assert _devx_005_prior_active_source_mismatches() == current_superseded_paths
        superseded_paths = (
            _arch_005s4d_s2_all_superseded_live_source_paths()
            | _devx_trading_cleanup_source_paths()
            | _trading_2459_doc_closeout_source_paths()
            | _data_gov_002_source_paths()
            | _devx_002_source_paths()
            | _devx_002_push_v2_source_paths()
            | _arch_004g2_observability_source_paths()
            | _arch_004g2_closure_threshold_source_paths()
            | _data_gov_002_phase_b1_source_paths()
            | _trading_2458_retirement_source_paths()
            | _trading_2458_closeout_source_paths()
            | _data_gov_002_phase_b2_source_paths()
            | _devx_003_source_paths()
            | _data_gov_002c1_source_paths()
            | _data_gov_002c2_source_paths()
            | _trading_2461_source_paths()
            | _arch_004g2_paper_weekly_source_paths()
            | _ops_069_terminal_archive_source_paths()
            | _devx_004_source_paths()
            | _devx_001_reconciliation_source_paths()
            | _devx_005_source_paths()
            | current_superseded_paths
        )
        authority_section = DEVX_005_SECTION
    elif DEVX_001_RECONCILIATION_SECTION in baseline:
        current_superseded_paths = _devx_001_reconciliation_superseded_live_source_paths()
        assert _devx_001_reconciliation_prior_active_source_mismatches() == current_superseded_paths
        superseded_paths = (
            _arch_005s4d_s2_all_superseded_live_source_paths()
            | _devx_trading_cleanup_source_paths()
            | _trading_2459_doc_closeout_source_paths()
            | _data_gov_002_source_paths()
            | _devx_002_source_paths()
            | _devx_002_push_v2_source_paths()
            | _arch_004g2_observability_source_paths()
            | _arch_004g2_closure_threshold_source_paths()
            | _data_gov_002_phase_b1_source_paths()
            | _trading_2458_retirement_source_paths()
            | _trading_2458_closeout_source_paths()
            | _data_gov_002_phase_b2_source_paths()
            | _devx_003_source_paths()
            | _data_gov_002c1_source_paths()
            | _data_gov_002c2_source_paths()
            | _trading_2461_source_paths()
            | _arch_004g2_paper_weekly_source_paths()
            | _ops_069_terminal_archive_source_paths()
            | _devx_004_source_paths()
            | _devx_001_reconciliation_source_paths()
            | current_superseded_paths
        )
        authority_section = DEVX_001_RECONCILIATION_SECTION
    elif DEVX_004_SECTION in baseline:
        current_superseded_paths = _devx_004_superseded_live_source_paths()
        assert _devx_004_prior_active_source_mismatches() == current_superseded_paths
        superseded_paths = (
            _arch_005s4d_s2_all_superseded_live_source_paths()
            | _devx_trading_cleanup_source_paths()
            | _trading_2459_doc_closeout_source_paths()
            | _data_gov_002_source_paths()
            | _devx_002_source_paths()
            | _devx_002_push_v2_source_paths()
            | _arch_004g2_observability_source_paths()
            | _arch_004g2_closure_threshold_source_paths()
            | _data_gov_002_phase_b1_source_paths()
            | _trading_2458_retirement_source_paths()
            | _trading_2458_closeout_source_paths()
            | _data_gov_002_phase_b2_source_paths()
            | _devx_003_source_paths()
            | _data_gov_002c1_source_paths()
            | _data_gov_002c2_source_paths()
            | _trading_2461_source_paths()
            | _arch_004g2_paper_weekly_source_paths()
            | _ops_069_terminal_archive_source_paths()
            | _devx_004_source_paths()
            | current_superseded_paths
        )
        authority_section = DEVX_004_SECTION
    elif OPS_069_TERMINAL_ARCHIVE_SECTION in baseline:
        current_superseded_paths = _ops_069_terminal_archive_superseded_live_source_paths()
        assert (
            _ops_069_terminal_archive_prior_active_source_mismatches() == current_superseded_paths
        )
        superseded_paths = (
            _arch_005s4d_s2_all_superseded_live_source_paths()
            | _devx_trading_cleanup_source_paths()
            | _trading_2459_doc_closeout_source_paths()
            | _data_gov_002_source_paths()
            | _devx_002_source_paths()
            | _devx_002_push_v2_source_paths()
            | _arch_004g2_observability_source_paths()
            | _arch_004g2_closure_threshold_source_paths()
            | _data_gov_002_phase_b1_source_paths()
            | _trading_2458_retirement_source_paths()
            | _trading_2458_closeout_source_paths()
            | _data_gov_002_phase_b2_source_paths()
            | _devx_003_source_paths()
            | _data_gov_002c1_source_paths()
            | _data_gov_002c2_source_paths()
            | _trading_2461_source_paths()
            | _arch_004g2_paper_weekly_source_paths()
            | _ops_069_terminal_archive_source_paths()
            | current_superseded_paths
        )
        authority_section = OPS_069_TERMINAL_ARCHIVE_SECTION
    elif ARCH_004G2_PAPER_WEEKLY_SECTION in baseline:
        current_superseded_paths = _arch_004g2_paper_weekly_superseded_live_source_paths()
        assert _arch_004g2_paper_weekly_prior_active_source_mismatches() == current_superseded_paths
        superseded_paths = (
            _arch_005s4d_s2_all_superseded_live_source_paths()
            | _devx_trading_cleanup_source_paths()
            | _trading_2459_doc_closeout_source_paths()
            | _data_gov_002_source_paths()
            | _devx_002_source_paths()
            | _devx_002_push_v2_source_paths()
            | _arch_004g2_observability_source_paths()
            | _arch_004g2_closure_threshold_source_paths()
            | _data_gov_002_phase_b1_source_paths()
            | _trading_2458_retirement_source_paths()
            | _trading_2458_closeout_source_paths()
            | _data_gov_002_phase_b2_source_paths()
            | _devx_003_source_paths()
            | _data_gov_002c1_source_paths()
            | _data_gov_002c2_source_paths()
            | _trading_2461_source_paths()
            | _arch_004g2_paper_weekly_source_paths()
            | current_superseded_paths
        )
        authority_section = ARCH_004G2_PAPER_WEEKLY_SECTION
    elif TRADING_2461_SECTION in baseline:
        current_superseded_paths = _trading_2461_superseded_live_source_paths()
        assert _trading_2461_prior_active_source_mismatches() == current_superseded_paths
        superseded_paths = (
            _arch_005s4d_s2_all_superseded_live_source_paths()
            | _devx_trading_cleanup_source_paths()
            | _trading_2459_doc_closeout_source_paths()
            | _data_gov_002_source_paths()
            | _devx_002_source_paths()
            | _devx_002_push_v2_source_paths()
            | _arch_004g2_observability_source_paths()
            | _arch_004g2_closure_threshold_source_paths()
            | _data_gov_002_phase_b1_source_paths()
            | _trading_2458_retirement_source_paths()
            | _trading_2458_closeout_source_paths()
            | _data_gov_002_phase_b2_source_paths()
            | _devx_003_source_paths()
            | _data_gov_002c1_source_paths()
            | _data_gov_002c2_source_paths()
            | _trading_2461_source_paths()
            | current_superseded_paths
        )
        authority_section = TRADING_2461_SECTION
    elif DATA_GOV_002C2_SECTION in baseline:
        current_superseded_paths = _data_gov_002c2_superseded_live_source_paths()
        assert _data_gov_002c2_prior_active_source_mismatches() == current_superseded_paths
        superseded_paths = (
            _arch_005s4d_s2_all_superseded_live_source_paths()
            | _devx_trading_cleanup_source_paths()
            | _trading_2459_doc_closeout_source_paths()
            | _data_gov_002_source_paths()
            | _devx_002_source_paths()
            | _devx_002_push_v2_source_paths()
            | _arch_004g2_observability_source_paths()
            | _arch_004g2_closure_threshold_source_paths()
            | _data_gov_002_phase_b1_source_paths()
            | _trading_2458_retirement_source_paths()
            | _trading_2458_closeout_source_paths()
            | _data_gov_002_phase_b2_source_paths()
            | _devx_003_source_paths()
            | _data_gov_002c1_source_paths()
            | _data_gov_002c2_source_paths()
            | current_superseded_paths
        )
        authority_section = DATA_GOV_002C2_SECTION
    elif DATA_GOV_002C1_SECTION in baseline:
        current_superseded_paths = _data_gov_002c1_superseded_live_source_paths()
        assert _data_gov_002c1_prior_active_source_mismatches() == current_superseded_paths
        superseded_paths = (
            _arch_005s4d_s2_all_superseded_live_source_paths()
            | _devx_trading_cleanup_source_paths()
            | _trading_2459_doc_closeout_source_paths()
            | _data_gov_002_source_paths()
            | _devx_002_source_paths()
            | _devx_002_push_v2_source_paths()
            | _arch_004g2_observability_source_paths()
            | _arch_004g2_closure_threshold_source_paths()
            | _data_gov_002_phase_b1_source_paths()
            | _trading_2458_retirement_source_paths()
            | _trading_2458_closeout_source_paths()
            | _data_gov_002_phase_b2_source_paths()
            | _devx_003_source_paths()
            | _data_gov_002c1_source_paths()
            | current_superseded_paths
        )
        authority_section = DATA_GOV_002C1_SECTION
    elif DEVX_003_SECTION in baseline:
        current_superseded_paths = _devx_003_superseded_live_source_paths()
        assert _devx_003_prior_active_source_mismatches() == current_superseded_paths
        superseded_paths = (
            _arch_005s4d_s2_all_superseded_live_source_paths()
            | _devx_trading_cleanup_source_paths()
            | _trading_2459_doc_closeout_source_paths()
            | _data_gov_002_source_paths()
            | _devx_002_source_paths()
            | _devx_002_push_v2_source_paths()
            | _arch_004g2_observability_source_paths()
            | _arch_004g2_closure_threshold_source_paths()
            | _data_gov_002_phase_b1_source_paths()
            | _trading_2458_retirement_source_paths()
            | _trading_2458_closeout_source_paths()
            | _data_gov_002_phase_b2_source_paths()
            | _devx_003_source_paths()
            | current_superseded_paths
        )
        authority_section = DEVX_003_SECTION
    elif DATA_GOV_002_PHASE_B2_SECTION in baseline:
        current_superseded_paths = _data_gov_002_phase_b2_superseded_live_source_paths()
        assert _data_gov_002_phase_b2_prior_active_source_mismatches() == current_superseded_paths
        superseded_paths = (
            _arch_005s4d_s2_all_superseded_live_source_paths()
            | _devx_trading_cleanup_source_paths()
            | _trading_2459_doc_closeout_source_paths()
            | _data_gov_002_source_paths()
            | _devx_002_source_paths()
            | _devx_002_push_v2_source_paths()
            | _arch_004g2_observability_source_paths()
            | _arch_004g2_closure_threshold_source_paths()
            | _data_gov_002_phase_b1_source_paths()
            | _trading_2458_retirement_source_paths()
            | _trading_2458_closeout_source_paths()
            | _data_gov_002_phase_b2_source_paths()
            | current_superseded_paths
        )
        authority_section = DATA_GOV_002_PHASE_B2_SECTION
    elif TRADING_2458_CLOSEOUT_SECTION in baseline:
        current_superseded_paths = _trading_2458_closeout_superseded_live_source_paths()
        assert _trading_2458_closeout_prior_active_source_mismatches() == (current_superseded_paths)
        superseded_paths = (
            _arch_005s4d_s2_all_superseded_live_source_paths()
            | _devx_trading_cleanup_source_paths()
            | _trading_2459_doc_closeout_source_paths()
            | _data_gov_002_source_paths()
            | _devx_002_source_paths()
            | _devx_002_push_v2_source_paths()
            | _arch_004g2_observability_source_paths()
            | _arch_004g2_closure_threshold_source_paths()
            | _data_gov_002_phase_b1_source_paths()
            | _trading_2458_retirement_source_paths()
            | _trading_2458_closeout_source_paths()
            | current_superseded_paths
        )
        authority_section = TRADING_2458_CLOSEOUT_SECTION
    elif TRADING_2458_RETIREMENT_SECTION in baseline:
        current_superseded_paths = _trading_2458_retirement_superseded_live_source_paths()
        assert _trading_2458_retirement_prior_active_source_mismatches() == current_superseded_paths
        superseded_paths = (
            _arch_005s4d_s2_all_superseded_live_source_paths()
            | _devx_trading_cleanup_source_paths()
            | _trading_2459_doc_closeout_source_paths()
            | _data_gov_002_source_paths()
            | _devx_002_source_paths()
            | _devx_002_push_v2_source_paths()
            | _arch_004g2_observability_source_paths()
            | _arch_004g2_closure_threshold_source_paths()
            | _data_gov_002_phase_b1_source_paths()
            | _trading_2458_retirement_source_paths()
        )
        authority_section = TRADING_2458_RETIREMENT_SECTION
    elif DATA_GOV_002_PHASE_B1_SECTION in baseline:
        current_superseded_paths = _data_gov_002_phase_b1_superseded_live_source_paths()
        assert _data_gov_002_phase_b1_prior_active_source_mismatches() == current_superseded_paths
        superseded_paths = (
            _arch_005s4d_s2_all_superseded_live_source_paths()
            | _devx_trading_cleanup_source_paths()
            | _trading_2459_doc_closeout_source_paths()
            | _data_gov_002_source_paths()
            | _devx_002_source_paths()
            | _devx_002_push_v2_source_paths()
            | _arch_004g2_observability_source_paths()
            | _arch_004g2_closure_threshold_source_paths()
            | _data_gov_002_phase_b1_source_paths()
        )
        authority_section = DATA_GOV_002_PHASE_B1_SECTION
    elif ARCH_004G2_CLOSURE_THRESHOLD_SECTION in baseline:
        current_superseded_paths = _arch_004g2_closure_threshold_superseded_live_source_paths()
        assert (
            _arch_004g2_closure_threshold_prior_active_source_mismatches()
            == current_superseded_paths
        )
        superseded_paths = (
            _arch_005s4d_s2_all_superseded_live_source_paths()
            | _devx_trading_cleanup_source_paths()
            | _trading_2459_doc_closeout_source_paths()
            | _data_gov_002_source_paths()
            | _devx_002_source_paths()
            | _devx_002_push_v2_source_paths()
            | _arch_004g2_observability_source_paths()
            | _arch_004g2_closure_threshold_source_paths()
        )
        authority_section = ARCH_004G2_CLOSURE_THRESHOLD_SECTION
    elif ARCH_004G2_OBSERVABILITY_SECTION in baseline:
        current_superseded_paths = _arch_004g2_observability_superseded_live_source_paths()
        assert (
            _arch_004g2_observability_prior_active_source_mismatches() == current_superseded_paths
        )
        superseded_paths = (
            _arch_005s4d_s2_all_superseded_live_source_paths()
            | _devx_trading_cleanup_source_paths()
            | _trading_2459_doc_closeout_source_paths()
            | _data_gov_002_source_paths()
            | _devx_002_source_paths()
            | _devx_002_push_v2_source_paths()
            | _arch_004g2_observability_source_paths()
        )
        authority_section = ARCH_004G2_OBSERVABILITY_SECTION
    elif DEVX_002_PUSH_V2_SECTION in baseline:
        current_superseded_paths = _devx_002_push_v2_superseded_live_source_paths()
        assert _devx_002_push_v2_prior_active_source_mismatches() == current_superseded_paths
        superseded_paths = (
            _arch_005s4d_s2_all_superseded_live_source_paths()
            | _devx_trading_cleanup_source_paths()
            | _trading_2459_doc_closeout_source_paths()
            | _data_gov_002_source_paths()
            | _devx_002_source_paths()
            | _devx_002_push_v2_source_paths()
        )
        authority_section = DEVX_002_PUSH_V2_SECTION
    elif DEVX_002_SECTION in baseline:
        current_superseded_paths = _devx_002_superseded_live_source_paths()
        assert _devx_002_prior_active_source_mismatches() == current_superseded_paths
        superseded_paths = (
            _arch_005s4d_s2_all_superseded_live_source_paths()
            | _devx_trading_cleanup_source_paths()
            | _trading_2459_doc_closeout_source_paths()
            | _data_gov_002_source_paths()
            | _devx_002_source_paths()
        )
        authority_section = DEVX_002_SECTION
    elif DATA_GOV_002_SECTION in baseline:
        current_superseded_paths = _data_gov_002_superseded_live_source_paths()
        assert _data_gov_002_prior_active_source_mismatches() == current_superseded_paths
        superseded_paths = (
            _arch_005s4d_s2_all_superseded_live_source_paths()
            | _devx_trading_cleanup_source_paths()
            | _trading_2459_doc_closeout_source_paths()
            | _data_gov_002_source_paths()
        )
        authority_section = DATA_GOV_002_SECTION
    elif TRADING_2459_DOC_CLOSEOUT_SECTION in baseline:
        current_superseded_paths = _trading_2459_doc_closeout_superseded_live_source_paths()
        assert _trading_2459_doc_closeout_prior_active_source_mismatches() == (
            current_superseded_paths
        )
        superseded_paths = (
            _arch_005s4d_s2_all_superseded_live_source_paths()
            | _devx_trading_cleanup_source_paths()
            | _trading_2459_doc_closeout_source_paths()
        )
        authority_section = TRADING_2459_DOC_CLOSEOUT_SECTION
    elif DEVX_TRADING_CLEANUP_SECTION in baseline:
        current_superseded_paths = _devx_trading_cleanup_superseded_live_source_paths()
        assert _devx_trading_cleanup_prior_active_source_mismatches() == (current_superseded_paths)
        superseded_paths = (
            _arch_005s4d_s2_all_superseded_live_source_paths()
            | _devx_trading_cleanup_source_paths()
        )
        authority_section = DEVX_TRADING_CLEANUP_SECTION
    elif TRADING_2458_2460_INTEGRATION_SECTION in baseline:
        current_superseded_paths = _trading_2458_2460_integration_superseded_live_source_paths()
        assert _trading_2458_2460_integration_prior_active_source_mismatches() == (
            current_superseded_paths
        )
        superseded_paths = _arch_005s4d_s2_all_superseded_live_source_paths()
        authority_section = TRADING_2458_2460_INTEGRATION_SECTION
    elif ARCH_005S4D_S2_SECTION in baseline:
        current_superseded_paths = _arch_005s4d_s2_superseded_live_source_paths()
        assert _arch_005s4d_s2_prior_active_source_mismatches() == (current_superseded_paths)
        superseded_paths = _arch_005s4d_s2_all_superseded_live_source_paths()
        authority_section = ARCH_005S4D_S2_SECTION
    elif ARCH_005S4E_SECTION in baseline:
        superseded_paths = _arch_005s4e_superseded_live_source_paths()
        assert _arch_005s4e_prior_active_source_mismatches() == superseded_paths
        authority_section = ARCH_005S4E_SECTION
    elif OPS_070_SECTION in baseline:
        superseded_paths = _ops_070_superseded_live_source_paths()
        assert _ops_070_prior_active_source_mismatches() == superseded_paths
        authority_section = OPS_070_SECTION
    elif OPS_069_SECTION in baseline:
        superseded_paths = _ops_069_superseded_live_source_paths()
        assert _ops_069_prior_active_source_mismatches() == superseded_paths
        authority_section = OPS_069_SECTION
    elif D0B2B_SECTION in baseline:
        superseded_paths = _d0b2b_superseded_live_source_paths()
        assert _d0b2b_prior_active_source_mismatches() == superseded_paths
        authority_section = D0B2B_SECTION
    elif WAVE15_SECTION in baseline:
        superseded_paths = _wave15_superseded_live_source_paths()
        assert _wave15_prior_active_source_mismatches() == superseded_paths
        authority_section = WAVE15_SECTION
    elif ARCH_005S4D_SECTION in baseline:
        superseded_paths = _arch_005s4d_superseded_live_source_paths()
        assert _arch_005s4d_prior_active_source_mismatches() == superseded_paths
        authority_section = ARCH_005S4D_SECTION
    elif OPS_068_SECTION in baseline:
        superseded_paths = _ops_068_superseded_live_source_paths()
        assert _ops_068_prior_active_source_mismatches() == superseded_paths
        authority_section = OPS_068_SECTION
    elif OPS_067_SECTION in baseline:
        superseded_paths = _ops_067_superseded_live_source_paths()
        assert _ops_067_prior_active_source_mismatches() == superseded_paths
        authority_section = OPS_067_SECTION
    elif WAVE14_S2_SECTION in baseline:
        superseded_paths = _wave14_s2_superseded_live_source_paths()
        assert _wave14_s2_prior_active_source_mismatches() == superseded_paths
        authority_section = WAVE14_S2_SECTION
    elif WAVE14_S0_1_SECTION in baseline:
        superseded_paths = _wave14_s0_1_superseded_live_source_paths()
        assert _wave14_s0_1_prior_active_source_mismatches() == superseded_paths
        authority_section = WAVE14_S0_1_SECTION
    elif WAVE13_SECTION in baseline:
        superseded_paths = _wave13_superseded_live_source_paths()
        assert _wave13_prior_active_source_mismatches() == superseded_paths
        authority_section = WAVE13_SECTION
    else:
        # The Wave13 assertion below still fails closed while the EOF section is
        # being assembled. Keeping the prior authority here prevents unrelated
        # historical contract tests from obscuring that single missing boundary.
        superseded_paths = _wave12_prior_active_source_mismatches()
        authority_section = WAVE12_SECTION
    if str(source["path"]) in superseded_paths:
        return str(source["sha256"])
    if _source_matches_checkout(
        source,
        _normalization_migrations_before(authority_section),
    ):
        return str(source["sha256"])
    return _raw_source_sha256(source)


def test_checkout_hash_migration_equivalence_is_cross_checkout_and_fail_closed(
    tmp_path: Path,
) -> None:
    path = tmp_path / "source.py"
    mixed_bytes = b"first\r\nsecond\n"
    canonical_bytes = mixed_bytes.replace(b"\r\n", b"\n")
    prior_sha256 = hashlib.sha256(mixed_bytes).hexdigest()
    canonical_sha256 = hashlib.sha256(canonical_bytes).hexdigest()
    prior: dict[str, object] = {"path": str(path), "sha256": prior_sha256}
    migration: dict[str, object] = {
        "path": str(path),
        "sha256": canonical_sha256,
        "hash_normalization": "git_eol_lf",
        "previous_worktree_sha256": prior_sha256,
    }
    migrations: dict[str, tuple[dict[str, object], ...]] = {str(path): (migration,)}

    path.write_bytes(mixed_bytes)
    assert _source_matches_checkout(prior, migrations)
    path.write_bytes(canonical_bytes)
    assert _source_matches_checkout(prior, migrations)
    path.write_bytes(b"materially changed\n")
    assert not _source_matches_checkout(prior, migrations)


def test_arch_004_phase_g_in_progress_policy_keeps_freeze_and_preserves_safety() -> None:
    policy = safe_load_yaml_path(POLICY_PATH)
    baseline = _compatibility_baseline()
    wave14_s2 = baseline.get(WAVE14_S2_SECTION)
    wave14_s2_status = (
        wave14_s2["status"] if isinstance(wave14_s2, dict) else "VALIDATING_WAVE14_S2"
    )
    assert wave14_s2_status in {"VALIDATING_WAVE14_S2", "COMPLETE_WAVE14_S2"}
    assert policy["schema_version"] == "arch_004_refactor_policy.v1"
    assert policy["status"] == "phase_g_in_progress"
    assert policy["program"]["current_phase"] == "ARCH-004G"
    assert policy["program"]["current_phase_status"] == "IN_PROGRESS"
    assert policy["program"]["next_phase"] == "ARCH-004H"
    assert policy["program"]["next_phase_unblocked"] is False
    assert policy["feature_freeze"]["active"] is True
    assert "NEW_TASK_SHAPED_RESEARCH_MODULE" in policy["feature_freeze"]["forbidden_change_classes"]
    assert (
        "STRUCTURAL_REFACTOR_MIXED_WITH_STRATEGY_TUNING"
        in policy["feature_freeze"]["forbidden_change_classes"]
    )
    assert policy["behavior_preservation"]["strategy_logic_changed"] is False
    assert policy["behavior_preservation"]["threshold_changed"] is False
    enforcement = policy["semantic_kernel_enforcement"]
    assert enforcement["active_for_new_investment_facing_artifacts"] is True
    assert enforcement["schema_version"] == "research_evaluation_context.v1"
    assert enforcement["requested_range_may_substitute_actual_or_effective_range"] is False
    assert enforcement["legacy_flat_fields_require_exact_parity"] is True
    assert enforcement["reference_consumer"] == "first_layer_v2_effective_coverage_audit"
    assert enforcement["next_phase_unblocked"] is True
    assert policy["phase_b_completion"]["full_parallel_validation"]["passed"] == 5375
    assert policy["phase_b_completion"]["full_parallel_validation"]["failed"] == 0
    phase_c = policy["phase_c_execution"]
    assert phase_c["status"] == "COMPLETE"
    assert phase_c["stages"] == {
        "C1_pure_contracts": "COMPLETE",
        "C2_canonical_io_and_facades": "COMPLETE",
        "C3_typed_config_resolver_and_split": "COMPLETE",
        "C4_workflow_and_report_adapters": "COMPLETE",
        "C5_architecture_dependency_gate": "COMPLETE",
        "C6_reference_integration_and_closeout": "COMPLETE",
    }
    assert phase_c["new_direct_artifact_writer_allowed"] is False
    assert phase_c["domain_wide_migration_allowed"] is False
    assert phase_c["completion_validation"]["focused"]["passed"] == 120
    assert phase_c["completion_validation"]["architecture_gate"] == {
        "status": "PASS",
        "scanned_python_files": 770,
        "frozen_direct_writer_calls": 894,
        "current_direct_writer_calls": 893,
        "violations": 0,
    }
    assert phase_c["completion_validation"]["contract_validation"]["passed"] == 197
    assert phase_c["completion_validation"]["full_parallel_validation"]["passed"] == 5404
    assert phase_c["completion_validation"]["full_parallel_validation"]["failed"] == 0
    phase_d = policy["phase_d_execution"]
    assert phase_d["status"] == "COMPLETE"
    assert phase_d["reference_slice"] == "growth_tilt_candidate_family_closure"
    assert phase_d["stages"] == {
        "D1_characterization_and_typed_spec": "COMPLETE",
        "D2_generic_runner_and_plugin_interfaces": "COMPLETE",
        "D3_reference_plugin_and_legacy_facade": "COMPLETE",
        "D4_envelope_ledger_report_integration": "COMPLETE",
        "D5_parity_proof_and_closeout": "COMPLETE",
    }
    assert phase_d["new_task_id_python_module_allowed"] is False
    assert phase_d["strategy_or_research_conclusion_change_allowed"] is False
    assert phase_d["completion_validation"]["focused"]["passed"] == 77
    assert phase_d["completion_validation"]["architecture_gate"] == {
        "status": "PASS",
        "scanned_python_files": 775,
        "frozen_direct_writer_calls": 894,
        "current_direct_writer_calls": 893,
        "violations": 0,
    }
    assert phase_d["completion_validation"]["contract_validation"]["passed"] == 197
    assert phase_d["completion_validation"]["full_parallel_validation"]["passed"] == 5411
    assert phase_d["completion_validation"]["full_parallel_validation"]["failed"] == 0
    phase_e = policy["phase_e_execution"]
    assert phase_e["status"] == "COMPLETE"
    assert phase_e["stages"] == {
        "E1_ownership_policy_and_manifests": "COMPLETE",
        "E2_impact_selection": "COMPLETE",
        "E3_architecture_fitness": "COMPLETE",
        "E4_scaffold_and_aggregate_fragments": "COMPLETE",
        "E5_control_plane_integration_and_closeout": "COMPLETE",
    }
    assert phase_e["existing_aggregate_source_of_truth_changed"] is False
    assert phase_e["impact_selection_may_replace_full_validation"] is False
    assert phase_e["worker_may_edit_shared_aggregates"] is False
    completion = phase_e["completion_validation"]
    assert completion["generated_manifests"] == {
        "status": "PASS",
        "module_count": 777,
        "test_file_count": 1107,
        "orphan_count": 0,
        "specific_overlap_count": 0,
    }
    assert completion["aggregate_shadow"] == {
        "status": "SHADOW_COMPATIBILITY_PASS",
        "target_count": 3,
        "fragment_count": 4,
        "existing_source_of_truth_changed": False,
    }
    assert completion["architecture_fitness"]["status"] == "PASS"
    assert completion["architecture_fitness"]["violations"] == 0
    assert completion["architecture_tier"]["passed"] == 78
    assert completion["contract_validation"]["passed"] == 197
    assert completion["full_parallel_validation"]["passed"] == 5420
    assert completion["full_parallel_validation"]["failed"] == 0
    phase_f2 = policy["phase_f2_execution"]
    assert phase_f2["status"] == "COMPLETE"
    assert phase_f2["stages"] == {
        "F2_1_current_state_inventory_and_trace": "COMPLETE",
        "F2_2_authoritative_execution_chain_document": "COMPLETE",
        "F2_3_lifecycle_contract_and_review_boundary": "DOCUMENTED_BASELINE_COMPLETE",
        "F2_4_reference_integration_and_validation": "COMPLETE",
        "F2_5_generic_lifecycle_runtime_migration": "COMPLETE",
        "F2_5a_canonical_contract_and_state_machine": "COMPLETE",
        "F2_5b_legacy_campaign_compatibility_assessment": "COMPLETE",
        "F2_5c_optional_experiment_lifecycle_plugin": "COMPLETE",
        "F2_5d_growth_tilt_reference_sidecar_parity": "COMPLETE",
        "F2_5e_validation_and_closeout": "COMPLETE",
    }
    assert phase_f2["periodic_review_may_auto_tune"] is False
    assert phase_f2["result_visible_before_preregistration_freeze_allowed"] is False
    assert phase_f2["strategy_or_threshold_change_allowed_in_documentation_slice"] is False
    assert phase_f2["interim_evidence"]["market_regime_and_research_window_separated"] is True
    assert phase_f2["interim_evidence"]["production_effect"] == "none"
    assert phase_f2["documentation_validation"]["focused_docs_and_policy"]["passed"] == 23
    assert phase_f2["documentation_validation"]["architecture_fitness"]["passed"] == 80
    assert phase_f2["documentation_validation"]["contract_validation"]["passed"] == 197
    assert phase_f2["competing_campaign_runner_allowed"] is False
    runtime_validation = phase_f2["runtime_migration_completion_validation"]
    assert runtime_validation["focused"] == {"status": "PASS", "passed": 15}
    assert runtime_validation["scoped_mypy"] == {"status": "PASS"}
    assert runtime_validation["old_core_artifact_bytes_parity"] == "PASS"
    assert runtime_validation["legacy_campaign_missing_binding_behavior"] == "BLOCKED"
    assert runtime_validation["architecture_fitness"]["passed"] == 88
    assert runtime_validation["contract_validation"]["passed"] == 197
    assert runtime_validation["full_parallel"]["passed"] == 5430
    assert runtime_validation["full_parallel"]["failed"] == 0
    phase_f1 = policy["phase_f1_execution"]
    assert phase_f1["status"] == "COMPLETE"
    assert phase_f1["stages"] == {
        "F1_1_inventory_due_contract_and_compatibility_adapter": "COMPLETE",
        "F1_2_shadow_plan_and_daily_parity": "COMPLETE",
        "F1_3_lock_retry_idempotency_and_resume": "COMPLETE",
        "F1_4_daily_executor_adapter_cut_in": "COMPLETE",
        "F1_5_non_daily_controlled_due_dispatch": "COMPLETE",
        "F1_6_validation_and_closeout": "COMPLETE",
    }
    assert phase_f1["scheduled_task_inventory"] == {
        "daily": 37,
        "non_daily": 41,
        "total": 78,
    }
    assert phase_f1["unified_external_trigger"] == "aits ops daily-run"
    assert phase_f1["additional_external_scheduler_entry_allowed"] is False
    assert phase_f1["non_daily_automatic_dispatch_enabled"] is False
    assert phase_f1["non_daily_manual_dispatch_enabled"] is True
    assert phase_f1["legacy_dispatch_enabled_by_shadow_adapter"] is False
    runtime_control = phase_f1["runtime_control"]
    assert runtime_control["policy_id"] == "operations_runtime_control_v1"
    assert runtime_control["deterministic_idempotency_key"] is True
    assert runtime_control["concurrent_workflow_date_lock"] == "BLOCKED"
    assert runtime_control["stale_lock_recovery"] == "EXPIRED_ONLY"
    assert runtime_control["duplicate_completed_trigger"] == "ALREADY_COMPLETE"
    assert runtime_control["step_attempt_budget_from_workflow_spec"] is True
    assert runtime_control["non_idempotent_partial_resume"] == "BLOCKED"
    assert runtime_control["legacy_daily_executor_cut_in_enabled"] is True
    assert runtime_control["execution_ledger_schema"] == "run_ledger.v1"
    assert runtime_control["validate_data_failure_blocks_downstream"] is True
    assert runtime_control["non_daily_dispatch_enabled"] is True
    assert phase_f1["compatibility_findings"]["periodic_task_plan_count"] == 41
    assert phase_f1["compatibility_findings"]["periodic_automatic_command_dispatch"] is False
    assert phase_f1["compatibility_findings"]["trading_day_daily_plan"] == "PASS"
    assert phase_f1["compatibility_findings"]["closed_market_daily_plan"] == "PASS"
    assert phase_f1["compatibility_findings"]["conditional_step_contract"] == {
        "official_policy_sources": "closed_market_only"
    }
    assert phase_f1["compatibility_findings"]["additive_shadow_artifact_emission"] == ("COMPLETE")
    phase_f3 = policy["phase_f3_execution"]
    assert phase_f3["status"] == "COMPLETE"
    assert phase_f3["stages"] == {
        "F3_1_inventory_policy_and_characterization": "COMPLETE",
        "F3_2_pure_contracts_and_typed_catalog": "COMPLETE",
        "F3_3_owner_daily_brief_reference_cut_in": "COMPLETE",
        "F3_4_research_review_pack": "COMPLETE",
        "F3_5_audit_index_and_generated_fragments": "COMPLETE",
        "F3_6_cut_in_parity_and_closeout": "COMPLETE",
    }
    assert phase_f3["owner_daily_brief_core_section_limit"] == 10
    assert phase_f3["owner_queue_requires_due_and_actionable"] is True
    assert phase_f3["reporting_layer_may_recompute_investment_conclusion"] is False
    assert phase_f3["legacy_unclassified_disposition"] == ("AUDIT_INDEX_LIMITED_UNCLASSIFIED")
    assert phase_f3["next_phase_unblocked"] is True
    assert phase_f3["completion_validation"]["full_parallel"] == {
        "status": "PASS",
        "passed": 5494,
        "failed": 0,
        "warnings": 642,
        "elapsed_seconds": 923.4,
        "runtime_artifact": (
            "outputs/validation_runtime/full_20260711T040642Z/test_runtime_summary.json"
        ),
    }
    phase_g = policy["phase_g_execution"]
    assert phase_g["status"] == "IN_PROGRESS"
    assert phase_g["stages"]["G0_inventory_deprecation_policy_and_removal_gate"] == ("COMPLETE")
    assert phase_g["stages"]["G1_shared_platform_helper_migration"] == "COMPLETE"
    assert phase_g["stages"]["G2_interfaces_and_etf_cli_migration"] == "COMPLETE"
    assert phase_g["stages"]["G3_reporting_native_migration"] == (
        "BASELINE_DONE_BOUNDED_SLICE_CLOSE_READY"
    )
    assert phase_g["stages"]["G4_operations_consumer_migration"] == (
        "BASELINE_DONE_FIRST_CONSUMER_CADENCE_PENDING"
    )
    assert phase_g["permanent_dual_track_allowed"] is False
    assert phase_g["runtime_removal_allowed_in_g0"] is False
    assert phase_g["investment_semantics_change_allowed"] is False
    assert phase_g["historical_artifact_deletion_allowed"] is False
    coordination_wave = phase_g["current_coordination_wave"]
    assert coordination_wave["wave_id"] == "WAVE15_D0B3_G4B_G3_CLOSE"
    assert coordination_wave["current_phase"] == "WAVE15_S3_FORMAL_EXIT_COMPLETE"
    assert coordination_wave["status"] == "BASELINE_DONE_OWNER_NEXT_SLICE_REQUIRED"
    assert coordination_wave["compatibility_status"] == "WAVE15_S3_COMPLETE"
    assert coordination_wave["task_id"] == ("ARCH-004W15_D0B3_G4B_G3_CLOSE_PARALLEL_READINESS")
    assert coordination_wave["authorization_commit"] == ("3030114be1c07b71eab5af2d8cbf4f54325cb2ef")
    assert coordination_wave["readiness_carrier_commit"] == (
        "7ec6fd713b0e676607e38522be36b8e4d6c20d55"
    )
    assert coordination_wave["readiness_carrier_sha256"] == (
        "50ba37193bf3aef67439e16a9cf3dd3183bb2e20df84fc63ea075293923a2e51"
    )
    assert coordination_wave["domain_focused_passed"] == 73
    assert coordination_wave["shared_combined_focused_passed"] == 152
    assert (
        coordination_wave["source_wave_closeout_commit"]
        == "e2da21894ea8e8921a86c6c1b48d7b191f0f142c"
    )
    assert coordination_wave["source_wave_tree"] == "73ba7a3830cbc47ccb6dbfb3488eeed5431653c2"
    assert (
        coordination_wave["wave14_s0_carrier_commit"] == "39a3ea7306a3937beda835020df4d8419c1cbbdf"
    )
    assert coordination_wave["historical_base_commit"] == WAVE12_CLOSEOUT_COMMIT
    assert coordination_wave["application_commit"] == WAVE13_BASE_COMMIT
    assert (
        coordination_wave["dry_run_manifest_id"] == "gov_006_decision_manifest_3fb5f2a038eca2361179"
    )
    assert (
        coordination_wave["applied_closeout_id"] == "gov_006_applied_closeout_bdbfd433a72d5349ead9"
    )
    assert coordination_wave["applied_closeout_path"] == WAVE13_APPLIED_CLOSEOUT_PATH.as_posix()
    assert (
        coordination_wave["applied_closeout_schema"]
        == "gov_006_portfolio_normalization_applied_closeout.v1"
    )
    assert coordination_wave["applied_closeout_status"] == "APPLIED_CLOSEOUT_READY"
    assert coordination_wave["applied_closeout_validation"] == "PASS"
    assert (
        coordination_wave["applied_closeout_canonical_sha256"]
        == WAVE13_APPLIED_CLOSEOUT_CANONICAL_SHA256
    )
    assert coordination_wave["decision_count"] == 30
    assert coordination_wave["done_count"] == 18
    assert coordination_wave["dropped_count"] == 12
    assert coordination_wave["active_task_count_before"] == 435
    assert coordination_wave["active_task_count_after"] == 405
    assert coordination_wave["completed_task_count_before"] == 457
    assert coordination_wave["completed_task_count_after"] == 487
    assert coordination_wave["total_task_count"] == 892
    assert coordination_wave["automatic_apply_allowed"] is False
    assert coordination_wave["task_source_of_truth"] == "LEGACY_MARKDOWN_ONLY"
    assert coordination_wave["task_source_of_truth_cutover"] is False
    assert coordination_wave["next_wave"] == {
        "wave_id": "WAVE15_D0B3_G4B_G3_CLOSE",
        "dispatch_allowed": False,
        "data_task": "DATA-GOV-001_D0B3",
        "reporting_task": "ARCH-004G3_REPORTING_NATIVE_MIGRATION",
        "authorized_consumers": ["daily_score_daily"],
        "other_consumer_authorization_allowed": False,
        "g5_unblocked": False,
        "max_active_domain_workers": 2,
        "shared_paths_coordinator_only": True,
        "g4c_cadence_observation_async": True,
        "prior_g2_5_rehearsal_is_dispatch_authority": False,
        "final_head_manifest_required": True,
        "s0_contract_readiness_status": "PASS",
        "next_slice_unblocked": False,
    }
    assert coordination_wave["strategy_logic_changed"] is False
    assert coordination_wave["data_or_runtime_changed"] is True
    assert coordination_wave["broker_action"] == "none"
    assert coordination_wave["production_effect"] == "none"
    assert phase_g["g0_evidence"] == {
        "policy_path": "config/architecture/arch_004g_deprecation_policy.yaml",
        "inventory_path": "inputs/architecture/arch_004g_deprecation_inventory.yaml",
        "module_count": 795,
        "test_file_count": 1112,
        "priority_target_count": 9,
        "active_target_count": 6,
        "deprecated_target_count": 3,
        "removal_ready_count": 0,
        "direct_writer_baseline": 894,
        "direct_writer_current": 893,
        "dynamic_strategy_wrapper_count": 99,
        "matching_research_quality_implementation_count": 48,
        "runtime_removal_performed": False,
    }
    assert phase_g["g0_validation"]["focused"] == {"status": "PASS", "passed": 6}
    assert phase_g["g0_validation"]["architecture_fitness"]["passed"] == 156
    assert phase_g["g0_validation"]["contract_validation"]["passed"] == 203
    assert phase_g["g1_slices"] == {
        "G1_1_three_governance_module_writer_implementation_migration": "COMPLETE",
        "G1_2_internal_caller_migration_and_private_wrapper_removal": "COMPLETE",
        "G1_3_next_shared_helper_family": "COMPLETE",
        "G1_3a_trading_engine_summary_writer_migration": "COMPLETE",
        "G1_3b_next_shared_helper_family": "COMPLETE",
        "G1_3b_notification_retry_writer_migration": "COMPLETE",
        "G1_3c_next_shared_helper_family": "COMPLETE",
        "G1_3c_checksum_helper_migration": "COMPLETE",
        "G1_3d_runtime_metadata_helper_inventory": "COMPLETE",
        "G1_3d_pit_replay_observe_only_metadata_migration": "COMPLETE",
        "G1_3e_data_quality_and_safety_helper_inventory": "COMPLETE",
        "G1_3e_growth_tilt_data_quality_gate_migration": "COMPLETE",
    }
    assert phase_g["g1_current_evidence"]["direct_writer_before"] == 893
    assert phase_g["g1_current_evidence"]["direct_writer_after"] == 861
    assert phase_g["g1_current_evidence"]["direct_writer_reduction"] == 32
    assert phase_g["g1_current_evidence"]["private_compatibility_wrappers_remaining"] == 0
    assert phase_g["g1_current_evidence"]["private_compatibility_wrappers_removed"] == 32
    assert phase_g["g1_current_evidence"]["internal_callers_using_canonical_writer"] is True
    assert phase_g["g1_current_evidence"]["runtime_removal_performed"] is False
    assert phase_g["g1_first_family_validation"]["focused"] == {
        "status": "PASS",
        "passed": 29,
    }
    assert phase_g["g1_first_family_validation"]["architecture_fitness"]["passed"] == 159
    assert (
        phase_g["g1_first_family_validation"]["architecture_fitness"]["current_direct_writer_calls"]
        == 887
    )
    assert phase_g["g1_second_family_plan"]["direct_writer_after"] == 877
    assert phase_g["g1_second_family_plan"]["removed_private_writer_count"] == 10
    assert phase_g["g1_second_family_plan"]["json_sort_keys"] is False
    assert phase_g["g1_second_family_plan"]["focused_validation"] == {
        "status": "PASS",
        "passed": 95,
    }
    assert phase_g["g1_second_family_plan"]["architecture_fitness"]["passed"] == 161
    assert phase_g["g1_third_family_plan"]["direct_writer_after"] == 861
    assert phase_g["g1_third_family_plan"]["removed_private_writer_count"] == 16
    assert phase_g["g1_third_family_plan"]["focused_validation"] == {
        "status": "PASS",
        "passed": 139,
    }
    assert phase_g["g1_third_family_plan"]["architecture_fitness"]["passed"] == 162
    assert phase_g["g1_current_evidence"]["canonical_checksum_helper"] == "sha256_path"
    assert phase_g["g1_current_evidence"]["private_checksum_helpers_removed"] == 8
    assert phase_g["g1_fourth_family_plan"]["canonical_caller_count"] == 13
    assert phase_g["g1_fourth_family_plan"]["direct_writer_count_unchanged"] == 861
    assert phase_g["g1_fourth_family_plan"]["focused_validation"] == {
        "status": "PASS",
        "passed": 155,
    }
    assert phase_g["g1_fourth_family_plan"]["architecture_fitness"]["passed"] == 164
    assert phase_g["g1_current_evidence"]["private_runtime_metadata_helpers_removed"] == 10
    assert (
        phase_g["g1_current_evidence"]["canonical_runtime_metadata_helper"]
        == "with_pit_replay_observe_only_runtime_metadata"
    )
    assert phase_g["g1_fifth_family_plan"]["inventory_ast_field_group_count"] == 14
    assert phase_g["g1_fifth_family_plan"]["canonical_safety_false_field_count"] == 39
    assert phase_g["g1_fifth_family_plan"]["private_metadata_helper_remaining_count"] == 0
    assert phase_g["g1_fifth_family_plan"]["focused_validation"] == {
        "status": "PASS",
        "passed": 182,
    }
    assert phase_g["g1_fifth_family_plan"]["architecture_fitness"]["passed"] == 166
    assert phase_g["g1_current_evidence"]["canonical_data_quality_gate"] == (
        "run_growth_tilt_data_quality_gate"
    )
    assert phase_g["g1_current_evidence"]["private_data_quality_gate_helpers_removed"] == 15
    assert phase_g["g1_current_evidence"]["private_secondary_price_helpers_removed"] == 15
    assert phase_g["g1_sixth_family_plan"]["direct_validate_data_cache_call_required"] is True
    assert phase_g["g1_sixth_family_plan"]["exception_downgrade_allowed"] is False
    assert phase_g["g1_sixth_family_plan"]["private_gate_helper_remaining_count"] == 0
    assert phase_g["g1_sixth_family_plan"]["focused_validation"] == {
        "status": "PASS",
        "passed": 242,
    }
    assert phase_g["g1_sixth_family_plan"]["architecture_fitness"]["passed"] == 168
    assert phase_g["stages"]["G1_shared_platform_helper_migration"] == "COMPLETE"
    assert phase_g["stages"]["G2_interfaces_and_etf_cli_migration"] == "COMPLETE"
    assert phase_g["g1_closeout"] == {
        "status": "COMPLETE",
        "canonical_family_count": 6,
        "private_helper_removal_count": 80,
        "direct_writer_before": 893,
        "direct_writer_after": 861,
        "direct_writer_reduction": 32,
        "dynamic_wrapper_lines_before_g1_3d": 89805,
        "dynamic_wrapper_lines_after_g1_3e": 88315,
        "dynamic_wrapper_line_reduction": 1490,
        "dynamic_wrapper_functions_before_g1_3d": 2154,
        "dynamic_wrapper_functions_after_g1_3e": 2114,
        "dynamic_wrapper_function_reduction": 40,
        "safety_assertion_groups_audited": 29,
        "unsafe_cross_semantic_abstraction_avoided": True,
        "legacy_callers_for_selected_families": 0,
        "architecture_fitness": {
            "status": "PASS",
            "passed": 168,
            "current_direct_writer_calls": 861,
            "violation_count": 0,
            "runtime_artifact": (
                "outputs/validation_runtime/"
                "architecture-fitness_20260711T064010Z/test_runtime_summary.json"
            ),
        },
        "production_effect": "none",
    }
    assert phase_g["g2_current_plan"]["status"] == "COMPLETE"
    assert phase_g["g2_current_plan"]["implementation_started_in_g1_closeout_slice"] is False
    assert phase_g["g2_current_plan"]["stages"] == {
        "G2_1_command_registry_and_golden_contract": "COMPLETE",
        "G2_2_registration_shell_and_shared_parameters": "COMPLETE",
        "G2_3_data_operations_reporting_groups": "COMPLETE",
        "G2_4_research_shadow_portfolio_groups": "COMPLETE",
        "G2_5_freeze_deprecation_and_closeout": "COMPLETE",
    }
    assert phase_g["g2_current_plan"]["closeout_status"] == ("COMPLETE_FORMAL_GATES_PASS")
    assert phase_g["g2_current_plan"]["next_wave"] == {
        "status": "IN_PROGRESS_W12_S2_SHARED_INTEGRATION",
        "architecture_stage": "G4_operations_consumer_migration",
        "paired_task": "DATA-GOV-001_D0B_CANONICAL_DQ_EVIDENCE",
        "requirement_path": (
            "docs/requirements/"
            "ARCH-004G4_D0B_Shared_DQ_Preflight_and_Periodic_Consumer_Migration.md"
        ),
        "readiness_policy_path": ("config/architecture/arch_004_wave12_g4_d0b_readiness.yaml"),
        "readiness_artifact_path": (
            "inputs/architecture/arch_004_wave12_g4_d0b_parallel_readiness.json"
        ),
        "max_active_domain_workers": 2,
        "g3_dispatch_allowed": False,
        "g5_dispatch_allowed": False,
        "production_effect": "none",
    }
    assert phase_g["g2_current_plan"]["g2_1_contract"]["leaf_command_count"] == 993
    assert phase_g["g2_current_plan"]["g2_1_contract"]["duplicate_path_count"] == 0
    assert phase_g["g2_current_plan"]["g2_1_contract"]["callback_location_in_contract"] is False
    assert phase_g["g2_current_plan"]["g2_1_contract"]["architecture_fitness"]["passed"] == 171
    g2_2 = phase_g["g2_current_plan"]["g2_2_registration_shell"]
    assert g2_2["typer_apps_moved"] == 291
    assert g2_2["add_typer_relationships_moved"] == 290
    assert g2_2["legacy_typer_app_definitions_remaining"] == 0
    assert g2_2["legacy_add_typer_relationships_remaining"] == 0
    assert g2_2["legacy_root_line_reduction"] == 1559
    assert g2_2["top_level_functions_unchanged"] == 1049
    assert g2_2["command_decorators_unchanged"] == 993
    assert g2_2["callback_functions_moved"] == 0
    assert g2_2["focused_validation"] == {"status": "PASS", "passed": 341, "file_count": 25}
    assert g2_2["contract_characterization"] == {"status": "PASS", "passed": 6}
    assert g2_2["architecture_fitness"]["passed"] == 174
    assert g2_2["runtime_behavior_changed"] is False
    g2_3_first = phase_g["g2_current_plan"]["g2_3_first_slice"]
    assert g2_3_first["callback_count"] == 3
    assert g2_3_first["legacy_callback_definitions_remaining"] == 0
    assert g2_3_first["legacy_helper_definitions_remaining"] == 0
    assert g2_3_first["compatibility_aliases_using_canonical_callbacks"] is True
    assert g2_3_first["legacy_root_top_level_functions_after"] == 1043
    assert g2_3_first["legacy_root_command_decorators_after"] == 990
    assert g2_3_first["focused_validation"] == {"status": "PASS", "passed": 72}
    assert g2_3_first["architecture_fitness"]["passed"] == 175
    g2_3_second = phase_g["g2_current_plan"]["g2_3_second_slice"]
    assert g2_3_second["callback_count"] == 3
    assert g2_3_second["legacy_callback_definitions_remaining"] == 0
    assert g2_3_second["direct_dispatch_using_canonical_callbacks"] is True
    assert g2_3_second["shared_helper_added"] is False
    assert g2_3_second["legacy_root_top_level_functions_after"] == 1040
    assert g2_3_second["legacy_root_command_decorators_after"] == 987
    assert g2_3_second["focused_validation"] == {"status": "PASS", "passed": 44}
    assert g2_3_second["architecture_fitness"]["passed"] == 176
    g2_3_third = phase_g["g2_current_plan"]["g2_3_third_slice"]
    assert g2_3_third["callback_count"] == 3
    assert g2_3_third["legacy_callback_definitions_remaining"] == 0
    assert g2_3_third["legacy_parser_definitions_remaining"] == 0
    assert g2_3_third["legacy_directory_constant_definitions_remaining"] == 0
    assert g2_3_third["direct_dispatch_using_canonical_callbacks"] is True
    assert g2_3_third["legacy_root_top_level_functions_after"] == 1036
    assert g2_3_third["legacy_root_command_decorators_after"] == 984
    assert g2_3_third["focused_validation"] == {"status": "PASS", "passed": 111}
    assert g2_3_third["architecture_fitness"]["passed"] == 177
    g2_3_fourth = phase_g["g2_current_plan"]["g2_3_fourth_slice"]
    assert g2_3_fourth["callback_count"] == 3
    assert g2_3_fourth["legacy_callback_definitions_remaining"] == 0
    assert g2_3_fourth["legacy_strategy_evidence_imports_remaining"] == 0
    assert g2_3_fourth["direct_dispatch_using_canonical_callbacks"] is True
    assert g2_3_fourth["legacy_root_top_level_functions_after"] == 1033
    assert g2_3_fourth["legacy_root_command_decorators_after"] == 981
    assert g2_3_fourth["focused_validation"] == {"status": "PASS", "passed": 44}
    assert g2_3_fourth["architecture_fitness"]["passed"] == 178
    g2_3_fifth = phase_g["g2_current_plan"]["g2_3_fifth_slice"]
    assert g2_3_fifth["callback_count"] == 4
    assert g2_3_fifth["legacy_callback_definitions_remaining"] == 0
    assert g2_3_fifth["legacy_helper_definitions_remaining"] == 0
    assert g2_3_fifth["legacy_weekly_review_imports_remaining"] == 0
    assert g2_3_fifth["legacy_callers_using_canonical_date_helper"] is True
    assert g2_3_fifth["legacy_root_top_level_functions_after"] == 1027
    assert g2_3_fifth["legacy_root_command_decorators_after"] == 977
    assert g2_3_fifth["focused_validation"] == {"status": "PASS", "passed": 84}
    assert g2_3_fifth["architecture_fitness"]["passed"] == 179
    g2_3_sixth = phase_g["g2_current_plan"]["g2_3_sixth_slice"]
    assert g2_3_sixth["callback_count"] == 4
    assert g2_3_sixth["legacy_callback_definitions_remaining"] == 0
    assert g2_3_sixth["legacy_helper_definitions_remaining"] == 0
    assert g2_3_sixth["legacy_parameter_review_imports_remaining"] == 0
    assert g2_3_sixth["canonical_date_helper_reused"] == "weekly_review_date"
    assert g2_3_sixth["legacy_root_top_level_functions_after"] == 1022
    assert g2_3_sixth["legacy_root_command_decorators_after"] == 973
    assert g2_3_sixth["focused_validation"] == {"status": "PASS", "passed": 65}
    assert g2_3_sixth["architecture_fitness"]["passed"] == 180
    g2_3_seventh = phase_g["g2_current_plan"]["g2_3_seventh_slice"]
    assert g2_3_seventh["callback_count"] == 3
    assert g2_3_seventh["legacy_callback_definitions_remaining"] == 0
    assert g2_3_seventh["legacy_helper_definitions_remaining"] == 0
    assert g2_3_seventh["legacy_satellite_attribution_imports_remaining"] == 0
    assert str(g2_3_seventh["default_ai_regime_start_unchanged"]) == "2022-12-01"
    assert g2_3_seventh["invalid_price_fixture_fail_closed"] is True
    assert g2_3_seventh["legacy_root_top_level_functions_after"] == 1017
    assert g2_3_seventh["legacy_root_command_decorators_after"] == 970
    assert g2_3_seventh["focused_validation"] == {"status": "PASS", "passed": 78}
    assert g2_3_seventh["architecture_fitness"]["passed"] == 181
    g2_3_eighth = phase_g["g2_current_plan"]["g2_3_eighth_slice"]
    assert g2_3_eighth["callback_count"] == 3
    assert g2_3_eighth["legacy_callback_definitions_remaining"] == 0
    assert g2_3_eighth["legacy_dq_helper_definitions_remaining"] == 0
    assert g2_3_eighth["legacy_trend_calibration_imports_remaining"] == 0
    assert g2_3_eighth["dq_gate_precedes_price_and_feature_build"] is True
    assert g2_3_eighth["dq_failure_fixture_fail_closed"] is True
    assert g2_3_eighth["legacy_root_top_level_functions_after"] == 1010
    assert g2_3_eighth["legacy_root_command_decorators_after"] == 967
    assert g2_3_eighth["focused_validation"] == {"status": "PASS", "passed": 54}
    assert g2_3_eighth["architecture_fitness"]["passed"] == 182
    g2_3_closeout = phase_g["g2_current_plan"]["g2_3_closeout"]
    assert g2_3_closeout["status"] == "COMPLETE"
    assert g2_3_closeout["slice_count"] == 8
    assert g2_3_closeout["canonical_module_count"] == 9
    assert g2_3_closeout["migrated_callback_count"] == 26
    assert g2_3_closeout["migrated_helper_count"] == 13
    assert g2_3_closeout["legacy_selected_callback_definitions_remaining"] == 0
    assert g2_3_closeout["legacy_selected_helper_definitions_remaining"] == 0
    assert g2_3_closeout["legacy_selected_domain_imports_remaining"] == 0
    assert g2_3_closeout["legacy_root_line_reduction"] == 1605
    assert g2_3_closeout["legacy_root_function_reduction"] == 39
    assert g2_3_closeout["legacy_root_command_decorator_reduction"] == 26
    assert g2_3_closeout["direct_writer_calls_after"] == 860
    assert g2_3_closeout["focused_closeout_validation"] == {"status": "PASS", "passed": 15}
    assert g2_3_closeout["architecture_fitness"]["passed"] == 183
    g2_4 = phase_g["g2_current_plan"]["g2_4_current_plan"]
    assert g2_4["status"] == "COMPLETE"
    assert g2_4["first_slice"] == "baseline_review"
    assert g2_4["implementation_started_in_g2_3_closeout"] is False
    assert g2_4["callback_count"] == 7
    assert g2_4["owner_decision_semantics_sensitive"] is True
    assert g2_4["production_runtime_state_mutation_allowed"] is False
    assert g2_4["governance_journal_write_allowed"] is True
    assert g2_4["journal_link_optional"] is True
    g2_4_first = phase_g["g2_current_plan"]["g2_4_first_slice"]
    assert g2_4_first["status"] == "COMPLETE"
    assert g2_4_first["callback_count"] == 7
    assert g2_4_first["legacy_callback_definitions_remaining"] == 0
    assert g2_4_first["legacy_helper_definitions_remaining"] == 0
    assert g2_4_first["legacy_baseline_review_imports_remaining"] == 0
    assert g2_4_first["governance_journal_write_allowed"] is True
    assert g2_4_first["production_runtime_state_mutation_allowed"] is False
    assert g2_4_first["proposal_is_draft_only"] is True
    assert g2_4_first["direct_writer_calls_after"] == 858
    assert g2_4_first["legacy_root_lines_after"] == 33950
    assert g2_4_first["legacy_root_top_level_functions_after"] == 1002
    assert g2_4_first["legacy_root_command_decorators_after"] == 960
    assert g2_4_first["focused_validation"] == {"status": "PASS", "passed": 36}
    assert g2_4_first["architecture_fitness"]["passed"] == 184
    g2_4_second = phase_g["g2_current_plan"]["g2_4_second_slice"]
    assert g2_4_second["status"] == "COMPLETE"
    assert g2_4_second["callback_count"] == 4
    assert g2_4_second["legacy_callback_definitions_remaining"] == 0
    assert g2_4_second["legacy_shadow_ready_review_imports_remaining"] == 0
    assert g2_4_second["candidate_governance_artifact_write_allowed"] is True
    assert g2_4_second["decision_journal_write_allowed"] is False
    assert g2_4_second["approved_enrollment_artifact_write_allowed"] is True
    assert g2_4_second["automatic_paper_shadow_execution_allowed"] is False
    assert g2_4_second["runtime_registry_mutation_allowed"] is False
    assert g2_4_second["direct_writer_calls_after"] == 858
    assert g2_4_second["legacy_root_lines_after"] == 33656
    assert g2_4_second["legacy_root_top_level_functions_after"] == 998
    assert g2_4_second["legacy_root_command_decorators_after"] == 956
    assert g2_4_second["focused_validation"] == {"status": "PASS", "passed": 21}
    assert g2_4_second["architecture_fitness"]["passed"] == 185
    g2_4_third = phase_g["g2_current_plan"]["g2_4_third_slice"]
    assert g2_4_third["status"] == "COMPLETE"
    assert g2_4_third["callback_count"] == 3
    assert g2_4_third["legacy_callback_definitions_remaining"] == 0
    assert g2_4_third["legacy_helper_definitions_remaining"] == 0
    assert g2_4_third["candidate_decision_artifact_write_allowed"] is True
    assert g2_4_third["candidate_policy_registry_artifact_write_allowed"] is True
    assert g2_4_third["runtime_registry_mutation_allowed"] is False
    assert g2_4_third["official_target_weights_mutation_allowed"] is False
    assert g2_4_third["production_rebalance_allowed"] is False
    assert g2_4_third["direct_writer_calls_after"] == 858
    assert g2_4_third["legacy_root_lines_after"] == 33405
    assert g2_4_third["legacy_root_top_level_functions_after"] == 993
    assert g2_4_third["legacy_root_command_decorators_after"] == 953
    assert g2_4_third["focused_validation"] == {"status": "PASS", "passed": 21}
    assert g2_4_third["architecture_fitness"]["passed"] == 186
    g2_4_fourth = phase_g["g2_current_plan"]["g2_4_fourth_slice"]
    assert g2_4_fourth["status"] == "COMPLETE"
    assert g2_4_fourth["callback_count"] == 3
    assert g2_4_fourth["legacy_callback_definitions_remaining"] == 0
    assert g2_4_fourth["research_cache_write_allowed"] is True
    assert g2_4_fourth["candidate_artifact_write_allowed"] is True
    assert g2_4_fourth["automatic_candidate_promotion_allowed"] is False
    assert g2_4_fourth["auto_enrollment_without_owner_approval_allowed"] is False
    assert g2_4_fourth["official_target_weights_mutation_allowed"] is False
    assert g2_4_fourth["validation_uses_canonical_cli_owner"] is True
    assert g2_4_fourth["direct_writer_calls_after"] == 858
    assert g2_4_fourth["legacy_root_lines_after"] == 33199
    assert g2_4_fourth["legacy_root_top_level_functions_after"] == 990
    assert g2_4_fourth["legacy_root_command_decorators_after"] == 950
    assert g2_4_fourth["focused_validation"] == {"status": "PASS", "passed": 24}
    assert g2_4_fourth["architecture_fitness"]["passed"] == 187
    g2_4_fifth = phase_g["g2_current_plan"]["g2_4_fifth_slice"]
    assert g2_4_fifth["status"] == "COMPLETE"
    assert g2_4_fifth["callback_count"] == 2
    assert g2_4_fifth["legacy_callback_definitions_remaining"] == 0
    assert g2_4_fifth["cached_dq_gate_precedes_standard_price_validation"] is True
    assert g2_4_fifth["standard_price_validation_precedes_robustness"] is True
    assert g2_4_fifth["dq_failure_fail_closed"] is True
    assert g2_4_fifth["latest_mode_read_only"] is True
    assert g2_4_fifth["shadow_enrollment_allowed"] is False
    assert g2_4_fifth["direct_writer_calls_after"] == 858
    assert g2_4_fifth["legacy_root_lines_after"] == 32979
    assert g2_4_fifth["legacy_root_top_level_functions_after"] == 988
    assert g2_4_fifth["legacy_root_command_decorators_after"] == 948
    assert g2_4_fifth["focused_validation"] == {"status": "PASS", "passed": 24}
    assert g2_4_fifth["architecture_fitness"]["passed"] == 188
    g2_4_sixth = phase_g["g2_current_plan"]["g2_4_sixth_slice"]
    assert g2_4_sixth["status"] == "COMPLETE"
    assert g2_4_sixth["callback_count"] == 3
    assert g2_4_sixth["legacy_callback_definitions_remaining"] == 0
    assert g2_4_sixth["cached_dq_gate_precedes_standard_price_validation"] is True
    assert g2_4_sixth["standard_price_validation_precedes_rescue_comparison"] is True
    assert g2_4_sixth["dq_failure_fail_closed"] is True
    assert g2_4_sixth["bounded_rescue_candidate_artifact_write_allowed"] is True
    assert g2_4_sixth["automatic_candidate_enrollment_allowed"] is False
    assert g2_4_sixth["owner_approval_executed"] is False
    assert g2_4_sixth["automatic_candidate_promotion_allowed"] is False
    assert g2_4_sixth["official_target_weights_mutation_allowed"] is False
    assert g2_4_sixth["broker_action_allowed"] is False
    assert g2_4_sixth["validation_uses_canonical_cli_owner"] is True
    assert g2_4_sixth["direct_writer_calls_after"] == 858
    assert g2_4_sixth["legacy_root_lines_after"] == 32713
    assert g2_4_sixth["legacy_root_top_level_functions_after"] == 985
    assert g2_4_sixth["legacy_root_command_decorators_after"] == 945
    assert g2_4_sixth["focused_validation"] == {"status": "PASS", "passed": 25}
    assert g2_4_sixth["architecture_fitness"]["passed"] == 189
    g2_4_seventh = phase_g["g2_current_plan"]["g2_4_seventh_slice"]
    assert g2_4_seventh["status"] == "COMPLETE"
    assert g2_4_seventh["callback_count"] == 3
    assert g2_4_seventh["legacy_callback_definitions_remaining"] == 0
    assert g2_4_seventh["existing_rescue_and_robustness_artifacts_read_only"] is True
    assert g2_4_seventh["market_backtest_reexecution_allowed"] is False
    assert g2_4_seventh["mandatory_source_missing_fail_closed"] is True
    assert g2_4_seventh["optional_shadow_missing_is_warning"] is True
    assert g2_4_seventh["latest_report_mode_read_only"] is True
    assert g2_4_seventh["review_artifact_write_allowed"] is True
    assert g2_4_seventh["owner_approval_executed"] is False
    assert g2_4_seventh["shadow_enrollment_allowed"] is False
    assert g2_4_seventh["automatic_candidate_promotion_allowed"] is False
    assert g2_4_seventh["official_target_weights_mutation_allowed"] is False
    assert g2_4_seventh["validation_uses_canonical_cli_owner"] is True
    assert g2_4_seventh["direct_writer_calls_after"] == 858
    assert g2_4_seventh["legacy_root_lines_after"] == 32546
    assert g2_4_seventh["legacy_root_top_level_functions_after"] == 982
    assert g2_4_seventh["legacy_root_command_decorators_after"] == 942
    assert g2_4_seventh["focused_validation"] == {"status": "PASS", "passed": 27}
    assert g2_4_seventh["architecture_fitness"]["passed"] == 190
    g2_4_eighth = phase_g["g2_current_plan"]["g2_4_eighth_slice"]
    assert g2_4_eighth["status"] == "COMPLETE"
    assert g2_4_eighth["callback_count"] == 3
    assert g2_4_eighth["legacy_callback_definitions_remaining"] == 0
    assert g2_4_eighth["remaining_dynamic_v3_commands_stay_legacy_owned"] is True
    assert g2_4_eighth["v0_4_review_package_read_only"] is True
    assert g2_4_eighth["base_candidate_must_match_reviewed_policy"] is True
    assert g2_4_eighth["latest_report_mode_read_only"] is True
    assert g2_4_eighth["candidate_evaluation_artifact_write_allowed"] is True
    assert g2_4_eighth["owner_approval_executed"] is False
    assert g2_4_eighth["shadow_enrollment_allowed"] is False
    assert g2_4_eighth["automatic_candidate_promotion_allowed"] is False
    assert g2_4_eighth["official_target_weights_mutation_allowed"] is False
    assert g2_4_eighth["validation_checks_canonical_and_legacy_owners"] is True
    assert g2_4_eighth["direct_writer_calls_after"] == 858
    assert g2_4_eighth["legacy_root_lines_after"] == 32389
    assert g2_4_eighth["legacy_root_top_level_functions_after"] == 979
    assert g2_4_eighth["legacy_root_command_decorators_after"] == 939
    assert g2_4_eighth["focused_validation"] == {"status": "PASS", "passed": 28}
    assert g2_4_eighth["architecture_fitness"]["passed"] == 191
    g2_4_ninth = phase_g["g2_current_plan"]["g2_4_ninth_slice"]
    assert g2_4_ninth["status"] == "COMPLETE"
    assert g2_4_ninth["callback_count"] == 3
    assert g2_4_ninth["legacy_callback_definitions_remaining"] == 0
    assert g2_4_ninth["cached_dq_gate_precedes_standard_price_validation"] is True
    assert g2_4_ninth["standard_price_validation_precedes_pit_evaluation"] is True
    assert g2_4_ninth["dq_failure_fail_closed"] is True
    assert g2_4_ninth["pit_no_lookahead_preserved"] is True
    assert g2_4_ninth["requested_range_and_ai_regime_separate"] is True
    assert g2_4_ninth["pre_regime_primary_conclusion_allowed"] is False
    assert g2_4_ninth["latest_report_mode_read_only"] is True
    assert g2_4_ninth["promotion_gate_executes_promotion"] is False
    assert g2_4_ninth["owner_approval_executed"] is False
    assert g2_4_ninth["shadow_enrollment_allowed"] is False
    assert g2_4_ninth["official_target_weights_mutation_allowed"] is False
    assert g2_4_ninth["validation_uses_canonical_cli_owner"] is True
    assert g2_4_ninth["direct_writer_calls_after"] == 858
    assert g2_4_ninth["legacy_root_lines_after"] == 32166
    assert g2_4_ninth["legacy_root_top_level_functions_after"] == 976
    assert g2_4_ninth["legacy_root_command_decorators_after"] == 936
    assert g2_4_ninth["focused_validation"] == {"status": "PASS", "passed": 28}
    assert g2_4_ninth["architecture_fitness"]["passed"] == 192
    g2_4_tenth = phase_g["g2_current_plan"]["g2_4_tenth_slice"]
    assert g2_4_tenth["status"] == "COMPLETE"
    assert g2_4_tenth["callback_count"] == 3
    assert g2_4_tenth["legacy_callback_definitions_remaining"] == 0
    assert g2_4_tenth["real_evaluation_lineage_loaded_before_dq"] is True
    assert g2_4_tenth["dq_as_of_inherits_explicit_or_source_end"] is True
    assert g2_4_tenth["cached_dq_gate_precedes_standard_price_validation"] is True
    assert g2_4_tenth["standard_price_validation_precedes_pit_attribution"] is True
    assert g2_4_tenth["dq_failure_fail_closed"] is True
    assert g2_4_tenth["source_artifact_mutation_allowed"] is False
    assert g2_4_tenth["requested_range_and_ai_regime_separate"] is True
    assert g2_4_tenth["latest_report_mode_read_only"] is True
    assert g2_4_tenth["review_or_recommendation_executes_promotion"] is False
    assert g2_4_tenth["owner_approval_executed"] is False
    assert g2_4_tenth["shadow_enrollment_allowed"] is False
    assert g2_4_tenth["official_target_weights_mutation_allowed"] is False
    assert g2_4_tenth["validation_uses_canonical_cli_owner"] is True
    assert g2_4_tenth["direct_writer_calls_after"] == 858
    assert g2_4_tenth["legacy_root_lines_after"] == 31876
    assert g2_4_tenth["legacy_root_top_level_functions_after"] == 973
    assert g2_4_tenth["legacy_root_command_decorators_after"] == 933
    assert g2_4_tenth["focused_validation"] == {"status": "PASS", "passed": 28}
    assert g2_4_tenth["architecture_fitness"]["passed"] == 193
    g2_4_eleventh = phase_g["g2_current_plan"]["g2_4_eleventh_slice"]
    assert g2_4_eleventh["status"] == "COMPLETE"
    assert g2_4_eleventh["callback_count"] == 2
    assert g2_4_eleventh["legacy_callback_definitions_remaining"] == 0
    assert g2_4_eleventh["reviewed_config_read_only"] is True
    assert g2_4_eleventh["schema_and_safety_validation_only"] is True
    assert g2_4_eleventh["stable_candidate_id_enumeration"] is True
    assert g2_4_eleventh["evaluator_execution_allowed"] is False
    assert g2_4_eleventh["backtest_or_pit_execution_allowed"] is False
    assert g2_4_eleventh["fresh_data_quality_gate_required"] is False
    assert g2_4_eleventh["runtime_artifact_write_allowed"] is False
    assert g2_4_eleventh["production_candidate_generated"] is False
    assert g2_4_eleventh["preview_limit_changes_candidate_universe"] is False
    assert g2_4_eleventh["runtime_commands_deferred_to_next_slice"] is True
    assert g2_4_eleventh["direct_writer_calls_after"] == 858
    assert g2_4_eleventh["legacy_root_lines_after"] == 31833
    assert g2_4_eleventh["legacy_root_top_level_functions_after"] == 971
    assert g2_4_eleventh["legacy_root_command_decorators_after"] == 931
    assert g2_4_eleventh["focused_validation"] == {"status": "PASS", "passed": 43}
    assert g2_4_eleventh["architecture_fitness"]["passed"] == 194
    g2_4_twelfth = phase_g["g2_current_plan"]["g2_4_twelfth_slice"]
    assert g2_4_twelfth["status"] == "COMPLETE"
    assert g2_4_twelfth["callback_count"] == 8
    assert g2_4_twelfth["helper_count"] == 1
    assert g2_4_twelfth["legacy_callback_definitions_remaining"] == 0
    assert g2_4_twelfth["legacy_helper_definitions_remaining"] == 0
    assert g2_4_twelfth["profile_list_and_validate_read_only"] is True
    assert g2_4_twelfth["research_runtime_artifact_write_allowed"] is True
    assert g2_4_twelfth["real_evaluator_uses_dq_and_pit_path"] is True
    assert g2_4_twelfth["tiny_fixture_not_for_investment_decision"] is True
    assert g2_4_twelfth["resume_evaluator_mode_mutation_allowed"] is False
    assert g2_4_twelfth["resume_worker_override_recorded"] is True
    assert g2_4_twelfth["derived_leaderboard_or_report_materialization_allowed"] is True
    assert g2_4_twelfth["production_candidate_generated"] is False
    assert g2_4_twelfth["automatic_candidate_promotion_allowed"] is False
    assert g2_4_twelfth["shadow_enrollment_allowed"] is False
    assert g2_4_twelfth["official_target_weights_mutation_allowed"] is False
    assert g2_4_twelfth["direct_writer_calls_after"] == 858
    assert g2_4_twelfth["legacy_root_lines_after"] == 31548
    assert g2_4_twelfth["legacy_root_top_level_functions_after"] == 962
    assert g2_4_twelfth["legacy_root_command_decorators_after"] == 923
    assert g2_4_twelfth["focused_validation"] == {"status": "PASS", "passed": 44}
    assert g2_4_twelfth["architecture_fitness"]["passed"] == 195
    g2_4_thirteenth = phase_g["g2_current_plan"]["g2_4_thirteenth_slice"]
    assert g2_4_thirteenth["status"] == "COMPLETE"
    assert g2_4_thirteenth["callback_count"] == 3
    assert g2_4_thirteenth["legacy_callback_definitions_remaining"] == 0
    assert g2_4_thirteenth["explicit_as_of_and_end_required"] is True
    assert g2_4_thirteenth["same_validate_data_path_used"] is True
    assert g2_4_thirteenth["failed_quality_evidence_write_allowed"] is True
    assert g2_4_thirteenth["dq_failure_may_be_reported_as_pass"] is False
    assert g2_4_thirteenth["checksum_and_provenance_artifacts_required"] is True
    assert g2_4_thirteenth["pit_coverage_and_data_gap_artifacts_required"] is True
    assert g2_4_thirteenth["latest_report_mode_read_only"] is True
    assert g2_4_thirteenth["validation_requires_dq_non_fail"] is True
    assert g2_4_thirteenth["cache_or_download_manifest_mutation_allowed"] is False
    assert g2_4_thirteenth["candidate_generation_allowed"] is False
    assert g2_4_thirteenth["backtest_or_pit_evaluation_allowed"] is False
    assert g2_4_thirteenth["direct_writer_calls_after"] == 858
    assert g2_4_thirteenth["legacy_root_lines_after"] == 31464
    assert g2_4_thirteenth["legacy_root_top_level_functions_after"] == 959
    assert g2_4_thirteenth["legacy_root_command_decorators_after"] == 920
    assert g2_4_thirteenth["focused_validation"] == {"status": "PASS", "passed": 45}
    assert g2_4_thirteenth["architecture_fitness"]["passed"] == 196
    g2_4_fourteenth = phase_g["g2_current_plan"]["g2_4_fourteenth_slice"]
    assert g2_4_fourteenth["status"] == "COMPLETE"
    assert g2_4_fourteenth["callback_count"] == 3
    assert g2_4_fourteenth["legacy_callback_definitions_remaining"] == 0
    assert g2_4_fourteenth["inspect_and_validate_read_only"] is True
    assert g2_4_fourteenth["manifest_repair_requires_explicit_mode"] is True
    assert g2_4_fourteenth["supported_repair_mode"] == "reconstruct-from-cache"
    assert g2_4_fourteenth["repair_requires_all_cache_files"] is True
    assert g2_4_fourteenth["reconstructed_rows_use_current_file_checksums"] is True
    assert g2_4_fourteenth["reconstructed_source_label_required"] == (
        "cache_rebuild_from_existing_file"
    )
    assert g2_4_fourteenth["reconstructed_provenance_status_required"] == ("RECONSTRUCTED_MANIFEST")
    assert g2_4_fourteenth["original_download_event_unavailable_disclosed"] is True
    assert g2_4_fourteenth["provider_or_endpoint_invention_allowed"] is False
    assert g2_4_fourteenth["reconstructed_may_claim_primary_download_provenance"] is False
    assert g2_4_fourteenth["candidate_or_backtest_execution_allowed"] is False
    assert g2_4_fourteenth["direct_writer_calls_after"] == 858
    assert g2_4_fourteenth["legacy_root_lines_after"] == 31379
    assert g2_4_fourteenth["legacy_root_top_level_functions_after"] == 956
    assert g2_4_fourteenth["legacy_root_command_decorators_after"] == 917
    assert g2_4_fourteenth["focused_validation"] == {"status": "PASS", "passed": 46}
    assert g2_4_fourteenth["architecture_fitness"]["passed"] == 197
    g2_4_fifteenth = phase_g["g2_current_plan"]["g2_4_fifteenth_slice"]
    assert g2_4_fifteenth["status"] in {"VALIDATING", "COMPLETE"}
    assert g2_4_fifteenth["callback_count"] == 4
    assert g2_4_fifteenth["legacy_callback_definitions_remaining"] == 0
    assert g2_4_fifteenth["as_of_option_semantics"] == "requested_start"
    assert g2_4_fifteenth["configured_requested_actual_ranges_distinct"] is True
    assert str(g2_4_fifteenth["ai_regime_default_start"]) == "2022-12-01"
    assert g2_4_fifteenth["pre_regime_actual_range_inherently_invalid"] is False
    assert g2_4_fifteenth["research_window_role_automatically_validated"] is False
    assert g2_4_fifteenth["missing_or_invalid_range_fails_closed"] is True
    assert g2_4_fifteenth["late_start_or_early_end_blocks_promotion"] is True
    assert g2_4_fifteenth["report_and_inspect_read_only"] is True
    assert g2_4_fifteenth["candidate_or_backtest_execution_allowed"] is False
    assert g2_4_fifteenth["direct_writer_calls_after"] == 858
    assert g2_4_fifteenth["legacy_root_lines_after"] == 31277
    assert g2_4_fifteenth["legacy_root_top_level_functions_after"] == 952
    assert g2_4_fifteenth["legacy_root_command_decorators_after"] == 913
    assert g2_4_fifteenth["focused_validation"] == {"status": "PASS", "passed": 48}
    assert g2_4_fifteenth["architecture_fitness"]["passed"] == 198
    g2_4_sixteenth = phase_g["g2_current_plan"]["g2_4_sixteenth_slice"]
    assert g2_4_sixteenth["status"] in {"VALIDATING", "COMPLETE"}
    assert g2_4_sixteenth["callback_count"] == 3
    assert g2_4_sixteenth["legacy_callback_definitions_remaining"] == 0
    assert g2_4_sixteenth["deterministic_base_plus_ofat_pairs"] is True
    assert g2_4_sixteenth["grid_prefix_may_prove_parameter_effect"] is False
    assert g2_4_sixteenth["parameter_effect_uses_matched_pairs_only"] is True
    assert g2_4_sixteenth["declared_mapping_alone_proves_consumption"] is False
    assert g2_4_sixteenth["independent_parameter_effect_artifact_required"] is True
    assert g2_4_sixteenth["insufficient_pair_status"] == ("INSUFFICIENT_MATCHED_PAIR_EVIDENCE")
    assert g2_4_sixteenth["insufficient_pair_coverage_audit_status"] == "INCOMPLETE"
    assert g2_4_sixteenth["validation_fails_on_incomplete_pair_coverage"] is True
    assert g2_4_sixteenth["real_evaluation_uses_dq_and_pit_context"] is True
    assert g2_4_sixteenth["latest_report_mode_read_only"] is True
    assert g2_4_sixteenth["automatic_candidate_promotion_allowed"] is False
    assert g2_4_sixteenth["direct_writer_calls_after"] == 858
    assert g2_4_sixteenth["legacy_root_lines_after"] == 31183
    assert g2_4_sixteenth["legacy_root_top_level_functions_after"] == 949
    assert g2_4_sixteenth["legacy_root_command_decorators_after"] == 910
    assert g2_4_sixteenth["focused_validation"] == {"status": "PASS", "passed": 53}
    assert g2_4_sixteenth["architecture_fitness"]["passed"] == 199
    assert policy["safety_boundary"] == {
        "research_only": True,
        "architecture_governance_only": True,
        "production_effect": "none",
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "data_quality_gate_bypass_allowed": False,
    }


def test_arch_004_reconciliation_reuses_predecessors_and_unblocks_phase_b() -> None:
    payload = safe_load_yaml_path(RECONCILIATION_PATH)

    assert payload["status"] == "ARCH_004_PREDECESSOR_RECONCILIATION_COMPLETE"
    assert payload["historical_baseline"]["closeout_status"] == "ENGINEERING_CLOSEOUT_READY"
    current = payload["current_control_plane_evidence"]
    assert current["engineering_surface_inventory"]["surface_count"] == 3812
    assert current["artifact_lifecycle"]["validation_status"] == "FAIL"
    assert current["engineering_stage_b"]["validation_status"] == "FAIL"
    assert current["canonical_system"]["doctor_status"] == "FAIL"
    assert current["reader_brief_consistency"]["native_template_gap_count"] == 1634

    dispositions = {
        row["capability_id"]: row["disposition"] for row in payload["predecessor_capabilities"]
    }
    assert dispositions["root_cli_modularization"] == "REUSE"
    assert dispositions["artifact_ref_and_workflow_types"] == "EXTEND"
    assert dispositions["reader_brief_effective_view_model"] == "CARRY_FORWARD"
    assert dispositions["clean_clone_release_acceptance"] == "REUSE"
    assert payload["phase_a_gate"] == {
        "predecessor_reconciliation_complete": True,
        "current_control_plane_evidence_captured": True,
        "full_parallel_validation_baseline_recorded": True,
        "existing_failures_have_root_cause_and_linked_task": True,
        "semantic_glossary_frozen": True,
        "command_and_artifact_compatibility_baseline_frozen": True,
        "shared_file_ownership_frozen": True,
        "clean_handoff_or_attributable_isolation_proven": True,
        "phase_b_unblocked": True,
    }
    assert current["full_parallel_validation"]["exit_validation"]["status"] == "PASS"
    assert current["full_parallel_validation"]["exit_validation"]["failed"] == 0
    assert payload["safety_boundary"]["waivers_added"] == 0


def test_arch_004_semantic_glossary_separates_regime_and_research_window() -> None:
    glossary = safe_load_yaml_path(GLOSSARY_PATH)
    terms = glossary["canonical_terms"]

    assert glossary["status"] == "frozen_phase_a"
    assert str(terms["anchor_event_date"]["canonical_value"]) == "2022-11-30"
    assert str(terms["market_regime_start"]["canonical_value"]) == "2022-12-01"
    assert terms["primary_research_window_id"]["canonical_value"] == ("exact_three_asset_validated")
    assert str(terms["primary_research_window_start"]["canonical_value"]) == ("2021-02-22")
    assert terms["primary_research_window_id"]["not_global_for_unrelated_asset_families"]
    assert glossary["resolution_rules"]["conflict_behavior"] == "FAIL_CLOSED"
    assert glossary["resolution_rules"]["implicit_date_aliasing_allowed"] is False
    assert (
        glossary["reporting_contract"]["market_regime_start_may_substitute_research_window_start"]
        is False
    )
    implementation = glossary["implementation_boundary"]
    assert implementation["runtime_enforcement_implemented"] is True
    assert implementation["context_schema_version"] == "research_evaluation_context.v1"
    assert implementation["existing_artifact_migration_status"] == "GOVERNED_WAVES_PENDING"


def test_arch_004_g2_5_wave11_is_append_only_current_hash_authority() -> None:
    _assert_current_wave11_historical_prefix_immutable()
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    wave11 = baseline[WAVE11_SECTION]

    assert (
        str(
            baseline["explicit_cli_adapter_contracts"]["date_range_kwargs"]["missing_start_default"]
        )
        == "2022-12-01"
    )
    assert wave11["status"] == "COMPLETE_WAVE11"
    assert wave11["boundary_id"] == "ARCH-004G2.5_WAVE11"
    assert wave11["task_ids"] == [
        "ARCH-004G2_PARALLEL_READINESS_GATE",
        "DATA-GOV-001_UNIFIED_DATA_FOUNDATION_GOVERNANCE",
        "GOV-006_ACTIVE_TASK_PORTFOLIO_NORMALIZATION",
    ]
    window = wave11["research_window"]
    assert window == {
        "active_default_start": "2021-02-22",
        "active_default_scope": "QQQ_SGOV_TQQQ_PRIMARY_RESEARCH_AND_BACKTESTS",
        "legacy_2022_12_01_active_default": False,
        "legacy_2022_12_01_required_comparator": False,
        "legacy_2022_12_01_minimum_allowed_start": False,
        "legacy_2022_12_01_role": ("IMMUTABLE_HISTORICAL_OR_EXPLICIT_SENSITIVITY_EVIDENCE_ONLY"),
    }
    assert wave11["prior_sections_immutability"] == {
        "source_commit": WAVE11_BASE_COMMIT,
        "repository_path": WAVE11_BASELINE_REPOSITORY_PATH,
        "git_blob_sha1": WAVE11_BASELINE_GIT_BLOB,
        "raw_byte_count": WAVE11_HISTORICAL_PREFIX_BYTE_COUNT,
        "raw_sha256": WAVE11_HISTORICAL_PREFIX_SHA256,
        "append_offset": WAVE11_HISTORICAL_PREFIX_BYTE_COUNT,
        "current_section_must_be_eof": True,
    }

    components = wave11["components"]
    g2_5 = components["g2_5_parallel_readiness"]
    assert g2_5["current_status"] == "COMPLETE_FORMAL_GATES_PASS"
    assert g2_5["evidence_status"] == "PASS"
    assert g2_5["max_active_domain_workers"] == 2
    assert g2_5["coordinator_count"] == 1
    assert g2_5["dispatch_allowed"] is False
    assert g2_5["lease_acquisition_allowed"] is False
    assert g2_5["automatic_merge_allowed"] is False
    assert g2_5["next_domain_slice_started"] is False
    d0a = components["data_d0a_immutable_publish"]
    assert d0a["current_status"] == "D0A_COMPLETE_FORMAL_GATES_PASS"
    assert d0a["atomic_independent_review_p0_p1_finding_count"] == 0
    assert d0a["contract_independent_review_p0_p1_finding_count"] == 0
    assert d0a["focused_passed"] == 48
    assert d0a["focused_skipped"] == 1
    assert d0a["dq_execution_provenance_verified"] is False
    assert d0a["consumer_cutover_allowed"] is False
    assert d0a["store_acl_verified"] is False
    assert d0a["crash_durability_verified"] is False
    assert d0a["cross_process_crash_power_loss_durability_claimed"] is False
    gov_n0 = components["gov_006_n0_normalization"]
    assert gov_n0["current_status"] == "COMPLETE_FORMAL_GATES_PASS"
    assert gov_n0["manifest_validation_status"] == "PASS"
    assert gov_n0["decision_count"] == 30
    assert gov_n0["automatic_apply_allowed"] is False
    assert gov_n0["task_register_mutated_by_n0"] is False

    expected_superseded_paths = {
        ".github/workflows/ci.yml",
        "config/architecture/arch_004_refactor_policy.yaml",
        "docs/architecture/dual_lane_development_operating_model.md",
        "docs/artifact_catalog.md",
        "docs/operations/operations_runbook.md",
        "docs/requirements/ARCH-004G2_Parallel_Readiness_Gate.md",
        "docs/requirements/ARCH-004G_Domain_Migration_and_Subtraction.md",
        "docs/requirements/ARCH-004_Post_2438N_System_Architecture_Refactor_Program.md",
        "docs/runbooks/scheduled_task_orchestration.md",
        "docs/system_flow.md",
        "docs/task_register.md",
        "docs/task_register_completed.md",
        "inputs/architecture/arch_004e_aggregate_shadow_index.yaml",
        "inputs/architecture/arch_004e_architecture_fitness.yaml",
        "inputs/architecture/arch_004e_module_manifest.yaml",
        "inputs/architecture/arch_004e_test_manifest.yaml",
        "inputs/architecture/arch_004g_deprecation_inventory.yaml",
        "inputs/architecture/arch_005_task_registry_baseline.yaml",
        "inputs/architecture/arch_005_task_shadow_index.yaml",
        "src/ai_trading_system/platform/architecture/bootstrap_handoff.py",
        "tests/test_arch_004_refactor_policy.py",
        "tests/test_arch_004g_deprecation.py",
        "tests/test_arch_005_bootstrap_handoff.py",
        "tests/test_trading2452_architecture_contract.py",
    }
    assert len(expected_superseded_paths) == 24
    assert set(wave11["superseded_live_source_paths"]) == expected_superseded_paths
    assert expected_superseded_paths <= _docs_gov_prior_active_source_mismatches()
    assert wave11["supersession"] == {
        "superseded_by_phase": "ARCH-004G2.5_WAVE11",
        "scope": "ALL_PRIOR_NON_HISTORICAL_SOURCE_RECORDS_FOR_EACH_LISTED_PATH",
        "historical_hashes_rewritten": False,
        "current_hash_authority": "phase_arch_004_g2_5_wave11.sources",
    }

    sources = wave11["sources"]
    source_paths = [str(source["path"]) for source in sources]
    assert len(source_paths) == 55
    assert len(source_paths) == len(set(source_paths))
    assert expected_superseded_paths <= set(source_paths)
    assert {
        "config/architecture/arch_004_g2_5_readiness.yaml",
        "config/governance/gov_006_wave1_normalization.yaml",
        "inputs/architecture/arch_004g2_5_parallel_readiness.json",
        "inputs/architecture/arch_005_bootstrap_validation_bundle.json",
        "inputs/governance/gov_006_wave1_decision_manifest.json",
        "src/ai_trading_system/data/immutable_publish.py",
        "src/ai_trading_system/platform/architecture/arch_004_g2_5_readiness.py",
        "src/ai_trading_system/platform/architecture/task_portfolio_normalization.py",
        "tests/test_arch_004_g2_5_readiness.py",
        "tests/test_external_request_cache_revalidation_coordination.py",
        "tests/test_gov_006_task_portfolio_normalization.py",
        "tests/test_immutable_data_publish.py",
    } <= set(source_paths)
    assert "inputs/architecture/arch_004_compatibility_baseline.yaml" not in source_paths
    assert "docs/research/growth_tilt_owner_diagnosis_pack.md" not in source_paths
    assert all(
        isinstance(source["sha256"], str)
        and len(source["sha256"]) == 64
        and int(source["sha256"], 16) >= 0
        for source in sources
    )
    for source in sources:
        assert _source_sha256_at_commit(source, DOCS_GOV_BASE_COMMIT) == source["sha256"], source[
            "path"
        ]
    validation = wave11["validation"]
    full_validation = validation["full_validation"]
    assert full_validation["attempts_append_only"] is True
    assert full_validation["initial_failure_may_be_removed_or_overwritten"] is False
    assert full_validation["executed_attempts_may_be_removed_or_overwritten"] is False
    assert full_validation["complete_status_requires_replacement_pass"] is True
    attempts = full_validation["attempts"]
    _assert_wave11_full_attempt_chain(attempts)
    assert len(attempts) == 3
    assert attempts[0] == {
        "attempt_id": "INITIAL_FORMAL_GATE_20260722T183541Z",
        "role": "INITIAL_FORMAL_GATE",
        "status": "FAIL",
        "passed": 6701,
        "failed": 2,
        "skipped": 3,
        "warnings": 643,
        "pytest_elapsed_seconds": 1108.72,
        "runner_elapsed_seconds": 1109.83,
        "workers": 16,
        "artifact": {
            "path": "outputs/validation_runtime/full_20260722T183541Z/test_runtime_summary.json",
            "sha256": "f9490a13b31637b0910ca0c0e14a00ed78cff88c643c8621532d4374d99919fc",
            "size_bytes": 26429,
            "local_file_required": False,
        },
        "cause": "LEGACY_TRADING2452_2453_TEST_DID_NOT_APPLY_WAVE11_SUPERSESSION",
        "durable_fix_path": "tests/test_trading2452_architecture_contract.py",
        "production_effect": "none",
    }
    assert attempts[1] == {
        "attempt_id": "FAILURE_FIX_REPLACEMENT_20260722T193219Z",
        "role": "FAILURE_FIX_REPLACEMENT",
        "replaces_attempt_id": "INITIAL_FORMAL_GATE_20260722T183541Z",
        "status": "FAIL",
        "passed": 6706,
        "failed": 1,
        "skipped": 3,
        "warnings": 642,
        "pytest_elapsed_seconds": 1110.81,
        "runner_elapsed_seconds": 1111.95,
        "workers": 16,
        "artifact": {
            "path": "outputs/validation_runtime/full_20260722T193219Z/test_runtime_summary.json",
            "sha256": "7d79db367e464f29975db1311120f8a8f0899db5b546f8d19b7600439015c8c8",
            "size_bytes": 26836,
            "local_file_required": False,
        },
        "cause": "RACE_DEPENDENT_TEST_ORACLE_REJECTED_VALID_LATE_CONTENDER_DOUBLE_CHECK_REUSE",
        "durable_fix_path": "tests/test_external_request_cache_revalidation_coordination.py",
        "production_effect": "none",
    }
    replacement_attempt = attempts[2]
    assert replacement_attempt == {
        "attempt_id": "FAILURE_FIX_REPLACEMENT_2_20260722T201357Z",
        "role": "FAILURE_FIX_REPLACEMENT",
        "replaces_attempt_id": "FAILURE_FIX_REPLACEMENT_20260722T193219Z",
        "status": "PASS",
        "passed": 6710,
        "failed": 0,
        "skipped": 3,
        "warnings": 643,
        "pytest_elapsed_seconds": 1105.37,
        "runner_elapsed_seconds": 1106.6,
        "workers": 16,
        "artifact": {
            "path": ("outputs/validation_runtime/full_20260722T201357Z/test_runtime_summary.json"),
            "sha256": "6e324617d82455e9af185aa80fa8f237054fc4a69d17e63853c928f19a606546",
            "size_bytes": 26538,
            "local_file_required": False,
        },
        "production_effect": "none",
    }
    assert wave11["source_hash_status"] == "FINAL_TRACKED_STATE_FRESH"
    assert validation["focused"] == {
        "status": "PASS",
        "passed": 183,
        "failed": 0,
        "skipped": 1,
        "workers": 16,
    }
    assert validation["architecture_fitness"]["status"] == "PASS"
    assert validation["architecture_fitness"]["passed"] == 482
    assert validation["contract_validation"]["status"] == "PASS"
    assert validation["contract_validation"]["passed"] == 266
    assert full_validation["status"] == "PASS_AFTER_FAILURE_FIX_REPLACEMENT"

    assert wave11["generated_state"] == {
        "status": "COMPLETE_WAVE11",
        "module_count": 1000,
        "test_file_count": 1161,
        "direct_writer_current_count": 856,
        "direct_writer_violation_count": 0,
        "deprecation_inventory_id": "arch_004g_deprecation_inventory_5d0dba8b6f4962b467d8",
        "active_task_count": 436,
        "completed_task_count": 456,
    }

    assert set(wave11["safety"].values()) <= {False, "none"}
    assert wave11["safety"]["production_effect"] == "none"
    assert wave11["safety"]["order_or_broker_action"] == "none"


def test_docs_gov_001_freshness_closeout_is_append_only_current_hash_authority() -> None:
    _assert_current_docs_gov_historical_prefix_immutable()
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    assert next(reversed(baseline)) == LATEST_COMPATIBILITY_SECTION
    docs_gov = baseline[DOCS_GOV_SECTION]

    assert docs_gov["schema_version"] == "docs_gov_001_freshness_closeout.v1"
    assert docs_gov["status"] == "COMPLETE_DOCS_FRESHNESS_CLOSEOUT"
    assert docs_gov["boundary_id"] == "DOCS-GOV-001_FRESHNESS_CLOSEOUT"
    assert docs_gov["task_ids"] == ["DOCS-GOV-001_EXISTING_FRESHNESS_METADATA_DEBT"]
    assert docs_gov["prior_sections_immutability"] == {
        "source_commit": DOCS_GOV_BASE_COMMIT,
        "repository_path": WAVE11_BASELINE_REPOSITORY_PATH,
        "git_blob_sha1": DOCS_GOV_BASELINE_GIT_BLOB,
        "raw_byte_count": DOCS_GOV_HISTORICAL_PREFIX_BYTE_COUNT,
        "raw_sha256": DOCS_GOV_HISTORICAL_PREFIX_SHA256,
        "append_offset": DOCS_GOV_HISTORICAL_PREFIX_BYTE_COUNT,
        "current_section_must_be_eof": True,
    }

    closeout = docs_gov["closeout"]
    assert closeout["task_counts"] == {
        "active": 435,
        "completed": 457,
        "total": 892,
    }
    assert closeout["freshness"] == {
        "issue_count_before": 11,
        "target_document_count": 11,
        "metadata_only_document_count": 11,
        "targeted_batch_document_counts": [5, 6],
        "targeted_issue_count_after": 0,
        "global_document_count": 688,
        "global_issue_count_after": 0,
    }

    validation = docs_gov["validation"]
    assert validation["targeted"] == {
        "status": "PASS",
        "document_count": 11,
        "issue_count": 0,
    }
    assert validation["global"] == {
        "status": "PASS",
        "document_count": 688,
        "issue_count": 0,
    }
    assert validation["docs"] == {
        "status": "PASS",
        "docs_freshness_tests": "PASS",
        "documentation_contract_tests": "PASS",
        "task_register_consistency": "PASS",
    }

    assert docs_gov["supersession"] == {
        "superseded_by_phase": "DOCS-GOV-001_FRESHNESS_CLOSEOUT",
        "scope": "ALL_PRIOR_NON_HISTORICAL_SOURCE_RECORDS_FOR_EACH_LISTED_PATH",
        "historical_hashes_rewritten": False,
        "current_hash_authority": f"{DOCS_GOV_SECTION}.sources",
    }
    assert set(docs_gov["superseded_live_source_paths"]) == (DOCS_GOV_SUPERSEDED_LIVE_SOURCE_PATHS)
    assert len(docs_gov["superseded_live_source_paths"]) == 19
    docs_gov_live_mismatches = _docs_gov_prior_active_source_mismatches()
    # Historical closeout sections retain their exact source records. A newer
    # append-only phase may legitimately change an older live path, so the old
    # section proves that its own supersession set remains covered while the
    # newest section is the exhaustive current-live authority.
    assert _wave12_superseded_live_source_paths() <= docs_gov_live_mismatches
    assert docs_gov_live_mismatches <= _arch_005s4d_s2_all_superseded_live_source_paths()

    sources = docs_gov["sources"]
    source_paths = [str(source["path"]) for source in sources]
    assert len(source_paths) == 20
    assert len(source_paths) == len(set(source_paths))
    assert set(source_paths) == DOCS_GOV_SOURCE_PATHS
    assert "inputs/architecture/arch_004_compatibility_baseline.yaml" not in source_paths
    assert "docs/research/growth_tilt_owner_diagnosis_pack.md" not in source_paths
    assert all(
        isinstance(source["sha256"], str)
        and len(source["sha256"]) == 64
        and int(source["sha256"], 16) >= 0
        for source in sources
    )
    for source in sources:
        assert _source_sha256_at_commit(source, WAVE12_BASE_COMMIT) == source["sha256"], source[
            "path"
        ]
    assert docs_gov["source_hash_status"] == "FINAL_TRACKED_STATE_FRESH"

    assert docs_gov["safety"] == {
        "metadata_only": True,
        "runtime_behavior_changed": False,
        "strategy_or_investment_interpretation_changed": False,
        "data_backtest_or_provider_execution": False,
        "order_or_broker_action": "none",
        "production_effect": "none",
    }


def test_arch_004_wave12_s2_is_append_only_current_hash_authority() -> None:
    _assert_current_wave12_historical_prefix_immutable()
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    assert next(reversed(baseline)) == LATEST_COMPATIBILITY_SECTION
    wave12 = baseline[WAVE12_SECTION]

    assert wave12["schema_version"] == "arch_004_wave12_g4_d0b_s2_closeout.v1"
    assert wave12["status"] in {
        "VALIDATING_FORMAL_GATE",
        "COMPLETE_WAVE12_S2",
    }
    assert wave12["boundary_id"] == "ARCH-004-WAVE12-S2"
    assert wave12["task_ids"] == [
        "ARCH-004G4_OPERATIONS_PERIODIC_CONSUMER_MIGRATION",
        "DATA-GOV-001_UNIFIED_DATA_FOUNDATION_GOVERNANCE",
    ]
    assert wave12["prior_sections_immutability"] == {
        "source_commit": WAVE12_BASE_COMMIT,
        "repository_path": WAVE11_BASELINE_REPOSITORY_PATH,
        "git_blob_sha1": WAVE12_BASELINE_GIT_BLOB,
        "raw_byte_count": WAVE12_HISTORICAL_PREFIX_BYTE_COUNT,
        "raw_sha256": WAVE12_HISTORICAL_PREFIX_SHA256,
        "append_offset": WAVE12_HISTORICAL_PREFIX_BYTE_COUNT,
        "current_section_must_be_eof": True,
    }
    assert wave12["research_window"] == {
        "default_start": "2021-02-22",
        "legacy_2022_default_active": False,
    }
    assert wave12["generated_state"] == {
        "status": "PASS",
        "module_count": 1004,
        "test_file_count": 1167,
        "direct_writer_current_count": 856,
        "direct_writer_violation_count": 0,
        "aggregate_fragment_count": 15,
        "deprecation_inventory_id": "arch_004g_deprecation_inventory_9ed017c2820799618496",
        "active_task_count": 435,
        "completed_task_count": 457,
    }

    superseded = set(wave12["superseded_live_source_paths"])
    wave12_live_mismatches = _wave12_prior_active_source_mismatches()
    assert superseded <= wave12_live_mismatches
    assert wave12_live_mismatches <= _arch_005s4d_s2_all_superseded_live_source_paths()
    assert wave12["supersession"] == {
        "superseded_by_phase": "ARCH-004-WAVE12-S2",
        "scope": "ALL_PRIOR_NON_HISTORICAL_SOURCE_RECORDS_FOR_EACH_LISTED_PATH",
        "historical_hashes_rewritten": False,
        "current_hash_authority": f"{WAVE12_SECTION}.sources",
    }

    sources = wave12["sources"]
    source_paths = [str(source["path"]) for source in sources]
    assert len(source_paths) == len(set(source_paths))
    assert superseded <= set(source_paths)
    assert WAVE11_BASELINE_REPOSITORY_PATH not in source_paths
    assert "docs/research/growth_tilt_owner_diagnosis_pack.md" not in source_paths
    readiness = safe_load_yaml_path(WAVE12_READINESS_POLICY_PATH)
    required_paths: set[str] = set()
    for manifest in readiness["change_manifests"]:
        required_paths.update(str(path) for path in manifest["owned_paths"])
        required_paths.update(str(path) for path in manifest["shared_paths"])
    required_paths.discard(WAVE11_BASELINE_REPOSITORY_PATH)
    assert required_paths <= set(source_paths)
    for source in sources:
        assert _source_sha256_at_commit(source, WAVE12_CLOSEOUT_COMMIT) == source["sha256"], source[
            "path"
        ]
    assert wave12["source_hash_status"] == "FINAL_TRACKED_STATE_FRESH"

    assert wave12["safety"] == {
        "strategy_logic_changed": False,
        "strategy_threshold_changed": False,
        "investment_interpretation_changed": False,
        "backtest_or_search_executed": False,
        "paper_shadow_or_portfolio_mutated": False,
        "data_consumer_cutover_performed": False,
        "automatic_command_dispatch_enabled": False,
        "order_or_broker_action": "none",
        "production_effect": "none",
    }


def test_arch_004_wave13_gov006_n1_is_append_only_current_hash_authority() -> None:
    _assert_current_wave13_historical_prefix_immutable()
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    assert next(reversed(baseline)) == LATEST_COMPATIBILITY_SECTION
    wave13 = baseline[WAVE13_SECTION]

    assert wave13["schema_version"] == "arch_004_wave13_gov006_n1_closeout.v1"
    assert wave13["status"] in {
        "VALIDATING_FORMAL_GATE",
        "COMPLETE_WAVE13_GOV006_N1",
    }
    assert wave13["boundary_id"] == "ARCH-004-WAVE13-GOV006-N1"
    assert wave13["task_ids"] == ["GOV-006_ACTIVE_TASK_PORTFOLIO_NORMALIZATION"]
    assert wave13["prior_sections_immutability"] == {
        "source_commit": WAVE13_BASE_COMMIT,
        "repository_path": WAVE11_BASELINE_REPOSITORY_PATH,
        "git_blob_sha1": WAVE13_BASELINE_GIT_BLOB,
        "raw_byte_count": WAVE13_HISTORICAL_PREFIX_BYTE_COUNT,
        "raw_sha256": WAVE13_HISTORICAL_PREFIX_SHA256,
        "append_offset": WAVE13_HISTORICAL_PREFIX_BYTE_COUNT,
        "current_section_must_be_eof": True,
    }
    assert wave13["research_window"] == {
        "default_start": "2021-02-22",
        "legacy_2022_default_active": False,
    }

    lineage = wave13["lineage"]
    assert lineage["wave12_closeout_commit"] == WAVE12_CLOSEOUT_COMMIT
    assert lineage["historical_base_commit"] == WAVE12_CLOSEOUT_COMMIT
    assert lineage["application_commit"] == WAVE13_BASE_COMMIT
    assert lineage["application_parent_is_historical_base"] is True
    assert lineage["historical_base_is_ancestor_of_application"] is True
    assert lineage["application_commit_is_ancestor_of_validation_head"] is True

    closeout_raw_sha256 = hashlib.sha256(WAVE13_APPLIED_CLOSEOUT_PATH.read_bytes()).hexdigest()
    assert closeout_raw_sha256 == WAVE13_APPLIED_CLOSEOUT_RAW_SHA256
    decision_raw_sha256 = hashlib.sha256(WAVE13_DECISION_MANIFEST_PATH.read_bytes()).hexdigest()
    assert decision_raw_sha256 == WAVE13_DECISION_MANIFEST_RAW_SHA256
    closeout = json.loads(WAVE13_APPLIED_CLOSEOUT_PATH.read_text(encoding="utf-8"))
    assert closeout["schema_version"] == "gov_006_portfolio_normalization_applied_closeout.v1"
    assert closeout["status"] == "APPLIED_CLOSEOUT_READY"
    assert closeout["closeout_id"] == "gov_006_applied_closeout_bdbfd433a72d5349ead9"
    assert closeout["closeout_sha256"] == WAVE13_APPLIED_CLOSEOUT_CANONICAL_SHA256
    assert closeout["lineage"] == {
        "application_commit": WAVE13_BASE_COMMIT,
        "application_commit_must_equal_or_be_ancestor_of_validation_head": True,
        "historical_base_commit": WAVE12_CLOSEOUT_COMMIT,
        "historical_base_is_ancestor_of_application": True,
    }

    governance_evidence = wave13["governance_evidence"]
    assert governance_evidence["dry_run"] == {
        "path": WAVE13_DECISION_MANIFEST_PATH.as_posix(),
        "manifest_id": "gov_006_decision_manifest_3fb5f2a038eca2361179",
        "raw_file_sha256": WAVE13_DECISION_MANIFEST_RAW_SHA256,
        "canonical_manifest_sha256": WAVE13_DECISION_MANIFEST_CANONICAL_SHA256,
        "source_base_commit": WAVE12_CLOSEOUT_COMMIT,
        "commit_bound_replay": "PASS",
        "decision_count": 30,
    }
    assert governance_evidence["applied_closeout"] == {
        "path": WAVE13_APPLIED_CLOSEOUT_PATH.as_posix(),
        "schema_version": "gov_006_portfolio_normalization_applied_closeout.v1",
        "status": "APPLIED_CLOSEOUT_READY",
        "closeout_id": "gov_006_applied_closeout_bdbfd433a72d5349ead9",
        "canonical_closeout_sha256": WAVE13_APPLIED_CLOSEOUT_CANONICAL_SHA256,
        "raw_file_sha256": WAVE13_APPLIED_CLOSEOUT_RAW_SHA256,
        "application_commit": WAVE13_BASE_COMMIT,
    }

    application = wave13["application"]
    assert application == {
        "decision_count": 30,
        "target_status_counts": {"DONE": 18, "DROPPED": 12},
        "before_task_counts": {"active": 435, "completed": 457, "total": 892},
        "after_task_counts": {"active": 405, "completed": 487, "total": 892},
        "after_completed_status_counts": {"DONE": 475, "DROPPED": 12},
        "active_physical_line_count_before": 1969,
        "active_physical_line_count_after": 1969,
        "vacated_source_line_count": 30,
        "physical_line_count_preserved": True,
        "task_id_set_conserved": True,
        "untargeted_partition_priority_status_unchanged": True,
        "retained_exclusions": [
            "TRADING-1087_EXTERNAL_REQUEST_INCREMENTAL_REFRESH_GUARDRAILS",
            "TRADING-1088_MARKETSTACK_MISSED_DAY_TAIL_CATCH_UP",
        ],
        "retained_master": [
            "TRADING-1806_to_1885_TWO_LANE_OPTIMIZATION_MASTER_CLOSEOUT",
        ],
    }
    assert closeout["application"]["decision_count"] == application["decision_count"]
    assert closeout["application"]["target_status_counts"] == application["target_status_counts"]
    assert (
        closeout["after_inventory"]["active_task_count"]
        == application["after_task_counts"]["active"]
    )
    assert (
        closeout["after_inventory"]["completed_task_count"]
        == application["after_task_counts"]["completed"]
    )
    assert (
        closeout["after_inventory"]["total_task_count"] == application["after_task_counts"]["total"]
    )

    generated_state = wave13["generated_state"]
    assert generated_state["status"] == "PASS"
    assert generated_state["module_count"] == 1004
    assert generated_state["test_file_count"] == 1167
    assert generated_state["direct_writer_current_count"] == 856
    assert generated_state["direct_writer_violation_count"] == 0
    assert generated_state["aggregate_fragment_count"] == 15
    assert (
        generated_state["deprecation_inventory_id"]
        == "arch_004g_deprecation_inventory_9ed017c2820799618496"
    )
    assert generated_state["active_task_count"] == 405
    assert generated_state["completed_task_count"] == 487
    assert generated_state["total_task_count"] == 892
    assert generated_state["task_registry_shadow_byte_identical"] is True
    readiness = json.loads(WAVE13_G2_5_READINESS_PATH.read_text(encoding="utf-8"))
    assert generated_state["g2_5_readiness"] == {
        "path": WAVE13_G2_5_READINESS_PATH.as_posix(),
        "status": readiness["status"],
        "source_base_commit": readiness["source_base_commit"],
        "dispatch_allowed": readiness["dispatch_allowed"],
        "report_checksum": readiness["report_checksum"],
    }
    assert readiness["status"] == "PASS"
    assert readiness["source_base_commit"] == WAVE13_BASE_COMMIT
    assert readiness["dispatch_allowed"] is False

    assert wave13["roadmap"] == {
        "next_wave": "WAVE14_D0B2_BOUNDED_G3",
        "data_task": "DATA-GOV-001_D0B2",
        "reporting_task": "ARCH-004G3_REPORTING_NATIVE_MIGRATION",
        "max_active_domain_workers": 2,
        "shared_paths_coordinator_only": True,
        "g4c_cadence_observation_async": True,
        "prior_g2_5_rehearsal_is_dispatch_authority": False,
        "final_head_manifest_required": True,
        "s0_contract_readiness_status": "NOT_STARTED",
        "next_slice_unblocked": False,
        "automatic_dispatch_enabled": False,
        "data_consumer_cutover_performed": False,
    }

    validation = wave13["validation"]
    phase_complete = wave13["status"] == "COMPLETE_WAVE13_GOV006_N1"
    if phase_complete:
        assert validation["focused"]["status"] == "PASS"
        assert validation["architecture_fitness"]["status"] in {
            "PASS",
            "FINAL_TRACKED_STATE_PASS",
        }
        assert validation["contract_validation"]["status"] == "PASS"
        assert validation["reproducibility"]["status"] == "PASS"
    else:
        assert validation["focused"]["status"] in {"PENDING", "PASS"}
        assert validation["architecture_fitness"]["status"] in {
            "PENDING",
            "PASS",
            "FINAL_TRACKED_STATE_PASS",
        }
        assert validation["contract_validation"]["status"] in {"PENDING", "PASS"}
        assert validation["reproducibility"]["status"] in {"PENDING", "PASS"}
    assert validation["static"] == {
        "ruff": "PASS",
        "black": "PASS",
        "mypy_strict": "PASS",
        "diff_check": "PASS",
    }
    full_validation = validation["full_validation"]
    assert full_validation["required"] is True
    attempts = full_validation["attempts"]
    assert isinstance(attempts, list) and attempts
    attempt_ids = [str(attempt["attempt_id"]) for attempt in attempts]
    assert len(attempt_ids) == len(set(attempt_ids))
    for index, attempt in enumerate(attempts):
        if index < len(attempts) - 1:
            assert attempt["status"] == "FAIL"
        if attempt["status"] == "PENDING":
            assert index == len(attempts) - 1
            assert "artifact" not in attempt
        else:
            assert attempt["status"] in {"FAIL", "PASS"}
            _wave11_portable_artifact_identity(attempt)
    latest_full_status = attempts[-1]["status"]
    if wave13["status"] == "COMPLETE_WAVE13_GOV006_N1":
        assert latest_full_status == "PASS"
        assert full_validation["status"] in {
            "PASS",
            "PASS_AFTER_FAILURE_FIX",
        }
    else:
        assert latest_full_status == "PENDING"
        assert full_validation["status"] == "PENDING"

    superseded = set(wave13["superseded_live_source_paths"])
    wave13_live_mismatches = _wave13_prior_active_source_mismatches()
    assert superseded <= wave13_live_mismatches
    assert wave13_live_mismatches <= _arch_005s4d_s2_all_superseded_live_source_paths()
    assert wave13["supersession"] == {
        "superseded_by_phase": "ARCH-004-WAVE13-GOV006-N1",
        "scope": "ALL_PRIOR_NON_HISTORICAL_SOURCE_RECORDS_FOR_EACH_LISTED_PATH",
        "historical_hashes_rewritten": False,
        "current_hash_authority": f"{WAVE13_SECTION}.sources",
    }
    sources = wave13["sources"]
    source_paths = [str(source["path"]) for source in sources]
    assert len(source_paths) == len(set(source_paths))
    assert superseded <= set(source_paths)
    assert {
        "config/architecture/arch_004_refactor_policy.yaml",
        "config/governance/gov_006_wave1_normalization.yaml",
        "docs/requirements/GOV-006_Active_Task_Portfolio_Normalization.md",
        "docs/system_flow.md",
        "docs/task_register.md",
        "docs/task_register_completed.md",
        WAVE13_APPLIED_CLOSEOUT_PATH.as_posix(),
        WAVE13_DECISION_MANIFEST_PATH.as_posix(),
        "inputs/architecture/arch_004e_module_manifest.yaml",
        "inputs/architecture/arch_004e_test_manifest.yaml",
        WAVE13_G2_5_READINESS_PATH.as_posix(),
        "inputs/architecture/arch_004g_deprecation_inventory.yaml",
        "inputs/architecture/arch_005_task_registry_baseline.yaml",
        "inputs/architecture/arch_005_task_shadow_index.yaml",
        "scripts/governance_task_portfolio_normalization.py",
        "src/ai_trading_system/platform/architecture/task_portfolio_normalization.py",
        "tests/test_arch_004_refactor_policy.py",
        "tests/test_gov_006_task_portfolio_normalization.py",
    } <= set(source_paths)
    assert WAVE11_BASELINE_REPOSITORY_PATH not in source_paths
    assert "docs/research/growth_tilt_owner_diagnosis_pack.md" not in source_paths
    for source in sources:
        assert _source_sha256_at_commit(source, WAVE14_S0_1_BASE_COMMIT) == source["sha256"], (
            source["path"]
        )
    assert wave13["source_hash_status"] == "FINAL_TRACKED_STATE_FRESH"

    assert wave13["worktree_attribution"] == {
        "known_unrelated_worktree_files": [
            "docs/research/growth_tilt_owner_diagnosis_pack.md",
        ],
        "active_shared_path_lease_count": 0,
    }
    assert set(wave13["safety"].values()) <= {False, "none"}
    assert wave13["safety"]["strategy_logic_changed"] is False
    assert wave13["safety"]["data_or_runtime_changed"] is False
    assert wave13["safety"]["data_consumer_cutover_performed"] is False
    assert wave13["safety"]["automatic_command_dispatch_enabled"] is False
    assert wave13["safety"]["order_or_broker_action"] == "none"
    assert wave13["safety"]["production_effect"] == "none"


def test_arch_004_wave14_s0_1_is_immutable_historical_hash_authority() -> None:
    _assert_current_wave14_s0_1_historical_prefix_immutable()
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    section_ids = list(baseline)
    assert section_ids.index(WAVE14_S0_1_SECTION) < section_ids.index(WAVE14_S2_SECTION)
    assert section_ids.index(WAVE14_S2_SECTION) < section_ids.index(OPS_067_SECTION)
    assert next(reversed(baseline)) == LATEST_COMPATIBILITY_SECTION
    wave14 = baseline[WAVE14_S0_1_SECTION]

    assert wave14["schema_version"] == ("arch_004_wave14_s0_1_readiness_infrastructure.v1")
    assert wave14["status"] == "COMPLETE_WAVE14_S0_1"
    assert wave14["boundary_id"] == "ARCH-004-WAVE14-S0.1"
    assert wave14["task_ids"] == [
        "ARCH-004W14_D0B2_G3_PARALLEL_READINESS",
    ]
    assert wave14["prior_sections_immutability"] == {
        "source_commit": WAVE14_S0_1_BASE_COMMIT,
        "repository_path": WAVE11_BASELINE_REPOSITORY_PATH,
        "git_blob_sha1": WAVE14_S0_1_BASELINE_GIT_BLOB,
        "raw_byte_count": WAVE14_S0_1_HISTORICAL_PREFIX_BYTE_COUNT,
        "raw_sha256": WAVE14_S0_1_HISTORICAL_PREFIX_SHA256,
        "append_offset": WAVE14_S0_1_HISTORICAL_PREFIX_BYTE_COUNT,
        "current_section_must_be_eof": True,
    }
    assert wave14["research_window"] == {
        "default_start": "2021-02-22",
        "legacy_2022_default_active": False,
    }
    assert wave14["lineage"] == {
        "source_wave_closeout_commit": WAVE14_S0_1_BASE_COMMIT,
        "source_wave_closeout_is_ancestor_of_validation_head": True,
    }
    assert (
        subprocess.run(
            [
                "git",
                "merge-base",
                "--is-ancestor",
                WAVE14_S0_1_BASE_COMMIT,
                "HEAD",
            ],
            check=False,
        ).returncode
        == 0
    )

    assert wave14["generated_state"] == {
        "status": "PASS",
        "module_count": 1005,
        "test_file_count": 1169,
        "direct_writer_current_count": 856,
        "direct_writer_violation_count": 0,
        "aggregate_fragment_count": 15,
        "deprecation_inventory_id": ("arch_004g_deprecation_inventory_813535e09228e6ca2542"),
        "active_task_count": 406,
        "completed_task_count": 487,
        "total_task_count": 893,
        "task_registry_shadow_byte_identical": True,
    }
    assert wave14["roadmap"] == {
        "current_phase": "WAVE14_S0_1_REUSABLE_READINESS_INFRA",
        "next_phase": "WAVE14_S0_2_EXACT_POLICY_EVIDENCE",
        "s0_contract_readiness_status": "IN_PROGRESS_INFRA",
        "next_slice_unblocked": False,
        "automatic_dispatch_enabled": False,
    }
    assert wave14["validation"] == {
        "focused": {
            "status": "PASS",
            "readiness_passed": 39,
            "g2_5_carrier_replay_passed": 30,
            "compatibility_checkout_regression_passed": 6,
            "direct_writer_gate_passed": 1,
        },
        "static": {
            "ruff": "PASS",
            "black": "PASS",
            "mypy_strict": "PASS",
            "diff_check": "PASS",
        },
        "architecture_fitness": {
            "status": "PASS",
            "passed": 534,
            "failed": 0,
            "elapsed_seconds": 68.64,
            "artifact": {
                "path": (
                    "outputs/validation_runtime/"
                    "architecture-fitness_20260723T112455Z/"
                    "test_runtime_summary.json"
                ),
                "sha256": ("3d9879036752fec7d04357ac66c8b47c82b05fb2cee486010e3d558b7abe344c"),
                "size_bytes": 27602,
            },
        },
        "contract_validation": {
            "status": "PASS",
            "passed": 266,
            "failed": 0,
            "elapsed_seconds": 138.53,
            "artifact": {
                "path": (
                    "outputs/validation_runtime/"
                    "contract-validation_20260723T112604Z/"
                    "test_runtime_summary.json"
                ),
                "sha256": ("b6eca48edb84c5926a680c1b970745d6a67915494a817cd7d5f733ecd12bb856"),
                "size_bytes": 27259,
            },
        },
        "full_validation": {
            "required": False,
            "reason": (
                "S0.1 reusable infrastructure; Wave14 final tracked state owns "
                "the one required Full"
            ),
        },
    }

    superseded = set(wave14["superseded_live_source_paths"])
    observed_live_mismatches = _wave14_s0_1_prior_active_source_mismatches()
    assert superseded <= observed_live_mismatches
    assert observed_live_mismatches <= (_arch_005s4d_s2_all_superseded_live_source_paths())
    assert len(superseded) == WAVE14_S0_1_EXPECTED_SUPERSEDED_SOURCE_COUNT
    assert wave14["supersession"] == {
        "superseded_by_phase": "ARCH-004-WAVE14-S0.1",
        "scope": "ALL_PRIOR_NON_HISTORICAL_SOURCE_RECORDS_FOR_EACH_LISTED_PATH",
        "historical_hashes_rewritten": False,
        "current_hash_authority": f"{WAVE14_S0_1_SECTION}.sources",
    }

    sources = wave14["sources"]
    source_paths = [str(source["path"]) for source in sources]
    assert len(source_paths) == WAVE14_S0_1_EXPECTED_SOURCE_COUNT
    assert len(source_paths) == len(set(source_paths))
    wave13_source_paths = {str(source["path"]) for source in baseline[WAVE13_SECTION]["sources"]}
    wave14_new_source_paths = {
        ("config/architecture/fragments/artifacts/growth_tilt_candidate_family_closure.yaml"),
        "docs/requirements/ARCH-004_Wave14_D0B2_G3_Parallel_Readiness.md",
        (
            "registry/development_tasks_shadow/active/0a/"
            "0abb8a3ad247288bda28379233fd84e047b071d1ab19dbbe650d6c1c8bf49355.yaml"
        ),
        (
            "registry/development_tasks_shadow/active/33/"
            "33b88fa1e99dbeeef9e82971ece0fceca99a3ba9fc2e24eebc122261b7ef9929.yaml"
        ),
        (
            "registry/development_tasks_shadow/active/74/"
            "742e8a85c69149942d2d85c1d5ded5e6a3dad9a34e87ac65e844110dce18c7ad.yaml"
        ),
        (
            "registry/development_tasks_shadow/active/76/"
            "76274bb2bb89b8ff54bb486ddf8b5c1c43f87d66fbfebca3cf760326c864e415.yaml"
        ),
        (
            "registry/development_tasks_shadow/active/c3/"
            "c381b25072ea135344a2576c2b622a9d2b3a891d53775df866277c77fada39f1.yaml"
        ),
        (
            "registry/development_tasks_shadow/active/d4/"
            "d4fc4bd29b4d23452253c0bc5a7889e5dbd8195817ffd56ac7561a9897c1d9bf.yaml"
        ),
        (
            "registry/development_tasks_shadow/active/e9/"
            "e977535673f7acd3be62f3915aaedec6d22598a52698a237144356d414738674.yaml"
        ),
        "scripts/architecture_wave_readiness.py",
        "src/ai_trading_system/platform/architecture/arch_004_g2_5_readiness.py",
        "src/ai_trading_system/platform/architecture/cli_contract.py",
        "src/ai_trading_system/platform/architecture/parallel_control_kernel.py",
        "src/ai_trading_system/platform/architecture/parallel_control_scheduler.py",
        "src/ai_trading_system/platform/architecture/supervised_automation.py",
        "src/ai_trading_system/platform/architecture/wave_readiness.py",
        "tests/test_arch_004_g2_5_readiness.py",
        "tests/test_arch_004_wave14_d0b2_g3_readiness.py",
        "tests/test_architecture_wave_readiness.py",
    }
    assert set(source_paths) == wave13_source_paths | wave14_new_source_paths
    assert superseded <= set(source_paths)
    assert WAVE11_BASELINE_REPOSITORY_PATH not in source_paths
    prohibited_user_path = "docs/research/growth_tilt_owner_diagnosis_pack.md"
    assert prohibited_user_path not in source_paths
    for source in sources:
        assert _source_sha256_at_commit(source, WAVE14_S2_BASE_COMMIT) == source["sha256"], source[
            "path"
        ]
    assert wave14["source_hash_status"] == "FINAL_TRACKED_STATE_FRESH"

    assert wave14["worktree_attribution"] == {
        "known_unrelated_worktree_files": [prohibited_user_path],
        "active_shared_path_lease_count": 0,
    }
    assert wave14["safety"] == {
        "strategy_logic_changed": False,
        "strategy_threshold_changed": False,
        "investment_interpretation_changed": False,
        "backtest_or_search_executed": False,
        "data_or_runtime_changed": False,
        "paper_shadow_or_portfolio_mutated": False,
        "data_consumer_cutover_performed": False,
        "automatic_command_dispatch_enabled": False,
        "order_or_broker_action": "none",
        "production_effect": "none",
    }


def test_arch_004_wave14_s0_1_rejects_historical_prefix_tamper() -> None:
    base_blob = _wave14_s0_1_base_baseline_blob()
    valid_append = base_blob + f"\n{WAVE14_S0_1_SECTION}:\n  status: TEST_ONLY\n".encode()
    _assert_wave14_s0_1_historical_prefix_immutable(valid_append, base_blob)

    tampered = bytearray(valid_append)
    tampered[0] ^= 1
    with pytest.raises(AssertionError, match="historical prefix differs"):
        _assert_wave14_s0_1_historical_prefix_immutable(
            bytes(tampered),
            base_blob,
        )


def test_arch_004_wave14_s2_is_append_only_current_hash_authority() -> None:
    _assert_current_wave14_s2_historical_prefix_immutable()
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    assert next(reversed(baseline)) == LATEST_COMPATIBILITY_SECTION
    wave14 = baseline[WAVE14_S2_SECTION]

    status = wave14["status"]
    assert status in {"VALIDATING_WAVE14_S2", "COMPLETE_WAVE14_S2"}
    assert wave14["schema_version"] == ("arch_004_wave14_s2_shared_integration_and_formal_exit.v1")
    assert wave14["boundary_id"] == "ARCH-004-WAVE14-S2"
    assert wave14["task_ids"] == ["ARCH-004W14_D0B2_G3_PARALLEL_READINESS"]
    assert wave14["prior_sections_immutability"] == {
        "source_commit": WAVE14_S2_BASE_COMMIT,
        "repository_path": WAVE11_BASELINE_REPOSITORY_PATH,
        "git_blob_sha1": WAVE14_S2_BASELINE_GIT_BLOB,
        "raw_byte_count": WAVE14_S2_HISTORICAL_PREFIX_BYTE_COUNT,
        "raw_sha256": WAVE14_S2_HISTORICAL_PREFIX_SHA256,
        "append_offset": WAVE14_S2_HISTORICAL_PREFIX_BYTE_COUNT,
        "current_section_must_be_eof": True,
    }
    assert wave14["research_window"] == {
        "default_start": "2021-02-22",
        "legacy_2022_default_active": False,
    }
    assert wave14["lineage"] == {
        "source_wave_s0_carrier_commit": WAVE14_S2_BASE_COMMIT,
        "source_wave_s0_carrier_is_ancestor_of_validation_head": True,
    }
    assert (
        subprocess.run(
            ["git", "merge-base", "--is-ancestor", WAVE14_S2_BASE_COMMIT, "HEAD"],
            check=False,
        ).returncode
        == 0
    )

    superseded = set(wave14["superseded_live_source_paths"])
    wave14_live_mismatches = _wave14_s2_prior_active_source_mismatches()
    assert superseded <= wave14_live_mismatches
    assert wave14_live_mismatches <= _arch_005s4d_s2_all_superseded_live_source_paths()
    assert wave14["supersession"] == {
        "superseded_by_phase": "ARCH-004-WAVE14-S2",
        "scope": "ALL_PRIOR_NON_HISTORICAL_SOURCE_RECORDS_FOR_EACH_LISTED_PATH",
        "historical_hashes_rewritten": False,
        "current_hash_authority": f"{WAVE14_S2_SECTION}.sources",
    }

    sources = wave14["sources"]
    source_paths = [str(source["path"]) for source in sources]
    assert len(source_paths) == len(set(source_paths))
    wave14_s0_source_paths = {
        str(source["path"]) for source in baseline[WAVE14_S0_1_SECTION]["sources"]
    }
    wave14_s2_new_source_paths = {
        "config/architecture/arch_004_wave14_d0b2_g3_scope_amendment.yaml",
        "config/architecture/devex_ownership_policy.yaml",
        ("config/architecture/fragments/artifacts/arch_004g3_reader_brief_native.yaml"),
        "config/architecture/fragments/flows/arch_004g3_reader_brief_native.yaml",
        "config/architecture/fragments/reports/arch_004g3_reader_brief_native.yaml",
        "docs/requirements/ARCH-005S4D_Shared_Checkout_Write_Lease_Guard.md",
        "src/ai_trading_system/cli_commands/research_external_validation.py",
        "src/ai_trading_system/cli_commands/research_trends.py",
        "src/ai_trading_system/cli_direct.py",
        "src/ai_trading_system/data/__init__.py",
        "src/ai_trading_system/data/download.py",
        "src/ai_trading_system/data/download_publication.py",
        "src/ai_trading_system/data/quality_execution.py",
        "src/ai_trading_system/equal_risk_growth_tilt.py",
        "src/ai_trading_system/external_validation.py",
        "src/ai_trading_system/platform/reporting/__init__.py",
        "src/ai_trading_system/platform/reporting/owner_daily.py",
        "src/ai_trading_system/platform/reporting/reader_brief_native.py",
        "src/ai_trading_system/research_campaign.py",
        "src/ai_trading_system/reports/reader_brief.py",
        "src/ai_trading_system/trading_calendar.py",
        (
            "tests/fixtures/growth_tilt_baseline_contract_adapters/"
            "baseline_exposure_unit_inventory.json"
        ),
        ("tests/fixtures/growth_tilt_baseline_contract_adapters/baseline_signal_inventory.json"),
        (
            "tests/fixtures/growth_tilt_baseline_contract_adapters/"
            "growth_tilt_hard_veto_resolution_matrix.json"
        ),
        ("tests/fixtures/trading2453_constraint_hit_diagnosis/data_quality_gate.json.gz"),
        "tests/fixtures/trading2453_constraint_hit_diagnosis/data_quality_gate.md.gz",
        ("tests/fixtures/trading2453_constraint_hit_diagnosis/effective_windows.json.gz"),
        ("tests/fixtures/trading2453_constraint_hit_diagnosis/evaluator_manifest.json.gz"),
        ("tests/fixtures/trading2453_constraint_hit_diagnosis/evaluator_runtime_telemetry.json.gz"),
        ("tests/fixtures/trading2453_constraint_hit_diagnosis/evaluator_validation.json.gz"),
        ("tests/fixtures/trading2453_constraint_hit_diagnosis/historical_seen_report.json.gz"),
        ("tests/fixtures/trading2453_constraint_hit_diagnosis/historical_seen_report.md.gz"),
        ("tests/fixtures/trading2453_constraint_hit_diagnosis/recent_known_diagnostics.jsonl"),
        ("tests/fixtures/trading2453_constraint_hit_diagnosis/test_evaluations.jsonl"),
        ("tests/fixtures/trading2453_constraint_hit_diagnosis/train_evaluations.jsonl.gz"),
        ("tests/fixtures/trading2453_constraint_hit_diagnosis/train_selections.jsonl"),
        ("tests/research_strategies/test_growth_tilt_baseline_contract_adapters.py"),
        ("tests/research_trends/test_first_layer_candidate_generators_regenerate_cli.py"),
        ("tests/research_trends/test_regenerated_candidate_actual_path_validation_cli.py"),
        "tests/test_arch_004_wave14_scope_amendment.py",
        "tests/test_arch_004f3_reporting_architecture.py",
        "tests/test_arch_004g3_reporting_native_migration.py",
        "tests/test_candidate_signal_prediction_artifact_audit.py",
        "tests/test_cli_direct.py",
        "tests/test_data_download.py",
        "tests/test_data_download_publication.py",
        "tests/test_data_quality.py",
        "tests/test_data_quality_execution.py",
        "tests/test_dual_forward_aging.py",
        "tests/test_external_validation.py",
        "tests/test_first_layer_gate_policy_v2_reconciliation.py",
        "tests/test_immutable_data_publish.py",
        "tests/test_research_campaign.py",
        "tests/test_trading2453_constraint_hit_diagnosis.py",
        "tests/test_trading_calendar.py",
        "tests/test_two_layer_boundary_contract.py",
        "tests/trading_engine/test_full_signal_limited_backtest.py",
        "tests/trading_engine/test_price_history_repair.py",
    }
    generated_new_source_paths = set(wave14["generated_new_source_paths"])
    assert generated_new_source_paths <= set(source_paths)
    assert all(
        path.startswith(
            (
                "registry/development_tasks_shadow/active/",
                "registry/development_tasks_shadow/completed/",
            )
        )
        and path.endswith(".yaml")
        for path in generated_new_source_paths
    )
    removed_live_source_paths = set(wave14["removed_live_source_paths"])
    expected_removed_live_source_paths = (
        {WAVE14_S2_ACTIVE_TASK_SHADOW_PATH} if status == "COMPLETE_WAVE14_S2" else set()
    )
    assert removed_live_source_paths == expected_removed_live_source_paths
    assert removed_live_source_paths <= wave14_s0_source_paths
    assert removed_live_source_paths.isdisjoint(source_paths)
    assert all(
        path.startswith("registry/development_tasks_shadow/active/") and path.endswith(".yaml")
        for path in removed_live_source_paths
    )
    assert wave14["removal_authority"] == {
        "schema_version": "compatibility_source_removal.v1",
        "removed_path_state": "ABSENT_FROM_CURRENT_TREE",
        "historical_source_records_preserved": True,
        "current_sources_exclude_removed_paths": True,
    }
    assert set(source_paths) == (
        (wave14_s0_source_paths - removed_live_source_paths)
        | wave14_s2_new_source_paths
        | generated_new_source_paths
    )
    assert removed_live_source_paths <= superseded
    assert superseded <= set(source_paths) | removed_live_source_paths
    if status == "COMPLETE_WAVE14_S2":
        assert WAVE14_S2_COMPLETED_TASK_SHADOW_PATH in source_paths
        assert not Path(WAVE14_S2_ACTIVE_TASK_SHADOW_PATH).exists()
        assert (
            subprocess.run(
                [
                    "git",
                    "ls-files",
                    "--error-unmatch",
                    "--",
                    WAVE14_S2_ACTIVE_TASK_SHADOW_PATH,
                ],
                check=False,
                capture_output=True,
            ).returncode
            != 0
        )
    else:
        assert WAVE14_S2_ACTIVE_TASK_SHADOW_PATH in source_paths
        assert WAVE14_S2_COMPLETED_TASK_SHADOW_PATH not in source_paths

    assert WAVE11_BASELINE_REPOSITORY_PATH not in source_paths
    assert WAVE14_S2_PROHIBITED_USER_PATH not in source_paths
    assert "tests/test_immutable_data_publish.py" in source_paths
    assert "tests/test_immutable_publish.py" not in source_paths
    allowed_removed_paths = _removed_live_source_paths_after(WAVE14_S2_SECTION)
    assert ARCH_005M2_ACTIVE_TASK_SHADOW_PATH in allowed_removed_paths
    _assert_wave14_s2_all_sources_tracked(
        source_paths,
        allowed_removed_paths=allowed_removed_paths,
    )
    for source in sources:
        assert _source_sha256(source) == source["sha256"], source["path"]

    full_sensitive_sources = wave14["full_sensitive_sources"]
    assert isinstance(full_sensitive_sources, list)
    assert full_sensitive_sources == sorted(full_sensitive_sources)
    assert full_sensitive_sources
    assert len(full_sensitive_sources) == len(set(full_sensitive_sources))
    assert WAVE11_BASELINE_REPOSITORY_PATH not in full_sensitive_sources
    assert WAVE14_S2_PROHIBITED_USER_PATH not in full_sensitive_sources

    post_full_paths = wave14["post_full_evidence_only_paths"]
    assert isinstance(post_full_paths, list)
    assert len(WAVE14_S2_APPROVED_POST_FULL_EVIDENCE_ONLY_PATHS) == 19
    assert post_full_paths == sorted(WAVE14_S2_APPROVED_POST_FULL_EVIDENCE_ONLY_PATHS)
    assert len(post_full_paths) == len(set(post_full_paths))
    assert WAVE14_S2_PROHIBITED_USER_PATH not in post_full_paths
    assert not any(path.startswith(("src/", "tests/")) for path in post_full_paths)
    for path in post_full_paths:
        assert _assert_portable_repository_relative_path(path) == path
    assert set(full_sensitive_sources) == set(source_paths) - (
        set(post_full_paths) & set(source_paths)
    )
    assert not set(full_sensitive_sources).intersection(post_full_paths)

    generated_state = wave14["generated_state"]
    assert generated_state["status"] == "PASS"
    assert generated_state["module_count"] > 0
    assert generated_state["test_file_count"] > 0
    assert generated_state["direct_writer_current_count"] >= 0
    assert generated_state["direct_writer_violation_count"] == 0
    assert generated_state["aggregate_fragment_count"] > 0
    assert generated_state["active_task_count"] > 0
    assert generated_state["completed_task_count"] > 0
    assert generated_state["total_task_count"] == (
        generated_state["active_task_count"] + generated_state["completed_task_count"]
    )
    assert generated_state["task_registry_shadow_byte_identical"] is True

    validation = wave14["validation"]
    phase_complete = status == "COMPLETE_WAVE14_S2"
    _assert_wave14_s2_formal_tiers(validation, phase_complete=phase_complete)
    full_validation = validation["full_validation"]
    post_full = validation["post_full_closeout"]
    attempts = _assert_wave14_s2_full_attempt_chain(full_validation)
    if status == "VALIDATING_WAVE14_S2":
        assert wave14["source_hash_status"] == "PRE_FULL_TRACKED_STATE_FRESH"
        assert attempts[-1]["status"] == "PENDING"
        assert post_full == {
            "status": "PENDING",
            "changed_paths": [],
            "full_sensitive_sources_unchanged": False,
            "changed_paths_are_evidence_only": False,
            "full_rerun_required": False,
        }
    else:
        assert wave14["source_hash_status"] == "FINAL_TRACKED_STATE_FRESH"
        assert attempts[-1]["status"] == "PASS"
        if OPS_067_SECTION in baseline:
            historical_payload = subprocess.run(
                [
                    "git",
                    "show",
                    f"{OPS_067_BASE_COMMIT}:{WAVE11_BASELINE_REPOSITORY_PATH}",
                ],
                check=True,
                capture_output=True,
            ).stdout.decode("utf-8")
            historical_baseline = safe_load_yaml_text(historical_payload)
            assert isinstance(historical_baseline, dict)
            assert historical_baseline[WAVE14_S2_SECTION] == wave14
        else:
            _assert_wave14_s2_final_full_evidence(
                wave14,
                sources,
                WAVE14_S2_APPROVED_POST_FULL_EVIDENCE_ONLY_PATHS,
            )

    assert wave14["roadmap"] == {
        "current_phase": "WAVE14_S2_SHARED_INTEGRATION_AND_FORMAL_EXIT",
        "next_phase": "ARCH-005S4D_OWNER_AUTHORIZATION_GATE",
        "next_slice_unblocked": False,
        "automatic_dispatch_enabled": False,
        "wave15_assignment_allowed": False,
    }
    assert wave14["worktree_attribution"] == {
        "known_unrelated_worktree_files": [WAVE14_S2_PROHIBITED_USER_PATH],
        "active_shared_path_lease_count": 0,
    }
    assert wave14["safety"] == {
        "strategy_logic_changed": False,
        "strategy_threshold_changed": False,
        "investment_interpretation_changed": False,
        "backtest_or_search_executed": False,
        "data_or_runtime_changed": True,
        "paper_shadow_or_portfolio_mutated": False,
        "data_consumer_cutover_performed": False,
        "automatic_command_dispatch_enabled": False,
        "order_or_broker_action": "none",
        "production_effect": "none",
    }


def test_ops_067_is_append_only_current_hash_authority() -> None:
    _assert_current_ops_067_historical_prefix_immutable()
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    assert next(reversed(baseline)) == LATEST_COMPATIBILITY_SECTION
    assert list(baseline).index(WAVE14_S2_SECTION) < list(baseline).index(OPS_067_SECTION)
    assert list(baseline).index(OPS_067_SECTION) < list(baseline).index(OPS_068_SECTION)
    ops_067 = baseline[OPS_067_SECTION]

    status = ops_067["status"]
    assert status in {
        "VALIDATING_OPS_067",
        "ENGINEERING_COMPLETE_AWAITING_CANONICAL_DAILY_ACCEPTANCE",
        "COMPLETE_OPS_067",
    }
    assert ops_067["schema_version"] == "ops_067_reader_brief_quality_fail_closed_finalization.v1"
    assert ops_067["boundary_id"] == "OPS-067"
    assert ops_067["task_ids"] == [
        "OPS-067_READER_BRIEF_QUALITY_FAIL_CLOSED_FINALIZATION",
        "ENG-VAL-010_VALIDATION_PARENT_RUN_PORTABLE_IMPORT",
    ]
    assert ops_067["prior_sections_immutability"] == {
        "source_commit": OPS_067_BASE_COMMIT,
        "repository_path": WAVE11_BASELINE_REPOSITORY_PATH,
        "git_blob_sha1": OPS_067_BASELINE_GIT_BLOB,
        "raw_byte_count": OPS_067_HISTORICAL_PREFIX_BYTE_COUNT,
        "raw_sha256": OPS_067_HISTORICAL_PREFIX_SHA256,
        "append_offset": OPS_067_HISTORICAL_PREFIX_BYTE_COUNT,
        "current_section_must_be_eof": True,
    }
    assert ops_067["implementation"] == {
        "strict_json_contract": True,
        "exact_integer_schema": True,
        "cli_fail_closed": True,
        "step_artifact_gate": True,
        "finalization_under_active_lease": True,
        "two_phase_manifest_publication": True,
        "whole_run_ledger_outcome": True,
        "closed_market_stale_reader_rejected": True,
        "portable_failed_full_parent_import": True,
        "repository_relative_managed_runtime_locators": True,
    }
    assert ops_067["durability_boundary"] == {
        "catchable_exception_compensation": True,
        "power_loss_durable_claimed": False,
        "remaining_owner": "DATA-GOV D0C",
    }

    superseded = set(ops_067["superseded_live_source_paths"])
    assert len(superseded) == 101
    ops_067_live_mismatches = _ops_067_prior_active_source_mismatches()
    assert _ops_068_superseded_live_source_paths() <= ops_067_live_mismatches
    assert ops_067_live_mismatches <= _arch_005s4d_s2_all_superseded_live_source_paths()
    assert _ops_068_superseded_live_source_paths() - superseded == {
        WAVE14_S2_COMPLETED_TASK_SHADOW_PATH
    }
    assert ops_067["supersession"] == {
        "superseded_by_phase": "OPS-067",
        "scope": "ALL_PRIOR_NON_HISTORICAL_SOURCE_RECORDS_FOR_EACH_LISTED_PATH",
        "historical_hashes_rewritten": False,
        "current_hash_authority": f"{OPS_067_SECTION}.sources",
    }
    inherited_removed = set(ops_067["inherited_removed_live_source_paths"])
    assert inherited_removed == {WAVE14_S2_ACTIVE_TASK_SHADOW_PATH}

    expected_new_source_paths = {
        ".gitignore",
        "docs/requirements/ENG-VAL-010_Validation_Parent_Run_Portable_Import.md",
        "docs/requirements/OPS-067_Reader_Brief_Quality_Fail_Closed_Finalization.md",
        (
            "registry/development_tasks_shadow/active/28/"
            "2832c2647b0ebf371274429f2fda1908702e29ee428932ff137afb98871e3eca.yaml"
        ),
        (
            "registry/development_tasks_shadow/active/ca/"
            "ca70315097648d8e19cb7498427fc2eab0fec34ebd092ada8937cfe518cfdfeb.yaml"
        ),
        "scripts/build_validation_parent_run_import.py",
        "scripts/run_validation_tier.py",
        "src/ai_trading_system/cli_commands/reports.py",
        "src/ai_trading_system/contracts/workflow.py",
        "src/ai_trading_system/external_request_cache.py",
        "src/ai_trading_system/platform/artifacts/__init__.py",
        "src/ai_trading_system/platform/artifacts/json_contract.py",
        "src/ai_trading_system/platform/artifacts/writer.py",
        "src/ai_trading_system/platform/operations/runtime_control.py",
        "src/ai_trading_system/platform/validation_parent_run_import.py",
        "src/ai_trading_system/platform/validation_trigger_provenance.py",
        "src/ai_trading_system/run_artifacts.py",
        "tests/test_arch_004c_artifact_writer.py",
        "tests/test_arch_004c_platform_contracts.py",
        "tests/test_arch_004f1_operations_control_plane.py",
        "tests/test_equal_risk_growth_tilt.py",
        "tests/test_ops_daily_finalization.py",
        "tests/test_reader_brief.py",
        "tests/test_report_quality_gate.py",
        "tests/test_run_artifacts.py",
        "tests/test_scheduled_tasks.py",
        "tests/test_validation_parent_run_import.py",
        "tests/test_validation_tier_script.py",
        "tests/test_validation_trigger_provenance.py",
    }
    assert set(ops_067["new_source_paths"]) == expected_new_source_paths
    sources = ops_067["sources"]
    source_paths = [str(source["path"]) for source in sources]
    assert len(source_paths) == 173
    assert len(source_paths) == len(set(source_paths))
    wave14_source_paths = {str(source["path"]) for source in baseline[WAVE14_S2_SECTION]["sources"]}
    assert set(source_paths) == wave14_source_paths | expected_new_source_paths
    assert WAVE11_BASELINE_REPOSITORY_PATH not in source_paths
    assert WAVE14_S2_PROHIBITED_USER_PATH not in source_paths
    assert inherited_removed.isdisjoint(source_paths)
    assert superseded <= set(source_paths) | inherited_removed
    allowed_removed_paths = _removed_live_source_paths_after(OPS_067_SECTION)
    assert ARCH_005M2_ACTIVE_TASK_SHADOW_PATH in allowed_removed_paths
    _assert_wave14_s2_all_sources_tracked(
        source_paths,
        allowed_removed_paths=allowed_removed_paths,
    )
    historical_baseline = safe_load_yaml_text(_ops_068_base_baseline_blob().decode("utf-8"))
    assert isinstance(historical_baseline, dict)
    assert historical_baseline[OPS_067_SECTION] == ops_067

    validation = ops_067["validation"]
    required_engineering_gates = (
        "focused",
        "static",
        "architecture_fitness",
        "contract_validation",
        "integration",
        "reproducibility",
        "full",
    )
    for gate in required_engineering_gates:
        allowed_statuses = {"PENDING", "PASS"}
        if gate == "full":
            allowed_statuses |= {"FAIL_REMEDIATING", "PASS_AFTER_FAILURE_FIX"}
        assert validation[gate]["status"] in allowed_statuses
    assert validation["canonical_daily_acceptance"]["status"] in {
        "PENDING",
        "PASS",
        "BLOCKED",
    }
    if status == "ENGINEERING_COMPLETE_AWAITING_CANONICAL_DAILY_ACCEPTANCE":
        assert all(validation[gate]["status"] == "PASS" for gate in required_engineering_gates)
        assert validation["canonical_daily_acceptance"]["status"] == "PENDING"
    elif status == "COMPLETE_OPS_067":
        assert all(validation[gate]["status"] == "PASS" for gate in required_engineering_gates)
        assert validation["canonical_daily_acceptance"]["status"] == "PASS"

    assert ops_067["source_hash_status"] in {
        "PRE_FORMAL_TRACKED_STATE_FRESH",
        "ENGINEERING_FINAL_TRACKED_STATE_FRESH",
        "FINAL_TRACKED_STATE_FRESH",
    }
    assert ops_067["worktree_attribution"] == {
        "known_unrelated_worktree_files": [WAVE14_S2_PROHIBITED_USER_PATH],
        "active_shared_path_lease_count": 0,
    }
    assert ops_067["safety"] == {
        "strategy_logic_changed": False,
        "strategy_threshold_changed": False,
        "investment_interpretation_changed": False,
        "backtest_or_search_executed": False,
        "data_or_runtime_changed": True,
        "paper_shadow_or_portfolio_mutated": False,
        "production_or_active_shadow_weights_written": False,
        "automatic_command_dispatch_enabled": False,
        "order_or_broker_action": "none",
        "production_effect": "none",
    }


def test_ops_067_rejects_historical_prefix_tamper() -> None:
    base_blob = _ops_067_base_baseline_blob()
    valid_append = base_blob + f"\n{OPS_067_SECTION}:\n  status: TEST_ONLY\n".encode()
    _assert_ops_067_historical_prefix_immutable(valid_append, base_blob)

    tampered = bytearray(valid_append)
    tampered[OPS_067_HISTORICAL_PREFIX_BYTE_COUNT - 1] ^= 1
    with pytest.raises(AssertionError, match="historical prefix differs"):
        _assert_ops_067_historical_prefix_immutable(
            bytes(tampered),
            base_blob,
        )


def test_ops_068_is_append_only_current_hash_authority() -> None:
    _assert_current_ops_068_historical_prefix_immutable()
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    assert next(reversed(baseline)) == LATEST_COMPATIBILITY_SECTION
    assert list(baseline).index(OPS_067_SECTION) < list(baseline).index(OPS_068_SECTION)
    ops_068 = baseline[OPS_068_SECTION]

    assert ops_068["status"] == "COMPLETE_OPS_068"
    assert ops_068["schema_version"] == "ops_068_limited_non_pit_reconstruction.v1"
    assert ops_068["boundary_id"] == "OPS-068"
    assert ops_068["task_ids"] == ["OPS-068_2026_07_21_DAILY_GAP_RECOVERY"]
    assert ops_068["prior_sections_immutability"] == {
        "source_commit": OPS_068_BASE_COMMIT,
        "repository_path": WAVE11_BASELINE_REPOSITORY_PATH,
        "git_blob_sha1": OPS_068_BASELINE_GIT_BLOB,
        "raw_byte_count": OPS_068_HISTORICAL_PREFIX_BYTE_COUNT,
        "raw_sha256": OPS_068_HISTORICAL_PREFIX_SHA256,
        "append_offset": OPS_068_HISTORICAL_PREFIX_BYTE_COUNT,
        "current_section_must_be_eof": True,
    }
    assert ops_068["implementation"] == {
        "explicit_inventory_only": True,
        "cache_only": True,
        "content_derived_validation": True,
        "canonical_guard_before_after": True,
        "strict_pipeline_output_required_null": True,
        "investment_conclusion_required_null": True,
        "latest_or_pointer_publication": False,
    }
    assert ops_068["result"] == {
        "as_of_date": "2026-07-21",
        "status": "LIMITED_NON_PIT_RECONSTRUCTION",
        "canonical_daily_evidence": "MISSING",
        "reconstruction_conclusion": "INSUFFICIENT_DATA",
        "isolated_data_quality_status": "PASS_WITH_WARNINGS",
        "error_count": 0,
        "warning_count": 1,
        "info_count": 13,
        "market_fact_count": 26,
        "macro_fact_count": 3,
        "strict_pipeline_output": None,
        "investment_conclusion": None,
        "production_effect": "none",
    }

    superseded = set(ops_068["superseded_live_source_paths"])
    assert len(superseded) == 102
    ops_068_live_mismatches = _ops_068_prior_active_source_mismatches()
    assert superseded <= ops_068_live_mismatches
    assert ops_068_live_mismatches <= _arch_005s4d_s2_all_superseded_live_source_paths()
    assert ops_068["supersession"] == {
        "superseded_by_phase": "OPS-068",
        "scope": "ALL_PRIOR_NON_HISTORICAL_SOURCE_RECORDS_FOR_EACH_LISTED_PATH",
        "historical_hashes_rewritten": False,
        "current_hash_authority": f"{OPS_068_SECTION}.sources",
    }
    assert ops_068["inherited_removed_live_source_paths"] == [WAVE14_S2_ACTIVE_TASK_SHADOW_PATH]
    assert set(ops_068["new_source_paths"]) == OPS_068_NEW_SOURCE_PATHS

    sources = ops_068["sources"]
    source_paths = [str(source["path"]) for source in sources]
    prior_source_paths = {str(source["path"]) for source in baseline[OPS_067_SECTION]["sources"]}
    assert len(source_paths) == len(set(source_paths))
    assert set(source_paths) == prior_source_paths | OPS_068_NEW_SOURCE_PATHS
    assert WAVE11_BASELINE_REPOSITORY_PATH not in source_paths
    assert WAVE14_S2_PROHIBITED_USER_PATH not in source_paths
    assert set(ops_068["inherited_removed_live_source_paths"]).isdisjoint(source_paths)
    assert superseded <= set(source_paths) | set(ops_068["inherited_removed_live_source_paths"])
    for source in sources:
        assert _source_sha256_at_commit(source, ARCH_005S4D_BASE_COMMIT) == source["sha256"], (
            source["path"]
        )

    assert ops_068["validation"] == {
        "bundle_validation": {"status": "PASS", "checks": 11, "failed": 0},
        "focused": {"status": "PASS", "passed": 4, "failed": 0},
        "fast_unit": {"status": "PASS", "passed": 333, "failed": 0},
        "architecture_fitness_pre_closeout": {
            "status": "FAIL_EXPECTED_GOVERNANCE_CLOSEOUT",
            "passed": 573,
            "failed": 4,
        },
        "post_commit_architecture_fitness_required": True,
        "contract_validation_required": True,
    }
    assert ops_068["worktree_attribution"] == {
        "known_unrelated_worktree_files": [WAVE14_S2_PROHIBITED_USER_PATH],
        "active_shared_path_lease_count": 0,
    }
    assert ops_068["safety"] == {
        "strategy_logic_changed": False,
        "strategy_threshold_changed": False,
        "investment_interpretation_changed": False,
        "backtest_or_search_executed": False,
        "data_or_runtime_changed": True,
        "paper_shadow_or_portfolio_mutated": False,
        "production_or_active_shadow_weights_written": False,
        "automatic_command_dispatch_enabled": False,
        "order_or_broker_action": "none",
        "production_effect": "none",
    }


def test_ops_068_rejects_historical_prefix_tamper() -> None:
    base_blob = _ops_068_base_baseline_blob()
    valid_append = base_blob + f"\n{OPS_068_SECTION}:\n  status: TEST_ONLY\n".encode()
    _assert_ops_068_historical_prefix_immutable(valid_append, base_blob)

    tampered = bytearray(valid_append)
    tampered[OPS_068_HISTORICAL_PREFIX_BYTE_COUNT - 1] ^= 1
    with pytest.raises(AssertionError, match="historical prefix differs"):
        _assert_ops_068_historical_prefix_immutable(
            bytes(tampered),
            base_blob,
        )


def test_arch_005s4d_is_append_only_current_hash_authority() -> None:
    _assert_current_arch_005s4d_historical_prefix_immutable()
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    assert next(reversed(baseline)) == LATEST_COMPATIBILITY_SECTION
    assert list(baseline).index(OPS_068_SECTION) < list(baseline).index(ARCH_005S4D_SECTION)
    phase = baseline[ARCH_005S4D_SECTION]

    assert phase["schema_version"] == "arch_005_s4d_compatibility_closeout.v1"
    assert phase["status"] in {
        "VALIDATING_ARCH_005S4D_NARROW_S0_S1",
        "BASELINE_DONE_ARCH_005S4D_NARROW_S0_S1",
    }
    assert phase["boundary_id"] == "ARCH-005S4D-S0-S1"
    assert phase["task_ids"] == ["ARCH-005S4D_SHARED_CHECKOUT_WRITE_LEASE_GUARD"]
    assert phase["owner_decision"] == (
        "owner_decision:ARCH-005S4D:2026-07-24:approve_narrow_s0_s1_v1"
    )
    assert phase["prior_sections_immutability"] == {
        "source_commit": ARCH_005S4D_BASE_COMMIT,
        "repository_path": WAVE11_BASELINE_REPOSITORY_PATH,
        "git_blob_sha1": ARCH_005S4D_BASELINE_GIT_BLOB,
        "raw_byte_count": ARCH_005S4D_HISTORICAL_PREFIX_BYTE_COUNT,
        "raw_sha256": ARCH_005S4D_HISTORICAL_PREFIX_SHA256,
        "append_offset": ARCH_005S4D_HISTORICAL_PREFIX_BYTE_COUNT,
        "current_section_must_be_eof": True,
    }
    assert phase["implementation"] == {
        "stable_checkout_identity": True,
        "path_and_operation_aware_conflict_matrix": True,
        "existing_execution_lease_authority_reused": True,
        "atomic_acquire_release": True,
        "heartbeat_expiry_replay": True,
        "daily_pre_body_guard": True,
        "dirty_unattributed_fail_closed": True,
        "known_unrelated_exact_path_only": True,
        "pid_is_not_authority": True,
    }

    superseded = set(phase["superseded_live_source_paths"])
    observed_live_mismatches = _arch_005s4d_prior_active_source_mismatches()
    assert superseded <= observed_live_mismatches
    assert observed_live_mismatches <= (_arch_005s4d_s2_all_superseded_live_source_paths())
    assert phase["supersession"] == {
        "superseded_by_phase": "ARCH-005S4D",
        "scope": "EXHAUSTIVE_CURRENT_LIVE_MISMATCH_SET_WITH_DELTA_SOURCES",
        "historical_hashes_rewritten": False,
        "current_hash_authority": f"{ARCH_005S4D_SECTION}.sources",
    }
    expected_new_source_paths = {
        "config/architecture/arch_005_s4d_checkout_guard.yaml",
        "scripts/architecture_arch005_checkout_guard.py",
        "src/ai_trading_system/platform/architecture/checkout_guard.py",
        "tests/test_arch_005_s4d_checkout_guard.py",
    }
    assert set(phase["new_source_paths"]) == expected_new_source_paths
    expected_source_delta_paths = {
        "README.md",
        "docs/architecture/dual_lane_development_operating_model.md",
        "docs/artifact_catalog.md",
        "docs/operations/operations_runbook.md",
        "docs/requirements/ARCH-005_Parallel_Development_Control_Plane.md",
        "docs/requirements/ARCH-005S4D_Shared_Checkout_Write_Lease_Guard.md",
        "docs/system_flow.md",
        "docs/task_register.md",
        "inputs/architecture/arch_004e_aggregate_shadow_index.yaml",
        "inputs/architecture/arch_004e_architecture_fitness.yaml",
        "inputs/architecture/arch_004e_module_manifest.yaml",
        "inputs/architecture/arch_004e_test_manifest.yaml",
        "inputs/architecture/arch_004g_deprecation_inventory.yaml",
        "inputs/architecture/arch_005_task_registry_baseline.yaml",
        "inputs/architecture/arch_005_task_shadow_index.yaml",
        (
            "registry/development_tasks_shadow/active/21/"
            "2187f015345139baf9aa2cf6246e38df0c732be6fbaa18b46bd8fe636b5ead47.yaml"
        ),
        "src/ai_trading_system/cli_commands/ops.py",
        "src/ai_trading_system/platform/architecture/__init__.py",
        "src/ai_trading_system/platform/architecture/parallel_control_kernel.py",
        "tests/test_arch_004_refactor_policy.py",
        "tests/test_arch_004g_deprecation.py",
        "tests/test_ops_daily.py",
        "tests/test_ops_daily_finalization.py",
    }
    assert set(phase["source_delta_paths"]) == expected_source_delta_paths
    assert expected_source_delta_paths <= superseded

    sources = phase["sources"]
    source_paths = [str(source["path"]) for source in sources]
    assert len(source_paths) == len(set(source_paths))
    assert source_paths == sorted(source_paths, key=str.casefold)
    assert set(source_paths) == expected_source_delta_paths | expected_new_source_paths
    assert WAVE11_BASELINE_REPOSITORY_PATH not in source_paths
    assert WAVE14_S2_PROHIBITED_USER_PATH not in source_paths
    for source in sources:
        assert _source_sha256(source) == source["sha256"], source["path"]

    validation = phase["validation"]
    assert validation["focused"] == {"status": "PASS", "passed": 92, "failed": 0}
    assert validation["architecture_fitness_initial"] == {
        "status": "FAIL_REMEDIATING",
        "passed": 576,
        "failed": 15,
        "runtime_artifact": (
            "outputs/validation_runtime/architecture-fitness_"
            "20260724T133821Z/test_runtime_summary.json"
        ),
    }
    for gate in (
        "architecture_fitness",
        "contract_validation",
        "integration",
        "reproducibility",
        "full",
    ):
        allowed_statuses = {"PENDING", "PASS", "PASS_AFTER_FAILURE_FIX"}
        if gate == "full":
            allowed_statuses.add("FAIL_REMEDIATING")
        assert validation[gate]["status"] in allowed_statuses

    assert phase["safety"] == {
        "s2_telemetry_authorized": False,
        "wave15_assignment_authorized": False,
        "task_source_cutover": False,
        "strategy_logic_changed": False,
        "strategy_threshold_changed": False,
        "data_provider_or_daily_executed": False,
        "production_or_active_shadow_weights_written": False,
        "order_or_broker_action": "none",
        "production_effect": "none",
    }


def test_arch_005s4d_rejects_historical_prefix_tamper() -> None:
    base_blob = _arch_005s4d_base_baseline_blob()
    valid_append = base_blob + f"\n{ARCH_005S4D_SECTION}:\n  status: TEST_ONLY\n".encode()
    _assert_arch_005s4d_historical_prefix_immutable(valid_append, base_blob)

    tampered = bytearray(valid_append)
    tampered[ARCH_005S4D_HISTORICAL_PREFIX_BYTE_COUNT - 1] ^= 1
    with pytest.raises(AssertionError, match="historical prefix differs"):
        _assert_arch_005s4d_historical_prefix_immutable(
            bytes(tampered),
            base_blob,
        )


def test_arch_004_wave15_is_append_only_current_hash_authority() -> None:
    _assert_current_wave15_historical_prefix_immutable()
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    assert next(reversed(baseline)) == LATEST_COMPATIBILITY_SECTION
    assert list(baseline).index(ARCH_005S4D_SECTION) < list(baseline).index(WAVE15_SECTION)
    phase = baseline[WAVE15_SECTION]

    assert phase["schema_version"] == "arch_004_wave15_s3_compatibility_closeout.v1"
    assert phase["status"] in {
        "VALIDATING_WAVE15_S3",
        "COMPLETE_WAVE15_S3",
    }
    assert phase["boundary_id"] == "ARCH-004-WAVE15-S3"
    assert phase["task_ids"] == [
        "ARCH-004W15_D0B3_G4B_G3_CLOSE_PARALLEL_READINESS",
        "TRADING-2458_CONSTRAINT_CAUSAL_DIAGNOSTIC",
    ]
    assert phase["prior_sections_immutability"] == {
        "source_commit": WAVE15_BASE_COMMIT,
        "repository_path": WAVE11_BASELINE_REPOSITORY_PATH,
        "git_blob_sha1": WAVE15_BASELINE_GIT_BLOB,
        "raw_byte_count": WAVE15_HISTORICAL_PREFIX_BYTE_COUNT,
        "raw_sha256": WAVE15_HISTORICAL_PREFIX_SHA256,
        "append_offset": WAVE15_HISTORICAL_PREFIX_BYTE_COUNT,
        "current_section_must_be_eof": True,
    }
    assert phase["authorization"] == {
        "strategy_decision": (
            "owner_decision:TRADING-2458:2026-07-25:approve_narrow_constraint_causal_diagnostic_v1"
        ),
        "engineering_decision": (
            "owner_decision:ARCH-004-WAVE15:2026-07-25:approve_narrow_d0b3_g4b_g3_close_v1"
        ),
        "authorization_commit": "3030114be1c07b71eab5af2d8cbf4f54325cb2ef",
        "readiness_carrier_commit": WAVE15_BASE_COMMIT,
        "readiness_carrier_sha256": (
            "50ba37193bf3aef67439e16a9cf3dd3183bb2e20df84fc63ea075293923a2e51"
        ),
    }
    assert phase["generated_state"] == {
        "module_count": 1015,
        "test_file_count": 1181,
        "active_task_count": 410,
        "completed_task_count": 489,
        "total_task_count": 899,
        "deprecation_inventory_id": "arch_004g_deprecation_inventory_0a35fc78bd70264de922",
        "task_registry_shadow_byte_identical": True,
    }
    assert phase["known_unrelated_exclusions"] == [WAVE14_S2_PROHIBITED_USER_PATH]

    superseded = set(phase["superseded_live_source_paths"])
    observed_wave15_mismatches = _wave15_prior_active_source_mismatches()
    assert superseded <= observed_wave15_mismatches
    assert observed_wave15_mismatches <= (_arch_005s4d_s2_all_superseded_live_source_paths())
    assert phase["supersession"] == {
        "superseded_by_phase": "ARCH-004-WAVE15-S3",
        "scope": "EXHAUSTIVE_CURRENT_LIVE_MISMATCH_SET_WITH_DELTA_SOURCES",
        "historical_hashes_rewritten": False,
        "current_hash_authority": f"{WAVE15_SECTION}.sources",
    }
    removed_live_paths = set(phase["removed_live_source_paths"])
    assert removed_live_paths == {WAVE14_S2_ACTIVE_TASK_SHADOW_PATH}
    assert set(phase["source_delta_paths"]) == superseded - removed_live_paths
    assert set(phase["new_source_paths"]) == WAVE15_NEW_SOURCE_PATHS
    assert WAVE14_S2_PROHIBITED_USER_PATH not in superseded
    assert WAVE14_S2_PROHIBITED_USER_PATH not in WAVE15_NEW_SOURCE_PATHS

    sources = phase["sources"]
    source_paths = [str(source["path"]) for source in sources]
    assert len(source_paths) == len(set(source_paths))
    assert source_paths == sorted(source_paths, key=str.casefold)
    assert set(source_paths) == (superseded - removed_live_paths) | WAVE15_NEW_SOURCE_PATHS
    assert WAVE11_BASELINE_REPOSITORY_PATH not in source_paths
    assert WAVE14_S2_PROHIBITED_USER_PATH not in source_paths
    for source in sources:
        assert _source_sha256(source) == source["sha256"], source["path"]

    validation = phase["validation"]
    assert validation["combined_focused_initial"] == {
        "status": "FAIL_EXPECTED_COMPATIBILITY_REFRESH",
        "passed": 175,
        "failed": 9,
    }
    assert validation["known_unrelated_zero_read_regression"] == {
        "status": "PASS",
        "passed": 1,
    }
    for gate in (
        "combined_focused",
        "architecture_fitness",
        "contract_validation",
        "integration",
        "report_validation",
        "reproducibility",
        "full",
    ):
        assert validation[gate]["status"] in {
            "PENDING",
            "PASS",
            "PASS_AFTER_FAILURE_FIX",
        }

    assert phase["safety"] == {
        "current_package_reopened": False,
        "strategy_gate_changed": False,
        "prospective_accessed": False,
        "other_consumer_authorization_allowed": False,
        "automatic_non_daily_dispatch": False,
        "g5_unblocked": False,
        "real_daily_or_provider_executed": False,
        "production_or_active_shadow_weights_written": False,
        "order_or_broker_action": "none",
        "production_effect": "none",
    }


def test_data_gov_001_d0b2b_is_append_only_current_hash_authority() -> None:
    _assert_current_d0b2b_historical_prefix_immutable()
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    assert next(reversed(baseline)) == LATEST_COMPATIBILITY_SECTION
    assert list(baseline).index(WAVE15_SECTION) < list(baseline).index(D0B2B_SECTION)
    phase = baseline[D0B2B_SECTION]

    assert phase["schema_version"] == ("data_gov_001_d0b2b_operational_acceptance_registration.v1")
    assert phase["status"] == "BLOCKED_OWNER_INPUT"
    assert phase["boundary_id"] == "DATA-GOV-001-D0B2B"
    assert phase["task_ids"] == [
        "DATA-GOV-001_D0B2B_CANONICAL_DAILY_ACCEPTANCE_REMEDIATION",
    ]
    assert phase["prior_sections_immutability"] == {
        "source_commit": D0B2B_BASE_COMMIT,
        "repository_path": WAVE11_BASELINE_REPOSITORY_PATH,
        "git_blob_sha1": D0B2B_BASELINE_GIT_BLOB,
        "raw_byte_count": D0B2B_HISTORICAL_PREFIX_BYTE_COUNT,
        "raw_sha256": D0B2B_HISTORICAL_PREFIX_SHA256,
        "append_offset": D0B2B_HISTORICAL_PREFIX_BYTE_COUNT,
        "current_section_must_be_eof": True,
    }
    assert phase["operational_acceptance"] == {
        "scheduler_trigger": "aits ops daily-run",
        "as_of": "2026-07-24",
        "run_id": "daily_ops_run:2026-07-24:20260725T003257Z",
        "run_control_key": "operations_run_3f036b9fc7d836160b483be8",
        "result": "FAIL",
        "download_data": "PASS",
        "validate_data": "FAIL",
        "downstream_step_count": 34,
        "downstream_status": "BLOCKED",
        "data_quality_errors": 3,
        "data_quality_warnings": 1,
        "same_key_retry_allowed": False,
        "canonical_daily_evidence": "MISSING",
        "strict_pipeline_output": None,
        "investment_conclusion": None,
        "production_effect": "none",
    }
    assert phase["owner_input_required"] == {
        "vix_calendar_model_choices": [
            "A_XNYS_DECISION_SESSION_ALIGNED",
            "B_PER_ASSET_CALENDAR",
        ],
        "recommended_choice": "A_XNYS_DECISION_SESSION_ALIGNED",
        "adjustment_event_policy_review_required": True,
        "implementation_authorized": False,
    }
    assert phase["generated_state"] == {
        "active_task_count": 411,
        "completed_task_count": 489,
        "total_task_count": 900,
        "task_registry_shadow_byte_identical": True,
    }
    assert phase["known_unrelated_exclusions"] == [WAVE14_S2_PROHIBITED_USER_PATH]

    superseded = set(phase["superseded_live_source_paths"])
    observed_live_mismatches = _d0b2b_prior_active_source_mismatches()
    assert superseded <= observed_live_mismatches
    assert observed_live_mismatches <= (_arch_005s4d_s2_all_superseded_live_source_paths())
    assert phase["supersession"] == {
        "superseded_by_phase": "DATA-GOV-001-D0B2B",
        "scope": "EXHAUSTIVE_CURRENT_LIVE_MISMATCH_SET_WITH_DELTA_SOURCES",
        "historical_hashes_rewritten": False,
        "current_hash_authority": f"{D0B2B_SECTION}.sources",
    }
    removed_live_paths = set(phase["removed_live_source_paths"])
    assert removed_live_paths == {WAVE14_S2_ACTIVE_TASK_SHADOW_PATH}
    assert set(phase["source_delta_paths"]) == superseded - removed_live_paths
    assert set(phase["new_source_paths"]) == D0B2B_NEW_SOURCE_PATHS
    assert WAVE14_S2_PROHIBITED_USER_PATH not in superseded
    assert WAVE14_S2_PROHIBITED_USER_PATH not in D0B2B_NEW_SOURCE_PATHS

    sources = phase["sources"]
    source_paths = [str(source["path"]) for source in sources]
    assert len(source_paths) == len(set(source_paths))
    assert source_paths == sorted(source_paths, key=str.casefold)
    assert set(source_paths) == (superseded - removed_live_paths) | D0B2B_NEW_SOURCE_PATHS
    assert WAVE11_BASELINE_REPOSITORY_PATH not in source_paths
    assert WAVE14_S2_PROHIBITED_USER_PATH not in source_paths
    for source in sources:
        assert _source_sha256(source) == source["sha256"], source["path"]

    validation = phase["validation"]
    assert validation["task_registry_generate"] == {
        "status": "PASS",
        "task_count": 900,
        "active_task_count": 411,
        "completed_task_count": 489,
        "byte_identical": True,
    }
    assert validation["task_registry_validate"] == {
        "status": "PASS",
        "task_count": 900,
        "active_task_count": 411,
        "completed_task_count": 489,
    }
    assert validation["focused_registry"] == {
        "status": "PASS",
        "passed": 17,
        "failed": 0,
    }
    assert validation["architecture_fitness_initial"] == {
        "status": "FAIL_REMEDIATED",
        "passed": 610,
        "failed": 5,
        "runtime_artifact": (
            "outputs/validation_runtime/"
            "architecture-fitness_20260725T012921Z/test_runtime_summary.json"
        ),
    }
    assert validation["refactor_policy_after_historical_restore"] == {
        "status": "FAIL_EXPECTED_COMPATIBILITY_REFRESH",
        "passed": 27,
        "failed": 1,
    }
    for gate in ("refactor_policy_focused", "architecture_fitness"):
        assert validation[gate]["status"] in {
            "PENDING",
            "PASS",
            "PASS_AFTER_FAILURE_FIX",
        }

    assert phase["safety"] == {
        "same_key_retry_executed": False,
        "canonical_daily_pass_claimed": False,
        "limited_reconstruction_promoted": False,
        "strict_consumer_authorized": False,
        "automatic_non_daily_dispatch": False,
        "strategy_logic_changed": False,
        "calendar_policy_changed": False,
        "production_or_active_shadow_weights_written": False,
        "order_or_broker_action": "none",
        "production_effect": "none",
    }


def test_ops_069_is_append_only_current_hash_authority() -> None:
    current_bytes = COMPATIBILITY_BASELINE_PATH.read_bytes()
    _assert_ops_069_historical_prefix_immutable(
        current_bytes,
        _ops_069_base_baseline_blob(),
    )
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    assert next(reversed(baseline)) == LATEST_COMPATIBILITY_SECTION
    assert list(baseline).index(D0B2B_SECTION) < list(baseline).index(OPS_069_SECTION)
    phase = baseline[OPS_069_SECTION]

    assert phase["schema_version"] == "ops_069_daily_input_capture.v1"
    assert phase["status"] == "IMPLEMENTED_AWAITING_OPERATIONAL_ACCEPTANCE"
    assert phase["boundary_id"] == "OPS-069"
    assert phase["task_ids"] == [
        "OPS-069_DAILY_INPUT_CAPTURE_AND_SESSION_GAP_LEDGER",
    ]
    assert phase["prior_sections_immutability"] == {
        "source_commit": OPS_069_BASE_COMMIT,
        "repository_path": WAVE11_BASELINE_REPOSITORY_PATH,
        "git_blob_sha1": OPS_069_BASELINE_GIT_BLOB,
        "raw_byte_count": OPS_069_HISTORICAL_PREFIX_BYTE_COUNT,
        "raw_sha256": OPS_069_HISTORICAL_PREFIX_SHA256,
        "append_offset": OPS_069_HISTORICAL_PREFIX_BYTE_COUNT,
        "current_section_must_be_eof": True,
    }
    assert phase["known_unrelated_exclusions"] == [WAVE14_S2_PROHIBITED_USER_PATH]

    superseded = set(phase["superseded_live_source_paths"])
    observed_live_mismatches = _ops_069_prior_active_source_mismatches()
    assert superseded <= observed_live_mismatches
    assert observed_live_mismatches <= (_arch_005s4d_s2_all_superseded_live_source_paths())
    assert phase["supersession"] == {
        "superseded_by_phase": "OPS-069",
        "scope": "EXHAUSTIVE_CURRENT_LIVE_MISMATCH_SET_WITH_NEW_SOURCES",
        "historical_hashes_rewritten": False,
        "current_hash_authority": f"{OPS_069_SECTION}.sources",
    }
    removed_live_paths = set(phase["removed_live_source_paths"])
    assert removed_live_paths == {WAVE14_S2_ACTIVE_TASK_SHADOW_PATH}
    assert set(phase["source_delta_paths"]) == superseded - removed_live_paths
    assert set(phase["new_source_paths"]) == OPS_069_NEW_SOURCE_PATHS
    assert WAVE14_S2_PROHIBITED_USER_PATH not in superseded
    assert WAVE14_S2_PROHIBITED_USER_PATH not in OPS_069_NEW_SOURCE_PATHS

    sources = phase["sources"]
    source_paths = [str(source["path"]) for source in sources]
    assert len(source_paths) == len(set(source_paths))
    assert source_paths == sorted(source_paths, key=str.casefold)
    assert set(source_paths) == (superseded - removed_live_paths) | OPS_069_NEW_SOURCE_PATHS
    assert WAVE11_BASELINE_REPOSITORY_PATH not in source_paths
    assert WAVE14_S2_PROHIBITED_USER_PATH not in source_paths
    for source in sources:
        assert _source_sha256(source) == source["sha256"], source["path"]

    assert phase["validation"] == {
        "engineering_status": "PENDING_FORMAL_CLOSEOUT",
        "operational_acceptance": "PENDING_NEXT_PROVIDER_READY_TRADING_DATE",
    }
    assert phase["safety"] == {
        "same_key_retry_executed": False,
        "canonical_daily_pass_claimed": False,
        "strict_consumer_gate_relaxed": False,
        "historical_strict_pit_backfilled": False,
        "production_or_active_shadow_weights_written": False,
        "order_or_broker_action": "none",
        "production_effect": "none",
    }


def test_ops_070_is_append_only_current_hash_authority() -> None:
    current_bytes = COMPATIBILITY_BASELINE_PATH.read_bytes()
    _assert_ops_070_historical_prefix_immutable(
        current_bytes,
        _ops_070_base_baseline_blob(),
    )
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    assert next(reversed(baseline)) == LATEST_COMPATIBILITY_SECTION
    assert list(baseline).index(OPS_069_SECTION) < list(baseline).index(OPS_070_SECTION)
    phase = baseline[OPS_070_SECTION]

    assert phase["schema_version"] == "ops_070_dependency_dag.v1"
    assert phase["status"] == "ENGINEERING_COMPLETE_OWNER_DEPLOYMENT_PENDING"
    assert phase["boundary_id"] == "OPS-070"
    assert phase["task_ids"] == [
        "OPS-070_OBJECTIVE_BLOCKER_AND_CONSUMER_DEPENDENCY_DAG",
    ]
    assert phase["prior_sections_immutability"] == {
        "source_commit": OPS_070_BASE_COMMIT,
        "repository_path": WAVE11_BASELINE_REPOSITORY_PATH,
        "git_blob_sha1": OPS_070_BASELINE_GIT_BLOB,
        "raw_byte_count": OPS_070_HISTORICAL_PREFIX_BYTE_COUNT,
        "raw_sha256": OPS_070_HISTORICAL_PREFIX_SHA256,
        "append_offset": OPS_070_HISTORICAL_PREFIX_BYTE_COUNT,
        "current_section_must_be_eof": True,
    }
    assert phase["known_unrelated_exclusions"] == [WAVE14_S2_PROHIBITED_USER_PATH]

    superseded = set(phase["superseded_live_source_paths"])
    observed_live_mismatches = _ops_070_prior_active_source_mismatches()
    assert superseded <= observed_live_mismatches
    assert observed_live_mismatches <= (_arch_005s4d_s2_all_superseded_live_source_paths())
    assert phase["supersession"] == {
        "superseded_by_phase": "OPS-070",
        "scope": "EXHAUSTIVE_CURRENT_LIVE_MISMATCH_SET_WITH_NEW_SOURCES",
        "historical_hashes_rewritten": False,
        "current_hash_authority": f"{OPS_070_SECTION}.sources",
    }
    removed_live_paths = set(phase["removed_live_source_paths"])
    assert removed_live_paths == {WAVE14_S2_ACTIVE_TASK_SHADOW_PATH}
    assert set(phase["source_delta_paths"]) == superseded
    assert set(phase["new_source_paths"]) == OPS_070_NEW_SOURCE_PATHS
    assert WAVE14_S2_PROHIBITED_USER_PATH not in superseded
    assert WAVE14_S2_PROHIBITED_USER_PATH not in OPS_070_NEW_SOURCE_PATHS

    sources = phase["sources"]
    source_paths = [str(source["path"]) for source in sources]
    assert len(source_paths) == len(set(source_paths))
    assert source_paths == sorted(source_paths, key=str.casefold)
    assert set(source_paths) == (superseded - removed_live_paths) | OPS_070_NEW_SOURCE_PATHS
    assert WAVE11_BASELINE_REPOSITORY_PATH not in source_paths
    assert WAVE14_S2_PROHIBITED_USER_PATH not in source_paths
    for source in sources:
        assert _source_sha256(source) == source["sha256"], source["path"]

    assert phase["validation"] == {
        "engineering_status": "FORMAL_VALIDATION_PASS",
        "focused": "PASS_133",
        "architecture_fitness": "PASS_622",
        "contract_validation": "PASS_275",
        "integration": "PASS_994",
        "reproducibility": "PASS_23",
        "full": "PASS_7233_SKIPPED_4",
        "operational_acceptance": "PENDING_NEXT_PROVIDER_READY_TRADING_DATE",
    }
    assert phase["safety"] == {
        "same_key_retry_executed": False,
        "canonical_daily_pass_claimed": False,
        "strict_consumer_gate_relaxed": False,
        "historical_strict_pit_backfilled": False,
        "scheduler_or_credentials_deployed": False,
        "production_or_active_shadow_weights_written": False,
        "order_or_broker_action": "none",
        "production_effect": "none",
    }


def test_arch_005s4e_is_append_only_current_hash_authority() -> None:
    current_bytes = COMPATIBILITY_BASELINE_PATH.read_bytes()
    _assert_arch_005s4e_historical_prefix_immutable(
        current_bytes,
        _arch_005s4e_base_baseline_blob(),
    )
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    assert next(reversed(baseline)) == LATEST_COMPATIBILITY_SECTION
    assert list(baseline).index(OPS_070_SECTION) < list(baseline).index(ARCH_005S4E_SECTION)
    phase = baseline[ARCH_005S4E_SECTION]

    assert phase["schema_version"] == "arch_005_s4e_compatibility_closeout.v1"
    assert phase["status"] in {
        "VALIDATING_ARCH_005S4E_S0_S1",
        "BASELINE_DONE_ARCH_005S4E_S0_S1_S2_PENDING",
        "BASELINE_DONE_ARCH_005S4E_S0_S2",
    }
    assert phase["boundary_id"] in {"ARCH-005S4E-S0-S1", "ARCH-005S4E-S0-S2"}
    assert phase["task_ids"] == [
        "ARCH-005S4E_CHECKOUT_HANDOFF_AND_SOURCE_RECONCILIATION",
    ]
    assert phase["owner_decision"] == (
        "owner_decision:ARCH-005S4E:2026-07-25:approve_checkout_handoff_reconciliation_v1"
    )
    assert phase["prior_sections_immutability"] == {
        "source_commit": ARCH_005S4E_BASE_COMMIT,
        "repository_path": WAVE11_BASELINE_REPOSITORY_PATH,
        "git_blob_sha1": ARCH_005S4E_BASELINE_GIT_BLOB,
        "raw_byte_count": ARCH_005S4E_HISTORICAL_PREFIX_BYTE_COUNT,
        "raw_sha256": ARCH_005S4E_HISTORICAL_PREFIX_SHA256,
        "append_offset": ARCH_005S4E_HISTORICAL_PREFIX_BYTE_COUNT,
        "current_section_must_be_eof": True,
    }
    assert phase["implementation"] == {
        "protected_main_domain_mutation_allowed": False,
        "protected_main_shared_actor": "integration-coordinator",
        "handoff_schema": "checkout_handoff.v1",
        "reconciliation_schema": "checkout_reconciliation_report.v1",
        "target_lineage": "FIRST_PARENT_ONLY",
        "automatic_cleanup_allowed": False,
        "generated_rebuild_owner": "integration-coordinator",
        "s2_current_main_reconciliation": ("PASS_RECONCILED_MAIN_KNOWN_UNRELATED_EXCLUDED"),
    }
    assert phase["recovery_audit"] == {
        "source_commit": "fc6313416d78f56a29519f41ca564eaa1f90e8ce",
        "target_commit": "913232c7519ca96a0041ae525b53e9b8e43dc331",
        "preservation_commit": "e45d77158",
        "canonical_restore_commit": "913232c75",
        "handoff_checksum": ("c14067df6affa0d5cb973435c743aaf300e54ad3d6040c56dec5b744513ee349"),
        "report_checksum": ("57ad581a0cc070dfe19908c54289414eef7fc34c362766f95d3dfb6483d18439"),
        "decision": "READY_FOR_COORDINATOR_RECONCILIATION",
        "exact_target_count": 9,
        "target_history_count": 17,
        "generated_rebuild_count": 324,
        "known_unrelated_not_read_count": 1,
        "blocking_count": 0,
        "unattributed_count": 0,
        "retained_count": 0,
        "tracked_paths_restored": 343,
        "untracked_paths_removed": 7,
        "automatic_cleanup_allowed": False,
    }
    assert phase["known_unrelated_exclusions"] == [WAVE14_S2_PROHIBITED_USER_PATH]

    superseded = set(phase["superseded_live_source_paths"])
    observed_live_mismatches = _arch_005s4e_prior_active_source_mismatches()
    assert superseded <= observed_live_mismatches
    assert observed_live_mismatches <= (_arch_005s4d_s2_all_superseded_live_source_paths())
    assert phase["supersession"] == {
        "superseded_by_phase": "ARCH-005S4E",
        "scope": "EXHAUSTIVE_CURRENT_LIVE_MISMATCH_SET_WITH_NEW_SOURCES",
        "historical_hashes_rewritten": False,
        "current_hash_authority": f"{ARCH_005S4E_SECTION}.sources",
    }
    removed_live_paths = set(phase["removed_live_source_paths"])
    assert removed_live_paths == {WAVE14_S2_ACTIVE_TASK_SHADOW_PATH}
    assert set(phase["source_delta_paths"]) == superseded
    assert set(phase["new_source_paths"]) == ARCH_005S4E_NEW_SOURCE_PATHS
    assert WAVE14_S2_PROHIBITED_USER_PATH not in superseded
    assert WAVE14_S2_PROHIBITED_USER_PATH not in ARCH_005S4E_NEW_SOURCE_PATHS

    sources = phase["sources"]
    source_paths = [str(source["path"]) for source in sources]
    assert len(source_paths) == len(set(source_paths))
    assert source_paths == sorted(source_paths, key=str.casefold)
    assert set(source_paths) == (superseded - removed_live_paths) | ARCH_005S4E_NEW_SOURCE_PATHS
    assert WAVE11_BASELINE_REPOSITORY_PATH not in source_paths
    assert WAVE14_S2_PROHIBITED_USER_PATH not in source_paths
    for source in sources:
        assert _source_sha256(source) == source["sha256"], source["path"]

    validation = phase["validation"]
    assert validation["engineering_status"] in {
        "PENDING_FORMAL_CLOSEOUT",
        "FORMAL_VALIDATION_PASS",
    }
    assert validation["focused"] == "PASS_20"
    for tier in (
        "architecture_fitness",
        "contract_validation",
        "integration",
        "reproducibility",
    ):
        assert validation[tier] == "PENDING" or str(validation[tier]).startswith("PASS_")
    assert validation["full"] == "PENDING" or str(validation["full"]).startswith("PASS_")
    assert phase["safety"] == {
        "automatic_cleanup_allowed": False,
        "automatic_restore_allowed": False,
        "automatic_delete_allowed": False,
        "automatic_commit_allowed": False,
        "automatic_merge_allowed": False,
        "automatic_push_allowed": False,
        "task_source_cutover": False,
        "strategy_logic_changed": False,
        "strategy_threshold_changed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def test_arch_005s4d_s2_is_append_only_current_hash_authority() -> None:
    current_bytes = COMPATIBILITY_BASELINE_PATH.read_bytes()
    _assert_arch_005s4d_s2_historical_prefix_immutable(
        current_bytes,
        _arch_005s4d_s2_base_baseline_blob(),
    )
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    assert next(reversed(baseline)) == LATEST_COMPATIBILITY_SECTION
    assert list(baseline).index(ARCH_005S4E_SECTION) < list(baseline).index(ARCH_005S4D_S2_SECTION)
    assert list(baseline).index(ARCH_005S4D_S2_SECTION) < list(baseline).index(
        TRADING_2458_2460_INTEGRATION_SECTION
    )
    phase = baseline[ARCH_005S4D_S2_SECTION]

    assert phase["schema_version"] == "arch_005_s4d_s2_compatibility_closeout.v1"
    assert phase["status"] in {
        "VALIDATING_ARCH_005S4D_S2_READ_ONLY_TELEMETRY",
        "BASELINE_DONE_ARCH_005S4D_S2_READ_ONLY_TELEMETRY",
    }
    assert phase["boundary_id"] == "ARCH-005S4D-S2"
    assert phase["task_ids"] == [
        "ARCH-005S4D_SHARED_CHECKOUT_WRITE_LEASE_GUARD",
    ]
    assert phase["owner_decision"] == (
        "owner_decision:ARCH-005S4D-S2:2026-07-26:approve_read_only_telemetry_v1"
    )
    assert phase["prior_sections_immutability"] == {
        "source_commit": ARCH_005S4D_S2_BASE_COMMIT,
        "repository_path": WAVE11_BASELINE_REPOSITORY_PATH,
        "git_blob_sha1": ARCH_005S4D_S2_BASELINE_GIT_BLOB,
        "raw_byte_count": ARCH_005S4D_S2_HISTORICAL_PREFIX_BYTE_COUNT,
        "raw_sha256": ARCH_005S4D_S2_HISTORICAL_PREFIX_SHA256,
        "append_offset": ARCH_005S4D_S2_HISTORICAL_PREFIX_BYTE_COUNT,
        "current_section_must_be_eof": True,
    }
    assert phase["implementation"] == {
        "policy_version": "arch_005_s4d_checkout_guard@1.2.0",
        "snapshot_schema": "checkout_guard_telemetry_snapshot.v1",
        "rollup_schema": "checkout_guard_telemetry_rollup.v1",
        "false_block_review_schema": "checkout_guard_false_block_review.v1",
        "minimum_observation_batches": 2,
        "raw_source_sha256_binding": True,
        "causal_lease_replay": True,
        "governed_immutable_output": True,
        "task_governance_status_mutation": False,
        "automatic_task_mutation": False,
        "s5_cutover_authorized": False,
    }
    evidence = phase["telemetry_evidence"]
    assert evidence["observed_batch_count"] in {1, 2}
    assert evidence["s4c_main_snapshot_id"] == ("checkout-telemetry-18eee4c13adf0b0490dd")
    assert evidence["s4c_main_expected_block_count"] == 7
    assert evidence["s4c_main_confirmed_false_block_count"] == 0
    assert evidence["s4c_main_unreviewed_block_count"] == 0
    assert evidence["s4c_main_active_lease_count"] == 0
    assert evidence["s5_owner_decision_required"] is True
    assert phase["known_unrelated_exclusions"] == [
        WAVE14_S2_PROHIBITED_USER_PATH,
    ]

    superseded = set(phase["superseded_live_source_paths"])
    observed_live_mismatches = _arch_005s4d_s2_prior_active_source_mismatches()
    assert superseded <= observed_live_mismatches
    assert observed_live_mismatches <= (_arch_005s4d_s2_all_superseded_live_source_paths())
    assert phase["supersession"] == {
        "superseded_by_phase": "ARCH-005S4D-S2",
        "scope": "LATEST_ACTIVE_CURRENT_MISMATCH_SET_WITH_NEW_SOURCES",
        "historical_hashes_rewritten": False,
        "inherited_supersession_authority": ARCH_005S4E_SECTION,
        "current_hash_authority": f"{ARCH_005S4D_S2_SECTION}.sources",
    }
    assert phase["removed_live_source_paths"] == []
    assert set(phase["source_delta_paths"]) == superseded
    assert set(phase["new_source_paths"]) == ARCH_005S4D_S2_NEW_SOURCE_PATHS
    assert WAVE14_S2_PROHIBITED_USER_PATH not in superseded
    assert WAVE14_S2_PROHIBITED_USER_PATH not in ARCH_005S4D_S2_NEW_SOURCE_PATHS

    sources = phase["sources"]
    source_paths = [str(source["path"]) for source in sources]
    assert len(source_paths) == len(set(source_paths))
    assert source_paths == sorted(source_paths, key=str.casefold)
    assert set(source_paths) == superseded | ARCH_005S4D_S2_NEW_SOURCE_PATHS
    assert WAVE11_BASELINE_REPOSITORY_PATH not in source_paths
    assert WAVE14_S2_PROHIBITED_USER_PATH not in source_paths
    for source in sources:
        assert _source_sha256(source) == source["sha256"], source["path"]

    validation = phase["validation"]
    assert validation["engineering_status"] in {
        "PENDING_FORMAL_CLOSEOUT",
        "FORMAL_VALIDATION_PASS",
    }
    assert str(validation["focused"]).startswith("PASS_")
    for tier in (
        "architecture_fitness",
        "contract_validation",
        "integration",
        "reproducibility",
        "full",
    ):
        assert validation[tier] == "PENDING" or str(validation[tier]).startswith("PASS_")
    assert phase["safety"] == {
        "task_governance_status_mutated": False,
        "automatic_task_mutation": False,
        "automatic_scheduler_priority_change": False,
        "automatic_cleanup_allowed": False,
        "automatic_commit_allowed": False,
        "automatic_merge_allowed": False,
        "automatic_push_allowed": False,
        "task_source_cutover": False,
        "s5_cutover_authorized": False,
        "strategy_logic_changed": False,
        "strategy_threshold_changed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def test_trading_2458_2460_integration_is_append_only_current_hash_authority() -> None:
    current_bytes = COMPATIBILITY_BASELINE_PATH.read_bytes()
    _assert_trading_2458_2460_integration_historical_prefix_immutable(
        current_bytes,
        _trading_2458_2460_integration_base_baseline_blob(),
    )
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    assert next(reversed(baseline)) == LATEST_COMPATIBILITY_SECTION
    phase = baseline[TRADING_2458_2460_INTEGRATION_SECTION]

    assert phase["schema_version"] == ("trading_2458_2460_clean_main_integration_compatibility.v1")
    assert phase["status"] in {
        "VALIDATING_TRADING_2458_2460_CLEAN_MAIN_INTEGRATION",
        "COMPLETE_TRADING_2458_2460_CLEAN_MAIN_INTEGRATION",
    }
    assert phase["boundary_id"] == "TRADING-2458-2460-CLEAN-MAIN-INTEGRATION"
    assert phase["task_ids"] == [
        "TRADING-2458_CONSTRAINT_CAUSAL_DIAGNOSTIC",
        "TRADING-2459_STRATEGY_STYLE_DISCOVERY_SPY_QLD_UNIVERSE",
        "TRADING-2460_DECISION_TARGET_CAPABILITY_AUDIT_LABEL_FOUNDATION",
        "DEVX-001_TEMPORARY_WORKSPACE_LIFECYCLE_AND_CLEANUP",
    ]
    assert phase["owner_authorization"] == (
        "owner_authorization:DEVX-001:2026-07-26:"
        "process_three_existing_worktrees_in_recommended_order"
    )
    assert phase["prior_sections_immutability"] == {
        "source_commit": TRADING_2458_2460_INTEGRATION_BASE_COMMIT,
        "repository_path": WAVE11_BASELINE_REPOSITORY_PATH,
        "git_blob_sha1": TRADING_2458_2460_INTEGRATION_BASELINE_GIT_BLOB,
        "raw_byte_count": TRADING_2458_2460_INTEGRATION_HISTORICAL_PREFIX_BYTE_COUNT,
        "raw_sha256": TRADING_2458_2460_INTEGRATION_HISTORICAL_PREFIX_SHA256,
        "append_offset": TRADING_2458_2460_INTEGRATION_HISTORICAL_PREFIX_BYTE_COUNT,
        "current_section_must_be_eof": True,
    }
    assert phase["implementation"] == {
        "source_checkpoint_commit": "95a26bcac",
        "source_worktree": ("D:\\Work\\AITradingSystem-TRADING-2459-style-discovery"),
        "integration_worktree": ("D:\\Work\\AITradingSystem_trading2459_integration_20260726"),
        "integration_branch": "codex/trading-2458-2460-integration",
        "clean_main_replay": True,
        "generated_authority_rebuilt": True,
        "runtime_evidence_sha256_verified": True,
        "old_runtime_artifact_group_count": 3,
        "old_runtime_artifact_byte_identical": True,
    }
    assert phase["known_unrelated_exclusions"] == [
        WAVE14_S2_PROHIBITED_USER_PATH,
    ]

    superseded = set(phase["superseded_live_source_paths"])
    observed_live_mismatches = _trading_2458_2460_integration_prior_active_source_mismatches()
    assert superseded <= observed_live_mismatches
    assert observed_live_mismatches <= _arch_005s4d_s2_all_superseded_live_source_paths()
    assert phase["supersession"] == {
        "superseded_by_phase": "TRADING-2458-2460-CLEAN-MAIN-INTEGRATION",
        "scope": "LATEST_ACTIVE_CURRENT_MISMATCH_SET_WITH_NEW_SOURCES",
        "historical_hashes_rewritten": False,
        "inherited_supersession_authority": ARCH_005S4D_S2_SECTION,
        "current_hash_authority": (f"{TRADING_2458_2460_INTEGRATION_SECTION}.sources"),
    }
    assert phase["removed_live_source_paths"] == []
    assert set(phase["source_delta_paths"]) == superseded
    assert set(phase["new_source_paths"]) == (
        TRADING_2458_2460_INTEGRATION_SOURCE_PATHS - superseded
    )

    sources = phase["sources"]
    source_paths = [str(source["path"]) for source in sources]
    assert len(source_paths) == len(set(source_paths))
    assert source_paths == sorted(source_paths, key=str.casefold)
    assert set(source_paths) == TRADING_2458_2460_INTEGRATION_SOURCE_PATHS
    assert WAVE11_BASELINE_REPOSITORY_PATH not in source_paths
    assert WAVE14_S2_PROHIBITED_USER_PATH not in source_paths
    for source in sources:
        assert _source_sha256(source) == source["sha256"], source["path"]

    validation = phase["validation"]
    assert validation["engineering_status"] in {
        "PENDING_FORMAL_CLOSEOUT",
        "FORMAL_VALIDATION_PASS",
    }
    for tier in (
        "focused",
        "architecture_fitness",
        "contract_validation",
        "report_validation",
        "reproducibility",
        "full",
    ):
        assert validation[tier] == "PENDING" or str(validation[tier]).startswith("PASS_")
    assert phase["safety"] == {
        "research_only": True,
        "historical_seen_only": True,
        "strategy_logic_changed": False,
        "strategy_threshold_changed": False,
        "official_target_weights_changed": False,
        "automatic_execution_allowed": False,
        "paper_shadow_changed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def test_devx_trading_cleanup_is_append_only_current_hash_authority() -> None:
    current_bytes = COMPATIBILITY_BASELINE_PATH.read_bytes()
    _assert_devx_trading_cleanup_historical_prefix_immutable(
        current_bytes,
        _devx_trading_cleanup_base_baseline_blob(),
    )
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    assert next(reversed(baseline)) == LATEST_COMPATIBILITY_SECTION
    phase = baseline[DEVX_TRADING_CLEANUP_SECTION]

    assert phase["schema_version"] == "devx_001_trading_workspace_cleanup_compatibility.v1"
    assert phase["status"] == "COMPLETE_TRADING_WORKSPACE_CLEANUP"
    assert phase["boundary_id"] == "DEVX-001-TRADING-WORKSPACE-CLEANUP"
    assert phase["task_ids"] == [
        "DEVX-001_TEMPORARY_WORKSPACE_LIFECYCLE_AND_CLEANUP",
        "TRADING-2458_CONSTRAINT_CAUSAL_DIAGNOSTIC",
        "TRADING-2459_STRATEGY_STYLE_DISCOVERY_SPY_QLD_UNIVERSE",
        "TRADING-2460_DECISION_TARGET_CAPABILITY_AUDIT_LABEL_FOUNDATION",
    ]
    assert phase["prior_sections_immutability"] == {
        "source_commit": DEVX_TRADING_CLEANUP_BASE_COMMIT,
        "repository_path": WAVE11_BASELINE_REPOSITORY_PATH,
        "git_blob_sha1": DEVX_TRADING_CLEANUP_BASELINE_GIT_BLOB,
        "raw_byte_count": DEVX_TRADING_CLEANUP_HISTORICAL_PREFIX_BYTE_COUNT,
        "raw_sha256": DEVX_TRADING_CLEANUP_HISTORICAL_PREFIX_SHA256,
        "append_offset": DEVX_TRADING_CLEANUP_HISTORICAL_PREFIX_BYTE_COUNT,
        "current_section_must_be_eof": True,
    }
    assert phase["cleanup"] == {
        "integration_commit": "0f585879650f3433008bbbfbbaf52f47dba1ae15",
        "source_checkpoint_commit": "95a26bcac11db460840736724226e6a37ff1e07d",
        "removed_worktrees": [
            "D:\\Work\\AITradingSystem-TRADING-2459-style-discovery",
            "D:\\Work\\AITradingSystem_trading2459_integration_20260726",
        ],
        "removed_file_count": 21_289,
        "removed_logical_bytes": 573_489_392,
        "canonical_validation_group_count": 8,
        "canonical_validation_file_count": 25,
        "canonical_validation_bytes": 13_924_990,
        "canonical_validation_sha256_verified": True,
        "active_process_reference_count": 0,
        "worktree_registration_removed": True,
    }
    assert phase["known_unrelated_exclusions"] == [
        WAVE14_S2_PROHIBITED_USER_PATH,
    ]

    superseded = set(phase["superseded_live_source_paths"])
    observed_live_mismatches = _devx_trading_cleanup_prior_active_source_mismatches()
    assert DEVX_TRADING_CLEANUP_SOURCE_PATHS <= observed_live_mismatches
    assert observed_live_mismatches <= _arch_005s4d_s2_all_superseded_live_source_paths()
    assert superseded == DEVX_TRADING_CLEANUP_SOURCE_PATHS
    assert phase["supersession"] == {
        "superseded_by_phase": "DEVX-001-TRADING-WORKSPACE-CLEANUP",
        "scope": "LATEST_ACTIVE_CURRENT_MISMATCH_SET",
        "historical_hashes_rewritten": False,
        "inherited_supersession_authority": TRADING_2458_2460_INTEGRATION_SECTION,
        "current_hash_authority": f"{DEVX_TRADING_CLEANUP_SECTION}.sources",
    }
    assert phase["removed_live_source_paths"] == []
    assert set(phase["source_delta_paths"]) == superseded
    assert phase["new_source_paths"] == []

    sources = phase["sources"]
    source_paths = [str(source["path"]) for source in sources]
    assert len(source_paths) == len(set(source_paths))
    assert source_paths == sorted(source_paths, key=str.casefold)
    assert set(source_paths) == DEVX_TRADING_CLEANUP_SOURCE_PATHS
    assert WAVE11_BASELINE_REPOSITORY_PATH not in source_paths
    assert WAVE14_S2_PROHIBITED_USER_PATH not in source_paths
    for source in sources:
        assert _source_sha256(source) == source["sha256"], source["path"]

    assert phase["validation"] == {
        "engineering_status": "FORMAL_VALIDATION_PASS",
        "focused": "PASS_64",
        "task_registry": "PASS_BYTE_IDENTICAL",
        "architecture_devex": "PASS",
        "worktree_cleanup_audit": "PASS",
    }
    assert phase["safety"] == {
        "task_status_remains_in_progress_for_ops_runtime": True,
        "strategy_logic_changed": False,
        "strategy_threshold_changed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def test_devx_trading_cleanup_rejects_historical_prefix_tamper() -> None:
    base_blob = _devx_trading_cleanup_base_baseline_blob()
    tampered = bytearray(COMPATIBILITY_BASELINE_PATH.read_bytes())
    tampered[DEVX_TRADING_CLEANUP_HISTORICAL_PREFIX_BYTE_COUNT - 1] ^= 1
    with pytest.raises(AssertionError, match="historical prefix differs"):
        _assert_devx_trading_cleanup_historical_prefix_immutable(
            bytes(tampered),
            base_blob,
        )


def test_trading_2459_documentation_closeout_is_current_hash_authority() -> None:
    current_bytes = COMPATIBILITY_BASELINE_PATH.read_bytes()
    _assert_trading_2459_doc_closeout_historical_prefix_immutable(
        current_bytes,
        _trading_2459_doc_closeout_base_baseline_blob(),
    )
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    assert next(reversed(baseline)) == LATEST_COMPATIBILITY_SECTION
    assert list(baseline).index(DEVX_TRADING_CLEANUP_SECTION) < list(baseline).index(
        TRADING_2459_DOC_CLOSEOUT_SECTION
    )
    phase = baseline[TRADING_2459_DOC_CLOSEOUT_SECTION]

    assert phase["schema_version"] == "trading_2459_documentation_closeout_compatibility.v1"
    assert phase["status"] == "COMPLETE_TRADING_2459_DOCUMENTATION_CLOSEOUT"
    assert phase["boundary_id"] == "TRADING-2459-DOCUMENTATION-CLOSEOUT"
    assert phase["task_ids"] == [
        "TRADING-2459_STRATEGY_STYLE_DISCOVERY_SPY_QLD_UNIVERSE",
    ]
    assert phase["owner_authorization"] == (
        "owner_request:TRADING-2459:2026-07-26:correct_closeout_documentation"
    )
    assert phase["prior_sections_immutability"] == {
        "source_commit": TRADING_2459_DOC_CLOSEOUT_BASE_COMMIT,
        "repository_path": WAVE11_BASELINE_REPOSITORY_PATH,
        "git_blob_sha1": TRADING_2459_DOC_CLOSEOUT_BASELINE_GIT_BLOB,
        "raw_byte_count": TRADING_2459_DOC_CLOSEOUT_HISTORICAL_PREFIX_BYTE_COUNT,
        "raw_sha256": TRADING_2459_DOC_CLOSEOUT_HISTORICAL_PREFIX_SHA256,
        "append_offset": TRADING_2459_DOC_CLOSEOUT_HISTORICAL_PREFIX_BYTE_COUNT,
        "current_section_must_be_eof": True,
    }
    assert phase["known_unrelated_exclusions"] == [
        WAVE14_S2_PROHIBITED_USER_PATH,
    ]

    superseded = set(phase["superseded_live_source_paths"])
    observed_live_mismatches = _trading_2459_doc_closeout_prior_active_source_mismatches()
    assert superseded <= observed_live_mismatches
    assert observed_live_mismatches <= _arch_005s4d_s2_all_superseded_live_source_paths()
    assert superseded == TRADING_2459_DOC_CLOSEOUT_SOURCE_PATHS
    assert phase["supersession"] == {
        "superseded_by_phase": "TRADING-2459-DOCUMENTATION-CLOSEOUT",
        "scope": "LATEST_ACTIVE_CURRENT_MISMATCH_SET",
        "historical_hashes_rewritten": False,
        "inherited_supersession_authority": DEVX_TRADING_CLEANUP_SECTION,
        "current_hash_authority": f"{TRADING_2459_DOC_CLOSEOUT_SECTION}.sources",
    }
    assert phase["removed_live_source_paths"] == []
    assert set(phase["source_delta_paths"]) == superseded
    assert phase["new_source_paths"] == []

    sources = phase["sources"]
    source_paths = [str(source["path"]) for source in sources]
    assert len(source_paths) == len(set(source_paths))
    assert source_paths == sorted(source_paths, key=str.casefold)
    assert set(source_paths) == TRADING_2459_DOC_CLOSEOUT_SOURCE_PATHS
    assert WAVE11_BASELINE_REPOSITORY_PATH not in source_paths
    assert WAVE14_S2_PROHIBITED_USER_PATH not in source_paths
    for source in sources:
        assert _source_sha256(source) == source["sha256"], source["path"]

    assert phase["validation"] == {
        "engineering_status": "FORMAL_VALIDATION_PASS",
        "focused": "PASS",
        "task_registry": "PASS_BYTE_IDENTICAL",
        "worktree_audit": "PASS",
    }
    assert phase["safety"] == {
        "documentation_only": True,
        "task_status_changed": False,
        "strategy_logic_changed": False,
        "strategy_threshold_changed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def test_trading_2459_documentation_closeout_rejects_historical_prefix_tamper() -> None:
    base_blob = _trading_2459_doc_closeout_base_baseline_blob()
    tampered = bytearray(COMPATIBILITY_BASELINE_PATH.read_bytes())
    tampered[TRADING_2459_DOC_CLOSEOUT_HISTORICAL_PREFIX_BYTE_COUNT - 1] ^= 1
    with pytest.raises(AssertionError, match="historical prefix differs"):
        _assert_trading_2459_doc_closeout_historical_prefix_immutable(
            bytes(tampered),
            base_blob,
        )


def test_data_gov_002_phase_a_is_append_only_current_hash_authority() -> None:
    current_bytes = COMPATIBILITY_BASELINE_PATH.read_bytes()
    _assert_data_gov_002_historical_prefix_immutable(
        current_bytes,
        _data_gov_002_base_baseline_blob(),
    )
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    assert list(baseline).index(DATA_GOV_002_SECTION) < list(baseline).index(DEVX_002_SECTION)
    assert list(baseline).index(TRADING_2459_DOC_CLOSEOUT_SECTION) < list(baseline).index(
        DATA_GOV_002_SECTION
    )
    phase = baseline[DATA_GOV_002_SECTION]

    assert phase["schema_version"] == "data_gov_002_capability_receipt_phase_a_compatibility.v1"
    assert phase["status"] == "BASELINE_DONE_PHASE_A"
    assert phase["boundary_id"] == "DATA-GOV-002-PHASE-A"
    assert phase["task_ids"] == [
        "DATA-GOV-002_CONSUMER_CAPABILITY_SCOPED_DATA_QUALITY_RECEIPTS",
        "TRADING-2460_DECISION_TARGET_CAPABILITY_AUDIT_LABEL_FOUNDATION",
    ]
    assert phase["owner_authorization"] == (
        "owner_decision:DATA-GOV-002:2026-07-26:approve_long_term_capability_receipt_engineering_v1"
    )
    assert phase["prior_sections_immutability"] == {
        "source_commit": DATA_GOV_002_BASE_COMMIT,
        "repository_path": WAVE11_BASELINE_REPOSITORY_PATH,
        "git_blob_sha1": DATA_GOV_002_BASELINE_GIT_BLOB,
        "raw_byte_count": DATA_GOV_002_HISTORICAL_PREFIX_BYTE_COUNT,
        "raw_sha256": DATA_GOV_002_HISTORICAL_PREFIX_SHA256,
        "append_offset": DATA_GOV_002_HISTORICAL_PREFIX_BYTE_COUNT,
        "current_section_must_be_eof": True,
    }
    assert phase["known_unrelated_exclusions"] == [
        WAVE14_S2_PROHIBITED_USER_PATH,
    ]

    superseded = set(phase["superseded_live_source_paths"])
    observed_live_mismatches = _data_gov_002_prior_active_source_mismatches()
    assert superseded <= (
        observed_live_mismatches | _arch_005m1_batch4_superseded_live_source_paths()
    )
    assert observed_live_mismatches <= _arch_005s4d_s2_all_superseded_live_source_paths()
    assert phase["supersession"] == {
        "superseded_by_phase": "DATA-GOV-002-PHASE-A",
        "scope": "LATEST_ACTIVE_CURRENT_MISMATCH_SET",
        "historical_hashes_rewritten": False,
        "inherited_supersession_authority": TRADING_2459_DOC_CLOSEOUT_SECTION,
        "current_hash_authority": f"{DATA_GOV_002_SECTION}.sources",
    }
    assert phase["removed_live_source_paths"] == []
    assert set(phase["source_delta_paths"]) == superseded | DATA_GOV_002_NEW_SOURCE_PATHS
    assert set(phase["new_source_paths"]) == DATA_GOV_002_NEW_SOURCE_PATHS

    sources = phase["sources"]
    source_paths = [str(source["path"]) for source in sources]
    assert len(source_paths) == len(set(source_paths))
    assert source_paths == sorted(source_paths, key=str.casefold)
    assert set(source_paths) == superseded | DATA_GOV_002_NEW_SOURCE_PATHS
    assert WAVE11_BASELINE_REPOSITORY_PATH not in source_paths
    assert WAVE14_S2_PROHIBITED_USER_PATH not in source_paths
    for source in sources:
        assert _source_sha256(source) == source["sha256"], source["path"]

    assert phase["validation"] == {
        "engineering_status": "FORMAL_VALIDATION_PASS",
        "focused": "PASS_118",
        "task_registry": "PASS_BYTE_IDENTICAL",
        "architecture_devex": "PASS",
        "architecture_fitness": "PASS_654",
        "report_validation": "PASS_57_WARNINGS_62",
        "reproducibility": "PASS_23",
        "contract_validation": "PASS_275",
        "integration": "PASS_995_WARNINGS_642",
        "full": "PASS_7292_SKIPPED_3_WARNINGS_643",
        "full_runtime_summary": (
            "outputs/validation_runtime/full_20260726T022008Z/test_runtime_summary.json"
        ),
    }
    assert phase["safety"] == {
        "full_canonical_dq_preserved": True,
        "global_cache_pass_claimed": False,
        "cross_consumer_reuse_allowed": False,
        "daily_operation_authorized": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def test_devx_002_is_append_only_current_hash_authority() -> None:
    current_bytes = COMPATIBILITY_BASELINE_PATH.read_bytes()
    _assert_devx_002_historical_prefix_immutable(
        current_bytes,
        _devx_002_base_baseline_blob(),
    )
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    assert list(baseline).index(DEVX_002_SECTION) < list(baseline).index(DEVX_002_PUSH_V2_SECTION)
    assert list(baseline).index(DATA_GOV_002_SECTION) < list(baseline).index(DEVX_002_SECTION)
    phase = baseline[DEVX_002_SECTION]

    assert phase["schema_version"] == "devx_002_governed_development_compatibility.v1"
    assert phase["status"] == "BASELINE_DONE"
    assert phase["boundary_id"] == "DEVX-002"
    assert phase["task_ids"] == ["DEVX-002_GOVERNED_DEVELOPMENT_WORKFLOW_SKILL"]
    assert phase["owner_authorization"] == (
        "owner_decision:DEVX-002:2026-07-26:adopt_governed_local_main_skill_v1"
    )
    assert phase["prior_sections_immutability"] == {
        "source_commit": DEVX_002_BASE_COMMIT,
        "repository_path": WAVE11_BASELINE_REPOSITORY_PATH,
        "git_blob_sha1": DEVX_002_BASELINE_GIT_BLOB,
        "raw_byte_count": DEVX_002_HISTORICAL_PREFIX_BYTE_COUNT,
        "raw_sha256": DEVX_002_HISTORICAL_PREFIX_SHA256,
        "append_offset": DEVX_002_HISTORICAL_PREFIX_BYTE_COUNT,
        "current_section_must_be_eof": True,
    }
    assert phase["known_unrelated_exclusions"] == [WAVE14_S2_PROHIBITED_USER_PATH]

    superseded = set(phase["superseded_live_source_paths"])
    observed_live_mismatches = _devx_002_prior_active_source_mismatches()
    assert superseded <= (
        observed_live_mismatches | _arch_005m3_superseded_live_source_paths()
    )
    assert observed_live_mismatches <= _arch_005s4d_s2_all_superseded_live_source_paths()
    assert phase["supersession"] == {
        "superseded_by_phase": "DEVX-002",
        "scope": "LATEST_ACTIVE_CURRENT_MISMATCH_SET",
        "historical_hashes_rewritten": False,
        "inherited_supersession_authority": DATA_GOV_002_SECTION,
        "current_hash_authority": f"{DEVX_002_SECTION}.sources",
    }
    assert phase["removed_live_source_paths"] == []
    assert set(phase["source_delta_paths"]) == superseded | DEVX_002_NEW_SOURCE_PATHS
    assert set(phase["new_source_paths"]) == DEVX_002_NEW_SOURCE_PATHS

    sources = phase["sources"]
    source_paths = [str(source["path"]) for source in sources]
    assert len(source_paths) == len(set(source_paths))
    assert source_paths == sorted(source_paths, key=str.casefold)
    assert set(source_paths) == superseded | DEVX_002_NEW_SOURCE_PATHS
    assert WAVE11_BASELINE_REPOSITORY_PATH not in source_paths
    assert WAVE14_S2_PROHIBITED_USER_PATH not in source_paths
    for source in sources:
        assert _source_sha256(source) == source["sha256"], source["path"]

    assert phase["validation"] == {
        "engineering_status": "PRE_CLOSEOUT_VALIDATION",
        "skill_quick_validate": "PASS_CANONICAL_AND_INSTALLED",
        "bundle_parity": "PASS_5_FILES",
        "skill_focused": "PASS_11",
        "combined_focused": "PASS_52",
        "task_registry": "PASS_BYTE_IDENTICAL",
        "architecture_devex": "PASS",
        "architecture_initial": "FAIL_14_AUTHORITY_DRIFT",
        "architecture_parent_rerun": "UNSUPPORTED_NON_FULL_LINKED_EVIDENCE",
        "architecture_rerun": "REQUIRED_WITH_TASK_BOUNDARY",
        "initial_runtime_summary": (
            "outputs/validation_runtime/architecture-fitness_20260726T032309Z/"
            "test_runtime_summary.json"
        ),
    }
    assert phase["safety"] == {
        "strategy_logic_changed": False,
        "strategy_threshold_changed": False,
        "data_flow_changed": False,
        "production_effect": "none",
        "broker_action": "none",
        "remote_action": "separate_authorization",
    }


def test_devx_002_default_ordinary_push_v2_is_current_hash_authority() -> None:
    current_bytes = COMPATIBILITY_BASELINE_PATH.read_bytes()
    _assert_devx_002_push_v2_historical_prefix_immutable(
        current_bytes,
        _devx_002_push_v2_base_baseline_blob(),
    )
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    assert next(reversed(baseline)) == LATEST_COMPATIBILITY_SECTION
    assert list(baseline).index(DEVX_002_SECTION) < list(baseline).index(DEVX_002_PUSH_V2_SECTION)
    assert list(baseline).index(DEVX_002_PUSH_V2_SECTION) < list(baseline).index(
        ARCH_004G2_OBSERVABILITY_SECTION
    )
    phase = baseline[DEVX_002_PUSH_V2_SECTION]

    assert phase["schema_version"] == "devx_002_default_ordinary_push_compatibility.v2"
    assert phase["status"] == "BASELINE_DONE"
    assert phase["boundary_id"] == "DEVX-002-PUSH-V2"
    assert phase["task_ids"] == ["DEVX-002_GOVERNED_DEVELOPMENT_WORKFLOW_SKILL"]
    assert phase["owner_authorization"] == (
        "owner_decision:DEVX-002:2026-07-26:default_ordinary_push_after_local_main_v2"
    )
    assert phase["prior_sections_immutability"] == {
        "source_commit": DEVX_002_PUSH_V2_BASE_COMMIT,
        "repository_path": WAVE11_BASELINE_REPOSITORY_PATH,
        "git_blob_sha1": DEVX_002_PUSH_V2_BASELINE_GIT_BLOB,
        "raw_byte_count": DEVX_002_PUSH_V2_HISTORICAL_PREFIX_BYTE_COUNT,
        "raw_sha256": DEVX_002_PUSH_V2_HISTORICAL_PREFIX_SHA256,
        "append_offset": DEVX_002_PUSH_V2_HISTORICAL_PREFIX_BYTE_COUNT,
        "current_section_must_be_eof": True,
    }
    assert phase["known_unrelated_exclusions"] == [WAVE14_S2_PROHIBITED_USER_PATH]

    superseded = set(phase["superseded_live_source_paths"])
    observed_live_mismatches = _devx_002_push_v2_prior_active_source_mismatches()
    assert superseded <= observed_live_mismatches
    assert observed_live_mismatches <= _arch_005s4d_s2_all_superseded_live_source_paths()
    assert phase["supersession"] == {
        "superseded_by_phase": "DEVX-002-PUSH-V2",
        "scope": "LATEST_ACTIVE_CURRENT_MISMATCH_SET",
        "historical_hashes_rewritten": False,
        "inherited_supersession_authority": DEVX_002_SECTION,
        "current_hash_authority": f"{DEVX_002_PUSH_V2_SECTION}.sources",
    }
    assert phase["removed_live_source_paths"] == []
    assert set(phase["source_delta_paths"]) == superseded
    assert phase["new_source_paths"] == []

    sources = phase["sources"]
    source_paths = [str(source["path"]) for source in sources]
    assert len(source_paths) == len(set(source_paths))
    assert source_paths == sorted(source_paths, key=str.casefold)
    assert set(source_paths) == superseded
    assert WAVE11_BASELINE_REPOSITORY_PATH not in source_paths
    assert WAVE14_S2_PROHIBITED_USER_PATH not in source_paths
    for source in sources:
        assert _source_sha256(source) == source["sha256"], source["path"]

    assert phase["validation"] == {
        "engineering_status": "PRE_CLOSEOUT_VALIDATION",
        "initial_main_push": "PASS_3_COMMITS",
        "initial_pushed_commit": DEVX_002_PUSH_V2_BASE_COMMIT,
        "skill_quick_validate": "REQUIRED",
        "bundle_parity": "REQUIRED",
        "focused": "REQUIRED",
        "architecture": "REQUIRED",
        "contract": "REQUIRED",
        "full": "REQUIRED",
    }
    assert phase["safety"] == {
        "default_remote_publication": "ORDINARY_NON_FORCE_PUSH_AFTER_LOCAL_MAIN",
        "pull_request_authorized": False,
        "force_push_authorized": False,
        "history_rewrite_authorized": False,
        "remote_divergence_auto_repair": False,
        "strategy_logic_changed": False,
        "strategy_threshold_changed": False,
        "data_flow_changed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def test_arch_004g2_observability_is_append_only_current_hash_authority() -> None:
    current_bytes = COMPATIBILITY_BASELINE_PATH.read_bytes()
    _assert_arch_004g2_observability_historical_prefix_immutable(
        current_bytes,
        _arch_004g2_observability_base_baseline_blob(),
    )
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    assert next(reversed(baseline)) == LATEST_COMPATIBILITY_SECTION
    assert list(baseline).index(DEVX_002_PUSH_V2_SECTION) < list(baseline).index(
        ARCH_004G2_OBSERVABILITY_SECTION
    )
    assert list(baseline).index(ARCH_004G2_OBSERVABILITY_SECTION) < list(baseline).index(
        ARCH_004G2_CLOSURE_THRESHOLD_SECTION
    )
    phase = baseline[ARCH_004G2_OBSERVABILITY_SECTION]

    assert phase["schema_version"] == "arch_004g2_validation_observability_compatibility.v1"
    assert phase["status"] == "IN_PROGRESS"
    assert phase["boundary_id"] == "ARCH-004G2-VALIDATION-OBSERVABILITY"
    assert phase["task_ids"] == ["ARCH-004G2_VALIDATION_RUNTIME_BUDGET_AND_FIXTURE_REUSE"]
    assert phase["owner_authorization"] == (
        "owner_continuation:ARCH-004G2:2026-07-26:continue_engineering_line"
    )
    assert phase["prior_sections_immutability"] == {
        "source_commit": ARCH_004G2_OBSERVABILITY_BASE_COMMIT,
        "repository_path": WAVE11_BASELINE_REPOSITORY_PATH,
        "git_blob_sha1": ARCH_004G2_OBSERVABILITY_BASELINE_GIT_BLOB,
        "raw_byte_count": ARCH_004G2_OBSERVABILITY_HISTORICAL_PREFIX_BYTE_COUNT,
        "raw_sha256": ARCH_004G2_OBSERVABILITY_HISTORICAL_PREFIX_SHA256,
        "append_offset": ARCH_004G2_OBSERVABILITY_HISTORICAL_PREFIX_BYTE_COUNT,
        "current_section_must_be_eof": True,
    }
    assert phase["known_unrelated_exclusions"] == [WAVE14_S2_PROHIBITED_USER_PATH]

    superseded = set(phase["superseded_live_source_paths"])
    if next(reversed(baseline)) == ARCH_004G2_OBSERVABILITY_SECTION:
        assert superseded == _arch_004g2_observability_prior_active_source_mismatches()
    assert phase["supersession"] == {
        "superseded_by_phase": "ARCH-004G2-VALIDATION-OBSERVABILITY",
        "scope": "LATEST_ACTIVE_CURRENT_MISMATCH_SET_WITH_NEW_SOURCES",
        "historical_hashes_rewritten": False,
        "inherited_supersession_authority": DEVX_002_PUSH_V2_SECTION,
        "current_hash_authority": f"{ARCH_004G2_OBSERVABILITY_SECTION}.sources",
    }
    assert phase["removed_live_source_paths"] == []
    assert set(phase["source_delta_paths"]) == (
        superseded | ARCH_004G2_OBSERVABILITY_NEW_SOURCE_PATHS
    )
    assert set(phase["new_source_paths"]) == ARCH_004G2_OBSERVABILITY_NEW_SOURCE_PATHS

    sources = phase["sources"]
    source_paths = [str(source["path"]) for source in sources]
    assert len(source_paths) == len(set(source_paths))
    assert source_paths == sorted(source_paths, key=str.casefold)
    assert set(source_paths) == superseded | ARCH_004G2_OBSERVABILITY_NEW_SOURCE_PATHS
    assert WAVE11_BASELINE_REPOSITORY_PATH not in source_paths
    assert WAVE14_S2_PROHIBITED_USER_PATH not in source_paths
    assert phase["validation"] == {
        "engineering_status": "FORMAL_VALIDATION_PASS",
        "focused_validation_session": "PASS_81_SKIPPED_1",
        "diagnostic_artifact": (
            "outputs/validation_runtime/smoothed_validation_observation_20260726T045543Z.json"
        ),
        "task_registry": "PASS_BYTE_IDENTICAL",
        "architecture_devex": "PASS",
        "architecture_initial": "FAIL_22_AUTHORITY_DRIFT",
        "architecture_rerun": "PASS_660",
        "contract": "PASS_275",
        "full": "NOT_RUN_OBSERVABILITY_ONLY",
    }
    assert phase["safety"] == {
        "stable_full_improvement_claimed": False,
        "optimization_authorized": False,
        "strategy_logic_changed": False,
        "strategy_threshold_changed": False,
        "data_flow_changed": False,
        "cached_data_mutated": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def test_arch_004g2_closure_threshold_miss_is_append_only_current_hash_authority() -> None:
    current_bytes = COMPATIBILITY_BASELINE_PATH.read_bytes()
    _assert_arch_004g2_closure_threshold_historical_prefix_immutable(
        current_bytes,
        _arch_004g2_closure_threshold_base_baseline_blob(),
    )
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    assert next(reversed(baseline)) == LATEST_COMPATIBILITY_SECTION
    phase = baseline[ARCH_004G2_CLOSURE_THRESHOLD_SECTION]

    assert phase["schema_version"] == "arch_004g2_closure_threshold_compatibility.v1"
    assert phase["status"] == "IN_PROGRESS"
    assert phase["boundary_id"] == "ARCH-004G2-SMOOTHED-CLOSURE-THRESHOLD-MISS"
    assert phase["task_ids"] == ["ARCH-004G2_VALIDATION_RUNTIME_BUDGET_AND_FIXTURE_REUSE"]
    assert phase["owner_authorization"] == (
        "owner_continuation:ARCH-004G2:2026-07-26:continue_engineering_line"
    )
    assert phase["prior_sections_immutability"] == {
        "source_commit": ARCH_004G2_CLOSURE_THRESHOLD_BASE_COMMIT,
        "repository_path": WAVE11_BASELINE_REPOSITORY_PATH,
        "git_blob_sha1": ARCH_004G2_CLOSURE_THRESHOLD_BASELINE_GIT_BLOB,
        "raw_byte_count": ARCH_004G2_CLOSURE_THRESHOLD_HISTORICAL_PREFIX_BYTE_COUNT,
        "raw_sha256": ARCH_004G2_CLOSURE_THRESHOLD_HISTORICAL_PREFIX_SHA256,
        "append_offset": ARCH_004G2_CLOSURE_THRESHOLD_HISTORICAL_PREFIX_BYTE_COUNT,
        "current_section_must_be_eof": True,
    }
    assert phase["known_unrelated_exclusions"] == [WAVE14_S2_PROHIBITED_USER_PATH]

    superseded = set(phase["superseded_live_source_paths"])
    if next(reversed(baseline)) == ARCH_004G2_CLOSURE_THRESHOLD_SECTION:
        assert superseded == _arch_004g2_closure_threshold_prior_active_source_mismatches()
    assert phase["supersession"] == {
        "superseded_by_phase": "ARCH-004G2-SMOOTHED-CLOSURE-THRESHOLD-MISS",
        "scope": "LATEST_ACTIVE_CURRENT_MISMATCH_SET",
        "historical_hashes_rewritten": False,
        "inherited_supersession_authority": ARCH_004G2_OBSERVABILITY_SECTION,
        "current_hash_authority": f"{ARCH_004G2_CLOSURE_THRESHOLD_SECTION}.sources",
    }
    assert phase["removed_live_source_paths"] == []
    assert set(phase["source_delta_paths"]) == superseded
    assert phase["new_source_paths"] == []

    sources = phase["sources"]
    source_paths = [str(source["path"]) for source in sources]
    assert len(source_paths) == len(set(source_paths))
    assert source_paths == sorted(source_paths, key=str.casefold)
    assert set(source_paths) == superseded
    assert WAVE11_BASELINE_REPOSITORY_PATH not in source_paths
    assert WAVE14_S2_PROHIBITED_USER_PATH not in source_paths
    for source in sources:
        if next(reversed(baseline)) == ARCH_004G2_CLOSURE_THRESHOLD_SECTION:
            assert _raw_source_sha256(source) == source["sha256"], source["path"]
        else:
            assert re.fullmatch(r"[0-9a-f]{64}", str(source["sha256"]))

    assert phase["validation"] == {
        "engineering_status": "REJECTED_THRESHOLD_MISS",
        "focused_smoothed_promotion": "PASS_7",
        "same_command_baseline_seconds": 105.232568,
        "same_command_after_seconds": 93.999076,
        "absolute_improvement_seconds": 11.233492,
        "relative_improvement_percent": 10.67,
        "required_after_max_seconds": 75.232568,
        "task_registry": "PASS_BYTE_IDENTICAL",
        "architecture_initial": "FAIL_2_AUTHORITY_DRIFT",
        "architecture_rerun": "PASS_662",
        "contract": "NOT_RUN_CANDIDATE_REVERTED",
        "full": "NOT_RUN_THRESHOLD_MISS",
    }
    assert phase["safety"] == {
        "candidate_retained": False,
        "production_source_byte_exact_base": True,
        "targeted_test_byte_exact_base": True,
        "diagnostic_script_byte_exact_base": True,
        "stable_full_improvement_claimed": False,
        "strategy_logic_changed": False,
        "strategy_threshold_changed": False,
        "data_flow_changed": False,
        "cached_data_mutated": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def test_data_gov_002_phase_b1_is_preserved_historical_authority() -> None:
    current_bytes = COMPATIBILITY_BASELINE_PATH.read_bytes()
    _assert_data_gov_002_phase_b1_historical_prefix_immutable(
        current_bytes,
        _data_gov_002_phase_b1_base_baseline_blob(),
    )
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    assert list(baseline).index(DATA_GOV_002_PHASE_B1_SECTION) < list(baseline).index(
        TRADING_2458_RETIREMENT_SECTION
    )
    assert list(baseline).index(ARCH_004G2_CLOSURE_THRESHOLD_SECTION) < list(baseline).index(
        DATA_GOV_002_PHASE_B1_SECTION
    )
    phase = baseline[DATA_GOV_002_PHASE_B1_SECTION]

    assert phase["schema_version"] == "data_gov_002_phase_b1_compatibility.v1"
    assert phase["status"] == "BASELINE_DONE"
    assert phase["boundary_id"] == "DATA-GOV-002-PHASE-B1-GENERIC-ADAPTER"
    assert phase["task_ids"] == ["DATA-GOV-002_CONSUMER_CAPABILITY_SCOPED_DATA_QUALITY_RECEIPTS"]
    assert phase["owner_authorization"] == (
        "owner_continuation:DATA-GOV-002:2026-07-26:continue_long_term_engineering_goal"
    )
    assert phase["prior_sections_immutability"] == {
        "source_commit": DATA_GOV_002_PHASE_B1_BASE_COMMIT,
        "repository_path": WAVE11_BASELINE_REPOSITORY_PATH,
        "git_blob_sha1": DATA_GOV_002_PHASE_B1_BASELINE_GIT_BLOB,
        "raw_byte_count": DATA_GOV_002_PHASE_B1_HISTORICAL_PREFIX_BYTE_COUNT,
        "raw_sha256": DATA_GOV_002_PHASE_B1_HISTORICAL_PREFIX_SHA256,
        "append_offset": DATA_GOV_002_PHASE_B1_HISTORICAL_PREFIX_BYTE_COUNT,
        "current_section_must_be_eof": True,
    }
    assert phase["known_unrelated_exclusions"] == [WAVE14_S2_PROHIBITED_USER_PATH]

    superseded = set(phase["superseded_live_source_paths"])
    assert phase["supersession"] == {
        "superseded_by_phase": "DATA-GOV-002-PHASE-B1-GENERIC-ADAPTER",
        "scope": "LATEST_ACTIVE_CURRENT_MISMATCH_SET_WITH_NEW_SOURCES",
        "historical_hashes_rewritten": False,
        "inherited_supersession_authority": ARCH_004G2_CLOSURE_THRESHOLD_SECTION,
        "current_hash_authority": f"{DATA_GOV_002_PHASE_B1_SECTION}.sources",
    }
    assert phase["removed_live_source_paths"] == []
    assert set(phase["source_delta_paths"]) == (superseded | DATA_GOV_002_PHASE_B1_NEW_SOURCE_PATHS)
    assert set(phase["new_source_paths"]) == DATA_GOV_002_PHASE_B1_NEW_SOURCE_PATHS

    sources = phase["sources"]
    source_paths = [str(source["path"]) for source in sources]
    assert len(source_paths) == len(set(source_paths))
    assert source_paths == sorted(source_paths, key=str.casefold)
    assert set(source_paths) == superseded | DATA_GOV_002_PHASE_B1_NEW_SOURCE_PATHS
    assert WAVE11_BASELINE_REPOSITORY_PATH not in source_paths
    assert WAVE14_S2_PROHIBITED_USER_PATH not in source_paths
    for source in sources:
        assert _source_sha256(source) == source["sha256"], source["path"]

    assert phase["validation"] == {
        "engineering_status": ("BASELINE_DONE_PHASE_B1_GENERIC_ADAPTER_CONTRACT_B2_PENDING"),
        "focused": "PASS_8",
        "task_registry": "PASS_BYTE_IDENTICAL",
        "architecture_devex": "PASS",
        "architecture_initial": "FAIL_19_AUTHORITY_DRIFT",
        "architecture_intermediate": "FAIL_2_MANIFEST_AND_COUNT_DRIFT",
        "architecture_rerun": "PASS_664",
        "contract": "PASS_275",
        "report": "PASS_57_WARN_62",
        "reproducibility": "PASS_23",
        "integration": "PASS_995_WARN_643",
        "full_initial": "FAIL_1_HISTORICAL_EXPECTATION_PATCH_7320_PASS_3_SKIP_642_WARN",
        "full_fix_regression": "PASS_3",
        "full": "PASS_7321_SKIP_3_WARN_642_PARENT_BOUND",
    }
    assert phase["safety"] == {
        "consumer_migration_executed": False,
        "daily_periodic_authorized": False,
        "strategy_logic_changed": False,
        "strategy_threshold_changed": False,
        "data_flow_changed": True,
        "cached_data_mutated": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def test_trading_2458_candidate_family_retirement_is_preserved_historical_authority() -> None:
    current_bytes = COMPATIBILITY_BASELINE_PATH.read_bytes()
    _assert_trading_2458_retirement_historical_prefix_immutable(
        current_bytes,
        _trading_2458_retirement_base_baseline_blob(),
    )
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    assert list(baseline).index(TRADING_2458_RETIREMENT_SECTION) < list(baseline).index(
        TRADING_2458_CLOSEOUT_SECTION
    )
    assert list(baseline).index(DATA_GOV_002_PHASE_B1_SECTION) < list(baseline).index(
        TRADING_2458_RETIREMENT_SECTION
    )
    phase = baseline[TRADING_2458_RETIREMENT_SECTION]

    assert phase["schema_version"] == "trading_2458_candidate_family_retirement_compatibility.v1"
    assert phase["status"] == "COMPLETE"
    assert phase["boundary_id"] == "TRADING-2458-CANDIDATE-FAMILY-RETIREMENT-V4"
    assert phase["task_ids"] == ["TRADING-2458_CONSTRAINT_CAUSAL_DIAGNOSTIC"]
    assert phase["owner_authorization"] == (
        "owner_decision:TRADING-2458:2026-07-25:retire_current_saturated_candidate_family"
    )
    assert phase["prior_sections_immutability"] == {
        "source_commit": TRADING_2458_RETIREMENT_BASE_COMMIT,
        "repository_path": WAVE11_BASELINE_REPOSITORY_PATH,
        "git_blob_sha1": TRADING_2458_RETIREMENT_BASELINE_GIT_BLOB,
        "raw_byte_count": TRADING_2458_RETIREMENT_HISTORICAL_PREFIX_BYTE_COUNT,
        "raw_sha256": TRADING_2458_RETIREMENT_HISTORICAL_PREFIX_SHA256,
        "append_offset": TRADING_2458_RETIREMENT_HISTORICAL_PREFIX_BYTE_COUNT,
        "current_section_must_be_eof": True,
    }
    assert phase["known_unrelated_exclusions"] == [WAVE14_S2_PROHIBITED_USER_PATH]

    superseded = set(phase["superseded_live_source_paths"])
    assert phase["supersession"] == {
        "superseded_by_phase": "TRADING-2458-CANDIDATE-FAMILY-RETIREMENT-V4",
        "scope": "LATEST_ACTIVE_CURRENT_MISMATCH_SET_WITH_NEW_SOURCES",
        "historical_hashes_rewritten": False,
        "inherited_supersession_authority": DATA_GOV_002_PHASE_B1_SECTION,
        "current_hash_authority": f"{TRADING_2458_RETIREMENT_SECTION}.sources",
    }
    assert phase["removed_live_source_paths"] == []
    assert set(phase["new_source_paths"]) == TRADING_2458_RETIREMENT_NEW_SOURCE_PATHS
    assert set(phase["source_delta_paths"]) == (
        superseded | TRADING_2458_RETIREMENT_NEW_SOURCE_PATHS
    )

    sources = phase["sources"]
    source_paths = [str(source["path"]) for source in sources]
    assert len(source_paths) == len(set(source_paths))
    assert source_paths == sorted(source_paths, key=str.casefold)
    assert set(source_paths) == set(phase["source_delta_paths"])
    assert WAVE11_BASELINE_REPOSITORY_PATH not in source_paths
    assert WAVE14_S2_PROHIBITED_USER_PATH not in source_paths
    for source in sources:
        assert source["hash_normalization"] == "git_eol_lf"
        assert _source_sha256(source) == source["sha256"], source["path"]

    assert phase["validation"] == {
        "engineering_status": "PRIOR_CARRIER_VALIDATED_V4_RECOMPUTE_PENDING",
        "v3_closeout_focused": "PASS_64",
        "v3_closeout_architecture": "PASS_665",
        "v3_closeout_contract": "PASS_275",
        "v3_closeout_full": "PASS_7338_SKIPPED_4_WARNINGS_642",
        "v3_carrier_drift": "FAIL_MAIN_ADVANCED_TO_B8463FAAC",
        "v4_exact_base": TRADING_2458_RETIREMENT_BASE_COMMIT,
        "v4_formal_validation": "DEFERRED_TO_CLOSEOUT_AUTHORITY",
    }
    assert phase["safety"] == {
        "strategy_logic_changed": False,
        "strategy_threshold_changed": False,
        "historical_artifact_bytes_changed": False,
        "candidate_family_reopened": False,
        "prospective_accessed": False,
        "paper_shadow_changed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def test_trading_2458_candidate_family_retirement_closeout_is_current_hash_authority() -> None:
    current_bytes = COMPATIBILITY_BASELINE_PATH.read_bytes()
    _assert_trading_2458_closeout_historical_prefix_immutable(
        current_bytes,
        _trading_2458_closeout_base_baseline_blob(),
    )
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    assert next(reversed(baseline)) == LATEST_COMPATIBILITY_SECTION
    assert list(baseline).index(TRADING_2458_CLOSEOUT_SECTION) < list(baseline).index(
        DATA_GOV_002_PHASE_B2_SECTION
    )
    phase = baseline[TRADING_2458_CLOSEOUT_SECTION]

    assert phase["schema_version"] == (
        "trading_2458_candidate_family_retirement_closeout_compatibility.v1"
    )
    assert phase["status"] in {"VALIDATING_FINAL_TREE", "COMPLETE"}
    assert phase["boundary_id"] == "TRADING-2458-CANDIDATE-FAMILY-RETIREMENT-V4-CLOSEOUT"
    assert phase["task_ids"] == ["TRADING-2458_CONSTRAINT_CAUSAL_DIAGNOSTIC"]
    assert phase["owner_authorization"] == (
        "owner_decision:TRADING-2458:2026-07-25:retire_current_saturated_candidate_family"
    )
    assert phase["prior_sections_immutability"] == {
        "source_commit": TRADING_2458_CLOSEOUT_BASE_COMMIT,
        "repository_path": WAVE11_BASELINE_REPOSITORY_PATH,
        "git_blob_sha1": TRADING_2458_CLOSEOUT_BASELINE_GIT_BLOB,
        "raw_byte_count": TRADING_2458_CLOSEOUT_HISTORICAL_PREFIX_BYTE_COUNT,
        "raw_sha256": TRADING_2458_CLOSEOUT_HISTORICAL_PREFIX_SHA256,
        "append_offset": TRADING_2458_CLOSEOUT_HISTORICAL_PREFIX_BYTE_COUNT,
        "current_section_must_be_eof": True,
    }
    assert phase["known_unrelated_exclusions"] == [WAVE14_S2_PROHIBITED_USER_PATH]

    superseded = set(phase["superseded_live_source_paths"])
    closeout_live_mismatches = _trading_2458_closeout_prior_active_source_mismatches()
    assert superseded <= closeout_live_mismatches
    assert _data_gov_002_phase_b2_superseded_live_source_paths() <= closeout_live_mismatches
    assert phase["supersession"] == {
        "superseded_by_phase": "TRADING-2458-CANDIDATE-FAMILY-RETIREMENT-V4-CLOSEOUT",
        "scope": "LATEST_ACTIVE_CURRENT_MISMATCH_SET_WITH_ARCHIVE_MOVE",
        "historical_hashes_rewritten": False,
        "inherited_supersession_authority": TRADING_2458_RETIREMENT_SECTION,
        "current_hash_authority": f"{TRADING_2458_CLOSEOUT_SECTION}.sources",
    }
    removed = set(phase["removed_live_source_paths"])
    assert removed == TRADING_2458_CLOSEOUT_REMOVED_SOURCE_PATHS
    assert removed <= superseded
    assert set(phase["new_source_paths"]) == TRADING_2458_CLOSEOUT_NEW_SOURCE_PATHS
    assert set(phase["source_delta_paths"]) == (
        (superseded - removed) | TRADING_2458_CLOSEOUT_NEW_SOURCE_PATHS
    )

    sources = phase["sources"]
    source_paths = [str(source["path"]) for source in sources]
    assert len(source_paths) == len(set(source_paths))
    assert source_paths == sorted(source_paths, key=str.casefold)
    assert set(source_paths) == set(phase["source_delta_paths"])
    assert WAVE11_BASELINE_REPOSITORY_PATH not in source_paths
    assert WAVE14_S2_PROHIBITED_USER_PATH not in source_paths
    for source in sources:
        assert source["hash_normalization"] == "git_eol_lf"
        assert _source_sha256(source) == source["sha256"], source["path"]

    validation = phase["validation"]
    assert validation["v3_closeout_full"] == "PASS_7338_SKIPPED_4_WARNINGS_642"
    assert validation["v3_carrier_drift"] == "FAIL_MAIN_ADVANCED_TO_B8463FAAC"
    assert validation["v4_exact_base"] == TRADING_2458_RETIREMENT_BASE_COMMIT
    if phase["status"] == "COMPLETE":
        assert validation["v4_focused"].startswith("PASS_")
        assert validation["v4_architecture"].startswith("PASS_")
        assert validation["v4_contract"].startswith("PASS_")
        assert validation["v4_full"].startswith("PASS_")
        assert validation["post_full_evidence_gate"].startswith("PASS_")
    else:
        assert validation["v4_focused"] == "PENDING"
        assert validation["v4_architecture"] == "PENDING"
        assert validation["v4_contract"] == "PENDING"
        assert validation["v4_full"] == "PENDING"
        assert validation["post_full_evidence_gate"] == "PENDING"
    assert phase["safety"] == {
        "strategy_logic_changed": False,
        "strategy_threshold_changed": False,
        "historical_artifact_bytes_changed": False,
        "candidate_family_reopened": False,
        "prospective_accessed": False,
        "paper_shadow_changed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def test_data_gov_002_phase_b2_is_current_hash_authority() -> None:
    current_bytes = COMPATIBILITY_BASELINE_PATH.read_bytes()
    _assert_data_gov_002_phase_b2_historical_prefix_immutable(
        current_bytes,
        _data_gov_002_phase_b2_base_baseline_blob(),
    )
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    assert next(reversed(baseline)) == LATEST_COMPATIBILITY_SECTION
    assert list(baseline).index(TRADING_2458_CLOSEOUT_SECTION) < list(baseline).index(
        DATA_GOV_002_PHASE_B2_SECTION
    )
    assert list(baseline).index(DATA_GOV_002_PHASE_B2_SECTION) < list(baseline).index(
        DEVX_003_SECTION
    )
    phase = baseline[DATA_GOV_002_PHASE_B2_SECTION]

    assert phase["schema_version"] == "data_gov_002_phase_b2_compatibility.v1"
    assert phase["status"] in {
        "VALIDATING_PHASE_B2_REGIME_LABEL_PILOT",
        "BASELINE_DONE",
    }
    assert phase["boundary_id"] == "DATA-GOV-002-PHASE-B2-REGIME-LABEL-PILOT"
    assert phase["task_ids"] == [
        "DATA-GOV-002_CONSUMER_CAPABILITY_SCOPED_DATA_QUALITY_RECEIPTS",
        "TRADING-2316_REGIME_LABEL_GENERATOR_DIAGNOSTIC_POC",
    ]
    assert phase["owner_authorization"] == (
        "owner_continuation:DATA-GOV-002:2026-07-26:continue_long_term_engineering_goal"
    )
    assert phase["prior_sections_immutability"] == {
        "source_commit": DATA_GOV_002_PHASE_B2_BASE_COMMIT,
        "repository_path": WAVE11_BASELINE_REPOSITORY_PATH,
        "git_blob_sha1": DATA_GOV_002_PHASE_B2_BASELINE_GIT_BLOB,
        "raw_byte_count": DATA_GOV_002_PHASE_B2_HISTORICAL_PREFIX_BYTE_COUNT,
        "raw_sha256": DATA_GOV_002_PHASE_B2_HISTORICAL_PREFIX_SHA256,
        "append_offset": DATA_GOV_002_PHASE_B2_HISTORICAL_PREFIX_BYTE_COUNT,
        "current_section_must_be_eof": True,
    }
    assert phase["known_unrelated_exclusions"] == [WAVE14_S2_PROHIBITED_USER_PATH]

    superseded = set(phase["superseded_live_source_paths"])
    phase_b2_live_mismatches = _data_gov_002_phase_b2_prior_active_source_mismatches()
    assert superseded <= phase_b2_live_mismatches
    assert (
        _devx_003_superseded_live_source_paths() - phase_b2_live_mismatches
        <= (
            _arch_005m2_superseded_live_source_paths()
            | _arch_005m3_superseded_live_source_paths()
            | _arch_005m1_batch3_superseded_live_source_paths()
            | _arch_005m1_batch4_superseded_live_source_paths()
        )
    )
    assert phase["supersession"] == {
        "superseded_by_phase": "DATA-GOV-002-PHASE-B2-REGIME-LABEL-PILOT",
        "scope": "LATEST_ACTIVE_CURRENT_MISMATCH_SET_WITH_NEW_SOURCES",
        "historical_hashes_rewritten": False,
        "inherited_supersession_authority": TRADING_2458_CLOSEOUT_SECTION,
        "current_hash_authority": f"{DATA_GOV_002_PHASE_B2_SECTION}.sources",
    }
    assert phase["removed_live_source_paths"] == []
    assert set(phase["new_source_paths"]) == DATA_GOV_002_PHASE_B2_NEW_SOURCE_PATHS
    assert set(phase["source_delta_paths"]) == (superseded | DATA_GOV_002_PHASE_B2_NEW_SOURCE_PATHS)

    sources = phase["sources"]
    source_paths = [str(source["path"]) for source in sources]
    assert len(source_paths) == len(set(source_paths))
    assert source_paths == sorted(source_paths, key=str.casefold)
    assert set(source_paths) == set(phase["source_delta_paths"])
    assert WAVE11_BASELINE_REPOSITORY_PATH not in source_paths
    assert WAVE14_S2_PROHIBITED_USER_PATH not in source_paths
    for source in sources:
        assert source["hash_normalization"] == "git_eol_lf"
        assert _source_sha256(source) == source["sha256"], source["path"]

    validation = phase["validation"]
    assert validation["engineering_status"] in {
        "VALIDATING_PHASE_B2_REGIME_LABEL_PILOT",
        "BASELINE_DONE_PHASE_B2_REGIME_LABEL_PILOT_PHASE_C_PENDING",
    }
    assert validation["focused"] == "PASS_16"
    assert validation["real_capability_receipt"] == (
        "PASS_FULL_FAIL_SCOPED_PASS_GLOBAL_CLAIM_FALSE"
    )
    assert validation["real_runner"] == ("PASS_7416_LABEL_ROWS_30_DISTRIBUTION_123_TRANSITION")
    assert validation["task_registry"] == "PASS_BYTE_IDENTICAL"
    assert validation["architecture_devex"] == "PASS"
    formal_fields = (
        "architecture",
        "contract",
        "report",
        "reproducibility",
        "integration",
        "full",
    )
    if phase["status"] == "BASELINE_DONE":
        for field in formal_fields:
            assert str(validation[field]).startswith("PASS_")
    else:
        for field in formal_fields:
            assert validation[field] == "PENDING"
    assert phase["safety"] == {
        "consumer_migration_executed": True,
        "consumer_id": "TRADING-2316_REGIME_LABEL_GENERATOR_DIAGNOSTIC_POC@1.0.0",
        "required_price_tickers": ["QQQ", "SMH", "SPY"],
        "required_rate_series": [],
        "full_data_quality_status": "FAIL",
        "scoped_data_quality_status": "PASS",
        "global_cache_pass_claimed": False,
        "capability_reused_from_other_consumer": False,
        "daily_periodic_authorized": False,
        "strategy_logic_changed": False,
        "strategy_threshold_changed": False,
        "data_flow_changed": True,
        "cached_data_mutated": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def test_devx_003_is_preserved_historical_authority() -> None:
    current_bytes = COMPATIBILITY_BASELINE_PATH.read_bytes()
    _assert_devx_003_historical_prefix_immutable(
        current_bytes,
        _devx_003_base_baseline_blob(),
    )
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    assert next(reversed(baseline)) == LATEST_COMPATIBILITY_SECTION
    assert list(baseline).index(DATA_GOV_002_PHASE_B2_SECTION) < list(baseline).index(
        DEVX_003_SECTION
    )
    assert list(baseline).index(DEVX_003_SECTION) < list(baseline).index(DATA_GOV_002C1_SECTION)
    assert list(baseline).index(DATA_GOV_002C1_SECTION) < list(baseline).index(
        DATA_GOV_002C2_SECTION
    )
    assert list(baseline).index(DATA_GOV_002C2_SECTION) < list(baseline).index(TRADING_2461_SECTION)
    phase = baseline[DEVX_003_SECTION]

    assert phase["schema_version"] == (
        "devx_003_governed_closeout_remote_preflight_compatibility.v1"
    )
    assert phase["status"] in {"VALIDATING", "BASELINE_DONE"}
    assert phase["boundary_id"] == "DEVX-003-GOVERNED-CLOSEOUT-REMOTE-PREFLIGHT"
    assert phase["task_ids"] == ["DEVX-003_GOVERNED_CLOSEOUT_REMOTE_PREFLIGHT"]
    assert phase["owner_authorization"] == (
        "owner_continuation:DEVX-003:2026-07-26:continue_long_term_engineering_goal"
    )
    assert phase["prior_sections_immutability"] == {
        "source_commit": DEVX_003_BASE_COMMIT,
        "repository_path": WAVE11_BASELINE_REPOSITORY_PATH,
        "git_blob_sha1": DEVX_003_BASELINE_GIT_BLOB,
        "raw_byte_count": DEVX_003_HISTORICAL_PREFIX_BYTE_COUNT,
        "raw_sha256": DEVX_003_HISTORICAL_PREFIX_SHA256,
        "append_offset": DEVX_003_HISTORICAL_PREFIX_BYTE_COUNT,
        "current_section_must_be_eof": True,
    }
    assert phase["known_unrelated_exclusions"] == [WAVE14_S2_PROHIBITED_USER_PATH]

    superseded = set(phase["superseded_live_source_paths"])
    observed_live_mismatches = _devx_003_prior_active_source_mismatches()
    assert (
        superseded - observed_live_mismatches
        <= (
            _arch_005m2_superseded_live_source_paths()
            | _arch_005m3_superseded_live_source_paths()
            | _arch_005m1_batch3_superseded_live_source_paths()
            | _arch_005m1_batch4_superseded_live_source_paths()
        )
    )
    assert observed_live_mismatches <= (
        superseded
        | _data_gov_002c1_superseded_live_source_paths()
        | _data_gov_002c2_superseded_live_source_paths()
        | _trading_2461_superseded_live_source_paths()
        | _arch_004g2_paper_weekly_superseded_live_source_paths()
        | _ops_069_terminal_archive_superseded_live_source_paths()
        | _devx_004_superseded_live_source_paths()
        | _devx_001_reconciliation_superseded_live_source_paths()
        | _devx_005_superseded_live_source_paths()
        | _trading_2462_superseded_live_source_paths()
        | _devx_006_superseded_live_source_paths()
        | _arch_005m2_superseded_live_source_paths()
        | _arch_005m3_superseded_live_source_paths()
        | _arch_005m1_batch3_superseded_live_source_paths()
        | _arch_005m1_batch4_superseded_live_source_paths()
        | _ops_070_stable_release_superseded_live_source_paths()
    )
    assert phase["supersession"] == {
        "superseded_by_phase": "DEVX-003-GOVERNED-CLOSEOUT-REMOTE-PREFLIGHT",
        "scope": "LATEST_ACTIVE_CURRENT_MISMATCH_SET_WITH_NEW_SOURCES",
        "historical_hashes_rewritten": False,
        "inherited_supersession_authority": DATA_GOV_002_PHASE_B2_SECTION,
        "current_hash_authority": f"{DEVX_003_SECTION}.sources",
    }
    assert phase["removed_live_source_paths"] == []
    assert set(phase["new_source_paths"]) == DEVX_003_NEW_SOURCE_PATHS
    assert set(phase["source_delta_paths"]) == (superseded | DEVX_003_NEW_SOURCE_PATHS)

    sources = phase["sources"]
    source_paths = [str(source["path"]) for source in sources]
    assert len(source_paths) == len(set(source_paths))
    assert source_paths == sorted(source_paths, key=str.casefold)
    assert set(source_paths) == set(phase["source_delta_paths"])
    assert WAVE11_BASELINE_REPOSITORY_PATH not in source_paths
    assert WAVE14_S2_PROHIBITED_USER_PATH not in source_paths
    for source in sources:
        assert source["hash_normalization"] == "git_eol_lf"
        assert _source_sha256(source) == source["sha256"], source["path"]

    validation = phase["validation"]
    assert validation["focused"] == "PASS_25"
    assert validation["black"] == "PASS"
    assert validation["ruff"] == "PASS"
    assert validation["strict_mypy"] == "PASS"
    assert validation["canonical_quick_validate"] == "PASS"
    assert validation["installed_quick_validate"] == "PASS"
    assert validation["bundle_parity"] == "PASS_5_FILES"
    assert validation["branch_negative_forward_test"] == ("PASS_NON_MAIN_DIRTY_BLOCKED")
    assert validation["task_registry"] == "PASS_BYTE_IDENTICAL"
    assert validation["architecture_devex"] == "PASS"
    formal_fields = ("architecture", "contract", "reproducibility", "full")
    for field in formal_fields:
        assert str(validation[field]).startswith("PASS_")
    if phase["status"] == "BASELINE_DONE":
        assert validation["main_closeout_forward_test"] == ("PASS_CLEAN_MAIN_ORIGIN_ANCESTOR")
    else:
        assert validation["main_closeout_forward_test"] == "PENDING_AFTER_LOCAL_MAIN_FF"
    assert phase["safety"] == {
        "preflight_is_read_only": True,
        "preflight_fetch_performed": False,
        "preflight_push_performed": False,
        "preflight_merge_or_rebase_performed": False,
        "pull_request_changed": False,
        "history_rewritten": False,
        "force_push_performed": False,
        "strategy_logic_changed": False,
        "strategy_threshold_changed": False,
        "data_flow_changed": False,
        "cached_data_mutated": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def test_data_gov_002c1_is_preserved_historical_authority() -> None:
    current_bytes = COMPATIBILITY_BASELINE_PATH.read_bytes()
    _assert_data_gov_002c1_historical_prefix_immutable(
        current_bytes,
        _data_gov_002c1_base_baseline_blob(),
    )
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    assert next(reversed(baseline)) == LATEST_COMPATIBILITY_SECTION
    assert list(baseline).index(DEVX_003_SECTION) < list(baseline).index(DATA_GOV_002C1_SECTION)
    assert list(baseline).index(DATA_GOV_002C1_SECTION) < list(baseline).index(
        DATA_GOV_002C2_SECTION
    )
    phase = baseline[DATA_GOV_002C1_SECTION]

    assert phase["schema_version"] == (
        "data_gov_002c1_dq_issue_attribution_readiness_inventory_compatibility.v1"
    )
    assert phase["status"] in {"VALIDATING", "BASELINE_DONE"}
    assert phase["boundary_id"] == ("DATA-GOV-002C1-DQ-ISSUE-ATTRIBUTION-READINESS-INVENTORY")
    assert phase["task_ids"] == ["DATA-GOV-002C1_DQ_ISSUE_ATTRIBUTION_READINESS_INVENTORY"]
    assert phase["owner_authorization"] == (
        "owner_continuation:DATA-GOV-002C1:2026-07-26:continue_long_term_engineering_goal"
    )
    assert phase["prior_sections_immutability"] == {
        "source_commit": DATA_GOV_002C1_BASE_COMMIT,
        "repository_path": WAVE11_BASELINE_REPOSITORY_PATH,
        "git_blob_sha1": DATA_GOV_002C1_BASELINE_GIT_BLOB,
        "raw_byte_count": DATA_GOV_002C1_HISTORICAL_PREFIX_BYTE_COUNT,
        "raw_sha256": DATA_GOV_002C1_HISTORICAL_PREFIX_SHA256,
        "append_offset": DATA_GOV_002C1_HISTORICAL_PREFIX_BYTE_COUNT,
        "current_section_must_be_eof": True,
    }
    assert phase["known_unrelated_exclusions"] == [WAVE14_S2_PROHIBITED_USER_PATH]

    superseded = set(phase["superseded_live_source_paths"])
    observed_live_mismatches = _data_gov_002c1_prior_active_source_mismatches()
    assert superseded <= observed_live_mismatches
    assert observed_live_mismatches <= (
        superseded
        | _data_gov_002c2_superseded_live_source_paths()
        | _trading_2461_superseded_live_source_paths()
        | _arch_004g2_paper_weekly_superseded_live_source_paths()
        | _ops_069_terminal_archive_superseded_live_source_paths()
        | _devx_004_superseded_live_source_paths()
        | _devx_001_reconciliation_superseded_live_source_paths()
        | _devx_005_superseded_live_source_paths()
        | _trading_2462_superseded_live_source_paths()
        | _devx_006_superseded_live_source_paths()
        | _arch_005m2_superseded_live_source_paths()
        | _arch_005m3_superseded_live_source_paths()
        | _arch_005m1_batch3_superseded_live_source_paths()
        | _arch_005m1_batch4_superseded_live_source_paths()
        | _ops_070_stable_release_superseded_live_source_paths()
    )
    assert phase["supersession"] == {
        "superseded_by_phase": ("DATA-GOV-002C1-DQ-ISSUE-ATTRIBUTION-READINESS-INVENTORY"),
        "scope": "LATEST_ACTIVE_CURRENT_MISMATCH_SET_WITH_NEW_SOURCES",
        "historical_hashes_rewritten": False,
        "inherited_supersession_authority": DEVX_003_SECTION,
        "current_hash_authority": f"{DATA_GOV_002C1_SECTION}.sources",
    }
    assert phase["removed_live_source_paths"] == []
    assert set(phase["new_source_paths"]) == DATA_GOV_002C1_NEW_SOURCE_PATHS
    assert set(phase["source_delta_paths"]) == (superseded | DATA_GOV_002C1_NEW_SOURCE_PATHS)

    sources = phase["sources"]
    source_paths = [str(source["path"]) for source in sources]
    assert len(source_paths) == len(set(source_paths))
    assert source_paths == sorted(source_paths, key=str.casefold)
    assert set(source_paths) == set(phase["source_delta_paths"])
    assert WAVE11_BASELINE_REPOSITORY_PATH not in source_paths
    assert WAVE14_S2_PROHIBITED_USER_PATH not in source_paths
    for source in sources:
        assert source["hash_normalization"] == "git_eol_lf"
        assert _source_sha256(source) == source["sha256"], source["path"]

    validation = phase["validation"]
    for field in (
        "focused",
        "architecture",
        "contract",
        "report",
        "reproducibility",
        "integration",
        "full",
    ):
        if phase["status"] == "BASELINE_DONE":
            assert str(validation[field]).startswith("PASS_")
        else:
            assert validation[field] == "PENDING" or str(validation[field]).startswith("PASS_")
    assert validation["inventory_build"] == "PASS_69_SITES"
    assert validation["inventory_check"] == "PASS_0_ERRORS"
    assert validation["black"] == "PASS"
    assert validation["ruff"] == "PASS"
    assert validation["strict_mypy"] == "PASS"
    assert validation["task_registry"] == "PASS_BYTE_IDENTICAL"
    assert validation["architecture_devex"] == "PASS"
    assert phase["safety"] == {
        "canonical_site_count": 69,
        "policy_authorized_site_count": 1,
        "owner_review_required_site_count": 68,
        "new_migration_authority_added": False,
        "message_or_sample_scope_parsing_used": False,
        "data_quality_behavior_changed": False,
        "capability_behavior_changed": False,
        "consumer_migration_executed": False,
        "strategy_logic_changed": False,
        "strategy_threshold_changed": False,
        "data_flow_changed": True,
        "cached_data_mutated": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def test_data_gov_002c2_is_preserved_historical_authority() -> None:
    current_bytes = COMPATIBILITY_BASELINE_PATH.read_bytes()
    _assert_data_gov_002c2_historical_prefix_immutable(
        current_bytes,
        _data_gov_002c2_base_baseline_blob(),
    )
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    assert next(reversed(baseline)) == LATEST_COMPATIBILITY_SECTION
    assert list(baseline).index(DATA_GOV_002C1_SECTION) < list(baseline).index(
        DATA_GOV_002C2_SECTION
    )
    assert list(baseline).index(DATA_GOV_002C2_SECTION) < list(baseline).index(TRADING_2461_SECTION)
    phase = baseline[DATA_GOV_002C2_SECTION]

    assert phase["schema_version"] == (
        "data_gov_002c2_rate_row_issue_attribution_source_owner_review_pack_compatibility.v1"
    )
    assert phase["status"] in {"VALIDATING", "BASELINE_DONE"}
    assert phase["boundary_id"] == (
        "DATA-GOV-002C2-RATE-ISSUE-ATTRIBUTION-SOURCE-OWNER-REVIEW-PACK"
    )
    assert phase["task_ids"] == [
        "DATA-GOV-002C2_RATE_ROW_ISSUE_ATTRIBUTION_SOURCE_OWNER_REVIEW_PACK"
    ]
    assert phase["owner_authorization"] == (
        "owner_continuation:DATA-GOV-002C2:2026-07-26:continue_long_term_engineering_goal"
    )
    assert phase["prior_sections_immutability"] == {
        "source_commit": DATA_GOV_002C2_BASE_COMMIT,
        "repository_path": WAVE11_BASELINE_REPOSITORY_PATH,
        "git_blob_sha1": DATA_GOV_002C2_BASELINE_GIT_BLOB,
        "raw_byte_count": DATA_GOV_002C2_HISTORICAL_PREFIX_BYTE_COUNT,
        "raw_sha256": DATA_GOV_002C2_HISTORICAL_PREFIX_SHA256,
        "append_offset": DATA_GOV_002C2_HISTORICAL_PREFIX_BYTE_COUNT,
        "current_section_must_be_eof": True,
    }
    assert phase["known_unrelated_exclusions"] == [WAVE14_S2_PROHIBITED_USER_PATH]

    superseded = set(phase["superseded_live_source_paths"])
    observed_live_mismatches = _data_gov_002c2_prior_active_source_mismatches()
    assert superseded <= observed_live_mismatches
    assert observed_live_mismatches <= (
        superseded
        | _trading_2461_superseded_live_source_paths()
        | _arch_004g2_paper_weekly_superseded_live_source_paths()
        | _ops_069_terminal_archive_superseded_live_source_paths()
        | _devx_004_superseded_live_source_paths()
        | _devx_001_reconciliation_superseded_live_source_paths()
        | _devx_005_superseded_live_source_paths()
        | _trading_2462_superseded_live_source_paths()
        | _devx_006_superseded_live_source_paths()
        | _arch_005m2_superseded_live_source_paths()
        | _arch_005m3_superseded_live_source_paths()
        | _arch_005m1_batch3_superseded_live_source_paths()
        | _arch_005m1_batch4_superseded_live_source_paths()
        | _ops_070_stable_release_superseded_live_source_paths()
    )
    assert phase["supersession"] == {
        "superseded_by_phase": ("DATA-GOV-002C2-RATE-ISSUE-ATTRIBUTION-SOURCE-OWNER-REVIEW-PACK"),
        "scope": "LATEST_ACTIVE_CURRENT_MISMATCH_SET_WITH_NEW_SOURCES",
        "historical_hashes_rewritten": False,
        "inherited_supersession_authority": DATA_GOV_002C1_SECTION,
        "current_hash_authority": f"{DATA_GOV_002C2_SECTION}.sources",
    }
    assert phase["removed_live_source_paths"] == []
    assert set(phase["new_source_paths"]) == DATA_GOV_002C2_NEW_SOURCE_PATHS
    assert set(phase["source_delta_paths"]) == (superseded | DATA_GOV_002C2_NEW_SOURCE_PATHS)

    sources = phase["sources"]
    source_paths = [str(source["path"]) for source in sources]
    assert len(source_paths) == len(set(source_paths))
    assert source_paths == sorted(source_paths, key=str.casefold)
    assert set(source_paths) == set(phase["source_delta_paths"])
    assert WAVE11_BASELINE_REPOSITORY_PATH not in source_paths
    assert WAVE14_S2_PROHIBITED_USER_PATH not in source_paths
    for source in sources:
        assert source["hash_normalization"] == "git_eol_lf"
        assert _source_sha256(source) == source["sha256"], source["path"]

    validation = phase["validation"]
    assert validation["review_pack_build"] == "PASS_6_SITES"
    assert validation["review_pack_check"] == "PASS_0_ERRORS"
    assert validation["focused"] == "PASS_166"
    assert validation["black"] == "PASS"
    assert validation["ruff"] == "PASS"
    assert validation["strict_mypy"] == "PASS"
    assert validation["task_registry"] == "PASS_BYTE_IDENTICAL"
    assert validation["architecture_devex"] == "PASS"
    assert validation["architecture_initial"] == "FAIL_1_C1_AUTHORITY_DRIFT_673_PASS"
    formal_fields = (
        "architecture",
        "contract",
        "report",
        "reproducibility",
        "integration",
        "full",
    )
    if phase["status"] == "BASELINE_DONE":
        for field in formal_fields:
            assert str(validation[field]).startswith("PASS_")
    else:
        for field in formal_fields:
            assert validation[field] == "PENDING"
    assert phase["safety"] == {
        "candidate_site_count": 6,
        "single_source_row_site_count": 4,
        "current_and_previous_observation_site_count": 2,
        "pending_source_owner_decision_count": 6,
        "proposal_is_authorization": False,
        "recommended_initial_isolation_rule": ("ALL_AFFECTED_RATE_SERIES_OUTSIDE_REQUIRED_SCOPE"),
        "window_or_row_level_isolation_authorized": False,
        "runtime_attribution_implemented": False,
        "new_issue_isolation_authorized": False,
        "message_or_sample_scope_parsing_used": False,
        "data_quality_behavior_changed": False,
        "capability_behavior_changed": False,
        "consumer_migration_executed": False,
        "strategy_logic_changed": False,
        "strategy_threshold_changed": False,
        "data_flow_changed": True,
        "cached_data_read": False,
        "cached_data_mutated": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def test_trading_2461_model_ladder_is_preserved_historical_authority() -> None:
    current_bytes = COMPATIBILITY_BASELINE_PATH.read_bytes()
    _assert_trading_2461_historical_prefix_immutable(
        current_bytes,
        _trading_2461_base_baseline_blob(),
    )
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    assert next(reversed(baseline)) == LATEST_COMPATIBILITY_SECTION
    assert list(baseline).index(DATA_GOV_002C2_SECTION) < list(baseline).index(TRADING_2461_SECTION)
    assert list(baseline).index(TRADING_2461_SECTION) < list(baseline).index(
        ARCH_004G2_PAPER_WEEKLY_SECTION
    )
    phase = baseline[TRADING_2461_SECTION]

    assert phase["schema_version"] == (
        "trading_2461_decision_target_capability_audit_model_ladder_compatibility.v1"
    )
    assert phase["status"] in {"VALIDATING_FINAL_EVIDENCE", "COMPLETE"}
    assert phase["boundary_id"] == "TRADING-2461-DECISION-TARGET-CAPABILITY-AUDIT-BATCH2"
    assert phase["task_ids"] == ["TRADING-2461_DECISION_TARGET_CAPABILITY_AUDIT_MODEL_LADDER"]
    assert phase["owner_authorization"] == (
        "owner_decision:TRADING-2461:2026-07-26:approve_decision_target_capability_audit_batch2_v1"
    )
    assert phase["prior_sections_immutability"] == {
        "source_commit": TRADING_2461_BASE_COMMIT,
        "repository_path": WAVE11_BASELINE_REPOSITORY_PATH,
        "git_blob_sha1": TRADING_2461_BASELINE_GIT_BLOB,
        "raw_byte_count": TRADING_2461_HISTORICAL_PREFIX_BYTE_COUNT,
        "raw_sha256": TRADING_2461_HISTORICAL_PREFIX_SHA256,
        "append_offset": TRADING_2461_HISTORICAL_PREFIX_BYTE_COUNT,
        "current_section_must_be_eof": True,
    }
    assert phase["known_unrelated_exclusions"] == [WAVE14_S2_PROHIBITED_USER_PATH]

    superseded = set(phase["superseded_live_source_paths"])
    observed_live_mismatches = _trading_2461_prior_active_source_mismatches()
    assert superseded <= observed_live_mismatches
    assert observed_live_mismatches <= (
        superseded
        | _arch_004g2_paper_weekly_superseded_live_source_paths()
        | _ops_069_terminal_archive_superseded_live_source_paths()
        | _devx_004_superseded_live_source_paths()
        | _devx_001_reconciliation_superseded_live_source_paths()
        | _devx_005_superseded_live_source_paths()
        | _trading_2462_superseded_live_source_paths()
        | _devx_006_superseded_live_source_paths()
        | _arch_005m2_superseded_live_source_paths()
        | _arch_005m3_superseded_live_source_paths()
        | _arch_005m1_batch3_superseded_live_source_paths()
        | _arch_005m1_batch4_superseded_live_source_paths()
        | _ops_070_stable_release_superseded_live_source_paths()
    )
    assert phase["supersession"] == {
        "superseded_by_phase": "TRADING-2461-DECISION-TARGET-CAPABILITY-AUDIT-BATCH2",
        "scope": "LATEST_ACTIVE_CURRENT_MISMATCH_SET_WITH_NEW_SOURCES",
        "historical_hashes_rewritten": False,
        "inherited_supersession_authority": DATA_GOV_002C2_SECTION,
        "current_hash_authority": f"{TRADING_2461_SECTION}.sources",
    }
    assert phase["removed_live_source_paths"] == []
    task_shadow_path = (
        TRADING_2461_COMPLETED_TASK_SHADOW_PATH
        if phase["status"] == "COMPLETE"
        else TRADING_2461_ACTIVE_TASK_SHADOW_PATH
    )
    expected_new_source_paths = TRADING_2461_CORE_NEW_SOURCE_PATHS | {task_shadow_path}
    assert set(phase["new_source_paths"]) == expected_new_source_paths
    assert set(phase["source_delta_paths"]) == superseded | expected_new_source_paths

    sources = phase["sources"]
    source_paths = [str(source["path"]) for source in sources]
    assert len(source_paths) == len(set(source_paths))
    assert source_paths == sorted(source_paths, key=str.casefold)
    assert set(source_paths) == set(phase["source_delta_paths"])
    assert WAVE11_BASELINE_REPOSITORY_PATH not in source_paths
    assert WAVE14_S2_PROHIBITED_USER_PATH not in source_paths
    for source in sources:
        assert source["hash_normalization"] == "git_eol_lf"
        assert _source_sha256(source) == source["sha256"], source["path"]

    validation = phase["validation"]
    expected_validation_fields = {
        "focused_and_authority",
        "reporting_architecture",
        "deprecation",
        "architecture",
        "contract",
        "report",
        "reproducibility",
        "full",
        "post_full_architecture",
        "post_full_contract",
    }
    assert set(validation) == expected_validation_fields
    if phase["status"] == "COMPLETE":
        assert all(str(value).startswith("PASS_") for value in validation.values())
    else:
        assert all(
            value == "PENDING" or str(value).startswith("PASS_") for value in validation.values()
        )
        assert validation["post_full_architecture"] == "PENDING"
        assert validation["post_full_contract"] == "PENDING"
    assert phase["safety"] == {
        "historical_seen_only": True,
        "promotion_authorized": False,
        "strategy_family_created": False,
        "risk_overlay_authorized": False,
        "qld_signal_role_changed": False,
        "prospective_accessed": False,
        "paper_shadow_changed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def test_arch_004g2_paper_weekly_is_preserved_historical_authority() -> None:
    current_bytes = COMPATIBILITY_BASELINE_PATH.read_bytes()
    _assert_arch_004g2_paper_weekly_historical_prefix_immutable(
        current_bytes,
        _arch_004g2_paper_weekly_base_baseline_blob(),
    )
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    assert next(reversed(baseline)) == LATEST_COMPATIBILITY_SECTION
    assert list(baseline).index(TRADING_2461_SECTION) < list(baseline).index(
        ARCH_004G2_PAPER_WEEKLY_SECTION
    )
    assert list(baseline).index(ARCH_004G2_PAPER_WEEKLY_SECTION) < list(baseline).index(
        OPS_069_TERMINAL_ARCHIVE_SECTION
    )
    phase = baseline[ARCH_004G2_PAPER_WEEKLY_SECTION]

    assert phase["schema_version"] == (
        "arch_004g2_paper_shadow_weekly_validation_authority_candidate_compatibility.v1"
    )
    assert phase["status"] in {
        "VALIDATING_REJECTED_THRESHOLD_MISS",
        "BASELINE_DONE_REJECTED_THRESHOLD_MISS",
    }
    assert phase["boundary_id"] == ("ARCH-004G2-PAPER-SHADOW-WEEKLY-VALIDATION-AUTHORITY-CANDIDATE")
    assert phase["task_ids"] == ["ARCH-004G2_VALIDATION_RUNTIME_BUDGET_AND_FIXTURE_REUSE"]
    assert phase["prior_sections_immutability"] == {
        "source_commit": ARCH_004G2_PAPER_WEEKLY_BASE_COMMIT,
        "repository_path": WAVE11_BASELINE_REPOSITORY_PATH,
        "git_blob_sha1": ARCH_004G2_PAPER_WEEKLY_BASELINE_GIT_BLOB,
        "raw_byte_count": ARCH_004G2_PAPER_WEEKLY_HISTORICAL_PREFIX_BYTE_COUNT,
        "raw_sha256": ARCH_004G2_PAPER_WEEKLY_HISTORICAL_PREFIX_SHA256,
        "append_offset": ARCH_004G2_PAPER_WEEKLY_HISTORICAL_PREFIX_BYTE_COUNT,
        "current_section_must_be_eof": True,
    }
    assert phase["known_unrelated_exclusions"] == [WAVE14_S2_PROHIBITED_USER_PATH]

    superseded = set(phase["superseded_live_source_paths"])
    observed_live_mismatches = _arch_004g2_paper_weekly_prior_active_source_mismatches()
    assert superseded <= observed_live_mismatches
    assert observed_live_mismatches <= (
        superseded
        | _ops_069_terminal_archive_superseded_live_source_paths()
        | _devx_004_superseded_live_source_paths()
        | _devx_001_reconciliation_superseded_live_source_paths()
        | _devx_005_superseded_live_source_paths()
        | _trading_2462_superseded_live_source_paths()
        | _devx_006_superseded_live_source_paths()
        | _arch_005m2_superseded_live_source_paths()
        | _arch_005m3_superseded_live_source_paths()
        | _arch_005m1_batch3_superseded_live_source_paths()
        | _arch_005m1_batch4_superseded_live_source_paths()
        | _ops_070_stable_release_superseded_live_source_paths()
    )
    assert phase["supersession"] == {
        "superseded_by_phase": ("ARCH-004G2-PAPER-SHADOW-WEEKLY-VALIDATION-AUTHORITY-CANDIDATE"),
        "scope": "LATEST_ACTIVE_CURRENT_MISMATCH_SET_WITH_NEW_SOURCES",
        "historical_hashes_rewritten": False,
        "inherited_supersession_authority": TRADING_2461_SECTION,
        "current_hash_authority": (f"{ARCH_004G2_PAPER_WEEKLY_SECTION}.sources"),
    }
    assert phase["removed_live_source_paths"] == []
    assert set(phase["new_source_paths"]) == ARCH_004G2_PAPER_WEEKLY_NEW_SOURCE_PATHS
    assert set(phase["source_delta_paths"]) == (
        superseded | ARCH_004G2_PAPER_WEEKLY_NEW_SOURCE_PATHS
    )

    sources = phase["sources"]
    source_paths = [str(source["path"]) for source in sources]
    assert source_paths == sorted(source_paths, key=str.casefold)
    assert len(source_paths) == len(set(source_paths))
    assert set(source_paths) == set(phase["source_delta_paths"])
    assert WAVE11_BASELINE_REPOSITORY_PATH not in source_paths
    assert WAVE14_S2_PROHIBITED_USER_PATH not in source_paths
    for source in sources:
        assert source["hash_normalization"] == "git_eol_lf"
        assert _source_sha256(source) == source["sha256"], source["path"]

    validation = phase["validation"]
    assert validation["baseline"] == "PASS_5_299_34_SECONDS"
    assert validation["contaminated_sample"] == ("EXCLUDED_542_51_SECONDS_EXTERNAL_FULL")
    assert validation["after_a"] == ("PASS_5_268_69_SECONDS_THRESHOLD_MISS_10_24_PERCENT")
    assert validation["after_b"] == "PASS_5_372_41_SECONDS_THRESHOLD_MISS"
    assert validation["implementation_restored"] == (
        "PASS_BLOB_6092152071797758d7413cc3d19bd5ebeac4126b"
    )
    assert validation["task_registry"] == "PASS_BYTE_IDENTICAL"
    assert validation["architecture_devex"] == "PASS"
    formal_fields = ("architecture", "contract")
    if phase["status"] == "BASELINE_DONE_REJECTED_THRESHOLD_MISS":
        for field in formal_fields:
            assert str(validation[field]).startswith("PASS_")
    else:
        for field in formal_fields:
            assert validation[field] == "PENDING"
    assert validation["full"] == "NOT_RUN_IMPLEMENTATION_REJECTED"

    safety = phase["safety"]
    assert safety["candidate_result"] == "REJECTED_THRESHOLD_MISS"
    assert safety["retained_test_implementation"] is False
    assert safety["production_source_changed"] is False
    assert safety["strategy_logic_changed"] is False
    assert safety["cached_data_mutated"] is False
    assert safety["production_effect"] == "none"
    assert safety["broker_action"] == "none"


def test_ops_069_terminal_archive_is_preserved_historical_authority() -> None:
    current_bytes = COMPATIBILITY_BASELINE_PATH.read_bytes()
    _assert_ops_069_terminal_archive_historical_prefix_immutable(
        current_bytes,
        _ops_069_terminal_archive_base_baseline_blob(),
    )
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    assert next(reversed(baseline)) == LATEST_COMPATIBILITY_SECTION
    assert list(baseline).index(OPS_069_TERMINAL_ARCHIVE_SECTION) < list(baseline).index(
        DEVX_004_SECTION
    )
    phase = baseline[OPS_069_TERMINAL_ARCHIVE_SECTION]

    assert phase["schema_version"] == (
        "ops_069_daily_input_capture_terminal_archive_compatibility.v1"
    )
    assert phase["status"] in {"VALIDATING", "COMPLETE"}
    assert phase["boundary_id"] == "OPS-069-TERMINAL-ARCHIVE"
    assert phase["task_ids"] == ["OPS-069_DAILY_INPUT_CAPTURE_AND_SESSION_GAP_LEDGER"]
    assert phase["owner_authorization"] == (
        "task_register_completed:OPS-069_DAILY_INPUT_CAPTURE_AND_SESSION_GAP_LEDGER:DONE"
    )
    assert phase["prior_sections_immutability"] == {
        "source_commit": OPS_069_TERMINAL_ARCHIVE_BASE_COMMIT,
        "repository_path": WAVE11_BASELINE_REPOSITORY_PATH,
        "git_blob_sha1": OPS_069_TERMINAL_ARCHIVE_BASELINE_GIT_BLOB,
        "raw_byte_count": OPS_069_TERMINAL_ARCHIVE_HISTORICAL_PREFIX_BYTE_COUNT,
        "raw_sha256": OPS_069_TERMINAL_ARCHIVE_HISTORICAL_PREFIX_SHA256,
        "append_offset": OPS_069_TERMINAL_ARCHIVE_HISTORICAL_PREFIX_BYTE_COUNT,
        "current_section_must_be_eof": True,
    }
    assert phase["known_unrelated_exclusions"] == [WAVE14_S2_PROHIBITED_USER_PATH]

    superseded = set(phase["superseded_live_source_paths"])
    observed_live_mismatches = _ops_069_terminal_archive_prior_active_source_mismatches()
    assert superseded <= observed_live_mismatches
    assert observed_live_mismatches <= (
        superseded
        | _devx_004_superseded_live_source_paths()
        | _devx_001_reconciliation_superseded_live_source_paths()
        | _devx_005_superseded_live_source_paths()
        | _trading_2462_superseded_live_source_paths()
        | _devx_006_superseded_live_source_paths()
        | _arch_005m2_superseded_live_source_paths()
        | _arch_005m3_superseded_live_source_paths()
        | _arch_005m1_batch3_superseded_live_source_paths()
        | _arch_005m1_batch4_superseded_live_source_paths()
        | _ops_070_stable_release_superseded_live_source_paths()
    )
    assert phase["supersession"] == {
        "superseded_by_phase": "OPS-069-TERMINAL-ARCHIVE",
        "scope": "LATEST_ACTIVE_CURRENT_MISMATCH_SET_WITH_ARCHIVE_MOVE",
        "historical_hashes_rewritten": False,
        "inherited_supersession_authority": ARCH_004G2_PAPER_WEEKLY_SECTION,
        "current_hash_authority": (f"{OPS_069_TERMINAL_ARCHIVE_SECTION}.sources"),
    }
    assert set(phase["removed_live_source_paths"]) == (
        OPS_069_TERMINAL_ARCHIVE_REMOVED_SOURCE_PATHS
    )
    assert set(phase["new_source_paths"]) == (OPS_069_TERMINAL_ARCHIVE_NEW_SOURCE_PATHS)
    expected_source_delta = (
        superseded - OPS_069_TERMINAL_ARCHIVE_REMOVED_SOURCE_PATHS
    ) | OPS_069_TERMINAL_ARCHIVE_NEW_SOURCE_PATHS
    assert set(phase["source_delta_paths"]) == expected_source_delta

    sources = phase["sources"]
    source_paths = [str(source["path"]) for source in sources]
    assert source_paths == sorted(source_paths, key=str.casefold)
    assert len(source_paths) == len(set(source_paths))
    assert set(source_paths) == expected_source_delta
    assert WAVE11_BASELINE_REPOSITORY_PATH not in source_paths
    assert WAVE14_S2_PROHIBITED_USER_PATH not in source_paths
    for source in sources:
        assert source["hash_normalization"] == "git_eol_lf"
        assert _source_sha256(source) == source["sha256"], source["path"]

    validation = phase["validation"]
    assert validation["focused_ops"] == "PASS_67"
    assert validation["runtime_capture_validation"] == "PASS_ISSUE_COUNT_0"
    assert validation["runtime_recovery_queue_validation"] == ("PASS_ISSUE_COUNT_0")
    assert validation["task_registry"] == "PASS_BYTE_IDENTICAL"
    assert validation["architecture_devex"] == "PASS"
    if phase["status"] == "COMPLETE":
        assert str(validation["architecture"]).startswith("PASS_")
        assert str(validation["contract"]).startswith("PASS_")
    else:
        assert validation["architecture"] == "PENDING"
        assert validation["contract"] == "PENDING"

    assert phase["safety"] == {
        "governance_status_correction_only": True,
        "runtime_behavior_changed": False,
        "data_flow_changed": False,
        "runtime_artifacts_read": True,
        "cached_data_read": False,
        "cached_data_mutated": False,
        "strict_pit_backfill_executed": False,
        "consumer_cutover_allowed": False,
        "weights_changed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def test_devx_004_is_preserved_historical_authority() -> None:
    _assert_devx_004_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _devx_004_base_baseline_blob(),
    )
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    assert next(reversed(baseline)) == LATEST_COMPATIBILITY_SECTION
    assert list(baseline).index(DEVX_004_SECTION) < list(baseline).index(
        DEVX_001_RECONCILIATION_SECTION
    )
    phase = baseline[DEVX_004_SECTION]
    assert phase["schema_version"] == (
        "devx_004_completed_task_closeout_registration_compatibility.v1"
    )
    assert phase["status"] in {"VALIDATING", "BASELINE_DONE"}
    assert phase["boundary_id"] == "DEVX-004-COMPLETED-TASK-CLOSEOUT"
    assert phase["task_ids"] == ["DEVX-004_COMPLETED_TASK_CLOSEOUT_REGISTRATION"]
    assert phase["prior_sections_immutability"] == {
        "source_commit": DEVX_004_BASE_COMMIT,
        "repository_path": WAVE11_BASELINE_REPOSITORY_PATH,
        "git_blob_sha1": DEVX_004_BASELINE_GIT_BLOB,
        "raw_byte_count": DEVX_004_HISTORICAL_PREFIX_BYTE_COUNT,
        "raw_sha256": DEVX_004_HISTORICAL_PREFIX_SHA256,
        "append_offset": DEVX_004_HISTORICAL_PREFIX_BYTE_COUNT,
        "current_section_must_be_eof": True,
    }
    superseded = set(phase["superseded_live_source_paths"])
    observed = _devx_004_prior_active_source_mismatches()
    assert superseded <= observed
    assert observed <= (
        superseded
        | _devx_001_reconciliation_superseded_live_source_paths()
        | _devx_005_superseded_live_source_paths()
        | _trading_2462_superseded_live_source_paths()
        | _devx_006_superseded_live_source_paths()
        | _arch_005m2_superseded_live_source_paths()
        | _arch_005m3_superseded_live_source_paths()
        | _arch_005m1_batch3_superseded_live_source_paths()
        | _arch_005m1_batch4_superseded_live_source_paths()
        | _ops_070_stable_release_superseded_live_source_paths()
    )
    assert phase["removed_live_source_paths"] == []
    assert set(phase["new_source_paths"]) == DEVX_004_NEW_SOURCE_PATHS
    expected = superseded | DEVX_004_NEW_SOURCE_PATHS
    assert set(phase["source_delta_paths"]) == expected
    sources = phase["sources"]
    assert [str(row["path"]) for row in sources] == sorted(expected, key=str.casefold)
    for source in sources:
        assert source["hash_normalization"] == "git_eol_lf"
        assert _source_sha256(source) == source["sha256"], source["path"]
    assert phase["validation"]["focused_skill"] == "PASS_30"
    assert phase["validation"]["bundle_parity"] == "PASS_5_FILES"
    if phase["status"] == "BASELINE_DONE":
        assert str(phase["validation"]["architecture"]).startswith("PASS_")
        assert str(phase["validation"]["contract"]).startswith("PASS_")
    assert phase["safety"] == {
        "completed_registration_closeout_only": True,
        "earlier_stages_fail_closed": True,
        "production_effect": "none",
        "broker_action": "none",
    }


def test_devx_001_reconciliation_is_current_hash_authority() -> None:
    _assert_devx_001_reconciliation_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _devx_001_reconciliation_base_baseline_blob(),
    )
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    assert next(reversed(baseline)) == LATEST_COMPATIBILITY_SECTION
    phase = baseline[DEVX_001_RECONCILIATION_SECTION]
    assert phase["schema_version"] == (
        "devx_001_research_worktree_lifecycle_reconciliation_compatibility.v1"
    )
    assert phase["status"] in {
        "VALIDATING_AUDIT_INCIDENT",
        "BASELINE_DONE_AUDIT_INCIDENT",
    }
    assert phase["boundary_id"] == "DEVX-001-RESEARCH-WORKTREE-RECONCILIATION"
    assert phase["task_ids"] == [
        "DEVX-001_TEMPORARY_WORKSPACE_LIFECYCLE_AND_CLEANUP",
        "DEVX-005_TARGET_BOUND_WORKTREE_AUDIT",
    ]
    assert phase["prior_sections_immutability"] == {
        "source_commit": DEVX_001_RECONCILIATION_BASE_COMMIT,
        "repository_path": WAVE11_BASELINE_REPOSITORY_PATH,
        "git_blob_sha1": DEVX_001_RECONCILIATION_BASELINE_GIT_BLOB,
        "raw_byte_count": DEVX_001_RECONCILIATION_HISTORICAL_PREFIX_BYTE_COUNT,
        "raw_sha256": DEVX_001_RECONCILIATION_HISTORICAL_PREFIX_SHA256,
        "append_offset": DEVX_001_RECONCILIATION_HISTORICAL_PREFIX_BYTE_COUNT,
        "current_section_must_be_eof": True,
    }
    superseded = set(phase["superseded_live_source_paths"])
    if DEVX_005_SECTION not in baseline:
        assert superseded == _devx_001_reconciliation_prior_active_source_mismatches()
    expected = superseded | DEVX_001_RECONCILIATION_NEW_SOURCE_PATHS
    assert phase["removed_live_source_paths"] == []
    assert set(phase["new_source_paths"]) == DEVX_001_RECONCILIATION_NEW_SOURCE_PATHS
    assert set(phase["source_delta_paths"]) == expected
    sources = phase["sources"]
    assert [str(row["path"]) for row in sources] == sorted(expected, key=str.casefold)
    for source in sources:
        assert source["hash_normalization"] == "git_eol_lf"
        assert _source_sha256(source) == source["sha256"], source["path"]
    assert phase["evidence"] == {
        "removed_worktree_count": 8,
        "removed_file_count": 78356,
        "released_logical_bytes": 2277275474,
        "migrated_file_count": 19,
        "migrated_bytes": 14017755,
        "migrated_bundle_sha256": (
            "0bfe5f2929ccfbd7404e6c15a509acf3a751db916acdbdffca373eb8a13cdc21"
        ),
        "retained_worktree_count": 3,
    }
    if phase["status"] == "BASELINE_DONE_AUDIT_INCIDENT":
        assert str(phase["validation"]["architecture"]).startswith("PASS_")
        assert str(phase["validation"]["contract"]).startswith("PASS_")
    assert phase["safety"] == {
        "target_tracked_diff_audit_proven": False,
        "audit_incident_recorded": True,
        "further_worktree_deletion_allowed": False,
        "known_unrelated_opened": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def test_devx_005_is_preserved_historical_authority() -> None:
    _assert_devx_005_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _devx_005_base_baseline_blob(),
    )
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    assert next(reversed(baseline)) == LATEST_COMPATIBILITY_SECTION
    assert list(baseline).index(DEVX_005_SECTION) < list(baseline).index(TRADING_2462_SECTION)
    phase = baseline[DEVX_005_SECTION]
    assert phase["schema_version"] == ("devx_005_target_bound_worktree_audit_compatibility.v1")
    assert phase["status"] in {"VALIDATING", "BASELINE_DONE"}
    assert phase["boundary_id"] == "DEVX-005-TARGET-BOUND-WORKTREE-AUDIT"
    assert phase["task_ids"] == ["DEVX-005_TARGET_BOUND_WORKTREE_AUDIT"]
    assert phase["prior_sections_immutability"] == {
        "source_commit": DEVX_005_BASE_COMMIT,
        "repository_path": WAVE11_BASELINE_REPOSITORY_PATH,
        "git_blob_sha1": DEVX_005_BASELINE_GIT_BLOB,
        "raw_byte_count": DEVX_005_HISTORICAL_PREFIX_BYTE_COUNT,
        "raw_sha256": DEVX_005_HISTORICAL_PREFIX_SHA256,
        "append_offset": DEVX_005_HISTORICAL_PREFIX_BYTE_COUNT,
        "current_section_must_be_eof": True,
    }
    superseded = set(phase["superseded_live_source_paths"])
    observed = _devx_005_prior_active_source_mismatches()
    assert superseded <= observed
    assert observed <= (
        superseded
        | _trading_2462_superseded_live_source_paths()
        | _devx_006_superseded_live_source_paths()
        | _arch_005m2_superseded_live_source_paths()
        | _arch_005m3_superseded_live_source_paths()
        | _arch_005m1_batch3_superseded_live_source_paths()
        | _arch_005m1_batch4_superseded_live_source_paths()
        | _ops_070_stable_release_superseded_live_source_paths()
    )
    expected = superseded | DEVX_005_NEW_SOURCE_PATHS
    assert phase["removed_live_source_paths"] == []
    assert set(phase["new_source_paths"]) == DEVX_005_NEW_SOURCE_PATHS
    assert set(phase["source_delta_paths"]) == expected
    sources = phase["sources"]
    assert [str(row["path"]) for row in sources] == sorted(expected, key=str.casefold)
    for source in sources:
        assert source["hash_normalization"] == "git_eol_lf"
        assert _source_sha256(source) == source["sha256"], source["path"]
    assert phase["evidence"] == {
        "audit_schema_version": "checkout_worktree_audit.v2",
        "focused_guard_tests": "PASS_20",
        "real_registered_target_audit": "PASS",
        "policy_and_target_toplevel_disclosed": True,
        "registration_identity_disclosed": True,
        "same_git_common_dir_proven": True,
    }
    if phase["status"] == "BASELINE_DONE":
        assert str(phase["validation"]["architecture"]).startswith("PASS_")
        assert str(phase["validation"]["contract"]).startswith("PASS_")
    assert phase["safety"] == {
        "target_binding_proven": True,
        "identity_drift_fails_closed": True,
        "independent_clone_fails_closed": True,
        "worktree_deletion_authorized": False,
        "known_unrelated_opened": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def test_trading_2462_tail_risk_audit_is_preserved_historical_authority() -> None:
    _assert_trading_2462_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _trading_2462_base_baseline_blob(),
    )
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    assert next(reversed(baseline)) == LATEST_COMPATIBILITY_SECTION
    assert list(baseline).index(DEVX_005_SECTION) < list(baseline).index(TRADING_2462_SECTION)
    assert list(baseline).index(TRADING_2462_SECTION) < list(baseline).index(DEVX_006_SECTION)
    phase = baseline[TRADING_2462_SECTION]

    assert phase["schema_version"] == (
        "trading_2462_tail_risk_robustness_falsification_audit_compatibility.v1"
    )
    assert phase["status"] in {"VALIDATING_FINAL_EVIDENCE", "COMPLETE"}
    assert phase["boundary_id"] == ("TRADING-2462-TAIL-RISK-ROBUSTNESS-FALSIFICATION-AUDIT")
    assert phase["task_ids"] == ["TRADING-2462_TAIL_RISK_CAPABILITY_ROBUSTNESS_FALSIFICATION_AUDIT"]
    assert phase["owner_authorization"] == (
        "owner_decision:TRADING-2462:2026-07-27:"
        "approve_tail_risk_capability_robustness_falsification_audit_v1"
    )
    assert phase["prior_sections_immutability"] == {
        "source_commit": TRADING_2462_BASE_COMMIT,
        "repository_path": WAVE11_BASELINE_REPOSITORY_PATH,
        "git_blob_sha1": TRADING_2462_BASELINE_GIT_BLOB,
        "raw_byte_count": TRADING_2462_HISTORICAL_PREFIX_BYTE_COUNT,
        "raw_sha256": TRADING_2462_HISTORICAL_PREFIX_SHA256,
        "append_offset": TRADING_2462_HISTORICAL_PREFIX_BYTE_COUNT,
        "current_section_must_be_eof": True,
    }
    assert phase["known_unrelated_exclusions"] == [WAVE14_S2_PROHIBITED_USER_PATH]

    superseded = set(phase["superseded_live_source_paths"])
    observed = _trading_2462_prior_active_source_mismatches()
    assert superseded <= observed
    assert observed <= (
        superseded
        | _devx_006_superseded_live_source_paths()
        | _arch_005m2_superseded_live_source_paths()
        | _arch_005m3_superseded_live_source_paths()
        | _arch_005m1_batch3_superseded_live_source_paths()
        | _arch_005m1_batch4_superseded_live_source_paths()
        | _ops_070_stable_release_superseded_live_source_paths()
    )
    assert phase["supersession"] == {
        "superseded_by_phase": ("TRADING-2462-TAIL-RISK-ROBUSTNESS-FALSIFICATION-AUDIT"),
        "scope": "LATEST_ACTIVE_CURRENT_MISMATCH_SET_WITH_NEW_SOURCES",
        "historical_hashes_rewritten": False,
        "inherited_supersession_authority": DEVX_005_SECTION,
        "current_hash_authority": f"{TRADING_2462_SECTION}.sources",
    }
    expected_removed_paths = (
        {TRADING_2462_ACTIVE_TASK_SHADOW_PATH} if phase["status"] == "COMPLETE" else set()
    )
    assert set(phase["removed_live_source_paths"]) == expected_removed_paths
    task_shadow_path = (
        TRADING_2462_COMPLETED_TASK_SHADOW_PATH
        if phase["status"] == "COMPLETE"
        else TRADING_2462_ACTIVE_TASK_SHADOW_PATH
    )
    expected_new_source_paths = TRADING_2462_CORE_NEW_SOURCE_PATHS | {task_shadow_path}
    assert set(phase["new_source_paths"]) == expected_new_source_paths
    assert set(phase["source_delta_paths"]) == (superseded | expected_new_source_paths)

    sources = phase["sources"]
    source_paths = [str(source["path"]) for source in sources]
    assert len(source_paths) == len(set(source_paths))
    assert source_paths == sorted(source_paths, key=str.casefold)
    assert set(source_paths) == set(phase["source_delta_paths"])
    assert WAVE11_BASELINE_REPOSITORY_PATH not in source_paths
    assert WAVE14_S2_PROHIBITED_USER_PATH not in source_paths
    for source in sources:
        assert source["hash_normalization"] == "git_eol_lf"
        assert _source_sha256(source) == source["sha256"], source["path"]

    validation = phase["validation"]
    expected_validation_fields = {
        "focused_and_authority",
        "reporting_architecture",
        "deprecation",
        "architecture",
        "contract",
        "report",
        "reproducibility",
        "full",
        "post_full_architecture",
        "post_full_contract",
    }
    assert set(validation) == expected_validation_fields
    if phase["status"] == "COMPLETE":
        assert all(str(value).startswith("PASS_") for value in validation.values())
    else:
        assert all(
            value == "PENDING" or str(value).startswith("PASS_") for value in validation.values()
        )
        assert validation["post_full_architecture"] == "PENDING"
        assert validation["post_full_contract"] == "PENDING"
    assert phase["safety"] == {
        "historical_seen_only": True,
        "result": "INSUFFICIENT_ROBUSTNESS_EVIDENCE",
        "decision_value_audit_authorized": False,
        "promotion_authorized": False,
        "strategy_family_created": False,
        "risk_overlay_authorized": False,
        "target_weights_generated": False,
        "qld_signal_role_changed": False,
        "prospective_accessed": False,
        "paper_shadow_changed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def test_devx_006_arch_005m1_is_current_hash_authority() -> None:
    _assert_devx_006_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _devx_006_base_baseline_blob(),
    )
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    assert list(baseline).index(DEVX_006_SECTION) < list(baseline).index(
        ARCH_005M2_SECTION
    )
    phase = baseline[DEVX_006_SECTION]
    assert phase["schema_version"] == (
        "devx_006_base_drift_aware_integration_revalidation_compatibility.v1"
    )
    assert phase["status"] in {"VALIDATING_FINAL_TREE", "BASELINE_DONE"}
    assert phase["boundary_id"] == "DEVX-006-ARCH-005M1-LATEST-MAIN-INTEGRATION"
    assert phase["task_ids"] == [
        "DEVX-006_BASE_DRIFT_AWARE_INTEGRATION_AND_REVALIDATION",
        "ARCH-005M1_STRICT_YAML_LOADER_CONSOLIDATION",
    ]
    assert phase["prior_sections_immutability"] == {
        "source_commit": DEVX_006_BASE_COMMIT,
        "repository_path": WAVE11_BASELINE_REPOSITORY_PATH,
        "git_blob_sha1": DEVX_006_BASELINE_GIT_BLOB,
        "raw_byte_count": DEVX_006_HISTORICAL_PREFIX_BYTE_COUNT,
        "raw_sha256": DEVX_006_HISTORICAL_PREFIX_SHA256,
        "append_offset": DEVX_006_HISTORICAL_PREFIX_BYTE_COUNT,
        "current_section_must_be_eof": True,
    }
    assert phase["known_unrelated_exclusions"] == [WAVE14_S2_PROHIBITED_USER_PATH]
    superseded = set(phase["superseded_live_source_paths"])
    current_mismatches = set(_devx_006_prior_active_source_mismatches())
    assert superseded <= current_mismatches
    assert current_mismatches - superseded <= set(
        _arch_005m2_superseded_live_source_paths()
        | _arch_005m3_superseded_live_source_paths()
        | _arch_005m1_batch3_superseded_live_source_paths()
        | _arch_005m1_batch4_superseded_live_source_paths()
        | _ops_070_stable_release_superseded_live_source_paths()
    )
    expected = superseded | DEVX_006_NEW_SOURCE_PATHS
    assert set(phase["removed_live_source_paths"]) == {
        DEVX_006_ACTIVE_TASK_SHADOW_PATH
    }
    assert set(phase["new_source_paths"]) == DEVX_006_NEW_SOURCE_PATHS
    assert set(phase["source_delta_paths"]) == expected
    sources = phase["sources"]
    assert [str(row["path"]) for row in sources] == sorted(expected, key=str.casefold)
    for source in sources:
        assert source["hash_normalization"] == "git_eol_lf"
        assert _source_sha256(source) == source["sha256"], source["path"]
    assert phase["implementation"] == {
        "frozen_lane_continues_during_main_drift": True,
        "real_git_delta_classification": True,
        "coordinator_refresh_once": True,
        "reviewed_reconciliation_plan_id": (
            "integration-revalidation-927fbb12af85ec52da7a"
        ),
        "strict_yaml_batch_1_integrated_without_rebuild": True,
        "automatic_git_mutation_allowed": False,
    }
    validation = phase["validation"]
    assert validation == {
        "focused": "PASS_278",
        "architecture": "PASS_710",
        "contract": "PASS_275",
        "integration": "PASS_995",
        "reproducibility": "PASS_23",
        "full": validation["full"],
    }
    assert validation["full"] == "PENDING_FINAL_TREE" or str(
        validation["full"]
    ).startswith("PASS_")
    assert phase["safety"] == {
        "known_unrelated_opened": False,
        "strategy_conclusion_changed": False,
        "data_quality_contract_changed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def test_arch_005m2_is_current_hash_authority() -> None:
    _assert_arch_005m2_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _arch_005m2_base_baseline_blob(),
    )
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    assert list(baseline).index(ARCH_005M2_SECTION) < list(baseline).index(
        ARCH_005M3_SECTION
    )
    phase = baseline[ARCH_005M2_SECTION]
    assert phase["schema_version"] == (
        "arch_005m2_portable_bootstrap_bundle_adoption_compatibility.v1"
    )
    assert phase["status"] in {"VALIDATING_FINAL_TREE", "COMPLETE"}
    assert phase["boundary_id"] == "ARCH-005M2-PORTABLE-BOOTSTRAP-BUNDLE-ADOPTION"
    assert phase["task_ids"] == [
        "ARCH-005M2_BOOTSTRAP_STANDALONE_VALIDATOR_PORTABLE_BUNDLE_ADOPTION"
    ]
    assert phase["prior_sections_immutability"] == {
        "source_commit": ARCH_005M2_BASE_COMMIT,
        "repository_path": WAVE11_BASELINE_REPOSITORY_PATH,
        "git_blob_sha1": ARCH_005M2_BASELINE_GIT_BLOB,
        "raw_byte_count": ARCH_005M2_HISTORICAL_PREFIX_BYTE_COUNT,
        "raw_sha256": ARCH_005M2_HISTORICAL_PREFIX_SHA256,
        "append_offset": ARCH_005M2_HISTORICAL_PREFIX_BYTE_COUNT,
        "current_section_must_be_eof": True,
    }
    assert phase["known_unrelated_exclusions"] == [WAVE14_S2_PROHIBITED_USER_PATH]
    superseded = set(phase["superseded_live_source_paths"])
    current_mismatches = set(_arch_005m2_prior_active_source_mismatches())
    assert superseded <= current_mismatches
    assert current_mismatches - superseded <= set(
        _arch_005m3_superseded_live_source_paths()
        | _arch_005m1_batch2_superseded_live_source_paths()
        | _arch_005m1_batch3_superseded_live_source_paths()
        | _arch_005m1_batch4_superseded_live_source_paths()
        | _ops_070_stable_release_superseded_live_source_paths()
    )
    task_shadow_path = (
        ARCH_005M2_COMPLETED_TASK_SHADOW_PATH
        if phase["status"] == "COMPLETE"
        else None
    )
    expected_new_paths = set(ARCH_005M2_CORE_NEW_SOURCE_PATHS)
    if task_shadow_path is not None:
        expected_new_paths.add(task_shadow_path)
    expected_removed_paths = (
        {ARCH_005M2_ACTIVE_TASK_SHADOW_PATH}
        if phase["status"] == "COMPLETE"
        else set()
    )
    assert set(phase["removed_live_source_paths"]) == expected_removed_paths
    assert set(phase["new_source_paths"]) == expected_new_paths
    assert set(phase["source_delta_paths"]) == superseded | expected_new_paths
    sources = phase["sources"]
    expected_source_paths = sorted(
        (superseded | expected_new_paths) - expected_removed_paths,
        key=str.casefold,
    )
    assert [str(row["path"]) for row in sources] == expected_source_paths
    for source in sources:
        assert source["hash_normalization"] == "git_eol_lf"
        assert _source_sha256(source) == source["sha256"], source["path"]
    assert phase["implementation"] == {
        "existing_bundle_schema_reused": "arch_005_bootstrap_validation_bundle.v1",
        "second_source_of_truth_created": False,
        "standalone_validator_reads_untracked_outputs": False,
        "historical_handoff_or_summary_bytes_changed": False,
        "g2_5_loader_signature_changed": False,
        "clean_clone_cli_passed": True,
        "clean_clone_validation_runtime_outputs_present": False,
    }
    validation = phase["validation"]
    assert validation["focused"] == "PASS_174"
    if phase["status"] == "COMPLETE":
        assert all(str(value).startswith("PASS_") for value in validation.values())
    else:
        assert all(
            value == "PENDING" or str(value).startswith("PASS_")
            for value in validation.values()
        )
    assert phase["safety"] == {
        "known_unrelated_opened": False,
        "strategy_conclusion_changed": False,
        "data_quality_contract_changed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def test_arch_005m3_is_current_hash_authority() -> None:
    _assert_arch_005m3_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _arch_005m3_base_baseline_blob(),
    )
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    assert list(baseline).index(ARCH_005M3_SECTION) < list(baseline).index(
        ARCH_005M1_BATCH2_SECTION
    )
    phase = baseline[ARCH_005M3_SECTION]
    assert phase["schema_version"] == (
        "arch_005m3_external_request_cache_multiprocess_harness_hardening_compatibility.v1"
    )
    assert phase["status"] in {"VALIDATING_FINAL_TREE", "COMPLETE"}
    assert phase["boundary_id"] == (
        "ARCH-005M3-EXTERNAL-REQUEST-CACHE-MULTIPROCESS-HARNESS"
    )
    assert phase["task_ids"] == [
        "ARCH-005M3_EXTERNAL_REQUEST_CACHE_MULTIPROCESS_HARNESS_HARDENING"
    ]
    assert phase["prior_sections_immutability"] == {
        "source_commit": ARCH_005M3_BASE_COMMIT,
        "repository_path": WAVE11_BASELINE_REPOSITORY_PATH,
        "git_blob_sha1": ARCH_005M3_BASELINE_GIT_BLOB,
        "raw_byte_count": ARCH_005M3_HISTORICAL_PREFIX_BYTE_COUNT,
        "raw_sha256": ARCH_005M3_HISTORICAL_PREFIX_SHA256,
        "append_offset": ARCH_005M3_HISTORICAL_PREFIX_BYTE_COUNT,
        "current_section_must_be_eof": True,
    }
    assert phase["known_unrelated_exclusions"] == [WAVE14_S2_PROHIBITED_USER_PATH]
    superseded = set(phase["superseded_live_source_paths"])
    current_mismatches = set(_arch_005m3_prior_active_source_mismatches())
    assert superseded <= current_mismatches
    assert current_mismatches - superseded <= set(
        _arch_005m1_batch2_superseded_live_source_paths()
        | _arch_005m1_batch3_superseded_live_source_paths()
        | _arch_005m1_batch4_superseded_live_source_paths()
        | _ops_070_stable_release_superseded_live_source_paths()
    )
    expected_new_paths = set(ARCH_005M3_CORE_NEW_SOURCE_PATHS)
    if phase["status"] == "COMPLETE":
        expected_new_paths.add(ARCH_005M3_COMPLETED_TASK_SHADOW_PATH)
    expected_removed_paths = (
        {ARCH_005M3_ACTIVE_TASK_SHADOW_PATH}
        if phase["status"] == "COMPLETE"
        else set()
    )
    assert set(phase["removed_live_source_paths"]) == expected_removed_paths
    assert set(phase["new_source_paths"]) == expected_new_paths
    assert set(phase["source_delta_paths"]) == superseded | expected_new_paths
    expected_source_paths = sorted(
        (superseded | expected_new_paths) - expected_removed_paths,
        key=str.casefold,
    )
    sources = phase["sources"]
    assert [str(row["path"]) for row in sources] == expected_source_paths
    for source in sources:
        assert source["hash_normalization"] == "git_eol_lf"
        assert _source_sha256(source) == source["sha256"], source["path"]

    for path in ARCH_005M3_FROZEN_PRODUCTION_PATHS:
        source = {"path": path, "hash_normalization": "git_eol_lf"}
        assert _raw_source_sha256(source) == _source_sha256_at_commit(
            source,
            ARCH_005M3_BASE_COMMIT,
        )
    assert phase["implementation"] == {
        "structured_probe_and_result_pid_binding": True,
        "single_monotonic_orchestration_deadline": True,
        "first_probe_early_exit_detection": True,
        "terminal_result_early_exit_detection": True,
        "terminate_join_kill_cleanup": True,
        "queue_close_and_join_thread_cleanup": True,
        "same_key_repeat_count": 20,
        "production_module_or_policy_changed": False,
    }
    validation = phase["validation"]
    assert validation["focused"] == "PASS_153"
    if phase["status"] == "COMPLETE":
        assert all(str(value).startswith("PASS_") for value in validation.values())
    else:
        assert all(
            value == "PENDING" or str(value).startswith("PASS_")
            for value in validation.values()
        )
    assert phase["safety"] == {
        "real_provider_request": False,
        "cached_market_data_mutated": False,
        "data_quality_contract_changed": False,
        "strategy_conclusion_changed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def test_arch_005m1_batch2_is_current_hash_authority() -> None:
    _assert_arch_005m1_batch2_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _arch_005m1_batch2_base_baseline_blob(),
    )
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    assert list(baseline).index(ARCH_005M1_BATCH2_SECTION) < list(baseline).index(
        ARCH_005M1_BATCH3_SECTION
    )
    phase = baseline[ARCH_005M1_BATCH2_SECTION]
    assert phase["schema_version"] == (
        "arch_005m1_batch2_integration_revalidation_loader_compatibility.v1"
    )
    assert phase["status"] in {"VALIDATING_BATCH_2", "BASELINE_DONE_BATCH_2"}
    assert phase["boundary_id"] == (
        "ARCH-005M1-BATCH2-INTEGRATION-REVALIDATION-LOADER"
    )
    assert phase["task_ids"] == ["ARCH-005M1_STRICT_YAML_LOADER_CONSOLIDATION"]
    assert phase["prior_sections_immutability"] == {
        "source_commit": ARCH_005M1_BATCH2_BASE_COMMIT,
        "repository_path": WAVE11_BASELINE_REPOSITORY_PATH,
        "git_blob_sha1": ARCH_005M1_BATCH2_BASELINE_GIT_BLOB,
        "raw_byte_count": ARCH_005M1_BATCH2_HISTORICAL_PREFIX_BYTE_COUNT,
        "raw_sha256": ARCH_005M1_BATCH2_HISTORICAL_PREFIX_SHA256,
        "append_offset": ARCH_005M1_BATCH2_HISTORICAL_PREFIX_BYTE_COUNT,
        "current_section_must_be_eof": True,
    }
    assert phase["known_unrelated_exclusions"] == [WAVE14_S2_PROHIBITED_USER_PATH]
    superseded = set(phase["superseded_live_source_paths"])
    current_mismatches = set(_arch_005m1_batch2_prior_active_source_mismatches())
    assert superseded <= current_mismatches
    assert current_mismatches - superseded <= set(
        _arch_005m1_batch3_superseded_live_source_paths()
        | _arch_005m1_batch4_superseded_live_source_paths()
        | _ops_070_stable_release_superseded_live_source_paths()
    )
    assert phase["removed_live_source_paths"] == []
    assert phase["new_source_paths"] == []
    assert set(phase["source_delta_paths"]) == superseded
    sources = phase["sources"]
    assert [str(row["path"]) for row in sources] == sorted(
        superseded,
        key=str.casefold,
    )
    for source in sources:
        assert source["hash_normalization"] == "git_eol_lf"
        assert _source_sha256(source) == source["sha256"], source["path"]

    later_changed_frozen_paths = (
        ARCH_005M1_BATCH2_FROZEN_PATHS
        & (
            _arch_005m1_batch3_superseded_live_source_paths()
            | _arch_005m1_batch4_superseded_live_source_paths()
        )
    )
    assert later_changed_frozen_paths == {
        "src/ai_trading_system/platform/architecture/wave_readiness.py",
        "src/ai_trading_system/us_equity_special_closure_policy.py",
    }
    for path in ARCH_005M1_BATCH2_FROZEN_PATHS - later_changed_frozen_paths:
        source = {"path": path, "hash_normalization": "git_eol_lf"}
        assert _raw_source_sha256(source) == _source_sha256_at_commit(
            source,
            ARCH_005M1_BATCH2_BASE_COMMIT,
        )
    assert phase["implementation"] == {
        "canonical_strict_yaml_primitive_reused": True,
        "local_unique_loader_removed": True,
        "key_policy": "STRING",
        "flatten_mapping": False,
        "reject_non_finite": False,
        "public_error_contract_preserved": True,
        "integration_plan_semantics_changed": False,
        "remaining_loader_count": 2,
    }
    validation = phase["validation"]
    assert validation["focused"] == "PASS_119"
    if phase["status"] == "BASELINE_DONE_BATCH_2":
        assert all(str(value).startswith("PASS_") for value in validation.values())
    else:
        assert all(
            value == "PENDING" or str(value).startswith("PASS_")
            for value in validation.values()
        )
    assert phase["safety"] == {
        "data_quality_contract_changed": False,
        "strategy_conclusion_changed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def test_arch_005m1_batch3_is_current_hash_authority() -> None:
    _assert_arch_005m1_batch3_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _arch_005m1_batch3_base_baseline_blob(),
    )
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    assert list(baseline).index(ARCH_005M1_BATCH3_SECTION) < list(baseline).index(
        ARCH_005M1_BATCH4_SECTION
    )
    phase = baseline[ARCH_005M1_BATCH3_SECTION]
    assert phase["schema_version"] == (
        "arch_005m1_batch3_wave_readiness_loader_compatibility.v1"
    )
    assert phase["status"] in {"VALIDATING_BATCH_3", "BASELINE_DONE_BATCH_3"}
    assert phase["boundary_id"] == "ARCH-005M1-BATCH3-WAVE-READINESS-LOADER"
    assert phase["task_ids"] == ["ARCH-005M1_STRICT_YAML_LOADER_CONSOLIDATION"]
    assert phase["prior_sections_immutability"] == {
        "source_commit": ARCH_005M1_BATCH3_BASE_COMMIT,
        "repository_path": WAVE11_BASELINE_REPOSITORY_PATH,
        "git_blob_sha1": ARCH_005M1_BATCH3_BASELINE_GIT_BLOB,
        "raw_byte_count": ARCH_005M1_BATCH3_HISTORICAL_PREFIX_BYTE_COUNT,
        "raw_sha256": ARCH_005M1_BATCH3_HISTORICAL_PREFIX_SHA256,
        "append_offset": ARCH_005M1_BATCH3_HISTORICAL_PREFIX_BYTE_COUNT,
        "current_section_must_be_eof": True,
    }
    assert phase["known_unrelated_exclusions"] == [WAVE14_S2_PROHIBITED_USER_PATH]
    superseded = set(phase["superseded_live_source_paths"])
    current_mismatches = set(_arch_005m1_batch3_prior_active_source_mismatches())
    assert superseded <= current_mismatches
    assert current_mismatches - superseded <= set(
        _arch_005m1_batch4_superseded_live_source_paths()
        | _ops_070_stable_release_superseded_live_source_paths()
    )
    assert phase["removed_live_source_paths"] == []
    assert phase["new_source_paths"] == []
    assert set(phase["source_delta_paths"]) == superseded
    sources = phase["sources"]
    assert [str(row["path"]) for row in sources] == sorted(
        superseded,
        key=str.casefold,
    )
    for source in sources:
        assert source["hash_normalization"] == "git_eol_lf"
        assert _source_sha256(source) == source["sha256"], source["path"]

    batch4_changed_frozen_paths = (
        ARCH_005M1_BATCH3_FROZEN_PATHS
        & _arch_005m1_batch4_superseded_live_source_paths()
    )
    assert batch4_changed_frozen_paths == {
        "src/ai_trading_system/us_equity_special_closure_policy.py"
    }
    for path in ARCH_005M1_BATCH3_FROZEN_PATHS - batch4_changed_frozen_paths:
        source = {"path": path, "hash_normalization": "git_eol_lf"}
        assert _raw_source_sha256(source) == _source_sha256_at_commit(
            source,
            ARCH_005M1_BATCH3_BASE_COMMIT,
        )
    assert phase["implementation"] == {
        "canonical_strict_yaml_primitive_reused": True,
        "local_unique_loader_removed": True,
        "key_policy": "STRING",
        "flatten_mapping": False,
        "canonical_reject_non_finite": False,
        "existing_recursive_value_validator_preserved": True,
        "public_error_contract_preserved": True,
        "readiness_decision_semantics_changed": False,
        "remaining_loader_count": 1,
    }
    validation = phase["validation"]
    assert validation["focused"] == "PASS_148"
    assert validation["strict_mypy"] == "PASS"
    assert validation["ruff"] == "PASS"
    if phase["status"] == "BASELINE_DONE_BATCH_3":
        assert all(str(value).startswith("PASS") for value in validation.values())
    else:
        assert all(
            value == "PENDING" or str(value).startswith("PASS")
            for value in validation.values()
        )
    assert phase["safety"] == {
        "data_quality_contract_changed": False,
        "strategy_conclusion_changed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def test_arch_005m1_batch4_is_current_hash_authority() -> None:
    _assert_arch_005m1_batch4_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _arch_005m1_batch4_base_baseline_blob(),
    )
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    assert list(baseline).index(ARCH_005M1_BATCH4_SECTION) < list(baseline).index(
        OPS_070_STABLE_RELEASE_SECTION
    )
    phase = baseline[ARCH_005M1_BATCH4_SECTION]
    assert phase["schema_version"] == (
        "arch_005m1_batch4_us_equity_special_closure_loader_compatibility.v1"
    )
    assert phase["status"] in {"VALIDATING_BATCH_4", "COMPLETE"}
    assert phase["boundary_id"] == (
        "ARCH-005M1-BATCH4-US-EQUITY-SPECIAL-CLOSURE-LOADER"
    )
    assert phase["task_ids"] == ["ARCH-005M1_STRICT_YAML_LOADER_CONSOLIDATION"]
    assert phase["prior_sections_immutability"] == {
        "source_commit": ARCH_005M1_BATCH4_BASE_COMMIT,
        "repository_path": WAVE11_BASELINE_REPOSITORY_PATH,
        "git_blob_sha1": ARCH_005M1_BATCH4_BASELINE_GIT_BLOB,
        "raw_byte_count": ARCH_005M1_BATCH4_HISTORICAL_PREFIX_BYTE_COUNT,
        "raw_sha256": ARCH_005M1_BATCH4_HISTORICAL_PREFIX_SHA256,
        "append_offset": ARCH_005M1_BATCH4_HISTORICAL_PREFIX_BYTE_COUNT,
        "current_section_must_be_eof": True,
    }
    assert phase["known_unrelated_exclusions"] == [WAVE14_S2_PROHIBITED_USER_PATH]
    superseded = set(phase["superseded_live_source_paths"])
    current_mismatches = set(_arch_005m1_batch4_prior_active_source_mismatches())
    assert superseded <= current_mismatches
    assert current_mismatches - superseded <= set(
        _ops_070_stable_release_superseded_live_source_paths()
    )
    expected_removed_paths = (
        {ARCH_005M1_BATCH4_ACTIVE_TASK_SHADOW_PATH}
        if phase["status"] == "COMPLETE"
        else set()
    )
    expected_new_paths = (
        {ARCH_005M1_BATCH4_COMPLETED_TASK_SHADOW_PATH}
        if phase["status"] == "COMPLETE"
        else set()
    )
    assert set(phase["removed_live_source_paths"]) == expected_removed_paths
    assert set(phase["new_source_paths"]) == expected_new_paths
    assert set(phase["source_delta_paths"]) == superseded | expected_new_paths
    sources = phase["sources"]
    expected_source_paths = sorted(
        (superseded | expected_new_paths) - expected_removed_paths,
        key=str.casefold,
    )
    assert [str(row["path"]) for row in sources] == expected_source_paths
    for source in sources:
        assert source["hash_normalization"] == "git_eol_lf"
        assert _source_sha256(source) == source["sha256"], source["path"]

    for path in ARCH_005M1_BATCH4_FROZEN_PATHS:
        source = {"path": path, "hash_normalization": "git_eol_lf"}
        assert _raw_source_sha256(source) == _source_sha256_at_commit(
            source,
            ARCH_005M1_BATCH4_BASE_COMMIT,
        )
    assert phase["implementation"] == {
        "canonical_strict_yaml_primitive_reused": True,
        "local_unique_loader_removed": True,
        "key_policy": "HASHABLE",
        "flatten_mapping": True,
        "reject_non_finite": False,
        "wrapped_value_error_contract_preserved": True,
        "policy_bytes_and_hash_changed": False,
        "calendar_decision_semantics_changed": False,
        "remaining_production_loader_count": 0,
    }
    validation = phase["validation"]
    assert validation["focused"] == "PASS_125"
    assert validation["consumer_regression"] == "PASS_76"
    assert validation["strict_mypy"] == "PASS"
    assert validation["ruff"] == "PASS"
    if phase["status"] == "COMPLETE":
        assert all(str(value).startswith("PASS") for value in validation.values())
    else:
        assert all(
            value == "PENDING" or str(value).startswith("PASS")
            for value in validation.values()
        )
    assert phase["safety"] == {
        "calendar_policy_bytes_changed": False,
        "data_quality_contract_changed": False,
        "strategy_conclusion_changed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def test_ops_070_stable_release_is_immutable_historical_authority() -> None:
    _assert_ops_070_stable_release_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _ops_070_stable_release_base_baseline_blob(),
    )
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    assert list(baseline).index(OPS_070_STABLE_RELEASE_SECTION) < list(baseline).index(
        OPS_070_RUNTIME_EXCLUDE_SECTION
    )
    phase = baseline[OPS_070_STABLE_RELEASE_SECTION]
    assert phase["schema_version"] == "ops_070_stable_ops_deployment_release_compatibility.v1"
    assert phase["status"] in {"VALIDATING_DEPLOYMENT", "DEPLOYMENT_ACCEPTED"}
    assert phase["boundary_id"] == "OPS-070-STABLE-OPS-DEPLOYMENT-RELEASE"
    assert phase["task_ids"] == [
        "OPS-070_OBJECTIVE_BLOCKER_AND_CONSUMER_DEPENDENCY_DAG",
        "DEVX-006_FRAGMENTED_GENERATED_AUTHORITY_AND_STABLE_TASK_SHADOW_V2",
    ]
    assert phase["prior_sections_immutability"] == {
        "source_commit": OPS_070_STABLE_RELEASE_BASE_COMMIT,
        "repository_path": WAVE11_BASELINE_REPOSITORY_PATH,
        "git_blob_sha1": OPS_070_STABLE_RELEASE_BASELINE_GIT_BLOB,
        "raw_byte_count": OPS_070_STABLE_RELEASE_HISTORICAL_PREFIX_BYTE_COUNT,
        "raw_sha256": OPS_070_STABLE_RELEASE_HISTORICAL_PREFIX_SHA256,
        "append_offset": OPS_070_STABLE_RELEASE_HISTORICAL_PREFIX_BYTE_COUNT,
        "current_section_must_be_eof": True,
    }
    assert phase["known_unrelated_exclusions"] == [WAVE14_S2_PROHIBITED_USER_PATH]
    superseded = set(phase["superseded_live_source_paths"])
    assert phase["removed_live_source_paths"] == []
    assert set(phase["new_source_paths"]) == OPS_070_STABLE_RELEASE_NEW_SOURCE_PATHS
    expected_source_paths = superseded | OPS_070_STABLE_RELEASE_NEW_SOURCE_PATHS
    assert set(phase["source_delta_paths"]) == expected_source_paths
    sources = phase["sources"]
    assert [str(row["path"]) for row in sources] == sorted(
        expected_source_paths,
        key=str.casefold,
    )
    for source in sources:
        assert source["hash_normalization"] == "git_eol_lf"
        assert (
            _source_sha256_at_commit(source, OPS_070_RUNTIME_EXCLUDE_BASE_COMMIT)
            == source["sha256"]
        ), source["path"]
    assert phase["implementation"] == {
        "independent_runtime_git_common_dir_required": True,
        "exact_reviewed_remote_main_required": True,
        "runtime_local_python_and_import_required": True,
        "release_candidate_and_deployment_receipts_content_derived": True,
        "validation_artifacts_bound_to_candidate_commit": True,
        "required_validation_tier_set_exact": True,
        "portable_validation_evidence_migrated_to_runtime": True,
        "required_critical_path_set_exact": True,
        "runtime_distribution_inventory_fingerprinted": True,
        "transactional_promotion_with_rollback": True,
        "checkout_write_guard_precedes_preflight_writes": True,
        "single_codex_scheduler_entry_required": True,
        "manual_execution_requires_explicit_flag": True,
        "generated_shared_views_coordinator_refresh_once": True,
    }
    validation = phase["validation"]
    assert all(
        value == "PENDING" or str(value).startswith("PASS")
        for value in validation.values()
    )
    assert phase["safety"] == {
        "data_quality_contract_changed": False,
        "strategy_logic_changed": False,
        "production_weights_written": False,
        "active_shadow_weights_written": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def test_ops_070_stable_release_rejects_historical_prefix_tamper() -> None:
    base_blob = _ops_070_stable_release_base_baseline_blob()
    tampered = bytearray(COMPATIBILITY_BASELINE_PATH.read_bytes())
    tampered[OPS_070_STABLE_RELEASE_HISTORICAL_PREFIX_BYTE_COUNT - 1] ^= 1
    with pytest.raises(AssertionError, match="historical prefix differs"):
        _assert_ops_070_stable_release_historical_prefix_immutable(
            bytes(tampered),
            base_blob,
        )


def test_ops_070_runtime_git_exclusion_is_immutable_historical_authority() -> None:
    _assert_ops_070_runtime_exclude_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _ops_070_runtime_exclude_base_baseline_blob(),
    )
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    assert list(baseline).index(OPS_070_RUNTIME_EXCLUDE_SECTION) < list(
        baseline
    ).index(OPS_070_CROSS_RELEASE_POLICY_SECTION)
    phase = baseline[OPS_070_RUNTIME_EXCLUDE_SECTION]
    assert (
        phase["schema_version"]
        == "ops_070_runtime_git_exclusion_contract_compatibility.v1"
    )
    assert phase["status"] in {
        "VALIDATING_RUNTIME_EXCLUDE",
        "RUNTIME_EXCLUDE_ACCEPTED",
    }
    assert phase["boundary_id"] == "OPS-070-RUNTIME-GIT-EXCLUSION-CONTRACT"
    assert phase["task_ids"] == [
        "OPS-070_OBJECTIVE_BLOCKER_AND_CONSUMER_DEPENDENCY_DAG"
    ]
    assert phase["prior_sections_immutability"] == {
        "source_commit": OPS_070_RUNTIME_EXCLUDE_BASE_COMMIT,
        "repository_path": WAVE11_BASELINE_REPOSITORY_PATH,
        "git_blob_sha1": OPS_070_RUNTIME_EXCLUDE_BASELINE_GIT_BLOB,
        "raw_byte_count": OPS_070_RUNTIME_EXCLUDE_HISTORICAL_PREFIX_BYTE_COUNT,
        "raw_sha256": OPS_070_RUNTIME_EXCLUDE_HISTORICAL_PREFIX_SHA256,
        "append_offset": OPS_070_RUNTIME_EXCLUDE_HISTORICAL_PREFIX_BYTE_COUNT,
        "current_section_must_be_eof": True,
    }
    assert phase["known_unrelated_exclusions"] == [WAVE14_S2_PROHIBITED_USER_PATH]
    superseded = set(phase["superseded_live_source_paths"])
    assert phase["removed_live_source_paths"] == []
    assert phase["new_source_paths"] == []
    assert set(phase["source_delta_paths"]) == superseded
    sources = phase["sources"]
    assert [str(row["path"]) for row in sources] == sorted(
        superseded,
        key=str.casefold,
    )
    for source in sources:
        assert source["hash_normalization"] == "git_eol_lf"
        assert (
            _source_sha256_at_commit(
                source,
                OPS_070_CROSS_RELEASE_POLICY_BASE_COMMIT,
            )
            == source["sha256"]
        ), source["path"]
    assert phase["implementation"] == {
        "runtime_only_git_exclude_reviewed": True,
        "exact_patterns": ["/outputs/", "/artifacts/", "/data/derived/"],
        "existing_unknown_rules_fail_closed": True,
        "git_metadata_path_bound": True,
        "deployment_receipt_live_revalidation": True,
        "development_dirty_semantics_changed": False,
        "migration_bytes_deleted": False,
    }
    validation = phase["validation"]
    assert all(
        value == "PENDING" or str(value).startswith("PASS")
        for value in validation.values()
    )
    assert phase["safety"] == {
        "data_quality_contract_changed": False,
        "strategy_logic_changed": False,
        "production_weights_written": False,
        "active_shadow_weights_written": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def test_ops_070_runtime_git_exclusion_rejects_historical_prefix_tamper() -> None:
    base_blob = _ops_070_runtime_exclude_base_baseline_blob()
    tampered = bytearray(COMPATIBILITY_BASELINE_PATH.read_bytes())
    tampered[OPS_070_RUNTIME_EXCLUDE_HISTORICAL_PREFIX_BYTE_COUNT - 1] ^= 1
    with pytest.raises(AssertionError, match="historical prefix differs"):
        _assert_ops_070_runtime_exclude_historical_prefix_immutable(
            bytes(tampered),
            base_blob,
        )


def test_ops_070_cross_release_policy_is_current_hash_authority() -> None:
    _assert_ops_070_cross_release_policy_historical_prefix_immutable(
        COMPATIBILITY_BASELINE_PATH.read_bytes(),
        _ops_070_cross_release_policy_base_baseline_blob(),
    )
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    assert next(reversed(baseline)) == OPS_070_CROSS_RELEASE_POLICY_SECTION
    phase = baseline[OPS_070_CROSS_RELEASE_POLICY_SECTION]
    assert (
        phase["schema_version"]
        == "ops_070_cross_release_promotion_policy_compatibility.v1"
    )
    assert phase["status"] in {
        "VALIDATING_CROSS_RELEASE_POLICY",
        "CROSS_RELEASE_POLICY_ACCEPTED",
    }
    assert phase["boundary_id"] == "OPS-070-CROSS-RELEASE-PROMOTION-POLICY"
    assert phase["task_ids"] == [
        "OPS-070_OBJECTIVE_BLOCKER_AND_CONSUMER_DEPENDENCY_DAG"
    ]
    assert phase["prior_sections_immutability"] == {
        "source_commit": OPS_070_CROSS_RELEASE_POLICY_BASE_COMMIT,
        "repository_path": WAVE11_BASELINE_REPOSITORY_PATH,
        "git_blob_sha1": OPS_070_CROSS_RELEASE_POLICY_BASELINE_GIT_BLOB,
        "raw_byte_count": OPS_070_CROSS_RELEASE_POLICY_HISTORICAL_PREFIX_BYTE_COUNT,
        "raw_sha256": OPS_070_CROSS_RELEASE_POLICY_HISTORICAL_PREFIX_SHA256,
        "append_offset": OPS_070_CROSS_RELEASE_POLICY_HISTORICAL_PREFIX_BYTE_COUNT,
        "current_section_must_be_eof": True,
    }
    assert phase["known_unrelated_exclusions"] == [WAVE14_S2_PROHIBITED_USER_PATH]
    superseded = set(phase["superseded_live_source_paths"])
    assert superseded == _ops_070_cross_release_policy_prior_active_source_mismatches()
    assert phase["removed_live_source_paths"] == []
    assert phase["new_source_paths"] == []
    assert set(phase["source_delta_paths"]) == superseded
    sources = phase["sources"]
    assert [str(row["path"]) for row in sources] == sorted(
        superseded,
        key=str.casefold,
    )
    for source in sources:
        assert source["hash_normalization"] == "git_eol_lf"
        assert _raw_source_sha256(source) == source["sha256"], source["path"]
    assert phase["implementation"] == {
        "pre_switch_policy_source": "coordinator_candidate",
        "audited_repository_binding": "permanent_runtime",
        "old_runtime_policy_parsed": False,
        "dirty_inventory_and_diff_checks_preserved": True,
        "manual_checkout_workaround_allowed": False,
        "policy_copy_workaround_allowed": False,
        "real_ancestor_schema_regression": True,
        "transaction_event_started_before_clean_audit": False,
        "runtime_head_changed_on_schema_blocker": False,
    }
    validation = phase["validation"]
    assert all(
        value == "PENDING" or str(value).startswith("PASS")
        for value in validation.values()
    )
    assert phase["safety"] == {
        "data_quality_contract_changed": False,
        "strategy_logic_changed": False,
        "production_weights_written": False,
        "active_shadow_weights_written": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def test_ops_070_cross_release_policy_rejects_historical_prefix_tamper() -> None:
    base_blob = _ops_070_cross_release_policy_base_baseline_blob()
    tampered = bytearray(COMPATIBILITY_BASELINE_PATH.read_bytes())
    tampered[OPS_070_CROSS_RELEASE_POLICY_HISTORICAL_PREFIX_BYTE_COUNT - 1] ^= 1
    with pytest.raises(AssertionError, match="historical prefix differs"):
        _assert_ops_070_cross_release_policy_historical_prefix_immutable(
            bytes(tampered),
            base_blob,
        )


def test_arch_005m1_batch4_rejects_historical_prefix_tamper() -> None:
    base_blob = _arch_005m1_batch4_base_baseline_blob()
    tampered = bytearray(COMPATIBILITY_BASELINE_PATH.read_bytes())
    tampered[ARCH_005M1_BATCH4_HISTORICAL_PREFIX_BYTE_COUNT - 1] ^= 1
    with pytest.raises(AssertionError, match="historical prefix differs"):
        _assert_arch_005m1_batch4_historical_prefix_immutable(
            bytes(tampered),
            base_blob,
        )


def test_arch_005m1_batch3_rejects_historical_prefix_tamper() -> None:
    base_blob = _arch_005m1_batch3_base_baseline_blob()
    tampered = bytearray(COMPATIBILITY_BASELINE_PATH.read_bytes())
    tampered[ARCH_005M1_BATCH3_HISTORICAL_PREFIX_BYTE_COUNT - 1] ^= 1
    with pytest.raises(AssertionError, match="historical prefix differs"):
        _assert_arch_005m1_batch3_historical_prefix_immutable(
            bytes(tampered),
            base_blob,
        )


def test_arch_005m1_batch2_rejects_historical_prefix_tamper() -> None:
    base_blob = _arch_005m1_batch2_base_baseline_blob()
    tampered = bytearray(COMPATIBILITY_BASELINE_PATH.read_bytes())
    tampered[ARCH_005M1_BATCH2_HISTORICAL_PREFIX_BYTE_COUNT - 1] ^= 1
    with pytest.raises(AssertionError, match="historical prefix differs"):
        _assert_arch_005m1_batch2_historical_prefix_immutable(
            bytes(tampered),
            base_blob,
        )


def test_arch_005m3_rejects_historical_prefix_tamper() -> None:
    base_blob = _arch_005m3_base_baseline_blob()
    tampered = bytearray(COMPATIBILITY_BASELINE_PATH.read_bytes())
    tampered[ARCH_005M3_HISTORICAL_PREFIX_BYTE_COUNT - 1] ^= 1
    with pytest.raises(AssertionError, match="historical prefix differs"):
        _assert_arch_005m3_historical_prefix_immutable(bytes(tampered), base_blob)


def test_arch_005m2_rejects_historical_prefix_tamper() -> None:
    base_blob = _arch_005m2_base_baseline_blob()
    tampered = bytearray(COMPATIBILITY_BASELINE_PATH.read_bytes())
    tampered[ARCH_005M2_HISTORICAL_PREFIX_BYTE_COUNT - 1] ^= 1
    with pytest.raises(AssertionError, match="historical prefix differs"):
        _assert_arch_005m2_historical_prefix_immutable(bytes(tampered), base_blob)


def test_devx_006_rejects_historical_prefix_tamper() -> None:
    base_blob = _devx_006_base_baseline_blob()
    tampered = bytearray(COMPATIBILITY_BASELINE_PATH.read_bytes())
    tampered[DEVX_006_HISTORICAL_PREFIX_BYTE_COUNT - 1] ^= 1
    with pytest.raises(AssertionError, match="historical prefix differs"):
        _assert_devx_006_historical_prefix_immutable(bytes(tampered), base_blob)


def test_trading_2458_retirement_rejects_historical_prefix_tamper() -> None:
    base_blob = _trading_2458_retirement_base_baseline_blob()
    tampered = bytearray(COMPATIBILITY_BASELINE_PATH.read_bytes())
    tampered[TRADING_2458_RETIREMENT_HISTORICAL_PREFIX_BYTE_COUNT - 1] ^= 1
    with pytest.raises(AssertionError, match="historical prefix differs"):
        _assert_trading_2458_retirement_historical_prefix_immutable(
            bytes(tampered),
            base_blob,
        )


def test_trading_2458_closeout_rejects_historical_prefix_tamper() -> None:
    base_blob = _trading_2458_closeout_base_baseline_blob()
    tampered = bytearray(COMPATIBILITY_BASELINE_PATH.read_bytes())
    tampered[TRADING_2458_CLOSEOUT_HISTORICAL_PREFIX_BYTE_COUNT - 1] ^= 1
    with pytest.raises(AssertionError, match="historical prefix differs"):
        _assert_trading_2458_closeout_historical_prefix_immutable(
            bytes(tampered),
            base_blob,
        )


def test_data_gov_002_phase_b2_rejects_historical_prefix_tamper() -> None:
    base_blob = _data_gov_002_phase_b2_base_baseline_blob()
    tampered = bytearray(COMPATIBILITY_BASELINE_PATH.read_bytes())
    tampered[DATA_GOV_002_PHASE_B2_HISTORICAL_PREFIX_BYTE_COUNT - 1] ^= 1
    with pytest.raises(AssertionError, match="historical prefix differs"):
        _assert_data_gov_002_phase_b2_historical_prefix_immutable(
            bytes(tampered),
            base_blob,
        )


def test_devx_003_rejects_historical_prefix_tamper() -> None:
    base_blob = _devx_003_base_baseline_blob()
    tampered = bytearray(COMPATIBILITY_BASELINE_PATH.read_bytes())
    tampered[DEVX_003_HISTORICAL_PREFIX_BYTE_COUNT - 1] ^= 1
    with pytest.raises(AssertionError, match="historical prefix differs"):
        _assert_devx_003_historical_prefix_immutable(
            bytes(tampered),
            base_blob,
        )


def test_data_gov_002c1_rejects_historical_prefix_tamper() -> None:
    base_blob = _data_gov_002c1_base_baseline_blob()
    tampered = bytearray(COMPATIBILITY_BASELINE_PATH.read_bytes())
    tampered[DATA_GOV_002C1_HISTORICAL_PREFIX_BYTE_COUNT - 1] ^= 1
    with pytest.raises(AssertionError, match="historical prefix differs"):
        _assert_data_gov_002c1_historical_prefix_immutable(
            bytes(tampered),
            base_blob,
        )


def test_data_gov_002c2_rejects_historical_prefix_tamper() -> None:
    base_blob = _data_gov_002c2_base_baseline_blob()
    tampered = bytearray(COMPATIBILITY_BASELINE_PATH.read_bytes())
    tampered[DATA_GOV_002C2_HISTORICAL_PREFIX_BYTE_COUNT - 1] ^= 1
    with pytest.raises(AssertionError, match="historical prefix differs"):
        _assert_data_gov_002c2_historical_prefix_immutable(
            bytes(tampered),
            base_blob,
        )


def test_trading_2461_rejects_historical_prefix_tamper() -> None:
    base_blob = _trading_2461_base_baseline_blob()
    tampered = bytearray(COMPATIBILITY_BASELINE_PATH.read_bytes())
    tampered[TRADING_2461_HISTORICAL_PREFIX_BYTE_COUNT - 1] ^= 1
    with pytest.raises(AssertionError, match="historical prefix differs"):
        _assert_trading_2461_historical_prefix_immutable(
            bytes(tampered),
            base_blob,
        )


def test_arch_004g2_paper_weekly_rejects_historical_prefix_tamper() -> None:
    base_blob = _arch_004g2_paper_weekly_base_baseline_blob()
    tampered = bytearray(COMPATIBILITY_BASELINE_PATH.read_bytes())
    tampered[ARCH_004G2_PAPER_WEEKLY_HISTORICAL_PREFIX_BYTE_COUNT - 1] ^= 1
    with pytest.raises(AssertionError, match="historical prefix differs"):
        _assert_arch_004g2_paper_weekly_historical_prefix_immutable(
            bytes(tampered),
            base_blob,
        )


def test_ops_069_terminal_archive_rejects_historical_prefix_tamper() -> None:
    base_blob = _ops_069_terminal_archive_base_baseline_blob()
    tampered = bytearray(COMPATIBILITY_BASELINE_PATH.read_bytes())
    tampered[OPS_069_TERMINAL_ARCHIVE_HISTORICAL_PREFIX_BYTE_COUNT - 1] ^= 1
    with pytest.raises(AssertionError, match="historical prefix differs"):
        _assert_ops_069_terminal_archive_historical_prefix_immutable(
            bytes(tampered),
            base_blob,
        )


def test_devx_004_rejects_historical_prefix_tamper() -> None:
    tampered = bytearray(COMPATIBILITY_BASELINE_PATH.read_bytes())
    tampered[DEVX_004_HISTORICAL_PREFIX_BYTE_COUNT - 1] ^= 1
    with pytest.raises(AssertionError, match="historical prefix differs"):
        _assert_devx_004_historical_prefix_immutable(
            bytes(tampered),
            _devx_004_base_baseline_blob(),
        )


def test_devx_001_reconciliation_rejects_historical_prefix_tamper() -> None:
    tampered = bytearray(COMPATIBILITY_BASELINE_PATH.read_bytes())
    tampered[DEVX_001_RECONCILIATION_HISTORICAL_PREFIX_BYTE_COUNT - 1] ^= 1
    with pytest.raises(AssertionError, match="historical prefix differs"):
        _assert_devx_001_reconciliation_historical_prefix_immutable(
            bytes(tampered),
            _devx_001_reconciliation_base_baseline_blob(),
        )


def test_devx_005_rejects_historical_prefix_tamper() -> None:
    tampered = bytearray(COMPATIBILITY_BASELINE_PATH.read_bytes())
    tampered[DEVX_005_HISTORICAL_PREFIX_BYTE_COUNT - 1] ^= 1
    with pytest.raises(AssertionError, match="historical prefix differs"):
        _assert_devx_005_historical_prefix_immutable(
            bytes(tampered),
            _devx_005_base_baseline_blob(),
        )


def test_trading_2462_rejects_historical_prefix_tamper() -> None:
    tampered = bytearray(COMPATIBILITY_BASELINE_PATH.read_bytes())
    tampered[TRADING_2462_HISTORICAL_PREFIX_BYTE_COUNT - 1] ^= 1
    with pytest.raises(AssertionError, match="historical prefix differs"):
        _assert_trading_2462_historical_prefix_immutable(
            bytes(tampered),
            _trading_2462_base_baseline_blob(),
        )


def test_data_gov_002_phase_b1_rejects_historical_prefix_tamper() -> None:
    base_blob = _data_gov_002_phase_b1_base_baseline_blob()
    tampered = bytearray(COMPATIBILITY_BASELINE_PATH.read_bytes())
    tampered[DATA_GOV_002_PHASE_B1_HISTORICAL_PREFIX_BYTE_COUNT - 1] ^= 1
    with pytest.raises(AssertionError, match="historical prefix differs"):
        _assert_data_gov_002_phase_b1_historical_prefix_immutable(
            bytes(tampered),
            base_blob,
        )


def test_data_gov_002_phase_a_rejects_historical_prefix_tamper() -> None:
    base_blob = _data_gov_002_base_baseline_blob()
    tampered = bytearray(COMPATIBILITY_BASELINE_PATH.read_bytes())
    tampered[DATA_GOV_002_HISTORICAL_PREFIX_BYTE_COUNT - 1] ^= 1
    with pytest.raises(AssertionError, match="historical prefix differs"):
        _assert_data_gov_002_historical_prefix_immutable(
            bytes(tampered),
            base_blob,
        )


def test_devx_002_rejects_historical_prefix_tamper() -> None:
    base_blob = _devx_002_base_baseline_blob()
    tampered = bytearray(COMPATIBILITY_BASELINE_PATH.read_bytes())
    tampered[DEVX_002_HISTORICAL_PREFIX_BYTE_COUNT - 1] ^= 1
    with pytest.raises(AssertionError, match="historical prefix differs"):
        _assert_devx_002_historical_prefix_immutable(
            bytes(tampered),
            base_blob,
        )


def test_devx_002_default_ordinary_push_v2_rejects_historical_prefix_tamper() -> None:
    base_blob = _devx_002_push_v2_base_baseline_blob()
    tampered = bytearray(COMPATIBILITY_BASELINE_PATH.read_bytes())
    tampered[DEVX_002_PUSH_V2_HISTORICAL_PREFIX_BYTE_COUNT - 1] ^= 1
    with pytest.raises(AssertionError, match="historical prefix differs"):
        _assert_devx_002_push_v2_historical_prefix_immutable(
            bytes(tampered),
            base_blob,
        )


def test_arch_004g2_observability_rejects_historical_prefix_tamper() -> None:
    base_blob = _arch_004g2_observability_base_baseline_blob()
    tampered = bytearray(COMPATIBILITY_BASELINE_PATH.read_bytes())
    tampered[ARCH_004G2_OBSERVABILITY_HISTORICAL_PREFIX_BYTE_COUNT - 1] ^= 1
    with pytest.raises(AssertionError, match="historical prefix differs"):
        _assert_arch_004g2_observability_historical_prefix_immutable(
            bytes(tampered),
            base_blob,
        )


def test_arch_004g2_closure_threshold_rejects_historical_prefix_tamper() -> None:
    base_blob = _arch_004g2_closure_threshold_base_baseline_blob()
    tampered = bytearray(COMPATIBILITY_BASELINE_PATH.read_bytes())
    tampered[ARCH_004G2_CLOSURE_THRESHOLD_HISTORICAL_PREFIX_BYTE_COUNT - 1] ^= 1
    with pytest.raises(AssertionError, match="historical prefix differs"):
        _assert_arch_004g2_closure_threshold_historical_prefix_immutable(
            bytes(tampered),
            base_blob,
        )


def test_trading_2458_2460_integration_rejects_historical_prefix_tamper() -> None:
    base_blob = _trading_2458_2460_integration_base_baseline_blob()
    tampered = bytearray(COMPATIBILITY_BASELINE_PATH.read_bytes())
    tampered[TRADING_2458_2460_INTEGRATION_HISTORICAL_PREFIX_BYTE_COUNT - 1] ^= 1
    with pytest.raises(AssertionError):
        _assert_trading_2458_2460_integration_historical_prefix_immutable(
            bytes(tampered),
            base_blob,
        )


def test_arch_005s4d_s2_rejects_historical_prefix_tamper() -> None:
    base_blob = _arch_005s4d_s2_base_baseline_blob()
    valid_append = base_blob + f"\n{ARCH_005S4D_S2_SECTION}:\n  status: TEST_ONLY\n".encode()
    _assert_arch_005s4d_s2_historical_prefix_immutable(valid_append, base_blob)

    tampered = bytearray(valid_append)
    tampered[ARCH_005S4D_S2_HISTORICAL_PREFIX_BYTE_COUNT - 1] ^= 1
    with pytest.raises(AssertionError, match="historical prefix differs"):
        _assert_arch_005s4d_s2_historical_prefix_immutable(
            bytes(tampered),
            base_blob,
        )


def test_arch_005s4e_rejects_historical_prefix_tamper() -> None:
    base_blob = _arch_005s4e_base_baseline_blob()
    valid_append = base_blob + f"\n{ARCH_005S4E_SECTION}:\n  status: TEST_ONLY\n".encode()
    _assert_arch_005s4e_historical_prefix_immutable(valid_append, base_blob)

    tampered = bytearray(valid_append)
    tampered[ARCH_005S4E_HISTORICAL_PREFIX_BYTE_COUNT - 1] ^= 1
    with pytest.raises(AssertionError, match="historical prefix differs"):
        _assert_arch_005s4e_historical_prefix_immutable(
            bytes(tampered),
            base_blob,
        )


def test_ops_070_rejects_historical_prefix_tamper() -> None:
    base_blob = _ops_070_base_baseline_blob()
    valid_append = base_blob + f"\n{OPS_070_SECTION}:\n  status: TEST_ONLY\n".encode()
    _assert_ops_070_historical_prefix_immutable(valid_append, base_blob)

    tampered = bytearray(valid_append)
    tampered[OPS_070_HISTORICAL_PREFIX_BYTE_COUNT - 1] ^= 1
    with pytest.raises(AssertionError, match="historical prefix differs"):
        _assert_ops_070_historical_prefix_immutable(
            bytes(tampered),
            base_blob,
        )


def test_ops_069_rejects_historical_prefix_tamper() -> None:
    base_blob = _ops_069_base_baseline_blob()
    valid_append = base_blob + f"\n{OPS_069_SECTION}:\n  status: TEST_ONLY\n".encode()
    _assert_ops_069_historical_prefix_immutable(valid_append, base_blob)

    tampered = bytearray(valid_append)
    tampered[OPS_069_HISTORICAL_PREFIX_BYTE_COUNT - 1] ^= 1
    with pytest.raises(AssertionError, match="historical prefix differs"):
        _assert_ops_069_historical_prefix_immutable(
            bytes(tampered),
            base_blob,
        )


def test_data_gov_001_d0b2b_rejects_historical_prefix_tamper() -> None:
    base_blob = _d0b2b_base_baseline_blob()
    valid_append = base_blob + f"\n{D0B2B_SECTION}:\n  status: TEST_ONLY\n".encode()
    _assert_d0b2b_historical_prefix_immutable(valid_append, base_blob)

    tampered = bytearray(valid_append)
    tampered[D0B2B_HISTORICAL_PREFIX_BYTE_COUNT - 1] ^= 1
    with pytest.raises(AssertionError, match="historical prefix differs"):
        _assert_d0b2b_historical_prefix_immutable(
            bytes(tampered),
            base_blob,
        )


def test_arch_004_wave15_rejects_historical_prefix_tamper() -> None:
    base_blob = _wave15_base_baseline_blob()
    valid_append = base_blob + f"\n{WAVE15_SECTION}:\n  status: TEST_ONLY\n".encode()
    _assert_wave15_historical_prefix_immutable(valid_append, base_blob)

    tampered = bytearray(valid_append)
    tampered[WAVE15_HISTORICAL_PREFIX_BYTE_COUNT - 1] ^= 1
    with pytest.raises(AssertionError, match="historical prefix differs"):
        _assert_wave15_historical_prefix_immutable(
            bytes(tampered),
            base_blob,
        )


def test_arch_004_wave14_s2_rejects_historical_prefix_tamper() -> None:
    base_blob = _wave14_s2_base_baseline_blob()
    valid_append = base_blob + f"\n{WAVE14_S2_SECTION}:\n  status: TEST_ONLY\n".encode()
    _assert_wave14_s2_historical_prefix_immutable(valid_append, base_blob)

    tampered = bytearray(valid_append)
    tampered[WAVE14_S2_HISTORICAL_PREFIX_BYTE_COUNT - 1] ^= 1
    with pytest.raises(AssertionError, match="historical prefix differs"):
        _assert_wave14_s2_historical_prefix_immutable(
            bytes(tampered),
            base_blob,
        )


def test_arch_004_wave14_s2_full_attempt_chain_is_append_only_and_strict() -> None:
    pending = {
        "required": True,
        "status": "PENDING",
        "run_count": 0,
        "attempts_append_only": True,
        "executed_attempts_may_be_removed_or_overwritten": False,
        "post_pass_repeat_full_allowed": False,
        "attempts": [
            {
                "attempt_id": "wave14_s2_full_1",
                "role": "INITIAL_FORMAL_GATE",
                "required": True,
                "status": "PENDING",
            }
        ],
    }
    _assert_wave14_s2_full_attempt_chain(pending)

    failed_then_pending = deepcopy(pending)
    failed_attempt = failed_then_pending["attempts"][0]
    failed_attempt.update(
        {
            "status": "FAIL",
            "tested_section_status": "VALIDATING_WAVE14_S2",
            "tested_commit": "1" * 40,
            "tested_tree": "2" * 40,
            "full_sensitive_source_manifest_sha256": "3" * 64,
            "artifact": {
                "path": (
                    "outputs/validation_runtime/full_20260724T000001Z/test_runtime_summary.json"
                ),
                "sha256": "4" * 64,
                "size_bytes": 101,
                "passed": 10,
                "failed": 1,
                "skipped": 0,
            },
        }
    )
    failed_then_pending["run_count"] = 1
    failed_then_pending["attempts"].append(
        {
            "attempt_id": "wave14_s2_full_2",
            "role": "FAILURE_FIX_REPLACEMENT",
            "required": True,
            "status": "PENDING",
            "replaces_attempt_id": "wave14_s2_full_1",
        }
    )
    _assert_wave14_s2_full_attempt_chain(failed_then_pending)

    failed_then_passed = deepcopy(failed_then_pending)
    passed_attempt = failed_then_passed["attempts"][-1]
    passed_attempt.update(
        {
            "status": "PASS",
            "tested_section_status": "VALIDATING_WAVE14_S2",
            "tested_commit": "5" * 40,
            "tested_tree": "6" * 40,
            "full_sensitive_source_manifest_sha256": "7" * 64,
            "artifact": {
                "path": (
                    "outputs/validation_runtime/full_20260724T000002Z/test_runtime_summary.json"
                ),
                "sha256": "8" * 64,
                "size_bytes": 202,
                "passed": 20,
                "failed": 0,
                "skipped": 1,
            },
        }
    )
    failed_then_passed["status"] = "PASS_AFTER_FAILURE_FIX"
    failed_then_passed["run_count"] = 2
    _assert_wave14_s2_full_attempt_chain(failed_then_passed)

    invalid_cases = []
    wrong_count = deepcopy(failed_then_passed)
    wrong_count["run_count"] = 1
    invalid_cases.append(wrong_count)
    boolean_failed = deepcopy(failed_then_passed)
    boolean_failed["attempts"][-1]["artifact"]["failed"] = False
    invalid_cases.append(boolean_failed)
    missing_failed = deepcopy(failed_then_passed)
    del missing_failed["attempts"][-1]["artifact"]["failed"]
    invalid_cases.append(missing_failed)
    absolute_artifact = deepcopy(failed_then_passed)
    absolute_artifact["attempts"][-1]["artifact"]["path"] = (
        "C:/outputs/validation_runtime/full_bad/test_runtime_summary.json"
    )
    invalid_cases.append(absolute_artifact)
    wrong_parent = deepcopy(failed_then_pending)
    wrong_parent["attempts"][-1]["replaces_attempt_id"] = "unrelated"
    invalid_cases.append(wrong_parent)
    reused_artifact_path = deepcopy(failed_then_passed)
    reused_artifact_path["attempts"][-1]["artifact"]["path"] = reused_artifact_path["attempts"][0][
        "artifact"
    ]["path"]
    invalid_cases.append(reused_artifact_path)
    reused_artifact_hash = deepcopy(failed_then_passed)
    reused_artifact_hash["attempts"][-1]["artifact"]["sha256"] = reused_artifact_hash["attempts"][
        0
    ]["artifact"]["sha256"]
    invalid_cases.append(reused_artifact_hash)
    deleted_failure = deepcopy(failed_then_passed)
    deleted_failure["attempts"] = [deleted_failure["attempts"][-1]]
    deleted_failure["run_count"] = 1
    deleted_failure["status"] = "PASS"
    invalid_cases.append(deleted_failure)

    for invalid in invalid_cases:
        with pytest.raises((AssertionError, KeyError)):
            _assert_wave14_s2_full_attempt_chain(invalid)


def test_arch_004_wave14_s2_complete_requires_every_formal_tier_pass() -> None:
    validation = {
        "required_formal_tiers": {
            "pre_full": list(WAVE14_S2_PRE_FULL_REQUIRED_TIERS),
            "post_full": list(WAVE14_S2_POST_FULL_REQUIRED_TIERS),
        },
        "pre_full_formal_tiers": {
            name: {"required": True, "status": "PASS", "passed": 1, "failed": 0}
            for name in WAVE14_S2_PRE_FULL_REQUIRED_TIERS
        },
        "post_full_formal_tiers": {
            name: {"required": True, "status": "PASS", "passed": 1, "failed": 0}
            for name in WAVE14_S2_POST_FULL_REQUIRED_TIERS
        },
    }
    _assert_wave14_s2_formal_tiers(validation, phase_complete=True)
    for group_name, tier_names in (
        ("pre_full_formal_tiers", WAVE14_S2_PRE_FULL_REQUIRED_TIERS),
        ("post_full_formal_tiers", WAVE14_S2_POST_FULL_REQUIRED_TIERS),
    ):
        for tier_name in tier_names:
            incomplete = deepcopy(validation)
            incomplete[group_name][tier_name]["status"] = "PENDING"
            with pytest.raises(AssertionError, match="COMPLETE requires"):
                _assert_wave14_s2_formal_tiers(incomplete, phase_complete=True)


def test_arch_004_wave13_rejects_historical_prefix_tamper() -> None:
    base_blob = _wave13_base_baseline_blob()
    valid_append = base_blob + f"\n{WAVE13_SECTION}:\n  status: TEST_ONLY\n".encode()
    _assert_wave13_historical_prefix_immutable(valid_append, base_blob)

    tampered = bytearray(valid_append)
    tampered[0] ^= 1
    with pytest.raises(AssertionError, match="historical prefix differs"):
        _assert_wave13_historical_prefix_immutable(bytes(tampered), base_blob)


def test_arch_004_g2_5_wave11_rejects_historical_source_hash_rewrite() -> None:
    current_bytes = COMPATIBILITY_BASELINE_PATH.read_bytes()
    base_blob = _wave11_base_baseline_blob()
    section_offset = current_bytes.index(b"\nintegrated_change_trading_2452:\n")
    source_marker = b"- {path: docs/system_flow.md, sha256: "
    source_offset = current_bytes.index(source_marker, section_offset)
    hash_offset = source_offset + len(source_marker)
    historical_hash = current_bytes[hash_offset : hash_offset + 64]
    assert historical_hash == (b"2e09f70fadd52c90dcac3bd18c24625154e9f95a7a43dc57ad98d05a15efb9a3")
    tampered = current_bytes[:hash_offset] + (b"0" * 64) + current_bytes[hash_offset + 64 :]

    with pytest.raises(AssertionError, match="historical prefix differs"):
        _assert_wave11_historical_prefix_immutable(tampered, base_blob)


def test_arch_004_g2_5_wave11_rejects_unportable_or_reused_replacement_evidence() -> None:
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    attempts = baseline[WAVE11_SECTION]["validation"]["full_validation"]["attempts"]
    _assert_wave11_full_attempt_chain(attempts)

    deleted_failure = [deepcopy(attempts[0]), deepcopy(attempts[-1])]
    reordered_failures = [
        deepcopy(attempts[1]),
        deepcopy(attempts[0]),
        deepcopy(attempts[-1]),
    ]
    wrong_parent = deepcopy(attempts)
    wrong_parent[-1]["replaces_attempt_id"] = "UNRELATED_ATTEMPT"
    reused_path = deepcopy(attempts)
    reused_path[1]["artifact"]["path"] = attempts[0]["artifact"]["path"]
    reused_sha256 = deepcopy(attempts)
    reused_sha256[1]["artifact"]["sha256"] = attempts[0]["artifact"]["sha256"]
    intermediate_pending = deepcopy(attempts)
    intermediate_pending[1]["status"] = "PENDING"
    duplicate_id = deepcopy(attempts)
    duplicate_id[1]["attempt_id"] = attempts[0]["attempt_id"]
    missing_artifact = deepcopy(attempts)
    del missing_artifact[1]["artifact"]
    latest_fail = deepcopy(attempts)
    latest_fail[-1].update({"status": "FAIL", "failed": 1})
    invalid_cases = [
        (
            deleted_failure,
            "immediately preceding failed attempt",
        ),
        (
            reordered_failures,
            "first Full attempt must remain the initial formal gate",
        ),
        (
            wrong_parent,
            "immediately preceding failed attempt",
        ),
        (
            reused_path,
            "artifact paths must be unique",
        ),
        (
            reused_sha256,
            "artifact SHA256 values must be unique",
        ),
        (intermediate_pending, "intermediate Full attempt must be FAIL"),
        (duplicate_id, "Full attempt ids must be unique"),
        (missing_artifact, "executed attempt requires portable artifact evidence"),
        (latest_fail, "latest Full attempt must be PENDING or PASS"),
    ]

    for invalid_attempts, error_pattern in invalid_cases:
        with pytest.raises(AssertionError, match=error_pattern):
            _assert_wave11_full_attempt_chain(invalid_attempts)

    equal_positive_size = deepcopy(attempts)
    equal_positive_size[1]["artifact"]["size_bytes"] = attempts[0]["artifact"]["size_bytes"]
    _assert_wave11_full_attempt_chain(equal_positive_size)


def test_arch_004_compatibility_baseline_freezes_surface_and_core_hashes() -> None:
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)

    assert baseline["status"] == "FROZEN_AFTER_ARCH_004A1_REMEDIATION"
    assert baseline["surface_inventory"]["total"] == 3812
    assert baseline["surface_inventory"]["types"]["cli_command"] == 1103
    assert baseline["surface_inventory"]["types"]["report_registry_entry"] == 1358
    assert baseline["repository_inventory"]["python_module_count"] == 752
    assert baseline["checkout_hash_normalization"] == {
        "schema_version": "arch_004_checkout_hash_normalization.v1",
        "status": "PASS",
        "policy": "git_eol_lf",
        "normalized_source_count": 9,
        "source_content_changed": False,
        "production_effect": "none",
        "paths": [
            "docs/artifact_catalog.md",
            "src/ai_trading_system/cli_commands/reports.py",
            "src/ai_trading_system/cli_commands/research_execution_common.py",
            "src/ai_trading_system/etf_portfolio/dynamic_v3_signal_filter_foundation.py",
            "src/ai_trading_system/etf_portfolio/dynamic_v3_system_target.py",
            "src/ai_trading_system/etf_portfolio/dynamic_v3_weight_batch_search.py",
            "src/ai_trading_system/interfaces/cli/etf_portfolio/"
            "dynamic_v3_signal_filter_foundation.py",
            "src/ai_trading_system/interfaces/cli/etf_portfolio/"
            "dynamic_v3_system_target_smoothed_freshness.py",
            "tests/test_cli_direct.py",
        ],
    }
    assert baseline["validation_baseline"]["full_after_fix"]["status"] == "PASS"
    assert baseline["validation_baseline"]["full_after_fix"]["failed"] == 0
    date_range_contract = baseline["explicit_cli_adapter_contracts"]["date_range_kwargs"]
    assert date_range_contract["positional_parameters"] == [
        "as_of",
        "start_date",
        "end_date",
    ]
    assert date_range_contract["exact_output_keys"] == [
        "as_of_date",
        "start_date",
        "end_date",
    ]
    assert str(date_range_contract["missing_start_default"]) == "2022-12-01"
    assert baseline["explicit_cli_adapter_contracts"]["as_of_kwargs"] == {
        "positional_parameters": ["as_of"],
        "exact_output_keys": ["as_of_date"],
    }
    execution_cli_source = next(
        source
        for source in baseline["frozen_sources"]
        if source["contract_id"] == "execution_cli_date_adapters_after_arch_004a1"
    )
    assert execution_cli_source["hash_normalization"] == "git_eol_lf"
    assert execution_cli_source["previous_worktree_sha256"] == (
        "188c9318e0a530a0b269b8c11bb6bd594f51bb96a8650b95cd1433750a48cbcc"
    )
    for source in baseline["frozen_sources"]:
        if source.get("historical_phase_a_hash"):
            assert source["superseded_by_phase"] in {
                "ARCH-004D",
                "ARCH-004F1",
                "ARCH-004G2.4P",
            }
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["contract_id"]
    phase_b = baseline["phase_b_semantic_kernel"]
    assert phase_b["status"] == "COMPLETE_PHASE_C_READY"
    assert phase_b["contract_schema"] == "research_evaluation_context.v1"
    assert phase_b["repository_inventory"] == {
        "python_module_count": 757,
        "python_test_file_count": 1100,
    }
    assert phase_b["validation"]["focused"]["passed"] == 74
    assert phase_b["validation"]["contract_validation"]["passed"] == 197
    assert phase_b["validation"]["full_parallel"]["status"] == "PASS"
    assert phase_b["validation"]["full_parallel"]["passed"] == 5375
    assert phase_b["validation"]["full_parallel"]["failed"] == 0
    for source in phase_b["sources"]:
        if source.get("historical_phase_b_hash"):
            assert source["superseded_by_phase"] == "ARCH-004C"
            assert source["current_hash_tracked_in"] == "phase_c_platform_contracts.sources"
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_c = baseline["phase_c_platform_contracts"]
    assert phase_c["status"] == "COMPLETE_PHASE_D_READY"
    assert phase_c["contract_schemas"] == [
        "artifact_envelope.v1",
        "data_quality_evidence.v1",
        "workflow_spec.v1",
        "run_ledger.v1",
        "report_spec.v1",
    ]
    assert phase_c["direct_writer_ratchet"] == {
        "baseline_path": "inputs/architecture/arch_004c_direct_writer_baseline.yaml",
        "baseline_call_count": 894,
        "current_call_count": 893,
        "violation_count": 0,
    }
    assert set(phase_c["parity"].values()) == {"PASS"}
    assert phase_c["validation"]["focused"]["passed"] == 120
    assert phase_c["validation"]["contract_validation"]["passed"] == 197
    assert phase_c["validation"]["full_parallel"]["passed"] == 5404
    assert phase_c["validation"]["full_parallel"]["failed"] == 0
    for source in phase_c["sources"]:
        if source.get("historical_phase_c_hash"):
            assert source["superseded_by_phase"] in {
                "ARCH-004F3",
                "ARCH-004G1",
                "TRADING-2443",
            }
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_d = baseline["phase_d_reference_vertical_slice"]
    assert phase_d["status"] == "COMPLETE_PHASE_E_READY"
    assert phase_d["reference_slice"] == "growth_tilt_candidate_family_closure"
    assert set(phase_d["parity"].values()) == {"PASS"}
    assert phase_d["additive_sidecars"] == {
        "artifact_envelope": "artifact_envelope.v1",
        "run_ledger": "run_ledger.v1",
        "data_quality_required": False,
        "data_quality_pass_fabricated": False,
    }
    assert phase_d["architecture"]["second_same_plugin_spec_without_python_module"] == "PASS"
    assert phase_d["validation"]["focused"]["passed"] == 77
    assert phase_d["validation"]["contract_validation"]["passed"] == 197
    assert phase_d["validation"]["full_parallel"]["passed"] == 5411
    assert phase_d["validation"]["full_parallel"]["failed"] == 0
    for source in phase_d["sources"]:
        if source.get("historical_phase_d_hash"):
            assert source["superseded_by_phase"] in {"ARCH-004E", "ARCH-004F2_RUNTIME"}
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_e = baseline["phase_e_devex_ownership_generated_indexes"]
    assert phase_e["status"] == "COMPLETE_PHASE_F_READY"
    assert phase_e["repository_inventory"] == {
        "python_module_count_including_ignored": 777,
        "python_test_and_support_file_count": 1107,
    }
    assert phase_e["ownership"]["owner_roles"] == [
        "code_owner",
        "policy_owner",
        "data_owner",
        "artifact_owner",
        "runtime_owner",
    ]
    assert phase_e["ownership"]["module_orphan_count"] == 0
    assert phase_e["ownership"]["module_specific_overlap_count"] == 0
    assert phase_e["ownership"]["test_orphan_count"] == 0
    assert phase_e["ownership"]["test_specific_overlap_count"] == 0
    assert phase_e["aggregate_shadow"] == {
        "target_count": 3,
        "fragment_count": 4,
        "existing_source_of_truth_changed": False,
        "deterministic": "PASS",
    }
    assert phase_e["impact_selection"]["replaces_full_validation"] is False
    assert phase_e["architecture_fitness"] == {
        "status": "PASS",
        "direct_writer_baseline": 894,
        "direct_writer_current": 893,
        "violation_count": 0,
    }
    assert phase_e["validation"]["architecture_tier"]["passed"] == 78
    assert phase_e["validation"]["contract_validation"]["passed"] == 197
    assert phase_e["validation"]["full_parallel"]["passed"] == 5420
    assert phase_e["validation"]["full_parallel"]["failed"] == 0
    for source in phase_e["sources"]:
        if source.get("historical_phase_e_hash"):
            assert source["superseded_by_phase"] in {
                "ARCH-004F2",
                "ARCH-004F2_RUNTIME",
                "ARCH-004G",
                "ARCH-004G2.4_EB0",
                "ARCH-005-PB1",
            }
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_f2 = baseline["phase_f2_research_lifecycle_and_execution_chain"]
    assert phase_f2["status"] == "BASELINE_DONE_RUNTIME_MIGRATION_PENDING"
    assert phase_f2["execution_chain"]["state_classes"] == [
        "CANONICAL",
        "REFERENCE",
        "LEGACY",
        "BLOCKED",
        "PLANNED",
    ]
    assert str(phase_f2["execution_chain"]["market_regime_start"]) == "2022-12-01"
    assert str(phase_f2["execution_chain"]["primary_research_window_start"]) == "2021-02-22"
    assert phase_f2["execution_chain"]["periodic_review_auto_tuning_allowed"] is False
    assert phase_f2["repository_inventory"]["python_test_and_support_file_count"] == 1108
    assert phase_f2["validation"]["focused_document_contract"]["passed"] == 23
    assert phase_f2["validation"]["architecture_fitness"]["passed"] == 80
    assert phase_f2["validation"]["contract_validation"]["passed"] == 197
    for source in phase_f2["sources"]:
        if source.get("historical_phase_f2_documentation_hash"):
            assert source["superseded_by_phase"] in {
                "ARCH-004F2_RUNTIME",
                "TRADING-2452",
            }
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    runtime = baseline["phase_f2_runtime_lifecycle"]
    assert runtime["status"] == "COMPLETE_F1_F3_READY"
    assert runtime["contracts"] == {
        "lifecycle_schema": "research_lifecycle.v1",
        "preregistration_schema": "research_preregistration.v1",
        "legacy_campaign_disposition": "REUSE_WITH_EXPLICIT_COMPATIBILITY_ASSESSMENT",
        "missing_binding_behavior": "BLOCKED",
        "periodic_review_auto_tuning_allowed": False,
        "lifecycle_sidecar_additive": True,
    }
    assert runtime["repository_inventory"] == {
        "python_module_count_including_ignored": 779,
        "python_test_and_support_file_count": 1109,
    }
    assert set(runtime["parity"].values()) == {"PASS"}
    assert runtime["validation"]["architecture_fitness"]["passed"] == 88
    assert runtime["validation"]["contract_validation"]["passed"] == 197
    assert runtime["validation"]["full_parallel"]["passed"] == 5430
    assert runtime["validation"]["full_parallel"]["failed"] == 0
    for source in runtime["sources"]:
        if source.get("historical_phase_f2_runtime_hash"):
            assert source["superseded_by_phase"] in {
                "ARCH-004F1",
                "ARCH-004G1.3D",
                "ARCH-004G2.4O",
            }
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_f1 = baseline["phase_f1_operations_control_plane"]
    assert phase_f1["status"] == "COMPLETE_F3_READY"
    assert phase_f1["contracts"]["shadow_execution_enabled"] is False
    assert phase_f1["contracts"]["additive_shadow_artifact_emission"] is True
    assert phase_f1["contracts"]["execution_state_schema"] == "operations_execution_state.v1"
    assert phase_f1["contracts"]["idempotent_only_resume"] is True
    assert phase_f1["contracts"]["legacy_daily_executor_cut_in_enabled"] is True
    assert phase_f1["contracts"]["execution_ledger_schema"] == "run_ledger.v1"
    assert phase_f1["contracts"]["non_daily_automatic_dispatch_enabled"] is False
    assert phase_f1["contracts"]["non_daily_manual_dispatch_enabled"] is True
    assert phase_f1["contracts"]["periodic_plan_schema"] == "periodic_operations_plan.v1"
    assert phase_f1["scheduled_task_inventory"] == {
        "daily": 37,
        "non_daily": 41,
        "total": 78,
    }
    assert phase_f1["parity"]["trading_day_fixture_1"] == "PASS"
    assert phase_f1["parity"]["closed_market_fixture_1"] == "PASS"
    assert phase_f1["parity"]["additive_shadow_artifact_emission"] == "PASS"
    assert phase_f1["parity"]["legacy_markdown_bytes"] == "PASS"
    assert phase_f1["parity"]["concurrent_lock"] == "BLOCKED"
    assert phase_f1["parity"]["stale_lock_recovery"] == "EXPIRED_ONLY"
    assert phase_f1["parity"]["duplicate_completed_trigger"] == "ALREADY_COMPLETE"
    assert phase_f1["parity"]["non_idempotent_partial_resume"] == "BLOCKED"
    assert phase_f1["parity"]["atomic_state_write"] == "PASS"
    assert phase_f1["parity"]["periodic_task_plan_count"] == 41
    assert phase_f1["parity"]["periodic_automatic_command_dispatch"] is False
    for source in phase_f1["sources"]:
        if source.get("historical_phase_f1_hash"):
            assert source["superseded_by_phase"] in {
                "ARCH-004F3",
                "ARCH-004G2.3C",
                "ARCH-004G2.4P",
                "TRADING-2443",
            }
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_f3 = baseline["phase_f3_reporting_architecture"]
    assert phase_f3["status"] == "COMPLETE_G_READY"
    assert phase_f3["contracts"]["owner_daily_core_section_count"] == 10
    assert phase_f3["contracts"]["owner_queue_requires_due_and_actionable"] is True
    assert phase_f3["contracts"]["reporting_layer_recompute_allowed"] is False
    assert phase_f3["contracts"]["research_auto_tune_allowed"] is False
    assert phase_f3["contracts"]["proposal_may_equal_adoption"] is False
    assert phase_f3["contracts"]["reader_brief_native_cut_in_enabled"] is False
    assert phase_f3["parity"]["report_registry_coverage_count"] == 1358
    assert phase_f3["parity"]["report_registry_silent_drop_count"] == 0
    assert phase_f3["repository_inventory"] == {
        "python_module_count_including_ignored": 793,
        "python_test_and_support_file_count": 1111,
        "aggregate_fragment_count": 13,
        "report_fragment_count": 4,
    }
    assert phase_f3["validation"]["full_parallel"]["passed"] == 5494
    assert phase_f3["validation"]["full_parallel"]["failed"] == 0
    for source in phase_f3["sources"]:
        if source.get("historical_phase_f3_hash"):
            assert source["superseded_by_phase"] in {
                "ARCH-004G",
                "ARCH-004G2.4P",
                "ARCH-004G2.4AH",
                "TRADING-2444",
                "TRADING-2452",
            }
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g0 = baseline["phase_g0_deprecation_inventory_and_policy"]
    assert phase_g0["status"] == "COMPLETE_G1_IN_PROGRESS"
    assert phase_g0["contracts"] == {
        "deprecation_record_schema": "deprecation_record.v1",
        "lifecycle": ["EXPERIMENTAL", "ACTIVE", "DEPRECATED", "FROZEN", "REMOVED"],
        "required_removal_gate_count": 12,
        "permanent_dual_track_allowed": False,
        "runtime_removal_allowed_in_g0": False,
        "unknown_reachability_is_removal_ready": False,
        "artifact_retention_separate_from_code_removal": True,
    }
    assert phase_g0["target_inventory"] == {
        "target_count": 9,
        "active_count": 6,
        "deprecated_count": 3,
        "removal_ready_count": 0,
        "runtime_removal_performed": False,
    }
    assert phase_g0["validation"]["architecture_fitness"]["passed"] == 156
    assert phase_g0["validation"]["contract_validation"]["passed"] == 203
    for source in phase_g0["sources"]:
        if source.get("historical_phase_g0_hash"):
            assert source["superseded_by_phase"] in {
                "ARCH-004G1",
                "ARCH-004G2.1",
                "ARCH-004G2.4P",
                "ARCH-004G2.4_EB0",
            }
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g1 = baseline["phase_g1_shared_writer_migration"]
    assert phase_g1["status"] == "FIRST_FAMILY_COMPLETE_G1_CONTINUES"
    assert phase_g1["family"] == {
        "canonical_json_writer": "write_json_atomic_without_trailing_newline",
        "canonical_text_writer": "write_text_atomic",
        "migrated_module_count": 3,
        "removed_private_wrapper_count": 6,
        "private_wrapper_remaining_count": 0,
        "internal_callers_use_canonical_writer": True,
        "direct_writer_before": 893,
        "direct_writer_after": 887,
        "direct_writer_reduction": 6,
    }
    assert phase_g1["parity"]["artifact_path_schema_status"] == "PASS"
    assert phase_g1["parity"]["data_quality_behavior_changed"] is False
    assert phase_g1["parity"]["production_effect"] == "none"
    assert phase_g1["validation"]["focused"]["passed"] == 29
    assert phase_g1["validation"]["architecture_fitness"]["passed"] == 159
    assert phase_g1["validation"]["architecture_fitness"]["current_direct_writer_calls"] == 887
    for source in phase_g1["sources"]:
        if source.get("historical_phase_g1_hash"):
            assert source["superseded_by_phase"] in {"ARCH-004G1.3A", "ARCH-004G1.3C"}
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g1_3a = baseline["phase_g1_3a_trading_engine_summary_writer_migration"]
    assert phase_g1_3a["status"] == "SECOND_FAMILY_COMPLETE_G1_CONTINUES"
    assert phase_g1_3a["family"] == {
        "canonical_json_writer": "write_json_atomic",
        "canonical_text_writer": "write_text_atomic",
        "migrated_module_count": 5,
        "removed_private_writer_count": 10,
        "private_writer_remaining_count": 0,
        "direct_writer_before": 887,
        "direct_writer_after": 877,
        "direct_writer_reduction": 10,
    }
    assert phase_g1_3a["parity"]["sort_keys"] is False
    assert phase_g1_3a["parity"]["trailing_newline"] is True
    assert phase_g1_3a["parity"]["oserror_boundary"] == "PASS"
    assert phase_g1_3a["parity"]["investment_semantics_changed"] is False
    assert phase_g1_3a["validation"]["focused"] == {"status": "PASS", "passed": 95}
    assert phase_g1_3a["validation"]["architecture_fitness"]["passed"] == 161
    for source in phase_g1_3a["sources"]:
        if source.get("historical_phase_g1_3a_hash"):
            assert source["superseded_by_phase"] in {"ARCH-004G1.3B", "ARCH-004G1.3C"}
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g1_3b = baseline["phase_g1_3b_notification_retry_writer_migration"]
    assert phase_g1_3b["status"] == "THIRD_FAMILY_COMPLETE_G1_CONTINUES"
    assert phase_g1_3b["family"] == {
        "canonical_json_writer": "write_json_atomic",
        "canonical_text_writer": "write_text_atomic",
        "migrated_module_count": 8,
        "removed_private_writer_count": 16,
        "private_writer_remaining_count": 0,
        "direct_writer_before": 877,
        "direct_writer_after": 861,
        "direct_writer_reduction": 16,
    }
    assert phase_g1_3b["parity"]["sort_keys"] is False
    assert phase_g1_3b["parity"]["trailing_newline"] is True
    assert phase_g1_3b["parity"]["artifact_path_schema_status"] == "PASS"
    assert phase_g1_3b["parity"]["workflow_decisions_changed"] is False
    assert phase_g1_3b["validation"]["focused"] == {"status": "PASS", "passed": 139}
    assert phase_g1_3b["validation"]["architecture_fitness"]["passed"] == 162
    for source in phase_g1_3b["sources"]:
        if source.get("historical_phase_g1_3b_hash"):
            assert source["superseded_by_phase"] in {"ARCH-004G1.3C", "ARCH-004G1.3D"}
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g1_3c = baseline["phase_g1_3c_streaming_checksum_helper_migration"]
    assert phase_g1_3c["status"] == "FOURTH_FAMILY_COMPLETE_G1_CONTINUES"
    assert phase_g1_3c["family"] == {
        "canonical_checksum_helper": "sha256_path",
        "default_chunk_size_bytes": 1048576,
        "migrated_module_count": 8,
        "migrated_caller_count": 13,
        "removed_private_checksum_helper_count": 8,
        "private_checksum_helper_remaining_count": 0,
        "direct_writer_before": 861,
        "direct_writer_after": 861,
    }
    assert phase_g1_3c["parity"]["default_cross_chunk_digest"] == "PASS"
    assert phase_g1_3c["parity"]["missing_path_oserror"] == "PASS"
    assert phase_g1_3c["parity"]["workflow_decisions_changed"] is False
    assert phase_g1_3c["validation"]["focused"] == {"status": "PASS", "passed": 155}
    assert phase_g1_3c["validation"]["architecture_fitness"]["passed"] == 164
    for source in phase_g1_3c["sources"]:
        if source.get("historical_phase_g1_3c_hash"):
            assert source["superseded_by_phase"] == "ARCH-004G1.3D"
            assert source["current_hash_tracked_in"] == (
                "phase_g1_3d_pit_replay_runtime_metadata_migration.sources"
            )
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g1_3d = baseline["phase_g1_3d_pit_replay_runtime_metadata_migration"]
    assert phase_g1_3d["status"] == "FIFTH_FAMILY_COMPLETE_G1_CONTINUES"
    assert phase_g1_3d["family"] == {
        "canonical_helper": "with_pit_replay_observe_only_runtime_metadata",
        "canonical_safety_constant": "PIT_REPLAY_OBSERVE_ONLY_SAFETY_FALSE_FIELDS",
        "inventory_file_count": 42,
        "inventory_ast_field_group_count": 14,
        "migrated_module_count": 10,
        "migrated_caller_count": 10,
        "removed_private_metadata_helper_count": 10,
        "private_metadata_helper_remaining_count": 0,
        "safety_false_field_count": 39,
    }
    assert phase_g1_3d["parity"]["field_order"] == "PASS"
    assert phase_g1_3d["parity"]["module_safety_constant_alias"] == "PASS"
    assert phase_g1_3d["parity"]["generic_extra_fields_allowed"] is False
    assert phase_g1_3d["validation"]["focused"] == {"status": "PASS", "passed": 182}
    assert phase_g1_3d["validation"]["architecture_fitness"]["passed"] == 166
    for source in phase_g1_3d["sources"]:
        if source.get("historical_phase_g1_3d_hash"):
            assert source["superseded_by_phase"] == "ARCH-004G1.3E"
            assert source["current_hash_tracked_in"] == (
                "phase_g1_3e_growth_tilt_data_quality_gate_migration.sources"
            )
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g1_3e = baseline["phase_g1_3e_growth_tilt_data_quality_gate_migration"]
    assert phase_g1_3e["status"] == "SIXTH_FAMILY_COMPLETE_G1_COMPLETE_G2_IN_PROGRESS"
    assert phase_g1_3e["family"] == {
        "canonical_helper": "run_growth_tilt_data_quality_gate",
        "inventory_helper_count": 106,
        "inventory_group_count": 51,
        "migrated_module_count": 15,
        "migrated_caller_count": 15,
        "removed_private_gate_helper_count": 15,
        "removed_private_secondary_helper_count": 15,
        "private_gate_helper_remaining_count": 0,
    }
    assert phase_g1_3e["parity"]["direct_validate_data_cache_call"] == "PASS"
    assert phase_g1_3e["parity"]["marketstack_requirement"] == "PASS"
    assert phase_g1_3e["parity"]["exception_downgrade_allowed"] is False
    assert phase_g1_3e["parity"]["fabricated_pass_allowed"] is False
    assert phase_g1_3e["validation"]["focused"] == {"status": "PASS", "passed": 242}
    assert phase_g1_3e["validation"]["architecture_fitness"]["passed"] == 168
    for source in phase_g1_3e["sources"]:
        if source.get("historical_phase_g1_3e_hash"):
            assert source["superseded_by_phase"] in {
                "ARCH-004G2.1",
                "ARCH-004G2.4AQ",
            }
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_1 = baseline["phase_g2_1_etf_cli_contract_baseline"]
    assert phase_g2_1["status"] == "COMPLETE_G2_2_IN_PROGRESS"
    assert phase_g2_1["contract"] == {
        "schema_version": "arch_004g2_cli_contract.v1",
        "root_command_count": 41,
        "group_count": 291,
        "leaf_command_count": 993,
        "registered_leaf_count": 993,
        "unique_path_count": 1284,
        "duplicate_path_count": 0,
        "tree_sha256": "afa0760c82cf347bb135ecb12ae133bc16238fb53e28b7a0cf3c699f6ba1cec2",
        "callback_location_in_contract": False,
        "runtime_behavior_changed": False,
        "production_effect": "none",
    }
    assert phase_g2_1["validation"]["focused"] == {"status": "PASS", "passed": 3}
    assert phase_g2_1["validation"]["architecture_fitness"]["passed"] == 171
    for source in phase_g2_1["sources"]:
        if source.get("historical_phase_g2_1_hash"):
            assert source["superseded_by_phase"] in {
                "ARCH-004G2.2",
                "ARCH-004G2.4CX1",
                "ARCH-004G2_EB0_S2C",
            }
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_2 = baseline["phase_g2_2_etf_cli_registration_shell"]
    assert phase_g2_2["status"] == "COMPLETE_G2_3_IN_PROGRESS"
    assert phase_g2_2["migration"] == {
        "typer_apps_moved": 291,
        "add_typer_relationships_moved": 290,
        "legacy_typer_app_definitions_remaining": 0,
        "legacy_add_typer_relationships_remaining": 0,
        "legacy_root_lines_before": 37604,
        "legacy_root_lines_after": 36045,
        "legacy_root_line_reduction": 1559,
        "top_level_functions_unchanged": 1049,
        "command_decorators_unchanged": 993,
        "tree_sha256": "afa0760c82cf347bb135ecb12ae133bc16238fb53e28b7a0cf3c699f6ba1cec2",
        "node_contracts_equal": True,
        "callback_functions_moved": 0,
        "runtime_behavior_changed": False,
        "production_effect": "none",
    }
    assert phase_g2_2["validation"]["cli_consumer_focused"] == {
        "status": "PASS",
        "passed": 341,
        "file_count": 25,
    }
    assert phase_g2_2["validation"]["architecture_fitness"]["passed"] == 174
    for source in phase_g2_2["sources"]:
        if source.get("historical_phase_g2_2_hash"):
            assert source["superseded_by_phase"] == "ARCH-004G2.3A"
            assert source["current_hash_tracked_in"] == (
                "phase_g2_3a_etf_cli_data_features.sources"
            )
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_3a = baseline["phase_g2_3a_etf_cli_data_features"]
    assert phase_g2_3a["status"] == "COMPLETE_G2_3_CONTINUES"
    assert phase_g2_3a["migration"]["callback_count"] == 3
    assert phase_g2_3a["migration"]["shared_helper_count"] == 3
    assert phase_g2_3a["migration"]["legacy_callback_definitions_remaining"] == 0
    assert phase_g2_3a["migration"]["compatibility_aliases_using_canonical_callbacks"] is True
    assert phase_g2_3a["migration"]["data_quality_behavior_changed"] is False
    assert phase_g2_3a["validation"]["focused"] == {"status": "PASS", "passed": 72}
    assert phase_g2_3a["validation"]["architecture_fitness"]["passed"] == 175
    for source in phase_g2_3a["sources"]:
        if source.get("historical_phase_g2_3a_hash"):
            assert source["superseded_by_phase"] in {"ARCH-004G2.3B", "ARCH-004G2.3G"}
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_3b = baseline["phase_g2_3b_etf_cli_data_quality"]
    assert phase_g2_3b["status"] == "COMPLETE_G2_3_CONTINUES"
    assert phase_g2_3b["migration"]["callback_count"] == 3
    assert phase_g2_3b["migration"]["legacy_callback_definitions_remaining"] == 0
    assert phase_g2_3b["migration"]["direct_dispatch_using_canonical_callbacks"] is True
    assert phase_g2_3b["migration"]["data_quality_behavior_changed"] is False
    assert phase_g2_3b["validation"]["focused"] == {"status": "PASS", "passed": 44}
    assert phase_g2_3b["validation"]["architecture_fitness"]["passed"] == 176
    for source in phase_g2_3b["sources"]:
        if source.get("historical_phase_g2_3b_hash"):
            assert source["superseded_by_phase"] in {"ARCH-004G2.3C", "ARCH-004G2.3H"}
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_3c = baseline["phase_g2_3c_etf_cli_operations"]
    assert phase_g2_3c["status"] == "COMPLETE_G2_3_CONTINUES"
    assert phase_g2_3c["migration"]["callback_count"] == 3
    assert phase_g2_3c["migration"]["shared_parser_count"] == 1
    assert phase_g2_3c["migration"]["directory_constant_count"] == 3
    assert phase_g2_3c["migration"]["legacy_callback_definitions_remaining"] == 0
    assert phase_g2_3c["migration"]["legacy_parser_definitions_remaining"] == 0
    assert phase_g2_3c["migration"]["legacy_directory_constant_definitions_remaining"] == 0
    assert phase_g2_3c["migration"]["direct_dispatch_using_canonical_callbacks"] is True
    assert phase_g2_3c["migration"]["operations_behavior_changed"] is False
    assert phase_g2_3c["validation"]["focused"] == {"status": "PASS", "passed": 111}
    assert phase_g2_3c["validation"]["architecture_fitness"]["passed"] == 177
    for source in phase_g2_3c["sources"]:
        if source.get("historical_phase_g2_3c_hash"):
            assert source["superseded_by_phase"] in {"ARCH-004G2.3D", "ARCH-004G2.4CM"}
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_3d = baseline["phase_g2_3d_etf_cli_evidence_dashboard"]
    assert phase_g2_3d["status"] == "COMPLETE_G2_3_CONTINUES"
    assert phase_g2_3d["migration"]["callback_count"] == 3
    assert phase_g2_3d["migration"]["legacy_callback_definitions_remaining"] == 0
    assert phase_g2_3d["migration"]["legacy_strategy_evidence_imports_remaining"] == 0
    assert phase_g2_3d["migration"]["direct_dispatch_using_canonical_callbacks"] is True
    assert phase_g2_3d["migration"]["direct_writer_calls_after"] == 860
    assert phase_g2_3d["migration"]["reporting_behavior_changed"] is False
    assert phase_g2_3d["validation"]["focused"] == {"status": "PASS", "passed": 44}
    assert phase_g2_3d["validation"]["architecture_fitness"]["passed"] == 178
    for source in phase_g2_3d["sources"]:
        if source.get("historical_phase_g2_3d_hash"):
            assert source["superseded_by_phase"] in {"ARCH-004G2.3E", "ARCH-004G2.4CM"}
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_3e = baseline["phase_g2_3e_etf_cli_weekly_review"]
    assert phase_g2_3e["status"] == "COMPLETE_G2_3_CONTINUES"
    assert phase_g2_3e["migration"]["callback_count"] == 4
    assert phase_g2_3e["migration"]["shared_helper_count"] == 2
    assert phase_g2_3e["migration"]["legacy_callback_definitions_remaining"] == 0
    assert phase_g2_3e["migration"]["legacy_helper_definitions_remaining"] == 0
    assert phase_g2_3e["migration"]["legacy_weekly_review_imports_remaining"] == 0
    assert phase_g2_3e["migration"]["legacy_callers_using_canonical_date_helper"] is True
    assert phase_g2_3e["migration"]["reporting_behavior_changed"] is False
    assert phase_g2_3e["validation"]["focused"] == {"status": "PASS", "passed": 84}
    assert phase_g2_3e["validation"]["architecture_fitness"]["passed"] == 179
    for source in phase_g2_3e["sources"]:
        if source.get("historical_phase_g2_3e_hash"):
            assert source["superseded_by_phase"] == "ARCH-004G2.3F"
            assert source["current_hash_tracked_in"] == (
                "phase_g2_3f_etf_cli_parameter_review.sources"
            )
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_3f = baseline["phase_g2_3f_etf_cli_parameter_review"]
    assert phase_g2_3f["status"] == "COMPLETE_G2_3_CONTINUES"
    assert phase_g2_3f["migration"]["callback_count"] == 4
    assert phase_g2_3f["migration"]["shared_helper_count"] == 1
    assert phase_g2_3f["migration"]["canonical_date_helper_reused"] == "weekly_review_date"
    assert phase_g2_3f["migration"]["legacy_callback_definitions_remaining"] == 0
    assert phase_g2_3f["migration"]["legacy_helper_definitions_remaining"] == 0
    assert phase_g2_3f["migration"]["legacy_parameter_review_imports_remaining"] == 0
    assert phase_g2_3f["migration"]["reporting_behavior_changed"] is False
    assert phase_g2_3f["validation"]["focused"] == {"status": "PASS", "passed": 65}
    assert phase_g2_3f["validation"]["architecture_fitness"]["passed"] == 180
    for source in phase_g2_3f["sources"]:
        if source.get("historical_phase_g2_3f_hash"):
            assert source["superseded_by_phase"] == "ARCH-004G2.3G"
            assert source["current_hash_tracked_in"] == (
                "phase_g2_3g_etf_cli_satellite_attribution.sources"
            )
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_3g = baseline["phase_g2_3g_etf_cli_satellite_attribution"]
    assert phase_g2_3g["status"] == "COMPLETE_G2_3_CONTINUES"
    assert phase_g2_3g["migration"]["callback_count"] == 3
    assert phase_g2_3g["migration"]["shared_helper_count"] == 2
    assert phase_g2_3g["migration"]["legacy_callback_definitions_remaining"] == 0
    assert phase_g2_3g["migration"]["legacy_helper_definitions_remaining"] == 0
    assert str(phase_g2_3g["migration"]["default_ai_regime_start"]) == "2022-12-01"
    assert phase_g2_3g["migration"]["invalid_price_fixture_fail_closed"] is True
    assert phase_g2_3g["migration"]["data_quality_behavior_changed"] is False
    assert phase_g2_3g["migration"]["regime_interpretation_changed"] is False
    assert phase_g2_3g["validation"]["focused"] == {"status": "PASS", "passed": 78}
    assert phase_g2_3g["validation"]["architecture_fitness"]["passed"] == 181
    for source in phase_g2_3g["sources"]:
        if source.get("historical_phase_g2_3g_hash"):
            assert source["superseded_by_phase"] in {"ARCH-004G2.3H", "ARCH-004G2.4A"}
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_3h = baseline["phase_g2_3h_etf_cli_trend_calibration"]
    assert phase_g2_3h["status"] == "COMPLETE_G2_3_CONTINUES"
    assert phase_g2_3h["migration"]["callback_count"] == 3
    assert phase_g2_3h["migration"]["shared_dq_helper_count"] == 4
    assert phase_g2_3h["migration"]["legacy_callback_definitions_remaining"] == 0
    assert phase_g2_3h["migration"]["legacy_dq_helper_definitions_remaining"] == 0
    assert phase_g2_3h["migration"]["dq_gate_precedes_price_and_feature_build"] is True
    assert phase_g2_3h["migration"]["dq_failure_fixture_fail_closed"] is True
    assert phase_g2_3h["migration"]["data_quality_behavior_changed"] is False
    assert phase_g2_3h["migration"]["regime_interpretation_changed"] is False
    assert phase_g2_3h["migration"]["strategy_or_threshold_changed"] is False
    assert phase_g2_3h["validation"]["focused"] == {"status": "PASS", "passed": 54}
    assert phase_g2_3h["validation"]["architecture_fitness"]["passed"] == 182
    for source in phase_g2_3h["sources"]:
        if source.get("historical_phase_g2_3h_hash"):
            assert source["superseded_by_phase"] in {"ARCH-004G2.3_CLOSEOUT", "ARCH-004G2.4A"}
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    closeout = baseline["phase_g2_3_closeout_g2_4_start"]
    assert closeout["status"] == "COMPLETE_G2_4_IN_PROGRESS"
    assert closeout["g2_3_closeout"]["slice_count"] == 8
    assert closeout["g2_3_closeout"]["canonical_module_count"] == 9
    assert closeout["g2_3_closeout"]["migrated_callback_count"] == 26
    assert closeout["g2_3_closeout"]["migrated_helper_count"] == 13
    assert closeout["g2_3_closeout"]["legacy_selected_definitions_remaining"] == 0
    assert closeout["g2_3_closeout"]["legacy_selected_domain_imports_remaining"] == 0
    assert closeout["g2_4_start"]["first_slice"] == "baseline_review"
    assert closeout["g2_4_start"]["implementation_started"] is False
    assert closeout["validation"]["architecture_fitness"]["passed"] == 183
    for source in closeout["sources"]:
        if source.get("historical_phase_g2_3_closeout_hash"):
            assert source["superseded_by_phase"] == "ARCH-004G2.4A"
            assert source["current_hash_tracked_in"] == (
                "phase_g2_4a_etf_cli_baseline_review.sources"
            )
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4a = baseline["phase_g2_4a_etf_cli_baseline_review"]
    assert phase_g2_4a["status"] == "COMPLETE_G2_4_CONTINUES"
    assert phase_g2_4a["migration"]["callback_count"] == 7
    assert phase_g2_4a["migration"]["shared_helper_count"] == 1
    assert phase_g2_4a["migration"]["legacy_callback_definitions_remaining"] == 0
    assert phase_g2_4a["migration"]["legacy_helper_definitions_remaining"] == 0
    assert phase_g2_4a["migration"]["legacy_baseline_review_imports_remaining"] == 0
    assert phase_g2_4a["migration"]["governance_journal_write_allowed"] is True
    assert phase_g2_4a["migration"]["production_runtime_state_mutation_allowed"] is False
    assert phase_g2_4a["migration"]["proposal_is_draft_only"] is True
    assert phase_g2_4a["migration"]["direct_writer_calls_after"] == 858
    assert phase_g2_4a["validation"]["focused"] == {"status": "PASS", "passed": 36}
    assert phase_g2_4a["validation"]["architecture_fitness"]["passed"] == 184
    for source in phase_g2_4a["sources"]:
        if source.get("historical_phase_g2_4a_hash"):
            assert source["superseded_by_phase"] in {"ARCH-004G2.4B", "ARCH-004G2.4C"}
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4b = baseline["phase_g2_4b_etf_cli_shadow_review"]
    assert phase_g2_4b["status"] == "COMPLETE_G2_4_CONTINUES"
    assert phase_g2_4b["migration"]["callback_count"] == 4
    assert phase_g2_4b["migration"]["legacy_callback_definitions_remaining"] == 0
    assert phase_g2_4b["migration"]["legacy_shadow_ready_review_imports_remaining"] == 0
    assert phase_g2_4b["migration"]["candidate_governance_artifact_write_allowed"] is True
    assert phase_g2_4b["migration"]["decision_journal_write_allowed"] is False
    assert phase_g2_4b["migration"]["automatic_paper_shadow_execution_allowed"] is False
    assert phase_g2_4b["migration"]["runtime_registry_mutation_allowed"] is False
    assert phase_g2_4b["migration"]["direct_writer_calls_after"] == 858
    assert phase_g2_4b["validation"]["focused"] == {"status": "PASS", "passed": 21}
    assert phase_g2_4b["validation"]["architecture_fitness"]["passed"] == 185
    for source in phase_g2_4b["sources"]:
        if source.get("historical_phase_g2_4b_hash"):
            assert source["superseded_by_phase"] == "ARCH-004G2.4C"
            assert source["current_hash_tracked_in"] == (
                "phase_g2_4c_etf_cli_dynamic_allocation.sources"
            )
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4c = baseline["phase_g2_4c_etf_cli_dynamic_allocation"]
    assert phase_g2_4c["status"] == "COMPLETE_G2_4_CONTINUES"
    assert phase_g2_4c["migration"]["callback_count"] == 3
    assert phase_g2_4c["migration"]["legacy_callback_definitions_remaining"] == 0
    assert phase_g2_4c["migration"]["legacy_helper_definitions_remaining"] == 0
    assert phase_g2_4c["migration"]["candidate_decision_artifact_write_allowed"] is True
    assert phase_g2_4c["migration"]["runtime_registry_mutation_allowed"] is False
    assert phase_g2_4c["migration"]["official_target_weights_mutation_allowed"] is False
    assert phase_g2_4c["migration"]["production_rebalance_allowed"] is False
    assert phase_g2_4c["migration"]["direct_writer_calls_after"] == 858
    assert phase_g2_4c["validation"]["focused"] == {"status": "PASS", "passed": 21}
    assert phase_g2_4c["validation"]["architecture_fitness"]["passed"] == 186
    for source in phase_g2_4c["sources"]:
        if source.get("historical_phase_g2_4c_hash"):
            assert source["superseded_by_phase"] in {
                "ARCH-004G2.4D",
                "ARCH-004G2.4-EB6",
            }
            expected_current = (
                "phase_g2_4eb6_weight_calibration_and_research_interfaces.sources"
                if source["superseded_by_phase"] == "ARCH-004G2.4-EB6"
                else "phase_g2_4d_etf_cli_dynamic_calibration.sources"
            )
            assert source["current_hash_tracked_in"] == expected_current
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4d = baseline["phase_g2_4d_etf_cli_dynamic_calibration"]
    assert phase_g2_4d["status"] == "COMPLETE_G2_4_CONTINUES"
    assert phase_g2_4d["migration"]["callback_count"] == 3
    assert phase_g2_4d["migration"]["legacy_callback_definitions_remaining"] == 0
    assert phase_g2_4d["migration"]["research_cache_write_allowed"] is True
    assert phase_g2_4d["migration"]["automatic_candidate_promotion_allowed"] is False
    assert phase_g2_4d["migration"]["auto_enrollment_without_owner_approval_allowed"] is False
    assert phase_g2_4d["migration"]["official_target_weights_mutation_allowed"] is False
    assert phase_g2_4d["migration"]["validation_uses_canonical_cli_owner"] is True
    assert phase_g2_4d["migration"]["direct_writer_calls_after"] == 858
    assert phase_g2_4d["validation"]["focused"] == {"status": "PASS", "passed": 24}
    assert phase_g2_4d["validation"]["architecture_fitness"]["passed"] == 187
    for source in phase_g2_4d["sources"]:
        if source.get("historical_phase_g2_4d_hash"):
            assert source["superseded_by_phase"] == "ARCH-004G2.4E"
            assert source["current_hash_tracked_in"] == (
                "phase_g2_4e_etf_cli_dynamic_robustness.sources"
            )
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4e = baseline["phase_g2_4e_etf_cli_dynamic_robustness"]
    assert phase_g2_4e["status"] == "COMPLETE_G2_4_CONTINUES"
    assert phase_g2_4e["migration"]["callback_count"] == 2
    assert phase_g2_4e["migration"]["cached_dq_gate_precedes_standard_price_validation"] is True
    assert phase_g2_4e["migration"]["standard_price_validation_precedes_robustness"] is True
    assert phase_g2_4e["migration"]["dq_failure_fail_closed"] is True
    assert phase_g2_4e["migration"]["latest_mode_read_only"] is True
    assert phase_g2_4e["migration"]["direct_writer_calls_after"] == 858
    assert phase_g2_4e["validation"]["focused"] == {"status": "PASS", "passed": 24}
    assert phase_g2_4e["validation"]["architecture_fitness"]["passed"] == 188
    for source in phase_g2_4e["sources"]:
        if source.get("historical_phase_g2_4e_hash"):
            assert source["superseded_by_phase"] == "ARCH-004G2.4F"
            assert source["current_hash_tracked_in"] == (
                "phase_g2_4f_etf_cli_dynamic_rescue.sources"
            )
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4f = baseline["phase_g2_4f_etf_cli_dynamic_rescue"]
    assert phase_g2_4f["status"] == "COMPLETE_G2_4_CONTINUES"
    assert phase_g2_4f["migration"]["callback_count"] == 3
    assert phase_g2_4f["migration"]["cached_dq_gate_precedes_standard_price_validation"] is True
    assert phase_g2_4f["migration"]["standard_price_validation_precedes_rescue_comparison"] is True
    assert phase_g2_4f["migration"]["dq_failure_fail_closed"] is True
    assert phase_g2_4f["migration"]["bounded_rescue_candidate_artifact_write_allowed"] is True
    assert phase_g2_4f["migration"]["automatic_candidate_enrollment_allowed"] is False
    assert phase_g2_4f["migration"]["owner_approval_executed"] is False
    assert phase_g2_4f["migration"]["automatic_candidate_promotion_allowed"] is False
    assert phase_g2_4f["migration"]["official_target_weights_mutation_allowed"] is False
    assert phase_g2_4f["migration"]["direct_writer_calls_after"] == 858
    assert phase_g2_4f["validation"]["focused"] == {"status": "PASS", "passed": 25}
    assert phase_g2_4f["validation"]["architecture_fitness"]["passed"] == 189
    for source in phase_g2_4f["sources"]:
        if source.get("historical_phase_g2_4f_hash"):
            assert source["superseded_by_phase"] == "ARCH-004G2.4G"
            assert source["current_hash_tracked_in"] == (
                "phase_g2_4g_etf_cli_dynamic_v2_review.sources"
            )
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4g = baseline["phase_g2_4g_etf_cli_dynamic_v2_review"]
    assert phase_g2_4g["status"] == "COMPLETE_G2_4_CONTINUES"
    assert phase_g2_4g["migration"]["callback_count"] == 3
    assert phase_g2_4g["migration"]["market_backtest_reexecution_allowed"] is False
    assert phase_g2_4g["migration"]["mandatory_source_missing_fail_closed"] is True
    assert phase_g2_4g["migration"]["optional_shadow_missing_is_warning"] is True
    assert phase_g2_4g["migration"]["latest_report_mode_read_only"] is True
    assert phase_g2_4g["migration"]["shadow_enrollment_allowed"] is False
    assert phase_g2_4g["migration"]["owner_approval_executed"] is False
    assert phase_g2_4g["migration"]["direct_writer_calls_after"] == 858
    assert phase_g2_4g["validation"]["focused"] == {"status": "PASS", "passed": 27}
    assert phase_g2_4g["validation"]["architecture_fitness"]["passed"] == 190
    for source in phase_g2_4g["sources"]:
        if source.get("historical_phase_g2_4g_hash"):
            assert source["superseded_by_phase"] == "ARCH-004G2.4H"
            assert source["current_hash_tracked_in"] == (
                "phase_g2_4h_etf_cli_dynamic_v3_rescue_base.sources"
            )
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4h = baseline["phase_g2_4h_etf_cli_dynamic_v3_rescue_base"]
    assert phase_g2_4h["status"] == "COMPLETE_G2_4_CONTINUES"
    assert phase_g2_4h["migration"]["callback_count"] == 3
    assert phase_g2_4h["migration"]["remaining_dynamic_v3_commands_stay_legacy_owned"] is True
    assert phase_g2_4h["migration"]["v0_4_review_package_read_only"] is True
    assert phase_g2_4h["migration"]["base_candidate_must_match_reviewed_policy"] is True
    assert phase_g2_4h["migration"]["latest_report_mode_read_only"] is True
    assert phase_g2_4h["migration"]["shadow_enrollment_allowed"] is False
    assert phase_g2_4h["migration"]["owner_approval_executed"] is False
    assert phase_g2_4h["migration"]["direct_writer_calls_after"] == 858
    assert phase_g2_4h["validation"]["focused"] == {"status": "PASS", "passed": 28}
    assert phase_g2_4h["validation"]["architecture_fitness"]["passed"] == 191
    for source in phase_g2_4h["sources"]:
        if source.get("historical_phase_g2_4h_hash"):
            assert source["superseded_by_phase"] in {"ARCH-004G2.4I", "ARCH-004G2.4BG"}
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4i = baseline["phase_g2_4i_etf_cli_dynamic_v3_real_evaluation"]
    assert phase_g2_4i["status"] == "COMPLETE_G2_4_CONTINUES"
    assert phase_g2_4i["migration"]["callback_count"] == 3
    assert phase_g2_4i["migration"]["cached_dq_gate_precedes_standard_price_validation"] is True
    assert phase_g2_4i["migration"]["standard_price_validation_precedes_pit_evaluation"] is True
    assert phase_g2_4i["migration"]["dq_failure_fail_closed"] is True
    assert phase_g2_4i["migration"]["requested_range_and_ai_regime_separate"] is True
    assert phase_g2_4i["migration"]["pre_regime_primary_conclusion_allowed"] is False
    assert phase_g2_4i["migration"]["promotion_gate_executes_promotion"] is False
    assert phase_g2_4i["migration"]["direct_writer_calls_after"] == 858
    assert phase_g2_4i["validation"]["focused"] == {"status": "PASS", "passed": 28}
    assert phase_g2_4i["validation"]["architecture_fitness"]["passed"] == 192
    for source in phase_g2_4i["sources"]:
        if source.get("historical_phase_g2_4i_hash"):
            assert source["superseded_by_phase"] == "ARCH-004G2.4J"
            assert source["current_hash_tracked_in"] == (
                "phase_g2_4j_etf_cli_dynamic_v3_failure_attribution.sources"
            )
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4j = baseline["phase_g2_4j_etf_cli_dynamic_v3_failure_attribution"]
    assert phase_g2_4j["status"] == "COMPLETE_G2_4_CONTINUES"
    assert phase_g2_4j["migration"]["callback_count"] == 3
    assert phase_g2_4j["migration"]["real_evaluation_lineage_loaded_before_dq"] is True
    assert phase_g2_4j["migration"]["dq_as_of_inherits_explicit_or_source_end"] is True
    assert phase_g2_4j["migration"]["cached_dq_gate_precedes_standard_price_validation"] is True
    assert phase_g2_4j["migration"]["standard_price_validation_precedes_pit_attribution"] is True
    assert phase_g2_4j["migration"]["source_artifact_mutation_allowed"] is False
    assert phase_g2_4j["migration"]["review_or_recommendation_executes_promotion"] is False
    assert phase_g2_4j["migration"]["direct_writer_calls_after"] == 858
    assert phase_g2_4j["validation"]["focused"] == {"status": "PASS", "passed": 28}
    assert phase_g2_4j["validation"]["architecture_fitness"]["passed"] == 193
    for source in phase_g2_4j["sources"]:
        if source.get("historical_phase_g2_4j_hash"):
            assert source["superseded_by_phase"] == "ARCH-004G2.4K"
            assert source["current_hash_tracked_in"] == (
                "phase_g2_4k_etf_cli_dynamic_v3_sweep_config.sources"
            )
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4k = baseline["phase_g2_4k_etf_cli_dynamic_v3_sweep_config"]
    assert phase_g2_4k["status"] == "COMPLETE_G2_4_CONTINUES"
    assert phase_g2_4k["migration"]["callback_count"] == 2
    assert phase_g2_4k["migration"]["reviewed_config_read_only"] is True
    assert phase_g2_4k["migration"]["stable_candidate_id_enumeration"] is True
    assert phase_g2_4k["migration"]["evaluator_execution_allowed"] is False
    assert phase_g2_4k["migration"]["runtime_artifact_write_allowed"] is False
    assert phase_g2_4k["migration"]["production_candidate_generated"] is False
    assert phase_g2_4k["migration"]["preview_limit_changes_candidate_universe"] is False
    assert phase_g2_4k["migration"]["direct_writer_calls_after"] == 858
    assert phase_g2_4k["validation"]["focused"] == {"status": "PASS", "passed": 43}
    assert phase_g2_4k["validation"]["architecture_fitness"]["passed"] == 194
    for source in phase_g2_4k["sources"]:
        if source.get("historical_phase_g2_4k_hash"):
            assert source["superseded_by_phase"] in {"ARCH-004G2.4L", "ARCH-004G2.4O"}
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4l = baseline["phase_g2_4l_etf_cli_dynamic_v3_sweep_runtime"]
    assert phase_g2_4l["status"] == "COMPLETE_G2_4_CONTINUES"
    assert phase_g2_4l["migration"]["callback_count"] == 8
    assert phase_g2_4l["migration"]["helper_count"] == 1
    assert phase_g2_4l["migration"]["real_evaluator_uses_dq_and_pit_path"] is True
    assert phase_g2_4l["migration"]["tiny_fixture_not_for_investment_decision"] is True
    assert phase_g2_4l["migration"]["resume_evaluator_mode_mutation_allowed"] is False
    assert phase_g2_4l["migration"]["resume_worker_override_recorded"] is True
    assert phase_g2_4l["migration"]["production_candidate_generated"] is False
    assert phase_g2_4l["migration"]["direct_writer_calls_after"] == 858
    assert phase_g2_4l["validation"]["focused"] == {"status": "PASS", "passed": 44}
    assert phase_g2_4l["validation"]["architecture_fitness"]["passed"] == 195
    for source in phase_g2_4l["sources"]:
        if source.get("historical_phase_g2_4l_hash"):
            assert source["superseded_by_phase"] in {"ARCH-004G2.4M", "ARCH-004G2.4O"}
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4m = baseline["phase_g2_4m_etf_cli_dynamic_v3_data_audit"]
    assert phase_g2_4m["status"] == "COMPLETE_G2_4_CONTINUES"
    assert phase_g2_4m["migration"]["callback_count"] == 3
    assert phase_g2_4m["migration"]["same_validate_data_path_used"] is True
    assert phase_g2_4m["migration"]["failed_quality_evidence_write_allowed"] is True
    assert phase_g2_4m["migration"]["dq_failure_may_be_reported_as_pass"] is False
    assert phase_g2_4m["migration"]["checksum_and_provenance_artifacts_required"] is True
    assert phase_g2_4m["migration"]["validation_requires_dq_non_fail"] is True
    assert phase_g2_4m["migration"]["cache_or_download_manifest_mutation_allowed"] is False
    assert phase_g2_4m["migration"]["direct_writer_calls_after"] == 858
    assert phase_g2_4m["validation"]["focused"] == {"status": "PASS", "passed": 45}
    assert phase_g2_4m["validation"]["architecture_fitness"]["passed"] == 196
    for source in phase_g2_4m["sources"]:
        if source.get("historical_phase_g2_4m_hash"):
            assert source["superseded_by_phase"] in {"ARCH-004G2.4N", "ARCH-004G2.4O"}
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4n = baseline["phase_g2_4n_etf_cli_dynamic_v3_data_provenance"]
    assert phase_g2_4n["status"] == "COMPLETE_G2_4_CONTINUES"
    assert phase_g2_4n["migration"]["callback_count"] == 3
    assert phase_g2_4n["migration"]["inspect_and_validate_read_only"] is True
    assert phase_g2_4n["migration"]["supported_repair_mode"] == "reconstruct-from-cache"
    assert phase_g2_4n["migration"]["repair_requires_all_cache_files"] is True
    assert phase_g2_4n["migration"]["original_download_event_unavailable_disclosed"] is True
    assert phase_g2_4n["migration"]["provider_or_endpoint_invention_allowed"] is False
    assert phase_g2_4n["migration"]["direct_writer_calls_after"] == 858
    assert phase_g2_4n["validation"]["focused"] == {"status": "PASS", "passed": 46}
    assert phase_g2_4n["validation"]["architecture_fitness"]["passed"] == 197
    for source in phase_g2_4n["sources"]:
        if source.get("historical_phase_g2_4n_hash"):
            assert source["superseded_by_phase"] == "ARCH-004G2.4O"
            assert source["current_hash_tracked_in"] == (
                "phase_g2_4o_etf_cli_dynamic_v3_window_audit.sources"
            )
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4o = baseline["phase_g2_4o_etf_cli_dynamic_v3_window_audit"]
    assert phase_g2_4o["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    assert phase_g2_4o["migration"]["callback_count"] == 4
    assert phase_g2_4o["migration"]["as_of_option_semantics"] == "requested_start"
    assert phase_g2_4o["migration"]["configured_requested_actual_ranges_distinct"] is True
    assert str(phase_g2_4o["migration"]["ai_regime_default_start"]) == "2022-12-01"
    assert phase_g2_4o["migration"]["pre_regime_actual_range_inherently_invalid"] is False
    assert phase_g2_4o["migration"]["research_window_role_automatically_validated"] is False
    assert phase_g2_4o["migration"]["missing_or_invalid_range_fails_closed"] is True
    assert phase_g2_4o["migration"]["late_start_or_early_end_blocks_promotion"] is True
    assert phase_g2_4o["migration"]["report_and_inspect_read_only"] is True
    assert phase_g2_4o["migration"]["candidate_or_backtest_execution_allowed"] is False
    assert phase_g2_4o["migration"]["direct_writer_calls_after"] == 858
    assert phase_g2_4o["validation"]["focused"] == {"status": "PASS", "passed": 48}
    assert phase_g2_4o["validation"]["architecture_fitness"]["passed"] == 198
    for source in phase_g2_4o["sources"]:
        if source.get("historical_phase_g2_4o_hash"):
            assert source["superseded_by_phase"] == "ARCH-004G2.4P"
            assert source["current_hash_tracked_in"] == (
                "phase_g2_4p_etf_cli_dynamic_v3_injection_audit.sources"
            )
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4p = baseline["phase_g2_4p_etf_cli_dynamic_v3_injection_audit"]
    assert phase_g2_4p["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    assert phase_g2_4p["migration"]["callback_count"] == 3
    assert phase_g2_4p["migration"]["deterministic_base_plus_ofat_pairs"] is True
    assert phase_g2_4p["migration"]["grid_prefix_may_prove_parameter_effect"] is False
    assert phase_g2_4p["migration"]["parameter_effect_uses_matched_pairs_only"] is True
    assert phase_g2_4p["migration"]["declared_mapping_alone_proves_consumption"] is False
    assert phase_g2_4p["migration"]["independent_parameter_effect_artifact_required"] is True
    assert phase_g2_4p["migration"]["insufficient_pair_status"] == (
        "INSUFFICIENT_MATCHED_PAIR_EVIDENCE"
    )
    assert phase_g2_4p["migration"]["validation_fails_on_incomplete_pair_coverage"] is True
    assert phase_g2_4p["migration"]["real_evaluation_uses_dq_and_pit_context"] is True
    assert phase_g2_4p["migration"]["latest_report_mode_read_only"] is True
    assert phase_g2_4p["migration"]["automatic_candidate_promotion_allowed"] is False
    assert phase_g2_4p["migration"]["direct_writer_calls_after"] == 858
    assert phase_g2_4p["validation"]["focused"] == {"status": "PASS", "passed": 53}
    assert phase_g2_4p["validation"]["architecture_fitness"]["passed"] == 199
    superseded_g2_4p = set(phase_g2_4p["superseded_source_paths"])
    for source in phase_g2_4p["sources"]:
        if source["path"] in superseded_g2_4p:
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4q = baseline["phase_g2_4q_etf_cli_dynamic_v3_weight_path"]
    assert phase_g2_4q["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    assert phase_g2_4q["migration"]["callback_count"] == 2
    assert phase_g2_4q["migration"]["shared_read_only_content_inspection"] is True
    assert phase_g2_4q["migration"]["metadata_declaration_alone_proves_completeness"] is False
    assert phase_g2_4q["migration"]["unique_evaluation_directory_required"] is True
    assert phase_g2_4q["migration"]["daily_weight_sum_validation_required"] is True
    assert phase_g2_4q["migration"]["metadata_content_parity_required"] is True
    assert phase_g2_4q["migration"]["event_and_turnover_content_validation_required"] is True
    assert phase_g2_4q["migration"]["invalid_core_observed_status"] == "INCOMPLETE"
    assert phase_g2_4q["migration"]["valid_minimal_observed_status"] == "PARTIAL"
    assert phase_g2_4q["migration"]["complete_requires_no_missing_fields"] is True
    assert phase_g2_4q["migration"]["complete_requires_parseable_detail_fields"] is True
    assert phase_g2_4q["migration"]["declared_observed_mismatch_fails_validation"] is True
    assert phase_g2_4q["migration"]["source_artifact_mutation_allowed"] is False
    assert phase_g2_4q["migration"]["automatic_candidate_promotion_allowed"] is False
    assert phase_g2_4q["migration"]["direct_writer_calls_after"] == 858
    assert phase_g2_4q["validation"]["focused"] == {"status": "PASS", "passed": 59}
    assert phase_g2_4q["validation"]["architecture_fitness"]["passed"] == 200
    superseded_g2_4q = set(phase_g2_4q["superseded_source_paths"])
    for source in phase_g2_4q["sources"]:
        if source["path"] in superseded_g2_4q:
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4r = baseline["phase_g2_4r_etf_cli_dynamic_v3_candidate_evidence"]
    assert phase_g2_4r["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    assert phase_g2_4r["migration"]["callback_count"] == 3
    assert phase_g2_4r["migration"]["explicit_candidate_report_prerequisite"] is True
    assert phase_g2_4r["migration"]["attribution_source_mutation_allowed"] is False
    assert phase_g2_4r["migration"]["candidate_results_and_report_checksums_required"] is True
    assert phase_g2_4r["migration"]["real_artifact_sweep_candidate_ownership_required"] is True
    assert phase_g2_4r["migration"]["observed_weight_path_completeness_required"] is True
    assert phase_g2_4r["migration"]["weight_delta_recomputed_from_daily_paths"] is True
    assert phase_g2_4r["migration"]["current_weight_reference"] == "static_base_candidate"
    assert phase_g2_4r["migration"]["dynamic_v0_4_weights_may_be_inferred_from_summary"] is False
    assert phase_g2_4r["migration"]["current_attribution_method"] == "path_and_aggregate_v2"
    assert phase_g2_4r["migration"]["complete_attribution_allowed"] is False
    assert (
        phase_g2_4r["migration"]["validation_recomputes_lineage_delta_status_and_checksums"] is True
    )
    assert phase_g2_4r["migration"]["sweep_or_real_evaluation_execution_allowed"] is False
    assert phase_g2_4r["migration"]["automatic_candidate_promotion_allowed"] is False
    assert phase_g2_4r["migration"]["direct_writer_calls_after"] == 858
    assert phase_g2_4r["validation"]["focused"] == {"status": "PASS", "passed": 62}
    assert phase_g2_4r["validation"]["architecture_fitness"]["passed"] == 201
    superseded_g2_4r = set(phase_g2_4r["superseded_source_paths"])
    for source in phase_g2_4r["sources"]:
        if source["path"] in superseded_g2_4r:
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4s = baseline["phase_g2_4s_etf_cli_dynamic_v3_validation_evidence"]
    assert phase_g2_4s["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4s = phase_g2_4s["migration"]
    assert migration_g2_4s["callback_count"] == 6
    assert migration_g2_4s["full_period_metrics_used_as_window_evidence"] is False
    assert migration_g2_4s["stable_hash_used_as_window_evidence"] is False
    assert migration_g2_4s["global_gate_pre_filters_train_candidates"] is False
    assert migration_g2_4s["rejected_or_unscored_train_candidate_may_be_selected"] is False
    assert migration_g2_4s["real_daily_path_window_recomputation_required"] is True
    assert migration_g2_4s["profile_config_source_lineage_checks_required"] is True
    assert migration_g2_4s["source_and_output_checksums_required"] is True
    assert migration_g2_4s["tiny_fixture_true_walk_forward_pass_allowed"] is False
    assert migration_g2_4s["partial_walk_forward_pass_allowed"] is False
    assert migration_g2_4s["current_walk_forward_status"] == "INCOMPLETE"
    assert migration_g2_4s["current_walk_forward_no_eligible_window_count"] == 2
    assert migration_g2_4s["current_overfit_method"] == "path_and_aggregate_overfit_v2"
    assert migration_g2_4s["partial_or_proxy_low_risk_allowed"] is False
    assert migration_g2_4s["content_recomputation_validation_required"] is True
    assert migration_g2_4s["source_sweep_execution_allowed"] is False
    assert migration_g2_4s["automatic_candidate_promotion_allowed"] is False
    assert migration_g2_4s["automatic_shadow_enrollment_allowed"] is False
    assert migration_g2_4s["direct_writer_calls_after"] == 858
    assert migration_g2_4s["python_module_count"] == 830
    assert phase_g2_4s["real_smoke"]["walk_forward_validation"] == "PASS"
    assert phase_g2_4s["real_smoke"]["selected_candidate_count"] == 0
    assert phase_g2_4s["real_smoke"]["superseded_intermediate_validation"] == "FAIL"
    assert phase_g2_4s["real_smoke"]["overfit_validation"] == "PASS"
    assert phase_g2_4s["validation"]["focused"] == {"status": "PASS", "passed": 65}
    assert phase_g2_4s["validation"]["architecture_fitness"]["passed"] == 202
    assert phase_g2_4s["sources"]
    superseded_g2_4s = set(phase_g2_4s["superseded_source_paths"])
    for source in phase_g2_4s["sources"]:
        if source["path"] in superseded_g2_4s:
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4t = baseline["phase_g2_4t_etf_cli_dynamic_v3_legacy_validation"]
    assert phase_g2_4t["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4t = phase_g2_4t["migration"]
    assert migration_g2_4t["callback_count"] == 6
    assert migration_g2_4t["legacy_full_period_metrics_used_as_window_evidence"] is False
    assert migration_g2_4t["legacy_stable_hash_used_as_window_evidence"] is False
    assert migration_g2_4t["shared_real_daily_path_window_recomputation_required"] is True
    assert migration_g2_4t["tiny_fixture_evidence_completeness"] == "PROXY_ONLY"
    assert migration_g2_4t["tiny_fixture_walk_forward_pass_allowed"] is False
    assert migration_g2_4t["source_and_output_checksums_required"] is True
    assert migration_g2_4t["content_recomputation_validation_required"] is True
    assert migration_g2_4t["real_neighbor_path_report_identity_required"] is True
    assert migration_g2_4t["aggregate_stress_may_be_dedicated_bucket_pass"] is False
    assert migration_g2_4t["regime_observation_may_be_stability_pass"] is False
    assert migration_g2_4t["old_manifest_complete_shadow_basis_allowed"] is False
    assert migration_g2_4t["validator_automatic_registry_repair_allowed"] is False
    assert migration_g2_4t["automatic_candidate_promotion_allowed"] is False
    assert migration_g2_4t["automatic_shadow_enrollment_allowed"] is False
    assert migration_g2_4t["direct_writer_calls_after"] == 858
    assert migration_g2_4t["legacy_root_lines_after"] == 30762
    assert migration_g2_4t["legacy_root_top_level_functions_after"] == 932
    assert migration_g2_4t["legacy_root_command_decorators_after"] == 893
    assert migration_g2_4t["parameter_research_g6_decomposition_debt_recorded"] is True
    assert migration_g2_4t["python_module_count"] == 831
    assert phase_g2_4t["real_smoke"]["walk_forward_validation"] == "PASS"
    assert phase_g2_4t["real_smoke"]["walk_forward_result_row_count"] == 40
    assert phase_g2_4t["real_smoke"]["robustness_validation"] == "PASS"
    assert phase_g2_4t["real_smoke"]["superseded_walk_forward_validation"] == "FAIL"
    assert phase_g2_4t["real_smoke"]["superseded_robustness_validation"] == "FAIL"
    assert phase_g2_4t["real_smoke"]["current_shadow_registry_validation"] == "FAIL"
    assert phase_g2_4t["real_smoke"]["automatic_registry_mutation_performed"] is False
    assert phase_g2_4t["sources"]
    superseded_g2_4t = set(phase_g2_4t["superseded_source_paths"])
    for source in phase_g2_4t["sources"]:
        if source["path"] in superseded_g2_4t:
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4u = baseline["phase_g2_4u_etf_cli_dynamic_v3_shadow_registry"]
    assert phase_g2_4u["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4u = phase_g2_4u["migration"]
    assert migration_g2_4u["callback_count"] == 4
    assert migration_g2_4u["implicit_mtime_evidence_selection_allowed"] is False
    assert migration_g2_4u["explicit_evidence_ids_must_be_paired"] is True
    assert migration_g2_4u["content_recomputation_validation_required"] is True
    assert migration_g2_4u["candidate_and_sweep_ownership_required"] is True
    assert migration_g2_4u["promotion_latest_fallback_allowed"] is False
    assert migration_g2_4u["validator_automatic_registry_repair_allowed"] is False
    assert migration_g2_4u["automatic_candidate_promotion_allowed"] is False
    assert migration_g2_4u["automatic_shadow_enrollment_allowed"] is False
    assert migration_g2_4u["legacy_root_lines_after"] == 30628
    assert migration_g2_4u["legacy_root_top_level_functions_after"] == 928
    assert migration_g2_4u["legacy_root_command_decorators_after"] == 889
    assert migration_g2_4u["python_module_count"] == 832
    assert phase_g2_4u["real_smoke"]["temporary_explicit_registration"] == "PASS"
    assert phase_g2_4u["real_smoke"]["current_shadow_registry_validation"] == "FAIL"
    assert phase_g2_4u["real_smoke"]["automatic_registry_mutation_performed"] is False
    assert phase_g2_4u["sources"]
    superseded_g2_4u = set(phase_g2_4u["superseded_source_paths"])
    for source in phase_g2_4u["sources"]:
        if source["path"] in superseded_g2_4u:
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4v = baseline["phase_g2_4v_etf_cli_dynamic_v3_research_control"]
    assert phase_g2_4v["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4v = phase_g2_4v["migration"]
    assert migration_g2_4v["callback_count"] == 11
    assert migration_g2_4v["governance_diff_read_only"] is True
    assert migration_g2_4v["research_query_compare_history_read_only"] is True
    assert migration_g2_4v["research_index_rebuild_only"] is True
    assert migration_g2_4v["artifact_latest_validate_stale_read_only"] is True
    assert migration_g2_4v["repair_latest_is_only_pointer_writer"] is True
    assert migration_g2_4v["repair_latest_canonical_root_only"] is True
    assert migration_g2_4v["source_artifact_mutation_allowed"] is False
    assert migration_g2_4v["research_or_candidate_execution_allowed"] is False
    assert migration_g2_4v["automatic_candidate_promotion_allowed"] is False
    assert migration_g2_4v["legacy_root_lines_after"] == 30391
    assert migration_g2_4v["legacy_root_top_level_functions_after"] == 917
    assert migration_g2_4v["legacy_root_command_decorators_after"] == 878
    assert migration_g2_4v["python_module_count"] == 833
    assert phase_g2_4v["sources"]
    superseded_g2_4v = set(phase_g2_4v["superseded_source_paths"])
    for source in phase_g2_4v["sources"]:
        if source["path"] in superseded_g2_4v:
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4w = baseline["phase_g2_4w_etf_cli_dynamic_v3_observation_lifecycle"]
    assert phase_g2_4w["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4w = phase_g2_4w["migration"]
    assert migration_g2_4w["callback_count"] == 7
    assert migration_g2_4w["shadow_monitor_observe_only"] is True
    assert migration_g2_4w["scheduled_observe_lightweight_gate_only"] is True
    assert migration_g2_4w["scheduled_observe_research_execution_allowed"] is False
    assert migration_g2_4w["promotion_pack_manual_review_only"] is True
    assert migration_g2_4w["promotion_pack_pass_means_production_approval"] is False
    assert migration_g2_4w["automatic_candidate_promotion_allowed"] is False
    assert migration_g2_4w["automatic_shadow_enrollment_allowed"] is False
    assert migration_g2_4w["official_weight_or_broker_mutation_allowed"] is False
    assert migration_g2_4w["legacy_root_lines_after"] == 30163
    assert migration_g2_4w["legacy_root_top_level_functions_after"] == 910
    assert migration_g2_4w["legacy_root_command_decorators_after"] == 871
    assert migration_g2_4w["python_module_count"] == 834
    assert phase_g2_4w["sources"]
    superseded_g2_4w = set(phase_g2_4w["superseded_source_paths"])
    for source in phase_g2_4w["sources"]:
        if source["path"] in superseded_g2_4w:
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4x = baseline["phase_g2_4x_etf_cli_dynamic_v3_evidence_readiness"]
    assert phase_g2_4x["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4x = phase_g2_4x["migration"]
    assert migration_g2_4x["callback_count"] == 17
    assert migration_g2_4x["evidence_summary_is_promotion"] is False
    assert migration_g2_4x["medium_real_report_runs_sweep"] is False
    assert migration_g2_4x["regime_coverage_requires_price_input"] is True
    assert migration_g2_4x["interpretation_is_manual_review_material"] is True
    assert migration_g2_4x["observe_pool_writes_shadow_registry"] is False
    assert migration_g2_4x["overnight_readiness_runs_overnight_sweep"] is False
    assert migration_g2_4x["ready_or_usable_means_production_ready"] is False
    assert migration_g2_4x["automatic_candidate_promotion_allowed"] is False
    assert migration_g2_4x["official_weight_or_broker_mutation_allowed"] is False
    assert migration_g2_4x["legacy_root_lines_after"] == 29703
    assert migration_g2_4x["legacy_root_top_level_functions_after"] == 893
    assert migration_g2_4x["legacy_root_command_decorators_after"] == 854
    assert migration_g2_4x["python_module_count"] == 835
    assert phase_g2_4x["sources"]
    superseded_g2_4x = set(phase_g2_4x["superseded_source_paths"])
    for source in phase_g2_4x["sources"]:
        if source["path"] in superseded_g2_4x:
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4y = baseline["phase_g2_4y_etf_cli_dynamic_v3_evidence_governance"]
    assert phase_g2_4y["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4y = phase_g2_4y["migration"]
    assert migration_g2_4y["callback_count"] == 19
    assert migration_g2_4y["gate_impact_simulation_only"] is True
    assert migration_g2_4y["policy_apply_requires_reviewed_policy"] is True
    assert migration_g2_4y["hard_blocker_downgrade_allowed"] is False
    assert migration_g2_4y["candidate_recovery_observe_only"] is True
    assert migration_g2_4y["observe_pool_rebuild_writes_shadow_registry"] is False
    assert migration_g2_4y["research_decision_executes_promotion"] is False
    assert migration_g2_4y["automatic_candidate_promotion_allowed"] is False
    assert migration_g2_4y["official_weight_or_broker_mutation_allowed"] is False
    assert migration_g2_4y["legacy_root_lines_after"] == 29165
    assert migration_g2_4y["legacy_root_top_level_functions_after"] == 874
    assert migration_g2_4y["legacy_root_command_decorators_after"] == 835
    assert migration_g2_4y["python_module_count"] == 836
    assert phase_g2_4y["sources"]
    superseded_g2_4y = set(phase_g2_4y["superseded_source_paths"])
    for source in phase_g2_4y["sources"]:
        if source["path"] in superseded_g2_4y:
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4z = baseline["phase_g2_4z_etf_cli_dynamic_v3_candidate_observation"]
    assert phase_g2_4z["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4z = phase_g2_4z["migration"]
    assert migration_g2_4z["callback_count"] == 13
    assert migration_g2_4z["shortlist_is_manual_review_set"] is True
    assert migration_g2_4z["cluster_is_similarity_diagnostic"] is True
    assert migration_g2_4z["shadow_shortlist_writes_legacy_registry"] is False
    assert migration_g2_4z["monitoring_activation_is_enrollment"] is False
    assert migration_g2_4z["monitoring_active_means_position_advisory"] is False
    assert migration_g2_4z["automatic_candidate_promotion_allowed"] is False
    assert migration_g2_4z["official_weight_or_broker_mutation_allowed"] is False
    assert migration_g2_4z["legacy_root_lines_after"] == 28802
    assert migration_g2_4z["legacy_root_top_level_functions_after"] == 861
    assert migration_g2_4z["legacy_root_command_decorators_after"] == 822
    assert migration_g2_4z["python_module_count"] == 837
    assert phase_g2_4z["sources"]
    superseded_g2_4z = set(phase_g2_4z["superseded_source_paths"])
    for source in phase_g2_4z["sources"]:
        if source["path"] in superseded_g2_4z:
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4aa = baseline["phase_g2_4aa_etf_cli_dynamic_v3_portfolio_intake"]
    assert phase_g2_4aa["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4aa = phase_g2_4aa["migration"]
    assert migration_g2_4aa["callback_count"] == 7
    assert migration_g2_4aa["snapshot_source_must_be_explicit"] is True
    assert migration_g2_4aa["normalization_infers_missing_positions"] is False
    assert migration_g2_4aa["intake_triggers_downstream_risk_chain"] is False
    assert migration_g2_4aa["normalized_pass_means_portfolio_approval"] is False
    assert migration_g2_4aa["official_weight_or_broker_mutation_allowed"] is False
    assert migration_g2_4aa["legacy_root_lines_after"] == 28604
    assert migration_g2_4aa["legacy_root_top_level_functions_after"] == 854
    assert migration_g2_4aa["legacy_root_command_decorators_after"] == 815
    assert migration_g2_4aa["python_module_count"] == 838
    assert phase_g2_4aa["sources"]
    superseded_g2_4aa = set(phase_g2_4aa["superseded_source_paths"])
    for source in phase_g2_4aa["sources"]:
        if source["path"] in superseded_g2_4aa:
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4ab = baseline["phase_g2_4ab_etf_cli_dynamic_v3_portfolio_risk_controls"]
    assert phase_g2_4ab["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4ab = phase_g2_4ab["migration"]
    assert migration_g2_4ab["callback_count"] == 9
    assert migration_g2_4ab["requires_explicit_upstream_artifacts"] is True
    assert migration_g2_4ab["runs_portfolio_intake"] is False
    assert migration_g2_4ab["builds_manual_execution_review"] is False
    assert migration_g2_4ab["recommended_action_is_execution_authorization"] is False
    assert migration_g2_4ab["source_or_policy_mutation_allowed"] is False
    assert migration_g2_4ab["official_weight_or_broker_mutation_allowed"] is False
    assert migration_g2_4ab["legacy_root_lines_after"] == 28335
    assert migration_g2_4ab["legacy_root_top_level_functions_after"] == 845
    assert migration_g2_4ab["legacy_root_command_decorators_after"] == 806
    assert migration_g2_4ab["python_module_count"] == 839
    assert phase_g2_4ab["sources"]
    superseded_g2_4ab = set(phase_g2_4ab["superseded_source_paths"])
    for source in phase_g2_4ab["sources"]:
        if source["path"] in superseded_g2_4ab:
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4ac = baseline["phase_g2_4ac_etf_cli_dynamic_v3_manual_execution_review"]
    assert phase_g2_4ac["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4ac = phase_g2_4ac["migration"]
    assert migration_g2_4ac["callback_count"] == 3
    assert migration_g2_4ac["requires_explicit_source_ids"] is True
    assert migration_g2_4ac["manual_execution_decision_is_owner_record"] is False
    assert migration_g2_4ac["runs_upstream_risk_controls"] is False
    assert migration_g2_4ac["records_owner_approval"] is False
    assert migration_g2_4ac["order_ticket_generation_allowed"] is False
    assert migration_g2_4ac["official_weight_or_broker_mutation_allowed"] is False
    assert migration_g2_4ac["legacy_root_lines_after"] == 28226
    assert migration_g2_4ac["legacy_root_top_level_functions_after"] == 842
    assert migration_g2_4ac["legacy_root_command_decorators_after"] == 803
    assert migration_g2_4ac["python_module_count"] == 840
    assert phase_g2_4ac["sources"]
    superseded_g2_4ac = set(phase_g2_4ac["superseded_source_paths"])
    for source in phase_g2_4ac["sources"]:
        if source["path"] in superseded_g2_4ac:
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4ad = baseline["phase_g2_4ad_etf_cli_dynamic_v3_real_snapshot_intake"]
    assert phase_g2_4ad["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4ad = phase_g2_4ad["migration"]
    assert migration_g2_4ad["callback_count"] == 5
    assert migration_g2_4ad["redaction_required_before_intake"] is True
    assert migration_g2_4ad["sensitive_broker_or_account_fields_allowed"] is False
    assert migration_g2_4ad["manual_snapshot_link_is_broker_sync"] is False
    assert migration_g2_4ad["runs_real_snapshot_dry_run"] is False
    assert migration_g2_4ad["real_portfolio_or_broker_mutation_allowed"] is False
    assert migration_g2_4ad["legacy_root_lines_after"] == 28115
    assert migration_g2_4ad["legacy_root_top_level_functions_after"] == 837
    assert migration_g2_4ad["legacy_root_command_decorators_after"] == 798
    assert migration_g2_4ad["python_module_count"] == 841
    assert phase_g2_4ad["sources"]
    superseded_g2_4ad = set(phase_g2_4ad["superseded_source_paths"])
    for source in phase_g2_4ad["sources"]:
        if source["path"] in superseded_g2_4ad:
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4ae = baseline["phase_g2_4ae_etf_cli_dynamic_v3_real_snapshot_dry_run"]
    assert phase_g2_4ae["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4ae = phase_g2_4ae["migration"]
    assert migration_g2_4ae["callback_count"] == 3
    assert migration_g2_4ae["explicit_operator_trigger_required"] is True
    assert migration_g2_4ae["writes_risk_control_and_review_artifacts"] is True
    assert migration_g2_4ae["runs_snapshot_intake"] is False
    assert migration_g2_4ae["creates_owner_decision_or_paper_action"] is False
    assert migration_g2_4ae["order_real_portfolio_or_broker_mutation_allowed"] is False
    assert migration_g2_4ae["legacy_root_lines_after"] == 28032
    assert migration_g2_4ae["legacy_root_top_level_functions_after"] == 834
    assert migration_g2_4ae["legacy_root_command_decorators_after"] == 795
    assert migration_g2_4ae["python_module_count"] == 842
    assert phase_g2_4ae["sources"]
    superseded_g2_4ae = set(phase_g2_4ae["superseded_source_paths"])
    for source in phase_g2_4ae["sources"]:
        if source["path"] in superseded_g2_4ae:
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4af = baseline["phase_g2_4af_etf_cli_dynamic_v3_real_execution_owner_review"]
    assert phase_g2_4af["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4af = phase_g2_4af["migration"]
    assert migration_g2_4af["callback_count"] == 4
    assert migration_g2_4af["owner_decision_recording_allowed"] is True
    assert migration_g2_4af["pending_is_recordable_final_decision"] is False
    assert migration_g2_4af["sensitive_owner_notes_allowed"] is False
    assert migration_g2_4af["auto_applies_paper_action"] is False
    assert migration_g2_4af["order_portfolio_or_broker_mutation_allowed"] is False
    assert migration_g2_4af["legacy_root_lines_after"] == 27937
    assert migration_g2_4af["legacy_root_top_level_functions_after"] == 830
    assert migration_g2_4af["legacy_root_command_decorators_after"] == 791
    assert migration_g2_4af["python_module_count"] == 843
    assert phase_g2_4af["sources"]
    superseded_g2_4af = set(phase_g2_4af["superseded_source_paths"])
    for source in phase_g2_4af["sources"]:
        if source["path"] in superseded_g2_4af:
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4ag = baseline["phase_g2_4ag_etf_cli_dynamic_v3_real_snapshot_paper_action"]
    assert phase_g2_4ag["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4ag = phase_g2_4ag["migration"]
    assert migration_g2_4ag["callback_count"] == 3
    assert migration_g2_4ag["requires_validated_final_owner_decision"] is True
    assert migration_g2_4ag["pending_owner_decision_allowed"] is False
    assert migration_g2_4ag["content_derived_validation"] is True
    assert migration_g2_4ag["source_checksum_binding"] is True
    assert migration_g2_4ag["mutates_existing_paper_portfolio"] is False
    assert migration_g2_4ag["real_portfolio_order_or_broker_mutation_allowed"] is False
    assert migration_g2_4ag["legacy_root_lines_after"] == 27855
    assert migration_g2_4ag["legacy_root_top_level_functions_after"] == 827
    assert migration_g2_4ag["legacy_root_command_decorators_after"] == 788
    assert migration_g2_4ag["python_module_count"] == 844
    assert phase_g2_4ag["sources"]
    superseded_g2_4ag = set(phase_g2_4ag["superseded_source_paths"])
    for source in phase_g2_4ag["sources"]:
        if source["path"] in superseded_g2_4ag:
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4ah = baseline["phase_g2_4ah_etf_cli_dynamic_v3_weekly_real_snapshot_review"]
    assert phase_g2_4ah["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4ah = phase_g2_4ah["migration"]
    assert migration_g2_4ah["callback_count"] == 3
    assert migration_g2_4ah["week_ending_cutoff_enforced"] is True
    assert migration_g2_4ah["owner_chain_anchor"] is True
    assert migration_g2_4ah["cross_chain_latest_allowed"] is False
    assert migration_g2_4ah["source_checksum_and_inventory_binding"] is True
    assert migration_g2_4ah["content_derived_render_validation"] is True
    assert migration_g2_4ah["runs_upstream_workflow"] is False
    assert migration_g2_4ah["portfolio_order_or_broker_mutation_allowed"] is False
    assert migration_g2_4ah["legacy_root_lines_after"] == 27770
    assert migration_g2_4ah["legacy_root_top_level_functions_after"] == 824
    assert migration_g2_4ah["legacy_root_command_decorators_after"] == 785
    assert migration_g2_4ah["python_module_count"] == 845
    assert phase_g2_4ah["sources"]
    superseded_g2_4ah = set(phase_g2_4ah["superseded_source_paths"])
    for source in phase_g2_4ah["sources"]:
        if source["path"] in superseded_g2_4ah:
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4ai = baseline["phase_g2_4ai_etf_cli_dynamic_v3_position_advisory"]
    assert phase_g2_4ai["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4ai = phase_g2_4ai["migration"]
    assert migration_g2_4ai["callback_count"] == 3
    assert migration_g2_4ai["requires_validated_shadow_shortlist"] is True
    assert migration_g2_4ai["requires_complete_candidate_weight_paths"] is True
    assert migration_g2_4ai["source_checksum_binding"] is True
    assert migration_g2_4ai["content_derived_validation"] is True
    assert migration_g2_4ai["snapshot_optional_target_only"] is True
    assert migration_g2_4ai["advisory_is_execution_authorization"] is False
    assert migration_g2_4ai["portfolio_order_or_broker_mutation_allowed"] is False
    assert migration_g2_4ai["legacy_root_lines_after"] == 27682
    assert migration_g2_4ai["legacy_root_top_level_functions_after"] == 821
    assert migration_g2_4ai["legacy_root_command_decorators_after"] == 782
    assert migration_g2_4ai["python_module_count"] == 846
    assert phase_g2_4ai["sources"]
    superseded_g2_4ai = set(phase_g2_4ai["superseded_source_paths"])
    for source in phase_g2_4ai["sources"]:
        if source["path"] in superseded_g2_4ai:
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4aj = baseline["phase_g2_4aj_etf_cli_dynamic_v3_position_advisory_daily"]
    assert phase_g2_4aj["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4aj = phase_g2_4aj["migration"]
    assert migration_g2_4aj["callback_count"] == 3
    assert migration_g2_4aj["requires_validated_monitor"] is True
    assert migration_g2_4aj["candidate_weight_invariants_required"] is True
    assert migration_g2_4aj["agreement_uses_policy_tolerance"] is True
    assert migration_g2_4aj["filesystem_mtime_selection_allowed"] is False
    assert migration_g2_4aj["same_chain_drift_validation_required"] is True
    assert migration_g2_4aj["future_snapshot_allowed"] is False
    assert migration_g2_4aj["source_checksum_and_inventory_binding"] is True
    assert migration_g2_4aj["content_derived_render_validation"] is True
    assert migration_g2_4aj["advisory_is_execution_authorization"] is False
    assert migration_g2_4aj["legacy_root_lines_after"] == 27580
    assert migration_g2_4aj["legacy_root_top_level_functions_after"] == 818
    assert migration_g2_4aj["legacy_root_command_decorators_after"] == 779
    assert migration_g2_4aj["python_module_count"] == 847
    assert phase_g2_4aj["sources"]
    superseded_g2_4aj = set(phase_g2_4aj["superseded_source_paths"])
    for source in phase_g2_4aj["sources"]:
        if source["path"] in superseded_g2_4aj:
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4ak = baseline["phase_g2_4ak_etf_cli_dynamic_v3_consensus_drift"]
    assert phase_g2_4ak["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4ak = phase_g2_4ak["migration"]
    assert migration_g2_4ak["callback_count"] == 3
    assert migration_g2_4ak["requires_validated_current_monitor"] is True
    assert migration_g2_4ak["candidate_weight_invariants_required"] is True
    assert migration_g2_4ak["generated_cutoff_enforced"] is True
    assert migration_g2_4ak["semantic_previous_monitor_selection"] is True
    assert migration_g2_4ak["filesystem_mtime_selection_allowed"] is False
    assert migration_g2_4ak["latest_relevant_invalid_fails_closed"] is True
    assert migration_g2_4ak["all_non_consensus_requires_manual_review"] is True
    assert migration_g2_4ak["previous_source_id_is_content_derived"] is True
    assert migration_g2_4ak["previous_delta_uses_symbol_union"] is True
    assert migration_g2_4ak["source_checksum_and_inventory_binding"] is True
    assert migration_g2_4ak["content_derived_render_validation"] is True
    assert migration_g2_4ak["drift_is_execution_authorization"] is False
    assert migration_g2_4ak["legacy_root_lines_after"] == 27497
    assert migration_g2_4ak["legacy_root_top_level_functions_after"] == 815
    assert migration_g2_4ak["legacy_root_command_decorators_after"] == 776
    assert migration_g2_4ak["python_module_count"] == 848
    assert phase_g2_4ak["sources"]
    superseded_g2_4ak = set(phase_g2_4ak["superseded_source_paths"])
    for source in phase_g2_4ak["sources"]:
        if source["path"] in superseded_g2_4ak:
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4al = baseline["phase_g2_4al_etf_cli_dynamic_v3_owner_review_journal"]
    assert phase_g2_4al["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4al = phase_g2_4al["migration"]
    assert migration_g2_4al["callback_count"] == 5
    assert migration_g2_4al["requires_validated_daily_advisory"] is True
    assert migration_g2_4al["append_only_checksum_event_chain"] is True
    assert migration_g2_4al["single_final_decision_enforced"] is True
    assert migration_g2_4al["shared_owner_notes_privacy_gate"] is True
    assert migration_g2_4al["daily_source_checksum_binding"] is True
    assert migration_g2_4al["materialized_views_derived_from_events"] is True
    assert migration_g2_4al["legacy_unchained_mutation_allowed"] is False
    assert migration_g2_4al["paper_action_content_bound"] is True
    assert migration_g2_4al["content_derived_render_validation"] is True
    assert migration_g2_4al["owner_decision_is_execution_authorization"] is False
    assert migration_g2_4al["legacy_root_lines_after"] == 27373
    assert migration_g2_4al["legacy_root_top_level_functions_after"] == 810
    assert migration_g2_4al["legacy_root_command_decorators_after"] == 771
    assert migration_g2_4al["python_module_count"] == 850
    assert phase_g2_4al["sources"]
    superseded_g2_4al = set(phase_g2_4al["superseded_source_paths"])
    for source in phase_g2_4al["sources"]:
        if source["path"] in superseded_g2_4al:
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4am = baseline["phase_g2_4am_etf_cli_dynamic_v3_paper_portfolio"]
    assert phase_g2_4am["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4am = phase_g2_4am["migration"]
    assert migration_g2_4am["callback_count"] == 5
    assert migration_g2_4am["requires_validated_initial_config_snapshot"] is True
    assert migration_g2_4am["append_only_checksum_event_chain"] is True
    assert migration_g2_4am["one_review_one_action"] is True
    assert migration_g2_4am["requires_validated_owner_review"] is True
    assert migration_g2_4am["requires_frozen_daily_source"] is True
    assert migration_g2_4am["manual_deltas_finite_zero_sum"] is True
    assert migration_g2_4am["policy_limited_content_replay"] is True
    assert migration_g2_4am["materialized_views_derived_from_events"] is True
    assert migration_g2_4am["legacy_unchained_mutation_allowed"] is False
    assert migration_g2_4am["paper_state_is_real_portfolio_mutation"] is False
    assert migration_g2_4am["legacy_root_lines_after"] == 27212
    assert migration_g2_4am["legacy_root_top_level_functions_after"] == 805
    assert migration_g2_4am["legacy_root_command_decorators_after"] == 766
    assert migration_g2_4am["python_module_count"] == 851
    assert phase_g2_4am["sources"]
    superseded_g2_4am = set(phase_g2_4am["superseded_source_paths"])
    for source in phase_g2_4am["sources"]:
        if source["path"] in superseded_g2_4am:
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4an = baseline["phase_g2_4an_etf_cli_dynamic_v3_advisory_outcome"]
    assert phase_g2_4an["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4an = phase_g2_4an["migration"]
    assert migration_g2_4an["callback_count"] == 4
    assert migration_g2_4an["immutable_decision_time_event"] is True
    assert migration_g2_4an["append_only_checksum_update_chain"] is True
    assert migration_g2_4an["requires_validated_daily_and_paper_sources"] is True
    assert migration_g2_4an["future_paper_action_lookahead_allowed"] is False
    assert migration_g2_4an["required_symbol_complete_date_windows"] is True
    assert migration_g2_4an["fixed_share_and_piecewise_paths"] is True
    assert migration_g2_4an["transaction_and_slippage_costs_applied"] is True
    assert migration_g2_4an["non_available_metrics_are_null"] is True
    assert migration_g2_4an["content_derived_source_snapshot_replay"] is True
    assert migration_g2_4an["legacy_unchained_update_allowed"] is False
    assert migration_g2_4an["portfolio_or_execution_effect"] is False
    assert migration_g2_4an["legacy_root_lines_after"] == 27086
    assert migration_g2_4an["legacy_root_top_level_functions_after"] == 801
    assert migration_g2_4an["legacy_root_command_decorators_after"] == 762
    assert migration_g2_4an["python_module_count"] == 852
    assert phase_g2_4an["sources"]
    superseded_g2_4an = set(phase_g2_4an["superseded_source_paths"])
    for source in phase_g2_4an["sources"]:
        if source["path"] in superseded_g2_4an:
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4ao = baseline["phase_g2_4ao_etf_cli_dynamic_v3_owner_attribution"]
    assert phase_g2_4ao["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4ao = phase_g2_4ao["migration"]
    assert migration_g2_4ao["callback_count"] == 3
    assert migration_g2_4ao["requires_validated_owner_reviews"] is True
    assert migration_g2_4ao["requires_validated_advisory_outcomes"] is True
    assert migration_g2_4ao["generated_cutoff_enforced"] is True
    assert migration_g2_4ao["one_daily_zero_or_one_outcome"] is True
    assert migration_g2_4ao["immutable_source_snapshots"] is True
    assert migration_g2_4ao["review_outcome_window_units_separated"] is True
    assert migration_g2_4ao["missing_horizon_metrics_are_null"] is True
    assert migration_g2_4ao["content_derived_snapshot_validation"] is True
    assert migration_g2_4ao["legacy_unsnapshotted_is_current_evidence"] is False
    assert migration_g2_4ao["attribution_is_causal_evidence"] is False
    assert migration_g2_4ao["portfolio_or_execution_effect"] is False
    assert migration_g2_4ao["legacy_root_lines_after"] == 27004
    assert migration_g2_4ao["legacy_root_top_level_functions_after"] == 798
    assert migration_g2_4ao["legacy_root_command_decorators_after"] == 759
    assert migration_g2_4ao["python_module_count"] == 853
    assert phase_g2_4ao["sources"]
    superseded_g2_4ao = set(phase_g2_4ao["superseded_source_paths"])
    for source in phase_g2_4ao["sources"]:
        if source["path"] in superseded_g2_4ao:
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4ap = baseline["phase_g2_4ap_etf_cli_dynamic_v3_shadow_aging"]
    assert phase_g2_4ap["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4ap = phase_g2_4ap["migration"]
    assert migration_g2_4ap["callback_count"] == 3
    assert migration_g2_4ap["requires_validated_shortlist"] is True
    assert migration_g2_4ap["requires_validated_monitor_drift_outcome"] is True
    assert migration_g2_4ap["generated_cutoff_enforced"] is True
    assert migration_g2_4ap["duplicate_source_binding_allowed"] is False
    assert migration_g2_4ap["immutable_source_snapshot"] is True
    assert migration_g2_4ap["true_weight_change_rebalance_count"] is True
    assert migration_g2_4ap["candidate_specific_outcome_replay"] is True
    assert migration_g2_4ap["missing_outcome_score_is_null"] is True
    assert migration_g2_4ap["reviewed_policy_thresholds"] is True
    assert migration_g2_4ap["selective_outcome_update_event_bound"] is True
    assert migration_g2_4ap["eligible_is_automatic_promotion"] is False
    assert migration_g2_4ap["portfolio_or_execution_effect"] is False
    assert migration_g2_4ap["legacy_root_lines_after"] == 26911
    assert migration_g2_4ap["legacy_root_top_level_functions_after"] == 795
    assert migration_g2_4ap["legacy_root_command_decorators_after"] == 756
    assert migration_g2_4ap["python_module_count"] == 854
    assert phase_g2_4ap["sources"]
    superseded_g2_4ap = set(phase_g2_4ap["superseded_source_paths"])
    for source in phase_g2_4ap["sources"]:
        if source["path"] in superseded_g2_4ap:
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4aq = baseline["phase_g2_4aq_etf_cli_dynamic_v3_weekly_advisory_review"]
    assert phase_g2_4aq["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4aq = phase_g2_4aq["migration"]
    assert migration_g2_4aq["callback_count"] == 3
    assert migration_g2_4aq["calendar_week_and_generated_cutoff_enforced"] is True
    assert migration_g2_4aq["validated_daily_monitor_anchor"] is True
    assert migration_g2_4aq["owner_paper_outcome_cutoff_prefix_replay"] is True
    assert migration_g2_4aq["ambiguous_source_binding_allowed"] is False
    assert migration_g2_4aq["immutable_source_snapshot"] is True
    assert migration_g2_4aq["missing_outcome_metrics_are_null"] is True
    assert migration_g2_4aq["reviewed_policy_coverage_and_precedence"] is True
    assert migration_g2_4aq["content_derived_all_views_validation"] is True
    assert migration_g2_4aq["independent_scheduler_added"] is False
    assert migration_g2_4aq["portfolio_or_execution_effect"] is False
    assert migration_g2_4aq["legacy_root_lines_after"] == 26802
    assert migration_g2_4aq["legacy_root_top_level_functions_after"] == 792
    assert migration_g2_4aq["legacy_root_command_decorators_after"] == 753
    assert migration_g2_4aq["python_module_count"] == 855
    assert phase_g2_4aq["sources"]
    superseded_g2_4aq = set(phase_g2_4aq["superseded_source_paths"])
    for source in phase_g2_4aq["sources"]:
        if source["path"] in superseded_g2_4aq:
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4ar = baseline["phase_g2_4ar_etf_cli_dynamic_v3_replay_inventory"]
    assert phase_g2_4ar["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4ar = phase_g2_4ar["migration"]
    assert migration_g2_4ar["callback_count"] == 3
    assert migration_g2_4ar["valid_range_and_generated_cutoff_enforced"] is True
    assert migration_g2_4ar["ambiguous_daily_or_cutoff_binding_allowed"] is False
    assert migration_g2_4ar["semantic_drift_selection_without_mtime"] is True
    assert migration_g2_4ar["immutable_source_snapshot"] is True
    assert migration_g2_4ar["price_is_outcome_availability_only"] is True
    assert migration_g2_4ar["hard_pit_limitations_ineligible"] is True
    assert migration_g2_4ar["content_derived_all_views_validation"] is True
    assert migration_g2_4ar["legacy_missing_artifact_accepted"] is False
    assert migration_g2_4ar["historical_replay_or_backfill_executed"] is False
    assert migration_g2_4ar["portfolio_or_execution_effect"] is False
    assert migration_g2_4ar["legacy_root_lines_after"] == 26686
    assert migration_g2_4ar["legacy_root_top_level_functions_after"] == 789
    assert migration_g2_4ar["legacy_root_command_decorators_after"] == 750
    assert migration_g2_4ar["python_module_count"] == 856
    assert phase_g2_4ar["sources"]
    superseded_g2_4ar = set(phase_g2_4ar["superseded_source_paths"])
    for source in phase_g2_4ar["sources"]:
        if source["path"] in superseded_g2_4ar:
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4as = baseline["phase_g2_4as_etf_cli_dynamic_v3_historical_replay"]
    assert phase_g2_4as["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4as = phase_g2_4as["migration"]
    assert migration_g2_4as["callback_count"] == 3
    assert migration_g2_4as["requires_full_validated_inventory"] is True
    assert migration_g2_4as["generated_cutoff_ordering_enforced"] is True
    assert migration_g2_4as["hard_pit_override_allowed"] is False
    assert migration_g2_4as["weight_simplex_enforced"] is True
    assert migration_g2_4as["fallback_source_status_explicit"] is True
    assert migration_g2_4as["one_way_l1_turnover_explicit"] is True
    assert migration_g2_4as["immutable_source_snapshot"] is True
    assert migration_g2_4as["content_derived_all_views_validation"] is True
    assert migration_g2_4as["outcome_price_read"] is False
    assert migration_g2_4as["portfolio_or_execution_effect"] is False
    assert migration_g2_4as["legacy_root_lines_after"] == 26602
    assert migration_g2_4as["legacy_root_top_level_functions_after"] == 786
    assert migration_g2_4as["legacy_root_command_decorators_after"] == 747
    assert migration_g2_4as["python_module_count"] == 857
    assert phase_g2_4as["sources"]
    superseded_g2_4as = set(phase_g2_4as.get("superseded_source_paths", []))
    for source in phase_g2_4as["sources"]:
        if source["path"] in superseded_g2_4as:
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4at = baseline["phase_g2_4at_etf_cli_dynamic_v3_backfilled_outcome"]
    assert phase_g2_4at["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4at = phase_g2_4at["migration"]
    assert migration_g2_4at["callback_count"] == 3
    assert migration_g2_4at["requires_full_validated_replay"] is True
    assert migration_g2_4at["generated_cutoff_ordering_enforced"] is True
    assert migration_g2_4at["cached_data_quality_gate_required"] is True
    assert migration_g2_4at["non_available_metrics_are_null"] is True
    assert migration_g2_4at["fixed_share_path_explicit"] is True
    assert migration_g2_4at["versioned_initial_turnover_cost_explicit"] is True
    assert migration_g2_4at["immutable_source_snapshot"] is True
    assert migration_g2_4at["content_derived_all_views_validation"] is True
    assert migration_g2_4at["portfolio_or_execution_effect"] is False
    assert migration_g2_4at["legacy_root_lines_after"] == 26504
    assert migration_g2_4at["legacy_root_top_level_functions_after"] == 783
    assert migration_g2_4at["legacy_root_command_decorators_after"] == 744
    assert migration_g2_4at["python_module_count"] == 858
    assert phase_g2_4at["sources"]
    superseded_g2_4at = set(phase_g2_4at.get("superseded_source_paths", []))
    for source in phase_g2_4at["sources"]:
        if source["path"] in superseded_g2_4at:
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4au = baseline["phase_g2_4au_etf_cli_dynamic_v3_historical_paper_sim"]
    assert phase_g2_4au["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4au = phase_g2_4au["migration"]
    assert migration_g2_4au["callback_count"] == 3
    assert migration_g2_4au["requires_full_validated_replay"] is True
    assert migration_g2_4au["cached_data_quality_gate_required"] is True
    assert migration_g2_4au["fixed_share_total_and_risk_path_consistent"] is True
    assert migration_g2_4au["missing_price_return_zero_allowed"] is False
    assert migration_g2_4au["event_target_reset_cost_explicit"] is True
    assert migration_g2_4au["immutable_source_snapshot"] is True
    assert migration_g2_4au["content_derived_all_views_validation"] is True
    assert migration_g2_4au["portfolio_or_execution_effect"] is False
    assert migration_g2_4au["legacy_root_lines_after"] == 26414
    assert migration_g2_4au["legacy_root_top_level_functions_after"] == 780
    assert migration_g2_4au["legacy_root_command_decorators_after"] == 741
    assert migration_g2_4au["python_module_count"] == 859
    assert phase_g2_4au["sources"]
    superseded_g2_4au = set(phase_g2_4au.get("superseded_source_paths", []))
    for source in phase_g2_4au["sources"]:
        if source["path"] in superseded_g2_4au:
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4av = baseline["phase_g2_4av_etf_cli_dynamic_v3_replay_performance_review"]
    assert phase_g2_4av["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4av = phase_g2_4av["migration"]
    assert migration_g2_4av["callback_count"] == 3
    assert migration_g2_4av["requires_full_validated_backfill_and_simulation"] is True
    assert migration_g2_4av["same_replay_and_time_ordering_enforced"] is True
    assert migration_g2_4av["unsupported_classification_metrics_are_null"] is True
    assert migration_g2_4av["reviewed_sample_floor_policy_required"] is True
    assert migration_g2_4av["automatic_config_or_promotion_allowed"] is False
    assert migration_g2_4av["immutable_source_snapshot"] is True
    assert migration_g2_4av["content_derived_all_views_validation"] is True
    assert migration_g2_4av["portfolio_or_execution_effect"] is False
    assert migration_g2_4av["legacy_root_lines_after"] == 26322
    assert migration_g2_4av["legacy_root_top_level_functions_after"] == 777
    assert migration_g2_4av["legacy_root_command_decorators_after"] == 738
    assert migration_g2_4av["python_module_count"] == 860
    assert phase_g2_4av["sources"]
    superseded_g2_4av = set(phase_g2_4av.get("superseded_source_paths", []))
    for source in phase_g2_4av["sources"]:
        if source["path"] in superseded_g2_4av:
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4aw = baseline["phase_g2_4aw_etf_cli_dynamic_v3_replay_diagnosis"]
    assert phase_g2_4aw["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4aw = phase_g2_4aw["migration"]
    assert migration_g2_4aw["callback_count"] == 3
    assert migration_g2_4aw["requires_five_full_validated_sources"] is True
    assert migration_g2_4aw["full_lineage_and_time_ordering_enforced"] is True
    assert migration_g2_4aw["unit_aware_pending_reasons"] is True
    assert migration_g2_4aw["healthy_unknown_blocker_allowed"] is False
    assert migration_g2_4aw["reviewed_comparison_readiness_gate"] is True
    assert migration_g2_4aw["immutable_source_snapshot"] is True
    assert migration_g2_4aw["content_derived_all_views_validation"] is True
    assert migration_g2_4aw["repair_or_calibration_executed"] is False
    assert migration_g2_4aw["portfolio_or_execution_effect"] is False
    assert migration_g2_4aw["legacy_root_lines_after"] == 26200
    assert migration_g2_4aw["legacy_root_top_level_functions_after"] == 774
    assert migration_g2_4aw["legacy_root_command_decorators_after"] == 735
    assert migration_g2_4aw["python_module_count"] == 861
    assert phase_g2_4aw["sources"]
    superseded_g2_4aw = set(phase_g2_4aw.get("superseded_source_paths", []))
    for source in phase_g2_4aw["sources"]:
        if source["path"] in superseded_g2_4aw:
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4ax = baseline["phase_g2_4ax_etf_cli_dynamic_v3_backfill_repair"]
    assert phase_g2_4ax["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4ax = phase_g2_4ax["migration"]
    assert migration_g2_4ax["callback_count"] == 3
    assert migration_g2_4ax["requires_three_full_validated_sources"] is True
    assert migration_g2_4ax["full_lineage_and_time_ordering_enforced"] is True
    assert migration_g2_4ax["cached_data_quality_gate_required"] is True
    assert migration_g2_4ax["original_available_rows_immutable"] is True
    assert migration_g2_4ax["repair_count_unit"] == "event_variant_window"
    assert migration_g2_4ax["immutable_source_snapshot"] is True
    assert migration_g2_4ax["content_derived_all_views_validation"] is True
    assert migration_g2_4ax["comparison_or_calibration_executed"] is False
    assert migration_g2_4ax["portfolio_or_execution_effect"] is False
    assert migration_g2_4ax["legacy_root_lines_after"] == 26099
    assert migration_g2_4ax["legacy_root_top_level_functions_after"] == 771
    assert migration_g2_4ax["legacy_root_command_decorators_after"] == 732
    assert migration_g2_4ax["python_module_count"] == 862
    assert phase_g2_4ax["sources"]
    superseded_g2_4ax = set(phase_g2_4ax.get("superseded_source_paths", []))
    for source in phase_g2_4ax["sources"]:
        if source["path"] in superseded_g2_4ax:
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4ay = baseline["phase_g2_4ay_etf_cli_dynamic_v3_variant_comparison"]
    assert phase_g2_4ay["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4ay = phase_g2_4ay["migration"]
    assert migration_g2_4ay["callback_count"] == 3
    assert migration_g2_4ay["requires_validated_backfill_and_optional_repair"] is True
    assert migration_g2_4ay["full_lineage_and_time_ordering_enforced"] is True
    assert migration_g2_4ay["duplicate_variant_window_keys_allowed"] is False
    assert migration_g2_4ay["missing_metrics_are_null"] is True
    assert migration_g2_4ay["same_event_primary_window_ranking"] is True
    assert migration_g2_4ay["reviewed_sample_floor_policy_required"] is True
    assert migration_g2_4ay["immutable_source_snapshot"] is True
    assert migration_g2_4ay["content_derived_all_views_validation"] is True
    assert migration_g2_4ay["automatic_calibration_allowed"] is False
    assert migration_g2_4ay["portfolio_or_execution_effect"] is False
    assert migration_g2_4ay["legacy_root_lines_after"] == 26007
    assert migration_g2_4ay["legacy_root_top_level_functions_after"] == 768
    assert migration_g2_4ay["legacy_root_command_decorators_after"] == 729
    assert migration_g2_4ay["python_module_count"] == 863
    assert phase_g2_4ay["sources"]
    for source in phase_g2_4ay["sources"]:
        if source["path"] in set(phase_g2_4ay.get("superseded_source_paths", [])):
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4az = baseline["phase_g2_4az_etf_cli_dynamic_v3_rule_calibration"]
    assert phase_g2_4az["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4az = phase_g2_4az["migration"]
    assert migration_g2_4az["callback_count"] == 3
    assert migration_g2_4az["requires_validated_variant_comparison"] is True
    assert migration_g2_4az["source_and_policy_snapshot_required"] is True
    assert migration_g2_4az["evidence_action_is_policy_proposal"] is False
    assert migration_g2_4az["insufficient_data_policy_change_allowed"] is False
    assert migration_g2_4az["directional_missing_metrics_are_null"] is True
    assert migration_g2_4az["content_derived_all_views_validation"] is True
    assert migration_g2_4az["automatic_policy_apply_allowed"] is False
    assert migration_g2_4az["portfolio_or_execution_effect"] is False
    assert migration_g2_4az["legacy_root_lines_after"] == 25921
    assert migration_g2_4az["legacy_root_top_level_functions_after"] == 765
    assert migration_g2_4az["legacy_root_command_decorators_after"] == 726
    assert migration_g2_4az["python_module_count"] == 864
    assert phase_g2_4az["sources"]
    for source in phase_g2_4az["sources"]:
        if source["path"] in set(phase_g2_4az.get("superseded_source_paths", [])):
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4ba = baseline["phase_g2_4ba_etf_cli_dynamic_v3_replay_forward_bridge"]
    assert phase_g2_4ba["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4ba = phase_g2_4ba["migration"]
    assert migration_g2_4ba["callback_count"] == 3
    assert migration_g2_4ba["requires_three_content_derived_sources"] is True
    assert migration_g2_4ba["full_lineage_and_time_ordering_enforced"] is True
    assert migration_g2_4ba["source_and_policy_snapshot_required"] is True
    assert migration_g2_4ba["evidence_action_is_policy_proposal"] is False
    assert migration_g2_4ba["unknown_reason_injected_when_empty"] is False
    assert migration_g2_4ba["content_derived_all_views_validation"] is True
    assert migration_g2_4ba["automatic_upstream_or_policy_apply_allowed"] is False
    assert migration_g2_4ba["portfolio_or_execution_effect"] is False
    assert migration_g2_4ba["legacy_root_lines_after"] == 25823
    assert migration_g2_4ba["legacy_root_top_level_functions_after"] == 762
    assert migration_g2_4ba["legacy_root_command_decorators_after"] == 723
    assert migration_g2_4ba["python_module_count"] == 865
    assert phase_g2_4ba["sources"]
    for source in phase_g2_4ba["sources"]:
        if source["path"] in set(phase_g2_4ba.get("superseded_source_paths", [])):
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4bb = baseline["phase_g2_4bb_etf_cli_dynamic_v3_outcome_due"]
    assert phase_g2_4bb["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4bb = phase_g2_4bb["migration"]
    assert migration_g2_4bb["callback_count"] == 4
    assert migration_g2_4bb["pre_output_data_quality_and_source_validation"] is True
    assert migration_g2_4bb["duplicate_daily_window_allowed"] is False
    assert migration_g2_4bb["cutoff_price_date_snapshot_required"] is True
    assert migration_g2_4bb["content_derived_scan_validation"] is True
    assert migration_g2_4bb["update_ready_single_use"] is True
    assert migration_g2_4bb["allowed_window_days_explicit"] is True
    assert migration_g2_4bb["not_due_or_price_missing_update_allowed"] is False
    assert migration_g2_4bb["portfolio_or_execution_effect"] is False
    assert migration_g2_4bb["legacy_root_lines_after"] == 25692
    assert migration_g2_4bb["legacy_root_top_level_functions_after"] == 758
    assert migration_g2_4bb["legacy_root_command_decorators_after"] == 719
    assert migration_g2_4bb["python_module_count"] == 866
    assert phase_g2_4bb["sources"]
    for source in phase_g2_4bb["sources"]:
        if source["path"] in set(phase_g2_4bb.get("superseded_source_paths", [])):
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4bc = baseline["phase_g2_4bc_etf_cli_dynamic_v3_replay_sample_expansion"]
    assert phase_g2_4bc["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4bc = phase_g2_4bc["migration"]
    assert migration_g2_4bc["callback_count"] == 3
    assert migration_g2_4bc["pre_output_range_time_data_quality_gate"] is True
    assert migration_g2_4bc["daily_owner_replay_full_validation"] is True
    assert migration_g2_4bc["duplicate_or_conflicting_event_allowed"] is False
    assert migration_g2_4bc["source_policy_price_snapshot_required"] is True
    assert migration_g2_4bc["pit_safety_separate_from_price_evaluability"] is True
    assert migration_g2_4bc["content_derived_all_views_validation"] is True
    assert migration_g2_4bc["automatic_replay_execution_allowed"] is False
    assert migration_g2_4bc["portfolio_or_execution_effect"] is False
    assert migration_g2_4bc["legacy_root_lines_after"] == 25589
    assert migration_g2_4bc["legacy_root_top_level_functions_after"] == 755
    assert migration_g2_4bc["legacy_root_command_decorators_after"] == 716
    assert migration_g2_4bc["python_module_count"] == 867
    assert phase_g2_4bc["sources"]
    for source in phase_g2_4bc["sources"]:
        if source["path"] in set(phase_g2_4bc.get("superseded_source_paths", [])):
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4bd = baseline["phase_g2_4bd_etf_cli_dynamic_v3_outcome_dashboard"]
    assert phase_g2_4bd["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4bd = phase_g2_4bd["migration"]
    assert migration_g2_4bd["callback_count"] == 3
    assert migration_g2_4bd["all_selected_sources_content_validated"] is True
    assert migration_g2_4bd["semantic_latest_and_cutoff_enforced"] is True
    assert migration_g2_4bd["duplicate_or_cross_lineage_sample_allowed"] is False
    assert migration_g2_4bd["source_and_pending_policy_snapshot_required"] is True
    assert migration_g2_4bd["mode_specific_sample_units_explicit"] is True
    assert migration_g2_4bd["content_derived_all_views_validation"] is True
    assert migration_g2_4bd["automatic_upstream_run_allowed"] is False
    assert migration_g2_4bd["portfolio_or_execution_effect"] is False
    assert migration_g2_4bd["legacy_root_lines_after"] == 25492
    assert migration_g2_4bd["legacy_root_top_level_functions_after"] == 752
    assert migration_g2_4bd["legacy_root_command_decorators_after"] == 713
    assert migration_g2_4bd["python_module_count"] == 868
    assert phase_g2_4bd["sources"]
    for source in phase_g2_4bd["sources"]:
        if source["path"] in set(phase_g2_4bd.get("superseded_source_paths", [])):
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4be = baseline["phase_g2_4be_etf_cli_dynamic_v3_limited_vs_notrade"]
    assert phase_g2_4be["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4be = phase_g2_4be["migration"]
    assert migration_g2_4be["callback_count"] == 3
    assert migration_g2_4be["all_selected_sources_content_validated"] is True
    assert migration_g2_4be["semantic_latest_and_cutoff_enforced"] is True
    assert migration_g2_4be["strict_unique_pairing_required"] is True
    assert migration_g2_4be["missing_metrics_remain_null"] is True
    assert migration_g2_4be["reviewed_policy_snapshot_required"] is True
    assert migration_g2_4be["real_regime_labels_only"] is True
    assert migration_g2_4be["content_derived_all_views_validation"] is True
    assert migration_g2_4be["automatic_policy_apply_allowed"] is False
    assert migration_g2_4be["portfolio_or_execution_effect"] is False
    assert migration_g2_4be["legacy_root_lines_after"] == 25399
    assert migration_g2_4be["legacy_root_top_level_functions_after"] == 749
    assert migration_g2_4be["legacy_root_command_decorators_after"] == 710
    assert migration_g2_4be["python_module_count"] == 869
    assert phase_g2_4be["sources"]
    for source in phase_g2_4be["sources"]:
        if source["path"] in set(phase_g2_4be.get("superseded_source_paths", [])):
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4bf = baseline["phase_g2_4bf_etf_cli_dynamic_v3_consensus_risk"]
    assert phase_g2_4bf["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4bf = phase_g2_4bf["migration"]
    assert migration_g2_4bf["callback_count"] == 3
    assert migration_g2_4bf["all_selected_sources_content_validated"] is True
    assert migration_g2_4bf["semantic_latest_cutoff_and_replay_lineage_enforced"] is True
    assert migration_g2_4bf["distinct_decision_date_exposure_required"] is True
    assert migration_g2_4bf["candidate_target_fallback_allowed"] is False
    assert migration_g2_4bf["strict_paired_drawdown_required"] is True
    assert migration_g2_4bf["distinct_event_turnover_required"] is True
    assert migration_g2_4bf["missing_metrics_remain_null"] is True
    assert migration_g2_4bf["reviewed_risk_policy_snapshot_required"] is True
    assert migration_g2_4bf["content_derived_all_views_validation"] is True
    assert migration_g2_4bf["default_execution_or_policy_apply_allowed"] is False
    assert migration_g2_4bf["portfolio_or_execution_effect"] is False
    assert migration_g2_4bf["legacy_root_lines_after"] == 25301
    assert migration_g2_4bf["legacy_root_top_level_functions_after"] == 746
    assert migration_g2_4bf["legacy_root_command_decorators_after"] == 707
    assert migration_g2_4bf["python_module_count"] == 870
    assert phase_g2_4bf["sources"]
    for source in phase_g2_4bf["sources"]:
        if source["path"] in set(phase_g2_4bf.get("superseded_source_paths", [])):
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4bg = baseline["phase_g2_4bg_etf_cli_dynamic_v3_outcome_update_review"]
    assert phase_g2_4bg["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4bg = phase_g2_4bg["migration"]
    assert migration_g2_4bg["callback_count"] == 3
    assert migration_g2_4bg["explicit_due_source_content_validated"] is True
    assert migration_g2_4bg["due_id_and_cutoff_enforced"] is True
    assert migration_g2_4bg["unique_outcome_window_identity_required"] is True
    assert migration_g2_4bg["ready_status_deterministically_derived"] is True
    assert migration_g2_4bg["no_future_data_date_proof_required"] is True
    assert migration_g2_4bg["full_due_bundle_snapshot_required"] is True
    assert migration_g2_4bg["content_derived_all_views_validation"] is True
    assert migration_g2_4bg["empty_artifact_is_insufficient_data"] is True
    assert migration_g2_4bg["outcome_update_or_data_refresh_allowed"] is False
    assert migration_g2_4bg["portfolio_or_execution_effect"] is False
    assert migration_g2_4bg["legacy_root_lines_after"] == 25221
    assert migration_g2_4bg["legacy_root_top_level_functions_after"] == 743
    assert migration_g2_4bg["legacy_root_command_decorators_after"] == 704
    assert migration_g2_4bg["python_module_count"] == 871
    assert phase_g2_4bg["sources"]
    for source in phase_g2_4bg["sources"]:
        if source["path"] in set(phase_g2_4bg.get("superseded_source_paths", [])):
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4bh = baseline["phase_g2_4bh_etf_cli_dynamic_v3_outcome_update"]
    assert phase_g2_4bh["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4bh = phase_g2_4bh["migration"]
    assert migration_g2_4bh["callback_count"] == 3
    assert migration_g2_4bh["explicit_review_content_validated"] is True
    assert migration_g2_4bh["single_committed_update_per_review"] is True
    assert migration_g2_4bh["unique_identity_and_live_pre_state_required"] is True
    assert migration_g2_4bh["isolated_full_batch_preflight_required"] is True
    assert migration_g2_4bh["transaction_states"] == [
        "PREPARED",
        "COMMITTED",
        "ROLLED_BACK",
    ]
    assert migration_g2_4bh["all_or_rollback_required"] is True
    assert migration_g2_4bh["full_review_pre_post_bundles_required"] is True
    assert migration_g2_4bh["selected_cohort_delta_required"] is True
    assert migration_g2_4bh["content_derived_all_views_validation"] is True
    assert migration_g2_4bh["automatic_downstream_refresh_allowed"] is False
    assert migration_g2_4bh["portfolio_or_execution_effect"] is False
    assert migration_g2_4bh["legacy_root_lines_after"] == 25111
    assert migration_g2_4bh["legacy_root_top_level_functions_after"] == 740
    assert migration_g2_4bh["legacy_root_command_decorators_after"] == 701
    assert migration_g2_4bh["python_module_count"] == 872
    assert phase_g2_4bh["sources"]
    for source in phase_g2_4bh["sources"]:
        if source["path"] in set(phase_g2_4bh.get("superseded_source_paths", [])):
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4bi = baseline["phase_g2_4bi_etf_cli_dynamic_v3_rolling_evidence_refresh"]
    assert phase_g2_4bi["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4bi = phase_g2_4bi["migration"]
    assert migration_g2_4bi["callback_count"] == 3
    assert migration_g2_4bi["explicit_committed_update_content_validated"] is True
    assert migration_g2_4bi["single_committed_refresh_per_update"] is True
    assert migration_g2_4bi["transaction_states"] == [
        "PREPARED",
        "COMMITTED",
        "ROLLED_BACK",
    ]
    assert migration_g2_4bi["partial_artifact_and_pointer_rollback_required"] is True
    assert migration_g2_4bi["all_downstream_content_validations_required"] is True
    assert migration_g2_4bi["full_update_baseline_post_bundles_required"] is True
    assert migration_g2_4bi["selected_cohort_forward_delta_required"] is True
    assert migration_g2_4bi["consumed_due_reuse_allowed"] is False
    assert migration_g2_4bi["reader_brief_section_is_global_update"] is False
    assert migration_g2_4bi["content_derived_all_views_validation"] is True
    assert migration_g2_4bi["portfolio_or_execution_effect"] is False
    assert migration_g2_4bi["legacy_root_lines_after"] == 25020
    assert migration_g2_4bi["legacy_root_top_level_functions_after"] == 737
    assert migration_g2_4bi["legacy_root_command_decorators_after"] == 698
    assert migration_g2_4bi["python_module_count"] == 873
    assert phase_g2_4bi["sources"]
    for source in phase_g2_4bi["sources"]:
        if source["path"] in set(phase_g2_4bi.get("superseded_source_paths", [])):
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4bj = baseline["phase_g2_4bj_etf_cli_dynamic_v3_evidence_trend"]
    assert phase_g2_4bj["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4bj = phase_g2_4bj["migration"]
    assert migration_g2_4bj["callback_count"] == 3
    assert migration_g2_4bj["validated_committed_refreshes_only"] is True
    assert migration_g2_4bj["unique_refresh_and_update_identity_required"] is True
    assert migration_g2_4bj["excluded_refresh_reason_evidence_required"] is True
    assert migration_g2_4bj["prepared_or_invalid_committed_blocks"] is True
    assert migration_g2_4bj["full_refresh_and_policy_snapshot_required"] is True
    assert migration_g2_4bj["full_dashboard_state_comparison_required"] is True
    assert migration_g2_4bj["null_preserving_metrics_required"] is True
    assert migration_g2_4bj["reviewed_trend_policy_required"] is True
    assert migration_g2_4bj["content_derived_all_views_validation"] is True
    assert migration_g2_4bj["automatic_upstream_or_policy_apply_allowed"] is False
    assert migration_g2_4bj["portfolio_or_execution_effect"] is False
    assert migration_g2_4bj["legacy_root_lines_after"] == 24948
    assert migration_g2_4bj["legacy_root_top_level_functions_after"] == 734
    assert migration_g2_4bj["legacy_root_command_decorators_after"] == 695
    assert migration_g2_4bj["python_module_count"] == 874
    if phase_g2_4bj["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4bj["sources"]
        for source in phase_g2_4bj["sources"]:
            if source["path"] in set(phase_g2_4bj.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4bk = baseline["phase_g2_4bk_etf_cli_dynamic_v3_forward_outcome_decision"]
    assert phase_g2_4bk["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4bk = phase_g2_4bk["migration"]
    assert migration_g2_4bk["callback_count"] == 3
    assert migration_g2_4bk["cutoff_bound_zero_or_one_source_selection"] is True
    assert migration_g2_4bk["explicit_source_ids_supported"] is True
    assert migration_g2_4bk["all_selected_sources_content_validated"] is True
    assert migration_g2_4bk["update_refresh_trend_lineage_required"] is True
    assert migration_g2_4bk["full_source_and_policy_snapshot_required"] is True
    assert migration_g2_4bk["missing_and_invalid_sources_distinct"] is True
    assert migration_g2_4bk["null_preserving_full_dashboard_state_required"] is True
    assert migration_g2_4bk["reviewed_decision_policy_required"] is True
    assert migration_g2_4bk["content_derived_all_views_validation"] is True
    assert migration_g2_4bk["automatic_upstream_or_policy_apply_allowed"] is False
    assert migration_g2_4bk["portfolio_or_execution_effect"] is False
    assert migration_g2_4bk["legacy_root_lines_after"] == 24861
    assert migration_g2_4bk["legacy_root_top_level_functions_after"] == 731
    assert migration_g2_4bk["legacy_root_command_decorators_after"] == 692
    assert migration_g2_4bk["python_module_count"] == 875
    if phase_g2_4bk["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4bk["sources"]
        for source in phase_g2_4bk["sources"]:
            if source["path"] in set(phase_g2_4bk.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4bl = baseline["phase_g2_4bl_etf_cli_dynamic_v3_backtest_sim_events"]
    assert phase_g2_4bl["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4bl = phase_g2_4bl["migration"]
    assert migration_g2_4bl["callback_count"] == 4
    assert migration_g2_4bl["strict_governed_config_validation"] is True
    assert migration_g2_4bl["pre_output_timezone_range_and_data_quality_gate"] is True
    assert migration_g2_4bl["zero_partial_artifact_on_preflight_failure"] is True
    assert migration_g2_4bl["full_governed_source_snapshot_required"] is True
    assert migration_g2_4bl["cutoff_bound_price_rate_rows_required"] is True
    assert migration_g2_4bl["candidate_identity_and_source_validation_required"] is True
    assert migration_g2_4bl["legal_empty_schedule_is_insufficient_data"] is True
    assert migration_g2_4bl["content_derived_all_views_validation"] is True
    assert migration_g2_4bl["automatic_downstream_simulation_allowed"] is False
    assert migration_g2_4bl["portfolio_or_execution_effect"] is False
    assert migration_g2_4bl["legacy_root_lines_after"] == 24761
    assert migration_g2_4bl["legacy_root_top_level_functions_after"] == 727
    assert migration_g2_4bl["legacy_root_command_decorators_after"] == 688
    assert migration_g2_4bl["python_module_count"] == 876
    if phase_g2_4bl["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4bl["sources"]
        for source in phase_g2_4bl["sources"]:
            if source["path"] in set(phase_g2_4bl.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4bm = baseline["phase_g2_4bm_etf_cli_dynamic_v3_backtest_sim_variants"]
    assert phase_g2_4bm["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4bm = phase_g2_4bm["migration"]
    assert migration_g2_4bm["callback_count"] == 3
    assert migration_g2_4bm["pre_output_event_content_validation"] is True
    assert migration_g2_4bm["source_generated_cutoff_required"] is True
    assert migration_g2_4bm["full_event_config_validation_snapshot_required"] is True
    assert migration_g2_4bm["exact_enabled_variant_coverage_required"] is True
    assert migration_g2_4bm["unique_event_variant_identity_required"] is True
    assert migration_g2_4bm["state_weight_delta_turnover_invariants_required"] is True
    assert migration_g2_4bm["content_derived_all_views_validation"] is True
    assert migration_g2_4bm["automatic_outcome_or_paper_run_allowed"] is False
    assert migration_g2_4bm["portfolio_or_execution_effect"] is False
    assert migration_g2_4bm["legacy_root_lines_after"] == 24676
    assert migration_g2_4bm["legacy_root_top_level_functions_after"] == 724
    assert migration_g2_4bm["legacy_root_command_decorators_after"] == 685
    assert migration_g2_4bm["python_module_count"] == 877
    if phase_g2_4bm["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4bm["sources"]
        for source in phase_g2_4bm["sources"]:
            if source["path"] in set(phase_g2_4bm.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4bn = baseline["phase_g2_4bn_etf_cli_dynamic_v3_backtest_sim_outcome"]
    assert phase_g2_4bn["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4bn = phase_g2_4bn["migration"]
    assert migration_g2_4bn["callback_count"] == 3
    assert migration_g2_4bn["canonical_owner"].endswith("dynamic_v3_backtest_sim_outcome.py")
    assert migration_g2_4bn["pre_output_variant_content_validation"] is True
    assert migration_g2_4bn["pre_output_data_quality_gate"] is True
    assert migration_g2_4bn["full_variant_cache_dq_snapshot_required"] is True
    assert migration_g2_4bn["unknown_metrics_must_be_null"] is True
    assert migration_g2_4bn["content_derived_all_views_validation"] is True
    assert migration_g2_4bn["automatic_downstream_simulation_allowed"] is False
    assert migration_g2_4bn["legacy_root_lines_after"] == 24581
    assert migration_g2_4bn["legacy_root_top_level_functions_after"] == 721
    assert migration_g2_4bn["legacy_root_command_decorators_after"] == 682
    assert migration_g2_4bn["python_module_count"] == 878
    assert migration_g2_4bn["production_effect"] == "none"
    if phase_g2_4bn["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4bn["validation"]["focused"]["passed"] == 398
        assert phase_g2_4bn["validation"]["architecture_fitness"]["passed"] == 249
        assert phase_g2_4bn["validation"]["contract_validation"]["passed"] == 203
        assert phase_g2_4bn["sources"]
        for source in phase_g2_4bn["sources"]:
            if source["path"] in set(phase_g2_4bn.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4bo = baseline["phase_g2_4bo_etf_cli_dynamic_v3_backtest_sim_paper"]
    assert phase_g2_4bo["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4bo = phase_g2_4bo["migration"]
    assert migration_g2_4bo["callback_count"] == 3
    assert migration_g2_4bo["canonical_owner"].endswith("dynamic_v3_backtest_sim_paper.py")
    assert migration_g2_4bo["pre_output_variant_content_validation"] is True
    assert migration_g2_4bo["pre_output_data_quality_gate"] is True
    assert migration_g2_4bo["full_variant_cache_dq_snapshot_required"] is True
    assert migration_g2_4bo["unknown_metrics_must_be_null"] is True
    assert migration_g2_4bo["gross_before_costs_disclosure_required"] is True
    assert migration_g2_4bo["content_derived_all_views_validation"] is True
    assert migration_g2_4bo["automatic_downstream_simulation_allowed"] is False
    assert migration_g2_4bo["legacy_root_lines_after"] == 24483
    assert migration_g2_4bo["legacy_root_top_level_functions_after"] == 718
    assert migration_g2_4bo["legacy_root_command_decorators_after"] == 679
    assert migration_g2_4bo["python_module_count"] == 879
    assert migration_g2_4bo["production_effect"] == "none"
    if phase_g2_4bo["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4bo["validation"]["focused"]["passed"] == 411
        assert phase_g2_4bo["validation"]["architecture_fitness"]["passed"] == 250
        assert phase_g2_4bo["validation"]["contract_validation"]["passed"] == 203
        assert phase_g2_4bo["sources"]
        for source in phase_g2_4bo["sources"]:
            if source["path"] in set(phase_g2_4bo.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4bp = baseline["phase_g2_4bp_etf_cli_dynamic_v3_backtest_sim_regime"]
    assert phase_g2_4bp["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4bp = phase_g2_4bp["migration"]
    assert migration_g2_4bp["callback_count"] == 3
    assert migration_g2_4bp["pre_output_outcome_content_validation"] is True
    assert migration_g2_4bp["full_outcome_validation_snapshot_required"] is True
    assert migration_g2_4bp["event_and_window_count_units_distinct"] is True
    assert migration_g2_4bp["missing_metrics_must_be_null"] is True
    assert migration_g2_4bp["content_derived_all_views_validation"] is True
    assert migration_g2_4bp["automatic_downstream_simulation_allowed"] is False
    assert migration_g2_4bp["legacy_root_lines_after"] == 24394
    assert migration_g2_4bp["legacy_root_top_level_functions_after"] == 715
    assert migration_g2_4bp["legacy_root_command_decorators_after"] == 676
    assert migration_g2_4bp["python_module_count"] == 880
    if phase_g2_4bp["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4bp["validation"]["focused"]["passed"] == 423
        assert phase_g2_4bp["validation"]["architecture_fitness"]["passed"] == 251
        assert phase_g2_4bp["validation"]["contract_validation"]["passed"] == 203
        assert phase_g2_4bp["sources"]
        for source in phase_g2_4bp["sources"]:
            if source["path"] in set(phase_g2_4bp.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4bq = baseline["phase_g2_4bq_etf_cli_dynamic_v3_backtest_sim_sensitivity"]
    assert phase_g2_4bq["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4bq = phase_g2_4bq["migration"]
    assert migration_g2_4bq["callback_count"] == 3
    assert migration_g2_4bq["pre_output_outcome_content_validation"] is True
    assert migration_g2_4bq["full_outcome_validation_snapshot_required"] is True
    assert migration_g2_4bq["single_frozen_variant_event_config_cache_lineage"] is True
    assert migration_g2_4bq["exact_unique_policy_grids_required"] is True
    assert migration_g2_4bq["missing_metrics_must_be_null"] is True
    assert migration_g2_4bq["event_window_and_result_units_distinct"] is True
    assert migration_g2_4bq["missing_dispersion_must_be_excluded"] is True
    assert migration_g2_4bq["strong_calibration_low_risk_only"] is True
    assert migration_g2_4bq["content_derived_all_views_validation"] is True
    assert migration_g2_4bq["automatic_downstream_simulation_allowed"] is False
    assert migration_g2_4bq["legacy_root_lines_after"] == 24296
    assert migration_g2_4bq["legacy_root_top_level_functions_after"] == 712
    assert migration_g2_4bq["legacy_root_command_decorators_after"] == 673
    assert migration_g2_4bq["python_module_count"] == 881
    if phase_g2_4bq["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4bq["validation"]["focused"]["passed"] == 437
        assert phase_g2_4bq["validation"]["architecture_fitness"]["passed"] == 252
        assert phase_g2_4bq["validation"]["contract_validation"]["passed"] == 203
        assert phase_g2_4bq["sources"]
        for source in phase_g2_4bq["sources"]:
            if source["path"] in set(phase_g2_4bq.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4br = baseline["phase_g2_4br_etf_cli_dynamic_v3_backtest_sim_calibration"]
    assert phase_g2_4br["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4br = phase_g2_4br["migration"]
    assert migration_g2_4br["callback_count"] == 3
    assert migration_g2_4br["four_source_content_validation_required"] is True
    assert migration_g2_4br["full_source_bundles_and_validations_snapshot_required"] is True
    assert migration_g2_4br["cross_source_lineage_required"] is True
    assert migration_g2_4br["missing_metrics_must_be_null"] is True
    assert migration_g2_4br["positive_proposal_low_risk_only"] is True
    assert migration_g2_4br["content_derived_all_views_validation"] is True
    assert migration_g2_4br["automatic_downstream_simulation_allowed"] is False
    assert migration_g2_4br["legacy_root_lines_after"] == 24180
    assert migration_g2_4br["legacy_root_top_level_functions_after"] == 709
    assert migration_g2_4br["legacy_root_command_decorators_after"] == 670
    assert migration_g2_4br["python_module_count"] == 882
    if phase_g2_4br["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4br["validation"]["focused"]["passed"] == 451
        assert phase_g2_4br["validation"]["architecture_fitness"]["passed"] == 253
        assert phase_g2_4br["validation"]["contract_validation"]["passed"] == 203
        assert phase_g2_4br["sources"]
        for source in phase_g2_4br["sources"]:
            if source["path"] in set(phase_g2_4br.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4bs = baseline["phase_g2_4bs_etf_cli_dynamic_v3_backtest_sim_forward_bridge"]
    assert phase_g2_4bs["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4bs = phase_g2_4bs["migration"]
    assert migration_g2_4bs["callback_count"] == 3
    assert migration_g2_4bs["calibration_content_validation_required"] is True
    assert migration_g2_4bs["full_calibration_bundle_and_validation_snapshot_required"] is True
    assert migration_g2_4bs["reviewed_forward_policy_required"] is True
    assert migration_g2_4bs["policy_numeric_fallback_allowed"] is False
    assert migration_g2_4bs["tracking_plan_only"] is True
    assert migration_g2_4bs["content_derived_all_views_validation"] is True
    assert migration_g2_4bs["automatic_downstream_simulation_allowed"] is False
    assert migration_g2_4bs["legacy_root_lines_after"] == 24096
    assert migration_g2_4bs["legacy_root_top_level_functions_after"] == 706
    assert migration_g2_4bs["legacy_root_command_decorators_after"] == 667
    assert migration_g2_4bs["python_module_count"] == 883
    if phase_g2_4bs["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4bs["validation"]["focused"]["passed"] > 451
        assert phase_g2_4bs["validation"]["architecture_fitness"]["passed"] > 253
        assert phase_g2_4bs["validation"]["contract_validation"]["passed"] == 203
        assert phase_g2_4bs["sources"]
        for source in phase_g2_4bs["sources"]:
            if source["path"] in set(phase_g2_4bs.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4bt = baseline["phase_g2_4bt_etf_cli_dynamic_v3_sim_interpretation"]
    assert phase_g2_4bt["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4bt = phase_g2_4bt["migration"]
    assert migration_g2_4bt["callback_count"] == 3
    assert migration_g2_4bt["three_source_content_validation_required"] is True
    assert migration_g2_4bt["same_outcome_lineage_required"] is True
    assert migration_g2_4bt["full_source_bundles_and_validations_snapshot_required"] is True
    assert migration_g2_4bt["paired_available_finite_cohort_required"] is True
    assert migration_g2_4bt["missing_metrics_must_be_null"] is True
    assert migration_g2_4bt["tracking_plan_is_not_forward_success"] is True
    assert migration_g2_4bt["content_derived_all_views_validation"] is True
    assert migration_g2_4bt["automatic_downstream_simulation_allowed"] is False
    assert migration_g2_4bt["legacy_root_lines_after"] == 23994
    assert migration_g2_4bt["legacy_root_top_level_functions_after"] == 703
    assert migration_g2_4bt["legacy_root_command_decorators_after"] == 664
    assert migration_g2_4bt["python_module_count"] == 884
    if phase_g2_4bt["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4bt["validation"]["focused"]["passed"] > 464
        assert phase_g2_4bt["validation"]["architecture_fitness"]["passed"] > 254
        assert phase_g2_4bt["validation"]["contract_validation"]["passed"] == 203
        assert phase_g2_4bt["sources"]
        for source in phase_g2_4bt["sources"]:
            if source["path"] in set(phase_g2_4bt.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4bu = baseline["phase_g2_4bu_etf_cli_dynamic_v3_sim_risk_return"]
    assert phase_g2_4bu["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4bu = phase_g2_4bu["migration"]
    assert migration_g2_4bu["callback_count"] == 3
    assert migration_g2_4bu["outcome_content_validation_required"] is True
    assert migration_g2_4bu["full_outcome_bundle_and_validation_snapshot_required"] is True
    assert migration_g2_4bu["same_event_20d_available_finite_pairs_required"] is True
    assert migration_g2_4bu["paired_event_and_window_counts_required"] is True
    assert migration_g2_4bu["missing_metrics_and_undefined_ratios_must_be_null"] is True
    assert migration_g2_4bu["content_derived_all_views_validation"] is True
    assert migration_g2_4bu["automatic_downstream_simulation_allowed"] is False
    assert migration_g2_4bu["legacy_root_lines_after"] == 23911
    assert migration_g2_4bu["legacy_root_top_level_functions_after"] == 700
    assert migration_g2_4bu["legacy_root_command_decorators_after"] == 661
    assert migration_g2_4bu["python_module_count"] == 885
    if phase_g2_4bu["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4bu["validation"]["focused"]["passed"] > 475
        assert phase_g2_4bu["validation"]["architecture_fitness"]["passed"] > 255
        assert phase_g2_4bu["validation"]["contract_validation"]["passed"] == 203
        assert phase_g2_4bu["sources"]
        for source in phase_g2_4bu["sources"]:
            if source["path"] in set(phase_g2_4bu.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4bv = baseline["phase_g2_4bv_etf_cli_dynamic_v3_sim_defensive_validation"]
    assert phase_g2_4bv["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4bv = phase_g2_4bv["migration"]
    assert migration_g2_4bv["callback_count"] == 3
    assert migration_g2_4bv["outcome_content_validation_required"] is True
    assert migration_g2_4bv["full_outcome_bundle_validation_and_policy_snapshot_required"] is True
    assert migration_g2_4bv["same_regime_event_window_available_finite_pairs_required"] is True
    assert migration_g2_4bv["paired_event_and_window_counts_required"] is True
    assert migration_g2_4bv["missing_metrics_must_be_null"] is True
    assert migration_g2_4bv["reviewed_defensive_policy_required"] is True
    assert migration_g2_4bv["content_derived_all_views_validation"] is True
    assert migration_g2_4bv["automatic_downstream_simulation_allowed"] is False
    assert migration_g2_4bv["legacy_root_lines_after"] == 23815
    assert migration_g2_4bv["legacy_root_top_level_functions_after"] == 697
    assert migration_g2_4bv["legacy_root_command_decorators_after"] == 658
    assert migration_g2_4bv["python_module_count"] == 886
    if phase_g2_4bv["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4bv["validation"]["focused"]["passed"] > 486
        assert phase_g2_4bv["validation"]["architecture_fitness"]["passed"] > 256
        assert phase_g2_4bv["validation"]["contract_validation"]["passed"] == 203
        assert phase_g2_4bv["sources"]
        for source in phase_g2_4bv["sources"]:
            if source["path"] in set(phase_g2_4bv.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4bw = baseline["phase_g2_4bw_etf_cli_dynamic_v3_advisory_proposal_review"]
    assert phase_g2_4bw["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4bw = phase_g2_4bw["migration"]
    assert migration_g2_4bw["callback_count"] == 3
    assert migration_g2_4bw["four_source_content_validation_required"] is True
    assert migration_g2_4bw["source_generated_cutoff_and_same_outcome_lineage_required"] is True
    assert migration_g2_4bw["full_source_bundles_validations_and_policy_snapshot_required"] is True
    assert migration_g2_4bw["fabricated_proposal_or_confidence_allowed"] is False
    assert migration_g2_4bw["empty_proposals_are_insufficient_data"] is True
    assert migration_g2_4bw["reviewed_proposal_policy_required"] is True
    assert migration_g2_4bw["content_derived_all_views_validation"] is True
    assert migration_g2_4bw["automatic_downstream_simulation_allowed"] is False
    assert migration_g2_4bw["legacy_root_lines_after"] == 23688
    assert migration_g2_4bw["legacy_root_top_level_functions_after"] == 694
    assert migration_g2_4bw["legacy_root_command_decorators_after"] == 655
    assert migration_g2_4bw["python_module_count"] == 887
    if phase_g2_4bw["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4bw["validation"]["focused"]["passed"] > 500
        assert phase_g2_4bw["validation"]["architecture_fitness"]["passed"] > 257
        assert phase_g2_4bw["validation"]["contract_validation"]["passed"] == 203
        assert phase_g2_4bw["sources"]
        for source in phase_g2_4bw["sources"]:
            if source["path"] in set(phase_g2_4bw.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4bx = baseline["phase_g2_4bx_etf_cli_dynamic_v3_forward_confirmation_plan"]
    assert phase_g2_4bx["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4bx = phase_g2_4bx["migration"]
    assert migration_g2_4bx["callback_count"] == 3
    assert migration_g2_4bx["two_source_content_validation_required"] is True
    assert migration_g2_4bx["source_generated_cutoff_and_same_calibration_lineage_required"] is True
    assert migration_g2_4bx["full_source_bundles_validations_and_policy_snapshot_required"] is True
    assert migration_g2_4bx["fabricated_target_or_numeric_criterion_allowed"] is False
    assert migration_g2_4bx["empty_or_unmatched_proposals_are_insufficient_data"] is True
    assert migration_g2_4bx["bridge_criteria_exact_inheritance_required"] is True
    assert migration_g2_4bx["reviewed_semantic_policy_required"] is True
    assert migration_g2_4bx["content_derived_all_views_validation"] is True
    assert migration_g2_4bx["automatic_target_registration_or_forward_run_allowed"] is False
    assert migration_g2_4bx["legacy_root_lines_after"] == 23584
    assert migration_g2_4bx["legacy_root_top_level_functions_after"] == 691
    assert migration_g2_4bx["legacy_root_command_decorators_after"] == 652
    assert migration_g2_4bx["python_module_count"] == 888
    if phase_g2_4bx["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4bx["validation"]["focused"]["passed"] > 514
        assert phase_g2_4bx["validation"]["architecture_fitness"]["passed"] > 258
        assert phase_g2_4bx["validation"]["contract_validation"]["passed"] == 203
        assert phase_g2_4bx["sources"]
        for source in phase_g2_4bx["sources"]:
            if source["path"] in set(phase_g2_4bx.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4by = baseline["phase_g2_4by_etf_cli_dynamic_v3_confirmation_targets"]
    assert phase_g2_4by["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4by = phase_g2_4by["migration"]
    assert migration_g2_4by["callback_count"] == 4
    assert migration_g2_4by["plan_content_validation_and_available_status_required"] is True
    assert migration_g2_4by["source_generated_cutoff_required"] is True
    assert (
        migration_g2_4by["full_plan_bundle_validation_and_registry_preimage_snapshot_required"]
        is True
    )
    assert migration_g2_4by["source_exact_targets_status_criteria_and_failures_required"] is True
    assert migration_g2_4by["duplicate_plan_registration_allowed"] is False
    assert migration_g2_4by["canonical_atomic_materialized_registry_required"] is True
    assert migration_g2_4by["content_derived_all_views_validation"] is True
    assert migration_g2_4by["automatic_progress_or_evaluation_allowed"] is False
    assert migration_g2_4by["legacy_root_lines_after"] == 23439
    assert migration_g2_4by["legacy_root_top_level_functions_after"] == 687
    assert migration_g2_4by["legacy_root_command_decorators_after"] == 648
    assert migration_g2_4by["python_module_count"] == 889
    if phase_g2_4by["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4by["validation"]["focused"]["passed"] > 529
        assert phase_g2_4by["validation"]["architecture_fitness"]["passed"] > 259
        assert phase_g2_4by["validation"]["contract_validation"]["passed"] == 203
        assert phase_g2_4by["sources"]
        for source in phase_g2_4by["sources"]:
            if source["path"] in set(phase_g2_4by.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4bz = baseline["phase_g2_4bz_etf_cli_dynamic_v3_confirmation_progress"]
    assert phase_g2_4bz["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4bz = phase_g2_4bz["migration"]
    assert migration_g2_4bz["callback_count"] == 3
    assert migration_g2_4bz["registry_content_validation_and_cutoff_required"] is True
    assert migration_g2_4bz["deterministic_validated_evidence_selection_required"] is True
    assert migration_g2_4bz["full_source_bundle_validation_snapshot_required"] is True
    assert migration_g2_4bz["source_exact_events_windows_and_criteria_required"] is True
    assert migration_g2_4bz["cross_window_event_double_count_allowed"] is False
    assert migration_g2_4bz["missing_metrics_must_remain_null"] is True
    assert migration_g2_4bz["ungoverned_near_ready_threshold_allowed"] is False
    assert migration_g2_4bz["content_derived_all_views_validation"] is True
    assert migration_g2_4bz["automatic_evaluation_allowed"] is False
    assert migration_g2_4bz["legacy_root_lines_after"] == 23336
    assert migration_g2_4bz["legacy_root_top_level_functions_after"] == 684
    assert migration_g2_4bz["legacy_root_command_decorators_after"] == 645
    assert migration_g2_4bz["python_module_count"] == 890
    if phase_g2_4bz["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4bz["validation"]["focused"]["passed"] > 540
        assert phase_g2_4bz["validation"]["architecture_fitness"]["passed"] > 260
        assert phase_g2_4bz["validation"]["contract_validation"]["passed"] == 203
        assert phase_g2_4bz["sources"]
        for source in phase_g2_4bz["sources"]:
            if source["path"] in set(phase_g2_4bz.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4ca = baseline["phase_g2_4ca_etf_cli_dynamic_v3_confirmation_evaluation"]
    assert phase_g2_4ca["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4ca = phase_g2_4ca["migration"]
    assert migration_g2_4ca["callback_count"] == 3
    assert migration_g2_4ca["progress_content_validation_and_cutoff_required"] is True
    assert migration_g2_4ca["full_progress_bundle_validation_snapshot_required"] is True
    assert migration_g2_4ca["not_ready_partial_criteria_evaluation_allowed"] is False
    assert migration_g2_4ca["ready_source_exact_finite_criteria_required"] is True
    assert migration_g2_4ca["all_criteria_pass_and_no_failure_required_for_success"] is True
    assert migration_g2_4ca["failure_boundary_must_derive_from_source_criterion"] is True
    assert migration_g2_4ca["unknown_failure_condition_allowed"] is False
    assert migration_g2_4ca["content_derived_all_views_validation"] is True
    assert migration_g2_4ca["automatic_rule_review_allowed"] is False
    assert migration_g2_4ca["legacy_root_lines_after"] == 23246
    assert migration_g2_4ca["legacy_root_top_level_functions_after"] == 681
    assert migration_g2_4ca["legacy_root_command_decorators_after"] == 642
    assert migration_g2_4ca["python_module_count"] == 891
    if phase_g2_4ca["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4ca["validation"]["focused"]["passed"] > 550
        assert phase_g2_4ca["validation"]["architecture_fitness"]["passed"] > 261
        assert phase_g2_4ca["validation"]["contract_validation"]["passed"] == 203
        assert phase_g2_4ca["sources"]
        for source in phase_g2_4ca["sources"]:
            if source["path"] in set(phase_g2_4ca.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4cb = baseline["phase_g2_4cb_etf_cli_dynamic_v3_rule_review_cycle"]
    assert phase_g2_4cb["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4cb = phase_g2_4cb["migration"]
    assert migration_g2_4cb["callback_count"] == 3
    assert migration_g2_4cb["three_source_content_validation_and_cutoff_required"] is True
    assert (
        migration_g2_4cb["strict_registry_progress_evaluation_lineage_and_chronology_required"]
        is True
    )
    assert migration_g2_4cb["exact_target_coverage_required"] is True
    assert migration_g2_4cb["bounded_full_byte_commitment_bundle_required"] is True
    assert migration_g2_4cb["source_failure_actions_preserved"] is True
    assert migration_g2_4cb["target_id_semantic_override_allowed"] is False
    assert migration_g2_4cb["content_derived_all_views_validation"] is True
    assert migration_g2_4cb["automatic_owner_decision_allowed"] is False
    assert migration_g2_4cb["legacy_root_lines_after"] == 23136
    assert migration_g2_4cb["legacy_root_top_level_functions_after"] == 678
    assert migration_g2_4cb["legacy_root_command_decorators_after"] == 639
    assert migration_g2_4cb["python_module_count"] == 892
    if phase_g2_4cb["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4cb["validation"]["focused"]["passed"] > 563
        assert phase_g2_4cb["validation"]["architecture_fitness"]["passed"] > 262
        assert phase_g2_4cb["validation"]["contract_validation"]["passed"] == 203
        assert phase_g2_4cb["sources"]
        for source in phase_g2_4cb["sources"]:
            if source["path"] in set(phase_g2_4cb.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4cc = baseline["phase_g2_4cc_etf_cli_dynamic_v3_rule_owner_decision"]
    assert phase_g2_4cc["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4cc = phase_g2_4cc["migration"]
    assert migration_g2_4cc["callback_count"] == 5
    assert migration_g2_4cc["rule_review_content_validation_and_cutoff_required"] is True
    assert migration_g2_4cc["bounded_cycle_snapshot_and_exact_scope_required"] is True
    assert migration_g2_4cc["one_decision_per_cycle_required"] is True
    assert migration_g2_4cc["append_only_event_sha256_chain_required"] is True
    assert (
        migration_g2_4cc["pending_single_final_transition_and_strict_chronology_required"] is True
    )
    assert migration_g2_4cc["evidence_bound_decision_eligibility_required"] is True
    assert migration_g2_4cc["legacy_unsnapshotted_write_allowed"] is False
    assert migration_g2_4cc["content_derived_all_views_validation"] is True
    assert migration_g2_4cc["automatic_policy_or_execution_allowed"] is False
    assert migration_g2_4cc["legacy_root_lines_after"] == 22981
    assert migration_g2_4cc["legacy_root_top_level_functions_after"] == 673
    assert migration_g2_4cc["legacy_root_command_decorators_after"] == 634
    assert migration_g2_4cc["python_module_count"] == 893
    if phase_g2_4cc["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4cc["validation"]["focused"]["passed"] > 570
        assert phase_g2_4cc["validation"]["architecture_fitness"]["passed"] > 263
        assert phase_g2_4cc["validation"]["contract_validation"]["passed"] == 203
        assert phase_g2_4cc["sources"]
        for source in phase_g2_4cc["sources"]:
            if source["path"] in set(phase_g2_4cc.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4cd = baseline["phase_g2_4cd_etf_cli_dynamic_v3_confirmation_operations"]
    assert phase_g2_4cd["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4cd = phase_g2_4cd["migration"]
    assert migration_g2_4cd["callback_count"] == 16
    assert migration_g2_4cd["pre_output_timezone_and_cutoff_required"] is True
    assert migration_g2_4cd["schedule_config_and_source_content_validation_required"] is True
    assert migration_g2_4cd["semantic_latest_selection_without_mtime_required"] is True
    assert migration_g2_4cd["bounded_source_commitment_snapshots_required"] is True
    assert migration_g2_4cd["weekly_step_chain_validation_required"] is True
    assert migration_g2_4cd["optional_absence_and_invalid_source_distinct"] is True
    assert migration_g2_4cd["dashboard_progress_readiness_override_allowed"] is False
    assert migration_g2_4cd["queue_cross_cycle_owner_decision_allowed"] is False
    assert migration_g2_4cd["pressure_pending_outcome_counts_as_defensive_evidence"] is False
    assert migration_g2_4cd["content_derived_all_views_validation"] is True
    assert migration_g2_4cd["default_weekly_mode"] == "dry_run"
    assert migration_g2_4cd["automatic_policy_or_execution_allowed"] is False
    assert migration_g2_4cd["legacy_root_lines_after"] == 22538
    assert migration_g2_4cd["legacy_root_top_level_functions_after"] == 657
    assert migration_g2_4cd["legacy_root_command_decorators_after"] == 618
    assert migration_g2_4cd["python_module_count"] == 894
    if phase_g2_4cd["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4cd["validation"]["focused"]["passed"] > 578
        assert phase_g2_4cd["validation"]["architecture_fitness"]["passed"] > 264
        assert phase_g2_4cd["validation"]["contract_validation"]["passed"] == 203
        assert phase_g2_4cd["sources"]
        for source in phase_g2_4cd["sources"]:
            if source["path"] in set(phase_g2_4cd.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4ce = baseline["phase_g2_4ce_etf_cli_dynamic_v3_pressure_validation"]
    assert phase_g2_4ce["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4ce = phase_g2_4ce["migration"]
    assert migration_g2_4ce["callback_count"] == 15
    assert migration_g2_4ce["pre_output_timezone_and_cutoff_required"] is True
    assert migration_g2_4ce["semantic_latest_selection_without_mtime_required"] is True
    assert migration_g2_4ce["content_derived_consumer_validation_required"] is True
    assert migration_g2_4ce["bounded_source_and_policy_commitments_required"] is True
    assert migration_g2_4ce["available_finite_unique_paired_evidence_only"] is True
    assert migration_g2_4ce["missing_metrics_must_remain_null"] is True
    assert migration_g2_4ce["policy_governed_distinct_event_floor_and_boundaries"] is True
    assert (
        migration_g2_4ce["all_configured_pressure_regimes_required_for_source_conclusion"] is True
    )
    assert migration_g2_4ce["weekly_distinct_forward_event_count_required"] is True
    assert migration_g2_4ce["downstream_pressure_capture_source_roots_explicit"] is True
    assert migration_g2_4ce["simulation_and_historical_evidence_research_only"] is True
    assert migration_g2_4ce["weekly_exact_lineage_and_chronology_required"] is True
    assert migration_g2_4ce["rule_approval_allowed"] is False
    assert migration_g2_4ce["auto_apply_allowed"] is False
    assert migration_g2_4ce["content_derived_all_views_validation"] is True
    assert migration_g2_4ce["automatic_policy_or_execution_allowed"] is False
    assert migration_g2_4ce["legacy_root_lines_after"] == 22069
    assert migration_g2_4ce["legacy_root_top_level_functions_after"] == 642
    assert migration_g2_4ce["legacy_root_command_decorators_after"] == 603
    assert migration_g2_4ce["python_module_count"] == 895
    if phase_g2_4ce["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4ce["validation"]["focused"]["passed"] > 580
        assert phase_g2_4ce["validation"]["architecture_fitness"]["passed"] > 265
        assert phase_g2_4ce["validation"]["contract_validation"]["passed"] == 203
        assert phase_g2_4ce["sources"]
        for source in phase_g2_4ce["sources"]:
            if source["path"] in set(phase_g2_4ce.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4cf = baseline["phase_g2_4cf_etf_cli_dynamic_v3_defensive_research_synthesis"]
    assert phase_g2_4cf["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4cf = phase_g2_4cf["migration"]
    assert migration_g2_4cf["callback_count"] == 15
    assert migration_g2_4cf["pre_output_timezone_and_cutoff_required"] is True
    assert migration_g2_4cf["content_derived_consumer_validation_required"] is True
    assert migration_g2_4cf["strict_same_lineage_and_chronology_required"] is True
    assert migration_g2_4cf["bounded_source_and_policy_commitments_required"] is True
    assert migration_g2_4cf["missing_metrics_must_remain_null"] is True
    assert migration_g2_4cf["policy_governed_research_interpretation"] is True
    assert migration_g2_4cf["simulation_can_support_rule_approval"] is False
    assert migration_g2_4cf["rename_or_mitigation_auto_apply_allowed"] is False
    assert migration_g2_4cf["content_derived_all_views_validation"] is True
    assert migration_g2_4cf["automatic_policy_or_execution_allowed"] is False
    assert migration_g2_4cf["legacy_root_lines_after"] == 21696
    assert migration_g2_4cf["legacy_root_top_level_functions_after"] == 627
    assert migration_g2_4cf["legacy_root_command_decorators_after"] == 588
    assert migration_g2_4cf["python_module_count"] == 897
    if phase_g2_4cf["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4cf["validation"]["focused"]["passed"] > 590
        assert phase_g2_4cf["validation"]["downstream_compatibility"]["passed"] >= 5
        assert phase_g2_4cf["validation"]["architecture_fitness"]["passed"] >= 266
        assert phase_g2_4cf["validation"]["contract_validation"]["passed"] == 203
        assert phase_g2_4cf["sources"]
        for source in phase_g2_4cf["sources"]:
            if source["path"] in set(phase_g2_4cf.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4cg = baseline["phase_g2_4cg_etf_cli_dynamic_v3_forward_pressure_evidence"]
    assert phase_g2_4cg["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4cg = phase_g2_4cg["migration"]
    assert migration_g2_4cg["callback_count"] == 15
    assert migration_g2_4cg["reviewed_capture_policy_required"] is True
    assert migration_g2_4cg["pre_output_data_quality_and_cutoff_required"] is True
    assert migration_g2_4cg["trigger_live_cache_and_policy_recompute_required"] is True
    assert migration_g2_4cg["exact_trigger_tag_backfill_compare_lineage_required"] is True
    assert migration_g2_4cg["semantic_cutoff_source_selection_required"] is True
    assert migration_g2_4cg["distinct_source_event_sample_unit_required"] is True
    assert migration_g2_4cg["bounded_source_policy_cache_snapshots_required"] is True
    assert migration_g2_4cg["explicit_test_fixture_dq_skip_only"] is True
    assert migration_g2_4cg["content_derived_all_views_validation"] is True
    assert migration_g2_4cg["automatic_policy_or_execution_allowed"] is False
    assert migration_g2_4cg["portfolio_or_execution_effect"] is False
    assert migration_g2_4cg["legacy_root_lines_after"] == 21286
    assert migration_g2_4cg["legacy_root_top_level_functions_after"] == 612
    assert migration_g2_4cg["legacy_root_command_decorators_after"] == 573
    assert migration_g2_4cg["python_module_count"] == 899
    assert migration_g2_4cg["python_test_file_count"] == 1114
    if phase_g2_4cg["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4cg["validation"]["focused"]["passed"] > 601
        assert phase_g2_4cg["validation"]["current_slice_and_cli_contract"]["passed"] >= 117
        assert phase_g2_4cg["validation"]["architecture_fitness"]["passed"] >= 268
        assert phase_g2_4cg["validation"]["contract_validation"]["passed"] == 203
        assert phase_g2_4cg["sources"]
        for source in phase_g2_4cg["sources"]:
            if source["path"] in set(phase_g2_4cg.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4ch = baseline["phase_g2_4ch_etf_cli_dynamic_v3_system_target_portfolio"]
    assert phase_g2_4ch["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4ch = phase_g2_4ch["migration"]
    assert migration_g2_4ch["callback_count"] == 17
    assert migration_g2_4ch["reviewed_target_and_paper_policy_required"] is True
    assert migration_g2_4ch["pre_output_source_validation_and_cutoff_required"] is True
    assert migration_g2_4ch["semantic_zero_or_one_selection_required"] is True
    assert migration_g2_4ch["fabricated_target_fallback_allowed"] is False
    assert migration_g2_4ch["immutable_paper_source_state_required"] is True
    assert migration_g2_4ch["append_only_rebalance_post_state_required"] is True
    assert migration_g2_4ch["duplicate_paper_target_allowed"] is False
    assert migration_g2_4ch["common_finite_date_performance_required"] is True
    assert migration_g2_4ch["missing_metrics_must_remain_null"] is True
    assert migration_g2_4ch["data_quality_and_cache_commitments_required"] is True
    assert migration_g2_4ch["exact_target_paper_performance_lineage_required"] is True
    assert migration_g2_4ch["performance_winner_may_equal_observation_priority"] is False
    assert migration_g2_4ch["bounded_source_policy_cache_snapshots_required"] is True
    assert migration_g2_4ch["content_derived_all_views_validation"] is True
    assert migration_g2_4ch["automatic_policy_or_execution_allowed"] is False
    assert migration_g2_4ch["portfolio_or_execution_effect"] is False
    assert migration_g2_4ch["legacy_root_lines_after"] == 20841
    assert migration_g2_4ch["legacy_root_top_level_functions_after"] == 595
    assert migration_g2_4ch["legacy_root_command_decorators_after"] == 556
    assert migration_g2_4ch["legacy_domain_lines_after"] == 27838
    assert migration_g2_4ch["legacy_domain_top_level_functions_after"] == 804
    assert migration_g2_4ch["python_module_count"] == 901
    assert migration_g2_4ch["python_test_file_count"] == 1114
    if phase_g2_4ch["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4ch["validation"]["focused"]["passed"] >= 225
        assert phase_g2_4ch["validation"]["core_positive_and_negative"]["passed"] >= 11
        assert phase_g2_4ch["validation"]["architecture_fitness"]["passed"] >= 269
        assert phase_g2_4ch["validation"]["contract_validation"]["passed"] == 203
        assert phase_g2_4ch["sources"]
        for source in phase_g2_4ch["sources"]:
            if source["path"] in set(phase_g2_4ch.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4ci = baseline["phase_g2_4ci_etf_cli_dynamic_v3_system_target_history"]
    assert phase_g2_4ci["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4ci = phase_g2_4ci["migration"]
    assert migration_g2_4ci["callback_count"] == 16
    assert migration_g2_4ci["validated_model_target_only"] is True
    assert migration_g2_4ci["mutable_latest_or_baseline_fallback_allowed"] is False
    assert migration_g2_4ci["common_finite_duplicate_free_dates_required"] is True
    assert migration_g2_4ci["versioned_costs_applied"] is True
    assert migration_g2_4ci["current_definition_replayed_historically"] is True
    assert migration_g2_4ci["pit_safe"] is False
    assert migration_g2_4ci["missing_metrics_must_remain_null"] is True
    assert migration_g2_4ci["regime_and_rank_thresholds_policy_governed"] is True
    assert migration_g2_4ci["exact_same_backfill_lineage_required"] is True
    assert migration_g2_4ci["reference_only_recommendation_allowed"] is False
    assert migration_g2_4ci["bounded_source_policy_cache_snapshots_required"] is True
    assert migration_g2_4ci["content_derived_all_views_validation"] is True
    assert migration_g2_4ci["automatic_policy_or_execution_allowed"] is False
    assert migration_g2_4ci["portfolio_or_execution_effect"] is False
    assert migration_g2_4ci["legacy_root_lines_after"] == 20437
    assert migration_g2_4ci["legacy_root_top_level_functions_after"] == 579
    assert migration_g2_4ci["legacy_root_command_decorators_after"] == 540
    assert migration_g2_4ci["legacy_domain_lines_after"] == 27034
    assert migration_g2_4ci["legacy_domain_top_level_functions_after"] == 806
    assert migration_g2_4ci["python_module_count"] == 903
    assert migration_g2_4ci["python_test_file_count"] == 1114
    if phase_g2_4ci["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4ci["validation"]["focused"]["passed"] >= 33
        assert phase_g2_4ci["validation"]["core_positive_and_negative"]["passed"] >= 10
        assert phase_g2_4ci["validation"]["architecture_fitness"]["passed"] >= 270
        assert phase_g2_4ci["validation"]["contract_validation"]["passed"] >= 203
        assert phase_g2_4ci["sources"]
        for source in phase_g2_4ci["sources"]:
            if source["path"] in set(phase_g2_4ci.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4cj = baseline["phase_g2_4cj_etf_cli_dynamic_v3_system_target_hardening"]
    assert phase_g2_4cj["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4cj = phase_g2_4cj["migration"]
    assert migration_g2_4cj["callback_count"] == 15
    assert migration_g2_4cj["pre_output_live_source_validation_required"] is True
    assert migration_g2_4cj["implicit_upstream_run_allowed"] is False
    assert migration_g2_4cj["missing_metrics_must_remain_null"] is True
    assert migration_g2_4cj["data_quality_penalty_scored"] is False
    assert migration_g2_4cj["exact_same_backfill_lineage_required"] is True
    assert migration_g2_4cj["warning_detail_missing_is_unknown"] is True
    assert migration_g2_4cj["hardening_same_selection_lineage_required"] is True
    assert migration_g2_4cj["workflow_pass_is_investment_conclusion"] is False
    assert migration_g2_4cj["bounded_source_policy_snapshots_required"] is True
    assert migration_g2_4cj["content_derived_all_views_validation"] is True
    assert migration_g2_4cj["automatic_policy_or_execution_allowed"] is False
    assert migration_g2_4cj["portfolio_or_execution_effect"] is False
    assert migration_g2_4cj["legacy_root_lines_after"] == 20017
    assert migration_g2_4cj["legacy_root_top_level_functions_after"] == 564
    assert migration_g2_4cj["legacy_root_command_decorators_after"] == 525
    assert migration_g2_4cj["legacy_domain_lines_after"] == 26087
    assert migration_g2_4cj["legacy_domain_top_level_functions_after"] == 801
    assert migration_g2_4cj["python_module_count"] == 905
    assert migration_g2_4cj["python_test_file_count"] == 1114
    if phase_g2_4cj["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4cj["validation"]["focused"]["passed"] >= 12
        assert phase_g2_4cj["validation"]["core_positive_and_negative"]["passed"] >= 7
        assert phase_g2_4cj["validation"]["downstream_compatibility"]["passed"] >= 5
        assert phase_g2_4cj["validation"]["architecture_fitness"]["passed"] >= 271
        assert phase_g2_4cj["validation"]["contract_validation"]["passed"] >= 203
        assert phase_g2_4cj["sources"]
        for source in phase_g2_4cj["sources"]:
            if source["path"] in set(phase_g2_4cj.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    phase_g2_4ck = baseline["phase_g2_4ck_etf_cli_dynamic_v3_system_target_refinement"]
    assert phase_g2_4ck["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4ck = phase_g2_4ck["migration"]
    assert migration_g2_4ck["callback_count"] == 15
    assert migration_g2_4ck["pre_output_live_source_validation_required"] is True
    assert migration_g2_4ck["implicit_upstream_run_allowed"] is False
    assert migration_g2_4ck["exact_same_backfill_and_selection_lineage_required"] is True
    assert migration_g2_4ck["source_chronology_required"] is True
    assert migration_g2_4ck["reviewed_method_refinement_policy_required"] is True
    assert migration_g2_4ck["risk_data_quality_and_cache_commitments_required"] is True
    assert migration_g2_4ck["common_finite_price_dates_required"] is True
    assert migration_g2_4ck["first_or_missing_return_may_be_filled_zero"] is False
    assert migration_g2_4ck["missing_metrics_must_remain_null"] is True
    assert migration_g2_4ck["conceptual_metrics_must_remain_null_or_unknown"] is True
    assert migration_g2_4ck["overlapping_risk_windows_are_independent_samples"] is False
    assert migration_g2_4ck["bounded_source_policy_cache_snapshots_required"] is True
    assert migration_g2_4ck["content_derived_all_views_validation"] is True
    assert migration_g2_4ck["automatic_policy_or_execution_allowed"] is False
    assert migration_g2_4ck["portfolio_or_execution_effect"] is False
    assert migration_g2_4ck["legacy_root_lines_after"] == 19586
    assert migration_g2_4ck["legacy_root_top_level_functions_after"] == 549
    assert migration_g2_4ck["legacy_root_command_decorators_after"] == 510
    assert migration_g2_4ck["legacy_domain_lines_after"] == 25367
    assert migration_g2_4ck["legacy_domain_top_level_functions_after"] == 801
    assert migration_g2_4ck["python_module_count"] == 907
    assert migration_g2_4ck["python_test_file_count"] == 1114
    if phase_g2_4ck["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4ck["validation"]["focused"]["passed"] >= 7
        assert phase_g2_4ck["validation"]["current_slice_and_cli_contract"]["passed"] >= 110
        assert phase_g2_4ck["validation"]["architecture_fitness"]["passed"] >= 272
        assert phase_g2_4ck["validation"]["contract_validation"]["passed"] >= 203
        assert phase_g2_4ck["sources"]
        for source in phase_g2_4ck["sources"]:
            if source["path"] in set(phase_g2_4ck.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    phase_g2_4cl = baseline["phase_g2_4cl_etf_cli_dynamic_v3_system_target_risk_capped"]
    assert phase_g2_4cl["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4cl = phase_g2_4cl["migration"]
    assert migration_g2_4cl["callback_count"] == 15
    assert migration_g2_4cl["pre_output_live_source_validation_required"] is True
    assert migration_g2_4cl["canonical_paper_backfill_orchestration_allowed"] is True
    assert migration_g2_4cl["all_other_implicit_upstream_run_allowed"] is False
    assert migration_g2_4cl["exact_same_backfill_lineage_required"] is True
    assert migration_g2_4cl["source_chronology_required"] is True
    assert migration_g2_4cl["reviewed_evaluation_policy_required"] is True
    assert migration_g2_4cl["risk_data_quality_and_cache_commitments_required"] is True
    assert migration_g2_4cl["duplicate_risk_method_observations_allowed"] is False
    assert migration_g2_4cl["missing_regime_metrics_must_remain_null"] is True
    assert migration_g2_4cl["bounded_source_policy_cache_snapshots_required"] is True
    assert len(migration_g2_4cl["snapshot_schemas"]) == 5
    assert migration_g2_4cl["content_derived_all_views_validation"] is True
    assert migration_g2_4cl["automatic_policy_or_execution_allowed"] is False
    assert migration_g2_4cl["portfolio_or_execution_effect"] is False
    assert migration_g2_4cl["legacy_root_lines_after"] == 19197
    assert migration_g2_4cl["legacy_root_top_level_functions_after"] == 534
    assert migration_g2_4cl["legacy_root_command_decorators_after"] == 495
    assert migration_g2_4cl["legacy_domain_lines_after"] == 24598
    assert migration_g2_4cl["legacy_domain_top_level_functions_after"] == 802
    assert migration_g2_4cl["python_module_count"] == 909
    assert migration_g2_4cl["python_test_file_count"] == 1115
    fixture_g2_4cl = phase_g2_4cl["fixture"]
    assert fixture_g2_4cl["date_start"].isoformat() == "2022-12-01"
    assert fixture_g2_4cl["date_end"].isoformat() == "2024-02-29"
    assert fixture_g2_4cl["workflow_pass_is_not_investment_conclusion"] is True
    if phase_g2_4cl["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4cl["validation"]["focused"]["passed"] >= 11
        assert phase_g2_4cl["validation"]["current_slice_and_cli_contract"]["passed"] >= 116
        assert phase_g2_4cl["validation"]["architecture_fitness"]["passed"] >= 273
        assert phase_g2_4cl["validation"]["contract_validation"]["passed"] >= 203
        assert phase_g2_4cl["sources"]
        for source in phase_g2_4cl["sources"]:
            if source["path"] in set(phase_g2_4cl.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    phase_g2_4cm = baseline["phase_g2_4cm_etf_cli_dynamic_v3_system_target_experiment_factory"]
    assert phase_g2_4cm["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4cm = phase_g2_4cm["migration"]
    assert migration_g2_4cm["callback_count"] == 21
    assert migration_g2_4cm["pre_output_live_source_validation_required"] is True
    assert migration_g2_4cm["exact_full_chain_lineage_required"] is True
    assert migration_g2_4cm["source_chronology_required"] is True
    assert migration_g2_4cm["common_finite_price_dates_required"] is True
    assert migration_g2_4cm["first_or_missing_return_may_be_filled_zero"] is False
    assert migration_g2_4cm["candidate_selection_must_execute"] is True
    assert migration_g2_4cm["missing_regime_metrics_must_remain_null"] is True
    assert migration_g2_4cm["zero_effect_transform_must_defer"] is True
    assert migration_g2_4cm["reviewed_complete_triage_policy_required"] is True
    assert migration_g2_4cm["expected_and_observed_evidence_are_separate"] is True
    assert len(migration_g2_4cm["snapshot_schemas"]) == 7
    assert migration_g2_4cm["content_derived_all_views_validation"] is True
    assert migration_g2_4cm["automatic_policy_or_execution_allowed"] is False
    assert migration_g2_4cm["portfolio_or_execution_effect"] is False
    assert migration_g2_4cm["legacy_root_lines_after"] == 18568
    assert migration_g2_4cm["legacy_root_top_level_functions_after"] == 513
    assert migration_g2_4cm["legacy_root_command_decorators_after"] == 474
    assert migration_g2_4cm["legacy_domain_lines_after"] == 23375
    assert migration_g2_4cm["legacy_domain_top_level_functions_after"] == 803
    assert migration_g2_4cm["python_module_count"] == 911
    assert migration_g2_4cm["python_test_file_count"] == 1116
    fixture_g2_4cm = phase_g2_4cm["fixture"]
    assert fixture_g2_4cm["requested_start_date"].isoformat() == "2022-12-01"
    assert fixture_g2_4cm["actual_return_start_date"].isoformat() == "2022-12-02"
    assert fixture_g2_4cm["promote_count"] == 0
    assert fixture_g2_4cm["keep_testing_count"] == 3
    assert fixture_g2_4cm["reject_count"] == 7
    assert fixture_g2_4cm["defer_count"] == 5
    assert fixture_g2_4cm["promotion_plan_status"] == "DEFER"
    assert fixture_g2_4cm["workflow_pass_is_not_investment_conclusion"] is True
    if phase_g2_4cm["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4cm["validation"]["focused"]["passed"] >= 12
        assert phase_g2_4cm["validation"]["current_slice_and_cli_contract"]["passed"] >= 118
        assert phase_g2_4cm["validation"]["architecture_fitness"]["passed"] >= 274
        assert phase_g2_4cm["validation"]["contract_validation"]["passed"] >= 203
        assert phase_g2_4cm["sources"]
        for source in phase_g2_4cm["sources"]:
            if source["path"] in set(phase_g2_4cm.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    phase_g2_4cn = baseline["phase_g2_4cn_etf_cli_dynamic_v3_system_target_smoothed_method"]
    assert phase_g2_4cn["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4cn = phase_g2_4cn["migration"]
    assert migration_g2_4cn["callback_count"] == 15
    assert migration_g2_4cn["pre_output_live_source_validation_required"] is True
    assert migration_g2_4cn["canonical_paper_backfill_orchestration_allowed"] is True
    assert migration_g2_4cn["all_other_implicit_upstream_run_allowed"] is False
    assert migration_g2_4cn["exact_same_backfill_lineage_required"] is True
    assert migration_g2_4cn["source_chronology_required"] is True
    assert migration_g2_4cn["reviewed_evaluation_policy_required"] is True
    assert migration_g2_4cn["duplicate_method_observations_allowed"] is False
    assert migration_g2_4cn["missing_metrics_must_remain_null"] is True
    assert migration_g2_4cn["evidence_driven_method_selection_required"] is True
    assert migration_g2_4cn["fixed_method_recommendation_allowed"] is False
    assert len(migration_g2_4cn["snapshot_schemas"]) == 5
    assert migration_g2_4cn["content_derived_all_views_validation"] is True
    assert migration_g2_4cn["automatic_policy_or_execution_allowed"] is False
    assert migration_g2_4cn["portfolio_or_execution_effect"] is False
    assert migration_g2_4cn["legacy_root_lines_after"] == 18164
    assert migration_g2_4cn["legacy_root_top_level_functions_after"] == 498
    assert migration_g2_4cn["legacy_root_command_decorators_after"] == 459
    assert migration_g2_4cn["legacy_domain_lines_after"] == 22551
    assert migration_g2_4cn["legacy_domain_top_level_functions_after"] == 804
    assert migration_g2_4cn["python_module_count"] == 913
    assert migration_g2_4cn["python_test_file_count"] == 1117
    fixture_g2_4cn = phase_g2_4cn["fixture"]
    assert fixture_g2_4cn["date_start"].isoformat() == "2022-12-01"
    assert fixture_g2_4cn["date_end"].isoformat() == "2024-02-29"
    assert fixture_g2_4cn["comparison_observation_count"] == 326
    assert fixture_g2_4cn["decision"] == "CONTINUE_OBSERVATION"
    assert fixture_g2_4cn["recommended_method"] is None
    assert fixture_g2_4cn["secondary_method"] is None
    assert fixture_g2_4cn["workflow_pass_is_not_investment_conclusion"] is True
    if phase_g2_4cn["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4cn["validation"]["focused"]["passed"] >= 45
        assert phase_g2_4cn["validation"]["current_slice_and_cli_contract"]["passed"] >= 152
        assert phase_g2_4cn["validation"]["architecture_fitness"]["passed"] >= 275
        assert phase_g2_4cn["validation"]["contract_validation"]["passed"] >= 203
        assert phase_g2_4cn["sources"]
        for source in phase_g2_4cn["sources"]:
            if source["path"] in set(phase_g2_4cn.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    phase_g2_4co = baseline["phase_g2_4co_etf_cli_dynamic_v3_system_target_smoothed_evidence"]
    assert phase_g2_4co["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4co = phase_g2_4co["migration"]
    assert migration_g2_4co["callback_count"] == 15
    assert migration_g2_4co["pre_output_live_source_validation_required"] is True
    assert migration_g2_4co["exact_review_comparison_backfill_lineage_required"] is True
    assert migration_g2_4co["source_chronology_required"] is True
    assert migration_g2_4co["reviewed_evidence_policy_required"] is True
    assert migration_g2_4co["missing_metrics_must_remain_null"] is True
    assert migration_g2_4co["per_method_evidence_required"] is True
    assert migration_g2_4co["fixed_method_roles_allowed"] is False
    assert migration_g2_4co["confirmation_requires_unique_eligible_recommendation"] is True
    assert migration_g2_4co["zero_targets_when_no_candidate_required"] is True
    assert len(migration_g2_4co["snapshot_schemas"]) == 5
    assert migration_g2_4co["content_derived_all_views_validation"] is True
    assert migration_g2_4co["automatic_policy_or_execution_allowed"] is False
    assert migration_g2_4co["portfolio_or_execution_effect"] is False
    assert migration_g2_4co["legacy_root_lines_after"] == 17739
    assert migration_g2_4co["legacy_root_top_level_functions_after"] == 483
    assert migration_g2_4co["legacy_root_command_decorators_after"] == 444
    assert migration_g2_4co["legacy_domain_lines_after"] == 21897
    assert migration_g2_4co["legacy_domain_top_level_functions_after"] == 805
    assert migration_g2_4co["python_module_count"] == 915
    assert migration_g2_4co["python_test_file_count"] == 1118
    fixture_g2_4co = phase_g2_4co["fixture"]
    assert fixture_g2_4co["date_start"].isoformat() == "2022-12-01"
    assert fixture_g2_4co["date_end"].isoformat() == "2024-02-29"
    assert fixture_g2_4co["common_return_observation_count"] == 326
    assert fixture_g2_4co["sideways_observation_count"] == 283
    assert fixture_g2_4co["recovery_observation_count"] == 7
    assert fixture_g2_4co["decision"] == "CONTINUE_OBSERVATION"
    assert fixture_g2_4co["candidate_method"] is None
    assert fixture_g2_4co["confirmation_status"] == "INSUFFICIENT_EVIDENCE"
    assert fixture_g2_4co["confirmation_target_count"] == 0
    assert fixture_g2_4co["workflow_pass_is_not_investment_conclusion"] is True
    if phase_g2_4co["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4co["validation"]["focused"]["passed"] >= 12
        assert phase_g2_4co["validation"]["current_slice_and_cli_contract"]["passed"] >= 120
        assert phase_g2_4co["validation"]["architecture_fitness"]["passed"] >= 276
        assert phase_g2_4co["validation"]["contract_validation"]["passed"] >= 203
        assert phase_g2_4co["sources"]
        for source in phase_g2_4co["sources"]:
            if source["path"] in set(phase_g2_4co.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    phase_g2_4cp = baseline["phase_g2_4cp_etf_cli_dynamic_v3_system_target_smoothed_readiness"]
    assert phase_g2_4cp["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4cp = phase_g2_4cp["migration"]
    assert migration_g2_4cp["callback_count"] == 15
    assert migration_g2_4cp["pre_output_live_source_validation_required"] is True
    assert migration_g2_4cp["exact_review_comparison_backfill_lineage_required"] is True
    assert migration_g2_4cp["exact_same_backfill_lineage_required"] is True
    assert migration_g2_4cp["source_chronology_required"] is True
    assert migration_g2_4cp["reviewed_readiness_policy_required"] is True
    assert migration_g2_4cp["explicit_delta_or_zero_turnover_identity_required"] is True
    assert migration_g2_4cp["duplicate_method_observations_allowed"] is False
    assert migration_g2_4cp["missing_metrics_must_remain_null"] is True
    assert migration_g2_4cp["missing_evidence_positive_score_allowed"] is False
    assert migration_g2_4cp["confirmation_candidate_is_only_candidate_authority"] is True
    assert migration_g2_4cp["fixed_method_roles_allowed"] is False
    assert migration_g2_4cp["candidate_less_promotion_allowed"] is False
    assert len(migration_g2_4cp["snapshot_schemas"]) == 5
    assert migration_g2_4cp["content_derived_all_views_validation"] is True
    assert migration_g2_4cp["automatic_policy_or_execution_allowed"] is False
    assert migration_g2_4cp["portfolio_or_execution_effect"] is False
    assert migration_g2_4cp["legacy_root_lines_after"] == 17328
    assert migration_g2_4cp["legacy_root_top_level_functions_after"] == 468
    assert migration_g2_4cp["legacy_root_command_decorators_after"] == 429
    assert migration_g2_4cp["legacy_domain_lines_after"] == 19856
    assert migration_g2_4cp["legacy_domain_top_level_functions_after"] == 760
    assert migration_g2_4cp["python_module_count"] == 917
    assert migration_g2_4cp["python_test_file_count"] == 1119
    fixture_g2_4cp = phase_g2_4cp["fixture"]
    assert fixture_g2_4cp["date_start"].isoformat() == "2022-12-01"
    assert fixture_g2_4cp["date_end"].isoformat() == "2024-02-29"
    assert fixture_g2_4cp["candidate_method"] is None
    assert fixture_g2_4cp["gap_tradeoff_can_be_resolved_by_backfill"] is False
    assert fixture_g2_4cp["gap_requires_forward_observation"] is True
    assert fixture_g2_4cp["scorecard_decision"] == "CONTINUE_OBSERVATION"
    assert fixture_g2_4cp["scorecard_evidence_status"] == "INSUFFICIENT_EVIDENCE"
    assert fixture_g2_4cp["recommended_method"] is None
    assert fixture_g2_4cp["secondary_method"] is None
    assert fixture_g2_4cp["owner_recommended_action"] == "request_additional_evidence"
    assert fixture_g2_4cp["owner_promotion_recommended"] is False
    assert fixture_g2_4cp["workflow_pass_is_not_investment_conclusion"] is True
    if phase_g2_4cp["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4cp["validation"]["focused_integration"]["passed"] >= 1
        assert phase_g2_4cp["validation"]["hardening"]["passed"] >= 7
        assert phase_g2_4cp["validation"]["current_slice_and_cli_contract"]["passed"] >= 9
        assert (
            phase_g2_4cp["validation"]["downstream_compatibility"]["status"]
            == "FAIL_CLOSED_NEXT_SLICE"
        )
        assert phase_g2_4cp["validation"]["downstream_compatibility"]["passed"] >= 1
        assert phase_g2_4cp["validation"]["architecture_fitness"]["passed"] >= 277
        assert phase_g2_4cp["validation"]["contract_validation"]["passed"] >= 203
        assert phase_g2_4cp["sources"]
        for source in phase_g2_4cp["sources"]:
            if source["path"] in set(phase_g2_4cp.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    phase_g2_4cq = baseline["phase_g2_4cq_etf_cli_dynamic_v3_system_target_smoothed_promotion"]
    assert phase_g2_4cq["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4cq = phase_g2_4cq["migration"]
    assert migration_g2_4cq["callback_count"] == 16
    assert migration_g2_4cq["pre_output_live_source_validation_required"] is True
    assert migration_g2_4cq["source_chronology_required"] is True
    assert migration_g2_4cq["exact_candidate_lineage_required"] is True
    assert migration_g2_4cq["reviewed_promotion_policy_required"] is True
    assert migration_g2_4cq["confirmation_candidate_is_only_candidate_authority"] is True
    assert migration_g2_4cq["fixed_method_fallback_allowed"] is False
    assert migration_g2_4cq["candidate_less_target_fabrication_allowed"] is False
    assert migration_g2_4cq["candidate_less_switch_proposal_allowed"] is False
    assert migration_g2_4cq["candidate_less_promote_record_allowed"] is False
    assert migration_g2_4cq["candidate_less_continue_observation_valid"] is True
    assert migration_g2_4cq["bounded_source_bundle_recursive_input_snapshot_allowed"] is False
    assert migration_g2_4cq["bounded_source_bundle_live_validator_required"] is True
    assert migration_g2_4cq["content_derived_evidence_criteria_targets_switch_journal"] is True
    assert migration_g2_4cq["owner_record_atomic_all_view_rebuild"] is True
    assert len(migration_g2_4cq["snapshot_schemas"]) == 5
    assert migration_g2_4cq["content_derived_all_views_validation"] is True
    assert migration_g2_4cq["automatic_policy_or_execution_allowed"] is False
    assert migration_g2_4cq["portfolio_or_execution_effect"] is False
    assert migration_g2_4cq["legacy_root_lines_after"] == 16761
    assert migration_g2_4cq["legacy_root_top_level_functions_after"] == 452
    assert migration_g2_4cq["legacy_root_command_decorators_after"] == 413
    assert migration_g2_4cq["legacy_domain_lines_after"] == 18248
    assert migration_g2_4cq["legacy_domain_top_level_functions_after"] == 740
    assert migration_g2_4cq["legacy_domain_compatibility_wrapper_count"] == 11
    assert migration_g2_4cq["python_module_count"] == 919
    assert migration_g2_4cq["python_test_file_count"] == 1120
    fixture_g2_4cq = phase_g2_4cq["fixture"]
    assert fixture_g2_4cq["candidate_method"] is None
    assert fixture_g2_4cq["promotion_can_enter_owner_review"] is False
    assert fixture_g2_4cq["promotion_can_become_primary_candidate"] == "NOT_ELIGIBLE"
    assert fixture_g2_4cq["gate_decision"] == "CONTINUE_OBSERVATION"
    assert fixture_g2_4cq["binding_status"] == "NOT_REGISTERED"
    assert fixture_g2_4cq["bound_target_count"] == 0
    assert fixture_g2_4cq["proposed_primary_research_candidate"] is None
    assert fixture_g2_4cq["switch_decision"] == "NO_ELIGIBLE_CANDIDATE"
    assert fixture_g2_4cq["owner_recommended_action"] == "request_more_forward_data"
    assert fixture_g2_4cq["invalid_candidate_less_promote_rejected"] is True
    assert fixture_g2_4cq["continue_observation_validation"] == "PASS"
    assert fixture_g2_4cq["actual_switch_executed"] is False
    assert fixture_g2_4cq["workflow_pass_is_not_investment_conclusion"] is True
    if phase_g2_4cq["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4cq["validation"]["hardening"]["passed"] >= 7
        assert phase_g2_4cq["validation"]["five_stage_integration"]["passed"] >= 5
        assert phase_g2_4cq["validation"]["architecture_fitness"]["passed"] >= 278
        assert phase_g2_4cq["validation"]["contract_validation"]["passed"] >= 203
        assert phase_g2_4cq["sources"]
        for source in phase_g2_4cq["sources"]:
            if source["path"] in set(phase_g2_4cq.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    phase_g2_4cr = baseline["phase_g2_4cr_etf_cli_dynamic_v3_system_target_smoothed_operations"]
    assert phase_g2_4cr["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4cr = phase_g2_4cr["migration"]
    assert migration_g2_4cr["callback_count"] == 15
    assert migration_g2_4cr["pre_output_live_source_validation_required"] is True
    assert migration_g2_4cr["source_chronology_required"] is True
    assert migration_g2_4cr["binding_candidate_and_targets_are_only_authority"] is True
    assert migration_g2_4cr["explicit_evidence_ids_and_exact_target_lineage_required"] is True
    assert migration_g2_4cr["cross_lineage_directory_scan_allowed"] is False
    assert migration_g2_4cr["progress_is_only_operations_lineage"] is True
    assert migration_g2_4cr["dashboard_monitor_same_progress_required"] is True
    assert migration_g2_4cr["recheck_exact_progress_candidate_lineage_required"] is True
    assert migration_g2_4cr["renewal_exact_recheck_owner_lineage_required"] is True
    assert migration_g2_4cr["fixed_method_fallback_allowed"] is False
    assert migration_g2_4cr["candidate_less_requirement_fabrication_allowed"] is False
    assert migration_g2_4cr["bounded_source_bundle_recursive_input_snapshot_allowed"] is False
    assert migration_g2_4cr["bounded_source_bundle_live_validator_required"] is True
    assert migration_g2_4cr["content_fingerprint_validation_session"] is True
    assert migration_g2_4cr["complete_file_fingerprint_required"] is True
    assert migration_g2_4cr["validation_cache_pass_only"] is True
    assert migration_g2_4cr["validation_cache_failure_reuse_allowed"] is False
    assert migration_g2_4cr["validation_cache_return_mutation_isolated"] is True
    assert len(migration_g2_4cr["snapshot_schemas"]) == 5
    assert migration_g2_4cr["content_derived_all_views_validation"] is True
    assert migration_g2_4cr["automatic_policy_or_execution_allowed"] is False
    assert migration_g2_4cr["portfolio_or_execution_effect"] is False
    assert migration_g2_4cr["legacy_root_lines_after"] == 16359
    assert migration_g2_4cr["legacy_root_top_level_functions_after"] == 437
    assert migration_g2_4cr["legacy_root_command_decorators_after"] == 398
    assert migration_g2_4cr["legacy_domain_lines_after"] == 16659
    assert migration_g2_4cr["legacy_domain_top_level_functions_after"] == 713
    assert migration_g2_4cr["legacy_domain_compatibility_wrapper_count"] == 15
    assert migration_g2_4cr["python_module_count"] == 922
    assert migration_g2_4cr["python_test_file_count"] == 1122
    fixture_g2_4cr = phase_g2_4cr["fixture"]
    assert fixture_g2_4cr["candidate_method"] is None
    assert fixture_g2_4cr["bound_target_count"] == 0
    assert fixture_g2_4cr["progress_status"] == "NOT_REGISTERED"
    assert fixture_g2_4cr["progress_requirement_count"] == 0
    assert fixture_g2_4cr["progress_forward_event_count"] == 0
    assert fixture_g2_4cr["dashboard_status"] == "NOT_REGISTERED"
    assert fixture_g2_4cr["dashboard_ready_for_switch_recheck"] is False
    assert fixture_g2_4cr["event_monitor_status"] == "NOT_REGISTERED"
    assert fixture_g2_4cr["sideways_event_count"] == 0
    assert fixture_g2_4cr["recovery_event_count"] == 0
    assert fixture_g2_4cr["recheck_decision"] == "NO_ELIGIBLE_CANDIDATE"
    assert fixture_g2_4cr["recheck_criteria_count"] == 0
    assert fixture_g2_4cr["owner_decision_required"] is False
    assert fixture_g2_4cr["can_execute_switch"] is False
    assert fixture_g2_4cr["renewal_promote_available"] is False
    assert fixture_g2_4cr["renewal_recommended_action"] == "request_more_forward_data"
    assert fixture_g2_4cr["workflow_pass_is_not_investment_conclusion"] is True
    performance_g2_4cr = phase_g2_4cr["performance"]
    assert performance_g2_4cr["progress_test_baseline_seconds"] == 557.27
    assert performance_g2_4cr["progress_test_bounded_source_seconds"] == 13.60
    assert performance_g2_4cr["elapsed_reduction_percent"] >= 97.5
    assert performance_g2_4cr["largest_snapshot_reduction_percent"] >= 98.5
    if phase_g2_4cr["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4cr["validation"]["operations_hardening"]["passed"] >= 1
        assert phase_g2_4cr["validation"]["five_stage_integration"]["passed"] >= 5
        assert phase_g2_4cr["validation"]["architecture_fitness"]["passed"] >= 279
        assert phase_g2_4cr["validation"]["contract_validation"]["passed"] >= 203
        assert phase_g2_4cr["sources"]
        for source in phase_g2_4cr["sources"]:
            if source["path"] in set(phase_g2_4cr.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    phase_g2_4cs = baseline["phase_g2_4cs_etf_cli_dynamic_v3_smoothed_forward_sample_bootstrap"]
    assert phase_g2_4cs["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4cs = phase_g2_4cs["migration"]
    assert migration_g2_4cs["callback_count"] == 15
    assert migration_g2_4cs["binding_candidate_and_targets_are_only_authority"] is True
    assert migration_g2_4cs["fixed_candidate_or_method_role_allowed"] is False
    assert migration_g2_4cs["candidate_less_sample_fabrication_allowed"] is False
    assert migration_g2_4cs["pre_output_live_source_validation_required"] is True
    assert migration_g2_4cs["source_chronology_required"] is True
    assert migration_g2_4cs["bounded_market_data_and_validate_data_required"] is True
    assert migration_g2_4cs["exact_emission_window_lineage_required"] is True
    assert migration_g2_4cs["cross_lineage_directory_scan_allowed"] is False
    assert migration_g2_4cs["null_preserving_calculation_required"] is True
    assert migration_g2_4cs["dynamic_candidate_baseline_required"] is True
    assert migration_g2_4cs["reporting_thresholds_are_investment_gates"] is False
    assert migration_g2_4cs["weekly_exact_nine_step_binding_required"] is True
    assert migration_g2_4cs["content_derived_all_views_validation"] is True
    assert migration_g2_4cs["source_and_output_tamper_fail_closed"] is True
    assert len(migration_g2_4cs["snapshot_schemas"]) == 5
    assert migration_g2_4cs["automatic_policy_or_execution_allowed"] is False
    assert migration_g2_4cs["portfolio_or_execution_effect"] is False
    assert migration_g2_4cs["legacy_root_lines_after"] == 15842
    assert migration_g2_4cs["legacy_root_top_level_functions_after"] == 422
    assert migration_g2_4cs["legacy_root_command_decorators_after"] == 383
    assert migration_g2_4cs["legacy_domain_lines_after"] == 14895
    assert migration_g2_4cs["legacy_domain_top_level_functions_after"] == 678
    assert migration_g2_4cs["legacy_domain_compatibility_wrapper_count"] == 15
    assert migration_g2_4cs["python_module_count"] == 924
    assert migration_g2_4cs["python_test_file_count"] == 1123
    fixture_g2_4cs = phase_g2_4cs["fixture"]
    assert fixture_g2_4cs["candidate_method"] is None
    assert fixture_g2_4cs["binding_status"] == "NOT_REGISTERED"
    assert fixture_g2_4cs["bound_target_count"] == 0
    assert fixture_g2_4cs["emitted_event_count"] == 0
    assert fixture_g2_4cs["event_status"] == "NOT_REGISTERED"
    assert fixture_g2_4cs["due_window_count"] == 0
    assert fixture_g2_4cs["updated_window_count"] == 0
    assert fixture_g2_4cs["classified_event_count"] == 0
    assert fixture_g2_4cs["recheck_decision"] == "NO_ELIGIBLE_CANDIDATE"
    assert fixture_g2_4cs["renewal_recommended_action"] == "request_more_forward_data"
    assert fixture_g2_4cs["can_execute_switch"] is False
    assert fixture_g2_4cs["workflow_pass_is_not_investment_conclusion"] is True
    performance_g2_4cs = phase_g2_4cs["performance"]
    assert performance_g2_4cs["smoothed_regression_before_seconds"] == 270.04
    assert performance_g2_4cs["smoothed_regression_after_seconds"] == 100.98
    assert performance_g2_4cs["elapsed_reduction_percent"] >= 62.6
    assert performance_g2_4cs["readiness_reduction_percent"] >= 64.9
    assert performance_g2_4cs["validation_gate_skipped"] is False
    if phase_g2_4cs["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4cs["validation"]["focused_formula_lineage_and_chain"]["passed"] >= 15
        assert phase_g2_4cs["validation"]["smoothed_regression"]["passed"] >= 41
        assert phase_g2_4cs["validation"]["cli_contract"]["passed"] >= 117
        assert phase_g2_4cs["validation"]["architecture_fitness"]["passed"] >= 280
        assert phase_g2_4cs["validation"]["contract_validation"]["passed"] >= 203
        assert phase_g2_4cs["validation"]["full_validation"]["passed"] >= 6012
        assert phase_g2_4cs["sources"]
        for source in phase_g2_4cs["sources"]:
            if source["path"] in set(phase_g2_4cs.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    phase_g2_4ct = baseline["phase_g2_4ct_etf_cli_dynamic_v3_smoothed_data_freshness"]
    assert phase_g2_4ct["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4ct = phase_g2_4ct["migration"]
    assert migration_g2_4ct["callback_count"] == 15
    assert migration_g2_4ct["legacy_domain_is_lazy_wrapper_only"] is True
    assert len(migration_g2_4ct["snapshot_schemas"]) == 5
    assert migration_g2_4ct["same_validate_data_path_and_live_replay_required"] is True
    assert migration_g2_4ct["strict_unique_cutoff_model_target_required"] is True
    assert migration_g2_4ct["invalid_or_ambiguous_target_silently_null_allowed"] is False
    assert migration_g2_4ct["exact_preflight_latest_explain_refresh_lineage_required"] is True
    assert migration_g2_4ct["retry_validated_weekly_or_latest_child_required"] is True
    assert migration_g2_4ct["blocked_retry_downstream_execution_allowed"] is False
    assert migration_g2_4ct["ready_explain_zero_blockers_allowed"] is True
    assert migration_g2_4ct["current_null_candidate_emission_count"] == 0
    assert migration_g2_4ct["current_null_candidate_event_status"] == "NOT_REGISTERED"
    assert migration_g2_4ct["content_derived_all_views_validation"] is True
    assert migration_g2_4ct["source_and_output_tamper_fail_closed"] is True
    assert migration_g2_4ct["validation_session_cache_reuse_enabled"] is True
    assert migration_g2_4ct["validation_gate_reduced_for_performance"] is False
    assert migration_g2_4ct["automatic_refresh_policy_or_execution_allowed"] is False
    assert migration_g2_4ct["portfolio_or_execution_effect"] is False
    assert migration_g2_4ct["legacy_root_lines_after"] == 15373
    assert migration_g2_4ct["legacy_root_top_level_functions_after"] == 407
    assert migration_g2_4ct["legacy_root_command_decorators_after"] == 368
    assert migration_g2_4ct["legacy_domain_lines_after"] == 13978
    assert migration_g2_4ct["legacy_domain_top_level_functions_after"] == 679
    assert migration_g2_4ct["legacy_domain_compatibility_wrapper_count"] == 15
    assert migration_g2_4ct["python_module_count"] == 926
    assert migration_g2_4ct["python_test_file_count"] == 1124
    fixture_g2_4ct = phase_g2_4ct["fixture"]
    assert fixture_g2_4ct["stale_freshness_status"] == "BLOCKED_STALE_DATA"
    assert str(fixture_g2_4ct["stale_latest_valid_as_of"]) == "2026-01-08"
    assert fixture_g2_4ct["latest_emitted_event_count"] == 0
    assert fixture_g2_4ct["latest_event_status"] == "NOT_REGISTERED"
    assert fixture_g2_4ct["ready_blocked_command_count"] == 0
    assert fixture_g2_4ct["cross_preflight_refresh_allowed"] is False
    assert fixture_g2_4ct["weekly_child_tamper_fails_retry"] is True
    assert fixture_g2_4ct["can_execute_switch"] is False
    if phase_g2_4ct["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4ct["validation"]["combined_focused"]["passed"] >= 125
        assert phase_g2_4ct["validation"]["architecture_fitness"]["passed"] >= 281
        assert phase_g2_4ct["validation"]["contract_validation"]["passed"] >= 203
        assert phase_g2_4ct["validation"]["full_validation"]["passed"] >= 6012
        assert phase_g2_4ct["sources"]
        smoothed_interface_source = next(
            source
            for source in phase_g2_4ct["sources"]
            if source["path"].endswith("dynamic_v3_system_target_smoothed_freshness.py")
            and "/interfaces/" in source["path"]
        )
        assert smoothed_interface_source["hash_normalization"] == "git_eol_lf"
        assert smoothed_interface_source["previous_worktree_sha256"] == (
            "bc268a1292730b9751c5febe2702dd1a456b85b7e74b6b56d7efc4508fe4b8d7"
        )
        for source in phase_g2_4ct["sources"]:
            if source["path"] in set(phase_g2_4ct.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    phase_g2_4cu = baseline["phase_g2_4cu_etf_cli_dynamic_v3_smoothed_data_refresh_operations"]
    assert phase_g2_4cu["status"] in {"IN_PROGRESS", "VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4cu = phase_g2_4cu["migration"]
    assert migration_g2_4cu["callback_count"] == 16
    assert migration_g2_4cu["domain_entrypoint_count"] == 15
    assert len(migration_g2_4cu["snapshot_schemas"]) == 5
    assert (
        migration_g2_4cu[
            "exact_refresh_plan_preflight_refresh_post_resume_growth_readiness_lineage_required"
        ]
        is True
    )
    assert migration_g2_4cu["explicit_execute_authorization_required"] is True
    assert migration_g2_4cu["dry_run_before_after_identity_required"] is True
    assert migration_g2_4cu["execute_before_after_commitments_required"] is True
    assert migration_g2_4cu["validator_provider_side_effect_replay_allowed"] is False
    assert migration_g2_4cu["blocked_downstream_child_generation_allowed"] is False
    assert migration_g2_4cu["content_derived_all_views_validation_required"] is True
    assert migration_g2_4cu["real_provider_blocker_may_be_fabricated_away"] is False
    assert migration_g2_4cu["can_execute_switch"] is False
    assert migration_g2_4cu["production_effect"] == "none"
    performance_g2_4cu = phase_g2_4cu["performance"]
    assert performance_g2_4cu["validation_session_nested_source_cache_reuse"] is True
    assert performance_g2_4cu["full_gate_reduced_for_performance"] is False
    assert performance_g2_4cu["observed_single_run_reduction_percent"] >= 22.8
    assert performance_g2_4cu["stable_full_improvement_claimed"] is False
    assert performance_g2_4cu["duration_and_peak_memory_aware_sharding_required"] is True
    assert performance_g2_4cu["active_node_heartbeat_required"] is True
    if phase_g2_4cu["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4cu["validation"]["focused"]["passed"] >= 123
        assert phase_g2_4cu["validation"]["architecture_fitness"]["passed"] >= 282
        assert phase_g2_4cu["validation"]["contract_validation"]["passed"] >= 203
        assert phase_g2_4cu["validation"]["full_validation"]["passed"] >= 6023
        assert phase_g2_4cu["sources"]
        for source in phase_g2_4cu["sources"]:
            if source["path"] in set(phase_g2_4cu.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    phase_g2_4cv1 = baseline["phase_g2_4cv1_etf_cli_dynamic_v3_weight_search_foundation"]
    assert phase_g2_4cv1["status"] in {
        "IN_PROGRESS",
        "VALIDATING",
        "COMPLETE_G2_4_CONTINUES",
    }
    migration_g2_4cv1 = phase_g2_4cv1["migration"]
    assert migration_g2_4cv1["callback_count"] == 10
    assert migration_g2_4cv1["domain_entrypoint_count"] == 12
    assert len(migration_g2_4cv1["snapshot_schemas"]) == 3
    assert migration_g2_4cv1["exact_search_to_matrix_to_backfill_lineage_required"] is True
    assert migration_g2_4cv1["exact_paper_backfill_binding_required"] is True
    assert migration_g2_4cv1["same_validate_data_path_required"] is True
    assert migration_g2_4cv1["live_config_and_cache_commitments_required"] is True
    assert migration_g2_4cv1["content_derived_all_views_validation_required"] is True
    assert migration_g2_4cv1["resume_requires_existing_artifact_pass"] is True
    assert migration_g2_4cv1["broker_action_allowed"] is False
    assert migration_g2_4cv1["production_effect"] == "none"
    subtraction_g2_4cv1 = phase_g2_4cv1["subtraction"]
    assert subtraction_g2_4cv1["legacy_cli_lines_after"] == 14551
    assert subtraction_g2_4cv1["legacy_cli_top_level_functions_after"] == 381
    assert subtraction_g2_4cv1["legacy_cli_callback_reduction"] == 10
    hardening_g2_4cv1 = phase_g2_4cv1["hardening"]
    assert hardening_g2_4cv1["search_emitted_views_tamper_checked"] == 4
    assert hardening_g2_4cv1["matrix_emitted_views_tamper_checked"] == 4
    assert hardening_g2_4cv1["backfill_emitted_views_tamper_checked"] == 10
    assert hardening_g2_4cv1["snapshot_schema_tamper_checked"] == 3
    assert hardening_g2_4cv1["tampered_progress_blocks_resume"] is True
    performance_g2_4cv1 = phase_g2_4cv1["performance"]
    assert performance_g2_4cv1["full_gate_reduced_for_performance"] is False
    assert performance_g2_4cv1["observed_regression_percent"] >= 28.9
    assert performance_g2_4cv1["stable_full_improvement_claimed"] is False
    assert performance_g2_4cv1["current_slice_hardening_in_slowest_50"] is False
    assert performance_g2_4cv1["duration_and_peak_memory_aware_sharding_required"] is True
    assert performance_g2_4cv1["active_node_heartbeat_and_eta_required"] is True
    if phase_g2_4cv1["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4cv1["validation"]["focused"]["passed"] >= 124
        assert phase_g2_4cv1["validation"]["architecture_fitness"]["passed"] >= 282
        assert phase_g2_4cv1["validation"]["contract_validation"]["passed"] >= 203
        assert phase_g2_4cv1["validation"]["full_validation"]["passed"] >= 6023
        assert phase_g2_4cv1["sources"]
        for source in phase_g2_4cv1["sources"]:
            if source["path"] in set(phase_g2_4cv1.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    phase_g2_4cv2 = baseline["phase_g2_4cv2_etf_cli_dynamic_v3_weight_search_evaluation"]
    assert phase_g2_4cv2["status"] in {
        "IN_PROGRESS",
        "VALIDATING",
        "COMPLETE_G2_4_CONTINUES",
    }
    migration_g2_4cv2 = phase_g2_4cv2["migration"]
    assert migration_g2_4cv2["callback_count"] == 11
    assert migration_g2_4cv2["domain_entrypoint_count"] == 11
    assert len(migration_g2_4cv2["snapshot_schemas"]) == 3
    assert migration_g2_4cv2["exact_backfill_matrix_to_scorecard_lineage_required"] is True
    assert migration_g2_4cv2["exact_scorecard_backfill_to_robustness_lineage_required"] is True
    assert migration_g2_4cv2["same_lineage_scorecard_robustness_to_adaptive_required"] is True
    assert migration_g2_4cv2["validated_branch_authorizes_expanded_matrix_required"] is True
    assert (
        migration_g2_4cv2["canonical_matrix_and_data_quality_backfill_delegation_required"] is True
    )
    assert migration_g2_4cv2["content_derived_all_views_validation_required"] is True
    assert migration_g2_4cv2["broker_action_allowed"] is False
    assert migration_g2_4cv2["production_effect"] == "none"
    subtraction_g2_4cv2 = phase_g2_4cv2["subtraction"]
    assert subtraction_g2_4cv2["legacy_cli_lines_after"] == 14262
    assert subtraction_g2_4cv2["legacy_cli_top_level_functions_after"] == 370
    assert subtraction_g2_4cv2["legacy_cli_callback_reduction"] == 11
    hardening_g2_4cv2 = phase_g2_4cv2["hardening"]
    assert hardening_g2_4cv2["scorecard_emitted_views_tamper_checked"] == 5
    assert hardening_g2_4cv2["robustness_emitted_views_tamper_checked"] == 6
    assert hardening_g2_4cv2["adaptive_emitted_views_tamper_checked"] == 3
    assert hardening_g2_4cv2["snapshot_schema_tamper_checked"] == 3
    assert hardening_g2_4cv2["cross_lineage_adaptive_fails"] is True
    assert hardening_g2_4cv2["tampered_branch_blocks_expanded_run"] is True
    if phase_g2_4cv2["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4cv2["validation"]["focused"]["passed"] >= 125
        assert phase_g2_4cv2["validation"]["architecture_fitness"]["passed"] >= 284
        assert phase_g2_4cv2["validation"]["contract_validation"]["passed"] >= 203
        assert phase_g2_4cv2["validation"]["full_validation"]["passed"] >= 6026
        assert phase_g2_4cv2["sources"]
        for source in phase_g2_4cv2["sources"]:
            if source["path"] in set(phase_g2_4cv2.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    phase_g2_4cv3 = baseline["phase_g2_4cv3_etf_cli_dynamic_v3_weight_search_decision"]
    assert phase_g2_4cv3["status"] in {
        "IN_PROGRESS",
        "VALIDATING",
        "COMPLETE_G2_4_CONTINUES",
    }
    migration_g2_4cv3 = phase_g2_4cv3["migration"]
    assert migration_g2_4cv3["callback_count"] == 18
    assert migration_g2_4cv3["domain_entrypoint_count"] == 18
    assert len(migration_g2_4cv3["snapshot_schemas"]) == 6
    assert migration_g2_4cv3["same_lineage_scorecard_robustness_to_cluster_required"] is True
    assert (
        migration_g2_4cv3["exact_cluster_to_interpretation_to_gate_to_plan_lineage_required"]
        is True
    )
    assert (
        migration_g2_4cv3["same_lineage_scorecard_adaptive_optional_gate_to_dashboard_required"]
        is True
    )
    assert migration_g2_4cv3["exact_dashboard_to_owner_pack_lineage_required"] is True
    assert migration_g2_4cv3["content_derived_all_views_validation_required"] is True
    assert migration_g2_4cv3["formal_plan_implemented"] is False
    assert migration_g2_4cv3["owner_options_manual_only"] is True
    assert migration_g2_4cv3["broker_action_allowed"] is False
    assert migration_g2_4cv3["production_effect"] == "none"
    subtraction_g2_4cv3 = phase_g2_4cv3["subtraction"]
    assert subtraction_g2_4cv3["legacy_cli_lines_after"] == 13828
    assert subtraction_g2_4cv3["legacy_cli_top_level_functions_after"] == 352
    assert subtraction_g2_4cv3["legacy_cli_callback_reduction"] == 18
    hardening_g2_4cv3 = phase_g2_4cv3["hardening"]
    assert hardening_g2_4cv3["total_emitted_views_tamper_checked"] == 27
    assert hardening_g2_4cv3["snapshot_schema_tamper_checked"] == 6
    assert hardening_g2_4cv3["cross_lineage_tamper_checked"] == 3
    performance_g2_4cv3 = phase_g2_4cv3["performance"]
    assert performance_g2_4cv3["immutable_fixture_built_once"] is True
    assert performance_g2_4cv3["recursive_baseline_validation_replay_still_present"] is True
    assert performance_g2_4cv3["full_gate_reduced_for_performance"] is False
    if phase_g2_4cv3["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4cv3["validation"]["focused"]["passed"] >= 132
        assert phase_g2_4cv3["validation"]["architecture_fitness"]["passed"] >= 285
        assert phase_g2_4cv3["validation"]["contract_validation"]["passed"] >= 203
        assert phase_g2_4cv3["validation"]["full_validation"]["passed"] >= 6029
        assert phase_g2_4cv3["sources"]
        for source in phase_g2_4cv3["sources"]:
            if source["path"] in set(phase_g2_4cv3.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    phase_g2_4cw1 = baseline["phase_g2_4cw1_etf_cli_dynamic_v3_weight_search_diagnostics"]
    assert phase_g2_4cw1["status"] in {
        "IN_PROGRESS",
        "VALIDATING",
        "COMPLETE_G2_4_CONTINUES",
    }
    migration_g2_4cw1 = phase_g2_4cw1["migration"]
    assert migration_g2_4cw1["callback_count"] == 12
    assert migration_g2_4cw1["domain_entrypoint_count"] == 12
    assert len(migration_g2_4cw1["snapshot_schemas"]) == 4
    assert (
        migration_g2_4cw1["exact_scorecard_to_review_to_near_miss_to_attribution_lineage_required"]
        is True
    )
    assert migration_g2_4cw1["exact_search_space_to_coverage_lineage_required"] is True
    assert migration_g2_4cw1["same_scorecard_and_policy_binding_required"] is True
    assert migration_g2_4cw1["content_derived_all_views_validation_required"] is True
    assert migration_g2_4cw1["live_source_and_policy_replay_required"] is True
    assert migration_g2_4cw1["broker_action_allowed"] is False
    assert migration_g2_4cw1["production_effect"] == "none"
    subtraction_g2_4cw1 = phase_g2_4cw1["subtraction"]
    assert subtraction_g2_4cw1["legacy_cli_lines_after"] == 13522
    assert subtraction_g2_4cw1["legacy_cli_top_level_functions_after"] == 340
    assert subtraction_g2_4cw1["legacy_cli_callback_reduction"] == 12
    assert subtraction_g2_4cw1["legacy_domain_lazy_wrapper_count"] == 12
    hardening_g2_4cw1 = phase_g2_4cw1["hardening"]
    assert hardening_g2_4cw1["total_emitted_views_tamper_checked"] == 21
    assert hardening_g2_4cw1["snapshot_schema_tamper_checked"] == 4
    assert hardening_g2_4cw1["cross_lineage_tamper_checked"] == 3
    assert hardening_g2_4cw1["policy_binding_tamper_checked"] is True
    performance_g2_4cw1 = phase_g2_4cw1["performance"]
    assert performance_g2_4cw1["stable_improvement_claimed"] is False
    assert performance_g2_4cw1["full_gate_reduced_for_performance"] is False
    if phase_g2_4cw1["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4cw1["validation"]["focused"]["passed"] >= 132
        assert phase_g2_4cw1["validation"]["architecture_fitness"]["passed"] >= 286
        assert phase_g2_4cw1["validation"]["contract_validation"]["passed"] >= 203
        assert phase_g2_4cw1["validation"]["full_validation"]["passed"] >= 6030
        assert phase_g2_4cw1["sources"]
        for source in phase_g2_4cw1["sources"]:
            if source["path"] in set(phase_g2_4cw1.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    phase_g2_4cw2 = baseline["phase_g2_4cw2_etf_cli_dynamic_v3_weight_search_targeted"]
    assert phase_g2_4cw2["status"] in {
        "IN_PROGRESS",
        "VALIDATING",
        "COMPLETE_G2_4_CONTINUES",
    }
    migration_g2_4cw2 = phase_g2_4cw2["migration"]
    assert migration_g2_4cw2["callback_count"] == 10
    assert migration_g2_4cw2["domain_entrypoint_count"] == 10
    assert len(migration_g2_4cw2["snapshot_schemas"]) == 3
    assert (
        migration_g2_4cw2["exact_coverage_near_miss_scorecard_to_matrix_lineage_required"] is True
    )
    assert migration_g2_4cw2["exact_matrix_weight_paper_backfill_lineage_required"] is True
    assert (
        migration_g2_4cw2["exact_backfill_matrix_near_miss_scorecard_to_ab_lineage_required"]
        is True
    )
    assert migration_g2_4cw2["reviewed_targeted_policy_required"] is True
    assert migration_g2_4cw2["pre_output_data_quality_gate_required"] is True
    assert migration_g2_4cw2["backfill_resume_prior_pass_required"] is True
    assert migration_g2_4cw2["live_source_policy_cache_and_dq_replay_required"] is True
    assert migration_g2_4cw2["broker_action_allowed"] is False
    assert migration_g2_4cw2["production_effect"] == "none"
    subtraction_g2_4cw2 = phase_g2_4cw2["subtraction"]
    assert subtraction_g2_4cw2["legacy_cli_lines_after"] == 13269
    assert subtraction_g2_4cw2["legacy_cli_top_level_functions_after"] == 330
    assert subtraction_g2_4cw2["legacy_cli_callback_reduction"] == 10
    assert subtraction_g2_4cw2["legacy_domain_lazy_wrapper_count"] == 10
    hardening_g2_4cw2 = phase_g2_4cw2["hardening"]
    assert hardening_g2_4cw2["total_emitted_views_tamper_checked"] == 16
    assert hardening_g2_4cw2["snapshot_schema_tamper_checked"] == 3
    assert hardening_g2_4cw2["cross_lineage_tamper_checked"] == 3
    assert hardening_g2_4cw2["policy_binding_tamper_checked"] is True
    assert hardening_g2_4cw2["price_and_rates_binding_tamper_checked"] == 2
    assert hardening_g2_4cw2["resume_tampered_or_incomplete_source_fail_closed_checked"] is True
    performance_g2_4cw2 = phase_g2_4cw2["performance"]
    assert performance_g2_4cw2["stable_improvement_claimed"] is False
    assert performance_g2_4cw2["full_gate_reduced_for_performance"] is False
    if phase_g2_4cw2["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4cw2["validation"]["focused"]["passed"] >= 132
        assert phase_g2_4cw2["validation"]["architecture_fitness"]["passed"] >= 286
        assert phase_g2_4cw2["validation"]["contract_validation"]["passed"] >= 203
        assert phase_g2_4cw2["validation"]["full_validation"]["passed"] >= 6030
        assert phase_g2_4cw2["sources"]
        for source in phase_g2_4cw2["sources"]:
            if source["path"] in set(phase_g2_4cw2.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    phase_g2_4cw3 = baseline["phase_g2_4cw3_etf_cli_dynamic_v3_weight_search_followup"]
    assert phase_g2_4cw3["status"] in {
        "IN_PROGRESS",
        "VALIDATING",
        "COMPLETE_G2_4_CONTINUES",
    }
    migration_g2_4cw3 = phase_g2_4cw3["migration"]
    assert migration_g2_4cw3["callback_count"] == 9
    assert migration_g2_4cw3["domain_entrypoint_count"] == 9
    assert len(migration_g2_4cw3["snapshot_schemas"]) == 3
    assert (
        migration_g2_4cw3["exact_backfill_matrix_ab_scorecard_near_miss_lineage_required"] is True
    )
    assert migration_g2_4cw3["exact_sensitivity_to_promotion_lineage_required"] is True
    assert migration_g2_4cw3["exact_promotion_to_next_plan_lineage_required"] is True
    assert migration_g2_4cw3["reviewed_followup_policy_required"] is True
    assert migration_g2_4cw3["relaxed_threshold_diagnostic_only_required"] is True
    assert migration_g2_4cw3["owner_review_required"] is True
    assert migration_g2_4cw3["implemented"] is False
    assert migration_g2_4cw3["formal_method_task_created"] is False
    assert migration_g2_4cw3["broker_action_allowed"] is False
    assert migration_g2_4cw3["production_effect"] == "none"
    subtraction_g2_4cw3 = phase_g2_4cw3["subtraction"]
    assert subtraction_g2_4cw3["legacy_cli_lines_after"] == 13016
    assert subtraction_g2_4cw3["legacy_cli_top_level_functions_after"] == 321
    assert subtraction_g2_4cw3["legacy_cli_callback_reduction"] == 9
    assert subtraction_g2_4cw3["legacy_domain_lazy_wrapper_count"] == 9
    hardening_g2_4cw3 = phase_g2_4cw3["hardening"]
    assert hardening_g2_4cw3["total_emitted_views_tamper_checked"] == 18
    assert hardening_g2_4cw3["snapshot_schema_tamper_checked"] == 3
    assert hardening_g2_4cw3["cross_lineage_tamper_checked"] == 3
    assert hardening_g2_4cw3["policy_binding_tamper_checked"] is True
    assert hardening_g2_4cw3["live_price_binding_cache_invalidation_checked"] is True
    performance_g2_4cw3 = phase_g2_4cw3["performance"]
    assert performance_g2_4cw3["pass_only_content_addressed_validation_session"] is True
    assert performance_g2_4cw3["cache_key_includes_recursive_live_bindings"] is True
    assert performance_g2_4cw3["fail_results_cached"] is False
    assert performance_g2_4cw3["observed_minimum_wall_time_improvement_percent"] >= 78.5
    assert performance_g2_4cw3["stable_full_suite_improvement_claimed"] is False
    assert performance_g2_4cw3["full_gate_reduced_for_performance"] is False
    if phase_g2_4cw3["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4cw3["validation"]["focused"]["passed"] >= 136
        assert phase_g2_4cw3["validation"]["architecture_fitness"]["passed"] >= 289
        assert phase_g2_4cw3["validation"]["contract_validation"]["passed"] >= 203
        assert phase_g2_4cw3["validation"]["full_validation"]["passed"] >= 6035
        assert phase_g2_4cw3["sources"]
        for source in phase_g2_4cw3["sources"]:
            if source["path"] in set(phase_g2_4cw3.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    phase_g2_4cx1 = baseline["phase_g2_4cx1_etf_cli_dynamic_v3_signal_diagnosis_foundation"]
    assert phase_g2_4cx1["status"] in {
        "IN_PROGRESS",
        "VALIDATING",
        "COMPLETE_G2_4_CONTINUES",
    }
    migration_g2_4cx1 = phase_g2_4cx1["migration"]
    assert migration_g2_4cx1["callback_count"] == 12
    assert migration_g2_4cx1["domain_entrypoint_count"] == 12
    assert len(migration_g2_4cx1["snapshot_schemas"]) == 4
    assert migration_g2_4cx1["no_dated_signal_event_fabrication_required"] is True
    assert migration_g2_4cx1["no_consensus_variant_fallback_required"] is True
    assert migration_g2_4cx1["missing_observation_null_and_insufficient_data_required"] is True
    assert migration_g2_4cx1["broker_action_allowed"] is False
    assert migration_g2_4cx1["production_effect"] == "none"
    subtraction_g2_4cx1 = phase_g2_4cx1["subtraction"]
    assert subtraction_g2_4cx1["legacy_cli_lines_after"] == 12693
    assert subtraction_g2_4cx1["legacy_cli_top_level_functions_after"] == 309
    assert subtraction_g2_4cx1["legacy_cli_callback_decorators_after"] == 270
    assert subtraction_g2_4cx1["legacy_cli_callback_reduction"] == 12
    assert subtraction_g2_4cx1["legacy_domain_lazy_wrapper_count"] == 12
    matrix_g2_4cx1 = phase_g2_4cx1["callback_matrix"]
    assert matrix_g2_4cx1["migrated_callback_count"] == 697
    assert matrix_g2_4cx1["pending_callback_count"] == 270
    assert matrix_g2_4cx1["phase_exit_ready"] is False
    hardening_g2_4cx1 = phase_g2_4cx1["hardening"]
    assert hardening_g2_4cx1["output_artifact_family_tamper_checked"] == 4
    assert hardening_g2_4cx1["snapshot_schema_tamper_checked"] == 4
    assert hardening_g2_4cx1["live_source_tamper_fail_closed_checked"] is True
    performance_g2_4cx1 = phase_g2_4cx1["performance"]
    assert performance_g2_4cx1["observed_wall_time_improvement_percent"] >= 61.0
    assert performance_g2_4cx1["test_policy_required_family_count"] == 6
    assert performance_g2_4cx1["production_policy_min_variant_count"] == 60
    assert performance_g2_4cx1["production_policy_max_variant_count"] == 120
    assert performance_g2_4cx1["stable_full_suite_improvement_claimed"] is False
    assert performance_g2_4cx1["full_gate_reduced_for_performance"] is False
    if phase_g2_4cx1["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4cx1["validation"]["focused"]["passed"] >= 166
        assert phase_g2_4cx1["validation"]["architecture_fitness"]["passed"] >= 292
        assert phase_g2_4cx1["validation"]["contract_validation"]["passed"] >= 203
        assert phase_g2_4cx1["validation"]["full_validation"]["passed"] >= 6040
        assert phase_g2_4cx1["sources"]
        for source in phase_g2_4cx1["sources"]:
            if source["path"] in set(phase_g2_4cx1.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    phase_g2_4cx2 = baseline["phase_g2_4cx2_etf_cli_dynamic_v3_micro_search_foundation"]
    assert phase_g2_4cx2["status"] in {
        "IN_PROGRESS",
        "VALIDATING",
        "COMPLETE_G2_4_CONTINUES",
    }
    migration_g2_4cx2 = phase_g2_4cx2["migration"]
    assert migration_g2_4cx2["callback_count"] == 12
    assert migration_g2_4cx2["domain_entrypoint_count"] == 12
    assert len(migration_g2_4cx2["snapshot_schemas"]) == 4
    assert migration_g2_4cx2["reviewed_micro_search_policy_required"] is True
    assert migration_g2_4cx2["exact_cx1_scorecard_matrix_backfill_lineage_required"] is True
    assert migration_g2_4cx2["exact_design_backfill_gate_lineage_required"] is True
    assert (
        migration_g2_4cx2["historical_calculation_and_current_quality_cache_roles_required"] is True
    )
    assert migration_g2_4cx2["insufficient_dated_evidence_inconclusive_required"] is True
    assert migration_g2_4cx2["market_regime_default_attribution_forbidden"] is True
    assert migration_g2_4cx2["broker_action_allowed"] is False
    assert migration_g2_4cx2["production_effect"] == "none"
    subtraction_g2_4cx2 = phase_g2_4cx2["subtraction"]
    assert subtraction_g2_4cx2["legacy_cli_lines_after"] == 12339
    assert subtraction_g2_4cx2["legacy_cli_top_level_functions_after"] == 297
    assert subtraction_g2_4cx2["legacy_cli_callback_decorators_after"] == 258
    assert subtraction_g2_4cx2["legacy_cli_callback_reduction"] == 12
    assert subtraction_g2_4cx2["legacy_domain_lazy_wrapper_count"] == 12
    matrix_g2_4cx2 = phase_g2_4cx2["callback_matrix"]
    assert matrix_g2_4cx2["migrated_callback_count"] == 709
    assert matrix_g2_4cx2["pending_callback_count"] == 258
    assert matrix_g2_4cx2["phase_exit_ready"] is False
    hardening_g2_4cx2 = phase_g2_4cx2["hardening"]
    assert hardening_g2_4cx2["output_artifact_family_tamper_checked"] == 4
    assert hardening_g2_4cx2["snapshot_policy_binding_tamper_checked"] == 4
    assert hardening_g2_4cx2["cross_lineage_fail_closed_checked"] is True
    performance_g2_4cx2 = phase_g2_4cx2["performance"]
    assert performance_g2_4cx2["observed_wall_time_improvement_percent"] >= 92.2
    assert performance_g2_4cx2["observed_speedup_ratio"] >= 12.8
    assert performance_g2_4cx2["stable_full_suite_improvement_claimed"] is False
    assert performance_g2_4cx2["full_gate_reduced_for_performance"] is False
    if phase_g2_4cx2["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4cx2["validation"]["focused"]["passed"] >= 169
        assert phase_g2_4cx2["validation"]["architecture_fitness"]["passed"] >= 298
        assert phase_g2_4cx2["validation"]["contract_validation"]["passed"] >= 203
        assert phase_g2_4cx2["validation"]["full_validation"]["passed"] >= 6047
        assert phase_g2_4cx2["sources"]
        for source in phase_g2_4cx2["sources"]:
            if source["path"] in set(phase_g2_4cx2.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    phase_g2_4cx3 = baseline["phase_g2_4cx3_etf_cli_dynamic_v3_research_direction_foundation"]
    assert phase_g2_4cx3["status"] in {
        "IN_PROGRESS",
        "VALIDATING",
        "COMPLETE_G2_4_CONTINUES",
    }
    migration_g2_4cx3 = phase_g2_4cx3["migration"]
    assert migration_g2_4cx3["callback_count"] == 6
    assert migration_g2_4cx3["domain_entrypoint_count"] == 6
    assert migration_g2_4cx3["snapshot_schemas"] == [
        "next_research_direction_input_snapshot.v2",
        "owner_research_roadmap_input_snapshot.v2",
    ]
    assert migration_g2_4cx3["reviewed_direction_policy_required"] is True
    assert migration_g2_4cx3["exact_attribution_to_direction_to_roadmap_lineage_required"] is True
    assert migration_g2_4cx3["insufficient_evidence_defer_mapping_required"] is True
    assert migration_g2_4cx3["unknown_shift_default_forbidden"] is True
    assert migration_g2_4cx3["historical_downstream_is_not_current_evidence_required"] is True
    assert migration_g2_4cx3["proposed_owner_review_only_required"] is True
    assert migration_g2_4cx3["broker_action_allowed"] is False
    assert migration_g2_4cx3["production_effect"] == "none"
    if phase_g2_4cx3["status"] == "COMPLETE_G2_4_CONTINUES":
        subtraction_g2_4cx3 = phase_g2_4cx3["subtraction"]
        assert subtraction_g2_4cx3["legacy_cli_lines_after"] == 12196
        assert subtraction_g2_4cx3["legacy_cli_top_level_functions_after"] == 291
        assert subtraction_g2_4cx3["legacy_cli_callback_decorators_after"] == 252
        assert subtraction_g2_4cx3["legacy_cli_callback_reduction"] == 6
        assert subtraction_g2_4cx3["legacy_domain_lazy_wrapper_count"] == 6
        matrix_g2_4cx3 = phase_g2_4cx3["callback_matrix"]
        assert matrix_g2_4cx3["migrated_callback_count"] == 715
        assert matrix_g2_4cx3["pending_callback_count"] == 252
        assert matrix_g2_4cx3["phase_exit_ready"] is False
        hardening_g2_4cx3 = phase_g2_4cx3["hardening"]
        assert hardening_g2_4cx3["output_view_tamper_checked"] == 10
        assert hardening_g2_4cx3["snapshot_policy_binding_tamper_checked"] == 2
        assert hardening_g2_4cx3["snapshot_safety_tamper_checked"] == 2
        assert hardening_g2_4cx3["source_binding_file_set_tamper_checked"] == 2
        assert hardening_g2_4cx3["source_binding_path_tamper_checked"] == 2
        assert hardening_g2_4cx3["cross_lineage_fail_closed_checked"] is True
        assert hardening_g2_4cx3["evidence_status_shift_mismatch_fail_closed_checked"] is True
        assert hardening_g2_4cx3["sufficient_evidence_checklist_branch_checked"] is True
        assert hardening_g2_4cx3["all_materialized_views_byte_rebuild_required"] is True
        performance_g2_4cx3 = phase_g2_4cx3["performance"]
        assert performance_g2_4cx3["stable_full_suite_improvement_claimed"] is False
        assert performance_g2_4cx3["full_gate_reduced_for_performance"] is False
        assert phase_g2_4cx3["validation"]["focused"]["passed"] >= 28
        assert phase_g2_4cx3["validation"]["architecture_fitness"]["passed"] >= 300
        assert phase_g2_4cx3["validation"]["contract_validation"]["passed"] >= 203
        assert phase_g2_4cx3["validation"]["full_validation"]["passed"] >= 6050
        assert phase_g2_4cx3["sources"]
        for source in phase_g2_4cx3["sources"]:
            if source["path"] in set(phase_g2_4cx3.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    eb0_s2b = baseline["integrated_change_arch_004g2_eb0_s2b"]
    assert eb0_s2b["status"] == "VALIDATING"
    assert eb0_s2b["task_id"] == "ARCH-004G2_REMAINING_PHASE_EFFICIENCY_EXECUTION"
    assert eb0_s2b["behavior"] == "bounded_high_node_compatibility_fingerprint_reuse"
    assert eb0_s2b["root_cause"] == {
        "snapshot_size_bytes": 9_531_096,
        "snapshot_json_node_count": 226_197,
        "snapshot_bound_path_count": 45,
        "previous_json_node_limit": 100_000,
        "current_json_node_limit": 500_000,
        "pre_fix_observed_read_gib": 171.05,
        "pre_fix_elapsed_lower_bound_seconds": 947.08,
    }
    assert eb0_s2b["safety"] == {
        "max_document_size_bytes": 64 * 1024 * 1024,
        "max_bound_path_count": 4_096,
        "pass_only_cache": True,
        "before_after_fingerprint_required": True,
        "link_and_topology_gate_preserved": True,
        "above_node_limit_bypasses_cache": True,
        "production_effect": "none",
    }
    assert eb0_s2b["validation"]["cache_hardening"]["passed"] == 78
    assert eb0_s2b["validation"]["same_node_same_command"] == {
        "status": "PASS",
        "passed": 1,
        "elapsed_seconds": 172.23,
        "slowest_call_seconds": 168.66,
    }
    assert eb0_s2b["validation"]["smoothed_focused"] == {
        "status": "PASS",
        "passed": 27,
        "elapsed_seconds": 498.56,
    }
    assert eb0_s2b["validation"]["confirmation_targets_focused"] == {
        "status": "PASS",
        "passed": 10,
        "elapsed_seconds": 85.37,
        "pre_fix_elapsed_lower_bound_seconds": 665.0,
        "reduction_lower_bound_percent": 87.2,
    }
    assert eb0_s2b["validation"]["advisory_proposal_review_focused"] == {
        "status": "PASS",
        "passed": 13,
        "elapsed_seconds": 119.35,
        "isolated_worker_elapsed_seconds": 450.0,
        "reduction_percent_approx": 73.5,
    }
    assert eb0_s2b["validation"]["forward_plan_and_rule_review_focused"] == {
        "status": "PASS",
        "passed": 22,
        "elapsed_seconds": 209.97,
        "forward_plan_reduction_percent_approx": 65.0,
        "rule_review_reduction_percent_approx": 70.0,
    }
    assert eb0_s2b["validation"]["confirmation_direct_chain_focused"] == {
        "status": "PASS",
        "passed": 45,
        "elapsed_seconds": 235.97,
    }
    assert eb0_s2b["validation"]["correctness_shards"] == {
        "status": "PASS",
        "passed": 5_782,
        "skipped": 1,
        "failed": 0,
        "junit_wall_seconds": [246.411, 371.109, 313.790, 249.942],
    }
    assert eb0_s2b["validation"]["historical_top45_diagnostic"] == {
        "status": "DIAGNOSTIC_STOP",
        "elapsed_seconds": 1_253,
        "full_pass_claimed": False,
        "peak_working_set_gib": 6.33,
        "peak_private_gib": 17.84,
        "available_memory_gib": 62.0,
        "residual_owner": "weight_search",
        "active_file_count": 6,
        "active_files": [
            "tests/test_weight_expanded_search.py",
            "tests/test_formal_method_auto_plan.py",
            "tests/test_near_miss_ab_comparison.py",
            "tests/test_search_coverage_gap.py",
            "tests/test_weight_top_candidate_interpretation.py",
            "tests/test_weight_candidate_cluster.py",
        ],
    }
    assert eb0_s2b["validation"]["formal_focused"] == {
        "status": "PASS",
        "passed": 274,
        "skipped": 1,
        "elapsed_seconds": 248.21,
        "junit_artifact": (
            "outputs/validation_runtime/arch004g2-eb0-focused_20260717T073027Z/junit.xml"
        ),
    }
    assert eb0_s2b["validation"]["architecture_fitness_first_clean_candidate"] == {
        "status": "FAIL",
        "passed": 310,
        "failed": 1,
        "elapsed_seconds": 62.70,
        "runtime_artifact": (
            "outputs/validation_runtime/architecture-fitness_20260717T073451Z/"
            "test_runtime_summary.json"
        ),
        "reason": "deprecation_inventory_raw_worktree_eol_hash_drift",
    }
    assert eb0_s2b["validation"]["architecture_fitness"]["status"] == "PASS"
    assert eb0_s2b["validation"]["architecture_fitness"]["passed"] == 312
    assert eb0_s2b["validation"]["contract_validation"]["status"] == "PASS"
    assert eb0_s2b["validation"]["contract_validation"]["passed"] == 204
    assert eb0_s2b["validation"]["full_validation"] == {
        "status": "PASS",
        "passed": 6_195,
        "skipped": 2,
        "warnings": 642,
        "elapsed_seconds": 2_138.84,
        "runtime_artifact": (
            "outputs/validation_runtime/full_20260717T075427Z/test_runtime_summary.json"
        ),
    }
    assert eb0_s2b["checkout_reproducibility"] == {
        "first_architecture_gate_status": "FAIL",
        "first_architecture_gate_passed": 302,
        "first_architecture_gate_failed": 2,
        "first_architecture_gate_elapsed_seconds": 55.60,
        "absolute_root_drift_node_count": 987,
        "project_relative_token": "<PROJECT_ROOT>",
        "runtime_defaults_changed": False,
        "cli_surface_changed": False,
        "deprecation_inventory_text_hash_policy": "universal_newline_lf",
        "deprecation_inventory_raw_byte_count_before": 528_634,
        "deprecation_inventory_canonical_byte_count": 515_757,
        "deprecation_inventory_source_content_changed": False,
        "production_effect": "none",
    }
    for source in eb0_s2b["sources"]:
        if source["path"] in set(eb0_s2b.get("superseded_source_paths", [])):
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]

    eb0_s3a = baseline["integrated_change_arch_004g2_eb0_s3a_weight_search_tail"]
    assert eb0_s3a["status"] == "COMPLETE_RUNTIME_TASK_CONTINUES"
    assert eb0_s3a["task_id"] == "ARCH-004G2_VALIDATION_RUNTIME_BUDGET_AND_FIXTURE_REUSE"
    assert eb0_s3a["behavior"] == "weight_search_tail_pass_only_validation_reuse"
    assert eb0_s3a["pre_change_full"] == {
        "passed": 6_195,
        "skipped": 2,
        "warnings": 642,
        "elapsed_seconds": 2_138.84,
        "runtime_artifact": (
            "outputs/validation_runtime/full_20260717T075427Z/test_runtime_summary.json"
        ),
    }
    assert eb0_s3a["same_command"] == {
        "node_count": 5,
        "before_elapsed_seconds": 889.01,
        "after_elapsed_seconds": 254.19,
        "reduction_percent": 71.4,
    }
    assert eb0_s3a["isolated_nodes"] == {
        "followup": {
            "before_seconds": 884.24,
            "after_seconds": 352.36,
            "reduction_percent": 60.1,
        },
        "decision": {
            "before_seconds": 500.55,
            "after_seconds": 102.93,
            "reduction_percent": 79.4,
        },
    }
    assert eb0_s3a["post_change_full"]["passed"] == 6_195
    assert eb0_s3a["post_change_full"]["elapsed_seconds"] == 1_789.86
    assert eb0_s3a["post_change_full"]["reduction_percent"] == 16.3
    assert eb0_s3a["post_change_full"]["followup_seconds"] == 408.82
    assert eb0_s3a["post_change_full"]["decision_hardening_seconds"] == 157.40
    assert eb0_s3a["safety"] == {
        "first_full_builder_preserved": True,
        "all_view_rebuild_preserved": True,
        "tamper_matrix_preserved": True,
        "cross_lineage_fail_closed_preserved": True,
        "pass_only_cache": True,
        "fail_not_cached": True,
        "strategy_logic_changed": False,
        "cached_data_mutated": False,
        "production_effect": "none",
    }
    assert eb0_s3a["scope"]["research_foundation_started"] is False
    assert eb0_s3a["scope"]["eb1_started"] is False
    assert eb0_s3a["validation"]["same_command"]["status"] == "PASS"
    assert eb0_s3a["validation"]["decision_isolated"]["status"] == "PASS"
    assert eb0_s3a["validation"]["scoped_ruff"]["status"] == "PASS"
    assert eb0_s3a["validation"]["formal_focused"] == {
        "status": "PASS",
        "passed": 291,
        "skipped": 1,
        "elapsed_seconds": 269.09,
        "junit_artifact": (
            "outputs/validation_runtime/arch004g2-eb0-s3a-focused_20260717T221426Z/junit.xml"
        ),
    }
    assert eb0_s3a["validation"]["architecture_fitness"]["status"] == "PASS"
    assert eb0_s3a["validation"]["architecture_fitness"]["passed"] == 312
    assert eb0_s3a["validation"]["contract_validation"]["status"] == "PASS"
    assert eb0_s3a["validation"]["contract_validation"]["passed"] == 204
    assert eb0_s3a["validation"]["full_validation"]["status"] == "PASS"
    assert eb0_s3a["validation"]["full_validation"]["passed"] == 6_195
    assert "s3n_adaptive_equal_risk_tail_closeout" not in phase_b
    s3n = eb0_s3a["s3n_adaptive_equal_risk_tail_closeout"]
    assert s3n["status"] == "COMPLETE_RUNTIME_TASK_CONTINUES"
    assert s3n["base_commit"] == "13d85f1e"
    assert set(s3n["lanes"]) == {
        "weight_adaptive_outer_session",
        "equal_risk_restart_cli_canonical_json_reuse",
        "equal_risk_tilt_cli_dag_canonical_json_reuse",
    }
    restart_lane = s3n["lanes"]["equal_risk_restart_cli_canonical_json_reuse"]
    assert restart_lane["real_cli_count_before"] == 1
    assert restart_lane["real_cli_count_after"] == 1
    assert restart_lane["real_cli_count_preserved"] is True
    tilt_lane = s3n["lanes"]["equal_risk_tilt_cli_dag_canonical_json_reuse"]
    assert tilt_lane["real_cli_count_before"] == 4
    assert tilt_lane["real_cli_count_after"] == 4
    assert tilt_lane["real_cli_count_preserved"] is True
    assert s3n["validation"]["full_validation"]["run_count"] == 1
    assert s3n["validation"]["full_validation"]["node_count"] == 6_248
    assert s3n["validation"]["full_validation"]["file_count"] == 1_068
    assert s3n["validation"]["active_source_count"] == 77
    assert len(eb0_s3a["sources"]) >= s3n["validation"]["active_source_count"]
    assert s3n["validation"]["worktree_attribution"] == {
        "status": "PASS",
        "changed_tracked_path_count": 12,
        "declared_changed_tracked_path_count": 12,
        "excluded_user_path_count": 3,
    }
    assert s3n["validation"]["post_full_tracked_state_pending"] is False
    assert s3n["next_work"]["post_full_pass_satisfied"] is True
    assert s3n["next_work"]["second_full_allowed"] is False
    assert s3n["next_phase_or_slice_unblocked"] is False
    s4 = eb0_s3a["s4_full_trigger_provenance"]
    assert s4["status"] == "COMPLETE_RUNTIME_TASK_CONTINUES"
    assert s4["base_commit"] == "2962e02f"
    assert s4["owner_authorization"] == {
        "selected_option": "A",
        "authorized_increment": "S4_FULL_TRIGGER_PROVENANCE",
        "return_to_g2_4_coordination_point_after_closeout": True,
        "eb1_requires_new_explicit_owner_instruction": True,
    }
    assert s4["contract"]["cli_over_environment_precedence"] == "whole_envelope"
    assert s4["contract"]["profile_binding_status_required_for_performance_pass"] is True
    assert s4["contract"]["full_benchmark_runtime_profile_status"] == "NOT_APPLICABLE"
    assert "runtime_profile_sha256" in s4["contract"]["failure_fix_parent_binding"]["binds"]
    assert s4["contract"]["failure_fix_parent_binding"]["formal_parent_proof"][-1] == (
        "inventory_sha_size_fresh"
    )
    assert s4["contract"]["benchmark_inherited_formal_profile_provenance_env_removed"] is True
    assert s4["contract"]["malformed_json_types_fail_closed_without_validator_exception"] is True
    assert s4["validation"]["full_run_count"] == 0
    assert s4["validation"]["full_validation_required_for_s4"] is False
    assert s4["validation"]["architecture_fitness"]["status"] == ("PASS_AFTER_FRESHNESS_CORRECTION")
    assert s4["validation"]["contract_validation"]["status"] == "PASS"
    assert s4["validation"]["active_source_count"] == len(eb0_s3a["sources"])
    assert s4["next_phase_or_slice_unblocked"] is False
    for source in eb0_s3a["sources"]:
        if source["path"] in set(eb0_s3a.get("superseded_source_paths", [])):
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]

    eb1 = baseline["phase_g2_4eb1_etf_cli_dynamic_v3_signal_filter_foundation"]
    assert eb1["status"] in {
        "VALIDATING_G2_4_CONTINUES",
        "COMPLETE_G2_4_CONTINUES",
    }
    assert eb1["base_commit"] == "ee604385"
    assert eb1["boundary_id"] == "ARCH-004G2.4-EB1"
    assert eb1["migration"]["callback_count"] == 15
    assert eb1["migration"]["domain_public_entrypoint_count"] == 15
    assert len(eb1["migration"]["callback_ids"]) == 15
    assert len(set(eb1["migration"]["callback_ids"])) == 15
    assert eb1["contract"]["input_snapshot_schemas"] == [
        "signal_failure_taxonomy_input_snapshot.v2",
        "candidate_signal_ledger_input_snapshot.v2",
        "signal_churn_root_cause_input_snapshot.v2",
        "regime_mismatch_attribution_input_snapshot.v2",
        "candidate_quality_filter_design_input_snapshot.v2",
    ]
    assert eb1["contract"]["materialized_view_count"] == 23
    assert eb1["contract"]["missing_dated_rows"] == {
        "events": "empty",
        "method_count": None,
        "method_return": None,
        "downstream_status": "INSUFFICIENT_DATA",
        "mitigations_or_filters_allowed": False,
    }
    assert eb1["contract"]["aggregate_proxy_may_create_dated_evidence"] is False
    assert eb1["subtraction"] == {
        "legacy_cli_lines_before": 12_196,
        "legacy_cli_lines_after": 11_837,
        "legacy_cli_top_level_functions_before": 291,
        "legacy_cli_top_level_functions_after": 276,
        "legacy_cli_decorators_before": 252,
        "legacy_cli_decorators_after": 237,
        "legacy_domain_lines_before": 7_010,
        "legacy_domain_lines_after": 5_668,
        "compatibility_wrapper_count": 15,
        "duplicate_implementation_retained": False,
    }
    assert eb1["callback_matrix"] == {
        "baseline_callback_count": 967,
        "migrated_callback_count": 730,
        "pending_callback_count": 237,
        "unresolved_callback_count": 0,
        "duplicate_registration_count": 0,
        "phase_exit_criteria_passed": False,
    }
    assert eb1["cli_contract"]["leaf_command_count"] == 993
    assert eb1["cli_contract"]["duplicate_path_count"] == 0
    assert eb1["hardening"]["no_fabricated_events_or_forward_returns"] is True
    assert eb1["safety"]["production_effect"] == "none"
    assert eb1["next_work"]["eb2_requires_new_explicit_owner_instruction"] is True
    assert eb1["next_work"]["phase_exit_or_handoff_triggered"] is False
    assert eb1["next_phase_or_slice_unblocked"] is False
    assert eb1["sources"]
    eb1_superseded = set(eb1["superseded_source_paths"])
    for source in eb1["sources"]:
        if source["path"] in eb1_superseded:
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]

    eb2 = baseline["phase_g2_4eb2_etf_cli_dynamic_v3_filtered_candidate_pipeline"]
    assert eb2["status"] in {
        "VALIDATING_G2_4_CONTINUES",
        "COMPLETE_G2_4_CONTINUES",
    }
    assert eb2["base_commit"] == "9c0025e4"
    assert eb2["boundary_id"] == "ARCH-004G2.4-EB2"
    assert eb2["migration"]["callback_count"] == 15
    assert eb2["migration"]["domain_public_entrypoint_count"] == 15
    assert len(eb2["migration"]["callback_ids"]) == 15
    assert len(set(eb2["migration"]["callback_ids"])) == 15
    assert eb2["contract"]["input_snapshot_schemas"] == [
        "filtered_candidate_backfill_input_snapshot.v2",
        "filtered_vs_original_comparison_input_snapshot.v2",
        "signal_gate_experiment_input_snapshot.v2",
        "filtered_candidate_promotion_review_input_snapshot.v2",
        "owner_signal_roadmap_input_snapshot.v2",
    ]
    assert eb2["contract"]["materialized_view_count"] == 24
    assert eb2["contract"]["empty_or_missing_evidence"] == {
        "variant_rows": "empty",
        "performance_rows": "empty",
        "comparison_rows": "empty",
        "gate_rows": "empty",
        "winner": None,
        "candidate": None,
        "confidence": None,
        "downstream_status": "INSUFFICIENT_DATA",
    }
    assert eb2["contract"]["synthesized_performance_or_denominator_allowed"] is False
    assert eb2["subtraction"] == {
        "legacy_cli_lines_before": 11_837,
        "legacy_cli_lines_after": 11_456,
        "legacy_cli_top_level_functions_before": 276,
        "legacy_cli_top_level_functions_after": 261,
        "legacy_cli_decorators_before": 237,
        "legacy_cli_decorators_after": 222,
        "legacy_domain_lines_before": 5_668,
        "legacy_domain_lines_after": 4_554,
        "compatibility_wrapper_count": 15,
        "duplicate_implementation_retained": False,
    }
    assert eb2["callback_matrix"] == {
        "baseline_callback_count": 967,
        "migrated_callback_count": 745,
        "pending_callback_count": 222,
        "unresolved_callback_count": 0,
        "duplicate_registration_count": 0,
        "phase_exit_criteria_passed": False,
    }
    assert eb2["cli_contract"]["leaf_command_count"] == 993
    assert eb2["cli_contract"]["duplicate_path_count"] == 0
    assert eb2["hardening"]["no_fabricated_filtered_outcomes_or_winner"] is True
    assert eb2["safety"]["production_effect"] == "none"
    assert eb2["next_work"] == {
        "pre_bootstrap_requires_eb2_integration_gate": False,
        "pre_bootstrap_unblocked": True,
        "eb3_or_formal_arch_005_s0_unblocked": False,
        "phase_exit_or_handoff_triggered": False,
    }
    assert eb2["next_work"]["phase_exit_or_handoff_triggered"] is False
    assert eb2["next_phase_or_slice_unblocked"] is False
    assert eb2["sources"]
    eb2_superseded = set(eb2["superseded_source_paths"])
    for source in eb2["sources"]:
        if source["path"] in eb2_superseded:
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]

    eb3 = baseline["phase_g2_4eb3_etf_cli_dynamic_v3_filtered_candidate_readiness"]
    assert eb3["status"] in {
        "VALIDATING_G2_4_CONTINUES",
        "COMPLETE_G2_4_CONTINUES",
    }
    assert eb3["base_commit"] == "073a0c57"
    assert eb3["boundary_id"] == "ARCH-004G2.4-EB3"
    assert eb3["migration"]["callback_count"] == 30
    assert eb3["migration"]["domain_public_entrypoint_count"] == 30
    assert len(eb3["migration"]["callback_ids"]) == 30
    assert len(set(eb3["migration"]["callback_ids"])) == 30
    assert eb3["contract"]["input_snapshot_schemas"] == [
        "filtered_candidate_evidence_input_snapshot.v2",
        "median_regime_filter_spec_input_snapshot.v2",
        "filtered_candidate_stress_backfill_input_snapshot.v2",
        "drawdown_mismatch_reduction_input_snapshot.v2",
        "flip_rotation_reduction_input_snapshot.v2",
        "filtered_candidate_ab_review_input_snapshot.v2",
        "signal_gate_confirmation_input_snapshot.v2",
        "filtered_formalization_readiness_input_snapshot.v2",
        "owner_filtered_candidate_review_input_snapshot.v2",
        "filtered_next_decision_input_snapshot.v2",
    ]
    assert eb3["contract"]["policy_schema"] == "filtered_formalization_policy.v1"
    assert eb3["contract"]["materialized_view_count"] == 47
    assert eb3["contract"]["empty_or_missing_dated_evidence"] == {
        "observed_rows": "empty",
        "confirmation_targets": "empty",
        "rates_and_metrics": None,
        "winner_and_confidence": None,
        "specification_status": "RESEARCH_SPEC_ONLY",
        "downstream_status": "INSUFFICIENT_DATA",
        "formal_research_method_status": "NOT_READY",
        "promotion_state": "NEEDS_MORE_EVIDENCE",
        "next_decision": "COLLECT_DATED_EVIDENCE",
    }
    assert eb3["subtraction"] == {
        "legacy_cli_lines_before": 11_456,
        "legacy_cli_lines_after": 10_725,
        "legacy_cli_top_level_functions_before": 261,
        "legacy_cli_top_level_functions_after": 231,
        "legacy_cli_decorators_before": 222,
        "legacy_cli_decorators_after": 192,
        "legacy_domain_lines_before": 6_155,
        "legacy_domain_lines_after": 4_114,
        "compatibility_wrapper_count": 30,
        "duplicate_implementation_retained": False,
    }
    assert eb3["callback_matrix"] == {
        "baseline_callback_count": 967,
        "migrated_callback_count": 775,
        "pending_callback_count": 192,
        "unresolved_callback_count": 0,
        "duplicate_registration_count": 0,
        "phase_exit_criteria_passed": False,
    }
    assert eb3["cli_contract"]["leaf_command_count"] == 993
    assert eb3["cli_contract"]["duplicate_path_count"] == 0
    assert eb3["hardening"]["no_synthetic_stress_performance_or_confirmation_targets"] is True
    assert eb3["safety"]["production_effect"] == "none"
    assert eb3["next_work"] == {
        "eb4_requires_new_explicit_owner_instruction": True,
        "formal_arch_005_s0_unblocked": False,
        "g2_5_unblocked": False,
        "phase_exit_or_handoff_triggered": False,
    }
    assert eb3["next_phase_or_slice_unblocked"] is False
    assert eb3["sources"]
    eb3_superseded = set(eb3["superseded_source_paths"])
    for source in eb3["sources"]:
        if source["path"] in eb3_superseded:
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]

    eb4 = baseline["phase_g2_4eb4_etf_cli_evidence_materialization_and_input_readiness"]
    assert eb4["status"] in {
        "VALIDATING_G2_4_CONTINUES",
        "COMPLETE_G2_4_CONTINUES",
    }
    assert eb4["base_commit"] == "26a45e0d"
    assert eb4["boundary_id"] == "ARCH-004G2.4-EB4"
    assert eb4["owner_authorization"] == {
        "instruction": "先按照这个顺序推进到可以考虑开始推进研究策略前把",
        "eb4_through_handoff_authorized": True,
        "arch_005_s0_s1_after_handoff_authorized": True,
        "g2_5_requires_later_explicit_instruction": True,
    }
    assert eb4["migration"]["callback_count"] == 39
    assert eb4["migration"]["app_callback_count"] == 26
    assert eb4["migration"]["matching_validator_callback_count"] == 13
    assert eb4["migration"]["domain_public_entrypoint_count"] == 39
    assert len(eb4["migration"]["canonical_interfaces"]) == 3
    assert len(eb4["migration"]["callback_ids"]) == 39
    assert len(set(eb4["migration"]["callback_ids"])) == 39
    assert len(eb4["contract"]["input_snapshot_schemas"]) == 14
    assert eb4["contract"]["materialized_view_count"] == 63
    assert eb4["contract"]["missing_or_unqualified_evidence"] == {
        "observed_metrics": None,
        "observed_rows": "empty",
        "status": "INSUFFICIENT_DATA",
        "promotion_ready": False,
        "automatic_promotion": False,
    }
    assert eb4["subtraction"] == {
        "legacy_cli_lines_before": 10_725,
        "legacy_cli_lines_after": 9_065,
        "legacy_cli_top_level_functions_before": 231,
        "legacy_cli_top_level_functions_after": 192,
        "legacy_cli_decorators_before": 192,
        "legacy_cli_decorators_after": 153,
        "legacy_readiness_domain_lines_before": 4_114,
        "legacy_readiness_domain_lines_after": 2_978,
        "compatibility_wrapper_count": 6,
        "duplicate_implementation_retained": False,
    }
    assert eb4["callback_matrix"] == {
        "baseline_callback_count": 967,
        "migrated_callback_count": 814,
        "pending_callback_count": 153,
        "unresolved_callback_count": 0,
        "duplicate_registration_count": 0,
        "phase_exit_criteria_passed": False,
    }
    assert eb4["cli_contract"] == {
        "root_command_count": 41,
        "group_count": 291,
        "leaf_command_count": 993,
        "duplicate_path_count": 0,
        "command_tree_sha256": ("01c78550ae58b38c2d8cca0683376643e2934f93e324710612c87d39eea7302d"),
    }
    assert eb4["focused_validation"]["performance_triggered"] is False
    assert eb4["formal_validation"]["architecture_fitness"]["status"] == "PASS"
    assert eb4["formal_validation"]["contract_validation"]["status"] == "PASS"
    full_validation = eb4["formal_validation"]["full_validation"]
    assert full_validation["status"] == "PASS_AFTER_AUDITED_FAILURE_FIX_RERUN"
    assert full_validation["actual_natural_run_count"] == 1
    assert full_validation["natural_run"]["status"] == "FAIL"
    assert full_validation["natural_run"]["failed"] == 7
    assert full_validation["failure_fix_rerun"]["status"] == "PASS"
    assert full_validation["failure_fix_rerun"]["passed"] == 6_373
    assert full_validation["failure_fix_rerun"]["failed"] == 0
    assert full_validation["failure_fix_rerun"]["scheduler_fallback"] is False
    assert full_validation["failure_fix_rerun"]["production_effect"] == "none"
    assert full_validation["duration_profile_refresh"]["version"] == 11
    assert eb4["formal_validation"]["architecture_devex"]["violation_count"] == 0
    assert eb4["formal_validation"]["deprecation_inventory"]["test_file_count"] == 1_130
    assert eb4["safety"]["production_effect"] == "none"
    assert eb4["next_work"] == {
        "eb5_after_eb4_integration_gate": True,
        "eb5_unblocked": True,
        "whole_g2_4_phase_exit_passed": False,
        "formal_arch_005_s0_unblocked": False,
        "g2_5_unblocked": False,
        "phase_exit_or_handoff_triggered": False,
    }
    assert eb4["next_phase_or_slice_unblocked"] is True
    assert len(eb4["sources"]) == 57
    eb4_superseded = set(eb4["superseded_source_paths"])
    for source in eb4["sources"]:
        if source["path"] in eb4_superseded:
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]

    eb5 = baseline["phase_g2_4eb5_paper_shadow_health_recovery_and_decision_support"]
    assert eb5["status"] in {
        "VALIDATING_G2_4_CONTINUES",
        "COMPLETE_G2_4_CONTINUES",
    }
    assert eb5["base_commit"] == ("8332e9613e3fabd542f61689c899bb90dc1bd995")
    assert eb5["boundary_id"] == "ARCH-004G2.4-EB5"
    assert eb5["owner_authorization"] == {
        "instruction": "先按照这个顺序推进到可以考虑开始推进研究策略前把",
        "eb5_through_handoff_authorized": True,
        "arch_005_s0_s1_after_handoff_authorized": True,
        "g2_5_requires_later_explicit_instruction": True,
    }
    assert eb5["migration"]["callback_count"] == 37
    assert eb5["migration"]["app_callback_count"] == 24
    assert eb5["migration"]["matching_validator_callback_count"] == 13
    assert eb5["migration"]["domain_public_entrypoint_count"] == 37
    assert len(eb5["migration"]["canonical_interfaces"]) == 3
    assert len(eb5["contract"]["input_snapshot_schemas"]) == 13
    assert eb5["contract"]["input_snapshot_sha256_sealed"] is True
    assert eb5["contract"]["validation_cache_pass_only"] is True
    assert eb5["contract"]["validation_cache_content_fingerprint_bound"] is True
    assert eb5["subtraction"] == {
        "legacy_cli_lines_before": 9_065,
        "legacy_cli_lines_after": 6_572,
        "legacy_cli_top_level_functions_before": 192,
        "legacy_cli_top_level_functions_after": 142,
        "legacy_cli_decorators_before": 153,
        "legacy_cli_decorators_after": 116,
        "duplicate_implementation_retained": False,
    }
    assert eb5["callback_matrix"] == {
        "baseline_callback_count": 967,
        "migrated_callback_count": 851,
        "pending_callback_count": 116,
        "unresolved_callback_count": 0,
        "duplicate_registration_count": 0,
        "phase_exit_criteria_passed": False,
    }
    assert eb5["cli_contract"] == {
        "root_command_count": 41,
        "group_count": 291,
        "leaf_command_count": 993,
        "duplicate_path_count": 0,
        "command_tree_sha256": ("01c78550ae58b38c2d8cca0683376643e2934f93e324710612c87d39eea7302d"),
    }
    assert eb5["focused_validation"]["corrected_family_run"]["passed"] == 50
    assert eb5["focused_validation"]["elapsed_reduction_percent"] == 45.3
    assert eb5["focused_validation"]["performance_triggered"] is False
    assert eb5["hardening"]["source_tamper_fail_closed"] is True
    assert eb5["hardening"]["snapshot_tamper_fail_closed"] is True
    assert eb5["hardening"]["output_tamper_fail_closed"] is True
    assert eb5["hardening"]["cross_lineage_tamper_fail_closed"] is True
    assert eb5["safety"]["production_effect"] == "none"
    assert len(eb5["sources"]) == 38
    if eb5["status"] == "VALIDATING_G2_4_CONTINUES":
        assert eb5["formal_validation"]["architecture_fitness"]["status"] == "PENDING"
        assert eb5["formal_validation"]["contract_validation"]["status"] == "PENDING"
        assert eb5["formal_validation"]["full_validation"]["status"] == "PENDING"
        assert eb5["next_phase_or_slice_unblocked"] is False
        assert all(source["sha256"] == 0 for source in eb5["sources"])
    else:
        assert eb5["formal_validation"]["architecture_fitness"]["status"] == "PASS"
        assert eb5["formal_validation"]["contract_validation"]["status"] == "PASS"
        assert eb5["formal_validation"]["full_validation"]["status"] in {
            "PASS",
            "PASS_AFTER_AUDITED_FAILURE_FIX_RERUN",
        }
        assert eb5["next_phase_or_slice_unblocked"] is True
        eb5_superseded = set(eb5["superseded_source_paths"])
        assert len(eb5_superseded) == 18
        for source in eb5["sources"]:
            if source["path"] in eb5_superseded:
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    eb6 = baseline["phase_g2_4eb6_weight_calibration_and_research_interfaces"]
    assert eb6["status"] in {
        "VALIDATING_G2_4_CONTINUES",
        "COMPLETE_G2_4_CONTINUES",
    }
    assert eb6["base_commit"] == ("e5fc456c4f2c67466f6f8f78f551e7dc69801644")
    assert eb6["boundary_id"] == "ARCH-004G2.4-EB6"
    assert eb6["migration"] == {
        "callback_count": 40,
        "weight_calibration_callback_count": 20,
        "weight_research_callback_count": 20,
        "canonical_interfaces": [
            "src/ai_trading_system/interfaces/cli/etf_portfolio/weight_calibration.py",
            "src/ai_trading_system/interfaces/cli/etf_portfolio/weight_research.py",
        ],
        "compatibility_app_reexports": True,
        "compatibility_callback_wrappers": False,
    }
    assert eb6["contract"] == {
        "interface_ownership_only": True,
        "weight_or_threshold_semantics_changed": False,
        "sample_holdout_regime_or_date_semantics_changed": False,
        "ranking_recommendation_or_promotion_semantics_changed": False,
        "data_quality_and_policy_provenance_preserved": True,
        "cli_path_help_default_exit_and_output_preserved": True,
        "latest_json_helper_has_no_investment_semantics": True,
    }
    assert eb6["subtraction"] == {
        "legacy_cli_lines_before": 6572,
        "legacy_cli_lines_after": 4038,
        "legacy_cli_top_level_functions_before": 142,
        "legacy_cli_top_level_functions_after": 100,
        "legacy_cli_decorators_before": 116,
        "legacy_cli_decorators_after": 76,
        "duplicate_implementation_retained": False,
    }
    assert eb6["callback_matrix"] == {
        "baseline_callback_count": 967,
        "migrated_callback_count": 891,
        "pending_callback_count": 76,
        "unresolved_callback_count": 0,
        "duplicate_registration_count": 0,
        "phase_exit_criteria_passed": False,
    }
    assert eb6["cli_contract"] == {
        "root_command_count": 41,
        "group_count": 291,
        "leaf_command_count": 993,
        "duplicate_path_count": 0,
        "command_tree_sha256": ("01c78550ae58b38c2d8cca0683376643e2934f93e324710612c87d39eea7302d"),
    }
    assert eb6["focused_validation"]["pre_migration_baseline"]["passed"] == 231
    assert eb6["focused_validation"]["corrected_post_migration_run"]["passed"] == 231
    assert eb6["focused_validation"]["performance_triggered"] is False
    assert eb6["safety"]["strategy_logic_changed"] is False
    assert eb6["safety"]["production_effect"] == "none"
    assert len(eb6["sources"]) == 42
    if eb6["status"] == "VALIDATING_G2_4_CONTINUES":
        assert eb6["formal_validation"]["architecture_fitness"]["status"] == "PENDING"
        assert eb6["formal_validation"]["contract_validation"]["status"] == "PENDING"
        assert eb6["formal_validation"]["full_validation"]["status"] == "PENDING"
        assert eb6["next_phase_or_slice_unblocked"] is False
        assert all(source["sha256"] == 0 for source in eb6["sources"])
    else:
        assert eb6["formal_validation"]["architecture_fitness"]["status"] == "PASS"
        assert eb6["formal_validation"]["contract_validation"]["status"] == "PASS"
        assert eb6["formal_validation"]["full_validation"]["status"] in {
            "PASS",
            "PASS_AFTER_AUDITED_FAILURE_FIX_RERUN",
        }
        assert eb6["next_phase_or_slice_unblocked"] is True
        eb6_superseded = set(eb6.get("superseded_source_paths", []))
        for source in eb6["sources"]:
            if source["path"] in eb6_superseded:
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    eb7 = baseline["phase_g2_4eb7_residual_cli_interfaces"]
    assert eb7["status"] in {
        "VALIDATING_G2_4_CONTINUES",
        "COMPLETE_G2_4_CONTINUES",
    }
    assert eb7["base_commit"] == "4371b419cf9b18c7a8445658c06bddb073eca004"
    assert eb7["boundary_id"] == "ARCH-004G2.4-EB7"
    assert eb7["migration"] == {
        "callback_count": 40,
        "dynamic_shadow_callback_count": 6,
        "satellite_callback_count": 6,
        "experiments_callback_count": 7,
        "p2_callback_count": 18,
        "simulation_callback_count": 3,
        "canonical_interfaces": [
            "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_shadow.py",
            "src/ai_trading_system/interfaces/cli/etf_portfolio/satellite.py",
            "src/ai_trading_system/interfaces/cli/etf_portfolio/experiments.py",
            "src/ai_trading_system/interfaces/cli/etf_portfolio/p2.py",
            "src/ai_trading_system/interfaces/cli/etf_portfolio/simulation.py",
        ],
        "compatibility_app_reexports": True,
        "compatibility_callback_reexports": 6,
        "compatibility_callback_wrappers": False,
    }
    assert eb7["subtraction"] == {
        "legacy_cli_lines_before": 4038,
        "legacy_cli_lines_after": 1781,
        "legacy_cli_top_level_functions_before": 100,
        "legacy_cli_top_level_functions_after": 41,
        "legacy_cli_decorators_before": 76,
        "legacy_cli_decorators_after": 36,
        "duplicate_implementation_retained": False,
    }
    assert eb7["callback_matrix"] == {
        "baseline_callback_count": 967,
        "migrated_callback_count": 931,
        "pending_callback_count": 36,
        "unresolved_callback_count": 0,
        "duplicate_registration_count": 0,
        "phase_exit_criteria_passed": False,
    }
    assert eb7["cli_contract"] == {
        "root_command_count": 41,
        "group_count": 291,
        "leaf_command_count": 993,
        "duplicate_path_count": 0,
        "command_tree_sha256": ("01c78550ae58b38c2d8cca0683376643e2934f93e324710612c87d39eea7302d"),
    }
    assert eb7["focused_validation"]["post_migration_run"]["passed"] == 257
    assert eb7["focused_validation"]["performance_triggered"] is False
    assert eb7["architecture_devex"]["module_count"] == 970
    assert eb7["deprecation_inventory"]["module_count"] == 970
    assert eb7["safety"]["strategy_logic_changed"] is False
    assert eb7["safety"]["production_effect"] == "none"
    assert len(eb7["sources"]) == 29
    if eb7["status"] == "VALIDATING_G2_4_CONTINUES":
        assert eb7["formal_validation"]["architecture_fitness"]["status"] == "PENDING"
        assert eb7["formal_validation"]["contract_validation"]["status"] == "PENDING"
        assert eb7["formal_validation"]["full_validation"]["status"] == "PENDING"
        assert eb7["next_phase_or_slice_unblocked"] is False
        assert all(source["sha256"] == 0 for source in eb7["sources"])
    else:
        assert eb7["formal_validation"]["architecture_fitness"]["status"] == "PASS"
        assert eb7["formal_validation"]["contract_validation"]["status"] == "PASS"
        assert eb7["formal_validation"]["full_validation"]["status"] in {
            "PASS",
            "PASS_AFTER_AUDITED_FAILURE_FIX_RERUN",
        }
        assert eb7["next_phase_or_slice_unblocked"] is True
        eb7_superseded = set(eb7.get("superseded_source_paths", []))
        for source in eb7["sources"]:
            if source["path"] in eb7_superseded:
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    eb8 = baseline["phase_g2_4eb8_final_cli_compatibility_facade"]
    assert eb8["status"] in {
        "VALIDATING_G2_4_CONTINUES",
        "COMPLETE_G2_4_CONTINUES",
    }
    assert eb8["base_commit"] == "bfbb38cf1ae0f9fbdd6fcefb10749bc5e59f03dc"
    assert eb8["boundary_id"] == "ARCH-004G2.4-EB8"
    assert eb8["migration"] == {
        "callback_count": 36,
        "ai_attribution_callback_count": 3,
        "ai_confirmation_callback_count": 4,
        "backtest_callback_count": 3,
        "decision_journal_callback_count": 8,
        "forward_callback_count": 5,
        "p1_callback_count": 6,
        "workflow_callback_count": 7,
        "canonical_interfaces": [
            "src/ai_trading_system/interfaces/cli/etf_portfolio/ai_attribution.py",
            "src/ai_trading_system/interfaces/cli/etf_portfolio/ai_confirmation.py",
            "src/ai_trading_system/interfaces/cli/etf_portfolio/backtest.py",
            "src/ai_trading_system/interfaces/cli/etf_portfolio/decision_journal.py",
            "src/ai_trading_system/interfaces/cli/etf_portfolio/forward.py",
            "src/ai_trading_system/interfaces/cli/etf_portfolio/p1.py",
            "src/ai_trading_system/interfaces/cli/etf_portfolio/workflow.py",
        ],
        "compatibility_app_reexports": True,
        "compatibility_callback_reexports": 8,
        "compatibility_callback_wrappers": False,
    }
    assert eb8["subtraction"] == {
        "legacy_cli_lines_before": 1781,
        "legacy_cli_lines_after": 146,
        "legacy_cli_top_level_functions_before": 41,
        "legacy_cli_top_level_functions_after": 0,
        "legacy_cli_decorators_before": 36,
        "legacy_cli_decorators_after": 0,
        "duplicate_implementation_retained": False,
    }
    assert eb8["callback_matrix"] == {
        "baseline_callback_count": 967,
        "migrated_callback_count": 967,
        "pending_callback_count": 0,
        "unresolved_callback_count": 0,
        "duplicate_registration_count": 0,
        "phase_exit_criteria_passed": True,
    }
    assert eb8["cli_contract"] == {
        "root_command_count": 41,
        "group_count": 291,
        "leaf_command_count": 993,
        "duplicate_path_count": 0,
        "command_tree_sha256": ("01c78550ae58b38c2d8cca0683376643e2934f93e324710612c87d39eea7302d"),
    }
    assert eb8["focused_validation"]["post_migration_run"]["passed"] == 269
    assert eb8["focused_validation"]["performance_triggered"] is False
    assert eb8["safety"]["strategy_logic_changed"] is False
    assert eb8["safety"]["production_effect"] == "none"
    assert len(eb8["sources"]) == 26
    if eb8["status"] == "VALIDATING_G2_4_CONTINUES":
        assert eb8["formal_validation"]["architecture_fitness"]["status"] == "PENDING"
        assert eb8["formal_validation"]["contract_validation"]["status"] == "PENDING"
        assert eb8["formal_validation"]["full_validation"]["status"] == "PENDING"
        assert eb8["next_phase_or_slice_unblocked"] is False
        assert all(source["sha256"] == 0 for source in eb8["sources"])
    else:
        assert eb8["formal_validation"]["architecture_fitness"]["status"] == "PASS"
        assert eb8["formal_validation"]["contract_validation"]["status"] == "PASS"
        assert eb8["formal_validation"]["full_validation"]["status"] in {
            "PASS",
            "PASS_AFTER_AUDITED_FAILURE_FIX_RERUN",
        }
        assert eb8["next_phase_or_slice_unblocked"] is True
        eb8_superseded = set(eb8.get("superseded_source_paths", []))
        for source in eb8["sources"]:
            if source["path"] in eb8_superseded:
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    phase_exit = baseline["phase_g2_4_phase_exit"]
    assert phase_exit["status"] in {"VALIDATING_PHASE_EXIT", "PASS"}
    assert phase_exit["task_id"] == "ARCH-004G2_INTERFACES_AND_ETF_CLI_MIGRATION"
    assert phase_exit["base_commit"] == "873ad74c662394e876472a4cdd027458457c2dec"
    assert phase_exit["boundary_id"] == "ARCH-004G2.4-PHASE-EXIT-HANDOFF"
    assert phase_exit["callback_matrix"] == {
        "baseline_callback_count": 967,
        "migrated_callback_count": 967,
        "pending_callback_count": 0,
        "unresolved_callback_count": 0,
        "duplicate_registration_count": 0,
        "phase_exit_criteria_passed": True,
    }
    assert phase_exit["cli_contract"] == {
        "root_command_count": 41,
        "group_count": 291,
        "leaf_command_count": 993,
        "duplicate_path_count": 0,
        "command_tree_sha256": ("01c78550ae58b38c2d8cca0683376643e2934f93e324710612c87d39eea7302d"),
    }
    assert phase_exit["shared_path_activity"] == {
        "status": "PASS",
        "active_shared_path_owner_count": 0,
        "active_shared_path_lease_count": 0,
        "active_shared_path_integration_count": 0,
        "lease_registry_present": False,
    }
    assert phase_exit["handoff"]["schema_version"] == "arch_005_bootstrap_handoff.v1"
    assert phase_exit["handoff"]["validator_frozen"] is True
    assert phase_exit["handoff"]["status"] == "PASS_COMMITTED_AND_PUSHED"
    assert phase_exit["handoff"]["tracked_file_hash_basis"] == ("source_commit_git_blob_sha256")
    assert phase_exit["handoff"]["next_slice_unblocked"] is False
    assert phase_exit["handoff"]["formal_arch_005_s0_unblocked"] is True
    assert phase_exit["handoff"]["g2_5_unblocked"] is False
    assert phase_exit["safety"]["production_effect"] == "none"
    if phase_exit["status"] == "VALIDATING_PHASE_EXIT":
        assert all(
            record["status"] == "PENDING" for record in phase_exit["required_validation"].values()
        )
        assert all(source["sha256"] == 0 for source in phase_exit["sources"])
    else:
        assert all(
            record["status"] == "PASS" for record in phase_exit["required_validation"].values()
        )
        superseded = set(phase_exit.get("superseded_source_paths", []))
        assert phase_exit.get("superseded_by_phase") == "ARCH-005-S0-S1"
        for source in phase_exit["sources"]:
            if source["path"] in superseded:
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    s0_s1 = baseline["phase_arch_005_s0_s1_shadow_registry"]
    assert s0_s1["status"] in {"VALIDATING_S0_S1", "COMPLETE_S0_S1"}
    assert s0_s1["task_id"] == "ARCH-005_PARALLEL_DEVELOPMENT_CONTROL_PLANE"
    assert s0_s1["base_commit"] == "f1045634f771955e3ddef721ff5ed39aea795b27"
    assert s0_s1["entry_gate"]["status"] == "PASS"
    assert s0_s1["entry_gate"]["tracked_file_hash_basis"] == ("source_commit_git_blob_sha256")
    assert s0_s1["entry_gate"]["next_slice_unblocked"] is False
    assert s0_s1["contracts"] == {
        "task_record_schema": "task_record.v1",
        "task_event_schema": "task_event.v1",
        "dependency_schema": "task_dependency.v1",
        "execution_lease_schema": "execution_lease.v1",
        "scheduler_decision_schema": "scheduler_decision.v1",
        "generated_view_schema": "task_register_generated_view.v1",
        "legacy_parser_version": "task_register_markdown_parser.v1_characterized",
        "shadow_compiler_version": "arch_005_shadow_registry_compiler.v1",
    }
    assert s0_s1["s0_inventory"]["active_task_count"] == 427
    assert s0_s1["s0_inventory"]["completed_task_count"] == 442
    assert s0_s1["s0_inventory"]["unique_task_count"] == 869
    assert s0_s1["s0_inventory"]["task_id_overlap_count"] == 0
    assert s0_s1["s0_inventory"]["ambiguous_extra_cell_row_count"] == 55
    assert s0_s1["s1_shadow"]["fragment_count"] == 869
    assert s0_s1["s1_shadow"]["missing_task_count"] == 0
    assert s0_s1["s1_shadow"]["duplicate_task_count"] == 0
    assert s0_s1["s1_shadow"]["compatibility_views_byte_identical"] is True
    assert s0_s1["source_of_truth"]["legacy_markdown_only"] is True
    assert s0_s1["source_of_truth"]["dual_write_allowed"] is False
    assert s0_s1["next_work"]["s2_unblocked"] is False
    assert s0_s1["next_work"]["arch_004_g2_5_unblocked"] is False
    assert s0_s1["safety"]["dispatch_allowed"] is False
    assert s0_s1["safety"]["lease_acquisition_allowed"] is False
    assert s0_s1["safety"]["production_effect"] == "none"
    if s0_s1["status"] == "VALIDATING_S0_S1":
        assert all(source["sha256"] == 0 for source in s0_s1["sources"])
    else:
        assert all(record["status"] == "PASS" for record in s0_s1["validation"].values())
        assert s0_s1.get("superseded_by_phase") == ("TRADING-2446_to_2448_RESEARCH_RESTART_R0_R2")
        superseded = set(s0_s1.get("superseded_source_paths", []))
        for source in s0_s1["sources"]:
            if source["path"] in superseded:
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    s2_s4 = baseline["phase_arch_005_s2_s4_parallel_control"]
    assert s2_s4["status"] in {"VALIDATING_S2_S4", "COMPLETE_S2_S4_S5_PENDING"}
    assert s2_s4["task_id"] == "ARCH-005_PARALLEL_DEVELOPMENT_CONTROL_PLANE"
    assert s2_s4["base_commit"] == "20b878ea2d84a31238311bdbcac14a28892dabe1"
    assert s2_s4["contracts"]["lease_replay_schema"] == "execution_lease_replay.v1"
    pilot_policy = s2_s4["reviewed_pilot_policy"]
    assert pilot_policy["max_parallel_domain_lanes"] == 2
    assert pilot_policy["max_active_leases"] == 3
    assert pilot_policy["max_reassignments"] == 1
    assert pilot_policy["priority_aging_enabled"] is False
    assert s2_s4["s2_kernel"]["status"] == "PASS"
    scheduler = s2_s4["s3_shadow_scheduler"]
    assert scheduler["status"] == "PASS"
    assert len(set(scheduler["decision_ids"])) == 1
    assert len(set(scheduler["decision_byte_sha256"])) == 1
    assert scheduler["dispatch_allowed"] is False
    dispatch = s2_s4["s4_controlled_dispatch"]
    assert dispatch["status"] == "PASS"
    assert dispatch["dispatch_id"] == "controlled-dispatch-aca2d27f60304e5a5c60"
    assert dispatch["validation_check_count"] == 29
    assert dispatch["successful_run_event_count"] == 13
    assert dispatch["successful_run_active_lease_count"] == 0
    assert dispatch["failed_run_chains_preserved"] == 2
    assert dispatch["all_failed_run_active_leases_closed"] is True
    assert s2_s4["research_lineage_refresh"]["r2_decision"] == ("CONTINUE_EVIDENCE_CLOSURE")
    assert s2_s4["source_of_truth"]["legacy_markdown_only"] is True
    assert s2_s4["source_of_truth"]["canonical_cutover_performed"] is False
    assert s2_s4["next_work"]["s5_unblocked"] is False
    assert s2_s4["next_work"]["arch_004_g2_5_unblocked"] is False
    assert s2_s4["safety"]["production_effect"] == "none"
    if s2_s4["status"] == "VALIDATING_S2_S4":
        assert all(source["sha256"] == 0 for source in s2_s4["sources"])
    else:
        assert all(record["status"] == "PASS" for record in s2_s4["validation"].values())
        for source in s2_s4["sources"]:
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    s4a = baseline["phase_arch_005_s4a_supervised_automation"]
    assert s4a["status"] in {"VALIDATING_S4A", "COMPLETE_S4A_S5_PENDING"}
    assert s4a["task_id"] == "ARCH-005_PARALLEL_DEVELOPMENT_CONTROL_PLANE"
    assert s4a["base_commit"] == "50a765fe3176b53b308adc92fdf7b66e96269386"
    assert s4a["contracts"]["run_report_schema"] == "supervised_automation_run.v1"
    assert s4a["reviewed_policy"]["max_workers"] == 2
    assert s4a["reviewed_policy"]["max_active_leases"] == 2
    assert s4a["reviewed_policy"]["automatic_merge_allowed"] is False
    failed_run = s4a["failed_run"]
    assert failed_run["status"] == "FAIL"
    assert failed_run["cli_wrapper_false_pass_detected"] is True
    assert failed_run["validator_rejected"] is True
    assert failed_run["active_lease_count"] == 0
    successful_run = s4a["successful_run"]
    assert successful_run["status"] == "PASS"
    assert successful_run["engineering_tests_passed"] == 17
    assert successful_run["research_tests_passed"] == 9
    assert successful_run["validation_check_count"] == 13
    assert successful_run["orphan_issue_count"] == 0
    assert successful_run["active_lease_count"] == 0
    assert successful_run["integration_candidate_status"] == ("AWAITING_HUMAN_COORDINATOR_APPROVAL")
    assert successful_run["human_coordinator_approved"] is False
    assert successful_run["merge_allowed"] is False
    assert s4a["source_of_truth"]["legacy_markdown_only"] is True
    assert s4a["source_of_truth"]["canonical_cutover_performed"] is False
    assert s4a["next_work"]["s5_unblocked"] is False
    assert s4a["next_work"]["arch_004_g2_5_unblocked"] is False
    assert s4a["safety"]["production_effect"] == "none"
    if s4a["status"] == "VALIDATING_S4A":
        assert all(source["sha256"] == 0 for source in s4a["sources"])
    else:
        assert all(record["status"] == "PASS" for record in s4a["validation"].values())
        for source in s4a["sources"]:
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    wave2 = baseline["phase_arch_005_s4b_dual_lane_wave2"]
    assert wave2["status"] == "COMPLETE_WAVE2"
    assert wave2["task_ids"] == [
        "ARCH-004G2_VALIDATION_RUNTIME_BUDGET_AND_FIXTURE_REUSE",
        "TRADING-2450_LEGACY_RESEARCH_ARTIFACT_PORTABLE_LINEAGE",
    ]
    assert wave2["base_commit"] == "ca9dea5e424c85b1e968456137c8829222334114"
    engineering = wave2["engineering_lane"]
    assert engineering["node_count_preserved"] == 5
    assert engineering["isolated_before_seconds"] == 340.62
    assert engineering["isolated_after_seconds"] == 286.28
    assert engineering["frozen_after_limit_seconds"] == 307.35
    assert engineering["focused_status"] == "PASS"
    assert engineering["stable_full_improvement_claimed"] is False
    strategy = wave2["strategy_evidence_lane"]
    assert strategy["sidecar_id"] == "portable-lineage_dfa5dfc7208e5913fc75"
    assert strategy["artifact_binding_count"] == 4
    assert strategy["source_binding_count"] == 108
    assert strategy["r0_status"] == "PASS"
    assert strategy["walk_forward_status"] == "PASS"
    assert strategy["robustness_status"] == "PASS"
    assert strategy["r2_status"] == "PASS"
    assert strategy["r2_decision"] == "CONTINUE_EVIDENCE_CLOSURE"
    assert strategy["trading2449_status"] == "BLOCKED_CONTAMINATED_LEGACY_SOURCE"
    assert strategy["immutable_artifacts_unchanged"] is True
    assert wave2["conflict_telemetry"] == {
        "owned_path_overlap_count": 0,
        "shared_path_writer": "integration_coordinator",
        "semantic_regression_detected_and_rejected": 1,
        "workaround_used": False,
    }
    assert wave2["generated_state"]["module_count"] == 991
    assert wave2["generated_state"]["test_file_count"] == 1142
    assert wave2["generated_state"]["active_task_count"] == 430
    assert wave2["generated_state"]["completed_task_count"] == 447
    assert wave2["generated_state"]["direct_writer_violation_count"] == 0
    required = wave2["required_validation"]
    for key in ("engineering_isolated", "strategy_focused", "ruff", "black", "mypy"):
        assert required[key]["status"] == "PASS"
    for key in ("architecture_fitness", "contract_validation", "reproducibility", "full"):
        assert required[key]["status"] == "PASS"
    assert required["full"]["node_count"] == 6489
    assert required["full"]["file_count"] == 1084
    assert required["full"]["scheduler_applied"] is True
    assert required["full"]["fallback_used"] is False
    assert wave2["performance_interpretation"]["broad_machine_slowdown_observed"] is True
    assert wave2["performance_interpretation"]["stable_full_improvement_claimed"] is False
    assert wave2["refreshed_duration_profile"] == {
        "path": "inputs/architecture/arch_004g2_full_duration_profile.yaml",
        "profile_id": "arch_004g2_wave2_full_duration_partial_seed",
        "version": 17,
        "status": "PARTIAL_SEED",
        "source_full": "outputs/validation_runtime/full_20260720T151936Z/test_runtime_profile.json",
        "file_count": 1084,
        "node_count": 6489,
        "production_effect": "none",
    }
    assert wave2["next_work"] == {
        "s5_unblocked": False,
        "arch_004_g2_5_unblocked": False,
        "trading2449_s1_unblocked": False,
    }
    assert wave2["safety"]["production_effect"] == "none"
    assert wave2["safety"]["broker_action"] == "none"
    for source in wave2["sources"]:
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]

    wave3 = baseline["phase_arch_005_s4b_dual_lane_wave3"]
    assert wave3["status"] == "COMPLETE_WAVE3_DUAL_LANE"
    assert wave3["task_ids"] == [
        "ARCH-004G2_VALIDATION_RUNTIME_BUDGET_AND_FIXTURE_REUSE",
        "TRADING-098",
    ]
    assert wave3["base_commit"] == "3156a4b91b170880c9b8e46802e85f81637af015"
    engineering3 = wave3["engineering_lane"]
    assert engineering3["owned_path"] == "tests/test_evidence_staleness_monitor.py"
    assert engineering3["node_count_preserved"] == 6
    assert engineering3["isolated_before_seconds"] == 125.36
    assert engineering3["isolated_after_seconds"] == 85.44
    assert engineering3["frozen_after_limit_seconds"] == 100.29
    assert engineering3["focused_status"] == "PASS"
    assert engineering3["stable_full_improvement_claimed"] is False
    strategy3 = wave3["strategy_evidence_lane"]
    assert strategy3["status"] == "COMPLETE_OBSERVE_ONLY_MIGRATION"
    assert strategy3["initial_status"] == "BLOCKED_INPUT"
    assert strategy3["task_id"] == "TRADING-098"
    assert strategy3["runtime_registry_gitignored"] is True
    assert strategy3["historical_row_count_preserved"] == 3
    assert strategy3["downgraded_complete_binding_count"] == 2
    assert strategy3["explicit_none_selection_count"] == 3
    assert strategy3["registry_validation_status"] == "PASS"
    assert strategy3["registry_validation_failed_count"] == 0
    assert strategy3["partial_legacy_pair_bound"] is False
    assert strategy3["fabricated_parallel_work"] is False
    assert wave3["failed_validation_attempts"] == [
        {
            "tier": "full",
            "status": "FAIL",
            "passed": 6486,
            "failed": 1,
            "skipped": 2,
            "warnings": 643,
            "runner_elapsed_seconds": 986.81,
            "artifact": (
                "outputs/validation_runtime/full_20260720T161303Z/test_runtime_summary.json"
            ),
            "failure": "TRACKED_PARTIAL_PROFILE_V17_SOURCE_BOUND_TEST_STILL_EXPECTED_V16",
            "scheduler_applied": True,
            "workaround_used": False,
            "counts_as_stable_performance_sample": False,
        }
    ]
    assert wave3["conflict_telemetry"] == {
        "owned_path_overlap_count": 0,
        "shared_path_overlap_count": 2,
        "late_shared_write_detected": True,
        "worker_shared_path_write_count": 2,
        "shared_path_writer": "integration_coordinator",
        "worker_stopped_shared_writes_after_coordination": True,
        "coordinator_rebuilt_shared_state": True,
        "work_lost_or_reverted": False,
        "workaround_used": False,
    }
    assert wave3["generated_state"]["task_count"] == 877
    assert wave3["generated_state"]["active_task_count"] == 429
    assert wave3["generated_state"]["completed_task_count"] == 448
    assert wave3["generated_state"]["module_count"] == 991
    assert wave3["generated_state"]["test_file_count"] == 1142
    required3 = wave3["required_validation"]
    for key in (
        "engineering_isolated",
        "ruff",
        "black",
        "diff_check",
        "strategy_registry",
        "strategy_focused",
        "strategy_expanded",
        "strategy_governance",
        "architecture_fitness",
        "contract_validation",
        "full",
    ):
        assert required3[key]["status"] == "PASS"
    assert required3["architecture_fitness"]["passed"] == 446
    assert required3["contract_validation"]["passed"] == 265
    assert required3["full"]["passed"] == 6487
    assert required3["full"]["skipped"] == 2
    assert required3["full"]["file_count"] == 1084
    assert required3["full"]["node_count"] == 6489
    assert required3["full"]["scheduler_applied"] is True
    assert required3["full"]["scheduler_fallback"] is False
    performance3 = wave3["performance_assessment"]
    assert performance3["target_file_reduction_percent"] == 31.37
    assert performance3["common_file_count"] == 1084
    assert performance3["stable_full_improvement_claimed"] is False
    assert wave3["next_work"]["strategy_lane_unblocked"] is False
    assert wave3["safety"]["runtime_registry_state_changed"] is True
    assert wave3["safety"]["production_effect"] == "none"
    assert wave3["safety"]["broker_action"] == "none"
    for source in wave3["sources"]:
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]

    wave4 = baseline["phase_arch_005_s4c_dual_lane_wave4"]
    assert wave4["status"] == "COMPLETE_WAVE4_DUAL_LANE"
    assert wave4["task_ids"] == [
        "ARCH-004G2_VALIDATION_RUNTIME_BUDGET_AND_FIXTURE_REUSE",
        "TRADING-2451_DYNAMIC_V3_CLEAN_SELECTION_S1_PREREGISTRATION",
    ]
    assert wave4["base_commit"] == "872d7ccbf29939b2a2c2f42a6ae34ce274b89b65"
    assert wave4["branch"] == "codex/dual-lane-wave4-prereg-runtime"
    engineering4 = wave4["engineering_lane"]
    assert engineering4["status"] == "COMPLETE_FOCUSED_AND_FULL_PROFILE"
    assert engineering4["owned_path"] == "tests/test_shadow_continuation_readiness.py"
    assert engineering4["node_count_preserved"] == 5
    assert engineering4["source_full_profile_worker_seconds"] == 479.3692383
    assert engineering4["isolated_before_seconds"] == 198.89
    assert engineering4["isolated_after_seconds"] == 155.18
    assert engineering4["frozen_after_limit_seconds"] == 169.06
    assert engineering4["minimum_absolute_reduction_seconds"] == 25.0
    assert engineering4["focused_status"] == "PASS"
    assert engineering4["stable_full_improvement_claimed"] is False
    assert engineering4["duration_seed"]["version"] == 18
    assert engineering4["duration_seed"]["source_file_count"] == 1084
    assert engineering4["duration_seed"]["source_node_count"] == 6489
    strategy4 = wave4["strategy_evidence_lane"]
    assert strategy4["status"] == ("COMPLETE_PREREGISTRATION_OWNER_AUTHORIZATION_PENDING")
    assert strategy4["task_id"] == ("TRADING-2451_DYNAMIC_V3_CLEAN_SELECTION_S1_PREREGISTRATION")
    assert strategy4["package_id"] == "dynamic-v3-clean-s1_cf88e2fc1cee51406b6b"
    assert strategy4["eligibility_status"] == ("ELIGIBLE_FOR_OWNER_AUTHORIZED_CLEAN_RUN")
    assert strategy4["candidate_count"] == 300
    assert strategy4["historical_fold_count"] == 4
    assert strategy4["result_artifact_count"] == 0
    assert strategy4["validator_status"] == "PASS"
    assert strategy4["validator_failed_check_count"] == 0
    assert strategy4["focused_passed"] == 11
    assert strategy4["fail_closed_tamper_classes"] == [
        "top_n",
        "score_weight",
        "slippage",
        "window_date",
    ]
    assert strategy4["clean_run_authorized"] is False
    assert strategy4["evaluator_executed"] is False
    assert strategy4["backtest_or_search_executed"] is False
    assert strategy4["prospective_holdout_accessed"] is False
    assert strategy4["unbiased_oos_claim_allowed"] is False
    assert wave4["conflict_telemetry"] == {
        "owned_path_overlap_count": 0,
        "worker_shared_path_precoord_write_count": 1,
        "worker_branch_switch_attempt_count": 1,
        "coordination_correction_applied": True,
        "worker_shared_path_write_count_after_coordination": 0,
        "shared_path_writer_after_coordination": "integration_coordinator",
        "work_lost_or_reverted": False,
        "workaround_used": False,
    }
    assert wave4["generated_state"]["task_count"] == 878
    assert wave4["generated_state"]["active_task_count"] == 429
    assert wave4["generated_state"]["completed_task_count"] == 449
    assert wave4["generated_state"]["module_count"] == 992
    assert wave4["generated_state"]["test_file_count"] == 1143
    required4 = wave4["required_validation"]
    for key in (
        "engineering_isolated",
        "strategy_validator",
        "strategy_focused",
        "duration_seed_source_binding",
        "ruff",
        "black",
        "diff_check",
    ):
        assert required4[key]["status"] == "PASS"
    for key in ("architecture_fitness", "contract_validation", "full"):
        assert required4[key]["status"] == "PASS"
    assert required4["architecture_fitness"]["passed"] == 446
    assert required4["contract_validation"]["passed"] == 265
    assert required4["full"]["passed"] == 6498
    assert required4["full"]["skipped"] == 2
    assert required4["full"]["file_count"] == 1085
    assert required4["full"]["node_count"] == 6500
    assert required4["full"]["scheduler_applied"] is True
    assert required4["full"]["scheduler_fallback"] is False
    assert wave4["failed_validation_attempts"] == [
        {
            "tier": "architecture_fitness",
            "status": "FAIL",
            "passed": 445,
            "failed": 1,
            "elapsed_seconds": 57.36,
            "artifact": (
                "outputs/validation_runtime/architecture-fitness_20260720T174852Z/"
                "test_runtime_summary.json"
            ),
            "failure": (
                "DEPRECATION_INVENTORY_MODULE_AND_REFERENCE_COUNTS_STALE_AFTER_NEW_STRATEGY_MODULE"
            ),
            "direct_fix_applied": True,
            "workaround_used": False,
        }
    ]
    assert wave4["validation_preflight_rejections"][0]["status"] == ("REJECTED_BEFORE_PYTEST")
    assert wave4["validation_preflight_rejections"][0]["counts_as_performance_sample"] is False
    assert wave4["performance_assessment"]["isolated_reduction_percent"] == 21.98
    assert wave4["performance_assessment"]["target_file_full_reduction_percent"] == 24.20
    assert wave4["performance_assessment"]["full_runner_reduction_percent"] == 7.78
    assert wave4["performance_assessment"]["common_file_count"] == 1084
    assert wave4["performance_assessment"]["stable_full_improvement_claimed"] is False
    assert wave4["next_work"] == {
        "strategy_clean_run_unblocked": False,
        "owner_clean_run_authorization_required": True,
        "arch_004_g2_5_unblocked": False,
        "arch_005_s5_unblocked": False,
        "next_engineering_leaf_selected": False,
    }
    assert wave4["safety"]["production_effect"] == "none"
    assert wave4["safety"]["broker_action"] == "none"
    for source in wave4["sources"]:
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]

    prebootstrap = baseline["arch_005_prebootstrap_primitives"]
    assert prebootstrap["status"] in {
        "IN_PROGRESS_NON_CUTOVER",
        "COMPLETE_NON_CUTOVER_G2_4_CONTINUES",
    }
    assert prebootstrap["task_id"] == ("ARCH-005_PARALLEL_DEVELOPMENT_CONTROL_PLANE")
    assert prebootstrap["slice_id"] == "ARCH-005-PB1"
    assert prebootstrap["base_commit"] == ("fe0e19b943e7ca2f49c091a50536a6b022657566")
    assert prebootstrap["owner_authorization"] == {
        "instruction": "继续按照这个思路实现",
        "boundary_id": "ARCH-005-PB1",
        "formal_s0_authorized": False,
    }
    assert prebootstrap["contracts"] == {
        "change_manifest_schema": "change_manifest.v1",
        "validation_evidence_schema": "validation_evidence.v1",
        "lane_plan_schema": "lane_plan.v1",
        "canonical_serialization_and_hash": True,
        "base_drift_fail_closed": True,
        "owned_shared_path_conflict": True,
        "module_conflict": True,
        "contract_access_and_version_conflict": True,
        "coordinator_only_guard": True,
        "explicit_lane_capacity_required": True,
        "deterministic_domain_waves": True,
        "coordinator_final_integration_wave": True,
        "evidence_manifest_base_tier_status_artifact_binding": True,
    }
    assert prebootstrap["non_cutover_boundary"] == {
        "dispatch_allowed": False,
        "lease_acquisition_allowed": False,
        "task_registry_mutated": False,
        "generated_task_views_written": False,
        "markdown_source_of_truth_changed": False,
        "production_effect": "none",
    }
    assert prebootstrap["phase_lock"]["eb2_integration_gate_passed"] is True
    assert prebootstrap["phase_lock"]["pre_bootstrap_unblocked"] is True
    assert prebootstrap["phase_lock"]["pre_bootstrap_complete"] is True
    assert prebootstrap["phase_lock"]["eb3_unblocked"] is False
    assert prebootstrap["phase_lock"]["formal_arch_005_s0_unblocked"] is False
    assert prebootstrap["phase_lock"]["g2_5_unblocked"] is False
    assert prebootstrap["phase_lock"]["next_phase_or_slice_unblocked"] is False
    assert prebootstrap["validation"]["architecture_devex"] == {
        "status": "PASS",
        "module_count": 954,
        "test_file_count": 1129,
        "direct_writer_count": 858,
        "violation_count": 0,
    }
    assert prebootstrap["validation"]["deprecation_inventory"] == {
        "status": "FRESH_REFRESHED",
        "inventory_id": "arch_004g_deprecation_inventory_a2ab38dc563643dacc6e",
        "module_count": 954,
        "test_file_count": 1129,
    }
    assert len(prebootstrap["sources"]) == 20
    prebootstrap_superseded = set(prebootstrap["superseded_source_paths"])
    for source in prebootstrap["sources"]:
        if source["path"] in prebootstrap_superseded:
            continue
        if prebootstrap["status"] == "IN_PROGRESS_NON_CUTOVER":
            assert source["sha256"] == 0
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]


def test_arch_004c_dependency_policy_uses_count_ratchet_without_waiver() -> None:
    policy = safe_load_yaml_path(DEPENDENCY_POLICY_PATH)
    direct = policy["direct_writer_ratchet"]
    baseline = safe_load_yaml_path(DIRECT_WRITER_BASELINE_PATH)

    assert policy["status"] == "active_phase_c"
    assert policy["canonical_writer_path"] == ("src/ai_trading_system/platform/artifacts/writer.py")
    assert direct["new_calls_allowed"] is False
    assert direct["baseline_status"] == "FROZEN_ARCH_004C_C2"
    assert baseline["status"] == "FROZEN_ARCH_004C_C2"
    assert baseline["direct_writer_call_count"] == 894
    assert baseline["entries"]


def test_arch_004_worktree_attribution_excludes_concurrent_user_changes() -> None:
    attribution = safe_load_yaml_path(ATTRIBUTION_PATH)

    assert attribution["status"] in {
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AL_IN_PROGRESS",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AL_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AM_IN_PROGRESS",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AM_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AN_IN_PROGRESS",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AN_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AO_IN_PROGRESS",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AO_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AP_IN_PROGRESS",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AP_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AQ_IN_PROGRESS",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AQ_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AR_IN_PROGRESS",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AR_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AS_IN_PROGRESS",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AS_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AT_IN_PROGRESS",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AT_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AU_IN_PROGRESS",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AU_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AV_IN_PROGRESS",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AV_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AW_IN_PROGRESS",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AW_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AX_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AX_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AY_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AY_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AZ_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AZ_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BA_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BA_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BB_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BB_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BC_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BC_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BD_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BD_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BE_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BE_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BF_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BF_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BG_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BG_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BG_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BH_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BH_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BH_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BI_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BI_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BI_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BJ_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BJ_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BJ_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BK_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BK_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BK_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BL_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BL_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BL_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BM_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BM_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BM_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BN_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BN_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BN_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BO_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BO_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BO_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BP_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BP_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BP_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BQ_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BQ_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BQ_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BR_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BR_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BR_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BS_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BS_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BS_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BT_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BT_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BT_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BU_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BU_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BU_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BV_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BV_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BV_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BW_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BW_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BW_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BX_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BX_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BX_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BY_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BY_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BY_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BZ_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BZ_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BZ_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CA_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CA_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CA_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CB_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CB_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CB_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CC_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CC_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CC_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CD_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CD_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CD_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CE_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CE_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CE_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CF_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CF_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CF_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CG_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CG_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CG_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CH_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CH_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CH_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CI_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CJ_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CK_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CK_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CL_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CL_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CM_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CM_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CN_IN_PROGRESS",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CN_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CO_IN_PROGRESS",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CO_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CO_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CP_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CP_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CQ_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CQ_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CR_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CR_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CR_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CS_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CT_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CT_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CU_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CU_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CU_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CV1_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CV1_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CV1_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CV2_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CV2_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CV2_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CV3_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CV3_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CV3_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CW1_IN_PROGRESS",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CW1_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CW2_IN_PROGRESS",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CW2_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CW3_VALIDATING",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CW3_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CX2_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CX2_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CX3_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CX3_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CX3_COMPLETE_G2_4_CONTINUES",
        "INTEGRATED_WORKTREE_ATTRIBUTION_PROVEN_PHASE_G2_4CX3_VALIDATING_G2_4_CONTINUES",
        "INTEGRATED_WORKTREE_ATTRIBUTION_PROVEN_PHASE_G2_4CX3_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4_EB1_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4_EB1_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4_EB2_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4_EB2_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4_EB3_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4_EB3_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4_EB4_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4_EB4_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4_EB5_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4_EB5_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4_EB6_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4_EB6_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4_EB6_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4_EB7_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4_EB7_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4_EB7_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4_EB8_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4_EB8_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4_EB8_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_ARCH_005_PREBOOTSTRAP_IN_PROGRESS",
        "ATTRIBUTABLE_ISOLATION_PROVEN_ARCH_005_PREBOOTSTRAP_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4_EXIT_IN_PROGRESS",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4_EXIT_PASS_HANDOFF_PENDING",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4_HANDOFF_PASS_STOPPED",
    }
    current_authority = attribution["current_staging_authority"]
    assert current_authority["task_id"] == ("ARCH-004G2_INTERFACES_AND_ETF_CLI_MIGRATION")
    assert current_authority["increment"] == "ARCH-004G2.4-PHASE-EXIT-HANDOFF"
    assert current_authority["status"] in {
        "IN_PROGRESS_PHASE_EXIT",
        "PASS_PHASE_EXIT_HANDOFF_PENDING",
        "HANDOFF_PASS_STOPPED",
    }
    assert current_authority["base_commit"] == ("873ad74c662394e876472a4cdd027458457c2dec")
    assert current_authority["declared_path_set_current"] is True
    owner_authorization = current_authority["owner_authorization"]
    assert owner_authorization["instruction"] == (
        "先按照这个顺序推进到可以考虑开始推进研究策略前把"
    )
    assert str(owner_authorization["authorized_at"]) == "2026-07-19"
    assert owner_authorization["boundary_id"] == "ARCH-004G2.4-PHASE-EXIT-HANDOFF"
    phase_lock = current_authority["phase_lock"]
    assert phase_lock["next_phase_or_slice_unblocked"] is False
    assert phase_lock["eb2_integration_gate_passed"] is True
    assert phase_lock["pre_bootstrap_complete"] is True
    assert phase_lock["eb3_complete"] is True
    assert phase_lock["eb4_unblocked"] is True
    assert phase_lock["eb5_complete"] is True
    assert phase_lock["eb6_complete"] is True
    assert phase_lock["eb7_complete"] is True
    assert phase_lock["eb8_complete"] is True
    assert phase_lock["whole_g2_4_phase_exit_review_unblocked"] is True
    assert phase_lock["whole_g2_4_phase_exit_passed"] is (
        current_authority["status"] != "IN_PROGRESS_PHASE_EXIT"
    )
    assert phase_lock["g2_5_unblocked"] is False
    assert len(current_authority["declared_changed_paths"]) == 27
    assert attribution["current_staging_authority"]["base_commit"] == attribution["base_commit"]
    assert attribution["current_staging_authority"]["task_id"] in attribution["integrated_task_ids"]
    assert {
        "tests/test_weight_adaptive_branch.py",
        "tests/test_equal_risk_growth_research_restart.py",
        "tests/test_equal_risk_growth_tilt.py",
    }.issubset(set(attribution["arch_004_owned_paths"]))
    excluded = set(attribution["excluded_user_or_other_task_paths"])
    assert excluded == {
        "docs/research/growth_tilt_owner_decision_resolution.md",
        "docs/research/indicator_family_only_model_review.md",
        "docs/research/layer1_selector_pause_or_continue_owner_pack.md",
    }
    assert attribution["staging_rule"]["exclude_user_or_other_task_paths"] is True
    assert attribution["safety_boundary"]["user_changes_preserved"] is True
