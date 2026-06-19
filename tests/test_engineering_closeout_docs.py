from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_platform_user_guide_covers_stage_c_contract() -> None:
    text = (PROJECT_ROOT / "docs" / "platform_user_guide.md").read_text(
        encoding="utf-8"
    )

    required_sections = [
        "三条使用路径",
        "15 分钟 Quickstart",
        "系统架构图",
        "Artifact 生命周期",
        "状态枚举词典",
        "Reader Brief 标准结构",
        "Troubleshooting Decision Tree",
        "从 Spec 到 Snapshot 的完整例子",
        "平台冻结检查",
    ]
    for section in required_sections:
        assert section in text

    assert "validation PASS 不得写成" in text
    assert "不能声明平台冻结完成" in text
    assert "official target weights" in text
    assert "broker/order" in text


def test_weight_research_turn_covers_stage_d_protocol() -> None:
    text = (
        PROJECT_ROOT / "docs" / "research" / "weight_research_turn_2026-06-19.md"
    ).read_text(encoding="utf-8")

    required_terms = [
        "Failure Taxonomy",
        "Weight-Control Architecture RFC",
        "Ablation Baseline Protocol",
        "Statistical Validation And Holdout Policy",
        "Next Research Program Roadmap",
        "`data_failure`",
        "`binding_failure`",
        "`signal_failure`",
        "`allocator_failure`",
        "`risk_control_failure`",
        "`execution_cost_failure`",
        "`validation_window_failure`",
        "`overfit_failure`",
        "`governance_failure`",
        "`B0`",
        "`B6`",
        "Candidate 1",
        "Candidate 2",
        "Candidate 3",
    ]
    for term in required_terms:
        assert term in text

    assert "不生成 official target weights" in text
    assert "不触发 broker/order" in text
