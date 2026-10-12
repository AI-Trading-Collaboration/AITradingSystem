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


