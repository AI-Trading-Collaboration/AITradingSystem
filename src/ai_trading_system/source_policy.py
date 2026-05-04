from __future__ import annotations

AUTOMATIC_SCORE_GRADES = frozenset({"S", "A"})
LIMITED_SCORE_GRADES = frozenset({"B"})
REPORT_ONLY_GRADES = frozenset({"C", "D", "X"})

PUBLIC_CONVENIENCE_SOURCE_TYPE = "public_convenience"
LLM_EXTRACTED_SOURCE_TYPE = "llm_extracted"
AUTOMATIC_SCORING_BLOCKED_SOURCE_TYPES = frozenset(
    {PUBLIC_CONVENIENCE_SOURCE_TYPE, LLM_EXTRACTED_SOURCE_TYPE}
)


def source_type_allows_automatic_scoring(source_type: str) -> bool:
    return source_type not in AUTOMATIC_SCORING_BLOCKED_SOURCE_TYPES


def evidence_grade_allows_automatic_scoring(evidence_grade: str) -> bool:
    return evidence_grade in AUTOMATIC_SCORE_GRADES or evidence_grade in LIMITED_SCORE_GRADES


def evidence_grade_allows_position_gate(evidence_grade: str) -> bool:
    return evidence_grade in AUTOMATIC_SCORE_GRADES


def evidence_grade_is_report_only(evidence_grade: str) -> bool:
    return evidence_grade in REPORT_ONLY_GRADES


def evidence_grade_label(evidence_grade: str) -> str:
    labels = {
        "S": "一手权威且可复现",
        "A": "可信供应商或人工结构化一手证据",
        "B": "人工确认的可信二手或口径受限证据",
        "C": "单一间接或未充分复核证据",
        "D": "公开便利、聚合或弱来源",
        "X": "LLM、传闻、不可验证或被驳回证据",
    }
    return labels.get(evidence_grade, evidence_grade)
