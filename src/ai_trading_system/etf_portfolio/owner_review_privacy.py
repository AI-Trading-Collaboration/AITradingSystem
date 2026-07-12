from __future__ import annotations

import re

ACCOUNT_NUMBER_RE = re.compile(r"\b\d{5,}\b")
OWNER_NOTE_SENSITIVE_TOKEN_RE = re.compile(
    r"\b(?:account[_\s-]*(?:number|no|id)|broker[_\s-]*account|"
    r"order[_\s-]*id|broker[_\s-]*order|tax[_\s-]*lot|ssn|passport|"
    r"national[_\s-]*id|personal[_\s-]*identifier|"
    r"(?:broker[_\s-]*)?statement[_\s-]*path|trade[_\s-]*confirmation[_\s-]*path)\b",
    re.IGNORECASE,
)


def owner_notes_sensitive_issues(owner_notes: str) -> list[str]:
    """Return stable issue ids for account/order/PII-like content in owner notes."""
    issues: list[str] = []
    if ACCOUNT_NUMBER_RE.search(owner_notes):
        issues.append("long_numeric_identifier")
    token_match = OWNER_NOTE_SENSITIVE_TOKEN_RE.search(owner_notes)
    if token_match:
        issues.append(f"sensitive_token:{token_match.group(0).lower()}")
    return issues


__all__ = ["ACCOUNT_NUMBER_RE", "owner_notes_sensitive_issues"]
