# ==========================================
# ROLE 3: OBSERVABILITY & QA ENGINEER
# ==========================================
# Task: Implement quality gates to reject corrupt data or logic discrepancies.

import re

TOXIC_STRINGS = [
    "null pointer exception",
    "nullpointerexception",
    "error",
    "traceback",
    "undefined",
    "nan",
    "corrupt",
    "fatal",
]

# Patterns to detect tax rate discrepancies in comments vs. code
# e.g., comment says "8%" but code multiplies by 0.10 (10%)
TAX_COMMENT_PATTERN = re.compile(r'tax.*?(\d+)\s*%', re.IGNORECASE)
TAX_CODE_PATTERN = re.compile(r'\*\s*0\.(\d+)', re.IGNORECASE)


def _get_content(document_dict) -> str:
    """Extract content string from either a dict or a UnifiedDocument object."""
    if isinstance(document_dict, dict):
        return document_dict.get("content", "")
    return getattr(document_dict, "content", "")


def _check_min_length(content: str) -> tuple[bool, str]:
    if len(content) < 20:
        return False, f"Content too short ({len(content)} chars, min 20)"
    return True, ""


def _check_toxic_strings(content: str) -> tuple[bool, str]:
    lower = content.lower()
    for toxic in TOXIC_STRINGS:
        if toxic in lower:
            return False, f"Toxic/error string detected: '{toxic}'"
    return True, ""


def _check_tax_discrepancy(content: str) -> tuple[bool, str]:
    comment_match = TAX_COMMENT_PATTERN.search(content)
    code_match = TAX_CODE_PATTERN.search(content)

    if comment_match and code_match:
        comment_rate = int(comment_match.group(1))
        # e.g., "* 0.10" → group(1) = "10" → 10%
        code_rate = int(code_match.group(1))
        if comment_rate != code_rate:
            return False, (
                f"Tax rate discrepancy: comment says {comment_rate}% "
                f"but code uses {code_rate}%"
            )
    return True, ""


def run_quality_gate(document_dict) -> bool:
    content = _get_content(document_dict)

    checks = [
        _check_min_length(content),
        _check_toxic_strings(content),
        _check_tax_discrepancy(content),
    ]

    for passed, reason in checks:
        if not passed:
            doc_id = (
                document_dict.get("document_id", "unknown")
                if isinstance(document_dict, dict)
                else getattr(document_dict, "document_id", "unknown")
            )
            print(f"[QA FAIL] Document '{doc_id}': {reason}")
            return False

    return True
