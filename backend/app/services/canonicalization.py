import re

COURSE_CODE_RE = re.compile(r"\b\d{2}:\d{3}:\d{3}\b")


def extract_canonical_course_code(raw: str) -> str | None:
    if not raw:
        return None
    match = COURSE_CODE_RE.search(raw)
    if not match:
        return None
    return match.group(0)
