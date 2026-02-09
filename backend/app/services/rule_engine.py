from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from jsonschema import ValidationError, validate

from app.services.ast_schema import AST_SCHEMA


@dataclass
class RuleEvalResult:
    supported: bool
    satisfied: bool
    missing_courses: list[str]


def validate_rule_schema(rule: dict[str, Any]) -> None:
    validate(instance=rule, schema=AST_SCHEMA)


def evaluate_rule(
    rule: dict[str, Any],
    available_courses: set[str],
    *,
    allow_complex: bool,
) -> RuleEvalResult:
    try:
        validate_rule_schema(rule)
    except ValidationError:
        return RuleEvalResult(supported=False, satisfied=False, missing_courses=[])

    return _eval_node(rule, available_courses, allow_complex=allow_complex)


def _eval_node(node: dict[str, Any], available_courses: set[str], *, allow_complex: bool) -> RuleEvalResult:
    if "course" in node:
        code = node["course"]
        if code in available_courses:
            return RuleEvalResult(supported=True, satisfied=True, missing_courses=[])
        return RuleEvalResult(supported=True, satisfied=False, missing_courses=[code])

    if "all" in node:
        missing: list[str] = []
        for child in node["all"]:
            result = _eval_node(child, available_courses, allow_complex=allow_complex)
            if not result.supported:
                return result
            if not result.satisfied:
                missing.extend(result.missing_courses)
        if missing:
            return RuleEvalResult(supported=True, satisfied=False, missing_courses=sorted(set(missing)))
        return RuleEvalResult(supported=True, satisfied=True, missing_courses=[])

    if "any" in node:
        if not allow_complex:
            return RuleEvalResult(supported=False, satisfied=False, missing_courses=[])
        child_results = [_eval_node(child, available_courses, allow_complex=allow_complex) for child in node["any"]]
        if any(not r.supported for r in child_results):
            return RuleEvalResult(supported=False, satisfied=False, missing_courses=[])
        if any(r.satisfied for r in child_results):
            return RuleEvalResult(supported=True, satisfied=True, missing_courses=[])
        all_missing: list[str] = []
        for result in child_results:
            all_missing.extend(result.missing_courses)
        return RuleEvalResult(supported=True, satisfied=False, missing_courses=sorted(set(all_missing)))

    if "countAtLeast" in node:
        if not allow_complex:
            return RuleEvalResult(supported=False, satisfied=False, missing_courses=[])
        payload = node["countAtLeast"]
        need = int(payload["count"])
        results = [_eval_node(child, available_courses, allow_complex=allow_complex) for child in payload["of"]]
        if any(not r.supported for r in results):
            return RuleEvalResult(supported=False, satisfied=False, missing_courses=[])
        success_count = sum(1 for r in results if r.satisfied)
        if success_count >= need:
            return RuleEvalResult(supported=True, satisfied=True, missing_courses=[])
        missing: list[str] = []
        for result in results:
            missing.extend(result.missing_courses)
        return RuleEvalResult(supported=True, satisfied=False, missing_courses=sorted(set(missing)))

    return RuleEvalResult(supported=False, satisfied=False, missing_courses=[])
