from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.services.degree_dsl_schema import validate_degree_dsl_rule_v2
from app.services.rule_engine import validate_rule_schema as validate_legacy_rule_schema


EXPLANATION_SATISFIED = "REQUIREMENT_SATISFIED"
EXPLANATION_INCOMPLETE = "REQUIREMENT_INCOMPLETE"
EXPLANATION_REQUIRED_MISSING = "REQUIRED_COURSE_MISSING"
EXPLANATION_UNSUPPORTED_LEGACY = "UNSUPPORTED_LEGACY_RULE"
EXPLANATION_PRIORITY = {
    EXPLANATION_UNSUPPORTED_LEGACY: 1,
    EXPLANATION_REQUIRED_MISSING: 2,
    EXPLANATION_INCOMPLETE: 3,
    EXPLANATION_SATISFIED: 4,
}


@dataclass(frozen=True)
class DegreeRuleEvalResult:
    supported: bool
    satisfied: bool
    missing_courses: list[str]
    explanation_codes: list[str]


def order_explanations(codes: set[str]) -> list[str]:
    return sorted(codes, key=lambda code: (EXPLANATION_PRIORITY.get(code, 99), code))


def _finalize(
    *,
    supported: bool,
    satisfied: bool,
    missing_courses: list[str],
    explanations: set[str],
) -> DegreeRuleEvalResult:
    if not supported:
        return DegreeRuleEvalResult(
            supported=False,
            satisfied=False,
            missing_courses=[],
            explanation_codes=[EXPLANATION_UNSUPPORTED_LEGACY],
        )
    if satisfied:
        return DegreeRuleEvalResult(
            supported=True,
            satisfied=True,
            missing_courses=[],
            explanation_codes=[EXPLANATION_SATISFIED],
        )

    explanation_set = set(explanations)
    explanation_set.add(EXPLANATION_INCOMPLETE)
    explanation_set.discard(EXPLANATION_SATISFIED)

    return DegreeRuleEvalResult(
        supported=True,
        satisfied=False,
        missing_courses=sorted({str(code) for code in missing_courses}),
        explanation_codes=order_explanations(explanation_set),
    )


def _eval_min_required_children(
    *,
    min_required: int,
    children: list[dict[str, Any]],
    evidence_codes: set[str],
) -> DegreeRuleEvalResult:
    satisfied_count = 0
    failed_children: list[DegreeRuleEvalResult] = []

    for child in children:
        child_result = _eval_v2(child, evidence_codes)
        # Any unsupported child poisons cardinality satisfiability reasoning.
        if not child_result.supported:
            return _finalize(
                supported=False,
                satisfied=False,
                missing_courses=[],
                explanations=set(),
            )
        if child_result.satisfied:
            satisfied_count += 1
        else:
            failed_children.append(child_result)

    if satisfied_count >= min_required:
        return _finalize(
            supported=True,
            satisfied=True,
            missing_courses=[],
            explanations={EXPLANATION_SATISFIED},
        )

    shortfall = min_required - satisfied_count
    witness_children = failed_children[:shortfall]
    missing_codes: set[str] = set()
    child_explanations: set[str] = set()
    for child_result in witness_children:
        missing_codes.update(child_result.missing_courses)
        child_explanations.update(child_result.explanation_codes)

    child_explanations.add(EXPLANATION_INCOMPLETE)
    child_explanations.discard(EXPLANATION_SATISFIED)
    return _finalize(
        supported=True,
        satisfied=False,
        missing_courses=list(missing_codes),
        explanations=child_explanations,
    )


def infer_requirement_rule_schema_version(rule: dict[str, Any]) -> int:
    return 2 if isinstance(rule, dict) and isinstance(rule.get("type"), str) else 1


def convert_legacy_rule_to_degree_dsl_v2(rule: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(rule, dict):
        return None

    if "type" in rule:
        return rule

    if "course" in rule and len(rule) == 1 and isinstance(rule["course"], str):
        return {"type": "COURSE_SET", "courses": [rule["course"]]}

    if "any" in rule and len(rule) == 1 and isinstance(rule["any"], list) and len(rule["any"]) >= 1:
        converted_children: list[dict[str, Any]] = []
        for child in rule["any"]:
            if not isinstance(child, dict):
                return None
            converted = convert_legacy_rule_to_degree_dsl_v2(child)
            if converted is None:
                return None
            converted_children.append(converted)
        return {"type": "N_OF", "n": 1, "children": converted_children}

    if "all" in rule and len(rule) == 1 and isinstance(rule["all"], list) and len(rule["all"]) >= 1:
        converted_children: list[dict[str, Any]] = []
        for child in rule["all"]:
            if not isinstance(child, dict):
                return None
            converted = convert_legacy_rule_to_degree_dsl_v2(child)
            if converted is None:
                return None
            converted_children.append(converted)
        return {"type": "ALL_OF", "children": converted_children}

    return None


def validate_degree_dsl_semantics_v2(rule: dict[str, Any]) -> None:
    _validate_degree_dsl_semantics_v2(rule)


def _validate_degree_dsl_semantics_v2(node: dict[str, Any]) -> None:
    node_type = node.get("type")

    if node_type == "COURSE_SET":
        courses = node.get("courses")
        if not isinstance(courses, list) or len(courses) != 1:
            raise ValueError("COURSE_SET must contain exactly one course")
        return

    if node_type == "ALL_OF":
        children = node.get("children")
        if not isinstance(children, list) or len(children) < 1:
            raise ValueError("ALL_OF children must be a non-empty list")
        for child in children:
            if not isinstance(child, dict):
                raise ValueError("ALL_OF children must be objects")
            _validate_degree_dsl_semantics_v2(child)
        return

    if node_type == "N_OF":
        n = node.get("n")
        children = node.get("children")
        if not isinstance(n, int) or n < 1:
            raise ValueError("N_OF n must be an integer >= 1")
        if not isinstance(children, list) or len(children) < 1:
            raise ValueError("N_OF children must be a non-empty list")
        if n > len(children):
            raise ValueError("N_OF n cannot exceed number of children")
        for child in children:
            if not isinstance(child, dict):
                raise ValueError("N_OF children must be objects")
            _validate_degree_dsl_semantics_v2(child)
        return

    if node_type == "COUNT_MIN":
        min_count = node.get("min_count")
        children = node.get("children")
        if not isinstance(min_count, int) or min_count < 1:
            raise ValueError("COUNT_MIN min_count must be an integer >= 1")
        if not isinstance(children, list) or len(children) < 1:
            raise ValueError("COUNT_MIN children must be a non-empty list")
        if min_count > len(children):
            raise ValueError("COUNT_MIN min_count cannot exceed number of children")
        for child in children:
            if not isinstance(child, dict):
                raise ValueError("COUNT_MIN children must be objects")
            _validate_degree_dsl_semantics_v2(child)
        return

    raise ValueError("Unsupported v2 node type")


def validate_requirement_rule_compat(rule: dict[str, Any]) -> None:
    converted = convert_legacy_rule_to_degree_dsl_v2(rule)
    if converted is not None:
        validate_degree_dsl_rule_v2(converted)
        validate_degree_dsl_semantics_v2(converted)
        return
    # Legacy-but-unsupported-for-v2 requirement shapes are allowed at ingest time.
    # They are evaluated as UNKNOWN in the degree evaluator.
    validate_legacy_rule_schema(rule)


def evaluate_degree_requirement_rule(
    rule: dict[str, Any],
    evidence_codes: set[str],
) -> DegreeRuleEvalResult:
    converted = convert_legacy_rule_to_degree_dsl_v2(rule)
    if converted is None:
        return _finalize(
            supported=False,
            satisfied=False,
            missing_courses=[],
            explanations=set(),
        )

    try:
        validate_degree_dsl_rule_v2(converted)
        validate_degree_dsl_semantics_v2(converted)
    except Exception:
        return _finalize(
            supported=False,
            satisfied=False,
            missing_courses=[],
            explanations=set(),
        )

    normalized_evidence = {str(code) for code in evidence_codes}
    return _eval_v2(converted, normalized_evidence)


def _eval_v2(node: dict[str, Any], evidence_codes: set[str]) -> DegreeRuleEvalResult:
    node_type = node.get("type")

    if node_type == "COURSE_SET":
        required_codes = sorted({str(code) for code in node.get("courses", [])})
        if len(required_codes) != 1:
            return _finalize(
                supported=False,
                satisfied=False,
                missing_courses=[],
                explanations=set(),
            )
        required_code = required_codes[0]
        if required_code in evidence_codes:
            return _finalize(
                supported=True,
                satisfied=True,
                missing_courses=[],
                explanations={EXPLANATION_SATISFIED},
            )
        return _finalize(
            supported=True,
            satisfied=False,
            missing_courses=[required_code],
            explanations={EXPLANATION_REQUIRED_MISSING, EXPLANATION_INCOMPLETE},
        )

    if node_type == "ALL_OF":
        missing_codes: set[str] = set()
        child_explanations: set[str] = set()
        any_failed = False
        for child in node.get("children", []):
            child_result = _eval_v2(child, evidence_codes)
            if not child_result.supported:
                return _finalize(
                    supported=False,
                    satisfied=False,
                    missing_courses=[],
                    explanations=set(),
                )
            if not child_result.satisfied:
                any_failed = True
                missing_codes.update(child_result.missing_courses)
                child_explanations.update(child_result.explanation_codes)
        if not any_failed:
            return _finalize(
                supported=True,
                satisfied=True,
                missing_courses=[],
                explanations={EXPLANATION_SATISFIED},
            )
        child_explanations.add(EXPLANATION_INCOMPLETE)
        child_explanations.discard(EXPLANATION_SATISFIED)
        return _finalize(
            supported=True,
            satisfied=False,
            missing_courses=list(missing_codes),
            explanations=child_explanations,
        )

    if node_type == "N_OF":
        return _eval_min_required_children(
            min_required=int(node.get("n", 0)),
            children=node.get("children", []),
            evidence_codes=evidence_codes,
        )

    if node_type == "COUNT_MIN":
        # COUNT_MIN is a semantic cardinality alias of N_OF witness mechanics.
        return _eval_min_required_children(
            min_required=int(node.get("min_count", 0)),
            children=node.get("children", []),
            evidence_codes=evidence_codes,
        )

    return _finalize(
        supported=False,
        satisfied=False,
        missing_courses=[],
        explanations=set(),
    )
