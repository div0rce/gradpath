from __future__ import annotations

import pytest

from app.services.degree_dsl_engine import (
    EXPLANATION_INCOMPLETE,
    EXPLANATION_REQUIRED_MISSING,
    EXPLANATION_SATISFIED,
    EXPLANATION_UNSUPPORTED_LEGACY,
    convert_legacy_rule_to_degree_dsl_v2,
    evaluate_degree_requirement_rule,
    infer_requirement_rule_schema_version,
    validate_requirement_rule_compat,
)
from app.services.degree_dsl_schema import validate_degree_dsl_rule_v2


def test_degree_dsl_schema_accepts_course_set_all_of_n_of_and_count_min():
    rule = {
        "type": "ALL_OF",
        "children": [
            {"type": "COURSE_SET", "courses": ["14:540:100"]},
            {"type": "COURSE_SET", "courses": ["14:540:200"]},
            {
                "type": "N_OF",
                "n": 1,
                "children": [{"type": "COURSE_SET", "courses": ["14:540:300"]}],
            },
            {
                "type": "COUNT_MIN",
                "min_count": 1,
                "children": [{"type": "COURSE_SET", "courses": ["14:540:400"]}],
            },
        ],
    }
    validate_degree_dsl_rule_v2(rule)


def test_degree_dsl_schema_rejects_invalid_shape():
    invalid = {"type": "COURSE_SET", "courses": []}
    with pytest.raises(Exception):
        validate_degree_dsl_rule_v2(invalid)


def test_degree_dsl_schema_rejects_multi_course_leaf():
    invalid = {"type": "COURSE_SET", "courses": ["14:540:100", "14:540:200"]}
    with pytest.raises(Exception):
        validate_degree_dsl_rule_v2(invalid)


def test_degree_dsl_semantics_reject_n_of_where_n_exceeds_children():
    invalid_semantic = {
        "type": "N_OF",
        "n": 2,
        "children": [{"type": "COURSE_SET", "courses": ["14:540:100"]}],
    }
    # Schema allows this relation; semantic validator must reject it.
    validate_degree_dsl_rule_v2(invalid_semantic)
    with pytest.raises(Exception):
        validate_requirement_rule_compat(invalid_semantic)


def test_degree_dsl_semantics_reject_count_min_where_min_count_exceeds_children():
    invalid_semantic = {
        "type": "COUNT_MIN",
        "min_count": 2,
        "children": [{"type": "COURSE_SET", "courses": ["14:540:100"]}],
    }
    # Schema allows this relation; semantic validator must reject it.
    validate_degree_dsl_rule_v2(invalid_semantic)
    with pytest.raises(Exception):
        validate_requirement_rule_compat(invalid_semantic)


def test_evaluate_course_set_satisfied_and_missing():
    rule = {"type": "COURSE_SET", "courses": ["14:540:100"]}
    satisfied = evaluate_degree_requirement_rule(rule, {"14:540:100"})
    assert satisfied.supported is True
    assert satisfied.satisfied is True
    assert satisfied.missing_courses == []
    assert satisfied.explanation_codes == [EXPLANATION_SATISFIED]

    missing = evaluate_degree_requirement_rule(rule, set())
    assert missing.supported is True
    assert missing.satisfied is False
    assert missing.missing_courses == ["14:540:100"]
    assert missing.explanation_codes == [EXPLANATION_REQUIRED_MISSING, EXPLANATION_INCOMPLETE]


def test_evaluate_all_of_is_deterministic():
    rule = {
        "type": "ALL_OF",
        "children": [
            {"type": "COURSE_SET", "courses": ["14:540:100"]},
            {"type": "COURSE_SET", "courses": ["14:540:200"]},
        ],
    }
    first = evaluate_degree_requirement_rule(rule, {"14:540:100"})
    second = evaluate_degree_requirement_rule(rule, {"14:540:100"})
    assert first == second
    assert first.supported is True
    assert first.satisfied is False
    assert first.missing_courses == ["14:540:200"]
    assert first.explanation_codes == [EXPLANATION_REQUIRED_MISSING, EXPLANATION_INCOMPLETE]


def test_evaluate_n_of_satisfied_and_failed():
    rule = {
        "type": "N_OF",
        "n": 2,
        "children": [
            {"type": "COURSE_SET", "courses": ["14:540:100"]},
            {"type": "COURSE_SET", "courses": ["14:540:200"]},
            {"type": "COURSE_SET", "courses": ["14:540:300"]},
        ],
    }
    satisfied = evaluate_degree_requirement_rule(rule, {"14:540:100", "14:540:300"})
    assert satisfied.supported is True
    assert satisfied.satisfied is True
    assert satisfied.missing_courses == []
    assert satisfied.explanation_codes == [EXPLANATION_SATISFIED]

    failed = evaluate_degree_requirement_rule(rule, {"14:540:100"})
    assert failed.supported is True
    assert failed.satisfied is False
    assert failed.missing_courses == ["14:540:200"]
    assert failed.explanation_codes == [EXPLANATION_REQUIRED_MISSING, EXPLANATION_INCOMPLETE]


def test_evaluate_count_min_satisfied_and_failed():
    rule = {
        "type": "COUNT_MIN",
        "min_count": 2,
        "children": [
            {"type": "COURSE_SET", "courses": ["14:540:100"]},
            {"type": "COURSE_SET", "courses": ["14:540:200"]},
            {"type": "COURSE_SET", "courses": ["14:540:300"]},
        ],
    }
    satisfied = evaluate_degree_requirement_rule(rule, {"14:540:100", "14:540:300"})
    assert satisfied.supported is True
    assert satisfied.satisfied is True
    assert satisfied.missing_courses == []
    assert satisfied.explanation_codes == [EXPLANATION_SATISFIED]

    failed = evaluate_degree_requirement_rule(rule, {"14:540:100"})
    assert failed.supported is True
    assert failed.satisfied is False
    assert failed.missing_courses == ["14:540:200"]
    assert failed.explanation_codes == [EXPLANATION_REQUIRED_MISSING, EXPLANATION_INCOMPLETE]


def test_evaluate_n_of_failure_witness_uses_first_failed_children_only():
    # For N_OF(n=2), one satisfied + two failed children yields shortfall=1.
    # Witness set must be first failed child only, in stored order.
    rule = {
        "type": "N_OF",
        "n": 2,
        "children": [
            {"type": "COURSE_SET", "courses": ["14:540:100"]},
            {"type": "COURSE_SET", "courses": ["14:540:200"]},
            {"type": "COURSE_SET", "courses": ["14:540:300"]},
        ],
    }
    result = evaluate_degree_requirement_rule(rule, {"14:540:100"})
    assert result.supported is True
    assert result.satisfied is False
    assert result.missing_courses == ["14:540:200"]
    assert result.explanation_codes == [EXPLANATION_REQUIRED_MISSING, EXPLANATION_INCOMPLETE]


def test_evaluate_is_deterministic_with_equivalent_evidence_ordering():
    rule = {
        "type": "N_OF",
        "n": 2,
        "children": [
            {"type": "COURSE_SET", "courses": ["14:540:100"]},
            {"type": "COURSE_SET", "courses": ["14:540:200"]},
            {"type": "COURSE_SET", "courses": ["14:540:300"]},
        ],
    }
    evidence_first = set(["14:540:300", "14:540:100"])
    evidence_second = set(["14:540:100", "14:540:300"])

    first = evaluate_degree_requirement_rule(rule, evidence_first)
    second = evaluate_degree_requirement_rule(rule, evidence_second)

    assert first == second
    assert first.supported is True
    assert first.satisfied is True
    assert first.missing_courses == []
    assert first.explanation_codes == [EXPLANATION_SATISFIED]


def test_evaluate_count_min_is_deterministic_with_equivalent_evidence_ordering():
    rule = {
        "type": "COUNT_MIN",
        "min_count": 2,
        "children": [
            {"type": "COURSE_SET", "courses": ["14:540:100"]},
            {"type": "COURSE_SET", "courses": ["14:540:200"]},
            {"type": "COURSE_SET", "courses": ["14:540:300"]},
        ],
    }
    evidence_first = set(["14:540:300", "14:540:100"])
    evidence_second = set(["14:540:100", "14:540:300"])

    first = evaluate_degree_requirement_rule(rule, evidence_first)
    second = evaluate_degree_requirement_rule(rule, evidence_second)

    assert first == second
    assert first.supported is True
    assert first.satisfied is True
    assert first.missing_courses == []
    assert first.explanation_codes == [EXPLANATION_SATISFIED]


def test_legacy_course_and_all_convert_to_v2_and_evaluate():
    legacy = {"all": [{"course": "14:540:100"}, {"course": "14:540:200"}]}
    converted = convert_legacy_rule_to_degree_dsl_v2(legacy)
    assert converted == {
        "type": "ALL_OF",
        "children": [
            {"type": "COURSE_SET", "courses": ["14:540:100"]},
            {"type": "COURSE_SET", "courses": ["14:540:200"]},
        ],
    }

    eval_result = evaluate_degree_requirement_rule(legacy, {"14:540:100", "14:540:200"})
    assert eval_result.supported is True
    assert eval_result.satisfied is True
    assert eval_result.explanation_codes == [EXPLANATION_SATISFIED]


def test_legacy_any_maps_to_n_of_and_evaluates():
    legacy_any = {"any": [{"course": "14:540:100"}, {"course": "14:540:200"}]}
    converted = convert_legacy_rule_to_degree_dsl_v2(legacy_any)
    assert converted == {
        "type": "N_OF",
        "n": 1,
        "children": [
            {"type": "COURSE_SET", "courses": ["14:540:100"]},
            {"type": "COURSE_SET", "courses": ["14:540:200"]},
        ],
    }

    satisfied = evaluate_degree_requirement_rule(legacy_any, {"14:540:100"})
    assert satisfied.supported is True
    assert satisfied.satisfied is True
    assert satisfied.explanation_codes == [EXPLANATION_SATISFIED]


def test_malformed_course_set_is_unsupported_deterministically():
    malformed = {"type": "COURSE_SET", "courses": ["14:540:100", "14:540:200"]}
    result = evaluate_degree_requirement_rule(malformed, {"14:540:100"})
    assert result.supported is False
    assert result.satisfied is False
    assert result.missing_courses == []
    assert result.explanation_codes == [EXPLANATION_UNSUPPORTED_LEGACY]


def test_unsupported_legacy_shape_is_marked_unknown_deterministically():
    legacy_count = {"countAtLeast": {"n": 1, "of": [{"course": "14:540:100"}]}}
    result = evaluate_degree_requirement_rule(legacy_count, {"14:540:100"})
    assert result.supported is False
    assert result.satisfied is False
    assert result.missing_courses == []
    assert result.explanation_codes == [EXPLANATION_UNSUPPORTED_LEGACY]


def test_n_of_with_unsupported_child_is_unsupported():
    # Child is semantically invalid (COURSE_SET with >1 course), so N_OF must be unsupported.
    rule = {
        "type": "N_OF",
        "n": 1,
        "children": [{"type": "COURSE_SET", "courses": ["14:540:100", "14:540:200"]}],
    }
    result = evaluate_degree_requirement_rule(rule, {"14:540:100"})
    assert result.supported is False
    assert result.satisfied is False
    assert result.missing_courses == []
    assert result.explanation_codes == [EXPLANATION_UNSUPPORTED_LEGACY]


def test_count_min_with_unsupported_child_is_unsupported():
    # Child is semantically invalid (COURSE_SET with >1 course), so COUNT_MIN must be unsupported.
    rule = {
        "type": "COUNT_MIN",
        "min_count": 1,
        "children": [{"type": "COURSE_SET", "courses": ["14:540:100", "14:540:200"]}],
    }
    result = evaluate_degree_requirement_rule(rule, {"14:540:100"})
    assert result.supported is False
    assert result.satisfied is False
    assert result.missing_courses == []
    assert result.explanation_codes == [EXPLANATION_UNSUPPORTED_LEGACY]


def test_count_min_parity_with_equivalent_n_of():
    count_min_rule = {
        "type": "COUNT_MIN",
        "min_count": 2,
        "children": [
            {"type": "COURSE_SET", "courses": ["14:540:100"]},
            {"type": "COURSE_SET", "courses": ["14:540:200"]},
            {"type": "COURSE_SET", "courses": ["14:540:300"]},
        ],
    }
    n_of_rule = {
        "type": "N_OF",
        "n": 2,
        "children": [
            {"type": "COURSE_SET", "courses": ["14:540:100"]},
            {"type": "COURSE_SET", "courses": ["14:540:200"]},
            {"type": "COURSE_SET", "courses": ["14:540:300"]},
        ],
    }
    evidence = {"14:540:100"}
    count_min_result = evaluate_degree_requirement_rule(count_min_rule, evidence)
    n_of_result = evaluate_degree_requirement_rule(n_of_rule, evidence)
    assert count_min_result == n_of_result


def test_requirement_rule_compat_validation_accepts_legacy_and_v2():
    validate_requirement_rule_compat({"course": "14:540:100"})
    validate_requirement_rule_compat({"type": "COURSE_SET", "courses": ["14:540:100"]})
    validate_requirement_rule_compat({"any": [{"course": "14:540:100"}]})
    validate_requirement_rule_compat(
        {"type": "N_OF", "n": 1, "children": [{"type": "COURSE_SET", "courses": ["14:540:100"]}]}
    )
    validate_requirement_rule_compat(
        {
            "type": "COUNT_MIN",
            "min_count": 1,
            "children": [{"type": "COURSE_SET", "courses": ["14:540:100"]}],
        }
    )
    with pytest.raises(Exception):
        validate_requirement_rule_compat({"all": [{"course": "not-a-code"}]})


def test_rule_schema_version_inference():
    assert infer_requirement_rule_schema_version({"course": "14:540:100"}) == 1
    assert infer_requirement_rule_schema_version({"type": "COURSE_SET", "courses": ["14:540:100"]}) == 2
    assert (
        infer_requirement_rule_schema_version(
            {"type": "N_OF", "n": 1, "children": [{"type": "COURSE_SET", "courses": ["14:540:100"]}]}
        )
        == 2
    )
    assert (
        infer_requirement_rule_schema_version(
            {
                "type": "COUNT_MIN",
                "min_count": 1,
                "children": [{"type": "COURSE_SET", "courses": ["14:540:100"]}],
            }
        )
        == 2
    )
