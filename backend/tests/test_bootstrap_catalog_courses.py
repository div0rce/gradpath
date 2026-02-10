from __future__ import annotations

import importlib.util
from pathlib import Path
import subprocess
import sys

import pytest


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "bootstrap_catalog_courses.py"
SPEC = importlib.util.spec_from_file_location("bootstrap_catalog_courses", SCRIPT_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def test_extraction_precedence_prefers_course_string():
    payload = [
        {
            "courseString": "01:198:111A",
            "subject": "999",
            "courseNumber": "999",
            "title": "Intro",
            "credits": "3",
        }
    ]
    candidates, rejections = MODULE.extract_candidates_from_payload(payload)
    assert rejections == {}
    assert list(candidates.keys()) == ["01:198:111A"]
    candidate = candidates["01:198:111A"]
    assert candidate.code == "01:198:111A"
    assert candidate.title == "Intro"
    assert candidate.credits == 3


def test_rejection_reasons_are_counted():
    payload = [
        {"courseString": ""},
        {"subject": "01", "courseNumber": ""},
        {"foo": "bar"},
    ]
    _candidates, rejections = MODULE.extract_candidates_from_payload(payload)
    assert rejections == {
        "INVALID_COURSE_STRING": 1,
        "MISSING_COURSE_IDENTITY": 2,
    }


def test_missing_set_is_deterministic_and_sorted():
    payload = [
        {"courseString": " 01:198:111 "},
        {"subject": "14", "courseNumber": "332:221"},
        {"courseString": "14:332:221"},
    ]
    candidates, _ = MODULE.extract_candidates_from_payload(payload)
    missing = MODULE.compute_missing_courses(
        fetched_candidates=candidates,
        existing_normalized={"01:198:111"},
    )
    assert [row.normalized_code for row in missing] == ["14:332:221"]
    assert missing[0].code == "14:332:221"


def test_promote_requires_apply():
    proc = subprocess.run(
        [sys.executable, str(SCRIPT_PATH), "--promote"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode != 0
    assert "--promote requires --apply" in proc.stderr


def test_coverage_gating_requires_force_for_narrow_apply():
    with pytest.raises(ValueError):
        MODULE.validate_apply_gating(
            apply=True,
            use_default_coverage=False,
            strict_coverage=False,
            force=False,
            campuses=["NB"],
            term_codes=["2025SU"],
        )

    MODULE.validate_apply_gating(
        apply=True,
        use_default_coverage=False,
        strict_coverage=False,
        force=True,
        campuses=["NB"],
        term_codes=["2025SU"],
    )
