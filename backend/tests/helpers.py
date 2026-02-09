from __future__ import annotations

from datetime import datetime


def stage_payload() -> dict:
    return {
        "source": "DEPARTMENT_CSV",
        "checksum": "sha256:test",
        "courses": [
            {"code": "14:540:100", "title": "Intro", "credits": 3, "active": True},
            {"code": "14:540:200", "title": "Advanced", "credits": 3, "active": True},
            {"code": "14:540:300", "title": "Capstone", "credits": 3, "active": True},
        ],
        "terms": [
            {"campus": "NB", "code": "2025SU", "year": 2025, "season": "SUMMER"},
            {"campus": "NB", "code": "2025FA", "year": 2025, "season": "FALL"},
        ],
        "offerings": [
            {"course_code": "14:540:100", "term_code": "2025SU", "campus": "NB", "offered": True},
            {"course_code": "14:540:200", "term_code": "2025SU", "campus": "NB", "offered": True},
            {"course_code": "14:540:300", "term_code": "2025FA", "campus": "NB", "offered": True},
        ],
        "rules": [
            {
                "course_code": "14:540:200",
                "kind": "PREREQ",
                "rule": {"all": [{"course": "14:540:100"}]},
            },
            {
                "course_code": "14:540:300",
                "kind": "PREREQ",
                "rule": {"any": [{"course": "14:540:100"}, {"course": "14:540:200"}]},
            },
        ],
        "programs": [
            {
                "code": "ISE-BS",
                "name": "Industrial Engineering",
                "campus": "NB",
                "catalog_year": "2025-2026",
                "effective_from": datetime.utcnow().isoformat(),
                "requirement_set_label": "ISE-2025",
                "requirements": [
                    {"orderIndex": 1, "label": "Intro", "rule": {"course": "14:540:100"}},
                    {"orderIndex": 2, "label": "Advanced", "rule": {"course": "14:540:200"}},
                    {
                        "orderIndex": 3,
                        "label": "Capstone",
                        "rule": {"any": [{"course": "14:540:300"}, {"course": "14:540:200"}]},
                    },
                ],
            }
        ],
    }


def stage_payload_ready() -> dict:
    return {
        "source": "DEPARTMENT_CSV",
        "checksum": "sha256:ready",
        "courses": [
            {"code": "14:540:100", "title": "Intro", "credits": 3, "active": True},
            {"code": "14:540:200", "title": "Advanced", "credits": 3, "active": True},
        ],
        "terms": [
            {"campus": "NB", "code": "2025SU", "year": 2025, "season": "SUMMER"},
        ],
        "offerings": [
            {"course_code": "14:540:100", "term_code": "2025SU", "campus": "NB", "offered": True},
            {"course_code": "14:540:200", "term_code": "2025SU", "campus": "NB", "offered": True},
        ],
        "rules": [
            {
                "course_code": "14:540:200",
                "kind": "PREREQ",
                "rule": {"all": [{"course": "14:540:100"}]},
            }
        ],
        "programs": [
            {
                "code": "ISE-BS",
                "name": "Industrial Engineering",
                "campus": "NB",
                "catalog_year": "2025-2026",
                "effective_from": datetime.utcnow().isoformat(),
                "requirement_set_label": "ISE-2025-READY",
                "requirements": [
                    {"orderIndex": 1, "label": "Intro", "rule": {"course": "14:540:100"}},
                    {"orderIndex": 2, "label": "Advanced", "rule": {"course": "14:540:200"}},
                ],
            }
        ],
    }
