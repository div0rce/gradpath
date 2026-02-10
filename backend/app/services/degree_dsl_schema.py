from __future__ import annotations

from typing import Any

from jsonschema import validate

CANONICAL_COURSE_CODE_PATTERN = r"^\d{2}:\d{3}:\d{3}$"

DEGREE_DSL_SCHEMA_V2: dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": {
        "node": {
            "oneOf": [
                {
                    "type": "object",
                    "properties": {
                        "type": {"const": "COURSE_SET"},
                        "courses": {
                            "type": "array",
                            "minItems": 1,
                            "maxItems": 1,
                            "uniqueItems": True,
                            "items": {"type": "string", "pattern": CANONICAL_COURSE_CODE_PATTERN},
                        },
                    },
                    "required": ["type", "courses"],
                    "additionalProperties": False,
                },
                {
                    "type": "object",
                    "properties": {
                        "type": {"const": "ALL_OF"},
                        "children": {
                            "type": "array",
                            "minItems": 1,
                            "items": {"$ref": "#/$defs/node"},
                        },
                    },
                    "required": ["type", "children"],
                    "additionalProperties": False,
                },
                {
                    "type": "object",
                    "properties": {
                        "type": {"const": "N_OF"},
                        "n": {"type": "integer", "minimum": 1},
                        "children": {
                            "type": "array",
                            "minItems": 1,
                            "items": {"$ref": "#/$defs/node"},
                        },
                    },
                    "required": ["type", "n", "children"],
                    "additionalProperties": False,
                },
            ]
        }
    },
    "$ref": "#/$defs/node",
}


def validate_degree_dsl_rule_v2(rule: dict[str, Any]) -> None:
    validate(instance=rule, schema=DEGREE_DSL_SCHEMA_V2)
