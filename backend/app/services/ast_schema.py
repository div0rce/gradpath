AST_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": {
        "node": {
            "oneOf": [
                {
                    "type": "object",
                    "properties": {
                        "course": {
                            "type": "string",
                            "pattern": r"^\d{2}:\d{3}:\d{3}$",
                        }
                    },
                    "required": ["course"],
                    "additionalProperties": False,
                },
                {
                    "type": "object",
                    "properties": {
                        "all": {
                            "type": "array",
                            "minItems": 1,
                            "items": {"$ref": "#/$defs/node"},
                        }
                    },
                    "required": ["all"],
                    "additionalProperties": False,
                },
                {
                    "type": "object",
                    "properties": {
                        "any": {
                            "type": "array",
                            "minItems": 1,
                            "items": {"$ref": "#/$defs/node"},
                        }
                    },
                    "required": ["any"],
                    "additionalProperties": False,
                },
                {
                    "type": "object",
                    "properties": {
                        "countAtLeast": {
                            "type": "object",
                            "properties": {
                                "count": {"type": "integer", "minimum": 1},
                                "of": {
                                    "type": "array",
                                    "minItems": 1,
                                    "items": {"$ref": "#/$defs/node"},
                                },
                            },
                            "required": ["count", "of"],
                            "additionalProperties": False,
                        }
                    },
                    "required": ["countAtLeast"],
                    "additionalProperties": False,
                },
            ]
        }
    },
    "$ref": "#/$defs/node",
}
