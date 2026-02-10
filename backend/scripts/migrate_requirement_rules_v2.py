#!/usr/bin/env python3
from __future__ import annotations

# RequirementNode ownership invariant:
# RequirementNode rows are program-version scoped and immutable per plan.
# Plans reference RequirementNode IDs; do not create plan-local copies/mutations.

import argparse
import json

from sqlalchemy import select

from app.db import SessionLocal
from app.models import RequirementNode
from app.services.degree_dsl_engine import convert_legacy_rule_to_degree_dsl_v2


def main() -> int:
    parser = argparse.ArgumentParser(description="Migrate legacy RequirementNode rules to Degree DSL v2.")
    parser.add_argument("--apply", action="store_true", help="Persist converted rules instead of dry-run.")
    args = parser.parse_args()

    scanned = 0
    already_v2 = 0
    converted = 0
    unsupported = 0

    with SessionLocal() as db:
        rows = db.execute(select(RequirementNode)).scalars().all()
        for row in rows:
            scanned += 1
            rule = row.rule or {}
            if isinstance(rule, dict) and isinstance(rule.get("type"), str):
                already_v2 += 1
                continue

            mapped = convert_legacy_rule_to_degree_dsl_v2(rule)
            if mapped is None:
                unsupported += 1
                continue

            converted += 1
            if args.apply:
                row.rule = mapped
                row.rule_schema_version = 2
                db.add(row)

        if args.apply:
            db.commit()

    print(
        json.dumps(
            {
                "scanned": scanned,
                "already_v2": already_v2,
                "converted": converted,
                "unsupported": unsupported,
                "apply": bool(args.apply),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
