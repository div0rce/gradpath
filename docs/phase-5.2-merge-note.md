# Phase 5.2 Merge Note (`N_OF` + legacy `any`)

## Status
- Phase: `5.2`
- State: `Merged semantic baseline`
- Type: Documentation closeout (no API/DB contract changes)

## Scope Shipped
- Added v2 DSL node: `N_OF` (children-only).
- Added legacy compatibility mapping: `{"any":[...]}` -> `{"type":"N_OF","n":1,"children":[...]}`.
- Added semantic validator for v2 relational constraints (`n <= len(children)`).
- Updated deterministic evaluator behavior and tests.
- Updated readiness expectation impacted by legacy `any` no longer being unsupported.

## Locked Invariants (Semantic Freeze)
The following are now baseline semantics and must not change without an explicit phase decision:

1. `COURSE_SET` is a single-course exact-match leaf.
2. `ALL_OF` is deterministic and never emits satisfied on failure.
3. `N_OF` uses children-only shape and deterministic witness selection:
   - If `|S| < n`, witness is first `n - |S|` failed children in stored order.
4. Unsupported-child poisoning:
   - If any child is unsupported, parent `N_OF` is unsupported.
5. Explanation invariants:
   - `supported=False` => `[UNSUPPORTED_LEGACY_RULE]` only.
   - `satisfied=True` => `[REQUIREMENT_SATISFIED]` only.
   - Failure includes `REQUIREMENT_INCOMPLETE`, excludes `REQUIREMENT_SATISFIED`.
6. Determinism invariants:
   - Stored child order is authoritative.
   - Missing courses sorted at final boundary.
   - Explanation codes ordered via stable priority.

## Explicit Non-Goals / Deferred Work
- No `COUNT_MIN`.
- No `CREDIT_MIN`.
- No synthesis/generation/ranking logic.
- No optimization/backtracking search.
- No API route/schema changes.
- No DB schema changes.

## Operational Migration Guidance
- Migration script: `backend/scripts/migrate_requirement_rules_v2.py`
- Safe default:
  - Run dry-run first.
  - Do not run `--apply` automatically in production.
- Expected dry-run output keys:
  - `scanned`, `already_v2`, `converted`, `unsupported`, `apply`

## Semantic Freeze Statement
Phase 5.2 semantics are frozen as the authoritative substrate for future degree-rule phases.  
Any change to witness selection, unsupported propagation, or explanation invariants requires an explicit Phase 5.x design decision and test-contract update.
