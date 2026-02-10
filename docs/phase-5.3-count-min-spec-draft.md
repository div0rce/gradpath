# Phase 5.3 Draft Spec: `COUNT_MIN` (Blocked Until Gate)

## Status
- State: `Draft`
- Implementation: `Blocked`
- Draft-only; does not authorize implementation.
- Block condition: Must follow the approved decision in `docs/phase-5.3-count-min-decision-matrix.md`.

## Goal
Add `COUNT_MIN` as a cardinality-only rule node without reopening 5.2 semantics.

## Proposed Node Shape (Draft)
```json
{
  "type": "COUNT_MIN",
  "min_count": 2,
  "children": [
    { "type": "COURSE_SET", "courses": ["14:540:100"] },
    { "type": "COURSE_SET", "courses": ["14:540:200"] }
  ]
}
```

## Draft Semantic Rules
1. Structural scope:
   - `children` only (no courses shortcut).
2. Validation:
   - `min_count` integer, `>= 1`.
   - `min_count <= len(children)`.
3. Unsupported propagation:
   - If any child is unsupported, parent is unsupported.
4. Failure witness (deterministic, no optimization):
   - Let `S` = satisfied children in stored order.
   - Let `F` = failed children in stored order.
   - If `|S| < min_count`, witness = first `min_count - |S|` children of `F`.
5. Explanations:
   - Reuse existing `_finalize` invariants and ordering.
   - No new explanation code in first `COUNT_MIN` cut unless explicitly approved.

## Non-Goals
- No optimization/backtracking.
- No synthesis planner behavior.
- No API/DB changes.
- No legacy mapping addition in this draft.

## Required Test Scenarios (for implementation phase)
1. Determinism under equivalent evidence ordering.
2. Witness reproducibility across reruns.
3. Unsupported-child poisoning consistency.
4. Readiness/audit blocker stability.
5. Integration with existing legacy compatibility behavior.

## Gate Statement
Do not implement `COUNT_MIN` until this draft is promoted to an approved Phase 5.3 spec and acceptance tests are locked.
