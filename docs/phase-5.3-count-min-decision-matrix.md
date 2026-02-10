# Phase 5.3 Decision Matrix: `COUNT_MIN`

Status: Draft-only. This document does not authorize implementation. COUNT_MIN code changes are blocked until one model is explicitly selected and recorded here.

## Purpose
Choose the semantic model for `COUNT_MIN` before any implementation work.

## Candidates

### Option A: Reuse `N_OF` witness-style cardinality logic
- `COUNT_MIN` treated as cardinality sibling with deterministic shortfall witness.
- Failure witness is selected by stored-order failed children (no search).
- Reuses existing `_finalize` and explanation ordering contracts.

### Option B: Distinct accumulation witness model
- `COUNT_MIN` introduces a separate accumulation-oriented witness strategy.
- May support richer future expression, but requires new witness semantics and higher implementation risk.

## Evaluation Criteria
Scoring: `1` (poor) to `5` (best).

| Criterion | Option A (`N_OF` witness reuse) | Option B (new accumulation witness) |
|---|---:|---:|
| Determinism simplicity | 5 | 3 |
| Explanation stability | 5 | 3 |
| Compatibility with current `_finalize` invariants | 5 | 3 |
| Upgrade/migration risk | 5 | 2 |
| Future synthesis impact | 4 | 4 |
| Implementation complexity | 5 | 2 |
| **Total** | **29** | **17** |

## Decision
**Selected: Option A (reuse `N_OF` witness-style cardinality logic).**

## Rationale
- Maximizes deterministic behavior with minimal semantic surface change.
- Preserves explanation contracts already locked in 5.1/5.2.
- Avoids early semantic debt and optimization leakage.
- Keeps Phase 5.3 incremental rather than architectural.

## Guardrails
1. No combinatorial search or backtracking.
2. Stored order remains authoritative for witness selection.
3. Unsupported-child poisoning remains consistent with `ALL_OF`/`N_OF`.
4. No new API/DB contracts introduced by `COUNT_MIN` unless explicitly approved.

## Implementation Gate
`COUNT_MIN` implementation is blocked until the Phase 5.3 spec references this decision and locks exact evaluator/test invariants.

## Merge Note Statement
- `COUNT_MIN introduced as semantic alias of N_OF; no new evaluation model.`
- No new explanation codes introduced.
- No optimization or backtracking behavior introduced.
