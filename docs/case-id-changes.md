# Case ID Support — Change Log

Adds support for multiple **cases** sharing a single image's AI history. A
case is a human submission identified by `case_id`; one AI payload for an
image can now be compared against several independent human cases instead of
exactly one. `case_id` is purely a human-side concept — AI payloads never
carry one, since a single AI payload is shared across every case for that
image+segment.

This required splitting the durable pending state into two independently
managed pools (case state vs. AI replay history), replacing count-based AI
iteration bookkeeping with identity-based tracking, moving aging from a
group-wide bulk check to per-entity checks, and widening the AI source read
with a lookback window to tolerate source-table insertion backlog. All of
these changes are related: the identity-based tracking built for case replay
turned out to be the exact mechanism needed to make the AI lookback safe.

---

## New Files

### `src/main/resources/avro/ai_pending_row.json`

**Purpose:** Schema for the new AI replay pool (`ai_pending_comparisons`).

**Shape:** `image_id`, `key_id`, `segment`, `payload`, `created_at`,
`first_seen_at`, `last_retried_at`, `retry_count`. No `pending_type`, no
`case_id`, no match-count field — an AI row is atomic and case-agnostic.

**Why needed:** AI rows are no longer removed from pending once matched (see
`FilterAndPairFn` below) — a case discovered later must still be able to
"replay" against AI payloads that arrived before it existed. Mixing that
always-retained, case-agnostic AI pool into the same table as case state
(which has real completeness/lifecycle semantics) would force every row to
carry columns meaningless to the other type.

---

### `src/main/resources/migrations/001_split_ai_pending.sql`

**Purpose:** One-time migration for existing deployments.

**Steps:**
1. Create `ai_pending_comparisons`.
2. Move existing `pending_type = 'ai'` rows out of `pending_comparisons` into
   it.
3. Rebuild `pending_comparisons` with the remaining (human/case) rows, adding
   `case_id` and `matched_ai_keys`, dropping `matched_ai_count`.
4. Additive `ALTER TABLE ... ADD COLUMN case_id` on `comparison_results`,
   `dead_letter_comparisons`, and the `human_payloads` source table.

**Why `matched_ai_keys` is seeded `NULL`, not approximated:** the old
`matched_ai_count` only recorded *how many* AI payloads were matched, never
*which* ones — and every AI row it could have referred to was already deleted
from pending the moment it matched (the old "consume on match" behavior), so
that identity data doesn't survive anywhere to migrate. An empty
`matched_ai_keys` is the true state, not a lossy stand-in.

---

## Modified Files

### `src/main/resources/avro/pending_row.json` / `pending_comparisons`

| What | Detail |
|------|--------|
| New field `case_id` | Nullable string. Distinguishes cases sharing one image's AI payload; `null` for segments with a single case. |
| Removed field `matched_ai_count` | A plain count could not answer "have I already matched *this specific* AI payload," which became necessary once AI rows are retained indefinitely (see below) and can legitimately reappear from source across separate runs (e.g. via `--aiLookbackHours`). |
| New field `matched_ai_keys` | Semicolon-joined AI identity keys (`payload + "\|" + created_at`) already matched to this case. Its element count is the case's next `ai_iteration - 1`. |
| Table now case-only | `pending_type` values are `human`, `human:merged`, `human:<subType>` — `ai` rows moved to `ai_pending_comparisons`. |

---

### `src/main/resources/avro/comparison_result.json` / `comparison_results`

| What | Detail |
|------|--------|
| New field `case_id` | Nullable string, populated per matched pair. |
| `ai_iteration` semantics changed | No longer a strict global `created_at` ordering — see the `FilterAndPairFn` entry below. Documented directly on the DDL column. |

---

### `src/main/resources/avro/dead_letter_row.json` / `dead_letter_comparisons`

| What | Detail |
|------|--------|
| New field `case_id` | Populated for aged-out case rows; `null` for aged-out AI rows. |

---

### `src/main/resources/bigquery_ddl.sql`

| What | Detail |
|------|--------|
| `human_payloads` | New `case_id STRING` column. |
| New table `ai_pending_comparisons` | AI replay pool — see the new Avro schema above. `WRITE_TRUNCATE`d every run, partitioned by `first_seen_at`. |
| `pending_comparisons` | Case-only now; `case_id` + `matched_ai_keys` added, `matched_ai_count` removed. |
| `comparison_results` / `dead_letter_comparisons` | `case_id` added. |

---

### `src/main/java/com/yourorg/pipeline/util/SchemaRegistry.java`

| What | Detail |
|------|--------|
| New constant `AI_PENDING_ROW` | `"ai_pending_row"` — resolves to `ai_pending_row.json` on the classpath. |

---

### `src/main/java/com/yourorg/pipeline/util/SchemaUtil.java`

| What | Detail |
|------|--------|
| New method `aiPendingSchema()` | Derives the `ai_pending_comparisons` `TableSchema` from the new Avro schema, same reflective conversion as the other schemas. |

---

### `src/main/java/com/yourorg/pipeline/transforms/DecryptAndKeyFn.java`

| What | Detail |
|------|--------|
| `processElement` (human branch) | Reads the source row's plain `case_id` column (not inside the encrypted payload) and stamps it onto the emitted row alongside `_human_sub_type`, so it survives into `FilterAndPairFn`. |

---

### `src/main/java/com/yourorg/pipeline/transforms/FilterAndPairFn.java`

This is the core of the change. Summary of the new model, then the specific
mechanics.

| Concept | Old behavior | New behavior |
|---|---|---|
| Grouping key | `imageId::segment` | Unchanged — `case_id` is an *inner* dimension handled inside `processElement`, not part of the co-group key (AI rows have no case to key by). |
| Human state | One `Map<subType, GenericRecord>` per group | `Map<caseKey, Map<subType, GenericRecord>>`. Segments that never populate `case_id` fall into one internal `NO_CASE` bucket, reproducing the old single-record-per-group behavior exactly (including `authanddocreview`-style sub-type merging). |
| AI row lifecycle | Removed from pending the moment it matched | **Always** re-pended every run regardless of match status, aged only against its own `first_seen_at`. A case discovered later can still be compared against AI payloads that arrived before it existed. |
| AI iteration bookkeeping | `matched_ai_count` — a plain count per group | `matched_ai_keys` — a per-case *set* of AI identity keys (`payload + "\|" + created_at`). A case matches against whichever AI rows from the shared pool aren't yet in its set. |
| `ai_iteration` correctness | Could assign a lower iteration number to a later-discovered AI row with an *older* `created_at` than one already matched and purged — violating "ordered by created_at ASC." | Every AI row is matched **exactly once** per case regardless of arrival order — no skips, no duplicates. `ai_iteration` reflects discovery/match order (append-only, immutable once written), not a strict global `created_at` guarantee — that tradeoff is unavoidable once `comparison_results` is append-only and rows can't be retroactively renumbered. |
| Aging | One group-wide bulk check: earliest `first_seen_at` across *every* pending row (any case, any AI row) decided whether *everything* in the group aged out together. | Independent per case and independent per AI row, each against its own `first_seen_at`. One stale case or AI backlog item can no longer force-expire an unrelated, healthy case sharing the same image+segment. |
| Tags | `SOURCE_TAG`, `PENDING_TAG`, `NEW_PENDING`, `AGED_OUT` | `SOURCE_TAG`, `CASE_PENDING_TAG`, `AI_PENDING_TAG` (inputs); `CASE_PENDING`, `CASE_AGED_OUT`, `AI_PENDING`, `AI_AGED_OUT` (outputs), plus unchanged `MATCHED`. |
| `MATCHED` key format | `imageId::segment::iteration` | `imageId::segment::caseId::iteration` — `caseId` is empty for the no-case bucket. |

**Why re-reading a re-selected AI row is safe:** `matched_ai_keys` is
checked by identity, not by "did I see this in a fresh source read this
run" — so a row appearing via `--aiLookbackHours`, the retained AI pool, or
both in the same run always collapses to one entry and is filtered out of a
case's `unmatched` list if it's already in that case's `matched_ai_keys`.

---

### `src/main/java/com/yourorg/pipeline/transforms/FlattenAndCompareFn.java`

| What | Detail |
|------|--------|
| Pair key parsing | `split("::", 4)` instead of 3 — extracts `caseId` (empty → `null`) between `segment` and `iteration`. |
| `emitRow` | New `caseId` parameter, written to the `case_id` column of every emitted `TableRow`. |

---

### `src/main/java/com/yourorg/pipeline/ImageComparisonPipeline.java`

| What | Detail |
|------|--------|
| New option `--aiPendingTable` | Required. Target table for the AI replay pool. |
| New option `--aiLookbackHours` | Default `6`. See below. |
| `AiSourceQueryProvider` | Changed from a hard `windowStart`/`windowEnd` floor to a trailing lookback from `windowEnd` (`TIMESTAMP_SUB(windowEnd, INTERVAL aiLookbackHours HOUR)`), mirroring `HumanSourceQueryProvider`'s existing lookback pattern. Handles AI source-table insertion backlog: a row's `created_at` can fall inside an already-processed window even though the row isn't written to the table until later — a hard floor would permanently miss it once the window advances past it. Safe because of the `matched_ai_keys` identity tracking above. |
| Removed local `windowStart` | Only `windowEnd` is read externally now; `WindowManager.claimWindow()` still runs correctly via the shared cache regardless of which `WindowValueProvider` instance triggers it. |
| Co-group | Two-way (`SOURCE_TAG` + `PENDING_TAG`) → three-way (`SOURCE_TAG` + `CASE_PENDING_TAG` + `AI_PENDING_TAG`). |
| `FilterAndPairFn` construction | New 4th constructor arg (`aiPendingSchema`); `withOutputTags` now lists all four secondary tags. |
| Write stages | Split into `WritePendingTable` (case state, `WRITE_TRUNCATE`) and `WriteAiPendingTable` (AI pool, `WRITE_TRUNCATE`). `CASE_AGED_OUT` and `AI_AGED_OUT` are flattened together before one `WriteDeadLetterTable` (`WRITE_APPEND`). |
| Row conversion helpers | `fromPendingRecord`/`fromAgedOutRecord` split into `fromCasePendingRecord`, `fromAiPendingRecord`, `fromCaseAgedOutRecord`, `fromAiAgedOutRecord`. |

---

### `dags/config/image_comparison_config.json` / `dags/image_comparison_dag.py`

| What | Detail |
|------|--------|
| `ai_pending_table` / `aiPendingTable` | New required config entry, wired into the Dataflow template parameters. |
| `ai_lookback_hours` / `aiLookbackHours` | New config entry, default `6`. |

---

### Tests

`src/test/java/com/yourorg/pipeline/util/SchemaUtilTest.java` — assertions for `case_id`, `matched_ai_keys`, and the new `aiPendingSchema()`.

`src/test/java/com/yourorg/pipeline/transforms/FilterAndPairFnTest.java` — rewritten for the three-way co-group and new tag names; new tests:
- `newCaseReplaysExistingAiHistory` — a case discovered on a later run replays the full retained AI history.
- `reappearingMatchedAiProducesNoDuplicate` — regression test proving an AI row a case already matched does **not** produce a duplicate `comparison_results` row when it reappears from source (the scenario the old count-based design would have gotten wrong once the AI lookback was introduced).

---

## Data Flow Summary

```
BQ source rows
      │
      ▼
DecryptAndKeyFn
  AI row   ──────────────────────────────► KV("imgX::main", aiRow)
  Human row (case=A) ─► case_id=A ────────► KV("imgX::main", humanRow)
  Human row (case=B) ─► case_id=B ────────► KV("imgX::main", humanRow)
      │
      ▼
FilterAndPairFn (CoGroupByKey on "imgX::main": source + case pending + AI pending)
  humanByCase = { A: {...}, B: {...} }
  aiRows = dedup(fresh AI + retained ai_pending_comparisons), sorted by created_at

  For case A (matched_ai_keys = {AI1}):
    unmatched = aiRows - {AI1}  → e.g. [AI2]     → MATCHED "imgX::main::A::2"
  For case B (matched_ai_keys = {}, newly discovered):
    unmatched = aiRows          → [AI1, AI2]     → MATCHED "imgX::main::B::1", "...::B::2"

  AI1, AI2 always re-pended to AI_PENDING regardless of match status.
  Case A, Case B always re-pended to CASE_PENDING with updated matched_ai_keys.
      │
      ▼
FlattenAndCompareFn
  parses caseId out of the 4-part key, writes it to every output row
      │
      ▼
BigQuery comparison_results
  case_id="A" ai_iteration=2  ...
  case_id="B" ai_iteration=1  ...
  case_id="B" ai_iteration=2  ...
```
