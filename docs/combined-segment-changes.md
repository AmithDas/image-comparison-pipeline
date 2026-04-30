# Combined Segment Feature — Change Log

Implements support for two business segments (auth and docreview) that share
the same `aiMethod` and `humanMethod`. The AI sends one payload covering both;
humans send two separate payloads which are merged before comparison. The
comparison table emits `segment = "auth"` or `segment = "docreview"` rows
(never `"combined"`), with shared fields written to both.

---

## New Files

### `src/main/java/com/yourorg/pipeline/util/PayloadParser.java`

**Purpose:** Parses the `id="<IMAGE_NAME>",request="<JSON>"` wire format used
by human payloads in the combined segment.

**Key API:**
```java
PayloadParser.Parsed p = PayloadParser.parse(decrypted);
p.imageId()  // → the image name extracted from id="..."
p.json()     // → the inner JSON string with \" unescaped
```

**Why needed:** Human payloads for the combined segment are not plain JSON —
they are wrapped in the `id_request` format. All places that need to extract
the image name or inner JSON from a human payload must go through this parser
when `seg.isIdRequestFormat()` is true.

---

### `src/main/java/com/yourorg/pipeline/transforms/HumanMerger.java`

**Purpose:** Merges two sub-type `GenericRecord`s (auth + docreview) into a
single record whose payload is re-encrypted and can be consumed by
`FlattenAndCompareFn` and `OrphanCompareFn` without any special handling.

**Merge strategy:**
1. Decrypts each sub-type's payload and applies `PayloadParser` when
   `payloadFormat = "id_request"`.
2. Identifies the **base sub-type** as the one whose `discriminatorField`
   value is a JSON array (docreview → `documentDetails`). Its full JSON
   becomes the starting point of the merged payload.
3. Each non-base sub-type contributes **only its `discriminatorField` value**
   (auth → `verifiedData`). No other fields from the non-base sub-type are
   carried over; shared fields (e.g. `customerId`) are taken from the base.
4. Picks the **earliest `created_at`** across all sub-types.
5. Re-encrypts the merged JSON with `BarricadeEncryptionUtil.encrypt()` so
   all downstream transforms call `decrypt()` uniformly.

**Result shape (example):**
```json
{
  "documentDetails": [{ "docType": "passport", ... }],
  "customerId": "C123",
  "verifiedData": { "status": "approved", ... }
}
```

---

## Modified Files

### `src/main/java/com/yourorg/pipeline/config/SegmentConfig.java`

**Changes:**

| What | Detail |
|------|--------|
| New field `payloadFormat` | `"id_request"` or `null` (plain JSON). |
| New field `humanSubTypes` | `List<HumanSubType>` — non-null when multiple human payloads must be merged. |
| New nested class `HumanSubType` | `name` (e.g. `"auth"`) + `discriminatorField` (top-level JSON key whose presence identifies this sub-type). |
| New method `requiresHumanMerge()` | Returns `true` when `humanSubTypes` has more than one entry. |
| New method `isIdRequestFormat()` | Returns `true` when `payloadFormat == "id_request"`. |
| New static `buildMethodToSegmentsMap()` | Returns `Map<String, List<SegmentConfig>>` covering both AI and human methods. Replaces the old `buildMethodToSideMap()` usage in `DecryptAndKeyFn`. |
| New static `buildNameMap()` | Returns `Map<String, SegmentConfig>` for fast lookup by segment name. Used by `FilterAndPairFn`. |
| Updated constructors | Added 5-arg constructor and Gson-compatible no-arg constructor. |
| Validation | Extended to validate `humanSubTypes[].name` and `.discriminatorField`. |

**Example config JSON for the combined segment:**
```json
{
  "name": "combined",
  "aiMethod": "aimetadata",
  "humanMethod": "controller.SubmitDispute",
  "payloadFormat": "id_request",
  "humanSubTypes": [
    { "name": "auth",      "discriminatorField": "verifiedData"    },
    { "name": "docreview", "discriminatorField": "documentDetails" }
  ]
}
```

---

### `src/main/java/com/yourorg/pipeline/transforms/DecryptAndKeyFn.java`

**Changes:**

| What | Detail |
|------|--------|
| `methodToSegments` type | Changed from `Map<String, String>` to `Map<String, List<SegmentConfig>>`. A method can now map to multiple segments. |
| AI row routing | Loops over all `SegmentConfig`s that list this `aiMethod` and emits one `KV<imageKey::segmentName, row>` per segment (broadcast). |
| Human row routing | Calls `resolveHumanJson()` (applies `PayloadParser` when `id_request`), then `resolveSubType()` to identify which sub-type this row belongs to, then injects `_human_sub_type` into the emitted row. |
| `resolveSubType()` | Uses `JsonObject.has(discriminatorField)` — **not** `JsonFieldExtractor.extractField()` — because `documentDetails` is an array and `extractField` returns `null` for arrays. |
| Image key extraction | `extractImageKey()` uses `PayloadParser.imageId()` for `id_request` format; falls back to `imageNameField` for plain JSON. |
| Factory methods unchanged | `forAi()` and `forHuman()` signatures are unchanged. |

---

### `src/main/java/com/yourorg/pipeline/transforms/FilterAndPairFn.java`

**Changes:**

| What | Detail |
|------|--------|
| `segmentByName` | New transient `Map<String, SegmentConfig>` built in `@Setup` from `segmentConfigsJson`. |
| `humanBySubType` in pending state | Keyed by sub-type name (`"auth"`, `"docreview"`, `"default"`, `"_merged"`). |
| `pendingType` values | `"human"` (simple), `"human:<subTypeName>"` (partial multi-sub-type), `"human:merged"` (all sub-types collected), `"ai"`. |
| Shared group clock | At the start of each `processElement`, scans all pending rows to find the **earliest `first_seen_at`** across the group. This prevents a late-arriving sub-type from resetting the clock and creating zombie state after the other sub-type aged out. |
| Age-out | If `groupDaysWaited >= MAX_WAIT_DAYS`, all pending rows for the group are immediately aged out and the group is cleared. |
| `isHumanComplete()` | Checks for `"_merged"` key first, then whether all sub-type names from `SegmentConfig.humanSubTypes` are present, then falls back to `"default"`. |
| `resolveHumanRecord()` | Calls `HumanMerger.merge()` for multi-sub-type segments; returns the single `GenericRecord` directly for simple segments. |
| `resolvedPendingType()` | Returns `"human:merged"` for merge segments, `"human"` for simple segments. |
| Pending restore | Handles `"human:merged"` → `"_merged"` key, `"human:<subType>"` → sub-type key, `"human"` → `"default"`, `"ai"`. |

---

### `src/main/java/com/yourorg/pipeline/transforms/FlattenAndCompareFn.java`

**Changes:**

| What | Detail |
|------|--------|
| `ARRAY_MATCH_KEYS` | Added `"documentDetails" → "docType"`. Array elements in the `documentDetails` field are now matched by their `docType` value rather than by position. |
| Constructor | Now takes a third `ValueProvider<String> segmentConfigsJson` argument. |
| `@Setup` | Parses `segmentConfigsJson` and builds `rootKeyToSegment` (`Map<String, String>`: root JSON key → sub-type name) and `subTypeNames` (`List<String>`) from `humanSubTypes[].discriminatorField`. |
| `resolveSegments(fieldPath, parentSegment)` | New helper. For a given flattened field path, returns the segment(s) to label the output row(s) with: <ul><li>If the field's root key is a known discriminator (e.g. `verifiedData` → `"auth"`), returns that one sub-type.</li><li>If no match, returns all sub-type names so shared fields are written to **both** `"auth"` and `"docreview"` rows.</li><li>Falls back to `parentSegment` when no sub-types are configured (simple segments).</li></ul> |
| `processElement` | Both the keyed and non-keyed emit loops now iterate over `resolveSegments()` and call `emitRow()` once per returned segment. |

---

### `src/main/java/com/yourorg/pipeline/ImageComparisonPipeline.java`

**Changes:**

| What | Detail |
|------|--------|
| `DecryptAndKeyFn.forAi` call | Now passes `options.getSegmentConfigs()` (`ValueProvider<String>`) instead of the removed `methodToSide` map. |
| `DecryptAndKeyFn.forHuman` call | Same — passes `segmentConfigs` and also passes `options.getHumanFilterField()` / `options.getHumanFilterValue()` as `ValueProvider`s directly. |
| `FilterAndPairFn` call | Passes `options.getSegmentConfigs()`. Removed the now-redundant `methodToSide` local variable. |
| `FlattenAndCompareFn` call | Updated from 2-arg to 3-arg constructor, adding `options.getSegmentConfigs()`. |

---

### `src/test/java/com/yourorg/pipeline/transforms/FilterAndPairFnTest.java`

**Changes:**

| What | Detail |
|------|--------|
| `pendingRow` helper | Added `segment = "main"` field to match the new `segment` column written by `FilterAndPairFn`. |
| Pair key assertions | Updated from `"img001::1"` format to `"img001::main::1"` (3-part key: `imageId::segment::iteration`). |
| `retryCountAsStringDoesNotThrow` | Fixed assertion from `2L` to `0L` — a fresh source human row has no prior pending metadata, so `retry_count` starts at `0`. |

---

## Data Flow Summary (Combined Segment)

```
BQ source rows
      │
      ▼
DecryptAndKeyFn
  AI row  ──────────────────────────────► KV("imgX::combined", aiRow)
  Human row (auth)    ─► _human_sub_type=auth      ► KV("imgX::combined", humanRow)
  Human row (docreview) ► _human_sub_type=docreview ► KV("imgX::combined", humanRow)
      │
      ▼
FilterAndPairFn (CoGroupByKey on "imgX::combined")
  Collects humanBySubType = {auth: rec, docreview: rec}
  When both present → HumanMerger.merge() → merged GenericRecord (re-encrypted)
  Pairs with AI GenericRecord → KV("imgX::combined::1", KV(merged, ai))
      │
      ▼
FlattenAndCompareFn
  resolveSegments("verifiedData.status",  "combined") → ["auth"]
  resolveSegments("documentDetails.docType", "combined") → ["docreview"]
  resolveSegments("customerId",            "combined") → ["auth", "docreview"]
      │
      ▼
BigQuery comparison table
  segment="auth"      rows: verifiedData.* fields + shared fields
  segment="docreview" rows: documentDetails.* fields + shared fields
```
