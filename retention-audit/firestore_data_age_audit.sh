#!/usr/bin/env bash
# ============================================================================
# Firestore data-age audit (7-year retention)
# ----------------------------------------------------------------------------
# Read-only. Uses only list / query / count calls with YOUR credentials.
# Nothing is deployed, written, indexed or deleted.
#
# Where to run: Cloud Shell (GCP Console > ">_" icon). gcloud, curl and jq
# are pre-installed there.
#
# Needs: roles/datastore.viewer (Cloud Datastore Viewer) on the prod project.
#
# Usage:
#   1. Set PROJECT (and DATABASE if not "(default)") below, or export them.
#   2. ./firestore_data_age_audit.sh discover
#        -> lists top-level collections, their subcollections, and the
#           date-like fields seen in sample documents.
#   3. Fill CHECKS below with one entry per collection (the business-date
#      field that defines the record's age).
#   4. ./firestore_data_age_audit.sh
#        -> per collection: total docs, docs with the date field, docs older
#           than 7 years, % old, oldest value.
#
# Cost: count queries are billed ~1 read per 1,000 documents counted.
# ============================================================================
set -euo pipefail

PROJECT="${PROJECT:-PROD_PROJECT}"
DATABASE="${DATABASE:-(default)}"
CUTOFF_DATE="$(date -u -d '7 years ago' +%Y-%m-%d)"
SAMPLE_DOCS=20

# One entry per collection:  "collection|field|type|scope"
#   field : business-date field; dotted path for nested maps (e.g. meta.createdAt)
#   type  : timestamp | string (ISO 'YYYY-MM-DD...') | epoch_ms | epoch_s
#   scope : collection -> top-level collection only
#           group      -> every collection with this id, incl. subcollections
#                         (needs a collection-group index on the field)
CHECKS=(
  # "orders|createdAt|timestamp|collection"
  # "events|event_date|string|collection"
  # "items|created_ms|epoch_ms|group"
)

BASE="https://firestore.googleapis.com/v1/projects/${PROJECT}/databases/${DATABASE}/documents"
TOKEN="$(gcloud auth print-access-token)"

post() {  # post <full-url> <json-body>
  curl -sS -g -X POST \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "Content-Type: application/json" \
    "$1" -d "$2"
}

list_collection_ids() {  # list_collection_ids <parent-url>
  local token="" resp
  while :; do
    resp="$(post "$1:listCollectionIds" "{\"pageSize\":500${token:+,\"pageToken\":\"$token\"}}")"
    echo "$resp" | jq -r '.collectionIds[]?'
    token="$(echo "$resp" | jq -r '.nextPageToken // empty')"
    [[ -z "$token" ]] && break
  done
}

# ----------------------------------------------------------------------------
# discover: collections, subcollections, date-like fields in sample docs
# ----------------------------------------------------------------------------
discover() {
  echo "Project: ${PROJECT}   Database: ${DATABASE}   Sample: ${SAMPLE_DOCS} docs/collection"
  echo
  list_collection_ids "$BASE" | while read -r col; do
    echo "== ${col}"
    local docs
    docs="$(post "${BASE}:runQuery" \
      "{\"structuredQuery\":{\"from\":[{\"collectionId\":\"${col}\"}],\"limit\":${SAMPLE_DOCS}}}")"

    echo "   date-like fields (field / type / sample value):"
    echo "$docs" | jq -r '
      def walk_fields($prefix):
        to_entries[] | .key as $k | .value as $v | ($prefix + $k) as $p |
        if   $v.mapValue then ($v.mapValue.fields // {} | walk_fields($p + "."))
        elif $v.timestampValue then "\($p)\ttimestamp\t\($v.timestampValue)"
        elif ($v.stringValue // "" | test("^[0-9]{4}-[0-9]{2}-[0-9]{2}")) then "\($p)\tstring\t\($v.stringValue)"
        elif ($v.integerValue and ($p | test("(?i)date|time|_at$|At$|_ts$|_ms$"))) then "\($p)\tinteger\t\($v.integerValue)"
        else empty end;
      .[] | .document? // empty | .fields // {} | walk_fields("")' \
      | sort -u -t $'\t' -k1,1 | sed 's/^/     /' || true

    echo "   subcollections (seen on sampled docs):"
    echo "$docs" | jq -r '.[] | .document.name? // empty' | while read -r name; do
      list_collection_ids "https://firestore.googleapis.com/v1/${name}"
    done | sort -u | sed 's/^/     /'
    echo
  done
}

# ----------------------------------------------------------------------------
# check: counts per configured collection
# ----------------------------------------------------------------------------
cutoff_value() {  # JSON Firestore value of the cutoff for a field type
  local s; s="$(date -u -d "$CUTOFF_DATE" +%s)"
  case "$1" in
    timestamp) echo "{\"timestampValue\":\"${CUTOFF_DATE}T00:00:00Z\"}" ;;
    string)    echo "{\"stringValue\":\"${CUTOFF_DATE}\"}" ;;
    epoch_ms)  echo "{\"integerValue\":\"$(( s * 1000 ))\"}" ;;
    epoch_s)   echo "{\"integerValue\":\"${s}\"}" ;;
    *) echo "unknown type: $1" >&2; exit 1 ;;
  esac
}

min_value() {  # lowest value of a type (keeps ordering within one type)
  case "$1" in
    timestamp)        echo '{"timestampValue":"0001-01-01T00:00:00Z"}' ;;
    string)           echo '{"stringValue":""}' ;;
    epoch_ms|epoch_s) echo '{"integerValue":"-9223372036854775808"}' ;;
  esac
}

count() {  # count <collection> <allDescendants> [where-json]
  local body="{\"structuredAggregationQuery\":{\"structuredQuery\":{\"from\":[{\"collectionId\":\"$1\",\"allDescendants\":$2}]${3:+,\"where\":$3}},\"aggregations\":[{\"alias\":\"n\",\"count\":{}}]}}"
  post "${BASE}:runAggregationQuery" "$body" | jq -r '
    if type == "array" then (.[0].result.aggregateFields.n.integerValue // "0")
    else "ERROR: " + (.error.message // tostring) end'
}

oldest() {  # oldest <collection> <allDescendants> <field> <type>
  local body="{\"structuredQuery\":{\"from\":[{\"collectionId\":\"$1\",\"allDescendants\":$2}],
    \"where\":{\"fieldFilter\":{\"field\":{\"fieldPath\":\"$3\"},\"op\":\"GREATER_THAN_OR_EQUAL\",\"value\":$(min_value "$4")}},
    \"orderBy\":[{\"field\":{\"fieldPath\":\"$3\"},\"direction\":\"ASCENDING\"}],\"limit\":1}}"
  post "${BASE}:runQuery" "$body" | jq -r --arg f "$3" '
    if type == "array" then
      ([.[] | .document? // empty][0].fields // {}) as $fields
      | ($f | split(".")) as $path
      | (reduce $path[:-1][] as $k ($fields; .[$k].mapValue.fields // {}) | .[$path[-1]])
      | (.timestampValue // .stringValue // .integerValue // "n/a")
    else "ERROR: " + (.error.message // tostring) end'
}

check() {
  if [[ ${#CHECKS[@]} -eq 0 ]]; then
    echo "CHECKS is empty. Run '$0 discover' first, then fill CHECKS in this script."
    exit 1
  fi
  echo "Project: ${PROJECT}   Database: ${DATABASE}   Cutoff: < ${CUTOFF_DATE}"
  echo
  {
    printf "collection\tfield\tscope\ttotal_docs\tdocs_with_field\tdocs_older_than_7y\tpct_old\toldest\n"
    for entry in "${CHECKS[@]}"; do
      IFS='|' read -r col field type scope <<< "$entry"
      local all=false; [[ "$scope" == "group" ]] && all=true
      local lt="{\"fieldFilter\":{\"field\":{\"fieldPath\":\"${field}\"},\"op\":\"LESS_THAN\",\"value\":$(cutoff_value "$type")}}"
      local nn="{\"unaryFilter\":{\"field\":{\"fieldPath\":\"${field}\"},\"op\":\"IS_NOT_NULL\"}}"

      local total with old first pct
      total="$(count "$col" "$all")"
      with="$(count "$col" "$all" "$nn")"
      old="$(count "$col" "$all" "$lt")"
      first="$(oldest "$col" "$all" "$field" "$type")"
      pct="$(awk -v o="$old" -v t="$total" 'BEGIN { if (t+0 > 0 && o ~ /^[0-9]+$/) printf "%.1f", 100*o/t; else print "n/a" }')"

      printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" "$col" "$field" "$scope" "$total" "$with" "$old" "$pct" "$first"
    done
  } | column -t -s $'\t'
}

case "${1:-check}" in
  discover) discover ;;
  check)    check ;;
  *) echo "usage: $0 [discover|check]"; exit 1 ;;
esac
