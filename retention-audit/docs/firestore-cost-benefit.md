# Firestore retention — cost benefit

*Prepared 2026-09-29. Status: estimate — the share of data older than 7 years
has not been measured yet (see [Next steps](#next-steps)).*

## Summary

Firestore currently costs about **$76,000 per month** for storage alone:

| Cost line | Per month | Per year |
|---|---|---|
| Stored data (documents + indexes) | ~$38,000 | ~$456,000 |
| Point-in-time recovery (PITR) | ~$38,000 | ~$456,000 |
| **Total** | **~$76,000** | **~$912,000** |

*Figures assumed to be monthly, in USD, for the prod project. Confirm against
the billing report (Service = Cloud Firestore, grouped by SKU).*

Purging data older than 7 years — already required by the compliance policy —
reduces **both** lines, because PITR is billed on roughly the size of the
database. Every GiB removed is saved twice.

## Why PITR tracks storage

PITR keeps a restorable version history for the last 7 days and is billed on
roughly the database size plus changes made in that window. That is why the
two lines are about equal today, and why:

- anything that shrinks the database also shrinks PITR;
- the PITR saving appears about **7 days after** the deletes, once deleted
  versions age out of the PITR window.

## Savings by share of old data

| Share of data older than 7 years | Storage saving/month | PITR saving/month | Total/month | Per year |
|---|---|---|---|---|
| 10% | $3,800 | $3,800 | $7,600 | ~$91k |
| 20% | $7,600 | $7,600 | $15,200 | ~$182k |
| 30% | $11,400 | $11,400 | $22,800 | ~$274k |
| 50% | $19,000 | $19,000 | $38,000 | ~$456k |

Calculation: `share × $38,000` for storage, the same for PITR.

## Cost levers, largest first

### 1. PITR requirement review — up to ~$38k/month, no data deleted

- Is 7-day point-in-time recovery a hard requirement (e.g. minutes-level
  recovery after a bad deploy)?
- If daily recovery points are enough, **scheduled backups** (Firestore →
  Backups, daily, short retention) may cost much less. Compare the backup
  storage rate for the database location with the PITR rate before deciding.
- This is a risk decision for the platform / disaster-recovery owners. If PITR
  stays, every other lever saves twice.

### 2. Purge data older than 7 years — `2 × old share × $38k`

The scan and purge pipelines already designed
([scan](firestore-scan-dataflow.txt), [purge](firestore-purge-dataflow.txt)).
Required for compliance regardless of cost.

### 3. Index exemptions — potentially large, no data deleted

Firestore indexes every field automatically (ascending and descending). On
large documents, index entries are often half or more of billed storage.

- **Size it:** billed storage ÷ sum of `est_size_bytes` from the scan = index
  overhead factor. A factor of 2+ means indexes are as large as the data.
- **Act:** Firestore → Indexes → Single field → *Add exemption* for fields
  never filtered or sorted on (large strings, payload blobs, big maps/arrays);
  remove unused composite indexes.
- **Risk:** any query relying on an exempted field fails. Every exemption must
  be checked against the application's queries first.

## One-time costs

| Item | Calculation |
|---|---|
| Scan reads | total documents × read price |
| Pre-purge safety backup | documents in affected collections × read price + 30 days of GCS storage |
| Dry run + verification reads | ~2 × old documents × read price |
| Purge deletes | old documents × delete price |
| Dataflow compute | a few worker-hours per job |

At ~$38k/month the database is likely hundreds of TiB — possibly **billions of
documents**. Example: 10 billion documents at ~$0.03–0.06 per 100,000 reads ≈
**$3k–6k** for the scan. Small against a saving of $10k+/month, but it should
be confirmed with real document counts before the scan is approved.

**Payback** = one-time cost ÷ monthly saving — expected to be **well under one
month** at any old-data share above ~5%.

## Archiving old data to GCS instead of deleting

Technically cheap: GCS Archive is ~$0.0012/GiB-month versus ~$0.36/GiB-month
for Firestore storage + PITR, and archived data carries no index overhead and
is compressed. An archive would give up almost none of the saving.

**But compliance decides, not cost.** The policy is to *store data only for
the last 7 years* (maximum retention). Data in a GCS archive is still stored,
so it likely still breaches the policy. Question for compliance:

> Does the 7-year rule mean data must be **deleted** after 7 years, or that it
> must be **kept at least** 7 years, with older data allowed in a restricted
> archive?

| Answer | Approach |
|---|---|
| Maximum retention | No long-term archive; keep only the 30-day safety backup, then delete |
| Minimum retention / archive allowed | Archive to GCS (Avro/Parquet, Archive class, restricted, lifecycle-deleted), then delete from Firestore |
| Some categories must be kept longer | Archive only those categories with their own retention rule; delete the rest |

Whatever the answer, each purge batch keeps a **30-day safety backup** — a
rollback window for mistakes, not an archive.

## Measuring the actual benefit

1. **Baseline:** billing export or report, Service = Cloud Firestore, grouped
   by SKU, last 30 days. `cost ÷ usage` for *Stored data* = effective rate per
   GiB-month; `storage cost ÷ rate` = billed database size.
2. **Old share:** from the scan table (`est_size_bytes` for documents older
   than 7 years ÷ total), or a `createTime` sample per large collection before
   the scan exists.
3. **Estimate:** `old share × (storage + PITR)` per month.
4. **Confirm after the purge:** compare the *Stored data* and PITR SKUs for the
   month before and after (billing uses daily averages; allow ~7 days for PITR
   to reflect the deletes).

## Next steps

1. Confirm the figures are monthly USD and get the effective storage rate from
   billing.
2. Ask the disaster-recovery owners whether 7-day PITR is required.
3. Ask compliance the maximum-vs-minimum retention question above.
4. Get document counts and a `createTime` sample for the largest collections
   to estimate the old share and the scan cost.
5. Update this document with the measured old share and a firm saving/payback
   figure.
