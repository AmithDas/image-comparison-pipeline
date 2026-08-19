# Multi-Case Reconciliation — Architecture Briefing

**Image Comparison Pipeline** · Prepared for: engineering manager · Status: ready to deploy pending migration

---

## Summary

The pipeline used to assume **one human case per image**. It now supports
several independent cases sharing a single AI submission, closes a gap where
AI data arriving late — up to three hours behind, at peak — was silently
skipped, and fixes a sequencing bug in how AI comparison rounds were
numbered.

Existing usage is unaffected: images that only ever had one case behave
exactly as before. Shipping requires one database migration, run once,
before deploy — outlined below.

| | |
|---|---|
| **1** new table | **4** tables updated |
| **6h** backlog tolerance | **67** automated tests passing |

---

## Why

A single AI submission for an image can now open several separate human
review cases — each one needs to be compared against that AI data
independently, tracked on its own timeline, matched to its own set of AI
iterations. The pipeline had no notion of a "case" distinct from "image," so
this wasn't representable before.

A second, unrelated problem surfaced while this work was underway: at peak
load, some AI submissions land in the source table up to three hours after
they logically occurred. The pipeline's batch windows only ever move
forward, so once a window closed, a late submission that belonged to it was
gone for good — the image was never compared, with no error or alert to say
so.

---

## What changed

**Cases are now first-class.** Matching, pending status, and expiry are
tracked per case rather than per image. Two cases opened against the same
image are compared independently and never interfere with each other.

**AI submissions are kept, not consumed.** Previously, once an AI submission
matched a case, it was discarded — a case discovered afterward could never
be compared against it. AI submissions are now retained and reused across
every case for that image, including cases opened well after the AI data
first arrived.

**Late AI data is no longer dropped.** Batch reads now look six hours
further back by default, catching submissions delayed by backlog. A
safeguard keyed to each submission's own identity prevents this from
re-processing anything already handled — no duplicate comparison rows get
created by re-reading the same data.

**Comparison round numbering, corrected.** Under load, a submission could
occasionally be assigned a lower round number than one that was
chronologically earlier but arrived later. This is fixed for everything
compared going forward. Rows already written under the old numbering are
not retroactively corrected — see Risk & Rollout below.

### Core behavior — before and after

| Behavior | Before | After |
|---|---|---|
| Cases per image | Exactly one | Any number, tracked independently |
| AI submission lifecycle | Discarded after first match | Retained; reused by every case |
| Delayed AI data | Permanently missed once its window closed | Caught within a 6-hour trailing window |
| Expiry of stale items | One old item could expire an entire image's backlog at once | Each case and each AI submission expires on its own timeline |

---

## Risk & rollout

The new structure requires a one-time database migration before this
version is deployed — the pipeline will refuse to write against the old
table shape rather than silently corrupt data, so this is a hard
prerequisite, not an optional cleanup step.

**Migration — run once, before deploy:**
1. Create the new AI-submission table.
2. Move currently in-flight AI submissions into it.
3. Add case tracking to the existing pending-work table.
4. Add the new case field to the results and audit tables (additive — no downtime).

No submission or comparison data is lost in the migration — everything that
can be preserved is. The one exception is deliberate: the old "how many
rounds matched" counter is replaced by a more precise per-round record, and
that old counter never recorded *which* rounds it referred to — so there's
nothing more specific to carry forward, and none of that data is still
reachable elsewhere in the system to recover.

Both fixes are covered by automated tests, including two written
specifically against the failure modes this change set out to close: a case
discovered after the fact still gets compared against prior AI history, and
a late-arriving AI submission is never compared twice.

---

## What didn't change

- Field-level comparison and match logic — untouched.
- Existing single-case images — identical behavior to today.
- Primary output tables keep their names; only new, optional fields are added.

---

*Full technical change log: [`case-id-changes.md`](case-id-changes.md)*
