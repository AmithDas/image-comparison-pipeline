package com.yourorg.pipeline.transforms;

import com.google.api.services.bigquery.model.TableRow;
import com.yourorg.pipeline.util.AvroSchemas;
import com.yourorg.pipeline.util.BarricadeEncryptionUtil;
import com.yourorg.pipeline.util.JsonFieldExtractor;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Core eligibility and pairing transform.
 *
 * <p>Source payloads are Barricade-encrypted. This transform decrypts each payload
 * transiently to extract {@code image_name}, then stores the encrypted payload
 * and {@code key_id} in the {@link GenericRecord} so that decryption of field
 * values happens once, in {@link FlattenAndCompareFn}.
 */
public class FilterAndPairFn
        extends DoFn<KV<String, CoGbkResult>,
                     KV<String, KV<GenericRecord, GenericRecord>>> {

    private static final Logger LOG = LoggerFactory.getLogger(FilterAndPairFn.class);

    public static final int MAX_WAIT_DAYS = 7;

    private final String aiMethod;
    private final String humanMethod;

    public FilterAndPairFn(String aiMethod, String humanMethod) {
        this.aiMethod    = aiMethod;
        this.humanMethod = humanMethod;
    }

    // ── CoGroupByKey input tags ───────────────────────────────────────────────

    public static final TupleTag<TableRow> SOURCE_TAG  = new TupleTag<>() {};
    public static final TupleTag<TableRow> PENDING_TAG = new TupleTag<>() {};

    // ── Output tags ──────────────────────────────────────────────────────────

    public static final TupleTag<KV<String, KV<GenericRecord, GenericRecord>>> MATCHED
            = new TupleTag<>() {};
    public static final TupleTag<GenericRecord> NEW_PENDING = new TupleTag<>() {};
    public static final TupleTag<GenericRecord> AGED_OUT    = new TupleTag<>() {};

    @ProcessElement
    public void processElement(ProcessContext ctx) {
        String imageId = ctx.element().getKey();
        CoGbkResult result = ctx.element().getValue();

        // ── Partition source rows by type ────────────────────────────────────
        List<GenericRecord> humanRows = new ArrayList<>();
        List<GenericRecord> aiRows    = new ArrayList<>();

        for (TableRow row : result.getAll(SOURCE_TAG)) {
            String encryptedPayload = (String) row.get("payload");
            String keyId            = (String) row.get("key_id");
            String method           = (String) row.get("method");

            String payloadType;
            if (aiMethod.equals(method)) {
                payloadType = "ai";
            } else if (humanMethod.equals(method)) {
                payloadType = "human";
            } else {
                LOG.warn("Unrecognised method '{}' for imageId={} — skipping", method, imageId);
                continue;
            }

            // Decrypt transiently to extract image_name; encrypted payload is kept in the record.
            String decrypted = BarricadeEncryptionUtil.decrypt(keyId, encryptedPayload);
            String extractedImageId = JsonFieldExtractor.extractField(decrypted, "image_name");

            GenericRecord p = newPayloadRow(extractedImageId, keyId, payloadType,
                    encryptedPayload, (String) row.get("created_at"));
            if ("human".equals(payloadType)) {
                humanRows.add(p);
            } else {
                aiRows.add(p);
            }
        }

        // ── Restore pending row if present (at most one per image) ───────────
        GenericRecord pending = null;
        Iterator<TableRow> pendingIter = result.getAll(PENDING_TAG).iterator();
        if (pendingIter.hasNext()) {
            pending = toPendingRecord(pendingIter.next());
        }

        if (pending != null) {
            String pendingType = str(pending.get("pending_type"));
            String pendingKeyId = str(pending.get("key_id"));
            if ("human".equals(pendingType) && humanRows.isEmpty()) {
                humanRows.add(newPayloadRow(imageId, pendingKeyId, "human",
                        str(pending.get("payload")), str(pending.get("created_at"))));
                LOG.debug("Restored pending human payload for imageId={}", imageId);
            } else if ("ai".equals(pendingType) && aiRows.isEmpty()) {
                aiRows.add(newPayloadRow(imageId, pendingKeyId, "ai",
                        str(pending.get("payload")), str(pending.get("created_at"))));
                LOG.debug("Restored pending AI payload for imageId={}", imageId);
            }
        }

        boolean humanPresent = humanRows.size() == 1;
        boolean aiPresent    = aiRows.size() == 1;

        Instant now         = Instant.now();
        String firstSeenStr = pending != null ? str(pending.get("first_seen_at")) : null;
        Instant firstSeen   = firstSeenStr != null ? parseInstant(firstSeenStr) : now;
        if (firstSeen == null) firstSeen = now;
        long daysWaited = ChronoUnit.DAYS.between(firstSeen, now);
        long retryCount = pending != null
                ? ((Number) pending.get("retry_count")).longValue() + 1 : 0L;

        if (humanPresent && aiPresent) {
            // ── State 1: both present → compare ─────────────────────────────
            String pairKey = imageId + "::1";
            ctx.output(MATCHED, KV.of(pairKey, KV.of(humanRows.get(0), aiRows.get(0))));
            LOG.info("Matched imageId={} — emitting for comparison", imageId);

        } else if (humanPresent) {
            // ── State 2: human arrived, AI missing ──────────────────────────
            GenericRecord human = humanRows.get(0);
            LOG.info("Human-only imageId={} — pending AI (days waited={})", imageId, daysWaited);
            emitPendingOrAgedOut(ctx, imageId, str(human.get("key_id")), "human",
                    str(human.get("payload")), str(human.get("created_at")),
                    firstSeen, now, retryCount, daysWaited);

        } else if (aiPresent) {
            // ── State 3: AI arrived, human missing ──────────────────────────
            GenericRecord ai = aiRows.get(0);
            LOG.info("AI-only imageId={} — pending human (days waited={})", imageId, daysWaited);
            emitPendingOrAgedOut(ctx, imageId, str(ai.get("key_id")), "ai",
                    str(ai.get("payload")), str(ai.get("created_at")),
                    firstSeen, now, retryCount, daysWaited);

        } else if (pending != null && daysWaited >= MAX_WAIT_DAYS) {
            // ── State 4: neither present, pending row aged out ───────────────
            LOG.warn("imageId={} aged out after {} days — routing to dead-letter", imageId, daysWaited);
            ctx.output(AGED_OUT, newPendingRow(imageId,
                    str(pending.get("key_id")), str(pending.get("pending_type")),
                    str(pending.get("payload")), str(pending.get("created_at")),
                    firstSeen, now, retryCount));
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private void emitPendingOrAgedOut(ProcessContext ctx,
                                       String imageId, String keyId, String pendingType,
                                       String payload, String createdAt,
                                       Instant firstSeen, Instant now,
                                       long retryCount, long daysWaited) {
        GenericRecord row = newPendingRow(imageId, keyId, pendingType, payload, createdAt,
                firstSeen, now, retryCount);
        if (daysWaited >= MAX_WAIT_DAYS) {
            ctx.output(AGED_OUT, row);
        } else {
            ctx.output(NEW_PENDING, row);
        }
    }

    private static GenericRecord newPayloadRow(String imageId, String keyId,
                                                String payloadType, String payload,
                                                String createdAt) {
        GenericRecord r = new GenericData.Record(AvroSchemas.PAYLOAD_ROW);
        r.put("image_id",     imageId);
        r.put("key_id",       keyId);
        r.put("payload_type", payloadType);
        r.put("payload",      payload);
        r.put("created_at",   createdAt);
        return r;
    }

    private static GenericRecord newPendingRow(String imageId, String keyId,
                                                String pendingType, String payload,
                                                String createdAt, Instant firstSeen,
                                                Instant now, long retryCount) {
        GenericRecord r = new GenericData.Record(AvroSchemas.PENDING_ROW);
        r.put("image_id",        imageId);
        r.put("key_id",          keyId);
        r.put("pending_type",    pendingType);
        r.put("payload",         payload);
        r.put("created_at",      createdAt);
        r.put("first_seen_at",   firstSeen != null ? firstSeen.toString() : null);
        r.put("last_retried_at", now.toString());
        r.put("retry_count",     retryCount);
        return r;
    }

    private static GenericRecord toPendingRecord(TableRow row) {
        GenericRecord r = new GenericData.Record(AvroSchemas.PENDING_ROW);
        r.put("image_id",        row.get("image_id"));
        r.put("key_id",          row.get("key_id"));
        r.put("pending_type",    row.get("pending_type"));
        r.put("payload",         row.get("payload"));
        r.put("created_at",      row.get("created_at"));
        r.put("first_seen_at",   row.get("first_seen_at") != null
                ? row.get("first_seen_at").toString() : null);
        r.put("last_retried_at", row.get("last_retried_at") != null
                ? row.get("last_retried_at").toString() : null);
        r.put("retry_count",     row.get("retry_count") != null
                ? ((Number) row.get("retry_count")).longValue() : 0L);
        return r;
    }

    private static String str(Object value) {
        return value != null ? value.toString() : null;
    }

    private static Instant parseInstant(String value) {
        if (value == null) return null;
        try {
            return Instant.parse(value);
        } catch (Exception e) {
            return null;
        }
    }
}
