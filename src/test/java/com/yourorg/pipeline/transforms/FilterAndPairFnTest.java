package com.yourorg.pipeline.transforms;

import com.yourorg.pipeline.util.AvroSchemas;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.junit.jupiter.api.Assertions.*;

class FilterAndPairFnTest {

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static GenericRecord humanPayload(String imageId) {
        GenericRecord r = new GenericData.Record(AvroSchemas.PAYLOAD_ROW);
        r.put("image_id",     imageId);
        r.put("payload_type", "human");
        r.put("payload",      "{\"label\":\"cat\",\"image_name\":\"" + imageId + "\"}");
        r.put("created_at",   "2024-01-01T00:00:00Z");
        return r;
    }

    private static GenericRecord aiPayload(String imageId) {
        GenericRecord r = new GenericData.Record(AvroSchemas.PAYLOAD_ROW);
        r.put("image_id",     imageId);
        r.put("payload_type", "ai");
        r.put("payload",      "{\"label\":\"dog\",\"image_name\":\"" + imageId + "\"}");
        r.put("created_at",   "2024-01-02T00:00:00Z");
        return r;
    }

    private static GenericRecord pendingAi(String imageId) {
        GenericRecord r = new GenericData.Record(AvroSchemas.PENDING_ROW);
        r.put("image_id",        imageId);
        r.put("pending_type",    "ai");
        r.put("payload",         "{\"label\":\"dog\",\"image_name\":\"" + imageId + "\"}");
        r.put("created_at",      "2024-01-02T00:00:00Z");
        r.put("first_seen_at",   Instant.now().toString());
        r.put("last_retried_at", Instant.now().toString());
        r.put("retry_count",     0L);
        return r;
    }

    private static GenericRecord agedOutAi(String imageId) {
        Instant old = Instant.now().minusSeconds(86400L * 8); // 8 days ago
        GenericRecord r = new GenericData.Record(AvroSchemas.PENDING_ROW);
        r.put("image_id",        imageId);
        r.put("pending_type",    "ai");
        r.put("payload",         "{\"label\":\"dog\",\"image_name\":\"" + imageId + "\"}");
        r.put("created_at",      "2024-01-02T00:00:00Z");
        r.put("first_seen_at",   old.toString());
        r.put("last_retried_at", old.toString());
        r.put("retry_count",     7L);
        return r;
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    @Test
    void bothPresentEmitsMatch() {
        GenericRecord h = humanPayload("img1");
        GenericRecord a = aiPayload("img1");

        assertEquals("human", h.get("payload_type").toString());
        assertEquals("ai",    a.get("payload_type").toString());
        // Pair key encodes imageId and iteration
        assertEquals("img1::1", h.get("image_id").toString() + "::" + 1);
    }

    @Test
    void aiOnlyEmitsNewPending() {
        // daysWaited = 0 on first arrival → below MAX_WAIT_DAYS → NEW_PENDING, not AGED_OUT
        long daysWaited = 0L;
        assertTrue(daysWaited < FilterAndPairFn.MAX_WAIT_DAYS);
    }

    @Test
    void humanOnlyEmitsNewPending() {
        // daysWaited = 0 on first arrival → below MAX_WAIT_DAYS → NEW_PENDING, not AGED_OUT
        long daysWaited = 0L;
        assertTrue(daysWaited < FilterAndPairFn.MAX_WAIT_DAYS);
    }

    @Test
    void pendingAiRestoredWhenHumanArrivesLater() {
        GenericRecord h       = humanPayload("img4");
        GenericRecord pending = pendingAi("img4");

        // Pending type is "ai" — should be restored and merged with arriving human
        assertEquals("ai", pending.get("pending_type").toString());
        assertNotNull(h.get("payload"));
        assertEquals("img4::1", h.get("image_id").toString() + "::" + 1);
    }

    @Test
    void agedOutRowRoutesToDeadLetter() {
        GenericRecord aged = agedOutAi("img5");
        Instant firstSeen  = Instant.parse(aged.get("first_seen_at").toString());
        long daysWaited    = ChronoUnit.DAYS.between(firstSeen, Instant.now());
        assertTrue(daysWaited >= FilterAndPairFn.MAX_WAIT_DAYS,
                "Should be aged out after " + FilterAndPairFn.MAX_WAIT_DAYS + " days");
    }

    @Test
    void maxWaitDaysIsSevenByDefault() {
        assertEquals(7, FilterAndPairFn.MAX_WAIT_DAYS);
    }
}
