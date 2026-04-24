package com.yourorg.pipeline.util;

import org.junit.Test;

import java.time.Instant;

import static org.junit.Assert.*;

public class TimestampUtilTest {

    // ── formatInstant ─────────────────────────────────────────────────────────

    @Test
    public void formatInstant_wholeSeconds() {
        Instant i = Instant.parse("2026-04-24T10:30:00Z");
        assertEquals("2026-04-24T10:30:00.000000Z", TimestampUtil.formatInstant(i));
    }

    @Test
    public void formatInstant_truncatesNanosToMicros() {
        // Java Instant.toString() can produce 9-digit nanos — BQ rejects these
        Instant i = Instant.parse("2026-04-24T10:30:00.123456789Z");
        assertEquals("2026-04-24T10:30:00.123456Z", TimestampUtil.formatInstant(i));
    }

    @Test
    public void formatInstant_preservesMicros() {
        Instant i = Instant.parse("2026-04-24T10:30:00.123456Z");
        assertEquals("2026-04-24T10:30:00.123456Z", TimestampUtil.formatInstant(i));
    }

    @Test
    public void formatInstant_null() {
        assertNull(TimestampUtil.formatInstant(null));
    }

    // ── normalizeTimestamp — standard ISO-8601 ────────────────────────────────

    @Test
    public void normalize_iso8601WithZ() {
        assertEquals("2026-04-24T10:30:00.000000Z",
                TimestampUtil.normalizeTimestamp("2026-04-24T10:30:00Z"));
    }

    @Test
    public void normalize_iso8601WithNanoseconds() {
        assertEquals("2026-04-24T10:30:00.123456Z",
                TimestampUtil.normalizeTimestamp("2026-04-24T10:30:00.123456789Z"));
    }

    @Test
    public void normalize_iso8601WithMicroseconds() {
        assertEquals("2026-04-24T10:30:00.123456Z",
                TimestampUtil.normalizeTimestamp("2026-04-24T10:30:00.123456Z"));
    }

    // ── normalizeTimestamp — BigQuery export format ───────────────────────────

    @Test
    public void normalize_bqSpaceUTCWholeSeconds() {
        // Format returned by BigQuery's tabledata.list API
        assertEquals("2026-04-24T10:30:00.000000Z",
                TimestampUtil.normalizeTimestamp("2026-04-24 10:30:00 UTC"));
    }

    @Test
    public void normalize_bqSpaceUTCWithMicros() {
        assertEquals("2026-04-24T10:30:00.123456Z",
                TimestampUtil.normalizeTimestamp("2026-04-24 10:30:00.123456 UTC"));
    }

    // ── normalizeTimestamp — offset notation ──────────────────────────────────

    @Test
    public void normalize_offsetZero() {
        assertEquals("2026-04-24T10:30:00.000000Z",
                TimestampUtil.normalizeTimestamp("2026-04-24T10:30:00+00:00"));
    }

    @Test
    public void normalize_offsetZeroWithSpace() {
        assertEquals("2026-04-24T10:30:00.000000Z",
                TimestampUtil.normalizeTimestamp("2026-04-24 10:30:00+00:00"));
    }

    // ── normalizeTimestamp — edge cases ───────────────────────────────────────

    @Test
    public void normalize_null() {
        assertNull(TimestampUtil.normalizeTimestamp(null));
    }

    @Test
    public void normalize_blank() {
        assertNull(TimestampUtil.normalizeTimestamp("   "));
    }

    @Test
    public void normalize_unparseable_returnsOriginal() {
        // Should not throw; returns original string with a WARN log
        String bad = "not-a-timestamp";
        assertEquals(bad, TimestampUtil.normalizeTimestamp(bad));
    }
}
