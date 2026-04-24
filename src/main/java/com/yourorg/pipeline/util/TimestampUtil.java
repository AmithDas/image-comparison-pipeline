package com.yourorg.pipeline.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;

/**
 * Timestamp normalization for BigQuery TIMESTAMP columns.
 *
 * <h3>The problem</h3>
 * BigQuery TIMESTAMP accepts RFC 3339 strings up to <b>microsecond</b> precision.
 * Several common sources produce incompatible formats:
 *
 * <ul>
 *   <li>{@code Instant.now().toString()} — nanosecond precision
 *       ({@code "2026-04-24T10:30:00.123456789Z"}). BigQuery rejects
 *       9-digit fractional seconds.</li>
 *   <li>BigQuery's own {@code tabledata.list} API — space-separated UTC string
 *       ({@code "2026-04-24 10:30:00 UTC"}) or offset notation
 *       ({@code "2026-04-24T10:30:00+00:00"}).</li>
 * </ul>
 *
 * <h3>The fix</h3>
 * <ul>
 *   <li>Use {@link #formatInstant(Instant)} wherever the code creates a new
 *       timestamp (e.g. {@code compared_at}, {@code aged_out_at},
 *       {@code last_retried_at}).</li>
 *   <li>Use {@link #normalizeTimestamp(String)} wherever a timestamp string
 *       is passed through from a BigQuery source row (e.g. {@code created_at},
 *       {@code first_seen_at}).</li>
 * </ul>
 *
 * Both methods produce {@code "YYYY-MM-DDTHH:MM:SS.ffffffZ"} — RFC 3339 with
 * exactly 6 fractional digits, which BigQuery accepts unconditionally.
 */
public final class TimestampUtil {

    private static final Logger LOG = LoggerFactory.getLogger(TimestampUtil.class);

    /**
     * BigQuery-safe RFC 3339 format: {@code "2026-04-24T10:30:00.000000Z"}.
     * Always 6 fractional digits (microsecond precision).
     */
    private static final DateTimeFormatter BQ_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")
                             .withZone(ZoneOffset.UTC);

    private TimestampUtil() {}

    // ── Public API ────────────────────────────────────────────────────────────

    /**
     * Formats an {@link Instant} as a BigQuery-safe RFC 3339 timestamp string.
     * Nanoseconds are truncated to microseconds.
     *
     * @param instant the instant to format; returns {@code null} if {@code null}
     * @return {@code "YYYY-MM-DDTHH:MM:SS.ffffffZ"}, e.g.
     *         {@code "2026-04-24T10:30:00.123456Z"}
     */
    public static String formatInstant(Instant instant) {
        if (instant == null) return null;
        return BQ_FORMAT.format(instant.truncatedTo(ChronoUnit.MICROS));
    }

    /**
     * Normalizes a timestamp string from any common format to a BigQuery-safe
     * RFC 3339 string, and returns {@code null} for null/blank input.
     *
     * <p>Handled formats (in order of attempt):
     * <ol>
     *   <li>Standard ISO-8601 instant — {@code "2026-04-24T10:30:00Z"} or
     *       {@code "2026-04-24T10:30:00.123456789Z"} (nanos truncated)</li>
     *   <li>BigQuery export format — {@code "2026-04-24 10:30:00 UTC"} or
     *       {@code "2026-04-24 10:30:00.123456 UTC"}</li>
     *   <li>BigQuery colon-microseconds format — {@code "2024-10-08 15:26:28:46200 UTC"}
     *       (BQ {@code readTableRows} returns TIMESTAMP with a colon before the fractional
     *       seconds instead of a dot)</li>
     *   <li>Offset notation — {@code "2026-04-24T10:30:00+00:00"} or
     *       {@code "2026-04-24 10:30:00+00:00"}</li>
     * </ol>
     *
     * <p>If none of the parsers succeed the original string is returned
     * unchanged with a WARN log so the problem is visible without crashing
     * the pipeline.
     *
     * @param value the raw timestamp string from a BigQuery {@code TableRow}
     * @return normalized {@code "YYYY-MM-DDTHH:MM:SS.ffffffZ"}, or {@code null},
     *         or the original value if unparseable
     */
    public static String normalizeTimestamp(String value) {
        if (value == null || value.isBlank()) return null;

        // 1. Standard ISO-8601 instant  ("...T...Z" or "...T...+00:00")
        try {
            return formatInstant(Instant.parse(value));
        } catch (DateTimeParseException ignored) { }

        // 2. BigQuery export format  ("2026-04-24 10:30:00 UTC" / "...123456 UTC")
        try {
            String normalized = value.replace(" UTC", "Z").replace(" ", "T");
            return formatInstant(Instant.parse(normalized));
        } catch (DateTimeParseException ignored) { }

        // 3. BigQuery colon-microseconds  ("2024-10-08 15:26:28:46200 UTC")
        //    BQ readTableRows() can return TIMESTAMP values where the fractional-seconds
        //    separator is a colon rather than a dot, and leading zeros may be suppressed
        //    (e.g. 046200 µs is emitted as 46200).  We:
        //      a) strip " UTC"
        //      b) find the *last* colon (the one before the microsecond digits)
        //      c) left-pad the microsecond digits to exactly 6 so Instant.parse
        //         does not misinterpret them as a decimal fraction
        //      d) replace the colon with a dot and finish parsing
        if (value.endsWith(" UTC")) {
            try {
                String withoutUtc = value.substring(0, value.length() - 4); // "2024-10-08 15:26:28:46200"
                int lastColon = withoutUtc.lastIndexOf(':');
                // Require the segment after the last colon to be all digits (microsecond field),
                // and that there is still a colon before it (i.e. a real HH:MM:SS prefix).
                String afterLastColon = withoutUtc.substring(lastColon + 1);
                if (lastColon > 0 && !afterLastColon.isEmpty() && afterLastColon.chars().allMatch(Character::isDigit)) {
                    // Left-pad to 6 digits: BQ may omit leading zeros in the µs field
                    String micros  = String.format("%06d", Long.parseLong(afterLastColon));
                    String withDot = withoutUtc.substring(0, lastColon) + "." + micros; // "2024-10-08 15:26:28.046200"
                    return formatInstant(Instant.parse(withDot.replace(" ", "T") + "Z"));
                }
            } catch (DateTimeParseException | NumberFormatException ignored) { }
        }

        // 4. Offset notation  ("2026-04-24T10:30:00+00:00" / with space)
        try {
            String normalized = value.replace(" ", "T");
            return formatInstant(
                    OffsetDateTime.parse(normalized, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                                  .toInstant());
        } catch (DateTimeParseException ignored) { }

        LOG.warn("Could not normalize timestamp '{}' to RFC 3339 — writing as-is", value);
        return value;
    }
}
