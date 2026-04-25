package com.yourorg.pipeline.util;

import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.JsonElement;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.List;

/**
 * Converts an Avro {@link GenericRecord} to a BigQuery {@link TableRow}
 * without requiring an external Avro schema file — the schema is read
 * directly from the record at runtime.
 *
 * <h3>Type mapping</h3>
 * <table border="1">
 *   <tr><th>Avro type / logical type</th><th>BigQuery / Java type</th></tr>
 *   <tr><td>{@code null}</td><td>{@code null}</td></tr>
 *   <tr><td>{@code string} / {@code Utf8}</td><td>{@code String}</td></tr>
 *   <tr><td>{@code int}, {@code long}, {@code float}, {@code double}, {@code boolean}</td>
 *       <td>passed through as-is</td></tr>
 *   <tr><td>{@code bytes} / {@code fixed}</td><td>Base64-encoded {@code String}</td></tr>
 *   <tr><td>{@code record}</td><td>nested {@link TableRow}</td></tr>
 *   <tr><td>{@code array}</td><td>{@code List<Object>}</td></tr>
 *   <tr><td>logical type {@code timestamp-millis}</td>
 *       <td>RFC 3339 {@code String} via {@link TimestampUtil}</td></tr>
 *   <tr><td>logical type {@code timestamp-micros}</td>
 *       <td>RFC 3339 {@code String} via {@link TimestampUtil}</td></tr>
 *   <tr><td>custom prop {@code "bqType":"TIMESTAMP"}</td>
 *       <td>normalised RFC 3339 {@code String} via {@link TimestampUtil}</td></tr>
 * </table>
 */
public final class AvroToTableRowConverter {

    private static final Logger LOG = LoggerFactory.getLogger(AvroToTableRowConverter.class);

    private AvroToTableRowConverter() {}

    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Converts a {@link GenericRecord} to a BigQuery {@link TableRow}.
     * Each field in the record's schema is mapped to a column using
     * {@link #convertValue(Object, Schema)}.
     *
     * @param record the Avro record to convert; must not be {@code null}
     * @return a {@link TableRow} with one entry per schema field
     */
    public static TableRow toTableRow(GenericRecord record) {
        TableRow row = new TableRow();
        for (Schema.Field field : record.getSchema().getFields()) {
            Object value = record.get(field.name());
            try {
                row.set(field.name(), convertValue(value, field.schema()));
            } catch (Exception e) {
                LOG.warn("Failed to convert field '{}' — setting null. Error: {}",
                        field.name(), e.getMessage());
                row.set(field.name(), null);
            }
        }
        return row;
    }

    // ── private helpers ───────────────────────────────────────────────────────

    /**
     * Recursively converts a single Avro value to its BigQuery-compatible
     * representation, guided by the field's {@link Schema}.
     *
     * @param value  the raw Avro value (may be {@code null})
     * @param schema the Avro schema for this value
     * @return the converted value, or {@code null} if the input is {@code null}
     */
    private static Object convertValue(Object value, Schema schema) {
        if (value == null) return null;

        // Unwrap union (e.g. ["null", "long"]) to get the actual type.
        Schema actual = unwrapUnion(schema);

        // ── Logical types (timestamps) ────────────────────────────────────────
        if (actual.getLogicalType() != null) {
            String logicalType = actual.getLogicalType().getName();
            if ("timestamp-millis".equals(logicalType)) {
                long millis = ((Number) value).longValue();
                return TimestampUtil.formatInstant(Instant.ofEpochMilli(millis));
            }
            if ("timestamp-micros".equals(logicalType)) {
                long micros = ((Number) value).longValue();
                return TimestampUtil.formatInstant(
                        Instant.ofEpochSecond(micros / 1_000_000L,
                                             (micros % 1_000_000L) * 1_000L));
            }
        }

        // ── Custom "bqType":"TIMESTAMP" property ──────────────────────────────
        JsonElement bqTypeProp = actual.getJsonProp("bqType");
        if (bqTypeProp != null && "TIMESTAMP".equals(bqTypeProp.getAsString())) {
            return TimestampUtil.normalizeTimestamp(value.toString());
        }

        // ── Structural types ──────────────────────────────────────────────────
        if (value instanceof GenericRecord) {
            return toTableRow((GenericRecord) value);
        }

        if (value instanceof Collection) {
            List<Object> list = new ArrayList<>();
            Schema elementSchema = actual.getType() == Schema.Type.ARRAY
                    ? actual.getElementType()
                    : Schema.create(Schema.Type.STRING); // fallback
            for (Object item : (Collection<?>) value) {
                list.add(convertValue(item, elementSchema));
            }
            return list;
        }

        // ── Primitive types ───────────────────────────────────────────────────
        if (value instanceof Utf8) {
            return value.toString();
        }

        if (value instanceof ByteBuffer) {
            byte[] bytes = new byte[((ByteBuffer) value).remaining()];
            ((ByteBuffer) value).get(bytes);
            return Base64.getEncoder().encodeToString(bytes);
        }

        // int, long, float, double, boolean — pass through as-is
        return value;
    }

    /**
     * Unwraps a union schema to its first non-null type.
     * For non-union schemas, returns the schema unchanged.
     *
     * <pre>
     *   ["null", "string"]                  → STRING
     *   ["null", {"type":"long","logicalType":"timestamp-micros"}] → LONG (timestamp-micros)
     *   "string"                            → STRING  (unchanged)
     * </pre>
     */
    private static Schema unwrapUnion(Schema schema) {
        if (schema.getType() != Schema.Type.UNION) return schema;
        return schema.getTypes().stream()
                .filter(s -> s.getType() != Schema.Type.NULL)
                .findFirst()
                .orElse(schema);
    }
}
