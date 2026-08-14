package com.yourorg.pipeline.util;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import org.junit.Test;

import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * Verifies that SchemaUtil derives correct BigQuery types from Avro schema files,
 * including the "bqType" property override used for TIMESTAMP fields.
 */
public class SchemaUtilTest {

    // ── comparisonResultsSchema ───────────────────────────────────────────────

    @Test
    public void comparisonResultsTimestampFields() {
        Map<String, TableFieldSchema> fields = fieldMap(SchemaUtil.comparisonResultsSchema());

        assertEquals("TIMESTAMP", fields.get("ai_created_at").getType());
        assertEquals("TIMESTAMP", fields.get("human_created_at").getType());
        assertEquals("TIMESTAMP", fields.get("load_time").getType());
    }

    @Test
    public void comparisonResultsNonTimestampFields() {
        Map<String, TableFieldSchema> fields = fieldMap(SchemaUtil.comparisonResultsSchema());

        assertEquals("STRING",  fields.get("image_id").getType());
        assertEquals("STRING",  fields.get("key_id").getType());
        assertEquals("INTEGER", fields.get("ai_iteration").getType());
        assertEquals("BOOLEAN", fields.get("is_match").getType());
        assertEquals("STRING",  fields.get("human_value").getType());
        assertEquals("STRING",  fields.get("ai_value").getType());
    }

    @Test
    public void comparisonResultsModes() {
        Map<String, TableFieldSchema> fields = fieldMap(SchemaUtil.comparisonResultsSchema());

        assertEquals("REQUIRED", fields.get("image_id").getMode());
        assertEquals("REQUIRED", fields.get("load_time").getMode());
        assertEquals("NULLABLE", fields.get("ai_created_at").getMode());
        assertEquals("NULLABLE", fields.get("human_created_at").getMode());
        assertEquals("NULLABLE", fields.get("human_value").getMode());
    }

    // ── pendingSchema ─────────────────────────────────────────────────────────

    @Test
    public void pendingSchemaTimestampFields() {
        Map<String, TableFieldSchema> fields = fieldMap(SchemaUtil.pendingSchema());

        assertEquals("TIMESTAMP", fields.get("created_at").getType());
        assertEquals("TIMESTAMP", fields.get("first_seen_at").getType());
        assertEquals("TIMESTAMP", fields.get("last_retried_at").getType());
    }

    @Test
    public void pendingSchemaNonTimestampFields() {
        Map<String, TableFieldSchema> fields = fieldMap(SchemaUtil.pendingSchema());

        assertEquals("STRING",  fields.get("image_id").getType());
        assertEquals("STRING",  fields.get("key_id").getType());
        assertEquals("STRING",  fields.get("pending_type").getType());
        assertEquals("INTEGER", fields.get("retry_count").getType());
    }

    // ── deadLetterSchema ──────────────────────────────────────────────────────

    @Test
    public void deadLetterSchemaTimestampFields() {
        Map<String, TableFieldSchema> fields = fieldMap(SchemaUtil.deadLetterSchema());

        assertEquals("TIMESTAMP", fields.get("created_at").getType());
        assertEquals("TIMESTAMP", fields.get("first_seen_at").getType());
        assertEquals("TIMESTAMP", fields.get("aged_out_at").getType());
    }

    @Test
    public void deadLetterSchemaRequiredTimestamp() {
        Map<String, TableFieldSchema> fields = fieldMap(SchemaUtil.deadLetterSchema());

        // aged_out_at is "string" (non-union) in Avro → REQUIRED in BQ
        assertEquals("REQUIRED", fields.get("aged_out_at").getMode());
        // nullable timestamps
        assertEquals("NULLABLE", fields.get("created_at").getMode());
        assertEquals("NULLABLE", fields.get("first_seen_at").getMode());
    }

    // ── Helper ────────────────────────────────────────────────────────────────

    private static Map<String, TableFieldSchema> fieldMap(TableSchema schema) {
        return schema.getFields().stream()
                .collect(Collectors.toMap(TableFieldSchema::getName, f -> f));
    }
}
