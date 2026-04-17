package com.yourorg.pipeline.util;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;

import java.util.List;

/**
 * Centralised BigQuery TableSchema definitions for all tables used by the pipeline.
 */
public final class SchemaUtil {

    private SchemaUtil() {}

    /** Schema for image_comparison_results output table. */
    public static TableSchema comparisonResultsSchema() {
        return new TableSchema().setFields(List.of(
            field("image_id",          "STRING",    "REQUIRED"),
            field("ai_iteration",      "INTEGER",   "REQUIRED"),
            field("ai_created_at",     "TIMESTAMP", "NULLABLE"),
            field("human_created_at",  "TIMESTAMP", "NULLABLE"),
            field("field_name",        "STRING",    "REQUIRED"),
            field("human_value",       "STRING",    "NULLABLE"),
            field("ai_value",          "STRING",    "NULLABLE"),
            field("is_match",          "BOOLEAN",   "REQUIRED"),
            field("compared_at",       "TIMESTAMP", "REQUIRED")
        ));
    }

    /** Schema for pending_comparisons state table. */
    public static TableSchema pendingSchema() {
        return new TableSchema().setFields(List.of(
            field("image_id",        "STRING",    "REQUIRED"),
            field("pending_type",    "STRING",    "REQUIRED"),   // "human" or "ai"
            field("payload",         "STRING",    "REQUIRED"),
            field("created_at",      "TIMESTAMP", "NULLABLE"),
            field("first_seen_at",   "TIMESTAMP", "REQUIRED"),
            field("last_retried_at", "TIMESTAMP", "NULLABLE"),
            field("retry_count",     "INTEGER",   "REQUIRED")
        ));
    }

    /** Schema for dead_letter_comparisons table. */
    public static TableSchema deadLetterSchema() {
        return new TableSchema().setFields(List.of(
            field("image_id",      "STRING",    "REQUIRED"),
            field("pending_type",  "STRING",    "REQUIRED"),
            field("payload",       "STRING",    "NULLABLE"),
            field("created_at",    "TIMESTAMP", "NULLABLE"),
            field("first_seen_at", "TIMESTAMP", "NULLABLE"),
            field("aged_out_at",   "TIMESTAMP", "REQUIRED"),
            field("reason",        "STRING",    "NULLABLE")
        ));
    }

    private static TableFieldSchema field(String name, String type, String mode) {
        return new TableFieldSchema()
                .setName(name)
                .setType(type)
                .setMode(mode);
    }
}
