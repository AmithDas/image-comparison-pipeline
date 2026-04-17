package com.yourorg.pipeline.util;

import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryAvroUtils;

/**
 * Derives BigQuery TableSchema definitions from the Avro schemas in AvroSchemas.
 * Adding or renaming a field only requires updating the corresponding .json schema file.
 */
public final class SchemaUtil {

    private SchemaUtil() {}

    public static TableSchema comparisonResultsSchema() {
        return BigQueryAvroUtils.toTableSchema(AvroSchemas.COMPARISON_RESULT);
    }

    public static TableSchema pendingSchema() {
        return BigQueryAvroUtils.toTableSchema(AvroSchemas.PENDING_ROW);
    }

    public static TableSchema deadLetterSchema() {
        return BigQueryAvroUtils.toTableSchema(AvroSchemas.DEAD_LETTER_ROW);
    }
}
