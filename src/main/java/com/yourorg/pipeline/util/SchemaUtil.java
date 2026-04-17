package com.yourorg.pipeline.util;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.avro.Schema;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Derives BigQuery TableSchema definitions from the Avro schemas in AvroSchemas.
 * Adding or renaming a field only requires updating the corresponding .json schema file.
 */
public final class SchemaUtil {

    private SchemaUtil() {}

    public static TableSchema comparisonResultsSchema() {
        return toTableSchema(AvroSchemas.COMPARISON_RESULT);
    }

    public static TableSchema pendingSchema() {
        return toTableSchema(AvroSchemas.PENDING_ROW);
    }

    public static TableSchema deadLetterSchema() {
        return toTableSchema(AvroSchemas.DEAD_LETTER_ROW);
    }

    // ── Avro → BigQuery conversion ────────────────────────────────────────────

    private static TableSchema toTableSchema(Schema avroSchema) {
        List<TableFieldSchema> fields = avroSchema.getFields().stream()
                .map(SchemaUtil::toTableField)
                .collect(Collectors.toList());
        return new TableSchema().setFields(fields);
    }

    private static TableFieldSchema toTableField(Schema.Field field) {
        Schema schema = field.schema();
        String mode  = "REQUIRED";

        if (schema.getType() == Schema.Type.UNION) {
            List<Schema> nonNull = schema.getTypes().stream()
                    .filter(s -> s.getType() != Schema.Type.NULL)
                    .collect(Collectors.toList());
            if (!nonNull.isEmpty()) {
                schema = nonNull.get(0);
                mode   = "NULLABLE";
            }
        }

        return new TableFieldSchema()
                .setName(field.name())
                .setType(toBqType(schema))
                .setMode(mode);
    }

    private static String toBqType(Schema schema) {
        switch (schema.getType()) {
            case BOOLEAN: return "BOOLEAN";
            case INT:
            case LONG:    return "INTEGER";
            case FLOAT:
            case DOUBLE:  return "FLOAT";
            case BYTES:   return "BYTES";
            default:      return "STRING";
        }
    }
}
