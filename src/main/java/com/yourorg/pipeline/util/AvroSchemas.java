package com.yourorg.pipeline.util;

import org.apache.avro.Schema;

import java.io.IOException;
import java.io.InputStream;

/**
 * Loads and exposes Avro schemas from JSON files on the classpath.
 * Schemas are parsed once at class-load time.
 */
public final class AvroSchemas {

    private AvroSchemas() {}

    public static final Schema PAYLOAD_ROW;
    public static final Schema PENDING_ROW;

    static {
        try {
            PAYLOAD_ROW = parse("/avro/payload_row.json");
            PENDING_ROW = parse("/avro/pending_row.json");
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static Schema parse(String resource) throws IOException {
        try (InputStream in = AvroSchemas.class.getResourceAsStream(resource)) {
            if (in == null) {
                throw new IOException("Avro schema not found on classpath: " + resource);
            }
            return new Schema.Parser().parse(in);
        }
    }
}
