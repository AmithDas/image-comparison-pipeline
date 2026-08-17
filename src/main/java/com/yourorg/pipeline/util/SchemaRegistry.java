package com.yourorg.pipeline.util;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Reusable Avro schema registry that loads schemas lazily from the classpath
 * and caches them by name. Designed to be shared across multiple pipelines.
 *
 * <h3>Default instance</h3>
 * The singleton returned by {@link #getInstance()} loads schemas from
 * {@code /avro/<name>.json} on the classpath. Schema names for this project are
 * declared as constants ({@link #PAYLOAD_ROW}, {@link #PENDING_ROW}, etc.).
 *
 * <pre>
 *   Schema payload = SchemaRegistry.getInstance().get(SchemaRegistry.PAYLOAD_ROW);
 * </pre>
 *
 * <h3>Custom instances</h3>
 * Construct a fresh instance with a different resource prefix/suffix to point at
 * a different classpath location, or use {@link #register} to inject schemas
 * programmatically (useful for tests or pipelines that define schemas in code):
 *
 * <pre>
 *   SchemaRegistry registry = new SchemaRegistry("/schemas/", ".avsc");
 *   registry.register("my_type", mySchema);
 * </pre>
 *
 * <h3>Thread safety</h3>
 * All methods are thread-safe. Schemas are loaded at most once per name per
 * instance (using {@link ConcurrentHashMap#computeIfAbsent}).
 */
public final class SchemaRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistry.class);

    // ── Well-known schema names for this project ──────────────────────────────

    public static final String PAYLOAD_ROW      = "payload_row";
    public static final String PENDING_ROW      = "pending_row";
    public static final String AI_PENDING_ROW   = "ai_pending_row";
    public static final String COMPARISON_RESULT = "comparison_result";
    public static final String DEAD_LETTER_ROW  = "dead_letter_row";

    // ── Default singleton ─────────────────────────────────────────────────────

    private static final SchemaRegistry INSTANCE = new SchemaRegistry("/avro/", ".json");

    /** Returns the shared default registry (loads from {@code /avro/<name>.json}). */
    public static SchemaRegistry getInstance() {
        return INSTANCE;
    }

    // ── Instance state ────────────────────────────────────────────────────────

    private final String resourcePrefix;
    private final String resourceSuffix;
    private final Map<String, Schema> cache = new ConcurrentHashMap<>();

    /**
     * Creates a registry that resolves schema names to classpath resources of the
     * form {@code <resourcePrefix><name><resourceSuffix>}.
     *
     * @param resourcePrefix classpath path prefix, e.g. {@code "/avro/"}
     * @param resourceSuffix file extension, e.g. {@code ".json"} or {@code ".avsc"}
     */
    public SchemaRegistry(String resourcePrefix, String resourceSuffix) {
        this.resourcePrefix = resourcePrefix;
        this.resourceSuffix = resourceSuffix;
    }

    // ── Public API ────────────────────────────────────────────────────────────

    /**
     * Returns the {@link Schema} for {@code name}, loading and caching it on first
     * access.
     *
     * @param name schema name (e.g. {@link #PAYLOAD_ROW})
     * @throws IllegalArgumentException if no classpath resource exists for {@code name}
     * @throws RuntimeException         if the resource cannot be parsed as Avro JSON
     */
    public Schema get(String name) {
        return cache.computeIfAbsent(name, this::load);
    }

    /**
     * Registers {@code schema} under {@code name}, replacing any previously cached
     * entry. Useful for programmatically-defined schemas or test overrides.
     *
     * @return {@code this} for chaining
     */
    public SchemaRegistry register(String name, Schema schema) {
        cache.put(name, schema);
        LOG.debug("Registered schema '{}'", name);
        return this;
    }

    // ── Loading ───────────────────────────────────────────────────────────────

    private Schema load(String name) {
        String resource = resourcePrefix + name + resourceSuffix;
        LOG.debug("Loading Avro schema '{}' from classpath: {}", name, resource);
        try (InputStream in = SchemaRegistry.class.getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalArgumentException(
                        "Avro schema '" + name + "' not found on classpath at: " + resource);
            }
            return new Schema.Parser().parse(in);
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse Avro schema: " + resource, e);
        }
    }
}
