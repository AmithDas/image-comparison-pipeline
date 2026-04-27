package com.yourorg.pipeline.config;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Configuration for a single pipeline segment.
 *
 * <p>A segment groups payloads that share a pair of {@code method} column values —
 * one for the AI payload and one for the human payload.  All segments are processed
 * by a single Dataflow job; results are written to one shared comparison table with
 * a {@code segment} column that identifies the origin.
 *
 * <h3>JSON format (passed via {@code --segmentConfigs})</h3>
 * <pre>
 * [
 *   {"name": "main",           "aiMethod": "aimetadata",    "humanMethod": "controller.SubmitDispute"},
 *   {"name": "authentication", "aiMethod": "auth.ai",        "humanMethod": "auth.human"},
 *   {"name": "docproof",       "aiMethod": "docproof.ai",    "humanMethod": "docproof.human"}
 * ]
 * </pre>
 *
 * <p>Adding a new segment requires only a DAG configuration change — no Java code changes needed.
 */
public class SegmentConfig implements Serializable {

    public final String name;
    public final String aiMethod;
    public final String humanMethod;

    public SegmentConfig(String name, String aiMethod, String humanMethod) {
        this.name        = name;
        this.aiMethod    = aiMethod;
        this.humanMethod = humanMethod;
    }

    // ── Parsing ───────────────────────────────────────────────────────────────

    /**
     * Parses a JSON array string into a list of {@link SegmentConfig} objects.
     *
     * @param json the JSON array string from {@code --segmentConfigs}
     * @return parsed list of segment configs
     * @throws IllegalArgumentException if JSON is null, blank, or malformed
     */
    public static List<SegmentConfig> parse(String json) {
        if (json == null || json.isBlank()) {
            throw new IllegalArgumentException(
                    "--segmentConfigs must be a non-empty JSON array");
        }
        Type listType = new TypeToken<List<SegmentConfig>>() {}.getType();
        List<SegmentConfig> configs = new Gson().fromJson(json, listType);
        if (configs == null || configs.isEmpty()) {
            throw new IllegalArgumentException(
                    "--segmentConfigs parsed to an empty list — at least one segment is required");
        }
        validate(configs);
        return configs;
    }

    // ── Map builders ──────────────────────────────────────────────────────────

    /**
     * Builds a map of {@code method → "ai" or "human"} from all segments.
     * Used by {@code FilterAndPairFn} to classify each source row's side.
     */
    public static Map<String, String> buildMethodToSideMap(List<SegmentConfig> segments) {
        Map<String, String> map = new HashMap<>();
        for (SegmentConfig s : segments) {
            map.put(s.aiMethod,    "ai");
            map.put(s.humanMethod, "human");
        }
        return map;
    }

    // ── Validation ────────────────────────────────────────────────────────────

    private static void validate(List<SegmentConfig> configs) {
        for (SegmentConfig c : configs) {
            if (blank(c.name)) {
                throw new IllegalArgumentException("SegmentConfig missing 'name'");
            }
            if (blank(c.aiMethod)) {
                throw new IllegalArgumentException(
                        "SegmentConfig '" + c.name + "' missing 'aiMethod'");
            }
            if (blank(c.humanMethod)) {
                throw new IllegalArgumentException(
                        "SegmentConfig '" + c.name + "' missing 'humanMethod'");
            }
        }
    }

    private static boolean blank(String s) {
        return s == null || s.isBlank();
    }

    @Override
    public String toString() {
        return "SegmentConfig{name=" + name
                + ", aiMethod=" + aiMethod
                + ", humanMethod=" + humanMethod + "}";
    }
}
