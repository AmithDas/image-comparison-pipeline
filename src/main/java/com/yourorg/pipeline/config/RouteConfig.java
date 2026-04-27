package com.yourorg.pipeline.config;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Holds the configuration for a single pipeline route.
 *
 * <p>A route groups payloads that share the same {@code method} values in the
 * source table and writes their comparison rows to a dedicated BigQuery table.
 *
 * <h3>JSON format (passed via {@code --routeConfigs})</h3>
 * <pre>
 * [
 *   {
 *     "route":       "main",
 *     "aiMethod":    "aimetadata",
 *     "humanMethod": "controller.SubmitDispute",
 *     "outputTable": "project:dataset.main_results"
 *   },
 *   {
 *     "route":       "authentication",
 *     "aiMethod":    "auth.ai",
 *     "humanMethod": "auth.human",
 *     "outputTable": "project:dataset.auth_results"
 *   },
 *   {
 *     "route":       "docproof",
 *     "aiMethod":    "docproof.ai",
 *     "humanMethod": "docproof.human",
 *     "outputTable": "project:dataset.docproof_results"
 *   }
 * ]
 * </pre>
 *
 * <p>Adding a new route requires only a DAG change — no Java code changes needed.
 */
public class RouteConfig implements Serializable {

    public final String route;
    public final String aiMethod;
    public final String humanMethod;
    public final String outputTable;

    public RouteConfig(String route, String aiMethod,
                       String humanMethod, String outputTable) {
        this.route       = route;
        this.aiMethod    = aiMethod;
        this.humanMethod = humanMethod;
        this.outputTable = outputTable;
    }

    // ── Parsing ───────────────────────────────────────────────────────────────

    /**
     * Parses a JSON array string into a list of {@link RouteConfig} objects.
     *
     * @param json the JSON array string from {@code --routeConfigs}
     * @return parsed list of route configs
     * @throws IllegalArgumentException if JSON is null, blank, or malformed
     */
    public static List<RouteConfig> parse(String json) {
        if (json == null || json.isBlank()) {
            throw new IllegalArgumentException(
                    "--routeConfigs must be a non-empty JSON array");
        }
        Type listType = new TypeToken<List<RouteConfig>>() {}.getType();
        List<RouteConfig> configs = new Gson().fromJson(json, listType);
        if (configs == null || configs.isEmpty()) {
            throw new IllegalArgumentException(
                    "--routeConfigs parsed to an empty list — at least one route is required");
        }
        validate(configs);
        return configs;
    }

    /**
     * Builds a map of {@code method → "ai" or "human"} from all routes.
     * Used by {@code FilterAndPairFn} to identify which side each source row belongs to.
     *
     * @param routes list of route configs
     * @return map of method name → side ("ai" or "human")
     */
    public static Map<String, String> buildMethodToSideMap(List<RouteConfig> routes) {
        Map<String, String> map = new HashMap<>();
        for (RouteConfig r : routes) {
            map.put(r.aiMethod,    "ai");
            map.put(r.humanMethod, "human");
        }
        return map;
    }

    // ── Validation ────────────────────────────────────────────────────────────

    private static void validate(List<RouteConfig> configs) {
        for (RouteConfig c : configs) {
            if (blank(c.route)) {
                throw new IllegalArgumentException("RouteConfig missing 'route'");
            }
            if (blank(c.aiMethod)) {
                throw new IllegalArgumentException(
                        "RouteConfig '" + c.route + "' missing 'aiMethod'");
            }
            if (blank(c.humanMethod)) {
                throw new IllegalArgumentException(
                        "RouteConfig '" + c.route + "' missing 'humanMethod'");
            }
            if (blank(c.outputTable)) {
                throw new IllegalArgumentException(
                        "RouteConfig '" + c.route + "' missing 'outputTable'");
            }
        }
    }

    private static boolean blank(String s) {
        return s == null || s.isBlank();
    }

    @Override
    public String toString() {
        return "RouteConfig{route=" + route
                + ", aiMethod=" + aiMethod
                + ", humanMethod=" + humanMethod
                + ", outputTable=" + outputTable + "}";
    }
}
