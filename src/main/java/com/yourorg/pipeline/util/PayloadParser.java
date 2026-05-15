package com.yourorg.pipeline.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses the {@code id=<IMAGE_NAME>,request=<JSON>} wire format used by
 * human payloads in certain segments.
 *
 * <p>Handles both quoted and unquoted value forms:
 * <ul>
 *   <li>{@code id="name",request="<json>"} — values wrapped in double-quotes</li>
 *   <li>{@code id=name,request=<json>}     — bare values, no surrounding quotes</li>
 * </ul>
 *
 * <p>Escaped quotes ({@code \"}) inside a quoted request value are unescaped
 * after extraction so the result is valid JSON.
 */
public final class PayloadParser {

    // Quoted form: id="<capture1>",request="<capture2>"
    private static final Pattern QUOTED = Pattern.compile(
            "^\\s*id=\"([^\"]+)\",\\s*request=\"(.+)\"\\s*$",
            Pattern.DOTALL);

    // Unquoted form: id=<capture1>,request=<capture2>
    // Image name is everything up to the first comma; request is everything after "request=".
    private static final Pattern UNQUOTED = Pattern.compile(
            "^\\s*id=([^,]+),\\s*request=(.+)\\s*$",
            Pattern.DOTALL);

    private PayloadParser() {}

    public static final class Parsed {
        private final String imageId;
        private final String json;

        public Parsed(String imageId, String json) {
            this.imageId = imageId;
            this.json    = json;
        }

        public String imageId() { return imageId; }
        public String json()    { return json;    }
    }

    /**
     * Parses {@code id=...,request=...} in either quoted or unquoted form.
     *
     * @param raw the raw decrypted string
     * @return parsed result, or {@code null} if the string does not match either pattern
     */
    public static Parsed parse(String raw) {
        if (raw == null || raw.isBlank()) return null;

        Matcher m = QUOTED.matcher(raw);
        if (m.matches()) {
            String imageId = m.group(1).trim();
            String json    = m.group(2).replace("\\\"", "\"");
            return new Parsed(imageId, json);
        }

        m = UNQUOTED.matcher(raw);
        if (m.matches()) {
            String imageId = m.group(1).trim();
            String json    = m.group(2).trim();
            return new Parsed(imageId, json);
        }

        return null;
    }
}
