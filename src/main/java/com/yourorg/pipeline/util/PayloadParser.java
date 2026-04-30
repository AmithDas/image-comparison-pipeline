package com.yourorg.pipeline.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses the {@code id="<IMAGE_NAME>",request="<JSON>"} wire format used by
 * human payloads in certain segments.
 *
 * <p>The outer quotes wrap the values; escaped quotes ({@code \"}) inside the
 * {@code request} value are unescaped after extraction so the result is valid JSON.
 */
public final class PayloadParser {

    // Matches: id="<capture1>",request="<capture2>"
    // The request value is everything between request=" and the final " in the string.
    private static final Pattern ID_REQUEST_PATTERN = Pattern.compile(
            "^\\s*id=\"([^\"]+)\",\\s*request=\"(.+)\"\\s*$",
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
     * Parses the {@code id="...",request="..."} format.
     *
     * @param raw the raw decrypted string
     * @return parsed result, or {@code null} if the string does not match the pattern
     */
    public static Parsed parse(String raw) {
        if (raw == null || raw.isBlank()) return null;
        Matcher m = ID_REQUEST_PATTERN.matcher(raw);
        if (!m.matches()) return null;
        String imageId = m.group(1).trim();
        String json    = m.group(2).replace("\\\"", "\"");
        return new Parsed(imageId, json);
    }
}
