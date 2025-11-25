package com.example.krestproxy.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

public class CursorUtil {

    private static final Logger logger = LoggerFactory.getLogger(CursorUtil.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static String createCursor(Map<Integer, Long> offsets) {
        try {
            String json = objectMapper.writeValueAsString(offsets);
            return Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            logger.error("Error creating cursor", e);
            return null;
        }
    }

    public static Map<Integer, Long> parseCursor(String cursor) {
        if (cursor == null || cursor.isEmpty()) {
            return null;
        }
        try {
            String json = new String(Base64.getDecoder().decode(cursor), StandardCharsets.UTF_8);
            return objectMapper.readValue(json, new TypeReference<Map<Integer, Long>>() {
            });
        } catch (Exception e) {
            logger.error("Error parsing cursor", e);
            return null;
        }
    }
}
