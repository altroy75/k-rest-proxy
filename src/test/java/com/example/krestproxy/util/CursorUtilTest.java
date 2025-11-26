package com.example.krestproxy.util;

import org.junit.jupiter.api.Test;
import java.util.HashMap;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class CursorUtilTest {

    @Test
    void createCursor_shouldReturnBase64String_whenOffsetsAreValid() {
        Map<String, Map<Integer, Long>> offsets = new HashMap<>();
        Map<Integer, Long> partitionOffsets = new HashMap<>();
        partitionOffsets.put(0, 100L);
        offsets.put("test-topic", partitionOffsets);

        String cursor = CursorUtil.createCursor(offsets);

        assertNotNull(cursor);
        assertFalse(cursor.isEmpty());
    }

    @Test
    void createCursor_shouldReturnNull_whenOffsetsAreNull() {
        String cursor = CursorUtil.createCursor(null);
        assertNull(cursor); // Or handle gracefully if implementation changes, currently it catches
                            // exception and returns null
    }

    @Test
    void parseCursor_shouldReturnOffsets_whenCursorIsValid() {
        Map<String, Map<Integer, Long>> offsets = new HashMap<>();
        Map<Integer, Long> partitionOffsets = new HashMap<>();
        partitionOffsets.put(0, 100L);
        offsets.put("test-topic", partitionOffsets);
        String cursor = CursorUtil.createCursor(offsets);

        Map<String, Map<Integer, Long>> parsedOffsets = CursorUtil.parseCursor(cursor);

        assertNotNull(parsedOffsets);
        assertEquals(1, parsedOffsets.size());
        assertTrue(parsedOffsets.containsKey("test-topic"));
        assertEquals(100L, parsedOffsets.get("test-topic").get(0));
    }

    @Test
    void parseCursor_shouldReturnNull_whenCursorIsNull() {
        Map<String, Map<Integer, Long>> parsedOffsets = CursorUtil.parseCursor(null);
        assertNull(parsedOffsets);
    }

    @Test
    void parseCursor_shouldReturnNull_whenCursorIsEmpty() {
        Map<String, Map<Integer, Long>> parsedOffsets = CursorUtil.parseCursor("");
        assertNull(parsedOffsets);
    }

    @Test
    void parseCursor_shouldReturnNull_whenCursorIsInvalidBase64() {
        String invalidCursor = "not-base-64!";
        Map<String, Map<Integer, Long>> parsedOffsets = CursorUtil.parseCursor(invalidCursor);
        assertNull(parsedOffsets);
    }

    @Test
    void parseCursor_shouldReturnNull_whenCursorIsValidBase64ButInvalidJson() {
        String invalidJsonCursor = java.util.Base64.getEncoder().encodeToString("not-json".getBytes());
        Map<String, Map<Integer, Long>> parsedOffsets = CursorUtil.parseCursor(invalidJsonCursor);
        assertNull(parsedOffsets);
    }
}
