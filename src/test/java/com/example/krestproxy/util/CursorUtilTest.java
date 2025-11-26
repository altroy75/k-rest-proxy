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

    // Multi-topic cursor tests

    @Test
    void createCursor_shouldHandleMultipleTopics() {
        Map<String, Map<Integer, Long>> offsets = new HashMap<>();

        Map<Integer, Long> topic1Offsets = new HashMap<>();
        topic1Offsets.put(0, 100L);
        topic1Offsets.put(1, 200L);
        offsets.put("topic-1", topic1Offsets);

        Map<Integer, Long> topic2Offsets = new HashMap<>();
        topic2Offsets.put(0, 300L);
        offsets.put("topic-2", topic2Offsets);

        String cursor = CursorUtil.createCursor(offsets);

        assertNotNull(cursor);
        assertFalse(cursor.isEmpty());
    }

    @Test
    void parseCursor_shouldHandleMultipleTopicsAndPartitions() {
        Map<String, Map<Integer, Long>> offsets = new HashMap<>();

        Map<Integer, Long> topic1Offsets = new HashMap<>();
        topic1Offsets.put(0, 100L);
        topic1Offsets.put(1, 200L);
        topic1Offsets.put(2, 300L);
        offsets.put("topic-1", topic1Offsets);

        Map<Integer, Long> topic2Offsets = new HashMap<>();
        topic2Offsets.put(0, 400L);
        topic2Offsets.put(1, 500L);
        offsets.put("topic-2", topic2Offsets);

        String cursor = CursorUtil.createCursor(offsets);
        Map<String, Map<Integer, Long>> parsedOffsets = CursorUtil.parseCursor(cursor);

        assertNotNull(parsedOffsets);
        assertEquals(2, parsedOffsets.size());
        assertTrue(parsedOffsets.containsKey("topic-1"));
        assertTrue(parsedOffsets.containsKey("topic-2"));
        assertEquals(3, parsedOffsets.get("topic-1").size());
        assertEquals(2, parsedOffsets.get("topic-2").size());
        assertEquals(100L, parsedOffsets.get("topic-1").get(0));
        assertEquals(200L, parsedOffsets.get("topic-1").get(1));
        assertEquals(300L, parsedOffsets.get("topic-1").get(2));
        assertEquals(400L, parsedOffsets.get("topic-2").get(0));
        assertEquals(500L, parsedOffsets.get("topic-2").get(1));
    }

    // Edge case tests

    @Test
    void createCursor_shouldHandleEmptyOffsetMap() {
        Map<String, Map<Integer, Long>> emptyOffsets = new HashMap<>();
        String cursor = CursorUtil.createCursor(emptyOffsets);

        assertNotNull(cursor);
        assertFalse(cursor.isEmpty());

        // Verify it can be parsed back
        Map<String, Map<Integer, Long>> parsedOffsets = CursorUtil.parseCursor(cursor);
        assertNotNull(parsedOffsets);
        assertTrue(parsedOffsets.isEmpty());
    }

    @Test
    void createCursor_shouldHandleZeroOffsets() {
        Map<String, Map<Integer, Long>> offsets = new HashMap<>();
        Map<Integer, Long> partitionOffsets = new HashMap<>();
        partitionOffsets.put(0, 0L);
        partitionOffsets.put(1, 0L);
        offsets.put("test-topic", partitionOffsets);

        String cursor = CursorUtil.createCursor(offsets);
        Map<String, Map<Integer, Long>> parsedOffsets = CursorUtil.parseCursor(cursor);

        assertNotNull(parsedOffsets);
        assertEquals(0L, parsedOffsets.get("test-topic").get(0));
        assertEquals(0L, parsedOffsets.get("test-topic").get(1));
    }

    @Test
    void createCursor_shouldHandleLargeOffsets() {
        Map<String, Map<Integer, Long>> offsets = new HashMap<>();
        Map<Integer, Long> partitionOffsets = new HashMap<>();
        partitionOffsets.put(0, Long.MAX_VALUE);
        partitionOffsets.put(1, Long.MAX_VALUE - 1);
        offsets.put("test-topic", partitionOffsets);

        String cursor = CursorUtil.createCursor(offsets);
        Map<String, Map<Integer, Long>> parsedOffsets = CursorUtil.parseCursor(cursor);

        assertNotNull(parsedOffsets);
        assertEquals(Long.MAX_VALUE, parsedOffsets.get("test-topic").get(0));
        assertEquals(Long.MAX_VALUE - 1, parsedOffsets.get("test-topic").get(1));
    }

    // Round-trip verification

    @Test
    void cursorRoundTrip_shouldPreserveAllData() {
        Map<String, Map<Integer, Long>> originalOffsets = new HashMap<>();

        // Create complex multi-topic, multi-partition cursor
        Map<Integer, Long> topic1Offsets = new HashMap<>();
        topic1Offsets.put(0, 12345L);
        topic1Offsets.put(1, 67890L);
        topic1Offsets.put(2, 0L);
        originalOffsets.put("topic-one", topic1Offsets);

        Map<Integer, Long> topic2Offsets = new HashMap<>();
        topic2Offsets.put(0, Long.MAX_VALUE);
        originalOffsets.put("topic-two", topic2Offsets);

        Map<Integer, Long> topic3Offsets = new HashMap<>();
        topic3Offsets.put(5, 999999L);
        topic3Offsets.put(10, 123L);
        originalOffsets.put("topic-three", topic3Offsets);

        // Create cursor and parse it back
        String cursor = CursorUtil.createCursor(originalOffsets);
        Map<String, Map<Integer, Long>> parsedOffsets = CursorUtil.parseCursor(cursor);

        // Verify all data is preserved
        assertNotNull(parsedOffsets);
        assertEquals(originalOffsets.size(), parsedOffsets.size());

        for (String topic : originalOffsets.keySet()) {
            assertTrue(parsedOffsets.containsKey(topic), "Topic " + topic + " should be present");
            Map<Integer, Long> originalPartitions = originalOffsets.get(topic);
            Map<Integer, Long> parsedPartitions = parsedOffsets.get(topic);
            assertEquals(originalPartitions.size(), parsedPartitions.size(),
                    "Partition count should match for topic " + topic);

            for (Integer partition : originalPartitions.keySet()) {
                assertEquals(originalPartitions.get(partition), parsedPartitions.get(partition),
                        "Offset should match for topic " + topic + " partition " + partition);
            }
        }
    }

    @Test
    void createCursor_shouldHandleTopicWithEmptyPartitionMap() {
        Map<String, Map<Integer, Long>> offsets = new HashMap<>();
        offsets.put("topic-with-no-partitions", new HashMap<>());

        String cursor = CursorUtil.createCursor(offsets);
        Map<String, Map<Integer, Long>> parsedOffsets = CursorUtil.parseCursor(cursor);

        assertNotNull(parsedOffsets);
        assertTrue(parsedOffsets.containsKey("topic-with-no-partitions"));
        assertTrue(parsedOffsets.get("topic-with-no-partitions").isEmpty());
    }
}
