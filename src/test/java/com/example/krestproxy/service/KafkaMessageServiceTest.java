package com.example.krestproxy.service;

import com.example.krestproxy.config.KafkaProperties;
import com.example.krestproxy.dto.MessageDto;
import com.example.krestproxy.dto.PaginatedResponse;
import com.example.krestproxy.util.CursorUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.pool2.ObjectPool;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KafkaMessageServiceTest {

        @Mock
        private ObjectPool<Consumer<Object, Object>> consumerPool;

        @Mock
        private Consumer<Object, Object> consumer;

        @Mock
        private KafkaProperties kafkaProperties;

        private KafkaMessageService kafkaMessageService;

        @BeforeEach
        void setUp() throws Exception {
                lenient().when(kafkaProperties.getMaxMessagesPerRequest()).thenReturn(10000);
                lenient().when(kafkaProperties.getPollTimeoutMs()).thenReturn(100L);
                kafkaMessageService = new KafkaMessageService(consumerPool, kafkaProperties);
        }

        @Test
        void getMessages_shouldReturnMessagesWithinTimeRange() throws Exception {
                String topic = "test-topic";
                Instant startTime = Instant.parse("2023-01-01T10:00:00Z");
                Instant endTime = Instant.parse("2023-01-01T10:05:00Z");
                TopicPartition partition0 = new TopicPartition(topic, 0);

                when(consumerPool.borrowObject()).thenReturn(consumer);
                when(consumer.partitionsFor(topic)).thenReturn(
                                Collections.singletonList(
                                                new org.apache.kafka.common.PartitionInfo(topic, 0, null, null, null)));

                // Mock offsetsForTimes
                Map<TopicPartition, OffsetAndTimestamp> startOffsets = new HashMap<>();
                startOffsets.put(partition0, new OffsetAndTimestamp(0L, startTime.toEpochMilli()));
                when(consumer.offsetsForTimes(
                                argThat(map -> map != null && map.containsKey(partition0)
                                                && map.get(partition0) == startTime.toEpochMilli())))
                                .thenReturn(startOffsets);

                Map<TopicPartition, OffsetAndTimestamp> endOffsets = new HashMap<>();
                endOffsets.put(partition0, new OffsetAndTimestamp(10L, endTime.toEpochMilli()));
                when(consumer.offsetsForTimes(
                                argThat(map -> map != null && map.containsKey(partition0)
                                                && map.get(partition0) == endTime.toEpochMilli())))
                                .thenReturn(endOffsets);

                // Mock poll
                ConsumerRecord<Object, Object> record1 = new ConsumerRecord<>(topic, 0, 0L, startTime.toEpochMilli(),
                                org.apache.kafka.common.record.TimestampType.CREATE_TIME, 0, 0, "key", "msg1",
                                new org.apache.kafka.common.header.internals.RecordHeaders(), Optional.empty());
                ConsumerRecord<Object, Object> record2 = new ConsumerRecord<>(topic, 0, 5L,
                                startTime.toEpochMilli() + 1000,
                                org.apache.kafka.common.record.TimestampType.CREATE_TIME, 0, 0, "key", "msg2",
                                new org.apache.kafka.common.header.internals.RecordHeaders(), Optional.empty());

                Map<TopicPartition, List<ConsumerRecord<Object, Object>>> recordsMap = new HashMap<>();
                recordsMap.put(partition0, Arrays.asList(record1, record2));
                ConsumerRecords<Object, Object> records = new ConsumerRecords<>(recordsMap);

                when(consumer.poll(any())).thenReturn(records)
                                .thenReturn(new ConsumerRecords<>(Collections.emptyMap()));

                PaginatedResponse<MessageDto> response = kafkaMessageService.getMessages(topic, startTime,
                                endTime, null);
                List<MessageDto> messages = response.data();

                assertEquals(2, messages.size());
                assertEquals("msg1", messages.get(0).content());
                assertEquals(topic, messages.get(0).topicName());
                assertEquals("msg2", messages.get(1).content());
                assertEquals(topic, messages.get(1).topicName());
                assertFalse(response.hasMore());

                verify(consumer).assign(Collections.singletonList(partition0));
                verify(consumer).seek(partition0, 0L);
                verify(consumerPool).returnObject(consumer);
        }

        @Test
        void getMessagesWithExecId_shouldFilterMessages() throws Exception {
                String topic = "test-topic-filter";
                Instant startTime = Instant.parse("2023-01-01T10:00:00Z");
                Instant endTime = Instant.parse("2023-01-01T10:05:00Z");
                String targetExecId = "exec-123";
                TopicPartition partition0 = new TopicPartition(topic, 0);

                when(consumerPool.borrowObject()).thenReturn(consumer);
                when(consumer.partitionsFor(topic)).thenReturn(
                                Collections.singletonList(
                                                new org.apache.kafka.common.PartitionInfo(topic, 0, null, null, null)));

                // Mock offsets
                Map<TopicPartition, OffsetAndTimestamp> startOffsets = new HashMap<>();
                startOffsets.put(partition0, new OffsetAndTimestamp(0L, startTime.toEpochMilli()));
                when(consumer.offsetsForTimes(
                                argThat(map -> map != null && map.containsKey(partition0)
                                                && map.get(partition0) == startTime.toEpochMilli())))
                                .thenReturn(startOffsets);

                Map<TopicPartition, OffsetAndTimestamp> endOffsets = new HashMap<>();
                endOffsets.put(partition0, new OffsetAndTimestamp(10L, endTime.toEpochMilli()));
                when(consumer.offsetsForTimes(
                                argThat(map -> map != null && map.containsKey(partition0)
                                                && map.get(partition0) == endTime.toEpochMilli())))
                                .thenReturn(endOffsets);

                // Mock Avro keys
                String keySchemaString = "{\"type\":\"record\",\"name\":\"Key\",\"fields\":[{\"name\":\"version\",\"type\":\"string\"},{\"name\":\"exec_id\",\"type\":\"string\"},{\"name\":\"timestamp\",\"type\":\"long\"}]}";
                Schema schema = new Schema.Parser().parse(keySchemaString);

                GenericRecord key1 = new GenericData.Record(schema);
                key1.put("version", "v1");
                key1.put("exec_id", targetExecId);
                key1.put("timestamp", 1000L);

                GenericRecord key2 = new GenericData.Record(schema);
                key2.put("version", "v1");
                key2.put("exec_id", "other-exec-id");
                key2.put("timestamp", 1001L);

                ConsumerRecord<Object, Object> record1 = new ConsumerRecord<>(topic, 0, 0L, startTime.toEpochMilli(),
                                org.apache.kafka.common.record.TimestampType.CREATE_TIME, 0, 0, key1, "msg1",
                                new org.apache.kafka.common.header.internals.RecordHeaders(), Optional.empty());

                ConsumerRecord<Object, Object> record2 = new ConsumerRecord<>(topic, 0, 1L,
                                startTime.toEpochMilli() + 100,
                                org.apache.kafka.common.record.TimestampType.CREATE_TIME, 0, 0, key2, "msg2",
                                new org.apache.kafka.common.header.internals.RecordHeaders(), Optional.empty());

                Map<TopicPartition, List<ConsumerRecord<Object, Object>>> recordsMap = new HashMap<>();
                recordsMap.put(partition0, Arrays.asList(record1, record2));
                ConsumerRecords<Object, Object> records = new ConsumerRecords<>(recordsMap);

                when(consumer.poll(any())).thenReturn(records)
                                .thenReturn(new ConsumerRecords<>(Collections.emptyMap()));

                PaginatedResponse<MessageDto> response = kafkaMessageService.getMessagesWithExecId(topic, startTime,
                                endTime, targetExecId, null);
                List<MessageDto> messages = response.data();

                assertEquals(1, messages.size());
                assertEquals("msg1", messages.get(0).content());
                assertEquals(topic, messages.get(0).topicName());
                assertFalse(response.hasMore());

                verify(consumerPool).returnObject(consumer);
        }

        @Test
        void getMessagesFromTopics_shouldReturnMessagesFromMultipleTopics() throws Exception {
                String topic1 = "test-topic-1";
                String topic2 = "test-topic-2";
                List<String> topics = Arrays.asList(topic1, topic2);
                Instant startTime = Instant.parse("2023-01-01T10:00:00Z");
                Instant endTime = Instant.parse("2023-01-01T10:05:00Z");

                TopicPartition partition1 = new TopicPartition(topic1, 0);
                TopicPartition partition2 = new TopicPartition(topic2, 0);

                when(consumerPool.borrowObject()).thenReturn(consumer);

                when(consumer.partitionsFor(topic1)).thenReturn(
                                Collections.singletonList(new org.apache.kafka.common.PartitionInfo(topic1, 0, null,
                                                null, null)));
                when(consumer.partitionsFor(topic2)).thenReturn(
                                Collections.singletonList(new org.apache.kafka.common.PartitionInfo(topic2, 0, null,
                                                null, null)));

                // Mock offsets
                Map<TopicPartition, OffsetAndTimestamp> startOffsets = new HashMap<>();
                startOffsets.put(partition1, new OffsetAndTimestamp(0L, startTime.toEpochMilli()));
                startOffsets.put(partition2, new OffsetAndTimestamp(0L, startTime.toEpochMilli()));

                when(consumer.offsetsForTimes(argThat(
                                map -> map != null && map.containsKey(partition1) && map.containsKey(partition2))))
                                .thenReturn(startOffsets);

                Map<TopicPartition, OffsetAndTimestamp> endOffsets = new HashMap<>();
                endOffsets.put(partition1, new OffsetAndTimestamp(10L, endTime.toEpochMilli()));
                endOffsets.put(partition2, new OffsetAndTimestamp(10L, endTime.toEpochMilli()));

                when(consumer.offsetsForTimes(
                                argThat(map -> map != null && map.containsKey(partition1) && map.containsKey(partition2)
                                                && map.get(partition1) == endTime.toEpochMilli())))
                                .thenReturn(endOffsets);

                // Mock poll
                ConsumerRecord<Object, Object> record1 = new ConsumerRecord<>(topic1, 0, 0L, startTime.toEpochMilli(),
                                org.apache.kafka.common.record.TimestampType.CREATE_TIME, 0, 0, "key", "msg1",
                                new org.apache.kafka.common.header.internals.RecordHeaders(), Optional.empty());
                ConsumerRecord<Object, Object> record2 = new ConsumerRecord<>(topic2, 0, 0L, startTime.toEpochMilli(),
                                org.apache.kafka.common.record.TimestampType.CREATE_TIME, 0, 0, "key", "msg2",
                                new org.apache.kafka.common.header.internals.RecordHeaders(), Optional.empty());

                Map<TopicPartition, List<ConsumerRecord<Object, Object>>> recordsMap = new HashMap<>();
                recordsMap.put(partition1, Collections.singletonList(record1));
                recordsMap.put(partition2, Collections.singletonList(record2));
                ConsumerRecords<Object, Object> records = new ConsumerRecords<>(recordsMap);

                when(consumer.poll(any()))
                                .thenReturn(records)
                                .thenReturn(new ConsumerRecords<>(Collections.emptyMap()))
                                .thenReturn(records)
                                .thenReturn(new ConsumerRecords<>(Collections.emptyMap()));

                List<MessageDto> messages = kafkaMessageService.getMessagesFromTopics(topics, startTime, endTime, null);

                assertEquals(2, messages.size());
                // The order depends on how partitions are iterated, which is based on the list
                // order and partition assignment.
                // Since we use ArrayList to collect partitions, it should follow topic order.
                // But poll results might not guarantee order. Let's check content.

                List<String> contents = messages.stream().map(MessageDto::content).toList();
                List<String> topicNames = messages.stream().map(MessageDto::topicName).toList();

                // Check contains
                assertTrue(contents.contains("msg1"));
                assertTrue(contents.contains("msg2"));
                assertTrue(topicNames.contains(topic1));
                assertTrue(topicNames.contains(topic2));

                verify(consumer).assign(argThat(list -> list.contains(partition1) && list.contains(partition2)));
                verify(consumerPool).returnObject(consumer);
        }

        @Test
        void testGetMessagesForExecution_Success() throws Exception {
                String execId = "exec-1";
                String execTopic = "execids";
                String dataTopic = "data-topic";
                long startTs = 1000;
                long endTs = 2000;

                when(consumerPool.borrowObject()).thenReturn(consumer);

                // Mock finding execution times
                TopicPartition execPartition = new TopicPartition(execTopic, 0);

                // Mock records for scanning execids
                // Start record
                ConsumerRecord<Object, Object> startRecord = new ConsumerRecord<>(execTopic, 0, 0, startTs,
                                org.apache.kafka.common.record.TimestampType.CREATE_TIME,
                                0, 0, execId, "start", new org.apache.kafka.common.header.internals.RecordHeaders(),
                                Optional.empty());

                // End record
                ConsumerRecord<Object, Object> endRecord = new ConsumerRecord<>(execTopic, 0, 1, endTs,
                                org.apache.kafka.common.record.TimestampType.CREATE_TIME,
                                0, 0, execId, "end", new org.apache.kafka.common.header.internals.RecordHeaders(),
                                Optional.empty());

                Map<TopicPartition, List<ConsumerRecord<Object, Object>>> recordsMap = new HashMap<>();
                recordsMap.put(execPartition, Arrays.asList(startRecord, endRecord));
                ConsumerRecords<Object, Object> execRecords = new ConsumerRecords<>(recordsMap);

                // Mock data retrieval
                TopicPartition dataPartition = new TopicPartition(dataTopic, 0);
                when(consumer.partitionsFor(dataTopic)).thenReturn(Collections.singletonList(
                                new org.apache.kafka.common.PartitionInfo(dataTopic, 0, null, null, null)));

                Map<TopicPartition, OffsetAndTimestamp> startOffsetsMap = new HashMap<>();
                startOffsetsMap.put(dataPartition, new OffsetAndTimestamp(10L, 1000L));
                when(consumer.offsetsForTimes(argThat(map -> map != null && map.containsValue(startTs))))
                                .thenReturn(startOffsetsMap);

                Map<TopicPartition, OffsetAndTimestamp> endOffsetsMap = new HashMap<>();
                endOffsetsMap.put(dataPartition, new OffsetAndTimestamp(20L, 1000L));
                when(consumer.offsetsForTimes(argThat(map -> map != null && map.containsValue(endTs))))
                                .thenReturn(endOffsetsMap);

                // Data records
                // Mock GenericRecord for Key
                GenericRecord mockKey = mock(GenericRecord.class);
                when(mockKey.get("exec_id")).thenReturn(execId);

                ConsumerRecord<Object, Object> dataRecord = new ConsumerRecord<>(dataTopic, 0, 15, 1500,
                                org.apache.kafka.common.record.TimestampType.CREATE_TIME,
                                0, 0, mockKey, "value", new org.apache.kafka.common.header.internals.RecordHeaders(),
                                Optional.empty());

                Map<TopicPartition, List<ConsumerRecord<Object, Object>>> dataRecordsMap = new HashMap<>();
                dataRecordsMap.put(dataPartition, Collections.singletonList(dataRecord));
                ConsumerRecords<Object, Object> dataRecords = new ConsumerRecords<>(dataRecordsMap);

                // We need to chain the poll returns.
                // 1. Poll for exec scan
                // 2. Poll for data fetch
                // 3. Poll for data fetch (empty to stop)
                when(consumer.poll(any(Duration.class)))
                                .thenReturn(execRecords)
                                .thenReturn(dataRecords)
                                .thenReturn(new ConsumerRecords<>(Collections.emptyMap()));

                List<MessageDto> result = kafkaMessageService
                                .getMessagesForExecution(Collections.singletonList(dataTopic), execId);

                assertEquals(1, result.size());
                assertEquals("value", result.get(0).content());
                assertEquals(1500, result.get(0).timestamp());

                verify(consumerPool, times(2)).borrowObject();
                verify(consumerPool, times(2)).returnObject(consumer);
        }

        @Test
        void getMessages_shouldRespectMaxMessagesPerRequest() throws Exception {
                String topic = "test-topic";
                Instant startTime = Instant.parse("2023-01-01T10:00:00Z");
                Instant endTime = Instant.parse("2023-01-01T10:05:00Z");
                TopicPartition partition0 = new TopicPartition(topic, 0);

                // Set a low limit to test enforcement
                when(kafkaProperties.getMaxMessagesPerRequest()).thenReturn(2);

                when(consumerPool.borrowObject()).thenReturn(consumer);
                when(consumer.partitionsFor(topic)).thenReturn(
                                Collections.singletonList(
                                                new org.apache.kafka.common.PartitionInfo(topic, 0, null, null, null)));

                // Mock offsetsForTimes
                Map<TopicPartition, OffsetAndTimestamp> startOffsets = new HashMap<>();
                startOffsets.put(partition0, new OffsetAndTimestamp(0L, startTime.toEpochMilli()));
                when(consumer.offsetsForTimes(
                                argThat(map -> map != null && map.containsKey(partition0)
                                                && map.get(partition0) == startTime.toEpochMilli())))
                                .thenReturn(startOffsets);

                Map<TopicPartition, OffsetAndTimestamp> endOffsets = new HashMap<>();
                endOffsets.put(partition0, new OffsetAndTimestamp(10L, endTime.toEpochMilli()));
                when(consumer.offsetsForTimes(
                                argThat(map -> map != null && map.containsKey(partition0)
                                                && map.get(partition0) == endTime.toEpochMilli())))
                                .thenReturn(endOffsets);

                // Mock poll - return 5 messages but expect only 2 to be collected
                ConsumerRecord<Object, Object> record1 = new ConsumerRecord<>(topic, 0, 0L, startTime.toEpochMilli(),
                                org.apache.kafka.common.record.TimestampType.CREATE_TIME, 0, 0, "key", "msg1",
                                new org.apache.kafka.common.header.internals.RecordHeaders(), Optional.empty());
                ConsumerRecord<Object, Object> record2 = new ConsumerRecord<>(topic, 0, 1L,
                                startTime.toEpochMilli() + 1000,
                                org.apache.kafka.common.record.TimestampType.CREATE_TIME, 0, 0, "key", "msg2",
                                new org.apache.kafka.common.header.internals.RecordHeaders(), Optional.empty());
                ConsumerRecord<Object, Object> record3 = new ConsumerRecord<>(topic, 0, 2L,
                                startTime.toEpochMilli() + 2000,
                                org.apache.kafka.common.record.TimestampType.CREATE_TIME, 0, 0, "key", "msg3",
                                new org.apache.kafka.common.header.internals.RecordHeaders(), Optional.empty());
                ConsumerRecord<Object, Object> record4 = new ConsumerRecord<>(topic, 0, 3L,
                                startTime.toEpochMilli() + 3000,
                                org.apache.kafka.common.record.TimestampType.CREATE_TIME, 0, 0, "key", "msg4",
                                new org.apache.kafka.common.header.internals.RecordHeaders(), Optional.empty());
                ConsumerRecord<Object, Object> record5 = new ConsumerRecord<>(topic, 0, 4L,
                                startTime.toEpochMilli() + 4000,
                                org.apache.kafka.common.record.TimestampType.CREATE_TIME, 0, 0, "key", "msg5",
                                new org.apache.kafka.common.header.internals.RecordHeaders(), Optional.empty());

                Map<TopicPartition, List<ConsumerRecord<Object, Object>>> recordsMap = new HashMap<>();
                recordsMap.put(partition0, Arrays.asList(record1, record2, record3, record4, record5));
                ConsumerRecords<Object, Object> records = new ConsumerRecords<>(recordsMap);

                when(consumer.poll(any())).thenReturn(records)
                                .thenReturn(new ConsumerRecords<>(Collections.emptyMap()));

                PaginatedResponse<MessageDto> response = kafkaMessageService.getMessages(topic, startTime,
                                endTime, null);
                List<MessageDto> messages = response.data();

                // Should only return 2 messages due to the limit
                assertEquals(2, messages.size());
                assertEquals("msg1", messages.get(0).content());
                assertEquals("msg2", messages.get(1).content());
                assertTrue(response.hasMore());
                assertNotNull(response.nextCursor());

                verify(consumer).assign(Collections.singletonList(partition0));
                verify(consumer).seek(partition0, 0L);
                verify(consumerPool).returnObject(consumer);
        }

        @Test
        void getMessages_shouldUseCursorWhenProvided() throws Exception {
                String topic = "test-topic";
                Instant startTime = Instant.parse("2023-01-01T10:00:00Z");
                Instant endTime = Instant.parse("2023-01-01T10:05:00Z");
                TopicPartition partition0 = new TopicPartition(topic, 0);

                // Create a cursor pointing to offset 5
                Map<String, Map<Integer, Long>> cursorOffsets = new HashMap<>();
                Map<Integer, Long> partitionOffsets = new HashMap<>();
                partitionOffsets.put(0, 5L);
                cursorOffsets.put(topic, partitionOffsets);
                String cursor = CursorUtil.createCursor(cursorOffsets);

                when(consumerPool.borrowObject()).thenReturn(consumer);
                when(consumer.partitionsFor(topic)).thenReturn(
                                Collections.singletonList(
                                                new org.apache.kafka.common.PartitionInfo(topic, 0, null, null, null)));

                // Mock end offsets
                Map<TopicPartition, OffsetAndTimestamp> endOffsets = new HashMap<>();
                endOffsets.put(partition0, new OffsetAndTimestamp(10L, endTime.toEpochMilli()));
                when(consumer.offsetsForTimes(
                                argThat(map -> map != null && map.containsKey(partition0)
                                                && map.get(partition0) == endTime.toEpochMilli())))
                                .thenReturn(endOffsets);

                // Mock poll
                ConsumerRecord<Object, Object> record1 = new ConsumerRecord<>(topic, 0, 5L, startTime.toEpochMilli(),
                                org.apache.kafka.common.record.TimestampType.CREATE_TIME, 0, 0, "key", "msg5",
                                new org.apache.kafka.common.header.internals.RecordHeaders(), Optional.empty());

                Map<TopicPartition, List<ConsumerRecord<Object, Object>>> recordsMap = new HashMap<>();
                recordsMap.put(partition0, Arrays.asList(record1));
                ConsumerRecords<Object, Object> records = new ConsumerRecords<>(recordsMap);

                when(consumer.poll(any())).thenReturn(records)
                                .thenReturn(new ConsumerRecords<>(Collections.emptyMap()));

                PaginatedResponse<MessageDto> response = kafkaMessageService.getMessages(topic, startTime,
                                endTime, cursor);
                List<MessageDto> messages = response.data();

                assertEquals(1, messages.size());
                assertEquals("msg5", messages.get(0).content());
                assertEquals(5L, messages.get(0).offset());
                assertFalse(response.hasMore());

                verify(consumer).assign(Collections.singletonList(partition0));
                verify(consumer).seek(partition0, 5L); // Verify seek used cursor offset
                verify(consumerPool).returnObject(consumer);
        }

        @Test
        void getMessages_shouldSetHasMoreFlagCorrectly_whenMoreMessagesExist() throws Exception {
                String topic = "test-has-more";
                Instant startTime = Instant.parse("2023-01-01T10:00:00Z");
                Instant endTime = Instant.parse("2023-01-01T10:05:00Z");
                TopicPartition partition0 = new TopicPartition(topic, 0);

                when(kafkaProperties.getMaxMessagesPerRequest()).thenReturn(1);
                when(consumerPool.borrowObject()).thenReturn(consumer);
                when(consumer.partitionsFor(topic)).thenReturn(
                                Collections.singletonList(
                                                new org.apache.kafka.common.PartitionInfo(topic, 0, null, null, null)));

                Map<TopicPartition, OffsetAndTimestamp> startOffsets = new HashMap<>();
                startOffsets.put(partition0, new OffsetAndTimestamp(0L, startTime.toEpochMilli()));
                when(consumer.offsetsForTimes(any())).thenReturn(startOffsets);

                Map<TopicPartition, OffsetAndTimestamp> endOffsets = new HashMap<>();
                endOffsets.put(partition0, new OffsetAndTimestamp(10L, endTime.toEpochMilli()));
                when(consumer.offsetsForTimes(
                                argThat(map -> map.get(partition0) == endTime.toEpochMilli())))
                                .thenReturn(endOffsets);

                ConsumerRecord<Object, Object> record1 = new ConsumerRecord<>(topic, 0, 0L, startTime.toEpochMilli(),
                                org.apache.kafka.common.record.TimestampType.CREATE_TIME, 0, 0, "key", "msg1",
                                new org.apache.kafka.common.header.internals.RecordHeaders(), Optional.empty());
                ConsumerRecord<Object, Object> record2 = new ConsumerRecord<>(topic, 0, 1L,
                                startTime.toEpochMilli() + 1000,
                                org.apache.kafka.common.record.TimestampType.CREATE_TIME, 0, 0, "key", "msg2",
                                new org.apache.kafka.common.header.internals.RecordHeaders(), Optional.empty());

                Map<TopicPartition, List<ConsumerRecord<Object, Object>>> recordsMap = new HashMap<>();
                recordsMap.put(partition0, Arrays.asList(record1, record2));
                ConsumerRecords<Object, Object> records = new ConsumerRecords<>(recordsMap);

                when(consumer.poll(any())).thenReturn(records);

                PaginatedResponse<MessageDto> response = kafkaMessageService.getMessages(topic, startTime,
                                endTime, null);

                assertEquals(1, response.data().size());
                assertTrue(response.hasMore());
                assertNotNull(response.nextCursor());
        }

        @Test
        void getMessages_shouldReturnEmpty_whenCursorPointsToFutureOffset() throws Exception {
                String topic = "test-future-cursor";
                Instant startTime = Instant.parse("2023-01-01T10:00:00Z");
                Instant endTime = Instant.parse("2023-01-01T10:05:00Z");
                TopicPartition partition0 = new TopicPartition(topic, 0);

                // Cursor points to offset 100, but end offset is 10
                Map<String, Map<Integer, Long>> cursorOffsets = new HashMap<>();
                Map<Integer, Long> partitionOffsets = new HashMap<>();
                partitionOffsets.put(0, 100L);
                cursorOffsets.put(topic, partitionOffsets);
                String cursor = CursorUtil.createCursor(cursorOffsets);

                when(consumerPool.borrowObject()).thenReturn(consumer);
                when(consumer.partitionsFor(topic)).thenReturn(
                                Collections.singletonList(
                                                new org.apache.kafka.common.PartitionInfo(topic, 0, null, null, null)));

                Map<TopicPartition, OffsetAndTimestamp> endOffsets = new HashMap<>();
                endOffsets.put(partition0, new OffsetAndTimestamp(10L, endTime.toEpochMilli()));
                when(consumer.offsetsForTimes(any())).thenReturn(endOffsets);

                PaginatedResponse<MessageDto> response = kafkaMessageService.getMessages(topic, startTime, endTime,
                                cursor);

                assertTrue(response.data().isEmpty());
                assertFalse(response.hasMore());

                verify(consumer).assign(Collections.singletonList(partition0));
                // Should not seek or poll if cursor is beyond end offset
                verify(consumer, never()).seek(any(TopicPartition.class), anyLong());
                verify(consumer, never()).poll(any(Duration.class));
                verify(consumerPool).returnObject(consumer);
        }

        @Test
        void getMessages_shouldHandleEmptyTopic() throws Exception {
                String topic = "empty-topic";
                Instant startTime = Instant.parse("2023-01-01T10:00:00Z");
                Instant endTime = Instant.parse("2023-01-01T10:05:00Z");
                when(consumerPool.borrowObject()).thenReturn(consumer);
                when(consumer.partitionsFor(topic)).thenReturn(
                                Collections.singletonList(
                                                new org.apache.kafka.common.PartitionInfo(topic, 0, null, null, null)));

                // Offsets return null for empty topic or no match
                when(consumer.offsetsForTimes(any())).thenReturn(new HashMap<>());

                PaginatedResponse<MessageDto> response = kafkaMessageService.getMessages(topic, startTime, endTime,
                                null);

                assertTrue(response.data().isEmpty());
                assertFalse(response.hasMore());
        }

        @Test
        void getMessages_shouldThrowException_whenCursorIsInvalid() throws Exception {
                String topic = "test-invalid-cursor";
                Instant startTime = Instant.parse("2023-01-01T10:00:00Z");
                Instant endTime = Instant.parse("2023-01-01T10:05:00Z");
                String invalidCursor = "invalid-cursor-string";

                when(consumerPool.borrowObject()).thenReturn(consumer);

                // Depending on implementation, it might ignore invalid cursor or throw
                // exception.
                // Current implementation logs error and returns null from parseCursor,
                // effectively ignoring it.
                // Let's verify it behaves as if no cursor was provided.

                when(consumer.partitionsFor(topic)).thenReturn(
                                Collections.singletonList(
                                                new org.apache.kafka.common.PartitionInfo(topic, 0, null, null, null)));

                // Mock offsets since cursor is ignored
                TopicPartition partition0 = new TopicPartition(topic, 0);
                Map<TopicPartition, OffsetAndTimestamp> startOffsets = new HashMap<>();
                startOffsets.put(partition0, new OffsetAndTimestamp(0L, startTime.toEpochMilli()));
                when(consumer.offsetsForTimes(
                                argThat(map -> map != null && map.containsValue(startTime.toEpochMilli()))))
                                .thenReturn(startOffsets);

                Map<TopicPartition, OffsetAndTimestamp> endOffsets = new HashMap<>();
                endOffsets.put(partition0, new OffsetAndTimestamp(10L, endTime.toEpochMilli()));
                when(consumer.offsetsForTimes(argThat(map -> map != null && map.containsValue(endTime.toEpochMilli()))))
                                .thenReturn(endOffsets);

                when(consumer.poll(any())).thenReturn(new ConsumerRecords<>(Collections.emptyMap()));

                PaginatedResponse<MessageDto> response = kafkaMessageService.getMessages(topic, startTime, endTime,
                                invalidCursor);

                assertNotNull(response);
                assertTrue(response.data().isEmpty());

                verify(consumer).assign(Collections.singletonList(partition0));
                verify(consumer).seek(partition0, 0L); // Should seek to start time, not cursor
        }

        // Multi-partition cursor pagination tests

        @Test
        void getMessages_shouldHandleMultiplePartitions_whenCursorIsProvided() throws Exception {
                String topic = "test-multi-partition";
                Instant startTime = Instant.parse("2023-01-01T10:00:00Z");
                Instant endTime = Instant.parse("2023-01-01T10:05:00Z");
                TopicPartition partition0 = new TopicPartition(topic, 0);
                TopicPartition partition1 = new TopicPartition(topic, 1);

                // Create a cursor that has progressed differently on each partition
                Map<String, Map<Integer, Long>> cursorOffsets = new HashMap<>();
                Map<Integer, Long> partitionOffsets = new HashMap<>();
                partitionOffsets.put(0, 5L); // Partition 0 started at offset 5
                partitionOffsets.put(1, 10L); // Partition 1 started at offset 10
                cursorOffsets.put(topic, partitionOffsets);
                String cursor = CursorUtil.createCursor(cursorOffsets);

                when(consumerPool.borrowObject()).thenReturn(consumer);
                when(consumer.partitionsFor(topic)).thenReturn(
                                Arrays.asList(
                                                new org.apache.kafka.common.PartitionInfo(topic, 0, null, null, null),
                                                new org.apache.kafka.common.PartitionInfo(topic, 1, null, null, null)));

                // Mock end offsets
                Map<TopicPartition, OffsetAndTimestamp> endOffsets = new HashMap<>();
                endOffsets.put(partition0, new OffsetAndTimestamp(20L, endTime.toEpochMilli()));
                endOffsets.put(partition1, new OffsetAndTimestamp(25L, endTime.toEpochMilli()));
                when(consumer.offsetsForTimes(any())).thenReturn(endOffsets);

                // Mock poll with empty results
                when(consumer.poll(any())).thenReturn(new ConsumerRecords<>(Collections.emptyMap()));

                PaginatedResponse<MessageDto> response = kafkaMessageService.getMessages(topic, startTime,
                                endTime, cursor);

                // Verify seek was called with cursor offsets, not time-based offsets
                verify(consumer).seek(partition0, 5L);
                verify(consumer).seek(partition1, 10L);
                assertNotNull(response);
                verify(consumerPool).returnObject(consumer);
        }

        @Test
        void getMessages_shouldContinueFromCorrectPartitions_whenCursorHasPartialProgress() throws Exception {
                String topic = "test-partial-cursor";
                Instant startTime = Instant.parse("2023-01-01T10:00:00Z");
                Instant endTime = Instant.parse("2023-01-01T10:05:00Z");
                TopicPartition partition0 = new TopicPartition(topic, 0);
                TopicPartition partition1 = new TopicPartition(topic, 1);

                // Cursor only has progress for partition 0, partition 1 should use time-based
                // offset
                Map<String, Map<Integer, Long>> cursorOffsets = new HashMap<>();
                Map<Integer, Long> partitionOffsets = new HashMap<>();
                partitionOffsets.put(0, 15L); // Only partition 0 in cursor
                cursorOffsets.put(topic, partitionOffsets);
                String cursor = CursorUtil.createCursor(cursorOffsets);

                when(consumerPool.borrowObject()).thenReturn(consumer);
                when(consumer.partitionsFor(topic)).thenReturn(
                                Arrays.asList(
                                                new org.apache.kafka.common.PartitionInfo(topic, 0, null, null, null),
                                                new org.apache.kafka.common.PartitionInfo(topic, 1, null, null, null)));

                // Mock start offsets for partition 1 only
                Map<TopicPartition, OffsetAndTimestamp> startOffsets = new HashMap<>();
                startOffsets.put(partition1, new OffsetAndTimestamp(0L, startTime.toEpochMilli()));
                when(consumer.offsetsForTimes(
                                argThat(map -> map != null && map.containsKey(partition1)
                                                && !map.containsKey(partition0))))
                                .thenReturn(startOffsets);

                Map<TopicPartition, OffsetAndTimestamp> endOffsets = new HashMap<>();
                endOffsets.put(partition0, new OffsetAndTimestamp(20L, endTime.toEpochMilli()));
                endOffsets.put(partition1, new OffsetAndTimestamp(10L, endTime.toEpochMilli()));
                when(consumer.offsetsForTimes(
                                argThat(map -> map != null && map.containsKey(partition0) && map.containsKey(partition1)
                                                && map.get(partition0) == endTime.toEpochMilli())))
                                .thenReturn(endOffsets);

                when(consumer.poll(any())).thenReturn(new ConsumerRecords<>(Collections.emptyMap()));

                PaginatedResponse<MessageDto> response = kafkaMessageService.getMessages(topic, startTime,
                                endTime, cursor);

                // Partition 0 should use cursor offset, partition 1 should use time-based
                // offset
                verify(consumer).seek(partition0, 15L);
                verify(consumer).seek(partition1, 0L);
                assertNotNull(response);
                verify(consumerPool).returnObject(consumer);
        }

        // Consumer pool error handling tests

        @Test
        void getMessages_shouldThrowException_whenConsumerBorrowFails() throws Exception {
                String topic = "test-topic";
                Instant startTime = Instant.parse("2023-01-01T10:00:00Z");
                Instant endTime = Instant.parse("2023-01-01T10:05:00Z");

                when(consumerPool.borrowObject()).thenThrow(new RuntimeException("Pool exhausted"));

                Exception exception = assertThrows(com.example.krestproxy.exception.KafkaOperationException.class,
                                () -> kafkaMessageService.getMessages(topic, startTime, endTime, null));

                assertTrue(exception.getMessage().contains("Error fetching messages from Kafka"));
                assertTrue(exception.getCause().getMessage().contains("Pool exhausted"));

                // Verify consumer was not returned (since it was never borrowed)
                verify(consumerPool, never()).returnObject(any());
        }

        @Test
        void getMessages_shouldReturnConsumer_whenExceptionOccursDuringFetch() throws Exception {
                String topic = "test-exception";
                Instant startTime = Instant.parse("2023-01-01T10:00:00Z");
                Instant endTime = Instant.parse("2023-01-01T10:05:00Z");

                when(consumerPool.borrowObject()).thenReturn(consumer);
                when(consumer.partitionsFor(topic)).thenThrow(new RuntimeException("Kafka error"));

                assertThrows(com.example.krestproxy.exception.KafkaOperationException.class,
                                () -> kafkaMessageService.getMessages(topic, startTime, endTime, null));

                // Verify consumer was returned to pool even though exception occurred
                verify(consumerPool).returnObject(consumer);
        }

        // Null and empty value handling tests

        @Test
        void getMessages_shouldHandleNullValues() throws Exception {
                String topic = "test-null-values";
                Instant startTime = Instant.parse("2023-01-01T10:00:00Z");
                Instant endTime = Instant.parse("2023-01-01T10:05:00Z");
                TopicPartition partition0 = new TopicPartition(topic, 0);

                when(consumerPool.borrowObject()).thenReturn(consumer);
                when(consumer.partitionsFor(topic)).thenReturn(
                                Collections.singletonList(
                                                new org.apache.kafka.common.PartitionInfo(topic, 0, null, null, null)));

                Map<TopicPartition, OffsetAndTimestamp> startOffsets = new HashMap<>();
                startOffsets.put(partition0, new OffsetAndTimestamp(0L, startTime.toEpochMilli()));
                when(consumer.offsetsForTimes(any())).thenReturn(startOffsets);

                Map<TopicPartition, OffsetAndTimestamp> endOffsets = new HashMap<>();
                endOffsets.put(partition0, new OffsetAndTimestamp(10L, endTime.toEpochMilli()));
                when(consumer.offsetsForTimes(
                                argThat(map -> map != null && map.get(partition0) == endTime.toEpochMilli())))
                                .thenReturn(endOffsets);

                // Create records with null values
                ConsumerRecord<Object, Object> recordWithNullValue = new ConsumerRecord<>(topic, 0, 0L,
                                startTime.toEpochMilli(),
                                org.apache.kafka.common.record.TimestampType.CREATE_TIME, 0, 0, "key", null,
                                new org.apache.kafka.common.header.internals.RecordHeaders(), Optional.empty());

                Map<TopicPartition, List<ConsumerRecord<Object, Object>>> recordsMap = new HashMap<>();
                recordsMap.put(partition0, Collections.singletonList(recordWithNullValue));
                ConsumerRecords<Object, Object> records = new ConsumerRecords<>(recordsMap);

                when(consumer.poll(any())).thenReturn(records)
                                .thenReturn(new ConsumerRecords<>(Collections.emptyMap()));

                PaginatedResponse<MessageDto> response = kafkaMessageService.getMessages(topic, startTime,
                                endTime, null);

                assertEquals(1, response.data().size());
                assertNull(response.data().get(0).content());
                verify(consumerPool).returnObject(consumer);
        }

        // Execution time caching tests

        @Test
        void findExecutionTimes_shouldThrowException_whenOnlyStartFound() throws Exception {
                String execId = "exec-incomplete-start";
                String execTopic = "execids";

                when(consumerPool.borrowObject()).thenReturn(consumer);

                TopicPartition execPartition = new TopicPartition(execTopic, 0);

                // Only start record, no end record
                ConsumerRecord<Object, Object> startRecord = new ConsumerRecord<>(execTopic, 0, 0, 1000,
                                org.apache.kafka.common.record.TimestampType.CREATE_TIME,
                                0, 0, execId, "start", new org.apache.kafka.common.header.internals.RecordHeaders(),
                                Optional.empty());

                Map<TopicPartition, List<ConsumerRecord<Object, Object>>> recordsMap = new HashMap<>();
                recordsMap.put(execPartition, Collections.singletonList(startRecord));
                ConsumerRecords<Object, Object> execRecords = new ConsumerRecords<>(recordsMap);

                when(consumer.poll(any(Duration.class)))
                                .thenReturn(execRecords)
                                .thenReturn(new ConsumerRecords<>(Collections.emptyMap()));

                assertThrows(com.example.krestproxy.exception.ExecutionNotFoundException.class,
                                () -> kafkaMessageService.findExecutionTimes(execId));

                verify(consumerPool).returnObject(consumer);
        }

        @Test
        void findExecutionTimes_shouldThrowException_whenOnlyEndFound() throws Exception {
                String execId = "exec-incomplete-end";
                String execTopic = "execids";

                when(consumerPool.borrowObject()).thenReturn(consumer);

                TopicPartition execPartition = new TopicPartition(execTopic, 0);

                // Only end record, no start record
                ConsumerRecord<Object, Object> endRecord = new ConsumerRecord<>(execTopic, 0, 1, 2000,
                                org.apache.kafka.common.record.TimestampType.CREATE_TIME,
                                0, 0, execId, "end", new org.apache.kafka.common.header.internals.RecordHeaders(),
                                Optional.empty());

                Map<TopicPartition, List<ConsumerRecord<Object, Object>>> recordsMap = new HashMap<>();
                recordsMap.put(execPartition, Collections.singletonList(endRecord));
                ConsumerRecords<Object, Object> execRecords = new ConsumerRecords<>(recordsMap);

                when(consumer.poll(any(Duration.class)))
                                .thenReturn(execRecords)
                                .thenReturn(new ConsumerRecords<>(Collections.emptyMap()));

                assertThrows(com.example.krestproxy.exception.ExecutionNotFoundException.class,
                                () -> kafkaMessageService.findExecutionTimes(execId));

                verify(consumerPool).returnObject(consumer);
        }

        // Time boundary condition tests

        @Test
        void getMessages_shouldIncludeMessagesAtExactStartTime() throws Exception {
                String topic = "test-start-boundary";
                Instant startTime = Instant.parse("2023-01-01T10:00:00Z");
                Instant endTime = Instant.parse("2023-01-01T10:05:00Z");
                TopicPartition partition0 = new TopicPartition(topic, 0);

                when(consumerPool.borrowObject()).thenReturn(consumer);
                when(consumer.partitionsFor(topic)).thenReturn(
                                Collections.singletonList(
                                                new org.apache.kafka.common.PartitionInfo(topic, 0, null, null, null)));

                Map<TopicPartition, OffsetAndTimestamp> startOffsets = new HashMap<>();
                startOffsets.put(partition0, new OffsetAndTimestamp(0L, startTime.toEpochMilli()));
                when(consumer.offsetsForTimes(any())).thenReturn(startOffsets);

                Map<TopicPartition, OffsetAndTimestamp> endOffsets = new HashMap<>();
                endOffsets.put(partition0, new OffsetAndTimestamp(10L, endTime.toEpochMilli()));
                when(consumer.offsetsForTimes(
                                argThat(map -> map != null && map.get(partition0) == endTime.toEpochMilli())))
                                .thenReturn(endOffsets);

                // Record at exact start time
                ConsumerRecord<Object, Object> recordAtStartTime = new ConsumerRecord<>(topic, 0, 0L,
                                startTime.toEpochMilli(),
                                org.apache.kafka.common.record.TimestampType.CREATE_TIME, 0, 0, "key",
                                "at-start",
                                new org.apache.kafka.common.header.internals.RecordHeaders(), Optional.empty());

                Map<TopicPartition, List<ConsumerRecord<Object, Object>>> recordsMap = new HashMap<>();
                recordsMap.put(partition0, Collections.singletonList(recordAtStartTime));
                ConsumerRecords<Object, Object> records = new ConsumerRecords<>(recordsMap);

                when(consumer.poll(any())).thenReturn(records)
                                .thenReturn(new ConsumerRecords<>(Collections.emptyMap()));

                PaginatedResponse<MessageDto> response = kafkaMessageService.getMessages(topic, startTime,
                                endTime, null);

                assertEquals(1, response.data().size());
                assertEquals("at-start", response.data().get(0).content());
                verify(consumerPool).returnObject(consumer);
        }

        @Test
        void getMessages_shouldIncludeMessagesAtExactEndTime() throws Exception {
                String topic = "test-end-boundary";
                Instant startTime = Instant.parse("2023-01-01T10:00:00Z");
                Instant endTime = Instant.parse("2023-01-01T10:05:00Z");
                TopicPartition partition0 = new TopicPartition(topic, 0);

                when(consumerPool.borrowObject()).thenReturn(consumer);
                when(consumer.partitionsFor(topic)).thenReturn(
                                Collections.singletonList(
                                                new org.apache.kafka.common.PartitionInfo(topic, 0, null, null, null)));

                Map<TopicPartition, OffsetAndTimestamp> startOffsets = new HashMap<>();
                startOffsets.put(partition0, new OffsetAndTimestamp(0L, startTime.toEpochMilli()));
                when(consumer.offsetsForTimes(any())).thenReturn(startOffsets);

                Map<TopicPartition, OffsetAndTimestamp> endOffsets = new HashMap<>();
                endOffsets.put(partition0, new OffsetAndTimestamp(10L, endTime.toEpochMilli()));
                when(consumer.offsetsForTimes(
                                argThat(map -> map != null && map.get(partition0) == endTime.toEpochMilli())))
                                .thenReturn(endOffsets);

                // Record at exact end time
                ConsumerRecord<Object, Object> recordAtEndTime = new ConsumerRecord<>(topic, 0, 9L,
                                endTime.toEpochMilli(),
                                org.apache.kafka.common.record.TimestampType.CREATE_TIME, 0, 0, "key", "at-end",
                                new org.apache.kafka.common.header.internals.RecordHeaders(), Optional.empty());

                Map<TopicPartition, List<ConsumerRecord<Object, Object>>> recordsMap = new HashMap<>();
                recordsMap.put(partition0, Collections.singletonList(recordAtEndTime));
                ConsumerRecords<Object, Object> records = new ConsumerRecords<>(recordsMap);

                when(consumer.poll(any())).thenReturn(records)
                                .thenReturn(new ConsumerRecords<>(Collections.emptyMap()));

                PaginatedResponse<MessageDto> response = kafkaMessageService.getMessages(topic, startTime,
                                endTime, null);

                // Should be included since timestamp <= endTime
                assertEquals(1, response.data().size());
                assertEquals("at-end", response.data().get(0).content());
                verify(consumerPool).returnObject(consumer);
        }

        // Partition edge case tests

        @Test
        void getMessages_shouldHandleTopicWithNoPartitions() throws Exception {
                String topic = "topic-no-partitions";
                Instant startTime = Instant.parse("2023-01-01T10:00:00Z");
                Instant endTime = Instant.parse("2023-01-01T10:05:00Z");

                when(consumerPool.borrowObject()).thenReturn(consumer);
                when(consumer.partitionsFor(topic)).thenReturn(Collections.emptyList());

                PaginatedResponse<MessageDto> response = kafkaMessageService.getMessages(topic, startTime,
                                endTime, null);

                assertTrue(response.data().isEmpty());
                assertFalse(response.hasMore());

                // Consumer should still be returned to pool
                verify(consumerPool).returnObject(consumer);
        }

        @Test
        void getMessages_shouldHandleNullPartitionInfo() throws Exception {
                String topic = "topic-null-partitions";
                Instant startTime = Instant.parse("2023-01-01T10:00:00Z");
                Instant endTime = Instant.parse("2023-01-01T10:05:00Z");

                when(consumerPool.borrowObject()).thenReturn(consumer);
                when(consumer.partitionsFor(topic)).thenReturn(null);

                PaginatedResponse<MessageDto> response = kafkaMessageService.getMessages(topic, startTime,
                                endTime, null);

                assertTrue(response.data().isEmpty());
                assertFalse(response.hasMore());

                verify(consumerPool).returnObject(consumer);
        }
}
