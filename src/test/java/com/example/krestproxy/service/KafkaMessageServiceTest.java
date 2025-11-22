package com.example.krestproxy.service;

import com.example.krestproxy.dto.MessageDto;
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

import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KafkaMessageServiceTest {

        @Mock
        private ObjectPool<Consumer<Object, Object>> consumerPool;

        @Mock
        private Consumer<Object, Object> consumer;

        private KafkaMessageService kafkaMessageService;

        @BeforeEach
        void setUp() throws Exception {
                kafkaMessageService = new KafkaMessageService(consumerPool);
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

                List<MessageDto> messages = kafkaMessageService.getMessages(topic, startTime,
                                endTime);

                assertEquals(2, messages.size());
                assertEquals("msg1", messages.get(0).content());
                assertEquals("msg2", messages.get(1).content());

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

            ConsumerRecord<Object, Object> record2 = new ConsumerRecord<>(topic, 0, 1L, startTime.toEpochMilli() + 100,
                    org.apache.kafka.common.record.TimestampType.CREATE_TIME, 0, 0, key2, "msg2",
                    new org.apache.kafka.common.header.internals.RecordHeaders(), Optional.empty());

            Map<TopicPartition, List<ConsumerRecord<Object, Object>>> recordsMap = new HashMap<>();
            recordsMap.put(partition0, Arrays.asList(record1, record2));
            ConsumerRecords<Object, Object> records = new ConsumerRecords<>(recordsMap);

            when(consumer.poll(any())).thenReturn(records)
                    .thenReturn(new ConsumerRecords<>(Collections.emptyMap()));

            List<MessageDto> messages = kafkaMessageService.getMessagesWithExecId(topic, startTime,
                    endTime, targetExecId);

            assertEquals(1, messages.size());
            assertEquals("msg1", messages.get(0).content());

            verify(consumerPool).returnObject(consumer);
        }
}
