package com.example.krestproxy.service;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.pool2.ObjectPool;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class KafkaMessageService {

    private final ObjectPool<Consumer<String, Object>> consumerPool;

    @Autowired
    public KafkaMessageService(ObjectPool<Consumer<String, Object>> consumerPool) {
        this.consumerPool = consumerPool;
    }

    public List<com.example.krestproxy.dto.MessageDto> getMessages(String topic, Instant startTime, Instant endTime) {
        Consumer<String, Object> consumer = null;
        try {
            consumer = consumerPool.borrowObject();

            // Assign all partitions of the topic to this consumer
            List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
                    .map(pi -> new TopicPartition(topic, pi.partition()))
                    .collect(Collectors.toList());

            consumer.assign(partitions);

            // Find offsets for start time
            Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
            for (TopicPartition partition : partitions) {
                timestampsToSearch.put(partition, startTime.toEpochMilli());
            }

            Map<TopicPartition, OffsetAndTimestamp> startOffsets = consumer.offsetsForTimes(timestampsToSearch);

            // Find offsets for end time
            Map<TopicPartition, Long> endTimestampsToSearch = new HashMap<>();
            for (TopicPartition partition : partitions) {
                endTimestampsToSearch.put(partition, endTime.toEpochMilli());
            }

            Map<TopicPartition, OffsetAndTimestamp> endOffsets = consumer.offsetsForTimes(endTimestampsToSearch);

            List<com.example.krestproxy.dto.MessageDto> messages = new ArrayList<>();

            for (TopicPartition partition : partitions) {
                OffsetAndTimestamp startOffset = startOffsets.get(partition);
                OffsetAndTimestamp endOffset = endOffsets.get(partition);

                if (startOffset != null) {
                    consumer.seek(partition, startOffset.offset());

                    boolean keepReading = true;
                    while (keepReading) {
                        ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis(100));
                        if (records.isEmpty()) {
                            break;
                        }

                        for (ConsumerRecord<String, Object> record : records.records(partition)) {
                            if (record.timestamp() >= startTime.toEpochMilli()
                                    && record.timestamp() <= endTime.toEpochMilli()) {
                                String content;
                                Object value = record.value();
                                if (value instanceof GenericRecord) {
                                    content = convertAvroToJson((GenericRecord) value);
                                } else if (value != null) {
                                    content = value.toString();
                                } else {
                                    content = null;
                                }

                                messages.add(new com.example.krestproxy.dto.MessageDto(
                                        content,
                                        record.timestamp(),
                                        record.partition(),
                                        record.offset()));
                            } else if (record.timestamp() > endTime.toEpochMilli()) {
                                keepReading = false;
                                break;
                            }

                            // Optimization: if we have a target end offset, we can check position.
                            if (endOffset != null && record.offset() >= endOffset.offset()) {
                                keepReading = false;
                                break;
                            }
                        }

                        // Safety break if we reached end of partition
                        if (endOffset != null && consumer.position(partition) >= endOffset.offset()) {
                            keepReading = false;
                        }
                    }
                }
            }
            return messages;
        } catch (Exception e) {
            throw new RuntimeException("Error fetching messages from Kafka", e);
        } finally {
            if (consumer != null) {
                try {
                    consumerPool.returnObject(consumer);
                } catch (Exception e) {
                    // Log error returning to pool
                }
            }
        }
    }

    private String convertAvroToJson(GenericRecord record) {
        try {
            java.io.ByteArrayOutputStream outputStream = new java.io.ByteArrayOutputStream();
            org.apache.avro.io.JsonEncoder jsonEncoder = org.apache.avro.io.EncoderFactory.get()
                    .jsonEncoder(record.getSchema(), outputStream);
            org.apache.avro.generic.GenericDatumWriter<GenericRecord> writer = new org.apache.avro.generic.GenericDatumWriter<>(
                    record.getSchema());
            writer.write(record, jsonEncoder);
            jsonEncoder.flush();
            return outputStream.toString();
        } catch (java.io.IOException e) {
            throw new RuntimeException("Error converting Avro to JSON", e);
        }
    }
}
