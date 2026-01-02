package com.example.krestproxy.service;

import com.example.krestproxy.config.KafkaProperties;
import com.example.krestproxy.dto.MessageDto;
import com.example.krestproxy.dto.PaginatedResponse;
import com.example.krestproxy.exception.ExecutionNotFoundException;
import com.example.krestproxy.exception.KafkaOperationException;
import com.example.krestproxy.util.CursorUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.commons.pool2.ObjectPool;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class KafkaMessageService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaMessageService.class);
    private static final String EXEC_IDS_TOPIC = "execids";

    private static final ThreadLocal<ReusableAvroResources> avroResources = ThreadLocal
            .withInitial(ReusableAvroResources::new);

    private static class ReusableAvroResources {
        final java.io.ByteArrayOutputStream outputStream = new java.io.ByteArrayOutputStream();
        JsonEncoder encoder = null;
        final GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>();
    }

    private final ObjectPool<Consumer<Object, Object>> consumerPool;
    private final KafkaProperties kafkaProperties;
    private final ObjectMapper objectMapper;

    @Autowired
    public KafkaMessageService(ObjectPool<Consumer<Object, Object>> consumerPool,
            KafkaProperties kafkaProperties, ObjectMapper objectMapper) {
        this.consumerPool = consumerPool;
        this.kafkaProperties = kafkaProperties;
        this.objectMapper = objectMapper;
        logger.info("KafkaMessageService initialized with consumer pool");
    }

    public List<MessageDto> getMessagesForExecution(List<String> topics, String execId) {
        logger.info("Fetching messages for execution: {}, topics: {}", execId, topics);
        var times = findExecutionTimes(execId);
        // For multi-topic execution fetch, we don't support cursor pagination yet as
        // per requirement "single topic"
        // But we need to adapt to the internal method signature change.
        // We can return just the list from the paginated response for now or keep it as
        // list if we overload internal.
        // Let's overload internal or just unwrap.
        return getMessagesInternal(topics, times.start(), times.end(), execId, null).data();
    }

    public record ExecTime(Instant start, Instant end) {
    }

    @Cacheable(value = "execTimes", key = "#execId")
    protected ExecTime findExecutionTimes(String execId) {
        logger.debug("Cache miss for execution ID: {}, scanning execids topic", execId);

        Consumer<Object, Object> consumer = null;
        try {
            consumer = consumerPool.borrowObject();
            var topicPartition = new TopicPartition(EXEC_IDS_TOPIC, 0);
            consumer.assign(List.of(topicPartition));
            consumer.seekToBeginning(List.of(topicPartition));

            Instant startTime = null;
            Instant endTime = null;

            // Assuming "few days" retention isn't massive, but we should be careful.
            // We scan until we find both or reach end.
            while (startTime == null || endTime == null) {
                var records = consumer.poll(Duration.ofMillis(kafkaProperties.getPollTimeoutMs()));
                if (records.isEmpty()) {
                    break;
                }

                for (var record : records) {
                    String keyStr = record.key().toString();
                    if (execId.equals(keyStr)) {
                        String valStr = record.value().toString();
                        if ("start".equals(valStr)) {
                            startTime = Instant.ofEpochMilli(record.timestamp());
                        } else if ("end".equals(valStr)) {
                            endTime = Instant.ofEpochMilli(record.timestamp());
                        }
                    }
                }
            }

            if (startTime == null || endTime == null) {
                logger.warn("Execution ID not found: {}", execId);
                throw new ExecutionNotFoundException(execId);
            }

            var execTime = new ExecTime(startTime, endTime);
            logger.info("Found execution times for {}: start={}, end={}", execId, startTime, endTime);
            return execTime;

        } catch (ExecutionNotFoundException e) {
            throw e;
        } catch (Exception e) {
            logger.error("Error scanning execids topic for execution ID: {}", execId, e);
            throw new KafkaOperationException("Error scanning execids topic", e);
        } finally {
            if (consumer != null) {
                try {
                    consumerPool.returnObject(consumer);
                } catch (Exception e) {
                    logger.error("Error returning consumer to pool", e);
                }
            }
        }
    }

    public PaginatedResponse<MessageDto> getMessages(String topic, Instant startTime, Instant endTime, String cursor) {
        logger.info("Fetching messages from topic: {}, startTime: {}, endTime: {}, cursor: {}", topic, startTime,
                endTime, cursor);
        return getMessagesInternal(List.of(topic), startTime, endTime, null, cursor);
    }

    public PaginatedResponse<MessageDto> getMessagesWithExecId(String topic, Instant startTime, Instant endTime,
            String execId, String cursor) {
        logger.info("Fetching messages from topic: {} with execId: {}, startTime: {}, endTime: {}, cursor: {}",
                topic, execId, startTime, endTime, cursor);
        return getMessagesInternal(List.of(topic), startTime, endTime, execId, cursor);
    }

    public List<MessageDto> getMessagesFromTopics(List<String> topics, Instant startTime, Instant endTime,
            String execId) {
        logger.info("Fetching messages from topics: {} with execId: {}, startTime: {}, endTime: {}",
                topics, execId, startTime, endTime);
        return getMessagesInternal(topics, startTime, endTime, execId, null).data();
    }

    public PaginatedResponse<MessageDto> getBatchMessages(List<String> topics, String cursor) {
        logger.info("Fetching batch messages from topics: {}, cursor: {}", topics, cursor);
        Consumer<Object, Object> consumer = null;
        try {
            logger.debug("Borrowing consumer from pool");
            consumer = consumerPool.borrowObject();

            List<TopicPartition> partitions = assignPartitions(consumer, topics);
            Map<String, Map<Integer, Long>> cursorOffsets = CursorUtil.parseCursor(cursor);
            Map<String, Map<Integer, Long>> nextCursorOffsets = initializeNextCursorOffsets(cursorOffsets);

            // For batch, we don't have time window. We need end offsets (High Watermark) to know when to stop.
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
            // We convert to OffsetAndTimestamp structure to reuse existing helper or just map.
            // But wait, existing endOffsets is Map<TopicPartition, OffsetAndTimestamp> (time based).
            // Here we have log end offsets (Long).
            // We will use a dedicated fetch method or adapt. dedicated seems cleaner given the logic difference.

            var messages = new ArrayList<MessageDto>();
            int maxMessages = kafkaProperties.getMaxMessagesPerRequest();

            boolean limitReached = fetchBatchMessages(consumer, partitions, cursorOffsets, endOffsets, messages, maxMessages, nextCursorOffsets);

            // Determine hasMore
            // For batch without time window, hasMore is true if:
            // 1. limitReached is true
            // AND
            // 2. We haven't reached the end of all partitions?
            // Actually, if limitReached is true, there is definitely more (unless we hit exact end).
            // But we need to check if we are at end of all partitions.

            boolean hasMore = determineBatchHasMore(limitReached, partitions, endOffsets, nextCursorOffsets, cursorOffsets);
            String nextCursor = hasMore ? CursorUtil.createCursor(nextCursorOffsets) : null;

            logger.info("Retrieved {} batch messages from topics: {}", messages.size(), topics);
            return new PaginatedResponse<>(messages, nextCursor, hasMore);

        } catch (Exception e) {
            logger.error("Error fetching batch messages from Kafka topics: {}", topics, e);
            throw new KafkaOperationException("Error fetching batch messages from Kafka", e);
        } finally {
            if (consumer != null) {
                try {
                    consumerPool.returnObject(consumer);
                } catch (Exception e) {
                    logger.error("Error returning consumer to pool", e);
                }
            }
        }
    }

    private boolean fetchBatchMessages(Consumer<Object, Object> consumer, List<TopicPartition> partitions,
            Map<String, Map<Integer, Long>> cursorOffsets, Map<TopicPartition, Long> endOffsets,
            List<MessageDto> messages, int maxMessages, Map<String, Map<Integer, Long>> nextCursorOffsets) {

        boolean limitReached = false;
        for (var partition : partitions) {
            long seekOffset;
            boolean hasCursor = cursorOffsets != null
                    && cursorOffsets.containsKey(partition.topic())
                    && cursorOffsets.get(partition.topic()).containsKey(partition.partition());

            if (hasCursor) {
                seekOffset = cursorOffsets.get(partition.topic()).get(partition.partition());
                consumer.seek(partition, seekOffset);
            } else {
                // If no cursor, start from beginning
                consumer.seekToBeginning(List.of(partition));
                // We need to know where we are starting from to populate nextCursor if we read nothing?
                // No, nextCursor is updated as we read.
            }

            Long endOffset = endOffsets.get(partition);
            if (endOffset == null) {
                // Should not happen if partition exists
                continue;
            }

            if (consumer.position(partition) >= endOffset) {
                continue;
            }

            var keepReading = true;
            while (keepReading) {
                var records = consumer.poll(Duration.ofMillis(kafkaProperties.getPollTimeoutMs()));
                if (records.isEmpty()) {
                    break;
                }

                for (var record : records.records(partition)) {
                    nextCursorOffsets.computeIfAbsent(partition.topic(), k -> new HashMap<>())
                            .put(partition.partition(), record.offset() + 1);

                    messages.add(createMessageDto(record));
                    if (messages.size() >= maxMessages) {
                        logger.warn("Reached maximum message limit of {} for batch", maxMessages);
                        keepReading = false;
                        limitReached = true;
                        break;
                    }

                    if (record.offset() >= endOffset - 1) {
                         // We reached the end offset (exclusive).
                         // Wait, record.offset is inclusive. endOffset is exclusive high watermark.
                         // So if record.offset == endOffset - 1, we are at the last message.
                        keepReading = false;
                        break;
                    }
                }

                // Double check position vs endOffset in case poll returned partial
                if (consumer.position(partition) >= endOffset) {
                    keepReading = false;
                }
            }

            if (limitReached) {
                break;
            }
        }
        return limitReached;
    }

    private boolean determineBatchHasMore(boolean limitReached, List<TopicPartition> partitions,
            Map<TopicPartition, Long> endOffsets,
            Map<String, Map<Integer, Long>> nextCursorOffsets, Map<String, Map<Integer, Long>> cursorOffsets) {
        if (!limitReached) {
            return false;
        }
        // If limit reached, we need to check if there is any partition that has not reached endOffset.
        // We check current position (from nextCursor or cursor or beginning) vs endOffset.

        for (var partition : partitions) {
            Long endOffset = endOffsets.get(partition);
            if (endOffset == null || endOffset == 0) {
                continue;
            }

            Long currentPosition = null;
            if (nextCursorOffsets.containsKey(partition.topic())
                    && nextCursorOffsets.get(partition.topic()).containsKey(partition.partition())) {
                currentPosition = nextCursorOffsets.get(partition.topic()).get(partition.partition());
            }

            if (currentPosition == null) {
                 if (cursorOffsets != null
                        && cursorOffsets.containsKey(partition.topic())
                        && cursorOffsets.get(partition.topic()).containsKey(partition.partition())) {
                    currentPosition = cursorOffsets.get(partition.topic()).get(partition.partition());
                } else {
                    // Start from beginning - we don't know the exact offset value of "beginning" here easily
                    // without querying, but we can assume if endOffset > 0, there is data.
                    // But maybe we already read it?
                    // If we didn't populate nextCursor for this partition, and limitReached is true,
                    // and we haven't processed this partition yet (because we stopped early), then we have more.
                    // But how do we know if we processed it?
                    // The fetch loop is sequential.
                    // If we stopped at partition `i`, then partitions `i+1`... are untouched.
                    // So if we find a partition with NO nextCursor entry, and it has data (endOffset > 0? No, beginning might be == endOffset).
                    // We can't strictly know if we have more without knowing beginning offset.
                    // However, we can approximate: if we haven't touched it, assume potentially has more.
                    // But safe bet: calculate if *any* partition is behind endOffset.
                    currentPosition = -1L; // Flag for "unknown/beginning"
                }
            }

            if (currentPosition == -1L) {
                 // We haven't read this partition. If it has messages, hasMore = true.
                 // We can't know for sure if beginning < endOffset without querying beginningOffsets.
                 // But typically if endOffset > 0 it likely has data (unless all expired).
                 // To be perfectly correct, we should probably fetch beginningOffsets too in getBatchMessages.
                 // Let's assume yes for now, or improve.
                 // Actually, if we stopped early, any subsequent partition in the list is "more".
                 return true;
            }

            if (currentPosition < endOffset) {
                return true;
            }
        }
        return false;
    }

    private PaginatedResponse<MessageDto> getMessagesInternal(java.util.Collection<String> topics, Instant startTime,
            Instant endTime, String execId, String cursor) {
        Consumer<Object, Object> consumer = null;
        try {
            logger.debug("Borrowing consumer from pool");
            consumer = consumerPool.borrowObject();

            List<TopicPartition> partitions = assignPartitions(consumer, topics);
            Map<String, Map<Integer, Long>> cursorOffsets = CursorUtil.parseCursor(cursor);
            Map<String, Map<Integer, Long>> nextCursorOffsets = initializeNextCursorOffsets(cursorOffsets);

            Map<TopicPartition, OffsetAndTimestamp> startOffsets = calculateStartOffsets(consumer, partitions,
                    startTime, cursorOffsets);
            Map<TopicPartition, OffsetAndTimestamp> endOffsets = calculateEndOffsets(consumer, partitions, endTime);

            var messages = new ArrayList<MessageDto>();
            int maxMessages = kafkaProperties.getMaxMessagesPerRequest();
            boolean limitReached = fetchMessages(consumer, partitions, startTime, endTime, execId, cursorOffsets,
                    startOffsets, endOffsets, messages, maxMessages, nextCursorOffsets);

            boolean hasMore = determineHasMore(limitReached, partitions, endOffsets, nextCursorOffsets, cursorOffsets,
                    startOffsets);
            String nextCursor = hasMore ? CursorUtil.createCursor(nextCursorOffsets) : null;

            logger.info("Retrieved {} messages from topics: {}", messages.size(), topics);
            return new PaginatedResponse<>(messages, nextCursor, hasMore);
        } catch (Exception e) {
            logger.error("Error fetching messages from Kafka topics: {}", topics, e);
            throw new KafkaOperationException("Error fetching messages from Kafka", e);
        } finally {
            if (consumer != null) {
                try {
                    consumerPool.returnObject(consumer);
                } catch (Exception e) {
                    logger.error("Error returning consumer to pool", e);
                }
            }
        }
    }

    private Map<String, Map<Integer, Long>> initializeNextCursorOffsets(Map<String, Map<Integer, Long>> cursorOffsets) {
        Map<String, Map<Integer, Long>> nextCursorOffsets = new HashMap<>();
        if (cursorOffsets != null) {
            for (var entry : cursorOffsets.entrySet()) {
                nextCursorOffsets.put(entry.getKey(), new HashMap<>(entry.getValue()));
            }
        }
        return nextCursorOffsets;
    }

    private List<TopicPartition> assignPartitions(Consumer<Object, Object> consumer,
            java.util.Collection<String> topics) {
        var partitions = new ArrayList<TopicPartition>();
        for (String topic : topics) {
            var partitionInfos = consumer.partitionsFor(topic);
            if (partitionInfos != null) {
                partitions.addAll(partitionInfos.stream()
                        .map(pi -> new TopicPartition(topic, pi.partition()))
                        .toList());
            }
        }
        consumer.assign(partitions);
        return partitions;
    }

    private Map<TopicPartition, OffsetAndTimestamp> calculateStartOffsets(Consumer<Object, Object> consumer,
            List<TopicPartition> partitions, Instant startTime, Map<String, Map<Integer, Long>> cursorOffsets) {
        // If we have a cursor for a partition, we don't need to look up the start
        // offset by time for that partition.
        // However, the consumer.offsetsForTimes API takes a map.
        // We can just look up all and then override with cursor if present.

        var timestampsToSearch = new HashMap<TopicPartition, Long>();
        for (var partition : partitions) {
            boolean hasCursor = cursorOffsets != null
                    && cursorOffsets.containsKey(partition.topic())
                    && cursorOffsets.get(partition.topic()).containsKey(partition.partition());

            if (!hasCursor) {
                timestampsToSearch.put(partition, startTime.toEpochMilli());
            }
        }

        if (timestampsToSearch.isEmpty()) {
            return new HashMap<>();
        }

        return consumer.offsetsForTimes(timestampsToSearch);
    }

    private Map<TopicPartition, OffsetAndTimestamp> calculateEndOffsets(Consumer<Object, Object> consumer,
            List<TopicPartition> partitions, Instant endTime) {
        var endTimestampsToSearch = new HashMap<TopicPartition, Long>();
        for (var partition : partitions) {
            endTimestampsToSearch.put(partition, endTime.toEpochMilli());
        }
        return consumer.offsetsForTimes(endTimestampsToSearch);
    }

    private boolean fetchMessages(Consumer<Object, Object> consumer, List<TopicPartition> partitions, Instant startTime,
            Instant endTime, String execId,
            Map<String, Map<Integer, Long>> cursorOffsets, Map<TopicPartition, OffsetAndTimestamp> startOffsets,
            Map<TopicPartition, OffsetAndTimestamp> endOffsets,
            List<MessageDto> messages, int maxMessages, Map<String, Map<Integer, Long>> nextCursorOffsets) {
        boolean limitReached = false;
        for (var partition : partitions) {
            long seekOffset;
            if (cursorOffsets != null
                    && cursorOffsets.containsKey(partition.topic())
                    && cursorOffsets.get(partition.topic()).containsKey(partition.partition())) {
                seekOffset = cursorOffsets.get(partition.topic()).get(partition.partition());
            } else if (startOffsets != null && startOffsets.get(partition) != null) {
                seekOffset = startOffsets.get(partition).offset();
            } else {
                continue;
            }

            var endOffset = endOffsets.get(partition);
            if (endOffset != null && seekOffset >= endOffset.offset()) {
                continue;
            }

            consumer.seek(partition, seekOffset);

            var keepReading = true;
            while (keepReading) {
                var records = consumer.poll(Duration.ofMillis(kafkaProperties.getPollTimeoutMs()));
                if (records.isEmpty()) {
                    break;
                }

                for (var record : records.records(partition)) {
                    nextCursorOffsets.computeIfAbsent(partition.topic(), k -> new HashMap<>())
                            .put(partition.partition(), record.offset() + 1);

                    if (record.timestamp() >= startTime.toEpochMilli()
                            && record.timestamp() <= endTime.toEpochMilli()) {
                        if (isMatch(record, execId)) {
                            messages.add(createMessageDto(record));
                            if (messages.size() >= maxMessages) {
                                logger.warn("Reached maximum message limit of {} for topics", maxMessages);
                                keepReading = false;
                                limitReached = true;
                                break;
                            }
                        }
                    } else if (record.timestamp() > endTime.toEpochMilli()) {
                        keepReading = false;
                        break;
                    }

                    if (endOffset != null && record.offset() >= endOffset.offset()) {
                        keepReading = false;
                        break;
                    }
                }

                if (endOffset != null && consumer.position(partition) >= endOffset.offset()) {
                    keepReading = false;
                }
            }

            if (limitReached) {
                break;
            }
        }
        return limitReached;
    }

    private boolean isMatch(org.apache.kafka.clients.consumer.ConsumerRecord<Object, Object> record, String execId) {
        if (execId == null) {
            return true;
        }
        if (record.key() instanceof GenericRecord keyRecord) {
            var execIdObj = keyRecord.get("exec_id");
            return execIdObj != null && execIdObj.toString().equals(execId);
        }
        return false;
    }

    private MessageDto createMessageDto(org.apache.kafka.clients.consumer.ConsumerRecord<Object, Object> record) {
        Object content = switch (record.value()) {
            case GenericRecord genericRecord -> {
                String jsonString = convertAvroToJson(genericRecord);
                try {
                    yield objectMapper.readTree(jsonString);
                } catch (Exception e) {
                    logger.error("Error parsing JSON content", e);
                    yield jsonString; // Fallback to string if parsing fails
                }
            }
            case null -> null;
            case Object o -> o.toString();
        };
        return new MessageDto(
                record.topic(),
                content,
                record.timestamp(),
                record.partition(),
                record.offset());
    }

    private boolean determineHasMore(boolean limitReached, List<TopicPartition> partitions,
            Map<TopicPartition, OffsetAndTimestamp> endOffsets,
            Map<String, Map<Integer, Long>> nextCursorOffsets, Map<String, Map<Integer, Long>> cursorOffsets,
            Map<TopicPartition, OffsetAndTimestamp> startOffsets) {
        if (!limitReached) {
            return false;
        }
        for (var partition : partitions) {
            var endOffsetRecord = endOffsets.get(partition);
            if (endOffsetRecord == null) {
                continue;
            }
            var endOffset = endOffsetRecord.offset();
            Long currentPosition = null;
            if (nextCursorOffsets.containsKey(partition.topic())
                    && nextCursorOffsets.get(partition.topic()).containsKey(partition.partition())) {
                currentPosition = nextCursorOffsets.get(partition.topic()).get(partition.partition());
            }

            if (currentPosition == null) {
                if (cursorOffsets != null
                        && cursorOffsets.containsKey(partition.topic())
                        && cursorOffsets.get(partition.topic()).containsKey(partition.partition())) {
                    currentPosition = cursorOffsets.get(partition.topic()).get(partition.partition());
                } else if (startOffsets != null && startOffsets.get(partition) != null) {
                    currentPosition = startOffsets.get(partition).offset();
                }
            }

            if (currentPosition != null && currentPosition < endOffset) {
                return true;
            }
        }
        return false;
    }

    private String convertAvroToJson(GenericRecord record) {
        try {
            var resources = avroResources.get();
            var outputStream = resources.outputStream;
            outputStream.reset();

            resources.encoder = EncoderFactory.get()
                    .jsonEncoder(record.getSchema(), outputStream);

            resources.writer.setSchema(record.getSchema());
            resources.writer.write(record, resources.encoder);
            resources.encoder.flush();
            return outputStream.toString(StandardCharsets.UTF_8);
        } catch (java.io.IOException e) {
            logger.error("Error converting Avro to JSON", e);
            throw new KafkaOperationException("Error converting Avro to JSON", e);
        }
    }
}
