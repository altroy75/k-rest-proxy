package com.example.krestproxy.controller;

import com.example.krestproxy.dto.MessageDto;
import com.example.krestproxy.dto.PaginatedResponse;
import com.example.krestproxy.service.KafkaMessageService;
import com.example.krestproxy.validation.RequestValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.List;

@RestController
@RequestMapping("/api/v1/messages")
public class MessageController {

    private static final Logger logger = LoggerFactory.getLogger(MessageController.class);
    private final KafkaMessageService kafkaMessageService;
    private final RequestValidator requestValidator;

    @Autowired
    public MessageController(KafkaMessageService kafkaMessageService, RequestValidator requestValidator) {
        this.kafkaMessageService = kafkaMessageService;
        this.requestValidator = requestValidator;
    }

    @GetMapping("/{topic}")
    public ResponseEntity<PaginatedResponse<MessageDto>> getMessages(
            @PathVariable String topic,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant startTime,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant endTime,
            @RequestParam(required = false) String cursor) {

        logger.info("GET /api/v1/messages/{} startTime={} endTime={} cursor={}", topic, startTime, endTime, cursor);
        requestValidator.validateTopicName(topic);
        requestValidator.validateTimeRange(startTime, endTime);

        var response = kafkaMessageService.getMessages(topic, startTime, endTime, cursor);
        logger.info("Returning {} messages for topic: {}", response.data().size(), topic);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/{topic}/filter")
    public ResponseEntity<PaginatedResponse<MessageDto>> getMessagesWithExecId(
            @PathVariable String topic,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant startTime,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant endTime,
            @RequestParam String execId,
            @RequestParam(required = false) String cursor) {

        logger.info("GET /api/v1/messages/{}/filter startTime={} endTime={} execId={} cursor={}",
                topic, startTime, endTime, execId, cursor);
        requestValidator.validateTopicName(topic);
        requestValidator.validateTimeRange(startTime, endTime);
        requestValidator.validateExecutionId(execId);

        var response = kafkaMessageService.getMessagesWithExecId(topic, startTime, endTime, execId, cursor);
        logger.info("Returning {} messages for topic: {} with execId: {}", response.data().size(), topic, execId);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/filter")
    public ResponseEntity<List<MessageDto>> getMessagesFromTopics(
            @RequestParam List<String> topics,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant startTime,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant endTime,
            @RequestParam(required = false) String execId) {

        logger.info("GET /api/v1/messages/filter topics={} startTime={} endTime={} execId={}",
                topics, startTime, endTime, execId);
        topics.forEach(requestValidator::validateTopicName);
        requestValidator.validateTimeRange(startTime, endTime);
        if (execId != null) {
            requestValidator.validateExecutionId(execId);
        }

        var messages = kafkaMessageService.getMessagesFromTopics(topics, startTime, endTime, execId);
        logger.info("Returning {} messages for topics: {}", messages.size(), topics);
        return ResponseEntity.ok(messages);
    }

    @GetMapping("/by-execution")
    public ResponseEntity<List<MessageDto>> getMessagesByExecution(
            @RequestParam List<String> topics,
            @RequestParam String execId) {

        logger.info("GET /api/v1/messages/by-execution topics={} execId={}", topics, execId);
        topics.forEach(requestValidator::validateTopicName);
        requestValidator.validateExecutionId(execId);

        var messages = kafkaMessageService.getMessagesForExecution(topics, execId);
        logger.info("Returning {} messages for execution: {}", messages.size(), execId);
        return ResponseEntity.ok(messages);
    }

    @GetMapping("/batch")
    public ResponseEntity<PaginatedResponse<MessageDto>> getBatchMessages(
            @RequestParam List<String> topics,
            @RequestParam(required = false) String cursor) {

        logger.info("GET /api/v1/messages/batch topics={} cursor={}", topics, cursor);
        requestValidator.validateTopicsListSize(topics);
        topics.forEach(requestValidator::validateTopicName);

        var response = kafkaMessageService.getBatchMessages(topics, cursor);
        logger.info("Returning {} batch messages for topics: {}", response.data().size(), topics);
        return ResponseEntity.ok(response);
    }
}
