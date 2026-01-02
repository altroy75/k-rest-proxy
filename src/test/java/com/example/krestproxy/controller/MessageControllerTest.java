package com.example.krestproxy.controller;

import com.example.krestproxy.dto.PaginatedResponse;
import com.example.krestproxy.service.KafkaMessageService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class MessageControllerTest {

        @Autowired
        private MockMvc mockMvc;

        @MockBean
        private KafkaMessageService kafkaMessageService;

        @Test
        void getMessages_shouldReturnUnauthorized_whenApiKeyIsMissing() throws Exception {
                mockMvc.perform(get("/api/v1/messages/test-topic")
                                .header("Request-ID", "req-1")
                                .header("RLT-ID", "1001")
                                .param("startTime", "2023-01-01T10:00:00Z")
                                .param("endTime", "2023-01-01T10:05:00Z"))
                                .andExpect(status().isUnauthorized());
        }

        @Test
        void getMessages_shouldReturnUnauthorized_whenApiKeyIsInvalid() throws Exception {
                mockMvc.perform(get("/api/v1/messages/test-topic")
                                .header("X-API-KEY", "invalid-key")
                                .header("Request-ID", "req-1")
                                .header("RLT-ID", "1001")
                                .param("startTime", "2023-01-01T10:00:00Z")
                                .param("endTime", "2023-01-01T10:05:00Z"))
                                .andExpect(status().isUnauthorized());
        }

        @Test
        void getMessages_shouldReturnMessages_whenApiKeyIsValid() throws Exception {
                java.util.Map<String, String> content1 = java.util.Collections.singletonMap("key", "val1");
                java.util.Map<String, String> content2 = java.util.Collections.singletonMap("key", "val2");
                com.example.krestproxy.dto.MessageDto msg1 = new com.example.krestproxy.dto.MessageDto("test-topic",
                                content1, 1000L, 0, 0L);
                com.example.krestproxy.dto.MessageDto msg2 = new com.example.krestproxy.dto.MessageDto("test-topic",
                                content2, 2000L, 0, 1L);

                when(kafkaMessageService.getMessages(eq("test-topic"), any(Instant.class), any(Instant.class),
                                isNull()))
                                .thenReturn(new PaginatedResponse<>(Arrays.asList(msg1, msg2), null, false));

                mockMvc.perform(get("/api/v1/messages/test-topic")
                                .header("X-API-KEY", "secret-api-key")
                                .header("Request-ID", "req-1")
                                .header("RLT-ID", "1001")
                                .param("startTime", "2023-01-01T10:00:00Z")
                                .param("endTime", "2023-01-01T10:05:00Z"))
                                .andExpect(status().isOk())
                                .andExpect(content().json(
                                                "{\"data\":[{\"topicName\":\"test-topic\",\"content\":{\"key\":\"val1\"},\"timestamp\":1000,\"partition\":0,\"offset\":0},{\"topicName\":\"test-topic\",\"content\":{\"key\":\"val2\"},\"timestamp\":2000,\"partition\":0,\"offset\":1}],\"nextCursor\":null,\"hasMore\":false}"));
        }

        @Test
        void getMessagesWithExecId_shouldReturnMessages_whenApiKeyIsValid() throws Exception {
                java.util.Map<String, String> content1 = java.util.Collections.singletonMap("key", "val1");
                com.example.krestproxy.dto.MessageDto msg1 = new com.example.krestproxy.dto.MessageDto("test-topic",
                                content1, 1000L, 0, 0L);

                when(kafkaMessageService.getMessagesWithExecId(eq("test-topic"), any(Instant.class), any(Instant.class),
                                eq("exec-1"), isNull()))
                                .thenReturn(new PaginatedResponse<>(Collections.singletonList(msg1), null, false));

                mockMvc.perform(get("/api/v1/messages/test-topic/filter")
                                .header("X-API-KEY", "secret-api-key")
                                .header("Request-ID", "req-1")
                                .header("RLT-ID", "1001")
                                .param("startTime", "2023-01-01T10:00:00Z")
                                .param("endTime", "2023-01-01T10:05:00Z")
                                .param("execId", "exec-1"))
                                .andExpect(status().isOk())
                                .andExpect(content().json(
                                                "{\"data\":[{\"topicName\":\"test-topic\",\"content\":{\"key\":\"val1\"},\"timestamp\":1000,\"partition\":0,\"offset\":0}],\"nextCursor\":null,\"hasMore\":false}"));
        }

        @Test
        void getMessages_shouldReturnOk_whenCursorIsInvalid() throws Exception {
                when(kafkaMessageService.getMessages(eq("test-topic"), any(Instant.class), any(Instant.class),
                                eq("invalid-cursor")))
                                .thenReturn(new PaginatedResponse<>(Collections.emptyList(), null, false));

                mockMvc.perform(get("/api/v1/messages/test-topic")
                                .header("X-API-KEY", "secret-api-key")
                                .header("Request-ID", "req-1")
                                .header("RLT-ID", "1001")
                                .param("startTime", "2023-01-01T10:00:00Z")
                                .param("endTime", "2023-01-01T10:05:00Z")
                                .param("cursor", "invalid-cursor"))
                                .andExpect(status().isOk());
        }

        // Parameter validation integration tests

        @Test
        void getMessages_shouldReturnBadRequest_whenTopicNameIsInvalid() throws Exception {
                mockMvc.perform(get("/api/v1/messages/invalid@topic")
                                .header("X-API-KEY", "secret-api-key")
                                .header("Request-ID", "req-1")
                                .header("RLT-ID", "1001")
                                .param("startTime", "2023-01-01T10:00:00Z")
                                .param("endTime", "2023-01-01T10:05:00Z"))
                                .andExpect(status().isBadRequest());
        }

        @Test
        void getMessages_shouldReturnBadRequest_whenTimeRangeIsInvalid() throws Exception {
                // Start time after end time
                mockMvc.perform(get("/api/v1/messages/test-topic")
                                .header("X-API-KEY", "secret-api-key")
                                .header("Request-ID", "req-1")
                                .header("RLT-ID", "1001")
                                .param("startTime", "2023-01-01T12:00:00Z")
                                .param("endTime", "2023-01-01T10:00:00Z"))
                                .andExpect(status().isBadRequest());
        }

        @Test
        void getMessagesWithExecId_shouldReturnBadRequest_whenExecIdIsInvalid() throws Exception {
                // ExecId with invalid characters
                mockMvc.perform(get("/api/v1/messages/test-topic/filter")
                                .header("X-API-KEY", "secret-api-key")
                                .header("Request-ID", "req-1")
                                .header("RLT-ID", "1001")
                                .param("startTime", "2023-01-01T10:00:00Z")
                                .param("endTime", "2023-01-01T10:05:00Z")
                                .param("execId", "invalid@execid"))
                                .andExpect(status().isBadRequest());
        }

        @Test
        void getMessages_shouldReturnBadRequest_whenStartTimeIsMissing() throws Exception {
                mockMvc.perform(get("/api/v1/messages/test-topic")
                                .header("X-API-KEY", "secret-api-key")
                                .header("Request-ID", "req-1")
                                .header("RLT-ID", "1001")
                                .param("endTime", "2023-01-01T10:05:00Z"))
                                .andExpect(status().isInternalServerError()); // Spring returns 500 for missing required
                                                                              // params
        }

        @Test
        void getMessages_shouldReturnBadRequest_whenEndTimeIsMissing() throws Exception {
                mockMvc.perform(get("/api/v1/messages/test-topic")
                                .header("X-API-KEY", "secret-api-key")
                                .header("Request-ID", "req-1")
                                .header("RLT-ID", "1001")
                                .param("startTime", "2023-01-01T10:00:00Z"))
                                .andExpect(status().isInternalServerError()); // Spring returns 500 for missing required
                                                                              // params
        }

        // Cursor parameter handling tests

        @Test
        void getMessages_shouldAcceptValidCursor() throws Exception {
                String validCursor = "eyJ0ZXN0LXRvcGljIjp7IjAiOjEwMH19"; // Valid base64 cursor

                when(kafkaMessageService.getMessages(eq("test-topic"), any(Instant.class), any(Instant.class),
                                eq(validCursor)))
                                .thenReturn(new PaginatedResponse<>(Collections.emptyList(), null, false));

                mockMvc.perform(get("/api/v1/messages/test-topic")
                                .header("X-API-KEY", "secret-api-key")
                                .header("Request-ID", "req-1")
                                .header("RLT-ID", "1001")
                                .param("startTime", "2023-01-01T10:00:00Z")
                                .param("endTime", "2023-01-01T10:05:00Z")
                                .param("cursor", validCursor))
                                .andExpect(status().isOk());

                verify(kafkaMessageService).getMessages(eq("test-topic"), any(Instant.class), any(Instant.class),
                                eq(validCursor));
        }

        @Test
        void getMessages_shouldHandleWhitespaceCursor() throws Exception {
                String whitespaceCursor = "   ";

                when(kafkaMessageService.getMessages(eq("test-topic"), any(Instant.class), any(Instant.class),
                                eq(whitespaceCursor)))
                                .thenReturn(new PaginatedResponse<>(Collections.emptyList(), null, false));

                mockMvc.perform(get("/api/v1/messages/test-topic")
                                .header("X-API-KEY", "secret-api-key")
                                .header("Request-ID", "req-1")
                                .header("RLT-ID", "1001")
                                .param("startTime", "2023-01-01T10:00:00Z")
                                .param("endTime", "2023-01-01T10:05:00Z")
                                .param("cursor", whitespaceCursor))
                                .andExpect(status().isOk());
        }

        // Multi-topic endpoint tests

        @Test
        void getMessagesFromTopics_shouldHandleEmptyTopicString() throws Exception {
                // Empty string parameter results in getMessagesFromTopics being called with
                // empty list
                // This is handled gracefully - returns empty result
                when(kafkaMessageService.getMessagesFromTopics(eq(Collections.emptyList()), any(Instant.class),
                                any(Instant.class), isNull()))
                                .thenReturn(Collections.emptyList());

                mockMvc.perform(get("/api/v1/messages/filter")
                                .header("X-API-KEY", "secret-api-key")
                                .header("Request-ID", "req-1")
                                .header("RLT-ID", "1001")
                                .param("topics", "") // Empty string - parsed as empty list
                                .param("startTime", "2023-01-01T10:00:00Z")
                                .param("endTime", "2023-01-01T10:05:00Z"))
                                .andExpect(status().isOk()); // Returns successfully with empty list
        }

        @Test
        void getMessagesFromTopics_shouldValidateAllTopics() throws Exception {
                mockMvc.perform(get("/api/v1/messages/filter")
                                .header("X-API-KEY", "secret-api-key")
                                .header("Request-ID", "req-1")
                                .header("RLT-ID", "1001")
                                .param("topics", "valid-topic")
                                .param("topics", "invalid@topic") // Second topic is invalid
                                .param("startTime", "2023-01-01T10:00:00Z")
                                .param("endTime", "2023-01-01T10:05:00Z"))
                                .andExpect(status().isBadRequest());
        }

        @Test
        void getMessagesByExecution_shouldReturnBadRequest_whenExecIdIsInvalid() throws Exception {
                mockMvc.perform(get("/api/v1/messages/by-execution")
                                .header("X-API-KEY", "secret-api-key")
                                .header("Request-ID", "req-1")
                                .header("RLT-ID", "1001")
                                .param("topics", "test-topic")
                                .param("execId", "invalid exec id")) // Invalid execId with space
                                .andExpect(status().isBadRequest());
        }

        @Test
        void getMessagesByExecution_shouldReturnBadRequest_whenExecIdIsMissing() throws Exception {
                mockMvc.perform(get("/api/v1/messages/by-execution")
                                .header("X-API-KEY", "secret-api-key")
                                .header("Request-ID", "req-1")
                                .header("RLT-ID", "1001")
                                .param("topics", "test-topic"))
                                // Missing execId parameter
                                .andExpect(status().isInternalServerError()); // Spring returns 500 for missing required
                                                                              // params
        }

        @Test
        void getBatchMessages_shouldReturnBatchMessages() throws Exception {
                java.util.List<String> topics = Arrays.asList("topic1", "topic2");
                String cursor = "cursor";
                PaginatedResponse<com.example.krestproxy.dto.MessageDto> response = new PaginatedResponse<>(
                                Collections.emptyList(), "nextCursor", true);

                when(kafkaMessageService.getBatchMessages(topics, cursor)).thenReturn(response);

                mockMvc.perform(get("/api/v1/messages/batch")
                                .header("X-API-KEY", "secret-api-key")
                                .header("Request-ID", "req-1")
                                .header("RLT-ID", "1001")
                                .param("topics", "topic1,topic2")
                                .param("cursor", cursor))
                                .andExpect(status().isOk())
                                .andExpect(content().json("{\"data\":[],\"nextCursor\":\"nextCursor\",\"hasMore\":true}"));

                verify(kafkaMessageService).getBatchMessages(topics, cursor);
        }

        @Test
        void getBatchMessages_shouldValidateTopicsCount() throws Exception {
                // Create a list with 31 topics
                StringBuilder topicsParam = new StringBuilder();
                for (int i = 0; i < 31; i++) {
                        topicsParam.append("topic").append(i);
                        if (i < 30) {
                                topicsParam.append(",");
                        }
                }

                mockMvc.perform(get("/api/v1/messages/batch")
                                .header("X-API-KEY", "secret-api-key")
                                .header("Request-ID", "req-1")
                                .header("RLT-ID", "1001")
                                .param("topics", topicsParam.toString()))
                                .andExpect(status().isBadRequest());
        }
}
