package com.example.krestproxy.validation;

import com.example.krestproxy.exception.InvalidRequestException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class RequestValidatorTest {

    private RequestValidator requestValidator;

    @BeforeEach
    void setUp() {
        requestValidator = new RequestValidator();
    }

    // Topic Name Validation Tests

    @Test
    void validateTopicName_shouldAcceptValidAlphanumericTopicName() {
        assertDoesNotThrow(() -> requestValidator.validateTopicName("test-topic"));
        assertDoesNotThrow(() -> requestValidator.validateTopicName("test_topic"));
        assertDoesNotThrow(() -> requestValidator.validateTopicName("test.topic"));
        assertDoesNotThrow(() -> requestValidator.validateTopicName("TestTopic123"));
        assertDoesNotThrow(() -> requestValidator.validateTopicName("a"));
    }

    @Test
    void validateTopicName_shouldThrowException_whenTopicNameIsNull() {
        InvalidRequestException exception = assertThrows(InvalidRequestException.class,
                () -> requestValidator.validateTopicName(null));
        assertEquals("Topic name cannot be null or empty", exception.getMessage());
    }

    @Test
    void validateTopicName_shouldThrowException_whenTopicNameIsEmpty() {
        InvalidRequestException exception = assertThrows(InvalidRequestException.class,
                () -> requestValidator.validateTopicName(""));
        assertEquals("Topic name cannot be null or empty", exception.getMessage());
    }

    @Test
    void validateTopicName_shouldThrowException_whenTopicNameIsBlank() {
        InvalidRequestException exception = assertThrows(InvalidRequestException.class,
                () -> requestValidator.validateTopicName("   "));
        assertEquals("Topic name cannot be null or empty", exception.getMessage());
    }

    @Test
    void validateTopicName_shouldThrowException_whenTopicNameExceeds255Characters() {
        String longTopicName = "a".repeat(256);
        InvalidRequestException exception = assertThrows(InvalidRequestException.class,
                () -> requestValidator.validateTopicName(longTopicName));
        assertEquals("Topic name cannot exceed 255 characters", exception.getMessage());
    }

    @Test
    void validateTopicName_shouldAcceptTopicNameWithExactly255Characters() {
        String exactlyMaxLength = "a".repeat(255);
        assertDoesNotThrow(() -> requestValidator.validateTopicName(exactlyMaxLength));
    }

    @Test
    void validateTopicName_shouldThrowException_whenTopicNameContainsSpaces() {
        InvalidRequestException exception = assertThrows(InvalidRequestException.class,
                () -> requestValidator.validateTopicName("test topic"));
        assertEquals("Topic name can only contain alphanumeric characters, dots, underscores, and hyphens",
                exception.getMessage());
    }

    @Test
    void validateTopicName_shouldThrowException_whenTopicNameContainsSpecialCharacters() {
        assertThrows(InvalidRequestException.class,
                () -> requestValidator.validateTopicName("test@topic"));
        assertThrows(InvalidRequestException.class,
                () -> requestValidator.validateTopicName("test#topic"));
        assertThrows(InvalidRequestException.class,
                () -> requestValidator.validateTopicName("test$topic"));
        assertThrows(InvalidRequestException.class,
                () -> requestValidator.validateTopicName("test/topic"));
    }

    // Time Range Validation Tests

    @Test
    void validateTimeRange_shouldAcceptValidTimeRange() {
        Instant start = Instant.parse("2023-01-01T10:00:00Z");
        Instant end = Instant.parse("2023-01-01T12:00:00Z");
        assertDoesNotThrow(() -> requestValidator.validateTimeRange(start, end));
    }

    @Test
    void validateTimeRange_shouldThrowException_whenStartTimeIsNull() {
        Instant end = Instant.parse("2023-01-01T12:00:00Z");
        InvalidRequestException exception = assertThrows(InvalidRequestException.class,
                () -> requestValidator.validateTimeRange(null, end));
        assertEquals("Start time cannot be null", exception.getMessage());
    }

    @Test
    void validateTimeRange_shouldThrowException_whenEndTimeIsNull() {
        Instant start = Instant.parse("2023-01-01T10:00:00Z");
        InvalidRequestException exception = assertThrows(InvalidRequestException.class,
                () -> requestValidator.validateTimeRange(start, null));
        assertEquals("End time cannot be null", exception.getMessage());
    }

    @Test
    void validateTimeRange_shouldThrowException_whenStartTimeIsAfterEndTime() {
        Instant start = Instant.parse("2023-01-01T12:00:00Z");
        Instant end = Instant.parse("2023-01-01T10:00:00Z");
        InvalidRequestException exception = assertThrows(InvalidRequestException.class,
                () -> requestValidator.validateTimeRange(start, end));
        assertEquals("Start time must be before end time", exception.getMessage());
    }

    @Test
    void validateTimeRange_shouldAcceptStartTimeEqualToEndTime() {
        Instant time = Instant.parse("2023-01-01T10:00:00Z");
        assertDoesNotThrow(() -> requestValidator.validateTimeRange(time, time));
    }

    @Test
    void validateTimeRange_shouldThrowException_whenTimeWindowExceeds24Hours() {
        Instant start = Instant.parse("2023-01-01T10:00:00Z");
        Instant end = start.plus(Duration.ofHours(25));
        InvalidRequestException exception = assertThrows(InvalidRequestException.class,
                () -> requestValidator.validateTimeRange(start, end));
        assertEquals("Time window cannot exceed 24 hours", exception.getMessage());
    }

    @Test
    void validateTimeRange_shouldAcceptTimeWindowOfExactly24Hours() {
        Instant start = Instant.parse("2023-01-01T10:00:00Z");
        Instant end = start.plus(Duration.ofHours(24));
        assertDoesNotThrow(() -> requestValidator.validateTimeRange(start, end));
    }

    @Test
    void validateTimeRange_shouldThrowException_whenEndTimeIsMoreThan1HourInFuture() {
        Instant start = Instant.now();
        Instant end = Instant.now().plus(Duration.ofHours(2));
        InvalidRequestException exception = assertThrows(InvalidRequestException.class,
                () -> requestValidator.validateTimeRange(start, end));
        assertEquals("End time cannot be more than 1 hour in the future", exception.getMessage());
    }

    @Test
    void validateTimeRange_shouldAcceptEndTimeExactly1HourInFuture() {
        Instant start = Instant.now().minus(Duration.ofHours(1));
        Instant end = Instant.now().plus(Duration.ofHours(1));
        assertDoesNotThrow(() -> requestValidator.validateTimeRange(start, end));
    }

    @Test
    void validateTimeRange_shouldAcceptPastTimeRanges() {
        Instant start = Instant.parse("2023-01-01T10:00:00Z");
        Instant end = Instant.parse("2023-01-01T12:00:00Z");
        assertDoesNotThrow(() -> requestValidator.validateTimeRange(start, end));
    }

    // Execution ID Validation Tests

    @Test
    void validateExecutionId_shouldAcceptValidExecutionIds() {
        assertDoesNotThrow(() -> requestValidator.validateExecutionId("exec-123"));
        assertDoesNotThrow(() -> requestValidator.validateExecutionId("exec_123"));
        assertDoesNotThrow(() -> requestValidator.validateExecutionId("ExecId123"));
        assertDoesNotThrow(() -> requestValidator.validateExecutionId("a"));
        assertDoesNotThrow(() -> requestValidator.validateExecutionId("ABC-123-XYZ"));
    }

    @Test
    void validateExecutionId_shouldThrowException_whenExecutionIdIsNull() {
        InvalidRequestException exception = assertThrows(InvalidRequestException.class,
                () -> requestValidator.validateExecutionId(null));
        assertEquals("Execution ID cannot be null or empty", exception.getMessage());
    }

    @Test
    void validateExecutionId_shouldThrowException_whenExecutionIdIsEmpty() {
        InvalidRequestException exception = assertThrows(InvalidRequestException.class,
                () -> requestValidator.validateExecutionId(""));
        assertEquals("Execution ID cannot be null or empty", exception.getMessage());
    }

    @Test
    void validateExecutionId_shouldThrowException_whenExecutionIdIsBlank() {
        InvalidRequestException exception = assertThrows(InvalidRequestException.class,
                () -> requestValidator.validateExecutionId("   "));
        assertEquals("Execution ID cannot be null or empty", exception.getMessage());
    }

    @Test
    void validateExecutionId_shouldThrowException_whenExecutionIdExceeds100Characters() {
        String longExecId = "a".repeat(101);
        InvalidRequestException exception = assertThrows(InvalidRequestException.class,
                () -> requestValidator.validateExecutionId(longExecId));
        assertEquals("Execution ID cannot exceed 100 characters", exception.getMessage());
    }

    @Test
    void validateExecutionId_shouldAcceptExecutionIdWithExactly100Characters() {
        String exactlyMaxLength = "a".repeat(100);
        assertDoesNotThrow(() -> requestValidator.validateExecutionId(exactlyMaxLength));
    }

    @Test
    void validateExecutionId_shouldThrowException_whenExecutionIdContainsInvalidCharacters() {
        assertThrows(InvalidRequestException.class,
                () -> requestValidator.validateExecutionId("exec@123"));
        assertThrows(InvalidRequestException.class,
                () -> requestValidator.validateExecutionId("exec#123"));
        assertThrows(InvalidRequestException.class,
                () -> requestValidator.validateExecutionId("exec 123"));
        assertThrows(InvalidRequestException.class,
                () -> requestValidator.validateExecutionId("exec.123"));
    }
}
