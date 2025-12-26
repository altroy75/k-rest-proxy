package com.example.krestproxy.dto;

public record MessageDto(String topicName, Object content, long timestamp, int partition, long offset) {}
