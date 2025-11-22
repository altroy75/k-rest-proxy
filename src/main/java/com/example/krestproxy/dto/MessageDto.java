package com.example.krestproxy.dto;

public record MessageDto(String content, long timestamp, int partition, long offset) {}
