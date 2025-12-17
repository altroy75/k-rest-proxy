package com.example.krestproxy.filter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.MDC;

import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

class RequestHeaderFilterTest {

    @InjectMocks
    private RequestHeaderFilter requestHeaderFilter;

    @Mock
    private HttpServletRequest request;

    @Mock
    private HttpServletResponse response;

    @Mock
    private FilterChain filterChain;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @AfterEach
    void tearDown() {
        MDC.clear();
    }

    @Test
    void testValidHeaders() throws Exception {
        when(request.getRequestURI()).thenReturn("/api/v1/messages");
        when(request.getHeader("Request-ID")).thenReturn("req-123");
        when(request.getHeader("RLT-ID")).thenReturn("12345");

        requestHeaderFilter.doFilterInternal(request, response, filterChain);

        verify(filterChain).doFilter(request, response);
        verify(response).addHeader("RLT-ID", "12345");

        // MDC checks are hard to verify directly as MDC is static,
        // but if execution reached filterChain.doFilter without error,
        // and we put it in before, it should be there.
        // We can verify that we didn't send an error.
        verify(response, never()).sendError(anyInt(), anyString());
    }

    @Test
    void testMissingRequestId() throws Exception {
        when(request.getRequestURI()).thenReturn("/api/v1/messages");
        when(request.getHeader("Request-ID")).thenReturn(null);
        when(request.getHeader("RLT-ID")).thenReturn("12345");

        requestHeaderFilter.doFilterInternal(request, response, filterChain);

        verify(response).sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing Request-ID");
        verify(filterChain, never()).doFilter(request, response);
    }

    @Test
    void testMissingRltId() throws Exception {
        when(request.getRequestURI()).thenReturn("/api/v1/messages");
        when(request.getHeader("Request-ID")).thenReturn("req-123");
        when(request.getHeader("RLT-ID")).thenReturn(null);

        requestHeaderFilter.doFilterInternal(request, response, filterChain);

        verify(response).sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing RLT-ID");
        verify(filterChain, never()).doFilter(request, response);
    }

    @Test
    void testInvalidRltId() throws Exception {
        when(request.getRequestURI()).thenReturn("/api/v1/messages");
        when(request.getHeader("Request-ID")).thenReturn("req-123");
        when(request.getHeader("RLT-ID")).thenReturn("not-an-int");

        requestHeaderFilter.doFilterInternal(request, response, filterChain);

        verify(response).sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid RLT-ID");
        verify(filterChain, never()).doFilter(request, response);
    }

    @Test
    void testActuatorExclusion() throws Exception {
        when(request.getRequestURI()).thenReturn("/actuator/health");
        // No headers provided
        when(request.getHeader("Request-ID")).thenReturn(null);
        when(request.getHeader("RLT-ID")).thenReturn(null);

        requestHeaderFilter.doFilterInternal(request, response, filterChain);

        // Should proceed without error
        verify(filterChain).doFilter(request, response);
        verify(response, never()).sendError(anyInt(), anyString());
    }
}
