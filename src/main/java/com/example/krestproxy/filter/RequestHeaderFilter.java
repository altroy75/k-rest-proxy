package com.example.krestproxy.filter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

@Component
public class RequestHeaderFilter extends OncePerRequestFilter {

    private static final Logger logger = LoggerFactory.getLogger(RequestHeaderFilter.class);
    private static final String REQUEST_ID_HEADER = "Request-ID";
    private static final String RLT_ID_HEADER = "RLT-ID";

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {

        String path = request.getRequestURI();

        // Exclude actuator endpoints
        if (path.startsWith("/actuator")) {
            filterChain.doFilter(request, response);
            return;
        }

        String requestId = request.getHeader(REQUEST_ID_HEADER);
        String rltIdStr = request.getHeader(RLT_ID_HEADER);

        if (requestId == null || requestId.isBlank()) {
            logger.warn("Missing {} header", REQUEST_ID_HEADER);
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing " + REQUEST_ID_HEADER);
            return;
        }

        if (rltIdStr == null || rltIdStr.isBlank()) {
            logger.warn("Missing {} header", RLT_ID_HEADER);
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing " + RLT_ID_HEADER);
            return;
        }

        try {
            // Validate RLT-ID is an integer
            Integer.parseInt(rltIdStr);
        } catch (NumberFormatException e) {
            logger.warn("Invalid {} header: {}", RLT_ID_HEADER, rltIdStr);
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid " + RLT_ID_HEADER);
            return;
        }

        try {
            MDC.put(REQUEST_ID_HEADER, requestId);
            MDC.put(RLT_ID_HEADER, rltIdStr);

            // Add RLT-ID to response
            response.addHeader(RLT_ID_HEADER, rltIdStr);

            filterChain.doFilter(request, response);
        } finally {
            MDC.remove(REQUEST_ID_HEADER);
            MDC.remove(RLT_ID_HEADER);
        }
    }
}
