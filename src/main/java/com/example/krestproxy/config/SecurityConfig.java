package com.example.krestproxy.config;

import com.example.krestproxy.filter.RequestHeaderFilter;
import com.example.krestproxy.security.ApiKeyAuthenticationFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.web.SecurityFilterChain;

@Configuration
@EnableWebSecurity
public class SecurityConfig {

    private static final Logger logger = LoggerFactory.getLogger(SecurityConfig.class);
    private final ApiKeyAuthenticationFilter apiKeyAuthenticationFilter;
    private final RequestHeaderFilter requestHeaderFilter;

    public SecurityConfig(ApiKeyAuthenticationFilter apiKeyAuthenticationFilter, RequestHeaderFilter requestHeaderFilter) {
        this.apiKeyAuthenticationFilter = apiKeyAuthenticationFilter;
        this.requestHeaderFilter = requestHeaderFilter;
        logger.info("Security configuration initialized with API key authentication");
    }

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http
                .csrf(AbstractHttpConfigurer::disable)
                .headers(headers -> headers
                        .contentTypeOptions(contentType -> contentType.disable())
                        .xssProtection(xss -> xss.disable())
                        .frameOptions(frame -> frame.deny())
                        .httpStrictTransportSecurity(hsts -> hsts
                                .includeSubDomains(true)
                                .maxAgeInSeconds(31536000)))
                .authorizeHttpRequests(auth -> auth
                        .requestMatchers("/actuator/health", "/actuator/health/**").permitAll()
                        .anyRequest().authenticated())
                .addFilterBefore(apiKeyAuthenticationFilter,
                        org.springframework.security.web.access.intercept.AuthorizationFilter.class)
                .addFilterBefore(requestHeaderFilter, ApiKeyAuthenticationFilter.class);

        logger.info("Security filter chain configured with headers and health endpoint exclusions");
        return http.build();
    }
}
