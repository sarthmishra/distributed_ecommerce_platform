package com.ecommerce.platform.observability;

import com.ecommerce.platform.common.filter.CorrelationIdFilter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

public class CorrelationIdFilterTest {

    private CorrelationIdFilter filter;

    @BeforeEach
    void setUp() {
        filter = new CorrelationIdFilter();
        MDC.clear();
    }

    @Test
    @DisplayName("Filter generates UUID when X-Correlation-ID header is missing")
    void testFilterGeneratesCorrelationIdWhenMissing() throws ServletException, IOException {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/api/v1/orders");
        MockHttpServletResponse response = new MockHttpServletResponse();
        AtomicReference<String> mdcValueDuringProcessing = new AtomicReference<>();

        FilterChain chain = (req, res) -> {
            mdcValueDuringProcessing.set(MDC.get(CorrelationIdFilter.CORRELATION_ID_MDC_KEY));
        };

        filter.doFilter(request, response, chain);

        String responseHeader = response.getHeader(CorrelationIdFilter.CORRELATION_ID_HEADER);
        assertNotNull(responseHeader, "Expected X-Correlation-ID header in HTTP response");
        assertFalse(responseHeader.isBlank());
        assertEquals(responseHeader, mdcValueDuringProcessing.get(), "MDC correlationId should match response header");
        assertNull(MDC.get(CorrelationIdFilter.CORRELATION_ID_MDC_KEY), "MDC context must be cleaned up after request completes");
    }

    @Test
    @DisplayName("Filter preserves supplied X-Correlation-ID header")
    void testFilterPreservesSuppliedCorrelationId() throws ServletException, IOException {
        String expectedCorrelationId = "TEST-CORRELATION-ID-9999";
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/api/v1/orders");
        request.addHeader(CorrelationIdFilter.CORRELATION_ID_HEADER, expectedCorrelationId);
        MockHttpServletResponse response = new MockHttpServletResponse();
        AtomicReference<String> mdcValueDuringProcessing = new AtomicReference<>();

        FilterChain chain = (req, res) -> {
            mdcValueDuringProcessing.set(MDC.get(CorrelationIdFilter.CORRELATION_ID_MDC_KEY));
        };

        filter.doFilter(request, response, chain);

        assertEquals(expectedCorrelationId, response.getHeader(CorrelationIdFilter.CORRELATION_ID_HEADER));
        assertEquals(expectedCorrelationId, mdcValueDuringProcessing.get());
        assertNull(MDC.get(CorrelationIdFilter.CORRELATION_ID_MDC_KEY), "MDC context must be cleaned up after request completes");
    }
}
