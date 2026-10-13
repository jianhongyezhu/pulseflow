package com.jyezhu.pulseflow.events;

import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class EventPublishingIntegrationTests {

    private static final String VALID_EVENT = """
            {
              "event_id": "8b3caad0-b708-4cc4-9008-27862f8f8e5e",
              "user_id": "user-123",
              "session_id": "session-456",
              "event_type": "PLAY",
              "content_id": "content-789",
              "device_type": "mobile",
              "country_code": "US",
              "event_ts": "2025-05-04T12:30:00Z"
            }
            """;

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private MeterRegistry meterRegistry;

    @MockitoBean
    private KafkaTemplate<String, UserEvent> kafkaTemplate;

    @Test
    void mapsKafkaFailureTo503AndUnexpectedFailureToSafe500() throws Exception {
        double publishedBefore = meterRegistry.counter("pulseflow.events.published").count();
        double failuresBefore = meterRegistry.counter("pulseflow.events.publish.failures").count();
        when(kafkaTemplate.send(anyString(), anyString(), any(UserEvent.class)))
                .thenReturn(CompletableFuture.failedFuture(new KafkaException("broker unavailable")))
                .thenThrow(new IllegalStateException("internal diagnostic sentinel"));

        postEvent().andExpect(status().isServiceUnavailable());
        postEvent()
                .andExpect(status().isInternalServerError())
                .andExpect(content().string(org.hamcrest.Matchers.not(
                        org.hamcrest.Matchers.containsString("internal diagnostic sentinel"))));

        verify(kafkaTemplate, times(2)).send(anyString(), anyString(), any(UserEvent.class));
        assertEquals(publishedBefore, meterRegistry.counter("pulseflow.events.published").count());
        assertEquals(
                failuresBefore + 2,
                meterRegistry.counter("pulseflow.events.publish.failures").count()
        );
    }

    @Test
    void prometheusEndpointExposesCompletedIngestionRequestHistogramBuckets() throws Exception {
        when(kafkaTemplate.send(anyString(), anyString(), any(UserEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        postEvent().andExpect(status().isAccepted());

        String metrics = mockMvc.perform(get("/actuator/prometheus"))
                .andExpect(status().isOk())
                .andReturn()
                .getResponse()
                .getContentAsString();

        boolean ingestionBucketExists = Arrays.stream(metrics.split("\\R"))
                .filter(line -> line.startsWith("http_server_requests_seconds_bucket{"))
                .anyMatch(line -> line.contains("uri=\"/v1/events\""));
        assertTrue(
                ingestionBucketExists,
                "Expected a completed-request histogram bucket for POST /v1/events"
        );
    }

    private org.springframework.test.web.servlet.ResultActions postEvent() throws Exception {
        return mockMvc.perform(post("/v1/events")
                .contentType(MediaType.APPLICATION_JSON)
                .content(VALID_EVENT));
    }
}
