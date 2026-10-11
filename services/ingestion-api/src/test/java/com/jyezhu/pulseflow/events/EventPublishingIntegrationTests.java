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

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
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
        when(kafkaTemplate.send(anyString(), anyString(), any(UserEvent.class)))
                .thenReturn(CompletableFuture.failedFuture(new KafkaException("broker unavailable")))
                .thenThrow(new IllegalStateException("internal diagnostic sentinel"));

        postEvent().andExpect(status().isServiceUnavailable());
        postEvent()
                .andExpect(status().isInternalServerError())
                .andExpect(content().string(org.hamcrest.Matchers.not(
                        org.hamcrest.Matchers.containsString("internal diagnostic sentinel"))));

        verify(kafkaTemplate, times(2)).send(anyString(), anyString(), any(UserEvent.class));
        assertEquals(0.0, meterRegistry.counter("pulseflow.events.published").count());
        assertEquals(2.0, meterRegistry.counter("pulseflow.events.publish.failures").count());
    }

    private org.springframework.test.web.servlet.ResultActions postEvent() throws Exception {
        return mockMvc.perform(post("/v1/events")
                .contentType(MediaType.APPLICATION_JSON)
                .content(VALID_EVENT));
    }
}
