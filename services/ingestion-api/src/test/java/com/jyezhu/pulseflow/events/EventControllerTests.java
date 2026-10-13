package com.jyezhu.pulseflow.events;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.server.ResponseStatusException;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(EventController.class)
class EventControllerTests {

    private static final String EVENT_ID = "8b3caad0-b708-4cc4-9008-27862f8f8e5e";
    private static final String VALID_EVENT = """
            {
              "event_id": "%s",
              "user_id": "user-123",
              "session_id": "session-456",
              "event_type": "PLAY",
              "content_id": "content-789",
              "device_type": "mobile",
              "country_code": "US",
              "event_ts": "2025-05-04T12:30:00Z"
            }
            """.formatted(EVENT_ID);

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private EventPublisher eventPublisher;

    @Test
    void validEventReturnsAcceptedWithClientEventId() throws Exception {
        mockMvc.perform(post("/v1/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(VALID_EVENT))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.event_id").value(EVENT_ID))
                .andExpect(jsonPath("$.status").value("accepted"));

        verify(eventPublisher).publish(org.mockito.ArgumentMatchers.argThat(
                event -> EVENT_ID.equals(event.eventId().toString())));
    }

    @Test
    void missingRequiredFieldsReturnBadRequest() throws Exception {
        assertBadRequest("{}");
    }

    @Test
    void blankUserIdReturnsBadRequest() throws Exception {
        assertBadRequest(VALID_EVENT.replace("\"user_id\": \"user-123\"", "\"user_id\": \"  \""));
    }

    @Test
    void unsupportedEventTypeReturnsBadRequest() throws Exception {
        assertBadRequest(VALID_EVENT.replace("\"event_type\": \"PLAY\"", "\"event_type\": \"UNKNOWN\""));
    }

    @Test
    void invalidDeviceTypeReturnsBadRequest() throws Exception {
        assertBadRequest(VALID_EVENT.replace("\"device_type\": \"mobile\"", "\"device_type\": \"console\""));
    }

    @Test
    void invalidCountryCodeReturnsBadRequest() throws Exception {
        assertBadRequest(VALID_EVENT.replace("\"country_code\": \"US\"", "\"country_code\": \"usa\""));
    }

    @Test
    void invalidUuidReturnsBadRequest() throws Exception {
        assertBadRequest(VALID_EVENT.replace(EVENT_ID, "not-a-uuid"));
    }

    @Test
    void invalidTimestampReturnsBadRequest() throws Exception {
        assertBadRequest(VALID_EVENT.replace("2025-05-04T12:30:00Z", "not-a-timestamp"));
    }

    @Test
    void timestampWithoutRfc3339SeparatorReturnsBadRequest() throws Exception {
        assertBadRequest(VALID_EVENT.replace("2025-05-04T12:30:00Z", "2025-05-04 12:30:00"));
    }

    @Test
    void unknownJsonPropertyReturnsBadRequest() throws Exception {
        assertBadRequest(VALID_EVENT.replace("\"event_id\":", "\"unexpected\": \"value\", \"event_id\":"));
    }

    @Test
    void numericEventTypeReturnsBadRequest() throws Exception {
        assertBadRequest(VALID_EVENT.replace("\"event_type\": \"PLAY\"", "\"event_type\": 0"));
    }

    @Test
    void numericDeviceTypeReturnsBadRequest() throws Exception {
        assertBadRequest(VALID_EVENT.replace("\"device_type\": \"mobile\"", "\"device_type\": 0"));
    }

    @Test
    void numericUserIdReturnsBadRequest() throws Exception {
        assertBadRequest(VALID_EVENT.replace("\"user_id\": \"user-123\"", "\"user_id\": 123"));
    }

    @Test
    void explicitNullAttributesReturnsBadRequest() throws Exception {
        assertBadRequest(VALID_EVENT.replace("\"event_ts\":", "\"attributes\": null, \"event_ts\":"));
    }

    @Test
    void nonStringAttributeValueReturnsBadRequest() throws Exception {
        assertBadRequest(VALID_EVENT.replace(
                "\"event_ts\":",
                "\"attributes\": {\"rank\": 5}, \"event_ts\":"
        ));
    }

    @Test
    void publishingFailureReturnsServiceUnavailable() throws Exception {
        doThrow(new ResponseStatusException(
                org.springframework.http.HttpStatus.SERVICE_UNAVAILABLE,
                "Event publishing is temporarily unavailable"))
                .when(eventPublisher)
                .publish(org.mockito.ArgumentMatchers.any(UserEvent.class));

        mockMvc.perform(post("/v1/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(VALID_EVENT))
                .andExpect(status().isServiceUnavailable());
    }

    private void assertBadRequest(String json) throws Exception {
        mockMvc.perform(post("/v1/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(eventPublisher);
    }
}
