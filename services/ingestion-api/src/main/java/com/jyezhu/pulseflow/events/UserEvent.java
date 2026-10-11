package com.jyezhu.pulseflow.events;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import tools.jackson.databind.annotation.JsonDeserialize;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@JsonDeserialize(using = UserEventDeserializer.class)
public record UserEvent(
        @NotNull
        @JsonProperty("event_id")
        UUID eventId,

        @NotBlank
        @JsonProperty("user_id")
        String userId,

        @NotBlank
        @JsonProperty("session_id")
        String sessionId,

        @NotNull
        @JsonProperty("event_type")
        EventType eventType,

        @NotBlank
        @JsonProperty("content_id")
        String contentId,

        @NotNull
        @JsonProperty("device_type")
        DeviceType deviceType,

        @NotBlank
        @Pattern(regexp = "^[A-Z]{2}$")
        @JsonProperty("country_code")
        String countryCode,

        @NotNull
        @JsonProperty("event_ts")
        Instant eventTs,

        @JsonProperty("attributes")
        Map<String, @NotNull String> attributes
) {

    public enum EventType {
        PLAY,
        PAUSE,
        SKIP,
        SEARCH,
        LIKE,
        ADD_TO_PLAYLIST,
        PAGE_VIEW
    }

    public enum DeviceType {
        mobile,
        desktop,
        tablet,
        tv
    }
}
