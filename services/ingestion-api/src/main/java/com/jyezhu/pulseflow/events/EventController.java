package com.jyezhu.pulseflow.events;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/v1/events")
public class EventController {

    private final EventPublisher eventPublisher;

    public EventController(EventPublisher eventPublisher) {
        this.eventPublisher = eventPublisher;
    }

    @PostMapping
    public ResponseEntity<AcceptedEventResponse> publish(@Valid @RequestBody UserEvent event) {
        eventPublisher.publish(event);
        return ResponseEntity.accepted()
                .body(new AcceptedEventResponse(event.eventId(), "accepted"));
    }

    public record AcceptedEventResponse(
            @JsonProperty("event_id") UUID eventId,
            String status
    ) {
    }
}
