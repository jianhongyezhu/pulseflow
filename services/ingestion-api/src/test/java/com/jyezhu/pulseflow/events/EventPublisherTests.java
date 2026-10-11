package com.jyezhu.pulseflow.events;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.server.ResponseStatusException;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EventPublisherTests {

    private static final String TOPIC = "user-behaviour.v1";
    private static final UserEvent EVENT = new UserEvent(
            UUID.fromString("8b3caad0-b708-4cc4-9008-27862f8f8e5e"),
            "user-123",
            "session-456",
            UserEvent.EventType.PLAY,
            "content-789",
            UserEvent.DeviceType.mobile,
            "US",
            Instant.parse("2025-05-04T12:30:00Z"),
            null
    );

    @Mock
    private KafkaTemplate<String, UserEvent> kafkaTemplate;

    private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    @AfterEach
    void closeMeterRegistry() {
        meterRegistry.close();
    }

    @Test
    void publishesToConfiguredTopicUsingUserIdAsKeyAfterAcknowledgement() {
        when(kafkaTemplate.send(TOPIC, EVENT.userId(), EVENT))
                .thenReturn(CompletableFuture.completedFuture(null));

        publisher().publish(EVENT);

        verify(kafkaTemplate).send(TOPIC, EVENT.userId(), EVENT);
        assertEquals(1.0, meterRegistry.counter("pulseflow.events.published").count());
        assertEquals(0.0, meterRegistry.counter("pulseflow.events.publish.failures").count());
    }

    @Test
    void kafkaFailureReturnsServiceUnavailableAndIncrementsOnlyFailureMetric() {
        when(kafkaTemplate.send(TOPIC, EVENT.userId(), EVENT))
                .thenReturn(CompletableFuture.failedFuture(new KafkaException("broker unavailable")));

        ResponseStatusException exception = assertThrows(
                ResponseStatusException.class,
                () -> publisher().publish(EVENT)
        );

        assertEquals(503, exception.getStatusCode().value());
        assertEquals("Event publishing is temporarily unavailable", exception.getReason());
        assertEquals(0.0, meterRegistry.counter("pulseflow.events.published").count());
        assertEquals(1.0, meterRegistry.counter("pulseflow.events.publish.failures").count());
    }

    @Test
    void acknowledgementTimeoutReturnsServiceUnavailable() throws Exception {
        CompletableFuture<SendResult<String, UserEvent>> future = org.mockito.Mockito.mock(CompletableFuture.class);
        when(kafkaTemplate.send(TOPIC, EVENT.userId(), EVENT)).thenReturn(future);
        when(future.get(3, TimeUnit.SECONDS)).thenThrow(new TimeoutException("timed out"));

        ResponseStatusException exception = assertThrows(
                ResponseStatusException.class,
                () -> publisher().publish(EVENT)
        );

        assertEquals(503, exception.getStatusCode().value());
        assertEquals(0.0, meterRegistry.counter("pulseflow.events.published").count());
        assertEquals(1.0, meterRegistry.counter("pulseflow.events.publish.failures").count());
    }

    @Test
    void unexpectedRuntimeFailureReturnsInternalServerErrorWithoutLeakingDetails() {
        when(kafkaTemplate.send(TOPIC, EVENT.userId(), EVENT))
                .thenThrow(new IllegalStateException("internal diagnostic sentinel"));

        ResponseStatusException exception = assertThrows(
                ResponseStatusException.class,
                () -> publisher().publish(EVENT)
        );

        assertEquals(500, exception.getStatusCode().value());
        assertEquals("An unexpected error occurred while publishing the event", exception.getReason());
        assertEquals(0.0, meterRegistry.counter("pulseflow.events.published").count());
        assertEquals(1.0, meterRegistry.counter("pulseflow.events.publish.failures").count());
    }

    @Test
    void interruptedPublishRestoresInterruptStatus() throws Exception {
        CompletableFuture<SendResult<String, UserEvent>> future = org.mockito.Mockito.mock(CompletableFuture.class);
        when(kafkaTemplate.send(TOPIC, EVENT.userId(), EVENT)).thenReturn(future);
        when(future.get(3, TimeUnit.SECONDS)).thenThrow(new InterruptedException());

        ResponseStatusException exception = assertThrows(
                ResponseStatusException.class,
                () -> publisher().publish(EVENT)
        );

        try {
            assertEquals(503, exception.getStatusCode().value());
            assertTrue(Thread.currentThread().isInterrupted());
            assertEquals(0.0, meterRegistry.counter("pulseflow.events.published").count());
            assertEquals(1.0, meterRegistry.counter("pulseflow.events.publish.failures").count());
        } finally {
            Thread.interrupted();
        }
    }

    private EventPublisher publisher() {
        return new EventPublisher(kafkaTemplate, meterRegistry, TOPIC);
    }
}
