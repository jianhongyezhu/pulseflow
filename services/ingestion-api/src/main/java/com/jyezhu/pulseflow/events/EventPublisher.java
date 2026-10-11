package com.jyezhu.pulseflow.events;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class EventPublisher {

    private static final Logger logger = LoggerFactory.getLogger(EventPublisher.class);
    private static final long ACKNOWLEDGEMENT_TIMEOUT_SECONDS = 3;

    private final KafkaTemplate<String, UserEvent> kafkaTemplate;
    private final String topic;
    private final Counter publishedEvents;
    private final Counter publishingFailures;

    public EventPublisher(
            KafkaTemplate<String, UserEvent> kafkaTemplate,
            MeterRegistry meterRegistry,
            @Value("${pulseflow.kafka.topic}") String topic
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
        this.publishedEvents = Counter.builder("pulseflow.events.published")
                .description("User events acknowledged by Kafka")
                .register(meterRegistry);
        this.publishingFailures = Counter.builder("pulseflow.events.publish.failures")
                .description("User event publishing failures")
                .register(meterRegistry);
    }

    public void publish(UserEvent event) {
        try {
            kafkaTemplate.send(topic, event.userId(), event)
                    .get(ACKNOWLEDGEMENT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            publishedEvents.increment();
        } catch (InterruptedException exception) {
            publishingFailures.increment();
            Thread.currentThread().interrupt();
            throw serviceUnavailable();
        } catch (ExecutionException | TimeoutException | KafkaException exception) {
            publishingFailures.increment();
            throw serviceUnavailable();
        } catch (RuntimeException exception) {
            publishingFailures.increment();
            logger.error("Unexpected runtime failure while publishing a user event", exception);
            throw new ResponseStatusException(
                    HttpStatus.INTERNAL_SERVER_ERROR,
                    "An unexpected error occurred while publishing the event"
            );
        }
    }

    private ResponseStatusException serviceUnavailable() {
        return new ResponseStatusException(
                HttpStatus.SERVICE_UNAVAILABLE,
                "Event publishing is temporarily unavailable"
        );
    }
}
