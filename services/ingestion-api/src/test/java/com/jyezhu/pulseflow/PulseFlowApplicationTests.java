package com.jyezhu.pulseflow;

import com.jyezhu.pulseflow.events.UserEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

@SpringBootTest
class PulseFlowApplicationTests {

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void contextLoads() {
    }

    @Test
    void rejectsUnknownUserEventProperties() {
        Assertions.assertThrows(JacksonException.class,
                () -> objectMapper.readValue("{\"unexpected\":\"value\"}", UserEvent.class));
    }

    @Test
    void rejectsInvalidUuidAndTimestampDuringJsonBinding() {
        Assertions.assertThrows(JacksonException.class,
                () -> objectMapper.readValue("{\"event_id\":\"invalid\"}", UserEvent.class));
        Assertions.assertThrows(JacksonException.class,
                () -> objectMapper.readValue(
                        "{\"event_id\":\"8b3caad0-b708-4cc4-9008-27862f8f8e5e\",\"event_ts\":\"invalid\"}",
                        UserEvent.class));
    }
}
