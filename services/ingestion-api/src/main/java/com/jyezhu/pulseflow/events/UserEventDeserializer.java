package com.jyezhu.pulseflow.events;

import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonParser;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.ValueDeserializer;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class UserEventDeserializer extends ValueDeserializer<UserEvent> {

    private static final TypeReference<Map<String, Object>> JSON_OBJECT = new TypeReference<>() {
    };
    private static final Set<String> PROPERTY_NAMES = Set.of(
            "event_id", "user_id", "session_id", "event_type", "content_id",
            "device_type", "country_code", "event_ts", "attributes"
    );

    @Override
    public UserEvent deserialize(JsonParser parser, DeserializationContext context) throws JacksonException {
        Map<String, Object> properties = context.readValue(parser, JSON_OBJECT);
        if (properties == null) {
            return context.reportInputMismatch(UserEvent.class, "User event must be a JSON object");
        }

        for (String propertyName : properties.keySet()) {
            if (!PROPERTY_NAMES.contains(propertyName)) {
                return context.reportInputMismatch(
                        UserEvent.class,
                        "Unknown user event property '%s'",
                        propertyName
                );
            }
        }

        String eventId = requiredString(properties, "event_id", context);
        String userId = requiredString(properties, "user_id", context);
        String sessionId = requiredString(properties, "session_id", context);
        UserEvent.EventType eventType = enumValue(
                requiredString(properties, "event_type", context),
                UserEvent.EventType.class,
                context
        );
        String contentId = requiredString(properties, "content_id", context);
        UserEvent.DeviceType deviceType = enumValue(
                requiredString(properties, "device_type", context),
                UserEvent.DeviceType.class,
                context
        );
        String countryCode = requiredString(properties, "country_code", context);
        String eventTimestamp = requiredString(properties, "event_ts", context);

        UUID uuid;
        try {
            uuid = UUID.fromString(eventId);
            if (eventId.length() != 36 || !uuid.toString().equalsIgnoreCase(eventId)) {
                return context.reportInputMismatch(UserEvent.class, "event_id must be a UUID");
            }
        } catch (IllegalArgumentException exception) {
            return context.reportInputMismatch(UserEvent.class, "event_id must be a UUID");
        }

        Instant timestamp;
        try {
            timestamp = Instant.parse(eventTimestamp);
        } catch (DateTimeParseException exception) {
            return context.reportInputMismatch(UserEvent.class, "event_ts must be a date-time");
        }

        Map<String, String> attributes = null;
        if (properties.containsKey("attributes")) {
            Object attributeValue = properties.get("attributes");
            if (!(attributeValue instanceof Map<?, ?> attributeMap)) {
                return context.reportInputMismatch(UserEvent.class, "attributes must be an object");
            }
            attributes = stringMap(attributeMap, context);
        }

        return new UserEvent(
                uuid,
                userId,
                sessionId,
                eventType,
                contentId,
                deviceType,
                countryCode,
                timestamp,
                attributes
        );
    }

    private static String requiredString(
            Map<String, Object> properties,
            String name,
            DeserializationContext context
    ) throws JacksonException {
        Object value = properties.get(name);
        if (!(value instanceof String string)) {
            return context.reportInputMismatch(UserEvent.class, "%s must be a string", name);
        }
        return string;
    }

    private static <E extends Enum<E>> E enumValue(
            String value,
            Class<E> enumType,
            DeserializationContext context
    ) throws JacksonException {
        try {
            return Enum.valueOf(enumType, value);
        } catch (IllegalArgumentException exception) {
            return context.reportInputMismatch(
                    UserEvent.class,
                    "Unsupported value for %s",
                    enumType.getSimpleName()
            );
        }
    }

    private static Map<String, String> stringMap(
            Map<?, ?> values,
            DeserializationContext context
    ) throws JacksonException {
        Map<String, String> typedValues = new HashMap<>();
        for (Map.Entry<?, ?> entry : values.entrySet()) {
            if (!(entry.getKey() instanceof String key) || !(entry.getValue() instanceof String value)) {
                return context.reportInputMismatch(UserEvent.class, "attributes must contain string values");
            }
            typedValues.put(key, value);
        }
        return typedValues;
    }
}
