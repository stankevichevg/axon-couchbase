package org.axonframework.extensions.couchbase.eventsourcing.eventstore;

import com.couchbase.client.java.document.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.axonframework.eventhandling.DomainEventData;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.serialization.SerializedMetaData;
import org.axonframework.serialization.SerializedObject;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.SimpleSerializedObject;

import java.time.Instant;
import java.util.function.Function;

@Slf4j
public class EventEntry implements DomainEventData<Object> {

    private final String aggregateIdentifier;
    private final String aggregateType;
    private final long sequenceNumber;
    private final long timestamp;
    private final String serializedPayload;
    private final String payloadType;
    private final String payloadRevision;
    private final Object serializedMetaData;
    private final String eventIdentifier;

    public EventEntry(DomainEventMessage<?> event, Serializer serializer) {
        this(event, serializer, i -> i);
    }

    public EventEntry(DomainEventMessage<?> event, Serializer serializer, Function<String, String> idGenerator) {
        aggregateIdentifier = idGenerator.apply(event.getAggregateIdentifier());
        aggregateType = event.getType();
        sequenceNumber = event.getSequenceNumber();
        eventIdentifier = event.getIdentifier();
        long start = System.currentTimeMillis();
        SerializedObject<String> serializedPayloadObject = event.serializePayload(serializer, String.class);
        log.debug("Event serialization duration: {}", System.currentTimeMillis() - start);
        SerializedObject<String> serializedMetaDataObject = event.serializeMetaData(serializer, String.class);
        serializedPayload = serializedPayloadObject.getData();
        payloadType = serializedPayloadObject.getType().getName();
        payloadRevision = serializedPayloadObject.getType().getRevision();
        serializedMetaData = serializedMetaDataObject.getData();
        timestamp = event.getTimestamp().toEpochMilli();
    }

    public EventEntry(JsonObject dbObject, EventEntryConfiguration configuration) {
        aggregateIdentifier = (String) dbObject.get(configuration.aggregateIdentifierProperty());
        aggregateType = (String) dbObject.get(configuration.typeProperty());
        sequenceNumber = ((Number) dbObject.get(configuration.sequenceNumberProperty())).longValue();
        serializedPayload = dbObject.getString(configuration.payloadProperty());
        timestamp = (Long) dbObject.get(configuration.timestampProperty());
        payloadType = (String) dbObject.get(configuration.payloadTypeProperty());
        payloadRevision = (String) dbObject.get(configuration.payloadRevisionProperty());
        serializedMetaData = dbObject.get(configuration.metaDataProperty());
        eventIdentifier = (String) dbObject.get(configuration.eventIdentifierProperty());
    }

    public JsonObject asDocument(EventEntryConfiguration configuration) {
        return JsonObject.create()
            .put(configuration.aggregateIdentifierProperty(), aggregateIdentifier)
            .put(configuration.typeProperty(), aggregateType)
            .put(configuration.sequenceNumberProperty(), sequenceNumber)
            .put(configuration.payloadProperty(), serializedPayload)
            .put(configuration.timestampProperty(), timestamp)
            .put(configuration.payloadTypeProperty(), payloadType)
            .put(configuration.payloadRevisionProperty(), payloadRevision)
            .put(configuration.metaDataProperty(), serializedMetaData)
            .put(configuration.eventIdentifierProperty(), eventIdentifier);
    }

    @Override
    public String getType() {
        return aggregateType;
    }

    @Override
    public String getAggregateIdentifier() {
        return aggregateIdentifier;
    }

    @Override
    public long getSequenceNumber() {
        return sequenceNumber;
    }

    @Override
    public String getEventIdentifier() {
        return eventIdentifier;
    }

    @Override
    public Instant getTimestamp() {
        return Instant.ofEpochMilli(timestamp);
    }

    @Override
    @SuppressWarnings("unchecked")
    public SerializedObject<Object> getMetaData() {
        return new SerializedMetaData(serializedMetaData, String.class);
    }

    @Override
    @SuppressWarnings("unchecked")
    public SerializedObject<Object> getPayload() {
        return new SimpleSerializedObject(serializedPayload, String.class, payloadType, payloadRevision);
    }

}
