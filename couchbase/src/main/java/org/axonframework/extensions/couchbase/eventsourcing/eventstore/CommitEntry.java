package org.axonframework.extensions.couchbase.eventsourcing.eventstore;

import com.couchbase.client.java.document.json.JsonObject;
import lombok.Getter;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.serialization.Serializer;

import java.util.ArrayList;
import java.util.List;

/**
 * Coucbase event storage entry containing an array of {@link EventEntry event entries} that are part of the same
 * UnitOfWork commit.
 */
public class CommitEntry {

    @Getter private final String aggregateIdentifier;
    @Getter private final long firstSequenceNumber;
    private final String aggregateType;
    private final long lastSequenceNumber;
    private final long firstTimestamp;
    private final long lastTimestamp;
    private final EventEntry[] eventEntries;
    private final String lastEventIdentifier;

    public CommitEntry(List<? extends DomainEventMessage<?>> events, Serializer serializer) {
        DomainEventMessage firstEvent = events.get(0);
        DomainEventMessage lastEvent = events.get(events.size() - 1);
        firstSequenceNumber = firstEvent.getSequenceNumber();
        firstTimestamp = firstEvent.getTimestamp().toEpochMilli();
        lastTimestamp = lastEvent.getTimestamp().toEpochMilli();
        lastSequenceNumber = lastEvent.getSequenceNumber();
        aggregateIdentifier = lastEvent.getAggregateIdentifier();
        lastEventIdentifier = lastEvent.getIdentifier();
        aggregateType = lastEvent.getType();
        eventEntries = new EventEntry[events.size()];
        for (int i = 0, eventsLength = events.size(); i < eventsLength; i++) {
            DomainEventMessage event = events.get(i);
            eventEntries[i] = new EventEntry(event, serializer);
        }
    }

    public JsonObject asDocument(CommitEntryConfiguration commitConfiguration,
                               EventEntryConfiguration eventConfiguration) {
        List<JsonObject> events = new ArrayList<>();
        for (EventEntry eventEntry : eventEntries) {
            events.add(eventEntry.asDocument(eventConfiguration));
        }
        return JsonObject.create()
            .put(eventConfiguration.aggregateIdentifierProperty(), aggregateIdentifier)
            .put(eventConfiguration.sequenceNumberProperty(), lastSequenceNumber)
            .put(eventConfiguration.eventIdentifierProperty(), lastEventIdentifier)
            .put(commitConfiguration.lastSequenceNumberProperty(), lastSequenceNumber)
            .put(commitConfiguration.firstSequenceNumberProperty(), firstSequenceNumber)
            .put(eventConfiguration.timestampProperty(), firstTimestamp)
            .put(commitConfiguration.firstTimestampProperty(), firstTimestamp)
            .put(commitConfiguration.lastTimestampProperty(), lastTimestamp)
            .put(eventConfiguration.typeProperty(), aggregateType)
            .put(commitConfiguration.eventsProperty(), events);
    }

}
