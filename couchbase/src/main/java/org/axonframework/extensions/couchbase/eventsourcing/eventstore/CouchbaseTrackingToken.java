package org.axonframework.extensions.couchbase.eventsourcing.eventstore;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.axonframework.common.Assert;
import org.axonframework.eventhandling.TrackingToken;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Collections.unmodifiableSet;

public class CouchbaseTrackingToken implements TrackingToken, Serializable {

    private final long timestamp;
    private final Map<String, Long> trackedEvents;

    private CouchbaseTrackingToken(long timestamp, Map<String, Long> trackedEvents) {
        this.timestamp = timestamp;
        this.trackedEvents = trackedEvents;
    }

    public static CouchbaseTrackingToken of(Instant timestamp, String eventIdentifier) {
        return new CouchbaseTrackingToken(timestamp.toEpochMilli(),
            Collections.singletonMap(eventIdentifier, timestamp.toEpochMilli()));
    }

    public static CouchbaseTrackingToken of(long timestamp, Map<String, Long> trackedEvents) {
        return new CouchbaseTrackingToken(timestamp, trackedEvents);
    }

    @JsonCreator
    public static CouchbaseTrackingToken of(
        @JsonProperty("timestamp") Instant timestamp,
        @JsonProperty("trackedEvents") Map<String, Long> trackedEvents) {
        return new CouchbaseTrackingToken(timestamp.toEpochMilli(), trackedEvents);
    }

    public CouchbaseTrackingToken advanceTo(Instant timestamp, String eventIdentifier, Duration lookBackTime) {
        if (trackedEvents.containsKey(eventIdentifier)) {
            throw new IllegalArgumentException(
                String.format("The event to advance to [%s] should not be one of the token's known events",
                    eventIdentifier));
        }
        long millis = timestamp.toEpochMilli();
        LinkedHashMap<String, Long> trackedEvents = new LinkedHashMap<>(this.trackedEvents);
        trackedEvents.put(eventIdentifier, millis);
        long newTimestamp = max(millis, this.timestamp);
        return new CouchbaseTrackingToken(newTimestamp, trim(trackedEvents, newTimestamp, lookBackTime));
    }

    private Map<String, Long> trim(LinkedHashMap<String, Long> priorEvents, long currentTime, Duration lookBackTime) {
        Long cutOffTimestamp = currentTime - lookBackTime.toMillis();
        Iterator<Long> iterator = priorEvents.values().iterator();
        while (iterator.hasNext()) {
            if (iterator.next().compareTo(cutOffTimestamp) < 0) {
                iterator.remove();
            } else {
                return priorEvents;
            }
        }
        return priorEvents;
    }

    public Instant getTimestamp() {
        return Instant.ofEpochMilli(timestamp);
    }

    public Map<String, Long> getTrackedEvents() {
        return Collections.unmodifiableMap(trackedEvents);
    }

    public Set<String> knownEventIds() {
        return unmodifiableSet(trackedEvents.keySet());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CouchbaseTrackingToken that = (CouchbaseTrackingToken) o;
        return timestamp == that.timestamp && Objects.equals(trackedEvents, that.trackedEvents);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, trackedEvents);
    }

    @Override
    public String toString() {
        return "CouchbaseTrackingToken{" + "timestamp=" + timestamp + ", trackedEvents=" + trackedEvents + '}';
    }

    @Override
    public TrackingToken lowerBound(TrackingToken other) {
        Assert.isTrue(other instanceof CouchbaseTrackingToken, () -> "Incompatible token type provided.");
        CouchbaseTrackingToken otherToken = (CouchbaseTrackingToken) other;

        Map<String, Long> intersection = new HashMap<>(this.trackedEvents);
        trackedEvents.keySet().forEach(k -> {
            if (!otherToken.trackedEvents.containsKey(k)) {
                intersection.remove(k);
            }
        });
        return new CouchbaseTrackingToken(min(timestamp, otherToken.timestamp), intersection);
    }

    @Override
    public TrackingToken upperBound(TrackingToken other) {
        Assert.isTrue(other instanceof CouchbaseTrackingToken, () -> "Incompatible token type provided.");
        Long timestamp = max(((CouchbaseTrackingToken)other).timestamp, this.timestamp);
        Map<String, Long> events = new HashMap<>(trackedEvents);
        events.putAll(((CouchbaseTrackingToken) other).trackedEvents);
        return new CouchbaseTrackingToken(timestamp, events);
    }

    @Override
    public boolean covers(TrackingToken other) {
        Assert.isTrue(other instanceof CouchbaseTrackingToken, () -> "Incompatible token type provided.");
        CouchbaseTrackingToken otherToken = (CouchbaseTrackingToken) other;

        long oldest = this.trackedEvents.values().stream().min(Comparator.naturalOrder()).orElse(0L);
        return otherToken.timestamp <= this.timestamp
            && otherToken.trackedEvents.keySet().stream()
            .allMatch(k -> this.trackedEvents.containsKey(k) ||
                otherToken.trackedEvents.get(k) < oldest);
    }
}
