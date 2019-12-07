package org.axonframework.extensions.couchbase.eventsourcing.eventstore;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CouchbaseTrackingTokenTest {

    private static final Duration ONE_SECOND = Duration.ofSeconds(1);

    @Test
    public void testAdvanceToLaterTimestamp() {
        CouchbaseTrackingToken start = CouchbaseTrackingToken.of(time(0), "0");
        CouchbaseTrackingToken subject = start.advanceTo(time(1), "1", ONE_SECOND);
        assertThat(start).isNotEqualTo(subject);
        assertThat(subject.getTimestamp()).isEqualTo(time(1));
        assertKnownEventIds(subject, "0", "1");
    }

    @Test
    public void testAdvanceToHigherSequenceNumber() {
        CouchbaseTrackingToken subject = CouchbaseTrackingToken.of(time(0), "0").advanceTo(time(0), "1", ONE_SECOND);
        assertThat(subject.getTimestamp()).isEqualTo(time(0));
        assertKnownEventIds(subject, "0", "1");
    }

    @Test
    public void testAdvanceToHigherIdentifier() {
        CouchbaseTrackingToken subject = CouchbaseTrackingToken.of(time(0), "0").advanceTo(time(0), "1", ONE_SECOND);
        assertThat(subject.getTimestamp()).isEqualTo(time(0));
        assertKnownEventIds(subject, "0", "1");
    }

    @Test
    public void testAdvanceToOlderTimestamp() {
        CouchbaseTrackingToken subject = CouchbaseTrackingToken.of(time(1), "0").advanceTo(time(0), "1", ONE_SECOND);
        assertThat(subject.getTimestamp()).isEqualTo(time(1));
        assertKnownEventIds(subject, "0", "1");
    }

    @Test
    public void testAdvanceToLowerSequenceNumber() {
        CouchbaseTrackingToken subject = CouchbaseTrackingToken.of(time(0), "0").advanceTo(time(0), "1", ONE_SECOND);
        assertThat(subject.getTimestamp()).isEqualTo(time(0));
        assertKnownEventIds(subject, "0", "1");
    }

    @Test
    public void testAdvanceToLowerIdentifier() {
        CouchbaseTrackingToken subject = CouchbaseTrackingToken.of(time(0), "1").advanceTo(time(0), "0", ONE_SECOND);
        assertThat(subject.getTimestamp()).isEqualTo(time(0));
        assertKnownEventIds(subject, "0", "1");
    }

    @Test
    public void testAdvanceToSameIdentifierNotAllowed() {
        assertThatThrownBy(() -> {
            CouchbaseTrackingToken.of(time(0), "0").advanceTo(time(1), "0", ONE_SECOND);
        }).isInstanceOf(Exception.class);
    }

    @Test
    public void testAdvanceToPriorIdentifierNotAllowed() {
        assertThatThrownBy(() -> {
            CouchbaseTrackingToken.of(time(0), "1")
                .advanceTo(time(1), "2", ONE_SECOND).advanceTo(time(2), "1", ONE_SECOND);
        }).isInstanceOf(Exception.class);
    }

    @Test
    public void testAdvanceToTrimsIdentifierCache() {
        CouchbaseTrackingToken subject = CouchbaseTrackingToken.of(time(0), "0").advanceTo(time(1001), "1", ONE_SECOND);
        assertThat(subject.getTimestamp()).isEqualTo(time(1001));
        assertKnownEventIds(subject, "1");
    }

    @Test
    public void testAdvancingATokenMakesItCoverThePrevious() {
        CouchbaseTrackingToken subject = CouchbaseTrackingToken.of(time(1000), "0");
        CouchbaseTrackingToken advancedToken = subject.advanceTo(time(1001), "1", ONE_SECOND);
        assertThat(advancedToken.covers(subject)).isTrue();
        assertThat(CouchbaseTrackingToken.of(time(1000), "1").covers(subject)).isFalse();
    }

    @Test
    public void testUpperBound() {
        CouchbaseTrackingToken first = CouchbaseTrackingToken.of(time(1000), "0")
            .advanceTo(time(1001), "1", Duration.ofHours(1))
            .advanceTo(time(1002), "2", Duration.ofHours(1));

        CouchbaseTrackingToken second = CouchbaseTrackingToken.of(time(1003), "3");
        assertThat(first.upperBound(second)).isEqualTo(first.advanceTo(time(1003), "3", Duration.ofHours(1)));
    }

    @Test
    public void testSerializationDeserialization() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        CouchbaseTrackingToken trackingToken = CouchbaseTrackingToken.of(time(1000), "0");
        String serialized = objectMapper.writeValueAsString(trackingToken);
        CouchbaseTrackingToken deserialized = objectMapper.readValue(serialized, CouchbaseTrackingToken.class);
        assertThat(trackingToken).isEqualTo(deserialized);
    }

    private static void assertKnownEventIds(CouchbaseTrackingToken token, String... expectedKnownIds) {
        assertThat(token.knownEventIds()).containsOnly(expectedKnownIds);
    }

    private static Instant time(int millis) {
        return Instant.ofEpochMilli(millis);
    }

}
