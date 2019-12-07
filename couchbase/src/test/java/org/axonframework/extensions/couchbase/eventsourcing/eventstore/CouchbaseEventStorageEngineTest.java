package org.axonframework.extensions.couchbase.eventsourcing.eventstore;

import lombok.extern.slf4j.Slf4j;
import org.axonframework.common.jdbc.PersistenceExceptionResolver;
import org.axonframework.eventhandling.DomainEventData;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.TrackedEventData;
import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.eventsourcing.eventstore.AbstractEventStorageEngine;
import org.axonframework.eventsourcing.eventstore.BatchingEventStorageEngineTest;
import org.axonframework.eventsourcing.eventstore.EventStoreException;
import org.axonframework.modelling.command.ConcurrencyException;
import org.axonframework.serialization.upcasting.event.EventUpcaster;
import org.junit.Before;
import org.junit.Test;
import org.axonframework.extensions.couchbase.CouchbaseBucketManager;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.axonframework.eventsourcing.utils.EventStoreTestUtils.AGGREGATE;
import static org.axonframework.eventsourcing.utils.EventStoreTestUtils.createEvent;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Slf4j
public class CouchbaseEventStorageEngineTest extends BatchingEventStorageEngineTest {

    private CouchbaseBucketManager couchbaseBucketManager = CouchbaseBucketManager.INSTANCE;
    private CouchbaseTemplate couchbaseTemplate;

    private CouchbaseEventStorageEngine storageEngine;

    @Before
    public void setUp() {
        couchbaseBucketManager.clearBuckets();
        couchbaseTemplate = new DefaultCouchbaseTemplate(
            couchbaseBucketManager.inventoryBucket(),
            couchbaseBucketManager.inventoryBucket(),
            couchbaseBucketManager.inventoryBucket(),
            couchbaseBucketManager.inventoryBucket()
        );
        storageEngine = CouchbaseEventStorageEngine.builder().couchbaseTemplate(couchbaseTemplate).build();
        storageEngine.ensureIndexes(
            couchbaseTemplate.getEventBucket(),
            couchbaseTemplate.getSnapshotBucket()
        );
        setTestSubject(storageEngine);
    }

    @Test
    public void testFetchHighestSequenceNumber() {
        storageEngine.appendEvents(createEvent(0), createEvent(1));
        storageEngine.appendEvents(createEvent(2), createEvent(3));

        assertEquals(3, (long) storageEngine.lastSequenceNumberFor(AGGREGATE).get());
        assertFalse(storageEngine.lastSequenceNumberFor("notexist").isPresent());
    }

    @Test
    public void testFetchingEventsReturnsResultsWhenMoreThanBatchSizeCommitsAreAvailable() {
        storageEngine.appendEvents(createEvent(0), createEvent(1), createEvent(2));
        storageEngine.appendEvents(createEvent(3), createEvent(4), createEvent(5));
        storageEngine.appendEvents(createEvent(6), createEvent(7), createEvent(8));
        storageEngine.appendEvents(createEvent(9), createEvent(10), createEvent(11));
        storageEngine.appendEvents(createEvent(12), createEvent(13), createEvent(14));

        List<? extends TrackedEventData<?>> result = storageEngine.fetchTrackedEvents(null, 2);
        TrackedEventData<?> last = result.get(result.size() - 2);// we decide to omit the last event
        List<? extends TrackedEventData<?>> actual = storageEngine.fetchTrackedEvents(last.trackingToken(), 2);
        assertFalse("Expected to retrieve some events", actual.isEmpty());
        // we want to make sure we get the first event from the next commit
        assertEquals(((DomainEventData<?>) actual.get(0)).getSequenceNumber(), 14);
    }

    @Test
    public void testFetchingEventsReturnsResultsWhenMoreThanBatchSizeCommitsAreAvailable_PartiallyReadingCommit() {
        storageEngine.appendEvents(createEvent(0), createEvent(1), createEvent(2));
        storageEngine.appendEvents(createEvent(3), createEvent(4), createEvent(5));
        storageEngine.appendEvents(createEvent(6), createEvent(7), createEvent(8));
        storageEngine.appendEvents(createEvent(9), createEvent(10), createEvent(11));
        storageEngine.appendEvents(createEvent(12), createEvent(13), createEvent(14));

        List<? extends TrackedEventData<?>> result = storageEngine.fetchTrackedEvents(null, 2);
        TrackedEventData<?> last = result.get(result.size() - 2);// we decide to omit the last event
        last.trackingToken();
        List<? extends TrackedEventData<?>> actual = storageEngine.fetchTrackedEvents(last.trackingToken(), 2);
        assertFalse("Expected to retrieve some events", actual.isEmpty());
        // we want to make sure we get the last event from the commit
        assertEquals(((DomainEventData<?>) actual.get(0)).getSequenceNumber(), 14);
    }


    @Test
    @Override
    public void testUniqueKeyConstraintOnEventIdentifier() {
        log.info("Unique event identifier is not currently guaranteed in the Mongo Event Storage Engine");
    }

    @Test(expected = ConcurrencyException.class)
    @Override
    public void testStoreDuplicateEventWithoutExceptionResolver() {
        storageEngine.appendEvents(createEvent(0), createEvent(1));
        storageEngine.appendEvents(createEvent(0), createEvent(1));
    }

    @Test(expected = EventStoreException.class)
    @Override
    public void testStoreDuplicateEventWithExceptionTranslator() {
        AbstractEventStorageEngine engine = createEngine((PersistenceExceptionResolver) null);
        engine.appendEvents(createEvent(0), createEvent(1));
        engine.appendEvents(createEvent(0), createEvent(1));
    }

    @Override
    @Test
    public void testCreateTokenAtExactTime() {
        DomainEventMessage<String> event1 = createEvent(0, Instant.parse("2007-12-03T10:15:30.00Z"));
        DomainEventMessage<String> event2 = createEvent(1, Instant.parse("2007-12-03T10:15:40.01Z"));
        DomainEventMessage<String> event3 = createEvent(2, Instant.parse("2007-12-03T10:15:35.00Z"));
        storageEngine.appendEvents(event1, event2, event3);
        TrackingToken tokenAt = storageEngine.createTokenAt(Instant.parse("2007-12-03T10:15:30.00Z"));
        List<EventMessage<?>> readEvents = storageEngine.readEvents(tokenAt, false).collect(toList());
        assertEventStreamsById(Arrays.asList(event1, event3, event2), readEvents);
    }

    @Override
    public void testCreateTailToken() {
        DomainEventMessage<String> event1 = createEvent(0, Instant.parse("2007-12-03T10:15:10.00Z"));
        DomainEventMessage<String> event2 = createEvent(1, Instant.parse("2007-12-03T10:15:40.00Z"));
        DomainEventMessage<String> event3 = createEvent(2, Instant.parse("2007-12-03T10:15:35.00Z"));
        storageEngine.appendEvents(event1);
        storageEngine.appendEvents(event2);
        storageEngine.appendEvents(event3);
        TrackingToken headToken = storageEngine.createTailToken();
        List<EventMessage<?>> readEvents = storageEngine.readEvents(headToken, false).collect(toList());
        assertEventStreamsById(Arrays.asList(event1, event3, event2), readEvents);
    }

    @Override
    public void testCreateTokenWithUnorderedEvents() {
        DomainEventMessage<String> event1 = createEvent(0, Instant.parse("2007-12-03T10:15:30.00Z"));
        DomainEventMessage<String> event2 = createEvent(1, Instant.parse("2007-12-03T10:15:40.00Z"));
        DomainEventMessage<String> event3 = createEvent(2, Instant.parse("2007-12-03T10:15:50.00Z"));
        DomainEventMessage<String> event4 = createEvent(3, Instant.parse("2007-12-03T10:15:45.00Z"));
        DomainEventMessage<String> event5 = createEvent(4, Instant.parse("2007-12-03T10:15:42.00Z"));
        storageEngine.appendEvents(event1, event2, event3, event4, event5);
        TrackingToken tokenAt = storageEngine.createTokenAt(Instant.parse("2007-12-03T10:15:45.00Z"));
        List<EventMessage<?>> readEvents = storageEngine.readEvents(tokenAt, false).collect(toList());
        assertEventStreamsById(Arrays.asList(event4, event3), readEvents);
    }

    @Test
    public void testStoreAndLoadSnapshot() {
        storageEngine.storeSnapshot(createEvent(0));
        storageEngine.storeSnapshot(createEvent(1));
        storageEngine.storeSnapshot(createEvent(3));
        storageEngine.storeSnapshot(createEvent(2));
        assertTrue(storageEngine.readSnapshot(AGGREGATE).isPresent());
        assertEquals(2, storageEngine.readSnapshot(AGGREGATE).get().getSequenceNumber());
    }

    @Override
    @Test
    public void testCreateTokenAt() {
        DomainEventMessage<String> event1 = createEvent(0, Instant.parse("2007-12-03T10:15:00.01Z"));
        DomainEventMessage<String> event2 = createEvent(1, Instant.parse("2007-12-03T10:15:40.00Z"));
        DomainEventMessage<String> event3 = createEvent(2, Instant.parse("2007-12-03T10:15:35.00Z"));
        storageEngine.appendEvents(event1);
        storageEngine.appendEvents(event2);
        storageEngine.appendEvents(event3);
        TrackingToken tokenAt = storageEngine.createTokenAt(Instant.parse("2007-12-03T10:15:30.00Z"));
        List<EventMessage<?>> readEvents = storageEngine.readEvents(tokenAt, false).collect(toList());
        assertEventStreamsById(Arrays.asList(event3, event2), readEvents);
    }

    @Override
    protected AbstractEventStorageEngine createEngine(EventUpcaster eventUpcaster) {
        return CouchbaseEventStorageEngine.builder()
            .upcasterChain(eventUpcaster)
            .couchbaseTemplate(couchbaseTemplate).build();
    }

    @Override
    protected AbstractEventStorageEngine createEngine(PersistenceExceptionResolver persistenceExceptionResolver) {
        return CouchbaseEventStorageEngine.builder()
            .persistenceExceptionResolver(persistenceExceptionResolver)
            .couchbaseTemplate(couchbaseTemplate).build();
    }
}
