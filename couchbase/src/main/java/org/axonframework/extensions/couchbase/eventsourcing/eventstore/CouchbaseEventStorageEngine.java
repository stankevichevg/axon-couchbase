package org.axonframework.extensions.couchbase.eventsourcing.eventstore;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.DocumentAlreadyExistsException;
import com.couchbase.client.java.query.N1qlParams;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.couchbase.client.java.query.Statement;
import com.couchbase.client.java.query.consistency.ScanConsistency;
import com.couchbase.client.java.query.dsl.Sort;
import org.axonframework.common.Assert;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.jdbc.PersistenceExceptionResolver;
import org.axonframework.eventhandling.DomainEventData;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.EventUtils;
import org.axonframework.eventhandling.TrackedEventData;
import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.eventsourcing.eventstore.BatchingEventStorageEngine;
import org.axonframework.serialization.SerializedObject;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.upcasting.event.EventUpcaster;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.couchbase.client.java.document.JsonDocument.create;
import static com.couchbase.client.java.document.json.JsonArray.from;
import static com.couchbase.client.java.query.Delete.deleteFrom;
import static com.couchbase.client.java.query.Index.createIndex;
import static com.couchbase.client.java.query.Select.select;
import static com.couchbase.client.java.query.dsl.Expression.s;
import static com.couchbase.client.java.query.dsl.Expression.x;
import static com.couchbase.client.java.query.dsl.Sort.asc;
import static com.couchbase.client.java.query.dsl.functions.AggregateFunctions.max;
import static com.couchbase.client.java.query.dsl.functions.AggregateFunctions.min;
import static java.util.Optional.empty;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;
import static org.axonframework.common.BuilderUtils.assertNonNull;


public class CouchbaseEventStorageEngine extends BatchingEventStorageEngine {

    private final EventEntryConfiguration eventConfiguration;
    private final CommitEntryConfiguration commitEntryConfiguration;
    private final CouchbaseTemplate template;
    private final Duration lookBackTime;

    public CouchbaseEventStorageEngine(Builder builder) {
        super(builder);
        eventConfiguration = builder.eventConfiguration;
        commitEntryConfiguration = builder.commitEntryConfiguration;
        template = builder.template;
        lookBackTime = builder.lookBackTime;
    }

    @Override
    protected void appendEvents(List<? extends EventMessage<?>> events, Serializer serializer) {
        if (!events.isEmpty()) {
            try {
                final CommitEntry entry = createCommitDocument(events, serializer);
                final String id = entry.getAggregateIdentifier() + ":" + entry.getFirstSequenceNumber();
                template.getEventBucket().insert(
                    create(id, entry.asDocument(commitEntryConfiguration, eventConfiguration))
                );
            } catch (Exception e) {
                handlePersistenceException(e, events.get(0));
            }
        }
    }

    private CommitEntry createCommitDocument(List<? extends EventMessage<?>> events, Serializer serializer) {
        return new CommitEntry(
            events.stream().map(EventUtils::asDomainEventMessage).collect(toList()),
            serializer
        );
    }

    @Override
    protected void storeSnapshot(DomainEventMessage<?> snapshot, Serializer serializer) {
        try {
            appendSnapshot(template.getSnapshotBucket(), snapshot, serializer);
            deleteSnapshots(
                template.getSnapshotBucket(), snapshot.getAggregateIdentifier(), snapshot.getSequenceNumber()
            );
        } catch (Exception e) {
            handlePersistenceException(e, snapshot);
        }
    }

    private void appendSnapshot(Bucket snapshotBucket, DomainEventMessage<?> snapshot, Serializer serializer) {
        snapshotBucket.upsert(create(
            createSnapshotId(snapshot.getAggregateIdentifier()) + ":" + snapshot.getSequenceNumber(),
            new EventEntry(snapshot, serializer, this::createSnapshotId).asDocument(eventConfiguration)
        ));
    }

    private void deleteSnapshots(Bucket snapshotBucket, String aggregateIdentifier, long sequenceNumber) {
        final String idProp = eventConfiguration.aggregateIdentifierProperty();
        final String seqProp = eventConfiguration.sequenceNumberProperty();
        snapshotBucket.query(
            N1qlQuery.simple(
                deleteFrom(snapshotBucket.name())
                    .where(
                        x(idProp).eq(s(createSnapshotId(aggregateIdentifier))).and(x(seqProp).lt(sequenceNumber))
                    ),
                N1qlParams.build().consistency(ScanConsistency.STATEMENT_PLUS)
            )
        );
    }

    private String createSnapshotId(String aggregateIdentifier) {
        return "snapshot:" + aggregateIdentifier;
    }

    @Override
    protected Stream<? extends DomainEventData<?>> readSnapshotData(String aggregateIdentifier) {
        final String idProp = eventConfiguration.aggregateIdentifierProperty();
        final String seqProp = eventConfiguration.sequenceNumberProperty();
        final Statement statement =
            select(x(template.getSnapshotBucket().name() + ".*"))
                .from(template.getSnapshotBucket().name())
                .where(x(idProp).eq(s(createSnapshotId(aggregateIdentifier))))
                .orderBy(asc(seqProp));
        final Iterable<N1qlQueryRow> result = () -> template.getSnapshotBucket().query(
            N1qlQuery.simple(statement, N1qlParams.build().consistency(ScanConsistency.STATEMENT_PLUS))
        ).rows();
        Stream<? extends DomainEventData<?>> eventStream = stream(result.spliterator(), false)
            .map(N1qlQueryRow::value)
            .map(v -> new EventEntry(v, eventConfiguration));
        return eventStream;
    }

    public void ensureIndexes(Bucket eventBucket, Bucket snapshotBucket) {
        eventBucket.query(createIndex("orderedAggregateEventsIndex").on(eventBucket.name(),
            x(eventConfiguration.aggregateIdentifierProperty()),
            x(eventConfiguration.sequenceNumberProperty())
        ));
        eventBucket.query(createIndex("orderedEventStreamIndex").on(eventBucket.name(),
            x(eventConfiguration.timestampProperty()),
            x(eventConfiguration.sequenceNumberProperty())
        ));
        snapshotBucket.query(createIndex("orderedAggregateEventsIndex").on(snapshotBucket.name(),
            x(eventConfiguration.aggregateIdentifierProperty()),
            x(eventConfiguration.sequenceNumberProperty())
        ));
    }

    @Override
    protected List<? extends DomainEventData<?>> fetchDomainEvents(String aggregateIdentifier, long firstSequenceNumber,
                                                                   int batchSize) {
        final String idProp = eventConfiguration.aggregateIdentifierProperty();
        final String seqProp = eventConfiguration.sequenceNumberProperty();
        final Statement statement =
            select(x("e.*"))
                .from(template.getEventBucket().name())
                .unnest(x("events")).as("e")
                .where(x("e." + idProp).eq(s(aggregateIdentifier)).and(x("e." + seqProp).gte(firstSequenceNumber)))
                .orderBy(asc("e." + seqProp));
        final Iterable<N1qlQueryRow> result = () -> template.getEventBucket().query(
            N1qlQuery.simple(statement, N1qlParams.build().consistency(ScanConsistency.STATEMENT_PLUS))
        ).rows();
        return stream(result.spliterator(), false)
            .map(N1qlQueryRow::value)
            .map(entry -> new EventEntry(entry, eventConfiguration))
            .filter(event -> event.getSequenceNumber() >= firstSequenceNumber)
            .collect(toList());

    }

    @Override
    protected List<? extends TrackedEventData<?>> fetchTrackedEvents(TrackingToken lastToken, int batchSize) {
        return findTrackedEvents(template.getEventBucket(), lastToken, batchSize);
    }

    private List<? extends TrackedEventData<?>> findTrackedEvents(
        Bucket eventBucket, TrackingToken lastToken, int batchSize) {
        final String timestampProp = eventConfiguration.timestampProperty();
        final String eventIdProp = eventConfiguration.eventIdentifierProperty();
        final Statement statement;
        if (lastToken == null) {
            statement = select(x("e.*"))
                .from(eventBucket.name())
                .unnest(x("events")).as("e")
                .orderBy(
                    Sort.asc("e." + eventConfiguration.timestampProperty()),
                    Sort.asc("e." + eventConfiguration.sequenceNumberProperty())
                );
        } else {
            Assert.isTrue(
                lastToken instanceof CouchbaseTrackingToken,
                () -> String.format("Token %s is of the wrong type", lastToken)
            );
            CouchbaseTrackingToken trackingToken = (CouchbaseTrackingToken) lastToken;
            statement =
                select(x("e.*"))
                    .from(eventBucket.name())
                    .unnest(x("events")).as("e")
                    .where(
                        x("e." + timestampProp).gte(trackingToken.getTimestamp().minus(lookBackTime).toEpochMilli()).and(
                            x("e." + eventIdProp).notIn(from(new ArrayList<>(trackingToken.knownEventIds()))))
                    ).orderBy(
                    Sort.asc("e." + eventConfiguration.timestampProperty()),
                    Sort.asc("e." + eventConfiguration.sequenceNumberProperty()
                    )
                );
        }
        AtomicReference<CouchbaseTrackingToken> previousToken = new AtomicReference<>((CouchbaseTrackingToken) lastToken);
        final Iterable<N1qlQueryRow> result = () -> eventBucket.query(
            N1qlQuery.simple(statement, N1qlParams.build().consistency(ScanConsistency.STATEMENT_PLUS))
        ).rows();
        return stream(result.spliterator(), false)
            .map(N1qlQueryRow::value)
            .map(entry -> new EventEntry(entry, eventConfiguration))
            .filter(ed -> previousToken.get() == null ||
                !previousToken.get().knownEventIds().contains(ed.getEventIdentifier())
            )
            .map(event ->
                new TrackedCouchbaseEventEntry<>(
                    event,
                    previousToken.updateAndGet(token -> token == null
                        ? CouchbaseTrackingToken.of(event.getTimestamp(), event.getEventIdentifier())
                        : token.advanceTo(event.getTimestamp(), event.getEventIdentifier(), lookBackTime)
                    )
                )
            ).collect(toList());
    }

    private Long extractHighestSequenceNumber(JsonObject document) {
        return document.getLong(commitEntryConfiguration.lastSequenceNumberProperty());
    }

    @Override
    public Optional<Long> lastSequenceNumberFor(String aggregateIdentifier) {
        final String idProp = eventConfiguration.aggregateIdentifierProperty();
        final String seqProp = eventConfiguration.sequenceNumberProperty();
        final List<N1qlQueryRow> result = template.getEventBucket().query(
            N1qlQuery.simple(
                select(max(x(seqProp)).as(commitEntryConfiguration.lastSequenceNumberProperty()))
                    .from(template.getEventBucket().name()).where(x(idProp).eq(s(aggregateIdentifier))),
                N1qlParams.build().consistency(ScanConsistency.STATEMENT_PLUS))
        ).allRows();
        return !result.isEmpty() ? ofNullable(extractHighestSequenceNumber(result.get(0).value())) : empty();
    }

    @Override
    public TrackingToken createTailToken() {
        final String timestampProp = eventConfiguration.timestampProperty();
        final List<N1qlQueryRow> result = template.getEventBucket().query(
            N1qlQuery.simple(
                select(min(x(timestampProp)).as(eventConfiguration.timestampProperty()))
                    .from(template.getEventBucket().name()),
                N1qlParams.build().consistency(ScanConsistency.STATEMENT_PLUS)
            )
        ).allRows();
        return result.isEmpty() ? null :
            CouchbaseTrackingToken.of(
                (Long) result.get(0).value().get(eventConfiguration.timestampProperty()),
                Collections.emptyMap()
            );
    }

    @Override
    public TrackingToken createHeadToken() {
        return createTokenAt(Instant.now());
    }

    @Override
    public TrackingToken createTokenAt(Instant dateTime) {
        return CouchbaseTrackingToken.of(dateTime.toEpochMilli(), Collections.emptyMap());
    }

    private static boolean isDuplicateKeyException(Exception exception) {
        return exception instanceof DocumentAlreadyExistsException;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends BatchingEventStorageEngine.Builder {

        private CouchbaseTemplate template;
        private EventEntryConfiguration eventConfiguration = EventEntryConfiguration.getDefault();
        private CommitEntryConfiguration commitEntryConfiguration = new CommitEntryConfiguration.Builder().build();
        private Duration lookBackTime = Duration.ofMillis(1000L);

        private Builder() {
            persistenceExceptionResolver(CouchbaseEventStorageEngine::isDuplicateKeyException);
        }

        @Override
        public Builder snapshotSerializer(Serializer snapshotSerializer) {
            super.snapshotSerializer(snapshotSerializer);
            return this;
        }

        @Override
        public Builder upcasterChain(EventUpcaster upcasterChain) {
            super.upcasterChain(upcasterChain);
            return this;
        }

        @Override
        public Builder persistenceExceptionResolver(PersistenceExceptionResolver persistenceExceptionResolver) {
            super.persistenceExceptionResolver(persistenceExceptionResolver);
            return this;
        }

        @Override
        public Builder eventSerializer(Serializer eventSerializer) {
            super.eventSerializer(eventSerializer);
            return this;
        }

        @Override
        public Builder snapshotFilter(Predicate<? super DomainEventData<?>> snapshotFilter) {
            super.snapshotFilter(snapshotFilter);
            return this;
        }

        @Override
        public Builder batchSize(int batchSize) {
            super.batchSize(batchSize);
            return this;
        }

        public Builder couchbaseTemplate(CouchbaseTemplate template) {
            assertNonNull(template, "CouchbaseTemplate may not be null");
            this.template = template;
            return this;
        }

        public Builder eventConfiguration(EventEntryConfiguration eventConfiguration) {
            assertNonNull(eventConfiguration, "EventEntryConfiguration may not be null");
            this.eventConfiguration = eventConfiguration;
            return this;
        }

        public Builder commitEntryConfiguration(CommitEntryConfiguration commitEntryConfiguration) {
            assertNonNull(commitEntryConfiguration, "CommitEntryConfiguration may not be null");
            this.commitEntryConfiguration = commitEntryConfiguration;
            return this;
        }

        public Builder eventConfiguration(Duration lookBackTime) {
            assertNonNull(lookBackTime, "LookBackTime may not be null");
            this.lookBackTime = lookBackTime;
            return this;
        }

        public CouchbaseEventStorageEngine build() {
            return new CouchbaseEventStorageEngine(this);
        }

        @Override
        protected void validate() throws AxonConfigurationException {
            super.validate();
            assertNonNull(template, "The MongoTemplate is a hard requirement and should be provided");
        }
    }

    protected static class TrackedCouchbaseEventEntry<T> implements DomainEventData<T>, TrackedEventData<T> {

        private final DomainEventData<T> delegate;
        private final TrackingToken trackingToken;

        public TrackedCouchbaseEventEntry(DomainEventData<T> delegate, TrackingToken trackingToken) {
            this.delegate = delegate;
            this.trackingToken = trackingToken;
        }

        @Override
        public String getType() {
            return delegate.getType();
        }

        @Override
        public String getAggregateIdentifier() {
            return delegate.getAggregateIdentifier();
        }

        @Override
        public long getSequenceNumber() {
            return delegate.getSequenceNumber();
        }

        @Override
        public TrackingToken trackingToken() {
            return trackingToken;
        }

        @Override
        public String getEventIdentifier() {
            return delegate.getEventIdentifier();
        }

        @Override
        public Instant getTimestamp() {
            return delegate.getTimestamp();
        }

        @Override
        public SerializedObject<T> getMetaData() {
            return delegate.getMetaData();
        }

        @Override
        public SerializedObject<T> getPayload() {
            return delegate.getPayload();
        }
    }

}
