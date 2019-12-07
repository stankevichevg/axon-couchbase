package org.axonframework.extensions.couchbase.eventsourcing.tokenstore;

import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.DocumentAlreadyExistsException;
import com.couchbase.client.java.query.Insert;
import com.couchbase.client.java.query.N1qlParams;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.couchbase.client.java.query.Statement;
import com.couchbase.client.java.query.consistency.ScanConsistency;
import com.couchbase.client.java.query.dsl.Expression;
import com.couchbase.client.java.query.dsl.path.InitialInsertPath;
import com.couchbase.client.java.query.dsl.path.InsertValuesPath;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.eventhandling.tokenstore.AbstractTokenEntry;
import org.axonframework.eventhandling.tokenstore.GenericTokenEntry;
import org.axonframework.eventhandling.tokenstore.TokenStore;
import org.axonframework.eventhandling.tokenstore.UnableToClaimTokenException;
import org.axonframework.eventhandling.tokenstore.UnableToInitializeTokenException;
import org.axonframework.eventhandling.tokenstore.jpa.TokenEntry;
import org.axonframework.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.axonframework.extensions.couchbase.eventsourcing.eventstore.CouchbaseTemplate;

import java.lang.management.ManagementFactory;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.List;
import java.util.Objects;

import static com.couchbase.client.java.document.JsonDocument.create;
import static com.couchbase.client.java.query.Delete.deleteFrom;
import static com.couchbase.client.java.query.Index.createIndex;
import static com.couchbase.client.java.query.Select.select;
import static com.couchbase.client.java.query.Update.update;
import static com.couchbase.client.java.query.dsl.Expression.NULL;
import static com.couchbase.client.java.query.dsl.Expression.par;
import static com.couchbase.client.java.query.dsl.Expression.s;
import static com.couchbase.client.java.query.dsl.Expression.x;
import static com.couchbase.client.java.query.dsl.Sort.asc;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.apache.commons.text.StringEscapeUtils.escapeXml11;
import static org.apache.commons.text.StringEscapeUtils.unescapeXml;
import static org.axonframework.common.BuilderUtils.assertNonNull;
import static org.axonframework.common.BuilderUtils.assertThat;


public class CouchbaseTokenStore implements TokenStore {

    private final static Logger logger = LoggerFactory.getLogger(CouchbaseTokenStore.class);
    private final static Clock clock = Clock.systemUTC();

    private final CouchbaseTemplate template;
    private final Serializer serializer;
    private final TemporalAmount claimTimeout;
    private final String nodeId;

    protected CouchbaseTokenStore(Builder builder) {
        builder.validate();
        this.template = builder.template;
        this.serializer = builder.serializer;
        this.claimTimeout = builder.claimTimeout;
        this.nodeId = builder.nodeId;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void storeToken(TrackingToken token, String processorName, int segment) throws UnableToClaimTokenException {
        AbstractTokenEntry<String> tokenEntry = new GenericTokenEntry<String>(
            token, serializer, String.class, processorName, segment
        );
        tokenEntry.claim(nodeId, claimTimeout);
        final Statement statement = update(template.getTokenBucket().name())
            .set("owner", s(nodeId))
            .set("timestamp", tokenEntry.timestamp().toEpochMilli())
            .set("token", escapeXml11(tokenEntry.getSerializedToken().getData()))
            .set("tokenType", s(tokenEntry.getSerializedToken().getType().getName()))
            .where(claimableTokenEntryFilter(processorName, segment));
        final N1qlQueryResult result = template.getTokenBucket().query(
            N1qlQuery.simple(statement, N1qlParams.build().consistency(ScanConsistency.REQUEST_PLUS))
        );
        if (result.info().mutationCount() == 0) {
            throw new UnableToClaimTokenException(format(
                "Unable to claim token '%s[%s]'. It is either already claimed or it does not exist",
                processorName, segment
            ));
        }
    }

    @Override
    public void initializeTokenSegments(String processorName, int segmentCount) throws UnableToClaimTokenException {
        initializeTokenSegments(processorName, segmentCount, null);
    }

    @Override
    public void initializeTokenSegments(String processorName, int segmentCount, TrackingToken initialToken)
        throws UnableToClaimTokenException {
        if (fetchSegments(processorName).length > 0) {
            throw new UnableToClaimTokenException(
                    "Unable to initialize segments. Some tokens were already present for the given processor."
            );
        }
        InitialInsertPath insert = Insert.insertInto(template.getTokenBucket().name());
        InsertValuesPath insertValuesPath = insert.values(
            tokenKey(processorName, 0),
            tokenEntryToDocument(new GenericTokenEntry<>(initialToken, serializer, String.class, processorName, 0))
        );
        List<GenericTokenEntry> tokens = range(1, segmentCount).mapToObj(
            segment -> new GenericTokenEntry<>(initialToken, serializer, String.class, processorName, segment)
        ).collect(toList());
        for (GenericTokenEntry token : tokens) {
            insertValuesPath = insertValuesPath.values(
                tokenKey(processorName, token.getSegment()),
                tokenEntryToDocument(token)
            );
        }
        final N1qlQueryResult result = template.getTokenBucket().query(
            N1qlQuery.simple(insertValuesPath, N1qlParams.build().consistency(ScanConsistency.STATEMENT_PLUS))
        );
        if (!result.finalSuccess()) {
            throw new UnableToClaimTokenException("Unable to initialize segments. Insert statement has failed.");
        }
    }

    @Override
    public TrackingToken fetchToken(String processorName, int segment) throws UnableToClaimTokenException {
        final Statement statement = update(template.getTokenBucket().name())
            .set("owner", nodeId)
            .set("timestamp", clock.millis())
            .where(claimableTokenEntryFilter(processorName, segment))
            .returning(template.getTokenBucket().name() + ".*");
        final N1qlQueryResult result = template.getTokenBucket().query(
            N1qlQuery.simple(statement, N1qlParams.build().consistency(ScanConsistency.STATEMENT_PLUS))
        );
        if (result.info().resultCount() == 0) {
            throw new UnableToClaimTokenException(
                format("Unable to claim token '%s[%s]'. It has not been initialized yet", processorName, segment)
            );
        }
        final JsonObject document = result.rows().next().value();
        AbstractTokenEntry<?> tokenEntry = documentToTokenEntry(document);
        if (!tokenEntry.claim(nodeId, claimTimeout)) {
            throw new UnableToClaimTokenException(format(
                "Unable to claim token '%s[%s]'. It is owned by '%s'", processorName, segment, tokenEntry.getOwner()
            ));
        }
        return tokenEntry.getToken(serializer);
    }

    @Override
    public void extendClaim(String processorName, int segment) throws UnableToClaimTokenException {

        final Statement statement = update(template.getTokenBucket().name())
            .set("timestamp", TokenEntry.clock.instant().toEpochMilli())
            .where(
                x("processorName").eq(s(processorName))
                    .and(x("segment").eq(segment))
                    .and(x("owner").eq(s(nodeId)))
            );
        final N1qlQueryResult result = template.getTokenBucket().query(
            N1qlQuery.simple(statement, N1qlParams.build().consistency(ScanConsistency.STATEMENT_PLUS))
        );
        if (result.info().mutationCount() == 0) {
            throw new UnableToClaimTokenException(format(
                    "Unable to extend claim on token token '%s[%s]'. It is owned by another segment.",
                    processorName, segment
            ));
        }
    }

    @Override
    public void releaseClaim(String processorName, int segment) {
        final Statement statement = update(template.getTokenBucket().name())
            .set("owner", NULL())
            .where(
                x("processorName").eq(s(processorName))
                    .and(x("segment").eq(segment))
                    .and(x("owner").eq(s(nodeId)))
            );
        final N1qlQueryResult result = template.getTokenBucket().query(
            N1qlQuery.simple(statement, N1qlParams.build().consistency(ScanConsistency.STATEMENT_PLUS))
        );
        if (result.info().mutationCount() == 0) {
            logger.warn("Releasing claim of token {}/{} failed. It was owned by another node.", processorName, segment);
        }
    }

    @Override
    public void initializeSegment(TrackingToken token, String processorName, int segment)
        throws UnableToInitializeTokenException {
        try {
            AbstractTokenEntry<?> tokenEntry = new GenericTokenEntry<>(
                token, serializer, String.class, processorName, segment
            );
            final String id = processorName + ":" + segment;
            template.getTokenBucket().insert(create(id, tokenEntryToDocument(tokenEntry)));
        } catch (DocumentAlreadyExistsException exception) {
            throw new UnableToInitializeTokenException(
                format("Unable to initialize token '%s[%s]'", processorName, segment)
            );
        }
    }

    @Override
    public void deleteToken(String processorName, int segment) throws UnableToClaimTokenException {
        final Statement statement = deleteFrom(template.getTokenBucket().name())
            .where(
                x("processorName").eq(s(processorName)).and(x("segment").eq(segment)).and(x("owner").eq(s(nodeId)))
            );
        final N1qlQueryResult result = template.getTokenBucket().query(
            N1qlQuery.simple(statement, N1qlParams.build().consistency(ScanConsistency.STATEMENT_PLUS))
        );
        if (result.info().mutationCount() == 0) {
            throw new UnableToClaimTokenException("Unable to remove token. It is not owned by " + nodeId);
        }
    }

    @Override
    public boolean requiresExplicitSegmentInitialization() {
        return true;
    }

    @Override
    public int[] fetchSegments(String processorName) {

        final Statement statement = select(x("segment"))
            .from(template.getTokenBucket().name())
            .where(x("processorName").eq(s(processorName)))
            .orderBy(asc(x("segment")));

        final N1qlQueryResult result = template.getTokenBucket().query(
            N1qlQuery.simple(statement, N1qlParams.build().consistency(ScanConsistency.STATEMENT_PLUS))
        );
        List<Integer> segments = result.allRows().stream()
            .map(N1qlQueryRow::value)
            .map(o -> o.getInt("segment"))
            .collect(toList());
        int[] ints = new int[segments.size()];
        for (int i = 0; i < ints.length; i++) {
            ints[i] = segments.get(i);
        }
        return ints;
    }

    public void ensureIndexes() {
        template.getTokenBucket().query(
            N1qlQuery.simple(
                createIndex("tokensIndex").on(template.getTokenBucket().name(), x("processorName"), x("segment")),
                N1qlParams.build().consistency(ScanConsistency.STATEMENT_PLUS)
            )
        );
    }

    private Expression claimableTokenEntryFilter(String processorName, int segment) {
        return x("processorName").eq(s(processorName))
            .and(x("segment").eq(segment))
            .and(par(
                x("owner").eq(s(nodeId))
                    .or(x("owner").isNull())
                    .or(x("timestamp").lt(clock.instant().minus(claimTimeout).toEpochMilli()))
            ));
    }

    private JsonObject tokenEntryToDocument(AbstractTokenEntry<?> tokenEntry) {
        return JsonObject.create()
            .put("processorName", tokenEntry.getProcessorName())
            .put("segment", tokenEntry.getSegment())
            .put("owner", tokenEntry.getOwner())
            .put("timestamp", tokenEntry.timestamp().toEpochMilli())
            .put("token", tokenEntry.getSerializedToken() == null ? null : tokenEntry.getSerializedToken().getData())
            .put("tokenType", tokenEntry.getSerializedToken() == null ? null :
                tokenEntry.getSerializedToken().getType().getName());
    }

    private AbstractTokenEntry<String> documentToTokenEntry(JsonObject document) {
        return new GenericTokenEntry<String>(
                unescapeXml(document.getString("token")),
                document.getString("tokenType"),
                Instant.ofEpochMilli(document.getLong("timestamp")).toString(),
                document.getString("owner"),
                document.getString("processorName"),
                document.getInt("segment"),
                String.class
        );
    }

    private String tokenKey(String processorName, int segment) {
        return "token:" + processorName + ":" + segment;
    }

    public static class Builder {

        private CouchbaseTemplate template;
        private Serializer serializer;
        private TemporalAmount claimTimeout = Duration.ofSeconds(10);
        private String nodeId = ManagementFactory.getRuntimeMXBean().getName();

        public Builder couchbaseTemplate(CouchbaseTemplate template) {
            assertNonNull(template, "CouchbaseTemplate may not be null");
            this.template = template;
            return this;
        }

        public Builder serializer(Serializer serializer) {
            assertNonNull(serializer, "Serializer may not be null");
            this.serializer = serializer;
            return this;
        }

        public Builder claimTimeout(TemporalAmount claimTimeout) {
            assertNonNull(claimTimeout, "The claim timeout may not be null");
            this.claimTimeout = claimTimeout;
            return this;
        }

        public Builder nodeId(String nodeId) {
            assertNodeId(nodeId, "The nodeId may not be null or empty");
            this.nodeId = nodeId;
            return this;
        }

        public CouchbaseTokenStore build() {
            return new CouchbaseTokenStore(this);
        }

        protected void validate() throws AxonConfigurationException {
            assertNonNull(template, "The CouchbaseTemplate is a hard requirement and should be provided");
            assertNonNull(serializer, "The Serializer is a hard requirement and should be provided");
        }

        private void assertNodeId(String nodeId, String exceptionMessage) {
            assertThat(nodeId, name -> Objects.nonNull(name) && !"".equals(name), exceptionMessage);
        }
    }
}
