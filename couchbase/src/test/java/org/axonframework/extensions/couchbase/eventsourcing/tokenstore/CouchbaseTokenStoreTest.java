package org.axonframework.extensions.couchbase.eventsourcing.tokenstore;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.query.N1qlParams;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.Statement;
import com.couchbase.client.java.query.consistency.ScanConsistency;
import org.axonframework.eventhandling.GlobalSequenceTrackingToken;
import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.eventhandling.tokenstore.TokenStore;
import org.axonframework.eventhandling.tokenstore.UnableToClaimTokenException;
import org.axonframework.eventhandling.tokenstore.UnableToInitializeTokenException;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.json.JacksonSerializer;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.junit.Before;
import org.junit.Test;
import org.axonframework.extensions.couchbase.CouchbaseBucketManager;
import org.axonframework.extensions.couchbase.eventsourcing.eventstore.CouchbaseTemplate;
import org.axonframework.extensions.couchbase.eventsourcing.eventstore.DefaultCouchbaseTemplate;

import java.time.Duration;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.couchbase.client.java.query.Select.select;
import static com.couchbase.client.java.query.dsl.Expression.s;
import static com.couchbase.client.java.query.dsl.Expression.x;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class CouchbaseTokenStoreTest {

    private static final String TEST_PROCESSOR_NAME = "testProcessorName";
    private static final int TEST_SEGMENT = 10;
    private static final String TEST_OWNER = "testOwner";
    private static final String ANOTHER_OWNER = "anotherOwner";

    private CouchbaseTokenStore tokenStore;
    private CouchbaseTokenStore tokenStoreDifferentOwner;

    private CouchbaseTemplate couchbaseTemplate;
    private Serializer serializer;
    private TemporalAmount claimTimeout = Duration.ofSeconds(5);

    private CouchbaseBucketManager couchbaseBucketManager = CouchbaseBucketManager.INSTANCE;

    @Before
    public void init() {
        couchbaseTemplate = createCouchbaseTemplate();
        final Bucket tokenBucket = couchbaseTemplate.getTokenBucket();
        couchbaseBucketManager.clearBucket(tokenBucket.name());
        serializer = JacksonSerializer.builder().build();
        CouchbaseTokenStore.Builder tokenStoreBuilder = CouchbaseTokenStore.builder()
            .couchbaseTemplate(couchbaseTemplate)
            .serializer(serializer)
            .claimTimeout(claimTimeout);
        tokenStore = tokenStoreBuilder.nodeId(TEST_OWNER).build();
        tokenStore.ensureIndexes();
        tokenStoreDifferentOwner = tokenStoreBuilder.nodeId(ANOTHER_OWNER).build();
    }

    @Test
    public void testClaimAndUpdateToken() {
        tokenStore.initializeTokenSegments(TEST_PROCESSOR_NAME, TEST_SEGMENT + 1);
        assertNull(tokenStore.fetchToken(TEST_PROCESSOR_NAME, TEST_SEGMENT));
        TrackingToken token = new GlobalSequenceTrackingToken(1L);
        tokenStore.storeToken(token, TEST_PROCESSOR_NAME, TEST_SEGMENT);
        assertEquals(token, tokenStore.fetchToken(TEST_PROCESSOR_NAME, TEST_SEGMENT));
    }

    @Test
    public void testInitializeTokens() {
        tokenStore.initializeTokenSegments("test1", 7);
        assertThat(tokenStore.fetchSegments("test1")).containsExactly(0, 1, 2, 3, 4, 5, 6);
    }

    @Test
    public void testInitializeTokensAtGivenPosition() {
        tokenStore.initializeTokenSegments("test1", 3, new GlobalSequenceTrackingToken(10));
        int[] actual = tokenStore.fetchSegments("test1");
        assertThat(actual).containsExactly(0, 1, 2);
        for (int segment : actual) {
            assertEquals(new GlobalSequenceTrackingToken(10), tokenStore.fetchToken("test1", segment));
        }
    }

    @Test
    public void testInitializeTokensWhileAlreadyPresent() {
        assertThatThrownBy(() -> {
            tokenStore.fetchToken("test1", 1);
            tokenStore.initializeTokenSegments("test1", 7);
        }).isInstanceOf(UnableToClaimTokenException.class);
    }

    @Test
    public void testAttemptToClaimAlreadyClaimedToken() {
        assertThatThrownBy(() -> {
            assertNull(tokenStore.fetchToken(TEST_PROCESSOR_NAME, TEST_SEGMENT));
            TrackingToken token = new GlobalSequenceTrackingToken(1L);
            tokenStore.storeToken(token, TEST_PROCESSOR_NAME, TEST_SEGMENT);
            tokenStoreDifferentOwner.storeToken(token, TEST_PROCESSOR_NAME, TEST_SEGMENT);
        }).isInstanceOf(UnableToClaimTokenException.class);
    }

    @Test
    public void testAttemptToExtendClaimOnAlreadyClaimedToken() {
        assertThatThrownBy(() -> {
            assertNull(tokenStore.fetchToken(TEST_PROCESSOR_NAME, TEST_SEGMENT));
            tokenStoreDifferentOwner.extendClaim(TEST_PROCESSOR_NAME, TEST_SEGMENT);
        }).isInstanceOf(UnableToClaimTokenException.class);
    }

    @Test
    public void testClaimAndExtend() {
        TrackingToken token = new GlobalSequenceTrackingToken(1L);
        tokenStore.initializeSegment(token, TEST_PROCESSOR_NAME, TEST_SEGMENT);
        tokenStore.storeToken(token, TEST_PROCESSOR_NAME, TEST_SEGMENT);
        assertThatThrownBy(() -> {
            tokenStoreDifferentOwner.fetchToken(TEST_PROCESSOR_NAME, TEST_SEGMENT);
        }).isInstanceOf(UnableToClaimTokenException.class);
        tokenStore.extendClaim(TEST_PROCESSOR_NAME, TEST_SEGMENT);
    }

    @Test
    public void testReleaseClaimAndExtendClaim() {
        tokenStore.initializeSegment(null, TEST_PROCESSOR_NAME, TEST_SEGMENT);
        TrackingToken token = new GlobalSequenceTrackingToken(1L);
        tokenStore.storeToken(token, TEST_PROCESSOR_NAME, TEST_SEGMENT);

        assertThatThrownBy(() -> {
            tokenStoreDifferentOwner.fetchToken(TEST_PROCESSOR_NAME, TEST_SEGMENT);
        }).isInstanceOf(UnableToClaimTokenException.class);

        assertThatThrownBy(() -> {
            tokenStore.releaseClaim(TEST_PROCESSOR_NAME, TEST_SEGMENT);
            tokenStoreDifferentOwner.extendClaim(TEST_PROCESSOR_NAME, TEST_SEGMENT);
        }).isInstanceOf(UnableToClaimTokenException.class);
    }

    @Test
    public void testFetchSegments() {
        tokenStore.initializeTokenSegments("processor1", 3);
        tokenStore.initializeTokenSegments("processor2", 1);
        assertThat(tokenStore.fetchSegments("processor1")).containsExactly(0, 1, 2);
        assertThat(tokenStore.fetchSegments("processor2")).containsExactly(0);
        assertThat(tokenStore.fetchSegments("processor3")).isEmpty();
    }

    @Test
    public void testConcurrentAccess() throws Exception {
        final int attempts = 10;
        ExecutorService executorService = Executors.newFixedThreadPool(attempts);

        List<Future<Integer>> futures = new ArrayList<>();
        for (int i = 0; i < attempts; i++) {
            final int iteration = i;
            Future<Integer> future = executorService.submit(() -> {
                try {
                    String owner = String.valueOf(iteration);
                    TokenStore tokenStore = CouchbaseTokenStore.builder()
                        .couchbaseTemplate(couchbaseTemplate)
                        .serializer(serializer)
                        .claimTimeout(claimTimeout)
                        .nodeId(owner)
                        .build();
                    GlobalSequenceTrackingToken token = new GlobalSequenceTrackingToken(iteration);
                    tokenStore.initializeSegment(token, TEST_PROCESSOR_NAME, TEST_SEGMENT);
                    tokenStore.storeToken(token, TEST_PROCESSOR_NAME, TEST_SEGMENT);
                    return iteration;
                } catch (UnableToClaimTokenException exception) {
                    return null;
                }
            });
            futures.add(future);
        }
        executorService.shutdown();
        assertThat(executorService.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

        List<Future<Integer>> successfulAttempts = futures.stream()
            .filter(future -> {
                try {
                    return future.get() != null;
                } catch (InterruptedException | ExecutionException e) {
                    return false;
                }
            })
            .collect(Collectors.toList());
        assertThat(successfulAttempts.size()).isEqualTo(1);

        Integer iterationOfSuccessfulAttempt = successfulAttempts.get(0).get();
        TokenStore tokenStore = CouchbaseTokenStore.builder()
            .couchbaseTemplate(couchbaseTemplate)
            .serializer(serializer)
            .claimTimeout(claimTimeout)
            .nodeId(String.valueOf(iterationOfSuccessfulAttempt))
            .build();

        assertThat(tokenStore.fetchToken(TEST_PROCESSOR_NAME, TEST_SEGMENT)).isEqualTo(
            new GlobalSequenceTrackingToken(iterationOfSuccessfulAttempt)
        );
    }

    @Test
    public void testStoreAndFetchTokenResultsInTheSameTokenWithXStreamSerializer() {
        TokenStore tokenStore = createTokenStore(TEST_OWNER);
        GlobalSequenceTrackingToken testToken = new GlobalSequenceTrackingToken(100);
        String testProcessorName = "processorName";
        tokenStore.initializeSegment(testToken, TEST_PROCESSOR_NAME, TEST_SEGMENT);
        tokenStore.storeToken(testToken, TEST_PROCESSOR_NAME, TEST_SEGMENT);
        assertThat(tokenStore.fetchToken(TEST_PROCESSOR_NAME, TEST_SEGMENT)).isEqualTo(testToken);
    }

    @Test
    public void testStoreAndFetchTokenResultsInTheSameTokenWithJacksonSerializer() {
        TokenStore tokenStore = createTokenStore(TEST_OWNER);
        GlobalSequenceTrackingToken testToken = new GlobalSequenceTrackingToken(100);
        String testProcessorName = "processorName";
        int testSegment = 0;

        tokenStore.initializeSegment(testToken, testProcessorName, testSegment);

        tokenStore.storeToken(testToken, testProcessorName, testSegment);
        assertThat(tokenStore.fetchToken(testProcessorName, testSegment)).isEqualTo(testToken);
    }

    @Test
    public void testRequiresExplicitSegmentInitializationReturnsTrue() {
        assertThat(tokenStore.requiresExplicitSegmentInitialization()).isTrue();
    }

    @Test
    public void testInitializeSegmentForNullTokenOnlyCreatesSegments() {
        tokenStore.initializeSegment(null, TEST_PROCESSOR_NAME, TEST_SEGMENT);
        assertThat(tokenStore.fetchSegments(TEST_PROCESSOR_NAME)).containsExactly(TEST_SEGMENT);
        assertThat(tokenStore.fetchToken(TEST_PROCESSOR_NAME, TEST_SEGMENT)).isNull();
    }

    @Test
    public void testInitializeSegmentInsertsTheProvidedTokenAndInitializesTheGivenSegment() {
        GlobalSequenceTrackingToken testToken = new GlobalSequenceTrackingToken(100);
        tokenStore.initializeSegment(testToken, TEST_PROCESSOR_NAME, TEST_SEGMENT);
        assertThat(tokenStore.fetchSegments(TEST_PROCESSOR_NAME)).containsExactly(TEST_SEGMENT);
        assertThat(tokenStore.fetchToken(TEST_PROCESSOR_NAME, TEST_SEGMENT)).isEqualTo(testToken);
    }

    @Test
    public void testInitializeSegmentThrowsUnableToInitializeTokenExceptionForDuplicateKey() {
        GlobalSequenceTrackingToken testToken = new GlobalSequenceTrackingToken(100);
        tokenStore.initializeSegment(testToken, TEST_PROCESSOR_NAME, TEST_SEGMENT);
        // Initializes the given token twice, causing the exception
        assertThatThrownBy(() -> tokenStore.initializeSegment(testToken, TEST_PROCESSOR_NAME, TEST_SEGMENT))
            .isInstanceOf(UnableToInitializeTokenException.class);
    }

    @Test
    public void testDeleteTokenRemovesTheSpecifiedToken() {
        tokenStore.initializeSegment(null, TEST_PROCESSOR_NAME, TEST_SEGMENT);
        // Claim the token by fetching it to be able to delete it
        tokenStore.fetchToken(TEST_PROCESSOR_NAME, TEST_SEGMENT);
        tokenStore.deleteToken(TEST_PROCESSOR_NAME, TEST_SEGMENT);

        final Statement select = select(couchbaseTemplate.getTokenBucket().name() + ".*")
            .from(couchbaseTemplate.getTokenBucket().name())
            .where(x("processorName").eq(s(TEST_PROCESSOR_NAME)).and(x("segment").eq(TEST_SEGMENT)));

        final N1qlQueryResult result = couchbaseTemplate.getTokenBucket().query(
            N1qlQuery.simple(select, N1qlParams.build().consistency(ScanConsistency.STATEMENT_PLUS))
        );

        assertThat(result.info().resultCount()).isEqualTo(0);
    }

    @Test
    public void testDeleteTokenThrowsUnableToClaimTokenExceptionIfTheCallingProcessDoesNotOwnTheToken() {
        tokenStore.initializeSegment(null, TEST_PROCESSOR_NAME, TEST_SEGMENT);
        // The token should be fetched to be claimed by somebody, so this should throw a UnableToClaimTokenException
        assertThatThrownBy(() -> {
            tokenStore.deleteToken(TEST_PROCESSOR_NAME, TEST_SEGMENT);
        }).isInstanceOf(UnableToClaimTokenException.class);
    }

    private TokenStore createTokenStore(String nodeId) {
        return CouchbaseTokenStore.builder()
            .serializer(XStreamSerializer.builder().build())
            .couchbaseTemplate(couchbaseTemplate)
            .claimTimeout(claimTimeout)
            .nodeId(nodeId)
            .build();
    }

    private CouchbaseTemplate createCouchbaseTemplate() {
        final Bucket bucket = couchbaseBucketManager.inventoryBucket();
        return new DefaultCouchbaseTemplate(bucket, bucket, bucket, bucket);
    }

}
