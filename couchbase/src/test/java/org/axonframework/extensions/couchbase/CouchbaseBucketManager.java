package org.axonframework.extensions.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.bucket.BucketType;
import com.couchbase.client.java.cluster.DefaultBucketSettings;
import com.couchbase.client.java.query.N1qlParams;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.consistency.ScanConsistency;
import org.testcontainers.couchbase.CouchbaseContainer;

import java.util.HashMap;
import java.util.Map;

public final class CouchbaseBucketManager {

    public static final CouchbaseBucketManager INSTANCE = new CouchbaseBucketManager();

    protected static final String INVENTORY_BUCKET_NAME = "inventory";
    protected static final String INVENTORY_BUCKET_PASSWORD = "password";

    private final CouchbaseContainer couchbaseContainer = initCouchbaseContainer();

    private final Map<String, Bucket> buckets = new HashMap<>();

    private CouchbaseBucketManager() {
    }

    private static CouchbaseContainer initCouchbaseContainer() {
        CouchbaseContainer couchbaseContainer = new CouchbaseContainer()
            .withNewBucket(
                DefaultBucketSettings.builder()
                    .enableFlush(true)
                    .name(INVENTORY_BUCKET_NAME)
                    .password(INVENTORY_BUCKET_PASSWORD)
                    .quota(100)
                    .replicas(0)
                    .type(BucketType.COUCHBASE)
                    .build()
            );
        couchbaseContainer.start();
        return couchbaseContainer;
    }

    public void clearBuckets() {
        buckets.keySet().forEach(this::clearBucket);
    }

    public void clearBucket(String name) {
        if (buckets.containsKey(name)) {
            final Bucket bucket = buckets.get(name);
            if (couchbaseContainer.isIndex() && couchbaseContainer.isQuery() && couchbaseContainer.isPrimaryIndex()) {
                bucket.query(
                    N1qlQuery.simple(
                        String.format("DELETE FROM `%s`", bucket.name()),
                        N1qlParams.build().consistency(ScanConsistency.STATEMENT_PLUS)
                    ));
            } else {
                bucket.bucketManager().flush();
            }
        }
    }

    public Bucket inventoryBucket() {
        return openBucket(INVENTORY_BUCKET_NAME, INVENTORY_BUCKET_PASSWORD);
    }

    public Bucket openBucket(String bucketName, String password) {
        if (buckets.containsKey(bucketName)) {
            return buckets.get(bucketName);
        } else {
            CouchbaseCluster cluster = couchbaseContainer.getCouchbaseCluster();
            final Bucket bucket = cluster.openBucket(bucketName, password);
            buckets.put(bucketName, bucket);
            return bucket;
        }
    }

}
