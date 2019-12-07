package org.axonframework.extensions.couchbase.eventsourcing.eventstore;

import com.couchbase.client.java.Bucket;

public class DefaultCouchbaseTemplate implements CouchbaseTemplate {

    private final Bucket eventBucket;
    private final Bucket tokenBucket;
    private final Bucket sagaBucket;
    private final Bucket snapshotBucket;

    public DefaultCouchbaseTemplate(
        Bucket eventBucket, Bucket tokenBucket, Bucket sagaBucket, Bucket snapshotBucket) {
        this.eventBucket = eventBucket;
        this.tokenBucket = tokenBucket;
        this.sagaBucket = sagaBucket;
        this.snapshotBucket = snapshotBucket;
    }

    @Override
    public Bucket getEventBucket() {
        return eventBucket;
    }

    @Override
    public Bucket getTokenBucket() {
        return tokenBucket;
    }

    @Override
    public Bucket getSagaBucket() {
        return sagaBucket;
    }

    @Override
    public Bucket getSnapshotBucket() {
        return snapshotBucket;
    }

}
