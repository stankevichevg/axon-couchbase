package org.axonframework.extensions.couchbase.eventsourcing.eventstore;

import com.couchbase.client.java.Bucket;

public interface CouchbaseTemplate {

    Bucket getEventBucket();

    Bucket getTokenBucket();

    Bucket getSagaBucket();

    Bucket getSnapshotBucket();

}
