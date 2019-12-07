package org.axonframework.extensions.couchbase.eventhandling.saga.repository;

import com.couchbase.client.java.query.N1qlParams;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.consistency.ScanConsistency;
import org.axonframework.modelling.saga.AssociationValue;
import org.axonframework.modelling.saga.AssociationValues;
import org.axonframework.modelling.saga.AssociationValuesImpl;
import org.axonframework.modelling.saga.repository.SagaStore;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.junit.Before;
import org.junit.Test;
import org.axonframework.extensions.couchbase.CouchbaseBucketManager;
import org.axonframework.extensions.couchbase.eventsourcing.eventstore.CouchbaseTemplate;
import org.axonframework.extensions.couchbase.eventsourcing.eventstore.DefaultCouchbaseTemplate;

import java.util.Set;
import java.util.UUID;

import static com.couchbase.client.java.document.JsonDocument.create;
import static com.couchbase.client.java.query.Select.select;
import static com.couchbase.client.java.query.dsl.Expression.s;
import static com.couchbase.client.java.query.dsl.Expression.x;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.axonframework.extensions.couchbase.eventhandling.saga.repository.SagaEntry.SAGA_IDENTIFIER;

public class CouchbaseSagaStoreTest {

    private static final AssociationValue ASSOCIATION = new AssociationValue("key", "value");

    private CouchbaseBucketManager couchbaseBucketManager = CouchbaseBucketManager.INSTANCE;
    private CouchbaseTemplate couchbaseTemplate;

    private CouchbaseSagaStore sagaStore;

    @Before
    public void setUp() {
        couchbaseBucketManager.clearBuckets();
        couchbaseTemplate = new DefaultCouchbaseTemplate(
            couchbaseBucketManager.inventoryBucket(),
            couchbaseBucketManager.inventoryBucket(),
            couchbaseBucketManager.inventoryBucket(),
            couchbaseBucketManager.inventoryBucket()
        );
        sagaStore = CouchbaseSagaStore.builder().template(couchbaseTemplate).build();
    }

    @Test
    public void testLoadSagaOfDifferentTypesWithSameAssociationValue_SagaFound() {
        MyTestSaga testSaga = new MyTestSaga();
        MyOtherTestSaga otherTestSaga = new MyOtherTestSaga();
        sagaStore.insertSaga(MyTestSaga.class, "test1", testSaga, singleton(ASSOCIATION));
        sagaStore.insertSaga(MyTestSaga.class, "test2", otherTestSaga, singleton(ASSOCIATION));
        Set<String> actual = sagaStore.findSagas(MyTestSaga.class, ASSOCIATION);
        assertEquals(1, actual.size());
        assertEquals(
            MyTestSaga.class,
            sagaStore.loadSaga(MyTestSaga.class, actual.iterator().next()).saga().getClass()
        );

        Set<String> actual2 = sagaStore.findSagas(MyOtherTestSaga.class, ASSOCIATION);
        assertEquals(1, actual2.size());
        assertEquals(
            MyOtherTestSaga.class,
            sagaStore.loadSaga(MyOtherTestSaga.class, actual2.iterator().next()).saga().getClass()
        );
        final N1qlQueryResult res = findSaga("test1");
        assertEquals("Amount of found sagas is not as expected", 1, res.info().resultCount());
    }

    private N1qlQueryResult findSaga(String sagaIdentifier) {
        return couchbaseTemplate.getSagaBucket().query(
            N1qlQuery.simple(
                select(couchbaseTemplate.getSagaBucket().name() + ".*")
                    .from(couchbaseTemplate.getSagaBucket().name())
                    .where(x(SAGA_IDENTIFIER).eq(s(sagaIdentifier))),
                N1qlParams.build().consistency(ScanConsistency.STATEMENT_PLUS)
            )
        );
    }

    @Test
    public void testLoadSagaOfDifferentTypesWithSameAssociationValue_NoSagaFound() {
        MyTestSaga testSaga = new MyTestSaga();
        MyOtherTestSaga otherTestSaga = new MyOtherTestSaga();
        sagaStore.insertSaga(MyTestSaga.class, "test1", testSaga, singleton(ASSOCIATION));
        sagaStore.insertSaga(MyTestSaga.class, "test2", otherTestSaga, singleton(ASSOCIATION));
        Set<String> actual = sagaStore.findSagas(NonExistentSaga.class, ASSOCIATION);
        assertTrue("Didn't expect any sagas", actual.isEmpty());
    }

    @Test
    public void testLoadSagaOfDifferentTypesWithSameAssociationValue_SagaDeleted() {
        MyTestSaga testSaga = new MyTestSaga();
        MyOtherTestSaga otherTestSaga = new MyOtherTestSaga();
        sagaStore.insertSaga(MyTestSaga.class, "test1", testSaga, singleton(ASSOCIATION));
        sagaStore.insertSaga(MyTestSaga.class, "test2", otherTestSaga, singleton(ASSOCIATION));
        sagaStore.deleteSaga(MyTestSaga.class, "test1", singleton(ASSOCIATION));
        Set<String> actual = sagaStore.findSagas(MyTestSaga.class, ASSOCIATION);
        assertTrue("Didn't expect any sagas", actual.isEmpty());
        final N1qlQueryResult res = findSaga("test1");
        assertEquals("No saga is expected after .end and .commit", 0, res.info().resultCount());
    }

    @Test
    public void testAddAndLoadSaga_ByIdentifier() {
        MyTestSaga saga = new MyTestSaga();
        sagaStore.insertSaga(MyTestSaga.class, "test1", saga, singleton(ASSOCIATION));
        SagaStore.Entry<MyTestSaga> loaded = sagaStore.loadSaga(MyTestSaga.class, "test1");
        assertEquals(singleton(ASSOCIATION), loaded.associationValues());
        assertEquals(MyTestSaga.class, loaded.saga().getClass());
        assertNotNull(couchbaseTemplate.getSagaBucket().get(sagaStore.sagaKey("test1")));
    }

    @Test
    public void testAddAndLoadSaga_ByAssociationValue() {
        MyTestSaga saga = new MyTestSaga();
        sagaStore.insertSaga(MyTestSaga.class, "test1", saga, singleton(ASSOCIATION));
        Set<String> loaded = sagaStore.findSagas(MyTestSaga.class, ASSOCIATION);
        assertEquals(1, loaded.size());
        SagaStore.Entry<MyTestSaga> loadedSaga = sagaStore.loadSaga(MyTestSaga.class, loaded.iterator().next());
        assertEquals(singleton(ASSOCIATION), loadedSaga.associationValues());
        assertNotNull(couchbaseTemplate.getSagaBucket().get(sagaStore.sagaKey("test1")));
    }

    @SuppressWarnings("UnusedAssignment")
    @Test
    public void testAddAndLoadSaga_MultipleHitsByAssociationValue() {
        String identifier1 = UUID.randomUUID().toString();
        String identifier2 = UUID.randomUUID().toString();
        MyTestSaga saga1 = new MyTestSaga();
        MyOtherTestSaga saga2 = new MyOtherTestSaga();
        sagaStore.insertSaga(MyTestSaga.class, identifier1, saga1, singleton(ASSOCIATION));
        sagaStore.insertSaga(MyOtherTestSaga.class, identifier2, saga2, singleton(ASSOCIATION));

        // load saga1
        Set<String> loaded1 = sagaStore.findSagas(MyTestSaga.class, ASSOCIATION);
        assertEquals(1, loaded1.size());
        SagaStore.Entry<MyTestSaga> loadedSaga1 = sagaStore.loadSaga(MyTestSaga.class, loaded1.iterator().next());
        assertEquals(singleton(ASSOCIATION), loadedSaga1.associationValues());
        assertNotNull(couchbaseTemplate.getSagaBucket().get(sagaStore.sagaKey(identifier1)));

        // load saga2
        Set<String> loaded2 = sagaStore.findSagas(MyOtherTestSaga.class, ASSOCIATION);
        assertEquals(1, loaded2.size());
        SagaStore.Entry<MyOtherTestSaga> loadedSaga2 =
            sagaStore.loadSaga(MyOtherTestSaga.class, loaded2.iterator().next());
        assertEquals(singleton(ASSOCIATION), loadedSaga2.associationValues());
        assertNotNull(couchbaseTemplate.getSagaBucket().get(sagaStore.sagaKey(identifier2)));
    }

    @Test
    public void testAddAndLoadSaga_AssociateValueAfterStorage() {
        String identifier = UUID.randomUUID().toString();
        MyTestSaga saga = new MyTestSaga();
        sagaStore.insertSaga(MyTestSaga.class, identifier, saga, singleton(ASSOCIATION));

        Set<String> loaded = sagaStore.findSagas(MyTestSaga.class, ASSOCIATION);
        assertEquals(1, loaded.size());
        SagaStore.Entry<MyTestSaga> loadedSaga = sagaStore.loadSaga(MyTestSaga.class, loaded.iterator().next());
        assertEquals(singleton(ASSOCIATION), loadedSaga.associationValues());
        assertNotNull(couchbaseTemplate.getSagaBucket().get(sagaStore.sagaKey(identifier)));
    }

    @Test
    public void testLoadSaga_NotFound() {
        assertNull(sagaStore.loadSaga(MyTestSaga.class, "123456"));
    }

    @Test
    public void testLoadSaga_AssociationValueRemoved() {
        String identifier = UUID.randomUUID().toString();
        MyTestSaga saga = new MyTestSaga();
        SagaEntry<MyTestSaga> testSagaEntry = new SagaEntry<>(
            identifier, saga, singleton(ASSOCIATION), XStreamSerializer.builder().build()
        );
        couchbaseTemplate.getSagaBucket().insert(create(sagaStore.sagaKey(identifier), testSagaEntry.asDocument()));

        SagaStore.Entry<MyTestSaga> loaded = sagaStore.loadSaga(MyTestSaga.class, identifier);
        AssociationValues av = new AssociationValuesImpl(loaded.associationValues());
        av.remove(ASSOCIATION);
        sagaStore.updateSaga(MyTestSaga.class, identifier, loaded.saga(), av);
        Set<String> found = sagaStore.findSagas(MyTestSaga.class, ASSOCIATION);
        assertEquals(0, found.size());
    }

    @Test
    public void testSaveSaga() {
        String identifier = UUID.randomUUID().toString();
        MyTestSaga saga = new MyTestSaga();
        XStreamSerializer serializer = XStreamSerializer.builder().build();

        sagaStore.insertSaga(saga.getClass(), identifier, saga, emptySet());

        SagaStore.Entry<MyTestSaga> loaded = sagaStore.loadSaga(MyTestSaga.class, identifier);
        loaded.saga().counter = 1;
        sagaStore.updateSaga(MyTestSaga.class,
            identifier,
            loaded.saga(),
            new AssociationValuesImpl(loaded.associationValues())
        );

        final N1qlQueryResult res = findSaga(identifier);
        assertNotEquals(0, res.info().resultCount());
        SagaEntry entry = new SagaEntry(res.rows().next().value());

        MyTestSaga actualSaga = (MyTestSaga) entry.getSaga(serializer);
        assertNotSame(loaded, actualSaga);
        assertEquals(1, actualSaga.counter);
    }

    @Test
    public void testEndSaga() {
        String identifier = UUID.randomUUID().toString();
        MyTestSaga saga = new MyTestSaga();
        SagaEntry<MyTestSaga> testSagaEntry = new SagaEntry<>(
            identifier, saga, singleton(ASSOCIATION), XStreamSerializer.builder().build()
        );
        couchbaseTemplate.getSagaBucket().insert(create(sagaStore.sagaKey(identifier), testSagaEntry.asDocument()));
        sagaStore.deleteSaga(MyTestSaga.class, identifier, singleton(ASSOCIATION));
        assertEquals(0, findSaga(identifier).info().resultCount());
    }

    private static class MyTestSaga {

        private int counter = 0;
    }

    private static class MyOtherTestSaga {

    }

    private class NonExistentSaga {

    }

}
