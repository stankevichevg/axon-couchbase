package org.axonframework.extensions.couchbase.eventhandling.saga.repository;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlParams;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.couchbase.client.java.query.Statement;
import com.couchbase.client.java.query.consistency.ScanConsistency;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.modelling.saga.AssociationValue;
import org.axonframework.modelling.saga.AssociationValues;
import org.axonframework.modelling.saga.repository.SagaStore;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.axonframework.extensions.couchbase.eventsourcing.eventstore.CouchbaseTemplate;

import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import static com.couchbase.client.java.document.JsonDocument.create;
import static com.couchbase.client.java.query.Select.select;
import static com.couchbase.client.java.query.Update.update;
import static com.couchbase.client.java.query.dsl.Expression.s;
import static com.couchbase.client.java.query.dsl.Expression.x;
import static com.couchbase.client.java.query.dsl.functions.Collections.anyIn;
import static java.util.Optional.ofNullable;
import static org.axonframework.common.BuilderUtils.assertNonNull;
import static org.axonframework.extensions.couchbase.eventhandling.saga.repository.SagaEntry.ASSOCIATIONS;
import static org.axonframework.extensions.couchbase.eventhandling.saga.repository.SagaEntry.ASSOCIATION_KEY;
import static org.axonframework.extensions.couchbase.eventhandling.saga.repository.SagaEntry.ASSOCIATION_VALUE;
import static org.axonframework.extensions.couchbase.eventhandling.saga.repository.SagaEntry.SAGA_IDENTIFIER;
import static org.axonframework.extensions.couchbase.eventhandling.saga.repository.SagaEntry.SAGA_TYPE;
import static org.axonframework.extensions.couchbase.eventhandling.saga.repository.SagaEntry.SERIALIZED_SAGA;

public class CouchbaseSagaStore implements SagaStore<Object> {

    private final CouchbaseTemplate template;
    private final Serializer serializer;

    protected CouchbaseSagaStore(Builder builder) {
        builder.validate();
        this.template = builder.template;
        this.serializer = builder.serializer;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public <S> Entry<S> loadSaga(Class<S> sagaType, String sagaIdentifier) {
        final Optional<JsonObject> result = ofNullable(
            template.getSagaBucket().get(sagaKey(sagaIdentifier))
        ).map(JsonDocument::content);

        if (!result.isPresent()) {
            return null;
        }
        SagaEntry<S> sagaEntry = new SagaEntry<S>(result.get());
        S loadedSaga = sagaEntry.getSaga(serializer);
        return new Entry<S>() {
            @Override
            public Set<AssociationValue> associationValues() {
                return sagaEntry.getAssociationValues();
            }

            @Override
            public S saga() {
                return loadedSaga;
            }
        };
    }

    protected String sagaKey(String sagaIdentifier) {
        return "saga:" + sagaIdentifier;
    }

    @Override
    public Set<String> findSagas(Class<?> sagaType, AssociationValue av) {

        final Statement statement = select(x(SAGA_IDENTIFIER))
            .from(template.getSagaBucket().name())
            .where(
                x(SAGA_TYPE).eq(s(getSagaTypeName(sagaType))).and(
                    anyIn("a", x(template.getSagaBucket().name() + "." + ASSOCIATIONS)).satisfies(
                        x("a.`" + ASSOCIATION_KEY + "`").eq(s(av.getKey()))
                            .and(x("a.`" + ASSOCIATION_VALUE + "`").eq(s(av.getValue())))
                    )
                )
            );

        Iterator<N1qlQueryRow> rows = template.getSagaBucket().query(
            N1qlQuery.simple(statement, N1qlParams.build().consistency(ScanConsistency.STATEMENT_PLUS))
        ).rows();

        Set<String> found = new TreeSet<>();
        while (rows.hasNext()) {
            final JsonObject row = rows.next().value();
            found.add(row.getString(SAGA_IDENTIFIER));
        }
        return found;
    }

    @Override
    public void deleteSaga(Class<?> sagaType, String sagaIdentifier, Set<AssociationValue> associationValues) {
        template.getSagaBucket().remove(sagaKey(sagaIdentifier));
    }

    @Override
    public void updateSaga(Class<?> sagaType, String sagaIdentifier, Object saga, AssociationValues associationValues) {
        SagaEntry<?> sagaEntry = new SagaEntry<>(sagaIdentifier, saga, associationValues.asSet(), serializer);
        final JsonObject sagaDocument = sagaEntry.asDocument();

        final Statement statement = update(template.getTokenBucket().name())
            .set(ASSOCIATIONS, sagaDocument.getArray(ASSOCIATIONS))
            .set(SERIALIZED_SAGA, sagaDocument.getString(SERIALIZED_SAGA))
            .where(x(SAGA_IDENTIFIER).eq(s(sagaIdentifier)));

        template.getSagaBucket().query(
            N1qlQuery.simple(statement, N1qlParams.build().consistency(ScanConsistency.STATEMENT_PLUS))
        );
    }

    @Override
    public void insertSaga(
        Class<?> sagaType, String sagaIdentifier, Object saga, Set<AssociationValue> associationValues) {
        SagaEntry<?> sagaEntry = new SagaEntry<>(sagaIdentifier, saga, associationValues, serializer);
        JsonObject sagaObject = sagaEntry.asDocument();
        template.getSagaBucket().insert(create(sagaKey(sagaIdentifier), sagaObject));
    }

    private String getSagaTypeName(Class<?> sagaType) {
        return serializer.typeForClass(sagaType).getName();
    }

    public static class Builder {

        private CouchbaseTemplate template;
        private Serializer serializer = XStreamSerializer.builder().build();

        public Builder template(CouchbaseTemplate template) {
            assertNonNull(template, "CouchbaseTemplate may not be null");
            this.template = template;
            return this;
        }

        public Builder serializer(Serializer serializer) {
            assertNonNull(serializer, "Serializer may not be null");
            this.serializer = serializer;
            return this;
        }

        public CouchbaseSagaStore build() {
            return new CouchbaseSagaStore(this);
        }

        protected void validate() throws AxonConfigurationException {
            assertNonNull(template, "The CouchbaseSagaStore is a hard requirement and should be provided");
        }
    }
}
