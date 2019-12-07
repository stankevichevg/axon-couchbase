package org.axonframework.extensions.couchbase.eventhandling.saga.repository;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import org.axonframework.modelling.saga.AssociationValue;
import org.axonframework.serialization.SerializedObject;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.SimpleSerializedObject;

import java.util.HashSet;
import java.util.Set;

public class SagaEntry<T> {

    public static final String SAGA_IDENTIFIER = "sagaIdentifier";
    public static final String SAGA_TYPE = "sagaType";
    public static final String SERIALIZED_SAGA = "serializedSaga";
    public static final String ASSOCIATIONS = "associations";
    public static final String ASSOCIATION_KEY = "key";
    public static final String ASSOCIATION_VALUE = "value";

    private final String sagaId;
    private final String sagaType;

    private final String serializedSaga;

    private volatile T saga;
    private final Set<AssociationValue> associationValues;

    public SagaEntry(String identifier, T saga, Set<AssociationValue> associationValues, Serializer serializer) {
        this.sagaId = identifier;
        SerializedObject<String> serialized = serializer.serialize(saga, String.class);
        this.serializedSaga = serialized.getData();
        this.sagaType = serializer.typeForClass(saga.getClass()).getName();
        this.saga = saga;
        this.associationValues = new HashSet<>(associationValues);
    }

    public SagaEntry(JsonObject dbSaga) {
        this.sagaId = dbSaga.getString(SAGA_IDENTIFIER);
        this.serializedSaga = dbSaga.getString(SERIALIZED_SAGA);
        this.sagaType = dbSaga.getString(SAGA_TYPE);
        this.associationValues = extractAssociations(dbSaga);
    }

    public T getSaga(Serializer serializer) {
        if (saga != null) {
            return saga;
        }
        return serializer.deserialize(new SimpleSerializedObject<String>(serializedSaga, String.class, sagaType, ""));
    }

    public String getSagaId() {
        return sagaId;
    }

    public Set<AssociationValue> getAssociationValues() {
        return associationValues;
    }

    public JsonObject asDocument() {
        return JsonObject.create()
            .put(SAGA_TYPE, sagaType)
            .put(SAGA_IDENTIFIER, sagaId)
            .put(SERIALIZED_SAGA, serializedSaga)
            .put(ASSOCIATIONS, toArray(associationValues));
    }

    @SuppressWarnings("unchecked")
    private Set<AssociationValue> extractAssociations(JsonObject dbSaga) {
        Set<AssociationValue> values = new HashSet<>();
        final JsonArray associations = dbSaga.getArray(ASSOCIATIONS);
        if (associations != null) {
            for (int i = 0; i < associations.size(); i++) {
                final JsonObject association = associations.getObject(i);
                values.add(new AssociationValue(
                    association.getString(ASSOCIATION_KEY),
                    association.getString(ASSOCIATION_VALUE))
                );
            }
        }
        return values;
    }

    private static JsonArray toArray(Iterable<AssociationValue> associationValues) {
        final JsonArray jsonArray = JsonArray.create();
        for (AssociationValue associationValue : associationValues) {
            jsonArray.add(
                JsonObject.create()
                    .put(ASSOCIATION_KEY, associationValue.getKey())
                    .put(ASSOCIATION_VALUE, associationValue.getValue())
            );
        }
        return jsonArray;
    }

}
