package org.axonframework.extensions.couchbase.eventsourcing.eventstore;

public class EventEntryConfiguration {

    private final String timestampProperty;
    private final String eventIdentifierProperty;
    private final String aggregateIdentifierProperty;
    private final String sequenceNumberProperty;
    private final String typeProperty;
    private final String payloadTypeProperty;
    private final String payloadRevisionProperty;
    private final String payloadProperty;
    private final String metaDataProperty;

    public static EventEntryConfiguration getDefault() {
        return builder().build();
    }

    private EventEntryConfiguration(Builder builder) {
        timestampProperty = builder.timestampProperty;
        eventIdentifierProperty = builder.eventIdentifierProperty;
        aggregateIdentifierProperty = builder.aggregateIdentifierProperty;
        sequenceNumberProperty = builder.sequenceNumberProperty;
        typeProperty = builder.typeProperty;
        payloadTypeProperty = builder.payloadTypeProperty;
        payloadRevisionProperty = builder.payloadRevisionProperty;
        payloadProperty = builder.payloadProperty;
        metaDataProperty = builder.metaDataProperty;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String timestampProperty() {
        return timestampProperty;
    }

    public String eventIdentifierProperty() {
        return eventIdentifierProperty;
    }

    public String aggregateIdentifierProperty() {
        return aggregateIdentifierProperty;
    }

    public String sequenceNumberProperty() {
        return sequenceNumberProperty;
    }

    public String typeProperty() {
        return typeProperty;
    }

    public String payloadTypeProperty() {
        return payloadTypeProperty;
    }

    public String payloadRevisionProperty() {
        return payloadRevisionProperty;
    }

    public String payloadProperty() {
        return payloadProperty;
    }

    public String metaDataProperty() {
        return metaDataProperty;
    }

    public static class Builder {

        private String timestampProperty = "timestamp";
        private String eventIdentifierProperty = "eventIdentifier";
        private String aggregateIdentifierProperty = "aggregateIdentifier";
        private String sequenceNumberProperty = "sequenceNumber";
        private String typeProperty = "type";
        private String payloadTypeProperty = "payloadType";
        private String payloadRevisionProperty = "payloadRevision";
        private String payloadProperty = "serializedPayload";
        private String metaDataProperty = "serializedMetaData";

        public Builder timestampProperty(String timestampProperty) {
            this.timestampProperty = timestampProperty;
            return this;
        }

        public Builder eventIdentifierProperty(String eventIdentifierProperty) {
            this.eventIdentifierProperty = eventIdentifierProperty;
            return this;
        }

        public Builder aggregateIdentifierProperty(String aggregateIdentifierProperty) {
            this.aggregateIdentifierProperty = aggregateIdentifierProperty;
            return this;
        }

        public Builder sequenceNumberProperty(String sequenceNumberProperty) {
            this.sequenceNumberProperty = sequenceNumberProperty;
            return this;
        }

        public Builder typeProperty(String typeProperty) {
            this.typeProperty = typeProperty;
            return this;
        }

        public Builder payloadTypeProperty(String payloadTypeProperty) {
            this.payloadTypeProperty = payloadTypeProperty;
            return this;
        }

        public Builder payloadRevisionProperty(String payloadRevisionProperty) {
            this.payloadRevisionProperty = payloadRevisionProperty;
            return this;
        }

        public Builder payloadProperty(String payloadProperty) {
            this.payloadProperty = payloadProperty;
            return this;
        }

        public Builder metaDataProperty(String metaDataProperty) {
            this.metaDataProperty = metaDataProperty;
            return this;
        }

        public EventEntryConfiguration build() {
            return new EventEntryConfiguration(this);
        }
    }
}
