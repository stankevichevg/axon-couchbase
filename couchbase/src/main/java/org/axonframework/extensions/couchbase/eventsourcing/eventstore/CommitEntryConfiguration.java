package org.axonframework.extensions.couchbase.eventsourcing.eventstore;

public class CommitEntryConfiguration {

    private final String firstTimestampProperty, lastTimestampProperty, firstSequenceNumberProperty,
        lastSequenceNumberProperty, eventsProperty;

    public static CommitEntryConfiguration getDefault() {
        return new Builder().build();
    }

    private CommitEntryConfiguration(Builder builder) {
        firstTimestampProperty = builder.firstTimestampProperty;
        lastTimestampProperty = builder.lastTimestampProperty;
        firstSequenceNumberProperty = builder.firstSequenceNumberProperty;
        lastSequenceNumberProperty = builder.lastSequenceNumberProperty;
        eventsProperty = builder.eventsProperty;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String firstTimestampProperty() {
        return firstTimestampProperty;
    }

    public String lastTimestampProperty() {
        return lastTimestampProperty;
    }

    public String firstSequenceNumberProperty() {
        return firstSequenceNumberProperty;
    }

    public String lastSequenceNumberProperty() {
        return lastSequenceNumberProperty;
    }

    public String eventsProperty() {
        return eventsProperty;
    }

    public static class Builder {

        private String firstTimestampProperty = "firstTimestamp";
        private String lastTimestampProperty = "lastTimestamp";
        private String firstSequenceNumberProperty = "firstSequenceNumber";
        private String lastSequenceNumberProperty = "lastSequenceNumber";
        private String eventsProperty = "events";

        public Builder firstTimestampProperty(String firstTimestampProperty) {
            this.firstTimestampProperty = firstTimestampProperty;
            return this;
        }

        public Builder lastTimestampProperty(String lastTimestampProperty) {
            this.lastTimestampProperty = lastTimestampProperty;
            return this;
        }

        public Builder firstSequenceNumberProperty(String firstSequenceNumberProperty) {
            this.firstSequenceNumberProperty = firstSequenceNumberProperty;
            return this;
        }

        public Builder lastSequenceNumberProperty(String lastSequenceNumberProperty) {
            this.lastSequenceNumberProperty = lastSequenceNumberProperty;
            return this;
        }

        public Builder eventsProperty(String eventsProperty) {
            this.eventsProperty = eventsProperty;
            return this;
        }

        public CommitEntryConfiguration build() {
            return new CommitEntryConfiguration(this);
        }
    }
}
