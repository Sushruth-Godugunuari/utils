package com.sushruth.kafka.eventfinder.exception;

public class TopicNotFoundException extends RuntimeException {
    private String connectionName;
    private String topicName;

    public TopicNotFoundException(String connectionName, String topicName) {
        super(String.format("topic: %s not found on server: %s",topicName, connectionName));
        this.connectionName = connectionName;
        this.topicName = topicName;
    }

    public TopicNotFoundException(String topicName) {
        super(String.format("topic: %s not found on server",topicName));
        this.topicName = topicName;
    }

    public String getConnectionName() {
        return connectionName;
    }

    public String getTopicName() {
        return topicName;
    }
}
