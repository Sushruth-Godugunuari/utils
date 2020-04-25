package com.sushruth.kafka.eventfinder.service;

import java.util.concurrent.ExecutionException;

public interface KafkaAdminService {
    String getConnectionStatus(String connectionName) throws ExecutionException, InterruptedException;
    void getConsumerGroups(String connectionName);
    void getConsumerGroup(String connectionName, String groupName);
    void getTopics(String connectionName);
    void getTopicInfo(String connectionName, String topicName);
}
