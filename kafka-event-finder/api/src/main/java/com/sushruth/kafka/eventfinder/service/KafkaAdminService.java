package com.sushruth.kafka.eventfinder.service;

import org.apache.kafka.clients.admin.TopicDescription;

import java.util.Set;
import java.util.concurrent.ExecutionException;

public interface KafkaAdminService {
    String getConnectionStatus(String connectionName) throws ExecutionException, InterruptedException;
    void getConsumerGroups(String connectionName);
    void getConsumerGroup(String connectionName, String groupName);
    Set<String> getTopics(String connectionName) throws ExecutionException, InterruptedException;
    TopicDescription getTopicInfo(String connectionName, String topicName) throws ExecutionException, InterruptedException;
}
