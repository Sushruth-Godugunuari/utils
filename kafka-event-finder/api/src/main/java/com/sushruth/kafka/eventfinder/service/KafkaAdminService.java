package com.sushruth.kafka.eventfinder.service;

import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.TopicDescription;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public interface KafkaAdminService {
    String getConnectionStatus(String connectionName) throws ExecutionException, InterruptedException;
    Collection<ConsumerGroupListing> getConsumerGroups(String connectionName) throws ExecutionException, InterruptedException;
    ConsumerGroupDescription getConsumerGroup(String connectionName, String groupName) throws ExecutionException, InterruptedException;
    Set<String> getTopics(String connectionName) throws ExecutionException, InterruptedException;
    TopicDescription getTopicInfo(String connectionName, String topicName) throws ExecutionException, InterruptedException;
}
