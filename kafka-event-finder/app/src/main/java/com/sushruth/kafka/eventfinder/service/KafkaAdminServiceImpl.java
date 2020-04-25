package com.sushruth.kafka.eventfinder.service;

import com.sushruth.kafka.eventfinder.exception.ConnectionNotFoundException;
import com.sushruth.kafka.eventfinder.model.KafkaServerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@Service
@Slf4j
public class KafkaAdminServiceImpl implements KafkaAdminService {
  Map<String, AdminClient> activeClients = new HashMap<>();
  ConfigService cfgSvc;

  @Autowired
  public KafkaAdminServiceImpl(ConfigService cfgSvc) {
    this.cfgSvc = cfgSvc;
  }

  private void createClientIfNotPresent(String connectionName){
    if (!activeClients.containsKey(connectionName)) {
      log.info(String.format("connection %s not found in cache", connectionName));
      Optional<KafkaServerConfig> cfg = cfgSvc.getServerByName(connectionName);
      if (cfg.isEmpty()) {
        throw new ConnectionNotFoundException(connectionName + " not found");
      }
      log.info(String.format("Create admin client for  %s ", cfg.get()));
      activeClients.put(connectionName, AdminClient.create(cfg.get().asProperties()));
    }
  }

  @Override
  public String getConnectionStatus(String connectionName) throws ExecutionException, InterruptedException {
    log.info("Get connection status for connection name: "  + connectionName);
    createClientIfNotPresent(connectionName);
    AdminClient ac = activeClients.get(connectionName);
    var clusterResult = ac.describeCluster();
    return clusterResult.clusterId().get();
  }

  @Override
  public Collection<ConsumerGroupListing> getConsumerGroups(String connectionName) throws ExecutionException, InterruptedException {
    createClientIfNotPresent(connectionName);
    AdminClient ac = activeClients.get(connectionName);
    ListConsumerGroupsResult listConsumerGroupsResult = ac.listConsumerGroups();
    return listConsumerGroupsResult.all().get();
  }

  @Override
  public ConsumerGroupDescription getConsumerGroup(String connectionName, String groupName) throws ExecutionException, InterruptedException {
    createClientIfNotPresent(connectionName);
    AdminClient ac = activeClients.get(connectionName);
    var groups = ac.describeConsumerGroups(List.of(groupName));
    return groups.all().get().get(groupName);
  }

  @Override
  public Set<String> getTopics(String connectionName) throws ExecutionException, InterruptedException {
    createClientIfNotPresent(connectionName);
    AdminClient ac = activeClients.get(connectionName);
    var topics = ac.listTopics();
    return topics.names().get();
  }

  @Override
  public TopicDescription getTopicInfo(String connectionName, String topicName) throws ExecutionException, InterruptedException {
    createClientIfNotPresent(connectionName);
    AdminClient ac = activeClients.get(connectionName);
    DescribeTopicsResult describeTopicsResult = ac.describeTopics(List.of(topicName));
    return describeTopicsResult.all().get().get(topicName);
  }
}
