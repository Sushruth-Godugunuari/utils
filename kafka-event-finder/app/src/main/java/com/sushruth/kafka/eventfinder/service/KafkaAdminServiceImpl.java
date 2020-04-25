package com.sushruth.kafka.eventfinder.service;

import com.sushruth.kafka.eventfinder.exception.ConnectionNotFoundException;
import com.sushruth.kafka.eventfinder.model.KafkaServerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
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

  @Override
  public String getConnectionStatus(String connectionName) throws ExecutionException, InterruptedException {
    log.info("Get connection status for connection name: "  + connectionName);
    if (!activeClients.containsKey(connectionName)) {
      log.info(String.format("connection %s not found in cache", connectionName));
      Optional<KafkaServerConfig> cfg = cfgSvc.getServerByName(connectionName);
      if (cfg.isEmpty()) {
        throw new ConnectionNotFoundException(connectionName + " not found");
      }
      log.info(String.format("Create admin client for  %s ", cfg.get()));
      activeClients.put(connectionName, AdminClient.create(cfg.get().asProperties()));
    }
    AdminClient ac = activeClients.get(connectionName);
    var clusterResult = ac.describeCluster();
    return clusterResult.clusterId().get();
  }

  @Override
  public void getConsumerGroups(String connectionName) {}

  @Override
  public void getConsumerGroup(String connectionName, String groupName) {}

  @Override
  public void getTopics(String connectionName) {}

  @Override
  public void getTopicInfo(String connectionName, String topicName) {}
}
