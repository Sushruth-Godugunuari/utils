package com.sushruth.kafka.eventfinder.service;

import com.sushruth.kafka.eventfinder.config.ServerConfig;
import com.sushruth.kafka.eventfinder.entity.ConnectionEntity;
import com.sushruth.kafka.eventfinder.model.KafkaServerConfig;
import com.sushruth.kafka.eventfinder.repo.ConnectionRepo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
public class ConfigServiceImpl implements ConfigService {
  private ServerConfig cfg;
  private ConnectionRepo repo;

  @Autowired
  public ConfigServiceImpl(ServerConfig cfg, ConnectionRepo repo) {
    this.cfg = cfg;
    this.repo = repo;
    loadFromConfig();
  }

  private void loadFromConfig() {
    var servers = cfg.getServers();
    servers.stream().map(ConfigServiceImpl::configToEntity).forEach(repo::save);
  }

  static ConnectionEntity configToEntity(ServerConfig.ServerProperties props) {
    var ce = new ConnectionEntity();
    ce.setId(UUID.randomUUID().toString());
    ce.setName(props.getName());
    ce.setBootstrapServers(props.getBootstrapServers());
    ce.setSecurityProtocol(props.getSecurityProtocol());
    ce.setApiKey(props.getApiKey());
    ce.setApiSecret(props.getApiSecret());

    ce.setMaxBlockMS(props.getMaxBlockMS());
    ce.setRequestTimeoutMS(props.getRequestTimeoutMS());
    ce.setRetryBackoffMS(props.getRetryBackoffMS());

    return ce;
  }

  static KafkaServerConfig entityToModel(ConnectionEntity connectionEntity) {
    KafkaServerConfig kafkaServerConfig = new KafkaServerConfig();

    kafkaServerConfig.setId(connectionEntity.getId());
    kafkaServerConfig.setName(connectionEntity.getName());
    kafkaServerConfig.setBootstrapServers(connectionEntity.getBootstrapServers());
    kafkaServerConfig.setSecurityProtocol(connectionEntity.getSecurityProtocol());
    kafkaServerConfig.setApiKey(connectionEntity.getApiKey());
    kafkaServerConfig.setApiSecret(connectionEntity.getApiSecret());

    kafkaServerConfig.setMaxBlockMS(connectionEntity.getMaxBlockMS());
    kafkaServerConfig.setRequestTimeoutMS(connectionEntity.getRequestTimeoutMS());
    kafkaServerConfig.setRetryBackoffMS(connectionEntity.getRetryBackoffMS());

    return kafkaServerConfig;
  }

  @Override
  public void addServer(KafkaServerConfig kafkaServerConfig) {}

  @Override
  public void deleteServerByName(String serverId) {}

  @Override
  public void deleteServerById(String id) {}

  @Override
  public void updateServer(KafkaServerConfig kafkaServerConfig) {}

  @Override
  public Optional<KafkaServerConfig> getServerByName(String serverName) {
    var serverCfg = repo.findByName(serverName);
    if (serverCfg.isPresent()) {
      return Optional.of(entityToModel(serverCfg.get()));
    }

    return Optional.empty();
  }

  @Override
  public Optional<KafkaServerConfig> getServerById(String serverById) {
    return Optional.empty();
  }

  @Override
  public List<KafkaServerConfig> getAllServers() {
    Iterable<ConnectionEntity> entities = repo.findAll();
    List<KafkaServerConfig> models = new ArrayList<>();
    entities.forEach(
        entity -> {
          models.add(entityToModel(entity));
        });
    return models;
  }
}
