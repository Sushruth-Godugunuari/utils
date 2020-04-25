package com.sushruth.kafka.eventfinder.service;

import com.sushruth.kafka.eventfinder.model.KafkaServerConfig;

import java.util.List;
import java.util.Optional;

public interface ConfigService {
    void addServer(KafkaServerConfig kafkaServerConfig);
    void deleteServerByName(String serverName);
    void deleteServerById(String id);
    void updateServer(KafkaServerConfig kafkaServerConfig);
    Optional<KafkaServerConfig> getServerByName(String serverName);
    Optional<KafkaServerConfig> getServerById(String serverById);
    List<KafkaServerConfig> getAllServers();
}
