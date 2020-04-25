package com.sushruth.kafka.eventfinder.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Properties;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class KafkaServerConfig {
  private String id;
  private String name;
  private String bootstrapServers;
  private int maxBlockMS;
  private int requestTimeoutMS;
  private int retryBackoffMS;
  private String securityProtocol; // TODO: Support other protocols
  private String apiKey;
  private String apiSecret;

  public Properties asProperties() {
    Properties properties = new Properties();

    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
    properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, getMaxBlockMS());
    properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, getRequestTimeoutMS());
    properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, getRetryBackoffMS());
    properties.put("security.protocol", getSecurityProtocol());
    properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
    properties.put(SaslConfigs.SASL_JAAS_CONFIG, getJAASConfig(getApiKey(), getApiSecret()));
    properties.put("ssl.endpoint.identification.algorithm", "https");
    return properties;
  }

  public static String getJAASConfig(String apiKey, String apiSecret) {
    return String.format(
        "org.apache.kafka.common.security.plain.PlainLoginModule required username='%s' password='%s';",
        apiKey, apiSecret);
  }
}
