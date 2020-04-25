package com.sushruth.kafka.eventfinder.config;

import lombok.Data;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@ConfigurationProperties(prefix = "connections")
@Configuration
@Data
public class ServerConfig {
    private boolean autoConnect;
    @NestedConfigurationProperty
    private List<ServerProperties> servers = new ArrayList<>();

    public static Properties asProperties(ServerProperties serverProperties) {
        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverProperties.getBootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, serverProperties.getMaxBlockMS());
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, serverProperties.getRequestTimeoutMS());
        properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, serverProperties.getRetryBackoffMS());
        properties.put("security.protocol", serverProperties.getSecurityProtocol());
        properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        properties.put(SaslConfigs.SASL_JAAS_CONFIG, getJAASConfig(serverProperties.getApiKey(), serverProperties.getApiSecret()));
        properties.put("ssl.endpoint.identification.algorithm", "https");
        return properties;
    }

  public static String getJAASConfig(String apiKey, String apiSecret) {
    return String.format(
        "org.apache.kafka.common.security.plain.PlainLoginModule required username='%s' password='%s';",
        apiKey, apiSecret);
  }

    @Data
    public static class ServerProperties {
        private String name;
        private String bootstrapServers;
        private int maxBlockMS;
        private int requestTimeoutMS;
        private int retryBackoffMS;
        private String securityProtocol; // TODO: Support other protocols
        private String apiKey;
        private String apiSecret;
    }
}
