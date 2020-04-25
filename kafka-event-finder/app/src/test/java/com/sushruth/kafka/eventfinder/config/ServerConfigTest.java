package com.sushruth.kafka.eventfinder.config;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;
import java.util.Properties;

@SpringBootTest
class ServerConfigTest {
  @Autowired ServerConfig serverConfig;

  @Test
  void autoConnectSetToFalse() {
    Assertions.assertFalse(serverConfig.isAutoConnect());
  }

  @Test
  public void validateServerDefaults() {
    List<ServerConfig.ServerProperties> servers = serverConfig.getServers();
    Assertions.assertEquals(2, servers.size(), "expected 2 servers to be configured");
  }

  @Test
  public void validateServer1Defaults() {
    ServerConfig.ServerProperties serverProperties = serverConfig.getServers().get(0);
    Assertions.assertEquals("server1", serverProperties.getName());
    Assertions.assertEquals("bootstrapserver1", serverProperties.getBootstrapServers());
    Assertions.assertEquals("apiKey", serverProperties.getApiKey());
    Assertions.assertEquals("apiSecret", serverProperties.getApiSecret());

    Assertions.assertEquals(10, serverProperties.getMaxBlockMS());
    Assertions.assertEquals(100, serverProperties.getRequestTimeoutMS());
    Assertions.assertEquals(100, serverProperties.getRetryBackoffMS());
  }

  @Test
  public void validateJaasString() {
    String expected =
        "org.apache.kafka.common.security.plain.PlainLoginModule required username='username' password='password';";
    Assertions.assertEquals(expected, ServerConfig.getJAASConfig("username", "password"));
  }

  @Test
    public void givenDefaultServerConfig_Then_ShouldHave10Properties(){
      ServerConfig.ServerProperties serverProperties = serverConfig.getServers().get(0);
      Properties props = ServerConfig.asProperties(serverProperties);
      Assertions.assertEquals(10, props.size());
  }
}
