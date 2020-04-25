package com.sushruth.kafka.eventfinder.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ConnectionEntity {
    @Id
    private String id;

    @Column(name = "name", nullable = false, unique = true)
    private String name;

    @Column(name ="bootstrap_servers", nullable = false)
    private String bootstrapServers;

    @Column(name="max_block_ms")
    private int maxBlockMS;

    @Column(name = "request_timeout_ms")
    private int requestTimeoutMS;

    @Column(name = "retry_backoff_ms")
    private int retryBackoffMS;

    @Column(name="security_protocol")
    private String securityProtocol; // TODO: Support other protocols

    @Column(name = "api_key")
    private String apiKey;

    @Column(name="api_secret")
    private String apiSecret;
}
