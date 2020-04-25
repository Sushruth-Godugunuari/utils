package com.sushruth.kafka.eventfinder.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class KafkaServerConfigRequestDto {
    private String name;
    private String bootstrapServers;
    private int maxBlockMS;
    private int requestTimeoutMS;
    private int retryBackoffMS;
    private String securityProtocol; // TODO: Support other protocols
    private String apiKey;
    private String apiSecret;

}
