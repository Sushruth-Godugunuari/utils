package com.sushruth.kafka.eventfinder.dto;

import lombok.Data;

@Data
public class ConsumerGroupListingDto {
    private String groupId;
    private boolean isSimpleConsumerGroup;
}
