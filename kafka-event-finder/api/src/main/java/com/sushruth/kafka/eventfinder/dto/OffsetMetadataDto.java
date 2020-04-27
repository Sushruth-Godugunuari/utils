package com.sushruth.kafka.eventfinder.dto;

import lombok.Data;

@Data
public class OffsetMetadataDto {
    private String topic;
    private int partition;
    private long begin;
    private long end;
}
