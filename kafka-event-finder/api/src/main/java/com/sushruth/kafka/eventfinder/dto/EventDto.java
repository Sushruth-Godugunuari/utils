package com.sushruth.kafka.eventfinder.dto;

import lombok.Data;

import java.util.List;

@Data
public class EventDto {
    List<Header> headers;
    String key;
    String value;
    long Offset;
    long timestamp;
    int partition;

    @Data
    public static class Header {
        String key;
        String value;
    }
}
