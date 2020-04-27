package com.sushruth.kafka.eventfinder.model;

import lombok.Data;

import java.util.List;

@Data
public class SearchEventRequest {
    String connection;
    String topic;
    Integer partition;
    List<Header> headers;

    @Data
    public static class Header{
        String key;
        String value;
    }
}
