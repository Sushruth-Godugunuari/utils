package com.sushruth.kafka.eventfinder.dto;

import lombok.Data;

import java.util.List;
import java.util.Optional;

@Data
public class SearchEventRequestDto {
    private Integer partition;
    private List<Header> headers;

    @Data
    public static class Header {
        private String key;
        private String value;
    }
}

