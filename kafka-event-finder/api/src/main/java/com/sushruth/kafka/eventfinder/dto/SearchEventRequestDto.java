package com.sushruth.kafka.eventfinder.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
public class SearchEventRequestDto {
  private List<Partition> partitions;
  private List<Header> headers;
  SearchStrategy searchStrategy;

  @Data
  public static class Header {
    private String key;
    private String value;
  }

  @Data
  public static class Partition {
    private int id;
    private int startPercentage;
  }

  @AllArgsConstructor
  public enum SearchStrategy {
    TOPIC,
    PARTITION,
  }
}
