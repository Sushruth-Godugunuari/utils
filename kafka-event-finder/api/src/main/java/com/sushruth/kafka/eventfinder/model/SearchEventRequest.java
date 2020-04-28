package com.sushruth.kafka.eventfinder.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
public class SearchEventRequest {
  private String connection;
  private String topic;
  private List<Header> headers;
  private SearchStrategy searchStrategy = SearchStrategy.PARTITION; // default to partition
  private List<Partition> partitions;

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
