package com.sushruth.kafka.eventfinder.dto;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class TopicInfoDto {
  private String name;
  private boolean internal;
  private Map<Integer, Offset> offsets;
  private List<TopicPartitionInfoDto> partitions;

  @Data
  public static class TopicPartitionInfoDto {
    private int partition;
    private Node leader;
    private List<Node> replicas;
    private List<Node> isr;
  }

  @Data
  public static class Node {
    private int id;
    private String idString;
    private String host;
    private int port;
    private String rack;
  }

  @Data
  public static class Offset {
    private long begin;
    private long end;
  }
}
