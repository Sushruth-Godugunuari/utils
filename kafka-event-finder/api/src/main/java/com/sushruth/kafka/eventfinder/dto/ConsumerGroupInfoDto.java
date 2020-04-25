package com.sushruth.kafka.eventfinder.dto;

import lombok.Data;

import java.util.Collection;
import java.util.Set;

@Data
public class ConsumerGroupInfoDto {
  private String groupId;
  private boolean isSimpleConsumerGroup;
  private Collection<MemberDescription> members;
  private String partitionAssignor;
  private String state;
  private Node coordinator;

  @Data
  public static class MemberDescription {
    private String memberId;
    private String groupInstanceId;
    private String clientId;
    private String host;
    private Set<TopicPartition> memberAssignmentPartition;
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
  public static class TopicPartition {
    private int partition;
    private String topic;
  }
}
