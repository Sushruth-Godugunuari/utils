package com.sushruth.kafka.eventfinder.dto;

import lombok.Data;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

@Data
public class ConsumerGroupInfoDto {
  private String groupId;
  private boolean isSimpleConsumerGroup;
  private Collection<MemberDescription> members;
  private String partitionAssignor;
  private String state;
  private Node coordinator;
  private Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap;

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

  @Data
  public static class OffsetAndMetadata {
    private long offset;
    private String metadata;

    // We use null to represent the absence of a leader epoch to simplify serialization.
    // I.e., older serializations of this class which do not have this field will automatically
    // initialize its value to null.
    private Integer leaderEpoch;
  }
}
