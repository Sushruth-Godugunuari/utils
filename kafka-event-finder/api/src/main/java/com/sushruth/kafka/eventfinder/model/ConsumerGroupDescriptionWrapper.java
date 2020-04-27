package com.sushruth.kafka.eventfinder.model;

import lombok.Data;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

@Data
public class ConsumerGroupDescriptionWrapper {
  ConsumerGroupDescription description;
  Map<TopicPartition, OffsetAndMetadata> offsets;
}
