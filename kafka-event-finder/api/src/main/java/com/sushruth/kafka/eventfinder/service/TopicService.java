package com.sushruth.kafka.eventfinder.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface TopicService {
  Map<String, Map<TopicPartition, Long>> getTopicOffsets(String server, String topic);

  List<ConsumerRecord<?, ?>> getFirstEvents(String server, String topic);

  Optional<ConsumerRecord<?, ?>> getFirstEvent(String server, String topic);

  Optional<ConsumerRecord<?, ?>> getLastEvent(String server, String topic);

  //  Object findEventByOffset(String server, String topic, int offset);
  //
  //  Object getTopicInfo(String server, String topic);
  //
  //  Object getLastEvent(String server, String topic);
  //
  //  Object getOffsets();
  //
  //  Object findEventByHeader(String header);
}
