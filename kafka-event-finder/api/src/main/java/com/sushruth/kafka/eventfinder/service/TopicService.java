package com.sushruth.kafka.eventfinder.service;

import com.sushruth.kafka.eventfinder.model.SearchEventRequest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface TopicService {
  Map<String, Map<TopicPartition, Long>> getTopicOffsets(String server, String topic);

  List<ConsumerRecord<?, ?>> getFirstEvents(String server, String topic);

  Optional<ConsumerRecord<?, ?>> getFirstEvent(String server, String topic, int partition);

  Optional<ConsumerRecord<?, ?>> getLastEvent(String server, String topic, int partition);

  List<ConsumerRecord<?, ?>> getLastEvents(String server, String topic);

  Optional<ConsumerRecord<?, ?>> searchEvent(SearchEventRequest searchEventRequest);

  Optional<ConsumerRecord<?, ?>> getEventByOffset(
      String server, String topic, int partition, long offset);

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
