package com.sushruth.kafka.eventfinder.service;

// import java.util.Map;

import com.sushruth.kafka.eventfinder.exception.ConnectionNotFoundException;
import com.sushruth.kafka.eventfinder.model.KafkaServerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

@Slf4j
@Service
public class TopicServiceImpl implements TopicService {
  ConfigService cfgSvc;
  Map<String, Properties> cachedCfg = new HashMap<>();

  @Autowired
  public TopicServiceImpl(ConfigService cfgSvc) {
    this.cfgSvc = cfgSvc;
  }


  @Override
  public Map<String, Map<TopicPartition, Long>> getTopicOffsets(String server, String topic) {
    Properties properties = getConsumerProps(server);
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {

      List<TopicPartition> topicPartitions = getTopicPartitions(consumer.partitionsFor(topic));
      return getOffsetMetadata(consumer, topicPartitions);

    }
  }


  @Override
  public  List<ConsumerRecord<?, ?>> getFirstEvents(String server, String topic) {
    Properties properties = getConsumerProps(server);

    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
      //      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -- set header and body serializer
      //      consumer.subscribe(List.of(topic));
      List<PartitionInfo> partitions = consumer.partitionsFor(topic);
      List<TopicPartition> topicPartitions = getTopicPartitions(partitions);
      Map<TopicPartition, Long> begin = getOffsetMetadata(consumer, topicPartitions).get("begin");
      List<ConsumerRecord<?, ?>> result = new ArrayList<>();
      for (var partition : getTopicPartitions(partitions)) {
        consumer.assign(Collections.singletonList(partition));
        consumer.seekToBeginning(Collections.singletonList(partition));
//        consumer.seek(partition, begin.get(partition) -1 < 0? 0: begin.get(partition) -1 );
        ConsumerRecords<?, ?> records = consumer.poll(Duration.ofSeconds(2));
        if (records.isEmpty()) {
          log.error("RECORDS was empty for given offset");
        }
        if (!records.isEmpty()) {
          for (ConsumerRecord<?, ?> record : records) {
            result.add(record);
            log.info("writing record " + record.headers().toString());
            break;
          }
        }
        consumer.unsubscribe();
      }
      return result;
    }
  }

  @Override
  public Optional<ConsumerRecord<?, ?>> getFirstEvent(String server, String topic) {
    Properties properties = getConsumerProps(server);
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
      List<PartitionInfo> partitions = consumer.partitionsFor(topic);
      List<TopicPartition> topicPartitions = getTopicPartitions(partitions);
      Map<String, Map<TopicPartition, Long>> minMax = getOffsetMetadata(consumer, topicPartitions);
      var topicPartition = getPartitionWithBeginOffset(minMax);
      log.info(String.format("Assigning topic %s and partition %d ", topicPartition.topic(), topicPartition.partition()));
      consumer.assign(Collections.singleton(topicPartition));
      consumer.seekToBeginning(Collections.singleton(topicPartition));
      var records = consumer.poll(Duration.ofSeconds(5));
      if (records.isEmpty()) {
        log.error("RECORDS was empty for given offset");
        return Optional.empty();
      }

      for (ConsumerRecord<?, ?> record : records) {
        consumer.unsubscribe();
        return Optional.of(record);
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
    // will not get here...
    return Optional.empty();
  }

  @Override
  public Optional<ConsumerRecord<?, ?>> getLastEvent(String server, String topic) {
    Properties properties = getConsumerProps(server);
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
      List<PartitionInfo> partitions = consumer.partitionsFor(topic);
      List<TopicPartition> topicPartitions = getTopicPartitions(partitions);
      Map<String, Map<TopicPartition, Long>> minMax = getOffsetMetadata(consumer, topicPartitions);
      var topicPartition = getPartitionWithEndOffset(minMax);
      log.info(String.format("Assigning topic %s and partition %d ", topicPartition.topic(), topicPartition.partition()));
      consumer.assign(Collections.singleton(topicPartition));
      consumer.seek(topicPartition, getLastOffset(minMax, topicPartition));
      var records = consumer.poll(Duration.ofSeconds(5));
      if (records.isEmpty()) {
        log.error("RECORDS was empty for given offset");
        return Optional.empty();
      }

      for (ConsumerRecord<?, ?> record : records) {
        consumer.unsubscribe();
        return Optional.of(record);
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
    // will not get here...
    return Optional.empty();
  }

  private static Long getLastOffset(Map<String, Map<TopicPartition, Long>> minMax, TopicPartition topicPartition) {
    Long offset = minMax.get("end").get(topicPartition);
    if (offset == 0){
      return offset;
    }
    return offset -1;
  }

  private Properties getConsumerProps(String server) {
    if (!cachedCfg.containsKey(server)) {
      Optional<KafkaServerConfig> cfg = cfgSvc.getServerByName(server);
      if (cfg.isEmpty()) {
        throw new ConnectionNotFoundException(server + " not found");
      }
      Properties properties = cfg.get().asProperties();
      properties.remove("key.serializer");
      properties.remove("value.serializer");
      properties.remove("max.block.ms");
      properties.put(ConsumerConfig.GROUP_ID_CONFIG, "DMSKafkaEventFinder");
      properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
      properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
      properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class);

      cachedCfg.put(server, properties);
    }
    return cachedCfg.get(server);
  }

  private static Map<String, Map<TopicPartition, Long>> getOffsetMetadata(
          KafkaConsumer<?, ?> consumer, List<TopicPartition> topicPartition) {

    Map<String, Map<TopicPartition, Long>> minMax = new HashMap<>();

    minMax.put("begin", consumer.beginningOffsets(topicPartition));
    minMax.put("end", consumer.endOffsets(topicPartition));
    return minMax;
  }

  private static TopicPartition getPartitionWithBeginOffset(Map<String, Map<TopicPartition, Long>> offsets){
    var beginOffsets = offsets.get("begin");
    long begin = 0;
    TopicPartition beginPartition = null;
    for(var tp : beginOffsets.keySet()){
      if (begin == 0 || beginOffsets.get(tp) <= begin){
        begin = beginOffsets.get(tp);
        beginPartition = tp;
      }
    }
    return beginPartition;
  }

  private static TopicPartition getPartitionWithEndOffset(Map<String, Map<TopicPartition, Long>> offsets){
    var endOffsets = offsets.get("end");
    long end = 0;
    TopicPartition endPartition = null;
    for(var tp : endOffsets.keySet()){
      if (end == 0 || endOffsets.get(tp) >= end){
        end  = endOffsets.get(tp);
        endPartition = tp;
      }
    }
    return endPartition;
  }

  private static List<TopicPartition> getTopicPartitions(List<PartitionInfo> partitions) {
    return partitions.stream()
            .map((partition) -> new TopicPartition(partition.topic(), partition.partition()))
            .collect(Collectors.toList());
  }
}
