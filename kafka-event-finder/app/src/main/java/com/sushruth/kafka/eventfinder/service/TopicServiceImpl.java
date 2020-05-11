package com.sushruth.kafka.eventfinder.service;

import com.sushruth.kafka.eventfinder.exception.ConnectionNotFoundException;
import com.sushruth.kafka.eventfinder.exception.TopicNotFoundException;
import com.sushruth.kafka.eventfinder.model.KafkaServerConfig;
import com.sushruth.kafka.eventfinder.model.SearchEventRequest;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Predicate;
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

      List<TopicPartition> topicPartitions = getTopicPartitions(getPartitions(consumer, topic));
      return getOffsetMetadata(consumer, topicPartitions);
    }
  }

  @Override
  public List<ConsumerRecord<?, ?>> getFirstEvents(String server, String topic) {
    Properties properties = getConsumerProps(server);
    properties.put(
        ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,
        ConsumerConfig.DEFAULT_MAX_PARTITION_FETCH_BYTES);

    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
      List<PartitionInfo> partitions = getPartitions(consumer, topic);

      List<TopicPartition> topicPartitions = getTopicPartitions(partitions);
      List<ConsumerRecord<?, ?>> result = new ArrayList<>();
      for (var partition : getTopicPartitions(partitions)) {
        consumer.assign(Collections.singletonList(partition));
        consumer.seekToBeginning(Collections.singletonList(partition));
        ConsumerRecords<?, ?> records = consumer.poll(Duration.ofSeconds(30));
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
  public Optional<ConsumerRecord<?, ?>> getFirstEvent(String server, String topic, int partition) {
    Properties properties = getConsumerProps(server);
    properties.put(
        ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,
        ConsumerConfig.DEFAULT_MAX_PARTITION_FETCH_BYTES);

    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
      List<PartitionInfo> partitions = getPartitions(consumer, topic);

      List<TopicPartition> topicPartitions = getTopicPartitions(partitions);
      Map<String, Map<TopicPartition, Long>> minMax = getOffsetMetadata(consumer, topicPartitions);
      var topicPartition = getPartitionWithBeginOffset(minMax);
      log.info(
          String.format(
              "Assigning topic %s and partition %d ",
              topicPartition.topic(), topicPartition.partition()));
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
  public List<ConsumerRecord<?, ?>> getLastEvents(String server, String topic) {
    return null;
  }

  @Override
  public Optional<ConsumerRecord<?, ?>> getLastEvent(String server, String topic, int partition) {
    Properties properties = getConsumerProps(server);
    properties.put(
        ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,
        ConsumerConfig.DEFAULT_MAX_PARTITION_FETCH_BYTES);

    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
      List<TopicPartition> topicPartitions = getTopicPartitions(getPartitions(consumer, topic));

      Map<String, Map<TopicPartition, Long>> minMax = getOffsetMetadata(consumer, topicPartitions);
      Optional<TopicPartition> optionalTopicPartition =
          topicPartitions.stream().filter(tp -> tp.partition() == partition).findFirst();
      if (optionalTopicPartition.isEmpty()) {
        throw new RuntimeException("Did not find partition " + partition + " on topic " + topic);
      }

      var topicPartition = optionalTopicPartition.get();

      log.info(
          String.format(
              "Assigning topic %s and partition %d ",
              topicPartition.topic(), topicPartition.partition()));
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

  @Override
  public Optional<ConsumerRecord<?, ?>> searchEvent(SearchEventRequest searchEventRequest) {
    Properties properties = getConsumerProps(searchEventRequest.getConnection());
    log.trace("Search strategy selected is " + searchEventRequest.getSearchStrategy().toString());

    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
      switch (searchEventRequest.getSearchStrategy()) {
        case TOPIC:
          return getConsumerRecordByTopic(consumer, searchEventRequest);
        case PARTITION:
          return getConsumerRecordByPartition(consumer, searchEventRequest);
        default:
          throw new RuntimeException(
              "Unknown search strategy " + searchEventRequest.getSearchStrategy().toString());
      }
    }
  }

  @Override
  public Optional<ConsumerRecord<?, ?>> getEventByOffset(
      String server, String topic, int partition, long offset) {
    Properties properties = getConsumerProps(server);
    properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
      List<TopicPartition> topicPartitions = getTopicPartitions(getPartitions(consumer, topic));
      TopicPartition topicPartition = new TopicPartition(topic, partition);
      consumer.assign(Collections.singletonList(topicPartition));
      consumer.seek(topicPartition, offset - 1);
      var records = consumer.poll(Duration.ofSeconds(30)); // should be enough to assign and seek;
      log.trace("processing records with batch size " + records.count());
      if (records.isEmpty()) {
        log.info(
            String.format(
                "Event with offset %d not found on partition %d of topic %s on server %s",
                offset, partition, topic, server));
        consumer.unsubscribe();
        return Optional.empty();
      }
      for (var record : records) {
        consumer.unsubscribe();
        return Optional.of(record);
      }
    }
    return Optional.empty();
  }

  private Optional<ConsumerRecord<?, ?>> getConsumerRecordByTopic(
      KafkaConsumer<String, String> consumer, SearchEventRequest searchEventRequest) {
    List<TopicPartition> topicPartitions =
        getTopicPartitions(getPartitions(consumer, searchEventRequest.getTopic()));

    var offsetMetadata = getOffsetMetadata(consumer, topicPartitions);
    Long lastOffset = getLastOffset(offsetMetadata, getPartitionWithEndOffset(offsetMetadata));
    log.trace("Last offset for topic " + searchEventRequest.getTopic() + " is " + lastOffset);
    consumer.assign(topicPartitions);
    consumer.seekToBeginning(topicPartitions);
    List<SearchEventRequest.Header> searchHeaders = searchEventRequest.getHeaders();

    return getConsumerRecord(consumer, searchEventRequest, lastOffset);
  }

  private Optional<ConsumerRecord<?, ?>> getConsumerRecordByPartition(
      KafkaConsumer<String, String> consumer, SearchEventRequest searchEventRequest) {

    List<SearchEventRequest.Header> searchHeaders = searchEventRequest.getHeaders();
    List<TopicPartition> topicPartitions =
        getTopicPartitions(getPartitions(consumer, searchEventRequest.getTopic()));

    Set<Integer> partitionsToScan =
        searchEventRequest.getPartitions().stream()
            .map(SearchEventRequest.Partition::getId)
            .collect(Collectors.toSet());

    var offsetMetadata = getOffsetMetadata(consumer, topicPartitions);
    for (var topicPartition : topicPartitions) {
      if (!partitionsToScan.contains(topicPartition.partition())) {
        log.info("Skip scanning partition: " + topicPartition.toString());
        continue;
      }
      Optional<SearchEventRequest.Partition> partition =
          searchEventRequest.getPartitions().stream()
              .filter(p -> p.getId() == topicPartition.partition())
              .findFirst();
      log.info("Search for event on partition: " + topicPartition.toString());
      if (partition.isEmpty()) {
        throw new RuntimeException("this should not happen...");
      }
      Long lastOffset = getLastOffset(offsetMetadata, topicPartition);
      Long beginOffset = getBeginOffset(offsetMetadata, topicPartition, partition.get());
      consumer.assign(Collections.singletonList(topicPartition));
      consumer.seek(topicPartition, beginOffset);
      var optionalConsumerRecord = getConsumerRecord(consumer, searchEventRequest, lastOffset);
      if (optionalConsumerRecord.isPresent()) {
        return optionalConsumerRecord;
      }
    }
    return Optional.empty();
  }

  private Optional<ConsumerRecord<?, ?>> getConsumerRecord(
      KafkaConsumer<String, String> consumer,
      SearchEventRequest searchEventRequest,
      Long lastOffset) {
    boolean stopPolling = false;
    List<SearchEventRequest.Header> searchHeaders = searchEventRequest.getHeaders();
    while (!stopPolling) {
      var records = consumer.poll(Duration.ofSeconds(30)); // should be enough to assign and seek;
      log.trace("processing records with batch size " + records.count());
      if (records.isEmpty()) {
        log.info("Search failed for " + searchEventRequest.toString());
        consumer.unsubscribe();
        return Optional.empty();
      }
      for (var record : records) {
        if (isAMatch(record, searchHeaders)) {
          log.info("Found a matching record");
          consumer.unsubscribe();
          return Optional.of(record);
        }
        if (record.offset() >= lastOffset) {
          log.info("Reached end of topic, no record matching search criteria was found");
          stopPolling = true;
        }
      }
      consumer.commitSync(Duration.ofSeconds(10));
    }

    consumer.unsubscribe();
    return Optional.empty();
  }

  private List<PartitionInfo> getPartitions(KafkaConsumer<String, String> consumer, String topic) {
    List<PartitionInfo> partitions = consumer.partitionsFor(topic);
    if (partitions == null || partitions.size() == 0) {
      throw new TopicNotFoundException(topic);
    }
    return partitions;
  }

  private boolean isAMatch(ConsumerRecord<?, ?> record, List<SearchEventRequest.Header> headers) {
    log.trace("check if match for event with offset " + record.offset());
    //    log.trace("Checking if event with " + record.offset() + "  is a match with headers " +
    // headers.toString());
    if (headers.isEmpty()) {
      log.trace("Headers for search were empty");
      return true;
    }
    Headers recordHeaders = record.headers();
    List<Boolean> matches = new ArrayList<>();

    for (var header : headers) {
      Iterator<Header> recordHeaderIterator = recordHeaders.headers(header.getKey()).iterator();
      if (!recordHeaderIterator.hasNext()) {
        log.trace("Header " + header.getKey() + " not found in record headers");
        matches.add(false);
        break; // did not find the header no need to continue
      }
      while (recordHeaderIterator.hasNext()) {
        Header recordHeader = recordHeaderIterator.next();
        if (header
            .getValue()
            .equalsIgnoreCase(new String(recordHeader.value(), StandardCharsets.UTF_8))) {
          matches.add(true);
          break; // found one value that matched, move to the next header
        }
      }
    }
    if (matches.size() == 0) {
      return false;
    }
    return matches.stream().allMatch(Predicate.isEqual(true));
  }

  private static Long getLastOffset(
      Map<String, Map<TopicPartition, Long>> minMax, TopicPartition topicPartition) {
    Long offset = minMax.get("end").get(topicPartition);
    if (offset == 0) {
      return offset;
    }
    return offset - 1;
  }

  private static Long getBeginOffset(
      Map<String, Map<TopicPartition, Long>> minMax,
      TopicPartition topicPartition,
      SearchEventRequest.Partition partition) {
    Long beginOffset = minMax.get("begin").get(topicPartition);
    Long endOffset = minMax.get("end").get(topicPartition);
    // handle base case
    if (endOffset == 0 || endOffset - beginOffset < 10) {
      return beginOffset;
    }
    long move = ((endOffset - beginOffset) * partition.getStartPercentage()) / 100;
    return beginOffset + move;
  }

  @SneakyThrows
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
      properties.put(
          ConsumerConfig.GROUP_ID_CONFIG,
          "DMSKafkaEventFinder" + InetAddress.getLocalHost().getHostName());
      properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
      properties.put(
          ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,
          ConsumerConfig.DEFAULT_MAX_PARTITION_FETCH_BYTES * 5);
      properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 60000);
      properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 300000);
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
    log.trace("begin offsets: " + minMax.get("begin").toString());
    log.trace("end offsets: " + minMax.get("end").toString());
    return minMax;
  }

  private static TopicPartition getPartitionWithBeginOffset(
      Map<String, Map<TopicPartition, Long>> offsets) {
    var beginOffsets = offsets.get("begin");
    long begin = 0;
    TopicPartition beginPartition = null;
    for (var tp : beginOffsets.keySet()) {
      if (begin == 0 || beginOffsets.get(tp) <= begin) {
        begin = beginOffsets.get(tp);
        beginPartition = tp;
      }
    }
    return beginPartition;
  }

  private static TopicPartition getPartitionWithEndOffset(
      Map<String, Map<TopicPartition, Long>> offsets) {
    var endOffsets = offsets.get("end");
    long end = 0;
    TopicPartition endPartition = null;
    for (var tp : endOffsets.keySet()) {
      if (end == 0 || endOffsets.get(tp) >= end) {
        end = endOffsets.get(tp);
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
