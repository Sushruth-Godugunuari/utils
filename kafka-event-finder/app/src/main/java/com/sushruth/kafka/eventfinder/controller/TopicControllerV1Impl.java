package com.sushruth.kafka.eventfinder.controller;

import com.sushruth.kafka.eventfinder.dto.EventDto;
import com.sushruth.kafka.eventfinder.dto.OffsetMetadataDto;
import com.sushruth.kafka.eventfinder.dto.SearchEventRequestDto;
import com.sushruth.kafka.eventfinder.service.TopicService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Controller
@Slf4j
public class TopicControllerV1Impl implements TopicControllerV1 {
  TopicService topicService;

  @Autowired
  public TopicControllerV1Impl(TopicService topicService) {
    this.topicService = topicService;
  }

  @Override
  public ResponseEntity<List<OffsetMetadataDto>> getOffSetMetadata(
      String connectionName, String topicName) {

    Map<String, Map<TopicPartition, Long>> offsets =
        topicService.getTopicOffsets(connectionName, topicName);
    Map<TopicPartition, Long> begin = offsets.get("begin");
    Map<TopicPartition, Long> end = offsets.get("end");

    List<OffsetMetadataDto> offsetMetadataDtos = new ArrayList<>();
    begin.forEach(
        (partition, beginOffset) -> {
          OffsetMetadataDto dto = new OffsetMetadataDto();
          dto.setTopic(partition.topic());
          dto.setPartition(partition.partition());
          dto.setBegin(beginOffset);
          dto.setEnd(end.get(partition));
          offsetMetadataDtos.add(dto);
        });
    return new ResponseEntity<>(offsetMetadataDtos, HttpStatus.OK);
  }

  @Override
  public ResponseEntity<List<EventDto>> getFirstEventsByPartitions(
      String connectionName, String topicName) {
    List<ConsumerRecord<?, ?>> events = topicService.getFirstEvents(connectionName, topicName);
    List<EventDto> dtos = new ArrayList<>();
    return new ResponseEntity<>(
        events.stream().map(TopicControllerMapper::mapToEventDto).collect(Collectors.toList()),
        HttpStatus.OK);
  }

    @Override
    public ResponseEntity<EventDto> getFirstEvent(String connectionName, String topicName, int partition) {
      Optional<ConsumerRecord<?,?>> optionalEvent = topicService.getFirstEvent(connectionName, topicName, partition);
      if (optionalEvent.isEmpty()){
        return ResponseEntity.notFound().build();
      }

      return new ResponseEntity<>(TopicControllerMapper.mapToEventDto(optionalEvent.get()), HttpStatus.OK);
    }

  @Override
  public ResponseEntity<EventDto> getLastEvent(String connectionName, String topicName, int partition) {
    Optional<ConsumerRecord<?,?>> optionalEvent = topicService.getLastEvent(connectionName, topicName, partition);
    if (optionalEvent.isEmpty()){
      return ResponseEntity.notFound().build();
    }

    return new ResponseEntity<>(TopicControllerMapper.mapToEventDto(optionalEvent.get()), HttpStatus.OK);
  }

  @Override
  public ResponseEntity<EventDto> searchEvent(String connectionName, String topicName, SearchEventRequestDto searchEventRequestDto) {
    log.info(String.format("Search server %s with topicName %s with %s ", connectionName, topicName, searchEventRequestDto));
    Optional<ConsumerRecord<?, ?>> optionalEvent = topicService.searchEvent(TopicControllerMapper.mapToSearchEventRequest(connectionName, topicName, searchEventRequestDto));
    if (optionalEvent.isEmpty()){
      return ResponseEntity.notFound().build();
    }
    return new ResponseEntity<>(TopicControllerMapper.mapToEventDto(optionalEvent.get()), HttpStatus.OK);
  }

  @Override
  public ResponseEntity<EventDto> getEventByCorrelationId(String correlationId) {
    // parse co
    log.info(String.format("find event by correlationId %s", correlationId));

    Matcher m = Pattern.compile("^([^/]+)/([^/]+)/([^/]+)/([^/]+)$").matcher(correlationId);
    if(!m.matches())
    {
      log.error("Unable to parse correlationId '" + correlationId + "'");
      return ResponseEntity.badRequest().build();
    }
    String server = m.group(1);
    String topic = m.group(2);
    int partitionID = Integer.parseInt(m.group(3));
    long offset = Long.parseLong(m.group(4));

    Optional<ConsumerRecord<?, ?>> optionalEvent = topicService.getEventByOffset(
            server, topic, partitionID, offset
    );
    if (optionalEvent.isEmpty()){
      return ResponseEntity.notFound().build();
    }
    return new ResponseEntity<>(TopicControllerMapper.mapToEventDto(optionalEvent.get()), HttpStatus.OK);
  }
}
