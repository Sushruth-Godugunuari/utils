package com.sushruth.kafka.eventfinder.controller;

import com.sushruth.kafka.eventfinder.dto.EventDto;
import com.sushruth.kafka.eventfinder.dto.OffsetMetadataDto;
import com.sushruth.kafka.eventfinder.dto.SearchEventRequestDto;
import com.sushruth.kafka.eventfinder.model.SearchEventRequest;
import com.sushruth.kafka.eventfinder.service.TopicService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.utils.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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
        events.stream().map(TopicControllerV1Impl::mapToEventDto).collect(Collectors.toList()),
        HttpStatus.OK);
  }

    @Override
    public ResponseEntity<EventDto> getFirstEvent(String connectionName, String topicName) {
      Optional<ConsumerRecord<?,?>> optionalEvent = topicService.getFirstEvent(connectionName, topicName);
      if (optionalEvent.isEmpty()){
        return ResponseEntity.notFound().build();
      }

      return new ResponseEntity<>(mapToEventDto(optionalEvent.get()), HttpStatus.OK);
    }

  @Override
  public ResponseEntity<EventDto> getLastEvent(String connectionName, String topicName) {
    Optional<ConsumerRecord<?,?>> optionalEvent = topicService.getLastEvent(connectionName, topicName);
    if (optionalEvent.isEmpty()){
      return ResponseEntity.notFound().build();
    }

    return new ResponseEntity<>(mapToEventDto(optionalEvent.get()), HttpStatus.OK);
  }

  @Override
  public ResponseEntity<EventDto> searchEvent(String connectionName, String topicName, SearchEventRequestDto searchEventRequestDto) {
    log.info(String.format("Search server %s with topicName %s with %s ", connectionName, topicName, searchEventRequestDto));
    Optional<ConsumerRecord<?, ?>> optionalEvent = topicService.searchEvent(mapToSearchEventRequest(connectionName, topicName, searchEventRequestDto));
    if (optionalEvent.isEmpty()){
      return ResponseEntity.notFound().build();
    }
    return new ResponseEntity<>(mapToEventDto(optionalEvent.get()), HttpStatus.OK);
  }

  private static EventDto mapToEventDto(ConsumerRecord<?, ?> record) {
    EventDto dto = new EventDto();
    dto.setPartition(record.partition());
    dto.setOffset(record.offset());
    dto.setTimestamp(record.timestamp());
    dto.setHeaders(
        StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(
                    record.headers().iterator(), Spliterator.ORDERED),
                false)
            .map(TopicControllerV1Impl::mapToEventDtoHeader)
            .collect(Collectors.toList()));

    if (record.key() == null){
      log.error("event key  is null");
    }

    if (record.value() == null){
      log.error("event value is null");
    }

    if (record.key() instanceof Bytes) {
      dto.setKey(Base64.getEncoder().encodeToString( ((Bytes)record.key()).get()));
    } else if (record.key() instanceof String){
        dto.setKey((String) record.key());
    }
    if (record.value() instanceof Bytes) {
      dto.setValue(Base64.getEncoder().encodeToString(((Bytes) record.value()).get()));
    } else if (record.value() instanceof String){
        dto.setValue((String)record.value());
    }
    return dto;
  }

  private static EventDto.Header mapToEventDtoHeader(Header header) {
    EventDto.Header dto = new EventDto.Header();
    dto.setKey(header.key());
    // attempt to convert bytes to string
    try {
      dto.setValue(new String(header.value(), StandardCharsets.UTF_8));
    } catch (Exception e) {
      dto.setValue(Base64.getEncoder().encodeToString(header.value()));
    }
    return dto;
  }

  private static SearchEventRequest mapToSearchEventRequest(String connection, String topic, SearchEventRequestDto dto){
    SearchEventRequest model = new SearchEventRequest();
    model.setConnection(connection);
    model.setTopic(topic);
    model.setPartition(dto.getPartition());
    model.setHeaders(dto.getHeaders().stream().map(TopicControllerV1Impl::mapToSearchEventRequestHeader).collect(Collectors.toList()));
    return model;
  }

  private static SearchEventRequest.Header mapToSearchEventRequestHeader(SearchEventRequestDto.Header dto){
    SearchEventRequest.Header model = new SearchEventRequest.Header();
    model.setKey(dto.getKey());
    model.setValue(dto.getValue());
    return model;
  }
}
