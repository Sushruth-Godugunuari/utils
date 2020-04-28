package com.sushruth.kafka.eventfinder.controller;

import com.sushruth.kafka.eventfinder.dto.EventDto;
import com.sushruth.kafka.eventfinder.dto.SearchEventRequestDto;
import com.sushruth.kafka.eventfinder.model.SearchEventRequest;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.utils.Bytes;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Slf4j
public class TopicControllerMapper {
  public static EventDto mapToEventDto(ConsumerRecord<?, ?> record) {
    EventDto dto = new EventDto();
    dto.setPartition(record.partition());
    dto.setOffset(record.offset());
    dto.setTimestamp(record.timestamp());
    dto.setHeaders(
        StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(
                    record.headers().iterator(), Spliterator.ORDERED),
                false)
            .map(TopicControllerMapper::mapToEventDtoHeader)
            .collect(Collectors.toList()));

    if (record.key() == null) {
      log.error("event key  is null");
    }

    if (record.value() == null) {
      log.error("event value is null");
    }

    if (record.key() instanceof Bytes) {
      dto.setKey(Base64.getEncoder().encodeToString(((Bytes) record.key()).get()));
    } else if (record.key() instanceof String) {
      dto.setKey((String) record.key());
    }
    if (record.value() instanceof Bytes) {
      dto.setValue(Base64.getEncoder().encodeToString(((Bytes) record.value()).get()));
    } else if (record.value() instanceof String) {
      dto.setValue((String) record.value());
    }
    return dto;
  }

  public static EventDto.Header mapToEventDtoHeader(Header header) {
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

  public static SearchEventRequest mapToSearchEventRequest(
      String connection, String topic, SearchEventRequestDto dto) {
    SearchEventRequest model = new SearchEventRequest();
    model.setConnection(connection);
    model.setTopic(topic);

    if (dto.getPartitions().size() > 0) {
      model.setPartitions(
          dto.getPartitions().stream()
              .map(TopicControllerMapper::mapToSearchEventRequestPartition)
              .collect(Collectors.toList()));
    }

    if (dto.getHeaders() == null || dto.getHeaders().size() == 0) {
      throw new RuntimeException("At least one search header required");
    }

    model.setHeaders(
        dto.getHeaders().stream()
            .map(TopicControllerMapper::mapToSearchEventRequestHeader)
            .collect(Collectors.toList()));

    model.setSearchStrategy(
        SearchEventRequest.SearchStrategy.valueOf(dto.getSearchStrategy().toString()));
    return model;
  }

  public static SearchEventRequest.Header mapToSearchEventRequestHeader(
      SearchEventRequestDto.Header dto) {
    SearchEventRequest.Header model = new SearchEventRequest.Header();
    model.setKey(dto.getKey());
    model.setValue(dto.getValue());
    return model;
  }

  public static SearchEventRequest.Partition mapToSearchEventRequestPartition(
      SearchEventRequestDto.Partition dto) {
    if (dto.getStartPercentage() < 0 || dto.getStartPercentage() > 100) {
      throw new RuntimeException("Invalid seek percentage provide");
    }
    SearchEventRequest.Partition model = new SearchEventRequest.Partition();
    model.setId(dto.getId());
    model.setStartPercentage(dto.getStartPercentage());
    return model;
  }
}
