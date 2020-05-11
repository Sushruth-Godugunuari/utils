package com.sushruth.kafka.eventfinder.controller;

import com.sushruth.kafka.eventfinder.dto.EventDto;
import com.sushruth.kafka.eventfinder.dto.OffsetMetadataDto;
import com.sushruth.kafka.eventfinder.dto.SearchEventRequestDto;
import io.swagger.annotations.Api;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("api/v1/topic")
@Api(
        value = "kafka-topic",
        description =
                "kafka topic browser",
        tags = {"topic"})
public interface TopicControllerV1 {

    @GetMapping("/{connectionName}/topics/{topicName}/offsets")
    ResponseEntity<List<OffsetMetadataDto>> getOffSetMetadata(@PathVariable(name = "connectionName") String connectionName, @PathVariable(name = "topicName") String topicName);

    @GetMapping("/{connectionName}/topics/{topicName}/first-events")
    ResponseEntity<List<EventDto>> getFirstEventsByPartitions(@PathVariable(name = "connectionName") String connectionName, @PathVariable(name = "topicName") String topicName);

    @GetMapping("/{connectionName}/topics/{topicName}/first-event")
    ResponseEntity<EventDto> getFirstEvent(@PathVariable(name = "connectionName") String connectionName, @PathVariable(name = "topicName")  String topicName, @RequestParam(name ="partition") int partition);

    @GetMapping("/{connectionName}/topics/{topicName}/last-event")
    ResponseEntity<EventDto> getLastEvent(@PathVariable(name = "connectionName") String connectionName, @PathVariable(name = "topicName") String topicName, @RequestParam(name ="partition") int partition);

    @PostMapping("/{connectionName}/topics/{topicName}/search")
    ResponseEntity<EventDto> searchEvent(@PathVariable(name = "connectionName") String connectionName, @PathVariable(name = "topicName") String topicName, @RequestBody SearchEventRequestDto searchEventRequestDto);

    @GetMapping("/correlation-id")
    ResponseEntity<EventDto> getEventByCorrelationId(@RequestParam(name = "correlationId") String correlationId);

}
