package com.sushruth.kafka.eventfinder.controller;

import com.sushruth.kafka.eventfinder.dto.KafkaServerConfigRequestDto;
import com.sushruth.kafka.eventfinder.dto.KafkaServerConfigResponseDto;
import com.sushruth.kafka.eventfinder.model.KafkaServerConfig;
import io.swagger.annotations.Api;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("api/v1/server-config")
@Api(
        value = "kafka-server-config",
        description =
                "configure kafka servers ",
        tags = {"config"})
public interface ConfigControllerV1 {
    @GetMapping
    ResponseEntity<List<KafkaServerConfigResponseDto>> getAllServersConfig();

    @PostMapping
    ResponseEntity<KafkaServerConfigResponseDto> addServer(@RequestBody KafkaServerConfigRequestDto requestDto);

    @GetMapping("/name/{name}")
    ResponseEntity<KafkaServerConfigResponseDto> getServerConfigByName(@PathVariable(name = "name") String name);

    @GetMapping("/id/{id}")
    ResponseEntity<KafkaServerConfigResponseDto> getServerConfigById(@PathVariable(name = "id") String id);

    @DeleteMapping("/name/{name}")
    ResponseEntity<Void> deleteServerConfigByName(@PathVariable(name = "name") String name);

    @DeleteMapping("/id/{id}")
    ResponseEntity<Void> deleteServerConfigById(@PathVariable(name = "id") String id);

    @PatchMapping("/name/{name}")
    ResponseEntity<KafkaServerConfigResponseDto> updateServerConfigByName(@RequestBody KafkaServerConfigRequestDto requestDto);

    @PatchMapping("/name/{id}")
    ResponseEntity<KafkaServerConfigResponseDto> updateServerConfigById(@RequestBody KafkaServerConfigRequestDto requestDto);

    @PostMapping("export")
    ResponseEntity<String> exportConfig();

}
