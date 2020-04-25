package com.sushruth.kafka.eventfinder.controller;

import com.sushruth.kafka.eventfinder.dto.KafkaServerConfigRequestDto;
import com.sushruth.kafka.eventfinder.dto.KafkaServerConfigResponseDto;
import com.sushruth.kafka.eventfinder.model.KafkaServerConfig;
import com.sushruth.kafka.eventfinder.service.ConfigService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;

import java.util.List;
import java.util.stream.Collectors;

@Controller
public class ConfigControllerV1Impl implements ConfigControllerV1 {
    ConfigService configService;

    @Autowired
    public ConfigControllerV1Impl(ConfigService configService) {
        this.configService = configService;
    }

    @Override
    public ResponseEntity<List<KafkaServerConfigResponseDto>> getAllServersConfig() {
        var list = configService.getAllServers().stream().map(ConfigControllerV1Impl::modelToResponseDto).collect(Collectors.toList());
        return new ResponseEntity<List<KafkaServerConfigResponseDto>>(list, HttpStatus.OK);
    }

    @Override
    public ResponseEntity<KafkaServerConfigResponseDto> addServer(KafkaServerConfigRequestDto requestDto) {
        return null;
    }

    @Override
    public ResponseEntity<KafkaServerConfigResponseDto> getServerConfigByName(String name) {
        return null;
    }

    @Override
    public ResponseEntity<KafkaServerConfigResponseDto> getServerConfigById(String id) {
        return null;
    }

    @Override
    public ResponseEntity<Void> deleteServerConfigByName(String name) {
        return null;
    }

    @Override
    public ResponseEntity<Void> deleteServerConfigById(String id) {
        return null;
    }

    @Override
    public ResponseEntity<KafkaServerConfigResponseDto> updateServerConfigByName(KafkaServerConfigRequestDto requestDto) {
        return null;
    }

    @Override
    public ResponseEntity<KafkaServerConfigResponseDto> updateServerConfigById(KafkaServerConfigRequestDto requestDto) {
        return null;
    }

    @Override
    public ResponseEntity<String> exportConfig() {
        return null;
    }

    public static KafkaServerConfigResponseDto modelToResponseDto(KafkaServerConfig model){
        KafkaServerConfigResponseDto dto = new KafkaServerConfigResponseDto();

        dto.setId(model.getId());
        dto.setName(model.getName());
        dto.setBootstrapServers(model.getBootstrapServers());
        dto.setSecurityProtocol(model.getSecurityProtocol());
        dto.setApiKey(model.getApiKey());
        dto.setApiSecret(model.getApiSecret());

        dto.setMaxBlockMS(model.getMaxBlockMS());
        dto.setRequestTimeoutMS(model.getRequestTimeoutMS());
        dto.setRetryBackoffMS(model.getRetryBackoffMS());

        return dto;
    }
}
