package com.sushruth.kafka.eventfinder.controller;

import com.sushruth.kafka.eventfinder.dto.KafkaServerConfigRequestDto;
import com.sushruth.kafka.eventfinder.dto.KafkaServerConfigResponseDto;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;

import java.util.List;

@Controller
public class ConfigControllerV1Impl implements ConfigControllerV1 {
    @Override
    public ResponseEntity<List<KafkaServerConfigResponseDto>> getAllServersConfig() {
        return null;
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
}
