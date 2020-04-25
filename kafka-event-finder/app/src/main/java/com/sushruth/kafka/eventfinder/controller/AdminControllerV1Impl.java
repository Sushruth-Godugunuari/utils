package com.sushruth.kafka.eventfinder.controller;

import com.sushruth.kafka.eventfinder.dto.TopicInfoDto;
import com.sushruth.kafka.eventfinder.exception.ConnectionNotFoundException;
import com.sushruth.kafka.eventfinder.service.KafkaAdminService;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Controller
public class AdminControllerV1Impl implements AdminControllerV1 {
  KafkaAdminService kafkaAdminService;

  @Autowired
  public AdminControllerV1Impl(KafkaAdminService kafkaAdminService) {
    this.kafkaAdminService = kafkaAdminService;
  }

  @Override
  public ResponseEntity<String> getConnectionStatus(String connectionName) {
    try {
      return new ResponseEntity<>(
          kafkaAdminService.getConnectionStatus(connectionName), HttpStatus.OK);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      e.printStackTrace();
      return new ResponseEntity<>(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
    } catch (ExecutionException e) {
      e.printStackTrace();
      return new ResponseEntity<>(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
    } catch (ConnectionNotFoundException ce) {
      return new ResponseEntity<>(ce.getLocalizedMessage(), HttpStatus.NOT_FOUND);
    }
  }

  @Override
  public void connect(String connectionName) {}

  @Override
  public void getConsumerGroups(String connectionName) {}

  @Override
  public void getConsumerGroup(String connectionName, String groupName) {}

  @Override
  public ResponseEntity<Set<String>> getTopics(String connectionName) {
    try {
      return new ResponseEntity<>(kafkaAdminService.getTopics(connectionName), HttpStatus.OK);
    } catch (ExecutionException e) {
      e.printStackTrace();
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
    } catch (InterruptedException e) {
      e.printStackTrace();
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
    } catch (ConnectionNotFoundException ce) {
      return ResponseEntity.notFound().build();
    }
  }

  @Override
  public ResponseEntity<TopicInfoDto> getTopicInfo(String connectionName, String topicName) {
    try {
      TopicDescription topicDescription = kafkaAdminService.getTopicInfo(connectionName, topicName);

      return new ResponseEntity<>(mapToTopicInfoDto(topicDescription), HttpStatus.OK);
    } catch (ExecutionException e) {
      e.printStackTrace();
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
    } catch (InterruptedException e) {
      e.printStackTrace();
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
    }
  }

  public static TopicInfoDto mapToTopicInfoDto(TopicDescription td) {
    TopicInfoDto dto = new TopicInfoDto();
    dto.setInternal(td.isInternal());
    dto.setName(td.name());
    dto.setPartitions(
        td.partitions().stream()
            .map(AdminControllerV1Impl::mapToTopicPartitionInfoDto)
            .collect(Collectors.toList()));
    return dto;
  }

  public static TopicInfoDto.TopicPartitionInfoDto mapToTopicPartitionInfoDto(
      TopicPartitionInfo tp) {
    TopicInfoDto.TopicPartitionInfoDto dto = new TopicInfoDto.TopicPartitionInfoDto();
    dto.setPartition(tp.partition());
    dto.setLeader(mapToNode(tp.leader()));
    dto.setIsr(
        tp.isr().stream().map(AdminControllerV1Impl::mapToNode).collect(Collectors.toList()));
    dto.setReplicas(
        tp.replicas().stream().map(AdminControllerV1Impl::mapToNode).collect(Collectors.toList()));
    return dto;
  }

  public static TopicInfoDto.Node mapToNode(Node node) {
    TopicInfoDto.Node dto = new TopicInfoDto.Node();
    dto.setHost(node.host());
    dto.setId(node.id());
    dto.setPort(node.port());
    dto.setIdString(node.idString());
    dto.setRack(node.rack());
    return dto;
  }
}
