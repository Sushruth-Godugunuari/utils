package com.sushruth.kafka.eventfinder.controller;

import com.sushruth.kafka.eventfinder.dto.ConsumerGroupInfoDto;
import com.sushruth.kafka.eventfinder.dto.ConsumerGroupListingDto;
import com.sushruth.kafka.eventfinder.dto.TopicInfoDto;
import com.sushruth.kafka.eventfinder.exception.ConnectionNotFoundException;
import com.sushruth.kafka.eventfinder.service.KafkaAdminService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Controller
@Slf4j
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
    } catch (ExecutionException | InterruptedException e) {
      e.printStackTrace();
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
    } catch (ConnectionNotFoundException ce) {
      return ResponseEntity.notFound().build();
    }
  }

  @Override
  public void connect(String connectionName) {}

  @Override
  public ResponseEntity<Collection<ConsumerGroupListingDto>> getConsumerGroups(
      String connectionName) {
    try {
      return new ResponseEntity<>(
          kafkaAdminService.getConsumerGroups(connectionName).stream()
              .map(AdminControllerV1Impl::mapToConsumerListingDto)
              .collect(Collectors.toList()),
          HttpStatus.OK);
    } catch (ExecutionException | InterruptedException e) {
      e.printStackTrace();
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
    } catch (ConnectionNotFoundException ce) {
      return ResponseEntity.notFound().build();
    }
  }

  @Override
  public ResponseEntity<ConsumerGroupInfoDto> getConsumerGroup(
      String connectionName, String groupName) {
    try {
      log.info("Got request for " + groupName);
      return new ResponseEntity<>(
          mapToConsumerGroupInfoDto(kafkaAdminService.getConsumerGroup(connectionName, groupName)),
          HttpStatus.OK);
    } catch (ExecutionException | InterruptedException e) {
      e.printStackTrace();
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
    } catch (ConnectionNotFoundException ce) {
      return ResponseEntity.notFound().build();
    }
  }

  @Override
  public ResponseEntity<Set<String>> getTopics(String connectionName) {
    try {
      return new ResponseEntity<>(kafkaAdminService.getTopics(connectionName), HttpStatus.OK);
    } catch (ExecutionException | InterruptedException e) {
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
    } catch (ExecutionException | InterruptedException e) {
      e.printStackTrace();
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
    } catch (ConnectionNotFoundException ce) {
      return ResponseEntity.notFound().build();
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
    dto.setLeader(TopicInfoNode(tp.leader()));
    dto.setIsr(
        tp.isr().stream().map(AdminControllerV1Impl::TopicInfoNode).collect(Collectors.toList()));
    dto.setReplicas(
        tp.replicas().stream()
            .map(AdminControllerV1Impl::TopicInfoNode)
            .collect(Collectors.toList()));
    return dto;
  }

  public static TopicInfoDto.Node TopicInfoNode(Node node) {
    TopicInfoDto.Node dto = new TopicInfoDto.Node();
    dto.setHost(node.host());
    dto.setId(node.id());
    dto.setPort(node.port());
    dto.setIdString(node.idString());
    dto.setRack(node.rack());
    return dto;
  }

  public static ConsumerGroupInfoDto.Node mapToGroupInfoNode(Node node) {
    ConsumerGroupInfoDto.Node dto = new ConsumerGroupInfoDto.Node();
    dto.setHost(node.host());
    dto.setId(node.id());
    dto.setPort(node.port());
    dto.setIdString(node.idString());
    dto.setRack(node.rack());
    return dto;
  }

  public static ConsumerGroupListingDto mapToConsumerListingDto(ConsumerGroupListing listing) {
    ConsumerGroupListingDto dto = new ConsumerGroupListingDto();
    dto.setGroupId(listing.groupId());
    dto.setSimpleConsumerGroup(listing.isSimpleConsumerGroup());
    return dto;
  }

  public static ConsumerGroupInfoDto.TopicPartition mapToTopicPartition(TopicPartition partition) {
    ConsumerGroupInfoDto.TopicPartition dto = new ConsumerGroupInfoDto.TopicPartition();
    dto.setPartition(partition.partition());
    dto.setTopic(partition.topic());
    return dto;
  }

  public static ConsumerGroupInfoDto.MemberDescription mapToMemberDescription(
      MemberDescription member) {
    ConsumerGroupInfoDto.MemberDescription dto = new ConsumerGroupInfoDto.MemberDescription();
    dto.setClientId(member.clientId());
    dto.setGroupInstanceId(member.groupInstanceId().orElse(""));
    dto.setHost(member.host());
    dto.setMemberAssignmentPartition(
        member.assignment().topicPartitions().stream()
            .map(AdminControllerV1Impl::mapToTopicPartition)
            .collect(Collectors.toSet()));
    dto.setMemberId(member.consumerId());
    return dto;
  }

  public static ConsumerGroupInfoDto mapToConsumerGroupInfoDto(ConsumerGroupDescription group) {
    ConsumerGroupInfoDto dto = new ConsumerGroupInfoDto();
    dto.setCoordinator(mapToGroupInfoNode(group.coordinator()));
    dto.setGroupId(group.groupId());
    dto.setPartitionAssignor(group.partitionAssignor());
    dto.setSimpleConsumerGroup(group.isSimpleConsumerGroup());
    dto.setState(group.state().name());
    dto.setMembers(
        group.members().stream()
            .map(AdminControllerV1Impl::mapToMemberDescription)
            .collect(Collectors.toList()));
    return dto;
  }
}
