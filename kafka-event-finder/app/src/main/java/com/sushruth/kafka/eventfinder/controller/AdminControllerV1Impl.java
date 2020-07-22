package com.sushruth.kafka.eventfinder.controller;

import com.sushruth.kafka.eventfinder.dto.ConsumerGroupInfoDto;
import com.sushruth.kafka.eventfinder.dto.ConsumerGroupListingDto;
import com.sushruth.kafka.eventfinder.dto.TopicInfoDto;
import com.sushruth.kafka.eventfinder.exception.ConnectionNotFoundException;
import com.sushruth.kafka.eventfinder.model.ConsumerGroupDescriptionWrapper;
import com.sushruth.kafka.eventfinder.service.KafkaAdminService;
import com.sushruth.kafka.eventfinder.service.TopicService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Controller
@Slf4j
public class AdminControllerV1Impl implements AdminControllerV1 {
  private final KafkaAdminService kafkaAdminService;
  private final TopicService topicService;

  @Autowired
  public AdminControllerV1Impl(KafkaAdminService kafkaAdminService, TopicService topicService) {
    this.kafkaAdminService = kafkaAdminService;
    this.topicService = topicService;
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
      Map<String, Map<TopicPartition, Long>> offsets =
          topicService.getTopicOffsets(connectionName, topicName);

      return new ResponseEntity<>(mapToTopicInfoDto(topicDescription, offsets), HttpStatus.OK);
    } catch (ExecutionException | InterruptedException e) {
      e.printStackTrace();
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
    } catch (ConnectionNotFoundException ce) {
      return ResponseEntity.notFound().build();
    }
  }

  public static TopicInfoDto mapToTopicInfoDto(
      TopicDescription td, Map<String, Map<TopicPartition, Long>> offsets) {
    TopicInfoDto dto = new TopicInfoDto();
    dto.setInternal(td.isInternal());
    dto.setName(td.name());
    dto.setPartitions(
        td.partitions().stream()
            .map(AdminControllerV1Impl::mapToTopicPartitionInfoDto)
            .collect(Collectors.toList()));
    dto.setOffsets(mapTopicPartitonOffsets(offsets));
    return dto;
  }

  public static Map<Integer, TopicInfoDto.Offset> mapTopicPartitonOffsets(
      Map<String, Map<TopicPartition, Long>> offsets) {

    Map<Integer, TopicInfoDto.Offset> out = new HashMap<>();
    // map begin
    var begin = offsets.get("begin");
    begin.forEach(
        (k, v) -> {
          log.info("map key {} and value {}", k, v);
          var offset = new TopicInfoDto.Offset();
          offset.setBegin(v);
          out.put(k.partition(), offset);
        });

    // map end
    var end = offsets.get("end");
    end.forEach(
        (k, v) -> {
          var offset = out.get(k);
          if (offset == null) {
            log.error("offset was null for {}", k);
          } else {
            offset.setEnd(v);
          }
        });

    return out;
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

  public static ConsumerGroupInfoDto.OffsetAndMetadata mapToOffsetAndMetadat(
      OffsetAndMetadata offset) {
    ConsumerGroupInfoDto.OffsetAndMetadata dto = new ConsumerGroupInfoDto.OffsetAndMetadata();
    dto.setLeaderEpoch(offset.leaderEpoch().orElse(0));
    dto.setMetadata(offset.metadata());
    dto.setOffset(offset.offset());
    return dto;
  }

  public static ConsumerGroupInfoDto mapToConsumerGroupInfoDto(
      ConsumerGroupDescriptionWrapper wrapper) {
    ConsumerGroupInfoDto dto = new ConsumerGroupInfoDto();
    dto.setCoordinator(mapToGroupInfoNode(wrapper.getDescription().coordinator()));
    dto.setGroupId(wrapper.getDescription().groupId());
    dto.setPartitionAssignor(wrapper.getDescription().partitionAssignor());
    dto.setSimpleConsumerGroup(wrapper.getDescription().isSimpleConsumerGroup());
    dto.setState(wrapper.getDescription().state().name());
    dto.setMembers(
        wrapper.getDescription().members().stream()
            .map(AdminControllerV1Impl::mapToMemberDescription)
            .collect(Collectors.toList()));

    // offsets
    Map<ConsumerGroupInfoDto.TopicPartition, ConsumerGroupInfoDto.OffsetAndMetadata> offesets =
        new HashMap<>();
    wrapper
        .getOffsets()
        .forEach((k, v) -> offesets.put(mapToTopicPartition(k), mapToOffsetAndMetadat(v)));
    dto.setOffsetAndMetadataMap(offesets);

    return dto;
  }
}
