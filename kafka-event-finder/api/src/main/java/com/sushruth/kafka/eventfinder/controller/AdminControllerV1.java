package com.sushruth.kafka.eventfinder.controller;

import com.sushruth.kafka.eventfinder.dto.TopicInfoDto;
import io.swagger.annotations.Api;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Set;

@RestController
@RequestMapping("api/v1/admin")
@Api(
        value = "kafka-admin",
        description =
                "kafka admin client",
        tags = {"admin"})
public interface AdminControllerV1 {
    @GetMapping("/{connectionName}")
    ResponseEntity<String> getConnectionStatus(@PathVariable(name = "connectionName")String connectionName);

    @PostMapping("/{connectionName}/connect")
    void connect(@PathVariable(name = "connectionName")String connectionName);

    @GetMapping("/{connectionName}/consumer-groups")
    void getConsumerGroups(@PathVariable(name = "connectionName") String connectionName);

    @GetMapping("/{connectionName}/consumer-groups/{consumerGroup}")
    void getConsumerGroup(@PathVariable(name = "connectionName") String connectionName, @PathVariable(name = "groupName") String groupName);

    @GetMapping("/{connectionName}/topics")
    ResponseEntity<Set<String>> getTopics(@PathVariable(name = "connectionName") String connectionName);

    @GetMapping("/{connectionName}/topics/{topicName}")
    ResponseEntity<TopicInfoDto> getTopicInfo(@PathVariable(name = "connectionName") String connectionName, @PathVariable(name = "topicName") String topicName);
}
