package com.sushruth.kafka.eventfinder.controller;

import com.sushruth.kafka.eventfinder.exception.ConnectionNotFoundException;
import com.sushruth.kafka.eventfinder.service.KafkaAdminService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;

import java.util.concurrent.ExecutionException;

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
            return  new ResponseEntity<>(kafkaAdminService.getConnectionStatus(connectionName), HttpStatus.OK);
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
    public void connect(String connectionName) {

    }

    @Override
    public void getConsumerGroups(String connectionName) {

    }

    @Override
    public void getConsumerGroup(String connectionName, String groupName) {

    }

    @Override
    public void getTopics(String connectionName) {

    }

    @Override
    public void getTopicInfo(String connectionName, String topicName) {

    }
}
