package com.sushruth.kafka.eventfinder.repo;

import com.sushruth.kafka.eventfinder.entity.ConnectionEntity;
import org.springframework.data.repository.CrudRepository;

import java.util.Optional;

public interface ConnectionRepo extends CrudRepository<ConnectionEntity, String> {
    Optional<ConnectionEntity> findByName(String name);
}
