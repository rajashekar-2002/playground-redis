package com.example.redis_service.repository;

import java.util.Optional;

import org.springframework.data.mongodb.repository.MongoRepository;

import com.example.redis_service.models.KeyValueDocument;

public interface KeyValueRepository extends MongoRepository<KeyValueDocument, String> {
    Optional<KeyValueDocument> findByKey(String key);

    void deleteByKey(String key);
}