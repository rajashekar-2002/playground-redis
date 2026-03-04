package com.example.redis_service.operations;

import com.example.redis_service.dto.RedisEvent;

public interface RedisOperationHandler {

    String getOperationType(); // ADD, DELETE, etc.

    void handle(RedisEvent event);
}