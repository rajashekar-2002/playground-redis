package com.example.redis_service.operations;

import com.example.redis_service.dto.RedisEvent;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class RedisOperationDispatcher {

    private final Map<String, RedisOperationHandler> handlerMap;

    public RedisOperationDispatcher(List<RedisOperationHandler> handlers) {

        this.handlerMap = handlers.stream()
                .collect(Collectors.toMap(
                        RedisOperationHandler::getOperationType,
                        handler -> handler));
    }

    public void dispatch(RedisEvent event) {

        RedisOperationHandler handler = handlerMap.get(event.getOperation());

        if (handler == null) {
            throw new RuntimeException(
                    "No handler found for operation: "
                            + event.getOperation());
        }

        handler.handle(event);
    }
}