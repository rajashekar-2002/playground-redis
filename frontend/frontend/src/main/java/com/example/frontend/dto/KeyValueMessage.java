package com.example.frontend.dto;

import lombok.*;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class KeyValueMessage {

    private OperationType operation;
    private String key;
    private String value;
}