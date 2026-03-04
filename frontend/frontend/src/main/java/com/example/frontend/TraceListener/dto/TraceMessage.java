package com.example.frontend.TraceListener.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TraceMessage {
    private String serviceName;
    private String serviceId;
    private String messageId;
    private String payload;
}