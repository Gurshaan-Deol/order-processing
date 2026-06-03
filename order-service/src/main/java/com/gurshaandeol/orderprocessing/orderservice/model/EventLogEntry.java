package com.gurshaandeol.orderprocessing.orderservice.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

@Document(collection = "event_log")
public class EventLogEntry {

    @Id
    private String id;
    private String eventId;
    private String eventType;
    private String orderId;
    private Object payload;
    private Instant processedAt;
    private String serviceId;

    public EventLogEntry() {}

    public EventLogEntry(String id, String eventId, String eventType, String orderId,
                         Object payload, Instant processedAt, String serviceId) {
        this.id = id;
        this.eventId = eventId;
        this.eventType = eventType;
        this.orderId = orderId;
        this.payload = payload;
        this.processedAt = processedAt;
        this.serviceId = serviceId;
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }

    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }

    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }

    public Object getPayload() { return payload; }
    public void setPayload(Object payload) { this.payload = payload; }

    public Instant getProcessedAt() { return processedAt; }
    public void setProcessedAt(Instant processedAt) { this.processedAt = processedAt; }

    public String getServiceId() { return serviceId; }
    public void setServiceId(String serviceId) { this.serviceId = serviceId; }
}
