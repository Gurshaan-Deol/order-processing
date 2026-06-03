package com.gurshaandeol.orderprocessing.paymentservice.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

@Document(collection = "dead_letter")
public class DeadLetterEntry {

    @Id
    private String id;
    private String originalEventId;
    private String eventType;
    private Object payload;
    private String errorMessage;
    private Instant failedAt;
    private int retryCount;

    public DeadLetterEntry() {}

    public DeadLetterEntry(String id, String originalEventId, String eventType,
                           Object payload, String errorMessage, Instant failedAt, int retryCount) {
        this.id = id;
        this.originalEventId = originalEventId;
        this.eventType = eventType;
        this.payload = payload;
        this.errorMessage = errorMessage;
        this.failedAt = failedAt;
        this.retryCount = retryCount;
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getOriginalEventId() { return originalEventId; }
    public void setOriginalEventId(String originalEventId) { this.originalEventId = originalEventId; }

    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }

    public Object getPayload() { return payload; }
    public void setPayload(Object payload) { this.payload = payload; }

    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }

    public Instant getFailedAt() { return failedAt; }
    public void setFailedAt(Instant failedAt) { this.failedAt = failedAt; }

    public int getRetryCount() { return retryCount; }
    public void setRetryCount(int retryCount) { this.retryCount = retryCount; }
}
