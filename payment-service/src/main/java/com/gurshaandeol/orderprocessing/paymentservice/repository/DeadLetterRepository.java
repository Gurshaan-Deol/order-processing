package com.gurshaandeol.orderprocessing.paymentservice.repository;

import com.gurshaandeol.orderprocessing.paymentservice.model.DeadLetterEntry;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface DeadLetterRepository extends MongoRepository<DeadLetterEntry, String> {
}
