package com.gurshaandeol.orderprocessing.orderservice.repository;

import com.gurshaandeol.orderprocessing.orderservice.model.Order;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface OrderRepository extends MongoRepository<Order, String> {

    /** Returns the number of orders with the given status. */
    long countByStatus(Order.OrderStatus status);
}
