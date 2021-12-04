package br.com.estudo.alura.kafka.core.order.model;

import java.math.BigDecimal;
import java.util.UUID;

public record Order (
        String orderId,
        String userId,
        BigDecimal amount
) {
    public static Order getRandom() {
        var orderId = UUID.randomUUID().toString();
        var userId = UUID.randomUUID().toString();
        BigDecimal amount = new BigDecimal(Math.random() * 5000 + 10);

        return new Order(orderId, userId, amount);
    }
}
