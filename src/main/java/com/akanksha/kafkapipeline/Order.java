package com.akanksha.kafkapipeline;

import java.math.BigDecimal;

/**
 * Simple POJO representing one retail order event.
 * This is the "message" that gets serialized to JSON and sent through Kafka.
 */
public class Order {
    private String orderId;
    private String customerId;
    private String product;
    private int quantity;
    private BigDecimal price;
    private long timestamp;

    // Needed by Jackson to build objects back from JSON
    public Order() {}

    public Order(String orderId, String customerId, String product, int quantity, BigDecimal price, long timestamp) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.product = product;
        this.quantity = quantity;
        this.price = price;
        this.timestamp = timestamp;
    }

    public String getOrderId() { return orderId; }
    public String getCustomerId() { return customerId; }
    public String getProduct() { return product; }
    public int getQuantity() { return quantity; }
    public BigDecimal getPrice() { return price; }
    public long getTimestamp() { return timestamp; }

    public void setOrderId(String orderId) { this.orderId = orderId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }
    public void setProduct(String product) { this.product = product; }
    public void setQuantity(int quantity) { this.quantity = quantity; }
    public void setPrice(BigDecimal price) { this.price = price; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    public BigDecimal getTotal() {
        return price.multiply(BigDecimal.valueOf(quantity));
    }

    @Override
    public String toString() {
        return "Order{orderId='" + orderId + "', customerId='" + customerId +
                "', product='" + product + "', quantity=" + quantity +
                ", price=" + price + ", timestamp=" + timestamp + "}";
    }
}
