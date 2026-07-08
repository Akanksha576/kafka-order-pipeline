package com.akanksha.kafkapipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.math.BigDecimal;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

/**
 * Simulates a stream of retail order events and publishes them to a Kafka topic.
 * Think of this as standing in for a checkout service that fires an event
 * every time a customer completes an order.
 */
public class OrderProducer {

    private static final String TOPIC = "orders";
    private static final List<String> PRODUCTS = List.of(
            "Laptop", "Headphones", "Backpack", "Water Bottle", "Desk Lamp", "Monitor", "Keyboard"
    );
    private static final List<String> CUSTOMERS = List.of(
            "cust-101", "cust-102", "cust-103", "cust-104", "cust-105"
    );

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // going with acks=all so we don't silently lose orders if a broker drops
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        ObjectMapper mapper = new ObjectMapper();
        Random random = new Random();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 50; i++) {
                String customerId = CUSTOMERS.get(random.nextInt(CUSTOMERS.size()));
                Order order = new Order(
                        UUID.randomUUID().toString(),
                        customerId,
                        PRODUCTS.get(random.nextInt(PRODUCTS.size())),
                        1 + random.nextInt(3),
                        BigDecimal.valueOf(10 + random.nextInt(200)),
                        System.currentTimeMillis()
                );

                String json = mapper.writeValueAsString(order);

                // keying by customerId so one customer's orders stay in order
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, customerId, json);

                producer.send(record, (RecordMetadata metadata, Exception exception) -> {
                    if (exception != null) {
                        System.err.println("Failed to send order: " + exception.getMessage());
                    } else {
                        System.out.printf("Sent order %s -> partition %d, offset %d%n",
                                order.getOrderId(), metadata.partition(), metadata.offset());
                    }
                });

                Thread.sleep(300); // slow it down a bit, otherwise it dumps everything instantly
            }
            producer.flush();
        }
        System.out.println("Finished producing orders.");
    }
}
