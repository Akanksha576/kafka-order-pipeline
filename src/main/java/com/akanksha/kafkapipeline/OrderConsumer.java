package com.akanksha.kafkapipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Consumes order events from the "orders" topic and keeps a running,
 * real-time revenue total per product — like a live sales dashboard
 * would need behind the scenes.
 */
public class OrderConsumer {

    private static final String TOPIC = "orders";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "order-analytics-group");

        // start from beginning if there's no committed offset yet (first run)
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // TODO: switch to manual commit once I add retry handling below,
        // don't want to ack a message before it's actually processed
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        ObjectMapper mapper = new ObjectMapper();
        Map<String, BigDecimal> revenueByProduct = new HashMap<>();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(TOPIC));
            System.out.println("Listening for orders... (Ctrl+C to stop)");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        Order order = mapper.readValue(record.value(), Order.class);

                        revenueByProduct.merge(order.getProduct(), order.getTotal(), BigDecimal::add);

                        System.out.printf(
                                "Consumed [partition %d, offset %d] customer=%s product=%s total=$%s | running total for %s = $%s%n",
                                record.partition(), record.offset(),
                                order.getCustomerId(), order.getProduct(), order.getTotal(),
                                order.getProduct(), revenueByProduct.get(order.getProduct())
                        );
                    } catch (Exception e) {
                        // not handling malformed messages properly yet, just skipping for now
                        System.err.println("Skipping unreadable message: " + e.getMessage());
                    }
                }
            }
        }
    }
}
