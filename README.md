# Real-Time Order Processing Pipeline (Kafka + Java)

Simulates an e-commerce checkout stream: a producer publishes order events to
Kafka, and a consumer processes them in real time to keep a running revenue
total per product — similar to what would sit behind a live sales dashboard.

## Stack
Java 17, Apache Kafka (via Docker), Maven, Jackson (JSON serialization)

## How to run it locally

1. **Start Kafka** (Zookeeper + broker + a web UI to see messages):
   ```bash
   docker-compose up -d
   ```
   Check it's running: open http://localhost:8080 (Kafka UI).

2. **Build the project:**
   ```bash
   mvn clean package
   ```

3. **Run the consumer first** (in one terminal), so it's listening before
   messages arrive:
   ```bash
   mvn exec:java -Dexec.mainClass="com.akanksha.kafkapipeline.OrderConsumer"
   ```

4. **Run the producer** (in a second terminal) to start generating orders:
   ```bash
   mvn exec:java -Dexec.mainClass="com.akanksha.kafkapipeline.OrderProducer"
   ```

You'll see the producer logging each order it sends with its partition/offset,
and the consumer logging each order it reads along with the running revenue
total per product.

5. **Tear down:**
   ```bash
   docker-compose down
   ```

## What's actually happening

- `Order.java` — the event schema (order id, customer, product, qty, price, timestamp).
- `OrderProducer.java` — generates 50 fake orders and publishes them to the
  `orders` topic, keyed by `customerId`.
- `OrderConsumer.java` — subscribes to `orders`, deserializes each message,
  and maintains an in-memory running total of revenue per product.

## Design decisions worth knowing cold for an interview

- **Keyed by `customerId`** — Kafka guarantees all messages with the same key
  go to the same partition, in order. So one customer's orders are always
  processed in sequence, even though different customers' orders can be
  processed in parallel across partitions.
- **`acks=all` on the producer** — the producer waits for all in-sync
  replicas to confirm a write before treating it as successful. Safer against
  data loss, at the cost of a bit of latency. Worth contrasting with
  `acks=1` (leader only) or `acks=0` (fire and forget).
- **Consumer group (`order-analytics-group`)** — if you ran a second instance
  of `OrderConsumer` with the same group id, Kafka would automatically split
  the topic's partitions between the two, giving parallel processing with no
  code changes. A different group id would instead get its own independent
  copy of the whole stream.
- **`auto.offset.reset=earliest`** — controls what happens the first time this
  consumer group runs, when it has no committed offset yet: start from the
  beginning of the topic rather than only new messages.
- **JSON over raw bytes** — trades a bit of message size/parsing overhead for
  human-readable, schema-flexible messages. In a production system at scale
  I'd likely move to Avro + a schema registry to enforce schema compatibility
  and cut payload size.
