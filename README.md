# Real-Time Order Processing Pipeline

I built this to get hands-on with Kafka since I'd only used it at a high level before (through PySpark pipelines at my last job). Wanted to actually build the producer/consumer side myself instead of just consuming from an existing stream.

The idea: pretend we're an e-commerce checkout system. Every time a customer "orders" something, that event gets published to Kafka. On the other end, a consumer picks it up and keeps a running total of revenue per product, live, instead of waiting for a batch job to run.

## Built with

Java 17, Kafka (running locally in Docker), Maven, Jackson for JSON

## Running it

You'll need Docker and Maven installed.

First, spin up Kafka:

docker-compose up -d

There's a UI at localhost:8080 if you want to actually see the topic and messages instead of just trusting it's working.

Build it:

mvn clean package

I usually start the consumer first so it's already listening when orders start coming in:

mvn exec:java -Dexec.mainClass="com.akanksha.kafkapipeline.OrderConsumer"

Then in a separate terminal, kick off the producer:

mvn exec:java -Dexec.mainClass="com.akanksha.kafkapipeline.OrderProducer"

It sends 50 fake orders with a short delay between each so you can actually watch it happen instead of it dumping everything at once. The consumer terminal will show each order coming in along with the updated running total for that product.

When you're done:

docker-compose down

## Files

- Order.java - just the data model, what an order looks like (id, customer, product, qty, price, timestamp)
- OrderProducer.java - generates the fake orders and sends them to the orders topic
- OrderConsumer.java - reads from orders and keeps a running revenue count per product in memory
