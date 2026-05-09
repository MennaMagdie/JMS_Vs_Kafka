package com.example;

import java.util.*;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.*;

public class Main {
    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // ─────────────────────────────────────────
        // A) RESPONSE TIME
        // Use nanoTime for accuracy (currentTimeMillis is too coarse)
        // ─────────────────────────────────────────
        System.out.println("========== A) PRODUCER RESPONSE TIME ==========");

        List<Long> responseTimes = new ArrayList<>();
 
        for (int i = 0; i < 1000; i++) {
            long sendTime = System.currentTimeMillis();
            String value = sendTime + "|" + "a".repeat(1004); // ~1KB with timestamp
 
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("kafka-exp-topic", "key-" + i, value);
 
            long start = System.nanoTime();
            producer.send(record).get(); // synchronous → accurate timing
            long responseTime = System.nanoTime() - start;
 
            responseTimes.add(responseTime);
        }
 
        Collections.sort(responseTimes);
        long medianNs = responseTimes.get(responseTimes.size() / 2);
        double medianMs = medianNs / 1_000_000.0;
        System.out.printf("Median Producer Response Time: %.3f ms%n", medianMs);

        // ─────────────────────────────────────────
        // B) MAXIMUM THROUGHPUT
        // Start at 100 msg/s, double each round
        // A test fails if:
        //   - any send throws an exception, OR
        //   - actual sent < 90% of target (Kafka couldn't keep up)
        // Report last successful throughput as max
        // ─────────────────────────────────────────
        System.out.println("\n========== B) PRODUCER MAX THROUGHPUT ==========");
 
        long throughput = 100;
        long lastGood   = 0;
        long MAX_CAP    = 1_000_000;
 
        while (throughput <= MAX_CAP) {
            long result = runThroughputTest(producer, throughput);
 
            if (result < 0) {
                // sent less than 90% of target → Kafka couldn't keep up
                System.out.printf("Tested %8d msg/s → FAILED (could not keep up)%n", throughput);
                System.out.println("Max Producer Throughput: " + lastGood + " msg/s");
                break;
            } else if (result > 0) {
                // some sends threw exceptions
                System.out.printf("Tested %8d msg/s → FAILED (%d send errors)%n", throughput, result);
                System.out.println("Max Producer Throughput: " + lastGood + " msg/s");
                break;
            } else {
                // success
                System.out.printf("Tested %8d msg/s → OK%n", throughput);
                lastGood   = throughput;
                throughput *= 2;
            }
        }
 
        if (throughput > MAX_CAP) {
            System.out.println("Max Producer Throughput: > " + MAX_CAP + " msg/s (hit cap)");
        }

        // ─────────────────────────────────────────
        // C) LATENCY (producer side)
        // Send 10,000 messages with embedded timestamp
        // Consumer reads timestamp and computes delay
        // ─────────────────────────────────────────
        System.out.println("\n========== C) PRODUCING FOR LATENCY TEST ==========");

        for (int i = 0; i < 10_000; i++) {
            long sendTime = System.currentTimeMillis();
            String value  = sendTime + "|" + "a".repeat(1004); // "<timestamp>|<padding>"

            ProducerRecord<String, String> record =
                    new ProducerRecord<>("kafka-exp-topic", "lat-" + i, value);

            producer.send(record); // async → real pipeline delay
        }

        producer.flush();
        System.out.println("10,000 messages sent with timestamps.");
        System.out.println("Now run the Consumer to measure median latency.");

        producer.close();
    }

    // ─────────────────────────────────────────
    // Send targetThroughput messages in 1 second
    // Sleep T - 0.2*T between each send

    // Returns:
    //   0   → success
    //  -1   → Kafka couldn't keep up (sent < 90% of target)
    //  >0   → number of send exceptions
    // ─────────────────────────────────────────
    private static long runThroughputTest(KafkaProducer<String, String> producer,
                                          long targetThroughput) throws Exception {
        long periodNanos = 1_000_000_000L / targetThroughput;
        long sleepNanos  = (long)(periodNanos * 0.8); // T - 0.2*T
 
        String value = "a".repeat(1024); // 1KB
        List<Future<RecordMetadata>> futures = new ArrayList<>();
 
        long windowStart = System.nanoTime();
        long windowEnd   = windowStart + 1_000_000_000L; // 1 second window
 
        int i = 0;
        while (System.nanoTime() < windowEnd) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("kafka-exp-topic", "tp-" + i++, value);
            futures.add(producer.send(record)); // async send
 
            long sleepUntil = System.nanoTime() + sleepNanos;
            while (System.nanoTime() < sleepUntil) { /* busy wait */ }
        }
 
        long actualDurationNanos = System.nanoTime() - windowStart;
 
        // count confirmed sends and failures
        long failures = 0;
        long sent     = 0;
        for (Future<RecordMetadata> f : futures) {
            try { f.get(); sent++; }
            catch (Exception e) { failures++; }
        }
 
        long actualThroughput = (long)(sent / (actualDurationNanos / 1_000_000_000.0));
        System.out.printf("  → sent %d / %d targeted | actual: %d msg/s%n",
                sent, targetThroughput, actualThroughput);
 
        // if we sent less than 90% of target, Kafka couldn't keep up
        if (sent < targetThroughput * 0.9) return -1;
 
        return failures;
    }
}