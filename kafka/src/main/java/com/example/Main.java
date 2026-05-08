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

//         // ─────────────────────────────────────────
//         // A) RESPONSE TIME
//         // Use nanoTime for accuracy (currentTimeMillis is too coarse)
//         // ─────────────────────────────────────────
//         System.out.println("A) PRODUCER RESPONSE TIME");

//         List<Long> responseTimes = new ArrayList<>();
//         for (int i = 0; i < 1000; i++) {
//             // Requirement: 1KB messages [cite: 20, 49]
//             String value = "a".repeat(1024); 
//             ProducerRecord<String, String> record = new ProducerRecord<>("kafka-exp-topic", "key-" + i, value);

//             long start = System.currentTimeMillis(); // Requirement: Measure produce API call [cite: 43]
//             producer.send(record).get();            // .get() forces synchronous wait to measure actual response
//             long responseTime = System.currentTimeMillis() - start;

//             responseTimes.add(responseTime);
// }
//         Collections.sort(responseTimes);
//         long medianNs = responseTimes.get(responseTimes.size() / 2);
//         double medianMs = medianNs / 1_000_000.0;    // convert ns → ms
//         System.out.printf("Median Producer Response Time: %.3f ms%n", medianMs);

//         // ─────────────────────────────────────────
//         // B) MAXIMUM THROUGHPUT
//         // Use long to avoid int overflow
//         // Cap at 1,000,000 msg/s (realistic limit)
//         // ─────────────────────────────────────────
//         System.out.println("\nB) PRODUCER MAX THROUGHPUT");

//         long throughput = 100;        // ← long not int (fixes overflow)
//         long lastGood   = 0;
//         long MAX_CAP    = 1_000_000;  // stop at 1M msg/s

//         while (throughput <= MAX_CAP) {
//             int failures = runThroughputTest(producer, throughput);
//             System.out.printf("Tested %8d msg/s → %s%n",
//                     throughput,
//                     failures == 0 ? "OK" : "FAILED (" + failures + " failures)");

//             if (failures > 0) {
//                 System.out.println("Max Producer Throughput: " + lastGood + " msg/s");
//                 break;
//             }

//             lastGood   = throughput;
//             throughput *= 2;
//         }

//         if (throughput > MAX_CAP) {
//             System.out.println("Max Producer Throughput: > " + MAX_CAP + " msg/s");
//         }

        // ─────────────────────────────────────────
        // C) LATENCY (producer side)
        // Send 10,000 messages with embedded timestamp
        // Consumer reads timestamp and computes delay
        // ─────────────────────────────────────────
        System.out.println("\nC) PRODUCING FOR LATENCY TEST");

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

    // // ─────────────────────────────────────────
    // // Send targetThroughput messages in 1 second
    // // Sleep T - 0.2*T between each send
    // // Returns number of failed sends
    // // ─────────────────────────────────────────
    // private static int runThroughputTest(KafkaProducer<String, String> producer, long targetThroughput) throws Exception {
    //     long periodNanos = 1_000_000_000L / targetThroughput; // safe: long division
    //     long sleepNanos  = (long)(periodNanos * 0.8);          // T - 0.2*T

    //     String value = "a".repeat(1024);
    //     List<Future<RecordMetadata>> futures = new ArrayList<>();

    //     long windowEnd = System.nanoTime() + 1_000_000_000L;  // 1 second window

    //     int i = 0;
    //     while (System.nanoTime() < windowEnd) {
    //         ProducerRecord<String, String> record =
    //                 new ProducerRecord<>("kafka-exp-topic", "tp-" + i++, value);
    //         futures.add(producer.send(record));

    //         long sleepUntil = System.nanoTime() + sleepNanos;
    //         while (System.nanoTime() < sleepUntil) { /* busy wait */ }
    //     }

    //     int failures = 0;
    //     for (Future<RecordMetadata> f : futures) {
    //         try { f.get(); }
    //         catch (Exception e) { failures++; }
    //     }
    //     return failures;
    // }
}