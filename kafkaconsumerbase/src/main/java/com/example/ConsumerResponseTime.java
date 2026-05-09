package com.example;
import org.apache.kafka.clients.consumer.*;
import java.time.Duration;
import java.util.*;

public class ConsumerResponseTime {

public static void main(String[] args) {
        // 1. Initialize the consumer using your base config 
        KafkaConsumer<String, String> consumer = KafkaConsumerBase.createConsumerBase();
        List<Long> responseTimes = new ArrayList<>();
        int totalRuns = 1000; 
        System.out.println("Starting response time test for 1000 runs...");
        
        int totalMessages = 0;
        for (int i = 0; i < totalRuns; i++) {
            long start = System.nanoTime();
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            long responseTime = System.nanoTime() - start;

            totalMessages += records.count();
            responseTimes.add(responseTime); // store in nanoseconds
        }
        System.out.println("Total messages retrieved: " + totalMessages);

        // 2. Calculate the Median 
        Collections.sort(responseTimes);
        double median;
        if (responseTimes.size() % 2 == 0) {
            median = ((double)responseTimes.get(responseTimes.size()/2) + 
                        (double)responseTimes.get(responseTimes.size()/2 - 1)) / 2;
        } else {
            median = (double) responseTimes.get(responseTimes.size()/2);
        }

        System.out.println("--- Results ---");
        System.out.println("Total Runs: " + responseTimes.size());
        System.out.println("Median Response Time: " + median / 1_000_000.0 + " ms");

        consumer.close();
    }
}