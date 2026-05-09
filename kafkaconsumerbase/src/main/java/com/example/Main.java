package com.example;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class Main {
    public static void main(String[] args) {
        var consumer = KafkaConsumerBase.createConsumerBase();

        while (true) {
            var records = consumer.poll(java.time.Duration.ofMillis(100));

            records.forEach(record -> {
                System.out.println("Received: " + record.value());
            });
        }
    }    
}
