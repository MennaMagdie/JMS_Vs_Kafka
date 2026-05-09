import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ConsumerLatencyTest {

    public static void main(String[] args) {
        // Use the base class you provided
        KafkaConsumer<String, String> consumer = KafkaConsumerBase.createConsumerBase();
        
        List<Long> latencies = new ArrayList<>();
        int targetMessages = 10000;
        int totalReceived = 0;

        System.out.println("CONSUMER LATENCY TEST: Waiting for 10,000 messages...");

     
        while (totalReceived < targetMessages) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            
            long currentTime = System.currentTimeMillis();

            for (ConsumerRecord<String, String> record : records) {
                String value = record.value();
                
                // Split the string by the pipe character '|'
                String[] parts = value.split("\\|");
                if (parts.length > 0) {
                    try {
                        long producerTimestamp = Long.parseLong(parts[0]);
                        long latency = currentTime - producerTimestamp;
                        latencies.add(latency);
                        totalReceived++;
                    } catch (NumberFormatException e) {
                        // Skip if message format is unexpected (like from other tests)
                    }
                }
                
                if (totalReceived >= targetMessages) break;
            }
            
            if (records.isEmpty()) {
                System.out.println("Still waiting... Current count: " + totalReceived);
            }
        }

        // Calculate the Median
        Collections.sort(latencies);
        double medianLatency;
        int size = latencies.size();
        if (size % 2 == 0) {
            medianLatency = (double) (latencies.get(size/2) + latencies.get(size/2 - 1)) / 2;
        } else {
            medianLatency = (double) latencies.get(size/2);
        }

        System.out.println("\n--- LATENCY RESULTS ---");
        System.out.println("Messages Processed: " + size);
        System.out.println("Median Latency: " + medianLatency + " ms");

    
        consumer.close();
    }
}