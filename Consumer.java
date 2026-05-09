
import jakarta.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;
import java.util.Arrays;

// 1. reponseTime  = time to receive
// 2. end-to-end latency = time of receive - sendTime
public class Consumer {

    private static final int NUM_MESSAGES = 10000;

    public static void main(String[] args) {
        String brokerUrl = "tcp://localhost:61616";
        String queueName = "Lab4Queue";

        try {
            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
            Connection connection = factory.createConnection();
            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue(queueName);
            MessageConsumer consumer = session.createConsumer(destination);

            long[] responseTimes = new long[NUM_MESSAGES];  // for responseTime Latency  computation
            long[] endToEndLatencies = new long[NUM_MESSAGES];  // for endToEnd Latency computation

            System.out.println("consuming " + NUM_MESSAGES + " messages...");

            for (int i = 0; i < NUM_MESSAGES; i++) {
                long startTime = System.currentTimeMillis();
                Message message = consumer.receive(); // blocks until a message arrives

                long receiveTime = System.currentTimeMillis();
                responseTimes[i] = receiveTime - startTime;

                long sendTime = message.getLongProperty("sendTime");
                endToEndLatencies[i] = receiveTime - sendTime;

                if (message == null) {
                    System.err.println("Warning: received null message at index " + i);
                }
            }

            Arrays.sort(responseTimes);
            long medianResponseTime = responseTimes[NUM_MESSAGES / 2];  // 50th percentile

            Arrays.sort(endToEndLatencies);
            long medialLatency = endToEndLatencies[NUM_MESSAGES / 2];

            System.out.println("Success! " + NUM_MESSAGES + " messages consumed.");
            System.out.println("Median Consume Response Time: " + medianResponseTime + " ms");
            System.out.println("Median End To End Latency: " + medialLatency + " ms");

            consumer.close();
            session.close();
            connection.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
