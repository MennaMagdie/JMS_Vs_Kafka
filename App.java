
import jakarta.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;
import java.util.Arrays;

public class App {

    private static final int NUM_MESSAGES = 1000;

    public static void main(String[] args) {
        String brokerUrl = "tcp://localhost:61616";
        String queueName = "Lab4Queue";

        try {
            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
            Connection connection = factory.createConnection();
            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue(queueName);
            MessageProducer producer = session.createProducer(destination);

            //note in readme (maybe we can try/report both as well)
            // producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);

            byte[] payload = new byte[1024]; // 1kb as required
            Arrays.fill(payload, (byte) 1);

            long[] responseTimes = new long[NUM_MESSAGES];
            // System.out.println("producing 1000 messages...");
            System.out.println("producing " + NUM_MESSAGES + " messages...");

            System.out.println("format used: 1KB BytesMessage with properties {sendTime, messageId}");

            for (int i = 0; i < NUM_MESSAGES; i++) {
                BytesMessage message = session.createBytesMessage();
                message.writeBytes(payload);

                // long start = System.currentTimeMillis();
                // producer.send(message);
                // responseTimes[i] = System.currentTimeMillis() - start;
                long sendTime = System.currentTimeMillis();
                message.setLongProperty("sendTime", sendTime);
                message.setIntProperty("messageId", i);

                long start = sendTime;
                producer.send(message);
                responseTimes[i] = System.currentTimeMillis() - start;
            }

            Arrays.sort(responseTimes);
            // long median = responseTimes[500];
            long median = responseTimes[NUM_MESSAGES / 2];
            System.out.println("Success! 1000 messages sent.");
            System.out.println("Median Produce Response Time: " + median + " ms");

            producer.close();
            session.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
