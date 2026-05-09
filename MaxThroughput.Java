import jakarta.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;
import java.util.Arrays;

public class MaxThroughput {

    public static void main(String[] args) {
        String brokerUrl = "tcp://localhost:61616";
        String queueName = "Lab4Queue";

        byte[] payload = new byte[1024]; // 1 KB
        Arrays.fill(payload, (byte) 1);

        //int[] steps = {100, 200, 400, 800, 1600, 3200, 6400};
        //int[] steps = {12000, 20000 ,25600}; 
        int[] steps = {51200, 102400, 204800};
        int maxThroughput = 0;

        try {
            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
            Connection connection = factory.createConnection();
            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue(queueName);
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT); // producer in presistent mode

            // Also set up a consumer to verify messages are actually received
            MessageConsumer consumer = session.createConsumer(destination);

            // the idea : test on a loop the values of X till one breaks(error>0), then the previous X was the maxThroughput
            for (int X : steps) {
                System.out.println("Testing throughput: " + X + " msg/sec...");

                long period = 1000 / X; // ms between each message
                boolean anyFailed = false;

                // Produce Loop
                // --- Produce X messages at rate X msg/sec ---
                for (int i = 0; i < X; i++) {
                    BytesMessage message = session.createBytesMessage();
                    message.writeBytes(payload);
                    message.setLongProperty("sendTime", System.currentTimeMillis());
                    message.setIntProperty("messageId", i);
                    producer.send(message);

                    // Sleep for period minus 20% to account for thread-switch overhead (T-0.2T)
                    long sleep = (long)(period * 0.8);
                    if (sleep > 0) Thread.sleep(sleep);
                }

                // Consume Loop
                // --- Try to consume all X messages within a timeout ---
                int received = 0;
                for (int i = 0; i < X; i++) {
                    // Timeout
                    Message msg = consumer.receive(100);
                    if (msg != null) {
                        received++;
                    } else {
                        anyFailed = true;
                        break;
                    }
                }

                if (received < X) anyFailed = true;

                if (anyFailed) {
                    System.out.println("FAILED at " + X + " msg/sec (only received " + received + "/" + X + ")");
                    System.out.println(">>> Max Throughput: " + maxThroughput + " msg/sec");
                    break;
                } else {
                    System.out.println("PASSED at " + X + " msg/sec");
                    maxThroughput = X;
                }
            }

            if (maxThroughput == steps[steps.length - 1]) {
                System.out.println(">>> Max Throughput: >= " + maxThroughput + " msg/sec");
            }

            producer.close();
            consumer.close();
            session.close();
            connection.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}