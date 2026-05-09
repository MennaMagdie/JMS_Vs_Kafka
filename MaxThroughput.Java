import jakarta.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;

public class MaxThroughput {

    public static void main(String[] args) {
        String brokerUrl = "tcp://localhost:61616";
        String queueName = "Lab4Queue";

        byte[] payload = new byte[1024]; // 1 KB
        Arrays.fill(payload, (byte) 1);

        int[] steps = {100, 200, 400, 800, 1600};
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
                AtomicInteger received = new AtomicInteger(0);
                AtomicBoolean anyFailed = new AtomicBoolean(false);

                // Consumer Thread — starts first, listens while producer sends
                Thread consumerThread = new Thread(() -> {
                    long startTime = System.currentTimeMillis();
                    for (int i = 0; i < X; i++) {
                        try {
                            Message msg = consumer.receive(1000);
                            if (msg != null) {
                                received.incrementAndGet();
                            } else {
                                anyFailed.set(true);
                                break;
                            }
                        } catch (JMSException e) {
                            anyFailed.set(true);
                            break;
                        }
                    }
                });

                // Producer Thread — sends at rate X msg/sec
                Thread producerThread = new Thread(() -> {
                    for (int i = 0; i < X; i++) {
                        try {
                            BytesMessage message = session.createBytesMessage();
                            message.writeBytes(payload);
                            message.setLongProperty("sendTime", System.currentTimeMillis());
                            message.setIntProperty("messageId", i);
                            producer.send(message);

                            // Sleep for period minus 20% to account for thread-switch overhead (T-0.2T)
                            long sleep = (long)(period * 0.8);
                            if (sleep > 0) Thread.sleep(sleep);
                        } catch (Exception e) {
                            anyFailed.set(true);
                        }
                    }
                });

                // Start consumer first so it's ready before producer sends
                consumerThread.start();
                producerThread.start();

                // Wait for both to finish
                producerThread.join();
                consumerThread.join();

                if (anyFailed.get() || received.get() < X) {
                    System.out.println("FAILED at " + X + " msg/sec (only received " + received.get() + "/" + X + ")");
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
        }catch(Exception e){
            // ...
        }
    }
}