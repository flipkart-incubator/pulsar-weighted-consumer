package examples;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerStats;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.MultiTopicsConsumerImpl;

import java.text.DecimalFormat;
import java.time.Duration;
import java.util.List;

public class StatPrinter {
    public static DecimalFormat THROUGHPUT_FORMAT = new DecimalFormat("0.00");
    public static int statIntervalSecs = 5;

    public static <T> void print(Consumer<T> consumer) {
        try {
            //Stats are calculated at fixed intervals in pulsar clients. Waiting to ensure latest aggregation is completed.
            Thread.sleep(Duration.ofSeconds(statIntervalSecs + 5).toMillis());
            if (consumer instanceof ConsumerImpl) {
                print((ConsumerImpl<T>) consumer);
            } else if (consumer instanceof MultiTopicsConsumerImpl) {
                print((MultiTopicsConsumerImpl<T>) consumer);
            }
        } catch (InterruptedException ex) {
            //swallow and exit
        }
    }

    private static <T> void print(ConsumerImpl<T> consumer) {
        System.out.println("Single Consumer stats\n-----");
        System.out.printf("[%s] [%s] [%s]%n",
                consumer.getTopic(), consumer.getSubscription(), consumer.getConsumerName());
        print(consumer.getStats());
    }

    private static <T> void print(MultiTopicsConsumerImpl<T> consumer) {
        System.out.println("Multi Consumer stats\n-----");
        System.out.printf("Internal-consumers=[%s] [%s] [%s]%n",
                consumer.getConsumers().size(), consumer.getSubscription(), consumer.getConsumerName());
        print(consumer.getStats());
        List<ConsumerImpl<T>> consumers = consumer.getConsumers();
        consumers.forEach(StatPrinter::print);
    }

    private static void print(ConsumerStats stats) {
        System.out.printf("Consume Total: %d msgs --- %d bytes --- Pending prefetched messages: %d%n",
                stats.getTotalMsgsReceived(), stats.getTotalBytesReceived(), stats.getMsgNumInReceiverQueue());
    }
}
