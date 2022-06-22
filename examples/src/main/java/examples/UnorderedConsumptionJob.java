package examples;

import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.url.DataURLStreamHandler;
import org.apache.pulsar.client.impl.WeightedConsumerBuilder;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;
import org.apache.pulsar.client.impl.weight.WeightedConsumerConfiguration;
import org.apache.pulsar.shade.com.google.gson.Gson;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class UnorderedConsumptionJob implements Runnable {
    public static void main(String[] args) {
        UnorderedConsumptionJob job = new UnorderedConsumptionJob(4, 3600);
        job.run();
    }

    /**
     * Configure number of workers you want for processing messages
     *
     * UNORDERED: Parallelism will be determined by this knob and can be greater than number of partitions
     *
     * ORDERED: If multiple consumers are run with same subscription, they will divide partitions among themselves
     * You can simulate this by running multiple instances of ConsumeDriver concurrently. When you are looking for parallel
     * consumption within same consumer, limit number of workers here to max(partitions/consumers, 1)
     */
    final int workers;
    final int jobDurationSeconds;
    final ExecutorService executor;
    AtomicBoolean terminator = new AtomicBoolean(false);

    String pulsarUrl;
    SubscriptionType subscriptionType;
    String subName;
    Map<String, Integer> topicWeights;

    public UnorderedConsumptionJob(int workers, int jobDurationSeconds) {
        this.workers = workers;
        this.jobDurationSeconds = jobDurationSeconds;
        this.executor = Executors.newFixedThreadPool(workers + 1);
    }

    protected void configure() {
        pulsarUrl = "";
        subscriptionType = SubscriptionType.Shared;
        subName = "sub-a";
        topicWeights = new HashMap<String, Integer>() {{
            put("persistent://public/default/ankur-hello-weight-1", null);
            put("persistent://public/default/ankur-hello-weight-2", null);
            put("persistent://public/default/ankur-hello-weight-3", 5);
        }};
    }

    @Override
    public void run() {
        configure();

        try {
            CompletableFuture<Void> job = CompletableFuture.runAsync(() -> {
                AtomicLong successCtr = new AtomicLong(0), failureCtr = new AtomicLong(0);
                List<CompletableFuture<Void>> tasks = new ArrayList<>();

                try (PulsarClient client = createPulsarClient();
                     Consumer<byte[]> consumer = createPriorityConsumer(client)) {
                    try {
                        for (int i = 0; i < workers; i++) {
                            tasks.add(CompletableFuture.runAsync(() -> {
                                try {
                                    consumeTask(consumer, successCtr, failureCtr);
                                } catch (PulsarClientException ex) {
                                    throw new RuntimeException(ex);
                                }
                            }, executor));
                        }
                        awaitCompletion(tasks);
                    } finally {
                        StatPrinter.print(consumer);
                    }
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                } finally {
                    System.out.printf("TOTAL successfully consumed=%d, failures=%d%n", successCtr.get(), failureCtr.get());
                }
            }, executor);

            Thread.sleep(jobDurationSeconds * 1000);
            terminator.set(true);
            job.get();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        } finally {
            executor.shutdown();
        }
    }

    protected PulsarClient createPulsarClient() throws PulsarClientException {
        return PulsarClient.builder()
                .serviceUrl(pulsarUrl)
                .statsInterval(StatPrinter.statIntervalSecs, TimeUnit.SECONDS)
                .build();
    }

    protected Consumer<byte[]> createPriorityConsumer(PulsarClient client) throws PulsarClientException {
        WeightedConsumerBuilder<byte[]> consumerBuilder = new WeightedConsumerBuilder<>(client, Schema.BYTES)
                .distributionStrategy(WeightedConsumerConfiguration.DistributionStrategy.EXPONENTIAL)
                .topics(topicWeights);
        return consumerBuilder
                .subscriptionName(subName)
                .subscriptionType(subscriptionType)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
    }

    protected <T> String processMessage(Message<T> message) throws RuntimeException {
        String payload = new String(message.getData());
        try {
            Thread.sleep(5); //simulate delay introduced because of actual message processing
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
        //System.out.println("Processed|" + message.getTopicName() + "|" + message.getMessageId() + "|" + payload);
        return payload;
    }

    private <T> void consumeTask(Consumer<T> consumer, AtomicLong successCtr, AtomicLong failureCtr) throws PulsarClientException {
        long success = 0, failures = 0;
        try {
            while (!terminator.get()) {
                Message<T> message = consumer.receive(1, TimeUnit.SECONDS);
                if (message != null) {
                    try {
                        processMessage(message);
                        consumer.acknowledgeAsync(message);
                        success++;
                    } catch (Exception ex) {
                        failures++;
                        consumer.negativeAcknowledge(message);
                    }
                }
            }
        } finally {
            System.out.printf("[%s] Consumed %d messages successfully and observed %d failures%n", Thread.currentThread().getName(), success, failures);
            if (successCtr != null ) {
                successCtr.addAndGet(success);
            }
            if (failureCtr != null) {
                failureCtr.addAndGet(failures);
            }
        }
    }

    /**
     * Waits for all jobs to finish, throws exception with errors collated across jobs
     * Not using {@link CompletableFuture#allOf(CompletableFuture[])} because it throws error as soon as one job fails
     */
    private static void awaitCompletion(List<CompletableFuture<Void>> jobs) throws InterruptedException, ExecutionException {
        AtomicBoolean errored = new AtomicBoolean(false);
        StringBuffer errorBuilder =  new StringBuffer("Errors:\n");
        for(CompletableFuture<Void> job: jobs) {
            job.handle((v, e) -> {
                if(e != null) {
                    errored.set(true);
                    errorBuilder.append(e.getMessage() + "\n");
                }
                return v;
            }).get();
        }
        if(errored.get()) {
            throw new CompletionException(new RuntimeException(errorBuilder.toString()));
        }
    }
}
