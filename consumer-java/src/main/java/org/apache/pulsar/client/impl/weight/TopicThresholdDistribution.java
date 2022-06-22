package org.apache.pulsar.client.impl.weight;

public interface TopicThresholdDistribution {
    int getWeight(String topic);
    int getMinValue();
    int getMaxValue();
    int getValue(String topic);
}
