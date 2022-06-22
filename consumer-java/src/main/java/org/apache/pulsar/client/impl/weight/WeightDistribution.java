package org.apache.pulsar.client.impl.weight;

public interface WeightDistribution {
    String getName();
    int getValue(int weight);
    int getMinValue();
    int getMaxValue();
}
