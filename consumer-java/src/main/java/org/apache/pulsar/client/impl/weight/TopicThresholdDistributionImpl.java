package org.apache.pulsar.client.impl.weight;

import org.apache.pulsar.common.naming.TopicName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class TopicThresholdDistributionImpl implements TopicThresholdDistribution {
    private static final Logger log = LoggerFactory.getLogger(TopicThresholdDistributionImpl.class);
    private final WeightDistribution distribution;
    private final Map<String, Integer> topicWeights;

    private TopicThresholdDistributionImpl(Map<String, Integer> topicWeights, WeightDistribution distribution) {
        this.topicWeights = topicWeights;
        this.distribution = distribution;
    }

    public int getWeight(String topic) {
        TopicName topicName = TopicName.get(topic);
        Integer weight = topicWeights.get(topic);

        if (weight == null && topicName.isPartitioned()) {
            weight = topicWeights.get(topicName.getPartitionedTopicName());
        }
        if (weight == null) {
            log.warn("Weight not found for topic={}, default to weight=1", topic);
            weight = 1;
        }
        return weight;
    }

    public int getMinValue() {
        return distribution.getMinValue();
    }

    public int getMaxValue() {
        return distribution.getMaxValue();
    }

    public int getValue(String topic) {
        int bound = distribution.getValue(getWeight(topic));
        log.debug("Topic threshold for topic={} is {}", topic, bound);
        return bound;
    }

    public static <T> TopicThresholdDistribution loadFromConf(WeightedConsumerConfiguration conf)
            throws IllegalArgumentException {
        int maxWeight = 1;
        for (Map.Entry<String, Integer> entry : conf.getTopicWeights().entrySet()) {
            maxWeight = Math.max(maxWeight, entry.getValue());
        }
        WeightDistribution weightDistribution;
        switch (conf.getDistributionStrategy()) {
            case LINEAR:
                weightDistribution = new LinearWeightDistribution(conf.getMinBound(), conf.getMaxBound(), maxWeight);
                break;
            case EXPONENTIAL:
                weightDistribution = new ExponentialWeightDistribution(conf.getMinBound(), conf.getMaxBound(), maxWeight);
                break;
            default:
                throw new IllegalArgumentException("Weight strategy " + conf.getDistributionStrategy() + " is not supported");
        }

        return new TopicThresholdDistributionImpl(conf.getTopicWeights(), weightDistribution);
    }
}
