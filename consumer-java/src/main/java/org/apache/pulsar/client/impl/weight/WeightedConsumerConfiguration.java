/*
 *
 *  * Copyright 2022-2026, the original author or authors.
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.pulsar.client.impl.weight;


import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.shade.com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

public class WeightedConsumerConfiguration {
    public static String WT_CONF_PREFIX = "WT_";
    //Different prefix for topics to prevent conflict, refer to #populateBuilder for usage
    public static String WT_TOPIC_CONF_PREFIX = "WTP_";

    private DistributionStrategy distributionStrategy = DistributionStrategy.LINEAR;
    private int minBound = 100;
    private int maxBound = 1000;
    private Map<String, Integer> topicWeights = new HashMap<>();
    private boolean throttleReceiveQueue = false;
    private int queueResumeThreshold = 0;
    private int maxWeightAllowed = 100;

    public void setDistributionStrategy(DistributionStrategy distributionStrategy) {
        this.distributionStrategy = distributionStrategy;
    }

    public void setMinBound(int minBound) {
        this.minBound = minBound;
    }

    public void setMaxBound(int maxBound) {
        this.maxBound = maxBound;
    }

    public void addTopic(String topic, Integer weight) {
        Preconditions.checkNotNull(topic);
        if(weight == null) {
            weight = parseWeightFromTopic(topic);
            if(weight == null) {
                weight = 1;
            }
        }
        this.topicWeights.put(topic, weight);
    }

    /**
     * In situations where local message processing is near instantaneous, user will not see any weighted consumption
     * across different topics irrespective of the weights assigned. In a way this can be desired behaviour to not
     * penalize low weighted topics if their message processing is fast enough (or faster than the network fetch).
     *
     * But in specific circumstances, you might want to enforce consumption to happen in a weighted manner and
     * artificially limit the messages to be polled from lower weighted topics. This flag does that at the cost of loss
     * of capability to make optimal use of local resources for maximum throughput.
     *
     * Set this to true only if you are clear with the impact of the flag
     *
     * @param shouldThrottle
     */
    public void setThrottleReceiveQueue(boolean shouldThrottle) {
        this.throttleReceiveQueue = shouldThrottle;
    }

    /**
     * Caution! One should never need to change this conf in normal circumstances.
     * Default queue resume threshold is zero which should provide the fairest weighted consumption
     * Changing this threshold to a positive value can result in lower weighted consumers not adhering to the distribution
     * Made configurable for debugging purposes
     * Applied bounds are [0, {@link #minBound}]
     *
     * @return
     */
    public void setQueueResumeThreshold(int queueResumeThreshold) {
        this.queueResumeThreshold = queueResumeThreshold;
    }

    public void setMaxWeightAllowed(int maxWeight) {
        this.maxWeightAllowed = maxWeight;
    }

    public DistributionStrategy getDistributionStrategy() {
        return distributionStrategy;
    }

    public int getMinBound() {
        return minBound;
    }

    public int getMaxBound() {
        return maxBound;
    }

    public Map<String, Integer> getTopicWeights() {
        return Collections.unmodifiableMap(topicWeights);
    }

    public boolean isThrottleReceiveQueue() {
        return throttleReceiveQueue;
    }

    public int getQueueResumeThreshold() {
        return queueResumeThreshold;
    }

    public int getMaxWeightAllowed() {
        return maxWeightAllowed;
    }

    public String getSubscriptionName(ConsumerConfigurationData conf){
        String subscriptionName=conf.getSubscriptionName();
        subscriptionName.replace("/", "_");
        return subscriptionName;
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append("Weight configuration:");
        str.append(" dist_strategy=").append(distributionStrategy);
        str.append(" min_bound=").append(minBound);
        str.append(" max_bound=").append(maxBound);
        str.append(" max_weight_allowed=").append(maxWeightAllowed);
        str.append(" throttle_recv_queue=").append(throttleReceiveQueue);
        str.append(" queue_resume_threshold=").append(queueResumeThreshold);
        str.append(" topic_weights=[");
        topicWeights.forEach((topic, weight) -> str.append(" {").append(topic).append(", ").append(weight).append("}"));
        str.append(" ]");
        return str.toString();
    }

    private void validate() {
        Preconditions.checkArgument(maxWeightAllowed >= 1, "max weight allowed should be equal or more than 1");
        Preconditions.checkArgument(minBound >= 100, "min bound should be at least 100");
        Preconditions.checkArgument(maxBound >= minBound, "max bound should be at least as much as min bound %s", minBound);
        Preconditions.checkArgument(queueResumeThreshold >= 0 && queueResumeThreshold <= minBound, "queue resume threshold should be in the range [0,minBound(%s)]", minBound);
        if(topicWeights.size() > 0) {
            for (Map.Entry<String, Integer> entry : topicWeights.entrySet()) {
                Integer weight = entry.getValue();
                Preconditions.checkNotNull(weight);
                Preconditions.checkArgument(weight >= 1 && weight <= maxWeightAllowed,
                        "non-null weights should be in the range [1,maxWeightAllowed(%s)], found %s for topic %s",
                        maxWeightAllowed, weight.toString(), entry.getKey());
            }
        }
    }

    public <T> void populateBuilder(ConsumerBuilder<T> builder) {
        validate();
        addProp(builder, "DIST_STRATEGY", distributionStrategy);
        addProp(builder, "MIN_BOUND", minBound);
        addProp(builder, "MAX_BOUND", maxBound);
        addProp(builder, "THROTTLE_RQ", throttleReceiveQueue);
        addProp(builder, "RESUME_THRESHOLD", queueResumeThreshold);
        addProp(builder, "MAX_WT_ALLOWED", maxWeightAllowed);
        topicWeights.forEach((topic, weight) -> populateTopicInBuilder(builder, topic, weight));
    }

    public static <T> void populateTopicInBuilder(ConsumerBuilder<T> builder, String topic, Integer weight) {
        builder.topic(topic);
        builder.property(WT_TOPIC_CONF_PREFIX + topic, weight == null ? "" : weight.toString());
    }

    public static <T> WeightedConsumerConfiguration loadFromConf(ConsumerConfigurationData<T> conf) {
        WeightedConsumerConfiguration weightConf = new WeightedConsumerConfiguration();
        weightConf.setDistributionStrategy(DistributionStrategy.valueOf(readPropOrDefault(conf, "DIST_STRATEGY", DistributionStrategy.LINEAR.name())));
        weightConf.setMinBound(parseConfAsIntOrDefault(conf, "MIN_BOUND", 100));
        weightConf.setMaxBound(parseConfAsIntOrDefault(conf, "MAX_BOUND", 1000));
        weightConf.setThrottleReceiveQueue(Boolean.parseBoolean(readPropOrDefault(conf, "THROTTLE_RQ", "false")));
        weightConf.setQueueResumeThreshold(parseConfAsIntOrDefault(conf, "RESUME_THRESHOLD", 0));
        weightConf.setMaxWeightAllowed(parseConfAsIntOrDefault(conf, "MAX_WT_ALLOWED", 100));

        SortedMap<String, String> props = conf.getProperties();
        for (String confKey : props.tailMap(WT_TOPIC_CONF_PREFIX).keySet()) {
            if (confKey.startsWith(WT_TOPIC_CONF_PREFIX)) {
                String topic = confKey.split(WT_TOPIC_CONF_PREFIX)[1];
                Integer weight;
                try {
                    weight = Integer.parseInt(conf.getProperties().get(confKey));
                } catch (NumberFormatException ex) {
                    weight = null;
                }
                weightConf.addTopic(topic, weight);
            }
        }

        weightConf.validate();
        return weightConf;
    }

    private static <T> void addProp(ConsumerBuilder<T> builder, String key, Object val) {
        builder.property(WT_CONF_PREFIX + key, val.toString());
    }

    private static <T> String readPropOrDefault(ConsumerConfigurationData<T> conf, String key, String defaultVal) {
        String val = conf.getProperties().get(WT_CONF_PREFIX + key);
        return val != null ? val : defaultVal;
    }

    private static <T> Integer parseConfAsIntOrDefault(ConsumerConfigurationData<T> conf, String key, Integer defaultVal) {
        try {
            return Integer.parseInt(readPropOrDefault(conf, key, defaultVal != null ? defaultVal.toString() : null));
        } catch (NumberFormatException ex) {
            return defaultVal;
        }
    }

    /**
     * Returns weight if the topic(partitioned or otherwise) follows the convention of appending weight to the name
     * Returns null if weight cannot be determined
     * @param topic
     * @return
     */
    private static Integer parseWeightFromTopic(String topic) {
        TopicName topicName = TopicName.get(topic);
        if (topicName.isPartitioned()) {
            topic = topicName.getPartitionedTopicName();
        }
        String[] parts = topic.split("-weight-");
        if (parts.length == 2) {
            try {
                return Integer.valueOf(parts[1]);
            } catch (NumberFormatException ex) {
            } //ignored
        }
        return null;
    }

    public enum DistributionStrategy {
        LINEAR, EXPONENTIAL
    }
}
