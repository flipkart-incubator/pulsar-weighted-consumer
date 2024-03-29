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

package org.apache.pulsar.client.impl;

import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.weight.WeightedConsumerConfiguration;
import org.apache.pulsar.client.util.RetryMessageUtil;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.shade.org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.pulsar.shade.com.google.common.base.Preconditions.checkArgument;

public class WeightedConsumerBuilder<T> extends ConsumerBuilderImpl<T> {
    private WeightedConsumerConfiguration weightConf;
    private Integer retryTopicWeight;

    public WeightedConsumerBuilder(PulsarClient client, Schema<T> schema) {
        this((PulsarClientImpl) client, new ConsumerConfigurationData<T>(), schema);
    }

    WeightedConsumerBuilder(PulsarClientImpl client, ConsumerConfigurationData<T> conf, Schema<T> schema) {
        super(client, conf, schema);
        weightConf = WeightedConsumerConfiguration.loadFromConf(conf);
    }

    public WeightedConsumerConfiguration getWeightConf() {
        return weightConf;
    }

    @Override
    public WeightedConsumerBuilder<T> loadConf(Map<String, Object> config) {
        super.loadConf(config);
        weightConf = WeightedConsumerConfiguration.loadFromConf(getConf());
        return this;
    }

    @Override
    public WeightedConsumerBuilder<T> clone() {
        return new WeightedConsumerBuilder<>(getClient(), getConf().clone(), getSchema());
    }

    public WeightedConsumerBuilder<T> distributionStrategy(WeightedConsumerConfiguration.DistributionStrategy distributionStrategy) {
        weightConf.setDistributionStrategy(distributionStrategy);
        return this;
    }

    public WeightedConsumerBuilder<T> minBound(int minBound) {
        weightConf.setMinBound(minBound);
        return this;
    }

    public WeightedConsumerBuilder<T> maxBound(int maxBound) {
        weightConf.setMaxBound(maxBound);
        return this;
    }

    public WeightedConsumerBuilder<T> retryTopicWeight(Integer weight) {
        if (weight != null) {
            checkArgument(weight >= 1 && weight <= weightConf.getMaxWeightAllowed(),
                    "non-null weights should be in the range [1,maxWeightAllowed(%s)]", weightConf.getMaxWeightAllowed());
        }
        this.retryTopicWeight = weight;
        return this;
    }

    public WeightedConsumerBuilder<T> throttleReceiveQueue(boolean shouldThrotle) {
        weightConf.setThrottleReceiveQueue(shouldThrotle);
        return this;
    }

    public WeightedConsumerBuilder<T> queueResumeThreshold(int resumeThreshold) {
        weightConf.setQueueResumeThreshold(resumeThreshold);
        return this;
    }

    public WeightedConsumerBuilder<T> maxWeightAllowed(int maxWeightAllowed) {
        weightConf.setMaxWeightAllowed(maxWeightAllowed);
        return this;
    }

    //If weight is null, attempt is made to parse it from topic name, otherwise defaults to 1
    public WeightedConsumerBuilder<T> topic(String topic, Integer weight) {
        weightConf.addTopic(topic, weight);
        return this;
    }

    public WeightedConsumerBuilder<T> topics(Map<String, Integer> topicWeights) {
        checkArgument(topicWeights != null && topicWeights.size() > 0, "non-empty topic-weight map required");
        for (Map.Entry<String, Integer> entry : topicWeights.entrySet()) {
            topic(entry.getKey(), entry.getValue());
        }
        return this;
    }
    private String getSubscriptionName(ConsumerConfigurationData conf){
        String subscriptionName=conf.getSubscriptionName();
        subscriptionName.replace("/", "__");
        return subscriptionName;
    }


    /**
     * Copied from {@link ConsumerBuilderImpl#subscribeAsync()}
     * Few changes, do a diff to quickly identify those
    **/
    @Override
    public CompletableFuture<Consumer<T>> subscribeAsync() {
        weightConf.populateBuilder(this);
        //Since the members are private in superclass, using getters here to setup relevant local variables
        ConsumerConfigurationData<T> conf = getConf();
        PulsarClientImpl client = getClient();
        Schema<T> schema = getSchema();
        List<ConsumerInterceptor<T>> interceptorList = getInterceptorList();

        if (conf.getTopicNames().isEmpty() && conf.getTopicsPattern() == null) {
            return FutureUtil
                    .failedFuture(new PulsarClientException.InvalidConfigurationException("Topic name must be set on the consumer builder"));
        }

        if (StringUtils.isBlank(conf.getSubscriptionName())) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.InvalidConfigurationException("Subscription name must be set on the consumer builder"));
        }

        if (conf.getKeySharedPolicy() != null && conf.getSubscriptionType() != SubscriptionType.Key_Shared) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.InvalidConfigurationException("KeySharedPolicy must set with KeyShared subscription"));
        }
        if(conf.isRetryEnable() && conf.getTopicNames().size() > 0 ) {
            TopicName topicFirst = TopicName.get(conf.getTopicNames().iterator().next());
            String subscriptionName=getSubscriptionName(conf);
            String retryLetterTopic = topicFirst.getNamespace() + "/" + subscriptionName + RetryMessageUtil.RETRY_GROUP_TOPIC_SUFFIX;
            String deadLetterTopic = topicFirst.getNamespace() + "/" + subscriptionName + RetryMessageUtil.DLQ_GROUP_TOPIC_SUFFIX;
            if(conf.getDeadLetterPolicy() == null) {
                conf.setDeadLetterPolicy(DeadLetterPolicy.builder()
                        .maxRedeliverCount(RetryMessageUtil.MAX_RECONSUMETIMES)
                        .retryLetterTopic(retryLetterTopic)
                        .deadLetterTopic(deadLetterTopic)
                        .build());
            } else {
                if (StringUtils.isBlank(conf.getDeadLetterPolicy().getRetryLetterTopic())) {
                    conf.getDeadLetterPolicy().setRetryLetterTopic(retryLetterTopic);
                }
                if (StringUtils.isBlank(conf.getDeadLetterPolicy().getDeadLetterTopic())) {
                    conf.getDeadLetterPolicy().setDeadLetterTopic(deadLetterTopic);
                }
            }

            /** hacky - Populate {@link ConsumerConfigurationData#topicNames} directly since weightConf is not going to be read from at this point **/
            WeightedConsumerConfiguration.populateTopicInBuilder(this, conf.getDeadLetterPolicy().getRetryLetterTopic(), retryTopicWeight);
        }

        return interceptorList == null || interceptorList.size() == 0 ?
                WeightedMultiTopicsConsumerImpl.subscribeAsync(client, conf, weightConf, schema, null) :
                WeightedMultiTopicsConsumerImpl.subscribeAsync(client, conf, weightConf, schema, new ConsumerInterceptors<>(interceptorList));
    }
}
