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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RangeDistribution implements WeightDistribution {
    private static final Logger log = LoggerFactory.getLogger(RangeDistribution.class);

    private final int[] distribution;

    public RangeDistribution(int lowerBound, int upperBound, int maxWeight) {
        validate(lowerBound, upperBound, maxWeight);

        distribution = distribute(lowerBound, upperBound, maxWeight);
        if (distribution == null) {
            throw new IllegalArgumentException("Null distribution generated in the strategy=" + getName());
        }

        log.info("Distribution for strategy={}: {}", getName(), distributionToString());
    }

    public int getValue(int weight) {
        if (weight <= 0 || weight > distribution.length) {
            throw new IllegalArgumentException("Invalid weight=" + weight + " supplied");
        }
        return distribution[weight -1];
    }

    public int getMinValue() {
        return distribution[0];
    }

    public int getMaxValue() {
        return distribution[distribution.length - 1];
    }

    @Override
    public String toString() {
        return String.format("name=%s, distribution=%s", getName(), distributionToString());
    }

    protected abstract int[] distribute(int lowerBound, int upperBound, int points);

    protected void validate(int lowerBound, int upperBound, int points) {
        if (lowerBound < 1) {
            throw new IllegalArgumentException("Min for lower bound is 1, provided=" + lowerBound);
        }
        if (points <= 0) {
            throw new IllegalArgumentException("Points should be greater than zero, provided=" + points);
        }
        if (upperBound < lowerBound) {
            throw new IllegalArgumentException(String.format("Upper bound should be greater than or equal to lower bound=%d, provided=%d",
                    lowerBound, upperBound));
        }
    }

    private String distributionToString() {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < distribution.length; i++) {
            if (i > 0) {
                builder.append(", ");
            }
            builder.append(distribution[i]);
        }
        return builder.toString();
    }
}
