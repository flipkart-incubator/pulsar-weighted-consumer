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

public class ExponentialWeightDistribution extends RangeDistribution {

    public ExponentialWeightDistribution(int lowerBound, int upperBound, int points) {
        super(lowerBound, upperBound, points);
    }

    @Override
    protected int[] distribute(int lowerBound, int upperBound, int points) {
        if (points == 1) {
            return new int[]{lowerBound};
        } else if (points == 2) {
            return new int[]{lowerBound, upperBound};
        }

        int[] distribution = new int[points];
        double multiplier = Math.pow((upperBound * 1.0) / lowerBound, 1.0 / (points - 1));
        for (int i = 0; i < points; i++) {
            if (i == points - 1) {
                distribution[i] = upperBound;
            } else {
                distribution[i] = (int) Math.round(lowerBound * Math.pow(multiplier, i));
            }
        }
        return distribution;
    }

    @Override
    public String getName() {
        return "EXPONENTIAL";
    }
}
