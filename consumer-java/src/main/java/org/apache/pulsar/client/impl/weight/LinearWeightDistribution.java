package org.apache.pulsar.client.impl.weight;

public class LinearWeightDistribution extends RangeDistribution {

    public LinearWeightDistribution(int lowerBound, int upperBound, int points) {
        super(lowerBound, upperBound, points);
    }

    @Override
    protected int[] distribute(int lowerBound, int upperBound, int points) {
        int[] distribution = new int[points];
        for (int i = 0; i < points; i++) {
            distribution[i] = lowerBound + Math.round(i * (upperBound - lowerBound) / ((points - 1) * 1.0f));
        }
        return distribution;
    }

    @Override
    public String getName() {
        return "LINEAR";
    }
}
