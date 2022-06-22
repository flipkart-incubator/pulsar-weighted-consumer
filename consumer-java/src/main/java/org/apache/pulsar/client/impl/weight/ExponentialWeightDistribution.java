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
