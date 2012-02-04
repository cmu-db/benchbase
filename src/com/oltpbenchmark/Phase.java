package com.oltpbenchmark;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class Phase {

    private final Random gen = new Random();
    public final int time;
    public final int rate;
    private final List<Double> weights;
    private final int num_weights;
    

    Phase(int t, int r, List<String> o) {

        ArrayList<Double> w = new ArrayList<Double>();

        for (String s : o)
            w.add(Double.parseDouble(s));

        time = t;
        rate = r;
        weights = Collections.unmodifiableList(w);
        this.num_weights = this.weights.size();
    }

    public int getWeightCount() {
        return (this.num_weights);
    }
    public List<Double> getWeights() {
        return (this.weights);
    }
    
    /**
     * Computes the sum of weights. Usually needs to add up to 100%
     * 
     * @return The total weight
     */
    public double totalWeight() {
        double total = 0.0;
        for (Double d : weights)
            total += d;
        return total;
    }

    /**
     * This simply computes the next transaction by randomly selecting one based
     * on the weights of this phase.
     * 
     * @return
     */
    public int chooseTransaction() {
        int randomPercentage = gen.nextInt(100) + 1;
        double weight = 0.0;
        for (int i = 0; i < this.num_weights; i++) {
            weight += weights.get(i).doubleValue();
            if (randomPercentage <= weight) {
                return i + 1;
            }
        } // FOR

        return -1;
    }

}