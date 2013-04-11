package com.oltpbenchmark;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class Phase {
    public enum Arrival {
        REGULAR,POISSON,
    }

    private final Random gen = new Random();
    public final String benchmarkName;
    public final int id;
    public final int time;
    public final int rate;
    public final Arrival arrival;


    private final boolean rateLimited;
    private final boolean disabled;
    private final List<Double> weights;
    private final int num_weights;
    private int activeTerminals;
    

    Phase(String benchmarkName, int id, int t, int r, List<String> o, boolean rateLimited, boolean disabled, int activeTerminals, Arrival a) {
        ArrayList<Double> w = new ArrayList<Double>();
        for (String s : o)
            w.add(Double.parseDouble(s));

        this.benchmarkName = benchmarkName;
        this.id = id;
        this.time = t;
        this.rate = r;
        this.weights = Collections.unmodifiableList(w);
        this.num_weights = this.weights.size();
        this.rateLimited = rateLimited;
        this.disabled = disabled;
        this.activeTerminals = activeTerminals;
        this.arrival=a;
    }
    
    public boolean isRateLimited() {
        return rateLimited;
    }

    public boolean isDisabled() {
        return disabled;
    }

    public int getActiveTerminals() {
        return activeTerminals;
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
    
    /**
     * Returns a string for logging purposes when entering the phase
     * 
     * @return Loggin String
     */
    public String currentPhaseString() {
        String retString ="[Starting Phase] [Workload= " + benchmarkName + "] ";
        if (isDisabled()){
            retString += "[Disabled= true]";
        } else {
            retString += "[Time= " + time + "] [Rate= " + (isRateLimited() ? rate : "unlimited") + "] [Arrival= " + arrival + "] [Ratios= " + getWeights() + "] [Active Workers=" + getActiveTerminals() + "]";
        }
        return retString;
    }

}