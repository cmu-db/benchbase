package com.oltpbenchmark.benchmarks.templated.util;

import java.util.Random;

import com.oltpbenchmark.distributions.ScrambledZipfianGenerator;
import com.oltpbenchmark.distributions.ZipfianGenerator;

public class TemplatedValue {
    String dist;
    Long min;
    Long max;
    Long seed;
    String value;
    String minS;
    String maxS;

    public final Object generatorObject;

    /**
     * 
     * @param dist  The desired value distribution
     * @param min   Minimum value. Default is 0
     * @param max   Maximum value. Default is 1
     * @param seed  The seed for the random generator. Default is 0
     * @param value Value that is used if no distribution is given
     */
    public TemplatedValue(String dist, String min, String max, String seed, String value) {
        this.dist = dist == null ? "" : dist;
        this.min = min == null ? 0 : Long.parseLong(min);
        this.max = max == null ? 1 : Long.parseLong(max);
        this.seed = seed == null ? 0 : Long.parseLong(seed);
        this.value = value;
        this.generatorObject = createGenerator(dist, this.min, this.max, this.seed);

        this.minS = min;
        this.maxS = max;
    }

    /**
     * 
     * @param distribution The distribution that drives the generator
     * @param min          Minimum value the generator will produce
     * @param max          Maximum value the generator will produce
     * @param seed         A seed for the generator
     * @return
     */
    private Object createGenerator(String distribution, Long min, Long max, Long seed) {

        if (distribution == null || distribution.length() < 1)
            return null;

        switch (distribution) {
            case "uniform":
            case "binomial":
                return new Random(seed);
            case "zipf":
                return new ZipfianGenerator(new Random(seed), min, max);
            case "scrambled":
                return new ScrambledZipfianGenerator(min, max);
            default:
                throw new RuntimeException(
                        "The distribution: '" + distribution
                                + "' is not supported. Currently supported are 'zipf' | 'scrambled' | 'normal' | 'uniform'");
        }

    }

    public String getDist() {
        return dist;
    }

    public Long getMin() {
        return min;
    }

    public Long getMax() {
        return max;
    }

    public Long getSeed() {
        return seed;
    }

    public String getValue() {
        return value;
    }

    public String getMinString() {
        return this.minS;
    }

    public String getMaxString() {
        return this.maxS;
    }

    public Object getGenerator() {
        return this.generatorObject;
    }
}
