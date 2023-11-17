package com.oltpbenchmark.benchmarks.templated.util;

import java.util.Random;

import com.oltpbenchmark.distributions.ScrambledZipfianGenerator;
import com.oltpbenchmark.distributions.ZipfianGenerator;

public class ComplexValue {
    String dist;
    Long min;
    Long max;
    Long seed;
    String value;
    String minS;
    String maxS;

    public final Object randGen;


    public ComplexValue(String dist, String min, String max, String seed, String value) {
        this.dist = dist == null ? "" : dist;
        this.min = min == null ? 0 : Long.parseLong(min);
        this.max = max == null ? 1 : Long.parseLong(max);
        this.seed = seed == null ? 0 : Long.parseLong(seed);
        this.value = value;
        this.randGen = createGen(dist,this.min,this.max,this.seed);
        this.minS = min;
        this.maxS = max;
    }

    private Object createGen(String distribution, Long min, Long max, Long seed) {

        if(distribution == null || distribution.length() < 1) return null;

        switch (distribution) {
            case "uniform":
            case "binomial":
                return new Random(seed);
            case "zipf":
                return new ZipfianGenerator(new Random(seed),min,max);
            case "scrambled":
                return new ScrambledZipfianGenerator(min,max);
            default:
                throw new RuntimeException(
                "The distribution: '" + distribution + "' is not supported. Currently supported are 'zipf' | 'scrambled' | 'normal' | 'uniform'");
        }

    }


    public String getDist(){
        return dist;
    }

    public Long getMin(){
        return min;
    }

    public Long getMax(){
        return max;
    }

    public Long getSeed(){
        return seed;
    }

    public String getValue(){
        return value;
    }

    public String getMinString(){
        return this.minS;
    }

    public String getMaxString(){
        return this.maxS;
    }

    public Object getGen(){
        return this.randGen;
    }
}
