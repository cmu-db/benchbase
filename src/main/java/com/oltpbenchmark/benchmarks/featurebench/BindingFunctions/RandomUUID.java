package com.oltpbenchmark.benchmarks.featurebench.BindingFunctions;

import java.util.UUID;


public class RandomUUID {
    public String getRandomUUID() {
        UUID uuid = UUID.randomUUID();
        String uuidAsString = uuid.toString();
        return uuidAsString;
    }
}

