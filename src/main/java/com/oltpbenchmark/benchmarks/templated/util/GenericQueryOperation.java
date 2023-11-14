/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.oltpbenchmark.benchmarks.templated.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import com.oltpbenchmark.api.Operation;
import com.oltpbenchmark.distributions.ScrambledZipfianGenerator;
import com.oltpbenchmark.distributions.ZipfianGenerator;

/**
 * Immutable class containing information about transactions.
 */
public class GenericQueryOperation extends Operation {

    public final List<Object> params;
    public final HashMap<Integer, Object> randomGenerators;

    public GenericQueryOperation(Object[] params) {
        super();
        this.params = Collections.unmodifiableList(Arrays.asList(params));
        this.randomGenerators = this.mapRandom();
    }

    public List<Object> getParams() {
        return params;
    }

    private HashMap<Integer, Object> mapRandom() {
        HashMap<Integer, Object> randomGenerators = new HashMap<Integer, Object>();
        Integer min, max;
        for (int i = 0; i < params.size(); i++) {
            String paramString = params.get(i).toString();
            switch (paramString) {
                case "zipf":
                    min = Integer.parseInt(params.get(i + 1).toString());
                    max = Integer.parseInt(params.get(i + 2).toString());
                    ZipfianGenerator zipf = new ZipfianGenerator(new Random(), min, max);
                    randomGenerators.put(i, zipf);
                    break;
                case "uniform":
                    randomGenerators.put(i, new Random());
                    break;
                case "binomial":
                    randomGenerators.put(i, new Random());
                    break;
                case "scrambled":
                    min = Integer.parseInt(params.get(i + 1).toString());
                    max = Integer.parseInt(params.get(i + 2).toString());
                    ScrambledZipfianGenerator scramZipf = new ScrambledZipfianGenerator(min, max);
                    randomGenerators.put(i, scramZipf);
                    break;
                case "string":
                    randomGenerators.put(i, new Random());
                    i -= 1;
                    break;
            }
            i += 2;
        }
        return randomGenerators;

    }

    public HashMap<Integer, Object> getRandomGenHashMap() {
        return this.randomGenerators;
    }
}
