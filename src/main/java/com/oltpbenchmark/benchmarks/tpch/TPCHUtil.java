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

package com.oltpbenchmark.benchmarks.tpch;

import com.oltpbenchmark.util.RandomGenerator;

public class TPCHUtil {

    /**
     * Returns a random element of the array
     *
     * @param array
     * @param rand
     * @param <T>
     * @return a random element of the array
     */
    public static <T> T choice(T[] array, RandomGenerator rand) {
        return array[rand.number(1, array.length) - 1];
    }

    /**
     * Returns the region key given the nation
     *
     * @param nation N_NAME
     * @return region key
     */
    public static int getRegionKeyFromNation(String nation) {
        switch (nation) {
            case "ALGERIA":
            case "ETHIOPIA":
            case "KENYA":
            case "MOROCCO":
            case "MOZAMBIQUE":
                return 0;
            case "ARGENTINA":
            case "BRAZIL":
            case "CANADA":
            case "PERU":
            case "UNITED STATES":
                return 1;
            case "INDIA":
            case "INDONESIA":
            case "JAPAN":
            case "CHINA":
            case "VIETNAM":
                return 2;
            case "FRANCE":
            case "GERMANY":
            case "ROMANIA":
            case "RUSSIA":
            case "UNITED KINGDOM":
                return 3;
            case "EGYPT":
            case "IRAN":
            case "IRAQ":
            case "JORDAN":
            case "SAUDI ARABIA":
                return 4;
            default:
                throw new IllegalArgumentException(String.format("Invalid nation %s", nation));
        }
    }

    /**
     * Returns the region given the region key
     *
     * @param regionKey region key
     * @return region
     */
    public static String getRegionFromRegionKey(int regionKey) {
        switch (regionKey) {
            case 0:
                return "AFRICA";
            case 1:
                return "AMERICA";
            case 2:
                return "ASIA";
            case 3:
                return "EUROPE";
            case 4:
                return "MIDDLE EAST";
            default:
                throw new IllegalArgumentException(String.format("Invalid region key %s", regionKey));
        }
    }

    /**
     * Generates a random brand string of the form 'Brand#MN' where M and N are
     * two single character strings representing two numbers randomly and
     * independently selected within [1 .. 5]
     *
     * @param rand Random generator to use
     * @return A random brand conforming to the TPCH specification
     */
    public static String randomBrand(RandomGenerator rand) {
        int M = rand.number(1, 5);
        int N = rand.number(1, 5);

        return String.format("Brand#%d%d", M, N);
    }

}
