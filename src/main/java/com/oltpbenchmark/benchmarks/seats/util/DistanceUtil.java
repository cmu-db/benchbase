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


package com.oltpbenchmark.benchmarks.seats.util;

import com.oltpbenchmark.util.Pair;

/**
 * Based on code found here:
 * http://www.zipcodeworld.com/samples/distance.java.html
 */
public abstract class DistanceUtil {

    /**
     * Calculate the distance between two points
     *
     * @param lat0
     * @param lon0
     * @param lat1
     * @param lon1
     * @return
     */
    public static double distance(double lat0, double lon0, double lat1, double lon1) {
        double theta = lon0 - lon1;
        double dist = Math.sin(deg2rad(lat0)) * Math.sin(deg2rad(lat1)) + Math.cos(deg2rad(lat0)) * Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(theta));
        dist = Math.acos(dist);
        dist = rad2deg(dist);
        return (dist * 60 * 1.1515);
    }

    /**
     * Pair<Latitude, Longitude>
     *
     * @param loc0
     * @param loc1
     * @return
     */
    public static double distance(Pair<Double, Double> loc0, Pair<Double, Double> loc1) {
        return (DistanceUtil.distance(loc0.first, loc0.second, loc1.first, loc1.second));
    }

    private static double deg2rad(double deg) {
        return (deg * Math.PI / 180.0);
    }

    private static double rad2deg(double rad) {
        return (rad * 180.0 / Math.PI);
    }
}
