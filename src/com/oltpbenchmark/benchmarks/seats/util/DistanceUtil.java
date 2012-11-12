/***************************************************************************
 *  Copyright (C) 2011 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  http://hstore.cs.brown.edu/                                            *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
package com.oltpbenchmark.benchmarks.seats.util;

import com.oltpbenchmark.util.Pair;

/**
 * Based on code found here:
 * http://www.zipcodeworld.com/samples/distance.java.html
 */
public abstract class DistanceUtil {

    /**
     * Calculate the distance between two points
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
