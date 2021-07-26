/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/


package com.oltpbenchmark.benchmarks.seats.util;

import junit.framework.TestCase;

public class TestDistanceUtil extends TestCase {

    /**
     * testDistance
     */
    public void testDistance() throws Exception {
        // { latitude, longitude }
        double[][] locations = {
                {39.175278, -76.668333}, // Baltimore-Washington, USA (BWI)
                {-22.808889, -43.243611}, // Rio de Janeiro, Brazil (GIG)
                {40.633333, -73.783333}, // New York, USA (JFK)
                {-33.946111, 151.177222}, // Syndey, Austrailia (SYD)
        };
        // expected distance in miles
        double[] expected = {
                4796,   // BWI->GIG
                183,    // BWI->JFK
                9787,   // BWI->SYD
                4802,   // GIG->JFK
                8402,   // GIG->SYD
                9950,   // JFK->SYD
        };

        int e = 0;
        for (int i = 0; i < locations.length - 1; i++) {
            double[] loc0 = locations[i];
            for (int j = i + 1; j < locations.length; j++) {
                double[] loc1 = locations[j];
                double distance = Math.round(DistanceUtil.distance(loc0[0], loc0[1], loc1[0], loc1[1]));
                assertEquals(expected[e++], distance);
            } // FOR
        } // FOR
    }

}
