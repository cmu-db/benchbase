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

import junit.framework.TestCase;

public class TestDistanceUtil extends TestCase {
    
    /**
     * testDistance
     */
    public void testDistance() throws Exception {
        // { latitude, longitude }
        double locations[][] = {
            {  39.175278, -76.668333 }, // Baltimore-Washington, USA (BWI)
            { -22.808889, -43.243611 }, // Rio de Janeiro, Brazil (GIG)
            {  40.633333, -73.783333 }, // New York, USA (JFK)
            { -33.946111, 151.177222 }, // Syndey, Austrailia (SYD)
        };
        // expected distance in miles
        double expected[] = {
            4796,   // BWI->GIG
            183,    // BWI->JFK
            9787,   // BWI->SYD
            4802,   // GIG->JFK
            8402,   // GIG->SYD
            9950,   // JFK->SYD                
        };

        int e = 0;
        for (int i = 0; i < locations.length - 1; i++) {
            double loc0[] = locations[i];
            for (int j = i + 1; j < locations.length; j++) {
                double loc1[] = locations[j];
                double distance = Math.round(DistanceUtil.distance(loc0[0], loc0[1], loc1[0], loc1[1]));
                assertEquals(expected[e++], distance);
            } // FOR
        } // FOR
    }

}
