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

import java.util.Map;
import java.util.regex.Pattern;

import com.oltpbenchmark.benchmarks.seats.SEATSBaseTestCase;
import com.oltpbenchmark.benchmarks.seats.SEATSConstants;
import com.oltpbenchmark.util.Histogram;

public class TestHistogramUtil extends SEATSBaseTestCase {

    /**
     * testLoadAirportFlights
     */
    public void testLoadAirportFlights() throws Exception {
        Map<String, Histogram<String>> histograms = SEATSHistogramUtil.loadAirportFlights(AIRLINE_DATA_DIR);
        assertNotNull(histograms);
        assertFalse(histograms.isEmpty());
        assert(histograms.size() >= 200);
        
        // Just some airports that we expect to be in there
        String airports[] = { "BWI", "LAX", "JFK", "MDW", "ATL", "SFO", "ORD" };
        for (String a : airports) {
            assertTrue(a, histograms.containsKey(a));
        } // FOR
        
        System.err.println(histograms);
        
        // We expect ATL to be the max
//        assertEquals("ATL", histogram.getMaxCountValue());
        
        // Make sure the values are formatted correctly
//        ListOrderedMap<String, Histogram<String>> m = new ListOrderedMap<String, Histogram<String>>();
        Pattern p = Pattern.compile("[\\d\\w]{3,3}");
        for (String s_airport : histograms.keySet()) {
            assert(p.matcher(s_airport).matches()) : "Invalid source airport: " + s_airport;
//            m.put(s_airport, histograms.get(s_airport));
            for (Object value : histograms.get(s_airport).values()) {
                assert(p.matcher(value.toString()).matches()) : "Invalid destination airport: " + value;  
            } // FOR
        } // FOR
//        System.err.println(StringUtil.formatMaps(m));
    }
    
    /**
     * testLoadFlightDepartTime
     */
    public void testLoadFlightDepartTime() throws Exception {
        Histogram<String> histogram = SEATSHistogramUtil.loadHistogram(SEATSConstants.HISTOGRAM_FLIGHTS_PER_DEPART_TIMES, AIRLINE_DATA_DIR, true);
        assertFalse(histogram.values().isEmpty());
        
        // We expect the times to be in 15 minute increments, therefore there should
        // be exactly 96 entries in the histogram
        assertEquals(94, histogram.values().size());
        
        // Make sure the values are formatted correctly
        Pattern p = Pattern.compile("[\\d]{2,2}:[\\d]{2,2}");
        for (Object value : histogram.values()) {
            assert(p.matcher(value.toString()).matches()) : "Invalid entry '" + value + "'";
        } // FOR
//        System.err.println("Values=" + histogram.getValueCount() + "\n" + histogram);
    }

}
