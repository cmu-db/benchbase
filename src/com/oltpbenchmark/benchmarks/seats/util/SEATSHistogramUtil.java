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

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.oltpbenchmark.benchmarks.seats.SEATSConstants;
import com.oltpbenchmark.util.Histogram;

public abstract class SEATSHistogramUtil {
    private static final Logger LOG = Logger.getLogger(SEATSHistogramUtil.class);

//    private static final Pattern p = Pattern.compile("\\|");
    
    private static final Map<File, Histogram<String>> cached_Histograms = new HashMap<File, Histogram<String>>();
    
    private static Map<String, Histogram<String>> cached_AirportFlights; 

    private static File getHistogramFile(File data_dir, String name) {
        File file = new File(data_dir.getAbsolutePath() + File.separator + "histogram." + name.toLowerCase());
        if (file.exists() == false) file = new File(file.getAbsolutePath() + ".gz");
        return (file);
    }
    
    public static Histogram<String> collapseAirportFlights(Map<String, Histogram<String>> m) {
        Histogram<String> h = new Histogram<String>();
        for (Entry<String, Histogram<String>> e : m.entrySet()) {
            String depart = e.getKey();
            Histogram<String> depart_h = e.getValue();
            for (String arrive : depart_h.values()) {
                String key = depart + "-" + arrive;
                h.put(key, depart_h.get(arrive));
            } // FOR (arrival airport)
        } // FOR (depart airport)
        return (h);
    }
    
    /**
     * Returns the Flights Per Airport Histogram
     * @param data_path
     * @return
     * @throws Exception
     */
    public static synchronized Map<String, Histogram<String>> loadAirportFlights(File data_path) throws Exception {
        if (cached_AirportFlights != null) return (cached_AirportFlights);
        
        File file = getHistogramFile(data_path, SEATSConstants.HISTOGRAM_FLIGHTS_PER_AIRPORT);
        Histogram<String> h = new Histogram<String>();
        h.load(file.getAbsolutePath());
        
        Map<String, Histogram<String>> m = new TreeMap<String, Histogram<String>>();
        Pattern pattern = Pattern.compile("-");
        Collection<String> values = h.values();
        for (String value : values) {
            String split[] = pattern.split(value);
            Histogram<String> src_h = m.get(split[0]);
            if (src_h == null) {
                src_h = new Histogram<String>();
                m.put(split[0], src_h);
            }
            src_h.put(split[1], h.get(value));
        } // FOR
        
        cached_AirportFlights = m;
        return (m);
    }
    
    /**
     * Construct a histogram from an airline-benchmark data file
     * @param name
     * @param data_path
     * @param has_header
     * @return
     * @throws Exception
     */
    public static synchronized Histogram<String> loadHistogram(String name, File data_path, boolean has_header) throws Exception {
        File file = getHistogramFile(data_path, name);
        Histogram<String> histogram = cached_Histograms.get(file);
        if (histogram == null) {
            histogram = new Histogram<String>();
            histogram.load(file.getAbsolutePath());
            cached_Histograms.put(file, histogram);
        }
        if (LOG.isDebugEnabled()) 
            LOG.debug(String.format("Histogram %s\n%s", name, histogram.toString()));
        
        return (histogram);
    }
}
