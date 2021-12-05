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

import com.oltpbenchmark.benchmarks.seats.SEATSConstants;
import com.oltpbenchmark.util.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

public abstract class SEATSHistogramUtil {
    private static final Logger LOG = LoggerFactory.getLogger(SEATSHistogramUtil.class);

    private static final Map<String, Histogram<String>> cached_Histograms = new HashMap<>();

    private static Map<String, Histogram<String>> cached_AirportFlights;

    private static String getHistogramFilePath(String data_dir, String name) {
        return data_dir + File.separator + "histogram." + name.toLowerCase();
    }

    /**
     * Returns the Flights Per Airport Histogram
     *
     * @param data_path
     * @return
     * @throws Exception
     */
    public static synchronized Map<String, Histogram<String>> loadAirportFlights(String data_path) throws Exception {
        if (cached_AirportFlights != null) {
            return (cached_AirportFlights);
        }

        String filePath = getHistogramFilePath(data_path, SEATSConstants.HISTOGRAM_FLIGHTS_PER_AIRPORT);
        Histogram<String> h = new Histogram<>();
        h.load(filePath);

        Map<String, Histogram<String>> m = new TreeMap<>();
        Pattern pattern = Pattern.compile("-");
        Collection<String> values = h.values();
        for (String value : values) {
            String[] split = pattern.split(value);
            Histogram<String> src_h = m.get(split[0]);
            if (src_h == null) {
                src_h = new Histogram<>();
                m.put(split[0], src_h);
            }
            src_h.put(split[1], h.get(value));
        }

        cached_AirportFlights = m;
        return (m);
    }

    /**
     * Construct a histogram from an airline-benchmark data file
     *
     * @param name
     * @param data_path
     * @param has_header
     * @return
     * @throws Exception
     */
    public static synchronized Histogram<String> loadHistogram(String name, String data_path, boolean has_header) throws Exception {
        String filePath = getHistogramFilePath(data_path, name);
        Histogram<String> histogram = cached_Histograms.get(filePath);
        if (histogram == null) {
            histogram = new Histogram<>();
            histogram.load(filePath);
            cached_Histograms.put(filePath, histogram);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Histogram %s\n%s", name, histogram));
        }

        return (histogram);
    }
}
