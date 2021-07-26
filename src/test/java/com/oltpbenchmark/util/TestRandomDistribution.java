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


package com.oltpbenchmark.util;

import junit.framework.TestCase;

import java.util.Random;

public class TestRandomDistribution extends TestCase {

    private final Random rand = new Random(0);

    private final int min = 0;
    private final int max = 20;

    private final int num_records = 100000;
    private final int num_rounds = 10;

    /**
     * testCalculateMean
     */
    public void testCalculateMean() throws Exception {
        final int expected = ((max - min) / 2) + min;
        final int samples = 10000;

        RandomDistribution.Gaussian gaussian = new RandomDistribution.Gaussian(this.rand, min, max);
        double mean = gaussian.calculateMean(samples);
//        System.err.println("mean="+ mean);
        assert ((expected - 1) <= mean) : (expected - 1) + " <= " + mean;
        assert ((expected + 1) >= mean) : (expected - 1) + " >= " + mean;
    }

    /**
     * testHistory
     */
    public void testHistory() throws Exception {
        double sigma = 1.0000001d;
        RandomDistribution.Zipf zipf = new RandomDistribution.Zipf(this.rand, min, max, sigma);
        zipf.enableHistory();

        Histogram<Long> hist = new Histogram<Long>();
        for (int i = 0; i < num_records; i++) {
            hist.put((long) zipf.nextInt());
        } // FOR

        Histogram<Long> tracking_hist = zipf.getHistory();
        assertEquals(hist.getSampleCount(), tracking_hist.getSampleCount());
        for (Long value : hist.values()) {
            assert (tracking_hist.contains(value));
            assertEquals(hist.get(value), tracking_hist.get(value));
        } // FOR
    }

    /**
     * testGaussianInt
     */
    public void testGaussian() throws Exception {
        int expected = ((max - min) / 2) + min;

        int round = num_rounds;
        while (round-- > 0) {
            RandomDistribution.Gaussian gaussian = new RandomDistribution.Gaussian(this.rand, min, max);
            Histogram<Integer> hist = new Histogram<Integer>();
            for (int i = 0; i < num_records; i++) {
                int value = gaussian.nextInt();
                // double value = rand.nextGaussian();
                hist.put(value);
            } // FOR
            // System.out.println(hist);
            int max_count_value = CollectionUtil.first(hist.getMaxCountValues());
            // System.out.println("expected=" + expected + ", max_count_value=" + max_count_value);
            assertTrue((expected - 1) <= max_count_value);
            assertTrue((expected + 1) >= max_count_value);
        } // WHILE
    }

    /**
     * testGaussianLong
     */
    public void testGaussianLong() throws Exception {
        int expected = ((max - min) / 2) + min;

        int round = num_rounds;
        while (round-- > 0) {
            RandomDistribution.Gaussian gaussian = new RandomDistribution.Gaussian(this.rand, min, max);
            Histogram<Long> hist = new Histogram<Long>();
            for (int i = 0; i < num_records; i++) {
                long value = gaussian.nextLong();
                // double value = rand.nextGaussian();
                hist.put(value);
            } // FOR
            // System.out.println(hist);
            Long max_count_value = CollectionUtil.first(hist.getMaxCountValues());
            // System.out.println("expected=" + expected + ", max_count_value=" + max_count_value);
            assertTrue((expected - 1) <= max_count_value);
            assertTrue((expected + 1) >= max_count_value);
        } // WHILE
    }

    /**
     * testZipfian
     */
    public void testZipfian() throws Exception {
        double sigma = 1.0000001d;

        int round = num_rounds;
        while (round-- > 0) {
            RandomDistribution.Zipf zipf = new RandomDistribution.Zipf(this.rand, min, max, sigma);
            Histogram<Integer> hist = new Histogram<Integer>();
            // System.out.println("Round #" + Math.abs(num_rounds - 10) + " [sigma=" + sigma + "]");
            for (int i = 0; i < num_records; i++) {
                int value = zipf.nextInt();
                hist.put(value);
            } // FOR
            Long last = null;
            for (Integer value : hist.values()) {
                long current = hist.get(value);
                if (last != null) {
                    // assertTrue(last >= current);
                }
                last = current;
            }
//            System.out.println(hist);
//            System.out.println("----------------------------------------------");
            sigma += 0.5d;
        } // FOR
    }

    /**
     * testFlatHistogramInt
     */
    public void testFlatHistogramInt() throws Exception {
        Histogram<Integer> hist = new Histogram<Integer>();
        RandomDistribution.Zipf zipf = new RandomDistribution.Zipf(this.rand, min, max, 1.0000001d);
        for (int i = 0; i < num_records; i++) {
            hist.put(zipf.nextInt());
        } // FOR

        RandomDistribution.FlatHistogram<Integer> flat = new RandomDistribution.FlatHistogram<Integer>(this.rand, hist);
        Histogram<Integer> hist2 = new Histogram<Integer>();
        for (int i = 0; i < num_records; i++) {
            hist2.put(flat.nextInt());
        } // FOR
        assertEquals(hist.getMaxCountValues(), hist2.getMaxCountValues());
    }

    /**
     * testFlatHistogramLong
     */
    public void testFlatHistogramLong() throws Exception {
        Histogram<Long> hist = new Histogram<Long>();
        RandomDistribution.Zipf zipf = new RandomDistribution.Zipf(this.rand, min, max, 1.0000001d);
        for (int i = 0; i < num_records; i++) {
            hist.put(zipf.nextLong());
        } // FOR

        RandomDistribution.FlatHistogram<Long> flat = new RandomDistribution.FlatHistogram<Long>(this.rand, hist);
        Histogram<Long> hist2 = new Histogram<Long>();
        for (int i = 0; i < num_records; i++) {
            hist2.put(flat.nextLong());
        } // FOR
        assertEquals(hist.getMaxCountValues(), hist2.getMaxCountValues());
    }
}