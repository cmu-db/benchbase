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

package com.oltpbenchmark.util;

import java.util.*;

/**
 * A class that generates random numbers that follow some distribution.
 * <p>
 * Copied from
 * <a href="https://issues.apache.org/jira/browse/HADOOP-3315">hadoop-3315 tfile</a>.
 * Remove after tfile is committed and use the tfile version of this class
 * instead.</p>
 */
public class RandomDistribution {
    /**
     * Interface for discrete (integer) random distributions.
     */
    public static abstract class DiscreteRNG extends Random {
        private static final long serialVersionUID = 1L;
        protected final long min;
        protected final long max;
        protected final Random random;
        protected final double mean;
        protected final long range_size;
        private Histogram<Long> history;

        public DiscreteRNG(Random random, long min, long max) {
            if (min >= max) {
                throw new IllegalArgumentException("Invalid range [" + min + " >= " + max + "]");
            }
            this.random = random;
            this.min = min;
            this.max = max;
            this.range_size = (max - min) + 1;
            this.mean = this.range_size / 2.0;
        }

        protected abstract long nextLongImpl();

        /**
         * Enable keeping track of the values that the RNG generates
         */
        public void enableHistory() {

            this.history = new Histogram<>();
        }

        /**
         * Return the histogram of the values that have been generated
         *
         * @return
         */
        public Histogram<Long> getHistory() {

            return (this.history);
        }

        public long getRange() {
            return this.range_size;
        }

        public long getMin() {
            return this.min;
        }

        public long getMax() {
            return this.max;
        }

        public Random getRandom() {
            return (this.random);
        }

        public double calculateMean(int num_samples) {
            long total = 0l;
            for (int i = 0; i < num_samples; i++) {
                total += this.nextLong();
            } // FOR
            return (total / (double)num_samples);
        }

        /**
         * Get the next random number as an int
         *
         * @return the next random number.
         */
        @Override
        public final int nextInt() {
            long val = (int) this.nextLongImpl();
            if (this.history != null) {
                this.history.put(val);
            }
            return ((int) val);
        }

        /**
         * Get the next random number as a long
         *
         * @return the next random number.
         */
        @Override
        public final long nextLong() {
            long val = this.nextLongImpl();
            if (this.history != null) {
                this.history.put(val);
            }
            return (val);
        }

        @Override
        public String toString() {
            return String.format("%s[min=%d, max=%d]", this.getClass().getSimpleName(), this.min, this.max);
        }


        public static long nextLong(Random rng, long n) {
            // error checking and 2^x checking removed for simplicity.
            long bits, val;
            do {
                bits = (rng.nextLong() << 1) >>> 1;
                val = bits % n;
            }
            while (bits - val + (n - 1) < 0L);
            return val;
        }
    }

    /**
     * P(i)=1/(max-min)
     */
    public static class Flat extends DiscreteRNG {
        private static final long serialVersionUID = 1L;

        /**
         * Generate random integers from min (inclusive) to max (exclusive)
         * following even distribution.
         *
         * @param random The basic random number generator.
         * @param min    Minimum integer
         * @param max    maximum integer (exclusive).
         */
        public Flat(Random random, long min, long max) {
            super(random, min, max);
        }

        /**
         * @see DiscreteRNG#nextInt()
         */
        @Override
        protected long nextLongImpl() {
            // error checking and 2^x checking removed for simplicity.
            long bits, val;
            do {
                bits = (random.nextLong() << 1) >>> 1;
                val = bits % (this.range_size - 1);
            }
            while (bits - val + (this.range_size - 1) < 0L);
            val += this.min;


            return val;
        }
    }

    /**
     * P(i)=1/(max-min)
     */
    public static class FlatHistogram<T extends Comparable<T>> extends DiscreteRNG {
        private static final long serialVersionUID = 1L;
        private final Flat inner;
        private final SortedMap<Long, T> value_rle = new TreeMap<>();
        private Histogram<T> history;

        /**
         * Generate a run-length of the values of the histogram
         */
        public FlatHistogram(Random random, Histogram<T> histogram) {
            super(random, 0, histogram.getSampleCount());
            this.inner = new Flat(random, 0, histogram.getSampleCount());

            long total = 0;
            for (T k : histogram.values()) {
                long v = histogram.get(k);
                total += v;
                this.value_rle.put(total, k);
            }
        }

        @Override
        public void enableHistory() {
            this.history = new Histogram<>();
        }


        public Histogram<T> getHistogramHistory() {
            if (this.history != null) {
                return (this.history);
            }
            return (null);
        }

        public T nextValue() {
            int idx = this.inner.nextInt();
            Long total = this.value_rle.tailMap((long) idx).firstKey();
            T val = this.value_rle.get(total);
            if (this.history != null) {
                this.history.put(val);
            }
            return (val);
//            assert(false) : "Went beyond our expected total '" + idx + "'";
//            return (null);
        }

        /**
         * @see DiscreteRNG#nextLong()
         */
        @Override
        protected long nextLongImpl() {
            Object val = this.nextValue();
            if (val instanceof Integer) {
                return ((Integer) val);
            }
            return ((Long) val);
        }
    }

    /**
     * Gaussian Distribution
     */
    public static class Gaussian extends DiscreteRNG {
        private static final long serialVersionUID = 1L;

        public Gaussian(Random random, long min, long max) {
            super(random, min, max);
        }

        @Override
        protected long nextLongImpl() {
            int value = -1;
            while (value < 0 || value >= this.range_size) {
                double gaussian = (this.random.nextGaussian() + 2.0) / 4.0;
                value = (int) Math.round(gaussian * this.range_size);
            }
            return (value + this.min);
        }
    }


    /**
     * Zipf distribution. The ratio of the probabilities of integer i and j is
     * defined as follows:
     * <p>
     * P(i)/P(j)=((j-min+1)/(i-min+1))^sigma.
     */
    public static class Zipf extends DiscreteRNG {
        private static final long serialVersionUID = 1L;
        private static final double DEFAULT_EPSILON = 0.001;
        private final ArrayList<Long> k;
        private final ArrayList<Double> v;

        /**
         * Constructor
         *
         * @param r     The random number generator.
         * @param min   minimum integer (inclusvie)
         * @param max   maximum integer (exclusive)
         * @param sigma parameter sigma. (sigma > 1.0)
         */
        public Zipf(Random r, long min, long max, double sigma) {
            this(r, min, max, sigma, DEFAULT_EPSILON);
        }

        /**
         * Constructor.
         *
         * @param r       The random number generator.
         * @param min     minimum integer (inclusvie)
         * @param max     maximum integer (exclusive)
         * @param sigma   parameter sigma. (sigma > 1.0)
         * @param epsilon Allowable error percentage (0 < epsilon < 1.0).
         */
        public Zipf(Random r, long min, long max, double sigma, double epsilon) {
            super(r, min, max);
            if ((max <= min) || (sigma <= 1) || (epsilon <= 0) || (epsilon >= 0.5)) {
                throw new IllegalArgumentException("Invalid arguments [min=" + min + ", max=" + max + ", sigma=" + sigma + ", epsilon=" + epsilon + "]");
            }
            k = new ArrayList<>();
            v = new ArrayList<>();

            double sum = 0;
            long last = -1;
            for (long i = min; i < max; ++i) {
                sum += Math.exp(-sigma * Math.log(i - min + 1));
                if ((last == -1) || i * (1 - epsilon) > last) {
                    k.add(i);
                    v.add(sum);
                    last = i;
                }
            }

            if (last != max - 1) {
                k.add(max - 1);
                v.add(sum);
            }

            v.set(v.size() - 1, 1.0);

            for (int i = v.size() - 2; i >= 0; --i) {
                v.set(i, v.get(i) / sum);
            }
        }


        /**
         * @see DiscreteRNG#nextInt()
         */
        @Override
        protected long nextLongImpl() {
            double d = random.nextDouble();
            int idx = Collections.binarySearch(v, d);

            if (idx > 0) {
                ++idx;
            } else {
                idx = -(idx + 1);
            }

            if (idx >= v.size()) {
                idx = v.size() - 1;
            }

            if (idx == 0) {
                return k.get(0);
            }

            long ceiling = k.get(idx);
            long lower = k.get(idx - 1);

            return ceiling - DiscreteRNG.nextLong(random, ceiling - lower);
        }
    }

}
