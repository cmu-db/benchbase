/*
 * Copyright 2012, Facebook, Inc.
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
 */
package com.oltpbenchmark.benchmarks.linkbench.distributions;

import java.util.Properties;
import java.util.Random;

import com.oltpbenchmark.benchmarks.linkbench.LinkBenchConstants;
import com.oltpbenchmark.benchmarks.linkbench.utils.ConfigUtil;


/**
 * Uniform distribution over integers in range [minID, maxID),
 * where minID is included in range and maxID excluded
 *
 */
public class UniformDistribution implements ProbabilityDistribution {

  private long min = 0;
  private long max = 1;
  private double scale = 1.0;

  public void init(long min, long max, Properties props, String keyPrefix) {
    if (max <= min) {
      throw new IllegalArgumentException("max = " + max + " <= min = " + min +
          ": probability distribution cannot have zero or negative domain");
    }
    this.min = min;
    this.max = max;
    if (props != null && props.containsKey(keyPrefix + LinkBenchConstants.PROB_MEAN)) {
      scale = (max - min) * ConfigUtil.getDouble(props,
                                  keyPrefix + LinkBenchConstants.PROB_MEAN);
    } else {
      scale = 1.0;
    }
  }

  public void init(long min, long max, double scale) {
    this.min = min;
    this.max = max;
    this.scale = scale;
  }

  @Override
  public double pdf(long id) {
    return scaledPDF(id, 1.0);
  }

  @Override
  public double expectedCount(long id) {
    return scaledPDF(id, scale);
  }

  private double scaledPDF(long id, double scale) {
    // Calculate this way to avoid losing precision by calculating very
    // small pdf number
    if (id < min || id >= max) return 0.0;
    return scale / (double) (max - min);
  }

  /**
   * Cumulative distribution function for distribution
   * @param id
   * @return
   */
  public double cdf(long id) {
    if (id >= max) {
      return 1.0;
    }
    if (id < min) {
      return 0.0;
    }
    long n = max - min;
    long rank = id - min + 1;

    return rank / (double)n;
  }

  /**
   * Quantile function
   */
  public long quantile(double p) {
    assert(p >= 0.0 && p <= 1.0);
    long n = max - min;
    long i = (long)Math.floor(p * n);
    if (i == n) return max - 1;
    return i + min;
  }

  // Total number of representable numbers by int
  private static final long UINT_RANGE = Integer.MAX_VALUE - (long) Integer.MIN_VALUE;

  /** Choose an id X uniformly in the range*/
  public long choose(Random rng) {
    long n = max - min;
    // Java's random number generator has less randomness in lower bits
    // so just taking a mod doesn't give a good quality result.
    if (n <= Integer.MAX_VALUE) {
      return min + (long)rng.nextInt((int)n);
    } else if (n < UINT_RANGE) {
      return randint2(rng, n);
    } else {
      return UINT_RANGE * rng.nextInt((int)(n / UINT_RANGE)) +
                          randint2(rng, n % UINT_RANGE);
    }
  }

  /**
   * Produce a random integer in range [0, n]
   * n must be in range [0, MAX_INT - MIN_INT]
   * @param rng
   * @param n
   * @return
   */
  private long randint2(Random rng, long n) {
    assert(n < UINT_RANGE);
    double p = Integer.MAX_VALUE / (double)n;
    if (rng.nextDouble() < p) {
      return rng.nextInt(Integer.MAX_VALUE);
    } else {
      return Integer.MAX_VALUE +
          (long)(rng.nextInt((int)(n - Integer.MAX_VALUE)));
    }
  }
}
