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

import org.apache.commons.math3.util.FastMath;

import com.oltpbenchmark.benchmarks.linkbench.LinkBenchConstants;
import com.oltpbenchmark.benchmarks.linkbench.utils.ConfigUtil;

/**
 * Geometric distribution
 *
 * NOTE: this generates values in the range [min, max).  Since the
 * real geometric distribution generates values in range [min, inf),
 * we truncate anything >= max
 */
public class GeometricDistribution implements ProbabilityDistribution {

  /** The probability parameter that defines the distribution */
  private double p = 0.0;

  /** Valid range */
  private long min = 0, max = 0;

  private double scale = 0.0;

  public static final String PROB_PARAM_KEY = "prob";

  @Override
  public void init(long min, long max, Properties props, String keyPrefix) {
    double parsedP = ConfigUtil.getDouble(props, keyPrefix + PROB_PARAM_KEY);

    double scaleVal = 1.0;;
    if (props.containsKey(LinkBenchConstants.PROB_MEAN)) {
      scaleVal = (max - min) * ConfigUtil.getDouble(props,
                            keyPrefix + LinkBenchConstants.PROB_MEAN);
    }
    init(min, max, parsedP, scaleVal);
  }

  public void init(long min, long max, double p, double scale) {
    this.min = min;
    this.max = max;
    this.p = p;
    this.scale = scale;
  }

  @Override
  public double pdf(long id) {
    return scaledPdf(id, 1.0);
  }

  @Override
  public double expectedCount(long id) {
    return scaledPdf(id, scale);
  }

  private double scaledPdf(long id, double scaleFactor) {
    if (id < min || id >= max) return 0.0;
    long x = id - min;
    return FastMath.pow(1 - p, x) * scaleFactor * p;
  }

  @Override
  public double cdf(long id) {
    if (id < min) return 0.0;
    if (id >= max) return 1.0;
    return 1 - FastMath.pow(1 - p, id - min + 1);
  }

  @Override
  public long choose(Random rng) {
    return quantile(rng.nextDouble());
  }

  @Override
  public long quantile(double r) {
    /*
     * Quantile function for geometric distribution over
     * range [0, inf) where 0 < r < 1
     * quantile(r) = ceiling(ln(1 - r) / ln (1 - p))
     * Source: http://www.math.uah.edu/stat/bernoulli/Geometric.html
     */
    if (r == 0.0) return min; // 0.0 must be handled specially

    long x = min + (long)FastMath.ceil(
            FastMath.log(1 - r) / FastMath.log(1 - p));
    // truncate over max
    return Math.min(x, max - 1);
  }

}
