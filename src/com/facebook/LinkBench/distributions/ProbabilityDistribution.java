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
package com.facebook.LinkBench.distributions;

import java.util.Properties;
import java.util.Random;

/**
 * Probability distribution over a range of integers [min, max), ranked
 * in descending order of probability, where min is most probable and
 * max - 1 is least probable.
 * @author tarmstrong
 *
 */
public interface ProbabilityDistribution {

  /**
   * Initialize probability distribution for range [min, max) with additional
   * parameters pulled from properties dictionary by implementation.
   * @param min
   * @param max
   * @param props Properties dictionary for any extra parameters
   * @param keyPrefix In case there are multiple distributions with
   *          different parameters in properties, this prefix can be
   *          provided to distinguish when looking up keys
   */
  public abstract void init(long min, long max,
                          Properties props, String keyPrefix);

  /**
   * Probability density function, i.e. P(X = id)
   * @param id
   * @return
   */
  public abstract double pdf(long id);

  /**
   * Probability density function scaled by an implementation-defined
   * factor (e.g. the number of trials, giving the expected number of values)
   * @param id
   * @return
   */
  public abstract double expectedCount(long id);

  /**
   * Cumulative distribution function, i.e. for a random variable
   * X chosen accord to the distribution P(X <= id).
   * E.g. cdf(min - 1) = 0.0, and cdf(max - 1) = 1.0
   * @param id
   * @return a probability in range [0.0, 1.0]
   */
  public abstract double cdf(long id);

  /**
   * Choose a random id in range [min, max) according to the probability
   * distribution.
   * @param rng a random number generator to use for random choice
   * @return the chosen id
   */
  public abstract long choose(Random rng);


  /**
   * Quantile function for the distribution
   * @return x such that Pr(X <= x) = p
   */
  public abstract long quantile(double p);
}
