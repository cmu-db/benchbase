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

import org.apache.log4j.Logger;

import com.oltpbenchmark.benchmarks.linkbench.distributions.RealDistribution.DistributionType;
import com.oltpbenchmark.benchmarks.linkbench.LinkBenchConfigError;
import com.oltpbenchmark.benchmarks.linkbench.LinkBenchConstants;
import com.oltpbenchmark.benchmarks.linkbench.utils.ConfigUtil;
import com.oltpbenchmark.util.ClassUtil;

public class LinkDistributions {
  public static interface LinkDistribution {
    public abstract long getNlinks(long id1);

    /**
     * Let caller know it should shuffle IDs
     * @return
     */
    public boolean doShuffle();
  }

  public static class ProbLinkDistribution implements LinkDistribution {
    private ProbabilityDistribution dist;

    public ProbLinkDistribution(ProbabilityDistribution dist) {
      this.dist = dist;
    }

    @Override
    public long getNlinks(long id1) {
      return (long) Math.round( dist.expectedCount(id1));
    }

    /** shuffle, otherwise ids will be in order of most to least ids */
    @Override
    public boolean doShuffle() {
      return true;
    }
  }

  /**
   * Built-in distributions
   */
  public static enum LinkDistMode {
    REAL, // observed distribution
    CONST, // Constant value
    RECIPROCAL, // 1/x
    MULTIPLES, // boost multiples of param
    PERFECT_SQUARES,
    EXPONENTIAL
  }

  /**
   * Some link distributions using arithmetic tricks
   */
  public static class ArithLinkDistribution implements LinkDistribution {

    private LinkDistMode mode;
    private long nlinks_config;
    private long nlinks_default;

    private long minid1, maxid1;

    public ArithLinkDistribution(long minid1, long maxid1, LinkDistMode mode,
        long nlinks_config, long nlinks_default) {
      this.minid1 = minid1;
      this.maxid1 = maxid1;
      this.mode = mode;
      this.nlinks_config = nlinks_config;
      this.nlinks_default = nlinks_default;
    }

    /**
     * Gets the #links to generate for an id1 based on distribution specified by
     * nlinks_func, nlinks_config
     */
    @Override
    public long getNlinks(long id1) {
      switch (mode) {
      case CONST:
        // Constant
        return nlinks_default;
      case RECIPROCAL:
        // Corresponds to function 1/x
        long n = maxid1 - minid1;
        long off = id1 - minid1;
        return nlinks_default
            + (long) Math.ceil((double) n / (double) off);
      case MULTIPLES:
        // if id1 is multiple of nlinks_config, then add nlinks_config
        return nlinks_default + (id1 % nlinks_config == 0 ? nlinks_config : 0);
      case EXPONENTIAL:
        // Corresponds to exponential distribution
        // If id1 is nlinks_config^k, then add
        // nlinks_config^k - nlinks_config^(k-1) more links
        long log = (long) Math.ceil(Math.log(id1) / Math.log(nlinks_config));
        long temp = (long) Math.pow(nlinks_config, log);
        return nlinks_default
            + (temp == id1 ? (id1 - (long) Math.pow(nlinks_config, log - 1))
                : 0);

      case PERFECT_SQUARES:
        // if nlinks_func is 2 then
        // if id1 is K * K, then add K * K - (K - 1) * (K - 1) more links.
        // The idea is to give more #links to perfect squares. The larger
        // the perfect square is, the more #links it will get.
        // Generalize the above for nlinks_func is n:
        // if id1 is K^n, then add K^n - (K - 1)^n more links
        long nthroot = (long) Math.ceil(Math.pow(id1, (1.0) / nlinks_config));
        long temp2 = (long) Math.pow(nthroot, nlinks_config);
        return nlinks_default += (temp2 == id1 ? (id1 - (long) Math.pow(
            nthroot - 1, nlinks_config)) : 0);
      default:
        throw new RuntimeException("Unknown mode: " + mode);
      }
    }

    @Override
    public boolean doShuffle() {
      // don't shuffle: these methods already randomize order by design
      return false;
    }
  }

  public static LinkDistribution loadLinkDistribution(Properties props,
      long minid1, long maxid1) {
    Logger logger = Logger.getLogger(ConfigUtil.LINKBENCH_LOGGER);
    String nlinks_func; // distribution function for #links

    nlinks_func = ConfigUtil.getPropertyRequired(props, LinkBenchConstants.NLINKS_FUNC);

    // We have built-in versions defined by LinkDistMode, and also support
    // dynamic loading of ProbabilityDistribution instances
    LinkDistMode mode;
    try {
      // Try to see if it is built-in
      mode = LinkDistMode.valueOf(nlinks_func.toUpperCase());
    } catch (IllegalArgumentException ex) {
      // If not built-in, assume it's a class name
      return tryDynamicLoad(nlinks_func, props, minid1, maxid1);
    }

    // real distribution has it own initialization
    if (mode == LinkDistMode.REAL) {
      logger.debug("Using real link distribution");
      RealDistribution realDist = new RealDistribution();
      realDist.init(props, minid1, maxid1, DistributionType.LINKS);
      return new ProbLinkDistribution(realDist);
    } else {
      // Various arithmetic modes
      // an additional parameter for the function
      int nlinks_config = ConfigUtil.getInt(props, LinkBenchConstants.NLINKS_CONFIG);
      // minimum #links - expected to be 0 or 1
      int nlinks_default = ConfigUtil.getInt(props, LinkBenchConstants.NLINKS_DEFAULT);
      logger.debug("Using built-in arithmetic link distribution " + mode
                    + " with default #links " + nlinks_config + " and "
                    + " config parameter " + nlinks_config);
      return new ArithLinkDistribution(minid1, maxid1, mode, nlinks_config,
          nlinks_default);

      // throw new LinkBenchConfigError("Unknown setting for links function: " +
      // nlinks_func);
    }
  }

  /**
   * Try to dynamically load a ProbabilityDistribution class
   * @param className
   * @param props
   * @param minid1
   * @param maxid1
   * @return
   */
  private static LinkDistribution tryDynamicLoad(String className,
      Properties props, long minid1, long maxid1) {
    try {
      Logger logger = Logger.getLogger(ConfigUtil.LINKBENCH_LOGGER);
      logger.debug("Using LinkDistribution class " + className);
      ProbabilityDistribution pDist = ClassUtil.newInstance(className,
                                                ProbabilityDistribution.class);
      pDist.init(minid1, maxid1, props, LinkBenchConstants.NLINKS_PREFIX);
      return new ProbLinkDistribution(pDist);
    } catch (ClassNotFoundException e) {
      throw new LinkBenchConfigError("Link distribution class " + className
          + " not successfully loaded: " + e.getMessage());
    }
  }

}
