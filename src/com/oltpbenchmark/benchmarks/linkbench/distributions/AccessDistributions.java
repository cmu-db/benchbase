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

import org.apache.log4j.Logger;

import com.oltpbenchmark.benchmarks.linkbench.distributions.RealDistribution.DistributionType;
import com.oltpbenchmark.benchmarks.linkbench.LinkBenchConfigError;
import com.oltpbenchmark.benchmarks.linkbench.LinkBenchConstants;
import com.oltpbenchmark.benchmarks.linkbench.utils.ConfigUtil;
import com.oltpbenchmark.benchmarks.linkbench.utils.InvertibleShuffler;
import com.oltpbenchmark.util.ClassUtil;



/**
 * Module for id access patterns that allows different implementations
 * of the AccessDistribution interface to be instantiated for configurable
 * access patterns.
 * @author tarmstrong
 *
 */
public class AccessDistributions {
  public interface AccessDistribution {
    /**
     * Choose the next id to be accessed
     * @param rng random number generator
     * @param previousId previous ID (for stateful generators)
     * @return
     */
    public abstract long nextID(Random rng, long previousId);

    /**
     * A shuffler to shuffle the results, or
     * null if the results shouldn't be shuffled
     * @return
     */
    public abstract InvertibleShuffler getShuffler();
  }

  public static class BuiltinAccessDistribution implements AccessDistribution {
    private AccessDistMode mode;
    protected long minid;
    protected long maxid;
    private long config;

    /** Use to generate decent quality random longs in range */
    UniformDistribution uniform;

    public BuiltinAccessDistribution(AccessDistMode mode,
                            long minid, long maxid, long config) {
      this.mode = mode;
      this.minid = minid;
      this.maxid = maxid;
      this.config = config;
      uniform = new UniformDistribution();
      uniform.init(minid, maxid, null, null);
    }

    @Override
    public long nextID(Random rng, long previousid) {
      long newid;
      double drange = (double)(maxid - minid);

      switch(mode) {
      case ROUND_ROBIN: //sequential from startid1 to maxid1 (circular)
        if (previousid <= minid) {
          newid = minid;
        } else {
          newid = previousid+1;
          if (newid >= maxid) {
            newid = minid;
          }
        }
        break;

      case RECIPROCAL: // inverse function f(x) = 1/x.
        newid = (long)(Math.ceil(drange/uniform.choose(rng)));
        if (newid < minid) newid = minid;
        if (newid >= maxid) newid = maxid;
        break;
      case MULTIPLE: // generate id1 that is even multiple of config
        newid = config * (long)(Math.ceil(uniform.choose(rng)/config));
        break;
      case POWER: // generate id1 that is power of config
        double log = Math.ceil(Math.log(uniform.choose(rng))/Math.log(config));
        newid = Math.min(maxid - 1, (long)Math.pow(config, log));
        break;
      case PERFECT_POWER: // generate id1 that is perfect square if config is 2,
        // perfect cube if config is 3 etc
        // get the nth root where n = distrconfig
        long nthroot = (long)Math.ceil(Math.pow(uniform.choose(rng), (1.0)/config));
        // get nthroot raised to power n
        newid = Math.min(maxid - 1, (long)Math.pow(nthroot, config));
        break;
      default:
        throw new RuntimeException("Unknown access dist mode: " + mode);
      }
      return newid;
    }

    @Override
    public InvertibleShuffler getShuffler() {
      // Don't shuffle these distributions
      return null;
    }
  }

  public static class ProbAccessDistribution implements AccessDistribution {
    private final ProbabilityDistribution dist;
    private InvertibleShuffler shuffler;

    public ProbAccessDistribution(ProbabilityDistribution dist,
                                  InvertibleShuffler shuffler) {
      super();
      this.dist = dist;
      this.shuffler = shuffler;
    }

    @Override
    public long nextID(Random rng, long previousId) {
      return dist.choose(rng);
    }

    @Override
    public InvertibleShuffler getShuffler() {
      return shuffler;
    }

  }

  public static enum AccessDistMode {
    REAL, // Real empirical distribution
    ROUND_ROBIN, // Cycle through ids
    RECIPROCAL, // Pick with probability
    MULTIPLE, // Pick a multiple of config parameter
    POWER, // Pick a power of config parameter
    PERFECT_POWER // Pick a perfect power (square, cube, etc) with exponent
                  // as configured
  }

  public static AccessDistribution loadAccessDistribution(Properties props,
      long minid, long maxid, DistributionType kind) throws LinkBenchConfigError {
    Logger logger = Logger.getLogger(ConfigUtil.LINKBENCH_LOGGER);
    String keyPrefix;
    switch(kind) {
    case LINK_READS:
      keyPrefix = LinkBenchConstants.READ_CONFIG_PREFIX;
      break;
    case LINK_READS_UNCORR:
      keyPrefix = LinkBenchConstants.READ_UNCORR_CONFIG_PREFIX;
      break;
    case LINK_WRITES:
      keyPrefix = LinkBenchConstants.WRITE_CONFIG_PREFIX;
      break;
    case LINK_WRITES_UNCORR:
      keyPrefix = LinkBenchConstants.WRITE_UNCORR_CONFIG_PREFIX;
      break;
    case NODE_READS:
      keyPrefix = LinkBenchConstants.NODE_READ_CONFIG_PREFIX;
      break;
    case NODE_UPDATES:
      keyPrefix = LinkBenchConstants.NODE_UPDATE_CONFIG_PREFIX;
      break;
    case NODE_DELETES:
      keyPrefix = LinkBenchConstants.NODE_DELETE_CONFIG_PREFIX;
      break;
    default:
      throw new RuntimeException("Bad kind " + kind);
    }

    String func_key = keyPrefix + LinkBenchConstants.ACCESS_FUNCTION_SUFFIX;
    String access_func = ConfigUtil.getPropertyRequired(props, func_key);

    try {
      AccessDistMode mode = AccessDistMode.valueOf(access_func.toUpperCase());

      if (mode == AccessDistMode.REAL) {
        RealDistribution realDist = new RealDistribution();
        realDist.init(props, minid, maxid, kind);
        InvertibleShuffler shuffler = RealDistribution.getShuffler(kind,
                                                    maxid - minid);
        logger.debug("Using real access distribution" +
                     " for " + kind.toString().toLowerCase());
        return new ProbAccessDistribution(realDist, shuffler);
      } else  {
        String config_key = keyPrefix + LinkBenchConstants.ACCESS_CONFIG_SUFFIX;
        long config_val = ConfigUtil.getLong(props, config_key);
        logger.debug("Using built-in access distribution " + mode +
                    " with config param " + config_val +
                    " for " + kind.toString().toLowerCase());
        return new BuiltinAccessDistribution(mode, minid, maxid, config_val);
      }
    } catch (IllegalArgumentException e) {
      return tryDynamicLoad(access_func, props, keyPrefix, minid, maxid, kind);
    }
  }

  /**
   *
   * @param className ProbabilityDistribution class name
   * @param props
   * @param keyPrefix prefix to use for looking up keys in props
   * @param minid
   * @param maxid
   * @return
   */
  private static AccessDistribution tryDynamicLoad(String className,
      Properties props, String keyPrefix, long minid, long maxid,
      DistributionType kind) {
    try {
      Logger logger = Logger.getLogger(ConfigUtil.LINKBENCH_LOGGER);
      logger.debug("Using ProbabilityDistribution class " + className +
                  " for " + kind.toString().toLowerCase());
      ProbabilityDistribution pDist = ClassUtil.newInstance(className,
                                                ProbabilityDistribution.class);
      pDist.init(minid, maxid, props, keyPrefix);
      InvertibleShuffler shuffler = RealDistribution.getShuffler(kind,
                                                       maxid - minid);
      return new ProbAccessDistribution(pDist, shuffler);
    } catch (ClassNotFoundException e) {
      throw new LinkBenchConfigError("Access distribution class " + className
          + " not successfully loaded: " + e.getMessage());
    }
  }
}
