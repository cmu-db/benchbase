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

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.InputMismatchException;
import java.util.Locale;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Random;
import java.util.Scanner;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.oltpbenchmark.benchmarks.linkbench.LinkBenchConfigError;
import com.oltpbenchmark.benchmarks.linkbench.LinkBenchConstants;
import com.oltpbenchmark.benchmarks.linkbench.utils.ConfigUtil;
import com.oltpbenchmark.benchmarks.linkbench.utils.InvertibleShuffler;


/*
 * This class simulates the real distribution based on statistical data.
 */

public class RealDistribution extends PiecewiseLinearDistribution {

  /** The locale used for number formats, etc in distribution file */
  private static final Locale INPUT_FILE_LOCALE = Locale.ENGLISH;

  public static final String DISTRIBUTION_CONFIG = "realdist";
  private static final Logger logger =
                      Logger.getLogger(ConfigUtil.LINKBENCH_LOGGER);
  /* params to shuffler for link degree */
  public static final long NLINKS_SHUFFLER_SEED = 20343988438726021L;
  public static final int NLINKS_SHUFFLER_GROUPS = 1024;

  /* shufflers to generate distributions uncorrelated to above */
  public static final long UNCORR_SHUFFLER_SEED = 53238253823453L;
  public static final int UNCORR_SHUFFLER_GROUPS = 1024;

  /* Shufflers for requests that are correlated with link degree */
  public static final long WRITE_CORR_SHUFFLER_SEED = NLINKS_SHUFFLER_SEED;
  public static final int WRITE_CORR_SHUFFLER_GROUPS = NLINKS_SHUFFLER_GROUPS;
  public static final long READ_CORR_SHUFFLER_SEED = NLINKS_SHUFFLER_SEED;
  public static final int READ_CORR_SHUFFLER_GROUPS = NLINKS_SHUFFLER_GROUPS;

  /* Shufflers for requests that are uncorrelated with link degree */
  public static final long WRITE_UNCORR_SHUFFLER_SEED = UNCORR_SHUFFLER_SEED;
  public static final int WRITE_UNCORR_SHUFFLER_GROUPS = UNCORR_SHUFFLER_GROUPS;
  public static final long READ_UNCORR_SHUFFLER_SEED = UNCORR_SHUFFLER_SEED;
  public static final int READ_UNCORR_SHUFFLER_GROUPS = UNCORR_SHUFFLER_GROUPS;

  public static final long NODE_READ_SHUFFLER_SEED = 4766565305853767165L;
  public static final int NODE_READ_SHUFFLER_GROUPS = 1024;
  public static final long NODE_UPDATE_SHUFFLER_SEED = NODE_READ_SHUFFLER_SEED;
  public static final int NODE_UPDATE_SHUFFLER_GROUPS =
                                                    NODE_READ_SHUFFLER_GROUPS;
  public static final long NODE_DELETE_SHUFFLER_SEED = NODE_READ_SHUFFLER_SEED;
  public static final int NODE_DELETE_SHUFFLER_GROUPS =
                                                    NODE_READ_SHUFFLER_GROUPS;

  public static enum DistributionType {
    LINKS,
    LINK_READS,
    LINK_READS_UNCORR,
    LINK_WRITES,
    LINK_WRITES_UNCORR,
    NODE_READS,
    NODE_UPDATES,
    NODE_DELETES,
  }

  private DistributionType type = null;

  public RealDistribution() {
    this.type = null;
  }

  @Override
  public void init(long min, long max, Properties props, String keyPrefix) {
    this.min = min;
    this.max = max;
    String dist = ConfigUtil.getPropertyRequired(props,
                              keyPrefix + DISTRIBUTION_CONFIG);

    DistributionType configuredType;
    if (dist.equals("link_reads")) {
      configuredType = DistributionType.LINK_READS;
    } else if (dist.equals("link_writes")) {
      configuredType = DistributionType.LINK_WRITES;
    } else if (dist.equals("node_reads")) {
      configuredType = DistributionType.NODE_READS;
    } else if (dist.equals("node_writes")) {
      configuredType = DistributionType.NODE_UPDATES;
    } else if (dist.equals("links")) {
      configuredType = DistributionType.LINKS;
    } else {
      throw new RuntimeException("Invalid distribution type for "
          + "RealDistribution: " + dist);
    }

    init(props, min, max, configuredType);
  }

  /*
   * Initialize this with one of the empirical distribution types
   * This will automatically load the data file if needed
   */
  public void init(Properties props, long min, long max,
                                              DistributionType type) {
    loadOneShot(props);
    switch (type) {
    case LINKS:
      init(min, max, nlinks_cdf, null, null, nlinks_expected_val);
      break;
    case LINK_WRITES:
      init(min, max, link_nwrites_cdf, nwrites_cs, nwrites_right_points,
                                              link_nwrites_expected_val);
      break;
    case LINK_READS:
      init(min, max, link_nreads_cdf, link_nreads_cs, link_nreads_right_points,
                                              link_nreads_expected_val);
      break;
    case NODE_UPDATES:
      init(min, max, node_nwrites_cdf, nwrites_cs, nwrites_right_points,
                                              node_nwrites_expected_val);
      break;
    case NODE_READS:
      init(min, max, node_nreads_cdf, node_nreads_cs, node_nreads_right_points,
                                              node_nreads_expected_val);
      break;
    default:
      throw new RuntimeException("Unknown distribution type: " + type);
    }
  }

  private static ArrayList<Point> nlinks_cdf, link_nreads_cdf, link_nwrites_cdf,
                  node_nreads_cdf, node_nwrites_cdf;
  private static double[] link_nreads_cs, nwrites_cs, node_nreads_cs, node_nwrites_cs;
  /**
   * These right_points arrays are used to keep track of state of
   * the id1 generation, with each cell holding the next id to
   * return.  These are shared between RealDistribution instances
   * and different threads.
   *
   * It is not clear that this works entirely as intended and it
   * certainly is non-deterministic when multiple threads are
   * involved.
   */
  private static long[] link_nreads_right_points, nwrites_right_points,
                        node_nreads_right_points, node_nwrites_right_points;
  private static double nlinks_expected_val, link_nreads_expected_val, link_nwrites_expected_val,
                        node_nreads_expected_val, node_nwrites_expected_val;

  /*
   * This method loads data from data file into memory;
   * must be called before any getNlinks or getNextId1s;
   * must be declared as synchronized method to prevent race condition.
   */
  public static synchronized void loadOneShot(Properties props) {
    if (nlinks_cdf == null) {
      try {
        getStatisticalData(props);
      } catch (FileNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /*
   * This method get the area below the distribution nreads_ccdf or
   * nwrite_ccdf. This helps to determine the number of nreads after which
   * the generating distribution would be approximately equal to real
   * distribution.
   *
   * Keep in mind the because the number of id1s is constant, the
   * generating #reads distribution keeps changing. It starts at "100% 0",
   * keeps growing and eventually at some point (after certain number of
   * reads) it should be equal to the real #reads distribution.
   *
   * Because the number of id1s is constant (equal to maxid1 - startid1),
   * the total number of reads is also a constant, according to the
   * following fomular:
   *
   * (number of reads) = (number of id1s) x (area below nreads_pdf)
   *
   * To illustrate, consider the following nreads_pdf distribution:
   * 60%=0; 20%=1; 10%=2; 10%=3; and there are 100 id1s.
   *
   * The number of reads would be a constanst:
   * 100 * (20% * 1 + 10% * 2 + 10% * 3) = 100 * 80%.
   * The multiplication factor (20% * 1 + 10% * 2 + 10% * 3) is what we
   * want this method to return.
   *
   * If we already have the ccdf (comlementary cumulative distribution
   * function): 40%>=1; 20%>=2; 10%>=3; and its cumulative sum:
   * [40%, 40%+20%, 40%+20%+10%] = [40%, 60%, 80%], then just need to
   * return the last cumulative sum (80%).
   */
  static double getArea(DistributionType type) {
    if (type == DistributionType.LINK_READS)
        return link_nreads_cs[link_nreads_cs.length - 1];
    else if (type == DistributionType.LINK_WRITES)
        return nwrites_cs[nwrites_cs.length - 1];
    else return 0;
  }


  //helper function:
  private static ArrayList<Point> readCDF(String filePath, Scanner scanner) {
    ArrayList<Point> points = new ArrayList<Point>();
    while (scanner.hasNextInt()) {
      int value = scanner.nextInt();
      // File on disk has percentages
      try {
        double percent = scanner.nextDouble();
        double probability = percent / 100;
        Point temp = new Point(value, probability);
        points.add(temp);
      } catch (InputMismatchException ex) {
        throw new LinkBenchConfigError("Expected to find floating point "
            + "value in input file" + filePath + " but found token \""
            + scanner.next() + "\"");
      } catch (NoSuchElementException ex) {
        throw new LinkBenchConfigError("Expected to find floating point "
            + "value in input file" + filePath + " but found end of file");
      }
    }
    return points;
  }

  //convert CDF from ArrayList<Point> to Map
  static NavigableMap<Integer, Double> getCDF(DistributionType dist) {
    ArrayList<Point> points =
      dist == DistributionType.LINKS ? nlinks_cdf :
      dist == DistributionType.LINK_READS? link_nreads_cdf :
      dist == DistributionType.LINK_WRITES ? link_nwrites_cdf :
      dist == DistributionType.NODE_READS ? node_nreads_cdf :
      dist == DistributionType.NODE_UPDATES ? node_nwrites_cdf :
                                                          null;
    if (points == null) return null;

    TreeMap<Integer, Double> map = new TreeMap<Integer, Double>();
    for (Point point : points) {
      map.put(point.value, point.probability);
    }
    return map;
  }

  /*
   * This method reads from data_file nlinks, nreads, nwrites discreate
   * cumulative distribution function (CDF) and produces corresponding
   * pdf and ccdf.
   *
   * The data file is generated by LinkBenchConfigGenerator, and can be
   * located by parameter data_file in the config file.
   *
   * CDF is returned under the form of an array whose elements are pairs of
   * value and the cumulative distribution at that value i.e. <x, CDF(x)>.
   */
  private static void getStatisticalData(Properties props) throws FileNotFoundException {
    String filename = ConfigUtil.getPropertyRequired(props,
                            LinkBenchConstants.DISTRIBUTION_DATA_FILE);

    // If relative path, should be relative to linkbench home directory
//    String fileAbsPath;
//    if (new File(filename).isAbsolute()) {
//      fileAbsPath = filename;
//    } else {
//      String linkBenchHome = ConfigUtil.findLinkBenchHome();
//      if (linkBenchHome == null) {
//        throw new RuntimeException("Data file config property "
//            + LinkBenchConstants.DISTRIBUTION_DATA_FILE
//            + " was specified using a relative path, but linkbench home"
//            + " directory was not specified through environment var "
//            + ConfigUtil.linkbenchHomeEnvVar);
//      } else {
//        fileAbsPath = linkBenchHome + File.separator + filename;
//      }
//    }

    logger.info("Loading real distribution data from " + filename);

    Scanner scanner = new Scanner(new File(filename));
    scanner.useLocale(INPUT_FILE_LOCALE);
    while (scanner.hasNext()) {
      String type = scanner.next();
      if (type.equals("nlinks")) {
        nlinks_cdf = readCDF(filename, scanner);
        nlinks_expected_val = expectedValue(nlinks_cdf);
      }
      else if (type.equals("link_nreads")) {
        link_nreads_cdf = readCDF(filename, scanner);
        double[] nreads_pdf = getPDF(link_nreads_cdf);
        double[] nreads_ccdf = getCCDF(nreads_pdf);
        link_nreads_cs = getCumulativeSum(nreads_ccdf);

        link_nreads_right_points = new long[link_nreads_cs.length];
        for (int i = 0; i < link_nreads_right_points.length; ++i) {
          link_nreads_right_points[i] = 0;
        }
        link_nreads_expected_val = expectedValue(link_nreads_cdf);
      }
      else if (type.equals("link_nwrites")) {
        link_nwrites_cdf = readCDF(filename, scanner);
        double[] nwrites_pdf = getPDF(link_nwrites_cdf);
        double[] nwrites_ccdf = getCCDF(nwrites_pdf);
        nwrites_cs = getCumulativeSum(nwrites_ccdf);

        nwrites_right_points = new long[nwrites_cs.length];
        for (int i = 0; i < nwrites_right_points.length; ++i) {
          nwrites_right_points[i] = 0;
        }
        link_nwrites_expected_val = expectedValue(link_nwrites_cdf);
      } else if (type.equals("node_nreads")) {
        node_nreads_cdf = readCDF(filename, scanner);
        double[] node_nreads_pdf = getPDF(node_nreads_cdf);
        double[] node_nreads_ccdf = getCCDF(node_nreads_pdf);
        node_nreads_cs = getCumulativeSum(node_nreads_ccdf);

        node_nreads_right_points = new long[node_nreads_cs.length];
        for (int i = 0; i < node_nreads_right_points.length; ++i) {
          node_nreads_right_points[i] = 0;
        }
        node_nreads_expected_val = expectedValue(node_nreads_cdf);
      }
      else if (type.equals("node_nwrites")) {
        node_nwrites_cdf = readCDF(filename, scanner);
        double[] node_nwrites_pdf = getPDF(node_nwrites_cdf);
        double[] node_nwrites_ccdf = getCCDF(node_nwrites_pdf);
        node_nwrites_cs = getCumulativeSum(node_nwrites_ccdf);

        node_nwrites_right_points = new long[node_nwrites_cs.length];
        for (int i = 0; i < node_nwrites_right_points.length; ++i) {
          node_nwrites_right_points[i] = 0;
        }
        node_nwrites_expected_val = expectedValue(node_nwrites_cdf);
      } else {
        throw new RuntimeException("Unexpected token in distribution file, "
                  + "expected name of next distribution: \"" + type + "\"");
      }
    }
  }

  static long getNlinks(long id1, long startid1, long maxid1) {
    // simple workload balancing
    return (long)expectedCount(startid1, maxid1, id1, nlinks_cdf);
  }

  @Override
  public long choose(Random rng) {
    if (type == DistributionType.LINKS) {
      throw new RuntimeException("choose not supported for LINKS");
    }
    return super.choose(rng);
  }

  public static InvertibleShuffler getShuffler(DistributionType type, long n) {
    switch (type) {
    case LINK_READS:
      return new InvertibleShuffler(READ_CORR_SHUFFLER_SEED,
            READ_CORR_SHUFFLER_GROUPS, n);
    case LINK_READS_UNCORR:
      return new InvertibleShuffler(READ_UNCORR_SHUFFLER_SEED,
            READ_UNCORR_SHUFFLER_GROUPS, n);
    case LINK_WRITES:
      return new InvertibleShuffler(WRITE_CORR_SHUFFLER_SEED,
          WRITE_CORR_SHUFFLER_GROUPS, n);
    case LINK_WRITES_UNCORR:
      return new InvertibleShuffler(WRITE_UNCORR_SHUFFLER_SEED,
          WRITE_UNCORR_SHUFFLER_GROUPS, n);
    case NODE_READS:
      return new InvertibleShuffler(NODE_READ_SHUFFLER_SEED,
          NODE_READ_SHUFFLER_GROUPS, n);
    case NODE_UPDATES:
      return new InvertibleShuffler(NODE_UPDATE_SHUFFLER_SEED,
          NODE_UPDATE_SHUFFLER_GROUPS, n);
    case NODE_DELETES:
      return new InvertibleShuffler(NODE_DELETE_SHUFFLER_SEED,
          NODE_DELETE_SHUFFLER_GROUPS, n);
    case LINKS:
      return new InvertibleShuffler(NLINKS_SHUFFLER_SEED,
          NLINKS_SHUFFLER_GROUPS, n);
    default:
      return null;
    }
  }
}

