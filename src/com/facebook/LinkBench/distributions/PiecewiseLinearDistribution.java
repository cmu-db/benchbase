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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

/**
 * A distribution where the cumulative density function is an arbitrary
 * piecewise linear function.
 *
 * Rather confusingly there are two possible ways of looking at the
 * distribution.  The first is to divide the keyspace by ids, and order
 * these IDs by the number of accesses.  Then DIST-A determines how likely
 * it is that that given key will be chosen.  The second is to divide the
 * keyspace into buckets, where there are multiple keys in each bucket which
 * have been accessed the same number of times.  There DIST-B determines how
 * likely a random key is to fall into each bucket.  The input data is
 * represented as DIST-B, but the probability distribution represented by
 * this class is DIST-A, so we need to convert from one representation to
 * another.
 *
 * The conversion process works as follows.
 * Suppose you have items numbered 0 to n - 1.  Then item i gets assigned
 * the percentile rank p = i / (n - 1), a number between 0 and 1.
 *
 * The input is a set of tuples (r, v), where v is the total number of
 * observations of the item at percentile p.  So the values of the are
 * denominated not in probability density, but rather in number of observation.
 *
 * This means that to convert the input to a probability density distribution,
 * we need to calculate the expected value of the distribution, and then divide
 * the value by that.
 *
 * This is an abstract class: the init method needs to be implemented
 * @author tarmstrong
 *
 */
public abstract class PiecewiseLinearDistribution implements ProbabilityDistribution {

  //helper class to store (value, probability)
  public static class Point implements Comparable<Point> {
    public int value;
    public double probability;

    public Point(int input_value, double input_probability) {
      this.value = input_value;
      this.probability = input_probability;
    }

    public int compareTo(Point obj) {
      Point p = (Point)obj;
      return this.value - p.value;
    }

    public String toString() {
      return "(" + value + ", " + probability + ")";
    }
  }

  protected void init(long min, long max, ArrayList<Point> cdf) {
    double pdf[] = getPDF(cdf);
    double ccdf[] = getCCDF(pdf);
    double cs[] = getCumulativeSum(ccdf);
    long right_points[] = new long[cs.length];
    init(min, max, cdf, cs, right_points, expectedValue(cdf));
  }

  /**
   * Init with precalculated values
   * @param min
   * @param max
   * @param cdf
   * @param cs
   * @param right_points
   * @param expectedValue
   */
  protected void init(long min, long max, ArrayList<Point> cdf,
        double cs[], long right_points[], double expectedValue) {
    this.min = min;
    this.max = max;
    this.cdf = cdf;
    this.cs = cs;
    this.right_points = right_points;
    this.expected_val = expectedValue;
  }

  protected long max;
  protected long min;
  protected ArrayList<Point> cdf;

  protected double[] cs;
  protected long[] right_points;

  /**
   * Total number of observations in data
   */
  private double expected_val;


  @Override
  public double pdf(long id) {
    long n = (max - min);
    double totalSum = expected_val * n;
    return expectedCount(id) / totalSum;
  }

  @Override
  public double expectedCount(long id) {
    return expectedCount(min, max, id, cdf);
  }

  public static double expectedCount(long min, long max, long id,
                ArrayList<Point> cdf) {
    if (id < min || id >= max) {
      return 0.0;
    }
    long n = (max - min);
    // Put in into range [0.0, 1.0] with most popular at 0.0
    double u = 1.0 - (id - min) / (double) n;
    int ix = binarySearch(cdf, u);
    Point p1 = cdf.get(ix);
    assert(u <= p1.probability);

    // Assuming piecewise linear, so equally as probably as p1.value
    return p1.value;
  }

  @Override
  public double cdf(long id) {
    // Since this should be the CDF function for DIST-A, rather
    // than DIST-B, it is non-trivial to calculate (requires some kind
    // of integration of DIST-B).
    throw new RuntimeException("Cdf not implemented yet");
  }

  @Override
  public long quantile(double p) {
    // This is not implemented, due to similar reasons to cdf
    throw new RuntimeException("Quantile not implemented yet");
  }

  @Override
  public long choose(Random rng) {
    return choose(rng, min, max, cs, right_points);
  }

  protected static long choose(Random rng, long startid1, long maxid1,
      double[] cs, long[] right_points) {
    double max_probability = cs[cs.length - 1];
    double p = max_probability * rng.nextDouble();

    int idx = binarySearch(cs, p);
    if (idx == 0) idx = 1;

    /*
     * TODO: this algorithm does not appear to generate data
     * faithful to the distribution.
     * Additional problems include data races if multiple threads are
     * concurrently modifying the shared arrays, and the fact
     * that a workload cannot be reproduced.
     */
    long result = right_points[idx] % (maxid1 - startid1);
    right_points[idx] = (result + 1) % (maxid1 - startid1);
    long id1 = startid1 + result;
    return id1;
  }

  /**
   * Get the expected value of the distribution (e.g. the
   * average number of links
   * @param cdf
   * @return
   */
  protected static double expectedValue(ArrayList<Point> cdf) {
    // This function is not entirely precise since it assumes
    // that the ID space is continuous, which is not an accurate
    // approximation for small ID counts

    if (cdf.size() == 0) return 0;
    // Assume CDF is piecewise linear
    double sum = 0;
    sum = cdf.get(0).probability * cdf.get(0).value;
    for (int i = 1; i < cdf.size(); i++) {
      Point prev = cdf.get(i-1);
      Point curr = cdf.get(i);
      double p = curr.probability - prev.probability;
      sum += p * curr.value;
    }
    return sum;
  }

  public static int binarySearch(ArrayList<Point> points, double p) {
    int left = 0, right = points.size() - 1;
    while (left < right) {
      int mid = (left + right)/2;
      if (points.get(mid).probability >= p) {
        right = mid;
      } else {
        left = mid + 1;
      }
    }
    if (points.get(left).probability >= p) {
      return left;
    } else {
      return left + 1;
    }
  }

  public static int binarySearch(double[] a, double p) {
    // Use built-in binary search
    int res = Arrays.binarySearch(a, p);
    if (res >= 0) {
      return res;
    } else {
      // Arrays.binarySearch returns (-(insertion point) - 1) when not found
      return -(res + 1);
    }
  }

  protected static double[] getPDF(ArrayList<Point> cdf) {
    int max_value = cdf.get(cdf.size() - 1).value;
    double[] pdf = new double[max_value + 1];

    // set all 0
    for (int i = 0; i < pdf.length; ++i) pdf[i] = 0;

    // convert cdf to pdf
    pdf[cdf.get(0).value] = cdf.get(0).probability;
    for (int i = 1; i < cdf.size(); ++i) {
      pdf[cdf.get(i).value] = cdf.get(i).probability -
        cdf.get(i - 1).probability;
    }
    return pdf;
  }

  protected static double[] getCCDF(double[] pdf) {
    int length = pdf.length;
    double[] ccdf = new double[length];
    ccdf[length - 1] = pdf[length - 1];
    for (int i = length - 2; i >= 0; --i) {
      ccdf[i] = ccdf[i + 1] + pdf[i];
    }
    return ccdf;
  }

  protected static double[] getCumulativeSum(double[] cdf) {
    int length = cdf.length;
    double[] cs = new double[length];
    cs[0] = 0; //ignore cdf[0]
    for (int i = 1; i < length; ++i) {
      cs[i] = cs[i - 1] + cdf[i];
    }
    return cs;
  }
}
