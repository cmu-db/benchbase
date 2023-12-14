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

package com.oltpbenchmark;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributionStatistics {
  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory.getLogger(DistributionStatistics.class);

  private static final double[] PERCENTILES = {0.0, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 1.0};

  private static final int MINIMUM = 0;
  private static final int PERCENTILE_25TH = 1;
  private static final int MEDIAN = 2;
  private static final int PERCENTILE_75TH = 3;
  private static final int PERCENTILE_90TH = 4;
  private static final int PERCENTILE_95TH = 5;
  private static final int PERCENTILE_99TH = 6;
  private static final int MAXIMUM = 7;

  private final int count;
  private final long[] percentiles;
  private final double average;
  private final double standardDeviation;

  public DistributionStatistics(
      int count, long[] percentiles, double average, double standardDeviation) {
    this.count = count;
    this.percentiles = Arrays.copyOfRange(percentiles, 0, PERCENTILES.length);
    this.average = average;
    this.standardDeviation = standardDeviation;
  }

  /** Computes distribution statistics over values. WARNING: This will sort values. */
  public static DistributionStatistics computeStatistics(int[] valuesAsMicroseconds) {
    if (valuesAsMicroseconds.length == 0) {
      long[] percentiles = new long[PERCENTILES.length];
      Arrays.fill(percentiles, -1);
      return new DistributionStatistics(0, percentiles, -1, -1);
    }

    Arrays.sort(valuesAsMicroseconds);

    double sum = 0;
    for (int value1 : valuesAsMicroseconds) {
      sum += value1;
    }
    double average = sum / valuesAsMicroseconds.length;

    double sumDiffsSquared = 0;
    for (int value : valuesAsMicroseconds) {
      double v = value - average;
      sumDiffsSquared += v * v;
    }
    double standardDeviation = 0;
    if (valuesAsMicroseconds.length > 1) {
      standardDeviation = Math.sqrt(sumDiffsSquared / (valuesAsMicroseconds.length - 1));
    }

    // NOTE: NIST recommends interpolating. This just selects the closest
    // value, which is described as another common technique.
    // http://www.itl.nist.gov/div898/handbook/prc/section2/prc252.htm
    long[] percentiles = new long[PERCENTILES.length];
    for (int i = 0; i < percentiles.length; ++i) {
      int index = (int) (PERCENTILES[i] * valuesAsMicroseconds.length);
      if (index == valuesAsMicroseconds.length) {
        index = valuesAsMicroseconds.length - 1;
      }
      percentiles[i] = valuesAsMicroseconds[index];
    }

    return new DistributionStatistics(
        valuesAsMicroseconds.length, percentiles, average, standardDeviation);
  }

  public int getCount() {
    return count;
  }

  public double getAverage() {
    return average;
  }

  public double getStandardDeviation() {
    return standardDeviation;
  }

  public double getMinimum() {
    return percentiles[MINIMUM];
  }

  public double get25thPercentile() {
    return percentiles[PERCENTILE_25TH];
  }

  public double getMedian() {
    return percentiles[MEDIAN];
  }

  public double get75thPercentile() {
    return percentiles[PERCENTILE_75TH];
  }

  public double get90thPercentile() {
    return percentiles[PERCENTILE_90TH];
  }

  public double get95thPercentile() {
    return percentiles[PERCENTILE_95TH];
  }

  public double get99thPercentile() {
    return percentiles[PERCENTILE_99TH];
  }

  public double getMaximum() {
    return percentiles[MAXIMUM];
  }

  @Override
  public String toString() {
    return "in milliseconds [min="
        + TimeUnit.MICROSECONDS.toMillis((long) getMinimum())
        + ", "
        + "25th="
        + TimeUnit.MICROSECONDS.toMillis((long) get25thPercentile())
        + ", "
        + "median="
        + TimeUnit.MICROSECONDS.toMillis((long) getMedian())
        + ", "
        + "avg="
        + TimeUnit.MICROSECONDS.toMillis((long) getAverage())
        + ", "
        + "75th="
        + TimeUnit.MICROSECONDS.toMillis((long) get75thPercentile())
        + ", "
        + "90th="
        + TimeUnit.MICROSECONDS.toMillis((long) get90thPercentile())
        + ", "
        + "95th="
        + TimeUnit.MICROSECONDS.toMillis((long) get95thPercentile())
        + ", "
        + "99th="
        + TimeUnit.MICROSECONDS.toMillis((long) get99thPercentile())
        + ", "
        + "max="
        + TimeUnit.MICROSECONDS.toMillis((long) getMaximum())
        + "]";
  }

  public Map<String, Integer> toMap() {
    Map<String, Integer> distMap = new LinkedHashMap<>();
    distMap.put("Minimum Latency (microseconds)", (int) getMinimum());
    distMap.put("25th Percentile Latency (microseconds)", (int) get25thPercentile());
    distMap.put("Median Latency (microseconds)", (int) getMedian());
    distMap.put("Average Latency (microseconds)", (int) getAverage());
    distMap.put("75th Percentile Latency (microseconds)", (int) get75thPercentile());
    distMap.put("90th Percentile Latency (microseconds)", (int) get90thPercentile());
    distMap.put("95th Percentile Latency (microseconds)", (int) get95thPercentile());
    distMap.put("99th Percentile Latency (microseconds)", (int) get99thPercentile());
    distMap.put("Maximum Latency (microseconds)", (int) getMaximum());
    return distMap;
  }
}
