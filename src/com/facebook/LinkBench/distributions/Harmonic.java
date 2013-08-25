package com.facebook.LinkBench.distributions;
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * This code was derived and modified from the Apache Commons
 * Math 3.0 source release and modified for use in LinkBench
 *
 *  @author tarmstrong
 */
import org.apache.commons.math3.util.FastMath;

public class Harmonic {
  /**
   * Calculates the Nth generalized harmonic number. See
   * <a href="http://mathworld.wolfram.com/HarmonicSeries.html">Harmonic
   * Series</a>.
   *
   * @param n Term in the series to calculate (must be larger than 1)
   * @param m Exponent (special case {@code m = 1} is the harmonic series).
   * @return the n<sup>th</sup> generalized harmonic number.
   */
  public static double generalizedHarmonic(final long n, final double m) {
      double value = 0;
      for (long k = n; k > 0; --k) {
          value += 1.0 / FastMath.pow(k, m);
      }
      return value;
  }
}
