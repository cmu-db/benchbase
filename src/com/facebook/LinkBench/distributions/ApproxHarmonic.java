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

/**
 * Approximations to harmonic numbers that speed up calculation time.
 */
public class ApproxHarmonic {
  private static final long APPROX_THRESHOLD = 100000;

  // Euler Mascheroni constant
  private static final double EULER_MASCHERONI =
                  0.5772156649015328606065120900824024310421;


  /**
   * Approximation to generalized harmonic for 0 >= m >= 1.
   * Designed to not take more than a couple of seconds to calculate,
   * and the have error of < 0.05%
   * @param n
   * @param m assume > 0 <=
   * @return
   */
  public static double generalizedHarmonic(final long n,
                                                 final double m) {
    if (n < 0) {
      throw new IllegalArgumentException("n must be non-negative");
    }
    if (m < 0 || m > 1) {
      throw new IllegalArgumentException("m = " + m + " outside " +
                                         "range [0, 1]");
    }
    if (n < APPROX_THRESHOLD) {
      // Approximation less accurate for small n, and full calculation
      // doesn't take as long
      return Harmonic.generalizedHarmonic(n, m);
    }

    if (m == 1) {
      // Standard approximation for regular harmonic numbers
      return Math.log(n) + EULER_MASCHERONI + 1 / (2 * n);
    } else {
      // Rough approximation for generalized harmonic for
      // m >= 0 and m <= 1

      // Standard integral of 1/(n^k)
      double integral = (1 / (1 - m)) * Math.pow(n, 1 - m);

      // Empirically derived correction factor that is good enough
      // to get to within 0.2% or so of exact number
      double correction = 0.58 - 1 / (1 - m);
      return integral + correction;
    }
  }
}
